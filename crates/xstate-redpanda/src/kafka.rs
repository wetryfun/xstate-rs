//! The Kafka driver: worker threads that consume the input topic, run each
//! partition's actors, and commit output, changelog and offsets in one
//! transaction per batch.
//!
//! Topics (create them beforehand):
//!
//! - the input topic: one record per event, keyed by actor id;
//! - the output topic: emitted events, keyed by actor id;
//! - the changelog topic: `cleanup.policy=compact`, with as many partitions
//!   as the input topic; partition `p` holds the snapshots of the actors of
//!   input partition `p`;
//! - optionally a dead-letter topic, for records that are not events (the
//!   reason is in the `reason` header).
//!
//! Each worker owns a consumer in the service's consumer group and a
//! transactional producer (`transactional.id` = prefix + worker index; with
//! KIP-447 transactions, fencing follows the group generation). A partition's
//! actors are restored from its changelog partition the first time a record
//! of that partition arrives. When a transaction fails, the worker aborts
//! it, forgets its partitions and rewinds to the committed offsets, so the
//! batch is processed again from the last committed state.
//!
//! Set `rack` (librdkafka `client.rack`) to the broker node's rack to read
//! from the local replica (follower fetching, `enable_rack_awareness` on the
//! cluster).

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, Message, OwnedHeaders};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::{Offset, TopicPartitionList};
use serde::de::DeserializeOwned;
use serde::Serialize;
use xstate_rt::{EventType, Machine};

use crate::engine::{EngineConfig, Output, Partition};

/// How the service connects and batches.
#[derive(Debug, Clone)]
pub struct ServiceConfig {
    pub bootstrap_servers: String,
    pub group_id: String,
    pub input_topic: String,
    pub output_topic: String,
    pub changelog_topic: String,
    pub dead_letter_topic: Option<String>,
    /// Prefix of the workers' `transactional.id`s; defaults to the group id.
    pub transactional_id: String,
    /// Worker threads (consumers); defaults to the number of cores.
    pub workers: usize,
    /// `client.rack`, for follower fetching from the local replica.
    pub rack: Option<String>,
    /// Records per transaction, at most.
    pub max_batch: usize,
    /// How long a batch keeps collecting records after the first one, while
    /// it is below `max_batch`. Longer batches amortize the transaction
    /// commit; shorter ones lower latency.
    pub batch_linger: Duration,
    /// How long a worker waits for records when it has no due timer.
    pub poll_timeout: Duration,
    /// Timeout for transaction and metadata calls.
    pub request_timeout: Duration,
    /// Extra librdkafka settings for every client.
    pub client_config: Vec<(String, String)>,
    pub engine: EngineConfig,
}

impl ServiceConfig {
    pub fn new(
        bootstrap_servers: impl Into<String>,
        group_id: impl Into<String>,
        input_topic: impl Into<String>,
        output_topic: impl Into<String>,
        changelog_topic: impl Into<String>,
    ) -> Self {
        let group_id = group_id.into();
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            transactional_id: group_id.clone(),
            group_id,
            input_topic: input_topic.into(),
            output_topic: output_topic.into(),
            changelog_topic: changelog_topic.into(),
            dead_letter_topic: None,
            workers: std::thread::available_parallelism().map_or(1, |n| n.get()),
            rack: None,
            max_batch: 10_000,
            batch_linger: Duration::from_millis(20),
            poll_timeout: Duration::from_millis(100),
            request_timeout: Duration::from_secs(30),
            client_config: Vec::new(),
            engine: EngineConfig::default(),
        }
    }

    fn client(&self) -> ClientConfig {
        let mut c = ClientConfig::new();
        c.set("bootstrap.servers", &self.bootstrap_servers);
        if let Some(rack) = &self.rack {
            c.set("client.rack", rack);
        }
        for (k, v) in &self.client_config {
            c.set(k, v);
        }
        c
    }
}

/// A service failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceError(pub String);

impl fmt::Display for ServiceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ServiceError {}

impl From<KafkaError> for ServiceError {
    fn from(e: KafkaError) -> Self {
        ServiceError(e.to_string())
    }
}

type Result<T> = std::result::Result<T, ServiceError>;

/// Milliseconds since the Unix epoch: the service's clock for actors.
pub fn clock_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

/// Running workers.
pub struct Service {
    stop: Arc<AtomicBool>,
    workers: Vec<JoinHandle<Result<()>>>,
}

impl Service {
    /// Starts `config.workers` worker threads running `machine`.
    pub fn start<C, E>(config: ServiceConfig, machine: Arc<Machine<C, E>>) -> Result<Self>
    where
        C: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
        E: EventType + Send + Sync,
    {
        let config = Arc::new(config);
        let stop = Arc::new(AtomicBool::new(false));
        let workers = (0..config.workers.max(1))
            .map(|i| {
                let (config, machine, stop) = (config.clone(), machine.clone(), stop.clone());
                std::thread::Builder::new()
                    .name(format!("xstate-worker-{i}"))
                    .spawn(move || Worker::new(i, &config, machine)?.run(&stop))
                    .map_err(|e| ServiceError(format!("spawning a worker failed: {e}")))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { stop, workers })
    }

    /// Whether every worker is still running.
    pub fn is_running(&self) -> bool {
        self.workers.iter().all(|w| !w.is_finished())
    }

    /// Stops the workers after their current batch and waits for them.
    pub fn stop(self) -> Result<()> {
        self.stop.store(true, Ordering::Relaxed);
        let mut first_error = None;
        for w in self.workers {
            match w.join() {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    first_error.get_or_insert(e);
                }
                Err(_) => {
                    first_error.get_or_insert(ServiceError("a worker panicked".into()));
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }
}

struct Worker<'a, C, E> {
    config: &'a ServiceConfig,
    machine: Arc<Machine<C, E>>,
    consumer: BaseConsumer,
    producer: BaseProducer,
    /// Reads changelog partitions when they are assigned; reused.
    restorer: BaseConsumer,
    partitions: HashMap<i32, Partition<C, E>>,
}

impl<'a, C, E> Worker<'a, C, E>
where
    C: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    E: EventType + Send + Sync,
{
    fn new(index: usize, config: &'a ServiceConfig, machine: Arc<Machine<C, E>>) -> Result<Self> {
        let consumer: BaseConsumer = config
            .client()
            .set("group.id", &config.group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("isolation.level", "read_committed")
            .create()?;
        consumer.subscribe(&[&config.input_topic])?;
        let producer: BaseProducer = config
            .client()
            .set("transactional.id", format!("{}-{index}", config.transactional_id))
            .set("enable.idempotence", "true")
            .create()?;
        producer.init_transactions(config.request_timeout)?;
        let restorer = restore_consumer(config)?;
        Ok(Self { config, machine, consumer, producer, restorer, partitions: HashMap::new() })
    }

    fn run(mut self, stop: &AtomicBool) -> Result<()> {
        while !stop.load(Ordering::Relaxed) {
            if let Err(e) = self.batch() {
                debug(|| format!("batch failed: {e}"));
                // Start over from the last committed state.
                let _ = self.producer.abort_transaction(self.config.request_timeout);
                self.partitions.clear();
                self.rewind()?;
                if is_fatal(&e) {
                    return Err(e);
                }
            }
        }
        // Leave the group now, so the partitions move to other workers
        // without waiting for this member's session to time out.
        self.consumer.unsubscribe();
        debug(|| "stopped".into());
        Ok(())
    }

    /// Polls up to `max_batch` records, runs them and due timers, and
    /// commits the result.
    fn batch(&mut self) -> Result<()> {
        let config = self.config;
        // Restore newly assigned partitions together, and forget revoked ones.
        let assigned: HashSet<i32> =
            self.consumer.assignment()?.elements_for_topic(&config.input_topic).iter().map(|e| e.partition()).collect();
        self.partitions.retain(|p, _| assigned.contains(p));
        let missing: Vec<i32> = assigned.iter().copied().filter(|p| !self.partitions.contains_key(p)).collect();
        if !missing.is_empty() {
            self.restore(&missing)?;
        }
        let mut offsets: HashMap<i32, i64> = HashMap::new();
        let now = clock_ms();
        let next_timer = self.partitions.values_mut().filter_map(|p| p.next_deadline()).min();
        let mut timeout = match next_timer {
            Some(due) => Duration::from_millis(due.saturating_sub(now)).min(config.poll_timeout),
            None => config.poll_timeout,
        };
        let started = std::time::Instant::now();
        let mut first: Option<std::time::Instant> = None;
        let mut polled = 0;
        while polled < config.max_batch {
            let Some(message) = self.consumer.poll(timeout) else { break };
            polled += 1;
            let first = *first.get_or_insert_with(std::time::Instant::now);
            timeout = config.batch_linger.saturating_sub(first.elapsed());
            let message = message?;
            let partition = message.partition();
            if !self.partitions.contains_key(&partition) {
                // Assigned during this poll.
                let restored = restore_with(&self.restorer, config, &self.machine, &[partition])?;
                self.partitions.extend(restored);
            }
            let key = message.key().map(|k| String::from_utf8_lossy(k).into_owned());
            let p = self.partitions.get_mut(&partition).expect("restored");
            p.handle(key.as_deref(), message.payload().unwrap_or_default(), clock_ms());
            offsets.insert(partition, message.offset());
        }

        let now = clock_ms();
        let mut outputs: Vec<(i32, Output)> = Vec::new();
        for (&id, p) in &mut self.partitions {
            p.tick(now);
            let out = p.flush();
            if !out.is_empty() {
                outputs.push((id, out));
            }
        }
        if outputs.is_empty() && offsets.is_empty() {
            return Ok(());
        }
        let processed = started.elapsed();
        debug(|| {
            format!(
                "committing {polled} records after {processed:?}: offsets {offsets:?}, output of partitions {:?}",
                outputs.iter().map(|o| o.0).collect::<Vec<_>>()
            )
        });
        let result = self.commit(outputs, offsets);
        debug(|| format!("committed after {:?}", started.elapsed()));
        result
    }

    fn restore(&mut self, partitions: &[i32]) -> Result<()> {
        let started = std::time::Instant::now();
        let restored = restore_with(&self.restorer, self.config, &self.machine, partitions)?;
        debug(|| {
            let actors: usize = restored.values().map(Partition::len).sum();
            format!("restored partitions {partitions:?} ({actors} actors) in {:?}", started.elapsed())
        });
        self.partitions.extend(restored);
        Ok(())
    }

    fn commit(&self, outputs: Vec<(i32, Output)>, offsets: HashMap<i32, i64>) -> Result<()> {
        let config = self.config;
        let producer = &self.producer;
        producer.begin_transaction()?;
        for (partition, out) in &outputs {
            for e in &out.emitted {
                let payload = e.event.to_string();
                send(producer, BaseRecord::to(&config.output_topic).key(&e.key).payload(&payload))?;
            }
            for (key, record) in &out.changelog {
                let mut r = BaseRecord::<str, [u8]>::to(&config.changelog_topic).partition(*partition).key(key);
                if let Some(record) = record {
                    r = r.payload(record);
                }
                send(producer, r)?;
            }
            if let Some(topic) = &config.dead_letter_topic {
                for d in &out.dead_letters {
                    let headers = OwnedHeaders::new().insert(Header { key: "reason", value: Some(&d.reason) });
                    let mut r = BaseRecord::<str, [u8]>::to(topic).payload(&d.payload).headers(headers);
                    if let Some(key) = &d.key {
                        r = r.key(key);
                    }
                    send(producer, r)?;
                }
            }
        }
        if !offsets.is_empty() {
            let mut tpl = TopicPartitionList::new();
            for (&partition, &offset) in &offsets {
                tpl.add_partition_offset(&config.input_topic, partition, Offset::Offset(offset + 1))?;
            }
            let group =
                self.consumer.group_metadata().ok_or_else(|| ServiceError("no consumer group metadata".into()))?;
            producer.send_offsets_to_transaction(&tpl, &group, config.request_timeout)?;
        }
        producer.commit_transaction(config.request_timeout)?;
        Ok(())
    }

    /// Moves every assigned partition back to its committed offset.
    fn rewind(&self) -> Result<()> {
        let committed = self.consumer.committed(self.config.request_timeout)?;
        for e in committed.elements() {
            let offset = match e.offset() {
                Offset::Offset(o) => Offset::Offset(o),
                _ => Offset::Beginning,
            };
            self.consumer.seek(e.topic(), e.partition(), offset, self.config.request_timeout)?;
        }
        Ok(())
    }
}

/// `XSTATE_REDPANDA_DEBUG=1` traces worker activity on stderr.
fn debug(message: impl FnOnce() -> String) {
    if std::env::var_os("XSTATE_REDPANDA_DEBUG").is_some() {
        eprintln!("[{} {}] {}", clock_ms(), std::thread::current().name().unwrap_or("?"), message());
    }
}

fn is_fatal(e: &ServiceError) -> bool {
    e.0.contains("fatal") || e.0.contains("Fenced") || e.0.contains("fenced")
}

/// Queues a record, waiting while the producer's queue is full.
fn send<K, P>(producer: &BaseProducer, mut record: BaseRecord<'_, K, P>) -> Result<()>
where
    K: rdkafka::message::ToBytes + ?Sized,
    P: rdkafka::message::ToBytes + ?Sized,
{
    loop {
        match producer.send(record) {
            Ok(()) => return Ok(()),
            Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), r)) => {
                producer.poll(Duration::from_millis(10));
                record = r;
            }
            Err((e, _)) => return Err(e.into()),
        }
    }
}

fn restore_consumer(config: &ServiceConfig) -> Result<BaseConsumer> {
    Ok(config
        .client()
        .set("group.id", format!("{}-restore", config.group_id))
        .set("enable.auto.commit", "false")
        .set("enable.partition.eof", "true")
        .set("isolation.level", "read_committed")
        // Reading to the end as fast as possible: don't wait for more data.
        .set("fetch.wait.max.ms", "10")
        .set("fetch.error.backoff.ms", "10")
        .create()?)
}

/// Rebuilds a partition's actors from its changelog partition (committed
/// records only, latest record per key).
pub fn restore<C, E>(config: &ServiceConfig, machine: &Arc<Machine<C, E>>, partition: i32) -> Result<Partition<C, E>>
where
    C: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    E: EventType + Send + Sync,
{
    let consumer = restore_consumer(config)?;
    let mut restored = restore_with(&consumer, config, machine, &[partition])?;
    Ok(restored.remove(&partition).expect("restored"))
}

/// Reads several changelog partitions to their end with `consumer`.
fn restore_with<C, E>(
    consumer: &BaseConsumer,
    config: &ServiceConfig,
    machine: &Arc<Machine<C, E>>,
    partitions: &[i32],
) -> Result<HashMap<i32, Partition<C, E>>>
where
    C: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    E: EventType + Send + Sync,
{
    let mut tpl = TopicPartitionList::new();
    for &p in partitions {
        tpl.add_partition_offset(&config.changelog_topic, p, Offset::Beginning)?;
    }
    consumer.assign(&tpl)?;
    let mut restored: HashMap<i32, Partition<C, E>> =
        partitions.iter().map(|&p| (p, Partition::new(machine.clone(), config.engine.clone()))).collect();
    let mut pending: HashSet<i32> = partitions.iter().copied().collect();
    while !pending.is_empty() {
        match consumer.poll(config.request_timeout) {
            Some(Ok(m)) => {
                let partition = m.partition();
                let key = m.key().map(|k| String::from_utf8_lossy(k).into_owned()).unwrap_or_default();
                let p = restored
                    .get_mut(&partition)
                    .ok_or_else(|| ServiceError(format!("unexpected partition {partition}")))?;
                p.restore(&key, m.payload()).map_err(|e| {
                    ServiceError(format!("restoring partition {partition} at offset {}: {e}", m.offset()))
                })?;
            }
            Some(Err(KafkaError::PartitionEOF(partition))) => {
                pending.remove(&partition);
            }
            Some(Err(e)) => return Err(e.into()),
            None => return Err(ServiceError(format!("timed out restoring changelog partitions {pending:?}"))),
        }
    }
    consumer.unassign()?;
    Ok(restored)
}
