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
//! - optionally an effects topic: external work the actors request (the
//!   async actors they invoke), keyed by actor id, as XState's
//!   `SystemExternalEffect` JSON. Whoever does the work produces the result
//!   to the input topic under the same key (see [`crate::engine`]);
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
//! With `state_dir` set, workers also write a local checkpoint of each
//! partition (every live actor's changelog record and the changelog offset
//! it covers) at most every `checkpoint_interval`, and a restore loads the
//! checkpoint and replays only the changelog after it. A checkpoint's offset
//! is the changelog's end before the transaction that produced its state, so
//! the replayed tail always ends in the same states; a missing, unreadable
//! or out-of-range checkpoint falls back to reading the whole changelog.
//!
//! Set `rack` (librdkafka `client.rack`) to the broker node's rack to read
//! from the local replica (follower fetching, `enable_rack_awareness` on the
//! cluster).

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::JoinHandle;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::{Header, Message, OwnedHeaders};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::{Offset, TopicPartitionList};
use serde::de::DeserializeOwned;
use serde::Serialize;
use xstate_rt::{EventType, Machine};

use crate::checkpoint::{self, Checkpoint};
use crate::engine::{EngineConfig, Output, Partition};
use crate::metrics::{Metrics, MetricsSnapshot};

/// How the service connects and batches.
#[derive(Debug, Clone)]
pub struct ServiceConfig {
    pub bootstrap_servers: String,
    pub group_id: String,
    pub input_topic: String,
    pub output_topic: String,
    pub changelog_topic: String,
    pub dead_letter_topic: Option<String>,
    /// Where requested external work goes; without it, work stays pending.
    pub effects_topic: Option<String>,
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
    /// Directory for local partition checkpoints; `None` disables them.
    pub state_dir: Option<PathBuf>,
    /// Minimum time between two checkpoints of a partition.
    pub checkpoint_interval: Duration,
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
            effects_topic: None,
            workers: std::thread::available_parallelism().map_or(1, |n| n.get()),
            rack: None,
            max_batch: 10_000,
            batch_linger: Duration::from_millis(20),
            poll_timeout: Duration::from_millis(100),
            request_timeout: Duration::from_secs(30),
            client_config: Vec::new(),
            engine: EngineConfig::default(),
            state_dir: None,
            checkpoint_interval: Duration::from_secs(60),
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
    metrics: Arc<Metrics>,
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
        let metrics = Arc::new(Metrics::new(config.workers.max(1)));
        let workers = (0..config.workers.max(1))
            .map(|i| {
                let (config, machine, stop, metrics) = (config.clone(), machine.clone(), stop.clone(), metrics.clone());
                std::thread::Builder::new()
                    .name(format!("xstate-worker-{i}"))
                    .spawn(move || Worker::new(i, &config, machine, metrics)?.run(&stop))
                    .map_err(|e| ServiceError(format!("spawning a worker failed: {e}")))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { stop, workers, metrics })
    }

    /// The service's counters so far.
    pub fn metrics(&self) -> MetricsSnapshot {
        self.metrics.snapshot()
    }

    /// The shared counters, for exporting them while the service runs.
    pub fn metrics_handle(&self) -> Arc<Metrics> {
        self.metrics.clone()
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
    index: usize,
    metrics: Arc<Metrics>,
    config: &'a ServiceConfig,
    machine: Arc<Machine<C, E>>,
    consumer: BaseConsumer,
    producer: BaseProducer,
    /// Reads changelog partitions when they are assigned; reused.
    restorer: BaseConsumer,
    partitions: HashMap<i32, Partition<C, E>>,
    /// When each partition was last checkpointed (or restored).
    checkpointed: HashMap<i32, Instant>,
    /// Partitions whose state changed since their last checkpoint.
    changed: HashSet<i32>,
}

impl<'a, C, E> Worker<'a, C, E>
where
    C: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    E: EventType + Send + Sync,
{
    fn new(
        index: usize,
        config: &'a ServiceConfig,
        machine: Arc<Machine<C, E>>,
        metrics: Arc<Metrics>,
    ) -> Result<Self> {
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
        Ok(Self {
            index,
            metrics,
            config,
            machine,
            consumer,
            producer,
            restorer,
            partitions: HashMap::new(),
            checkpointed: HashMap::new(),
            changed: HashSet::new(),
        })
    }

    fn run(mut self, stop: &AtomicBool) -> Result<()> {
        while !stop.load(Ordering::Relaxed) {
            if let Err(e) = self.batch() {
                debug(|| format!("batch failed: {e}"));
                self.metrics.aborted();
                // Start over from the last committed state.
                let _ = self.producer.abort_transaction(self.config.request_timeout);
                self.forget(|_| false);
                self.rewind()?;
                if is_fatal(&e) {
                    return Err(e);
                }
            }
        }
        // A clean stop leaves exact checkpoints: nothing to replay.
        let due = self.checkpoints_due(true);
        self.write_checkpoints(due, &HashSet::new());
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
        self.forget(|p| assigned.contains(&p));
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
        let started = Instant::now();
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
                restore_into(
                    &self.restorer,
                    config,
                    &self.machine,
                    &self.metrics,
                    &mut self.partitions,
                    &mut self.checkpointed,
                    &[partition],
                )?;
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
            // Everything is committed: checkpoints now cover the whole changelog.
            let due = self.checkpoints_due(false);
            self.write_checkpoints(due, &HashSet::new());
            return Ok(());
        }
        let processed = started.elapsed();
        debug(|| {
            format!(
                "committing {polled} records after {processed:?}: offsets {offsets:?}, output of partitions {:?}",
                outputs.iter().map(|o| o.0).collect::<Vec<_>>()
            )
        });
        let count = |f: fn(&Output) -> usize| outputs.iter().map(|(_, o)| f(o) as u64).sum::<u64>();
        let (emitted, dead, changelog, effects) = (
            count(|o| o.emitted.len()),
            count(|o| o.dead_letters.len()),
            count(|o| o.changelog.len()),
            count(|o| o.effects.len()),
        );
        let touched: HashSet<i32> = outputs.iter().filter(|(_, o)| !o.changelog.is_empty()).map(|(p, _)| *p).collect();
        self.changed.extend(&touched);
        let checkpoints = self.checkpoints_due(false);
        let committing = Instant::now();
        self.commit(outputs, offsets)?;
        let micros = committing.elapsed().as_micros() as u64;
        self.metrics.committed(polled as u64, emitted, dead, changelog, effects, micros);
        self.metrics.set_actors(self.index, self.partitions.values().map(|p| p.len() as u64).sum());
        self.write_checkpoints(checkpoints, &touched);
        debug(|| format!("committed after {:?}", started.elapsed()));
        Ok(())
    }

    fn restore(&mut self, partitions: &[i32]) -> Result<()> {
        restore_into(
            &self.restorer,
            self.config,
            &self.machine,
            &self.metrics,
            &mut self.partitions,
            &mut self.checkpointed,
            partitions,
        )
    }

    /// Drops the partitions `keep` rejects.
    fn forget(&mut self, keep: impl Fn(i32) -> bool) {
        self.partitions.retain(|&p, _| keep(p));
        self.checkpointed.retain(|&p, _| keep(p));
        self.changed.retain(|&p| keep(p));
    }

    /// The changed partitions due for a checkpoint (all of them when
    /// `force`), each with its changelog partition's end offset before this
    /// batch's transaction: the offset the checkpoint will cover. Call
    /// before committing.
    fn checkpoints_due(&self, force: bool) -> Vec<(i32, i64)> {
        if self.config.state_dir.is_none() {
            return Vec::new();
        }
        let interval = self.config.checkpoint_interval;
        let mut due = Vec::new();
        for &p in &self.changed {
            if !force && self.checkpointed.get(&p).is_some_and(|t| t.elapsed() < interval) {
                continue;
            }
            match self.restorer.fetch_watermarks(&self.config.changelog_topic, p, self.config.request_timeout) {
                Ok((_, high)) => due.push((p, high)),
                Err(e) => debug(|| format!("no checkpoint of partition {p}: {e}")),
            }
        }
        due
    }

    /// Writes the checkpoints [`Self::checkpoints_due`] chose, after their
    /// transaction committed. A checkpoint of a partition the transaction
    /// `touched` leaves that transaction's records to replay, so the
    /// partition stays changed and the next batch that does not touch it
    /// writes an exact one. A failed write only costs restore time.
    fn write_checkpoints(&mut self, due: Vec<(i32, i64)>, touched: &HashSet<i32>) {
        let Some(dir) = &self.config.state_dir else { return };
        for (p, next_offset) in due {
            let Some(partition) = self.partitions.get(&p) else { continue };
            let path = checkpoint::path(dir, &self.config.changelog_topic, p);
            let c = Checkpoint { next_offset, records: partition.snapshot_records() };
            match checkpoint::write(&path, &c) {
                Ok(()) => {
                    if !touched.contains(&p) {
                        self.changed.remove(&p);
                    }
                    self.checkpointed.insert(p, Instant::now());
                    self.metrics.checkpointed();
                    debug(|| {
                        format!("checkpointed partition {p} at offset {next_offset} ({} actors)", c.records.len())
                    });
                }
                Err(e) => eprintln!("xstate-redpanda: writing {} failed: {e}", path.display()),
            }
        }
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
            if let Some(topic) = &config.effects_topic {
                for e in &out.effects {
                    let payload = e.effect.to_string();
                    send(producer, BaseRecord::to(topic).key(&e.key).payload(&payload))?;
                }
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

/// Restores `partitions` into a worker's partition map (a function of the
/// worker's fields, so it can run while a polled message borrows the
/// consumer).
fn restore_into<C, E>(
    restorer: &BaseConsumer,
    config: &ServiceConfig,
    machine: &Arc<Machine<C, E>>,
    metrics: &Metrics,
    into: &mut HashMap<i32, Partition<C, E>>,
    checkpointed: &mut HashMap<i32, Instant>,
    partitions: &[i32],
) -> Result<()>
where
    C: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    E: EventType + Send + Sync,
{
    let started = Instant::now();
    let (restored, replayed) = restore_with(restorer, config, machine, partitions)?;
    let actors: usize = restored.values().map(Partition::len).sum();
    metrics.restored(partitions.len() as u64, actors as u64, replayed);
    debug(|| {
        format!(
            "restored partitions {partitions:?} ({actors} actors, {replayed} changelog records) in {:?}",
            started.elapsed()
        )
    });
    for &p in partitions {
        checkpointed.insert(p, started);
    }
    into.extend(restored);
    Ok(())
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
    let (mut restored, _) = restore_with(&consumer, config, machine, &[partition])?;
    Ok(restored.remove(&partition).expect("restored"))
}

type Partitions<C, E> = HashMap<i32, Partition<C, E>>;

/// Reads several changelog partitions to their end with `consumer`, each
/// from its local checkpoint when there is a usable one. Returns the
/// partitions and the number of changelog records replayed.
fn restore_with<C, E>(
    consumer: &BaseConsumer,
    config: &ServiceConfig,
    machine: &Arc<Machine<C, E>>,
    partitions: &[i32],
) -> Result<(Partitions<C, E>, u64)>
where
    C: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    E: EventType + Send + Sync,
{
    let mut tpl = TopicPartitionList::new();
    let mut restored: HashMap<i32, Partition<C, E>> = HashMap::new();
    for &p in partitions {
        let mut partition = Partition::new(machine.clone(), config.engine.clone());
        let start = match load_checkpoint(consumer, config, p, &mut partition) {
            Some(offset) => Offset::Offset(offset),
            None => {
                partition = Partition::new(machine.clone(), config.engine.clone());
                Offset::Beginning
            }
        };
        tpl.add_partition_offset(&config.changelog_topic, p, start)?;
        restored.insert(p, partition);
    }
    consumer.assign(&tpl)?;
    let mut replayed = 0;
    let mut pending: HashSet<i32> = partitions.iter().copied().collect();
    while !pending.is_empty() {
        match consumer.poll(config.request_timeout) {
            Some(Ok(m)) => {
                replayed += 1;
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
    Ok((restored, replayed))
}

/// Applies partition `p`'s checkpoint to `partition` and returns the
/// changelog offset to continue from, or `None` (leaving `partition` to be
/// discarded) when there is no usable checkpoint: none, unreadable, or
/// outside the changelog partition's offsets (a recreated or truncated
/// topic).
fn load_checkpoint<C, E>(
    consumer: &BaseConsumer,
    config: &ServiceConfig,
    p: i32,
    partition: &mut Partition<C, E>,
) -> Option<i64>
where
    C: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    E: EventType + Send + Sync,
{
    let dir = config.state_dir.as_ref()?;
    let path = checkpoint::path(dir, &config.changelog_topic, p);
    let warn = |what: String| eprintln!("xstate-redpanda: ignoring checkpoint {}: {what}", path.display());
    let c = match checkpoint::read(&path) {
        Ok(c) => c?,
        Err(e) => {
            warn(e.to_string());
            return None;
        }
    };
    let (low, high) = match consumer.fetch_watermarks(&config.changelog_topic, p, config.request_timeout) {
        Ok(w) => w,
        Err(e) => {
            warn(format!("no changelog offsets: {e}"));
            return None;
        }
    };
    if c.next_offset < low || c.next_offset > high {
        warn(format!("offset {} is outside the changelog's {low}..{high}", c.next_offset));
        return None;
    }
    for (key, record) in &c.records {
        if let Err(e) = partition.restore(key, Some(record)) {
            warn(e);
            return None;
        }
    }
    debug(|| format!("partition {p}: {} actors from the checkpoint at offset {}", c.records.len(), c.next_offset));
    Some(c.next_offset)
}
