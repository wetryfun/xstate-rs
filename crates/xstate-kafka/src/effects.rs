//! A worker for the external work the service publishes to its effects
//! topic (chiefly invoked async actors): it reads each request, runs a
//! handler, and produces the result to the input topic, keyed by the actor,
//! as XState's `{ "type": "xstate.system.effect.result", "effectId",
//! "event"? }`.
//!
//! Delivery is at least once. A request's offset is committed only after
//! its result was delivered, so a crash in between runs the handler again
//! and delivers a second result; the service takes the first and makes the
//! second a dead letter (the work is no longer pending). Handlers that
//! change other systems should be idempotent, keyed by [`Work::id`] for
//! instance, as with any at-least-once consumer. Requests are read
//! `read_committed`: the service publishes them in its transactions.
//!
//! ```no_run
//! # use std::sync::atomic::AtomicBool;
//! use serde_json::json;
//! use xstate_kafka::effects::{run, Outcome, WorkerConfig};
//!
//! let config = WorkerConfig::new("localhost:9092", "orders-effects", "orders-effects", "orders");
//! let stop = AtomicBool::new(false);
//! run(&config, &stop, |work| match work.logic.as_deref() {
//!     Some("fetchUser") => Outcome::Resolve(json!({ "name": "Ada" })),
//!     _ => Outcome::Reject(json!({ "message": "unknown work" })),
//! })
//! .unwrap();
//! ```

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer};
use rdkafka::message::{Header, Message, OwnedHeaders};
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::{Offset, TopicPartitionList};
use serde_json::{json, Value};

use crate::engine::EFFECT_RESULT;
use crate::service::ServiceError;

type Result<T> = std::result::Result<T, ServiceError>;

/// One piece of work, as the service published it.
#[derive(Debug, Clone, PartialEq)]
pub struct Work {
    /// The actor's key (the result goes back under it).
    pub key: String,
    /// The effect id (`system:3`): stable across redeliveries.
    pub id: String,
    /// The source actor's logic: for invoked async logic, the invoke's `src`.
    pub logic: Option<String>,
    /// The source actor's input (the invoke's `input`).
    pub input: Option<Value>,
    /// The whole request (XState's `SystemExternalEffect`, plus `logic` and
    /// `input`).
    pub effect: Value,
}

impl Work {
    /// Reads a request record.
    pub fn parse(key: &str, payload: &[u8]) -> std::result::Result<Self, String> {
        let effect: Value = serde_json::from_slice(payload).map_err(|e| format!("not JSON: {e}"))?;
        let id = effect.get("id").and_then(Value::as_str).ok_or("a request needs a string \"id\"")?.to_owned();
        Ok(Self {
            key: key.to_owned(),
            id,
            logic: effect.get("logic").and_then(Value::as_str).map(str::to_owned),
            input: effect.get("input").cloned(),
            effect,
        })
    }
}

/// What came of a piece of work.
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    /// It succeeded with this output (`xstate.async.resolve`, the invoke's
    /// `onDone`).
    Resolve(Value),
    /// It failed with this error (`xstate.async.reject`, the invoke's
    /// `onError`).
    Reject(Value),
    /// Retire the work without an event.
    Retire,
    /// Not this worker's: produce nothing (another worker, or a later run,
    /// must answer it, or the actor waits).
    Ignore,
}

/// The input record that delivers `outcome` (`None` for [`Outcome::Ignore`]).
pub fn result_record(work: &Work, outcome: &Outcome) -> Option<Value> {
    let event = match outcome {
        Outcome::Resolve(data) => Some(json!({ "type": "xstate.async.resolve", "data": data })),
        Outcome::Reject(data) => Some(json!({ "type": "xstate.async.reject", "data": data })),
        Outcome::Retire => None,
        Outcome::Ignore => return None,
    };
    let mut result = json!({ "type": EFFECT_RESULT, "effectId": work.id });
    if let Some(event) = event {
        result["event"] = event;
    }
    Some(result)
}

/// Where the worker reads and writes.
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub bootstrap_servers: String,
    /// The worker's consumer group: instances share the requests by
    /// partition.
    pub group_id: String,
    pub effects_topic: String,
    /// The service's input topic, where results go.
    pub input_topic: String,
    /// Extra librdkafka settings (TLS, SASL, ...).
    pub client_config: Vec<(String, String)>,
    /// Requests handled before their results are flushed and their offsets
    /// committed.
    pub max_batch: usize,
    pub poll_timeout: Duration,
    pub request_timeout: Duration,
}

impl WorkerConfig {
    pub fn new(
        bootstrap_servers: impl Into<String>,
        group_id: impl Into<String>,
        effects_topic: impl Into<String>,
        input_topic: impl Into<String>,
    ) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            group_id: group_id.into(),
            effects_topic: effects_topic.into(),
            input_topic: input_topic.into(),
            client_config: Vec::new(),
            max_batch: 500,
            poll_timeout: Duration::from_millis(100),
            request_timeout: Duration::from_secs(30),
        }
    }

    fn client(&self) -> ClientConfig {
        let mut c = ClientConfig::new();
        c.set("bootstrap.servers", &self.bootstrap_servers);
        for (k, v) in &self.client_config {
            c.set(k, v);
        }
        c
    }
}

/// Handles requests until `stop` is set, then commits what it handled and
/// leaves the group. Requests that are not valid are skipped (and logged).
pub fn run(config: &WorkerConfig, stop: &AtomicBool, mut handler: impl FnMut(&Work) -> Outcome) -> Result<()> {
    let consumer: BaseConsumer = config
        .client()
        .set("group.id", &config.group_id)
        .set("enable.auto.commit", "false")
        .set("auto.offset.reset", "earliest")
        .set("isolation.level", "read_committed")
        .set("partition.assignment.strategy", "cooperative-sticky")
        .create()?;
    consumer.subscribe(&[&config.effects_topic])?;
    let producer: BaseProducer = config.client().set("enable.idempotence", "true").create()?;
    let headers = || OwnedHeaders::new().insert(Header { key: "content-type", value: Some("application/json") });

    while !stop.load(Ordering::Relaxed) {
        let mut done = TopicPartitionList::new();
        let mut handled = 0;
        let mut timeout = config.poll_timeout;
        while handled < config.max_batch {
            let Some(message) = consumer.poll(timeout) else { break };
            let message = message?;
            // Later polls in the batch only take what has arrived.
            timeout = Duration::ZERO;
            handled += 1;
            done.add_partition_offset(message.topic(), message.partition(), Offset::Offset(message.offset() + 1))?;
            let key = message.key().map(String::from_utf8_lossy).unwrap_or_default();
            let work = match Work::parse(&key, message.payload().unwrap_or_default()) {
                Ok(work) => work,
                Err(e) => {
                    eprintln!("xstate-kafka effects: skipping the request at offset {}: {e}", message.offset());
                    continue;
                }
            };
            let Some(result) = result_record(&work, &handler(&work)) else { continue };
            let payload = result.to_string();
            let mut record =
                BaseRecord::<str, str>::to(&config.input_topic).key(&work.key).payload(&payload).headers(headers());
            // A full queue: deliver what is queued, then try again.
            loop {
                match producer.send(record) {
                    Ok(()) => break,
                    Err((
                        rdkafka::error::KafkaError::MessageProduction(rdkafka::types::RDKafkaErrorCode::QueueFull),
                        r,
                    )) => {
                        producer.poll(Duration::from_millis(100));
                        record = r;
                    }
                    Err((e, _)) => return Err(e.into()),
                }
            }
        }
        if handled == 0 {
            continue;
        }
        // Results first, then the offsets: at least once.
        producer.flush(config.request_timeout)?;
        consumer.commit(&done, CommitMode::Sync)?;
    }
    consumer.unsubscribe();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_and_results() {
        let request = br#"{"id":"system:4","source":{"$actor":"lookup/fetch","incarnation":"1"},"type":"@xstate.effect","logic":"fetchUser","input":{"user":7}}"#;
        let work = Work::parse("u1", request).unwrap();
        assert_eq!(
            (work.id.as_str(), work.logic.as_deref(), &work.input),
            ("system:4", Some("fetchUser"), &Some(json!({ "user": 7 })))
        );
        assert_eq!(
            result_record(&work, &Outcome::Resolve(json!({ "name": "Ada" }))),
            Some(
                json!({ "type": EFFECT_RESULT, "effectId": "system:4", "event": { "type": "xstate.async.resolve", "data": { "name": "Ada" } } })
            )
        );
        assert_eq!(
            result_record(&work, &Outcome::Retire),
            Some(json!({ "type": EFFECT_RESULT, "effectId": "system:4" }))
        );
        assert_eq!(result_record(&work, &Outcome::Ignore), None);
        assert!(Work::parse("u1", b"{}").is_err());
    }
}
