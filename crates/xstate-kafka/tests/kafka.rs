//! The service against librdkafka's in-process mock cluster (transactions,
//! consumer groups): output, timers, dead letters, and a restart that
//! restores actors from the changelog without duplicating output.
#![cfg(feature = "kafka")]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Headers, Message};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::{Offset, TopicPartitionList};
use serde_json::{json, Value};
use xstate_kafka::service::{Service, ServiceConfig};
use xstate_rt::{Machine, Registry};

const PARTITIONS: i32 = 4;

/// A record read back: key, payload, and the `reason` header.
type Record = (Option<String>, Option<Vec<u8>>, Option<String>);

fn machine() -> Arc<Machine<Value, Value>> {
    let at = |s: &str| json!([{ "type": "@xstate.emit", "event": { "type": "at", "state": s } }]);
    let definition = json!({
        "id": "steps",
        "initial": "s0",
        "states": {
            "s0": { "on": { "next": { "target": "s1" } } },
            "s1": { "entry": at("s1"), "on": { "next": { "target": "s2" }, "wait": { "target": "waiting" } } },
            "waiting": { "after": { "200": { "target": "s2" } } },
            "s2": { "entry": at("s2"), "on": { "next": { "target": "s3" } } },
            "s3": { "type": "final", "entry": at("s3") }
        }
    });
    Arc::new(Machine::from_json(&definition, &Registry::new()).unwrap())
}

fn produce(bootstrap: &str, records: &[(Option<&str>, &str)]) {
    let producer: BaseProducer = ClientConfig::new().set("bootstrap.servers", bootstrap).create().unwrap();
    for (key, payload) in records {
        let mut r = BaseRecord::<str, str>::to("input").payload(payload);
        if let Some(key) = key {
            r = r.key(key);
        }
        producer.send(r).unwrap();
    }
    producer.flush(Duration::from_secs(10)).unwrap();
}

/// Reads committed records of `topic` from the beginning until `count`
/// arrived (or a deadline passed), then a little longer to catch extras.
fn read(bootstrap: &str, topic: &str, count: usize) -> Vec<Record> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", format!("reader-{topic}-{}", rand_suffix()))
        .set("isolation.level", "read_committed")
        .set("enable.auto.commit", "false")
        .create()
        .unwrap();
    let mut tpl = TopicPartitionList::new();
    for p in 0..PARTITIONS {
        tpl.add_partition_offset(topic, p, Offset::Beginning).unwrap();
    }
    consumer.assign(&tpl).unwrap();
    let mut out = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut quiet_until: Option<Instant> = None;
    while Instant::now() < deadline && quiet_until.map_or(true, |q| Instant::now() < q) {
        if let Some(Ok(m)) = consumer.poll(Duration::from_millis(50)) {
            let reason = m
                .headers()
                .and_then(|h| h.iter().find(|h| h.key == "reason"))
                .and_then(|h| h.value)
                .map(|v| String::from_utf8_lossy(v).into_owned());
            out.push((
                m.key().map(|k| String::from_utf8_lossy(k).into_owned()),
                m.payload().map(<[u8]>::to_vec),
                reason,
            ));
        }
        if out.len() >= count && quiet_until.is_none() {
            quiet_until = Some(Instant::now() + Duration::from_millis(500));
        }
    }
    out
}

fn commit_processed(bootstrap: &str) {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap)
        .set("group.id", "steps-service")
        .set("enable.auto.commit", "false")
        .set("session.timeout.ms", "6000")
        .create()
        .unwrap();
    // Join the group (the mock only takes commits from members).
    consumer.subscribe(&["input"]).unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while consumer.assignment().unwrap().count() < PARTITIONS as usize && Instant::now() < deadline {
        consumer.poll(Duration::from_millis(50));
    }
    let mut tpl = TopicPartitionList::new();
    for p in 0..PARTITIONS {
        let (_, high) = consumer.fetch_watermarks("input", p, Duration::from_secs(5)).unwrap();
        tpl.add_partition_offset("input", p, Offset::Offset(high)).unwrap();
    }
    consumer.commit(&tpl, rdkafka::consumer::CommitMode::Sync).unwrap();
    consumer.unsubscribe();
}

fn rand_suffix() -> u128 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
}

fn config(bootstrap: &str) -> ServiceConfig {
    let mut c = ServiceConfig::new(bootstrap, "steps-service", "input", "output", "changelog");
    c.dead_letter_topic = Some("dead".into());
    c.workers = 2;
    c.poll_timeout = Duration::from_millis(20);
    c.request_timeout = Duration::from_secs(10);
    c.client_config =
        vec![("session.timeout.ms".into(), "6000".into()), ("heartbeat.interval.ms".into(), "200".into())];
    c
}

/// Emitted `at` states per key, in order.
fn states(records: &[Record]) -> HashMap<String, Vec<String>> {
    let mut by_key: HashMap<String, Vec<String>> = HashMap::new();
    for (key, payload, _) in records {
        let event: Value = serde_json::from_slice(payload.as_deref().unwrap()).unwrap();
        by_key.entry(key.clone().unwrap()).or_default().push(event["state"].as_str().unwrap().to_owned());
    }
    by_key
}

#[test]
fn exactly_once_actors_with_restore() {
    let cluster = MockCluster::new(3).unwrap();
    for topic in ["input", "output", "changelog", "dead"] {
        cluster.create_topic(topic, PARTITIONS, 3).unwrap();
    }
    let bootstrap = cluster.bootstrap_servers();

    produce(
        &bootstrap,
        &[
            (Some("a"), r#"{"type":"next"}"#),
            (Some("b"), r#"{"type":"next"}"#),
            (Some("c"), r#"{"type":"next"}"#),
            (Some("a"), r#"{"type":"next"}"#),
            (Some("b"), r#"{"type":"wait"}"#),
            (None, r#"{"type":"next"}"#),
            (Some("c"), "not json"),
        ],
    );
    let service = Service::start(config(&bootstrap), machine()).unwrap();
    // a: s1, s2; b: s1, then s2 when its 200 ms timer fires; c: s1.
    let output = read(&bootstrap, "output", 5);
    assert!(service.is_running());
    service.stop().unwrap();
    let by_key = states(&output);
    assert_eq!(by_key["a"], vec!["s1", "s2"]);
    assert_eq!(by_key["b"], vec!["s1", "s2"]);
    assert_eq!(by_key["c"], vec!["s1"]);
    assert_eq!(output.len(), 5, "{output:?}");

    let dead = read(&bootstrap, "dead", 2);
    assert_eq!(dead.len(), 2);
    let reasons: Vec<&str> = dead.iter().map(|d| d.2.as_deref().unwrap()).collect();
    assert!(reasons.iter().any(|r| r.contains("key")), "{reasons:?}");
    assert!(reasons.iter().any(|r| r.contains("not JSON")), "{reasons:?}");

    // librdkafka's mock cluster accepts `TxnOffsetCommit` but never stores
    // the offsets (rdkafka_mock_handlers.c), where a broker applies them when
    // the transaction commits. Do that step here: everything was processed,
    // so the group's position is the end of each partition.
    commit_processed(&bootstrap);

    // A new service instance continues from the changelog.
    produce(&bootstrap, &[(Some("a"), r#"{"type":"next"}"#), (Some("c"), r#"{"type":"next"}"#)]);
    let service = Service::start(config(&bootstrap), machine()).unwrap();
    let output = read(&bootstrap, "output", 7);
    service.stop().unwrap();
    let by_key = states(&output);
    assert_eq!(by_key["a"], vec!["s1", "s2", "s3"], "{output:?}");
    assert_eq!(by_key["c"], vec!["s1", "s2"]);
    assert_eq!(output.len(), 7, "no duplicates: {output:?}");

    // `a` finished: its latest changelog record is a tombstone.
    let changelog = read(&bootstrap, "changelog", 1);
    let last_a = changelog.iter().rev().find(|r| r.0.as_deref() == Some("a")).unwrap();
    assert!(last_a.1.is_none(), "tombstone for a");
    let last_c = changelog.iter().rev().find(|r| r.0.as_deref() == Some("c")).unwrap();
    let snapshot = xstate_kafka::Codec::decode(last_c.1.as_deref().unwrap()).unwrap();
    // Compact snapshots keep the active state ids (the value is derived).
    assert_eq!(snapshot["actors"]["steps"]["snapshot"]["nodeIds"], json!(["steps", "steps.s2"]));
}

#[test]
fn restores_from_local_checkpoints() {
    let cluster = MockCluster::new(1).unwrap();
    for topic in ["input", "output", "changelog", "dead"] {
        cluster.create_topic(topic, PARTITIONS, 1).unwrap();
    }
    let bootstrap = cluster.bootstrap_servers();
    let dir = std::env::temp_dir().join(format!("xstate-checkpoints-{}", rand_suffix()));
    let with_checkpoints = || {
        let mut c = config(&bootstrap);
        c.state_dir = Some(dir.clone());
        c.checkpoint_interval = Duration::ZERO;
        // One worker, so no rebalance restores a partition twice and the
        // restore counters are exact.
        c.workers = 1;
        c
    };
    let keys: Vec<String> = (0..30).map(|i| format!("k{i}")).collect();
    let next = r#"{"type":"next"}"#;

    // Rounds, so the last transaction (the tail a checkpoint does not
    // cover) is small.
    let service = Service::start(with_checkpoints(), machine()).unwrap();
    let round: Vec<(Option<&str>, &str)> = keys.iter().map(|k| (Some(k.as_str()), next)).collect();
    produce(&bootstrap, &round);
    assert_eq!(read(&bootstrap, "output", 30).len(), 30);
    produce(&bootstrap, &round);
    assert_eq!(read(&bootstrap, "output", 60).len(), 60);
    // k0 finishes (a tombstone).
    produce(&bootstrap, &[(Some("k0"), next)]);
    assert_eq!(read(&bootstrap, "output", 61).len(), 61);
    let first = service.metrics();
    service.stop().unwrap();
    assert!(first.checkpoints >= 2, "{first:?}");
    let files = std::fs::read_dir(&dir).unwrap().count();
    assert!(files >= 1, "checkpoint files in {}", dir.display());
    commit_processed(&bootstrap);

    // The workers checkpointed when they stopped: a restart restores
    // without reading the changelog.
    let service = Service::start(with_checkpoints(), machine()).unwrap();
    produce(&bootstrap, &[(Some("k1"), next)]);
    let output = read(&bootstrap, "output", 62);
    let second = service.metrics();
    service.stop().unwrap();
    assert_eq!(output.len(), 62, "{output:?}");
    assert_eq!(states(&output)["k1"], vec!["s1", "s2", "s3"]);
    assert_eq!(second.restored_partitions, PARTITIONS as u64);
    assert_eq!(second.restored_actors, 29, "{second:?}");
    assert_eq!(second.restored_records, 0, "{second:?}");
    commit_processed(&bootstrap);

    // Unreadable checkpoints fall back to the whole changelog.
    for entry in std::fs::read_dir(&dir).unwrap() {
        std::fs::write(entry.unwrap().path(), b"garbage").unwrap();
    }
    let service = Service::start(with_checkpoints(), machine()).unwrap();
    produce(&bootstrap, &[(Some("k2"), next)]);
    let output = read(&bootstrap, "output", 63);
    let third = service.metrics();
    service.stop().unwrap();
    assert_eq!(states(&output)["k2"], vec!["s1", "s2", "s3"]);
    assert_eq!(third.restored_actors, 28, "{third:?}");
    // 30 + 30 snapshots, 2 tombstones.
    assert!(third.restored_records >= 62, "{third:?}");
    std::fs::remove_dir_all(&dir).ok();
}
