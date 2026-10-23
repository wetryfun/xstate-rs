//! The service against librdkafka's in-process mock cluster (transactions,
//! consumer groups): output, timers, dead letters, and a restart that
//! restores actors from the changelog without duplicating output.
#![cfg(feature = "kafka")]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Header, Headers, Message, OwnedHeaders};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::{Offset, TopicPartitionList};
use serde_json::{json, Value};
use xstate_kafka::service::{Service, ServiceConfig};
use xstate_kafka::{Dedupe, MachineSet};
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

/// [`machine`] with a `version`.
fn versioned(version: &str) -> Arc<Machine<Value, Value>> {
    let at = |s: &str| json!([{ "type": "@xstate.emit", "event": { "type": "at", "state": s } }]);
    let definition = json!({
        "id": "steps",
        "version": version,
        "initial": "s0",
        "states": {
            "s0": { "on": { "next": { "target": "s1" } } },
            "s1": { "entry": at("s1"), "on": { "next": { "target": "s2" } } },
            "s2": { "entry": at("s2") }
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

/// Produces one keyed input record with a `content-type` header.
fn produce_typed(bootstrap: &str, key: &str, payload: &[u8], content_type: &str) {
    let producer: BaseProducer = ClientConfig::new().set("bootstrap.servers", bootstrap).create().unwrap();
    let headers = OwnedHeaders::new().insert(Header { key: "Content-Type", value: Some(content_type) });
    producer.send(BaseRecord::<str, [u8]>::to("input").key(key).payload(payload).headers(headers)).unwrap();
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
            (Some("e"), r#"{"type":"next","seq":1}"#),
            (Some("e"), r#"{"type":"next","seq":1}"#),
        ],
    );
    // The `content-type` header selects the event format.
    produce_typed(
        &bootstrap,
        "d",
        &rmp_serde::to_vec_named(&json!({ "type": "next" })).unwrap(),
        "application/msgpack",
    );
    produce_typed(&bootstrap, "d", b"next", "text/plain");
    let deduping = || {
        let mut c = config(&bootstrap);
        c.engine.dedupe = Some(Dedupe { sequence: "seq".into(), epoch: None, retain_ms: None });
        c
    };
    let service = Service::start(deduping(), machine()).unwrap();
    // a: s1, s2; b: s1, then s2 when its 200 ms timer fires; c: s1; d: s1;
    // e: s1 (its second record is a duplicate).
    let output = read(&bootstrap, "output", 7);
    assert!(service.is_running());
    service.stop().unwrap();
    let by_key = states(&output);
    assert_eq!(by_key["a"], vec!["s1", "s2"]);
    assert_eq!(by_key["b"], vec!["s1", "s2"]);
    assert_eq!(by_key["c"], vec!["s1"]);
    assert_eq!(by_key["d"], vec!["s1"]);
    assert_eq!(by_key["e"], vec!["s1"]);
    assert_eq!(output.len(), 7, "{output:?}");

    let dead = read(&bootstrap, "dead", 3);
    assert_eq!(dead.len(), 3);
    let reasons: Vec<&str> = dead.iter().map(|d| d.2.as_deref().unwrap()).collect();
    assert!(reasons.iter().any(|r| r.contains("key")), "{reasons:?}");
    assert!(reasons.iter().any(|r| r.contains("not JSON")), "{reasons:?}");
    assert!(reasons.iter().any(|r| r.contains("content-type")), "{reasons:?}");

    // librdkafka's mock cluster accepts `TxnOffsetCommit` but never stores
    // the offsets (rdkafka_mock_handlers.c), where a broker applies them when
    // the transaction commits. Do that step here: everything was processed,
    // so the group's position is the end of each partition.
    commit_processed(&bootstrap);

    // A new service instance continues from the changelog.
    // e's mark was restored with it: seq 1 again is a duplicate.
    produce(
        &bootstrap,
        &[
            (Some("a"), r#"{"type":"next"}"#),
            (Some("c"), r#"{"type":"next"}"#),
            (Some("e"), r#"{"type":"next","seq":1}"#),
            (Some("e"), r#"{"type":"next","seq":2}"#),
        ],
    );
    let service = Service::start(deduping(), machine()).unwrap();
    let output = read(&bootstrap, "output", 10);
    service.stop().unwrap();
    let by_key = states(&output);
    assert_eq!(by_key["a"], vec!["s1", "s2", "s3"], "{output:?}");
    assert_eq!(by_key["c"], vec!["s1", "s2"]);
    assert_eq!(by_key["e"], vec!["s1", "s2"]);
    assert_eq!(output.len(), 10, "no duplicates: {output:?}");

    // `a` finished: its latest changelog record is a tombstone.
    let changelog = read(&bootstrap, "changelog", 1);
    let last_a = changelog.iter().rev().find(|r| r.0.as_deref() == Some("a")).unwrap();
    assert!(last_a.1.is_none(), "tombstone for a");
    let last_c = changelog.iter().rev().find(|r| r.0.as_deref() == Some("c")).unwrap();
    let snapshot = xstate_kafka::Codec::decode(last_c.1.as_deref().unwrap()).unwrap();
    assert_eq!(snapshot["actors"]["steps"]["snapshot"]["value"], json!("s2"));
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

#[test]
fn a_failed_commit_discards_the_batch_behind_it() {
    use rdkafka::types::{RDKafkaApiKey, RDKafkaRespErr};
    let cluster = MockCluster::new(1).unwrap();
    for topic in ["input", "output", "changelog", "dead"] {
        cluster.create_topic(topic, PARTITIONS, 1).unwrap();
    }
    let bootstrap = cluster.bootstrap_servers();
    let keys: Vec<String> = (0..200).map(|i| format!("k{i}")).collect();
    let next = r#"{"type":"next"}"#;
    produce(&bootstrap, &keys.iter().map(|k| (Some(k.as_str()), next)).collect::<Vec<_>>());
    // The first transaction fails when it commits its offsets (abortably),
    // while the next batch is already being processed behind it. The mock
    // never stores transactional offsets, so this only works for the first
    // transaction: the rewind goes back to the beginning.
    cluster.request_errors(
        RDKafkaApiKey::TxnOffsetCommit,
        &[RDKafkaRespErr::RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED],
    );
    let mut c = config(&bootstrap);
    c.workers = 1;
    c.max_batch = 50;
    let service = Service::start(c, machine()).unwrap();
    let output = read(&bootstrap, "output", 200);
    let m = service.metrics();
    service.stop().unwrap();
    assert_eq!(m.aborted_transactions, 1, "{m:?}");
    assert_eq!(m.records, 200, "{m:?}");
    // Every key reached s1 exactly once: nothing of the aborted transaction
    // or of the discarded batch behind it is visible or applied twice.
    assert_eq!(output.len(), 200, "{output:?}");
    let by_key = states(&output);
    assert_eq!(by_key.len(), 200);
    assert!(by_key.values().all(|s| s == &vec!["s1".to_owned()]), "{by_key:?}");
}

#[test]
fn a_record_of_an_unknown_version_is_a_dead_letter_not_a_crash() {
    let cluster = MockCluster::new(3).unwrap();
    for topic in ["input", "output", "changelog", "dead"] {
        cluster.create_topic(topic, PARTITIONS, 3).unwrap();
    }
    let bootstrap = cluster.bootstrap_servers();
    produce(&bootstrap, &[(Some("a"), r#"{"type":"next"}"#), (Some("b"), r#"{"type":"next"}"#)]);
    let service = Service::start(config(&bootstrap), versioned("1")).unwrap();
    assert_eq!(read(&bootstrap, "output", 2).len(), 2);
    service.stop().unwrap();
    commit_processed(&bootstrap);

    // Version 2 alone does not know version 1's actors: they are dead
    // letters and their keys start over; the workers keep running.
    produce(&bootstrap, &[(Some("a"), r#"{"type":"next"}"#)]);
    let service = Service::start(config(&bootstrap), versioned("2")).unwrap();
    let dead = read(&bootstrap, "dead", 2);
    let output = read(&bootstrap, "output", 3);
    assert!(service.is_running());
    service.stop().unwrap();
    assert_eq!(dead.len(), 2, "{dead:?}");
    assert!(dead.iter().all(|d| d.2.as_deref().unwrap().contains("no machine for version \"1\"")), "{dead:?}");
    // `a` started over on version 2: s1 again, not s2.
    assert_eq!(states(&output)["a"], vec!["s1", "s1"]);

    // With version 1 kept, a restart restores nothing broken: the keys
    // were tombstoned, and `a` runs on version 2.
    commit_processed(&bootstrap);
    let set = MachineSet::new(versioned("2")).keep(versioned("1")).unwrap();
    produce(&bootstrap, &[(Some("a"), r#"{"type":"next"}"#)]);
    let service = Service::start(config(&bootstrap), set).unwrap();
    let output = read(&bootstrap, "output", 4);
    service.stop().unwrap();
    assert_eq!(states(&output)["a"], vec!["s1", "s1", "s2"]);
    assert_eq!(read(&bootstrap, "dead", 2).len(), 2, "no new dead letters");
}

/// TLS, SASL and compression are built in: a client configured for SCRAM
/// over TLS with zstd can be created (no connection is made).
#[test]
fn clients_support_tls_sasl_and_compression() {
    let native = ClientConfig::new().create_native_config().unwrap();
    let features = native.get("builtin.features").unwrap();
    for feature in ["ssl", "sasl_plain", "sasl_scram", "sasl_oauthbearer", "zstd", "gzip", "lz4", "snappy"] {
        assert!(features.split(',').any(|f| f == feature), "{feature} missing from {features}");
    }
    let producer: Result<BaseProducer, _> = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9093")
        .set("security.protocol", "SASL_SSL")
        .set("sasl.mechanisms", "SCRAM-SHA-512")
        .set("sasl.username", "user")
        .set("sasl.password", "secret")
        .set("compression.type", "zstd")
        .create();
    assert!(producer.is_ok(), "{:?}", producer.err());
}

/// Plain HTTP GET of `path`: the status code and the body.
fn http_get(address: std::net::SocketAddr, path: &str) -> (u16, String) {
    use std::io::{Read, Write};
    let mut stream = std::net::TcpStream::connect(address).unwrap();
    write!(stream, "GET {path} HTTP/1.1\r\nHost: x\r\n\r\n").unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let status = response.split_whitespace().nth(1).unwrap().parse().unwrap();
    (status, response.split("\r\n\r\n").nth(1).unwrap_or("").to_owned())
}

/// A second instance joins while records flow: partitions move to it
/// cooperatively, and every key still ends exactly once in its final state.
/// Readiness, liveness and lag are served over HTTP.
#[test]
fn partitions_move_cooperatively_and_probes_report_it() {
    let cluster = MockCluster::new(3).unwrap();
    for topic in ["input", "output", "changelog", "dead"] {
        cluster.create_topic(topic, PARTITIONS, 3).unwrap();
    }
    let bootstrap = cluster.bootstrap_servers();
    let mut first_config = config(&bootstrap);
    first_config.stats_interval = Duration::from_millis(200);
    let first = Service::start(first_config, machine()).unwrap();
    let (address, _) = xstate_kafka::metrics::serve("127.0.0.1:0", first.metrics_handle()).unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    while http_get(address, "/ready").0 != 200 {
        assert!(Instant::now() < deadline, "never ready");
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(http_get(address, "/health").0, 200);

    let keys: Vec<String> = (0..40).map(|i| format!("k{i}")).collect();
    let next: Vec<(Option<&str>, &str)> = keys.iter().map(|k| (Some(k.as_str()), r#"{"type":"next"}"#)).collect();
    produce(&bootstrap, &next);
    assert_eq!(read(&bootstrap, "output", 40).len(), 40);

    // A second instance joins: some partitions move to it.
    let second = Service::start(config(&bootstrap), machine()).unwrap();
    produce(&bootstrap, &next);
    produce(&bootstrap, &next);
    let output = read(&bootstrap, "output", 120);
    let (_, metrics) = http_get(address, "/metrics");
    assert!(first.is_running() && second.is_running());
    first.stop().unwrap();
    second.stop().unwrap();

    let by_key = states(&output);
    for k in &keys {
        assert_eq!(by_key[k.as_str()], vec!["s1", "s2", "s3"], "key {k}: {output:?}");
    }
    assert_eq!(output.len(), 120, "no duplicates");
    assert!(read(&bootstrap, "dead", 0).is_empty());
    assert!(metrics.contains("xstate_consumer_lag{partition="), "{metrics}");
    assert_eq!(http_get(address, "/health").0, 503, "stopped");
}

/// Invoked async work goes out on the effects topic, a worker does it, and
/// its result brings the actor to its final state.
#[test]
fn an_effects_worker_completes_invoked_work() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use xstate_kafka::effects::{self, Outcome, WorkerConfig};

    let cluster = MockCluster::new(3).unwrap();
    for topic in ["input", "output", "changelog", "dead", "effects"] {
        cluster.create_topic(topic, PARTITIONS, 3).unwrap();
    }
    let bootstrap = cluster.bootstrap_servers();
    let definition = json!({
        "id": "lookup", "initial": "fetching",
        "states": {
            "fetching": {
                "invoke": {
                    "src": "fetchUser", "input": { "user": 7 },
                    "onDone": { "target": "found", "actions": [{ "type": "@xstate.emit", "event": { "type": "at", "state": "found" } }] },
                    "onError": { "target": "failed", "actions": [{ "type": "@xstate.emit", "event": { "type": "at", "state": "failed" } }] }
                }
            },
            "found": { "type": "final" },
            "failed": { "type": "final" }
        }
    });
    let mut registry = Registry::<Value, Value>::new();
    registry.actor("fetchUser", Arc::new(xstate_rt::step::async_logic(Some("fetchUser".into()), None).unwrap()));
    let machine = Arc::new(Machine::from_json(&definition, &registry).unwrap());
    let mut service_config = config(&bootstrap);
    service_config.effects_topic = Some("effects".into());
    let service = Service::start(service_config, machine).unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let worker = {
        let (stop, bootstrap) = (stop.clone(), bootstrap.clone());
        std::thread::spawn(move || {
            let config = WorkerConfig::new(bootstrap, "workers", "effects", "input");
            effects::run(&config, &stop, |work| match (work.logic.as_deref(), &work.input) {
                // `bad` keys fail.
                (Some("fetchUser"), Some(input)) if !work.key.starts_with("bad") => Outcome::Resolve(input.clone()),
                _ => Outcome::Reject(json!({ "message": "no" })),
            })
        })
    };
    produce(
        &bootstrap,
        &[(Some("u1"), r#"{"type":"go"}"#), (Some("u2"), r#"{"type":"go"}"#), (Some("bad1"), r#"{"type":"go"}"#)],
    );
    let output = read(&bootstrap, "output", 3);
    stop.store(true, Ordering::Relaxed);
    worker.join().unwrap().unwrap();
    service.stop().unwrap();
    let by_key = states(&output);
    assert_eq!(by_key["u1"], vec!["found"]);
    assert_eq!(by_key["u2"], vec!["found"]);
    assert_eq!(by_key["bad1"], vec!["failed"]);
    assert!(read(&bootstrap, "dead", 0).is_empty());
}
