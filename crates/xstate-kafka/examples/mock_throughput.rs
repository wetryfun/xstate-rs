//! End-to-end throughput on librdkafka's in-process mock cluster: events
//! produced to the input topic, processed by the service (one transaction
//! per batch: output, changelog, offsets), and read back committed from the
//! output topic.
//!
//!   cargo run --release -p xstate-kafka --features kafka --example mock_throughput -- \
//!       [events=200000] [keys=10000] [partitions=8] [workers=4] [max_batch=10000]
//!
//! The mock cluster has no network or disk, so this measures the client
//! side: the actors, encoding, and librdkafka's produce/consume/transaction
//! path. The single reader thread that counts committed output shares the
//! cores with the workers and is usually the bottleneck; run with
//! `XSTATE_KAFKA_DEBUG=1` to see each worker's batch and commit times.

use std::sync::Arc;
use std::time::{Duration, Instant};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::{Offset, TopicPartitionList};
use serde_json::{json, Value};
use xstate_kafka::service::{Service, ServiceConfig};
use xstate_rt::{Machine, Registry};

fn arg(i: usize, default: usize) -> usize {
    std::env::args().nth(i).and_then(|a| a.parse().ok()).unwrap_or(default)
}

fn main() {
    let (events, keys, partitions, workers, max_batch) =
        (arg(1, 200_000), arg(2, 10_000), arg(3, 8), arg(4, 4), arg(5, 10_000));
    let definition = json!({
        "id": "order",
        "initial": "open",
        "context": { "items": 0 },
        "states": {
            "open": {
                "on": { "add": { "context": { "items": 1 }, "actions": [{ "type": "@xstate.emit", "event": { "type": "added" } }] } }
            }
        }
    });
    let machine: Arc<Machine<Value, Value>> = Arc::new(Machine::from_json(&definition, &Registry::new()).unwrap());

    let cluster = MockCluster::new(1).unwrap();
    for topic in ["input", "output", "changelog"] {
        cluster.create_topic(topic, partitions as i32, 1).unwrap();
    }
    let bootstrap = cluster.bootstrap_servers();

    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("linger.ms", "5")
        .set("queue.buffering.max.messages", "1000000")
        .create()
        .unwrap();
    let keys: Vec<String> = (0..keys).map(|i| format!("order-{i}")).collect();
    let payload = r#"{"type":"add"}"#;
    let started = Instant::now();
    for i in 0..events {
        let record = BaseRecord::<str, str>::to("input").key(&keys[i % keys.len()]).payload(payload);
        if let Err((e, _)) = producer.send(record) {
            panic!("produce failed: {e}");
        }
        if i % 10_000 == 0 {
            producer.poll(Duration::ZERO);
        }
    }
    producer.flush(Duration::from_secs(60)).unwrap();
    eprintln!("produced {events} events in {:?}", started.elapsed());

    let mut config = ServiceConfig::new(&bootstrap, "bench", "input", "output", "changelog");
    config.workers = workers;
    config.max_batch = max_batch;
    config.poll_timeout = Duration::from_millis(10);
    let started = Instant::now();
    let service = Service::start(config, machine).unwrap();

    let reader: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("group.id", "bench-reader")
        .set("isolation.level", "read_committed")
        .create()
        .unwrap();
    let mut tpl = TopicPartitionList::new();
    for p in 0..partitions as i32 {
        tpl.add_partition_offset("output", p, Offset::Beginning).unwrap();
    }
    reader.assign(&tpl).unwrap();
    let mut seen = 0;
    let deadline = Instant::now() + Duration::from_secs(600);
    while seen < events && Instant::now() < deadline {
        if let Some(Ok(_)) = reader.poll(Duration::from_millis(100)) {
            seen += 1;
        }
    }
    let elapsed = started.elapsed();
    service.stop().unwrap();
    println!(
        "{seen} of {events} events over {} keys, {partitions} partitions, {workers} workers, batches of {max_batch}: {:.2?}, {:.0} events/s end to end",
        keys.len(),
        elapsed,
        seen as f64 / elapsed.as_secs_f64()
    );
}
