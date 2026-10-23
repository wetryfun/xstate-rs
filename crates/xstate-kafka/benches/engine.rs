//! Engine benchmarks: snapshot codecs, and records through a partition
//! (JSON parsing, the step, emitted events, and the changelog snapshots of
//! one flush per batch).
//!
//!   cargo bench -p xstate-kafka --bench engine

use std::hint::black_box;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use serde_json::{json, Value};
use xstate_kafka::avro::{registry::MemoryRegistry, AvroSchemas};
use xstate_kafka::{Codec, EngineConfig, Partition};
use xstate_rt::{Actor, Machine, Registry};

fn machine() -> Arc<Machine<Value, Value>> {
    let definition = json!({
        "id": "order",
        "initial": "open",
        "context": { "items": 0, "customer": "c-1", "tags": ["web", "eu"] },
        "states": {
            "open": {
                "on": {
                    "add": { "context": { "items": 1 }, "actions": [{ "type": "@xstate.emit", "event": { "type": "added" } }] },
                    "pay": { "target": "paying" }
                },
                "after": { "86400000": { "target": "expired" } }
            },
            "paying": { "on": { "ok": { "target": "paid" }, "add": { "target": "open" } } },
            "paid": { "type": "final" },
            "expired": { "type": "final" }
        }
    });
    Arc::new(Machine::from_json(&definition, &Registry::new()).unwrap())
}

fn avro_schemas(bundle: &Value) -> Arc<AvroSchemas> {
    let mut s = AvroSchemas::from_bundle(bundle, Arc::new(MemoryRegistry::default())).unwrap();
    s.register([Some("in-value"), Some("out-value"), Some("changelog-value")], true).unwrap();
    Arc::new(s)
}

fn bench(c: &mut Criterion) {
    let m = machine();
    let mut actor = Actor::start(m.clone(), 0).unwrap();
    actor.send_json(json!({ "type": "add" }), 1).unwrap();
    let snapshot = actor.persist();

    let mut g = c.benchmark_group("codec");
    for codec in [Codec::Json, Codec::MessagePack, Codec::Protobuf] {
        let encoded = codec.encode(&snapshot);
        eprintln!("{codec:?}: {} bytes per snapshot", encoded.len());
        g.bench_function(format!("{codec:?}/encode"), |b| b.iter(|| codec.encode(black_box(&snapshot))));
        g.bench_function(format!("{codec:?}/decode"), |b| b.iter(|| Codec::decode(black_box(&encoded)).unwrap()));
    }
    g.bench_function("persist", |b| b.iter(|| black_box(actor.persist())));
    for codec in [Codec::Json, Codec::MessagePack] {
        g.bench_function(format!("{codec:?}/persist_view_encode"), |b| {
            b.iter(|| codec.encode_serialize(black_box(&actor.persist_view())))
        });
    }
    for codec in [Codec::MessagePack, Codec::Protobuf] {
        let encoded = codec.encode_serialize(&actor.persist_compact());
        eprintln!("{codec:?}, compact: {} bytes per snapshot", encoded.len());
        g.bench_function(format!("{codec:?}/compact_encode"), |b| {
            b.iter(|| codec.encode_serialize(black_box(&actor.persist_compact())))
        });
        g.bench_function(format!("{codec:?}/compact_decode"), |b| {
            b.iter(|| Codec::decode(black_box(&encoded)).unwrap())
        });
    }
    // Avro (Schema Registry wire format), the snapshot through `Value`.
    let avro = avro_schemas(&json!({ "machine": { "id": "order" }, "schemas": {} }));
    let mut encoded = Vec::new();
    avro.encode_snapshot(&snapshot, &mut encoded).unwrap();
    eprintln!("Avro: {} bytes per snapshot", encoded.len());
    g.bench_function("Avro/encode", |b| {
        b.iter(|| {
            let mut out = Vec::with_capacity(512);
            avro.encode_snapshot(black_box(&snapshot), &mut out).unwrap();
            out
        })
    });
    g.bench_function("Avro/decode", |b| b.iter(|| avro.decode_snapshot(black_box(&encoded)).unwrap()));
    g.finish();

    // One input event (the checkout example's `cart.add`) decoded from each
    // format into the JSON event the engine runs.
    let bundle: Value = serde_json::from_str(
        &std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../../examples/checkout/machines/checkout.xstate.json"
        ))
        .unwrap(),
    )
    .unwrap();
    let event = json!({ "type": "cart.add", "line": { "sku": "A-1", "quantity": 3 } });
    let proto = xstate_kafka::proto::events::EventSchema::from_bundle(&bundle).unwrap();
    let avro_events = avro_schemas(&bundle);
    let records = [
        ("Json", serde_json::to_vec(&event).unwrap()),
        ("MessagePack", rmp_serde::to_vec_named(&event).unwrap()),
        ("Protobuf", proto.encode(&event).unwrap()),
        ("Avro", avro_events.encode_event(&event).unwrap()),
    ];
    let mut g = c.benchmark_group("events");
    for (name, record) in &records {
        eprintln!("{name}: {} bytes per event", record.len());
        let decode: Box<dyn Fn(&[u8]) -> Value> = match *name {
            "Json" => Box::new(|b| serde_json::from_slice(b).unwrap()),
            "MessagePack" => Box::new(|b| rmp_serde::from_slice(b).unwrap()),
            "Protobuf" => Box::new(|b| proto.decode(b).unwrap()),
            _ => Box::new(|b| avro_events.decode_event(b).unwrap()),
        };
        assert_eq!(decode(record), event);
        g.bench_function(format!("{name}/decode"), |b| b.iter(|| decode(black_box(record))));
    }
    g.finish();

    // A batch of records over `keys` warm actors, then one flush (one
    // snapshot per key). Few keys stay in cache; many keys (one event per
    // actor per batch) measure the cost of touching cold actors.
    for (records, keys) in [(1000usize, 100usize), (10_000, 10_000)] {
        let mut g = c.benchmark_group("partition");
        g.throughput(Throughput::Elements(records as u64));
        let batch: Vec<(String, Vec<u8>)> = (0..records)
            .map(|i| {
                let event = if i % 2 == 0 { r#"{"type":"add"}"# } else { r#"{"type":"pay"}"# };
                (format!("order-{}", i % keys), event.as_bytes().to_vec())
            })
            .collect();
        for codec in [Codec::Json, Codec::MessagePack, Codec::Protobuf, Codec::Avro] {
            let avro = (codec == Codec::Avro).then(|| avro.clone());
            g.bench_function(format!("batch_{records}_records_{keys}_keys/{codec:?}"), |b| {
                b.iter_batched(
                    || {
                        let config =
                            EngineConfig { codec, evict_final: true, avro: avro.clone(), ..Default::default() };
                        let mut p = Partition::new(m.clone(), config);
                        // Warm: every key already has an actor.
                        for (key, payload) in batch.iter().take(keys) {
                            p.handle(Some(key), payload, 0);
                        }
                        p.flush();
                        p
                    },
                    |mut p| {
                        for (i, (key, payload)) in batch.iter().enumerate() {
                            p.handle(Some(key), payload, i as u64);
                        }
                        let out = p.flush();
                        // Returned, so dropping the actors is not timed.
                        (black_box(out), p)
                    },
                    BatchSize::LargeInput,
                )
            });
        }
        g.finish();
    }
}

criterion_group!(benches, bench);
criterion_main!(benches);
