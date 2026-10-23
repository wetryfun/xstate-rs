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

fn bench(c: &mut Criterion) {
    let m = machine();
    let mut actor = Actor::start(m.clone(), 0).unwrap();
    actor.send_json(json!({ "type": "add" }), 1).unwrap();
    let snapshot = actor.persist();

    let mut g = c.benchmark_group("codec");
    for codec in [Codec::Json, Codec::MessagePack] {
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
        for codec in [Codec::Json, Codec::MessagePack] {
            g.bench_function(format!("batch_{records}_records_{keys}_keys/{codec:?}"), |b| {
                b.iter_batched(
                    || {
                        let mut p =
                            Partition::new(m.clone(), EngineConfig { codec, evict_final: true, ..Default::default() });
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
