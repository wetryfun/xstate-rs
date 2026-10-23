//! Hot-path benchmarks: events through `Actor::send`, timers through
//! `Actor::advance`, and persist/restore.
//!
//!   cargo bench -p xstate-rt --bench interpreter

use std::hint::black_box;
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use serde_json::{json, Value};
use xstate_rt::{Actor, Machine, Registry};

fn machine(definition: Value, registry: &Registry<Value, Value>) -> Arc<Machine<Value, Value>> {
    Arc::new(Machine::from_json(&definition, registry).unwrap())
}

fn toggle() -> Arc<Machine<Value, Value>> {
    machine(
        json!({
            "id": "toggle",
            "initial": "off",
            "states": {
                "off": { "on": { "flip": { "target": "on" } } },
                "on": { "on": { "flip": { "target": "off" } } }
            }
        }),
        &Registry::new(),
    )
}

/// Parallel regions, guards, context patches, entry/exit actions and a
/// custom action: a step that does a bit of everything.
fn busy() -> Arc<Machine<Value, Value>> {
    let mut r: Registry<Value, Value> = Registry::new();
    r.guard_with("below", |args, max: &u64| args.context["n"].as_u64().unwrap_or(0) < *max).action("track", || {});
    machine(
        json!({
            "id": "busy",
            "type": "parallel",
            "context": { "n": 0, "label": "x" },
            "states": {
                "left": {
                    "initial": "a",
                    "states": {
                        "a": {
                            "entry": [{ "type": "track" }],
                            "on": { "tick": { "target": "b", "guard": { "type": "below", "params": 1000000 }, "context": { "n": 1 } } }
                        },
                        "b": {
                            "exit": [{ "type": "@xstate.assign", "context": { "label": "y" } }],
                            "on": { "tick": { "target": "a" } }
                        }
                    }
                },
                "right": {
                    "initial": "idle",
                    "states": {
                        "idle": { "on": { "tick": { "target": "working" } } },
                        "working": { "on": { "tick": { "target": "idle", "actions": [{ "type": "@xstate.emit", "event": { "type": "done" } }] } } }
                    }
                }
            }
        }),
        &r,
    )
}

fn timers() -> Arc<Machine<Value, Value>> {
    machine(
        json!({
            "id": "timers",
            "initial": "a",
            "states": {
                "a": { "after": { "10": { "target": "b" } } },
                "b": { "after": { "10": { "target": "a" } } }
            }
        }),
        &Registry::new(),
    )
}

fn bench(c: &mut Criterion) {
    let mut g = c.benchmark_group("send");
    g.throughput(Throughput::Elements(1));
    let m = toggle();
    let mut actor = Actor::start(m.clone(), 0).unwrap();
    let flip = json!({ "type": "flip" });
    g.bench_function("toggle", |b| b.iter(|| actor.send_json(black_box(flip.clone()), 0).unwrap()));

    let m = busy();
    let mut actor = Actor::start(m.clone(), 0).unwrap();
    let tick = json!({ "type": "tick" });
    g.bench_function("parallel_guards_actions", |b| {
        b.iter(|| {
            actor.send_json(black_box(tick.clone()), 0).unwrap();
            actor.take_emitted();
        })
    });
    let unhandled = json!({ "type": "nothing" });
    g.bench_function("unhandled_event", |b| b.iter(|| actor.send_json(black_box(unhandled.clone()), 0).unwrap()));
    g.finish();

    let mut g = c.benchmark_group("timers");
    g.throughput(Throughput::Elements(1));
    let m = timers();
    let mut actor = Actor::start(m.clone(), 0).unwrap();
    let mut now = 0;
    g.bench_function("after_fires", |b| {
        b.iter(|| {
            now += 10;
            actor.advance(now).unwrap();
        })
    });
    g.finish();

    let mut g = c.benchmark_group("lifecycle");
    let m = busy();
    g.bench_function("start", |b| b.iter(|| Actor::start(black_box(m.clone()), 0).unwrap()));
    let mut actor = Actor::start(m.clone(), 0).unwrap();
    actor.send_json(json!({ "type": "tick" }), 0).unwrap();
    g.bench_function("persist", |b| b.iter(|| black_box(actor.persist())));
    let saved = actor.persist();
    g.bench_function("restore", |b| {
        b.iter_batched(|| saved.clone(), |s| Actor::restore(m.clone(), &s).unwrap(), BatchSize::SmallInput)
    });
    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
