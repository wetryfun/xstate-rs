//! A loop of `Actor::persist`, for profilers:
//!   cargo build --release -p xstate-rt --example persist_loop
//!   valgrind --tool=callgrind target/release/examples/persist_loop
use std::sync::Arc;

use serde_json::{json, Value};
use xstate_rt::{Actor, Machine, Registry};

fn main() {
    let definition = json!({
        "id": "order",
        "initial": "open",
        "context": { "items": 0, "customer": "c-1", "tags": ["web", "eu"] },
        "states": {
            "open": {
                "on": { "add": { "context": { "items": 1 } }, "pay": { "target": "paying" } },
                "after": { "86400000": { "target": "expired" } }
            },
            "paying": { "on": { "ok": { "target": "paid" } } },
            "paid": { "type": "final" },
            "expired": { "type": "final" }
        }
    });
    let n: u32 = std::env::args().nth(1).and_then(|a| a.parse().ok()).unwrap_or(20_000);
    let machine: Machine<Value, Value> = Machine::from_json(&definition, &Registry::new()).unwrap();
    let mut actor = Actor::start(Arc::new(machine), 0).unwrap();
    actor.send_json(json!({ "type": "add" }), 1).unwrap();
    let started = std::time::Instant::now();
    let mut size = 0;
    for _ in 0..n {
        size += actor.persist().as_object().map_or(0, |o| o.len());
    }
    println!("{n} persists, {:?} each ({size})", started.elapsed() / n);
}
