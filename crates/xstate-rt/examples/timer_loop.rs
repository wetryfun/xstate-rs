//! A loop of `after` timer firings, for profilers:
//!   cargo build --release -p xstate-rt --example timer_loop
//!   valgrind --tool=callgrind target/release/examples/timer_loop
use std::sync::Arc;

use serde_json::{json, Value};
use xstate_rt::{Actor, Machine, Registry};

fn main() {
    let definition = json!({
        "id": "timers",
        "initial": "a",
        "states": {
            "a": { "after": { "10": { "target": "b" } } },
            "b": { "after": { "10": { "target": "a" } } }
        }
    });
    let n: u64 = std::env::args().nth(1).and_then(|a| a.parse().ok()).unwrap_or(20_000);
    let machine: Machine<Value, Value> = Machine::from_json(&definition, &Registry::new()).unwrap();
    let mut actor = Actor::start(Arc::new(machine), 0).unwrap();
    let started = std::time::Instant::now();
    for i in 1..=n {
        actor.advance(i * 10).unwrap();
    }
    println!("{n} timers, {:?} per timer, final {}", started.elapsed() / n as u32, actor.value());
}
