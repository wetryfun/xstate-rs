//! A loop over the hot path, for profilers:
//!   cargo build --release -p xstate-rt --example hot_loop
//!   valgrind --tool=callgrind target/release/examples/hot_loop
use std::sync::Arc;

use serde_json::{json, Value};
use xstate_rt::{Actor, Machine, Registry};

fn main() {
    let definition = json!({
        "id": "toggle",
        "initial": "off",
        "states": {
            "off": { "on": { "flip": { "target": "on" } } },
            "on": { "on": { "flip": { "target": "off" } } }
        }
    });
    let n: usize = std::env::args().nth(1).and_then(|a| a.parse().ok()).unwrap_or(20_000);
    let machine: Machine<Value, Value> = Machine::from_json(&definition, &Registry::new()).unwrap();
    let mut actor = Actor::start(Arc::new(machine), 0).unwrap();
    let flip = json!({ "type": "flip" });
    let started = std::time::Instant::now();
    for _ in 0..n {
        actor.send_json(flip.clone(), 0).unwrap();
    }
    let per = started.elapsed() / n as u32;
    println!("{n} events, {per:?} per event, final {}", actor.value());
}
