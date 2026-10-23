//! A worker for the service's external work: answers `fetchUser` requests
//! (the input's `user` echoed back as a name) and fails anything else.
//! Replace the handler with real work.
//!
//!   cargo run --release -p xstate-kafka --features kafka --example effects_worker -- \
//!       localhost:9092 orders-effects orders-effects orders

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde_json::json;
use xstate_kafka::effects::{run, Outcome, WorkerConfig};

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [bootstrap, group, effects_topic, input_topic] = args.as_slice() else {
        eprintln!("usage: effects_worker <bootstrap> <group> <effects topic> <input topic>");
        std::process::exit(2);
    };
    let config = WorkerConfig::new(bootstrap, group, effects_topic, input_topic);
    let stop = Arc::new(AtomicBool::new(false));
    let flag = stop.clone();
    ctrlc::set_handler(move || flag.store(true, Ordering::Relaxed)).expect("signal handler");
    let result = run(&config, &stop, |work| match (work.logic.as_deref(), &work.input) {
        (Some("fetchUser"), Some(input)) => Outcome::Resolve(json!({ "name": format!("user {}", input["user"]) })),
        (logic, _) => Outcome::Reject(json!({ "message": format!("no handler for {logic:?}") })),
    });
    if let Err(e) = result {
        eprintln!("effects_worker: {e}");
        std::process::exit(1);
    }
}
