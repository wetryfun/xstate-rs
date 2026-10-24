//! What a JavaScript code node costs per event, next to the same machine
//! with Rust expressions: `cargo run --release -p xstate-js --example cost`.

use std::sync::Arc;
use std::time::Instant;

use serde_json::{json, Value};
use xstate_js::JsEvaluator;
use xstate_rt::{Actor, Machine, Registry};

fn machine(lang: &str, guard: &str, next: &str) -> Value {
    json!({
        "@exprLang": lang, "context": { "n": 0 }, "initial": "a",
        "states": { "a": { "on": { "inc": {
            "guard": { "@expr": guard },
            "context": { "n": { "@expr": next } }
        } } } }
    })
}

fn per_event(definition: Value, registry: &Registry<Value, Value>) -> f64 {
    let mut actor = Actor::start(Arc::new(Machine::from_json(&definition, registry).unwrap()), 0).unwrap();
    let event = json!({ "type": "inc" });
    for _ in 0..1000 {
        actor.send(event.clone(), 0).unwrap();
    }
    let n = 100_000;
    let start = Instant::now();
    for _ in 0..n {
        actor.send(event.clone(), 0).unwrap();
    }
    start.elapsed().as_secs_f64() * 1e6 / n as f64
}

fn main() {
    let mut r: Registry<Value, Value> = Registry::new();
    r.expr("small", |a| json!(a.context["n"].as_f64().unwrap_or(0.0) < 1e12))
        .expr("next", |a| json!(a.context["n"].as_i64().unwrap_or(0) + 1));
    r.evaluator("js", JsEvaluator::new());
    let rust = per_event(machine("rust", "small", "next"), &r);
    let js = per_event(machine("js", "context.n < 1e12", "context.n + 1"), &r);
    println!("two Rust expressions: {rust:.2} µs per event");
    println!("two JavaScript code nodes: {js:.2} µs per event ({:.2} µs per node more)", (js - rust) / 2.0);
    if std::env::var_os("BREAKDOWN").is_some() {
        let trivial = per_event(machine("js", "true", "1"), &r);
        println!("two trivial JavaScript nodes: {trivial:.2} µs per event");
        let code = per_event(
            json!({
                "@exprLang": "js", "context": { "n": 0 }, "initial": "a",
                "states": { "a": { "on": { "inc": {
                    "guard": { "@code": "({ context }) => context.n < 1e12" },
                    "context": { "n": { "@code": "({ context }) => context.n + 1" } }
                } } } }
            }),
            &r,
        );
        println!("as @code functions: {code:.2} µs per event");
    }
}
