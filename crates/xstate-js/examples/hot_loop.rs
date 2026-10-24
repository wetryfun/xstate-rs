//! A loop of JavaScript code-node evaluations, for profilers such as
//! `valgrind --tool=callgrind target/release/examples/hot_loop`.
use serde_json::{json, Value};
use std::sync::Arc;
use xstate_js::JsEvaluator;
use xstate_rt::{Actor, Machine, Registry};
fn main() {
    let mut r: Registry<Value, Value> = Registry::new();
    r.evaluator("js", JsEvaluator::new());
    let def = json!({ "@exprLang": "js", "context": { "n": 0 }, "initial": "a",
        "states": { "a": { "on": { "inc": { "guard": { "@expr": "context.n < 1e12" }, "context": { "n": { "@expr": "context.n + 1" } } } } } } });
    let mut actor = Actor::start(Arc::new(Machine::from_json(&def, &r).unwrap()), 0).unwrap();
    let event = json!({ "type": "inc" });
    for _ in 0..20_000 {
        actor.send(event.clone(), 0).unwrap();
    }
}
