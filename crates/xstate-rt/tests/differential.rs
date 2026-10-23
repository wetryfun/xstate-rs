//! Replays every fixture in `fixtures/` and compares against the trace XState
//! produced (`<name>.expected.json`, written by `tools/differential`).

use std::path::PathBuf;
use std::sync::Arc;

use serde_json::Value;
use xstate_rt::{Actor, Machine, Registry, Status};

fn as_f64(v: Option<&Value>) -> Option<f64> {
    v.and_then(Value::as_f64)
}

/// Rust equivalents of the stubs in `tools/differential/generate.mjs`.
fn stubs() -> Registry<Value, Value> {
    #[derive(serde::Deserialize)]
    struct KeyValue {
        key: String,
        value: Value,
    }
    let mut r: Registry<Value, Value> = Registry::new();
    r.guard("yes", |_| true)
        .guard("no", |_| false)
        .guard_with("ctxEquals", |args, p: &KeyValue| args.context.get(&p.key) == Some(&p.value))
        .guard_with("ctxAtLeast", |args, p: &KeyValue| {
            matches!((as_f64(args.context.get(&p.key)), p.value.as_f64()), (Some(a), Some(b)) if a >= b)
        })
        .guard_with("eventEquals", |args, p: &KeyValue| args.event.field(&p.key).as_ref() == Some(&p.value))
        .action("noop", |_| {})
        .delay_ms("short", 100)
        .delay_ms("long", 1000);
    r
}

fn status_str(s: Status) -> &'static str {
    match s {
        Status::Active => "active",
        Status::Done => "done",
        Status::Error => "error",
        Status::Stopped => "stopped",
    }
}

fn record(actor: &mut Actor<Value, Value>, label: &str) -> Value {
    let mut entry = serde_json::json!({
        "label": label,
        "value": actor.value(),
        "context": actor.context().clone(),
        "status": status_str(actor.status()),
        "emitted": actor.take_emitted(),
    });
    if let Some(output) = actor.output() {
        entry["output"] = output.clone();
    }
    entry
}

fn run_fixture(path: &PathBuf) -> Result<(), String> {
    let fixture: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let expected_path = path.with_extension("expected.json");
    let expected: Vec<Value> = serde_json::from_str(
        &std::fs::read_to_string(&expected_path).map_err(|e| format!("{}: {e}", expected_path.display()))?,
    )
    .unwrap();

    let machine = Machine::from_json(&fixture["machine"], &stubs()).map_err(|e| e.to_string())?;
    let mut actor = Actor::start(Arc::new(machine), 0).map_err(|e| e.to_string())?;
    let mut actual = vec![record(&mut actor, "start")];
    for (i, step) in fixture["steps"].as_array().unwrap().iter().enumerate() {
        let at = step["at"].as_u64().unwrap();
        actor.advance(at).map_err(|e| e.to_string())?;
        if let Some(event) = step.get("send") {
            actor.send(event.clone(), at).map_err(|e| e.to_string())?;
        }
        actual.push(record(&mut actor, &format!("step {i}")));
    }

    for (exp, act) in expected.iter().zip(&actual) {
        let mut exp = exp.clone();
        let mut act = act.clone();
        // A machine without context has `undefined` context in XState.
        if exp["context"].is_null() {
            exp["context"] = Value::Null;
            act["context"] = Value::Null;
        }
        if exp != act {
            return Err(format!(
                "mismatch at {}\n  expected: {}\n  actual:   {}",
                exp["label"],
                serde_json::to_string(&exp).unwrap(),
                serde_json::to_string(&act).unwrap()
            ));
        }
    }
    if expected.len() != actual.len() {
        return Err(format!("expected {} records, got {}", expected.len(), actual.len()));
    }
    Ok(())
}

#[test]
fn fixtures_match_xstate() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures");
    let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            let name = p.file_name().unwrap().to_str().unwrap();
            name.ends_with(".json") && !name.ends_with(".expected.json")
        })
        .collect();
    paths.sort();
    assert!(!paths.is_empty(), "no fixtures found in {}", dir.display());
    let failures: Vec<String> = paths
        .iter()
        .filter_map(|p| run_fixture(p).err().map(|e| format!("{}: {e}", p.file_name().unwrap().to_str().unwrap())))
        .collect();
    assert!(
        failures.is_empty(),
        "{} of {} fixtures differ from XState:\n{}",
        failures.len(),
        paths.len(),
        failures.join("\n")
    );
}
