//! Replays every fixture in `fixtures/` and compares against the trace XState
//! produced (`<name>.expected.json`, written by `tools/differential`), and
//! the generated corpus in `fixtures/fuzz/` (written by `fuzz.mjs`).

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
        .action("noop", || {})
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
    let viewed = serde_json::to_string(&actor.persist_view()).unwrap();
    assert_eq!(viewed, actor.persist().to_string(), "the persist view differs from persist");
    entry
}

/// A call may fail only by putting the actor in `error`, which XState
/// reports through the snapshot rather than by throwing.
fn tolerate(actor: &Actor<Value, Value>, result: Result<(), xstate_rt::SystemError>) -> Result<(), String> {
    match result {
        Err(e) if actor.status() != Status::Error => Err(e.to_string()),
        _ => Ok(()),
    }
}

/// The Rust trace for a fixture, in the shape `runner.mjs` records. `None`
/// when the initial transition fails (XState then reports an error snapshot
/// with no value).
fn trace(fixture: &Value) -> Result<Option<Vec<Value>>, String> {
    let machine = Machine::from_json(&fixture["machine"], &stubs()).map_err(|e| e.to_string())?;
    let mut actor = match Actor::start(Arc::new(machine), 0) {
        Ok(actor) => actor,
        Err(_) => return Ok(None),
    };
    let mut actual = vec![record(&mut actor, "start")];
    let mut now = 0;
    for (i, step) in fixture["steps"].as_array().unwrap().iter().enumerate() {
        // As runner.mjs: the clock only moves forward.
        let at = step["at"].as_u64().unwrap_or(0).max(now);
        if at > now {
            now = at;
            let advanced = actor.advance(at);
            tolerate(&actor, advanced)?;
        }
        if let Some(event) = step.get("send") {
            let sent = actor.send(event.clone(), at);
            tolerate(&actor, sent)?;
        }
        actual.push(record(&mut actor, &format!("step {i}")));
    }
    Ok(Some(actual))
}

fn run(fixture: &Value, expected: &[Value]) -> Result<(), String> {
    let Some(actual) = trace(fixture)? else {
        return if expected.iter().all(|r| r["status"] == "error" && r.get("value").is_none()) {
            Ok(())
        } else {
            Err("the initial transition failed".into())
        };
    };

    for (exp, act) in expected.iter().zip(&actual) {
        let mut exp = exp.clone();
        let mut act = act.clone();
        // A machine without context has `undefined` context in XState.
        // After a failed macrostep XState reports the previous snapshot, but
        // entry actions of the failed step may already have written their
        // context into it (`nextState.context = ...` on an uncloned snapshot,
        // an aliasing bug in alpha.70). Rust reports the previous context.
        if exp["context"].is_null() || exp["status"] == "error" {
            exp["context"] = Value::Null;
            act["context"] = Value::Null;
        }
        // Compare serialized forms: key order is part of XState's behavior
        // (state values follow the configuration order).
        if serde_json::to_string(&exp).unwrap() != serde_json::to_string(&act).unwrap() {
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

fn run_fixture(path: &PathBuf) -> Result<(), String> {
    let fixture: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let expected_path = path.with_extension("expected.json");
    let expected: Vec<Value> = serde_json::from_str(
        &std::fs::read_to_string(&expected_path).map_err(|e| format!("{}: {e}", expected_path.display()))?,
    )
    .unwrap();
    run(&fixture, &expected)
}

fn run_corpus(path: &PathBuf) -> Vec<String> {
    let cases: Vec<Value> = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let mut failures = Vec::new();
    let only = std::env::var("XSTATE_FUZZ_CASE").ok();
    for case in &cases {
        let name = case["name"].as_str().unwrap();
        if only.as_deref().is_some_and(|o| o != name) {
            continue;
        }
        let result = match &case["expected"] {
            Value::Array(expected) => run(case, expected),
            other => match Machine::<Value, Value>::from_json(&case["machine"], &stubs()) {
                Ok(_) => Err(format!("XState rejects the definition ({}) but Rust loads it", other["loadError"])),
                Err(_) => Ok(()),
            },
        };
        if let Err(e) = result {
            failures.push(format!("{name}: {e}\n  machine: {}", case["machine"]));
        }
    }
    failures
}

/// The committed corpus, plus `XSTATE_FUZZ_CORPUS` (a corpus file written by
/// `tools/differential/fuzz.sh`) when set.
#[test]
fn fuzz_corpus_matches_xstate() {
    let mut paths: Vec<PathBuf> =
        std::fs::read_dir(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/fuzz"))
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| {
                p.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.starts_with("corpus") && n.ends_with(".json"))
            })
            .collect();
    if let Some(extra) = std::env::var_os("XSTATE_FUZZ_CORPUS") {
        paths.push(PathBuf::from(extra));
    }
    paths.sort();
    let failures: Vec<String> = paths.iter().flat_map(run_corpus).collect();
    assert!(failures.is_empty(), "{} generated cases differ from XState:\n\n{}", failures.len(), failures.join("\n\n"));
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

/// `XSTATE_REPLAY=<fixture.json> XSTATE_REPLAY_OUT=<trace.json>`: writes the
/// Rust trace of one fixture, for tools that compare it with XState's (such
/// as a minimizer).
#[test]
fn replay() {
    let (Some(input), Some(output)) = (std::env::var_os("XSTATE_REPLAY"), std::env::var_os("XSTATE_REPLAY_OUT")) else {
        return;
    };
    let fixture: Value = serde_json::from_str(&std::fs::read_to_string(input).unwrap()).unwrap();
    let result = match trace(&fixture) {
        Ok(Some(t)) => Value::Array(t),
        Ok(None) => serde_json::json!({ "startFailed": true }),
        Err(e) => serde_json::json!({ "error": e }),
    };
    std::fs::write(output, result.to_string()).unwrap();
}
