//! Replays every fixture in `fixtures/system/` on the Rust [`System`] and
//! compares the full system snapshot and the returned effects with what
//! XState's pure system API produced (`<name>.expected.json`, written by
//! `tools/differential/generate-system.mjs`).

use std::path::PathBuf;
use std::sync::Arc;

use serde_json::Value;
use xstate_rt::step::async_logic;
use xstate_rt::{InitOptions, Logic, Machine, Registry, System, SystemLogic};

/// Rust equivalents of the stubs in `generate-system.mjs`.
#[path = "common/exprs.rs"]
mod exprs;

/// The languages the including test crate registers evaluators for: none
/// here; `xstate-js` includes this file as a module and defines its own at
/// its crate root, which then runs only the fixtures that declare them.
#[allow(dead_code)]
pub const LANGUAGES: &[&str] = &[];

#[allow(dead_code)]
pub fn register_languages(_registry: &mut Registry<Value, Value>) {}

/// Whether a fixture (its `languages`) runs in this test crate: one that
/// needs evaluators only where they are registered, the others only here.
fn runs_here(fixture: &Value) -> bool {
    match fixture.get("languages").and_then(Value::as_array) {
        Some(langs) => {
            !crate::LANGUAGES.is_empty()
                && langs.iter().all(|l| l.as_str().is_some_and(|l| crate::LANGUAGES.contains(&l)))
        }
        None => crate::LANGUAGES.is_empty(),
    }
}

fn registry(actors: &[(String, Arc<dyn Logic>)]) -> Registry<Value, Value> {
    #[derive(serde::Deserialize)]
    struct KeyValue {
        key: String,
        value: Value,
    }
    let mut r: Registry<Value, Value> = Registry::new();
    r.guard("yes", |_| true)
        .guard("no", |_| false)
        .guard_with("ctxEquals", |args, p: &KeyValue| args.context.get(&p.key) == Some(&p.value))
        .guard_with("eventEquals", |args, p: &KeyValue| args.event.field(&p.key).as_ref() == Some(&p.value))
        .action("noop", || {})
        .action("track", || {})
        .delay_ms("short", 100)
        .delay_ms("long", 1000);
    exprs::register(&mut r);
    crate::register_languages(&mut r);
    for (name, logic) in actors {
        r.actor(name.clone(), logic.clone());
    }
    r
}

fn build(fixture: &Value) -> Result<SystemLogic, String> {
    let mut built: Vec<(String, Arc<dyn Logic>)> = Vec::new();
    for def in fixture["logic"].as_array().unwrap() {
        let name = def["name"].as_str().unwrap().to_owned();
        let logic: Arc<dyn Logic> = if let Some(a) = def.get("async") {
            Arc::new(async_logic(None, a.get("timeout").cloned()).map_err(|e| e.to_string())?)
        } else {
            let actors: Vec<(String, Arc<dyn Logic>)> = def["actors"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|n| built.iter().find(|(b, _)| b == n.as_str().unwrap()).cloned().unwrap())
                .collect();
            let machine: Machine<Value, Value> =
                Machine::from_json(&def["machine"], &registry(&actors)).map_err(|e| e.to_string())?;
            Arc::new(machine)
        };
        built.push((name, logic));
    }
    let root = fixture["root"].as_str().unwrap();
    Ok(SystemLogic::new(built.into_iter().find(|(n, _)| n == root).unwrap().1))
}

fn pending_ids(snapshot: &Value) -> Vec<String> {
    let mut ids: Vec<String> = snapshot["externalEffects"].as_object().unwrap().keys().cloned().collect();
    ids.sort_by_key(|id| id.rsplit(':').next().unwrap().parse::<u64>().unwrap());
    ids
}

/// Object comparison ignores key order, except in state values, where key
/// order is the configuration order.
fn compare(label: &str, expected: &Value, actual: &Value) -> Result<(), String> {
    if expected != actual {
        return Err(format!("{label}: mismatch\n  expected: {expected}\n  actual:   {actual}"));
    }
    if let (Some(e), Some(a)) = (expected.get("actors").and_then(Value::as_object), actual.get("actors")) {
        for (address, actor) in e {
            let ev = actor["snapshot"].get("value");
            let av = a[address]["snapshot"].get("value");
            if ev.map(Value::to_string) != av.map(Value::to_string) {
                return Err(format!("{label}: state value key order differs for {address}: {ev:?} vs {av:?}"));
            }
        }
    }
    Ok(())
}

fn run_fixture(path: &PathBuf) -> Result<(), String> {
    let fixture: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    if !runs_here(&fixture) {
        return Ok(());
    }
    let expected: Vec<Value> = serde_json::from_str(
        &std::fs::read_to_string(path.with_extension("expected.json")).map_err(|e| e.to_string())?,
    )
    .unwrap();
    run(&fixture, &expected)
}

fn run(fixture: &Value, expected: &[Value]) -> Result<(), String> {
    let logic = build(fixture)?;
    let init = &fixture["init"];
    let options = InitOptions {
        time: init["time"].as_u64().unwrap_or(0),
        input: init.get("input").cloned(),
        id: init["id"].as_str().map(str::to_owned),
        ..Default::default()
    };

    let mut records = expected.iter();
    let check = |label: &str,
                 exp: &Value,
                 result: Result<Vec<xstate_rt::ExternalEffect>, xstate_rt::SystemError>,
                 sys: Option<&System>| {
        match (exp.get("error"), result) {
            (Some(_), Err(_)) => Ok(()),
            (Some(e), Ok(_)) => Err(format!("{label}: XState failed ({e}) but Rust succeeded")),
            (None, Err(e)) => Err(format!("{label}: Rust failed: {e}")),
            (None, Ok(effects)) => {
                let effects = Value::Array(effects.iter().map(|e| e.to_json()).collect());
                compare(&format!("{label} effects"), &exp["effects"], &effects)?;
                let actual = sys.unwrap().to_json();
                compare(&format!("{label} snapshot"), &exp["snapshot"], &actual)?;
                // The serialize-without-building view writes the same data.
                let viewed = serde_json::to_string(&sys.unwrap().view()).unwrap();
                let text = actual.to_string();
                if viewed != text {
                    return Err(format!(
                        "{label}: the view differs from to_json\n  view:    {viewed}\n  to_json: {actual}"
                    ));
                }
                // The next deadline is the earliest pending timer's.
                let earliest =
                    actual["timers"].as_array().and_then(|t| t.iter().filter_map(|t| t["dueAt"].as_u64()).min());
                if sys.unwrap().next_deadline() != earliest {
                    return Err(format!(
                        "{label}: next_deadline {:?}, timers due {earliest:?}",
                        sys.unwrap().next_deadline()
                    ));
                }
                // The compact snapshot restores to the same system.
                let compact = serde_json::to_value(sys.unwrap().compact_view()).unwrap();
                let from_compact = System::from_json(&logic, &compact, 10_000)
                    .map_err(|e| format!("{label}: restoring the compact snapshot failed: {e}"))?;
                let restored_text = from_compact.to_json().to_string();
                if restored_text != text {
                    return Err(format!(
                        "{label}: the compact snapshot restores differently\n  compact:  {compact}\n  restored: {restored_text}\n  to_json:  {actual}"
                    ));
                }
                // XState's own snapshot data restores into an equal Rust system.
                let restored = System::from_json(&logic, &exp["snapshot"], 10_000)
                    .map_err(|e| format!("{label}: restoring XState's snapshot failed: {e}"))?;
                compare(&format!("{label} restored"), &exp["snapshot"], &restored.to_json())
            }
        }
    };

    let first = records.next().ok_or("no records")?;
    let (mut sys, effects) = match System::initialize(&logic, options) {
        Ok(r) => r,
        Err(e) => return if first.get("error").is_some() { Ok(()) } else { Err(format!("init failed: {e}")) },
    };
    check("init", first, Ok(effects), Some(&sys))?;

    for (i, step) in fixture["steps"].as_array().unwrap().iter().enumerate() {
        let label = format!("step {i}");
        let exp = records.next().ok_or(format!("{label}: no expected record"))?;
        let before = sys.to_json();
        let result = if let Some(address) = step.get("send") {
            sys.send(address.as_str().unwrap(), step["event"].clone())
        } else if let Some(t) = step.get("advance") {
            sys.advance(t.as_u64().unwrap())
        } else {
            let index = step["resolve"].as_u64().unwrap() as usize;
            match pending_ids(&before).get(index) {
                Some(id) => sys.resolve_effect(id, step.get("event").cloned()),
                None => Err(xstate_rt::SystemError::Rejected(format!("no pending effect at index {index}"))),
            }
        };
        let failed = result.is_err();
        check(&label, exp, result, Some(&sys))?;
        if failed {
            // XState leaves its input snapshot unchanged when a call throws.
            sys = System::from_json(&logic, &before, 10_000).map_err(|e| e.to_string())?;
            compare(&format!("{label} (unchanged)"), &before, &sys.to_json())?;
        }
    }
    Ok(())
}

#[test]
fn system_fixtures_match_xstate() {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/system");
    let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| {
            let name = p.file_name().unwrap().to_str().unwrap();
            name.ends_with(".json") && !name.ends_with(".expected.json")
        })
        .collect();
    paths.sort();
    assert!(!paths.is_empty());
    let failures: Vec<String> = paths
        .iter()
        .filter_map(|p| run_fixture(p).err().map(|e| format!("{}: {e}", p.file_name().unwrap().to_str().unwrap())))
        .collect();
    assert!(
        failures.is_empty(),
        "{} of {} system fixtures differ from XState:\n\n{}",
        failures.len(),
        paths.len(),
        failures.join("\n\n")
    );
}

/// The generated corpus (`fixtures/fuzz/system-corpus.json`, written by
/// `tools/differential/fuzz-system.mjs`), plus `XSTATE_SYSTEM_FUZZ_CORPUS`
/// when set. `XSTATE_FUZZ_CASE` selects one case.
#[test]
fn system_fuzz_corpus_matches_xstate() {
    if !crate::LANGUAGES.is_empty() {
        return;
    }
    let mut paths = vec![PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/fuzz/system-corpus.json")];
    if let Some(extra) = std::env::var_os("XSTATE_SYSTEM_FUZZ_CORPUS") {
        paths.push(PathBuf::from(extra));
    }
    let only = std::env::var("XSTATE_FUZZ_CASE").ok();
    let mut failures = Vec::new();
    let mut total = 0;
    for path in paths {
        let cases: Vec<Value> = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
        for case in &cases {
            let name = case["name"].as_str().unwrap();
            if only.as_deref().is_some_and(|o| o != name) {
                continue;
            }
            total += 1;
            let expected = case["expected"].as_array().unwrap();
            if let Err(e) = run(case, expected) {
                failures.push(format!("{name}: {e}"));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {total} generated systems differ from XState:\n\n{}",
        failures.len(),
        failures.join("\n\n")
    );
}
