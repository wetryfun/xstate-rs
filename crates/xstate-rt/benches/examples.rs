//! Realistic machines: the fixtures ported from XState's own examples
//! (`fixtures/ex_*.json` through an `Actor`, `fixtures/system/ex_*.json`
//! through a `System`), each fixture's steps replayed from a fresh actor or
//! system. Throughput is per step (an event, a clock advance or an effect
//! result), so the time per step is the cost of one realistic call.
//!
//!   cargo bench -p xstate-rt --bench examples
//!
//! `XSTATE_BENCH_FIXTURES` selects fixtures by name prefix (default `ex_`).

use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion, Throughput};
use serde_json::Value;
use xstate_rt::step::async_logic;
use xstate_rt::{Actor, InitOptions, Logic, Machine, Registry, System, SystemLogic};

/// The stubs of `tools/differential` (machine and system runners).
fn stubs(actors: &[(String, Arc<dyn Logic>)]) -> Registry<Value, Value> {
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
            matches!((args.context.get(&p.key).and_then(Value::as_f64), p.value.as_f64()), (Some(a), Some(b)) if a >= b)
        })
        .guard_with("eventEquals", |args, p: &KeyValue| args.event.field(&p.key).as_ref() == Some(&p.value))
        .action("noop", || {})
        .action("track", || {})
        .delay_ms("short", 100)
        .delay_ms("long", 1000);
    for (name, logic) in actors {
        r.actor(name.clone(), logic.clone());
    }
    r
}

fn fixtures(dir: &Path) -> Vec<(String, Value)> {
    let prefix = std::env::var("XSTATE_BENCH_FIXTURES").unwrap_or_else(|_| "ex_".into());
    let mut found: Vec<(String, Value)> = std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .filter_map(|e| {
            let path = e.ok()?.path();
            let name = path.file_name()?.to_str()?.strip_suffix(".json")?.to_owned();
            (name.starts_with(&prefix) && !name.ends_with(".expected")).then_some(())?;
            Some((name, serde_json::from_str(&std::fs::read_to_string(&path).ok()?).ok()?))
        })
        .collect();
    found.sort_by(|a, b| a.0.cmp(&b.0));
    found
}

fn system_logic(fixture: &Value) -> SystemLogic {
    let mut built: Vec<(String, Arc<dyn Logic>)> = Vec::new();
    for def in fixture["logic"].as_array().unwrap() {
        let name = def["name"].as_str().unwrap().to_owned();
        let logic: Arc<dyn Logic> = match def.get("async") {
            Some(a) => Arc::new(async_logic(None, a.get("timeout").cloned()).unwrap()),
            None => {
                let actors: Vec<(String, Arc<dyn Logic>)> = def["actors"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|n| built.iter().find(|(b, _)| b == n.as_str().unwrap()).cloned().unwrap())
                    .collect();
                Arc::new(Machine::<Value, Value>::from_json(&def["machine"], &stubs(&actors)).unwrap())
            }
        };
        built.push((name, logic));
    }
    let root = fixture["root"].as_str().unwrap();
    SystemLogic::new(built.into_iter().find(|(n, _)| n == root).unwrap().1)
}

fn bench(c: &mut Criterion) {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures");
    let mut g = c.benchmark_group("examples");
    for (name, fixture) in fixtures(&root) {
        let machine = Arc::new(Machine::<Value, Value>::from_json(&fixture["machine"], &stubs(&[])).unwrap());
        let steps: Vec<(u64, Option<Value>)> = fixture["steps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| (s["at"].as_u64().unwrap_or(0), s.get("send").cloned()))
            .collect();
        g.throughput(Throughput::Elements(steps.len() as u64));
        g.bench_function(&name, |b| {
            b.iter_batched(
                || Actor::start(machine.clone(), 0).unwrap(),
                |mut actor| {
                    let mut now = 0;
                    for (at, event) in &steps {
                        if *at > now {
                            now = *at;
                            let _ = actor.advance(now);
                        }
                        if let Some(event) = event {
                            let _ = actor.send(event.clone(), now);
                        }
                        black_box(actor.take_emitted());
                    }
                    actor
                },
                BatchSize::SmallInput,
            )
        });
    }
    for (name, fixture) in fixtures(&root.join("system")) {
        let logic = system_logic(&fixture);
        let init = &fixture["init"];
        let options = InitOptions {
            time: init["time"].as_u64().unwrap_or(0),
            input: init.get("input").cloned(),
            id: init["id"].as_str().map(str::to_owned),
            ..Default::default()
        };
        let steps = fixture["steps"].as_array().unwrap().clone();
        g.throughput(Throughput::Elements(steps.len() as u64));
        g.bench_function(format!("system/{name}"), |b| {
            b.iter_batched(
                || System::initialize(&logic, options.clone()).unwrap().0,
                |mut sys| {
                    for step in &steps {
                        let result = if let Some(address) = step.get("send") {
                            sys.send(address.as_str().unwrap(), step["event"].clone())
                        } else if let Some(t) = step.get("advance") {
                            sys.advance(t.as_u64().unwrap())
                        } else {
                            // The n-th pending effect, in id order.
                            let mut ids: Vec<(u64, String)> = sys
                                .external_effects()
                                .map(|e| (e.id.rsplit(':').next().unwrap().parse().unwrap(), e.id.clone()))
                                .collect();
                            ids.sort();
                            match ids.get(step["resolve"].as_u64().unwrap() as usize) {
                                Some((_, id)) => sys.resolve_effect(id, step.get("event").cloned()),
                                None => Ok(Vec::new()),
                            }
                        };
                        let _ = black_box(result);
                    }
                    sys
                },
                BatchSize::SmallInput,
            )
        });
    }
    g.finish();
}

criterion_group!(benches, bench);
criterion_main!(benches);
