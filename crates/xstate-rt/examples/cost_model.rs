//! A cost model of the runtime: the time of each operation it counts
//! (`xstate_rt::ops`), fitted over the differential corpus, so the cost of a
//! machine can be predicted from what it does.
//!
//!   cargo run --release -p xstate-rt --features op-counts --example cost_model -- fit [model.json]
//!   cargo run --release -p xstate-rt --features op-counts --example cost_model -- predict <fixture.json> [model.json]
//!
//! `fit` replays every machine and system fixture and both fuzz corpora with
//! the differential stubs, counts each case's operations once and times it
//! (the fastest of several batches), then fits a non-negative cost per
//! operation that minimizes the relative error of the predicted case times.
//! It reports the error of predictions for cases left out of the fit
//! (five-fold cross-validation) and writes the costs (default
//! `crates/xstate-rt/cost-model.json`).
//!
//! `predict` runs a fixture (`{ machine, steps, input? }` or a system
//! fixture) in counting mode and prints each step's operations and predicted
//! time, next to the measured time. Predictions cover the runtime only: the
//! stubs' guards, actions and expressions cost next to nothing, so a real
//! machine adds the time of its own implementations.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value};
use xstate_rt::ops::{self, Counts, Op};
use xstate_rt::step::async_logic;
use xstate_rt::{Actor, InitOptions, Logic, Machine, Registry, System, SystemLogic};

#[path = "../tests/common/exprs.rs"]
mod exprs;

/// The differential stubs (`tools/differential`).
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
    exprs::register(&mut r);
    for (name, logic) in actors {
        r.actor(name.clone(), logic.clone());
    }
    r
}

/// A replayable case: a machine run by an `Actor`, or a system.
enum Case {
    Machine { machine: Arc<Machine<Value, Value>>, input: Option<Value>, steps: Vec<(u64, Option<Value>)> },
    System { logic: SystemLogic, options: InitOptions, steps: Vec<Value> },
}

impl Case {
    fn from_fixture(fixture: &Value) -> Option<Case> {
        if fixture.get("logic").is_some() {
            return Some(Case::System {
                logic: system_logic(fixture)?,
                options: InitOptions {
                    time: fixture["init"]["time"].as_u64().unwrap_or(0),
                    input: fixture["init"].get("input").cloned(),
                    id: fixture["init"]["id"].as_str().map(str::to_owned),
                    ..Default::default()
                },
                steps: fixture["steps"].as_array()?.clone(),
            });
        }
        let machine = Machine::from_json(&fixture["machine"], &stubs(&[])).ok()?;
        let steps = fixture["steps"]
            .as_array()?
            .iter()
            .map(|s| (s["at"].as_u64().unwrap_or(0), s.get("send").cloned()))
            .collect();
        Some(Case::Machine { machine: Arc::new(machine), input: fixture.get("input").cloned(), steps })
    }

    /// Runs the case; `step` is called after the start and after each step.
    fn run(&self, step: &mut dyn FnMut()) -> bool {
        match self {
            Case::Machine { machine, input, steps } => {
                let options = InitOptions { input: input.clone(), ..Default::default() };
                let Ok(mut actor) = Actor::start_with(machine.clone(), options) else { return false };
                step();
                let mut now = 0;
                for (at, event) in steps {
                    if *at > now {
                        now = *at;
                        let _ = actor.advance(now);
                    }
                    if let Some(event) = event {
                        let _ = actor.send(event.clone(), now);
                    }
                    std::hint::black_box(actor.take_emitted());
                    step();
                }
                true
            }
            Case::System { logic, options, steps } => {
                let Ok((mut sys, _)) = System::initialize(logic, options.clone()) else { return false };
                step();
                for s in steps {
                    let result = if let Some(address) = s.get("send") {
                        sys.send(address.as_str().unwrap_or(""), s["event"].clone())
                    } else if let Some(t) = s.get("advance") {
                        sys.advance(t.as_u64().unwrap_or(0))
                    } else {
                        let mut ids: Vec<(u64, String)> = sys
                            .external_effects()
                            .map(|e| (e.id.rsplit(':').next().and_then(|n| n.parse().ok()).unwrap_or(0), e.id.clone()))
                            .collect();
                        ids.sort();
                        match ids.get(s["resolve"].as_u64().unwrap_or(0) as usize) {
                            Some((_, id)) => sys.resolve_effect(id, s.get("event").cloned()),
                            None => Ok(Vec::new()),
                        }
                    };
                    let _ = std::hint::black_box(result);
                    step();
                }
                true
            }
        }
    }
}

fn system_logic(fixture: &Value) -> Option<SystemLogic> {
    let mut built: Vec<(String, Arc<dyn Logic>)> = Vec::new();
    for def in fixture["logic"].as_array()? {
        let name = def["name"].as_str()?.to_owned();
        let logic: Arc<dyn Logic> = match def.get("async") {
            Some(a) => Arc::new(async_logic(None, a.get("timeout").cloned()).ok()?),
            None => {
                let actors: Vec<(String, Arc<dyn Logic>)> = def["actors"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .filter_map(|n| built.iter().find(|(b, _)| Some(b.as_str()) == n.as_str()).cloned())
                    .collect();
                Arc::new(Machine::<Value, Value>::from_json(&def["machine"], &stubs(&actors)).ok()?)
            }
        };
        built.push((name, logic));
    }
    let root = fixture["root"].as_str()?;
    Some(SystemLogic::new(built.into_iter().find(|(n, _)| n == root)?.1))
}

/// Every case of the corpus, by name.
fn corpus(root: &Path) -> Vec<(String, Case)> {
    let mut out = Vec::new();
    let mut add = |name: String, fixture: &Value| {
        if let Some(case) = Case::from_fixture(fixture) {
            out.push((name, case));
        }
    };
    for dir in [root.to_path_buf(), root.join("system")] {
        let mut paths: Vec<PathBuf> = std::fs::read_dir(&dir)
            .into_iter()
            .flatten()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|e| e == "json") && !p.to_string_lossy().ends_with(".expected.json"))
            .collect();
        paths.sort();
        for p in paths {
            let fixture: Value = serde_json::from_str(&std::fs::read_to_string(&p).unwrap()).unwrap();
            add(p.file_stem().unwrap().to_string_lossy().into_owned(), &fixture);
        }
    }
    for file in ["fuzz/corpus.json", "fuzz/system-corpus.json"] {
        let cases: Vec<Value> = serde_json::from_str(&std::fs::read_to_string(root.join(file)).unwrap()).unwrap();
        for c in cases.iter().filter(|c| c["expected"].get("loadError").is_none()) {
            add(format!("{file}:{}", c["name"].as_str().unwrap_or("?")), c);
        }
    }
    out
}

/// The operations of one run (the start and every step together).
fn count(case: &Case) -> Counts {
    ops::set_enabled(true);
    ops::take();
    case.run(&mut || {});
    let counts = ops::take();
    ops::set_enabled(false);
    counts
}

/// Whether a case runs into the microstep bound (a runaway loop: a failing
/// step, not normal operation, and cheaper per operation than normal steps).
fn runaway(x: &Counts) -> bool {
    x[Op::Microstep as usize] >= 1000
}

/// Operations that nearly always come together share one cost, which the
/// fit could not split between them: a state entered and one exited, and a
/// candidate and its guard (the stubs' guards cost next to nothing).
const TIED: &[(Op, Op)] = &[(Op::Enter, Op::Exit), (Op::Guard, Op::Candidate)];

/// The counts with each tied operation folded into its partner.
fn grouped(x: &Counts) -> Counts {
    let mut g = *x;
    for &(from, to) in TIED {
        g[to as usize] += g[from as usize];
        g[from as usize] = 0;
    }
    g
}

/// Costs fitted on grouped counts, given back per operation.
fn ungroup(mut c: Vec<f64>) -> Vec<f64> {
    for &(from, to) in TIED {
        c[from as usize] = c[to as usize];
    }
    c
}

/// The time of one run: the fastest mean of several batches.
fn time(case: &Case) -> f64 {
    let once = || {
        let t = Instant::now();
        case.run(&mut || {});
        t.elapsed()
    };
    let probe = (0..3).map(|_| once()).min().unwrap().max(Duration::from_nanos(100));
    let reps = (Duration::from_millis(3).as_nanos() / probe.as_nanos()).clamp(5, 200_000) as u32;
    (0..7)
        .map(|_| {
            let t = Instant::now();
            for _ in 0..reps {
                case.run(&mut || {});
            }
            t.elapsed().as_nanos() as f64 / reps as f64
        })
        .fold(f64::INFINITY, f64::min)
}

/// Non-negative least squares on relative error: the costs `c >= 0`
/// minimizing `sum ((t - x.c) / t)^2`, by coordinate descent on the normal
/// equations.
fn fit(xs: &[Counts], ts: &[f64]) -> Vec<f64> {
    let n = Op::ALL.len();
    let mut a = vec![vec![0.0; n]; n];
    let mut b = vec![0.0; n];
    for (x, &t) in xs.iter().zip(ts) {
        let w = 1.0 / (t * t);
        for i in 0..n {
            b[i] += w * x[i] as f64 * t;
            for j in 0..n {
                a[i][j] += w * x[i] as f64 * x[j] as f64;
            }
        }
    }
    let mut c = vec![0.0; n];
    for _ in 0..20_000 {
        let mut change: f64 = 0.0;
        for i in 0..n {
            if a[i][i] == 0.0 {
                continue;
            }
            let rest: f64 = (0..n).filter(|&j| j != i).map(|j| a[i][j] * c[j]).sum();
            let next = ((b[i] - rest) / a[i][i]).max(0.0);
            change = change.max((next - c[i]).abs());
            c[i] = next;
        }
        if change < 1e-9 {
            break;
        }
    }
    c
}

fn predict(costs: &[f64], x: &Counts) -> f64 {
    x.iter().zip(costs).map(|(&n, &c)| n as f64 * c).sum()
}

fn percentile(mut v: Vec<f64>, p: f64) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    v[((v.len() - 1) as f64 * p).round() as usize]
}

fn report(label: &str, errors: Vec<f64>) {
    println!(
        "{label}: median {:.1}%, p90 {:.1}%, max {:.1}% (relative error of predicted case time, {} cases)",
        percentile(errors.clone(), 0.5) * 100.0,
        percentile(errors.clone(), 0.9) * 100.0,
        percentile(errors.clone(), 1.0) * 100.0,
        errors.len()
    );
}

fn main() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures");
    let default_model = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("cost-model.json");
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("fit") => {
            let cases = corpus(&root);
            eprintln!("measuring {} cases...", cases.len());
            let mut xs = Vec::new();
            let mut ts = Vec::new();
            let mut names = Vec::new();
            let mut runaways = 0;
            for (name, case) in &cases {
                let x = count(case);
                if runaway(&x) {
                    runaways += 1;
                    continue;
                }
                xs.push(x);
                ts.push(time(case));
                names.push(name.clone());
            }
            eprintln!("left out {runaways} runaway loops");
            let gs: Vec<Counts> = xs.iter().map(grouped).collect();
            let costs = ungroup(fit(&gs, &ts));
            println!("{:<16} {:>10} {:>14}", "operation", "ns / op", "count (corpus)");
            for (i, op) in Op::ALL.iter().enumerate() {
                let total: u64 = xs.iter().map(|x| x[i]).sum();
                println!("{:<16} {:>10.1} {:>14}", op.name(), costs[i], total);
            }
            let in_sample = xs.iter().zip(&ts).map(|(x, t)| (predict(&costs, x) - t).abs() / t).collect();
            report("in sample", in_sample);
            // Five-fold cross-validation: each case predicted by costs
            // fitted without it.
            let mut held_out = Vec::new();
            for fold in 0..5 {
                let train: Vec<usize> = (0..xs.len()).filter(|i| i % 5 != fold).collect();
                let c = ungroup(fit(
                    &train.iter().map(|&i| gs[i]).collect::<Vec<_>>(),
                    &train.iter().map(|&i| ts[i]).collect::<Vec<_>>(),
                ));
                for i in (0..xs.len()).filter(|i| i % 5 == fold) {
                    held_out.push((predict(&c, &xs[i]) - ts[i]).abs() / ts[i]);
                }
            }
            report("held out (5-fold)", held_out.clone());
            if std::env::var_os("COST_WORST").is_some() {
                let mut idx: Vec<usize> = (0..xs.len()).collect();
                idx.sort_by(|&a, &b| {
                    let ea = (predict(&costs, &xs[a]) - ts[a]) / ts[a];
                    let eb = (predict(&costs, &xs[b]) - ts[b]) / ts[b];
                    eb.abs().partial_cmp(&ea.abs()).unwrap()
                });
                for &i in idx.iter().take(15) {
                    let ops: Vec<String> = Op::ALL
                        .iter()
                        .enumerate()
                        .filter(|(j, _)| xs[i][*j] > 0)
                        .map(|(j, op)| format!("{} {}", op.name(), xs[i][j]))
                        .collect();
                    println!(
                        "{:>+6.0}% {:>9.1}µs {}: {}",
                        (predict(&costs, &xs[i]) - ts[i]) / ts[i] * 100.0,
                        ts[i] / 1000.0,
                        names[i],
                        ops.join(", ")
                    );
                }
            }
            let model: serde_json::Map<String, Value> = Op::ALL
                .iter()
                .zip(&costs)
                .map(|(op, c)| (op.name().to_owned(), json!((c * 10.0).round() / 10.0)))
                .collect();
            let path = args.get(1).map(PathBuf::from).unwrap_or(default_model);
            std::fs::write(&path, serde_json::to_string_pretty(&json!({ "nsPerOp": model })).unwrap() + "\n").unwrap();
            println!("wrote {}", path.display());
        }
        Some("predict") => {
            let fixture: Value = serde_json::from_str(&std::fs::read_to_string(&args[1]).unwrap()).unwrap();
            let path = args.get(2).map(PathBuf::from).unwrap_or(default_model);
            let model: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
            let costs: Vec<f64> =
                Op::ALL.iter().map(|op| model["nsPerOp"][op.name()].as_f64().unwrap_or(0.0)).collect();
            let case = Case::from_fixture(&fixture).expect("a loadable fixture");
            let mut per_step = Vec::new();
            ops::set_enabled(true);
            ops::take();
            case.run(&mut || per_step.push(ops::take()));
            ops::set_enabled(false);
            println!("{:<8} {:>10}  operations", "step", "predicted");
            for (i, x) in per_step.iter().enumerate() {
                let label = if i == 0 { "start".to_owned() } else { format!("{}", i - 1) };
                let ops: Vec<String> = Op::ALL
                    .iter()
                    .enumerate()
                    .filter(|(j, _)| x[*j] > 0)
                    .map(|(j, op)| format!("{} {}", op.name(), x[j]))
                    .collect();
                println!("{label:<8} {:>8.2}µs  {}", predict(&costs, x) / 1000.0, ops.join(", "));
            }
            let total: Counts = per_step.iter().fold([0; Op::ALL.len()], |mut acc, x| {
                acc.iter_mut().zip(x).for_each(|(a, b)| *a += b);
                acc
            });
            println!(
                "total: predicted {:.2}µs, measured {:.2}µs",
                predict(&costs, &total) / 1000.0,
                time(&case) / 1000.0
            );
        }
        _ => eprintln!("usage: cost_model fit [model.json] | predict <fixture.json> [model.json]"),
    }
}
