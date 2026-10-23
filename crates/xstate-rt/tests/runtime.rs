use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use xstate_rt::*;

// ---- typed context and events ------------------------------------------------

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct Ctx {
    count: i64,
    #[serde(default)]
    log: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
enum Event {
    #[serde(rename = "inc")]
    Inc { by: i64 },
    #[serde(rename = "reset")]
    Reset,
    #[serde(rename = "finish")]
    Finish,
}

impl EventType for Event {
    fn event_type(&self) -> &str {
        match self {
            Event::Inc { .. } => "inc",
            Event::Reset => "reset",
            Event::Finish => "finish",
        }
    }
}

#[derive(Deserialize)]
struct Limit {
    max: i64,
}

fn counter_registry() -> Registry<Ctx, Event> {
    let mut r: Registry<Ctx, Event> = Registry::new();
    r.guard_with("below", |args, p: &Limit| args.context.count < p.max)
        .action("add", |args| {
            let by = match &args.event {
                Ev::User(Event::Inc { by }) => *by,
                _ => 0,
            };
            args.context_mut().count += by;
        })
        .action_with("note", |args, text: &String| {
            args.context_mut().log.push(text.clone());
            args.emit(json!({ "type": "noted", "text": text }));
        })
        .delay("backoff", |ctx, _| (ctx.count as u64) * 100);
    r
}

fn counter_machine() -> Arc<Machine<Ctx, Event>> {
    let def = json!({
        "id": "counter",
        "initial": "counting",
        "context": { "count": 0 },
        "states": {
            "counting": {
                "entry": [{ "type": "note", "params": "counting" }],
                "on": {
                    "inc": [
                        { "guard": { "type": "below", "params": { "max": 10 } }, "actions": [{ "type": "add" }] },
                        { "target": "full" }
                    ],
                    "finish": { "target": "cooldown" }
                }
            },
            "full": {
                "tags": ["full"],
                "on": { "reset": { "target": "counting", "actions": [{ "type": "@xstate.assign", "context": { "count": 0 } }] } }
            },
            "cooldown": { "after": { "backoff": { "target": "done" } } },
            "done": { "type": "final", "output": { "ok": true } }
        }
    });
    Arc::new(Machine::from_json(&def, &counter_registry()).unwrap())
}

#[test]
fn typed_context_rust_actions_and_params() {
    let mut actor = Actor::start(counter_machine(), 0).unwrap();
    assert_eq!(actor.context().log, vec!["counting"]);
    assert_eq!(actor.take_emitted(), vec![json!({ "type": "noted", "text": "counting" })]);

    assert!(actor.send(Event::Inc { by: 4 }, 0).unwrap());
    assert!(actor.send(Event::Inc { by: 4 }, 0).unwrap());
    assert_eq!(actor.context().count, 8);
    assert!(actor.send(Event::Inc { by: 4 }, 0).unwrap());
    assert_eq!(actor.context().count, 12);
    // 12 is not below 10: the second transition wins.
    actor.send(Event::Inc { by: 1 }, 0).unwrap();
    assert_eq!(actor.value(), json!("full"));
    assert!(actor.has_tag("full"));
    assert!(!actor.send(Event::Finish, 0).unwrap(), "finish is not handled in full");

    actor.send(Event::Reset, 0).unwrap();
    assert_eq!(actor.context().count, 0);
    assert_eq!(actor.context().log, vec!["counting", "counting"]);
}

#[test]
fn named_delay_from_registry_reads_context() {
    let mut actor = Actor::start(counter_machine(), 0).unwrap();
    actor.send(Event::Inc { by: 3 }, 0).unwrap();
    actor.send(Event::Finish, 50).unwrap();
    assert_eq!(actor.next_deadline(), Some(350));
    assert_eq!(actor.advance(349).unwrap(), 0);
    assert_eq!(actor.advance(350).unwrap(), 1);
    assert_eq!(actor.status(), Status::Done);
    assert_eq!(actor.output(), Some(&json!({ "ok": true })));
    assert_eq!(actor.next_deadline(), None);
    // A done actor handles nothing.
    assert!(!actor.send(Event::Reset, 400).unwrap());
}

// ---- untyped helpers ----------------------------------------------------------------

fn machine(def: Value) -> Arc<Machine<Value, Value>> {
    Arc::new(Machine::from_json(&def, &Registry::new()).unwrap())
}

fn send(actor: &mut Actor<Value, Value>, ty: &str) -> bool {
    actor.send(json!({ "type": ty }), actor.now()).unwrap()
}

#[test]
fn traffic_light() {
    let m = machine(json!({
        "id": "light",
        "initial": "green",
        "states": {
            "green": { "on": { "timer": { "target": "yellow" } } },
            "yellow": { "on": { "timer": { "target": "red" } } },
            "red": { "on": { "timer": { "target": "green" } } }
        }
    }));
    let mut a = Actor::start(m, 0).unwrap();
    for expected in ["yellow", "red", "green", "yellow"] {
        assert!(send(&mut a, "timer"));
        assert_eq!(a.value(), json!(expected));
    }
    assert!(a.matches("yellow"));
    assert!(a.matches("#light.yellow"));
    assert!(!a.matches("red"));
}

#[test]
fn on_done_is_accepted_and_matches_its_own_state() {
    let m = machine(json!({
        "id": "m",
        "initial": "outer",
        "states": {
            "outer": {
                "initial": "inner",
                "states": {
                    "inner": {
                        "initial": "x",
                        "states": { "x": { "on": { "go": { "target": "y" } } }, "y": { "type": "final" } },
                        "onDone": { "target": "innerDone" }
                    },
                    "innerDone": { "on": { "go": { "target": "end" } } },
                    "end": { "type": "final", "output": 42 }
                },
                "onDone": { "target": "finished" }
            },
            "finished": { "type": "final" }
        }
    }));
    let mut a = Actor::start(m, 0).unwrap();
    send(&mut a, "go");
    assert_eq!(a.value(), json!({ "outer": "innerDone" }));
    send(&mut a, "go");
    assert_eq!(a.value(), json!("finished"));
    assert_eq!(a.status(), Status::Done);
    assert_eq!(a.output(), None);
}

#[test]
fn deep_history_restores_atomic_descendants() {
    let m = machine(json!({
        "initial": "app",
        "states": {
            "app": {
                "initial": "a",
                "states": {
                    "a": { "initial": "a1", "states": { "a1": { "on": { "next": { "target": "a2" } } }, "a2": {} } },
                    "hist": { "type": "history", "history": "deep", "target": "a" }
                },
                "on": { "leave": { "target": "away" } }
            },
            "away": { "on": { "back": { "target": "app.hist" } } }
        }
    }));
    let mut a = Actor::start(m.clone(), 0).unwrap();
    send(&mut a, "back");
    send(&mut a, "next");
    send(&mut a, "leave");
    assert_eq!(a.value(), json!("away"));
    let saved = serde_json::to_value(a.persist()).unwrap();
    send(&mut a, "back");
    assert_eq!(a.value(), json!({ "app": { "a": "a2" } }));

    // The recorded history survives a persist/restore round trip.
    let restored = serde_json::from_value(saved).unwrap();
    let mut b = Actor::restore(m, restored).unwrap();
    send(&mut b, "back");
    assert_eq!(b.value(), json!({ "app": { "a": "a2" } }));
}

#[test]
fn runaway_eventless_loop_is_an_error() {
    let m = Arc::new(
        Machine::<Value, Value>::from_json_with_options(
            &json!({
                "initial": "a",
                "states": {
                    "a": { "on": { "go": { "target": "b" } } },
                    "b": { "always": { "target": "c" } },
                    "c": { "always": { "target": "b" } }
                }
            }),
            &Registry::new(),
            MachineOptions { max_iterations: 50 },
        )
        .unwrap(),
    );
    let mut a = Actor::start(m, 0).unwrap();
    let err = a.send(json!({ "type": "go" }), 0).unwrap_err();
    assert!(matches!(err, RuntimeError::InfiniteLoop { max_iterations: 50, .. }), "{err}");
    assert_eq!(a.status(), Status::Error);
}

#[test]
fn raised_event_loop_is_an_error() {
    let m = machine(json!({
        "initial": "a",
        "states": {
            "a": { "on": { "ping": { "actions": [{ "type": "@xstate.raise", "event": { "type": "ping" } }] } } }
        }
    }));
    let mut a = Actor::start(m, 0).unwrap();
    assert!(matches!(a.send(json!({ "type": "ping" }), 0), Err(RuntimeError::InfiniteLoop { .. })));
}

#[test]
fn targetless_eventless_transition_without_effect_does_not_loop() {
    let m = machine(json!({
        "initial": "a",
        "states": { "a": { "always": { "actions": [{ "type": "@xstate.emit", "event": { "type": "tick" } }] } } }
    }));
    let mut a = Actor::start(m, 0).unwrap();
    assert_eq!(a.take_emitted(), vec![json!({ "type": "tick" })]);
    send(&mut a, "anything");
    assert_eq!(a.take_emitted(), vec![json!({ "type": "tick" })]);
}

#[test]
fn persist_and_restore_pending_timers() {
    let def = json!({
        "id": "t",
        "version": "1",
        "initial": "a",
        "context": { "n": 1 },
        "states": {
            "a": {
                "entry": [
                    { "type": "@xstate.raise", "event": { "type": "named" }, "delay": 300, "id": "named" },
                    { "type": "@xstate.raise", "event": { "type": "anon", "x": 1 }, "delay": 200 }
                ],
                "after": { "500": { "target": "b" } },
                "on": {
                    "named": { "actions": [{ "type": "@xstate.emit", "event": { "type": "gotNamed" } }] },
                    "anon": { "actions": [{ "type": "@xstate.assign", "context": { "n": 2 } }] }
                }
            },
            "b": {}
        }
    });
    let m = machine(def);
    let a = Actor::start(m.clone(), 1000).unwrap();
    let json = serde_json::to_string(&a.persist()).unwrap();
    let snapshot: PersistedSnapshot<Value> = serde_json::from_str(&json).unwrap();
    assert_eq!(snapshot.timers.len(), 3);
    assert_eq!(snapshot.configuration, vec!["t".to_string(), "t.a".to_string()]);

    let mut b = Actor::restore(m, snapshot).unwrap();
    assert_eq!(b.now(), 1000);
    assert_eq!(b.next_deadline(), Some(1200));
    b.advance(1300).unwrap();
    assert_eq!(b.context(), &json!({ "n": 2 }));
    assert_eq!(b.take_emitted(), vec![json!({ "type": "gotNamed" })]);
    b.advance(1500).unwrap();
    assert_eq!(b.value(), json!("b"));
    assert_eq!(b.next_deadline(), None);
}

#[test]
fn restore_rejects_unknown_states_and_illegal_configurations() {
    let m = machine(json!({ "id": "x", "initial": "a", "states": { "a": {}, "b": {} } }));
    let a = Actor::start(m.clone(), 0).unwrap();
    let mut snap = a.persist();
    snap.configuration = vec!["x".into(), "x.gone".into()];
    assert!(Actor::restore(m.clone(), snap).unwrap_err().0.contains("x.gone"));

    let mut snap = a.persist();
    snap.configuration = vec!["x".into(), "x.a".into(), "x.b".into()];
    assert!(Actor::restore(m.clone(), snap).unwrap_err().0.contains("exactly one active child"));

    let other = machine(json!({ "id": "y", "initial": "a", "states": { "a": {} } }));
    assert!(Actor::restore(other, a.persist()).is_err());
}

#[test]
fn delayed_raise_with_same_id_replaces_the_pending_timer() {
    let m = machine(json!({
        "initial": "a",
        "states": {
            "a": {
                "on": {
                    "arm": { "actions": [{ "type": "@xstate.raise", "event": { "type": "fire" }, "delay": 100, "id": "t" }] },
                    "fire": { "target": "b" }
                }
            },
            "b": {}
        }
    }));
    let mut a = Actor::start(m, 0).unwrap();
    a.send(json!({ "type": "arm" }), 0).unwrap();
    a.send(json!({ "type": "arm" }), 50).unwrap();
    assert_eq!(a.next_deadline(), Some(150));
    assert_eq!(a.advance(149).unwrap(), 0);
    assert_eq!(a.advance(150).unwrap(), 1);
    assert_eq!(a.value(), json!("b"));
}

#[test]
fn internal_events_cannot_be_sent_from_outside() {
    let m = machine(json!({
        "initial": "a",
        "internalEvents": ["secret.*"],
        "states": { "a": { "on": { "secret.open": { "target": "b" } } }, "b": {} }
    }));
    let mut a = Actor::start(m, 0).unwrap();
    assert!(matches!(a.send(json!({ "type": "secret.open" }), 0), Err(RuntimeError::InternalEvent { .. })));
    assert_eq!(a.value(), json!("a"));
}

#[test]
fn stop_cancels_timers_without_exit_actions() {
    let m = machine(json!({
        "initial": "a",
        "states": {
            "a": {
                "exit": [{ "type": "@xstate.emit", "event": { "type": "exited" } }],
                "after": { "100": { "target": "b" } }
            },
            "b": {}
        }
    }));
    let mut a = Actor::start(m, 0).unwrap();
    a.stop();
    assert_eq!(a.status(), Status::Stopped);
    assert_eq!(a.next_deadline(), None);
    assert!(a.take_emitted().is_empty());
    assert!(!send(&mut a, "anything"));
}

#[test]
fn final_root_child_runs_exit_actions_and_clears_timers() {
    let m = machine(json!({
        "initial": "a",
        "exit": [{ "type": "@xstate.emit", "event": { "type": "rootExit" } }],
        "states": {
            "a": {
                "entry": [{ "type": "@xstate.raise", "event": { "type": "late" }, "delay": 1000 }],
                "on": { "end": { "target": "z" } }
            },
            "z": { "type": "final", "exit": [{ "type": "@xstate.emit", "event": { "type": "zExit" } }] }
        }
    }));
    let mut a = Actor::start(m, 0).unwrap();
    assert_eq!(a.next_deadline(), Some(1000));
    send(&mut a, "end");
    assert_eq!(a.status(), Status::Done);
    assert_eq!(a.take_emitted(), vec![json!({ "type": "zExit" }), json!({ "type": "rootExit" })]);
    assert_eq!(a.next_deadline(), None);
}

#[test]
fn logs_are_collected() {
    let m = machine(json!({
        "initial": "a",
        "states": { "a": { "entry": [{ "type": "@xstate.log", "args": ["hello", 1] }] } }
    }));
    let mut a = Actor::start(m, 0).unwrap();
    assert_eq!(a.take_logs(), vec![vec![json!("hello"), json!(1)]]);
}

#[test]
fn guard_args_expose_the_configuration() {
    let mut r: Registry<Value, Value> = Registry::new();
    r.guard("inB", |args| args.in_state("#p.r2.b"));
    let def = json!({
        "id": "p",
        "type": "parallel",
        "states": {
            "r1": { "initial": "x", "states": { "x": { "on": { "go": { "target": "y", "guard": { "type": "inB" } } } }, "y": {} } },
            "r2": { "initial": "a", "states": { "a": { "on": { "flip": { "target": "b" } } }, "b": {} } }
        }
    });
    let mut a = Actor::start(Arc::new(Machine::from_json(&def, &r).unwrap()), 0).unwrap();
    assert!(!send(&mut a, "go"));
    send(&mut a, "flip");
    assert!(send(&mut a, "go"));
    assert_eq!(a.value(), json!({ "r1": "y", "r2": "b" }));
}

// ---- load errors --------------------------------------------------------------

fn load_err(def: Value) -> LoadError {
    match Machine::<Value, Value>::from_json(&def, &Registry::new()) {
        Ok(_) => panic!("expected a load error"),
        Err(e) => e,
    }
}

#[test]
fn rejects_code_with_its_path() {
    let e = load_err(json!({
        "initial": "a",
        "states": { "a": { "on": { "go": { "@code": "() => ({ target: 'b' })", "@lang": "ts" } } }, "b": {} }
    }));
    assert_eq!(e.kind, LoadErrorKind::Code);
    assert_eq!(e.path, "$.states.a.on.go");
}

#[test]
fn rejects_unsupported_features() {
    let e = load_err(json!({ "initial": "a", "states": { "a": { "invoke": { "src": "worker" } } } }));
    assert_eq!((e.kind, e.path.as_str()), (LoadErrorKind::Unsupported, "$.states.a.invoke"));

    let e = load_err(json!({ "initial": "a", "states": { "a": { "type": "choice", "choice": [] } } }));
    assert_eq!(e.kind, LoadErrorKind::Unsupported);

    let e = load_err(json!({ "initial": "a", "states": { "a": { "timeout": 100, "onTimeout": { "target": "a" } } } }));
    assert_eq!(e.kind, LoadErrorKind::Unsupported);

    let e = load_err(json!({ "initial": "a", "states": { "a": { "on": { "go": { "target": "a", "input": {} } } } } }));
    assert_eq!((e.kind, e.path.as_str()), (LoadErrorKind::Unsupported, "$.states.a.on.go.input"));
}

#[test]
fn rejects_invalid_definitions() {
    let e = load_err(json!({ "initial": "a", "states": { "a": { "on": { "go": "b" } }, "b": {} } }));
    assert!(e.message.contains("instead of a string"), "{e}");

    let e = load_err(json!({ "initial": "a", "states": { "a": { "bogus": 1 } } }));
    assert_eq!((e.kind, e.path.as_str()), (LoadErrorKind::UnknownKey, "$.states.a"));

    let e = load_err(json!({ "initial": "a", "states": { "a": { "on": { "go": { "target": "nope" } } } } }));
    assert_eq!((e.kind, e.path.as_str()), (LoadErrorKind::InvalidTarget, "$.states.a.on.go.target"));

    let e = load_err(json!({ "initial": "a", "states": { "a": { "id": "dup" }, "b": { "id": "dup" } } }));
    assert_eq!(e.kind, LoadErrorKind::DuplicateId);

    let e = load_err(json!({ "states": { "a": {} } }));
    assert!(e.message.contains("no initial state"), "{e}");

    let e = load_err(
        json!({ "initial": "a", "states": { "a": { "states": { "h": { "type": "history" } }, "initial": "h" } } }),
    );
    assert!(e.message.contains("history state must declare"), "{e}");

    let e = load_err(json!({
        "type": "parallel",
        "states": { "a": { "on": { "go": { "target": ["c", "c.x"] } } }, "b": {}, "c": { "initial": "x", "states": { "x": {} } } }
    }));
    assert_eq!(e.kind, LoadErrorKind::InvalidTarget, "{e}");
}

#[test]
fn rejects_missing_implementations() {
    let e = load_err(json!({ "initial": "a", "states": { "a": { "on": { "go": { "guard": { "type": "nope" } } } } } }));
    assert_eq!((e.kind, e.path.as_str()), (LoadErrorKind::MissingImplementation, "$.states.a.on.go.guard"));

    let e = load_err(json!({ "initial": "a", "states": { "a": { "entry": [{ "type": "nope" }] } } }));
    assert_eq!((e.kind, e.path.as_str()), (LoadErrorKind::MissingImplementation, "$.states.a.entry[0]"));

    let e = load_err(json!({ "initial": "a", "states": { "a": { "after": { "soon": { "target": "a" } } } } }));
    assert_eq!(e.kind, LoadErrorKind::MissingImplementation);

    let e = load_err(json!({
        "initial": "a",
        "actions": { "loop": { "type": "loop" } },
        "states": { "a": { "entry": [{ "type": "loop" }] } }
    }));
    assert!(e.message.contains("circular action reference"), "{e}");
}

#[test]
fn rejects_bad_params_at_load_time() {
    let mut r: Registry<Value, Value> = Registry::new();
    r.guard_with("below", |_, _p: &u32| true);
    let def = json!({ "initial": "a", "states": { "a": { "on": { "go": { "guard": { "type": "below", "params": "ten" } } } } } });
    let e = Machine::from_json(&def, &r).err().unwrap();
    assert_eq!((e.kind, e.path.as_str()), (LoadErrorKind::InvalidParams, "$.states.a.on.go.guard.params"));
}
