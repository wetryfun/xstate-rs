//! Builds the `xstate-wasi` component and drives it through its WIT
//! interface from a Wasmtime host.

use std::path::PathBuf;
use std::sync::OnceLock;

use serde_json::{json, Value};
use wasmtime::component::{Component, Linker, ResourceAny, ResourceTable};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

wasmtime::component::bindgen!({ path: "../xstate-wasi/wit", world: "runtime" });

/// Builds the component once per test run (in its own target directory, so
/// it does not wait on the lock of the `cargo test` that runs this).
fn component_path() -> &'static PathBuf {
    static PATH: OnceLock<PathBuf> = OnceLock::new();
    PATH.get_or_init(|| {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let target_dir = root.join("target/wasi");
        let status = std::process::Command::new(env!("CARGO"))
            .args(["build", "--quiet", "--release", "-p", "xstate-wasi", "--target", "wasm32-wasip2", "--target-dir"])
            .arg(&target_dir)
            .current_dir(&root)
            .status()
            .expect("cargo runs");
        assert!(status.success(), "building the component failed");
        target_dir.join("wasm32-wasip2/release/xstate_wasi.wasm")
    })
}

/// The host: implements the component's imports and records what it saw.
struct Host {
    wasi: WasiCtx,
    table: ResourceTable,
    actions: Vec<(String, Option<Value>)>,
    guard_calls: Vec<(String, Option<Value>, Value, Value)>,
}

impl WasiView for Host {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView { ctx: &mut self.wasi, table: &mut self.table }
    }
}

impl xstate::runtime::implementations::Host for Host {
    fn guard(&mut self, name: String, params: Option<String>, context: String, event: String) -> bool {
        let params = params.map(|p| serde_json::from_str::<Value>(&p).unwrap());
        let context: Value = serde_json::from_str(&context).unwrap();
        let event: Value = serde_json::from_str(&event).unwrap();
        self.guard_calls.push((name.clone(), params.clone(), context.clone(), event.clone()));
        match name.as_str() {
            "atLeast" => event["n"].as_f64() >= params.as_ref().and_then(|p| p["min"].as_f64()),
            "retryable" => event["error"]["retryable"] == json!(true),
            other => panic!("unexpected guard {other}"),
        }
    }

    fn action(&mut self, name: String, params: Option<String>) {
        self.actions.push((name, params.map(|p| serde_json::from_str(&p).unwrap())));
    }

    fn delay(&mut self, name: String, context: String, _event: String) -> u64 {
        assert_eq!(name, "backoff");
        let context: Value = serde_json::from_str(&context).unwrap();
        100 * (1 + context["tries"].as_u64().unwrap_or(0))
    }

    fn expr(&mut self, name: String, scope: String) -> String {
        let scope: Value = serde_json::from_str(&scope).unwrap();
        match name.as_str() {
            "nextTries" => json!(scope["context"]["tries"].as_u64().unwrap() + 1),
            "carEvent" => json!({ "type": "seen", "n": scope["event"]["n"], "label": scope["params"]["label"] }),
            other => panic!("unexpected expression {other}"),
        }
        .to_string()
    }

    fn context_expr(&mut self, name: String, input: Option<String>) -> String {
        assert_eq!(name, "fromInput");
        let input: Value = input.map_or(Value::Null, |i| serde_json::from_str(&i).unwrap());
        json!({ "tries": input["tries"].as_u64().unwrap_or(0) }).to_string()
    }
}

/// A component instance with its store.
struct Rt {
    store: Store<Host>,
    world: Runtime,
}

fn engine() -> &'static Engine {
    static ENGINE: OnceLock<Engine> = OnceLock::new();
    ENGINE.get_or_init(|| Engine::new(&Config::new()).unwrap())
}

impl Rt {
    fn new() -> Self {
        let engine = engine();
        let component = Component::from_file(engine, component_path()).expect("component loads");
        let mut linker: Linker<Host> = Linker::new(engine);
        wasmtime_wasi::p2::add_to_linker_sync(&mut linker).unwrap();
        xstate::runtime::implementations::add_to_linker::<Host, wasmtime::component::HasSelf<Host>>(&mut linker, |h| h)
            .unwrap();
        let host = Host {
            wasi: WasiCtx::builder().build(),
            table: ResourceTable::new(),
            actions: Vec::new(),
            guard_calls: Vec::new(),
        };
        let mut store = Store::new(engine, host);
        let world = Runtime::instantiate(&mut store, &component, &linker).expect("instantiates");
        Rt { store, world }
    }

    fn start(&mut self, definition: &Value, options: &Value, now: u64) -> Result<ResourceAny, String> {
        let (d, o) = (definition.to_string(), options.to_string());
        let actor = self.world.xstate_runtime_actors().actor();
        actor.call_start(&mut self.store, &d, &o, now).unwrap()
    }

    fn restore(&mut self, definition: &Value, options: &Value, snapshot: &str) -> Result<ResourceAny, String> {
        let (d, o) = (definition.to_string(), options.to_string());
        let actor = self.world.xstate_runtime_actors().actor();
        actor.call_restore(&mut self.store, &d, &o, snapshot).unwrap()
    }

    fn send(&mut self, a: ResourceAny, event: Value, now: u64) -> Result<(), String> {
        let actor = self.world.xstate_runtime_actors().actor();
        actor.call_send(&mut self.store, a, &event.to_string(), now).unwrap()
    }

    fn send_to(&mut self, a: ResourceAny, address: &str, event: Value, now: u64) -> Result<(), String> {
        let actor = self.world.xstate_runtime_actors().actor();
        actor.call_send_to(&mut self.store, a, address, &event.to_string(), now).unwrap()
    }

    fn advance(&mut self, a: ResourceAny, now: u64) -> Result<(), String> {
        let actor = self.world.xstate_runtime_actors().actor();
        actor.call_advance(&mut self.store, a, now).unwrap()
    }

    fn next_deadline(&mut self, a: ResourceAny) -> Option<u64> {
        let actor = self.world.xstate_runtime_actors().actor();
        actor.call_next_deadline(&mut self.store, a).unwrap()
    }

    fn snapshot(&mut self, a: ResourceAny) -> Value {
        let actor = self.world.xstate_runtime_actors().actor();
        serde_json::from_str(&actor.call_snapshot(&mut self.store, a).unwrap()).unwrap()
    }

    fn persist(&mut self, a: ResourceAny) -> String {
        let actor = self.world.xstate_runtime_actors().actor();
        actor.call_persist(&mut self.store, a).unwrap()
    }

    fn take_emitted(&mut self, a: ResourceAny) -> Vec<Value> {
        let actor = self.world.xstate_runtime_actors().actor();
        let emitted = actor.call_take_emitted(&mut self.store, a).unwrap();
        emitted.iter().map(|e| serde_json::from_str(e).unwrap()).collect()
    }

    fn take_logs(&mut self, a: ResourceAny) -> Vec<Value> {
        let actor = self.world.xstate_runtime_actors().actor();
        let logs = actor.call_take_logs(&mut self.store, a).unwrap();
        logs.iter().map(|e| serde_json::from_str(e).unwrap()).collect()
    }

    fn pending_effects(&mut self, a: ResourceAny) -> Vec<Value> {
        let actor = self.world.xstate_runtime_actors().actor();
        let effects = actor.call_pending_effects(&mut self.store, a).unwrap();
        effects.iter().map(|e| serde_json::from_str(e).unwrap()).collect()
    }

    fn resolve_effect(&mut self, a: ResourceAny, id: &str, event: Option<Value>) -> Result<(), String> {
        let actor = self.world.xstate_runtime_actors().actor();
        let event = event.map(|e| e.to_string());
        actor.call_resolve_effect(&mut self.store, a, id, event.as_deref()).unwrap()
    }

    fn stop(&mut self, a: ResourceAny) -> Result<(), String> {
        let actor = self.world.xstate_runtime_actors().actor();
        actor.call_stop(&mut self.store, a).unwrap()
    }
}

fn light() -> Value {
    json!({
        "id": "light",
        "initial": "green",
        "context": { "tries": 0 },
        "states": {
            "green": {
                "on": {
                    "car": [
                        { "target": "yellow", "guard": { "type": "atLeast", "params": { "min": 3 } } },
                        { "actions": [{ "type": "honk", "params": "beep" }, { "type": "count" }] }
                    ]
                }
            },
            "yellow": {
                "entry": [{ "type": "@xstate.log", "args": ["yellow"] }],
                "after": { "backoff": { "target": "red", "context": { "tries": 1 } } }
            },
            "red": {
                "entry": [{ "type": "@xstate.emit", "event": { "type": "stopped", "at": "red" } }],
                "on": { "go": { "target": "done" } }
            },
            "done": { "type": "final", "output": { "ok": true } }
        }
    })
}

#[test]
fn guards_actions_and_delays_come_from_the_host() {
    let mut rt = Rt::new();
    let a = rt.start(&light(), &json!({}), 0).unwrap();
    assert_eq!(rt.snapshot(a), json!({ "status": "active", "value": "green", "context": { "tries": 0 } }));

    rt.send(a, json!({ "type": "car", "n": 1 }), 10).unwrap();
    assert_eq!(rt.store.data().actions, vec![("honk".into(), Some(json!("beep"))), ("count".into(), None)]);
    let (name, params, context, event) = rt.store.data().guard_calls[0].clone();
    assert_eq!(
        (name.as_str(), params, context, event),
        ("atLeast", Some(json!({ "min": 3 })), json!({ "tries": 0 }), json!({ "type": "car", "n": 1 }))
    );

    rt.send(a, json!({ "type": "car", "n": 5 }), 20).unwrap();
    assert_eq!(rt.snapshot(a)["value"], json!("yellow"));
    assert_eq!(rt.take_logs(a), vec![json!(["yellow"])]);
    // `backoff` came from the host: 100 * (1 + tries).
    assert_eq!(rt.next_deadline(a), Some(120));
    rt.advance(a, 119).unwrap();
    assert_eq!(rt.snapshot(a)["value"], json!("yellow"));
    rt.advance(a, 120).unwrap();
    assert_eq!(rt.snapshot(a)["value"], json!("red"));
    assert_eq!(rt.snapshot(a)["context"], json!({ "tries": 1 }));
    assert_eq!(rt.take_emitted(a), vec![json!({ "type": "stopped", "at": "red" })]);
    assert_eq!(rt.take_emitted(a), Vec::<Value>::new());

    rt.send(a, json!({ "type": "go" }), 200).unwrap();
    assert_eq!(
        rt.snapshot(a),
        json!({ "status": "done", "value": "done", "context": { "tries": 1 }, "output": { "ok": true } })
    );
}

#[test]
fn expressions_come_from_the_host() {
    let code = |name: &str| json!({ "@code": name, "@lang": "rust" });
    let definition = json!({
        "id": "counted",
        "context": code("fromInput"),
        "actions": { "report": [{ "type": "@xstate.emit", "event": code("carEvent") }] },
        "initial": "a",
        "states": { "a": { "on": { "car": {
            "context": { "tries": code("nextTries") },
            "actions": [{ "type": "report", "params": { "label": "car" } }]
        } } } }
    });
    let mut rt = Rt::new();
    let a = rt.start(&definition, &json!({ "input": { "tries": 4 } }), 0).unwrap();
    assert_eq!(rt.snapshot(a)["context"], json!({ "tries": 4 }));
    rt.send(a, json!({ "type": "car", "n": 2 }), 1).unwrap();
    assert_eq!(rt.snapshot(a)["context"], json!({ "tries": 5 }));
    assert_eq!(rt.take_emitted(a), vec![json!({ "type": "seen", "n": 2, "label": "car" })]);
}

#[test]
fn persist_and_restore_across_instances() {
    let mut rt = Rt::new();
    let a = rt.start(&light(), &json!({}), 0).unwrap();
    rt.send(a, json!({ "type": "car", "n": 9 }), 50).unwrap();
    let saved = rt.persist(a);

    // A fresh component instance picks up the pending timer.
    let mut other = Rt::new();
    let b = other.restore(&light(), &json!({}), &saved).unwrap();
    assert_eq!(other.snapshot(b)["value"], json!("yellow"));
    assert_eq!(other.next_deadline(b), Some(150));
    assert_eq!(other.persist(b), saved);
    other.advance(b, 150).unwrap();
    assert_eq!(other.snapshot(b)["value"], json!("red"));
}

#[test]
fn invoked_work_is_done_by_the_host_or_by_child_machines() {
    let definition = json!({
        "id": "order",
        "initial": "charging",
        "context": { "tries": 0 },
        "states": {
            "charging": {
                "invoke": {
                    "src": "charge",
                    "onDone": { "target": "shipping" },
                    "onError": [{ "target": "charging", "reenter": true, "guard": { "type": "retryable" } }, { "target": "failed" }]
                }
            },
            "shipping": {
                "invoke": { "id": "courier", "src": "courier", "onDone": { "target": "delivered" } }
            },
            "delivered": { "type": "final" },
            "failed": { "type": "final" }
        }
    });
    let courier = json!({
        "id": "courier",
        "initial": "driving",
        "states": {
            "driving": { "on": { "arrive": { "target": "arrived" } } },
            "arrived": { "type": "final", "entry": [{ "type": "notify", "params": { "to": "customer" } }] }
        }
    });
    let mut rt = Rt::new();
    let a = rt.start(&definition, &json!({ "children": { "courier": courier } }), 0).unwrap();

    let effects = rt.pending_effects(a);
    assert_eq!(effects.len(), 1);
    assert_eq!(effects[0]["type"], json!("@xstate.effect"));
    let id = effects[0]["id"].as_str().unwrap().to_owned();
    let reject = json!({ "type": "xstate.async.reject", "data": { "retryable": true } });
    rt.resolve_effect(a, &id, Some(reject)).unwrap();
    assert_eq!(rt.snapshot(a)["value"], json!("charging"), "retried");

    let id = rt.pending_effects(a)[0]["id"].as_str().unwrap().to_owned();
    assert!(rt.resolve_effect(a, "system:999", None).is_err());
    rt.resolve_effect(a, &id, Some(json!({ "type": "xstate.async.resolve", "data": { "receipt": 1 } }))).unwrap();
    assert_eq!(rt.snapshot(a)["value"], json!("shipping"));

    // The child machine lives in the same system; its custom action goes to
    // the host like the root's, and its completion moves the parent on.
    let system: Value = serde_json::from_str(&rt.persist(a)).unwrap();
    assert!(system["actors"].get("order/courier").is_some(), "{system}");
    assert!(rt.send_to(a, "order/nobody", json!({ "type": "arrive" }), 10).is_err());
    rt.send_to(a, "order/courier", json!({ "type": "arrive" }), 10).unwrap();
    assert_eq!(rt.store.data().actions, vec![("notify".into(), Some(json!({ "to": "customer" })))]);
    assert_eq!(rt.snapshot(a)["status"], json!("done"));
    assert_eq!(rt.snapshot(a)["value"], json!("delivered"));
    assert!(rt.stop(a).is_ok());
}

#[test]
fn errors_are_reported_as_strings() {
    let mut rt = Rt::new();
    let code =
        json!({ "id": "x", "initial": "a", "states": { "a": { "entry": { "@code": "() => {}", "@lang": "ts" } } } });
    let err = rt.start(&code, &json!({}), 0).unwrap_err();
    assert!(err.contains("@code"), "{err}");
    let err = rt.start(&light(), &json!({ "bogus": 1 }), 0).unwrap_err();
    assert!(err.contains("unknown option"), "{err}");

    let a = rt.start(&light(), &json!({}), 0).unwrap();
    assert!(rt.send(a, json!({ "no": "type" }), 0).unwrap_err().contains("type"));
    assert!(rt.advance(a, 0).is_ok());
    let looping = json!({
        "id": "loop", "initial": "a",
        "states": { "a": { "on": { "go": { "actions": [{ "type": "@xstate.raise", "event": { "type": "go" } }] } } } }
    });
    let b = rt.start(&looping, &json!({}), 0).unwrap();
    let err = rt.send(b, json!({ "type": "go" }), 0).unwrap_err();
    assert!(err.contains("infinite loop"), "{err}");
    assert_eq!(rt.snapshot(b)["status"], json!("error"));
}
