//! Builds the typed checkout component (`examples/checkout-component`,
//! world `xstate:machine/machine`) and drives it from Wasmtime. Its guards,
//! actions and delay run inside the component: the host imports nothing but
//! WASI, and sees the actions only on the component's stderr.

use std::path::PathBuf;
use std::sync::OnceLock;

use serde_json::{json, Value};
use wasmtime::component::{Component, Linker, ResourceAny, ResourceTable};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::p2::pipe::MemoryOutputPipe;
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

wasmtime::component::bindgen!({ path: "../xstate-wasi-guest/wit", world: "machine" });

fn component_path() -> &'static PathBuf {
    static PATH: OnceLock<PathBuf> = OnceLock::new();
    PATH.get_or_init(|| {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let target_dir = root.join("target/wasi");
        let status = std::process::Command::new(env!("CARGO"))
            .args(["build", "--quiet", "--release", "-p", "checkout-component", "--target", "wasm32-wasip2"])
            .arg("--target-dir")
            .arg(&target_dir)
            .current_dir(&root)
            .status()
            .expect("cargo runs");
        assert!(status.success(), "building the component failed");
        target_dir.join("wasm32-wasip2/release/checkout_component.wasm")
    })
}

struct Host {
    wasi: WasiCtx,
    table: ResourceTable,
}

impl WasiView for Host {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView { ctx: &mut self.wasi, table: &mut self.table }
    }
}

struct Rt {
    store: Store<Host>,
    world: Machine,
    stderr: MemoryOutputPipe,
}

impl Rt {
    fn new() -> Self {
        let engine = Engine::new(&Config::new()).unwrap();
        let component = Component::from_file(&engine, component_path()).expect("component loads");
        let mut linker: Linker<Host> = Linker::new(&engine);
        wasmtime_wasi::p2::add_to_linker_sync(&mut linker).unwrap();
        let stderr = MemoryOutputPipe::new(1 << 16);
        let host = Host { wasi: WasiCtx::builder().stderr(stderr.clone()).build(), table: ResourceTable::new() };
        let mut store = Store::new(&engine, host);
        let world = Machine::instantiate(&mut store, &component, &linker).expect("instantiates");
        Rt { store, world, stderr }
    }

    /// Lines the component wrote to stderr so far.
    fn stderr(&self) -> Vec<String> {
        String::from_utf8_lossy(&self.stderr.contents()).lines().map(str::to_owned).collect()
    }

    fn start(&mut self, options: &Value, now: u64) -> Result<ResourceAny, String> {
        let actor = self.world.xstate_machine_actors().actor();
        actor.call_start(&mut self.store, &options.to_string(), now).unwrap()
    }

    fn restore(&mut self, snapshot: &str) -> Result<ResourceAny, String> {
        let actor = self.world.xstate_machine_actors().actor();
        actor.call_restore(&mut self.store, snapshot).unwrap()
    }

    fn send(&mut self, a: ResourceAny, event: Value, now: u64) -> Result<(), String> {
        let actor = self.world.xstate_machine_actors().actor();
        actor.call_send(&mut self.store, a, &event.to_string(), now).unwrap()
    }

    fn advance(&mut self, a: ResourceAny, now: u64) -> Result<(), String> {
        let actor = self.world.xstate_machine_actors().actor();
        actor.call_advance(&mut self.store, a, now).unwrap()
    }

    fn next_deadline(&mut self, a: ResourceAny) -> Option<u64> {
        let actor = self.world.xstate_machine_actors().actor();
        actor.call_next_deadline(&mut self.store, a).unwrap()
    }

    fn snapshot(&mut self, a: ResourceAny) -> Value {
        let actor = self.world.xstate_machine_actors().actor();
        serde_json::from_str(&actor.call_snapshot(&mut self.store, a).unwrap()).unwrap()
    }

    fn persist(&mut self, a: ResourceAny) -> String {
        let actor = self.world.xstate_machine_actors().actor();
        actor.call_persist(&mut self.store, a).unwrap()
    }

    fn take_emitted(&mut self, a: ResourceAny) -> Vec<Value> {
        let actor = self.world.xstate_machine_actors().actor();
        let emitted = actor.call_take_emitted(&mut self.store, a).unwrap();
        emitted.iter().map(|e| serde_json::from_str(e).unwrap()).collect()
    }

    fn pending_effects(&mut self, a: ResourceAny) -> Vec<Value> {
        let actor = self.world.xstate_machine_actors().actor();
        let effects = actor.call_pending_effects(&mut self.store, a).unwrap();
        effects.iter().map(|e| serde_json::from_str(e).unwrap()).collect()
    }

    fn resolve_effect(&mut self, a: ResourceAny, id: &str, event: Value) -> Result<(), String> {
        let actor = self.world.xstate_machine_actors().actor();
        actor.call_resolve_effect(&mut self.store, a, id, Some(&event.to_string())).unwrap()
    }

    /// The id of the one pending card charge.
    fn charge(&mut self, a: ResourceAny) -> String {
        let effects = self.pending_effects(a);
        assert_eq!(effects.len(), 1, "{effects:?}");
        assert_eq!(effects[0]["type"], json!("@xstate.effect"));
        effects[0]["id"].as_str().unwrap().to_owned()
    }
}

#[test]
fn describes_its_machine() {
    let mut rt = Rt::new();
    let info = rt.world.xstate_machine_info();
    assert_eq!(info.call_id(&mut rt.store).unwrap(), "checkout");
    assert_eq!(info.call_version(&mut rt.store).unwrap(), None);
    let definition: Value = serde_json::from_str(&info.call_definition(&mut rt.store).unwrap()).unwrap();
    assert_eq!(definition["initial"], json!("cart"));
    let schemas: Value = serde_json::from_str(&info.call_schemas(&mut rt.store).unwrap()).unwrap();
    assert!(schemas["events"]["checkout"].is_object(), "{schemas}");
}

#[test]
fn guards_and_actions_run_inside_the_component() {
    let mut rt = Rt::new();
    let a = rt.start(&json!({}), 0).unwrap();
    assert_eq!(rt.snapshot(a)["value"], json!("cart"));

    // Events are checked against the machine's types.
    let err = rt.send(a, json!({ "type": "checkout", "total": "lots" }), 1).unwrap_err();
    assert!(err.contains("not an event of this machine"), "{err}");
    assert!(rt.send(a, json!({ "type": "nope" }), 1).is_err());

    // Over the limit: the guard (in the component) fails, and the fallback
    // transition's action writes to stderr.
    rt.send(a, json!({ "type": "checkout", "total": 5000 }), 5).unwrap();
    assert_eq!(rt.snapshot(a)["value"], json!("cart"));
    assert_eq!(rt.stderr(), ["notify Email over-limit"]);

    rt.send(a, json!({ "type": "payment.method", "method": { "kind": "card", "last4": "4242" } }), 8).unwrap();
    rt.send(a, json!({ "type": "checkout", "total": 120 }), 10).unwrap();
    assert_eq!(rt.snapshot(a)["value"], json!("paying"));
    assert_eq!(rt.snapshot(a)["context"]["status"], json!("paying"));
    assert_eq!(rt.stderr()[1..], ["audit"]);

    // A retryable failure (the `retryable` guard reads the error) waits the
    // component's backoff delay (250 ms for an empty cart), then retries.
    let charge = rt.charge(a);
    let error = json!({ "type": "xstate.async.reject", "data": { "retryable": true } });
    rt.resolve_effect(a, &charge, error).unwrap();
    assert_eq!(rt.snapshot(a)["value"], json!("retrying"));
    assert_eq!(rt.next_deadline(a), Some(260));
    rt.advance(a, 260).unwrap();
    assert_eq!(rt.snapshot(a)["value"], json!("paying"));

    // Persisted mid-charge and restored: the charge is still pending.
    let persisted = rt.persist(a);
    let b = rt.restore(&persisted).unwrap();
    let charge = rt.charge(b);
    rt.resolve_effect(b, &charge, json!({ "type": "xstate.async.resolve", "data": { "id": "ch_1" } })).unwrap();
    let snapshot = rt.snapshot(b);
    assert_eq!((&snapshot["status"], &snapshot["output"]), (&json!("done"), &json!({ "paid": true })));
    assert_eq!(rt.take_emitted(b), vec![json!({ "type": "order.paid", "channel": "sms" })]);
    assert_eq!(rt.stderr().last().map(String::as_str), Some("notify Sms paid"));
}

#[test]
fn a_typed_component_is_about_a_megabyte() {
    let size = std::fs::metadata(component_path()).unwrap().len();
    assert!(size < 2 << 20, "{size} bytes");
}
