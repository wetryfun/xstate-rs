//! A WASI 0.2 component (`wasm32-wasip2`) that runs XState v6 machine
//! definitions with `xstate-rt`. See `wit/xstate.wit` for the interface.
//!
//! The component is generic: it loads any `MachineJSON` at run time. Custom
//! guards, actions and named delays are imports the host implements; child
//! machines for `invoke` come with the start options, and any other invoke
//! source is async work the host completes through `resolve-effect`.
//!
//! Build it with `cargo build -p xstate-wasi --target wasm32-wasip2 --release`.

use std::cell::RefCell;
use std::collections::HashSet;
use std::sync::Arc;

use serde_json::{json, Map, Value};
use xstate_rt::step::async_logic;
use xstate_rt::{Actor as RtActor, InitOptions, Logic, Machine, Registry, Status};

wit_bindgen::generate!({ world: "runtime", path: "wit" });

use exports::xstate::runtime::actors::{Actor as ActorHandle, Guest, GuestActor};
use xstate::runtime::implementations as host;

struct Component;

// The component-model exports only make sense (and only link) on wasm32;
// native builds just type-check the crate.
#[cfg(target_arch = "wasm32")]
export!(Component);

impl Guest for Component {
    type Actor = ActorResource;
}

/// Children may invoke children; a cycle of child definitions is an error.
const MAX_CHILD_DEPTH: usize = 32;

#[derive(Default)]
struct Options {
    input: Option<Value>,
    id: Option<String>,
    children: Map<String, Value>,
    max_steps: Option<usize>,
}

fn parse_json(what: &str, text: &str) -> Result<Value, String> {
    serde_json::from_str(text).map_err(|e| format!("invalid {what} JSON: {e}"))
}

fn parse_options(text: &str) -> Result<Options, String> {
    if text.trim().is_empty() {
        return Ok(Options::default());
    }
    let Value::Object(mut o) = parse_json("options", text)? else {
        return Err("options must be a JSON object".into());
    };
    let options = Options {
        input: o.remove("input"),
        id: match o.remove("id") {
            None | Some(Value::Null) => None,
            Some(Value::String(s)) => Some(s),
            Some(_) => return Err("options.id must be a string".into()),
        },
        children: match o.remove("children") {
            None | Some(Value::Null) => Map::new(),
            Some(Value::Object(c)) => c,
            Some(_) => return Err("options.children must map invoke sources to machine definitions".into()),
        },
        max_steps: match o.remove("maxSteps") {
            None | Some(Value::Null) => None,
            Some(v) => Some(v.as_u64().ok_or("options.maxSteps must be a positive integer")? as usize),
        },
    };
    if let Some(k) = o.keys().next() {
        return Err(format!("unknown option \"{k}\""));
    }
    Ok(options)
}

/// `params` as the host sees them: absent when the reference has none.
fn params_json(p: &Value) -> Option<String> {
    (!p.is_null()).then(|| p.to_string())
}

/// Compiles a definition, wiring every name it references to the host and
/// every invoke source to a child machine or to host async work.
fn compile(definition: &Value, children: &Map<String, Value>, depth: usize) -> Result<Machine<Value, Value>, String> {
    if depth > MAX_CHILD_DEPTH {
        return Err("child machines are nested too deeply (a child invokes itself?)".into());
    }
    let refs = Machine::<Value, Value>::references(definition).map_err(|e| e.to_string())?;
    let mut registry: Registry<Value, Value> = Registry::new();
    let mut seen = HashSet::new();
    for r in &refs.guards {
        if seen.insert(("guard", r.name.clone())) {
            let name = r.name.clone();
            registry.guard_with(r.name.clone(), move |args, p: &Value| {
                host::guard(
                    &name,
                    params_json(p).as_deref(),
                    &args.context.to_string(),
                    &args.event.to_json().to_string(),
                )
            });
        }
    }
    for r in &refs.actions {
        if seen.insert(("action", r.name.clone())) {
            let name = r.name.clone();
            registry.action_with(r.name.clone(), move |p: &Value| host::action(&name, params_json(p).as_deref()));
        }
    }
    for r in &refs.delays {
        if seen.insert(("delay", r.name.clone())) {
            let name = r.name.clone();
            registry.delay(r.name.clone(), move |context, event| {
                host::delay(&name, &context.to_string(), &event.to_json().to_string())
            });
        }
    }
    for r in &refs.actors {
        if seen.insert(("actor", r.name.clone())) {
            let logic: Arc<dyn Logic> = match children.get(&r.name) {
                Some(child) => {
                    Arc::new(compile(child, children, depth + 1).map_err(|e| format!("child \"{}\": {e}", r.name))?)
                }
                None => Arc::new(async_logic(Some(r.name.clone()), None).map_err(|e| e.to_string())?),
            };
            registry.actor(r.name.clone(), logic);
        }
    }
    Machine::from_json(definition, &registry).map_err(|e| e.to_string())
}

fn load(definition: &str, options: &str) -> Result<(Arc<Machine<Value, Value>>, Options), String> {
    let options = parse_options(options)?;
    let definition = parse_json("definition", definition)?;
    Ok((Arc::new(compile(&definition, &options.children, 0)?), options))
}

fn status_str(status: Status) -> &'static str {
    match status {
        Status::Active => "active",
        Status::Done => "done",
        Status::Error => "error",
        Status::Stopped => "stopped",
    }
}

/// The `actor` resource. Calls into the host (guards, actions, delays) happen
/// while the actor is borrowed, so the host must not call back into the same
/// actor from them.
pub struct ActorResource {
    actor: RefCell<RtActor<Value, Value>>,
}

impl ActorResource {
    fn with<T>(
        &self,
        f: impl FnOnce(&mut RtActor<Value, Value>) -> Result<T, xstate_rt::SystemError>,
    ) -> Result<T, String> {
        f(&mut self.actor.borrow_mut()).map_err(|e| e.to_string())
    }
}

impl GuestActor for ActorResource {
    fn start(definition: String, options: String, now: u64) -> Result<ActorHandle, String> {
        let (machine, options) = load(&definition, &options)?;
        let init = InitOptions {
            input: options.input,
            id: options.id,
            time: now,
            max_steps: options.max_steps.unwrap_or(InitOptions::default().max_steps),
            ..Default::default()
        };
        let actor = RtActor::start_with(machine, init).map_err(|e| e.to_string())?;
        Ok(ActorHandle::new(ActorResource { actor: RefCell::new(actor) }))
    }

    fn restore(definition: String, options: String, snapshot: String) -> Result<ActorHandle, String> {
        let (machine, _) = load(&definition, &options)?;
        let snapshot = parse_json("snapshot", &snapshot)?;
        let actor = RtActor::restore(machine, &snapshot).map_err(|e| e.to_string())?;
        Ok(ActorHandle::new(ActorResource { actor: RefCell::new(actor) }))
    }

    fn send(&self, event: String, now: u64) -> Result<(), String> {
        let event = parse_json("event", &event)?;
        if !event.get("type").is_some_and(Value::is_string) {
            return Err("an event must be an object with a string \"type\"".into());
        }
        self.with(|a| a.send_json(event, now))
    }

    fn send_to(&self, address: String, event: String, now: u64) -> Result<(), String> {
        let event = parse_json("event", &event)?;
        if !event.get("type").is_some_and(Value::is_string) {
            return Err("an event must be an object with a string \"type\"".into());
        }
        self.with(|a| a.send_to(&address, event, now))
    }

    fn advance(&self, now: u64) -> Result<(), String> {
        self.with(|a| a.advance(now))
    }

    fn next_deadline(&self) -> Option<u64> {
        self.actor.borrow().next_deadline()
    }

    fn snapshot(&self) -> String {
        let a = self.actor.borrow();
        let mut s = json!({
            "status": status_str(a.status()),
            "value": a.value(),
            "context": a.context(),
        });
        if let Some(output) = a.output() {
            s["output"] = output.clone();
        }
        if let Some(error) = a.error() {
            s["error"] = error.clone();
        }
        s.to_string()
    }

    fn persist(&self) -> String {
        self.actor.borrow().persist().to_string()
    }

    fn take_emitted(&self) -> Vec<String> {
        self.actor.borrow_mut().take_emitted().iter().map(Value::to_string).collect()
    }

    fn take_logs(&self) -> Vec<String> {
        self.actor.borrow_mut().take_logs().into_iter().map(|args| Value::Array(args).to_string()).collect()
    }

    fn pending_effects(&self) -> Vec<String> {
        self.actor.borrow().pending_effects().iter().map(|e| e.to_json().to_string()).collect()
    }

    fn resolve_effect(&self, id: String, event: Option<String>) -> Result<(), String> {
        let event = event.map(|e| parse_json("event", &e)).transpose()?;
        self.with(|a| a.resolve_effect(&id, event))
    }

    fn stop(&self) -> Result<(), String> {
        self.with(|a| a.stop())
    }
}
