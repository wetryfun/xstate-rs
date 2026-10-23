//! Typed WASI 0.2 components for one machine each: the machine's guards,
//! actions, delays and invoked actors are Rust compiled into the component
//! (no call into the host per guard), and its events are checked against the
//! machine's generated types at the boundary. See `wit/machine.wit` for the
//! interface (world `xstate:machine/machine`).
//!
//! In a `cdylib` crate built for `wasm32-wasip2`, with the machine generated
//! by `xstate-codegen`:
//!
//! ```ignore
//! mod checkout {
//!     xstate_rt::include_machine!("checkout");
//! }
//!
//! struct Shop;
//! impl checkout::CheckoutImpl for Shop { /* guards, actions, delays, actors */ }
//!
//! struct Component;
//! impl xstate_wasi_guest::MachineComponent for Component {
//!     type Context = checkout::Context;
//!     type Event = checkout::Event;
//!     const DEFINITION: &'static str = checkout::DEFINITION;
//!     const SCHEMAS: &'static str = checkout::SCHEMAS;
//!     fn machine() -> Result<xstate_rt::Machine<Self::Context, Self::Event>, String> {
//!         checkout::machine(std::sync::Arc::new(Shop)).map_err(|e| e.to_string())
//!     }
//! }
//! xstate_wasi_guest::export_machine!(Component);
//! ```
//!
//! The generic component (`xstate-wasi`) loads any definition at run time and
//! calls the host for every guard and action; this one is built per machine.
//! JSON crosses the boundary in both: WIT has no maps, no recursive types and
//! no `unknown`, which JSON Schemas of contexts and events use freely, and
//! the events usually arrive as JSON anyway (Kafka records, HTTP bodies).

use std::cell::RefCell;
use std::sync::{Arc, OnceLock};

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Value};
use xstate_rt::{Actor, EventType, InitOptions, Machine, Status};

/// The generated bindings (used by [`export_machine!`]).
#[doc(hidden)]
pub mod bindings {
    wit_bindgen::generate!({
        world: "machine",
        path: "wit",
        pub_export_macro: true,
        export_macro_name: "export_bindings",
        default_bindings_module: "::xstate_wasi_guest::bindings",
    });
}

use bindings::exports::xstate::machine::actors::{Actor as ActorHandle, GuestActor};

/// The machine a component is built for.
pub trait MachineComponent: 'static {
    type Context: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;
    type Event: EventType + Send + Sync + 'static;
    /// The `MachineJSON` definition (codegen's `DEFINITION`).
    const DEFINITION: &'static str;
    /// The bundle's JSON Schemas (codegen's `SCHEMAS`).
    const SCHEMAS: &'static str;
    /// Compiles the machine with its implementations (codegen's `machine`).
    /// Called once per component instance.
    fn machine() -> Result<Machine<Self::Context, Self::Event>, String>;
}

/// A compiled machine, or why it did not compile.
pub type Compiled<T> = Result<Arc<Machine<<T as MachineComponent>::Context, <T as MachineComponent>::Event>>, String>;

/// The compiled machine, shared by the instance's actors. A component
/// instance is single-threaded; the lock is never contended.
#[doc(hidden)]
pub fn machine_of<T: MachineComponent>(cell: &'static OnceLock<Compiled<T>>) -> Compiled<T> {
    cell.get_or_init(|| T::machine().map(Arc::new)).clone()
}

/// Where [`export_machine!`] keeps the compiled machine.
#[doc(hidden)]
pub trait MachineCell: MachineComponent + Sized {
    fn compiled() -> Compiled<Self>;
}

/// Exports `info` and `actors` for a [`MachineComponent`].
#[macro_export]
macro_rules! export_machine {
    ($component:ident) => {
        impl $crate::MachineCell for $component {
            fn compiled() -> $crate::Compiled<$component> {
                static MACHINE: ::std::sync::OnceLock<$crate::Compiled<$component>> = ::std::sync::OnceLock::new();
                $crate::machine_of::<$component>(&MACHINE)
            }
        }

        impl $crate::bindings::exports::xstate::machine::actors::Guest for $component {
            type Actor = $crate::ActorResource<$component>;
        }

        impl $crate::bindings::exports::xstate::machine::info::Guest for $component {
            fn id() -> ::std::string::String {
                $crate::info::<$component>().0
            }
            fn version() -> ::core::option::Option<::std::string::String> {
                $crate::info::<$component>().1
            }
            fn definition() -> ::std::string::String {
                <$component as $crate::MachineComponent>::DEFINITION.into()
            }
            fn schemas() -> ::std::string::String {
                <$component as $crate::MachineComponent>::SCHEMAS.into()
            }
        }

        // The component-model exports only link on wasm32; native builds
        // just type-check the crate.
        #[cfg(target_arch = "wasm32")]
        $crate::bindings::export_bindings!($component with_types_in $crate::bindings);
    };
}

/// The machine's id and version, from its definition.
#[doc(hidden)]
pub fn info<T: MachineComponent>() -> (String, Option<String>) {
    let definition: Value = serde_json::from_str(T::DEFINITION).unwrap_or(Value::Null);
    let id = definition.get("id").and_then(Value::as_str).unwrap_or("(machine)").to_owned();
    (id, definition.get("version").and_then(Value::as_str).map(str::to_owned))
}

fn parse_json(what: &str, text: &str) -> Result<Value, String> {
    serde_json::from_str(text).map_err(|e| format!("invalid {what} JSON: {e}"))
}

fn init_options(text: &str, now: u64) -> Result<InitOptions, String> {
    let mut init = InitOptions { time: now, ..Default::default() };
    if text.trim().is_empty() {
        return Ok(init);
    }
    let Value::Object(mut o) = parse_json("options", text)? else {
        return Err("options must be a JSON object".into());
    };
    init.input = o.remove("input");
    match o.remove("id") {
        None | Some(Value::Null) => {}
        Some(Value::String(s)) => init.id = Some(s),
        Some(_) => return Err("options.id must be a string".into()),
    }
    match o.remove("maxSteps") {
        None | Some(Value::Null) => {}
        Some(v) => init.max_steps = v.as_u64().ok_or("options.maxSteps must be a positive integer")? as usize,
    }
    if let Some(k) = o.keys().next() {
        return Err(format!("unknown option \"{k}\""));
    }
    Ok(init)
}

fn status_str(status: Status) -> &'static str {
    match status {
        Status::Active => "active",
        Status::Done => "done",
        Status::Error => "error",
        Status::Stopped => "stopped",
    }
}

/// The `actor` resource of a [`MachineComponent`].
pub struct ActorResource<T: MachineComponent> {
    actor: RefCell<Actor<T::Context, T::Event>>,
}

impl<T: MachineCell> ActorResource<T> {
    fn with<R>(
        &self,
        f: impl FnOnce(&mut Actor<T::Context, T::Event>) -> Result<R, xstate_rt::SystemError>,
    ) -> Result<R, String> {
        f(&mut self.actor.borrow_mut()).map_err(|e| e.to_string())
    }
}

impl<T: MachineCell> GuestActor for ActorResource<T> {
    fn start(options: String, now: u64) -> Result<ActorHandle, String> {
        let actor = Actor::start_with(T::compiled()?, init_options(&options, now)?).map_err(|e| e.to_string())?;
        Ok(ActorHandle::new(Self { actor: RefCell::new(actor) }))
    }

    fn restore(snapshot: String) -> Result<ActorHandle, String> {
        let snapshot = parse_json("snapshot", &snapshot)?;
        let actor = Actor::restore(T::compiled()?, &snapshot).map_err(|e| e.to_string())?;
        Ok(ActorHandle::new(Self { actor: RefCell::new(actor) }))
    }

    fn send(&self, event: String, now: u64) -> Result<(), String> {
        let event = parse_json("event", &event)?;
        // Checked against the machine's event types (an untyped machine takes
        // any object with a string `type`).
        if !event.get("type").is_some_and(Value::is_string) {
            return Err("an event must be an object with a string \"type\"".into());
        }
        let event = T::Event::from_json(event).map_err(|e| format!("not an event of this machine: {e}"))?;
        self.with(|a| a.send(event, now))
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
        let mut context = serde_json::to_value(a.context()).unwrap_or(Value::Null);
        xstate_rt::js_numbers(&mut context);
        let mut s = json!({ "status": status_str(a.status()), "value": a.value(), "context": context });
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
