//! `xstate-rt` runs XState v6 machine definitions, exported as JSON with
//! `machineToJSON` / `machineConfigToJSON`, with guards, actions and delays
//! implemented in Rust.
//!
//! ```
//! use std::sync::Arc;
//! use serde_json::{json, Value};
//! use xstate_rt::{Actor, Machine, Registry};
//!
//! let definition = json!({
//!     "id": "light",
//!     "initial": "green",
//!     "states": {
//!         "green": { "after": { "1000": { "target": "yellow" } } },
//!         "yellow": { "on": { "next": { "target": "red" } } },
//!         "red": { "type": "final" }
//!     }
//! });
//! let machine: Machine<Value, Value> = Machine::from_json(&definition, &Registry::new()).unwrap();
//! let mut actor = Actor::start(Arc::new(machine), 0).unwrap();
//! assert_eq!(actor.next_deadline(), Some(1000));
//! actor.advance(1000).unwrap();
//! actor.send(json!({ "type": "next" }), 1500).unwrap();
//! assert_eq!(actor.value(), json!("red"));
//! ```

mod actor;
mod delay;
mod error;
mod event;
mod interp;
mod load;
mod machine;
mod registry;
mod snapshot;

use serde::de::DeserializeOwned;
use serde_json::Value;

pub use actor::Actor;
pub use error::{LoadError, LoadErrorKind, RestoreError, RuntimeError};
pub use event::{Ev, EventType};
pub use interp::Status;
pub use machine::{Machine, MachineOptions, NodeId};
pub use registry::{ActionArgs, GuardArgs, Registry};
pub use snapshot::{PersistedSnapshot, PersistedTimer, PersistedTimerKey};

impl<C, E> Machine<C, E>
where
    C: DeserializeOwned,
    E: EventType,
{
    /// Compiles a `MachineJSON` definition. Every referenced guard, action and
    /// named delay must be in `registry` (or defined in the JSON itself).
    pub fn from_json(json: &Value, registry: &Registry<C, E>) -> Result<Self, LoadError> {
        load::load(json, registry, MachineOptions::default())
    }

    /// Compiles a definition with non-default options.
    pub fn from_json_with_options(
        json: &Value,
        registry: &Registry<C, E>,
        options: MachineOptions,
    ) -> Result<Self, LoadError> {
        load::load(json, registry, options)
    }

    /// Parses and compiles a definition from a JSON string. Key order is
    /// preserved, which transition priority depends on.
    pub fn from_json_str(json: &str, registry: &Registry<C, E>) -> Result<Self, LoadError> {
        let value: Value = serde_json::from_str(json)
            .map_err(|e| LoadError::new(LoadErrorKind::Invalid, "$", format!("invalid JSON: {e}")))?;
        Self::from_json(&value, registry)
    }
}
