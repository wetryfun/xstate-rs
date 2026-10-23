//! Rust implementations of the guards, actions and delays a definition
//! references by name.
//!
//! Each entry is a factory that receives the reference's JSON `params` once,
//! at load time, and returns a bound closure. The interpreter only ever calls
//! the bound closures, by index.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::event::{Ev, EventType};
use crate::machine::{Machine, NodeId};

pub(crate) type GuardFn<C, E> = Box<dyn Fn(&GuardArgs<'_, C, E>) -> bool + Send + Sync>;
pub(crate) type ActionFn<C, E> = Box<dyn Fn(&mut ActionArgs<'_, C, E>) + Send + Sync>;
pub(crate) type DelayFn<C, E> = Box<dyn Fn(&C, &Ev<E>) -> u64 + Send + Sync>;

type Factory<T> = Box<dyn Fn(&Value) -> Result<T, String> + Send + Sync>;

/// What a guard sees: the context, the event, and the active configuration.
pub struct GuardArgs<'a, C, E> {
    pub context: &'a C,
    pub event: &'a Ev<E>,
    pub(crate) machine: &'a Machine<C, E>,
    pub(crate) active: &'a [bool],
}

impl<C, E> GuardArgs<'_, C, E> {
    /// Whether the state with this id (with or without a leading `#`) is active.
    pub fn in_state(&self, id: &str) -> bool {
        let id = id.strip_prefix('#').unwrap_or(id);
        self.machine.node_by_id(id).is_some_and(|n| self.active[n as usize])
    }
}

/// What a custom action sees. Context reads go through [`ActionArgs::context`]
/// and writes through [`ActionArgs::context_mut`]; the runtime tracks writes to
/// decide whether eventless transitions need another look.
pub struct ActionArgs<'a, C, E> {
    pub event: &'a Ev<E>,
    pub(crate) context: &'a mut C,
    pub(crate) context_written: &'a mut bool,
    pub(crate) emitted: &'a mut Vec<Value>,
    pub(crate) raised: &'a mut VecDeque<Ev<E>>,
    pub(crate) machine: &'a Machine<C, E>,
    pub(crate) active: &'a [bool],
}

impl<C, E: EventType> ActionArgs<'_, C, E> {
    pub fn context(&self) -> &C {
        self.context
    }

    /// Mutable access to the context. This is an extension over XState's JSON
    /// actions, which can only update context through `@xstate.assign`.
    pub fn context_mut(&mut self) -> &mut C {
        *self.context_written = true;
        self.context
    }

    /// Emits an event to the actor's listeners, like `@xstate.emit`.
    pub fn emit(&mut self, event: Value) {
        self.emitted.push(event);
    }

    /// Raises an event on the actor's internal queue, like `@xstate.raise`
    /// without a delay.
    pub fn raise(&mut self, event: E) {
        self.raised.push_back(Ev::User(event));
    }

    /// Whether the state with this id (with or without a leading `#`) is active.
    pub fn in_state(&self, id: &str) -> bool {
        let id = id.strip_prefix('#').unwrap_or(id);
        self.machine.node_by_id(id).is_some_and(|n: NodeId| self.active[n as usize])
    }
}

/// Named guard, action and delay implementations for a context type `C` and
/// event type `E`.
pub struct Registry<C, E> {
    pub(crate) guards: HashMap<String, Factory<GuardFn<C, E>>>,
    pub(crate) actions: HashMap<String, Factory<ActionFn<C, E>>>,
    pub(crate) delays: HashMap<String, Factory<DelayFn<C, E>>>,
}

impl<C, E> Default for Registry<C, E> {
    fn default() -> Self {
        Self { guards: HashMap::new(), actions: HashMap::new(), delays: HashMap::new() }
    }
}

fn parse_params<P: DeserializeOwned>(params: &Value) -> Result<P, String> {
    P::deserialize(params).map_err(|e| e.to_string())
}

impl<C: 'static, E: 'static> Registry<C, E> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a guard that ignores `params`.
    pub fn guard(
        &mut self,
        name: impl Into<String>,
        f: impl Fn(&GuardArgs<'_, C, E>) -> bool + Send + Sync + 'static,
    ) -> &mut Self {
        let f = Arc::new(f);
        self.guards.insert(
            name.into(),
            Box::new(move |_| {
                let f = f.clone();
                Ok(Box::new(move |args: &GuardArgs<'_, C, E>| f(args)) as GuardFn<C, E>)
            }),
        );
        self
    }

    /// Registers a guard whose `params` deserialize into `P` once, at load time.
    /// A reference without `params` deserializes from `null`.
    pub fn guard_with<P>(
        &mut self,
        name: impl Into<String>,
        f: impl Fn(&GuardArgs<'_, C, E>, &P) -> bool + Send + Sync + 'static,
    ) -> &mut Self
    where
        P: DeserializeOwned + Send + Sync + 'static,
    {
        let f = Arc::new(f);
        self.guards.insert(
            name.into(),
            Box::new(move |params| {
                let p: P = parse_params(params)?;
                let f = f.clone();
                Ok(Box::new(move |args: &GuardArgs<'_, C, E>| f(args, &p)) as GuardFn<C, E>)
            }),
        );
        self
    }

    /// Registers an action that ignores `params`.
    pub fn action(
        &mut self,
        name: impl Into<String>,
        f: impl Fn(&mut ActionArgs<'_, C, E>) + Send + Sync + 'static,
    ) -> &mut Self {
        let f = Arc::new(f);
        self.actions.insert(
            name.into(),
            Box::new(move |_| {
                let f = f.clone();
                Ok(Box::new(move |args: &mut ActionArgs<'_, C, E>| f(args)) as ActionFn<C, E>)
            }),
        );
        self
    }

    /// Registers an action whose `params` deserialize into `P` once, at load time.
    pub fn action_with<P>(
        &mut self,
        name: impl Into<String>,
        f: impl Fn(&mut ActionArgs<'_, C, E>, &P) + Send + Sync + 'static,
    ) -> &mut Self
    where
        P: DeserializeOwned + Send + Sync + 'static,
    {
        let f = Arc::new(f);
        self.actions.insert(
            name.into(),
            Box::new(move |params| {
                let p: P = parse_params(params)?;
                let f = f.clone();
                Ok(Box::new(move |args: &mut ActionArgs<'_, C, E>| f(args, &p)) as ActionFn<C, E>)
            }),
        );
        self
    }

    /// Registers a named delay computed from the context and event at the
    /// moment the timer is scheduled.
    pub fn delay(
        &mut self,
        name: impl Into<String>,
        f: impl Fn(&C, &Ev<E>) -> u64 + Send + Sync + 'static,
    ) -> &mut Self {
        let f = Arc::new(f);
        self.delays.insert(
            name.into(),
            Box::new(move |_| {
                let f = f.clone();
                Ok(Box::new(move |c: &C, e: &Ev<E>| f(c, e)) as DelayFn<C, E>)
            }),
        );
        self
    }

    /// Registers a named delay with a fixed number of milliseconds.
    pub fn delay_ms(&mut self, name: impl Into<String>, ms: u64) -> &mut Self {
        self.delay(name, move |_, _| ms)
    }
}
