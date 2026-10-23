//! Rust implementations of the guards, actions and delays a definition
//! references by name.
//!
//! Each entry is a factory that receives the reference's JSON `params` once,
//! at load time, and returns a bound closure. The interpreter only calls the
//! bound closures, by index.
//!
//! As in XState's `machineFromJSON`, guards see the transition arguments
//! (context, event, configuration) and their params; custom actions are
//! effects that run after the snapshot is computed and receive only their
//! params. Context changes go through `@xstate.assign` or transition
//! `context`.

use std::collections::HashMap;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::core::Nid;
use crate::event::Ev;
use crate::machine::Machine;

pub(crate) type GuardFn<C, E> = Box<dyn Fn(&GuardArgs<'_, C, E>) -> bool + Send + Sync>;
pub(crate) type ActionFn = crate::system::EffectExec;
pub(crate) type DelayFn<C, E> = Box<dyn Fn(&C, &Ev<E>) -> u64 + Send + Sync>;

type Factory<T> = Box<dyn Fn(&Value) -> Result<T, String> + Send + Sync>;

/// What a guard sees: the context, the event, and the active configuration.
pub struct GuardArgs<'a, C, E> {
    pub context: &'a C,
    pub event: &'a Ev<E>,
    pub(crate) machine: &'a Machine<C, E>,
    pub(crate) config: &'a [Nid],
}

impl<C, E> GuardArgs<'_, C, E> {
    /// Whether the state with this id (with or without a leading `#`) is
    /// active, like `snapshot.matches('#id')`.
    pub fn in_state(&self, id: &str) -> bool {
        let id = id.strip_prefix('#').unwrap_or(id);
        self.machine.node_by_id(id).is_some_and(|n| self.config.contains(&n))
    }
}

/// Named guard, action and delay implementations for a context type `C` and
/// event type `E`.
pub struct Registry<C, E> {
    pub(crate) guards: HashMap<String, Factory<GuardFn<C, E>>>,
    pub(crate) actions: HashMap<String, Factory<ActionFn>>,
    pub(crate) delays: HashMap<String, Factory<DelayFn<C, E>>>,
    pub(crate) actors: Vec<(String, Arc<dyn crate::system::Logic>)>,
}

impl<C, E> Default for Registry<C, E> {
    fn default() -> Self {
        Self { guards: HashMap::new(), actions: HashMap::new(), delays: HashMap::new(), actors: Vec::new() }
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

    /// Registers a guard whose `params` deserialize into `P` once, at load
    /// time. A reference without `params` deserializes from `null`.
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

    /// Registers a custom action: an effect that runs after the snapshot is
    /// computed, in order with the other effects.
    pub fn action(&mut self, name: impl Into<String>, f: impl Fn() + Send + Sync + 'static) -> &mut Self {
        let f = Arc::new(f);
        self.actions.insert(
            name.into(),
            Box::new(move |_| {
                let f = f.clone();
                Ok(Arc::new(move || f()) as ActionFn)
            }),
        );
        self
    }

    /// Registers a custom action whose `params` deserialize into `P` once, at
    /// load time. The effect receives only its params, as in XState.
    pub fn action_with<P>(&mut self, name: impl Into<String>, f: impl Fn(&P) + Send + Sync + 'static) -> &mut Self
    where
        P: DeserializeOwned + Send + Sync + 'static,
    {
        let f = Arc::new(f);
        self.actions.insert(
            name.into(),
            Box::new(move |params| {
                let p: P = parse_params(params)?;
                let f = f.clone();
                Ok(Arc::new(move || f(&p)) as ActionFn)
            }),
        );
        self
    }

    /// Registers actor logic that `invoke` can reference by `src`: another
    /// machine (`Arc<Machine<..>>`), a [`crate::step::StepLogic`], or any
    /// [`crate::system::Logic`].
    pub fn actor(&mut self, name: impl Into<String>, logic: Arc<dyn crate::system::Logic>) -> &mut Self {
        let name = name.into();
        match self.actors.iter_mut().find(|(n, _)| *n == name) {
            Some(entry) => entry.1 = logic,
            None => self.actors.push((name, logic)),
        }
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
