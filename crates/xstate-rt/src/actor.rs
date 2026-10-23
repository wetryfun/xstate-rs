use std::marker::PhantomData;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use crate::core::Kind;
use crate::event::EventType;
use crate::interp::State;
use crate::machine::{Machine, ROOT};
use crate::system::{EffectKind, ExternalEffect, InitOptions, Status, System, SystemError, SystemLogic};

/// A running machine and its invoked children: an actor [`System`] whose
/// root is `machine`, with conveniences for the root.
///
/// The actor never reads a clock. The host passes the current time in
/// milliseconds; [`Actor::next_deadline`] says when timers are due.
///
/// Effects are handled as a live XState actor would: the root's emitted
/// events and logs are collected ([`Actor::take_emitted`],
/// [`Actor::take_logs`]); custom actions with a Rust implementation run
/// immediately; async work (`@xstate.effect`) stays pending until the host
/// resolves it with [`Actor::resolve_effect`].
pub struct Actor<C, E> {
    machine: Arc<Machine<C, E>>,
    logic: SystemLogic,
    system: System,
    /// The root's address (it never changes).
    root: Arc<str>,
    emitted: Vec<Value>,
    logs: Vec<Vec<Value>>,
    _event: PhantomData<fn(E)>,
}

impl<C, E> std::fmt::Debug for Actor<C, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Actor")
            .field("machine", &self.machine.id)
            .field("root", &self.system.root())
            .finish_non_exhaustive()
    }
}

impl<C, E> Actor<C, E>
where
    C: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    E: EventType + Send + Sync,
{
    /// Starts an actor at time `now_ms`.
    pub fn start(machine: Arc<Machine<C, E>>, now_ms: u64) -> Result<Self, SystemError> {
        Self::start_with(machine, InitOptions { time: now_ms, ..Default::default() })
    }

    /// Starts an actor with explicit system options (input, root id,
    /// registry key, execution id, time, step bound).
    pub fn start_with(machine: Arc<Machine<C, E>>, options: InitOptions) -> Result<Self, SystemError> {
        let logic = SystemLogic::new(machine.clone());
        let (system, effects) = System::initialize(&logic, options)?;
        let root = Arc::from(system.root());
        let mut actor =
            Self { machine, logic, system, root, emitted: Vec::new(), logs: Vec::new(), _event: PhantomData };
        actor.handle(effects);
        Ok(actor)
    }

    /// Restores an actor from [`Actor::persist`] data. Runs no actions.
    pub fn restore(machine: Arc<Machine<C, E>>, data: &Value) -> Result<Self, SystemError> {
        let logic = SystemLogic::new(machine.clone());
        let system = System::from_json(&logic, data, InitOptions::default().max_steps)?;
        if system.snapshot(system.root()).and_then(|s| s.as_any().downcast_ref::<State<C, E>>()).is_none() {
            return Err(SystemError::Rejected("the snapshot's root is not this machine".into()));
        }
        let root = Arc::from(system.root());
        Ok(Self { machine, logic, system, root, emitted: Vec::new(), logs: Vec::new(), _event: PhantomData })
    }

    fn handle(&mut self, effects: Vec<ExternalEffect>) {
        let root = self.root.clone();
        for effect in effects {
            let from_root = *effect.source.address == *root;
            let done = match effect.kind {
                EffectKind::Emit => {
                    if from_root {
                        if let Some(e) = &effect.event {
                            self.emitted.push(e.clone());
                        }
                    }
                    true
                }
                _ if effect.ty == "@xstate.log" => {
                    if from_root {
                        self.logs.push(effect.args.clone().unwrap_or_default());
                    }
                    true
                }
                _ if effect.ty == "@xstate.cancelEffect" || effect.ty == "@xstate.cleanup" => true,
                _ => effect.run(),
            };
            if done {
                self.system.acknowledge(&effect.id);
            }
        }
    }

    /// Sends `event` to the root at `now_ms` (see [`Actor::send_json`]).
    pub fn send(&mut self, event: E, now_ms: u64) -> Result<(), SystemError> {
        self.send_json(event.to_json(), now_ms)
    }

    /// Sends a JSON event to the root at `now_ms`. When time has moved
    /// forward, timers due by `now_ms` fire first; a timer scheduled at the
    /// current time (a zero delay) fires on the next [`Actor::advance`], as
    /// with XState's `SimulatedClock`.
    pub fn send_json(&mut self, event: Value, now_ms: u64) -> Result<(), SystemError> {
        if now_ms > self.system.now() {
            self.advance(now_ms)?;
        }
        let root = self.root.clone();
        let effects = self.system.send(&root, event)?;
        self.handle(effects);
        Ok(())
    }

    /// Sends a JSON event to any actor in the system by address (the root's
    /// is its id; an invoked child's is `<parent>/<invoke id>`), as
    /// [`Actor::send_json`] does for the root.
    pub fn send_to(&mut self, address: &str, event: Value, now_ms: u64) -> Result<(), SystemError> {
        if now_ms > self.system.now() {
            self.advance(now_ms)?;
        }
        let effects = self.system.send(address, event)?;
        self.handle(effects);
        Ok(())
    }

    /// Fires every timer due at or before `now_ms`, chronologically. Each
    /// timer is its own step, as with a live XState actor: when a later
    /// timer fails, the effects of the earlier ones have already run.
    pub fn advance(&mut self, now_ms: u64) -> Result<(), SystemError> {
        let target = now_ms.max(self.system.now());
        while let Some(effects) = self.system.fire_next_timer(target)? {
            self.handle(effects);
        }
        let effects = self.system.advance(target)?;
        self.handle(effects);
        Ok(())
    }

    /// Delivers the result of pending async work (an `@xstate.effect`), such
    /// as `{ "type": "xstate.async.resolve", "data": ... }`.
    pub fn resolve_effect(&mut self, effect_id: &str, event: Option<Value>) -> Result<(), SystemError> {
        let effects = self.system.resolve_effect(effect_id, event)?;
        self.handle(effects);
        Ok(())
    }

    /// External work waiting for the host.
    pub fn pending_effects(&self) -> Vec<ExternalEffect> {
        self.system.external_effects().cloned().collect()
    }

    /// Stops the root and its children, as XState's `actor.stop()`.
    pub fn stop(&mut self) -> Result<(), SystemError> {
        let root = self.root.clone();
        let effects = self.system.send(&root, serde_json::json!({ "type": "xstate.stop" }))?;
        self.handle(effects);
        Ok(())
    }

    /// When the next timer is due, if any.
    pub fn next_deadline(&self) -> Option<u64> {
        self.system.next_deadline()
    }

    fn state(&self) -> &State<C, E> {
        self.system
            .snapshot(self.system.root())
            .and_then(|s| s.as_any().downcast_ref::<State<C, E>>())
            .expect("the root is this machine")
    }

    pub fn status(&self) -> Status {
        self.state().status
    }

    pub fn context(&self) -> &C {
        &self.state().context
    }

    /// The actor's output, once it is done.
    pub fn output(&self) -> Option<&Value> {
        self.state().output.as_ref()
    }

    /// The error of an actor in the `error` status.
    pub fn error(&self) -> Option<&Value> {
        self.state().error.as_ref()
    }

    /// The system's virtual time.
    pub fn now(&self) -> u64 {
        self.system.now()
    }

    /// XState's nested state value, such as `{"b": "b1"}`.
    pub fn value(&self) -> Value {
        self.machine.state_value(&self.state().nodes)
    }

    /// Whether a state is active: `#id` by state id, or a `.`-separated path
    /// of state keys from the root, such as `"checkout.payment"`.
    pub fn matches(&self, state: &str) -> bool {
        let nodes = &self.state().nodes;
        if let Some(id) = state.strip_prefix('#') {
            return self.machine.node_by_id(id).is_some_and(|n| nodes.contains(&n));
        }
        let mut current = ROOT;
        for key in state.split('.') {
            let node = self.machine.tree.node(current);
            match node.children.iter().copied().find(|&c| self.machine.data(c).key == key) {
                Some(c) if nodes.contains(&c) => current = c,
                _ => return false,
            }
        }
        true
    }

    /// Whether any active state has this tag.
    pub fn has_tag(&self, tag: &str) -> bool {
        self.state().nodes.iter().any(|&n| self.machine.data(n).tags.iter().any(|t| t == tag))
    }

    /// Ids of the active states, in configuration order.
    pub fn configuration(&self) -> Vec<&str> {
        self.state().nodes.iter().map(|&n| self.machine.data(n).id.as_str()).collect()
    }

    /// Ids of the active atomic and final states, in configuration order.
    pub fn active_leaf_ids(&self) -> Vec<&str> {
        self.state()
            .nodes
            .iter()
            .filter(|&&n| matches!(self.machine.tree.kind(n), Kind::Atomic | Kind::Final))
            .map(|&n| self.machine.data(n).id.as_str())
            .collect()
    }

    /// The root's invoked children: id and actor address.
    pub fn children(&self) -> Vec<(&str, &str)> {
        self.state().children.iter().map(|(k, r)| (k.as_str(), &*r.address)).collect()
    }

    /// Takes the events the root emitted since the last call.
    pub fn take_emitted(&mut self) -> Vec<Value> {
        std::mem::take(&mut self.emitted)
    }

    /// Takes the argument lists of the root's `@xstate.log` actions.
    pub fn take_logs(&mut self) -> Vec<Vec<Value>> {
        std::mem::take(&mut self.logs)
    }

    /// The whole system snapshot (XState's `SystemSnapshot` shape).
    pub fn persist(&self) -> Value {
        self.system.to_json()
    }

    pub fn machine(&self) -> &Arc<Machine<C, E>> {
        &self.machine
    }

    /// The underlying system, for access to children.
    pub fn system(&self) -> &System {
        &self.system
    }

    /// The system's definitions.
    pub fn system_logic(&self) -> &SystemLogic {
        &self.logic
    }
}
