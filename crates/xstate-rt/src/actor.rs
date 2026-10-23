use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use crate::core::Kind;
use crate::error::{RestoreError, RuntimeError};
use crate::event::{Ev, EventType};
use crate::interp::{MacroEvent, State, Status};
use crate::machine::{Machine, ROOT};
use crate::snapshot::PersistedSnapshot;

/// A running instance of a [`Machine`].
///
/// The actor never reads a clock. The host passes the current time in
/// milliseconds to [`Actor::send`] and [`Actor::advance`], asks
/// [`Actor::next_deadline`] when it should call `advance` next, and
/// collects emitted events with [`Actor::take_emitted`].
pub struct Actor<C, E> {
    machine: Arc<Machine<C, E>>,
    st: State<C, E>,
}

impl<C: std::fmt::Debug, E> std::fmt::Debug for Actor<C, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Actor")
            .field("machine", &self.machine.id)
            .field("status", &self.st.status)
            .field("value", &self.machine.state_value(&self.st.nodes))
            .field("context", &self.st.context)
            .finish_non_exhaustive()
    }
}

impl<C, E> Actor<C, E>
where
    C: Serialize + DeserializeOwned + Clone,
    E: EventType,
{
    /// Starts an actor with the definition's initial context, entering the
    /// initial configuration at time `now_ms`.
    pub fn start(machine: Arc<Machine<C, E>>, now_ms: u64) -> Result<Self, RuntimeError> {
        let context = machine.initial_context.clone();
        Self::start_with_context(machine, context, now_ms)
    }

    /// Starts an actor with a caller-supplied initial context.
    pub fn start_with_context(machine: Arc<Machine<C, E>>, context: C, now_ms: u64) -> Result<Self, RuntimeError> {
        let mut st = State::new(context, now_ms);
        let result = machine.macrostep(&mut st, MacroEvent::Init);
        let actor = Self { machine, st };
        result.map(|_| actor)
    }

    /// Restores an actor from a persisted snapshot. Fails if the definition
    /// no longer contains a state, history state or `after` entry the
    /// snapshot refers to. Restoring runs no actions.
    pub fn restore(machine: Arc<Machine<C, E>>, snapshot: PersistedSnapshot<C>) -> Result<Self, RestoreError> {
        let st = machine.restore(snapshot)?;
        Ok(Self { machine, st })
    }

    /// Sends a user event at time `now_ms` and runs it to completion.
    /// Returns whether a transition handled it. Due timers are not fired;
    /// call [`Actor::advance`] first if time has passed.
    pub fn send(&mut self, event: E, now_ms: u64) -> Result<bool, RuntimeError> {
        self.send_ev(Ev::User(event), now_ms)
    }

    /// Sends a JSON event: converted into `E` when it fits, otherwise
    /// delivered as an internal event.
    pub fn send_json(&mut self, event: Value, now_ms: u64) -> Result<bool, RuntimeError> {
        match Ev::from_json(event) {
            Some(ev) => self.send_ev(ev, now_ms),
            None => Ok(false),
        }
    }

    fn send_ev(&mut self, ev: Ev<E>, now_ms: u64) -> Result<bool, RuntimeError> {
        if self.machine.is_internal_event(ev.event_type()) {
            return Err(RuntimeError::InternalEvent { event_type: ev.event_type().to_owned() });
        }
        self.st.now = self.st.now.max(now_ms);
        self.machine.macrostep(&mut self.st, MacroEvent::External(ev))
    }

    /// When the next timer is due, if any.
    pub fn next_deadline(&self) -> Option<u64> {
        if self.st.status != Status::Active {
            return None;
        }
        self.st.next_timer().map(|i| self.st.timers[i].due)
    }

    /// Fires every timer due at or before `now_ms`, in (due time, scheduling
    /// order) order. While a timer fires, the actor's clock reads its due
    /// time, so timers scheduled by it are deterministic. Returns how many
    /// timers fired.
    pub fn advance(&mut self, now_ms: u64) -> Result<usize, RuntimeError> {
        let mut fired = 0;
        while self.st.status == Status::Active {
            let Some(i) = self.st.next_timer() else { break };
            if self.st.timers[i].due > now_ms {
                break;
            }
            let timer = self.st.timers.remove(i);
            self.st.now = self.st.now.max(timer.due);
            fired += 1;
            self.machine.macrostep(&mut self.st, MacroEvent::Timer(timer.event))?;
        }
        self.st.now = self.st.now.max(now_ms);
        Ok(fired)
    }

    /// Stops the actor: cancels its timers without running exit actions,
    /// as XState's `actor.stop()`.
    pub fn stop(&mut self) {
        if self.st.status == Status::Active {
            self.st.status = Status::Stopped;
        }
        self.st.timers.clear();
    }

    pub fn status(&self) -> Status {
        self.st.status
    }

    pub fn context(&self) -> &C {
        &self.st.context
    }

    /// The actor's output, once it is done.
    pub fn output(&self) -> Option<&Value> {
        self.st.output.as_ref()
    }

    /// The actor's clock: the latest time passed in, or the due time of the
    /// timer being fired.
    pub fn now(&self) -> u64 {
        self.st.now
    }

    /// XState's nested state value, such as `{"b": "b1"}`.
    pub fn value(&self) -> Value {
        self.machine.state_value(&self.st.nodes)
    }

    /// The error of an actor in the `error` status.
    pub fn error(&self) -> Option<&Value> {
        self.st.error.as_ref()
    }

    /// Whether a state is active: `#id` by state id, or a `.`-separated path
    /// of state keys from the root, such as `"checkout.payment"`.
    pub fn matches(&self, state: &str) -> bool {
        if let Some(id) = state.strip_prefix('#') {
            return self.machine.node_by_id(id).is_some_and(|n| self.st.nodes.contains(&n));
        }
        let mut current = ROOT;
        for key in state.split('.') {
            let node = self.machine.tree.node(current);
            match node.children.iter().copied().find(|&c| self.machine.data(c).key == key) {
                Some(c) if self.st.nodes.contains(&c) => current = c,
                _ => return false,
            }
        }
        true
    }

    /// Whether any active state has this tag.
    pub fn has_tag(&self, tag: &str) -> bool {
        self.st.nodes.iter().any(|&n| self.machine.data(n).tags.iter().any(|t| t == tag))
    }

    /// Ids of the active states, in configuration order.
    pub fn configuration(&self) -> Vec<&str> {
        self.st.nodes.iter().map(|&n| self.machine.data(n).id.as_str()).collect()
    }

    /// Ids of the active atomic and final states, in configuration order.
    pub fn active_leaf_ids(&self) -> Vec<&str> {
        self.st
            .nodes
            .iter()
            .filter(|&&n| matches!(self.machine.tree.kind(n), Kind::Atomic | Kind::Final))
            .map(|&n| self.machine.data(n).id.as_str())
            .collect()
    }

    /// Takes the events emitted with `@xstate.emit` since the last call.
    pub fn take_emitted(&mut self) -> Vec<Value> {
        std::mem::take(&mut self.st.emitted)
    }

    /// Takes the argument lists of `@xstate.log` actions since the last call.
    pub fn take_logs(&mut self) -> Vec<Vec<Value>> {
        std::mem::take(&mut self.st.logs)
    }

    pub fn persist(&self) -> PersistedSnapshot<C> {
        self.machine.persist(&self.st)
    }

    pub fn machine(&self) -> &Arc<Machine<C, E>> {
        &self.machine
    }
}
