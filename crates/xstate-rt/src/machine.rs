//! The compiled form of a JSON machine definition: the structural [`Tree`]
//! shared with every host, plus the values the native host needs.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::{Map, Value};

use crate::core::{Nid, Tid, Tree};
use crate::event::{matches_descriptor, Ev};
use crate::registry::{DelayFn, GuardFn};
use crate::system::{EffectExec, Logic};

/// Index of a state node, in document order.
pub type NodeId = Nid;

pub(crate) const ROOT: Nid = 0;

/// Per-node values.
pub(crate) struct NodeData {
    pub key: String,
    pub id: String,
    pub entry: Vec<Action>,
    pub exit: Vec<Action>,
    /// Delayed transitions' timers, in XState's order: `after` entries,
    /// the state `timeout`, then invoke timeouts.
    pub delayed: Vec<DelayedDef>,
    pub invokes: Vec<InvokeDef>,
    pub output: Option<Value>,
    pub tags: Vec<String>,
    /// The static targets of a compound state's `initial`.
    pub initial: Arc<[Nid]>,
    /// The default targets of a history state.
    pub history_target: Arc<[Nid]>,
}

/// A timer a state schedules on entry and cancels on exit; its event is
/// caught by the state's delayed transitions.
pub(crate) struct DelayedDef {
    pub kind: DelayedKind,
    pub delay: DelaySpec,
}

pub(crate) enum DelayedKind {
    /// `after`: raises `{ type: "xstate.after", delay, stateId }`, where
    /// `delay` is a number for numeric keys and the key string otherwise.
    After { event_delay: Value },
    /// The state `timeout`: raises `{ type: "xstate.timeout.state", stateId }`.
    StateTimeout,
    /// An invoke `timeout`: raises `{ type: "xstate.timeout.actor", actorId, sessionId }`.
    InvokeTimeout { invoke_id: String },
}

/// One `invoke` of a state.
pub(crate) struct InvokeDef {
    pub id: String,
    pub logic: Arc<dyn Logic>,
    pub input: Option<Value>,
    pub registry_key: Option<String>,
    /// Whether the invoke has `onSnapshot` (the child then reports snapshots).
    pub sync_snapshot: bool,
}

/// A custom action reference: its name and params, and the bound Rust
/// implementation (if the registry provides one).
pub(crate) struct CustomAction {
    pub name: String,
    pub params: Option<Value>,
    pub exec: EffectExec,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum DelaySpec {
    Fixed(u64),
    Named(u32),
}

pub(crate) struct TransitionData {
    /// `None` for a targetless transition.
    pub targets: Option<Arc<[Nid]>>,
    pub matches: Vec<(String, Value)>,
    pub guard: Option<Guard>,
    /// The transition's `context` patch.
    pub context: Option<Map<String, Value>>,
    pub actions: Vec<Action>,
    pub reenter: bool,
    /// For invoke completion transitions: the child's id, whose session must
    /// match the event's `sessionId` (XState's `_eventMatcher`).
    pub session: Option<String>,
    /// For completion transitions of an invoke with a `timeout`: the timer
    /// key the transition cancels after its own operations.
    pub cancel_timeout: Option<Value>,
}

impl TransitionData {
    /// Whether XState compiles this transition to a transition function
    /// (`machineFromJSON` does so for any `context` or `actions`; invoke
    /// timeouts wrap completion transitions in one).
    pub fn is_dynamic(&self) -> bool {
        self.context.is_some() || !self.actions.is_empty() || self.cancel_timeout.is_some()
    }
}

#[derive(Debug, Clone)]
pub(crate) enum Guard {
    Custom(u32),
    Not(Box<Guard>),
    StateIn(Nid),
    /// A route's match: the event's `to` is this `#id`.
    RouteTo(String),
    /// Both, in order (a route's match, then its named guard).
    And(Box<Guard>, Box<Guard>),
}

#[derive(Debug, Clone)]
pub(crate) enum Action {
    /// Index into `Machine::raise_events`.
    Raise {
        event: u32,
        id: Option<String>,
        delay: Option<u64>,
    },
    Cancel(String),
    Log(Vec<Value>),
    Emit(Value),
    Assign(Map<String, Value>),
    Custom(u32),
}

/// Options for compiling a machine.
#[derive(Debug, Clone)]
pub struct MachineOptions {
    /// The microstep bound for one macrostep (XState's `options.maxIterations`).
    pub max_iterations: usize,
}

impl Default for MachineOptions {
    fn default() -> Self {
        Self { max_iterations: 1000 }
    }
}

/// A compiled machine definition. Create actors from it with
/// [`crate::Actor::start`]; one machine can be shared by many actors.
pub struct Machine<C, E> {
    pub(crate) id: Option<String>,
    pub(crate) version: Option<String>,
    pub(crate) tree: Tree,
    pub(crate) nodes: Vec<NodeData>,
    pub(crate) transitions: Vec<TransitionData>,
    pub(crate) guards: Vec<GuardFn<C, E>>,
    pub(crate) actions: Vec<CustomAction>,
    pub(crate) actors: Vec<(String, Arc<dyn Logic>)>,
    pub(crate) delays: Vec<DelayFn<C, E>>,
    pub(crate) raise_events: Vec<Ev<E>>,
    pub(crate) id_map: HashMap<String, Nid>,
    pub(crate) internal_events: Vec<String>,
    pub(crate) initial_context: C,
    pub(crate) output: Option<Value>,
    pub(crate) options: MachineOptions,
    /// Why starting fails although the definition loads: a root `initial`
    /// without child states, which XState accepts and fails on at start.
    pub(crate) start_error: Option<String>,
    /// Empty snapshot parts that new snapshots share until they first write
    /// them (copy-on-write), so an actor allocates none it does not use.
    pub(crate) empty: crate::interp::EmptyParts<E>,
}

impl<C, E> Machine<C, E> {
    /// The machine's `id`, if the definition declares one.
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    /// The definition's `version`, if any.
    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }

    /// The id of every state node, in document order.
    pub fn state_ids(&self) -> impl Iterator<Item = &str> {
        self.nodes.iter().map(|n| n.id.as_str())
    }

    pub(crate) fn node_by_id(&self, id: &str) -> Option<Nid> {
        self.id_map.get(id).copied()
    }

    #[inline]
    pub(crate) fn data(&self, n: Nid) -> &NodeData {
        &self.nodes[n as usize]
    }

    pub(crate) fn transition(&self, t: Tid) -> &TransitionData {
        &self.transitions[t as usize]
    }

    pub(crate) fn is_internal_event(&self, event_type: &str) -> bool {
        self.internal_events.iter().any(|d| matches_descriptor(event_type, d))
    }

    /// XState's state value for a configuration.
    pub(crate) fn state_value(&self, config: &[Nid]) -> Value {
        let all = crate::core::all_state_nodes(&self.tree, config, |n| self.data(n).initial.clone());
        let adj = self.tree.active_children(all.as_slice());
        self.node_value(ROOT, &adj)
    }

    fn node_value(&self, node: Nid, adj: &[Vec<Nid>]) -> Value {
        let children = &adj[node as usize];
        if children.is_empty() {
            return Value::Object(Map::new());
        }
        if self.tree.kind(node) == crate::core::Kind::Compound {
            let child = children[0];
            if self.tree.kind(child).is_atomic() {
                return Value::String(self.data(child).key.clone());
            }
        }
        let mut map = Map::new();
        for &c in children {
            map.insert(self.data(c).key.clone(), self.node_value(c, adj));
        }
        Value::Object(map)
    }
}
