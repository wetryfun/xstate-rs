//! The compiled form of a JSON machine definition: the structural [`Tree`]
//! shared with every host, plus the values the native host needs.

use std::collections::HashMap;

use serde_json::{Map, Value};

use crate::core::{Nid, Tid, Tree};
use crate::event::{matches_descriptor, Ev};
use crate::registry::{ActionFn, DelayFn, GuardFn};

/// Index of a state node, in document order.
pub type NodeId = Nid;

pub(crate) const ROOT: Nid = 0;

/// Per-node values.
pub(crate) struct NodeData {
    pub key: String,
    pub id: String,
    pub entry: Vec<Action>,
    pub exit: Vec<Action>,
    pub after: Vec<AfterDef>,
    pub output: Option<Value>,
    pub tags: Vec<String>,
    /// The static targets of a compound state's `initial`.
    pub initial: Vec<Nid>,
    /// The default targets of a history state.
    pub history_target: Vec<Nid>,
}

/// One `after` entry. Its timer raises `xstate.after` with `delay` and
/// `stateId`, and the entry's transitions match exactly that payload.
pub(crate) struct AfterDef {
    /// The key as written, for persistence.
    pub key: String,
    /// The `delay` field of the event: a number for numeric keys, the key
    /// string otherwise.
    pub event_delay: Value,
    pub delay: DelaySpec,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum DelaySpec {
    Fixed(u64),
    Named(u32),
}

pub(crate) struct TransitionData {
    /// `None` for a targetless transition.
    pub targets: Option<Vec<Nid>>,
    pub matches: Vec<(String, Value)>,
    pub guard: Option<Guard>,
    /// The transition's `context` patch.
    pub context: Option<Map<String, Value>>,
    pub actions: Vec<Action>,
    pub reenter: bool,
}

impl TransitionData {
    /// Whether XState compiles this transition to a transition function
    /// (`machineFromJSON` does so for any `context` or `actions`).
    pub fn is_dynamic(&self) -> bool {
        self.context.is_some() || !self.actions.is_empty()
    }
}

#[derive(Debug, Clone)]
pub(crate) enum Guard {
    Custom(u32),
    Not(Box<Guard>),
    StateIn(Nid),
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
    pub(crate) actions: Vec<ActionFn>,
    pub(crate) delays: Vec<DelayFn<C, E>>,
    pub(crate) raise_events: Vec<Ev<E>>,
    pub(crate) id_map: HashMap<String, Nid>,
    pub(crate) internal_events: Vec<String>,
    pub(crate) initial_context: C,
    pub(crate) output: Option<Value>,
    pub(crate) options: MachineOptions,
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
        let adj = self.tree.active_children(config);
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
