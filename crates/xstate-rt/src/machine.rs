//! The compiled form of a machine definition: a flat arena of state nodes in
//! document (pre-order) order, with every name resolved to an index.

use std::collections::HashMap;

use serde_json::{Map, Value};

use crate::event::{matches_descriptor, Ev};
use crate::registry::{ActionFn, DelayFn, GuardFn};

/// Index of a state node. Index order is document order, so ancestors come
/// before descendants and entry order is ascending.
pub type NodeId = u32;
pub(crate) type TransitionId = u32;

pub(crate) const ROOT: NodeId = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeKind {
    Atomic,
    Compound,
    Parallel,
    Final,
    History { deep: bool },
}

pub(crate) struct Node {
    pub key: String,
    pub id: String,
    pub parent: Option<NodeId>,
    /// One past the last descendant: descendants are `id + 1 .. end`.
    pub end: NodeId,
    pub kind: NodeKind,
    /// Child states, excluding history states, in document order.
    pub children: Vec<NodeId>,
    /// History pseudo-states declared directly in this node.
    pub history_children: Vec<NodeId>,
    /// The resolved `initial` of a compound state.
    pub initial: Option<NodeId>,
    /// The default targets of a history state.
    pub history_target: Vec<NodeId>,
    pub entry: Vec<Action>,
    pub exit: Vec<Action>,
    /// Transitions by exact event descriptor (including wildcard descriptors,
    /// which also match literally), sorted by event key.
    pub on_exact: Vec<(u32, Vec<TransitionId>)>,
    /// Wildcard descriptors (`*`, `prefix.*`), longest first.
    pub on_wildcard: Vec<(String, Vec<TransitionId>)>,
    pub always: Vec<TransitionId>,
    pub after: Vec<AfterDef>,
    pub output: Option<Value>,
    pub tags: Vec<String>,
}

impl Node {
    pub fn is_atomic_like(&self) -> bool {
        matches!(self.kind, NodeKind::Atomic | NodeKind::Final)
    }
}

/// One `after` entry. The timer raises `xstate.after` with `delay` and
/// `stateId`, and the entry's transitions match exactly that payload.
pub(crate) struct AfterDef {
    /// The key as written, for persistence.
    pub key: String,
    /// The `delay` field of the event: a number for numeric keys, the key
    /// string for named delays and duration strings.
    pub event_delay: Value,
    pub delay: DelaySpec,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum DelaySpec {
    Fixed(u64),
    Named(u32),
}

pub(crate) struct Transition {
    pub source: NodeId,
    pub targets: Vec<NodeId>,
    pub matches: Vec<(String, Value)>,
    pub guard: Option<Guard>,
    /// The transition's `context` patch, merged before its actions run.
    pub context: Option<Map<String, Value>>,
    pub actions: Vec<Action>,
    pub reenter: bool,
}

#[derive(Debug, Clone)]
pub(crate) enum Guard {
    Custom(u32),
    Not(Box<Guard>),
    StateIn(NodeId),
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
    /// The microstep bound for one macrostep (XState's `maxIterations`).
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
    pub(crate) nodes: Vec<Node>,
    pub(crate) transitions: Vec<Transition>,
    pub(crate) guards: Vec<GuardFn<C, E>>,
    pub(crate) actions: Vec<ActionFn<C, E>>,
    pub(crate) delays: Vec<DelayFn<C, E>>,
    pub(crate) raise_events: Vec<Ev<E>>,
    pub(crate) id_map: HashMap<String, NodeId>,
    pub(crate) event_keys: HashMap<String, u32>,
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

    pub(crate) fn node_by_id(&self, id: &str) -> Option<NodeId> {
        self.id_map.get(id).copied()
    }

    #[inline]
    pub(crate) fn node(&self, id: NodeId) -> &Node {
        &self.nodes[id as usize]
    }

    /// Strict descendant check; `None` stands for "above the root", which
    /// contains every node.
    #[inline]
    pub(crate) fn is_descendant(&self, node: NodeId, ancestor: Option<NodeId>) -> bool {
        match ancestor {
            None => true,
            Some(a) => node > a && node < self.node(a).end,
        }
    }

    /// Ancestors of `node` from its parent upwards, stopping before `to`
    /// (`None` walks up to and including the root).
    pub(crate) fn proper_ancestors(&self, node: NodeId, to: Option<NodeId>) -> Vec<NodeId> {
        let mut out = Vec::new();
        if Some(node) == to {
            return out;
        }
        let mut m = self.node(node).parent;
        while let Some(a) = m {
            if Some(a) == to {
                break;
            }
            out.push(a);
            m = self.node(a).parent;
        }
        out
    }

    pub(crate) fn is_internal_event(&self, event_type: &str) -> bool {
        self.internal_events.iter().any(|d| matches_descriptor(event_type, d))
    }

    /// Candidate transitions of a node for an event, in priority order: exact
    /// descriptor matches first, then wildcards, longest descriptor first.
    pub(crate) fn candidates<'a>(
        &'a self,
        node: NodeId,
        event_key: Option<u32>,
        event_type: &'a str,
    ) -> impl Iterator<Item = TransitionId> + 'a {
        let n = self.node(node);
        let exact: &[TransitionId] = match event_key {
            Some(k) => match n.on_exact.binary_search_by_key(&k, |(key, _)| *key) {
                Ok(i) => &n.on_exact[i].1,
                Err(_) => &[],
            },
            None => &[],
        };
        let wildcards = n
            .on_wildcard
            .iter()
            .filter(move |(d, _)| d != event_type && matches_descriptor(event_type, d))
            .flat_map(|(_, ts)| ts.iter().copied());
        exact.iter().copied().chain(wildcards)
    }
}
