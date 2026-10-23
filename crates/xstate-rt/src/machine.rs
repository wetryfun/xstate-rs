//! The compiled form of a JSON machine definition: the structural [`Tree`]
//! shared with every host, plus the values the native host needs.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::{Map, Value};

use crate::core::{Nid, Tid, Tree};
use crate::event::{matches_descriptor, Ev};
use crate::registry::{ActionFn, ContextFn, DelayFn, ExprFn, Factory, GuardFn};
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
    pub output: Option<Template>,
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
    pub input: Option<Template>,
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

/// A custom action or guard whose `params` hold expressions: its
/// implementation is bound to the params each time it runs.
pub(crate) struct Dynamic<T> {
    pub name: String,
    pub factory: Factory<T>,
    pub params: Template,
}

/// A JSON value with expressions in it (XState's resolvable values).
#[derive(Debug, Clone)]
pub(crate) enum Template {
    Static(Value),
    /// Index into `Machine::exprs` (or `Machine::context_exprs` in the root
    /// `context`).
    Expr(u32),
    Object(Vec<(String, Template)>),
    Array(Vec<Template>),
}

impl Template {
    /// The value, with `expr` computing each expression.
    pub fn eval(&self, expr: &mut dyn FnMut(u32) -> Value) -> Value {
        match self {
            Template::Static(v) => v.clone(),
            Template::Expr(i) => expr(*i),
            Template::Object(fields) => Value::Object(fields.iter().map(|(k, t)| (k.clone(), t.eval(expr))).collect()),
            Template::Array(items) => Value::Array(items.iter().map(|t| t.eval(expr)).collect()),
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum DelaySpec {
    Fixed(u64),
    Named(u32),
    /// An expression (index into `Machine::exprs`): milliseconds or a
    /// duration string.
    Expr(u32),
}

pub(crate) struct TransitionData {
    /// `None` for a targetless transition.
    pub targets: Option<Arc<[Nid]>>,
    pub matches: Vec<(String, Value)>,
    pub guard: Option<Guard>,
    /// The transition's `context` patch.
    pub context: Option<Map<String, Value>>,
    /// Instead of `context`: a patch with expressions in it.
    pub context_expr: Option<Template>,
    pub actions: Vec<Action>,
    pub reenter: bool,
    /// For invoke completion transitions: the child's id, whose session must
    /// match the event's `sessionId` (XState's `_eventMatcher`).
    pub session: Option<String>,
    /// For completion transitions of an invoke with a `timeout`: the timer
    /// key the transition cancels after its own operations.
    pub cancel_timeout: Option<crate::system::TimerKey>,
    /// A transition function (an expression in the `transition` slot):
    /// its result gives the targets, context and `reenter`.
    pub function: Option<TransitionFn>,
}

/// A transition computed when it is selected, as XState's `to` functions
/// (expression indices are into `Machine::exprs`).
#[derive(Debug, Clone)]
pub(crate) enum TransitionFn {
    /// A transition expression, with `enq`.
    Expr(u32),
    /// A route function: its state is the target of every result (a falsy
    /// result blocks the route, `true` takes it as it is).
    Route { expr: u32, target: Arc<[Nid]> },
    /// A choice state's expression, which must give a target.
    ChoiceExpr { expr: u32, id: String },
    /// A choice state's branches: the first whose `when` passes.
    Choice { branches: Vec<ChoiceBranch>, path: String, id: String },
}

#[derive(Debug, Clone)]
pub(crate) struct ChoiceBranch {
    pub when: Option<Guard>,
    pub targets: Option<Arc<[Nid]>>,
    pub context: Option<Template>,
    pub reenter: bool,
}

impl TransitionData {
    /// Whether XState compiles this transition to a transition function
    /// (`machineFromJSON` does so for any `context` or `actions`; invoke
    /// timeouts wrap completion transitions in one).
    pub fn is_dynamic(&self) -> bool {
        self.context.is_some()
            || self.context_expr.is_some()
            || !self.actions.is_empty()
            || self.cancel_timeout.is_some()
            || self.function.is_some()
    }

    /// Whether the transition runs expressions, so its result is computed
    /// when it is selected, as XState evaluates transition functions.
    pub fn evaluates(&self) -> bool {
        self.function.is_some() || self.context_expr.is_some() || self.actions.iter().any(Action::evaluates)
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
    /// An inline expression, truthy as in JavaScript.
    Expr(u32),
    /// A custom guard with expressions in its params (index into
    /// `Machine::dynamic_guards`).
    Dynamic(u32),
    /// A named JSON guard with params its expressions read: the params
    /// (resolved in the outer scope), then the guard.
    Scoped(Box<Template>, Box<Guard>),
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
    /// `@xstate.raise` of an event with expressions in it.
    RaiseExpr {
        event: Template,
        id: Option<String>,
        delay: Option<u64>,
    },
    EmitExpr(Template),
    AssignExpr(Template),
    /// Index into `Machine::dynamic_actions`.
    CustomExpr(u32),
    /// An expression as an action: its result's `context` is merged.
    Expr(u32),
    /// The start of a named JSON action whose params its expressions read:
    /// the params, resolved here, are their scope's until `EndParams`.
    Params(Template),
    EndParams,
}

impl Action {
    /// Whether the action writes the context (and so needs the context it
    /// starts from).
    pub fn assigns(&self) -> bool {
        matches!(self, Action::Assign(_) | Action::AssignExpr(_) | Action::Expr(_))
    }

    /// Whether the action evaluates expressions (which read the context).
    pub fn evaluates(&self) -> bool {
        matches!(
            self,
            Action::RaiseExpr { .. }
                | Action::EmitExpr(_)
                | Action::AssignExpr(_)
                | Action::CustomExpr(_)
                | Action::Expr(_)
                | Action::Params(_)
        )
    }
}

pub(crate) enum InitialContext<C> {
    Value(C),
    Template(Template),
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
    pub(crate) exprs: Vec<ExprFn<C, E>>,
    pub(crate) context_exprs: Vec<ContextFn>,
    pub(crate) dynamic_actions: Vec<Dynamic<ActionFn>>,
    pub(crate) dynamic_guards: Vec<Dynamic<GuardFn<C, E>>>,
    /// The registry's custom actions, for `enq.action` in expressions.
    pub(crate) action_factories: HashMap<String, Factory<ActionFn>>,
    pub(crate) id_map: HashMap<String, Nid>,
    pub(crate) internal_events: Vec<String>,
    /// The initial context: a value, or, when the root `context` has
    /// expressions, computed from the actor's input at start.
    pub(crate) initial_context: InitialContext<C>,
    pub(crate) output: Option<Template>,
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

    /// XState's `resolveTarget` for one target of a transition from
    /// `source`: `#id`, `.child.path` below the source, or a path below the
    /// source's parent.
    pub(crate) fn resolve_target(&self, source: Nid, target: &str) -> Result<Nid, String> {
        if let Some(id) = target.strip_prefix('#') {
            return self.node_by_id(id).ok_or_else(|| format!("no state has id \"{id}\""));
        }
        let (mut current, path) = match target.strip_prefix('.') {
            Some(rest) => (source, rest),
            None => match self.tree.node(source).parent {
                Some(parent) => (parent, target),
                None => return Err(format!("invalid target \"{target}\" on the root; use \".child\" or \"#id\"")),
            },
        };
        for segment in crate::load::to_state_path(path) {
            if segment.is_empty() {
                break;
            }
            let node = self.tree.node(current);
            current = node
                .children
                .iter()
                .chain(&node.history_children)
                .copied()
                .find(|&c| self.data(c).key == segment)
                .ok_or_else(|| format!("child state '{segment}' does not exist on '{}'", self.data(current).id))?;
        }
        Ok(current)
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
