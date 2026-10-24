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
//!
//! Values the JSON computes are XState's code nodes in the `rust` language,
//! `{ "@code": "<name>", "@lang": "rust" }` (or `@expr`, or no `@lang` with
//! a root `"@exprLang": "rust"`), which `machineFromJSON` hands to the
//! evaluator registered for the language. Here the source is the name of an
//! [`Registry::expr`] (or, in the root `context`, a [`Registry::context`]):
//! a context value, an assigned patch, a raised or emitted event, action or
//! guard params, an inline guard, a delay, an invoke's input, an output, or
//! an action whose result's `context` is merged.
//!
//! Code nodes in other languages run through the evaluator registered for
//! the language ([`Registry::evaluator`]), as `machineFromJSON` hands them to
//! `sources.evaluators[lang]`: the evaluator gets the node ([`Code`]: its
//! source, kind, slot and JSON path) and XState's scope ([`Scope`]).

use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::core::Nid;
use crate::event::Ev;
use crate::machine::Machine;
use crate::system::{ActorRef, Context};
use indexmap::IndexMap;

pub(crate) type GuardFn<C, E> = Box<dyn Fn(&GuardArgs<'_, C, E>) -> bool + Send + Sync>;
pub(crate) type ActionFn = crate::system::EffectExec;
pub(crate) type DelayFn<C, E> = Box<dyn Fn(&C, &Ev<E>) -> u64 + Send + Sync>;
pub(crate) type ExprFn<C, E> = Arc<dyn Fn(&ExprArgs<'_, C, E>) -> Value + Send + Sync>;
pub(crate) type ContextFn = Arc<dyn Fn(&ContextArgs<'_>) -> Value + Send + Sync>;

pub(crate) type Factory<T> = Arc<dyn Fn(&Value) -> Result<T, String> + Send + Sync>;

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

/// What an expression sees (XState's evaluator scope): the context as the
/// actions before it left it, the event, the params of the named JSON
/// action or guard it is part of, and the active configuration.
pub struct ExprArgs<'a, C, E> {
    pub context: &'a C,
    pub event: &'a Ev<E>,
    /// The resolved `params` of the enclosing named action or guard defined
    /// in the JSON (`actions` / `guards` at the root), if any.
    pub params: Option<&'a Value>,
    /// The state input (XState's state `input`) of the state whose action,
    /// output or invoke this is, or of a transition's source state.
    pub input: Option<&'a Value>,
    pub(crate) machine: &'a Machine<C, E>,
    pub(crate) config: &'a [Nid],
    pub(crate) enq: Option<&'a Enq<'a>>,
    pub(crate) children: &'a IndexMap<String, ActorRef>,
    pub(crate) cx: &'a dyn Context,
    pub(crate) error: &'a RefCell<Option<String>>,
}

impl<C, E> ExprArgs<'_, C, E> {
    /// The actor's children by id (XState's `children`).
    pub fn children(&self) -> &IndexMap<String, ActorRef> {
        self.children
    }

    /// The actor itself (XState's `self`).
    pub fn self_ref(&self) -> ActorRef {
        self.cx.self_ref()
    }

    /// The actor's parent (XState's `self._parent`); `None` for the root.
    pub fn parent_ref(&self) -> Option<ActorRef> {
        self.cx.parent_ref()
    }

    /// Fails the step with `message` once the expression returns, as an
    /// exception thrown by XState's evaluator does (the first failure wins).
    pub fn fail(&self, message: impl Into<String>) {
        self.error.borrow_mut().get_or_insert_with(|| message.into());
    }

    /// Whether the state with this id (with or without a leading `#`) is
    /// active, like `snapshot.matches('#id')`.
    pub fn in_state(&self, id: &str) -> bool {
        let id = id.strip_prefix('#').unwrap_or(id);
        self.machine.node_by_id(id).is_some_and(|n| self.config.contains(&n))
    }

    /// XState's `enq`, for expressions that are actions or transitions (in
    /// other places XState's scope has none).
    pub fn enq(&self) -> Option<&Enq<'_>> {
        self.enq
    }
}

/// XState's `enq` for Rust expressions: what an action or a transition
/// function enqueues, run in order with the actions around it.
#[derive(Default)]
pub struct Enq<'a> {
    pub(crate) ops: RefCell<Vec<EnqOp>>,
    /// The host's child id allocation, for `spawn`.
    pub(crate) ids: Option<&'a dyn ChildIds>,
}

/// The host side of `enq.spawn`: XState's child id allocation.
pub(crate) trait ChildIds {
    /// The id a spawn of `src` gets (the explicit `id`, or `<src>:<n>`).
    /// With `commit`, the id is allocated now (actions); otherwise it is
    /// only predicted (transitions, which allocate when they are taken).
    fn spawn_id(&self, src: &str, id: Option<&str>, commit: bool) -> Result<String, String>;
    /// Whether ids allocated now are committed (see `spawn_id`).
    fn commits(&self) -> bool;
}

/// Options for [`Enq::spawn`] (XState's spawn options).
#[derive(Debug, Clone, Default)]
pub struct SpawnOptions {
    /// The child's id; `<src>:<n>` when absent.
    pub id: Option<String>,
    pub input: Option<Value>,
    /// The key the child is registered under in the system (`systemId`).
    pub system_id: Option<String>,
    /// Whether the child reports its snapshots to the parent.
    pub sync_snapshot: Option<bool>,
}

#[derive(Debug, Clone)]
pub(crate) enum EnqOp {
    Raise {
        event: Value,
        delay: Option<u64>,
        id: Option<String>,
    },
    Emit(Value),
    Cancel(String),
    Log(Vec<Value>),
    Action {
        name: String,
        params: Value,
    },
    SendTo {
        to: SendTarget,
        event: Value,
        delay: Option<u64>,
        id: Option<String>,
    },
    Stop(String),
    /// `id` is the predicted id when not `committed` (allocated again when
    /// the operation is applied).
    Spawn {
        src: String,
        id: String,
        committed: bool,
        explicit: bool,
        options: SpawnOptions,
    },
    /// An `enq` call that failed (XState throws from it).
    Fail(String),
}

/// Who `enq.sendTo` sends to.
#[derive(Debug, Clone)]
pub(crate) enum SendTarget {
    /// A child, by id (resolved when the operation runs).
    Child(String),
    /// An actor by reference (a stopped one's events are dropped).
    Ref(ActorRef),
    /// Not an actor: the event is a dead letter.
    Nowhere,
    /// The actor's parent (none for the root).
    Parent,
}

impl std::fmt::Debug for Enq<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Enq").field("ops", &self.ops).finish_non_exhaustive()
    }
}

impl Enq<'_> {
    /// `enq.raise(event)`: the event goes to the internal queue.
    pub fn raise(&self, event: Value) {
        self.ops.borrow_mut().push(EnqOp::Raise { event, delay: None, id: None });
    }

    /// `enq.raise(event, { delay, id })`: a delayed raise, cancellable by `id`.
    pub fn raise_delayed(&self, event: Value, delay_ms: u64, id: Option<&str>) {
        self.ops.borrow_mut().push(EnqOp::Raise { event, delay: Some(delay_ms), id: id.map(str::to_owned) });
    }

    /// `enq.emit(event)`.
    pub fn emit(&self, event: Value) {
        self.ops.borrow_mut().push(EnqOp::Emit(event));
    }

    /// `enq.cancel(id)`: cancels a delayed raise.
    pub fn cancel(&self, id: &str) {
        self.ops.borrow_mut().push(EnqOp::Cancel(id.to_owned()));
    }

    /// `enq.log(...args)`.
    pub fn log(&self, args: Vec<Value>) {
        self.ops.borrow_mut().push(EnqOp::Log(args));
    }

    /// `enq(actions.name, params)`: the registered custom action `name`
    /// with these params (an effect, as for a `{ type, params }` action).
    pub fn action(&self, name: &str, params: Value) {
        self.ops.borrow_mut().push(EnqOp::Action { name: name.to_owned(), params });
    }

    /// `enq.sendTo(children[child], event)`: a missing child makes the event
    /// a dead letter (`@xstate.deadLetter`, reason `missingTarget`).
    pub fn send_to(&self, child: &str, event: Value) {
        self.push_send(SendTarget::Child(child.to_owned()), event, None, None);
    }

    /// `enq.sendTo(children[child], event, { delay, id })`: a logical timer
    /// with the child as its target, cancellable by `id`.
    pub fn send_to_delayed(&self, child: &str, event: Value, delay_ms: u64, id: Option<&str>) {
        self.push_send(SendTarget::Child(child.to_owned()), event, Some(delay_ms), id);
    }

    /// `enq.sendTo(ref, event)` with a reference kept in the context
    /// ([`ContextArgs::spawn`]'s `{ "$actor", "incarnation" }`), or a child
    /// id as a string; anything else makes the event a dead letter.
    pub fn send_to_ref(&self, actor: &Value, event: Value) {
        self.push_send(Self::target(actor), event, None, None);
    }

    /// [`Enq::send_to_ref`] after `delay_ms`, cancellable by `id`.
    pub fn send_to_ref_delayed(&self, actor: &Value, event: Value, delay_ms: u64, id: Option<&str>) {
        self.push_send(Self::target(actor), event, Some(delay_ms), id);
    }

    fn target(actor: &Value) -> SendTarget {
        match actor {
            Value::String(child) => SendTarget::Child(child.clone()),
            other => ActorRef::from_json(other).map_or(SendTarget::Nowhere, SendTarget::Ref),
        }
    }

    /// `enq.sendTo(self._parent, event)` (a dead letter from the root).
    pub fn send_parent(&self, event: Value) {
        self.push_send(SendTarget::Parent, event, None, None);
    }

    /// `enq.sendTo(self._parent, event, { delay, id })`.
    pub fn send_parent_delayed(&self, event: Value, delay_ms: u64, id: Option<&str>) {
        self.push_send(SendTarget::Parent, event, Some(delay_ms), id);
    }

    /// `enq.stop(children[child])` (nothing when there is no such child).
    pub fn stop(&self, child: &str) {
        self.ops.borrow_mut().push(EnqOp::Stop(child.to_owned()));
    }

    fn push_send(&self, to: SendTarget, event: Value, delay: Option<u64>, id: Option<&str>) {
        self.ops.borrow_mut().push(EnqOp::SendTo { to, event, delay, id: id.map(str::to_owned) });
    }

    /// `enq.spawn(src, options)`: a child running the registered actor
    /// logic `src` (an invoke source of this machine), started after the
    /// step. Returns its id. An explicit id already in use fails the step.
    pub fn spawn(&self, src: &str, options: SpawnOptions) -> String {
        let explicit = options.id.is_some();
        let (commit, result) = match self.ids {
            Some(ids) => (ids.commits(), ids.spawn_id(src, options.id.as_deref(), ids.commits())),
            None => (false, Ok(options.id.clone().unwrap_or_else(|| format!("{src}:0")))),
        };
        match result {
            Ok(id) => {
                self.ops.borrow_mut().push(EnqOp::Spawn {
                    src: src.to_owned(),
                    id: id.clone(),
                    committed: commit,
                    explicit,
                    options,
                });
                id
            }
            Err(message) => {
                self.ops.borrow_mut().push(EnqOp::Fail(message));
                options.id.unwrap_or_default()
            }
        }
    }

    pub(crate) fn take(&self) -> Vec<EnqOp> {
        std::mem::take(&mut self.ops.borrow_mut())
    }
}

/// Named guard, action and delay implementations for a context type `C` and
/// event type `E`.
pub struct Registry<C, E> {
    pub(crate) guards: HashMap<String, Factory<GuardFn<C, E>>>,
    pub(crate) actions: HashMap<String, Factory<ActionFn>>,
    pub(crate) delays: HashMap<String, Factory<DelayFn<C, E>>>,
    pub(crate) actors: Vec<(String, Arc<dyn crate::system::Logic>)>,
    pub(crate) exprs: HashMap<String, ExprFn<C, E>>,
    pub(crate) contexts: HashMap<String, ContextFn>,
    pub(crate) evaluators: HashMap<String, Arc<dyn Evaluator<C, E>>>,
}

impl<C, E> Default for Registry<C, E> {
    fn default() -> Self {
        Self {
            guards: HashMap::new(),
            actions: HashMap::new(),
            delays: HashMap::new(),
            actors: Vec::new(),
            exprs: HashMap::new(),
            contexts: HashMap::new(),
            evaluators: HashMap::new(),
        }
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
            Arc::new(move |_| {
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
            Arc::new(move |params| {
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
            Arc::new(move |_| {
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
            Arc::new(move |params| {
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
            Arc::new(move |_| {
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

    /// Registers an expression: the value of every `{ "@code": name,
    /// "@lang": "rust" }` node outside the root `context`, computed when
    /// XState would evaluate it. What the value means depends on where the
    /// node is: a context value or patch, an event, params, a guard (truthy
    /// as in JavaScript), a delay (milliseconds or a duration string), an
    /// input or output, or, as an action, `{ "context": patch }` to merge.
    pub fn expr(
        &mut self,
        name: impl Into<String>,
        f: impl Fn(&ExprArgs<'_, C, E>) -> Value + Send + Sync + 'static,
    ) -> &mut Self {
        self.exprs.insert(name.into(), Arc::new(f));
        self
    }

    /// Registers an expression for the root `context` (XState's context
    /// factory), computed from the actor's input when the actor starts.
    pub fn context(
        &mut self,
        name: impl Into<String>,
        f: impl Fn(Option<&Value>) -> Value + Send + Sync + 'static,
    ) -> &mut Self {
        self.contexts.insert(name.into(), Arc::new(move |args: &ContextArgs<'_>| f(args.input)));
        self
    }

    /// [`Registry::context`] with XState's whole context factory scope:
    /// the input and `spawn` ([`ContextArgs`]).
    pub fn context_with(
        &mut self,
        name: impl Into<String>,
        f: impl Fn(&ContextArgs<'_>) -> Value + Send + Sync + 'static,
    ) -> &mut Self {
        self.contexts.insert(name.into(), Arc::new(f));
        self
    }

    /// Registers the evaluator of the code nodes in language `lang`
    /// (`"@lang": lang`, or the root `@exprLang`), XState's
    /// `sources.evaluators[lang]`. The `rust` language is built in
    /// ([`Registry::expr`]), so `lang` must be another one.
    pub fn evaluator(&mut self, lang: impl Into<String>, evaluator: impl Evaluator<C, E> + 'static) -> &mut Self {
        let lang = lang.into();
        assert!(lang != crate::load::RUST_LANG, "the rust language is built in: use Registry::expr");
        self.evaluators.insert(lang, Arc::new(evaluator));
        self
    }
}

/// Spawns children for a context factory (the machine's initial transition).
pub(crate) trait Spawner {
    fn spawn(&self, src: &str, options: SpawnOptions) -> Option<ActorRef>;
    fn self_ref(&self) -> ActorRef;
    fn fail(&self, message: String);
}

/// What a root `context` expression sees: XState's context factory
/// arguments, the actor's input and `spawn`.
pub struct ContextArgs<'a> {
    pub input: Option<&'a Value>,
    pub(crate) spawner: Option<&'a dyn Spawner>,
}

impl ContextArgs<'_> {
    /// The actor whose context this is (XState's `self`); `None` outside
    /// an actor system (when listing references).
    pub fn self_ref(&self) -> Option<ActorRef> {
        self.spawner.map(|s| s.self_ref())
    }

    /// Fails the actor's start with `message` once the expression returns,
    /// as an exception thrown by XState's context factory does.
    pub fn fail(&self, message: impl Into<String>) {
        if let Some(s) = self.spawner {
            s.fail(message.into());
        }
    }

    /// XState's context factory `spawn(src, options)`: a child running the
    /// registered actor logic `src` (an invoke source of this machine),
    /// allocated now and started with the actor. Returns its reference as
    /// XState persists it, `{ "$actor": address, "incarnation": "<n>" }`:
    /// keep it in the context to send to the child ([`Enq::send_to_ref`]).
    /// An unknown source or an explicit id in use fails the actor's start
    /// (and this returns `null`).
    pub fn spawn(&self, src: &str, options: SpawnOptions) -> Value {
        match self.spawner.and_then(|s| s.spawn(src, options)) {
            Some(r) => r.to_json(),
            None => Value::Null,
        }
    }
}

/// The evaluator slot a code node fills: where XState evaluates it (the
/// `slot` XState passes to its evaluators).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Slot {
    /// The root `context` (scope: [`ContextArgs`]).
    Context,
    /// An inline guard, a guard's params, a named guard's `when`.
    Guard,
    /// A choice state's `choice` expression, or a branch's `when`.
    Choice,
    /// A transition's or choice branch's `context`, an `@xstate.assign`'s.
    TransitionContext,
    /// A transition's, choice branch's or invoke's `input`.
    Input,
    /// A transition function or a route function.
    Transition,
    /// An expression as an action.
    Action,
    /// A raised or emitted event, a custom action's params.
    ActionParams,
    /// A delay's duration, a state's or invoke's `timeout`.
    Delay,
    /// A final state's or the root's `output`.
    Output,
}

impl Slot {
    /// XState's name for the slot.
    pub fn as_str(self) -> &'static str {
        match self {
            Slot::Context => "context",
            Slot::Guard => "guard",
            Slot::Choice => "choice",
            Slot::TransitionContext => "transitionContext",
            Slot::Input => "input",
            Slot::Transition => "transition",
            Slot::Action => "action",
            Slot::ActionParams => "actionParams",
            Slot::Delay => "delay",
            Slot::Output => "output",
        }
    }
}

/// Whether a code node is `{ "@code": ... }` or `{ "@expr": ... }`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CodeKind {
    Code,
    Expr,
}

impl CodeKind {
    /// XState's name for the kind (`code` or `expr`).
    pub fn as_str(self) -> &'static str {
        match self {
            CodeKind::Code => "code",
            CodeKind::Expr => "expr",
        }
    }
}

/// A code node handed to an [`Evaluator`]: XState's evaluator arguments
/// without the scope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Code {
    /// Unique per code node of a loaded machine for the life of the process,
    /// so evaluators can cache what they compile from `source`.
    pub id: u64,
    pub lang: String,
    pub source: String,
    pub kind: CodeKind,
    pub slot: Slot,
    /// The node's JSON path (`$.states.a.entry[0]`).
    pub path: String,
}

/// What a code node sees: XState's evaluator scope. The root `context` is
/// computed by the context factory ([`ContextArgs`]: the input, `spawn`,
/// `self`); everything else sees the transition arguments ([`ExprArgs`]).
pub enum Scope<'s, 'a, C, E> {
    Expr(&'s ExprArgs<'a, C, E>),
    Context(&'s ContextArgs<'a>),
}

/// Evaluates the code nodes of one language (XState's
/// `sources.evaluators[lang]`), called where and when XState calls it. An
/// error fails the step, as an exception from XState's evaluator does.
pub trait Evaluator<C, E>: Send + Sync {
    fn eval(&self, code: &Code, scope: Scope<'_, '_, C, E>) -> Result<Value, String>;
}

impl<C, E, F> Evaluator<C, E> for F
where
    F: Fn(&Code, Scope<'_, '_, C, E>) -> Result<Value, String> + Send + Sync,
{
    fn eval(&self, code: &Code, scope: Scope<'_, '_, C, E>) -> Result<Value, String> {
        self(code, scope)
    }
}

impl<C, E> Evaluator<C, E> for Arc<dyn Evaluator<C, E>> {
    fn eval(&self, code: &Code, scope: Scope<'_, '_, C, E>) -> Result<Value, String> {
        (**self).eval(code, scope)
    }
}
