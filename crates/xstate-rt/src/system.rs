//! A deterministic actor system: a port of XState's pure system transitions
//! (`systemTransition.ts`: `initialSystemTransition`, `systemTransition`,
//! `advanceSystemTime`).
//!
//! The system holds every actor's snapshot and topology, the registry, the
//! FIFO message queue, timers in virtual time, and pending external work.
//! Each call completes actor macrosteps and drains inter-actor messages.
//! Effects that leave the system (emitted events, custom actions, async work,
//! logs) are returned as data with stable ids; results of external work
//! re-enter as correlated events. Nothing here reads a clock or runs I/O.
//!
//! Actors run [`Logic`]: state machines ([`crate::Machine`]), and Rust-defined
//! step and async logic ([`crate::step`]). Snapshots, events, inputs and
//! outputs cross actor boundaries as JSON, as they do in XState's system
//! snapshots, which this module reproduces field for field.

use std::any::Any;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::sync::Arc;

use indexmap::IndexMap;
use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};
use serde_json::{json, Map, Value};

/// The lifecycle status of an actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Active,
    Done,
    Error,
    Stopped,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Active => "active",
            Status::Done => "done",
            Status::Error => "error",
            Status::Stopped => "stopped",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "active" => Status::Active,
            "done" => Status::Done,
            "error" => Status::Error,
            "stopped" => Status::Stopped,
            _ => return None,
        })
    }
}

/// An actor identity: its address and incarnation. Serialized as XState's
/// `{ "$actor": address, "incarnation": "<n>" }`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ActorRef {
    pub address: Arc<str>,
    pub incarnation: u64,
}

impl ActorRef {
    pub fn to_json(&self) -> Value {
        json!({ "$actor": &*self.address, "incarnation": self.incarnation.to_string() })
    }

    pub fn from_json(value: &Value) -> Option<Self> {
        let address = value.get("$actor")?.as_str()?;
        let incarnation = value.get("incarnation")?.as_str()?.parse().ok()?;
        Some(Self { address: address.into(), incarnation })
    }
}

/// An error raised by a system call. As in XState, the system must not be
/// used after an error other than [`SystemError::Rejected`]: state may be
/// partially updated.
#[derive(Debug, Clone, PartialEq)]
pub enum SystemError {
    /// The call was rejected before changing anything (an unknown actor, a
    /// stale effect result, time moving backwards, ...).
    Rejected(String),
    /// A machine exceeded its microstep bound.
    InfiniteLoop { event_type: String, max_iterations: usize, states: Vec<Value> },
    /// More than `max_steps` messages, timers or allocations in one call.
    MaxSteps,
    /// Any other failure while transitioning.
    Failed(String),
}

impl fmt::Display for SystemError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SystemError::Rejected(m) | SystemError::Failed(m) => f.write_str(m),
            SystemError::InfiniteLoop { event_type, max_iterations, states } => {
                let states: Vec<String> = states.iter().map(|v| v.to_string()).collect();
                write!(
                    f,
                    "infinite loop processing event \"{event_type}\": more than {max_iterations} microsteps without reaching a stable state. Last states: {}",
                    states.join(" -> ")
                )
            }
            SystemError::MaxSteps => f.write_str("System transition exceeded maxSteps."),
        }
    }
}

impl std::error::Error for SystemError {}

fn rejected<T>(message: impl Into<String>) -> Result<T, SystemError> {
    Err(SystemError::Rejected(message.into()))
}

/// The kind of an external effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectKind {
    Builtin,
    Emit,
    Action,
}

impl EffectKind {
    fn as_str(self) -> &'static str {
        match self {
            EffectKind::Builtin => "builtin",
            EffectKind::Emit => "emit",
            EffectKind::Action => "action",
        }
    }
}

/// A callable implementation attached to a custom action effect.
pub type EffectExec = Arc<dyn Fn() + Send + Sync>;

/// The type of an external effect, before the system names it.
#[derive(Clone)]
pub enum EffectType {
    /// A fixed type, such as `@xstate.log` or an emitted event's type.
    Fixed(String),
    /// A custom action of the actor's machine, by its name in the machine's
    /// sources; named `<definition key>/actions/<name>` by the system.
    Action(String),
}

/// External work produced by a transition (an XState executable effect that
/// is not a system builtin).
#[derive(Clone)]
pub struct ExternalSpec {
    pub kind: EffectKind,
    pub ty: EffectType,
    /// For an emit, `None` means the event without `type`.
    pub params: Option<Value>,
    pub args: Vec<Value>,
    pub event: Option<Value>,
    pub reason: Option<String>,
    pub exec: Option<EffectExec>,
}

/// An effect returned by a logic's transition, as XState's `ExecutableEffect`.
#[derive(Clone)]
pub enum Effect {
    /// A child was allocated (already reflected in the system).
    Spawn(ActorRef),
    /// Start a child allocated in this transition.
    Start(ActorRef),
    /// Stop a child.
    Stop(ActorRef),
    /// Cancel the logical timer with this key (`{id}`, `{seq}` or `{event}`).
    Cancel(Value),
    /// Schedule the logical timer with this key.
    DelayedRaise { key: Value, delay: u64 },
    /// Deliver an event to another actor.
    SendTo { target: ActorRef, event: Value },
    /// The actor reached `done` or `error`: notify its parent.
    Terminate,
    /// Work for the host.
    External(ExternalSpec),
}

/// A transition result.
pub struct Transition {
    pub snapshot: Box<dyn AnySnapshot>,
    pub effects: Vec<Effect>,
}

/// A logic's snapshot, type-erased.
pub trait AnySnapshot: Send + 'static {
    fn status(&self) -> Status;
    fn output(&self) -> Option<Value>;
    fn error(&self) -> Option<Value>;
    /// Whether a logical timer with this key (`{id}`, `{seq}` or `{event}`)
    /// is pending in the snapshot.
    fn timer_pending(&self, _key: &Value) -> bool {
        false
    }
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

/// Actor logic: a pure transition function over snapshots.
pub trait Logic: Send + Sync + 'static {
    /// The definition version, checked when restoring.
    fn version(&self) -> Option<&str> {
        None
    }
    /// The logic's own id (a machine's `id`), used for the root actor.
    fn logic_id(&self) -> Option<String> {
        None
    }
    /// XState's snapshot data, as stored in a system snapshot.
    fn snapshot_data(&self, snapshot: &dyn AnySnapshot) -> Value;
    /// [`Logic::snapshot_data`] as something that serializes to the same
    /// data, so a snapshot can be written without building it as a `Value`.
    /// With `compact`, fields that [`Logic::restore`] rebuilds identically
    /// when they are missing may be left out (see [`System::compact_view`]).
    fn snapshot_serialize<'a>(
        &'a self,
        snapshot: &'a dyn AnySnapshot,
        _compact: bool,
    ) -> Box<dyn erased_serde::Serialize + 'a> {
        Box::new(self.snapshot_data(snapshot))
    }
    /// The initial snapshot and effects for `input`.
    fn initial_transition(&self, input: Option<&Value>, cx: &mut dyn Context) -> Result<Transition, SystemError>;
    /// Transitions `snapshot` (in place, or by replacing it) for `event`
    /// and returns the effects. On error the snapshot stays with its actor.
    fn transition(
        &self,
        snapshot: &mut Box<dyn AnySnapshot>,
        event: Value,
        cx: &mut dyn Context,
    ) -> Result<Vec<Effect>, SystemError>;
    /// Rebuilds a snapshot from its data.
    fn restore(&self, data: &Value) -> Result<Box<dyn AnySnapshot>, SystemError>;
    /// Event types only the actor itself may send.
    fn is_internal_event_type(&self, _event_type: &str) -> bool {
        false
    }
    /// Named actor logic this logic can invoke (`sources.actors`), in order.
    fn actor_sources(&self) -> Vec<(String, Arc<dyn Logic>)> {
        Vec::new()
    }
    /// For async logic: its `timeout` as written and in milliseconds.
    fn async_timeout(&self) -> Option<(Value, u64)> {
        None
    }
}

/// Options for allocating an actor.
#[derive(Debug, Clone, Default)]
pub struct AllocateOptions {
    pub id: String,
    pub input: Option<Value>,
    pub parent: Option<ActorRef>,
    pub registry_key: Option<String>,
    pub sync_snapshot: Option<bool>,
}

/// What a running transition can ask of the system.
pub trait Context {
    /// The actor running the transition.
    fn self_ref(&self) -> ActorRef;
    /// The running actor's parent.
    fn parent_ref(&self) -> Option<ActorRef>;
    /// Virtual time.
    fn now(&self) -> u64;
    /// Creates an actor and computes its initial snapshot. It starts when
    /// the returned `Start` effect is processed.
    fn allocate(&mut self, logic: &Arc<dyn Logic>, options: AllocateOptions) -> Result<ActorRef, SystemError>;
    /// The status of an actor (`stopped` for a stale reference).
    fn status_of(&self, actor: &ActorRef) -> Status;
    /// Removes an actor from the registry ahead of its stop.
    fn unregister(&mut self, actor: &ActorRef);
}

/// XState's `encodeAddressSegment`.
pub fn encode_address_segment(id: &str) -> String {
    if id.contains('/') || id.contains('%') {
        id.replace('%', "%25").replace('/', "%2F")
    } else {
        id.to_owned()
    }
}

/// An accepted message waiting for delivery.
#[derive(Debug, Clone)]
pub struct Message {
    pub source: Option<ActorRef>,
    pub target: ActorRef,
    pub event: Value,
    pub sequence: u64,
    pub timer_occurrence: Option<u64>,
}

/// A pending deadline in virtual time.
#[derive(Debug, Clone, PartialEq)]
pub struct SystemTimer {
    pub source: ActorRef,
    pub occurrence: u64,
    pub scheduled_at: u64,
    pub due_at: u64,
    pub sequence: u64,
    pub event: TimerEvent,
}

/// A system timer's event. A machine's timers (`{ type: "xstate.timer",
/// ...key }`) keep their key in typed form for the common shapes and build
/// the event only when it is delivered or persisted: a JSON object costs
/// several allocations, and an idle actor typically has an `after` timer.
#[derive(Debug, Clone, PartialEq)]
pub enum TimerEvent {
    /// `{ type: "xstate.timer", id }`.
    Id(String),
    /// `{ type: "xstate.timer", seq }`.
    Seq(serde_json::Number),
    /// `{ type: "xstate.timer", event: { type: "xstate.after", delay, stateId } }`.
    After { delay: Value, state_id: String },
    /// `{ type: "xstate.timer", event: { type: "xstate.timeout.state", stateId } }`.
    StateTimeout { state_id: String },
    /// Any other event, as it is.
    Other(Value),
}

const TIMER: &str = "xstate.timer";

impl TimerEvent {
    /// `{ type: "xstate.timer", ...key }` for a machine timer `key`.
    fn for_key(key: Value) -> Self {
        Self::typed(&key).unwrap_or_else(|| Self::Other(timer_event(&key)))
    }

    /// Reads a persisted event (any event, kept as it is unless it has one
    /// of the typed shapes exactly).
    pub fn from_value(event: Value) -> Self {
        if let Value::Object(m) = &event {
            let mut entries = m.iter();
            if entries.next().is_some_and(|(k, v)| k == "type" && v.as_str() == Some(TIMER)) {
                let key: Map<String, Value> = entries.map(|(k, v)| (k.clone(), v.clone())).collect();
                if let Some(typed) = Self::typed(&Value::Object(key)) {
                    return typed;
                }
            }
        }
        Self::Other(event)
    }

    fn typed(key: &Value) -> Option<Self> {
        let key = key.as_object()?;
        let only =
            |m: &Map<String, Value>, names: &[&str]| m.len() == names.len() && m.keys().zip(names).all(|(k, n)| k == n);
        if only(key, &["id"]) {
            return key["id"].as_str().map(|id| Self::Id(id.to_owned()));
        }
        if only(key, &["seq"]) {
            return key["seq"].as_number().map(|n| Self::Seq(n.clone()));
        }
        if !only(key, &["event"]) {
            return None;
        }
        let event = key["event"].as_object()?;
        match event.get("type")?.as_str()? {
            "xstate.after" if only(event, &["type", "delay", "stateId"]) => {
                Some(Self::After { delay: event["delay"].clone(), state_id: event["stateId"].as_str()?.to_owned() })
            }
            "xstate.timeout.state" if only(event, &["type", "stateId"]) => {
                Some(Self::StateTimeout { state_id: event["stateId"].as_str()?.to_owned() })
            }
            _ => None,
        }
    }

    /// The event as JSON.
    pub fn to_value(&self) -> Value {
        match self {
            Self::Other(v) => v.clone(),
            _ => serde_json::to_value(self).expect("timer events serialize"),
        }
    }

    /// The machine timer key (`{ id } | { seq } | { event }`) of an
    /// `xstate.timer` event (XState's `readTimerKey`).
    fn machine_key(&self) -> Option<Value> {
        match self {
            Self::Id(id) => Some(json!({ "id": id })),
            Self::Seq(n) => Some(json!({ "seq": n })),
            Self::After { delay, state_id } => {
                Some(json!({ "event": { "type": "xstate.after", "delay": delay, "stateId": state_id } }))
            }
            Self::StateTimeout { state_id } => {
                Some(json!({ "event": { "type": "xstate.timeout.state", "stateId": state_id } }))
            }
            Self::Other(v) if v.get("type").and_then(Value::as_str) == Some(TIMER) => read_timer_key(v),
            Self::Other(_) => None,
        }
    }

    fn is_machine_timer(&self) -> bool {
        match self {
            Self::Other(v) => v.get("type").and_then(Value::as_str) == Some(TIMER),
            _ => true,
        }
    }

    /// Appends the part of the system timer key the event decides (see
    /// [`system_timer_key`]).
    fn push_key(&self, out: &mut String) {
        match self {
            Self::Id(id) => {
                out.push_str("id,");
                push_json_str(out, id);
            }
            Self::Seq(n) => {
                out.push_str("seq,");
                out.push_str(&n.to_string());
            }
            Self::After { delay, state_id } => {
                out.push_str("event,{\"type\":\"xstate.after\",\"delay\":");
                push_json(out, delay);
                out.push_str(",\"stateId\":");
                push_json_str(out, state_id);
                out.push('}');
            }
            Self::StateTimeout { state_id } => {
                out.push_str("event,{\"type\":\"xstate.timeout.state\",\"stateId\":");
                push_json_str(out, state_id);
                out.push('}');
            }
            Self::Other(event) => {
                let ty = event.get("type").and_then(Value::as_str);
                if ty != Some(TIMER) || !push_timer_event_key(out, event) {
                    push_json_str(out, ty.unwrap_or(""));
                }
            }
        }
    }
}

impl Serialize for TimerEvent {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        struct Inner<'a>(&'static str, Option<&'a Value>, &'a str);
        impl Serialize for Inner<'_> {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                let mut m = s.serialize_map(Some(2 + usize::from(self.1.is_some())))?;
                m.serialize_entry("type", self.0)?;
                if let Some(delay) = self.1 {
                    m.serialize_entry("delay", delay)?;
                }
                m.serialize_entry("stateId", self.2)?;
                m.end()
            }
        }
        if let Self::Other(v) = self {
            return v.serialize(s);
        }
        let mut m = s.serialize_map(Some(2))?;
        m.serialize_entry("type", TIMER)?;
        match self {
            Self::Id(id) => m.serialize_entry("id", id)?,
            Self::Seq(n) => m.serialize_entry("seq", n)?,
            Self::After { delay, state_id } => {
                m.serialize_entry("event", &Inner("xstate.after", Some(delay), state_id))?
            }
            Self::StateTimeout { state_id } => {
                m.serialize_entry("event", &Inner("xstate.timeout.state", None, state_id))?
            }
            Self::Other(_) => unreachable!(),
        }
        m.end()
    }
}

/// External work for the host, with a stable id.
#[derive(Clone)]
pub struct ExternalEffect {
    /// Empty (and `params` of an emit `None`) only for the effects
    /// [`crate::Actor`] handles as it receives them, which are never pending.
    pub id: String,
    pub source: ActorRef,
    pub ty: String,
    pub kind: EffectKind,
    pub params: Option<Value>,
    pub args: Option<Vec<Value>>,
    pub event: Option<Value>,
    pub reason: Option<String>,
    /// The Rust implementation of a custom action, if the machine has one.
    pub exec: Option<EffectExec>,
}

impl fmt::Debug for ExternalEffect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_json().to_string())
    }
}

impl ExternalEffect {
    /// XState's `SystemExternalEffect` data.
    pub fn to_json(&self) -> Value {
        let mut m = Map::new();
        m.insert("id".into(), Value::String(self.id.clone()));
        m.insert("source".into(), self.source.to_json());
        m.insert("type".into(), Value::String(self.ty.clone()));
        m.insert("kind".into(), Value::String(self.kind.as_str().into()));
        if let Some(p) = &self.params {
            m.insert("params".into(), p.clone());
        }
        if let Some(a) = &self.args {
            m.insert("args".into(), Value::Array(a.clone()));
        }
        if let Some(e) = &self.event {
            m.insert("event".into(), e.clone());
        }
        if let Some(r) = &self.reason {
            m.insert("reason".into(), Value::String(r.clone()));
        }
        Value::Object(m)
    }

    fn from_json(value: &Value) -> Option<Self> {
        Some(Self {
            id: value.get("id")?.as_str()?.to_owned(),
            source: ActorRef::from_json(value.get("source")?)?,
            ty: value.get("type")?.as_str()?.to_owned(),
            kind: match value.get("kind")?.as_str()? {
                "builtin" => EffectKind::Builtin,
                "emit" => EffectKind::Emit,
                _ => EffectKind::Action,
            },
            params: value.get("params").cloned(),
            args: value.get("args").and_then(Value::as_array).cloned(),
            event: value.get("event").cloned(),
            reason: value.get("reason").and_then(Value::as_str).map(str::to_owned),
            exec: None,
        })
    }

    /// Runs the attached Rust implementation of a custom action, if any.
    /// Returns whether one ran.
    pub fn run(&self) -> bool {
        match &self.exec {
            Some(exec) => {
                exec();
                true
            }
            None => false,
        }
    }
}

struct ActorEntry {
    id: String,
    address: Arc<str>,
    incarnation: u64,
    src: String,
    /// The logic `src` names, resolved once (no lookup per event).
    logic: Arc<dyn Logic>,
    logic_version: Option<String>,
    parent: Option<ActorRef>,
    registry_key: Option<String>,
    sync_snapshot: Option<bool>,
    started: bool,
    /// Boxed: rarely set, and a `Value` is large inline.
    input: Option<Box<Value>>,
    /// `None` while the actor's own transition runs.
    snapshot: Option<Box<dyn AnySnapshot>>,
    status: Status,
}

impl ActorEntry {
    fn reference(&self) -> ActorRef {
        ActorRef { address: self.address.clone(), incarnation: self.incarnation }
    }

    fn snapshot_data(&self, logic: Option<&Arc<dyn Logic>>) -> Value {
        match (&self.snapshot, logic) {
            (Some(s), Some(l)) => l.snapshot_data(&**s),
            _ => json!({ "status": self.status.as_str() }),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Counters {
    actor: u64,
    timer: u64,
    sequence: u64,
    effect: u64,
}

/// Immutable definitions for a system: the root logic and logic for dynamic
/// spawns. Machine actor sources are discovered from the root.
#[derive(Clone)]
pub struct SystemLogic {
    pub root: Arc<dyn Logic>,
    pub actors: Vec<(String, Arc<dyn Logic>)>,
}

impl SystemLogic {
    pub fn new(root: Arc<dyn Logic>) -> Self {
        Self { root, actors: Vec::new() }
    }
}

fn same_logic(a: &Arc<dyn Logic>, b: &Arc<dyn Logic>) -> bool {
    std::ptr::eq(Arc::as_ptr(a) as *const (), Arc::as_ptr(b) as *const ())
}

/// XState's `definitions(systemLogic)`: every logic by definition key.
fn definitions(logic: &SystemLogic) -> Vec<(String, Arc<dyn Logic>)> {
    fn add(
        out: &mut Vec<(String, Arc<dyn Logic>)>,
        visited: &mut Vec<Arc<dyn Logic>>,
        key: String,
        logic: Arc<dyn Logic>,
    ) {
        match out.iter_mut().find(|(k, _)| *k == key) {
            Some(entry) => entry.1 = logic.clone(),
            None => out.push((key.clone(), logic.clone())),
        }
        if visited.iter().any(|v| same_logic(v, &logic)) {
            return;
        }
        visited.push(logic.clone());
        for (name, child) in logic.actor_sources() {
            add(out, visited, format!("{key}/actors/{}", encode_address_segment(&name)), child);
        }
    }
    let mut out = Vec::new();
    let mut visited = Vec::new();
    add(&mut out, &mut visited, "root".into(), logic.root.clone());
    for (key, l) in &logic.actors {
        add(&mut out, &mut visited, format!("actors/{}", encode_address_segment(key)), l.clone());
    }
    out
}

/// XState's `readTimerKey`.
pub(crate) fn read_timer_key(event: &Value) -> Option<Value> {
    if let Some(id) = event.get("id").and_then(Value::as_str) {
        return Some(json!({ "id": id }));
    }
    if let Some(seq) = event.get("seq").filter(|v| v.is_number()) {
        return Some(json!({ "seq": seq }));
    }
    match event.get("event") {
        Some(e @ Value::Object(o)) if o.get("type").is_some_and(Value::is_string) => Some(json!({ "event": e })),
        _ => None,
    }
}

/// XState's built-in timer identity for lifecycle timer events.
pub(crate) fn builtin_timer_identity(event: &Value) -> Option<Value> {
    let ty = event.get("type")?.as_str()?;
    let field = |k: &str| event.get(k).cloned();
    let mut m = Map::new();
    m.insert("type".into(), Value::String(ty.into()));
    match ty {
        "xstate.after" => {
            if let Some(v) = field("delay") {
                m.insert("delay".into(), v);
            }
            if let Some(v) = field("stateId") {
                m.insert("stateId".into(), v);
            }
        }
        "xstate.timeout.state" => {
            if let Some(v) = field("stateId") {
                m.insert("stateId".into(), v);
            }
        }
        "xstate.timeout.actor" => {
            if let Some(v) = field("actorId") {
                m.insert("actorId".into(), v);
            }
        }
        _ => return None,
    }
    Some(Value::Object(m))
}

/// Appends `v` as JSON.
fn push_json(out: &mut String, v: &Value) {
    use std::fmt::Write;
    let _ = write!(out, "{v}");
}

fn push_json_str(out: &mut String, s: &str) {
    push_json(out, &Value::String(s.to_owned()));
}

/// XState's `serializeTimerKey`, appended to `out`: equal keys give equal
/// text and different keys different text (every part is JSON).
fn push_timer_key(out: &mut String, key: &Value) {
    if let Some(id) = key.get("id") {
        out.push_str("id,");
        push_json(out, id);
        return;
    }
    if let Some(seq) = key.get("seq") {
        out.push_str("seq,");
        push_json(out, seq);
        return;
    }
    out.push_str("event,");
    match key.get("event") {
        Some(event) => push_event_identity(out, event),
        None => out.push_str("null"),
    }
}

/// [`builtin_timer_identity`] (or the whole event) as JSON, without
/// building the identity object.
fn push_event_identity(out: &mut String, event: &Value) {
    let fields: &[&str] = match event.get("type").and_then(Value::as_str) {
        Some("xstate.after") => &["delay", "stateId"],
        Some("xstate.timeout.state") => &["stateId"],
        Some("xstate.timeout.actor") => &["actorId"],
        _ => return push_json(out, event),
    };
    out.push_str("{\"type\":");
    push_json(out, &event["type"]);
    for f in fields {
        if let Some(v) = event.get(*f) {
            out.push_str(",\"");
            out.push_str(f);
            out.push_str("\":");
            push_json(out, v);
        }
    }
    out.push('}');
}

/// XState's `systemTimerKey`: the source actor and the timer's key.
fn system_timer_key(source: &ActorRef, event: &TimerEvent) -> String {
    let mut out = String::with_capacity(64);
    push_json_str(&mut out, &source.address);
    out.push(',');
    out.push_str(&source.incarnation.to_string());
    out.push(',');
    event.push_key(&mut out);
    out.shrink_to_fit();
    out
}

/// The key of a `{ type: 'xstate.timer', ...key }` event, appended as
/// [`push_timer_key`] would append [`read_timer_key`]'s result. Returns
/// whether the event has a key.
fn push_timer_event_key(out: &mut String, event: &Value) -> bool {
    if let Some(id) = event.get("id").filter(|v| v.is_string()) {
        out.push_str("id,");
        push_json(out, id);
        return true;
    }
    if let Some(seq) = event.get("seq").filter(|v| v.is_number()) {
        out.push_str("seq,");
        push_json(out, seq);
        return true;
    }
    match event.get("event") {
        Some(e @ Value::Object(o)) if o.get("type").is_some_and(Value::is_string) => {
            out.push_str("event,");
            push_event_identity(out, e);
            true
        }
        _ => false,
    }
}

/// The system timer key for a machine timer `key` (`{ id } | { seq } | { event }`).
fn system_timer_key_for(source: &ActorRef, key: &Value) -> String {
    let mut out = String::with_capacity(64);
    push_json_str(&mut out, &source.address);
    out.push(',');
    out.push_str(&source.incarnation.to_string());
    out.push(',');
    push_timer_key(&mut out, key);
    out
}

/// `{ type: 'xstate.timer', ...key }`.
fn timer_event(key: &Value) -> Value {
    let mut m = Map::new();
    m.insert("type".into(), Value::String("xstate.timer".into()));
    if let Value::Object(k) = key {
        for (a, b) in k {
            m.insert(a.clone(), b.clone());
        }
    }
    Value::Object(m)
}

/// Options for [`System::initialize`].
#[derive(Debug, Clone)]
pub struct InitOptions {
    pub input: Option<Value>,
    /// The root actor's id; defaults to the root logic's id (machine id), else `root`.
    pub id: Option<String>,
    pub registry_key: Option<String>,
    /// Namespaces effect ids; defaults to `system`.
    pub execution_id: String,
    pub time: u64,
    /// Bounds messages, timers and allocations per call (default 10 000).
    pub max_steps: usize,
}

impl Default for InitOptions {
    fn default() -> Self {
        Self { input: None, id: None, registry_key: None, execution_id: "system".into(), time: 0, max_steps: 10_000 }
    }
}

fn execution_id(id: String) -> std::borrow::Cow<'static, str> {
    if id == "system" {
        std::borrow::Cow::Borrowed("system")
    } else {
        std::borrow::Cow::Owned(id)
    }
}

/// The hasher of a system's maps: stateless (a `RandomState` is 16 bytes in
/// every map of every idle system), with fixed keys. Each map holds one
/// system's few actors, timers and effects.
type Fixed = std::hash::BuildHasherDefault<std::collections::hash_map::DefaultHasher>;

/// A running actor system.
pub struct System {
    /// Definitions are derived from it when an actor is allocated (rarely),
    /// so a system holds no copy of them.
    logic: SystemLogic,
    /// Borrowed for the default, `system`.
    execution_id: std::borrow::Cow<'static, str>,
    /// The root's address, shared with its actor entry.
    root: Arc<str>,
    now: u64,
    actors: IndexMap<Arc<str>, ActorEntry, Fixed>,
    registry: IndexMap<String, ActorRef, Fixed>,
    messages: VecDeque<Message>,
    timers: IndexMap<String, SystemTimer, Fixed>,
    deadlines: BinaryHeap<Reverse<(u64, u64, u64)>>,
    external_effects: IndexMap<String, ExternalEffect, Fixed>,
    /// Set by [`crate::Actor`], which handles emits, logs, cancellations,
    /// cleanups and Rust-implemented actions as soon as a call returns them:
    /// such effects are returned without being stored as pending (the
    /// acknowledgement would remove them right away).
    transient_effects: bool,
    counters: Counters,
    max_steps: usize,
    // Per-call state.
    scratch: Option<Box<Scratch>>,
    remaining: usize,
}

/// Per-call state, lent from a per-thread pool for the length of a call
/// (idle systems are most of a partition's memory).
#[derive(Default)]
struct Scratch {
    out: Vec<ExternalEffect>,
    initial_effects: HashMap<ActorRef, Vec<Effect>>,
    stopping: HashSet<ActorRef>,
}

impl System {
    /// XState's `initialSystemTransition`: allocates and starts the root.
    /// Returns the system and the external effects produced.
    pub fn initialize(logic: &SystemLogic, options: InitOptions) -> Result<(Self, Vec<ExternalEffect>), SystemError> {
        let mut sys = Self {
            logic: logic.clone(),
            execution_id: execution_id(options.execution_id.clone()),
            root: Arc::from(""),
            now: options.time,
            actors: IndexMap::default(),
            registry: IndexMap::default(),
            messages: VecDeque::new(),
            timers: IndexMap::default(),
            deadlines: BinaryHeap::new(),
            external_effects: IndexMap::default(),
            transient_effects: false,
            counters: Counters::default(),
            max_steps: options.max_steps.max(1),
            scratch: None,
            remaining: 0,
        };
        sys.begin();
        let id = options.id.or_else(|| logic.root.logic_id()).unwrap_or_else(|| "root".into());
        let root_logic = logic.root.clone();
        let root = sys.allocate(
            &root_logic,
            AllocateOptions { id, input: options.input, registry_key: options.registry_key, ..Default::default() },
        )?;
        sys.root = root.address.clone();
        sys.start(&root)?;
        sys.settle()?;
        let effects = sys_end(&mut sys);
        Ok((sys, effects))
    }

    fn begin(&mut self) {
        if self.messages.capacity() == 0 {
            self.messages = take_message_queue();
        }
        self.remaining = self.max_steps;
        let scratch = self.scratch();
        scratch.out.clear();
        scratch.initial_effects.clear();
        scratch.stopping.clear();
    }

    fn scratch(&mut self) -> &mut Scratch {
        self.scratch.get_or_insert_with(take_scratch)
    }

    fn step(&mut self) -> Result<(), SystemError> {
        if self.remaining == 0 {
            return Err(SystemError::MaxSteps);
        }
        self.remaining -= 1;
        Ok(())
    }

    /// The root actor's address.
    pub fn root(&self) -> &str {
        &self.root
    }

    /// [`System::root`], shared.
    pub fn root_address(&self) -> &Arc<str> {
        &self.root
    }

    /// Virtual time.
    pub fn now(&self) -> u64 {
        self.now
    }

    /// The earliest pending deadline.
    pub fn next_deadline(&self) -> Option<u64> {
        // `deadlines` holds exactly the pending timers (see `forget_deadline`).
        self.deadlines.peek().map(|Reverse((due, _, _))| *due)
    }

    /// Removes a timer's entry from `deadlines`, so the heap holds exactly
    /// the pending timers.
    fn forget_deadline(&mut self, timer: &SystemTimer) {
        let entry = (timer.due_at, timer.sequence, timer.occurrence);
        self.deadlines.retain(|Reverse(e)| *e != entry);
    }

    /// Pending external work, by id.
    pub fn external_effects(&self) -> impl Iterator<Item = &ExternalEffect> {
        self.external_effects.values()
    }

    /// The current reference of the actor at `address`.
    pub fn actor_ref(&self, address: &str) -> Option<ActorRef> {
        self.actors.get(address).map(ActorEntry::reference)
    }

    /// The registry: key to actor.
    pub fn registry(&self) -> impl Iterator<Item = (&str, &ActorRef)> {
        self.registry.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// The snapshot of the actor at `address`.
    /// The logic the system was created with.
    pub fn logic(&self) -> &SystemLogic {
        &self.logic
    }

    pub fn snapshot(&self, address: &str) -> Option<&dyn AnySnapshot> {
        self.actors.get(address)?.snapshot.as_deref()
    }

    fn current(&self, r: &ActorRef) -> Option<&ActorEntry> {
        self.actors.get(&*r.address).filter(|a| a.incarnation == r.incarnation)
    }

    fn allocate(&mut self, logic: &Arc<dyn Logic>, options: AllocateOptions) -> Result<ActorRef, SystemError> {
        self.step()?;
        let Some((src, _)) = definitions(&self.logic).into_iter().find(|(_, l)| same_logic(l, logic)) else {
            return Err(SystemError::Failed("Register dynamically spawned logic in systemLogic.actors.".into()));
        };
        let segment = encode_address_segment(&options.id);
        let address: Arc<str> = match &options.parent {
            Some(p) => format!("{}/{}", p.address, segment).into(),
            None => segment.into(),
        };
        if let Some(occupied) = self.actors.get(&address) {
            if occupied.status == Status::Active {
                let previous = occupied.reference();
                if !self.scratch.as_ref().is_some_and(|s| s.stopping.contains(&previous)) {
                    return Err(SystemError::Failed(format!("Actor address '{address}' is already occupied.")));
                }
                self.stop(&previous)?;
            }
        }
        let incarnation = self.counters.actor;
        self.counters.actor += 1;
        let r = ActorRef { address: address.clone(), incarnation };
        let entry = ActorEntry {
            id: options.id.clone(),
            address: address.clone(),
            incarnation,
            src,
            logic: logic.clone(),
            logic_version: logic.version().map(str::to_owned),
            parent: options.parent.clone(),
            registry_key: options.registry_key.clone(),
            sync_snapshot: options.sync_snapshot,
            started: false,
            input: options.input.clone().map(Box::new),
            snapshot: None,
            status: Status::Active,
        };
        // Most systems hold one actor: no spare capacity per system.
        if self.actors.len() == self.actors.capacity() {
            self.actors.reserve_exact(1);
        }
        self.actors.insert(address.clone(), entry);
        if let Some(key) = &options.registry_key {
            if self.registry.contains_key(key) {
                return Err(SystemError::Failed(format!("Registry key '{key}' is already occupied.")));
            }
            self.registry.insert(key.clone(), r.clone());
        }
        let result = {
            let mut cx = Cx { sys: self, self_ref: r.clone() };
            logic.initial_transition(options.input.as_ref(), &mut cx)?
        };
        let effects = finalize(Status::Active, true, result.snapshot.status(), result.effects);
        self.store(&r, result.snapshot);
        self.scratch().initial_effects.insert(r.clone(), effects);
        Ok(r)
    }

    fn store(&mut self, r: &ActorRef, snapshot: Box<dyn AnySnapshot>) {
        if let Some(entry) = self.actors.get_mut(&*r.address).filter(|a| a.incarnation == r.incarnation) {
            entry.status = snapshot.status();
            entry.snapshot = Some(snapshot);
        }
    }

    fn external(&mut self, r: &ActorRef, spec: ExternalSpec) {
        let ty = match spec.ty {
            EffectType::Fixed(t) => t,
            EffectType::Action(name) => {
                let src = self.current(r).map(|a| a.src.clone()).unwrap_or_default();
                format!("{src}/actions/{}", encode_address_segment(&name))
            }
        };
        let transient = self.transient_effects
            && (spec.kind == EffectKind::Emit
                || spec.exec.is_some()
                || matches!(ty.as_str(), "@xstate.log" | "@xstate.cancelEffect" | "@xstate.cleanup"));
        // A transient effect is never looked up, so it gets no id string
        // (the counter still advances, as the effect id would).
        let id = if transient { String::new() } else { format!("{}:{}", self.execution_id, self.counters.effect) };
        self.counters.effect += 1;
        let mut params = spec.params;
        if spec.kind == EffectKind::Emit && params.is_none() && !transient {
            // An emit's params are its event without `type`.
            let mut p = spec.event.as_ref().and_then(Value::as_object).cloned().unwrap_or_default();
            p.remove("type");
            params = Some(Value::Object(p));
        }
        let effect = ExternalEffect {
            id: id.clone(),
            source: r.clone(),
            ty,
            kind: spec.kind,
            params,
            args: Some(spec.args),
            event: spec.event,
            reason: spec.reason,
            exec: spec.exec,
        };
        if transient {
            self.scratch().out.push(effect);
        } else {
            self.scratch().out.push(effect.clone());
            self.external_effects.insert(id, effect);
        }
    }

    /// See the `transient_effects` field; for [`crate::Actor`].
    pub(crate) fn set_transient_effects(&mut self, on: bool) {
        self.transient_effects = on;
    }

    fn enqueue(&mut self, source: Option<ActorRef>, target: ActorRef, event: Value, timer_occurrence: Option<u64>) {
        let sequence = self.counters.sequence;
        self.counters.sequence += 1;
        self.messages.push_back(Message { source, target, event, sequence, timer_occurrence });
    }

    fn cancel(&mut self, r: &ActorRef, key: Option<&Value>) {
        let cancelled = key.map(|k| system_timer_key_for(r, k));
        let mut removed = Vec::new();
        self.timers.retain(|timer_key, timer| {
            let keep = !(timer.source == *r && cancelled.as_ref().map_or(true, |c| c == timer_key));
            if !keep {
                removed.push((timer.due_at, timer.sequence, timer.occurrence));
            }
            keep
        });
        if !removed.is_empty() {
            self.deadlines.retain(|Reverse(e)| !removed.contains(e));
        }
    }

    fn effects_for(&mut self, r: &ActorRef, effects: Vec<Effect>) -> Result<(), SystemError> {
        for effect in effects {
            match effect {
                Effect::Spawn(_) => {}
                Effect::Start(child) => self.start(&child)?,
                Effect::Stop(child) => self.stop(&child)?,
                Effect::Cancel(key) => self.cancel(r, Some(&key)),
                Effect::DelayedRaise { key, delay } => {
                    let pending =
                        self.current(r).and_then(|a| a.snapshot.as_ref()).is_some_and(|s| s.timer_pending(&key));
                    if pending {
                        self.schedule(r, delay, TimerEvent::for_key(key))?;
                    }
                }
                Effect::SendTo { target, event } => self.enqueue(Some(r.clone()), target, event, None),
                Effect::Terminate => self.terminate(r),
                Effect::External(spec) => self.external(r, spec),
            }
        }
        Ok(())
    }

    fn schedule(&mut self, r: &ActorRef, delay: u64, event: TimerEvent) -> Result<(), SystemError> {
        let due_at = self.now.checked_add(delay).ok_or_else(|| {
            SystemError::Failed("System timer deadlines must be finite and delays nonnegative.".into())
        })?;
        let timer = SystemTimer {
            source: r.clone(),
            occurrence: self.counters.timer,
            scheduled_at: self.now,
            due_at,
            sequence: self.counters.sequence,
            event,
        };
        self.counters.timer += 1;
        self.counters.sequence += 1;
        let key = system_timer_key(r, &timer.event);
        if let Some(replaced) = self.timers.shift_remove(&key) {
            self.forget_deadline(&replaced);
        }
        if self.deadlines.len() == self.deadlines.capacity() {
            self.deadlines.reserve_exact(1);
        }
        self.deadlines.push(Reverse((timer.due_at, timer.sequence, timer.occurrence)));
        if self.timers.len() == self.timers.capacity() {
            self.timers.reserve_exact(1);
        }
        self.timers.insert(key, timer);
        Ok(())
    }

    fn start(&mut self, r: &ActorRef) -> Result<(), SystemError> {
        let Some(actor) = self.current(r) else { return Ok(()) };
        if actor.started || actor.status == Status::Stopped {
            return Ok(());
        }
        let logic = actor.logic.clone();
        self.actors.get_mut(&*r.address).expect("current").started = true;
        let effects = self.scratch.as_mut().and_then(|s| s.initial_effects.remove(r)).unwrap_or_default();
        self.effects_for(r, effects)?;
        let active = self.current(r).is_some_and(|a| a.status == Status::Active);
        if active {
            if let Some((timeout, delay)) = logic.async_timeout() {
                let shown = match &timeout {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                let error =
                    json!({ "$error": "TimeoutError", "message": format!("Async logic timed out after {shown}.") });
                self.schedule(r, delay, TimerEvent::Other(json!({ "type": "xstate.async.reject", "data": error })))?;
            }
        }
        Ok(())
    }

    fn stop(&mut self, r: &ActorRef) -> Result<(), SystemError> {
        if !self.current(r).is_some_and(|a| a.status == Status::Active) {
            return Ok(());
        }
        let sequence = self.counters.sequence;
        self.counters.sequence += 1;
        self.process(Message {
            source: Some(r.clone()),
            target: r.clone(),
            event: json!({ "type": "xstate.stop" }),
            sequence,
            timer_occurrence: None,
        })?;
        self.cancel(r, None);
        self.unregister(r);
        self.messages.retain(|m| m.target != *r);
        self.cancel_external(r);
        Ok(())
    }

    fn unregister(&mut self, r: &ActorRef) {
        self.scratch().stopping.insert(r.clone());
        self.registry.retain(|_, v| v != r);
    }

    fn cancel_external(&mut self, r: &ActorRef) {
        let ids: Vec<String> = self
            .external_effects
            .values()
            .filter(|e| {
                e.kind == EffectKind::Action
                    && e.ty != "@xstate.cancelEffect"
                    && e.ty != "@xstate.cleanup"
                    && e.source == *r
            })
            .map(|e| e.id.clone())
            .collect();
        for id in ids {
            self.external_effects.shift_remove(&id);
            let cid = format!("{}:{}", self.execution_id, self.counters.effect);
            self.counters.effect += 1;
            let cancellation = ExternalEffect {
                id: cid.clone(),
                source: r.clone(),
                ty: "@xstate.cancelEffect".into(),
                kind: EffectKind::Action,
                params: Some(json!({ "effectId": id })),
                args: None,
                event: None,
                reason: None,
                exec: None,
            };
            self.scratch().out.push(cancellation.clone());
            self.external_effects.insert(cid, cancellation);
        }
    }

    fn terminate(&mut self, r: &ActorRef) {
        let Some(actor) = self.current(r) else { return };
        let parent = actor.parent.clone();
        let id = actor.id.clone();
        let (status, output, error) = match &actor.snapshot {
            Some(s) => (s.status(), s.output(), s.error()),
            None => (actor.status, None, None),
        };
        self.cancel(r, None);
        self.cancel_external(r);
        if let Some(parent) = parent {
            let mut event = Map::new();
            if status == Status::Done {
                event.insert("type".into(), Value::String("xstate.done.actor".into()));
                if let Some(o) = output {
                    event.insert("output".into(), o);
                }
            } else {
                event.insert("type".into(), Value::String("xstate.error.actor".into()));
                if let Some(e) = error {
                    event.insert("error".into(), e);
                }
            }
            event.insert("actorId".into(), Value::String(id));
            event.insert("sessionId".into(), Value::String(r.incarnation.to_string()));
            self.enqueue(Some(r.clone()), parent, Value::Object(event), None);
        }
    }

    fn process(&mut self, message: Message) -> Result<(), SystemError> {
        self.step()?;
        let Some(actor) = self.current(&message.target) else { return Ok(()) };
        if actor.status != Status::Active {
            return Ok(());
        }
        let event_type = message.event.get("type").and_then(Value::as_str).unwrap_or("").to_owned();
        if event_type == "xstate.timer" && message.timer_occurrence.is_none() {
            return Err(SystemError::Failed("Only the system scheduler may deliver a timer occurrence.".into()));
        }
        let logic = actor.logic.clone();
        if logic.is_internal_event_type(&event_type) && message.source.as_ref() != Some(&message.target) {
            return Err(SystemError::Failed(format!(
                "Internal event '{event_type}' cannot be injected into a pure system."
            )));
        }
        if message.source.is_none()
            && (event_type == "xstate.after"
                || event_type.starts_with("xstate.after.")
                || event_type == "xstate.timeout"
                || event_type.starts_with("xstate.timeout."))
        {
            return Err(SystemError::Failed("Select a pending timer instead of injecting its internal event.".into()));
        }
        let target = message.target.clone();
        let entry = self.actors.get_mut(&*target.address).expect("current");
        let previous_status = entry.status;
        let mut snapshot = entry.snapshot.take().expect("snapshot present outside its transition");
        let result = {
            let mut cx = Cx { sys: self, self_ref: target.clone() };
            logic.transition(&mut snapshot, message.event, &mut cx)
        };
        let effects = match result {
            Ok(effects) => effects,
            Err(e) => {
                self.store(&target, snapshot);
                return Err(e);
            }
        };
        let status = snapshot.status();
        let effects = finalize(previous_status, false, status, effects);
        // Only a child invoked with `onSnapshot` reports its snapshot.
        let reports_snapshot = self
            .current(&target)
            .is_some_and(|a| a.sync_snapshot == Some(true) && a.parent.is_some() && status == Status::Active);
        let data_for_parent = reports_snapshot.then(|| logic.snapshot_data(&*snapshot));
        self.store(&target, snapshot);
        self.effects_for(&target, effects)?;
        if let Some(actor) = self.current(&target) {
            if actor.sync_snapshot == Some(true) && status == Status::Active {
                if let Some(parent) = actor.parent.clone() {
                    let event = json!({
                        "type": "xstate.snapshot.actor",
                        "actorId": actor.id,
                        "sessionId": actor.incarnation.to_string(),
                        "snapshot": data_for_parent.unwrap_or(Value::Null),
                    });
                    self.enqueue(Some(target.clone()), parent, event, None);
                }
            }
        }
        Ok(())
    }

    fn settle(&mut self) -> Result<(), SystemError> {
        while let Some(message) = self.messages.pop_front() {
            self.process(message)?;
        }
        Ok(())
    }

    fn advance_to(&mut self, time: u64) -> Result<(), SystemError> {
        if time < self.now {
            return rejected("System time must be finite and cannot move backwards.");
        }
        self.settle()?;
        while self.fire_one(time)? {}
        self.now = time;
        Ok(())
    }

    /// Fires the earliest timer occurrence due at or before `time` and
    /// completes its work. Returns whether an occurrence was due.
    fn fire_one(&mut self, time: u64) -> Result<bool, SystemError> {
        while let Some(&Reverse((due, sequence, occurrence))) = self.deadlines.peek() {
            // Nothing is due when the earliest deadline, current or stale,
            // is later (the common case: no lookup).
            if due > time {
                return Ok(false);
            }
            let current_key = self
                .timers
                .iter()
                .find(|(_, t)| t.occurrence == occurrence && t.sequence == sequence)
                .map(|(k, _)| k.clone());
            let Some(key) = current_key else {
                self.deadlines.pop();
                continue;
            };
            if due > time {
                return Ok(false);
            }
            self.step()?;
            self.deadlines.pop();
            let timer = self.timers.shift_remove(&key).expect("found");
            self.now = self.now.max(timer.due_at);
            let Some(actor) = self.current(&timer.source) else { return Ok(true) };
            if actor.status != Status::Active {
                return Ok(true);
            }
            if timer.event.is_machine_timer() {
                let key = timer.event.machine_key();
                let pending = match (&key, &actor.snapshot) {
                    (Some(k), Some(s)) => s.timer_pending(k),
                    _ => false,
                };
                if !pending {
                    return Ok(true);
                }
            }
            let event = match timer.event {
                TimerEvent::Other(event) => event,
                typed => typed.to_value(),
            };
            self.enqueue(Some(timer.source.clone()), timer.source.clone(), event, Some(timer.occurrence));
            self.settle()?;
            return Ok(true);
        }
        Ok(false)
    }

    /// Fires only the earliest timer occurrence due at or before `time` (the
    /// clock moves to its due time), as a live actor processes each timer
    /// as its own step. Returns `None` when no timer is due.
    pub fn fire_next_timer(&mut self, time: u64) -> Result<Option<Vec<ExternalEffect>>, SystemError> {
        self.begin();
        if time < self.now {
            return rejected("System time must be finite and cannot move backwards.");
        }
        self.settle()?;
        let fired = self.fire_one(time)?;
        let effects = sys_end(self);
        Ok(fired.then_some(effects))
    }

    /// XState's `advanceSystemTime`: fires every timer due at or before
    /// `time`, chronologically, then sets the clock to `time`.
    pub fn advance(&mut self, time: u64) -> Result<Vec<ExternalEffect>, SystemError> {
        self.begin();
        self.advance_to(time)?;
        Ok(sys_end(self))
    }

    /// XState's `systemTransition`: delivers `event` to the actor at
    /// `address` and completes all resulting work. Due timers fire first.
    pub fn send(&mut self, address: &str, event: Value) -> Result<Vec<ExternalEffect>, SystemError> {
        self.begin();
        let ty = event.get("type").and_then(Value::as_str).unwrap_or("").to_owned();
        if ty == "xstate.timer" {
            let Some(actor) = self.actors.get(address) else {
                return rejected(format!("Unknown actor address '{address}'."));
            };
            let r = actor.reference();
            let occurrence = event.get("occurrence").and_then(Value::as_u64);
            let Some(due) =
                self.timers.values().find(|t| t.source == r && Some(t.occurrence) == occurrence).map(|t| t.due_at)
            else {
                return rejected("Select a current timer occurrence.");
            };
            self.advance_to(due)?;
            return Ok(sys_end(self));
        }
        self.advance_to(self.now)?;
        let Some(actor) = self.actors.get(address) else {
            return rejected(format!("Unknown actor address '{address}'."));
        };
        let r = actor.reference();
        let status = actor.status;
        if ty == "xstate.system.effect.result" {
            let effect_id = event.get("effectId").and_then(Value::as_str).unwrap_or("");
            let inner = event.get("event").cloned();
            let valid = self.external_effects.get(effect_id).is_some_and(|e| {
                *e.source.address == *address
                    && (inner.is_none()
                        || (e.source.incarnation == r.incarnation
                            && status == Status::Active
                            && e.ty != "@xstate.cancelEffect"
                            && e.ty != "@xstate.cleanup"))
            });
            if !valid {
                return rejected("External effect result does not match its owner or is stale.");
            }
            self.external_effects.shift_remove(effect_id);
            if let Some(inner) = inner {
                self.enqueue(Some(r.clone()), r, inner, None);
                self.settle()?;
            }
            return Ok(sys_end(self));
        }
        if ty == "xstate.stop" {
            self.stop(&r)?;
            self.settle()?;
            return Ok(sys_end(self));
        }
        if ty.starts_with("xstate.async.") || ty.starts_with("xstate.logic.effect.") {
            return rejected("Deliver external work results with xstate.system.effect.result.");
        }
        self.enqueue(None, r, event, None);
        self.settle()?;
        Ok(sys_end(self))
    }

    /// Acknowledges external work, optionally delivering its result to the
    /// actor that requested it (`xstate.system.effect.result`).
    pub fn resolve_effect(
        &mut self,
        effect_id: &str,
        event: Option<Value>,
    ) -> Result<Vec<ExternalEffect>, SystemError> {
        let Some(effect) = self.external_effects.get(effect_id) else {
            return rejected("External effect result does not match its owner or is stale.");
        };
        let address = effect.source.address.to_string();
        let mut result = json!({ "type": "xstate.system.effect.result", "effectId": effect_id });
        if let Some(e) = event {
            result["event"] = e;
        }
        self.send(&address, result)
    }

    /// Retires pending external work without delivering a result (an
    /// `xstate.system.effect.result` without `event`). Returns whether it
    /// was pending.
    pub fn acknowledge(&mut self, effect_id: &str) -> bool {
        !self.external_effects.is_empty() && self.external_effects.shift_remove(effect_id).is_some()
    }

    /// XState's `SystemSnapshot` as JSON.
    pub fn to_json(&self) -> Value {
        let mut actors = Map::new();
        for (address, a) in &self.actors {
            let mut m = Map::new();
            m.insert("id".into(), Value::String(a.id.clone()));
            m.insert("address".into(), Value::String(address.to_string()));
            m.insert("incarnation".into(), Value::String(a.incarnation.to_string()));
            m.insert("src".into(), Value::String(a.src.clone()));
            if let Some(v) = &a.logic_version {
                m.insert("logicVersion".into(), Value::String(v.clone()));
            }
            if let Some(p) = &a.parent {
                m.insert("parent".into(), p.to_json());
            }
            if let Some(k) = &a.registry_key {
                m.insert("registryKey".into(), Value::String(k.clone()));
            }
            if let Some(s) = a.sync_snapshot {
                m.insert("syncSnapshot".into(), Value::Bool(s));
            }
            m.insert("started".into(), Value::Bool(a.started));
            if let Some(i) = &a.input {
                m.insert("input".into(), (**i).clone());
            }
            m.insert("snapshot".into(), a.snapshot_data(Some(&a.logic)));
            actors.insert(address.to_string(), Value::Object(m));
        }
        let registry: Map<String, Value> = self.registry.iter().map(|(k, v)| (k.clone(), v.to_json())).collect();
        let timers: Vec<Value> = self
            .timers
            .values()
            .map(|t| {
                json!({
                    "source": t.source.to_json(),
                    "occurrence": t.occurrence,
                    "scheduledAt": t.scheduled_at,
                    "dueAt": t.due_at,
                    "sequence": t.sequence,
                    "event": t.event.to_value(),
                })
            })
            .collect();
        let effects: Map<String, Value> = self.external_effects.iter().map(|(k, e)| (k.clone(), e.to_json())).collect();
        let messages: Vec<Value> = self
            .messages
            .iter()
            .map(|m| {
                let mut o = Map::new();
                if let Some(s) = &m.source {
                    o.insert("source".into(), s.to_json());
                }
                o.insert("target".into(), m.target.to_json());
                o.insert("event".into(), m.event.clone());
                o.insert("sequence".into(), json!(m.sequence));
                if let Some(t) = m.timer_occurrence {
                    o.insert("timerOccurrence".into(), json!(t));
                }
                Value::Object(o)
            })
            .collect();
        // Built by moving the parts in: `json!` would copy maps through
        // serialization.
        let mut counters = Map::with_capacity(4);
        counters.insert("actor".into(), Value::from(self.counters.actor));
        counters.insert("timer".into(), Value::from(self.counters.timer));
        counters.insert("sequence".into(), Value::from(self.counters.sequence));
        counters.insert("effect".into(), Value::from(self.counters.effect));
        let mut out = Map::with_capacity(9);
        out.insert("root".into(), Value::String(self.root.to_string()));
        out.insert("executionId".into(), Value::String(self.execution_id.to_string()));
        out.insert("now".into(), Value::from(self.now));
        out.insert("actors".into(), Value::Object(actors));
        out.insert("registry".into(), Value::Object(registry));
        out.insert("messages".into(), Value::Array(messages));
        out.insert("timers".into(), Value::Array(timers));
        out.insert("externalEffects".into(), Value::Object(effects));
        out.insert("counters".into(), Value::Object(counters));
        Value::Object(out)
    }

    /// Restores a system from [`System::to_json`] data and the same logic.
    pub fn from_json(logic: &SystemLogic, data: &Value, max_steps: usize) -> Result<Self, SystemError> {
        let bad = |what: &str| SystemError::Rejected(format!("invalid system snapshot: {what}"));
        let defs = definitions(logic);
        let mut actors = IndexMap::default();
        for (address, a) in data.get("actors").and_then(Value::as_object).ok_or_else(|| bad("actors"))? {
            let src = a.get("src").and_then(Value::as_str).ok_or_else(|| bad("src"))?.to_owned();
            let l = defs
                .iter()
                .find(|(k, _)| *k == src)
                .map(|(_, l)| l.clone())
                .ok_or_else(|| SystemError::Rejected(format!("Missing actor definition '{src}'.")))?;
            let logic_version = a.get("logicVersion").and_then(Value::as_str).map(str::to_owned);
            if logic_version.as_deref() != l.version() {
                return Err(SystemError::Rejected(format!("Actor definition/version mismatch for '{address}'.")));
            }
            let snapshot = l.restore(a.get("snapshot").ok_or_else(|| bad("snapshot"))?)?;
            let address: Arc<str> = address.as_str().into();
            actors.insert(
                address.clone(),
                ActorEntry {
                    id: a.get("id").and_then(Value::as_str).ok_or_else(|| bad("id"))?.to_owned(),
                    address,
                    incarnation: a
                        .get("incarnation")
                        .and_then(Value::as_str)
                        .and_then(|s| s.parse().ok())
                        .ok_or_else(|| bad("incarnation"))?,
                    src,
                    logic: l,
                    logic_version,
                    parent: a.get("parent").and_then(ActorRef::from_json),
                    registry_key: a.get("registryKey").and_then(Value::as_str).map(str::to_owned),
                    sync_snapshot: a.get("syncSnapshot").and_then(Value::as_bool),
                    started: a.get("started").and_then(Value::as_bool).unwrap_or(false),
                    input: a.get("input").cloned().map(Box::new),
                    status: snapshot.status(),
                    snapshot: Some(snapshot),
                },
            );
        }
        let registry = data
            .get("registry")
            .and_then(Value::as_object)
            .map(|m| m.iter().filter_map(|(k, v)| Some((k.clone(), ActorRef::from_json(v)?))).collect())
            .unwrap_or_default();
        let mut timers = IndexMap::default();
        let mut deadlines = BinaryHeap::new();
        for t in data.get("timers").and_then(Value::as_array).into_iter().flatten() {
            let timer = SystemTimer {
                source: ActorRef::from_json(t.get("source").ok_or_else(|| bad("timer source"))?)
                    .ok_or_else(|| bad("timer source"))?,
                occurrence: t.get("occurrence").and_then(Value::as_u64).ok_or_else(|| bad("occurrence"))?,
                scheduled_at: t.get("scheduledAt").and_then(Value::as_u64).unwrap_or(0),
                due_at: t.get("dueAt").and_then(Value::as_u64).ok_or_else(|| bad("dueAt"))?,
                sequence: t.get("sequence").and_then(Value::as_u64).unwrap_or(0),
                event: TimerEvent::from_value(t.get("event").cloned().ok_or_else(|| bad("timer event"))?),
            };
            deadlines.push(Reverse((timer.due_at, timer.sequence, timer.occurrence)));
            timers.insert(system_timer_key(&timer.source, &timer.event), timer);
        }
        let mut external_effects = IndexMap::default();
        for (id, e) in data.get("externalEffects").and_then(Value::as_object).into_iter().flatten() {
            external_effects.insert(id.clone(), ExternalEffect::from_json(e).ok_or_else(|| bad("effect"))?);
        }
        let messages = data
            .get("messages")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|m| {
                Some(Message {
                    source: m.get("source").and_then(ActorRef::from_json),
                    target: ActorRef::from_json(m.get("target")?)?,
                    event: m.get("event")?.clone(),
                    sequence: m.get("sequence")?.as_u64()?,
                    timer_occurrence: m.get("timerOccurrence").and_then(Value::as_u64),
                })
            })
            .collect();
        let c = data.get("counters").ok_or_else(|| bad("counters"))?;
        let counter = |k: &str| c.get(k).and_then(Value::as_u64).unwrap_or(0);
        Ok(Self {
            logic: logic.clone(),
            execution_id: execution_id(data.get("executionId").and_then(Value::as_str).unwrap_or("system").to_owned()),
            root: {
                let root = data.get("root").and_then(Value::as_str).unwrap_or("");
                actors.get_key_value(root).map_or_else(|| Arc::from(root), |(k, _)| k.clone())
            },
            now: data.get("now").and_then(Value::as_u64).unwrap_or(0),
            actors,
            registry,
            messages,
            timers,
            deadlines,
            external_effects,
            transient_effects: false,
            counters: Counters {
                actor: counter("actor"),
                timer: counter("timer"),
                sequence: counter("sequence"),
                effect: counter("effect"),
            },
            max_steps: max_steps.max(1),
            scratch: None,
            remaining: 0,
        })
    }
}

fn sys_end(sys: &mut System) -> Vec<ExternalEffect> {
    // Between calls an actor keeps no scratch capacity: idle actors are
    // what a partition holds most of, so their memory counts. The message
    // queue goes back to a per-thread pool; the rest is rarely used.
    if sys.messages.is_empty() {
        return_message_queue(std::mem::take(&mut sys.messages));
    }
    let Some(mut scratch) = sys.scratch.take() else { return Vec::new() };
    let out = std::mem::take(&mut scratch.out);
    return_scratch(scratch);
    out
}

thread_local! {
    /// Per-call state lent to systems for the length of a call (boxed: a
    /// system holds the box itself while it runs).
    #[allow(clippy::vec_box)]
    static SCRATCH: std::cell::RefCell<Vec<Box<Scratch>>> = const { std::cell::RefCell::new(Vec::new()) };
}

fn take_scratch() -> Box<Scratch> {
    SCRATCH.with(|p| p.borrow_mut().pop()).unwrap_or_default()
}

fn return_scratch(mut scratch: Box<Scratch>) {
    if scratch.initial_effects.capacity() > 64 || scratch.stopping.capacity() > 64 {
        return;
    }
    scratch.initial_effects.clear();
    scratch.stopping.clear();
    // `try_with`: thread-local storage may already be gone at exit.
    let _ = SCRATCH.try_with(|p| {
        let mut pool = p.borrow_mut();
        if pool.len() < 16 {
            pool.push(scratch);
        }
    });
}

thread_local! {
    /// Empty message queues lent to systems for the length of a call.
    static MESSAGE_QUEUES: std::cell::RefCell<Vec<VecDeque<Message>>> = const { std::cell::RefCell::new(Vec::new()) };
}

fn take_message_queue() -> VecDeque<Message> {
    MESSAGE_QUEUES.with(|p| p.borrow_mut().pop()).unwrap_or_default()
}

fn return_message_queue(queue: VecDeque<Message>) {
    if queue.capacity() > 0 && queue.capacity() <= 64 {
        // `try_with`: thread-local storage may already be gone at exit.
        let _ = MESSAGE_QUEUES.try_with(|p| {
            let mut pool = p.borrow_mut();
            if pool.len() < 16 {
                pool.push(queue);
            }
        });
    }
}

/// XState's `finalizeTransitionResult`: a newly terminal snapshot gets one
/// termination effect.
fn finalize(previous: Status, initial: bool, status: Status, mut effects: Vec<Effect>) -> Vec<Effect> {
    let became_terminal = matches!(status, Status::Done | Status::Error);
    let was_terminal = !initial && matches!(previous, Status::Done | Status::Error);
    if became_terminal && !was_terminal && !effects.iter().any(|e| matches!(e, Effect::Terminate)) {
        effects.push(Effect::Terminate);
    }
    effects
}

struct Cx<'a> {
    sys: &'a mut System,
    self_ref: ActorRef,
}

impl Context for Cx<'_> {
    fn self_ref(&self) -> ActorRef {
        self.self_ref.clone()
    }

    fn now(&self) -> u64 {
        self.sys.now
    }

    fn parent_ref(&self) -> Option<ActorRef> {
        self.sys.current(&self.self_ref).and_then(|a| a.parent.clone())
    }

    fn allocate(&mut self, logic: &Arc<dyn Logic>, options: AllocateOptions) -> Result<ActorRef, SystemError> {
        self.sys.allocate(logic, options)
    }

    fn status_of(&self, actor: &ActorRef) -> Status {
        self.sys.current(actor).map_or(Status::Stopped, |a| a.status)
    }

    fn unregister(&mut self, actor: &ActorRef) {
        self.sys.unregister(actor);
    }
}

/// Parses a status from snapshot data.
pub fn parse_status(data: &Value) -> Result<Status, SystemError> {
    data.get("status")
        .and_then(Value::as_str)
        .and_then(Status::parse)
        .ok_or_else(|| SystemError::Rejected("snapshot data has no valid status".into()))
}

/// Downcasts a snapshot.
pub fn downcast<T: 'static>(snapshot: &dyn AnySnapshot) -> Option<&T> {
    snapshot.as_any().downcast_ref::<T>()
}

/// A borrowed view of a [`System`] that serializes exactly as
/// [`System::to_json`] does, without building the `Value` first: snapshots
/// are written straight into the output encoding.
pub struct SystemView<'a> {
    sys: &'a System,
    compact: bool,
}

struct RefView<'a>(&'a ActorRef);

impl Serialize for RefView<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(Some(2))?;
        m.serialize_entry("$actor", &*self.0.address)?;
        m.serialize_entry("incarnation", &self.0.incarnation.to_string())?;
        m.end()
    }
}

struct EntryView<'a>(&'a ActorEntry, bool);

impl Serialize for EntryView<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let (a, compact) = (self.0, self.1);
        // Compact: the address is the key of the entry already.
        let len = 6 - usize::from(compact)
            + usize::from(a.logic_version.is_some())
            + usize::from(a.parent.is_some())
            + usize::from(a.registry_key.is_some())
            + usize::from(a.sync_snapshot.is_some())
            + usize::from(a.input.is_some());
        let mut m = s.serialize_map(Some(len))?;
        m.serialize_entry("id", &a.id)?;
        if !compact {
            m.serialize_entry("address", &*a.address)?;
        }
        m.serialize_entry("incarnation", &a.incarnation.to_string())?;
        m.serialize_entry("src", &a.src)?;
        if let Some(v) = &a.logic_version {
            m.serialize_entry("logicVersion", v)?;
        }
        if let Some(p) = &a.parent {
            m.serialize_entry("parent", &RefView(p))?;
        }
        if let Some(k) = &a.registry_key {
            m.serialize_entry("registryKey", k)?;
        }
        if let Some(v) = a.sync_snapshot {
            m.serialize_entry("syncSnapshot", &v)?;
        }
        m.serialize_entry("started", &a.started)?;
        if let Some(i) = &a.input {
            m.serialize_entry("input", i)?;
        }
        match &a.snapshot {
            Some(snap) => m.serialize_entry("snapshot", &*a.logic.snapshot_serialize(&**snap, compact))?,
            None => m.serialize_entry("snapshot", &a.snapshot_data(Some(&a.logic)))?,
        }
        m.end()
    }
}

struct ActorsView<'a>(&'a System, bool);

impl Serialize for ActorsView<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(Some(self.0.actors.len()))?;
        for (address, a) in &self.0.actors {
            m.serialize_entry(&**address, &EntryView(a, self.1))?;
        }
        m.end()
    }
}

struct TimerView<'a>(&'a SystemTimer);

impl Serialize for TimerView<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let t = self.0;
        let mut m = s.serialize_map(Some(6))?;
        m.serialize_entry("source", &RefView(&t.source))?;
        m.serialize_entry("occurrence", &t.occurrence)?;
        m.serialize_entry("scheduledAt", &t.scheduled_at)?;
        m.serialize_entry("dueAt", &t.due_at)?;
        m.serialize_entry("sequence", &t.sequence)?;
        m.serialize_entry("event", &t.event)?;
        m.end()
    }
}

struct Seq<I>(std::cell::RefCell<Option<I>>);

impl<I: Iterator<Item = T>, T: Serialize> Serialize for Seq<I> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let items = self.0.borrow_mut().take().expect("serialized once");
        let len = match items.size_hint() {
            (low, Some(high)) if low == high => Some(low),
            _ => None,
        };
        let mut seq = s.serialize_seq(len)?;
        for item in items {
            seq.serialize_element(&item)?;
        }
        seq.end()
    }
}

impl Serialize for SystemView<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let (sys, compact) = (self.sys, self.compact);
        // Compact leaves out what restore defaults to: the default execution
        // id and empty registry, messages, timers and external effects.
        let keep_execution_id = !compact || sys.execution_id != "system";
        let (keep_registry, keep_messages, keep_timers, keep_effects) = if compact {
            (
                !sys.registry.is_empty(),
                !sys.messages.is_empty(),
                !sys.timers.is_empty(),
                !sys.external_effects.is_empty(),
            )
        } else {
            (true, true, true, true)
        };
        let len = 4 + [keep_execution_id, keep_registry, keep_messages, keep_timers, keep_effects]
            .iter()
            .filter(|k| **k)
            .count();
        let mut m = s.serialize_map(Some(len))?;
        m.serialize_entry("root", &*sys.root)?;
        if keep_execution_id {
            m.serialize_entry("executionId", &sys.execution_id)?;
        }
        m.serialize_entry("now", &sys.now)?;
        m.serialize_entry("actors", &ActorsView(sys, compact))?;
        if keep_registry {
            m.serialize_entry("registry", &RegistryView(&sys.registry))?;
        }
        // Messages and external effects are rare at rest; reuse `to_json`'s.
        let rest = if sys.messages.is_empty() && sys.external_effects.is_empty() { None } else { Some(sys.to_json()) };
        if keep_messages {
            match &rest {
                Some(v) => m.serialize_entry("messages", &v["messages"])?,
                None => m.serialize_entry("messages", &[(); 0])?,
            }
        }
        if keep_timers {
            m.serialize_entry("timers", &Seq(std::cell::RefCell::new(Some(sys.timers.values().map(TimerView)))))?;
        }
        if keep_effects {
            match &rest {
                Some(v) => m.serialize_entry("externalEffects", &v["externalEffects"])?,
                None => m.serialize_entry("externalEffects", &Map::new())?,
            }
        }
        m.serialize_entry("counters", &CountersView(&sys.counters))?;
        m.end()
    }
}

struct RegistryView<'a>(&'a IndexMap<String, ActorRef, Fixed>);

impl Serialize for RegistryView<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_map(self.0.iter().map(|(k, r)| (k, RefView(r))))
    }
}

struct CountersView<'a>(&'a Counters);

impl Serialize for CountersView<'_> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let c = self.0;
        let mut m = s.serialize_map(Some(4))?;
        m.serialize_entry("actor", &c.actor)?;
        m.serialize_entry("timer", &c.timer)?;
        m.serialize_entry("sequence", &c.sequence)?;
        m.serialize_entry("effect", &c.effect)?;
        m.end()
    }
}

impl System {
    /// The system snapshot as a [`SystemView`]: serializes to the same data
    /// as [`System::to_json`], without building it.
    pub fn view(&self) -> SystemView<'_> {
        SystemView { sys: self, compact: false }
    }

    /// A smaller snapshot that [`System::from_json`] restores to the same
    /// system: XState's `SystemSnapshot` without the fields restore rebuilds
    /// identically when they are missing (empty collections, the default
    /// execution id, actor addresses, which are the keys of `actors`, and in
    /// machine snapshots the state `value`, derived from `nodeIds`, and
    /// empty `children`, `historyValue`, `timers` and `_stateInputs`). It is
    /// not meant for XState's own restore.
    pub fn compact_view(&self) -> SystemView<'_> {
        SystemView { sys: self, compact: true }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timer_keys() {
        let r = ActorRef { address: "a".into(), incarnation: 0 };
        let key = |event: Value| system_timer_key(&r, &TimerEvent::from_value(event));
        let seq = key(json!({"type":"xstate.timer","seq":0}));
        assert_eq!(seq, system_timer_key_for(&r, &json!({"seq":0})));
        assert_ne!(seq, key(json!({"type":"xstate.timer","id":"0"})));
        let after = json!({"type":"xstate.after","delay":10,"stateId":"m.a","extra":1});
        let k = key(json!({"type":"xstate.timer","event":after}));
        assert_eq!(k, system_timer_key_for(&r, &json!({"event":{"stateId":"m.a","type":"xstate.after","delay":10}})));
        assert_eq!(k, r#""a",0,event,{"type":"xstate.after","delay":10,"stateId":"m.a"}"#);
        // Typed shapes give the same keys and events as their JSON.
        let typed = json!({"type":"xstate.timer","event":{"type":"xstate.after","delay":"short","stateId":"m.a"}});
        assert!(matches!(TimerEvent::from_value(typed.clone()), TimerEvent::After { .. }));
        assert_eq!(key(typed.clone()), r#""a",0,event,{"type":"xstate.after","delay":"short","stateId":"m.a"}"#);
        for event in [
            typed,
            json!({"type":"xstate.timer","id":"t"}),
            json!({"type":"xstate.timer","seq":3}),
            json!({"type":"xstate.timer","event":{"type":"xstate.timeout.state","stateId":"m.b"}}),
            json!({"type":"xstate.timer","event":{"type":"xstate.timeout.actor","actorId":"c","sessionId":"1"}}),
            json!({"type":"xstate.async.reject","data":{}}),
            json!({"seq":3,"type":"xstate.timer"}),
        ] {
            let parsed = TimerEvent::from_value(event.clone());
            assert_eq!(parsed.to_value().to_string(), event.to_string());
            assert_eq!(serde_json::to_string(&parsed).unwrap(), event.to_string());
            assert_eq!(parsed.machine_key(), read_timer_key(&event).filter(|_| event["type"] == TIMER));
        }
        assert_eq!(read_timer_key(&json!({"type":"xstate.timer","id":"t"})), Some(json!({"id":"t"})));
        assert_eq!(encode_address_segment("a/b%"), "a%2Fb%25");
    }
}
