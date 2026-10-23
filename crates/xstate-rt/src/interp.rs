//! The native host: JSON machines as pure actor logic on the shared
//! [`crate::core`] algorithm, with XState's value semantics for
//! `machineFromJSON` machines.
//!
//! - Transition results (`context`, `actions`) are computed against the
//!   snapshot at the start of the microstep and merged after exits, as
//!   XState's transition functions are.
//! - Actions produce effects in XState's vocabulary. `@xstate.raise`
//!   without a delay goes to the internal queue; delayed raises and cancels
//!   update the snapshot's logical timers and return timer effects; emits,
//!   logs and custom actions become external effects.
//! - Invoked children are allocated through the system while the state is
//!   entered and started after the macrostep, as in XState.
//! - Eventless transitions are re-selected only after a microstep that
//!   produced a new snapshot, mirroring XState's snapshot-identity check.

use std::any::Any;
use std::borrow::Cow;
use std::collections::VecDeque;
use std::sync::Arc;

use indexmap::IndexMap;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Map, Value};

use crate::core::{self, Host, MicrostepResult, Nid, RegionOutput, Resolved, Resolver, TRef, Tid};
use crate::event::{Ev, EventType, AFTER, DONE_STATE};
use crate::machine::*;
use crate::registry::GuardArgs;
use crate::system::{
    builtin_timer_identity, ActorRef, AllocateOptions, AnySnapshot, Context, Effect, EffectKind, EffectType,
    ExternalSpec, Logic, Status, SystemError, Transition,
};

/// A logical timer in a machine snapshot (XState's `snapshot.timers` entry).
#[derive(Clone)]
pub(crate) struct LogicalTimer<E> {
    pub id: Option<String>,
    pub seq: Option<u64>,
    pub delay: u64,
    pub event: TimerEv<E>,
}

/// A logical timer's event. `after` events (most timers) are kept without
/// their JSON object, which costs several allocations per pending timer.
#[derive(Clone)]
pub(crate) enum TimerEv<E> {
    /// `{ type: "xstate.after", delay, stateId }`.
    After {
        delay: Value,
        state_id: String,
    },
    Ev(Ev<E>),
}

impl<E: EventType> TimerEv<E> {
    fn new(event: Ev<E>) -> Self {
        let after = |data: &Map<String, Value>| {
            data.len() == 2 && data.keys().zip(["delay", "stateId"]).all(|(k, n)| k == n) && data["stateId"].is_string()
        };
        match event {
            Ev::Internal { event_type, mut data } if event_type == AFTER && after(&data) => {
                let Some(Value::String(state_id)) = data.swap_remove("stateId") else { unreachable!() };
                TimerEv::After { delay: data.swap_remove("delay").expect("checked"), state_id }
            }
            // A restored untyped event.
            Ev::User(e) if e.event_type() == AFTER => match e.to_json() {
                Value::Object(mut data) if data.keys().next().map(String::as_str) == Some("type") => {
                    data.shift_remove("type");
                    if after(&data) {
                        let Some(Value::String(state_id)) = data.swap_remove("stateId") else { unreachable!() };
                        TimerEv::After { delay: data.swap_remove("delay").expect("checked"), state_id }
                    } else {
                        TimerEv::Ev(Ev::User(e))
                    }
                }
                _ => TimerEv::Ev(Ev::User(e)),
            },
            other => TimerEv::Ev(other),
        }
    }

    fn event_type(&self) -> &str {
        match self {
            TimerEv::After { .. } => AFTER,
            TimerEv::Ev(e) => e.event_type(),
        }
    }

    fn field(&self, key: &str) -> Option<Value> {
        match (self, key) {
            (TimerEv::After { delay, .. }, "delay") => Some(delay.clone()),
            (TimerEv::After { state_id, .. }, "stateId") => Some(Value::String(state_id.clone())),
            (TimerEv::After { .. }, _) => None,
            (TimerEv::Ev(e), _) => e.field(key),
        }
    }

    fn to_json(&self) -> Value {
        match self {
            TimerEv::After { delay, state_id } => json!({ "type": AFTER, "delay": delay, "stateId": state_id }),
            TimerEv::Ev(e) => e.to_json(),
        }
    }

    fn into_ev(self) -> Ev<E> {
        match self {
            TimerEv::After { delay, state_id } => {
                let mut data = Map::with_capacity(2);
                data.insert("delay".into(), delay);
                data.insert("stateId".into(), Value::String(state_id));
                Ev::Internal { event_type: Cow::Borrowed(AFTER), data }
            }
            TimerEv::Ev(e) => e,
        }
    }
}

impl<E: EventType> LogicalTimer<E> {
    /// XState's `getTimerKey`.
    fn key(&self) -> Value {
        if let Some(id) = &self.id {
            return json!({ "id": id });
        }
        if let Some(seq) = self.seq {
            return json!({ "seq": seq });
        }
        let ty = self.event.event_type();
        let fields: &[&str] = match ty {
            "xstate.after" => &["delay", "stateId"],
            "xstate.timeout.state" => &["stateId"],
            "xstate.timeout.actor" => &["actorId"],
            _ => return json!({ "event": self.event.to_json() }),
        };
        // `builtinTimerIdentity`, from the fields it keeps.
        let mut identity = Map::with_capacity(fields.len() + 1);
        identity.insert("type".into(), Value::String(ty.to_owned()));
        for f in fields {
            if let Some(v) = self.event.field(f) {
                identity.insert((*f).to_owned(), v);
            }
        }
        json!({ "event": identity })
    }

    /// XState's `matchesTimerKey`.
    fn matches(&self, key: &Value) -> bool {
        if let Some(id) = key.get("id") {
            return self.id.as_deref().map(Value::from).as_ref() == Some(id);
        }
        if self.id.is_some() {
            return false;
        }
        if let Some(seq) = key.get("seq") {
            return self.seq.is_some_and(|s| seq.as_u64() == Some(s));
        }
        if self.seq.is_some() {
            return false;
        }
        match key.get("event").and_then(Value::as_object) {
            Some(pattern) => pattern.iter().all(|(k, v)| match k.as_str() {
                "type" => v.as_str() == Some(self.event.event_type()),
                _ => self.event.field(k).as_ref() == Some(v),
            }),
            None => false,
        }
    }

    fn to_data(&self) -> Value {
        let mut m = Map::new();
        if let Some(id) = &self.id {
            m.insert("id".into(), Value::String(id.clone()));
        }
        if let Some(seq) = self.seq {
            m.insert("seq".into(), json!(seq));
        }
        m.insert("delay".into(), json!(self.delay));
        m.insert("type".into(), json!("@xstate.raise"));
        m.insert("event".into(), self.event.to_json());
        m.insert("target".into(), json!("self"));
        Value::Object(m)
    }
}

/// Recorded history: each history state and its recorded states.
pub(crate) type HistoryValues = Vec<(Nid, Vec<Nid>)>;

/// A machine actor's snapshot.
pub(crate) struct State<C, E> {
    /// The configuration, in XState's order.
    pub nodes: Vec<Nid>,
    pub context: C,
    pub status: Status,
    /// Boxed, like `error`: rarely set, and a `Value` is large inline.
    pub output: Option<Box<Value>>,
    pub error: Option<Box<Value>>,
    /// `historyValue`, in key order. Copy-on-write (`Arc::make_mut`) like
    /// `children` and `timers`, so a macrostep's rollback copy is cheap.
    pub history: Arc<HistoryValues>,
    /// `snapshot.children`, in order.
    pub children: Arc<IndexMap<String, ActorRef>>,
    /// `snapshot.timers`, in order.
    pub timers: Arc<Vec<LogicalTimer<E>>>,
    pub next_timer_id: u64,
    /// For an untyped context: known to need no number normalization
    /// (`js_numbers`), so merges need not check it again. Not persisted.
    pub context_normalized: bool,
}

impl<C: Clone, E: Clone> Clone for State<C, E> {
    fn clone(&self) -> Self {
        Self {
            nodes: self.nodes.clone(),
            context: self.context.clone(),
            status: self.status,
            output: self.output.clone(),
            error: self.error.clone(),
            history: self.history.clone(),
            children: self.children.clone(),
            timers: self.timers.clone(),
            next_timer_id: self.next_timer_id,
            context_normalized: self.context_normalized,
        }
    }
}

/// Shared empty `history`, `children` and `timers` (see [`Machine`]).
pub(crate) struct EmptyParts<E> {
    history: Arc<HistoryValues>,
    children: Arc<IndexMap<String, ActorRef>>,
    timers: Arc<Vec<LogicalTimer<E>>>,
}

impl<E> Default for EmptyParts<E> {
    fn default() -> Self {
        Self { history: Arc::default(), children: Arc::default(), timers: Arc::default() }
    }
}

impl<C, E> State<C, E> {
    pub fn new(context: C, empty: &EmptyParts<E>) -> Self {
        Self {
            nodes: vec![ROOT],
            context,
            status: Status::Active,
            output: None,
            error: None,
            history: empty.history.clone(),
            children: empty.children.clone(),
            timers: empty.timers.clone(),
            next_timer_id: 0,
            context_normalized: false,
        }
    }
}

impl<C, E> AnySnapshot for State<C, E>
where
    C: Send + 'static,
    E: EventType + Send + Sync,
{
    fn status(&self) -> Status {
        self.status
    }

    fn output(&self) -> Option<Value> {
        self.output.as_deref().cloned()
    }

    fn error(&self) -> Option<Value> {
        self.error.as_deref().cloned()
    }

    fn timer_pending(&self, key: &Value) -> bool {
        self.timers.iter().any(|t| t.matches(key))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

/// `Object.is` over JSON values, except that numbers compare by value.
fn js_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64() == y.as_f64(),
        _ => a == b,
    }
}

/// What an action list asks for, before resolution.
enum Op<E> {
    Raise { event: Ev<E>, id: Option<String>, delay: u64 },
    Cancel(Value),
    Log(Vec<Value>),
    Emit(Value),
    Custom(u32),
    Spawn(ActorRef),
    Stop(ActorRef),
}

/// The result of running an action list or a transition function: the
/// context object XState would merge (`res.context`), the queued operations
/// and the raised events.
struct Outcome<E> {
    context: Option<Map<String, Value>>,
    /// Instead of `context`: the merge is this transition's static
    /// `context` patch, read from the machine when merged (not copied).
    static_patch: Option<Tid>,
    ops: Vec<Op<E>>,
    internal: Vec<Ev<E>>,
}

impl<E> Outcome<E> {
    fn empty() -> Self {
        Self { context: None, static_patch: None, ops: Vec::new(), internal: Vec::new() }
    }
}

/// The parts of a snapshot a failed macrostep restores. The context, the
/// history, the children and the timers are saved separately, by the host,
/// just before the macrostep first writes them (`SavedParts`).
struct Saved {
    nodes: core::Buf<Nid>,
    status: Status,
    output: Option<Box<Value>>,
    error: Option<Box<Value>>,
    next_timer_id: u64,
}

impl Saved {
    fn of<C, E>(st: &State<C, E>) -> Self {
        Saved {
            nodes: core::Buf::from_slice(&st.nodes),
            status: st.status,
            output: st.output.clone(),
            error: st.error.clone(),
            next_timer_id: st.next_timer_id,
        }
    }

    fn restore<C, E>(self, st: &mut State<C, E>, parts: SavedParts<E>) {
        st.nodes.clear();
        st.nodes.extend_from_slice(&self.nodes);
        st.status = self.status;
        st.output = self.output;
        st.error = self.error;
        st.next_timer_id = self.next_timer_id;
        if let Some(history) = parts.history {
            st.history = history;
        }
        if let Some(children) = parts.children {
            st.children = children;
        }
        if let Some(timers) = parts.timers {
            st.timers = timers;
        }
    }
}

/// The copy-on-write parts of a snapshot as they were before a macrostep
/// first wrote them (`None`: not written).
struct SavedParts<E> {
    history: Option<Arc<HistoryValues>>,
    children: Option<Arc<IndexMap<String, ActorRef>>>,
    timers: Option<Arc<Vec<LogicalTimer<E>>>>,
}

impl<E> Default for SavedParts<E> {
    fn default() -> Self {
        Self { history: None, children: None, timers: None }
    }
}

/// A context as JSON: a clone for untyped (`Value`) contexts, serde for
/// typed ones (with numbers as JavaScript writes them).
fn context_json<C: Serialize + 'static>(context: &C) -> Value {
    if let Some(v) = (context as &dyn Any).downcast_ref::<Value>() {
        let mut v = v.clone();
        crate::event::js_numbers(&mut v);
        return v;
    }
    let mut v = serde_json::to_value(context).unwrap_or(Value::Null);
    crate::event::js_numbers(&mut v);
    v
}

/// A JSON value that serializes with whole-number floats written as
/// integers, as [`crate::event::js_numbers`] would leave it, without
/// copying it.
struct JsNumbers<'a>(&'a Value);

impl Serialize for JsNumbers<'_> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::{SerializeMap as _, SerializeSeq as _};
        const MAX_SAFE: f64 = 9_007_199_254_740_991.0;
        match self.0 {
            Value::Number(n) if n.is_f64() => {
                let f = n.as_f64().unwrap_or(f64::NAN);
                if f.fract() == 0.0 && f.abs() <= MAX_SAFE {
                    serializer.serialize_i64(f as i64)
                } else {
                    serializer.serialize_f64(f)
                }
            }
            Value::Array(items) => {
                let mut seq = serializer.serialize_seq(Some(items.len()))?;
                for item in items {
                    seq.serialize_element(&JsNumbers(item))?;
                }
                seq.end()
            }
            Value::Object(map) => {
                let mut out = serializer.serialize_map(Some(map.len()))?;
                for (k, v) in map {
                    out.serialize_entry(k, &JsNumbers(v))?;
                }
                out.end()
            }
            other => other.serialize(serializer),
        }
    }
}

/// An event that serializes as [`Ev::to_json`] builds it.
struct EvView<'a, E>(&'a Ev<E>);

impl<E: EventType> Serialize for EvView<'_, E> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap as _;
        match self.0 {
            Ev::Internal { event_type, data } if !data.contains_key("type") => {
                let mut map = s.serialize_map(Some(data.len() + 1))?;
                map.serialize_entry("type", &**event_type)?;
                for (k, v) in data {
                    map.serialize_entry(k, v)?;
                }
                map.end()
            }
            other => other.to_json().serialize(s),
        }
    }
}

/// `{ type: "xstate.after", delay, stateId }`.
struct AfterView<'a>(&'a Value, &'a str);

impl Serialize for AfterView<'_> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap as _;
        let mut map = s.serialize_map(Some(3))?;
        map.serialize_entry("type", AFTER)?;
        map.serialize_entry("delay", self.0)?;
        map.serialize_entry("stateId", self.1)?;
        map.end()
    }
}

/// A logical timer that serializes as [`LogicalTimer::to_data`] builds it.
struct TimerData<'a, E>(&'a LogicalTimer<E>);

impl<E: EventType> Serialize for TimerData<'_, E> {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap as _;
        let t = self.0;
        let mut map = s.serialize_map(Some(4 + usize::from(t.id.is_some()) + usize::from(t.seq.is_some())))?;
        if let Some(id) = &t.id {
            map.serialize_entry("id", id)?;
        }
        if let Some(seq) = t.seq {
            map.serialize_entry("seq", &seq)?;
        }
        map.serialize_entry("delay", &t.delay)?;
        map.serialize_entry("type", "@xstate.raise")?;
        match &t.event {
            TimerEv::After { delay, state_id } => map.serialize_entry("event", &AfterView(delay, state_id))?,
            TimerEv::Ev(e) => map.serialize_entry("event", &EvView(e))?,
        }
        map.serialize_entry("target", "self")?;
        map.end()
    }
}

/// A machine snapshot that serializes as [`Machine::snapshot_data`] builds it.
struct SnapshotView<'a, C, E> {
    m: &'a Machine<C, E>,
    st: &'a State<C, E>,
    /// Leave out what restore rebuilds identically (`System::compact_view`).
    compact: bool,
}

impl<C, E> Serialize for SnapshotView<'_, C, E>
where
    C: Serialize + DeserializeOwned + Clone + 'static,
    E: EventType,
{
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        struct Ids<'a, C, E>(&'a Machine<C, E>, &'a [Nid]);
        impl<C, E> Serialize for Ids<'_, C, E> {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.collect_seq(self.1.iter().map(|&n| self.0.data(n).id.as_str()))
            }
        }
        struct StateNodes<'a, C, E>(&'a Machine<C, E>, &'a [Nid]);
        impl<C, E> Serialize for StateNodes<'_, C, E> {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.collect_seq(self.1.iter().map(|&n| {
                    let mut one = std::collections::BTreeMap::new();
                    one.insert("$stateNode", self.0.data(n).id.as_str());
                    one
                }))
            }
        }
        struct History<'a, C, E>(&'a Machine<C, E>, &'a [(Nid, Vec<Nid>)]);
        impl<C, E> Serialize for History<'_, C, E> {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.collect_map(self.1.iter().map(|(h, nodes)| (self.0.data(*h).id.as_str(), StateNodes(self.0, nodes))))
            }
        }
        struct Children<'a>(&'a IndexMap<String, ActorRef>);
        impl Serialize for Children<'_> {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                struct RefView<'a>(&'a ActorRef);
                impl Serialize for RefView<'_> {
                    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                        use serde::ser::SerializeMap as _;
                        let mut m = s.serialize_map(Some(2))?;
                        m.serialize_entry("$actor", &*self.0.address)?;
                        m.serialize_entry("incarnation", &self.0.incarnation.to_string())?;
                        m.end()
                    }
                }
                s.collect_map(self.0.iter().map(|(k, r)| (k, RefView(r))))
            }
        }
        let (m, st, compact) = (self.m, self.st, self.compact);
        // Compact leaves out what restore rebuilds: the value (from
        // `nodeIds`), empty children, timers and history, the always-empty
        // `_stateInputs` and a zero `_nextTimerId`.
        let keep = |non_empty: bool| !compact || non_empty;
        let (keep_children, keep_timers, keep_history, keep_next) = (
            keep(!st.children.is_empty()),
            keep(!st.timers.is_empty()),
            keep(!st.history.is_empty()),
            keep(st.next_timer_id != 0),
        );
        // Exact lengths let encoders such as MessagePack write maps directly
        // instead of buffering them.
        let len = 3
            + usize::from(st.output.is_some())
            + usize::from(st.error.is_some())
            + if compact { 0 } else { 2 }
            + [keep_children, keep_timers, keep_history, keep_next].iter().filter(|k| **k).count();
        let mut map = s.serialize_map(Some(len))?;
        map.serialize_entry("status", st.status.as_str())?;
        if let Some(o) = &st.output {
            map.serialize_entry("output", o)?;
        }
        if let Some(e) = &st.error {
            map.serialize_entry("error", e)?;
        }
        match (&st.context as &dyn Any).downcast_ref::<Value>() {
            Some(v) => map.serialize_entry("context", &JsNumbers(v))?,
            None => map.serialize_entry("context", &context_json(&st.context))?,
        }
        if !compact {
            map.serialize_entry("value", &m.state_value(&st.nodes))?;
        }
        if keep_children {
            map.serialize_entry("children", &Children(&st.children))?;
        }
        struct Timers<'a, E>(&'a [LogicalTimer<E>]);
        impl<E: EventType> Serialize for Timers<'_, E> {
            fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                s.collect_seq(self.0.iter().map(TimerData))
            }
        }
        if keep_timers {
            map.serialize_entry("timers", &Timers(&st.timers))?;
        }
        if keep_history {
            map.serialize_entry("historyValue", &History(m, &st.history))?;
        }
        if !compact {
            map.serialize_entry("_stateInputs", &Map::new())?;
        }
        if keep_next {
            map.serialize_entry("_nextTimerId", &st.next_timer_id)?;
        }
        map.serialize_entry("nodeIds", &Ids(m, &st.nodes))?;
        map.end()
    }
}

/// Which macrostep is being run.
pub(crate) enum MacroEvent {
    Init,
    Event(Value),
}

struct NativeHost<'a, C, E> {
    m: &'a Machine<C, E>,
    st: &'a mut State<C, E>,
    cx: &'a mut dyn Context,
    event: Ev<E>,
    queue: VecDeque<Ev<E>>,
    effects: Vec<Effect>,
    /// The snapshot object's identity: bumped whenever XState would clone it.
    token: u64,
    /// Whether the current microstep has cloned the snapshot.
    cloned: bool,
    /// Transition results computed this microstep (`getCurrentTransitionResult`).
    outcomes: Vec<(Tid, Outcome<E>)>,
    /// The last configurations, for the infinite-loop error.
    recent: VecDeque<Vec<Nid>>,
    /// Children stopped in this macrostep (XState's `stoppedIds`).
    stopped_ids: Vec<String>,
    /// The identity of the snapshot the last microstep returned.
    microstep_token_value: u64,
    /// The context before this macrostep's first write, to roll back to
    /// when the macrostep fails.
    context_backup: Option<C>,
    /// For an untyped context patched in place: whether the undo log, not
    /// `context_backup`, records this macrostep's writes, and the log (each
    /// written key and the value it replaced, `None` for a key it added).
    context_undo_active: bool,
    context_undo: Vec<(String, Option<Value>)>,
    /// Whether a failure rolls the macrostep back (not for the initial one),
    /// and the parts saved for it.
    rollback: bool,
    saved_parts: SavedParts<E>,
}

impl<C, E> NativeHost<'_, C, E>
where
    C: Serialize + DeserializeOwned + Clone + 'static,
    E: EventType,
{
    fn context_object(&self) -> Result<Map<String, Value>, SystemError> {
        if let Some(v) = (&self.st.context as &dyn Any).downcast_ref::<Value>() {
            let mut map = v.as_object().cloned().unwrap_or_default();
            map.values_mut().for_each(crate::event::js_numbers);
            return Ok(map);
        }
        match serde_json::to_value(&self.st.context) {
            Ok(Value::Object(mut map)) => {
                map.values_mut().for_each(crate::event::js_numbers);
                Ok(map)
            }
            Ok(_) => Ok(Map::new()),
            Err(e) => Err(SystemError::Failed(format!("invalid context: {e}"))),
        }
    }

    /// The timers, to write (saving them first for a rollback).
    fn timers_mut(&mut self) -> &mut Vec<LogicalTimer<E>> {
        if self.rollback && self.saved_parts.timers.is_none() {
            self.saved_parts.timers = Some(self.st.timers.clone());
        }
        Arc::make_mut(&mut self.st.timers)
    }

    /// The children, to write (saving them first for a rollback).
    fn children_mut(&mut self) -> &mut IndexMap<String, ActorRef> {
        if self.rollback && self.saved_parts.children.is_none() {
            self.saved_parts.children = Some(self.st.children.clone());
        }
        Arc::make_mut(&mut self.st.children)
    }

    /// The history, to write (saving it first for a rollback).
    fn history_mut(&mut self) -> &mut HistoryValues {
        if self.rollback && self.saved_parts.history.is_none() {
            self.saved_parts.history = Some(self.st.history.clone());
        }
        Arc::make_mut(&mut self.st.history)
    }

    /// `mergeContextPatch`.
    fn merge_context(&mut self, patch: &Map<String, Value>) -> Result<(), SystemError> {
        let normalized = self.st.context_normalized;
        if let Some(v) = (&mut self.st.context as &mut dyn Any).downcast_mut::<Value>() {
            // An untyped context is patched in place. To roll back a failed
            // macrostep, an undo log keeps each overwritten value (no copy
            // of the context) while the context is an object whose numbers
            // are already normalized; otherwise the whole context is copied
            // on the macrostep's first write.
            let first_write = self.context_backup.is_none() && !self.context_undo_active;
            if first_write {
                if v.as_object().is_some_and(|m| normalized || !m.values().any(crate::event::needs_js_numbers)) {
                    self.context_undo_active = true;
                } else {
                    let copy = v.clone();
                    self.context_backup = Some(*(Box::new(copy) as Box<dyn Any>).downcast::<C>().expect("C is Value"));
                }
            }
            if !v.is_object() {
                *v = Value::Object(Map::new());
            }
            let map = v.as_object_mut().expect("an object");
            if !self.context_undo_active {
                map.values_mut().for_each(crate::event::js_numbers);
            }
            for (k, val) in patch {
                let mut val = val.clone();
                crate::event::js_numbers(&mut val);
                let old = match map.get_mut(k) {
                    Some(slot) => Some(std::mem::replace(slot, val)),
                    None => map.insert(k.clone(), val),
                };
                if self.context_undo_active {
                    self.context_undo.push((k.clone(), old));
                }
            }
            self.st.context_normalized = true;
            self.cloned = true;
            return Ok(());
        }
        let mut map = self.context_object()?;
        for (k, v) in patch {
            map.insert(k.clone(), v.clone());
        }
        let merged = Value::Object(map);
        let context = if std::any::TypeId::of::<C>() == std::any::TypeId::of::<Value>() {
            // An untyped context takes the object as it is.
            *(Box::new(merged) as Box<dyn Any>).downcast::<C>().expect("C is Value")
        } else {
            C::deserialize(&merged).map_err(|e| SystemError::Failed(format!("invalid context: {e}")))?
        };
        let previous = std::mem::replace(&mut self.st.context, context);
        self.context_backup.get_or_insert(previous);
        self.cloned = true;
        Ok(())
    }

    /// The context an action list starts from, which only `@xstate.assign`
    /// reads (serializing the context is skipped otherwise).
    fn action_base(&self, actions: &[Action]) -> Result<Map<String, Value>, SystemError> {
        if actions.iter().any(|a| matches!(a, Action::Assign(_))) {
            self.context_object()
        } else {
            Ok(Map::new())
        }
    }

    /// `executeActions` from `machineFromJSON`, against `base` as the context.
    fn execute_actions(&self, actions: &[Action], base: &Map<String, Value>) -> Outcome<E> {
        let mut assigned: Option<Map<String, Value>> = None;
        let mut out = Outcome::empty();
        for action in actions {
            match action {
                Action::Raise { event, id, delay } => {
                    let event = self.m.raise_events[*event as usize].clone();
                    match delay {
                        None => out.internal.push(event),
                        Some(delay) => out.ops.push(Op::Raise { event, id: id.clone(), delay: *delay }),
                    }
                }
                Action::Cancel(id) => out.ops.push(Op::Cancel(json!({ "id": id }))),
                Action::Log(args) => out.ops.push(Op::Log(args.clone())),
                Action::Emit(event) => out.ops.push(Op::Emit(event.clone())),
                Action::Assign(patch) => {
                    let a = assigned.get_or_insert_with(Map::new);
                    for (k, v) in patch {
                        a.insert(k.clone(), v.clone());
                    }
                }
                Action::Custom(i) => out.ops.push(Op::Custom(*i)),
            }
        }
        out.context = assigned.map(|patch| {
            let mut full = base.clone();
            for (k, v) in patch {
                full.insert(k, v);
            }
            full
        });
        out
    }

    /// The transition function `machineFromJSON` builds for a transition
    /// with `context` or `actions` (wrapped, for invoke completions with a
    /// timeout, by a function that then cancels the timeout).
    fn transition_outcome(&self, t: Tid) -> Result<Outcome<E>, SystemError> {
        let tr = self.m.transition(t);
        if !tr.is_dynamic() {
            return Ok(Outcome::empty());
        }
        let mut out = if tr.context.is_some() || !tr.actions.is_empty() {
            let mut base = self.action_base(&tr.actions)?;
            // Only `@xstate.assign` reads the base (`action_base` is empty
            // otherwise), so only then does it need the patch.
            if let Some(patch) =
                tr.context.as_ref().filter(|_| tr.actions.iter().any(|a| matches!(a, Action::Assign(_))))
            {
                for (k, v) in patch {
                    base.insert(k.clone(), v.clone());
                }
            }
            let mut out =
                if tr.actions.is_empty() { Outcome::empty() } else { self.execute_actions(&tr.actions, &base) };
            if out.context.is_none() && tr.context.is_some() {
                out.static_patch = Some(t);
            }
            out
        } else {
            Outcome::empty()
        };
        if let Some(key) = &tr.cancel_timeout {
            out.ops.push(Op::Cancel(key.clone()));
        }
        Ok(out)
    }

    /// `resolveActionsWithContext`: logical timers update the snapshot now;
    /// everything becomes an effect.
    fn resolve_ops(&mut self, ops: Vec<Op<E>>) {
        for op in ops {
            match op {
                Op::Raise { event, id, delay } => {
                    let seq = if id.is_none() && builtin_timer_identity(&event.to_json()).is_none() {
                        let seq = self.st.next_timer_id;
                        self.st.next_timer_id += 1;
                        Some(seq)
                    } else {
                        None
                    };
                    let timer = LogicalTimer { id, seq, delay, event: TimerEv::new(event) };
                    let key = timer.key();
                    let timers = self.timers_mut();
                    match timers.iter_mut().find(|t| t.matches(&key)) {
                        Some(existing) => *existing = timer,
                        None => {
                            // Snapshots hold few timers: no spare capacity.
                            timers.reserve_exact(1);
                            timers.push(timer);
                        }
                    }
                    self.cloned = true;
                    self.effects.push(Effect::DelayedRaise { key, delay });
                }
                Op::Cancel(key) => {
                    if self.st.timers.iter().any(|t| t.matches(&key)) {
                        self.timers_mut().retain(|t| !t.matches(&key));
                        self.cloned = true;
                    }
                    self.effects.push(Effect::Cancel(key));
                }
                Op::Log(args) => self.effects.push(Effect::External(ExternalSpec {
                    kind: EffectKind::Action,
                    ty: EffectType::Fixed("@xstate.log".into()),
                    params: Some(json!({})),
                    args,
                    event: None,
                    reason: None,
                    exec: None,
                })),
                Op::Emit(event) => {
                    let ty = event.get("type").and_then(Value::as_str).unwrap_or("").to_owned();
                    self.effects.push(Effect::External(ExternalSpec {
                        kind: EffectKind::Emit,
                        ty: EffectType::Fixed(ty),
                        // The system derives the params (the event without
                        // `type`) when it keeps the effect.
                        params: None,
                        args: Vec::new(),
                        event: Some(event),
                        reason: None,
                        exec: None,
                    }));
                }
                Op::Custom(i) => {
                    let action = &self.m.actions[i as usize];
                    self.effects.push(Effect::External(ExternalSpec {
                        kind: EffectKind::Action,
                        ty: EffectType::Action(action.name.clone()),
                        params: Some(json!({})),
                        args: vec![action.params.clone().unwrap_or(Value::Null)],
                        event: None,
                        reason: None,
                        exec: Some(action.exec.clone()),
                    }));
                }
                Op::Spawn(r) => self.effects.push(Effect::Spawn(r)),
                Op::Stop(r) => {
                    // XState's `applyChildUpdate`: the child leaves `children`
                    // and the registry at once, and counts as stopping.
                    if self.st.children.values().any(|c| *c == r) {
                        self.children_mut().retain(|_, c| *c != r);
                    }
                    self.cx.unregister(&r);
                    self.cloned = true;
                    self.effects.push(Effect::Stop(r));
                }
            }
        }
    }

    /// A state's exit function: its timers are cancelled (the wrappers
    /// nest, so the last first), then the exit actions run.
    fn exit_outcome(&self, node: Nid) -> Result<Outcome<E>, SystemError> {
        let data = self.m.data(node);
        let base = self.action_base(&data.exit)?;
        let mut out = self.execute_actions(&data.exit, &base);
        let mut ops: Vec<Op<E>> =
            data.delayed.iter().rev().map(|d| Op::Cancel(json!({ "event": self.delayed_identity(node, d) }))).collect();
        ops.append(&mut out.ops);
        out.ops = ops;
        Ok(out)
    }

    fn delayed_identity(&self, node: Nid, d: &DelayedDef) -> Value {
        let id = &self.m.data(node).id;
        match &d.kind {
            DelayedKind::After { event_delay } => json!({ "type": AFTER, "delay": event_delay, "stateId": id }),
            DelayedKind::StateTimeout => json!({ "type": "xstate.timeout.state", "stateId": id }),
            DelayedKind::InvokeTimeout { invoke_id } => json!({ "type": "xstate.timeout.actor", "actorId": invoke_id }),
        }
    }

    fn delayed_event(&self, node: Nid, d: &DelayedDef) -> Ev<E> {
        let id = Value::String(self.m.data(node).id.clone());
        let mut data = Map::new();
        let ty: &'static str = match &d.kind {
            DelayedKind::After { event_delay } => {
                data.insert("delay".into(), event_delay.clone());
                data.insert("stateId".into(), id);
                AFTER
            }
            DelayedKind::StateTimeout => {
                data.insert("stateId".into(), id);
                "xstate.timeout.state"
            }
            DelayedKind::InvokeTimeout { invoke_id } => {
                data.insert("actorId".into(), Value::String(invoke_id.clone()));
                if let Some(child) = self.st.children.get(invoke_id) {
                    data.insert("sessionId".into(), Value::String(child.incarnation.to_string()));
                }
                "xstate.timeout.actor"
            }
        };
        Ev::Internal { event_type: Cow::Borrowed(ty), data }
    }

    fn resolve_delay(&self, spec: DelaySpec) -> u64 {
        match spec {
            DelaySpec::Fixed(ms) => ms,
            DelaySpec::Named(i) => (self.m.delays[i as usize])(&self.st.context, &self.event),
        }
    }

    fn done_event(&self, node: Nid, output: Option<Value>) -> Ev<E> {
        let mut data = Map::new();
        data.insert("stateId".into(), Value::String(self.m.data(node).id.clone()));
        if let Some(output) = output {
            data.insert("output".into(), output);
        }
        Ev::Internal { event_type: Cow::Borrowed(DONE_STATE), data }
    }

    fn queued_done_output(&self, node: Nid) -> Option<Value> {
        let id = &self.m.data(node).id;
        self.queue.iter().find_map(|e| match e {
            Ev::Internal { event_type, data }
                if event_type == DONE_STATE && data.get("stateId").and_then(Value::as_str) == Some(id) =>
            {
                data.get("output").cloned()
            }
            _ => None,
        })
    }

    fn eval_guard(&self, guard: &Guard) -> bool {
        match guard {
            Guard::Custom(i) => {
                let args = GuardArgs {
                    context: &self.st.context,
                    event: &self.event,
                    machine: self.m,
                    config: &self.st.nodes,
                };
                (self.m.guards[*i as usize])(&args)
            }
            Guard::Not(inner) => !self.eval_guard(inner),
            Guard::StateIn(node) => self.st.nodes.contains(node),
            Guard::RouteTo(to) => self.event.field("to").as_ref().and_then(Value::as_str) == Some(to.as_str()),
            Guard::And(a, b) => self.eval_guard(a) && self.eval_guard(b),
        }
    }

    /// XState's `matchesActorSession`.
    fn matches_session(&self, actor_id: &str) -> bool {
        let Some(child) = self.st.children.get(actor_id) else { return true };
        match self.event.field("sessionId") {
            None => true,
            Some(session) => session.as_str() == Some(child.incarnation.to_string().as_str()),
        }
    }

    /// XState's `removeTerminatedChild`.
    fn remove_terminated_child(&mut self) {
        let ty = self.event.event_type();
        if ty != "xstate.done.actor" && ty != "xstate.error.actor" {
            return;
        }
        let Some(actor_id) = self.event.field("actorId").and_then(|v| v.as_str().map(str::to_owned)) else { return };
        let Some(child) = self.st.children.get(&actor_id).cloned() else { return };
        if self.event.field("sessionId").as_ref().and_then(Value::as_str)
            != Some(child.incarnation.to_string().as_str())
        {
            return;
        }
        self.children_mut().shift_remove(&actor_id);
        self.cx.unregister(&child);
        self.token += 1;
    }

    fn stop_children_ops(&self) -> Vec<Op<E>> {
        self.st.children.values().map(|c| Op::Stop(c.clone())).collect()
    }

    fn cancel_timer_ops(&self) -> Vec<Op<E>> {
        self.st.timers.iter().map(|t| Op::Cancel(t.key())).collect()
    }
}

impl<C, E> Host for NativeHost<'_, C, E>
where
    C: Serialize + DeserializeOwned + Clone + 'static,
    E: EventType,
{
    type Error = SystemError;

    fn config(&self, out: &mut Vec<Nid>) {
        out.extend_from_slice(&self.st.nodes);
    }

    fn history(&self, history: Nid) -> Option<Vec<Nid>> {
        self.st.history.iter().find(|(h, _)| *h == history).map(|(_, v)| v.clone())
    }

    fn static_initial(&self, node: Nid) -> Arc<[Nid]> {
        self.m.data(node).initial.clone()
    }

    fn begin_selection(&mut self) -> Result<(), SystemError> {
        Ok(())
    }

    fn eval_candidate(&mut self, t: Tid, _node: Nid, _eventless: bool) -> Result<bool, SystemError> {
        let tr = self.m.transition(t);
        for (key, expected) in &tr.matches {
            match self.event.field(key) {
                Some(actual) if js_eq(&actual, expected) => {}
                _ => return Ok(false),
            }
        }
        if let Some(actor_id) = &tr.session {
            if !self.matches_session(actor_id) {
                return Ok(false);
            }
        }
        Ok(match &tr.guard {
            Some(g) => self.eval_guard(g),
            None => true,
        })
    }

    fn resolve(&mut self, resolver: Resolver, t: TRef) -> Result<Resolved, SystemError> {
        match t {
            TRef::T(t) => {
                let tr = self.m.transition(t);
                if resolver == Resolver::Current && tr.is_dynamic() && !self.outcomes.iter().any(|(x, _)| *x == t) {
                    let outcome = self.transition_outcome(t)?;
                    self.outcomes.push((t, outcome));
                }
                Ok(Resolved { targets: tr.targets.clone(), reenter: tr.reenter })
            }
            TRef::Initial(n) => Ok(Resolved { targets: Some(self.m.data(n).initial.clone()), reenter: false }),
            TRef::HistoryDefault(h) => {
                Ok(Resolved { targets: Some(self.m.data(h).history_target.clone()), reenter: false })
            }
            TRef::InitialMicrostep => unreachable!("resolved by the core"),
        }
    }

    fn begin_microstep(&mut self) -> Result<(), SystemError> {
        self.outcomes.clear();
        self.cloned = false;
        Ok(())
    }

    fn exit_state(&mut self, node: Nid) -> Result<(), SystemError> {
        let out = self.exit_outcome(node)?;
        self.queue.extend(out.internal);
        if let Some(ctx) = &out.context {
            self.merge_context(ctx)?;
        }
        self.resolve_ops(out.ops);
        let stops: Vec<Op<E>> = self
            .m
            .data(node)
            .invokes
            .iter()
            .filter_map(|inv| self.st.children.get(&inv.id).cloned())
            .map(Op::Stop)
            .collect();
        for op in &stops {
            if let Op::Stop(c) = op {
                if let Some((id, _)) = self.st.children.iter().find(|(_, r)| *r == c) {
                    self.stopped_ids.push(id.clone());
                }
            }
        }
        self.resolve_ops(stops);
        Ok(())
    }

    fn transition_content(&mut self, filtered: &[TRef]) -> Result<(), SystemError> {
        let mut ops = Vec::new();
        let mut internal = Vec::new();
        for &t in filtered {
            let TRef::T(t) = t else { continue };
            let outcome = match self.outcomes.iter().position(|(x, _)| *x == t) {
                Some(i) => self.outcomes.remove(i).1,
                None => self.transition_outcome(t)?,
            };
            if let Some(ctx) = &outcome.context {
                self.merge_context(ctx)?;
            } else if let Some(t) = outcome.static_patch {
                let m = self.m;
                self.merge_context(m.transition(t).context.as_ref().expect("a static patch"))?;
            }
            ops.extend(outcome.ops);
            internal.extend(outcome.internal);
        }
        self.queue.extend(internal);
        self.resolve_ops(ops);
        Ok(())
    }

    fn begin_entry(&mut self, _active: &[Nid], _entered: &[(TRef, core::Buf<Nid>)]) -> Result<(), SystemError> {
        Ok(())
    }

    fn enter_state(&mut self, node: Nid, _default_entry: bool, _history_defaults: &[Nid]) -> Result<(), SystemError> {
        let data = self.m.data(node);
        let mut ops: Vec<Op<E>> = Vec::new();
        // Final states are inert: their invocations never start.
        if self.m.tree.kind(node) != core::Kind::Final {
            for inv in &data.invokes {
                if let Some(existing) = self.st.children.get(&inv.id) {
                    let occupied = !self.stopped_ids.contains(&inv.id) && self.cx.status_of(existing) == Status::Active;
                    if occupied {
                        return Err(SystemError::Failed(format!(
                            "Cannot create a child actor with id '{}': a child with that id already exists.",
                            inv.id
                        )));
                    }
                }
                let child = self.cx.allocate(
                    &inv.logic,
                    AllocateOptions {
                        id: inv.id.clone(),
                        input: inv.input.clone(),
                        parent: Some(self.cx.self_ref()),
                        registry_key: inv.registry_key.clone(),
                        sync_snapshot: Some(inv.sync_snapshot),
                    },
                )?;
                ops.push(Op::Spawn(child.clone()));
                self.children_mut().insert(inv.id.clone(), child);
                self.cloned = true;
            }
        }
        let base = self.action_base(&data.entry)?;
        let mut out = self.execute_actions(&data.entry, &base);
        // Delayed transitions' timers are scheduled by entry wrappers, after
        // the entry actions and against the context they produced.
        if let Some(ctx) = &out.context {
            self.merge_context(ctx)?;
        }
        for d in &data.delayed {
            let delay = self.resolve_delay(d.delay);
            out.ops.push(Op::Raise { event: self.delayed_event(node, d), id: None, delay });
        }
        self.queue.extend(out.internal);
        ops.append(&mut out.ops);
        self.resolve_ops(ops);
        // The `initial` and history default transitions of JSON machines
        // carry no context or actions.
        Ok(())
    }

    fn done_compound(&mut self, parent: Nid, final_child: Nid) -> Result<(), SystemError> {
        let event = self.done_event(parent, self.m.data(final_child).output.clone());
        self.queue.push_back(event);
        Ok(())
    }

    fn done_parallel(&mut self, parallel: Nid, regions: &[RegionOutput]) -> Result<(), SystemError> {
        let mut output = Map::new();
        for region in regions {
            let (node, value) = match *region {
                RegionOutput::Final(r) => (r, self.m.data(r).output.clone()),
                RegionOutput::Parallel(r) => (r, self.queued_done_output(r)),
                RegionOutput::Compound(r, child) => (r, child.and_then(|c| self.m.data(c).output.clone())),
            };
            if let Some(value) = value {
                output.insert(self.m.data(node).key.clone(), value);
            }
        }
        let event = self.done_event(parallel, Some(Value::Object(output)));
        self.queue.push_back(event);
        Ok(())
    }

    fn machine_done(&mut self, completion: Nid) -> Result<(), SystemError> {
        let tree = &self.m.tree;
        let completion_output = if self.m.data(completion).output.is_some() && tree.node(completion).parent.is_some() {
            let root_done =
                if tree.node(completion).parent == Some(ROOT) { self.queued_done_output(ROOT) } else { None };
            root_done.or_else(|| self.m.data(completion).output.clone())
        } else if tree.kind(completion) == core::Kind::Parallel {
            self.queued_done_output(completion)
        } else {
            None
        };
        self.st.status = Status::Done;
        self.st.output = match &self.m.output {
            Some(output) => Some(Box::new(output.clone())),
            None if tree.node(completion).parent == Some(ROOT) => completion_output.map(Box::new),
            None => None,
        };
        self.cloned = true;
        Ok(())
    }

    fn end_entry(&mut self) -> Result<(), SystemError> {
        Ok(())
    }

    fn is_done(&self) -> bool {
        self.st.status == Status::Done
    }

    fn exit_on_done(&mut self, nodes_desc: &[Nid]) -> Result<(), SystemError> {
        let mut ops = Vec::new();
        for &node in nodes_desc {
            let out = self.exit_outcome(node)?;
            if let Some(ctx) = &out.context {
                self.merge_context(ctx)?;
            }
            ops.extend(out.ops);
            self.queue.extend(out.internal);
        }
        self.resolve_ops(ops);
        let stops = self.stop_children_ops();
        self.resolve_ops(stops);
        let cancels = self.cancel_timer_ops();
        self.resolve_ops(cancels);
        Ok(())
    }

    fn apply_microstep(&mut self, result: Option<MicrostepResult>, _enabled: &[TRef]) -> Result<(), SystemError> {
        if let Some(r) = result {
            if r.structure_changed {
                self.st.nodes.clear();
                self.st.nodes.extend_from_slice(&r.nodes);
                for (h, nodes) in r.history_updates {
                    let history = self.history_mut();
                    match history.iter_mut().find(|(x, _)| *x == h) {
                        Some(entry) => entry.1 = nodes,
                        None => history.push((h, nodes)),
                    }
                }
                self.cloned = true;
            }
        }
        if self.cloned {
            self.token += 1;
        }
        let token = self.token;
        self.remove_terminated_child();
        // The identity the loop compares is the microstep's own result.
        self.microstep_token_value = token;
        Ok(())
    }

    fn is_active(&self) -> bool {
        self.st.status == Status::Active
    }

    fn check_iteration(&mut self, iteration: usize) -> Result<(), SystemError> {
        if iteration > self.m.options.max_iterations {
            return Err(SystemError::InfiniteLoop {
                event_type: self.event.event_type().to_owned(),
                max_iterations: self.m.options.max_iterations,
                states: self.recent.iter().map(|c| self.m.state_value(c)).collect(),
            });
        }
        if self.recent.len() == 5 {
            let mut oldest = self.recent.pop_front().unwrap_or_default();
            oldest.clone_from(&self.st.nodes);
            self.recent.push_back(oldest);
        } else {
            self.recent.push_back(self.st.nodes.clone());
        }
        Ok(())
    }

    fn snapshot_token(&self) -> u64 {
        self.token
    }

    fn microstep_token(&self) -> u64 {
        self.microstep_token_value
    }

    fn drain_raised(&mut self) -> Result<(), SystemError> {
        Ok(())
    }

    fn queue_is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    fn dequeue(&mut self) -> Result<Option<String>, SystemError> {
        Ok(self.queue.pop_front().map(|e| {
            let ty = e.event_type().to_owned();
            self.event = e;
            ty
        }))
    }
}

impl<C, E> Machine<C, E>
where
    C: Serialize + DeserializeOwned + Clone + 'static,
    E: EventType,
{
    /// One macrostep of XState's machine logic (`initialTransition` or
    /// `transition`): the effects it produces, in order, including deferred
    /// child starts and the termination effect.
    pub(crate) fn run_macrostep(
        &self,
        st: &mut State<C, E>,
        cx: &mut dyn Context,
        event: MacroEvent,
    ) -> Result<Vec<Effect>, SystemError> {
        let initial = matches!(event, MacroEvent::Init);
        let previous_status = st.status;
        // A macrostep that throws leaves the previous snapshot in the error
        // status, as XState's actor does (`_setErrorSnapshot`); the failed
        // step's state and effects are discarded. The context is saved on
        // its first write.
        let saved = (!initial).then(|| Saved::of(st));
        let mut h = NativeHost {
            m: self,
            st,
            cx,
            event: Ev::Init,
            queue: VecDeque::new(),
            effects: Vec::new(),
            token: 0,
            cloned: false,
            outcomes: Vec::new(),
            recent: VecDeque::new(),
            stopped_ids: Vec::new(),
            microstep_token_value: 0,
            context_backup: None,
            context_undo_active: false,
            context_undo: Vec::new(),
            rollback: !initial,
            saved_parts: SavedParts::default(),
        };
        if let Err(e) = self.macrostep_body(&mut h, event) {
            let context = h.context_backup.take();
            let undo = std::mem::take(&mut h.context_undo);
            let parts = std::mem::take(&mut h.saved_parts);
            if let Some(saved) = saved {
                saved.restore(st, parts);
                st.context_normalized = false;
                if let Some(context) = context {
                    st.context = context;
                }
                if let Some(Value::Object(map)) = (&mut st.context as &mut dyn Any).downcast_mut::<Value>() {
                    // In reverse: a replaced value goes back to its place,
                    // and an added key (the last one at that point) leaves.
                    for (key, old) in undo.into_iter().rev() {
                        match old {
                            Some(old) => {
                                map.insert(key, old);
                            }
                            None => {
                                map.shift_remove(&key);
                            }
                        }
                    }
                }
            }
            st.status = Status::Error;
            st.error = Some(Box::new(Value::String(e.to_string())));
            return Err(e);
        }
        let mut effects = std::mem::take(&mut h.effects);
        let status = h.st.status;
        // `completeMacrostep`: children start after the snapshot is committed.
        let starts: Vec<Effect> = effects
            .iter()
            .filter_map(|e| match e {
                Effect::Spawn(r) => Some(Effect::Start(r.clone())),
                _ => None,
            })
            .collect();
        effects.extend(starts);
        if (previous_status == Status::Active || initial) && matches!(status, Status::Done | Status::Error) {
            effects.push(Effect::Terminate);
        }
        Ok(effects)
    }

    fn macrostep_body(&self, h: &mut NativeHost<'_, C, E>, event: MacroEvent) -> Result<(), SystemError> {
        let tree = &self.tree;
        let event = match event {
            MacroEvent::Init => {
                h.event = Ev::Init;
                core::initial_microstep(tree, h)?;
                return core::run_to_completion(tree, h);
            }
            MacroEvent::Event(event) => event,
        };
        if h.st.status != Status::Active {
            return Ok(());
        }
        let ty = event.get("type").and_then(Value::as_str).unwrap_or("").to_owned();
        match ty.as_str() {
            "xstate.stop" => {
                let stops = h.stop_children_ops();
                h.resolve_ops(stops);
                let cancels = h.cancel_timer_ops();
                h.resolve_ops(cancels);
                h.st.status = Status::Stopped;
                return Ok(());
            }
            "xstate.timer" => {
                let key = crate::system::read_timer_key(&event);
                let index = key.as_ref().and_then(|k| h.st.timers.iter().position(|t| t.matches(k)));
                let Some(index) = index else { return Ok(()) };
                let timer = h.timers_mut().remove(index);
                h.token += 1;
                h.queue.push_back(timer.event.into_ev());
                h.event = Ev::Internal { event_type: Cow::Borrowed("xstate.timer"), data: Map::new() };
            }
            _ => {
                h.event = Ev::from_json(event)
                    .ok_or_else(|| SystemError::Rejected("an event must have a string \"type\"".into()))?;
                let transitions = core::select_transitions(tree, h, &ty)?;
                if ty.starts_with("xstate.error.") && transitions.is_empty() {
                    if ty == "xstate.error.actor" {
                        let actor_id = h.event.field("actorId").and_then(|v| v.as_str().map(str::to_owned));
                        if let Some(actor_id) = actor_id {
                            if !h.matches_session(&actor_id) {
                                return Ok(());
                            }
                        }
                    }
                    h.st.status = Status::Error;
                    h.st.error = h.event.field("error").map(Box::new);
                    h.remove_terminated_child();
                    return Ok(());
                }
                let enabled: core::Buf<TRef> = transitions.iter().map(|&t| TRef::T(t)).collect();
                core::microstep(tree, h, &enabled, None)?;
            }
        }
        core::run_to_completion(tree, h)
    }

    /// XState's snapshot data for a machine snapshot (`snapshotData`).
    pub(crate) fn snapshot_data(&self, st: &State<C, E>) -> Value {
        let mut m = Map::new();
        m.insert("status".into(), json!(st.status.as_str()));
        if let Some(o) = &st.output {
            m.insert("output".into(), (**o).clone());
        }
        if let Some(e) = &st.error {
            m.insert("error".into(), (**e).clone());
        }
        m.insert("context".into(), context_json(&st.context));
        m.insert("value".into(), self.state_value(&st.nodes));
        m.insert("children".into(), Value::Object(st.children.iter().map(|(k, r)| (k.clone(), r.to_json())).collect()));
        m.insert("timers".into(), Value::Array(st.timers.iter().map(LogicalTimer::to_data).collect()));
        m.insert(
            "historyValue".into(),
            Value::Object(
                st.history
                    .iter()
                    .map(|(h, nodes)| {
                        let refs = nodes.iter().map(|&n| json!({ "$stateNode": self.data(n).id })).collect();
                        (self.data(*h).id.clone(), Value::Array(refs))
                    })
                    .collect(),
            ),
        );
        m.insert("_stateInputs".into(), json!({}));
        m.insert("_nextTimerId".into(), json!(st.next_timer_id));
        m.insert("nodeIds".into(), Value::Array(st.nodes.iter().map(|&n| json!(self.data(n).id)).collect()));
        Value::Object(m)
    }

    /// Rebuilds a machine snapshot from [`Machine::snapshot_data`].
    pub(crate) fn restore_state(&self, data: &Value) -> Result<State<C, E>, SystemError> {
        let bad = |what: &str| SystemError::Rejected(format!("invalid machine snapshot: {what}"));
        let lookup = |id: &str| {
            self.node_by_id(id).ok_or_else(|| {
                SystemError::Rejected(format!("state \"{id}\" does not exist in this machine definition"))
            })
        };
        let context = C::deserialize(data.get("context").unwrap_or(&Value::Null))
            .map_err(|e| SystemError::Rejected(format!("context does not fit the context type: {e}")))?;
        let mut st = State::new(context, &self.empty);
        st.status = crate::system::parse_status(data)?;
        st.output = data.get("output").cloned().map(Box::new);
        st.error = data.get("error").cloned().map(Box::new);
        st.nodes = data
            .get("nodeIds")
            .and_then(Value::as_array)
            .ok_or_else(|| bad("nodeIds"))?
            .iter()
            .map(|v| lookup(v.as_str().unwrap_or("")))
            .collect::<Result<_, _>>()?;
        self.check_configuration(&st.nodes)?;
        for (h, recorded) in data.get("historyValue").and_then(Value::as_object).into_iter().flatten() {
            let h = lookup(h)?;
            let nodes = recorded
                .as_array()
                .ok_or_else(|| bad("historyValue"))?
                .iter()
                .map(|r| lookup(r.get("$stateNode").and_then(Value::as_str).unwrap_or("")))
                .collect::<Result<_, _>>()?;
            Arc::make_mut(&mut st.history).push((h, nodes));
        }
        for (k, r) in data.get("children").and_then(Value::as_object).into_iter().flatten() {
            Arc::make_mut(&mut st.children).insert(k.clone(), ActorRef::from_json(r).ok_or_else(|| bad("children"))?);
        }
        for t in data.get("timers").and_then(Value::as_array).into_iter().flatten() {
            let event =
                Ev::from_json(t.get("event").cloned().unwrap_or(Value::Null)).ok_or_else(|| bad("timer event"))?;
            Arc::make_mut(&mut st.timers).push(LogicalTimer {
                id: t.get("id").and_then(Value::as_str).map(str::to_owned),
                seq: t.get("seq").and_then(Value::as_u64),
                delay: t.get("delay").and_then(Value::as_u64).unwrap_or(0),
                event: TimerEv::new(event),
            });
        }
        st.next_timer_id = data.get("_nextTimerId").and_then(Value::as_u64).unwrap_or(0);
        Ok(st)
    }

    /// Checks that a configuration is legal.
    /// What restore checks: every state at most once, and no history state.
    /// The configuration need not be legal: XState can produce illegal ones
    /// (fixture `conflicting_regions`) and restores whatever ids it is given.
    fn check_configuration(&self, nodes: &[Nid]) -> Result<(), SystemError> {
        let mut active = vec![false; self.nodes.len()];
        for &n in nodes {
            let id = &self.data(n).id;
            if std::mem::replace(&mut active[n as usize], true) {
                return Err(SystemError::Rejected(format!("state \"{id}\" appears twice")));
            }
            if matches!(self.tree.kind(n), core::Kind::History { .. }) {
                return Err(SystemError::Rejected(format!("history state \"{id}\" cannot be active")));
            }
        }
        Ok(())
    }
}

impl<C, E> Logic for Machine<C, E>
where
    C: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    E: EventType + Send + Sync,
{
    fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }

    fn logic_id(&self) -> Option<String> {
        Some(self.id.clone().unwrap_or_else(|| "(machine)".into()))
    }

    fn initial_transition(&self, _input: Option<&Value>, cx: &mut dyn Context) -> Result<Transition, SystemError> {
        if let Some(e) = &self.start_error {
            return Err(SystemError::Failed(e.clone()));
        }
        let mut st = State::new(self.initial_context.clone(), &self.empty);
        let effects = self.run_macrostep(&mut st, cx, MacroEvent::Init)?;
        Ok(Transition { snapshot: Box::new(st), effects })
    }

    fn transition(
        &self,
        snapshot: &mut Box<dyn AnySnapshot>,
        event: Value,
        cx: &mut dyn Context,
    ) -> Result<Vec<Effect>, SystemError> {
        let st = snapshot
            .as_any_mut()
            .downcast_mut::<State<C, E>>()
            .ok_or_else(|| SystemError::Failed("machine received a foreign snapshot".into()))?;
        self.run_macrostep(st, cx, MacroEvent::Event(event))
    }

    fn snapshot_data(&self, snapshot: &dyn AnySnapshot) -> Value {
        match snapshot.as_any().downcast_ref::<State<C, E>>() {
            Some(st) => Machine::snapshot_data(self, st),
            None => Value::Null,
        }
    }

    fn snapshot_serialize<'a>(
        &'a self,
        snapshot: &'a dyn AnySnapshot,
        compact: bool,
    ) -> Box<dyn erased_serde::Serialize + 'a> {
        match snapshot.as_any().downcast_ref::<State<C, E>>() {
            Some(st) => Box::new(SnapshotView { m: self, st, compact }),
            None => Box::new(Value::Null),
        }
    }

    fn restore(&self, data: &Value) -> Result<Box<dyn AnySnapshot>, SystemError> {
        Ok(Box::new(self.restore_state(data)?))
    }

    fn is_internal_event_type(&self, event_type: &str) -> bool {
        event_type == "xstate.timer" || self.is_internal_event(event_type)
    }

    fn actor_sources(&self) -> Vec<(String, Arc<dyn Logic>)> {
        self.actors.clone()
    }
}
