//! The native host: runs JSON machines on the shared [`crate::core`]
//! algorithm with XState's value semantics for `machineFromJSON` machines.
//!
//! - Transition results (`context`, `actions`) are computed against the
//!   snapshot at the start of the microstep and merged after exits, as
//!   XState's transition functions are.
//! - Actions produce effects. `@xstate.raise` without a delay goes to the
//!   internal queue; delayed raises and cancels update the snapshot's
//!   logical timers while the step is resolved; emits, logs and custom
//!   actions run after the macrostep, in order.
//! - Eventless transitions are re-selected only after a microstep that
//!   produced a new snapshot, mirroring XState's snapshot-identity check.

use std::borrow::Cow;
use std::collections::VecDeque;

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{Map, Value};

use crate::core::{self, Host, MicrostepResult, Nid, RegionOutput, Resolved, Resolver, TRef, Tid};
use crate::error::RuntimeError;
use crate::event::{Ev, EventType, AFTER, DONE_STATE};
use crate::machine::*;
use crate::registry::GuardArgs;

/// The lifecycle status of an actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Active,
    Done,
    Error,
    Stopped,
}

/// A logical timer's identity, as XState's `getTimerKey`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TimerKey {
    /// A delayed `@xstate.raise` with an `id`.
    Id(String),
    /// A delayed `@xstate.raise` without an `id`, numbered by `_nextTimerId`.
    Seq(u64),
    /// The `index`-th `after` entry of a state (identified by its event).
    After { node: Nid, index: u32 },
}

pub(crate) struct Timer<E> {
    pub key: TimerKey,
    pub delay: u64,
    pub event: Ev<E>,
    /// When the scheduler fires it.
    pub due: u64,
    /// Scheduling order, which breaks ties between timers due at once.
    pub order: u64,
}

/// The actor's snapshot plus its scheduler and outboxes.
pub(crate) struct State<C, E> {
    /// The configuration, in XState's order.
    pub nodes: Vec<Nid>,
    pub context: C,
    pub status: Status,
    pub output: Option<Value>,
    pub error: Option<Value>,
    /// `historyValue`, in key order.
    pub history: Vec<(Nid, Vec<Nid>)>,
    /// `snapshot.timers`, in order.
    pub timers: Vec<Timer<E>>,
    pub next_timer_id: u64,
    pub timer_order: u64,
    pub now: u64,
    pub emitted: Vec<Value>,
    pub logs: Vec<Vec<Value>>,
}

impl<C, E> State<C, E> {
    pub fn new(context: C, now: u64) -> Self {
        Self {
            nodes: vec![ROOT],
            context,
            status: Status::Active,
            output: None,
            error: None,
            history: Vec::new(),
            timers: Vec::new(),
            next_timer_id: 0,
            timer_order: 0,
            now,
            emitted: Vec::new(),
            logs: Vec::new(),
        }
    }

    /// The timer the scheduler fires next.
    pub fn next_timer(&self) -> Option<usize> {
        self.timers.iter().enumerate().min_by_key(|(_, t)| (t.due, t.order)).map(|(i, _)| i)
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
    CancelId(String),
    CancelAfter { node: Nid, index: u32 },
    ScheduleAfter { node: Nid, index: u32, delay: u64 },
    Log(Vec<Value>),
    Emit(Value),
    Custom(u32),
}

/// An effect that runs after the macrostep.
enum Effect {
    Emit(Value),
    Log(Vec<Value>),
    Custom(u32),
}

/// The result of running an action list or a transition function: the
/// context object XState would merge (`res.context`), the queued operations
/// and the raised events.
struct Outcome<E> {
    context: Option<Map<String, Value>>,
    ops: Vec<Op<E>>,
    internal: Vec<Ev<E>>,
}

impl<E> Outcome<E> {
    fn empty() -> Self {
        Self { context: None, ops: Vec::new(), internal: Vec::new() }
    }
}

/// Which macrostep is being run.
pub(crate) enum MacroEvent<E> {
    Init,
    External(Ev<E>),
    Timer(Ev<E>),
}

struct NativeHost<'a, C, E> {
    m: &'a Machine<C, E>,
    st: &'a mut State<C, E>,
    event: Ev<E>,
    queue: VecDeque<Ev<E>>,
    effects: Vec<Effect>,
    /// The snapshot object's identity: bumped whenever XState would clone it.
    token: u64,
    /// Whether the current microstep has cloned the snapshot.
    cloned: bool,
    /// Transition results computed this microstep (`getCurrentTransitionResult`).
    outcomes: Vec<(Tid, Outcome<E>)>,
    recent: VecDeque<Value>,
}

impl<C, E> NativeHost<'_, C, E>
where
    C: Serialize + DeserializeOwned,
    E: EventType,
{
    fn context_object(&self) -> Result<Map<String, Value>, RuntimeError> {
        match serde_json::to_value(&self.st.context) {
            Ok(Value::Object(map)) => Ok(map),
            Ok(_) => Ok(Map::new()),
            Err(e) => Err(RuntimeError::Context { message: e.to_string() }),
        }
    }

    /// `mergeContextPatch`.
    fn merge_context(&mut self, patch: &Map<String, Value>) -> Result<(), RuntimeError> {
        let mut map = self.context_object()?;
        for (k, v) in patch {
            map.insert(k.clone(), v.clone());
        }
        self.st.context =
            C::deserialize(&Value::Object(map)).map_err(|e| RuntimeError::Context { message: e.to_string() })?;
        self.cloned = true;
        Ok(())
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
                Action::Cancel(id) => out.ops.push(Op::CancelId(id.clone())),
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
    /// with `context` or `actions`, evaluated against the current snapshot.
    fn transition_outcome(&self, t: Tid) -> Result<Outcome<E>, RuntimeError> {
        let tr = self.m.transition(t);
        if !tr.is_dynamic() {
            return Ok(Outcome::empty());
        }
        let mut base = self.context_object()?;
        if let Some(patch) = &tr.context {
            for (k, v) in patch {
                base.insert(k.clone(), v.clone());
            }
        }
        let mut out = if tr.actions.is_empty() { Outcome::empty() } else { self.execute_actions(&tr.actions, &base) };
        if out.context.is_none() {
            out.context = tr.context.clone();
        }
        Ok(out)
    }

    /// `resolveActionsWithContext`: timers update the snapshot now, the
    /// rest become effects.
    fn resolve_ops(&mut self, ops: Vec<Op<E>>) {
        for op in ops {
            match op {
                Op::Raise { event, id, delay } => {
                    let key = match id {
                        Some(id) => TimerKey::Id(id),
                        None => {
                            let seq = self.st.next_timer_id;
                            self.st.next_timer_id += 1;
                            TimerKey::Seq(seq)
                        }
                    };
                    self.schedule(key, delay, event);
                }
                Op::ScheduleAfter { node, index, delay } => {
                    let def = &self.m.data(node).after[index as usize];
                    let mut data = Map::new();
                    data.insert("delay".into(), def.event_delay.clone());
                    data.insert("stateId".into(), Value::String(self.m.data(node).id.clone()));
                    let event = Ev::Internal { event_type: Cow::Borrowed(AFTER), data };
                    self.schedule(TimerKey::After { node, index }, delay, event);
                }
                Op::CancelId(id) => self.cancel(|k| matches!(k, TimerKey::Id(x) if *x == id)),
                Op::CancelAfter { node, index } => {
                    self.cancel(|k| matches!(k, TimerKey::After { node: n, index: i } if *n == node && *i == index))
                }
                Op::Log(args) => self.effects.push(Effect::Log(args)),
                Op::Emit(event) => self.effects.push(Effect::Emit(event)),
                Op::Custom(i) => self.effects.push(Effect::Custom(i)),
            }
        }
    }

    /// Schedules a timer; a pending timer with the same key is replaced in place.
    fn schedule(&mut self, key: TimerKey, delay: u64, event: Ev<E>) {
        let order = self.st.timer_order;
        self.st.timer_order += 1;
        let timer = Timer { key, delay, event, due: self.st.now.saturating_add(delay), order };
        match self.st.timers.iter_mut().find(|t| t.key == timer.key) {
            Some(existing) => *existing = timer,
            None => self.st.timers.push(timer),
        }
        self.cloned = true;
    }

    fn cancel(&mut self, matches: impl Fn(&TimerKey) -> bool) {
        let before = self.st.timers.len();
        self.st.timers.retain(|t| !matches(&t.key));
        if self.st.timers.len() != before {
            self.cloned = true;
        }
    }

    /// A state's exit function: `after` timers are cancelled (the wrappers
    /// nest, so the last key first), then the exit actions run.
    fn exit_outcome(&self, node: Nid) -> Result<Outcome<E>, RuntimeError> {
        let data = self.m.data(node);
        let base = self.context_object()?;
        let mut out = self.execute_actions(&data.exit, &base);
        let mut ops: Vec<Op<E>> =
            (0..data.after.len() as u32).rev().map(|index| Op::CancelAfter { node, index }).collect();
        ops.append(&mut out.ops);
        out.ops = ops;
        Ok(out)
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
        }
    }
}

impl<C, E> Host for NativeHost<'_, C, E>
where
    C: Serialize + DeserializeOwned,
    E: EventType,
{
    type Error = RuntimeError;

    fn config(&self) -> Vec<Nid> {
        self.st.nodes.clone()
    }

    fn history(&self, history: Nid) -> Option<Vec<Nid>> {
        self.st.history.iter().find(|(h, _)| *h == history).map(|(_, v)| v.clone())
    }

    fn static_initial(&self, node: Nid) -> Vec<Nid> {
        self.m.data(node).initial.clone()
    }

    fn begin_selection(&mut self) -> Result<(), RuntimeError> {
        Ok(())
    }

    fn eval_candidate(&mut self, t: Tid, _node: Nid, _eventless: bool) -> Result<bool, RuntimeError> {
        let tr = self.m.transition(t);
        for (key, expected) in &tr.matches {
            match self.event.field(key) {
                Some(actual) if js_eq(&actual, expected) => {}
                _ => return Ok(false),
            }
        }
        Ok(match &tr.guard {
            Some(g) => self.eval_guard(g),
            None => true,
        })
    }

    fn resolve(&mut self, resolver: Resolver, t: TRef) -> Result<Resolved, RuntimeError> {
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

    fn begin_microstep(&mut self) -> Result<(), RuntimeError> {
        self.outcomes.clear();
        self.cloned = false;
        Ok(())
    }

    fn exit_state(&mut self, node: Nid) -> Result<(), RuntimeError> {
        let out = self.exit_outcome(node)?;
        self.queue.extend(out.internal);
        if let Some(ctx) = &out.context {
            self.merge_context(ctx)?;
        }
        self.resolve_ops(out.ops);
        Ok(())
    }

    fn transition_content(&mut self, filtered: &[TRef]) -> Result<(), RuntimeError> {
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
            }
            ops.extend(outcome.ops);
            internal.extend(outcome.internal);
        }
        self.queue.extend(internal);
        self.resolve_ops(ops);
        Ok(())
    }

    fn begin_entry(&mut self, _active: &[Nid], _entered: &[(TRef, Vec<Nid>)]) -> Result<(), RuntimeError> {
        Ok(())
    }

    fn enter_state(&mut self, node: Nid, _default_entry: bool, _history_defaults: &[Nid]) -> Result<(), RuntimeError> {
        let data = self.m.data(node);
        let base = self.context_object()?;
        let mut out = self.execute_actions(&data.entry, &base);
        // `after` timers are scheduled by entry wrappers, after the entry
        // actions and against the context they produced.
        if let Some(ctx) = &out.context {
            self.merge_context(ctx)?;
        }
        for (index, def) in data.after.iter().enumerate() {
            let delay = self.resolve_delay(def.delay);
            out.ops.push(Op::ScheduleAfter { node, index: index as u32, delay });
        }
        self.queue.extend(out.internal);
        self.resolve_ops(out.ops);
        // The `initial` and history default transitions of JSON machines
        // carry no context or actions.
        Ok(())
    }

    fn done_compound(&mut self, parent: Nid, final_child: Nid) -> Result<(), RuntimeError> {
        let event = self.done_event(parent, self.m.data(final_child).output.clone());
        self.queue.push_back(event);
        Ok(())
    }

    fn done_parallel(&mut self, parallel: Nid, regions: &[RegionOutput]) -> Result<(), RuntimeError> {
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

    fn machine_done(&mut self, completion: Nid) -> Result<(), RuntimeError> {
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
            Some(output) => Some(output.clone()),
            None if tree.node(completion).parent == Some(ROOT) => completion_output,
            None => None,
        };
        self.cloned = true;
        Ok(())
    }

    fn end_entry(&mut self) -> Result<(), RuntimeError> {
        Ok(())
    }

    fn is_done(&self) -> bool {
        self.st.status == Status::Done
    }

    fn exit_on_done(&mut self, nodes_desc: &[Nid]) -> Result<(), RuntimeError> {
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
        if !self.st.timers.is_empty() {
            self.st.timers.clear();
            self.cloned = true;
        }
        Ok(())
    }

    fn apply_microstep(&mut self, result: Option<MicrostepResult>, _enabled: &[TRef]) -> Result<(), RuntimeError> {
        if let Some(r) = result {
            if r.structure_changed {
                self.st.nodes = r.nodes;
                for (h, nodes) in r.history_updates {
                    match self.st.history.iter_mut().find(|(x, _)| *x == h) {
                        Some(entry) => entry.1 = nodes,
                        None => self.st.history.push((h, nodes)),
                    }
                }
                self.cloned = true;
            }
        }
        if self.cloned {
            self.token += 1;
        }
        Ok(())
    }

    fn is_active(&self) -> bool {
        self.st.status == Status::Active
    }

    fn check_iteration(&mut self, iteration: usize) -> Result<(), RuntimeError> {
        if iteration > self.m.options.max_iterations {
            return Err(RuntimeError::InfiniteLoop {
                event_type: self.event.event_type().to_owned(),
                max_iterations: self.m.options.max_iterations,
                states: self.recent.iter().cloned().collect(),
            });
        }
        self.recent.push_back(self.m.state_value(&self.st.nodes));
        if self.recent.len() > 5 {
            self.recent.pop_front();
        }
        Ok(())
    }

    fn snapshot_token(&self) -> u64 {
        self.token
    }

    fn drain_raised(&mut self) -> Result<(), RuntimeError> {
        Ok(())
    }

    fn queue_is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    fn dequeue(&mut self) -> Result<Option<String>, RuntimeError> {
        Ok(self.queue.pop_front().map(|e| {
            let ty = e.event_type().to_owned();
            self.event = e;
            ty
        }))
    }
}

impl<C, E> Machine<C, E>
where
    C: Serialize + DeserializeOwned,
    E: EventType,
{
    /// Runs one macrostep (XState's `macrostep`, or the initial transition)
    /// and then its effects. Returns whether an external event was handled.
    pub(crate) fn macrostep(&self, st: &mut State<C, E>, event: MacroEvent<E>) -> Result<bool, RuntimeError> {
        let mut h = NativeHost {
            m: self,
            st,
            event: Ev::Init,
            queue: VecDeque::new(),
            effects: Vec::new(),
            token: 0,
            cloned: false,
            outcomes: Vec::new(),
            recent: VecDeque::new(),
        };
        let result = run_macrostep(self, &mut h, event);
        let effects = std::mem::take(&mut h.effects);
        match result {
            Ok(handled) => {
                for effect in effects {
                    match effect {
                        Effect::Emit(e) => st.emitted.push(e),
                        Effect::Log(args) => st.logs.push(args),
                        Effect::Custom(i) => (self.actions[i as usize])(),
                    }
                }
                Ok(handled)
            }
            Err(e) => {
                st.status = Status::Error;
                st.error = Some(Value::String(e.to_string()));
                st.timers.clear();
                Err(e)
            }
        }
    }
}

fn run_macrostep<C, E>(
    m: &Machine<C, E>,
    h: &mut NativeHost<'_, C, E>,
    event: MacroEvent<E>,
) -> Result<bool, RuntimeError>
where
    C: Serialize + DeserializeOwned,
    E: EventType,
{
    let tree = &m.tree;
    let mut handled = false;
    match event {
        MacroEvent::Init => {
            h.event = Ev::Init;
            core::initial_microstep(tree, h)?;
        }
        MacroEvent::External(ev) => {
            if h.st.status != Status::Active {
                return Ok(false);
            }
            let ty = ev.event_type().to_owned();
            h.event = ev;
            let transitions = core::select_transitions(tree, h, &ty)?;
            if ty.starts_with("xstate.error.") && transitions.is_empty() {
                h.st.status = Status::Error;
                h.st.error = h.event.field("error");
                return Ok(false);
            }
            handled = !transitions.is_empty();
            let enabled: Vec<TRef> = transitions.into_iter().map(TRef::T).collect();
            core::microstep(tree, h, &enabled, None)?;
        }
        MacroEvent::Timer(ev) => {
            if h.st.status != Status::Active {
                return Ok(false);
            }
            h.queue.push_back(ev);
            h.event = Ev::Internal { event_type: Cow::Borrowed("xstate.timer"), data: Map::new() };
        }
    }
    core::run_to_completion(tree, h)?;
    Ok(handled)
}
