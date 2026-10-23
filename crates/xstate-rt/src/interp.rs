//! The interpreter: transition selection, microsteps and macrosteps.
//!
//! This follows the SCXML algorithm as XState v6 implements it
//! (`createActor` / `macrostep` / `microstep` in the `xstate` package), including
//! its departures from SCXML: per-node candidate selection that bubbles from
//! the deepest state, transition domains that narrow past parallel states so
//! sibling regions are not exited, and `xstate.done.state` / `xstate.after`
//! lifecycle events that carry their identity as payload.

use std::borrow::Cow;
use std::collections::VecDeque;

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{Map, Value};

use crate::error::RuntimeError;
use crate::event::{Ev, EventType, AFTER, DONE_STATE};
use crate::machine::*;
use crate::registry::{ActionArgs, GuardArgs};

/// The lifecycle status of an actor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Status {
    Active,
    Done,
    Error,
    Stopped,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TimerKey {
    /// A delayed `@xstate.raise` with an `id`.
    Id(String),
    /// A delayed `@xstate.raise` without an `id`, numbered by the actor.
    Seq(u64),
    /// The `index`-th `after` entry of a state.
    After { node: NodeId, index: u32 },
}

pub(crate) struct Timer<E> {
    pub key: TimerKey,
    pub due: u64,
    /// Scheduling order, which breaks ties between timers due at once.
    pub order: u64,
    pub delay: u64,
    pub event: Ev<E>,
}

pub(crate) struct State<C, E> {
    pub active: Vec<bool>,
    pub context: C,
    pub status: Status,
    pub output: Option<Value>,
    /// Recorded configurations, indexed by history node.
    pub history: Vec<Option<Vec<NodeId>>>,
    pub timers: Vec<Timer<E>>,
    pub next_timer_id: u64,
    pub timer_order: u64,
    pub now: u64,
    pub queue: VecDeque<Ev<E>>,
    pub emitted: Vec<Value>,
    pub logs: Vec<Vec<Value>>,
}

impl<C, E> State<C, E> {
    pub fn new(node_count: usize, context: C, now: u64) -> Self {
        Self {
            active: vec![false; node_count],
            context,
            status: Status::Active,
            output: None,
            history: vec![None; node_count],
            timers: Vec::new(),
            next_timer_id: 0,
            timer_order: 0,
            now,
            queue: VecDeque::new(),
            emitted: Vec::new(),
            logs: Vec::new(),
        }
    }

    /// The timer that fires next: earliest due time, then scheduling order.
    pub fn next_timer(&self) -> Option<usize> {
        self.timers.iter().enumerate().min_by_key(|(_, t)| (t.due, t.order)).map(|(i, _)| i)
    }
}

/// `Object.is` over JSON values, except that numbers compare by value so
/// `1000` and `1000.0` match.
fn js_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64() == y.as_f64(),
        _ => a == b,
    }
}

/// Per-transition data computed against the configuration before the
/// microstep exits anything.
struct Planned {
    t: TransitionId,
    domain: Option<NodeId>,
    effective_targets: Vec<NodeId>,
}

impl<C, E> Machine<C, E>
where
    C: Serialize + DeserializeOwned,
    E: EventType,
{
    // ---- selection -------------------------------------------------------

    fn eval_guard(&self, guard: &Guard, st: &State<C, E>, ev: &Ev<E>) -> bool {
        match guard {
            Guard::Custom(i) => {
                let args = GuardArgs { context: &st.context, event: ev, machine: self, active: &st.active };
                (self.guards[*i as usize])(&args)
            }
            Guard::Not(inner) => !self.eval_guard(inner, st, ev),
            Guard::StateIn(node) => st.active[*node as usize],
        }
    }

    fn eval_candidate(&self, t: TransitionId, st: &State<C, E>, ev: &Ev<E>) -> bool {
        let tr = &self.transitions[t as usize];
        for (key, expected) in &tr.matches {
            match ev.field(key) {
                Some(actual) if js_eq(&actual, expected) => {}
                _ => return false,
            }
        }
        match &tr.guard {
            Some(g) => self.eval_guard(g, st, ev),
            None => true,
        }
    }

    /// The first enabled candidate of one node.
    fn node_next(
        &self,
        node: NodeId,
        st: &State<C, E>,
        ev: &Ev<E>,
        key: Option<u32>,
        out: &mut Vec<TransitionId>,
    ) -> bool {
        if self.node(node).kind == NodeKind::Final {
            // Final states are inert.
            return false;
        }
        for t in self.candidates(node, key, ev.event_type()) {
            if self.eval_candidate(t, st, ev) {
                out.push(t);
                return true;
            }
        }
        false
    }

    /// XState's `transitionNode`: the deepest active states get the first
    /// chance; a node is asked only when none of its active descendants
    /// selected a transition.
    fn select_node(
        &self,
        node: NodeId,
        st: &State<C, E>,
        ev: &Ev<E>,
        key: Option<u32>,
        out: &mut Vec<TransitionId>,
    ) -> bool {
        let n = self.node(node);
        let found = match n.kind {
            NodeKind::Atomic | NodeKind::Final | NodeKind::History { .. } => false,
            NodeKind::Compound => match n.children.iter().copied().find(|&c| st.active[c as usize]) {
                Some(child) => self.select_node(child, st, ev, key, out),
                None => false,
            },
            NodeKind::Parallel => {
                let mut found = false;
                for &child in &n.children {
                    if st.active[child as usize] && self.select_node(child, st, ev, key, out) {
                        found = true;
                    }
                }
                found
            }
        };
        found || self.node_next(node, st, ev, key, out)
    }

    pub(crate) fn select_transitions(&self, st: &State<C, E>, ev: &Ev<E>) -> Vec<TransitionId> {
        let key = self.event_keys.get(ev.event_type()).copied();
        let mut out = Vec::new();
        self.select_node(ROOT, st, ev, key, &mut out);
        if out.len() > 1 {
            out = self.remove_conflicts(&out, st);
        }
        out
    }

    fn select_eventless(&self, st: &State<C, E>, ev: &Ev<E>) -> Vec<TransitionId> {
        let mut enabled: Vec<TransitionId> = Vec::new();
        for (s, node) in self.nodes.iter().enumerate() {
            if !st.active[s] || !node.is_atomic_like() {
                continue;
            }
            let mut m = Some(s as NodeId);
            'ancestors: while let Some(x) = m {
                for &t in &self.node(x).always {
                    if self.eval_candidate(t, st, ev) {
                        if !enabled.contains(&t) {
                            enabled.push(t);
                        }
                        break 'ancestors;
                    }
                }
                m = self.node(x).parent;
            }
        }
        if enabled.len() > 1 {
            enabled = self.remove_conflicts(&enabled, st);
        }
        enabled
    }

    fn remove_conflicts(&self, enabled: &[TransitionId], st: &State<C, E>) -> Vec<TransitionId> {
        let exit_sets: Vec<Vec<bool>> = enabled.iter().map(|&t| self.exit_set(t, st)).collect();
        let intersects = |a: usize, b: usize| exit_sets[a].iter().zip(&exit_sets[b]).any(|(x, y)| *x && *y);
        // Indices into `enabled`.
        let mut filtered: Vec<usize> = Vec::new();
        for i in 0..enabled.len() {
            let source = self.transitions[enabled[i] as usize].source;
            let mut preempted = false;
            let mut remove = Vec::new();
            for &j in &filtered {
                if intersects(i, j) {
                    if self.is_descendant(source, Some(self.transitions[enabled[j] as usize].source)) {
                        remove.push(j);
                    } else {
                        preempted = true;
                        break;
                    }
                }
            }
            if !preempted {
                filtered.retain(|j| !remove.contains(j));
                filtered.push(i);
            }
        }
        filtered.into_iter().map(|i| enabled[i]).collect()
    }

    // ---- domains -----------------------------------------------------------

    /// Targets with history states replaced by their recorded or default
    /// configuration.
    fn effective_targets(&self, targets: &[NodeId], st: &State<C, E>) -> Vec<NodeId> {
        let mut out = Vec::new();
        for &target in targets {
            if matches!(self.node(target).kind, NodeKind::History { .. }) {
                let resolved = match &st.history[target as usize] {
                    Some(recorded) => recorded.clone(),
                    None => self.effective_targets(&self.node(target).history_target, st),
                };
                for n in resolved {
                    if !out.contains(&n) {
                        out.push(n);
                    }
                }
            } else if !out.contains(&target) {
                out.push(target);
            }
        }
        out
    }

    /// Narrows a parallel domain to the region that contains every target,
    /// so a transition confined to one region leaves its siblings alone.
    fn narrow_parallel(&self, mut domain: NodeId, targets: &[NodeId]) -> NodeId {
        while self.node(domain).kind == NodeKind::Parallel {
            let region = self
                .node(domain)
                .children
                .iter()
                .copied()
                .find(|&child| targets.iter().all(|&t| t == child || self.is_descendant(t, Some(child))));
            match region {
                Some(r) => domain = r,
                None => break,
            }
        }
        domain
    }

    /// The transition domain; `None` means the whole machine, root included.
    fn transition_domain(&self, t: TransitionId, effective: &[NodeId]) -> Option<NodeId> {
        let tr = &self.transitions[t as usize];
        let source = tr.source;
        let restores_source_via_history =
            tr.targets.iter().any(|&x| matches!(self.node(x).kind, NodeKind::History { .. }))
                && effective.contains(&source);
        if !restores_source_via_history && effective.iter().all(|&x| x == source || self.is_descendant(x, Some(source)))
        {
            return Some(if tr.reenter { source } else { self.narrow_parallel(source, effective) });
        }
        let head = effective.first().copied().unwrap_or(source);
        for ancestor in self.proper_ancestors(head, None) {
            let all_inside = effective
                .iter()
                .skip(1)
                .chain(std::iter::once(&source))
                .all(|&x| self.is_descendant(x, Some(ancestor)));
            if all_inside {
                return Some(if tr.reenter { ancestor } else { self.narrow_parallel(ancestor, effective) });
            }
        }
        if tr.reenter {
            return None;
        }
        Some(self.narrow_parallel(ROOT, effective))
    }

    fn exit_set(&self, t: TransitionId, st: &State<C, E>) -> Vec<bool> {
        let mut set = vec![false; self.nodes.len()];
        let tr = &self.transitions[t as usize];
        if tr.targets.is_empty() {
            return set;
        }
        let effective = self.effective_targets(&tr.targets, st);
        let domain = self.transition_domain(t, &effective);
        self.mark_exit_set(tr, domain, st, &mut set);
        set
    }

    fn mark_exit_set(&self, tr: &Transition, domain: Option<NodeId>, st: &State<C, E>, set: &mut [bool]) {
        if tr.reenter && domain == Some(tr.source) {
            set[tr.source as usize] = true;
        }
        for (s, &active) in st.active.iter().enumerate() {
            if active && self.is_descendant(s as NodeId, domain) {
                set[s] = true;
            }
        }
    }

    // ---- microstep ---------------------------------------------------------

    /// Runs one microstep. Returns whether anything a guard could observe
    /// changed (configuration, history, context or timers).
    pub(crate) fn microstep(
        &self,
        transitions: &[TransitionId],
        st: &mut State<C, E>,
        ev: &Ev<E>,
    ) -> Result<bool, RuntimeError> {
        if transitions.is_empty() {
            return Ok(false);
        }
        let mut changed = false;
        let changes_state = transitions.iter().any(|&t| {
            let tr = &self.transitions[t as usize];
            !tr.targets.is_empty() || tr.reenter
        });
        // Domains and effective targets use the history recorded before this
        // microstep, as XState does.
        let planned: Vec<Planned> = transitions
            .iter()
            .map(|&t| {
                let effective_targets = self.effective_targets(&self.transitions[t as usize].targets, st);
                let domain = self.transition_domain(t, &effective_targets);
                Planned { t, domain, effective_targets }
            })
            .collect();

        if changes_state {
            self.exit_states(&planned, st, ev, &mut changed)?;
        }

        for p in &planned {
            let tr = &self.transitions[p.t as usize];
            if let Some(patch) = &tr.context {
                self.assign(patch, st)?;
                changed = true;
            }
            self.run_actions(&tr.actions, st, ev, &mut changed)?;
        }

        if changes_state {
            let mut to_enter = vec![false; self.nodes.len()];
            for p in &planned {
                self.add_entry_set(p, st, &mut to_enter);
            }
            self.enter_states(&to_enter, st, ev, &mut changed)?;
        }

        if st.status == Status::Done {
            self.exit_all(st, ev, &mut changed)?;
        }
        Ok(changed)
    }

    fn exit_states(
        &self,
        planned: &[Planned],
        st: &mut State<C, E>,
        ev: &Ev<E>,
        changed: &mut bool,
    ) -> Result<(), RuntimeError> {
        let mut set = vec![false; self.nodes.len()];
        for p in planned {
            let tr = &self.transitions[p.t as usize];
            if !tr.targets.is_empty() {
                self.mark_exit_set(tr, p.domain, st, &mut set);
            }
        }
        // Record history against the configuration before anything exits.
        for s in (0..self.nodes.len()).rev().filter(|&s| set[s]) {
            for &h in &self.node(s as NodeId).history_children {
                let deep = matches!(self.node(h).kind, NodeKind::History { deep: true });
                let recorded: Vec<NodeId> = (0..self.nodes.len() as NodeId)
                    .filter(|&n| st.active[n as usize])
                    .filter(|&n| {
                        if deep {
                            self.node(n).is_atomic_like() && self.is_descendant(n, Some(s as NodeId))
                        } else {
                            self.node(n).parent == Some(s as NodeId)
                        }
                    })
                    .collect();
                st.history[h as usize] = Some(recorded);
                *changed = true;
            }
        }
        for s in (0..self.nodes.len()).rev().filter(|&s| set[s]) {
            let node = s as NodeId;
            self.cancel_after_timers(node, st, changed);
            self.run_actions(&self.node(node).exit, st, ev, changed)?;
            st.active[s] = false;
            *changed = true;
        }
        Ok(())
    }

    fn add_entry_set(&self, p: &Planned, st: &State<C, E>, to_enter: &mut [bool]) {
        let tr = &self.transitions[p.t as usize];
        for &target in &tr.targets {
            let is_history = matches!(self.node(target).kind, NodeKind::History { .. });
            if !is_history && (tr.source != target || Some(tr.source) != p.domain || tr.reenter) {
                to_enter[target as usize] = true;
            }
            self.add_descendants(target, st, to_enter);
        }
        let reentrancy_domain = if tr.source == ROOT && tr.reenter { None } else { p.domain };
        for &s in &p.effective_targets {
            let mut ancestors = self.proper_ancestors(s, p.domain);
            if let Some(d) = p.domain {
                if self.node(d).kind == NodeKind::Parallel {
                    ancestors.push(d);
                }
            }
            self.add_ancestors(&ancestors, reentrancy_domain, st, to_enter);
        }
        if tr.reenter && p.domain == Some(tr.source) {
            to_enter[tr.source as usize] = true;
        }
    }

    fn has_descendant_in(&self, node: NodeId, set: &[bool]) -> bool {
        (node + 1..self.node(node).end).any(|d| set[d as usize])
    }

    fn add_ancestors(
        &self,
        ancestors: &[NodeId],
        reentrancy_domain: Option<NodeId>,
        st: &State<C, E>,
        to_enter: &mut [bool],
    ) {
        for &anc in ancestors {
            if self.is_descendant(anc, reentrancy_domain) {
                to_enter[anc as usize] = true;
            }
            if self.node(anc).kind == NodeKind::Parallel {
                for &child in &self.node(anc).children {
                    if !self.has_descendant_in(child, to_enter) {
                        to_enter[child as usize] = true;
                        self.add_descendants(child, st, to_enter);
                    }
                }
            }
        }
    }

    fn add_descendants(&self, node: NodeId, st: &State<C, E>, to_enter: &mut [bool]) {
        let n = self.node(node);
        match n.kind {
            NodeKind::History { .. } => {
                let parent = n.parent.expect("history states have a parent");
                let targets = match &st.history[node as usize] {
                    Some(recorded) => recorded.clone(),
                    None => n.history_target.clone(),
                };
                for &s in &targets {
                    to_enter[s as usize] = true;
                    self.add_descendants(s, st, to_enter);
                }
                for &s in &targets {
                    let ancestors = self.proper_ancestors(s, Some(parent));
                    self.add_ancestors(&ancestors, None, st, to_enter);
                }
            }
            NodeKind::Compound => {
                let initial = n.initial.expect("compound states have an initial state");
                if !matches!(self.node(initial).kind, NodeKind::History { .. }) {
                    to_enter[initial as usize] = true;
                }
                self.add_descendants(initial, st, to_enter);
                let ancestors = self.proper_ancestors(initial, Some(node));
                self.add_ancestors(&ancestors, None, st, to_enter);
            }
            NodeKind::Parallel => {
                for &child in &n.children {
                    if !self.has_descendant_in(child, to_enter) {
                        to_enter[child as usize] = true;
                        self.add_descendants(child, st, to_enter);
                    }
                }
            }
            NodeKind::Atomic | NodeKind::Final => {}
        }
    }

    fn in_final_state(&self, node: NodeId, active: &[bool]) -> bool {
        let n = self.node(node);
        match n.kind {
            NodeKind::Compound => {
                n.children.iter().any(|&c| self.node(c).kind == NodeKind::Final && active[c as usize])
            }
            NodeKind::Parallel => n.children.iter().all(|&c| self.in_final_state(c, active)),
            kind => kind == NodeKind::Final,
        }
    }

    fn done_event(&self, node: NodeId, output: Option<Value>) -> Ev<E> {
        let mut data = Map::new();
        data.insert("stateId".into(), Value::String(self.node(node).id.clone()));
        if let Some(output) = output {
            data.insert("output".into(), output);
        }
        Ev::Internal { event_type: Cow::Borrowed(DONE_STATE), data }
    }

    fn queued_done_output(&self, node: NodeId, st: &State<C, E>) -> Option<Value> {
        let id = &self.node(node).id;
        st.queue.iter().find_map(|e| match e {
            Ev::Internal { event_type, data }
                if event_type == DONE_STATE && data.get("stateId").and_then(Value::as_str) == Some(id) =>
            {
                data.get("output").cloned()
            }
            _ => None,
        })
    }

    pub(crate) fn enter_states(
        &self,
        to_enter: &[bool],
        st: &mut State<C, E>,
        ev: &Ev<E>,
        changed: &mut bool,
    ) -> Result<(), RuntimeError> {
        let mut completed = vec![false; self.nodes.len()];
        for s in (0..self.nodes.len()).filter(|&s| to_enter[s]) {
            let node = s as NodeId;
            st.active[s] = true;
            *changed = true;
            self.run_actions(&self.node(node).entry, st, ev, changed)?;
            self.schedule_after_timers(node, st, ev, changed);

            let n = self.node(node);
            if n.kind != NodeKind::Final {
                continue;
            }
            let parent = n.parent;
            let mut marker = match parent {
                Some(p) if self.node(p).kind == NodeKind::Parallel => Some(p),
                Some(p) => self.node(p).parent,
                None => None,
            };
            let mut completion = marker.unwrap_or(node);
            if let Some(p) = parent {
                if self.node(p).kind == NodeKind::Compound {
                    let event = self.done_event(p, n.output.clone());
                    st.queue.push_back(event);
                }
            }
            while let Some(m) = marker {
                if self.node(m).kind != NodeKind::Parallel
                    || completed[m as usize]
                    || !self.in_final_state(m, &st.active)
                {
                    break;
                }
                completed[m as usize] = true;
                let mut region_output = Map::new();
                for &region in &self.node(m).children {
                    let r = self.node(region);
                    let output = match r.kind {
                        NodeKind::Final => r.output.clone(),
                        NodeKind::Parallel => self.queued_done_output(region, st),
                        _ => r
                            .children
                            .iter()
                            .find(|&&c| self.node(c).kind == NodeKind::Final && st.active[c as usize])
                            .and_then(|&c| self.node(c).output.clone()),
                    };
                    if let Some(output) = output {
                        region_output.insert(r.key.clone(), output);
                    }
                }
                let event = self.done_event(m, Some(Value::Object(region_output)));
                st.queue.push_back(event);
                completion = m;
                marker = self.node(m).parent;
            }
            if marker.is_some() {
                continue;
            }
            // A final child of the root (or a completed parallel root) completes the actor.
            st.status = Status::Done;
            st.output = self.output.clone().or_else(|| {
                let c = self.node(completion);
                if c.parent == Some(ROOT) {
                    c.output.clone()
                } else {
                    None
                }
            });
        }
        Ok(())
    }

    /// When the actor completes, every active state's exit actions run and
    /// all timers are cancelled. The configuration is kept for inspection.
    fn exit_all(&self, st: &mut State<C, E>, ev: &Ev<E>, changed: &mut bool) -> Result<(), RuntimeError> {
        for s in (0..self.nodes.len()).rev() {
            if st.active[s] {
                self.run_actions(&self.node(s as NodeId).exit, st, ev, changed)?;
            }
        }
        st.timers.clear();
        Ok(())
    }

    // ---- actions and timers ------------------------------------------------

    /// `@xstate.assign` and transition `context` patches: a shallow merge
    /// through JSON. This is the slow path; Rust actions should write the
    /// context directly.
    fn assign(&self, patch: &Map<String, Value>, st: &mut State<C, E>) -> Result<(), RuntimeError> {
        let mut value =
            serde_json::to_value(&st.context).map_err(|e| RuntimeError::Context { message: e.to_string() })?;
        match &mut value {
            Value::Object(map) => {
                for (k, v) in patch {
                    map.insert(k.clone(), v.clone());
                }
            }
            other => *other = Value::Object(patch.clone()),
        }
        st.context = C::deserialize(&value).map_err(|e| {
            st.status = Status::Error;
            RuntimeError::Context { message: e.to_string() }
        })?;
        Ok(())
    }

    fn run_actions(
        &self,
        actions: &[Action],
        st: &mut State<C, E>,
        ev: &Ev<E>,
        changed: &mut bool,
    ) -> Result<(), RuntimeError> {
        for action in actions {
            match action {
                Action::Raise { event, id, delay } => {
                    let event = self.raise_events[*event as usize].clone();
                    match delay {
                        None => st.queue.push_back(event),
                        Some(delay) => {
                            let key = match id {
                                Some(id) => TimerKey::Id(id.clone()),
                                None => {
                                    let seq = st.next_timer_id;
                                    st.next_timer_id += 1;
                                    TimerKey::Seq(seq)
                                }
                            };
                            schedule(st, key, *delay, event);
                            *changed = true;
                        }
                    }
                }
                Action::Cancel(id) => {
                    let before = st.timers.len();
                    st.timers.retain(|t| !matches!(&t.key, TimerKey::Id(x) if x == id));
                    if st.timers.len() != before {
                        *changed = true;
                    }
                }
                Action::Log(args) => st.logs.push(args.clone()),
                Action::Emit(event) => st.emitted.push(event.clone()),
                Action::Assign(patch) => {
                    self.assign(patch, st)?;
                    *changed = true;
                }
                Action::Custom(i) => {
                    let mut written = false;
                    let mut args = ActionArgs {
                        event: ev,
                        context: &mut st.context,
                        context_written: &mut written,
                        emitted: &mut st.emitted,
                        raised: &mut st.queue,
                        machine: self,
                        active: &st.active,
                    };
                    (self.actions[*i as usize])(&mut args);
                    if written {
                        *changed = true;
                    }
                }
            }
        }
        Ok(())
    }

    fn schedule_after_timers(&self, node: NodeId, st: &mut State<C, E>, ev: &Ev<E>, changed: &mut bool) {
        let n = self.node(node);
        for (index, def) in n.after.iter().enumerate() {
            let delay = match def.delay {
                DelaySpec::Fixed(ms) => ms,
                DelaySpec::Named(i) => (self.delays[i as usize])(&st.context, ev),
            };
            let mut data = Map::new();
            data.insert("delay".into(), def.event_delay.clone());
            data.insert("stateId".into(), Value::String(n.id.clone()));
            let event = Ev::Internal { event_type: Cow::Borrowed(AFTER), data };
            schedule(st, TimerKey::After { node, index: index as u32 }, delay, event);
            *changed = true;
        }
    }

    fn cancel_after_timers(&self, node: NodeId, st: &mut State<C, E>, changed: &mut bool) {
        if self.node(node).after.is_empty() {
            return;
        }
        let before = st.timers.len();
        st.timers.retain(|t| !matches!(t.key, TimerKey::After { node: n, .. } if n == node));
        if st.timers.len() != before {
            *changed = true;
        }
    }

    // ---- macrostep ---------------------------------------------------------

    /// Enters the initial configuration and settles.
    pub(crate) fn start(&self, st: &mut State<C, E>) -> Result<(), RuntimeError> {
        let mut to_enter = vec![false; self.nodes.len()];
        to_enter[ROOT as usize] = true;
        self.add_descendants(ROOT, st, &mut to_enter);
        let mut changed = false;
        let init = Ev::Init;
        self.enter_states(&to_enter, st, &init, &mut changed)?;
        if st.status == Status::Done {
            self.exit_all(st, &init, &mut changed)?;
        }
        self.settle(st, init)
    }

    /// Processes one external event. Returns whether a transition handled it.
    pub(crate) fn process_external(&self, st: &mut State<C, E>, ev: Ev<E>) -> Result<bool, RuntimeError> {
        if st.status != Status::Active {
            return Ok(false);
        }
        let transitions = self.select_transitions(st, &ev);
        let handled = !transitions.is_empty();
        self.microstep(&transitions, st, &ev)?;
        self.settle(st, ev)?;
        Ok(handled)
    }

    /// Processes a fired timer's event as an internal event.
    pub(crate) fn process_timer(&self, st: &mut State<C, E>, ev: Ev<E>) -> Result<(), RuntimeError> {
        if st.status != Status::Active {
            return Ok(());
        }
        st.queue.push_back(ev);
        self.settle(st, Ev::Internal { event_type: Cow::Borrowed("xstate.timer"), data: Map::new() })
    }

    /// Takes eventless transitions and internal events until the machine is
    /// stable, bounded by `max_iterations` microsteps.
    fn settle(&self, st: &mut State<C, E>, mut current: Ev<E>) -> Result<(), RuntimeError> {
        let mut select_eventless = true;
        let mut iterations = 0;
        while st.status == Status::Active {
            iterations += 1;
            if iterations > self.options.max_iterations {
                st.status = Status::Error;
                st.queue.clear();
                return Err(RuntimeError::InfiniteLoop {
                    event_type: current.event_type().to_owned(),
                    max_iterations: self.options.max_iterations,
                    state: self.state_value(&st.active),
                });
            }
            let mut enabled = if select_eventless { self.select_eventless(st, &current) } else { Vec::new() };
            let eventless = !enabled.is_empty();
            if !eventless {
                match st.queue.pop_front() {
                    None => break,
                    Some(next) => {
                        current = next;
                        enabled = self.select_transitions(st, &current);
                    }
                }
            }
            let changed = self.microstep(&enabled, st, &current)?;
            // After an eventless microstep that changed nothing, the same
            // eventless transitions would be selected again.
            select_eventless = !eventless || changed;
        }
        st.queue.clear();
        Ok(())
    }
}

impl<C, E> Machine<C, E> {
    // ---- state value -------------------------------------------------------

    /// XState's nested state value: a compound state with an atomic active
    /// child renders as the child's key, everything else as an object.
    pub(crate) fn state_value(&self, active: &[bool]) -> Value {
        self.node_value(ROOT, active)
    }

    fn node_value(&self, node: NodeId, active: &[bool]) -> Value {
        let n = self.node(node);
        match n.kind {
            NodeKind::Compound => match n.children.iter().copied().find(|&c| active[c as usize]) {
                None => Value::Object(Map::new()),
                Some(c) if self.node(c).is_atomic_like() => Value::String(self.node(c).key.clone()),
                Some(c) => {
                    let mut map = Map::new();
                    map.insert(self.node(c).key.clone(), self.node_value(c, active));
                    Value::Object(map)
                }
            },
            NodeKind::Parallel => {
                let mut map = Map::new();
                for &c in &n.children {
                    if active[c as usize] {
                        map.insert(self.node(c).key.clone(), self.node_value(c, active));
                    }
                }
                Value::Object(map)
            }
            _ => Value::Object(Map::new()),
        }
    }
}

/// Schedules a timer, replacing a pending timer with the same key.
fn schedule<E>(st: &mut State<impl Sized, E>, key: TimerKey, delay: u64, event: Ev<E>) {
    let order = st.timer_order;
    st.timer_order += 1;
    let timer = Timer { key, due: st.now.saturating_add(delay), order, delay, event };
    match st.timers.iter_mut().find(|t| t.key == timer.key) {
        Some(existing) => *existing = timer,
        None => st.timers.push(timer),
    }
}
