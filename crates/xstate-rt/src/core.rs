//! The structural statechart algorithm, shared by every host.
//!
//! This is a port of the structural half of XState v6's `stateUtils.ts`
//! (`transitionNode`, `selectEventlessTransitions`, `removeConflictingTransitions`,
//! `getTransitionDomain`, `computeExitSet`, `microstep`, `initialMicrostep` and
//! the loop in `macrostep`). Everything that touches values (guards,
//! transition functions, entry and exit functions, effects, snapshots) is
//! delegated to a [`Host`], called in exactly the order XState performs the
//! same work. The native interpreter is one host; the patched XState package
//! used to run XState's own test suite (through `xstate-wasm`) is another.
//!
//! The configuration is an insertion-ordered set, like XState's `nodes`
//! array: state values, parallel region order and selection order all follow
//! it, and it can differ from document order after a region is re-entered.

/// A state node, by document order (XState's `StateNode.order`).
pub type Nid = u32;
/// A transition definition registered with the tree.
pub type Tid = u32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    Atomic,
    Compound,
    Parallel,
    Final,
    History {
        deep: bool,
    },
    /// Choice states are atomic for the algorithm; their branches are `always` transitions.
    Choice,
}

impl Kind {
    /// XState's `isAtomicStateNode`.
    pub fn is_atomic(self) -> bool {
        matches!(self, Kind::Atomic | Kind::Final | Kind::Choice)
    }
}

/// XState's internal `_transitionDomain` override.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DomainHint {
    #[default]
    None,
    Internal,
    External,
}

#[derive(Debug, Clone)]
pub struct TreeNode {
    pub parent: Option<Nid>,
    pub kind: Kind,
    /// Child states except history states, in `states` order.
    pub children: Vec<Nid>,
    /// History states declared in `states`, in order.
    pub history_children: Vec<Nid>,
    /// The node's transition map in insertion order: descriptor and the
    /// transitions registered under it.
    pub on: Vec<(String, Vec<Tid>)>,
    pub always: Vec<Tid>,
    /// One past the last descendant.
    end: Nid,
}

impl TreeNode {
    pub fn new(parent: Option<Nid>, kind: Kind) -> Self {
        Self {
            parent,
            kind,
            children: Vec::new(),
            history_children: Vec::new(),
            on: Vec::new(),
            always: Vec::new(),
            end: 0,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TreeTransition {
    pub source: Nid,
    pub domain: DomainHint,
}

/// The structure of a machine: nodes in document order and the source of
/// every transition.
#[derive(Debug, Clone)]
pub struct Tree {
    pub nodes: Vec<TreeNode>,
    pub transitions: Vec<TreeTransition>,
    pub has_eventless: bool,
}

impl Tree {
    /// Finishes a tree whose nodes are in pre-order with parents set.
    /// Fills in `end` and `has_eventless`.
    pub fn new(mut nodes: Vec<TreeNode>, transitions: Vec<TreeTransition>) -> Self {
        let n = nodes.len() as Nid;
        for i in 0..n {
            nodes[i as usize].end = i + 1;
        }
        for i in (1..n).rev() {
            if let Some(p) = nodes[i as usize].parent {
                let end = nodes[i as usize].end;
                if end > nodes[p as usize].end {
                    nodes[p as usize].end = end;
                }
            }
        }
        let has_eventless = nodes.iter().any(|n| !n.always.is_empty());
        Self { nodes, transitions, has_eventless }
    }

    #[inline]
    pub fn node(&self, n: Nid) -> &TreeNode {
        &self.nodes[n as usize]
    }

    #[inline]
    pub fn kind(&self, n: Nid) -> Kind {
        self.nodes[n as usize].kind
    }

    #[inline]
    fn is_history(&self, n: Nid) -> bool {
        matches!(self.kind(n), Kind::History { .. })
    }

    /// XState's `isDescendant(child, parent)`: strict; `None` (no domain)
    /// contains every node, the root included.
    #[inline]
    pub fn is_descendant(&self, node: Nid, ancestor: Option<Nid>) -> bool {
        match ancestor {
            None => true,
            Some(a) => node > a && node < self.node(a).end,
        }
    }

    /// XState's `getProperAncestors(node, to)`.
    pub fn proper_ancestors(&self, node: Nid, to: Option<Nid>) -> Vec<Nid> {
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

    /// XState's `getCandidates`: exact descriptor first, then the other
    /// matching descriptors, longest first.
    pub fn candidates(&self, node: Nid, event_type: &str) -> Vec<Tid> {
        let on = &self.node(node).on;
        let mut out: Vec<Tid> = on.iter().find(|(d, _)| d == event_type).map(|(_, ts)| ts.clone()).unwrap_or_default();
        let mut wild: Vec<&(String, Vec<Tid>)> =
            on.iter().filter(|(d, _)| d != event_type && matches_event_descriptor(event_type, d)).collect();
        wild.sort_by_key(|(d, _)| std::cmp::Reverse(d.len()));
        for (_, ts) in wild {
            out.extend_from_slice(ts);
        }
        out
    }

    /// XState's `isInFinalState`, over a configuration membership test.
    pub fn is_in_final_state(&self, set: &OrderedSet, node: Nid) -> bool {
        let n = self.node(node);
        match n.kind {
            Kind::Compound => n.children.iter().any(|&c| self.kind(c) == Kind::Final && set.contains(c)),
            Kind::Parallel => n.children.iter().all(|&c| self.is_in_final_state(set, c)),
            kind => kind == Kind::Final,
        }
    }

    /// XState's `getStateValue` structure: for each active node, its active
    /// children in configuration order. Returns, per node, the ordered list.
    pub fn active_children(&self, config: &[Nid]) -> Vec<Vec<Nid>> {
        let mut adj = vec![Vec::new(); self.nodes.len()];
        for &s in config {
            if let Some(p) = self.node(s).parent {
                adj[p as usize].push(s);
            }
        }
        adj
    }
}

/// XState's `matchesEventDescriptor`.
pub fn matches_event_descriptor(event_type: &str, descriptor: &str) -> bool {
    if descriptor == event_type || descriptor == "*" {
        return true;
    }
    if !descriptor.ends_with(".*") {
        return false;
    }
    let mut event_tokens = event_type.split('.');
    let mut partial_tokens = descriptor.split('.').peekable();
    while let Some(partial) = partial_tokens.next() {
        let event_token = event_tokens.next();
        if partial == "*" {
            return partial_tokens.peek().is_none();
        }
        if Some(partial) != event_token {
            return false;
        }
    }
    true
}

/// An insertion-ordered set of nodes with O(1) membership, like a JS `Set`.
#[derive(Debug, Clone)]
pub struct OrderedSet {
    order: Vec<Nid>,
    member: Vec<bool>,
}

impl OrderedSet {
    pub fn new(size: usize) -> Self {
        Self { order: Vec::new(), member: vec![false; size] }
    }

    pub fn from_slice(size: usize, items: &[Nid]) -> Self {
        let mut s = Self::new(size);
        for &i in items {
            s.insert(i);
        }
        s
    }

    #[inline]
    pub fn contains(&self, n: Nid) -> bool {
        self.member[n as usize]
    }

    pub fn insert(&mut self, n: Nid) -> bool {
        if self.member[n as usize] {
            return false;
        }
        self.member[n as usize] = true;
        self.order.push(n);
        true
    }

    pub fn remove(&mut self, n: Nid) -> bool {
        if !self.member[n as usize] {
            return false;
        }
        self.member[n as usize] = false;
        if let Some(i) = self.order.iter().position(|&x| x == n) {
            self.order.remove(i);
        }
        true
    }

    pub fn as_slice(&self) -> &[Nid] {
        &self.order
    }

    pub fn len(&self) -> usize {
        self.order.len()
    }

    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    fn intersects(&self, other: &OrderedSet) -> bool {
        let (small, big) = if self.len() <= other.len() { (self, other) } else { (other, self) };
        small.order.iter().any(|&n| big.contains(n))
    }
}

/// A transition as the algorithm sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TRef {
    /// A registered transition definition.
    T(Tid),
    /// The `initial` transition of a compound state.
    Initial(Nid),
    /// The default transition of a history state (`resolveHistoryDefaultTransition`).
    HistoryDefault(Nid),
    /// The synthetic transition of the initial microstep.
    InitialMicrostep,
}

/// Which of XState's transition-result resolvers a lookup goes through.
/// Each has its own cache in XState; hosts must keep them apart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Resolver {
    /// `selectEventlessTransitions`' conflict resolution
    /// (`resolveActions: false`, no selection results).
    EventlessSelection,
    /// The microstep's conflict resolution (`resolveActions: false`).
    Conflicts,
    /// The microstep's `getCurrentTransitionResult` (`resolveActions: true`).
    Current,
}

/// The structural part of a transition result.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Resolved {
    pub targets: Option<Vec<Nid>>,
    pub reenter: bool,
}

/// How a region contributes to a parallel state's done output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegionOutput {
    /// The region is itself a final state.
    Final(Nid),
    /// The region is parallel: its output is its queued done event's output.
    Parallel(Nid),
    /// A compound region and its active final child, if any.
    Compound(Nid, Option<Nid>),
}

/// The value-level half of the algorithm. Every method corresponds to a
/// point in XState's `stateUtils.ts` where it reads or writes values.
pub trait Host {
    type Error;

    /// The current configuration (`snapshot.nodes`), in order.
    fn config(&self) -> Vec<Nid>;
    /// The recorded history of a history state in the current snapshot.
    fn history(&self, history: Nid) -> Option<Vec<Nid>>;
    /// The static targets of a compound state's `initial` (`initial.target`).
    fn static_initial(&self, node: Nid) -> Vec<Nid>;

    /// `machine._microstepHooks?.begin(self)`, at the start of selection.
    fn begin_selection(&mut self) -> Result<(), Self::Error>;
    /// `evaluateCandidate`: `matches`, guard, and transition function selection.
    /// `eventless` tells whether the candidate is an `always` transition.
    fn eval_candidate(&mut self, t: Tid, node: Nid, eventless: bool) -> Result<bool, Self::Error>;
    /// A transition result through one of the resolvers.
    fn resolve(&mut self, resolver: Resolver, t: TRef) -> Result<Resolved, Self::Error>;

    /// Start of a microstep (`_microstepHooks?.drain`), before anything else.
    fn begin_microstep(&mut self) -> Result<(), Self::Error>;
    /// Exits one state: exit function, its actions, stopping its invokes.
    fn exit_state(&mut self, node: Nid) -> Result<(), Self::Error>;
    /// Executes the content of the filtered transitions, in order.
    fn transition_content(&mut self, filtered: &[TRef]) -> Result<(), Self::Error>;
    /// Before entering: the configuration after exits, and the targets each
    /// transition enters explicitly (for state inputs).
    fn begin_entry(&mut self, active: &[Nid], entered: &[(TRef, Vec<Nid>)]) -> Result<(), Self::Error>;
    /// Enters one state: invokes, entry function, and, for default entry, its
    /// `initial` transition and the default transitions of `history_defaults`.
    fn enter_state(&mut self, node: Nid, default_entry: bool, history_defaults: &[Nid]) -> Result<(), Self::Error>;
    /// A final child was entered: queue `xstate.done.state` for its compound parent.
    fn done_compound(&mut self, parent: Nid, final_child: Nid) -> Result<(), Self::Error>;
    /// A parallel state completed: queue its done event with the region outputs.
    fn done_parallel(&mut self, parallel: Nid, regions: &[RegionOutput]) -> Result<(), Self::Error>;
    /// The machine completed through `completion`.
    fn machine_done(&mut self, completion: Nid) -> Result<(), Self::Error>;
    /// After all states were entered.
    fn end_entry(&mut self) -> Result<(), Self::Error>;
    /// Whether the machine completed in this microstep.
    fn is_done(&self) -> bool;
    /// The machine completed: exit every active state (deepest-last order
    /// reversed: descending document order), stop children, cancel timers.
    fn exit_on_done(&mut self, nodes_desc: &[Nid]) -> Result<(), Self::Error>;
    /// Applies the microstep's structural result to the snapshot. `None`
    /// means no transition was taken.
    fn apply_microstep(&mut self, result: Option<MicrostepResult>, enabled: &[TRef]) -> Result<(), Self::Error>;

    // The macrostep loop.

    /// Whether the snapshot is active.
    fn is_active(&self) -> bool;
    /// Called at the top of each loop iteration with the iteration count;
    /// fails when the microstep bound is exceeded.
    fn check_iteration(&mut self, iteration: usize) -> Result<(), Self::Error>;
    /// An identity for the current snapshot object.
    fn snapshot_token(&self) -> u64;
    /// The identity of the snapshot the last microstep returned (before any
    /// post-microstep bookkeeping, such as removing a terminated child).
    fn microstep_token(&self) -> u64;
    /// `_microstepHooks?.drain`: raised events from failed eventless guards.
    fn drain_raised(&mut self) -> Result<(), Self::Error>;
    fn queue_is_empty(&self) -> bool;
    /// Takes the next internal event, making it the current event. Returns
    /// its type, or `None` when the queue is empty.
    fn dequeue(&mut self) -> Result<Option<String>, Self::Error>;
}

/// The structural outcome of a microstep.
#[derive(Debug, Clone)]
pub struct MicrostepResult {
    /// The transitions taken, after conflict removal.
    pub filtered: Vec<TRef>,
    /// The new configuration. Equal to the old one, order included, unless
    /// `structure_changed`.
    pub nodes: Vec<Nid>,
    /// Whether membership or history changed (XState then builds a new
    /// node list and history value).
    pub structure_changed: bool,
    /// History values recorded by this microstep, in recording order.
    pub history_updates: Vec<(Nid, Vec<Nid>)>,
}

/// The synthetic transition of the initial microstep.
#[derive(Debug, Clone)]
pub struct InitialPlan {
    pub targets: Vec<Nid>,
    pub default_entry: Vec<Nid>,
}

// ---- selection ----------------------------------------------------------------

/// `StateNode.next`: the first enabled candidate of one node.
fn node_next<H: Host>(tree: &Tree, h: &mut H, node: Nid, event_type: &str) -> Result<Option<Tid>, H::Error> {
    if tree.kind(node) == Kind::Final {
        return Ok(None);
    }
    for t in tree.candidates(node, event_type) {
        if h.eval_candidate(t, node, false)? {
            return Ok(Some(t));
        }
    }
    Ok(None)
}

/// XState's `transitionNode`, over the configuration's value structure.
fn transition_node<H: Host>(
    tree: &Tree,
    h: &mut H,
    adj: &[Vec<Nid>],
    node: Nid,
    event_type: &str,
    out: &mut Vec<Tid>,
) -> Result<(), H::Error> {
    let children = &adj[node as usize];
    // A compound state whose active child is atomic has a string value.
    let leaf_child = tree.kind(node) == Kind::Compound && children.first().is_some_and(|&c| tree.kind(c).is_atomic());
    if leaf_child {
        let child = children[0];
        if let Some(t) = node_next(tree, h, child, event_type)? {
            out.push(t);
            return Ok(());
        }
        if let Some(t) = node_next(tree, h, node, event_type)? {
            out.push(t);
        }
        return Ok(());
    }
    // Object value: one entry per active child (all of them, for parallel
    // states; for a compound state only the first appears in the value).
    let keys: &[Nid] = if tree.kind(node) == Kind::Compound { &children[..children.len().min(1)] } else { children };
    let before = out.len();
    for &child in keys {
        transition_node(tree, h, adj, child, event_type, out)?;
    }
    if out.len() == before {
        if let Some(t) = node_next(tree, h, node, event_type)? {
            out.push(t);
        }
    }
    Ok(())
}

/// `machine.getTransitionData`: the transitions an event enables.
pub fn select_transitions<H: Host>(tree: &Tree, h: &mut H, event_type: &str) -> Result<Vec<Tid>, H::Error> {
    h.begin_selection()?;
    let config = h.config();
    let adj = tree.active_children(&config);
    let mut out = Vec::new();
    if !config.is_empty() {
        transition_node(tree, h, &adj, 0, event_type, &mut out)?;
    }
    Ok(out)
}

/// `selectEventlessTransitions`.
pub fn select_eventless<H: Host>(tree: &Tree, h: &mut H) -> Result<Vec<TRef>, H::Error> {
    h.begin_selection()?;
    let config = h.config();
    let mut enabled: Vec<TRef> = Vec::new();
    for &atomic in config.iter().filter(|&&n| tree.kind(n).is_atomic()) {
        let mut m = Some(atomic);
        'ancestors: while let Some(node) = m {
            for &t in &tree.node(node).always {
                if h.eval_candidate(t, node, true)? {
                    if !enabled.contains(&TRef::T(t)) {
                        enabled.push(TRef::T(t));
                    }
                    break 'ancestors;
                }
            }
            m = tree.node(node).parent;
        }
    }
    let set = OrderedSet::from_slice(tree.nodes.len(), &config);
    let mut ctx = Ctx { tree, h, resolver: Resolver::EventlessSelection, initial: None };
    ctx.remove_conflicting(&enabled, &set)
}

// ---- domains and sets -------------------------------------------------------------

struct Ctx<'a, H> {
    tree: &'a Tree,
    h: &'a mut H,
    resolver: Resolver,
    initial: Option<&'a InitialPlan>,
}

impl<H: Host> Ctx<'_, H> {
    fn source(&self, t: TRef) -> Nid {
        match t {
            TRef::T(t) => self.tree.transitions[t as usize].source,
            TRef::Initial(n) | TRef::HistoryDefault(n) => n,
            TRef::InitialMicrostep => 0,
        }
    }

    fn domain_hint(&self, t: TRef) -> DomainHint {
        match t {
            TRef::T(t) => self.tree.transitions[t as usize].domain,
            _ => DomainHint::None,
        }
    }

    fn resolve_with(&mut self, resolver: Resolver, t: TRef) -> Result<Resolved, H::Error> {
        if let (TRef::InitialMicrostep, Some(plan)) = (t, self.initial) {
            return Ok(Resolved { targets: Some(plan.targets.clone()), reenter: true });
        }
        self.h.resolve(resolver, t)
    }

    fn resolve(&mut self, t: TRef) -> Result<Resolved, H::Error> {
        self.resolve_with(self.resolver, t)
    }

    /// `getEffectiveTargetStates`, against the snapshot's (old) history.
    fn effective_targets(&mut self, t: TRef) -> Result<Vec<Nid>, H::Error> {
        let Some(targets) = self.resolve(t)?.targets else { return Ok(Vec::new()) };
        let mut set: Vec<Nid> = Vec::new();
        let add = |set: &mut Vec<Nid>, n: Nid| {
            if !set.contains(&n) {
                set.push(n);
            }
        };
        for target in targets {
            if self.tree.is_history(target) {
                match self.h.history(target) {
                    Some(recorded) => recorded.into_iter().for_each(|n| add(&mut set, n)),
                    None => {
                        for n in self.effective_targets(TRef::HistoryDefault(target))? {
                            add(&mut set, n);
                        }
                    }
                }
            } else {
                add(&mut set, target);
            }
        }
        Ok(set)
    }

    fn narrow_parallel(&self, mut domain: Nid, targets: &[Nid]) -> Nid {
        let tree = self.tree;
        while tree.kind(domain) == Kind::Parallel {
            let region = tree
                .node(domain)
                .children
                .iter()
                .copied()
                .find(|&child| targets.iter().all(|&t| t == child || tree.is_descendant(t, Some(child))));
            match region {
                Some(r) => domain = r,
                None => break,
            }
        }
        domain
    }

    /// `getTransitionDomain`. `None` is "no domain": the whole machine.
    fn domain(&mut self, t: TRef) -> Result<Option<Nid>, H::Error> {
        let tree = self.tree;
        let effective = self.effective_targets(t)?;
        let Resolved { targets, reenter } = self.resolve(t)?;
        let source = self.source(t);

        let hint = self.domain_hint(t);
        if hint != DomainHint::None {
            if hint == DomainHint::Internal
                && tree.kind(source) == Kind::Compound
                && effective.iter().all(|&x| tree.is_descendant(x, Some(source)))
            {
                return Ok(Some(source));
            }
            let mut all = effective.clone();
            all.push(source);
            let (head, tail) = (all[0], &all[1..]);
            for ancestor in tree.proper_ancestors(head, None) {
                if tree.kind(ancestor) == Kind::Compound && tail.iter().all(|&x| tree.is_descendant(x, Some(ancestor)))
                {
                    return Ok(Some(ancestor));
                }
            }
            return Ok(None);
        }

        let restores_source_via_history =
            targets.as_ref().is_some_and(|ts| ts.iter().any(|&x| tree.is_history(x))) && effective.contains(&source);
        if !restores_source_via_history && effective.iter().all(|&x| x == source || tree.is_descendant(x, Some(source)))
        {
            return Ok(Some(if reenter { source } else { self.narrow_parallel(source, &effective) }));
        }
        let mut all = effective.clone();
        all.push(source);
        let (head, tail) = (all[0], &all[1..]);
        for ancestor in tree.proper_ancestors(head, None) {
            if tail.iter().all(|&x| tree.is_descendant(x, Some(ancestor))) {
                return Ok(Some(if reenter { ancestor } else { self.narrow_parallel(ancestor, &effective) }));
            }
        }
        if reenter {
            return Ok(None);
        }
        Ok(Some(self.narrow_parallel(0, &effective)))
    }

    /// `computeExitSet`.
    fn exit_set(&mut self, transitions: &[TRef], config: &OrderedSet) -> Result<OrderedSet, H::Error> {
        let mut out = OrderedSet::new(self.tree.nodes.len());
        for &t in transitions {
            let Resolved { targets, reenter } = self.resolve(t)?;
            if targets.is_some_and(|ts| !ts.is_empty()) {
                let domain = self.domain(t)?;
                let source = self.source(t);
                if reenter && Some(source) == domain {
                    out.insert(source);
                }
                for &s in config.as_slice() {
                    if self.tree.is_descendant(s, domain) {
                        out.insert(s);
                    }
                }
            }
        }
        Ok(out)
    }

    /// `removeConflictingTransitions`.
    fn remove_conflicting(&mut self, enabled: &[TRef], config: &OrderedSet) -> Result<Vec<TRef>, H::Error> {
        let mut filtered: Vec<TRef> = Vec::new();
        let mut exit_sets: Vec<(TRef, OrderedSet)> = Vec::new();
        for &t1 in enabled {
            let mut preempted = false;
            let mut remove: Vec<TRef> = Vec::new();
            let e1 = self.cached_exit_set(&mut exit_sets, t1, config)?;
            for &t2 in &filtered {
                let e2 = self.cached_exit_set(&mut exit_sets, t2, config)?;
                if exit_sets[e1].1.intersects(&exit_sets[e2].1) {
                    if self.tree.is_descendant(self.source(t1), Some(self.source(t2))) {
                        remove.push(t2);
                    } else {
                        preempted = true;
                        break;
                    }
                }
            }
            if !preempted {
                filtered.retain(|t| !remove.contains(t));
                filtered.push(t1);
            }
        }
        Ok(filtered)
    }

    fn cached_exit_set(
        &mut self,
        cache: &mut Vec<(TRef, OrderedSet)>,
        t: TRef,
        config: &OrderedSet,
    ) -> Result<usize, H::Error> {
        if let Some(i) = cache.iter().position(|(x, _)| *x == t) {
            return Ok(i);
        }
        let set = self.exit_set(&[t], config)?;
        cache.push((t, set));
        Ok(cache.len() - 1)
    }
}

// ---- microstep ----------------------------------------------------------------------

/// Entry-set state, as the closures in XState's `enterStates`.
struct Entry {
    to_enter: OrderedSet,
    default_entry: Vec<bool>,
    history_defaults: Vec<Vec<Nid>>,
}

impl<H: Host> Ctx<'_, H> {
    fn has_descendant_state(&self, set: &OrderedSet, parent: Nid) -> bool {
        set.as_slice().iter().any(|&s| self.tree.is_descendant(s, Some(parent)))
    }

    fn add_ancestors(
        &mut self,
        e: &mut Entry,
        history: &HistoryView,
        ancestors: &[Nid],
        reentrancy_domain: Option<Nid>,
    ) -> Result<(), H::Error> {
        for &anc in ancestors {
            if reentrancy_domain.is_none() || self.tree.is_descendant(anc, reentrancy_domain) {
                e.to_enter.insert(anc);
            }
            if self.tree.kind(anc) == Kind::Parallel {
                for child in self.tree.node(anc).children.clone() {
                    if !self.has_descendant_state(&e.to_enter, child) {
                        e.to_enter.insert(child);
                        self.add_descendants(e, history, child)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn add_descendants(&mut self, e: &mut Entry, history: &HistoryView, node: Nid) -> Result<(), H::Error> {
        let tree = self.tree;
        match tree.kind(node) {
            Kind::History { .. } => {
                let parent = tree.node(node).parent.expect("history states have a parent");
                if let Some(recorded) = history.get(self.h, node) {
                    for &s in &recorded {
                        e.to_enter.insert(s);
                        self.add_descendants(e, history, s)?;
                    }
                    for &s in &recorded {
                        let ancestors = tree.proper_ancestors(s, Some(parent));
                        self.add_ancestors(e, history, &ancestors, None)?;
                    }
                } else {
                    e.default_entry[parent as usize] = true;
                    e.history_defaults[parent as usize].push(node);
                    let targets = self.resolve(TRef::HistoryDefault(node))?.targets.unwrap_or_default();
                    for &s in &targets {
                        e.to_enter.insert(s);
                        // `historyDefaultTransition === stateNode.parent.initial`
                        // only adds `parent` again, which is already marked.
                        self.add_descendants(e, history, s)?;
                    }
                    for &s in &targets {
                        let ancestors = tree.proper_ancestors(s, Some(parent));
                        self.add_ancestors(e, history, &ancestors, None)?;
                    }
                }
            }
            Kind::Compound => {
                let initial = self.resolve(TRef::Initial(node))?.targets.unwrap_or_default();
                for &s in &initial {
                    if !tree.is_history(s) {
                        e.to_enter.insert(s);
                    }
                }
                e.default_entry[node as usize] = true;
                for &s in &initial {
                    self.add_descendants(e, history, s)?;
                }
                for &s in &initial {
                    let ancestors = tree.proper_ancestors(s, Some(node));
                    self.add_ancestors(e, history, &ancestors, None)?;
                }
            }
            Kind::Parallel => {
                for child in tree.node(node).children.clone() {
                    if !self.has_descendant_state(&e.to_enter, child) {
                        e.to_enter.insert(child);
                        e.default_entry[child as usize] = true;
                        self.add_descendants(e, history, child)?;
                    }
                }
            }
            Kind::Atomic | Kind::Final | Kind::Choice => {}
        }
        Ok(())
    }
}

/// The history visible to entry: the snapshot's, overlaid with what this
/// microstep recorded.
struct HistoryView {
    updates: Option<Vec<(Nid, Vec<Nid>)>>,
}

impl HistoryView {
    fn get<H: Host>(&self, h: &H, node: Nid) -> Option<Vec<Nid>> {
        if let Some(updates) = &self.updates {
            if let Some((_, v)) = updates.iter().find(|(n, _)| *n == node) {
                return Some(v.clone());
            }
        }
        h.history(node)
    }
}

/// XState's `getInitialStateNodes(root)` and `collectDefaultEntryStates`,
/// from the static `initial.target` of each compound state.
pub fn initial_plan<H: Host>(tree: &Tree, h: &H) -> InitialPlan {
    let size = tree.nodes.len();
    let mut set = OrderedSet::new(size);
    fn iter<H: Host>(tree: &Tree, h: &H, set: &mut OrderedSet, n: Nid) {
        if !set.insert(n) {
            return;
        }
        match tree.kind(n) {
            Kind::Compound => {
                for t in h.static_initial(n) {
                    iter(tree, h, set, t);
                }
            }
            Kind::Parallel => {
                for &c in &tree.node(n).children {
                    iter(tree, h, set, c);
                }
            }
            _ => {}
        }
    }
    iter(tree, h, &mut set, 0);
    for s in set.as_slice().to_vec() {
        for a in tree.proper_ancestors(s, Some(0)) {
            set.insert(a);
        }
    }
    let nodes = set.as_slice();
    let targets: Vec<Nid> =
        nodes.iter().copied().filter(|&s| !nodes.iter().any(|&o| o != s && tree.is_descendant(o, Some(s)))).collect();

    let mut default_entry = Vec::new();
    fn collect<H: Host>(tree: &Tree, h: &H, out: &mut Vec<Nid>, n: Nid) {
        match tree.kind(n) {
            Kind::Compound => {
                if !out.contains(&n) {
                    out.push(n);
                }
                for t in h.static_initial(n) {
                    collect(tree, h, out, t);
                }
            }
            Kind::Parallel => {
                for &c in &tree.node(n).children {
                    collect(tree, h, out, c);
                }
            }
            _ => {}
        }
    }
    collect(tree, h, &mut default_entry, 0);
    InitialPlan { targets, default_entry }
}

/// XState's `microstep`. `initial` is set for the initial microstep, whose
/// only transition is [`TRef::InitialMicrostep`].
pub fn microstep<H: Host>(
    tree: &Tree,
    h: &mut H,
    transitions: &[TRef],
    initial: Option<&InitialPlan>,
) -> Result<(), H::Error> {
    h.begin_microstep()?;
    if transitions.is_empty() {
        return h.apply_microstep(None, transitions);
    }
    let is_initial = initial.is_some();
    let size = tree.nodes.len();
    let original = h.config();
    let mut set = OrderedSet::from_slice(size, &original);

    let filtered = if transitions.len() == 1 {
        transitions.to_vec()
    } else {
        let mut ctx = Ctx { tree, h: &mut *h, resolver: Resolver::Conflicts, initial };
        ctx.remove_conflicting(transitions, &set)?
    };

    let mut ctx = Ctx { tree, h: &mut *h, resolver: Resolver::Current, initial };
    let mut changes_state = false;
    for &t in &filtered {
        let r = ctx.resolve(t)?;
        if r.targets.is_some_and(|ts| !ts.is_empty()) || r.reenter {
            changes_state = true;
        }
    }

    // Exit states.
    let mut history_updates: Option<Vec<(Nid, Vec<Nid>)>> = None;
    if !is_initial && changes_state {
        let exit = ctx.exit_set(&filtered, &set)?;
        let mut to_exit = exit.as_slice().to_vec();
        to_exit.sort_by(|a, b| b.cmp(a));
        let current: Vec<Nid> = set.as_slice().to_vec();
        for &s in &to_exit {
            for &hn in &tree.node(s).history_children {
                let deep = matches!(tree.kind(hn), Kind::History { deep: true });
                let recorded: Vec<Nid> = current
                    .iter()
                    .copied()
                    .filter(|&n| {
                        if deep {
                            tree.kind(n).is_atomic() && tree.is_descendant(n, Some(s))
                        } else {
                            tree.node(n).parent == Some(s)
                        }
                    })
                    .collect();
                let updates = history_updates.get_or_insert_with(Vec::new);
                match updates.iter_mut().find(|(n, _)| *n == hn) {
                    Some(entry) => entry.1 = recorded,
                    None => updates.push((hn, recorded)),
                }
            }
        }
        for &s in &to_exit {
            ctx.h.exit_state(s)?;
            set.remove(s);
        }
    }

    // Transition content.
    ctx.h.transition_content(&filtered)?;

    // Enter states.
    if is_initial || changes_state {
        let history = HistoryView { updates: history_updates.clone() };
        let mut e = Entry {
            to_enter: OrderedSet::new(size),
            default_entry: vec![false; size],
            history_defaults: vec![Vec::new(); size],
        };
        let mut entered: Vec<(TRef, Vec<Nid>)> = Vec::new();
        for &t in &filtered {
            let domain = ctx.domain(t)?;
            let Resolved { targets, reenter } = ctx.resolve(t)?;
            let source = ctx.source(t);
            let mut entered_targets = Vec::new();
            for &target in targets.as_deref().unwrap_or(&[]) {
                if !tree.is_history(target) && (source != target || Some(source) != domain || reenter) {
                    if !entered_targets.contains(&target) {
                        entered_targets.push(target);
                    }
                    e.to_enter.insert(target);
                    e.default_entry[target as usize] = true;
                }
                ctx.add_descendants(&mut e, &history, target)?;
            }
            entered.push((t, entered_targets));
            for s in ctx.effective_targets(t)? {
                let mut ancestors = tree.proper_ancestors(s, domain);
                if let Some(d) = domain {
                    if tree.kind(d) == Kind::Parallel {
                        ancestors.push(d);
                    }
                }
                let reentrancy = if tree.node(source).parent.is_none() && reenter { None } else { domain };
                ctx.add_ancestors(&mut e, &history, &ancestors, reentrancy)?;
            }
            if reenter && domain == Some(source) {
                e.to_enter.insert(source);
            }
        }
        if let Some(plan) = initial {
            for &n in &plan.default_entry {
                e.default_entry[n as usize] = true;
            }
        }

        ctx.h.begin_entry(set.as_slice(), &entered)?;
        let mut order = e.to_enter.as_slice().to_vec();
        order.sort_unstable();
        let mut completed = vec![false; size];
        for s in order {
            set.insert(s);
            let defaults = std::mem::take(&mut e.history_defaults[s as usize]);
            ctx.h.enter_state(s, e.default_entry[s as usize], &defaults)?;
            if tree.kind(s) != Kind::Final {
                continue;
            }
            let parent = tree.node(s).parent;
            let mut marker = match parent {
                Some(p) if tree.kind(p) == Kind::Parallel => Some(p),
                Some(p) => tree.node(p).parent,
                None => None,
            };
            let mut completion = marker.unwrap_or(s);
            if let Some(p) = parent {
                if tree.kind(p) == Kind::Compound {
                    ctx.h.done_compound(p, s)?;
                }
            }
            while let Some(m) = marker {
                if tree.kind(m) != Kind::Parallel || completed[m as usize] || !tree.is_in_final_state(&set, m) {
                    break;
                }
                completed[m as usize] = true;
                let regions: Vec<RegionOutput> = tree
                    .node(m)
                    .children
                    .iter()
                    .map(|&r| match tree.kind(r) {
                        Kind::Final => RegionOutput::Final(r),
                        Kind::Parallel => RegionOutput::Parallel(r),
                        _ => RegionOutput::Compound(
                            r,
                            tree.node(r)
                                .children
                                .iter()
                                .copied()
                                .find(|&c| tree.kind(c) == Kind::Final && set.contains(c)),
                        ),
                    })
                    .collect();
                ctx.h.done_parallel(m, &regions)?;
                completion = m;
                marker = tree.node(m).parent;
            }
            if marker.is_some() {
                continue;
            }
            ctx.h.machine_done(completion)?;
        }
        ctx.h.end_entry()?;
    }

    let mut nodes: Vec<Nid> = set.as_slice().to_vec();
    if ctx.h.is_done() {
        // XState sorts its node list in place here, so a completed machine's
        // configuration is in descending document order.
        nodes.sort_by(|a, b| b.cmp(a));
        ctx.h.exit_on_done(&nodes)?;
    }

    let same_membership = original.len() == set.len() && original.iter().all(|&n| set.contains(n));
    let structure_changed = history_updates.is_some() || !same_membership;
    let result = MicrostepResult {
        filtered: filtered.clone(),
        nodes: if structure_changed { nodes } else { original },
        structure_changed,
        history_updates: history_updates.unwrap_or_default(),
    };
    h.apply_microstep(Some(result), transitions)
}

/// XState's `initialMicrostep`.
pub fn initial_microstep<H: Host>(tree: &Tree, h: &mut H) -> Result<(), H::Error> {
    let plan = initial_plan(tree, h);
    microstep(tree, h, &[TRef::InitialMicrostep], Some(&plan))
}

/// The loop at the end of XState's `macrostep`: eventless transitions and
/// internal events until the machine is stable.
pub fn run_to_completion<H: Host>(tree: &Tree, h: &mut H) -> Result<(), H::Error> {
    if h.queue_is_empty() && !tree.has_eventless {
        return Ok(());
    }
    let mut should_select_eventless = true;
    let mut iteration = 0;
    while h.is_active() {
        iteration += 1;
        h.check_iteration(iteration)?;
        let mut enabled = if should_select_eventless { select_eventless(tree, h)? } else { Vec::new() };
        let previous = if enabled.is_empty() { None } else { Some(h.snapshot_token()) };
        h.drain_raised()?;
        if enabled.is_empty() {
            let Some(event_type) = h.dequeue()? else { break };
            enabled = select_transitions(tree, h, &event_type)?.into_iter().map(TRef::T).collect();
        }
        microstep(tree, h, &enabled, None)?;
        should_select_eventless = Some(h.microstep_token()) != previous;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptors() {
        assert!(matches_event_descriptor("a.b", "a.*"));
        assert!(matches_event_descriptor("a", "a.*"));
        assert!(!matches_event_descriptor("ab", "a.*"));
        assert!(!matches_event_descriptor("a.b.c", "a.*.c"));
        assert!(matches_event_descriptor("x", "*"));
    }

    #[test]
    fn ordered_set() {
        let mut s = OrderedSet::from_slice(5, &[0, 1, 2]);
        s.remove(1);
        s.insert(1);
        assert_eq!(s.as_slice(), &[0, 2, 1]);
        assert!(!s.insert(2));
    }
}
