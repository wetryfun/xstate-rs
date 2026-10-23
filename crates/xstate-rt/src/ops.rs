//! Operation counts, for the cost model (`examples/cost_model.rs`).
//!
//! With the `op-counts` feature, the runtime counts what it does, per
//! thread: macrosteps, microsteps, candidates evaluated, guards, states
//! exited and entered, context merges, and so on. Multiplied by the fitted
//! cost of each operation, the counts of a scenario predict its run time.
//! Without the feature, [`count`] compiles to nothing.

/// The operations the runtime counts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
pub enum Op {
    /// A call into the system: send, advance, resolve or start.
    SystemCall,
    /// A machine macrostep (an event, a fired timer, or the start).
    Macrostep,
    /// A microstep (one set of transitions taken).
    Microstep,
    /// An internal event processed (raised, done, after, ...).
    InternalEvent,
    /// A transition candidate evaluated (matches, guard, function).
    Candidate,
    /// A guard evaluated.
    Guard,
    /// A state exited.
    Exit,
    /// A state entered.
    Enter,
    /// An action run (built-in or custom).
    Action,
    /// A context merge (a patch, an assign).
    ContextMerge,
    /// An expression call.
    Expr,
    /// An emitted event, log or custom action effect.
    Effect,
    /// A logical timer scheduled.
    TimerSchedule,
    /// A logical timer cancelled.
    TimerCancel,
    /// A child actor allocated and started (invoke or spawn).
    ActorStart,
    /// A child actor stopped.
    ActorStop,
    /// A message delivered from one actor to another.
    Message,
    /// An external effect the system keeps pending (with an id), as the
    /// pure `System` API does; `Actor` acknowledges most effects at once.
    EffectStored,
    /// An active state walked by a selection (the configuration's size, per
    /// selection).
    ActiveState,
    /// A JSON value copied with the context (an assign reads the whole
    /// context): every value, nested ones included.
    ContextValue,
    /// An entry of a system's actor list passed over by a lookup.
    ActorScan,
}

impl Op {
    pub const ALL: [Op; 21] = [
        Op::SystemCall,
        Op::Macrostep,
        Op::Microstep,
        Op::InternalEvent,
        Op::Candidate,
        Op::Guard,
        Op::Exit,
        Op::Enter,
        Op::Action,
        Op::ContextMerge,
        Op::Expr,
        Op::Effect,
        Op::TimerSchedule,
        Op::TimerCancel,
        Op::ActorStart,
        Op::ActorStop,
        Op::Message,
        Op::EffectStored,
        Op::ActiveState,
        Op::ContextValue,
        Op::ActorScan,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Op::SystemCall => "system_call",
            Op::Macrostep => "macrostep",
            Op::Microstep => "microstep",
            Op::InternalEvent => "internal_event",
            Op::Candidate => "candidate",
            Op::Guard => "guard",
            Op::Exit => "exit",
            Op::Enter => "enter",
            Op::Action => "action",
            Op::ContextMerge => "context_merge",
            Op::Expr => "expr",
            Op::Effect => "effect",
            Op::TimerSchedule => "timer_schedule",
            Op::TimerCancel => "timer_cancel",
            Op::ActorStart => "actor_start",
            Op::ActorStop => "actor_stop",
            Op::Message => "message",
            Op::EffectStored => "effect_stored",
            Op::ActiveState => "active_state",
            Op::ContextValue => "context_value",
            Op::ActorScan => "actor_scan",
        }
    }
}

/// Counts per operation, indexed by `Op as usize`.
pub type Counts = [u64; Op::ALL.len()];

#[cfg(feature = "op-counts")]
thread_local! {
    static COUNTS: std::cell::Cell<Counts> = const { std::cell::Cell::new([0; Op::ALL.len()]) };
    static ENABLED: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Turns counting on or off on this thread (off by default), so timed runs
/// pay nothing for it.
pub fn set_enabled(on: bool) {
    #[cfg(feature = "op-counts")]
    ENABLED.with(|e| e.set(on));
    #[cfg(not(feature = "op-counts"))]
    let _ = on;
}

/// Whether counting is on (always `false` without the `op-counts` feature),
/// for counts that cost something to compute.
#[inline(always)]
pub fn enabled() -> bool {
    #[cfg(feature = "op-counts")]
    return ENABLED.with(|e| e.get());
    #[cfg(not(feature = "op-counts"))]
    false
}

/// Counts one operation (nothing unless counting is on).
#[inline(always)]
pub fn count(op: Op) {
    count_n(op, 1);
}

/// Counts `n` of one operation.
#[inline(always)]
pub fn count_n(op: Op, n: u64) {
    #[cfg(feature = "op-counts")]
    if enabled() {
        COUNTS.with(|c| {
            let mut counts = c.get();
            counts[op as usize] += n;
            c.set(counts);
        });
    }
    #[cfg(not(feature = "op-counts"))]
    let _ = (op, n);
}

/// The number of JSON values in `value`, nested ones included.
pub fn json_size(value: &serde_json::Value) -> u64 {
    1 + match value {
        serde_json::Value::Array(items) => items.iter().map(json_size).sum(),
        serde_json::Value::Object(map) => map.values().map(json_size).sum(),
        _ => 0,
    }
}

/// The counts so far on this thread, resetting them.
pub fn take() -> Counts {
    #[cfg(feature = "op-counts")]
    return COUNTS.with(|c| c.replace([0; Op::ALL.len()]));
    #[cfg(not(feature = "op-counts"))]
    [0; Op::ALL.len()]
}
