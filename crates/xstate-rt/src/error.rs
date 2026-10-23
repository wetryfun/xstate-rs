use std::fmt;

/// What went wrong while loading a machine definition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LoadErrorKind {
    /// The definition is not valid JSON, or a value has the wrong shape.
    Invalid,
    /// A `{"@code": ...}` or `{"@expr": ...}` node. This runtime cannot evaluate code.
    Code,
    /// A key that `machineFromJSON` does not accept.
    UnknownKey,
    /// A v6 feature this runtime does not support (yet).
    Unsupported,
    /// A transition target, `initial`, history target or `stateIn` id that does not resolve.
    InvalidTarget,
    /// A guard, action or delay referenced by name with no implementation.
    MissingImplementation,
    /// An implementation rejected the `params` it was given.
    InvalidParams,
    /// Two states share an id.
    DuplicateId,
}

/// An error raised while parsing, validating or compiling a machine definition.
///
/// `path` is a JSON path in the style XState uses, for example
/// `$.states.idle.on.go[0].guard`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoadError {
    pub kind: LoadErrorKind,
    pub path: String,
    pub message: String,
}

impl LoadError {
    pub(crate) fn new(kind: LoadErrorKind, path: impl Into<String>, message: impl Into<String>) -> Self {
        Self { kind, path: path.into(), message: message.into() }
    }
}

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} at {}", self.message, self.path)
    }
}

impl std::error::Error for LoadError {}

/// An error raised while running an actor.
#[derive(Debug, Clone, PartialEq)]
pub enum RuntimeError {
    /// One macrostep took more microsteps than the configured bound, usually
    /// because eventless transitions or raised events form a cycle. The actor
    /// is left in the `error` status.
    InfiniteLoop { event_type: String, max_iterations: usize, states: Vec<serde_json::Value> },
    /// The event type is declared in `internalEvents` and cannot be sent from outside.
    InternalEvent { event_type: String },
    /// `@xstate.assign` produced a context that does not deserialize into the
    /// context type. The actor is left in the `error` status.
    Context { message: String },
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RuntimeError::InfiniteLoop { event_type, max_iterations, states } => {
                let states: Vec<String> = states.iter().map(|v| v.to_string()).collect();
                write!(
                    f,
                    "infinite loop processing event \"{event_type}\": more than {max_iterations} microsteps without reaching a stable state. Last states: {}",
                    states.join(" -> ")
                )
            }
            RuntimeError::InternalEvent { event_type } => {
                write!(f, "event \"{event_type}\" is internal and cannot be sent from outside the machine")
            }
            RuntimeError::Context { message } => write!(f, "invalid context: {message}"),
        }
    }
}

impl std::error::Error for RuntimeError {}

/// An error raised while restoring a persisted snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RestoreError(pub String);

impl fmt::Display for RestoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cannot restore snapshot: {}", self.0)
    }
}

impl std::error::Error for RestoreError {}
