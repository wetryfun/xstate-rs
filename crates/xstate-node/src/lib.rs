//! A Node.js native addon (napi-rs) that runs XState v6 machine definitions
//! with `xstate-rt`. It exposes the same actor API as the WASI component
//! (`crates/xstate-wasi`, `wit/xstate.wit`), in camelCase:
//!
//! ```ts
//! class Actor {
//!   static start(definition: string, options: string, now: number, implementations?: Implementations): Actor
//!   static restore(definition: string, options: string, snapshot: string, implementations?: Implementations): Actor
//!   send(event: string, now: number): void
//!   sendTo(address: string, event: string, now: number): void
//!   advance(now: number): void
//!   nextDeadline(): number | null
//!   snapshot(): string
//!   persist(): string
//!   takeEmitted(): string[]
//!   takeLogs(): string[]
//!   pendingEffects(): string[]
//!   resolveEffect(id: string, event?: string | null): void
//!   stop(): void
//! }
//! interface Implementations {
//!   guard?(name: string, params: string | undefined, context: string, event: string): boolean
//!   action?(name: string, params: string | undefined): void
//!   delay?(name: string, context: string, event: string): number
//!   expr?(name: string, scope: string): string
//!   contextExpr?(name: string, input: string | undefined): string
//! }
//! ```
//!
//! Like the component, the addon is generic: it loads any `MachineJSON` at run
//! time. Custom guards, actions, named delays and expressions are the
//! `implementations` callbacks, which receive and return JSON text exactly as
//! the WIT `implementations` interface does; child machines for `invoke` come
//! with the start options, and any other invoke source is async work the
//! caller completes through `resolveEffect`.
//!
//! Callbacks run synchronously on the JavaScript thread, during the actor call
//! that needs them: each call makes its `Env` reachable to the registry
//! closures for its duration. A callback that throws (or returns something
//! unusable) unwinds out of the runtime, the call rethrows its exception, and
//! the actor is unusable afterwards, since the step it interrupted is half
//! done (as a trapping host call leaves the WASI component).
//!
//! Build it with `crates/xstate-node/build.sh` (`cargo build -p xstate-node
//! --release`, then the library copied to `target/release/xstate_node.node`).

use std::any::Any;
use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::panic::{self, AssertUnwindSafe};
use std::sync::Arc;

use napi::bindgen_prelude::{Either, FnArgs, Function, FunctionRef, Object, Undefined, Unknown};
use napi::{sys, Env, JsValue, ValueType};
use napi_derive::napi;
use serde_json::{json, Map, Value};
use xstate_rt::step::async_logic;
use xstate_rt::{Actor as RtActor, InitOptions, Logic, Machine, Registry, Status};

/// Children may invoke children; a cycle of child definitions is an error.
const MAX_CHILD_DEPTH: usize = 32;

#[derive(Default)]
struct Options {
    input: Option<Value>,
    id: Option<String>,
    children: Map<String, Value>,
    max_steps: Option<usize>,
}

fn parse_json(what: &str, text: &str) -> Result<Value, String> {
    serde_json::from_str(text).map_err(|e| format!("invalid {what} JSON: {e}"))
}

fn parse_options(text: &str) -> Result<Options, String> {
    if text.trim().is_empty() {
        return Ok(Options::default());
    }
    let Value::Object(mut o) = parse_json("options", text)? else {
        return Err("options must be a JSON object".into());
    };
    let options = Options {
        input: o.remove("input"),
        id: match o.remove("id") {
            None | Some(Value::Null) => None,
            Some(Value::String(s)) => Some(s),
            Some(_) => return Err("options.id must be a string".into()),
        },
        children: match o.remove("children") {
            None | Some(Value::Null) => Map::new(),
            Some(Value::Object(c)) => c,
            Some(_) => return Err("options.children must map invoke sources to machine definitions".into()),
        },
        max_steps: match o.remove("maxSteps") {
            None | Some(Value::Null) => None,
            Some(v) => Some(v.as_u64().ok_or("options.maxSteps must be a positive integer")? as usize),
        },
    };
    if let Some(k) = o.keys().next() {
        return Err(format!("unknown option \"{k}\""));
    }
    Ok(options)
}

/// `params` as the callbacks see them: `undefined` when the reference has none.
fn params_arg(p: &Value) -> Either<String, Undefined> {
    if p.is_null() {
        Either::B(())
    } else {
        Either::A(p.to_string())
    }
}

/// An optional JSON argument: `undefined` when absent.
fn optional_arg(v: Option<String>) -> Either<String, Undefined> {
    v.map_or(Either::B(()), Either::A)
}

// ---------------------------------------------------------------------------
// Calling JavaScript from the runtime

type GuardRef = FunctionRef<FnArgs<(String, Either<String, Undefined>, String, String)>, Unknown<'static>>;
type ActionRef = FunctionRef<FnArgs<(String, Either<String, Undefined>)>, Unknown<'static>>;
type DelayRef = FunctionRef<FnArgs<(String, String, String)>, Unknown<'static>>;
type ExprRef = FunctionRef<FnArgs<(String, String)>, Unknown<'static>>;
type ContextExprRef = FunctionRef<FnArgs<(String, Either<String, Undefined>)>, Unknown<'static>>;

/// The `implementations` callbacks of one actor, each bound to the object it
/// came from (so they may use `this`). The registry closures share them.
#[derive(Default)]
struct Callbacks {
    guard: Option<GuardRef>,
    action: Option<ActionRef>,
    delay: Option<DelayRef>,
    expr: Option<ExprRef>,
    context_expr: Option<ContextExprRef>,
}

/// One callback of `implementations`, bound to it: `None` when absent
/// (`undefined` or `null`), an error when it is not a function.
fn callback<Args, Return>(object: &Object, key: &str) -> napi::Result<Option<FunctionRef<Args, Return>>>
where
    Args: napi::bindgen_prelude::JsValuesTupleIntoVec,
    Return: napi::bindgen_prelude::FromNapiValue,
{
    let Some(value) = object.get::<Unknown>(key)? else { return Ok(None) };
    match value.get_type()? {
        ValueType::Undefined | ValueType::Null => Ok(None),
        ValueType::Function => {
            // SAFETY: the value was just checked to be a function.
            let f: Function<Args, Return> = unsafe { value.cast()? };
            Ok(Some(f.bind(object)?.create_ref()?))
        }
        _ => Err(napi::Error::from_reason(format!("implementations.{key} must be a function"))),
    }
}

impl Callbacks {
    fn from_js(implementations: Option<Object>) -> napi::Result<Self> {
        let Some(o) = implementations else { return Ok(Self::default()) };
        Ok(Self {
            guard: callback(&o, "guard")?,
            action: callback(&o, "action")?,
            delay: callback(&o, "delay")?,
            expr: callback(&o, "expr")?,
            context_expr: callback(&o, "contextExpr")?,
        })
    }
}

thread_local! {
    /// The `Env` of the actor call in progress on this thread (calls nest when
    /// a callback calls another actor).
    static ENV: Cell<Option<sys::napi_env>> = const { Cell::new(None) };
    /// Why a callback failed, for the actor call that catches the unwind.
    static FAILURE: RefCell<Option<napi::Error>> = const { RefCell::new(None) };
}

/// The unwind payload of a failed callback; the error is in `FAILURE`.
struct CallbackFailed;

/// Gives up on the runtime call in progress: unwinds to the actor call, which
/// throws `error`. (`resume_unwind` skips the panic hook.)
fn fail(error: napi::Error) -> ! {
    FAILURE.with(|f| *f.borrow_mut() = Some(error));
    panic::resume_unwind(Box::new(CallbackFailed))
}

/// Runs `f` with the current call's `Env`, in a handle scope of its own so that
/// a long step does not pile up handles; a failure unwinds (`fail`).
fn call_js<T>(f: impl FnOnce(&Env) -> napi::Result<T>) -> T {
    let Some(raw) = ENV.with(Cell::get) else {
        fail(napi::Error::from_reason("an implementation was called outside a call into the actor"))
    };
    let env = Env::from_raw(raw);
    env.run_in_scope(|| f(&env)).unwrap_or_else(|e| fail(e))
}

/// A callback's JSON text result (`expr`, `contextExpr`): `null` when it does
/// not parse, as in the WASI component.
fn json_result(what: &str, name: &str, value: Unknown) -> napi::Result<Value> {
    if value.get_type()? != ValueType::String {
        return Err(napi::Error::from_reason(format!("{what} \"{name}\" must return a JSON string")));
    }
    // SAFETY: the value was just checked to be a string.
    let text: String = unsafe { value.cast()? };
    Ok(serde_json::from_str(&text).unwrap_or(Value::Null))
}

/// What a callback that a definition needs but `implementations` lacks is
/// reported as.
fn missing(kind: &str, name: &str, key: &str) -> String {
    format!("the definition references {kind} \"{name}\" but implementations.{key} is missing")
}

/// Compiles a definition, wiring every name it references to the callbacks and
/// every invoke source to a child machine or to host async work.
fn compile(
    definition: &Value,
    children: &Map<String, Value>,
    callbacks: &Arc<Callbacks>,
    depth: usize,
) -> Result<Machine<Value, Value>, String> {
    if depth > MAX_CHILD_DEPTH {
        return Err("child machines are nested too deeply (a child invokes itself?)".into());
    }
    let refs = Machine::<Value, Value>::references(definition).map_err(|e| e.to_string())?;
    let mut registry: Registry<Value, Value> = Registry::new();
    let mut seen = HashSet::new();
    for r in &refs.guards {
        if seen.insert(("guard", r.name.clone())) {
            if callbacks.guard.is_none() {
                return Err(missing("guard", &r.name, "guard"));
            }
            let (name, cb) = (r.name.clone(), callbacks.clone());
            registry.guard_with(r.name.clone(), move |args, p: &Value| {
                let args = (name.clone(), params_arg(p), args.context.to_string(), args.event.to_json().to_string());
                call_js(|env| cb.guard.as_ref().unwrap().borrow_back(env)?.call(args.into())?.coerce_to_bool())
            });
        }
    }
    for r in &refs.actions {
        if seen.insert(("action", r.name.clone())) {
            if callbacks.action.is_none() {
                return Err(missing("action", &r.name, "action"));
            }
            let (name, cb) = (r.name.clone(), callbacks.clone());
            registry.action_with(r.name.clone(), move |p: &Value| {
                let args = (name.clone(), params_arg(p));
                call_js(|env| cb.action.as_ref().unwrap().borrow_back(env)?.call(args.into()).map(drop))
            });
        }
    }
    for r in &refs.delays {
        if seen.insert(("delay", r.name.clone())) {
            if callbacks.delay.is_none() {
                return Err(missing("delay", &r.name, "delay"));
            }
            let (name, cb) = (r.name.clone(), callbacks.clone());
            registry.delay(r.name.clone(), move |context, event| {
                let args = (name.clone(), context.to_string(), event.to_json().to_string());
                call_js(|env| {
                    let result = cb.delay.as_ref().unwrap().borrow_back(env)?.call(args.into())?;
                    let ms = match result.get_type()? {
                        // SAFETY: the value was just checked to be a number.
                        ValueType::Number => unsafe { result.cast::<f64>()? },
                        _ => f64::NAN,
                    };
                    if !(ms.is_finite() && ms >= 0.0) {
                        return Err(napi::Error::from_reason(format!(
                            "delay \"{name}\" must return a non-negative number of milliseconds"
                        )));
                    }
                    // Fractional milliseconds round up, like numeric `after` keys.
                    Ok(ms.ceil() as u64)
                })
            });
        }
    }
    for r in &refs.exprs {
        if callbacks.expr.is_none() {
            return Err(missing("expression", &r.name, "expr"));
        }
        let (name, cb) = (r.name.clone(), callbacks.clone());
        registry.expr(r.name.clone(), move |args| {
            let mut scope = json!({ "context": args.context, "event": args.event.to_json() });
            if let Some(params) = args.params {
                scope["params"] = params.clone();
            }
            let args = (name.clone(), scope.to_string());
            call_js(|env| json_result("expr", &name, cb.expr.as_ref().unwrap().borrow_back(env)?.call(args.into())?))
        });
    }
    for r in &refs.contexts {
        if callbacks.context_expr.is_none() {
            return Err(missing("context expression", &r.name, "contextExpr"));
        }
        let (name, cb) = (r.name.clone(), callbacks.clone());
        registry.context(r.name.clone(), move |input| {
            let args = (name.clone(), optional_arg(input.map(Value::to_string)));
            call_js(|env| {
                let result = cb.context_expr.as_ref().unwrap().borrow_back(env)?.call(args.into())?;
                json_result("contextExpr", &name, result)
            })
        });
    }
    for r in &refs.actors {
        if seen.insert(("actor", r.name.clone())) {
            let logic: Arc<dyn Logic> = match children.get(&r.name) {
                Some(child) => Arc::new(
                    compile(child, children, callbacks, depth + 1).map_err(|e| format!("child \"{}\": {e}", r.name))?,
                ),
                None => Arc::new(async_logic(Some(r.name.clone()), None).map_err(|e| e.to_string())?),
            };
            registry.actor(r.name.clone(), logic);
        }
    }
    Machine::from_json(definition, &registry).map_err(|e| e.to_string())
}

fn load(
    definition: &str,
    options: &str,
    callbacks: &Arc<Callbacks>,
) -> Result<(Arc<Machine<Value, Value>>, Options), String> {
    let options = parse_options(options)?;
    let definition = parse_json("definition", definition)?;
    Ok((Arc::new(compile(&definition, &options.children, callbacks, 0)?), options))
}

fn status_str(status: Status) -> &'static str {
    match status {
        Status::Active => "active",
        Status::Done => "done",
        Status::Error => "error",
        Status::Stopped => "stopped",
    }
}

/// `now` from JavaScript: whole milliseconds (fractions are dropped).
fn millis(now: f64) -> napi::Result<u64> {
    if now.is_finite() && now >= 0.0 {
        Ok(now as u64)
    } else {
        Err(napi::Error::from_reason("now must be a non-negative number of milliseconds"))
    }
}

/// How a call into the runtime went wrong.
enum Failure {
    /// The runtime returned an error; the actor is still usable.
    Error(String),
    /// A callback failed or the runtime panicked, part way through a step.
    Fatal(napi::Error),
}

fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic".to_owned()
    }
}

/// Runs `f` with `env` reachable to the callbacks, catching their failures
/// (and any panic, which must not unwind into Node).
fn run<T>(env: &Env, f: impl FnOnce() -> Result<T, String>) -> Result<T, Failure> {
    struct Restore(Option<sys::napi_env>);
    impl Drop for Restore {
        fn drop(&mut self) {
            ENV.with(|e| e.set(self.0));
        }
    }
    let _restore = Restore(ENV.with(|e| e.replace(Some(env.raw()))));
    match panic::catch_unwind(AssertUnwindSafe(f)) {
        Ok(result) => result.map_err(Failure::Error),
        Err(payload) if payload.is::<CallbackFailed>() => Err(Failure::Fatal(
            FAILURE
                .with(|f| f.borrow_mut().take())
                .unwrap_or_else(|| napi::Error::from_reason("an implementation failed")),
        )),
        Err(payload) => {
            Err(Failure::Fatal(napi::Error::from_reason(format!("internal error: {}", panic_message(&*payload)))))
        }
    }
}

/// For `start` and `restore`, where a failure leaves no actor behind.
fn construct(env: &Env, f: impl FnOnce() -> Result<RtActor<Value, Value>, String>) -> napi::Result<Actor> {
    match run(env, f) {
        Ok(actor) => Ok(Actor { state: RefCell::new(Ok(actor)) }),
        Err(Failure::Error(msg)) => Err(napi::Error::from_reason(msg)),
        Err(Failure::Fatal(e)) => Err(e),
    }
}

fn event_json(event: &str) -> Result<Value, String> {
    let event = parse_json("event", event)?;
    if !event.get("type").is_some_and(Value::is_string) {
        return Err("an event must be an object with a string \"type\"".into());
    }
    Ok(event)
}

/// A running machine and its invoked children (the WIT `actor` resource).
/// Callbacks run while the actor is borrowed: one that calls back into the
/// same actor gets an error.
#[napi]
pub struct Actor {
    /// The runtime actor, or why it is no longer usable.
    state: RefCell<Result<RtActor<Value, Value>, String>>,
}

impl Actor {
    fn with<T>(&self, env: &Env, f: impl FnOnce(&mut RtActor<Value, Value>) -> Result<T, String>) -> napi::Result<T> {
        let Ok(mut state) = self.state.try_borrow_mut() else {
            return Err(napi::Error::from_reason("the actor is busy: an implementation called back into it"));
        };
        let actor = match &mut *state {
            Ok(actor) => actor,
            Err(why) => return Err(napi::Error::from_reason(format!("the actor is unusable: {why}"))),
        };
        match run(env, || f(actor)) {
            Ok(v) => Ok(v),
            Err(Failure::Error(msg)) => Err(napi::Error::from_reason(msg)),
            Err(Failure::Fatal(e)) => {
                *state = Err(format!("an earlier call failed part way through a step ({})", e.reason));
                Err(e)
            }
        }
    }
}

#[napi]
impl Actor {
    /// Loads `definition` (MachineJSON) and starts it at `now`.
    #[napi(factory)]
    pub fn start(
        env: Env,
        definition: String,
        options: String,
        now: f64,
        implementations: Option<Object>,
    ) -> napi::Result<Self> {
        let now = millis(now)?;
        let callbacks = Arc::new(Callbacks::from_js(implementations)?);
        construct(&env, || {
            let (machine, options) = load(&definition, &options, &callbacks)?;
            let init = InitOptions {
                input: options.input,
                id: options.id,
                time: now,
                max_steps: options.max_steps.unwrap_or(InitOptions::default().max_steps),
                ..Default::default()
            };
            RtActor::start_with(machine, init).map_err(|e| e.to_string())
        })
    }

    /// Restores an actor from `persist` output. Runs no actions.
    #[napi(factory)]
    pub fn restore(
        env: Env,
        definition: String,
        options: String,
        snapshot: String,
        implementations: Option<Object>,
    ) -> napi::Result<Self> {
        let callbacks = Arc::new(Callbacks::from_js(implementations)?);
        construct(&env, || {
            let (machine, _) = load(&definition, &options, &callbacks)?;
            let snapshot = parse_json("snapshot", &snapshot)?;
            RtActor::restore(machine, &snapshot).map_err(|e| e.to_string())
        })
    }

    /// Sends an event (`{ "type": ..., ... }`) to the root at `now`. When time
    /// moved forward, due timers fire first.
    #[napi]
    pub fn send(&self, env: Env, event: String, now: f64) -> napi::Result<()> {
        let now = millis(now)?;
        self.with(&env, |a| a.send_json(event_json(&event)?, now).map_err(|e| e.to_string()))
    }

    /// Sends an event to any actor in the system by address: the root's is its
    /// id, an invoked child's is `<parent address>/<invoke id>`.
    #[napi]
    pub fn send_to(&self, env: Env, address: String, event: String, now: f64) -> napi::Result<()> {
        let now = millis(now)?;
        self.with(&env, |a| a.send_to(&address, event_json(&event)?, now).map_err(|e| e.to_string()))
    }

    /// Fires every timer due at or before `now`.
    #[napi]
    pub fn advance(&self, env: Env, now: f64) -> napi::Result<()> {
        let now = millis(now)?;
        self.with(&env, |a| a.advance(now).map_err(|e| e.to_string()))
    }

    /// When the next timer is due.
    #[napi]
    pub fn next_deadline(&self, env: Env) -> napi::Result<Option<f64>> {
        self.with(&env, |a| Ok(a.next_deadline().map(|t| t as f64)))
    }

    /// The root's snapshot: `{ "status", "value", "context", "output"?, "error"? }`.
    #[napi]
    pub fn snapshot(&self, env: Env) -> napi::Result<String> {
        self.with(&env, |a| {
            let mut s = json!({
                "status": status_str(a.status()),
                "value": a.value(),
                "context": a.context(),
            });
            if let Some(output) = a.output() {
                s["output"] = output.clone();
            }
            if let Some(error) = a.error() {
                s["error"] = error.clone();
            }
            Ok(s.to_string())
        })
    }

    /// The whole system (XState's `SystemSnapshot` shape), for `restore`.
    #[napi]
    pub fn persist(&self, env: Env) -> napi::Result<String> {
        self.with(&env, |a| Ok(a.persist().to_string()))
    }

    /// Events the root emitted since the last call, oldest first.
    #[napi]
    pub fn take_emitted(&self, env: Env) -> napi::Result<Vec<String>> {
        self.with(&env, |a| Ok(a.take_emitted().iter().map(Value::to_string).collect()))
    }

    /// Argument lists of the root's `@xstate.log` actions since the last call.
    #[napi]
    pub fn take_logs(&self, env: Env) -> napi::Result<Vec<String>> {
        self.with(&env, |a| Ok(a.take_logs().into_iter().map(|args| Value::Array(args).to_string()).collect()))
    }

    /// External work waiting for the host (XState's external effect data).
    #[napi]
    pub fn pending_effects(&self, env: Env) -> napi::Result<Vec<String>> {
        self.with(&env, |a| Ok(a.pending_effects().iter().map(|e| e.to_json().to_string()).collect()))
    }

    /// Completes a pending effect, optionally with a result event.
    #[napi]
    pub fn resolve_effect(&self, env: Env, id: String, event: Option<String>) -> napi::Result<()> {
        self.with(&env, |a| {
            let event = event.map(|e| parse_json("event", &e)).transpose()?;
            a.resolve_effect(&id, event).map_err(|e| e.to_string())
        })
    }

    /// Stops the root and its children.
    #[napi]
    pub fn stop(&self, env: Env) -> napi::Result<()> {
        self.with(&env, |a| a.stop().map_err(|e| e.to_string()))
    }
}
