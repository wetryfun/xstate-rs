//! A JavaScript evaluator for xstate-rs: runs the code nodes of a language
//! (`{ "@code": "({ context }) => context.n + 1", "@lang": "js" }`, or `ts`
//! with its types stripped by the extract script) in an embedded QuickJS,
//! as XState's own evaluators run them:
//!
//! - `@code` is a function, called as `fn(scope, scope.enq)`
//!   (`Function("return (" + source + ")")()(scope, scope.enq)`);
//! - `@expr` is an expression over the scope's names
//!   (`with (scope) { return (expr); }`).
//!
//! The scope is XState's: `context`, `event`, `input`, `params`, `self`
//! (with `_parent`), `children`, `actions` and `actors` (each name maps to
//! itself: `enq(actions.notify, params)`, `enq.spawn(actors.worker)`), and
//! `enq` in actions and transitions (`raise`, `emit`, `cancel`, `log`,
//! `sendTo`, `spawn`, `stop`; `listen` and `subscribeTo` throw). The root
//! `context` sees `input`, `self`, `actors` and `spawn`. Actor references
//! are `{ $actor, incarnation }` as XState persists them, with `id` as a
//! non-enumerable property.
//!
//! ```
//! use serde_json::{json, Value};
//! use xstate_js::JsEvaluator;
//! use xstate_rt::{Actor, Machine, Registry};
//!
//! let mut registry: Registry<Value, Value> = Registry::new();
//! registry.evaluator("js", JsEvaluator::new());
//! let definition = json!({
//!     "@exprLang": "js",
//!     "context": { "count": 0 },
//!     "initial": "on",
//!     "states": { "on": { "on": { "inc": {
//!         "guard": { "@expr": "context.count < 2" },
//!         "context": { "count": { "@code": "({ context, event }) => context.count + event.by" } }
//!     } } } }
//! });
//! let machine = Machine::from_json(&definition, &registry).unwrap();
//! let mut actor = Actor::start(std::sync::Arc::new(machine), 0).unwrap();
//! actor.send(json!({ "type": "inc", "by": 5 }), 0).unwrap();
//! assert_eq!(actor.context()["count"], 5);
//! ```
//!
//! Values cross as JSON text. Code runs deterministically by default:
//! `Math.random()` and argument-less `Date` throw (see [`JsOptions`]), as
//! the runtime never reads a clock. Each thread keeps its own QuickJS
//! runtime per evaluator (and per nesting depth, for a context `spawn` whose
//! child runs code too), with a memory limit, a stack limit and an
//! interrupt budget per call, so a runaway loop fails the step instead of
//! the worker.

use std::cell::{Cell, RefCell};
use std::collections::HashSet;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use rquickjs::{CatchResultExt, CaughtError, Context, Ctx, Exception, Function, Persistent, Runtime};
use serde::Serialize;
use serde_json::Value;
use xstate_rt::{Code, CodeKind, ContextArgs, Enq, Evaluator, EventType, ExprArgs, Scope, SpawnOptions};

/// Limits and behavior of a [`JsEvaluator`].
#[derive(Debug, Clone)]
pub struct JsOptions {
    /// Bytes the QuickJS heap of one runtime may use (default 32 MiB).
    pub memory_limit: usize,
    /// Bytes of stack a call may use (default 512 KiB).
    pub max_stack_size: usize,
    /// QuickJS interrupt checks a call may take, each after about 10 000
    /// bytecode instructions (default 1000): past it the call fails.
    pub interrupt_limit: u64,
    /// Whether `Math.random()`, `Date.now()` and `new Date()` throw
    /// (default true), so code nodes cannot read randomness or a clock.
    pub deterministic: bool,
}

impl Default for JsOptions {
    fn default() -> Self {
        Self { memory_limit: 32 << 20, max_stack_size: 512 << 10, interrupt_limit: 1000, deterministic: true }
    }
}

/// Evaluates JavaScript code nodes: register it for a language with
/// [`xstate_rt::Registry::evaluator`] (`"js"`, and `"ts"` for TypeScript
/// sources the extract script stripped). Clones share their engines.
#[derive(Clone)]
pub struct JsEvaluator {
    inner: Arc<Inner>,
}

struct Inner {
    id: u64,
    options: JsOptions,
}

impl Drop for Inner {
    fn drop(&mut self) {
        // This thread's engines go now; other threads' go when they exit.
        let _ = ENGINES.try_with(|engines| engines.borrow_mut().retain(|(id, _, _)| *id != self.id));
    }
}

static NEXT_EVALUATOR: AtomicU64 = AtomicU64::new(0);

impl Default for JsEvaluator {
    fn default() -> Self {
        Self::new()
    }
}

impl JsEvaluator {
    pub fn new() -> Self {
        Self::with_options(JsOptions::default())
    }

    pub fn with_options(options: JsOptions) -> Self {
        Self { inner: Arc::new(Inner { id: NEXT_EVALUATOR.fetch_add(1, Ordering::Relaxed), options }) }
    }

    /// Runs `f` with this thread's engine for the current nesting depth.
    fn with_engine<R>(&self, f: impl FnOnce(&Engine) -> R) -> Result<R, String> {
        let depth = DEPTH.with(Cell::get);
        let id = self.inner.id;
        let found = ENGINES.with(|e| e.borrow().iter().find(|(i, d, _)| *i == id && *d == depth).map(|e| e.2.clone()));
        let engine = match found {
            Some(engine) => engine,
            None => {
                let engine = Rc::new(Engine::new(&self.inner.options)?);
                ENGINES.with(|e| e.borrow_mut().push((id, depth, engine.clone())));
                engine
            }
        };
        DEPTH.with(|d| d.set(depth + 1));
        let _depth = Restore(&DEPTH, depth);
        Ok(f(&engine))
    }
}

/// Restores a thread-local cell when dropped.
struct Restore<T: Copy + 'static>(&'static std::thread::LocalKey<Cell<T>>, T);

impl<T: Copy + 'static> Drop for Restore<T> {
    fn drop(&mut self) {
        let value = self.1;
        let _ = self.0.try_with(|c| c.set(value));
    }
}

thread_local! {
    /// Engines by evaluator id and nesting depth.
    static ENGINES: RefCell<Vec<(u64, usize, Rc<Engine>)>> = const { RefCell::new(Vec::new()) };
    /// Evaluations running on this thread.
    static DEPTH: Cell<usize> = const { Cell::new(0) };
    /// The call `enq` and `spawn` act for (see [`Live`]).
    static LIVE: Cell<*const Live<'static>> = const { Cell::new(std::ptr::null()) };
}

/// What JavaScript's `enq`, `spawn` and the actor references reach during
/// one call.
struct Live<'a> {
    enq: Option<&'a Enq<'a>>,
    context: Option<&'a ContextArgs<'a>>,
    /// `{ self, parent?, children? }`, built when the code reads them.
    refs: Option<&'a dyn Fn() -> Value>,
}

/// Makes `live` what `enq` and `spawn` act for while `f` runs.
fn with_live<R>(live: &Live<'_>, f: impl FnOnce() -> R) -> R {
    // Only the lifetime is erased; the pointer is cleared (restored to the
    // enclosing call's) before `live` goes out of scope, panics included.
    let ptr: *const Live<'static> = std::ptr::from_ref(live).cast();
    let previous = LIVE.with(|l| l.replace(ptr));
    let _restore = Restore(&LIVE, previous);
    f()
}

/// Calls `f` with the live call, if there is one.
fn live<R>(f: impl FnOnce(&Live<'_>) -> R) -> Option<R> {
    let ptr = LIVE.with(Cell::get);
    if ptr.is_null() {
        return None;
    }
    // SAFETY: `ptr` is set only by `with_live`, from a reference that
    // outlives the call (the restore guard clears it before the reference
    // ends), and JavaScript runs on this thread only during that call. The
    // reference handed to `f` cannot escape it.
    Some(f(unsafe { &*ptr }))
}

struct Engine {
    /// `__xstate.compile`, `run` and `runContext` (dropped before the
    /// runtime: fields drop in order).
    functions: [Persistent<Function<'static>>; 3],
    compiled: RefCell<HashSet<u64>>,
    calls: Rc<Cell<u64>>,
    limit: u64,
    context: Context,
    _runtime: Runtime,
}

/// Defines `__xstate` (`compile`, `run`, `runContext`), given the Rust
/// bridge as `__xstate_bridge`.
const PRELUDE: &str = include_str!("prelude.js");

/// Makes randomness and the clock throw.
const DETERMINISM: &str = r#"(() => {
  const fail = (what) => () => { throw new Error(`${what} is not available: code nodes run deterministically`); };
  Math.random = fail('Math.random()');
  const RealDate = Date;
  function DeterministicDate(...args) {
    if (args.length === 0) fail('new Date()')();
    return new.target ? new RealDate(...args) : RealDate(...args);
  }
  DeterministicDate.prototype = RealDate.prototype;
  DeterministicDate.UTC = RealDate.UTC;
  DeterministicDate.parse = RealDate.parse;
  DeterministicDate.now = fail('Date.now()');
  globalThis.Date = DeterministicDate;
})();"#;

impl Engine {
    fn new(options: &JsOptions) -> Result<Self, String> {
        let runtime = Runtime::new().map_err(|e| e.to_string())?;
        runtime.set_memory_limit(options.memory_limit);
        runtime.set_max_stack_size(options.max_stack_size);
        let calls = Rc::new(Cell::new(0));
        let counter = calls.clone();
        let limit = options.interrupt_limit;
        runtime.set_interrupt_handler(Some(Box::new(move || {
            counter.set(counter.get() + 1);
            counter.get() > limit
        })));
        let context = Context::full(&runtime).map_err(|e| e.to_string())?;
        let functions = context.with(|ctx| -> Result<_, String> {
            let bridge = Function::new(ctx.clone(), bridge).map_err(|e| e.to_string())?;
            ctx.globals().set("__xstate_bridge", bridge).map_err(|e| e.to_string())?;
            ctx.eval::<(), _>(PRELUDE).catch(&ctx).map_err(|e| format!("prelude: {e}"))?;
            if options.deterministic {
                ctx.eval::<(), _>(DETERMINISM).catch(&ctx).map_err(|e| format!("prelude: {e}"))?;
            }
            let xstate: rquickjs::Object = ctx.globals().get("__xstate").map_err(|e| e.to_string())?;
            let get = |name: &str| -> Result<Persistent<Function<'static>>, String> {
                let f: Function = xstate.get(name).map_err(|e| e.to_string())?;
                Ok(Persistent::save(&ctx, f))
            };
            Ok([get("compile")?, get("run")?, get("runContext")?])
        })?;
        Ok(Self { functions, compiled: RefCell::new(HashSet::new()), calls, limit, context, _runtime: runtime })
    }

    /// Runs a code node with its scope (each value as JSON text) and
    /// returns its result.
    fn run(&self, code: &Code, scope: &ScopeText) -> Result<Value, String> {
        self.calls.set(0);
        self.context.with(|ctx| {
            let function = |i: usize| self.functions[i].clone().restore(&ctx).map_err(|e| e.to_string());
            let kind = code.kind.as_str();
            let id = code.id as f64;
            if !self.compiled.borrow().contains(&code.id) {
                function(0)?
                    .call::<_, ()>((id, kind, code.source.as_str()))
                    .catch(&ctx)
                    .map_err(|e| self.message(&ctx, e))?;
                self.compiled.borrow_mut().insert(code.id);
            }
            let code_mode = i32::from(code.kind == CodeKind::Code);
            let out: Option<String> = match scope {
                ScopeText::Expr { enq, context, event, input, params } => {
                    let mode = code_mode | if *enq { 2 } else { 0 };
                    function(1)?.call((id, mode, context.as_str(), event.as_str(), input.as_deref(), params.as_deref()))
                }
                ScopeText::Context { input, self_ref } => {
                    function(2)?.call((id, code_mode, input.as_deref(), self_ref.as_deref()))
                }
            }
            .catch(&ctx)
            .map_err(|e| self.message(&ctx, e))?;
            match out {
                None => Ok(Value::Null),
                Some(text) => serde_json::from_str(&text).map_err(|e| format!("the result is not JSON: {e}")),
            }
        })
    }

    fn message(&self, _ctx: &Ctx<'_>, error: CaughtError<'_>) -> String {
        if self.calls.get() > self.limit {
            return "the code ran too long (its interrupt budget ran out)".into();
        }
        match error {
            CaughtError::Exception(e) => e.message().unwrap_or_else(|| e.to_string()),
            CaughtError::Value(v) => v.as_string().and_then(|s| s.to_string().ok()).unwrap_or_else(|| format!("{v:?}")),
            CaughtError::Error(e) => e.to_string(),
        }
    }
}

/// A code node's scope, each value as JSON text.
enum ScopeText {
    Expr { enq: bool, context: String, event: String, input: Option<String>, params: Option<String> },
    Context { input: Option<String>, self_ref: Option<String> },
}

/// The JavaScript side's calls into Rust: `(op, JSON args)` to JSON text.
fn bridge<'js>(ctx: Ctx<'js>, op: String, args: String) -> rquickjs::Result<Option<String>> {
    let args: Vec<Value> = serde_json::from_str(&args).unwrap_or_default();
    let result = live(|live| apply(live, &op, args))
        .unwrap_or_else(|| Err(format!("enq.{op} can only be used while the code node that received it runs")));
    match result {
        Ok(v) => Ok(v.map(|v| v.to_string())),
        Err(message) => Err(Exception::throw_message(&ctx, &message)),
    }
}

fn apply(live: &Live<'_>, op: &str, mut args: Vec<Value>) -> Result<Option<Value>, String> {
    args.resize(3, Value::Null);
    let mut args = args.into_iter();
    let (a, b, c) = (args.next().unwrap_or_default(), args.next().unwrap_or_default(), args.next().unwrap_or_default());
    if op == "refs" {
        return Ok(live.refs.map(|refs| refs()));
    }
    if op == "spawnContext" {
        let cx = live.context.ok_or("spawn is only available in the root context")?;
        let src = a.as_str().ok_or("spawn: the source must be a string (actors.<name>)")?;
        return Ok(Some(cx.spawn(src, spawn_options(&b))));
    }
    let enq = live.enq.ok_or_else(|| format!("enq.{op} is not available here"))?;
    match op {
        "action" => {
            let name = a.as_str().ok_or("enq(action): name the action as actions.<name>")?;
            enq.action(name, b);
        }
        "raise" => match delay(&b) {
            Some(ms) => enq.raise_delayed(a, ms, b.get("id").and_then(Value::as_str)),
            None => enq.raise(a),
        },
        "emit" => enq.emit(a),
        "cancel" => enq.cancel(a.as_str().unwrap_or_default()),
        "log" => enq.log(match a {
            Value::Array(items) => items,
            other => vec![other],
        }),
        "sendTo" => {
            // A ref (`$actor`), or a child by id (a string or `{ id }`).
            let target = match &a {
                Value::Object(o) if !o.contains_key("$actor") => o.get("id").cloned().unwrap_or(Value::Null),
                Value::Object(o) => {
                    let mut r = o.clone();
                    r.remove("id");
                    Value::Object(r)
                }
                other => other.clone(),
            };
            match delay(&c) {
                Some(ms) => enq.send_to_ref_delayed(&target, b, ms, c.get("id").and_then(Value::as_str)),
                None => enq.send_to_ref(&target, b),
            }
        }
        "stop" => {
            if let Some(id) = a.as_str().or_else(|| a.get("id").and_then(Value::as_str)) {
                enq.stop(id);
            }
        }
        "spawn" => {
            let src = a.as_str().ok_or("enq.spawn: the source must be a string (actors.<name>)")?;
            return Ok(Some(Value::String(enq.spawn(src, spawn_options(&b)))));
        }
        other => return Err(format!("enq.{other} is not supported")),
    }
    Ok(None)
}

fn delay(options: &Value) -> Option<u64> {
    let ms = options.get("delay")?.as_f64()?;
    Some(if ms > 0.0 { ms.ceil() as u64 } else { 0 })
}

fn spawn_options(options: &Value) -> SpawnOptions {
    SpawnOptions {
        id: options.get("id").and_then(Value::as_str).map(str::to_owned),
        input: options.get("input").cloned(),
        system_id: options.get("systemId").and_then(Value::as_str).map(str::to_owned),
        sync_snapshot: options.get("syncSnapshot").and_then(Value::as_bool),
    }
}

impl<C: Serialize, E: EventType> Evaluator<C, E> for JsEvaluator {
    fn eval(&self, code: &Code, scope: Scope<'_, '_, C, E>) -> Result<Value, String> {
        match scope {
            Scope::Expr(a) => {
                let text = expr_scope(a)?;
                let refs = || refs(a);
                let live = Live { enq: a.enq(), context: None, refs: Some(&refs) };
                self.with_engine(|engine| with_live(&live, || engine.run(code, &text)))?
            }
            Scope::Context(a) => {
                let text = ScopeText::Context {
                    input: a.input.map(Value::to_string),
                    self_ref: a.self_ref().map(|r| r.to_json().to_string()),
                };
                let live = Live { enq: None, context: Some(a), refs: None };
                self.with_engine(|engine| with_live(&live, || engine.run(code, &text)))?
            }
        }
    }
}

/// `self` (with its parent) and the children, as JavaScript reads them.
fn refs<C, E>(a: &ExprArgs<'_, C, E>) -> Value {
    let mut refs = serde_json::Map::new();
    refs.insert("self".into(), a.self_ref().to_json());
    if let Some(parent) = a.parent_ref() {
        refs.insert("parent".into(), parent.to_json());
    }
    let children = a.children();
    if !children.is_empty() {
        let children = children.iter().map(|(id, r)| (id.clone(), r.to_json())).collect();
        refs.insert("children".into(), Value::Object(children));
    }
    Value::Object(refs)
}

fn expr_scope<C: Serialize, E: EventType>(a: &ExprArgs<'_, C, E>) -> Result<ScopeText, String> {
    Ok(ScopeText::Expr {
        enq: a.enq().is_some(),
        context: serde_json::to_string(a.context).map_err(|e| format!("the context does not serialize: {e}"))?,
        event: a.event.to_json().to_string(),
        input: a.input.map(Value::to_string),
        params: a.params.map(Value::to_string),
    })
}

#[cfg(test)]
mod tests;
