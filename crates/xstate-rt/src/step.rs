//! Rust-defined actor logic: XState's `createStepLogic` and
//! `createAsyncLogic`, with identical snapshots and effects.
//!
//! Async work never runs inside the system. Async logic asks for it with an
//! `@xstate.effect` external effect; the host does the work and delivers the
//! outcome with [`crate::System::resolve_effect`] and an
//! `xstate.async.resolve` (`data`) or `xstate.async.reject` (`data`) event.

use std::any::Any;
use std::sync::Arc;

use serde_json::{json, Map, Value};

use crate::system::{
    parse_status, AnySnapshot, Context, Effect, EffectExec, EffectKind, EffectType, ExternalSpec, Logic, Status,
    SystemError, Transition,
};

/// A step logic snapshot: XState's `{ status, output, error, input, context, effects }`.
#[derive(Debug, Clone, PartialEq)]
pub struct StepSnapshot {
    pub status: Status,
    pub output: Option<Value>,
    pub error: Option<Value>,
    pub input: Option<Value>,
    pub context: Option<Value>,
    /// Keyed effect states, such as `{ "async": { "status": "active" } }`.
    pub effects: Option<Map<String, Value>>,
}

impl AnySnapshot for StepSnapshot {
    fn status(&self) -> Status {
        self.status
    }

    fn output(&self) -> Option<Value> {
        self.output.clone()
    }

    fn error(&self) -> Option<Value> {
        self.error.clone()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

impl StepSnapshot {
    /// XState's snapshot data.
    pub fn to_data(&self) -> Value {
        let mut m = Map::new();
        m.insert("status".into(), Value::String(self.status.as_str().into()));
        if let Some(v) = &self.output {
            m.insert("output".into(), v.clone());
        }
        if let Some(v) = &self.error {
            m.insert("error".into(), v.clone());
        }
        if let Some(v) = &self.input {
            m.insert("input".into(), v.clone());
        }
        if let Some(v) = &self.context {
            m.insert("context".into(), v.clone());
        }
        if let Some(v) = &self.effects {
            m.insert("effects".into(), Value::Object(v.clone()));
        }
        Value::Object(m)
    }
}

/// What a step function sees.
pub struct StepArgs<'a> {
    pub context: Option<&'a Value>,
    pub event: &'a Value,
    pub input: Option<&'a Value>,
}

/// A partial update of the snapshot, as returned by XState's `run`.
/// `Some(None)` for `input` clears it.
#[derive(Debug, Clone, Default)]
pub struct StepPatch {
    pub context: Option<Value>,
    pub status: Option<Status>,
    pub output: Option<Value>,
    pub error: Option<Value>,
    pub input: Option<Option<Value>>,
    pub effects: Option<Map<String, Value>>,
}

/// The step logic enqueuer.
pub struct StepEnqueue<'a> {
    effects: Vec<Effect>,
    tracked: Map<String, Value>,
    recorded: Option<&'a Map<String, Value>>,
    is_init: bool,
    parent: Option<crate::system::ActorRef>,
    self_ref: crate::system::ActorRef,
}

impl StepEnqueue<'_> {
    /// Emits an event to observers.
    pub fn emit(&mut self, event: Value) {
        let mut params = event.as_object().cloned().unwrap_or_default();
        params.remove("type");
        let ty = event.get("type").and_then(Value::as_str).unwrap_or("").to_owned();
        self.effects.push(Effect::External(ExternalSpec {
            kind: EffectKind::Emit,
            ty: EffectType::Fixed(ty),
            params: Some(Value::Object(params)),
            args: Vec::new(),
            event: Some(event),
            reason: None,
            exec: None,
        }));
    }

    /// Sends an event to the parent actor.
    pub fn send_back(&mut self, event: Value) {
        if let Some(parent) = self.parent.clone() {
            self.effects.push(Effect::SendTo { target: parent, event });
        }
    }

    /// Sends an event back into this logic.
    pub fn raise(&mut self, event: Value) {
        self.effects.push(Effect::SendTo { target: self.self_ref.clone(), event });
    }

    /// Asks the host to run a side effect.
    pub fn effect(&mut self, exec: Option<EffectExec>) {
        self.effects.push(Effect::External(ExternalSpec {
            kind: EffectKind::Action,
            ty: EffectType::Fixed("@xstate.effect".into()),
            params: None,
            args: Vec::new(),
            event: None,
            reason: None,
            exec,
        }));
    }

    /// Asks the host to run a keyed side effect once; tracked on
    /// `snapshot.effects[key]`.
    pub fn effect_keyed(&mut self, key: &str, exec: Option<EffectExec>) {
        let recorded = self.recorded.and_then(|r| r.get(key));
        if let Some(recorded) = recorded {
            let active = recorded.get("status").and_then(Value::as_str) == Some("active");
            if !(self.is_init && active) {
                return;
            }
        }
        self.effects.push(Effect::External(ExternalSpec {
            kind: EffectKind::Action,
            ty: EffectType::Fixed("@xstate.effect".into()),
            params: Some(json!({ "key": key })),
            args: Vec::new(),
            event: None,
            reason: None,
            exec,
        }));
        self.tracked.insert(key.to_owned(), json!({ "status": "active" }));
    }
}

/// The function that defines a step logic.
pub type StepFn = Arc<dyn Fn(&StepArgs<'_>, &mut StepEnqueue<'_>) -> Option<StepPatch> + Send + Sync>;

/// Computes a step logic's initial context from its input.
pub type ContextFn = Arc<dyn Fn(Option<&Value>) -> Option<Value> + Send + Sync>;

/// XState's `createStepLogic`: an actor with context that handles events.
pub struct StepLogic {
    id: Option<String>,
    context: Option<ContextFn>,
    run: StepFn,
    timeout: Option<(Value, u64)>,
}

impl StepLogic {
    /// A step logic. `context` computes the initial context from the input.
    pub fn new(id: Option<String>, context: Option<ContextFn>, run: StepFn) -> Self {
        Self { id, context, run, timeout: None }
    }

    /// The logic's `id`.
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    fn step(&self, snapshot: StepSnapshot, event: &Value, cx: &mut dyn Context) -> Transition {
        if snapshot.status != Status::Active {
            return Transition { snapshot: Box::new(snapshot), effects: Vec::new() };
        }
        let ty = event.get("type").and_then(Value::as_str).unwrap_or("");
        let with_effect = |mut s: StepSnapshot, key: Option<&Value>, state: Value| {
            let key = key.and_then(Value::as_str).unwrap_or("").to_owned();
            s.effects.get_or_insert_with(Map::new).insert(key, state);
            Transition { snapshot: Box::new(s), effects: Vec::new() }
        };
        match ty {
            "xstate.stop" => {
                let mut s = snapshot;
                s.status = Status::Stopped;
                s.input = None;
                let cleanup = Effect::External(ExternalSpec {
                    kind: EffectKind::Action,
                    ty: EffectType::Fixed("@xstate.cleanup".into()),
                    params: None,
                    args: Vec::new(),
                    event: None,
                    reason: None,
                    exec: None,
                });
                return Transition { snapshot: Box::new(s), effects: vec![cleanup] };
            }
            "xstate.logic.effect.start" => {
                return with_effect(snapshot, event.get("key"), json!({ "status": "active" }))
            }
            "xstate.logic.effect.resolve" => {
                let mut state = json!({ "status": "done" });
                if let Some(o) = event.get("output") {
                    state["output"] = o.clone();
                }
                return with_effect(snapshot, event.get("key"), state);
            }
            "xstate.logic.effect.reject" => {
                let mut state = json!({ "status": "error" });
                if let Some(e) = event.get("error") {
                    state["error"] = e.clone();
                }
                return with_effect(snapshot, event.get("key"), state);
            }
            _ => {}
        }
        let self_ref = cx.self_ref();
        let mut enq = StepEnqueue {
            effects: Vec::new(),
            tracked: Map::new(),
            recorded: snapshot.effects.as_ref(),
            is_init: ty == "xstate.init",
            parent: cx.parent_ref(),
            self_ref,
        };
        let patch = (self.run)(
            &StepArgs { context: snapshot.context.as_ref(), event, input: snapshot.input.as_ref() },
            &mut enq,
        );
        let StepEnqueue { effects, tracked, .. } = enq;
        let mut next = snapshot.clone();
        if let Some(p) = &patch {
            if let Some(c) = &p.context {
                next.context = Some(c.clone());
            }
            if let Some(s) = p.status {
                next.status = s;
            }
            if let Some(o) = &p.output {
                next.output = Some(o.clone());
            }
            if let Some(e) = &p.error {
                next.error = Some(e.clone());
            }
            if let Some(i) = &p.input {
                next.input = i.clone();
            }
        }
        let patch_effects = patch.as_ref().and_then(|p| p.effects.clone());
        if patch_effects.is_some() || !tracked.is_empty() {
            let mut merged = snapshot.effects.clone().unwrap_or_default();
            for (k, v) in patch_effects.into_iter().flatten() {
                merged.insert(k, v);
            }
            for (k, v) in tracked {
                merged.insert(k, v);
            }
            next.effects = Some(merged);
        }
        let mut effects = effects;
        if matches!(next.status, Status::Done | Status::Error) {
            effects.push(Effect::Terminate);
        }
        Transition { snapshot: Box::new(next), effects }
    }
}

impl Logic for StepLogic {
    fn initial_transition(&self, input: Option<&Value>, cx: &mut dyn Context) -> Result<Transition, SystemError> {
        let snapshot = StepSnapshot {
            status: Status::Active,
            output: None,
            error: None,
            input: input.cloned(),
            context: self.context.as_ref().and_then(|f| f(input)),
            effects: None,
        };
        let mut init = Map::new();
        init.insert("type".into(), Value::String("xstate.init".into()));
        if let Some(i) = input {
            init.insert("input".into(), i.clone());
        }
        Ok(self.step(snapshot, &Value::Object(init), cx))
    }

    fn transition(
        &self,
        snapshot: &mut Box<dyn AnySnapshot>,
        event: Value,
        cx: &mut dyn Context,
    ) -> Result<Vec<Effect>, SystemError> {
        let event = &event;
        let current = crate::system::downcast::<StepSnapshot>(&**snapshot)
            .cloned()
            .ok_or_else(|| SystemError::Failed("step logic received a foreign snapshot".into()))?;
        let result = self.step(current, event, cx);
        *snapshot = result.snapshot;
        Ok(result.effects)
    }

    fn logic_id(&self) -> Option<String> {
        self.id.clone()
    }

    fn snapshot_data(&self, snapshot: &dyn AnySnapshot) -> Value {
        crate::system::downcast::<StepSnapshot>(snapshot).map_or(Value::Null, StepSnapshot::to_data)
    }

    fn restore(&self, data: &Value) -> Result<Box<dyn AnySnapshot>, SystemError> {
        Ok(Box::new(StepSnapshot {
            status: parse_status(data)?,
            output: data.get("output").cloned(),
            error: data.get("error").cloned(),
            input: data.get("input").cloned(),
            context: data.get("context").cloned(),
            effects: data.get("effects").and_then(Value::as_object).cloned(),
        }))
    }

    fn async_timeout(&self) -> Option<(Value, u64)> {
        self.timeout.clone()
    }
}

/// XState's `createAsyncLogic`: one unit of async work, run by the host.
///
/// On start it requests an `@xstate.effect`; deliver
/// `{ "type": "xstate.async.resolve", "data": output }` or
/// `{ "type": "xstate.async.reject", "data": error }` as its result. With a
/// `timeout` (milliseconds, or a duration string such as `"10s"`), the
/// system rejects it with a `TimeoutError` when the deadline passes.
pub fn async_logic(id: Option<String>, timeout: Option<Value>) -> Result<StepLogic, SystemError> {
    let timeout = match timeout {
        None => None,
        Some(v) => {
            let ms = match &v {
                Value::Number(n) => n.as_f64(),
                Value::String(s) => crate::delay::parse_duration_ms(s),
                _ => None,
            }
            .and_then(crate::delay::ms_to_u64)
            .ok_or_else(|| SystemError::Rejected("Unable to resolve async actor timeout.".into()))?;
            Some((v, ms))
        }
    };
    let run: StepFn = Arc::new(|args, enq| {
        let ty = args.event.get("type").and_then(Value::as_str).unwrap_or("");
        match ty {
            "xstate.async.resolve" => {
                let output = args.event.get("data").or_else(|| args.event.get("output")).cloned();
                let mut state = json!({ "status": "done" });
                if let Some(o) = &output {
                    state["output"] = o.clone();
                }
                Some(StepPatch {
                    status: Some(Status::Done),
                    output,
                    input: Some(None),
                    effects: Some(Map::from_iter([("async".to_owned(), state)])),
                    ..Default::default()
                })
            }
            "xstate.async.reject" => {
                let error = args.event.get("data").or_else(|| args.event.get("error")).cloned();
                let mut state = json!({ "status": "error" });
                if let Some(e) = &error {
                    state["error"] = e.clone();
                }
                Some(StepPatch {
                    status: Some(Status::Error),
                    error,
                    input: Some(None),
                    effects: Some(Map::from_iter([("async".to_owned(), state)])),
                    ..Default::default()
                })
            }
            "xstate.init" => {
                enq.effect(None);
                Some(StepPatch {
                    effects: Some(Map::from_iter([("async".to_owned(), json!({ "status": "active" }))])),
                    ..Default::default()
                })
            }
            _ => None,
        }
    });
    Ok(StepLogic { id, context: None, run, timeout })
}
