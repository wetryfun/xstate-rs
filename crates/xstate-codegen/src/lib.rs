//! Generates a typed Rust module for an XState v6 machine bundle.
//!
//! A bundle (`<name>.xstate.json`, written by `tools/extract/extract.mjs`)
//! holds a `MachineJSON` definition and the JSON Schemas of its context,
//! events, emitted events, input, output and guard and action params. For
//! each bundle the generator emits:
//!
//! - `Context`, `Event` (a serde enum tagged by `type`, with an
//!   [`EventType`](xstate_rt::EventType) impl that matches instead of
//!   serializing), `Emitted`, `Input`, `Output` and one `*Params` type per
//!   guard or action with params;
//! - for each child in `schemas.children` (keyed by invoke `id`, with the
//!   child's accepted `events` and its `output`): `<Id>ChildEvent`,
//!   `<Id>ChildOutput`, a handle `<Id>Child` (from `child_<id>(&mut actor)`)
//!   that sends typed events to the running child, and
//!   `child_<id>_output(event)`, which reads the child's typed output from
//!   its `xstate.done.actor` event;
//! - a trait, `<MachineId>Impl`, with one method per guard, action, delay
//!   and actor the definition needs from Rust;
//! - `registry(impl)` and `machine(impl)`, which wire an implementation of
//!   the trait into the runtime.
//!
//! Before generating, the bundle is checked: the definition must load
//! (`Machine::references`), every referenced guard and action needs an entry
//! in `schemas.guards` / `schemas.actions`, and every static value the
//! definition contains (params, the initial context, `@xstate.assign` and
//! transition `context` patches, raised and emitted events, output) must fit
//! its schema. Handled, raised and emitted event types must be declared.
//!
//! Use it from `build.rs`:
//!
//! ```no_run
//! xstate_codegen::Builder::new().machine("machines/checkout.xstate.json").build().unwrap();
//! ```
//!
//! and include the module with `xstate_rt::include_machine!("checkout")`.

mod names;
mod types;

use std::collections::HashSet;
use std::fmt;
use std::path::{Path, PathBuf};

use serde_json::{Map, Value};
use xstate_rt::{Machine, Reference};

use names::{pascal, raw_lit, snake, string_lit};
use types::{validate, variant_text, Root, Types, VALUE};

/// A problem with a bundle, at a JSON path (`$...` for the definition,
/// `schemas...` for the schemas).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Error {
    pub file: Option<PathBuf>,
    pub path: String,
    pub message: String,
}

impl Error {
    pub(crate) fn new(path: &str, message: impl Into<String>) -> Self {
        Self { file: None, path: path.to_owned(), message: message.into() }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(file) = &self.file {
            write!(f, "{}: ", file.display())?;
        }
        write!(f, "{}: {}", self.path, self.message)
    }
}

impl std::error::Error for Error {}

/// The generated module and any warnings (unused schema entries).
#[derive(Debug, Clone)]
pub struct Generated {
    pub code: String,
    pub warnings: Vec<String>,
}

const SCHEMA_KEYS: &[&str] =
    &["context", "input", "output", "events", "internalEvents", "emitted", "actions", "guards", "children"];

fn obj(v: Option<&Value>) -> Option<&Map<String, Value>> {
    v.and_then(Value::as_object)
}

/// Every transition of a state, with its path: `on`, `always`, `after`,
/// `onDone`, `onTimeout` and invoke completion transitions.
fn transitions<'a>(state: &'a Map<String, Value>, path: &str) -> Vec<(String, &'a Value)> {
    let mut out = Vec::new();
    let mut add = |p: String, v: &'a Value| match v {
        Value::Array(items) => {
            for (i, t) in items.iter().enumerate() {
                out.push((format!("{p}[{i}]"), t));
            }
        }
        t => out.push((p, t)),
    };
    for key in ["on", "after"] {
        for (k, v) in obj(state.get(key)).into_iter().flatten() {
            add(format!("{path}.{key}.{k}"), v);
        }
    }
    for key in ["always", "onDone", "onTimeout"] {
        if let Some(v) = state.get(key) {
            add(format!("{path}.{key}"), v);
        }
    }
    let invokes: Vec<(String, &Value)> = match state.get("invoke") {
        Some(Value::Array(items)) => {
            items.iter().enumerate().map(|(i, v)| (format!("{path}.invoke[{i}]"), v)).collect()
        }
        Some(v) => vec![(format!("{path}.invoke"), v)],
        None => vec![],
    };
    for (ipath, invoke) in invokes {
        for key in ["onDone", "onError", "onSnapshot", "onTimeout"] {
            if let Some(v) = invoke.get(key) {
                add(format!("{ipath}.{key}"), v);
            }
        }
    }
    out
}

/// Calls `f` for every state node, depth first.
fn each_state<'a>(state: &'a Map<String, Value>, path: &str, f: &mut dyn FnMut(&'a Map<String, Value>, &str)) {
    f(state, path);
    for (k, child) in obj(state.get("states")).into_iter().flatten() {
        if let Some(child) = child.as_object() {
            each_state(child, &format!("{path}.states.{k}"), f);
        }
    }
}

/// Every action object with a built-in `type` in the definition.
fn builtin_actions<'a>(value: &'a Value, path: &str, out: &mut Vec<(String, &'a Map<String, Value>)>) {
    match value {
        Value::Array(items) => {
            for (i, v) in items.iter().enumerate() {
                builtin_actions(v, &format!("{path}[{i}]"), out);
            }
        }
        Value::Object(o) => {
            if o.get("type").and_then(Value::as_str).is_some_and(|t| t.starts_with("@xstate."))
                && o.contains_key("event")
                || o.get("type").and_then(Value::as_str) == Some("@xstate.assign")
            {
                out.push((path.to_owned(), o));
            }
            for (k, v) in o {
                if !matches!(k.as_str(), "meta" | "context" | "output" | "description" | "params" | "event") {
                    builtin_actions(v, &format!("{path}.{k}"), out);
                }
            }
        }
        _ => {}
    }
}

struct Checker<'a> {
    schemas: &'a Map<String, Value>,
    declared_events: Option<HashSet<&'a str>>,
}

impl Checker<'_> {
    fn check(value: &Value, schema: &Value, path: &str) -> Result<(), Error> {
        validate(value, schema, schema, path).map_err(|m| match m.split_once(": ") {
            Some((p, m)) => Error::new(p, m),
            None => Error::new(path, m),
        })
    }

    fn event_schema(&self, ty: &str) -> Option<&Value> {
        ["events", "internalEvents"].iter().find_map(|k| self.schemas.get(*k)?.get(ty))
    }

    /// Checks an event object's payload (everything but `type`).
    fn payload(event: &Map<String, Value>, schema: &Value, path: &str) -> Result<(), Error> {
        let mut payload = event.clone();
        payload.remove("type");
        let mut schema = schema.clone();
        if let Some(props) = schema.get_mut("properties").and_then(Value::as_object_mut) {
            props.remove("type");
        }
        if let Some(req) = schema.get_mut("required").and_then(Value::as_array_mut) {
            req.retain(|r| r != "type");
        }
        Self::check(&Value::Object(payload), &schema, path)
    }

    fn references(&self, kind: &str, refs: &[Reference], warnings: &mut Vec<String>) -> Result<(), Error> {
        let empty = Map::new();
        let declared = obj(self.schemas.get(kind)).unwrap_or(&empty);
        for r in refs {
            let def = declared.get(&r.name).ok_or_else(|| {
                Error::new(&r.path, format!("`{}` has no entry in schemas.{kind}; declare its params there", r.name))
            })?;
            let params = r.params.clone().unwrap_or(Value::Null);
            match def.get("params") {
                Some(schema) => Self::check(&params, schema, &format!("{}.params", r.path))?,
                None if !params.is_null() => {
                    return Err(Error::new(
                        &format!("{}.params", r.path),
                        format!("`{}` takes no params (schemas.{kind}.{} declares none)", r.name, r.name),
                    ))
                }
                None => {}
            }
        }
        for name in declared.keys() {
            if !refs.iter().any(|r| r.name == *name) {
                warnings.push(format!("schemas.{kind}.{name} is declared but never referenced"));
            }
        }
        Ok(())
    }

    fn definition(&self, machine: &Map<String, Value>) -> Result<(), Error> {
        let context_schema = self.schemas.get("context");
        let initial = machine.get("context").cloned();
        match (&initial, context_schema) {
            (Some(ctx), Some(schema)) => Self::check(ctx, schema, "$.context")?,
            (Some(_), None) => {
                return Err(Error::new("schemas.context", "the machine has context but no context schema"))
            }
            _ => {}
        }
        let patch = |path: &str, patch: &Value, initial: &Option<Value>| -> Result<(), Error> {
            let (Some(schema), Some(patch)) = (context_schema, patch.as_object()) else { return Ok(()) };
            let mut merged = initial.clone().and_then(|v| v.as_object().cloned()).unwrap_or_default();
            for (k, v) in patch {
                merged.insert(k.clone(), v.clone());
            }
            Self::check(&Value::Object(merged), schema, path)
        };

        let mut result = Ok(());
        each_state(machine, "$", &mut |state, path| {
            if result.is_err() {
                return;
            }
            result = (|| {
                if let Some(declared) = &self.declared_events {
                    for key in obj(state.get("on")).into_iter().flatten().map(|(k, _)| k) {
                        if !key.contains('*') && !key.starts_with("xstate.") && !declared.contains(key.as_str()) {
                            return Err(Error::new(
                                &format!("{path}.on.{key}"),
                                format!("event `{key}` is not declared in schemas.events or schemas.internalEvents"),
                            ));
                        }
                    }
                }
                for (tpath, t) in transitions(state, path) {
                    if let Some(ctx) = t.get("context") {
                        patch(&format!("{tpath}.context"), ctx, &initial)?;
                    }
                }
                Ok(())
            })();
        });
        result?;

        let mut actions = Vec::new();
        let whole = Value::Object(machine.clone());
        builtin_actions(&whole, "$", &mut actions);
        for (path, action) in actions {
            let ty = action.get("type").and_then(Value::as_str).unwrap_or_default();
            if ty == "@xstate.assign" {
                if let Some(ctx) = action.get("context") {
                    patch(&format!("{path}.context"), ctx, &initial)?;
                }
                continue;
            }
            let Some(event) = action.get("event").and_then(Value::as_object) else { continue };
            let event_type = event.get("type").and_then(Value::as_str).unwrap_or_default();
            let epath = format!("{path}.event");
            match ty {
                "@xstate.raise" if !event_type.starts_with("xstate.") => {
                    if let Some(declared) = &self.declared_events {
                        if !declared.contains(event_type) {
                            return Err(Error::new(&epath, format!("raised event `{event_type}` is not declared in schemas.events or schemas.internalEvents")));
                        }
                        Self::payload(event, self.event_schema(event_type).unwrap(), &epath)?;
                    }
                }
                "@xstate.emit" => {
                    let schema = self.schemas.get("emitted").and_then(|e| e.get(event_type)).ok_or_else(|| {
                        Error::new(&epath, format!("emitted event `{event_type}` is not declared in schemas.emitted"))
                    })?;
                    Self::payload(event, schema, &epath)?;
                }
                _ => {}
            }
        }

        if let Some(schema) = self.schemas.get("output") {
            if let Some(output) = machine.get("output") {
                Self::check(output, schema, "$.output")?;
            } else {
                for (k, child) in obj(machine.get("states")).into_iter().flatten() {
                    if child.get("type").and_then(Value::as_str) == Some("final") {
                        if let Some(output) = child.get("output") {
                            Self::check(output, schema, &format!("$.states.{k}.output"))?;
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// The `id`s of every invoke in the definition, with their paths.
fn invoke_ids(machine: &Map<String, Value>) -> Vec<(String, String)> {
    let mut ids = Vec::new();
    each_state(machine, "$", &mut |state, path| {
        let invokes: Vec<(String, &Value)> = match state.get("invoke") {
            Some(Value::Array(items)) => {
                items.iter().enumerate().map(|(i, v)| (format!("{path}.invoke[{i}]"), v)).collect()
            }
            Some(v) => vec![(format!("{path}.invoke"), v)],
            None => vec![],
        };
        for (ipath, invoke) in invokes {
            if let Some(id) = invoke.get("id").and_then(Value::as_str) {
                ids.push((id.to_owned(), ipath));
            }
        }
    });
    ids
}

/// Types, a handle and an output reader for each declared child.
fn children(types: &mut Types, machine: &Map<String, Value>, schemas: &Map<String, Value>) -> Result<String, Error> {
    let Some(declared) = schemas.get("children") else { return Ok(String::new()) };
    let declared = declared.as_object().ok_or_else(|| Error::new("schemas.children", "expected an object"))?;
    let ids = invoke_ids(machine);
    let mut code = String::new();
    for (id, def) in declared {
        let path = format!("schemas.children.{id}");
        let def = def.as_object().ok_or_else(|| Error::new(&path, "expected `{ events?, output? }`"))?;
        if let Some(k) = def.keys().find(|k| !matches!(k.as_str(), "events" | "output")) {
            return Err(Error::new(&format!("{path}.{k}"), "unknown key (expected `events` or `output`)"));
        }
        let Some((_, invoke_path)) = ids.iter().find(|(i, _)| i == id) else {
            return Err(Error::new(&path, format!("no invoke in the machine has `id: \"{id}\"`")));
        };
        let p = pascal(id);
        let (handle, event, output) = (format!("{p}Child"), format!("{p}ChildEvent"), format!("{p}ChildOutput"));
        types.claim(&handle, &path)?;
        match def.get("events") {
            Some(Value::Object(events)) => {
                event_enum(types, &event, &event, &[(&format!("children.{id}.events"), events)])?
            }
            Some(_) => return Err(Error::new(&format!("{path}.events"), "expected an object of event schemas")),
            None => {
                types.claim(&event, &path)?;
                types.out.push(format!("pub type {event} = {VALUE};\n"));
            }
        }
        match def.get("output") {
            Some(schema) => types.named(schema, &output, &format!("{path}.output"))?,
            None => {
                types.claim(&output, &path)?;
                types.out.push(format!("pub type {output} = {VALUE};\n"));
            }
        }
        let snake_id = snake(id);
        let lit = string_lit(id);
        code.push_str(&format!(
            r#"/// The running child invoked with `id: {lit}` (at `{invoke_path}`).
pub struct {handle}<'a> {{
    actor: &'a mut Actor,
    address: String,
}}

impl {handle}<'_> {{
    /// The invoke `id`.
    pub const ID: &'static str = {lit};

    /// The child's address in the actor's system.
    pub fn address(&self) -> &str {{
        &self.address
    }}

    /// The child's status.
    pub fn status(&self) -> __rt::Status {{
        self.actor.system().snapshot(&self.address).map_or(__rt::Status::Stopped, |s| s.status())
    }}

    /// Sends an event to the child at `now_ms` (see `Actor::send_to`).
    pub fn send(&mut self, event: {event}, now_ms: u64) -> Result<(), __rt::SystemError> {{
        let event = __rt::EventType::to_json(&event);
        self.actor.send_to(&self.address, event, now_ms)
    }}
}}

/// The child invoked with `id: {lit}`, while the machine's active states invoke it.
pub fn child_{snake_id}(actor: &mut Actor) -> Option<{handle}<'_>> {{
    let address = actor.children().into_iter().find(|(id, _)| *id == {lit})?.1.to_owned();
    Some({handle} {{ actor, address }})
}}

/// The output of the child invoked with `id: {lit}`, when `event` is its
/// `xstate.done.actor` event (`None` for other events, or an output that
/// does not fit `{output}`).
pub fn child_{snake_id}_output(event: &__rt::Ev<Event>) -> Option<{output}> {{
    if event.event_type() != "xstate.done.actor" || event.field("actorId")?.as_str() != Some({lit}) {{
        return None;
    }}
    __rt::serde_json::from_value(event.field("output").unwrap_or(__rt::serde_json::Value::Null)).ok()
}}

"#
        ));
    }
    Ok(code)
}

/// Distinct names in reference order.
fn distinct(refs: &[Reference]) -> Vec<&Reference> {
    let mut seen = HashSet::new();
    refs.iter().filter(|r| seen.insert(r.name.as_str())).collect()
}

/// An event enum (`Event` or `Emitted`) from `type -> schema` maps.
fn event_enum(
    types: &mut Types,
    name: &str,
    nested_prefix: &str,
    maps: &[(&str, &Map<String, Value>)],
) -> Result<(), Error> {
    types.claim(name, &format!("schemas.{}", maps[0].0))?;
    let mut item = format!(
        "{}\n#[serde(crate = \"__rt::serde\", tag = \"type\")]\npub enum {name} {{\n",
        "#[derive(Debug, Clone, PartialEq, __rt::serde::Serialize, __rt::serde::Deserialize)]"
    );
    let mut arms = String::new();
    let mut seen = HashSet::new();
    for (key, map) in maps {
        for (ty, schema) in *map {
            let path = format!("schemas.{key}.{ty}");
            let variant = pascal(ty);
            if !seen.insert(variant.clone()) {
                return Err(Error::new(&path, format!("event types map to the same variant `{variant}`")));
            }
            let hint = format!("{nested_prefix}{variant}");
            let root = Root { schema, name: &hint };
            let resolved = match schema.get("$ref").and_then(Value::as_str) {
                Some(r) => r.strip_prefix("#/$defs/").and_then(|n| schema.get("$defs")?.get(n)).unwrap_or(schema),
                None => schema,
            };
            let o = resolved
                .as_object()
                .filter(|o| o.get("type").and_then(Value::as_str) == Some("object"))
                .ok_or_else(|| Error::new(&path, "an event schema must be an object (z.object)"))?;
            let fields = types.fields(o, &hint, &path, root, &["type"], "")?;
            item.push_str(&variant_text(&variant, ty, &fields, o));
            arms.push_str(&format!("            Self::{variant} {{ .. }} => {},\n", string_lit(ty)));
        }
    }
    item.push_str("}\n\n");
    item.push_str(&format!(
        "impl __rt::EventType for {name} {{\n    fn event_type(&self) -> &str {{\n        match self {{\n{arms}        }}\n    }}\n}}\n"
    ));
    types.out.push(item);
    Ok(())
}

/// Generates the module for a bundle. `module` names it in comments and is
/// the trait's name when the machine has no `id`.
pub fn generate(bundle: &Value, module: &str) -> Result<Generated, Error> {
    let machine =
        obj(bundle.get("machine")).ok_or_else(|| Error::new("machine", "the bundle has no `machine` object"))?;
    let empty = Map::new();
    let schemas = match bundle.get("schemas") {
        None => &empty,
        Some(Value::Object(s)) => s,
        Some(_) => return Err(Error::new("schemas", "expected an object")),
    };
    for k in schemas.keys() {
        if !SCHEMA_KEYS.contains(&k.as_str()) {
            return Err(Error::new(&format!("schemas.{k}"), "unknown schema key"));
        }
    }
    let machine_value = Value::Object(machine.clone());
    let refs = Machine::<Value, Value>::references(&machine_value).map_err(|e| Error::new(&e.path, e.message))?;

    let events = obj(schemas.get("events"));
    let internal = obj(schemas.get("internalEvents"));
    let declared_events = (events.is_some() || internal.is_some())
        .then(|| events.into_iter().chain(internal).flat_map(|m| m.keys().map(String::as_str)).collect::<HashSet<_>>());
    let checker = Checker { schemas, declared_events };
    let mut warnings = Vec::new();
    checker.references("guards", &refs.guards, &mut warnings)?;
    checker.references("actions", &refs.actions, &mut warnings)?;
    checker.definition(machine)?;

    let id = machine.get("id").and_then(Value::as_str).unwrap_or("");
    let trait_name = format!("{}Impl", pascal(if id.is_empty() { module } else { id }));
    let mut types = Types::default();
    for name in ["Actor", "ID", "DEFINITION", trait_name.as_str()] {
        types.claim(name, "$")?;
    }

    match schemas.get("context") {
        Some(schema) => types.named(schema, "Context", "schemas.context")?,
        None => {
            types.named(&serde_json::json!({ "type": "object", "properties": {} }), "Context", "schemas.context")?
        }
    }
    let mut maps = Vec::new();
    if let Some(e) = events {
        maps.push(("events", e));
    }
    if let Some(e) = internal {
        maps.push(("internalEvents", e));
    }
    if maps.is_empty() {
        types.claim("Event", "schemas.events")?;
        types.out.push(format!("pub type Event = {VALUE};\n"));
    } else {
        event_enum(&mut types, "Event", "", &maps)?;
    }
    if let Some(e) = obj(schemas.get("emitted")) {
        event_enum(&mut types, "Emitted", "Emitted", &[("emitted", e)])?;
    }
    for (key, name) in [("input", "Input"), ("output", "Output")] {
        if let Some(schema) = schemas.get(key) {
            types.named(schema, name, &format!("schemas.{key}"))?;
        }
    }

    let children_code = children(&mut types, machine, schemas)?;

    // The implementation trait and the registry wiring.
    let mut methods = Vec::new();
    let mut wiring = Vec::new();
    let mut method_names = HashSet::new();
    let mut method = |name: String, path: &str| -> Result<String, Error> {
        if !method_names.insert(name.clone()) {
            return Err(Error::new(path, format!("two sources map to the method `{name}`; rename one")));
        }
        Ok(name)
    };
    let params_type = |types: &mut Types, kind: &str, r: &Reference| -> Result<Option<String>, Error> {
        let Some(schema) = schemas.get(kind).and_then(|d| d.get(&r.name)).and_then(|d| d.get("params")) else {
            return Ok(None);
        };
        let name = format!("{}Params", pascal(&r.name));
        types.named(schema, &name, &format!("schemas.{kind}.{}.params", r.name))?;
        Ok(Some(name))
    };
    for r in distinct(&refs.guards) {
        let m = method(format!("guard_{}", snake(&r.name)), &r.path)?;
        let doc = format!("    /// Guard `{}` (referenced at `{}`).\n", r.name, r.path);
        let lit = string_lit(&r.name);
        match params_type(&mut types, "guards", r)? {
            Some(p) => {
                methods.push(format!(
                    "{doc}    fn {m}(&self, args: &__rt::GuardArgs<'_, Context, Event>, params: &{p}) -> bool;\n"
                ));
                wiring.push(format!(
                    "    let i = imp.clone();\n    r.guard_with({lit}, move |args, p: &{p}| i.{m}(args, p));\n"
                ));
            }
            None => {
                methods.push(format!("{doc}    fn {m}(&self, args: &__rt::GuardArgs<'_, Context, Event>) -> bool;\n"));
                wiring.push(format!("    let i = imp.clone();\n    r.guard({lit}, move |args| i.{m}(args));\n"));
            }
        }
    }
    for r in distinct(&refs.actions) {
        let m = method(format!("action_{}", snake(&r.name)), &r.path)?;
        let doc = format!(
            "    /// Action `{}` (referenced at `{}`). Runs as an effect after the step that enqueued it.\n",
            r.name, r.path
        );
        let lit = string_lit(&r.name);
        match params_type(&mut types, "actions", r)? {
            Some(p) => {
                methods.push(format!("{doc}    fn {m}(&self, params: &{p});\n"));
                wiring.push(format!("    let i = imp.clone();\n    r.action_with({lit}, move |p: &{p}| i.{m}(p));\n"));
            }
            None => {
                methods.push(format!("{doc}    fn {m}(&self);\n"));
                wiring.push(format!("    let i = imp.clone();\n    r.action({lit}, move || i.{m}());\n"));
            }
        }
    }
    for r in distinct(&refs.delays) {
        let m = method(format!("delay_{}", snake(&r.name)), &r.path)?;
        methods.push(format!(
            "    /// Delay `{}` in milliseconds (referenced at `{}`), computed when the timer is scheduled.\n    fn {m}(&self, context: &Context, event: &__rt::Ev<Event>) -> u64;\n",
            r.name, r.path
        ));
        wiring.push(format!(
            "    let i = imp.clone();\n    r.delay({}, move |c, e| i.{m}(c, e));\n",
            string_lit(&r.name)
        ));
    }
    for r in distinct(&refs.actors) {
        let m = method(format!("actor_{}", snake(&r.name)), &r.path)?;
        methods.push(format!(
            "    /// Actor logic for `src: {}` (referenced at `{}`).\n    fn {m}(&self) -> ::std::sync::Arc<dyn __rt::Logic>;\n",
            r.name, r.path
        ));
        wiring.push(format!("    r.actor({}, imp.{m}());\n", string_lit(&r.name)));
    }

    let mut code = String::new();
    code.push_str(&format!("// @generated by xstate-codegen from `{module}`. Do not edit.\n\n"));
    code.push_str("#[allow(unused_imports)]\nuse ::xstate_rt::__codegen as __rt;\n\n");
    code.push_str(&format!("/// The machine's `id`.\npub const ID: &str = {};\n\n", string_lit(id)));
    code.push_str(&format!(
        "/// The `MachineJSON` definition.\npub const DEFINITION: &str = {};\n\n",
        raw_lit(&serde_json::to_string(&machine_value).expect("JSON"))
    ));
    code.push_str(&format!(
        "/// The bundle's JSON Schemas (`schemas`: context, events, emitted, ...).\npub const SCHEMAS: &str = {};\n\n",
        raw_lit(&serde_json::to_string(schemas).expect("JSON"))
    ));
    for item in &types.out {
        code.push_str(item);
        code.push('\n');
    }
    code.push_str(&format!(
        "/// The guards, actions, delays and actors `{module}` needs from Rust.\npub trait {trait_name}: Send + Sync + 'static {{\n{}}}\n\n",
        methods.join("\n")
    ));
    code.push_str(&format!(
        "/// A registry that calls `imp`.\npub fn registry<I: {trait_name}>(imp: ::std::sync::Arc<I>) -> __rt::Registry<Context, Event> {{\n    let mut r = __rt::Registry::new();\n{}    let _ = imp;\n    r\n}}\n\n",
        wiring.concat()
    ));
    code.push_str(&format!(
        "/// Compiles the definition with `imp`.\npub fn machine<I: {trait_name}>(imp: ::std::sync::Arc<I>) -> Result<__rt::Machine<Context, Event>, __rt::LoadError> {{\n    __rt::Machine::from_json_str(DEFINITION, &registry(imp))\n}}\n\n"
    ));
    code.push_str("/// An actor running this machine.\npub type Actor = __rt::Actor<Context, Event>;\n");
    if !children_code.is_empty() {
        code.push('\n');
        code.push_str(&children_code);
    }
    Ok(Generated { code, warnings })
}

/// Generates modules for bundles from `build.rs`.
#[derive(Debug, Default)]
pub struct Builder {
    machines: Vec<PathBuf>,
    out_dir: Option<PathBuf>,
}

impl Builder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a bundle (`<name>.xstate.json`). The module is named `<name>`.
    pub fn machine(mut self, path: impl AsRef<Path>) -> Self {
        self.machines.push(path.as_ref().to_owned());
        self
    }

    /// Where to write the modules. Defaults to `$OUT_DIR/xstate`, where
    /// `include_machine!` looks.
    pub fn out_dir(mut self, dir: impl AsRef<Path>) -> Self {
        self.out_dir = Some(dir.as_ref().to_owned());
        self
    }

    /// Writes one module per bundle, prints `cargo:rerun-if-changed` for each
    /// bundle and `cargo:warning` for each warning, and returns the files.
    pub fn build(self) -> Result<Vec<PathBuf>, Error> {
        let io = |file: &Path, e: std::io::Error| Error {
            file: Some(file.to_owned()),
            path: String::new(),
            message: e.to_string(),
        };
        let out_dir = match self.out_dir {
            Some(d) => d,
            None => PathBuf::from(
                std::env::var_os("OUT_DIR")
                    .ok_or_else(|| Error::new("", "OUT_DIR is not set; call build() from build.rs or set out_dir"))?,
            )
            .join("xstate"),
        };
        std::fs::create_dir_all(&out_dir).map_err(|e| io(&out_dir, e))?;
        let mut written = Vec::new();
        for path in self.machines {
            println!("cargo:rerun-if-changed={}", path.display());
            let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or_default();
            let module =
                file_name.strip_suffix(".xstate.json").or_else(|| file_name.strip_suffix(".json")).unwrap_or(file_name);
            let text = std::fs::read_to_string(&path).map_err(|e| io(&path, e))?;
            let bundle: Value = serde_json::from_str(&text).map_err(|e| Error {
                file: Some(path.clone()),
                path: String::new(),
                message: e.to_string(),
            })?;
            let generated = generate(&bundle, module).map_err(|e| Error { file: Some(path.clone()), ..e })?;
            for w in generated.warnings {
                println!("cargo:warning={}: {w}", path.display());
            }
            let out = out_dir.join(format!("{module}.rs"));
            std::fs::write(&out, generated.code).map_err(|e| io(&out, e))?;
            written.push(out);
        }
        Ok(written)
    }
}
