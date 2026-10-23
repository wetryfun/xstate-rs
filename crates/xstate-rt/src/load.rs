//! Parsing, validation and compilation of `MachineJSON` (the output of
//! XState v6 `machineToJSON` / `machineConfigToJSON`) into a [`Machine`].

use std::collections::HashMap;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde_json::{json, Map, Value};

use crate::core::{DomainHint, Kind as NodeKind, Nid, Tid, Tree, TreeNode, TreeTransition};
use crate::delay::{ms_to_u64, parse_duration_ms};
use crate::error::{LoadError, LoadErrorKind as K};
use crate::event::{Ev, EventType, AFTER, DONE_STATE};
use crate::machine::*;
use crate::registry::Registry;

type TransitionId = Tid;

/// A node while loading; split into the shared [`Tree`] and [`NodeData`] at the end.
struct Node {
    key: String,
    id: String,
    parent: Option<NodeId>,
    end: NodeId,
    kind: NodeKind,
    children: Vec<NodeId>,
    history_children: Vec<NodeId>,
    initial: Option<NodeId>,
    history_target: Vec<NodeId>,
    entry: Vec<Action>,
    exit: Vec<Action>,
    on: Vec<(String, Vec<Tid>)>,
    always: Vec<Tid>,
    delayed: Vec<DelayedDef>,
    invokes: Vec<InvokeDef>,
    output: Option<Template>,
    tags: Vec<String>,
}

type Result<T> = std::result::Result<T, LoadError>;

/// Keys `machineFromJSON` accepts on a state node, plus `onDone`, which
/// `machineToJSON` emits and `createMachine` accepts.
const STATE_KEYS: &[&str] = &[
    "id",
    "key",
    "type",
    "initial",
    "states",
    "on",
    "onDone",
    "onError",
    "after",
    "always",
    "choice",
    "route",
    "invoke",
    "entry",
    "exit",
    "meta",
    "description",
    "tags",
    "input",
    "timeout",
    "onTimeout",
    "history",
    "target",
    "output",
    "context",
];
const MACHINE_KEYS: &[&str] =
    &["@exprLang", "version", "actions", "guards", "actors", "delays", "schemas", "internalEvents"];
const TRANSITION_KEYS: &[&str] =
    &["target", "matches", "context", "actions", "guard", "description", "reenter", "meta", "input"];

/// State-node keys that name v6 features this runtime does not run yet.
const UNSUPPORTED_STATE_KEYS: &[(&str, &str)] = &[("choice", "choice states"), ("input", "state input")];

fn err(kind: K, path: &str, message: impl Into<String>) -> LoadError {
    LoadError::new(kind, path, message)
}

fn invalid(path: &str, message: impl Into<String>) -> LoadError {
    err(K::Invalid, path, message)
}

fn child_path(path: &str, key: &str) -> String {
    format!("{path}.{key}")
}

/// The language of the code nodes this runtime evaluates: the source is the
/// name of a Rust implementation ([`Registry::expr`]).
pub const RUST_LANG: &str = "rust";

/// The source of a code node (`{"@code": ...}` or `{"@expr": ...}`).
fn code_source(value: &Value) -> Option<&str> {
    let map = value.as_object()?;
    map.get("@code").or_else(|| map.get("@expr")).and_then(Value::as_str)
}

/// Checks every code node's language: only `rust` runs here (the node's
/// `@lang`, else the root `@exprLang`).
fn check_code(value: &Value, path: &str, default_lang: Option<&str>) -> Result<()> {
    match value {
        Value::Object(map) => {
            if let Some(key) = ["@code", "@expr"].into_iter().find(|k| map.contains_key(*k)) {
                if !map[key].is_string() {
                    return Err(err(K::Code, path, format!("{key} must be a string")));
                }
                let lang = match map.get("@lang") {
                    Some(Value::String(l)) => Some(l.as_str()),
                    Some(_) => return Err(err(K::Code, path, "@lang must be a string")),
                    None => default_lang,
                };
                return match lang {
                    Some(RUST_LANG) => Ok(()),
                    Some(other) => Err(err(
                        K::Code,
                        path,
                        format!(
                            "{key} in language '{other}' cannot run in xstate-rs; use \"@lang\": \"rust\" with the \
                             name of a Rust expression, or reference a named implementation"
                        ),
                    )),
                    None => Err(err(K::Code, path, format!("{key} without @lang (and no root @exprLang)"))),
                };
            }
            for (k, v) in map {
                check_code(v, &child_path(path, k), default_lang)?;
            }
        }
        Value::Array(items) => {
            for (i, v) in items.iter().enumerate() {
                check_code(v, &format!("{path}[{i}]"), default_lang)?;
            }
        }
        _ => {}
    }
    Ok(())
}

/// Rejects code nodes where this runtime does not evaluate them.
fn reject_code(value: &Value, path: &str, what: &str) -> Result<()> {
    match value {
        Value::Object(map) => {
            if code_source(value).is_some() {
                return Err(err(K::Unsupported, path, format!("expressions are not supported in {what}")));
            }
            for (k, v) in map {
                reject_code(v, &child_path(path, k), what)?;
            }
        }
        Value::Array(items) => {
            for (i, v) in items.iter().enumerate() {
                reject_code(v, &format!("{path}[{i}]"), what)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn contains_code(value: &Value) -> bool {
    match value {
        Value::Object(map) => code_source(value).is_some() || map.values().any(contains_code),
        Value::Array(items) => items.iter().any(contains_code),
        _ => false,
    }
}

/// Which expressions a template's code nodes name.
#[derive(Clone, Copy, PartialEq)]
enum Slot {
    /// [`Registry::expr`].
    Scope,
    /// The root `context`: [`Registry::context`].
    Context,
}

fn has_unescaped_dot(key: &str) -> bool {
    let mut chars = key.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => {
                chars.next();
            }
            '.' => return true,
            _ => {}
        }
    }
    false
}

/// Splits a state path on unescaped dots. `\.` stays in the segment (state
/// keys keep their escapes); other escapes drop the backslash.
fn to_state_path(path: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut segment = String::new();
    let mut chars = path.chars();
    while let Some(c) = chars.next() {
        match c {
            '\\' => match chars.next() {
                Some('.') => segment.push_str("\\."),
                Some(other) => segment.push(other),
                None => {}
            },
            '.' => out.push(std::mem::take(&mut segment)),
            other => segment.push(other),
        }
    }
    out.push(segment);
    out
}

struct RawNode<'j> {
    json: &'j Map<String, Value>,
    path: String,
}

struct Loader<'j, 'r, C, E> {
    root_json: &'j Map<String, Value>,
    registry: &'r Registry<C, E>,
    machine_id: Option<String>,
    nodes: Vec<Node>,
    raw: Vec<RawNode<'j>>,
    id_map: HashMap<String, NodeId>,
    transitions: Vec<TransitionData>,
    transition_sources: Vec<Nid>,
    guards: Vec<crate::registry::GuardFn<C, E>>,
    actions: Vec<CustomAction>,
    delays: Vec<crate::registry::DelayFn<C, E>>,
    named_delays: HashMap<String, DelaySpec>,
    raise_events: Vec<Ev<E>>,
    exprs: Vec<crate::registry::ExprFn<C, E>>,
    expr_index: HashMap<String, u32>,
    context_exprs: Vec<crate::registry::ContextFn>,
    context_index: HashMap<String, u32>,
    /// Code nodes compiled so far (to tell whether a named action or guard
    /// has expressions that read its params).
    expr_refs: usize,
    dynamic_actions: Vec<Dynamic<crate::registry::ActionFn>>,
    dynamic_guards: Vec<Dynamic<crate::registry::GuardFn<C, E>>>,
    /// When set, missing implementations are recorded here and replaced by
    /// stubs instead of failing the load (see [`references`]).
    refs: Option<References>,
}

/// A name the definition references without defining it, with the JSON path
/// of the reference and its `params`.
#[derive(Debug, Clone, PartialEq)]
pub struct Reference {
    pub name: String,
    pub path: String,
    pub params: Option<Value>,
}

/// Every guard, action, delay and actor source a definition needs from the
/// host, in document order. Names defined in the JSON itself and built-ins
/// are not included.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct References {
    pub guards: Vec<Reference>,
    pub actions: Vec<Reference>,
    pub delays: Vec<Reference>,
    pub actors: Vec<Reference>,
    /// Expressions (`{"@code": name, "@lang": "rust"}`), each name once.
    pub exprs: Vec<Reference>,
    /// Expressions in the root `context`, computed from the input.
    pub contexts: Vec<Reference>,
}

/// Validates a definition as [`load`] does and lists the implementations it
/// needs, with the runtime's own name resolution.
pub(crate) fn references(json: &Value) -> Result<References> {
    let registry: Registry<Value, Value> = Registry::new();
    let (_, refs) = load_inner(json, &registry, MachineOptions::default(), Some(References::default()))?;
    Ok(refs.unwrap_or_default())
}

/// Compiles a machine definition against a registry of implementations.
pub(crate) fn load<C, E>(json: &Value, registry: &Registry<C, E>, options: MachineOptions) -> Result<Machine<C, E>>
where
    C: DeserializeOwned,
    E: EventType,
{
    load_inner(json, registry, options, None).map(|(m, _)| m)
}

fn load_inner<C, E>(
    json: &Value,
    registry: &Registry<C, E>,
    options: MachineOptions,
    refs: Option<References>,
) -> Result<(Machine<C, E>, Option<References>)>
where
    C: DeserializeOwned,
    E: EventType,
{
    let root_json = json.as_object().ok_or_else(|| invalid("$", "a machine definition must be a JSON object"))?;
    let default_lang = opt_str(root_json, "@exprLang", "$")?;
    check_code(json, "$", default_lang)?;
    let machine_id = opt_str(root_json, "id", "$")?.map(str::to_owned);
    let version = opt_str(root_json, "version", "$")?.map(str::to_owned);

    let mut loader = Loader {
        root_json,
        registry,
        machine_id,
        nodes: Vec::new(),
        raw: Vec::new(),
        id_map: HashMap::new(),
        transitions: Vec::new(),
        transition_sources: Vec::new(),
        guards: Vec::new(),
        actions: Vec::new(),
        delays: Vec::new(),
        named_delays: HashMap::new(),
        raise_events: Vec::new(),
        exprs: Vec::new(),
        expr_index: HashMap::new(),
        context_exprs: Vec::new(),
        context_index: HashMap::new(),
        expr_refs: 0,
        dynamic_actions: Vec::new(),
        dynamic_guards: Vec::new(),
        refs,
    };
    loader.check_machine_keys()?;
    loader.load_named_delays()?;
    loader.build_node(root_json, String::new(), None, "$".to_string(), &mut Vec::new())?;
    for i in 0..loader.nodes.len() {
        loader.resolve_node(i as NodeId)?;
    }

    let initial_context = match root_json.get("context") {
        Some(ctx) if contains_code(ctx) => {
            InitialContext::Template(loader.compile_template(ctx, "$.context", Slot::Context)?)
        }
        Some(ctx) => InitialContext::Value(
            C::deserialize(ctx)
                .map_err(|e| invalid("$.context", format!("context does not fit the context type: {e}")))?,
        ),
        None => InitialContext::Value(
            C::deserialize(&Value::Object(Map::new())).or_else(|_| C::deserialize(&Value::Null)).map_err(|e| {
                invalid(
                    "$",
                    format!("the definition has no context and the context type cannot be built from {{}}: {e}"),
                )
            })?,
        ),
    };
    let output = match root_json.get("output") {
        Some(o) => Some(loader.compile_template(o, "$.output", Slot::Scope)?),
        None => None,
    };
    let internal_events = match root_json.get("internalEvents") {
        None => Vec::new(),
        Some(Value::Array(items)) => items
            .iter()
            .enumerate()
            .map(|(i, v)| {
                v.as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| invalid(&format!("$.internalEvents[{i}]"), "expected a string"))
            })
            .collect::<Result<_>>()?,
        Some(_) => return Err(invalid("$.internalEvents", "expected an array of event descriptors")),
    };

    let mut tree_nodes = Vec::with_capacity(loader.nodes.len());
    let mut nodes = Vec::with_capacity(loader.nodes.len());
    for n in loader.nodes {
        let mut t = TreeNode::new(n.parent, n.kind);
        t.children = n.children;
        t.history_children = n.history_children;
        t.on = n.on;
        t.always = n.always;
        tree_nodes.push(t);
        nodes.push(NodeData {
            key: n.key,
            id: n.id,
            entry: n.entry,
            exit: n.exit,
            delayed: n.delayed,
            invokes: n.invokes,
            output: n.output,
            tags: n.tags,
            initial: n.initial.into_iter().collect::<Vec<_>>().into(),
            history_target: n.history_target.into(),
        });
    }
    let tree_transitions =
        loader.transition_sources.iter().map(|&source| TreeTransition { source, domain: DomainHint::None }).collect();

    let refs = loader.refs;
    let machine = Machine {
        id: loader.machine_id,
        version,
        tree: Tree::new(tree_nodes, tree_transitions),
        nodes,
        transitions: loader.transitions,
        guards: loader.guards,
        actions: loader.actions,
        actors: registry.actors.clone(),
        delays: loader.delays,
        raise_events: loader.raise_events,
        exprs: loader.exprs,
        context_exprs: loader.context_exprs,
        dynamic_actions: loader.dynamic_actions,
        dynamic_guards: loader.dynamic_guards,
        id_map: loader.id_map,
        internal_events,
        initial_context,
        output,
        options,
        start_error: (root_json.contains_key("initial") && obj_is_empty(root_json.get("states")))
            .then(|| "Initial state not found on \"#\"".to_owned()),
        empty: Default::default(),
    };
    Ok((machine, refs))
}

fn obj_is_empty(v: Option<&Value>) -> bool {
    v.and_then(Value::as_object).map_or(true, Map::is_empty)
}

fn opt_str<'a>(obj: &'a Map<String, Value>, key: &str, path: &str) -> Result<Option<&'a str>> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::String(s)) => Ok(Some(s)),
        Some(_) => Err(invalid(&child_path(path, key), "expected a string")),
    }
}

fn opt_object<'a>(obj: &'a Map<String, Value>, key: &str, path: &str) -> Result<Option<&'a Map<String, Value>>> {
    match obj.get(key) {
        None => Ok(None),
        Some(Value::Object(m)) => Ok(Some(m)),
        Some(_) => Err(invalid(&child_path(path, key), "expected an object")),
    }
}

fn check_reserved(kind: &str, name: &str, path: &str) -> Result<()> {
    if name.starts_with("xstate.") || name.starts_with("@xstate.") {
        return Err(invalid(
            path,
            format!(
                "invalid {kind} name '{name}': the 'xstate.' and '@xstate.' prefixes are reserved for built-in names"
            ),
        ));
    }
    Ok(())
}

impl<'j, C, E: EventType> Loader<'j, '_, C, E> {
    /// A JSON value with code nodes compiled to expressions.
    fn compile_template(&mut self, value: &Value, path: &str, slot: Slot) -> Result<Template> {
        if let Some(name) = code_source(value) {
            self.expr_refs += 1;
            return self.expr(name, path, slot).map(Template::Expr);
        }
        if !contains_code(value) {
            return Ok(Template::Static(value.clone()));
        }
        match value {
            Value::Object(map) => map
                .iter()
                .map(|(k, v)| Ok((k.clone(), self.compile_template(v, &child_path(path, k), slot)?)))
                .collect::<Result<_>>()
                .map(Template::Object),
            Value::Array(items) => items
                .iter()
                .enumerate()
                .map(|(i, v)| self.compile_template(v, &format!("{path}[{i}]"), slot))
                .collect::<Result<_>>()
                .map(Template::Array),
            _ => unreachable!("only objects and arrays contain code"),
        }
    }

    /// The index of a named expression.
    fn expr(&mut self, name: &str, path: &str, slot: Slot) -> Result<u32> {
        let (index, found) = match slot {
            Slot::Scope => (&self.expr_index, self.registry.exprs.contains_key(name)),
            Slot::Context => (&self.context_index, self.registry.contexts.contains_key(name)),
        };
        if let Some(&i) = index.get(name) {
            return Ok(i);
        }
        if !found {
            let recorded = match slot {
                Slot::Scope => self.record(|r| &mut r.exprs, name, path, None),
                Slot::Context => self.record(|r| &mut r.contexts, name, path, None),
            };
            if !recorded {
                let hint = match slot {
                    Slot::Scope if self.registry.contexts.contains_key(name) => {
                        " (it is registered with `Registry::context`, which only the root `context` uses)"
                    }
                    Slot::Context if self.registry.exprs.contains_key(name) => {
                        " (the root `context` is computed from the input: register it with `Registry::context`)"
                    }
                    _ => "",
                };
                return Err(err(K::MissingImplementation, path, format!("missing expression \"{name}\"{hint}")));
            }
        }
        let i = match slot {
            Slot::Scope => {
                let f = self.registry.exprs.get(name).cloned().unwrap_or_else(|| Arc::new(|_| Value::Null));
                self.exprs.push(f);
                self.expr_index.insert(name.to_owned(), self.exprs.len() as u32 - 1);
                self.exprs.len() as u32 - 1
            }
            Slot::Context => {
                let f = self.registry.contexts.get(name).cloned().unwrap_or_else(|| Arc::new(|_| Value::Null));
                self.context_exprs.push(f);
                self.context_index.insert(name.to_owned(), self.context_exprs.len() as u32 - 1);
                self.context_exprs.len() as u32 - 1
            }
        };
        Ok(i)
    }

    /// A delay given as a code node.
    fn delay_expr(&mut self, value: &Value, path: &str) -> Result<Option<DelaySpec>> {
        match code_source(value) {
            Some(name) => {
                self.expr_refs += 1;
                Ok(Some(DelaySpec::Expr(self.expr(name, path, Slot::Scope)?)))
            }
            None => Ok(None),
        }
    }

    fn check_machine_keys(&self) -> Result<()> {
        for kind in ["actions", "guards", "delays"] {
            if let Some(map) = opt_object(self.root_json, kind, "$")? {
                for name in map.keys() {
                    check_reserved(kind, name, &format!("$.{kind}.{name}"))?;
                }
            }
        }
        if let Some(guards) = opt_object(self.root_json, "guards", "$")? {
            for (name, def) in guards {
                if def.get("when").is_none() {
                    return Err(invalid(&format!("$.guards.{name}"), "a named guard must be { \"when\": <guard> }"));
                }
            }
        }
        Ok(())
    }

    fn load_named_delays(&mut self) -> Result<()> {
        let Some(delays) = opt_object(self.root_json, "delays", "$")? else { return Ok(()) };
        for (name, def) in delays {
            let path = format!("$.delays.{name}");
            if code_source(def).is_some() {
                return Err(invalid(&path, "a computed delay is { \"duration\": <expression> }"));
            }
            if let Some(spec) =
                def.get("duration").map(|d| self.delay_expr(d, &child_path(&path, "duration"))).transpose()?.flatten()
            {
                self.named_delays.insert(name.clone(), spec);
                continue;
            }
            let value = match def {
                Value::Object(o) => o
                    .get("duration")
                    .ok_or_else(|| invalid(&path, "expected a number, a duration string or { \"duration\": ... }"))?,
                other => other,
            };
            let ms = match value {
                Value::Number(n) => n.as_f64(),
                Value::String(s) => parse_duration_ms(s),
                _ => None,
            }
            .and_then(ms_to_u64)
            .ok_or_else(|| {
                invalid(
                    &path,
                    "invalid delay: expected milliseconds or a duration string such as \"500ms\", \"1.5s\" or \"PT1M\"",
                )
            })?;
            self.named_delays.insert(name.clone(), DelaySpec::Fixed(ms));
        }
        Ok(())
    }

    /// Builds the arena in pre-order, so a node's index is its document order.
    fn build_node(
        &mut self,
        json: &'j Map<String, Value>,
        key: String,
        parent: Option<NodeId>,
        path: String,
        key_path: &mut Vec<String>,
    ) -> Result<NodeId> {
        let is_root = parent.is_none();
        for k in json.keys() {
            let known = STATE_KEYS.contains(&k.as_str()) || (is_root && MACHINE_KEYS.contains(&k.as_str()));
            if !known {
                return Err(err(K::UnknownKey, &path, format!("unknown key \"{k}\"")));
            }
            if let Some((_, feature)) = UNSUPPORTED_STATE_KEYS.iter().find(|(name, _)| name == k) {
                return Err(err(K::Unsupported, &child_path(&path, k), format!("{feature} is not supported yet")));
            }
        }
        if !is_root && json.contains_key("context") {
            return Err(err(
                K::Unsupported,
                &child_path(&path, "context"),
                "context is only supported on the root node",
            ));
        }
        if !is_root && has_unescaped_dot(&key) {
            return Err(invalid(
                &path,
                format!("state key \"{key}\" contains \".\"; nest the states or escape it as \"\\\\.\""),
            ));
        }

        let states = opt_object(json, "states", &path)?;
        let has_children = states.is_some_and(|s| !s.is_empty());
        let kind = match opt_str(json, "type", &path)? {
            Some("atomic") => NodeKind::Atomic,
            Some("compound") => NodeKind::Compound,
            Some("parallel") => NodeKind::Parallel,
            Some("final") => NodeKind::Final,
            Some("history") => NodeKind::History { deep: false },
            Some("choice") => {
                return Err(err(K::Unsupported, &child_path(&path, "type"), "choice states are not supported yet"))
            }
            Some(other) => return Err(invalid(&child_path(&path, "type"), format!("unknown state type \"{other}\""))),
            None if has_children => NodeKind::Compound,
            None if json.contains_key("history") => NodeKind::History { deep: false },
            None => NodeKind::Atomic,
        };
        let kind = match kind {
            NodeKind::History { .. } => match json.get("history") {
                None | Some(Value::Bool(true)) => NodeKind::History { deep: false },
                Some(Value::String(s)) if s == "shallow" => NodeKind::History { deep: false },
                Some(Value::String(s)) if s == "deep" => NodeKind::History { deep: true },
                Some(_) => return Err(invalid(&child_path(&path, "history"), "expected \"shallow\" or \"deep\"")),
            },
            other => other,
        };
        if has_children && matches!(kind, NodeKind::Atomic | NodeKind::Final | NodeKind::History { .. }) {
            return Err(invalid(
                &child_path(&path, "states"),
                "only compound and parallel states can have child states",
            ));
        }
        if is_root && matches!(kind, NodeKind::Final | NodeKind::History { .. }) {
            return Err(invalid(&path, "the root state must be atomic, compound or parallel"));
        }
        if kind == NodeKind::Compound && !json.contains_key("initial") {
            return Err(invalid(&path, "no initial state specified for compound state"));
        }

        let id = match opt_str(json, "id", &path)? {
            Some(explicit) if !is_root => explicit.to_owned(),
            _ => {
                let mut parts: Vec<&str> = Vec::with_capacity(key_path.len() + 1);
                if let Some(mid) = &self.machine_id {
                    parts.push(mid);
                }
                parts.extend(key_path.iter().map(String::as_str));
                parts.join(".")
            }
        };
        let index = self.nodes.len() as NodeId;
        if self.id_map.insert(id.clone(), index).is_some() {
            return Err(err(K::DuplicateId, &path, format!("duplicate state id '{id}'; state ids must be unique")));
        }
        self.nodes.push(Node {
            key,
            id,
            parent,
            end: index + 1,
            kind,
            children: Vec::new(),
            history_children: Vec::new(),
            initial: None,
            history_target: Vec::new(),
            entry: Vec::new(),
            exit: Vec::new(),
            on: Vec::new(),
            always: Vec::new(),
            delayed: Vec::new(),
            invokes: Vec::new(),
            output: None,
            tags: Vec::new(),
        });
        self.raw.push(RawNode { json, path: path.clone() });

        if let Some(states) = states {
            for (child_key, child_json) in states {
                let child_path = format!("{path}.states.{child_key}");
                let child_json =
                    child_json.as_object().ok_or_else(|| invalid(&child_path, "a state node must be an object"))?;
                key_path.push(child_key.clone());
                let child = self.build_node(child_json, child_key.clone(), Some(index), child_path, key_path)?;
                key_path.pop();
                if matches!(self.nodes[child as usize].kind, NodeKind::History { .. }) {
                    self.nodes[index as usize].history_children.push(child);
                } else {
                    self.nodes[index as usize].children.push(child);
                }
            }
        }
        self.nodes[index as usize].end = self.nodes.len() as NodeId;
        Ok(index)
    }

    fn child_by_key(&self, node: NodeId, key: &str) -> Option<NodeId> {
        let n = &self.nodes[node as usize];
        n.children.iter().chain(&n.history_children).copied().find(|&c| self.nodes[c as usize].key == key)
    }

    /// `getStateNodeByPath`: `#id`, or a `.`-separated path below `base`.
    fn node_by_path(&self, base: NodeId, path: &str) -> std::result::Result<NodeId, String> {
        if let Some(id) = path.strip_prefix('#') {
            return self.id_map.get(id).copied().ok_or_else(|| format!("no state has id \"{id}\""));
        }
        let mut current = base;
        for segment in to_state_path(path) {
            if segment.is_empty() {
                break;
            }
            current = self.child_by_key(current, &segment).ok_or_else(|| {
                format!("child state '{segment}' does not exist on '{}'", self.nodes[current as usize].id)
            })?;
        }
        Ok(current)
    }

    /// Resolves a transition target relative to its source state.
    fn resolve_target(&self, source: NodeId, target: &str, path: &str) -> Result<NodeId> {
        let resolved = if target.starts_with('#') {
            self.node_by_path(source, target)
        } else if let Some(rest) = target.strip_prefix('.') {
            self.node_by_path(source, rest)
        } else {
            match self.nodes[source as usize].parent {
                Some(parent) => self.node_by_path(parent, target),
                None => Err(format!("invalid target \"{target}\" on the root; use \".child\" or \"#id\"")),
            }
        };
        resolved.map_err(|m| err(K::InvalidTarget, path, m))
    }

    fn resolve_targets(
        &self,
        source: NodeId,
        value: Option<&Value>,
        path: &str,
        base_parent: bool,
    ) -> Result<Vec<NodeId>> {
        let list: Vec<&str> = match value {
            None => return Ok(Vec::new()),
            Some(Value::String(s)) => vec![s.as_str()],
            Some(Value::Array(items)) => items
                .iter()
                .map(|v| v.as_str().ok_or_else(|| invalid(path, "a target must be a string")))
                .collect::<Result<_>>()?,
            Some(_) => return Err(invalid(path, "a target must be a string or an array of strings")),
        };
        let mut targets = Vec::with_capacity(list.len());
        for t in list {
            let node = if base_parent {
                // History defaults resolve against the history state's parent.
                let parent = self.nodes[source as usize].parent.expect("history states have a parent");
                self.node_by_path(parent, t).map_err(|m| err(K::InvalidTarget, path, m))?
            } else {
                self.resolve_target(source, t, path)?
            };
            targets.push(node);
        }
        self.assert_legal_target_set(&targets, path)?;
        Ok(targets)
    }

    fn assert_legal_target_set(&self, targets: &[NodeId], path: &str) -> Result<()> {
        for (i, &a) in targets.iter().enumerate() {
            for &b in &targets[i + 1..] {
                let lca = self.lca(a, b);
                let related = a == b || self.is_desc(a, b) || self.is_desc(b, a);
                if related || lca.map(|l| self.nodes[l as usize].kind) != Some(NodeKind::Parallel) {
                    return Err(err(
                        K::InvalidTarget,
                        path,
                        "invalid target set: multiple targets must be in different regions of a parallel state",
                    ));
                }
            }
        }
        Ok(())
    }

    fn is_desc(&self, node: NodeId, ancestor: NodeId) -> bool {
        node > ancestor && node < self.nodes[ancestor as usize].end
    }

    fn lca(&self, a: NodeId, b: NodeId) -> Option<NodeId> {
        let mut m = Some(a);
        while let Some(x) = m {
            if x == b || self.is_desc(b, x) {
                return Some(x);
            }
            m = self.nodes[x as usize].parent;
        }
        None
    }

    fn resolve_node(&mut self, node: NodeId) -> Result<()> {
        let json: &'j Map<String, Value> = self.raw[node as usize].json;
        let path = self.raw[node as usize].path.clone();
        let kind = self.nodes[node as usize].kind;

        match kind {
            NodeKind::Compound => {
                let initial_path = child_path(&path, "initial");
                let initial = json
                    .get("initial")
                    .and_then(Value::as_str)
                    .ok_or_else(|| invalid(&initial_path, "expected the key of a child state"))?;
                let target = self.node_by_path(node, initial).map_err(|m| err(K::InvalidTarget, &initial_path, m))?;
                if !self.is_desc(target, node) {
                    return Err(err(K::InvalidTarget, &initial_path, "the initial state must be a descendant"));
                }
                self.nodes[node as usize].initial = Some(target);
            }
            NodeKind::History { .. } => {
                let target_path = child_path(&path, "target");
                let has_target = match json.get("target") {
                    Some(Value::String(s)) => !s.trim().is_empty(),
                    Some(Value::Array(items)) => {
                        !items.is_empty() && items.iter().all(|t| t.as_str().is_some_and(|s| !s.trim().is_empty()))
                    }
                    _ => false,
                };
                if !has_target {
                    return Err(invalid(&path, "a history state must declare a non-empty target"));
                }
                let targets = self.resolve_targets(node, json.get("target"), &target_path, true)?;
                self.nodes[node as usize].history_target = targets;
            }
            _ => {}
        }

        // Entry and exit actions.
        let entry = self.compile_action_list(json.get("entry"), &child_path(&path, "entry"), true)?;
        let exit = self.compile_action_list(json.get("exit"), &child_path(&path, "exit"), true)?;

        // Event transitions, in the order XState's `formatTransitions` builds
        // its transition map: `on` keys; `onDone` (appended to
        // `xstate.done.state`); `onError` (replacing `xstate.error.*`); each
        // invoke's completion transitions; then the delayed transitions.
        let mut by_descriptor: Vec<(String, Vec<TransitionId>)> = Vec::new();
        let add = |list: &mut Vec<(String, Vec<TransitionId>)>, d: &str, ts: Vec<TransitionId>| match list
            .iter_mut()
            .find(|(k, _)| k == d)
        {
            Some((_, existing)) => existing.extend(ts),
            None => list.push((d.to_owned(), ts)),
        };
        if let Some(on) = opt_object(json, "on", &path)? {
            for (descriptor, value) in on {
                let tpath = format!("{path}.on.{descriptor}");
                if descriptor.is_empty() {
                    return Err(invalid(&tpath, "empty event descriptor; use `always` for eventless transitions"));
                }
                let ts = self.compile_transitions(node, value, &tpath, &[])?;
                add(&mut by_descriptor, descriptor, ts);
            }
        }
        if let Some(value) = json.get("onDone") {
            let id = Value::String(self.nodes[node as usize].id.clone());
            let ts = self.compile_transitions(node, value, &child_path(&path, "onDone"), &[("stateId".into(), id)])?;
            add(&mut by_descriptor, DONE_STATE, ts);
        }
        if let Some(value) = json.get("onError") {
            let ts = self.compile_transitions(node, value, &child_path(&path, "onError"), &[])?;
            match by_descriptor.iter_mut().find(|(k, _)| k == "xstate.error.*") {
                Some((_, existing)) => *existing = ts,
                None => by_descriptor.push(("xstate.error.*".into(), ts)),
            }
        }
        let state_id = Value::String(self.nodes[node as usize].id.clone());
        let mut invokes = Vec::new();
        let mut invoke_timeouts = Vec::new();
        if let Some(value) = json.get("invoke") {
            let (items, many) = match value {
                Value::Array(items) => (items.iter().collect::<Vec<_>>(), true),
                single => (vec![single], false),
            };
            for (index, item) in items.into_iter().enumerate() {
                let ipath = if many { format!("{path}.invoke[{index}]") } else { child_path(&path, "invoke") };
                let (def, timeout) = self.compile_invoke(node, item, index, &ipath, &mut by_descriptor)?;
                if let Some(timeout) = timeout {
                    invoke_timeouts.push((def.id.clone(), timeout));
                }
                invokes.push(def);
            }
        }
        let mut delayed = Vec::new();
        let mut delayed_transitions: Vec<(&str, Vec<TransitionId>)> = Vec::new();
        if let Some(after) = opt_object(json, "after", &path)? {
            for (key, value) in after {
                let apath = format!("{path}.after.{key}");
                let (event_delay, delay) = self.after_delay(key, &apath)?;
                let extra = [("delay".to_owned(), event_delay.clone()), ("stateId".to_owned(), state_id.clone())];
                let ts = self.compile_transitions(node, value, &apath, &extra)?;
                delayed_transitions.push((AFTER, ts));
                delayed.push(DelayedDef { kind: DelayedKind::After { event_delay }, delay });
            }
        }
        match (json.get("timeout"), json.get("onTimeout")) {
            (None, None) => {}
            (Some(timeout), Some(on_timeout)) => {
                let tpath = child_path(&path, "timeout");
                let computed = self.delay_expr(timeout, &tpath)?;
                let delay = match timeout {
                    _ if computed.is_some() => computed,
                    Value::Number(n) => n.as_f64().and_then(ms_to_u64).map(DelaySpec::Fixed),
                    Value::String(s) => self
                        .named_delay(s)
                        .or_else(|| parse_duration_ms(s).and_then(ms_to_u64).map(DelaySpec::Fixed))
                        .or_else(|| self.stub_delay(s, &tpath)),
                    _ => None,
                }
                .ok_or_else(|| {
                    invalid(&tpath, "invalid timeout: expected milliseconds, a named delay or a duration string")
                })?;
                let extra = [("stateId".to_owned(), state_id.clone())];
                let ts = self.compile_transitions(node, on_timeout, &child_path(&path, "onTimeout"), &extra)?;
                delayed_transitions.push(("xstate.timeout.state", ts));
                delayed.push(DelayedDef { kind: DelayedKind::StateTimeout, delay });
            }
            (Some(_), None) => return Err(invalid(&path, "a state with `timeout` must declare `onTimeout`")),
            (None, Some(_)) => return Err(invalid(&path, "a state with `onTimeout` must declare `timeout`")),
        }
        for (invoke_id, (delay, ts)) in invoke_timeouts {
            delayed_transitions.push(("xstate.timeout.actor", ts));
            delayed.push(DelayedDef { kind: DelayedKind::InvokeTimeout { invoke_id }, delay });
        }
        for (descriptor, ts) in delayed_transitions {
            add(&mut by_descriptor, descriptor, ts);
        }
        if node == 0 {
            // `formatRouteTransitions`: the root's `xstate.route` transitions,
            // set after its other transitions (replacing any of its own).
            let routes = self.compile_routes()?;
            if !routes.is_empty() {
                match by_descriptor.iter_mut().find(|(k, _)| k == "xstate.route") {
                    Some((_, existing)) => *existing = routes,
                    None => by_descriptor.push(("xstate.route".into(), routes)),
                }
            }
        }
        let always = match json.get("always") {
            Some(value) => self.compile_transitions(node, value, &child_path(&path, "always"), &[])?,
            None => Vec::new(),
        };

        let tags = match json.get("tags") {
            None => Vec::new(),
            Some(Value::Array(items)) => items
                .iter()
                .map(|t| {
                    t.as_str()
                        .map(str::to_owned)
                        .ok_or_else(|| invalid(&child_path(&path, "tags"), "tags must be strings"))
                })
                .collect::<Result<_>>()?,
            Some(_) => return Err(invalid(&child_path(&path, "tags"), "expected an array of strings")),
        };
        let output = match json.get("output") {
            Some(o) if kind == NodeKind::Final => {
                Some(self.compile_template(o, &child_path(&path, "output"), Slot::Scope)?)
            }
            _ => None,
        };

        let n = &mut self.nodes[node as usize];
        n.entry = entry;
        n.exit = exit;
        n.on = by_descriptor;
        n.always = always;
        n.delayed = delayed;
        n.invokes = invokes;
        n.tags = tags;
        n.output = output;
        Ok(())
    }

    /// Resolves an `after` key: a number of milliseconds, a named delay, or a
    /// duration string.
    fn after_delay(&mut self, key: &str, path: &str) -> Result<(Value, DelaySpec)> {
        if let Ok(ms) = key.trim().parse::<f64>() {
            let spec =
                ms_to_u64(ms).ok_or_else(|| invalid(path, "a delay must be a non-negative number of milliseconds"))?;
            let event_delay = if ms.fract() == 0.0 { Value::from(ms as u64) } else { Value::from(ms) };
            return Ok((event_delay, DelaySpec::Fixed(spec)));
        }
        let spec = self
            .named_delay(key)
            .or_else(|| parse_duration_ms(key).and_then(ms_to_u64).map(DelaySpec::Fixed))
            .or_else(|| self.stub_delay(key, path));
        match spec {
            Some(spec) => Ok((Value::String(key.to_owned()), spec)),
            None => Err(err(K::MissingImplementation, path, format!("missing delay source \"{key}\""))),
        }
    }

    /// Records a missing reference when collecting [`References`]; returns
    /// whether the caller should continue with a stub.
    fn record(
        &mut self,
        kind: fn(&mut References) -> &mut Vec<Reference>,
        name: &str,
        path: &str,
        params: Option<&Value>,
    ) -> bool {
        match &mut self.refs {
            Some(refs) => {
                kind(refs).push(Reference { name: name.to_owned(), path: path.to_owned(), params: params.cloned() });
                true
            }
            None => false,
        }
    }

    fn stub_delay(&mut self, name: &str, path: &str) -> Option<DelaySpec> {
        self.record(|r| &mut r.delays, name, path, None).then_some(DelaySpec::Fixed(0))
    }

    fn named_delay(&mut self, name: &str) -> Option<DelaySpec> {
        if let Some(spec) = self.named_delays.get(name) {
            return Some(*spec);
        }
        let factory = self.registry.delays.get(name)?;
        let f = factory(&Value::Null).ok()?;
        let spec = DelaySpec::Named(self.delays.len() as u32);
        self.delays.push(f);
        self.named_delays.insert(name.to_owned(), spec);
        Some(spec)
    }

    /// One `invoke`: its definition, and its timeout (delay and `onTimeout`
    /// transitions) if it has one. Completion transitions are added to
    /// `by_descriptor` as XState's `formatTransitions` adds them.
    #[allow(clippy::type_complexity)]
    fn compile_invoke(
        &mut self,
        node: NodeId,
        value: &Value,
        index: usize,
        path: &str,
        by_descriptor: &mut Vec<(String, Vec<TransitionId>)>,
    ) -> Result<(InvokeDef, Option<(DelaySpec, Vec<TransitionId>)>)> {
        const INVOKE_KEYS: &[&str] =
            &["id", "registryKey", "src", "input", "onDone", "onError", "onSnapshot", "timeout", "onTimeout"];
        let obj = value.as_object().ok_or_else(|| invalid(path, "an invoke must be an object"))?;
        for k in obj.keys() {
            if !INVOKE_KEYS.contains(&k.as_str()) {
                return Err(err(K::UnknownKey, path, format!("unknown key \"{k}\"")));
            }
        }
        let src = opt_str(obj, "src", path)?
            .ok_or_else(|| invalid(&child_path(path, "src"), "an invoke needs a string `src`"))?;
        let logic = match self.registry.actors.iter().find(|(name, _)| name == src) {
            Some((_, l)) => l.clone(),
            None if self.record(|r| &mut r.actors, src, &child_path(path, "src"), obj.get("input")) => {
                Arc::new(crate::step::async_logic(None, None).expect("no timeout")) as Arc<dyn crate::system::Logic>
            }
            None => {
                return Err(err(
                    K::MissingImplementation,
                    &child_path(path, "src"),
                    format!("missing actor source \"{src}\""),
                ))
            }
        };
        let id = match opt_str(obj, "id", path)? {
            Some(id) => id.to_owned(),
            None => format!("{}:{index}", self.nodes[node as usize].id),
        };
        let registry_key = opt_str(obj, "registryKey", path)?.map(str::to_owned);
        let timeout_key =
            crate::system::TimerKey::Event(Box::new(json!({ "type": "xstate.timeout.actor", "actorId": id })));
        let timeout = match (obj.get("timeout"), obj.get("onTimeout")) {
            (None, None) => None,
            (Some(timeout), Some(_)) => {
                let computed = self.delay_expr(timeout, &child_path(path, "timeout"))?;
                let delay = match timeout {
                    _ if computed.is_some() => computed,
                    Value::Number(n) => n.as_f64().and_then(ms_to_u64).map(DelaySpec::Fixed),
                    Value::String(s) => parse_duration_ms(s).and_then(ms_to_u64).map(DelaySpec::Fixed),
                    _ => None,
                }
                .ok_or_else(|| {
                    invalid(&child_path(path, "timeout"), "invalid timeout: expected milliseconds or a duration string")
                })?;
                Some(delay)
            }
            (Some(_), None) => return Err(invalid(path, "an invoke with `timeout` must declare `onTimeout`")),
            (None, Some(_)) => return Err(invalid(path, "an invoke with `onTimeout` must declare `timeout`")),
        };
        let actor_match = [("actorId".to_owned(), Value::String(id.clone()))];
        let add = |list: &mut Vec<(String, Vec<TransitionId>)>, d: &str, ts: Vec<TransitionId>| match list
            .iter_mut()
            .find(|(k, _)| k == d)
        {
            Some((_, existing)) => existing.extend(ts),
            None => list.push((d.to_owned(), ts)),
        };
        let completion = |this: &mut Self, value: &Value, p: String| -> Result<Vec<TransitionId>> {
            let ts = this.compile_transitions(node, value, &p, &actor_match)?;
            for &t in &ts {
                let tr = &mut this.transitions[t as usize];
                tr.session = Some(id.clone());
                if timeout.is_some() {
                    tr.cancel_timeout = Some(timeout_key.clone());
                }
            }
            Ok(ts)
        };
        let cancel_only = |this: &mut Self| -> TransitionId {
            this.transitions.push(TransitionData {
                targets: None,
                matches: vec![("actorId".to_owned(), Value::String(id.clone()))],
                guard: None,
                context: None,
                context_expr: None,
                actions: Vec::new(),
                reenter: false,
                session: Some(id.clone()),
                cancel_timeout: Some(timeout_key.clone()),
            });
            this.transition_sources.push(node);
            this.transitions.len() as TransitionId - 1
        };
        if let Some(value) = obj.get("onDone") {
            let mut ts = completion(self, value, child_path(path, "onDone"))?;
            if timeout.is_some() {
                ts.push(cancel_only(self));
            }
            add(by_descriptor, "xstate.done.actor", ts);
        } else if timeout.is_some() {
            let t = cancel_only(self);
            add(by_descriptor, "xstate.done.actor", vec![t]);
        }
        if let Some(value) = obj.get("onError") {
            let ts = completion(self, value, child_path(path, "onError"))?;
            add(by_descriptor, "xstate.error.actor", ts);
        }
        if let Some(value) = obj.get("onSnapshot") {
            let ts = self.compile_transitions(node, value, &child_path(path, "onSnapshot"), &actor_match)?;
            for &t in &ts {
                self.transitions[t as usize].session = Some(id.clone());
            }
            add(by_descriptor, "xstate.snapshot.actor", ts);
        }
        let timeout = match (timeout, obj.get("onTimeout")) {
            (Some(delay), Some(on_timeout)) => {
                let ts = self.compile_transitions(node, on_timeout, &child_path(path, "onTimeout"), &actor_match)?;
                for &t in &ts {
                    self.transitions[t as usize].session = Some(id.clone());
                }
                Some((delay, ts))
            }
            _ => None,
        };
        let input = match obj.get("input") {
            Some(i) => Some(self.compile_template(i, &child_path(path, "input"), Slot::Scope)?),
            None => None,
        };
        let def = InvokeDef { id, logic, input, registry_key, sync_snapshot: obj.contains_key("onSnapshot") };
        Ok((def, timeout))
    }

    /// One transition on the root per routable state (explicit `id` and a
    /// `route`), in document order: `{ ...route, target: '#id' }`, enabled
    /// when the event's `to` is `#id` (and the route's named `guard`, if any,
    /// passes).
    fn compile_routes(&mut self) -> Result<Vec<TransitionId>> {
        let mut out = Vec::new();
        for n in 1..self.nodes.len() {
            let json = self.raw[n].json;
            let Some(route) = json.get("route") else { continue };
            let Some(Value::String(id)) = json.get("id") else { continue };
            let path = child_path(&self.raw[n].path, "route");
            if code_source(route).is_some() {
                return Err(err(K::Unsupported, &path, "computed routes (a route expression) are not supported"));
            }
            let mut config = match route {
                Value::Null | Value::Bool(false) => continue,
                Value::Bool(true) => Map::new(),
                Value::Object(o) => o.clone(),
                _ => return Err(invalid(&path, "a route must be an object (or a code expression)")),
            };
            for k in config.keys() {
                if !["context", "reenter", "meta", "description", "input", "guard"].contains(&k.as_str()) {
                    return Err(err(K::UnknownKey, &path, format!("unknown key \"{k}\"")));
                }
            }
            if config.contains_key("input") {
                return Err(err(
                    K::Unsupported,
                    &child_path(&path, "input"),
                    "route input (state input) is not supported yet",
                ));
            }
            let named = match config.remove("guard") {
                None | Some(Value::Null) => None,
                Some(Value::String(g)) => Some(g),
                Some(_) => return Err(invalid(&child_path(&path, "guard"), "a route guard is the name of a guard")),
            };
            let to = format!("#{id}");
            config.insert("target".into(), Value::String(to.clone()));
            let t = self.compile_transition(0, &Value::Object(config), &path, &[])?;
            let mut guard = Guard::RouteTo(to);
            if let Some(name) = named {
                let named =
                    self.compile_guard(&json!({ "type": name }), &child_path(&path, "guard"), &mut Vec::new())?;
                guard = Guard::And(Box::new(guard), Box::new(named));
            }
            self.transitions[t as usize].guard = Some(guard);
            out.push(t);
        }
        Ok(out)
    }

    fn compile_transitions(
        &mut self,
        source: NodeId,
        value: &Value,
        path: &str,
        extra_matches: &[(String, Value)],
    ) -> Result<Vec<TransitionId>> {
        match value {
            Value::Array(items) => items
                .iter()
                .enumerate()
                .map(|(i, t)| self.compile_transition(source, t, &format!("{path}[{i}]"), extra_matches))
                .collect(),
            single => Ok(vec![self.compile_transition(source, single, path, extra_matches)?]),
        }
    }

    fn compile_transition(
        &mut self,
        source: NodeId,
        value: &Value,
        path: &str,
        extra_matches: &[(String, Value)],
    ) -> Result<TransitionId> {
        if code_source(value).is_some() {
            return Err(err(
                K::Unsupported,
                path,
                "transition functions (a transition expression) are not supported; compute values in the \
                 transition's `context`, `actions` or `guard` instead",
            ));
        }
        let obj = match value {
            Value::Object(o) => o,
            Value::String(s) => {
                return Err(invalid(
                    path,
                    format!(
                        "invalid transition: use {{ \"target\": {} }} instead of a string",
                        Value::String(s.clone())
                    ),
                ))
            }
            _ => return Err(invalid(path, "invalid transition: expected an object")),
        };
        for k in obj.keys() {
            if !TRANSITION_KEYS.contains(&k.as_str()) {
                return Err(err(K::UnknownKey, path, format!("unknown key \"{k}\"")));
            }
        }
        if obj.contains_key("input") {
            return Err(err(
                K::Unsupported,
                &child_path(path, "input"),
                "transition input (state input) is not supported yet",
            ));
        }
        let targets = match obj.get("target") {
            Some(t) => Some(self.resolve_targets(source, Some(t), &child_path(path, "target"), false)?.into()),
            None => None,
        };
        if let Some(m) = obj.get("matches") {
            reject_code(m, &child_path(path, "matches"), "`matches` patterns")?;
        }
        let mut matches: Vec<(String, Value)> = match opt_object(obj, "matches", path)? {
            Some(m) => m.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
            None => Vec::new(),
        };
        for (k, v) in extra_matches {
            match matches.iter_mut().find(|(mk, _)| mk == k) {
                Some(entry) => entry.1 = v.clone(),
                None => matches.push((k.clone(), v.clone())),
            }
        }
        let guard = match obj.get("guard") {
            Some(g) => Some(self.compile_guard(g, &child_path(path, "guard"), &mut Vec::new())?),
            None => None,
        };
        let (context, context_expr) = match obj.get("context") {
            Some(c) if contains_code(c) => {
                (None, Some(self.compile_template(c, &child_path(path, "context"), Slot::Scope)?))
            }
            _ => (opt_object(obj, "context", path)?.cloned(), None),
        };
        let actions = self.compile_action_list(obj.get("actions"), &child_path(path, "actions"), false)?;
        let reenter = match obj.get("reenter") {
            None => false,
            Some(Value::Bool(b)) => *b,
            Some(_) => return Err(invalid(&child_path(path, "reenter"), "expected a boolean")),
        };
        let id = self.transitions.len() as TransitionId;
        self.transitions.push(TransitionData {
            targets,
            matches,
            guard,
            context,
            context_expr,
            actions,
            reenter,
            session: None,
            cancel_timeout: None,
        });
        self.transition_sources.push(source);
        Ok(id)
    }

    fn compile_guard(&mut self, value: &Value, path: &str, stack: &mut Vec<String>) -> Result<Guard> {
        if let Some(name) = code_source(value) {
            self.expr_refs += 1;
            return Ok(Guard::Expr(self.expr(name, path, Slot::Scope)?));
        }
        let obj =
            value.as_object().ok_or_else(|| invalid(path, "a guard must be { \"type\": ..., \"params\"?: ... }"))?;
        let ty = opt_str(obj, "type", path)?.ok_or_else(|| invalid(path, "a guard must have a string \"type\""))?;
        let params = obj.get("params").unwrap_or(&Value::Null);
        let params_path = format!("{path}.params");
        match ty {
            "@xstate.not" => {
                let inner =
                    params.get("guard").ok_or_else(|| invalid(&format!("{path}.params.guard"), "missing guard"))?;
                Ok(Guard::Not(Box::new(self.compile_guard(inner, &format!("{path}.params.guard"), stack)?)))
            }
            "@xstate.stateIn" => {
                let spath = format!("{path}.params.stateId");
                reject_code(params, &params_path, "`@xstate.stateIn` params")?;
                let id =
                    params.get("stateId").and_then(Value::as_str).ok_or_else(|| invalid(&spath, "missing state id"))?;
                let id = id.strip_prefix('#').unwrap_or(id);
                let node = self
                    .id_map
                    .get(id)
                    .copied()
                    .ok_or_else(|| err(K::InvalidTarget, &spath, format!("unknown state id \"{id}\"")))?;
                Ok(Guard::StateIn(node))
            }
            _ => {
                if let Some(def) = self.root_json.get("guards").and_then(|g| g.get(ty)) {
                    if stack.iter().any(|s| s == ty) {
                        return Err(invalid(path, format!("circular guard reference: {} -> {ty}", stack.join(" -> "))));
                    }
                    // The guard's params are its expressions' scope; they
                    // are resolved only when an expression reads them.
                    let scope = match obj.get("params") {
                        Some(p) => Some(self.compile_template(p, &params_path, Slot::Scope)?),
                        None => None,
                    };
                    let before = self.expr_refs;
                    stack.push(ty.to_owned());
                    let when = def.get("when").expect("checked in check_machine_keys");
                    let guard = self.compile_guard(when, &format!("$.guards.{ty}.when"), stack);
                    stack.pop();
                    let guard = guard?;
                    return Ok(match scope {
                        Some(scope) if self.expr_refs > before => Guard::Scoped(Box::new(scope), Box::new(guard)),
                        _ => guard,
                    });
                }
                if let Some(factory) = self.registry.guards.get(ty) {
                    if contains_code(params) {
                        let template = self.compile_template(params, &params_path, Slot::Scope)?;
                        self.dynamic_guards.push(Dynamic {
                            name: ty.to_owned(),
                            factory: factory.clone(),
                            params: template,
                        });
                        return Ok(Guard::Dynamic(self.dynamic_guards.len() as u32 - 1));
                    }
                    let f = factory(params).map_err(|m| {
                        err(K::InvalidParams, &params_path, format!("invalid params for guard '{ty}': {m}"))
                    })?;
                    self.guards.push(f);
                    return Ok(Guard::Custom(self.guards.len() as u32 - 1));
                }
                if ty.starts_with("@xstate.") {
                    return Err(err(K::Unsupported, path, format!("unknown built-in guard \"{ty}\"")));
                }
                if self.record(|r| &mut r.guards, ty, path, obj.get("params")) {
                    if contains_code(params) {
                        self.compile_template(params, &params_path, Slot::Scope)?;
                    }
                    self.guards.push(Box::new(|_| false));
                    return Ok(Guard::Custom(self.guards.len() as u32 - 1));
                }
                Err(err(K::MissingImplementation, path, format!("missing guard source \"{ty}\"")))
            }
        }
    }

    fn compile_action_list(&mut self, value: Option<&Value>, path: &str, allow_single: bool) -> Result<Vec<Action>> {
        let mut out = Vec::new();
        match value {
            None => {}
            Some(Value::Array(items)) => {
                for (i, a) in items.iter().enumerate() {
                    self.compile_action(a, &format!("{path}[{i}]"), &mut Vec::new(), &mut out)?;
                }
            }
            Some(single @ Value::Object(_)) if allow_single => {
                self.compile_action(single, &format!("{path}[0]"), &mut Vec::new(), &mut out)?;
            }
            Some(_) => return Err(invalid(path, "expected an array of actions")),
        }
        Ok(out)
    }

    fn compile_action(
        &mut self,
        value: &Value,
        path: &str,
        stack: &mut Vec<String>,
        out: &mut Vec<Action>,
    ) -> Result<()> {
        if let Some(name) = code_source(value) {
            self.expr_refs += 1;
            out.push(Action::Expr(self.expr(name, path, Slot::Scope)?));
            return Ok(());
        }
        let obj = value.as_object().ok_or_else(|| invalid(path, "invalid action: expected { \"type\": ... }"))?;
        let ty = opt_str(obj, "type", path)?.ok_or_else(|| invalid(path, "invalid action: missing string \"type\""))?;
        let event_object = |key: &str| -> Result<Value> {
            match obj.get(key) {
                Some(e @ Value::Object(o)) if o.get("type").is_some_and(Value::is_string) => Ok(e.clone()),
                _ => Err(invalid(&child_path(path, key), "expected an event object with a string \"type\"")),
            }
        };
        // A computed event: a code node, or an object with code in it.
        let computed_event = |this: &mut Self, key: &str| -> Result<Option<Template>> {
            match obj.get(key) {
                Some(e) if contains_code(e) => {
                    if let Some(t) = e.get("type").filter(|t| !t.is_string() && code_source(t).is_none()) {
                        return Err(invalid(
                            &child_path(path, key),
                            format!("an event's \"type\" must be a string, not {t}"),
                        ));
                    }
                    this.compile_template(e, &child_path(path, key), Slot::Scope).map(Some)
                }
                _ => Ok(None),
            }
        };
        match ty {
            "@xstate.raise" => {
                let computed = computed_event(self, "event")?;
                let event = match &computed {
                    Some(_) => Value::Null,
                    None => event_object("event")?,
                };
                let id = opt_str(obj, "id", path)?.map(str::to_owned);
                let delay =
                    match obj.get("delay") {
                        None => None,
                        Some(Value::Number(n)) => Some(n.as_f64().and_then(ms_to_u64).ok_or_else(|| {
                            invalid(&child_path(path, "delay"), "a delay must be a non-negative number")
                        })?),
                        Some(_) => {
                            return Err(invalid(&child_path(path, "delay"), "a delay must be a number of milliseconds"))
                        }
                    };
                if let Some(event) = computed {
                    out.push(Action::RaiseExpr { event, id, delay });
                    return Ok(());
                }
                let ev = Ev::<E>::from_json(event).expect("checked to have a string type");
                self.raise_events.push(ev);
                out.push(Action::Raise { event: self.raise_events.len() as u32 - 1, id, delay });
            }
            "@xstate.cancel" => {
                let id = opt_str(obj, "id", path)?.ok_or_else(|| invalid(&child_path(path, "id"), "missing id"))?;
                out.push(Action::Cancel(id.to_owned()));
            }
            "@xstate.log" => {
                if let Some(args) = obj.get("args") {
                    reject_code(args, &child_path(path, "args"), "`@xstate.log` args")?;
                }
                let args = match obj.get("args") {
                    None => Vec::new(),
                    Some(Value::Array(a)) => a.clone(),
                    Some(_) => return Err(invalid(&child_path(path, "args"), "expected an array")),
                };
                out.push(Action::Log(args));
            }
            "@xstate.emit" => match computed_event(self, "event")? {
                Some(event) => out.push(Action::EmitExpr(event)),
                None => out.push(Action::Emit(event_object("event")?)),
            },
            "@xstate.assign" => {
                if let Some(c) = obj.get("context").filter(|c| contains_code(c)) {
                    let t = self.compile_template(c, &child_path(path, "context"), Slot::Scope)?;
                    out.push(Action::AssignExpr(t));
                    return Ok(());
                }
                let patch = opt_object(obj, "context", path)?
                    .ok_or_else(|| invalid(&child_path(path, "context"), "missing context patch"))?;
                out.push(Action::Assign(patch.clone()));
            }
            _ if ty.starts_with("@xstate.") => {
                return Err(err(K::Unsupported, path, format!("unsupported built-in action \"{ty}\"")));
            }
            _ => {
                if let Some(def) = self.root_json.get("actions").and_then(|a| a.get(ty)) {
                    if stack.iter().any(|s| s == ty) {
                        return Err(invalid(
                            path,
                            format!("circular action reference: {} -> {ty}", stack.join(" -> ")),
                        ));
                    }
                    // The reference's params (resolved where it runs) are the
                    // scope of the expressions in the definition; they are
                    // resolved only when one of those reads them.
                    let scope = match obj.get("params") {
                        Some(p) => Some(self.compile_template(p, &child_path(path, "params"), Slot::Scope)?),
                        None => None,
                    };
                    let (start, before) = (out.len(), self.expr_refs);
                    stack.push(ty.to_owned());
                    let items: Vec<&Value> = match def {
                        Value::Array(items) => items.iter().collect(),
                        single => vec![single],
                    };
                    let many = items.len() > 1;
                    for (i, item) in items.into_iter().enumerate() {
                        let ipath = if many { format!("$.actions.{ty}[{i}]") } else { format!("$.actions.{ty}") };
                        self.compile_action(item, &ipath, stack, out)?;
                    }
                    stack.pop();
                    if let Some(scope) = scope.filter(|_| self.expr_refs > before) {
                        out.insert(start, Action::Params(scope));
                        out.push(Action::EndParams);
                    }
                    return Ok(());
                }
                let params = obj.get("params").unwrap_or(&Value::Null);
                if contains_code(params) {
                    let template = self.compile_template(params, &child_path(path, "params"), Slot::Scope)?;
                    let factory = match self.registry.actions.get(ty) {
                        Some(factory) => factory.clone(),
                        None if self.record(|r| &mut r.actions, ty, path, obj.get("params")) => {
                            Arc::new(|_: &Value| Ok(Arc::new(|| {}) as crate::registry::ActionFn))
                        }
                        None => {
                            return Err(err(K::MissingImplementation, path, format!("missing action source \"{ty}\"")))
                        }
                    };
                    self.dynamic_actions.push(Dynamic { name: ty.to_owned(), factory, params: template });
                    out.push(Action::CustomExpr(self.dynamic_actions.len() as u32 - 1));
                    return Ok(());
                }
                let f = match self.registry.actions.get(ty) {
                    Some(factory) => factory(params).map_err(|m| {
                        err(
                            K::InvalidParams,
                            &child_path(path, "params"),
                            format!("invalid params for action '{ty}': {m}"),
                        )
                    })?,
                    None if self.record(|r| &mut r.actions, ty, path, obj.get("params")) => Arc::new(|| {}),
                    None => return Err(err(K::MissingImplementation, path, format!("missing action source \"{ty}\""))),
                };
                self.actions.push(CustomAction { name: ty.to_owned(), params: obj.get("params").cloned(), exec: f });
                out.push(Action::Custom(self.actions.len() as u32 - 1));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn state_paths() {
        assert_eq!(to_state_path("a.b"), vec!["a", "b"]);
        assert_eq!(to_state_path("a\\.b.c"), vec!["a\\.b", "c"]);
        assert!(has_unescaped_dot("a.b"));
        assert!(!has_unescaped_dot("a\\.b"));
    }
}
