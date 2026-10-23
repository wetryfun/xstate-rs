//! The checks `generate` runs before emitting code, and the schema shapes
//! it rejects.

use serde_json::{json, Value};
use xstate_codegen::generate;

fn bundle(machine: Value, schemas: Value) -> Value {
    json!({ "machine": machine, "schemas": schemas })
}

fn simple(on: Value) -> Value {
    json!({ "id": "m", "initial": "a", "states": { "a": { "on": on }, "b": {} } })
}

fn error(b: Value) -> (String, String) {
    let e = generate(&b, "m").expect_err("expected an error");
    (e.path, e.message)
}

const OBJECT: &str = r#"{ "type": "object", "properties": {}, "additionalProperties": false }"#;

fn obj() -> Value {
    serde_json::from_str(OBJECT).unwrap()
}

#[test]
fn guards_and_actions_need_schema_entries() {
    let m = simple(json!({ "go": { "target": "b", "guard": { "type": "ok" } } }));
    let (path, message) = error(bundle(m, json!({ "events": { "go": obj() } })));
    assert_eq!(path, "$.states.a.on.go.guard");
    assert!(message.contains("schemas.guards"), "{message}");

    let m = simple(json!({ "go": { "actions": [{ "type": "track" }] } }));
    let (path, message) = error(bundle(m, json!({ "events": { "go": obj() } })));
    assert_eq!(path, "$.states.a.on.go.actions[0]");
    assert!(message.contains("schemas.actions"), "{message}");
}

#[test]
fn params_are_checked_against_their_schema() {
    let params = json!({ "type": "object", "properties": { "max": { "type": "integer" } }, "required": ["max"] });
    let schemas = json!({ "events": { "go": obj() }, "guards": { "below": { "params": params } } });
    let guard = |p: Value| simple(json!({ "go": { "guard": { "type": "below", "params": p } } }));
    assert!(generate(&bundle(guard(json!({ "max": 3 })), schemas.clone()), "m").is_ok());
    let (path, message) = error(bundle(guard(json!({ "max": 1.5 })), schemas.clone()));
    assert_eq!(path, "$.states.a.on.go.guard.params.max");
    assert!(message.contains("expected integer"), "{message}");
    let (path, message) = error(bundle(guard(json!({})), schemas));
    assert_eq!(path, "$.states.a.on.go.guard.params");
    assert!(message.contains("missing required property `max`"), "{message}");

    let m = simple(json!({ "go": { "guard": { "type": "plain", "params": { "x": 1 } } } }));
    let (_, message) = error(bundle(m, json!({ "events": { "go": obj() }, "guards": { "plain": {} } })));
    assert!(message.contains("takes no params"), "{message}");
}

#[test]
fn events_must_be_declared() {
    let m = simple(json!({ "go": { "target": "b" }, "user.*": {}, "xstate.done.state": {} }));
    let (path, message) = error(bundle(m.clone(), json!({ "events": { "stop": obj() } })));
    assert_eq!(path, "$.states.a.on.go");
    assert!(message.contains("not declared"), "{message}");
    assert!(generate(&bundle(m, json!({ "internalEvents": { "go": obj() } })), "m").is_ok());

    let raise = simple(json!({ "go": { "actions": [{ "type": "@xstate.raise", "event": { "type": "later" } }] } }));
    let (path, _) = error(bundle(raise, json!({ "events": { "go": obj() } })));
    assert_eq!(path, "$.states.a.on.go.actions[0].event");

    let emit =
        simple(json!({ "go": { "actions": [{ "type": "@xstate.emit", "event": { "type": "done", "n": "x" } }] } }));
    let (path, message) = error(bundle(emit.clone(), json!({ "events": { "go": obj() } })));
    assert_eq!(path, "$.states.a.on.go.actions[0].event");
    assert!(message.contains("schemas.emitted"), "{message}");
    let n = json!({ "type": "object", "properties": { "n": { "type": "number" } }, "required": ["n"] });
    let (path, _) = error(bundle(emit, json!({ "events": { "go": obj() }, "emitted": { "done": n } })));
    assert_eq!(path, "$.states.a.on.go.actions[0].event.n");
}

#[test]
fn context_and_patches_must_fit_the_context_schema() {
    let schema = json!({
        "type": "object",
        "properties": { "count": { "type": "integer" } },
        "required": ["count"],
        "additionalProperties": false
    });
    let machine = |patch: Value| {
        json!({
            "id": "m", "initial": "a", "context": { "count": 0 },
            "states": { "a": {
                "entry": [{ "type": "@xstate.assign", "context": patch }],
                "on": { "go": { "context": { "count": 1 } } }
            } }
        })
    };
    let schemas = json!({ "context": schema, "events": { "go": obj() } });
    assert!(generate(&bundle(machine(json!({ "count": 2 })), schemas.clone()), "m").is_ok());
    let (path, _) = error(bundle(machine(json!({ "count": "x" })), schemas.clone()));
    assert_eq!(path, "$.states.a.entry[0].context.count");
    let (path, message) = error(bundle(machine(json!({ "other": 1 })), schemas));
    assert_eq!(path, "$.states.a.entry[0].context.other");
    assert!(message.contains("unknown property"), "{message}");

    let (path, _) = error(bundle(machine(json!({})), json!({ "events": { "go": obj() } })));
    assert_eq!(path, "schemas.context");
}

#[test]
fn definition_errors_come_from_the_runtime_loader() {
    let m = simple(json!({ "go": { "@code": "() => {}", "@lang": "ts" } }));
    let (path, message) = error(bundle(m, json!({})));
    assert_eq!(path, "$.states.a.on.go");
    assert!(message.contains("@code"), "{message}");
    let m = json!({ "id": "m", "initial": "nope", "states": { "a": {} } });
    assert!(generate(&bundle(m, json!({})), "m").is_err());
}

#[test]
fn unsupported_schema_shapes_are_rejected() {
    let with_context = |prop: Value| {
        let schema = json!({ "type": "object", "properties": { "x": prop }, "required": ["x"] });
        bundle(json!({ "id": "m", "initial": "a", "states": { "a": {} } }), json!({ "context": schema }))
    };
    let cases = [
        (json!({ "allOf": [{ "type": "string" }, { "type": "string" }] }), "intersection"),
        (json!({ "anyOf": [{ "type": "string" }, { "type": "number" }] }), "untagged union"),
        (json!({ "type": ["string", "number"] }), "untagged union"),
        (json!({ "enum": [1, 2] }), "non-string"),
        (json!({ "const": 3 }), "non-string literal"),
        (json!({ "$ref": "#/$defs/Missing" }), "unresolved reference"),
    ];
    for (prop, expected) in cases {
        let (path, message) = error(with_context(prop.clone()));
        assert!(path.starts_with("schemas.context.properties.x"), "{prop}: {path}");
        assert!(message.contains(expected), "{prop}: {message}");
    }
}

#[test]
fn generated_names_must_not_collide() {
    let schemas = json!({ "events": { "a.b": obj(), "aB": obj() } });
    let m = simple(json!({ "a.b": {}, "aB": {} }));
    let (_, message) = error(bundle(m, schemas));
    assert!(message.contains("same variant `AB`"), "{message}");
}

#[test]
fn unused_schema_entries_are_warnings() {
    let m = simple(json!({ "go": { "target": "b" } }));
    let generated =
        generate(&bundle(m, json!({ "events": { "go": obj() }, "guards": { "unused": {} } })), "m").unwrap();
    assert_eq!(generated.warnings, vec!["schemas.guards.unused is declared but never referenced"]);
    assert!(generated.code.contains("pub trait MImpl: Send + Sync + 'static"));
}

#[test]
fn children_are_declared_by_invoke_id() {
    let m = json!({ "id": "m", "initial": "a", "states": { "a": { "invoke": { "id": "worker", "src": "job" } } } });
    let ping = json!({ "type": "object", "properties": { "n": { "type": "integer" } }, "required": ["n"] });
    let children = json!({ "worker": { "events": { "ping": ping }, "output": { "type": "string" } } });
    let code = generate(&bundle(m.clone(), json!({ "children": children })), "m").unwrap().code;
    for item in [
        "pub enum WorkerChildEvent",
        "Ping {",
        "pub type WorkerChildOutput = String;",
        "pub struct WorkerChild<'a>",
        "pub fn child_worker(actor: &mut Actor) -> Option<WorkerChild<'_>>",
        "pub fn child_worker_output(event: &__rt::Ev<Event>) -> Option<WorkerChildOutput>",
    ] {
        assert!(code.contains(item), "missing `{item}` in\n{code}");
    }

    let (path, message) = error(bundle(m.clone(), json!({ "children": { "other": {} } })));
    assert_eq!(path, "schemas.children.other");
    assert!(message.contains("no invoke"), "{message}");
    let (path, _) = error(bundle(m, json!({ "children": { "worker": { "input": {} } } })));
    assert_eq!(path, "schemas.children.worker.input");
}
