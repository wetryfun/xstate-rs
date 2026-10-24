use std::sync::Arc;

use serde_json::{json, Value};
use xstate_rt::{Actor, Machine, Registry, Status};

use crate::{JsEvaluator, JsOptions};

fn actor(definition: Value) -> Actor<Value, Value> {
    actor_with(definition, JsEvaluator::new())
}

fn actor_with(definition: Value, js: JsEvaluator) -> Actor<Value, Value> {
    let mut r: Registry<Value, Value> = Registry::new();
    r.evaluator("js", js);
    Actor::start(Arc::new(Machine::from_json(&definition, &r).unwrap()), 0).unwrap()
}

fn one_event(transition: Value) -> Value {
    json!({
        "@exprLang": "js", "id": "m", "context": { "n": 1 }, "initial": "a",
        "states": { "a": { "on": { "go": transition } }, "b": {} }
    })
}

#[test]
fn code_and_expressions_compute_values() {
    let mut a = actor(one_event(json!({
        "guard": { "@expr": "context.n > 0 && event.by > 1" },
        "context": { "n": { "@code": "({ context, event }) => context.n + event.by" } },
        "actions": [{ "@code": "({ context }, enq) => { enq.emit({ type: 'n', n: context.n }); }" }]
    })));
    a.send(json!({ "type": "go", "by": 1 }), 0).unwrap();
    assert_eq!(a.context()["n"], 1);
    a.send(json!({ "type": "go", "by": 2 }), 0).unwrap();
    assert_eq!(a.context()["n"], 3);
    assert_eq!(a.take_emitted(), vec![json!({ "type": "n", "n": 3 })]);
}

#[test]
fn transition_functions_and_enq() {
    let mut a = actor(one_event(json!({
        "@code": "({ context }, enq) => { enq.raise({ type: 'later' }, { delay: 50, id: 'l' }); enq.log('n', context.n); return { target: 'b', context: { n: context.n * 10 } }; }"
    })));
    a.send(json!({ "type": "go" }), 0).unwrap();
    assert!(a.matches("b"));
    assert_eq!(a.context()["n"], 10);
    assert_eq!(a.take_logs(), vec![vec![json!("n"), json!(1)]]);
    assert_eq!(a.next_deadline(), Some(50));
}

#[test]
fn errors_fail_the_step() {
    let mut a = actor(one_event(json!({ "guard": { "@expr": "context.missing.deep" }, "target": "b" })));
    let e = a.send(json!({ "type": "go" }), 0).unwrap_err().to_string();
    assert!(e.contains("cannot read property"), "{e}");
    assert_eq!(a.status(), Status::Error);

    let mut a = actor(one_event(json!({ "guard": { "@expr": "(" }, "target": "b" })));
    let e = a.send(json!({ "type": "go" }), 0).unwrap_err().to_string();
    assert!(e.to_lowercase().contains("unexpected"), "{e}");
}

#[test]
fn randomness_and_the_clock_are_unavailable_by_default() {
    let mut a = actor(one_event(json!({ "context": { "n": { "@expr": "Math.random()" } } })));
    let e = a.send(json!({ "type": "go" }), 0).unwrap_err().to_string();
    assert!(e.contains("Math.random() is not available"), "{e}");

    let mut a =
        actor(one_event(json!({ "context": { "n": { "@expr": "new Date(0).getTime() + Date.UTC(1970, 0, 2)" } } })));
    a.send(json!({ "type": "go" }), 0).unwrap();
    assert_eq!(a.context()["n"], 86_400_000);
    let mut a = actor(one_event(json!({ "context": { "n": { "@expr": "Date.now()" } } })));
    assert!(a.send(json!({ "type": "go" }), 0).is_err());

    let options = JsOptions { deterministic: false, ..Default::default() };
    let mut a = actor_with(
        one_event(json!({ "context": { "n": { "@expr": "Date.now() > 0" } } })),
        JsEvaluator::with_options(options),
    );
    a.send(json!({ "type": "go" }), 0).unwrap();
    assert_eq!(a.context()["n"], true);
}

#[test]
fn runaway_code_is_interrupted() {
    let options = JsOptions { interrupt_limit: 10, ..Default::default() };
    let mut a = actor_with(
        one_event(json!({ "guard": { "@expr": "(() => { for (;;) {} })()" } })),
        JsEvaluator::with_options(options),
    );
    let e = a.send(json!({ "type": "go" }), 0).unwrap_err().to_string();
    assert!(e.contains("ran too long"), "{e}");
}

#[test]
fn typed_contexts_cross_as_json() {
    #[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
    struct Ctx {
        count: f64,
        label: String,
    }
    let mut r: Registry<Ctx, Value> = Registry::new();
    r.evaluator("js", JsEvaluator::new());
    let definition = json!({
        "@exprLang": "js", "context": { "count": 1.0, "label": "x" }, "initial": "a",
        "states": { "a": { "on": { "go": { "context": {
            "count": { "@expr": "context.count + 0.5" },
            "label": { "@expr": "context.label + event.suffix" }
        } } } } }
    });
    let mut a = Actor::start(Arc::new(Machine::from_json(&definition, &r).unwrap()), 0).unwrap();
    a.send(json!({ "type": "go", "suffix": "y" }), 0).unwrap();
    assert_eq!(a.context(), &Ctx { count: 1.5, label: "xy".into() });
}

#[test]
fn the_root_context_sees_input_and_self() {
    let mut r: Registry<Value, Value> = Registry::new();
    r.evaluator("js", JsEvaluator::new());
    let definition = json!({
        "@exprLang": "js", "id": "m", "initial": "a", "states": { "a": {} },
        "context": { "@code": "({ input, self }) => ({ start: input.start, me: self.id })" }
    });
    let machine = Arc::new(Machine::from_json(&definition, &r).unwrap());
    let options = xstate_rt::InitOptions { input: Some(json!({ "start": 4 })), ..Default::default() };
    let a = Actor::start_with(machine, options).unwrap();
    assert_eq!(a.context(), &json!({ "start": 4, "me": "m" }));
}

#[test]
fn failures_in_functions_actions_and_the_context_fail_the_step() {
    // A transition function that reads a name its module had (serialized
    // closures lose their scope): the step fails, as in XState.
    let mut a = actor(one_event(json!({ "@code": "({ context }) => ({ context: { n: context.n + LIMIT } })" })));
    let e = a.send(json!({ "type": "go" }), 0).unwrap_err().to_string();
    assert!(e.contains("LIMIT"), "{e}");
    assert_eq!(a.status(), Status::Error);

    let mut a = actor(one_event(json!({ "actions": [{ "@code": "() => { throw new Error('nope'); }" }] })));
    assert_eq!(a.send(json!({ "type": "go" }), 0).unwrap_err().to_string(), "nope");

    let mut r: Registry<Value, Value> = Registry::new();
    r.evaluator("js", JsEvaluator::new());
    let definition =
        json!({ "@exprLang": "js", "context": { "@code": "() => missing" }, "initial": "a", "states": { "a": {} } });
    let machine = Arc::new(Machine::from_json(&definition, &r).unwrap());
    let e = Actor::start(machine, 0).err().expect("the start fails").to_string();
    assert!(e.contains("missing"), "{e}");
}
