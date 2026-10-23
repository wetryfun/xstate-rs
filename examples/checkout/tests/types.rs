//! The generated types for `machines/types.ts` round-trip the JSON XState
//! uses, and the generated module runs.

use std::sync::{Arc, Mutex};

use checkout_example::types::{self, Context, ContextKind, ContextTree, Event, Input, Money, SetValue, TypesImpl};
use serde_json::{json, Value};
use xstate_rt::{Actor, Ev, GuardArgs, Status};

#[derive(Default)]
struct Impl {
    recorded: Mutex<Vec<Vec<String>>>,
}

impl TypesImpl for Impl {
    fn guard_at_least(&self, args: &GuardArgs<'_, Context, Event>, min: &f64) -> bool {
        matches!(args.event, Ev::User(Event::Set { value: SetValue::Add { by } }) if by >= min)
    }

    fn action_record(&self, params: &Vec<String>) {
        self.recorded.lock().unwrap().push(params.clone());
    }
}

#[test]
fn context_round_trips() {
    let json = serde_json::from_str::<Value>(types::DEFINITION).unwrap()["context"].clone();
    let context: Context = serde_json::from_value(json.clone()).unwrap();
    assert_eq!(context.prices["a"], Money { amount: 100, currency: "SEK".into() });
    assert_eq!(context.point, (1.0, 2.0));
    assert_eq!(context.note, None);
    assert_eq!(context.r#type, "basic");
    assert_eq!(context.kind, ContextKind::Demo);
    let leaf = ContextTree { name: "leaf".into(), children: vec![] };
    assert_eq!(context.tree.children, vec![Box::new(leaf)]);
    assert_eq!(context.anything, json!({ "free": ["form"] }));
    // Serde writes `f64` 1.0 as `1.0`; the runtime writes it as JavaScript does.
    let mut written = serde_json::to_value(&context).unwrap();
    assert_ne!(written, json);
    xstate_rt::js_numbers(&mut written);
    assert_eq!(written, json);

    // A field with a default may be left out.
    let mut partial = json.clone();
    partial.as_object_mut().unwrap().remove("type");
    assert_eq!(serde_json::from_value::<Context>(partial).unwrap().r#type, "basic");
}

#[test]
fn input_and_nested_unions() {
    assert_eq!(serde_json::from_value::<Input>(json!({})).unwrap(), Input { seed: None });
    assert_eq!(serde_json::from_value::<Input>(json!({ "seed": 3 })).unwrap(), Input { seed: Some(3) });
    let e: Event = serde_json::from_value(json!({ "type": "set", "value": { "op": "reset" } })).unwrap();
    assert_eq!(e, Event::Set { value: SetValue::Reset });
}

#[test]
fn runs_with_scalar_params() {
    let imp = Arc::new(Impl::default());
    let machine = Arc::new(types::machine(imp.clone()).unwrap());
    let mut actor = Actor::start(machine, 0).unwrap();
    let definition: Value = serde_json::from_str(types::DEFINITION).unwrap();
    let persisted = actor.persist();
    let root = persisted["root"].as_str().unwrap().to_owned();
    assert_eq!(persisted["actors"][&root]["snapshot"]["context"], definition["context"]);
    actor.send(Event::Set { value: SetValue::Add { by: 1.0 } }, 1).unwrap();
    assert_eq!(actor.value(), json!("idle"));
    actor.send(Event::Set { value: SetValue::Add { by: 2.0 } }, 2).unwrap();
    assert_eq!(actor.status(), Status::Done);
    let output: types::Output = serde_json::from_value(actor.output().cloned().unwrap()).unwrap();
    assert_eq!(output, "finished");
    assert_eq!(*imp.recorded.lock().unwrap(), vec![vec!["a".to_owned(), "b".to_owned()]]);
}
