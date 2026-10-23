//! Migrations: persisted actors of an earlier machine version restored onto
//! the current one.

use std::sync::Arc;

use serde_json::{json, Value};
use xstate_rt::{Actor, Machine, Migration, Registry};

fn machine(definition: Value) -> Arc<Machine<Value, Value>> {
    Arc::new(Machine::from_json(&definition, &Registry::new()).unwrap())
}

fn v1() -> Arc<Machine<Value, Value>> {
    machine(json!({
        "id": "order", "version": "1", "initial": "open", "context": { "items": 2 },
        "states": {
            "open": {
                "after": { "1000": { "target": "expired" } },
                "on": { "pay": { "target": "paying" } }
            },
            "paying": {
                "initial": "card",
                "states": { "card": {}, "hist": { "type": "history", "target": "card" } },
                "on": { "back": { "target": "open" } }
            },
            "expired": { "type": "final" }
        }
    }))
}

/// `open` is now `cart`, and `paying` gained a parallel region pair.
fn v2() -> Arc<Machine<Value, Value>> {
    let emit = |t: &str| json!([{ "type": "@xstate.emit", "event": { "type": t } }]);
    machine(json!({
        "id": "order", "version": "2", "initial": "cart", "context": { "items": 0 },
        "states": {
            "cart": {
                "after": { "1000": { "target": "expired", "actions": emit("expired") } },
                "on": { "pay": { "target": "paying" } }
            },
            "paying": {
                "type": "parallel",
                "states": {
                    "method": { "initial": "card", "states": { "card": {}, "invoice": {} } },
                    "fraud": { "initial": "checking", "states": { "checking": {}, "cleared": {} } }
                },
                "on": { "back": { "target": "cart" } }
            },
            "expired": { "type": "final" }
        }
    }))
}

fn migration() -> Migration {
    serde_json::from_value(json!({
        "states": { "#order.open": "order.cart", "order.paying.card": "#order.paying.method.card" },
        "context": { "currency": "EUR" }
    }))
    .unwrap()
}

#[test]
fn snapshots_move_to_the_current_machine() {
    let (old, new) = (v1(), v2());
    migration().check(Some(&*old), &*new).unwrap();

    // Waiting in `open` with its `after` timer pending.
    let actor = Actor::start(old.clone(), 0).unwrap();
    let mut snapshot = actor.persist();
    migration().apply(&mut snapshot, &*new).unwrap();
    let mut actor = Actor::restore(new.clone(), &snapshot).expect("restores on v2");
    assert_eq!(actor.value(), json!("cart"));
    assert_eq!(actor.context(), &json!({ "items": 2, "currency": "EUR" }));
    assert_eq!(actor.persist()["actors"]["order"]["logicVersion"], json!("2"));
    // The timer was the earlier `open`'s; it now fires as `cart`'s.
    assert_eq!(actor.next_deadline(), Some(1000));
    actor.advance(1000).unwrap();
    assert_eq!(actor.value(), json!("expired"));
    assert_eq!(actor.take_emitted(), vec![json!({ "type": "expired" })]);

    // In `paying.card`, with history recorded for `paying.hist` (gone in
    // v2): the new parallel state gets its other region, and the history
    // is dropped.
    let mut actor = Actor::start(old, 0).unwrap();
    actor.send_json(json!({ "type": "pay" }), 10).unwrap();
    actor.send_json(json!({ "type": "back" }), 20).unwrap();
    actor.send_json(json!({ "type": "pay" }), 30).unwrap();
    let mut snapshot = actor.persist();
    assert!(!snapshot["actors"]["order"]["snapshot"]["historyValue"].as_object().unwrap().is_empty());
    migration().apply(&mut snapshot, &*new).unwrap();
    let actor = Actor::restore(new, &snapshot).unwrap();
    assert_eq!(actor.value(), json!({ "paying": { "method": "card", "fraud": "checking" } }));
}

#[test]
fn checks_report_missing_and_unknown_states() {
    let (old, new) = (v1(), v2());
    let err = Migration::default().check(Some(&*old), &*new).unwrap_err();
    assert!(err.contains("order.open") && err.contains("order.paying.card"), "{err}");
    let bad: Migration = serde_json::from_value(json!({ "states": { "order.open": "order.nope" } })).unwrap();
    assert!(bad.check(None::<&Machine<Value, Value>>, &*new).unwrap_err().contains("order.nope"));
    // Without the mapping, the earlier state is an error when applied.
    let mut snapshot = Actor::start(old, 0).unwrap().persist();
    let err = Migration::default().apply(&mut snapshot, &*new).unwrap_err();
    assert!(err.contains("\"order.open\" does not exist"), "{err}");
}
