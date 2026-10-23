use std::sync::Arc;

use checkout_example::checkout::{
    self, Actor, ContextStatus, Emitted, EmittedOrderPaidChannel, Event, NotifyParamsChannel, Output,
    PaymentMethodMethod,
};
use checkout_example::Shop;
use serde_json::{json, Value};
use xstate_rt::Status;

fn start(shop: &Arc<Shop>) -> Actor {
    let machine = checkout::machine(shop.clone()).expect("the definition loads");
    Actor::start(Arc::new(machine), 0).expect("starts")
}

/// The id of the pending charge (`@xstate.effect` of the invoked actor).
fn pending_charge(actor: &Actor) -> String {
    let effects = actor.pending_effects();
    assert_eq!(effects.len(), 1, "{effects:?}");
    assert_eq!(effects[0].ty, "@xstate.effect");
    effects[0].id.to_string()
}

fn emitted(actor: &mut Actor) -> Vec<Emitted> {
    actor.take_emitted().into_iter().map(|e| serde_json::from_value(e).expect("a declared emitted event")).collect()
}

#[test]
fn events_serialize_as_xstate_events() {
    let e = Event::PaymentMethod { method: PaymentMethodMethod::Card { last4: "4242".into() } };
    let v = serde_json::to_value(&e).unwrap();
    assert_eq!(v, json!({ "type": "payment.method", "method": { "kind": "card", "last4": "4242" } }));
    assert_eq!(serde_json::from_value::<Event>(v).unwrap(), e);
    assert_eq!(serde_json::to_value(Event::Cancel).unwrap(), json!({ "type": "cancel" }));
    assert_eq!(xstate_rt::EventType::event_type(&Event::Checkout { total: 1.0 }), "checkout");
}

#[test]
fn over_the_limit_stays_in_the_cart() {
    let shop = Arc::new(Shop::default());
    let mut actor = start(&shop);
    actor.send(Event::Checkout { total: 5000.0 }, 10).unwrap();
    assert!(actor.matches("cart"));
    let notes = shop.notifications.lock().unwrap();
    assert_eq!(notes.len(), 1);
    assert_eq!(notes[0].channel, NotifyParamsChannel::Email);
    assert_eq!(notes[0].template, "over-limit");
}

#[test]
fn paying_succeeds() {
    let shop = Arc::new(Shop::default());
    let mut actor = start(&shop);
    actor.send(Event::PaymentMethod { method: PaymentMethodMethod::Invoice { reference: None } }, 5).unwrap();
    actor.send(Event::Checkout { total: 120.0 }, 10).unwrap();
    assert!(actor.matches("paying"));
    assert_eq!(actor.context().status, ContextStatus::Paying);
    assert_eq!(shop.audited.load(std::sync::atomic::Ordering::Relaxed), 1);

    let charge = pending_charge(&actor);
    actor.resolve_effect(&charge, Some(json!({ "type": "xstate.async.resolve", "data": { "receipt": 7 } }))).unwrap();
    assert_eq!(actor.status(), Status::Done);
    assert_eq!(actor.context().status, ContextStatus::Paid);
    let output: Output = serde_json::from_value(actor.output().cloned().unwrap()).unwrap();
    assert_eq!(output, Output { paid: true });
    assert_eq!(emitted(&mut actor), vec![Emitted::OrderPaid { channel: EmittedOrderPaidChannel::Sms }]);
    assert_eq!(shop.notifications.lock().unwrap()[0].template, "paid");
}

#[test]
fn retries_after_backoff_then_fails() {
    let shop = Arc::new(Shop::default());
    let mut actor = start(&shop);
    actor.send(Event::Checkout { total: 10.0 }, 0).unwrap();

    let charge = pending_charge(&actor);
    let retryable = json!({ "type": "xstate.async.reject", "data": { "retryable": true } });
    actor.resolve_effect(&charge, Some(retryable)).unwrap();
    assert!(actor.matches("retrying"));
    assert_eq!(actor.next_deadline(), Some(250));

    actor.advance(250).unwrap();
    assert!(actor.matches("paying"), "{:?}", actor.value());
    let charge = pending_charge(&actor);
    actor.resolve_effect(&charge, Some(json!({ "type": "xstate.async.reject", "data": {} }))).unwrap();
    assert_eq!(actor.status(), Status::Done);
    assert_eq!(actor.value(), json!("failed"));
    assert_eq!(emitted(&mut actor), vec![Emitted::OrderFailed]);
}

#[test]
fn a_slow_charge_times_out() {
    let shop = Arc::new(Shop::default());
    let mut actor = start(&shop);
    actor.send(Event::Checkout { total: 10.0 }, 1000).unwrap();
    assert_eq!(actor.next_deadline(), Some(6000));
    actor.advance(6000).unwrap();
    assert!(actor.matches("retrying"));
    // The timed-out charge was cancelled.
    assert!(actor.pending_effects().iter().all(|e| e.ty != "@xstate.effect"));
}

#[test]
fn persists_and_restores_typed_context() {
    let shop = Arc::new(Shop::default());
    let mut actor = start(&shop);
    actor.send(Event::Checkout { total: 10.0 }, 0).unwrap();
    let saved: Value = actor.persist();
    let machine = Arc::new(checkout::machine(shop.clone()).unwrap());
    let restored = Actor::restore(machine, &saved).unwrap();
    assert_eq!(restored.context().status, ContextStatus::Paying);
    assert_eq!(restored.persist(), saved);
}
