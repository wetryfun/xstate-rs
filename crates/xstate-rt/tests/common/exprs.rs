//! Expressions for `{ "@code": name, "@lang": "rust" }` nodes in the
//! differential fixtures. Keep in sync with tools/differential/exprs.mjs
//! (JavaScript arithmetic: numbers are `f64`, written as integers when
//! whole).

use serde_json::{json, Value};
use xstate_rt::{Registry, SpawnOptions};

fn num(v: Option<&Value>) -> f64 {
    v.and_then(Value::as_f64).unwrap_or(f64::NAN)
}

/// A JavaScript number as JSON (`NaN` and infinities become `null`, as
/// `JSON.stringify` writes them).
fn js(n: f64) -> Value {
    if n.fract() == 0.0 && n.abs() < 9007199254740992.0 {
        Value::from(n as i64)
    } else {
        serde_json::Number::from_f64(n).map_or(Value::Null, Value::Number)
    }
}

pub fn register(r: &mut Registry<Value, Value>) {
    r.expr("countPlusOne", |a| js(num(a.context.get("count")) + 1.0))
        .expr("totalPlusAmount", |a| js(num(a.context.get("total")) + num(a.event.field("amount").as_ref())))
        .expr("attemptsPlusOne", |a| js(num(a.context.get("attempts")) + 1.0))
        .expr("eventValue", |a| a.event.field("value").unwrap_or(Value::Null))
        .expr("eventType", |a| Value::String(a.event.event_type().to_owned()))
        .expr("paramsValue", |a| a.params.and_then(|p| p.get("value")).cloned().unwrap_or(Value::Null))
        .expr("countIsEven", |a| Value::Bool(num(a.context.get("count")) % 2.0 == 0.0))
        .expr("incrementCount", |a| json!({ "context": { "count": js(num(a.context.get("count")) + 1.0) } }))
        .expr("appendItem", |a| {
            let mut items = a.context.get("items").and_then(Value::as_array).cloned().unwrap_or_default();
            items.push(a.event.field("item").unwrap_or(Value::Null));
            Value::Array(items)
        })
        .expr("backoff", |a| js(100.0 * 2f64.powf(num(a.context.get("attempts")))))
        .expr("eventOutput", |a| a.event.field("output").unwrap_or(Value::Null))
        .expr("totalFromOutput", |a| {
            let output = a.event.field("output");
            js(num(a.context.get("total")) + num(output.as_ref().and_then(|o| o.get("total"))))
        })
        .expr(
            "summary",
            |a| json!({ "count": a.context.get("count").cloned(), "total": a.context.get("total").cloned() }),
        )
        // Transition functions (`null`: not enabled) and `enq`.
        .expr("pickTarget", |a| match a.event.field("value") {
            None => Value::Null,
            Some(v) => {
                let target = if num(Some(&v)) > 0.0 { "positive" } else { "#fn.negative" };
                json!({ "target": target, "context": { "count": js(num(a.context.get("count")) + 1.0) } })
            }
        })
        .expr("pickReenter", |a| json!({ "target": ".inner", "reenter": num(a.context.get("count")) > 1.0 }))
        .expr("enqueueOnly", |a| {
            a.enq().unwrap().emit(json!({ "type": "enqueued", "count": a.context.get("count") }));
            Value::Null
        })
        .expr("raiseLater", |a| {
            let enq = a.enq().unwrap();
            enq.raise_delayed(json!({ "type": "LATER", "value": a.context.get("count") }), 50, Some("later"));
            enq.raise(json!({ "type": "NOW" }));
            json!({ "context": { "count": js(num(a.context.get("count")) + 100.0) } })
        })
        .expr("cancelLater", |a| {
            a.enq().unwrap().cancel("later");
            json!({})
        })
        .expr("logAndCall", |a| {
            let enq = a.enq().unwrap();
            enq.log(vec![json!("count"), a.context.get("count").cloned().unwrap_or(Value::Null)]);
            enq.action("noop", json!({ "value": a.context.get("count") }));
            Value::Null
        })
        .expr("emitTwice", |a| {
            let enq = a.enq().unwrap();
            enq.emit(json!({ "type": "one" }));
            enq.emit(json!({ "type": "two", "count": a.context.get("count") }));
            json!({ "context": { "count": js(num(a.context.get("count")) + 10.0) } })
        })
        .expr("routeIfEven", |a| {
            let count = num(a.context.get("count"));
            if count % 2.0 == 0.0 {
                json!({ "context": { "routed": js(count) } })
            } else {
                json!(false)
            }
        })
        .expr("routeAlways", |_| json!(true))
        .expr("choosePick", |a| {
            let count = num(a.context.get("count"));
            if count % 2.0 == 0.0 {
                json!({ "target": "even" })
            } else {
                json!({ "target": "odd", "context": { "picked": js(count) } })
            }
        })
        .expr("pickEvenAlways", |a| {
            if num(a.context.get("count")) % 2.0 == 0.0 {
                json!({ "target": "even" })
            } else {
                Value::Null
            }
        })
        .expr("doneToEven", |a| json!({ "target": "even", "context": { "result": a.event.field("output") } }))
        // Sending and stopping.
        .expr("sendPing", |a| {
            a.enq().unwrap().send_to("c", json!({ "type": "PING" }));
            Value::Null
        })
        .expr("sendPingLater", |a| {
            a.enq().unwrap().send_to_delayed("c", json!({ "type": "PING" }), 100, Some("ping"));
            Value::Null
        })
        .expr("sendMissing", |a| {
            a.enq().unwrap().send_to("nope", json!({ "type": "PING" }));
            Value::Null
        })
        .expr("replyParent", |a| {
            a.enq().unwrap().send_parent(json!({ "type": "PONG" }));
            Value::Null
        })
        .expr("stopChild", |a| {
            a.enq().unwrap().stop("c");
            Value::Null
        })
        .expr("spawnKid", |a| {
            let options = SpawnOptions { input: Some(json!({ "start": 10 })), ..Default::default() };
            let id = a.enq().unwrap().spawn("kid", options);
            json!({ "context": { "last": id } })
        })
        .expr("spawnNamed", |a| {
            let options =
                SpawnOptions { id: Some("named".into()), input: Some(json!({ "start": 5 })), ..Default::default() };
            a.enq().unwrap().spawn("kid", options);
            Value::Null
        })
        .expr("pingLast", |a| {
            let last = a.context["last"].as_str().unwrap_or_default();
            a.enq().unwrap().send_to(last, json!({ "type": "PING" }));
            Value::Null
        })
        .expr("finishLast", |a| {
            let last = a.context["last"].as_str().unwrap_or_default();
            a.enq().unwrap().send_to(last, json!({ "type": "STOP" }));
            Value::Null
        })
        .expr("stopLast", |a| {
            a.enq().unwrap().stop(a.context["last"].as_str().unwrap_or_default());
            Value::Null
        })
        .expr("spawnInTransition", |a| {
            let id = a.enq().unwrap().spawn("kid", SpawnOptions::default());
            json!({ "context": { "last": id } })
        })
        .expr("pongsPlusOne", |a| js(num(a.context.get("pongs")) + 1.0))
        // State input.
        .expr("stateInput", |a| a.input.cloned().unwrap_or(Value::Null))
        .expr("hasInput", |a| Value::Bool(a.input.is_some()))
        .expr("toBWithInput", |a| json!({ "target": "b", "input": { "fromFn": a.context.get("count") } }))
        // examples/pattern-retry-policy (fixtures/system/ex_retry_policy*).
        .expr("canRetry", |a| Value::Bool(num(a.context.get("attempts")) + 1.0 < 5.0))
        .expr("contextName", |a| a.context.get("name").cloned().unwrap_or(Value::Null))
        .expr("errorMessage", |a| {
            let error = a.event.field("error").unwrap_or(Value::Null);
            Value::String(match error.get("message") {
                Some(Value::String(m)) => m.clone(),
                other => other.map_or("undefined".into(), Value::to_string),
            })
        })
        .expr("outputRecordId", |a| a.event.field("output").and_then(|o| o.get("recordId").cloned()).unwrap_or(Value::Null))
        .expr("retryMessage", |a| {
            let attempts = num(a.context.get("attempts"));
            let error = match a.context.get("error") {
                Some(Value::String(e)) => e.clone(),
                other => other.map_or("undefined".into(), Value::to_string),
            };
            Value::String(format!(
                "attempt {} failed ({error}); retrying in {}ms",
                js(attempts),
                js(100.0 * 2f64.powf(attempts))
            ))
        })
        .expr("storedOutput", |a| json!({ "ok": true, "recordId": a.context.get("recordId") }))
        .expr("gaveUpOutput", |a| json!({ "ok": false, "attempts": a.context.get("attempts") }))
        .context("retryContext", |input| {
            json!({ "name": input.and_then(|i| i.get("name")).cloned(), "attempts": 0, "recordId": null, "error": null })
        })
        .context_with("contextSpawning", |a| {
            let start = a.input.and_then(|i| i.get("start")).cloned().unwrap_or(json!(3));
            let first = SpawnOptions { id: Some("first".into()), input: Some(json!({ "start": start })), ..Default::default() };
            let third = SpawnOptions { input: Some(json!({ "start": 100 })), ..Default::default() };
            json!({
                "first": a.spawn("kid", first),
                "second": a.spawn("kid", SpawnOptions::default()),
                "third": a.spawn("kid", third),
                "pongs": 0
            })
        })
        .expr("pingFirst", |a| {
            a.enq().unwrap().send_to_ref(&a.context["first"], json!({ "type": "PING" }));
            Value::Null
        })
        .expr("pingSecondLater", |a| {
            a.enq().unwrap().send_to_ref_delayed(&a.context["second"], json!({ "type": "PING" }), 100, Some("later"));
            Value::Null
        })
        .expr("finishFirst", |a| {
            a.enq().unwrap().send_to_ref(&a.context["first"], json!({ "type": "STOP" }));
            Value::Null
        })
        .context("contextFromInput", |input| {
            let start = input.and_then(|i| i.get("start")).cloned().unwrap_or(json!(0));
            json!({ "count": start, "total": 0, "attempts": 0, "items": [] })
        });
}
