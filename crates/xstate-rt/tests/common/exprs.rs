//! Expressions for `{ "@code": name, "@lang": "rust" }` nodes in the
//! differential fixtures. Keep in sync with tools/differential/exprs.mjs
//! (JavaScript arithmetic: numbers are `f64`, written as integers when
//! whole).

use serde_json::{json, Value};
use xstate_rt::Registry;

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
        .context("contextFromInput", |input| {
            let start = input.and_then(|i| i.get("start")).cloned().unwrap_or(json!(0));
            json!({ "count": start, "total": 0, "attempts": 0, "items": [] })
        });
}
