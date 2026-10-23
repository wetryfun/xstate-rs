use std::borrow::Cow;

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{Map, Value};

/// A user event type: usually a generated serde enum with
/// `#[serde(tag = "type")]`, or `serde_json::Value` for untyped use.
///
/// Only [`EventType::event_type`] is required. The other methods have serde
/// based defaults; override them when a faster path exists.
pub trait EventType: Clone + Serialize + DeserializeOwned + 'static {
    /// The event's `type` string.
    fn event_type(&self) -> &str;

    /// A payload field, used by transition `matches` patterns. Only called for
    /// transitions that declare `matches`.
    fn field(&self, key: &str) -> Option<Value> {
        match serde_json::to_value(self) {
            Ok(Value::Object(mut map)) => map.remove(key).map(|mut v| {
                js_numbers(&mut v);
                v
            }),
            _ => None,
        }
    }

    /// The JSON form of the event, `{ "type": ..., ...payload }`.
    fn to_json(&self) -> Value {
        let mut v = serde_json::to_value(self).unwrap_or(Value::Null);
        js_numbers(&mut v);
        v
    }

    /// Converts a JSON event into `Self`, handing the value back when it does
    /// not fit.
    fn from_json(value: Value) -> Result<Self, Value> {
        match Self::deserialize(&value) {
            Ok(event) => Ok(event),
            Err(_) => Err(value),
        }
    }
}

/// Rewrites whole-number floats as integers, as JavaScript's JSON does: a
/// Rust `f64` field holding `1.0` becomes `1`, as XState would write it.
pub fn js_numbers(value: &mut Value) {
    const MAX_SAFE: f64 = 9_007_199_254_740_991.0;
    match value {
        Value::Number(n) if n.is_f64() => {
            let f = n.as_f64().unwrap_or(f64::NAN);
            if f.fract() == 0.0 && f.abs() <= MAX_SAFE {
                *value = Value::from(f as i64);
            }
        }
        Value::Array(items) => items.iter_mut().for_each(js_numbers),
        Value::Object(map) => map.values_mut().for_each(js_numbers),
        _ => {}
    }
}

/// Whether [`js_numbers`] would change `value`.
pub(crate) fn needs_js_numbers(value: &Value) -> bool {
    const MAX_SAFE: f64 = 9_007_199_254_740_991.0;
    match value {
        Value::Number(n) if n.is_f64() => {
            let f = n.as_f64().unwrap_or(f64::NAN);
            f.fract() == 0.0 && f.abs() <= MAX_SAFE
        }
        Value::Array(items) => items.iter().any(needs_js_numbers),
        Value::Object(map) => map.values().any(needs_js_numbers),
        _ => false,
    }
}

impl EventType for Value {
    fn event_type(&self) -> &str {
        self.get("type").and_then(Value::as_str).unwrap_or("")
    }

    fn field(&self, key: &str) -> Option<Value> {
        self.get(key).cloned()
    }

    fn to_json(&self) -> Value {
        self.clone()
    }

    fn from_json(value: Value) -> Result<Self, Value> {
        if value.get("type").and_then(Value::as_str).is_some() {
            Ok(value)
        } else {
            Err(value)
        }
    }
}

pub(crate) const INIT: &str = "xstate.init";
pub(crate) const DONE_STATE: &str = "xstate.done.state";
pub(crate) const AFTER: &str = "xstate.after";

/// An event as the interpreter sees it: the initial event, a user event, or
/// an event that is not part of `E` (lifecycle events such as
/// `xstate.done.state`, and raised events `E` cannot represent).
#[derive(Debug, Clone, PartialEq)]
pub enum Ev<E> {
    Init,
    User(E),
    Internal { event_type: Cow<'static, str>, data: Map<String, Value> },
}

impl<E: EventType> Ev<E> {
    pub fn event_type(&self) -> &str {
        match self {
            Ev::Init => INIT,
            Ev::User(e) => e.event_type(),
            Ev::Internal { event_type, .. } => event_type,
        }
    }

    /// A payload field (anything but `type`).
    pub fn field(&self, key: &str) -> Option<Value> {
        match self {
            Ev::Init => None,
            Ev::User(e) => e.field(key),
            Ev::Internal { data, .. } => data.get(key).cloned(),
        }
    }

    pub fn to_json(&self) -> Value {
        match self {
            Ev::Init => serde_json::json!({ "type": INIT }),
            Ev::User(e) => e.to_json(),
            Ev::Internal { event_type, data } => {
                let mut map = Map::with_capacity(data.len() + 1);
                map.insert("type".into(), Value::String(event_type.to_string()));
                for (k, v) in data {
                    map.insert(k.clone(), v.clone());
                }
                Value::Object(map)
            }
        }
    }

    /// Converts a JSON event: into `E` when it fits, otherwise into
    /// [`Ev::Internal`]. Returns `None` when the value has no string `type`.
    pub fn from_json(value: Value) -> Option<Self> {
        match E::from_json(value) {
            Ok(e) => Some(Ev::User(e)),
            Err(Value::Object(mut map)) => {
                let event_type = match map.remove("type") {
                    Some(Value::String(t)) => t,
                    _ => return None,
                };
                if event_type == INIT {
                    return Some(Ev::Init);
                }
                Some(Ev::Internal { event_type: Cow::Owned(event_type), data: map })
            }
            Err(_) => None,
        }
    }

    /// The `output` field of done events.
    pub fn output(&self) -> Option<Value> {
        self.field("output")
    }
}

/// Matches an event type against a descriptor: exact, `*`, or `prefix.*`
/// (which also matches `prefix` itself), as XState's `matchesEventDescriptor`.
pub(crate) fn matches_descriptor(event_type: &str, descriptor: &str) -> bool {
    if descriptor == event_type || descriptor == "*" {
        return true;
    }
    if !descriptor.ends_with(".*") {
        return false;
    }
    let mut event_tokens = event_type.split('.');
    let mut partial_tokens = descriptor.split('.').peekable();
    while let Some(partial) = partial_tokens.next() {
        let event_token = event_tokens.next();
        if partial == "*" {
            return partial_tokens.peek().is_none();
        }
        if Some(partial) != event_token {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn descriptors() {
        assert!(matches_descriptor("a", "a"));
        assert!(matches_descriptor("a.b", "*"));
        assert!(matches_descriptor("a.b", "a.*"));
        assert!(matches_descriptor("a.b.c", "a.*"));
        assert!(matches_descriptor("a", "a.*"));
        assert!(!matches_descriptor("ab", "a.*"));
        assert!(!matches_descriptor("b.a", "a.*"));
        assert!(!matches_descriptor("a.b.c", "a.*.c"));
        assert!(!matches_descriptor("ab", "a*"));
    }

    #[test]
    fn numbers_as_javascript_writes_them() {
        let mut v = serde_json::json!({ "a": 1.0, "b": [2.5, -0.0, 3.0], "c": 7 });
        js_numbers(&mut v);
        assert_eq!(v.to_string(), r#"{"a":1,"b":[2.5,0,3],"c":7}"#);
    }
}
