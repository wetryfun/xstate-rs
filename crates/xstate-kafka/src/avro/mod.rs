//! Avro for input events, output (emitted) events and changelog snapshots,
//! in the Schema Registry wire format: a zero byte, the schema id (four
//! bytes, big-endian), then the Avro binary datum.
//!
//! Schemas are generated from a bundle's JSON Schemas, like the protobuf
//! ones (`crate::proto::events`):
//!
//! - Input events (`schemas.events`) and emitted events (`schemas.emitted`)
//!   are each a union of records, one per event type in schema order, named
//!   `xstate.events.<machine>.<EventType>` / `xstate.emitted.<machine>.<...>`;
//!   the record name is the event type (`type` is not a field). Producers
//!   using a single record schema per type (record-name subjects) work too.
//! - Objects with properties are records (required properties are plain
//!   fields, optional ones `["null", T]` defaulting to null), string enums
//!   are enums (when every value is a valid Avro name), integers `long`,
//!   numbers `double`, arrays `array`, records (`additionalProperties`)
//!   `map`, discriminated unions (`oneOf` objects sharing a `const` key) a
//!   union of records without the key, and anything else (free-form JSON)
//!   a string holding the JSON text (marked `"xstate.json": true`).
//! - Snapshots (XState's `SystemSnapshot`) use a fixed schema
//!   (`xstate.snapshot.v1`), the root machine's context typed by
//!   `schemas.context` when there is one. Every field is optional, and keys
//!   or values a record does not describe go to its `extra` list (key and
//!   JSON text), so every snapshot round-trips; ordered maps (actors,
//!   children, registry) are arrays of `{ key, value }`.
//!
//! Records are read with schema resolution: a datum written with any
//! registered schema (found by its id) is resolved against the generated
//! one, so producers can evolve their schemas compatibly.

pub mod registry;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use apache_avro::types::Value as Avro;
use apache_avro::Schema;
use serde_json::{json, Map, Number, Value};

pub use registry::Registry;

/// The Schema Registry wire format's first byte.
pub const MAGIC: u8 = 0;

/// How a JSON value maps to Avro.
#[derive(Debug, Clone)]
enum Shape {
    Bool,
    Long,
    Double,
    Str,
    /// Any JSON, as its text.
    Json,
    Enum(Arc<EnumShape>),
    Array(Box<Shape>),
    Map(Box<Shape>),
    /// An object with any keys, in order: an array of `{ key, value }`.
    Entries(Arc<RecordShape>),
    Record(Arc<RecordShape>),
    /// `["null", T]`, where null is an absent key.
    Optional(Box<Shape>),
    /// `["null", T]`, where null is JSON null.
    Nullable(Box<Shape>),
    /// Records told apart by the string `key` (not a field of theirs).
    Union(Arc<UnionShape>),
}

#[derive(Debug)]
struct EnumShape {
    name: String,
    symbols: Vec<String>,
}

#[derive(Debug)]
struct RecordShape {
    name: String,
    fields: Vec<FieldShape>,
    /// Keep what the fields do not describe in an `extra` list (snapshots).
    extra: bool,
}

#[derive(Debug)]
struct FieldShape {
    /// The JSON key.
    key: String,
    /// The Avro field name.
    name: String,
    shape: Shape,
}

#[derive(Debug)]
struct UnionShape {
    key: String,
    branches: Vec<(String, Arc<RecordShape>)>,
}

/// A valid Avro name for `s` (letters, digits and `_`, not starting with a
/// digit).
fn avro_name(s: &str) -> String {
    let mut out: String = s.chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' { c } else { '_' }).collect();
    if out.is_empty() || out.starts_with(|c: char| c.is_ascii_digit()) {
        out.insert(0, '_');
    }
    out
}

fn is_avro_name(s: &str) -> bool {
    avro_name(s) == s
}

fn pascal(s: &str) -> String {
    let mut out = String::new();
    let mut upper = true;
    for c in s.chars() {
        if c.is_ascii_alphanumeric() {
            out.push(if upper { c.to_ascii_uppercase() } else { c });
            upper = false;
        } else {
            upper = true;
        }
    }
    avro_name(&out)
}

/// The Avro schema (JSON) of a shape; named types are defined once.
fn schema_json(shape: &Shape, defined: &mut HashSet<String>) -> Value {
    match shape {
        Shape::Bool => json!("boolean"),
        Shape::Long => json!("long"),
        Shape::Double => json!("double"),
        Shape::Str => json!("string"),
        Shape::Json => json!({ "type": "string", "xstate.json": true }),
        Shape::Enum(e) => {
            if !defined.insert(e.name.clone()) {
                return json!(e.name);
            }
            json!({ "type": "enum", "name": e.name, "symbols": e.symbols })
        }
        Shape::Array(items) => json!({ "type": "array", "items": schema_json(items, defined) }),
        Shape::Map(values) => json!({ "type": "map", "values": schema_json(values, defined) }),
        Shape::Entries(entry) => json!({ "type": "array", "items": record_json(entry, defined) }),
        Shape::Record(r) => record_json(r, defined),
        Shape::Optional(inner) | Shape::Nullable(inner) => json!(["null", schema_json(inner, defined)]),
        Shape::Union(u) => Value::Array(u.branches.iter().map(|(_, r)| record_json(r, defined)).collect()),
    }
}

fn record_json(r: &RecordShape, defined: &mut HashSet<String>) -> Value {
    if !defined.insert(r.name.clone()) {
        return json!(r.name);
    }
    let mut fields: Vec<Value> = r
        .fields
        .iter()
        .map(|f| {
            let mut field = json!({ "name": f.name, "type": schema_json(&f.shape, defined) });
            if matches!(f.shape, Shape::Optional(_)) {
                field["default"] = Value::Null;
            }
            if f.name != f.key {
                field["json"] = json!(f.key);
            }
            field
        })
        .collect();
    if r.extra {
        let entry = format!("{}Extra", r.name);
        let extra_json = if defined.insert(entry.clone()) {
            json!({ "type": "record", "name": entry, "fields": [
                { "name": "key", "type": "string" },
                { "name": "value", "type": { "type": "string", "xstate.json": true } }
            ] })
        } else {
            json!(entry)
        };
        fields.push(json!({ "name": "extra", "type": { "type": "array", "items": extra_json }, "default": [] }));
    }
    json!({ "type": "record", "name": r.name, "fields": fields })
}

/// A JSON number from an `f64`, written as JavaScript writes it.
fn number(f: f64) -> Result<Value, String> {
    if f.fract() == 0.0 && f.abs() < 9_007_199_254_740_992.0 {
        return Ok(Value::from(f as i64));
    }
    Number::from_f64(f).map(Value::Number).ok_or_else(|| format!("{f} is not a JSON number"))
}

/// The `apache-avro` value of `v` (the reference the direct writer is
/// tested against).
#[cfg(test)]
fn to_avro(shape: &Shape, v: &Value) -> Result<Avro, String> {
    let mismatch = || format!("{v} does not fit {}", schema_json(shape, &mut HashSet::new()));
    Ok(match shape {
        Shape::Bool => Avro::Boolean(v.as_bool().ok_or_else(mismatch)?),
        Shape::Long => Avro::Long(match v.as_i64() {
            Some(i) => i,
            None => {
                v.as_f64().filter(|f| f.fract() == 0.0 && f.abs() < 9.2e18).map(|f| f as i64).ok_or_else(mismatch)?
            }
        }),
        Shape::Double => Avro::Double(v.as_f64().ok_or_else(mismatch)?),
        Shape::Str => Avro::String(v.as_str().ok_or_else(mismatch)?.to_owned()),
        Shape::Json => Avro::String(v.to_string()),
        Shape::Enum(e) => {
            let s = v.as_str().ok_or_else(mismatch)?;
            let i = e.symbols.iter().position(|x| x == s).ok_or_else(mismatch)?;
            Avro::Enum(i as u32, s.to_owned())
        }
        Shape::Array(items) => {
            Avro::Array(v.as_array().ok_or_else(mismatch)?.iter().map(|x| to_avro(items, x)).collect::<Result<_, _>>()?)
        }
        Shape::Map(values) => Avro::Map(
            v.as_object()
                .ok_or_else(mismatch)?
                .iter()
                .map(|(k, x)| Ok((k.clone(), to_avro(values, x)?)))
                .collect::<Result<_, String>>()?,
        ),
        Shape::Entries(entry) => {
            let value_shape = &entry.fields[1].shape;
            Avro::Array(
                v.as_object()
                    .ok_or_else(mismatch)?
                    .iter()
                    .map(|(k, x)| {
                        Ok(Avro::Record(vec![
                            ("key".into(), Avro::String(k.clone())),
                            ("value".into(), to_avro(value_shape, x)?),
                        ]))
                    })
                    .collect::<Result<_, String>>()?,
            )
        }
        Shape::Record(r) => record_to_avro(r, v.as_object().ok_or_else(mismatch)?, None)?,
        Shape::Optional(inner) | Shape::Nullable(inner) => match v {
            Value::Null => Avro::Union(0, Box::new(Avro::Null)),
            v => Avro::Union(1, Box::new(to_avro(inner, v)?)),
        },
        Shape::Union(u) => {
            let o = v.as_object().ok_or_else(mismatch)?;
            let tag =
                o.get(&u.key).and_then(Value::as_str).ok_or_else(|| format!("{v} has no string \"{}\"", u.key))?;
            let (i, (_, r)) = u
                .branches
                .iter()
                .enumerate()
                .find(|(_, (t, _))| t == tag)
                .ok_or_else(|| format!("\"{}\": \"{tag}\" is not in the schema", u.key))?;
            Avro::Union(i as u32, Box::new(record_to_avro(r, o, Some(&u.key))?))
        }
    })
}

/// A record from a JSON object (`skip`: a union's tag key). A record with
/// `extra` keeps what its fields cannot hold in the `extra` list.
#[cfg(test)]
fn record_to_avro(r: &RecordShape, o: &Map<String, Value>, skip: Option<&str>) -> Result<Avro, String> {
    let mut fields = Vec::with_capacity(r.fields.len() + 1);
    let mut extra = Vec::new();
    for f in &r.fields {
        let v = o.get(&f.key);
        let avro = match (&f.shape, v) {
            (Shape::Optional(_), None) => Avro::Union(0, Box::new(Avro::Null)),
            (Shape::Optional(inner), Some(v)) if r.extra => match to_avro(inner, v) {
                Ok(a) => Avro::Union(1, Box::new(a)),
                Err(_) => {
                    extra.push((f.key.clone(), v));
                    Avro::Union(0, Box::new(Avro::Null))
                }
            },
            (_, Some(v)) => to_avro(&f.shape, v).map_err(|e| format!("\"{}\": {e}", f.key))?,
            (_, None) => return Err(format!("missing \"{}\"", f.key)),
        };
        fields.push((f.name.clone(), avro));
    }
    for (k, v) in o {
        if Some(k.as_str()) == skip || r.fields.iter().any(|f| &f.key == k) {
            continue;
        }
        if !r.extra {
            return Err(format!("\"{k}\" is not in the schema of {}", r.name));
        }
        extra.push((k.clone(), v));
    }
    if r.extra {
        let items = extra
            .into_iter()
            .map(|(k, v)| {
                Avro::Record(vec![("key".into(), Avro::String(k)), ("value".into(), Avro::String(v.to_string()))])
            })
            .collect();
        fields.push(("extra".into(), Avro::Array(items)));
    }
    Ok(Avro::Record(fields))
}

fn from_avro(shape: &Shape, a: Avro) -> Result<Value, String> {
    let mismatch = |a: &Avro| format!("{a:?} does not fit {}", schema_json(shape, &mut HashSet::new()));
    Ok(match (shape, a) {
        (Shape::Bool, Avro::Boolean(b)) => Value::Bool(b),
        (Shape::Long, Avro::Long(i)) => Value::from(i),
        (Shape::Long, Avro::Int(i)) => Value::from(i),
        (Shape::Double, Avro::Double(f)) => number(f)?,
        (Shape::Double, Avro::Float(f)) => number(f64::from(f))?,
        (Shape::Double, Avro::Long(i)) => Value::from(i),
        (Shape::Double, Avro::Int(i)) => Value::from(i),
        (Shape::Str, Avro::String(s)) => Value::String(s),
        (Shape::Json, Avro::String(s)) => serde_json::from_str(&s).map_err(|e| format!("not JSON text: {e}"))?,
        (Shape::Enum(_), Avro::Enum(_, s)) => Value::String(s),
        (Shape::Array(items), Avro::Array(xs)) => {
            Value::Array(xs.into_iter().map(|x| from_avro(items, x)).collect::<Result<_, _>>()?)
        }
        (Shape::Map(values), Avro::Map(m)) => {
            // Avro maps are unordered: sort the keys for a stable result.
            let mut entries: Vec<(String, Avro)> = m.into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            Value::Object(
                entries.into_iter().map(|(k, x)| Ok((k, from_avro(values, x)?))).collect::<Result<_, String>>()?,
            )
        }
        (Shape::Entries(entry), Avro::Array(xs)) => {
            let value_shape = &entry.fields[1].shape;
            let mut o = Map::with_capacity(xs.len());
            for x in xs {
                let Avro::Record(mut kv) = x else { return Err("an entry is not a record".into()) };
                let (Some((_, value)), Some((_, Avro::String(key)))) = (kv.pop(), kv.pop()) else {
                    return Err("an entry needs a key and a value".into());
                };
                o.insert(key, from_avro(value_shape, value)?);
            }
            Value::Object(o)
        }
        (Shape::Record(r), Avro::Record(fields)) => Value::Object(record_from_avro(r, fields)?),
        (Shape::Optional(inner) | Shape::Nullable(inner), Avro::Union(_, a)) => match *a {
            Avro::Null => Value::Null,
            a => from_avro(inner, a)?,
        },
        (Shape::Optional(_) | Shape::Nullable(_), Avro::Null) => Value::Null,
        (Shape::Union(u), Avro::Union(i, a)) => {
            let (tag, r) = u.branches.get(i as usize).ok_or("a union branch out of range")?;
            let Avro::Record(fields) = *a else { return Err("a union branch is not a record".into()) };
            let mut o = Map::with_capacity(fields.len() + 1);
            o.insert(u.key.clone(), Value::String(tag.clone()));
            o.extend(record_from_avro(r, fields)?);
            Value::Object(o)
        }
        (_, a) => return Err(mismatch(&a)),
    })
}

fn record_from_avro(r: &RecordShape, fields: Vec<(String, Avro)>) -> Result<Map<String, Value>, String> {
    let mut o = Map::with_capacity(fields.len());
    for (name, a) in fields {
        if r.extra && name == "extra" {
            let Avro::Array(items) = a else { return Err("\"extra\" is not an array".into()) };
            for item in items {
                let Avro::Record(mut kv) = item else { return Err("an extra entry is not a record".into()) };
                let (Some((_, Avro::String(value))), Some((_, Avro::String(key)))) = (kv.pop(), kv.pop()) else {
                    return Err("an extra entry needs a key and a value".into());
                };
                o.insert(key, serde_json::from_str(&value).map_err(|e| format!("extra \"{}\": {e}", value))?);
            }
            continue;
        }
        let Some(f) = r.fields.iter().find(|f| f.name == name) else { continue };
        match (&f.shape, a) {
            (Shape::Optional(_), Avro::Union(_, a)) if matches!(*a, Avro::Null) => {}
            (Shape::Optional(_), Avro::Null) => {}
            (shape, a) => {
                o.insert(f.key.clone(), from_avro(shape, a).map_err(|e| format!("\"{}\": {e}", f.key))?);
            }
        }
    }
    Ok(o)
}

// The binary encoding, written and read straight from the shapes, for
// records with the generated schemas' own ids (snapshots, outputs, and
// events from producers using the registered schema): no `Value` tree, no
// schema validation per record.

fn put_long(out: &mut Vec<u8>, v: i64) {
    crate::proto::wire::put_varint(out, crate::proto::wire::zigzag(v));
}

fn put_str(out: &mut Vec<u8>, s: &str) {
    put_long(out, s.len() as i64);
    out.extend_from_slice(s.as_bytes());
}

/// Writes `v` as `shape`. On an error, part of it may have been written:
/// the caller truncates.
fn write(shape: &Shape, v: &Value, out: &mut Vec<u8>) -> Result<(), String> {
    let mismatch = || format!("{v} does not fit {}", schema_json(shape, &mut HashSet::new()));
    match shape {
        Shape::Bool => out.push(u8::from(v.as_bool().ok_or_else(mismatch)?)),
        Shape::Long => put_long(
            out,
            match v.as_i64() {
                Some(i) => i,
                None => v
                    .as_f64()
                    .filter(|f| f.fract() == 0.0 && f.abs() < 9.2e18)
                    .map(|f| f as i64)
                    .ok_or_else(mismatch)?,
            },
        ),
        Shape::Double => out.extend_from_slice(&v.as_f64().ok_or_else(mismatch)?.to_le_bytes()),
        Shape::Str => put_str(out, v.as_str().ok_or_else(mismatch)?),
        Shape::Json => put_str(out, &v.to_string()),
        Shape::Enum(e) => {
            let s = v.as_str().ok_or_else(mismatch)?;
            put_long(out, e.symbols.iter().position(|x| x == s).ok_or_else(mismatch)? as i64);
        }
        Shape::Array(items) => {
            let a = v.as_array().ok_or_else(mismatch)?;
            if !a.is_empty() {
                put_long(out, a.len() as i64);
                for x in a {
                    write(items, x, out)?;
                }
            }
            put_long(out, 0);
        }
        Shape::Map(values) => {
            let o = v.as_object().ok_or_else(mismatch)?;
            if !o.is_empty() {
                put_long(out, o.len() as i64);
                for (k, x) in o {
                    put_str(out, k);
                    write(values, x, out)?;
                }
            }
            put_long(out, 0);
        }
        Shape::Entries(entry) => {
            let o = v.as_object().ok_or_else(mismatch)?;
            if !o.is_empty() {
                put_long(out, o.len() as i64);
                for (k, x) in o {
                    put_str(out, k);
                    write(&entry.fields[1].shape, x, out)?;
                }
            }
            put_long(out, 0);
        }
        Shape::Record(r) => write_record(r, v.as_object().ok_or_else(mismatch)?, None, out)?,
        Shape::Optional(inner) | Shape::Nullable(inner) => match v {
            Value::Null => put_long(out, 0),
            v => {
                put_long(out, 1);
                write(inner, v, out)?;
            }
        },
        Shape::Union(u) => {
            let o = v.as_object().ok_or_else(mismatch)?;
            let tag =
                o.get(&u.key).and_then(Value::as_str).ok_or_else(|| format!("{v} has no string \"{}\"", u.key))?;
            let (i, (_, r)) = u
                .branches
                .iter()
                .enumerate()
                .find(|(_, (t, _))| t == tag)
                .ok_or_else(|| format!("\"{}\": \"{tag}\" is not in the schema", u.key))?;
            put_long(out, i as i64);
            write_record(r, o, Some(&u.key), out)?;
        }
    }
    Ok(())
}

fn write_record(r: &RecordShape, o: &Map<String, Value>, skip: Option<&str>, out: &mut Vec<u8>) -> Result<(), String> {
    let mut extra: Vec<(&str, &Value)> = Vec::new();
    for f in &r.fields {
        match (&f.shape, o.get(&f.key)) {
            (Shape::Optional(_), None) => put_long(out, 0),
            (Shape::Optional(inner), Some(v)) if r.extra => {
                let mark = out.len();
                put_long(out, 1);
                if write(inner, v, out).is_err() {
                    out.truncate(mark);
                    put_long(out, 0);
                    extra.push((&f.key, v));
                }
            }
            (shape, Some(v)) => write(shape, v, out).map_err(|e| format!("\"{}\": {e}", f.key))?,
            (_, None) => return Err(format!("missing \"{}\"", f.key)),
        }
    }
    for (k, v) in o {
        if Some(k.as_str()) == skip || r.fields.iter().any(|f| &f.key == k) {
            continue;
        }
        if !r.extra {
            return Err(format!("\"{k}\" is not in the schema of {}", r.name));
        }
        extra.push((k, v));
    }
    if r.extra {
        if !extra.is_empty() {
            put_long(out, extra.len() as i64);
            for (k, v) in extra {
                put_str(out, k);
                put_str(out, &v.to_string());
            }
        }
        put_long(out, 0);
    }
    Ok(())
}

/// A datum being read.
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn long(&mut self) -> Result<i64, String> {
        let mut v = 0u64;
        for i in 0..10 {
            let (&b, rest) = self.0.split_first().ok_or("truncated Avro datum")?;
            self.0 = rest;
            v |= u64::from(b & 0x7f) << (7 * i);
            if b < 0x80 {
                return Ok(crate::proto::wire::unzigzag(v));
            }
        }
        Err("an Avro long is too long".into())
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], String> {
        if self.0.len() < n {
            return Err("truncated Avro datum".into());
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(head)
    }

    fn str(&mut self) -> Result<&'a str, String> {
        let n = usize::try_from(self.long()?).map_err(|_| "a negative string length")?;
        std::str::from_utf8(self.take(n)?).map_err(|_| "a string is not UTF-8".into())
    }

    /// Calls `item` for each item of an array or map (blocks of items).
    fn blocks(&mut self, mut item: impl FnMut(&mut Self) -> Result<(), String>) -> Result<(), String> {
        loop {
            let mut n = self.long()?;
            if n == 0 {
                return Ok(());
            }
            if n < 0 {
                n = -n;
                self.long()?; // the block's size in bytes
            }
            for _ in 0..n {
                item(self)?;
            }
        }
    }
}

fn read(shape: &Shape, r: &mut Reader<'_>) -> Result<Value, String> {
    Ok(match shape {
        Shape::Bool => Value::Bool(r.take(1)?[0] != 0),
        Shape::Long => Value::from(r.long()?),
        Shape::Double => number(f64::from_le_bytes(r.take(8)?.try_into().expect("8 bytes")))?,
        Shape::Str => Value::String(r.str()?.to_owned()),
        Shape::Json => serde_json::from_str(r.str()?).map_err(|e| format!("not JSON text: {e}"))?,
        Shape::Enum(e) => {
            let i = r.long()?;
            Value::String(e.symbols.get(i as usize).ok_or("an enum index out of range")?.clone())
        }
        Shape::Array(items) => {
            let mut a = Vec::new();
            r.blocks(|r| {
                a.push(read(items, r)?);
                Ok(())
            })?;
            Value::Array(a)
        }
        Shape::Map(values) => {
            let mut entries = Vec::new();
            r.blocks(|r| {
                let k = r.str()?.to_owned();
                entries.push((k, read(values, r)?));
                Ok(())
            })?;
            // Avro maps are unordered: sort the keys for a stable result.
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            Value::Object(entries.into_iter().collect())
        }
        Shape::Entries(entry) => {
            let mut o = Map::new();
            r.blocks(|r| {
                let k = r.str()?.to_owned();
                o.insert(k, read(&entry.fields[1].shape, r)?);
                Ok(())
            })?;
            Value::Object(o)
        }
        Shape::Record(rec) => Value::Object(read_record(rec, r)?),
        Shape::Optional(inner) | Shape::Nullable(inner) => match r.long()? {
            0 => Value::Null,
            1 => read(inner, r)?,
            _ => return Err("a union index out of range".into()),
        },
        Shape::Union(u) => {
            let (tag, rec) = u.branches.get(r.long()? as usize).ok_or("a union index out of range")?;
            let mut o = Map::with_capacity(rec.fields.len() + 1);
            o.insert(u.key.clone(), Value::String(tag.clone()));
            o.extend(read_record(rec, r)?);
            Value::Object(o)
        }
    })
}

fn read_record(rec: &RecordShape, r: &mut Reader<'_>) -> Result<Map<String, Value>, String> {
    let mut o = Map::with_capacity(rec.fields.len());
    for f in &rec.fields {
        match &f.shape {
            Shape::Optional(inner) => match r.long()? {
                0 => {}
                1 => {
                    o.insert(f.key.clone(), read(inner, r).map_err(|e| format!("\"{}\": {e}", f.key))?);
                }
                _ => return Err("a union index out of range".into()),
            },
            shape => {
                o.insert(f.key.clone(), read(shape, r).map_err(|e| format!("\"{}\": {e}", f.key))?);
            }
        }
    }
    if rec.extra {
        r.blocks(|r| {
            let k = r.str()?.to_owned();
            let v = serde_json::from_str(r.str()?).map_err(|e| format!("extra \"{k}\": {e}"))?;
            o.insert(k, v);
            Ok(())
        })?;
    }
    Ok(o)
}

/// Builds shapes from JSON Schemas.
struct Builder<'a> {
    namespace: String,
    names: HashMap<String, usize>,
    defs: Option<&'a Map<String, Value>>,
}

impl<'a> Builder<'a> {
    fn unique(&mut self, name: &str) -> String {
        let n = self.names.entry(name.to_owned()).or_insert(0);
        *n += 1;
        let local = if *n == 1 { name.to_owned() } else { format!("{name}{n}") };
        format!("{}.{local}", self.namespace)
    }

    fn resolve(&self, s: &'a Value) -> &'a Value {
        let mut s = s;
        for _ in 0..16 {
            match s.get("$ref").and_then(Value::as_str).and_then(|r| r.strip_prefix("#/$defs/")) {
                Some(name) => match self.defs.and_then(|d| d.get(name)) {
                    Some(def) => s = def,
                    None => return s,
                },
                None => return s,
            }
        }
        s
    }

    /// The shape of `s`; `depth` bounds recursive references (deeper parts
    /// become JSON text).
    fn shape(&mut self, s: &'a Value, hint: &str, depth: usize) -> Shape {
        if depth > 8 {
            return Shape::Json;
        }
        let s = self.resolve(s);
        for key in ["anyOf", "oneOf"] {
            if let Some(options) = s.get(key).and_then(Value::as_array) {
                let non_null: Vec<&Value> = options
                    .iter()
                    .map(|o| self.resolve(o))
                    .filter(|o| o.get("type").and_then(Value::as_str) != Some("null"))
                    .collect();
                let nullable = non_null.len() < options.len();
                let inner = if non_null.len() == 1 {
                    self.shape(non_null[0], hint, depth + 1)
                } else {
                    self.union(&non_null, hint, depth).unwrap_or(Shape::Json)
                };
                return if nullable { nullable_of(inner) } else { inner };
            }
        }
        let (ty, nullable) = match s.get("type") {
            Some(Value::Array(types)) => {
                let non_null: Vec<&str> = types.iter().filter_map(Value::as_str).filter(|t| *t != "null").collect();
                match non_null.as_slice() {
                    [t] => (*t, non_null.len() < types.len()),
                    _ => return Shape::Json,
                }
            }
            Some(Value::String(t)) => (t.as_str(), false),
            _ => return Shape::Json,
        };
        let shape = match ty {
            "string" => match s.get("enum").and_then(Value::as_array) {
                Some(values) if !values.is_empty() && values.iter().all(|v| v.as_str().is_some_and(is_avro_name)) => {
                    let symbols: Vec<String> = values.iter().filter_map(|v| v.as_str().map(str::to_owned)).collect();
                    let name = self.unique(hint);
                    Shape::Enum(Arc::new(EnumShape { name, symbols }))
                }
                _ => Shape::Str,
            },
            "boolean" => Shape::Bool,
            "integer" => Shape::Long,
            "number" => Shape::Double,
            "array" => match s.get("items").filter(|i| i.is_object()) {
                Some(items) if s.get("prefixItems").is_none() => {
                    Shape::Array(Box::new(self.shape(items, &format!("{hint}Item"), depth + 1)))
                }
                _ => Shape::Json,
            },
            "object" => match s.get("properties").and_then(Value::as_object) {
                Some(p) if !p.is_empty() => Shape::Record(self.record(s, hint, depth, None)),
                _ => match s.get("additionalProperties").filter(|a| a.is_object()) {
                    Some(values) => Shape::Map(Box::new(self.shape(values, &format!("{hint}Value"), depth + 1))),
                    None => Shape::Json,
                },
            },
            _ => Shape::Json,
        };
        if nullable {
            nullable_of(shape)
        } else {
            shape
        }
    }

    /// A discriminated union of objects.
    fn union(&mut self, options: &[&'a Value], hint: &str, depth: usize) -> Option<Shape> {
        if options.len() < 2 || !options.iter().all(|o| o.get("type").and_then(Value::as_str) == Some("object")) {
            return None;
        }
        let first = options[0].get("properties").and_then(Value::as_object)?;
        let key = first
            .iter()
            .find(|(k, _)| {
                options.iter().all(|o| {
                    o.get("properties")
                        .and_then(|p| p.get(k.as_str()))
                        .and_then(|d| d.get("const"))
                        .is_some_and(Value::is_string)
                        && o.get("required")
                            .and_then(Value::as_array)
                            .is_some_and(|r| r.iter().any(|x| x == k.as_str()))
                })
            })
            .map(|(k, _)| k.clone())?;
        let branches = options
            .iter()
            .map(|o| {
                let tag = o["properties"][key.as_str()]["const"].as_str().unwrap_or_default().to_owned();
                let record = self.record(o, &format!("{hint}{}", pascal(&tag)), depth, Some(&key));
                (tag, record)
            })
            .collect();
        Some(Shape::Union(Arc::new(UnionShape { key, branches })))
    }

    fn record(&mut self, s: &'a Value, hint: &str, depth: usize, skip: Option<&str>) -> Arc<RecordShape> {
        let name = self.unique(hint);
        let required: Vec<&str> =
            s.get("required").and_then(Value::as_array).into_iter().flatten().filter_map(Value::as_str).collect();
        let mut fields: Vec<FieldShape> = Vec::new();
        for (key, prop) in s.get("properties").and_then(Value::as_object).into_iter().flatten() {
            if Some(key.as_str()) == skip {
                continue;
            }
            let shape = self.shape(prop, &format!("{hint}{}", pascal(key)), depth + 1);
            let shape = if required.contains(&key.as_str()) {
                shape
            } else {
                // Optional (and an optional nullable field is one option).
                match shape {
                    Shape::Nullable(inner) | Shape::Optional(inner) => Shape::Optional(inner),
                    shape => Shape::Optional(Box::new(shape)),
                }
            };
            let mut field_name = avro_name(key);
            while fields.iter().any(|f| f.name == field_name) {
                field_name.push('_');
            }
            fields.push(FieldShape { key: key.clone(), name: field_name, shape });
        }
        Arc::new(RecordShape { name, fields, extra: false })
    }
}

fn nullable_of(shape: Shape) -> Shape {
    match shape {
        Shape::Nullable(_) | Shape::Optional(_) | Shape::Json => shape,
        shape => Shape::Nullable(Box::new(shape)),
    }
}

/// Event records from `schemas.events` / `schemas.emitted`: a union keyed by
/// `type`.
fn events_shape(schemas: &Map<String, Value>, namespace: String) -> Result<Shape, String> {
    let mut b = Builder { namespace, names: HashMap::new(), defs: None };
    let mut branches = Vec::new();
    for (event_type, schema) in schemas {
        b.defs = schema.get("$defs").and_then(Value::as_object);
        let resolved = b.resolve(schema);
        if resolved.get("type").and_then(Value::as_str) != Some("object") {
            return Err(format!("the schema of \"{event_type}\" must be an object"));
        }
        branches.push((event_type.clone(), b.record(resolved, &pascal(event_type), 0, Some("type"))));
    }
    if branches.is_empty() {
        return Err("no event schemas".into());
    }
    Ok(Shape::Union(Arc::new(UnionShape { key: "type".into(), branches })))
}

/// The snapshot shape, with the root context typed by `context` when given.
fn snapshot_shape(context: Option<&Value>) -> Shape {
    let ns = "xstate.snapshot.v1";
    let field = |key: &str, shape: Shape| FieldShape {
        key: key.into(),
        name: avro_name(key),
        shape: Shape::Optional(Box::new(shape)),
    };
    let record = |name: &str, fields: Vec<FieldShape>| {
        Arc::new(RecordShape { name: format!("{ns}.{name}"), fields, extra: true })
    };
    let entries = |name: &str, value: Shape| {
        Shape::Entries(Arc::new(RecordShape {
            name: format!("{ns}.{name}"),
            fields: vec![
                FieldShape { key: "key".into(), name: "key".into(), shape: Shape::Str },
                FieldShape { key: "value".into(), name: "value".into(), shape: value },
            ],
            extra: false,
        }))
    };
    let actor_ref = record("ActorRef", vec![field("$actor", Shape::Str), field("incarnation", Shape::Str)]);
    let context = match context {
        Some(schema) => {
            let mut b = Builder {
                namespace: format!("{ns}.context"),
                names: HashMap::new(),
                defs: schema.get("$defs").and_then(Value::as_object),
            };
            b.shape(schema, "Context", 0)
        }
        None => Shape::Json,
    };
    let logical_timer = record(
        "LogicalTimer",
        vec![
            field("id", Shape::Str),
            field("seq", Shape::Long),
            field("delay", Shape::Long),
            field("type", Shape::Str),
            field("event", Shape::Json),
            field("target", Shape::Str),
        ],
    );
    let machine = record(
        "ActorSnapshot",
        vec![
            field("status", Shape::Str),
            field("output", Shape::Json),
            field("error", Shape::Json),
            field("context", context),
            field("value", Shape::Json),
            field("children", entries("ActorRefEntry", Shape::Record(actor_ref.clone()))),
            field("timers", Shape::Array(Box::new(Shape::Record(logical_timer)))),
            field("historyValue", Shape::Json),
            field("_stateInputs", Shape::Json),
            field("_nextTimerId", Shape::Long),
            field("nodeIds", Shape::Array(Box::new(Shape::Str))),
        ],
    );
    let actor = record(
        "ActorEntry",
        vec![
            field("id", Shape::Str),
            field("address", Shape::Str),
            field("incarnation", Shape::Str),
            field("src", Shape::Str),
            field("logicVersion", Shape::Str),
            field("parent", Shape::Record(actor_ref.clone())),
            field("registryKey", Shape::Str),
            field("syncSnapshot", Shape::Bool),
            field("started", Shape::Bool),
            field("input", Shape::Json),
            field("snapshot", Shape::Record(machine)),
        ],
    );
    let system_timer = record(
        "SystemTimer",
        vec![
            field("source", Shape::Record(actor_ref.clone())),
            field("occurrence", Shape::Long),
            field("scheduledAt", Shape::Long),
            field("dueAt", Shape::Long),
            field("sequence", Shape::Long),
            field("event", Shape::Json),
        ],
    );
    let counters = record(
        "Counters",
        vec![
            field("actor", Shape::Long),
            field("timer", Shape::Long),
            field("sequence", Shape::Long),
            field("effect", Shape::Long),
        ],
    );
    Shape::Record(record(
        "SystemSnapshot",
        vec![
            field("root", Shape::Str),
            field("executionId", Shape::Str),
            field("now", Shape::Long),
            field("actors", entries("ActorEntryEntry", Shape::Record(actor))),
            field("registry", entries("RegistryEntry", Shape::Record(actor_ref))),
            field("messages", Shape::Json),
            field("timers", Shape::Array(Box::new(Shape::Record(system_timer)))),
            field("externalEffects", Shape::Json),
            field("counters", Shape::Record(counters)),
        ],
    ))
}

/// A generated schema: its shape, parsed Avro schema, JSON text, and id
/// once registered.
#[derive(Debug)]
pub struct Root {
    shape: Shape,
    schema: Schema,
    json: String,
    /// Each branch alone (for writers that use one record per event type),
    /// by full name.
    branches: Vec<(String, Schema)>,
    id: Option<u32>,
}

impl Root {
    fn new(shape: Shape) -> Result<Self, String> {
        let value = schema_json(&shape, &mut HashSet::new());
        let json = value.to_string();
        let schema = Schema::parse_str(&json).map_err(|e| format!("generated Avro schema: {e}"))?;
        let mut branches = Vec::new();
        if let Shape::Union(u) = &shape {
            for (_, r) in &u.branches {
                let alone = record_json(r, &mut HashSet::new()).to_string();
                branches.push((
                    r.name.clone(),
                    Schema::parse_str(&alone).map_err(|e| format!("generated Avro schema: {e}"))?,
                ));
            }
        }
        Ok(Self { shape, schema, json, branches, id: None })
    }

    /// The Avro schema (JSON text).
    pub fn schema(&self) -> &str {
        &self.json
    }

    /// Its registry id, once registered or looked up.
    pub fn id(&self) -> Option<u32> {
        self.id
    }
}

/// Avro support for one machine: generated schemas, their registry ids, and
/// the writer schemas of records read (by id).
pub struct AvroSchemas {
    registry: Arc<dyn Registry>,
    pub events: Option<Root>,
    pub emitted: Option<Root>,
    pub snapshot: Root,
    writers: Mutex<HashMap<u32, Arc<Schema>>>,
}

impl std::fmt::Debug for AvroSchemas {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AvroSchemas").field("events", &self.events.as_ref().map(|r| r.id)).finish_non_exhaustive()
    }
}

/// Splits a record in the wire format into its schema id and datum.
pub fn split(record: &[u8]) -> Result<(u32, &[u8]), String> {
    match record {
        [MAGIC, a, b, c, d, rest @ ..] => Ok((u32::from_be_bytes([*a, *b, *c, *d]), rest)),
        _ => Err("not in the Schema Registry wire format (magic byte 0, schema id)".into()),
    }
}

#[cfg(test)]
fn frame(id: u32, datum: &[u8], out: &mut Vec<u8>) {
    out.push(MAGIC);
    out.extend_from_slice(&id.to_be_bytes());
    out.extend_from_slice(datum);
}

impl AvroSchemas {
    /// Schemas for a bundle (`{ "machine", "schemas" }`): its events,
    /// emitted events and context (each when declared).
    pub fn from_bundle(bundle: &Value, registry: Arc<dyn Registry>) -> Result<Self, String> {
        let schemas = bundle.get("schemas");
        let id = bundle.get("machine").and_then(|m| m.get("id")).and_then(Value::as_str).unwrap_or("machine");
        let ns = |kind: &str| format!("xstate.{kind}.{}", avro_name(&id.to_ascii_lowercase()));
        let events = match schemas.and_then(|s| s.get("events")).and_then(Value::as_object) {
            Some(e) if !e.is_empty() => {
                Some(Root::new(events_shape(e, ns("events")).map_err(|e| format!("schemas.events: {e}"))?)?)
            }
            _ => None,
        };
        let emitted = match schemas.and_then(|s| s.get("emitted")).and_then(Value::as_object) {
            Some(e) if !e.is_empty() => {
                Some(Root::new(events_shape(e, ns("emitted")).map_err(|e| format!("schemas.emitted: {e}"))?)?)
            }
            _ => None,
        };
        let snapshot = Root::new(snapshot_shape(schemas.and_then(|s| s.get("context"))))?;
        Ok(Self { registry, events, emitted, snapshot, writers: Mutex::new(HashMap::new()) })
    }

    /// Registers the generated schemas under these subjects (or, without
    /// `auto_register`, looks them up), recording their ids.
    pub fn register(&mut self, subjects: [Option<&str>; 3], auto_register: bool) -> Result<(), String> {
        let registry = self.registry.clone();
        for (root, subject) in
            [self.events.as_mut(), self.emitted.as_mut(), Some(&mut self.snapshot)].into_iter().zip(subjects)
        {
            let (Some(root), Some(subject)) = (root, subject) else { continue };
            let id = if auto_register {
                registry.register(subject, &root.json)
            } else {
                registry.lookup(subject, &root.json)
            }
            .map_err(|e| format!("subject {subject}: {e}"))?;
            self.writers.lock().unwrap_or_else(|e| e.into_inner()).insert(id, Arc::new(root.schema.clone()));
            root.id = Some(id);
        }
        Ok(())
    }

    /// The schema a record was written with.
    fn writer(&self, id: u32) -> Result<Arc<Schema>, String> {
        if let Some(s) = self.writers.lock().unwrap_or_else(|e| e.into_inner()).get(&id) {
            return Ok(s.clone());
        }
        let text = self.registry.schema(id)?;
        let schema = Arc::new(Schema::parse_str(&text).map_err(|e| format!("schema {id}: {e}"))?);
        self.writers.lock().unwrap_or_else(|e| e.into_inner()).insert(id, schema.clone());
        Ok(schema)
    }

    fn decode(&self, root: &Root, record: &[u8]) -> Result<Value, String> {
        let (id, mut datum) = split(record)?;
        if Some(id) == root.id {
            let mut r = Reader(datum);
            let value = read(&root.shape, &mut r).map_err(|e| format!("Avro datum (schema {id}): {e}"))?;
            if !r.0.is_empty() {
                return Err(format!("Avro datum (schema {id}): {} bytes left over", r.0.len()));
            }
            return Ok(value);
        }
        let writer = self.writer(id)?;
        let value = if let (Schema::Record(w), Shape::Union(_)) = (&*writer, &root.shape) {
            // One record per event type: resolve against that branch.
            let name = w.name.fullname(None);
            let index = root
                .branches
                .iter()
                .position(|(n, _)| *n == name)
                .ok_or_else(|| format!("record {name} is not one of this machine's events"))?;
            apache_avro::from_avro_datum(&writer, &mut datum, Some(&root.branches[index].1))
                .map(|v| Avro::Union(index as u32, Box::new(v)))
        } else {
            apache_avro::from_avro_datum(&writer, &mut datum, Some(&root.schema))
        }
        .map_err(|e| format!("Avro datum (schema {id}): {e}"))?;
        from_avro(&root.shape, value)
    }

    fn encode(root: &Root, value: &Value, out: &mut Vec<u8>) -> Result<(), String> {
        let id = root.id.ok_or("the schema is not registered")?;
        let start = out.len();
        out.push(MAGIC);
        out.extend_from_slice(&id.to_be_bytes());
        write(&root.shape, value, out).inspect_err(|_| out.truncate(start))
    }

    /// An input event (`{ "type", ... }`) from a record.
    pub fn decode_event(&self, record: &[u8]) -> Result<Value, String> {
        self.decode(self.events.as_ref().ok_or("Avro events need schemas.events")?, record)
    }

    /// An input event as a record (for producers and tests).
    pub fn encode_event(&self, event: &Value) -> Result<Vec<u8>, String> {
        let mut out = Vec::new();
        Self::encode(self.events.as_ref().ok_or("Avro events need schemas.events")?, event, &mut out)?;
        Ok(out)
    }

    /// An emitted event as an output record.
    pub fn encode_emitted(&self, event: &Value) -> Result<Vec<u8>, String> {
        let mut out = Vec::new();
        Self::encode(self.emitted.as_ref().ok_or("Avro outputs need schemas.emitted")?, event, &mut out)?;
        Ok(out)
    }

    /// An output record back as its event (for consumers and tests).
    pub fn decode_emitted(&self, record: &[u8]) -> Result<Value, String> {
        self.decode(self.emitted.as_ref().ok_or("Avro outputs need schemas.emitted")?, record)
    }

    /// Appends a snapshot (XState's `SystemSnapshot` JSON) as a record.
    pub fn encode_snapshot(&self, snapshot: &Value, out: &mut Vec<u8>) -> Result<(), String> {
        Self::encode(&self.snapshot, snapshot, out)
    }

    pub fn decode_snapshot(&self, record: &[u8]) -> Result<Value, String> {
        self.decode(&self.snapshot, record)
    }
}

#[cfg(test)]
mod tests {
    use super::registry::MemoryRegistry;
    use super::*;

    fn checkout() -> Value {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../examples/checkout/machines/checkout.xstate.json");
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
    }

    fn schemas() -> AvroSchemas {
        let mut s = AvroSchemas::from_bundle(&checkout(), Arc::new(MemoryRegistry::default())).unwrap();
        s.register([Some("in-value"), Some("out-value"), Some("changelog-value")], true).unwrap();
        s
    }

    #[test]
    fn events_round_trip() {
        let s = schemas();
        for event in [
            json!({ "type": "cart.add", "line": { "sku": "A-1", "quantity": 3 } }),
            json!({ "type": "checkout", "total": 120.5 }),
            json!({ "type": "payment.method", "method": { "kind": "card", "last4": "4242" } }),
            json!({ "type": "payment.method", "method": { "kind": "invoice" } }),
            json!({ "type": "cancel" }),
        ] {
            let record = s.encode_event(&event).unwrap();
            assert_eq!(record[0], MAGIC);
            assert_eq!(s.decode_event(&record).unwrap(), event);
        }
        assert!(s.encode_event(&json!({ "type": "nope" })).unwrap_err().contains("not in the schema"));
        assert!(s.encode_event(&json!({ "type": "checkout" })).unwrap_err().contains("missing \"total\""));
        assert!(s.encode_event(&json!({ "type": "checkout", "total": 1, "x": 2 })).unwrap_err().contains("\"x\""));
    }

    #[test]
    fn writers_with_other_schemas_are_resolved() {
        let s = schemas();
        // A producer's own schema for one event type, an earlier version
        // without the optional field, registered under its own subject.
        let ns = s.events.as_ref().unwrap().branches[0].0.rsplit_once('.').unwrap().0.to_owned();
        let writer = json!({ "type": "record", "name": format!("{ns}.Checkout"), "fields": [{ "name": "total", "type": "float" }] });
        let id = s.registry.register("checkout-value", &writer.to_string()).unwrap();
        let schema = Schema::parse_str(&writer.to_string()).unwrap();
        let datum =
            apache_avro::to_avro_datum(&schema, Avro::Record(vec![("total".into(), Avro::Float(2.5))])).unwrap();
        let mut record = Vec::new();
        frame(id, &datum, &mut record);
        assert_eq!(s.decode_event(&record).unwrap(), json!({ "type": "checkout", "total": 2.5 }));
    }

    #[test]
    fn snapshots_round_trip_with_typed_context() {
        let s = schemas();
        let snapshot = json!({
            "root": "checkout", "now": 5,
            "actors": { "checkout": {
                "id": "checkout", "src": "root", "incarnation": "0", "started": true, "logicVersion": null,
                "snapshot": { "status": "active", "context": s_context(), "value": "cart", "nodeIds": ["checkout", "checkout.cart"],
                              "_nextTimerId": 0, "somethingNew": [1, 2] } } },
            "timers": [], "counters": { "actor": 1, "timer": 0, "sequence": 3, "effect": 0 }
        });
        let mut record = Vec::new();
        s.encode_snapshot(&snapshot, &mut record).unwrap();
        assert_eq!(s.decode_snapshot(&record).unwrap(), snapshot);
    }

    /// The direct writer writes exactly what `apache-avro` writes for the
    /// same value, and `apache-avro` reads it back: over every recorded
    /// snapshot and the checkout example's events.
    #[test]
    fn the_direct_codec_matches_apache_avro() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures");
        let mut snapshots = Vec::new();
        let mut add = |records: &Value| {
            for r in records.as_array().into_iter().flatten() {
                if let Some(s) = r.get("snapshot").filter(|s| s.is_object()) {
                    snapshots.push(s.clone());
                }
            }
        };
        for entry in std::fs::read_dir(root.join("system")).unwrap() {
            let path = entry.unwrap().path();
            if path.to_string_lossy().ends_with(".expected.json") {
                add(&serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap());
            }
        }
        let corpus: Value =
            serde_json::from_str(&std::fs::read_to_string(root.join("fuzz/system-corpus.json")).unwrap()).unwrap();
        for case in corpus.as_array().unwrap() {
            add(&case["expected"]);
        }
        let s = schemas();
        let events = [
            json!({ "type": "cart.add", "line": { "sku": "A-1", "quantity": 3 } }),
            json!({ "type": "payment.method", "method": { "kind": "invoice" } }),
            json!({ "type": "checkout", "total": 120.5 }),
        ];
        let cases =
            snapshots.iter().map(|v| (&s.snapshot, v)).chain(events.iter().map(|v| (s.events.as_ref().unwrap(), v)));
        let mut n = 0;
        for (root, v) in cases {
            let mut direct = Vec::new();
            write(&root.shape, v, &mut direct).unwrap();
            let reference = apache_avro::to_avro_datum(&root.schema, to_avro(&root.shape, v).unwrap()).unwrap();
            assert_eq!(direct, reference, "{v}");
            let back = apache_avro::from_avro_datum(&root.schema, &mut &direct[..], None).unwrap();
            assert_eq!(&from_avro(&root.shape, back).unwrap(), v);
            n += 1;
        }
        assert!(n > 400, "{n} cases");
    }

    fn s_context() -> Value {
        json!({ "status": "open", "lines": [{ "sku": "A-1", "quantity": 2 }], "coupon": "SPRING" })
    }
}
