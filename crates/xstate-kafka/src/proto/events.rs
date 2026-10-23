//! Input events as protobuf, with messages derived from a machine bundle's
//! event schemas (`schemas.events`, the JSON Schemas `tools/extract` writes
//! from Zod). [`EventSchema::proto_file`] writes the `.proto` file for
//! producers; [`EventSchema::decode`] turns a message into the JSON event
//! the machine handles, so everything after decoding is unchanged.
//!
//! An event is an `Event` envelope with one field per event type (a
//! `oneof`), numbered by the order of `schemas.events`; each event type is a
//! message whose fields are numbered by property order. Either number can be
//! pinned with `.meta({ protoField: n })` in Zod (the `protoField` keyword
//! in JSON Schema), which keeps numbers stable when types or properties are
//! reordered or inserted.
//!
//! Mapping: `string` is `string` (an `enum` of strings is a protobuf enum),
//! `integer` is `sint64` (`uint64` with `minimum >= 0`), `number` is
//! `double`, `boolean` is `bool`, arrays are `repeated`, objects with
//! properties are messages, records (`additionalProperties`) are
//! `map<string, T>`, `T | null` is `optional T`, discriminated unions
//! (`oneOf` objects sharing a `const` key) are a message with a `oneof`, and
//! anything else (`{}`, other unions, tuples, nested arrays) is the generic
//! `Value` message. Optional properties are `optional` fields (they stay
//! absent); a required property that is absent on the wire gets protobuf's
//! default (`0`, `""`, `false`, `[]`, an empty message), or `null` when
//! nullable.

use std::collections::HashMap;
use std::fmt::Write as _;

use serde_json::{Map, Value};

use super::value;
use super::wire::{self, err, DecodeError, Raw, Reader};

#[derive(Debug, Clone, PartialEq)]
enum Ty {
    Str,
    Bool,
    Int,
    UInt,
    Double,
    Enum(usize),
    Msg(usize),
    Map(Box<Ty>),
    Value,
}

#[derive(Debug, Clone)]
struct FieldDef {
    json: String,
    name: String,
    number: u32,
    ty: Ty,
    repeated: bool,
    required: bool,
    nullable: bool,
}

#[derive(Debug, Clone)]
struct Union {
    discriminator: String,
    /// The discriminator value, the field number, the field name and the
    /// variant's message.
    variants: Vec<(String, u32, String, usize)>,
}

#[derive(Debug, Clone)]
struct MessageDef {
    name: String,
    fields: Vec<FieldDef>,
    union: Option<Union>,
}

#[derive(Debug, Clone)]
struct EnumDef {
    name: String,
    values: Vec<String>,
}

#[derive(Debug, Clone)]
struct EventDef {
    event_type: String,
    number: u32,
    field: String,
    message: usize,
}

/// The protobuf form of a machine's input events.
#[derive(Debug, Clone)]
pub struct EventSchema {
    package: String,
    events: Vec<EventDef>,
    by_number: HashMap<u32, usize>,
    by_type: HashMap<String, usize>,
    messages: Vec<MessageDef>,
    enums: Vec<EnumDef>,
}

fn pascal(s: &str) -> String {
    let mut out = String::new();
    let mut upper = true;
    for c in s.chars() {
        if c.is_ascii_alphanumeric() {
            if upper {
                out.push(c.to_ascii_uppercase());
            } else {
                out.push(c);
            }
            upper = false;
        } else {
            upper = true;
        }
    }
    if out.is_empty() || out.starts_with(|c: char| c.is_ascii_digit()) {
        out.insert(0, 'T');
    }
    out
}

fn snake(s: &str) -> String {
    let mut out = String::new();
    let mut prev_lower = false;
    for c in s.chars() {
        if c.is_ascii_uppercase() {
            if prev_lower {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
            prev_lower = false;
        } else if c.is_ascii_alphanumeric() {
            out.push(c);
            prev_lower = c.is_ascii_lowercase() || c.is_ascii_digit();
        } else {
            if !out.ends_with('_') && !out.is_empty() {
                out.push('_');
            }
            prev_lower = false;
        }
    }
    let out = out.trim_end_matches('_').to_owned();
    if out.is_empty() || out.starts_with(|c: char| c.is_ascii_digit()) {
        format!("f_{out}")
    } else {
        out
    }
}

fn upper_snake(s: &str) -> String {
    snake(s).to_ascii_uppercase()
}

/// Builds messages from JSON Schemas.
struct Builder<'a> {
    messages: Vec<MessageDef>,
    enums: Vec<EnumDef>,
    names: HashMap<String, usize>,
    /// `$defs` of the schema being converted.
    defs: Option<&'a Map<String, Value>>,
    /// Converted `$defs`, by name.
    def_types: HashMap<String, Ty>,
}

impl<'a> Builder<'a> {
    fn unique(&mut self, name: String) -> String {
        let n = self.names.entry(name.clone()).or_insert(0);
        *n += 1;
        if *n == 1 {
            name
        } else {
            format!("{name}{n}")
        }
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

    /// The type of `s`, and whether it is nullable and repeated.
    fn ty(&mut self, s: &'a Value, hint: &str, path: &str) -> Result<(Ty, bool, bool), String> {
        if let Some(name) = s.get("$ref").and_then(Value::as_str).and_then(|r| r.strip_prefix("#/$defs/")) {
            if let Some(t) = self.def_types.get(name) {
                return Ok((t.clone(), false, false));
            }
            let target = self.resolve(s);
            let (t, nullable, repeated) = self.ty(target, &pascal(name), path)?;
            if !nullable && !repeated {
                self.def_types.insert(name.to_owned(), t.clone());
            }
            return Ok((t, nullable, repeated));
        }
        // `T | null`
        for key in ["anyOf", "oneOf"] {
            if let Some(options) = s.get(key).and_then(Value::as_array) {
                let non_null: Vec<&Value> =
                    options.iter().filter(|o| o.get("type").and_then(Value::as_str) != Some("null")).collect();
                if non_null.len() + 1 == options.len() && non_null.len() == 1 {
                    let (t, _, repeated) = self.ty(non_null[0], hint, path)?;
                    return Ok((t, !repeated, repeated));
                }
                if let Some(t) = self.union(&non_null, hint, path)? {
                    return Ok((t, non_null.len() < options.len(), false));
                }
                return Ok((Ty::Value, false, false));
            }
        }
        // `"type": ["string", "null"]` is a nullable string.
        let (ty, nullable) = match s.get("type") {
            Some(Value::Array(types)) => {
                let non_null: Vec<&str> = types.iter().filter_map(Value::as_str).filter(|t| *t != "null").collect();
                if non_null.len() == 1 && non_null.len() < types.len() {
                    (non_null[0], true)
                } else {
                    return Ok((Ty::Value, false, false));
                }
            }
            Some(Value::String(t)) => (t.as_str(), false),
            _ => return Ok((Ty::Value, false, false)),
        };
        let (t, _, repeated) = self.ty_of(s, ty, hint, path)?;
        Ok((t, nullable && !repeated, repeated))
    }

    /// The type of `s` taken as JSON Schema type `ty`.
    fn ty_of(&mut self, s: &'a Value, ty: &str, hint: &str, path: &str) -> Result<(Ty, bool, bool), String> {
        Ok(match ty {
            "string" => match s.get("enum").and_then(Value::as_array) {
                Some(values) if values.iter().all(Value::is_string) => {
                    let values: Vec<String> = values.iter().filter_map(|v| v.as_str().map(str::to_owned)).collect();
                    let name = self.unique(hint.to_owned());
                    self.enums.push(EnumDef { name, values });
                    (Ty::Enum(self.enums.len() - 1), false, false)
                }
                _ => (Ty::Str, false, false),
            },
            "boolean" => (Ty::Bool, false, false),
            "integer" => {
                let unsigned = s.get("minimum").and_then(Value::as_f64).is_some_and(|m| m >= 0.0)
                    || s.get("exclusiveMinimum").and_then(Value::as_f64).is_some_and(|m| m >= -1.0);
                (if unsigned { Ty::UInt } else { Ty::Int }, false, false)
            }
            "number" => (Ty::Double, false, false),
            "null" => (Ty::Value, false, false),
            "array" => {
                let Some(items) = s.get("items").filter(|i| i.is_object()) else {
                    return Ok((Ty::Value, false, false));
                };
                if s.get("prefixItems").is_some() {
                    return Ok((Ty::Value, false, false));
                }
                let (t, nullable, repeated) = self.ty(items, &format!("{hint}Item"), path)?;
                if nullable || repeated {
                    // Repeated of repeated (or of nullable) has no protobuf form.
                    (Ty::Value, false, false)
                } else {
                    (t, false, true)
                }
            }
            "object" => {
                let props = s.get("properties").and_then(Value::as_object);
                match props {
                    Some(p) if !p.is_empty() => (Ty::Msg(self.message(s, hint, path, None)?), false, false),
                    _ => match s.get("additionalProperties").filter(|a| a.is_object()) {
                        Some(values) => {
                            let (t, nullable, repeated) = self.ty(values, &format!("{hint}Value"), path)?;
                            match t {
                                Ty::Map(_) => (Ty::Value, false, false),
                                _ if nullable || repeated => (Ty::Value, false, false),
                                t => (Ty::Map(Box::new(t)), false, false),
                            }
                        }
                        // An object without properties: an empty message
                        // would drop extra keys, so keep it generic.
                        None => (Ty::Value, false, false),
                    },
                }
            }
            _ => (Ty::Value, false, false),
        })
    }

    /// A discriminated union of objects: a message with a `oneof`.
    fn union(&mut self, options: &[&'a Value], hint: &str, path: &str) -> Result<Option<Ty>, String> {
        let objects: Vec<&'a Value> = options.iter().map(|o| self.resolve(o)).collect();
        if objects.len() < 2 || !objects.iter().all(|o| o.get("type").and_then(Value::as_str) == Some("object")) {
            return Ok(None);
        }
        let first = objects[0].get("properties").and_then(Value::as_object);
        let Some(discriminator) = first.and_then(|p| {
            p.iter()
                .find(|(k, _)| {
                    objects.iter().all(|o| {
                        o.get("properties")
                            .and_then(|p| p.get(k.as_str()))
                            .and_then(|d| d.get("const"))
                            .is_some_and(Value::is_string)
                            && o.get("required")
                                .and_then(Value::as_array)
                                .is_some_and(|r| r.iter().any(|x| x == k.as_str()))
                    })
                })
                .map(|(k, _)| k.clone())
        }) else {
            return Ok(None);
        };
        let name = self.unique(hint.to_owned());
        self.messages.push(MessageDef { name: name.clone(), fields: Vec::new(), union: None });
        let index = self.messages.len() - 1;
        let mut variants = Vec::new();
        for (i, o) in objects.iter().enumerate() {
            let value = o["properties"][discriminator.as_str()]["const"].as_str().unwrap_or_default().to_owned();
            let number = o.get("protoField").and_then(Value::as_u64).map_or(i as u32 + 1, |n| n as u32);
            let msg = self.message(o, &format!("{name}{}", pascal(&value)), path, Some(&discriminator))?;
            variants.push((value.clone(), number, snake(&value), msg));
        }
        check_numbers(variants.iter().map(|v| v.1), path)?;
        self.messages[index].union = Some(Union { discriminator, variants });
        Ok(Some(Ty::Msg(index)))
    }

    /// A message for an object schema (without `skip`, a union's
    /// discriminator).
    fn message(&mut self, s: &'a Value, hint: &str, path: &str, skip: Option<&str>) -> Result<usize, String> {
        let name = self.unique(hint.to_owned());
        self.messages.push(MessageDef { name: name.clone(), fields: Vec::new(), union: None });
        let index = self.messages.len() - 1;
        let required: Vec<&str> =
            s.get("required").and_then(Value::as_array).into_iter().flatten().filter_map(Value::as_str).collect();
        let mut fields = Vec::new();
        let mut used_names = HashMap::new();
        for (i, (key, prop)) in s.get("properties").and_then(Value::as_object).into_iter().flatten().enumerate() {
            if Some(key.as_str()) == skip {
                continue;
            }
            let number = prop.get("protoField").and_then(Value::as_u64).map_or(i as u32 + 1, |n| n as u32);
            let fpath = format!("{path}.properties.{key}");
            let (ty, nullable, repeated) = self.ty(prop, &format!("{name}{}", pascal(key)), &fpath)?;
            let mut fname = snake(key);
            let count = used_names.entry(fname.clone()).or_insert(0);
            *count += 1;
            if *count > 1 {
                fname = format!("{fname}_{count}");
            }
            fields.push(FieldDef {
                json: key.clone(),
                name: fname,
                number,
                ty,
                repeated,
                required: required.contains(&key.as_str()),
                nullable,
            });
        }
        check_numbers(fields.iter().map(|f| f.number), path)?;
        self.messages[index].fields = fields;
        Ok(index)
    }
}

fn check_numbers(numbers: impl Iterator<Item = u32>, path: &str) -> Result<(), String> {
    let mut seen = Vec::new();
    for n in numbers {
        if n == 0 || n > 536_870_911 || (19_000..=19_999).contains(&n) {
            return Err(format!("{path}: field number {n} is not allowed in protobuf"));
        }
        if seen.contains(&n) {
            return Err(format!("{path}: field number {n} is used twice (check protoField)"));
        }
        seen.push(n);
    }
    Ok(())
}

impl EventSchema {
    /// The schema for a bundle (`{ "machine", "schemas" }`): its public
    /// events (`schemas.events`).
    pub fn from_bundle(bundle: &Value) -> Result<Self, String> {
        let events = bundle
            .get("schemas")
            .and_then(|s| s.get("events"))
            .and_then(Value::as_object)
            .ok_or("protobuf events need a bundle with schemas.events (written by tools/extract)")?;
        let id = bundle.get("machine").and_then(|m| m.get("id")).and_then(Value::as_str).unwrap_or("machine");
        let mut b = Builder {
            messages: Vec::new(),
            enums: Vec::new(),
            names: HashMap::new(),
            defs: None,
            def_types: HashMap::new(),
        };
        // Reserved message names.
        for reserved in ["Event", "Value", "List", "Object", "Field", "NullValue"] {
            b.names.insert(reserved.into(), 1);
        }
        let mut defs = Vec::new();
        for (i, (event_type, schema)) in events.iter().enumerate() {
            let path = format!("schemas.events.{event_type}");
            b.defs = schema.get("$defs").and_then(Value::as_object);
            b.def_types.clear();
            let resolved = b.resolve(schema);
            if resolved.get("type").and_then(Value::as_str) != Some("object") {
                return Err(format!("{path}: an event schema must be an object"));
            }
            let message = b.message(resolved, &pascal(event_type), &path, Some("type"))?;
            let number = schema.get("protoField").and_then(Value::as_u64).map_or(i as u32 + 1, |n| n as u32);
            defs.push(EventDef { event_type: event_type.clone(), number, field: snake(event_type), message });
        }
        check_numbers(defs.iter().map(|d| d.number), "schemas.events")?;
        let mut field_names: HashMap<&str, usize> = HashMap::new();
        for d in &defs {
            *field_names.entry(&d.field).or_default() += 1;
        }
        if let Some((name, _)) = field_names.iter().find(|(_, n)| **n > 1) {
            return Err(format!("schemas.events: event types map to the same field name `{name}`"));
        }
        let by_number = defs.iter().enumerate().map(|(i, d)| (d.number, i)).collect();
        let by_type = defs.iter().enumerate().map(|(i, d)| (d.event_type.clone(), i)).collect();
        let package = format!("xstate.events.{}", snake(id));
        Ok(Self { package, events: defs, by_number, by_type, messages: b.messages, enums: b.enums })
    }

    /// The `.proto` file producers compile.
    pub fn proto_file(&self) -> String {
        let mut out = format!(
            "// Input events of machine `{}`, generated by xstate-kafka from the\n\
             // bundle's event schemas. Field numbers follow schema order unless\n\
             // pinned with protoField.\n\
             syntax = \"proto3\";\n\npackage {};\n\n",
            self.package.trim_start_matches("xstate.events."),
            self.package
        );
        out.push_str("// One event: exactly one field is set.\nmessage Event {\n  oneof event {\n");
        for d in &self.events {
            let _ = writeln!(
                out,
                "    {} {} = {}; // \"{}\"",
                self.messages[d.message].name, d.field, d.number, d.event_type
            );
        }
        out.push_str("  }\n}\n");
        for m in &self.messages {
            let _ = write!(out, "\nmessage {} {{\n", m.name);
            if let Some(u) = &m.union {
                let _ = writeln!(
                    out,
                    "  // \"{}\" selects the variant.\n  oneof {} {{",
                    u.discriminator,
                    snake(&u.discriminator)
                );
                for (value, number, field, msg) in &u.variants {
                    let _ = writeln!(out, "    {} {field} = {number}; // \"{value}\"", self.messages[*msg].name);
                }
                out.push_str("  }\n");
            }
            for f in &m.fields {
                let ty = self.proto_type(&f.ty);
                let label = if f.repeated {
                    "repeated "
                } else if matches!(f.ty, Ty::Map(_)) || matches!(f.ty, Ty::Msg(_) | Ty::Value) {
                    ""
                } else if !f.required || f.nullable {
                    "optional "
                } else {
                    ""
                };
                let note = if f.json != f.name { format!(" // \"{}\"", f.json) } else { String::new() };
                let _ = writeln!(out, "  {label}{ty} {} = {};{note}", f.name, f.number);
            }
            out.push_str("}\n");
        }
        for e in &self.enums {
            let prefix = upper_snake(&e.name);
            let _ = write!(out, "\nenum {} {{\n  {prefix}_UNSPECIFIED = 0;\n", e.name);
            for (i, v) in e.values.iter().enumerate() {
                let _ = writeln!(out, "  {prefix}_{} = {}; // \"{v}\"", upper_snake(v), i + 1);
            }
            out.push_str("}\n");
        }
        out.push_str(&super::snapshot::value_proto());
        out
    }

    fn proto_type(&self, ty: &Ty) -> String {
        match ty {
            Ty::Str => "string".into(),
            Ty::Bool => "bool".into(),
            Ty::Int => "sint64".into(),
            Ty::UInt => "uint64".into(),
            Ty::Double => "double".into(),
            Ty::Enum(i) => self.enums[*i].name.clone(),
            Ty::Msg(i) => self.messages[*i].name.clone(),
            Ty::Map(v) => format!("map<string, {}>", self.proto_type(v)),
            Ty::Value => "Value".into(),
        }
    }

    /// Decodes an `Event` message into the JSON event (`type` first).
    pub fn decode(&self, bytes: &[u8]) -> Result<Value, DecodeError> {
        let mut r = Reader::new(bytes);
        let mut found = None;
        while let Some((number, raw)) = r.next()? {
            if let Some(&i) = self.by_number.get(&number) {
                found = Some((i, raw.bytes()?));
            }
        }
        let Some((i, body)) = found else { return err("no known event type is set") };
        let d = &self.events[i];
        let mut event = Map::new();
        event.insert("type".into(), Value::String(d.event_type.clone()));
        self.decode_message(body, d.message, &mut event)?;
        Ok(Value::Object(event))
    }

    fn decode_message(&self, bytes: &[u8], index: usize, out: &mut Map<String, Value>) -> Result<(), DecodeError> {
        let m = &self.messages[index];
        let mut values: Vec<Option<Value>> = vec![None; m.fields.len()];
        let mut r = Reader::new(bytes);
        let mut variant: Option<(usize, &[u8])> = None;
        while let Some((number, raw)) = r.next()? {
            if let Some(u) = &m.union {
                if let Some(v) = u.variants.iter().position(|v| v.1 == number) {
                    variant = Some((v, raw.bytes()?));
                    continue;
                }
            }
            let Some(fi) = m.fields.iter().position(|f| f.number == number) else { continue };
            let f = &m.fields[fi];
            if f.repeated {
                let items = values[fi].get_or_insert_with(|| Value::Array(Vec::new()));
                let Value::Array(items) = items else { unreachable!() };
                match (&f.ty, raw) {
                    // Packed scalars.
                    (Ty::Bool | Ty::Int | Ty::UInt | Ty::Enum(_), Raw::Len(b)) => {
                        for v in wire::packed_varints(b)? {
                            if let Some(v) = self.scalar(&f.ty, Raw::Varint(v))? {
                                items.push(v);
                            }
                        }
                    }
                    (Ty::Double, Raw::Len(b)) => {
                        for v in wire::packed_doubles(b)? {
                            items.push(json_f64(v)?);
                        }
                    }
                    _ => {
                        if let Some(v) = self.single(&f.ty, raw)? {
                            items.push(v);
                        }
                    }
                }
            } else if let Ty::Map(vt) = &f.ty {
                let map = values[fi].get_or_insert_with(|| Value::Object(Map::new()));
                let Value::Object(map) = map else { unreachable!() };
                let mut er = Reader::new(raw.bytes()?);
                let (mut k, mut v) = (String::new(), None);
                while let Some((n, raw)) = er.next()? {
                    match n {
                        1 => k = raw.str()?.to_owned(),
                        2 => v = self.single(vt, raw)?,
                        _ => {}
                    }
                }
                let v = match v {
                    Some(v) => v,
                    None => self.default(vt),
                };
                map.insert(k, v);
            } else {
                values[fi] = self.single(&f.ty, raw)?;
            }
        }
        if let (Some(u), Some((v, body))) = (&m.union, variant) {
            let (value, _, _, msg) = &u.variants[v];
            out.insert(u.discriminator.clone(), Value::String(value.clone()));
            self.decode_message(body, *msg, out)?;
        }
        for (f, v) in m.fields.iter().zip(values) {
            let v = match v {
                Some(v) => v,
                None if !f.required => continue,
                None if f.nullable => Value::Null,
                None if f.repeated => Value::Array(Vec::new()),
                // An unset enum has no JSON value: leave it out (the
                // machine sees a missing field).
                None if matches!(f.ty, Ty::Enum(_)) => continue,
                None => self.default(&f.ty),
            };
            out.insert(f.json.clone(), v);
        }
        Ok(())
    }

    fn default(&self, ty: &Ty) -> Value {
        match ty {
            Ty::Str => Value::String(String::new()),
            Ty::Bool => Value::Bool(false),
            Ty::Int | Ty::UInt => Value::from(0),
            Ty::Double => Value::from(0.0),
            Ty::Enum(_) | Ty::Value => Value::Null,
            Ty::Map(_) => Value::Object(Map::new()),
            Ty::Msg(i) => {
                let mut m = Map::new();
                let _ = self.decode_message(&[], *i, &mut m);
                Value::Object(m)
            }
        }
    }

    /// One non-repeated value; `None` for an enum value out of range.
    fn single(&self, ty: &Ty, raw: Raw<'_>) -> Result<Option<Value>, DecodeError> {
        match ty {
            Ty::Str => Ok(Some(Value::String(raw.str()?.to_owned()))),
            Ty::Double => Ok(Some(json_f64(raw.double()?)?)),
            Ty::Msg(i) => {
                let mut m = Map::new();
                self.decode_message(raw.bytes()?, *i, &mut m)?;
                Ok(Some(Value::Object(m)))
            }
            Ty::Value => Ok(Some(value::decode_raw(raw)?)),
            Ty::Map(_) => err("a map inside a map"),
            _ => self.scalar(ty, raw),
        }
    }

    fn scalar(&self, ty: &Ty, raw: Raw<'_>) -> Result<Option<Value>, DecodeError> {
        let v = raw.varint()?;
        Ok(Some(match ty {
            Ty::Bool => Value::Bool(v != 0),
            Ty::Int => Value::from(wire::unzigzag(v)),
            Ty::UInt => Value::from(v),
            Ty::Enum(i) => {
                let values = &self.enums[*i].values;
                match usize::try_from(v).ok().and_then(|v| v.checked_sub(1)).and_then(|v| values.get(v)) {
                    Some(s) => Value::String(s.clone()),
                    None => return Ok(None),
                }
            }
            _ => return err("unexpected varint"),
        }))
    }

    /// Encodes a JSON event as an `Event` message (for producers written in
    /// Rust, tools and tests). Fields the schema does not describe are
    /// rejected, as are values of the wrong type.
    pub fn encode(&self, event: &Value) -> Result<Vec<u8>, String> {
        let ty = event.get("type").and_then(Value::as_str).ok_or("an event needs a string \"type\"")?;
        let &i = self.by_type.get(ty).ok_or_else(|| format!("event type \"{ty}\" is not in schemas.events"))?;
        let d = &self.events[i];
        let mut out = Vec::new();
        let start = wire::begin_len(&mut out, d.number);
        self.encode_message(&mut out, d.message, event, Some("type"))?;
        wire::end_len(&mut out, start);
        Ok(out)
    }

    fn encode_message(&self, out: &mut Vec<u8>, index: usize, v: &Value, skip: Option<&str>) -> Result<(), String> {
        let m = &self.messages[index];
        let obj = v.as_object().ok_or_else(|| format!("{}: expected an object", m.name))?;
        if let Some(u) = &m.union {
            let tag = obj
                .get(&u.discriminator)
                .and_then(Value::as_str)
                .ok_or_else(|| format!("{}: missing \"{}\"", m.name, u.discriminator))?;
            let (_, number, _, msg) = u
                .variants
                .iter()
                .find(|x| x.0 == tag)
                .ok_or_else(|| format!("{}: unknown \"{}\" value \"{tag}\"", m.name, u.discriminator))?;
            let start = wire::begin_len(out, *number);
            self.encode_message(out, *msg, v, Some(&u.discriminator))?;
            wire::end_len(out, start);
        }
        for (k, value) in obj {
            if Some(k.as_str()) == skip || m.union.as_ref().is_some_and(|u| &u.discriminator == k) {
                continue;
            }
            if m.union
                .as_ref()
                .is_some_and(|u| u.variants.iter().any(|x| self.messages[x.3].fields.iter().any(|f| &f.json == k)))
            {
                continue;
            }
            let f =
                m.fields.iter().find(|f| &f.json == k).ok_or_else(|| format!("{}: unknown field \"{k}\"", m.name))?;
            if value.is_null() && f.nullable {
                continue;
            }
            if f.repeated {
                for item in value.as_array().ok_or_else(|| format!("{}.{k}: expected an array", m.name))? {
                    self.encode_single(out, &f.ty, f.number, item, k)?;
                }
            } else if let Ty::Map(vt) = &f.ty {
                for (mk, mv) in value.as_object().ok_or_else(|| format!("{}.{k}: expected an object", m.name))? {
                    let start = wire::begin_len(out, f.number);
                    wire::put_str(out, 1, mk);
                    self.encode_single(out, vt, 2, mv, k)?;
                    wire::end_len(out, start);
                }
            } else {
                self.encode_single(out, &f.ty, f.number, value, k)?;
            }
        }
        Ok(())
    }

    fn encode_single(&self, out: &mut Vec<u8>, ty: &Ty, number: u32, v: &Value, key: &str) -> Result<(), String> {
        let bad = || format!("\"{key}\": {v} does not fit {}", self.proto_type(ty));
        match ty {
            Ty::Str => wire::put_str(out, number, v.as_str().ok_or_else(bad)?),
            Ty::Bool => wire::put_bool(out, number, v.as_bool().ok_or_else(bad)?),
            Ty::Int => wire::put_sint(out, number, v.as_i64().ok_or_else(bad)?),
            Ty::UInt => wire::put_uint(out, number, v.as_u64().ok_or_else(bad)?),
            Ty::Double => wire::put_double(out, number, v.as_f64().ok_or_else(bad)?),
            Ty::Enum(i) => {
                let s = v.as_str().ok_or_else(bad)?;
                let n = self.enums[*i].values.iter().position(|x| x == s).ok_or_else(bad)?;
                wire::put_uint(out, number, n as u64 + 1);
            }
            Ty::Msg(i) => {
                let start = wire::begin_len(out, number);
                self.encode_message(out, *i, v, None)?;
                wire::end_len(out, start);
            }
            Ty::Value => value::encode_field(out, number, v),
            Ty::Map(_) => return Err(bad()),
        }
        Ok(())
    }
}

fn json_f64(v: f64) -> Result<Value, DecodeError> {
    serde_json::Number::from_f64(v).map(Value::Number).ok_or_else(|| DecodeError("NaN or infinity".into()))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::EventSchema;

    fn bundle() -> serde_json::Value {
        json!({
            "machine": { "id": "checkout" },
            "schemas": { "events": {
                "cart.add": { "type": "object", "properties": {
                    "type": { "type": "string", "const": "cart.add" },
                    "sku": { "type": "string" },
                    "qty": { "type": "integer", "minimum": 1 },
                    "price": { "type": "number" },
                    "tags": { "type": "array", "items": { "type": "string" } },
                    "note": { "anyOf": [{ "type": "string" }, { "type": "null" }] },
                    "attrs": { "type": "object", "additionalProperties": { "type": "integer", "minimum": -10 } },
                    "channel": { "type": "string", "enum": ["web", "pos"] },
                    "coupon": { "type": "string" },
                    "meta": {}
                }, "required": ["type", "sku", "qty", "price", "tags", "note", "attrs", "channel"] },
                "payment.method": { "type": "object", "properties": {
                    "type": { "type": "string", "const": "payment.method" },
                    "method": { "oneOf": [
                        { "type": "object", "properties": { "kind": { "type": "string", "const": "card" }, "last4": { "type": "string" } }, "required": ["kind", "last4"] },
                        { "type": "object", "properties": { "kind": { "type": "string", "const": "invoice" }, "reference": { "type": "string" } }, "required": ["kind"] }
                    ] },
                    "points": { "type": "array", "items": { "type": "number" } }
                }, "required": ["type", "method"], "protoField": 9 },
                "cancel": { "type": "object", "properties": { "type": { "type": "string", "const": "cancel" } }, "required": ["type"] }
            } }
        })
    }

    #[test]
    fn events_round_trip_and_defaults_apply() {
        let schema = EventSchema::from_bundle(&bundle()).unwrap();
        for event in [
            json!({ "type": "cart.add", "sku": "A-1", "qty": 2, "price": 9.5, "tags": ["x", "y"], "note": "hi",
                    "attrs": { "a": -3, "b": 4 }, "channel": "pos", "coupon": "C", "meta": { "any": [1, null] } }),
            json!({ "type": "payment.method", "method": { "kind": "card", "last4": "4242" }, "points": [1.5, 2.0] }),
            json!({ "type": "payment.method", "method": { "kind": "invoice" } }),
            json!({ "type": "cancel" }),
        ] {
            let bytes = schema.encode(&event).unwrap();
            assert_eq!(schema.decode(&bytes).unwrap(), event, "{event}");
        }
        // Required fields absent on the wire get protobuf's defaults; a
        // nullable one is null; optional ones stay absent.
        let bytes = schema.encode(&json!({ "type": "cart.add", "sku": "", "channel": "web" })).unwrap();
        assert_eq!(
            schema.decode(&bytes).unwrap(),
            json!({ "type": "cart.add", "sku": "", "qty": 0, "price": 0.0, "tags": [], "note": null, "attrs": {}, "channel": "web" })
        );
        // Much smaller than JSON.
        let add = json!({ "type": "cart.add", "sku": "SKU-12345", "qty": 2, "price": 19.95, "tags": [], "note": null, "attrs": {}, "channel": "web" });
        assert!(schema.encode(&add).unwrap().len() * 3 < add.to_string().len());
        assert!(schema.decode(&[0x08, 0x01]).is_err(), "no event field");
        assert!(schema.encode(&json!({ "type": "cancel", "extra": 1 })).is_err());
    }

    #[test]
    fn the_proto_file_numbers_fields_by_order_or_protofield() {
        let proto = EventSchema::from_bundle(&bundle()).unwrap().proto_file();
        for line in [
            "package xstate.events.checkout;",
            "    CartAdd cart_add = 1; // \"cart.add\"",
            "    PaymentMethod payment_method = 9; // \"payment.method\"",
            "    Cancel cancel = 3; // \"cancel\"",
            "  string sku = 2;",
            "  uint64 qty = 3;",
            "  repeated string tags = 5;",
            "  optional string note = 6;",
            "  map<string, sint64> attrs = 7;",
            "  CartAddChannel channel = 8;",
            "  optional string coupon = 9;",
            "  Value meta = 10;",
            "  oneof kind {",
            "    PaymentMethodMethodCard card = 1; // \"card\"",
            "  repeated double points = 3;",
            "  CART_ADD_CHANNEL_POS = 2; // \"pos\"",
        ] {
            assert!(proto.contains(line), "missing `{line}` in\n{proto}");
        }
        let mut bad = bundle();
        bad["schemas"]["events"]["cancel"]["protoField"] = json!(9);
        assert!(EventSchema::from_bundle(&bad).unwrap_err().contains("used twice"));
    }
}
