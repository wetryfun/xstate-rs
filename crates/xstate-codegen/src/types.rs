//! Rust types from the JSON Schemas Zod 4 produces, and a validator for the
//! same subset.
//!
//! Mapping: `string` is `String` (a string `enum` or `const` is a Rust enum),
//! `integer` is `i64`, `number` is `f64`, `boolean` is `bool`, arrays are
//! `Vec` (tuples with `prefixItems`), objects with properties are structs
//! (optional properties are `Option`), records are `IndexMap<String, _>`,
//! nullable types are `Option`, discriminated unions are tagged enums, and
//! unconstrained schemas are `serde_json::Value`. Intersections, untagged
//! unions and other combinators are rejected.

use std::collections::{HashMap, HashSet};

use serde_json::{Map, Value};

use crate::names::{pascal, raw_lit, snake, string_lit, unraw};
use crate::Error;

pub const VALUE: &str = "__rt::serde_json::Value";
const DERIVE: &str = "#[derive(Debug, Clone, PartialEq, __rt::serde::Serialize, __rt::serde::Deserialize)]";
const DERIVE_UNIT: &str =
    "#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, __rt::serde::Serialize, __rt::serde::Deserialize)]";
const SERDE_CRATE: &str = "#[serde(crate = \"__rt::serde\")]";

/// One top-level schema with its `$defs`.
#[derive(Clone, Copy)]
pub struct Root<'a> {
    pub schema: &'a Value,
    /// The Rust name of the root type, for `"$ref": "#"`.
    pub name: &'a str,
}

impl<'a> Root<'a> {
    fn def<'r>(&self, reference: &'r str) -> Option<(&'r str, &'a Value)> {
        let name = reference.strip_prefix("#/$defs/")?;
        self.schema.get("$defs")?.get(name).map(|d| (name, d))
    }
}

/// A field of a struct or a struct variant.
pub struct Field {
    pub lines: Vec<String>,
}

#[derive(Default)]
pub struct Types {
    /// Emitted items, in order.
    pub out: Vec<String>,
    names: HashSet<String>,
    /// `$defs` already generated: name in the schema -> (schema, Rust name).
    defs: HashMap<String, (Value, String)>,
    /// Definitions being generated: a reference to one is recursive.
    in_progress: HashSet<String>,
    defaults: usize,
}

fn unsupported(path: &str, what: &str) -> Error {
    Error::new(path, format!("{what} is not supported in schemas for Rust"))
}

fn doc(o: &Map<String, Value>, indent: &str) -> String {
    match o.get("description").and_then(Value::as_str) {
        Some(d) => d.lines().map(|l| format!("{indent}/// {l}\n")).collect(),
        None => String::new(),
    }
}

impl Types {
    /// Reserves a type name.
    pub fn claim(&mut self, name: &str, path: &str) -> Result<(), Error> {
        if !self.names.insert(name.to_owned()) {
            return Err(Error::new(
                path,
                format!("the generated type name `{name}` is used twice; rename one of the sources"),
            ));
        }
        Ok(())
    }

    /// Generates the type for a top-level schema under `name`. When the
    /// schema is not a struct or enum, emits `pub type name = ...;`.
    pub fn named(&mut self, schema: &Value, name: &str, path: &str) -> Result<(), Error> {
        let root = Root { schema, name };
        let t = self.ty(schema, name, path, root)?;
        if t != name {
            self.claim(name, path)?;
            self.out.push(format!("pub type {name} = {t};\n"));
        }
        Ok(())
    }

    /// The Rust type expression for `s`, generating named types as needed
    /// (named after `hint`).
    pub fn ty(&mut self, s: &Value, hint: &str, path: &str, root: Root<'_>) -> Result<String, Error> {
        let o = match s {
            Value::Bool(true) => return Ok(VALUE.into()),
            Value::Object(o) => o,
            _ => return Err(unsupported(path, "this schema")),
        };
        if let Some(r) = o.get("$ref") {
            return self.reference(r.as_str().unwrap_or_default(), hint, path, root);
        }
        if o.contains_key("allOf") {
            return Err(unsupported(path, "an intersection (allOf)"));
        }
        for k in ["not", "if", "then", "else", "patternProperties", "dependentSchemas", "dependentRequired"] {
            if o.contains_key(k) {
                return Err(unsupported(path, &format!("`{k}`")));
            }
        }
        if let Some(u) = o.get("anyOf").or_else(|| o.get("oneOf")) {
            return self.union(u.as_array().map(Vec::as_slice).unwrap_or_default(), o, hint, path, root);
        }
        if let Some(c) = o.get("const") {
            return match c {
                Value::String(c) => self.string_enum(std::slice::from_ref(c), o, hint, path),
                Value::Null => Ok("()".into()),
                _ => Err(unsupported(path, "a non-string literal")),
            };
        }
        if let Some(e) = o.get("enum").and_then(Value::as_array) {
            let nullable = e.iter().any(Value::is_null);
            let values: Vec<String> = e.iter().filter_map(|v| v.as_str().map(str::to_owned)).collect();
            if values.len() + usize::from(nullable) != e.len() {
                return Err(unsupported(path, "an enum of non-string values"));
            }
            let t = self.string_enum(&values, o, hint, path)?;
            return Ok(if nullable { format!("Option<{t}>") } else { t });
        }
        match o.get("type") {
            None => Ok(VALUE.into()),
            Some(Value::String(t)) => self.typed(t, o, hint, path, root),
            Some(Value::Array(ts)) => {
                let non_null: Vec<&str> = ts.iter().filter_map(Value::as_str).filter(|t| *t != "null").collect();
                match non_null.as_slice() {
                    [t] if ts.len() == 2 => Ok(format!("Option<{}>", self.typed(t, o, hint, path, root)?)),
                    [t] => self.typed(t, o, hint, path, root),
                    _ => Err(unsupported(path, "an untagged union of types")),
                }
            }
            Some(_) => Err(unsupported(path, "this `type`")),
        }
    }

    fn typed(
        &mut self,
        t: &str,
        o: &Map<String, Value>,
        hint: &str,
        path: &str,
        root: Root<'_>,
    ) -> Result<String, Error> {
        Ok(match t {
            "string" => "String".into(),
            "integer" => "i64".into(),
            "number" => "f64".into(),
            "boolean" => "bool".into(),
            "null" => "()".into(),
            "array" => {
                if let Some(items) = o.get("prefixItems").and_then(Value::as_array) {
                    let mut parts = Vec::new();
                    for (i, item) in items.iter().enumerate() {
                        parts.push(self.ty(item, &format!("{hint}{i}"), &format!("{path}.prefixItems[{i}]"), root)?);
                    }
                    format!("({},)", parts.join(", "))
                } else {
                    match o.get("items") {
                        Some(items) => {
                            format!("Vec<{}>", self.ty(items, &format!("{hint}Item"), &format!("{path}.items"), root)?)
                        }
                        None => format!("Vec<{VALUE}>"),
                    }
                }
            }
            "object" => self.object(o, hint, path, root)?,
            other => return Err(unsupported(path, &format!("type `{other}`"))),
        })
    }

    fn reference(&mut self, r: &str, hint: &str, path: &str, root: Root<'_>) -> Result<String, Error> {
        if r == "#" {
            return Ok(format!("Box<{}>", root.name));
        }
        let (name, def) = root.def(r).ok_or_else(|| Error::new(path, format!("unresolved reference `{r}`")))?;
        // Ids Zod generates (`__schema0`) are local to one schema; explicit
        // ids (`.meta({ id })`) name one shared type.
        let key = if name.starts_with("__") { format!("{}/{name}", root.name) } else { name.to_owned() };
        if let Some((schema, rust)) = self.defs.get(&key) {
            if schema != def {
                return Err(Error::new(path, format!("two different schemas share the id `{name}`")));
            }
            let recursive = self.in_progress.contains(rust);
            return Ok(if recursive { format!("Box<{rust}>") } else { rust.clone() });
        }
        let rust = if name.starts_with("__") { hint.to_owned() } else { pascal(name) };
        self.defs.insert(key, (def.clone(), rust.clone()));
        self.in_progress.insert(rust.clone());
        let t = self.ty(def, &rust, &format!("{path}{{$defs.{name}}}"), root);
        self.in_progress.remove(&rust);
        let t = t?;
        if t != rust {
            self.claim(&rust, path)?;
            self.out.push(format!("pub type {rust} = {t};\n"));
        }
        Ok(rust)
    }

    fn string_enum(
        &mut self,
        values: &[String],
        o: &Map<String, Value>,
        hint: &str,
        path: &str,
    ) -> Result<String, Error> {
        self.claim(hint, path)?;
        let mut item = doc(o, "");
        item.push_str(&format!("{DERIVE_UNIT}\n{SERDE_CRATE}\npub enum {hint} {{\n"));
        let mut seen = HashSet::new();
        for v in values {
            let variant = pascal(v);
            if !seen.insert(variant.clone()) {
                return Err(Error::new(path, format!("enum values map to the same variant `{variant}`")));
            }
            if variant != *v {
                item.push_str(&format!("    #[serde(rename = {})]\n", string_lit(v)));
            }
            item.push_str(&format!("    {variant},\n"));
        }
        item.push_str("}\n");
        self.out.push(item);
        Ok(hint.to_owned())
    }

    fn union(
        &mut self,
        variants: &[Value],
        o: &Map<String, Value>,
        hint: &str,
        path: &str,
        root: Root<'_>,
    ) -> Result<String, Error> {
        let is_null = |v: &Value| v.get("type").and_then(Value::as_str) == Some("null");
        let nullable = variants.iter().any(is_null);
        let rest: Vec<&Value> = variants.iter().filter(|v| !is_null(v)).collect();
        let t = match rest.as_slice() {
            [] => "()".to_owned(),
            [one] => self.ty(one, hint, path, root)?,
            _ => self.tagged(&rest, o, hint, path, root)?,
        };
        Ok(if nullable { format!("Option<{t}>") } else { t })
    }

    /// A discriminated union: every variant is an object with a required
    /// string `const` property under the same key.
    fn tagged(
        &mut self,
        variants: &[&Value],
        o: &Map<String, Value>,
        hint: &str,
        path: &str,
        root: Root<'_>,
    ) -> Result<String, Error> {
        let resolve = |v: &Value| -> Value {
            match v.get("$ref").and_then(Value::as_str).and_then(|r| root.def(r)) {
                Some((_, d)) => d.clone(),
                None => v.clone(),
            }
        };
        let objects: Vec<Value> = variants.iter().map(|v| resolve(v)).collect();
        let tag_of = |obj: &Value, key: &str| -> Option<String> {
            let required = obj.get("required")?.as_array()?.iter().any(|r| r == key);
            let p = obj.get("properties")?.get(key)?;
            let c =
                p.get("const").or_else(|| p.get("enum").and_then(|e| e.as_array().filter(|e| e.len() == 1)?.first()));
            required.then(|| c?.as_str().map(str::to_owned)).flatten()
        };
        let keys: Vec<String> = objects[0]
            .get("properties")
            .and_then(Value::as_object)
            .map(|p| p.keys().cloned().collect())
            .unwrap_or_default();
        let Some(key) = keys.into_iter().find(|k| objects.iter().all(|obj| tag_of(obj, k).is_some())) else {
            return Err(unsupported(path, "an untagged union (use z.discriminatedUnion)"));
        };
        self.claim(hint, path)?;
        let mut item = doc(o, "");
        item.push_str(&format!(
            "{DERIVE}\n#[serde(crate = \"__rt::serde\", tag = {})]\npub enum {hint} {{\n",
            string_lit(&key)
        ));
        let mut seen = HashSet::new();
        for (i, obj) in objects.iter().enumerate() {
            let tag = tag_of(obj, &key).unwrap();
            let variant = pascal(&tag);
            if !seen.insert(variant.clone()) {
                return Err(Error::new(path, format!("union tags map to the same variant `{variant}`")));
            }
            let vpath = format!("{path}.anyOf[{i}]");
            let fields =
                self.fields(obj.as_object().unwrap(), &format!("{hint}{variant}"), &vpath, root, &[&key], "")?;
            item.push_str(&variant_text(&variant, &tag, &fields, obj.as_object().unwrap()));
        }
        item.push_str("}\n");
        self.out.push(item);
        Ok(hint.to_owned())
    }

    fn object(&mut self, o: &Map<String, Value>, hint: &str, path: &str, root: Root<'_>) -> Result<String, Error> {
        let props = o.get("properties").and_then(Value::as_object);
        let additional = o.get("additionalProperties");
        let map_of = |this: &mut Self, a: &Value| -> Result<String, Error> {
            let v = this.ty(a, &format!("{hint}Value"), &format!("{path}.additionalProperties"), root)?;
            Ok(format!("__rt::IndexMap<String, {v}>"))
        };
        match (props, additional) {
            (None, Some(a @ Value::Object(_))) => return map_of(self, a),
            (None, None | Some(Value::Bool(true))) => return Ok(format!("__rt::serde_json::Map<String, {VALUE}>")),
            _ => {}
        }
        self.claim(hint, path)?;
        let mut fields = self.fields(o, hint, path, root, &[], "pub ")?;
        if let Some(a @ Value::Object(_)) = additional {
            let t = map_of(self, a)?;
            fields.push(Field { lines: vec!["#[serde(flatten)]".into(), format!("pub extra: {t},")] });
        }
        let mut item = doc(o, "");
        item.push_str(&format!("{DERIVE}\n{SERDE_CRATE}\npub struct {hint} {{\n"));
        for f in fields {
            for l in f.lines {
                item.push_str(&format!("    {l}\n"));
            }
        }
        item.push_str("}\n");
        self.out.push(item);
        Ok(hint.to_owned())
    }

    /// The fields of an object schema, skipping `skip` (a union tag or an
    /// event's `type`).
    pub fn fields(
        &mut self,
        o: &Map<String, Value>,
        hint: &str,
        path: &str,
        root: Root<'_>,
        skip: &[&str],
        vis: &str,
    ) -> Result<Vec<Field>, Error> {
        let empty = Map::new();
        let props = o.get("properties").and_then(Value::as_object).unwrap_or(&empty);
        let required: HashSet<&str> =
            o.get("required").and_then(Value::as_array).into_iter().flatten().filter_map(Value::as_str).collect();
        let mut idents = HashSet::new();
        let mut fields = Vec::new();
        for (key, prop) in props {
            if skip.contains(&key.as_str()) {
                continue;
            }
            let ppath = format!("{path}.properties.{key}");
            let ident = snake(key);
            if !idents.insert(ident.clone()) {
                return Err(Error::new(&ppath, format!("properties map to the same field `{ident}`")));
            }
            let mut t = self.ty(prop, &format!("{hint}{}", pascal(key)), &ppath, root)?;
            let mut lines: Vec<String> =
                doc(prop.as_object().unwrap_or(&empty), "").lines().map(str::to_owned).collect();
            let mut serde = Vec::new();
            if unraw(&ident) != key {
                serde.push(format!("rename = {}", string_lit(key)));
            }
            if !required.contains(key.as_str()) {
                if !t.starts_with("Option<") {
                    t = format!("Option<{t}>");
                }
                serde.push("default".into());
                serde.push("skip_serializing_if = \"Option::is_none\"".into());
            } else if let Some(default) = prop.get("default") {
                self.defaults += 1;
                let f = format!("__default_{}", self.defaults);
                self.out.push(format!(
                    "fn {f}<T: __rt::serde::de::DeserializeOwned>() -> T {{\n    __rt::serde_json::from_str({}).expect(\"a valid default\")\n}}\n",
                    raw_lit(&default.to_string())
                ));
                serde.push(format!("default = {}", string_lit(&f)));
            }
            if !serde.is_empty() {
                lines.push(format!("#[serde({})]", serde.join(", ")));
            }
            lines.push(format!("{vis}{ident}: {t},"));
            fields.push(Field { lines });
        }
        Ok(fields)
    }
}

/// An enum variant: unit when it has no fields, a struct variant otherwise.
pub fn variant_text(variant: &str, tag: &str, fields: &[Field], o: &Map<String, Value>) -> String {
    let mut s = doc(o, "    ");
    if variant != tag {
        s.push_str(&format!("    #[serde(rename = {})]\n", string_lit(tag)));
    }
    if fields.is_empty() {
        s.push_str(&format!("    {variant},\n"));
    } else {
        s.push_str(&format!("    {variant} {{\n"));
        for f in fields {
            for l in &f.lines {
                s.push_str(&format!("        {l}\n"));
            }
        }
        s.push_str("    },\n");
    }
    s
}

/// Validates `value` against `schema` (the subset [`Types`] supports, plus
/// numeric and length bounds).
pub fn validate(value: &Value, schema: &Value, root: &Value, path: &str) -> Result<(), String> {
    let o = match schema {
        Value::Bool(true) => return Ok(()),
        Value::Bool(false) => return Err(format!("{path}: no value is allowed here")),
        Value::Object(o) => o,
        _ => return Ok(()),
    };
    if let Some(r) = o.get("$ref").and_then(Value::as_str) {
        let def =
            if r == "#" { Some(root) } else { r.strip_prefix("#/$defs/").and_then(|n| root.get("$defs")?.get(n)) };
        return match def {
            Some(d) => validate(value, d, root, path),
            None => Err(format!("{path}: unresolved reference `{r}`")),
        };
    }
    if let Some(u) = o.get("anyOf").or_else(|| o.get("oneOf")).and_then(Value::as_array) {
        let errors: Vec<String> = u.iter().filter_map(|s| validate(value, s, root, path).err()).collect();
        if errors.len() == u.len() {
            return Err(errors.into_iter().next().unwrap_or_else(|| format!("{path}: matches no variant")));
        }
    }
    if let Some(c) = o.get("const") {
        if value != c {
            return Err(format!("{path}: expected {c}, got {value}"));
        }
    }
    if let Some(e) = o.get("enum").and_then(Value::as_array) {
        if !e.contains(value) {
            return Err(format!("{path}: expected one of {}, got {value}", Value::Array(e.clone())));
        }
    }
    if let Some(t) = o.get("type") {
        let types: Vec<&str> = match t {
            Value::String(t) => vec![t],
            Value::Array(ts) => ts.iter().filter_map(Value::as_str).collect(),
            _ => vec![],
        };
        let fits = |t: &str| match t {
            "string" => value.is_string(),
            "integer" => value.as_f64().is_some_and(|n| n.fract() == 0.0),
            "number" => value.is_number(),
            "boolean" => value.is_boolean(),
            "null" => value.is_null(),
            "array" => value.is_array(),
            "object" => value.is_object(),
            _ => true,
        };
        if !types.iter().any(|t| fits(t)) {
            return Err(format!("{path}: expected {}, got {value}", types.join(" or ")));
        }
    }
    if let Some(n) = value.as_f64() {
        let bound = |k: &str| o.get(k).and_then(Value::as_f64);
        if bound("minimum").is_some_and(|m| n < m) || bound("exclusiveMinimum").is_some_and(|m| n <= m) {
            return Err(format!("{path}: {value} is too small"));
        }
        if bound("maximum").is_some_and(|m| n > m) || bound("exclusiveMaximum").is_some_and(|m| n >= m) {
            return Err(format!("{path}: {value} is too large"));
        }
    }
    let len_bound = |k: &str| o.get(k).and_then(Value::as_u64).map(|n| n as usize);
    if let Some(s) = value.as_str() {
        let len = s.chars().count();
        if len_bound("minLength").is_some_and(|m| len < m) || len_bound("maxLength").is_some_and(|m| len > m) {
            return Err(format!("{path}: string length {len} is out of bounds"));
        }
    }
    if let Some(items) = value.as_array() {
        if len_bound("minItems").is_some_and(|m| items.len() < m)
            || len_bound("maxItems").is_some_and(|m| items.len() > m)
        {
            return Err(format!("{path}: {} items is out of bounds", items.len()));
        }
        let prefix = o.get("prefixItems").and_then(Value::as_array);
        for (i, item) in items.iter().enumerate() {
            let s = match prefix.and_then(|p| p.get(i)) {
                Some(s) => s,
                None => match o.get("items") {
                    Some(s) => s,
                    None => continue,
                },
            };
            validate(item, s, root, &format!("{path}[{i}]"))?;
        }
    }
    if let Some(obj) = value.as_object() {
        let empty = Map::new();
        let props = o.get("properties").and_then(Value::as_object).unwrap_or(&empty);
        for r in o.get("required").and_then(Value::as_array).into_iter().flatten().filter_map(Value::as_str) {
            if !obj.contains_key(r) {
                return Err(format!("{path}: missing required property `{r}`"));
            }
        }
        for (k, v) in obj {
            let p = format!("{path}.{k}");
            match (props.get(k), o.get("additionalProperties")) {
                (Some(s), _) => validate(v, s, root, &p)?,
                (None, Some(Value::Bool(false))) => return Err(format!("{p}: unknown property")),
                (None, Some(s @ Value::Object(_))) => validate(v, s, root, &p)?,
                _ => {}
            }
        }
    }
    Ok(())
}
