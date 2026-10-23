//! Snapshots as protobuf: XState's `SystemSnapshot` (in its compact form,
//! `xstate_rt::System::compact_view`) with field numbers instead of field
//! names. One table describes the messages; the encoder (a serde serializer
//! guided by it), the decoder (back to the same JSON, which restore reads)
//! and `proto/xstate_snapshot.proto` all follow from it.
//!
//! Contexts, events, inputs and outputs have no fixed shape and are generic
//! `Value` messages (see [`super::value`]). Any key the table does not know,
//! or a value of an unexpected shape, goes to the message's `extra` field
//! (field 100) as a key and a `Value`, so nothing is lost when snapshots
//! gain fields.

use std::fmt;

use serde::ser::{self, Serialize};
use serde_json::{Map, Value};

use super::value::{self, KeySerializer, ValueSerializer};
use super::wire::{self, err, DecodeError, Raw, Reader};

/// The field holding entries the table does not describe.
pub const EXTRA: u32 = 100;

#[derive(Debug, Clone, Copy)]
enum Kind {
    Str,
    /// One of a few known strings, as a protobuf enum (other strings go to
    /// `extra`).
    Enum(&'static EnumDef),
    Bool,
    U64,
    /// A JSON string holding a decimal `u64` (XState's incarnations).
    U64Str,
    /// A generic `Value` message.
    Value,
    Msg(&'static Message),
    RepeatedMsg(&'static Message),
    RepeatedStr,
    RepeatedValue,
    /// A JSON object of objects: one entry message per key, the key in
    /// field 1 and the object's fields after it.
    MapMsg(&'static Message),
    /// XState's `historyValue`: a JSON object of `[{ "$stateNode": id }]`,
    /// one entry message per key: `string key = 1; repeated string states = 2`.
    MapStateNodes,
}

#[derive(Debug)]
struct Field {
    key: &'static str,
    number: u32,
    kind: Kind,
    /// The name in the `.proto` file.
    name: &'static str,
}

#[derive(Debug)]
struct EnumDef {
    name: &'static str,
    /// The JSON strings and the enum value names, numbered from 1 (0 is
    /// the unspecified value protobuf requires).
    values: &'static [(&'static str, &'static str)],
}

static STATUS: EnumDef = EnumDef {
    name: "Status",
    values: &[
        ("active", "STATUS_ACTIVE"),
        ("done", "STATUS_DONE"),
        ("error", "STATUS_ERROR"),
        ("stopped", "STATUS_STOPPED"),
    ],
};

static TIMER_TYPE: EnumDef = EnumDef { name: "TimerType", values: &[("@xstate.raise", "TIMER_TYPE_RAISE")] };

static TIMER_TARGET: EnumDef = EnumDef { name: "TimerTarget", values: &[("self", "TIMER_TARGET_SELF")] };

#[derive(Debug)]
pub struct Message {
    name: &'static str,
    /// Entries of a `MapMsg` keep their key in field 1.
    keyed: bool,
    fields: &'static [Field],
}

const fn f(key: &'static str, number: u32, kind: Kind, name: &'static str) -> Field {
    Field { key, number, kind, name }
}

static ACTOR_REF: Message = Message {
    name: "ActorRef",
    keyed: false,
    fields: &[f("$actor", 1, Kind::Str, "actor"), f("incarnation", 2, Kind::U64Str, "incarnation")],
};

static ACTOR_REF_ENTRY: Message = Message {
    name: "ActorRefEntry",
    keyed: true,
    fields: &[f("$actor", 2, Kind::Str, "actor"), f("incarnation", 3, Kind::U64Str, "incarnation")],
};

static COUNTERS: Message = Message {
    name: "Counters",
    keyed: false,
    fields: &[
        f("actor", 1, Kind::U64, "actor"),
        f("timer", 2, Kind::U64, "timer"),
        f("sequence", 3, Kind::U64, "sequence"),
        f("effect", 4, Kind::U64, "effect"),
    ],
};

/// Timer and lifecycle events (`xstate.after`, `xstate.timer`, ...):
/// their usual fields by number; any other field (a raised event's
/// payload) is in `extra`.
static EVENT: Message = Message {
    name: "Event",
    keyed: false,
    fields: &[
        f("type", 1, Kind::Str, "type"),
        f("delay", 2, Kind::U64, "delay"),
        f("stateId", 3, Kind::Str, "state_id"),
        f("event", 4, Kind::Msg(&EVENT), "event"),
        f("id", 5, Kind::Str, "id"),
        f("seq", 6, Kind::U64, "seq"),
        f("occurrence", 7, Kind::U64, "occurrence"),
        f("actorId", 8, Kind::Str, "actor_id"),
    ],
};

static LOGICAL_TIMER: Message = Message {
    name: "LogicalTimer",
    keyed: false,
    fields: &[
        f("id", 1, Kind::Str, "id"),
        f("seq", 2, Kind::U64, "seq"),
        f("delay", 3, Kind::U64, "delay"),
        f("type", 4, Kind::Enum(&TIMER_TYPE), "type"),
        f("event", 5, Kind::Msg(&EVENT), "event"),
        f("target", 6, Kind::Enum(&TIMER_TARGET), "target"),
    ],
};

static ACTOR_SNAPSHOT: Message = Message {
    name: "ActorSnapshot",
    keyed: false,
    fields: &[
        f("status", 1, Kind::Enum(&STATUS), "status"),
        f("output", 2, Kind::Value, "output"),
        f("error", 3, Kind::Value, "error"),
        f("context", 4, Kind::Value, "context"),
        f("value", 5, Kind::Value, "value"),
        f("children", 6, Kind::MapMsg(&ACTOR_REF_ENTRY), "children"),
        f("timers", 7, Kind::RepeatedMsg(&LOGICAL_TIMER), "timers"),
        f("historyValue", 8, Kind::MapStateNodes, "history_value"),
        f("_stateInputs", 9, Kind::Value, "state_inputs"),
        f("_nextTimerId", 10, Kind::U64, "next_timer_id"),
        f("nodeIds", 11, Kind::RepeatedStr, "node_ids"),
    ],
};

static ACTOR_ENTRY: Message = Message {
    name: "ActorEntry",
    keyed: true,
    fields: &[
        f("id", 2, Kind::Str, "id"),
        f("address", 3, Kind::Str, "address"),
        f("incarnation", 4, Kind::U64Str, "incarnation"),
        f("src", 5, Kind::Str, "src"),
        f("logicVersion", 6, Kind::Str, "logic_version"),
        f("parent", 7, Kind::Msg(&ACTOR_REF), "parent"),
        f("registryKey", 8, Kind::Str, "registry_key"),
        f("syncSnapshot", 9, Kind::Bool, "sync_snapshot"),
        f("started", 10, Kind::Bool, "started"),
        f("input", 11, Kind::Value, "input"),
        f("snapshot", 12, Kind::Msg(&ACTOR_SNAPSHOT), "snapshot"),
    ],
};

static SYSTEM_TIMER: Message = Message {
    name: "SystemTimer",
    keyed: false,
    fields: &[
        f("source", 1, Kind::Msg(&ACTOR_REF), "source"),
        f("occurrence", 2, Kind::U64, "occurrence"),
        f("scheduledAt", 3, Kind::U64, "scheduled_at"),
        f("dueAt", 4, Kind::U64, "due_at"),
        f("sequence", 5, Kind::U64, "sequence"),
        f("event", 6, Kind::Msg(&EVENT), "event"),
    ],
};

/// The root message.
pub static SYSTEM_SNAPSHOT: Message = Message {
    name: "SystemSnapshot",
    keyed: false,
    fields: &[
        f("root", 1, Kind::Str, "root"),
        f("executionId", 2, Kind::Str, "execution_id"),
        f("now", 3, Kind::U64, "now"),
        f("actors", 4, Kind::MapMsg(&ACTOR_ENTRY), "actors"),
        f("registry", 5, Kind::MapMsg(&ACTOR_REF_ENTRY), "registry"),
        f("messages", 6, Kind::RepeatedValue, "messages"),
        f("timers", 7, Kind::RepeatedMsg(&SYSTEM_TIMER), "timers"),
        f("externalEffects", 8, Kind::Value, "external_effects"),
        f("counters", 9, Kind::Msg(&COUNTERS), "counters"),
    ],
};

/// Writes a snapshot (anything that serializes like XState's
/// `SystemSnapshot` JSON) as a `SystemSnapshot` message.
pub fn encode<T: Serialize + ?Sized>(out: &mut Vec<u8>, snapshot: &T) -> Result<(), Error> {
    snapshot.serialize(MsgSerializer { out, msg: &SYSTEM_SNAPSHOT })
}

/// Reads a `SystemSnapshot` message back into XState's snapshot JSON.
pub fn decode(bytes: &[u8]) -> Result<Value, DecodeError> {
    let mut map = Map::new();
    decode_into(bytes, &SYSTEM_SNAPSHOT, &mut map)?;
    Ok(Value::Object(map))
}

fn decode_into(bytes: &[u8], msg: &Message, map: &mut Map<String, Value>) -> Result<(), DecodeError> {
    let mut r = Reader::new(bytes);
    while let Some((number, raw)) = r.next()? {
        if number == EXTRA {
            let (k, v) = value::decode_field(raw.bytes()?)?;
            map.insert(k, v);
            continue;
        }
        if msg.keyed && number == 1 {
            continue;
        }
        let Some(field) = msg.fields.iter().find(|f| f.number == number) else { continue };
        let key = field.key;
        match field.kind {
            Kind::Str => {
                map.insert(key.into(), Value::String(raw.str()?.to_owned()));
            }
            Kind::Enum(e) => {
                let n = raw.varint()?;
                let s = usize::try_from(n)
                    .ok()
                    .and_then(|n| n.checked_sub(1))
                    .and_then(|i| e.values.get(i))
                    .ok_or_else(|| DecodeError(format!("unknown {} value {n}", e.name)))?;
                map.insert(key.into(), Value::String(s.0.to_owned()));
            }
            Kind::Bool => {
                map.insert(key.into(), Value::Bool(raw.varint()? != 0));
            }
            Kind::U64 => {
                map.insert(key.into(), Value::from(raw.varint()?));
            }
            Kind::U64Str => {
                map.insert(key.into(), Value::String(raw.varint()?.to_string()));
            }
            Kind::Value => {
                map.insert(key.into(), value::decode_raw(raw)?);
            }
            Kind::Msg(m) => {
                let mut inner = Map::new();
                decode_into(raw.bytes()?, m, &mut inner)?;
                map.insert(key.into(), Value::Object(inner));
            }
            Kind::RepeatedMsg(m) => {
                let mut inner = Map::new();
                decode_into(raw.bytes()?, m, &mut inner)?;
                push(map, key, Value::Object(inner))?;
            }
            Kind::RepeatedStr => push(map, key, Value::String(raw.str()?.to_owned()))?,
            Kind::RepeatedValue => push(map, key, value::decode_raw(raw)?)?,
            Kind::MapMsg(m) => {
                let bytes = raw.bytes()?;
                let entry_key = entry_key(bytes)?;
                let mut inner = Map::new();
                decode_into(bytes, m, &mut inner)?;
                object(map, key)?.insert(entry_key, Value::Object(inner));
            }
            Kind::MapStateNodes => {
                let bytes = raw.bytes()?;
                let entry_key = entry_key(bytes)?;
                let mut states = Vec::new();
                let mut er = Reader::new(bytes);
                while let Some((n, raw)) = er.next()? {
                    if n == 2 {
                        let mut node = Map::new();
                        node.insert("$stateNode".into(), Value::String(raw.str()?.to_owned()));
                        states.push(Value::Object(node));
                    }
                }
                object(map, key)?.insert(entry_key, Value::Array(states));
            }
        }
    }
    Ok(())
}

fn upper_snake(name: &str) -> String {
    let mut out = String::new();
    for (i, c) in name.chars().enumerate() {
        if c.is_uppercase() && i > 0 {
            out.push('_');
        }
        out.push(c.to_ascii_uppercase());
    }
    out
}

fn entry_key(bytes: &[u8]) -> Result<String, DecodeError> {
    let mut r = Reader::new(bytes);
    while let Some((n, raw)) = r.next()? {
        if n == 1 {
            return Ok(raw.str()?.to_owned());
        }
    }
    err("a map entry without a key")
}

fn push(map: &mut Map<String, Value>, key: &str, v: Value) -> Result<(), DecodeError> {
    match map.entry(key).or_insert_with(|| Value::Array(Vec::new())) {
        Value::Array(items) => {
            items.push(v);
            Ok(())
        }
        _ => err(format!("\"{key}\" is both repeated and not")),
    }
}

fn object<'a>(map: &'a mut Map<String, Value>, key: &str) -> Result<&'a mut Map<String, Value>, DecodeError> {
    match map.entry(key).or_insert_with(|| Value::Object(Map::new())) {
        Value::Object(m) => Ok(m),
        _ => err(format!("\"{key}\" is both a map and not")),
    }
}

/// The `.proto` file for snapshots (checked in as `proto/xstate_snapshot.proto`).
pub fn proto_file() -> String {
    let mut out = String::from(
        "// Snapshots of xstate-rs actor systems: XState's SystemSnapshot (compact\n\
         // form) with field numbers instead of names. Generated from the table in\n\
         // crates/xstate-kafka/src/proto/snapshot.rs; a test keeps them equal.\n\
         // Keys a message does not describe are in `extra` as key/value pairs.\n\
         syntax = \"proto3\";\n\npackage xstate.snapshot.v1;\n",
    );
    let mut seen = Vec::new();
    write_message(&mut out, &SYSTEM_SNAPSHOT, &mut seen);
    out.push_str(&value_proto());
    out
}

/// The generic `Value` messages, as a `.proto` fragment.
pub fn value_proto() -> String {
    "\n// A JSON value. Integers and floats stay distinct; object keys keep their order.\n\
     message Value {\n  oneof kind {\n    NullValue null = 1;\n    bool bool = 2;\n    sint64 int = 3;\n    uint64 uint = 4;\n    double float = 5;\n    string string = 6;\n    List list = 7;\n    Object object = 8;\n  }\n}\n\n\
     enum NullValue {\n  NULL_VALUE = 0;\n}\n\n\
     message List {\n  repeated Value values = 1;\n}\n\n\
     message Object {\n  repeated Field fields = 1;\n}\n\n\
     message Field {\n  string key = 1;\n  Value value = 2;\n}\n"
        .to_owned()
}

fn write_message(out: &mut String, msg: &'static Message, seen: &mut Vec<&'static str>) {
    if seen.contains(&msg.name) {
        return;
    }
    seen.push(msg.name);
    out.push_str(&format!("\nmessage {} {{\n", msg.name));
    if msg.keyed {
        out.push_str("  string key = 1;\n");
    }
    let mut nested = Vec::new();
    let mut enums: Vec<&'static EnumDef> = Vec::new();
    for field in msg.fields {
        let (label, ty) = match field.kind {
            Kind::Str => ("optional ", "string".to_owned()),
            Kind::Enum(e) => {
                enums.push(e);
                ("optional ", e.name.to_owned())
            }
            Kind::Bool => ("optional ", "bool".to_owned()),
            Kind::U64 | Kind::U64Str => ("optional ", "uint64".to_owned()),
            Kind::Value => ("", "Value".to_owned()),
            Kind::Msg(m) => {
                nested.push(m);
                ("", m.name.to_owned())
            }
            Kind::RepeatedMsg(m) | Kind::MapMsg(m) => {
                nested.push(m);
                ("repeated ", m.name.to_owned())
            }
            Kind::RepeatedStr => ("repeated ", "string".to_owned()),
            Kind::RepeatedValue => ("repeated ", "Value".to_owned()),
            Kind::MapStateNodes => ("repeated ", "HistoryEntry".to_owned()),
        };
        let note = match field.kind {
            Kind::U64Str => " // a decimal string in JSON",
            Kind::MapMsg(_) | Kind::MapStateNodes => " // a JSON object by key",
            _ => "",
        };
        out.push_str(&format!("  {label}{ty} {} = {};{note}\n", field.name, field.number));
    }
    out.push_str(&format!("  repeated Field extra = {EXTRA};\n}}\n"));
    if msg.fields.iter().any(|f| matches!(f.kind, Kind::MapStateNodes)) && !seen.contains(&"HistoryEntry") {
        seen.push("HistoryEntry");
        out.push_str(
            "\n// A history state's recorded states ({ \"$stateNode\": id } in JSON).\nmessage HistoryEntry {\n  string key = 1;\n  repeated string states = 2;\n}\n",
        );
    }
    for e in enums {
        if seen.contains(&e.name) {
            continue;
        }
        seen.push(e.name);
        out.push_str(&format!("\nenum {} {{\n  {}_UNSPECIFIED = 0;\n", e.name, upper_snake(e.name)));
        for (i, (json, name)) in e.values.iter().enumerate() {
            out.push_str(&format!("  {name} = {}; // \"{json}\"\n", i + 1));
        }
        out.push_str("}\n");
    }
    for m in nested {
        write_message(out, m, seen);
    }
}

/// A failure to write a snapshot. `Mismatch` (a value whose shape is not
/// the table's) never escapes: such entries go to `extra`.
#[derive(Debug)]
pub enum Error {
    Mismatch,
    Other(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Mismatch => f.write_str("a value does not have the expected shape"),
            Error::Other(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for Error {}

impl ser::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Error::Other(msg.to_string())
    }
}

impl From<value::Error> for Error {
    fn from(e: value::Error) -> Self {
        Error::Other(e.0)
    }
}

/// Writes the fields of message `msg` from a value serializing as a map.
struct MsgSerializer<'a> {
    out: &'a mut Vec<u8>,
    msg: &'static Message,
}

/// The fields of a message being written.
struct MsgCompound<'a> {
    out: &'a mut Vec<u8>,
    msg: &'static Message,
    key: Option<value::Key>,
    /// Where the next key is looked up first: keys usually come in table
    /// order.
    next: usize,
}

impl MsgCompound<'_> {
    /// An entry whose key serializes as a string: looked up in the table
    /// without copying it.
    fn entry_with<K: Serialize + ?Sized, T: Serialize + ?Sized>(&mut self, key: &K, value: &T) -> Result<(), Error> {
        let mark = self.out.len();
        if let Some(i) = key.serialize(FieldLookup { msg: self.msg, start: self.next })? {
            self.next = i + 1;
            let field = &self.msg.fields[i];
            match write_field(self.out, field, value) {
                Ok(()) => return Ok(()),
                Err(Error::Mismatch) => self.out.truncate(mark),
                Err(e) => return Err(e),
            }
        }
        value::encode_entry_with(self.out, EXTRA, key, value)?;
        Ok(())
    }

    fn entry<T: Serialize + ?Sized>(&mut self, key: &str, value: &T) -> Result<(), Error> {
        let mark = self.out.len();
        if let Some(field) = self.msg.fields.iter().find(|f| f.key == key) {
            match write_field(self.out, field, value) {
                Ok(()) => return Ok(()),
                Err(Error::Mismatch) => self.out.truncate(mark),
                Err(e) => return Err(e),
            }
        }
        value::encode_entry(self.out, EXTRA, key, value)?;
        Ok(())
    }
}

/// Writes `value` as `field`, or fails with `Mismatch` (having possibly
/// written part of it: the caller truncates).
fn write_field<T: Serialize + ?Sized>(out: &mut Vec<u8>, field: &Field, value: &T) -> Result<(), Error> {
    match field.kind {
        Kind::Str | Kind::Enum(_) | Kind::Bool | Kind::U64 | Kind::U64Str => {
            value.serialize(ScalarSerializer { out, number: field.number, kind: field.kind })
        }
        Kind::Value => {
            let start = wire::begin_len(out, field.number);
            value.serialize(ValueSerializer { out })?;
            wire::end_len(out, start);
            Ok(())
        }
        Kind::Msg(m) => {
            let start = wire::begin_len(out, field.number);
            value.serialize(MsgSerializer { out, msg: m })?;
            wire::end_len(out, start);
            Ok(())
        }
        Kind::RepeatedMsg(_) | Kind::RepeatedStr | Kind::RepeatedValue | Kind::MapMsg(_) | Kind::MapStateNodes => {
            value.serialize(CollectionSerializer { out, field })
        }
    }
}

/// Fails with `Mismatch` for every serde type not listed.
macro_rules! mismatch {
    ($($name:ident($($t:ty),*)),*) => {
        $(fn $name(self, $(_: $t),*) -> Result<Self::Ok, Error> { Err(Error::Mismatch) })*
    };
}

macro_rules! mismatch_compounds {
    () => {
        fn serialize_some<T: Serialize + ?Sized>(self, v: &T) -> Result<Self::Ok, Error> {
            v.serialize(self)
        }
        fn serialize_newtype_struct<T: Serialize + ?Sized>(self, _: &'static str, v: &T) -> Result<Self::Ok, Error> {
            v.serialize(self)
        }
        fn serialize_newtype_variant<T: Serialize + ?Sized>(
            self,
            _: &'static str,
            _: u32,
            _: &'static str,
            _: &T,
        ) -> Result<Self::Ok, Error> {
            Err(Error::Mismatch)
        }
        fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple, Error> {
            Err(Error::Mismatch)
        }
        fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeTupleStruct, Error> {
            Err(Error::Mismatch)
        }
        fn serialize_tuple_variant(
            self,
            _: &'static str,
            _: u32,
            _: &'static str,
            _: usize,
        ) -> Result<Self::SerializeTupleVariant, Error> {
            Err(Error::Mismatch)
        }
        fn serialize_struct_variant(
            self,
            _: &'static str,
            _: u32,
            _: &'static str,
            _: usize,
        ) -> Result<Self::SerializeStructVariant, Error> {
            Err(Error::Mismatch)
        }
    };
}

/// Writes one scalar field of kind `kind`.
struct ScalarSerializer<'a> {
    out: &'a mut Vec<u8>,
    number: u32,
    kind: Kind,
}

impl ScalarSerializer<'_> {
    fn uint(self, v: u64) -> Result<(), Error> {
        match self.kind {
            Kind::U64 => {
                wire::put_uint(self.out, self.number, v);
                Ok(())
            }
            _ => Err(Error::Mismatch),
        }
    }
}

impl ser::Serializer for ScalarSerializer<'_> {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = ser::Impossible<(), Error>;
    type SerializeTuple = ser::Impossible<(), Error>;
    type SerializeTupleStruct = ser::Impossible<(), Error>;
    type SerializeTupleVariant = ser::Impossible<(), Error>;
    type SerializeMap = ser::Impossible<(), Error>;
    type SerializeStruct = ser::Impossible<(), Error>;
    type SerializeStructVariant = ser::Impossible<(), Error>;

    fn serialize_bool(self, v: bool) -> Result<(), Error> {
        match self.kind {
            Kind::Bool => {
                wire::put_bool(self.out, self.number, v);
                Ok(())
            }
            _ => Err(Error::Mismatch),
        }
    }
    fn serialize_u8(self, v: u8) -> Result<(), Error> {
        self.uint(v.into())
    }
    fn serialize_u16(self, v: u16) -> Result<(), Error> {
        self.uint(v.into())
    }
    fn serialize_u32(self, v: u32) -> Result<(), Error> {
        self.uint(v.into())
    }
    fn serialize_u64(self, v: u64) -> Result<(), Error> {
        self.uint(v)
    }
    fn serialize_i64(self, v: i64) -> Result<(), Error> {
        u64::try_from(v).map_err(|_| Error::Mismatch).and_then(|v| self.uint(v))
    }
    fn serialize_i32(self, v: i32) -> Result<(), Error> {
        self.serialize_i64(v.into())
    }
    fn serialize_i16(self, v: i16) -> Result<(), Error> {
        self.serialize_i64(v.into())
    }
    fn serialize_i8(self, v: i8) -> Result<(), Error> {
        self.serialize_i64(v.into())
    }
    fn serialize_str(self, v: &str) -> Result<(), Error> {
        match self.kind {
            Kind::Str => {
                wire::put_str(self.out, self.number, v);
                Ok(())
            }
            Kind::Enum(e) => match e.values.iter().position(|(s, _)| *s == v) {
                Some(i) => {
                    wire::put_uint(self.out, self.number, i as u64 + 1);
                    Ok(())
                }
                None => Err(Error::Mismatch),
            },
            // Only the canonical decimal form, so it reads back the same.
            Kind::U64Str => match v.parse::<u64>() {
                // Canonical: digits only, no leading zero (but "0").
                Ok(n) if v.bytes().all(|b| b.is_ascii_digit()) && (v == "0" || !v.starts_with('0')) => {
                    wire::put_uint(self.out, self.number, n);
                    Ok(())
                }
                _ => Err(Error::Mismatch),
            },
            _ => Err(Error::Mismatch),
        }
    }
    fn serialize_unit_variant(self, _: &'static str, _: u32, v: &'static str) -> Result<(), Error> {
        self.serialize_str(v)
    }
    mismatch!(
        serialize_f32(f32),
        serialize_f64(f64),
        serialize_char(char),
        serialize_bytes(&[u8]),
        serialize_none(),
        serialize_unit(),
        serialize_unit_struct(&'static str)
    );
    mismatch_compounds!();
    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq, Error> {
        Err(Error::Mismatch)
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, Error> {
        Err(Error::Mismatch)
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct, Error> {
        Err(Error::Mismatch)
    }
}

impl<'a> ser::Serializer for MsgSerializer<'a> {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = ser::Impossible<(), Error>;
    type SerializeTuple = ser::Impossible<(), Error>;
    type SerializeTupleStruct = ser::Impossible<(), Error>;
    type SerializeTupleVariant = ser::Impossible<(), Error>;
    type SerializeMap = MsgCompound<'a>;
    type SerializeStruct = MsgCompound<'a>;
    type SerializeStructVariant = ser::Impossible<(), Error>;

    fn serialize_map(self, _: Option<usize>) -> Result<MsgCompound<'a>, Error> {
        Ok(MsgCompound { out: self.out, msg: self.msg, key: None, next: 0 })
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<MsgCompound<'a>, Error> {
        Ok(MsgCompound { out: self.out, msg: self.msg, key: None, next: 0 })
    }
    mismatch!(
        serialize_bool(bool),
        serialize_i8(i8),
        serialize_i16(i16),
        serialize_i32(i32),
        serialize_i64(i64),
        serialize_u8(u8),
        serialize_u16(u16),
        serialize_u32(u32),
        serialize_u64(u64),
        serialize_f32(f32),
        serialize_f64(f64),
        serialize_char(char),
        serialize_str(&str),
        serialize_bytes(&[u8]),
        serialize_none(),
        serialize_unit(),
        serialize_unit_struct(&'static str)
    );
    fn serialize_unit_variant(self, _: &'static str, _: u32, _: &'static str) -> Result<(), Error> {
        Err(Error::Mismatch)
    }
    mismatch_compounds!();
    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq, Error> {
        Err(Error::Mismatch)
    }
}

impl ser::SerializeMap for MsgCompound<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Error> {
        self.key = Some(key.serialize(KeySerializer)?);
        Ok(())
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        let key = self.key.take().ok_or_else(|| Error::Other("a map value without a key".into()))?;
        self.entry(key.as_str(), value)
    }
    fn serialize_entry<K: Serialize + ?Sized, V: Serialize + ?Sized>(
        &mut self,
        key: &K,
        value: &V,
    ) -> Result<(), Error> {
        self.entry_with(key, value)
    }
    fn end(self) -> Result<(), Error> {
        Ok(())
    }
}

impl ser::SerializeStruct for MsgCompound<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, key: &'static str, value: &T) -> Result<(), Error> {
        self.entry(key, value)
    }
    fn end(self) -> Result<(), Error> {
        Ok(())
    }
}

/// Finds the table field of a key that serializes as a string.
struct FieldLookup {
    msg: &'static Message,
    /// The index tried first.
    start: usize,
}

macro_rules! lookup_rejects {
    ($($name:ident($($t:ty),*)),*) => {
        $(fn $name(self, $(_: $t),*) -> Result<Option<usize>, Error> {
            Err(Error::Other("map keys must be strings".into()))
        })*
    };
}

impl ser::Serializer for FieldLookup {
    type Ok = Option<usize>;
    type Error = Error;
    type SerializeSeq = ser::Impossible<Self::Ok, Error>;
    type SerializeTuple = ser::Impossible<Self::Ok, Error>;
    type SerializeTupleStruct = ser::Impossible<Self::Ok, Error>;
    type SerializeTupleVariant = ser::Impossible<Self::Ok, Error>;
    type SerializeMap = ser::Impossible<Self::Ok, Error>;
    type SerializeStruct = ser::Impossible<Self::Ok, Error>;
    type SerializeStructVariant = ser::Impossible<Self::Ok, Error>;

    fn serialize_str(self, v: &str) -> Result<Self::Ok, Error> {
        let fields = self.msg.fields;
        let start = self.start.min(fields.len());
        // Length and first byte first: most candidates differ there, which
        // saves a `memcmp` call each.
        let first = v.as_bytes().first();
        let same = |k: &str| k.len() == v.len() && k.as_bytes().first() == first && k == v;
        let found = (start..fields.len()).chain(0..start).find(|&i| same(fields[i].key));
        Ok(found)
    }
    fn serialize_unit_variant(self, _: &'static str, _: u32, v: &'static str) -> Result<Self::Ok, Error> {
        self.serialize_str(v)
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(self, _: &'static str, v: &T) -> Result<Self::Ok, Error> {
        v.serialize(self)
    }
    lookup_rejects!(
        serialize_bool(bool),
        serialize_i8(i8),
        serialize_i16(i16),
        serialize_i32(i32),
        serialize_i64(i64),
        serialize_u8(u8),
        serialize_u16(u16),
        serialize_u32(u32),
        serialize_u64(u64),
        serialize_f32(f32),
        serialize_f64(f64),
        serialize_char(char),
        serialize_bytes(&[u8]),
        serialize_none(),
        serialize_unit(),
        serialize_unit_struct(&'static str)
    );
    fn serialize_some<T: Serialize + ?Sized>(self, _: &T) -> Result<Self::Ok, Error> {
        Err(Error::Other("map keys must be strings".into()))
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> Result<Self::Ok, Error> {
        Err(Error::Other("map keys must be strings".into()))
    }
    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq, Error> {
        Err(Error::Other("map keys must be strings".into()))
    }
    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple, Error> {
        Err(Error::Other("map keys must be strings".into()))
    }
    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeTupleStruct, Error> {
        Err(Error::Other("map keys must be strings".into()))
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, Error> {
        Err(Error::Other("map keys must be strings".into()))
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, Error> {
        Err(Error::Other("map keys must be strings".into()))
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct, Error> {
        Err(Error::Other("map keys must be strings".into()))
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, Error> {
        Err(Error::Other("map keys must be strings".into()))
    }
}

/// Writes a repeated or map field from a JSON array or object.
struct CollectionSerializer<'a> {
    out: &'a mut Vec<u8>,
    field: &'a Field,
}

/// The elements of a repeated field, or the entries of a map field. An
/// empty collection writes nothing on the wire, so it fails with `Mismatch`
/// and goes to `extra`, which keeps the key.
struct CollectionCompound<'a> {
    out: &'a mut Vec<u8>,
    field: &'a Field,
    key: Option<value::Key>,
    count: usize,
}

impl CollectionCompound<'_> {
    fn finish(self) -> Result<(), Error> {
        if self.count == 0 {
            return Err(Error::Mismatch);
        }
        Ok(())
    }

    fn element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        self.count += 1;
        let number = self.field.number;
        match self.field.kind {
            Kind::RepeatedStr => value.serialize(ScalarSerializer { out: self.out, number, kind: Kind::Str }),
            Kind::RepeatedValue => {
                let start = wire::begin_len(self.out, number);
                value.serialize(ValueSerializer { out: self.out })?;
                wire::end_len(self.out, start);
                Ok(())
            }
            Kind::RepeatedMsg(m) => {
                let start = wire::begin_len(self.out, number);
                value.serialize(MsgSerializer { out: self.out, msg: m })?;
                wire::end_len(self.out, start);
                Ok(())
            }
            _ => Err(Error::Mismatch),
        }
    }

    fn entry<K: Serialize + ?Sized, T: Serialize + ?Sized>(&mut self, key: &K, value: &T) -> Result<(), Error> {
        self.count += 1;
        let number = self.field.number;
        let start = wire::begin_len(self.out, number);
        key.serialize(value::KeyWriter { out: self.out, field: 1 })?;
        match self.field.kind {
            Kind::MapMsg(m) => value.serialize(MsgSerializer { out: self.out, msg: m })?,
            Kind::MapStateNodes => value.serialize(StateNodesSerializer { out: self.out })?,
            _ => return Err(Error::Mismatch),
        }
        wire::end_len(self.out, start);
        Ok(())
    }
}

impl<'a> ser::Serializer for CollectionSerializer<'a> {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = CollectionCompound<'a>;
    type SerializeTuple = ser::Impossible<(), Error>;
    type SerializeTupleStruct = ser::Impossible<(), Error>;
    type SerializeTupleVariant = ser::Impossible<(), Error>;
    type SerializeMap = CollectionCompound<'a>;
    type SerializeStruct = ser::Impossible<(), Error>;
    type SerializeStructVariant = ser::Impossible<(), Error>;

    fn serialize_seq(self, _: Option<usize>) -> Result<CollectionCompound<'a>, Error> {
        match self.field.kind {
            Kind::RepeatedMsg(_) | Kind::RepeatedStr | Kind::RepeatedValue => {
                Ok(CollectionCompound { out: self.out, field: self.field, key: None, count: 0 })
            }
            _ => Err(Error::Mismatch),
        }
    }
    fn serialize_map(self, _: Option<usize>) -> Result<CollectionCompound<'a>, Error> {
        match self.field.kind {
            Kind::MapMsg(_) | Kind::MapStateNodes => {
                Ok(CollectionCompound { out: self.out, field: self.field, key: None, count: 0 })
            }
            _ => Err(Error::Mismatch),
        }
    }
    mismatch!(
        serialize_bool(bool),
        serialize_i8(i8),
        serialize_i16(i16),
        serialize_i32(i32),
        serialize_i64(i64),
        serialize_u8(u8),
        serialize_u16(u16),
        serialize_u32(u32),
        serialize_u64(u64),
        serialize_f32(f32),
        serialize_f64(f64),
        serialize_char(char),
        serialize_str(&str),
        serialize_bytes(&[u8]),
        serialize_none(),
        serialize_unit(),
        serialize_unit_struct(&'static str)
    );
    fn serialize_unit_variant(self, _: &'static str, _: u32, _: &'static str) -> Result<(), Error> {
        Err(Error::Mismatch)
    }
    mismatch_compounds!();
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct, Error> {
        Err(Error::Mismatch)
    }
}

impl ser::SerializeSeq for CollectionCompound<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        self.element(value)
    }
    fn end(self) -> Result<(), Error> {
        self.finish()
    }
}

impl ser::SerializeMap for CollectionCompound<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Error> {
        self.key = Some(key.serialize(KeySerializer)?);
        Ok(())
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        let key = self.key.take().ok_or_else(|| Error::Other("a map value without a key".into()))?;
        self.entry(key.as_str(), value)
    }
    fn serialize_entry<K: Serialize + ?Sized, V: Serialize + ?Sized>(
        &mut self,
        key: &K,
        value: &V,
    ) -> Result<(), Error> {
        self.entry(key, value)
    }
    fn end(self) -> Result<(), Error> {
        self.finish()
    }
}

/// Writes `[{ "$stateNode": id }, ...]` as `repeated string states = 2`.
struct StateNodesSerializer<'a> {
    out: &'a mut Vec<u8>,
}

struct StateNodesCompound<'a> {
    out: &'a mut Vec<u8>,
}

impl<'a> ser::Serializer for StateNodesSerializer<'a> {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = StateNodesCompound<'a>;
    type SerializeTuple = ser::Impossible<(), Error>;
    type SerializeTupleStruct = ser::Impossible<(), Error>;
    type SerializeTupleVariant = ser::Impossible<(), Error>;
    type SerializeMap = ser::Impossible<(), Error>;
    type SerializeStruct = ser::Impossible<(), Error>;
    type SerializeStructVariant = ser::Impossible<(), Error>;

    fn serialize_seq(self, _: Option<usize>) -> Result<StateNodesCompound<'a>, Error> {
        Ok(StateNodesCompound { out: self.out })
    }
    mismatch!(
        serialize_bool(bool),
        serialize_i8(i8),
        serialize_i16(i16),
        serialize_i32(i32),
        serialize_i64(i64),
        serialize_u8(u8),
        serialize_u16(u16),
        serialize_u32(u32),
        serialize_u64(u64),
        serialize_f32(f32),
        serialize_f64(f64),
        serialize_char(char),
        serialize_str(&str),
        serialize_bytes(&[u8]),
        serialize_none(),
        serialize_unit(),
        serialize_unit_struct(&'static str)
    );
    fn serialize_unit_variant(self, _: &'static str, _: u32, _: &'static str) -> Result<(), Error> {
        Err(Error::Mismatch)
    }
    mismatch_compounds!();
    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, Error> {
        Err(Error::Mismatch)
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct, Error> {
        Err(Error::Mismatch)
    }
}

static STATE_NODE: Message =
    Message { name: "StateNode", keyed: false, fields: &[f("$stateNode", 2, Kind::Str, "state")] };

impl ser::SerializeSeq for StateNodesCompound<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        // Each element is `{ "$stateNode": id }`: its one field, written in
        // place as `states` (field 2). Anything else does not fit.
        let mark = self.out.len();
        let mut probe = Vec::new();
        value.serialize(MsgSerializer { out: &mut probe, msg: &STATE_NODE })?;
        let mut r = Reader::new(&probe);
        match (r.next(), r.next()) {
            (Ok(Some((2, Raw::Len(_)))), Ok(None)) => {
                self.out.extend_from_slice(&probe);
                Ok(())
            }
            _ => {
                self.out.truncate(mark);
                Err(Error::Mismatch)
            }
        }
    }
    fn end(self) -> Result<(), Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    #[test]
    fn snapshots_round_trip_with_unknown_keys_kept() {
        let snapshot = json!({
            "root": "order", "executionId": "x", "now": 5,
            "actors": { "order": {
                "id": "order", "incarnation": "0", "src": "root", "started": true, "input": { "a": 1 },
                "snapshot": {
                    "status": "active", "context": { "items": 1.0, "tags": ["web"] },
                    "children": { "c": { "$actor": "order/c", "incarnation": "3" } },
                    "timers": [{ "delay": 10, "type": "@xstate.raise", "event": { "type": "xstate.after", "delay": 10, "stateId": "order.a" }, "target": "self" }],
                    "historyValue": { "order.h": [{ "$stateNode": "order.a" }, { "$stateNode": "order.b" }] },
                    "_nextTimerId": 2, "nodeIds": ["order", "order.a"], "somethingNew": [1, 2]
                }
            } },
            "timers": [{ "source": { "$actor": "order", "incarnation": "0" }, "occurrence": 0, "scheduledAt": 0, "dueAt": 10, "sequence": 1, "event": { "type": "xstate.timer" } }],
            "counters": { "actor": 1, "timer": 1, "sequence": 2, "effect": 0 },
            // Not of the table's shapes: kept as extra entries.
            "now2": "x", "registry": [1]
        });
        let mut out = Vec::new();
        super::encode(&mut out, &snapshot).unwrap();
        let back = super::decode(&out).unwrap();
        assert_eq!(back, snapshot);
        // Every key and value round-trips; the extras keep their values.
        assert_eq!(back["registry"], json!([1]));
        assert!(out.len() < serde_json::to_vec(&snapshot).unwrap().len() / 2, "{} bytes", out.len());
    }

    #[test]
    fn the_checked_in_proto_file_is_current() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/proto/xstate_snapshot.proto");
        let generated = super::proto_file();
        if std::env::var_os("XSTATE_WRITE_PROTO").is_some() {
            std::fs::write(path, &generated).unwrap();
        }
        let checked_in = std::fs::read_to_string(path).unwrap_or_default();
        assert!(
            checked_in == generated,
            "{path} is stale; regenerate with XSTATE_WRITE_PROTO=1 cargo test -p xstate-kafka --lib"
        );
    }
}
