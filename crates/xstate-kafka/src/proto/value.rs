//! A JSON value as a protobuf message, for the parts of snapshots and events
//! that have no fixed shape (contexts, event payloads, outputs):
//!
//! ```proto
//! message Value {
//!   oneof kind {
//!     NullValue null = 1;  // always 0
//!     bool bool = 2;
//!     sint64 int = 3;
//!     uint64 uint = 4;     // only above the sint64 range
//!     double float = 5;
//!     string string = 6;
//!     List list = 7;
//!     Object object = 8;
//!   }
//! }
//! message List { repeated Value values = 1; }
//! message Object { repeated Field fields = 1; }  // in key order
//! message Field { string key = 1; Value value = 2; }
//! ```
//!
//! Integers and floats keep their JSON form (`1` and `1.0` stay distinct),
//! and object keys keep their order, so a value round-trips exactly.

use std::fmt;

use serde::ser::{self, Serialize};
use serde_json::{Map, Number, Value};

use super::wire::{self, err, DecodeError, Raw, Reader};

const NULL: u32 = 1;
const BOOL: u32 = 2;
const INT: u32 = 3;
const UINT: u32 = 4;
const FLOAT: u32 = 5;
const STRING: u32 = 6;
const LIST: u32 = 7;
const OBJECT: u32 = 8;

/// Writes the fields of a `Value` message for `v`.
pub fn encode(out: &mut Vec<u8>, v: &Value) {
    v.serialize(ValueSerializer { out }).expect("JSON values serialize");
}

/// Writes `v` as a `Value` message in field `field` of the enclosing message.
pub fn encode_field(out: &mut Vec<u8>, field: u32, v: &Value) {
    let start = wire::begin_len(out, field);
    encode(out, v);
    wire::end_len(out, start);
}

/// Reads a `Value` message.
pub fn decode(bytes: &[u8]) -> Result<Value, DecodeError> {
    let mut r = Reader::new(bytes);
    // An empty message is null; the last kind written wins, as for a oneof.
    let mut value = Value::Null;
    while let Some((field, raw)) = r.next()? {
        value = match field {
            NULL => {
                raw.varint()?;
                Value::Null
            }
            BOOL => Value::Bool(raw.varint()? != 0),
            INT => Value::from(wire::unzigzag(raw.varint()?)),
            UINT => Value::from(raw.varint()?),
            FLOAT => {
                Value::Number(Number::from_f64(raw.double()?).ok_or_else(|| DecodeError("NaN or infinity".into()))?)
            }
            STRING => Value::String(raw.str()?.to_owned()),
            LIST => {
                let mut items = Vec::new();
                let mut lr = Reader::new(raw.bytes()?);
                while let Some((f, item)) = lr.next()? {
                    if f == 1 {
                        items.push(decode(item.bytes()?)?);
                    }
                }
                Value::Array(items)
            }
            OBJECT => {
                let mut map = Map::new();
                let mut or = Reader::new(raw.bytes()?);
                while let Some((f, entry)) = or.next()? {
                    if f == 1 {
                        let (k, v) = decode_field(entry.bytes()?)?;
                        map.insert(k, v);
                    }
                }
                Value::Object(map)
            }
            _ => continue,
        };
    }
    Ok(value)
}

/// Reads a `Field` message: a key and its value.
pub fn decode_field(bytes: &[u8]) -> Result<(String, Value), DecodeError> {
    let mut r = Reader::new(bytes);
    let (mut key, mut value) = (None, Value::Null);
    while let Some((f, raw)) = r.next()? {
        match f {
            1 => key = Some(raw.str()?.to_owned()),
            2 => value = decode(raw.bytes()?)?,
            _ => {}
        }
    }
    Ok((key.ok_or_else(|| DecodeError("a field without a key".into()))?, value))
}

/// Writes a `Field` message (key and value) in field `field`.
pub fn encode_entry<T: Serialize + ?Sized>(out: &mut Vec<u8>, field: u32, key: &str, value: &T) -> Result<(), Error> {
    let start = wire::begin_len(out, field);
    wire::put_str(out, 1, key);
    let v = wire::begin_len(out, 2);
    value.serialize(ValueSerializer { out })?;
    wire::end_len(out, v);
    wire::end_len(out, start);
    Ok(())
}

/// A failure to write a value: map keys must be strings.
#[derive(Debug)]
pub struct Error(pub String);

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Error {}

impl ser::Error for Error {
    fn custom<T: fmt::Display>(msg: T) -> Self {
        Error(msg.to_string())
    }
}

/// Writes the fields of a `Value` message for anything serde can serialize.
pub struct ValueSerializer<'a> {
    pub out: &'a mut Vec<u8>,
}

/// A list or object being written.
pub struct Compound<'a> {
    out: &'a mut Vec<u8>,
    start: usize,
}

impl Compound<'_> {
    fn element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        let start = wire::begin_len(self.out, 1);
        value.serialize(ValueSerializer { out: self.out })?;
        wire::end_len(self.out, start);
        Ok(())
    }

    fn entry<T: Serialize + ?Sized>(&mut self, key: &str, value: &T) -> Result<(), Error> {
        encode_entry(self.out, 1, key, value)
    }

    fn finish(self) {
        wire::end_len(self.out, self.start);
    }
}

/// An object being written, whose next key is pending.
pub struct ObjectCompound<'a> {
    inner: Compound<'a>,
    key: Option<Key>,
}

/// Writes a map key (which must serialize as a string) as string field
/// `field`, without copying it first.
pub struct KeyWriter<'a> {
    pub out: &'a mut Vec<u8>,
    pub field: u32,
}

macro_rules! writer_rejects {
    ($($name:ident($($t:ty),*)),*) => {
        $(fn $name(self, $(_: $t),*) -> Result<(), Error> { Err(key_error()) })*
    };
}

impl ser::Serializer for KeyWriter<'_> {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = ser::Impossible<(), Error>;
    type SerializeTuple = ser::Impossible<(), Error>;
    type SerializeTupleStruct = ser::Impossible<(), Error>;
    type SerializeTupleVariant = ser::Impossible<(), Error>;
    type SerializeMap = ser::Impossible<(), Error>;
    type SerializeStruct = ser::Impossible<(), Error>;
    type SerializeStructVariant = ser::Impossible<(), Error>;

    fn serialize_str(self, v: &str) -> Result<(), Error> {
        wire::put_str(self.out, self.field, v);
        Ok(())
    }
    fn serialize_char(self, v: char) -> Result<(), Error> {
        self.serialize_str(v.encode_utf8(&mut [0; 4]))
    }
    fn serialize_unit_variant(self, _: &'static str, _: u32, v: &'static str) -> Result<(), Error> {
        self.serialize_str(v)
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(self, _: &'static str, v: &T) -> Result<(), Error> {
        v.serialize(self)
    }
    writer_rejects!(
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
        serialize_bytes(&[u8]),
        serialize_none(),
        serialize_unit(),
        serialize_unit_struct(&'static str)
    );
    fn serialize_some<T: Serialize + ?Sized>(self, _: &T) -> Result<(), Error> {
        Err(key_error())
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> Result<(), Error> {
        Err(key_error())
    }
    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq, Error> {
        Err(key_error())
    }
    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple, Error> {
        Err(key_error())
    }
    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeTupleStruct, Error> {
        Err(key_error())
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, Error> {
        Err(key_error())
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, Error> {
        Err(key_error())
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct, Error> {
        Err(key_error())
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, Error> {
        Err(key_error())
    }
}

/// Writes a `Field` message (key and value) in field `field`, the key
/// written as it serializes.
pub fn encode_entry_with<K: Serialize + ?Sized, T: Serialize + ?Sized>(
    out: &mut Vec<u8>,
    field: u32,
    key: &K,
    value: &T,
) -> Result<(), Error> {
    let start = wire::begin_len(out, field);
    key.serialize(KeyWriter { out, field: 1 })?;
    let v = wire::begin_len(out, 2);
    value.serialize(ValueSerializer { out })?;
    wire::end_len(out, v);
    wire::end_len(out, start);
    Ok(())
}

/// A captured map key: on the stack when short, as most keys are.
pub struct Key {
    len: usize,
    buf: [u8; 56],
    heap: Option<String>,
}

impl Key {
    fn new(s: &str) -> Self {
        let mut key = Key { len: s.len(), buf: [0; 56], heap: None };
        if s.len() <= key.buf.len() {
            key.buf[..s.len()].copy_from_slice(s.as_bytes());
        } else {
            key.heap = Some(s.to_owned());
        }
        key
    }

    pub fn as_str(&self) -> &str {
        match &self.heap {
            Some(s) => s,
            None => std::str::from_utf8(&self.buf[..self.len]).expect("copied from a str"),
        }
    }
}

/// Captures a map key, which must serialize as a string.
pub struct KeySerializer;

fn key_error() -> Error {
    Error("map keys must be strings".into())
}

macro_rules! key_rejects {
    ($($name:ident($($t:ty),*)),*) => {
        $(fn $name(self, $(_: $t),*) -> Result<Key, Error> { Err(key_error()) })*
    };
}

impl ser::Serializer for KeySerializer {
    type Ok = Key;
    type Error = Error;
    type SerializeSeq = ser::Impossible<Key, Error>;
    type SerializeTuple = ser::Impossible<Key, Error>;
    type SerializeTupleStruct = ser::Impossible<Key, Error>;
    type SerializeTupleVariant = ser::Impossible<Key, Error>;
    type SerializeMap = ser::Impossible<Key, Error>;
    type SerializeStruct = ser::Impossible<Key, Error>;
    type SerializeStructVariant = ser::Impossible<Key, Error>;

    fn serialize_str(self, v: &str) -> Result<Key, Error> {
        Ok(Key::new(v))
    }
    fn serialize_char(self, v: char) -> Result<Key, Error> {
        Ok(Key::new(v.encode_utf8(&mut [0; 4])))
    }
    fn serialize_unit_variant(self, _: &'static str, _: u32, v: &'static str) -> Result<Key, Error> {
        Ok(Key::new(v))
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(self, _: &'static str, v: &T) -> Result<Key, Error> {
        v.serialize(self)
    }
    key_rejects!(
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
        serialize_bytes(&[u8]),
        serialize_none(),
        serialize_unit(),
        serialize_unit_struct(&'static str)
    );
    fn serialize_some<T: Serialize + ?Sized>(self, _: &T) -> Result<Key, Error> {
        Err(key_error())
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: &T,
    ) -> Result<Key, Error> {
        Err(key_error())
    }
    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq, Error> {
        Err(key_error())
    }
    fn serialize_tuple(self, _: usize) -> Result<Self::SerializeTuple, Error> {
        Err(key_error())
    }
    fn serialize_tuple_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeTupleStruct, Error> {
        Err(key_error())
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeTupleVariant, Error> {
        Err(key_error())
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Self::SerializeMap, Error> {
        Err(key_error())
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Self::SerializeStruct, Error> {
        Err(key_error())
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Self::SerializeStructVariant, Error> {
        Err(key_error())
    }
}

impl<'a> ser::Serializer for ValueSerializer<'a> {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = Compound<'a>;
    type SerializeTuple = Compound<'a>;
    type SerializeTupleStruct = Compound<'a>;
    type SerializeTupleVariant = Compound<'a>;
    type SerializeMap = ObjectCompound<'a>;
    type SerializeStruct = Compound<'a>;
    type SerializeStructVariant = Compound<'a>;

    fn serialize_bool(self, v: bool) -> Result<(), Error> {
        wire::put_bool(self.out, BOOL, v);
        Ok(())
    }
    fn serialize_i8(self, v: i8) -> Result<(), Error> {
        self.serialize_i64(v.into())
    }
    fn serialize_i16(self, v: i16) -> Result<(), Error> {
        self.serialize_i64(v.into())
    }
    fn serialize_i32(self, v: i32) -> Result<(), Error> {
        self.serialize_i64(v.into())
    }
    fn serialize_i64(self, v: i64) -> Result<(), Error> {
        wire::put_sint(self.out, INT, v);
        Ok(())
    }
    fn serialize_u8(self, v: u8) -> Result<(), Error> {
        self.serialize_i64(v.into())
    }
    fn serialize_u16(self, v: u16) -> Result<(), Error> {
        self.serialize_i64(v.into())
    }
    fn serialize_u32(self, v: u32) -> Result<(), Error> {
        self.serialize_i64(v.into())
    }
    fn serialize_u64(self, v: u64) -> Result<(), Error> {
        match i64::try_from(v) {
            Ok(v) => wire::put_sint(self.out, INT, v),
            Err(_) => wire::put_uint(self.out, UINT, v),
        }
        Ok(())
    }
    fn serialize_f32(self, v: f32) -> Result<(), Error> {
        self.serialize_f64(v.into())
    }
    fn serialize_f64(self, v: f64) -> Result<(), Error> {
        wire::put_double(self.out, FLOAT, v);
        Ok(())
    }
    fn serialize_char(self, v: char) -> Result<(), Error> {
        self.serialize_str(v.encode_utf8(&mut [0; 4]))
    }
    fn serialize_str(self, v: &str) -> Result<(), Error> {
        wire::put_str(self.out, STRING, v);
        Ok(())
    }
    fn serialize_bytes(self, v: &[u8]) -> Result<(), Error> {
        // As JSON would: a list of numbers.
        use ser::SerializeSeq;
        let mut seq = self.serialize_seq(Some(v.len()))?;
        for b in v {
            seq.serialize_element(b)?;
        }
        seq.end()
    }
    fn serialize_none(self) -> Result<(), Error> {
        self.serialize_unit()
    }
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<(), Error> {
        value.serialize(self)
    }
    fn serialize_unit(self) -> Result<(), Error> {
        wire::put_uint(self.out, NULL, 0);
        Ok(())
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<(), Error> {
        self.serialize_unit()
    }
    fn serialize_unit_variant(self, _: &'static str, _: u32, variant: &'static str) -> Result<(), Error> {
        self.serialize_str(variant)
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(self, _: &'static str, value: &T) -> Result<(), Error> {
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _: &'static str,
        _: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<(), Error> {
        let start = wire::begin_len(self.out, OBJECT);
        encode_entry(self.out, 1, variant, value)?;
        wire::end_len(self.out, start);
        Ok(())
    }
    fn serialize_seq(self, _: Option<usize>) -> Result<Compound<'a>, Error> {
        let start = wire::begin_len(self.out, LIST);
        Ok(Compound { out: self.out, start })
    }
    fn serialize_tuple(self, len: usize) -> Result<Compound<'a>, Error> {
        self.serialize_seq(Some(len))
    }
    fn serialize_tuple_struct(self, _: &'static str, len: usize) -> Result<Compound<'a>, Error> {
        self.serialize_seq(Some(len))
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        len: usize,
    ) -> Result<Compound<'a>, Error> {
        self.serialize_seq(Some(len))
    }
    fn serialize_map(self, _: Option<usize>) -> Result<ObjectCompound<'a>, Error> {
        let start = wire::begin_len(self.out, OBJECT);
        Ok(ObjectCompound { inner: Compound { out: self.out, start }, key: None })
    }
    fn serialize_struct(self, _: &'static str, _: usize) -> Result<Compound<'a>, Error> {
        let start = wire::begin_len(self.out, OBJECT);
        Ok(Compound { out: self.out, start })
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        _: &'static str,
        _: usize,
    ) -> Result<Compound<'a>, Error> {
        let start = wire::begin_len(self.out, OBJECT);
        Ok(Compound { out: self.out, start })
    }
}

impl ser::SerializeSeq for Compound<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        self.element(value)
    }
    fn end(self) -> Result<(), Error> {
        self.finish();
        Ok(())
    }
}

impl ser::SerializeTuple for Compound<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        self.element(value)
    }
    fn end(self) -> Result<(), Error> {
        self.finish();
        Ok(())
    }
}

impl ser::SerializeTupleStruct for Compound<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        self.element(value)
    }
    fn end(self) -> Result<(), Error> {
        self.finish();
        Ok(())
    }
}

impl ser::SerializeTupleVariant for Compound<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        self.element(value)
    }
    fn end(self) -> Result<(), Error> {
        self.finish();
        Ok(())
    }
}

impl ser::SerializeStruct for Compound<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, key: &'static str, value: &T) -> Result<(), Error> {
        self.entry(key, value)
    }
    fn end(self) -> Result<(), Error> {
        self.finish();
        Ok(())
    }
}

impl ser::SerializeStructVariant for Compound<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, key: &'static str, value: &T) -> Result<(), Error> {
        self.entry(key, value)
    }
    fn end(self) -> Result<(), Error> {
        self.finish();
        Ok(())
    }
}

impl ser::SerializeMap for ObjectCompound<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Error> {
        self.key = Some(key.serialize(KeySerializer)?);
        Ok(())
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        let key = self.key.take().ok_or_else(|| Error("a map value without a key".into()))?;
        self.inner.entry(key.as_str(), value)
    }
    fn serialize_entry<K: Serialize + ?Sized, V: Serialize + ?Sized>(
        &mut self,
        key: &K,
        value: &V,
    ) -> Result<(), Error> {
        encode_entry_with(self.inner.out, 1, key, value)
    }
    fn end(self) -> Result<(), Error> {
        self.inner.finish();
        Ok(())
    }
}

/// The value of a field that should hold a `Value` message.
pub fn decode_raw(raw: Raw<'_>) -> Result<Value, DecodeError> {
    match raw {
        Raw::Len(b) => decode(b),
        _ => err("expected a Value message"),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    #[test]
    fn values_round_trip_exactly() {
        for v in [
            json!(null),
            json!(true),
            json!(0),
            json!(-7),
            json!(u64::MAX),
            json!(i64::MIN),
            json!(1.0),
            json!(-2.5e300),
            json!("ü€"),
            json!([]),
            json!({}),
            json!({ "z": [1, { "a": null, "b": [true, 2.5, "s"] }], "a": { "": {} } }),
        ] {
            let mut out = Vec::new();
            super::encode(&mut out, &v);
            let back = super::decode(&out).unwrap();
            assert_eq!(back.to_string(), v.to_string(), "{v}");
        }
    }
}
