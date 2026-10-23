//! A small MessagePack writer for snapshots: a serde `Serializer` into a
//! `Vec<u8>` that cannot fail, for JSON-shaped data (maps with string keys,
//! sequences, strings, numbers, booleans, null). Records are read back with
//! `rmp_serde`; the encoding is the one `rmp_serde` would choose for the
//! same data (smallest integer and length markers, `f64` floats, maps with
//! string keys), so either can read the other's output.
//!
//! It exists because snapshots are written once per changed actor per
//! transaction, and `rmp_serde`'s general-purpose writer (fallible writes,
//! configuration, unknown-length buffering) cost several times the encoding
//! itself.

use std::fmt;

use serde::ser::{self, Serialize};

/// Writes `value` as MessagePack, appending to `out`.
pub fn write<T: Serialize + ?Sized>(out: &mut Vec<u8>, value: &T) -> Result<(), Error> {
    value.serialize(&mut Writer { out })
}

/// The only failure: a sequence or map whose length was not given up front,
/// or data that has no MessagePack form here.
#[derive(Debug)]
pub struct Error(String);

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

struct Writer<'a> {
    out: &'a mut Vec<u8>,
}

impl Writer<'_> {
    fn uint(&mut self, n: u64) {
        let out = &mut *self.out;
        if n < 0x80 {
            out.push(n as u8);
        } else if n <= u8::MAX as u64 {
            out.extend_from_slice(&[0xcc, n as u8]);
        } else if n <= u16::MAX as u64 {
            out.push(0xcd);
            out.extend_from_slice(&(n as u16).to_be_bytes());
        } else if n <= u32::MAX as u64 {
            out.push(0xce);
            out.extend_from_slice(&(n as u32).to_be_bytes());
        } else {
            out.push(0xcf);
            out.extend_from_slice(&n.to_be_bytes());
        }
    }

    fn int(&mut self, n: i64) {
        if n >= 0 {
            return self.uint(n as u64);
        }
        let out = &mut *self.out;
        if n >= -32 {
            out.push(n as i8 as u8);
        } else if n >= i8::MIN as i64 {
            out.extend_from_slice(&[0xd0, n as i8 as u8]);
        } else if n >= i16::MIN as i64 {
            out.push(0xd1);
            out.extend_from_slice(&(n as i16).to_be_bytes());
        } else if n >= i32::MIN as i64 {
            out.push(0xd2);
            out.extend_from_slice(&(n as i32).to_be_bytes());
        } else {
            out.push(0xd3);
            out.extend_from_slice(&n.to_be_bytes());
        }
    }

    fn str(&mut self, s: &str) {
        let len = s.len();
        let out = &mut *self.out;
        if len < 32 {
            out.push(0xa0 | len as u8);
        } else if len <= u8::MAX as usize {
            out.extend_from_slice(&[0xd9, len as u8]);
        } else if len <= u16::MAX as usize {
            out.push(0xda);
            out.extend_from_slice(&(len as u16).to_be_bytes());
        } else {
            out.push(0xdb);
            out.extend_from_slice(&(len as u32).to_be_bytes());
        }
        out.extend_from_slice(s.as_bytes());
    }

    fn bin(&mut self, b: &[u8]) {
        let len = b.len();
        let out = &mut *self.out;
        if len <= u8::MAX as usize {
            out.extend_from_slice(&[0xc4, len as u8]);
        } else if len <= u16::MAX as usize {
            out.push(0xc5);
            out.extend_from_slice(&(len as u16).to_be_bytes());
        } else {
            out.push(0xc6);
            out.extend_from_slice(&(len as u32).to_be_bytes());
        }
        out.extend_from_slice(b);
    }

    fn array_len(&mut self, len: usize) {
        let out = &mut *self.out;
        if len < 16 {
            out.push(0x90 | len as u8);
        } else if len <= u16::MAX as usize {
            out.push(0xdc);
            out.extend_from_slice(&(len as u16).to_be_bytes());
        } else {
            out.push(0xdd);
            out.extend_from_slice(&(len as u32).to_be_bytes());
        }
    }

    fn map_len(&mut self, len: usize) {
        let out = &mut *self.out;
        if len < 16 {
            out.push(0x80 | len as u8);
        } else if len <= u16::MAX as usize {
            out.push(0xde);
            out.extend_from_slice(&(len as u16).to_be_bytes());
        } else {
            out.push(0xdf);
            out.extend_from_slice(&(len as u32).to_be_bytes());
        }
    }
}

fn unknown_length() -> Error {
    Error("a sequence or map of unknown length".into())
}

impl<'a, 'b> ser::Serializer for &'b mut Writer<'a> {
    type Ok = ();
    type Error = Error;
    type SerializeSeq = Self;
    type SerializeTuple = Self;
    type SerializeTupleStruct = Self;
    type SerializeTupleVariant = Self;
    type SerializeMap = Self;
    type SerializeStruct = Self;
    type SerializeStructVariant = Self;

    fn serialize_bool(self, v: bool) -> Result<(), Error> {
        self.out.push(if v { 0xc3 } else { 0xc2 });
        Ok(())
    }
    fn serialize_i8(self, v: i8) -> Result<(), Error> {
        self.int(v.into());
        Ok(())
    }
    fn serialize_i16(self, v: i16) -> Result<(), Error> {
        self.int(v.into());
        Ok(())
    }
    fn serialize_i32(self, v: i32) -> Result<(), Error> {
        self.int(v.into());
        Ok(())
    }
    fn serialize_i64(self, v: i64) -> Result<(), Error> {
        self.int(v);
        Ok(())
    }
    fn serialize_u8(self, v: u8) -> Result<(), Error> {
        self.uint(v.into());
        Ok(())
    }
    fn serialize_u16(self, v: u16) -> Result<(), Error> {
        self.uint(v.into());
        Ok(())
    }
    fn serialize_u32(self, v: u32) -> Result<(), Error> {
        self.uint(v.into());
        Ok(())
    }
    fn serialize_u64(self, v: u64) -> Result<(), Error> {
        self.uint(v);
        Ok(())
    }
    fn serialize_f32(self, v: f32) -> Result<(), Error> {
        self.out.push(0xca);
        self.out.extend_from_slice(&v.to_be_bytes());
        Ok(())
    }
    fn serialize_f64(self, v: f64) -> Result<(), Error> {
        self.out.push(0xcb);
        self.out.extend_from_slice(&v.to_be_bytes());
        Ok(())
    }
    fn serialize_char(self, v: char) -> Result<(), Error> {
        self.str(v.encode_utf8(&mut [0; 4]));
        Ok(())
    }
    fn serialize_str(self, v: &str) -> Result<(), Error> {
        self.str(v);
        Ok(())
    }
    fn serialize_bytes(self, v: &[u8]) -> Result<(), Error> {
        self.bin(v);
        Ok(())
    }
    fn serialize_none(self) -> Result<(), Error> {
        self.out.push(0xc0);
        Ok(())
    }
    fn serialize_some<T: Serialize + ?Sized>(self, value: &T) -> Result<(), Error> {
        value.serialize(self)
    }
    fn serialize_unit(self) -> Result<(), Error> {
        self.out.push(0xc0);
        Ok(())
    }
    fn serialize_unit_struct(self, _name: &'static str) -> Result<(), Error> {
        self.serialize_unit()
    }
    fn serialize_unit_variant(self, _name: &'static str, _index: u32, variant: &'static str) -> Result<(), Error> {
        self.str(variant);
        Ok(())
    }
    fn serialize_newtype_struct<T: Serialize + ?Sized>(self, _name: &'static str, value: &T) -> Result<(), Error> {
        value.serialize(self)
    }
    fn serialize_newtype_variant<T: Serialize + ?Sized>(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<(), Error> {
        self.map_len(1);
        self.str(variant);
        value.serialize(self)
    }
    fn serialize_seq(self, len: Option<usize>) -> Result<Self, Error> {
        self.array_len(len.ok_or_else(unknown_length)?);
        Ok(self)
    }
    fn serialize_tuple(self, len: usize) -> Result<Self, Error> {
        self.array_len(len);
        Ok(self)
    }
    fn serialize_tuple_struct(self, _name: &'static str, len: usize) -> Result<Self, Error> {
        self.array_len(len);
        Ok(self)
    }
    fn serialize_tuple_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self, Error> {
        self.map_len(1);
        self.str(variant);
        self.array_len(len);
        Ok(self)
    }
    fn serialize_map(self, len: Option<usize>) -> Result<Self, Error> {
        self.map_len(len.ok_or_else(unknown_length)?);
        Ok(self)
    }
    fn serialize_struct(self, _name: &'static str, len: usize) -> Result<Self, Error> {
        self.map_len(len);
        Ok(self)
    }
    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self, Error> {
        self.map_len(1);
        self.str(variant);
        self.map_len(len);
        Ok(self)
    }
    fn collect_str<T: fmt::Display + ?Sized>(self, value: &T) -> Result<(), Error> {
        self.str(&value.to_string());
        Ok(())
    }
}

impl ser::SerializeSeq for &mut Writer<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        Ok(())
    }
}

impl ser::SerializeTuple for &mut Writer<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_element<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        Ok(())
    }
}

impl ser::SerializeTupleStruct for &mut Writer<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        Ok(())
    }
}

impl ser::SerializeTupleVariant for &mut Writer<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        Ok(())
    }
}

impl ser::SerializeMap for &mut Writer<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_key<T: Serialize + ?Sized>(&mut self, key: &T) -> Result<(), Error> {
        key.serialize(&mut **self)
    }
    fn serialize_value<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<(), Error> {
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        Ok(())
    }
}

impl ser::SerializeStruct for &mut Writer<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, key: &'static str, value: &T) -> Result<(), Error> {
        self.str(key);
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        Ok(())
    }
}

impl ser::SerializeStructVariant for &mut Writer<'_> {
    type Ok = ();
    type Error = Error;
    fn serialize_field<T: Serialize + ?Sized>(&mut self, key: &'static str, value: &T) -> Result<(), Error> {
        self.str(key);
        value.serialize(&mut **self)
    }
    fn end(self) -> Result<(), Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    /// The same bytes as `rmp_serde` (named maps), and they decode back.
    fn check(v: &Value) {
        let mut ours = Vec::new();
        super::write(&mut ours, v).unwrap();
        let mut theirs = Vec::new();
        rmp_serde::encode::write_named(&mut theirs, v).unwrap();
        assert_eq!(ours, theirs, "{v}");
        let back: Value = rmp_serde::from_slice(&ours).unwrap();
        assert_eq!(back.to_string(), v.to_string());
    }

    #[test]
    fn matches_rmp_serde() {
        let long = |n: usize| "x".repeat(n);
        let ints: Vec<Value> = [
            0i64,
            1,
            127,
            128,
            255,
            256,
            65_535,
            65_536,
            4_294_967_295,
            4_294_967_296,
            -1,
            -32,
            -33,
            -128,
            -129,
            -32_768,
            -32_769,
            -2_147_483_648,
            -2_147_483_649,
            i64::MIN,
            i64::MAX,
        ]
        .iter()
        .map(|&n| json!(n))
        .collect();
        for v in ints {
            check(&v);
        }
        check(&json!(u64::MAX));
        for v in [json!(1.5), json!(-0.25), json!(1e300), json!(null), json!(true), json!(false), json!("ü€")] {
            check(&v);
        }
        for n in [0, 31, 32, 255, 256, 65_535, 65_536] {
            check(&json!(long(n)));
        }
        for n in [0, 15, 16, 65_535, 65_536] {
            check(&Value::Array((0..n).map(|i| json!(i % 3)).collect()));
            check(&Value::Object((0..n).map(|i| (format!("k{i}"), json!(i))).collect()));
        }
        check(&json!({ "z": [1, { "a": null, "b": [true, 2.5, "s"] }], "a": { "": {} } }));
    }
}
