//! Protocol Buffers wire format: varints, tags, length-delimited fields.
//! Writing is infallible (into a `Vec<u8>`); reading checks every bound.

/// Wire types used here.
pub const VARINT: u8 = 0;
pub const I64: u8 = 1;
pub const LEN: u8 = 2;
pub const I32: u8 = 5;

#[inline]
pub fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

#[inline]
pub fn put_tag(out: &mut Vec<u8>, field: u32, wire: u8) {
    put_varint(out, (u64::from(field) << 3) | u64::from(wire));
}

#[inline]
pub fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

pub fn unzigzag(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

#[inline]
pub fn put_uint(out: &mut Vec<u8>, field: u32, v: u64) {
    put_tag(out, field, VARINT);
    put_varint(out, v);
}

#[inline]
pub fn put_sint(out: &mut Vec<u8>, field: u32, v: i64) {
    put_uint(out, field, zigzag(v));
}

#[inline]
pub fn put_bool(out: &mut Vec<u8>, field: u32, v: bool) {
    put_uint(out, field, u64::from(v));
}

#[inline]
pub fn put_double(out: &mut Vec<u8>, field: u32, v: f64) {
    put_tag(out, field, I64);
    out.extend_from_slice(&v.to_le_bytes());
}

#[inline]
pub fn put_bytes(out: &mut Vec<u8>, field: u32, v: &[u8]) {
    put_tag(out, field, LEN);
    put_varint(out, v.len() as u64);
    out.extend_from_slice(v);
}

#[inline]
pub fn put_str(out: &mut Vec<u8>, field: u32, v: &str) {
    put_bytes(out, field, v.as_bytes());
}

/// Starts a length-delimited field whose length is not known yet; finish it
/// with [`end_len`]. Returns where its content starts. One byte is reserved
/// for the length: content shorter than 128 bytes needs no moving.
#[inline]
pub fn begin_len(out: &mut Vec<u8>, field: u32) -> usize {
    put_tag(out, field, LEN);
    out.push(0);
    out.len()
}

/// Writes the length of the field begun at `start`. Longer content is moved
/// right by the extra bytes of the length varint.
#[inline]
pub fn end_len(out: &mut Vec<u8>, start: usize) {
    let len = (out.len() - start) as u64;
    if len < 0x80 {
        out[start - 1] = len as u8;
        return;
    }
    let mut varint = [0u8; 10];
    let mut n = 0;
    let mut v = len;
    while v >= 0x80 {
        varint[n] = (v as u8) | 0x80;
        v >>= 7;
        n += 1;
    }
    varint[n] = v as u8;
    out[start - 1] = varint[0];
    out.splice(start..start, varint[1..=n].iter().copied());
}

/// A failure to read a message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodeError(pub String);

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for DecodeError {}

pub fn err<T>(what: impl Into<String>) -> Result<T, DecodeError> {
    Err(DecodeError(what.into()))
}

/// A field's value as read from the wire.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Raw<'a> {
    Varint(u64),
    I64(u64),
    Len(&'a [u8]),
    I32(u32),
}

impl<'a> Raw<'a> {
    pub fn varint(self) -> Result<u64, DecodeError> {
        match self {
            Raw::Varint(v) => Ok(v),
            _ => err("expected a varint"),
        }
    }

    pub fn bytes(self) -> Result<&'a [u8], DecodeError> {
        match self {
            Raw::Len(b) => Ok(b),
            _ => err("expected a length-delimited field"),
        }
    }

    pub fn str(self) -> Result<&'a str, DecodeError> {
        std::str::from_utf8(self.bytes()?).or_else(|_| err("a string is not UTF-8"))
    }

    pub fn double(self) -> Result<f64, DecodeError> {
        match self {
            Raw::I64(v) => Ok(f64::from_bits(v)),
            Raw::I32(v) => Ok(f64::from(f32::from_bits(v))),
            _ => err("expected a double"),
        }
    }
}

/// Reads the fields of one message in order.
pub struct Reader<'a> {
    buf: &'a [u8],
}

impl<'a> Reader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        Self { buf }
    }

    fn varint(&mut self) -> Result<u64, DecodeError> {
        let mut v = 0u64;
        for i in 0..10 {
            let Some((&b, rest)) = self.buf.split_first() else { return err("truncated varint") };
            self.buf = rest;
            v |= u64::from(b & 0x7f) << (7 * i);
            if b < 0x80 {
                return Ok(v);
            }
        }
        err("varint too long")
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.buf.len() < n {
            return err("truncated field");
        }
        let (head, rest) = self.buf.split_at(n);
        self.buf = rest;
        Ok(head)
    }

    /// The next field number and value, or `None` at the end.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<(u32, Raw<'a>)>, DecodeError> {
        if self.buf.is_empty() {
            return Ok(None);
        }
        let key = self.varint()?;
        let field = u32::try_from(key >> 3).or_else(|_| err("field number too large"))?;
        if field == 0 {
            return err("field number 0");
        }
        let raw = match (key & 7) as u8 {
            VARINT => Raw::Varint(self.varint()?),
            I64 => Raw::I64(u64::from_le_bytes(self.take(8)?.try_into().expect("8 bytes"))),
            LEN => {
                let n = usize::try_from(self.varint()?).or_else(|_| err("length too large"))?;
                Raw::Len(self.take(n)?)
            }
            I32 => Raw::I32(u32::from_le_bytes(self.take(4)?.try_into().expect("4 bytes"))),
            w => return err(format!("unsupported wire type {w}")),
        };
        Ok(Some((field, raw)))
    }
}

/// The varints of a packed repeated field.
pub fn packed_varints(bytes: &[u8]) -> Result<Vec<u64>, DecodeError> {
    let mut r = Reader { buf: bytes };
    let mut out = Vec::new();
    while !r.buf.is_empty() {
        out.push(r.varint()?);
    }
    Ok(out)
}

/// The doubles of a packed repeated field.
pub fn packed_doubles(bytes: &[u8]) -> Result<Vec<f64>, DecodeError> {
    if bytes.len() % 8 != 0 {
        return err("packed doubles of a length not a multiple of 8");
    }
    Ok(bytes.chunks_exact(8).map(|c| f64::from_le_bytes(c.try_into().expect("8 bytes"))).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varints_zigzag_and_nested_lengths() {
        for v in [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX] {
            let mut out = Vec::new();
            put_uint(&mut out, 1, v);
            let (f, raw) = Reader::new(&out).next().unwrap().unwrap();
            assert_eq!((f, raw.varint().unwrap()), (1, v));
        }
        for v in [0i64, -1, 1, -64, 64, i64::MIN, i64::MAX] {
            assert_eq!(unzigzag(zigzag(v)), v);
        }
        let mut out = Vec::new();
        let start = begin_len(&mut out, 7);
        out.extend(std::iter::repeat_n(1u8, 200));
        end_len(&mut out, start);
        let (f, raw) = Reader::new(&out).next().unwrap().unwrap();
        assert_eq!((f, raw.bytes().unwrap().len()), (7, 200));
        assert!(Reader::new(&[0x08]).next().is_err(), "truncated");
    }
}
