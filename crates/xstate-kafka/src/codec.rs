//! Snapshot encodings for the changelog.
//!
//! A record is one format byte followed by the payload, so the encoding can
//! change without breaking existing changelogs: records are decoded by their
//! own format byte, whatever the configured encoding is.
//!
//! With deduplication on (`engine::Dedupe`), a record starts with the
//! actor's [`Mark`] instead: [`MARK`], the epoch and the sequence as
//! varints, then the snapshot record. A finished, evicted actor leaves a
//! [`RETIRED`] record: its mark and when it was retired (varints), with no
//! snapshot.

use serde_json::Value;

/// How snapshots are written to the changelog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Codec {
    /// JSON text: readable with any Kafka tool.
    Json,
    /// MessagePack: smaller and faster to write and read.
    #[default]
    MessagePack,
    /// Protocol Buffers (`proto/xstate_snapshot.proto`): field numbers
    /// instead of names. Always the compact snapshot form; not for XState's
    /// own restore.
    Protobuf,
    /// Avro in the Schema Registry wire format (`crate::avro`). Written and
    /// read by a partition, which holds the schemas: the other codecs'
    /// functions do not handle it.
    Avro,
}

const JSON: u8 = 1;
const MESSAGE_PACK: u8 = 2;
const PROTOBUF: u8 = 3;
/// The format byte of a record that starts with a [`Mark`].
pub const MARK: u8 = 16;
/// The format byte of the record of a retired actor's [`Mark`].
pub const RETIRED: u8 = 17;

/// The latest input event an actor took, by its `(epoch, sequence)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Mark {
    pub epoch: u64,
    pub sequence: u64,
}

/// Writes the prefix of a record with a mark.
pub fn write_mark(out: &mut Vec<u8>, mark: Mark) {
    out.push(MARK);
    crate::proto::wire::put_varint(out, mark.epoch);
    crate::proto::wire::put_varint(out, mark.sequence);
}

/// Writes the record of a retired actor's mark, retired at `at`.
pub fn write_retired(out: &mut Vec<u8>, mark: Mark, at: u64) {
    out.push(RETIRED);
    crate::proto::wire::put_varint(out, mark.epoch);
    crate::proto::wire::put_varint(out, mark.sequence);
    crate::proto::wire::put_varint(out, at);
}

/// When a [`RETIRED`] record's actor was retired (`None` for other records,
/// and for records of a mark alone written before retirement times were).
pub fn retired_at(record: &[u8]) -> Option<u64> {
    let (&RETIRED, mut rest) = record.split_first()? else { return None };
    read_varint(&mut rest).ok()?;
    read_varint(&mut rest).ok()?;
    read_varint(&mut rest).ok()
}

fn read_varint(rest: &mut &[u8]) -> Result<u64, String> {
    let mut v = 0u64;
    for i in 0..10 {
        let (&b, tail) = rest.split_first().ok_or("truncated record mark")?;
        *rest = tail;
        v |= u64::from(b & 0x7f) << (7 * i);
        if b < 0x80 {
            return Ok(v);
        }
    }
    Err("invalid record mark".into())
}

/// Splits a record into its mark (if it has one) and its snapshot record,
/// which is empty for the record of a retired actor.
pub fn split_mark(record: &[u8]) -> Result<(Option<Mark>, &[u8]), String> {
    let retired = match record.first() {
        Some(&MARK) => false,
        Some(&RETIRED) => true,
        _ => return Ok((None, record)),
    };
    let mut rest = &record[1..];
    let mut varint = || read_varint(&mut rest);
    let (epoch, sequence) = (varint()?, varint()?);
    if retired {
        varint()?;
        return Ok((Some(Mark { epoch, sequence }), &[]));
    }
    Ok((Some(Mark { epoch, sequence }), rest))
}

impl Codec {
    pub fn encode(self, snapshot: &Value) -> Vec<u8> {
        self.encode_serialize(snapshot)
    }

    /// Encodes anything that serializes as JSON data would, such as
    /// [`xstate_rt::Actor::persist_view`], without building a `Value`.
    pub fn encode_serialize<T: serde::Serialize + ?Sized>(self, snapshot: &T) -> Vec<u8> {
        let mut out = Vec::with_capacity(if self == Codec::Protobuf { 256 } else { 512 });
        self.encode_into(&mut out, snapshot);
        out
    }

    /// [`Codec::encode_serialize`], appending to `out`.
    pub fn encode_into<T: serde::Serialize + ?Sized>(self, out: &mut Vec<u8>, snapshot: &T) {
        let start = out.len();
        match self {
            Codec::Json => {
                out.push(JSON);
                serde_json::to_writer(&mut *out, snapshot).expect("snapshots serialize");
            }
            Codec::MessagePack => {
                out.push(MESSAGE_PACK);
                // Maps as maps with string keys, so any reader can decode it.
                // The dedicated writer needs every length up front; anything
                // else goes through rmp_serde, which buffers.
                if crate::msgpack::write(out, snapshot).is_err() {
                    out.truncate(start + 1);
                    rmp_serde::encode::write_named(out, snapshot).expect("snapshots serialize");
                }
            }
            Codec::Protobuf => {
                out.push(PROTOBUF);
                crate::proto::snapshot::encode(out, snapshot).expect("snapshots serialize");
            }
            Codec::Avro => panic!("Avro snapshots need the schema registry: a partition writes them"),
        }
    }

    /// Decodes a record written by any codec (skipping its mark, if any).
    pub fn decode(record: &[u8]) -> Result<Value, String> {
        let (_, record) = split_mark(record)?;
        match record.split_first() {
            Some((&JSON, payload)) => {
                serde_json::from_slice(payload).map_err(|e| format!("invalid JSON snapshot: {e}"))
            }
            Some((&MESSAGE_PACK, payload)) => {
                rmp_serde::from_slice(payload).map_err(|e| format!("invalid MessagePack snapshot: {e}"))
            }
            Some((&PROTOBUF, payload)) => {
                crate::proto::snapshot::decode(payload).map_err(|e| format!("invalid protobuf snapshot: {e}"))
            }
            Some((&crate::avro::MAGIC, _)) => Err("an Avro snapshot needs the schema registry".into()),
            Some((format, _)) => Err(format!("unknown snapshot format {format}")),
            None => Err("empty snapshot record".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_keep_key_order_and_numbers() {
        let v =
            serde_json::json!({ "z": 1, "a": [1.5, -2, "s", null, true], "m": { "y": {}, "b": 9007199254740991u64 } });
        for codec in [Codec::Json, Codec::MessagePack, Codec::Protobuf] {
            let bytes = codec.encode(&v);
            let back = Codec::decode(&bytes).unwrap();
            assert_eq!(back, v);
            assert_eq!(back.to_string(), v.to_string(), "{codec:?} keeps key order");
        }
        assert!(Codec::decode(&[]).is_err());
        assert!(Codec::decode(&[9, 1, 2]).is_err());
    }

    #[test]
    fn marks_prefix_records() {
        let v = serde_json::json!({ "a": 1 });
        let mark = Mark { epoch: 3, sequence: 300 };
        let mut record = Vec::new();
        write_mark(&mut record, mark);
        let prefix = record.len();
        Codec::MessagePack.encode_into(&mut record, &v);
        assert_eq!(split_mark(&record).unwrap(), (Some(mark), &record[prefix..]));
        assert_eq!(Codec::decode(&record).unwrap(), v);
        assert_eq!(split_mark(&record[..prefix]).unwrap(), (Some(mark), &[][..]));
        assert!(split_mark(&record[..2]).is_err());
        let plain = Codec::Json.encode(&v);
        assert_eq!(split_mark(&plain).unwrap(), (None, &plain[..]));
        assert!(Mark { epoch: 1, sequence: 0 } > Mark { epoch: 0, sequence: 9 });
    }
}
