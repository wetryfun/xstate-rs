//! Snapshot encodings for the changelog.
//!
//! A record is one format byte followed by the payload, so the encoding can
//! change without breaking existing changelogs: records are decoded by their
//! own format byte, whatever the configured encoding is.

use serde_json::Value;

/// How snapshots are written to the changelog.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Codec {
    /// JSON text: readable with any Kafka tool.
    Json,
    /// MessagePack: smaller and faster to write and read.
    #[default]
    MessagePack,
}

const JSON: u8 = 1;
const MESSAGE_PACK: u8 = 2;

impl Codec {
    pub fn encode(self, snapshot: &Value) -> Vec<u8> {
        self.encode_serialize(snapshot)
    }

    /// Encodes anything that serializes as JSON data would, such as
    /// [`xstate_rt::Actor::persist_view`], without building a `Value`.
    pub fn encode_serialize<T: serde::Serialize + ?Sized>(self, snapshot: &T) -> Vec<u8> {
        match self {
            Codec::Json => {
                let mut out = Vec::with_capacity(512);
                out.push(JSON);
                serde_json::to_writer(&mut out, snapshot).expect("snapshots serialize");
                out
            }
            Codec::MessagePack => {
                let mut out = Vec::with_capacity(512);
                out.push(MESSAGE_PACK);
                // Maps as maps with string keys, so any reader can decode it.
                // The dedicated writer needs every length up front; anything
                // else goes through rmp_serde, which buffers.
                if crate::msgpack::write(&mut out, snapshot).is_err() {
                    out.truncate(1);
                    rmp_serde::encode::write_named(&mut out, snapshot).expect("snapshots serialize");
                }
                out
            }
        }
    }

    /// Decodes a record written by any codec.
    pub fn decode(record: &[u8]) -> Result<Value, String> {
        match record.split_first() {
            Some((&JSON, payload)) => {
                serde_json::from_slice(payload).map_err(|e| format!("invalid JSON snapshot: {e}"))
            }
            Some((&MESSAGE_PACK, payload)) => {
                rmp_serde::from_slice(payload).map_err(|e| format!("invalid MessagePack snapshot: {e}"))
            }
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
        for codec in [Codec::Json, Codec::MessagePack] {
            let bytes = codec.encode(&v);
            let back = Codec::decode(&bytes).unwrap();
            assert_eq!(back, v);
            assert_eq!(back.to_string(), v.to_string(), "{codec:?} keeps key order");
        }
        assert!(Codec::decode(&[]).is_err());
        assert!(Codec::decode(&[9, 1, 2]).is_err());
    }
}
