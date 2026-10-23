//! Protocol Buffers support, without generated code: the wire format, a
//! generic JSON `Value` message, snapshots (opt-in changelog encoding) and
//! input events whose messages derive from a machine's event schemas.

pub mod events;
pub mod snapshot;
pub mod value;
pub mod wire;

pub use wire::DecodeError;
