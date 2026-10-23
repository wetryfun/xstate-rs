//! XState machines as partition-sharded actors next to a Redpanda (or any
//! Kafka API) broker.
//!
//! Each record on the input topic is an event (`{ "type": ..., ... }`, JSON)
//! for the actor named by the record key. Actors live in memory, one
//! [`engine::Partition`] per input partition, and their snapshots go to a
//! compacted changelog topic with the same partitioning. With the `kafka`
//! feature, [`kafka::Service`] runs one worker thread per core, each with its
//! own consumer (in one consumer group) and transactional producer: every
//! batch commits the emitted events, the changelog records and the input
//! offsets in one Kafka transaction, so state, output and progress move
//! together (exactly once). Timers fire from the wall clock and their effects
//! commit the same way.
//!
//! [`engine`] is deterministic and has no Kafka dependency.

pub mod codec;
pub mod engine;
#[cfg(feature = "kafka")]
pub mod kafka;

pub use codec::Codec;
pub use engine::{DeadLetter, Emitted, EngineConfig, Output, Partition};
