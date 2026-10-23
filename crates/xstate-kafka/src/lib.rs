//! XState machines as partition-sharded actors next to a Kafka-API
//! broker (Apache Kafka, Redpanda, ...).
//!
//! Each record on the input topic is an event (`{ "type": ..., ... }`, JSON)
//! for the actor named by the record key. Actors live in memory, one
//! [`engine::Partition`] per input partition, and their snapshots go to a
//! compacted changelog topic with the same partitioning. With the `kafka`
//! feature, [`service::Service`] runs one worker thread per core, each with its
//! own consumer (in one consumer group) and transactional producer: every
//! batch commits the emitted events, the changelog records and the input
//! offsets in one Kafka transaction, so state, output and progress move
//! together (exactly once). Timers fire from the wall clock and their effects
//! commit the same way.
//!
//! [`engine`] is deterministic and has no Kafka dependency.

pub mod checkpoint;
pub mod codec;
#[cfg(feature = "kafka")]
pub mod config;
#[cfg(feature = "kafka")]
pub mod effects;
pub mod engine;
pub mod metrics;
mod msgpack;
pub mod proto;
#[cfg(feature = "kafka")]
pub mod service;

pub use codec::{Codec, Mark};
pub use engine::{
    DeadLetter, Dedupe, EffectRequest, Emitted, EngineConfig, EventFormat, MachineSet, Output, Partition,
    RestoreErrors, SnapshotFormat, EFFECT_RESULT,
};
pub use metrics::{BatchCounts, Metrics, MetricsSnapshot};
