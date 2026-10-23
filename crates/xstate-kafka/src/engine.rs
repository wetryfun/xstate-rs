//! The deterministic part of the service: one [`Partition`] per input
//! partition, holding one actor per record key.
//!
//! A partition never touches Kafka or a clock. The driver feeds it records
//! and the current time, and takes an [`Output`] per transaction: the events
//! the actors emitted, the changelog records for the actors that changed
//! (their persisted snapshots, or tombstones), external work requested by
//! the actors, and dead letters for records that could not be processed.
//!
//! External work is what the runtime leaves to the host, chiefly the async
//! actors a machine invokes (`@xstate.effect`). Each piece of work is
//! published once, as XState's `SystemExternalEffect` JSON (`id`, `source`,
//! `type`, `params`, ...), and stays pending in the actor's snapshot. Its
//! result comes back as an input record for the same key, in XState's own
//! shape: `{ "type": "xstate.system.effect.result", "effectId": "system:3",
//! "event": { "type": "xstate.async.resolve", "data": ... } }` (or
//! `xstate.async.reject` with `data`; no `event` just retires the work).
//!
//! Input can arrive more than once (MQTT QoS 1, producer retries). With
//! [`Dedupe`], events carry a sequence number (and optionally an epoch) and
//! an actor takes each `(epoch, sequence)` only if it is greater than the
//! last one it took; others are counted as duplicates and skipped. The mark
//! is part of the actor's changelog record, and a finished actor that is
//! evicted leaves a record of its mark alone, so a late duplicate cannot
//! start a new actor.

use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use xstate_rt::{Actor, EventType, InitOptions, Machine, Status};

use crate::codec::{self, Codec, Mark};
use crate::proto::events::EventSchema;

/// How a partition treats its actors.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Snapshot encoding for the changelog.
    pub codec: Codec,
    /// Drop an actor once it is done or stopped and write a tombstone, so
    /// the compacted changelog forgets it. A later event for the same key
    /// starts a new actor. When false, final snapshots are kept.
    pub evict_final: bool,
    /// What a changelog snapshot holds.
    pub snapshots: SnapshotFormat,
    /// How input records are encoded when the record does not say (see
    /// [`EventFormat::from_content_type`]).
    pub events: EventFormat,
    /// The protobuf form of the machine's input events, for
    /// [`EventFormat::Protobuf`] records.
    pub event_schema: Option<Arc<EventSchema>>,
    /// Skip input events an actor already took.
    pub dedupe: Option<Dedupe>,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            codec: Codec::default(),
            evict_final: true,
            snapshots: SnapshotFormat::default(),
            events: EventFormat::default(),
            event_schema: None,
            dedupe: None,
        }
    }
}

/// Which event fields order an actor's input. An event without the
/// `sequence` field is always taken (results of external work are never
/// deduplicated: they are matched to pending work by id). Values are
/// non-negative integers; an event is new when its `(epoch, sequence)` is
/// greater than the last one its actor took, so a device whose counter
/// restarts at boot sends a greater epoch (a boot counter, or the boot
/// time). A missing epoch is 0.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Dedupe {
    pub sequence: String,
    #[serde(default)]
    pub epoch: Option<String>,
}

impl Dedupe {
    /// The event's mark, `None` when it has no sequence.
    fn mark(&self, event: &Value) -> Result<Option<Mark>, String> {
        let counter = |field: &str| -> Result<Option<u64>, String> {
            match event.get(field) {
                None | Some(Value::Null) => Ok(None),
                Some(v) => v
                    .as_u64()
                    .or_else(|| {
                        // Whole floats (protobuf `double` fields, say) up to 2^53.
                        v.as_f64()
                            .filter(|f| *f >= 0.0 && f.fract() == 0.0 && *f <= 9_007_199_254_740_991.0)
                            .map(|f| f as u64)
                    })
                    .map(Some)
                    .ok_or_else(|| format!("\"{field}\" must be a non-negative integer, not {v}")),
            }
        };
        let Some(sequence) = counter(&self.sequence)? else { return Ok(None) };
        let epoch = match &self.epoch {
            Some(field) => counter(field)?.unwrap_or(0),
            None => 0,
        };
        Ok(Some(Mark { epoch, sequence }))
    }
}

/// How an input record's event is encoded.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EventFormat {
    /// `{ "type": ..., ... }` as JSON text.
    #[default]
    Json,
    /// An `Event` message of the machine's events `.proto`
    /// ([`EventSchema::proto_file`]).
    Protobuf,
    /// The JSON event as MessagePack.
    MessagePack,
}

impl EventFormat {
    /// The format a `content-type` header names: `application/json`,
    /// `application/x-protobuf` (or `application/protobuf`,
    /// `application/vnd.google.protobuf`) and `application/msgpack` (or
    /// `application/x-msgpack`), parameters ignored.
    pub fn from_content_type(content_type: &str) -> Option<Self> {
        let media = content_type.split(';').next().unwrap_or("").trim().to_ascii_lowercase();
        match media.as_str() {
            "application/json" | "text/json" => Some(EventFormat::Json),
            "application/x-protobuf" | "application/protobuf" | "application/vnd.google.protobuf" => {
                Some(EventFormat::Protobuf)
            }
            "application/msgpack" | "application/x-msgpack" | "application/vnd.msgpack" => {
                Some(EventFormat::MessagePack)
            }
            _ => None,
        }
    }
}

/// The shape of changelog snapshots. Restore reads either.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SnapshotFormat {
    /// XState's `SystemSnapshot` without what restore rebuilds identically
    /// (`xstate_rt::System::compact_view`): smaller and faster to write,
    /// but not for XState's own restore. Opt in when only this service
    /// reads the changelog.
    Compact,
    /// XState's full `SystemSnapshot`, as XState persists it.
    #[default]
    XState,
}

/// An event an actor emitted.
#[derive(Debug, Clone, PartialEq)]
pub struct Emitted {
    pub key: String,
    pub event: Value,
}

/// A record the partition could not process, and why.
#[derive(Debug, Clone, PartialEq)]
pub struct DeadLetter {
    pub key: Option<String>,
    pub payload: Vec<u8>,
    pub reason: String,
}

/// External work an actor requested.
#[derive(Debug, Clone, PartialEq)]
pub struct EffectRequest {
    pub key: String,
    /// XState's `SystemExternalEffect` data.
    pub effect: Value,
}

/// The input record type that delivers the result of external work.
pub const EFFECT_RESULT: &str = "xstate.system.effect.result";

/// What a partition produced since the last [`Partition::flush`].
#[derive(Debug, Default)]
pub struct Output {
    pub emitted: Vec<Emitted>,
    /// Changelog records, one per changed actor: its encoded snapshot, or
    /// `None` (a tombstone) when it was evicted (with [`Dedupe`], a record
    /// of its mark alone).
    pub changelog: Vec<(String, Option<Vec<u8>>)>,
    pub dead_letters: Vec<DeadLetter>,
    /// New external work, in the order it was requested.
    pub effects: Vec<EffectRequest>,
    /// Input events skipped as duplicates ([`Dedupe`]).
    pub duplicates: u64,
}

impl Output {
    pub fn is_empty(&self) -> bool {
        self.emitted.is_empty()
            && self.changelog.is_empty()
            && self.dead_letters.is_empty()
            && self.effects.is_empty()
            && self.duplicates == 0
    }
}

/// Record keys are external input: hash them with a fast, randomly seeded
/// hasher.
type HashMap<K, V> = std::collections::HashMap<K, V, foldhash::fast::RandomState>;

/// An actor and the partition's bookkeeping for it.
struct Slot<C, E> {
    actor: Actor<C, E>,
    /// Changed since the last flush (and listed in `Partition::dirty`).
    dirty: bool,
    /// The deadline last pushed to `Partition::timers`.
    scheduled: Option<u64>,
    /// Ids of the actor's pending external work that was already published
    /// (a few at most: a list is smaller than a set).
    published: Vec<String>,
    /// The latest input event the actor took ([`Dedupe`]).
    mark: Option<Mark>,
}

impl<C, E> Slot<C, E> {
    fn new(actor: Actor<C, E>) -> Box<Self> {
        Box::new(Self { actor, dirty: false, scheduled: None, published: Vec::new(), mark: None })
    }
}

/// The actors of one input partition, keyed by record key.
pub struct Partition<C, E> {
    machine: Arc<Machine<C, E>>,
    config: EngineConfig,
    /// Boxed: a slot is several hundred bytes, and the table keeps spare
    /// capacity (up to half of it after growing), which would hold that
    /// much per empty bucket inline.
    actors: HashMap<String, Box<Slot<C, E>>>,
    /// Marks of evicted actors ([`Dedupe`]): a key's next actor takes only
    /// events past it.
    retired: HashMap<String, Mark>,
    /// Due times of actor timers. Entries can be stale; each is checked
    /// against the actor's own next deadline when it comes due.
    timers: BinaryHeap<Reverse<(u64, String)>>,
    /// Actors changed since the last flush, in order of first change.
    dirty: Vec<String>,
    output: Output,
    /// The partition's clock: never moves backwards.
    now: u64,
}

impl<C, E> Partition<C, E>
where
    C: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    E: EventType + Send + Sync,
{
    pub fn new(machine: Arc<Machine<C, E>>, config: EngineConfig) -> Self {
        Self {
            machine,
            config,
            actors: HashMap::default(),
            retired: HashMap::default(),
            timers: BinaryHeap::new(),
            dirty: Vec::new(),
            output: Output::default(),
            now: 0,
        }
    }

    /// Number of live actors.
    pub fn len(&self) -> usize {
        self.actors.len()
    }

    pub fn is_empty(&self) -> bool {
        self.actors.is_empty()
    }

    /// Number of evicted actors whose marks are kept ([`Dedupe`]).
    pub fn retired(&self) -> usize {
        self.retired.len()
    }

    pub fn actor(&self, key: &str) -> Option<&Actor<C, E>> {
        self.actors.get(key).map(|s| &s.actor)
    }

    /// Applies one changelog record while restoring: a snapshot replaces the
    /// key's actor, a tombstone removes it, and a mark alone removes it but
    /// keeps the mark. Produces no output.
    pub fn restore(&mut self, key: &str, record: Option<&[u8]>) -> Result<(), String> {
        let Some(record) = record else {
            self.actors.remove(key);
            self.retired.remove(key);
            return Ok(());
        };
        let (mark, body) = codec::split_mark(record)?;
        // Marks count only while deduplication is on.
        let mark = mark.filter(|_| self.config.dedupe.is_some());
        if body.is_empty() {
            self.actors.remove(key);
            match mark {
                Some(mark) => self.retired.insert(key.to_owned(), mark),
                None => self.retired.remove(key),
            };
            return Ok(());
        }
        self.retired.remove(key);
        let snapshot = Codec::decode(body)?;
        let actor = Actor::restore(self.machine.clone(), &snapshot).map_err(|e| format!("key {key:?}: {e}"))?;
        self.now = self.now.max(actor.now());
        let mut slot = Slot::new(actor);
        slot.mark = mark;
        if let Some(due) = slot.actor.next_deadline() {
            self.timers.push(Reverse((due, key.to_owned())));
            slot.scheduled = Some(due);
        }
        // Pending work in a committed snapshot was published with it.
        slot.published = slot.actor.system().external_effects().map(|e| e.id.clone()).collect();
        self.actors.insert(key.to_owned(), slot);
        Ok(())
    }

    /// Handles one input record: a JSON event for the actor at `key`,
    /// starting the actor if it does not exist. Records that are not events
    /// become dead letters.
    pub fn handle(&mut self, key: Option<&str>, payload: &[u8], now: u64) {
        self.handle_as(key, payload, self.config.events, now);
    }

    /// Records a record that cannot be processed as a dead letter.
    pub fn reject(&mut self, key: Option<&str>, payload: &[u8], reason: String) {
        self.output.dead_letters.push(DeadLetter { key: key.map(str::to_owned), payload: payload.to_vec(), reason });
    }

    /// [`Partition::handle`] for a record whose event is encoded as `format`
    /// (from its `content-type` header, say).
    pub fn handle_as(&mut self, key: Option<&str>, payload: &[u8], format: EventFormat, now: u64) {
        let dead = |reason: String| DeadLetter { key: key.map(str::to_owned), payload: payload.to_vec(), reason };
        let Some(key) = key else {
            self.output.dead_letters.push(dead("a record needs a key (the actor id)".into()));
            return;
        };
        let event: Value = match format {
            EventFormat::Json => match serde_json::from_slice(payload) {
                Ok(e) => e,
                Err(e) => return self.output.dead_letters.push(dead(format!("not JSON: {e}"))),
            },
            EventFormat::MessagePack => match rmp_serde::from_slice(payload) {
                Ok(e) => e,
                Err(e) => return self.output.dead_letters.push(dead(format!("not MessagePack: {e}"))),
            },
            EventFormat::Protobuf => {
                let Some(schema) = &self.config.event_schema else {
                    return self
                        .output
                        .dead_letters
                        .push(dead("a protobuf event, but no event schema is configured".into()));
                };
                match schema.decode(payload) {
                    Ok(e) => e,
                    Err(e) => return self.output.dead_letters.push(dead(format!("not a protobuf event: {e}"))),
                }
            }
        };
        if !event.get("type").is_some_and(Value::is_string) {
            return self.output.dead_letters.push(dead("an event needs a string \"type\"".into()));
        }
        if event["type"] == EFFECT_RESULT {
            if let Err(reason) = self.resolve(key, event, now) {
                self.output.dead_letters.push(dead(reason));
            }
            return;
        }
        let mark = match self.config.dedupe.as_ref().map(|d| d.mark(&event)) {
            Some(Ok(mark)) => mark,
            Some(Err(reason)) => return self.output.dead_letters.push(dead(reason)),
            None => None,
        };
        if let Some(mark) = mark {
            let seen = match self.actors.get(key) {
                Some(slot) => slot.mark,
                None => self.retired.get(key).copied(),
            };
            if seen.is_some_and(|seen| mark <= seen) {
                self.output.duplicates += 1;
                return;
            }
        }
        let result = self.send(key, event, now);
        // Taken even when the step failed: a redelivery is still a duplicate.
        if let (Some(mark), Some(slot)) = (mark, self.actors.get_mut(key)) {
            slot.mark = Some(mark);
        }
        if let Err(reason) = result {
            self.output.dead_letters.push(dead(reason));
        }
    }

    /// Sends an event to the actor at `key` (starting it if needed).
    pub fn send(&mut self, key: &str, event: Value, now: u64) -> Result<(), String> {
        self.now = self.now.max(now);
        let now = self.now;
        // Due timers of this actor fire before the event.
        let slot = match self.actors.get_mut(key) {
            Some(slot) => slot,
            None => {
                let actor = Actor::start_with(self.machine.clone(), InitOptions { time: now, ..Default::default() })
                    .map_err(|e| format!("starting the actor failed: {e}"))?;
                let mut slot = Slot::new(actor);
                slot.mark = self.retired.remove(key);
                self.actors.entry(key.to_owned()).or_insert(slot)
            }
        };
        let result = slot.actor.send_json(event, now).map_err(|e| e.to_string());
        self.after_step(key);
        result
    }

    /// Delivers the result of external work (an [`EFFECT_RESULT`] record)
    /// to the actor at `key`. Fails for unknown actors and for work that is
    /// no longer pending (cancelled, or already resolved).
    pub fn resolve(&mut self, key: &str, result: Value, now: u64) -> Result<(), String> {
        let effect_id = result
            .get("effectId")
            .and_then(Value::as_str)
            .ok_or_else(|| format!("{EFFECT_RESULT} needs a string \"effectId\""))?
            .to_owned();
        self.now = self.now.max(now);
        let now = self.now;
        let actor =
            &mut self.actors.get_mut(key).ok_or_else(|| format!("no actor {key:?} for effect {effect_id}"))?.actor;
        if !actor.system().external_effects().any(|e| e.id == effect_id) {
            return Err(format!("effect {effect_id} of {key:?} is not pending (cancelled or already resolved)"));
        }
        // Timers due by now fire first, as for any event.
        let outcome = actor
            .advance(now)
            .and_then(|()| actor.resolve_effect(&effect_id, result.get("event").cloned()))
            .map_err(|e| e.to_string());
        self.after_step(key);
        outcome
    }

    /// Fires every timer due at or before `now`.
    pub fn tick(&mut self, now: u64) {
        self.now = self.now.max(now);
        while let Some(Reverse((due, _))) = self.timers.peek() {
            if *due > self.now {
                break;
            }
            let Reverse((due, key)) = self.timers.pop().expect("peeked");
            let Some(slot) = self.actors.get_mut(&key) else { continue };
            if slot.scheduled == Some(due) {
                slot.scheduled = None;
            }
            let actor = &mut slot.actor;
            if actor.next_deadline().map_or(true, |d| d > self.now) {
                continue; // stale: the timer was cancelled or already fired
            }
            if let Err(e) = actor.advance(self.now) {
                self.output.dead_letters.push(DeadLetter {
                    key: Some(key.clone()),
                    payload: Vec::new(),
                    reason: format!("a timer failed: {e}"),
                });
            }
            self.after_step(&key);
        }
    }

    /// When the earliest timer is due.
    pub fn next_deadline(&mut self) -> Option<u64> {
        // Drop stale entries so the answer is exact.
        while let Some(Reverse((due, key))) = self.timers.peek() {
            match self.actors.get(key).and_then(|s| s.actor.next_deadline()) {
                Some(d) if d == *due => return Some(*due),
                _ => {
                    self.timers.pop();
                }
            }
        }
        None
    }

    fn after_step(&mut self, key: &str) {
        let slot = self.actors.get_mut(key).expect("present");
        let actor = &mut slot.actor;
        for event in actor.take_emitted() {
            self.output.emitted.push(Emitted { key: key.to_owned(), event });
        }
        actor.take_logs();
        match actor.next_deadline() {
            Some(due) if slot.scheduled != Some(due) => {
                self.timers.push(Reverse((due, key.to_owned())));
                slot.scheduled = Some(due);
            }
            Some(_) => {}
            None => slot.scheduled = None,
        }
        // Publish new external work once.
        let mut pending = actor.system().external_effects().peekable();
        if pending.peek().is_some() || !slot.published.is_empty() {
            let mut current = Vec::new();
            for effect in pending {
                if !slot.published.contains(&effect.id) {
                    self.output.effects.push(EffectRequest { key: key.to_owned(), effect: effect.to_json() });
                }
                current.push(effect.id.clone());
            }
            slot.published = current;
        }
        if !slot.dirty {
            slot.dirty = true;
            self.dirty.push(key.to_owned());
        }
    }

    fn encode(config: &EngineConfig, slot: &Slot<C, E>) -> Vec<u8> {
        let mut out = Vec::with_capacity(if config.codec == Codec::Protobuf { 256 } else { 512 });
        if let Some(mark) = slot.mark.filter(|_| config.dedupe.is_some()) {
            codec::write_mark(&mut out, mark);
        }
        // Protobuf records always hold the compact form: they are not for
        // XState's own restore anyway.
        match (config.snapshots, config.codec) {
            (SnapshotFormat::Compact, _) | (_, Codec::Protobuf) => {
                config.codec.encode_into(&mut out, &slot.actor.persist_compact())
            }
            (SnapshotFormat::XState, _) => config.codec.encode_into(&mut out, &slot.actor.persist_view()),
        }
        out
    }

    fn mark_record(mark: Mark) -> Vec<u8> {
        let mut out = Vec::with_capacity(21);
        codec::write_mark(&mut out, mark);
        out
    }

    /// The changelog record of every live actor (and the mark of every
    /// evicted one), for a checkpoint. Call it right after a flush whose
    /// output was committed: the records then equal the latest committed
    /// changelog record of each key.
    pub fn snapshot_records(&self) -> Vec<(String, Vec<u8>)> {
        let mut records: Vec<(String, Vec<u8>)> =
            self.actors.iter().map(|(key, slot)| (key.clone(), Self::encode(&self.config, slot))).collect();
        records.extend(self.retired.iter().map(|(key, mark)| (key.clone(), Self::mark_record(*mark))));
        records.sort_by(|a, b| a.0.cmp(&b.0));
        records
    }

    /// Takes the output since the last flush, with the snapshots of the
    /// actors that changed.
    pub fn flush(&mut self) -> Output {
        for key in std::mem::take(&mut self.dirty) {
            let Some(slot) = self.actors.get_mut(&key) else { continue };
            slot.dirty = false;
            let finished = matches!(slot.actor.status(), Status::Done | Status::Stopped);
            if finished && self.config.evict_final {
                let mark = slot.mark.filter(|_| self.config.dedupe.is_some());
                self.actors.remove(&key);
                match mark {
                    Some(mark) => {
                        self.retired.insert(key.clone(), mark);
                        self.output.changelog.push((key, Some(Self::mark_record(mark))));
                    }
                    None => self.output.changelog.push((key, None)),
                }
            } else {
                let record = Self::encode(&self.config, slot);
                self.output.changelog.push((key, Some(record)));
            }
        }
        std::mem::take(&mut self.output)
    }
}
