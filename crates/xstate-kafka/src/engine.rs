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

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use xstate_rt::{Actor, EventType, InitOptions, Machine, Status};

use crate::codec::Codec;

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
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self { codec: Codec::default(), evict_final: true, snapshots: SnapshotFormat::default() }
    }
}

/// The shape of changelog snapshots. Restore reads either.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SnapshotFormat {
    /// XState's `SystemSnapshot` without what restore rebuilds identically
    /// (`xstate_rt::System::compact_view`): smaller and faster to write,
    /// but not for XState's own restore.
    #[default]
    Compact,
    /// XState's full `SystemSnapshot`.
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
    /// `None` (a tombstone) when it was evicted.
    pub changelog: Vec<(String, Option<Vec<u8>>)>,
    pub dead_letters: Vec<DeadLetter>,
    /// New external work, in the order it was requested.
    pub effects: Vec<EffectRequest>,
}

impl Output {
    pub fn is_empty(&self) -> bool {
        self.emitted.is_empty() && self.changelog.is_empty() && self.dead_letters.is_empty() && self.effects.is_empty()
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
    /// Ids of the actor's pending external work that was already published.
    published: HashSet<String>,
}

impl<C, E> Slot<C, E> {
    fn new(actor: Actor<C, E>) -> Self {
        Self { actor, dirty: false, scheduled: None, published: HashSet::new() }
    }
}

/// The actors of one input partition, keyed by record key.
pub struct Partition<C, E> {
    machine: Arc<Machine<C, E>>,
    config: EngineConfig,
    actors: HashMap<String, Slot<C, E>>,
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

    pub fn actor(&self, key: &str) -> Option<&Actor<C, E>> {
        self.actors.get(key).map(|s| &s.actor)
    }

    /// Applies one changelog record while restoring: a snapshot replaces the
    /// key's actor, a tombstone removes it. Produces no output.
    pub fn restore(&mut self, key: &str, record: Option<&[u8]>) -> Result<(), String> {
        let Some(record) = record else {
            self.actors.remove(key);
            return Ok(());
        };
        let snapshot = Codec::decode(record)?;
        let actor = Actor::restore(self.machine.clone(), &snapshot).map_err(|e| format!("key {key:?}: {e}"))?;
        self.now = self.now.max(actor.now());
        let mut slot = Slot::new(actor);
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
        let dead = |reason: String| DeadLetter { key: key.map(str::to_owned), payload: payload.to_vec(), reason };
        let Some(key) = key else {
            self.output.dead_letters.push(dead("a record needs a key (the actor id)".into()));
            return;
        };
        let event: Value = match serde_json::from_slice(payload) {
            Ok(e) => e,
            Err(e) => return self.output.dead_letters.push(dead(format!("not JSON: {e}"))),
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
        if let Err(reason) = self.send(key, event, now) {
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
                self.actors.entry(key.to_owned()).or_insert(Slot::new(actor))
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
            let mut current = HashSet::new();
            for effect in pending {
                if !slot.published.contains(&effect.id) {
                    self.output.effects.push(EffectRequest { key: key.to_owned(), effect: effect.to_json() });
                }
                current.insert(effect.id.clone());
            }
            slot.published = current;
        }
        if !slot.dirty {
            slot.dirty = true;
            self.dirty.push(key.to_owned());
        }
    }

    fn encode(&self, actor: &Actor<C, E>) -> Vec<u8> {
        Self::encode_with(&self.config, actor)
    }

    fn encode_with(config: &EngineConfig, actor: &Actor<C, E>) -> Vec<u8> {
        match config.snapshots {
            SnapshotFormat::Compact => config.codec.encode_serialize(&actor.persist_compact()),
            SnapshotFormat::XState => config.codec.encode_serialize(&actor.persist_view()),
        }
    }

    /// The changelog record of every live actor, for a checkpoint. Call it
    /// right after a flush whose output was committed: the records then
    /// equal the latest committed changelog record of each key.
    pub fn snapshot_records(&self) -> Vec<(String, Vec<u8>)> {
        let mut records: Vec<(String, Vec<u8>)> =
            self.actors.iter().map(|(key, slot)| (key.clone(), self.encode(&slot.actor))).collect();
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
                self.actors.remove(&key);
                self.output.changelog.push((key, None));
            } else {
                let record = Self::encode_with(&self.config, &slot.actor);
                self.output.changelog.push((key, Some(record)));
            }
        }
        std::mem::take(&mut self.output)
    }
}
