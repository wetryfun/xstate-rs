//! The deterministic part of the service: one [`Partition`] per input
//! partition, holding one actor per record key.
//!
//! A partition never touches Kafka or a clock. The driver feeds it records
//! and the current time, and takes an [`Output`] per transaction: the events
//! the actors emitted, the changelog records for the actors that changed
//! (their persisted snapshots, or tombstones), and dead letters for records
//! that could not be processed.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
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
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self { codec: Codec::default(), evict_final: true }
    }
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

/// What a partition produced since the last [`Partition::flush`].
#[derive(Debug, Default)]
pub struct Output {
    pub emitted: Vec<Emitted>,
    /// Changelog records, one per changed actor: its encoded snapshot, or
    /// `None` (a tombstone) when it was evicted.
    pub changelog: Vec<(String, Option<Vec<u8>>)>,
    pub dead_letters: Vec<DeadLetter>,
}

impl Output {
    pub fn is_empty(&self) -> bool {
        self.emitted.is_empty() && self.changelog.is_empty() && self.dead_letters.is_empty()
    }
}

/// The actors of one input partition, keyed by record key.
pub struct Partition<C, E> {
    machine: Arc<Machine<C, E>>,
    config: EngineConfig,
    actors: HashMap<String, Actor<C, E>>,
    /// Due times of actor timers. Entries can be stale; each is checked
    /// against the actor's own next deadline when it comes due.
    timers: BinaryHeap<Reverse<(u64, String)>>,
    /// Actors changed since the last flush, in order of first change.
    dirty: Vec<String>,
    dirty_set: HashSet<String>,
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
            actors: HashMap::new(),
            timers: BinaryHeap::new(),
            dirty: Vec::new(),
            dirty_set: HashSet::new(),
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
        self.actors.get(key)
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
        if let Some(due) = actor.next_deadline() {
            self.timers.push(Reverse((due, key.to_owned())));
        }
        self.actors.insert(key.to_owned(), actor);
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
        if let Err(reason) = self.send(key, event, now) {
            self.output.dead_letters.push(dead(reason));
        }
    }

    /// Sends an event to the actor at `key` (starting it if needed).
    pub fn send(&mut self, key: &str, event: Value, now: u64) -> Result<(), String> {
        self.now = self.now.max(now);
        let now = self.now;
        // Due timers of this actor fire before the event.
        if !self.actors.contains_key(key) {
            let actor = Actor::start_with(self.machine.clone(), InitOptions { time: now, ..Default::default() })
                .map_err(|e| format!("starting the actor failed: {e}"))?;
            self.actors.insert(key.to_owned(), actor);
        }
        let actor = self.actors.get_mut(key).expect("present");
        let result = actor.send_json(event, now).map_err(|e| e.to_string());
        self.after_step(key);
        result
    }

    /// Fires every timer due at or before `now`.
    pub fn tick(&mut self, now: u64) {
        self.now = self.now.max(now);
        while let Some(Reverse((due, _))) = self.timers.peek() {
            if *due > self.now {
                break;
            }
            let Reverse((_, key)) = self.timers.pop().expect("peeked");
            let Some(actor) = self.actors.get_mut(&key) else { continue };
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
            match self.actors.get(key).and_then(|a| a.next_deadline()) {
                Some(d) if d == *due => return Some(*due),
                _ => {
                    self.timers.pop();
                }
            }
        }
        None
    }

    fn after_step(&mut self, key: &str) {
        let actor = self.actors.get_mut(key).expect("present");
        for event in actor.take_emitted() {
            self.output.emitted.push(Emitted { key: key.to_owned(), event });
        }
        actor.take_logs();
        if let Some(due) = actor.next_deadline() {
            self.timers.push(Reverse((due, key.to_owned())));
        }
        if self.dirty_set.insert(key.to_owned()) {
            self.dirty.push(key.to_owned());
        }
    }

    /// Takes the output since the last flush, with the snapshots of the
    /// actors that changed.
    pub fn flush(&mut self) -> Output {
        for key in std::mem::take(&mut self.dirty) {
            self.dirty_set.remove(&key);
            let Some(actor) = self.actors.get(&key) else { continue };
            let finished = matches!(actor.status(), Status::Done | Status::Stopped);
            if finished && self.config.evict_final {
                self.actors.remove(&key);
                self.output.changelog.push((key, None));
            } else {
                let record = self.config.codec.encode_serialize(&actor.persist_view());
                self.output.changelog.push((key, Some(record)));
            }
        }
        std::mem::take(&mut self.output)
    }
}
