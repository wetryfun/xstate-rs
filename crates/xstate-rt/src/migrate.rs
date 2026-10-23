//! Moving persisted actors from an earlier version of a machine onto the
//! current one.
//!
//! A [`Migration`] rewrites a persisted system snapshot (XState's
//! `SystemSnapshot`, as [`crate::System::to_json`] writes it) before it is
//! restored: the root actor's state ids (its configuration, recorded
//! history and the events of its pending timers, in the machine snapshot and
//! in the system's timers), a shallow patch merged into its context, and its
//! `logicVersion`. The configuration is then completed for the new
//! definition (ancestors of the mapped states, and the initial states of
//! compound states left without an active child, all regions of parallel
//! ones) and put in document order, as a fresh entry would order it. Like
//! any restore, it runs no actions.

use std::collections::BTreeSet;

use indexmap::IndexMap;
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::core::{Kind, Nid};
use crate::machine::Machine;

/// How snapshots of one earlier machine version become snapshots of the
/// current one.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Migration {
    /// Earlier state id to current state id (`#` optional on both sides).
    /// Ids that are not listed are kept.
    #[serde(default)]
    pub states: IndexMap<String, String>,
    /// Merged into the root actor's context (an object), key by key.
    #[serde(default)]
    pub context: Map<String, Value>,
}

fn bare(id: &str) -> &str {
    id.strip_prefix('#').unwrap_or(id)
}

impl Migration {
    fn map<'a>(&'a self, id: &'a str) -> &'a str {
        self.states.get(id).or_else(|| self.states.get(&format!("#{id}"))).map_or(id, |to| bare(to))
    }

    /// Checks the migration against the current machine `to` (every target
    /// exists) and, when the earlier definition is known, against it: every
    /// mapped id exists there, and every earlier state that the current
    /// machine lacks is mapped.
    pub fn check<C, E, C2, E2>(&self, from: Option<&Machine<C, E>>, to: &Machine<C2, E2>) -> Result<(), String> {
        for (old, new) in &self.states {
            if to.node_by_id(bare(new)).is_none() {
                return Err(format!(
                    "migration target \"{new}\" (for \"{old}\") is not a state of the current machine"
                ));
            }
            if let Some(from) = from {
                if from.node_by_id(bare(old)).is_none() {
                    return Err(format!("migrated state \"{old}\" is not a state of the earlier machine"));
                }
            }
        }
        if let Some(from) = from {
            let unmapped: Vec<&str> = from
                .state_ids()
                .filter(|id| to.node_by_id(id).is_none() && self.map(id) == *id)
                .filter(|id| !matches!(from.tree.kind(from.node_by_id(id).expect("own id")), Kind::History { .. }))
                .collect();
            if !unmapped.is_empty() {
                return Err(format!(
                    "states of the earlier machine missing from the current one need a mapping: {}",
                    unmapped.join(", ")
                ));
            }
        }
        Ok(())
    }

    /// Rewrites `snapshot` (a whole system snapshot) for the machine `to`.
    pub fn apply<C, E>(&self, snapshot: &mut Value, to: &Machine<C, E>) -> Result<(), String> {
        let root = snapshot.get("root").and_then(Value::as_str).ok_or("the snapshot has no root")?.to_owned();
        let actor = snapshot
            .get_mut("actors")
            .and_then(|a| a.get_mut(&root))
            .and_then(Value::as_object_mut)
            .ok_or("the snapshot has no root actor")?;
        match to.version() {
            Some(v) => actor.insert("logicVersion".into(), Value::String(v.to_owned())),
            None => actor.remove("logicVersion"),
        };
        let data = actor.get_mut("snapshot").and_then(Value::as_object_mut).ok_or("the root actor has no snapshot")?;

        if let Some(ids) = data.get("nodeIds").and_then(Value::as_array) {
            let mut nodes = BTreeSet::new();
            for id in ids {
                let id = self.map(id.as_str().ok_or("a state id is not a string")?);
                let n =
                    to.node_by_id(id).ok_or_else(|| format!("state \"{id}\" does not exist in the current machine"))?;
                nodes.insert(n);
            }
            let nodes = complete(to, nodes);
            data.insert("nodeIds".into(), nodes.iter().map(|&n| Value::String(to.data(n).id.clone())).collect());
            // Restore derives the value from the configuration.
            data.remove("value");
        }
        if let Some(Value::Object(history)) = data.get_mut("historyValue") {
            // Recorded history the current machine cannot hold is dropped:
            // the history state then enters its default target.
            let old = std::mem::take(history);
            for (h, mut recorded) in old {
                let h = self.map(&h);
                let mut known = to.node_by_id(h).is_some();
                for r in recorded.as_array_mut().into_iter().flatten() {
                    if let Some(Value::String(id)) = r.get_mut("$stateNode") {
                        *id = self.map(id).to_owned();
                        known &= to.node_by_id(id).is_some();
                    }
                }
                if known {
                    history.insert(h.to_owned(), recorded);
                }
            }
        }
        for t in data.get_mut("timers").and_then(Value::as_array_mut).into_iter().flatten() {
            self.map_event(t.get_mut("event"));
        }
        if !self.context.is_empty() {
            match data.get_mut("context") {
                Some(Value::Object(context)) => {
                    for (k, v) in &self.context {
                        context.insert(k.clone(), v.clone());
                    }
                }
                _ => return Err("a context patch needs an object context".into()),
            }
        }
        // The system's own timers of the root carry the same events.
        for t in snapshot.get_mut("timers").and_then(Value::as_array_mut).into_iter().flatten() {
            if t.get("source").and_then(|s| s.get("$actor")).and_then(Value::as_str) == Some(&root) {
                if let Some(event) = t.get_mut("event") {
                    self.map_event(event.get_mut("event"));
                }
            }
        }
        Ok(())
    }

    /// Maps the `stateId` of a lifecycle timer event.
    fn map_event(&self, event: Option<&mut Value>) {
        if let Some(Value::String(id)) = event.and_then(|e| e.get_mut("stateId")) {
            *id = self.map(id).to_owned();
        }
    }
}

/// `nodes` with their ancestors and default descendants (initial states,
/// all regions), in document order.
fn complete<C, E>(m: &Machine<C, E>, mut nodes: BTreeSet<Nid>) -> BTreeSet<Nid> {
    for n in nodes.clone() {
        let mut p = m.tree.node(n).parent;
        while let Some(a) = p {
            nodes.insert(a);
            p = m.tree.node(a).parent;
        }
    }
    let mut pending: Vec<Nid> = nodes.iter().copied().collect();
    while let Some(n) = pending.pop() {
        let children = &m.tree.node(n).children;
        let defaults: Vec<Nid> = match m.tree.kind(n) {
            Kind::Compound if !children.iter().any(|c| nodes.contains(c)) => m.data(n).initial.to_vec(),
            Kind::Parallel => children.iter().copied().filter(|c| !nodes.contains(c)).collect(),
            _ => Vec::new(),
        };
        for d in defaults {
            if nodes.insert(d) {
                pending.push(d);
            }
        }
    }
    nodes
}
