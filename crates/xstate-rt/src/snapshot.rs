//! Persisted snapshots. States are recorded by their stable ids, never by
//! arena index, so a snapshot survives recompiling the same definition.

use std::collections::BTreeMap;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::error::RestoreError;
use crate::event::{Ev, EventType};
use crate::interp::{State, Status, Timer, TimerKey};
use crate::machine::{Machine, NodeId, NodeKind, ROOT};

/// Everything needed to resume an actor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistedSnapshot<C> {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    pub status: Status,
    /// XState's nested state value, for display. Restoring reads
    /// `configuration`, not this.
    pub value: Value,
    /// Ids of all active states, in document order.
    pub configuration: Vec<String>,
    pub context: C,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    /// Recorded history, by history state id.
    #[serde(default)]
    pub history_value: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    pub timers: Vec<PersistedTimer>,
    #[serde(default)]
    pub next_timer_id: u64,
    #[serde(default)]
    pub timer_order: u64,
    /// The actor's clock when the snapshot was taken.
    #[serde(default)]
    pub now: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PersistedTimer {
    pub key: PersistedTimerKey,
    pub due: u64,
    pub order: u64,
    pub delay: u64,
    pub event: Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
pub enum PersistedTimerKey {
    /// A delayed raise with an `id`.
    Id { id: String },
    /// A delayed raise without an `id`.
    Seq { seq: u64 },
    /// An `after` timer of a state, by the `after` key as written.
    #[serde(rename_all = "camelCase")]
    After { state_id: String, delay: String },
}

fn fail<T>(message: impl Into<String>) -> Result<T, RestoreError> {
    Err(RestoreError(message.into()))
}

impl<C, E> Machine<C, E>
where
    C: Serialize + DeserializeOwned + Clone,
    E: EventType,
{
    pub(crate) fn persist(&self, st: &State<C, E>) -> PersistedSnapshot<C> {
        let configuration = (0..self.nodes.len()).filter(|&i| st.active[i]).map(|i| self.nodes[i].id.clone()).collect();
        let history_value = st
            .history
            .iter()
            .enumerate()
            .filter_map(|(h, recorded)| {
                let recorded = recorded.as_ref()?;
                Some((self.nodes[h].id.clone(), recorded.iter().map(|&n| self.node(n).id.clone()).collect()))
            })
            .collect();
        let timers = st
            .timers
            .iter()
            .map(|t| PersistedTimer {
                key: match &t.key {
                    TimerKey::Id(id) => PersistedTimerKey::Id { id: id.clone() },
                    TimerKey::Seq(seq) => PersistedTimerKey::Seq { seq: *seq },
                    TimerKey::After { node, index } => PersistedTimerKey::After {
                        state_id: self.node(*node).id.clone(),
                        delay: self.node(*node).after[*index as usize].key.clone(),
                    },
                },
                due: t.due,
                order: t.order,
                delay: t.delay,
                event: t.event.to_json(),
            })
            .collect();
        PersistedSnapshot {
            machine_id: self.id.clone(),
            version: self.version.clone(),
            status: st.status,
            value: self.state_value(&st.active),
            configuration,
            context: st.context.clone(),
            output: st.output.clone(),
            history_value,
            timers,
            next_timer_id: st.next_timer_id,
            timer_order: st.timer_order,
            now: st.now,
        }
    }

    fn lookup(&self, id: &str, what: &str) -> Result<NodeId, RestoreError> {
        match self.node_by_id(id) {
            Some(n) => Ok(n),
            None => fail(format!("{what} \"{id}\" does not exist in this machine definition")),
        }
    }

    pub(crate) fn restore(&self, snapshot: PersistedSnapshot<C>) -> Result<State<C, E>, RestoreError> {
        if snapshot.machine_id != self.id {
            return fail(format!("snapshot is for machine {:?}, not {:?}", snapshot.machine_id, self.id));
        }
        if snapshot.version.is_some() && self.version.is_some() && snapshot.version != self.version {
            return fail(format!("snapshot is for version {:?}, not {:?}", snapshot.version, self.version));
        }
        let mut st = State::new(self.nodes.len(), snapshot.context, snapshot.now);
        st.status = snapshot.status;
        st.output = snapshot.output;
        st.next_timer_id = snapshot.next_timer_id;
        st.timer_order = snapshot.timer_order;
        for id in &snapshot.configuration {
            let n = self.lookup(id, "state")?;
            st.active[n as usize] = true;
        }
        self.check_configuration(&st.active)?;
        for (id, recorded) in snapshot.history_value {
            let h = self.lookup(&id, "history state")?;
            if !matches!(self.node(h).kind, NodeKind::History { .. }) {
                return fail(format!("\"{id}\" is not a history state"));
            }
            let nodes = recorded.iter().map(|r| self.lookup(r, "state")).collect::<Result<Vec<_>, _>>()?;
            st.history[h as usize] = Some(nodes);
        }
        for t in snapshot.timers {
            let key = match t.key {
                PersistedTimerKey::Id { id } => TimerKey::Id(id),
                PersistedTimerKey::Seq { seq } => TimerKey::Seq(seq),
                PersistedTimerKey::After { state_id, delay } => {
                    let node = self.lookup(&state_id, "state")?;
                    let index = match self.node(node).after.iter().position(|a| a.key == delay) {
                        Some(i) => i as u32,
                        None => return fail(format!("state \"{state_id}\" has no `after` entry \"{delay}\"")),
                    };
                    TimerKey::After { node, index }
                }
            };
            let event = match Ev::from_json(t.event) {
                Some(e) => e,
                None => return fail("a timer event has no string \"type\""),
            };
            st.timers.push(Timer { key, due: t.due, order: t.order, delay: t.delay, event });
        }
        Ok(st)
    }

    /// Checks that the active set is a legal configuration.
    fn check_configuration(&self, active: &[bool]) -> Result<(), RestoreError> {
        if !active[ROOT as usize] {
            return fail("the root state is not active");
        }
        for (i, node) in self.nodes.iter().enumerate() {
            if !active[i] {
                continue;
            }
            if let Some(p) = node.parent {
                if !active[p as usize] {
                    return fail(format!("state \"{}\" is active but its parent is not", node.id));
                }
            }
            let active_children = node.children.iter().filter(|&&c| active[c as usize]).count();
            match node.kind {
                NodeKind::Compound if active_children != 1 => {
                    return fail(format!("compound state \"{}\" must have exactly one active child", node.id))
                }
                NodeKind::Parallel if active_children != node.children.len() => {
                    return fail(format!("parallel state \"{}\" must have all regions active", node.id))
                }
                NodeKind::History { .. } => return fail(format!("history state \"{}\" cannot be active", node.id)),
                _ => {}
            }
        }
        Ok(())
    }
}
