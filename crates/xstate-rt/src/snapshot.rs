//! Persisted snapshots. States are recorded by their stable ids, never by
//! arena index, so a snapshot survives recompiling the same definition.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::core::{Kind, Nid};
use crate::error::RestoreError;
use crate::event::{Ev, EventType};
use crate::interp::{State, Status, Timer, TimerKey};
use crate::machine::{Machine, ROOT};

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
    /// Ids of all active states, in XState's configuration order (which
    /// state values and selection order follow).
    pub configuration: Vec<String>,
    pub context: C,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<Value>,
    /// Recorded history: history state id to state ids, in key order.
    #[serde(default)]
    pub history_value: Map<String, Value>,
    #[serde(default)]
    pub timers: Vec<PersistedTimer>,
    /// XState's `_nextTimerId`.
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
    pub delay: u64,
    pub event: Value,
    pub due: u64,
    pub order: u64,
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
    fn ids(&self, nodes: &[Nid]) -> Vec<String> {
        nodes.iter().map(|&n| self.data(n).id.clone()).collect()
    }

    pub(crate) fn persist(&self, st: &State<C, E>) -> PersistedSnapshot<C> {
        let history_value = st
            .history
            .iter()
            .map(|(h, nodes)| {
                (self.data(*h).id.clone(), Value::Array(self.ids(nodes).into_iter().map(Value::String).collect()))
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
                        state_id: self.data(*node).id.clone(),
                        delay: self.data(*node).after[*index as usize].key.clone(),
                    },
                },
                delay: t.delay,
                event: t.event.to_json(),
                due: t.due,
                order: t.order,
            })
            .collect();
        PersistedSnapshot {
            machine_id: self.id.clone(),
            version: self.version.clone(),
            status: st.status,
            value: self.state_value(&st.nodes),
            configuration: self.ids(&st.nodes),
            context: st.context.clone(),
            output: st.output.clone(),
            error: st.error.clone(),
            history_value,
            timers,
            next_timer_id: st.next_timer_id,
            timer_order: st.timer_order,
            now: st.now,
        }
    }

    fn lookup(&self, id: &str, what: &str) -> Result<Nid, RestoreError> {
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
        let mut st = State::new(snapshot.context, snapshot.now);
        st.status = snapshot.status;
        st.output = snapshot.output;
        st.error = snapshot.error;
        st.next_timer_id = snapshot.next_timer_id;
        st.timer_order = snapshot.timer_order;
        st.nodes = snapshot.configuration.iter().map(|id| self.lookup(id, "state")).collect::<Result<_, _>>()?;
        self.check_configuration(&st.nodes)?;
        for (id, recorded) in snapshot.history_value {
            let h = self.lookup(&id, "history state")?;
            if !matches!(self.tree.kind(h), Kind::History { .. }) {
                return fail(format!("\"{id}\" is not a history state"));
            }
            let Value::Array(items) = recorded else { return fail(format!("history of \"{id}\" is not an array")) };
            let nodes = items
                .iter()
                .map(|v| match v.as_str() {
                    Some(s) => self.lookup(s, "state"),
                    None => fail("history entries must be state ids"),
                })
                .collect::<Result<Vec<_>, _>>()?;
            st.history.push((h, nodes));
        }
        for t in snapshot.timers {
            let key = match t.key {
                PersistedTimerKey::Id { id } => TimerKey::Id(id),
                PersistedTimerKey::Seq { seq } => TimerKey::Seq(seq),
                PersistedTimerKey::After { state_id, delay } => {
                    let node = self.lookup(&state_id, "state")?;
                    let index = match self.data(node).after.iter().position(|a| a.key == delay) {
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
            st.timers.push(Timer { key, delay: t.delay, event, due: t.due, order: t.order });
        }
        Ok(st)
    }

    /// Checks that the active set is a legal configuration.
    fn check_configuration(&self, nodes: &[Nid]) -> Result<(), RestoreError> {
        let mut active = vec![false; self.nodes.len()];
        for &n in nodes {
            if active[n as usize] {
                return fail(format!("state \"{}\" appears twice", self.data(n).id));
            }
            active[n as usize] = true;
        }
        if !active[ROOT as usize] {
            return fail("the root state is not active");
        }
        for &n in nodes {
            let node = self.tree.node(n);
            let id = &self.data(n).id;
            if let Some(p) = node.parent {
                if !active[p as usize] {
                    return fail(format!("state \"{id}\" is active but its parent is not"));
                }
            }
            let active_children = node.children.iter().filter(|&&c| active[c as usize]).count();
            match node.kind {
                Kind::Compound if active_children != 1 => {
                    return fail(format!("compound state \"{id}\" must have exactly one active child"))
                }
                Kind::Parallel if active_children != node.children.len() => {
                    return fail(format!("parallel state \"{id}\" must have all regions active"))
                }
                Kind::History { .. } => return fail(format!("history state \"{id}\" cannot be active")),
                _ => {}
            }
        }
        Ok(())
    }
}
