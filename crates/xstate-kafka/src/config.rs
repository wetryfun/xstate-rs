//! The service's configuration file (JSON) and machine loading for the
//! `xstate-kafka` binary.
//!
//! ```json
//! {
//!   "machine": "order.json",
//!   "bootstrapServers": "localhost:9092",
//!   "groupId": "orders",
//!   "topics": { "input": "orders", "output": "orders-out", "changelog": "orders-changelog", "deadLetter": "orders-dead", "effects": "orders-effects" },
//!   "workers": 4,
//!   "rack": "rack-a",
//!   "maxBatch": 10000, "batchLingerMs": 20, "pollTimeoutMs": 100, "requestTimeoutMs": 30000,
//!   "transactionalId": "orders",
//!   "codec": "messagepack",
//!   "evictFinal": true,
//!   "client": { "compression.type": "lz4" },
//!   "stateDir": "/var/lib/xstate", "checkpointIntervalMs": 60000,
//!   "metricsAddress": "0.0.0.0:9464",
//!   "logIntervalMs": 10000
//! }
//! ```
//!
//! `machine` is a path (relative to the configuration file) to a
//! `MachineJSON` file or an extract bundle (`{ "machine", "schemas" }`);
//! a relative `stateDir` is relative to the configuration file too. Every
//! actor the machine invokes (`invoke.src`) runs as async work published to
//! `topics.effects` (required then), completed by results on the input
//! topic (see `xstate_kafka::engine`).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use serde_json::Value;
use xstate_rt::step::async_logic;
use xstate_rt::{Machine, Registry};

use crate::codec::Codec;
use crate::engine::EngineConfig;
use crate::service::ServiceConfig;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Topics {
    pub input: String,
    pub output: String,
    pub changelog: String,
    #[serde(default)]
    pub dead_letter: Option<String>,
    #[serde(default)]
    pub effects: Option<String>,
}

/// The configuration file.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FileConfig {
    pub machine: PathBuf,
    pub bootstrap_servers: String,
    pub group_id: String,
    pub topics: Topics,
    #[serde(default)]
    pub transactional_id: Option<String>,
    #[serde(default)]
    pub workers: Option<usize>,
    #[serde(default)]
    pub rack: Option<String>,
    #[serde(default)]
    pub max_batch: Option<usize>,
    #[serde(default)]
    pub batch_linger_ms: Option<u64>,
    #[serde(default)]
    pub poll_timeout_ms: Option<u64>,
    #[serde(default)]
    pub request_timeout_ms: Option<u64>,
    #[serde(default)]
    pub codec: Option<String>,
    #[serde(default)]
    pub evict_final: Option<bool>,
    #[serde(default)]
    pub client: serde_json::Map<String, Value>,
    #[serde(default)]
    pub state_dir: Option<PathBuf>,
    #[serde(default)]
    pub checkpoint_interval_ms: Option<u64>,
    #[serde(default)]
    pub metrics_address: Option<String>,
    #[serde(default)]
    pub log_interval_ms: Option<u64>,
}

impl FileConfig {
    /// Reads a configuration file; `machine` becomes relative to its directory.
    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
        let mut config: FileConfig = serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))?;
        let base = path.parent().unwrap_or(Path::new("."));
        if config.machine.is_relative() {
            config.machine = base.join(&config.machine);
        }
        if let Some(dir) = config.state_dir.as_mut().filter(|d| d.is_relative()) {
            *dir = base.join(&*dir);
        }
        Ok(config)
    }

    /// The service settings.
    pub fn service(&self) -> Result<ServiceConfig, String> {
        let t = &self.topics;
        let mut c = ServiceConfig::new(&self.bootstrap_servers, &self.group_id, &t.input, &t.output, &t.changelog);
        c.dead_letter_topic = t.dead_letter.clone();
        c.effects_topic = t.effects.clone();
        if let Some(id) = &self.transactional_id {
            c.transactional_id = id.clone();
        }
        if let Some(w) = self.workers {
            c.workers = w.max(1);
        }
        c.rack = self.rack.clone();
        if let Some(n) = self.max_batch {
            c.max_batch = n.max(1);
        }
        let ms = Duration::from_millis;
        if let Some(v) = self.batch_linger_ms {
            c.batch_linger = ms(v);
        }
        if let Some(v) = self.poll_timeout_ms {
            c.poll_timeout = ms(v);
        }
        if let Some(v) = self.request_timeout_ms {
            c.request_timeout = ms(v);
        }
        c.state_dir = self.state_dir.clone();
        if let Some(v) = self.checkpoint_interval_ms {
            c.checkpoint_interval = ms(v);
        }
        let codec = match self.codec.as_deref() {
            None | Some("messagepack") => Codec::MessagePack,
            Some("json") => Codec::Json,
            Some(other) => return Err(format!("unknown codec \"{other}\" (expected \"messagepack\" or \"json\")")),
        };
        c.engine = EngineConfig { codec, evict_final: self.evict_final.unwrap_or(true) };
        for (k, v) in &self.client {
            let v = match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            c.client_config.push((k.clone(), v));
        }
        Ok(c)
    }

    /// Loads the machine. The generic service has no Rust implementations,
    /// so the definition must not reference custom guards, actions or
    /// delays (build a binary of your own with `Service::start` for those).
    /// Invoked actors become async work on `topics.effects`.
    pub fn machine(&self) -> Result<Machine<Value, Value>, String> {
        let path = &self.machine;
        let at = |e: String| format!("{}: {e}", path.display());
        let text = std::fs::read_to_string(path).map_err(|e| at(e.to_string()))?;
        let json: Value = serde_json::from_str(&text).map_err(|e| at(e.to_string()))?;
        let definition = match json.get("machine") {
            Some(m) if json.get("states").is_none() => m.clone(),
            _ => json,
        };
        let refs = Machine::<Value, Value>::references(&definition).map_err(|e| at(e.to_string()))?;
        let mut needed: Vec<String> = Vec::new();
        for (kind, list) in [("guard", &refs.guards), ("action", &refs.actions), ("delay", &refs.delays)] {
            for r in list {
                needed.push(format!("{kind} \"{}\" at {}", r.name, r.path));
            }
        }
        if !needed.is_empty() {
            return Err(at(format!(
                "the machine needs Rust implementations ({}); run it with xstate_kafka::service::Service::start in a binary of your own",
                needed.join(", ")
            )));
        }
        let mut registry = Registry::new();
        if let Some(r) = refs.actors.first() {
            if self.topics.effects.is_none() {
                return Err(at(format!(
                    "the machine invokes \"{}\" at {}; set topics.effects so its work can be done outside",
                    r.name, r.path
                )));
            }
        }
        for r in &refs.actors {
            let logic = async_logic(Some(r.name.clone()), None).map_err(|e| at(e.to_string()))?;
            registry.actor(r.name.clone(), Arc::new(logic));
        }
        Machine::from_json(&definition, &registry).map_err(|e| at(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(dir: &Path, name: &str, text: &str) -> PathBuf {
        let p = dir.join(name);
        std::fs::write(&p, text).unwrap();
        p
    }

    #[test]
    fn loads_config_and_machine() {
        let dir = std::env::temp_dir().join(format!("xstate-config-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        write(&dir, "m.json", r#"{ "machine": { "id": "m", "initial": "a", "states": { "a": {} } }, "schemas": {} }"#);
        let path = write(
            &dir,
            "c.json",
            r#"{ "machine": "m.json", "bootstrapServers": "b:9092", "groupId": "g",
                 "topics": { "input": "i", "output": "o", "changelog": "c" },
                 "workers": 3, "codec": "json", "batchLingerMs": 5, "client": { "linger.ms": 7 },
                 "stateDir": "state", "checkpointIntervalMs": 1000 }"#,
        );
        let config = FileConfig::load(&path).unwrap();
        let service = config.service().unwrap();
        assert_eq!(service.workers, 3);
        assert_eq!(service.engine.codec, Codec::Json);
        assert_eq!(service.batch_linger, Duration::from_millis(5));
        assert_eq!(service.state_dir, Some(dir.join("state")));
        assert_eq!(service.checkpoint_interval, Duration::from_secs(1));
        assert_eq!(service.client_config, vec![("linger.ms".to_owned(), "7".to_owned())]);
        assert!(config.machine().is_ok());

        write(
            &dir,
            "m.json",
            r#"{ "id": "m", "initial": "a", "states": { "a": { "entry": [{ "type": "track" }] } } }"#,
        );
        let err = config.machine().err().expect("rejected");
        assert!(err.contains("action \"track\""), "{err}");
        let bad = write(
            &dir,
            "bad.json",
            r#"{ "machine": "m.json", "bootstrapServers": "b", "groupId": "g", "topics": { "input": "i", "output": "o", "changelog": "c" }, "typo": 1 }"#,
        );
        assert!(FileConfig::load(&bad).unwrap_err().contains("typo"));
        std::fs::remove_dir_all(&dir).ok();
    }
}
