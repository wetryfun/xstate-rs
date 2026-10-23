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
//!   "maxBatch": 50000, "batchLingerMs": 20, "pollTimeoutMs": 100, "requestTimeoutMs": 30000,
//!   "transactionalId": "orders",
//!   "codec": "messagepack",
//!   "evictFinal": true,
//!   "snapshots": "xstate",
//!   "events": "json",
//!   "dedupe": { "sequence": "seq", "epoch": "boot", "retainMs": 2592000000 },
//!   "previousVersions": [
//!     { "machine": "order-v2.json" },
//!     { "machine": "order-v1.json", "migrate": { "states": { "order.open": "order.cart" }, "context": { "currency": "EUR" } } },
//!     { "version": "0", "migrate": { "states": {} } }
//!   ],
//!   "onRestoreError": "deadLetter",
//!   "rebalance": "cooperative", "statsIntervalMs": 5000,
//!   "outputs": "json",
//!   "schemaRegistry": { "url": "http://localhost:8081", "username": "u", "password": { "env": "SR_PASSWORD" } },
//!   "client": {
//!     "compression.type": "zstd",
//!     "security.protocol": "SASL_SSL", "sasl.mechanisms": "SCRAM-SHA-256",
//!     "sasl.username": "orders", "sasl.password": { "env": "KAFKA_PASSWORD" },
//!     "ssl.ca.location": "/etc/ssl/certs/ca-certificates.crt"
//!   },
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
//! topic (see `xstate_kafka::engine`). `dedupe` skips events an actor
//! already took, by their `sequence` (and optional `epoch`) fields (see
//! `xstate_kafka::Dedupe`).
//!
//! `previousVersions` lists earlier versions of the machine (by their
//! definitions' `version`; the current machine needs one too). Actors
//! persisted by an earlier version keep running on it (`machine` alone), or
//! are migrated to the current machine when restored (`migrate`: state ids
//! to map, a context patch; with `machine`, the migration is checked against
//! it, else `version` names it). `onRestoreError` (`"deadLetter"`, the
//! default when a dead-letter topic is set, or `"fail"`) decides what a
//! record that cannot be restored does.
//!
//! `client` passes librdkafka settings through (TLS and SASL PLAIN, SCRAM
//! and OAUTHBEARER are built in); a value can name a secret instead of
//! holding it: `{ "env": "NAME" }` or `{ "file": "/run/secrets/name" }`.
//!
//! Avro (`"events"`, `"outputs"` or `"codec": "avro"`) needs
//! `schemaRegistry` and a bundle; the generated schemas are registered at
//! start (or looked up, with `"autoRegister": false`) under the topics'
//! `<topic>-value` subjects (see `xstate_kafka::avro`).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use serde_json::Value;
use xstate_rt::step::async_logic;
use xstate_rt::{Machine, Migration, Registry};

use crate::avro::registry::HttpRegistry;
use crate::avro::AvroSchemas;
use crate::codec::Codec;
use crate::engine::{Dedupe, EngineConfig, EventFormat, MachineSet, OutputFormat, RestoreErrors, SnapshotFormat};
use crate::proto::events::EventSchema;
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

/// Where Avro schemas are registered and looked up (the Confluent REST API,
/// as Redpanda's built-in registry serves it).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SchemaRegistryConfig {
    /// Like `http://redpanda:8081`.
    pub url: String,
    /// Basic auth (strings or `{ "env" }` / `{ "file" }` secrets).
    #[serde(default)]
    pub username: Option<Value>,
    #[serde(default)]
    pub password: Option<Value>,
    /// Register the generated schemas at start (default), or only look
    /// them up (registered by a deployment pipeline).
    #[serde(default)]
    pub auto_register: Option<bool>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

/// An earlier version of the machine.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PreviousVersion {
    /// Its definition (relative to the configuration file).
    #[serde(default)]
    pub machine: Option<PathBuf>,
    /// Its version, when `machine` is not given.
    #[serde(default)]
    pub version: Option<String>,
    /// Move its actors to the current machine as they are restored.
    #[serde(default)]
    pub migrate: Option<Migration>,
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
    /// `"xstate"` (default: XState's full `SystemSnapshot`) or `"compact"`.
    /// Ignored for the `"protobuf"` codec, which always writes the compact
    /// form.
    #[serde(default)]
    pub snapshots: Option<String>,
    /// The encoding of input records without a `content-type` header:
    /// `"json"` (default), `"protobuf"` (needs a bundle with
    /// `schemas.events`) or `"messagepack"`.
    #[serde(default)]
    pub events: Option<String>,
    /// Skip input events an actor already took.
    #[serde(default)]
    pub dedupe: Option<Dedupe>,
    /// Earlier versions of the machine whose actors may be in the changelog.
    #[serde(default)]
    pub previous_versions: Vec<PreviousVersion>,
    /// `"deadLetter"` or `"fail"`.
    #[serde(default)]
    pub on_restore_error: Option<String>,
    /// How output records are encoded: `"json"` (default) or `"avro"`.
    #[serde(default)]
    pub outputs: Option<String>,
    /// The Schema Registry, for Avro events, outputs and snapshots.
    #[serde(default)]
    pub schema_registry: Option<SchemaRegistryConfig>,
    /// `"cooperative"` (default) or `"eager"`.
    #[serde(default)]
    pub rebalance: Option<String>,
    #[serde(default)]
    pub stats_interval_ms: Option<u64>,
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
        // Secret files are relative to the configuration file too.
        for v in config.client.values_mut() {
            if let Some(Value::String(file)) = v.as_object_mut().and_then(|o| o.get_mut("file")) {
                if Path::new(file.as_str()).is_relative() {
                    *file = base.join(&*file).to_string_lossy().into_owned();
                }
            }
        }
        for p in &mut config.previous_versions {
            if let Some(m) = p.machine.as_mut().filter(|m| m.is_relative()) {
                *m = base.join(&*m);
            }
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
        c.cooperative = match self.rebalance.as_deref() {
            None | Some("cooperative") => true,
            Some("eager") => false,
            Some(other) => {
                return Err(format!("unknown rebalance \"{other}\" (expected \"cooperative\" or \"eager\")"))
            }
        };
        if let Some(v) = self.stats_interval_ms {
            c.stats_interval = ms(v);
        }
        if let Some(v) = self.checkpoint_interval_ms {
            c.checkpoint_interval = ms(v);
        }
        let codec = match self.codec.as_deref() {
            None | Some("messagepack") => Codec::MessagePack,
            Some("json") => Codec::Json,
            Some("protobuf") => Codec::Protobuf,
            Some("avro") => Codec::Avro,
            Some(other) => {
                return Err(format!(
                    "unknown codec \"{other}\" (expected \"messagepack\", \"json\", \"protobuf\" or \"avro\")"
                ))
            }
        };
        let snapshots = match self.snapshots.as_deref() {
            Some("compact") => SnapshotFormat::Compact,
            None | Some("xstate") => SnapshotFormat::XState,
            Some(other) => {
                return Err(format!("unknown snapshot format \"{other}\" (expected \"xstate\" or \"compact\")"))
            }
        };
        let events = match self.events.as_deref() {
            None | Some("json") => EventFormat::Json,
            Some("protobuf") => EventFormat::Protobuf,
            Some("messagepack") => EventFormat::MessagePack,
            Some("avro") => EventFormat::Avro,
            Some(other) => {
                return Err(format!(
                    "unknown event format \"{other}\" (expected \"json\", \"protobuf\", \"messagepack\" or \"avro\")"
                ))
            }
        };
        let outputs = match self.outputs.as_deref() {
            None | Some("json") => OutputFormat::Json,
            Some("avro") => OutputFormat::Avro,
            Some(other) => return Err(format!("unknown output format \"{other}\" (expected \"json\" or \"avro\")")),
        };
        let bundle = self.read_machine_file()?;
        // A bundle with event schemas can always take protobuf records
        // (announced by their content-type header).
        let event_schema = match EventSchema::from_bundle(&bundle) {
            Ok(schema) => Some(Arc::new(schema)),
            Err(e) if events == EventFormat::Protobuf => {
                return Err(format!("{}: \"events\": \"protobuf\": {e}", self.machine.display()))
            }
            Err(_) => None,
        };
        if let Some(d) = &self.dedupe {
            if d.sequence.is_empty() || d.epoch.as_ref().is_some_and(|e| e.is_empty() || *e == d.sequence) {
                return Err("\"dedupe\" needs a \"sequence\" field name and a different \"epoch\" one".into());
            }
        }
        let restore_errors = match self.on_restore_error.as_deref() {
            Some("fail") => RestoreErrors::Fail,
            None | Some("deadLetter") if t.dead_letter.is_some() => RestoreErrors::DeadLetter,
            None => RestoreErrors::Fail,
            Some("deadLetter") => return Err("\"onRestoreError\": \"deadLetter\" needs topics.deadLetter".into()),
            Some(other) => {
                return Err(format!("unknown onRestoreError \"{other}\" (expected \"deadLetter\" or \"fail\")"))
            }
        };
        let avro = self.avro(&bundle, events, outputs, codec)?;
        c.engine = EngineConfig {
            codec,
            evict_final: self.evict_final.unwrap_or(true),
            snapshots,
            events,
            event_schema,
            dedupe: self.dedupe.clone(),
            restore_errors,
            avro,
            outputs,
        };
        for (k, v) in &self.client {
            let v = match v {
                Value::String(s) => s.clone(),
                Value::Object(o) => secret(o).map_err(|e| format!("client.{k}: {e}"))?,
                other => other.to_string(),
            };
            c.client_config.push((k.clone(), v));
        }
        Ok(c)
    }

    /// The Avro schemas, registered under `<topic>-value` subjects: the
    /// input topic's (whenever the bundle has `schemas.events`, so records
    /// can announce Avro by their content type), and the output and
    /// changelog topics' when those are Avro.
    fn avro(
        &self,
        bundle: &Value,
        events: EventFormat,
        outputs: OutputFormat,
        codec: Codec,
    ) -> Result<Option<Arc<AvroSchemas>>, String> {
        let used = events == EventFormat::Avro || outputs == OutputFormat::Avro || codec == Codec::Avro;
        let Some(r) = &self.schema_registry else {
            return if used { Err("Avro needs \"schemaRegistry\"".into()) } else { Ok(None) };
        };
        let text = |v: &Option<Value>, what: &str| -> Result<Option<String>, String> {
            match v {
                None => Ok(None),
                Some(Value::String(s)) => Ok(Some(s.clone())),
                Some(Value::Object(o)) => secret(o).map(Some).map_err(|e| format!("schemaRegistry.{what}: {e}")),
                Some(_) => Err(format!("schemaRegistry.{what}: expected a string or a secret")),
            }
        };
        let credentials = match (text(&r.username, "username")?, text(&r.password, "password")?) {
            (Some(u), Some(p)) => Some((u, p)),
            (None, None) => None,
            _ => return Err("schemaRegistry needs both username and password".into()),
        };
        let timeout = Duration::from_millis(r.timeout_ms.unwrap_or(10_000));
        let registry = Arc::new(HttpRegistry::new(&r.url, credentials, timeout));
        let mut schemas =
            AvroSchemas::from_bundle(bundle, registry).map_err(|e| format!("{}: {e}", self.machine.display()))?;
        if events == EventFormat::Avro && schemas.events.is_none() {
            return Err("\"events\": \"avro\" needs a bundle with schemas.events".into());
        }
        if outputs == OutputFormat::Avro && schemas.emitted.is_none() {
            return Err("\"outputs\": \"avro\" needs a bundle with schemas.emitted".into());
        }
        let t = &self.topics;
        let subject = |topic: &str| format!("{topic}-value");
        let (input, output, changelog) = (subject(&t.input), subject(&t.output), subject(&t.changelog));
        let subjects = [
            schemas.events.as_ref().map(|_| input.as_str()),
            (outputs == OutputFormat::Avro).then_some(output.as_str()),
            (codec == Codec::Avro).then_some(changelog.as_str()),
        ];
        schemas.register(subjects, r.auto_register.unwrap_or(true)).map_err(|e| format!("schemaRegistry: {e}"))?;
        Ok(Some(Arc::new(schemas)))
    }

    /// The machine file: a `MachineJSON` definition or a bundle.
    pub fn read_machine_file(&self) -> Result<Value, String> {
        read_json(&self.machine)
    }

    /// The machine and its earlier versions.
    pub fn machines(&self) -> Result<MachineSet<Value, Value>, String> {
        let mut set = MachineSet::new(Arc::new(self.machine()?));
        for p in &self.previous_versions {
            let earlier = p.machine.as_ref().map(|path| self.load_machine(path)).transpose()?;
            let version = match (&earlier, &p.version) {
                (Some(m), None) => m.version().map(str::to_owned),
                (None, Some(v)) => Some(v.clone()),
                (Some(m), Some(v)) if m.version() == Some(v.as_str()) => Some(v.clone()),
                (Some(_), Some(v)) => {
                    return Err(format!("previousVersions: \"{v}\" is not the version of its machine"))
                }
                (None, None) => return Err("previousVersions: an entry needs \"machine\" or \"version\"".into()),
            };
            let version = version.ok_or("previousVersions: an earlier machine needs a \"version\"")?;
            set = match (&p.migrate, earlier) {
                (Some(migration), earlier) => set.migrate(&version, migration.clone(), earlier.as_ref()),
                (None, Some(earlier)) => set.keep(Arc::new(earlier)),
                (None, None) => {
                    Err(format!("previousVersions: version \"{version}\" needs \"machine\" or \"migrate\""))
                }
            }
            .map_err(|e| format!("previousVersions: {e}"))?;
        }
        Ok(set)
    }

    /// The protobuf form of the machine's input events (the machine file
    /// must be a bundle with `schemas.events`).
    pub fn event_schema(&self) -> Result<EventSchema, String> {
        EventSchema::from_bundle(&self.read_machine_file()?).map_err(|e| format!("{}: {e}", self.machine.display()))
    }

    /// Loads the machine. The generic service has no Rust implementations,
    /// so the definition must not reference custom guards, actions or
    /// delays (build a binary of your own with `Service::start` for those).
    /// Invoked actors become async work on `topics.effects`.
    pub fn machine(&self) -> Result<Machine<Value, Value>, String> {
        self.load_machine(&self.machine)
    }

    fn load_machine(&self, path: &Path) -> Result<Machine<Value, Value>, String> {
        let at = |e: String| format!("{}: {e}", path.display());
        let json = read_json(path)?;
        let definition = match json.get("machine") {
            Some(m) if json.get("states").is_none() => m.clone(),
            _ => json,
        };
        let refs = Machine::<Value, Value>::references(&definition).map_err(|e| at(e.to_string()))?;
        let mut needed: Vec<String> = Vec::new();
        for (kind, list) in [
            ("guard", &refs.guards),
            ("action", &refs.actions),
            ("delay", &refs.delays),
            ("expression", &refs.exprs),
            ("context expression", &refs.contexts),
        ] {
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

/// A client setting kept out of the file: `{ "env": "NAME" }` (an
/// environment variable) or `{ "file": "/run/secrets/x" }` (a file's
/// contents, without a trailing newline).
fn secret(o: &serde_json::Map<String, Value>) -> Result<String, String> {
    match (o.len(), o.get("env").and_then(Value::as_str), o.get("file").and_then(Value::as_str)) {
        (1, Some(name), _) => std::env::var(name).map_err(|_| format!("environment variable {name} is not set")),
        (1, _, Some(path)) => std::fs::read_to_string(path)
            .map(|s| s.trim_end_matches(['\n', '\r']).to_owned())
            .map_err(|e| format!("{path}: {e}")),
        _ => Err("expected a string, a number, { \"env\": name } or { \"file\": path }".into()),
    }
}

fn read_json(path: &Path) -> Result<Value, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    serde_json::from_str(&text).map_err(|e| format!("{}: {e}", path.display()))
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
                 "workers": 3, "codec": "json", "batchLingerMs": 5,
                 "client": { "linger.ms": 7, "sasl.password": { "file": "secret.txt" }, "sasl.username": { "env": "HOME" } },
                 "stateDir": "state", "checkpointIntervalMs": 1000, "snapshots": "compact",
                 "dedupe": { "sequence": "seq" } }"#,
        );
        write(&dir, "secret.txt", "s3cret\n");
        let config = FileConfig::load(&path).unwrap();
        let service = config.service().unwrap();
        assert_eq!(service.workers, 3);
        assert_eq!(service.engine.codec, Codec::Json);
        assert_eq!(service.engine.snapshots, SnapshotFormat::Compact);
        assert_eq!(service.engine.dedupe, Some(Dedupe { sequence: "seq".into(), epoch: None, retain_ms: None }));
        assert_eq!(service.batch_linger, Duration::from_millis(5));
        assert_eq!(service.state_dir, Some(dir.join("state")));
        assert_eq!(service.checkpoint_interval, Duration::from_secs(1));
        assert_eq!(
            service.client_config,
            vec![
                ("linger.ms".to_owned(), "7".to_owned()),
                ("sasl.password".to_owned(), "s3cret".to_owned()),
                ("sasl.username".to_owned(), std::env::var("HOME").unwrap())
            ]
        );
        assert!(config.machine().is_ok());

        write(
            &dir,
            "m.json",
            r#"{ "id": "m", "initial": "a", "states": { "a": { "entry": [{ "type": "track" }] } } }"#,
        );
        let err = config.machine().err().expect("rejected");
        assert!(err.contains("action \"track\""), "{err}");
        // Earlier versions: kept, migrated (checked against the earlier
        // definition), and a dead-letter topic for unrestorable records.
        let v = |version: &str, state: &str| {
            format!(r#"{{ "id": "m", "version": "{version}", "initial": "{state}", "states": {{ "{state}": {{}} }} }}"#)
        };
        write(&dir, "m3.json", &v("3", "c"));
        write(&dir, "m2.json", &v("2", "c"));
        write(&dir, "m1.json", &v("1", "a"));
        let base = r#""bootstrapServers": "b", "groupId": "g", "topics": { "input": "i", "output": "o", "changelog": "c", "deadLetter": "d" }"#;
        let versions = |previous: &str| {
            let path = write(
                &dir,
                "v.json",
                &format!(r#"{{ "machine": "m3.json", {base}, "previousVersions": {previous} }}"#),
            );
            FileConfig::load(&path).unwrap()
        };
        let ok = versions(
            r#"[{ "machine": "m2.json" }, { "machine": "m1.json", "migrate": { "states": { "m.a": "m.c" } } }]"#,
        );
        assert!(ok.machines().is_ok());
        assert_eq!(ok.service().unwrap().engine.restore_errors, RestoreErrors::DeadLetter);
        let unmapped = versions(r#"[{ "machine": "m1.json", "migrate": {} }]"#).machines().err().unwrap();
        assert!(unmapped.contains("need a mapping: m.a"), "{unmapped}");
        let twice =
            versions(r#"[{ "machine": "m2.json" }, { "version": "2", "migrate": {} }]"#).machines().err().unwrap();
        assert!(twice.contains("listed twice"), "{twice}");
        assert!(versions(r#"[{ "version": "0" }]"#).machines().is_err());
        assert_eq!(service.engine.restore_errors, RestoreErrors::Fail, "no dead-letter topic");

        let bad = write(
            &dir,
            "bad.json",
            r#"{ "machine": "m.json", "bootstrapServers": "b", "groupId": "g", "topics": { "input": "i", "output": "o", "changelog": "c" }, "typo": 1 }"#,
        );
        assert!(FileConfig::load(&bad).unwrap_err().contains("typo"));
        std::fs::remove_dir_all(&dir).ok();
    }
}
