//! Protobuf snapshots: every snapshot XState recorded in the system fixtures
//! and the fuzz corpus round-trips exactly, and real actors restore from
//! protobuf records.

use std::path::Path;
use std::sync::Arc;

use serde_json::{json, Value};
use xstate_kafka::{Codec, EngineConfig, Partition};
use xstate_rt::{Machine, Registry};

fn snapshots_in(records: &Value, out: &mut Vec<Value>) {
    for r in records.as_array().into_iter().flatten() {
        if let Some(s) = r.get("snapshot").filter(|s| s.is_object()) {
            out.push(s.clone());
        }
    }
}

#[test]
fn every_recorded_xstate_snapshot_round_trips() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../fixtures");
    let mut snapshots = Vec::new();
    for entry in std::fs::read_dir(root.join("system")).unwrap() {
        let path = entry.unwrap().path();
        if path.to_string_lossy().ends_with(".expected.json") {
            let records: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
            snapshots_in(&records, &mut snapshots);
        }
    }
    let corpus: Value =
        serde_json::from_str(&std::fs::read_to_string(root.join("fuzz/system-corpus.json")).unwrap()).unwrap();
    for case in corpus.as_array().unwrap() {
        snapshots_in(&case["expected"], &mut snapshots);
    }
    assert!(snapshots.len() > 300, "{} snapshots", snapshots.len());
    let (mut json_bytes, mut proto_bytes) = (0, 0);
    for s in &snapshots {
        let record = Codec::Protobuf.encode(s);
        let back = Codec::decode(&record).unwrap();
        assert_eq!(&back, s, "{s}");
        json_bytes += serde_json::to_vec(s).unwrap().len();
        proto_bytes += record.len();
    }
    eprintln!("{} snapshots: {json_bytes} bytes as JSON, {proto_bytes} as protobuf", snapshots.len());
}

#[test]
fn actors_restore_from_protobuf_records() {
    let definition = json!({
        "id": "order", "initial": "open", "context": { "items": 0, "tags": ["web"] },
        "states": {
            "open": { "on": { "add": { "context": { "items": 1 } }, "pay": { "target": "paying" } }, "after": { "60000": { "target": "expired" } } },
            "paying": { "after": { "100": { "target": "paid" } } },
            "paid": { "type": "final" }, "expired": { "type": "final" }
        }
    });
    let m: Arc<Machine<Value, Value>> = Arc::new(Machine::from_json(&definition, &Registry::new()).unwrap());
    let config = EngineConfig { codec: Codec::Protobuf, ..Default::default() };
    let mut p = Partition::new(m.clone(), config.clone());
    p.handle(Some("a"), br#"{"type":"add"}"#, 10);
    p.handle(Some("b"), br#"{"type":"pay"}"#, 20);
    let log = p.flush().changelog;
    assert!(log.iter().all(|(_, r)| r.as_ref().unwrap()[0] == 3), "protobuf records");
    let mut q = Partition::new(m, config);
    for (key, record) in &log {
        q.restore(key, record.as_deref()).unwrap();
    }
    for key in ["a", "b"] {
        assert_eq!(q.actor(key).unwrap().persist(), p.actor(key).unwrap().persist(), "{key}");
    }
    // Timers survive: b pays after 100 ms.
    q.tick(120);
    assert_eq!(q.flush().changelog, vec![("b".to_owned(), None)]);
}

/// The `.proto` files, written as prost structs: an independent protobuf
/// implementation must read what the codec writes.
mod independent {
    use prost::Message;

    #[derive(Clone, PartialEq, Message)]
    pub struct SystemSnapshot {
        #[prost(string, optional, tag = "1")]
        pub root: Option<String>,
        #[prost(uint64, optional, tag = "3")]
        pub now: Option<u64>,
        #[prost(message, repeated, tag = "4")]
        pub actors: Vec<ActorEntry>,
        #[prost(message, repeated, tag = "7")]
        pub timers: Vec<SystemTimer>,
        #[prost(message, optional, tag = "9")]
        pub counters: Option<Counters>,
        #[prost(message, repeated, tag = "100")]
        pub extra: Vec<Field>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct ActorEntry {
        #[prost(string, tag = "1")]
        pub key: String,
        #[prost(string, optional, tag = "2")]
        pub id: Option<String>,
        #[prost(uint64, optional, tag = "4")]
        pub incarnation: Option<u64>,
        #[prost(string, optional, tag = "5")]
        pub src: Option<String>,
        #[prost(bool, optional, tag = "10")]
        pub started: Option<bool>,
        #[prost(message, optional, tag = "12")]
        pub snapshot: Option<ActorSnapshot>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct ActorSnapshot {
        #[prost(int32, optional, tag = "1")]
        pub status: Option<i32>,
        #[prost(message, optional, tag = "4")]
        pub context: Option<Value>,
        #[prost(message, repeated, tag = "7")]
        pub timers: Vec<LogicalTimer>,
        #[prost(string, repeated, tag = "11")]
        pub node_ids: Vec<String>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct LogicalTimer {
        #[prost(uint64, optional, tag = "3")]
        pub delay: Option<u64>,
        #[prost(int32, optional, tag = "4")]
        pub r#type: Option<i32>,
        #[prost(message, optional, tag = "5")]
        pub event: Option<Event>,
        #[prost(int32, optional, tag = "6")]
        pub target: Option<i32>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct Event {
        #[prost(string, optional, tag = "1")]
        pub r#type: Option<String>,
        #[prost(uint64, optional, tag = "2")]
        pub delay: Option<u64>,
        #[prost(string, optional, tag = "3")]
        pub state_id: Option<String>,
        #[prost(message, optional, boxed, tag = "4")]
        pub event: Option<Box<Event>>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct SystemTimer {
        #[prost(message, optional, tag = "1")]
        pub source: Option<ActorRef>,
        #[prost(uint64, optional, tag = "4")]
        pub due_at: Option<u64>,
        #[prost(message, optional, tag = "6")]
        pub event: Option<Event>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct ActorRef {
        #[prost(string, optional, tag = "1")]
        pub actor: Option<String>,
        #[prost(uint64, optional, tag = "2")]
        pub incarnation: Option<u64>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct Counters {
        #[prost(uint64, optional, tag = "1")]
        pub actor: Option<u64>,
        #[prost(uint64, optional, tag = "3")]
        pub sequence: Option<u64>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct Value {
        #[prost(oneof = "Kind", tags = "1, 2, 3, 4, 5, 6, 7, 8")]
        pub kind: Option<Kind>,
    }
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Kind {
        #[prost(int32, tag = "1")]
        Null(i32),
        #[prost(bool, tag = "2")]
        Bool(bool),
        #[prost(sint64, tag = "3")]
        Int(i64),
        #[prost(uint64, tag = "4")]
        Uint(u64),
        #[prost(double, tag = "5")]
        Float(f64),
        #[prost(string, tag = "6")]
        String(String),
        #[prost(message, tag = "7")]
        List(List),
        #[prost(message, tag = "8")]
        Object(Object),
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct List {
        #[prost(message, repeated, tag = "1")]
        pub values: Vec<Value>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct Object {
        #[prost(message, repeated, tag = "1")]
        pub fields: Vec<Field>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct Field {
        #[prost(string, tag = "1")]
        pub key: String,
        #[prost(message, optional, tag = "2")]
        pub value: Option<Value>,
    }
}

#[test]
fn an_independent_protobuf_implementation_reads_snapshots() {
    use independent::{Kind, SystemSnapshot};
    use prost::Message as _;
    let definition = json!({
        "id": "order", "initial": "open", "context": { "items": 2, "tags": ["web", "eu"], "price": 9.5 },
        "states": { "open": { "after": { "60000": { "target": "done" } } }, "done": { "type": "final" } }
    });
    let m: Arc<Machine<Value, Value>> = Arc::new(Machine::from_json(&definition, &Registry::new()).unwrap());
    let actor = xstate_rt::Actor::start(m, 7).unwrap();
    let record = Codec::Protobuf.encode_serialize(&actor.persist_compact());
    let s = SystemSnapshot::decode(&record[1..]).unwrap();
    assert_eq!(s.root.as_deref(), Some("order"));
    assert_eq!(s.now, Some(7));
    let entry = &s.actors[0];
    assert_eq!((entry.key.as_str(), entry.id.as_deref(), entry.incarnation), ("order", Some("order"), Some(0)));
    assert_eq!((entry.src.as_deref(), entry.started), (Some("root"), Some(true)));
    let snap = entry.snapshot.as_ref().unwrap();
    assert_eq!(snap.status, Some(1), "STATUS_ACTIVE");
    assert_eq!(snap.node_ids, vec!["order", "order.open"]);
    let Some(Kind::Object(ctx)) = snap.context.as_ref().and_then(|v| v.kind.clone()) else { panic!("context") };
    let keys: Vec<&str> = ctx.fields.iter().map(|f| f.key.as_str()).collect();
    assert_eq!(keys, vec!["items", "tags", "price"], "key order");
    assert_eq!(ctx.fields[0].value.as_ref().unwrap().kind, Some(Kind::Int(2)));
    assert_eq!(ctx.fields[2].value.as_ref().unwrap().kind, Some(Kind::Float(9.5)));
    let timer = &snap.timers[0];
    assert_eq!((timer.delay, timer.r#type, timer.target), (Some(60000), Some(1), Some(1)));
    let event = timer.event.as_ref().unwrap();
    assert_eq!(event.r#type.as_deref(), Some("xstate.after"));
    assert_eq!((event.delay, event.state_id.as_deref()), (Some(60000), Some("order.open")));
    let st = &s.timers[0];
    assert_eq!(st.source.as_ref().unwrap().actor.as_deref(), Some("order"));
    assert_eq!(st.due_at, Some(60007));
    assert_eq!(st.event.as_ref().unwrap().event.as_ref().unwrap().state_id.as_deref(), Some("order.open"));
    assert_eq!(s.counters.as_ref().unwrap().actor, Some(1));
    // prost re-encodes what it read, and the codec reads that back.
    let full = SystemSnapshot::decode(&record[1..]).unwrap();
    assert!(full.extra.is_empty(), "nothing fell back to extra: {:?}", full.extra);
}

/// The checkout example's events as producers would send them, encoded by
/// prost from structs written after the generated `.proto`.
mod checkout_events {
    use prost::Message;

    #[derive(Clone, PartialEq, Message)]
    pub struct Event {
        #[prost(oneof = "Kind", tags = "1, 2, 3, 4")]
        pub event: Option<Kind>,
    }
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Kind {
        #[prost(message, tag = "1")]
        CartAdd(CartAdd),
        #[prost(message, tag = "2")]
        Checkout(Checkout),
        #[prost(message, tag = "3")]
        PaymentMethod(PaymentMethod),
        #[prost(message, tag = "4")]
        Cancel(Cancel),
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct CartAdd {
        #[prost(message, optional, tag = "1")]
        pub line: Option<Line>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct Line {
        #[prost(string, tag = "1")]
        pub sku: String,
        #[prost(uint64, tag = "2")]
        pub quantity: u64,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct Checkout {
        #[prost(double, tag = "1")]
        pub total: f64,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct PaymentMethod {
        #[prost(message, optional, tag = "1")]
        pub method: Option<Method>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct Method {
        #[prost(oneof = "MethodKind", tags = "1, 2")]
        pub kind: Option<MethodKind>,
    }
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum MethodKind {
        #[prost(message, tag = "1")]
        Card(Card),
        #[prost(message, tag = "2")]
        Invoice(Invoice),
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct Card {
        #[prost(string, tag = "2")]
        pub last4: String,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct Invoice {
        #[prost(string, optional, tag = "2")]
        pub reference: Option<String>,
    }
    #[derive(Clone, PartialEq, Message)]
    pub struct Cancel {}
}

#[test]
fn events_from_an_independent_producer_decode_to_json_events() {
    use checkout_events::*;
    use prost::Message as _;
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../examples/checkout/machines/checkout.xstate.json");
    let bundle: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let schema = xstate_kafka::proto::events::EventSchema::from_bundle(&bundle).unwrap();
    let cases = [
        (
            Kind::CartAdd(CartAdd { line: Some(Line { sku: "A-1".into(), quantity: 3 }) }),
            json!({ "type": "cart.add", "line": { "sku": "A-1", "quantity": 3 } }),
        ),
        (Kind::Checkout(Checkout { total: 120.5 }), json!({ "type": "checkout", "total": 120.5 })),
        (
            Kind::PaymentMethod(PaymentMethod {
                method: Some(Method { kind: Some(MethodKind::Card(Card { last4: "4242".into() })) }),
            }),
            json!({ "type": "payment.method", "method": { "kind": "card", "last4": "4242" } }),
        ),
        (
            Kind::PaymentMethod(PaymentMethod {
                method: Some(Method { kind: Some(MethodKind::Invoice(Invoice { reference: None })) }),
            }),
            json!({ "type": "payment.method", "method": { "kind": "invoice" } }),
        ),
        (Kind::Cancel(Cancel {}), json!({ "type": "cancel" })),
    ];
    for (kind, expected) in cases {
        let bytes = Event { event: Some(kind) }.encode_to_vec();
        assert_eq!(schema.decode(&bytes).unwrap(), expected);
        // And our encoder writes what prost reads back to the same event.
        let ours = schema.encode(&expected).unwrap();
        assert_eq!(schema.decode(&Event::decode(ours.as_slice()).unwrap().encode_to_vec()).unwrap(), expected);
    }
}

/// A partition takes JSON, protobuf and MessagePack records (as their
/// `content-type` header selects) and drives the same actor with them.
#[test]
fn a_partition_takes_events_in_every_format() {
    use xstate_kafka::proto::events::EventSchema;
    use xstate_kafka::EventFormat;
    let bundle = json!({
        "machine": {
            "id": "meter",
            "initial": "on",
            "context": { "total": 0 },
            "states": {
                "on": {
                    "on": {
                        "reading": { "actions": [{ "type": "@xstate.emit", "event": { "type": "seen" } }] },
                        "off": { "target": "off" }
                    }
                },
                "off": { "type": "final" }
            }
        },
        "schemas": {
            "events": {
                "reading": {
                    "type": "object",
                    "properties": { "value": { "type": "number" }, "unit": { "type": "string" } },
                    "required": ["value"],
                    "additionalProperties": false
                },
                "off": { "type": "object", "properties": {}, "additionalProperties": false }
            }
        }
    });
    let schema = Arc::new(EventSchema::from_bundle(&bundle).unwrap());
    let machine = Arc::new(Machine::<Value, Value>::from_json(&bundle["machine"], &Registry::new()).unwrap());
    let config = EngineConfig { event_schema: Some(schema.clone()), ..Default::default() };
    let mut p = Partition::new(machine.clone(), config);

    let reading = json!({ "type": "reading", "value": 21.5, "unit": "C" });
    let format = |ct: &str| EventFormat::from_content_type(ct).unwrap();
    p.handle_as(Some("m1"), &serde_json::to_vec(&reading).unwrap(), format("application/json"), 1);
    p.handle_as(Some("m1"), &schema.encode(&reading).unwrap(), format("application/x-protobuf"), 2);
    p.handle_as(Some("m1"), &rmp_serde::to_vec_named(&reading).unwrap(), format("Application/MsgPack; v=1"), 3);
    // No header: the configured default (JSON).
    p.handle(Some("m1"), br#"{ "type": "reading", "value": 1 }"#, 4);
    p.handle_as(Some("m1"), b"\x0a\x03junk", EventFormat::Protobuf, 5);
    p.handle_as(Some("m1"), &schema.encode(&json!({ "type": "off" })).unwrap(), EventFormat::Protobuf, 6);
    let out = p.flush();
    assert_eq!(out.emitted.len(), 4, "{:?}", out.dead_letters);
    assert_eq!(out.dead_letters.len(), 1);
    assert!(out.dead_letters[0].reason.starts_with("not a protobuf event"), "{}", out.dead_letters[0].reason);
    assert_eq!(out.changelog, vec![("m1".to_owned(), None)], "the final actor was evicted");
    assert_eq!(EventFormat::from_content_type("text/plain"), None);

    // Without a schema, protobuf records are dead letters.
    let mut p = Partition::new(machine, EngineConfig::default());
    p.handle_as(Some("m2"), &schema.encode(&reading).unwrap(), EventFormat::Protobuf, 1);
    let out = p.flush();
    assert!(out.dead_letters[0].reason.contains("no event schema"), "{}", out.dead_letters[0].reason);
}
