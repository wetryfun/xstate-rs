//! Avro snapshots, events and outputs (`xstate_kafka::avro`).

use std::path::Path;
use std::sync::Arc;

use serde_json::{json, Value};
use xstate_kafka::avro::registry::MemoryRegistry;
use xstate_kafka::avro::AvroSchemas;

fn snapshots_in(records: &Value, out: &mut Vec<Value>) {
    for r in records.as_array().into_iter().flatten() {
        if let Some(s) = r.get("snapshot").filter(|s| s.is_object()) {
            out.push(s.clone());
        }
    }
}

fn checkout() -> Value {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../examples/checkout/machines/checkout.xstate.json");
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn schemas(bundle: &Value) -> AvroSchemas {
    let mut s = AvroSchemas::from_bundle(bundle, Arc::new(MemoryRegistry::default())).unwrap();
    s.register([Some("in-value"), Some("out-value"), Some("changelog-value")], true).unwrap();
    s
}

/// Every system snapshot XState recorded in the fixtures survives an Avro
/// record (with the snapshot schema of a bundle without a typed context).
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
    let s = schemas(&json!({ "machine": { "id": "m" }, "schemas": {} }));
    for snapshot in &snapshots {
        let mut record = Vec::new();
        s.encode_snapshot(snapshot, &mut record).unwrap();
        assert_eq!(&s.decode_snapshot(&record).unwrap(), snapshot);
    }
}

/// With a bundle's context schema, the root context is a typed record (and
/// a context that does not fit it still round-trips, as extra JSON).
#[test]
fn snapshot_contexts_are_typed_from_the_bundle() {
    let s = schemas(&checkout());
    assert!(s.snapshot.schema().contains("\"name\":\"xstate.snapshot.v1.context.Context\""), "{}", s.snapshot.schema());
    for context in [json!({ "status": "open", "lines": [{ "sku": "A", "quantity": 1 }] }), json!({ "other": true })] {
        let snapshot =
            json!({ "root": "c", "actors": { "c": { "snapshot": { "context": context, "nodeIds": ["c"] } } } });
        let mut record = Vec::new();
        s.encode_snapshot(&snapshot, &mut record).unwrap();
        assert_eq!(s.decode_snapshot(&record).unwrap(), snapshot);
    }
}

/// A small machine with Avro-ready schemas.
fn counter_bundle() -> Value {
    json!({
        "machine": {
            "id": "counter", "initial": "counting", "context": { "count": 0 },
            "states": {
                "counting": { "on": {
                    "add": { "context": { "count": 1 }, "actions": [{ "type": "@xstate.emit", "event": { "type": "added", "by": 1 } }] },
                    "odd": { "actions": [{ "type": "@xstate.emit", "event": { "type": "undeclared" } }] },
                    "stop": { "target": "done" }
                } },
                "done": { "type": "final" }
            }
        },
        "schemas": {
            "context": { "type": "object", "properties": { "count": { "type": "integer" } }, "required": ["count"] },
            "events": {
                "add": { "type": "object", "properties": { "note": { "type": "string" } } },
                "odd": { "type": "object", "properties": {} },
                "stop": { "type": "object", "properties": {} }
            },
            "emitted": { "added": { "type": "object", "properties": { "by": { "type": "integer" } }, "required": ["by"] } }
        }
    })
}

fn counter_machine(bundle: &Value) -> Arc<xstate_rt::Machine<Value, Value>> {
    Arc::new(xstate_rt::Machine::from_json(&bundle["machine"], &xstate_rt::Registry::new()).unwrap())
}

#[test]
fn a_partition_takes_avro_events_and_writes_avro_outputs_and_snapshots() {
    use xstate_kafka::{Codec, EngineConfig, EventFormat, OutputFormat, Partition};
    let bundle = counter_bundle();
    let avro = Arc::new(schemas(&bundle));
    let config = EngineConfig {
        codec: Codec::Avro,
        evict_final: false,
        events: EventFormat::Avro,
        outputs: OutputFormat::Avro,
        avro: Some(avro.clone()),
        ..Default::default()
    };
    let mut p = Partition::new(counter_machine(&bundle), config.clone());
    let add = avro.encode_event(&json!({ "type": "add", "note": "hi" })).unwrap();
    p.handle(Some("c1"), &add, 1);
    p.handle_as(Some("c1"), br#"{"type":"add"}"#, EventFormat::Json, 2);
    p.handle(Some("c1"), &avro.encode_event(&json!({ "type": "odd" })).unwrap(), 3);
    p.handle(Some("c1"), b"\x00\x00\x00\x00\x09junk", 4);
    let out = p.flush();
    assert_eq!(out.emitted.len(), 2, "{:?}", out.dead_letters);
    for e in &out.emitted {
        assert_eq!(avro.decode_emitted(e.payload.as_deref().unwrap()).unwrap(), json!({ "type": "added", "by": 1 }));
    }
    let reasons: Vec<&str> = out.dead_letters.iter().map(|d| d.reason.as_str()).collect();
    assert_eq!(reasons.len(), 2, "{reasons:?}");
    assert!(reasons[0].starts_with("not an Avro event") && reasons[1].contains("no Avro form"), "{reasons:?}");

    // The snapshot is an Avro record, and a partition restores from it.
    let record = out.changelog[0].1.clone().unwrap();
    assert_eq!(record[0], xstate_kafka::avro::MAGIC);
    let mut q = Partition::new(counter_machine(&bundle), config);
    q.restore("c1", Some(&record)).unwrap();
    assert_eq!(q.actor("c1").unwrap().context(), &json!({ "count": 1 }));
    q.handle(Some("c1"), &avro.encode_event(&json!({ "type": "stop" })).unwrap(), 5);
    assert_eq!(q.actor("c1").unwrap().status(), xstate_rt::Status::Done);
}
