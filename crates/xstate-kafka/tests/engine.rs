//! The deterministic engine: per-key actors, timers, output, changelog
//! records, dead letters and restore.

use std::sync::Arc;

use serde_json::{json, Value};
use xstate_kafka::{Codec, EngineConfig, Partition, SnapshotFormat};
use xstate_rt::{Machine, Registry, Status};

fn order_machine() -> Arc<Machine<Value, Value>> {
    let definition = json!({
        "id": "order",
        "initial": "open",
        "context": { "items": 0 },
        "states": {
            "open": {
                "on": {
                    "add": { "context": { "items": 1 }, "actions": [{ "type": "@xstate.emit", "event": { "type": "added" } }] },
                    "pay": { "target": "paying" }
                },
                "after": { "60000": { "target": "expired" } }
            },
            "paying": {
                "after": { "100": { "target": "paid" } },
                "on": { "loop": { "actions": [{ "type": "@xstate.raise", "event": { "type": "loop" } }] } }
            },
            "paid": { "type": "final", "entry": [{ "type": "@xstate.emit", "event": { "type": "paid" } }] },
            "expired": { "type": "final" }
        }
    });
    Arc::new(Machine::from_json(&definition, &Registry::new()).unwrap())
}

fn partition(evict_final: bool) -> Partition<Value, Value> {
    Partition::new(order_machine(), EngineConfig { codec: Codec::MessagePack, evict_final, ..Default::default() })
}

#[test]
fn records_start_and_drive_actors_by_key() {
    let mut p = partition(true);
    p.handle(Some("a"), br#"{"type":"add"}"#, 1000);
    p.handle(Some("b"), br#"{"type":"pay"}"#, 1000);
    p.handle(Some("a"), br#"{"type":"add"}"#, 1001);
    assert_eq!(p.len(), 2);
    let out = p.flush();
    let emitted: Vec<(&str, &Value)> = out.emitted.iter().map(|e| (e.key.as_str(), &e.event)).collect();
    assert_eq!(emitted, vec![("a", &json!({ "type": "added" })), ("a", &json!({ "type": "added" }))]);
    // One changelog record per changed actor, in order of first change.
    let keys: Vec<&str> = out.changelog.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(keys, vec!["a", "b"]);
    assert!(out.dead_letters.is_empty());
    assert!(p.flush().is_empty());
}

#[test]
fn timers_fire_on_tick_and_final_actors_are_evicted() {
    let mut p = partition(true);
    p.handle(Some("b"), br#"{"type":"pay"}"#, 5000);
    p.flush();
    assert_eq!(p.next_deadline(), Some(5100));
    p.tick(5099);
    assert!(p.flush().is_empty());
    p.tick(5100);
    let out = p.flush();
    assert_eq!(out.emitted.len(), 1);
    assert_eq!(out.emitted[0].event, json!({ "type": "paid" }));
    assert_eq!(out.changelog, vec![("b".to_owned(), None)], "a tombstone for the finished actor");
    assert!(p.is_empty());
    assert_eq!(p.next_deadline(), None);

    // Kept when not evicting.
    let mut p = partition(false);
    p.handle(Some("b"), br#"{"type":"pay"}"#, 0);
    p.tick(100);
    let out = p.flush();
    assert!(out.changelog[0].1.is_some());
    assert_eq!(p.actor("b").unwrap().status(), Status::Done);
}

#[test]
fn bad_records_become_dead_letters() {
    let mut p = partition(true);
    p.handle(None, br#"{"type":"add"}"#, 0);
    p.handle(Some("a"), b"not json", 0);
    p.handle(Some("a"), br#"{"no":"type"}"#, 0);
    p.handle(Some("c"), br#"{"type":"pay"}"#, 0);
    p.handle(Some("c"), br#"{"type":"loop"}"#, 0);
    let out = p.flush();
    let reasons: Vec<&str> = out.dead_letters.iter().map(|d| d.reason.as_str()).collect();
    assert_eq!(reasons.len(), 4, "{reasons:?}");
    assert!(reasons[0].contains("key"));
    assert!(reasons[1].contains("not JSON"));
    assert!(reasons[2].contains("type"));
    assert!(reasons[3].contains("infinite loop"), "{reasons:?}");
    // The failed actor keeps its last good state, in error.
    assert_eq!(p.actor("c").unwrap().status(), Status::Error);
    assert_eq!(p.actor("c").unwrap().value(), json!("paying"));
}

#[test]
fn restore_from_the_changelog_continues_where_it_left() {
    let formats = [SnapshotFormat::Compact, SnapshotFormat::XState];
    for (codec, snapshots) in [Codec::Json, Codec::MessagePack].into_iter().flat_map(|c| formats.map(|f| (c, f))) {
        let mut p = Partition::new(order_machine(), EngineConfig { codec, evict_final: true, snapshots });
        p.handle(Some("a"), br#"{"type":"add"}"#, 10);
        p.handle(Some("b"), br#"{"type":"pay"}"#, 20);
        p.handle(Some("gone"), br#"{"type":"pay"}"#, 20);
        p.tick(120);
        let mut log = p.flush().changelog;
        // A compacted changelog keeps the latest record per key.
        log.dedup_by(|x, y| x.0 == y.0);

        // Records are read whatever the configured codec and format.
        let mut q = Partition::new(order_machine(), EngineConfig::default());
        for (key, record) in &log {
            q.restore(key, record.as_deref()).unwrap();
        }
        assert_eq!(q.len(), 1, "{codec:?} {snapshots:?}: b and gone finished");
        let a = q.actor("a").unwrap();
        assert_eq!(a.context(), &json!({ "items": 1 }));
        // The restored timer still expires the order.
        assert_eq!(q.next_deadline(), Some(60_010));
        q.tick(60_010);
        let out = q.flush();
        assert_eq!(out.changelog, vec![("a".to_owned(), None)]);
    }
}

fn lookup_machine() -> Arc<Machine<Value, Value>> {
    let definition = json!({
        "id": "lookup",
        "initial": "idle",
        "states": {
            "idle": { "on": { "go": { "target": "fetching" } } },
            "fetching": {
                "invoke": {
                    "id": "fetch",
                    "src": "fetchUser",
                    "onDone": { "target": "found", "actions": [{ "type": "@xstate.emit", "event": { "type": "found" } }] },
                    "onError": { "target": "idle" }
                },
                "on": { "cancel": { "target": "idle" } }
            },
            "found": { "type": "final" }
        }
    });
    let mut registry = Registry::new();
    registry.actor("fetchUser", Arc::new(xstate_rt::step::async_logic(Some("fetchUser".into()), None).unwrap()));
    Arc::new(Machine::from_json(&definition, &registry).unwrap())
}

#[test]
fn external_work_is_published_once_and_resolved_by_records() {
    let config = EngineConfig { codec: Codec::MessagePack, evict_final: true, ..Default::default() };
    let mut p = Partition::new(lookup_machine(), config.clone());
    p.handle(Some("u1"), br#"{"type":"go"}"#, 10);
    let out = p.flush();
    assert_eq!(out.effects.len(), 1, "{out:?}");
    let effect = &out.effects[0];
    assert_eq!(effect.key, "u1");
    assert_eq!(effect.effect["type"], "@xstate.effect");
    let id = effect.effect["id"].as_str().unwrap().to_owned();
    let snapshot = out.changelog[0].1.clone().unwrap();

    // Other events don't publish it again.
    p.handle(Some("u1"), br#"{"type":"unknown"}"#, 11);
    assert!(p.flush().effects.is_empty());

    // A restored partition knows the work was published with the snapshot.
    let mut restored = Partition::new(lookup_machine(), config);
    restored.restore("u1", Some(&snapshot)).unwrap();
    for p in [&mut p, &mut restored] {
        let result = json!({ "type": xstate_kafka::EFFECT_RESULT, "effectId": id,
                             "event": { "type": "xstate.async.resolve", "data": { "name": "Ada" } } });
        p.handle(Some("u1"), result.to_string().as_bytes(), 20);
        let out = p.flush();
        assert!(out.dead_letters.is_empty(), "{:?}", out.dead_letters);
        assert!(out.effects.is_empty());
        assert_eq!(out.emitted.len(), 1);
        assert_eq!(out.emitted[0].event, json!({ "type": "found" }));
        assert_eq!(out.changelog, vec![("u1".to_owned(), None)], "done and evicted");

        // A late duplicate result is a dead letter.
        p.handle(Some("u1"), result.to_string().as_bytes(), 21);
        let out = p.flush();
        assert_eq!(out.dead_letters.len(), 1);
        assert!(out.dead_letters[0].reason.contains("no actor"), "{}", out.dead_letters[0].reason);
    }

    // Leaving the invoking state cancels the work: its result is stale, and
    // re-entering publishes new work.
    let mut p = Partition::new(lookup_machine(), EngineConfig::default());
    p.handle(Some("u2"), br#"{"type":"go"}"#, 10);
    let first = p.flush().effects[0].effect["id"].as_str().unwrap().to_owned();
    p.handle(Some("u2"), br#"{"type":"cancel"}"#, 11);
    p.handle(Some("u2"), br#"{"type":"go"}"#, 12);
    let out = p.flush();
    assert_eq!(out.effects.len(), 1);
    assert_ne!(out.effects[0].effect["id"].as_str().unwrap(), first);
    let stale = json!({ "type": xstate_kafka::EFFECT_RESULT, "effectId": first,
                        "event": { "type": "xstate.async.resolve" } });
    p.handle(Some("u2"), stale.to_string().as_bytes(), 13);
    let out = p.flush();
    assert_eq!(out.dead_letters.len(), 1);
    assert!(out.dead_letters[0].reason.contains("not pending"), "{}", out.dead_letters[0].reason);
    assert_eq!(p.actor("u2").unwrap().value(), json!("fetching"));
}
