//! The Schema Registry client against a fake registry speaking the
//! Confluent REST API, and the config wiring that registers subjects.
#![cfg(feature = "kafka")]

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};

use serde_json::{json, Value};
use xstate_kafka::avro::registry::{HttpRegistry, Registry};

/// Registered schemas (id - 1) with their subjects, and the requests seen.
#[derive(Default)]
struct Fake {
    schemas: Vec<(String, String)>,
    requests: Vec<String>,
}

/// Serves the registry API on a random port until the test ends.
fn fake_registry() -> (String, Arc<Mutex<Fake>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let state = Arc::new(Mutex::new(Fake::default()));
    let shared = state.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut reader = BufReader::new(&stream);
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let (mut length, mut auth) = (0, String::new());
            loop {
                let mut header = String::new();
                reader.read_line(&mut header).unwrap();
                let lower = header.to_ascii_lowercase();
                if let Some(v) = lower.strip_prefix("content-length:") {
                    length = v.trim().parse().unwrap();
                }
                if lower.starts_with("authorization:") {
                    auth = header["authorization:".len()..].trim().to_owned();
                }
                if header == "\r\n" || header.is_empty() {
                    break;
                }
            }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
            let mut parts = line.split_whitespace();
            let (method, path) = (parts.next().unwrap().to_owned(), parts.next().unwrap().to_owned());
            let mut fake = shared.lock().unwrap();
            fake.requests.push(format!("{method} {path} {auth}"));
            let segments: Vec<&str> = path.trim_matches('/').split('/').collect();
            let (status, reply) = match (method.as_str(), segments.as_slice()) {
                ("POST", ["subjects", subject, "versions"]) => {
                    let schema = body["schema"].as_str().unwrap().to_owned();
                    let id = match fake.schemas.iter().position(|s| s.0 == schema) {
                        Some(i) => i + 1,
                        None => {
                            fake.schemas.push((schema, subject.to_string()));
                            fake.schemas.len()
                        }
                    };
                    (200, json!({ "id": id }))
                }
                ("POST", ["subjects", subject]) => {
                    let schema = body["schema"].as_str().unwrap();
                    match fake.schemas.iter().position(|s| s.0 == schema && s.1 == *subject) {
                        Some(i) => (200, json!({ "id": i + 1, "subject": subject, "version": 1 })),
                        None => (404, json!({ "error_code": 40403, "message": "Schema not found" })),
                    }
                }
                ("GET", ["schemas", "ids", id]) => match fake.schemas.get(id.parse::<usize>().unwrap() - 1) {
                    Some(s) => (200, json!({ "schema": s.0 })),
                    None => (404, json!({ "error_code": 40403, "message": "Schema not found" })),
                },
                _ => (404, json!({ "error_code": 404, "message": "not found" })),
            };
            drop(fake);
            let text = reply.to_string();
            let mut stream = &stream;
            let _ = write!(
                stream,
                "HTTP/1.1 {status} X\r\nContent-Type: application/vnd.schemaregistry.v1+json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{text}",
                text.len()
            );
        }
    });
    (url, state)
}

#[test]
fn the_http_client_registers_looks_up_and_reads_schemas() {
    let (url, state) = fake_registry();
    let r = HttpRegistry::new(&url, Some(("user".into(), "pass".into())), std::time::Duration::from_secs(5));
    let schema = r#"{"type":"record","name":"A","fields":[]}"#;
    assert!(r.lookup("t-value", schema).unwrap_err().contains("404 Schema not found"));
    assert_eq!(r.register("t-value", schema).unwrap(), 1);
    assert_eq!(r.register("t-value", schema).unwrap(), 1);
    assert_eq!(r.lookup("t-value", schema).unwrap(), 1);
    assert_eq!(r.schema(1).unwrap(), schema);
    assert!(r.schema(9).is_err());
    let requests = state.lock().unwrap().requests.clone();
    assert_eq!(requests[1], "POST /subjects/t-value/versions Basic dXNlcjpwYXNz");
    assert_eq!(requests.last().unwrap(), "GET /schemas/ids/9 Basic dXNlcjpwYXNz");
}

#[test]
fn a_config_with_avro_registers_its_subjects() {
    let (url, state) = fake_registry();
    let dir = std::env::temp_dir().join(format!("xstate-avro-config-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let bundle = concat!(env!("CARGO_MANIFEST_DIR"), "/../../examples/checkout/machines/checkout.xstate.json");
    let config = json!({
        "machine": bundle, "bootstrapServers": "b", "groupId": "g",
        "topics": { "input": "orders", "output": "orders-out", "changelog": "orders-log" },
        "events": "avro", "outputs": "avro", "codec": "avro",
        "schemaRegistry": { "url": url, "username": "u", "password": { "env": "HOME" } }
    });
    let path = dir.join("c.json");
    std::fs::write(&path, config.to_string()).unwrap();
    let service = xstate_kafka::config::FileConfig::load(&path).unwrap().service().unwrap();
    let avro = service.engine.avro.unwrap();
    assert_eq!(
        (avro.events.as_ref().unwrap().id(), avro.emitted.as_ref().unwrap().id(), avro.snapshot.id()),
        (Some(1), Some(2), Some(3))
    );
    let subjects: Vec<String> = state.lock().unwrap().schemas.iter().map(|s| s.1.clone()).collect();
    assert_eq!(subjects, ["orders-value", "orders-out-value", "orders-log-value"]);

    // Without a registry, Avro is an error.
    let mut no_registry = config.clone();
    no_registry.as_object_mut().unwrap().remove("schemaRegistry");
    std::fs::write(&path, no_registry.to_string()).unwrap();
    let err = xstate_kafka::config::FileConfig::load(&path).unwrap().service().unwrap_err();
    assert!(err.contains("schemaRegistry"), "{err}");
    std::fs::remove_dir_all(&dir).ok();
}
