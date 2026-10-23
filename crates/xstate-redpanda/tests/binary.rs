//! The `xstate-redpanda` binary against a mock cluster in this process:
//! configuration, processing, the metrics endpoint, and a graceful stop on
//! SIGTERM.
#![cfg(feature = "kafka")]

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::mocking::MockCluster;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::{Offset, TopicPartitionList};

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

fn http_get(address: &str, path: &str) -> Option<String> {
    let mut stream = TcpStream::connect(address).ok()?;
    write!(stream, "GET {path} HTTP/1.1\r\nHost: x\r\n\r\n").ok()?;
    let mut response = String::new();
    stream.read_to_string(&mut response).ok()?;
    Some(response)
}

#[test]
fn runs_from_a_config_file_and_stops_on_sigterm() {
    let cluster = MockCluster::new(1).unwrap();
    for topic in ["in", "out", "log"] {
        cluster.create_topic(topic, 2, 1).unwrap();
    }
    let bootstrap = cluster.bootstrap_servers();

    let dir = std::env::temp_dir().join(format!("xstate-binary-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("machine.json"),
        r#"{ "id": "lamp", "initial": "off", "states": {
              "off": { "on": { "toggle": { "target": "on" } } },
              "on": { "entry": [{ "type": "@xstate.emit", "event": { "type": "lit" } }], "on": { "toggle": { "target": "off" } } } } }"#,
    )
    .unwrap();
    let metrics = format!("127.0.0.1:{}", free_port());
    let config = dir.join("service.json");
    std::fs::write(
        &config,
        format!(
            r#"{{ "machine": "machine.json", "bootstrapServers": "{bootstrap}", "groupId": "lamps",
                 "topics": {{ "input": "in", "output": "out", "changelog": "log" }},
                 "workers": 2, "pollTimeoutMs": 20, "metricsAddress": "{metrics}", "logIntervalMs": 0,
                 "client": {{ "session.timeout.ms": 6000 }} }}"#
        ),
    )
    .unwrap();

    let binary = PathBuf::from(env!("CARGO_BIN_EXE_xstate-redpanda"));
    let check = Command::new(&binary).arg("--check").arg(&config).output().unwrap();
    assert!(check.status.success(), "{}", String::from_utf8_lossy(&check.stderr));

    let mut child = Command::new(&binary).arg(&config).stderr(Stdio::piped()).spawn().unwrap();

    let producer: BaseProducer = ClientConfig::new().set("bootstrap.servers", &bootstrap).create().unwrap();
    for i in 0..20 {
        let key = format!("lamp-{}", i % 5);
        producer.send(BaseRecord::<str, str>::to("in").key(&key).payload(r#"{"type":"toggle"}"#)).unwrap();
    }
    producer.flush(Duration::from_secs(10)).unwrap();

    // Each lamp toggles 4 times: lit twice.
    let reader: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("group.id", "reader")
        .set("isolation.level", "read_committed")
        .create()
        .unwrap();
    let mut tpl = TopicPartitionList::new();
    for p in 0..2 {
        tpl.add_partition_offset("out", p, Offset::Beginning).unwrap();
    }
    reader.assign(&tpl).unwrap();
    let mut lit = 0;
    let deadline = Instant::now() + Duration::from_secs(30);
    while lit < 10 && Instant::now() < deadline {
        if let Some(Ok(_)) = reader.poll(Duration::from_millis(50)) {
            lit += 1;
        }
    }
    assert_eq!(lit, 10);

    // Readers can see a transaction before the worker's commit call
    // returns (and counts it): wait for the counters.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut body = String::new();
    while Instant::now() < deadline {
        body = http_get(&metrics, "/metrics").expect("metrics endpoint");
        if body.contains("xstate_emitted_total 10\n") {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert!(body.starts_with("HTTP/1.1 200 OK"), "{body}");
    assert!(body.contains("xstate_records_total 20\n"), "{body}");
    assert!(body.contains("xstate_emitted_total 10\n"), "{body}");
    assert!(http_get(&metrics, "/").unwrap().starts_with("HTTP/1.1 404"));

    let status = Command::new("kill").arg("-TERM").arg(child.id().to_string()).status().unwrap();
    assert!(status.success());
    let exit = child.wait().unwrap();
    let mut stderr = String::new();
    child.stderr.take().unwrap().read_to_string(&mut stderr).unwrap();
    assert!(exit.success(), "{stderr}");
    assert!(stderr.contains("stopping (records=20 emitted=10"), "{stderr}");
    std::fs::remove_dir_all(&dir).ok();
}
