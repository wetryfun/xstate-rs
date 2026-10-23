//! The generic service: runs a machine without custom implementations from
//! a configuration file (see `xstate_kafka::config`).
//!
//!   xstate-kafka <config.json>              run until Ctrl-C / SIGTERM
//!   xstate-kafka --check <config.json>      validate the config and machine
//!   xstate-kafka proto events <file>        print the input events' .proto
//!                                           (file: a bundle or a config)
//!   xstate-kafka proto snapshot             print the snapshot .proto
//!   xstate-kafka avro <events|emitted|snapshot> <file>
//!                                           print an Avro schema (file: a
//!                                           bundle or a config)
//!
//! `XSTATE_KAFKA_DEBUG=1` traces worker batches on stderr.

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use xstate_kafka::config::FileConfig;
use xstate_kafka::service::Service;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("xstate-kafka: {e}");
            ExitCode::FAILURE
        }
    }
}

/// Prints a `.proto` file for producers and other readers.
fn proto(args: &[String]) -> Result<(), String> {
    match args {
        [what] if what == "snapshot" => {
            print!("{}", xstate_kafka::proto::snapshot::proto_file());
            Ok(())
        }
        [what, path] if what == "events" => {
            let text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
            let json: serde_json::Value = serde_json::from_str(&text).map_err(|e| format!("{path}: {e}"))?;
            // A service config names the machine file; a bundle is used as is.
            let schema = if json.get("bootstrapServers").is_some() {
                FileConfig::load(path.as_ref())?.event_schema()?
            } else {
                xstate_kafka::proto::events::EventSchema::from_bundle(&json).map_err(|e| format!("{path}: {e}"))?
            };
            print!("{}", schema.proto_file());
            Ok(())
        }
        _ => {
            Err("usage: xstate-kafka proto events <bundle.xstate.json | config.json> | xstate-kafka proto snapshot"
                .into())
        }
    }
}

/// Prints an Avro schema (`.avsc` JSON) for producers and other readers.
fn avro(args: &[String]) -> Result<(), String> {
    let usage = "usage: xstate-kafka avro <events | emitted | snapshot> <bundle.xstate.json | config.json>";
    let [what, path] = args else { return Err(usage.into()) };
    let text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
    let mut json: serde_json::Value = serde_json::from_str(&text).map_err(|e| format!("{path}: {e}"))?;
    if json.get("bootstrapServers").is_some() {
        json = FileConfig::load(path.as_ref())?.read_machine_file()?;
    }
    let registry = Arc::new(xstate_kafka::avro::registry::MemoryRegistry::default());
    let schemas = xstate_kafka::avro::AvroSchemas::from_bundle(&json, registry).map_err(|e| format!("{path}: {e}"))?;
    let root = match what.as_str() {
        "events" => schemas.events.as_ref().ok_or("the bundle has no schemas.events")?,
        "emitted" => schemas.emitted.as_ref().ok_or("the bundle has no schemas.emitted")?,
        "snapshot" => &schemas.snapshot,
        _ => return Err(usage.into()),
    };
    let pretty: serde_json::Value = serde_json::from_str(root.schema()).map_err(|e| e.to_string())?;
    println!("{}", serde_json::to_string_pretty(&pretty).map_err(|e| e.to_string())?);
    Ok(())
}

fn run() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("proto") {
        return proto(&args[1..]);
    }
    if args.first().map(String::as_str) == Some("avro") {
        return avro(&args[1..]);
    }
    let (check, path) = match args.as_slice() {
        [flag, path] if flag == "--check" => (true, path),
        [path] if !path.starts_with('-') => (false, path),
        _ => return Err("usage: xstate-kafka [--check] <config.json>".into()),
    };
    let config = FileConfig::load(path.as_ref())?;
    let service_config = config.service()?;
    let machine = config.machines()?;
    if check {
        eprintln!("xstate-kafka: {path} is valid ({} workers)", service_config.workers);
        return Ok(());
    }

    let stop = Arc::new(AtomicBool::new(false));
    let flag = stop.clone();
    ctrlc::set_handler(move || flag.store(true, Ordering::Relaxed)).map_err(|e| format!("signal handler: {e}"))?;

    let workers = service_config.workers;
    let service = Service::start(service_config, machine).map_err(|e| e.to_string())?;
    if let Some(address) = &config.metrics_address {
        let (bound, _) = xstate_kafka::metrics::serve(address, service.metrics_handle())
            .map_err(|e| format!("metrics on {address}: {e}"))?;
        eprintln!("xstate-kafka: metrics on http://{bound}/metrics");
    }
    eprintln!("xstate-kafka: running {workers} workers for group {}", config.group_id);

    let interval = Duration::from_millis(config.log_interval_ms.unwrap_or(10_000));
    let mut last_log = Instant::now();
    while !stop.load(Ordering::Relaxed) && service.is_running() {
        std::thread::sleep(Duration::from_millis(100));
        if !interval.is_zero() && last_log.elapsed() >= interval {
            eprintln!("xstate-kafka: {}", service.metrics());
            last_log = Instant::now();
        }
    }
    let crashed = !service.is_running();
    eprintln!("xstate-kafka: stopping ({})", service.metrics());
    service.stop().map_err(|e| e.to_string())?;
    if crashed {
        return Err("a worker stopped unexpectedly".into());
    }
    Ok(())
}
