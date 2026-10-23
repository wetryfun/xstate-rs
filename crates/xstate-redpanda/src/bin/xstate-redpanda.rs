//! The generic service: runs a machine without custom implementations from
//! a configuration file (see `xstate_redpanda::config`).
//!
//!   xstate-redpanda <config.json>           run until Ctrl-C / SIGTERM
//!   xstate-redpanda --check <config.json>   validate the config and machine
//!
//! `XSTATE_REDPANDA_DEBUG=1` traces worker batches on stderr.

use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use xstate_redpanda::config::FileConfig;
use xstate_redpanda::kafka::Service;

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("xstate-redpanda: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (check, path) = match args.as_slice() {
        [flag, path] if flag == "--check" => (true, path),
        [path] if !path.starts_with('-') => (false, path),
        _ => return Err("usage: xstate-redpanda [--check] <config.json>".into()),
    };
    let config = FileConfig::load(path.as_ref())?;
    let service_config = config.service()?;
    let machine = Arc::new(config.machine()?);
    if check {
        eprintln!("xstate-redpanda: {path} is valid ({} workers)", service_config.workers);
        return Ok(());
    }

    let stop = Arc::new(AtomicBool::new(false));
    let flag = stop.clone();
    ctrlc::set_handler(move || flag.store(true, Ordering::Relaxed)).map_err(|e| format!("signal handler: {e}"))?;

    let workers = service_config.workers;
    let service = Service::start(service_config, machine).map_err(|e| e.to_string())?;
    if let Some(address) = &config.metrics_address {
        let (bound, _) = xstate_redpanda::metrics::serve(address, service.metrics_handle())
            .map_err(|e| format!("metrics on {address}: {e}"))?;
        eprintln!("xstate-redpanda: metrics on http://{bound}/metrics");
    }
    eprintln!("xstate-redpanda: running {workers} workers for group {}", config.group_id);

    let interval = Duration::from_millis(config.log_interval_ms.unwrap_or(10_000));
    let mut last_log = Instant::now();
    while !stop.load(Ordering::Relaxed) && service.is_running() {
        std::thread::sleep(Duration::from_millis(100));
        if !interval.is_zero() && last_log.elapsed() >= interval {
            eprintln!("xstate-redpanda: {}", service.metrics());
            last_log = Instant::now();
        }
    }
    let crashed = !service.is_running();
    eprintln!("xstate-redpanda: stopping ({})", service.metrics());
    service.stop().map_err(|e| e.to_string())?;
    if crashed {
        return Err("a worker stopped unexpectedly".into());
    }
    Ok(())
}
