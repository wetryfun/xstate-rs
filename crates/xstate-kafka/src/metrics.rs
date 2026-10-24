//! Service counters, shared by the workers, readable at any time, and
//! renderable in the Prometheus text format.

use std::fmt::{self, Write};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Mutex;

/// What one committed transaction adds to the counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BatchCounts {
    pub records: u64,
    pub emitted: u64,
    pub dead_letters: u64,
    pub changelog: u64,
    pub effects: u64,
    pub duplicates: u64,
    pub restore_failures: u64,
    pub migrated: u64,
}

/// Counters updated by the workers.
#[derive(Debug)]
pub struct Metrics {
    records: AtomicU64,
    emitted: AtomicU64,
    dead_letters: AtomicU64,
    changelog_records: AtomicU64,
    effects: AtomicU64,
    duplicates: AtomicU64,
    restore_failures: AtomicU64,
    migrated: AtomicU64,
    transactions: AtomicU64,
    aborted_transactions: AtomicU64,
    restored_partitions: AtomicU64,
    restored_actors: AtomicU64,
    restored_records: AtomicU64,
    checkpoints: AtomicU64,
    pauses: AtomicU64,
    paused_micros: AtomicU64,
    commit_micros_total: AtomicU64,
    commit_micros_max: AtomicU64,
    /// Live actors, per worker.
    actors: Vec<AtomicU64>,
    workers: Vec<WorkerState>,
}

/// One worker's health, for `/health` and `/ready`.
#[derive(Debug, Default)]
struct WorkerState {
    /// Running (set when it starts, cleared when it stops or fails).
    alive: AtomicBool,
    /// Has joined the consumer group (received an assignment).
    joined: AtomicBool,
    /// Restoring newly assigned partitions.
    restoring: AtomicBool,
    /// Its partitions are paused while it is busy.
    paused: AtomicBool,
    /// Consumer lag per input partition, from the latest statistics.
    lag: Mutex<Vec<(i32, i64)>>,
}

/// A point-in-time copy of [`Metrics`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub records: u64,
    pub emitted: u64,
    pub dead_letters: u64,
    pub changelog_records: u64,
    pub effects: u64,
    pub duplicates: u64,
    pub restore_failures: u64,
    pub migrated: u64,
    pub transactions: u64,
    pub aborted_transactions: u64,
    pub restored_partitions: u64,
    pub restored_actors: u64,
    pub restored_records: u64,
    pub checkpoints: u64,
    /// Times a busy worker paused its partitions, and for how long in all.
    pub pauses: u64,
    pub paused_micros: u64,
    /// Workers whose partitions are paused now.
    pub paused_workers: u64,
    pub commit_micros_total: u64,
    pub commit_micros_max: u64,
    pub actors: u64,
    pub healthy: bool,
    pub ready: bool,
    /// Consumer lag per input partition, by partition.
    pub lag: Vec<(i32, i64)>,
}

impl Metrics {
    pub fn new(workers: usize) -> Self {
        let zero = || AtomicU64::new(0);
        Self {
            records: zero(),
            emitted: zero(),
            dead_letters: zero(),
            changelog_records: zero(),
            effects: zero(),
            duplicates: zero(),
            restore_failures: zero(),
            migrated: zero(),
            transactions: zero(),
            aborted_transactions: zero(),
            restored_partitions: zero(),
            restored_actors: zero(),
            restored_records: zero(),
            checkpoints: zero(),
            pauses: zero(),
            paused_micros: zero(),
            commit_micros_total: zero(),
            commit_micros_max: zero(),
            actors: (0..workers.max(1)).map(|_| zero()).collect(),
            workers: (0..workers.max(1)).map(|_| WorkerState::default()).collect(),
        }
    }

    pub fn committed(&self, batch: &BatchCounts, micros: u64) {
        self.records.fetch_add(batch.records, Ordering::Relaxed);
        self.emitted.fetch_add(batch.emitted, Ordering::Relaxed);
        self.dead_letters.fetch_add(batch.dead_letters, Ordering::Relaxed);
        self.changelog_records.fetch_add(batch.changelog, Ordering::Relaxed);
        self.effects.fetch_add(batch.effects, Ordering::Relaxed);
        self.duplicates.fetch_add(batch.duplicates, Ordering::Relaxed);
        self.restore_failures.fetch_add(batch.restore_failures, Ordering::Relaxed);
        self.migrated.fetch_add(batch.migrated, Ordering::Relaxed);
        self.transactions.fetch_add(1, Ordering::Relaxed);
        self.commit_micros_total.fetch_add(micros, Ordering::Relaxed);
        self.commit_micros_max.fetch_max(micros, Ordering::Relaxed);
    }

    pub fn aborted(&self) {
        self.aborted_transactions.fetch_add(1, Ordering::Relaxed);
    }

    /// Partitions restored, with their actors and the changelog records
    /// read (only the tail after a checkpoint, when there was one).
    pub fn restored(&self, partitions: u64, actors: u64, records: u64) {
        self.restored_partitions.fetch_add(partitions, Ordering::Relaxed);
        self.restored_actors.fetch_add(actors, Ordering::Relaxed);
        self.restored_records.fetch_add(records, Ordering::Relaxed);
    }

    pub fn checkpointed(&self) {
        self.checkpoints.fetch_add(1, Ordering::Relaxed);
    }

    /// A worker paused its partitions while busy.
    pub fn paused(&self, worker: usize) {
        self.pauses.fetch_add(1, Ordering::Relaxed);
        if let Some(w) = self.workers.get(worker) {
            w.paused.store(true, Ordering::Relaxed);
        }
    }

    /// A worker resumed its partitions after `micros` paused.
    pub fn resumed(&self, worker: usize, micros: u64) {
        self.paused_micros.fetch_add(micros, Ordering::Relaxed);
        if let Some(w) = self.workers.get(worker) {
            w.paused.store(false, Ordering::Relaxed);
        }
    }

    pub fn set_alive(&self, worker: usize, alive: bool) {
        if let Some(w) = self.workers.get(worker) {
            w.alive.store(alive, Ordering::Relaxed);
        }
    }

    pub fn set_joined(&self, worker: usize) {
        if let Some(w) = self.workers.get(worker) {
            w.joined.store(true, Ordering::Relaxed);
        }
    }

    pub fn set_restoring(&self, worker: usize, restoring: bool) {
        if let Some(w) = self.workers.get(worker) {
            w.restoring.store(restoring, Ordering::Relaxed);
        }
    }

    /// The worker's consumer lag per input partition.
    pub fn set_lag(&self, worker: usize, lag: Vec<(i32, i64)>) {
        if let Some(w) = self.workers.get(worker) {
            *w.lag.lock().unwrap_or_else(|e| e.into_inner()) = lag;
        }
    }

    /// Every worker is running.
    pub fn healthy(&self) -> bool {
        self.workers.iter().all(|w| w.alive.load(Ordering::Relaxed))
    }

    /// Every worker is running, has joined the group and is not restoring.
    pub fn ready(&self) -> bool {
        self.workers.iter().all(|w| {
            w.alive.load(Ordering::Relaxed) && w.joined.load(Ordering::Relaxed) && !w.restoring.load(Ordering::Relaxed)
        })
    }

    pub fn set_actors(&self, worker: usize, actors: u64) {
        if let Some(slot) = self.actors.get(worker) {
            slot.store(actors, Ordering::Relaxed);
        }
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        let get = |a: &AtomicU64| a.load(Ordering::Relaxed);
        MetricsSnapshot {
            records: get(&self.records),
            emitted: get(&self.emitted),
            dead_letters: get(&self.dead_letters),
            changelog_records: get(&self.changelog_records),
            effects: get(&self.effects),
            duplicates: get(&self.duplicates),
            restore_failures: get(&self.restore_failures),
            migrated: get(&self.migrated),
            transactions: get(&self.transactions),
            aborted_transactions: get(&self.aborted_transactions),
            restored_partitions: get(&self.restored_partitions),
            restored_actors: get(&self.restored_actors),
            restored_records: get(&self.restored_records),
            checkpoints: get(&self.checkpoints),
            pauses: get(&self.pauses),
            paused_micros: get(&self.paused_micros),
            paused_workers: self.workers.iter().filter(|w| w.paused.load(Ordering::Relaxed)).count() as u64,
            commit_micros_total: get(&self.commit_micros_total),
            commit_micros_max: get(&self.commit_micros_max),
            actors: self.actors.iter().map(get).sum(),
            healthy: self.healthy(),
            ready: self.ready(),
            lag: {
                let mut lag: Vec<(i32, i64)> =
                    self.workers.iter().flat_map(|w| w.lag.lock().unwrap_or_else(|e| e.into_inner()).clone()).collect();
                lag.sort_unstable();
                lag
            },
        }
    }
}

impl MetricsSnapshot {
    /// The Prometheus text exposition format.
    pub fn prometheus(&self) -> String {
        let mut out = String::new();
        let mut metric = |name: &str, kind: &str, help: &str, value: u64| {
            let _ = writeln!(out, "# HELP xstate_{name} {help}\n# TYPE xstate_{name} {kind}\nxstate_{name} {value}");
        };
        metric("records_total", "counter", "Input records committed.", self.records);
        metric("emitted_total", "counter", "Emitted events committed to the output topic.", self.emitted);
        metric("dead_letters_total", "counter", "Records that could not be processed.", self.dead_letters);
        metric("changelog_records_total", "counter", "Snapshots and tombstones written.", self.changelog_records);
        metric("effects_total", "counter", "External work requested (effects topic).", self.effects);
        metric("duplicates_total", "counter", "Input events skipped as duplicates.", self.duplicates);
        metric(
            "restore_failures_total",
            "counter",
            "Changelog records that could not be restored (dead letters, keys tombstoned).",
            self.restore_failures,
        );
        metric("migrated_total", "counter", "Restored actors migrated to the current machine version.", self.migrated);
        metric("transactions_total", "counter", "Committed transactions.", self.transactions);
        metric("aborted_transactions_total", "counter", "Aborted transactions.", self.aborted_transactions);
        metric(
            "restored_partitions_total",
            "counter",
            "Partitions restored from the changelog.",
            self.restored_partitions,
        );
        metric("restored_actors_total", "counter", "Actors restored from the changelog.", self.restored_actors);
        metric("restored_records_total", "counter", "Changelog records read while restoring.", self.restored_records);
        metric("checkpoints_total", "counter", "Partition checkpoints written (to any store).", self.checkpoints);
        metric(
            "pauses_total",
            "counter",
            "Times a busy worker paused fetching its partitions (kept polling to stay in the group).",
            self.pauses,
        );
        metric("paused_micros_total", "counter", "Time workers' partitions were paused.", self.paused_micros);
        metric("paused_workers", "gauge", "Workers whose partitions are paused now.", self.paused_workers);
        metric("commit_micros_total", "counter", "Time spent committing transactions.", self.commit_micros_total);
        metric("commit_micros_max", "gauge", "Longest transaction commit.", self.commit_micros_max);
        metric("actors", "gauge", "Live actors.", self.actors);
        metric(
            "ready",
            "gauge",
            "1 when every worker has joined the group and none is restoring.",
            u64::from(self.ready),
        );
        metric("consumer_lag_total", "gauge", "Input records not yet processed, all partitions.", self.lag_total());
        let _ = writeln!(
            out,
            "# HELP xstate_consumer_lag Input records not yet processed, per partition.\n# TYPE xstate_consumer_lag gauge"
        );
        for (partition, lag) in &self.lag {
            let _ = writeln!(out, "xstate_consumer_lag{{partition=\"{partition}\"}} {lag}");
        }
        out
    }

    /// The consumer lag of all partitions.
    pub fn lag_total(&self) -> u64 {
        self.lag.iter().map(|(_, l)| (*l).max(0) as u64).sum()
    }
}

impl fmt::Display for MetricsSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let avg = self.commit_micros_total.checked_div(self.transactions).unwrap_or(0);
        write!(
            f,
            "records={} emitted={} dead_letters={} duplicates={} changelog={} transactions={} aborted={} actors={} commit_avg_us={} commit_max_us={}",
            self.records,
            self.emitted,
            self.dead_letters,
            self.duplicates,
            self.changelog_records,
            self.transactions,
            self.aborted_transactions,
            self.actors,
            avg,
            self.commit_micros_max
        )
    }
}

/// Serves `GET /metrics` (Prometheus text format) on `address` from a
/// background thread. Returns the bound address (useful with port 0).
pub fn serve(
    address: &str,
    metrics: std::sync::Arc<Metrics>,
) -> std::io::Result<(std::net::SocketAddr, std::thread::JoinHandle<()>)> {
    use std::io::{BufRead, BufReader, Write as _};
    let listener = std::net::TcpListener::bind(address)?;
    let bound = listener.local_addr()?;
    let handle = std::thread::Builder::new().name("xstate-metrics".into()).spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut reader = BufReader::new(&stream);
            let mut request_line = String::new();
            if reader.read_line(&mut request_line).is_err() {
                continue;
            }
            // Drain the headers.
            let mut line = String::new();
            while reader.read_line(&mut line).is_ok_and(|n| n > 2) {
                line.clear();
            }
            let path = request_line.split_whitespace().nth(1).unwrap_or("");
            let probe = |ok: bool| if ok { ("200 OK", "ok\n".to_owned()) } else { ("503 Service Unavailable", "not ready\n".to_owned()) };
            let (status, body) = match path {
                "/metrics" => ("200 OK", metrics.snapshot().prometheus()),
                // Liveness: every worker is running.
                "/health" => probe(metrics.healthy()),
                // Readiness: joined the group, nothing restoring.
                "/ready" => probe(metrics.ready()),
                _ => ("404 Not Found", "not found\n".to_owned()),
            };
            let mut stream = &stream;
            let _ = write!(
                stream,
                "HTTP/1.1 {status}\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
        }
    })?;
    Ok((bound, handle))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_and_renders() {
        let m = Metrics::new(2);
        let batch = |records, emitted, dead_letters, changelog, effects, duplicates| BatchCounts {
            records,
            emitted,
            dead_letters,
            changelog,
            effects,
            duplicates,
            ..Default::default()
        };
        m.committed(&batch(10, 3, 1, 4, 0, 2), 500);
        m.committed(&batch(5, 0, 0, 2, 1, 0), 900);
        m.aborted();
        m.set_actors(0, 7);
        m.set_actors(1, 2);
        let s = m.snapshot();
        assert_eq!((s.records, s.transactions, s.commit_micros_max, s.actors), (15, 2, 900, 9));
        let text = s.prometheus();
        assert!(text.contains("xstate_records_total 15\n"), "{text}");
        assert!(text.contains("xstate_duplicates_total 2\n"), "{text}");
        assert!(text.contains("# TYPE xstate_actors gauge\nxstate_actors 9\n"), "{text}");
        assert!(s.to_string().contains("commit_avg_us=700"));
    }

    #[test]
    fn health_readiness_and_lag() {
        let m = Metrics::new(2);
        assert!(!m.healthy() && !m.ready(), "not started");
        m.set_alive(0, true);
        m.set_alive(1, true);
        assert!(m.healthy() && !m.ready(), "not joined");
        m.set_joined(0);
        m.set_joined(1);
        assert!(m.ready());
        m.set_restoring(1, true);
        assert!(!m.ready());
        m.set_restoring(1, false);
        m.set_lag(0, vec![(2, 5), (0, 1)]);
        m.set_lag(1, vec![(1, 0)]);
        let s = m.snapshot();
        assert_eq!((s.lag.clone(), s.lag_total()), (vec![(0, 1), (1, 0), (2, 5)], 6));
        let text = s.prometheus();
        assert!(text.contains("xstate_consumer_lag{partition=\"2\"} 5\n"), "{text}");
        assert!(text.contains("xstate_ready 1\n") && text.contains("xstate_consumer_lag_total 6\n"), "{text}");
        m.set_alive(1, false);
        assert!(!m.healthy() && !m.ready());
    }
}
