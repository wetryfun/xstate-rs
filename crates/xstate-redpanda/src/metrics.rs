//! Service counters, shared by the workers, readable at any time, and
//! renderable in the Prometheus text format.

use std::fmt::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};

/// Counters updated by the workers.
#[derive(Debug)]
pub struct Metrics {
    records: AtomicU64,
    emitted: AtomicU64,
    dead_letters: AtomicU64,
    changelog_records: AtomicU64,
    transactions: AtomicU64,
    aborted_transactions: AtomicU64,
    restored_partitions: AtomicU64,
    restored_actors: AtomicU64,
    restored_records: AtomicU64,
    checkpoints: AtomicU64,
    commit_micros_total: AtomicU64,
    commit_micros_max: AtomicU64,
    /// Live actors, per worker.
    actors: Vec<AtomicU64>,
}

/// A point-in-time copy of [`Metrics`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MetricsSnapshot {
    pub records: u64,
    pub emitted: u64,
    pub dead_letters: u64,
    pub changelog_records: u64,
    pub transactions: u64,
    pub aborted_transactions: u64,
    pub restored_partitions: u64,
    pub restored_actors: u64,
    pub restored_records: u64,
    pub checkpoints: u64,
    pub commit_micros_total: u64,
    pub commit_micros_max: u64,
    pub actors: u64,
}

impl Metrics {
    pub fn new(workers: usize) -> Self {
        let zero = || AtomicU64::new(0);
        Self {
            records: zero(),
            emitted: zero(),
            dead_letters: zero(),
            changelog_records: zero(),
            transactions: zero(),
            aborted_transactions: zero(),
            restored_partitions: zero(),
            restored_actors: zero(),
            restored_records: zero(),
            checkpoints: zero(),
            commit_micros_total: zero(),
            commit_micros_max: zero(),
            actors: (0..workers.max(1)).map(|_| zero()).collect(),
        }
    }

    pub fn committed(&self, records: u64, emitted: u64, dead_letters: u64, changelog: u64, micros: u64) {
        self.records.fetch_add(records, Ordering::Relaxed);
        self.emitted.fetch_add(emitted, Ordering::Relaxed);
        self.dead_letters.fetch_add(dead_letters, Ordering::Relaxed);
        self.changelog_records.fetch_add(changelog, Ordering::Relaxed);
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
            transactions: get(&self.transactions),
            aborted_transactions: get(&self.aborted_transactions),
            restored_partitions: get(&self.restored_partitions),
            restored_actors: get(&self.restored_actors),
            restored_records: get(&self.restored_records),
            checkpoints: get(&self.checkpoints),
            commit_micros_total: get(&self.commit_micros_total),
            commit_micros_max: get(&self.commit_micros_max),
            actors: self.actors.iter().map(get).sum(),
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
        metric("checkpoints_total", "counter", "Local partition checkpoints written.", self.checkpoints);
        metric("commit_micros_total", "counter", "Time spent committing transactions.", self.commit_micros_total);
        metric("commit_micros_max", "gauge", "Longest transaction commit.", self.commit_micros_max);
        metric("actors", "gauge", "Live actors.", self.actors);
        out
    }
}

impl fmt::Display for MetricsSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let avg = self.commit_micros_total.checked_div(self.transactions).unwrap_or(0);
        write!(
            f,
            "records={} emitted={} dead_letters={} changelog={} transactions={} aborted={} actors={} commit_avg_us={} commit_max_us={}",
            self.records,
            self.emitted,
            self.dead_letters,
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
            let (status, body) = if path == "/metrics" {
                ("200 OK", metrics.snapshot().prometheus())
            } else {
                ("404 Not Found", "not found\n".to_owned())
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
        m.committed(10, 3, 1, 4, 500);
        m.committed(5, 0, 0, 2, 900);
        m.aborted();
        m.set_actors(0, 7);
        m.set_actors(1, 2);
        let s = m.snapshot();
        assert_eq!((s.records, s.transactions, s.commit_micros_max, s.actors), (15, 2, 900, 9));
        let text = s.prometheus();
        assert!(text.contains("xstate_records_total 15\n"), "{text}");
        assert!(text.contains("# TYPE xstate_actors gauge\nxstate_actors 9\n"), "{text}");
        assert!(s.to_string().contains("commit_avg_us=700"));
    }
}
