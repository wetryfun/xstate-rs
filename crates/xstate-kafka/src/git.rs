//! A Git repository as a checkpoint [`Store`]: the cold path for archive
//! and audit.
//!
//! Every checkpoint of a changelog partition is a commit on the partition's
//! own ref, `<prefix>/<topic>/<partition>` (default prefix `refs/xstate`;
//! characters of the topic other than ASCII letters, digits, `-` and `_`
//! become `_`), whose parent is the previous checkpoint. A commit's tree is
//! readable JSON:
//!
//! - `checkpoint.json`: `{ "topic", "partition", "nextOffset", "actors" }`;
//! - `actors/<key>.json`, one per live (or retired) actor: `{ "key",
//!   "mark"?: { "epoch", "sequence" }, "retiredAt"?, "snapshot"? }`, the
//!   snapshot as XState's `SystemSnapshot` JSON. A record the store cannot
//!   decode (Avro without [`GitStore::decoder`]) is kept as `"record"`, its
//!   bytes in hex. File names are the key with bytes outside `[A-Za-z0-9_-]`
//!   percent-encoded (long keys are cut and suffixed with a hash).
//!
//! So `git log -p refs/xstate/orders-changelog/3 -- actors/order-17.json` is
//! the history of one actor as of each checkpoint. Unchanged actors share
//! their blobs between commits.
//!
//! Commits are written with `git fast-import` (one process per checkpoint,
//! no work tree or index, so workers write different partitions at once),
//! and `git gc --auto` runs after each. With a `remote`, each commit is
//! pushed to the same ref there (a failed push is reported and the commit
//! stays local), and a load first fetches the remote's ref and moves the
//! local one to it when the remote's is ahead, so a node with an empty
//! repository restores from the remote. Partition ownership keeps one
//! writer per ref; if two histories diverge anyway, the local one is kept
//! and its pushes are reported as failed.
//!
//! Needs the `git` command.

use std::fmt;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{json, Map, Value};

use crate::checkpoint::{Checkpoint, Store};
use crate::codec::{self, Codec, Mark};

/// Decodes a snapshot record (after its mark) to JSON, for a codec the store
/// cannot decode by itself (Avro).
pub type Decoder = Arc<dyn Fn(&[u8]) -> Result<Value, String> + Send + Sync>;

/// Checkpoints as commits in a Git repository.
#[derive(Clone)]
pub struct GitStore {
    repository: PathBuf,
    remote: Option<String>,
    prefix: String,
    decoder: Option<Decoder>,
    /// Set once the repository is known to exist.
    ready: Arc<OnceLock<()>>,
}

impl fmt::Debug for GitStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GitStore")
            .field("repository", &self.repository)
            .field("remote", &self.remote)
            .field("prefix", &self.prefix)
            .finish_non_exhaustive()
    }
}

impl GitStore {
    /// The repository at `repository` (any repository, bare or not: only
    /// refs and objects are written). A bare one is created there on first
    /// use if there is none.
    pub fn new(repository: impl Into<PathBuf>) -> Self {
        Self {
            repository: repository.into(),
            remote: None,
            prefix: "refs/xstate".into(),
            decoder: None,
            ready: Arc::new(OnceLock::new()),
        }
    }

    /// [`GitStore::new`], creating the repository now if there is none.
    pub fn open(repository: impl Into<PathBuf>) -> io::Result<Self> {
        let store = Self::new(repository);
        store.ensure()?;
        Ok(store)
    }

    fn ensure(&self) -> io::Result<()> {
        if self.ready.get().is_some() {
            return Ok(());
        }
        if self.git(&["rev-parse", "--git-dir"]).is_err() {
            std::fs::create_dir_all(&self.repository)?;
            let out = Command::new("git").arg("init").arg("--bare").arg("--quiet").arg(&self.repository).output()?;
            check(&out, "git init")?;
        }
        let _ = self.ready.set(());
        Ok(())
    }

    /// Pushes every checkpoint to `remote` (a remote name or URL) and
    /// restores from it when it is ahead.
    pub fn remote(mut self, remote: impl Into<String>) -> Self {
        self.remote = Some(remote.into());
        self
    }

    /// The prefix of the partitions' refs (default `refs/xstate`).
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = prefix.into().trim_end_matches('/').to_owned();
        self
    }

    /// Decodes records the store's own codecs cannot (Avro snapshots).
    pub fn decoder(mut self, decoder: Decoder) -> Self {
        self.decoder = Some(decoder);
        self
    }

    pub fn repository(&self) -> &Path {
        &self.repository
    }

    /// The ref of a changelog partition's checkpoints.
    pub fn ref_name(&self, topic: &str, partition: i32) -> String {
        let topic: String =
            topic.chars().map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' }).collect();
        format!("{}/{topic}/{partition}", self.prefix)
    }

    fn command(&self) -> Command {
        let mut c = Command::new("git");
        c.arg("--git-dir").arg(&self.repository);
        c
    }

    /// Runs git and returns its standard output.
    fn git(&self, args: &[&str]) -> io::Result<Vec<u8>> {
        let out = self.command().args(args).stdin(Stdio::null()).output()?;
        check(&out, &format!("git {}", args.first().unwrap_or(&"")))?;
        Ok(out.stdout)
    }

    /// The commit a ref points to, if it exists.
    fn resolve(&self, name: &str) -> io::Result<Option<String>> {
        let out = self
            .command()
            .args(["rev-parse", "--verify", "--quiet"])
            .arg(format!("{name}^{{commit}}"))
            .stdin(Stdio::null())
            .output()?;
        Ok(out.status.success().then(|| String::from_utf8_lossy(&out.stdout).trim().to_owned()))
    }

    /// The JSON of one actor's record.
    fn actor_json(&self, key: &str, record: &[u8]) -> Value {
        let mut o = Map::new();
        o.insert("key".into(), key.into());
        let (mark, body) = match codec::split_mark(record) {
            Ok(split) => split,
            Err(_) => return raw(o, record),
        };
        if let Some(mark) = mark {
            o.insert("mark".into(), json!({ "epoch": mark.epoch, "sequence": mark.sequence }));
        }
        if body.is_empty() {
            match codec::retired_at(record) {
                Some(at) => o.insert("retiredAt".into(), at.into()),
                None => return raw(o, record),
            };
            return Value::Object(o);
        }
        let decoded = match (body.first(), &self.decoder) {
            (Some(&crate::avro::MAGIC), Some(decode)) => decode(body),
            _ => Codec::decode(body),
        };
        match decoded {
            Ok(snapshot) => {
                o.insert("snapshot".into(), snapshot);
                Value::Object(o)
            }
            Err(_) => raw(o, record),
        }
    }

    /// Fetches the remote's ref and moves the local one to it when the
    /// remote's is ahead (or the local one does not exist).
    fn sync_from_remote(&self, remote: &str, name: &str) -> io::Result<()> {
        let tracking = format!("refs/xstate-remote/{}", name.trim_start_matches("refs/"));
        let out = self
            .command()
            .args(["fetch", "--quiet", "--no-tags", remote])
            .arg(format!("+{name}:{tracking}"))
            .stdin(Stdio::null())
            .output()?;
        if !out.status.success() {
            // Most often the remote has no checkpoint of this partition yet.
            return Ok(());
        }
        let Some(theirs) = self.resolve(&tracking)? else { return Ok(()) };
        let ahead = match self.resolve(name)? {
            None => true,
            Some(ours) if ours == theirs => false,
            Some(ours) => self
                .command()
                .args(["merge-base", "--is-ancestor", &ours, &theirs])
                .stdin(Stdio::null())
                .status()?
                .success(),
        };
        if ahead {
            self.git(&["update-ref", name, &theirs])?;
        }
        Ok(())
    }
}

/// A record kept as hex, without the fields decoded from it.
fn raw(mut o: Map<String, Value>, record: &[u8]) -> Value {
    o.retain(|k, _| k == "key");
    o.insert("record".into(), hex(record).into());
    Value::Object(o)
}

impl Store for GitStore {
    fn save(&self, topic: &str, partition: i32, checkpoint: &Checkpoint) -> io::Result<()> {
        self.ensure()?;
        let name = self.ref_name(topic, partition);
        let parent = self.resolve(&name)?;
        let mut child = self
            .command()
            .args(["fast-import", "--quiet", "--done"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?;
        let mut stdin = child.stdin.take().expect("piped");
        let written = (|| -> io::Result<()> {
            let mut w = io::BufWriter::new(&mut stdin);
            let secs = SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
            let message = format!(
                "Checkpoint {topic}/{partition} at offset {} ({} actors)\n",
                checkpoint.next_offset,
                checkpoint.records.len()
            );
            writeln!(w, "commit {name}")?;
            writeln!(w, "committer xstate-kafka <xstate-kafka@localhost> {secs} +0000")?;
            data(&mut w, message.as_bytes())?;
            if let Some(parent) = &parent {
                writeln!(w, "from {parent}")?;
            }
            writeln!(w, "deleteall")?;
            let summary = json!({
                "topic": topic,
                "partition": partition,
                "nextOffset": checkpoint.next_offset,
                "actors": checkpoint.records.len(),
            });
            file(&mut w, "checkpoint.json", &summary)?;
            for (key, record) in &checkpoint.records {
                file(&mut w, &format!("actors/{}.json", file_name(key)), &self.actor_json(key, record))?;
            }
            writeln!(w, "done")?;
            w.flush()
        })();
        drop(stdin);
        let mut stderr = String::new();
        if let Some(mut e) = child.stderr.take() {
            e.read_to_string(&mut stderr)?;
        }
        let status = child.wait()?;
        written?;
        if !status.success() {
            return Err(io::Error::other(format!("git fast-import failed: {}", stderr.trim())));
        }
        // Packs accumulate, one per checkpoint; `gc --auto` repacks when there
        // are enough of them.
        let _ = self.command().args(["gc", "--auto", "--quiet"]).stdin(Stdio::null()).output();
        if let Some(remote) = &self.remote {
            let out = self
                .command()
                .args(["push", "--quiet", remote])
                .arg(format!("{name}:{name}"))
                .stdin(Stdio::null())
                .output()?;
            if !out.status.success() {
                eprintln!(
                    "xstate-kafka: pushing {name} to {remote} failed (the checkpoint is kept locally): {}",
                    String::from_utf8_lossy(&out.stderr).trim()
                );
            }
        }
        Ok(())
    }

    fn load(&self, topic: &str, partition: i32) -> io::Result<Option<Checkpoint>> {
        self.ensure()?;
        let name = self.ref_name(topic, partition);
        if let Some(remote) = &self.remote {
            self.sync_from_remote(remote, &name)?;
        }
        let Some(commit) = self.resolve(&name)? else { return Ok(None) };
        let tree = self.git(&["ls-tree", "-r", "-z", &commit])?;
        let mut blobs = Vec::new();
        for entry in tree.split(|&b| b == 0).filter(|e| !e.is_empty()) {
            let entry = String::from_utf8_lossy(entry);
            let (meta, path) = entry.split_once('\t').ok_or_else(|| bad("unreadable tree"))?;
            let sha = meta.split(' ').nth(2).ok_or_else(|| bad("unreadable tree"))?;
            blobs.push((path.to_owned(), sha.to_owned()));
        }
        let contents = self.cat_blobs(blobs.iter().map(|(_, sha)| sha.as_str()))?;
        let mut next_offset = None;
        let mut records = Vec::new();
        for ((path, _), content) in blobs.iter().zip(contents) {
            let value: Value = serde_json::from_slice(&content).map_err(|e| bad(&format!("{path}: {e}")))?;
            if path == "checkpoint.json" {
                next_offset = value.get("nextOffset").and_then(Value::as_i64);
            } else if path.starts_with("actors/") {
                records.push(actor_record(&value).map_err(|e| bad(&format!("{path}: {e}")))?);
            }
        }
        let next_offset = next_offset.ok_or_else(|| bad("checkpoint.json has no nextOffset"))?;
        records.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(Some(Checkpoint { next_offset, records }))
    }

    fn location(&self, topic: &str, partition: i32) -> String {
        format!("{} {}", self.repository.display(), self.ref_name(topic, partition))
    }
}

impl GitStore {
    /// The contents of blobs, in order, through one `git cat-file --batch`.
    fn cat_blobs<'a>(&self, shas: impl Iterator<Item = &'a str>) -> io::Result<Vec<Vec<u8>>> {
        let shas: Vec<&str> = shas.collect();
        let mut child =
            self.command().args(["cat-file", "--batch"]).stdin(Stdio::piped()).stdout(Stdio::piped()).spawn()?;
        let mut stdin = child.stdin.take().expect("piped");
        let input = shas.iter().map(|s| format!("{s}\n")).collect::<String>();
        // Write from another thread: git answers while it reads.
        let writer = std::thread::spawn(move || stdin.write_all(input.as_bytes()));
        let mut out = BufReader::new(child.stdout.take().expect("piped"));
        let mut contents = Vec::with_capacity(shas.len());
        for sha in &shas {
            let mut header = String::new();
            out.read_line(&mut header)?;
            let size: usize = header
                .trim_end()
                .rsplit(' ')
                .next()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| bad(&format!("object {sha}: {}", header.trim())))?;
            let mut content = vec![0u8; size + 1];
            out.read_exact(&mut content)?;
            content.pop();
            contents.push(content);
        }
        writer.join().map_err(|_| io::Error::other("cat-file writer panicked"))??;
        child.wait()?;
        Ok(contents)
    }
}

/// The changelog record of an actor's JSON.
fn actor_record(value: &Value) -> Result<(String, Vec<u8>), String> {
    let key = value.get("key").and_then(Value::as_str).ok_or("no key")?.to_owned();
    if let Some(hex) = value.get("record") {
        let bytes = hex.as_str().and_then(unhex).ok_or("record is not hex")?;
        return Ok((key, bytes));
    }
    let mark = match value.get("mark") {
        None => None,
        Some(m) => {
            let field = |f: &str| m.get(f).and_then(Value::as_u64).ok_or(format!("mark.{f} is not a number"));
            Some(Mark { epoch: field("epoch")?, sequence: field("sequence")? })
        }
    };
    let mut record = Vec::new();
    if let Some(at) = value.get("retiredAt") {
        let at = at.as_u64().ok_or("retiredAt is not a number")?;
        codec::write_retired(&mut record, mark.ok_or("a retired actor without a mark")?, at);
        return Ok((key, record));
    }
    let snapshot = value.get("snapshot").ok_or("no snapshot")?;
    if let Some(mark) = mark {
        codec::write_mark(&mut record, mark);
    }
    Codec::Json.encode_into(&mut record, snapshot);
    Ok((key, record))
}

fn data(w: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    writeln!(w, "data {}", bytes.len())?;
    w.write_all(bytes)?;
    writeln!(w)
}

fn file(w: &mut impl Write, path: &str, value: &Value) -> io::Result<()> {
    writeln!(w, "M 100644 inline {path}")?;
    let mut text = serde_json::to_vec_pretty(value).map_err(io::Error::other)?;
    text.push(b'\n');
    data(w, &text)
}

/// A file name for a key: injective, safe on any file system and in
/// fast-import paths.
fn file_name(key: &str) -> String {
    let mut name = String::with_capacity(key.len());
    for b in key.bytes() {
        if b.is_ascii_alphanumeric() || b == b'-' || b == b'_' {
            name.push(b as char);
        } else {
            name.push_str(&format!("%{b:02X}"));
        }
    }
    if name.is_empty() {
        name.push('%');
    }
    if name.len() > 200 {
        // FNV-1a of the whole key keeps cut names apart.
        let hash = key.bytes().fold(0xcbf2_9ce4_8422_2325u64, |h, b| (h ^ u64::from(b)).wrapping_mul(0x100_0000_01b3));
        name.truncate(180);
        name.push_str(&format!("~{hash:016x}"));
    }
    name
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len()).step_by(2).map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok()).collect()
}

fn check(out: &std::process::Output, what: &str) -> io::Result<()> {
    if out.status.success() {
        Ok(())
    } else {
        Err(io::Error::other(format!("{what} failed: {}", String::from_utf8_lossy(&out.stderr).trim())))
    }
}

fn bad(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("invalid Git checkpoint: {what}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("xstate-git-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn records() -> Vec<(String, Vec<u8>)> {
        let snapshot = json!({ "status": "active", "context": { "n": 1.5, "s": "ü" } });
        let mut marked = Vec::new();
        codec::write_mark(&mut marked, Mark { epoch: 2, sequence: 7 });
        Codec::MessagePack.encode_into(&mut marked, &snapshot);
        let mut retired = Vec::new();
        codec::write_retired(&mut retired, Mark { epoch: 0, sequence: 3 }, 1000);
        vec![
            ("a/b c".into(), Codec::Json.encode(&snapshot)),
            ("marked".into(), marked),
            ("old".into(), retired),
            ("weird".into(), vec![crate::avro::MAGIC, 0, 0, 0, 1, 2]),
        ]
    }

    /// Records restore to the same actors: snapshots come back as JSON
    /// records with the same marks, retired marks and raw records exactly.
    fn assert_equivalent(back: &[(String, Vec<u8>)], records: &[(String, Vec<u8>)]) {
        assert_eq!(back.len(), records.len());
        for ((k1, r1), (k2, r2)) in back.iter().zip(records) {
            assert_eq!(k1, k2);
            assert_eq!(codec::split_mark(r1).unwrap().0, codec::split_mark(r2).unwrap().0, "{k1}");
            match Codec::decode(r2) {
                Ok(v) => assert_eq!(Codec::decode(r1).unwrap(), v, "{k1}"),
                Err(_) => assert_eq!(r1, r2, "{k1}"),
            }
        }
    }

    #[test]
    fn commits_readable_checkpoints_and_loads_them() {
        let dir = temp("store");
        let store = GitStore::open(&dir).unwrap();
        assert_eq!(store.load("orders.changelog", 3).unwrap(), None);
        let first = Checkpoint { next_offset: 10, records: records() };
        store.save("orders.changelog", 3, &first).unwrap();
        let back = store.load("orders.changelog", 3).unwrap().unwrap();
        assert_eq!(back.next_offset, 10);
        assert_equivalent(&back.records, &first.records);

        let name = store.ref_name("orders.changelog", 3);
        assert_eq!(name, "refs/xstate/orders_changelog/3");
        let file = store.git(&["show", &format!("{name}:actors/a%2Fb%20c.json")]).unwrap();
        let file: Value = serde_json::from_slice(&file).unwrap();
        assert_eq!(file["snapshot"]["context"]["s"], "ü");
        let file: Value =
            serde_json::from_slice(&store.git(&["show", &format!("{name}:actors/old.json")]).unwrap()).unwrap();
        assert_eq!(file, json!({ "key": "old", "mark": { "epoch": 0, "sequence": 3 }, "retiredAt": 1000 }));

        // A second checkpoint is a child commit; the history stays.
        let second = Checkpoint { next_offset: 25, records: records()[..1].to_vec() };
        store.save("orders.changelog", 3, &second).unwrap();
        let log = store.git(&["log", "--format=%s", &name]).unwrap();
        assert_eq!(
            String::from_utf8(log).unwrap(),
            "Checkpoint orders.changelog/3 at offset 25 (1 actors)\nCheckpoint orders.changelog/3 at offset 10 (4 actors)\n"
        );
        let back = store.load("orders.changelog", 3).unwrap().unwrap();
        assert_eq!(back.next_offset, 25);
        assert_equivalent(&back.records, &second.records);
        // Other partitions are separate refs.
        assert_eq!(store.load("orders.changelog", 4).unwrap(), None);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn a_decoder_reads_other_codecs() {
        let dir = temp("decoder");
        let store = GitStore::open(&dir).unwrap().decoder(Arc::new(|_| Ok(json!({ "decoded": true }))));
        let c = Checkpoint { next_offset: 1, records: records()[3..].to_vec() };
        store.save("t", 0, &c).unwrap();
        let back = store.load("t", 0).unwrap().unwrap();
        assert_eq!(Codec::decode(&back.records[0].1).unwrap(), json!({ "decoded": true }));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn restores_from_a_remote() {
        let remote = temp("remote");
        let (a, b) = (temp("node-a"), temp("node-b"));
        GitStore::open(&remote).unwrap();
        let remote_path = remote.to_str().unwrap().to_owned();
        let node_a = GitStore::open(&a).unwrap().remote(&remote_path);
        let node_b = GitStore::open(&b).unwrap().remote(&remote_path);
        assert_eq!(node_b.load("t", 1).unwrap(), None);
        let c = Checkpoint { next_offset: 5, records: records() };
        node_a.save("t", 1, &c).unwrap();
        // A node with an empty repository restores from the remote, and its
        // next checkpoint continues the same history.
        let back = node_b.load("t", 1).unwrap().unwrap();
        assert_eq!(back.next_offset, 5);
        node_b.save("t", 1, &Checkpoint { next_offset: 9, records: vec![] }).unwrap();
        assert_eq!(node_a.load("t", 1).unwrap().unwrap().next_offset, 9);
        let log = GitStore::open(&remote).unwrap().git(&["rev-list", "--count", "refs/xstate/t/1"]).unwrap();
        assert_eq!(String::from_utf8(log).unwrap().trim(), "2");
        for d in [remote, a, b] {
            std::fs::remove_dir_all(d).ok();
        }
    }

    #[test]
    fn file_names_are_safe_and_distinct() {
        assert_eq!(file_name("order-17"), "order-17");
        assert_eq!(file_name(".."), "%2E%2E");
        assert_eq!(file_name(""), "%");
        let long = "x".repeat(300);
        let mut other = long.clone();
        other.push('y');
        assert!(file_name(&long).len() <= 200);
        assert_ne!(file_name(&long), file_name(&other));
        assert_eq!(unhex(&hex(&[0, 255, 16])).unwrap(), vec![0, 255, 16]);
        assert_eq!(unhex("abc"), None);
    }
}
