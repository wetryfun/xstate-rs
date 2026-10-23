//! Local checkpoints of a partition's actors, so a restore reads a local
//! file and only the tail of the changelog.
//!
//! A checkpoint holds the latest changelog record of every live actor (the
//! same bytes the changelog holds) and `next_offset`: the changelog offset
//! from which records are not yet in the checkpoint. Files are written to a
//! temporary name, synced, and renamed, so a crash leaves the previous
//! checkpoint intact.
//!
//! Format: `XSCK`, version `1` (u8), `next_offset` (i64 LE), record count
//! (u32 LE), then per record: key length (u32 LE), key bytes, record length
//! (u32 LE), record bytes.

use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 4] = b"XSCK";
const VERSION: u8 = 1;

/// A partition's actors as of a changelog offset.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Checkpoint {
    /// The first changelog offset not reflected in `records`.
    pub next_offset: i64,
    /// Key and changelog record of every live actor.
    pub records: Vec<(String, Vec<u8>)>,
}

/// The checkpoint file of a changelog partition in `dir`.
pub fn path(dir: &Path, changelog_topic: &str, partition: i32) -> PathBuf {
    let topic: String =
        changelog_topic.chars().map(|c| if c.is_ascii_alphanumeric() || "-_.".contains(c) { c } else { '_' }).collect();
    dir.join(format!("{topic}-{partition}.checkpoint"))
}

/// Writes `checkpoint` atomically.
pub fn write(path: &Path, checkpoint: &Checkpoint) -> io::Result<()> {
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let tmp = path.with_extension("checkpoint.tmp");
    {
        let file = File::create(&tmp)?;
        let mut w = BufWriter::new(&file);
        w.write_all(MAGIC)?;
        w.write_all(&[VERSION])?;
        w.write_all(&checkpoint.next_offset.to_le_bytes())?;
        w.write_all(&len(checkpoint.records.len())?.to_le_bytes())?;
        for (key, record) in &checkpoint.records {
            w.write_all(&len(key.len())?.to_le_bytes())?;
            w.write_all(key.as_bytes())?;
            w.write_all(&len(record.len())?.to_le_bytes())?;
            w.write_all(record)?;
        }
        w.flush()?;
        drop(w);
        file.sync_all()?;
    }
    std::fs::rename(&tmp, path)?;
    if let Some(dir) = path.parent() {
        // Make the rename durable where the platform allows it.
        if let Ok(d) = File::open(dir) {
            let _ = d.sync_all();
        }
    }
    Ok(())
}

fn len(n: usize) -> io::Result<u32> {
    u32::try_from(n).map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "checkpoint entry too large"))
}

/// Reads a checkpoint; `None` when there is none.
pub fn read(path: &Path) -> io::Result<Option<Checkpoint>> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let mut r = BufReader::new(file);
    let bad = |what: &str| {
        io::Error::new(io::ErrorKind::InvalidData, format!("invalid checkpoint {}: {what}", path.display()))
    };
    let mut magic = [0u8; 4];
    r.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(bad("not a checkpoint"));
    }
    let mut version = [0u8; 1];
    r.read_exact(&mut version)?;
    if version[0] != VERSION {
        return Err(bad("unknown version"));
    }
    let mut offset = [0u8; 8];
    r.read_exact(&mut offset)?;
    let mut u32buf = [0u8; 4];
    let mut read_u32 = |r: &mut BufReader<File>| -> io::Result<u32> {
        r.read_exact(&mut u32buf)?;
        Ok(u32::from_le_bytes(u32buf))
    };
    let count = read_u32(&mut r)?;
    let mut records = Vec::with_capacity(count.min(1 << 20) as usize);
    for _ in 0..count {
        let n = read_u32(&mut r)? as usize;
        let mut key = vec![0u8; n];
        r.read_exact(&mut key)?;
        let key = String::from_utf8(key).map_err(|_| bad("key is not UTF-8"))?;
        let n = read_u32(&mut r)? as usize;
        let mut record = vec![0u8; n];
        r.read_exact(&mut record)?;
        records.push((key, record));
    }
    let mut rest = [0u8; 1];
    if r.read(&mut rest)? != 0 {
        return Err(bad("trailing data"));
    }
    Ok(Some(Checkpoint { next_offset: i64::from_le_bytes(offset), records }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_and_replace() {
        let dir = std::env::temp_dir().join(format!("xstate-ckpt-{}", std::process::id()));
        let p = path(&dir, "orders/changelog", 3);
        assert!(p.ends_with("orders_changelog-3.checkpoint"));
        assert_eq!(read(&p).unwrap(), None);
        let c = Checkpoint { next_offset: 42, records: vec![("a".into(), vec![2, 1]), ("ü".into(), vec![])] };
        write(&p, &c).unwrap();
        assert_eq!(read(&p).unwrap(), Some(c));
        let c2 = Checkpoint { next_offset: 50, records: vec![] };
        write(&p, &c2).unwrap();
        assert_eq!(read(&p).unwrap(), Some(c2));
        std::fs::write(&p, b"nope").unwrap();
        assert!(read(&p).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }
}
