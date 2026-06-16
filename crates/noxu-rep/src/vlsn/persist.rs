//! Persistence for the in-memory VLSN index.
//!
//! Closes finding F11 (the 2026 review).
//!
//! The VLSN index maps VLSNs to log positions (file_number, file_offset) and
//! is rebuilt as entries are received from the master.  Without persistence,
//! a clean shutdown + restart of a replica forces a full network restore
//! because the in-memory mapping is gone.
//!
//! # On-disk format
//!
//! A single file `vlsn.idx` lives in the env home directory with the
//! following little-endian binary layout:
//!
//! ```text
//!   bytes  field
//!   ---------------------------------------------------------
//!   0..4   magic           = b"VIDX"
//!   4..6   version         = u16, currently 1
//!   6..10  bucket_stride   = u32
//!  10..14  entry_count     = u32  (number of (vlsn, file, offset) triples)
//!  14..18  range_first     = u64  (low 32 bits)
//!  18..22  range_first_hi  = u32  (high 32 bits)  -- fits a u64 first vlsn
//!  22..26  range_last_lo   = u32
//!  26..30  range_last_hi   = u32
//!  30..32  reserved        = 2 bytes (pad for 4-byte alignment of body)
//!  32..    body            = entry_count * (u64 vlsn || u32 file || u32 offset)
//!  ...end  crc32           = u32 checksum over bytes [0, end-4)
//! ```
//!
//! The format is intentionally minimal — every entry is `4 + 4 + 8 = 16`
//! bytes, so a 1M-entry index is ~16MiB.  Truncation/overwrite is atomic at
//! the OS level (write-and-rename); if the rename does not complete the old
//! file is preserved.
//!
//! Recovery on load:
//!
//! * Bad magic or version → return `Err`; caller treats as "no persisted
//!   index" and must initiate a network restore.
//! * Truncated body or bad CRC → return `Err`; same handling.
//! * Otherwise, the index is rebuilt entry-by-entry via `put`.

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use noxu_util::Lsn;

use super::vlsn_index::VlsnIndex;

/// File name used for the persisted VLSN index inside the env home.
pub const VLSN_INDEX_FILE: &str = "vlsn.idx";
/// Temp file used during atomic write.
const VLSN_INDEX_TMP_FILE: &str = "vlsn.idx.tmp";

const MAGIC: &[u8; 4] = b"VIDX";
const VERSION: u16 = 1;
const HEADER_LEN: usize = 32;

/// Errors specific to VLSN index persistence.
#[derive(Debug, thiserror::Error)]
pub enum VlsnPersistError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("bad magic in vlsn.idx (got {0:?})")]
    BadMagic([u8; 4]),
    #[error("unsupported vlsn.idx version {0} (this build supports {VERSION})")]
    BadVersion(u16),
    #[error("truncated vlsn.idx (expected {expected} bytes, got {got})")]
    Truncated { expected: usize, got: usize },
    #[error(
        "vlsn.idx checksum mismatch: stored {stored:08x}, computed {computed:08x}"
    )]
    BadChecksum { stored: u32, computed: u32 },
}

pub type Result<T> = std::result::Result<T, VlsnPersistError>;

/// Build the absolute path to the VLSN index file inside `env_home`.
pub fn index_path(env_home: &Path) -> PathBuf {
    env_home.join(VLSN_INDEX_FILE)
}

fn tmp_path(env_home: &Path) -> PathBuf {
    env_home.join(VLSN_INDEX_TMP_FILE)
}

/// Snapshot the index's contents and write them atomically to
/// `env_home/vlsn.idx`.
///
/// The function writes to `vlsn.idx.tmp`, fsyncs, then renames over the
/// final path.  The rename is atomic on POSIX filesystems.
///
/// Returns the number of entries written.
pub fn flush_to_disk(index: &VlsnIndex, env_home: &Path) -> Result<u32> {
    let entries = index.snapshot_entries();
    let range = index.get_range();
    let stride = index.bucket_stride();

    let tmp = tmp_path(env_home);
    let final_path = index_path(env_home);

    // Open with truncate so a partial previous write is overwritten.
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)?;
    let mut w = BufWriter::new(file);

    // Buffer the body so we can compute the CRC over it before flushing
    // to disk.
    let mut buf: Vec<u8> = Vec::with_capacity(HEADER_LEN + entries.len() * 16);
    buf.extend_from_slice(MAGIC);
    buf.extend_from_slice(&VERSION.to_le_bytes());
    buf.extend_from_slice(&stride.to_le_bytes());
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());

    let first = range.get_first();
    let last = range.get_last();
    buf.extend_from_slice(&(first as u32).to_le_bytes());
    buf.extend_from_slice(&((first >> 32) as u32).to_le_bytes());
    buf.extend_from_slice(&(last as u32).to_le_bytes());
    buf.extend_from_slice(&((last >> 32) as u32).to_le_bytes());
    buf.extend_from_slice(&[0u8; 2]); // reserved padding

    debug_assert_eq!(buf.len(), HEADER_LEN);

    for (vlsn, file_no, offset) in &entries {
        buf.extend_from_slice(&vlsn.to_le_bytes());
        buf.extend_from_slice(&file_no.to_le_bytes());
        buf.extend_from_slice(&offset.to_le_bytes());
    }

    let crc = crc32fast::hash(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());

    w.write_all(&buf)?;
    w.flush()?;
    let f = w.into_inner().map_err(|e| io::Error::other(e.to_string()))?;
    f.sync_all()?;
    drop(f);

    std::fs::rename(&tmp, &final_path)?;

    Ok(entries.len() as u32)
}

/// Same as [`flush_to_disk`] but filters out entries whose log position
/// (file_number, file_offset) exceeds `cap_lsn`.
///
/// # X-2 fix — VLSN index persistence tied to checkpoint boundaries
///
/// The periodic VLSN flush daemon calls this variant with the last
/// durable checkpoint's end LSN as `cap_lsn`.  This ensures the
/// persisted index never claims VLSNs beyond the durable B-tree state:
/// after a crash the recovered tree and the VLSN index are coherent.
///
/// If `cap_lsn` is `NULL_LSN` (no checkpoint yet) the function is a
/// no-op and returns `Ok(0)` — there is nothing durable to cap against.
pub fn flush_to_disk_capped(
    index: &VlsnIndex,
    env_home: &Path,
    cap_lsn: Lsn,
) -> Result<u32> {
    use noxu_util::NULL_LSN;
    // If no checkpoint has been completed yet, do not persist — nothing
    // in the tree is durably checkpointed, so there's nothing safe to
    // record.
    if cap_lsn == NULL_LSN {
        return Ok(0);
    }

    // Filter entries whose WAL position (file_no, file_offset) is within
    // the durable checkpoint range.
    let all_entries = index.snapshot_entries();
    let capped: Vec<(u64, u32, u32)> = all_entries
        .into_iter()
        .filter(|(_, file_no, offset)| Lsn::new(*file_no, *offset) <= cap_lsn)
        .collect();

    if capped.is_empty() {
        // Nothing within the checkpoint range — no-op.
        return Ok(0);
    }

    // Recompute the persisted range from the filtered entries.
    let first_vlsn = capped.first().map(|e| e.0).unwrap_or(0);
    let last_vlsn = capped.last().map(|e| e.0).unwrap_or(0);
    let stride = index.bucket_stride();

    let tmp = tmp_path(env_home);
    let final_path = index_path(env_home);

    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&tmp)?;
    let mut w = BufWriter::new(file);

    let mut buf: Vec<u8> = Vec::with_capacity(HEADER_LEN + capped.len() * 16);
    buf.extend_from_slice(MAGIC);
    buf.extend_from_slice(&VERSION.to_le_bytes());
    buf.extend_from_slice(&stride.to_le_bytes());
    buf.extend_from_slice(&(capped.len() as u32).to_le_bytes());

    buf.extend_from_slice(&(first_vlsn as u32).to_le_bytes());
    buf.extend_from_slice(&((first_vlsn >> 32) as u32).to_le_bytes());
    buf.extend_from_slice(&(last_vlsn as u32).to_le_bytes());
    buf.extend_from_slice(&((last_vlsn >> 32) as u32).to_le_bytes());
    buf.extend_from_slice(&[0u8; 2]); // reserved padding

    debug_assert_eq!(buf.len(), HEADER_LEN);

    for (vlsn, file_no, offset) in &capped {
        buf.extend_from_slice(&vlsn.to_le_bytes());
        buf.extend_from_slice(&file_no.to_le_bytes());
        buf.extend_from_slice(&offset.to_le_bytes());
    }

    let crc = crc32fast::hash(&buf);
    buf.extend_from_slice(&crc.to_le_bytes());

    w.write_all(&buf)?;
    w.flush()?;
    let f = w.into_inner().map_err(|e| io::Error::other(e.to_string()))?;
    f.sync_all()?;
    drop(f);

    std::fs::rename(&tmp, &final_path)?;

    Ok(capped.len() as u32)
}

/// Load a VLSN index from `env_home/vlsn.idx`.
///
/// Returns `Ok(None)` if the file does not exist (caller treats as
/// "fresh node, no persisted state"); returns `Err` if the file exists
/// but is corrupt (caller should fall back to network restore and
/// remove the bad file).
pub fn load_from_disk(env_home: &Path) -> Result<Option<VlsnIndex>> {
    let path = index_path(env_home);
    let file = match File::open(&path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };

    let mut r = BufReader::new(file);
    let mut bytes = Vec::new();
    r.read_to_end(&mut bytes)?;

    if bytes.len() < HEADER_LEN + 4 {
        return Err(VlsnPersistError::Truncated {
            expected: HEADER_LEN + 4,
            got: bytes.len(),
        });
    }

    // Verify CRC over [0, len-4).
    let body_end = bytes.len() - 4;
    let mut crc_buf = [0u8; 4];
    crc_buf.copy_from_slice(&bytes[body_end..]);
    let stored_crc = u32::from_le_bytes(crc_buf);
    let computed_crc = crc32fast::hash(&bytes[..body_end]);
    if stored_crc != computed_crc {
        return Err(VlsnPersistError::BadChecksum {
            stored: stored_crc,
            computed: computed_crc,
        });
    }

    let mut magic = [0u8; 4];
    magic.copy_from_slice(&bytes[0..4]);
    if &magic != MAGIC {
        return Err(VlsnPersistError::BadMagic(magic));
    }
    let mut v = [0u8; 2];
    v.copy_from_slice(&bytes[4..6]);
    let version = u16::from_le_bytes(v);
    if version != VERSION {
        return Err(VlsnPersistError::BadVersion(version));
    }

    let mut s = [0u8; 4];
    s.copy_from_slice(&bytes[6..10]);
    let stride = u32::from_le_bytes(s);
    s.copy_from_slice(&bytes[10..14]);
    let entry_count = u32::from_le_bytes(s) as usize;

    // Skip range first/last (re-derived from entries via put()).
    let body_start = HEADER_LEN;
    let body_size = entry_count * 16;
    let want = body_start + body_size + 4;
    if bytes.len() < want {
        return Err(VlsnPersistError::Truncated {
            expected: want,
            got: bytes.len(),
        });
    }

    let stride = if stride == 0 { 10 } else { stride };
    let index = VlsnIndex::new(stride);

    let mut off = body_start;
    let mut buf8 = [0u8; 8];
    let mut buf4 = [0u8; 4];
    for _ in 0..entry_count {
        buf8.copy_from_slice(&bytes[off..off + 8]);
        let vlsn = u64::from_le_bytes(buf8);
        off += 8;
        buf4.copy_from_slice(&bytes[off..off + 4]);
        let file_no = u32::from_le_bytes(buf4);
        off += 4;
        buf4.copy_from_slice(&bytes[off..off + 4]);
        let offset = u32::from_le_bytes(buf4);
        off += 4;
        // Skip the synthetic 0 NULL_VLSN — defensive against stale files.
        if vlsn != 0 {
            index.put(vlsn, file_no, offset);
        }
    }

    Ok(Some(index))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn populate(idx: &VlsnIndex, n: u64) {
        for v in 1..=n {
            idx.put(v, (v / 100) as u32, (v * 10) as u32);
        }
    }

    #[test]
    fn test_load_missing_returns_none() {
        let dir = TempDir::new().unwrap();
        let result = load_from_disk(dir.path()).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_round_trip_small() {
        let dir = TempDir::new().unwrap();
        let idx = VlsnIndex::new(5);
        populate(&idx, 50);
        let written = flush_to_disk(&idx, dir.path()).unwrap();
        assert!(written > 0);

        let loaded = load_from_disk(dir.path()).unwrap().expect("file exists");
        let r = loaded.get_range();
        assert_eq!(r.get_first(), 1);
        assert_eq!(r.get_last(), 50);
        for v in 1..=50u64 {
            assert!(
                loaded.get_lsn(v).is_some(),
                "vlsn {} should round-trip",
                v
            );
        }
    }

    #[test]
    fn test_round_trip_large() {
        let dir = TempDir::new().unwrap();
        let idx = VlsnIndex::new(7);
        populate(&idx, 1000);
        flush_to_disk(&idx, dir.path()).unwrap();

        let loaded = load_from_disk(dir.path()).unwrap().expect("file");
        assert_eq!(loaded.get_latest_vlsn(), 1000);
        // Spot-check several values
        for v in [1u64, 100, 500, 999, 1000] {
            assert!(loaded.get_lsn(v).is_some());
        }
    }

    #[test]
    fn test_atomic_overwrite() {
        let dir = TempDir::new().unwrap();
        let idx1 = VlsnIndex::new(3);
        populate(&idx1, 10);
        flush_to_disk(&idx1, dir.path()).unwrap();

        let idx2 = VlsnIndex::new(3);
        populate(&idx2, 25);
        flush_to_disk(&idx2, dir.path()).unwrap();

        let loaded = load_from_disk(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.get_latest_vlsn(), 25);
    }

    #[test]
    fn test_corrupted_crc_returns_err() {
        let dir = TempDir::new().unwrap();
        let idx = VlsnIndex::new(5);
        populate(&idx, 10);
        flush_to_disk(&idx, dir.path()).unwrap();

        let path = index_path(dir.path());
        let mut bytes = std::fs::read(&path).unwrap();
        // Corrupt the trailing CRC.
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        std::fs::write(&path, bytes).unwrap();

        let result = load_from_disk(dir.path());
        assert!(matches!(result, Err(VlsnPersistError::BadChecksum { .. })));
    }

    #[test]
    fn test_truncated_returns_err() {
        let dir = TempDir::new().unwrap();
        let idx = VlsnIndex::new(5);
        populate(&idx, 10);
        flush_to_disk(&idx, dir.path()).unwrap();

        let path = index_path(dir.path());
        let bytes = std::fs::read(&path).unwrap();
        std::fs::write(&path, &bytes[..bytes.len() - 4]).unwrap();
        // Truncating the CRC also makes the body length inconsistent.
        let result = load_from_disk(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_empty_index_round_trips() {
        let dir = TempDir::new().unwrap();
        let idx = VlsnIndex::new(5);
        flush_to_disk(&idx, dir.path()).unwrap();

        let loaded = load_from_disk(dir.path()).unwrap().unwrap();
        assert_eq!(loaded.get_latest_vlsn(), 0);
        assert!(loaded.get_range().is_empty());
    }

    /// X-2: flush_to_disk_capped must not persist entries whose WAL position
    /// exceeds the checkpoint LSN — this prevents a post-crash VLSN
    /// high-watermark from exceeding the recovered tree state.
    ///
    /// Scenario:
    ///   * VLSNs 1-10 are stored with WAL positions that fall within the
    ///     checkpoint (file=0, offsets 0-90, checkpoint end = (0, 90)).
    ///   * VLSNs 11-20 are stored with WAL positions beyond the checkpoint
    ///     (file=0, offsets 110-200).
    ///   * We flush with cap_lsn = Lsn::new(0, 90).
    ///   * After loading, the index must contain only VLSNs 1-10.
    #[test]
    fn test_x2_flush_capped_excludes_post_checkpoint_entries() {
        let dir = TempDir::new().unwrap();
        let idx = VlsnIndex::new(1); // stride 1 — every VLSN is stored

        // VLSNs 1-10: positions within checkpoint.
        for v in 1u64..=10 {
            idx.put(v, 0, (v * 9) as u32); // offset 9, 18, ..., 90
        }
        // VLSNs 11-20: positions beyond checkpoint.
        for v in 11u64..=20 {
            idx.put(v, 0, (v * 10) as u32); // offset 110, 120, ..., 200
        }
        assert_eq!(
            idx.get_latest_vlsn(),
            20,
            "precondition: 20 VLSNs in index"
        );

        // Checkpoint end is at (file=0, offset=90): covers VLSNs 1-10.
        let cap_lsn = Lsn::new(0, 90);
        let n = flush_to_disk_capped(&idx, dir.path(), cap_lsn).unwrap();
        assert_eq!(n, 10, "only 10 entries within cap should be persisted");

        // After loading, the index high-watermark must be ≤ cap.
        let loaded = load_from_disk(dir.path()).unwrap().unwrap();
        let loaded_latest = loaded.get_latest_vlsn();
        assert!(
            loaded_latest <= 10,
            "X-2: persisted VLSN HWM {} must not exceed checkpointed state (VLSN 10)",
            loaded_latest
        );
        assert_eq!(
            loaded_latest, 10,
            "all 10 capped entries should load cleanly"
        );
    }

    /// X-2: flush_to_disk_capped with NULL_LSN cap is a no-op (returns 0,
    /// writes no file).
    #[test]
    fn test_x2_flush_capped_null_lsn_is_noop() {
        let dir = TempDir::new().unwrap();
        let idx = VlsnIndex::new(1);
        for v in 1u64..=5 {
            idx.put(v, 0, v as u32 * 10);
        }
        let n = flush_to_disk_capped(&idx, dir.path(), noxu_util::NULL_LSN)
            .unwrap();
        assert_eq!(n, 0, "NULL_LSN cap must be a no-op");
        // No file should have been written.
        assert!(
            !index_path(dir.path()).exists(),
            "no vlsn.idx should be written for NULL_LSN cap"
        );
    }
}
