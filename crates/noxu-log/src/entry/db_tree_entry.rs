//! DbTree log entry — BIN-version index written at each checkpoint.
//!
//! The `DbTreeEntry` records the current canonical LSN of every BIN in every
//! database at the moment a `CkptEnd` is written.  This allows recovery to
//! find all BINs (stable *and* dirty) using `read_at_lsn`, without scanning
//! the entire log from LSN 0.
//!
//! ### On-disk format (all multi-byte fields big-endian)
//!
//! ```text
//! checkpoint_id: u64
//! bin_count:     u32
//! [per BIN]
//!   db_id:          u64
//!   node_id:        u64
//!   bin_lsn:        u64   — LSN of the current-version BIN/delta
//!   prev_full_lsn:  u64   — LSN of base full BIN (0 if bin_lsn is a full BIN)
//!   is_delta:       u8    — 1 if bin_lsn points to a BINDelta
//! ```
//!
//! Backward compatibility: old log files that predate Wave GB carry no
//! `DbTree` entry.  Recovery checks `CkptEnd.root_lsn`; when `None` (or
//! when `first_active_lsn == Lsn::new(0,0)`), it falls back to the
//! conservative full-scan path unchanged.
//!
//! Log-version gate: `LOG_VERSION >= 3` (introduced in this wave).

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use noxu_util::Lsn;
use std::io::{self, Cursor};
use thiserror::Error;

/// Error type for DbTree entry serialization.
#[derive(Debug, Error)]
pub enum DbTreeEntryError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("malformed DbTree entry: {0}")]
    Malformed(&'static str),
}

/// A single BIN reference stored in the DbTree index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DbTreeBinRef {
    /// Database ID that owns this BIN.
    pub db_id: u64,
    /// Node ID of the BIN.
    pub node_id: u64,
    /// LSN of the current-version BIN (or BINDelta if `is_delta` is true).
    ///
    /// Recovery calls `read_at_lsn(bin_lsn)` to materialise this BIN.
    pub bin_lsn: Lsn,
    /// LSN of the most recent *full* BIN write.
    ///
    /// When `is_delta` is true, the chain must be walked from `prev_full_lsn`
    /// (base full BIN) through intermediate deltas up to `bin_lsn` to
    /// reconstruct the complete BIN.  When `is_delta` is false this field
    /// equals `bin_lsn` (or is `NULL_LSN` for a brand-new BIN that has only
    /// ever been written as a full BIN at `bin_lsn`).
    pub prev_full_lsn: Lsn,
    /// `true` if `bin_lsn` points to a `BINDelta` entry rather than a full
    /// `BIN` entry.
    pub is_delta: bool,
}

impl DbTreeBinRef {
    /// Serialized size of a single `DbTreeBinRef` (fixed).
    pub const WIRE_SIZE: usize = 8 + 8 + 8 + 8 + 1; // 33 bytes

    /// Write one `DbTreeBinRef` to `buf`.
    pub fn write_to(&self, buf: &mut Vec<u8>) {
        buf.write_u64::<BigEndian>(self.db_id).unwrap();
        buf.write_u64::<BigEndian>(self.node_id).unwrap();
        buf.write_u64::<BigEndian>(self.bin_lsn.as_u64()).unwrap();
        buf.write_u64::<BigEndian>(self.prev_full_lsn.as_u64()).unwrap();
        buf.write_u8(self.is_delta as u8).unwrap();
    }

    /// Read one `DbTreeBinRef` from a cursor.
    pub fn read_from(c: &mut Cursor<&[u8]>) -> Result<Self, DbTreeEntryError> {
        let db_id = c.read_u64::<BigEndian>()?;
        let node_id = c.read_u64::<BigEndian>()?;
        let bin_lsn = Lsn::from_u64(c.read_u64::<BigEndian>()?);
        let prev_full_lsn = Lsn::from_u64(c.read_u64::<BigEndian>()?);
        let is_delta = c.read_u8()? != 0;
        Ok(DbTreeBinRef { db_id, node_id, bin_lsn, prev_full_lsn, is_delta })
    }
}

/// DbTree log entry: the BIN-version index for one checkpoint.
///
/// Written to the WAL just before `CkptEnd` at each checkpoint.
/// The `CkptEnd.root_lsn` field is set to the LSN of this entry so that
/// recovery can locate it.
#[derive(Debug, Clone)]
pub struct DbTreeEntry {
    /// Checkpoint ID (matches the enclosing `CkptEnd`).
    pub checkpoint_id: u64,
    /// The BIN references, one per BIN across all databases.
    pub bins: Vec<DbTreeBinRef>,
}

impl DbTreeEntry {
    /// Create a new `DbTreeEntry`.
    pub fn new(checkpoint_id: u64, bins: Vec<DbTreeBinRef>) -> Self {
        Self { checkpoint_id, bins }
    }

    /// Serialized byte length (for pre-allocation).
    pub fn log_size(&self) -> usize {
        8 // checkpoint_id
        + 4 // bin_count
        + self.bins.len() * DbTreeBinRef::WIRE_SIZE
    }

    /// Serialize to `buf`.
    pub fn write_to_log(&self, buf: &mut Vec<u8>) {
        buf.write_u64::<BigEndian>(self.checkpoint_id).unwrap();
        buf.write_u32::<BigEndian>(self.bins.len() as u32).unwrap();
        for b in &self.bins {
            b.write_to(buf);
        }
    }

    /// Deserialize from `bytes`.
    pub fn read_from_log(bytes: &[u8]) -> Result<Self, DbTreeEntryError> {
        if bytes.len() < 12 {
            return Err(DbTreeEntryError::Malformed("too short"));
        }
        let mut c = Cursor::new(bytes);
        let checkpoint_id = c.read_u64::<BigEndian>()?;
        let bin_count = c.read_u32::<BigEndian>()? as usize;

        let expected_len = 12 + bin_count * DbTreeBinRef::WIRE_SIZE;
        if bytes.len() < expected_len {
            return Err(DbTreeEntryError::Malformed("bin array truncated"));
        }

        let mut bins = Vec::with_capacity(bin_count);
        for _ in 0..bin_count {
            bins.push(DbTreeBinRef::read_from(&mut c)?);
        }
        Ok(Self { checkpoint_id, bins })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_util::NULL_LSN;

    #[test]
    fn test_round_trip_empty() {
        let entry = DbTreeEntry::new(42, vec![]);
        let mut buf = Vec::new();
        entry.write_to_log(&mut buf);
        assert_eq!(buf.len(), entry.log_size());

        let restored = DbTreeEntry::read_from_log(&buf).unwrap();
        assert_eq!(restored.checkpoint_id, 42);
        assert!(restored.bins.is_empty());
    }

    #[test]
    fn test_round_trip_with_bins() {
        let bins = vec![
            DbTreeBinRef {
                db_id: 1,
                node_id: 100,
                bin_lsn: Lsn::new(3, 200),
                prev_full_lsn: Lsn::new(2, 100),
                is_delta: true,
            },
            DbTreeBinRef {
                db_id: 1,
                node_id: 101,
                bin_lsn: Lsn::new(3, 400),
                prev_full_lsn: Lsn::new(3, 400),
                is_delta: false,
            },
            DbTreeBinRef {
                db_id: 2,
                node_id: 50,
                bin_lsn: Lsn::new(1, 50),
                prev_full_lsn: NULL_LSN,
                is_delta: false,
            },
        ];

        let entry = DbTreeEntry::new(7, bins.clone());
        let mut buf = Vec::new();
        entry.write_to_log(&mut buf);
        assert_eq!(buf.len(), entry.log_size());

        let restored = DbTreeEntry::read_from_log(&buf).unwrap();
        assert_eq!(restored.checkpoint_id, 7);
        assert_eq!(restored.bins.len(), 3);
        assert_eq!(restored.bins[0], bins[0]);
        assert_eq!(restored.bins[1], bins[1]);
        assert_eq!(restored.bins[2], bins[2]);
    }

    #[test]
    fn test_wire_size_matches_actual_serialization() {
        let b = DbTreeBinRef {
            db_id: 99,
            node_id: 12345,
            bin_lsn: Lsn::new(5, 1000),
            prev_full_lsn: Lsn::new(4, 500),
            is_delta: true,
        };
        let mut buf = Vec::new();
        b.write_to(&mut buf);
        assert_eq!(buf.len(), DbTreeBinRef::WIRE_SIZE);
    }

    #[test]
    fn test_malformed_too_short() {
        let result = DbTreeEntry::read_from_log(&[0u8; 5]);
        assert!(result.is_err());
    }

    #[test]
    fn test_malformed_bin_count_exceeds_payload() {
        // Claim 100 bins but provide zero bin bytes.
        let mut buf = Vec::new();
        buf.write_u64::<BigEndian>(1).unwrap(); // checkpoint_id
        buf.write_u32::<BigEndian>(100).unwrap(); // bin_count = 100 (no actual data)
        let result = DbTreeEntry::read_from_log(&buf);
        assert!(result.is_err());
    }
}
