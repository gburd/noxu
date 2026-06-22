//! RollbackStart log entry.
//!
//! Port of `com.sleepycat.je.txn.RollbackStart`.
//!
//! Written at the start of a replay (HA) rollback to mark the beginning of
//! the recovery period. JE's `RollbackStart` carries the matchpoint VLSN, the
//! matchpoint LSN, and the set of active (unfinished) transaction ids that the
//! syncup is rolling back, plus a debugging timestamp:
//!
//! ```text
//! RollbackStart {
//!     matchpointVLSN : VLSN,
//!     matchpointLSN  : long,
//!     activeTxnIds   : Set<Long>,
//!     time           : Timestamp,
//! }
//! ```
//!
//! The `activeTxnIds` set is consumed at recovery by
//! `RollbackTracker.RollbackPeriod.containsLN(lsn, txnId)` so that a
//! committed/aborted transaction's LNs in the rollback window are *not*
//! reverted (see `RollbackStart.getActiveTxnIds()` and
//! `RollbackTracker.RollbackPeriod`).
//!
//! # On-disk format
//!
//! This is an HA-only, new-format (`LOG_VERSION = 3`) log entry. The
//! `matchpoint_vlsn` and `active_txn_ids` fields were added in v6.3.0 (REP-1);
//! the previous `{active_txn_start, matchpoint_lsn}` shape was never produced
//! by a released non-HA build. See CHANGELOG.

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use noxu_util::{lsn::Lsn, vlsn::Vlsn};
use std::io::{self, Cursor};
use thiserror::Error;

/// Error type for RollbackStart log entry operations.
#[derive(Debug, Error)]
pub enum RollbackStartEntryError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

/// RollbackStart log entry.
///
/// Marks the beginning of an HA rollback. Written before any log entries are
/// rolled back so that recovery can identify the rollback period and the set
/// of transactions being undone.
///
/// # Fields
///
/// - `matchpoint_vlsn`: VLSN of the matchpoint that is the logical start of
///   this rollback period (`RollbackStart.matchpointVLSN`).
/// - `matchpoint_lsn`: LSN of the matchpoint that triggered the rollback
///   (`RollbackStart.matchpointLSN`).
/// - `active_txn_ids`: ids of the unfinished transactions that will be rolled
///   back by syncup (`RollbackStart.activeTxnIds`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollbackStartEntry {
    /// VLSN of the matchpoint (logical start of the rollback period).
    pub matchpoint_vlsn: Vlsn,
    /// LSN of the matchpoint that triggered the rollback.
    pub matchpoint_lsn: Lsn,
    /// Ids of the active (unfinished) transactions being rolled back.
    pub active_txn_ids: Vec<i64>,
}

impl RollbackStartEntry {
    /// Creates a new RollbackStart entry.
    ///
    /// `active_txn_ids` is stored in ascending order so that the serialized
    /// form is deterministic (JE sorts only for `dumpLog`, but a canonical
    /// order keeps round-trip equality stable across the wire).
    pub fn new(
        matchpoint_vlsn: Vlsn,
        matchpoint_lsn: Lsn,
        mut active_txn_ids: Vec<i64>,
    ) -> Self {
        active_txn_ids.sort_unstable();
        active_txn_ids.dedup();
        Self { matchpoint_vlsn, matchpoint_lsn, active_txn_ids }
    }

    /// Returns the serialized size in bytes.
    pub fn log_size(&self) -> usize {
        8 + // matchpoint_vlsn (i64 sequence)
        8 + // matchpoint_lsn
        4 + // active_txn_ids length (u32)
        8 * self.active_txn_ids.len() // each txn id (i64)
    }

    /// Writes this entry to a buffer.
    pub fn write_to_log(&self, buf: &mut BytesMut) {
        buf.put_i64(self.matchpoint_vlsn.sequence());
        buf.put_u64(self.matchpoint_lsn.as_u64());
        buf.put_u32(self.active_txn_ids.len() as u32);
        for &id in &self.active_txn_ids {
            buf.put_i64(id);
        }
    }

    /// Reads an entry from a buffer.
    pub fn read_from_log(buf: &[u8]) -> Result<Self, RollbackStartEntryError> {
        let mut cursor = Cursor::new(buf);
        let matchpoint_vlsn = Vlsn::new(cursor.read_i64::<BigEndian>()?);
        let matchpoint_lsn = Lsn::from_u64(cursor.read_u64::<BigEndian>()?);
        let count = cursor.read_u32::<BigEndian>()? as usize;
        let mut active_txn_ids = Vec::with_capacity(count);
        for _ in 0..count {
            active_txn_ids.push(cursor.read_i64::<BigEndian>()?);
        }
        Ok(Self { matchpoint_vlsn, matchpoint_lsn, active_txn_ids })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_util::lsn::{Lsn, NULL_LSN};
    use noxu_util::vlsn::{NULL_VLSN, Vlsn};

    #[test]
    fn test_rollback_start_roundtrip() {
        let entry = RollbackStartEntry::new(
            Vlsn::new(99),
            Lsn::new(7, 4000),
            vec![10, 20, 30],
        );

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = RollbackStartEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
        assert_eq!(decoded.matchpoint_vlsn, Vlsn::new(99));
        assert_eq!(decoded.matchpoint_lsn, Lsn::new(7, 4000));
        assert_eq!(decoded.active_txn_ids, vec![10, 20, 30]);
    }

    #[test]
    fn test_rollback_start_empty_txn_set() {
        let entry = RollbackStartEntry::new(NULL_VLSN, NULL_LSN, Vec::new());

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = RollbackStartEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
        assert!(decoded.active_txn_ids.is_empty());
    }

    #[test]
    fn test_active_txn_ids_sorted_and_deduped() {
        let entry = RollbackStartEntry::new(
            Vlsn::new(1),
            Lsn::new(1, 100),
            vec![30, 10, 20, 10],
        );
        assert_eq!(entry.active_txn_ids, vec![10, 20, 30]);
    }

    #[test]
    fn test_log_size() {
        let entry = RollbackStartEntry::new(NULL_VLSN, NULL_LSN, vec![1, 2, 3]);
        // 8 (vlsn) + 8 (lsn) + 4 (len) + 3*8 (ids) = 44
        assert_eq!(entry.log_size(), 44);
        assert_eq!(entry.log_size(), buf_size(&entry));
    }

    #[test]
    fn test_log_size_empty() {
        let entry = RollbackStartEntry::new(NULL_VLSN, NULL_LSN, vec![]);
        assert_eq!(entry.log_size(), 20);
        assert_eq!(entry.log_size(), buf_size(&entry));
    }

    fn buf_size(entry: &RollbackStartEntry) -> usize {
        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);
        buf.len()
    }
}
