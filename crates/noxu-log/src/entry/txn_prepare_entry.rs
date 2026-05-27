//! Transaction prepare log entry.
//!
//! Written by `Txn::prepare()` (the XA prepare path) so that, after a crash,
//! `noxu-recovery` can identify transactions that completed phase 1 of
//! two-phase commit but did not yet receive `xa_commit` or `xa_rollback`.
//!
//! The recovery layer must:
//!   * NOT undo a transaction whose tail entry is `TxnPrepare` (it is
//!     "in-doubt", waiting for the transaction manager).
//!   * NOT redo its LN entries into the in-memory tree (the prepared writes
//!     are not visible to other transactions until `xa_commit`).
//!   * Surface (xid, txn_id, first_lsn, last_lsn) tuples to the XA layer so
//!     `xa_recover()` can return the in-doubt XIDs to the TM and a
//!     subsequent `xa_commit(xid)` / `xa_rollback(xid)` can resolve them.
//!
//! On-disk format (big-endian, mirroring `TxnEndEntry`):
//!
//! ```text
//!   txn_id              : i64           (8 bytes)
//!   timestamp           : u64           (8 bytes, ms since epoch)
//!   first_lsn           : u64           (8 bytes, first LN logged by this txn)
//!   last_lsn            : u64           (8 bytes, last LN logged before prepare)
//!   xid_format_id       : i32           (4 bytes)
//!   xid_gtrid_len       : u8            (1 byte, 0..=64)
//!   xid_bqual_len       : u8            (1 byte, 0..=64)
//!   xid_gtrid           : [u8; gtrid_len]
//!   xid_bqual           : [u8; bqual_len]
//! ```
//!
//! Fixed prefix is 38 bytes.  Variable suffix is 0..=128 bytes.  The maximum
//! gtrid/bqual lengths (64 each) match `noxu-xa::xid::{MAXGTRIDSIZE,
//! MAXBQUALSIZE}` so well-formed `Xid` values always fit.

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io;
use thiserror::Error;

/// Maximum supported gtrid component (matches `noxu_xa::xid::MAXGTRIDSIZE`).
pub const MAX_GTRID_LEN: u8 = 64;
/// Maximum supported bqual component (matches `noxu_xa::xid::MAXBQUALSIZE`).
pub const MAX_BQUAL_LEN: u8 = 64;

/// Errors deserializing a `TxnPrepareEntry`.
#[derive(Debug, Error)]
pub enum TxnPrepareError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("xid gtrid length {0} exceeds maximum {MAX_GTRID_LEN}")]
    GtridTooLong(u8),
    #[error("xid bqual length {0} exceeds maximum {MAX_BQUAL_LEN}")]
    BqualTooLong(u8),
    #[error("payload truncated at offset {offset}: needed {needed}, got {got}")]
    Truncated { offset: usize, needed: usize, got: usize },
}

/// Transaction prepare entry — written by the XA prepare path before the
/// transaction manager can issue `xa_commit` or `xa_rollback`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxnPrepareEntry {
    /// Transaction ID being prepared.
    pub txn_id: i64,
    /// Wall-clock timestamp (ms since epoch).
    pub timestamp_ms: u64,
    /// LSN of the first LN logged by this transaction (`NULL_LSN` if none).
    ///
    /// Recovery and `xa_commit` use this to bound the WAL scan that replays
    /// the prepared LNs into the in-memory tree at resolution time.
    pub first_lsn: u64,
    /// LSN of the last LN logged before prepare.  Used by recovery to chain
    /// undo records the same way `TxnAbort.last_lsn` does.
    pub last_lsn: u64,
    /// XID format identifier (-1 == null).
    pub xid_format_id: i32,
    /// XID global transaction id (0..=64 bytes).
    pub xid_gtrid: Vec<u8>,
    /// XID branch qualifier (0..=64 bytes).
    pub xid_bqual: Vec<u8>,
}

impl TxnPrepareEntry {
    /// Constructs a new entry.  `gtrid` / `bqual` lengths are validated.
    pub fn new(
        txn_id: i64,
        timestamp_ms: u64,
        first_lsn: u64,
        last_lsn: u64,
        xid_format_id: i32,
        xid_gtrid: Vec<u8>,
        xid_bqual: Vec<u8>,
    ) -> Result<Self, TxnPrepareError> {
        if xid_gtrid.len() > MAX_GTRID_LEN as usize {
            return Err(TxnPrepareError::GtridTooLong(xid_gtrid.len() as u8));
        }
        if xid_bqual.len() > MAX_BQUAL_LEN as usize {
            return Err(TxnPrepareError::BqualTooLong(xid_bqual.len() as u8));
        }
        Ok(Self {
            txn_id,
            timestamp_ms,
            first_lsn,
            last_lsn,
            xid_format_id,
            xid_gtrid,
            xid_bqual,
        })
    }

    /// Returns the serialized size in bytes.
    pub fn log_size(&self) -> usize {
        8 + 8 + 8 + 8 + 4 + 1 + 1 + self.xid_gtrid.len() + self.xid_bqual.len()
    }

    /// Writes this entry to a byte buffer (big-endian, matching `TxnEnd`).
    pub fn write_to_log(&self, buf: &mut Vec<u8>) {
        buf.write_i64::<BigEndian>(self.txn_id).unwrap();
        buf.write_u64::<BigEndian>(self.timestamp_ms).unwrap();
        buf.write_u64::<BigEndian>(self.first_lsn).unwrap();
        buf.write_u64::<BigEndian>(self.last_lsn).unwrap();
        buf.write_i32::<BigEndian>(self.xid_format_id).unwrap();
        buf.write_u8(self.xid_gtrid.len() as u8).unwrap();
        buf.write_u8(self.xid_bqual.len() as u8).unwrap();
        buf.extend_from_slice(&self.xid_gtrid);
        buf.extend_from_slice(&self.xid_bqual);
    }

    /// Reads an entry from a byte buffer.
    pub fn read_from_log(buf: &[u8]) -> Result<Self, TxnPrepareError> {
        let mut cursor = io::Cursor::new(buf);
        let txn_id = cursor.read_i64::<BigEndian>()?;
        let timestamp_ms = cursor.read_u64::<BigEndian>()?;
        let first_lsn = cursor.read_u64::<BigEndian>()?;
        let last_lsn = cursor.read_u64::<BigEndian>()?;
        let xid_format_id = cursor.read_i32::<BigEndian>()?;
        let gtrid_len = cursor.read_u8()?;
        let bqual_len = cursor.read_u8()?;

        if gtrid_len > MAX_GTRID_LEN {
            return Err(TxnPrepareError::GtridTooLong(gtrid_len));
        }
        if bqual_len > MAX_BQUAL_LEN {
            return Err(TxnPrepareError::BqualTooLong(bqual_len));
        }

        let pos = cursor.position() as usize;
        let needed = gtrid_len as usize + bqual_len as usize;
        if buf.len() < pos + needed {
            return Err(TxnPrepareError::Truncated {
                offset: pos,
                needed,
                got: buf.len() - pos,
            });
        }
        let xid_gtrid = buf[pos..pos + gtrid_len as usize].to_vec();
        let xid_bqual = buf[pos + gtrid_len as usize
            ..pos + gtrid_len as usize + bqual_len as usize]
            .to_vec();

        Ok(Self {
            txn_id,
            timestamp_ms,
            first_lsn,
            last_lsn,
            xid_format_id,
            xid_gtrid,
            xid_bqual,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(gtrid: &[u8], bqual: &[u8]) -> TxnPrepareEntry {
        TxnPrepareEntry::new(
            42,
            123_456_789,
            0x1100_2200,
            0x1100_5500,
            7,
            gtrid.to_vec(),
            bqual.to_vec(),
        )
        .unwrap()
    }

    #[test]
    fn new_validates_gtrid_length() {
        let res = TxnPrepareEntry::new(
            1,
            0,
            0,
            0,
            0,
            vec![0u8; 65],
            vec![],
        );
        assert!(matches!(res, Err(TxnPrepareError::GtridTooLong(65))));
    }

    #[test]
    fn new_validates_bqual_length() {
        let res = TxnPrepareEntry::new(
            1,
            0,
            0,
            0,
            0,
            vec![],
            vec![0u8; 65],
        );
        assert!(matches!(res, Err(TxnPrepareError::BqualTooLong(65))));
    }

    #[test]
    fn log_size_fixed_when_xid_empty() {
        let e = entry(b"", b"");
        assert_eq!(e.log_size(), 38);
    }

    #[test]
    fn round_trip_full() {
        let original = entry(b"global_txn_xid", b"branch_42");
        let mut buf = Vec::new();
        original.write_to_log(&mut buf);
        assert_eq!(buf.len(), original.log_size());

        let decoded = TxnPrepareEntry::read_from_log(&buf).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn round_trip_max_xid_lengths() {
        let original = entry(&[0xAB; 64], &[0xCD; 64]);
        let mut buf = Vec::new();
        original.write_to_log(&mut buf);
        assert_eq!(buf.len(), 38 + 128);

        let decoded = TxnPrepareEntry::read_from_log(&buf).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn read_truncated_payload_is_error() {
        let e = entry(b"hello", b"world");
        let mut buf = Vec::new();
        e.write_to_log(&mut buf);
        // Drop the last byte so the bqual region is short.
        buf.pop();
        let result = TxnPrepareEntry::read_from_log(&buf);
        assert!(matches!(result, Err(TxnPrepareError::Truncated { .. })));
    }

    #[test]
    fn read_invalid_gtrid_len_is_error() {
        let mut buf = Vec::new();
        // 8 + 8 + 8 + 8 = 32 bytes of zero header
        buf.resize(32, 0u8);
        // format_id (i32 BE)
        buf.extend_from_slice(&0i32.to_be_bytes());
        // gtrid_len > MAX_GTRID_LEN
        buf.push(200);
        // bqual_len
        buf.push(0);
        let result = TxnPrepareEntry::read_from_log(&buf);
        assert!(matches!(result, Err(TxnPrepareError::GtridTooLong(200))));
    }
}
