//! LN (Leaf Node) log entry.
//!
//! Port of `com.sleepycat.je.log.entry.LNLogEntry`.
//!
//! LNLogEntry is the most common log entry type - it represents a write
//! operation (insert, update, or delete) on a data record. Each LNLogEntry
//! describes a single record modification within a transaction or as a
//! non-transactional operation.

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use noxu_util::{
    lsn::{Lsn, NULL_LSN},
    vlsn::{NULL_VLSN, Vlsn},
};
use std::io::{self, Cursor};
use thiserror::Error;

/// Error type for LN log entry operations.
#[derive(Debug, Error)]
pub enum LnLogEntryError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

/// LN log entry flags.
#[derive(Debug, Clone, Copy)]
struct LnFlags {
    bits: u8,
}

impl LnFlags {
    const ABORT_KD_MASK: u8 = 0x01;
    const EMBEDDED_LN_MASK: u8 = 0x02;
    const HAVE_ABORT_KEY_MASK: u8 = 0x04;
    const HAVE_ABORT_DATA_MASK: u8 = 0x08;
    const HAVE_ABORT_VLSN_MASK: u8 = 0x10;
    const HAVE_ABORT_LSN_MASK: u8 = 0x20;
    const HAVE_ABORT_EXPIRATION_MASK: u8 = 0x40;
    const HAVE_EXPIRATION_MASK: u8 = 0x80;

    fn new() -> Self {
        Self { bits: 0 }
    }

    fn from_bits(bits: u8) -> Self {
        Self { bits }
    }

    fn set_abort_known_deleted(&mut self, val: bool) {
        if val {
            self.bits |= Self::ABORT_KD_MASK;
        }
    }

    fn set_embedded_ln(&mut self, val: bool) {
        if val {
            self.bits |= Self::EMBEDDED_LN_MASK;
        }
    }

    fn set_have_abort_key(&mut self, val: bool) {
        if val {
            self.bits |= Self::HAVE_ABORT_KEY_MASK;
        }
    }

    fn set_have_abort_data(&mut self, val: bool) {
        if val {
            self.bits |= Self::HAVE_ABORT_DATA_MASK;
        }
    }

    fn set_have_abort_vlsn(&mut self, val: bool) {
        if val {
            self.bits |= Self::HAVE_ABORT_VLSN_MASK;
        }
    }

    fn set_have_abort_lsn(&mut self, val: bool) {
        if val {
            self.bits |= Self::HAVE_ABORT_LSN_MASK;
        }
    }

    fn set_have_abort_expiration(&mut self, val: bool) {
        if val {
            self.bits |= Self::HAVE_ABORT_EXPIRATION_MASK;
        }
    }

    fn set_have_expiration(&mut self, val: bool) {
        if val {
            self.bits |= Self::HAVE_EXPIRATION_MASK;
        }
    }

    fn abort_known_deleted(&self) -> bool {
        (self.bits & Self::ABORT_KD_MASK) != 0
    }

    fn embedded_ln(&self) -> bool {
        (self.bits & Self::EMBEDDED_LN_MASK) != 0
    }

    fn have_abort_key(&self) -> bool {
        (self.bits & Self::HAVE_ABORT_KEY_MASK) != 0
    }

    fn have_abort_data(&self) -> bool {
        (self.bits & Self::HAVE_ABORT_DATA_MASK) != 0
    }

    fn have_abort_vlsn(&self) -> bool {
        (self.bits & Self::HAVE_ABORT_VLSN_MASK) != 0
    }

    fn have_abort_lsn(&self) -> bool {
        (self.bits & Self::HAVE_ABORT_LSN_MASK) != 0
    }

    fn have_abort_expiration(&self) -> bool {
        (self.bits & Self::HAVE_ABORT_EXPIRATION_MASK) != 0
    }

    fn have_expiration(&self) -> bool {
        (self.bits & Self::HAVE_EXPIRATION_MASK) != 0
    }
}

/// LN (Leaf Node) log entry.
///
/// Represents a write operation on a data record. This is the most common
/// log entry type in the system.
///
/// # Fields
///
/// - `db_id`: Database ID containing this record
/// - `txn_id`: Transaction ID (None for non-transactional operations)
/// - `abort_lsn`: LSN of the abort version (for rollback)
/// - `abort_known_deleted`: Whether the abort version was deleted
/// - `abort_key`: Key of the abort version (if different due to key updates)
/// - `abort_data`: Data of the abort version (for embedded LNs)
/// - `abort_vlsn`: VLSN of the abort version (for replication)
/// - `abort_expiration`: Expiration time of abort version
/// - `embedded_ln`: Whether the record is embedded in the BIN after this operation
/// - `key`: Record key after this operation
/// - `data`: Record data after this operation (None for deletions)
/// - `expiration`: Expiration time of the record
/// - `vlsn`: VLSN assigned to this log entry (for replication)
///
/// NOTE: Since tree types (LN, BIN) aren't implemented yet, we use Vec<u8>
/// as placeholder for serialized node data.
#[derive(Debug, Clone)]
pub struct LnLogEntry {
    /// Database ID.
    pub db_id: u64,
    /// Transaction ID (None for non-transactional).
    pub txn_id: Option<i64>,
    /// LSN of the abort version.
    pub abort_lsn: Lsn,
    /// Whether abort version was deleted.
    pub abort_known_deleted: bool,
    /// Abort version key (if different).
    pub abort_key: Option<Vec<u8>>,
    /// Abort version data (if embedded).
    pub abort_data: Option<Vec<u8>>,
    /// Abort version VLSN.
    pub abort_vlsn: Vlsn,
    /// Abort expiration time (0 if none).
    pub abort_expiration: i32,
    /// Whether LN is embedded in BIN after this operation.
    pub embedded_ln: bool,
    /// Record key.
    pub key: Vec<u8>,
    /// Record data (None for deletions).
    // TODO(noxu-tree): Replace with actual LN type when available
    pub data: Option<Vec<u8>>,
    /// Expiration time (0 if none).
    pub expiration: i32,
    /// VLSN for replication.
    pub vlsn: Vlsn,
}

impl LnLogEntry {
    /// Creates a new LN log entry for a write operation.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        db_id: u64,
        txn_id: Option<i64>,
        abort_lsn: Lsn,
        abort_known_deleted: bool,
        abort_key: Option<Vec<u8>>,
        abort_data: Option<Vec<u8>>,
        abort_vlsn: Vlsn,
        abort_expiration: i32,
        embedded_ln: bool,
        key: Vec<u8>,
        data: Option<Vec<u8>>,
        expiration: i32,
        vlsn: Vlsn,
    ) -> Self {
        Self {
            db_id,
            txn_id,
            abort_lsn,
            abort_known_deleted,
            abort_key,
            abort_data,
            abort_vlsn,
            abort_expiration,
            embedded_ln,
            key,
            data,
            expiration,
            vlsn,
        }
    }

    /// Returns true if this entry is transactional.
    pub fn is_transactional(&self) -> bool {
        self.txn_id.is_some()
    }

    /// Returns true if this represents a deletion.
    pub fn is_deleted(&self) -> bool {
        self.data.is_none()
    }

    /// Returns the serialized size in bytes.
    pub fn log_size(&self) -> usize {
        let mut size = 1; // flags

        size += 8; // db_id

        if self.is_transactional() {
            if !self.abort_lsn.is_null() {
                size += 8; // abort_lsn
            }
            size += 8; // txn_id
        }

        if let Some(ref k) = self.abort_key {
            size += 4 + k.len();
        }
        if let Some(ref d) = self.abort_data {
            size += 4 + d.len();
        }
        if !self.abort_vlsn.is_null() {
            size += 8;
        }
        if self.abort_expiration != 0 {
            size += 4;
        }

        if self.expiration != 0 {
            size += 4;
        }

        // Data
        if let Some(ref d) = self.data {
            size += 4 + d.len();
        } else {
            size += 4; // Length field even for None
        }

        // Key
        size += 4 + self.key.len();

        size
    }

    /// Writes this entry to a buffer.
    pub fn write_to_log(&self, buf: &mut BytesMut) {
        // Build flags
        let mut flags = LnFlags::new();
        flags.set_abort_known_deleted(self.abort_known_deleted);
        flags.set_embedded_ln(self.embedded_ln);
        flags.set_have_abort_key(self.abort_key.is_some());
        flags.set_have_abort_data(self.abort_data.is_some());
        flags.set_have_abort_vlsn(!self.abort_vlsn.is_null());
        flags.set_have_abort_lsn(!self.abort_lsn.is_null());
        flags.set_have_abort_expiration(self.abort_expiration != 0);
        flags.set_have_expiration(self.expiration != 0);

        buf.put_u8(flags.bits);

        // Database ID
        buf.put_u64(self.db_id);

        // Transactional fields
        if self.is_transactional() {
            if !self.abort_lsn.is_null() {
                buf.put_u64(self.abort_lsn.as_u64());
            }
            buf.put_i64(self.txn_id.unwrap());
        }

        // Abort key/data/vlsn
        if let Some(ref k) = self.abort_key {
            buf.put_u32(k.len() as u32);
            buf.extend_from_slice(k);
        }
        if let Some(ref d) = self.abort_data {
            buf.put_u32(d.len() as u32);
            buf.extend_from_slice(d);
        }
        if !self.abort_vlsn.is_null() {
            buf.put_i64(self.abort_vlsn.sequence());
        }
        if self.abort_expiration != 0 {
            buf.put_i32(self.abort_expiration);
        }

        // Expiration
        if self.expiration != 0 {
            buf.put_i32(self.expiration);
        }

        // Data
        if let Some(ref d) = self.data {
            buf.put_u32(d.len() as u32);
            buf.extend_from_slice(d);
        } else {
            buf.put_u32(0);
        }

        // Key
        buf.put_u32(self.key.len() as u32);
        buf.extend_from_slice(&self.key);
    }

    /// Reads an entry from a buffer.
    ///
    /// `is_transactional` must match the log entry type used when the entry was
    /// written (e.g. `InsertLNTxn` → true, `InsertLN` → false).  JE stores
    /// this information in the outer `LogEntryType` byte, not in the LN payload
    /// flags, so callers must pass it explicitly.
    pub fn read_from_log(
        buf: &[u8],
        is_transactional: bool,
    ) -> Result<Self, LnLogEntryError> {
        let mut cursor = Cursor::new(buf);

        // Read flags
        let flags = LnFlags::from_bits(cursor.read_u8()?);

        // Database ID
        let db_id = cursor.read_u64::<BigEndian>()?;

        // Transactional fields
        let (txn_id, abort_lsn) = if is_transactional {
            let lsn = if flags.have_abort_lsn() {
                Lsn::from_u64(cursor.read_u64::<BigEndian>()?)
            } else {
                NULL_LSN
            };
            let txn = cursor.read_i64::<BigEndian>()?;
            (Some(txn), lsn)
        } else {
            (None, NULL_LSN)
        };

        // Abort key
        let abort_key = if flags.have_abort_key() {
            let len = cursor.read_u32::<BigEndian>()? as usize;
            let mut key = vec![0u8; len];
            io::Read::read_exact(&mut cursor, &mut key)?;
            Some(key)
        } else {
            None
        };

        // Abort data
        let abort_data = if flags.have_abort_data() {
            let len = cursor.read_u32::<BigEndian>()? as usize;
            let mut data = vec![0u8; len];
            io::Read::read_exact(&mut cursor, &mut data)?;
            Some(data)
        } else {
            None
        };

        // Abort VLSN
        let abort_vlsn = if flags.have_abort_vlsn() {
            Vlsn::new(cursor.read_i64::<BigEndian>()?)
        } else {
            NULL_VLSN
        };

        // Abort expiration
        let abort_expiration = if flags.have_abort_expiration() {
            cursor.read_i32::<BigEndian>()?
        } else {
            0
        };

        // Expiration
        let expiration = if flags.have_expiration() {
            cursor.read_i32::<BigEndian>()?
        } else {
            0
        };

        // Data
        let data_len = cursor.read_u32::<BigEndian>()? as usize;
        let data = if data_len > 0 {
            let mut d = vec![0u8; data_len];
            io::Read::read_exact(&mut cursor, &mut d)?;
            Some(d)
        } else {
            None
        };

        // Key
        let key_len = cursor.read_u32::<BigEndian>()? as usize;
        let mut key = vec![0u8; key_len];
        io::Read::read_exact(&mut cursor, &mut key)?;

        Ok(Self {
            db_id,
            txn_id,
            abort_lsn,
            abort_known_deleted: flags.abort_known_deleted(),
            abort_key,
            abort_data,
            abort_vlsn,
            abort_expiration,
            embedded_ln: flags.embedded_ln(),
            key,
            data,
            expiration,
            vlsn: NULL_VLSN, // VLSN comes from entry header, not body
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ln_log_entry_roundtrip_insert() {
        let entry = LnLogEntry::new(
            100,
            Some(42),
            Lsn::new(1, 500),
            false,
            None,
            None,
            NULL_VLSN,
            0,
            true,
            b"mykey".to_vec(),
            Some(b"mydata".to_vec()),
            0,
            Vlsn::new(10),
        );

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = LnLogEntry::read_from_log(&buf, true).unwrap();
        assert_eq!(entry.db_id, decoded.db_id);
        assert_eq!(entry.txn_id, decoded.txn_id);
        assert_eq!(entry.key, decoded.key);
        assert_eq!(entry.data, decoded.data);
        assert_eq!(entry.embedded_ln, decoded.embedded_ln);
    }

    #[test]
    fn test_ln_log_entry_roundtrip_delete() {
        let entry = LnLogEntry::new(
            200,
            None,
            NULL_LSN,
            false,
            None,
            None,
            NULL_VLSN,
            0,
            false,
            b"deletedkey".to_vec(),
            None, // Deletion
            0,
            NULL_VLSN,
        );

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = LnLogEntry::read_from_log(&buf, false).unwrap();
        assert_eq!(entry.db_id, decoded.db_id);
        assert_eq!(entry.key, decoded.key);
        assert!(decoded.is_deleted());
    }

    #[test]
    fn test_ln_log_entry_with_abort_info() {
        let entry = LnLogEntry::new(
            300,
            Some(99),
            Lsn::new(5, 1000),
            true,
            Some(b"oldkey".to_vec()),
            Some(b"olddata".to_vec()),
            Vlsn::new(8),
            123,
            false,
            b"newkey".to_vec(),
            Some(b"newdata".to_vec()),
            456,
            Vlsn::new(20),
        );

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = LnLogEntry::read_from_log(&buf, true).unwrap();
        assert_eq!(entry.abort_lsn, decoded.abort_lsn);
        assert_eq!(entry.abort_known_deleted, decoded.abort_known_deleted);
        assert_eq!(entry.abort_key, decoded.abort_key);
        assert_eq!(entry.abort_data, decoded.abort_data);
    }
}
