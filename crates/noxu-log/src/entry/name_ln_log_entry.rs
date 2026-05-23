//! NameLN log entry for database operations.
//!
//!
//! NameLNLogEntry extends the regular LNLogEntry with additional information
//! about database operations (create, remove, truncate, rename, update config).
//! This is used for replication of database metadata operations.

use super::{DbOperationType, LnLogEntry};
use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use noxu_util::{lsn::Lsn, vlsn::Vlsn};
use std::io::{self, Cursor};
use thiserror::Error;

/// Error type for NameLN log entry operations.
#[derive(Debug, Error)]
pub enum NameLnLogEntryError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("Invalid database operation type: {0}")]
    InvalidOpType(#[from] super::db_operation_type::DbOperationTypeError),
    #[error("LN log entry error: {0}")]
    LnEntry(#[from] super::ln_log_entry::LnLogEntryError),
}

/// NameLN log entry.
///
/// A NameLN is a special LN that maps database names to database IDs and
/// configurations. This log entry extends the basic LNLogEntry with information
/// about the database operation that caused the NameLN to be logged.
///
/// # Fields
///
/// - All fields from LnLogEntry (inherited via composition)
/// - `operation_type`: The type of database operation (create, remove, etc.)
/// - `replicated_create_config`: Database config (for create/update operations)
/// - `truncate_old_db_id`: Old database ID (for truncate operations)
///
/// The `replicated_create_config` field carries a length-prefixed byte
/// serialization of the database configuration (4-byte big-endian length
/// prefix + raw bytes), mirroring `NameLNLogEntry.writeEntry()` /
/// `readEntry()` which writes `replicatedCreateConfig` via
/// `LogUtils.writeByteArray`.  Callers pass a pre-serialized `DatabaseConfig`
/// byte representation.
#[derive(Debug, Clone)]
pub struct NameLnLogEntry {
    /// The underlying LN log entry.
    pub ln_entry: LnLogEntry,
    /// Database operation type.
    pub operation_type: DbOperationType,
    /// Serialized `DatabaseConfig` for CREATE/UPDATE_CONFIG operations.
    ///
    /// On-disk format: 4-byte big-endian length prefix followed by the raw
    /// config bytes.  `None` for operations that do not carry a config
    /// (Remove, Truncate, Rename).
    pub replicated_create_config: Option<Vec<u8>>,
    /// Old database ID (for TRUNCATE operations).
    pub truncate_old_db_id: Option<u64>,
}

impl NameLnLogEntry {
    /// Creates a new NameLN log entry.
    pub fn new(
        ln_entry: LnLogEntry,
        operation_type: DbOperationType,
        replicated_create_config: Option<Vec<u8>>,
        truncate_old_db_id: Option<u64>,
    ) -> Self {
        Self {
            ln_entry,
            operation_type,
            replicated_create_config,
            truncate_old_db_id,
        }
    }

    /// Creates a NameLN entry for a database creation.
    pub fn new_create(
        db_id: u64,
        txn_id: Option<i64>,
        abort_lsn: Lsn,
        key: Vec<u8>,
        data: Vec<u8>,
        config: Vec<u8>,
    ) -> Self {
        let ln_entry = LnLogEntry::new(
            db_id,
            txn_id,
            abort_lsn,
            false, // abort_known_deleted
            None,  // abort_key
            None,  // abort_data
            Vlsn::new(0),
            0, // abort_expiration
            false,
            key,
            Some(data),
            0, // expiration
            Vlsn::new(0),
        );

        Self {
            ln_entry,
            operation_type: DbOperationType::Create,
            replicated_create_config: Some(config),
            truncate_old_db_id: None,
        }
    }

    /// Creates a NameLN entry for a database removal.
    pub fn new_remove(
        db_id: u64,
        txn_id: Option<i64>,
        abort_lsn: Lsn,
        key: Vec<u8>,
    ) -> Self {
        let ln_entry = LnLogEntry::new(
            db_id,
            txn_id,
            abort_lsn,
            false,
            None,
            None,
            Vlsn::new(0),
            0,
            false,
            key,
            None, // Deletion
            0,
            Vlsn::new(0),
        );

        Self {
            ln_entry,
            operation_type: DbOperationType::Remove,
            replicated_create_config: None,
            truncate_old_db_id: None,
        }
    }

    /// Creates a NameLN entry for a database truncation.
    pub fn new_truncate(
        db_id: u64,
        txn_id: Option<i64>,
        abort_lsn: Lsn,
        key: Vec<u8>,
        data: Vec<u8>,
        old_db_id: u64,
    ) -> Self {
        let ln_entry = LnLogEntry::new(
            db_id,
            txn_id,
            abort_lsn,
            false,
            None,
            None,
            Vlsn::new(0),
            0,
            false,
            key,
            Some(data),
            0,
            Vlsn::new(0),
        );

        Self {
            ln_entry,
            operation_type: DbOperationType::Truncate,
            replicated_create_config: None,
            truncate_old_db_id: Some(old_db_id),
        }
    }

    /// Returns the serialized size in bytes.
    pub fn log_size(&self) -> usize {
        let mut size = self.ln_entry.log_size();
        size += DbOperationType::log_size();

        if self.operation_type.is_write_config_type()
            && let Some(ref config) = self.replicated_create_config
        {
            size += 4 + config.len();
        }

        if self.operation_type == DbOperationType::Truncate {
            size += 8; // truncate_old_db_id
        }

        size
    }

    /// Writes this entry to a buffer.
    pub fn write_to_log(&self, buf: &mut BytesMut) {
        // Write the base LN entry
        self.ln_entry.write_to_log(buf);

        // Write operation type
        self.operation_type.write_to_log(buf);

        // Write config if needed
        if self.operation_type.is_write_config_type()
            && let Some(ref config) = self.replicated_create_config
        {
            buf.put_u32(config.len() as u32);
            buf.extend_from_slice(config);
        }

        // Write old db id if truncate
        if self.operation_type == DbOperationType::Truncate {
            buf.put_u64(self.truncate_old_db_id.unwrap_or(0));
        }
    }

    /// Reads an entry from a buffer.
    ///
    /// `is_transactional` must be true when the NameLN was written inside a
    /// transaction (i.e. the outer `LogEntryType` is a `*Txn` variant).
    /// NameLN operations (create, rename, truncate) are typically transactional.
    pub fn read_from_log(
        buf: &[u8],
        is_transactional: bool,
    ) -> Result<Self, NameLnLogEntryError> {
        // Read the base LN entry first
        // We need to track position manually since LnLogEntry consumes variable bytes
        let ln_entry = LnLogEntry::read_from_log(buf, is_transactional)?;

        // Calculate how many bytes the LN entry consumed
        let mut temp_buf = BytesMut::new();
        ln_entry.write_to_log(&mut temp_buf);
        let ln_size = temp_buf.len();

        // Continue reading from after the LN entry
        let mut cursor = Cursor::new(&buf[ln_size..]);

        // Read operation type
        let op_byte_buf = &buf[ln_size..ln_size + 1];
        let operation_type = DbOperationType::read_from_log(op_byte_buf)?;
        cursor.set_position(cursor.position() + 1);

        // Read config if needed
        let replicated_create_config = if operation_type.is_write_config_type()
        {
            let config_len = cursor.read_u32::<BigEndian>()? as usize;
            let mut config = vec![0u8; config_len];
            io::Read::read_exact(&mut cursor, &mut config)?;
            Some(config)
        } else {
            None
        };

        // Read old db id if truncate
        let truncate_old_db_id = if operation_type == DbOperationType::Truncate
        {
            Some(cursor.read_u64::<BigEndian>()?)
        } else {
            None
        };

        Ok(Self {
            ln_entry,
            operation_type,
            replicated_create_config,
            truncate_old_db_id,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_util::lsn::NULL_LSN;

    #[test]
    fn test_name_ln_create_roundtrip() {
        let entry = NameLnLogEntry::new_create(
            100,
            Some(42),
            NULL_LSN,
            b"mydb".to_vec(),
            b"name_ln_data".to_vec(),
            b"db_config_bytes".to_vec(),
        );

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = NameLnLogEntry::read_from_log(&buf, true).unwrap();
        assert_eq!(entry.operation_type, decoded.operation_type);
        assert_eq!(entry.ln_entry.db_id, decoded.ln_entry.db_id);
        assert_eq!(entry.ln_entry.key, decoded.ln_entry.key);
        assert_eq!(
            entry.replicated_create_config,
            decoded.replicated_create_config
        );
    }

    #[test]
    fn test_name_ln_remove_roundtrip() {
        let entry =
            NameLnLogEntry::new_remove(200, None, NULL_LSN, b"olddb".to_vec());

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = NameLnLogEntry::read_from_log(&buf, false).unwrap();
        assert_eq!(entry.operation_type, DbOperationType::Remove);
        assert_eq!(decoded.operation_type, DbOperationType::Remove);
        assert!(decoded.ln_entry.is_deleted());
    }

    #[test]
    fn test_name_ln_truncate_roundtrip() {
        let entry = NameLnLogEntry::new_truncate(
            300,
            Some(99),
            Lsn::new(1, 500),
            b"truncdb".to_vec(),
            b"new_mapping".to_vec(),
            999,
        );

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = NameLnLogEntry::read_from_log(&buf, true).unwrap();
        assert_eq!(entry.operation_type, DbOperationType::Truncate);
        assert_eq!(decoded.truncate_old_db_id, Some(999));
    }

    // ── new() constructor ─────────────────────────────────────────────────────

    #[test]
    fn test_new_constructor() {
        use super::LnLogEntry;
        use noxu_util::vlsn::Vlsn;

        let ln_entry = LnLogEntry::new(
            1,
            None,
            NULL_LSN,
            false,
            None,
            None,
            Vlsn::new(0),
            0,
            false,
            b"key".to_vec(),
            Some(b"data".to_vec()),
            0,
            Vlsn::new(0),
        );

        let entry =
            NameLnLogEntry::new(ln_entry, DbOperationType::Rename, None, None);

        assert_eq!(entry.operation_type, DbOperationType::Rename);
        assert!(entry.replicated_create_config.is_none());
        assert!(entry.truncate_old_db_id.is_none());
    }

    // ── log_size ──────────────────────────────────────────────────────────────

    #[test]
    fn test_log_size_remove_entry() {
        let entry =
            NameLnLogEntry::new_remove(10, None, NULL_LSN, b"db".to_vec());
        let size = entry.log_size();
        // Must be at least ln_entry size + 1 byte for op type.
        assert!(size > 1);
    }

    #[test]
    fn test_log_size_truncate_entry() {
        // Use identical ln_entry base (same db_id, key, data) for both entries
        // so we can isolate the truncate_old_db_id overhead (+8 bytes).
        use super::LnLogEntry;
        use noxu_util::vlsn::Vlsn;

        let make_base_ln = || {
            LnLogEntry::new(
                20,
                None,
                NULL_LSN,
                false,
                None,
                None,
                Vlsn::new(0),
                0,
                false,
                b"db".to_vec(),
                Some(b"data".to_vec()),
                0,
                Vlsn::new(0),
            )
        };

        let entry_trunc = NameLnLogEntry::new(
            make_base_ln(),
            DbOperationType::Truncate,
            None,
            Some(42),
        );
        let entry_remove = NameLnLogEntry::new(
            make_base_ln(),
            DbOperationType::Remove,
            None,
            None,
        );

        // Truncate adds 8 bytes for old_db_id over the same base.
        assert_eq!(entry_trunc.log_size(), entry_remove.log_size() + 8);
    }

    #[test]
    fn test_log_size_create_entry_includes_config() {
        // Build both entries from the same ln_entry base to isolate config overhead.
        use super::LnLogEntry;
        use noxu_util::vlsn::Vlsn;

        let config = b"some_config_bytes".to_vec();

        let make_base_ln = || {
            LnLogEntry::new(
                30,
                None,
                NULL_LSN,
                false,
                None,
                None,
                Vlsn::new(0),
                0,
                false,
                b"db".to_vec(),
                Some(b"data".to_vec()),
                0,
                Vlsn::new(0),
            )
        };

        let entry_create = NameLnLogEntry::new(
            make_base_ln(),
            DbOperationType::Create,
            Some(config.clone()),
            None,
        );
        let entry_remove = NameLnLogEntry::new(
            make_base_ln(),
            DbOperationType::Remove,
            None,
            None,
        );

        // Create adds 4 bytes (length prefix) + config.len() over the same base.
        assert_eq!(
            entry_create.log_size(),
            entry_remove.log_size() + 4 + config.len()
        );
    }

    // ── write_to_log / read_from_log for all operation types ─────────────────

    #[test]
    fn test_rename_operation_roundtrip() {
        use super::LnLogEntry;
        use noxu_util::vlsn::Vlsn;

        let ln_entry = LnLogEntry::new(
            5,
            None,
            NULL_LSN,
            false,
            None,
            None,
            Vlsn::new(0),
            0,
            false,
            b"mydb".to_vec(),
            Some(b"newname".to_vec()),
            0,
            Vlsn::new(0),
        );

        let entry =
            NameLnLogEntry::new(ln_entry, DbOperationType::Rename, None, None);

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = NameLnLogEntry::read_from_log(&buf, false).unwrap();
        assert_eq!(decoded.operation_type, DbOperationType::Rename);
        assert!(decoded.replicated_create_config.is_none());
        assert!(decoded.truncate_old_db_id.is_none());
    }

    #[test]
    fn test_update_config_operation_roundtrip() {
        use super::LnLogEntry;
        use noxu_util::vlsn::Vlsn;

        let config_bytes = b"updated_config".to_vec();
        let ln_entry = LnLogEntry::new(
            6,
            None,
            NULL_LSN,
            false,
            None,
            None,
            Vlsn::new(0),
            0,
            false,
            b"cfgdb".to_vec(),
            Some(b"value".to_vec()),
            0,
            Vlsn::new(0),
        );

        let entry = NameLnLogEntry::new(
            ln_entry,
            DbOperationType::UpdateConfig,
            Some(config_bytes.clone()),
            None,
        );

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = NameLnLogEntry::read_from_log(&buf, false).unwrap();
        assert_eq!(decoded.operation_type, DbOperationType::UpdateConfig);
        assert_eq!(decoded.replicated_create_config, Some(config_bytes));
        assert!(decoded.truncate_old_db_id.is_none());
    }

    #[test]
    fn test_none_operation_roundtrip() {
        use super::LnLogEntry;
        use noxu_util::vlsn::Vlsn;

        let ln_entry = LnLogEntry::new(
            7,
            None,
            NULL_LSN,
            false,
            None,
            None,
            Vlsn::new(0),
            0,
            false,
            b"nonedb".to_vec(),
            None,
            0,
            Vlsn::new(0),
        );

        let entry =
            NameLnLogEntry::new(ln_entry, DbOperationType::None, None, None);

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = NameLnLogEntry::read_from_log(&buf, false).unwrap();
        assert_eq!(decoded.operation_type, DbOperationType::None);
        assert!(decoded.replicated_create_config.is_none());
        assert!(decoded.truncate_old_db_id.is_none());
    }

    // ── field access / debug ──────────────────────────────────────────────────

    #[test]
    fn test_name_ln_log_entry_debug() {
        let entry =
            NameLnLogEntry::new_remove(1, None, NULL_LSN, b"x".to_vec());
        let s = format!("{:?}", entry);
        assert!(s.contains("NameLnLogEntry"));
    }

    #[test]
    fn test_name_ln_log_entry_clone() {
        let original =
            NameLnLogEntry::new_remove(1, None, NULL_LSN, b"y".to_vec());
        let cloned = original.clone();
        assert_eq!(original.operation_type, cloned.operation_type);
        assert_eq!(original.ln_entry.db_id, cloned.ln_entry.db_id);
    }

    #[test]
    fn test_new_create_sets_fields() {
        let entry = NameLnLogEntry::new_create(
            42,
            Some(10),
            NULL_LSN,
            b"createdb".to_vec(),
            b"data".to_vec(),
            b"cfg".to_vec(),
        );

        assert_eq!(entry.operation_type, DbOperationType::Create);
        assert_eq!(entry.ln_entry.db_id, 42);
        assert!(entry.replicated_create_config.is_some());
        assert_eq!(entry.replicated_create_config.unwrap(), b"cfg");
        assert!(entry.truncate_old_db_id.is_none());
    }

    #[test]
    fn test_new_remove_sets_fields() {
        let entry =
            NameLnLogEntry::new_remove(77, Some(5), NULL_LSN, b"rdb".to_vec());

        assert_eq!(entry.operation_type, DbOperationType::Remove);
        assert_eq!(entry.ln_entry.db_id, 77);
        assert!(entry.replicated_create_config.is_none());
        assert!(entry.truncate_old_db_id.is_none());
    }

    #[test]
    fn test_new_truncate_sets_fields() {
        let entry = NameLnLogEntry::new_truncate(
            88,
            None,
            NULL_LSN,
            b"tdb".to_vec(),
            b"val".to_vec(),
            12345,
        );

        assert_eq!(entry.operation_type, DbOperationType::Truncate);
        assert_eq!(entry.ln_entry.db_id, 88);
        assert!(entry.replicated_create_config.is_none());
        assert_eq!(entry.truncate_old_db_id, Some(12345));
    }

    #[test]
    fn test_write_to_log_creates_nonempty_buffer() {
        let entry =
            NameLnLogEntry::new_remove(100, None, NULL_LSN, b"testdb".to_vec());
        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);
        assert!(!buf.is_empty());
        assert_eq!(buf.len(), entry.log_size());
    }
}
