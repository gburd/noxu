//! MapLN  -  Leaf Node that maps database IDs to database metadata.
//!
//!
//! A MapLN stores the configuration and state for a database. MapLNs live
//! in the database mapping tree (DbTree) and map database IDs to their
//! metadata, including whether the database is deleted or transient.

use crate::ln::Ln;

/// A MapLN maps a database ID to its configuration and state.
///
/// It lives in the database mapping tree (DbTree). Each MapLN holds:
/// - An underlying LN containing serialized database configuration
/// - A database ID
/// - Deleted flag (soft delete marker)
/// - Transient flag (for temporary databases)
#[derive(Debug, Clone)]
pub struct MapLn {
    /// The underlying LN holding serialized database config.
    ln: Ln,
    /// Database ID.
    db_id: u64,
    /// Whether the database is deleted.
    deleted: bool,
    /// Whether the database is transient (temporary).
    is_transient: bool,
}

impl MapLn {
    /// Creates a new MapLN for the given database ID with configuration data.
    ///
    /// # Arguments
    ///
    /// * `db_id` - The database ID
    /// * `config_data` - Serialized database configuration
    pub fn new(db_id: u64, config_data: Vec<u8>) -> Self {
        MapLn {
            ln: Ln::new(Some(config_data)),
            db_id,
            deleted: false,
            is_transient: false,
        }
    }

    /// Returns the database ID.
    pub fn get_db_id(&self) -> u64 {
        self.db_id
    }

    /// Returns true if the database is marked as deleted.
    pub fn is_deleted(&self) -> bool {
        self.deleted
    }

    /// Sets the deleted flag.
    pub fn set_deleted(&mut self, deleted: bool) {
        self.deleted = deleted;
        self.ln.set_dirty();
    }

    /// Returns true if the database is transient (temporary).
    pub fn is_transient(&self) -> bool {
        self.is_transient
    }

    /// Sets the transient flag.
    pub fn set_transient(&mut self, transient: bool) {
        self.is_transient = transient;
        self.ln.set_dirty();
    }

    /// Returns a reference to the underlying LN.
    pub fn get_ln(&self) -> &Ln {
        &self.ln
    }

    /// Returns a mutable reference to the underlying LN.
    pub fn get_ln_mut(&mut self) -> &mut Ln {
        &mut self.ln
    }

    /// Returns the serialized size of this MapLN.
    pub fn log_size(&self) -> usize {
        self.ln.log_size() + 8 + 1 + 1 // db_id + deleted flag + transient flag
    }

    /// Writes this MapLN to a byte buffer.
    pub fn write_to_log(&self, buf: &mut Vec<u8>) {
        self.ln.write_to_log(buf);
        buf.extend_from_slice(&self.db_id.to_be_bytes());
        buf.push(if self.deleted { 1 } else { 0 });
        buf.push(if self.is_transient { 1 } else { 0 });
    }

    /// Reads a MapLN from a byte buffer.
    pub fn read_from_log(buf: &[u8]) -> std::io::Result<Self> {
        let ln = Ln::read_from_log(buf)?;
        let ln_size = ln.log_size();
        let remaining = &buf[ln_size..];

        use byteorder::{BigEndian, ReadBytesExt};
        use std::io::Cursor;
        let mut cursor = Cursor::new(remaining);
        let db_id = cursor.read_u64::<BigEndian>()?;
        let deleted = cursor.read_u8()? != 0;
        let is_transient = cursor.read_u8()? != 0;

        Ok(MapLn { ln, db_id, deleted, is_transient })
    }
}

impl std::fmt::Display for MapLn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "<mapln db_id={} deleted={} transient={}>",
            self.db_id, self.deleted, self.is_transient
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_util::Vlsn;

    #[test]
    fn test_map_ln_new() {
        let config = b"database config data".to_vec();
        let map_ln = MapLn::new(42, config.clone());

        assert_eq!(map_ln.get_db_id(), 42);
        assert!(!map_ln.is_deleted());
        assert!(!map_ln.is_transient());
        assert_eq!(map_ln.get_ln().get_data(), Some(config.as_slice()));
    }

    #[test]
    fn test_map_ln_roundtrip() {
        let config = b"test config".to_vec();
        let mut map_ln = MapLn::new(100, config.clone());
        map_ln.get_ln_mut().set_vlsn(Vlsn::new(50));

        let mut buf = Vec::new();
        map_ln.write_to_log(&mut buf);

        let map_ln2 = MapLn::read_from_log(&buf).unwrap();

        assert_eq!(map_ln2.get_db_id(), 100);
        assert!(!map_ln2.is_deleted());
        assert!(!map_ln2.is_transient());
        assert_eq!(map_ln2.get_ln().get_data(), Some(config.as_slice()));
        assert_eq!(map_ln2.get_ln().get_vlsn().sequence(), 50);
    }

    #[test]
    fn test_map_ln_deleted() {
        let config = b"config".to_vec();
        let mut map_ln = MapLn::new(200, config);

        assert!(!map_ln.is_deleted());

        map_ln.set_deleted(true);
        assert!(map_ln.is_deleted());
        assert!(map_ln.get_ln().is_dirty());

        // Round-trip with deleted flag
        let mut buf = Vec::new();
        map_ln.write_to_log(&mut buf);

        let map_ln2 = MapLn::read_from_log(&buf).unwrap();

        assert_eq!(map_ln2.get_db_id(), 200);
        assert!(map_ln2.is_deleted());
        assert!(!map_ln2.is_transient());
    }

    #[test]
    fn test_map_ln_transient() {
        let config = b"transient config".to_vec();
        let mut map_ln = MapLn::new(300, config);

        assert!(!map_ln.is_transient());

        map_ln.set_transient(true);
        assert!(map_ln.is_transient());
        assert!(map_ln.get_ln().is_dirty());

        // Round-trip with transient flag
        let mut buf = Vec::new();
        map_ln.write_to_log(&mut buf);

        let map_ln2 = MapLn::read_from_log(&buf).unwrap();

        assert_eq!(map_ln2.get_db_id(), 300);
        assert!(!map_ln2.is_deleted());
        assert!(map_ln2.is_transient());
    }

    #[test]
    fn test_map_ln_both_flags() {
        let config = b"config".to_vec();
        let mut map_ln = MapLn::new(400, config);

        map_ln.set_deleted(true);
        map_ln.set_transient(true);

        let mut buf = Vec::new();
        map_ln.write_to_log(&mut buf);

        let map_ln2 = MapLn::read_from_log(&buf).unwrap();

        assert_eq!(map_ln2.get_db_id(), 400);
        assert!(map_ln2.is_deleted());
        assert!(map_ln2.is_transient());
    }

    #[test]
    fn test_map_ln_log_size() {
        let config = b"test".to_vec();
        let map_ln = MapLn::new(500, config);

        let size = map_ln.log_size();

        let mut buf = Vec::new();
        map_ln.write_to_log(&mut buf);

        assert_eq!(size, buf.len());
    }

    #[test]
    fn test_map_ln_display() {
        let config = b"config".to_vec();
        let map_ln = MapLn::new(600, config);

        let s = format!("{}", map_ln);
        assert!(s.contains("db_id=600"));
        assert!(s.contains("deleted=false"));
        assert!(s.contains("transient=false"));
    }
}
