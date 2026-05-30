//! NameLN  -  Leaf Node that maps database names to database IDs.
//!
//!
//! A NameLN stores the mapping from a database name to its database ID.
//! NameLNs live in the naming tree, where keys are database names and
//! values (stored in the NameLN) are database IDs.

use crate::tree::ln::Ln;

/// A NameLN maps a database name to its database ID.
///
/// Lives in the naming tree. The database name is the key (stored in the
/// parent BIN), and the NameLN holds the database ID as its data.
#[derive(Debug, Clone)]
pub struct NameLn {
    /// The underlying LN.
    ln: Ln,
    /// The database ID this name maps to.
    db_id: u64,
}

impl NameLn {
    /// Creates a new NameLN for the given database ID.
    ///
    /// # Arguments
    ///
    /// * `db_id` - The database ID to map to
    pub fn new(db_id: u64) -> Self {
        // The LN data is the database ID serialized
        let data = db_id.to_be_bytes().to_vec();
        NameLn { ln: Ln::new(Some(data)), db_id }
    }

    /// Returns the database ID.
    pub fn get_db_id(&self) -> u64 {
        self.db_id
    }

    /// Sets the database ID.
    pub fn set_db_id(&mut self, db_id: u64) {
        self.db_id = db_id;
        let data = db_id.to_be_bytes().to_vec();
        self.ln.set_data(Some(data));
    }

    /// Returns a reference to the underlying LN.
    pub fn get_ln(&self) -> &Ln {
        &self.ln
    }

    /// Returns a mutable reference to the underlying LN.
    pub fn get_ln_mut(&mut self) -> &mut Ln {
        &mut self.ln
    }

    /// Returns the serialized size of this NameLN.
    pub fn log_size(&self) -> usize {
        self.ln.log_size() + 8 // db_id
    }

    /// Writes this NameLN to a byte buffer.
    pub fn write_to_log(&self, buf: &mut Vec<u8>) {
        self.ln.write_to_log(buf);
        buf.extend_from_slice(&self.db_id.to_be_bytes());
    }

    /// Reads a NameLN from a byte buffer.
    pub fn read_from_log(buf: &[u8]) -> std::io::Result<Self> {
        let ln = Ln::read_from_log(buf)?;
        let ln_size = ln.log_size();
        let remaining = &buf[ln_size..];

        use byteorder::{BigEndian, ReadBytesExt};
        use std::io::Cursor;
        let mut cursor = Cursor::new(remaining);
        let db_id = cursor.read_u64::<BigEndian>()?;

        Ok(NameLn { ln, db_id })
    }
}

impl std::fmt::Display for NameLn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "<nameln db_id={}>", self.db_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::Vlsn;

    #[test]
    fn test_name_ln_new() {
        let name_ln = NameLn::new(42);

        assert_eq!(name_ln.get_db_id(), 42);

        // Verify the LN data contains the serialized db_id
        let data = name_ln.get_ln().get_data().unwrap();
        assert_eq!(data, &42u64.to_be_bytes());
    }

    #[test]
    fn test_name_ln_roundtrip() {
        let mut name_ln = NameLn::new(12345);
        name_ln.get_ln_mut().set_vlsn(Vlsn::new(100));

        let mut buf = Vec::new();
        name_ln.write_to_log(&mut buf);

        let name_ln2 = NameLn::read_from_log(&buf).unwrap();

        assert_eq!(name_ln2.get_db_id(), 12345);
        assert_eq!(name_ln2.get_ln().get_vlsn().sequence(), 100);

        // Verify the LN data
        let data = name_ln2.get_ln().get_data().unwrap();
        assert_eq!(data, &12345u64.to_be_bytes());
    }

    #[test]
    fn test_name_ln_set_db_id() {
        let mut name_ln = NameLn::new(100);
        assert_eq!(name_ln.get_db_id(), 100);

        name_ln.set_db_id(200);
        assert_eq!(name_ln.get_db_id(), 200);

        // Verify the LN data was updated
        let data = name_ln.get_ln().get_data().unwrap();
        assert_eq!(data, &200u64.to_be_bytes());

        // Verify dirty flag was set
        assert!(name_ln.get_ln().is_dirty());
    }

    #[test]
    fn test_name_ln_log_size() {
        let name_ln = NameLn::new(500);

        let size = name_ln.log_size();

        let mut buf = Vec::new();
        name_ln.write_to_log(&mut buf);

        assert_eq!(size, buf.len());
    }

    #[test]
    fn test_name_ln_display() {
        let name_ln = NameLn::new(999);

        let s = format!("{}", name_ln);
        assert!(s.contains("db_id=999"));
    }

    #[test]
    fn test_name_ln_zero_id() {
        let name_ln = NameLn::new(0);
        assert_eq!(name_ln.get_db_id(), 0);

        let mut buf = Vec::new();
        name_ln.write_to_log(&mut buf);

        let name_ln2 = NameLn::read_from_log(&buf).unwrap();
        assert_eq!(name_ln2.get_db_id(), 0);
    }

    #[test]
    fn test_name_ln_max_id() {
        let name_ln = NameLn::new(u64::MAX);
        assert_eq!(name_ln.get_db_id(), u64::MAX);

        let mut buf = Vec::new();
        name_ln.write_to_log(&mut buf);

        let name_ln2 = NameLn::read_from_log(&buf).unwrap();
        assert_eq!(name_ln2.get_db_id(), u64::MAX);
    }
}
