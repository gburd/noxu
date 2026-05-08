//! Leaf Node (LN) implementation.
//!
//!
//! An LN represents a Leaf Node in the tree. LNs hold the actual
//! data records (key-value pairs). The key is stored in the parent BIN
//! slot; the LN holds the data (value).

use noxu_util::Vlsn;

/// Dirty flag bit.
const DIRTY_BIT: u32 = 0x80000000;
const CLEAR_DIRTY_BIT: u32 = !DIRTY_BIT;
/// Fetched-cold flag  -  set when LN was fetched but should be evicted soon.
const FETCHED_COLD_BIT: u32 = 0x40000000;

/// A Leaf Node in the B+tree.
///
/// Holds the data (value) for a record. The key is stored in the parent
/// BIN slot, not in the LN itself.
#[derive(Debug, Clone)]
pub struct Ln {
    /// The record data. None means this LN is deleted.
    data: Option<Vec<u8>>,

    /// Transient flags (dirty, fetched_cold). Not persisted.
    flags: u32,

    /// VLSN assigned to this version of the record (for replication).
    vlsn: Vlsn,

    /// Memory size of this LN (for budget tracking).
    memory_size: usize,
}

impl Ln {
    /// Creates a new LN with the given data.
    /// Pass None to create a deleted LN.
    pub fn new(data: Option<Vec<u8>>) -> Self {
        let memory_size = Self::compute_memory_size(&data);
        let mut ln = Ln { data, flags: 0, vlsn: Vlsn::new(0), memory_size };
        ln.set_dirty();
        ln
    }

    /// Creates a new LN from a byte slice (copies the data).
    pub fn from_bytes(data: &[u8]) -> Self {
        Self::new(Some(data.to_vec()))
    }

    /// Creates a deleted LN (no data).
    pub fn new_deleted() -> Self {
        Self::new(None)
    }

    /// Returns the data, or None if deleted.
    pub fn get_data(&self) -> Option<&[u8]> {
        self.data.as_deref()
    }

    /// Sets the data. Pass None to mark as deleted.
    pub fn set_data(&mut self, data: Option<Vec<u8>>) {
        self.memory_size = Self::compute_memory_size(&data);
        self.data = data;
        self.set_dirty();
    }

    /// Returns true if this LN has been deleted.
    pub fn is_deleted(&self) -> bool {
        self.data.is_none()
    }

    /// Returns true if this LN has been modified in memory.
    pub fn is_dirty(&self) -> bool {
        self.flags & DIRTY_BIT != 0
    }

    /// Marks this LN as dirty.
    pub fn set_dirty(&mut self) {
        self.flags |= DIRTY_BIT;
    }

    /// Clears the dirty flag.
    pub fn clear_dirty(&mut self) {
        self.flags &= CLEAR_DIRTY_BIT;
    }

    /// Returns true if this LN was fetched cold.
    pub fn is_fetched_cold(&self) -> bool {
        self.flags & FETCHED_COLD_BIT != 0
    }

    /// Sets the fetched-cold flag.
    pub fn set_fetched_cold(&mut self) {
        self.flags |= FETCHED_COLD_BIT;
    }

    /// Gets the VLSN.
    pub fn get_vlsn(&self) -> Vlsn {
        self.vlsn
    }

    /// Sets the VLSN.
    pub fn set_vlsn(&mut self, vlsn: Vlsn) {
        self.vlsn = vlsn;
    }

    /// Returns the memory size of this LN for budget tracking.
    pub fn get_memory_size(&self) -> usize {
        self.memory_size
    }

    /// Returns memory size included by parent (for BIN memory accounting).
    /// LNs return their full size since they're not on the INList.
    pub fn get_memory_size_included_by_parent(&self) -> usize {
        self.memory_size
    }

    /// Computes memory size for a given data.
    fn compute_memory_size(data: &Option<Vec<u8>>) -> usize {
        // Base overhead for the Ln struct itself
        let base = std::mem::size_of::<Ln>();
        match data {
            Some(d) => base + d.len(),
            None => base,
        }
    }

    // --- Serialization ---

    /// Returns the serialized size.
    pub fn log_size(&self) -> usize {
        let mut size = 1; // deleted flag byte
        if let Some(ref d) = self.data {
            size += 4 + d.len(); // length prefix + data
        }
        size += 8; // vlsn
        size
    }

    /// Writes this LN to a byte buffer.
    pub fn write_to_log(&self, buf: &mut Vec<u8>) {
        if self.is_deleted() {
            buf.push(1); // deleted flag
        } else {
            buf.push(0);
            let data = self.data.as_ref().unwrap();
            buf.extend_from_slice(&(data.len() as u32).to_be_bytes());
            buf.extend_from_slice(data);
        }
        buf.extend_from_slice(&self.vlsn.sequence().to_be_bytes());
    }

    /// Reads an LN from a byte buffer.
    pub fn read_from_log(buf: &[u8]) -> std::io::Result<Self> {
        use byteorder::{BigEndian, ReadBytesExt};
        use std::io::{Cursor, Read};

        let mut cursor = Cursor::new(buf);
        let deleted_flag = cursor.read_u8()?;

        let data = if deleted_flag == 0 {
            let len = cursor.read_u32::<BigEndian>()? as usize;
            let mut data = vec![0u8; len];
            cursor.read_exact(&mut data)?;
            Some(data)
        } else {
            None
        };

        let vlsn_seq = cursor.read_i64::<BigEndian>()?;

        let mut ln =
            Ln { data, flags: 0, vlsn: Vlsn::new(vlsn_seq), memory_size: 0 };
        ln.memory_size = Ln::compute_memory_size(&ln.data);
        Ok(ln)
    }
}

impl std::fmt::Display for Ln {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_deleted() {
            write!(f, "<ln deleted>")
        } else {
            write!(
                f,
                "<ln data_len={}>",
                self.data.as_ref().map_or(0, |d| d.len())
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_ln() {
        let data = b"hello world".to_vec();
        let ln = Ln::new(Some(data.clone()));

        assert!(!ln.is_deleted());
        assert_eq!(ln.get_data(), Some(data.as_slice()));
        assert!(ln.is_dirty());
        assert!(!ln.is_fetched_cold());
    }

    #[test]
    fn test_deleted_ln() {
        let ln = Ln::new_deleted();

        assert!(ln.is_deleted());
        assert_eq!(ln.get_data(), None);
        assert!(ln.is_dirty());
    }

    #[test]
    fn test_dirty_flag() {
        let mut ln = Ln::new(Some(b"test".to_vec()));

        assert!(ln.is_dirty());

        ln.clear_dirty();
        assert!(!ln.is_dirty());

        ln.set_dirty();
        assert!(ln.is_dirty());
    }

    #[test]
    fn test_set_data() {
        let mut ln = Ln::new(Some(b"original".to_vec()));
        ln.clear_dirty();

        let new_data = b"modified".to_vec();
        ln.set_data(Some(new_data.clone()));

        assert_eq!(ln.get_data(), Some(new_data.as_slice()));
        assert!(ln.is_dirty());
    }

    #[test]
    fn test_serialization_roundtrip() {
        let data = b"test data for serialization".to_vec();
        let mut ln = Ln::new(Some(data.clone()));
        ln.set_vlsn(Vlsn::new(42));

        let mut buf = Vec::new();
        ln.write_to_log(&mut buf);

        let ln2 = Ln::read_from_log(&buf).unwrap();

        assert_eq!(ln2.get_data(), Some(data.as_slice()));
        assert_eq!(ln2.get_vlsn().sequence(), 42);
        assert!(!ln2.is_dirty()); // Flags are transient
    }

    #[test]
    fn test_serialization_roundtrip_deleted() {
        let mut ln = Ln::new_deleted();
        ln.set_vlsn(Vlsn::new(99));

        let mut buf = Vec::new();
        ln.write_to_log(&mut buf);

        let ln2 = Ln::read_from_log(&buf).unwrap();

        assert!(ln2.is_deleted());
        assert_eq!(ln2.get_data(), None);
        assert_eq!(ln2.get_vlsn().sequence(), 99);
    }

    #[test]
    fn test_vlsn() {
        let mut ln = Ln::new(Some(b"test".to_vec()));

        assert_eq!(ln.get_vlsn().sequence(), 0);

        ln.set_vlsn(Vlsn::new(12345));
        assert_eq!(ln.get_vlsn().sequence(), 12345);
    }

    #[test]
    fn test_memory_size() {
        let small_ln = Ln::new(Some(b"abc".to_vec()));
        let large_ln = Ln::new(Some(vec![0u8; 1000]));
        let deleted_ln = Ln::new_deleted();

        assert!(small_ln.get_memory_size() > 0);
        assert!(large_ln.get_memory_size() > small_ln.get_memory_size());
        assert!(deleted_ln.get_memory_size() > 0); // Still has base overhead

        // Memory size included by parent should equal total memory size
        assert_eq!(
            small_ln.get_memory_size(),
            small_ln.get_memory_size_included_by_parent()
        );
    }

    #[test]
    fn test_fetched_cold() {
        let mut ln = Ln::new(Some(b"test".to_vec()));

        assert!(!ln.is_fetched_cold());

        ln.set_fetched_cold();
        assert!(ln.is_fetched_cold());
    }

    #[test]
    fn test_from_bytes() {
        let data = b"hello";
        let ln = Ln::from_bytes(data);

        assert_eq!(ln.get_data(), Some(data.as_slice()));
        assert!(!ln.is_deleted());
    }

    #[test]
    fn test_log_size() {
        let ln = Ln::new(Some(b"test".to_vec()));
        let size = ln.log_size();

        // 1 byte deleted flag + 4 bytes length + 4 bytes data + 8 bytes VLSN
        assert_eq!(size, 1 + 4 + 4 + 8);

        let deleted_ln = Ln::new_deleted();
        let deleted_size = deleted_ln.log_size();

        // 1 byte deleted flag + 8 bytes VLSN
        assert_eq!(deleted_size, 1 + 8);
    }

    #[test]
    fn test_display() {
        let ln = Ln::new(Some(b"hello".to_vec()));
        let s = format!("{}", ln);
        assert!(s.contains("data_len=5"));

        let deleted = Ln::new_deleted();
        let s = format!("{}", deleted);
        assert!(s.contains("deleted"));
    }
}
