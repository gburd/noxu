//! Database entry for keys and data.
//!

/// Encodes database key and data items as byte arrays.
///
/// Both key and data items are represented by DatabaseEntry objects.
/// Key and data byte arrays may refer to arrays of zero length up to
/// arrays of essentially unlimited length.
///
/// 
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseEntry {
    /// The data bytes.
    data: Option<Vec<u8>>,
    /// Offset into the data array.
    offset: usize,
    /// Size of the data.
    size: usize,
    /// Whether this is a partial entry.
    partial: bool,
    /// Offset for partial operations.
    partial_offset: usize,
    /// Length for partial operations.
    partial_length: usize,
}

impl DatabaseEntry {
    /// Creates an empty DatabaseEntry.
    pub fn new() -> Self {
        Self {
            data: None,
            offset: 0,
            size: 0,
            partial: false,
            partial_offset: 0,
            partial_length: 0,
        }
    }

    /// Creates a DatabaseEntry from a byte slice.
    ///
    /// Alias: `from_data` is also available.
    pub fn from_bytes(data: &[u8]) -> Self {
        Self {
            data: Some(data.to_vec()),
            offset: 0,
            size: data.len(),
            partial: false,
            partial_offset: 0,
            partial_length: 0,
        }
    }

    /// Creates a DatabaseEntry from an owned Vec.
    pub fn from_vec(data: Vec<u8>) -> Self {
        let size = data.len();
        Self {
            data: Some(data),
            offset: 0,
            size,
            partial: false,
            partial_offset: 0,
            partial_length: 0,
        }
    }

    /// Gets a reference to the data.
    ///
    /// Returns None if the entry is empty, otherwise returns a slice
    /// from offset to offset+size.
    pub fn get_data(&self) -> Option<&[u8]> {
        self.data.as_ref().map(|d| {
            let start = self.offset.min(d.len());
            let end = (self.offset + self.size).min(d.len());
            &d[start..end]
        })
    }

    /// Sets the data from a byte slice.
    pub fn set_data(&mut self, data: &[u8]) {
        self.data = Some(data.to_vec());
        self.offset = 0;
        self.size = data.len();
    }

    /// Sets the data from an owned Vec.
    pub fn set_data_vec(&mut self, data: Vec<u8>) {
        self.size = data.len();
        self.data = Some(data);
        self.offset = 0;
    }

    /// Creates a DatabaseEntry from a byte slice.
    ///
    /// Alias for `from_bytes`.
    pub fn from_data(data: &[u8]) -> Self {
        Self::from_bytes(data)
    }

    /// Gets the data as a byte slice, returning an empty slice if no data.
    ///
    /// Convenience method that unwraps the Option from `get_data()`.
    pub fn data(&self) -> &[u8] {
        self.get_data().unwrap_or(&[])
    }

    /// Gets the size of the data.
    pub fn get_size(&self) -> usize {
        self.size
    }

    /// Sets the offset within the data array.
    pub fn set_offset(&mut self, offset: usize) {
        self.offset = offset;
    }

    /// Gets the offset within the data array.
    pub fn get_offset(&self) -> usize {
        self.offset
    }

    /// Sets the size of the data.
    pub fn set_size(&mut self, size: usize) {
        self.size = size;
    }

    /// Configures this entry as a partial entry.
    ///
    /// Partial entries are used to read or write only a portion of a record.
    pub fn set_partial(&mut self, offset: usize, length: usize, partial: bool) {
        self.partial = partial;
        self.partial_offset = offset;
        self.partial_length = length;
    }

    /// Returns whether this is a partial entry.
    pub fn is_partial(&self) -> bool {
        self.partial
    }

    /// Gets the partial offset.
    pub fn get_partial_offset(&self) -> usize {
        self.partial_offset
    }

    /// Gets the partial length.
    pub fn get_partial_length(&self) -> usize {
        self.partial_length
    }

    /// Clears the entry, removing all data.
    pub fn clear(&mut self) {
        self.data = None;
        self.offset = 0;
        self.size = 0;
        self.partial = false;
        self.partial_offset = 0;
        self.partial_length = 0;
    }

    /// Returns true if the entry is empty (has no data).
    pub fn is_empty(&self) -> bool {
        self.data.is_none() || self.size == 0
    }
}

impl Default for DatabaseEntry {
    fn default() -> Self {
        Self::new()
    }
}

impl From<Vec<u8>> for DatabaseEntry {
    fn from(data: Vec<u8>) -> Self {
        Self::from_vec(data)
    }
}

impl From<&[u8]> for DatabaseEntry {
    fn from(data: &[u8]) -> Self {
        Self::from_bytes(data)
    }
}

impl From<String> for DatabaseEntry {
    fn from(s: String) -> Self {
        Self::from_vec(s.into_bytes())
    }
}

impl From<&str> for DatabaseEntry {
    fn from(s: &str) -> Self {
        Self::from_bytes(s.as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_empty() {
        let entry = DatabaseEntry::new();
        assert!(entry.is_empty());
        assert_eq!(entry.get_size(), 0);
        assert_eq!(entry.get_data(), None);
    }

    #[test]
    fn test_from_bytes() {
        let data = b"hello";
        let entry = DatabaseEntry::from_bytes(data);
        assert!(!entry.is_empty());
        assert_eq!(entry.get_size(), 5);
        assert_eq!(entry.get_data(), Some(&data[..]));
    }

    #[test]
    fn test_from_vec() {
        let data = vec![1, 2, 3, 4, 5];
        let entry = DatabaseEntry::from_vec(data.clone());
        assert_eq!(entry.get_size(), 5);
        assert_eq!(entry.get_data(), Some(&data[..]));
    }

    #[test]
    fn test_set_data() {
        let mut entry = DatabaseEntry::new();
        entry.set_data(b"test");
        assert_eq!(entry.get_size(), 4);
        assert_eq!(entry.get_data(), Some(b"test".as_ref()));
    }

    #[test]
    fn test_set_data_vec() {
        let mut entry = DatabaseEntry::new();
        let data = vec![10, 20, 30];
        entry.set_data_vec(data.clone());
        assert_eq!(entry.get_size(), 3);
        assert_eq!(entry.get_data(), Some(&data[..]));
    }

    #[test]
    fn test_offset_and_size() {
        let mut entry = DatabaseEntry::from_bytes(b"hello world");
        entry.set_offset(6);
        entry.set_size(5);
        assert_eq!(entry.get_data(), Some(b"world".as_ref()));
    }

    #[test]
    fn test_partial_entry() {
        let mut entry = DatabaseEntry::new();
        assert!(!entry.is_partial());

        entry.set_partial(10, 20, true);
        assert!(entry.is_partial());
        assert_eq!(entry.get_partial_offset(), 10);
        assert_eq!(entry.get_partial_length(), 20);

        entry.set_partial(0, 0, false);
        assert!(!entry.is_partial());
    }

    #[test]
    fn test_clear() {
        let mut entry = DatabaseEntry::from_bytes(b"data");
        assert!(!entry.is_empty());

        entry.clear();
        assert!(entry.is_empty());
        assert_eq!(entry.get_size(), 0);
        assert_eq!(entry.get_data(), None);
    }

    #[test]
    fn test_default() {
        let entry = DatabaseEntry::default();
        assert!(entry.is_empty());
        assert_eq!(entry.get_size(), 0);
    }

    #[test]
    fn test_from_string() {
        let entry = DatabaseEntry::from(String::from("test"));
        assert_eq!(entry.get_data(), Some(b"test".as_ref()));
    }

    #[test]
    fn test_from_str() {
        let entry = DatabaseEntry::from("hello");
        assert_eq!(entry.get_data(), Some(b"hello".as_ref()));
    }

    #[test]
    fn test_clone() {
        let entry1 = DatabaseEntry::from_bytes(b"original");
        let entry2 = entry1.clone();
        assert_eq!(entry1.get_data(), entry2.get_data());
    }

    #[test]
    fn test_equality() {
        let entry1 = DatabaseEntry::from_bytes(b"data");
        let entry2 = DatabaseEntry::from_bytes(b"data");
        let entry3 = DatabaseEntry::from_bytes(b"other");
        assert_eq!(entry1, entry2);
        assert_ne!(entry1, entry3);
    }

    #[test]
    fn test_empty_slice() {
        let entry = DatabaseEntry::from_bytes(b"");
        assert!(entry.is_empty());
        assert_eq!(entry.get_size(), 0);
    }

    #[test]
    fn test_offset_beyond_data() {
        let mut entry = DatabaseEntry::from_bytes(b"short");
        entry.set_offset(10);
        entry.set_size(5);
        // Should return empty slice when offset is beyond data
        assert_eq!(entry.get_data(), Some(&[][..]));
    }

    #[test]
    fn test_size_beyond_data() {
        let mut entry = DatabaseEntry::from_bytes(b"test");
        entry.set_size(100);
        // Should cap at actual data length
        assert_eq!(entry.get_data(), Some(b"test".as_ref()));
    }
}
