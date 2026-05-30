//! Whole entry structure for read operations.
//!
//!
//! This struct packages the log entry header and the log entry contents
//! together for components that need information from both parts.

use crate::log::entry_header::LogEntryHeader;

/// A complete log entry including both header and payload data.
///
/// Used when reading log entries to provide access to both the metadata
/// (in the header) and the deserialized entry contents.
#[derive(Debug)]
pub struct WholeEntry<T> {
    /// The log entry header containing metadata.
    header: LogEntryHeader,

    /// The deserialized log entry contents.
    entry: T,
}

impl<T> WholeEntry<T> {
    /// Creates a new WholeEntry from a header and entry.
    pub fn new(header: LogEntryHeader, entry: T) -> Self {
        WholeEntry { header, entry }
    }

    /// Returns a reference to the header.
    pub fn header(&self) -> &LogEntryHeader {
        &self.header
    }

    /// Returns a reference to the entry.
    pub fn entry(&self) -> &T {
        &self.entry
    }

    /// Returns a mutable reference to the entry.
    pub fn entry_mut(&mut self) -> &mut T {
        &mut self.entry
    }

    /// Consumes the WholeEntry and returns the header and entry.
    pub fn into_parts(self) -> (LogEntryHeader, T) {
        (self.header, self.entry)
    }

    /// Consumes the WholeEntry and returns just the entry.
    pub fn into_entry(self) -> T {
        self.entry
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log::entry_type::LogEntryType;
    use crate::log::provisional::Provisional;

    #[test]
    fn test_whole_entry() {
        let header = LogEntryHeader::new(
            LogEntryType::BIN,
            100,
            Provisional::No,
            false,
            None,
        );
        let entry_data = vec![1u8, 2, 3, 4, 5];

        let whole = WholeEntry::new(header, entry_data.clone());
        assert_eq!(whole.entry(), &entry_data);
        assert_eq!(whole.header().item_size(), 100);

        let (h, e) = whole.into_parts();
        assert_eq!(h.item_size(), 100);
        assert_eq!(e, entry_data);
    }
}
