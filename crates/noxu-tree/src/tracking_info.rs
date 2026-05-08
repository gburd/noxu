//! Tracking information for cleaner and log management.
//!

use noxu_util::Lsn;

/// Tracking info packages tree tracing information for the cleaner.
///
/// Used to track the last logged LSN, size, and VLSN for a BIN slot's LN.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TrackingInfo {
    /// LSN of the last logged version of the LN.
    pub lsn: Lsn,

    /// Node ID of the BIN.
    pub node_id: u64,

    /// Number of entries in the BIN.
    pub entries: usize,

    /// Slot index within the BIN.
    pub index: usize,
}

impl TrackingInfo {
    /// Creates a new TrackingInfo without an index.
    pub fn new(lsn: Lsn, node_id: u64, entries: usize) -> Self {
        Self { lsn, node_id, entries, index: 0 }
    }

    /// Creates a new TrackingInfo with all fields specified.
    pub fn with_index(
        lsn: Lsn,
        node_id: u64,
        entries: usize,
        index: usize,
    ) -> Self {
        Self { lsn, node_id, entries, index }
    }

    /// Sets the slot index.
    pub fn set_index(&mut self, index: usize) {
        self.index = index;
    }
}

impl std::fmt::Display for TrackingInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "lsn={} node={} entries={} index={}",
            self.lsn, self.node_id, self.entries, self.index
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_util::NULL_LSN;

    #[test]
    fn test_new() {
        let info = TrackingInfo::new(Lsn::from_u64(100), 42, 10);
        assert_eq!(info.lsn, Lsn::from_u64(100));
        assert_eq!(info.node_id, 42);
        assert_eq!(info.entries, 10);
        assert_eq!(info.index, 0);
    }

    #[test]
    fn test_with_index() {
        let info = TrackingInfo::with_index(Lsn::from_u64(200), 99, 20, 5);
        assert_eq!(info.lsn, Lsn::from_u64(200));
        assert_eq!(info.node_id, 99);
        assert_eq!(info.entries, 20);
        assert_eq!(info.index, 5);
    }

    #[test]
    fn test_set_index() {
        let mut info = TrackingInfo::new(NULL_LSN, 1, 5);
        assert_eq!(info.index, 0);

        info.set_index(3);
        assert_eq!(info.index, 3);
    }

    #[test]
    fn test_display() {
        let info = TrackingInfo::with_index(
            Lsn::from_u64(0x12340000_00000010),
            42,
            10,
            3,
        );
        let s = info.to_string();
        assert!(s.contains("lsn="));
        assert!(s.contains("node=42"));
        assert!(s.contains("entries=10"));
        assert!(s.contains("index=3"));
    }
}
