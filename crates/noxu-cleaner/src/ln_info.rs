//! LN information for cleaning operations.
//!
//! Port of `LNInfo.java` - holds information about an LN entry found during
//! file processing, used for pending LNs and look-ahead caches.

use noxu_util::Lsn;

/// Information about an LN entry found during file processing.
///
/// Used for pending LNs that are locked and must be migrated later, or
/// cannot be migrated immediately during a split. Also used in a look-ahead
/// cache in FileProcessor.
#[derive(Debug, Clone)]
pub struct LnInfo {
    /// The LSN of the LN entry in the log.
    pub lsn: Lsn,

    /// The database ID this LN belongs to.
    pub db_id: i64,

    /// The key for the LN.
    pub key: Vec<u8>,

    /// Whether the LN was found to be obsolete.
    pub obsolete: bool,

    /// Size of the LN entry in the log (in bytes).
    pub log_size: i32,

    /// Whether this is a deleted LN.
    pub deleted: bool,

    /// Expiration time (in milliseconds since epoch, or 0 if no expiration).
    pub expiration_time: u64,
}

impl LnInfo {
    /// Creates a new LN info record.
    pub fn new(
        lsn: Lsn,
        db_id: i64,
        key: Vec<u8>,
        log_size: i32,
        deleted: bool,
        expiration_time: u64,
    ) -> Self {
        Self {
            lsn,
            db_id,
            key,
            obsolete: false,
            log_size,
            deleted,
            expiration_time,
        }
    }

    /// Returns the LSN of this LN entry.
    pub fn lsn(&self) -> Lsn {
        self.lsn
    }

    /// Returns the database ID.
    pub fn db_id(&self) -> i64 {
        self.db_id
    }

    /// Returns a reference to the key.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the size of the entry in the log.
    pub fn log_size(&self) -> i32 {
        self.log_size
    }

    /// Returns whether this LN is deleted.
    pub fn is_deleted(&self) -> bool {
        self.deleted
    }

    /// Returns whether this LN is obsolete.
    pub fn is_obsolete(&self) -> bool {
        self.obsolete
    }

    /// Marks this LN as obsolete.
    pub fn set_obsolete(&mut self, obsolete: bool) {
        self.obsolete = obsolete;
    }

    /// Returns the expiration time.
    pub fn expiration_time(&self) -> u64 {
        self.expiration_time
    }

    /// Returns whether this LN has an expiration time.
    pub fn is_expired(&self, current_time: u64) -> bool {
        self.expiration_time > 0 && self.expiration_time <= current_time
    }

    /// Estimates the memory size of this LN info.
    ///
    /// Includes the key size and fixed overhead, but not the LN data itself
    /// since it may not be resident.
    pub fn memory_size(&self) -> usize {
        // Base overhead: LSN (8) + db_id (8) + log_size (4) + flags (2) + expiration_time (8) + Vec overhead (24)
        let base = 54;
        // Key data
        let key_size = self.key.len();
        base + key_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_ln_info() {
        let lsn = Lsn::new(1, 1000);
        let key = vec![1, 2, 3, 4];
        let info = LnInfo::new(lsn, 42, key.clone(), 128, false, 0);

        assert_eq!(info.lsn(), lsn);
        assert_eq!(info.db_id(), 42);
        assert_eq!(info.key(), &[1, 2, 3, 4]);
        assert_eq!(info.log_size(), 128);
        assert!(!info.is_deleted());
        assert!(!info.is_obsolete());
        assert_eq!(info.expiration_time(), 0);
    }

    #[test]
    fn test_deleted_ln() {
        let lsn = Lsn::new(1, 1000);
        let info = LnInfo::new(lsn, 42, vec![1, 2, 3], 64, true, 0);

        assert!(info.is_deleted());
        assert!(!info.is_obsolete());
    }

    #[test]
    fn test_mark_obsolete() {
        let lsn = Lsn::new(1, 1000);
        let mut info = LnInfo::new(lsn, 42, vec![1, 2, 3], 64, false, 0);

        assert!(!info.is_obsolete());

        info.set_obsolete(true);
        assert!(info.is_obsolete());

        info.set_obsolete(false);
        assert!(!info.is_obsolete());
    }

    #[test]
    fn test_expiration() {
        let lsn = Lsn::new(1, 1000);
        let expiration_time = 1000000;
        let info =
            LnInfo::new(lsn, 42, vec![1, 2, 3], 64, false, expiration_time);

        assert_eq!(info.expiration_time(), expiration_time);
        assert!(!info.is_expired(500000)); // Before expiration
        assert!(info.is_expired(1000000)); // At expiration
        assert!(info.is_expired(1500000)); // After expiration
    }

    #[test]
    fn test_no_expiration() {
        let lsn = Lsn::new(1, 1000);
        let info = LnInfo::new(lsn, 42, vec![1, 2, 3], 64, false, 0);

        assert!(!info.is_expired(1000000)); // Never expires
    }

    #[test]
    fn test_memory_size() {
        let lsn = Lsn::new(1, 1000);
        let small_key = vec![1, 2, 3];
        let large_key = vec![0u8; 1000];

        let info_small = LnInfo::new(lsn, 42, small_key.clone(), 64, false, 0);
        let info_large = LnInfo::new(lsn, 42, large_key.clone(), 64, false, 0);

        let size_small = info_small.memory_size();
        let size_large = info_large.memory_size();

        // Larger key should result in larger memory size
        assert!(size_large > size_small);
        assert_eq!(size_large - size_small, large_key.len() - small_key.len());
    }

    #[test]
    fn test_clone() {
        let lsn = Lsn::new(1, 1000);
        let info = LnInfo::new(lsn, 42, vec![1, 2, 3, 4], 128, false, 5000);

        let cloned = info.clone();

        assert_eq!(cloned.lsn(), info.lsn());
        assert_eq!(cloned.db_id(), info.db_id());
        assert_eq!(cloned.key(), info.key());
        assert_eq!(cloned.log_size(), info.log_size());
        assert_eq!(cloned.is_deleted(), info.is_deleted());
        assert_eq!(cloned.expiration_time(), info.expiration_time());
    }

    #[test]
    fn test_large_database_id() {
        let lsn = Lsn::new(1, 1000);
        let info = LnInfo::new(lsn, i64::MAX, vec![1, 2, 3], 64, false, 0);

        assert_eq!(info.db_id(), i64::MAX);
    }

    #[test]
    fn test_empty_key() {
        let lsn = Lsn::new(1, 1000);
        let info = LnInfo::new(lsn, 42, vec![], 64, false, 0);

        assert_eq!(info.key(), &[]);
        assert!(info.memory_size() > 0); // Still has overhead
    }

    #[test]
    fn test_accessors() {
        let lsn = Lsn::new(5, 12345);
        let key = vec![10, 20, 30];
        let info = LnInfo::new(lsn, 99, key.clone(), 256, true, 88888);

        assert_eq!(info.lsn(), Lsn::new(5, 12345));
        assert_eq!(info.db_id(), 99);
        assert_eq!(info.key(), &[10, 20, 30]);
        assert_eq!(info.log_size(), 256);
        assert!(info.is_deleted());
        assert_eq!(info.expiration_time(), 88888);
    }
}
