//! File protection from deletion during processing.
//!
//! protects log files from deletion while they
//! are being read or processed by various subsystems (backup, replication, etc.).

use hashbrown::HashMap;
use noxu_sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// Sentinel meaning "no replication-protected floor is set" (all files are
/// eligible for deletion as far as replication is concerned).
const NO_FLOOR: u64 = u64::MAX;

/// Protects log files from deletion while they are being read or processed.
///
/// Files can be protected by multiple consumers (backup, disk-ordered cursor,
/// replication feeders, etc.). A file is only safe to delete when its
/// protection count reaches zero.
///
/// Replication additionally installs a *file floor* (`set_replication_floor`):
/// every log file whose number is at or after the floor is protected,
/// regardless of its per-file protection count. This is the Noxu port of JE's
/// `FileProtector` replication-protected range: the master pins every file at
/// or after the file containing the global CBVLSN so the cleaner never deletes
/// a file a lagging replica still needs (`GlobalCBVLSN` / `LocalCBVLSNUpdater`
/// plus `FileProtector`). The floor rises as replicas catch up, releasing
/// older files.
#[derive(Debug)]
pub struct FileProtector {
    /// Map of file_number -> protection information.
    protected_files: Mutex<HashMap<u32, ProtectionInfo>>,

    /// Replication-protected file floor (JE `FileProtector` replication range).
    ///
    /// `NO_FLOOR` means no floor is set. Otherwise every file with number
    /// `>= replication_floor` is protected from deletion because a lagging
    /// replica may still need to read it. Stored as `u64` so `NO_FLOOR` is
    /// representable; the meaningful values are `0..=u32::MAX`.
    replication_floor: AtomicU64,
}

/// Information about why a file is protected.
#[derive(Debug, Clone)]
pub struct ProtectionInfo {
    /// Number of active protections for this file.
    pub count: u32,

    /// Description of the protecting entity (for debugging).
    pub reason: String,
}

impl FileProtector {
    /// Creates a new file protector with no protected files.
    pub fn new() -> Self {
        Self {
            protected_files: Mutex::new(HashMap::new()),
            replication_floor: AtomicU64::new(NO_FLOOR),
        }
    }

    /// Protects a file from deletion.
    ///
    /// Increments the protection count for the given file. The same file
    /// can be protected multiple times by the same or different reasons.
    ///
    /// # Arguments
    /// * `file_number` - The log file number to protect
    /// * `reason` - Description of why this file is protected (e.g., "Backup", "Feeder:node1")
    pub fn protect_file(&self, file_number: u32, reason: &str) {
        let mut protected = self.protected_files.lock();

        protected
            .entry(file_number)
            .and_modify(|info| {
                info.count += 1;
                // Update reason to reflect multiple protections
                if !info.reason.contains(reason) {
                    info.reason = format!("{}, {}", info.reason, reason);
                }
            })
            .or_insert_with(|| ProtectionInfo {
                count: 1,
                reason: reason.to_string(),
            });
    }

    /// Removes one level of protection from a file.
    ///
    /// Decrements the protection count for the given file. When the count
    /// reaches zero, the file is no longer protected and can be deleted.
    ///
    /// # Arguments
    /// * `file_number` - The log file number to unprotect
    ///
    /// # Returns
    /// `true` if the file is no longer protected, `false` if it still has active protections
    pub fn unprotect_file(&self, file_number: u32) -> bool {
        let mut protected = self.protected_files.lock();

        if let Some(info) = protected.get_mut(&file_number) {
            if info.count > 1 {
                info.count -= 1;
                return false;
            } else {
                protected.remove(&file_number);
                return true;
            }
        }

        // File wasn't protected - this is a no-op
        true
    }

    /// Returns whether a file is currently protected.
    ///
    /// A file is protected if it has a nonzero per-file protection count OR
    /// it lies at or after the replication-protected file floor (see
    /// [`set_replication_floor`](Self::set_replication_floor)).
    pub fn is_protected(&self, file_number: u32) -> bool {
        if self.is_replication_protected(file_number) {
            return true;
        }
        self.protected_files.lock().contains_key(&file_number)
    }

    /// Sets the replication-protected file floor.
    ///
    /// `Some(f)`: every log file with number `>= f` is protected from
    /// deletion (a lagging replica may still need it). `None`: clears the
    /// floor (no file is replication-protected).
    ///
    /// The master derives `f` from the global CBVLSN: the file that contains
    /// the CBVLSN, so files fully below it (already replayed by every
    /// still-attached electable replica) can be reclaimed while the file the
    /// slowest replica is still reading — and everything after it — is pinned.
    /// JE: the master pins every log file at or after the file containing the
    /// global CBVLSN via the `FileProtector` replication-protected range.
    pub fn set_replication_floor(&self, floor: Option<u32>) {
        let v = floor.map(u64::from).unwrap_or(NO_FLOOR);
        self.replication_floor.store(v, Ordering::Release);
    }

    /// Returns the current replication-protected file floor, if any.
    pub fn replication_floor(&self) -> Option<u32> {
        match self.replication_floor.load(Ordering::Acquire) {
            NO_FLOOR => None,
            v => Some(v as u32),
        }
    }

    /// Returns whether `file_number` is protected by the replication floor
    /// (i.e. it lies at or after the CBVLSN-derived floor).
    pub fn is_replication_protected(&self, file_number: u32) -> bool {
        u64::from(file_number) >= self.replication_floor.load(Ordering::Acquire)
    }

    /// Returns a list of all protected files with their reasons.
    ///
    /// Useful for debugging and status reporting.
    pub fn get_protected_files(&self) -> Vec<(u32, String)> {
        let protected = self.protected_files.lock();
        protected
            .iter()
            .map(|(file, info)| (*file, info.reason.clone()))
            .collect()
    }

    /// Returns the number of currently protected files.
    pub fn get_protected_size(&self) -> usize {
        self.protected_files.lock().len()
    }

    /// Returns the protection count for a specific file.
    ///
    /// Returns 0 if the file is not protected.
    pub fn get_protection_count(&self, file_number: u32) -> u32 {
        self.protected_files
            .lock()
            .get(&file_number)
            .map(|info| info.count)
            .unwrap_or(0)
    }

    /// Removes all protections (for testing/recovery).
    pub fn clear(&self) {
        self.protected_files.lock().clear();
    }
}

impl Default for FileProtector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_protector() {
        let protector = FileProtector::new();
        assert_eq!(protector.get_protected_size(), 0);
        assert!(!protector.is_protected(1));
    }

    #[test]
    fn test_protect_file() {
        let protector = FileProtector::new();

        protector.protect_file(1, "Backup");
        assert!(protector.is_protected(1));
        assert_eq!(protector.get_protected_size(), 1);
        assert_eq!(protector.get_protection_count(1), 1);
    }

    // JE `FileProtectorTest.testDiskOrderedCursor` / `testBackup`: a file may
    // be protected by several subsystems at once (backup, feeder, an open
    // DiskOrderedCursor), and stays protected until EVERY protector releases
    // it. The DiskOrderedCursor end-to-end wiring lives in
    // `noxu-dbi::disk_ordered_cursor_impl` (protect on open, unprotect on
    // drop; the cleaner delete path checks `is_protected`).
    #[test]
    fn test_protect_multiple_times() {
        let protector = FileProtector::new();

        protector.protect_file(1, "Backup");
        protector.protect_file(1, "Feeder");
        protector.protect_file(1, "DiskOrderedCursor");

        assert!(protector.is_protected(1));
        assert_eq!(protector.get_protection_count(1), 3);
    }

    #[test]
    fn test_unprotect_file() {
        let protector = FileProtector::new();

        protector.protect_file(1, "Backup");
        assert!(protector.is_protected(1));

        let fully_unprotected = protector.unprotect_file(1);
        assert!(fully_unprotected);
        assert!(!protector.is_protected(1));
        assert_eq!(protector.get_protected_size(), 0);
    }

    #[test]
    fn test_unprotect_with_multiple_protections() {
        let protector = FileProtector::new();

        protector.protect_file(1, "Backup");
        protector.protect_file(1, "Feeder");
        protector.protect_file(1, "Cursor");

        // First unprotect - still protected
        let result = protector.unprotect_file(1);
        assert!(!result);
        assert!(protector.is_protected(1));
        assert_eq!(protector.get_protection_count(1), 2);

        // Second unprotect - still protected
        let result = protector.unprotect_file(1);
        assert!(!result);
        assert!(protector.is_protected(1));
        assert_eq!(protector.get_protection_count(1), 1);

        // Third unprotect - now unprotected
        let result = protector.unprotect_file(1);
        assert!(result);
        assert!(!protector.is_protected(1));
        assert_eq!(protector.get_protection_count(1), 0);
    }

    #[test]
    fn test_unprotect_unprotected_file() {
        let protector = FileProtector::new();

        // Unprotecting a file that was never protected is a no-op
        let result = protector.unprotect_file(99);
        assert!(result);
        assert!(!protector.is_protected(99));
    }

    #[test]
    fn test_multiple_files() {
        let protector = FileProtector::new();

        protector.protect_file(1, "Backup");
        protector.protect_file(2, "Feeder");
        protector.protect_file(3, "Cursor");

        assert_eq!(protector.get_protected_size(), 3);
        assert!(protector.is_protected(1));
        assert!(protector.is_protected(2));
        assert!(protector.is_protected(3));
        assert!(!protector.is_protected(4));
    }

    #[test]
    fn test_get_protected_files() {
        let protector = FileProtector::new();

        protector.protect_file(1, "Backup");
        protector.protect_file(2, "Feeder:node1");
        protector.protect_file(3, "DiskOrderedCursor");

        let protected = protector.get_protected_files();
        assert_eq!(protected.len(), 3);

        // Check that all files are present (order doesn't matter)
        let file_numbers: Vec<u32> =
            protected.iter().map(|(f, _)| *f).collect();
        assert!(file_numbers.contains(&1));
        assert!(file_numbers.contains(&2));
        assert!(file_numbers.contains(&3));
    }

    #[test]
    fn test_reason_tracking() {
        let protector = FileProtector::new();

        protector.protect_file(1, "Backup");
        protector.protect_file(1, "Feeder");

        let protected = protector.get_protected_files();
        assert_eq!(protected.len(), 1);

        let (file, reason) = &protected[0];
        assert_eq!(*file, 1);
        assert!(reason.contains("Backup"));
        assert!(reason.contains("Feeder"));
    }

    #[test]
    fn test_clear() {
        let protector = FileProtector::new();

        protector.protect_file(1, "Backup");
        protector.protect_file(2, "Feeder");
        protector.protect_file(3, "Cursor");

        assert_eq!(protector.get_protected_size(), 3);

        protector.clear();

        assert_eq!(protector.get_protected_size(), 0);
        assert!(!protector.is_protected(1));
        assert!(!protector.is_protected(2));
        assert!(!protector.is_protected(3));
    }

    #[test]
    fn test_protection_count() {
        let protector = FileProtector::new();

        assert_eq!(protector.get_protection_count(1), 0);

        protector.protect_file(1, "Reason1");
        assert_eq!(protector.get_protection_count(1), 1);

        protector.protect_file(1, "Reason2");
        assert_eq!(protector.get_protection_count(1), 2);

        protector.unprotect_file(1);
        assert_eq!(protector.get_protection_count(1), 1);

        protector.unprotect_file(1);
        assert_eq!(protector.get_protection_count(1), 0);
    }

    #[test]
    fn test_default() {
        let protector = FileProtector::default();
        assert_eq!(protector.get_protected_size(), 0);
    }

    #[test]
    fn test_replication_floor_protects_file_and_above() {
        let protector = FileProtector::new();
        // No floor: nothing protected by replication.
        assert_eq!(protector.replication_floor(), None);
        assert!(!protector.is_protected(0));

        // Floor at file 2 → files 2,3,.. protected; 0,1 cleanable.
        protector.set_replication_floor(Some(2));
        assert_eq!(protector.replication_floor(), Some(2));
        assert!(!protector.is_protected(0));
        assert!(!protector.is_protected(1));
        assert!(protector.is_protected(2));
        assert!(protector.is_protected(3));
        assert!(protector.is_replication_protected(2));
        assert!(!protector.is_replication_protected(1));

        // Floor rises to file 3 → file 2 released.
        protector.set_replication_floor(Some(3));
        assert!(!protector.is_protected(2));
        assert!(protector.is_protected(3));

        // Clearing the floor releases everything (no per-file counts here).
        protector.set_replication_floor(None);
        assert_eq!(protector.replication_floor(), None);
        assert!(!protector.is_protected(3));
    }

    #[test]
    fn test_replication_floor_and_count_are_independent() {
        let protector = FileProtector::new();
        // A per-file protection below the floor still protects that file.
        protector.protect_file(0, "Backup");
        protector.set_replication_floor(Some(2));
        assert!(protector.is_protected(0), "per-file count protects file 0");
        assert!(!protector.is_protected(1), "file 1 below floor, no count");
        assert!(protector.is_protected(2), "file 2 at floor");

        // Dropping the per-file count releases file 0 (still below the floor).
        protector.unprotect_file(0);
        assert!(!protector.is_protected(0));
    }
}
