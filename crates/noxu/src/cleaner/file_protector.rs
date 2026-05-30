//! File protection from deletion during processing.
//!
//! protects log files from deletion while they
//! are being read or processed by various subsystems (backup, replication, etc.).

use hashbrown::HashMap;
use crate::sync::Mutex;

/// Protects log files from deletion while they are being read or processed.
///
/// Files can be protected by multiple consumers (backup, disk-ordered cursor,
/// replication feeders, etc.). A file is only safe to delete when its
/// protection count reaches zero.
#[derive(Debug)]
pub struct FileProtector {
    /// Map of file_number -> protection information.
    protected_files: Mutex<HashMap<u32, ProtectionInfo>>,
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
        Self { protected_files: Mutex::new(HashMap::new()) }
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
    pub fn is_protected(&self, file_number: u32) -> bool {
        self.protected_files.lock().contains_key(&file_number)
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
}
