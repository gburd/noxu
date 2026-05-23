//! Filter interface for sequential database scans.
//!

/// Result returned by [`ScanFilter::check_key`] to control scan inclusion
/// and termination.
///
///
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanResult {
    /// Include the key and continue scanning.
    ///
    ///
    Include,

    /// Exclude the key but continue scanning.
    ///
    ///
    Exclude,

    /// Include the key and stop scanning.
    ///
    ///
    IncludeStop,

    /// Exclude the key and stop scanning.
    ///
    ///
    ExcludeStop,
}

impl ScanResult {
    /// Returns `true` for [`ScanResult::Include`] and [`ScanResult::IncludeStop`].
    ///
    ///
    pub fn get_include(self) -> bool {
        matches!(self, ScanResult::Include | ScanResult::IncludeStop)
    }

    /// Returns `true` for [`ScanResult::IncludeStop`] and [`ScanResult::ExcludeStop`].
    ///
    ///
    pub fn get_stop(self) -> bool {
        matches!(self, ScanResult::IncludeStop | ScanResult::ExcludeStop)
    }
}

/// Interface for filtering and optionally stopping a sequential scan.
///
///
///
/// Passed to scan operations (e.g. `Database::scan_with_filter`) to control
/// which records are returned and whether the scan continues.
///
/// # Example
///
/// ```
/// use noxu_db::{ScanFilter, ScanResult};
///
/// struct PrefixFilter<'a>(&'a [u8]);
///
/// impl<'a> ScanFilter for PrefixFilter<'a> {
///     fn check_key(&self, key: &[u8]) -> ScanResult {
///         if key.starts_with(self.0) {
///             ScanResult::Include
///         } else if key < self.0 {
///             ScanResult::Exclude
///         } else {
///             // Past the prefix — stop scanning.
///             ScanResult::ExcludeStop
///         }
///     }
/// }
/// ```
pub trait ScanFilter: Send + Sync {
    /// Called for each key to determine whether it should be included and
    /// whether the scan should continue.
    ///
    ///
    fn check_key(&self, key: &[u8]) -> ScanResult;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scan_result_include() {
        assert!(ScanResult::Include.get_include());
        assert!(!ScanResult::Include.get_stop());
    }

    #[test]
    fn test_scan_result_exclude() {
        assert!(!ScanResult::Exclude.get_include());
        assert!(!ScanResult::Exclude.get_stop());
    }

    #[test]
    fn test_scan_result_include_stop() {
        assert!(ScanResult::IncludeStop.get_include());
        assert!(ScanResult::IncludeStop.get_stop());
    }

    #[test]
    fn test_scan_result_exclude_stop() {
        assert!(!ScanResult::ExcludeStop.get_include());
        assert!(ScanResult::ExcludeStop.get_stop());
    }

    struct AlwaysInclude;
    impl ScanFilter for AlwaysInclude {
        fn check_key(&self, _key: &[u8]) -> ScanResult {
            ScanResult::Include
        }
    }

    #[test]
    fn test_scan_filter_always_include() {
        let f = AlwaysInclude;
        assert_eq!(f.check_key(b"hello"), ScanResult::Include);
    }

    struct PrefixFilter<'a>(&'a [u8]);
    impl<'a> ScanFilter for PrefixFilter<'a> {
        fn check_key(&self, key: &[u8]) -> ScanResult {
            if key.starts_with(self.0) {
                ScanResult::Include
            } else {
                ScanResult::ExcludeStop
            }
        }
    }

    #[test]
    fn test_prefix_filter() {
        let f = PrefixFilter(b"abc");
        assert_eq!(f.check_key(b"abcdef"), ScanResult::Include);
        assert_eq!(f.check_key(b"xyz"), ScanResult::ExcludeStop);
    }
}
