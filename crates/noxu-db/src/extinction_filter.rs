//! Filter interface for identifying extinct records.
//!
//!
//! # Record Extinction
//!
//! Record extinction is an optimized deletion mechanism for large sets of
//! records that are known to be permanently unused. Instead of logging a
//! delete entry per record, a single [`Environment::discard_extinct_records`]
//! call logs one entry covering the entire key range, and the cleaner
//! asynchronously removes the records and reclaims disk space.
//!
//! **Semantics**: Once records are marked extinct via
//! `discard_extinct_records`, the application must not read or write them
//! again. does not guarantee transactional semantics for extinct records.
//!
//! ExtinctionFilter.ExtinctionStatus` (extended fork 18.1+).

/// Classification returned by [`ExtinctionFilter::get_extinction_status`].
///
/// 
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtinctionStatus {
    /// The record is extinct: it was specified in a previous
    /// `discard_extinct_records` call and will never be accessed again.
    ///
    /// 
    Extinct,

    /// The record is not extinct: it has not been specified for extinction.
    ///
    /// 
    NotExtinct,

    /// The record may or may not be extinct. The application temporarily
    /// cannot determine extinction status (e.g. during startup before
    /// metadata is loaded). The cleaner will fall back to a BTree lookup.
    ///
    /// 
    MaybeExtinct,
}

/// Callback for classifying records as extinct.
///
/// 
///
/// Implement this trait and register it with `EnvironmentConfig` before
/// calling [`crate::Environment::discard_extinct_records`].
///
/// # Requirements
///
/// For every key previously specified in `discard_extinct_records`, this
/// method **must** return [`ExtinctionStatus::Extinct`] or
/// [`ExtinctionStatus::MaybeExtinct`]. Returning
/// [`ExtinctionStatus::NotExtinct`] for an extinct key is a contract
/// violation and may trigger an `EnvironmentFailureException`.
pub trait ExtinctionFilter: Send + Sync {
    /// Determine the extinction status of a record.
    ///
    /// # Arguments
    ///
    /// * `db_name` — name of the database containing the record.
    /// * `dups` — whether the database uses duplicate keys (secondary DB).
    /// * `key` — the primary key of the record. When `dups` is true this is
    ///   the record's data field, treated as a primary key.
    ///
    /// 
    fn get_extinction_status(
        &self,
        db_name: &str,
        dups: bool,
        key: &[u8],
    ) -> ExtinctionStatus;
}

#[cfg(test)]
mod tests {
    use super::*;

    struct AlwaysExtinct;

    impl ExtinctionFilter for AlwaysExtinct {
        fn get_extinction_status(
            &self,
            _db_name: &str,
            _dups: bool,
            _key: &[u8],
        ) -> ExtinctionStatus {
            ExtinctionStatus::Extinct
        }
    }

    #[test]
    fn test_always_extinct() {
        let f = AlwaysExtinct;
        assert_eq!(
            f.get_extinction_status("mydb", false, b"key1"),
            ExtinctionStatus::Extinct
        );
    }

    #[test]
    fn test_status_eq() {
        assert_eq!(ExtinctionStatus::Extinct, ExtinctionStatus::Extinct);
        assert_ne!(ExtinctionStatus::Extinct, ExtinctionStatus::NotExtinct);
    }
}
