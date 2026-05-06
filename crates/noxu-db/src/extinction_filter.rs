//! Filter interface for identifying extinct records.
//!
//! Port of `com.sleepycat.je.ExtinctionFilter` from the Oracle NoSQL JE fork.
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
//! again. JE does not guarantee transactional semantics for extinct records.
//!
//! Port of `com.sleepycat.je.ExtinctionFilter` and
//! `com.sleepycat.je.ExtinctionFilter.ExtinctionStatus` (NoSQL JE 18.1+).

/// Classification returned by [`ExtinctionFilter::get_extinction_status`].
///
/// Port of `com.sleepycat.je.ExtinctionFilter.ExtinctionStatus`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExtinctionStatus {
    /// The record is extinct: it was specified in a previous
    /// `discard_extinct_records` call and will never be accessed again.
    ///
    /// Port of `ExtinctionStatus.EXTINCT`.
    Extinct,

    /// The record is not extinct: it has not been specified for extinction.
    ///
    /// Port of `ExtinctionStatus.NOT_EXTINCT`.
    NotExtinct,

    /// The record may or may not be extinct. The application temporarily
    /// cannot determine extinction status (e.g. during startup before
    /// metadata is loaded). The cleaner will fall back to a BTree lookup.
    ///
    /// Port of `ExtinctionStatus.MAYBE_EXTINCT`.
    MaybeExtinct,
}

/// Callback for classifying records as extinct.
///
/// Port of `com.sleepycat.je.ExtinctionFilter`.
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
    /// * `db_name` — name of the JE database containing the record.
    /// * `dups` — whether the database uses duplicate keys (secondary DB).
    /// * `key` — the primary key of the record. When `dups` is true this is
    ///   the record's data field, treated as a primary key.
    ///
    /// Port of `ExtinctionFilter.getExtinctionStatus(String, boolean, byte[])`.
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
