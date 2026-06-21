//! Observer trait for log write events.
//!
//! (`serialLogWork`). `UtilizationTracker` is fetched from `envImpl`
//! and called under the Log Write Latch (LWL) each time an entry is written.
//!
//! Defining the trait here in `noxu-log` avoids a circular dependency:
//! `noxu-cleaner` depends on `noxu-log`, so the trait must live in `noxu-log`
//! while the implementation lives in `noxu-cleaner`.

/// Which obsolete-counting variant the log write path should apply.
///
/// Mirrors JE's three `UtilizationTracker` obsolete methods. Defined here in
/// `noxu-log` (rather than `noxu-cleaner`) so the trait signature does not
/// create a circular dependency; `noxu-cleaner` maps it onto its own
/// `ObsoleteKind`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObsoleteKind {
    /// `countObsoleteNode`: exact LSN, track offset, dedup-checked.
    Exact,
    /// `countObsoleteNodeInexact`: approximate LSN, no offset tracked.
    Inexact,
    /// `countObsoleteNodeDupsAllowed`: track offset, double-count allowed.
    DupsAllowed,
}

/// An obsolete LSN to be counted on the log write path, with the metadata
/// JE's `UtilizationTracker` needs: the owning DB, the entry size, and which
/// counting variant to apply.
///
/// Bundled into one struct so adding the per-DB axis (CLN-9) and the
/// three-method distinction (CLN-10) does not require threading three extra
/// parameters through every `log()` overload.
#[derive(Debug, Clone, Copy)]
pub struct ObsoleteLsn {
    /// The LSN of the now-obsolete entry.
    pub lsn: noxu_util::lsn::Lsn,
    /// The database that owned the entry (CLN-9 per-DB axis), if known.
    pub db_id: Option<u32>,
    /// Size of the obsolete entry (0 if unknown / not applicable).
    pub size: i32,
    /// True if the obsolete entry is an LN (vs an IN).
    pub is_ln: bool,
    /// Which `countObsolete*` variant to apply.
    pub kind: ObsoleteKind,
}

impl ObsoleteLsn {
    /// Convenience constructor for the common exact-LN case.
    pub fn exact(
        lsn: noxu_util::lsn::Lsn,
        db_id: Option<u32>,
        size: i32,
        is_ln: bool,
    ) -> Self {
        Self { lsn, db_id, size, is_ln, kind: ObsoleteKind::Exact }
    }
}

/// Callback interface for utilization tracking on every log write.
///
/// `UtilizationTracker.countNewLogEntry` /
/// `countObsoleteNode` / `countObsoleteNodeInexact` calls made from
/// `LogManager.serialLogWork()`.
///
/// Implementations MUST be `Send + Sync` and MUST handle their own internal
/// locking because these methods are called **under the LWL**.
pub trait LogWriteObserver: Send + Sync {
    /// Called for every new log entry, with the assigned LSN.
    ///
    ///
    ///
    /// # Parameters
    /// - `file_num`   : Log file number component of the assigned LSN.
    /// - `offset`     : File offset component of the assigned LSN.
    /// - `entry_size` : Total size of the log record (header + payload).
    /// - `is_ln`      : True if the entry is any LN type.
    /// - `is_in`      : True if the entry is any IN type.
    /// - `db_id`      : Owning database id (CLN-9 per-DB axis), if known.
    fn count_new_entry(
        &self,
        file_num: u32,
        offset: u32,
        entry_size: u32,
        is_ln: bool,
        is_in: bool,
        db_id: Option<u32>,
    );

    /// Called when a previous version of a node is being replaced.
    ///
    ///
    ///
    /// The `ObsoleteLsn` carries the obsolete LSN, the owning DB id, the size,
    /// the LN/IN flag, and which `countObsolete*` variant to apply.
    fn count_obsolete(&self, obsolete: ObsoleteLsn);
}
