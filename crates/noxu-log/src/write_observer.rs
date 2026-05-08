//! Observer trait for log write events.
//!
//! (`serialLogWork`). In JE, `UtilizationTracker` is fetched from `envImpl`
//! and called under the Log Write Latch (LWL) each time an entry is written.
//!
//! Defining the trait here in `noxu-log` avoids a circular dependency:
//! `noxu-cleaner` depends on `noxu-log`, so the trait must live in `noxu-log`
//! while the implementation lives in `noxu-cleaner`.

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
    fn count_new_entry(
        &self,
        file_num: u32,
        offset: u32,
        entry_size: u32,
        is_ln: bool,
        is_in: bool,
    );

    /// Called when a previous version of a node is being replaced.
    ///
    /// 
    ///
    /// # Parameters
    /// - `file_num`   : File number of the obsolete LSN.
    /// - `offset`     : File offset of the obsolete LSN.
    /// - `entry_size` : Size of the obsolete entry (0 if unknown).
    /// - `is_ln`      : True if the obsolete entry is an LN type.
    fn count_obsolete(
        &self,
        file_num: u32,
        offset: u32,
        entry_size: u32,
        is_ln: bool,
    );
}
