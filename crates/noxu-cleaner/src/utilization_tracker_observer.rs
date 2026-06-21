//! `LogWriteObserver` implementation backed by a `UtilizationTracker`.
//!
//! Wraps an `Arc<Mutex<UtilizationTracker>>` and implements the
//! `noxu_log::LogWriteObserver` trait so that the `LogManager` can notify
//! it (under the LWL) for every log write.
//!
//! Utilization tracking hooks invoked from the log write path.
//! `countObsoleteNode` calls made from `LogManager.serialLogWork()`.

use std::sync::Arc;

use noxu_log::{
    LogWriteObserver, ObsoleteKind as LogObsoleteKind, ObsoleteLsn,
};
use noxu_sync::Mutex;

use crate::UtilizationTracker;
use crate::utilization_tracker::ObsoleteKind;

/// An `Arc<Mutex<UtilizationTracker>>` wrapper that implements `LogWriteObserver`.
///
/// Install this in the `LogManager` via `set_write_observer()` so that every
/// log write is automatically reflected in the utilization statistics.
pub struct UtilizationTrackerObserver {
    tracker: Arc<Mutex<UtilizationTracker>>,
}

impl UtilizationTrackerObserver {
    /// Wraps an existing tracker.
    pub fn new(tracker: Arc<Mutex<UtilizationTracker>>) -> Self {
        UtilizationTrackerObserver { tracker }
    }

    /// Returns the underlying tracker.
    pub fn tracker(&self) -> &Arc<Mutex<UtilizationTracker>> {
        &self.tracker
    }
}

impl LogWriteObserver for UtilizationTrackerObserver {
    fn count_new_entry(
        &self,
        file_num: u32,
        _offset: u32,
        entry_size: u32,
        is_ln: bool,
        is_in: bool,
        db_id: Option<u32>,
    ) {
        self.tracker.lock().count_new_log_entry_db(
            file_num,
            entry_size as i32,
            is_ln,
            is_in,
            db_id,
        );
    }

    fn count_obsolete(&self, obsolete: ObsoleteLsn) {
        let kind = match obsolete.kind {
            LogObsoleteKind::Exact => ObsoleteKind::Exact,
            LogObsoleteKind::Inexact => ObsoleteKind::Inexact,
            LogObsoleteKind::DupsAllowed => ObsoleteKind::DupsAllowed,
        };
        let file_num = obsolete.lsn.file_number();
        let offset = obsolete.lsn.file_offset();
        let mut tracker = self.tracker.lock();
        match kind {
            ObsoleteKind::Exact => tracker.count_obsolete_node(
                file_num,
                offset,
                obsolete.size,
                obsolete.is_ln,
                obsolete.db_id,
            ),
            ObsoleteKind::Inexact => tracker.count_obsolete_node_inexact(
                file_num,
                offset,
                obsolete.size,
                obsolete.is_ln,
                obsolete.db_id,
            ),
            ObsoleteKind::DupsAllowed => tracker
                .count_obsolete_node_dups_allowed(
                    file_num,
                    offset,
                    obsolete.size,
                    obsolete.is_ln,
                    obsolete.db_id,
                ),
        }
    }
}
