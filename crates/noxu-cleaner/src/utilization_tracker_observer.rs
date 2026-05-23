//! `LogWriteObserver` implementation backed by a `UtilizationTracker`.
//!
//! Wraps an `Arc<Mutex<UtilizationTracker>>` and implements the
//! `noxu_log::LogWriteObserver` trait so that the `LogManager` can notify
//! it (under the LWL) for every log write.
//!
//! Utilization tracking hooks invoked from the log write path.
//! `countObsoleteNode` calls made from `LogManager.serialLogWork()`.

use std::sync::Arc;

use noxu_log::LogWriteObserver;
use noxu_sync::Mutex;

use crate::UtilizationTracker;

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
    ) {
        self.tracker.lock().count_new_log_entry(
            file_num,
            entry_size as i32,
            is_ln,
            is_in,
        );
    }

    fn count_obsolete(
        &self,
        file_num: u32,
        offset: u32,
        entry_size: u32,
        is_ln: bool,
    ) {
        self.tracker.lock().track_obsolete(
            file_num,
            offset,
            entry_size as i32,
            is_ln,
        );
    }
}
