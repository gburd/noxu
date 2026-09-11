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
        let mut tracker = self.tracker.lock();
        apply_obsolete(&mut tracker, obsolete);
    }

    /// Batched merge: takes the tracker mutex ONCE for the whole batch
    /// instead of once per entry.
    ///
    /// This is the actual fix for the measured throughput cliff
    /// (`docs/src/internal/space-amplification-2026-09.md` Phase 4):
    /// `LogManager::count_obsolete_commit_lsns` now hands over a whole
    /// commit's obsolete-LSN set at once, so a multi-key transaction pays
    /// one lock acquisition instead of N.
    fn count_obsolete_batch(&self, obsolete: &[ObsoleteLsn]) {
        if obsolete.is_empty() {
            return;
        }
        let mut tracker = self.tracker.lock();
        for &o in obsolete {
            apply_obsolete(&mut tracker, o);
        }
    }
}

/// Shared single-entry application, used by both the one-at-a-time and
/// batched paths so the two can never diverge in behaviour.
fn apply_obsolete(tracker: &mut UtilizationTracker, obsolete: ObsoleteLsn) {
    let kind = match obsolete.kind {
        LogObsoleteKind::Exact => ObsoleteKind::Exact,
        LogObsoleteKind::Inexact => ObsoleteKind::Inexact,
        LogObsoleteKind::DupsAllowed => ObsoleteKind::DupsAllowed,
    };
    let file_num = obsolete.lsn.file_number();
    let offset = obsolete.lsn.file_offset();
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
        ObsoleteKind::DupsAllowed => tracker.count_obsolete_node_dups_allowed(
            file_num,
            offset,
            obsolete.size,
            obsolete.is_ln,
            obsolete.db_id,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_util::lsn::Lsn;

    /// Merge-protocol equivalence: applying a batch of obsolete LSNs through
    /// [`UtilizationTrackerObserver::count_obsolete_batch`] (one lock
    /// acquisition) must produce EXACTLY the same tracker state as applying
    /// the same entries one at a time through
    /// [`UtilizationTrackerObserver::count_obsolete`] (one lock acquisition
    /// per entry). Correctness of the batched merge rests entirely on this:
    /// batching must never change WHAT gets counted, only how many times the
    /// shared mutex is taken.
    #[test]
    fn batched_merge_matches_sequential_single_calls() {
        let batch: Vec<ObsoleteLsn> = (0..50u32)
            .map(|i| {
                ObsoleteLsn::exact(Lsn::new(1, 100 + i * 8), Some(7), 64, true)
            })
            .collect();

        let batched_tracker =
            Arc::new(Mutex::new(UtilizationTracker::new(true)));
        let batched_observer =
            UtilizationTrackerObserver::new(Arc::clone(&batched_tracker));
        batched_observer.count_obsolete_batch(&batch);

        let sequential_tracker =
            Arc::new(Mutex::new(UtilizationTracker::new(true)));
        let sequential_observer =
            UtilizationTrackerObserver::new(Arc::clone(&sequential_tracker));
        for &o in &batch {
            sequential_observer.count_obsolete(o);
        }

        let batched = batched_tracker.lock();
        let sequential = sequential_tracker.lock();
        let batched_summary =
            batched.get_tracked_summary(1).unwrap().get_summary();
        let sequential_summary =
            sequential.get_tracked_summary(1).unwrap().get_summary();
        assert_eq!(batched_summary, sequential_summary);
        assert_eq!(
            batched.get_db_file_summary(7, 1),
            sequential.get_db_file_summary(7, 1)
        );
    }

    /// An empty batch must be a no-op: no file gets created in the tracker,
    /// and (implicitly, since there is nothing to lock over) no lock is
    /// taken for nothing.
    #[test]
    fn empty_batch_is_a_no_op() {
        let tracker = Arc::new(Mutex::new(UtilizationTracker::new(true)));
        let observer = UtilizationTrackerObserver::new(Arc::clone(&tracker));
        observer.count_obsolete_batch(&[]);
        assert_eq!(tracker.lock().get_tracked_file_count(), 0);
    }
}
