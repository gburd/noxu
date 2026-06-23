//! In-memory utilization tracking.
//!
//! Base and per-file utilization tracking for log space accounting.
//! tracks per-file utilization changes in memory between checkpoints.
//!
//! ## Property tests
//!
//! Oracle properties comparing the tracker against a brute-force scan over
//! the LN write/delete event log live in
//! `crates/noxu-cleaner/tests/prop_tests.rs` (Wave 11-E):
//! `prop_tracker_total_size_matches_writes`,
//! `prop_tracker_obsolete_count_matches_oracle`,
//! `prop_tracker_file_set_is_union`, `prop_tracker_clear_resets`.

use crate::db_file_summary::DbFileSummary;
use crate::tracked_file_summary::TrackedFileSummary;
use hashbrown::HashMap;

/// Which `BaseUtilizationTracker.countObsolete` variant to apply.
///
/// JE has three public obsolete-counting methods on `UtilizationTracker`,
/// differing only in `trackOffset` / `checkDupOffsets`:
///
/// | Variant | `trackOffset` | `checkDupOffsets` | JE method |
/// |---|---|---|---|
/// | `Exact` | true | true | `countObsoleteNode` |
/// | `Inexact` | false | false | `countObsoleteNodeInexact` |
/// | `DupsAllowed` | true | false | `countObsoleteNodeDupsAllowed` |
///
/// Cite: `UtilizationTracker.countObsoleteNode` /
/// `countObsoleteNodeInexact` / `countObsoleteNodeDupsAllowed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObsoleteKind {
    /// `countObsoleteNode`: exact LSN, track offset, assert no dup offset.
    Exact,
    /// `countObsoleteNodeInexact`: approximate LSN, do not track offset.
    Inexact,
    /// `countObsoleteNodeDupsAllowed`: track offset, allow double-count.
    DupsAllowed,
}

impl ObsoleteKind {
    /// Whether this variant tracks the obsolete LSN offset.
    fn track_offset(self) -> bool {
        matches!(self, ObsoleteKind::Exact | ObsoleteKind::DupsAllowed)
    }

    /// Whether this variant asserts the offset has not been counted before.
    fn check_dup_offsets(self) -> bool {
        matches!(self, ObsoleteKind::Exact)
    }
}

/// Tracks per-file utilization changes in memory.
///
/// The tracker maintains a map of file numbers to tracked summaries, accumulating changes
/// between checkpoints. When a checkpoint occurs, the tracked data is transferred to the
/// persistent UtilizationProfile.
///
/// CLN-9 (the per-DB axis): JE keeps the `DbFileSummary` map on each
/// `DatabaseImpl` and reaches it via `getDbFileSummary`. The global tracker
/// in noxu cannot reach `DatabaseImpl` (it would be a circular dependency:
/// `noxu-dbi` depends on `noxu-cleaner`), so the per-DB summaries are kept on
/// the tracker itself, keyed by `(db_id, file_num)`. This holds exactly the
/// same counters JE's `DbFileSummary` does; only the storage location differs.
#[derive(Debug)]
pub struct UtilizationTracker {
    /// Map of file_number -> TrackedFileSummary.
    tracked_files: HashMap<u32, TrackedFileSummary>,
    /// CLN-9: per-DB-per-file summaries, keyed by `db_id` then `file_num`.
    /// Mirrors JE's `DatabaseImpl.dbFileSummaries` (a `DbFileSummaryMap`).
    db_file_summaries: HashMap<u32, HashMap<u32, DbFileSummary>>,
    /// Bytes of tracked info (for memory budget).
    tracked_bytes: i64,
    /// Whether to track obsolete offset details.
    track_detail: bool,
    /// Maximum bytes of tracked obsolete-offset detail before `evict_memory`
    /// starts dropping it. JE `MemoryBudget.trackerBudget`
    /// (`= cachePortion * CLEANER_DETAIL_MAX_MEMORY_PERCENTAGE / 100`). A
    /// value of 0 disables the cap (used by the legacy/test constructor).
    tracker_budget: i64,
}

/// One megabyte: a tracked file holding at least this much detail is flushed
/// immediately by `evict_memory`, regardless of the total. JE
/// `UtilizationTracker.evictMemory` `ONE_MB`.
const ONE_MB: i64 = 1024 * 1024;

impl UtilizationTracker {
    /// Creates a new utilization tracker with no budget cap (detail grows
    /// unbounded). Prefer [`Self::with_budget`] in production; this is kept
    /// for tests and call sites that do not have a cache size to derive a
    /// budget from.
    pub fn new(track_detail: bool) -> Self {
        Self::with_budget(track_detail, 0)
    }

    /// Creates a new utilization tracker with an explicit byte budget for the
    /// obsolete-offset detail.
    ///
    /// `tracker_budget` is JE's `MemoryBudget.trackerBudget`
    /// (`cachePortion * CLEANER_DETAIL_MAX_MEMORY_PERCENTAGE / 100`). When the
    /// tracked detail exceeds it, [`Self::evict_memory`] drops detail to stay
    /// bounded. A budget of 0 disables the cap.
    pub fn with_budget(track_detail: bool, tracker_budget: i64) -> Self {
        Self {
            tracked_files: HashMap::new(),
            db_file_summaries: HashMap::new(),
            tracked_bytes: 0,
            track_detail,
            tracker_budget,
        }
    }

    /// Counts a node that has become obsolete and tracks the LSN offset.
    ///
    /// JE `UtilizationTracker.countObsoleteNode`
    /// (`trackOffset=true, checkDupOffsets=true`): exact LSN, dedup-checked.
    ///
    /// `db_id` is the database that owned the node (CLN-9 per-DB axis); pass
    /// `None` only when no DB is associated (which JE asserts never happens
    /// for trackable types, but the global write path may not always know it).
    pub fn count_obsolete_node(
        &mut self,
        file_number: u32,
        offset: u32,
        size: i32,
        count_as_ln: bool,
        db_id: Option<u32>,
    ) {
        self.count_obsolete(
            file_number,
            offset,
            size,
            count_as_ln,
            db_id,
            ObsoleteKind::Exact,
        );
    }

    /// Counts an obsolete node without tracking the (inexact) LSN offset.
    ///
    /// JE `UtilizationTracker.countObsoleteNodeInexact`
    /// (`trackOffset=false, checkDupOffsets=false`). Used for
    /// `isImmediatelyObsolete` deleted/embedded LNs (L-6) where the LSN is
    /// only approximate.
    pub fn count_obsolete_node_inexact(
        &mut self,
        file_number: u32,
        offset: u32,
        size: i32,
        count_as_ln: bool,
        db_id: Option<u32>,
    ) {
        self.count_obsolete(
            file_number,
            offset,
            size,
            count_as_ln,
            db_id,
            ObsoleteKind::Inexact,
        );
    }

    /// Counts an obsolete node, tracking the offset but allowing a duplicate.
    ///
    /// JE `UtilizationTracker.countObsoleteNodeDupsAllowed`
    /// (`trackOffset=true, checkDupOffsets=false`). Used where the same LSN
    /// offset may legitimately be counted twice: a BIN-delta `prevDeltaLsn`
    /// (L-5) and during recovery.
    pub fn count_obsolete_node_dups_allowed(
        &mut self,
        file_number: u32,
        offset: u32,
        size: i32,
        count_as_ln: bool,
        db_id: Option<u32>,
    ) {
        self.count_obsolete(
            file_number,
            offset,
            size,
            count_as_ln,
            db_id,
            ObsoleteKind::DupsAllowed,
        );
    }

    /// Shared obsolete-counting core.
    ///
    /// JE `BaseUtilizationTracker.countObsolete`. Updates both the per-FILE
    /// `FileSummary` and (when `db_id` is supplied) the per-DB
    /// `DbFileSummary`, then conditionally tracks the LSN offset.
    fn count_obsolete(
        &mut self,
        file_number: u32,
        offset: u32,
        size: i32,
        count_as_ln: bool,
        db_id: Option<u32>,
        kind: ObsoleteKind,
    ) {
        let track_detail = self.track_detail;
        let tracked =
            self.tracked_files.entry(file_number).or_insert_with(|| {
                TrackedFileSummary::new(file_number, track_detail)
            });

        // countPerFile: update the per-FILE summary counters.
        // JE BaseUtilizationTracker.countObsolete, countPerFile branch.
        let summary = tracked.get_summary_mut();
        if count_as_ln {
            summary.obsolete_ln_count += 1;
            // The size is OPTIONAL when tracking obsolete LNs — only
            // accumulate it (and the counted tally feeding the avg-LN-size
            // estimator) when a real size was supplied.
            // JE: countObsolete "The size is optional when tracking obsolete
            // LNs."
            if size > 0 {
                summary.obsolete_ln_size += size;
                summary.obsolete_ln_size_counted += 1;
            }
        } else {
            summary.obsolete_in_count += 1;
            // JE asserts size == 0 for obsolete INs ("not allowed").
            debug_assert_eq!(
                size, 0,
                "obsolete IN size must be 0 (JE BaseUtilizationTracker.countObsolete)"
            );
        }

        // trackOffset: record the offset (skip offset 0 = the file header,
        // which is always treated as obsolete). checkDupOffsets asserts the
        // offset was not already counted on the Exact path. [#15365]
        // JE: countObsolete, trackOffset branch -> fileSummary.trackObsolete.
        if kind.track_offset() && offset != 0 {
            if kind.check_dup_offsets() {
                debug_assert!(
                    !tracked.get_obsolete_offsets().contains(&offset),
                    "checkDupOffsets: offset {offset} in file {file_number} already counted obsolete (JE invariant)"
                );
            }
            tracked.add_obsolete_offset(offset);
        }

        // countPerDb: mirror the counters in the per-DB DbFileSummary (CLN-9).
        // JE: countObsolete, countPerDb branch -> getDbFileSummary.
        if let Some(db) = db_id {
            let db_summary = self
                .db_file_summaries
                .entry(db)
                .or_default()
                .entry(file_number)
                .or_default();
            if count_as_ln {
                db_summary.obsolete_ln_count += 1;
                if size > 0 {
                    db_summary.obsolete_ln_size += size;
                    db_summary.obsolete_ln_size_counted += 1;
                }
            } else {
                db_summary.obsolete_in_count += 1;
            }
        }

        // Update memory budget
        self.update_tracked_bytes();
    }

    /// Tracks an obsolete log entry (legacy shim).
    ///
    /// Equivalent to [`Self::count_obsolete_node_dups_allowed`] with no
    /// `db_id`: it tracks the offset but does NOT fire the exact-path dedup
    /// assertion, matching the pre-CLN-10 accumulator semantics relied on by
    /// the property tests and other call sites that may legitimately repeat
    /// an offset. New code that knows the LSN is exact-and-unique should call
    /// [`Self::count_obsolete_node`] instead.
    ///
    /// # Arguments
    /// * `file_number` - The file containing the obsolete entry
    /// * `offset` - The offset of the obsolete entry
    /// * `size` - The size of the obsolete entry
    /// * `count_as_ln` - Whether to count this as an LN (vs IN)
    pub fn track_obsolete(
        &mut self,
        file_number: u32,
        offset: u32,
        size: i32,
        count_as_ln: bool,
    ) {
        self.count_obsolete_node_dups_allowed(
            file_number,
            offset,
            size,
            count_as_ln,
            None,
        );
    }

    /// Counts all active bytes in a database as obsolete (CLN-11).
    ///
    /// Called when a database is removed or truncated: the still-active
    /// amounts in each of the DB's files (total minus already-counted
    /// obsolete) become reclaimable. JE
    /// `BaseUtilizationTracker.countObsoleteDb`.
    ///
    /// Includes the [#19144] self-heal: when a DB becomes obsolete, the size
    /// of all its obsolete LNs can finally be counted accurately, because
    /// every LN byte in the DB is now obsolete — so `obsoleteLNSizeCounted`
    /// is bumped to cover both the LNs becoming obsolete now and those that
    /// went obsolete earlier with their size uncounted.
    ///
    /// After counting, the DB's per-DB summaries are dropped (JE deletes the
    /// MapLN; the per-DB map goes away with it).
    pub fn count_obsolete_db(&mut self, db_id: u32) {
        let Some(per_file) = self.db_file_summaries.remove(&db_id) else {
            return;
        };
        let track_detail = self.track_detail;
        for (file_num, db_summary) in per_file {
            let tracked =
                self.tracked_files.entry(file_num).or_insert_with(|| {
                    TrackedFileSummary::new(file_num, track_detail)
                });
            let summary = tracked.get_summary_mut();

            // Active = total - already-counted-obsolete.
            let ln_obsolete_count =
                db_summary.total_ln_count - db_summary.obsolete_ln_count;
            let ln_obsolete_size =
                db_summary.total_ln_size - db_summary.obsolete_ln_size;
            let in_obsolete_count =
                db_summary.total_in_count - db_summary.obsolete_in_count;
            summary.obsolete_ln_count += ln_obsolete_count;
            summary.obsolete_ln_size += ln_obsolete_size;
            summary.obsolete_in_count += in_obsolete_count;

            // [#19144] obsolete-LN-size self-heal.
            let ln_obsolete_size_counted = ln_obsolete_count
                + (db_summary.obsolete_ln_count
                    - db_summary.obsolete_ln_size_counted);
            summary.obsolete_ln_size_counted += ln_obsolete_size_counted;
        }
        self.update_tracked_bytes();
    }

    /// Counts a new log entry.
    ///
    /// Equivalent to [`Self::count_new_log_entry_db`] with no `db_id`.
    ///
    /// # Arguments
    /// * `file_number` - The file containing the new entry
    /// * `size` - The size of the entry
    /// * `is_ln` - Whether this is an LN entry
    /// * `is_in` - Whether this is an IN entry
    pub fn count_new_log_entry(
        &mut self,
        file_number: u32,
        size: i32,
        is_ln: bool,
        is_in: bool,
    ) {
        self.count_new_log_entry_db(file_number, size, is_ln, is_in, None);
    }

    /// Counts the addition of a new log entry in both the per-FILE and (when
    /// `db_id` is supplied) the per-DB summaries.
    ///
    /// JE `BaseUtilizationTracker.countNew`.
    pub fn count_new_log_entry_db(
        &mut self,
        file_number: u32,
        size: i32,
        is_ln: bool,
        is_in: bool,
        db_id: Option<u32>,
    ) {
        let track_detail = self.track_detail;
        let tracked =
            self.tracked_files.entry(file_number).or_insert_with(|| {
                TrackedFileSummary::new(file_number, track_detail)
            });

        let summary = tracked.get_summary_mut();
        summary.total_count += 1;
        summary.total_size += size;

        if is_ln {
            summary.total_ln_count += 1;
            summary.total_ln_size += size;
            if size > summary.max_ln_size {
                summary.max_ln_size = size;
            }
        }

        if is_in {
            summary.total_in_count += 1;
            summary.total_in_size += size;
        }

        // Per-DB axis (CLN-9): JE countNew updates the DbFileSummary too.
        if let Some(db) = db_id {
            let db_summary = self
                .db_file_summaries
                .entry(db)
                .or_default()
                .entry(file_number)
                .or_default();
            if is_ln {
                db_summary.total_ln_count += 1;
                db_summary.total_ln_size += size;
            }
            if is_in {
                db_summary.total_in_count += 1;
                db_summary.total_in_size += size;
            }
        }

        self.update_tracked_bytes();
    }

    /// Returns the per-DB-per-file summary for a database/file, if tracked.
    pub fn get_db_file_summary(
        &self,
        db_id: u32,
        file_number: u32,
    ) -> Option<&DbFileSummary> {
        self.db_file_summaries.get(&db_id)?.get(&file_number)
    }

    /// Returns a reference to the tracked summary for a file.
    pub fn get_tracked_summary(
        &self,
        file_number: u32,
    ) -> Option<&TrackedFileSummary> {
        self.tracked_files.get(&file_number)
    }

    /// Returns a mutable reference to the tracked summary for a file.
    pub fn get_tracked_summary_mut(
        &mut self,
        file_number: u32,
    ) -> Option<&mut TrackedFileSummary> {
        self.tracked_files.get_mut(&file_number)
    }

    /// Returns a reference to all tracked files.
    pub fn get_tracked_files(&self) -> &HashMap<u32, TrackedFileSummary> {
        &self.tracked_files
    }

    /// Returns a mutable reference to all tracked files.
    pub fn get_tracked_files_mut(
        &mut self,
    ) -> &mut HashMap<u32, TrackedFileSummary> {
        &mut self.tracked_files
    }

    /// Removes and returns all tracked files, clearing the tracker.
    ///
    /// This is typically called when transferring tracked data to the utilization profile.
    pub fn remove_all_tracked_files(
        &mut self,
    ) -> HashMap<u32, TrackedFileSummary> {
        self.tracked_bytes = 0;
        std::mem::take(&mut self.tracked_files)
    }

    /// Returns the bytes of tracked information (for memory budget).
    pub fn get_bytes_tracked(&self) -> i64 {
        self.tracked_bytes
    }

    /// Returns the byte budget for tracked obsolete-offset detail (0 = no cap).
    ///
    /// JE `MemoryBudget.getTrackerBudget`.
    pub fn get_tracker_budget(&self) -> i64 {
        self.tracker_budget
    }

    /// Sets the byte budget for tracked obsolete-offset detail. Used at
    /// env-open once the cache size is known, and when the mutable config
    /// `CLEANER_DETAIL_MAX_MEMORY_PERCENTAGE` changes. JE recomputes
    /// `MemoryBudget.trackerBudget` in `MemoryBudget.reset`.
    pub fn set_tracker_budget(&mut self, tracker_budget: i64) {
        self.tracker_budget = tracker_budget;
    }

    /// Returns the total bytes of tracked obsolete-offset detail across all
    /// tracked files.
    ///
    /// JE aggregates `TrackedFileSummary.getMemorySize` over
    /// `getTrackedFiles()` inside `UtilizationTracker.evictMemory`; this is
    /// the same sum exposed for the budget check and for tests. Only the
    /// per-LSN offset detail is budgeted (JE budgets `memSize`, not the
    /// per-object overhead).
    pub fn get_memory_usage(&self) -> i64 {
        self.tracked_files.values().map(|t| t.detail_memory_size() as i64).sum()
    }

    /// Evicts tracked obsolete-offset detail when the tracker budget is
    /// exceeded, keeping the aggregate counters intact. Returns the number of
    /// detail bytes freed.
    ///
    /// Faithful to JE `UtilizationTracker.evictMemory`:
    /// * A file whose detail is at least 1 MB is flushed immediately,
    ///   regardless of the total (keeps eviction batches small).
    /// * Otherwise the largest flushable file is the candidate; it is flushed
    ///   only if the running total of small-file detail exceeds the budget.
    /// * Files pinned via [`TrackedFileSummary::set_allow_flush`]`(false)`
    ///   (JE `getUnflushableTrackedSummary` — a file the cleaner is actively
    ///   processing) are never chosen as the budget candidate.
    ///
    /// "Flush" here drops only the per-LSN OFFSET DETAIL and KEEPS the
    /// aggregate `FileSummary` counts (see
    /// [`TrackedFileSummary::discard_obsolete_detail`] for why this diverges
    /// from JE's persist-then-`reset`). A budget of 0 disables the cap.
    pub fn evict_memory(&mut self) -> i64 {
        // If not tracking detail, there is nothing to evict.
        // JE: `if (!cleaner.trackDetail) return 0;`
        if !self.track_detail || self.tracker_budget <= 0 {
            return 0;
        }

        let mut total_evicted: i64 = 0;
        let mut total_bytes: i64 = 0;
        let mut largest_bytes: i64 = 0;
        let mut best_file: Option<u32> = None;

        // First pass: flush ≥ 1 MB files immediately; find the largest
        // flushable small file. JE iterates `getTrackedFiles()`.
        for (&file_num, tfs) in self.tracked_files.iter() {
            let mem = tfs.detail_memory_size() as i64;
            if mem >= ONE_MB {
                continue;
            }
            total_bytes += mem;
            if mem > largest_bytes && tfs.get_allow_flush() {
                largest_bytes = mem;
                best_file = Some(file_num);
            }
        }

        // Drop the ≥ 1 MB files' detail (collected separately to avoid
        // mutating while iterating above).
        let big_files: Vec<u32> = self
            .tracked_files
            .iter()
            .filter(|(_, tfs)| tfs.detail_memory_size() as i64 >= ONE_MB)
            .map(|(&f, _)| f)
            .collect();
        for f in big_files {
            if let Some(tfs) = self.tracked_files.get_mut(&f) {
                let mem = tfs.detail_memory_size() as i64;
                tfs.discard_obsolete_detail();
                total_evicted += mem;
            }
        }

        // Then, if the small-file total exceeds the budget, flush the single
        // largest flushable file. JE flushes at most one to keep batches small.
        if total_bytes > self.tracker_budget
            && let Some(f) = best_file
            && let Some(tfs) = self.tracked_files.get_mut(&f)
        {
            tfs.discard_obsolete_detail();
            total_evicted += largest_bytes;
        }

        self.update_tracked_bytes();
        total_evicted
    }

    /// Returns the number of files being tracked.
    pub fn get_tracked_file_count(&self) -> usize {
        self.tracked_files.len()
    }

    /// Clears all tracked information.
    pub fn clear(&mut self) {
        self.tracked_files.clear();
        self.db_file_summaries.clear();
        self.tracked_bytes = 0;
    }

    /// Updates the tracked bytes count based on current tracked files.
    fn update_tracked_bytes(&mut self) {
        self.tracked_bytes =
            self.tracked_files.values().map(|t| t.memory_size() as i64).sum();
    }
}

impl Default for UtilizationTracker {
    fn default() -> Self {
        Self::new(true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let tracker = UtilizationTracker::new(true);
        assert_eq!(tracker.get_tracked_file_count(), 0);
        assert_eq!(tracker.get_bytes_tracked(), 0);
    }

    #[test]
    fn test_track_obsolete_ln() {
        let mut tracker = UtilizationTracker::new(true);
        tracker.track_obsolete(1, 100, 50, true);

        let tracked = tracker.get_tracked_summary(1).unwrap();
        assert_eq!(tracked.get_summary().obsolete_ln_count, 1);
        assert_eq!(tracked.get_summary().obsolete_ln_size, 50);
        assert_eq!(tracked.get_summary().obsolete_ln_size_counted, 1);
        assert_eq!(tracked.obsolete_offset_count(), 1);
    }

    /// CLN-F3: when an obsolete LN is tracked with size 0 ("the size is
    /// optional"), the count is incremented but the size and counted tallies
    /// that feed the avg-LN-size estimator must stay unchanged.
    ///
    /// JE: BaseUtilizationTracker.countObsoleteNode (~184-189) guards
    /// `obsoleteLNSize`/`obsoleteLNSizeCounted` on `size > 0`.
    #[test]
    fn test_track_obsolete_ln_size_zero_does_not_count_size() {
        let mut tracker = UtilizationTracker::new(true);
        tracker.track_obsolete(1, 100, 0, true);

        let tracked = tracker.get_tracked_summary(1).unwrap();
        assert_eq!(
            tracked.get_summary().obsolete_ln_count,
            1,
            "count still increments for a size-0 obsolete LN"
        );
        assert_eq!(
            tracked.get_summary().obsolete_ln_size,
            0,
            "CLN-F3: size must NOT accumulate when size <= 0"
        );
        assert_eq!(
            tracked.get_summary().obsolete_ln_size_counted,
            0,
            "CLN-F3: counted must NOT increment when size <= 0"
        );
    }

    #[test]
    fn test_track_obsolete_in() {
        let mut tracker = UtilizationTracker::new(true);
        // JE asserts size == 0 for obsolete INs ("not allowed").
        tracker.track_obsolete(1, 100, 0, false);

        let tracked = tracker.get_tracked_summary(1).unwrap();
        assert_eq!(tracked.get_summary().obsolete_in_count, 1);
        assert_eq!(tracked.get_summary().obsolete_ln_count, 0);
    }

    #[test]
    fn test_count_new_ln_entry() {
        let mut tracker = UtilizationTracker::new(true);
        tracker.count_new_log_entry(1, 100, true, false);

        let tracked = tracker.get_tracked_summary(1).unwrap();
        assert_eq!(tracked.get_summary().total_count, 1);
        assert_eq!(tracked.get_summary().total_size, 100);
        assert_eq!(tracked.get_summary().total_ln_count, 1);
        assert_eq!(tracked.get_summary().total_ln_size, 100);
        assert_eq!(tracked.get_summary().max_ln_size, 100);
    }

    #[test]
    fn test_count_new_in_entry() {
        let mut tracker = UtilizationTracker::new(true);
        tracker.count_new_log_entry(1, 200, false, true);

        let tracked = tracker.get_tracked_summary(1).unwrap();
        assert_eq!(tracked.get_summary().total_count, 1);
        assert_eq!(tracked.get_summary().total_size, 200);
        assert_eq!(tracked.get_summary().total_in_count, 1);
        assert_eq!(tracked.get_summary().total_in_size, 200);
    }

    #[test]
    fn test_max_ln_size_tracking() {
        let mut tracker = UtilizationTracker::new(true);
        tracker.count_new_log_entry(1, 50, true, false);
        tracker.count_new_log_entry(1, 100, true, false);
        tracker.count_new_log_entry(1, 75, true, false);

        let tracked = tracker.get_tracked_summary(1).unwrap();
        assert_eq!(tracked.get_summary().max_ln_size, 100);
    }

    #[test]
    fn test_multiple_files() {
        let mut tracker = UtilizationTracker::new(true);
        tracker.count_new_log_entry(1, 100, true, false);
        tracker.count_new_log_entry(2, 200, true, false);
        tracker.count_new_log_entry(3, 300, true, false);

        assert_eq!(tracker.get_tracked_file_count(), 3);
        assert!(tracker.get_tracked_summary(1).is_some());
        assert!(tracker.get_tracked_summary(2).is_some());
        assert!(tracker.get_tracked_summary(3).is_some());
    }

    #[test]
    fn test_track_detail_disabled() {
        let mut tracker = UtilizationTracker::new(false);
        tracker.track_obsolete(1, 100, 50, true);

        let tracked = tracker.get_tracked_summary(1).unwrap();
        // Counters should be updated
        assert_eq!(tracked.get_summary().obsolete_ln_count, 1);
        // But offsets should not be tracked
        assert_eq!(tracked.obsolete_offset_count(), 0);
    }

    #[test]
    fn test_remove_all_tracked_files() {
        let mut tracker = UtilizationTracker::new(true);
        tracker.count_new_log_entry(1, 100, true, false);
        tracker.count_new_log_entry(2, 200, true, false);

        let tracked_files = tracker.remove_all_tracked_files();
        assert_eq!(tracked_files.len(), 2);
        assert_eq!(tracker.get_tracked_file_count(), 0);
        assert_eq!(tracker.get_bytes_tracked(), 0);
    }

    #[test]
    fn test_clear() {
        let mut tracker = UtilizationTracker::new(true);
        tracker.count_new_log_entry(1, 100, true, false);
        tracker.track_obsolete(1, 100, 50, true);

        tracker.clear();

        assert_eq!(tracker.get_tracked_file_count(), 0);
        assert_eq!(tracker.get_bytes_tracked(), 0);
    }

    #[test]
    fn test_get_tracked_files_mut() {
        let mut tracker = UtilizationTracker::new(true);
        tracker.count_new_log_entry(1, 100, true, false);

        {
            let files = tracker.get_tracked_files_mut();
            if let Some(tracked) = files.get_mut(&1) {
                tracked.get_summary_mut().total_count += 10;
            }
        }

        let tracked = tracker.get_tracked_summary(1).unwrap();
        assert_eq!(tracked.get_summary().total_count, 11);
    }

    #[test]
    fn test_bytes_tracked_increases() {
        let mut tracker = UtilizationTracker::new(true);
        let initial_bytes = tracker.get_bytes_tracked();

        tracker.count_new_log_entry(1, 100, true, false);
        let after_entry = tracker.get_bytes_tracked();
        assert!(after_entry > initial_bytes);

        tracker.track_obsolete(1, 100, 50, true);
        let after_obsolete = tracker.get_bytes_tracked();
        assert!(after_obsolete >= after_entry);
    }

    #[test]
    fn test_accumulate_entries_same_file() {
        let mut tracker = UtilizationTracker::new(true);

        for i in 0..10 {
            tracker.count_new_log_entry(1, 100, true, false);
            // Offsets must be non-zero: JE never tracks the file-header
            // offset 0 as obsolete.
            tracker.track_obsolete(1, (i + 1) * 100, 50, true);
        }

        let tracked = tracker.get_tracked_summary(1).unwrap();
        assert_eq!(tracked.get_summary().total_count, 10);
        assert_eq!(tracked.get_summary().obsolete_ln_count, 10);
        assert_eq!(tracked.obsolete_offset_count(), 10);
    }

    /// JE `countObsoleteNode` (Exact): tracks the offset and updates the
    /// per-DB DbFileSummary.
    #[test]
    fn test_count_obsolete_node_exact_with_db() {
        let mut tracker = UtilizationTracker::new(true);
        tracker.count_new_log_entry_db(1, 80, true, false, Some(7));
        tracker.count_obsolete_node(1, 200, 80, true, Some(7));

        let tracked = tracker.get_tracked_summary(1).unwrap();
        assert_eq!(tracked.get_summary().obsolete_ln_count, 1);
        assert_eq!(tracked.obsolete_offset_count(), 1, "exact tracks offset");

        let db = tracker.get_db_file_summary(7, 1).unwrap();
        assert_eq!(db.total_ln_count, 1);
        assert_eq!(db.obsolete_ln_count, 1);
        assert_eq!(db.obsolete_ln_size, 80);
    }

    /// JE `countObsoleteNodeInexact`: does NOT track the offset (LSN is
    /// approximate) but still bumps the obsolete counters. Used for deleted /
    /// embedded LNs counted obsolete at write time (L-6).
    #[test]
    fn test_count_obsolete_node_inexact_skips_offset() {
        let mut tracker = UtilizationTracker::new(true);
        tracker.count_obsolete_node_inexact(1, 200, 80, true, Some(7));

        let tracked = tracker.get_tracked_summary(1).unwrap();
        assert_eq!(tracked.get_summary().obsolete_ln_count, 1);
        assert_eq!(
            tracked.obsolete_offset_count(),
            0,
            "inexact must NOT track the (approximate) offset"
        );
    }

    /// JE `countObsoleteNodeDupsAllowed`: the same offset may be counted
    /// twice (BIN-delta prevDeltaLsn = L-5, recovery) without firing the
    /// dedup assertion. Both counts land, and the offset is tracked twice.
    #[test]
    fn test_count_obsolete_node_dups_allowed() {
        let mut tracker = UtilizationTracker::new(true);
        tracker.count_obsolete_node_dups_allowed(1, 200, 80, true, Some(7));
        // Counting the SAME offset again must not panic on the Exact dedup
        // assertion, because DupsAllowed sets checkDupOffsets=false.
        tracker.count_obsolete_node_dups_allowed(1, 200, 80, true, Some(7));

        let tracked = tracker.get_tracked_summary(1).unwrap();
        assert_eq!(
            tracked.get_summary().obsolete_ln_count,
            2,
            "both counts land (double-count is legitimate here)"
        );
        assert_eq!(
            tracked.obsolete_offset_count(),
            2,
            "dups-allowed tracks the offset each time"
        );
    }

    /// JE `countObsoleteDb` ([#19144]): on DB remove/truncate, the still-active
    /// bytes (total - already-obsolete) become obsolete, and the obsolete-LN
    /// size is healed so every LN byte is counted.
    #[test]
    fn test_count_obsolete_db() {
        let mut tracker = UtilizationTracker::new(true);
        // DB 7 writes 3 LNs of 100 bytes each into file 1; one already went
        // obsolete (with size counted).
        tracker.count_new_log_entry_db(1, 100, true, false, Some(7));
        tracker.count_new_log_entry_db(1, 100, true, false, Some(7));
        tracker.count_new_log_entry_db(1, 100, true, false, Some(7));
        tracker.count_obsolete_node(1, 300, 100, true, Some(7));

        // Before remove: 1 of 3 LNs obsolete.
        let before = tracker.get_tracked_summary(1).unwrap();
        assert_eq!(before.get_summary().obsolete_ln_count, 1);

        tracker.count_obsolete_db(7);

        // After remove: all 3 LNs obsolete, all 300 bytes counted.
        let after = tracker.get_tracked_summary(1).unwrap();
        assert_eq!(after.get_summary().obsolete_ln_count, 3);
        assert_eq!(after.get_summary().obsolete_ln_size, 300);
        assert_eq!(after.get_summary().obsolete_ln_size_counted, 3);
        // Per-DB summaries are dropped after counting.
        assert!(tracker.get_db_file_summary(7, 1).is_none());
    }

    #[test]
    fn test_default() {
        let tracker = UtilizationTracker::default();
        assert_eq!(tracker.get_tracked_file_count(), 0);
    }

    /// DBI-24 HEADLINE: a tracker that falls far behind accumulates
    /// obsolete-offset detail for many files. With a budget set, repeated
    /// `evict_memory` (JE `UtilizationTracker.evictMemory`, called
    /// frequently) keeps memory bounded by dropping the per-LSN OFFSET
    /// DETAIL — while the AGGREGATE obsolete counts that drive util%
    /// file-selection are PRESERVED.
    ///
    /// Fail-pre on main: there is no budget/evict path, so detail grows
    /// unbounded. Pass-post: detail is flushed at the budget, aggregates
    /// intact. Cite: `UtilizationTracker.evictMemory`,
    /// `MemoryBudget.getTrackerBudget`, `TrackedFileSummary.reset`.
    #[test]
    fn test_evict_memory_bounds_detail_preserves_aggregates() {
        const FILES: u32 = 200;
        const OFFSETS_PER_FILE: u32 = 500;

        // A tight budget: far smaller than the unbounded detail will be.
        let budget = 64 * 1024;
        let mut tracker = UtilizationTracker::with_budget(true, budget);

        // Accumulate a lot of obsolete-offset detail across many files.
        for file in 1..=FILES {
            for i in 1..=OFFSETS_PER_FILE {
                // size 50, count as LN; offset must be non-zero.
                tracker.count_obsolete_node(file, i, 50, true, None);
            }
        }

        // Snapshot the aggregate obsolete counts BEFORE eviction.
        let mut expected_obsolete: HashMap<u32, i32> = HashMap::new();
        let mut total_offsets_before: usize = 0;
        for (&f, tfs) in tracker.get_tracked_files() {
            expected_obsolete.insert(f, tfs.get_summary().obsolete_ln_count);
            total_offsets_before += tfs.obsolete_offset_count();
        }
        assert_eq!(
            total_offsets_before as u32,
            FILES * OFFSETS_PER_FILE,
            "all offsets tracked before eviction"
        );
        let usage_before = tracker.get_memory_usage();
        assert!(
            usage_before > budget,
            "detail must exceed budget before eviction ({usage_before} <= {budget})"
        );

        // JE calls evictMemory repeatedly (one small file per call); loop
        // until it stops freeing.
        let mut iters = 0;
        loop {
            let freed = tracker.evict_memory();
            iters += 1;
            if freed == 0 {
                break;
            }
            assert!(iters < 100_000, "evict_memory must converge");
        }

        // BOUNDED MEMORY: after eviction converges, the small-file detail
        // total is within the budget (the one largest flushable file is
        // flushed whenever the total exceeds it, so the residual is bounded).
        let usage_after = tracker.get_memory_usage();
        assert!(
            usage_after <= budget,
            "detail must be bounded by budget after eviction ({usage_after} > {budget})"
        );

        // AGGREGATES INTACT: every file's obsolete_ln_count is unchanged —
        // dropping offset detail must NOT change the aggregate util%.
        for (&f, &expected) in &expected_obsolete {
            let got = tracker
                .get_tracked_summary(f)
                .expect("file still tracked")
                .get_summary()
                .obsolete_ln_count;
            assert_eq!(
                got, expected,
                "file {f}: aggregate obsolete count must survive a detail flush"
            );
        }
    }

    /// `evict_memory` must NOT flush a file the cleaner has pinned via
    /// `set_allow_flush(false)` (JE `getUnflushableTrackedSummary`).
    #[test]
    fn test_evict_memory_respects_allow_flush() {
        let budget = 1024;
        let mut tracker = UtilizationTracker::with_budget(true, budget);
        for i in 1..=2000 {
            tracker.count_obsolete_node(1, i, 50, true, None);
        }
        // Pin file 1 so it is never chosen as the budget candidate.
        tracker.get_tracked_summary_mut(1).unwrap().set_allow_flush(false);
        let offsets_before =
            tracker.get_tracked_summary(1).unwrap().obsolete_offset_count();

        // Loop; since the only file is pinned (and < 1 MB), nothing is freed.
        for _ in 0..10 {
            tracker.evict_memory();
        }
        assert_eq!(
            tracker.get_tracked_summary(1).unwrap().obsolete_offset_count(),
            offsets_before,
            "a pinned (allow_flush=false) file must not be evicted"
        );
    }

    /// A budget of 0 disables the cap (legacy `new`): detail grows unbounded.
    #[test]
    fn test_evict_memory_disabled_with_zero_budget() {
        let mut tracker = UtilizationTracker::new(true); // budget 0
        for i in 1..=1000 {
            tracker.count_obsolete_node(1, i, 50, true, None);
        }
        let freed = tracker.evict_memory();
        assert_eq!(freed, 0, "zero budget disables eviction");
        assert_eq!(
            tracker.get_tracked_summary(1).unwrap().obsolete_offset_count(),
            1000
        );
    }
}
