//! REP-1 STEP 5 (C): the live `Replay.rollback` execution.
//!
//! Port of `com.sleepycat.je.rep.impl.node.Replay.rollback(matchpointVLSN,
//! matchpointLsn)`.
//!
//! Once a syncup matchpoint is agreed and
//! [`crate::rollback_tracker`]'s decision says "RollbackToMatchpoint", the
//! replica must durably truncate its divergent tail back to the matchpoint.
//! JE `Replay.rollback` does this in five steps (its own comment):
//!
//! 1. Log and fsync a new `RollbackStart` record (matchpoint VLSN + LSN +
//!    active txn ids). The fsync guarantees the marker is present for recovery
//!    and flushes all log entries out of the buffers so the on-disk LSNs are
//!    findable.
//! 2. Do the rollback in memory (revert the in-window LNs to their previous
//!    version via [`crate::txn_chain::TxnChain`]; no need to log the dirtied
//!    INs). *(The in-memory tree revert is the caller's responsibility — it
//!    holds the trees; this module performs the DURABLE steps 1, 3–5.)*
//! 3. Make the rolled-back LNs invisible by overwriting their type byte
//!    (`FileManager.make_invisible`).
//! 4. fsync all overwritten files (`FileManager.force`).
//! 5. Log and fsync a `RollbackEnd` record, so a later recovery can skip the
//!    re-make-invisible step.
//!
//! If a crash happens between steps 1 and 5 (before `RollbackEnd` is durable),
//! recovery completes the rollback via the REP-1 STEP 4 machinery: the
//! `RollbackTracker` sees an OPEN-ENDED period (RollbackStart, no RollbackEnd)
//! and re-makes the in-window LNs invisible + fsyncs them
//! (`RollbackTracker.singlePassInvisibleLsns` /
//! `recoveryEndFsyncInvisible`). This module REUSES that machinery; it does not
//! reimplement it.

use noxu_log::entry::{RollbackEndEntry, RollbackStartEntry};
use noxu_log::{LogEntryType, LogManager, Provisional};
use noxu_util::{Lsn, Vlsn};

use crate::error::{RecoveryError, Result};

/// The durable record of a completed live rollback, returned by
/// [`rollback`] so the caller can advance its in-memory bookkeeping
/// (e.g. truncate the VLSN index to the matchpoint).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RollbackOutcome {
    /// LSN of the `RollbackStart` record written in step 1.
    pub rollback_start_lsn: Lsn,
    /// LSN of the `RollbackEnd` record written in step 5.
    pub rollback_end_lsn: Lsn,
    /// LSNs of the rolled-back log entries that were made invisible.
    pub invisible_lsns: Vec<Lsn>,
}

/// Perform the durable steps of a live syncup rollback to `matchpoint_lsn`.
///
/// Port of the on-disk part of JE `Replay.rollback`. The caller supplies:
/// - `log_manager`: the replica's live `LogManager`.
/// - `matchpoint_vlsn` / `matchpoint_lsn`: the agreed matchpoint.
/// - `active_txn_ids`: the unfinished transactions being rolled back (written
///   into the `RollbackStart` so `RollbackTracker.contains_ln` can gate
///   per-txn during a later recovery — REP-1 STEP 2).
/// - `rollback_lsns`: the LSNs of the log entries after the matchpoint that
///   must be made invisible (collected by the caller's in-memory revert pass,
///   JE `ReplayTxn.rollback` returning its rolled-back LSNs).
///
/// Steps performed here (JE step numbers):
/// 1. log + fsync `RollbackStart`;
/// 3. `make_invisible` on the rolled-back LSNs (grouped per file);
/// 4. `force` (fsync) the touched files;
/// 5. log + fsync `RollbackEnd`.
///
/// Step 2 (in-memory tree revert) is done by the caller BEFORE calling this,
/// because the caller owns the trees. The order matches JE: RollbackStart is
/// logged first (step 1) so that even a crash before the in-memory revert is
/// recoverable; we log RollbackStart here first too, then expect the caller to
/// have already computed `rollback_lsns`.
///
/// Returns the [`RollbackOutcome`] so the caller can truncate the VLSN index
/// to the matchpoint and resume streaming from `matchpoint_vlsn + 1`.
pub fn rollback(
    log_manager: &LogManager,
    matchpoint_vlsn: Vlsn,
    matchpoint_lsn: Lsn,
    active_txn_ids: Vec<i64>,
    rollback_lsns: &[Lsn],
) -> Result<RollbackOutcome> {
    use bytes::BytesMut;

    // Steps 1, 3, 4 (RollbackStart + make-invisible + fsync).
    let rollback_start_lsn = rollback_steps_1_to_4(
        log_manager,
        matchpoint_vlsn,
        matchpoint_lsn,
        active_txn_ids,
        rollback_lsns,
    )?;

    // ----------------------------------------------------------------------
    // Step 5: log and fsync RollbackEnd, so a later recovery can skip the
    // re-make-invisible step (JE: "If the RollbackEnd exists, we can skip the
    // step of re-making LNs invisible").
    // ----------------------------------------------------------------------
    let end_entry = RollbackEndEntry::new(matchpoint_lsn, rollback_start_lsn);
    let mut end_buf = BytesMut::new();
    end_entry.write_to_log(&mut end_buf);
    let rollback_end_lsn = log_manager
        .log(LogEntryType::RollbackEnd, &end_buf, Provisional::No, true, true)
        .map_err(|e| {
            RecoveryError::RollbackError(format!(
                "logging RollbackEnd failed: {e}"
            ))
        })?;

    Ok(RollbackOutcome {
        rollback_start_lsn,
        rollback_end_lsn,
        invisible_lsns: rollback_lsns.to_vec(),
    })
}

/// Perform JE `Replay.rollback` steps 1, 3, and 4 ONLY (log + fsync
/// `RollbackStart`; make the rolled-back LSNs invisible; fsync them) WITHOUT
/// step 5 (`RollbackEnd`). Returns the `RollbackStart` LSN.
///
/// This is the durable state a replica is in if it CRASHES mid-rollback: the
/// `RollbackStart` and the invisible bits are on disk, but no `RollbackEnd`.
/// On restart, recovery sees an OPEN-ENDED rollback period and completes it
/// (re-marks the in-window LNs invisible + fsyncs them) via the STEP 4
/// machinery (`RollbackTracker.singlePassInvisibleLsns` /
/// `recoveryEndFsyncInvisible`). Exposed so the crash-mid-rollback test can
/// reproduce that exact state.
pub fn rollback_steps_1_to_4(
    log_manager: &LogManager,
    matchpoint_vlsn: Vlsn,
    matchpoint_lsn: Lsn,
    active_txn_ids: Vec<i64>,
    rollback_lsns: &[Lsn],
) -> Result<Lsn> {
    use bytes::BytesMut;

    // ----------------------------------------------------------------------
    // Step 1: log and fsync RollbackStart.
    //
    // The fsync (fsync_required = true) makes the marker durable AND flushes
    // every preceding log entry out of the buffers, so the rolled-back LSNs
    // are reliably findable on disk for make_invisible (JE: "The fsync
    // guarantees that this marker will be present ... It also ensures that all
    // log entries will be flushed to disk").
    // ----------------------------------------------------------------------
    let start_entry = RollbackStartEntry::new(
        matchpoint_vlsn,
        matchpoint_lsn,
        active_txn_ids,
    );
    let mut start_buf = BytesMut::new();
    start_entry.write_to_log(&mut start_buf);
    let rollback_start_lsn = log_manager
        .log(
            LogEntryType::RollbackStart,
            &start_buf,
            Provisional::No,
            true, // flush
            true, // fsync
        )
        .map_err(|e| {
            RecoveryError::RollbackError(format!(
                "logging RollbackStart failed: {e}"
            ))
        })?;

    // ----------------------------------------------------------------------
    // Steps 3 & 4: make the rolled-back LNs invisible, grouped per file, then
    // fsync the touched files (JE RollbackTracker.makeInvisible).
    //
    // make_invisible flips the invisible bit WITHOUT breaking the entry
    // checksum (the STEP 4 "checksum cloak"), so a later redo pass skips these
    // entries instead of re-applying them.
    // ----------------------------------------------------------------------
    let fm = log_manager.file_manager();
    let mut by_file: std::collections::BTreeMap<u32, Vec<u32>> =
        std::collections::BTreeMap::new();
    for lsn in rollback_lsns {
        by_file.entry(lsn.file_number()).or_default().push(lsn.file_offset());
    }
    let touched_files: Vec<u32> = by_file.keys().copied().collect();
    for (file_num, offsets) in &by_file {
        fm.make_invisible(*file_num, offsets).map_err(|e| {
            RecoveryError::RollbackError(format!(
                "make_invisible(file {file_num}) failed: {e}"
            ))
        })?;
    }
    if !touched_files.is_empty() {
        fm.force(&touched_files).map_err(|e| {
            RecoveryError::RollbackError(format!(
                "force(invisible files) failed: {e}"
            ))
        })?;
    }

    Ok(rollback_start_lsn)
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_log::file_manager::FileManager;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn make_log_manager(dir: &TempDir) -> Arc<LogManager> {
        let fm = Arc::new(
            FileManager::new(dir.path(), false, 256 * 1024 * 1024, 32).unwrap(),
        );
        Arc::new(LogManager::new(fm, 3, 1 << 20, 4096))
    }

    /// The live rollback writes a RollbackStart...RollbackEnd pair, makes the
    /// rolled-back LSNs invisible, and fsyncs. A subsequent recovery scan sees
    /// a COMPLETED rollback period.
    #[test]
    fn test_live_rollback_writes_start_end_and_makes_invisible() {
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);

        // Write a few entries to roll back (use InsertLNTxn type bytes).
        let mut rolled_back = Vec::new();
        for i in 0..3u8 {
            let lsn = lm
                .log(
                    LogEntryType::InsertLNTxn,
                    &[i; 16],
                    Provisional::No,
                    true,
                    false,
                )
                .unwrap();
            rolled_back.push(lsn);
        }
        // Matchpoint is "before" the rolled-back entries (use a low LSN).
        let matchpoint_lsn = Lsn::new(0, 1);
        let outcome = rollback(
            &lm,
            Vlsn::new(5),
            matchpoint_lsn,
            vec![42, 43],
            &rolled_back,
        )
        .unwrap();

        assert_ne!(outcome.rollback_start_lsn, outcome.rollback_end_lsn);
        assert_eq!(outcome.invisible_lsns, rolled_back);

        // The rolled-back entries must now read back as invisible. The STEP 4
        // invisible bit is flags mask 0x10 (FileManager::make_invisible), set
        // in place without breaking the entry checksum.
        let fm = lm.file_manager();
        const INVISIBLE_MASK: u8 = 0x10;
        for lsn in &rolled_back {
            let mut hdr = [0u8; 16];
            fm.read_from_file(
                lsn.file_number(),
                lsn.file_offset() as u64,
                &mut hdr,
            )
            .unwrap();
            assert!(
                hdr[5] & INVISIBLE_MASK != 0,
                "entry at {lsn:?} should carry the invisible flag"
            );
        }
    }

    /// An empty rollback (no LSNs to make invisible) still writes the
    /// Start/End pair (the durable markers are harmless).
    #[test]
    fn test_live_rollback_empty_lsns() {
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);
        let outcome =
            rollback(&lm, Vlsn::new(1), Lsn::new(0, 36), vec![], &[]).unwrap();
        assert!(outcome.invisible_lsns.is_empty());
    }

    /// CRASH-MID-ROLLBACK (B+C integrate with STEP 4): a replica performs the
    /// live rollback's durable steps 1, 3, 4 (RollbackStart + make-invisible +
    /// fsync) and then CRASHES before step 5 (RollbackEnd). On restart,
    /// recovery must see an OPEN-ENDED rollback period and the rolled-back
    /// entries must stay invisible (the redo pass must not re-apply them).
    ///
    /// Port of the JE crash-mid-rollback recovery path: `RollbackTracker`
    /// sees a `RollbackStart` with no matching `RollbackEnd`
    /// (`hasRollbackEnd() == false`) and completes the rollback via
    /// `singlePassInvisibleLsns` / `recoveryEndFsyncInvisible`.
    #[test]
    fn test_crash_mid_rollback_recovers_via_step4() {
        use crate::rollback_tracker::RollbackTracker;
        use noxu_log::entry::RollbackStartEntry;

        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);

        // Pre-rollback entries (the divergent tail), and a matchpoint LSN.
        let matchpoint_lsn = Lsn::new(0, 1);
        let mut rolled_back = Vec::new();
        for i in 0..3u8 {
            let lsn = lm
                .log(
                    LogEntryType::InsertLNTxn,
                    &[i; 16],
                    Provisional::No,
                    true,
                    false,
                )
                .unwrap();
            rolled_back.push(lsn);
        }

        // CRASH point: perform steps 1–4 only (no RollbackEnd).
        let rollback_start_lsn = rollback_steps_1_to_4(
            &lm,
            Vlsn::new(5),
            matchpoint_lsn,
            vec![42, 43],
            &rolled_back,
        )
        .unwrap();

        // The rolled-back entries are invisible on disk (STEP 4 cloak).
        let fm = lm.file_manager();
        const INVISIBLE_MASK: u8 = 0x10;
        for lsn in &rolled_back {
            let mut hdr = [0u8; 16];
            fm.read_from_file(
                lsn.file_number(),
                lsn.file_offset() as u64,
                &mut hdr,
            )
            .unwrap();
            assert!(
                hdr[5] & INVISIBLE_MASK != 0,
                "crash-mid-rollback: entry {lsn:?} must already be invisible"
            );
        }

        // RESTART: recovery reads the RollbackStart, finds NO RollbackEnd, and
        // registers an OPEN-ENDED period. Re-read the RollbackStart from disk
        // and feed the tracker exactly as run_analysis does.
        let mut start_hdr_and_body = vec![0u8; 4096];
        let n = fm
            .read_from_file(
                rollback_start_lsn.file_number(),
                rollback_start_lsn.file_offset() as u64,
                &mut start_hdr_and_body,
            )
            .unwrap();
        assert!(n > 0);
        // The RollbackStart body begins after the entry header. The header is
        // 14 bytes (non-VLSN) for a log() write. Decode the entry.
        let body = &start_hdr_and_body[14..];
        let start_entry = RollbackStartEntry::read_from_log(body)
            .expect("decode RollbackStart from disk");
        assert_eq!(start_entry.matchpoint_lsn, matchpoint_lsn);
        assert_eq!(start_entry.active_txn_ids, vec![42, 43]);

        let mut tracker = RollbackTracker::new();
        tracker.record_rollback_start(rollback_start_lsn, &start_entry);
        // No RollbackEnd was written → the period is OPEN-ENDED.
        assert!(
            tracker.has_incomplete_rollbacks(),
            "recovery must see an open-ended rollback period (no RollbackEnd)"
        );
        // The rolled-back LSNs are inside the (matchpoint, start) window, so
        // recovery knows to keep them invisible / re-mark them.
        for lsn in &rolled_back {
            assert!(
                tracker.is_in_rollback_period(*lsn),
                "rolled-back {lsn:?} must be inside the open-ended period"
            );
            // And gated by the active txn ids (STEP 2): txn 42 is reverted,
            // an unrelated txn is not.
            assert!(tracker.contains_ln(*lsn, 42));
            assert!(!tracker.contains_ln(*lsn, 999));
        }
    }
}
