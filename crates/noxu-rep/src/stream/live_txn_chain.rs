//! Gap A, step 1: the LIVE `TxnChain` source.
//!
//! Port of the data-gathering half of JE `TxnChain`'s constructor
//! (`~/ws/je/src/com/sleepycat/je/txn/TxnChain.java:109-260`) driven from a
//! live replica's WAL instead of recovery's `AnalysisResult`.
//!
//! ## Why this exists (see `docs/src/operations/known-limitations.md`, Gap A)
//!
//! [`noxu_recovery::TxnChain::build`] is already a faithful, unit-tested
//! port of JE `TxnChain`'s backward revert-info walk. It takes a
//! *pre-collected* `Vec<(Lsn, LnRecord)>` of one transaction's LN logrecs —
//! it never reads the WAL itself. During crash recovery, that Vec comes from
//! `RecoveryManager::build_rollback_chains`'s forward scan of the log
//! (`recovery_manager.rs` ~1093-1160). During LIVE syncup there is no
//! equivalent collector: `noxu_dbi::ReplicaReplay` buffers a still-active
//! txn's LNs only until commit, then DISCARDS the buffer (`commit_txn`
//! drains it into the tree and drops the Vec; `abort_txn` just drops it) —
//! there is nothing left in memory to walk backward through once a txn has
//! resolved. So a live syncup rollback needs a **fresh WAL re-scan**, driven
//! by the same primitives `RecoveryManager::build_rollback_chains` uses
//! (`noxu_recovery::LogScanner::scan_forward` decoding `LogEntry::Ln`
//! records), but keyed by a single `(txn_id, matchpoint_lsn)` pair supplied
//! by the syncup driver rather than by a `RollbackPeriod` from
//! `RollbackTracker` (which is recovery-time-only; see
//! `crates/noxu-recovery/src/rollback_tracker.rs`).
//!
//! JE's OWN live path (`ReplayTxn.undoWrites`,
//! `~/ws/je/src/com/sleepycat/je/rep/txn/ReplayTxn.java:496-524`) does not
//! need a re-scan at all: it walks `currLsn = lastLoggedLsn` backward by
//! following an explicit "prev LSN of same txn" pointer embedded in every
//! on-disk transactional `LNLogEntry`
//! (`~/ws/je/src/com/sleepycat/je/log/entry/LNLogEntry.java:58-59`).
//! Noxu's on-disk `LnLogEntry` format carries no such pointer (confirmed by
//! `crates/noxu-log/src/entry/ln_log_entry.rs`'s `write_to_log`/
//! `parse_from_slice`): the transactional payload has `abort_lsn` (the
//! *pre-txn* version) and `txn_id`, but no chain-of-this-txn's-own-writes
//! LSN. A forward re-scan filtering by `txn_id` is therefore the only way to
//! reconstruct "this txn's other logrecs" from the WAL — exactly what
//! `RecoveryManager::build_rollback_chains` already does for recovery.
//!
//! ## What this module does NOT do
//!
//! It does not decide whether a computed chain may be USED to admit a
//! previously-refused case. See the module doc on
//! [`crate::stream::syncup::classify_tail`] for the safety bar; this module
//! is Gap A step 1 (the pure, testable data source) plus step 2 (proving the
//! computed set is correct), not step 4 (narrowing the refusal). No call site
//! in this module changes `classify_tail`'s verdict.

use std::sync::Arc;

use noxu_dbi::FileManagerLogScanner;
use noxu_log::file_manager::FileManager;
use noxu_recovery::{KeyCmp, LnRecord, LogEntry, LogScanner, TxnChain};
use noxu_util::{Lsn, NULL_LSN};

/// Build a live [`TxnChain`] for `txn_id` by re-scanning `fm`'s WAL.
///
/// Port of the data-gathering half of JE `TxnChain`'s constructor, driven by
/// a live re-scan rather than an in-memory `lastLoggedLsn` chain pointer (see
/// the module doc for why Noxu's on-disk format cannot support the latter).
///
/// - `fm`: the replica's live [`FileManager`] — the same one
///   [`crate::stream::syncup_reader::SyncupLogView::scan_with_manager`]
///   re-reads for the matchpoint search, so this adds no new file handle.
/// - `txn_id`: the transaction whose chain to build.
/// - `matchpoint_lsn`: the verified syncup matchpoint. Logrecs at or below
///   this LSN are the txn's PRESERVED versions (possible revert targets);
///   logrecs above it are ROLLED BACK.
/// - `scan_from`: where the forward scan starts. Recovery's
///   `build_rollback_chains` always starts at `NULL_LSN` (it already walks
///   the whole log for other passes); a live syncup rollback is latency
///   sensitive, so the caller may pass `matchpoint_lsn` instead — any
///   pre-matchpoint revert target is, by definition, AT OR BEFORE the
///   matchpoint LSN, so starting exactly there (not after it) still captures
///   every logrec `TxnChain::build` can need. Passing `NULL_LSN` is always
///   correct too (just does more work); this parameter exists to let callers
///   choose the recovery-identical unbounded scan for tests, and the bounded
///   scan for the production call site.
/// - `scan_to`: exclusive upper bound of the forward scan. The caller passes
///   `last_logged_lsn + 1 offset unit` (or `NULL_LSN` for "to end of log");
///   `TxnChain::build` only needs logrecs at or below the txn's true
///   last-logged LSN, so bounding the scan there avoids reading log entries
///   this rollback will never touch.
/// - `cmp`: the key comparator for the txn's database(s) (same contract as
///   `TxnChain::build`'s own `cmp` parameter).
///
/// Returns `None` if the scan finds no logrec for `txn_id` at all (nothing to
/// build — the caller should treat this as an empty chain, not an error).
pub fn build_live_chain(
    fm: &Arc<FileManager>,
    txn_id: u64,
    matchpoint_lsn: Lsn,
    scan_from: Lsn,
    scan_to: Lsn,
    cmp: KeyCmp<'_>,
) -> Option<TxnChain> {
    let scanner = FileManagerLogScanner::new(Arc::clone(fm));
    let entries = scanner.scan_forward(scan_from, scan_to);

    let mut logrecs: Vec<(Lsn, LnRecord)> = Vec::new();
    for pe in entries {
        if let LogEntry::Ln(rec) = pe.entry
            && rec.txn_id == Some(txn_id)
        {
            logrecs.push((pe.lsn, rec));
        }
    }

    if logrecs.is_empty() {
        return None;
    }

    Some(TxnChain::build(logrecs, matchpoint_lsn, cmp))
}

/// Convenience wrapper: unbounded scan from the start of the log, matching
/// `RecoveryManager::build_rollback_chains`'s own `NULL_LSN` start exactly
/// (used by tests that want to assert parity with the recovery-time
/// collector, and by any caller that cannot cheaply bound `scan_from`).
pub fn build_live_chain_unbounded(
    fm: &Arc<FileManager>,
    txn_id: u64,
    matchpoint_lsn: Lsn,
    cmp: KeyCmp<'_>,
) -> Option<TxnChain> {
    build_live_chain(fm, txn_id, matchpoint_lsn, NULL_LSN, NULL_LSN, cmp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use noxu_log::entry::LnLogEntry;
    use noxu_log::{LogEntryType, LogManager, Provisional};
    use noxu_util::vlsn::NULL_VLSN;
    use tempfile::TempDir;

    fn make_fm(dir: &std::path::Path) -> (Arc<FileManager>, Arc<LogManager>) {
        let fm = Arc::new(
            FileManager::new(dir, false, 64 * 1024 * 1024, 100).unwrap(),
        );
        let lm =
            Arc::new(LogManager::new(Arc::clone(&fm), 3, 1024 * 1024, 65536));
        (fm, lm)
    }

    /// Write one transactional LN entry to the real WAL via `LogManager`,
    /// returning the LSN it landed at. Mirrors the on-disk shape
    /// `CursorImpl::log_ln_write` produces (see cursor_impl.rs ~3409-3460):
    /// `abort_lsn`/`abort_data` carry the txn's PRE-TXN before-image, exactly
    /// what `TxnChain::from_abort` reads.
    #[allow(clippy::too_many_arguments)]
    fn write_ln(
        lm: &LogManager,
        db_id: u64,
        txn_id: i64,
        key: &[u8],
        data: Option<&[u8]>,
        abort_lsn: Lsn,
        abort_known_deleted: bool,
        abort_data: Option<&[u8]>,
    ) -> Lsn {
        let entry = LnLogEntry::new(
            db_id,
            Some(txn_id),
            abort_lsn,
            abort_known_deleted,
            None,
            abort_data.map(|d| d.to_vec()),
            NULL_VLSN,
            0,
            true,
            key.to_vec(),
            data.map(|d| d.to_vec()),
            0,
            NULL_VLSN,
        );
        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);
        let ty = if data.is_some() {
            LogEntryType::InsertLNTxn
        } else {
            LogEntryType::DeleteLNTxn
        };
        lm.log(ty, &buf, Provisional::No, true, false).unwrap()
    }

    /// HEADLINE: the live re-scan must reproduce EXACTLY what
    /// `TxnChain::build` computes from a hand-built `Vec` for the same
    /// logrecs — same case as `txn_chain.rs`'s
    /// `test_intra_txnal_revert_to_v1_not_skip_both` (an in-window write must
    /// revert to the PRESERVED previous version, not skip to the pre-txn
    /// abort), but sourced from a real on-disk WAL via a real `FileManager`
    /// instead of a hand-built `Vec<(Lsn, LnRecord)>`.
    #[test]
    fn test_live_chain_matches_hand_built_chain_intra_txnal() {
        let dir = TempDir::new().unwrap();
        let (fm, lm) = make_fm(dir.path());

        // v1 (preserved, at/before matchpoint) then v2 (rolled back).
        let lsn_v1 =
            write_ln(&lm, 7, 1, b"A", Some(b"v1"), NULL_LSN, true, None);
        let matchpoint = lsn_v1;
        let lsn_v2 =
            write_ln(&lm, 7, 1, b"A", Some(b"v2"), NULL_LSN, true, None);
        lm.flush_sync().unwrap();

        let mut chain = build_live_chain_unbounded(
            &fm,
            1,
            matchpoint,
            &|a: &[u8], b: &[u8]| a.cmp(b),
        )
        .expect("txn 1 has logrecs");

        assert_eq!(chain.len(), 1, "only v2 (above matchpoint) is rolled back");
        let ri = chain.pop().unwrap();
        assert_eq!(
            ri.revert_lsn, lsn_v1,
            "v2 must revert to the PRESERVED v1, not skip to pre-txn"
        );
        assert!(!ri.revert_kd);
        assert_eq!(ri.revert_data.as_deref(), Some(&b"v1"[..]));
        assert_eq!(chain.remaining_locked_nodes(), &[lsn_v1]);

        // sanity: v2 really landed above the matchpoint (else the test
        // would trivially pass with an empty chain).
        assert!(lsn_v2 > matchpoint);
    }

    /// JE's own worked example (`TxnChain.java`'s class doc), sourced from a
    /// real WAL: two slots, interleaved writes, matchpoint before all of
    /// them. Parity with `txn_chain::tests::test_je_worked_example_two_slots`,
    /// which proves the same case against a hand-built `Vec`.
    #[test]
    fn test_live_chain_je_worked_example_two_slots() {
        let dir = TempDir::new().unwrap();
        let (fm, lm) = make_fm(dir.path());

        // Pre-txn versions (txn 0, standing in for "not this txn" — any
        // other txn id, or non-transactional, works; use a different txn id
        // so the scan's txn_id filter is genuinely exercised).
        let pretxn_a =
            write_ln(&lm, 7, 99, b"A", Some(b"10"), NULL_LSN, true, None);
        let pretxn_b =
            write_ln(&lm, 7, 99, b"B", Some(b"100"), NULL_LSN, true, None);
        let matchpoint = pretxn_b;

        let lsn_200 = write_ln(
            &lm,
            7,
            1,
            b"A",
            Some(b"20"),
            pretxn_a,
            false,
            Some(b"10"),
        );
        let lsn_300 =
            write_ln(&lm, 7, 1, b"A", None, pretxn_a, false, Some(b"10"));
        let lsn_400 = write_ln(
            &lm,
            7,
            1,
            b"B",
            Some(b"200"),
            pretxn_b,
            false,
            Some(b"100"),
        );
        assert!(lsn_400 > matchpoint, "sanity: txn-1 write above matchpoint");
        let lsn_500 = write_ln(
            &lm,
            7,
            1,
            b"A",
            Some(b"30"),
            pretxn_a,
            false,
            Some(b"10"),
        );
        lm.flush_sync().unwrap();

        let mut chain = build_live_chain_unbounded(
            &fm,
            1,
            matchpoint,
            &|a: &[u8], b: &[u8]| a.cmp(b),
        )
        .expect("txn 1 has logrecs");
        assert_eq!(chain.len(), 4, "all 4 txn-1 logrecs are above matchpoint");

        let ri500 = chain.pop().unwrap();
        assert_eq!(ri500.revert_lsn, lsn_300, "A=30 reverts to A=del @300");
        assert!(ri500.revert_pd);

        let ri400 = chain.pop().unwrap();
        assert_eq!(ri400.revert_lsn, pretxn_b, "B reverts to pre-txn B");

        let ri300 = chain.pop().unwrap();
        assert_eq!(ri300.revert_lsn, lsn_200, "A=del reverts to A=20 @200");

        let ri200 = chain.pop().unwrap();
        assert_eq!(ri200.revert_lsn, pretxn_a, "A=20 reverts to pre-txn A");

        // sanity: none of the txn-99 pre-txn writes leaked into txn 1's chain.
        assert!(lsn_500 > matchpoint);
    }

    /// The BOUNDED scan (starting exactly at `matchpoint_lsn`, not
    /// `NULL_LSN`) must still find a preserved revert target that was
    /// written AT the matchpoint LSN itself — the off-by-one this bound
    /// could otherwise introduce (flagged in `.agent/notes-gapa2.md`).
    #[test]
    fn test_bounded_scan_from_matchpoint_still_sees_preserved_target() {
        let dir = TempDir::new().unwrap();
        let (fm, lm) = make_fm(dir.path());

        let lsn_v1 =
            write_ln(&lm, 7, 1, b"A", Some(b"v1"), NULL_LSN, true, None);
        let matchpoint = lsn_v1; // matchpoint IS the preserved logrec's LSN.
        let _lsn_v2 =
            write_ln(&lm, 7, 1, b"A", Some(b"v2"), NULL_LSN, true, None);
        lm.flush_sync().unwrap();

        // Bounded: start exactly at matchpoint, not NULL_LSN.
        let mut chain = build_live_chain(
            &fm,
            1,
            matchpoint,
            matchpoint,
            NULL_LSN,
            &|a: &[u8], b: &[u8]| a.cmp(b),
        )
        .expect("txn 1 has logrecs");

        assert_eq!(chain.len(), 1);
        let ri = chain.pop().unwrap();
        assert_eq!(
            ri.revert_lsn, lsn_v1,
            "the bounded scan must still see the preserved logrec written \
             AT the matchpoint LSN, not only strictly-after it"
        );
    }

    /// A txn id absent from the log entirely yields `None` (nothing to
    /// build), not a panic or a spurious empty chain masking a real bug.
    #[test]
    fn test_no_logrecs_for_txn_returns_none() {
        let dir = TempDir::new().unwrap();
        let (fm, lm) = make_fm(dir.path());
        write_ln(&lm, 7, 1, b"A", Some(b"v1"), NULL_LSN, true, None);
        lm.flush_sync().unwrap();

        let chain = build_live_chain_unbounded(
            &fm,
            404,
            NULL_LSN,
            &|a: &[u8], b: &[u8]| a.cmp(b),
        );
        assert!(chain.is_none(), "txn 404 never wrote anything");
    }

    /// The scan must filter STRICTLY by `txn_id`: a different txn's logrecs
    /// in the same log must never leak into this chain (the exact hazard a
    /// naive "everything in range" collector would hit).
    #[test]
    fn test_scan_filters_by_txn_id_not_by_lsn_range_alone() {
        let dir = TempDir::new().unwrap();
        let (fm, lm) = make_fm(dir.path());

        // A real (non-NULL) matchpoint before any of these writes -- like
        // recovery's `RollbackPeriod::matchpoint_lsn`, `TxnChain::build`
        // requires a valid LSN (comparing against NULL_LSN panics; see
        // `Lsn::cmp`), never NULL_LSN.
        let pretxn =
            write_ln(&lm, 7, 0, b"seed", Some(b"x0"), NULL_LSN, true, None);
        write_ln(&lm, 7, 1, b"mine", Some(b"v1"), NULL_LSN, true, None);
        write_ln(&lm, 7, 2, b"theirs", Some(b"x1"), NULL_LSN, true, None);
        write_ln(&lm, 7, 1, b"mine", Some(b"v2"), NULL_LSN, true, None);
        lm.flush_sync().unwrap();

        let chain = build_live_chain_unbounded(
            &fm,
            1,
            pretxn,
            &|a: &[u8], b: &[u8]| a.cmp(b),
        )
        .expect("txn 1 has logrecs");
        // Both of txn 1's writes to "mine" are above the matchpoint (rolled
        // back: v2 -> v1 -> pre-txn), so len is 2 -- the key assertion is
        // that txn 2's logrec never appears (a naive "everything in range"
        // collector would have pulled it in too).
        assert_eq!(
            chain.len(),
            2,
            "only txn 1's two writes; txn 2's must not leak in"
        );
    }
}
