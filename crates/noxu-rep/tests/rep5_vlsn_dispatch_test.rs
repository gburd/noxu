//! REP-5: the production VLSN registration path must advance
//! `lastSync`/`lastTxnEnd` (sync_vlsn/commit_vlsn) by dispatching on the
//! streamed entry's `LogEntryType`.
//!
//! Before the fix, `VlsnIndex::put`/`register` only called
//! `VlsnRange::extend`, so a running node's `sync_vlsn`/`commit_vlsn` stayed
//! at 0 (NULL_VLSN) no matter how many sync-points or commits it logged.
//! `update_for_new_mapping` (the JE-faithful dispatch) was reachable only
//! from unit tests.
//!
//! JE: `VLSNIndex.put(LogItem)` reads the entry type from the log-item
//! header and routes through `VLSNTracker.track` ->
//! `VLSNRange.getUpdateForNewMapping(vlsn, entryTypeNum)`
//! (VLSNRange.java:162-190): a sync point advances `lastSync`; a
//! commit/abort advances `lastTxnEnd`.

use noxu_log::LogEntryType;
use noxu_rep::vlsn::VlsnIndex;

/// REP-5 reproduce-first: after the master logs a sync-point and a commit
/// through the typed registration path, the range's `sync_vlsn` and
/// `commit_vlsn` ADVANCE.
///
/// FAILS on main (extend-only `put` leaves both at 0); PASSES after routing
/// through `put_with_type` -> `VlsnRange::update_for_new_mapping`.
#[test]
fn test_typed_registration_advances_sync_and_commit() {
    let index = VlsnIndex::new(10);

    // A plain insert LN: extends first/last, leaves sync/commit at 0.
    index.put_with_type(1, 0, 100, LogEntryType::InsertLN);
    let r = index.get_range();
    assert_eq!(r.get_last(), 1);
    assert_eq!(r.get_sync_vlsn(), 0, "InsertLN is not a sync point");
    assert_eq!(r.get_commit_vlsn(), 0, "InsertLN is not a commit/abort");

    // A sync point (Matchpoint) at vlsn 2: advances lastSync only.
    index.put_with_type(2, 0, 200, LogEntryType::Matchpoint);
    let r = index.get_range();
    assert_eq!(r.get_sync_vlsn(), 2, "matchpoint must advance sync_vlsn");
    assert_eq!(
        r.get_commit_vlsn(),
        0,
        "matchpoint is not a txn end -> commit_vlsn unchanged"
    );

    // A commit at vlsn 3: advances lastTxnEnd AND lastSync (commit is a
    // sync point in JE: LogEntryType::is_sync_point() includes TxnCommit).
    index.put_with_type(3, 0, 300, LogEntryType::TxnCommit);
    let r = index.get_range();
    assert_eq!(r.get_commit_vlsn(), 3, "commit must advance commit_vlsn");
    assert_eq!(r.get_sync_vlsn(), 3, "commit is a sync point -> sync_vlsn");

    // Sanity: the same sequence through the OLD extend-only `put` would
    // leave both boundaries at 0 (this is exactly the REP-5 bug).
    let extend_only = VlsnIndex::new(10);
    extend_only.put(1, 0, 100);
    extend_only.put(2, 0, 200);
    extend_only.put(3, 0, 300);
    let er = extend_only.get_range();
    assert_eq!(er.get_last(), 3, "extend-only still tracks first/last");
    assert_eq!(
        er.get_sync_vlsn(),
        0,
        "extend-only NEVER advances sync_vlsn (the bug)"
    );
    assert_eq!(
        er.get_commit_vlsn(),
        0,
        "extend-only NEVER advances commit_vlsn (the bug)"
    );
}

/// REP-5: `lastSync` may run AHEAD of `lastTxnEnd` when a Matchpoint follows
/// a commit (the exact JE scenario the two distinct fields exist for —
/// VLSNRange.java:55-59).
#[test]
fn test_sync_runs_ahead_of_txn_end() {
    let index = VlsnIndex::new(10);
    index.put_with_type(1, 0, 10, LogEntryType::TxnCommit);
    index.put_with_type(2, 0, 20, LogEntryType::Matchpoint);
    let r = index.get_range();
    assert_eq!(r.get_commit_vlsn(), 1, "commit at vlsn 1");
    assert_eq!(r.get_sync_vlsn(), 2, "matchpoint runs sync ahead to vlsn 2");
}
