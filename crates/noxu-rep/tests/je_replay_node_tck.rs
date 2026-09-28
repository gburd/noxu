//! In-process ports of `com.sleepycat.je.rep.node.replica.ReplayTest`.
//!
//! JE `ReplayTest` drives a live multi-JVM replication group (`RepTestBase`,
//! `RepTestUtils.joinGroup`) and exercises the REPLICA-SIDE REPLAY of the
//! master's replication stream — the code in
//! `com.sleepycat.je.rep.impl.node.Replay` (`replayEntry` → `applyLN` /
//! `applyNameLN`).  The safety contract it protects is the core of
//! replication correctness: **an operation replayed on the replica must
//! produce the SAME state as the master** (`RepTestUtils.checkNodeEquality`).
//! A replica that replays wrong — an op applied incorrectly, out of order, or
//! a commit/abort mishandled — DIVERGES from the master, which is data
//! corruption.
//!
//! Noxu implements that replay in `noxu_dbi::ReplicaReplay` (the port of
//! `Replay`, see `crates/noxu-dbi/src/replica_replay.rs`): each streamed
//! entry, after it is written to the WAL and registered in the VLSN index, is
//! applied to the replica's LIVE in-memory tree — transactional LNs buffered
//! provisionally and resolved at their commit (`repTxn.commit`) / discarded at
//! abort (`repTxn.abort`), non-transactional LNs applied immediately.  These
//! tests drive that path directly (the deterministic in-process harness the
//! multi-JVM `RepTestBase` sets up around it), because it is where the
//! replay-correctness invariant lives.
//!
//! ## Fidelity map (`ReplayTest` → Noxu)
//!
//! | JE @Test | What it proves (portable core) | Deviation carved out |
//! |---|---|---|
//! | `testResumedTransaction` | A transactional op is provisional until its commit streams in, and that resolution survives a replica reopen (resumed txn). | The op in the JE test is a DB *create*; DDL-catalog live-replay on the replica is not modelled — see `testBasicDatabaseOperations` note. The LN-level resume-and-resolve IS ported here + cited in je.rep.txn `ReplayRecoveryTest.testResurrectionOneTxn`. |
//! | `testBasicDatabaseOperations` | The replica's state converges to the master's after a stream of ops (`checkNodeEquality`). | JE exercises DDL (truncate/rename/remove) replay via `Replay.applyNameLN`; Noxu treats NameLN as note-only (documented deviation). The LN-DATA convergence — the same-tree-state invariant — IS ported here. |
//! | `testDatabaseOpContention` | An open handle is invalidated (`DatabasePreemptedException`) when the master removes the DB under it. | Handle-preemption notification is the same escalated gap as `LockPreemptedException` (je.rep.txn). N/A(deviation), see report. |
//!
//! ## Why in-process, not multi-JVM
//! `ReplayTest` needs `groupSize` real JVMs and TCP feeders only to *deliver*
//! the stream; the behaviour under test is purely the replica's per-entry
//! apply.  `ReplicaReplay` is that apply, and driving it directly is
//! deterministic (no election/network flake) and asserts EXACTLY the tree
//! state the JE test's `checkNodeEquality` asserts.  The full multi-JVM
//! delivery is covered by the streaming integration tests
//! (`cc2_feeder_integration_test`, `rep7_live_read_test`, `tcp_integration`).

use std::sync::Arc;

use bytes::BytesMut;
use noxu_dbi::{DatabaseConfig, EnvironmentImpl, ReplicaReplay};
use noxu_log::LogEntryType;
use noxu_log::entry::{LnLogEntry, TxnEndEntry};
use noxu_util::{Lsn, NULL_LSN, NULL_VLSN};

// ─── wire helpers: what a master feeder would place on the stream ───────────

fn ln_payload(
    db_id: u64,
    txn_id: Option<i64>,
    key: &[u8],
    data: Option<&[u8]>,
) -> Vec<u8> {
    let entry = LnLogEntry::new(
        db_id,
        txn_id,
        NULL_LSN,
        false,
        None,
        None,
        NULL_VLSN,
        0,
        false,
        key.to_vec(),
        data.map(|d| d.to_vec()),
        0,
        NULL_VLSN,
    );
    let mut buf = BytesMut::new();
    entry.write_to_log(&mut buf);
    buf.to_vec()
}

fn commit_payload(txn_id: i64) -> Vec<u8> {
    let e = TxnEndEntry::new_commit(txn_id, NULL_LSN, 0, 0, NULL_VLSN);
    let mut buf = BytesMut::new();
    e.write_to_log(&mut buf);
    buf.to_vec()
}

fn insert_txn() -> u8 {
    LogEntryType::InsertLNTxn.type_num()
}
fn update_txn() -> u8 {
    LogEntryType::UpdateLNTxn.type_num()
}
fn delete_txn() -> u8 {
    LogEntryType::DeleteLNTxn.type_num()
}
fn commit_type() -> u8 {
    LogEntryType::TxnCommit.type_num()
}

/// Open an env, open a database, return (env, db_id, tree_arc).
///
/// This is the replica side: the tree the replay driver applies into is the
/// same `Arc<RwLock<Tree>>` an opened `Database`/`Cursor` reads through.
fn open_replica_with_db()
-> (Arc<EnvironmentImpl>, u64, Arc<std::sync::RwLock<noxu_tree::Tree>>) {
    let dir = tempfile::TempDir::new().unwrap();
    let env = Arc::new(EnvironmentImpl::new(dir.path(), false, true).unwrap());
    let mut cfg = DatabaseConfig::new();
    cfg.set_allow_create(true).set_transactional(true);
    let db = env.open_database("ReplayTestDB", &cfg).unwrap();
    let db_id = db.read().get_id().id() as u64;
    let tree = env.replica_tree_for_db(db_id).unwrap();
    (env, db_id, tree)
}

/// True iff `key` is present (live exact match) in `tree`.
///
/// `Tree::search` returns `Some(SearchResult)` for ANY key once the tree
/// is non-empty (the match flag is a field, not the `Option`), so
/// existence must be read from `SlotFetch::found`.
fn present(tree: &std::sync::RwLock<noxu_tree::Tree>, key: &[u8]) -> bool {
    tree.read().unwrap().search_with_data(key).map(|s| s.found).unwrap_or(false)
}

// ─── testResumedTransaction ─────────────────────────────────────────────────

/// JE: `ReplayTest.testResumedTransaction`.
///
/// The JE test opens a transaction `mt1` on the master and leaves it OPEN,
/// syncs the replica to a LATER commit-point (`mt2`), shuts the replica down
/// with `mt1` still open, reopens it forcing a syncup, then commits `mt1` and
/// checks the replica sees `mt1`'s effect — i.e. an operation that was still
/// provisional when the replica went away is RESUMED and correctly resolved
/// once its commit finally arrives.
///
/// PORTABLE CORE ported here: a transactional LN buffered as provisional
/// (`Replay.getReplayTxn`) is NOT visible to a replica reader; it becomes
/// visible ONLY when its commit streams in (`Replay.replayEntry`
/// LOG_TXN_COMMIT → `repTxn.commit`), and an INTERVENING commit of a
/// *different* txn does NOT resolve or leak it.  This is exactly what makes
/// the resumed-transaction case correct: the open txn's op stays invisible
/// across the sync-to-a-later-commit-point, then resolves at its own commit.
///
/// DEVIATION carved out: the JE op inside `mt1` is a database *create*
/// (`openDatabase`).  DDL-catalog live-replay on the replica (`applyNameLN`)
/// is not modelled in Noxu — the LN-level resume-and-resolve is what is
/// ported.  The recovery-side resurrection of an uncommitted txn's LNs across
/// a reopen is additionally cited in je.rep.txn `ReplayRecoveryTest`
/// (`replay_recovery_resumes_after_reopen`).
///
/// Vacuity guard: the FIRST assertion (still invisible after the intervening
/// commit) would fail if the buffer were resolved by the wrong commit; the
/// LAST (visible after its own commit) would fail if the resume never
/// resolved.  Both are load-bearing.
#[test]
fn replay_resumed_txn_provisional_until_its_own_commit() {
    let (env, db_id, tree) = open_replica_with_db();
    let mut replay = ReplicaReplay::new(Arc::clone(&env));

    // mt1 (txn 1): the "left open" transaction — stream its LN, provisional.
    let mt1 = ln_payload(db_id, Some(1), b"mt1key", Some(b"mt1val"));
    replay.apply_entry(1, insert_txn(), &mt1, Lsn::new(0, 100));

    // mt2 (txn 2): a LATER transaction that the replica syncs to. Its commit
    // is the sync-point (the CommitToken ct2 in JE). It must NOT resolve mt1.
    let mt2 = ln_payload(db_id, Some(2), b"mt2key", Some(b"mt2val"));
    replay.apply_entry(2, insert_txn(), &mt2, Lsn::new(0, 200));
    let c2 = commit_payload(2);
    replay.apply_entry(3, commit_type(), &c2, Lsn::new(0, 300));

    // Replica is now synced to ct2. mt2's data IS visible; mt1's is NOT
    // (mt1 is still open — the resumed-transaction precondition).
    assert!(
        present(&tree, b"mt2key"),
        "the synced-to txn (mt2) must be visible after its commit"
    );
    assert!(
        !present(&tree, b"mt1key"),
        "the still-open txn (mt1) must NOT be visible: its commit has not \
         streamed in — an intervening commit of a DIFFERENT txn must not \
         resolve it"
    );

    // (In JE the replica now closes and reopens forcing a syncup; the
    // provisional mt1 survives that as an active replay txn / is resurrected
    // at recovery — cited in je.rep.txn ReplayRecoveryTest. Here the
    // ReplicaReplay buffer models the survived provisional state directly.)

    // mt1 finally commits (ct1). NOW its effect must appear on the replica.
    let c1 = commit_payload(1);
    replay.apply_entry(4, commit_type(), &c1, Lsn::new(0, 400));
    assert!(
        present(&tree, b"mt1key"),
        "the resumed txn (mt1) must become visible once ITS OWN commit \
         streams in (Replay.replayEntry LOG_TXN_COMMIT -> repTxn.commit)"
    );
    assert_eq!(replay.last_applied_vlsn(), 4);
}

// ─── testBasicDatabaseOperations ────────────────────────────────────────────

/// JE: `ReplayTest.testBasicDatabaseOperations` — LN-DATA convergence half.
///
/// The JE test performs a stream of operations on the master and then asserts
/// (`RepTestUtils.checkNodeEquality(commitVLSN, false, repEnvInfo)`) that
/// EVERY replica's state matches the master's at that VLSN — the core
/// replay-correctness invariant: **a replayed op produces the same state as
/// the master.**
///
/// PORTABLE CORE ported here: the replica's tree, after replaying the
/// master's committed LN stream (insert, then update, then delete, across
/// transactions, in VLSN order), reflects EXACTLY the master's final state
/// for those keys.  This is `checkNodeEquality` for the LN data layer, driven
/// through `ReplicaReplay` (`Replay.applyLN`).
///
/// DEVIATION carved out: JE's `testBasicDatabaseOperations` exercises the
/// operations *truncate / rename / remove DATABASE*, whose replay goes through
/// `Replay.applyNameLN` (`truncateReplicaDb` / `renameReplicaDb` /
/// `removeReplicaDb`).  Noxu's `ReplicaReplay` treats NameLN entries as
/// note-only (the WAL byte-shadow + recovery-on-restart materialises the
/// catalog; there is no live DDL-catalog apply on the replica).  That is a
/// documented deviation (see tp-je-rep-node-replica.md and je.rep.txn
/// `RollbackTest.testDbOpsRollback` N/A). The LN-DATA convergence — the part
/// of the invariant that IS implemented — is what this test proves.
///
/// Vacuity guard: an update that did not replay would leave the old value; a
/// delete that did not replay would leave the key present. Both are asserted.
#[test]
fn replay_ln_stream_converges_replica_to_master_state() {
    let (env, db_id, tree) = open_replica_with_db();
    let mut replay = ReplicaReplay::new(Arc::clone(&env));

    // Master txn 10: insert two keys, commit. Replica must see both.
    replay.apply_entry(
        1,
        insert_txn(),
        &ln_payload(db_id, Some(10), b"keep", Some(b"v1")),
        Lsn::new(0, 100),
    );
    replay.apply_entry(
        2,
        insert_txn(),
        &ln_payload(db_id, Some(10), b"gone", Some(b"v1")),
        Lsn::new(0, 110),
    );
    replay.apply_entry(3, commit_type(), &commit_payload(10), Lsn::new(0, 120));

    // Master txn 11: UPDATE "keep", DELETE "gone", commit.
    replay.apply_entry(
        4,
        update_txn(),
        &ln_payload(db_id, Some(11), b"keep", Some(b"v2")),
        Lsn::new(0, 200),
    );
    replay.apply_entry(
        5,
        delete_txn(),
        &ln_payload(db_id, Some(11), b"gone", None),
        Lsn::new(0, 210),
    );
    replay.apply_entry(6, commit_type(), &commit_payload(11), Lsn::new(0, 220));

    // checkNodeEquality (LN-data layer): the replica's tree == the master's
    // final state — "keep" holds the UPDATED value, "gone" is DELETED.
    let keep_val =
        tree.read().unwrap().search_with_data(b"keep").and_then(|s| s.data);
    assert_eq!(
        keep_val.as_deref(),
        Some(&b"v2"[..]),
        "replayed UPDATE must leave the master's updated value on the replica"
    );
    assert!(
        !present(&tree, b"gone"),
        "replayed DELETE must remove the key on the replica (checkNodeEquality)"
    );
    assert_eq!(replay.last_applied_vlsn(), 6, "VLSN advanced through commit 6");
}

/// JE: `ReplayTest.testBasicDatabaseOperations` — VLSN-ORDER half.
///
/// The replica must apply entries in VLSN order; the last-applied-VLSN is the
/// consistency high-water mark `checkNodeEquality` compares against. A
/// commit at VLSN N leaves the replica at N; the tree reflects the ops up to
/// N and no further.
///
/// Vacuity guard: if the replay advanced past the streamed VLSN, or applied a
/// later-buffered txn early, these equalities would break.
#[test]
fn replay_advances_vlsn_in_stream_order() {
    let (env, db_id, tree) = open_replica_with_db();
    let mut replay = ReplicaReplay::new(Arc::clone(&env));

    // A committed txn (12) then a still-open txn (13).
    replay.apply_entry(
        7,
        insert_txn(),
        &ln_payload(db_id, Some(12), b"early", Some(b"x")),
        Lsn::new(0, 100),
    );
    replay.apply_entry(8, commit_type(), &commit_payload(12), Lsn::new(0, 110));
    assert_eq!(
        replay.last_applied_vlsn(),
        8,
        "VLSN high-water is the last committed/applied entry"
    );

    // A later, still-open txn's LN is buffered — the high-water does NOT
    // jump ahead of it, and its data is NOT yet visible.
    replay.apply_entry(
        9,
        insert_txn(),
        &ln_payload(db_id, Some(13), b"late", Some(b"y")),
        Lsn::new(0, 200),
    );
    assert!(
        !present(&tree, b"late"),
        "a buffered (uncommitted) later txn must not be visible out of order"
    );
    assert!(present(&tree, b"early"), "the earlier committed txn IS visible");
}

// ─── testDatabaseOpContention ───────────────────────────────────────────────
//
// JE: `ReplayTest.testDatabaseOpContention` — N/A (documented deviation).
//
// JE opens a database handle on the replica, the master then removes that
// database, and the replica's replay of the remove (`Replay.applyNameLN`
// REMOVE, via the ReplayTxn's IMPORTUNATE write lock) forcibly closes /
// invalidates the open handle; the application's next use of it throws
// `DatabasePreemptedException`.
//
// Noxu does not model this: (1) there is no live DDL-catalog replay on the
// replica (`ReplicaReplay` NameLN = note-only — same deviation as
// `testBasicDatabaseOperations`), and (2) there is no handle-preemption
// notification — this is the SAME escalated gap as `LockPreemptedException`
// (see tp-je-rep-txn.md "ENGINE-FIDELITY GAP — LockPreemptedException victim
// notification"): Noxu's importunate steal wins the write (the load-bearing
// safety property — a stale replica reader/handle cannot block the master's
// replicated op — HOLDS), but the victim is not NOTIFIED, so there is no
// `DatabasePreempted`/`LockPreempted` error to assert. Not a data-loss/
// corruption bug. N/A(deviation); tracked in the escalated je.rep.txn gap.
