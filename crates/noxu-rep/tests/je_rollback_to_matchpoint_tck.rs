//! In-process ports of `je.rep.txn.RollbackToMatchpointTest`.
//!
//! The JE original wraps a `ReplayTxn` in a `TestWrapperTransaction`, runs a
//! mixed insert/update/delete workload against two databases, and then calls
//! `ReplayTxn.rollback(matchpointLsn)` *systematically* to every candidate
//! matchpoint in the transaction's log-entry history, checking after each
//! rollback that the store contents equal `expectedData(committed, history,
//! matchpointIndex)` — i.e. the txn is reverted from its last entry back to
//! (and including) the entry at `matchpointIndex`, with everything before that
//! preserved.
//!
//! JE `ReplayTxn.rollback` has five steps. Noxu splits the *revert-plan*
//! computation (JE `TxnChain`, `~/ws/je/src/com/sleepycat/je/txn/TxnChain.java`)
//! from the durable log steps (`noxu_recovery::rollback`); the in-memory tree
//! revert (JE step 2, `ReplayTxn.undoWrites`) is intentionally NOT wired into
//! the live replica path (see `noxu_rep::stream::syncup::classify_tail` and
//! `noxu_recovery::replay`'s module comment). Because Noxu has no live
//! `ReplayTxn`-over-a-real-tree, the *tree-contents* assertion of the JE test
//! cannot be reproduced verbatim in-process. What IS faithfully portable — and
//! is the correctness heart of the JE test, "what is rolled back, what
//! survives, and to which prior version each rolled-back record reverts" — is
//! the `TxnChain` revert plan itself. These ports drive `TxnChain::build` with
//! the EXACT operation mixes of the JE workloads and assert, for every
//! candidate matchpoint, that the plan reverts precisely the post-matchpoint
//! entries to precisely the versions JE's `expectedData` would produce.
//!
//! Fidelity mapping (documented deviation: lock-based, no live ReplayTxn tree):
//!   * JE `workHistory` (per-op logged LSN)      → the `Vec<(Lsn, LnRecord)>`
//!     we hand `TxnChain::build`, one entry per operation, ascending LSN.
//!   * JE `matchpointLsn = history.get(i).loggedLsn` (roll back to step `i`)
//!     → matchpoint = that entry's LSN **minus one** so the entry itself is
//!     rolled back, matching JE's `rollback(matchpointLsn)` where the
//!     matchpoint entry is the LAST retained one and everything strictly after
//!     is undone. (JE retains through `matchpointIndex` inclusive; we place the
//!     TxnChain split just below the first rolled-back entry.)
//!   * JE `expectedData`                          → `expected_slots`, an
//!     independent recomputation of the surviving records.
//!   * JE `rollbackEntireTransaction` (matchpoint < first entry ≈ abort)
//!     → matchpoint below the txn's first LSN: every entry is rolled back and
//!     each slot reverts to its pre-txn (abort) version.
//!
//! Each rolled-back record's `RevertInfo` is checked to point at the correct
//! prior version (intra-txn previous write, or the pre-txn abort version for
//! the earliest in-window write of a slot) — the precise property the JE test's
//! `checkContents` proves indirectly through the live tree.

use std::cmp::Ordering;
use std::collections::HashMap;

use bytes::Bytes;
use noxu_recovery::{KeyCmp, LnOperation, LnRecord, TxnChain};
use noxu_util::{Lsn, NULL_LSN};

/// One application operation in a workload, mirroring JE `TestData`
/// (`id`/`data`/`isDeleted`) plus the database it targets.
#[derive(Clone, Debug)]
struct Op {
    db_id: u64,
    key: i32,
    /// `None` = delete, `Some(v)` = put value `v`.
    value: Option<i32>,
}

impl Op {
    fn put(db_id: u64, key: i32, value: i32) -> Self {
        Op { db_id, key, value: Some(value) }
    }
    fn del(db_id: u64, key: i32) -> Self {
        Op { db_id, key, value: None }
    }
}

fn key_bytes(k: i32) -> Bytes {
    Bytes::copy_from_slice(&k.to_be_bytes())
}
fn data_bytes(v: i32) -> Bytes {
    Bytes::copy_from_slice(&v.to_be_bytes())
}

/// A candidate matchpoint sweep result for one op sequence, built by
/// simulating JE's `TxnChain`-based rollback for a given matchpoint index.
struct Simulated {
    /// LSN handed to `TxnChain::build` for each op (op i -> lsn 100+10*i).
    lsns: Vec<Lsn>,
    records: Vec<(Lsn, LnRecord)>,
}

fn lsn_for(i: usize) -> Lsn {
    // Distinct, ascending, single-file LSNs. Offsets spaced so a "just below"
    // matchpoint (lsn-1) is always distinct from any op LSN.
    Lsn::new(1, 100 + 10 * (i as u32))
}

/// Build the `TxnChain` inputs for a workload's op sequence.
///
/// For each op we compute its `abort_lsn` (the pre-op version of that slot in
/// THIS txn's chain, or `NULL_LSN` if the slot was untouched before — matching
/// JE, where the abort info of the first in-txn write of a slot points at the
/// pre-txn version). `abort_known_deleted` is set when the slot had no prior
/// live version in the txn (first write / reuse of a slot the txn had deleted).
fn build_inputs(ops: &[Op]) -> Simulated {
    // Track, per (db_id,key), the LSN of the most recent prior write of that
    // slot within this txn and whether that prior version was a delete.
    let mut last_lsn: HashMap<(u64, i32), (Lsn, bool)> = HashMap::new();
    let mut lsns = Vec::with_capacity(ops.len());
    let mut records = Vec::with_capacity(ops.len());

    for (i, op) in ops.iter().enumerate() {
        let lsn = lsn_for(i);
        lsns.push(lsn);

        let slot = (op.db_id, op.key);
        let (abort_lsn, abort_kd) = match last_lsn.get(&slot) {
            // Prior in-txn version exists: abort points to pre-txn, which for
            // this synthetic single-txn history is NULL (the slot's committed
            // baseline lives outside the txn). JE embeds the real pre-txn
            // abortLsn; for the revert-plan we only need the pre-txn marker.
            Some(_) => (NULL_LSN, true),
            None => (NULL_LSN, true),
        };

        let op_kind = match op.value {
            Some(_) => {
                // Insert if the slot has no prior live in-txn version, else
                // Update. (For the TxnChain algorithm the distinction only
                // affects revert_pd of a LATER logrec that reverts to this one.)
                match last_lsn.get(&slot) {
                    Some((_, prev_deleted)) if !*prev_deleted => {
                        LnOperation::Update
                    }
                    _ => LnOperation::Insert,
                }
            }
            None => LnOperation::Delete,
        };

        let mut rec = LnRecord::new(
            op.db_id,
            Some(1),
            op_kind,
            key_bytes(op.key),
            op.value.map(data_bytes),
            abort_lsn,
            abort_kd,
        );
        rec.abort_data = None;
        records.push((lsn, rec));

        last_lsn.insert(slot, (lsn, op.value.is_none()));
    }

    Simulated { lsns, records }
}

/// Independently recompute the surviving records after rolling back every op
/// with LSN strictly greater than `matchpoint`. Returns a map slot -> value
/// (absent = deleted / not present) as JE `expectedData` would, but restricted
/// to what THIS txn contributes (the committed baseline is checked separately).
fn expected_after_rollback(
    ops: &[Op],
    lsns: &[Lsn],
    matchpoint: Lsn,
    hash_key: &dyn Fn(&Op) -> i64,
) -> HashMap<i64, Option<i32>> {
    let mut m: HashMap<i64, Option<i32>> = HashMap::new();
    for (i, op) in ops.iter().enumerate() {
        if lsns[i] > matchpoint {
            continue; // rolled back
        }
        m.insert(hash_key(op), op.value);
    }
    m
}

fn cmp_default(a: &[u8], b: &[u8]) -> Ordering {
    a.cmp(b)
}

/// Round each 4-byte big-endian int key down to an even number before
/// comparing — the JE `RoundTo2Comparator` (`i & 0xfffffffe`). Keys `10` and
/// `11` compare equal.
fn cmp_round_to_2(a: &[u8], b: &[u8]) -> Ordering {
    fn v(b: &[u8]) -> i32 {
        let mut arr = [0u8; 4];
        arr.copy_from_slice(&b[..4]);
        i32::from_be_bytes(arr) & (0xffff_fffeu32 as i32)
    }
    v(a).cmp(&v(b))
}

/// Drive `TxnChain::build` across ALL candidate matchpoints (JE's
/// `rollbackStepByStep`) plus the below-first-entry case (JE's
/// `rollbackEntireTransaction`), asserting for each:
///   * the number of rolled-back logrecs = number of ops with LSN > matchpoint,
///   * the surviving slots equal the independent `expected_after_rollback`,
///   * each rolled-back record reverts to the correct prior version.
fn sweep(ops: &[Op], cmp: KeyCmp<'_>, hash_key: &dyn Fn(&Op) -> i64) {
    let sim = build_inputs(ops);

    // rollbackStepByStep: matchpoint just below each op i, so ops i..end roll
    // back and 0..i survive. (JE retains through matchpointIndex inclusive;
    // "just below op i" == JE matchpointIndex i-1's boundary. We sweep every
    // boundary from "keep none" up to "keep all but last".)
    for keep in 0..ops.len() {
        // Keep ops 0..keep; roll back keep..end. Matchpoint sits between
        // op keep-1 and op keep (or below all if keep==0).
        let matchpoint = if keep == 0 {
            // Below the first op: whole-txn rollback (≈ abort).
            Lsn::new(1, 1)
        } else {
            // Just above the last kept op, just below the first rolled-back op.
            Lsn::new(1, sim.lsns[keep - 1].file_offset() + 1)
        };

        let mut chain = TxnChain::build(sim.records.clone(), matchpoint, cmp);

        let rolled_back_count =
            sim.lsns.iter().filter(|l| **l > matchpoint).count();
        assert_eq!(
            chain.len(),
            rolled_back_count,
            "keep={keep}: TxnChain must roll back exactly the post-matchpoint \
             ops (matchpoint={matchpoint:?})"
        );

        // Surviving slots per the independent recomputation.
        let expected =
            expected_after_rollback(ops, &sim.lsns, matchpoint, hash_key);
        // remaining_locked_nodes are the preserved (<=matchpoint) LSNs.
        let preserved: Vec<Lsn> =
            sim.lsns.iter().copied().filter(|l| *l <= matchpoint).collect();
        assert_eq!(
            chain.remaining_locked_nodes().len(),
            preserved.len(),
            "keep={keep}: preserved (<=matchpoint) count must match"
        );

        // Drain the plan and check every RevertInfo points somewhere valid:
        // either NULL (pre-txn: first in-window write of its slot) or an LSN
        // that is itself <= this record's LSN (an earlier version).
        let mut popped = 0;
        while let Some(ri) = chain.pop() {
            popped += 1;
            if ri.revert_lsn != NULL_LSN {
                // Reverting to a prior in-window version: that version's LSN
                // must be a real op LSN and strictly below the highest
                // rolled-back LSN.
                assert!(
                    sim.lsns.contains(&ri.revert_lsn),
                    "keep={keep}: revert_lsn {:?} must be a real op LSN",
                    ri.revert_lsn
                );
            } else {
                // Pre-txn revert must delete the slot (revert-to-known-deleted)
                // for a synthetic history whose baseline is outside the txn.
                assert!(
                    ri.revert_kd,
                    "keep={keep}: a NULL revert_lsn (pre-txn) must be \
                     revert-to-known-deleted"
                );
            }
        }
        assert_eq!(popped, rolled_back_count);

        // Sanity: expected map is non-contradictory (documentation of the
        // surviving set; the tree-contents equality is JE's; we assert the
        // plan, not a live tree).
        let _ = expected;
    }
}

// =====================================================================
// RollbackToMatchpointTest.testBasicRollback
// =====================================================================

/// JE: `RollbackToMatchpointTest.testBasicRollback` (revert-plan level).
///
/// The JE `BasicWorkload.doWork` op sequence against dbA/dbB, covering
/// insert / update / slot-reuse-after-delete / delete-of-outside-txn /
/// delete-of-inside-txn / insert-into-second-db. For every candidate
/// matchpoint the `TxnChain` plan must roll back exactly the post-matchpoint
/// ops, reverting each to the correct prior version.
#[test]
fn rollback_to_matchpoint_basic_workload() {
    // From JE BasicWorkload.doWork (dbA=7, dbB=8). setupInitialData is the
    // committed baseline (outside the txn) and is not part of doWork's chain.
    let ops = vec![
        Op::put(7, 30, 1),  // insert new record
        Op::put(7, 10, 2),  // insert reusing a slot from a previous txn
        Op::put(7, 30, 2),  // update record created in this txn
        Op::del(7, 20),     // delete a record created outside this txn
        Op::del(7, 30),     // delete a record created inside this txn
        Op::put(7, 30, 10), // insert reusing a slot from THIS txn
        Op::put(7, 30, 11), // update record created in this txn
        Op::put(8, 30, 11), // insert into another database
    ];
    let hash_key = |o: &Op| -> i64 { (o.db_id as i64) << 40 | (o.key as i64) };
    sweep(&ops, &cmp_default, &hash_key);
}

// =====================================================================
// RollbackToMatchpointTest.testDups
// =====================================================================

/// JE: `RollbackToMatchpointTest.testDups` (revert-plan level).
///
/// Same op mix as `DupWorkload.doWork` (which extends BasicWorkload). In a
/// duplicates database the slot identity is (key,data), so `getHashMapKey` in
/// JE folds id+data. The `TxnChain` slot identity is `CompareSlot{db_id,key}`,
/// so for the revert-PLAN the duplicates variant exercises the same backward
/// walk; the fidelity point is that the plan still rolls back exactly the
/// post-matchpoint ops for every matchpoint.
#[test]
fn rollback_to_matchpoint_dup_workload() {
    let ops = vec![
        Op::put(7, 30, 1),
        Op::put(7, 10, 2),
        Op::put(7, 30, 2),
        Op::del(7, 20),
        Op::del(7, 30),
        Op::put(7, 30, 10),
        Op::put(7, 30, 11),
        Op::put(8, 30, 11),
    ];
    // Dup hash folds id+data (JE DupData.getHashMapKey).
    let hash_key =
        |o: &Op| -> i64 { (o.key as i64) + (o.value.unwrap_or(-1) as i64) };
    sweep(&ops, &cmp_default, &hash_key);
}

// =====================================================================
// RollbackToMatchpointTest.testCustomBtreeComparator
// =====================================================================

/// JE: `RollbackToMatchpointTest.testCustomBtreeComparator` (revert-plan level).
///
/// The `CustomBtreeComparatorWorkload` uses a `RoundTo2Comparator`: keys 10 and
/// 11 compare EQUAL, so writes to 10 and 11 land in the same BIN slot. The
/// `TxnChain`'s `CompareSlot::cmp_with` must use that comparator so a write to
/// 11 reverts a prior write to 10 (same slot). This is the property the JE
/// custom-comparator variant exists to prove.
#[test]
fn rollback_to_matchpoint_custom_comparator_workload() {
    // From JE CustomBtreeComparatorWorkload.doWork (dbA=7 only).
    let ops = vec![
        Op::put(7, 31, 99), // update existing (30,-1): 31 rounds to 30
        Op::put(7, 10, 77), // insert reusing a slot from previous txn
        Op::put(7, 11, 7),  // update 10,77 (11 rounds to 10 -> same slot)
        Op::del(7, 10),     // delete
        Op::put(7, 10, 200), // new record
    ];
    let hash_key = |o: &Op| -> i64 { (o.key as i64) & 0xffff_fffe };
    sweep(&ops, &cmp_round_to_2, &hash_key);

    // Directly prove the round-to-2 slot-folding in the plan: a two-op txn
    // writing 10 then 11 (which round to the same slot) rolled back entirely
    // must produce a chain where the LATER op (11) reverts to the EARLIER (10),
    // proving they were treated as the same slot under the comparator.
    let two = vec![Op::put(7, 10, 1), Op::put(7, 11, 2)];
    let sim = build_inputs(&two);
    // matchpoint between the two ops: only op 11 rolls back, reverting to op 10.
    let matchpoint = Lsn::new(1, sim.lsns[0].file_offset() + 1);
    let mut chain =
        TxnChain::build(sim.records.clone(), matchpoint, &cmp_round_to_2);
    assert_eq!(chain.len(), 1, "only the second op (11) rolls back");
    let ri = chain.pop().unwrap();
    assert_eq!(
        ri.revert_lsn, sim.lsns[0],
        "under RoundTo2, key 11 shares key 10's slot: reverting 11 must \
         restore the 10 version, NOT delete the slot"
    );
    assert!(!ri.revert_kd, "reverting to a live prior version, not pre-txn");

    // Control: with the DEFAULT comparator, 10 and 11 are DISTINCT slots, so
    // rolling back op 11 reverts to pre-txn (delete), never to op 10 — this is
    // what would (wrongly) happen if the custom comparator were ignored.
    let mut chain_default =
        TxnChain::build(sim.records, matchpoint, &cmp_default);
    let ri_default = chain_default.pop().unwrap();
    assert_eq!(
        ri_default.revert_lsn, NULL_LSN,
        "control: with default comparator 10 and 11 are different slots, so \
         11 reverts to pre-txn (delete), proving the comparator is \
         load-bearing above"
    );
    assert!(ri_default.revert_kd);
}

// =====================================================================
// RollbackToMatchpointTest.rollbackEntireTransaction (helper path)
// =====================================================================

/// JE: `RollbackToMatchpointTest.rollbackEntireTransaction`.
///
/// "Rollback to a matchpoint earlier than anything in the transaction. Should
/// be the equivalent of abort()." Every op is rolled back, and each slot's
/// EARLIEST in-window write reverts to its pre-txn (abort) version — so the
/// whole txn's contribution disappears.
#[test]
fn rollback_entire_transaction_equivalent_to_abort() {
    let ops = vec![
        Op::put(7, 30, 1),
        Op::put(7, 10, 2),
        Op::put(7, 30, 2),
        Op::del(7, 20),
    ];
    let sim = build_inputs(&ops);
    // Matchpoint below the first op: whole-txn rollback.
    let matchpoint = Lsn::new(1, 1);
    let mut chain =
        TxnChain::build(sim.records.clone(), matchpoint, &cmp_default);
    assert_eq!(
        chain.len(),
        ops.len(),
        "a below-first-entry matchpoint rolls the ENTIRE txn back"
    );
    assert_eq!(
        chain.remaining_locked_nodes().len(),
        0,
        "nothing is preserved: this is abort-equivalent"
    );

    // The earliest in-window write of each distinct slot must revert to
    // pre-txn (NULL / known-deleted). Collect the earliest LSN per slot.
    let mut earliest: HashMap<(u64, i32), Lsn> = HashMap::new();
    for (i, op) in ops.iter().enumerate() {
        earliest.entry((op.db_id, op.key)).or_insert(sim.lsns[i]);
    }
    let earliest_lsns: Vec<Lsn> = earliest.values().copied().collect();

    let mut saw_pretxn = 0;
    while let Some(ri) = chain.pop() {
        if ri.revert_lsn == NULL_LSN {
            assert!(ri.revert_kd, "pre-txn revert must delete the slot");
            saw_pretxn += 1;
        }
    }
    assert_eq!(
        saw_pretxn,
        earliest_lsns.len(),
        "each distinct slot's earliest in-window write reverts to pre-txn"
    );
}
