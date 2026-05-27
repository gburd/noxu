//! Ports of JE replication TCK tests under `je.rep.stream` that
//! exercise the master→replica replication stream: VLSN progression,
//! ordering, gap-handling, and catch-up on real (multi-node) groups.
//!
//! The JE originals (`FeederReaderTest`, `FeederWriteQueueTest`,
//! `FeederFilterTest`, `ReplicaSyncupReaderTest`) tap directly into JE
//! log internals and a `LogPopulator` helper that has no direct Noxu
//! analog.  These ports therefore drop down one layer to assert the
//! *observable* stream invariants — VLSN range coverage, replica VLSN
//! monotonicity, multi-replica fan-out — using the in-memory
//! [`crate::test_harness::RepTestBase`] harness.
//!
//! Each test names the JE source file and method in its doc comment.

use noxu_rep::test_harness::RepTestBase;

// =====================================================================
// FeederReaderTest — `je.rep.stream.FeederReaderTest`
// =====================================================================

/// JE: `FeederReaderTest.testForwardScans`.
///
/// "A replica scanning the log forward from VLSN i sees every entry
/// from i through the master's last VLSN."  Stream-level analog: after
/// `populate_db`, every replica reports `current_vlsn() == count`.
#[test]
fn forward_scan_replicas_see_every_entry() {
    let mut group = RepTestBase::builder("fwd_scan").group_size(3).build();
    group.create_group(1).unwrap();

    group.populate_db(1, 100).unwrap();
    group.assert_all_at_vlsn(100);

    // Each replica's VLSN range must span [1, 100].
    for idx in 0..3 {
        let env = group.node(idx).get_env();
        let range = env.get_vlsn_range();
        assert_eq!(range.last(), 100, "node {idx} last must be 100");
        assert!(range.contains(1), "node {idx} range must contain VLSN 1");
        assert!(range.contains(100), "node {idx} range must contain VLSN 100");
    }
}

/// JE: `FeederReaderTest.testBackwardScans`.
///
/// "After a backwards scan from the last VLSN, all entries in the
/// scanned range are visible."  Stream-level analog: the VLSN range
/// after replication has both first and last set correctly.
#[test]
fn vlsn_range_first_and_last_are_consistent() {
    let mut group = RepTestBase::builder("vlsn_first_last").group_size(2).build();
    group.create_group(1).unwrap();

    group.populate_db(1, 50).unwrap();
    let env = group.node(1).get_env();
    let range = env.get_vlsn_range();
    assert!(range.first() <= range.last(), "first <= last invariant");
    assert_eq!(range.last(), 50);
    assert!(range.contains(25), "midpoint must be in range");
}

/// JE: `FeederReaderTest.testFindSyncableentries`.
///
/// "A replica catching up from VLSN N picks up at exactly VLSN N+1."
/// Stream-level: after master writes 1..=10, then the replica is told
/// to start at 6, only entries 6..=10 are applied — and the replica's
/// VLSN advances by exactly 5.
#[test]
fn replica_catch_up_starts_at_correct_vlsn() {
    let mut group = RepTestBase::builder("syncable").group_size(2).build();
    group.create_group(1).unwrap();

    // Both nodes catch up to VLSN 5.
    group.populate_db(1, 5).unwrap();
    let baseline = group.node(1).current_vlsn();
    assert_eq!(baseline, 5);

    // Master writes 6..=10 alone.
    group.populate_master_only(6, 5).unwrap();
    assert_eq!(group.node(0).current_vlsn(), 10);
    assert_eq!(group.node(1).current_vlsn(), 5);

    // Catch up the replica from 6 to 10 — exactly 5 entries applied.
    group.catch_up_replica(1, 6, 5).unwrap();
    assert_eq!(group.node(1).current_vlsn(), 10);
}

// =====================================================================
// FeederWriteQueueTest — `je.rep.stream.FeederWriteQueueTest`
// =====================================================================

/// JE: `FeederWriteQueueTest.testDataInWriteQueue`.
///
/// "Entries appear in the feeder's write queue in VLSN order."  Stream-
/// level analog: register_vlsn entries arrive at every replica in VLSN
/// order, and the replica's apply order matches.
#[test]
fn write_queue_preserves_vlsn_order() {
    let mut group =
        RepTestBase::builder("write_queue").group_size(2).build();
    group.create_group(1).unwrap();

    // Replicate in strict VLSN order.
    for vlsn in 1u64..=20 {
        group.replicate_one(vlsn, 0, (vlsn as u32) * 16, 0).unwrap();
        // After each replicate, every node must agree.
        assert_eq!(group.node(0).current_vlsn(), vlsn);
        assert_eq!(group.node(1).current_vlsn(), vlsn);
    }
}

// =====================================================================
// ProtocolTest — `je.rep.stream.ProtocolTest`
// =====================================================================

/// JE: `ProtocolTest.testBasic`.
///
/// "Once a master and a replica connect, the replica sees every commit
/// the master makes."  Harness analog: smoke-test of master → replica
/// fan-out with a varied entry count.
#[test]
fn protocol_basic_full_fan_out() {
    let mut group = RepTestBase::builder("proto_basic").group_size(4).build();
    group.create_group(1).unwrap();

    group.populate_db(1, 60).unwrap();
    group.assert_all_at_vlsn(60);
}

// =====================================================================
// ReplicaSyncupReaderTest — `je.rep.stream.ReplicaSyncupReaderTest`
// =====================================================================

/// JE: `ReplicaSyncupReaderTest.testRepAndNonRepCommits`.
///
/// "Syncup correctly distinguishes replicated commits from local-only
/// (non-replicated) commits."  In Noxu's stream model, the equivalent
/// invariant: a `populate_master_only` call does NOT advance the
/// replica's VLSN (those entries are master-local, like JE's non-rep
/// commits).
#[test]
fn syncup_distinguishes_replicated_vs_master_only() {
    let mut group = RepTestBase::builder("syncup").group_size(2).build();
    group.create_group(1).unwrap();

    // 5 replicated entries.
    group.populate_db(1, 5).unwrap();
    assert_eq!(group.node(0).current_vlsn(), 5);
    assert_eq!(group.node(1).current_vlsn(), 5);

    // 5 master-only entries.
    group.populate_master_only(6, 5).unwrap();
    assert_eq!(group.node(0).current_vlsn(), 10);
    assert_eq!(
        group.node(1).current_vlsn(),
        5,
        "replica must NOT see master-only entries",
    );
}

/// JE: `ReplicaSyncupReaderTest.testMultipleCkpts`.
///
/// "Multiple checkpoints in the master's log do not disrupt the
/// replica's syncup; the replica still ends at the master's tail."
/// Harness analog: large batch replication broken into multiple chunks
/// (simulating checkpoint boundaries) still leaves replicas at the
/// final VLSN.
#[test]
fn multiple_checkpoint_chunks_replicate_cleanly() {
    let mut group =
        RepTestBase::builder("multi_ckpt").group_size(3).build();
    group.create_group(1).unwrap();

    // Three checkpoint-style chunks.
    group.populate_db(1, 30).unwrap();
    group.assert_all_at_vlsn(30);

    group.populate_db(31, 40).unwrap();
    group.assert_all_at_vlsn(70);

    group.populate_db(71, 50).unwrap();
    group.assert_all_at_vlsn(120);
}

// =====================================================================
// FeederFilterTest — `je.rep.stream.FeederFilterTest`
// =====================================================================

/// JE: `FeederFilterTest.testNoOpFilter`.
///
/// "An identity (no-op) filter passes every entry through, so the
/// replica sees the master's full log."  Stream-level: with no filter
/// configured, populate_db replicates every VLSN to every replica.
/// (Noxu does not yet expose pluggable feeder filters; this asserts
/// the no-op-equivalent baseline.)
#[test]
fn feeder_filter_no_op_baseline() {
    let mut group = RepTestBase::builder("ff_noop").group_size(3).build();
    group.create_group(1).unwrap();

    group.populate_db(1, 80).unwrap();
    group.assert_all_at_vlsn(80);

    // Every node's range covers every VLSN.
    for idx in 0..3 {
        let r = group.node(idx).get_env().get_vlsn_range();
        for v in [1u64, 20, 40, 60, 80] {
            assert!(r.contains(v), "node {idx} range must contain VLSN {v}");
        }
    }
}

/// JE: `FeederFilterTest.testFilterWithStatistics` (subset).
///
/// "Statistics tracking does not skip any entry that the filter
/// permits."  Harness analog: every replicated VLSN is observable on
/// the master via `get_current_vlsn()` and on every replica via the
/// VLSN range.  No entries are silently dropped.
#[test]
fn feeder_no_silent_drops_under_replication() {
    let mut group = RepTestBase::builder("ff_stats").group_size(2).build();
    group.create_group(1).unwrap();

    let mut expected = 0u64;
    for chunk in 1..=5u64 {
        let n = chunk * 7; // 7, 14, 21, 28, 35
        let start = expected + 1;
        group.populate_db(start, n).unwrap();
        expected += n;
        assert_eq!(group.node(0).current_vlsn(), expected, "master");
        assert_eq!(group.node(1).current_vlsn(), expected, "replica");
    }
    // Final cross-check: total entries replicated == sum of chunks.
    assert_eq!(expected, 7 + 14 + 21 + 28 + 35);
    assert_eq!(group.node(1).current_vlsn(), expected);
}
