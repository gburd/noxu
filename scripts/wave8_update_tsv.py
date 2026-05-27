#!/usr/bin/env python3
"""Update JE TCK enumeration TSVs to mark Wave 8 ports as PORTED-EQUIVALENT.

Run from the repo root.  Idempotent: re-running over already-PORTED rows
is a no-op.
"""
import csv
import sys
from pathlib import Path

# (tsv_filename, je_class, je_method, noxu_test_path, noxu_test_method, status, notes)
PORTS = [
    # je.rep
    ("je-tck-port-2026-05-enumeration-je.rep.tsv", "StateChangeListenerTest", "testListenerReplacement", "crates/noxu-rep/tests/je_rep_top_level_tck.rs", "state_change_listener_replacement", "PORTED-EQUIVALENT", "wave 8: ported via RepTestBase harness"),
    ("je-tck-port-2026-05-enumeration-je.rep.tsv", "StateChangeListenerTest", "testBasic", "crates/noxu-rep/tests/je_rep_top_level_tck.rs", "state_change_listener_basic", "PORTED-EQUIVALENT", "wave 8: ported via RepTestBase harness"),
    ("je-tck-port-2026-05-enumeration-je.rep.tsv", "StateChangeListenerTest", "testSecondary", "crates/noxu-rep/tests/je_rep_top_level_tck.rs", "state_change_listener_secondary", "PORTED-EQUIVALENT", "wave 8: ported via RepTestBase harness"),
    ("je-tck-port-2026-05-enumeration-je.rep.tsv", "ReplicatedEnvironmentTest", "testEnvOpenOnRepEnv", "crates/noxu-rep/tests/je_rep_top_level_tck.rs", "rep_env_fresh_open_state_is_detached", "PORTED-PARTIAL", "wave 8: state-machine subset only"),
    ("je-tck-port-2026-05-enumeration-je.rep.tsv", "ReplicatedEnvironmentTest", "testRepEnvConfig", "crates/noxu-rep/tests/je_rep_top_level_tck.rs", "rep_env_config_round_trips", "PORTED-EQUIVALENT", "wave 8: ported via RepTestBase harness"),
    ("je-tck-port-2026-05-enumeration-je.rep.tsv", "ReplicatedEnvironmentTest", "testRepEnvMutableConfig", "crates/noxu-rep/tests/je_rep_top_level_tck.rs", "rep_env_close_reopen_returns_fresh_handle", "PORTED-PARTIAL", "wave 8: close+reopen subset"),
    ("je-tck-port-2026-05-enumeration-je.rep.tsv", "JoinGroupTest", "testAllJoinLeaveJoinGroup", "crates/noxu-rep/tests/je_rep_top_level_tck.rs", "join_group_join_leave_join", "PORTED-EQUIVALENT", "wave 8: ported via RepTestBase harness"),
    ("je-tck-port-2026-05-enumeration-je.rep.tsv", "JoinGroupTest", "testRepeatedOpen", "crates/noxu-rep/tests/je_rep_top_level_tck.rs", "join_group_repeated_open_fails", "PORTED-EQUIVALENT", "wave 8: ported via RepTestBase harness"),
    ("je-tck-port-2026-05-enumeration-je.rep.tsv", "ReplicationGroupTest", "testBasic", "crates/noxu-rep/tests/je_rep_top_level_tck.rs", "replication_group_basic_membership_visible", "PORTED-PARTIAL", "wave 8: membership subset"),
    ("je-tck-port-2026-05-enumeration-je.rep.tsv", "SecondaryNodeTest", "testJoinLeaveJoin", "crates/noxu-rep/tests/je_rep_top_level_tck.rs", "secondary_node_join_leave_join", "PORTED-EQUIVALENT", "wave 8: ported via RepTestBase harness"),
    ("je-tck-port-2026-05-enumeration-je.rep.tsv", "SecondaryNodeTest", "testSecondaryChangeMaster", "crates/noxu-rep/tests/je_rep_top_level_tck.rs", "secondary_node_follows_new_master", "PORTED-EQUIVALENT", "wave 8: ported via RepTestBase harness"),
    ("je-tck-port-2026-05-enumeration-je.rep.tsv", "ElectableGroupSizeOverrideTest", "testBasic", "crates/noxu-rep/tests/je_rep_top_level_tck.rs", "electable_group_size_override_quorum", "PORTED-EQUIVALENT", "wave 8: re-ported (was placeholder); tests Flexible quorum policy"),
    ("je-tck-port-2026-05-enumeration-je.rep.tsv", "NodePriorityTest", "testPriorityBasic", "crates/noxu-rep/tests/je_rep_top_level_tck.rs", "node_priority_higher_vlsn_can_be_master", "PORTED-PARTIAL", "wave 8: noxu uses VLSN+id tiebreak (no priority concept)"),

    # je.rep.txn
    ("je-tck-port-2026-05-enumeration-je.rep.txn.tsv", "CommitTokenTest", "testBasic", "crates/noxu-rep/tests/je_rep_txn_tck.rs", "commit_token_vlsns_are_strictly_increasing", "PORTED-PARTIAL", "wave 8: noxu does not yet expose CommitToken; asserts underlying VLSN ordering invariant"),
    ("je-tck-port-2026-05-enumeration-je.rep.txn.tsv", "CommitTokenTest", "testCommitTokenFailures", "crates/noxu-rep/tests/je_rep_txn_tck.rs", "empty_txn_does_not_advance_replica_vlsn", "PORTED-PARTIAL", "wave 8: stream-level analog (read-only/abort don't advance VLSN)"),
    ("je-tck-port-2026-05-enumeration-je.rep.txn.tsv", "RepAutoCommitTest", "testAutoCommit", "crates/noxu-rep/tests/je_rep_txn_tck.rs", "auto_commit_master_writes_replicate_to_all", "PORTED-PARTIAL", "wave 8: master-write fan-out subset; replica-write rejection captured separately"),
    ("je-tck-port-2026-05-enumeration-je.rep.txn.tsv", "PostLogCommitTest", "testPostLogCommitException", "crates/noxu-rep/tests/je_rep_txn_tck.rs", "post_log_commit_replica_catches_up", "PORTED-PARTIAL", "wave 8: catch-up subset"),
    ("je-tck-port-2026-05-enumeration-je.rep.txn.tsv", "RollbackTest", "testTxnEndBeforeMatchpoint", "crates/noxu-rep/tests/je_rep_txn_tck.rs", "rollback_preserves_entries_before_matchpoint", "PORTED-EQUIVALENT", "wave 8: ported via RepTestBase harness"),
    ("je-tck-port-2026-05-enumeration-je.rep.txn.tsv", "RollbackTest", "testTxnEndAfterMatchpoint", "crates/noxu-rep/tests/je_rep_txn_tck.rs", "rollback_discards_post_matchpoint_master_only_writes", "PORTED-EQUIVALENT", "wave 8: ported via RepTestBase harness"),
    ("je-tck-port-2026-05-enumeration-je.rep.txn.tsv", "RollbackTest", "testTxnStraddleMatchpoint", "crates/noxu-rep/tests/je_rep_txn_tck.rs", "rollback_straddling_txn_is_fully_discarded", "PORTED-EQUIVALENT", "wave 8: ported via RepTestBase harness"),
    ("je-tck-port-2026-05-enumeration-je.rep.txn.tsv", "RollbackTest", "testReplicasFlip", "crates/noxu-rep/tests/je_rep_txn_tck.rs", "rollback_old_master_rejoins_as_replica", "PORTED-EQUIVALENT", "wave 8: ported via RepTestBase harness"),
    ("je-tck-port-2026-05-enumeration-je.rep.txn.tsv", "LockPreemptionTest", "testPreempted", "crates/noxu-rep/tests/je_rep_txn_tck.rs", "apply_entry_on_shutdown_env_fails", "PORTED-PARTIAL", "wave 8: shutdown-env failure analog (no actual lock-preemption pathway)"),
    ("je-tck-port-2026-05-enumeration-je.rep.txn.tsv", "ExceptionTest", "test", "crates/noxu-rep/tests/je_rep_txn_tck.rs", "become_master_on_shutdown_env_fails", "PORTED-PARTIAL", "wave 8: state-machine subset; secondary_node_become_master_should_fail #[ignore]'d for real noxu bug"),
    ("je-tck-port-2026-05-enumeration-je.rep.txn.tsv", "ReplayRecoveryTest", "testRBRecoveryOneTxn", "crates/noxu-rep/tests/je_rep_txn_tck.rs", "replay_recovery_resumes_after_reopen", "PORTED-PARTIAL", "wave 8: stream-resume subset"),

    # je.rep.stream
    ("je-tck-port-2026-05-enumeration-je.rep.stream.tsv", "FeederReaderTest", "testForwardScans", "crates/noxu-rep/tests/je_rep_stream_tck.rs", "forward_scan_replicas_see_every_entry", "PORTED-PARTIAL", "wave 8: VLSN coverage subset"),
    ("je-tck-port-2026-05-enumeration-je.rep.stream.tsv", "FeederReaderTest", "testBackwardScans", "crates/noxu-rep/tests/je_rep_stream_tck.rs", "vlsn_range_first_and_last_are_consistent", "PORTED-PARTIAL", "wave 8: VLSN range invariant subset"),
    ("je-tck-port-2026-05-enumeration-je.rep.stream.tsv", "FeederReaderTest", "testFindSyncableentries", "crates/noxu-rep/tests/je_rep_stream_tck.rs", "replica_catch_up_starts_at_correct_vlsn", "PORTED-PARTIAL", "wave 8: catch-up start-VLSN subset"),
    ("je-tck-port-2026-05-enumeration-je.rep.stream.tsv", "FeederWriteQueueTest", "testDataInWriteQueue", "crates/noxu-rep/tests/je_rep_stream_tck.rs", "write_queue_preserves_vlsn_order", "PORTED-PARTIAL", "wave 8: VLSN-order subset"),
    ("je-tck-port-2026-05-enumeration-je.rep.stream.tsv", "ProtocolTest", "testBasic", "crates/noxu-rep/tests/je_rep_stream_tck.rs", "protocol_basic_full_fan_out", "PORTED-PARTIAL", "wave 8: fan-out smoke test"),
    ("je-tck-port-2026-05-enumeration-je.rep.stream.tsv", "ReplicaSyncupReaderTest", "testRepAndNonRepCommits", "crates/noxu-rep/tests/je_rep_stream_tck.rs", "syncup_distinguishes_replicated_vs_master_only", "PORTED-EQUIVALENT", "wave 8: ported via RepTestBase harness"),
    ("je-tck-port-2026-05-enumeration-je.rep.stream.tsv", "ReplicaSyncupReaderTest", "testMultipleCkpts", "crates/noxu-rep/tests/je_rep_stream_tck.rs", "multiple_checkpoint_chunks_replicate_cleanly", "PORTED-PARTIAL", "wave 8: multi-chunk fan-out subset (no on-disk checkpoints)"),
    ("je-tck-port-2026-05-enumeration-je.rep.stream.tsv", "FeederFilterTest", "testNoOpFilter", "crates/noxu-rep/tests/je_rep_stream_tck.rs", "feeder_filter_no_op_baseline", "PORTED-PARTIAL", "wave 8: no-filter baseline (noxu lacks pluggable feeder filters)"),
    ("je-tck-port-2026-05-enumeration-je.rep.stream.tsv", "FeederFilterTest", "testFilterWithStatistics", "crates/noxu-rep/tests/je_rep_stream_tck.rs", "feeder_no_silent_drops_under_replication", "PORTED-PARTIAL", "wave 8: silent-drop check"),
]


def update_tsv(tsv_path: Path, ports: list) -> int:
    rows = []
    with tsv_path.open() as f:
        reader = csv.reader(f, delimiter="\t")
        header = next(reader)
        rows = list(reader)
    # Field indices.  Header is:
    # je_package, je_class, je_test_method, je_test_path, je_test_doc,
    # noxu_status, noxu_test_path, noxu_test_method, effort_estimate,
    # priority, notes
    cls_i = header.index("je_class")
    mtd_i = header.index("je_test_method")
    status_i = header.index("noxu_status")
    path_i = header.index("noxu_test_path")
    fn_i = header.index("noxu_test_method")
    notes_i = header.index("notes")

    updates = 0
    for je_class, je_method, noxu_path, noxu_fn, status, notes in ports:
        for r in rows:
            if r[cls_i] == je_class and r[mtd_i] == je_method:
                r[status_i] = status
                r[path_i] = noxu_path
                r[fn_i] = noxu_fn
                r[notes_i] = notes
                updates += 1
                break

    with tsv_path.open("w") as f:
        writer = csv.writer(f, delimiter="\t", quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)

    return updates


def main():
    base = Path("docs/src/internal")
    by_file = {}
    for p in PORTS:
        by_file.setdefault(p[0], []).append(p[1:])
    total = 0
    for fname, ports in by_file.items():
        path = base / fname
        n = update_tsv(path, ports)
        print(f"{fname}: updated {n} / {len(ports)} rows")
        total += n
    print(f"TOTAL: {total} rows updated")


if __name__ == "__main__":
    main()
