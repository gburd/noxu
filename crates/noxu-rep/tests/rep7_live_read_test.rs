//! REP-7 headline integration tests: a streaming replica serves LIVE reads.
//!
//! Port of the JE replica read path: `Replay.replayEntry` applies each
//! streamed entry to the replica's live tree, so a read on the replica returns
//! the master's committed data WITHOUT a restart / recovery.
//!
//! ## What is tested (the 3 headline gates)
//!
//! 1. `test_replica_serves_live_read_without_restart` — the master writes
//!    records; the replica streams them through `ReplicaReceiver` +
//!    `EnvironmentLogWriter::with_replay`; a READ on the replica's LIVE tree
//!    returns the master's data with NO restart/recovery.
//!    - **Fail-pre (origin/main):** `EnvironmentLogWriter` only writes the WAL
//!      byte-shadow; the replica's in-memory tree stays empty, so the read
//!      returns nothing (the tree is only materialised by recovery on
//!      restart).
//!    - **Pass-post:** the live-apply populates the tree; the read returns the
//!      data live.
//!
//! 2. `test_replica_does_not_see_uncommitted_txn` — a read on the replica does
//!    NOT see an UNCOMMITTED master txn until its commit streams in
//!    (provisional-apply resolved at commit; JE `Replay` `ReplayTxn`).
//!
//! 3. The crash-consistency gate (live-apply == recovery-redo) is covered by
//!    `rep7_crash_consistency_test.rs` (piece C).

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use noxu_dbi::{EnvironmentImpl, ReplicaReplay};
use noxu_log::entry::{LnLogEntry, TxnEndEntry};
use noxu_log::{LogEntryType, LogManager};
use noxu_rep::net::channel::{Channel, LocalChannelPair};
use noxu_rep::stream::replica_stream::{EnvironmentLogWriter, ReplicaReceiver};
use noxu_rep::vlsn::vlsn_index::VlsnIndex;
use noxu_util::{Lsn, NULL_LSN, NULL_VLSN};

// ─── wire helpers (what a master feeder sends) ──────────────────────────────

const FRAME_HEADER_LEN: usize = 8 + 1 + 4 + 4;

fn make_frame(vlsn: u64, entry_type: u8, payload: &[u8]) -> Vec<u8> {
    let crc = crc32fast::hash(payload);
    let mut f = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
    f.extend_from_slice(&vlsn.to_le_bytes());
    f.push(entry_type);
    f.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    f.extend_from_slice(&crc.to_le_bytes());
    f.extend_from_slice(payload);
    f
}

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

fn txn_commit_payload(txn_id: i64) -> Vec<u8> {
    let e = TxnEndEntry::new_commit(txn_id, NULL_LSN, 0, 0, NULL_VLSN);
    let mut buf = BytesMut::new();
    e.write_to_log(&mut buf);
    buf.to_vec()
}

/// Open a replica `EnvironmentImpl`, open the replicated database, and return
/// the live tree handle + log manager for wiring the receive path.
fn replica_setup() -> (
    Arc<EnvironmentImpl>,
    u64,
    Arc<std::sync::RwLock<noxu_tree::Tree>>,
    Arc<LogManager>,
) {
    use noxu_dbi::DatabaseConfig;
    let dir = tempfile::TempDir::new().unwrap();
    // Keep the dir alive for the test (env holds open file handles).
    let path = dir.keep();
    let env = Arc::new(EnvironmentImpl::new(&path, false, true).unwrap());
    let mut cfg = DatabaseConfig::new();
    cfg.set_allow_create(true).set_transactional(true);
    let db = env.open_database("repl_db", &cfg).unwrap();
    let db_id = db.read().get_id().id() as u64;
    let tree = env.replica_tree_for_db(db_id).unwrap();
    let log_mgr = env.get_log_manager().unwrap();
    (env, db_id, tree, log_mgr)
}

// ─── HEADLINE 1: replica serves a live read without restart ─────────────────

#[test]
fn test_replica_serves_live_read_without_restart() {
    let (env, db_id, tree, log_mgr) = replica_setup();
    let insert_ln = LogEntryType::InsertLN.type_num();

    // Master writes 3 NON-transactional committed records.
    let frames = vec![
        make_frame(1, insert_ln, &ln_payload(db_id, None, b"k1", Some(b"v1"))),
        make_frame(2, insert_ln, &ln_payload(db_id, None, b"k2", Some(b"v2"))),
        make_frame(3, insert_ln, &ln_payload(db_id, None, b"k3", Some(b"v3"))),
    ];

    let pair = LocalChannelPair::new();
    let master_side: Arc<dyn Channel> = Arc::new(pair.channel_a);
    let replica_side: Arc<dyn Channel> = Arc::new(pair.channel_b);

    let master_clone = Arc::clone(&master_side);
    let master_handle = std::thread::spawn(move || {
        for f in &frames {
            master_clone.send(f).unwrap();
        }
        for _ in 0..3 {
            let _ = master_clone.receive(Duration::from_secs(5));
        }
        master_clone.close().unwrap();
    });

    // Replica: the wired receive path (WAL + VLSN index + live tree apply).
    let replay = ReplicaReplay::new(Arc::clone(&env));
    let vlsn_index = Arc::new(VlsnIndex::new(10));
    let mut writer =
        EnvironmentLogWriter::with_replay(log_mgr, vlsn_index, replay);
    let receiver = ReplicaReceiver::new(Arc::clone(&replica_side));
    receiver.run(&mut writer).unwrap();
    master_handle.join().unwrap();

    // READ on the replica's LIVE tree — no restart, no recovery.
    for (k, v) in [(b"k1", b"v1"), (b"k2", b"v2"), (b"k3", b"v3")] {
        let fetch = tree.read().unwrap().search_with_data(k);
        let fetch = fetch.unwrap_or_else(|| {
            panic!(
                "FAIL-PRE: replica read of {:?} returned nothing (origin/main \
                 replica tree is empty until recovery)",
                std::str::from_utf8(k).unwrap()
            )
        });
        assert!(fetch.found, "replica read must find {:?}", k);
        assert_eq!(
            fetch.data.as_deref(),
            Some(&v[..]),
            "replica read must return the master's value for {:?}",
            k
        );
    }
}

// ─── HEADLINE 2: replica does NOT see an uncommitted master txn ─────────────
//
// Stream order: LN(txn 42) → [read: invisible] → commit(42) → [read: visible].
// The `ReplicaReplay` is driven directly here to assert the phase boundary
// precisely; the WAL + receiver wiring around it is proven by headline 1.

#[test]
fn test_replica_does_not_see_uncommitted_txn() {
    let (env, db_id, tree, _log_mgr) = replica_setup();
    let insert_ln_txn = LogEntryType::InsertLNTxn.type_num();
    let txn_commit = LogEntryType::TxnCommit.type_num();

    let mut replay = ReplicaReplay::new(Arc::clone(&env));

    // Stream a transactional insert (txn 42) — buffered, NOT yet visible.
    let p = ln_payload(db_id, Some(42), b"tk", Some(b"tv"));
    replay.apply_entry(1, insert_ln_txn, &p, Lsn::new(0, 100));

    let before = tree.read().unwrap().search_with_data(b"tk");
    assert!(
        before.as_ref().map(|f| f.found) != Some(true),
        "uncommitted master txn must NOT be visible on the replica before \
         its commit streams in"
    );
    assert_eq!(replay.last_applied_vlsn(), 0, "no commit yet");

    // Stream the commit — now the record IS visible.
    let c = txn_commit_payload(42);
    replay.apply_entry(2, txn_commit, &c, Lsn::new(0, 200));

    let after = tree.read().unwrap().search_with_data(b"tk");
    let after = after.expect("committed txn record must be visible");
    assert!(after.found, "committed txn must be visible after commit streams");
    assert_eq!(after.data.as_deref(), Some(&b"tv"[..]));
    assert_eq!(replay.last_applied_vlsn(), 2);
}
