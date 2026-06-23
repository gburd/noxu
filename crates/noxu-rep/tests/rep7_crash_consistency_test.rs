//! REP-7 (C) crash-consistency: the live-applied tree == the recovery-redo
//! tree.
//!
//! The replica writes each streamed entry to its WAL (durability) AND applies
//! it to the live in-memory tree (the read-serving optimization).  On a
//! replica crash, recovery must reproduce the SAME tree the live-apply
//! produced — the WAL is the source of truth and the live-apply is an
//! optimization recovery re-derives.  A divergence between live-apply and
//! recovery-redo would be a correctness bug (worse than warm-standby).
//!
//! This is structurally guaranteed: the live-apply
//! ([`noxu_dbi::ReplicaReplay`]) and the crash-recovery redo pass both call
//! the SAME `noxu_recovery::apply_redo_ln`, and recovery scans the SAME WAL
//! entries the live-apply consumed.  These tests PROVE it end-to-end:
//!
//! 1. `test_replica_crash_recovers_to_live_applied_state` — stream a mix of
//!    committed records into a replica (WAL + live-apply), snapshot the live
//!    tree, then "crash" (drop the env without close) and reopen so recovery
//!    rebuilds the tree from the WAL.  Assert the recovered tree == the
//!    live-applied snapshot (no double-apply, no missing).
//!
//! 2. `test_aborted_txn_absent_from_both` — an aborted master txn is visible
//!    in neither the live tree nor the recovered tree.

use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use noxu_dbi::{DatabaseConfig, EnvironmentImpl, ReplicaReplay};
use noxu_log::entry::{LnLogEntry, TxnEndEntry};
use noxu_log::{LogEntryType, LogManager};
use noxu_rep::net::channel::{Channel, LocalChannelPair};
use noxu_rep::stream::replica_stream::{EnvironmentLogWriter, ReplicaReceiver};
use noxu_rep::vlsn::vlsn_index::VlsnIndex;
use noxu_util::{NULL_LSN, NULL_VLSN};

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

fn txn_end_payload(txn_id: i64, commit: bool) -> Vec<u8> {
    let e = if commit {
        TxnEndEntry::new_commit(txn_id, NULL_LSN, 0, 0, NULL_VLSN)
    } else {
        TxnEndEntry::new_abort(txn_id, NULL_LSN, 0, 0, NULL_VLSN)
    };
    let mut buf = BytesMut::new();
    e.write_to_log(&mut buf);
    buf.to_vec()
}

/// Read every (key, value) currently live in a tree by probing a known set of
/// keys.  We use a fixed key set so the comparison is deterministic.
fn read_keys(
    tree: &Arc<std::sync::RwLock<noxu_tree::Tree>>,
    keys: &[&[u8]],
) -> Vec<(Vec<u8>, Option<Vec<u8>>)> {
    let g = tree.read().unwrap();
    keys.iter()
        .filter_map(|k| {
            let f = g.search_with_data(k)?;
            if f.found { Some((k.to_vec(), f.data)) } else { None }
        })
        .collect()
}

/// Drive a full receive cycle: send `frames`, run the receiver against a
/// `with_replay` writer, collect acks, return when the channel closes.
fn stream_into(
    env: &Arc<EnvironmentImpl>,
    log_mgr: Arc<LogManager>,
    frames: Vec<Vec<u8>>,
) {
    let pair = LocalChannelPair::new();
    let master: Arc<dyn Channel> = Arc::new(pair.channel_a);
    let replica: Arc<dyn Channel> = Arc::new(pair.channel_b);

    let n = frames.len();
    let m = Arc::clone(&master);
    let handle = std::thread::spawn(move || {
        for f in &frames {
            m.send(f).unwrap();
        }
        for _ in 0..n {
            let _ = m.receive(Duration::from_secs(5));
        }
        m.close().unwrap();
    });

    let replay = ReplicaReplay::new(Arc::clone(env));
    let vlsn_index = Arc::new(VlsnIndex::new(10));
    let mut writer =
        EnvironmentLogWriter::with_replay(log_mgr, vlsn_index, replay);
    let receiver = ReplicaReceiver::new(replica);
    receiver.run(&mut writer).unwrap();
    handle.join().unwrap();
}

// ─── HEADLINE 3: crash recovers to the live-applied state ───────────────────

#[test]
fn test_replica_crash_recovers_to_live_applied_state() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    let insert_ln = LogEntryType::InsertLN.type_num();
    let insert_ln_txn = LogEntryType::InsertLNTxn.type_num();
    let txn_commit = LogEntryType::TxnCommit.type_num();

    let probe_keys: Vec<&[u8]> =
        vec![b"k1", b"k2", b"k3", b"tk1", b"tk2", b"absent"];

    // ── Session 1: open replica, stream a mix, snapshot the LIVE tree ──────
    let live_snapshot;
    let db_id;
    {
        let env = Arc::new(EnvironmentImpl::new(&path, false, true).unwrap());
        let mut cfg = DatabaseConfig::new();
        cfg.set_allow_create(true).set_transactional(true);
        let db = env.open_database("repl_db", &cfg).unwrap();
        db_id = db.read().get_id().id() as u64;
        let tree = env.replica_tree_for_db(db_id).unwrap();
        let log_mgr = env.get_log_manager().unwrap();

        // Master writes: 3 non-txn committed records + a 2-record committed
        // txn (5).  All must survive a crash.
        let frames = vec![
            make_frame(
                1,
                insert_ln,
                &ln_payload(db_id, None, b"k1", Some(b"v1")),
            ),
            make_frame(
                2,
                insert_ln,
                &ln_payload(db_id, None, b"k2", Some(b"v2")),
            ),
            make_frame(
                3,
                insert_ln,
                &ln_payload(db_id, None, b"k3", Some(b"v3")),
            ),
            make_frame(
                4,
                insert_ln_txn,
                &ln_payload(db_id, Some(5), b"tk1", Some(b"w1")),
            ),
            make_frame(
                5,
                insert_ln_txn,
                &ln_payload(db_id, Some(5), b"tk2", Some(b"w2")),
            ),
            make_frame(6, txn_commit, &txn_end_payload(5, true)),
        ];
        stream_into(&env, Arc::clone(&log_mgr), frames);

        // Snapshot the live-applied tree.
        live_snapshot = read_keys(&tree, &probe_keys);

        // Flush the WAL so recovery can find the entries, then "crash":
        // drop the env WITHOUT close (no clean shutdown / final checkpoint).
        log_mgr.flush_sync().unwrap();
        // Drop env (simulated crash — no env.close()).
        drop(tree);
        drop(env);
    }

    // The live tree saw all committed records.
    assert!(
        live_snapshot.iter().any(|(k, _)| k == b"k1"),
        "live-apply must have populated the tree"
    );
    assert_eq!(
        live_snapshot.len(),
        5,
        "live tree should hold 3 non-txn + 2 committed-txn records, got {:?}",
        live_snapshot
    );

    // ── Session 2: reopen → recovery rebuilds the tree from the WAL ────────
    let recovered_snapshot;
    {
        let env = Arc::new(EnvironmentImpl::new(&path, false, true).unwrap());
        let mut cfg = DatabaseConfig::new();
        cfg.set_allow_create(true).set_transactional(true);
        // Reopen the same database — recovery transplants its recovered tree.
        let db = env.open_database("repl_db", &cfg).unwrap();
        let rdb_id = db.read().get_id().id() as u64;
        assert_eq!(rdb_id, db_id, "db id must be stable across recovery");
        let tree = env.replica_tree_for_db(rdb_id).unwrap();
        recovered_snapshot = read_keys(&tree, &probe_keys);
    }

    // ── The recovered tree must MATCH the live-applied tree ────────────────
    let mut live_sorted = live_snapshot;
    let mut rec_sorted = recovered_snapshot;
    live_sorted.sort();
    rec_sorted.sort();
    assert_eq!(
        rec_sorted, live_sorted,
        "crash-consistency: recovery-redo tree must equal the live-applied \
         tree (no double-apply, no missing).\n live={:?}\n recovered={:?}",
        live_sorted, rec_sorted,
    );
}

// ─── An aborted txn appears in neither tree ─────────────────────────────────

#[test]
fn test_aborted_txn_absent_from_both() {
    let dir = tempfile::TempDir::new().unwrap();
    let path = dir.path().to_path_buf();

    let insert_ln = LogEntryType::InsertLN.type_num();
    let insert_ln_txn = LogEntryType::InsertLNTxn.type_num();
    let txn_abort = LogEntryType::TxnAbort.type_num();

    let probe_keys: Vec<&[u8]> = vec![b"keep", b"abrt"];

    let live_snapshot;
    {
        let env = Arc::new(EnvironmentImpl::new(&path, false, true).unwrap());
        let mut cfg = DatabaseConfig::new();
        cfg.set_allow_create(true).set_transactional(true);
        let db = env.open_database("repl_db", &cfg).unwrap();
        let db_id = db.read().get_id().id() as u64;
        let tree = env.replica_tree_for_db(db_id).unwrap();
        let log_mgr = env.get_log_manager().unwrap();

        let frames = vec![
            make_frame(
                1,
                insert_ln,
                &ln_payload(db_id, None, b"keep", Some(b"v")),
            ),
            make_frame(
                2,
                insert_ln_txn,
                &ln_payload(db_id, Some(9), b"abrt", Some(b"x")),
            ),
            make_frame(3, txn_abort, &txn_end_payload(9, false)),
        ];
        stream_into(&env, Arc::clone(&log_mgr), frames);

        live_snapshot = read_keys(&tree, &probe_keys);
        log_mgr.flush_sync().unwrap();
        drop(tree);
        drop(env);
    }

    // Live: "keep" present, "abrt" absent.
    assert!(live_snapshot.iter().any(|(k, _)| k == b"keep"));
    assert!(
        !live_snapshot.iter().any(|(k, _)| k == b"abrt"),
        "aborted txn must not be live"
    );

    // Recovered: same.
    let recovered_snapshot;
    {
        let env = Arc::new(EnvironmentImpl::new(&path, false, true).unwrap());
        let mut cfg = DatabaseConfig::new();
        cfg.set_allow_create(true).set_transactional(true);
        let db = env.open_database("repl_db", &cfg).unwrap();
        let db_id = db.read().get_id().id() as u64;
        let tree = env.replica_tree_for_db(db_id).unwrap();
        recovered_snapshot = read_keys(&tree, &probe_keys);
    }
    assert!(recovered_snapshot.iter().any(|(k, _)| k == b"keep"));
    assert!(
        !recovered_snapshot.iter().any(|(k, _)| k == b"abrt"),
        "aborted txn must not survive recovery either"
    );

    let mut a = live_snapshot;
    let mut b = recovered_snapshot;
    a.sort();
    b.sort();
    assert_eq!(a, b, "live and recovered trees must agree on the abort");
}
