// FID-CURSOR-1 crash-undo witness (deterministic; checkpointer daemon OFF).
//
// A transactional delete that is UNCOMMITTED at crash time but whose BIN was
// made durable by a (forced) checkpoint MUST be undone by recovery: the record
// is present again after reopen, because the deleting txn never committed.
//
// This is a crash-recovery analogue of the JE undo contract
// (RecoveryManager undo / IN.recoverRecord, IN.java:3837-3922): an active
// (uncommitted) transaction's writes are reversed, and a deleted slot is
// reverted to its before-image (abort LSN).
//
// Why it is load-bearing (the bug it guards):
//   v7.11.0 (b17f8258) handled a txn delete with `tree.delete()` — a PHYSICAL
//   slot removal at operation time (pre-commit). A forced checkpoint then
//   logged a BIN with NO slot for the deleted key, so after a crash recovery
//   had nothing to undo against: the uncommitted delete became PERMANENT
//   (an S1 data-loss bug). The slot-retention introduced on the v7.11.1 RC
//   (A5) and preserved here (as PENDING_DELETED) keeps the slot, so recovery
//   reverts it. This witness is GREEN on the RC and on this branch, and would
//   be RED on v7.11.0 (physical-removal). It is a permanent regression guard
//   against reverting to physical-removal-at-op-time for a txn delete.
//
// Deterministic: the background checkpointer daemon is DISABLED so the single
// forced checkpoint is the only one (no race) and the delete's BIN is durable
// at a known point. The crash is simulated by copying the env files (minus the
// lock) while the txn is still open, exactly as the dup_txn_undo crash tests do.

use noxu_db::{
    CheckpointConfig, DatabaseConfig, DatabaseEntry, Environment,
    EnvironmentConfig, Get, OperationStatus,
};
use std::sync::Arc;
use tempfile::TempDir;

fn snapshot_crash_image(dir: &std::path::Path) -> TempDir {
    let img = TempDir::new().unwrap();
    for f in std::fs::read_dir(dir).unwrap() {
        let f = f.unwrap();
        if f.file_type().unwrap().is_file() && f.file_name() != "noxu.lck" {
            std::fs::copy(f.path(), img.path().join(f.file_name())).unwrap();
        }
    }
    img
}

#[test]
fn fid_cursor1_crash_recovery_undoes_uncommitted_delete_deterministic() {
    let dir = TempDir::new().unwrap();
    let img = {
        let cfg = EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            .with_node_max_entries(6)
            .with_run_checkpointer(false); // deterministic single checkpoint
        let env = Arc::new(Environment::open(cfg).unwrap());
        let dbcfg = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true)
            .with_sorted_duplicates(false);
        let db = env.open_database(None, "xdb", &dbcfg).unwrap();
        // Seed A..G (two BINs for node_max=6).
        let txn = env.begin_transaction(None).unwrap();
        for k in [&b"A"[..], b"B", b"C", b"D", b"E", b"F", b"G"] {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(k),
                DatabaseEntry::from_bytes(&[1u8; 10]),
            )
            .unwrap();
        }
        txn.commit().unwrap();
        // Uncommitted delete of D, made durable by a forced checkpoint.
        let txn = env.begin_transaction(None).unwrap();
        assert!(
            db.delete_in(&txn, DatabaseEntry::from_bytes(b"D")).unwrap(),
            "delete D"
        );
        env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
            .unwrap();
        // Snapshot the crash state: delete durable in the log, txn UNRESOLVED.
        let img = snapshot_crash_image(dir.path());
        txn.abort().unwrap();
        db.close().unwrap();
        env.close().unwrap();
        img
    };

    // Reopen + recover from the crash image.
    let cfg = EnvironmentConfig::new(img.path().to_path_buf())
        .with_allow_create(false)
        .with_transactional(true)
        .with_node_max_entries(6)
        .with_run_checkpointer(false);
    let env = Arc::new(Environment::open(cfg).unwrap());
    let dbcfg = DatabaseConfig::new()
        .with_transactional(true)
        .with_sorted_duplicates(false);
    let db = env.open_database(None, "xdb", &dbcfg).unwrap();
    let txn = env.begin_transaction(None).unwrap();
    // D must be PRESENT: recovery undid the uncommitted delete.
    let mut k = DatabaseEntry::from_bytes(b"D");
    let mut d = DatabaseEntry::new();
    let s = {
        let mut c = db.open_cursor_in(&txn, None).unwrap();
        c.get(&mut k, &mut d, Get::Search, None).unwrap()
    };
    assert_eq!(
        s,
        OperationStatus::Success,
        "FID-CURSOR-1 crash-undo RED: D must be present after recovery undoes \
         the uncommitted delete that was checkpoint-durable at crash time. A \
         physical-removal-at-op-time delete (v7.11.0) would make it permanent."
    );
    // Full scan must show all seven keys (D restored, correct order).
    let mut got = Vec::new();
    {
        let mut c = db.open_cursor_in(&txn, None).unwrap();
        let mut kk = DatabaseEntry::new();
        let mut dd = DatabaseEntry::new();
        let mut st = c.get(&mut kk, &mut dd, Get::First, None).unwrap();
        while st == OperationStatus::Success {
            got.push(kk.data_opt().unwrap().to_vec());
            kk = DatabaseEntry::new();
            dd = DatabaseEntry::new();
            st = c.get(&mut kk, &mut dd, Get::Next, None).unwrap();
        }
    }
    txn.commit().unwrap();
    let want: Vec<Vec<u8>> = [b"A", b"B", b"C", b"D", b"E", b"F", b"G"]
        .iter()
        .map(|k| k.to_vec())
        .collect();
    assert_eq!(got, want, "scan must be A..G with D restored");
}
