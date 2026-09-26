//! JE recovery test-parity ports, batch 2: the level-2-split-vs-checkpoint
//! data invariant (Level2SplitBugTest) and the dup-slot-reuse-after-recovery
//! scenario (RecoveryAbortTest.testSR13726).
//!
//! Both JE originals drive an internal race / internal-node mechanism via
//! reflection (`Checkpointer.examineINForCheckpointHook`, manual `child.split`,
//! DIN/DBIN dup-tree nodes). Noxu forbids test access to engine internals and
//! stores duplicates without DIN/DBIN nodes, so these ports assert the
//! observable RECOVERY BEHAVIOR the JE tests ultimately protect: after a heavy
//! split workload + checkpoint, and after a dup delete/re-insert sequence that
//! leaves a known-deleted slot, recovery yields exactly the committed data
//! (no dirty BIN lost by the checkpoint, no ghost/stale slot resurrected).

#![allow(clippy::unwrap_used)]

use noxu_db::{
    CheckpointConfig, DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get,
    OperationStatus, VerifyConfig,
};
use std::path::Path;
use tempfile::TempDir;

fn open_env(dir: &Path, node_max: u32) -> noxu_db::Environment {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    cfg.set_run_evictor(false);
    cfg.set_run_in_compressor(false);
    if node_max > 0 {
        cfg.set_node_max_entries(node_max);
    }
    noxu_db::Environment::open(cfg).unwrap()
}

fn open_db(env: &noxu_db::Environment, dups: bool) -> noxu_db::Database {
    env.open_database(
        None,
        "foo",
        &DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true)
            .with_sorted_duplicates(dups),
    )
    .unwrap()
}

fn ckpt(env: &noxu_db::Environment) {
    env.checkpoint(Some(&CheckpointConfig::new().with_force(true))).unwrap();
}

fn ikey(i: u32) -> String {
    format!("k{i:08}")
}

// ===========================================================================
// JE Level2SplitBugTest.testLevel2SplitBug   (data invariant)
//
// JE reproduces an off-heap-only bug where a level-2 IN split concurrent with
// a checkpoint's INList iteration could leave a dirty BIN un-logged, so
// recovery lost records. JE forces the race with a static test hook
// (Checkpointer.examineINForCheckpointHook), a manual child.split() on a
// separate thread, and INList reflection — none of which Noxu exposes (and JE
// itself concedes the race "can't be relied on"). Noxu coordinates split with
// checkpoint via its DirtyINMap; the OBSERVABLE guarantee is: after a workload
// that splits level-2 INs and a forced checkpoint, every committed record is
// present in-memory (verify) AND after recovery.
//
// Method: write 10*NODE_MAX^2 sequential records (JE N_RECORDS) at NODE_MAX=30
// so the tree grows past 2 levels with many level-2 INs, WRITE THEM AGAIN to
// dirty every BIN, force a checkpoint, verify the live tree, then close +
// recover and verify the exact sequential set. A dirty BIN dropped by the
// checkpoint would lose records → the post-recovery scan gaps.
// ===========================================================================
#[test]
fn level2_split_vs_checkpoint_data_survives() {
    const NODE_MAX: u32 = 30;
    const N: u32 = 10 * NODE_MAX * NODE_MAX; // JE N_RECORDS = 9000
    let dir = TempDir::new().unwrap();

    {
        let env = open_env(dir.path(), NODE_MAX);
        let db = open_db(&env, false);

        // First write pass (builds the multi-level tree).
        let txn = env.begin_transaction(None).unwrap();
        for i in 0..N {
            let k = ikey(i);
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(k.as_bytes()),
                DatabaseEntry::from_bytes(k.as_bytes()),
            )
            .unwrap();
        }
        txn.commit().unwrap();

        // Second write pass (JE: write() again → all BINs dirty).
        let txn = env.begin_transaction(None).unwrap();
        for i in 0..N {
            let k = ikey(i);
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(k.as_bytes()),
                DatabaseEntry::from_bytes(k.as_bytes()),
            )
            .unwrap();
        }
        txn.commit().unwrap();

        // Force a checkpoint while every BIN is dirty (JE's checkpoint that
        // races the level-2 split). All dirty BINs must be flushed.
        ckpt(&env);

        // JE verify(): live sequential scan hits every key in order.
        {
            let mut c = db.open_cursor(None).unwrap();
            let mut k = DatabaseEntry::new();
            let mut d = DatabaseEntry::new();
            let mut expect = 0u32;
            let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
            while s == OperationStatus::Success {
                let got = k.data_opt().unwrap();
                assert_eq!(
                    got,
                    ikey(expect).as_bytes(),
                    "pre-close: sequential scan gap at {expect}"
                );
                expect += 1;
                s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
            }
            c.close().unwrap();
            assert_eq!(expect, N, "pre-close: must see all {N} keys");
        }

        db.close().unwrap();
        env.close().unwrap();
    }

    // Recover and verify the exact sequential set (JE open(); verify()).
    let env = open_env(dir.path(), NODE_MAX);
    let db = open_db(&env, false);
    let vr = env.verify(&VerifyConfig::new()).unwrap();
    assert_eq!(vr.error_count(), 0, "verify errors: {:?}", vr.errors);
    let mut c = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut expect = 0u32;
    let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
    while s == OperationStatus::Success {
        assert_eq!(
            k.data_opt().unwrap(),
            ikey(expect).as_bytes(),
            "post-recovery: sequential scan gap at {expect} (a dirty BIN was \
             dropped by the checkpoint during a level-2 split)"
        );
        expect += 1;
        s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    c.close().unwrap();
    assert_eq!(expect, N, "post-recovery: must see all {N} keys");
    assert_eq!(db.count().unwrap(), N as u64);
    drop(db);
    drop(env);
}

// ===========================================================================
// JE RecoveryAbortTest.testSR13726
//
// A BIN slot that ends up referring to a deleted dup (known-deleted) after a
// delete + compress + recover must accept later dup insertions correctly: a
// dup batch inserted-then-aborted must leave no trace, and a subsequent
// committed dup batch must be fully readable, with cursor.count() == the
// committed dup count.
//
// JE drives this through DupCountLN / DelDupLN internal nodes; Noxu has no such
// nodes, so the port exercises the same observable sequence: insert dups +
// commit, delete the key + commit, compress, recover, then insert dups +
// abort (no trace) and insert dups + commit (all readable).
// ===========================================================================
#[test]
fn sr13726_deldup_slot_reuse_after_recover() {
    let dir = TempDir::new().unwrap();
    let key = b"k0".to_vec();

    {
        let env = open_env(dir.path(), 0);
        let db = open_db(&env, true);

        // Insert some dups, commit (JE: 3 dups → a dup structure).
        let txn = env.begin_transaction(None).unwrap();
        for i in 0u32..3 {
            let d = ikey(i).into_bytes();
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(&key),
                DatabaseEntry::from_bytes(&d),
            )
            .unwrap();
        }
        txn.commit().unwrap();

        // Delete the whole key (→ a deleted-dup slot), commit.
        let txn = env.begin_transaction(None).unwrap();
        assert!(
            db.delete_in(&txn, DatabaseEntry::from_bytes(&key)).unwrap(),
            "delete initial dups"
        );
        txn.commit().unwrap();

        // Compress → clean up the dup slot (JE env.compress()).
        let _ = env.compress().unwrap();

        db.close().unwrap();
        env.close().unwrap();
    }

    // Recover: the BIN now refers to a deleted (known-deleted) slot.
    let env = open_env(dir.path(), 0);
    let db = open_db(&env, true);
    let vr = env.verify(&VerifyConfig::new()).unwrap();
    assert_eq!(vr.error_count(), 0, "verify errors: {:?}", vr.errors);
    // Key must be absent after the delete + recover.
    assert!(
        db.get(&key[..]).unwrap().is_none(),
        "deleted key must not resurrect after recovery"
    );

    // Insert dups under the same key, ABORT → must leave no trace.
    let txn = env.begin_transaction(None).unwrap();
    for i in 0u32..3 {
        let d = ikey(i).into_bytes();
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&key),
            DatabaseEntry::from_bytes(&d),
        )
        .unwrap();
    }
    txn.abort().unwrap();
    assert!(
        db.get(&key[..]).unwrap().is_none(),
        "aborted dup batch must leave no trace on the known-deleted slot"
    );

    // Insert dups under the same key, COMMIT → all readable.
    let txn = env.begin_transaction(None).unwrap();
    for i in 0u32..3 {
        let d = ikey(i).into_bytes();
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&key),
            DatabaseEntry::from_bytes(&d),
        )
        .unwrap();
    }
    txn.commit().unwrap();

    // cursor.count() on the dup chain == committed dup count (JE assertion).
    let mut c = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    assert_eq!(
        c.get(&mut k, &mut d, Get::First, None).unwrap(),
        OperationStatus::Success,
        "committed dups must be present after slot reuse"
    );
    assert_eq!(k.data_opt().unwrap(), &key[..]);
    let mut scanned = 0u64;
    let mut s = OperationStatus::Success;
    while s == OperationStatus::Success {
        if k.data_opt().unwrap() == &key[..] {
            scanned += 1;
        }
        s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
    }
    c.close().unwrap();
    assert_eq!(scanned, 3, "exactly 3 committed dups readable via cursor");
    assert_eq!(db.count().unwrap(), 3, "db.count() == committed dup count");
    drop(db);
    drop(env);
}
