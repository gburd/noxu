//! Regression: a writer must keep its old-LSN WRITE lock until its tree slot
//! update (and, for a Txn, until commit/abort), so the cleaner cannot migrate
//! the stale version in between.  JE: only HandleLocker may release old-LSN
//! locks after an LSN change (HandleLocker.java:117-120, Locker.java:775,
//! CursorImpl.java:3704-3733); LN.log locks the new LSN and keeps the old
//! (LN.java:742-758).  Schedule: writer pauses after `finalize_write_lock`,
//! before the slot update; the cleaner runs one full migration attempt; the
//! writer resumes and finishes; a crash image (no checkpoint) is reopened.
use super::*;
use crate::{DatabaseConfig, DatabaseEntry, Durability, OperationStatus, Put};
use noxu_cleaner::file_processor::{
    MigrationOutcome, SharedTreeLookup, TreeLookup,
};
use noxu_dbi::cursor_impl::PUBLICATION_PROBE;
use noxu_util::Lsn;
use std::sync::mpsc::channel;
use std::time::Duration;

const KEY: &[u8] = b"writer-oldlsn-key";

fn run(mode: &'static str) {
    let temp = tempfile::TempDir::new().unwrap();
    let mut cfg = EnvironmentConfig::new(temp.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        .with_durability(Durability::COMMIT_SYNC);
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    cfg.set_run_evictor(false);
    cfg.set_run_in_compressor(false);
    let env = Arc::new(Environment::open(cfg).unwrap());
    let db = Arc::new(
        env.open_database(
            None,
            "oldlsn",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap(),
    );
    let d0 = vec![0x30; 512];
    let d1 = vec![0x31; 512];
    db.put(KEY, &d0).unwrap();
    let (tree, lm, locks, db_id) = {
        let inner = env.env_impl.lock();
        let dbi = inner
            .get_all_database_impls()
            .into_iter()
            .find(|db| db.read().get_name() == "oldlsn")
            .unwrap();
        let dbi = dbi.read();
        (
            dbi.get_real_tree_arc().unwrap(),
            inner.get_log_manager().unwrap(),
            Arc::clone(inner.get_lock_manager()),
            dbi.get_id().id(),
        )
    };
    assert!(512 > tree.read().unwrap().max_embedded_ln);
    let slot_lsn = || {
        let slot = tree.read().unwrap().search_with_data(KEY).unwrap();
        assert!(slot.found);
        Lsn::from_u64(slot.lsn)
    };
    let l0 = slot_lsn();
    let lookup = SharedTreeLookup::with_lock_manager(
        tree.clone(),
        lm.clone(),
        locks.clone(),
    );
    let (at_tx, at_rx) = channel();
    let (go_tx, go_rx) = channel::<()>();
    let (wdb, wenv, wd1) = (db.clone(), env.clone(), d1.clone());
    let writer = std::thread::spawn(move || {
        PUBLICATION_PROBE.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |old, new| {
                assert_eq!(old, l0.as_u64());
                at_tx.send(new).unwrap();
                go_rx
                    .recv_timeout(Duration::from_secs(20))
                    .expect("writer resume timed out");
            }))
        });
        match mode {
            "txn" => {
                let txn = wenv.begin_transaction(None).unwrap();
                wdb.put_in(&txn, KEY, &wd1).unwrap();
                txn.commit_with_durability(Durability::COMMIT_SYNC).unwrap();
            }
            "auto" => wdb.put(KEY, &wd1).unwrap(),
            "cursor" => {
                // FID-CURSOR-AUTOCOMMIT: a cursor write on a transactional db
                // requires a transactional locker (JE
                // `Cursor.updateOperationsProhibited`); open the cursor under
                // an explicit txn.  This still exercises the cursor write
                // path (the old-LSN write-lock-until-slot-update invariant
                // under test), which is what this probe checks.
                let txn = wenv.begin_transaction(None).unwrap();
                {
                    let mut c = wdb.open_cursor_in(&txn, None).unwrap();
                    let st = c
                        .put(
                            &DatabaseEntry::from_bytes(KEY),
                            &DatabaseEntry::from_bytes(&wd1),
                            Put::Overwrite,
                        )
                        .unwrap();
                    assert_eq!(st, OperationStatus::Success);
                    c.close().unwrap();
                }
                txn.commit_with_durability(Durability::COMMIT_SYNC).unwrap();
            }
            _ => unreachable!(),
        }
    });
    let l1: Lsn = at_rx
        .recv_timeout(Duration::from_secs(20))
        .expect("writer never reached finalize_write_lock");
    // Writer paused: L1 logged and lock finalized, slot still L0.
    assert_eq!(slot_lsn(), l0);
    let l0_info = locks.get_lock_info(l0.as_u64());
    let outcome = lookup.migrate_ln_slot(db_id, KEY, l0, l0);
    eprintln!(
        "mode={mode} L0={l0:?} L1={l1:?} lock_info(L0) at pause={l0_info:?} \
         cleaner outcome={outcome:?} slot after cleaner={:?}",
        slot_lsn()
    );
    go_tx.send(()).unwrap();
    writer.join().unwrap();
    assert_eq!(db.get(KEY).unwrap().unwrap().as_ref(), d1.as_slice());

    // Crash image without a checkpoint: flush, copy minus env lock, reopen.
    lm.flush_sync().unwrap();
    let image = tempfile::TempDir::new().unwrap();
    for e in std::fs::read_dir(temp.path()).unwrap() {
        let e = e.unwrap();
        if e.file_type().unwrap().is_file() && e.file_name() != "noxu.lck" {
            std::fs::copy(e.path(), image.path().join(e.file_name())).unwrap();
        }
    }
    let mut rcfg = EnvironmentConfig::new(image.path().to_path_buf())
        .with_transactional(true);
    rcfg.set_run_cleaner(false);
    rcfg.set_run_checkpointer(false);
    let renv = Environment::open(rcfg).unwrap();
    let rdb = renv
        .open_database(
            None,
            "oldlsn",
            &DatabaseConfig::new().with_transactional(true),
        )
        .unwrap();
    let got = rdb.get(KEY).unwrap().unwrap();
    eprintln!(
        "RESULT mode={mode}: cleaner outcome={outcome:?}; recovered first byte={:#x}",
        got[0]
    );
    assert_eq!(
        got.as_ref(),
        d1.as_slice(),
        "mode={mode}: recovered value must be committed D1, not cleaner-logged D0 (outcome={outcome:?})"
    );
    assert!(
        matches!(
            outcome,
            MigrationOutcome::Locked | MigrationOutcome::Obsolete
        ),
        "mode={mode}: cleaner must not migrate while writer owns L0: {outcome:?}"
    );
    rdb.close().unwrap();
    renv.close().unwrap();
    Arc::try_unwrap(db).ok().unwrap().close().unwrap();
    Arc::try_unwrap(env).ok().unwrap().close().unwrap();
}

#[test]
fn writer_oldlsn_explicit_txn_keeps_committed() {
    run("txn");
}
#[test]
fn writer_oldlsn_auto_commit_keeps_committed() {
    run("auto");
}
#[test]
fn writer_oldlsn_no_txn_cursor_keeps_committed() {
    run("cursor");
}
