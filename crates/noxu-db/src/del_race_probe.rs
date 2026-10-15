//! NEW-DEL-RACE-1 recurrence witness (TRIAGE): two transactions deleting the
//! SAME record must serialize so that EXACTLY ONE reports success; every other
//! deleter must observe the record already gone (KeyEmpty / NotFound) or lose a
//! lock conflict.  This reproduces, deterministically, the race the stack-v1
//! full-workspace census hit only intermittently
//! (`je_secondary_multi_test::test_multi_delete_unordered`, success=2).
//!
//! Mechanism (coordinator hypothesis, confirmed here):
//!   Deleter 1 logs its DeleteLN at L1 and, in `finalize_write_lock`, moves its
//!   WRITE lock L0 -> L1.  It then runs `apply_tree_delete`, which publishes the
//!   deletion (slot L0 -> gone).  The window BETWEEN the lock move and the slot
//!   update is exactly where `PUBLICATION_PROBE` fires.
//!     * At BASE `Txn::move_write_lock_to_new_lsn` / the no-txn
//!       `finalize_write_lock` branch released WRITE(L0) EAGERLY in that window.
//!       Deleter 2, blocked on WRITE(L0), was granted it while the slot still
//!       showed L0, so its post-lock revalidation (`current_lsn == old_lsn`)
//!       PASSED and it deleted+committed a SECOND time -> success=2.
//!     * On RC, WRITE(L0) is RETAINED until commit/abort (txn, 8955a610) or
//!       until after `apply_tree_delete` (no-txn, d3f9a6ed) -- JE
//!       `Locker.allowReleaseLockAfterLsnChange()==false` (Locker.java:775).
//!       Deleter 2 therefore wakes only after the slot is already updated, sees
//!       `current_lsn != old_lsn`, and returns KeyEmpty.
//!
//! Schedule: deleter 1 pauses at `PUBLICATION_PROBE` (lock moved, slot still
//! L0); deleter 2 is launched and must NOT be able to complete a second
//! successful delete while deleter 1 holds WRITE(L0); deleter 1 resumes and
//! commits; deleter 2 then finishes.  Exactly one success.
use super::*;
use crate::{DatabaseConfig, Durability};
use noxu_dbi::cursor_impl::PUBLICATION_PROBE;
use noxu_util::Lsn;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::channel;
use std::time::Duration;

const KEY: &[u8] = b"del-race-key";

/// `mode` selects the locker on the two deleters:
///   "txn"  -> explicit transactions (exercises `move_write_lock_to_new_lsn`,
///             fixed by 8955a610);
///   "auto" -> auto-commit deletes (exercises the no-txn `finalize_write_lock`
///             branch, fixed by d3f9a6ed).
fn run(mode: &'static str) {
    let temp = tempfile::TempDir::new().unwrap();
    let mut cfg = EnvironmentConfig::new(temp.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        .with_durability(Durability::COMMIT_SYNC);
    // Daemons off: this witness is about deleter/deleter serialization, not the
    // cleaner (that is BUG-CLEANER-LIVE-FILE-DELETE, a separate witness).
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    cfg.set_run_evictor(false);
    cfg.set_run_in_compressor(false);
    let env = Arc::new(Environment::open(cfg).unwrap());
    let db = Arc::new(
        env.open_database(
            None,
            "delrace",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap(),
    );
    // Seed the single committed record both deleters will race to remove.
    db.put(KEY, b"v0").unwrap();

    let slot_lsn = {
        let inner = env.env_impl.lock();
        let dbi = inner
            .get_all_database_impls()
            .into_iter()
            .find(|d| d.read().get_name() == "delrace")
            .unwrap();
        let tree = dbi.read().get_real_tree_arc().unwrap();
        move || {
            let slot = tree.read().unwrap().search_with_data(KEY).unwrap();
            if slot.found {
                Lsn::from_u64(slot.lsn)
            } else {
                noxu_util::NULL_LSN
            }
        }
    };
    let l0 = slot_lsn();
    assert!(!l0.is_null(), "seed must have a real slot LSN");

    let success = Arc::new(AtomicUsize::new(0));
    let keyempty = Arc::new(AtomicUsize::new(0));
    let conflict = Arc::new(AtomicUsize::new(0));

    // Channels: deleter 1 announces it reached the probe (slot still L0), then
    // blocks until we tell it to resume.
    let (at_tx, at_rx) = channel::<Lsn>();
    let (go_tx, go_rx) = channel::<()>();

    // ---- Deleter 1: pauses in the publish window. ----
    let (d1_env, d1_db) = (env.clone(), db.clone());
    let (s1, ke1, cf1) = (success.clone(), keyempty.clone(), conflict.clone());
    let deleter1 = std::thread::spawn(move || {
        PUBLICATION_PROBE.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |old, new| {
                // We are AFTER the lock move, BEFORE apply_tree_delete.
                let _ = (old, new);
                at_tx.send(Lsn::from_u64(old)).unwrap();
                go_rx
                    .recv_timeout(Duration::from_secs(30))
                    .expect("deleter1 resume timed out");
            }))
        });
        let r = match mode {
            "txn" => {
                let txn = d1_env.begin_transaction(None).unwrap();
                let st = d1_db.delete_in(&txn, KEY);
                match st {
                    Ok(true) => {
                        txn.commit_with_durability(Durability::COMMIT_SYNC)
                            .unwrap();
                        Ok(true)
                    }
                    Ok(false) => {
                        txn.commit().unwrap();
                        Ok(false)
                    }
                    Err(e) => {
                        let _ = txn.abort();
                        Err(e)
                    }
                }
            }
            "auto" => d1_db.delete(KEY),
            _ => unreachable!(),
        };
        match r {
            Ok(true) => s1.fetch_add(1, Ordering::SeqCst),
            Ok(false) => ke1.fetch_add(1, Ordering::SeqCst),
            Err(_) => cf1.fetch_add(1, Ordering::SeqCst),
        };
    });

    // Wait until deleter 1 is parked in the publish window.
    let paused_old = at_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("deleter1 never reached PUBLICATION_PROBE");
    assert_eq!(paused_old, l0, "deleter1 locked and moved off L0");
    // Deleter 1 paused: DeleteLN logged, lock moved, slot STILL L0.
    assert_eq!(
        slot_lsn(),
        l0,
        "slot must still be L0 while deleter1 is parked"
    );

    // ---- Deleter 2: must NOT win a second success in this window. ----
    // On RC it blocks on WRITE(L0) (held by deleter1) and only wakes after we
    // resume+commit deleter1, then sees the slot gone -> KeyEmpty.  On BASE it
    // was granted the eagerly-released WRITE(L0) and wrongly succeeded.
    let (d2_env, d2_db) = (env.clone(), db.clone());
    let (s2, ke2, cf2) = (success.clone(), keyempty.clone(), conflict.clone());
    let deleter2 = std::thread::spawn(move || {
        // Make sure deleter2 does NOT install the probe hook.
        PUBLICATION_PROBE.with(|hook| *hook.borrow_mut() = None);
        let r = match mode {
            "txn" => {
                let txn = d2_env.begin_transaction(None).unwrap();
                match d2_db.delete_in(&txn, KEY) {
                    Ok(true) => {
                        txn.commit_with_durability(Durability::COMMIT_SYNC)
                            .unwrap();
                        Ok(true)
                    }
                    Ok(false) => {
                        txn.commit().unwrap();
                        Ok(false)
                    }
                    Err(e) => {
                        let _ = txn.abort();
                        Err(e)
                    }
                }
            }
            "auto" => d2_db.delete(KEY),
            _ => unreachable!(),
        };
        match r {
            Ok(true) => s2.fetch_add(1, Ordering::SeqCst),
            Ok(false) => ke2.fetch_add(1, Ordering::SeqCst),
            Err(_) => cf2.fetch_add(1, Ordering::SeqCst),
        };
    });

    // Give deleter 2 time to either block on WRITE(L0) (RC, correct) or race
    // through the still-L0 slot (BASE, the bug).  A bounded wait is enough:
    // the anomaly is a prompt second success, not a slow one.
    std::thread::sleep(Duration::from_millis(300));
    // Resume deleter 1; it runs apply_tree_delete + commit, releasing L0.
    go_tx.send(()).unwrap();

    deleter1.join().unwrap();
    deleter2.join().unwrap();

    let s = success.load(Ordering::SeqCst);
    let ke = keyempty.load(Ordering::SeqCst);
    let cf = conflict.load(Ordering::SeqCst);

    // The record must end up deleted exactly once.
    assert_eq!(
        db.get(KEY).unwrap(),
        None,
        "mode={mode}: record must be gone after the race"
    );
    // THE invariant NEW-DEL-RACE-1 violates at BASE: exactly one deleter wins.
    assert_eq!(
        s, 1,
        "mode={mode}: exactly one deleter must succeed; got success={s} \
         keyempty={ke} conflict={cf} (NEW-DEL-RACE-1: concurrent deletes of \
         one record not serialized -- a second deleter was granted the \
         eagerly-released WRITE(old) in the publish window)"
    );
    assert_eq!(
        ke + cf,
        1,
        "mode={mode}: the losing deleter must observe KeyEmpty or a conflict; \
         got keyempty={ke} conflict={cf}"
    );

    Arc::try_unwrap(db).ok().unwrap().close().unwrap();
    Arc::try_unwrap(env).ok().unwrap().close().unwrap();
}

/// Explicit-transaction deleters (the `move_write_lock_to_new_lsn` path fixed
/// by 8955a610). RED with that retention reverted, GREEN on RC.
#[test]
fn concurrent_deletes_of_one_record_serialize_txn() {
    run("txn");
}

/// Auto-commit deleters (the no-txn `finalize_write_lock` path fixed by
/// d3f9a6ed). RED with that retention reverted, GREEN on RC.
#[test]
fn concurrent_deletes_of_one_record_serialize_auto() {
    run("auto");
}
