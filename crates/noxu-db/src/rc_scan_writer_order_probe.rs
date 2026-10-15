//! BUG-RC-SCAN-TORN-READ deterministic witnesses, driven by the writer-side
//! `SLOT_PUBLISHED_PROBE` seam (`cfg(feature = "testing")`, compiled out of
//! default builds).  The seam fires on the writer thread immediately AFTER the
//! tree slot update publishes `new_lsn` into the BIN slot, while the writer's
//! transaction still holds WRITE(`new_lsn`) (held until commit/abort).
//!
//! Two tests:
//!
//! 1. `writer_holds_write_new_once_slot_shows_it` — the invariant guard
//!    (coordinator's "writer-order witness").  Once the slot shows `new_lsn`,
//!    WRITE(`new_lsn`) MUST be held by the writer.  This is GREEN at the base
//!    (C2 A23 already acquires WRITE(new) before `apply_tree_insert`); it is a
//!    regression tripwire for any future reordering that would publish the slot
//!    before taking the new-LSN lock.
//!
//! 2. `rc_reader_never_sees_published_uncommitted_slot` — the deterministic
//!    torn-read witness (coordinator's "livelock witness", primary red/green).
//!    With the writer paused at the seam (slot shows the uncommitted `new_lsn`,
//!    latch released, WRITE(new) held), a READ_COMMITTED reader scans the slot
//!    on another thread.  It MUST NOT return the uncommitted value: holding
//!    WRITE(new), the reader's record-lock re-read blocks until the writer
//!    resolves.  We release the reader by ABORTING the writer, after which the
//!    slot is restored to the committed value, so the reader returns committed
//!    bytes (or a lock error) — never the uncommitted bytes.  RED at d3f9a6ed
//!    in BOTH profiles (the pre-fix re-read returns the published uncommitted
//!    bytes without holding the lock); GREEN with the fix.
//!
//! JE: `CursorImpl.lockLN` takes the record lock and re-reads the slot under
//! the BIN latch; it never returns bytes read without the lock held
//! (CursorImpl.java:3639-3680).  `Locker.allowReleaseLockAfterLsnChange()` is
//! false (Locker.java:775): the writer keeps WRITE(new) until commit/abort.

use super::*;
use crate::{
    DatabaseConfig, DatabaseEntry, OperationStatus, TransactionConfig,
};
use noxu_dbi::cursor_impl::SLOT_PUBLISHED_PROBE;
use noxu_util::Lsn;
use std::sync::mpsc::channel;
use std::time::Duration;

const KEY: &[u8] = b"K00";

/// Open a transactional env+db, seed `KEY` with the committed value `Avalue`,
/// and return the pieces the witnesses need.
fn setup() -> (
    tempfile::TempDir,
    Arc<Environment>,
    Arc<crate::Database>,
    Arc<noxu_txn::LockManager>,
) {
    let temp = tempfile::TempDir::new().unwrap();
    let mut cfg = EnvironmentConfig::new(temp.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    cfg.set_run_evictor(false);
    cfg.set_run_in_compressor(false);
    let env = Arc::new(Environment::open(cfg).unwrap());
    let db = Arc::new(
        env.open_database(
            None,
            "rcscan",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap(),
    );
    db.put(KEY, b"Avalue").unwrap();
    let lm = {
        let inner = env.env_impl.lock();
        Arc::clone(inner.get_lock_manager())
    };
    (temp, env, db, lm)
}

/// INVARIANT GUARD (writer-order witness): once the slot shows `new_lsn`,
/// WRITE(`new_lsn`) is held by the writer.  GREEN at the base and with the fix.
#[test]
fn writer_holds_write_new_once_slot_shows_it() {
    let (_temp, env, db, lm) = setup();

    let (at_tx, at_rx) = channel::<(u64, u64)>();
    let (go_tx, go_rx) = channel::<()>();
    let (wenv, wdb) = (env.clone(), db.clone());
    let writer = std::thread::spawn(move || {
        SLOT_PUBLISHED_PROBE.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |old, new: Lsn| {
                at_tx.send((old, new.as_u64())).unwrap();
                // Pause the writer here (slot published, WRITE(new) held) until
                // the test has made its assertion.
                go_rx
                    .recv_timeout(Duration::from_secs(20))
                    .expect("writer resume timed out");
            }))
        });
        let txn = wenv.begin_transaction(None).unwrap();
        wdb.put_in(&txn, KEY, b"Bvalue").unwrap();
        // Abort so the uncommitted write never commits (matches the torn-read
        // witness; the invariant we assert holds regardless of outcome).
        txn.abort().unwrap();
    });

    let (_old, new_lsn) = at_rx
        .recv_timeout(Duration::from_secs(20))
        .expect("writer never reached the slot-published seam");

    // The slot now shows new_lsn and the writer is paused.  WRITE(new_lsn) MUST
    // be held: a foreign reader probing it must see a write owner.  i64::MIN is
    // a locker id no real cursor/txn uses, so `probe_read_uncontended` returns
    // false iff a foreign WRITE owner holds new_lsn.
    let uncontended = lm.probe_read_uncontended(new_lsn, i64::MIN);
    assert!(
        !uncontended,
        "INVARIANT VIOLATED: slot shows new_lsn={new_lsn} but WRITE(new_lsn) \
         is NOT held (probe_read_uncontended returned true). The writer \
         published the slot before (or without) holding the new-LSN write \
         lock — the window that lets an RC reader see uncommitted bytes."
    );
    let (n_owners, _n_waiters) = lm.get_lock_info(new_lsn);
    assert!(
        n_owners >= 1,
        "slot shows new_lsn={new_lsn} but the lock table has no owner for it"
    );

    go_tx.send(()).unwrap();
    writer.join().unwrap();
    // Aborted: committed value restored.
    assert_eq!(db.get(KEY).unwrap().unwrap().as_ref(), b"Avalue");
}

/// TORN-READ WITNESS (primary red/green): a READ_COMMITTED reader must never
/// return the writer's published-but-uncommitted slot bytes.  RED at d3f9a6ed
/// in BOTH profiles; GREEN with the fix.
#[test]
fn rc_reader_never_sees_published_uncommitted_slot() {
    let (_temp, env, db, _lm) = setup();

    let (at_tx, at_rx) = channel::<u64>();
    let (go_tx, go_rx) = channel::<()>();
    let (wenv, wdb) = (env.clone(), db.clone());
    let writer = std::thread::spawn(move || {
        SLOT_PUBLISHED_PROBE.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |_old, new: Lsn| {
                // Slot now shows the uncommitted new_lsn (Bvalue), latch
                // released, WRITE(new) held.  Tell the test, then wait for it
                // to position the reader before we abort.
                at_tx.send(new.as_u64()).unwrap();
                go_rx
                    .recv_timeout(Duration::from_secs(20))
                    .expect("writer resume timed out");
            }))
        });
        let txn = wenv.begin_transaction(None).unwrap();
        wdb.put_in(&txn, KEY, b"Bvalue").unwrap();
        // Resolve (abort) only AFTER the test signals; the uncommitted Bvalue
        // is never committed.
        txn.abort().unwrap();
    });

    let _new_lsn = at_rx
        .recv_timeout(Duration::from_secs(20))
        .expect("writer never reached the slot-published seam");

    // Reader thread: a READ_COMMITTED scan positioned on the first record.
    // At the base this returns the published uncommitted Bvalue immediately
    // (dirty re-read / unlocked fallback). With the fix it blocks on WRITE(new)
    // until the writer below aborts, then returns the restored committed value.
    let (renv, rdb) = (env.clone(), db.clone());
    let (res_tx, res_rx) = channel::<Vec<u8>>();
    let reader = std::thread::spawn(move || {
        let rc = TransactionConfig::new().with_read_committed(true);
        let txn = renv.begin_transaction(Some(&rc)).unwrap();
        let mut cur = rdb.open_cursor_in(&txn, None).unwrap();
        let mut k = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        let got = match cur.get(&mut k, &mut d, crate::Get::First, None) {
            Ok(OperationStatus::Success) => {
                d.data_opt().map(|v| v.to_vec()).unwrap_or_default()
            }
            // A lock-timeout / conflict is acceptable: the reader waited out a
            // perpetually-locked slot and returned an ERROR, not dirty data.
            // Report a sentinel so the assertion below treats it as "not torn".
            _ => b"<lock-error-or-notfound>".to_vec(),
        };
        drop(cur);
        let _ = txn.commit();
        res_tx.send(got).unwrap();
    });

    // Give the reader a moment to reach (and, with the fix, block on) the
    // record lock, then release the writer so it aborts and restores Avalue.
    std::thread::sleep(Duration::from_millis(300));
    go_tx.send(()).unwrap();

    let got = res_rx
        .recv_timeout(Duration::from_secs(30))
        .expect("reader did not finish");
    reader.join().unwrap();
    writer.join().unwrap();

    assert_ne!(
        got.as_slice(),
        b"Bvalue",
        "RC reader returned the writer's PUBLISHED-but-UNCOMMITTED value \
         (torn read). It read the slot's new_lsn bytes without holding the \
         record lock through the re-read."
    );
    // Final committed state is the restored Avalue.
    assert_eq!(db.get(KEY).unwrap().unwrap().as_ref(), b"Avalue");
}

/// DEDICATED LIVELOCK WITNESS (primary red/green, deterministic in BOTH
/// profiles via the `RELOCK_TEST_CAP` knob).
///
/// An abort-cycling writer keeps `KEY` PERPETUALLY write-locked: it repeatedly
/// writes an uncommitted `Bvalue` (a fresh LSN, WRITE held) and aborts, so the
/// slot is at every instant either the committed `Avalue` or an uncommitted
/// `Bvalue` whose WRITE lock is held.  A READ_COMMITTED reader whose record
/// re-read loop is capped (simulating a slot that moves faster than the loop
/// converges) MUST NOT return the uncommitted `Bvalue`.
///
/// At the pre-fix base the capped loop exhausts into the UNLOCKED fallback and
/// returns the live (uncommitted) slot bytes — RED.  With the fix the capped
/// loop returns a `LockConflict` ERROR (never dirty data); the reader surfaces
/// a lock error rather than `Bvalue` — GREEN.  The cap makes the exhaustion
/// deterministic in DEBUG as well as RELEASE.
#[test]
fn rc_scan_livelock_never_returns_uncommitted_under_capped_relock() {
    use noxu_dbi::cursor_impl::RELOCK_TEST_CAP;

    let (_temp, env, db, _lm) = setup();

    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let torn = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Abort-cycling writer: keep KEY perpetually write-locked with uncommitted
    // Bvalue, never committing it.
    let (wenv, wdb, wdone) = (env.clone(), db.clone(), done.clone());
    let writer = std::thread::spawn(move || {
        while !wdone.load(std::sync::atomic::Ordering::Relaxed) {
            let Ok(txn) = wenv.begin_transaction(None) else { continue };
            let _ = wdb.put_in(&txn, KEY, b"Bvalue");
            let _ = txn.abort();
        }
    });

    // Reader: READ_COMMITTED scan with a low re-read cap, so the loop's
    // termination path (fallback at base / lock-error at fix) fires fast and
    // deterministically.  Run many rounds so a RED base is caught reliably.
    let rc = TransactionConfig::new().with_read_committed(true);
    for _ in 0..2000 {
        RELOCK_TEST_CAP.with(|c| c.set(4));
        let Ok(txn) = env.begin_transaction(Some(&rc)) else { continue };
        if let Ok(mut cur) = db.open_cursor_in(&txn, None) {
            let mut k = DatabaseEntry::new();
            let mut d = DatabaseEntry::new();
            if let Ok(OperationStatus::Success) =
                cur.get(&mut k, &mut d, crate::Get::First, None)
                && d.data_opt() == Some(b"Bvalue".as_slice())
            {
                torn.store(true, std::sync::atomic::Ordering::Relaxed);
            }
            drop(cur);
        }
        let _ = txn.commit();
        RELOCK_TEST_CAP.with(|c| c.set(0));
        if torn.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }
    }

    done.store(true, std::sync::atomic::Ordering::Relaxed);
    writer.join().unwrap();

    assert!(
        !torn.load(std::sync::atomic::Ordering::Relaxed),
        "RC scan returned the writer's UNCOMMITTED value under a capped re-read \
         loop: the loop fell back to an unlocked read instead of surfacing a \
         lock-conflict error (BUG-RC-SCAN-TORN-READ livelock fallback)."
    );
}
