//! SLOT-RELOCK committed-data-loss (P0) — DETERMINISTIC 2-thread regression.
//!
//! Pins `final=None`: a committed `(k, v)` DESTROYED by two transactions that
//! only ABORT, via the stale-slot-LSN TOCTOU in the insert path.
//! `get_slot_before_image(k)` reads the slot LSN, then
//! `lock_write_before_log(old_lsn)` locks THAT LSN -- but if the reader WAITED
//! for that lock (a writer held it) and the writer's abort both released the
//! lock AND moved the slot to a different LSN, the reader is granted a STALE
//! lock that no longer protects the live slot. JE `CursorImpl.lockLN`
//! (CursorImpl.java:3641-3680) re-latches after the wait and, if the slot LSN
//! changed, reverts + re-locks the new LSN; the before-image (JE
//! `prepareForUpdate`: abortLsn, abortKD) then comes from the LOCKED current
//! slot. Noxu lacked that re-read-under-lock loop.
//!
//! Captured mechanism (instrumented f5349c3c, non-dup NoOverwrite):
//!   old_lsn=35079 old_exists=false  ->  (after lock) post_lsn=85
//!   post_exists=true (the committed original "v") -- the reader was granted a
//!   lock on the STALE 35079 while the live slot sat at 85.
//!
//! Deterministic schedule (2 threads + a post-grant hook; the only timing is a
//! one-way wait so the reader is parked on the lock before the writer aborts):
//!   0. Commit (k, v).  (For the dup variant: a dup DB and NoDupData.)
//!   1. Writer W txn1 deletes (k) UNCOMMITTED -> slot KD at d1, W holds d1.
//!   2. Reader R put_no_overwrite(k): the initial existence check sees the KD
//!      slot ABSENT, enters the New-insert block, reads old_lsn=d1, and BLOCKS
//!      in lock_write_before_log(d1).
//!   3. W (after a short pause so R is parked on d1) aborts txn1 -> restores
//!      (k, v) live at its original LSN, releases d1. R is GRANTED the STALE d1.
//!   4. [post-grant hook in R] R signals W; W txn2 deletes (k) UNCOMMITTED ->
//!      KD at d2 (succeeds: R holds only d1). W signals R; the hook returns.
//!   5. R's existence recheck sees KD -> INSERTS, recording abort_lsn=d1,
//!      abort_known_deleted=true (the bug). put returns a spurious Success.
//!   6. W aborts txn2 (restores (k, v)); R aborts -> abort_known_deleted=true
//!      -> tree.delete(k) on the SHARED slot -> committed (k, v) destroyed.
//!
//! ORACLE: the insert must be KeyExists, and the committed (k, v) must survive
//! both aborts, in process and after reopen. On the defect: Success + final=None.
use noxu_db::*;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

fn open(d: &TempDir, dups: bool) -> (Arc<Environment>, Arc<Database>) {
    let e = Arc::new(
        Environment::open(
            EnvironmentConfig::new(d.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true)
                .with_lock_timeout(30000),
        )
        .unwrap(),
    );
    let b = Arc::new(
        e.open_database(
            None,
            "x",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true)
                .with_sorted_duplicates(dups),
        )
        .unwrap(),
    );
    (e, b)
}

/// Shared driver for both the non-dup NoOverwrite and dup NoDupData variants.
fn run_schedule(dups: bool) {
    let d = TempDir::new().unwrap();
    let (e, b) = open(&d, dups);

    // Step 0: commit (k, v).
    b.put(b"k", b"v").unwrap();
    assert_eq!(b.get(b"k").unwrap().as_deref(), Some(b"v".as_slice()));

    // Writer thread. Commands: "del2" (txn2 delete, stay live), "abort2".
    // txn1 (the first delete + abort that strands the reader on d1) is driven
    // inline: the writer deletes, acks "d1", pauses, then aborts txn1.
    let (to_w, w_rx): (Sender<&'static str>, Receiver<&'static str>) =
        channel();
    let (d1_tx, d1_done): (Sender<()>, Receiver<()>) = channel();
    let (del2_tx, del2_done): (Sender<()>, Receiver<()>) = channel();
    let (abort2_tx, abort2_done): (Sender<()>, Receiver<()>) = channel();
    let (e_w, b_w) = (Arc::clone(&e), Arc::clone(&b));
    let writer = std::thread::spawn(move || {
        // txn1: delete (k) uncommitted -> slot KD at d1, hold WRITE(d1).
        let t1 = e_w.begin_transaction(None).unwrap();
        let _ = b_w.delete_in(&t1, b"k");
        d1_tx.send(()).unwrap();
        // Pause so the reader reaches the blocking WRITE(d1) request, then
        // abort txn1: restores (k, v) live, releases d1; reader granted stale d1.
        std::thread::sleep(Duration::from_millis(200));
        t1.abort().unwrap();

        // txn2: delete (k) uncommitted -> slot KD at d2 (reader holds d1 only).
        assert_eq!(w_rx.recv().unwrap(), "del2");
        let t2 = e_w.begin_transaction(None).unwrap();
        let _ = b_w.delete_in(&t2, b"k");
        del2_tx.send(()).unwrap();

        assert_eq!(w_rx.recv().unwrap(), "abort2");
        t2.abort().unwrap();
        abort2_tx.send(()).unwrap();
    });

    // Wait for txn1's delete so the reader enters with the slot KD at d1.
    d1_done.recv().unwrap();

    // Post-grant hook (fires on the reader thread AFTER it is granted the stale
    // d1 and BEFORE the existence recheck): drive the writer's txn2 delete.
    let to_w_hook = to_w.clone();
    noxu_dbi::cursor_impl::nodup_recheck_hook::arm_after_slot_locked(Box::new(
        move || {
            to_w_hook.send("del2").unwrap();
            del2_done.recv().unwrap();
        },
    ));

    // Reader: put_no_overwrite(k) (non-dup) or NoDupData(k, v) (dup). The
    // initial existence check sees the KD slot absent -> New-insert block ->
    // blocks on WRITE(d1) -> granted stale d1 -> hook -> recheck -> insert.
    let status = if dups {
        let rt = e.begin_transaction(None).unwrap();
        let mut c = b.open_cursor_in(&rt, None).unwrap();
        let s = c.put(
            &DatabaseEntry::from_bytes(b"k"),
            &DatabaseEntry::from_bytes(b"v"),
            Put::NoDupData,
        );
        c.close().ok();
        // Finish the writer handshake, then abort both (writer first).
        to_w.send("abort2").unwrap();
        abort2_done.recv().unwrap();
        writer.join().unwrap();
        let _ = rt.abort();
        s.map(|st| st == OperationStatus::Success)
    } else {
        let rt = e.begin_transaction(None).unwrap();
        let s = b.put_no_overwrite_in(&rt, b"k", b"x");
        to_w.send("abort2").unwrap();
        abort2_done.recv().unwrap();
        writer.join().unwrap();
        let _ = rt.abort();
        s
    };

    noxu_dbi::cursor_impl::nodup_recheck_hook::clear();

    eprintln!("SLOT-RELOCK dups={dups}: reader_inserted={status:?}");
    // Check the committed-data survival FIRST (the actual P0), then the
    // spurious-insert status, so a run that destroys data is reported as the
    // data-loss failure it is.
    let final_v = b.get(b"k").unwrap();
    assert_eq!(
        final_v.as_deref(),
        Some(b"v".as_slice()),
        "P0 DATA LOSS: committed (k,v) destroyed by abort-only transactions \
         (final=None) via the stale-lock slot-reuse TOCTOU"
    );
    assert_eq!(
        status.ok(),
        Some(false),
        "insert over a committed-present record (restored under a stale lock) \
         must be KeyExists, not a spurious Success"
    );

    drop(b);
    drop(e);
    let (_, b3) = open(&d, dups);
    assert_eq!(
        b3.get(b"k").unwrap().as_deref(),
        Some(b"v".as_slice()),
        "committed (k,v) lost after reopen"
    );
}

#[test]
fn slot_relock_nondup_nooverwrite_must_not_destroy_committed() {
    run_schedule(false);
}

// The dup NoDupData variant exercises the SAME `lock_slot_current_lsn` fix in
// the sorted-dup common insert path, but is not driven deterministically here:
// the dup NoDupData pre-lock check LOCKS a known-deleted slot before the
// common path (JE lockLNAndCheckDefunct), so the reader does not reach the
// common-path lock in the WAITING state this 2-thread schedule relies on (it
// is granted in the pre-lock check and returns KeyExist). The dup path's
// residual spurious insert is covered by the probabilistic
// `nodup_dataloss_stress_test` + `nodup_spurious_insert_test::stress_dup`
// under load; the fix is the same helper. Kept ignored to document the intent.
#[test]
#[ignore = "dup pre-lock locks the KD slot; not WAIT-driven deterministically"]
fn slot_relock_dup_nodupdata_must_not_destroy_committed() {
    run_schedule(true);
}
