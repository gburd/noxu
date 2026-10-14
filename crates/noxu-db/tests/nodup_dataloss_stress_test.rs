//! WAVE4 NoDupData DATA-LOSS (P0) — focused regression.
//!
//! Distilled from `stress_dup` (nodup_spurious_insert_test.rs). Pins the
//! `final=None` outcome: a committed `(k, v)` DESTROYED by transactions that
//! only ABORT.
//!
//! Mechanism (confirmed by bisect + code read on 2d719ed9 / 1c33cb86):
//!   * A writer txn runs `delete_in(k,v)` + `abort()` in a tight loop. Each
//!     delete flips the shared `k|v` slot to known-deleted (KD) at a fresh
//!     DeleteLN lsn and holds the write lock on THAT lsn; the abort restores
//!     the committed pair and releases that lsn.
//!   * A reader txn runs `cursor.put(k, v, NoDupData)` + `abort()` in a loop.
//!     In the transient window around the writer's abort (lock on the prior
//!     DeleteLN lsn already released, slot momentarily appearing absent/KD),
//!     the reader's common-insert recheck can observe the pair ABSENT and
//!     INSERT a spurious dup over the shared slot, recording
//!     `abort_known_deleted = true` (cursor_impl.rs finalize_write_lock:
//!     `abort_known_deleted = !old_exists`, with `old_exists=false` for a KD
//!     slot — the bug: reusing a KD slot owned by a live txn must record the
//!     slot's prior before-image, JE insertRecordInternal slot-reuse).
//!   * The reader's abort then does `tree.delete(k|v)` on the SHARED slot,
//!     destroying the committed pair the writer restored => final=None.
//!
//! Oracle: after the loop, the committed `(k, v)` MUST still be present. On
//! the defect it is intermittently gone (final=None) AND spurious inserts
//! leak (inserted > 0). This is the exact P0 the nodup fix was meant to close
//! but does not under contention.
//!
//! NOTE: probabilistic (race), but the window is hit within a few thousand
//! iterations under normal scheduling; kept bounded so it is a usable gate.
use noxu_db::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

#[test]
fn nodup_dup_abort_only_must_not_lose_committed_pair() {
    let d = TempDir::new().unwrap();
    let e = Arc::new(
        Environment::open(
            EnvironmentConfig::new(d.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true)
                .with_lock_timeout(5000),
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
                .with_sorted_duplicates(true),
        )
        .unwrap(),
    );
    b.put(b"k", b"v").unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let (e2, b2, s2) = (Arc::clone(&e), Arc::clone(&b), Arc::clone(&stop));
    let w = thread::spawn(move || {
        while !s2.load(Ordering::Relaxed) {
            let t = e2.begin_transaction(None).unwrap();
            let _ = b2.delete_in(&t, b"k");
            t.abort().unwrap();
        }
    });

    let (bad, lost_mid) = (AtomicUsize::new(0), AtomicUsize::new(0));
    for _ in 0..6000 {
        let t = e.begin_transaction(None).unwrap();
        let mut c = b.open_cursor_in(&t, None).unwrap();
        if let Ok(OperationStatus::Success) = c.put(
            &DatabaseEntry::from_bytes(b"k"),
            &DatabaseEntry::from_bytes(b"v"),
            Put::NoDupData,
        ) {
            bad.fetch_add(1, Ordering::Relaxed);
        }
        c.close().ok();
        let _ = t.abort();
        // Spot the loss mid-flight too (both txns only abort => must persist).
        if b.get(b"k").unwrap().is_none() {
            lost_mid.fetch_add(1, Ordering::Relaxed);
        }
    }
    stop.store(true, Ordering::Relaxed);
    w.join().unwrap();

    let final_v = b.get(b"k").unwrap();
    eprintln!(
        "DATALOSS PROBE: spurious_inserts={} mid_flight_None={} final={:?}",
        bad.load(Ordering::Relaxed),
        lost_mid.load(Ordering::Relaxed),
        final_v.as_deref().map(<[u8]>::to_vec)
    );
    assert_eq!(
        final_v.as_deref(),
        Some(b"v".as_slice()),
        "P0 DATA LOSS: committed (k,v) destroyed by abort-only transactions \
         (final=None)"
    );
    assert_eq!(
        bad.load(Ordering::Relaxed),
        0,
        "spurious NoDupData insert over a committed-present pair"
    );
}
