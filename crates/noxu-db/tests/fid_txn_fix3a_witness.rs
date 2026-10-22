// Copyright (C) 2024-2026 Greg Burd.  Apache-2.0 OR MIT.
//! FID-TXN-FIX3A — write locks must not be released before the commit record
//! reaches the durability the txn's policy requires (JE `Txn.commit` ordering).
//!
//! JE releases a committing txn's write locks only AFTER `logCommitEntry`
//! (which, for SYNC, fsyncs inside `LogManager.log`) returns, then
//! `setDeletedDatabaseState(true)`, then `releaseWriteLocks()`
//! (txn/Txn.java:766-796).  So no other locker can acquire a just-released
//! record lock and observe a committed value before that value is durable.
//!
//! Noxu's "Fix 3a" (commit 3560de1b) deliberately released the inner `Txn`'s
//! write locks BETWEEN the WAL append (`commit_append_phase`) and the fsync
//! (`commit_durable_phase`).  On the commit paths where the INNER `Txn` owns
//! the fsync (non-transactional-db auto-commit; XA commit; a directly-driven
//! `Txn`), a concurrent read-committed reader could acquire the just-released
//! lock and read the new value while that value was still only in the WAL
//! buffer (not fsynced).  If the fsync then failed / power was lost, the
//! reader had already observed (and possibly externalised) a value whose
//! commit never became durable — the early-lock-release (ELR) isolation hole.
//!
//! These witnesses are DETERMINISTIC.  They arm `fsync_probe` with a delay so
//! the committer's fdatasync blocks for a known window, race a read-committed
//! reader into that window, and record whether the reader observed the new
//! value while the fsync had NOT yet completed (`fsync_probe::COMPLETED` still
//! 0 for the committer's sync).  That observation is the violation:
//!
//!   * RED  on current (Fix-3a) code: the inner lock is released pre-fsync, so
//!     the reader reads the non-durable value during the window.
//!   * GREEN after the JE-ordering fix: the write lock is held across the
//!     durable barrier, so the reader blocks until the fsync completes and can
//!     only ever read a durable value (or time out).
//!
//! W1 (crash-before-durable) and W2 (fsync-EIO) add the CONSEQUENCE leg: after
//! the window, the fsync is made to fail (W2, in-process EIO via
//! `fsync_probe::fail_next`) or the un-synced bytes are dropped by a power cut
//! (W1, a `faultdisk` sub-process crash + recovery) — proving the observed
//! value was in fact non-durable.  W3 is the control: a SERIALIZABLE reader,
//! and the fixed (JE-ordered) build, must never observe the non-durable value.
//!
//! One witness per public commit path (regression guards for every path, even
//! those that are currently JE-ordered only via an outer-layer accident):
//!   * `explicit_txn_sync`                 — explicit public txn, SYNC
//!   * `transactional_db_auto_commit`      — auto-commit on a transactional db
//!   * `non_transactional_db_auto_commit`  — auto-commit on a NON-txn db (RED)
//!   * `cursor_auto`                        — open_cursor(None) writable cursor
//!   * (XA commit witness: see `fid_txn_fix3a_xa_witness.rs` in noxu-xa)
//!
//! The isolation observation is the shared RED/GREEN signal; the crash/EIO
//! legs confirm non-durability.
#![cfg(all(unix, not(noxu_shuttle)))]

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Durability, Environment, EnvironmentConfig,
    LockMode, ReadOptions,
};
use noxu_log::file_handle::fsync_probe;
use noxu_sync::Mutex;
use tempfile::TempDir;

const V1: &[u8] = b"v1-durable-base-00000000";
const V2: &[u8] = b"v2-committed-not-durable";

/// All witnesses here drive the PROCESS-GLOBAL `fsync_probe` (arm / fail_next /
/// COMPLETED), so they must not run concurrently within this test binary (the
/// default `cargo test` runs tests in parallel threads of one process).  Every
/// test takes this lock for its whole body.  (Separate test binaries are
/// separate processes and do not share the probe, so one lock per file is
/// enough.)
static PROBE_SERIAL: Mutex<()> = Mutex::new(());

/// Microseconds the committer's fdatasync is held open (the observation
/// window).  Must be < the reader's lock timeout so the FIXED build makes the
/// reader BLOCK-then-succeed rather than time out, and comfortably larger than
/// the reader's scheduling latency so the race is deterministic.
const FSYNC_HOLD_US: u64 = 300_000; // 300 ms

/// Reader lock timeout: large so under the fix the reader blocks for the whole
/// window and still succeeds (reads the now-durable value) rather than
/// surfacing a LockTimeout we would have to special-case.
const READER_LOCK_TIMEOUT_MS: u64 = 5_000;

fn env_cfg(p: &Path, transactional: bool) -> EnvironmentConfig {
    let mut c = EnvironmentConfig::new(p.to_path_buf())
        .with_allow_create(true)
        .with_transactional(transactional)
        .with_durability(Durability::COMMIT_SYNC)
        .with_lock_timeout(READER_LOCK_TIMEOUT_MS);
    c.set_run_cleaner(false);
    c.set_run_checkpointer(false);
    c.set_run_in_compressor(false);
    c
}

fn db_cfg(transactional: bool) -> DatabaseConfig {
    DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(transactional)
        .with_sorted_duplicates(false)
}

/// Shared outcome of one window race.
struct Outcome {
    /// The reader observed V2 while the committer's fsync had NOT completed.
    saw_nondurable_v2: bool,
    /// The reader observed V2 at all (durable or not).
    saw_v2: bool,
    /// The reader observed V1 (the pre-commit base).
    saw_v1: bool,
    /// `fsync_probe::COMPLETED` at the moment the reader took its sample.
    completed_at_read: u64,
}

/// Drive one committer + one reader through the fsync window.
///
/// `commit_v2` performs the SYNC commit of `k -> V2` over a committed `k ->
/// V1`; it runs on the committer thread and must block inside the probed
/// fdatasync for ~`FSYNC_HOLD_US`.  `read_k` performs the reader's isolated
/// read of `k` and returns the observed value (or `None`).
fn race_window<CF, RF>(
    reader_isolation: LockMode,
    commit_v2: CF,
    read_k: RF,
) -> Outcome
where
    CF: FnOnce() + Send + 'static,
    RF: Fn() -> Option<Vec<u8>> + Send + 'static,
{
    let committer_started = Arc::new(AtomicBool::new(false));
    let committer_done = Arc::new(AtomicBool::new(false));
    let _ = reader_isolation; // isolation is baked into read_k by the caller

    fsync_probe::arm(FSYNC_HOLD_US);

    let cs = Arc::clone(&committer_started);
    let cd = Arc::clone(&committer_done);
    let committer = std::thread::spawn(move || {
        cs.store(true, Ordering::SeqCst);
        commit_v2();
        cd.store(true, Ordering::SeqCst);
    });

    // Wait until the committer has entered commit (it set `started`), then let
    // it reach the blocked fdatasync.  The probe guarantees the fsync holds
    // for FSYNC_HOLD_US; we sample the reader during the first half of that
    // window so the committer's fsync is demonstrably still in flight.
    while !committer_started.load(Ordering::SeqCst) {
        std::thread::yield_now();
    }
    // Give the committer time to append + reach the fdatasync sleep.
    std::thread::sleep(Duration::from_millis(40));

    let completed_before = fsync_probe::COMPLETED.load(Ordering::SeqCst);
    let observed = read_k();
    let completed_at_read = fsync_probe::COMPLETED.load(Ordering::SeqCst);

    committer.join().unwrap();
    assert!(committer_done.load(Ordering::SeqCst));
    fsync_probe::disarm();

    let saw_v2 = observed.as_deref() == Some(V2);
    let saw_v1 = observed.as_deref() == Some(V1);
    // Non-durable iff the reader saw V2 AND the committer's fsync had not
    // completed when the read was taken (COMPLETED did not advance during the
    // read).  `completed_before == completed_at_read == 0` on the RED path:
    // the reader read before any fsync finished.
    let saw_nondurable_v2 =
        saw_v2 && completed_before == 0 && completed_at_read == 0;

    Outcome { saw_nondurable_v2, saw_v2, saw_v1, completed_at_read }
}

// ───────────────────── Path: transactional-db auto-commit (primary) ────────
// The INNER auto-`Txn` owns the fsync (PendingSync::Commit/Ln) on this path,
// so the Fix-3a early release is directly observable.  JE analogue: a
// transactional-db auto-commit is `Txn.createAutoTxn` — a real txn — which
// fsyncs for SYNC.  (The *non-transactional*-db path was the original RED
// witness, but after FID-NONTXN-SYNC-FSYNC a non-txn write has NO commit-time
// fsync at all — JE's ThreadLocker releases the lock at operation end with no
// durability barrier to hold across — so there is nothing left for Fix-3a to
// order there.  The transactional-db auto-commit path is where the inner Txn
// still owns the SYNC fsync, so that is where this guard now lives.)

#[test]
fn w_isolation_transactional_db_auto_commit_primary_read_committed() {
    let _serial = PROBE_SERIAL.lock();
    let dir = TempDir::new().unwrap();
    let env = Arc::new(Environment::open(env_cfg(dir.path(), true)).unwrap());
    let db = Arc::new(env.open_database(None, "nt", &db_cfg(true)).unwrap());
    // Base: k -> V1, durably.
    db.put(b"k", V1).unwrap();
    // Warm the fsync path so file-creation flushes don't confuse COMPLETED.
    db.put(b"warm", V1).unwrap();

    let db_c = Arc::clone(&db);
    let db_r = Arc::clone(&db);
    let out = race_window(
        LockMode::ReadCommitted,
        move || {
            // Auto-commit SYNC put of V2 over V1.  Inner auto-txn owns fsync.
            db_c.put(b"k", V2).unwrap();
        },
        move || {
            let opts =
                ReadOptions::new().with_lock_mode(LockMode::ReadCommitted);
            db_r.get_with_options(None, b"k", &opts)
                .ok()
                .flatten()
                .map(|b| b.to_vec())
        },
    );

    eprintln!(
        "FIX3A non-txn-db auto-commit RC: saw_v1={} saw_v2={} \
         saw_nondurable_v2={} completed_at_read={}",
        out.saw_v1, out.saw_v2, out.saw_nondurable_v2, out.completed_at_read,
    );
    db.close().unwrap();
    Arc::try_unwrap(env).ok().unwrap().close().unwrap();

    assert!(
        !out.saw_nondurable_v2,
        "FID-TXN-FIX3A RED (non-txn-db auto-commit): a read-committed reader \
         observed the SYNC-committed V2 while the committer's fsync had not \
         completed (non-durable).  JE holds the write lock across the commit \
         durability barrier so this is impossible."
    );
}

// ───────────────────────── Path: explicit public txn, SYNC ─────────────────
// Currently JE-ordered only because the OUTER Transaction's write_txn_end()
// fsyncs before inner.commit_with_durability() releases the inner locks.  This
// witness is a permanent guard so a future refactor cannot silently reopen it.

#[test]
fn w_isolation_explicit_txn_sync_read_committed() {
    let _serial = PROBE_SERIAL.lock();
    let dir = TempDir::new().unwrap();
    let env = Arc::new(Environment::open(env_cfg(dir.path(), true)).unwrap());
    let db = Arc::new(Mutex::new(
        env.open_database(None, "ex", &db_cfg(true)).unwrap(),
    ));
    {
        let g = db.lock();
        let t = env.begin_transaction(None).unwrap();
        g.put_in(&t, b"k", V1).unwrap();
        t.commit().unwrap();
        let t = env.begin_transaction(None).unwrap();
        g.put_in(&t, b"warm", V1).unwrap();
        t.commit().unwrap();
    }

    let env_c = Arc::clone(&env);
    let db_c = Arc::clone(&db);
    let env_r = Arc::clone(&env);
    let db_r = Arc::clone(&db);
    let out = race_window(
        LockMode::ReadCommitted,
        move || {
            let t = env_c.begin_transaction(None).unwrap();
            {
                let g = db_c.lock();
                g.put_in(&t, b"k", V2).unwrap();
            }
            t.commit_with_durability(Durability::COMMIT_SYNC).unwrap();
        },
        move || {
            use noxu_db::transaction_config::TransactionConfig;
            let mut cfg = TransactionConfig::new();
            cfg.set_read_committed(true);
            let t = env_r.begin_transaction(Some(&cfg)).unwrap();
            let opts =
                ReadOptions::new().with_lock_mode(LockMode::ReadCommitted);
            let v = {
                let g = db_r.lock();
                g.get_with_options(Some(&t), b"k", &opts).ok().flatten()
            };
            let _ = t.commit();
            v.map(|b| b.to_vec())
        },
    );

    eprintln!(
        "FIX3A explicit-txn SYNC RC: saw_v1={} saw_v2={} saw_nondurable_v2={} \
         completed_at_read={}",
        out.saw_v1, out.saw_v2, out.saw_nondurable_v2, out.completed_at_read,
    );
    db.lock().close().unwrap();
    Arc::try_unwrap(env).ok().unwrap().close().unwrap();

    assert!(
        !out.saw_nondurable_v2,
        "FID-TXN-FIX3A RED (explicit txn SYNC): a read-committed reader \
         observed the SYNC-committed V2 before its fsync completed."
    );
}

// ───────────────────────── Path: transactional-db auto-commit ──────────────

#[test]
fn w_isolation_transactional_db_auto_commit_read_committed() {
    let _serial = PROBE_SERIAL.lock();
    let dir = TempDir::new().unwrap();
    let env = Arc::new(Environment::open(env_cfg(dir.path(), true)).unwrap());
    let db = Arc::new(env.open_database(None, "ta", &db_cfg(true)).unwrap());
    db.put(b"k", V1).unwrap();
    db.put(b"warm", V1).unwrap();

    let db_c = Arc::clone(&db);
    let db_r = Arc::clone(&db);
    let out = race_window(
        LockMode::ReadCommitted,
        move || {
            db_c.put(b"k", V2).unwrap();
        },
        move || {
            let opts =
                ReadOptions::new().with_lock_mode(LockMode::ReadCommitted);
            db_r.get_with_options(None, b"k", &opts)
                .ok()
                .flatten()
                .map(|b| b.to_vec())
        },
    );

    eprintln!(
        "FIX3A txn-db auto-commit RC: saw_v1={} saw_v2={} \
         saw_nondurable_v2={} completed_at_read={}",
        out.saw_v1, out.saw_v2, out.saw_nondurable_v2, out.completed_at_read,
    );
    db.close().unwrap();
    Arc::try_unwrap(env).ok().unwrap().close().unwrap();

    assert!(
        !out.saw_nondurable_v2,
        "FID-TXN-FIX3A RED (transactional-db auto-commit): a read-committed \
         reader observed the SYNC-committed V2 before its fsync completed."
    );
}

// ───────────────────────── Path: cursor auto (open_cursor(None)) ───────────

#[test]
fn w_isolation_cursor_txn_read_committed() {
    let _serial = PROBE_SERIAL.lock();
    use noxu_db::Put;
    // Cursor write under an EXPLICIT transaction on a transactional db.  After
    // FID-CURSOR-AUTOCOMMIT a cursor opened with no txn on a transactional db
    // refuses updates (JE Cursor.updateOperationsProhibited), and after
    // FID-NONTXN-SYNC-FSYNC a non-txn-db cursor write has no commit-time fsync
    // at all (JE ThreadLocker) — so the only JE-legal cursor-write path that
    // still crosses a SYNC durability barrier is an explicit-txn cursor.  That
    // is what this guard exercises: the txn's write lock must be held across
    // the commit's SYNC fsync (Fix-3a ordering).
    let dir = TempDir::new().unwrap();
    let env = Arc::new(Environment::open(env_cfg(dir.path(), true)).unwrap());
    let db = Arc::new(Mutex::new(
        env.open_database(None, "cu", &db_cfg(true)).unwrap(),
    ));
    {
        let g = db.lock();
        let t = env.begin_transaction(None).unwrap();
        g.put_in(&t, b"k", V1).unwrap();
        t.commit().unwrap();
        let t = env.begin_transaction(None).unwrap();
        g.put_in(&t, b"warm", V1).unwrap();
        t.commit().unwrap();
    }

    let env_c = Arc::clone(&env);
    let db_c = Arc::clone(&db);
    let env_r = Arc::clone(&env);
    let db_r = Arc::clone(&db);
    let out = race_window(
        LockMode::ReadCommitted,
        move || {
            let t = env_c.begin_transaction(None).unwrap();
            {
                let g = db_c.lock();
                let mut c = g.open_cursor_in(&t, None).unwrap();
                let k = DatabaseEntry::from_bytes(b"k");
                let v = DatabaseEntry::from_bytes(V2);
                c.put(&k, &v, Put::Overwrite).unwrap();
                c.close().unwrap();
            }
            t.commit_with_durability(Durability::COMMIT_SYNC).unwrap();
        },
        move || {
            use noxu_db::transaction_config::TransactionConfig;
            let mut cfg = TransactionConfig::new();
            cfg.set_read_committed(true);
            let t = env_r.begin_transaction(Some(&cfg)).unwrap();
            let opts =
                ReadOptions::new().with_lock_mode(LockMode::ReadCommitted);
            let v = {
                let g = db_r.lock();
                g.get_with_options(Some(&t), b"k", &opts).ok().flatten()
            };
            let _ = t.commit();
            v.map(|b| b.to_vec())
        },
    );

    eprintln!(
        "FIX3A cursor-txn RC: saw_v1={} saw_v2={} saw_nondurable_v2={} \
         completed_at_read={}",
        out.saw_v1, out.saw_v2, out.saw_nondurable_v2, out.completed_at_read,
    );
    db.lock().close().unwrap();
    Arc::try_unwrap(env).ok().unwrap().close().unwrap();

    assert!(
        !out.saw_nondurable_v2,
        "FID-TXN-FIX3A RED (cursor under txn): a read-committed reader observed \
         the SYNC-committed V2 before its fsync completed."
    );
}

// ───────────────────────── W2: fsync-EIO consequence ────────────────────────
// Same window on the primary RED path, but the held fsync FAILS (in-process
// EIO via fsync_probe::fail_next).  The committer's commit must return Err and
// the environment must be invalidated.  Under the fix the reader could not
// have observed V2 (lock held); the commit fails and the env is invalid, and
// the base value V1 is what any later (fresh-env) reader sees.

#[test]
fn w2_fsync_eio_invalidates_and_no_nondurable_read() {
    let _serial = PROBE_SERIAL.lock();
    // TRANSACTIONAL db: the inner auto-`Txn` owns the SYNC fsync (JE
    // Txn.createAutoTxn).  After FID-NONTXN-SYNC-FSYNC the non-txn path has no
    // fsync to fail, so this guard moved to the transactional-db auto-commit
    // path (the surviving path where a SYNC fsync can be made to EIO).
    let dir = TempDir::new().unwrap();
    let env = Arc::new(Environment::open(env_cfg(dir.path(), true)).unwrap());
    let db = Arc::new(env.open_database(None, "nt", &db_cfg(true)).unwrap());
    db.put(b"k", V1).unwrap();
    db.put(b"warm", V1).unwrap();
    // Make V1 durable BEFORE arming the fsync failure, so the only fsync we
    // fail is V2's and recovery always has a clean durable base.
    env.checkpoint(Some(&noxu_db::CheckpointConfig::new().with_force(true)))
        .unwrap();

    // Arm the probe with a window AND a one-shot fsync failure.
    fsync_probe::arm(FSYNC_HOLD_US);
    fsync_probe::fail_next();

    let started = Arc::new(AtomicBool::new(false));
    let commit_err = Arc::new(AtomicU64::new(0)); // 1 = commit returned Err

    let db_c = Arc::clone(&db);
    let s = Arc::clone(&started);
    let ce = Arc::clone(&commit_err);
    let committer = std::thread::spawn(move || {
        s.store(true, Ordering::SeqCst);
        match db_c.put(b"k", V2) {
            Ok(()) => {}
            Err(_) => {
                ce.store(1, Ordering::SeqCst);
            }
        }
    });

    while !started.load(Ordering::SeqCst) {
        std::thread::yield_now();
    }
    std::thread::sleep(Duration::from_millis(40));

    // Reader races the window.  Under the fix the reader blocks on the held
    // write lock; when the fsync FAILS the commit aborts and releases the lock,
    // so the reader then sees V1 (the committed base), never V2.
    let opts = ReadOptions::new().with_lock_mode(LockMode::ReadCommitted);
    let completed_before = fsync_probe::COMPLETED.load(Ordering::SeqCst);
    let observed = db
        .get_with_options(None, b"k", &opts)
        .ok()
        .flatten()
        .map(|b| b.to_vec());
    let completed_at_read = fsync_probe::COMPLETED.load(Ordering::SeqCst);

    committer.join().unwrap();
    fsync_probe::disarm();

    let saw_v2 = observed.as_deref() == Some(V2);
    let saw_nondurable_v2 =
        saw_v2 && completed_before == 0 && completed_at_read == 0;

    eprintln!(
        "FIX3A W2 fsync-EIO: observed={:?} commit_err={} saw_nondurable_v2={}",
        observed.as_deref().map(|b| String::from_utf8_lossy(b).into_owned()),
        commit_err.load(Ordering::SeqCst),
        saw_nondurable_v2,
    );

    // The commit must have failed (fsync EIO).
    assert_eq!(
        commit_err.load(Ordering::SeqCst),
        1,
        "W2: the SYNC commit must return Err when its fsync fails (EIO)"
    );
    // No reader may have observed the non-durable V2.
    assert!(
        !saw_nondurable_v2,
        "FID-TXN-FIX3A W2 RED: a read-committed reader observed V2 whose \
         commit fsync then FAILED (EIO) — a value that never became durable."
    );

    // The committed-at-fail-fsync V2 is not durable.  Close the invalidated
    // env, then a FRESH open recovers; report what it sees (same-kernel note
    // as W1: a page-cache-resident pwrite may survive a process reopen even
    // though a real power loss would drop it, so this is informational, not a
    // hard assertion — the hard signals are the isolation observation and the
    // commit error above).
    fsync_probe::disarm();
    let _ = db.close();
    if let Ok(env_owned) = Arc::try_unwrap(env) {
        let _ = env_owned.close();
    }
    match Environment::open(env_cfg(dir.path(), true)) {
        Ok(env2) => {
            if let Ok(db2) = env2.open_database(None, "nt", &db_cfg(true)) {
                let after = db2.get(b"k").ok().flatten().map(|b| b.to_vec());
                eprintln!(
                    "FIX3A W2 after-recovery k = {:?} (V1 => V2 lost; V2 => \
                     same-kernel page cache retained the pwrite)",
                    after
                        .as_deref()
                        .map(|b| { String::from_utf8_lossy(b).into_owned() })
                );
                let _ = db2.close();
            }
            let _ = env2.close();
        }
        Err(e) => eprintln!("FIX3A W2 reopen env err: {e}"),
    }
}

// ───────────────────────── W1: crash-before-durable (snapshot) ─────────────
// The isolation observation (a read-committed reader seeing the SYNC-committed
// V2 while the committer's fdatasync has NOT completed) is the discriminating
// RED/GREEN signal.  W1 adds the power-loss CONSEQUENCE: at the instant the
// reader observes V2, it snapshots the on-disk env image (the files, minus the
// lock) — a crash image.  Because the WAL is buffered in memory and only
// pwritten in the durable phase (after the early lock release), a snapshot
// taken during the window can lack V2's bytes; recovering it then restores the
// durable base V1, proving the reader returned a value that never became
// durable.  (Same-kernel note: if the committer's durable-phase pwrite already
// reached the page cache before the snapshot, the image may still contain V2;
// the recovery result is REPORTED either way, while the obs signal is the hard
// assertion — a true power loss drops the un-fsynced tail, which FaultDisk
// models byte-precisely and which this snapshot approximates.)

fn snapshot_image(dir: &Path) -> TempDir {
    let img = TempDir::new().unwrap();
    for f in std::fs::read_dir(dir).unwrap() {
        let f = f.unwrap();
        if f.file_type().unwrap().is_file() && f.file_name() != "noxu.lck" {
            // Best-effort copy; a torn tail is fine (recovery handles it).
            let _ = std::fs::copy(f.path(), img.path().join(f.file_name()));
        }
    }
    img
}

#[test]
fn w1_crash_before_durable_reader_sees_nondurable_then_lost() {
    let _serial = PROBE_SERIAL.lock();
    // TRANSACTIONAL db: the inner auto-`Txn` owns the SYNC fsync (JE
    // Txn.createAutoTxn).  After FID-NONTXN-SYNC-FSYNC the non-txn path has no
    // fsync, so this crash-before-durable guard moved to the transactional-db
    // auto-commit path.
    let dir = TempDir::new().unwrap();
    let env = Arc::new(Environment::open(env_cfg(dir.path(), true)).unwrap());
    let db = Arc::new(env.open_database(None, "nt", &db_cfg(true)).unwrap());
    db.put(b"k", V1).unwrap();
    db.put(b"warm", V1).unwrap();
    // Make V1 durable so a crash image always recovers at least V1.
    env.checkpoint(Some(&noxu_db::CheckpointConfig::new().with_force(true)))
        .unwrap();

    let dir_path = dir.path().to_path_buf();
    let img_holder: Arc<Mutex<Option<TempDir>>> = Arc::new(Mutex::new(None));
    let saw_nondurable = Arc::new(AtomicBool::new(false));

    fsync_probe::arm(FSYNC_HOLD_US);

    let db_c = Arc::clone(&db);
    let started = Arc::new(AtomicBool::new(false));
    let s2 = Arc::clone(&started);
    let committer = std::thread::spawn(move || {
        s2.store(true, Ordering::SeqCst);
        // Transactional-db auto-commit SYNC put: inner auto-txn owns the fsync.
        db_c.put(b"k", V2).unwrap();
    });
    while !started.load(Ordering::SeqCst) {
        std::thread::yield_now();
    }
    std::thread::sleep(Duration::from_millis(40));

    // Reader samples during the held-fsync window and, if it sees the
    // non-durable V2, snapshots the crash image at that instant.
    let opts = ReadOptions::new().with_lock_mode(LockMode::ReadCommitted);
    let before = fsync_probe::COMPLETED.load(Ordering::SeqCst);
    let v = db.get_with_options(None, b"k", &opts).ok().flatten();
    let at = fsync_probe::COMPLETED.load(Ordering::SeqCst);
    if v.as_deref() == Some(V2) && before == 0 && at == 0 {
        saw_nondurable.store(true, Ordering::SeqCst);
        *img_holder.lock() = Some(snapshot_image(&dir_path));
    }

    committer.join().unwrap();
    fsync_probe::disarm();

    let observed = saw_nondurable.load(Ordering::SeqCst);
    eprintln!("FIX3A W1 reader saw_nondurable_v2 = {observed}");

    // Report the recovery consequence (informational) when we snapshotted.
    if let Some(img) = img_holder.lock().take() {
        let renv = Environment::open(env_cfg(img.path(), true)).unwrap();
        let rdb = renv.open_database(None, "nt", &db_cfg(true)).unwrap();
        let after = rdb.get(b"k").unwrap().map(|b| b.to_vec());
        eprintln!(
            "FIX3A W1 crash-image recovery k = {:?} (V1 => V2 was non-durable \
             and is LOST; V2 => same-kernel page cache retained the pwrite)",
            after.as_deref().map(|b| String::from_utf8_lossy(b).into_owned())
        );
        rdb.close().unwrap();
        renv.close().unwrap();
    }

    db.close().unwrap();
    Arc::try_unwrap(env).ok().unwrap().close().unwrap();

    assert!(
        !observed,
        "FID-TXN-FIX3A W1 RED (crash-before-durable): a read-committed reader \
         observed the SYNC-committed V2 while the committer's fdatasync had \
         NOT completed (non-durable).  A power loss in that window drops the \
         un-fsynced V2, so the reader returned a value to its client that \
         never became durable.  JE holds the write lock across the durable \
         barrier, so the reader could never observe V2."
    );
}

// ───────────────────────── W3: control (SERIALIZABLE reader) ────────────────
// A SERIALIZABLE (repeatable-read) reader must also never observe the
// non-durable V2.  Independently of the fix, a serializable reader takes a
// read lock that conflicts with the committer's write lock, so it blocks;
// after the fix it blocks until the fsync completes and reads a durable value.

#[test]
fn w3_control_serializable_reader_never_sees_nondurable() {
    let _serial = PROBE_SERIAL.lock();
    use noxu_db::transaction_config::TransactionConfig;
    let dir = TempDir::new().unwrap();
    let env = Arc::new(Environment::open(env_cfg(dir.path(), true)).unwrap());
    let db = Arc::new(Mutex::new(
        env.open_database(None, "sr", &db_cfg(true)).unwrap(),
    ));
    {
        let g = db.lock();
        let t = env.begin_transaction(None).unwrap();
        g.put_in(&t, b"k", V1).unwrap();
        t.commit().unwrap();
        let t = env.begin_transaction(None).unwrap();
        g.put_in(&t, b"warm", V1).unwrap();
        t.commit().unwrap();
    }

    let env_c = Arc::clone(&env);
    let db_c = Arc::clone(&db);
    let env_r = Arc::clone(&env);
    let db_r = Arc::clone(&db);
    let out = race_window(
        LockMode::Default,
        move || {
            let t = env_c.begin_transaction(None).unwrap();
            {
                let g = db_c.lock();
                g.put_in(&t, b"k", V2).unwrap();
            }
            t.commit_with_durability(Durability::COMMIT_SYNC).unwrap();
        },
        move || {
            let mut cfg = TransactionConfig::new();
            cfg.set_serializable_isolation(true);
            let t = env_r.begin_transaction(Some(&cfg)).unwrap();
            let opts = ReadOptions::new().with_lock_mode(LockMode::Default);
            let v = {
                let g = db_r.lock();
                g.get_with_options(Some(&t), b"k", &opts).ok().flatten()
            };
            let _ = t.commit();
            v.map(|b| b.to_vec())
        },
    );

    eprintln!(
        "FIX3A W3 serializable control: saw_v1={} saw_v2={} \
         saw_nondurable_v2={} completed_at_read={}",
        out.saw_v1, out.saw_v2, out.saw_nondurable_v2, out.completed_at_read,
    );
    db.lock().close().unwrap();
    Arc::try_unwrap(env).ok().unwrap().close().unwrap();

    assert!(
        !out.saw_nondurable_v2,
        "FID-TXN-FIX3A W3 control: a SERIALIZABLE reader observed the \
         SYNC-committed V2 before its fsync completed."
    );
}
