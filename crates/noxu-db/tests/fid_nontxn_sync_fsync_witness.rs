// Copyright (C) 2024-2026 Greg Burd.  Apache-2.0 OR MIT.
//! FID-NONTXN-SYNC-FSYNC — a NON-transactional database write must NOT fsync
//! per operation (JE `ThreadLocker`/`BasicLocker` semantics).
//!
//! JE: a non-transactional write logs the LN with the `LogParams` defaults
//! `flushRequired=false, fsyncRequired=false` (`LN.logInternal`, LN.java:636;
//! `LogParams.java:42,53`), regardless of env durability; and
//! `BasicLocker`/`ThreadLocker.operationEnd` (`BasicLocker.java:198-247`) only
//! RELEASES the record locks — it never flushes or fsyncs.  Durability for a
//! non-txn write comes later: from a checkpoint, `Environment.sync()`
//! (`Environment.java:1603`, a forced checkpoint), `Environment.flushLog()`
//! (`Environment.java:1644`), a subsequent SYNC commit that drains the buffer,
//! or env close.
//!
//! Before this fix, a non-transactional-db `Database::put` under the default
//! (SYNC) policy routed through an inner auto-`Txn` that fsynced the LN at
//! operation end (`database.rs::with_auto_commit_transaction` else-branch,
//! `PendingSync::Ln`).  That is MORE durability than JE provides on that path.
//!
//! Witnesses:
//!   * `nontxn_put_does_no_per_write_fsync` — N non-txn puts add 0 fsyncs
//!     (RED before the fix: each put fsynced; GREEN after: delta == 0).
//!   * `nontxn_cursor_put_does_no_per_write_fsync` — same via a bare
//!     `open_cursor(None)` write on a non-txn db.
//!   * `txnal_sync_commit_still_fsyncs` — an explicit SYNC commit on a
//!     transactional db still fsyncs (the fix does not weaken txnal SYNC).
//!   * `transactional_db_auto_commit_still_fsyncs` — `Database::put` with no
//!     txn on a TRANSACTIONAL db (JE `Txn.createAutoTxn`) still fsyncs.
//!   * `env_sync_makes_nontxn_writes_durable` — `env.sync()` fsyncs the
//!     buffered non-txn writes (fsync count rises; data readable after it).
//!   * `env_flush_log_fsync_makes_nontxn_writes_durable` — `env.flush_log(true)`
//!     fsyncs; `env.flush_log(false)` does not.
//!
//! The crash-survival leg (synced survive a SIGKILL, unsynced are lost) is the
//! `crash_worker` subprocess harness, exercised by
//! `fid_nontxn_sync_fsync_crash_witness.rs` (crash mode `nontxn_sync_survives`).

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Durability, Environment, EnvironmentConfig,
    Put,
};
use tempfile::TempDir;

fn open_env(dir: &TempDir) -> Environment {
    Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            // Keep daemons quiet so no background checkpoint/evictor fsyncs
            // pollute the per-put fsync-count delta.
            .with_run_checkpointer(false)
            .with_run_cleaner(false)
            .with_run_evictor(false)
            .with_run_in_compressor(false)
            .with_run_verifier(false),
    )
    .expect("open env")
}

#[test]
fn nontxn_put_does_no_per_write_fsync() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    // A plain non-transactional database (JE ThreadLocker write path).
    let db = env
        .open_database(
            None,
            "nontxn",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .expect("open non-txn db");

    let before = env.stat_fsync_count();
    for i in 0u32..64 {
        db.put(i.to_be_bytes(), b"v").expect("non-txn put");
    }
    let after = env.stat_fsync_count();
    assert_eq!(
        after - before,
        0,
        "non-txn puts must add ZERO fsyncs (JE LN.logInternal \
         flushRequired=false,fsyncRequired=false); got delta {}",
        after - before
    );
    // The writes are visible in-memory (durability is deferred, not the write).
    assert_eq!(db.get(0u32.to_be_bytes()).unwrap().as_deref(), Some(&b"v"[..]));
}

#[test]
fn nontxn_cursor_put_does_no_per_write_fsync() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = env
        .open_database(
            None,
            "nontxn",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .expect("open non-txn db");

    let before = env.stat_fsync_count();
    {
        let mut c = db.open_cursor(None).expect("open auto-commit cursor");
        for i in 0u32..64 {
            c.put(
                &DatabaseEntry::from_bytes(&i.to_be_bytes()),
                &DatabaseEntry::from_bytes(b"v"),
                Put::Overwrite,
            )
            .expect("non-txn cursor put");
        }
        c.close().unwrap();
    }
    let after = env.stat_fsync_count();
    assert_eq!(
        after - before,
        0,
        "non-txn cursor puts must add ZERO fsyncs; got delta {}",
        after - before
    );
}

#[test]
fn txnal_sync_commit_still_fsyncs() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = env
        .open_database(
            None,
            "txnal",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .expect("open txnal db");

    let before = env.stat_fsync_count();
    let txn = env.begin_transaction(None).unwrap();
    db.put_in(&txn, b"k", b"v").unwrap();
    txn.commit_with_durability(Durability::COMMIT_SYNC).unwrap();
    let after = env.stat_fsync_count();
    assert!(
        after > before,
        "an explicit SYNC commit MUST still fsync (JE Txn.commit SYNC); \
         delta was {}",
        after - before
    );
}

#[test]
fn transactional_db_auto_commit_still_fsyncs() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    // Auto-commit `Database::put` with NO txn on a TRANSACTIONAL db is JE
    // `Txn.createAutoTxn` — a real transaction, which DOES fsync for SYNC.
    let db = env
        .open_database(
            None,
            "txnal",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .expect("open txnal db");

    let before = env.stat_fsync_count();
    db.put(b"k", b"v").unwrap();
    let after = env.stat_fsync_count();
    assert!(
        after > before,
        "auto-commit put on a TRANSACTIONAL db (JE Txn.createAutoTxn) must \
         fsync; delta was {}",
        after - before
    );
}

#[test]
fn env_sync_makes_nontxn_writes_durable() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = env
        .open_database(
            None,
            "nontxn",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .expect("open non-txn db");

    for i in 0u32..32 {
        db.put(i.to_be_bytes(), b"v").unwrap();
    }
    let before = env.stat_fsync_count();
    // JE Environment.sync() = a forced checkpoint; it must make the buffered
    // non-txn writes durable (fsync the log).
    env.sync().expect("env.sync()");
    let after = env.stat_fsync_count();
    assert!(
        after > before,
        "env.sync() must fsync the buffered non-txn writes; delta was {}",
        after - before
    );
    // Reopen and confirm the synced data survives.
    drop(db);
    env.close().unwrap();
    let env2 = open_env(&dir);
    let db2 = env2
        .open_database(None, "nontxn", &DatabaseConfig::new())
        .expect("reopen non-txn db");
    for i in 0u32..32 {
        assert_eq!(
            db2.get(i.to_be_bytes()).unwrap().as_deref(),
            Some(&b"v"[..]),
            "record {i} must survive env.sync()+reopen",
        );
    }
}

#[test]
fn env_flush_log_fsync_makes_nontxn_writes_durable() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = env
        .open_database(
            None,
            "nontxn",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .expect("open non-txn db");

    for i in 0u32..16 {
        db.put(i.to_be_bytes(), b"v").unwrap();
    }
    // flush_log(false): file write, no fsync.
    let before_nf = env.stat_fsync_count();
    env.flush_log(false).expect("flush_log(false)");
    assert_eq!(
        env.stat_fsync_count() - before_nf,
        0,
        "flush_log(false) must NOT fsync",
    );
    // flush_log(true): fsync.
    let before_f = env.stat_fsync_count();
    env.flush_log(true).expect("flush_log(true)");
    assert!(
        env.stat_fsync_count() - before_f >= 1,
        "flush_log(true) must fsync (JE Environment.flushLog(true))",
    );
}
