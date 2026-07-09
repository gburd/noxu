// Copyright (C) 2024-2025 Greg Burd.  Apache-2.0 OR MIT.
//! Regression: a read-only-in-practice transaction commit at `COMMIT_SYNC`
//! must NOT write a `TxnCommit` WAL frame and must NOT fdatasync.
//!
//! A transaction created with `begin_transaction(None)` (or any config that
//! does not set `with_read_only(true)`) is *write-capable*, but if it only
//! performed reads it logged no LN and has nothing to commit durably.
//!
//! Before the fix, `Transaction::commit_with_durability` gated the
//! `write_txn_end` (TxnCommit frame + fsync) on the static `read_only`
//! config flag instead of the dynamic "did this txn log anything" signal
//! (`Txn::has_logged_entries`).  So every explicit read txn drove
//! `write_txn_end -> LogManager::log -> flush_sync -> fdatasync` at SYNC,
//! serialising 100%-cache-hit readers on the log-write latch + fsync
//! group-commit condvar (read-commit-contention audit, 2026-07; a
//! 100%-read workload was 3-4x slower at SYNC than NO_SYNC purely from this).
//!
//! JE: `Txn.commit()` writes a commit entry only for txns that have logged
//! entries.  This test locks that in via the public `stat_fsync_count()`.

use noxu_db::{Durability, Environment, EnvironmentConfig};

fn scratch(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "noxu-ro-commit-{}-{}",
        tag,
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn open(dir: &std::path::Path) -> (Environment, noxu_db::Database) {
    let env = Environment::open(
        EnvironmentConfig::new(dir.to_path_buf())
            .with_transactional(true)
            .with_allow_create(true)
            // Env default durability is SYNC; the write-capable read txns
            // commit at SYNC, which is exactly the path that used to fsync.
            .with_durability(Durability::COMMIT_SYNC),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "d",
            &noxu_db::DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    (env, db)
}

#[test]
fn readonly_txn_commit_does_not_fsync_at_sync() {
    const N: u32 = 500;
    let dir = scratch("noop");
    let (env, db) = open(&dir);

    // Seed one record so the reads actually hit data (cache hit).
    db.put(b"k000000", b"v").unwrap();

    // Baseline: N write txns at SYNC fsync ~per commit (this is the
    // "the machinery works" control — proves the counter moves).
    let before_writes = env.stat_fsync_count();
    for i in 0..N {
        let t = env.begin_transaction(None).unwrap();
        db.put_in(&t, format!("w{i:06}").as_bytes(), b"v").unwrap();
        t.commit().unwrap();
    }
    let write_fsyncs = env.stat_fsync_count() - before_writes;

    // The regression check: N read-only explicit txns at SYNC.  Each is
    // begin_transaction(None) (write-capable, NOT read_only) + a get + a
    // SYNC commit — the exact xbench ycsb_c shape.  They must fdatasync
    // essentially zero times because they logged nothing.
    let before_reads = env.stat_fsync_count();
    for _ in 0..N {
        let t = env.begin_transaction(None).unwrap();
        let _ = db.get_in(&t, b"k000000").unwrap();
        t.commit().unwrap();
    }
    let read_fsyncs = env.stat_fsync_count() - before_reads;

    db.close().unwrap();
    env.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);

    // Control: write commits DID fsync (coalescing aside).
    assert!(
        write_fsyncs > N as u64 / 4,
        "control: SYNC write commits should fdatasync ~per commit; got \
         {write_fsyncs} for {N} write txns",
    );
    // The fix: read-only commits must NOT fsync per commit.  Only incidental
    // background/file-flip fsyncs are tolerated, and must be a tiny fraction
    // of the write count.
    assert!(
        read_fsyncs < N as u64 / 20,
        "REGRESSION: read-only txn commits fsynced at SYNC ({read_fsyncs} \
         fsyncs for {N} read-only commits; write baseline was {write_fsyncs}). \
         A read-only-in-practice commit must skip write_txn_end + flush_sync \
         (gate on has_logged_entries, not the static read_only flag).",
    );
}
