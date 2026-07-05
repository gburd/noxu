// Copyright (C) 2024-2025 Greg Burd.  Apache-2.0 OR MIT.
//! Regression: `EnvironmentConfig::with_durability(COMMIT_NO_SYNC)` must
//! actually skip the per-commit fdatasync on the auto-commit `put()` path.
//!
//! Before the fix, `with_durability(COMMIT_NO_SYNC)` set a `config.durability`
//! field that the auto-commit path ignored (it read only the deprecated
//! `txn_no_sync` boolean), so every auto-commit `put()` still fdatasync'd —
//! one fsync per put even though NO_SYNC should do none. This test locks
//! that in via the public `stat_fsync_count()`.

use noxu_db::{Durability, Environment, EnvironmentConfig};

fn scratch(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir()
        .join(format!("noxu-nosync-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn run(durability: Durability, n: u32) -> u64 {
    let dir = scratch(&format!("{:?}", durability.local_sync));
    let env = Environment::open(
        EnvironmentConfig::new(dir.clone())
            .with_transactional(true)
            .with_allow_create(true)
            .with_durability(durability),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "d",
            &noxu_db::DatabaseConfig::new().with_allow_create(true),
        )
        .unwrap();
    let before = env.stat_fsync_count();
    for i in 0..n {
        db.put(format!("k{i:06}").as_bytes(), b"v").unwrap();
    }
    let fsyncs = env.stat_fsync_count() - before;
    db.close().unwrap();
    env.close().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    fsyncs
}

#[test]
fn with_durability_no_sync_skips_per_commit_fsync() {
    const N: u32 = 500;
    let sync_fsyncs = run(Durability::COMMIT_SYNC, N);
    let nosync_fsyncs = run(Durability::COMMIT_NO_SYNC, N);

    // COMMIT_SYNC fdatasyncs roughly per commit (coalescing aside, many).
    assert!(
        sync_fsyncs > N as u64 / 4,
        "COMMIT_SYNC should fdatasync ~per commit; got {sync_fsyncs} for {N} puts",
    );
    // COMMIT_NO_SYNC must NOT fdatasync per commit — only incidental
    // file-flips / background. Must be a tiny fraction of the SYNC count.
    assert!(
        nosync_fsyncs < N as u64 / 10,
        "COMMIT_NO_SYNC must skip per-commit fdatasync (regression: durability \
         ignored on auto-commit); got {nosync_fsyncs} fsyncs for {N} NO_SYNC puts \
         (SYNC did {sync_fsyncs})",
    );
}
