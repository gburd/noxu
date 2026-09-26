// INDEPENDENT NEW-10 probe: is the evict+daemon loss DURABLE (point-get-false after reopen)?
use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
};
use tempfile::TempDir;

fn open_small(
    dir: &std::path::Path,
    cache: u64,
    run_evictor: bool,
) -> (Environment, Database) {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf());
    cfg.set_allow_create(true);
    cfg.set_transactional(true);
    cfg.set_cache_percent(0);
    cfg.set_cache_size(cache);
    cfg.set_run_evictor(run_evictor);
    let env = Environment::open(cfg).expect("open");
    let db = env
        .open_database(
            None,
            "evict",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .expect("db");
    (env, db)
}
fn fill(env: &Environment, db: &Database, n: usize) {
    let val = vec![7u8; 80];
    let mut i = 0;
    while i < n {
        let e = (i + 1000).min(n);
        let txn = env.begin_transaction(None).unwrap();
        for j in i..e {
            db.put_in(
                &txn,
                DatabaseEntry::from_vec(format!("{:010}", j).into_bytes()),
                DatabaseEntry::from_bytes(&val),
            )
            .unwrap();
        }
        txn.commit().unwrap();
        i = e;
    }
}

#[test]
fn new10_durable_loss_probe() {
    let n = 20_000usize;
    for attempt in 0..12 {
        let dir = TempDir::new().unwrap();
        {
            let (env, db) = open_small(dir.path(), 2 * 1024 * 1024, true); // daemon ON
            fill(&env, &db, n);
            for _ in 0..6 {
                let _ = env.evict_memory().unwrap();
            }
            db.close().ok();
            env.close().unwrap();
        }
        // REOPEN, daemon OFF, point-get every key (definitive durable check, no cursor)
        let (env, db) = open_small(dir.path(), 64 * 1024 * 1024, false);
        let mut missing = Vec::new();
        for j in 0..n {
            let k = format!("{:010}", j).into_bytes();
            if db.get(&k).unwrap().is_none() {
                missing.push(j);
            }
        }
        db.close().ok();
        env.close().unwrap();
        if !missing.is_empty() {
            panic!(
                "NEW-10 DURABLE LOSS attempt {}: {} keys ABSENT after reopen via POINT-GET: {:?}",
                attempt,
                missing.len(),
                &missing[..missing.len().min(8)]
            );
        }
        eprintln!(
            "attempt {}: all {} keys present after reopen (point-get)",
            attempt, n
        );
    }
}
