//! EVICTOR-RECLAIM-1 headline test: cache-pressure reclaim across a USER tree.
//!
//! Regression guard for the multi-tree eviction fix.  Before the fix the
//! evictor reclaimed almost nothing under cache pressure: resident memory
//! stayed ~1.45x the configured budget.  Two distinct defects combined:
//!
//!   1. Split-created BINs/INs were never registered with the evictor's LRU
//!      (only the first-key root+BIN and re-fetched nodes were).  JE
//!      `IN.splitInternal` calls `inList.add(newSibling)`; Noxu's split path
//!      did not.  So the policy lists stayed nearly empty and `evict_batch`
//!      selected almost nothing.
//!   2. The evictor held a single primary-tree slot; its strip/flush/detach/
//!      evict_root lookups searched ONLY that tree.  A second database's BINs
//!      (db_id != the primary slot) were TARGETED via the InList listener but
//!      could never be found/stripped.  JE walks ONE env-wide INList covering
//!      all DBs and resolves each target IN's owning DB via
//!      `target.getDatabase()` (Evictor.processTarget, Evictor.java:2374);
//!      Noxu's per-DB trees require the shared `db_trees_registry`.
//!
//! This test populates a working set FAR larger than a small cache across TWO
//! user databases (so the second lives only in the registry, exercising the
//! multi-tree lookup) and asserts:
//!   * resident `cache_usage_bytes()` drops to NEAR the budget (<= 1.2x),
//!   * the evictor actually fired (nodes stripped/evicted, freed > 0),
//!   * every record re-fetches correctly (no data loss).
//!
//! Runs on REAL disk under /scratch (NOT tmpfs) per the EVICTOR-RECLAIM-1
//! measurement discipline.

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    OperationStatus,
};

/// Create a unique env dir on /scratch (real disk).  Falls back to the
/// system temp dir only if /scratch is not present, so the test still runs in
/// CI environments without /scratch (the reclaim assertion is disk-agnostic).
fn scratch_dir(tag: &str) -> std::path::PathBuf {
    let base = std::path::Path::new("/scratch");
    let root = if base.is_dir() {
        base.join(format!("noxu-evreclaim-{}-{}", tag, std::process::id()))
    } else {
        std::env::temp_dir().join(format!(
            "noxu-evreclaim-{}-{}",
            tag,
            std::process::id()
        ))
    };
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create scratch dir");
    root
}

#[test]
fn evictor_reclaims_to_budget_across_user_dbs() {
    let dir = scratch_dir("multitree");

    // 16 MiB cache, ~21 MB working set split across TWO user databases -> the
    // cache cannot hold the whole set, so eviction MUST reclaim toward budget.
    let cache_bytes: u64 = 16 * 1024 * 1024;
    let mut cfg = EnvironmentConfig::new(dir.clone());
    cfg.set_allow_create(true);
    cfg.set_transactional(true);
    cfg.set_cache_percent(0); // so set_cache_size takes effect
    cfg.set_cache_size(cache_bytes);
    let env = Environment::open(cfg).expect("open env");

    // Two USER databases.  The first takes the primary tree slot; the SECOND
    // (db_id != the primary slot) lives only in the db_trees_registry -- the
    // case the single-tree evictor could never reclaim.
    let db_a: Database = env
        .open_database(
            None,
            "userdb_a",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .expect("open user db a");
    let db_b: Database = env
        .open_database(
            None,
            "userdb_b",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .expect("open user db b");

    // ~21 MB total: 150k records * ~140 B, half in each database.
    let n = 150_000usize;
    let val = vec![0xABu8; 120];
    for i in 0..n {
        let k = DatabaseEntry::from_vec(format!("{:010}", i).into_bytes());
        let v = DatabaseEntry::from_bytes(&val);
        let db = if i % 2 == 0 { &db_a } else { &db_b };
        db.put(None, &k, &v).unwrap();
    }

    let usage_before = env.cache_usage_bytes().unwrap();

    // Drive eviction until usage reaches the budget or converges (the daemon
    // runs in the background too; this makes the result deterministic).
    let mut last = usage_before;
    for _ in 0..200 {
        let _ = env.evict_memory().unwrap();
        let now = env.cache_usage_bytes().unwrap();
        if now <= cache_bytes as i64 {
            break;
        }
        if (last - now).abs() < 4096 {
            break;
        }
        last = now;
    }
    let usage_after = env.cache_usage_bytes().unwrap();

    let stats = env.get_stats().unwrap().evictor;
    eprintln!(
        "EVICTOR-RECLAIM-1: budget={} before={} after={} ratio_after={:.3} \
         targeted={} stripped={} evicted={} freed_bytes={}",
        cache_bytes,
        usage_before,
        usage_after,
        usage_after as f64 / cache_bytes as f64,
        stats.nodes_targeted,
        stats.nodes_stripped,
        stats.nodes_evicted,
        stats.bytes_evicted,
    );

    // The evictor must actually have fired across the user trees.
    assert!(
        stats.bytes_evicted > 0,
        "evictor must reclaim bytes from the user trees; freed={} (pre-fix ~0)",
        stats.bytes_evicted
    );
    assert!(
        stats.nodes_stripped + stats.nodes_evicted > 100,
        "evictor must strip/evict many user-tree nodes; stripped={} evicted={} \
         (pre-fix stripped~1, evicted~0)",
        stats.nodes_stripped,
        stats.nodes_evicted
    );

    // HEADLINE: resident usage must drop to NEAR the budget (<= 1.2x), NOT
    // stay stuck at ~1.45x as it did pre-fix.
    assert!(
        (usage_after as f64) <= 1.2 * cache_bytes as f64,
        "resident cache_usage must drop to <= 1.2x budget after eviction; \
         got {} bytes ({:.3}x budget {})",
        usage_after,
        usage_after as f64 / cache_bytes as f64,
        cache_bytes
    );

    // CORRECTNESS (sacred): every sampled record in BOTH databases must
    // re-fetch correctly -- stripped/evicted data is recoverable from the log.
    for i in (0..n).step_by(101) {
        let k = DatabaseEntry::from_vec(format!("{:010}", i).into_bytes());
        let db = if i % 2 == 0 { &db_a } else { &db_b };
        let mut out = DatabaseEntry::new();
        let st = db.get(None, &k, &mut out).unwrap();
        assert_eq!(
            st,
            OperationStatus::Success,
            "record {} must survive eviction",
            i
        );
        assert_eq!(out.data(), &val[..], "record {} data intact", i);
    }

    drop(db_a);
    drop(db_b);
    drop(env);
    let _ = std::fs::remove_dir_all(&dir);
}
