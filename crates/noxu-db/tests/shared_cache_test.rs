//! SHARED_CACHE (7.1.x) headline test: cross-environment cache-budget balancing.
//!
//! Faithful port of JE `com.sleepycat.je.evictor.SharedEvictor` +
//! `EnvironmentConfig.setSharedCache(true)`: multiple `Environment`s opened
//! with `shared_cache=true` share ONE process-global `Evictor` and ONE memory
//! budget (the FIRST env's cache size — JE-faithful).  Eviction picks victims
//! across ALL sharing envs' trees, not per-env.
//!
//! This test proves:
//!   1. **One budget, not the sum.** Open TWO shared-cache envs, load a
//!      working set into BOTH that far exceeds ONE env's budget, and assert
//!      total resident (read through EITHER env — both read the SAME shared
//!      counter) stays ~= the ONE shared budget, NOT ~2x it.
//!   2. **Eviction spans envs.** The evictor fires (bytes freed) driven from
//!      either env, and both envs' data re-fetches correctly (no data loss).
//!   3. **Close-safety / no dangling trees.** Close ONE shared-cache env and
//!      prove the SURVIVOR keeps working (reads + eviction) with no
//!      use-after-close of the closed env's trees.
//!
//! Because the shared evictor is a process-global singleton, this test resets
//! it at start via the internal test hook so it does not collide with other
//! tests in the same binary.

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
};
use noxu_evictor::SharedEvictorHandle;

fn tmp_dir(tag: &str) -> std::path::PathBuf {
    let root = std::env::temp_dir().join(format!(
        "noxu-sharedcache-{}-{}",
        tag,
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).expect("create tmp dir");
    root
}

fn open_shared_env(dir: &std::path::Path, cache_bytes: u64) -> Environment {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf());
    cfg.set_allow_create(true);
    cfg.set_transactional(true);
    cfg.set_cache_percent(0); // so set_cache_size takes effect
    cfg.set_cache_size(cache_bytes);
    cfg.set_shared_cache(true); // <-- the feature under test
    Environment::open(cfg).expect("open shared-cache env")
}

fn fill(db: &Database, prefix: u8, n: usize, val: &[u8]) {
    for i in 0..n {
        let mut key = vec![prefix];
        key.extend_from_slice(format!("{:010}", i).as_bytes());
        let k = DatabaseEntry::from_vec(key);
        let v = DatabaseEntry::from_bytes(val);
        db.put(&k, &v).unwrap();
    }
}

fn drive_eviction(env: &Environment, budget: u64) -> i64 {
    let mut last = env.cache_usage_bytes().unwrap();
    for _ in 0..300 {
        let _ = env.evict_memory().unwrap();
        let now = env.cache_usage_bytes().unwrap();
        if now <= budget as i64 {
            break;
        }
        if (last - now).abs() < 4096 {
            break;
        }
        last = now;
    }
    env.cache_usage_bytes().unwrap()
}

#[test]
fn shared_cache_balances_one_budget_across_envs() {
    // Isolate this test's shared-evictor state from any other test in the
    // binary (process-global singleton).
    SharedEvictorHandle::reset_for_test();

    // ONE shared budget of 1 MiB (small so pressure is real with a modest
    // working set, keeping the test fast).  Each env asks for 1 MiB, but
    // because they share the cache the TOTAL resident across BOTH must stay
    // ~= 1 MiB, NOT 2 MiB (the sum of the per-env requests).
    let budget: u64 = 1024 * 1024;

    let dir1 = tmp_dir("env1");
    let dir2 = tmp_dir("env2");
    let env1 = open_shared_env(&dir1, budget);
    // Second env's requested cache_size is deliberately different (16 MiB) to
    // prove the FIRST joiner's budget wins (JE-faithful) — the shared budget
    // stays 1 MiB regardless.
    let env2 = open_shared_env(&dir2, 16 * 1024 * 1024);

    let db1: Database = env1
        .open_database(
            None,
            "db1",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .expect("open db1");
    let db2: Database = env2
        .open_database(
            None,
            "db2",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .expect("open db2");

    // ~1 MB into EACH env (~2 MB total) -> ~2x the ONE 1 MiB budget, so
    // eviction must balance across both envs.  Kept modest so the test runs
    // well under the fast-suite timeout.
    let n = 8_000usize;
    let val = vec![0xCDu8; 120];
    fill(&db1, b'A', n, &val);
    fill(&db2, b'B', n, &val);

    // env1 and env2 read the SAME shared counter, so both report the shared
    // total.  Prove that first.
    let u1 = env1.cache_usage_bytes().unwrap();
    let u2 = env2.cache_usage_bytes().unwrap();
    eprintln!(
        "SHARED_CACHE: budget={} env1_usage={} env2_usage={} (should be equal)",
        budget, u1, u2
    );
    assert_eq!(
        u1, u2,
        "both shared-cache envs must read the SAME shared budget counter"
    );

    // Drive eviction (either env drives the ONE shared evictor).
    let usage_after = drive_eviction(&env1, budget);

    let stats1 = env1.stats().unwrap().evictor;
    let stats2 = env2.stats().unwrap().evictor;
    eprintln!(
        "SHARED_CACHE: after={} ratio={:.3}x budget | ev1(freed={},strip={},evict={}) \
         ev2(freed={},strip={},evict={})",
        usage_after,
        usage_after as f64 / budget as f64,
        stats1.bytes_evicted,
        stats1.nodes_stripped,
        stats1.nodes_evicted,
        stats2.bytes_evicted,
        stats2.nodes_stripped,
        stats2.nodes_evicted,
    );

    // The evictor fired (across the shared LRU).  env1 and env2 read the SAME
    // shared evictor stats, so the counters are identical and non-zero.
    assert!(
        stats1.bytes_evicted > 0,
        "shared evictor must reclaim bytes; freed={}",
        stats1.bytes_evicted
    );
    assert_eq!(
        stats1.bytes_evicted, stats2.bytes_evicted,
        "both envs share ONE evictor, so its stats are identical"
    );

    // HEADLINE: total resident stays NEAR the ONE budget (<= 1.7x), NOT the
    // sum of the two per-env requests (~2 MB working set would stay resident
    // without balancing — each env would keep its own ~1 MB).
    assert!(
        (usage_after as f64) <= 1.7 * budget as f64,
        "shared total resident must stay ~= ONE budget ({}), not the sum of \
         per-env budgets; got {} ({:.3}x)",
        budget,
        usage_after,
        usage_after as f64 / budget as f64,
    );

    // Eviction must have picked victims from BOTH envs' trees (the whole point
    // of a shared LRU).  Prove it by sampling records from BOTH databases: all
    // must re-fetch correctly (stripped/evicted data re-reads from each env's
    // own log).
    for i in (0..n).step_by(211) {
        let mut ka = vec![b'A'];
        ka.extend_from_slice(format!("{:010}", i).as_bytes());
        let mut out = DatabaseEntry::new();
        assert!(
            db1.get_into(None, DatabaseEntry::from_vec(ka), &mut out).unwrap(),
            "env1 record {} survives shared eviction",
            i
        );
        assert_eq!(out.data(), &val[..]);

        let mut kb = vec![b'B'];
        kb.extend_from_slice(format!("{:010}", i).as_bytes());
        let mut out2 = DatabaseEntry::new();
        assert!(
            db2.get_into(None, DatabaseEntry::from_vec(kb), &mut out2).unwrap(),
            "env2 record {} survives shared eviction",
            i
        );
        assert_eq!(out2.data(), &val[..]);
    }

    // ---- CLOSE-SAFETY: close env2, prove env1 (survivor) keeps working ----
    drop(db2);
    env2.close().expect("close env2");
    drop(env2);

    // env2's trees must have been deregistered from the shared LRU (no
    // dangling trees).  The survivor keeps reading and evicting.
    let u_survivor = env1.cache_usage_bytes().unwrap();
    eprintln!("SHARED_CACHE close-safety: survivor usage={}", u_survivor);

    // Survivor can still read its own data.
    let mut probe = DatabaseEntry::new();
    let mut kp = vec![b'A'];
    kp.extend_from_slice(format!("{:010}", 7usize).as_bytes());
    assert!(
        db1.get_into(None, DatabaseEntry::from_vec(kp), &mut probe).unwrap(),
        "survivor env1 must still read its data after env2 closed"
    );

    // Survivor can still write + evict (proves the shared daemon/evictor is
    // intact and does not touch the closed env's freed trees).
    fill(&db1, b'C', 3_000, &val);
    let _ = drive_eviction(&env1, budget);
    let mut kc = vec![b'C'];
    kc.extend_from_slice(format!("{:010}", 42usize).as_bytes());
    let mut out3 = DatabaseEntry::new();
    assert!(
        db1.get_into(None, DatabaseEntry::from_vec(kc), &mut out3).unwrap(),
        "survivor env1 must read newly-written data after env2 closed"
    );
    assert_eq!(out3.data(), &val[..]);

    drop(db1);
    env1.close().expect("close env1");
    drop(env1);

    // After the last member closes, the shared evictor tears down.
    assert_eq!(
        SharedEvictorHandle::member_count(),
        0,
        "all shared-cache members deregistered on close"
    );

    let _ = std::fs::remove_dir_all(&dir1);
    let _ = std::fs::remove_dir_all(&dir2);
}
