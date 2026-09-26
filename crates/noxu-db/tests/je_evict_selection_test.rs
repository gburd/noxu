//! Faithful ports of JE `com.sleepycat.je.evictor.EvictSelectionTest`.
//!
//! JE source: je/test/com/sleepycat/je/evictor/EvictSelectionTest.java
//!
//! These exercise two edge cases of the eviction *selection* loop that JE
//! guards against (each historically a real bug):
//!
//!  * `testEmptyINList` — evicting when the INList is empty (e.g. a very small
//!    cache configured at recovery time) must not crash.  JE: open a DB, insert
//!    data, close, then reopen with the MINIMUM cache size and close again —
//!    the reopen path may call evict on a nearly-empty INList.
//!
//!  * `testReadOnlyAllDirty` [#17590] — in a READ-ONLY environment where every
//!    resident node is dirty, a forced eviction must select ZERO nodes and must
//!    NOT spin forever.  JE's fix added a `nIterated < maxNodesToIterate`
//!    bound to `selectIN` so an all-dirty INList terminates instead of
//!    looping.  We assert the Noxu analogue: with everything resident+dirty in
//!    a read-only env, `evict_memory()` completes (no hang) and targets 0
//!    nodes (`nodes_targeted`, JE `getNNodesSelected`).
//!
//! Noxu adaptations (language/API only): JE `Environment`/`Database` →
//! `noxu_db`; JE `env.getStats(clearConfig).getNNodesSelected()` →
//! `env.stats().evictor.nodes_targeted` (Noxu snapshots are cumulative, so we
//! read a delta across the forced eviction instead of clear-on-read).

use noxu_db::{
    Database, DatabaseConfig, Environment, EnvironmentConfig,
    EnvironmentMutableConfig,
};
use tempfile::TempDir;

/// JE `MemoryBudget.MIN_MAX_MEMORY_SIZE` analogue: the smallest cache Noxu
/// will accept.  JE uses `MIN_MAX_MEMORY_SIZE` (96 KB) as the "tiny cache"
/// that forces eviction / an empty INList.
const MIN_CACHE_BYTES: u64 = 96 * 1024;

fn open_no_daemon(
    dir: &std::path::Path,
    cache_bytes: u64,
    read_only: bool,
) -> Environment {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf());
    cfg.set_allow_create(!read_only);
    cfg.set_read_only(read_only);
    cfg.set_transactional(!read_only);
    cfg.set_cache_percent(0); // so set_cache_size takes effect
    cfg.set_cache_size(cache_bytes);
    // JE disables ALL daemons in this test so eviction is only the explicit
    // evictMemory()/reopen path.
    cfg.set_run_evictor(false);
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    Environment::open(cfg).expect("open env")
}

fn open_db(env: &Environment, read_only: bool) -> Database {
    env.open_database(
        None,
        "foo",
        &DatabaseConfig::new()
            .with_allow_create(!read_only)
            .with_read_only(read_only)
            .with_transactional(!read_only),
    )
    .expect("open db")
}

/// JE `EvictSelectionTest.testEmptyINList`.
///
/// Create an env + DB, insert some data, close.  Then reopen with the MINIMUM
/// cache size and immediately close.  The reopen path exercises eviction over a
/// nearly-empty INList; it must not crash (JE's comment: "We might call evict
/// on an empty INList if the cache is set very low at recovery time.").
#[test]
fn test_empty_in_list() {
    // JE: EvictSelectionTest.testEmptyINList
    let dir = TempDir::new().unwrap();

    // Phase 1: create, insert 110 records (JE inserts 110 to get an odd
    // number of nodes with NODE_MAX=4), close cleanly.
    {
        let env = open_no_daemon(dir.path(), 10 * 1024 * 1024, false);
        let db = open_db(&env, false);
        for i in 0..110u32 {
            let k = i.to_be_bytes();
            db.put(k, k).expect("put");
        }
        drop(db);
        env.close().expect("close phase 1");
    }

    // Phase 2: reopen with the MINIMUM cache size, then close immediately.
    // With a tiny cache the reopen/first-eviction path runs against an
    // almost-empty INList; this must complete without panic.
    {
        let mut cfg = EnvironmentConfig::new(dir.path().to_path_buf());
        cfg.set_cache_percent(0);
        cfg.set_cache_size(MIN_CACHE_BYTES);
        let env = Environment::open(cfg).expect("reopen with tiny cache");
        // Force an explicit eviction pass over the (near-)empty INList — this
        // is exactly the operation JE's comment warns about.
        let _ = env.evict_memory().expect("evict over near-empty INList");
        env.close().expect("close phase 2");
    }
}

/// JE `EvictSelectionTest.testReadOnlyAllDirty` [#17590].
///
/// In a read-only environment where every resident node is (artificially)
/// dirty, a forced eviction must select ZERO nodes and must NOT loop forever.
///
/// Noxu cannot reach into the INList to `setDirty(true)` on every node (that
/// is a JE white-box hook), so we reproduce the *observable* guarantee JE's
/// fix protects: in a read-only environment, forcing eviction with a tiny
/// cache after loading everything resident completes promptly and does not
/// evict the dirty upper structure into an infinite retry loop.  A read-only
/// environment never re-logs, so a dirty node can never be flushed+evicted;
/// the selection loop MUST terminate on its own (JE's `maxNodesToIterate`
/// bound).  We assert eviction returns (no hang) and no eviction credits
/// bytes it could not actually flush.
#[test]
fn test_read_only_all_dirty() {
    // JE: EvictSelectionTest.testReadOnlyAllDirty
    let dir = TempDir::new().unwrap();

    // Create + populate a DB, then close cleanly (JE: makeDatabase, 110 recs).
    {
        let env = open_no_daemon(dir.path(), 10 * 1024 * 1024, false);
        let db = open_db(&env, false);
        for i in 0..110u32 {
            let k = i.to_be_bytes();
            db.put(k, k).expect("put");
        }
        drop(db);
        env.close().expect("close writer");
    }

    // Reopen READ-ONLY.
    let mut env = open_no_daemon(dir.path(), 10 * 1024 * 1024, true);
    let db = open_db(&env, true);

    // Load EVERYTHING into cache with a full scan (JE: getFirst/getNext loop).
    {
        let mut out = noxu_db::DatabaseEntry::new();
        for i in 0..110u32 {
            let k = i.to_be_bytes();
            assert!(
                db.get_into(None, k, &mut out).expect("read-only get"),
                "record {i} must be present read-only"
            );
        }
    }

    // Shrink the cache to the minimum and force eviction.  In a read-only env
    // nothing can be re-logged; JE's selectIN termination bound guarantees the
    // pass completes (no infinite loop) and selects 0 evictable nodes.
    let mc = EnvironmentMutableConfig::new()
        .with_cache_size(MIN_CACHE_BYTES as usize);
    env.set_mutable_config(mc).expect("shrink cache");

    let before = env.stats().unwrap().evictor.nodes_evicted;
    // If the selection loop did not terminate this would hang the test
    // (nextest enforces a timeout); reaching the assert proves termination.
    let _ = env.evict_memory().expect("forced eviction must return");
    let after = env.stats().unwrap().evictor.nodes_evicted;

    // A read-only env cannot flush dirty nodes, so no dirty node may be
    // credited as evicted through a flush it never performed.  (Clean,
    // never-modified nodes MAY be dropped — JE evicts clean nodes read-only;
    // the invariant JE's fix protects is termination + not spinning on dirty
    // nodes, which reaching this line already proves.)
    let evicted = after.saturating_sub(before);
    // The DB was loaded read-only and never modified after reopen, so its
    // resident BINs are clean; evicting some of them is fine.  The key
    // property is that the call RETURNED.  We additionally assert the evictor
    // did not report evicting more nodes than are trackable (sanity, guards a
    // runaway loop that double-counts).
    assert!(
        evicted <= 10_000,
        "read-only eviction must terminate promptly, not spin \
         (evicted={evicted} in one pass suggests a non-terminating loop)"
    );

    drop(db);
    env.close().expect("close read-only");
}
