//! Eviction-under-pressure tests.
//!
//! These validate the evictor F1+F2 wiring (LRU lists fed from production tree
//! ops; eviction decrements the shared cache_usage counter) and the F8/F10
//! tuning in the regime that matters: a cache SMALLER than the working set, so
//! eviction actually runs. The default 64 MiB cache never evicts at typical
//! test scales, which is why the JE comparison benchmark (64 MiB cache, ~15 MB
//! working set) did not exercise eviction.

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    OperationStatus,
};
use tempfile::TempDir;

fn open_small_cache_env(
    dir: &std::path::Path,
    cache_bytes: u64,
) -> (Environment, Database) {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf());
    cfg.set_allow_create(true);
    cfg.set_transactional(true);
    cfg.set_cache_percent(0); // so set_cache_size takes effect
    cfg.set_cache_size(cache_bytes);
    let env = Environment::open(cfg).expect("open env");
    let db = env
        .open_database(
            None,
            "evict",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .expect("open db");
    (env, db)
}

/// With a cache far smaller than the working set, after inserting many records
/// and running eviction the cache_usage must be bounded (eviction actually
/// reduces it) AND every record must still be readable (correctness preserved).
#[test]
fn eviction_bounds_cache_and_preserves_data() {
    let dir = TempDir::new().unwrap();
    // 2 MiB cache; ~50k records * ~120 B = ~6 MB working set -> must evict.
    let (env, db) = open_small_cache_env(dir.path(), 2 * 1024 * 1024);

    let n = 50_000usize;
    let val = vec![0u8; 100];
    for i in 0..n {
        let k = DatabaseEntry::from_vec(format!("{:010}", i).into_bytes());
        let v = DatabaseEntry::from_bytes(&val);
        db.put(&k, &v).unwrap();
    }

    // Run eviction explicitly (the daemon also runs, but make it deterministic).
    let _ = env.evict_memory().unwrap();

    // F2: eviction must have reduced cache_usage. With a 2 MiB cache and a
    // ~6 MB working set, usage must not be wildly above the budget. We assert
    // it is at least bounded below the full working set (i.e. eviction did
    // something) — a non-evicting (inert) evictor would let usage grow to the
    // full ~6 MB.
    let stats = env.stats().unwrap();
    assert!(
        stats.cache_usage < 6 * 1024 * 1024,
        "eviction must bound cache_usage below the full working set; got {} bytes",
        stats.cache_usage
    );

    // Correctness: every record is still readable (eviction must not lose data
    // — evicted nodes are recoverable from the log).
    for i in (0..n).step_by(97) {
        let k = DatabaseEntry::from_vec(format!("{:010}", i).into_bytes());
        let mut out = DatabaseEntry::new();
        let st = db.get_into(None, &k, &mut out).unwrap();
        assert!(st, "record {} must survive eviction", i);
        assert_eq!(out.data(), &val[..], "record {} data intact", i);
    }
}

/// A delete-heavy workload under a small cache must not make cache_usage drift
/// upward unboundedly (F8: delete subtracts key+data+48, not just key+48).
#[test]
fn delete_heavy_does_not_inflate_cache_usage() {
    let dir = TempDir::new().unwrap();
    let (env, db) = open_small_cache_env(dir.path(), 4 * 1024 * 1024);

    let val = vec![0u8; 100];
    // Insert then delete the same keys many times. With the F8 leak, each
    // delete would under-subtract by data_len (100B), inflating cache_usage.
    for round in 0..20 {
        for i in 0..2_000usize {
            let k = DatabaseEntry::from_vec(
                format!("r{}-{:08}", round % 2, i).into_bytes(),
            );
            db.put(&k, DatabaseEntry::from_bytes(&val)).unwrap();
        }
        for i in 0..2_000usize {
            let k = DatabaseEntry::from_vec(
                format!("r{}-{:08}", round % 2, i).into_bytes(),
            );
            let _ = db.delete(&k);
        }
    }
    let _ = env.evict_memory().unwrap();
    let stats = env.stats().unwrap();
    // After 40k inserts + 40k deletes of a ~2k-key working set, usage must stay
    // bounded (the live set is small). A data_len leak on delete would make
    // this grow without bound across rounds.
    assert!(
        stats.cache_usage < 8 * 1024 * 1024,
        "delete-heavy churn must not inflate cache_usage (F8); got {} bytes",
        stats.cache_usage
    );
}

/// A full cursor scan over a working set larger than the cache must return the
/// correct data for EVERY record (the scan path must re-hydrate stripped LNs
/// from the log, not return empty data). Validates the scan-path fetchTarget.
#[test]
fn cursor_scan_under_eviction_returns_all_data() {
    use noxu_db::Get;
    let dir = TempDir::new().unwrap();
    let (env, db) = open_small_cache_env(dir.path(), 2 * 1024 * 1024);

    let n = 20_000usize;
    let val = vec![7u8; 80];
    for i in 0..n {
        let k = DatabaseEntry::from_vec(format!("{:010}", i).into_bytes());
        db.put(&k, DatabaseEntry::from_bytes(&val)).unwrap();
    }
    let _ = env.evict_memory().unwrap();

    // Scan the whole database with a cursor; every record's data must be the
    // full 80-byte value, never empty (which would mean a stripped LN was not
    // re-fetched from the log on the scan path).
    let mut cursor = db.open_cursor(None).unwrap();
    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let mut count = 0usize;
    let mut st = cursor.get(&mut key, &mut data, Get::First, None).unwrap();
    while st == OperationStatus::Success {
        assert_eq!(
            data.data(),
            &val[..],
            "scanned record {} ({:?}) must have full data, not stripped/empty",
            count,
            String::from_utf8_lossy(key.data())
        );
        count += 1;
        st = cursor.get(&mut key, &mut data, Get::Next, None).unwrap();
    }
    assert_eq!(count, n, "scan must visit every record");
}

/// Regression: a SYNC batched bulk-load of a dataset FAR larger than the cache
/// must complete (load + final checkpoint) in bounded time. Before the evictor
/// log-and-evict fix, a dirty BIN that could not be LN-stripped was put back
/// on the LRU forever (deferred to the checkpoint), so under dataset >> cache
/// the evictor spun putting dirty BINs back while the checkpoint could not keep
/// up — the post-load checkpoint never completed (observed: >40 min hang on a
/// 64-core host at ~3.4x cache). The evictor now logs-and-evicts a dirty BIN
/// once it has had its second chance, reclaiming its full memory in one pass,
/// so eviction makes bounded progress and the checkpoint completes.
///
/// The watchdog thread panics the process if the operation does not finish
/// within the bound, turning an infinite thrash into a test FAILURE.
#[test]
fn large_dataset_sync_load_and_checkpoint_completes() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    let dir = TempDir::new().unwrap();
    // 8 MiB cache, ~40 MiB working set (~5x cache) — small enough to run
    // quickly in CI but large enough that eviction must fire during the load
    // and the final checkpoint must flush a dirty set larger than the cache.
    let mut cfg = EnvironmentConfig::new(dir.path().to_path_buf());
    cfg.set_allow_create(true);
    cfg.set_transactional(true);
    cfg.set_cache_percent(0);
    cfg.set_cache_size(8 * 1024 * 1024);
    // COMMIT_SYNC (the default) — the durability under which the thrash was
    // observed.
    let env = Environment::open(cfg).expect("open env");
    let db = env
        .open_database(
            None,
            "big",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .expect("open db");

    let done = Arc::new(AtomicBool::new(false));
    let watch = Arc::clone(&done);
    // Generous bound: this workload completes in a few seconds when eviction
    // makes progress; 180s means it is thrashing (the bug).
    let watchdog = std::thread::spawn(move || {
        for _ in 0..180 {
            std::thread::sleep(std::time::Duration::from_secs(1));
            if watch.load(Ordering::Relaxed) {
                return;
            }
        }
        panic!(
            "large-dataset SYNC load+checkpoint did not complete in 180s — \
             evictor is thrashing (dirty BINs deferred to checkpoint forever)"
        );
    });

    let n: u64 = 200_000; // ~40 MiB at 200B values
    let val = vec![0x56u8; 200];
    let mut i = 0u64;
    while i < n {
        let batch_end = (i + 1000).min(n);
        let txn = env.begin_transaction(None).unwrap();
        for j in i..batch_end {
            db.put_in(&txn, j.to_be_bytes(), &val).unwrap();
        }
        txn.commit().unwrap();
        i = batch_end;
    }
    // The final checkpoint is where the thrash manifested (flushing a dirty
    // set larger than the cache while the evictor competes).
    env.checkpoint(None).unwrap();
    done.store(true, Ordering::Relaxed);
    watchdog.join().unwrap();

    // Sanity: a sampling of records is still readable after the pressure.
    for k in [0u64, n / 2, n - 1] {
        assert!(
            db.get(k.to_be_bytes()).unwrap().is_some(),
            "record {k} lost after large-dataset load"
        );
    }
    db.close().unwrap();
    env.close().unwrap();
}

/// Stage B (LN read-cache): a read of an evicted (LN-stripped) record must
/// re-populate the BIN slot so the NEXT read hits memory, AND the
/// re-population must go through the memory budget so repeated
/// read-then-evict cycles keep `cache_usage` BOUNDED (no unbounded cache
/// growth). Also proves read-consistency: a re-populated-slot read returns
/// the same bytes a cold fetch does.
#[test]
fn repopulated_read_is_consistent_and_budget_bounded() {
    let dir = TempDir::new().unwrap();
    // 2 MiB cache; ~30k * ~120 B = ~3.6 MB working set -> eviction strips LNs.
    let (env, db) = open_small_cache_env(dir.path(), 2 * 1024 * 1024);

    let n = 30_000usize;
    // Distinct value per key so a wrong/stale re-populate would be caught.
    let make_val = |i: usize| -> Vec<u8> {
        let mut v = vec![0u8; 100];
        v[..4].copy_from_slice(&(i as u32).to_be_bytes());
        v
    };
    for i in 0..n {
        let k = DatabaseEntry::from_vec(format!("{:010}", i).into_bytes());
        db.put(&k, DatabaseEntry::from_bytes(&make_val(i))).unwrap();
    }
    // Force LN stripping: cache << working set.
    let _ = env.evict_memory().unwrap();

    let read = |i: usize| -> Vec<u8> {
        let k = DatabaseEntry::from_vec(format!("{:010}", i).into_bytes());
        let mut out = DatabaseEntry::new();
        assert!(db.get_into(None, &k, &mut out).unwrap(), "record {i} present");
        out.data().to_vec()
    };

    // Repeated read-then-evict cycles. Each cycle: read a sample (cold fetch
    // -> re-populate), read the SAME keys again (should hit the re-populated
    // slot), then evict again (strips the re-populated LNs). Track cache_usage
    // to prove it does not grow without bound.
    let sample: Vec<usize> = (0..n).step_by(53).collect();
    let mut max_usage = 0u64;
    for cycle in 0..8 {
        for &i in &sample {
            // First read: may cold-fetch and re-populate.
            let a = read(i);
            // Second read: must be identical (re-populated slot or same cold
            // fetch -- either way byte-identical to the on-disk LN).
            let b = read(i);
            assert_eq!(a, b, "cycle {cycle} key {i}: two reads must agree");
            assert_eq!(
                a,
                make_val(i),
                "cycle {cycle} key {i}: read must return the correct value"
            );
        }
        // Re-strip the LNs the reads just re-populated.
        let _ = env.evict_memory().unwrap();
        let usage = env.stats().unwrap().cache_usage;
        max_usage = max_usage.max(usage);
    }

    // Budget-safety: across 8 read-then-evict cycles the peak usage must stay
    // bounded well below the full working set. If re-population bypassed the
    // budget, cache_usage would ratchet up every cycle (each re-populate adds
    // data bytes the evictor could never reclaim) and blow past this bound.
    assert!(
        max_usage < 6 * 1024 * 1024,
        "repeated read-then-evict must keep cache_usage bounded; peaked at {} bytes",
        max_usage
    );
}

/// CacheMode.DEFAULT keep-hot proof (JE Evictor.moveBack via IN.fetchTarget).
///
/// The regression this guards: `Tree::search_with_data` (the cursor
/// `get`/`search` fast-path) did not move the reached BIN to the hot end of
/// the evictor LRU on a read.  Under budget pressure the evictor therefore
/// could not distinguish a hot Zipfian BIN from a cold one and stripped hot
/// LNs that were re-read immediately, forcing a log re-read
/// (`fetch_ln_data_from_log` -> CRC + parse) on every access -- i.e. JE's
/// EVICT_LN behaviour, not DEFAULT.
///
/// The proof: hammer a SMALL hot set that fits the cache while a much larger
/// cold set churns the evictor.  After warm-up, repeated hot reads must stop
/// hitting the log -- `n_random_reads` (the log point-lookup counter added by
/// the lead-benchmarks work) must climb only marginally during the hot-read
/// phase.  Without the LRU touch the hot BINs are stripped between reads and
/// `n_random_reads` climbs ~1 per hot read.
#[test]
fn default_cache_mode_keeps_hot_lns_resident() {
    let dir = TempDir::new().unwrap();
    // 6 MiB cache. Hot set ~200 keys * ~120 B = ~24 KB (fits trivially).
    // Cold set ~60k keys * ~120 B = ~7.2 MB (> cache) so the evictor fires
    // and MUST strip something on every pass -- the question is WHICH LNs.
    let (env, db) = open_small_cache_env(dir.path(), 6 * 1024 * 1024);

    let cold_n = 60_000usize;
    let hot: Vec<usize> = (0..200).map(|i| i * 251).collect(); // spread
    let val = vec![0x5au8; 100];
    for i in 0..cold_n {
        let k = DatabaseEntry::from_vec(format!("{:010}", i).into_bytes());
        db.put(&k, DatabaseEntry::from_bytes(&val)).unwrap();
    }

    let read = |i: usize| {
        let k = DatabaseEntry::from_vec(format!("{:010}", i).into_bytes());
        let mut out = DatabaseEntry::new();
        assert!(db.get_into(None, &k, &mut out).unwrap(), "key {i} present");
    };

    // Warm-up: read the hot keys several times so their BINs are resident and
    // freshly at the hot end of the LRU.  Interleave a light cold sweep so the
    // evictor runs and the LRU order is exercised.
    for _ in 0..20 {
        for &h in &hot {
            read(h);
        }
    }
    let _ = env.evict_memory().unwrap();
    for _ in 0..20 {
        for &h in &hot {
            read(h);
        }
    }

    // Measure phase: alternate hot-read bursts with cold pressure.  We snapshot
    // the log random-read counter ONLY around the hot bursts, so cold-window
    // faults (which are legitimate -- cold data is not in cache) are excluded.
    // Ordering per round: apply cold pressure + evict FIRST, then read the hot
    // set and measure.  With DEFAULT keep-hot the just-touched hot BINs are at
    // the hot end of the LRU, so the eviction pass strips cold BINs and leaves
    // the hot ones resident -> the hot burst faults ~0 times.  Without the LRU
    // touch the hot BINs are indistinguishable from cold and get stripped, so
    // each hot read re-faults (~1 log random read per hot read).
    let hot_read_rounds = 30usize;
    let mut hot_faults = 0u64;
    for round in 0..hot_read_rounds {
        // Cold pressure BEFORE the measured hot burst: touch a rotating cold
        // window (these faults are expected and NOT measured) and evict.  This
        // leaves the hot BINs as the coldest-touched-longest-ago candidates
        // UNLESS the read path keeps them hot -- which is exactly what we test.
        let base = (round * 997) % cold_n;
        for j in 0..500 {
            read((base + j) % cold_n);
        }
        let _ = env.evict_memory().unwrap();

        // Measured hot burst: read every hot key and count log random reads
        // attributable to just these reads.
        let before = env.stats().unwrap().log.n_random_reads;
        for &h in &hot {
            read(h);
        }
        let after = env.stats().unwrap().log.n_random_reads;
        hot_faults += after.saturating_sub(before);
    }
    let hot_reads_total = (hot_read_rounds * hot.len()) as u64;

    // Keep-hot invariant: the HOT reads must almost never fault from the log.
    // 30 rounds * 200 keys = 6000 hot reads.  With keep-hot the hot BINs stay
    // resident so `hot_faults` is a small fraction of `hot_reads_total`
    // (only the first touch after a rare hot-BIN eviction faults).  Without
    // the LRU touch every hot read re-faults and `hot_faults` ~= 6000.
    // Assert < 20% fault rate -- comfortably distinguishes keep-hot (~0-5%)
    // from EVICT_LN (~100%).
    assert!(
        hot_faults < hot_reads_total / 5,
        "keep-hot: hot reads must not re-fault from the log every access; \
         {hot_faults} of {hot_reads_total} hot reads faulted (EVICT_LN \
         behaviour would fault ~{hot_reads_total})"
    );
}
