// Copyright (C) 2024-2025 Greg Burd.  Apache-2.0 OR MIT.
//! WRITE-PATH-AT-SCALE diagnostic (not a CI test — run with --ignored --nocapture).
//!
//! Reproduces the AWS Phase-1 finding (JE 1.3-1.8x faster on writes once
//! dataset > cache) locally by FORCING the over-cache regime with a tiny cache,
//! then attributes the per-write cost to a subsystem (re-fetch / eviction /
//! cleaner / checkpoint) via the engine's own stat counters — to answer
//! "where is Noxu different from JE in a way that impacts write throughput
//! when dataset > cache?".
//!
//! Run:  cargo test -p noxu-db --test write_scale_probe -- --ignored --nocapture

use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
use std::time::Instant;

fn scratch(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "noxu-wsp-{}-{}",
        tag,
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

#[test]
#[ignore = "diagnostic; run with --ignored --nocapture"]
fn write_scale_probe() {
    // Tiny cache (4 MiB) so even a modest dataset far exceeds it — the
    // over-cache regime the AWS run hit at 1M records, reproduced cheaply.
    const CACHE: u64 = 4 * 1024 * 1024;
    let value = vec![0xABu8; 64];

    println!(
        "\n=== write-path-at-scale probe (cache = {} MiB) ===",
        CACHE / 1024 / 1024
    );
    println!(
        "{:>9} {:>10} {:>12} {:>12} {:>12} {:>12} {:>10} {:>10}",
        "records",
        "ns/write",
        "binFetch",
        "binMiss",
        "evicted",
        "evRuns",
        "clnRuns",
        "ckpts"
    );

    for &n in &[100_000usize, 500_000, 1_000_000, 2_000_000] {
        let dir = scratch(&format!("n{n}"));
        let env = Environment::open(
            EnvironmentConfig::new(dir.clone())
                .with_transactional(true)
                .with_allow_create(true)
                .with_cache_size(CACHE),
        )
        .unwrap();
        let db = env
            .open_database(
                None,
                "wsp",
                &DatabaseConfig::new().with_allow_create(true),
            )
            .unwrap();

        let s0 = env.stats().unwrap();
        let t0 = Instant::now();
        // Sequential auto-commit writes — the w01_seq_write pattern.
        for i in 0..n {
            let key = (i as u64).to_be_bytes();
            db.put(key, &value).unwrap();
        }
        let elapsed = t0.elapsed();
        let s1 = env.stats().unwrap();

        let ns_per = elapsed.as_nanos() as f64 / n as f64;
        let bf = s1.evictor.bin_fetch.saturating_sub(s0.evictor.bin_fetch);
        let bm =
            s1.evictor.bin_fetch_miss.saturating_sub(s0.evictor.bin_fetch_miss);
        let ev =
            s1.evictor.nodes_evicted.saturating_sub(s0.evictor.nodes_evicted);
        let er =
            s1.evictor.eviction_runs.saturating_sub(s0.evictor.eviction_runs);
        let cln = s1.cleaner.runs.saturating_sub(s0.cleaner.runs);
        let ckp =
            s1.checkpoint.checkpoints.saturating_sub(s0.checkpoint.checkpoints);

        println!(
            "{:>9} {:>10.0} {:>12} {:>12} {:>12} {:>12} {:>10} {:>10}",
            n, ns_per, bf, bm, ev, er, cln, ckp
        );

        // Per-write attribution: fetch-misses and evictions per write are the
        // key ratios. If they grow super-linearly with n, that subsystem is the
        // scale cost.
        println!(
            "          per-write: binMiss={:.3}  evicted={:.3}  cacheUsage={}MiB/{}MiB",
            bm as f64 / n as f64,
            ev as f64 / n as f64,
            s1.cache_usage / 1024 / 1024,
            s1.cache_size / 1024 / 1024,
        );

        db.close().unwrap();
        env.close().unwrap();
        let _ = std::fs::remove_dir_all(&dir);
    }
}
