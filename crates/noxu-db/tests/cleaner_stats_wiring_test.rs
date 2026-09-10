//! `CleanerStats`' disk-usage and utilization fields must report REAL values.
//!
//! Six fields on `CleanerStats` were never written by production code — the only
//! stores lived in `cleaner_stat.rs`'s own test module — so `env.stats()` and the
//! `noxu-observe` Prometheus gauges derived from them read 0 forever. An operator
//! watching those gauges to judge whether the cleaner was keeping up saw zeros
//! regardless of what the cleaner was doing.
//!
//! Four of the six are now populated by `Cleaner::publish_disk_usage_stats`,
//! called from `do_clean`. The other two were REMOVED rather than wired, because
//! no honest source exists: JE's own `getNCleanerProbeRuns()` is deprecated and
//! always returns zero, and `repeat_iterator_reads` counts a buffer-regrow
//! mechanism our exact-sized cleaner reader structurally cannot exhibit. See
//! `.agent/notes-cleanerstats.md` for the per-field determination.
//!
//! This test asserts the values are non-zero AND plausible. Asserting only
//! "non-zero" would pass against a hardcoded 1, and asserting a `store`/`load`
//! round-trip would test nothing at all.

use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
use tempfile::TempDir;

#[test]
fn cleaning_publishes_real_disk_usage_and_utilization_stats() {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path();
    let value = vec![0xABu8; 512];
    let n = 400u32;

    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        // Small files so a few hundred records span many of them, giving the
        // utilization aggregate something real to average over.
        .with_log_file_max_bytes(64 * 1024)
        .with_cache_size(16 * 1024 * 1024);
    cfg.set_run_cleaner(false); // drive cleaning explicitly, no daemon race
    cfg.set_run_checkpointer(false);
    let env = Environment::open(cfg).unwrap();
    let db = env
        .open_database(
            None,
            "stats",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .unwrap();

    // Load, then churn so a meaningful fraction of the log is obsolete.
    for i in 0..n {
        db.put(format!("k{i:06}").as_bytes(), &value).unwrap();
    }
    for _ in 0..3 {
        for i in 0..n {
            db.put(format!("k{i:06}").as_bytes(), &value).unwrap();
        }
    }
    env.checkpoint(None).unwrap();

    // Before cleaning has run, these are expected to be untouched. This is the
    // fails-on-base half: on the unfixed code they stay 0 forever, including
    // after clean_log() below.
    env.clean_log().unwrap();

    let stats = env.stats().unwrap();
    let c = &stats.cleaner;

    // Disk usage must reflect bytes that actually exist on disk. Compare against
    // the real directory size rather than a magic constant.
    let on_disk: u64 = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map(|x| x == "ndb").unwrap_or(false))
        .filter_map(|e| e.metadata().ok())
        .map(|m| m.len())
        .sum();
    assert!(on_disk > 0, "precondition: the test wrote some .ndb files");

    assert!(
        c.total_log_size > 0,
        "total_log_size must be published by do_clean; 0 means the stat is \
         still unwired (env.stats() and the Prometheus gauge read 0 forever)"
    );
    // Within an order of magnitude of the real directory size — loose enough to
    // tolerate files created/deleted between the two observations, tight enough
    // that a hardcoded or unit-confused value fails.
    assert!(
        c.total_log_size >= on_disk / 4 && c.total_log_size <= on_disk * 4,
        "total_log_size ({}) is not plausibly the on-disk log size ({on_disk})",
        c.total_log_size
    );
    assert!(
        c.active_log_size > 0,
        "active_log_size must be published by do_clean"
    );

    // Utilization is a PERCENTAGE and must be in range. These are computed
    // statistics (JE UtilizationCalculator::getCurrentMin/MaxUtilization), not
    // the cleaner's min_utilization CONFIG threshold of the same name.
    assert!(
        c.min_utilization > 0 && c.min_utilization <= 100,
        "min_utilization ({}) must be a published percentage in 1..=100; 0 \
         means unwired",
        c.min_utilization
    );
    assert!(
        c.max_utilization > 0 && c.max_utilization <= 100,
        "max_utilization ({}) must be a published percentage in 1..=100; 0 \
         means unwired",
        c.max_utilization
    );
    assert!(
        c.min_utilization <= c.max_utilization,
        "min_utilization ({}) must not exceed max_utilization ({}) -- the two \
         bounds are computed from the same aggregate pass and are ordered by \
         construction, so this inversion means the obsolete bounds were swapped",
        c.min_utilization,
        c.max_utilization
    );

    drop(db);
    env.close().unwrap();
}
