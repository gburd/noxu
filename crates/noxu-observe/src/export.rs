// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Built-in metrics export over the [`metrics`](https://docs.rs/metrics)
//! facade.
//!
//! This mirrors BDB-JE's read-only JMX MBean export (`JEMonitor` /
//! `EnvironmentStats`): a recorder periodically samples the same
//! `EnvironmentStats` snapshot that `Environment::get_stats()` returns and
//! publishes each field as a gauge or counter. Because it goes through the
//! `metrics` facade, any installed recorder (Prometheus, StatsD,
//! OpenTelemetry, a test recorder, …) collects them; with no recorder
//! installed the calls are cheap no-ops.
//!
//! ## JE stat-group mapping
//!
//! Every emitted metric maps 1:1 onto a field of [`EnvironmentStats`], which
//! in turn mirrors a JE `StatGroup` (`EnvironmentStats` → `EVICTOR_*`,
//! `LOGMGR_*` / `FILEMGR_*` / `FSYNCMGR_*`, `LOCK_*`, `Txn`, `Cleaner`,
//! `Checkpointer`, throughput `THROUGHPUT_PRI_*`). The cited group names in
//! each section below are the JE `*StatDefinition` classes the corresponding
//! Noxu snapshot type was ported from.

use metrics::{counter, gauge};
use noxu_engine::EnvironmentStats;

// ─── Metric names ────────────────────────────────────────────────────────────
//
// Names follow the existing `noxu_db_*` convention already used by the
// hot-path `observe_*` macros so a single recorder sees one coherent
// namespace. Cumulative quantities are counters (`*_total`); instantaneous
// quantities are gauges.

/// Describe every metric the periodic exporter emits. Call once after
/// installing a recorder so backends that support metadata (Prometheus HELP,
/// units) get descriptions. Safe to call when no recorder is installed.
pub fn describe_export_metrics() {
    use metrics::{Unit, describe_counter, describe_gauge};

    // ── Cache (JE EnvironmentStats: cacheTotalBytes / dataBytes) ──────────
    describe_gauge!(
        "noxu_cache_size_bytes",
        Unit::Bytes,
        "Configured cache budget"
    );
    describe_gauge!(
        "noxu_cache_usage_bytes",
        Unit::Bytes,
        "Current cache usage"
    );
    describe_gauge!(
        "noxu_cache_utilization_ratio",
        Unit::Percent,
        "cache_usage / cache_size (0.0–1.0+)"
    );

    // ── Evictor (JE EvictorStatDefinition) ────────────────────────────────
    describe_counter!(
        "noxu_evictor_runs_total",
        Unit::Count,
        "EVICTOR_EVICTION_RUNS"
    );
    describe_counter!(
        "noxu_evictor_nodes_evicted_total",
        Unit::Count,
        "EVICTOR_NODES_EVICTED"
    );
    describe_counter!(
        "noxu_evictor_bytes_evicted_total",
        Unit::Bytes,
        "EVICTOR_*_BYTES"
    );
    describe_counter!(
        "noxu_evictor_bin_fetch_total",
        Unit::Count,
        "EVICTOR_BIN_FETCH"
    );
    describe_counter!(
        "noxu_evictor_bin_fetch_miss_total",
        Unit::Count,
        "EVICTOR_BIN_FETCH_MISS"
    );
    describe_gauge!(
        "noxu_evictor_cache_hit_ratio",
        Unit::Percent,
        "1 - binFetchMiss/binFetch"
    );
    describe_gauge!(
        "noxu_evictor_lru_size",
        Unit::Count,
        "EVICTOR_PRI1_LRU_SIZE + PRI2"
    );

    // ── Log / FileManager / FsyncManager (JE LogStatDefinition) ───────────
    describe_counter!(
        "noxu_log_fsyncs_total",
        Unit::Count,
        "FSYNCMGR_FSYNCS (NLogFSyncs)"
    );
    describe_counter!(
        "noxu_log_fsync_requests_total",
        Unit::Count,
        "FSYNCMGR_FSYNC_REQUESTS"
    );
    describe_counter!(
        "noxu_log_group_commits_total",
        Unit::Count,
        "FSYNCMGR_N_GROUP_COMMIT_REQUESTS"
    );
    describe_counter!(
        "noxu_log_fsync_batch_size_sum",
        Unit::Count,
        "Sum of group-commit batch sizes (NFSyncBatchSize numerator)"
    );
    describe_counter!(
        "noxu_log_bytes_written_total",
        Unit::Bytes,
        "FILEMGR_SEQUENTIAL_WRITE_BYTES"
    );
    describe_counter!(
        "noxu_log_bytes_read_total",
        Unit::Bytes,
        "FILEMGR_*_READ_BYTES"
    );
    describe_gauge!(
        "noxu_log_end_of_log_lsn",
        Unit::Count,
        "LOGMGR end-of-log LSN (raw u64)"
    );
    describe_gauge!(
        "noxu_log_last_flush_lsn",
        Unit::Count,
        "LOGMGR last completed flush LSN"
    );

    // ── Lock manager (JE LockStatDefinition) ──────────────────────────────
    describe_counter!("noxu_lock_requests_total", Unit::Count, "LOCK_REQUESTS");
    describe_counter!("noxu_lock_waits_total", Unit::Count, "LOCK_WAITS");
    describe_counter!(
        "noxu_lock_timeouts_total",
        Unit::Count,
        "LOCK_*_TIMEOUTS"
    );
    describe_gauge!("noxu_lock_total_locks", Unit::Count, "LOCK_TOTAL");
    describe_gauge!("noxu_lock_waiters", Unit::Count, "LOCK_WAITERS");

    // ── Transactions (JE Txn stat group) ──────────────────────────────────
    describe_counter!("noxu_txn_begins_total", Unit::Count, "nBegins");
    describe_counter!("noxu_txn_commits_total", Unit::Count, "nCommits");
    describe_counter!("noxu_txn_aborts_total", Unit::Count, "nAborts");
    describe_gauge!("noxu_txn_active", Unit::Count, "nActive");

    // ── Cleaner (JE CleanerStatDefinition) ────────────────────────────────
    describe_counter!("noxu_cleaner_runs_total", Unit::Count, "CLEANER_RUNS");
    describe_counter!(
        "noxu_cleaner_files_deleted_total",
        Unit::Count,
        "CLEANER_DELETIONS"
    );
    describe_gauge!(
        "noxu_cleaner_min_utilization",
        Unit::Percent,
        "CLEANER_MIN_UTILIZATION"
    );
    describe_gauge!(
        "noxu_cleaner_backlog",
        Unit::Count,
        "CLEANER_PENDING_LN_QUEUE_SIZE"
    );
    describe_gauge!(
        "noxu_cleaner_total_log_size_bytes",
        Unit::Bytes,
        "CLEANER_TOTAL_LOG_SIZE"
    );
    describe_gauge!(
        "noxu_cleaner_active_log_size_bytes",
        Unit::Bytes,
        "CLEANER_ACTIVE_LOG_SIZE"
    );

    // ── Checkpointer (JE CheckpointStatDefinition) ────────────────────────
    describe_counter!(
        "noxu_checkpoint_count_total",
        Unit::Count,
        "CKPT_CHECKPOINTS"
    );
    describe_gauge!(
        "noxu_checkpoint_last_interval_ms",
        Unit::Milliseconds,
        "CKPT_LAST_CKPT_INTERVAL"
    );
    describe_gauge!("noxu_checkpoint_last_id", Unit::Count, "CKPT_LAST_CKPTID");

    // ── Throughput (JE THROUGHPUT_PRI_*) ──────────────────────────────────
    describe_counter!(
        "noxu_db_pri_inserts_total",
        Unit::Count,
        "THROUGHPUT_PRI_INSERT"
    );
    describe_counter!(
        "noxu_db_pri_updates_total",
        Unit::Count,
        "THROUGHPUT_PRI_UPDATE"
    );
    describe_counter!(
        "noxu_db_pri_deletes_total",
        Unit::Count,
        "THROUGHPUT_PRI_DELETE"
    );
    describe_counter!(
        "noxu_db_pri_searches_total",
        Unit::Count,
        "THROUGHPUT_PRI_SEARCH"
    );

    describe_gauge!("noxu_databases_open", Unit::Count, "Open database count");
}

/// Emit one sample of `stats` to the `metrics` facade.
///
/// Counters are set via [`metrics::counter!`]`.absolute(..)` because the
/// snapshot already holds the cumulative-since-open value (the same number a
/// JMX scrape of `EnvironmentStats` would report); the recorder computes
/// rates. Gauges hold the instantaneous reading.
pub fn emit(stats: &EnvironmentStats) {
    // ── Cache ─────────────────────────────────────────────────────────────
    gauge!("noxu_cache_size_bytes").set(stats.cache_size as f64);
    gauge!("noxu_cache_usage_bytes").set(stats.cache_usage as f64);
    gauge!("noxu_cache_utilization_ratio")
        .set(stats.cache_utilization_percent() / 100.0);

    // ── Evictor ───────────────────────────────────────────────────────────
    let e = &stats.evictor;
    counter!("noxu_evictor_runs_total").absolute(e.eviction_runs);
    counter!("noxu_evictor_nodes_evicted_total").absolute(e.nodes_evicted);
    counter!("noxu_evictor_bytes_evicted_total").absolute(e.bytes_evicted);
    counter!("noxu_evictor_bin_fetch_total").absolute(e.bin_fetch);
    counter!("noxu_evictor_bin_fetch_miss_total").absolute(e.bin_fetch_miss);
    gauge!("noxu_evictor_cache_hit_ratio")
        .set(1.0 - stats.bin_fetch_miss_ratio());
    gauge!("noxu_evictor_lru_size").set(e.lru_size as f64);

    // ── Log ───────────────────────────────────────────────────────────────
    let l = &stats.log;
    counter!("noxu_log_fsyncs_total").absolute(l.n_log_fsyncs);
    counter!("noxu_log_fsync_requests_total").absolute(l.n_fsync_requests);
    counter!("noxu_log_group_commits_total").absolute(l.n_group_commits);
    counter!("noxu_log_fsync_batch_size_sum")
        .absolute(l.n_fsync_batch_size_sum);
    counter!("noxu_log_bytes_written_total")
        .absolute(l.n_sequential_write_bytes);
    counter!("noxu_log_bytes_read_total")
        .absolute(l.n_sequential_read_bytes + l.n_random_read_bytes);
    gauge!("noxu_log_end_of_log_lsn").set(l.end_of_log as f64);
    gauge!("noxu_log_last_flush_lsn").set(l.last_flush_lsn as f64);

    // ── Lock ──────────────────────────────────────────────────────────────
    let k = &stats.lock;
    counter!("noxu_lock_requests_total").absolute(k.n_requests);
    counter!("noxu_lock_waits_total").absolute(k.n_waits);
    counter!("noxu_lock_timeouts_total").absolute(k.n_lock_timeouts);
    gauge!("noxu_lock_total_locks").set(k.n_total_locks as f64);
    gauge!("noxu_lock_waiters").set(k.n_waiters as f64);

    // ── Txn ───────────────────────────────────────────────────────────────
    let t = &stats.txn;
    counter!("noxu_txn_begins_total").absolute(t.n_begins);
    counter!("noxu_txn_commits_total").absolute(t.n_commits);
    counter!("noxu_txn_aborts_total").absolute(t.n_aborts);
    gauge!("noxu_txn_active").set(t.n_active as f64);

    // ── Cleaner ───────────────────────────────────────────────────────────
    let c = &stats.cleaner;
    counter!("noxu_cleaner_runs_total").absolute(c.runs);
    counter!("noxu_cleaner_files_deleted_total").absolute(c.deletions);
    gauge!("noxu_cleaner_min_utilization").set(c.min_utilization as f64);
    gauge!("noxu_cleaner_backlog").set(c.pending_ln_queue_size as f64);
    gauge!("noxu_cleaner_total_log_size_bytes").set(c.total_log_size as f64);
    gauge!("noxu_cleaner_active_log_size_bytes").set(c.active_log_size as f64);

    // ── Checkpointer ──────────────────────────────────────────────────────
    let p = &stats.checkpoint;
    counter!("noxu_checkpoint_count_total").absolute(p.checkpoints);
    gauge!("noxu_checkpoint_last_interval_ms").set(p.last_ckpt_interval as f64);
    gauge!("noxu_checkpoint_last_id").set(p.last_ckpt_id as f64);

    // ── Throughput ────────────────────────────────────────────────────────
    let h = &stats.throughput;
    counter!("noxu_db_pri_inserts_total").absolute(h.n_pri_inserts);
    counter!("noxu_db_pri_updates_total").absolute(h.n_pri_updates);
    counter!("noxu_db_pri_deletes_total").absolute(h.n_pri_deletes);
    counter!("noxu_db_pri_searches_total").absolute(h.n_pri_searches);

    gauge!("noxu_databases_open").set(stats.n_databases as f64);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn emit_does_not_panic_without_recorder() {
        // No recorder installed: every facade call is a no-op.
        describe_export_metrics();
        emit(&EnvironmentStats::default());
    }
}
