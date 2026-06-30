// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Headline test for the built-in metrics export (the `observability`
//! feature).
//!
//! Installs a real `metrics` recorder (`metrics_util::DebuggingRecorder`,
//! proving the export is recorder-agnostic), runs a workload (puts / gets /
//! commits / a checkpoint), samples `get_stats()` through the exporter, and
//! asserts the expected JE-stat-derived gauges/counters were emitted with
//! sane values.
//!
//! The whole file is `cfg`-gated on `observability`; with the feature off it
//! compiles to nothing and the engine pulls no metrics crates.
#![cfg(feature = "observability")]

use std::collections::HashMap;

use metrics_util::debugging::{DebugValue, DebuggingRecorder};
use noxu_db::metrics_export::MetricsExporter;
use noxu_db::{DatabaseConfig, DatabaseEntry, EnvironmentConfig};
use tempfile::TempDir;

/// Flatten a debugging snapshot to {metric_name -> f64}. Counters and gauges
/// only (no labels used by the periodic exporter).
fn snapshot_values(
    snapshotter: &metrics_util::debugging::Snapshotter,
) -> HashMap<String, f64> {
    snapshotter
        .snapshot()
        .into_vec()
        .into_iter()
        .map(|(ck, _unit, _desc, value)| {
            let name = ck.key().name().to_string();
            let v = match value {
                DebugValue::Counter(c) => c as f64,
                DebugValue::Gauge(g) => g.into_inner(),
                DebugValue::Histogram(h) => {
                    h.last().map(|x| x.into_inner()).unwrap_or(0.0)
                }
            };
            (name, v)
        })
        .collect()
}

#[test]
fn metrics_export_emits_je_stat_set() {
    // 1. Install a real recorder (recorder-agnostic facade).
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    recorder.install().expect("no other global recorder should be installed");

    // 2. Open a transactional environment.
    let dir = TempDir::new().unwrap();
    let env_config = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env =
        std::sync::Arc::new(noxu_db::Environment::open(env_config).unwrap());
    let db_config =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    let db = env.open_database(None, "metrics", &db_config).unwrap();

    // 3. Run a workload: N committed put transactions, then reads.
    const N: u64 = 50;
    for i in 0..N {
        let txn = env.begin_transaction(None).unwrap();
        let k = DatabaseEntry::from_bytes(&i.to_be_bytes());
        let v = DatabaseEntry::from_bytes(&[1, 2, 3, 4]);
        db.put(Some(&txn), &k, &v).unwrap();
        txn.commit().unwrap();
    }
    for i in 0..N {
        let k = DatabaseEntry::from_bytes(&i.to_be_bytes());
        let mut out = DatabaseEntry::new();
        let _ = db.get(None, &k, &mut out);
    }

    // 4. Force a checkpoint so checkpoint stats are non-zero.
    env.checkpoint(None).unwrap();

    // 5. Sample once synchronously (deterministic — no sleep needed).
    MetricsExporter::sample_once(&env);

    let m = snapshot_values(&snapshotter);

    // ── Assertions on the JE stat set ────────────────────────────────────

    // Txn (JE Txn group): commits == N, begins >= N.
    assert_eq!(
        m.get("noxu_txn_commits_total").copied(),
        Some(N as f64),
        "commits must equal the {N} committed txns; got {m:#?}"
    );
    assert!(
        m["noxu_txn_begins_total"] >= N as f64,
        "begins >= N: {}",
        m["noxu_txn_begins_total"]
    );

    // Log (JE FSYNCMGR/FILEMGR): fsyncs > 0 and bytes written > 0.
    assert!(
        m["noxu_log_fsyncs_total"] > 0.0,
        "fsyncs must be > 0 after committed writes: {}",
        m["noxu_log_fsyncs_total"]
    );
    assert!(
        m["noxu_log_bytes_written_total"] > 0.0,
        "log bytes written must be > 0"
    );

    // Cache (JE EnvironmentStats): size > 0; usage >= 0 and present.
    assert!(m["noxu_cache_size_bytes"] > 0.0, "cache budget must be > 0");
    assert!(m.contains_key("noxu_cache_usage_bytes"));
    assert!(m.contains_key("noxu_cache_utilization_ratio"));

    // Checkpointer (JE CheckpointStatDefinition): at least one checkpoint.
    assert!(
        m["noxu_checkpoint_count_total"] >= 1.0,
        "checkpoint count must be >= 1: {}",
        m["noxu_checkpoint_count_total"]
    );

    // Lock manager (JE LockStatDefinition): requests were made.
    assert!(
        m["noxu_lock_requests_total"] > 0.0,
        "lock requests must be > 0 after a transactional workload"
    );

    // Throughput (JE THROUGHPUT_PRI_*): the gauges are exported, but the
    // underlying per-DB throughput counters are not yet wired in the engine
    // (ThroughputStats is defined but never incremented on the write path —
    // a pre-existing gap, see docs/operations/monitoring.md). Assert the
    // metric is present rather than its value so the export stays honest
    // without faking a count the engine never produced.
    assert!(
        m.contains_key("noxu_db_pri_inserts_total"),
        "primary-insert throughput metric must be exported"
    );

    // Gauges present.
    assert_eq!(m.get("noxu_databases_open").copied(), Some(1.0));

    // Evictor group present (counts may be 0 with a small workload).
    assert!(m.contains_key("noxu_evictor_runs_total"));
    assert!(m.contains_key("noxu_evictor_cache_hit_ratio"));
}
