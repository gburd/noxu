// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Integration test: the STATS_FILE_* periodic dump writes and rotates.

use noxu_db::environment::Environment;
use noxu_db::environment_config::EnvironmentConfig;
use std::time::{Duration, Instant};
use tempfile::TempDir;

/// With `stats_collect` enabled and a short interval, the dumper writes at
/// least one rotating CSV stats file into the configured directory.
#[test]
fn stats_file_is_written_and_rotates() {
    let env_dir = TempDir::new().unwrap();
    let stats_dir = TempDir::new().unwrap();

    let config = EnvironmentConfig::new(env_dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        .with_stats_collect(true)
        // Sample every second (min enforced); tiny row_count so we rotate fast.
        .with_stats_collect_interval_secs(1)
        .with_stats_file_row_count(2)
        .with_stats_max_files(3)
        .with_stats_file_directory(stats_dir.path().to_path_buf());

    let env = Environment::open(config).unwrap();

    // Wait until at least a couple of stats files exist (samples land ~1s
    // apart; row_count=2 means a new file every ~2 samples). Poll up to 10 s.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut n_files = 0usize;
    while Instant::now() < deadline {
        n_files = std::fs::read_dir(stats_dir.path())
            .unwrap()
            .flatten()
            .filter(|e| {
                e.path()
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("noxu.stat.") && n.ends_with(".csv"))
                    .unwrap_or(false)
            })
            .count();
        if n_files >= 2 {
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    env.close().unwrap();

    assert!(
        n_files >= 1,
        "expected at least one stats file to be written, found {n_files}"
    );

    // At most stats_max_files (3) should be retained.
    let final_files = std::fs::read_dir(stats_dir.path())
        .unwrap()
        .flatten()
        .filter(|e| {
            e.path()
                .file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with("noxu.stat.") && n.ends_with(".csv"))
                .unwrap_or(false)
        })
        .count();
    assert!(
        final_files <= 3,
        "stats_max_files=3 must bound retained files, found {final_files}"
    );

    // Each stats file must start with the CSV header.
    for e in std::fs::read_dir(stats_dir.path()).unwrap().flatten() {
        let name = e.file_name();
        let name = name.to_str().unwrap();
        if name.starts_with("noxu.stat.") && name.ends_with(".csv") {
            let contents = std::fs::read_to_string(e.path()).unwrap();
            assert!(
                contents.starts_with("time_ms,"),
                "stats file {name} must start with the CSV header"
            );
        }
    }
}

/// When `stats_collect` is off (the default), no stats file is written.
#[test]
fn no_stats_file_when_collection_disabled() {
    let env_dir = TempDir::new().unwrap();
    let stats_dir = TempDir::new().unwrap();
    let config = EnvironmentConfig::new(env_dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        .with_stats_file_directory(stats_dir.path().to_path_buf());
    // stats_collect defaults to false.
    let env = Environment::open(config).unwrap();
    std::thread::sleep(Duration::from_millis(300));
    env.close().unwrap();

    let n = std::fs::read_dir(stats_dir.path()).unwrap().flatten().count();
    assert_eq!(n, 0, "no stats files should be written when collection is off");
}
