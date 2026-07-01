// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Periodic stats-file dump with rotation (`STATS_FILE_*`).
//!
//! A faithful analogue of BDB-JE's `StatCapture` background daemon (gated by
//! `STATS_COLLECT`): a daemon samples the environment's stats on the
//! configured interval and appends a CSV row to a rotating stats file.  After
//! `stats_file_row_count` rows the current file is closed and a new one
//! started; at most `stats_max_files` files are retained (the oldest are
//! pruned).  Files are written to `stats_file_directory` (default: the
//! environment home).
//!
//! JE ref: `EnvironmentParams.STATS_FILE_DIRECTORY` / `STATS_FILE_ROW_COUNT`
//! / `STATS_MAX_FILES`, `com.sleepycat.je.statcap.StatCapture`.
//!
//! Unlike the `metrics`-facade export (`crate::metrics_export`, opt-in behind
//! the `observability` feature), the stats file is a self-contained CSV that
//! needs no external recorder — it is aimed at simple ops/monitoring
//! (import into a spreadsheet, `tail -f`, etc.).

use crate::environment::build_environment_stats;
use noxu_dbi::EnvironmentImpl;
use noxu_log::LogManager;
use noxu_sync::Mutex;
use noxu_util::daemon::DaemonThread;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Base name (without the numeric suffix) of a rotating stats file.
const STATS_FILE_STEM: &str = "noxu.stat";
/// Extension used for stats files.
const STATS_FILE_EXT: &str = "csv";

/// The CSV header written at the top of every stats file.
const CSV_HEADER: &str = "time_ms,cache_size,cache_usage,n_databases,\
n_log_fsyncs,n_fsync_requests,n_group_commits,\
lock_n_requests,lock_n_waits,lock_n_total_locks,\
txn_n_begins,txn_n_commits,txn_n_aborts,txn_n_active\n";

/// Handle to the background stats-file dumping daemon.
///
/// Dropping the handle signals shutdown without blocking; call [`stop`] to
/// join the thread.
///
/// [`stop`]: StatsFileDumper::stop
pub struct StatsFileDumper {
    daemon: DaemonThread,
}

impl StatsFileDumper {
    /// Spawn the stats-file dumper daemon.
    ///
    /// * `env_impl` / `log_manager` / `cache_size` — the same handles
    ///   [`crate::environment::Environment::stats`] samples from.
    /// * `dir` — output directory (already resolved: env home if the caller
    ///   left `stats_file_directory` unset).
    /// * `interval` — sampling interval (from `stats_collect_interval_secs`).
    /// * `row_count` — rows per file before rotation (`stats_file_row_count`).
    /// * `max_files` — max stats files retained (`stats_max_files`).
    pub fn start(
        env_impl: Arc<Mutex<EnvironmentImpl>>,
        log_manager: Option<Arc<LogManager>>,
        cache_size: u64,
        dir: PathBuf,
        interval: Duration,
        row_count: u32,
        max_files: u32,
    ) -> Self {
        let writer = Arc::new(Mutex::new(RotatingWriter::new(
            dir,
            row_count.max(1),
            max_files.max(1),
        )));
        // Resume the file sequence past any files a previous run left behind so
        // a restart does not clobber history.
        writer.lock().seed_next_index();

        let seq = Arc::new(AtomicU64::new(0));

        let daemon =
            DaemonThread::spawn("noxu-stats-file", interval, move || {
                // If the environment is closed/poisoned, stop sampling.
                let stats = {
                    let guard = match env_impl.try_lock() {
                        Some(g) => g,
                        // Contended this tick — skip, try next interval.
                        None => return true,
                    };
                    build_environment_stats(
                        &guard,
                        log_manager.as_deref(),
                        cache_size,
                    )
                };
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                let _ = seq.fetch_add(1, Ordering::Relaxed);
                let row = format_row(now_ms, &stats);
                if let Err(e) = writer.lock().write_row(&row) {
                    log::warn!("stats-file dump failed: {e}");
                }
                true
            });
        StatsFileDumper { daemon }
    }

    /// Request shutdown and join the daemon thread.
    pub fn stop(self) {
        self.daemon.shutdown();
    }
}

/// Format one CSV row from a stats snapshot.  Kept free-standing so it is
/// unit-testable without spawning the daemon.
fn format_row(now_ms: u64, s: &noxu_engine::EnvironmentStats) -> String {
    format!(
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{}\n",
        now_ms,
        s.cache_size,
        s.cache_usage,
        s.n_databases,
        s.log.n_log_fsyncs,
        s.log.n_fsync_requests,
        s.log.n_group_commits,
        s.lock.n_requests,
        s.lock.n_waits,
        s.lock.n_total_locks,
        s.txn.n_begins,
        s.txn.n_commits,
        s.txn.n_aborts,
        s.txn.n_active,
    )
}

/// A row-count-bounded, file-count-bounded rotating CSV writer.
struct RotatingWriter {
    dir: PathBuf,
    row_count: u32,
    max_files: u32,
    /// Index of the file currently being written.
    file_index: u64,
    /// Rows written to the current file (excludes the header).
    rows_in_current: u32,
    /// The open file handle, or `None` before the first write / after a
    /// rotation (opened lazily).
    current: Option<File>,
}

impl RotatingWriter {
    fn new(dir: PathBuf, row_count: u32, max_files: u32) -> Self {
        RotatingWriter {
            dir,
            row_count,
            max_files,
            file_index: 0,
            rows_in_current: 0,
            current: None,
        }
    }

    /// Path for a given file index.
    fn path_for(&self, index: u64) -> PathBuf {
        self.dir.join(format!("{STATS_FILE_STEM}.{index}.{STATS_FILE_EXT}"))
    }

    /// Set `file_index` past the highest existing stats file so a restart
    /// appends new files rather than overwriting history.
    fn seed_next_index(&mut self) {
        let mut max_seen: Option<u64> = None;
        if let Ok(entries) = fs::read_dir(&self.dir) {
            for e in entries.flatten() {
                if let Some(idx) = parse_stats_index(&e.path()) {
                    max_seen = Some(max_seen.map_or(idx, |m| m.max(idx)));
                }
            }
        }
        if let Some(m) = max_seen {
            self.file_index = m + 1;
        }
    }

    /// Open (creating) the current file with a fresh CSV header.
    fn open_current(&mut self) -> std::io::Result<()> {
        fs::create_dir_all(&self.dir)?;
        let path = self.path_for(self.file_index);
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        f.write_all(CSV_HEADER.as_bytes())?;
        self.current = Some(f);
        self.rows_in_current = 0;
        Ok(())
    }

    /// Append one already-formatted CSV row, rotating first if the current
    /// file is full, and pruning old files past `max_files`.
    fn write_row(&mut self, row: &str) -> std::io::Result<()> {
        if self.current.is_none() {
            self.open_current()?;
        } else if self.rows_in_current >= self.row_count {
            // Rotate: advance to a new file and prune the oldest.
            self.file_index += 1;
            self.open_current()?;
            self.prune_old_files();
        }
        if let Some(f) = self.current.as_mut() {
            f.write_all(row.as_bytes())?;
            f.flush()?;
            self.rows_in_current += 1;
        }
        Ok(())
    }

    /// Delete stats files older than the newest `max_files`.
    fn prune_old_files(&self) {
        let mut indices: Vec<u64> = Vec::new();
        if let Ok(entries) = fs::read_dir(&self.dir) {
            for e in entries.flatten() {
                if let Some(idx) = parse_stats_index(&e.path()) {
                    indices.push(idx);
                }
            }
        }
        indices.sort_unstable();
        let keep = self.max_files as usize;
        if indices.len() > keep {
            for idx in &indices[..indices.len() - keep] {
                let _ = fs::remove_file(self.path_for(*idx));
            }
        }
    }
}

/// Parse the numeric index out of a `noxu.stat.<N>.csv` path, if it matches.
fn parse_stats_index(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let rest = name.strip_prefix(&format!("{STATS_FILE_STEM}."))?;
    let idx_str = rest.strip_suffix(&format!(".{STATS_FILE_EXT}"))?;
    idx_str.parse::<u64>().ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn parse_index_roundtrip() {
        let p = PathBuf::from("/x/noxu.stat.7.csv");
        assert_eq!(parse_stats_index(&p), Some(7));
        assert_eq!(parse_stats_index(&PathBuf::from("/x/other.csv")), None);
        assert_eq!(
            parse_stats_index(&PathBuf::from("/x/noxu.stat.notanum.csv")),
            None
        );
    }

    #[test]
    fn rotates_after_row_count_and_prunes() {
        let tmp = TempDir::new().unwrap();
        // row_count=2, max_files=2: write 6 rows -> 3 files would be created,
        // but only the newest 2 are kept.
        let mut w = RotatingWriter::new(tmp.path().to_path_buf(), 2, 2);
        for i in 0..6 {
            w.write_row(&format!("{i},0,0,0,0,0,0,0,0,0,0,0,0,0\n")).unwrap();
        }
        let mut files: Vec<u64> = fs::read_dir(tmp.path())
            .unwrap()
            .flatten()
            .filter_map(|e| parse_stats_index(&e.path()))
            .collect();
        files.sort_unstable();
        // 6 rows / 2 per file = 3 files (index 0,1,2); prune keeps newest 2.
        assert_eq!(files, vec![1, 2], "should retain only the newest 2 files");
        // The last file should carry a header + up to row_count rows.
        let last =
            fs::read_to_string(tmp.path().join("noxu.stat.2.csv")).unwrap();
        assert!(last.starts_with("time_ms,"), "header present");
        assert_eq!(
            last.lines().count(),
            1 + 2, // header + 2 data rows
            "last file: header + 2 rows"
        );
    }

    #[test]
    fn seed_next_index_resumes_past_existing() {
        let tmp = TempDir::new().unwrap();
        // Leave a stale file at index 4.
        fs::write(tmp.path().join("noxu.stat.4.csv"), b"x").unwrap();
        let mut w = RotatingWriter::new(tmp.path().to_path_buf(), 10, 10);
        w.seed_next_index();
        assert_eq!(w.file_index, 5, "resume past highest existing index");
    }
}
