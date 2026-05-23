//! Background daemon for periodic log flushing.
//!
//!
//! Flushes the log buffers (and write queue) periodically to disk and to the
//! file system, as specified by configuration parameters.

use crate::log_manager::LogManager;
use noxu_util::daemon::DaemonThread;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Background daemon that periodically flushes log buffers.
///
///
pub struct LogFlusher {
    /// The flush daemon thread (with fsync).
    flush_sync_daemon: Option<DaemonThread>,

    /// The flush daemon thread (without fsync).
    flush_no_sync_daemon: Option<DaemonThread>,

    /// Flush interval for sync operations (in milliseconds).
    flush_sync_interval_ms: u64,

    /// Flush interval for no-sync operations (in milliseconds).
    flush_no_sync_interval_ms: u64,

    /// Number of commits at last flush (sync).
    last_n_commits_sync: Arc<AtomicU64>,

    /// Number of commits at last flush (no-sync).
    last_n_commits_no_sync: Arc<AtomicU64>,

    /// Total number of flushes performed (sync).
    flush_sync_count: Arc<AtomicU64>,

    /// Total number of flushes performed (no-sync).
    flush_no_sync_count: Arc<AtomicU64>,
}

impl LogFlusher {
    /// Creates a new LogFlusher with the specified configuration.
    ///
    /// # Parameters
    /// - `log_manager`: Reference to the LogManager to flush
    /// - `flush_sync_interval_ms`: Interval for sync flushes (0 to disable)
    /// - `flush_no_sync_interval_ms`: Interval for no-sync flushes (0 to disable)
    /// - `get_n_commits`: Function to get the current commit count
    pub fn new(
        log_manager: Arc<LogManager>,
        flush_sync_interval_ms: u64,
        flush_no_sync_interval_ms: u64,
        get_n_commits: Arc<dyn Fn() -> u64 + Send + Sync>,
    ) -> Self {
        let last_n_commits_sync = Arc::new(AtomicU64::new(get_n_commits()));
        let last_n_commits_no_sync = Arc::new(AtomicU64::new(get_n_commits()));
        let flush_sync_count = Arc::new(AtomicU64::new(0));
        let flush_no_sync_count = Arc::new(AtomicU64::new(0));

        let flush_sync_daemon = if flush_sync_interval_ms > 0 {
            let lm = Arc::clone(&log_manager);
            let get_commits = Arc::clone(&get_n_commits);
            let last_commits = Arc::clone(&last_n_commits_sync);
            let count = Arc::clone(&flush_sync_count);

            Some(DaemonThread::spawn(
                "LogFlusher-Sync",
                Duration::from_millis(flush_sync_interval_ms),
                move || {
                    let current_commits = get_commits();
                    let last = last_commits.load(Ordering::Relaxed);

                    // Only flush if there have been new commits
                    if current_commits > last {
                        if let Err(e) = lm.flush_sync() {
                            eprintln!("LogFlusher sync error: {:?}", e);
                        } else {
                            last_commits
                                .store(current_commits, Ordering::Relaxed);
                            count.fetch_add(1, Ordering::Relaxed);
                        }
                    }

                    true // Continue running
                },
            ))
        } else {
            None
        };

        let flush_no_sync_daemon = if flush_no_sync_interval_ms > 0 {
            let lm = Arc::clone(&log_manager);
            let get_commits = Arc::clone(&get_n_commits);
            let last_commits = Arc::clone(&last_n_commits_no_sync);
            let count = Arc::clone(&flush_no_sync_count);

            Some(DaemonThread::spawn(
                "LogFlusher-NoSync",
                Duration::from_millis(flush_no_sync_interval_ms),
                move || {
                    let current_commits = get_commits();
                    let last = last_commits.load(Ordering::Relaxed);

                    // Only flush if there have been new commits
                    if current_commits > last {
                        if let Err(e) = lm.flush_no_sync() {
                            eprintln!("LogFlusher no-sync error: {:?}", e);
                        } else {
                            last_commits
                                .store(current_commits, Ordering::Relaxed);
                            count.fetch_add(1, Ordering::Relaxed);
                        }
                    }

                    true // Continue running
                },
            ))
        } else {
            None
        };

        LogFlusher {
            flush_sync_daemon,
            flush_no_sync_daemon,
            flush_sync_interval_ms,
            flush_no_sync_interval_ms,
            last_n_commits_sync,
            last_n_commits_no_sync,
            flush_sync_count,
            flush_no_sync_count,
        }
    }

    /// Returns the configured flush sync interval in milliseconds.
    pub fn get_flush_sync_interval(&self) -> u64 {
        self.flush_sync_interval_ms
    }

    /// Returns the configured flush no-sync interval in milliseconds.
    pub fn get_flush_no_sync_interval(&self) -> u64 {
        self.flush_no_sync_interval_ms
    }

    /// Returns the number of sync flushes performed.
    pub fn get_flush_sync_count(&self) -> u64 {
        self.flush_sync_count.load(Ordering::Relaxed)
    }

    /// Returns the number of no-sync flushes performed.
    pub fn get_flush_no_sync_count(&self) -> u64 {
        self.flush_no_sync_count.load(Ordering::Relaxed)
    }

    /// Requests shutdown of the flusher daemons.
    pub fn request_shutdown(&self) {
        if let Some(ref daemon) = self.flush_sync_daemon {
            daemon.request_shutdown();
        }
        if let Some(ref daemon) = self.flush_no_sync_daemon {
            daemon.request_shutdown();
        }
    }

    /// Shuts down the flusher daemons and waits for them to complete.
    pub fn shutdown(self) {
        if let Some(daemon) = self.flush_sync_daemon {
            daemon.shutdown();
        }
        if let Some(daemon) = self.flush_no_sync_daemon {
            daemon.shutdown();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_manager::FileManager;
    use crate::log_manager::LogManager;
    use std::sync::atomic::AtomicU64;
    use tempfile::TempDir;

    /// Build a LogManager backed by a real FileManager in a temp directory.
    fn make_log_manager(dir: &TempDir) -> Arc<LogManager> {
        let fm = Arc::new(
            FileManager::new(dir.path(), false, 10_000_000, 100).unwrap(),
        );
        Arc::new(LogManager::new(fm, 3, 1024 * 1024, 4096))
    }

    #[test]
    fn test_new_flusher() {
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);
        let commit_count = Arc::new(AtomicU64::new(0));
        let commit_count_clone = Arc::clone(&commit_count);

        let flusher = LogFlusher::new(
            lm,
            1000,
            500,
            Arc::new(move || commit_count_clone.load(Ordering::Relaxed)),
        );

        assert_eq!(flusher.get_flush_sync_interval(), 1000);
        assert_eq!(flusher.get_flush_no_sync_interval(), 500);

        flusher.shutdown();
    }

    #[test]
    fn test_flusher_triggers_on_commits() {
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);
        let commit_count = Arc::new(AtomicU64::new(0));
        let commit_count_clone = Arc::clone(&commit_count);

        let flusher = LogFlusher::new(
            lm,
            50, // Short interval for testing
            0,  // Disable no-sync
            Arc::new(move || commit_count_clone.load(Ordering::Relaxed)),
        );

        // Simulate some commits
        commit_count.store(10, Ordering::Relaxed);

        // Wait for flush to happen
        std::thread::sleep(Duration::from_millis(200));

        // At least one flush should have occurred
        assert!(flusher.get_flush_sync_count() > 0);

        flusher.shutdown();
    }

    #[test]
    fn test_flusher_no_flush_without_commits() {
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);
        let commit_count = Arc::new(AtomicU64::new(5));
        let commit_count_clone = Arc::clone(&commit_count);

        let flusher = LogFlusher::new(
            lm,
            50, // Short interval
            0,
            Arc::new(move || commit_count_clone.load(Ordering::Relaxed)),
        );

        // Don't change commit count
        std::thread::sleep(Duration::from_millis(200));

        // No flushes should occur since commits haven't changed
        assert_eq!(flusher.get_flush_sync_count(), 0);

        flusher.shutdown();
    }
}
