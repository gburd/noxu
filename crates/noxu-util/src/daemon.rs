//! Daemon thread abstraction.
//!
//! Port of the daemon thread pattern used throughout JE for background tasks
//! (Cleaner, Checkpointer, Evictor, INCompressor, etc.).
//!
//! Provides a controlled lifecycle for background threads with graceful
//! shutdown and configurable wake intervals.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// A handle to a background daemon thread.
///
/// The daemon runs a task function periodically until shutdown is requested.
pub struct DaemonThread {
    name: String,
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl DaemonThread {
    /// Spawns a new daemon thread that calls `task` repeatedly with
    /// `wake_interval` between invocations.
    ///
    /// The task function should return `true` to continue running, or
    /// `false` to stop the daemon immediately.
    pub fn spawn<F>(
        name: impl Into<String>,
        wake_interval: Duration,
        task: F,
    ) -> Self
    where
        F: Fn() -> bool + Send + 'static,
    {
        let name = name.into();
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = shutdown.clone();
        let thread_name = name.clone();

        let handle = thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                while !shutdown_clone.load(Ordering::Relaxed) {
                    if !task() {
                        break;
                    }
                    // Sleep in small increments to check shutdown flag
                    let mut remaining = wake_interval;
                    let check_interval = Duration::from_millis(100);
                    while remaining > Duration::ZERO {
                        if shutdown_clone.load(Ordering::Relaxed) {
                            return;
                        }
                        let sleep_time = remaining.min(check_interval);
                        thread::sleep(sleep_time);
                        remaining = remaining.saturating_sub(sleep_time);
                    }
                }
            })
            .expect("failed to spawn daemon thread");

        DaemonThread { name, shutdown, handle: Some(handle) }
    }

    /// Returns the name of this daemon thread.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns true if shutdown has been requested.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }

    /// Requests the daemon to shut down.
    ///
    /// Does not block; use `join()` to wait for completion.
    pub fn request_shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    /// Requests shutdown and waits for the daemon thread to exit.
    pub fn shutdown(mut self) {
        self.request_shutdown();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for DaemonThread {
    fn drop(&mut self) {
        self.request_shutdown();
        // Don't block in Drop - just signal shutdown
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    #[test]
    fn test_daemon_runs_and_stops() {
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let daemon = DaemonThread::spawn(
            "test-daemon",
            Duration::from_millis(10),
            move || {
                counter_clone.fetch_add(1, Ordering::Relaxed);
                true
            },
        );

        thread::sleep(Duration::from_millis(100));
        daemon.shutdown();

        let count = counter.load(Ordering::Relaxed);
        assert!(count > 0, "daemon should have run at least once");
    }

    #[test]
    fn test_daemon_stops_on_false() {
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let daemon = DaemonThread::spawn(
            "stop-daemon",
            Duration::from_millis(10),
            move || {
                let c = counter_clone.fetch_add(1, Ordering::Relaxed);
                c < 3 // Run 3 times then stop
            },
        );

        thread::sleep(Duration::from_millis(200));
        let count = counter.load(Ordering::Relaxed);
        // Should have run exactly 4 times (0,1,2 return true; 3 returns false)
        assert_eq!(count, 4);
        daemon.shutdown();
    }
}
