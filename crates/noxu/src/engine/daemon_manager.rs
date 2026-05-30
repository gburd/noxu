//! Background daemon lifecycle management.

use crate::engine::engine_config::EngineConfig;
use crate::cleaner::Cleaner;
use crate::evictor::{EvictionSource, Evictor};
use crate::recovery::Checkpointer;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// A wakeup handle used by daemon threads to sleep with early-exit on shutdown.
///
/// Each daemon receives a clone of this handle. When `notify()` is called
/// (at shutdown), the daemon wakes from its sleep immediately rather than
/// waiting for the full interval to elapse.
#[derive(Clone)]
struct WakeHandle {
    pair: Arc<(Mutex<bool>, Condvar)>,
}

impl WakeHandle {
    fn new() -> Self {
        Self { pair: Arc::new((Mutex::new(false), Condvar::new())) }
    }

    /// Sleep for `duration`, but return early if `notify()` is called.
    ///
    /// Returns `true` if the wakeup was triggered by a shutdown notification,
    /// `false` if the timeout elapsed normally.
    fn wait_timeout(&self, duration: Duration) -> bool {
        let (lock, cvar) = &*self.pair;
        let guard = lock.lock().unwrap();
        let (guard, _) = cvar.wait_timeout(guard, duration).unwrap();
        *guard
    }

    /// Notify the sleeping daemon to wake up immediately.
    fn notify(&self) {
        let (lock, cvar) = &*self.pair;
        *lock.lock().unwrap() = true;
        cvar.notify_all();
    }
}

/// Manages the lifecycle of background daemon threads.
///
/// The DaemonManager is responsible for:
/// - Starting daemon threads (evictor, cleaner, checkpointer)
/// - Coordinating shutdown of all daemons
/// - Tracking daemon running state
///
/// Each daemon runs in its own thread, periodically waking up to perform work.
/// On shutdown, daemons are notified via a Condvar so they exit immediately
/// instead of sleeping through their full wakeup interval.
pub struct DaemonManager {
    /// Shutdown signal shared by all daemon threads.
    shutdown: Arc<AtomicBool>,

    /// Wakeup handles for each daemon (used to unblock their sleep on shutdown).
    evictor_wake: WakeHandle,
    cleaner_wake: WakeHandle,
    checkpointer_wake: WakeHandle,

    /// Evictor daemon thread handle.
    evictor_handle: Option<JoinHandle<()>>,

    /// Cleaner daemon thread handle.
    cleaner_handle: Option<JoinHandle<()>>,

    /// Checkpointer daemon thread handle.
    checkpointer_handle: Option<JoinHandle<()>>,

    /// Whether evictor is enabled.
    evictor_enabled: bool,

    /// Whether cleaner is enabled.
    cleaner_enabled: bool,

    /// Whether checkpointer is enabled.
    checkpointer_enabled: bool,

    /// Evictor wakeup interval.
    evictor_wakeup_ms: u64,

    /// Cleaner wakeup interval.
    cleaner_wakeup_ms: u64,

    /// Checkpointer wakeup interval.
    checkpointer_wakeup_ms: u64,
}

impl DaemonManager {
    /// Creates a new DaemonManager from the given configuration.
    ///
    /// Daemons are not started until `start_daemons()` is called.
    pub fn new(config: &EngineConfig) -> Self {
        Self {
            shutdown: Arc::new(AtomicBool::new(false)),
            evictor_wake: WakeHandle::new(),
            cleaner_wake: WakeHandle::new(),
            checkpointer_wake: WakeHandle::new(),
            evictor_handle: None,
            cleaner_handle: None,
            checkpointer_handle: None,
            evictor_enabled: config.evictor_enabled,
            cleaner_enabled: config.cleaner_enabled,
            checkpointer_enabled: config.checkpointer_enabled,
            evictor_wakeup_ms: config.evictor_wakeup_interval_ms,
            cleaner_wakeup_ms: config.cleaner_wakeup_interval_ms,
            checkpointer_wakeup_ms: config.checkpointer_wakeup_interval_ms,
        }
    }

    /// Starts all enabled daemon threads.
    ///
    /// Each daemon runs in a loop:
    /// 1. Sleep for its wakeup interval
    /// 2. Check shutdown flag
    /// 3. Perform work (eviction, cleaning, checkpoint)
    /// 4. Repeat
    ///
    /// # Arguments
    /// * `evictor` - The evictor to use for eviction operations
    /// * `cleaner` - The cleaner to use for cleaning operations
    /// * `checkpointer` - The checkpointer to use for checkpoint operations
    pub fn start_daemons(
        &mut self,
        evictor: Arc<Evictor>,
        cleaner: Arc<Cleaner>,
        checkpointer: Arc<Checkpointer>,
    ) {
        // Start evictor daemon
        if self.evictor_enabled {
            let shutdown = Arc::clone(&self.shutdown);
            let wakeup_ms = self.evictor_wakeup_ms;
            let evictor = Arc::clone(&evictor);
            let wake = self.evictor_wake.clone();

            self.evictor_handle = Some(thread::spawn(move || {
                log::info!("Evictor daemon started");
                while !shutdown.load(Ordering::Relaxed) {
                    // Sleep for the wakeup interval, but return early on shutdown.
                    let notified =
                        wake.wait_timeout(Duration::from_millis(wakeup_ms));
                    if notified || shutdown.load(Ordering::Relaxed) {
                        break;
                    }

                    // Perform eviction
                    let result = evictor.do_evict(EvictionSource::Daemon);
                    if result.nodes_evicted > 0 {
                        log::debug!(
                            "Evictor: evicted {} nodes, {} bytes",
                            result.nodes_evicted,
                            result.bytes_evicted
                        );
                    }
                }
                log::info!("Evictor daemon stopped");
            }));
        }

        // Start cleaner daemon
        if self.cleaner_enabled {
            let shutdown = Arc::clone(&self.shutdown);
            let wakeup_ms = self.cleaner_wakeup_ms;
            let cleaner = Arc::clone(&cleaner);
            let wake = self.cleaner_wake.clone();

            self.cleaner_handle = Some(thread::spawn(move || {
                log::info!("Cleaner daemon started");
                while !shutdown.load(Ordering::Relaxed) {
                    // Sleep for the wakeup interval, but return early on shutdown.
                    let notified =
                        wake.wait_timeout(Duration::from_millis(wakeup_ms));
                    if notified || shutdown.load(Ordering::Relaxed) {
                        break;
                    }

                    // Perform cleaning
                    match cleaner.do_clean(1, false) {
                        Ok(result) => {
                            if result.files_cleaned > 0 {
                                log::debug!(
                                    "Cleaner: cleaned {} files, deleted {} files",
                                    result.files_cleaned,
                                    result.files_deleted
                                );
                            }
                        }
                        Err(e) => {
                            log::warn!("Cleaner error: {}", e);
                        }
                    }
                }
                log::info!("Cleaner daemon stopped");
            }));
        }

        // Start checkpointer daemon
        if self.checkpointer_enabled {
            let shutdown = Arc::clone(&self.shutdown);
            let wakeup_ms = self.checkpointer_wakeup_ms;
            let checkpointer = Arc::clone(&checkpointer);
            let wake = self.checkpointer_wake.clone();

            self.checkpointer_handle = Some(thread::spawn(move || {
                log::info!("Checkpointer daemon started");
                while !shutdown.load(Ordering::Relaxed) {
                    // Sleep for the wakeup interval, but return early on shutdown.
                    let notified =
                        wake.wait_timeout(Duration::from_millis(wakeup_ms));
                    if notified || shutdown.load(Ordering::Relaxed) {
                        break;
                    }

                    // Perform checkpoint
                    match checkpointer.do_checkpoint("daemon") {
                        Ok(result) => {
                            log::debug!(
                                "Checkpoint: id={}, flushed {} nodes",
                                result.checkpoint_id,
                                result.total_nodes_flushed()
                            );
                        }
                        Err(e) => {
                            log::warn!("Checkpoint error: {}", e);
                        }
                    }
                }
                log::info!("Checkpointer daemon stopped");
            }));
        }
    }

    /// Signals shutdown and waits for all daemon threads to complete.
    ///
    /// This method:
    /// 1. Sets the shutdown flag
    /// 2. Notifies all sleeping daemons via their Condvar so they wake immediately
    /// 3. Joins all daemon thread handles
    /// 4. Waits for all threads to exit cleanly
    pub fn shutdown(&mut self) {
        // Signal shutdown
        self.shutdown.store(true, Ordering::Relaxed);

        // Wake all sleeping daemons immediately so they don't wait out their
        // full sleep interval before noticing the shutdown flag.
        self.evictor_wake.notify();
        self.cleaner_wake.notify();
        self.checkpointer_wake.notify();

        // Join evictor
        if let Some(handle) = self.evictor_handle.take()
            && let Err(e) = handle.join()
        {
            log::error!("Failed to join evictor thread: {:?}", e);
        }

        // Join cleaner
        if let Some(handle) = self.cleaner_handle.take()
            && let Err(e) = handle.join()
        {
            log::error!("Failed to join cleaner thread: {:?}", e);
        }

        // Join checkpointer
        if let Some(handle) = self.checkpointer_handle.take()
            && let Err(e) = handle.join()
        {
            log::error!("Failed to join checkpointer thread: {:?}", e);
        }
    }

    /// Returns `true` while this manager has not been shut down.
    ///
    /// Specifically, this returns `true` from construction until
    /// [`shutdown`](Self::shutdown) is invoked. It does **not** prove that
    /// any daemon thread is currently alive: a freshly-constructed manager
    /// (before [`start_daemons`](Self::start_daemons) is called) reports
    /// `true` here while [`running_count`](Self::running_count) returns 0.
    ///
    /// This semantic is codified by `test_daemon_manager_creation`, which
    /// asserts both `is_running() == true` and `running_count() == 0`
    /// before any daemons are started. Use `running_count()` if you need
    /// the actual count of spawned daemon threads.
    pub fn is_running(&self) -> bool {
        // NB: name is historical. We return `!shutdown_requested` rather
        // than checking the JoinHandles so that the post-`new`/pre-`start`
        // contract above remains stable.
        !self.shutdown.load(Ordering::Relaxed)
    }

    /// Returns the number of running daemons.
    pub fn running_count(&self) -> usize {
        let mut count = 0;
        if self.evictor_enabled && self.evictor_handle.is_some() {
            count += 1;
        }
        if self.cleaner_enabled && self.cleaner_handle.is_some() {
            count += 1;
        }
        if self.checkpointer_enabled && self.checkpointer_handle.is_some() {
            count += 1;
        }
        count
    }
}

impl Drop for DaemonManager {
    fn drop(&mut self) {
        // Ensure clean shutdown
        if self.is_running() {
            self.shutdown();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evictor::Arbiter;
    use crate::recovery::CheckpointConfig;
    use std::sync::atomic::AtomicI64;

    #[test]
    fn test_daemon_manager_creation() {
        let config = EngineConfig::default();
        let manager = DaemonManager::new(&config);

        assert!(manager.evictor_enabled);
        assert!(manager.cleaner_enabled);
        assert!(manager.checkpointer_enabled);
        assert!(manager.is_running());
        assert_eq!(manager.running_count(), 0); // Not started yet
    }

    #[test]
    fn test_daemon_manager_with_disabled_daemons() {
        let config = EngineConfig::default()
            .evictor_enabled(false)
            .cleaner_enabled(false)
            .checkpointer_enabled(false);
        let manager = DaemonManager::new(&config);

        assert!(!manager.evictor_enabled);
        assert!(!manager.cleaner_enabled);
        assert!(!manager.checkpointer_enabled);
    }

    #[test]
    fn test_daemon_manager_start_and_shutdown() {
        let config = EngineConfig::default()
            .evictor_wakeup_interval_ms(100)
            .cleaner_wakeup_interval_ms(100)
            .checkpointer_wakeup_interval_ms(100);

        let mut manager = DaemonManager::new(&config);

        // Create subsystems
        let usage = Arc::new(AtomicI64::new(500));
        let arbiter = Arbiter::new(1000, usage, 100, 200);
        let evictor = Arc::new(Evictor::new(arbiter, 100, false));
        let cleaner = Arc::new(Cleaner::new(50, 5, 0));
        let checkpointer =
            Arc::new(Checkpointer::new(CheckpointConfig::default()));

        // Start daemons
        manager.start_daemons(evictor, cleaner, checkpointer);

        // Give threads time to start
        thread::sleep(Duration::from_millis(50));
        assert!(manager.is_running());
        assert_eq!(manager.running_count(), 3);

        // Shutdown
        manager.shutdown();
        assert!(!manager.is_running());
    }

    #[test]
    fn test_daemon_manager_selective_daemons() {
        let config = EngineConfig::default()
            .evictor_enabled(true)
            .cleaner_enabled(false)
            .checkpointer_enabled(true)
            .evictor_wakeup_interval_ms(100)
            .checkpointer_wakeup_interval_ms(100);

        let mut manager = DaemonManager::new(&config);

        let usage = Arc::new(AtomicI64::new(500));
        let arbiter = Arbiter::new(1000, usage, 100, 200);
        let evictor = Arc::new(Evictor::new(arbiter, 100, false));
        let cleaner = Arc::new(Cleaner::new(50, 5, 0));
        let checkpointer =
            Arc::new(Checkpointer::new(CheckpointConfig::default()));

        manager.start_daemons(evictor, cleaner, checkpointer);

        thread::sleep(Duration::from_millis(50));
        assert_eq!(manager.running_count(), 2); // Only evictor and checkpointer

        manager.shutdown();
    }

    #[test]
    fn test_daemon_manager_drop_cleanup() {
        let config = EngineConfig::default()
            .evictor_wakeup_interval_ms(100)
            .cleaner_wakeup_interval_ms(100)
            .checkpointer_wakeup_interval_ms(100);

        let mut manager = DaemonManager::new(&config);

        let usage = Arc::new(AtomicI64::new(500));
        let arbiter = Arbiter::new(1000, usage, 100, 200);
        let evictor = Arc::new(Evictor::new(arbiter, 100, false));
        let cleaner = Arc::new(Cleaner::new(50, 5, 0));
        let checkpointer =
            Arc::new(Checkpointer::new(CheckpointConfig::default()));

        manager.start_daemons(evictor, cleaner, checkpointer);

        thread::sleep(Duration::from_millis(50));
        assert!(manager.is_running());

        // Drop should trigger cleanup
        drop(manager);
    }

    #[test]
    fn test_daemon_wakeup_intervals() {
        let config = EngineConfig::default()
            .evictor_wakeup_interval_ms(1000)
            .cleaner_wakeup_interval_ms(2000)
            .checkpointer_wakeup_interval_ms(3000);

        let manager = DaemonManager::new(&config);
        assert_eq!(manager.evictor_wakeup_ms, 1000);
        assert_eq!(manager.cleaner_wakeup_ms, 2000);
        assert_eq!(manager.checkpointer_wakeup_ms, 3000);
    }

    /// Verify that shutdown returns quickly even when daemons are configured
    /// with a long wakeup interval.  If the condvar notification is working,
    /// this completes in well under the 5-second interval.
    #[test]
    fn test_shutdown_wakes_daemons_early() {
        use std::time::Instant;

        // Use a 5-second interval; shutdown must complete far faster than that.
        let config = EngineConfig::default()
            .evictor_wakeup_interval_ms(5000)
            .cleaner_wakeup_interval_ms(5000)
            .checkpointer_wakeup_interval_ms(5000);

        let mut manager = DaemonManager::new(&config);

        let usage = Arc::new(AtomicI64::new(500));
        let arbiter = Arbiter::new(1000, usage, 100, 200);
        let evictor = Arc::new(Evictor::new(arbiter, 100, false));
        let cleaner = Arc::new(Cleaner::new(50, 5, 0));
        let checkpointer =
            Arc::new(Checkpointer::new(CheckpointConfig::default()));

        manager.start_daemons(evictor, cleaner, checkpointer);

        // Give threads a moment to enter their wait.
        thread::sleep(Duration::from_millis(50));

        let start = Instant::now();
        manager.shutdown();
        let elapsed = start.elapsed();

        // Shutdown must complete in under 1 second even though sleep is 5 s.
        assert!(
            elapsed < Duration::from_secs(1),
            "shutdown took {:?}, expected < 1s",
            elapsed
        );
    }

    #[test]
    fn test_wake_handle_timeout() {
        let handle = WakeHandle::new();

        // With no notification the wait should time out (returns false).
        let notified = handle.wait_timeout(Duration::from_millis(50));
        assert!(!notified);
    }

    #[test]
    fn test_wake_handle_notify() {
        use std::time::Instant;

        let handle = WakeHandle::new();
        let handle2 = handle.clone();

        // Spawn a thread that notifies after a short delay.
        let t = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            handle2.notify();
        });

        let start = Instant::now();
        // Wait up to 5 seconds; notification should arrive ~20 ms in.
        let notified = handle.wait_timeout(Duration::from_secs(5));
        let elapsed = start.elapsed();

        t.join().unwrap();

        assert!(notified, "expected notify to return true");
        assert!(
            elapsed < Duration::from_millis(500),
            "took {:?}, expected wakeup within 500ms",
            elapsed
        );
    }
}
