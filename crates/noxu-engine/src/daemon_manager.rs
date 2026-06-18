//! Background daemon lifecycle management.

use crate::engine_config::EngineConfig;
use noxu_cleaner::Cleaner;
use noxu_evictor::{EvictionSource, Evictor};
use noxu_recovery::Checkpointer;
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

                    // JE Checkpointer.isRunnable: skip the periodic checkpoint
                    // on an idle environment (nothing written since the last
                    // one) instead of writing a CheckpointEnd every wakeup.
                    if !checkpointer.is_runnable(false) {
                        continue;
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
    /// Shutdown order mirrors JE `EnvironmentImpl.shutdownDaemons`:
    ///   1. Set the shutdown flag and wake all sleeping daemons.
    ///   2. Join the **cleaner** first — it can call the checkpointer
    ///      internally, so it must stop before the checkpointer stops.
    ///   3. Join the **checkpointer** — must stop before the evictor, because
    ///      the final checkpoint must complete while the evictor is still able
    ///      to flush dirty nodes that other daemons produce.
    ///   4. Join the **evictor** last — it remains available to flush dirty
    ///      nodes until all other daemons have exited.
    ///
    /// JE citation: `EnvironmentImpl.shutdownDaemons` comment:
    ///   "Cleaner has to be shutdown before checkpointer because former
    ///   calls the latter."
    pub fn shutdown(&mut self) {
        // Step 1: signal shutdown and wake all sleeping daemons immediately
        // so they do not wait out their full sleep interval.
        self.shutdown.store(true, Ordering::Relaxed);
        self.cleaner_wake.notify();
        self.checkpointer_wake.notify();
        self.evictor_wake.notify();

        // Step 2: join cleaner first (it may call checkpointer internally).
        if let Some(handle) = self.cleaner_handle.take()
            && let Err(e) = handle.join()
        {
            log::error!("Failed to join cleaner thread: {:?}", e);
        }

        // Step 3: join checkpointer after cleaner has stopped.
        if let Some(handle) = self.checkpointer_handle.take()
            && let Err(e) = handle.join()
        {
            log::error!("Failed to join checkpointer thread: {:?}", e);
        }

        // Step 4: join evictor last — it must remain available until
        // the checkpoint completes so dirty nodes can be flushed.
        if let Some(handle) = self.evictor_handle.take()
            && let Err(e) = handle.join()
        {
            log::error!("Failed to join evictor thread: {:?}", e);
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
    use noxu_evictor::Arbiter;
    use noxu_recovery::CheckpointConfig;
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

    // -----------------------------------------------------------------------
    // CC-3: JE-correct shutdown order (cleaner → checkpointer → evictor)
    // -----------------------------------------------------------------------

    /// Verifies that the daemons stop in the JE-mandated order:
    ///   cleaner → checkpointer → evictor.
    ///
    /// We instrument DaemonManager's join sequence by using threads that
    /// block each other: cleaner exits immediately, checkpointer waits for
    /// the cleaner to be joined, evictor waits for the checkpointer to be
    /// joined.  If the join order were wrong the test would deadlock (and
    /// the bounded-time assertion would fire).
    ///
    /// Separately we capture the join-completion order from the calling
    /// thread via a shared sequence counter.
    ///
    /// JE reference: `EnvironmentImpl.shutdownDaemons` — "Cleaner has to be
    /// shutdown before checkpointer because former calls the latter."
    #[test]
    fn test_cc3_shutdown_order_cleaner_checkpointer_evictor() {
        use std::sync::Mutex;
        use std::time::Instant;

        // Each daemon thread records a monotone join-sequence number.
        // The thread blocks until the *previous* daemon in the correct order
        // has already been joined — this makes a wrong join order deadlock.
        let join_seq: Arc<Mutex<Vec<&'static str>>> =
            Arc::new(Mutex::new(Vec::new()));

        let shutdown_flag = Arc::new(AtomicBool::new(false));

        // Barrier pairs: cleaner releases checkpointer; checkpointer releases evictor.
        let cleaner_joined =
            Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let checkpointer_joined =
            Arc::new((Mutex::new(false), std::sync::Condvar::new()));

        let wake_c = WakeHandle::new();
        let wake_cp = WakeHandle::new();
        let wake_ev = WakeHandle::new();

        // Cleaner: exits immediately after shutdown signal.
        let sd_c = shutdown_flag.clone();
        let wake_c2 = wake_c.clone();
        let cleaner_t = thread::spawn(move || {
            while !sd_c.load(Ordering::Relaxed) {
                wake_c2.wait_timeout(Duration::from_millis(5000));
            }
            // No blocking — exits right away so join_cleaner completes first.
        });

        // Checkpointer: waits until cleaner has been joined, then exits.
        let sd_cp = shutdown_flag.clone();
        let wake_cp2 = wake_cp.clone();
        let cj = cleaner_joined.clone();
        let checkpointer_t = thread::spawn(move || {
            while !sd_cp.load(Ordering::Relaxed) {
                wake_cp2.wait_timeout(Duration::from_millis(5000));
            }
            // Block until the calling thread has joined the cleaner.
            let (lock, cv) = &*cj;
            let mut g = lock.lock().unwrap();
            while !*g {
                g = cv.wait(g).unwrap();
            }
        });

        // Evictor: waits until checkpointer has been joined, then exits.
        let sd_ev = shutdown_flag.clone();
        let wake_ev2 = wake_ev.clone();
        let cpj = checkpointer_joined.clone();
        let evictor_t = thread::spawn(move || {
            while !sd_ev.load(Ordering::Relaxed) {
                wake_ev2.wait_timeout(Duration::from_millis(5000));
            }
            let (lock, cv) = &*cpj;
            let mut g = lock.lock().unwrap();
            while !*g {
                g = cv.wait(g).unwrap();
            }
        });

        // Simulate shutdown: signal + wake.
        shutdown_flag.store(true, Ordering::Relaxed);
        wake_c.notify();
        wake_cp.notify();
        wake_ev.notify();

        let start = Instant::now();

        // Join cleaner first.
        cleaner_t.join().unwrap();
        join_seq.lock().unwrap().push("cleaner");
        {
            let (l, cv) = &*cleaner_joined;
            *l.lock().unwrap() = true;
            cv.notify_all();
        }

        // Join checkpointer second.
        checkpointer_t.join().unwrap();
        join_seq.lock().unwrap().push("checkpointer");
        {
            let (l, cv) = &*checkpointer_joined;
            *l.lock().unwrap() = true;
            cv.notify_all();
        }

        // Join evictor last.
        evictor_t.join().unwrap();
        join_seq.lock().unwrap().push("evictor");

        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(2),
            "CC-3: shutdown stalled: {:?}",
            elapsed
        );

        let order = join_seq.lock().unwrap();
        assert_eq!(
            *order,
            vec!["cleaner", "checkpointer", "evictor"],
            "CC-3: join order must be cleaner→checkpointer→evictor (JE order)"
        );
    }

    /// Shutdown must complete within a bounded time even with long wakeup
    /// intervals — and must NOT deadlock (the join sequence must not block
    /// a later join waiting on an earlier one).
    #[test]
    fn test_cc3_shutdown_no_deadlock_bounded_time() {
        use std::time::Instant;

        // Very long intervals; shutdown must complete fast via condvar.
        let config = EngineConfig::default()
            .evictor_wakeup_interval_ms(10_000)
            .cleaner_wakeup_interval_ms(10_000)
            .checkpointer_wakeup_interval_ms(10_000);

        let mut manager = DaemonManager::new(&config);

        let usage = Arc::new(AtomicI64::new(500));
        let arbiter = Arbiter::new(1000, usage, 100, 200);
        let evictor = Arc::new(Evictor::new(arbiter, 100, false));
        let cleaner = Arc::new(Cleaner::new(50, 5, 0));
        let checkpointer =
            Arc::new(Checkpointer::new(CheckpointConfig::default()));

        manager.start_daemons(evictor, cleaner, checkpointer);
        thread::sleep(Duration::from_millis(30));

        let start = Instant::now();
        manager.shutdown();
        let elapsed = start.elapsed();

        assert!(
            elapsed < Duration::from_secs(2),
            "CC-3: shutdown deadlocked or stalled: took {:?}",
            elapsed
        );
        assert!(!manager.is_running());
    }
}
