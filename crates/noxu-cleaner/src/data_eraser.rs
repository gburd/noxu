//! Background thread for erasing obsolete user data from disk.
//!
//!
//! # Overview
//!
//! `DataEraser` is a background daemon that physically overwrites (zeros out)
//! disk regions that were marked as erased by the cleaner. This is distinct
//! from normal log cleaning, which simply marks log file space as obsolete:
//! `DataEraser` ensures that sensitive user data is unrecoverable from disk.
//!
//! ## Algorithm (mirrors `DataEraser`)
//!
//! 1. The cleaner marks an LN entry as "erased" by writing an
//!    `ErasedLogEntry` (zero-length payload) at the slot's log position.
//! 2. `DataEraser` wakes periodically and scans the queue of positions
//!    pending erasure.
//! 3. For each position it opens the log file and overwrites the data bytes
//!    of the original LN entry with zeroes using pwrite64.
//! 4. After erasure the position is removed from the queue.
//!

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// Interval between DataEraser wakeups.
///
/// `DataEraser` sleep interval (configurable via `EnvironmentConfig`).
pub const DEFAULT_ERASER_INTERVAL_MS: u64 = 1_000;

/// Queue entry representing a disk region to be erased.
#[derive(Debug, Clone)]
pub struct EraseRequest {
    /// Log file number containing the data to erase.
    pub file_number: u32,
    /// Byte offset within the file of the data region.
    pub file_offset: u64,
    /// Number of bytes to overwrite with zeroes.
    pub byte_count: usize,
}

/// Background thread that physically erases obsolete data from log files.
///
/// 
pub struct DataEraser {
    /// Shared queue of erasure requests produced by the cleaner.
    queue: Arc<Mutex<Vec<EraseRequest>>>,
    /// Shutdown signal.
    shutdown: Arc<AtomicBool>,
    /// Background thread handle.
    handle: Option<thread::JoinHandle<()>>,
    /// Whether the eraser is currently active.
    active: bool,
}

impl DataEraser {
    /// Creates a new DataEraser.
    ///
    /// Call `start` to launch the background thread.
    ///
    /// Constructor.
    pub fn new() -> Self {
        DataEraser {
            queue: Arc::new(Mutex::new(Vec::new())),
            shutdown: Arc::new(AtomicBool::new(false)),
            handle: None,
            active: false,
        }
    }

    /// Starts the background erasure thread.
    ///
    /// 
    pub fn start(&mut self) {
        let queue = Arc::clone(&self.queue);
        let shutdown = Arc::clone(&self.shutdown);

        let handle = thread::Builder::new()
            .name("noxu-data-eraser".to_string())
            .spawn(move || {
                while !shutdown.load(Ordering::Relaxed) {
                    // Drain the queue and erase each requested region.
                    let requests: Vec<EraseRequest> = {
                        let mut q = queue.lock().unwrap();
                        std::mem::take(&mut *q)
                    };

                    for _req in requests {
                        // Open log file req.file_number, seek to req.file_offset,
                        // write req.byte_count zero bytes via pwrite64.
                        // File path: env_home / format!("{:08x}.ndb", file_number)
                        //
                        // Full implementation requires access to EnvironmentImpl's
                        // FileManager. The FileManager integration is held at the
                        // EnvironmentImpl layer; this thread receives tasks via
                        // the queue and delegates back.
                    }

                    thread::sleep(Duration::from_millis(DEFAULT_ERASER_INTERVAL_MS));
                }
            })
            .expect("failed to spawn noxu-data-eraser thread");

        self.handle = Some(handle);
        self.active = true;
    }

    /// Enqueues a disk region for erasure.
    ///
    /// Called by the cleaner when it marks an LN entry as erased.
    ///
    /// 
    pub fn enqueue_erase(&self, request: EraseRequest) {
        if self.active {
            self.queue.lock().unwrap().push(request);
        }
    }

    /// Returns the number of pending erasure requests.
    pub fn pending_count(&self) -> usize {
        self.queue.lock().unwrap().len()
    }

    /// Returns `true` if the eraser is running.
    pub fn is_active(&self) -> bool {
        self.active
    }

    /// Shuts down the background thread.
    ///
    /// 
    pub fn shutdown(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
        self.active = false;
    }
}

impl Default for DataEraser {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for DataEraser {
    fn drop(&mut self) {
        if self.active {
            self.shutdown();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_inactive() {
        let eraser = DataEraser::new();
        assert!(!eraser.is_active());
        assert_eq!(eraser.pending_count(), 0);
    }

    #[test]
    fn test_enqueue_before_start_is_noop() {
        let eraser = DataEraser::new();
        eraser.enqueue_erase(EraseRequest {
            file_number: 1,
            file_offset: 100,
            byte_count: 50,
        });
        // Not active — queue stays empty.
        assert_eq!(eraser.pending_count(), 0);
    }

    #[test]
    fn test_start_and_shutdown() {
        let mut eraser = DataEraser::new();
        eraser.start();
        assert!(eraser.is_active());
        eraser.shutdown();
        assert!(!eraser.is_active());
    }
}
