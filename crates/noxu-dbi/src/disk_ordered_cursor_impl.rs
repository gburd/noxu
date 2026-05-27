//! Disk-ordered cursor implementation.
//!
//! Sits behind the public [`noxu_db::DiskOrderedCursor`] API.  Spawns a
//! background producer thread that walks the log files sequentially, decodes
//! LN entries that belong to the targeted databases, and pushes
//! `(key, data)` tuples through a bounded channel for the consumer to drain
//! via [`DiskOrderedCursorImpl::next_entry`].
//!
//! See `crates/noxu-db/src/disk_ordered_cursor.rs` for the public-API
//! contract and consistency guarantees.
//!
//! # Producer-thread lifecycle
//!
//! ```text
//!   open() ──spawn──> Producer thread (filescan + decode + send)
//!                        │
//!                        ▼
//!   next_entry() <──── bounded mpsc channel
//!                        ▲
//!                        │
//!   shutdown() ──signal──┘   (joins on drop)
//! ```
//!
//! Two channels are used:
//!
//! * **Data channel** — `sync_channel::<DocItem>(queue_size)`: producer sends
//!   results, consumer receives.
//! * **Shutdown flag** — `Arc<AtomicBool>`: consumer sets it; producer
//!   checks it between entries and terminates promptly.
//!
//! # Memory budget
//!
//! `internal_memory_limit` is interpreted as a soft cap on the *cumulative
//! key+data byte size* of items currently buffered in the channel.  The
//! producer tracks it via an `Arc<AtomicUsize>` that the consumer
//! decrements as it drains; the producer blocks on a `Condvar` when the
//! limit is reached.  This is approximate (race windows around
//! send/recv), in line with JE's own approximation.

use std::collections::HashSet;
use std::sync::{
    Arc, Condvar, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
    mpsc::{self, RecvTimeoutError, SyncSender},
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use bytes::Bytes;
use noxu_log::LogManager;
use noxu_recovery::{
    LnOperation, LogEntry, LogScanner, PositionedEntry,
};

use crate::database_id::DatabaseId;
use crate::error::{DbiError, Result};
use crate::file_manager_scanner::FileManagerLogScanner;

/// Items pushed by the producer through the data channel.
type DocItem = std::result::Result<(Vec<u8>, Vec<u8>), DbiError>;

/// Plain-data options accepted by [`DiskOrderedCursorImpl::open`].
///
/// The public `noxu-db::DiskOrderedCursorConfig` is mapped onto this struct
/// at the API boundary so the implementation crate has no knowledge of the
/// outer config type.
#[derive(Debug, Clone)]
pub struct DiskOrderedCursorOptions {
    /// Maximum number of entries the data channel may hold.
    pub queue_size: usize,
    /// Soft cap on the cumulative key+data bytes buffered in the channel.
    pub internal_memory_limit: usize,
    /// Advisory: max LSNs to consider per producer batch.  Currently
    /// honoured as a periodic shutdown-flag check interval.
    pub lsn_batch_size: usize,
    /// If `true`, only key bytes are returned; data is empty.
    pub keys_only: bool,
    /// If `true`, keep a `(db_idx, key)` HashSet and skip duplicates.
    pub dedup_keys: bool,
}

impl Default for DiskOrderedCursorOptions {
    fn default() -> Self {
        Self {
            queue_size: 1000,
            internal_memory_limit: usize::MAX,
            lsn_batch_size: usize::MAX,
            keys_only: false,
            dedup_keys: false,
        }
    }
}

/// Memory-budget tracker shared between producer and consumer.
struct MemoryBudget {
    in_use: AtomicUsize,
    limit: usize,
    /// Producer parks here when in_use >= limit.  Notified by consumer
    /// after each successful recv() that decrements `in_use`.
    cv: Condvar,
    /// Mutex protecting the Condvar (no shared data — Mutex<()> is fine).
    mu: Mutex<()>,
}

impl MemoryBudget {
    fn new(limit: usize) -> Self {
        Self {
            in_use: AtomicUsize::new(0),
            limit,
            cv: Condvar::new(),
            mu: Mutex::new(()),
        }
    }

    /// Block on the condvar until there is room for `bytes`, or `cancel`
    /// fires.  Returns `false` if cancelled before room was available.
    fn reserve(&self, bytes: usize, cancel: &AtomicBool) -> bool {
        if self.limit == usize::MAX {
            self.in_use.fetch_add(bytes, Ordering::Relaxed);
            return true;
        }
        let mut guard = self.mu.lock().unwrap_or_else(|p| p.into_inner());
        loop {
            if cancel.load(Ordering::Acquire) {
                return false;
            }
            let cur = self.in_use.load(Ordering::Acquire);
            // Always allow at least one item even if it exceeds the budget,
            // so that giant payloads still progress.
            if cur == 0 || cur + bytes <= self.limit {
                self.in_use.fetch_add(bytes, Ordering::Relaxed);
                return true;
            }
            // Wait for a release().  Bound the wait so cancellation is
            // observed even if the consumer is also stalled.
            let (g, _) = self
                .cv
                .wait_timeout(guard, Duration::from_millis(50))
                .unwrap_or_else(|p| p.into_inner());
            guard = g;
        }
    }

    fn release(&self, bytes: usize) {
        if self.limit == usize::MAX {
            self.in_use.fetch_sub(bytes, Ordering::Relaxed);
            return;
        }
        self.in_use.fetch_sub(bytes, Ordering::Relaxed);
        // Wake the producer.
        let _g = self.mu.lock();
        self.cv.notify_all();
    }
}

/// Internals of a disk-ordered cursor.
///
/// Owned by the public `DiskOrderedCursor` and lives on the consumer thread.
/// Sending `Self` between threads is not supported (the receiver is a
/// non-`Sync` `Receiver`).
pub struct DiskOrderedCursorImpl {
    /// Receives `(key, data)` items (or errors) from the producer thread.
    rx: mpsc::Receiver<DocItem>,
    /// Joined by `shutdown()`.
    handle: Option<JoinHandle<()>>,
    /// Set to true to signal the producer to stop.
    cancel: Arc<AtomicBool>,
    /// Memory budget — consumer releases bytes on each recv.
    budget: Arc<MemoryBudget>,
    /// Whether next_entry() has observed end-of-stream.
    drained: bool,
    /// Sticky terminal error from the producer (latched on the first
    /// `Err` received so subsequent calls keep returning it).
    terminal_err: Option<DbiError>,
}

impl DiskOrderedCursorImpl {
    /// Construct and start a disk-ordered cursor over the given target
    /// databases.
    ///
    /// `log_manager == None` produces a cursor that immediately reaches
    /// end-of-stream — no entries can be returned because the environment
    /// has no WAL to scan.
    pub fn open(
        log_manager: Option<Arc<LogManager>>,
        target_db_ids: Vec<DatabaseId>,
        opts: DiskOrderedCursorOptions,
    ) -> Result<Self> {
        let queue_size = opts.queue_size.max(1);
        let (tx, rx) = mpsc::sync_channel::<DocItem>(queue_size);
        let cancel = Arc::new(AtomicBool::new(false));
        let budget = Arc::new(MemoryBudget::new(opts.internal_memory_limit));

        let handle = match log_manager {
            Some(lm) => {
                let cancel_p = Arc::clone(&cancel);
                let budget_p = Arc::clone(&budget);
                let tx_p = tx;
                let opts_p = opts;
                let target = target_db_ids;
                let builder = thread::Builder::new()
                    .name("noxu-disk-ordered-cursor".to_string());
                let h = builder
                    .spawn(move || {
                        produce(lm, target, opts_p, tx_p, cancel_p, budget_p)
                    })
                    .map_err(|e| {
                        DbiError::OperationFailed(format!(
                            "failed to spawn disk-ordered-cursor producer: {e}"
                        ))
                    })?;
                Some(h)
            }
            None => {
                // No log: drop tx so rx returns Disconnected immediately.
                drop(tx);
                None
            }
        };

        Ok(Self {
            rx,
            handle,
            cancel,
            budget,
            drained: false,
            terminal_err: None,
        })
    }

    /// Receive the next `(key, data)` tuple.
    ///
    /// Returns `Ok(None)` at end-of-log.  Once `None` has been returned,
    /// every subsequent call also returns `Ok(None)`.  After a producer
    /// error every subsequent call returns the same error (latched).
    pub fn next_entry(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if let Some(e) = &self.terminal_err {
            return Err(clone_dbi_err(e));
        }
        if self.drained {
            return Ok(None);
        }
        loop {
            match self.rx.recv_timeout(Duration::from_millis(100)) {
                Ok(Ok((k, d))) => {
                    let n = k.len() + d.len();
                    self.budget.release(n);
                    return Ok(Some((k, d)));
                }
                Ok(Err(e)) => {
                    let cloned = clone_dbi_err(&e);
                    self.terminal_err = Some(e);
                    return Err(cloned);
                }
                Err(RecvTimeoutError::Timeout) => {
                    if self.cancel.load(Ordering::Acquire) {
                        self.drained = true;
                        return Ok(None);
                    }
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => {
                    self.drained = true;
                    return Ok(None);
                }
            }
        }
    }

    /// Signal the producer thread to stop and join it.
    ///
    /// Idempotent.  Called by the public `DiskOrderedCursor::close()` and
    /// also by its `Drop` impl, so applications never observe a leaked
    /// thread.
    pub fn shutdown(&mut self) {
        self.cancel.store(true, Ordering::Release);
        // Wake the producer if it is parked in MemoryBudget::reserve().
        {
            let _g = self.budget.mu.lock();
            self.budget.cv.notify_all();
        }
        // Drain remaining items so the producer never blocks on send.
        while self.rx.try_recv().is_ok() {}
        if let Some(h) = self.handle.take() {
            // Join is best-effort — the producer exits promptly once it
            // sees `cancel`.  A panic in the producer is converted to a
            // log message; we don't propagate it because shutdown() is
            // also called from Drop.
            if let Err(e) = h.join() {
                log::warn!(
                    target: "noxu-disk-ordered-cursor",
                    "producer thread panicked during shutdown: {e:?}"
                );
            }
        }
        self.drained = true;
    }
}

impl Drop for DiskOrderedCursorImpl {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Best-effort clone of a `DbiError` so the consumer can latch a copy of
/// the producer's terminal error.  `DbiError` does not derive `Clone`
/// because of its embedded `io::Error`; we degrade those to a string
/// representation.
fn clone_dbi_err(e: &DbiError) -> DbiError {
    match e {
        DbiError::OperationFailed(s) => DbiError::OperationFailed(s.clone()),
        DbiError::IoError(io) => DbiError::OperationFailed(format!(
            "disk-ordered-cursor producer I/O error: {io}"
        )),
        other => DbiError::OperationFailed(format!(
            "disk-ordered-cursor producer error: {other}"
        )),
    }
}

/// Producer thread body.
///
/// Walks every log file in ascending order, scans entries via
/// `FileManagerLogScanner::scan_forward` per file, filters to LN entries
/// belonging to a target db, and pushes results onto `tx`.
fn produce(
    log_manager: Arc<LogManager>,
    target_db_ids: Vec<DatabaseId>,
    opts: DiskOrderedCursorOptions,
    tx: SyncSender<DocItem>,
    cancel: Arc<AtomicBool>,
    budget: Arc<MemoryBudget>,
) {
    let target_set: HashSet<u64> = target_db_ids
        .iter()
        .map(|d| d.as_i64() as u64)
        .collect();
    let fm = Arc::clone(log_manager.file_manager());
    let scanner = FileManagerLogScanner::new(fm);

    let file_nums = match log_manager.file_manager().list_file_numbers() {
        Ok(v) => v,
        Err(e) => {
            let _ = tx.send(Err(DbiError::OperationFailed(format!(
                "list_file_numbers: {e}"
            ))));
            return;
        }
    };

    let mut dedup: Option<HashSet<Vec<u8>>> =
        opts.dedup_keys.then(HashSet::new);
    let mut counter_since_check = 0usize;

    for &file_num in &file_nums {
        if cancel.load(Ordering::Acquire) {
            return;
        }
        let start = noxu_util::Lsn::new(file_num, 0);
        let end = noxu_util::Lsn::new(file_num.saturating_add(1), 0);
        let entries: Vec<PositionedEntry> = scanner.scan_forward(start, end);
        for pe in entries {
            counter_since_check += 1;
            if counter_since_check >= 64
                || counter_since_check >= opts.lsn_batch_size
            {
                counter_since_check = 0;
                if cancel.load(Ordering::Acquire) {
                    return;
                }
            }

            let LogEntry::Ln(ln) = pe.entry else { continue };

            // Skip deletes — JE returns only live records.
            if matches!(ln.operation, LnOperation::Delete) || ln.data.is_none()
            {
                continue;
            }
            if !target_set.contains(&ln.db_id) {
                continue;
            }

            let key_bytes: Bytes = ln.key;
            let data_bytes: Bytes = ln.data.unwrap_or_default();

            let key_vec = key_bytes.to_vec();
            let data_vec = if opts.keys_only {
                Vec::new()
            } else {
                data_bytes.to_vec()
            };

            if let Some(set) = dedup.as_mut() {
                if !set.insert(key_vec.clone()) {
                    continue;
                }
            }

            // Reserve budget before sending.  If reserve returns false,
            // the consumer cancelled — exit promptly.
            let n = key_vec.len() + data_vec.len();
            if !budget.reserve(n, &cancel) {
                return;
            }

            // Backpressure on the channel itself: send blocks when the
            // bounded queue is full.
            if tx.send(Ok((key_vec, data_vec))).is_err() {
                // Receiver dropped — consumer is gone.
                budget.release(n);
                return;
            }
        }
    }
    // Falling out of the loop closes `tx` (drop on return), which the
    // consumer observes as Disconnected → end-of-log.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_with_no_log_manager_yields_empty() {
        let mut doc = DiskOrderedCursorImpl::open(
            None,
            vec![DatabaseId::new(1)],
            DiskOrderedCursorOptions::default(),
        )
        .unwrap();
        assert_eq!(doc.next_entry().unwrap(), None);
        // Idempotent end-of-stream.
        assert_eq!(doc.next_entry().unwrap(), None);
    }

    #[test]
    fn shutdown_is_idempotent() {
        let mut doc = DiskOrderedCursorImpl::open(
            None,
            vec![DatabaseId::new(1)],
            DiskOrderedCursorOptions::default(),
        )
        .unwrap();
        doc.shutdown();
        doc.shutdown();
        assert_eq!(doc.next_entry().unwrap(), None);
    }

    #[test]
    fn budget_release_balances_reserve() {
        let b = MemoryBudget::new(1024);
        let cancel = AtomicBool::new(false);
        assert!(b.reserve(512, &cancel));
        assert_eq!(b.in_use.load(Ordering::Relaxed), 512);
        b.release(512);
        assert_eq!(b.in_use.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn budget_unbounded_short_circuits() {
        let b = MemoryBudget::new(usize::MAX);
        let cancel = AtomicBool::new(false);
        assert!(b.reserve(1_000_000, &cancel));
        b.release(1_000_000);
        assert_eq!(b.in_use.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn budget_cancel_unblocks_reserve() {
        use std::thread;
        let b = Arc::new(MemoryBudget::new(8));
        let cancel = Arc::new(AtomicBool::new(false));
        let cancel2 = Arc::clone(&cancel);
        let b2 = Arc::clone(&b);
        // Saturate the budget.
        assert!(b.reserve(8, &cancel));
        let h = thread::spawn(move || {
            // This call should block until cancel fires.
            b2.reserve(8, &cancel2)
        });
        thread::sleep(Duration::from_millis(20));
        cancel.store(true, Ordering::Release);
        let _g = b.mu.lock();
        b.cv.notify_all();
        drop(_g);
        let res = h.join().unwrap();
        assert!(!res, "reserve should return false when cancel fires");
    }
}
