//! Pool of log buffers for managing write buffering.
//!
//! `LogBufferPool` manages a circular pool of [`LogBuffer`]s.
//! The `currentWriteBuffer` is the buffer that is currently used to add data.
//! When the buffer is full, the next (adjacent) buffer is made available for
//! writing. The buffer pool has a dirty list of buffers. A buffer becomes a
//! member of the dirty list when the `currentWriteBuffer` is moved to another
//! buffer. Buffers are removed from the dirty list when they are written.
//!
//! The `dirtyStart`/`dirtyEnd` variables indicate the list of dirty buffers.
//! A value of -1 for either variable indicates that there are no dirty buffers.
//! These variables are synchronized via the `bufferPoolLatch`. The
//! `LogManager.logWriteLatch` (aka LWL) is used to serialize access to the
//! `currentWriteBuffer`, so that entries are added in write/LSN order.
//!
//! # JE faithfulness note (Part 1 — DRIFT-2)
//!
//! `write_dirty` now calls [`FileManager::write_buffer`] for every dirty buffer
//! in the dirty chain, mirroring `LogBufferPool.writeDirty` →
//! `writeBufferToFile` → `fileManager.writeLogBuffer` in JE.  Prior to this
//! fix the method was a no-op stub that reset the dirty indices without
//! writing any bytes, causing a latent panic ("No free log buffers") under
//! buffer pressure.
//!
//! References:
//! - JE `LogBufferPool.writeDirty` (calls `writeBufferToFile`)
//! - JE `LogBufferPool.writeBufferToFile` (calls `fileManager.writeLogBuffer`)
//! - JE `FileManager.writeLogBuffer` (pwrite via `writeToFile`)

use crate::error::{LogError, Result};
use crate::file_manager::FileManager;
use crate::log_buffer::LogBuffer;
use noxu_latch::ExclusiveLatch;
use noxu_sync::Mutex;
use noxu_util::lsn::Lsn;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Manages a circular pool of [`LogBuffer`]s.
///
/// Ported from `LogBufferPool.java` in JE.
///
/// Holds a reference to the [`FileManager`] so that `write_dirty` can issue
/// the actual `pwrite` calls (JE `writeBufferToFile` → `fileManager.writeLogBuffer`).
pub struct LogBufferPool {
    /// The pool of buffers (typically 3 buffers).
    buffers: Vec<Arc<Mutex<LogBuffer>>>,

    /// Index of the first dirty buffer (-1 if none).
    dirty_start: i32,

    /// Index of the last dirty buffer (-1 if none).
    dirty_end: i32,

    /// Buffer that holds the current log end. All writes go to this buffer.
    /// Protected by the LogManager.logWriteLatch.
    current_write_buffer_index: usize,

    /// Total number of buffers in the pool.
    num_buffers: usize,

    /// Size of each buffer in bytes.
    buffer_size: usize,

    /// Synchronizes access and changes to the buffer pool.
    buffer_pool_latch: ExclusiveLatch,

    /// A minimum LSN property for the pool that can be checked without latching.
    /// An LSN less than min_buffer_lsn is guaranteed not to be in the pool.
    ///
    /// Stored as a shared `Arc<AtomicU64>` (the raw `Lsn::as_u64()`) so that
    /// `LogManager::read_entry` can consult it on the read hot path WITHOUT
    /// taking the global `buffer_pool` mutex — a read whose LSN is older than
    /// any in-memory buffer bypasses the mutex + park loop entirely and goes
    /// straight to the disk/page-cache read (JE `LogBufferPool.java:604`
    /// `getReadBufferByLsn` min-LSN skip; Stage A read-path fix 2026-07).
    min_buffer_lsn: Arc<AtomicU64>,

    /// Statistics counters.
    n_not_resident: u64,
    n_cache_miss: u64,
    n_no_free_buffer: u64,

    /// Reference to the FileManager for issuing the actual pwrite calls in
    /// `write_dirty`.
    ///
    /// JE: `LogBufferPool` holds a reference to the `FileManager` (passed via
    /// the `EnvironmentImpl`) and calls `fileManager.writeLogBuffer()` from
    /// `writeBufferToFile()`.
    file_manager: Arc<FileManager>,
}

impl LogBufferPool {
    /// Creates a new `LogBufferPool` with the given number of buffers and buffer
    /// size, wired to `file_manager` for actual disk writes.
    ///
    /// JE: the pool receives the `FileManager` via `EnvironmentImpl` at
    /// construction time; it calls `fileManager.writeLogBuffer()` from
    /// `writeBufferToFile()`.
    pub fn new(
        num_buffers: usize,
        buffer_size: usize,
        file_manager: Arc<FileManager>,
    ) -> Self {
        let mut buffers = Vec::with_capacity(num_buffers);
        for _ in 0..num_buffers {
            buffers.push(Arc::new(Mutex::new(LogBuffer::new(buffer_size))));
        }

        LogBufferPool {
            buffers,
            dirty_start: -1,
            dirty_end: -1,
            current_write_buffer_index: 0,
            num_buffers,
            buffer_size,
            buffer_pool_latch: ExclusiveLatch::named("LogBufferPool"),
            min_buffer_lsn: Arc::new(AtomicU64::new(0)),
            n_not_resident: 0,
            n_cache_miss: 0,
            n_no_free_buffer: 0,
            file_manager,
        }
    }

    /// Returns the configured log buffer size.
    pub fn get_log_buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// Returns the shared "minimum buffered LSN" handle.
    ///
    /// A read whose LSN is strictly less than this value cannot be in any
    /// in-memory buffer, so the caller may skip the global `buffer_pool` mutex
    /// and read straight from disk/page-cache.  The handle is an
    /// `Arc<AtomicU64>` so `LogManager` can clone it once at construction and
    /// consult it on the read hot path WITHOUT locking the pool (Stage A).
    pub fn min_buffer_lsn_handle(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.min_buffer_lsn)
    }

    /// Gets the current write buffer for writing an entry of size_needed bytes.
    ///
    /// The LWL must be held.
    ///
    /// If size_needed won't fit in currentWriteBuffer, but is LTE the LogBuffer
    /// capacity, we bump the buffer to get an empty currentWriteBuffer. If there
    /// are no free write buffers, then all dirty buffers must be flushed.
    ///
    /// If size_needed is greater than the LogBuffer capacity, flush all dirty
    /// buffers and return an empty (but too small) currentWriteBuffer. The caller
    /// must then write the entry to the file directly.
    pub fn get_write_buffer(
        &mut self,
        size_needed: usize,
        flipped_file: bool,
    ) -> Result<Arc<Mutex<LogBuffer>>> {
        // If we've flipped to a new file or the current buffer is full, handle it
        if flipped_file {
            self.bump_and_write_dirty(size_needed, true)?;

            // JE faithfulness (Part-3, DRIFT-3/7): after flushing all dirty
            // buffers to the old file, fsync and "close" the old file BEFORE
            // the LSN bookkeeping is advanced to the new file.  This mirrors
            // JE `LogBufferPool.getWriteBuffer(flippedFile=true)` which calls
            // `fileManager.syncLogEndAndFinishFile()` after `bumpAndWriteDirty`.
            //
            // At this point `file_manager.current_file_num` still points to
            // the OLD file because `set_last_position` in `log_internal` is
            // called AFTER `get_write_buffer` returns (corrected ordering).
            //
            // Reference: JE `LogBufferPool.getWriteBuffer` (flippedFile=true
            //   branch calls bumpAndWriteDirty then syncLogEndAndFinishFile).
            self.file_manager.sync_log_end_and_finish_file()?;
        } else {
            let current = self.buffers[self.current_write_buffer_index].lock();
            let has_room = current.has_room(size_needed);
            drop(current);

            if !has_room {
                if !self.bump_current(size_needed)? {
                    // Could not bump, need to write dirty buffers
                    self.bump_and_write_dirty(size_needed, false)?;
                } else {
                    let current =
                        self.buffers[self.current_write_buffer_index].lock();
                    let has_room_after_bump = current.has_room(size_needed);
                    drop(current);

                    if !has_room_after_bump {
                        // Item is larger than buffer size, write dirty to prepare for direct write
                        self.bump_and_write_dirty(size_needed, false)?;
                    }
                }
            }
        }

        Ok(Arc::clone(&self.buffers[self.current_write_buffer_index]))
    }

    /// Bumps current write buffer and writes the dirty buffers.
    ///
    /// The LWL must be held.
    fn bump_and_write_dirty(
        &mut self,
        size_needed: usize,
        flush_write_queue: bool,
    ) -> Result<()> {
        if !self.bump_current(size_needed)? {
            // Could not bump, write dirty buffers first
            self.write_dirty(flush_write_queue)?;

            if !self.bump_current(size_needed)? {
                // Should not happen — after writing dirty buffers we should be
                // able to bump. Faithful to JE LogBufferPool.bumpAndWriteDirty
                // (LogBufferPool.java:363), which throws
                // EnvironmentFailureException.unexpectedState rather than
                // aborting the JVM; we return a recoverable LogError so a
                // single wedged write does not crash the whole process.
                return Err(LogError::Internal(
                    "No free log buffers after flushing dirty buffers"
                        .to_string(),
                ));
            }
        }

        // Write the dirty buffers
        self.write_dirty(flush_write_queue)
    }

    /// Moves the current write buffer to the next buffer in the pool.
    ///
    /// The LWL must be held.
    ///
    /// Returns false when the buffer needs flushing but there are no free buffers.
    /// Returns true when the buffer is empty or when the buffer is non-empty and is bumped.
    fn bump_current(&mut self, _size_needed: usize) -> Result<bool> {
        let _guard = self
            .buffer_pool_latch
            .acquire()
            .map_err(|e| LogError::LatchTimeout(e.to_string()))?;

        let current = self.buffers[self.current_write_buffer_index].lock();
        current.latch_for_write();

        // Is there anything in this write buffer?
        if current.get_first_lsn().is_null() {
            current.release();
            return Ok(true);
        }

        // Check if there is an undirty buffer to use
        if self.dirty_start >= 0 {
            let next_slot = self.get_next_slot(self.current_write_buffer_index);
            if next_slot == self.dirty_start as usize {
                self.n_no_free_buffer += 1;
                current.release();
                return Ok(false);
            }
        } else {
            self.dirty_start = self.current_write_buffer_index as i32;
        }

        self.dirty_end = self.current_write_buffer_index as i32;
        self.current_write_buffer_index =
            self.get_next_slot(self.current_write_buffer_index);

        let next_buffer_index = self.current_write_buffer_index;
        let new_initial_buffer_index =
            self.get_next_slot(self.current_write_buffer_index);

        current.release();
        drop(current);

        // Reinit the next buffer
        let mut next_to_use = self.buffers[next_buffer_index].lock();
        next_to_use.reinit();
        drop(next_to_use);

        // Update min_buffer_lsn
        let new_initial_buffer = self.buffers[new_initial_buffer_index].lock();
        let new_min_lsn = new_initial_buffer.get_first_lsn();
        drop(new_initial_buffer);

        if !new_min_lsn.is_null() {
            self.min_buffer_lsn.store(new_min_lsn.as_u64(), Ordering::Release);
        }

        Ok(true)
    }

    /// Returns the next buffer slot number from the input buffer slot number.
    ///
    /// The bufferPoolLatch must be held.
    fn get_next_slot(&self, slot_number: usize) -> usize {
        if slot_number < self.buffers.len() - 1 { slot_number + 1 } else { 0 }
    }

    /// Writes the dirty log buffers to disk.
    ///
    /// Iterates the dirty buffer chain and, for each buffer, calls
    /// [`FileManager::write_buffer`] to issue the actual `pwrite` — matching
    /// JE `LogBufferPool.writeDirty` → `writeBufferToFile` →
    /// `fileManager.writeLogBuffer`.
    ///
    /// Prior to Part-1 (DRIFT-2 fix) this was a no-op stub that reset the
    /// dirty indices without writing any bytes, causing a latent panic ("No
    /// free log buffers") under buffer pressure.
    ///
    /// # Safety / locking
    /// The LWL must be held by the caller.  The `bufferPoolLatch` is acquired
    /// internally for the duration of the dirty-list traversal.
    fn write_dirty(&mut self, _flush_write_queue: bool) -> Result<()> {
        let _guard = self
            .buffer_pool_latch
            .acquire()
            .map_err(|e| LogError::LatchTimeout(e.to_string()))?;

        if self.dirty_start < 0 {
            return Ok(());
        }

        let mut current_dirty = self.dirty_start as usize;
        // JE: `writeDirty` iterates [dirtyStart..dirtyEnd], writes each buffer
        // via `writeBufferToFile` → `fileManager.writeLogBuffer`, then resets
        // dirtyStart/dirtyEnd to -1.  We replicate that exactly here.
        loop {
            let is_last = current_dirty == self.dirty_end as usize;

            {
                let mut buffer = self.buffers[current_dirty].lock();
                // Wait for all writers to drain their pin counts and latch the
                // buffer — JE `lb.waitForZeroAndLatch()`.
                buffer.wait_for_zero_and_latch();

                // Write unflushed bytes to disk — JE `writeBufferToFile` →
                // `fileManager.writeLogBuffer` → pwrite.
                // Use write_buffer_to_file(first_lsn.file_number()) so dirty
                // buffers from the OLD file are written to the OLD file even
                // after current_file_num has advanced to the new file.
                let first_lsn = buffer.get_first_lsn();
                if !first_lsn.is_null() {
                    let unflushed = buffer.get_unflushed_data();
                    if !unflushed.is_empty() {
                        let offset = buffer.flushed_file_offset();
                        // Clone to avoid holding the buffer borrow across the
                        // write call (FileManager may acquire internal locks).
                        let data = unflushed.to_vec();
                        buffer.mark_flushed();
                        buffer.release();
                        drop(buffer);

                        self.file_manager.write_buffer_to_file(
                            first_lsn.file_number(),
                            &data,
                            offset,
                        )?;
                    } else {
                        buffer.release();
                        drop(buffer);
                    }
                } else {
                    buffer.release();
                    drop(buffer);
                }
            }

            if is_last {
                break;
            }
            current_dirty = self.get_next_slot(current_dirty);
        }

        self.dirty_start = -1;
        self.dirty_end = -1;
        Ok(())
    }

    /// Finds a buffer that contains the given LSN location.
    ///
    /// No latches need be held.
    ///
    /// Returns the buffer that contains the given LSN location, latched and ready
    /// to read, or returns None.
    pub fn get_read_buffer_by_lsn(
        &mut self,
        lsn: Lsn,
    ) -> Result<Option<Arc<Mutex<LogBuffer>>>> {
        self.n_not_resident += 1;

        // Avoid latching if the LSN is known not to be in the pool
        if lsn.as_u64() < self.min_buffer_lsn.load(Ordering::Acquire) {
            self.n_cache_miss += 1;
            return Ok(None);
        }

        // Latch and check the buffer pool
        let _guard = self
            .buffer_pool_latch
            .acquire()
            .map_err(|e| LogError::LatchTimeout(e.to_string()))?;

        // Stage A part 3: check the SETTLED (historical) buffers before the
        // active write buffer.  The active buffer
        // (`current_write_buffer_index`) is the one that holds writers'
        // outstanding pins (`write_pin_count > 0`), so `contains_lsn` on it may
        // have to wait_for_zero_and_latch (park).  Settled buffers have a zero
        // pin count, so they latch immediately.  Most reads on a
        // dataset>>cache workload want already-settled data, so checking those
        // first avoids the park in the common case (JE
        // `LogBufferPool.getReadBufferByLsn` TODO: current write buffer last).
        let active = self.current_write_buffer_index;
        for (i, buffer_arc) in self.buffers.iter().enumerate() {
            if i == active {
                continue;
            }
            let buffer = buffer_arc.lock();
            if buffer.contains_lsn(lsn) {
                // Buffer is latched by contains_lsn if it returns true
                drop(buffer);
                return Ok(Some(Arc::clone(buffer_arc)));
            }
            drop(buffer);
        }

        // Finally check the active write buffer (may park on its pin count).
        {
            let buffer_arc = &self.buffers[active];
            let buffer = buffer_arc.lock();
            if buffer.contains_lsn(lsn) {
                drop(buffer);
                return Ok(Some(Arc::clone(buffer_arc)));
            }
            drop(buffer);
        }

        self.n_cache_miss += 1;
        Ok(None)
    }

    /// Returns a snapshot of all buffer arcs in the pool.
    ///
    /// Used by `LogManager::flush_dirty_buffers()` to drain all buffers to
    /// disk, matching `LogBufferPool.writeDirty()` traversal.
    pub fn get_all_buffers(&self) -> Vec<Arc<Mutex<LogBuffer>>> {
        self.buffers.clone()
    }

    /// Returns statistics about buffer pool usage.
    pub fn get_stats(&self) -> BufferPoolStats {
        BufferPoolStats {
            num_buffers: self.num_buffers,
            buffer_size: self.buffer_size,
            n_not_resident: self.n_not_resident,
            n_cache_miss: self.n_cache_miss,
            n_no_free_buffer: self.n_no_free_buffer,
        }
    }
}

/// Statistics for the buffer pool.
#[derive(Debug, Clone)]
pub struct BufferPoolStats {
    pub num_buffers: usize,
    pub buffer_size: usize,
    pub n_not_resident: u64,
    pub n_cache_miss: u64,
    pub n_no_free_buffer: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_manager::FileManager;
    use tempfile::TempDir;

    /// Create a pool backed by a real (but temp-dir) FileManager.
    fn make_pool(
        num_buffers: usize,
        buffer_size: usize,
    ) -> (LogBufferPool, TempDir) {
        let dir = TempDir::new().unwrap();
        let fm = Arc::new(
            FileManager::new(dir.path(), false, 100_000_000, 10).unwrap(),
        );
        let pool = LogBufferPool::new(num_buffers, buffer_size, fm);
        (pool, dir)
    }

    #[test]
    fn test_new_pool() {
        let (pool, _dir) = make_pool(3, 1024);
        assert_eq!(pool.get_log_buffer_size(), 1024);
        assert_eq!(pool.num_buffers, 3);
    }

    #[test]
    fn test_get_write_buffer() {
        let (mut pool, _dir) = make_pool(3, 1024);
        let buffer =
            pool.get_write_buffer(100, false).expect("get_write_buffer");
        let buf = buffer.lock();
        assert!(buf.has_room(100));
    }

    #[test]
    fn test_buffer_cycling() {
        let (mut pool, _dir) = make_pool(3, 100);

        // Fill first buffer
        {
            let buffer =
                pool.get_write_buffer(50, false).expect("get_write_buffer");
            let buf = buffer.lock();
            buf.latch_for_write();
            buf.register_lsn(Lsn::new(0, 0));
            buf.allocate(50);
            buf.release();
        }

        // Request more space, should bump to next buffer
        {
            let buffer =
                pool.get_write_buffer(60, false).expect("get_write_buffer");
            let buf = buffer.lock();
            assert!(buf.has_room(60));
        }
    }

    #[test]
    fn test_get_next_slot() {
        let (pool, _dir) = make_pool(3, 1024);
        assert_eq!(pool.get_next_slot(0), 1);
        assert_eq!(pool.get_next_slot(1), 2);
        assert_eq!(pool.get_next_slot(2), 0); // Wrap around
    }

    #[test]
    fn test_stats_initial() {
        let (pool, _dir) = make_pool(3, 1024);
        let stats = pool.get_stats();
        assert_eq!(stats.num_buffers, 3);
        assert_eq!(stats.buffer_size, 1024);
        assert_eq!(stats.n_not_resident, 0);
        assert_eq!(stats.n_cache_miss, 0);
        assert_eq!(stats.n_no_free_buffer, 0);
    }

    #[test]
    fn test_read_buffer_lsn_below_min_is_miss() {
        let (mut pool, _dir) = make_pool(3, 1024);
        // min_buffer_lsn starts at 0, so any LSN >= Lsn(0,0) could be
        // searched. We force a cache miss by searching for a high LSN
        // in a pool whose buffers have no registered LSNs yet.
        let lsn = Lsn::new(99, 5000);
        let result =
            pool.get_read_buffer_by_lsn(lsn).expect("get_read_buffer_by_lsn");
        assert!(result.is_none());
        assert_eq!(pool.get_stats().n_cache_miss, 1);
    }

    #[test]
    fn test_write_buffer_has_enough_space() {
        let (mut pool, _dir) = make_pool(3, 512);
        let buf = pool.get_write_buffer(256, false).expect("get_write_buffer");
        let inner = buf.lock();
        assert!(inner.has_room(256));
    }

    #[test]
    fn test_two_buffers_pool_wraps_around() {
        let (pool, _dir) = make_pool(2, 64);
        assert_eq!(pool.get_next_slot(0), 1);
        assert_eq!(pool.get_next_slot(1), 0);
    }

    // -----------------------------------------------------------------------
    // Part-1 acceptance test (DRIFT-2 fix)
    //
    // FAIL-PRE : before this fix `write_dirty` was a no-op; under buffer
    //            pressure `bump_and_write_dirty` panicked with
    //            "No free log buffers after flushing dirty buffers".
    // PASS-POST: `write_dirty` drains real bytes via `FileManager`; the ring
    //            wraps successfully with no panic and all data is durable.
    // -----------------------------------------------------------------------
    /// Fill all 3 log buffers (tiny 64-byte buffers, 6-byte entries) so the
    /// ring MUST wrap.  Confirmed via `LogManager::read_entry` after flush.
    #[test]
    fn test_write_dirty_drains_ring_no_panic() {
        use crate::entry_type::LogEntryType;
        use crate::log_manager::LogManager;
        use crate::provisional::Provisional;

        let dir = TempDir::new().unwrap();
        let fm = Arc::new(
            FileManager::new(dir.path(), false, 100_000_000, 10).unwrap(),
        );
        // Very small buffers: 3 × 64 bytes.  Each log entry is at least
        // MIN_HEADER_SIZE (14) + payload bytes.  With a 1-byte payload each
        // entry is 15 bytes; ~4 entries per buffer, ~12 before the ring wraps.
        // Writing 20 forces multiple ring-wraps — exactly the pressure path
        // that previously panicked.
        let lm = LogManager::new(Arc::clone(&fm), 3, 64, 4096);

        let mut lsns = Vec::new();
        for i in 0u8..20 {
            let lsn = lm
                .log(LogEntryType::Trace, &[i], Provisional::No, false, false)
                .expect("log() must not panic (DRIFT-2 fix)");
            lsns.push((lsn, i));
        }

        // Flush to disk so we can read back from the cold path.
        lm.flush_no_sync().expect("flush_no_sync");

        // Verify a few entries are readable back.
        for (lsn, expected) in &lsns[..5] {
            let (_, payload) = lm.read_entry(*lsn).expect("read_entry");
            assert_eq!(payload, &[*expected], "payload mismatch at {lsn:?}");
        }
    }
}
