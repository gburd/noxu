//! Central coordinator for log reading and writing.
//!
//!
//! The LogManager supports reading and writing to the log. The writing of
//! data to the log is serialized via the logWriteMutex. Typically space is
//! allocated under the LWL. The client computes the checksum and copies the
//! data into the log buffer (not holding the LWL).
//!
//! # Write path (serialLogWork -> Rust LogManager::log)
//!
//! 1. Under the LWL, determine whether the current file would overflow if we
//!    appended `entry_size` bytes; if so, flip to a new file.
//! 2. Compute `currentLsn` (file number + offset of this entry).
//! 3. Serialise: build the raw byte slice [header | payload].
//! 4. Fill in the checksum and prev_offset in the header.
//! 5. Obtain a write buffer from the pool; allocate a segment.
//! 6. If the entry fits in the pool buffer, register the LSN and copy bytes
//!    into the segment under the LWL.
//!    If the entry is too large for any pool buffer, write directly to the
//!    file via FileManager (also under LWL).
//! 7. Advance next_available_lsn / last_used_lsn in the FileManager.
//! 8. Return the assigned LSN.
//!
//! # Flush/fsync path (flush_sync)
//!
//! Under LWL: collect dirty buffers + pwrite64 (JE logWriteMutex design).
//! Outside LWL: fdatasync via FsyncManager leader/waiter (group-commit).
//! Holding LWL through pwrite64 ensures concurrent threads complete their
//! kernel writes before releasing, so they arrive at FsyncManager
//! simultaneously and coalesce into a single fdatasync.
//!
//! # Read path (getLogEntryFromLogSource -> Rust LogManager::read_entry)
//!
//! 1. Check whether the LSN is still in a write buffer (hot read).
//! 2. If not, open the log file indicated by lsn.file_number().
//! 3. Seek to lsn.file_offset() and read MIN_HEADER_SIZE bytes.
//! 4. Determine whether a VLSN is present (flags byte); if so read 8 more.
//! 5. Validate CRC32 over bytes [CHECKSUM_BYTES..header_size+item_size].
//! 6. Read item_size payload bytes.
//! 7. Return (entry_type, payload_bytes).

use crate::checksum::ChecksumValidator;
use crate::entry_header::{CHECKSUM_BYTES, MAX_HEADER_SIZE, MIN_HEADER_SIZE};
use crate::entry_type::LogEntryType;
use crate::error::{NoxuLogError, Result};
use crate::file_manager::FileManager;
use crate::fsync_manager::FsyncManager;
use crate::log_buffer_pool::LogBufferPool;
use crate::provisional::Provisional;
use crate::write_observer::LogWriteObserver;
use noxu_util::lsn::{Lsn, NULL_LSN};
use noxu_sync::Mutex;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// The central coordinator for log operations.
///
///
pub struct LogManager {
    /// Pool of log buffers for staging writes before they reach the file.
    buffer_pool: Arc<Mutex<LogBufferPool>>,

    /// Serializes all log writes so entries appear in LSN order.
    /// JE calls this the "Log Write Latch" (LWL).
    ///
    /// Held through LSN assignment, memcpy into the write buffer, and the
    /// pwrite64 syscall (matching JE's `logWriteMutex` design).  Holding the
    /// latch through pwrite64 ensures that all concurrent writers complete
    /// their kernel writes before entering `FsyncManager`, so they arrive
    /// simultaneously and the leader/waiter algorithm can coalesce multiple
    /// commits into a single fdatasync.
    log_write_latch: Mutex<()>,

    /// Last flushed LSN (updated when buffers are written to disk).
    last_flush_lsn: AtomicU64,

    /// Statistics.
    n_repeat_fault_reads: AtomicU64,
    n_temp_buffer_writes: AtomicU64,

    /// Buffer size used for fault-in (random read) operations.
    read_buffer_size: usize,

    /// The FileManager that owns the on-disk log files.
    file_manager: Arc<FileManager>,

    /// Coalesces concurrent fsync requests (group commit).
    ///
    ///
    /// Initialised with threshold=0, interval=0 (group commit disabled),
    /// matching default configuration.
    fsync_manager: FsyncManager,

    /// Optional utilization tracking observer.
    ///
    /// When set, called under the LWL for every log entry written:
    ///   - `count_new_entry()` for the freshly assigned LSN
    ///   - `count_obsolete()` when replacing a previous version
    ///
    write_observer: Option<Arc<dyn LogWriteObserver>>,
}

impl LogManager {
    /// Creates a new LogManager backed by the given FileManager.
    ///
    /// # Parameters
    /// - `file_manager`     : Shared reference to the FileManager.
    /// - `num_buffers`      : Number of log buffers in the pool (typically 3).
    /// - `buffer_size`      : Size of each log buffer in bytes (default 1 MB).
    /// - `read_buffer_size` : Size of buffer for fault-in read operations.
    pub fn new(
        file_manager: Arc<FileManager>,
        num_buffers: usize,
        buffer_size: usize,
        read_buffer_size: usize,
    ) -> Self {
        let buffer_pool = LogBufferPool::new(num_buffers, buffer_size);

        LogManager {
            buffer_pool: Arc::new(Mutex::new(buffer_pool)),
            log_write_latch: Mutex::new(()),
            // 0 means "nothing flushed yet". NULL_LSN = u64::MAX would make
            // flush_sync_if_needed's `already_flushed >= lsn` always true,
            // causing all flushes to be skipped.
            last_flush_lsn: AtomicU64::new(0),
            n_repeat_fault_reads: AtomicU64::new(0),
            n_temp_buffer_writes: AtomicU64::new(0),
            read_buffer_size,
            file_manager,
            // Group commit disabled by default (threshold=0, interval=0),
            // matching LOG_GROUP_COMMIT_THRESHOLD / LOG_GROUP_COMMIT_INTERVAL
            // defaults of 0.
            fsync_manager: FsyncManager::new(0, 0),
            write_observer: None,
        }
    }

    /// Reconfigures the group-commit parameters.
    ///
    /// Can be called after construction (e.g. from `EnvironmentImpl::open()`
    /// after applying `EnvironmentConfig`).
    ///
    /// - `threshold`   : min concurrent waiters before leader fsyncs immediately (0 = disabled)
    /// - `interval_ms` : max ms the leader waits for more waiters (0 = disabled)
    pub fn set_group_commit(&mut self, threshold: usize, interval_ms: u64) {
        self.fsync_manager = FsyncManager::new(threshold, interval_ms);
    }

    /// Installs the utilization tracking observer.
    ///
    /// Called by `EnvironmentImpl::open()` after creating the `LogManager` and
    /// `UtilizationTracker`.  The observer is called under the LWL on every
    /// log write so that utilization accounting is always consistent with the
    /// on-disk log.
    ///
    /// Receiving `envImpl.getUtilizationTracker()`.
    pub fn set_write_observer(&mut self, observer: Arc<dyn LogWriteObserver>) {
        self.write_observer = Some(observer);
    }

    /// Logs a raw entry to the WAL, optionally marking an old LSN obsolete.
    ///
    /// This is the main write path.  The caller must have already serialised
    /// the entry payload; this method builds the full on-disk record:
    ///
    /// ```text
    /// [checksum: u32 LE] [entry_type: u8] [flags: u8]
    /// [prev_offset: u32 LE] [item_size: u32 LE]
    /// [vlsn?: i64 LE]   <- only when provisional == Replicated
    /// [payload bytes]
    /// ```
    ///
    /// When `old_lsn` is `Some`, the observer is notified that the previous
    /// version at that LSN is now obsolete (the: `countObsoleteNode`).
    ///
    /// # Parameters
    /// - `entry_type`     : Log entry type.
    /// - `payload`        : Serialised payload bytes (excludes header).
    /// - `provisional`    : Provisional status flag.
    /// - `flush_required` : If true, flush all dirty buffers after logging.
    /// - `fsync_required` : If true, also fsync after flushing.
    /// - `old_lsn`        : Previous LSN for this slot, if any (used for
    ///   utilization tracking).
    ///
    /// # Returns
    /// The LSN assigned to this log entry.
    pub fn log_with_old_lsn(
        &self,
        entry_type: LogEntryType,
        payload: &[u8],
        provisional: Provisional,
        flush_required: bool,
        fsync_required: bool,
        old_lsn: Option<Lsn>,
    ) -> Result<Lsn> {
        self.log_internal(entry_type, payload, provisional, flush_required, fsync_required, old_lsn)
    }

    /// Logs a raw entry (header + payload already serialised) to the WAL.
    ///
    /// This is the main write path.  The caller must have already serialised
    /// the entry payload; this method builds the full on-disk record:
    ///
    /// ```text
    /// [checksum: u32 LE] [entry_type: u8] [flags: u8]
    /// [prev_offset: u32 LE] [item_size: u32 LE]
    /// [vlsn?: i64 LE]   <- only when provisional == Replicated
    /// [payload bytes]
    /// ```
    ///
    /// # Parameters
    /// - `entry_type`     : Log entry type.
    /// - `payload`        : Serialised payload bytes (excludes header).
    /// - `provisional`    : Provisional status flag.
    /// - `flush_required` : If true, flush all dirty buffers after logging.
    /// - `fsync_required` : If true, also fsync after flushing.
    ///
    /// # Returns
    /// The LSN assigned to this log entry.
    pub fn log(
        &self,
        entry_type: LogEntryType,
        payload: &[u8],
        provisional: Provisional,
        flush_required: bool,
        fsync_required: bool,
    ) -> Result<Lsn> {
        self.log_internal(entry_type, payload, provisional, flush_required, fsync_required, None)
    }

    fn log_internal(
        &self,
        entry_type: LogEntryType,
        payload: &[u8],
        provisional: Provisional,
        flush_required: bool,
        fsync_required: bool,
        old_lsn: Option<Lsn>,
    ) -> Result<Lsn> {
        // Build the header bytes + payload into one contiguous buffer so we
        // can compute the checksum in one pass (matching approach).
        let item_size = payload.len() as u32;
        let header_size = MIN_HEADER_SIZE; // no VLSN for non-replicated entries

        // Pre-allocate the full buffer: [header | payload]
        let entry_size = header_size + item_size as usize;
        let mut entry_buf = vec![0u8; entry_size];

        // Fill in the header fields (checksum and prev_offset filled later).
        // Layout: [checksum:4][type:1][flags:1][prev_offset:4][item_size:4]
        entry_buf[4] = entry_type.type_num();                // type

        let flags: u8 = match provisional {
            Provisional::Yes => 0x80,
            Provisional::BeforeCkptEnd => 0x40,
            Provisional::No => 0x00,
        };
        entry_buf[5] = flags;                               // flags
        // prev_offset at [6..10] filled after we know it
        entry_buf[10..14].copy_from_slice(&item_size.to_le_bytes()); // item_size
        // payload
        entry_buf[header_size..].copy_from_slice(payload);

        // Acquire the LWL - all LSN assignment and file position advancement
        // happens under this latch, matching serialLog/serialLogWork.
        let lsn = {
            let _lwl = self.log_write_latch.lock();

            // Determine whether a file flip is needed before assigning the LSN.
            // ShouldFlipFile -> calculateNextLsn -> advanceLsn
            let next_lsn = self.file_manager.get_next_available_lsn();
            let current_file = next_lsn.file_number();

            let flipped = {
                let file_offset = next_lsn.file_offset() as u64;
                file_offset + entry_size as u64
                    > self.file_manager.max_file_size()
            };

            let (current_lsn, file_num) = if flipped {
                let new_file = current_file + 1;
                let first_offset =
                    crate::file_manager::first_log_entry_offset();
                (Lsn::new(new_file, first_offset), new_file)
            } else {
                (next_lsn, current_file)
            };

            // prev_offset: offset of the last used LSN in the same file, or 0
            // when this is the first entry in the file (the: advanceLsn).
            let last_used = self.file_manager.get_last_used_lsn();
            let prev_offset: u32 = if last_used.is_null()
                || last_used.file_number() != file_num
            {
                // Either first ever entry, or first entry in this new file.
                0
            } else {
                last_used.file_offset()
            };

            // Patch prev_offset into the header buffer.
            entry_buf[6..10].copy_from_slice(&prev_offset.to_le_bytes());

            // Compute CRC32 over bytes [CHECKSUM_BYTES..entry_size].
            // skips the first 4 bytes (the checksum field itself) when
            // computing the checksum.
            let crc = ChecksumValidator::compute_range(
                &entry_buf,
                CHECKSUM_BYTES,
                entry_size - CHECKSUM_BYTES,
            );
            entry_buf[0..4].copy_from_slice(&crc.to_le_bytes());

            // Advance LSN bookkeeping in the FileManager so that the next
            // call to get_next_available_lsn() returns the correct value.
            // We do this before the actual file write (matching JE).
            let new_next = Lsn::new(
                file_num,
                current_lsn.file_offset() + entry_size as u32,
            );
            self.file_manager.set_last_position(new_next, current_lsn);

            // Utilization tracking — called under the LWL, matching the
            // serialLogWork() tracker calls.
            if let Some(obs) = &self.write_observer {
                // Mark old version obsolete (the: countObsoleteNode).
                if let Some(old) = old_lsn
                    && !old.is_null()
                {
                    obs.count_obsolete(
                        old.file_number(),
                        old.file_offset(),
                        0, // size unknown at this point
                        entry_type.is_ln_type(),
                    );
                }
                // Count the new entry (the: countNewLogEntry).
                obs.count_new_entry(
                    current_lsn.file_number(),
                    current_lsn.file_offset(),
                    entry_size as u32,
                    entry_type.is_ln_type(),
                    entry_type.is_in_type(),
                );
            }

            // Obtain a write buffer that can hold entry_size bytes.
            let buffer_arc = {
                let mut pool = self.buffer_pool.lock();
                pool.get_write_buffer(entry_size, flipped)?
            };
            let mut buffer = buffer_arc.lock();

            buffer.latch_for_write();
            let segment_opt = buffer.allocate(entry_size);

            match segment_opt {
                Some(segment) => {
                    // Entry fits in the write buffer.
                    buffer.register_lsn(current_lsn);
                    buffer.release();
                    drop(buffer);

                    // Copy bytes into the buffer segment outside the latch.
                    segment.put(&entry_buf);
                }
                None => {
                    // Entry is too large for any pool buffer - write directly
                    // to the file, as does in serialLogWork.
                    buffer.release();
                    drop(buffer);

                    self.n_temp_buffer_writes.fetch_add(1, Ordering::Relaxed);

                    self.file_manager.write_buffer(
                        &entry_buf,
                        current_lsn.file_offset() as u64,
                    )?;
                }
            }

            current_lsn
        };
        // LWL released here.

        // Flush / fsync if requested, outside the LWL (matching JE).
        // Use flush_sync_if_needed(lsn) rather than flush_sync() so that a
        // concurrent committer whose data was already flushed by a racing
        // leader thread can return immediately.  One thread flushes all
        // pending writes; the others see last_flush_lsn > their_commit_lsn
        // and skip the I/O entirely.
        // This is the JE LogManager.flushTo(lsn) coalescing optimisation.
        if fsync_required {
            self.flush_sync_if_needed(lsn)?;
        } else if flush_required {
            self.flush_no_sync()?;
        }

        Ok(lsn)
    }

    /// Flushes all dirty write buffers to disk and performs an fdatasync.
    ///
    /// This is the durable commit path.  The implementation mirrors the
    /// group-commit pattern (`FSyncManager`):
    ///
    /// 1. Acquire the LWL, drain all dirty write buffers to disk, then
    ///    **release the LWL**.  Releasing before the fsync is the key to
    ///    group commit: other threads can now enter `flush_sync()` and add
    ///    their writes to the same batch.
    /// 2. Call `fsync_manager.fsync()` **outside** the LWL.  Concurrent
    ///    callers elect one leader; the leader does a single fdatasync and
    ///    all waiters return together.  This turns N per-commit fsyncs into
    ///    ≈1 fsync for a burst of N concurrent commits (identical to the
    ///    `FSyncManager.fsync()` flow).
    ///
    /// Returns the total number of fdatasync calls performed by this log manager.
    ///
    /// Stat in `EnvironmentStats`.
    pub fn fsync_count(&self) -> u64 {
        self.fsync_manager.fsync_count()
    }

    /// → `FSyncManager.fsync()`.
    ///
    /// Three-phase write coalescing:
    ///
    /// Phase 1 — under LWL (includes pwrite64, matching JE logWriteMutex):
    ///   snapshot each dirty buffer's pending bytes, do pwrite64 while still
    ///   holding the LWL, then release the LWL.  Holding the LWL through
    ///   pwrite64 means that all concurrent committers complete their kernel
    ///   writes before the next thread acquires LWL, so they all arrive at
    ///   `FsyncManager` nearly simultaneously — enabling fsync coalescing
    ///   without a separate group-commit window.
    ///
    /// Phase 2 — outside LWL (fdatasync via FsyncManager): multiple concurrent
    ///   callers elect one leader; the leader calls fdatasync once while waiters
    ///   piggyback, matching JE's FSyncManager group-commit flow.
    pub fn flush_sync(&self) -> Result<Lsn> {
        // Under LWL: snapshot dirty buffers and pwrite64 (JE logWriteMutex
        // design).  Holding through pwrite64 ensures threads serialise their
        // kernel writes and then all arrive at FsyncManager simultaneously,
        // allowing the leader/waiter algorithm to coalesce fsyncs.
        let eol = {
            let _lwl = self.log_write_latch.lock();
            let pending = self.collect_dirty_buffers();
            let eol = self.file_manager.get_next_available_lsn();
            for (data, offset) in pending {
                self.file_manager.write_buffer(&data, offset)?;
            }
            eol
        };
        // LWL released — all pwrite64s done, data in kernel page cache.
        // Concurrent waiters on LWL are now released and will each complete
        // their own pwrite64 under LWL, then enter FsyncManager ~
        // simultaneously, enabling coalesced fdatasync.

        let fm = &self.file_manager;
        self.fsync_manager.fsync(|| {
            fm.sync_log_end().map_err(|e| {
                std::io::Error::other(e.to_string())
            })
        })?;

        self.last_flush_lsn.store(eol.as_u64(), Ordering::Release);
        Ok(eol)
    }

    /// Port of JE `LogManager.flushTo(lsn)`:
    /// flush and fsync only if `lsn` has not yet been flushed.
    ///
    /// Fast path: if `last_flush_lsn >= lsn`, return immediately — a
    /// concurrent or preceding `flush_sync()` already covers our data.
    /// Slow path: call the full `flush_sync()`.
    ///
    /// This is the key coalescing primitive for concurrent commit throughput.
    /// Example with 8 concurrent writers:
    ///
    /// 1. Thread A calls flush_sync() first; its LWL snapshot captures ALL
    ///    pending writes from threads A–H; updates last_flush_lsn past all.
    /// 2. Threads B–H call flush_sync_if_needed(their_commit_lsn) and each
    ///    sees last_flush_lsn >= their_commit_lsn → skip fsync immediately.
    ///
    /// Result: 1 fdatasync for 8 commits (8:1 coalescing, no config needed).
    pub fn flush_sync_if_needed(&self, lsn: Lsn) -> Result<Lsn> {
        // NULL_LSN (= u64::MAX) means "no write LSN known" — always flush.
        // last_flush_lsn is initialised to 0 ("nothing flushed") so that a
        // fresh environment never skips the first flush.
        if lsn != NULL_LSN {
            let already_flushed = self.last_flush_lsn.load(Ordering::Acquire);
            // Strict `>`: `eol` in flush_sync() is `get_next_available_lsn()`
            // AFTER the snapshot — the next LSN to be assigned, not the last
            // one written.  So `last_flush_lsn = X` means everything up to
            // (not including) X was flushed.  We need `already_flushed > lsn`
            // to guarantee `lsn` was included.  Equality means the previous
            // flush computed its eol just before our write was allocated — our
            // data was NOT in that flush.
            if already_flushed > lsn.as_u64() {
                return Ok(Lsn::from_u64(already_flushed));
            }
        }
        self.flush_sync()
    }

    /// Flushes all dirty write buffers to disk without an fsync.
    pub fn flush_no_sync(&self) -> Result<Lsn> {
        let eol = {
            let _lwl = self.log_write_latch.lock();
            let pending = self.collect_dirty_buffers();
            let eol = self.file_manager.get_next_available_lsn();
            for (data, offset) in pending {
                self.file_manager.write_buffer(&data, offset)?;
            }
            eol
        };
        self.last_flush_lsn.store(eol.as_u64(), Ordering::Release);
        Ok(eol)
    }

    /// Collects each dirty write buffer's pending bytes under the caller's LWL.
    ///
    /// For each dirty buffer:
    ///   1. Latches the buffer (waits for any in-progress writer to finish).
    ///   2. Snapshots the unflushed byte slice and the current file offset.
    ///   3. Calls `mark_flushed()` so the watermark advances immediately.
    ///   4. Releases the buffer latch.
    ///
    /// Returns a `Vec<(data, file_offset)>` for the caller to write to disk.
    /// Must be called under the LWL; the caller does pwrite64 before releasing
    /// the LWL (JE logWriteMutex design).
    fn collect_dirty_buffers(&self) -> Vec<(Vec<u8>, u64)> {
        let pool = self.buffer_pool.lock();
        let buffers = pool.get_all_buffers();
        drop(pool);

        let mut pending: Vec<(Vec<u8>, u64)> = Vec::new();

        for buf_arc in buffers {
            let mut buf = buf_arc.lock();
            buf.wait_for_zero_and_latch();

            let first_lsn = buf.get_first_lsn();
            if !first_lsn.is_null() {
                let unflushed = buf.get_unflushed_data();
                if !unflushed.is_empty() {
                    let data = unflushed.to_vec();
                    let offset = buf.flushed_file_offset();
                    // Advance the watermark now (under the buffer latch) so a
                    // subsequent collect_dirty_buffers() call sees this range as
                    // already flushed and does not re-collect it.
                    buf.mark_flushed();
                    buf.release();
                    drop(buf);
                    pending.push((data, offset));
                    continue;
                }
            }
            buf.release();
            drop(buf);
        }

        pending
    }

    /// Reads a single log entry from the given LSN.
    ///
    /// 
    ///
    /// Procedure:
    /// 1. Check the write-buffer pool first (hot path).
    /// 2. If not in the pool, read from disk.
    /// 3. Parse the header to determine total size and validate CRC32.
    /// 4. Return `(entry_type, payload_bytes)`.
    ///
    /// # Arguments
    /// * `lsn` - Location of the entry in the log.
    ///
    /// # Returns
    /// `(entry_type, payload_bytes)` for the entry at `lsn`.
    pub fn read_entry(
        &self,
        lsn: Lsn,
    ) -> Result<(LogEntryType, Vec<u8>)> {
        // ------------------------------------------------------------------
        // Hot path: check whether the entry is still in a write buffer.
        // ------------------------------------------------------------------
        {
            let mut pool = self.buffer_pool.lock();
            if let Some(buf_arc) = pool.get_read_buffer_by_lsn(lsn)? {
                let buf = buf_arc.lock();
                // get_bytes returns a slice starting at lsn.file_offset()
                let slice = buf.get_bytes(lsn.file_offset());

                if slice.len() >= MIN_HEADER_SIZE {
                    // Parse enough to know the total entry size.
                    let item_size = u32::from_le_bytes([
                        slice[10], slice[11], slice[12], slice[13],
                    ]) as usize;
                    let flags = slice[5];
                    let vlsn_present = (flags & 0x08) != 0
                        || (flags & 0x20) != 0; // VLSN_PRESENT | REPLICATED
                    let header_size =
                        if vlsn_present { MAX_HEADER_SIZE } else { MIN_HEADER_SIZE };
                    let entry_size = header_size + item_size;

                    if slice.len() >= entry_size {
                        let entry_type_num = slice[4];
                        let payload =
                            slice[header_size..entry_size].to_vec();
                        buf.release();
                        drop(buf);

                        let entry_type =
                            LogEntryType::from_type_num(entry_type_num)
                                .ok_or(NoxuLogError::InvalidEntryType {
                                    type_num: entry_type_num,
                                    lsn,
                                })?;
                        return Ok((entry_type, payload));
                    }
                }

                buf.release();
                drop(buf);
            }
        }

        // ------------------------------------------------------------------
        // Cold path: read from disk.
        // ------------------------------------------------------------------
        self.read_entry_from_disk(lsn)
    }

    /// Reads a log entry from disk at the given LSN.
    ///
    /// Disk-read branch of `LogManager.getLogEntryFromLogSource()`.
    ///
    /// Format on disk (little-endian):
    /// ```text
    /// offset  0: checksum   u32
    /// offset  4: entry_type u8
    /// offset  5: flags      u8
    /// offset  6: prev_offset u32
    /// offset 10: item_size  u32
    /// offset 14: vlsn?      i64  (present when VLSN_PRESENT or REPLICATED flag)
    /// offset 14 or 22: payload bytes[item_size]
    /// ```
    ///
    /// CRC32 is computed over bytes [4..header_size+item_size], i.e. skipping
    /// the first CHECKSUM_BYTES (4) bytes of the header.
    fn read_entry_from_disk(
        &self,
        lsn: Lsn,
    ) -> Result<(LogEntryType, Vec<u8>)> {
        let file_offset = lsn.file_offset() as u64;

        // Step 1: Read the minimum header.
        // Uses the random-read path (point lookup), not sequential scan.
        let mut header_buf = vec![0u8; MIN_HEADER_SIZE];
        let n = self.file_manager.read_from_file_random(
            lsn.file_number(),
            file_offset,
            &mut header_buf,
        )?;
        if n < MIN_HEADER_SIZE {
            return Err(NoxuLogError::UnexpectedEof {
                lsn,
                message: format!(
                    "short read: need {} bytes for header, got {}",
                    MIN_HEADER_SIZE, n
                ),
            });
        }

        // Step 2: Parse header fields.
        let stored_checksum =
            u32::from_le_bytes([header_buf[0], header_buf[1], header_buf[2], header_buf[3]]);
        let entry_type_num = header_buf[4];
        let flags = header_buf[5];
        let item_size =
            u32::from_le_bytes([header_buf[10], header_buf[11], header_buf[12], header_buf[13]])
                as usize;

        // Sanity check item_size before allocating.
        if item_size > 100_000_000 {
            return Err(NoxuLogError::InvalidEntrySize {
                lsn,
                size: item_size as i32,
            });
        }

        // Step 3: Determine whether a VLSN is present (extends the header).
        let vlsn_present = (flags & 0x08) != 0 || (flags & 0x20) != 0;
        let header_size =
            if vlsn_present { MAX_HEADER_SIZE } else { MIN_HEADER_SIZE };
        let entry_size = header_size + item_size;

        // Step 4: Read the full entry (header + payload) in one call.
        let mut full_buf = vec![0u8; entry_size];
        let n = self.file_manager.read_from_file_random(
            lsn.file_number(),
            file_offset,
            &mut full_buf,
        )?;
        if n < entry_size {
            return Err(NoxuLogError::UnexpectedEof {
                lsn,
                message: format!(
                    "short read: need {} bytes for entry, got {}",
                    entry_size, n
                ),
            });
        }

        // Step 5: Validate CRC32.
        // computes the checksum over everything after the checksum field:
        // bytes [CHECKSUM_BYTES..entry_size].
        let computed_crc = ChecksumValidator::compute_range(
            &full_buf,
            CHECKSUM_BYTES,
            entry_size - CHECKSUM_BYTES,
        );
        if computed_crc != stored_checksum {
            return Err(NoxuLogError::Checksum {
                lsn,
                message: format!(
                    "expected {:#x}, got {:#x}",
                    stored_checksum, computed_crc
                ),
            });
        }

        // Step 6: Validate and return the entry type and payload.
        let entry_type =
            LogEntryType::from_type_num(entry_type_num).ok_or(
                NoxuLogError::InvalidEntryType { type_num: entry_type_num, lsn },
            )?;

        let payload = full_buf[header_size..].to_vec();
        Ok((entry_type, payload))
    }

    /// Returns the current end-of-log position.
    pub fn get_end_of_log(&self) -> Lsn {
        self.file_manager.get_next_available_lsn()
    }

    /// Returns the LSN of the last flushed entry.
    pub fn get_last_flush_lsn(&self) -> Lsn {
        Lsn::from_u64(self.last_flush_lsn.load(Ordering::Relaxed))
    }

    /// Returns a reference to the shared FileManager.
    pub fn file_manager(&self) -> &Arc<FileManager> {
        &self.file_manager
    }

    /// Returns statistics about log manager operations.
    pub fn get_stats(&self) -> LogManagerStats {
        let pool = self.buffer_pool.lock();
        let pool_stats = pool.get_stats();

        let io_stats = self.file_manager.get_io_stats();
        LogManagerStats {
            end_of_log: self.get_end_of_log(),
            last_flush_lsn: self.get_last_flush_lsn(),
            n_repeat_fault_reads: self
                .n_repeat_fault_reads
                .load(Ordering::Relaxed),
            n_temp_buffer_writes: self
                .n_temp_buffer_writes
                .load(Ordering::Relaxed),
            buffer_pool_stats: pool_stats,
            n_log_fsyncs: self.fsync_manager.fsync_count(),
            n_fsync_requests: self.fsync_manager.fsync_request_count(),
            n_fsync_timeouts: self.fsync_manager.fsync_timeout_count(),
            n_group_commits: self.fsync_manager.group_commit_count(),
            fsync_time_ms: self.fsync_manager.fsync_time_ms(),
            n_fsync_batch_size_sum: self.fsync_manager.fsync_batch_size_sum(),
            n_file_opens: io_stats.n_file_opens,
            n_sequential_reads: io_stats.n_sequential_reads,
            n_sequential_read_bytes: io_stats.n_sequential_read_bytes,
            n_sequential_writes: io_stats.n_sequential_writes,
            n_sequential_write_bytes: io_stats.n_sequential_write_bytes,
            n_random_reads: io_stats.n_random_reads,
            n_random_read_bytes: io_stats.n_random_read_bytes,
        }
    }
}

/// Statistics for LogManager operations.
#[derive(Debug, Clone)]
pub struct LogManagerStats {
    pub end_of_log: Lsn,
    pub last_flush_lsn: Lsn,
    pub n_repeat_fault_reads: u64,
    pub n_temp_buffer_writes: u64,
    pub buffer_pool_stats: crate::log_buffer_pool::BufferPoolStats,
    /// Number of fsync calls completed (after group-commit coalescing).
    pub n_log_fsyncs: u64,
    /// Number of fsync requests (before coalescing).
    pub n_fsync_requests: u64,
    /// Number of fsync requests that timed out.
    pub n_fsync_timeouts: u64,
    /// Number of group-commit batches (leader served ≥1 waiter).
    pub n_group_commits: u64,
    /// Cumulative fsync duration in milliseconds.
    pub fsync_time_ms: u64,
    /// Sum of all group-commit batch sizes (total waiters served across all batches).
    pub n_fsync_batch_size_sum: u64,
    /// Number of log file opens (cache miss).
    pub n_file_opens: u64,
    /// Number of sequential read operations (recovery scan).
    pub n_sequential_reads: u64,
    /// Total bytes read sequentially.
    pub n_sequential_read_bytes: u64,
    /// Number of sequential write operations.
    pub n_sequential_writes: u64,
    /// Total bytes written sequentially.
    pub n_sequential_write_bytes: u64,
    /// Number of random (point-lookup) read operations.
    pub n_random_reads: u64,
    /// Total bytes from random reads.
    pub n_random_read_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_manager::FileManager;
    use crate::entry_type::LogEntryType;
    use crate::provisional::Provisional;
    use noxu_util::lsn::Lsn;
    use std::sync::Arc;
    use tempfile::TempDir;

    /// Helper: create a LogManager backed by a real FileManager in a temp dir.
    fn make_log_manager(dir: &TempDir) -> LogManager {
        let fm = Arc::new(
            FileManager::new(dir.path(), false, 10_000_000, 100).unwrap(),
        );
        LogManager::new(fm, 3, 1024 * 1024, 4096)
    }

    #[test]
    fn test_new_log_manager() {
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);
        // End-of-log starts at beginning of file 0.
        assert_eq!(lm.get_end_of_log().file_number(), 0);
    }

    #[test]
    fn test_log_entry_returns_lsn() {
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);

        let payload = b"hello world";
        let result =
            lm.log(LogEntryType::Trace, payload, Provisional::No, false, false);
        assert!(result.is_ok(), "log() returned {:?}", result.err());

        let lsn = result.unwrap();
        assert_eq!(lsn.file_number(), 0);
        assert!(!lsn.is_null());
    }

    #[test]
    fn test_flush_operations() {
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);

        // Log something first so there is a file to flush.
        lm.log(LogEntryType::Trace, b"x", Provisional::No, false, false)
            .unwrap();

        assert!(lm.flush_no_sync().is_ok());
        assert!(lm.flush_sync().is_ok());
    }

    #[test]
    fn test_get_stats() {
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);
        let stats = lm.get_stats();
        assert_eq!(stats.buffer_pool_stats.num_buffers, 3);
    }

    #[test]
    fn test_log_multiple_entries_advance_offset() {
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);

        let lsn1 = lm
            .log(LogEntryType::Trace, b"entry1", Provisional::No, false, false)
            .unwrap();
        let lsn2 = lm
            .log(LogEntryType::Trace, b"entry2", Provisional::No, false, false)
            .unwrap();

        // Both entries are in the same file; second must be at a higher offset.
        assert_eq!(lsn1.file_number(), lsn2.file_number());
        assert!(lsn2.file_offset() > lsn1.file_offset());
    }

    #[test]
    fn test_log_provisional_yes() {
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);

        let result =
            lm.log(LogEntryType::BIN, b"provisional_data", Provisional::Yes, false, false);
        assert!(result.is_ok());
    }

    #[test]
    fn test_log_provisional_before_ckpt_end() {
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);

        let result = lm.log(
            LogEntryType::CkptStart,
            b"ckpt",
            Provisional::BeforeCkptEnd,
            false,
            false,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_log_with_flush_required() {
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);

        let result = lm.log(
            LogEntryType::Trace,
            b"flush_me",
            Provisional::No,
            true,  // flush_required
            false,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_log_with_fsync_required() {
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);

        let result = lm.log(
            LogEntryType::TxnCommit,
            b"commit",
            Provisional::No,
            false,
            true, // fsync_required
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_read_entry_after_flush() {
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);

        let payload = b"read_back_test";
        let lsn = lm
            .log(LogEntryType::Trace, payload, Provisional::No, false, false)
            .unwrap();

        // Flush so the entry lands on disk (cold-path read).
        lm.flush_no_sync().unwrap();

        let (entry_type, read_payload) = lm.read_entry(lsn).unwrap();
        assert_eq!(entry_type, LogEntryType::Trace);
        assert_eq!(read_payload, payload);
    }

    #[test]
    fn test_read_entry_hot_path() {
        // Read before flushing — should come from the write buffer.
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);

        let payload = b"hot_read";
        let lsn = lm
            .log(LogEntryType::IN, payload, Provisional::No, false, false)
            .unwrap();

        // Do NOT flush; the entry should still be in the write buffer.
        let (entry_type, read_payload) = lm.read_entry(lsn).unwrap();
        assert_eq!(entry_type, LogEntryType::IN);
        assert_eq!(read_payload, payload);
    }

    #[test]
    fn test_read_entry_multiple_entries() {
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);

        let lsn1 = lm
            .log(LogEntryType::Trace, b"first", Provisional::No, false, false)
            .unwrap();
        let lsn2 = lm
            .log(LogEntryType::BIN, b"second", Provisional::No, false, false)
            .unwrap();

        lm.flush_no_sync().unwrap();

        let (t1, p1) = lm.read_entry(lsn1).unwrap();
        let (t2, p2) = lm.read_entry(lsn2).unwrap();

        assert_eq!(t1, LogEntryType::Trace);
        assert_eq!(p1, b"first");
        assert_eq!(t2, LogEntryType::BIN);
        assert_eq!(p2, b"second");
    }

    #[test]
    fn test_get_last_flush_lsn_updates_after_flush() {
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);

        lm.log(LogEntryType::Trace, b"x", Provisional::No, false, false)
            .unwrap();

        let before = lm.get_last_flush_lsn();
        lm.flush_no_sync().unwrap();
        let after = lm.get_last_flush_lsn();

        // After flushing the last_flush_lsn should advance.
        assert!(after.as_u64() > before.as_u64() || after == lm.get_end_of_log());
    }

    #[test]
    fn test_get_end_of_log_advances_with_writes() {
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);

        let eol_before = lm.get_end_of_log();
        lm.log(LogEntryType::Trace, b"advance", Provisional::No, false, false)
            .unwrap();
        let eol_after = lm.get_end_of_log();

        assert!(eol_after.file_offset() > eol_before.file_offset()
            || eol_after.file_number() > eol_before.file_number());
    }

    #[test]
    fn test_file_manager_accessor() {
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);
        // file_manager() must return the same Arc (same address).
        let fm1 = lm.file_manager();
        let fm2 = lm.file_manager();
        assert!(Arc::ptr_eq(fm1, fm2));
    }

    #[test]
    fn test_stats_fields() {
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);

        lm.log(LogEntryType::Trace, b"stats_test", Provisional::No, false, false)
            .unwrap();

        let stats = lm.get_stats();
        // n_repeat_fault_reads starts at 0 and n_temp_buffer_writes at 0
        // (entry fits in the buffer pool for small payloads).
        assert_eq!(stats.n_repeat_fault_reads, 0);
        assert_eq!(stats.n_temp_buffer_writes, 0);
        assert!(!stats.end_of_log.is_null());
    }

    #[test]
    fn test_log_empty_payload() {
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);

        let lsn = lm
            .log(LogEntryType::Trace, b"", Provisional::No, false, false)
            .unwrap();
        assert!(!lsn.is_null());

        lm.flush_no_sync().unwrap();
        let (entry_type, payload) = lm.read_entry(lsn).unwrap();
        assert_eq!(entry_type, LogEntryType::Trace);
        assert!(payload.is_empty());
    }

    #[test]
    fn test_log_large_payload_round_trip() {
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);

        // 32 KiB payload — fits in the 1 MiB buffer.
        let payload = vec![0xABu8; 32 * 1024];
        let lsn = lm
            .log(LogEntryType::IN, &payload, Provisional::No, false, false)
            .unwrap();

        lm.flush_no_sync().unwrap();
        let (entry_type, read_back) = lm.read_entry(lsn).unwrap();
        assert_eq!(entry_type, LogEntryType::IN);
        assert_eq!(read_back, payload);
    }

    #[test]
    fn test_read_entry_bad_lsn_returns_error() {
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);

        // Write one entry so file 0 exists on disk.
        lm.log(LogEntryType::Trace, b"x", Provisional::No, false, false)
            .unwrap();
        lm.flush_no_sync().unwrap();

        // Try to read from an offset far beyond the written data.
        let bad_lsn = Lsn::new(0, 1_000_000);
        let result = lm.read_entry(bad_lsn);
        assert!(result.is_err());
    }

    #[test]
    fn test_flush_sync_on_empty_log() {
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);
        // flush_sync with nothing written should not panic or fail.
        let result = lm.flush_sync();
        assert!(result.is_ok());
    }

    #[test]
    fn test_flush_no_sync_on_empty_log() {
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);
        let result = lm.flush_no_sync();
        assert!(result.is_ok());
    }
}
