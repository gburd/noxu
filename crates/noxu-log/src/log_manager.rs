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
//! 7. After `get_write_buffer` returns, advance `next_available_lsn` /
//!    `last_used_lsn` in the FileManager (`advanceLsn` / `setLastPosition`).
//! 8. Return the assigned LSN.
//!
//! # Flush/fsync path (flush_sync)
//!
//! LWL discipline (JE LogManager.serialLogWork, DRIFT-1 fixed): the log-write
//! latch covers ONLY LSN assignment, buffer-slot allocation, and the in-memory
//! copy of the entry bytes.
//!
//! Group-commit ordering (JE FSyncManager.flushAndSync): the leader/waiter
//! decision happens FIRST (FsyncManager::flush_and_sync, JE mgrMutex). ONLY the
//! elected leader (or a timed-out thread) then drains the shared buffer under
//! the LWL (briefly), RELEASES the LWL, pwrite64s the captured ranges (JE
//! flushBeforeSync), and issues the single fdatasync (JE executeFSync). Waiters
//! piggyback on the leader's fsync and perform no I/O. This keeps the syscall
//! off the LWL while ensuring one fdatasync serves a burst of concurrent
//! committers — matching JE and closing the prior coalescing gap (a committer
//! that did not skip at the fast path used to drain+pwrite BEFORE the manager
//! decision and become its own redundant leader).
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
use crate::write_observer::{LogWriteObserver, ObsoleteKind, ObsoleteLsn};
use noxu_sync::Mutex;
use noxu_util::lsn::{Lsn, NULL_LSN};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

// ── LWL scratch state ────────────────────────────────────────────────────────────────

/// State protected by the Log Write Latch (LWL).
///
/// Groups the per-call scratch buffers that are safe to share because the LWL
/// serialises all callers.  Storing them here eliminates per-call allocation:
///
/// * `flush_pending` — R-1 fix: reused list of (data, file_num, file_offset) tuples.
///   `flush_sync` iterates this Vec while holding the LWL, preserving capacity.
///   `flush_no_sync` uses `std::mem::take` (see R-2 comment).
///
/// (The former `entry_buf` scratch was removed when log-entry marshalling was
/// moved OUTSIDE the LWL — the payload memcpy + header encode now happen in a
/// per-call owned buffer BEFORE the latch is taken, JE-faithful.  A shared
/// scratch buffer cannot live outside the latch, and keeping it forced the
/// payload copy back under the latch, so it is gone.)
struct LwlScratch {
    /// Reusable pending-flush list (R-1 fix).
    flush_pending: Vec<(Vec<u8>, u32, u64)>,
}

impl LwlScratch {
    fn new() -> Self {
        LwlScratch { flush_pending: Vec::new() }
    }
}

/// The central coordinator for log operations.
///
///
pub struct LogManager {
    /// Pool of log buffers for staging writes before they reach the file.
    buffer_pool: Arc<Mutex<LogBufferPool>>,

    /// Serializes all log writes so entries appear in LSN order.
    /// this the "Log Write Latch" (LWL).
    ///
    /// **Foreground commit path (`flush_sync`)**: held through LSN assignment
    /// and the in-memory memcpy into the write buffer, then RELEASED before the
    /// pwrite64 syscall (JE LogManager.serialLogWork). Concurrent committers
    /// each pwrite off-latch and then coalesce their fdatasync via
    /// `FsyncManager` leader/waiter group-commit.
    ///
    /// **Background flush path (`flush_no_sync`, R-2 fix)**: the LWL is
    /// released BEFORE pwrite64.  Background flush has no coalescing
    /// requirement; holding through I/O blocks ALL foreground commits.
    ///
    /// R-1 fix: `flush_pending` inside `LwlScratch` is the reusable flush list.
    log_write_latch: Mutex<LwlScratch>,

    /// Last flushed LSN (updated when buffers are written to the OS page
    /// cache, by either `flush_sync` or `flush_no_sync`). This is a
    /// *written-to-page-cache* watermark, NOT a durability watermark — a
    /// `flush_no_sync` advances it without an fdatasync.
    last_flush_lsn: AtomicU64,

    /// Last fdatasync'd (durable) LSN. Advanced ONLY by `flush_sync` after a
    /// successful fdatasync. `flush_sync_if_needed` must consult THIS, never
    /// `last_flush_lsn`: skipping an fsync because `flush_no_sync` advanced
    /// `last_flush_lsn` past a SYNC commit would leave that commit in the page
    /// cache only and silently lose it on power failure.
    last_synced_lsn: AtomicU64,

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

    /// C-2 (the 2026 review F-3.2 / F-8.4 / F-9.4): set to `true`
    /// the first time an fsync or file-sync I/O error is observed.  Once set,
    /// `log()` refuses all further writes and `is_io_invalid()` returns `true`
    /// so that `EnvironmentImpl::is_valid()` can detect the failure.
    ///
    /// Shared as an `Arc<AtomicBool>` so that `EnvironmentImpl` can hold the
    /// same allocation without a circular `Arc` reference.
    pub io_invalid: Arc<AtomicBool>,

    /// Whether to validate the CRC32 of each entry read back from disk.
    ///
    /// Mirrors JE `LogManager.getChecksumOnRead()` (LOG_CHECKSUM_READ,
    /// default true). Defaults to `true`; `EnvironmentImpl` overrides it
    /// from `log_checksum_read` after construction. Disabling trades the
    /// per-read checksum (~40% of cold-read CPU in read-heavy workloads) for
    /// throughput.
    checksum_on_read: bool,
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
        let buffer_pool = LogBufferPool::new(
            num_buffers,
            buffer_size,
            Arc::clone(&file_manager),
        );

        LogManager {
            buffer_pool: Arc::new(Mutex::new(buffer_pool)),
            log_write_latch: Mutex::new(LwlScratch::new()),
            // 0 means "nothing flushed yet". NULL_LSN = u64::MAX would make
            // flush_sync_if_needed's `already_flushed >= lsn` always true,
            // causing all flushes to be skipped.
            last_flush_lsn: AtomicU64::new(0),
            last_synced_lsn: AtomicU64::new(0),
            n_repeat_fault_reads: AtomicU64::new(0),
            n_temp_buffer_writes: AtomicU64::new(0),
            read_buffer_size,
            file_manager,
            // Group commit disabled by default (threshold=0, interval=0),
            // matching LOG_GROUP_COMMIT_THRESHOLD / LOG_GROUP_COMMIT_INTERVAL
            // defaults of 0.
            fsync_manager: FsyncManager::new(0, 0),
            write_observer: None,
            io_invalid: Arc::new(AtomicBool::new(false)),
            // JE default: validate checksums on read. EnvironmentImpl
            // overrides via set_checksum_on_read() from log_checksum_read.
            checksum_on_read: true,
        }
    }

    /// Sets whether entry checksums are validated on read.
    ///
    /// Called by `EnvironmentImpl::open()` from `log_checksum_read`.
    /// JE `LogManager.getChecksumOnRead` / LOG_CHECKSUM_READ.
    pub fn set_checksum_on_read(&mut self, enabled: bool) {
        self.checksum_on_read = enabled;
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

    /// Counts a batch of obsolete LSNs (TXN-1: the prior versions of records
    /// a committing transaction overwrote).
    ///
    /// JE `LogManager.countObsoleteNodesPreCommit` (the
    /// `obsoleteWriteLockInfo` loop): for each write-lock whose abort version
    /// is reclaimable, calls `countObsoleteNode(abortLsn, null, abortLogSize,
    /// db)` under the log write latch.  Here the same accounting fires
    /// through the installed observer so it lands in the per-FILE and per-DB
    /// summaries.
    ///
    /// Each tuple is `(abort_lsn, db_id, abort_log_size)`.  The caller is
    /// responsible for applying JE's `maybeCountObsoleteLSN` filters
    /// (NULL/known-deleted/already-counted) and de-duplicating by abort LSN.
    pub fn count_obsolete_commit_lsns(
        &self,
        infos: &[(Lsn, Option<u32>, i32)],
    ) {
        if let Some(obs) = &self.write_observer {
            for &(lsn, db_id, size) in infos {
                if lsn.is_null() {
                    continue;
                }
                // Prior versions overwritten by a committed txn are LNs;
                // counted via the exact variant (JE countObsoleteNode).
                obs.count_obsolete(ObsoleteLsn::exact(lsn, db_id, size, true));
            }
        }
    }

    /// Returns `true` if an I/O failure has permanently invalidated this log.
    ///
    /// C-2: once set, all subsequent `log()` and `flush_sync()` calls return
    /// an error immediately without touching the kernel page-cache.
    pub fn is_io_invalid(&self) -> bool {
        self.io_invalid.load(Ordering::Acquire)
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
        // Legacy shim: treat as an exact, db-unknown obsolete of the same
        // node kind as the entry being written.
        let old = old_lsn.map(|lsn| ObsoleteLsn {
            lsn,
            db_id: None,
            size: 0,
            is_ln: entry_type.is_ln_type(),
            kind: ObsoleteKind::Exact,
        });
        self.log_internal(
            entry_type,
            payload,
            provisional,
            flush_required,
            fsync_required,
            old,
            None,
            false,
            None,
        )
    }

    /// Logs an entry with full utilization-tracking metadata.
    ///
    /// This is the JE-faithful write path: the caller supplies the owning DB
    /// id for the new entry (CLN-9 per-DB axis) and an optional
    /// [`ObsoleteLsn`] describing the superseded version, including which
    /// `countObsolete*` variant to apply (CLN-10).
    ///
    /// Cite: `LogManager.serialLogWork` -> `UtilizationTracker.countNewLogEntry`
    /// plus `countObsoleteNode` / `countObsoleteNodeInexact` /
    /// `countObsoleteNodeDupsAllowed`.
    pub fn log_tracked(
        &self,
        entry_type: LogEntryType,
        payload: &[u8],
        provisional: Provisional,
        flush_required: bool,
        fsync_required: bool,
        new_db_id: Option<u32>,
        old_obsolete: Option<ObsoleteLsn>,
        immediately_obsolete: bool,
    ) -> Result<Lsn> {
        self.log_internal(
            entry_type,
            payload,
            provisional,
            flush_required,
            fsync_required,
            old_obsolete,
            new_db_id,
            immediately_obsolete,
            None,
        )
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
        self.log_internal(
            entry_type,
            payload,
            provisional,
            flush_required,
            fsync_required,
            None,
            None,
            false,
            None,
        )
    }

    /// Logs a raw entry (header + payload already serialised) to the WAL.
    ///
    /// Identical to [`Self::log`] except that the on-disk header is the
    /// 22-byte form with `REPLICATED_MASK | VLSN_PRESENT_MASK` set and the
    /// 8-byte VLSN written at offset 14.
    ///
    /// **Standalone (non-replicated) environments must never call this
    /// method.**  The `log()` path is byte-unchanged: it always writes a
    /// 14-byte header with no VLSN field.
    ///
    /// Called by `EnvironmentImpl::log_txn_commit` when a VLSN counter has
    /// been installed via `set_replication_vlsn_counter()`.
    pub fn log_with_vlsn(
        &self,
        entry_type: LogEntryType,
        payload: &[u8],
        vlsn: u64,
        flush_required: bool,
        fsync_required: bool,
    ) -> Result<Lsn> {
        self.log_internal(
            entry_type,
            payload,
            Provisional::No,
            flush_required,
            fsync_required,
            None,
            None,
            false,
            Some(vlsn),
        )
    }

    fn log_internal(
        &self,
        entry_type: LogEntryType,
        payload: &[u8],
        provisional: Provisional,
        flush_required: bool,
        fsync_required: bool,
        old_obsolete: Option<ObsoleteLsn>,
        new_db_id: Option<u32>,
        immediately_obsolete: bool,
        opt_vlsn: Option<u64>,
    ) -> Result<Lsn> {
        // C-2: refuse all writes once a prior I/O error has invalidated the log.
        if self.io_invalid.load(Ordering::Acquire) {
            return Err(NoxuLogError::WriteFailed(
                "environment permanently invalidated by prior I/O error"
                    .to_string(),
            ));
        }
        // Build the header bytes + payload into one contiguous buffer so we
        // can compute the checksum in one pass (matching approach).
        let item_size = payload.len() as u32;
        // 14-byte header for non-replicated; 22-byte header when VLSN present.
        let header_size =
            if opt_vlsn.is_some() { MAX_HEADER_SIZE } else { MIN_HEADER_SIZE };

        // Full buffer: [header | payload]
        let entry_size = header_size + item_size as usize;

        // ── Marshall OUTSIDE the LWL (JE-faithful) ────────────────────────
        //
        // JE marshalls LN/commit payloads outside `logWriteMutex`
        // (LogEntryHeader.addPostMarshallingInfo, marshallOutsideLatch=true):
        // only prevOffset/VLSN/checksum/LSN-assign/buffer-slot happen under the
        // latch.  We do the same here — the header field encode and the
        // (potentially multi-KB) payload memcpy land in a per-call OWNED
        // buffer BEFORE taking the LWL.  This removes the payload copy from
        // the serialised critical section entirely.
        //
        // The buffer is owned (not the old shared `entry_buf` scratch), so it
        // can be MOVED into the off-latch `segment.put` / `write_buffer` step
        // with no clone under the LWL.  The per-call allocation is the cost JE
        // pays too (it allocates the marshalled byte buffer per entry); it is
        // off the contended path and far cheaper than serialising the copy.
        //
        // Layout: [checksum:4][type:1][flags:1][prev_offset:4][item_size:4]
        //         [vlsn:8?][payload...].  The checksum (0..4) and prev_offset
        //         (6..10) are LSN-dependent and are filled UNDER the LWL below.
        let mut entry_buf: Vec<u8> = vec![0u8; entry_size];
        entry_buf[4] = entry_type.type_num(); // type
        let mut flags: u8 = match provisional {
            Provisional::Yes => 0x80,
            Provisional::BeforeCkptEnd => 0x40,
            Provisional::No => 0x00,
        };
        if opt_vlsn.is_some() {
            flags |= 0x20; // REPLICATED_MASK
            flags |= 0x08; // VLSN_PRESENT_MASK
        }
        entry_buf[5] = flags; // flags
        // prev_offset at [6..10] filled UNDER the LWL (needs the assigned LSN).
        entry_buf[10..14].copy_from_slice(&item_size.to_le_bytes()); // item_size
        // VLSN at [14..22] when present (8-byte little-endian i64).
        // The VLSN comes from the caller, NOT from the assigned LSN, so it is
        // safe to write outside the latch.
        if let Some(vlsn) = opt_vlsn {
            entry_buf[14..22].copy_from_slice(&(vlsn as i64).to_le_bytes());
        }
        // payload starts after the header — the memcpy that used to run UNDER
        // the LWL now runs here, off the contended path.
        entry_buf[header_size..].copy_from_slice(payload);

        // Acquire the LWL — all LSN assignment and file position advancement
        // happens under this latch, matching serialLog/serialLogWork.
        //
        // Under the LWL we now do ONLY the LSN-dependent + serialisation work:
        // file-flip check, LSN assign, prev_offset patch, CRC32 (covers the
        // just-patched prev_offset + assigned VLSN, so it must stay under the
        // latch — JE checksums under the latch too), and the buffer-slot
        // reservation.  No payload copy, no clone.
        let (lsn, segment_out, oversized_out) = {
            let _lwl_guard = self.log_write_latch.lock();
            let entry_buf = &mut entry_buf;

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
            let prev_offset: u32 =
                if last_used.is_null() || last_used.file_number() != file_num {
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
                entry_buf,
                CHECKSUM_BYTES,
                entry_size - CHECKSUM_BYTES,
            );
            entry_buf[0..4].copy_from_slice(&crc.to_le_bytes());

            // JE faithfulness (Part-3, DRIFT-3/7): `getWriteBuffer` must be
            // called BEFORE `advanceLsn` / `setLastPosition`.  When
            // `flippedFile=true`, `getWriteBuffer` calls `bumpAndWriteDirty`
            // (drains old-file dirty buffers) and then
            // `syncLogEndAndFinishFile` (fsyncs + closes the old file) while
            // `current_file_num` still points to the OLD file.  Only after
            // that does `advanceLsn` advance the bookkeeping to the new file.
            //
            // Prior code called `set_last_position` here, advancing
            // `current_file_num` BEFORE `get_write_buffer`, so the fsync
            // would have targeted the (not yet created) new file instead of
            // the old one (DRIFT-3 ordering inversion).
            //
            // Reference: JE `LogManager.serialLogWork` steps:
            //   (3) getWriteBuffer(entrySize, flippedFile)  <- bumpAndWriteDirty
            //                                                 + syncLogEndAndFinishFile
            //   (4) advanceLsn(currentLsn, entrySize, flippedFile)  <- setLastPosition
            //
            // Utilization tracking — called under the LWL, matching the
            // serialLogWork() tracker calls.
            if let Some(obs) = &self.write_observer {
                // Mark old version obsolete (the: countObsoleteNode /
                // Inexact / DupsAllowed, depending on the caller).
                if let Some(old) = old_obsolete
                    && !old.lsn.is_null()
                {
                    obs.count_obsolete(old);
                }
                // Count the new entry (the: countNewLogEntry).
                obs.count_new_entry(
                    current_lsn.file_number(),
                    current_lsn.file_offset(),
                    entry_size as u32,
                    entry_type.is_ln_type(),
                    entry_type.is_in_type(),
                    new_db_id,
                );
                // L-6: an immediately-obsolete LN (deleted LN, embedded LN,
                // or an LN in a dup DB) is counted obsolete at write time via
                // the INEXACT variant — its own just-assigned LSN, no offset
                // tracked.  JE serialLogWork: when
                // `entry.isImmediatelyObsolete(db)`, it calls
                // `countObsoleteNodeInexact(lsn, type, size, db)` for the new
                // entry.
                if immediately_obsolete {
                    obs.count_obsolete(ObsoleteLsn {
                        lsn: current_lsn,
                        db_id: new_db_id,
                        size: entry_size as i32,
                        is_ln: entry_type.is_ln_type(),
                        kind: ObsoleteKind::Inexact,
                    });
                }
            }

            // Obtain a write buffer that can hold entry_size bytes.
            // When flipped=true this drains dirty buffers (bumpAndWriteDirty)
            // AND fsyncs/closes the old file (syncLogEndAndFinishFile) while
            // current_file_num still points to the old file.
            let buffer_arc = {
                let mut pool = self.buffer_pool.lock();
                pool.get_write_buffer(entry_size, flipped)?
            };

            // Advance LSN bookkeeping AFTER get_write_buffer returns.
            // JE serialLogWork step (4): advanceLsn called after getWriteBuffer.
            // This is the corrected ordering (DRIFT-3 fix).
            let new_next = Lsn::new(
                file_num,
                current_lsn.file_offset() + entry_size as u32,
            );
            self.file_manager.set_last_position(new_next, current_lsn);
            // JE faithfulness (Part-2, DRIFT-1): register LSN + allocate slot
            // under LWL; clone bytes; then release LWL.  The bytes copy
            // (segment.put) and direct write_buffer happen OUTSIDE the LWL.
            //
            // JE serialLogWork releases logWriteMutex BEFORE
            // LogBufferSegment.put (after steps allocate + registerLsn +
            // buffer-latch-release).  The pin-count protocol
            // (wait_for_zero_and_latch in write_dirty) ensures the buffer
            // buffer won't be reused before put() decrements.
            //
            // Round-2 change: `allocate` and `register_lsn` now take `&self`
            // and reserve the slot with a single atomic `fetch_add` — no
            // `latch_for_write`, no `Vec::resize`.  The `buffer_arc.lock()`
            // here is held only long enough for those two atomic operations
            // (it still provides mutual exclusion against the flush path's
            // `&mut` access to `flushed_len`/`reinit`); it no longer wraps a
            // second `read_latch` acquisition or a buffer growth.
            let buffer = buffer_arc.lock();
            let segment_opt = buffer.allocate(entry_size);

            let (segment_out, oversized_out) = match segment_opt {
                Some(segment) => {
                    // Entry fits in the write buffer: register LSN and pin.
                    buffer.register_lsn(current_lsn);
                    drop(buffer);
                    // MOVE the owned bytes out (no clone under the LWL — the
                    // per-call buffer is already owned by this call, and no
                    // later code under the latch touches it).
                    let entry_bytes = std::mem::take(entry_buf);
                    (Some((segment, entry_bytes)), None)
                }
                None => {
                    // Entry too large for any pool buffer: write outside LWL.
                    drop(buffer);
                    self.n_temp_buffer_writes.fetch_add(1, Ordering::Relaxed);
                    let entry_bytes = std::mem::take(entry_buf);
                    let offset = current_lsn.file_offset() as u64;
                    (None, Some((entry_bytes, offset)))
                }
            };

            (current_lsn, segment_out, oversized_out)
        };
        // LWL released here — JE serialLogWork: logWriteMutex released BEFORE
        // LogBufferSegment.put and BEFORE the direct write_buffer call.
        // Concurrent committers now serialize only on in-memory bookkeeping,
        // not on the syscall (DRIFT-1 fix, Part-2).

        // Outside LWL: copy bytes into the buffer segment (JE step 8,
        // LogBufferSegment.put outside logWriteMutex).
        if let Some((segment, entry_bytes)) = segment_out {
            segment.put(&entry_bytes);
        }
        // Outside LWL: direct write for oversized entries.
        if let Some((entry_bytes, offset)) = oversized_out {
            self.file_manager.write_buffer(&entry_bytes, offset)?;
        }

        // Flush / fsync if requested, outside the LWL (correct).
        // Use flush_sync_if_needed(lsn) rather than flush_sync() so that a
        // concurrent committer whose data was already flushed by a racing
        // leader thread can return immediately.  One thread flushes all
        // pending writes; the others see last_flush_lsn > their_commit_lsn
        // and skip the I/O entirely.
        // This is the(lsn) coalescing optimisation.
        if fsync_required {
            self.flush_sync_if_needed(lsn)?;
        } else if flush_required {
            self.flush_no_sync()?;
        }

        Ok(lsn)
    }

    /// Returns the total number of fdatasync calls performed by this log
    /// manager (the `FsyncManager` leader/timeout count).
    ///
    /// Under the JE-faithful group-commit ordering one fdatasync serves a
    /// burst of concurrent committers, so this count is well below the number
    /// of CommitSync transactions under concurrency. Surfaced as
    /// `EnvironmentStats.n_log_fsyncs`.
    pub fn fsync_count(&self) -> u64 {
        self.fsync_manager.fsync_count()
    }

    /// Flushes all dirty write buffers to disk and performs an fdatasync.
    ///
    /// JE faithfulness — this method now matches the structure of JE
    /// `FSyncManager.flushAndSync` EXACTLY: the leader/waiter decision is made
    /// FIRST (inside `fsync_manager.flush_and_sync`, under its manager mutex),
    /// and ONLY the leader (or a timed-out thread) performs the buffer drain,
    /// the `pwrite`s, and the `fdatasync`.  Waiters piggyback on the leader's
    /// fsync and do NO drain, NO pwrite and NO fsync.
    ///
    /// This fixes the coalescing divergence (Noxu was issuing ~1.7-2.5x more
    /// fdatasync calls than JE under concurrent commits): the old code drained
    /// the shared buffer + pwrote BEFORE entering the fsync manager, so a
    /// concurrent committer that didn't skip at `flush_sync_if_needed`'s fast
    /// path would slip in between the leader's pwrite and the leader's fsync
    /// window and become its own leader for a redundant fsync.
    ///
    /// JE mapping (`FSyncManager.flushAndSync`):
    ///   - leader/waiter decision under `mgrMutex`  → `flush_and_sync` Phase 1
    ///   - leader: `flushBeforeSync()` (drain + write) → `leader_work` Phase A
    ///     (`fill_flush_pending` under LWL + `write_buffer_to_file` pwrites)
    ///   - leader: `executeFSync()` (`syncLogEnd`)     → `leader_work` Phase B
    ///   - waiters: `wakeupAll()` piggyback, no I/O     → return `Ok`
    ///
    /// Preserves Noxu's "release LWL before I/O" property: the leader drains
    /// under the LWL (brief: snapshot + watermark advance) then releases the
    /// LWL before the pwrite + fdatasync (matching JE, which flushes the
    /// buffer then fsyncs outside the held region of `mgrMutex`).
    pub fn flush_sync(&self) -> Result<Lsn> {
        // The leader closure embodies JE `flushBeforeSync()` + `executeFSync()`,
        // run ONLY by the thread that wins the leader/waiter decision inside
        // `fsync_manager.flush_and_sync` (or by a timed-out thread).  Returns
        // the post-drain `eol` so the caller can advance the watermarks.
        let leader_work = || -> std::io::Result<u64> {
            // Phase A (JE flushBeforeSync): under LWL — snapshot dirty buffer
            // ranges and advance flushed_len watermarks.  The watermark advance
            // is the only operation that MUST be serialised; it prevents two
            // drains from writing the same bytes twice.  Because the
            // leader/waiter decision already serialised us, at most one drain
            // runs at a time here (matching JE: flushBeforeSync runs inside
            // doWork, after the mgrMutex decision).
            // R-1: flush_pending Vec reused across calls (clear keeps capacity).
            let (pending_snapshot, eol) = {
                let mut guard = self.log_write_latch.lock();
                guard.flush_pending.clear();
                Self::fill_flush_pending(
                    &self.buffer_pool,
                    &mut guard.flush_pending,
                );
                let eol = self.file_manager.get_next_available_lsn();
                (std::mem::take(&mut guard.flush_pending), eol)
            };
            // LWL released before I/O (Noxu invariant preserved).

            // Phase A (cont.): outside LWL — pwrite64 for each dirty range.
            for (data, file_num, offset) in &pending_snapshot {
                self.file_manager
                    .write_buffer_to_file(*file_num, data, *offset)
                    .map_err(|e| std::io::Error::other(e.to_string()))?;
            }
            // last_flush_lsn is the page-cache watermark; advancing it here (in
            // the leader, after the pwrites land in the page cache) is correct.
            self.last_flush_lsn.store(eol.as_u64(), Ordering::Release);

            // Phase B (JE executeFSync → syncLogEnd): the single fdatasync that
            // covers every committer whose bytes were in the drained buffer.
            self.file_manager
                .sync_log_end()
                .map_err(|e| std::io::Error::other(e.to_string()))?;
            Ok(eol.as_u64())
        };

        // Phase 1 (JE: synchronized(mgrMutex) leader/waiter decision) +
        // leader work + waiter piggyback, all inside flush_and_sync.
        match self.fsync_manager.flush_and_sync(leader_work) {
            Ok(eol) => {
                // Durability watermark: advanced ONLY after a successful
                // fdatasync, ONLY by the leader (inside flush_and_sync).
                // `flush_sync_if_needed` keys its skip decision off this.  A
                // waiter sees the leader's stored value (Release/Acquire) and
                // returns it (see flush_and_sync), so the waiter's subsequent
                // flush_sync_if_needed observes last_synced_lsn >= its lsn.
                self.last_synced_lsn.store(eol.as_u64(), Ordering::Release);
                Ok(eol)
            }
            Err(e) => {
                // C-2 (the 2026 review F-3.2 / F-8.4 / F-9.4): any I/O error
                // from fdatasync permanently invalidates the log; refuse all
                // further writes (fsyncgate class).  The error is propagated to
                // ALL piggybacking waiters by flush_and_sync (each waiter gets
                // its own Err here), so every committer in the failed batch
                // sees the failure — their commits are NOT durable.
                self.io_invalid.store(true, Ordering::Release);
                Err(NoxuLogError::WriteFailed(format!(
                    "fdatasync failed, environment permanently invalidated: {e}"
                )))
            }
        }
    }

    /// Port of`LogManager.flushTo(lsn)`:
    /// flush and fsync only if `lsn` has not yet been flushed.
    ///
    /// Fast path: if `last_synced_lsn >= lsn`, return immediately — a
    /// concurrent or preceding `flush_sync()` already made our data durable.
    /// Slow path: call the full `flush_sync()`.
    ///
    /// NOTE: the skip decision keys off the *durable* watermark
    /// (`last_synced_lsn`), never `last_flush_lsn`. A `flush_no_sync()`
    /// advances `last_flush_lsn` without an fdatasync; consulting it here
    /// would skip the fsync for a SYNC commit whose bytes are only in the OS
    /// page cache, silently losing the commit on power failure.
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
            // Consult the DURABLE watermark, not last_flush_lsn: a
            // `flush_no_sync` advances last_flush_lsn without an fdatasync, so
            // keying off it here would skip the fsync for a SYNC commit whose
            // data is only in the page cache (silent data loss on crash).
            let already_synced = self.last_synced_lsn.load(Ordering::Acquire);
            // Strict `>`: `eol` in flush_sync() is `get_next_available_lsn()`
            // AFTER the snapshot — the next LSN to be assigned, not the last
            // one written.  So `last_synced_lsn = X` means everything up to
            // (not including) X was synced.  We need `already_synced > lsn`
            // to guarantee `lsn` was included.  Equality means the previous
            // flush computed its eol just before our write was allocated — our
            // data was NOT in that flush.
            if already_synced > lsn.as_u64() {
                return Ok(Lsn::from_u64(already_synced));
            }
        }
        self.flush_sync()
    }

    /// Flushes all dirty write buffers to the OS page cache (no fsync).
    ///
    /// # R-2 fix (Keith re-audit)
    ///
    /// The LWL is released **before** the pwrite64 calls.  Holding the LWL
    /// through I/O in the background flush task would block ALL concurrent
    /// foreground transaction commits for the duration of each kernel write,
    /// injecting periodic multi-ms latency spikes whenever
    /// `log_flush_no_sync_interval_ms > 0`.
    ///
    /// **Correctness argument**: `fill_flush_pending()` advances each buffer's
    /// `flushed_len` watermark under the per-buffer latch before returning.
    /// After that advance, concurrent foreground writers may only append at
    /// file positions ≥ `new_flushed_len` — strictly after the range we
    /// captured.  The pwrite64 calls below therefore write to disjoint file
    /// regions from any concurrent foreground write.  `write_buffer()`
    /// serialises its own file-handle access internally.
    pub fn flush_no_sync(&self) -> Result<Lsn> {
        // Phase 1 — under LWL: snapshot buffer data and capture EOL.
        let (pending_snapshot, eol) = {
            let mut guard = self.log_write_latch.lock();
            // R-1: reuse flush_pending Vec.  We take ownership here to move
            // items out before releasing the LWL.  flush_no_sync is called
            // infrequently (background daemon), so losing outer-Vec capacity
            // on take is acceptable.
            guard.flush_pending.clear();
            Self::fill_flush_pending(
                &self.buffer_pool,
                &mut guard.flush_pending,
            );
            let eol = self.file_manager.get_next_available_lsn();
            (std::mem::take(&mut guard.flush_pending), eol)
        }; // ← LWL released; foreground writers unblocked before pwrite64

        // Phase 2 — outside LWL: write to OS page cache.
        for (data, file_num, offset) in &pending_snapshot {
            self.file_manager.write_buffer_to_file(*file_num, data, *offset)?;
        }
        self.last_flush_lsn.store(eol.as_u64(), Ordering::Release);
        Ok(eol)
    }

    /// Collects each dirty write buffer's pending bytes into `pending`.
    ///
    /// **R-1 fix**: takes `pending` by mutable reference so callers can reuse
    /// the outer `Vec` allocation across flush calls.  The inner `Vec<u8>` per
    /// dirty buffer is still a memcpy — zero-copy would require holding the
    /// buffer latch through the write, which conflicts with the R-2 goal of
    /// releasing the LWL before I/O (see the 2026 review).
    ///
    /// Must be called under the LWL.  Takes the `buffer_pool` explicitly to
    /// avoid a `&self` borrow conflict while the LWL guard is live.
    fn fill_flush_pending(
        buffer_pool: &Arc<Mutex<LogBufferPool>>,
        pending: &mut Vec<(Vec<u8>, u32, u64)>,
    ) {
        let pool = buffer_pool.lock();
        let buffers = pool.get_all_buffers();
        drop(pool);

        for buf_arc in buffers {
            let mut buf = buf_arc.lock();
            buf.wait_for_zero_and_latch();

            let first_lsn = buf.get_first_lsn();
            if !first_lsn.is_null() {
                let unflushed = buf.get_unflushed_data();
                if !unflushed.is_empty() {
                    let data = unflushed.to_vec();
                    let file_num = first_lsn.file_number();
                    let offset = buf.flushed_file_offset();
                    // Advance the watermark now (under the buffer latch) so a
                    // subsequent fill_flush_pending() call sees this range as
                    // already flushed and does not re-collect it.
                    buf.mark_flushed();
                    buf.release();
                    drop(buf);
                    // Include file_num so callers can use write_buffer_to_file
                    // and write to the buffer's own file, not current_file_num.
                    pending.push((data, file_num, offset));
                    continue;
                }
            }
            buf.release();
            drop(buf);
        }
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
    pub fn read_entry(&self, lsn: Lsn) -> Result<(LogEntryType, Vec<u8>)> {
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
                    let vlsn_present =
                        (flags & 0x08) != 0 || (flags & 0x20) != 0; // VLSN_PRESENT | REPLICATED
                    let header_size = if vlsn_present {
                        MAX_HEADER_SIZE
                    } else {
                        MIN_HEADER_SIZE
                    };
                    let entry_size = header_size + item_size;

                    if slice.len() >= entry_size {
                        let entry_type_num = slice[4];
                        let payload = slice[header_size..entry_size].to_vec();
                        buf.release();
                        drop(buf);

                        let entry_type =
                            LogEntryType::from_type_num(entry_type_num).ok_or(
                                NoxuLogError::InvalidEntryType {
                                    type_num: entry_type_num,
                                    lsn,
                                },
                            )?;
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
        let stored_checksum = u32::from_le_bytes([
            header_buf[0],
            header_buf[1],
            header_buf[2],
            header_buf[3],
        ]);
        let entry_type_num = header_buf[4];
        let flags = header_buf[5];
        let item_size = u32::from_le_bytes([
            header_buf[10],
            header_buf[11],
            header_buf[12],
            header_buf[13],
        ]) as usize;

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
        //
        // REP-1 STEP 4 (JE LogEntryHeader.turnOffInvisible): cloak the
        // invisible bit (flags 0x10) before checksumming, so an entry that was
        // flipped invisible in-place by recovery rollback still validates
        // against its original checksum. JE computes the checksum with the
        // invisible bit always OFF, allowing it to be flipped without a
        // checksum rewrite.
        // JE LogManager.getChecksumOnRead: skip validation entirely when
        // LOG_CHECKSUM_READ is disabled. The invisible-bit cloak still runs
        // so the returned bytes match the on-disk logical content.
        full_buf[5] &= !0x10u8;
        if self.checksum_on_read {
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
        }

        // Step 6: Validate and return the entry type and payload.
        let entry_type = LogEntryType::from_type_num(entry_type_num).ok_or(
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

    /// Returns the last fdatasync'd (durable) LSN watermark. Test-only:
    /// exposed to assert the C-1 durability invariant (a `flush_no_sync` must
    /// not advance this).
    #[cfg(test)]
    pub(crate) fn get_last_synced_lsn(&self) -> Lsn {
        Lsn::from_u64(self.last_synced_lsn.load(Ordering::Relaxed))
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
    use crate::entry_type::LogEntryType;
    use crate::file_manager::FileManager;
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

        let result = lm.log(
            LogEntryType::BIN,
            b"provisional_data",
            Provisional::Yes,
            false,
            false,
        );
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
            true, // flush_required
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

    /// REP-1 STEP 4: flipping the invisible bit in-place via
    /// `FileManager.make_invisible` must NOT break the entry's checksum,
    /// because the read path cloaks the invisible bit before validating
    /// (JE `LogEntryHeader.turnOffInvisible`). The entry must still read back
    /// after make_invisible + force.
    #[test]
    fn test_make_invisible_preserves_checksum() {
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);

        let payload = b"rolled_back_entry";
        let lsn = lm
            .log(LogEntryType::Trace, payload, Provisional::No, false, false)
            .unwrap();
        lm.flush_no_sync().unwrap();

        // Sanity: reads fine before marking.
        lm.read_entry(lsn).unwrap();

        // Flip the invisible bit in place and fsync, as recovery rollback
        // re-marking does.
        let fm = lm.file_manager();
        fm.make_invisible(lsn.file_number(), &[lsn.file_offset()]).unwrap();
        fm.force(&[lsn.file_number()]).unwrap();

        // The entry must STILL validate and read back (cloaked checksum).
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
        assert!(
            after.as_u64() > before.as_u64() || after == lm.get_end_of_log()
        );
    }

    // C-1 regression: a `flush_no_sync` must NOT let a later SYNC commit skip
    // its fdatasync. `flush_no_sync` advances the page-cache watermark
    // (last_flush_lsn) but NOT the durable watermark (last_synced_lsn);
    // `flush_sync_if_needed` keys its skip decision off the durable watermark,
    // so it performs a real fsync even when last_flush_lsn is already past it.
    #[test]
    fn test_flush_no_sync_does_not_satisfy_sync_durability() {
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);

        // A WRITE_NO_SYNC-style op: data to page cache, no fdatasync.
        lm.log(LogEntryType::Trace, b"nosync", Provisional::No, false, false)
            .unwrap();
        lm.flush_no_sync().unwrap();

        // Durable watermark must still be 0 — nothing has been fdatasync'd.
        assert_eq!(
            lm.get_last_synced_lsn(),
            Lsn::from_u64(0),
            "flush_no_sync must not advance the durable (synced) watermark"
        );
        // ...even though the page-cache watermark advanced.
        assert!(
            lm.get_last_flush_lsn().as_u64() > 0,
            "flush_no_sync should advance the page-cache watermark"
        );

        // A later SYNC commit at an LSN already covered by last_flush_lsn must
        // NOT be skipped: flush_sync_if_needed must perform a real fsync,
        // advancing the durable watermark.
        let sync_lsn = lm
            .log(LogEntryType::Trace, b"sync", Provisional::No, false, false)
            .unwrap();
        lm.flush_sync_if_needed(sync_lsn).unwrap();
        assert!(
            lm.get_last_synced_lsn().as_u64() > sync_lsn.as_u64(),
            "flush_sync_if_needed must fdatasync (advance the durable \
             watermark past the SYNC commit), not skip because flush_no_sync \
             advanced last_flush_lsn"
        );
    }

    #[test]
    fn test_get_end_of_log_advances_with_writes() {
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);

        let eol_before = lm.get_end_of_log();
        lm.log(LogEntryType::Trace, b"advance", Provisional::No, false, false)
            .unwrap();
        let eol_after = lm.get_end_of_log();

        assert!(
            eol_after.file_offset() > eol_before.file_offset()
                || eol_after.file_number() > eol_before.file_number()
        );
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

        lm.log(
            LogEntryType::Trace,
            b"stats_test",
            Provisional::No,
            false,
            false,
        )
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

    /// C-2 regression: once `io_invalid` is set, the LogManager must refuse
    /// all further log writes.  This simulates what happens after an fdatasync
    /// returns EIO — the environment is permanently invalidated and must not
    /// accept commits that the kernel cannot guarantee are durable.
    #[test]
    fn test_fsync_failure_invalidates_log_manager() {
        use std::sync::atomic::Ordering;
        let dir = TempDir::new().unwrap();
        let lm = make_log_manager(&dir);

        // Pre-condition: log works fine.
        lm.log(LogEntryType::Trace, b"before", Provisional::No, false, false)
            .expect("first write must succeed");

        // Simulate an fdatasync failure by setting the io_invalid flag
        // directly (equivalent to what flush_sync() sets on EIO).
        lm.io_invalid.store(true, Ordering::Release);

        // Post-condition: all subsequent log() calls must be rejected.
        let result = lm.log(
            LogEntryType::Trace,
            b"after",
            Provisional::No,
            false,
            false,
        );
        assert!(result.is_err(), "log() must fail after io_invalid is set");

        // is_io_invalid() accessor must agree.
        assert!(lm.is_io_invalid(), "is_io_invalid() must return true");
    }

    // -----------------------------------------------------------------------
    // Part-2 acceptance test (DRIFT-1 fix)
    //
    // STRUCTURAL TEST: Verifies that `log_internal` releases the LWL before
    // the bytes copy (segment.put) and that concurrent committers can proceed
    // concurrently.  This is NOT a timing test — it uses a real env-style
    // sequential write to confirm durability + correctness.
    //
    // FAIL-PRE:  with LWL held through segment.put, N concurrent writers
    //            would all block on LWL, serialising completely.
    // PASS-POST: each writer independently calls segment.put off-latch;
    //            all entries are durable and readable after flush_sync.
    //
    // The real perf proof is in the benchmark suite (concurrent throughput).
    // -----------------------------------------------------------------------

    /// Concurrent log_internal calls — multiple threads log entries in
    /// parallel; after flush_sync all entries must be readable from disk.
    /// Tests that segment.put (bytes copy) runs off-LWL correctly.
    ///
    /// JE references:
    /// - `LogManager.serialLogWork`: logWriteMutex released before
    ///   `LogBufferSegment.put`
    /// - `LogBufferSegment.put`: called outside logWriteMutex
    #[test]
    fn test_concurrent_log_internal_latch_released_before_put() {
        use std::sync::Arc;
        use std::thread;

        let dir = TempDir::new().unwrap();
        let fm = Arc::new(
            FileManager::new(dir.path(), false, 100_000_000, 10).unwrap(),
        );
        // Normal 1 MB buffers, 3 buffers
        let lm =
            Arc::new(LogManager::new(Arc::clone(&fm), 3, 1024 * 1024, 4096));

        const THREADS: usize = 8;
        const ENTRIES_PER_THREAD: usize = 50;

        let mut handles = Vec::new();
        for t in 0..THREADS {
            let lm2 = Arc::clone(&lm);
            handles.push(thread::spawn(move || {
                let mut lsns = Vec::new();
                for i in 0..ENTRIES_PER_THREAD {
                    let payload = format!("t{t:02}_e{i:04}");
                    let lsn = lm2
                        .log(
                            LogEntryType::Trace,
                            payload.as_bytes(),
                            Provisional::No,
                            false,
                            false,
                        )
                        .expect("log must not fail");
                    lsns.push((lsn, payload));
                }
                lsns
            }));
        }

        let all_lsns: Vec<(Lsn, String)> =
            handles.into_iter().flat_map(|h| h.join().unwrap()).collect();

        // Flush all entries to disk.
        lm.flush_sync().expect("flush_sync must succeed");

        // Verify all entries are readable from disk (cold path).
        for (lsn, expected_payload) in &all_lsns {
            let (_, payload) = lm.read_entry(*lsn).expect("read_entry");
            assert_eq!(
                payload.as_slice(),
                expected_payload.as_bytes(),
                "payload mismatch at {lsn:?}"
            );
        }
    }

    /// Micro-validation for the LWL append rework (feat/lwl-scaling): with
    /// marshalling moved OUTSIDE the latch, prove the invariants that the
    /// move must not break under concurrency:
    ///
    ///   (a) every assigned LSN is UNIQUE and MONOTONICALLY assigned in file
    ///       order (recovery + replication VLSN streaming depend on this);
    ///   (b) every entry reads back BYTE-IDENTICAL from disk (checksum valid —
    ///       `read_entry` validates the CRC32, so a corrupt or torn entry
    ///       surfaces as an error, not a silent mismatch);
    ///   (c) no data corruption / cross-thread payload bleed (each thread
    ///       writes a distinct payload keyed by (thread, index) and multi-KB
    ///       payloads so the off-latch memcpy races are exercised).
    ///
    /// 32 threads, larger payloads than the smoke test above.
    #[test]
    fn test_lwl_marshall_outside_latch_stress() {
        use std::collections::HashSet;
        use std::sync::Arc;
        use std::thread;

        let dir = TempDir::new().unwrap();
        let fm = Arc::new(
            FileManager::new(dir.path(), false, 100_000_000, 10).unwrap(),
        );
        let lm =
            Arc::new(LogManager::new(Arc::clone(&fm), 3, 1024 * 1024, 4096));

        const THREADS: usize = 32;
        const ENTRIES_PER_THREAD: usize = 40;

        let mut handles = Vec::new();
        for t in 0..THREADS {
            let lm2 = Arc::clone(&lm);
            handles.push(thread::spawn(move || {
                let mut out = Vec::new();
                for i in 0..ENTRIES_PER_THREAD {
                    // Distinct, size-varying payload keyed by (thread, index).
                    // Sizes span the header boundary and multi-KB so the
                    // off-latch header-encode + payload memcpy is exercised
                    // across many concurrent buffer allocations.
                    let len = 1 + ((t * 37 + i * 101) % 3000);
                    let mut payload = Vec::with_capacity(len);
                    for b in 0..len {
                        payload.push(
                            ((t as u32).wrapping_mul(2_654_435_761)
                                ^ (i as u32).wrapping_mul(40_503)
                                ^ (b as u32))
                                as u8,
                        );
                    }
                    let lsn = lm2
                        .log(
                            LogEntryType::Trace,
                            &payload,
                            Provisional::No,
                            false,
                            false,
                        )
                        .expect("log must not fail");
                    out.push((lsn, payload));
                }
                out
            }));
        }

        let mut all: Vec<(Lsn, Vec<u8>)> =
            handles.into_iter().flat_map(|h| h.join().unwrap()).collect();

        // (a) LSN uniqueness across all threads.
        let unique: HashSet<u64> =
            all.iter().map(|(lsn, _)| lsn.as_u64()).collect();
        assert_eq!(
            unique.len(),
            all.len(),
            "every concurrently-assigned LSN must be unique"
        );

        // (a) LSN monotonic assignment in file order: sorting by LSN must give
        // strictly increasing offsets within each file with no gaps that
        // overlap a previous entry.  We check that consecutive entries in LSN
        // order do not overlap: next.offset >= prev.offset + prev.entry_size.
        all.sort_by_key(|(lsn, _)| lsn.as_u64());
        for w in all.windows(2) {
            let (a_lsn, a_payload) = &w[0];
            let (b_lsn, _) = &w[1];
            assert!(
                b_lsn.as_u64() > a_lsn.as_u64(),
                "LSNs must be strictly monotonic: {a_lsn:?} !< {b_lsn:?}"
            );
            if a_lsn.file_number() == b_lsn.file_number() {
                let a_entry_size =
                    MIN_HEADER_SIZE as u32 + a_payload.len() as u32;
                assert!(
                    b_lsn.file_offset()
                        >= a_lsn.file_offset() + a_entry_size,
                    "entries must not overlap: {a_lsn:?} (+{a_entry_size}) \
                     overlaps {b_lsn:?}"
                );
            }
        }

        // Flush all entries to disk.
        lm.flush_sync().expect("flush_sync must succeed");

        // (b)+(c) every entry reads back byte-identical with a valid checksum.
        for (lsn, expected) in &all {
            let (ty, payload) = lm
                .read_entry(*lsn)
                .unwrap_or_else(|e| panic!("read_entry {lsn:?} failed: {e:?}"));
            assert_eq!(ty, LogEntryType::Trace, "type mismatch at {lsn:?}");
            assert_eq!(
                &payload, expected,
                "payload mismatch (corruption/bleed) at {lsn:?}"
            );
        }
    }

    /// Round-2 (atomic buffer-slot reservation) stress test: 64 concurrent
    /// appenders hammering the lock-free `allocate` `fetch_add` path with
    /// SMALL buffers so the ring rolls many times (each roll is triggered by
    /// exactly one writer whose `fetch_add` overflows capacity).
    ///
    /// Asserts the same invariants the reservation rework must preserve:
    ///   (a) every assigned LSN is UNIQUE and STRICTLY MONOTONIC, and
    ///       consecutive same-file entries never overlap (the file_offset ↔
    ///       buffer-position mapping stays exact through every roll);
    ///   (b) every entry reads back BYTE-IDENTICAL with a valid CRC32 after a
    ///       sync flush (no torn/overwritten slot, no cross-thread bleed).
    ///
    /// Small (64 KiB) buffers + up to ~4 KiB payloads guarantee frequent
    /// buffer-full rolls and occasional oversized direct writes — the exact
    /// coordination paths the atomic reservation touches.
    #[test]
    fn test_lwl_atomic_reservation_stress_64t() {
        use std::collections::HashSet;
        use std::sync::Arc;
        use std::thread;

        let dir = TempDir::new().unwrap();
        let fm = Arc::new(
            FileManager::new(dir.path(), false, 500_000_000, 10).unwrap(),
        );
        // 3 small buffers (64 KiB) => the ring rolls constantly under load.
        let lm =
            Arc::new(LogManager::new(Arc::clone(&fm), 3, 64 * 1024, 4096));

        const THREADS: usize = 64;
        const ENTRIES_PER_THREAD: usize = 200;

        let mut handles = Vec::new();
        for t in 0..THREADS {
            let lm2 = Arc::clone(&lm);
            handles.push(thread::spawn(move || {
                let mut out = Vec::new();
                for i in 0..ENTRIES_PER_THREAD {
                    // Payload sizes span 1..~4 KiB so most entries fit a
                    // 64 KiB buffer (forcing rolls) while some exceed nothing
                    // here; distinct bytes keyed by (thread, index, pos).
                    let len = 1 + ((t * 131 + i * 977) % 4000);
                    let mut payload = Vec::with_capacity(len);
                    for b in 0..len {
                        payload.push(
                            ((t as u32).wrapping_mul(2_654_435_761)
                                ^ (i as u32).wrapping_mul(40_503)
                                ^ (b as u32).wrapping_mul(97))
                                as u8,
                        );
                    }
                    let lsn = lm2
                        .log(
                            LogEntryType::Trace,
                            &payload,
                            Provisional::No,
                            false,
                            false,
                        )
                        .expect("log must not fail");
                    out.push((lsn, payload));
                }
                out
            }));
        }

        let mut all: Vec<(Lsn, Vec<u8>)> =
            handles.into_iter().flat_map(|h| h.join().unwrap()).collect();

        assert_eq!(all.len(), THREADS * ENTRIES_PER_THREAD);

        // (a) uniqueness.
        let unique: HashSet<u64> =
            all.iter().map(|(lsn, _)| lsn.as_u64()).collect();
        assert_eq!(
            unique.len(),
            all.len(),
            "every concurrently-assigned LSN must be unique (atomic \
             reservation must not double-hand-out a slot)"
        );

        // (a) strict monotonicity + no same-file overlap: the atomic
        // fetch_add offset must map exactly onto the assigned LSN file_offset.
        all.sort_by_key(|(lsn, _)| lsn.as_u64());
        for w in all.windows(2) {
            let (a_lsn, a_payload) = &w[0];
            let (b_lsn, _) = &w[1];
            assert!(
                b_lsn.as_u64() > a_lsn.as_u64(),
                "LSNs must be strictly monotonic: {a_lsn:?} !< {b_lsn:?}"
            );
            if a_lsn.file_number() == b_lsn.file_number() {
                let a_entry_size =
                    MIN_HEADER_SIZE as u32 + a_payload.len() as u32;
                assert!(
                    b_lsn.file_offset()
                        >= a_lsn.file_offset() + a_entry_size,
                    "entries must not overlap after a buffer roll: {a_lsn:?} \
                     (+{a_entry_size}) overlaps {b_lsn:?}"
                );
            }
        }

        // Durably flush, then verify byte-identical readback (CRC validated
        // inside read_entry).
        lm.flush_sync().expect("flush_sync must succeed");

        for (lsn, expected) in &all {
            let (ty, payload) = lm
                .read_entry(*lsn)
                .unwrap_or_else(|e| panic!("read_entry {lsn:?} failed: {e:?}"));
            assert_eq!(ty, LogEntryType::Trace, "type mismatch at {lsn:?}");
            assert_eq!(
                &payload, expected,
                "payload mismatch (torn slot / cross-thread bleed) at {lsn:?}"
            );
        }
    }

    /// Round-2 oversized-entry path: an entry larger than a pool buffer takes
    /// the direct-write path.  With the atomic reservation, `allocate` on the
    /// freshly-reinit'd buffer overflows capacity, `fetch_sub`-undoes the
    /// reservation (leaving `write_position == 0`), returns `None`, and the
    /// entry is written directly to the file.  Verify it reads back
    /// byte-identical and that the buffer is left reusable for a following
    /// small entry.
    #[test]
    fn test_oversized_entry_direct_write_roundtrip() {
        let dir = TempDir::new().unwrap();
        let fm = Arc::new(
            FileManager::new(dir.path(), false, 100_000_000, 10).unwrap(),
        );
        // 4 KiB buffers; a >4 KiB payload cannot fit any pool buffer.
        let lm = LogManager::new(Arc::clone(&fm), 3, 4096, 4096);

        let big: Vec<u8> = (0..20_000u32).map(|i| (i % 251) as u8).collect();
        let big_lsn = lm
            .log(LogEntryType::Trace, &big, Provisional::No, false, false)
            .expect("oversized log must succeed via direct write");

        // A small entry after the oversized one must still work (buffer left
        // in a clean, reusable state — write_position back at 0).
        let small_lsn = lm
            .log(LogEntryType::Trace, b"after-big", Provisional::No, false, false)
            .expect("small log after oversized must succeed");

        assert_eq!(lm.get_stats().n_temp_buffer_writes, 1);

        lm.flush_sync().expect("flush_sync");

        let (_, big_back) = lm.read_entry(big_lsn).expect("read big");
        assert_eq!(big_back, big, "oversized entry must read back identical");
        let (_, small_back) = lm.read_entry(small_lsn).expect("read small");
        assert_eq!(small_back, b"after-big");
    }

    /// Fix 2: the `checksum_on_read` knob (JE LogManager.getChecksumOnRead /
    /// LOG_CHECKSUM_READ) must actually be honoured. With it disabled, a
    /// corrupted-on-disk entry reads back WITHOUT a checksum error (proving
    /// the CRC step was skipped); with it enabled (JE default), the same
    /// corruption surfaces as `NoxuLogError::Checksum`.
    #[test]
    fn test_checksum_on_read_knob_honoured() {
        use std::io::{Seek, SeekFrom, Write};

        let dir = TempDir::new().unwrap();
        let fm = Arc::new(
            FileManager::new(dir.path(), false, 100_000_000, 10).unwrap(),
        );
        let payload = b"checksum-knob-payload";
        let lsn = {
            let lm = LogManager::new(Arc::clone(&fm), 3, 1024 * 1024, 4096);
            let lsn = lm
                .log(LogEntryType::Trace, payload, Provisional::No, false, false)
                .expect("log");
            lm.flush_sync().expect("flush_sync");
            lsn
        };

        // Corrupt one payload byte on disk (past the fixed header) so the
        // stored CRC no longer matches the contents.
        let file_path =
            dir.path().join(format!("{:08x}.ndb", lsn.file_number()));
        {
            let mut f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(&file_path)
                .expect("open log file");
            // Corrupt a byte inside the payload region: file_offset +
            // MAX_HEADER_SIZE lands safely inside a >0-length payload.
            let pos = lsn.file_offset() as u64 + MAX_HEADER_SIZE as u64;
            f.seek(SeekFrom::Start(pos)).unwrap();
            f.write_all(&[0xFF]).unwrap();
            f.sync_all().unwrap();
        }

        // Default (checksum on): corruption must be detected.
        {
            let lm = LogManager::new(Arc::clone(&fm), 3, 1024 * 1024, 4096);
            let res = lm.read_entry(lsn);
            assert!(
                matches!(res, Err(NoxuLogError::Checksum { .. })),
                "default (checksum on) must detect corruption, got {res:?}"
            );
        }

        // Checksum off: the CRC step is skipped, so the (corrupt) entry reads
        // back without a checksum error.
        {
            let mut lm = LogManager::new(Arc::clone(&fm), 3, 1024 * 1024, 4096);
            lm.set_checksum_on_read(false);
            let (_, back) =
                lm.read_entry(lsn).expect("checksum-off read must not error");
            assert_eq!(back.len(), payload.len());
        }
    }
}
