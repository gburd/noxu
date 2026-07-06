//! File manager for log files.
//!
//!
//! The FileManager presents the abstraction of one contiguous log file,
//! managing the actual on-disk log files, file handles, and LSN allocation.

use crate::error::{LogError, Result};
use crate::file_handle::FileHandle;
use crate::file_header::{
    FILE_HEADER_SIZE, FileHeader, LOG_VERSION, on_disk_size,
};
use hashbrown::HashMap;
use memmap2::Mmap;
use noxu_latch::ExclusiveLatch;
use noxu_sync::{Mutex, RwLock};
use noxu_util::lsn::Lsn;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// File extension for noxu database log files.
pub const LOG_FILE_EXTENSION: &str = ".ndb";

/// Lock file name for environment locking.
pub const LOCK_FILE_NAME: &str = "noxu.lck";

/// Holds writes that were blocked by an in-flight fsync/write and enqueued
/// for later execution by the next thread to fsync or write (JE
/// `LogEndFileDescriptor.queuedWrites` et al., FileManager.java:2793-2802).
///
/// The queue only ever holds writes for a SINGLE file, appended CONTIGUOUSLY
/// (the WAL is append-only): `qw_starting_offset` is the on-disk offset of
/// byte 0 in `buf`, `pos` is the current fill length, and `qw_file_num` is the
/// destination file. Guarded by its own mutex; the latch order is fsync-lock
/// THEN this queue mutex (JE: "Latch order is fsyncFileSynchronizer, followed
/// by the queuedWrites mutex", FileManager.java:2788-2790).
struct WriteQueue {
    /// Backing buffer sized to `write_queue_size` (JE `queuedWrites`).
    buf: Box<[u8]>,
    /// Current fill position in `buf` (JE `queuedWritesPosition`).
    pos: usize,
    /// On-disk offset of `buf[0]` (JE `qwStartingOffset`).
    qw_starting_offset: u64,
    /// Destination file number for the queued bytes (JE `qwFileNum`, -1 =
    /// none; we use `Option` for the -1 sentinel).
    qw_file_num: Option<u32>,
}

impl WriteQueue {
    fn new(size: usize) -> Self {
        WriteQueue {
            buf: vec![0u8; size].into_boxed_slice(),
            pos: 0,
            qw_starting_offset: 0,
            qw_file_num: None,
        }
    }
}

/// Outcome of one non-blocking enqueue attempt (JE `enqueueWrite1` throwing
/// `RelatchRequiredException` on overflow so the caller can dequeue + retry).
enum EnqueueOutcome {
    /// Bytes were queued; the caller returns with no I/O.
    Queued,
    /// The queue would overflow; the caller must dequeue then retry.
    Relatch,
}

/// File mode for opening log files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileMode {
    /// Read-only access.
    ReadOnly,
    /// Read-write access.
    ReadWrite,
}

/// Returns the byte offset of the first log entry in a **new** log file.
///
/// New files are always written as the current `LOG_VERSION`, so this
/// returns `FILE_HEADER_SIZE` (36 for v3).  When reading an **existing**
/// file use [`FileManager::file_header_size_for`] to account for v2 files
/// whose first entry is at offset 32.
#[inline]
pub fn first_log_entry_offset() -> u32 {
    FILE_HEADER_SIZE as u32
}

/// Formats a file number as an 8-digit lowercase hex string.
///
/// Example: 42 -> "0000002a"
fn format_file_number(file_num: u32) -> String {
    format!("{:08x}", file_num)
}

/// Parses a file number from a hex string filename.
///
/// Example: "0000002a.ndb" -> 42
fn parse_file_number(filename: &str) -> Option<u32> {
    let stem = filename.strip_suffix(LOG_FILE_EXTENSION)?;
    u32::from_str_radix(stem, 16).ok()
}

/// LRU cache of open file handles.
///
/// The key is the log file
/// number; values are `Arc`-wrapped so callers may hold a reference after the
/// cache evicts the entry (matching `FileHandle` reference-counting
/// pattern).  Capacity is configurable (default: `ENV_RUN_CLEANER_THREADS
/// + 2`; Noxu default: 10).
type FileHandleCache = lru::LruCache<u32, Arc<FileHandle>>;

/// Manages log files in the environment directory.
pub struct FileManager {
    /// Environment directory path.
    env_dir: PathBuf,
    /// Whether the environment is read-only.
    read_only: bool,
    /// Maximum size of a single log file (bytes).
    max_file_size: u64,
    /// LRU cache of open file handles.
    ///
    /// Protected by `noxu_sync::Mutex` because `lru::LruCache::get()` mutates
    /// the eviction order, so a shared read lock would not be safe.
    file_cache: Mutex<FileHandleCache>,
    /// Current file number being written to.
    current_file_num: AtomicU32,
    /// Next available LSN for writing.
    next_available_lsn: AtomicU64,
    /// Last LSN that was used in the current file.
    last_used_lsn: AtomicU64,
    /// Map of file number to last LSN used in that file (for file headers).
    per_file_last_lsn: RwLock<HashMap<u32, Lsn>>,
    /// Latch protecting file creation and file number advancement.
    file_latch: ExclusiveLatch,
    /// Lock file handle (for environment locking).
    lock_file: RwLock<Option<File>>,
    /// Number of log files opened (cache miss = new file open).
    pub n_file_opens: AtomicU64,
    /// Number of sequential read calls.
    pub n_sequential_reads: AtomicU64,
    /// Total bytes read sequentially.
    pub n_sequential_read_bytes: AtomicU64,
    /// Number of sequential write calls.
    pub n_sequential_writes: AtomicU64,
    /// Total bytes written sequentially.
    pub n_sequential_write_bytes: AtomicU64,
    /// Number of random (point-lookup) read operations.
    pub n_random_reads: AtomicU64,
    /// Total bytes from random read operations.
    pub n_random_read_bytes: AtomicU64,

    // ── Write Queue (JE LogEndFileDescriptor) ──────────────────────────────
    /// Whether the Write Queue is enabled (JE `useWriteQueue`,
    /// `LOG_USE_WRITE_QUEUE`). Set via [`FileManager::configure_write_queue`].
    use_write_queue: std::sync::atomic::AtomicBool,
    /// Size of the write queue buffer in bytes (JE `writeQueueSize`,
    /// `LOG_WRITE_QUEUE_SIZE`, default 1 MiB). Read once when the queue is
    /// first configured.
    write_queue_size: AtomicU64,
    /// Non-blocking fsync latch (JE `fsyncFileSynchronizer`, a
    /// `ReentrantLock`). A `write_buffer_to_file` caller `try_lock`s it; if it
    /// cannot be acquired an fsync/write is in progress and the caller
    /// enqueues instead of blocking. `sync_log_end` acquires it blocking
    /// before its fdatasync. Committers must NEVER hold this across a return.
    /// Guards `write_queue` (latch order: this lock THEN the queue mutex).
    fsync_lock: Mutex<()>,
    /// The queued writes (JE `queuedWrites` + position/offset/fileNum),
    /// lazily allocated when the queue is first enabled.
    write_queue: Mutex<Option<WriteQueue>>,
    /// Stat: bytes served to readers directly from the write queue
    /// (JE `nBytesReadFromWriteQueue`).
    pub n_bytes_read_from_write_queue: AtomicU64,
    /// Stat: writes flushed from the queue by a dequeue (JE
    /// `nWritesFromWriteQueue`).
    pub n_writes_from_write_queue: AtomicU64,
    /// Stat: enqueue attempts that overflowed and fell back to a direct write
    /// (JE `nWriteQueueOverflowFailures`).
    pub n_write_queue_overflow: AtomicU64,
}

impl FileManager {
    /// Creates a new FileManager.
    ///
    /// # Arguments
    ///
    /// * `env_dir` - Path to the environment directory
    /// * `read_only` - Whether to open in read-only mode
    /// * `max_file_size` - Maximum size of a single log file (bytes)
    /// * `cache_size` - Maximum number of file handles to cache
    ///
    /// # Returns
    ///
    /// A new FileManager instance, or an error if the directory is invalid
    /// or the environment is locked.
    pub fn new(
        env_dir: impl AsRef<Path>,
        read_only: bool,
        max_file_size: u64,
        cache_size: usize,
    ) -> Result<Self> {
        let env_dir = env_dir.as_ref().to_path_buf();

        // Verify directory exists
        if !env_dir.exists() {
            return Err(LogError::InvalidDirectory(format!(
                "Environment directory does not exist: {}",
                env_dir.display()
            )));
        }

        if !env_dir.is_dir() {
            return Err(LogError::InvalidDirectory(format!(
                "Path is not a directory: {}",
                env_dir.display()
            )));
        }

        let capacity = NonZeroUsize::new(cache_size.max(1))
            .expect("cache_size.max(1) is always >= 1");
        let manager = FileManager {
            env_dir,
            read_only,
            max_file_size,
            file_cache: Mutex::new(lru::LruCache::new(capacity)),
            current_file_num: AtomicU32::new(0),
            next_available_lsn: AtomicU64::new(
                Lsn::new(0, first_log_entry_offset()).as_u64(),
            ),
            last_used_lsn: AtomicU64::new(noxu_util::lsn::NULL_LSN.as_u64()),
            per_file_last_lsn: RwLock::new(HashMap::new()),
            file_latch: ExclusiveLatch::named("file_manager"),
            lock_file: RwLock::new(None),
            n_file_opens: AtomicU64::new(0),
            n_sequential_reads: AtomicU64::new(0),
            n_sequential_read_bytes: AtomicU64::new(0),
            n_sequential_writes: AtomicU64::new(0),
            n_sequential_write_bytes: AtomicU64::new(0),
            n_random_reads: AtomicU64::new(0),
            n_random_read_bytes: AtomicU64::new(0),
            // Write Queue: disabled until `configure_write_queue` is called
            // (production wires it from `DbiEnvConfig`; tests that use
            // `FileManager::new` directly get the direct-write path, which is
            // the pre-Write-Queue behaviour). JE reads the config at
            // construction; Noxu defers so the many test call sites of `new`
            // are unaffected.
            use_write_queue: std::sync::atomic::AtomicBool::new(false),
            write_queue_size: AtomicU64::new(1 << 20),
            fsync_lock: Mutex::new(()),
            write_queue: Mutex::new(None),
            n_bytes_read_from_write_queue: AtomicU64::new(0),
            n_writes_from_write_queue: AtomicU64::new(0),
            n_write_queue_overflow: AtomicU64::new(0),
        };

        // Lock the environment
        manager.lock_environment()?;

        Ok(manager)
    }

    /// Locks the environment to prevent concurrent access.
    fn lock_environment(&self) -> Result<()> {
        if self.read_only {
            // For read-only environments, we don't take an exclusive lock
            // (in a full implementation, we'd use a shared lock)
            return Ok(());
        }

        let lock_path = self.env_dir.join(LOCK_FILE_NAME);

        // Try to create/open the lock file
        let lock_file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)?;

        // Try to acquire an exclusive lock.
        // fs2::FileExt is supported on unix and windows; on other platforms
        // (e.g. WASM, embedded) we skip the lock — acceptable since those
        // targets run single-process environments.
        #[cfg(any(unix, windows))]
        {
            use fs2::FileExt;
            lock_file.try_lock_exclusive().map_err(|_| {
                LogError::EnvironmentLocked(format!(
                    "Environment is locked by another process: {}",
                    self.env_dir.display()
                ))
            })?;
        }

        *self.lock_file.write() = Some(lock_file);

        Ok(())
    }

    /// Returns the path to a log file for the given file number.
    fn file_path(&self, file_num: u32) -> PathBuf {
        let filename =
            format!("{}{}", format_file_number(file_num), LOG_FILE_EXTENSION);
        self.env_dir.join(filename)
    }

    /// Lists all log file numbers in the environment directory.
    ///
    /// Returns the file numbers sorted in ascending order.
    pub fn list_file_numbers(&self) -> Result<Vec<u32>> {
        let mut file_nums = Vec::new();

        for entry in fs::read_dir(&self.env_dir)? {
            let entry = entry?;
            let filename = entry.file_name();
            let filename_str = filename.to_string_lossy();

            if let Some(file_num) = parse_file_number(&filename_str) {
                file_nums.push(file_num);
            }
        }

        file_nums.sort_unstable();
        Ok(file_nums)
    }

    /// Returns the first (lowest numbered) file, or None if no files exist.
    pub fn get_first_file_num(&self) -> Result<Option<u32>> {
        Ok(self.list_file_numbers()?.into_iter().next())
    }

    /// Returns the last (highest numbered) file, or None if no files exist.
    pub fn get_last_file_num(&self) -> Result<Option<u32>> {
        Ok(self.list_file_numbers()?.into_iter().last())
    }

    /// Returns the configured maximum log file size in bytes.
    /// Returns true if this FileManager was opened read-only.
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    pub fn max_file_size(&self) -> u64 {
        self.max_file_size
    }

    /// Returns the environment directory holding the log files.
    pub fn env_dir(&self) -> &Path {
        &self.env_dir
    }

    /// Returns the total size, in bytes, of all `.ndb` log files on disk.
    ///
    /// This is the disk-limit "total log size" used by the disk-usage probe.
    /// JE computes the analogous value in
    /// `FileProtector.getLogSizeStats()` by summing `activeFiles` (plus the
    /// last file's length); Noxu has no reserved-file machinery (the cleaner
    /// deletes files outright rather than parking them as "reserved"), so the
    /// total is simply the sum of every log file's length — equivalent to
    /// JE's `activeSize` with `reservedSize == 0`.
    pub fn total_log_size(&self) -> Result<u64> {
        let mut total = 0u64;
        for file_num in self.list_file_numbers()? {
            // A file may be deleted by the cleaner between listing and stat;
            // skip it rather than fail the whole probe.
            if let Ok(len) = self.get_file_length(file_num) {
                total += len;
            }
        }
        Ok(total)
    }

    /// Returns the filesystem free (usable) space, in bytes, for the
    /// environment directory.
    ///
    /// JE calls `Cleaner.getDiskFreeSpace()` →
    /// `FileStoreInfo.getUsableSpace()` (a `statvfs`). Noxu uses
    /// `fs2::available_space` (also `statvfs`-backed), which reports space
    /// available to a non-privileged process — the same notion JE uses.
    pub fn disk_free_space(&self) -> Result<u64> {
        Ok(fs2::available_space(&self.env_dir)?)
    }

    /// Returns the current file number being written to.
    pub fn get_current_file_num(&self) -> u32 {
        self.current_file_num.load(Ordering::Acquire)
    }

    /// Returns the next available LSN for writing.
    pub fn get_next_available_lsn(&self) -> Lsn {
        Lsn::from_u64(self.next_available_lsn.load(Ordering::Acquire))
    }

    /// Returns the last used LSN.
    pub fn get_last_used_lsn(&self) -> Lsn {
        Lsn::from_u64(self.last_used_lsn.load(Ordering::Acquire))
    }

    /// Sets the end-of-log position.
    ///
    /// Called during recovery to set where the log should continue from.
    pub fn set_last_position(
        &self,
        next_available_lsn: Lsn,
        last_used_lsn: Lsn,
    ) {
        self.last_used_lsn.store(last_used_lsn.as_u64(), Ordering::Release);
        self.per_file_last_lsn
            .write()
            .insert(last_used_lsn.file_number(), last_used_lsn);
        self.next_available_lsn
            .store(next_available_lsn.as_u64(), Ordering::Release);
        self.current_file_num
            .store(next_available_lsn.file_number(), Ordering::Release);
    }

    /// Enables/configures the Write Queue (JE reads `LOG_USE_WRITE_QUEUE` /
    /// `LOG_WRITE_QUEUE_SIZE` in the `FileManager` constructor,
    /// FileManager.java:341-345). Noxu defers to a setter so the many
    /// `FileManager::new` call sites in tests keep the direct-write path;
    /// production calls this from `environment_impl` with the values threaded
    /// through `DbiEnvConfig`.
    ///
    /// Must be called before any concurrent writes begin (during environment
    /// open, single-threaded). `size` is clamped to JE's [4 KiB, 256 MiB]
    /// range.
    pub fn configure_write_queue(&self, enabled: bool, size: usize) {
        let size = size.clamp(1 << 12, 1 << 28);
        self.write_queue_size.store(size as u64, Ordering::Release);
        if enabled {
            *self.write_queue.lock() = Some(WriteQueue::new(size));
        } else {
            *self.write_queue.lock() = None;
        }
        self.use_write_queue.store(enabled, Ordering::Release);
    }

    /// Whether the Write Queue is enabled (JE `useWriteQueue`).
    fn write_queue_enabled(&self) -> bool {
        self.use_write_queue.load(Ordering::Acquire)
    }

    /// Enqueue a blocked write for later execution (JE
    /// `LogEndFileDescriptor.enqueueWrite` / `enqueueWrite1`,
    /// FileManager.java:2861-2985). The fsync-lock is NOT held here.
    ///
    /// Returns `Ok(true)` if the bytes were queued (caller returns with no
    /// I/O), `Ok(false)` if the queue overflowed after up-to-two dequeue
    /// retries (caller must fall back to a direct write).
    ///
    /// The queue holds a single file's CONTIGUOUS writes; a
    /// `curPos + qwStartingOffset != destOffset` mismatch is a fatal log
    /// integrity error (JE `EnvironmentFailureReason.LOG_INTEGRITY`).
    fn enqueue_write(
        &self,
        file_num: u32,
        data: &[u8],
        dest_offset: u64,
    ) -> Result<bool> {
        // JE enqueueWrite: try enqueueWrite1 up to 2x, dequeuing between
        // attempts on a RelatchRequiredException (overflow). Give up after 2.
        for _ in 0..2 {
            match self.enqueue_write1(file_num, data, dest_offset)? {
                EnqueueOutcome::Queued => return Ok(true),
                EnqueueOutcome::Relatch => {
                    // Overflow: dequeue current writes (JE
                    // `dequeuePendingWrites`, which locks the fsync-lock),
                    // then retry.
                    self.dequeue_pending_writes()?;
                }
            }
        }
        // Give up after two tries (JE nWriteQueueOverflowFailures).
        self.n_write_queue_overflow.fetch_add(1, Ordering::Relaxed);
        Ok(false)
    }

    /// One enqueue attempt (JE `enqueueWrite1`, FileManager.java:2894-2985).
    /// The fsync-lock is NOT held; the queue mutex is taken internally.
    fn enqueue_write1(
        &self,
        file_num: u32,
        data: &[u8],
        dest_offset: u64,
    ) -> Result<EnqueueOutcome> {
        // JE: the queuedWrites queue only ever holds writes for a single file.
        // If the queue currently targets an OLDER file, dequeue it first so we
        // can retarget (JE: `if (qwFileNum < fileNum) { dequeuePendingWrites();
        // qwFileNum = fileNum; }`, done OUTSIDE the queuedWrites mutex because
        // dequeuePendingWrites takes the fsync-lock).
        {
            let need_dequeue = {
                let q = self.write_queue.lock();
                matches!(
                    q.as_ref().and_then(|q| q.qw_file_num),
                    Some(cur) if cur < file_num
                )
            };
            if need_dequeue {
                self.dequeue_pending_writes()?;
                if let Some(q) = self.write_queue.lock().as_mut() {
                    q.qw_file_num = Some(file_num);
                }
            }
        }

        let mut guard = self.write_queue.lock();
        let q = match guard.as_mut() {
            Some(q) => q,
            // Queue disabled between the enabled-check and here: treat as
            // overflow so the caller does a direct write.
            None => return Ok(EnqueueOutcome::Relatch),
        };

        let size = data.len();
        let overflow = (q.buf.len() - q.pos) < size;
        if overflow {
            // JE: throw RelatchRequiredException so the caller dequeues (under
            // the fsync-lock) then retries — we cannot dequeue here without
            // latching out of order (fsync-lock is below the queue mutex).
            return Ok(EnqueueOutcome::Relatch);
        }

        let cur_pos = q.pos;
        if cur_pos == 0 {
            // First entry in the queue sets the starting offset AND (re)targets
            // the file (covers the fresh / just-dequeued case, JE relies on
            // qwFileNum having been set by the caller / prior branch).
            q.qw_starting_offset = dest_offset;
            q.qw_file_num = Some(file_num);
        }

        // JE: non-consecutive writes are a fatal LOG_INTEGRITY error — the WAL
        // is append-only and the queue holds a single contiguous run.
        if q.qw_starting_offset + cur_pos as u64 != dest_offset
            || q.qw_file_num != Some(file_num)
        {
            return Err(LogError::Internal(format!(
                "write queue integrity: non-consecutive queued write \
                 (qw_file={:?} qw_start={} pos={} dest_file={file_num} \
                 dest_offset={dest_offset})",
                q.qw_file_num, q.qw_starting_offset, q.pos
            )));
        }

        q.buf[cur_pos..cur_pos + size].copy_from_slice(data);
        q.pos += size;
        Ok(EnqueueOutcome::Queued)
    }

    /// Flush pending queued writes, acquiring the fsync-lock first (JE
    /// `dequeuePendingWrites`, FileManager.java:2999-3010). Used from the
    /// overflow retry path.
    fn dequeue_pending_writes(&self) -> Result<()> {
        let _fsync = self.fsync_lock.lock();
        self.dequeue_pending_writes1()
    }

    /// Flush pending queued writes; the fsync-lock MUST already be held (JE
    /// `dequeuePendingWrites1`, FileManager.java:3015-3055). Writes the queued
    /// bytes to their destination file with a positioned write, then resets
    /// the queue.
    fn dequeue_pending_writes1(&self) -> Result<()> {
        // Snapshot the queued bytes under the queue mutex, then release it
        // before the pwrite (the file handle acquisition may block; we must
        // not hold the queue mutex across it). Because we hold the fsync-lock,
        // no concurrent enqueue can advance `pos` past what we snapshot: a
        // would-be enqueuer's `write_buffer_to_file` fails its `try_lock` and
        // enqueues — but a fresh enqueue after we reset targets a new starting
        // offset, and any enqueue racing THIS reset is serialised by the queue
        // mutex (we re-check pos under the mutex below).
        let (data, file_num, offset) = {
            let mut guard = self.write_queue.lock();
            let q = match guard.as_mut() {
                Some(q) if q.pos > 0 => q,
                _ => return Ok(()), // Nothing to see here. Move along.
            };
            let file_num = match q.qw_file_num {
                Some(f) => f,
                None => return Ok(()),
            };
            let data = q.buf[..q.pos].to_vec();
            let offset = q.qw_starting_offset;
            // Reset the queue now (JE resets queuedWritesPosition = 0 after the
            // write, still under the queuedWrites mutex + fsync-lock). We reset
            // BEFORE releasing the queue mutex so a concurrent enqueuer that
            // wins the queue mutex after us starts a fresh contiguous run.
            q.pos = 0;
            (data, file_num, offset)
        };

        // pwrite the queued bytes to the destination file (JE
        // getWritableFile(qwFileNum) + seek + write). Route through the
        // file-handle write path so it is covered by the DST fault layer.
        let handle = self.get_writable_file(file_num)?;
        {
            let mut fh = handle.acquire()?;
            fh.write_at(offset, &data)?;
        }
        self.n_writes_from_write_queue.fetch_add(1, Ordering::Relaxed);
        self.n_sequential_write_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);
        Ok(())
    }

    /// Get (or create) the writable file handle for `file_num` (JE
    /// `getWritableFile`). Must be reachable while holding the fsync-lock;
    /// uses `file_latch` for the create path exactly as `write_buffer_to_file`
    /// does. Reuses a cached handle when present.
    fn get_writable_file(&self, file_num: u32) -> Result<Arc<FileHandle>> {
        let _guard = self
            .file_latch
            .acquire()
            .map_err(|e| LogError::LatchTimeout(e.to_string()))?;
        if self.file_path(file_num).exists() {
            self.get_file_handle(file_num)
        } else {
            self.create_file_internal(file_num)
        }
    }

    /// If `[requested_offset, requested_offset+buf.len())` for `file_num`
    /// overlaps bytes still sitting in the write queue, copy the queued bytes
    /// into `buf` and return how many were served (JE
    /// `LogEndFileDescriptor.checkWriteCache`, FileManager.java:2808-2860).
    ///
    /// A reader at the end of the log may need bytes that were enqueued (and
    /// have since cycled out of the log buffer pool). Every end-of-log reader
    /// must consult the queue AFTER its disk read and overlay any queued bytes
    /// that the disk read could not have seen.
    ///
    /// Returns the number of bytes written into `buf` starting at `buf[0]`
    /// that correspond to file offsets `[requested_offset, ...)`. `0` means
    /// nothing in the queue matched.
    fn check_write_cache(
        &self,
        buf: &mut [u8],
        requested_offset: u64,
        file_num: u32,
    ) -> usize {
        if !self.write_queue_enabled() {
            return 0;
        }
        let guard = self.write_queue.lock();
        let q = match guard.as_ref() {
            Some(q) => q,
            None => return 0,
        };
        if q.qw_file_num != Some(file_num) || q.pos == 0 {
            return 0;
        }
        let qw_end = q.qw_starting_offset + q.pos as u64;
        // Requested range must overlap [qw_starting_offset, qw_end).
        if requested_offset < q.qw_starting_offset || requested_offset >= qw_end
        {
            return 0;
        }
        let src_start = (requested_offset - q.qw_starting_offset) as usize;
        let avail = q.pos - src_start;
        let n = avail.min(buf.len());
        buf[..n].copy_from_slice(&q.buf[src_start..src_start + n]);
        self.n_bytes_read_from_write_queue
            .fetch_add(n as u64, Ordering::Relaxed);
        n
    }

    /// Gets a file handle for the given file number.
    ///
    /// Checks the LRU cache first.
    /// On a cache miss the file is opened, its header validated, and the
    /// resulting `Arc<FileHandle>` is inserted — with automatic LRU eviction
    /// when the cache is at capacity.  Because `lru::LruCache::get()` mutates
    /// the eviction order, the entire lookup+insert is done under a single
    /// `Mutex` lock, eliminating any TOCTOU race between a cache miss and the
    /// subsequent insert.
    pub fn get_file_handle(&self, file_num: u32) -> Result<Arc<FileHandle>> {
        let mut cache = self.file_cache.lock();

        // Fast path: cache hit — LruCache::get() promotes the entry to MRU.
        if let Some(handle) = cache.get(&file_num) {
            return Ok(handle.clone());
        }

        // Slow path: open the file, validate its header, and insert into cache.
        let path = self.file_path(file_num);
        if !path.exists() {
            return Err(LogError::FileNotFound(format!(
                "Log file not found: {}",
                path.display()
            )));
        }

        let mut handle = FileHandle::new(file_num);

        // Open the file.
        let file = if self.read_only {
            File::open(&path)?
        } else {
            OpenOptions::new().read(true).write(true).open(&path)?
        };

        // Read and validate the header.
        let log_version = self.read_and_validate_header(&file, file_num)?;

        // Initialize the handle.
        handle.init(file, log_version);

        let handle = Arc::new(handle);

        // Insert into the LRU cache (evicts the least-recently-used entry when
        // the cache is at capacity, mirroring FileHandleCache eviction).
        cache.put(file_num, handle.clone());
        self.n_file_opens.fetch_add(1, Ordering::Relaxed);

        Ok(handle)
    }

    /// Reads and validates the file header.
    ///
    /// Handles both v2 (32-byte) and v3 (36-byte) files:
    ///
    /// - Reads up to `FILE_HEADER_SIZE` (36) bytes, capped at the actual file
    ///   length. For a v2 file the 32-byte header is followed by entry bytes
    ///   that are irrelevant; `FileHeader::read_from` stops consuming after
    ///   32 bytes when it sees `log_version == 2`.
    /// - A v2 header-only file (exactly 32 bytes) is read cleanly without
    ///   over-reading.
    fn read_and_validate_header(
        &self,
        file: &File,
        file_num: u32,
    ) -> Result<u32> {
        // Clamp to actual file length so we never over-read a legacy v2
        // header-only file (exactly 32 bytes).
        let file_len = file.metadata()?.len() as usize;
        let read_size = FILE_HEADER_SIZE.min(file_len);
        let mut header_buf = vec![0u8; read_size];
        crate::posio::read_exact_at(file, &mut header_buf, 0)?;

        // Parse header — read_from branches on version.
        let mut cursor = std::io::Cursor::new(header_buf);
        let header = FileHeader::read_from(&mut cursor)?;

        // Validate
        header.validate(file_num)
    }

    /// Returns the on-disk header size (= first-entry byte offset) for a
    /// given log file.
    ///
    /// Opens (or cache-hits) the file to read its `log_version`, then
    /// returns `FileHeader::on_disk_size(version)`:
    ///
    /// - v2 file → 32 bytes → first entry at offset 32
    /// - v3 file → 36 bytes → first entry at offset 36
    ///
    /// Use this whenever computing the "first entry offset" for an
    /// **existing** file instead of the bare `FILE_HEADER_SIZE` constant.
    pub fn file_header_size_for(&self, file_num: u32) -> Result<usize> {
        let handle = self.get_file_handle(file_num)?;
        Ok(on_disk_size(handle.log_version()))
    }

    /// Creates a new log file with the given file number.
    ///
    /// Writes the file header with a link to the previous file.
    pub fn create_file(&self, file_num: u32) -> Result<Arc<FileHandle>> {
        let _guard = self
            .file_latch
            .acquire()
            .map_err(|e| LogError::LatchTimeout(e.to_string()))?;
        self.create_file_internal(file_num)
    }

    /// Flips to the next log file.
    ///
    /// Called when the current file reaches its maximum size.
    pub fn flip_file(&self) -> Result<u32> {
        let _guard = self
            .file_latch
            .acquire()
            .map_err(|e| LogError::LatchTimeout(e.to_string()))?;

        let current = self.current_file_num.load(Ordering::Acquire);
        let next = current + 1;

        // Save last LSN for current file
        let last_lsn =
            Lsn::from_u64(self.last_used_lsn.load(Ordering::Acquire));
        if !last_lsn.is_null() {
            self.per_file_last_lsn.write().insert(current, last_lsn);
        }

        // Create next file (note: create_file_internal doesn't acquire the latch)
        self.create_file_internal(next)?;

        // Update current file number
        self.current_file_num.store(next, Ordering::Release);

        // Update next available LSN to point to start of new file
        self.next_available_lsn.store(
            Lsn::new(next, first_log_entry_offset()).as_u64(),
            Ordering::Release,
        );

        Ok(next)
    }

    /// Internal helper to create a file without acquiring the file latch.
    fn create_file_internal(&self, file_num: u32) -> Result<Arc<FileHandle>> {
        if self.read_only {
            return Err(LogError::WriteFailed(
                "Cannot create file in read-only mode".to_string(),
            ));
        }

        let path = self.file_path(file_num);

        // Create the file
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;

        // Determine last entry offset in previous file
        let last_entry_offset = if file_num > 0 {
            self.per_file_last_lsn
                .read()
                .get(&(file_num - 1))
                .map(|lsn| lsn.file_offset())
                .unwrap_or(0)
        } else {
            0
        };

        // Write the header
        let header = FileHeader::new(file_num, last_entry_offset);
        header.write_to(&mut file)?;
        file.flush()?;
        file.sync_all()?;

        // C-1 (2026 audit F-3.1 / 2026 audit 1-G):
        // After fsync-ing the new file, fsync the parent directory so the
        // directory entry itself is durable.  Without this a power-loss between
        // file creation and the next directory write loses the file entirely.
        // Cross-platform: real dir-fsync on Unix; best-effort on Windows
        // (directory handle needs FILE_FLAG_BACKUP_SEMANTICS; NTFS journals
        // the entry).  See `crate::posio::sync_dir`.
        crate::posio::sync_dir(&self.env_dir)?;

        // Create handle
        let mut handle = FileHandle::new(file_num);
        handle.init(file, LOG_VERSION);

        let handle = Arc::new(handle);

        // Insert into the LRU cache.
        self.file_cache.lock().put(file_num, handle.clone());

        Ok(handle)
    }

    /// Deletes a log file.
    ///
    /// Used by the cleaner to remove old log files.
    pub fn delete_file(&self, file_num: u32) -> Result<()> {
        if self.read_only {
            return Err(LogError::WriteFailed(
                "Cannot delete file in read-only mode".to_string(),
            ));
        }

        // Remove from cache.
        self.file_cache.lock().pop(&file_num);

        // Delete the file
        let path = self.file_path(file_num);
        if path.exists() {
            fs::remove_file(&path)?;
        }

        Ok(())
    }

    /// Clears the file handle cache.
    pub fn clear_cache(&self) {
        self.file_cache.lock().clear();
    }

    /// Physically truncate log file `file_num` to `offset` bytes (JE
    /// `FileManager.truncateSingleFile`, FileManager.java:2345). Used at
    /// recovery to remove a torn / half-written trailing entry so it cannot
    /// be misread on a later scan. Evicts the cached handle first so a stale
    /// open handle does not see the old length.
    pub fn truncate_single_file(
        &self,
        file_num: u32,
        offset: u64,
    ) -> Result<()> {
        if self.read_only {
            return Err(LogError::WriteFailed(
                "Cannot truncate file in read-only mode".to_string(),
            ));
        }
        // Drop any cached handle so the next open sees the truncated length.
        self.file_cache.lock().pop(&file_num);
        let path = self.file_path(file_num);
        if path.exists() {
            let f = fs::OpenOptions::new().write(true).open(&path)?;
            f.set_len(offset)?;
            f.sync_all()?;
        }
        Ok(())
    }

    /// Flip the invisible bit (flags 0x10) on each LSN's log-entry header,
    /// in file order, WITHOUT recomputing the checksum.
    ///
    /// Port of JE `FileManager.makeInvisible` (called from
    /// `RollbackTracker.setInvisible`). The invisible bit is excluded from the
    /// CRC at read time (cloaked, see `LogEntryHeader.turnOffInvisible` /
    /// `log_manager` checksum path), so flipping it in place is a single-byte
    /// `pwrite` per entry. The flags byte is at `file_offset + FLAGS_OFFSET`
    /// (offset 5) of each entry.
    ///
    /// Caller must `force` the affected files afterwards for durability
    /// (JE `RollbackTracker.recoveryEndFsyncInvisible`).
    pub fn make_invisible(&self, file_num: u32, offsets: &[u32]) -> Result<()> {
        if self.read_only {
            return Err(LogError::WriteFailed(
                "Cannot make entries invisible in read-only mode".to_string(),
            ));
        }
        if offsets.is_empty() {
            return Ok(());
        }
        let path = self.file_path(file_num);
        if !path.exists() {
            return Ok(());
        }
        // Drop any cached handle so the bit flip is observed on the next read.
        self.file_cache.lock().pop(&file_num);
        let file = OpenOptions::new().read(true).write(true).open(&path)?;
        // FLAGS_OFFSET within an entry header is 5 (checksum[0..4], type[4],
        // flags[5]).
        const FLAGS_OFFSET: u64 = 5;
        const INVISIBLE_MASK: u8 = 0x10;
        for &off in offsets {
            let flags_pos = off as u64 + FLAGS_OFFSET;
            let mut byte = [0u8; 1];
            Self::pread_exact(&file, flags_pos, &mut byte)?;
            byte[0] |= INVISIBLE_MASK;
            Self::pwrite_exact(&file, flags_pos, &byte)?;
        }
        Ok(())
    }

    /// fsync the given set of log files (JE `FileManager.force`). Used after
    /// `make_invisible` to make the rollback's invisible bits durable so a
    /// crash mid-rollback does not re-apply rolled-back entries.
    pub fn force(&self, file_nums: &[u32]) -> Result<()> {
        if self.read_only {
            return Ok(());
        }
        for &file_num in file_nums {
            let path = self.file_path(file_num);
            if path.exists() {
                let f = OpenOptions::new().write(true).open(&path)?;
                f.sync_all()?;
            }
        }
        Ok(())
    }

    #[cfg(unix)]
    fn pread_exact(file: &File, offset: u64, buf: &mut [u8]) -> Result<()> {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(buf, offset)?;
        Ok(())
    }

    #[cfg(unix)]
    fn pwrite_exact(file: &File, offset: u64, buf: &[u8]) -> Result<()> {
        // Route header writes through posio so the DST fault layer covers them
        // too (inactive in production -> identical to a direct write_all_at).
        crate::posio::write_all_at(file, buf, offset)?;
        Ok(())
    }

    #[cfg(not(unix))]
    fn pread_exact(file: &File, offset: u64, buf: &mut [u8]) -> Result<()> {
        use std::io::{Read, Seek, SeekFrom};
        let mut f = file.try_clone()?;
        f.seek(SeekFrom::Start(offset))?;
        f.read_exact(buf)?;
        Ok(())
    }

    #[cfg(not(unix))]
    fn pwrite_exact(file: &File, offset: u64, buf: &[u8]) -> Result<()> {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = file.try_clone()?;
        f.seek(SeekFrom::Start(offset))?;
        f.write_all(buf)?;
        Ok(())
    }

    /// Truncate the log at (`file_num`, `offset`): truncate `file_num` to
    /// `offset` and delete every higher-numbered file, in descending order to
    /// avoid a log-entry gap (JE `FileManager.truncateLog`, FileManager.java:2374,
    /// SR [#19463]). If `offset == 0` the file header itself is gone, so the
    /// whole file is deleted too.
    pub fn truncate_log(&self, file_num: u32, offset: u64) -> Result<()> {
        if self.read_only {
            return Err(LogError::WriteFailed(
                "Cannot truncate log in read-only mode".to_string(),
            ));
        }
        let last = self.get_last_file_num()?.unwrap_or(file_num);
        let mut i = last as i64;
        while i >= file_num as i64 {
            let fnum = i as u32;
            if self.file_path(fnum).exists() {
                if fnum == file_num {
                    self.truncate_single_file(fnum, offset)?;
                    if offset != 0 {
                        i -= 1;
                        continue;
                    }
                }
                self.delete_file(fnum)?;
            }
            i -= 1;
        }
        Ok(())
    }

    /// Writes `data` to the current log file at the given file offset.
    ///
    /// `writeToFile()`.  The caller must supply the exact file-level byte
    /// offset at which `data` should be written (i.e. `firstLsn.fileOffset`
    /// in terms).  After a successful write the method checks whether the
    /// file has grown past `max_file_size`; if so it calls `flip_file()` and
    /// returns the new file number, otherwise it returns the current one.
    ///
    /// # Arguments
    /// * `data`        - The raw bytes to append (header + payload).
    /// * `file_offset` - Byte offset within the file at which to write.
    ///
    /// # Returns
    /// The file number that was actually written to.
    /// Writes `data` at `file_offset` within log file `file_num`.
    ///
    /// JE faithfulness: JE `FileManager.writeLogBuffer` uses
    /// `fullBuffer.getFirstLsn()` to determine which file to write to, not
    /// `currentFileNum`.  This method mirrors that by accepting an explicit
    /// `file_num` parameter so `write_dirty` and `fill_flush_pending` can
    /// write dirty buffers to the file their `first_lsn` belongs to.
    ///
    /// The auto-flip (check `file_len >= max_file_size` and call `flip_file`)
    /// has been removed: file flips are managed exclusively by
    /// `LogManager::log_internal` via the `flipped` flag and
    /// `get_write_buffer`/`sync_log_end_and_finish_file`.  Auto-flip in this
    /// method would race with the explicit flip and double-create files.
    pub fn write_buffer_to_file(
        &self,
        file_num: u32,
        data: &[u8],
        file_offset: u64,
    ) -> Result<()> {
        self.write_to_file(file_num, data, file_offset, false)
    }

    /// Like [`write_buffer_to_file`] but FORCES a direct positioned write and
    /// never enqueues into the Write Queue (JE `writeToFile(..., flushWriteQueue=true)`).
    ///
    /// This is the durability-critical variant used by the COMMIT_SYNC drain in
    /// `LogManager::flush_sync`.  The bounded fsync pipeline (depth > 1) lets
    /// several leaders capture disjoint drained ranges and fdatasync
    /// concurrently; each leader then advances the durable watermark
    /// (`last_synced_lsn`) to the logical `eol` it captured.  For that advance
    /// to be sound, every byte below `eol` MUST already be in the OS page cache
    /// (pwritten, not merely queued) before the leader's fdatasync runs —
    /// otherwise a higher-eol leader could publish a watermark that names bytes
    /// still sitting in the Write Queue that no completed fdatasync has covered.
    /// Forcing the direct write here (never enqueue) guarantees the drained
    /// bytes reach the page cache synchronously, so the leader's own fdatasync
    /// — and any concurrent higher-eol leader's fdatasync — covers them.
    ///
    /// The Write Queue still serves NON-commit / background writers (they use
    /// `write_buffer_to_file`, which may enqueue to overlap with an in-flight
    /// fsync).  Only the synchronous commit flush path force-writes.
    pub fn write_buffer_to_file_forced(
        &self,
        file_num: u32,
        data: &[u8],
        file_offset: u64,
    ) -> Result<()> {
        self.write_to_file(file_num, data, file_offset, true)
    }

    /// Writes `data` at `file_offset` within log file `file_num`, with the JE
    /// Write Queue interposed (JE `FileManager.writeToFile`,
    /// FileManager.java:1738-1816).
    ///
    /// `flush_write_queue = true` forces a direct write and never enqueues (JE
    /// `flushWriteQueue` argument, set by the file-flip drain so the OLD
    /// file's bytes land before the file switch, FileManager.java:1778).
    ///
    /// Algorithm (faithful to JE writeToFile):
    ///   1. `try_lock` the fsync-lock.
    ///   2. If NOT acquired && write-queue enabled && !flush_write_queue →
    ///      enqueue and RETURN with no I/O.
    ///   3. Otherwise (acquired, or enqueue overflowed): if we did not already
    ///      hold the fsync-lock, acquire it blocking; dequeue pending writes;
    ///      do the positioned write; release the fsync-lock.
    pub fn write_to_file(
        &self,
        file_num: u32,
        data: &[u8],
        file_offset: u64,
        flush_write_queue: bool,
    ) -> Result<()> {
        if self.read_only {
            return Err(LogError::WriteFailed(
                "Cannot write in read-only mode".to_string(),
            ));
        }

        // JE writeToFile step 1: try to grab the fsync latch (non-blocking).
        // If we can't get it, an fsync or write is in progress and we'd block
        // anyway — so queue the write instead (unless a forced flush).
        let use_wq = self.write_queue_enabled();
        let fsync_guard = self.fsync_lock.try_lock();
        let fsync_acquired = fsync_guard.is_some();

        if !fsync_acquired && use_wq && !flush_write_queue {
            // JE: enqueueWrite. On success the write is deferred to the next
            // thread that fsyncs or writes; we return with NO I/O. This is the
            // committer-decoupling that overlaps writes with in-flight fsyncs.
            if self.enqueue_write(file_num, data, file_offset)? {
                self.n_sequential_writes.fetch_add(1, Ordering::Relaxed);
                self.n_sequential_write_bytes
                    .fetch_add(data.len() as u64, Ordering::Relaxed);
                return Ok(());
            }
            // enqueue overflowed (fell through 2 dequeue retries): fall to the
            // direct-write path below, acquiring the fsync-lock blocking.
        }

        // JE writeToFile step 3: direct write under the fsync-lock. If we did
        // not already hold it (try_lock failed), acquire it blocking now.
        let _blocking;
        let _held = if fsync_acquired {
            fsync_guard
        } else {
            _blocking = self.fsync_lock.lock();
            None
        };

        // JE: dequeue pending writes BEFORE our own write, so any queued bytes
        // (which precede ours in the file) are written first, preserving
        // on-disk order.
        if use_wq {
            self.dequeue_pending_writes1()?;
        }

        // Obtain (or create) the file handle under `file_latch`.
        //
        // We MUST hold `file_latch` for the entire exists-check → get/create
        // sequence to avoid a TOCTOU race:
        //
        //   Thread A: inside create_file_internal (created empty file, writing
        //             header but not done yet)
        //   Thread B: file_path.exists()=true → get_file_handle → tries to
        //             read the header from an empty file → "failed to fill
        //             whole buffer" (UnexpectedEof)
        //
        // Holding file_latch serialises creation and subsequent opens so that
        // Thread B waits until Thread A's create_file_internal (which also
        // holds file_latch) has written and fsynced the full header.
        let handle = {
            let _guard = self
                .file_latch
                .acquire()
                .map_err(|e| LogError::LatchTimeout(e.to_string()))?;

            if self.file_path(file_num).exists() {
                self.get_file_handle(file_num)?
            } else {
                // create_file_internal (called here directly, since we already
                // hold file_latch) creates the file, writes the header, fsyncs.
                self.create_file_internal(file_num)?
            }
        };

        {
            let mut guard = handle.acquire()?;
            guard.write_at(file_offset, data)?;
        }
        // fsync-lock released here (guard drop).

        self.n_sequential_writes.fetch_add(1, Ordering::Relaxed);
        self.n_sequential_write_bytes
            .fetch_add(data.len() as u64, Ordering::Relaxed);

        Ok(())
    }

    /// Writes `data` at `file_offset` within the CURRENT log file.
    ///
    /// For new entries written by `LogManager::log_internal` when the entry is
    /// too large for the buffer pool (temp-buffer path). The current file is
    /// always correct here because `set_last_position` has already advanced
    /// `current_file_num` to the file that holds `current_lsn`.
    ///
    /// Callers that write data belonging to a SPECIFIC file (dirty buffer
    /// flush in `write_dirty` / `fill_flush_pending`) must use
    /// `write_buffer_to_file` instead to avoid writing old data to the
    /// wrong file after a flip.
    pub fn write_buffer(&self, data: &[u8], file_offset: u64) -> Result<u32> {
        if self.read_only {
            return Err(LogError::WriteFailed(
                "Cannot write in read-only mode".to_string(),
            ));
        }

        let file_num = self.current_file_num.load(Ordering::Acquire);
        self.write_buffer_to_file(file_num, data, file_offset)?;
        Ok(file_num)
    }

    /// Reads bytes from a log file at a given offset.
    ///
    ///
    ///
    /// # Arguments
    /// * `file_num` - The log file number to read from.
    /// * `offset`   - Byte offset within the file.
    /// * `buf`      - Output buffer; filled with as many bytes as available
    ///   (may be less than `buf.len()` at end of file).
    ///
    /// # Returns
    /// The number of bytes actually read.
    pub fn read_from_file(
        &self,
        file_num: u32,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<usize> {
        let handle = self.get_file_handle(file_num)?;
        let mut guard = handle.acquire()?;
        let n = guard.read_at(offset, buf)?;
        drop(guard);
        let n = self.overlay_write_cache(file_num, offset, buf, n);
        self.n_sequential_reads.fetch_add(1, Ordering::Relaxed);
        self.n_sequential_read_bytes.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }

    /// Reads bytes from a log file at a given offset, counted as a random
    /// (point-lookup) read rather than a sequential scan read.
    ///
    /// Used by `LogManager::read_at_lsn` for in-flight log reads.
    pub fn read_from_file_random(
        &self,
        file_num: u32,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<usize> {
        let handle = self.get_file_handle(file_num)?;
        let mut guard = handle.acquire()?;
        let n = guard.read_at(offset, buf)?;
        drop(guard);
        let n = self.overlay_write_cache(file_num, offset, buf, n);
        self.n_random_reads.fetch_add(1, Ordering::Relaxed);
        self.n_random_read_bytes.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
    }

    /// Overlay any bytes still sitting in the write queue on top of a disk
    /// read (JE `checkWriteCache`, called from the log-source read path).
    ///
    /// `disk_n` is how many bytes the positioned disk read produced. If the
    /// requested region overlaps the queue, the queued bytes are authoritative
    /// for that overlap (they may not yet be on disk, or the disk copy may be
    /// stale relative to a queued rewrite — though the append-only WAL never
    /// rewrites, so they are identical when both exist). Returns the total
    /// number of valid bytes in `buf` (max of the disk read and the queue
    /// coverage).
    ///
    /// Correctness (the subtlest point of the Write Queue port): a reader at
    /// the end of the log — recovery's tail scan, a rep syncup / feeder
    /// end-of-log read, or an in-flight fault-in — may request bytes that a
    /// committer enqueued and that have since cycled out of the log buffer
    /// pool. Those bytes exist ONLY in the queue until the next fsync/write
    /// dequeues them. Consulting the queue here makes every disk-read path see
    /// them. `mmap_file` is exempt because it refuses the current write file,
    /// and the queue only ever holds current-file bytes.
    fn overlay_write_cache(
        &self,
        file_num: u32,
        offset: u64,
        buf: &mut [u8],
        disk_n: usize,
    ) -> usize {
        if !self.write_queue_enabled() {
            return disk_n;
        }
        let n_from_queue = self.check_write_cache(buf, offset, file_num);
        disk_n.max(n_from_queue)
    }

    /// Returns the length of a log file in bytes.
    pub fn get_file_length(&self, file_num: u32) -> Result<u64> {
        let path = self.file_path(file_num);
        if !path.exists() {
            return Err(LogError::FileNotFound(format!(
                "Log file not found: {}",
                path.display()
            )));
        }
        Ok(path.metadata()?.len())
    }

    /// Memory-maps a log file for read-only sequential access.
    ///
    /// Returns a `Mmap` covering the entire file.  The OS handles page-in
    /// lazily with automatic sequential read-ahead, eliminating all per-entry
    /// `pread64` syscalls during recovery scanning.
    ///
    /// # Safety
    /// The caller must not hold a mutable reference into the mapped memory
    /// while other processes write to the file.  During recovery, log files
    /// are read-only, making this safe.
    pub fn mmap_file(&self, file_num: u32) -> Result<Mmap> {
        // Never mmap the current write file. It can be appended to
        // concurrently (pwrite64 on the log-writer thread) while a
        // disk-ordered cursor reads it; `memmap2` requires that a mapped file
        // is not modified for the lifetime of the mapping, so mapping the live
        // write file would be undefined behaviour. Callers (e.g. the
        // file-manager log scanner) fall back to positioned `pread` reads,
        // which are safe under concurrent appends. Complete (sealed) files are
        // never written again and are safe to map.
        if file_num == self.get_current_file_num() {
            return Err(LogError::Io(std::io::Error::other(format!(
                "refusing to mmap the current write file {file_num} \
                 (may be concurrently appended); use pread fallback"
            ))));
        }
        let path = self.file_path(file_num);
        let file = File::open(&path).map_err(|e| {
            LogError::FileNotFound(format!(
                "Cannot open {:?} for mmap: {}",
                path, e
            ))
        })?;
        // SAFETY: `file_num` is not the current write file (checked above), so
        // it is a sealed log file whose bytes do not change for the lifetime
        // of the mapping.
        let mmap = unsafe { Mmap::map(&file) }.map_err(|e| {
            LogError::Io(std::io::Error::other(format!(
                "mmap {:?}: {}",
                path, e
            )))
        })?;
        Ok(mmap)
    }

    /// Returns current I/O statistics for this FileManager.
    pub fn get_io_stats(&self) -> FileManagerIoStats {
        FileManagerIoStats {
            n_file_opens: self.n_file_opens.load(Ordering::Relaxed),
            n_sequential_reads: self.n_sequential_reads.load(Ordering::Relaxed),
            n_sequential_read_bytes: self
                .n_sequential_read_bytes
                .load(Ordering::Relaxed),
            n_sequential_writes: self
                .n_sequential_writes
                .load(Ordering::Relaxed),
            n_sequential_write_bytes: self
                .n_sequential_write_bytes
                .load(Ordering::Relaxed),
            n_random_reads: self.n_random_reads.load(Ordering::Relaxed),
            n_random_read_bytes: self
                .n_random_read_bytes
                .load(Ordering::Relaxed),
        }
    }

    /// Fsyncs the current log file to stable storage and removes it from the
    /// file-handle cache, making the old file handle eligible for GC.
    ///
    /// JE faithfulness (Part-3, DRIFT-3/7): mirrors
    /// `FileManager.syncLogEndAndFinishFile()` which calls `syncLogEnd()` then
    /// `endOfLog.close()`.  Called by `LogBufferPool.getWriteBuffer` when
    /// `flippedFile=true`, under the LWL, BEFORE `advanceLsn` advances the
    /// LSN bookkeeping to the new file.  This establishes the JE invariant
    /// that the OLD file is durably closed before any entry is written to the
    /// NEW file.
    ///
    /// References:
    /// - JE `FileManager.syncLogEndAndFinishFile` (line 2077)
    /// - JE `LogBufferPool.getWriteBuffer` (called after `bumpAndWriteDirty`
    ///   when `flippedFile=true`)
    pub fn sync_log_end_and_finish_file(&self) -> Result<()> {
        self.sync_log_end()?;
        // Evict the current (old) file from the LRU cache so its OS file
        // descriptor is released promptly — JE `endOfLog.close()`.
        let file_num = self.current_file_num.load(Ordering::Acquire);
        let mut cache = self.file_cache.lock();
        cache.pop(&file_num);
        Ok(())
    }

    /// Fsyncs the current log file to stable storage.
    ///
    /// JE: `FileManager.syncLogEnd()` → `LogEndFileDescriptor.force()`
    /// (FileManager.java:3082-3149).
    ///
    /// Faithful to JE `force()`: acquire the fsync-lock (blocking), DEQUEUE
    /// any pending queued writes (real positioned writes of the queued bytes),
    /// then fdatasync, then release. Draining the queue before the fdatasync
    /// is what makes the enqueue path durable: a committer's enqueued bytes
    /// are written AND covered by this fdatasync before the leader's
    /// `flush_and_sync` returns, so no commit is ever told durable early.
    pub fn sync_log_end(&self) -> Result<()> {
        if self.read_only {
            return Ok(());
        }

        let file_num = self.current_file_num.load(Ordering::Acquire);
        let path = self.file_path(file_num);

        if !path.exists() {
            // Nothing to sync yet.
            return Ok(());
        }

        // JE force(): take the fsync-lock ONLY to drain the write queue (the
        // queue is shared mutable state), then RELEASE it before the fdatasync
        // so concurrent committers' fdatasyncs can overlap (bounded fsync
        // pipeline).  Holding the lock across the fdatasync would reserialise
        // every committer to one-sync-at-a-time — the exact bottleneck we are
        // removing.  The dequeue-before-fdatasync ordering is preserved: any
        // queued bytes are pwritten (into the page cache) under the lock, so
        // the fdatasync below — which is idempotent and covers ALL prior
        // page-cache writes — makes them durable.
        {
            let _fsync = self.fsync_lock.lock();
            if self.write_queue_enabled() {
                // Flush any queued writes so their bytes are in the page cache
                // and covered by the fdatasync below (JE
                // `dequeuePendingWrites1()` before `ch.force(false)`).
                self.dequeue_pending_writes1()?;
            }
        }
        // fsync-lock released — the fdatasync runs WITHOUT it, so up to N
        // committers can fdatasync the same fd concurrently (Linux serialises
        // fdatasync internally; it is safe concurrent with pwrite on the same
        // descriptor).

        let handle = self.get_file_handle(file_num)?;
        // JE parity + write-scaling fix: fdatasync WITHOUT holding the file's
        // exclusive write latch, so concurrent pwrites (the next group's drain)
        // proceed during this in-flight fsync instead of serialising behind it
        // (JE FileManager separate fsyncFileSynchronizer + Write Queue).
        handle.sync_data_no_latch()?;

        // JE force(): flush any writes queued WHILE we were fsync'ing, so a
        // writer that enqueued during the fdatasync does not leave bytes
        // stranded in the queue targeting this (now-synced) file. These bytes
        // are NOT covered by the fdatasync we just did; a committer relying on
        // them will call flush_sync again, which drains + fsyncs them. This
        // second drain matches JE exactly (the trailing dequeuePendingWrites1
        // in force()).  Re-acquire the fsync-lock briefly for the drain only.
        if self.write_queue_enabled() {
            let _fsync = self.fsync_lock.lock();
            self.dequeue_pending_writes1()?;
        }
        Ok(())
    }

    /// Closes the file manager, releasing all resources.
    pub fn close(&self) -> Result<()> {
        self.clear_cache();

        // Release the lock file
        if let Some(lock_file) = self.lock_file.write().take() {
            {
                #[allow(unused_imports)]
                use fs2::FileExt;
                let _ = lock_file.unlock();
            }
            drop(lock_file);
        }

        Ok(())
    }
}

impl Drop for FileManager {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// Snapshot of FileManager I/O statistics.
///
/// FILEMGR_FILE_OPENS, FILEMGR_SEQUENTIAL_READS/WRITES,
/// FILEMGR_RANDOM_READS etc.
#[derive(Debug, Clone, Default)]
pub struct FileManagerIoStats {
    /// Number of log files opened (LRU cache miss).
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
    /// Total bytes from random read operations.
    pub n_random_read_bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_format_parse_file_number() {
        assert_eq!(format_file_number(0), "00000000");
        assert_eq!(format_file_number(42), "0000002a");
        assert_eq!(format_file_number(255), "000000ff");
        assert_eq!(format_file_number(0x12345678), "12345678");

        assert_eq!(parse_file_number("00000000.ndb"), Some(0));
        assert_eq!(parse_file_number("0000002a.ndb"), Some(42));
        assert_eq!(parse_file_number("000000ff.ndb"), Some(255));
        assert_eq!(parse_file_number("12345678.ndb"), Some(0x12345678));

        assert_eq!(parse_file_number("invalid.ndb"), None);
        assert_eq!(parse_file_number("00000000.txt"), None);
    }

    #[test]
    fn test_file_manager_create() {
        let temp_dir = TempDir::new().unwrap();
        let manager =
            FileManager::new(temp_dir.path(), false, 10_000_000, 100).unwrap();

        assert_eq!(manager.get_current_file_num(), 0);
        assert_eq!(manager.get_first_file_num().unwrap(), None);
    }

    #[test]
    fn test_file_manager_create_file() {
        let temp_dir = TempDir::new().unwrap();
        let manager =
            FileManager::new(temp_dir.path(), false, 10_000_000, 100).unwrap();

        let handle = manager.create_file(0).unwrap();
        assert_eq!(handle.file_num(), 0);
        assert_eq!(handle.log_version(), LOG_VERSION);

        // File should exist
        let path = manager.file_path(0);
        assert!(path.exists());

        // Should be able to get it again from cache
        let handle2 = manager.get_file_handle(0).unwrap();
        assert_eq!(handle2.file_num(), 0);
    }

    #[test]
    fn test_file_manager_list_files() {
        let temp_dir = TempDir::new().unwrap();
        let manager =
            FileManager::new(temp_dir.path(), false, 10_000_000, 100).unwrap();

        manager.create_file(0).unwrap();
        manager.create_file(2).unwrap();
        manager.create_file(1).unwrap();

        let files = manager.list_file_numbers().unwrap();
        assert_eq!(files, vec![0, 1, 2]);

        assert_eq!(manager.get_first_file_num().unwrap(), Some(0));
        assert_eq!(manager.get_last_file_num().unwrap(), Some(2));
    }

    #[test]
    fn test_file_manager_flip_file() {
        let temp_dir = TempDir::new().unwrap();

        {
            let manager =
                FileManager::new(temp_dir.path(), false, 10_000_000, 100)
                    .unwrap();

            // Create initial file
            manager.create_file(0).unwrap();

            // Set current file
            manager.current_file_num.store(0, Ordering::Release);
            manager
                .last_used_lsn
                .store(Lsn::new(0, 1000).as_u64(), Ordering::Release);

            // Flip to next file
            let next = manager.flip_file().unwrap();
            assert_eq!(next, 1);
            assert_eq!(manager.get_current_file_num(), 1);

            // Should have created file 1
            let files = manager.list_file_numbers().unwrap();
            assert!(files.contains(&1));
        } // manager dropped here, releasing lock
    }

    #[test]
    fn test_environment_locking() {
        let temp_dir = TempDir::new().unwrap();

        // First manager locks the environment
        let _manager1 =
            FileManager::new(temp_dir.path(), false, 10_000_000, 100).unwrap();

        // Second manager should fail to lock
        let result = FileManager::new(temp_dir.path(), false, 10_000_000, 100);
        assert!(result.is_err());
        match result {
            Err(LogError::EnvironmentLocked(_)) => (),
            _ => panic!("Expected EnvironmentLocked error"),
        }
    }

    #[test]
    fn test_nonexistent_directory_fails() {
        let result = FileManager::new(
            "/tmp/does_not_exist_noxu_xyz",
            false,
            10_000_000,
            100,
        );
        assert!(result.is_err());
        match result {
            Err(LogError::InvalidDirectory(_)) => (),
            _ => panic!("Expected InvalidDirectory error"),
        }
    }

    #[test]
    fn test_get_file_handle_missing_file_fails() {
        let temp_dir = TempDir::new().unwrap();
        let manager =
            FileManager::new(temp_dir.path(), false, 10_000_000, 100).unwrap();

        let result = manager.get_file_handle(99);
        assert!(result.is_err());
        match result {
            Err(LogError::FileNotFound(_)) => (),
            _ => panic!("Expected FileNotFound error"),
        }
    }

    #[test]
    fn test_delete_file() {
        let temp_dir = TempDir::new().unwrap();
        let manager =
            FileManager::new(temp_dir.path(), false, 10_000_000, 100).unwrap();

        manager.create_file(0).unwrap();
        assert!(manager.file_path(0).exists());

        manager.delete_file(0).unwrap();
        assert!(!manager.file_path(0).exists());
        assert_eq!(manager.list_file_numbers().unwrap(), Vec::<u32>::new());
    }

    #[test]
    fn test_delete_nonexistent_file_is_ok() {
        let temp_dir = TempDir::new().unwrap();
        let manager =
            FileManager::new(temp_dir.path(), false, 10_000_000, 100).unwrap();

        // Deleting a file that does not exist should not return an error.
        assert!(manager.delete_file(42).is_ok());
    }

    #[test]
    fn test_set_last_position() {
        let temp_dir = TempDir::new().unwrap();
        let manager =
            FileManager::new(temp_dir.path(), false, 10_000_000, 100).unwrap();

        let next = Lsn::new(3, 1024);
        let last = Lsn::new(2, 512);
        manager.set_last_position(next, last);

        assert_eq!(manager.get_next_available_lsn(), next);
        assert_eq!(manager.get_last_used_lsn(), last);
        assert_eq!(manager.get_current_file_num(), 3);
    }

    #[test]
    fn test_read_only_create_file_fails() {
        let temp_dir = TempDir::new().unwrap();
        // Create a writable manager first to avoid the lock conflict.
        {
            let _mgr =
                FileManager::new(temp_dir.path(), false, 10_000_000, 100)
                    .unwrap();
        } // lock released on drop

        // Read-only mode must not create files.
        let ro_mgr =
            FileManager::new(temp_dir.path(), true, 10_000_000, 100).unwrap();
        let result = ro_mgr.create_file(0);
        assert!(result.is_err());
    }

    #[test]
    fn test_first_and_last_file_num_empty() {
        let temp_dir = TempDir::new().unwrap();
        let manager =
            FileManager::new(temp_dir.path(), false, 10_000_000, 100).unwrap();

        assert_eq!(manager.get_first_file_num().unwrap(), None);
        assert_eq!(manager.get_last_file_num().unwrap(), None);
    }

    #[test]
    fn test_clear_cache() {
        let temp_dir = TempDir::new().unwrap();
        let manager =
            FileManager::new(temp_dir.path(), false, 10_000_000, 100).unwrap();

        manager.create_file(0).unwrap();
        // Clearing the cache should not panic or corrupt state.
        manager.clear_cache();

        // After clearing, get_file_handle must re-open the file.
        let handle = manager.get_file_handle(0).unwrap();
        assert_eq!(handle.file_num(), 0);
    }

    /// C-1 regression: parent directory must be fsynced after creating each
    /// new log file so the directory entry is durable across a power loss.
    ///
    /// This test verifies that `create_file_internal` completes without error
    /// (which confirms the dir-open + sync_all code path runs), and that
    /// the created file is visible in a directory listing performed after the
    /// call returns — i.e. the same state recovery would see after a restart.
    #[test]
    fn test_parent_dir_fsynced_after_file_create() {
        let temp_dir = TempDir::new().unwrap();
        let manager =
            FileManager::new(temp_dir.path(), false, 10_000_000, 100).unwrap();

        // Creating file 0 must succeed (includes parent-dir fsync).
        manager.create_file(0).unwrap();

        // The file must be present in the directory listing — the same check
        // recovery performs when scanning for log files to replay.
        let listed = manager.list_file_numbers().unwrap();
        assert_eq!(
            listed,
            vec![0],
            "file 0 must be visible in dir listing after create"
        );

        // Create a second file (flip) to exercise the path for file_num > 0.
        manager.flip_file().unwrap();
        let listed2 = manager.list_file_numbers().unwrap();
        assert!(listed2.contains(&1), "file 1 must be visible after flip");
    }

    // ── Write Queue (JE LogEndFileDescriptor) ───────────────────────────

    /// Header bytes so the created file has a valid header; the write queue
    /// operates on data written after the header.
    fn wq_manager() -> (TempDir, FileManager) {
        let dir = TempDir::new().unwrap();
        let m = FileManager::new(dir.path(), false, 10_000_000, 100).unwrap();
        m.configure_write_queue(true, 1 << 16); // 64 KiB queue
        m.create_file(0).unwrap();
        (dir, m)
    }

    /// With the fsync-lock held by a simulated in-flight fsync, a
    /// `write_to_file` ENQUEUES rather than blocking, does NO disk I/O, and a
    /// subsequent read still returns the queued bytes (JE checkWriteCache).
    /// After `sync_log_end` (JE force: dequeue + fdatasync) the bytes are on
    /// disk and readable directly.
    #[test]
    fn wq_enqueue_when_fsync_held_then_dequeue_on_sync() {
        let (_dir, m) = wq_manager();
        let off = first_log_entry_offset() as u64;
        let data = b"hello-write-queue";

        // Simulate an in-flight fsync by holding the fsync-lock.
        {
            let _held = m.fsync_lock.lock();
            // Write while the lock is held → must enqueue (no I/O).
            m.write_buffer_to_file(0, data, off).unwrap();
            // Nothing on disk yet: the file is still just the header.
            let mut disk = vec![0u8; data.len()];
            let handle = m.get_file_handle(0).unwrap();
            let n = handle.acquire().unwrap().read_at(off, &mut disk).unwrap();
            assert_eq!(n, 0, "enqueued bytes must NOT be on disk yet");
            // But a read THROUGH the FileManager sees the queued bytes
            // (checkWriteCache overlay).
            let mut via = vec![0u8; data.len()];
            let n2 = m.read_from_file(0, off, &mut via).unwrap();
            assert_eq!(n2, data.len());
            assert_eq!(&via, data, "read must overlay the write queue");
        }

        // fsync-lock released. sync_log_end drains the queue then fdatasyncs.
        m.sync_log_end().unwrap();
        let mut disk = vec![0u8; data.len()];
        let handle = m.get_file_handle(0).unwrap();
        let n = handle.acquire().unwrap().read_at(off, &mut disk).unwrap();
        assert_eq!(n, data.len(), "dequeue must have written to disk");
        assert_eq!(&disk, data);
    }

    /// When the fsync-lock is FREE, `write_to_file` takes it and writes
    /// directly — no queueing (JE writeToFile: tryLock succeeds).
    #[test]
    fn wq_direct_write_when_fsync_free() {
        let (_dir, m) = wq_manager();
        let off = first_log_entry_offset() as u64;
        let data = b"direct";
        m.write_buffer_to_file(0, data, off).unwrap();
        // No queued bytes remain.
        assert_eq!(m.write_queue.lock().as_ref().unwrap().pos, 0);
        let mut disk = vec![0u8; data.len()];
        let handle = m.get_file_handle(0).unwrap();
        let n = handle.acquire().unwrap().read_at(off, &mut disk).unwrap();
        assert_eq!(n, data.len());
        assert_eq!(&disk, data);
    }

    /// A queue overflow (write larger than the queue) falls back to a direct
    /// write (JE enqueueWrite returns false -> writeToFile does the direct
    /// write). Driven from a helper thread so the fsync-lock can be held by
    /// the test thread to force the enqueue attempt, then released so the
    /// writer's blocking fallback acquire succeeds.
    #[test]
    fn wq_overflow_falls_back_to_direct_write() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};
        let dir = TempDir::new().unwrap();
        let m = Arc::new(
            FileManager::new(dir.path(), false, 10_000_000, 100).unwrap(),
        );
        m.configure_write_queue(true, 1 << 12); // tiny 4 KiB queue
        m.create_file(0).unwrap();
        let off = first_log_entry_offset() as u64;
        let big = vec![0xABu8; (1 << 12) + 100]; // larger than the queue

        // Hold the fsync-lock so the writer's try_lock fails and it attempts
        // to enqueue; the enqueue overflows (data > queue) and the writer
        // falls through to the BLOCKING fsync-lock acquire for a direct write.
        let holding = Arc::new(AtomicBool::new(true));
        let m2 = Arc::clone(&m);
        let big2 = big.clone();
        let writer = std::thread::spawn(move || {
            m2.write_buffer_to_file(0, &big2, off).unwrap();
        });
        // Give the writer time to attempt enqueue + overflow, then release.
        {
            let held = m.fsync_lock.lock();
            std::thread::sleep(std::time::Duration::from_millis(30));
            drop(held);
            holding.store(false, Ordering::Relaxed);
        }
        writer.join().unwrap();
        assert!(!holding.load(Ordering::Relaxed));

        // The oversized write landed directly on disk.
        let mut disk = vec![0u8; big.len()];
        let handle = m.get_file_handle(0).unwrap();
        let n = handle.acquire().unwrap().read_at(off, &mut disk).unwrap();
        assert_eq!(n, big.len());
        assert_eq!(disk, big);
        // Overflow counter bumped.
        assert!(m.n_write_queue_overflow.load(Ordering::Relaxed) >= 1);
    }

    /// Non-contiguous queued write is a fatal LOG_INTEGRITY error (JE
    /// enqueueWrite1: `curPos + qwStartingOffset != destOffset`).
    #[test]
    fn wq_non_contiguous_write_is_integrity_error() {
        let (_dir, m) = wq_manager();
        let off = first_log_entry_offset() as u64;
        {
            let _held = m.fsync_lock.lock();
            m.write_buffer_to_file(0, b"aaaa", off).unwrap();
            // Next queued write must be at off+4; a gap is an integrity error.
            let err = m.write_buffer_to_file(0, b"bbbb", off + 100);
            assert!(err.is_err(), "non-contiguous queued write must fail");
        }
    }

    /// A read of the current end-of-log that starts exactly at the queue's
    /// starting offset returns the queued bytes even when the disk copy is
    /// short (JE checkWriteCache: bytes only in the queue).
    #[test]
    fn wq_read_overlay_extends_short_disk_read() {
        let (_dir, m) = wq_manager();
        let off = first_log_entry_offset() as u64;
        let data = b"queued-only-bytes";
        {
            let _held = m.fsync_lock.lock();
            m.write_buffer_to_file(0, data, off).unwrap();
            let mut buf = vec![0u8; data.len()];
            // random-read path must also overlay.
            let n = m.read_from_file_random(0, off, &mut buf).unwrap();
            assert_eq!(n, data.len());
            assert_eq!(&buf, data);
        }
    }
}
