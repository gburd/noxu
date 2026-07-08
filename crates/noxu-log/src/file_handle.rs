//! File handle with latch protection.
//!
//!
//! A FileHandle wraps a file descriptor with a latch to ensure exclusive
//! access during I/O operations.

use crate::error::{LogError, Result};
use noxu_latch::{ExclusiveLatch, ExclusiveLatchGuard};
use noxu_sync::Mutex;
use std::fs::File;
use std::sync::Arc;

use crate::posio;

/// Test-only fdatasync probe (bounded-fsync-pipeline durability oracle seam).
///
/// Two process-global atomics, both `0` in production and read with a single
/// relaxed load on the fsync path — so this is free unless a test arms it.
/// It lets an integration test (a) SLOW each fdatasync deterministically
/// (widening the concurrent-leader window the pipeline durability hole would
/// live in) and (b) record the highest byte-EOF any COMPLETED fdatasync has
/// made durable, which the test compares against every returned durable
/// watermark.  Nothing outside tests ever arms it.
pub mod fsync_probe {
    use std::sync::atomic::{AtomicU64, Ordering};

    /// Microseconds to sleep at the START of each fdatasync (0 = disabled).
    pub(super) static DELAY_US: AtomicU64 = AtomicU64::new(0);
    /// Highest contiguous on-disk EOF (u64 LSN) a COMPLETED fdatasync covered.
    /// A committer that returns durable at LSN `L` must satisfy
    /// `L <= SYNCED_EOF` at the moment it returns (the oracle invariant).
    pub static SYNCED_EOF: AtomicU64 = AtomicU64::new(0);
    /// Number of fdatasyncs that have COMPLETED (armed only).
    pub static COMPLETED: AtomicU64 = AtomicU64::new(0);
    /// When non-zero, the probe is armed.
    pub(super) static ARMED: AtomicU64 = AtomicU64::new(0);

    /// Arm the probe: sleep `delay_us` at the start of each fdatasync and
    /// track the completed-fdatasync EOF watermark.  Test-only.
    pub fn arm(delay_us: u64) {
        SYNCED_EOF.store(0, Ordering::SeqCst);
        COMPLETED.store(0, Ordering::SeqCst);
        DELAY_US.store(delay_us, Ordering::SeqCst);
        ARMED.store(1, Ordering::SeqCst);
    }

    /// Disarm the probe (test cleanup).
    pub fn disarm() {
        ARMED.store(0, Ordering::SeqCst);
        DELAY_US.store(0, Ordering::SeqCst);
    }

    /// Whether the probe is armed.
    pub(super) fn is_armed() -> bool {
        ARMED.load(Ordering::Relaxed) != 0
    }
}

/// A file handle with latch protection for thread-safe I/O.
///
/// The handle holds a file descriptor and an exclusive latch.
/// All I/O operations must be performed while holding the latch.
pub struct FileHandle {
    /// The underlying file (wrapped in Mutex for interior mutability).
    file: Mutex<Option<File>>,
    /// Latch protecting access to the file.
    latch: Arc<ExclusiveLatch>,
    /// Log version of this file.
    log_version: u32,
    /// File number this handle represents.
    file_num: u32,
}

impl FileHandle {
    /// Creates a new uninitialized file handle.
    ///
    /// The file must be initialized via `init()` before use.
    pub fn new(file_num: u32) -> Self {
        let latch =
            Arc::new(ExclusiveLatch::named(format!("file_{:08x}", file_num)));

        FileHandle { file: Mutex::new(None), latch, log_version: 0, file_num }
    }

    /// Initializes the handle with an open file and log version.
    pub fn init(&mut self, file: File, log_version: u32) {
        let mut f = self.file.lock();
        assert!(f.is_none(), "FileHandle already initialized");
        *f = Some(file);
        self.log_version = log_version;
    }

    /// Returns the file number.
    pub fn file_num(&self) -> u32 {
        self.file_num
    }

    /// Returns the log version.
    pub fn log_version(&self) -> u32 {
        self.log_version
    }

    /// Returns true if the file is initialized.
    pub fn is_initialized(&self) -> bool {
        self.file.lock().is_some()
    }

    /// Acquires the latch and returns a guard that provides access to the file.
    ///
    /// Returns `Ok(guard)` on success, or `Err(LogError::LatchTimeout)` if the
    /// latch acquisition times out. The latch is released when the guard drops.
    pub fn acquire(&self) -> Result<FileHandleGuard<'_>> {
        let _latch_guard = self
            .latch
            .acquire()
            .map_err(|e| LogError::LatchTimeout(e.to_string()))?;
        Ok(FileHandleGuard { handle: self, _latch_guard })
    }

    /// fdatasync the file WITHOUT holding the exclusive write latch
    /// (JE `FileManager` separate `fsyncFileSynchronizer` + Write Queue).
    ///
    /// The write latch (`acquire`) serialises pwrites; holding it across the
    /// fdatasync would block every concurrent committer's pwrite for the
    /// ~60-100us of the sync, strictly serialising the write/fsync pipeline
    /// and capping fsync THROUGHPUT far below the device (the root cause of
    /// the write-scaling gap vs JE — see write-perf-fix-FALSIFIED.md). JE
    /// decouples the two: fsync takes a SEPARATE synchronizer and blocked
    /// writes are queued, so pwrites proceed DURING an in-flight fsync.
    ///
    /// On Linux, `fdatasync(fd)` is safe concurrent with `pwrite(fd)` on the
    /// same descriptor (the kernel serialises internally); we only need the
    /// `file` Mutex briefly to borrow the `&File`, NOT the write latch.  With
    /// the bounded fsync pipeline (`LOG_FSYNC_MAX_LEADERS > 1`) up to N of
    /// these run concurrently against the same fd; each fdatasync flushes ALL
    /// of the fd's dirty page-cache pages, so any completed one makes durable
    /// every byte pwritten before it started (the pipeline's durability
    /// invariant — see `LogManager::flush_sync`).  With the default
    /// `max_leaders == 1` the FsyncManager still serialises leaders, so exactly
    /// one fdatasync runs at a time.
    pub fn sync_data_no_latch(&self) -> Result<()> {
        // Test-only durability-oracle probe (inactive in production: one
        // relaxed load).  Capture the on-disk EOF this fdatasync is ABOUT to
        // make durable BEFORE the sync, optionally sleep to widen the
        // concurrent-leader window, then — AFTER the sync completes — publish
        // that EOF as the highest durable offset via a monotonic max.  The
        // fdatasync makes durable exactly what was in the page cache when it
        // started, so a smaller-EOF sync completing after a larger-EOF sync
        // must NOT roll the durable watermark backwards — monotonic max.
        if fsync_probe::is_armed() {
            let pre_eof = {
                let g = self.file.lock();
                match g.as_ref() {
                    Some(f) => f
                        .metadata()
                        .map(|m| {
                            noxu_util::Lsn::new(self.file_num, m.len() as u32)
                                .as_u64()
                        })
                        .unwrap_or(0),
                    None => 0,
                }
            };
            let delay = fsync_probe::DELAY_US
                .load(std::sync::atomic::Ordering::Relaxed);
            if delay > 0 {
                std::thread::sleep(std::time::Duration::from_micros(delay));
            }
            {
                let file_guard = self.file.lock();
                let file = file_guard.as_ref().ok_or_else(|| {
                    LogError::Internal("FileHandle not initialized".to_string())
                })?;
                if crate::faultdisk::on_fsync() {
                    drop(file_guard);
                    crate::faultdisk::power_cut();
                } else {
                    file.sync_data()?;
                }
            }
            // Publish AFTER the sync completes: monotonic max so an
            // out-of-order (smaller-EOF) completion never regresses the
            // durable watermark the oracle checks against.
            let mut cur = fsync_probe::SYNCED_EOF
                .load(std::sync::atomic::Ordering::Relaxed);
            while cur < pre_eof {
                match fsync_probe::SYNCED_EOF.compare_exchange_weak(
                    cur,
                    pre_eof,
                    std::sync::atomic::Ordering::SeqCst,
                    std::sync::atomic::Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(o) => cur = o,
                }
            }
            fsync_probe::COMPLETED
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            return Ok(());
        }
        let file_guard = self.file.lock();
        let file = file_guard.as_ref().ok_or_else(|| {
            LogError::Internal("FileHandle not initialized".to_string())
        })?;
        // DST fault layer (inactive in production).
        if crate::faultdisk::on_fsync() {
            drop(file_guard);
            crate::faultdisk::power_cut();
        }
        file.sync_data()?;
        Ok(())
    }

    /// Attempts to acquire the latch without blocking.
    ///
    /// Returns `None` if the latch is currently held.
    pub fn try_acquire(&self) -> Option<FileHandleGuard<'_>> {
        self.latch
            .try_acquire()
            .map(|_latch_guard| FileHandleGuard { handle: self, _latch_guard })
    }

    /// Closes the file handle.
    ///
    /// This should only be called when the handle is no longer in use.
    pub fn close(&mut self) -> Result<()> {
        if let Some(file) = self.file.lock().take() {
            drop(file); // File is closed when dropped
        }
        Ok(())
    }
}

impl Drop for FileHandle {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

/// RAII guard providing access to the file while the latch is held.
pub struct FileHandleGuard<'a> {
    handle: &'a FileHandle,
    _latch_guard: ExclusiveLatchGuard<'a>,
}

impl<'a> FileHandleGuard<'a> {
    /// Reads data from the file at the given offset.
    ///
    /// # Arguments
    ///
    /// * `offset` - File offset to read from
    /// * `buf` - Buffer to read into
    ///
    /// # Returns
    ///
    /// Number of bytes read.
    /// Reads data from the file at the given offset.
    ///
    /// Uses `pread64` (one syscall) instead of `lseek + read` (two syscalls).
    /// The JVM
    /// lowers to `pread64` on Linux.
    pub fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let file_guard = self.handle.file.lock();
        let file = file_guard.as_ref().ok_or_else(|| {
            LogError::Internal("FileHandle not initialized".to_string())
        })?;
        Ok(posio::read_at(file, buf, offset)?)
    }

    /// Reads exactly `buf.len()` bytes from the file at the given offset.
    ///
    /// Uses `pread64` in a retry loop.
    /// Returns an error if fewer bytes are available.
    pub fn read_exact_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<()> {
        let file_guard = self.handle.file.lock();
        let file = file_guard.as_ref().ok_or_else(|| {
            LogError::Internal("FileHandle not initialized".to_string())
        })?;
        posio::read_exact_at(file, buf, offset)?;
        Ok(())
    }

    /// Writes data to the file at the given offset.
    ///
    /// Uses `pwrite64` (one syscall) instead of `lseek + write` (two syscalls).
    /// `FileChannel.write(ByteBuffer, position)` which the JVM
    /// lowers to `pwrite64` on Linux.  This eliminates half the syscalls on
    /// the hot write path and removes the need to serialise seek+write under
    /// the guard (pwrite64 is inherently positional and thread-safe).
    ///
    /// # Arguments
    ///
    /// * `offset` - File offset to write to (passed directly to pwrite64)
    /// * `buf` - Data to write
    ///
    /// # Returns
    ///
    /// Number of bytes written (always `buf.len()` on success).
    pub fn write_at(&mut self, offset: u64, buf: &[u8]) -> Result<usize> {
        let file_guard = self.handle.file.lock();
        let file = file_guard.as_ref().ok_or_else(|| {
            LogError::Internal("FileHandle not initialized".to_string())
        })?;
        posio::write_all_at(file, buf, offset)?;
        Ok(buf.len())
    }

    /// Syncs all file data and metadata to disk (fsync).
    ///
    /// Use this when the file's metadata (size, mtime) must also be durable —
    /// typically for file-header writes.  For log-data writes prefer
    /// `sync_data()` which is faster.
    pub fn sync(&mut self) -> Result<()> {
        let file_guard = self.handle.file.lock();
        let file = file_guard.as_ref().ok_or_else(|| {
            LogError::Internal("FileHandle not initialized".to_string())
        })?;
        // DST fault layer (inactive in production): a dropped fsync is
        // acknowledged without flushing, then power is cut so the unsynced
        // bytes vanish — modelling a disk that lies about durability.
        if crate::faultdisk::on_fsync() {
            drop(file_guard);
            crate::faultdisk::power_cut();
        }
        file.sync_all()?;
        Ok(())
    }

    /// Syncs only the file data to disk (fdatasync).
    ///
    /// Faster than `sync()` because it does not flush file metadata (mtime
    /// etc.).  uses `FileChannel.force(false)` (= fdatasync) for all
    /// log-data writes and `force(true)` (= fsync) only for file-header writes.
    ///
    /// / `FileChannel.force(false)`.
    pub fn sync_data(&mut self) -> Result<()> {
        let file_guard = self.handle.file.lock();
        let file = file_guard.as_ref().ok_or_else(|| {
            LogError::Internal("FileHandle not initialized".to_string())
        })?;
        // DST fault layer (inactive in production): see `sync` above.
        if crate::faultdisk::on_fsync() {
            drop(file_guard);
            crate::faultdisk::power_cut();
        }
        file.sync_data()?;
        Ok(())
    }

    /// Returns true if the file is empty.
    pub fn is_empty(&mut self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Returns the file length.
    pub fn len(&mut self) -> Result<u64> {
        let file_guard = self.handle.file.lock();
        let file = file_guard.as_ref().ok_or_else(|| {
            LogError::Internal("FileHandle not initialized".to_string())
        })?;
        Ok(file.metadata()?.len())
    }

    /// Truncates the file to the given length.
    pub fn truncate(&mut self, len: u64) -> Result<()> {
        let file_guard = self.handle.file.lock();
        let file = file_guard.as_ref().ok_or_else(|| {
            LogError::Internal("FileHandle not initialized".to_string())
        })?;
        file.set_len(len)?;
        Ok(())
    }

    /// Returns the file number.
    pub fn file_num(&self) -> u32 {
        self.handle.file_num()
    }

    /// Returns the log version.
    pub fn log_version(&self) -> u32 {
        self.handle.log_version()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_file_handle_basic() {
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(b"Hello, world!").unwrap();
        temp_file.flush().unwrap();

        let file = File::open(temp_file.path()).unwrap();

        let mut handle = FileHandle::new(0);
        handle.init(file, 1);

        assert_eq!(handle.file_num(), 0);
        assert_eq!(handle.log_version(), 1);
        assert!(handle.is_initialized());
    }

    #[test]
    fn test_file_handle_read_write() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = File::options()
            .read(true)
            .write(true)
            .open(temp_file.path())
            .unwrap();

        let mut handle = FileHandle::new(0);
        handle.init(file, 1);

        {
            let mut guard = handle.acquire().expect("acquire");
            guard.write_at(0, b"test data").unwrap();
            guard.sync().unwrap();
        }

        {
            let mut guard = handle.acquire().expect("acquire");
            let mut buf = vec![0u8; 9];
            let n = guard.read_at(0, &mut buf).unwrap();
            assert_eq!(n, 9);
            assert_eq!(&buf, b"test data");
        }
    }

    #[test]
    fn test_file_handle_new_uninitialized() {
        let handle = FileHandle::new(42);
        assert_eq!(handle.file_num(), 42);
        assert_eq!(handle.log_version(), 0);
        assert!(!handle.is_initialized());
    }

    #[test]
    fn test_file_handle_log_version_set_on_init() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = File::open(temp_file.path()).unwrap();
        let mut handle = FileHandle::new(7);
        handle.init(file, 5);
        assert_eq!(handle.log_version(), 5);
        assert!(handle.is_initialized());
    }

    #[test]
    fn test_file_handle_file_num_preserved() {
        let mut handle = FileHandle::new(0xFF);
        let temp_file = NamedTempFile::new().unwrap();
        let file = File::open(temp_file.path()).unwrap();
        handle.init(file, 1);
        assert_eq!(handle.file_num(), 0xFF);
    }

    #[test]
    fn test_file_handle_close_uninitialised() {
        let mut handle = FileHandle::new(0);
        // Closing a non-initialized handle should not error
        assert!(handle.close().is_ok());
        assert!(!handle.is_initialized());
    }

    #[test]
    fn test_file_handle_close_initialized() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = File::open(temp_file.path()).unwrap();
        let mut handle = FileHandle::new(1);
        handle.init(file, 1);
        assert!(handle.is_initialized());
        assert!(handle.close().is_ok());
        assert!(!handle.is_initialized());
    }

    #[test]
    fn test_file_handle_guard_file_num() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = File::open(temp_file.path()).unwrap();
        let mut handle = FileHandle::new(99);
        handle.init(file, 3);
        let guard = handle.acquire().expect("acquire");
        assert_eq!(guard.file_num(), 99);
        assert_eq!(guard.log_version(), 3);
    }

    #[test]
    fn test_file_handle_guard_read_exact() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = File::options()
            .read(true)
            .write(true)
            .open(temp_file.path())
            .unwrap();
        let mut handle = FileHandle::new(0);
        handle.init(file, 1);

        {
            let mut guard = handle.acquire().expect("acquire");
            guard.write_at(0, b"hello").unwrap();
        }
        {
            let mut guard = handle.acquire().expect("acquire");
            let mut buf = vec![0u8; 5];
            guard.read_exact_at(0, &mut buf).unwrap();
            assert_eq!(&buf, b"hello");
        }
    }

    #[test]
    fn test_file_handle_guard_len_and_is_empty() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = File::options()
            .read(true)
            .write(true)
            .open(temp_file.path())
            .unwrap();
        let mut handle = FileHandle::new(0);
        handle.init(file, 1);

        {
            let mut guard = handle.acquire().expect("acquire");
            assert!(guard.is_empty().unwrap());
            assert_eq!(guard.len().unwrap(), 0);
            guard.write_at(0, b"abc").unwrap();
        }
        {
            let mut guard = handle.acquire().expect("acquire");
            assert!(!guard.is_empty().unwrap());
            assert_eq!(guard.len().unwrap(), 3);
        }
    }

    #[test]
    fn test_file_handle_guard_truncate() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = File::options()
            .read(true)
            .write(true)
            .open(temp_file.path())
            .unwrap();
        let mut handle = FileHandle::new(0);
        handle.init(file, 1);

        {
            let mut guard = handle.acquire().expect("acquire");
            guard.write_at(0, b"hello world").unwrap();
        }
        {
            let mut guard = handle.acquire().expect("acquire");
            guard.truncate(5).unwrap();
            assert_eq!(guard.len().unwrap(), 5);
        }
    }

    #[test]
    fn test_file_handle_try_acquire() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = File::open(temp_file.path()).unwrap();
        let mut handle = FileHandle::new(0);
        handle.init(file, 1);

        let guard = handle.try_acquire();
        assert!(guard.is_some());
        // Guard released when dropped, then try_acquire succeeds again
        drop(guard);
        let guard2 = handle.try_acquire();
        assert!(guard2.is_some());
    }

    #[test]
    fn test_file_handle_read_at_offset() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = File::options()
            .read(true)
            .write(true)
            .open(temp_file.path())
            .unwrap();
        let mut handle = FileHandle::new(0);
        handle.init(file, 1);

        {
            let mut guard = handle.acquire().expect("acquire");
            guard.write_at(0, b"ABCDEF").unwrap();
        }
        {
            let mut guard = handle.acquire().expect("acquire");
            let mut buf = vec![0u8; 3];
            let n = guard.read_at(2, &mut buf).unwrap();
            assert_eq!(n, 3);
            assert_eq!(&buf, b"CDE");
        }
    }

    #[test]
    fn test_file_handle_write_at_offset() {
        let temp_file = NamedTempFile::new().unwrap();
        let file = File::options()
            .read(true)
            .write(true)
            .open(temp_file.path())
            .unwrap();
        let mut handle = FileHandle::new(0);
        handle.init(file, 1);

        {
            let mut guard = handle.acquire().expect("acquire");
            guard.write_at(0, b"XXXXXXXX").unwrap();
            guard.write_at(2, b"AB").unwrap();
        }
        {
            let mut guard = handle.acquire().expect("acquire");
            let mut buf = vec![0u8; 8];
            guard.read_exact_at(0, &mut buf).unwrap();
            assert_eq!(&buf[2..4], b"AB");
        }
    }
}
