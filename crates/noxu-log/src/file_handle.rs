//! File handle with latch protection.
//!
//! Port of `com.sleepycat.je.log.FileHandle`.
//!
//! A FileHandle wraps a file descriptor with a latch to ensure exclusive
//! access during I/O operations.

use crate::error::{LogError, Result};
use noxu_latch::{ExclusiveLatch, ExclusiveLatchGuard};
use noxu_sync::Mutex;
use std::fs::File;
use std::sync::Arc;

// Positional I/O: maps to pread64 / pwrite64 on Linux (single syscall, no
// seek needed).  Port of Java's FileChannel.read(buf, position) /
// FileChannel.write(buf, position) which the JVM lowers to pread64/pwrite64.
#[cfg(unix)]
use std::os::unix::fs::FileExt as PosFileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt as PosFileExt;

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
    /// The latch is automatically released when the guard is dropped.
    pub fn acquire(&self) -> FileHandleGuard<'_> {
        let _latch_guard = self.latch.acquire();
        FileHandleGuard { handle: self, _latch_guard }
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
    /// Port of Java `FileChannel.read(ByteBuffer, position)` which the JVM
    /// lowers to `pread64` on Linux.
    pub fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let file_guard = self.handle.file.lock();
        let file = file_guard.as_ref().ok_or_else(|| {
            LogError::Internal("FileHandle not initialized".to_string())
        })?;
        Ok(PosFileExt::read_at(file, buf, offset)?)
    }

    /// Reads exactly `buf.len()` bytes from the file at the given offset.
    ///
    /// Uses `pread64` in a retry loop (port of Java `FileChannel.read` loop).
    /// Returns an error if fewer bytes are available.
    pub fn read_exact_at(
        &mut self,
        offset: u64,
        buf: &mut [u8],
    ) -> Result<()> {
        let file_guard = self.handle.file.lock();
        let file = file_guard.as_ref().ok_or_else(|| {
            LogError::Internal("FileHandle not initialized".to_string())
        })?;
        PosFileExt::read_exact_at(file, buf, offset)?;
        Ok(())
    }

    /// Writes data to the file at the given offset.
    ///
    /// Uses `pwrite64` (one syscall) instead of `lseek + write` (two syscalls).
    /// Port of JE `FileChannel.write(ByteBuffer, position)` which the JVM
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
        PosFileExt::write_all_at(file, buf, offset)?;
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
        file.sync_all()?;
        Ok(())
    }

    /// Syncs only the file data to disk (fdatasync).
    ///
    /// Faster than `sync()` because it does not flush file metadata (mtime
    /// etc.).  JE uses `FileChannel.force(false)` (= fdatasync) for all
    /// log-data writes and `force(true)` (= fsync) only for file-header writes.
    ///
    /// Port of `FileManager.syncLogEnd()` / `FileChannel.force(false)`.
    pub fn sync_data(&mut self) -> Result<()> {
        let file_guard = self.handle.file.lock();
        let file = file_guard.as_ref().ok_or_else(|| {
            LogError::Internal("FileHandle not initialized".to_string())
        })?;
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
            let mut guard = handle.acquire();
            guard.write_at(0, b"test data").unwrap();
            guard.sync().unwrap();
        }

        {
            let mut guard = handle.acquire();
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
        let guard = handle.acquire();
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
            let mut guard = handle.acquire();
            guard.write_at(0, b"hello").unwrap();
        }
        {
            let mut guard = handle.acquire();
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
            let mut guard = handle.acquire();
            assert!(guard.is_empty().unwrap());
            assert_eq!(guard.len().unwrap(), 0);
            guard.write_at(0, b"abc").unwrap();
        }
        {
            let mut guard = handle.acquire();
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
            let mut guard = handle.acquire();
            guard.write_at(0, b"hello world").unwrap();
        }
        {
            let mut guard = handle.acquire();
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
            let mut guard = handle.acquire();
            guard.write_at(0, b"ABCDEF").unwrap();
        }
        {
            let mut guard = handle.acquire();
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
            let mut guard = handle.acquire();
            guard.write_at(0, b"XXXXXXXX").unwrap();
            guard.write_at(2, b"AB").unwrap();
        }
        {
            let mut guard = handle.acquire();
            let mut buf = vec![0u8; 8];
            guard.read_exact_at(0, &mut buf).unwrap();
            assert_eq!(&buf[2..4], b"AB");
        }
    }
}
