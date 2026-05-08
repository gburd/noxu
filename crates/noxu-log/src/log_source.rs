//! Abstraction for reading log data.
//!
//! Log source abstractions for reading log data.
//!
//! LogSource provides an abstraction for reading data from different sources
//! (files, buffers, etc.) in a uniform way.

use crate::error::Result;
use crate::file_handle::{FileHandle, FileHandleGuard};
use std::sync::Arc;

/// Trait for reading log data from various sources.
pub trait LogSource {
    /// Reads data from the given offset into the buffer.
    ///
    /// # Arguments
    ///
    /// * `offset` - Offset within the source to read from
    /// * `buf` - Buffer to read into
    ///
    /// # Returns
    ///
    /// Number of bytes read.
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize>;

    /// Returns the log version for entries from this source.
    fn log_version(&self) -> u32;

    /// Releases resources associated with the source.
    fn release(&mut self) -> Result<()>;
}

/// Log source backed by a file handle.
///
/// This is the most common log source, reading from a RandomAccessFile
/// through a FileHandle with latch protection.
pub struct FileLogSource {
    /// Guard providing access to the file.
    guard: Option<FileHandleGuard<'static>>,
    /// File handle (kept alive for the lifetime of the guard).
    _handle: Arc<FileHandle>,
    /// File number for this source.
    file_num: u32,
    /// Log version of the file.
    log_version: u32,
}

impl FileLogSource {
    /// Creates a new file log source from a file handle.
    ///
    /// The handle must be acquired (latched) before creating the source.
    pub fn new(handle: Arc<FileHandle>) -> Result<Self> {
        let file_num = handle.file_num();
        let log_version = handle.log_version();

        // Acquire the latch on the handle
        // SAFETY: We extend the lifetime of the guard to 'static because we keep
        // the Arc<FileHandle> alive for as long as the guard exists.
        let guard = unsafe {
            let guard = handle.acquire();
            std::mem::transmute::<FileHandleGuard<'_>, FileHandleGuard<'static>>(
                guard,
            )
        };

        Ok(FileLogSource {
            guard: Some(guard),
            _handle: handle,
            file_num,
            log_version,
        })
    }

    /// Returns the file number for this source.
    pub fn file_num(&self) -> u32 {
        self.file_num
    }
}

impl LogSource for FileLogSource {
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        if let Some(guard) = &mut self.guard {
            guard.read_at(offset, buf)
        } else {
            Err(crate::error::LogError::Internal(
                "FileLogSource already released".to_string(),
            ))
        }
    }

    fn log_version(&self) -> u32 {
        self.log_version
    }

    fn release(&mut self) -> Result<()> {
        // Drop the guard, which releases the latch
        self.guard = None;
        Ok(())
    }
}

impl Drop for FileLogSource {
    fn drop(&mut self) {
        let _ = self.release();
    }
}

/// Log source backed by an in-memory buffer.
///
/// Used for reading log entries that are still in the write buffer
/// and haven't been flushed to disk yet.
pub struct BufferLogSource {
    /// The buffer containing log data.
    buffer: Vec<u8>,
    /// Log version.
    log_version: u32,
}

impl BufferLogSource {
    /// Creates a new buffer log source.
    pub fn new(buffer: Vec<u8>, log_version: u32) -> Self {
        BufferLogSource { buffer, log_version }
    }
}

impl LogSource for BufferLogSource {
    fn read_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<usize> {
        let offset = offset as usize;
        if offset >= self.buffer.len() {
            return Ok(0);
        }

        let available = self.buffer.len() - offset;
        let to_read = buf.len().min(available);
        buf[..to_read].copy_from_slice(&self.buffer[offset..offset + to_read]);
        Ok(to_read)
    }

    fn log_version(&self) -> u32 {
        self.log_version
    }

    fn release(&mut self) -> Result<()> {
        // Nothing to release for buffer source
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_log_source() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        let mut source = BufferLogSource::new(data, 1);

        let mut buf = vec![0u8; 5];
        let n = source.read_at(2, &mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buf, &[3, 4, 5, 6, 7]);

        // Read past end
        let mut buf = vec![0u8; 10];
        let n = source.read_at(8, &mut buf).unwrap();
        assert_eq!(n, 2);
        assert_eq!(&buf[..2], &[9, 10]);
    }

    #[test]
    fn test_buffer_log_source_read_at_start() {
        let data = vec![10u8, 20, 30, 40, 50];
        let mut source = BufferLogSource::new(data, 2);
        let mut buf = vec![0u8; 5];
        let n = source.read_at(0, &mut buf).unwrap();
        assert_eq!(n, 5);
        assert_eq!(buf, vec![10, 20, 30, 40, 50]);
    }

    #[test]
    fn test_buffer_log_source_read_at_exact_end_offset() {
        let data = vec![1u8, 2, 3];
        let mut source = BufferLogSource::new(data, 1);
        let mut buf = vec![0u8; 4];
        // offset == len: nothing available
        let n = source.read_at(3, &mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn test_buffer_log_source_read_past_end() {
        let data = vec![1u8, 2, 3];
        let mut source = BufferLogSource::new(data, 1);
        let mut buf = vec![0u8; 4];
        let n = source.read_at(100, &mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn test_buffer_log_source_log_version() {
        let source = BufferLogSource::new(vec![], 42);
        assert_eq!(source.log_version(), 42);
    }

    #[test]
    fn test_buffer_log_source_release() {
        let mut source = BufferLogSource::new(vec![1, 2, 3], 1);
        assert!(source.release().is_ok());
        // After release, read still works (buffer is not destroyed)
        let mut buf = vec![0u8; 1];
        let n = source.read_at(0, &mut buf).unwrap();
        assert_eq!(n, 1);
    }

    #[test]
    fn test_buffer_log_source_empty_buffer() {
        let mut source = BufferLogSource::new(vec![], 1);
        let mut buf = vec![0u8; 4];
        let n = source.read_at(0, &mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn test_buffer_log_source_partial_read() {
        // Buffer shorter than requested buf length
        let data = vec![0xAAu8, 0xBB, 0xCC];
        let mut source = BufferLogSource::new(data, 1);
        let mut buf = vec![0u8; 10];
        let n = source.read_at(1, &mut buf).unwrap();
        assert_eq!(n, 2);
        assert_eq!(buf[0], 0xBB);
        assert_eq!(buf[1], 0xCC);
    }

    #[test]
    fn test_buffer_log_source_single_byte_reads() {
        let data = vec![10u8, 20, 30];
        let mut source = BufferLogSource::new(data, 1);
        for (i, &expected) in [10u8, 20, 30].iter().enumerate() {
            let mut buf = vec![0u8; 1];
            let n = source.read_at(i as u64, &mut buf).unwrap();
            assert_eq!(n, 1);
            assert_eq!(buf[0], expected);
        }
    }

    #[test]
    fn test_buffer_log_source_zero_length_read() {
        let data = vec![1u8, 2, 3];
        let mut source = BufferLogSource::new(data, 1);
        let mut buf = vec![];
        let n = source.read_at(0, &mut buf).unwrap();
        assert_eq!(n, 0);
    }

    // --- FileLogSource tests ---

    #[test]
    fn test_file_log_source_read_after_release_returns_error() {
        use crate::file_handle::FileHandle;
        use std::fs::File;
        use std::io::Write;
        use tempfile::NamedTempFile;

        // Create a temp file with some data.
        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(b"hello world").unwrap();
        temp_file.flush().unwrap();

        let file = File::open(temp_file.path()).unwrap();
        let mut handle = FileHandle::new(0);
        handle.init(file, 1);

        let arc_handle = Arc::new(handle);
        let mut source = FileLogSource::new(arc_handle).unwrap();

        // Release drops the guard.
        source.release().unwrap();

        // read_at after release should hit the else branch → error.
        let mut buf = vec![0u8; 4];
        let result = source.read_at(0, &mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_file_log_source_read_at_valid() {
        use crate::file_handle::FileHandle;
        use std::fs::File;
        use std::io::Write;
        use tempfile::NamedTempFile;

        let mut temp_file = NamedTempFile::new().unwrap();
        temp_file.write_all(b"ABCDEFGH").unwrap();
        temp_file.flush().unwrap();

        let file = File::open(temp_file.path()).unwrap();
        let mut handle = FileHandle::new(5);
        handle.init(file, 3);

        let arc_handle = Arc::new(handle);
        let mut source = FileLogSource::new(arc_handle).unwrap();

        assert_eq!(source.file_num(), 5);
        assert_eq!(source.log_version(), 3);

        let mut buf = vec![0u8; 4];
        let n = source.read_at(2, &mut buf).unwrap();
        assert_eq!(n, 4);
        assert_eq!(&buf, b"CDEF");
    }

    #[test]
    fn test_file_log_source_release_twice_is_idempotent() {
        use crate::file_handle::FileHandle;
        use std::fs::File;
        use tempfile::NamedTempFile;

        let temp_file = NamedTempFile::new().unwrap();
        let file = File::open(temp_file.path()).unwrap();
        let mut handle = FileHandle::new(0);
        handle.init(file, 1);

        let arc_handle = Arc::new(handle);
        let mut source = FileLogSource::new(arc_handle).unwrap();

        assert!(source.release().is_ok());
        // Second release: guard is already None, should succeed without panic.
        assert!(source.release().is_ok());
    }

    #[test]
    fn test_file_log_source_log_version() {
        use crate::file_handle::FileHandle;
        use std::fs::File;
        use tempfile::NamedTempFile;

        let temp_file = NamedTempFile::new().unwrap();
        let file = File::open(temp_file.path()).unwrap();
        let mut handle = FileHandle::new(7);
        handle.init(file, 42);

        let arc_handle = Arc::new(handle);
        let source = FileLogSource::new(arc_handle).unwrap();
        assert_eq!(source.log_version(), 42);
    }
}
