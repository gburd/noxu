//! File manager for log files.
//!
//!
//! The FileManager presents the abstraction of one contiguous log file,
//! managing the actual on-disk log files, file handles, and LSN allocation.

use crate::error::{LogError, Result};
use crate::file_handle::FileHandle;
use crate::file_header::{FILE_HEADER_SIZE, FileHeader, LOG_VERSION};
use memmap2::Mmap;
use noxu_latch::ExclusiveLatch;
use noxu_util::lsn::Lsn;
use noxu_sync::{Mutex, RwLock};
use hashbrown::HashMap;
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

/// File mode for opening log files.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileMode {
    /// Read-only access.
    ReadOnly,
    /// Read-write access.
    ReadWrite,
}

/// Returns the offset of the first log entry in a file (after the header).
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
        let lock_file =
            OpenOptions::new().create(true).truncate(false).write(true).open(&lock_path)?;

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
    pub fn max_file_size(&self) -> u64 {
        self.max_file_size
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

    /// Gets a file handle for the given file number.
    ///
    /// Checks the LRU cache first`).
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
    fn read_and_validate_header(
        &self,
        file: &File,
        file_num: u32,
    ) -> Result<u32> {
        #[cfg(unix)]
        use std::os::unix::fs::FileExt as PosixFileExt;
        #[cfg(windows)]
        use std::os::windows::fs::FileExt as PosixFileExt;

        // Read the header bytes
        let mut header_buf = vec![0u8; FILE_HEADER_SIZE];
        file.read_exact_at(&mut header_buf, 0)?;

        // Parse header
        let mut cursor = std::io::Cursor::new(header_buf);
        let header = FileHeader::read_from(&mut cursor)?;

        // Validate
        header.validate(file_num)
    }

    /// Creates a new log file with the given file number.
    ///
    /// Writes the file header with a link to the previous file.
    pub fn create_file(&self, file_num: u32) -> Result<Arc<FileHandle>> {
        let _guard = self.file_latch.acquire()
            .map_err(|e| LogError::LatchTimeout(e.to_string()))?;
        self.create_file_internal(file_num)
    }

    /// Flips to the next log file.
    ///
    /// Called when the current file reaches its maximum size.
    pub fn flip_file(&self) -> Result<u32> {
        let _guard = self.file_latch.acquire()
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
    pub fn write_buffer(
        &self,
        data: &[u8],
        file_offset: u64,
    ) -> Result<u32> {
        if self.read_only {
            return Err(LogError::WriteFailed(
                "Cannot write in read-only mode".to_string(),
            ));
        }

        let file_num = self.current_file_num.load(Ordering::Acquire);

        // Obtain (or create) the file handle for the current file.
        // If no log file exists yet, create the first one.
        let handle = if self
            .file_path(file_num)
            .exists()
        {
            self.get_file_handle(file_num)?
        } else {
            self.create_file(file_num)?
        };

        // Write the data at the specified offset.
        {
            let mut guard = handle.acquire()?;
            guard.write_at(file_offset, data)?;
        }

        // Do NOT update next_available_lsn or last_used_lsn here.
        // Those are managed exclusively by set_last_position() under the LWL
        // in LogManager::log().  flush_dirty_buffers() writes pool buffers in
        // pool-index order (not temporal/LSN order); unconditionally storing
        // file_offset+len here would set next_available_lsn backward whenever
        // an older buffer is written after a newer one (ring-wrapped pool).

        // Track sequential-write stats.
        self.n_sequential_writes.fetch_add(1, Ordering::Relaxed);
        self.n_sequential_write_bytes.fetch_add(data.len() as u64, Ordering::Relaxed);

        // Check whether we need to flip to a new file.
        let path = self.file_path(file_num);
        let file_len = path.metadata().map(|m| m.len()).unwrap_or(0);
        if file_len >= self.max_file_size {
            self.flip_file()?;
        }

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
        self.n_random_reads.fetch_add(1, Ordering::Relaxed);
        self.n_random_read_bytes.fetch_add(n as u64, Ordering::Relaxed);
        Ok(n)
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
        let path = self.file_path(file_num);
        let file = File::open(&path).map_err(|e| {
            LogError::FileNotFound(format!(
                "Cannot open {:?} for mmap: {}",
                path, e
            ))
        })?;
        // SAFETY: We only mmap files that are complete (not the current
        // active write file) during recovery, so the underlying bytes do
        // not change while the mapping is alive.
        let mmap = unsafe { Mmap::map(&file) }.map_err(|e| {
            LogError::Io(std::io::Error::other(format!("mmap {:?}: {}", path, e)))
        })?;
        Ok(mmap)
    }

    /// Returns current I/O statistics for this FileManager.
    pub fn get_io_stats(&self) -> FileManagerIoStats {
        FileManagerIoStats {
            n_file_opens: self.n_file_opens.load(Ordering::Relaxed),
            n_sequential_reads: self.n_sequential_reads.load(Ordering::Relaxed),
            n_sequential_read_bytes: self.n_sequential_read_bytes.load(Ordering::Relaxed),
            n_sequential_writes: self.n_sequential_writes.load(Ordering::Relaxed),
            n_sequential_write_bytes: self.n_sequential_write_bytes.load(Ordering::Relaxed),
            n_random_reads: self.n_random_reads.load(Ordering::Relaxed),
            n_random_read_bytes: self.n_random_read_bytes.load(Ordering::Relaxed),
        }
    }

    /// Fsyncs the current log file to stable storage.
    ///
    /// 
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

        let handle = self.get_file_handle(file_num)?;
        let mut guard = handle.acquire()?;
        // Use fdatasync (sync_data) — only log data must be durable here,
        // not file metadata.  uses FileChannel.force(false) for this.
        guard.sync_data()?;
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
/// Mirrors JE FILEMGR_FILE_OPENS, FILEMGR_SEQUENTIAL_READS/WRITES,
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
        let result =
            FileManager::new("/tmp/does_not_exist_noxu_xyz", false, 10_000_000, 100);
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
}
