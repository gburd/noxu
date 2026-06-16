//! Network restore for copying database files from another node.
//!
//! NetworkRestore
//! copies log files from a network peer to restore a node that has fallen
//! too far behind the replication stream. This is used when a replica
//! discovers an `InsufficientLogException`  -  its local log files are too
//! old for the feeder to supply a contiguous stream.

use std::io::{Read as IoRead, Write as IoWrite};
use std::net::TcpStream;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use noxu_sync::Mutex;

use crate::error::{RepError, Result};

/// Magic bytes sent at the start of every restore-request frame.
///
/// 4-byte little-endian value: `0x4E52_5354` ('N','R','S','T').
const RESTORE_MAGIC: u32 = 0x4E52_5354;

/// Configuration for a network restore operation.
///
/// Specifies the source node
/// to copy from and whether existing log files should be retained.
#[derive(Debug, Clone)]
pub struct NetworkRestoreConfig {
    /// Name of the source node to restore from.
    pub source_node: String,
    /// Hostname of the source node.
    pub source_host: String,
    /// Source node.
    pub source_port: u16,
    /// Whether to retain existing log files (rename rather than delete).
    pub retain_log_files: bool,
}

/// The current state of a network restore operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RestoreState {
    /// The restore has not yet started.
    NotStarted,
    /// The restore is currently transferring files.
    InProgress,
    /// The restore completed successfully.
    Completed,
    /// The restore failed.
    Failed,
}

/// Progress information for a network restore operation.
#[derive(Debug, Clone)]
pub struct RestoreProgress {
    /// Current state of the restore.
    pub state: RestoreState,
    /// Total bytes transferred so far.
    pub bytes_transferred: u64,
    /// Total files transferred so far.
    pub files_transferred: u32,
    /// Time elapsed since the restore started.
    pub elapsed: Duration,
}

/// A network restore operation that copies database files from a peer node.
///
/// Manages the lifecycle of a restore:
/// starting the transfer, tracking progress, and completing or failing.
pub struct NetworkRestore {
    /// Configuration for this restore.
    config: NetworkRestoreConfig,
    /// Current restore state.
    state: Mutex<RestoreState>,
    /// Progress tracking.
    progress: Mutex<RestoreProgress>,
    /// Local directory where restored log files are written.
    ///
    /// If `None`, files are written to the process's current directory.
    local_log_dir: Option<PathBuf>,
}

/// Validate a server-supplied filename before it is joined with the local
/// log directory.
///
/// The wire protocol carries arbitrary UTF-8 strings, so a malicious or
/// compromised peer can otherwise direct writes outside `local_log_dir` via
/// path traversal (`../../etc/passwd`), absolute paths
/// (`/etc/cron.d/evil`), embedded NULs, or hidden dotfiles
/// (`.bashrc`).  We reject any of those here and only allow plain
/// log-file basenames.
///
/// # Rejection rules
///
/// - empty string
/// - `.` or `..`
/// - any byte equal to `/`, `\\`, or `\0`
/// - leading `.` (hidden file)
fn validate_restore_filename(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(RepError::ProtocolError("unsafe filename: empty".into()));
    }
    if name == "." || name == ".." {
        return Err(RepError::ProtocolError(format!(
            "unsafe filename: {:?}",
            name
        )));
    }
    if name.starts_with('.') {
        return Err(RepError::ProtocolError(format!(
            "unsafe filename: hidden dotfile {:?}",
            name
        )));
    }
    for b in name.as_bytes() {
        match *b {
            b'/' | b'\\' => {
                return Err(RepError::ProtocolError(format!(
                    "unsafe filename: path separator in {:?}",
                    name
                )));
            }
            0 => {
                return Err(RepError::ProtocolError(format!(
                    "unsafe filename: null byte in {:?}",
                    name
                )));
            }
            _ => {}
        }
    }
    Ok(())
}

impl NetworkRestore {
    /// Create a new network restore with the given configuration.
    pub fn new(config: NetworkRestoreConfig) -> Self {
        Self {
            config,
            state: Mutex::new(RestoreState::NotStarted),
            progress: Mutex::new(RestoreProgress {
                state: RestoreState::NotStarted,
                bytes_transferred: 0,
                files_transferred: 0,
                elapsed: Duration::ZERO,
            }),
            local_log_dir: None,
        }
    }

    /// Set the local directory where restored `.ndb` files will be written.
    ///
    /// If not set, the current working directory is used.
    pub fn with_local_dir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.local_log_dir = Some(dir.into());
        self
    }

    /// Get the current restore state.
    pub fn get_state(&self) -> RestoreState {
        *self.state.lock()
    }

    /// Get a snapshot of the current progress.
    pub fn get_progress(&self) -> RestoreProgress {
        self.progress.lock().clone()
    }

    /// Get the restore configuration.
    pub fn get_config(&self) -> &NetworkRestoreConfig {
        &self.config
    }

    /// Execute a full network restore: connect to the source node, transfer
    /// all `.ndb` log files, and write them to the local log directory.
    ///
    /// # Wire protocol (simple restore protocol)
    ///
    /// ```text
    /// Client → Server: [magic: u32 LE]            (4 bytes)  "NRST"
    /// Server → Client: [file_count: u32 LE]        (4 bytes)
    /// For each file:
    ///   Server → Client: [name_len: u16 LE]        (2 bytes)
    ///                    [name: UTF-8 bytes]        (name_len bytes)
    ///                    [file_size: u64 LE]        (8 bytes)
    ///                    [data: file_size bytes]
    /// ```
    ///
    ///
    pub fn execute(&self) -> Result<()> {
        // Validate state: must be NotStarted.
        {
            let state = self.state.lock();
            if *state != RestoreState::NotStarted {
                return Err(RepError::NetworkRestoreError(format!(
                    "execute called in wrong state: {:?}",
                    *state
                )));
            }
        }

        // Transition to InProgress.
        self.start()?;

        let started_at = Instant::now();
        let addr =
            format!("{}:{}", self.config.source_host, self.config.source_port);

        // Connect to the source node.
        let mut stream = TcpStream::connect(&addr).map_err(|e| {
            RepError::NetworkRestoreError(format!(
                "cannot connect to source {}: {}",
                addr, e
            ))
        })?;

        // Set a generous read timeout so we don't hang forever on a dead peer.
        let _ = stream.set_read_timeout(Some(Duration::from_secs(120)));

        // Send the restore-request magic.
        stream.write_all(&RESTORE_MAGIC.to_le_bytes()).map_err(|e| {
            RepError::NetworkRestoreError(format!(
                "sending restore magic: {}",
                e
            ))
        })?;

        // Read the file count.
        let mut count_buf = [0u8; 4];
        stream.read_exact(&mut count_buf).map_err(|e| {
            RepError::NetworkRestoreError(format!("reading file count: {}", e))
        })?;
        let file_count = u32::from_le_bytes(count_buf);

        let log_dir = self.local_log_dir.clone().unwrap_or_else(|| {
            std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
        });

        let mut total_bytes: u64 = 0;
        let mut files_done: u32 = 0;

        for _ in 0..file_count {
            // Read filename length + name.
            let mut name_len_buf = [0u8; 2];
            stream.read_exact(&mut name_len_buf).map_err(|e| {
                RepError::NetworkRestoreError(format!(
                    "reading filename length: {}",
                    e
                ))
            })?;
            let name_len = u16::from_le_bytes(name_len_buf) as usize;

            let mut name_buf = vec![0u8; name_len];
            stream.read_exact(&mut name_buf).map_err(|e| {
                RepError::NetworkRestoreError(format!(
                    "reading filename: {}",
                    e
                ))
            })?;
            let filename = String::from_utf8(name_buf).map_err(|e| {
                RepError::NetworkRestoreError(format!(
                    "non-UTF8 filename: {}",
                    e
                ))
            })?;
            validate_restore_filename(&filename)?;

            // Read file size.
            let mut size_buf = [0u8; 8];
            stream.read_exact(&mut size_buf).map_err(|e| {
                RepError::NetworkRestoreError(format!(
                    "reading file size for '{}': {}",
                    filename, e
                ))
            })?;
            let file_size = u64::from_le_bytes(size_buf);

            // Determine destination path.
            // If `retain_log_files` is set and the file already exists,
            // rename the existing file before writing the new one.
            let dest_path = log_dir.join(&filename);
            if self.config.retain_log_files && dest_path.exists() {
                let backup = log_dir.join(format!("{}.bak", filename));
                let _ = std::fs::rename(&dest_path, &backup);
            }

            // Stream file bytes directly to disk in 64 KiB chunks.
            let mut out = std::fs::File::create(&dest_path).map_err(|e| {
                RepError::NetworkRestoreError(format!(
                    "creating '{}': {}",
                    dest_path.display(),
                    e
                ))
            })?;

            let mut remaining = file_size;
            let mut chunk = vec![0u8; 65536];
            while remaining > 0 {
                let to_read = (remaining as usize).min(chunk.len());
                stream.read_exact(&mut chunk[..to_read]).map_err(|e| {
                    RepError::NetworkRestoreError(format!(
                        "reading data for '{}': {}",
                        filename, e
                    ))
                })?;
                out.write_all(&chunk[..to_read]).map_err(|e| {
                    RepError::NetworkRestoreError(format!(
                        "writing '{}': {}",
                        dest_path.display(),
                        e
                    ))
                })?;
                remaining -= to_read as u64;
                total_bytes += to_read as u64;
            }

            files_done += 1;
            self.update_progress(total_bytes, files_done);
            self.update_elapsed(started_at.elapsed());

            log::debug!(
                "NetworkRestore: received '{}' ({} bytes)",
                filename,
                file_size
            );
        }

        self.update_elapsed(started_at.elapsed());
        self.complete()?;

        log::info!(
            "NetworkRestore from {}: {} file(s), {} bytes transferred in {:?}",
            addr,
            files_done,
            total_bytes,
            started_at.elapsed(),
        );

        Ok(())
    }

    /// Execute a network restore against a peer's `RESTORE` service
    /// running on the `TcpServiceDispatcher`.
    ///
    /// Closes findings F2 / F4 of the 2026 review.
    ///
    /// `execute()` (above) connects raw TCP and writes `RESTORE_MAGIC`,
    /// which works only against the standalone `NetworkRestoreServer::start`
    /// path.  Production replicated environments register the
    /// `NetworkRestoreServer` as a `ServiceHandler` on the dispatcher;
    /// the dispatcher first reads a length-prefixed service-name
    /// handshake, then delegates to the handler.  The handler reads
    /// `RESTORE_MAGIC` over the channel framing (not raw stream bytes)
    /// and replies with a single framed payload.
    ///
    /// This method speaks that protocol: it goes through
    /// `connect_to_service(RESTORE)`, sends the magic over the channel,
    /// then receives one framed payload containing the entire
    /// `[count][file_records...]` structure and decodes it into local
    /// files.
    pub fn execute_via_dispatcher(&self) -> Result<()> {
        use crate::net::Channel;
        use crate::net::service_dispatcher::connect_to_service;
        use crate::network_restore_server::RESTORE_SERVICE_NAME;

        // Validate state.
        {
            let state = self.state.lock();
            if *state != RestoreState::NotStarted {
                return Err(RepError::NetworkRestoreError(format!(
                    "execute_via_dispatcher called in wrong state: {:?}",
                    *state
                )));
            }
        }

        self.start()?;
        let started_at = Instant::now();

        let addr_str =
            format!("{}:{}", self.config.source_host, self.config.source_port);
        let addr: std::net::SocketAddr = addr_str.parse().map_err(|e| {
            RepError::NetworkRestoreError(format!(
                "bad source address {}: {}",
                addr_str, e
            ))
        })?;

        let channel =
            connect_to_service(addr, RESTORE_SERVICE_NAME).map_err(|e| {
                RepError::NetworkRestoreError(format!(
                    "connect_to_service(RESTORE) at {}: {}",
                    addr, e
                ))
            })?;

        // Send the RESTORE magic over the channel framing.
        channel.send(&RESTORE_MAGIC.to_le_bytes()).map_err(|e| {
            RepError::NetworkRestoreError(format!(
                "sending restore magic via dispatcher: {}",
                e
            ))
        })?;

        // Receive one framed payload containing the entire restore body.
        let payload = channel
            .receive(Duration::from_secs(120))
            .map_err(|e| {
                RepError::NetworkRestoreError(format!(
                    "receiving restore payload: {}",
                    e
                ))
            })?
            .ok_or_else(|| {
                RepError::NetworkRestoreError(
                    "empty restore payload from dispatcher".to_string(),
                )
            })?;

        // Decode the payload.
        if payload.len() < 4 {
            return Err(RepError::NetworkRestoreError(format!(
                "truncated restore payload: {} bytes",
                payload.len()
            )));
        }
        let mut off = 0usize;
        let mut buf4 = [0u8; 4];
        buf4.copy_from_slice(&payload[off..off + 4]);
        off += 4;
        let file_count = u32::from_le_bytes(buf4);

        let log_dir = self.local_log_dir.clone().unwrap_or_else(|| {
            std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."))
        });
        std::fs::create_dir_all(&log_dir).map_err(|e| {
            RepError::NetworkRestoreError(format!(
                "creating log dir {}: {}",
                log_dir.display(),
                e
            ))
        })?;

        let mut total_bytes: u64 = 0;
        let mut files_done: u32 = 0;
        let mut buf2 = [0u8; 2];
        let mut buf8 = [0u8; 8];

        for _ in 0..file_count {
            if off + 2 > payload.len() {
                return Err(RepError::NetworkRestoreError(
                    "truncated restore payload at name_len".to_string(),
                ));
            }
            buf2.copy_from_slice(&payload[off..off + 2]);
            off += 2;
            let name_len = u16::from_le_bytes(buf2) as usize;
            if off + name_len + 8 > payload.len() {
                return Err(RepError::NetworkRestoreError(
                    "truncated restore payload at name+size".to_string(),
                ));
            }
            let name_bytes = payload[off..off + name_len].to_vec();
            off += name_len;
            let filename = String::from_utf8(name_bytes).map_err(|e| {
                RepError::NetworkRestoreError(format!(
                    "non-UTF8 filename: {}",
                    e
                ))
            })?;
            validate_restore_filename(&filename)?;

            buf8.copy_from_slice(&payload[off..off + 8]);
            off += 8;
            let file_size = u64::from_le_bytes(buf8) as usize;
            if off + file_size > payload.len() {
                return Err(RepError::NetworkRestoreError(format!(
                    "truncated restore payload at file body for {:?} \
                     (need {} bytes, have {})",
                    filename,
                    file_size,
                    payload.len() - off,
                )));
            }

            let dest_path = log_dir.join(&filename);
            if self.config.retain_log_files && dest_path.exists() {
                let backup = log_dir.join(format!("{}.bak", filename));
                let _ = std::fs::rename(&dest_path, &backup);
            }

            std::fs::write(&dest_path, &payload[off..off + file_size])
                .map_err(|e| {
                    RepError::NetworkRestoreError(format!(
                        "writing '{}': {}",
                        dest_path.display(),
                        e
                    ))
                })?;
            off += file_size;
            total_bytes += file_size as u64;
            files_done += 1;
            self.update_progress(total_bytes, files_done);
            self.update_elapsed(started_at.elapsed());
        }

        self.update_elapsed(started_at.elapsed());
        self.complete()?;

        log::info!(
            "NetworkRestore via dispatcher from {}: {} file(s), {} bytes in {:?}",
            addr,
            files_done,
            total_bytes,
            started_at.elapsed(),
        );
        Ok(())
    }

    /// Mark the restore as in-progress.
    ///
    /// State-transition helper that moves the restore from
    /// `RestoreState::NotStarted` to `RestoreState::InProgress` and
    /// updates the public `progress` snapshot to match. It performs no
    /// I/O — the actual file transfer is driven by [`execute`](Self::execute).
    ///
    /// Callers that drive the restore via `execute()` do not need to
    /// invoke this directly; `execute()` performs the same state
    /// transition internally before any work begins.
    pub fn start(&self) -> Result<()> {
        let mut state = self.state.lock();
        match *state {
            RestoreState::NotStarted => {
                *state = RestoreState::InProgress;
                let mut progress = self.progress.lock();
                progress.state = RestoreState::InProgress;
                Ok(())
            }
            RestoreState::Completed => Err(RepError::NetworkRestoreError(
                "restore already completed".into(),
            )),
            RestoreState::Failed => Err(RepError::NetworkRestoreError(
                "restore already failed; create a new instance".into(),
            )),
            RestoreState::InProgress => Err(RepError::NetworkRestoreError(
                "restore already in progress".into(),
            )),
        }
    }

    /// Update the progress of an in-progress restore.
    ///
    /// # Arguments
    /// * `bytes` - Total bytes transferred so far.
    /// * `files` - Total files transferred so far.
    pub fn update_progress(&self, bytes: u64, files: u32) {
        let mut progress = self.progress.lock();
        progress.bytes_transferred = bytes;
        progress.files_transferred = files;
    }

    /// Update the elapsed time for progress tracking.
    pub fn update_elapsed(&self, elapsed: Duration) {
        let mut progress = self.progress.lock();
        progress.elapsed = elapsed;
    }

    /// Mark the restore as completed successfully.
    pub fn complete(&self) -> Result<()> {
        let mut state = self.state.lock();
        match *state {
            RestoreState::InProgress => {
                *state = RestoreState::Completed;
                let mut progress = self.progress.lock();
                progress.state = RestoreState::Completed;
                Ok(())
            }
            other => Err(RepError::NetworkRestoreError(format!(
                "cannot complete from state {:?}",
                other
            ))),
        }
    }

    /// Mark the restore as failed.
    pub fn fail(&self) -> Result<()> {
        let mut state = self.state.lock();
        match *state {
            RestoreState::InProgress => {
                *state = RestoreState::Failed;
                let mut progress = self.progress.lock();
                progress.state = RestoreState::Failed;
                Ok(())
            }
            other => Err(RepError::NetworkRestoreError(format!(
                "cannot fail from state {:?}",
                other
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> NetworkRestoreConfig {
        NetworkRestoreConfig {
            source_node: "node1".into(),
            source_host: "192.168.1.10".into(),
            source_port: 5001,
            retain_log_files: false,
        }
    }

    #[test]
    fn test_initial_state() {
        let restore = NetworkRestore::new(test_config());
        assert_eq!(restore.get_state(), RestoreState::NotStarted);

        let progress = restore.get_progress();
        assert_eq!(progress.state, RestoreState::NotStarted);
        assert_eq!(progress.bytes_transferred, 0);
        assert_eq!(progress.files_transferred, 0);
        assert_eq!(progress.elapsed, Duration::ZERO);
    }

    #[test]
    fn test_start() {
        let restore = NetworkRestore::new(test_config());
        restore.start().unwrap();
        assert_eq!(restore.get_state(), RestoreState::InProgress);
        assert_eq!(restore.get_progress().state, RestoreState::InProgress);
    }

    #[test]
    fn test_start_twice_fails() {
        let restore = NetworkRestore::new(test_config());
        restore.start().unwrap();
        let result = restore.start();
        assert!(result.is_err());
    }

    #[test]
    fn test_update_progress() {
        let restore = NetworkRestore::new(test_config());
        restore.start().unwrap();

        restore.update_progress(1024 * 1024, 3);
        let progress = restore.get_progress();
        assert_eq!(progress.bytes_transferred, 1024 * 1024);
        assert_eq!(progress.files_transferred, 3);
    }

    #[test]
    fn test_update_elapsed() {
        let restore = NetworkRestore::new(test_config());
        restore.start().unwrap();

        let elapsed = Duration::from_secs(42);
        restore.update_elapsed(elapsed);
        assert_eq!(restore.get_progress().elapsed, elapsed);
    }

    #[test]
    fn test_complete() {
        let restore = NetworkRestore::new(test_config());
        restore.start().unwrap();
        restore.complete().unwrap();
        assert_eq!(restore.get_state(), RestoreState::Completed);
        assert_eq!(restore.get_progress().state, RestoreState::Completed);
    }

    #[test]
    fn test_complete_from_not_started_fails() {
        let restore = NetworkRestore::new(test_config());
        let result = restore.complete();
        assert!(result.is_err());
    }

    #[test]
    fn test_fail() {
        let restore = NetworkRestore::new(test_config());
        restore.start().unwrap();
        restore.fail().unwrap();
        assert_eq!(restore.get_state(), RestoreState::Failed);
        assert_eq!(restore.get_progress().state, RestoreState::Failed);
    }

    #[test]
    fn test_fail_from_not_started_fails() {
        let restore = NetworkRestore::new(test_config());
        let result = restore.fail();
        assert!(result.is_err());
    }

    #[test]
    fn test_start_after_completed_fails() {
        let restore = NetworkRestore::new(test_config());
        restore.start().unwrap();
        restore.complete().unwrap();
        let result = restore.start();
        assert!(result.is_err());
    }

    #[test]
    fn test_start_after_failed_fails() {
        let restore = NetworkRestore::new(test_config());
        restore.start().unwrap();
        restore.fail().unwrap();
        let result = restore.start();
        assert!(result.is_err());
    }

    #[test]
    fn test_config_accessor() {
        let config = test_config();
        let restore = NetworkRestore::new(config);
        assert_eq!(restore.get_config().source_node, "node1");
        assert_eq!(restore.get_config().source_host, "192.168.1.10");
        assert_eq!(restore.get_config().source_port, 5001);
        assert!(!restore.get_config().retain_log_files);
    }

    #[test]
    fn test_retain_log_files_config() {
        let mut config = test_config();
        config.retain_log_files = true;
        let restore = NetworkRestore::new(config);
        assert!(restore.get_config().retain_log_files);
    }

    #[test]
    fn test_full_lifecycle() {
        let restore = NetworkRestore::new(test_config());

        assert_eq!(restore.get_state(), RestoreState::NotStarted);

        restore.start().unwrap();
        assert_eq!(restore.get_state(), RestoreState::InProgress);

        restore.update_progress(512, 1);
        restore.update_progress(2048, 2);
        restore.update_elapsed(Duration::from_secs(5));

        let progress = restore.get_progress();
        assert_eq!(progress.bytes_transferred, 2048);
        assert_eq!(progress.files_transferred, 2);
        assert_eq!(progress.elapsed, Duration::from_secs(5));

        restore.complete().unwrap();
        assert_eq!(restore.get_state(), RestoreState::Completed);
    }

    #[test]
    fn test_fail_lifecycle() {
        let restore = NetworkRestore::new(test_config());
        restore.start().unwrap();
        restore.update_progress(256, 1);
        restore.fail().unwrap();

        assert_eq!(restore.get_state(), RestoreState::Failed);
        // Progress data should still be accessible after failure.
        let progress = restore.get_progress();
        assert_eq!(progress.bytes_transferred, 256);
        assert_eq!(progress.files_transferred, 1);
    }

    // -----------------------------------------------------------------------
    // LOG-4: server-supplied filename validation
    // -----------------------------------------------------------------------

    fn assert_unsafe(name: &str) {
        let err = validate_restore_filename(name)
            .expect_err(&format!("expected rejection for {:?}", name));
        match err {
            RepError::ProtocolError(msg) => assert!(
                msg.contains("unsafe filename"),
                "unexpected message for {:?}: {}",
                name,
                msg
            ),
            other => {
                panic!("expected ProtocolError for {:?}, got {:?}", name, other)
            }
        }
    }

    #[test]
    fn test_validate_filename_rejects_empty() {
        assert_unsafe("");
    }

    #[test]
    fn test_validate_filename_rejects_dot_and_dotdot() {
        assert_unsafe(".");
        assert_unsafe("..");
    }

    #[test]
    fn test_validate_filename_rejects_hidden_dotfile() {
        assert_unsafe(".bashrc");
        assert_unsafe(".hidden");
    }

    #[test]
    fn test_validate_filename_rejects_path_separators() {
        assert_unsafe("../etc/passwd");
        assert_unsafe("/etc/passwd");
        assert_unsafe("subdir/file.ndb");
        assert_unsafe("dir\\file.ndb");
        assert_unsafe("..\\windows\\system32");
    }

    #[test]
    fn test_validate_filename_rejects_null_byte() {
        assert_unsafe("good\0name.ndb");
        assert_unsafe("\0");
    }

    #[test]
    fn test_validate_filename_accepts_normal_log_files() {
        validate_restore_filename("00000000.ndb").unwrap();
        validate_restore_filename("00000001.ndb").unwrap();
        validate_restore_filename("ffffffff.ndb").unwrap();
        validate_restore_filename("data.bin").unwrap();
        validate_restore_filename("name-with-dashes_and_underscores.ndb")
            .unwrap();
    }
}
