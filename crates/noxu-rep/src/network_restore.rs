//! Network restore for copying database files from another node.
//!
//! NetworkRestore
//! copies log files from a network peer to restore a node that has fallen
//! too far behind the replication stream. This is used when a replica
//! discovers an `InsufficientLogException`  -  its local log files are too
//! old for the feeder to supply a contiguous stream.

use std::io::{Read as IoRead, Write as IoWrite};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use noxu_log::create_marker_file;
use noxu_log::entry::restore_required::FailureType;
use noxu_log::remove_marker_file;
use noxu_sync::Mutex;

use crate::error::{RepError, Result};

/// Magic bytes sent at the start of every restore-request frame.
///
/// 4-byte little-endian value: `0x4E52_5354` ('N','R','S','T').
const RESTORE_MAGIC: u32 = 0x4E52_5354;

/// FID-NR-2 test hook: when set on a `NetworkRestore`, `execute_via_dispatcher`
/// simulates a crash AFTER writing the `.tmp` file but BEFORE the atomic
/// rename, so a witness can prove the final `.ndb` is never a torn/partial
/// file. Per-instance (not a global) so parallel tests do not interfere.
/// Compiled only in test builds; absent from production.
#[cfg(test)]
use std::sync::atomic::AtomicBool as TestCrashFlag;

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
    /// FID-NR-2 per-instance crash hook (test builds only).
    #[cfg(test)]
    crash_before_rename: TestCrashFlag,
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
            #[cfg(test)]
            crash_before_rename: TestCrashFlag::new(false),
        }
    }

    /// FID-NR-2 test hook setter: arm the simulated crash before the atomic
    /// rename (test builds only).
    #[cfg(test)]
    pub(crate) fn arm_crash_before_rename(&self) {
        self.crash_before_rename
            .store(true, std::sync::atomic::Ordering::SeqCst);
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

        // FID-NR-1: fence the environment BEFORE mutating any local log file.
        // A crash from here until the marker is removed (after the full set
        // transfers) leaves the marker in place, so reopening the env refuses.
        if file_count > 0 {
            self.write_restore_marker(&log_dir)?;
        }

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
            let dest_path = log_dir.join(&filename);

            // FID-NR-2 (JE NetworkBackup.getFile, NetworkBackup.java:575):
            // write the incoming bytes to a `.tmp` file and atomically rename
            // it onto the final name ONLY after the digest verifies. A crash
            // mid-write then leaves a `.tmp` file, never a truncated/garbage
            // live `.ndb` log file that recovery would read.
            let tmp_path = log_dir.join(format!("{}.tmp", filename));
            // Delete a stale tmp if one exists (JE deletes N.jdb.tmp first).
            if tmp_path.exists() {
                std::fs::remove_file(&tmp_path).map_err(|e| {
                    RepError::NetworkRestoreError(format!(
                        "deleting stale tmp '{}': {}",
                        tmp_path.display(),
                        e
                    ))
                })?;
            }

            // Stream file bytes to the tmp file in 64 KiB chunks.
            let mut out = std::fs::File::create(&tmp_path).map_err(|e| {
                RepError::NetworkRestoreError(format!(
                    "creating '{}': {}",
                    tmp_path.display(),
                    e
                ))
            })?;

            let mut remaining = file_size;
            let mut chunk = vec![0u8; 65536];
            // D10: recompute the CRC32 over received bytes and verify against
            // the 4-byte trailer the server appends, to reject a truncated or
            // corrupted transfer before it is accepted as a valid log file.
            let mut digest = crc32fast::Hasher::new();
            while remaining > 0 {
                let to_read = (remaining as usize).min(chunk.len());
                stream.read_exact(&mut chunk[..to_read]).map_err(|e| {
                    RepError::NetworkRestoreError(format!(
                        "reading data for '{}': {}",
                        filename, e
                    ))
                })?;
                digest.update(&chunk[..to_read]);
                out.write_all(&chunk[..to_read]).map_err(|e| {
                    RepError::NetworkRestoreError(format!(
                        "writing '{}': {}",
                        tmp_path.display(),
                        e
                    ))
                })?;
                remaining -= to_read as u64;
                total_bytes += to_read as u64;
            }
            // Verify the CRC32 trailer.
            let mut crc_buf = [0u8; 4];
            stream.read_exact(&mut crc_buf).map_err(|e| {
                RepError::NetworkRestoreError(format!(
                    "reading digest for '{}': {}",
                    filename, e
                ))
            })?;
            let want = u32::from_le_bytes(crc_buf);
            let got = digest.finalize();
            if want != got {
                let _ = std::fs::remove_file(&tmp_path);
                return Err(RepError::NetworkRestoreError(format!(
                    "digest mismatch for '{}': expected {:#010x}, got {:#010x}                      (file corrupted or truncated in transit)",
                    filename, want, got
                )));
            }

            // FID-NR-2: fsync the tmp file, then atomically rename it onto the
            // final name. The rename is the single commit point; before it the
            // final `.ndb` does not exist (or still holds the previous
            // contents), so a crash can never leave a torn live log file.
            out.sync_all().map_err(|e| {
                RepError::NetworkRestoreError(format!(
                    "fsync '{}': {}",
                    tmp_path.display(),
                    e
                ))
            })?;
            drop(out);
            // FID-NR-3 (JE NetworkBackup.getFile: disposeObsoleteFiles(file)
            // before renameTo, NetworkBackup.java:720): dispose this file and
            // all higher-numbered local log files, highest-first, so an
            // interrupted multi-file replace leaves a consistent prefix.
            if let Some(num) = Self::parse_restore_file_number(&filename) {
                self.dispose_obsolete_files(&log_dir, num)?;
            }
            std::fs::rename(&tmp_path, &dest_path).map_err(|e| {
                RepError::NetworkRestoreError(format!(
                    "renaming '{}' -> '{}': {}",
                    tmp_path.display(),
                    dest_path.display(),
                    e
                ))
            })?;

            files_done += 1;
            self.update_progress(total_bytes, files_done);
            self.update_elapsed(started_at.elapsed());

            log::debug!(
                "NetworkRestore: received '{}' ({} bytes)",
                filename,
                file_size
            );
        }

        // FID-NR-1: the full set transferred coherently — remove the marker so
        // the environment is openable again (JE removeMarkerFile after the
        // whole backup finishes).
        if file_count > 0 {
            self.remove_restore_marker(&log_dir)?;
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

        // FID-NR-1: fence the environment BEFORE mutating any local log file.
        if file_count > 0 {
            self.write_restore_marker(&log_dir)?;
        }

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
            // D10: file body is followed by a 4-byte CRC32 trailer.
            if off + file_size + 4 > payload.len() {
                return Err(RepError::NetworkRestoreError(format!(
                    "truncated restore payload at file body for {:?} \
                     (need {} bytes + 4 digest, have {})",
                    filename,
                    file_size,
                    payload.len() - off,
                )));
            }
            // Verify the CRC32 trailer before accepting the file.
            let body = &payload[off..off + file_size];
            let want = u32::from_le_bytes(
                payload[off + file_size..off + file_size + 4]
                    .try_into()
                    .expect("4-byte CRC slice"),
            );
            let got = crc32fast::hash(body);
            if want != got {
                return Err(RepError::NetworkRestoreError(format!(
                    "digest mismatch for '{}': expected {:#010x}, got {:#010x} \
                     (file corrupted or truncated in transit)",
                    filename, want, got
                )));
            }

            let dest_path = log_dir.join(&filename);

            // FID-NR-2 (JE NetworkBackup.getFile, NetworkBackup.java:575):
            // write to a `.tmp` file, fsync, then atomically rename onto the
            // final name, so a crash can never leave a torn live `.ndb`.
            // (The CRC was already verified in RAM above; the tmp+rename adds
            // the on-disk atomicity.)
            let tmp_path = log_dir.join(format!("{}.tmp", filename));
            {
                use std::io::Write as _;
                let mut f = std::fs::File::create(&tmp_path).map_err(|e| {
                    RepError::NetworkRestoreError(format!(
                        "creating '{}': {}",
                        tmp_path.display(),
                        e
                    ))
                })?;
                f.write_all(body).map_err(|e| {
                    RepError::NetworkRestoreError(format!(
                        "writing '{}': {}",
                        tmp_path.display(),
                        e
                    ))
                })?;
                f.sync_all().map_err(|e| {
                    RepError::NetworkRestoreError(format!(
                        "fsync '{}': {}",
                        tmp_path.display(),
                        e
                    ))
                })?;
            }
            // FID-NR-2 crash witness: simulate a crash after the tmp file is
            // durable but before the atomic rename. The final `.ndb` must not
            // exist (or be left torn); only the `.tmp` is on disk.
            #[cfg(test)]
            if self
                .crash_before_rename
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(RepError::NetworkRestoreError(
                    "simulated crash before rename (FID-NR-2 witness)"
                        .to_string(),
                ));
            }
            // FID-NR-3 (JE NetworkBackup.getFile: disposeObsoleteFiles(file)
            // before renameTo, NetworkBackup.java:720): dispose this file and
            // all higher-numbered local log files, highest-first.
            if let Some(num) = Self::parse_restore_file_number(&filename) {
                self.dispose_obsolete_files(&log_dir, num)?;
            }
            std::fs::rename(&tmp_path, &dest_path).map_err(|e| {
                RepError::NetworkRestoreError(format!(
                    "renaming '{}' -> '{}': {}",
                    tmp_path.display(),
                    dest_path.display(),
                    e
                ))
            })?;
            off += file_size + 4;
            total_bytes += file_size as u64;
            files_done += 1;
            self.update_progress(total_bytes, files_done);
            self.update_elapsed(started_at.elapsed());
        }

        // FID-NR-1: the full set transferred coherently — remove the marker.
        if file_count > 0 {
            self.remove_restore_marker(&log_dir)?;
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

    /// FID-NR-3 (JE `NetworkBackup.disposeObsoleteFiles`,
    /// `NetworkBackup.java:720`): rename-to-`.bak` (or delete) this log file
    /// and every higher-numbered local log file, **highest-numbered first**,
    /// so the integrity of the log files in the directory is always preserved
    /// even if the dispose is itself interrupted. An interrupted multi-file
    /// replace then leaves a consistent lower prefix rather than a gap. The
    /// restore marker is a non-`.ndb` file and is therefore never considered
    /// (JE explicitly skips the marker so it survives until the whole restore
    /// finishes).
    ///
    /// `start_file_num` is the lowest-numbered file that must be disposed (the
    /// incoming file being placed). A failed rename/delete ERRORS
    /// (JE EFE.unexpectedState; see FID-NR-7).
    fn dispose_obsolete_files(
        &self,
        log_dir: &Path,
        start_file_num: u32,
    ) -> Result<()> {
        // Collect existing local .ndb files with their numbers.
        let mut nums: Vec<u32> = Vec::new();
        let rd = std::fs::read_dir(log_dir).map_err(|e| {
            RepError::NetworkRestoreError(format!(
                "listing log dir {} for obsolete-file disposal: {}",
                log_dir.display(),
                e
            ))
        })?;
        for entry in rd {
            let entry = entry.map_err(|e| {
                RepError::NetworkRestoreError(format!(
                    "reading a directory entry in {}: {}",
                    log_dir.display(),
                    e
                ))
            })?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(stem) = name.strip_suffix(".ndb")
                && let Ok(num) = u32::from_str_radix(stem, 16)
            {
                nums.push(num);
            }
        }
        // Highest-numbered first, down to start_file_num.
        nums.sort_unstable();
        for &num in nums.iter().rev() {
            if num < start_file_num {
                break;
            }
            let name = format!("{num:08x}.ndb");
            let path = log_dir.join(&name);
            if self.config.retain_log_files {
                let backup = log_dir.join(format!("{name}.bak"));
                // FID-NR-7: a failed rename must error, not be swallowed.
                std::fs::rename(&path, &backup).map_err(|e| {
                    RepError::NetworkRestoreError(format!(
                        "disposing obsolete log file: could not rename '{}' \
                         to '{}': {}",
                        path.display(),
                        backup.display(),
                        e
                    ))
                })?;
            } else {
                std::fs::remove_file(&path).map_err(|e| {
                    RepError::NetworkRestoreError(format!(
                        "disposing obsolete log file: could not delete '{}': {}",
                        path.display(),
                        e
                    ))
                })?;
            }
        }
        Ok(())
    }

    /// Parse the `u32` log-file number from a `NNNNNNNN.ndb` basename.
    fn parse_restore_file_number(filename: &str) -> Option<u32> {
        let stem = filename.strip_suffix(".ndb")?;
        u32::from_str_radix(stem, 16).ok()
    }

    /// Update the elapsed time for progress tracking.
    pub fn update_elapsed(&self, elapsed: Duration) {
        let mut progress = self.progress.lock();
        progress.elapsed = elapsed;
    }

    /// FID-NR-1 (JE `NetworkBackup.getFile` -> `RestoreMarker.createMarkerFile`,
    /// `NetworkBackup.java:683`): create the `RestoreRequired(NETWORK_RESTORE)`
    /// marker in `log_dir` BEFORE any local log file is mutated, so a crash
    /// mid-restore fences the environment — reopening it refuses (or forces a
    /// restore) rather than mounting a half-restored, incoherent file set.
    /// Idempotent (JE: "can be safely called multiple times").
    fn write_restore_marker(&self, log_dir: &Path) -> Result<()> {
        let mut props: hashbrown::HashMap<String, String> =
            hashbrown::HashMap::new();
        props
            .insert("source_node".to_string(), self.config.source_node.clone());
        props.insert(
            "source".to_string(),
            format!("{}:{}", self.config.source_host, self.config.source_port),
        );
        create_marker_file(log_dir, FailureType::NetworkRestore, props).map_err(
            |e| {
                RepError::NetworkRestoreError(format!(
                    "creating restore marker in {}: {}",
                    log_dir.display(),
                    e
                ))
            },
        )
    }

    /// FID-NR-1 (JE `NetworkBackup.getFiles` -> `restoreMarker.removeMarkerFile`,
    /// `NetworkBackup.java:496`): remove the marker only after the ENTIRE set
    /// has been transferred, so the environment is coherent again.
    fn remove_restore_marker(&self, log_dir: &Path) -> Result<()> {
        remove_marker_file(log_dir).map_err(|e| {
            RepError::NetworkRestoreError(format!(
                "removing restore marker in {}: {}",
                log_dir.display(),
                e
            ))
        })
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

    // -----------------------------------------------------------------------
    // execute_via_dispatcher: malformed-payload fault injection
    // -----------------------------------------------------------------------
    //
    // These stand up a real `TcpServiceDispatcher` with a fake RESTORE
    // handler that replies with a hand-crafted (malformed) payload, so the
    // decode error paths inside `execute_via_dispatcher` -- otherwise only
    // reachable via real wire corruption -- are exercised for real, over a
    // real socket, rather than by unit-testing extracted logic.

    struct FixedReplyRestoreService {
        reply: Vec<u8>,
    }

    impl crate::net::service_dispatcher::ServiceHandler
        for FixedReplyRestoreService
    {
        fn service_name(&self) -> &str {
            crate::network_restore_server::RESTORE_SERVICE_NAME
        }

        fn handle(&self, channel: Box<dyn crate::net::Channel>) -> Result<()> {
            // Drain the magic the client sends, then reply with the
            // canned (possibly malformed) payload.
            let _ = channel.receive(Duration::from_secs(5))?;
            channel.send(&self.reply)?;
            Ok(())
        }
    }

    /// Bind a `TcpServiceDispatcher` on an ephemeral port serving a fixed
    /// RESTORE reply, returning its bound address.
    fn spawn_fixed_reply_restore_server(
        reply: Vec<u8>,
    ) -> (
        crate::net::service_dispatcher::TcpServiceDispatcher,
        std::net::SocketAddr,
    ) {
        let dispatcher =
            crate::net::service_dispatcher::TcpServiceDispatcher::new(
                "127.0.0.1:0".parse().unwrap(),
            )
            .unwrap();
        dispatcher.register(
            crate::network_restore_server::RESTORE_SERVICE_NAME,
            std::sync::Arc::new(FixedReplyRestoreService { reply }),
        );
        let addr = dispatcher.start().unwrap();
        (dispatcher, addr)
    }

    fn dispatcher_config(addr: std::net::SocketAddr) -> NetworkRestoreConfig {
        NetworkRestoreConfig {
            source_node: "peer".into(),
            source_host: addr.ip().to_string(),
            source_port: addr.port(),
            retain_log_files: false,
        }
    }

    /// A payload shorter than the mandatory 4-byte file-count prefix must
    /// surface as a "truncated restore payload" error, not panic on the
    /// `payload[off..off + 4]` slice.
    #[test]
    fn execute_via_dispatcher_rejects_payload_shorter_than_file_count() {
        let (_disp, addr) = spawn_fixed_reply_restore_server(vec![0x01, 0x02]);
        let dir = tempfile::TempDir::new().unwrap();
        let restore = NetworkRestore::new(dispatcher_config(addr))
            .with_local_dir(dir.path());
        let err = restore.execute_via_dispatcher().unwrap_err();
        assert!(
            err.to_string().contains("truncated restore payload"),
            "unexpected error: {err}"
        );
    }

    /// A file-count claiming one file, but with no bytes at all for its
    /// `name_len` field, must surface "truncated ... at name_len".
    #[test]
    fn execute_via_dispatcher_rejects_truncated_name_len() {
        let mut payload = 1u32.to_le_bytes().to_vec(); // file_count = 1
        // no name_len bytes follow
        let (_disp, addr) =
            spawn_fixed_reply_restore_server(std::mem::take(&mut payload));
        let dir = tempfile::TempDir::new().unwrap();
        let restore = NetworkRestore::new(dispatcher_config(addr))
            .with_local_dir(dir.path());
        let err = restore.execute_via_dispatcher().unwrap_err();
        assert!(
            err.to_string().contains("name_len"),
            "unexpected error: {err}"
        );
    }

    /// A file-count/name_len pair that claims more name+size bytes than the
    /// payload actually carries must surface "truncated ... at name+size".
    #[test]
    fn execute_via_dispatcher_rejects_truncated_name_and_size() {
        let mut payload = 1u32.to_le_bytes().to_vec(); // file_count = 1
        payload.extend_from_slice(&100u16.to_le_bytes()); // name_len = 100
        payload.extend_from_slice(b"short"); // far fewer bytes than claimed
        let (_disp, addr) = spawn_fixed_reply_restore_server(payload);
        let dir = tempfile::TempDir::new().unwrap();
        let restore = NetworkRestore::new(dispatcher_config(addr))
            .with_local_dir(dir.path());
        let err = restore.execute_via_dispatcher().unwrap_err();
        assert!(
            err.to_string().contains("name+size"),
            "unexpected error: {err}"
        );
    }

    /// A well-formed file record whose CRC32 trailer does not match its body
    /// must be rejected as a digest mismatch — the wire-corruption guard
    /// D10 exists for.
    #[test]
    fn execute_via_dispatcher_rejects_digest_mismatch() {
        let filename = b"00000001.ndb";
        let body = b"hello world";
        let mut payload = 1u32.to_le_bytes().to_vec(); // file_count = 1
        payload.extend_from_slice(&(filename.len() as u16).to_le_bytes());
        payload.extend_from_slice(filename);
        payload.extend_from_slice(&(body.len() as u64).to_le_bytes());
        payload.extend_from_slice(body);
        // Wrong CRC32 (all zero) instead of crc32fast::hash(body).
        payload.extend_from_slice(&0u32.to_le_bytes());

        let (_disp, addr) = spawn_fixed_reply_restore_server(payload);
        let dir = tempfile::TempDir::new().unwrap();
        let restore = NetworkRestore::new(dispatcher_config(addr))
            .with_local_dir(dir.path());
        let err = restore.execute_via_dispatcher().unwrap_err();
        assert!(
            err.to_string().contains("digest mismatch"),
            "unexpected error: {err}"
        );
        // The corrupted file must NOT have been left on disk.
        assert!(!dir.path().join("00000001.ndb").exists());
    }

    /// A correctly-framed single file with a correct CRC32 trailer must be
    /// written to `local_log_dir` — the round-trip counterpart to the
    /// digest-mismatch rejection above, proving the CRC check is not simply
    /// disabled.
    #[test]
    fn execute_via_dispatcher_accepts_valid_digest() {
        let filename = b"00000002.ndb";
        let body = b"valid payload bytes";
        let mut payload = 1u32.to_le_bytes().to_vec();
        payload.extend_from_slice(&(filename.len() as u16).to_le_bytes());
        payload.extend_from_slice(filename);
        payload.extend_from_slice(&(body.len() as u64).to_le_bytes());
        payload.extend_from_slice(body);
        payload.extend_from_slice(&crc32fast::hash(body).to_le_bytes());

        let (_disp, addr) = spawn_fixed_reply_restore_server(payload);
        let dir = tempfile::TempDir::new().unwrap();
        let restore = NetworkRestore::new(dispatcher_config(addr))
            .with_local_dir(dir.path());
        restore.execute_via_dispatcher().expect("valid digest must succeed");
        let written = std::fs::read(dir.path().join("00000002.ndb")).unwrap();
        assert_eq!(written, body);
        // FID-NR-2: no leftover tmp file after a successful restore.
        assert!(
            !dir.path().join("00000002.ndb.tmp").exists(),
            "the tmp file must be renamed away on success"
        );
    }

    /// FID-NR-2 (S1, JE `NetworkBackup.getFile`, `NetworkBackup.java:575`):
    /// a restore must write to a `.tmp` file and atomically rename it onto
    /// the final name only after the digest verifies. A crash mid-write (here
    /// simulated by the CRASH_BEFORE_RENAME hook firing after the tmp is
    /// durable but before the rename) must leave the final `.ndb` UNCHANGED
    /// (its prior intact contents, or absent) — never a torn/partial live log
    /// file.
    ///
    /// Witness: RED at 81b2f048 (the base writes `std::fs::write(dest_path,
    /// body)` directly, so a crash mid-write overwrites/truncates the live
    /// `.ndb`). GREEN after the fix (the destination is only ever replaced by
    /// an atomic rename, so a pre-rename crash leaves the old file intact and
    /// only a `.tmp` behind).
    #[test]
    fn execute_via_dispatcher_crash_before_rename_leaves_final_intact() {
        let filename = b"00000007.ndb";
        let new_body = b"NEW restore bytes that must not land torn";
        let old_body = b"OLD intact log file contents";
        let mut payload = 1u32.to_le_bytes().to_vec();
        payload.extend_from_slice(&(filename.len() as u16).to_le_bytes());
        payload.extend_from_slice(filename);
        payload.extend_from_slice(&(new_body.len() as u64).to_le_bytes());
        payload.extend_from_slice(new_body);
        payload.extend_from_slice(&crc32fast::hash(new_body).to_le_bytes());

        let (_disp, addr) = spawn_fixed_reply_restore_server(payload);
        let dir = tempfile::TempDir::new().unwrap();
        // Pre-populate the destination with an intact prior log file.
        std::fs::write(dir.path().join("00000007.ndb"), old_body).unwrap();

        let restore = NetworkRestore::new(dispatcher_config(addr))
            .with_local_dir(dir.path());

        // Arm the per-instance crash hook, then run.
        restore.arm_crash_before_rename();
        let res = restore.execute_via_dispatcher();

        assert!(res.is_err(), "the simulated crash must surface as an error");
        // The live `.ndb` must still hold the OLD intact contents, never a
        // partial copy of the new body.
        let on_disk = std::fs::read(dir.path().join("00000007.ndb")).unwrap();
        assert_eq!(
            on_disk, old_body,
            "FID-NR-2: a crash before the atomic rename must leave the live \
             log file intact, not torn/overwritten"
        );
    }

    /// `execute_via_dispatcher` called a second time (state already
    /// `InProgress`/`Completed`) must reject with a state error instead of
    /// re-running the transfer.
    #[test]
    fn execute_via_dispatcher_rejects_wrong_state() {
        let (_disp, addr) = spawn_fixed_reply_restore_server(
            0u32.to_le_bytes().to_vec(), // file_count = 0, valid empty restore
        );
        let dir = tempfile::TempDir::new().unwrap();
        let restore = NetworkRestore::new(dispatcher_config(addr))
            .with_local_dir(dir.path());
        restore.execute_via_dispatcher().expect("first call must succeed");
        let err = restore.execute_via_dispatcher().unwrap_err();
        assert!(
            err.to_string().contains("wrong state"),
            "unexpected error: {err}"
        );
    }

    /// FID-NR-7 (S2, JE `NetworkBackup.disposeFile`, `NetworkBackup.java:751`):
    /// on `retain_log_files`, a failed rename of the existing file to `.bak`
    /// must ERROR, not be swallowed and the retained file silently
    /// overwritten. JE throws `EnvironmentFailureException.unexpectedState`.
    ///
    /// Witness: `retain_log_files = true`, with `<name>.bak` pre-created as a
    /// directory so `rename(file, dir)` fails. RED at 81b2f048 (`let _ =
    /// std::fs::rename(..)` swallows it; the restore proceeds and the
    /// retained copy is lost). GREEN after the fix (the error propagates).
    #[cfg(unix)]
    #[test]
    fn execute_via_dispatcher_bak_rename_failure_errors() {
        let filename = b"00000009.ndb";
        let body = b"incoming restore bytes";
        let mut payload = 1u32.to_le_bytes().to_vec();
        payload.extend_from_slice(&(filename.len() as u16).to_le_bytes());
        payload.extend_from_slice(filename);
        payload.extend_from_slice(&(body.len() as u64).to_le_bytes());
        payload.extend_from_slice(body);
        payload.extend_from_slice(&crc32fast::hash(body).to_le_bytes());

        let (_disp, addr) = spawn_fixed_reply_restore_server(payload);
        let dir = tempfile::TempDir::new().unwrap();
        // An existing log file to retain.
        std::fs::write(dir.path().join("00000009.ndb"), b"old").unwrap();
        // Make the `.bak` target a directory so renaming a file onto it fails.
        std::fs::create_dir(dir.path().join("00000009.ndb.bak")).unwrap();
        std::fs::write(
            dir.path().join("00000009.ndb.bak").join("blocker"),
            b"x",
        )
        .unwrap();

        let mut cfg = dispatcher_config(addr);
        cfg.retain_log_files = true;
        let restore = NetworkRestore::new(cfg).with_local_dir(dir.path());
        let res = restore.execute_via_dispatcher();
        assert!(
            res.is_err(),
            "a failed .bak rename must surface as an error (FID-NR-7)"
        );
        assert!(
            res.unwrap_err().to_string().contains("rename"),
            "error must describe the failed rename"
        );
    }

    /// FID-NR-1 (S2, JE `NetworkBackup.getFile` -> `RestoreMarker`,
    /// `NetworkBackup.java:683`; `EnvironmentImpl` open-time check): the client
    /// must create the `RestoreRequired(NETWORK_RESTORE)` marker BEFORE
    /// mutating any local log file, and a present marker must refuse the
    /// environment on open. A crash mid-restore then fences the half-restored
    /// env instead of letting it reopen as a usable-but-incoherent log.
    ///
    /// Witness: a two-file restore crashed (CRASH_BEFORE_RENAME) during the
    /// first file. The marker must be present, and opening an
    /// `EnvironmentImpl` on the dir must fail with a restore-required error.
    /// RED at 81b2f048 (no marker is ever written; the env opens a partial
    /// set). GREEN after the fix.
    #[test]
    fn crash_mid_restore_writes_marker_and_env_open_refuses() {
        use noxu_dbi::EnvironmentImpl;

        // Two-file payload so the crash lands after the marker is written and
        // before the set is complete.
        let mut payload = 2u32.to_le_bytes().to_vec();
        for (name, body) in [
            (b"00000000.ndb".as_slice(), b"file zero body".as_slice()),
            (b"00000001.ndb".as_slice(), b"file one body".as_slice()),
        ] {
            payload.extend_from_slice(&(name.len() as u16).to_le_bytes());
            payload.extend_from_slice(name);
            payload.extend_from_slice(&(body.len() as u64).to_le_bytes());
            payload.extend_from_slice(body);
            payload.extend_from_slice(&crc32fast::hash(body).to_le_bytes());
        }

        let (_disp, addr) = spawn_fixed_reply_restore_server(payload);
        let dir = tempfile::TempDir::new().unwrap();

        let restore = NetworkRestore::new(dispatcher_config(addr))
            .with_local_dir(dir.path());
        restore.arm_crash_before_rename();
        let res = restore.execute_via_dispatcher();
        assert!(res.is_err(), "the simulated crash must surface as an error");

        // FID-NR-1: the marker must be present after a crashed restore.
        assert!(
            noxu_log::marker_present(dir.path()),
            "a crashed restore must leave the RestoreRequired marker"
        );

        // FID-NR-1: opening the environment on this incoherent dir must refuse.
        let open = EnvironmentImpl::new(dir.path(), false, true);
        let err =
            open.err().expect("opening a marked (half-restored) env must fail");
        let msg = err.to_string();
        assert!(
            msg.contains("requires restore") || msg.contains("NETWORK_RESTORE"),
            "refusal must mention the restore requirement, got: {msg}"
        );
    }

    /// FID-NR-1: a COMPLETE restore removes the marker, so the environment is
    /// not spuriously fenced.
    #[test]
    fn complete_restore_removes_marker() {
        let filename = b"00000000.ndb";
        let body = b"complete file";
        let mut payload = 1u32.to_le_bytes().to_vec();
        payload.extend_from_slice(&(filename.len() as u16).to_le_bytes());
        payload.extend_from_slice(filename);
        payload.extend_from_slice(&(body.len() as u64).to_le_bytes());
        payload.extend_from_slice(body);
        payload.extend_from_slice(&crc32fast::hash(body).to_le_bytes());

        let (_disp, addr) = spawn_fixed_reply_restore_server(payload);
        let dir = tempfile::TempDir::new().unwrap();
        let restore = NetworkRestore::new(dispatcher_config(addr))
            .with_local_dir(dir.path());
        restore.execute_via_dispatcher().expect("restore must succeed");

        // Marker removed on success — the env is not fenced by a stale marker.
        assert!(
            !noxu_log::marker_present(dir.path()),
            "a completed restore must remove the marker"
        );
    }

    /// FID-NR-3 (S2, JE `NetworkBackup.disposeObsoleteFiles`,
    /// `NetworkBackup.java:720`): obsolete files must be disposed
    /// highest-numbered first, so if the dispose is interrupted the remaining
    /// files form a consistent lower prefix (no gap). Files below the start
    /// file are untouched.
    ///
    /// Witness: local files 2,3,4,5 exist; file 3 is made un-deletable (a
    /// non-empty directory). Disposing from start=2 must remove 5 then 4
    /// (highest-first), then FAIL on 3 — leaving {2,3} as a consistent prefix,
    /// never a gap like {2,4} or {2,5}. RED at 81b2f048 (no
    /// disposeObsoleteFiles / no ordering). GREEN after the fix.
    #[cfg(unix)]
    #[test]
    fn dispose_obsolete_files_highest_first_leaves_prefix() {
        let dir = tempfile::TempDir::new().unwrap();
        for n in [2u32, 4, 5] {
            std::fs::write(dir.path().join(format!("{n:08x}.ndb")), b"x")
                .unwrap();
        }
        // File 3 is a non-empty directory so remove_file fails on it.
        std::fs::create_dir(dir.path().join("00000003.ndb")).unwrap();
        std::fs::write(dir.path().join("00000003.ndb").join("blk"), b"x")
            .unwrap();

        // Non-retain restore so dispose deletes.
        let cfg = NetworkRestoreConfig {
            source_node: "s".into(),
            source_host: "127.0.0.1".into(),
            source_port: 1,
            retain_log_files: false,
        };
        let restore = NetworkRestore::new(cfg).with_local_dir(dir.path());

        let res = restore.dispose_obsolete_files(dir.path(), 2);
        assert!(res.is_err(), "dispose must fail on the un-deletable file 3");

        // Highest-first: 5 and 4 are gone (disposed before 3 was reached);
        // 3 (the failure) and 2 (below start, untouched) remain — a consistent
        // lower prefix, never a gap.
        assert!(
            !dir.path().join("00000005.ndb").exists(),
            "file 5 (highest) must be disposed first"
        );
        assert!(
            !dir.path().join("00000004.ndb").exists(),
            "file 4 must be disposed before 3"
        );
        assert!(
            dir.path().join("00000003.ndb").exists(),
            "the dispose failed at file 3; it must remain"
        );
        assert!(
            dir.path().join("00000002.ndb").exists(),
            "file 2 is below the start file and must be untouched"
        );
    }

    /// FID-NR-3 integration: restoring a SHORTER set over a longer
    /// pre-existing local log disposes the obsolete higher-numbered files.
    #[test]
    fn restore_disposes_obsolete_higher_local_files() {
        // Server sends only file 0.
        let body = b"fresh file zero";
        let mut payload = 1u32.to_le_bytes().to_vec();
        payload
            .extend_from_slice(&(b"00000000.ndb".len() as u16).to_le_bytes());
        payload.extend_from_slice(b"00000000.ndb");
        payload.extend_from_slice(&(body.len() as u64).to_le_bytes());
        payload.extend_from_slice(body);
        payload.extend_from_slice(&crc32fast::hash(body).to_le_bytes());

        let (_disp, addr) = spawn_fixed_reply_restore_server(payload);
        let dir = tempfile::TempDir::new().unwrap();
        // Pre-populate with stale higher-numbered local files.
        for n in [0u32, 1, 2] {
            std::fs::write(dir.path().join(format!("{n:08x}.ndb")), b"stale")
                .unwrap();
        }

        let restore = NetworkRestore::new(dispatcher_config(addr))
            .with_local_dir(dir.path());
        restore.execute_via_dispatcher().expect("restore");

        // File 0 is the fresh restored copy.
        assert_eq!(
            std::fs::read(dir.path().join("00000000.ndb")).unwrap(),
            body
        );
        // Stale higher files 1 and 2 were disposed (deleted, non-retain).
        assert!(
            !dir.path().join("00000001.ndb").exists(),
            "obsolete file 1 must be disposed"
        );
        assert!(
            !dir.path().join("00000002.ndb").exists(),
            "obsolete file 2 must be disposed"
        );
    }
}
