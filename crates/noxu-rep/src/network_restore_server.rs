//! Server-side network restore: stream log files to a requesting node.
//!
//! The restore server accepts TCP connections from nodes running
//! `NetworkRestore::execute()`. On each connection it:
//!
//! 1. Reads the 4-byte restore magic (`NRST` = `0x4E52_5354`).
//! 2. Lists all `.ndb` files in `env_home`, sorted by name.
//! 3. Writes `[file_count: u32 LE]`.
//! 4. For each file: writes `[name_len: u16 LE][name bytes][file_size: u64
//!    LE][file bytes]` in 64 KiB chunks.
//!
//! Two modes are available:
//!
//! - **Standalone**: call `NetworkRestoreServer::start(addr)` to bind a
//!   dedicated `TcpListener` and serve all incoming connections in the
//!   background.
//! - **Dispatcher-integrated**: register `NetworkRestoreServer` as a
//!   `ServiceHandler` named `"RESTORE"` with a `TcpServiceDispatcher`.
//!   The service dispatcher handles TCP negotiation; the handler receives a
//!   pre-opened channel through which the RESTORE protocol runs.

use std::io::{Read as IoRead, Write as IoWrite};
use std::net::{SocketAddr, TcpListener};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use crate::error::{RepError, Result};
use crate::net::channel::Channel;
use crate::net::service_dispatcher::ServiceHandler;

/// `0x4E52_5354` — the four bytes `NRST` as a little-endian u32.
const RESTORE_MAGIC: u32 = 0x4E52_5354;

/// The service name used when registered with `TcpServiceDispatcher`.
pub const RESTORE_SERVICE_NAME: &str = "RESTORE";

/// Serves log files to nodes performing a network restore.
///
/// Implements both standalone TCP serving and the `ServiceHandler` trait so
/// it can be plugged into a `TcpServiceDispatcher`.
pub struct NetworkRestoreServer {
    /// Directory containing `.ndb` log files to serve.
    env_home: PathBuf,
    /// Running flag; used to stop the accept loop.
    running: Arc<AtomicBool>,
}

impl NetworkRestoreServer {
    /// Create a new restore server that will serve files from `env_home`.
    pub fn new(env_home: impl Into<PathBuf>) -> Self {
        Self {
            env_home: env_home.into(),
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Wrap in `Arc` so the same instance can be shared between the accept
    /// loop thread and the `ServiceHandler` registration.
    pub fn into_arc(self) -> Arc<Self> {
        Arc::new(self)
    }

    /// Stop the standalone accept loop (if one was started).
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Whether the standalone accept loop is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Start a dedicated TCP accept loop on `addr`.
    ///
    /// Returns the actual bound address (useful when `addr` has port 0).
    /// Connections are handled in per-connection threads.
    pub fn start(self: &Arc<Self>, addr: SocketAddr) -> Result<SocketAddr> {
        let listener = TcpListener::bind(addr)
            .map_err(|e| RepError::NetworkError(e.to_string()))?;
        let bound = listener
            .local_addr()
            .map_err(|e| RepError::NetworkError(e.to_string()))?;

        self.running.store(true, Ordering::SeqCst);

        let server = Arc::clone(self);
        thread::spawn(move || {
            while server.running.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _peer)) => {
                        let srv = Arc::clone(&server);
                        thread::spawn(move || {
                            let _ = srv.serve_raw(stream);
                        });
                    }
                    Err(_) => break,
                }
            }
            server.running.store(false, Ordering::SeqCst);
        });

        Ok(bound)
    }

    /// Serve a single raw `TcpStream` connection using the RESTORE protocol.
    ///
    /// Called by the standalone accept loop.
    fn serve_raw(&self, mut stream: std::net::TcpStream) -> Result<()> {
        // Read and validate magic.
        let mut magic_buf = [0u8; 4];
        stream.read_exact(&mut magic_buf).map_err(|e| {
            RepError::NetworkRestoreError(format!("reading magic: {}", e))
        })?;
        let magic = u32::from_le_bytes(magic_buf);
        if magic != RESTORE_MAGIC {
            return Err(RepError::NetworkRestoreError(format!(
                "bad restore magic: 0x{:08X}",
                magic
            )));
        }

        self.send_files_to(&mut stream)
    }

    /// Core file-transfer logic: enumerate `.ndb` files, send count, then
    /// stream each file's name + size + bytes to `out`.
    ///
    /// Used by both `serve_raw` and the `ServiceHandler::handle` path.
    fn send_files_to<W: IoRead + IoWrite>(&self, out: &mut W) -> Result<()> {
        // Enumerate all .ndb files in env_home, sorted by name.
        let mut files: Vec<(String, PathBuf)> =
            std::fs::read_dir(&self.env_home)
                .map_err(|e| {
                    RepError::NetworkRestoreError(format!(
                        "cannot read env_home {}: {}",
                        self.env_home.display(),
                        e
                    ))
                })?
                .filter_map(|entry| {
                    let entry = entry.ok()?;
                    let path = entry.path();
                    if path.extension()?.to_str()? == "ndb" {
                        let name = path.file_name()?.to_str()?.to_string();
                        Some((name, path))
                    } else {
                        None
                    }
                })
                .collect();
        files.sort_by(|a, b| a.0.cmp(&b.0));

        // Send file count.
        let count = files.len() as u32;
        out.write_all(&count.to_le_bytes()).map_err(|e| {
            RepError::NetworkRestoreError(format!("writing file count: {}", e))
        })?;

        let mut chunk = vec![0u8; 65536];

        for (name, path) in &files {
            // Verify name fits in a u16 length prefix.
            let name_bytes = name.as_bytes();
            if name_bytes.len() > u16::MAX as usize {
                return Err(RepError::NetworkRestoreError(format!(
                    "filename too long: {}",
                    name
                )));
            }

            let name_len = name_bytes.len() as u16;
            out.write_all(&name_len.to_le_bytes()).map_err(|e| {
                RepError::NetworkRestoreError(format!(
                    "writing name_len for '{}': {}",
                    name, e
                ))
            })?;
            out.write_all(name_bytes).map_err(|e| {
                RepError::NetworkRestoreError(format!(
                    "writing filename '{}': {}",
                    name, e
                ))
            })?;

            // File size.
            let metadata = std::fs::metadata(path).map_err(|e| {
                RepError::NetworkRestoreError(format!(
                    "stat '{}': {}",
                    path.display(),
                    e
                ))
            })?;
            let file_size = metadata.len();
            out.write_all(&file_size.to_le_bytes()).map_err(|e| {
                RepError::NetworkRestoreError(format!(
                    "writing size for '{}': {}",
                    name, e
                ))
            })?;

            // Stream file data in 64 KiB chunks.
            let mut file = std::fs::File::open(path).map_err(|e| {
                RepError::NetworkRestoreError(format!(
                    "open '{}': {}",
                    path.display(),
                    e
                ))
            })?;
            let mut remaining = file_size as usize;
            while remaining > 0 {
                let to_read = remaining.min(chunk.len());
                let n = file.read(&mut chunk[..to_read]).map_err(|e| {
                    RepError::NetworkRestoreError(format!(
                        "reading '{}': {}",
                        path.display(),
                        e
                    ))
                })?;
                if n == 0 {
                    break; // Unexpected EOF — file may have been truncated.
                }
                out.write_all(&chunk[..n]).map_err(|e| {
                    RepError::NetworkRestoreError(format!(
                        "sending data for '{}': {}",
                        name, e
                    ))
                })?;
                remaining -= n;
            }

            log::debug!(
                "NetworkRestoreServer: sent '{}' ({} bytes)",
                name,
                file_size
            );
        }

        out.flush().map_err(|e| {
            RepError::NetworkRestoreError(format!("flushing output: {}", e))
        })?;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ServiceHandler implementation
// ---------------------------------------------------------------------------

/// `NetworkRestoreServer` can be registered with `TcpServiceDispatcher` under
/// the `"RESTORE"` service name. The service dispatcher reads the service name
/// from each new connection before calling `handle()`; the channel passed here
/// is ready for the RESTORE protocol (magic bytes onward).
impl ServiceHandler for NetworkRestoreServer {
    fn service_name(&self) -> &str {
        RESTORE_SERVICE_NAME
    }

    fn handle(&self, channel: Box<dyn Channel>) -> Result<()> {
        // Read the RESTORE magic through the channel.
        use std::time::Duration;

        let magic_bytes =
            channel.receive(Duration::from_secs(30))?.ok_or_else(|| {
                RepError::NetworkRestoreError(
                    "no magic bytes received on RESTORE channel".into(),
                )
            })?;
        if magic_bytes.len() < 4 {
            return Err(RepError::NetworkRestoreError(format!(
                "short magic: {} bytes",
                magic_bytes.len()
            )));
        }
        let magic = u32::from_le_bytes([
            magic_bytes[0],
            magic_bytes[1],
            magic_bytes[2],
            magic_bytes[3],
        ]);
        if magic != RESTORE_MAGIC {
            return Err(RepError::NetworkRestoreError(format!(
                "bad restore magic: 0x{:08X}",
                magic
            )));
        }

        // Build file list and send via the channel's framing.
        let mut files: Vec<(String, PathBuf)> =
            std::fs::read_dir(&self.env_home)
                .map_err(|e| {
                    RepError::NetworkRestoreError(format!(
                        "read_dir {}: {}",
                        self.env_home.display(),
                        e
                    ))
                })?
                .filter_map(|entry| {
                    let entry = entry.ok()?;
                    let path = entry.path();
                    if path.extension()?.to_str()? == "ndb" {
                        let name = path.file_name()?.to_str()?.to_string();
                        Some((name, path))
                    } else {
                        None
                    }
                })
                .collect();
        files.sort_by(|a, b| a.0.cmp(&b.0));

        // Send a single framed message containing the entire restore payload.
        // The payload uses the same wire layout as the raw-TCP path so the
        // client's `execute()` can work regardless of transport.
        let mut payload: Vec<u8> = Vec::new();
        let count = files.len() as u32;
        payload.extend_from_slice(&count.to_le_bytes());

        let mut chunk = vec![0u8; 65536];
        for (name, path) in &files {
            let name_bytes = name.as_bytes();
            let name_len = name_bytes.len() as u16;
            payload.extend_from_slice(&name_len.to_le_bytes());
            payload.extend_from_slice(name_bytes);

            let metadata = std::fs::metadata(path).map_err(|e| {
                RepError::NetworkRestoreError(format!(
                    "stat '{}': {}",
                    path.display(),
                    e
                ))
            })?;
            let file_size = metadata.len();
            payload.extend_from_slice(&file_size.to_le_bytes());

            let mut file = std::fs::File::open(path).map_err(|e| {
                RepError::NetworkRestoreError(format!(
                    "open '{}': {}",
                    path.display(),
                    e
                ))
            })?;
            let mut remaining = file_size as usize;
            while remaining > 0 {
                let to_read = remaining.min(chunk.len());
                let n = file.read(&mut chunk[..to_read]).map_err(|e| {
                    RepError::NetworkRestoreError(format!(
                        "reading '{}': {}",
                        path.display(),
                        e
                    ))
                })?;
                if n == 0 {
                    break;
                }
                payload.extend_from_slice(&chunk[..n]);
                remaining -= n;
            }
        }

        channel.send(&payload)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::time::Duration;
    use tempfile::TempDir;

    use crate::network_restore::{NetworkRestore, NetworkRestoreConfig};

    /// Create a temp env_home with some synthetic .ndb files.
    fn make_env_home(files: &[(&str, &[u8])]) -> TempDir {
        let dir = tempfile::tempdir().expect("temp dir");
        for (name, data) in files {
            let mut f =
                std::fs::File::create(dir.path().join(name)).expect("create");
            f.write_all(data).expect("write");
        }
        dir
    }

    // -----------------------------------------------------------------------
    // Standalone TCP server tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_server_starts_and_stops() {
        let dir = make_env_home(&[]);
        let server = Arc::new(NetworkRestoreServer::new(dir.path()));
        let _addr = server.start("127.0.0.1:0".parse().unwrap()).unwrap();
        assert!(server.is_running());
        server.stop();
        std::thread::sleep(Duration::from_millis(50));
        assert!(!server.is_running());
    }

    #[test]
    fn test_restore_empty_env_home() {
        let dir = make_env_home(&[]);
        let server = Arc::new(NetworkRestoreServer::new(dir.path()));
        let bound = server.start("127.0.0.1:0".parse().unwrap()).unwrap();
        std::thread::sleep(Duration::from_millis(20));

        let restore_dir = tempfile::tempdir().expect("restore dir");
        let config = NetworkRestoreConfig {
            source_node: "test".to_string(),
            source_host: "127.0.0.1".to_string(),
            source_port: bound.port(),
            retain_log_files: false,
        };
        let restore =
            NetworkRestore::new(config).with_local_dir(restore_dir.path());
        restore.execute().expect("empty restore should succeed");

        let received: Vec<_> = std::fs::read_dir(restore_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        assert_eq!(received.len(), 0);
        server.stop();
    }

    #[test]
    fn test_restore_single_file() {
        let content = b"log file content for testing";
        let dir = make_env_home(&[("00000001.ndb", content)]);
        let server = Arc::new(NetworkRestoreServer::new(dir.path()));
        let bound = server.start("127.0.0.1:0".parse().unwrap()).unwrap();
        std::thread::sleep(Duration::from_millis(20));

        let restore_dir = tempfile::tempdir().expect("restore dir");
        let config = NetworkRestoreConfig {
            source_node: "node1".to_string(),
            source_host: "127.0.0.1".to_string(),
            source_port: bound.port(),
            retain_log_files: false,
        };
        let restore =
            NetworkRestore::new(config).with_local_dir(restore_dir.path());
        restore.execute().expect("single-file restore");

        let received = std::fs::read(restore_dir.path().join("00000001.ndb"))
            .expect("received file");
        assert_eq!(&received, content);
        server.stop();
    }

    #[test]
    fn test_restore_multiple_files() {
        let file_data: Vec<(&str, Vec<u8>)> = (0u32..5)
            .map(|i| {
                let name: &'static str =
                    Box::leak(format!("{:08}.ndb", i).into_boxed_str());
                let data = vec![(i & 0xFF) as u8; 1024 * (i as usize + 1)];
                (name, data)
            })
            .collect();

        let file_refs: Vec<(&str, &[u8])> =
            file_data.iter().map(|(n, d)| (*n, d.as_slice())).collect();
        let dir = make_env_home(&file_refs);
        let server = Arc::new(NetworkRestoreServer::new(dir.path()));
        let bound = server.start("127.0.0.1:0".parse().unwrap()).unwrap();
        std::thread::sleep(Duration::from_millis(20));

        let restore_dir = tempfile::tempdir().expect("restore dir");
        let config = NetworkRestoreConfig {
            source_node: "node1".to_string(),
            source_host: "127.0.0.1".to_string(),
            source_port: bound.port(),
            retain_log_files: false,
        };
        let restore =
            NetworkRestore::new(config).with_local_dir(restore_dir.path());
        restore.execute().expect("multi-file restore");

        for (name, expected) in &file_data {
            let got = std::fs::read(restore_dir.path().join(name)).expect(name);
            assert_eq!(&got, expected, "file {} mismatch", name);
        }
        server.stop();
    }

    #[test]
    fn test_restore_non_ndb_files_not_sent() {
        // Only .ndb files should be transferred.
        let dir = make_env_home(&[
            ("00000001.ndb", b"log data"),
            ("noxu.config.csv", b"config"),
            ("README.txt", b"readme"),
        ]);
        let server = Arc::new(NetworkRestoreServer::new(dir.path()));
        let bound = server.start("127.0.0.1:0".parse().unwrap()).unwrap();
        std::thread::sleep(Duration::from_millis(20));

        let restore_dir = tempfile::tempdir().expect("restore dir");
        let config = NetworkRestoreConfig {
            source_node: "node1".to_string(),
            source_host: "127.0.0.1".to_string(),
            source_port: bound.port(),
            retain_log_files: false,
        };
        let restore =
            NetworkRestore::new(config).with_local_dir(restore_dir.path());
        restore.execute().expect("restore");

        // Only the .ndb file should appear.
        let mut names: Vec<String> = std::fs::read_dir(restore_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        names.sort();
        assert_eq!(names, vec!["00000001.ndb"]);
        server.stop();
    }

    #[test]
    fn test_restore_retain_log_files() {
        let original = b"original content";
        let updated = b"new content from restore";

        let src_dir = make_env_home(&[("00000001.ndb", updated)]);
        let server = Arc::new(NetworkRestoreServer::new(src_dir.path()));
        let bound = server.start("127.0.0.1:0".parse().unwrap()).unwrap();
        std::thread::sleep(Duration::from_millis(20));

        // Pre-populate the destination with the original file.
        let restore_dir = tempfile::tempdir().expect("restore dir");
        std::fs::write(restore_dir.path().join("00000001.ndb"), original)
            .expect("pre-populate");

        let config = NetworkRestoreConfig {
            source_node: "node1".to_string(),
            source_host: "127.0.0.1".to_string(),
            source_port: bound.port(),
            retain_log_files: true,
        };
        let restore =
            NetworkRestore::new(config).with_local_dir(restore_dir.path());
        restore.execute().expect("restore with retain");

        // The restored file should contain the new data.
        let got =
            std::fs::read(restore_dir.path().join("00000001.ndb")).unwrap();
        assert_eq!(&got, updated);

        // The backup file should still contain the original.
        let bak =
            std::fs::read(restore_dir.path().join("00000001.ndb.bak")).unwrap();
        assert_eq!(&bak, original);
        server.stop();
    }

    #[test]
    fn test_restore_large_file() {
        // 200 KiB — ensures chunking through the 64 KiB buffer.
        let large = vec![0xABu8; 200 * 1024];
        let dir = make_env_home(&[("large.ndb", &large)]);
        let server = Arc::new(NetworkRestoreServer::new(dir.path()));
        let bound = server.start("127.0.0.1:0".parse().unwrap()).unwrap();
        std::thread::sleep(Duration::from_millis(20));

        let restore_dir = tempfile::tempdir().expect("restore dir");
        let config = NetworkRestoreConfig {
            source_node: "node1".to_string(),
            source_host: "127.0.0.1".to_string(),
            source_port: bound.port(),
            retain_log_files: false,
        };
        let restore =
            NetworkRestore::new(config).with_local_dir(restore_dir.path());
        restore.execute().expect("large file restore");

        let got = std::fs::read(restore_dir.path().join("large.ndb")).unwrap();
        assert_eq!(got.len(), large.len());
        assert_eq!(&got, &large);
        server.stop();
    }

    #[test]
    fn test_server_service_name() {
        let dir = make_env_home(&[]);
        let server = NetworkRestoreServer::new(dir.path());
        assert_eq!(server.service_name(), RESTORE_SERVICE_NAME);
        assert_eq!(server.service_name(), "RESTORE");
    }

    #[test]
    fn test_restore_progress_tracking() {
        let content = b"progress test data";
        let dir = make_env_home(&[("00000001.ndb", content)]);
        let server = Arc::new(NetworkRestoreServer::new(dir.path()));
        let bound = server.start("127.0.0.1:0".parse().unwrap()).unwrap();
        std::thread::sleep(Duration::from_millis(20));

        let restore_dir = tempfile::tempdir().expect("restore dir");
        let config = NetworkRestoreConfig {
            source_node: "node1".to_string(),
            source_host: "127.0.0.1".to_string(),
            source_port: bound.port(),
            retain_log_files: false,
        };
        let restore =
            NetworkRestore::new(config).with_local_dir(restore_dir.path());
        restore.execute().expect("restore");

        let progress = restore.get_progress();
        assert_eq!(progress.files_transferred, 1);
        assert_eq!(progress.bytes_transferred, content.len() as u64);
        server.stop();
    }
}
