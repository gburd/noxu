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
            // D10: compute a CRC32 over the file bytes and send it as a 4-byte
            // trailer after the data, so the client can detect truncation or
            // corruption in transit (JE NetworkBackup sends a MessageDigest
            // with FileEnd; we use the project-wide CRC32 from crc32fast).
            let mut digest = crc32fast::Hasher::new();
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
                digest.update(&chunk[..n]);
                out.write_all(&chunk[..n]).map_err(|e| {
                    RepError::NetworkRestoreError(format!(
                        "sending data for '{}': {}",
                        name, e
                    ))
                })?;
                remaining -= n;
            }
            // Send the CRC32 trailer.
            out.write_all(&digest.finalize().to_le_bytes()).map_err(|e| {
                RepError::NetworkRestoreError(format!(
                    "sending digest for '{}': {}",
                    name, e
                ))
            })?;

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
            // D10: append a CRC32 trailer per file (same layout as the
            // raw-TCP send_files_to path) so execute_via_dispatcher can verify
            // integrity before accepting the file.
            let mut digest = crc32fast::Hasher::new();
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
                digest.update(&chunk[..n]);
                payload.extend_from_slice(&chunk[..n]);
                remaining -= n;
            }
            payload.extend_from_slice(&digest.finalize().to_le_bytes());
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
    fn test_restore_digest_detects_corruption() {
        // D10: send a file into an in-memory buffer via send_files_to, which
        // appends a CRC32 trailer; flip one data byte; confirm the recomputed
        // CRC over the corrupted body no longer matches the trailer (the exact
        // condition the client verify rejects on).
        use std::io::Cursor;
        let content = b"the quick brown fox jumps over the lazy dog";
        let dir = make_env_home(&[("00000001.ndb", content)]);
        let server = NetworkRestoreServer::new(dir.path());
        let mut buf = Cursor::new(Vec::new());
        server.send_files_to(&mut buf).expect("send into buffer");
        let mut wire = buf.into_inner();

        // Locate the file body: skip count(4) + name_len(4) + name + size(8).
        // count
        let mut off = 4usize;
        let name_len =
            u16::from_le_bytes(wire[off..off + 2].try_into().unwrap()) as usize;
        off += 2 + name_len;
        let file_size =
            u64::from_le_bytes(wire[off..off + 8].try_into().unwrap()) as usize;
        off += 8;
        let body_start = off;
        let trailer_start = body_start + file_size;

        // The trailer must match the clean body.
        let want = u32::from_le_bytes(
            wire[trailer_start..trailer_start + 4].try_into().unwrap(),
        );
        assert_eq!(want, crc32fast::hash(&wire[body_start..trailer_start]));

        // Flip one body byte; the CRC must now mismatch (client rejects).
        wire[body_start] ^= 0xFF;
        let got = crc32fast::hash(&wire[body_start..trailer_start]);
        assert_ne!(
            want, got,
            "D10: corrupted body must fail the CRC32 trailer check"
        );
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

    // -----------------------------------------------------------------------
    // Wire-protocol error-path coverage
    // -----------------------------------------------------------------------

    #[test]
    fn test_into_arc_wraps_self() {
        let dir = make_env_home(&[]);
        let server = NetworkRestoreServer::new(dir.path());
        let arc = server.into_arc();
        // Arc::strong_count is 1 right after wrapping; verify the
        // running flag is reachable and false.
        assert!(!arc.is_running());
        assert_eq!(Arc::strong_count(&arc), 1);
    }

    #[test]
    fn test_serve_raw_rejects_bad_magic() {
        // Connect to the server and send 4 bytes of garbage. The
        // server should close the connection with an Err on its
        // side; on the client we observe EOF / unexpected close.
        let dir = make_env_home(&[]);
        let server = Arc::new(NetworkRestoreServer::new(dir.path()));
        let bound = server.start("127.0.0.1:0".parse().unwrap()).unwrap();
        std::thread::sleep(Duration::from_millis(20));

        let mut stream = std::net::TcpStream::connect(bound).unwrap();
        stream.write_all(&[0xDE, 0xAD, 0xBE, 0xEF]).unwrap();

        // Server should close the stream rather than keep talking.
        // Read with a short timeout — expect 0 bytes (EOF) or an
        // error.
        stream.set_read_timeout(Some(Duration::from_millis(500))).unwrap();
        let mut buf = [0u8; 4];
        let r = std::io::Read::read(&mut stream, &mut buf);
        match r {
            Ok(0) => {} // clean EOF — server hung up
            Ok(_n) => panic!("server replied to bad magic instead of closing"),
            Err(_) => {} // timeout or reset — also acceptable
        }
        server.stop();
    }

    #[test]
    fn test_serve_raw_short_read_on_magic() {
        // Connect and immediately close (send no bytes). The server
        // should fail its read_exact with a short-read error and
        // not panic. The accept thread continues.
        let dir = make_env_home(&[]);
        let server = Arc::new(NetworkRestoreServer::new(dir.path()));
        let bound = server.start("127.0.0.1:0".parse().unwrap()).unwrap();
        std::thread::sleep(Duration::from_millis(20));

        // Connect and drop immediately.
        {
            let _ = std::net::TcpStream::connect(bound).unwrap();
        }
        // Subsequent connection should still work — accept loop
        // didn't crash.
        std::thread::sleep(Duration::from_millis(20));
        assert!(server.is_running());
        server.stop();
    }

    #[test]
    fn test_serve_raw_real_handshake_streams_files() {
        // End-to-end: use the standalone server (start + serve_raw)
        // to transfer one file. The existing test_restore_single_file
        // also exercises this, but via the higher-level
        // NetworkRestore client; here we open a raw socket and
        // walk the wire protocol manually so the read_exact /
        // u32::from_le_bytes paths are exercised.
        let content = b"hello world";
        let dir = make_env_home(&[("00000000.ndb", content)]);
        let server = Arc::new(NetworkRestoreServer::new(dir.path()));
        let bound = server.start("127.0.0.1:0".parse().unwrap()).unwrap();
        std::thread::sleep(Duration::from_millis(20));

        let mut stream = std::net::TcpStream::connect(bound).unwrap();
        stream.write_all(&RESTORE_MAGIC.to_le_bytes()).unwrap();

        // Read file count (u32, little-endian).
        let mut count_buf = [0u8; 4];
        std::io::Read::read_exact(&mut stream, &mut count_buf).unwrap();
        let count = u32::from_le_bytes(count_buf);
        assert_eq!(count, 1);

        // Read filename length (u16) + name bytes.
        let mut name_len_buf = [0u8; 2];
        std::io::Read::read_exact(&mut stream, &mut name_len_buf).unwrap();
        let name_len = u16::from_le_bytes(name_len_buf) as usize;
        let mut name_buf = vec![0u8; name_len];
        std::io::Read::read_exact(&mut stream, &mut name_buf).unwrap();
        assert_eq!(&name_buf, b"00000000.ndb");

        // Read file size (u64) + file bytes.
        let mut size_buf = [0u8; 8];
        std::io::Read::read_exact(&mut stream, &mut size_buf).unwrap();
        let size = u64::from_le_bytes(size_buf);
        assert_eq!(size as usize, content.len());

        let mut payload = vec![0u8; size as usize];
        std::io::Read::read_exact(&mut stream, &mut payload).unwrap();
        assert_eq!(&payload, content);

        server.stop();
    }

    #[test]
    fn test_start_returns_error_for_unbindable_addr() {
        let dir = make_env_home(&[]);
        let server = Arc::new(NetworkRestoreServer::new(dir.path()));
        // 192.0.2.1 is RFC 5737 TEST-NET-1 — guaranteed not assigned to any
        // local interface, so bind() fails with EADDRNOTAVAIL on both Unix
        // and Windows. (Port 1 is unreliable cross-platform: privileged on
        // Unix, but freely bindable by unprivileged users on Windows.)
        let r = server.start("192.0.2.1:9999".parse().unwrap());
        assert!(
            r.is_err(),
            "binding to a non-local address should fail on all platforms"
        );
    }

    #[test]
    fn test_stop_is_idempotent() {
        let dir = make_env_home(&[]);
        let server = Arc::new(NetworkRestoreServer::new(dir.path()));
        let _ = server.start("127.0.0.1:0".parse().unwrap()).unwrap();
        server.stop();
        server.stop();
        std::thread::sleep(Duration::from_millis(20));
        assert!(!server.is_running());
    }

    // -----------------------------------------------------------------------
    // ServiceHandler::handle path (multiplexed-channel transport)
    // -----------------------------------------------------------------------

    #[test]
    fn test_service_handler_handle_streams_via_channel() {
        use crate::net::channel::LocalChannelPair;

        let content = b"abcdef";
        let dir = make_env_home(&[("00000005.ndb", content)]);
        let server = NetworkRestoreServer::new(dir.path());

        let pair = LocalChannelPair::new();
        let server_channel: Box<dyn crate::net::channel::Channel> =
            Box::new(pair.channel_a);
        let client_channel = pair.channel_b;

        // Client sends magic.
        client_channel.send(&RESTORE_MAGIC.to_le_bytes()).unwrap();

        // Server runs handle().
        let r = server.handle(server_channel);
        assert!(r.is_ok(), "handle returned Err: {:?}", r.err());

        // Client receives the framed payload.
        use crate::net::channel::Channel;
        let payload = client_channel
            .receive(Duration::from_secs(5))
            .unwrap()
            .expect("payload");

        // Expected wire format: u32 count + (u16 name_len + name + u64 size + bytes).
        let count = u32::from_le_bytes([
            payload[0], payload[1], payload[2], payload[3],
        ]);
        assert_eq!(count, 1);

        let name_len = u16::from_le_bytes([payload[4], payload[5]]) as usize;
        assert_eq!(name_len, b"00000005.ndb".len());
        let name = &payload[6..6 + name_len];
        assert_eq!(name, b"00000005.ndb");

        let size_off = 6 + name_len;
        let mut size_bytes = [0u8; 8];
        size_bytes.copy_from_slice(&payload[size_off..size_off + 8]);
        let size = u64::from_le_bytes(size_bytes) as usize;
        assert_eq!(size, content.len());

        let data_off = size_off + 8;
        assert_eq!(&payload[data_off..data_off + size], content);
    }

    #[test]
    fn test_service_handler_handle_rejects_bad_magic() {
        use crate::net::channel::LocalChannelPair;

        let dir = make_env_home(&[]);
        let server = NetworkRestoreServer::new(dir.path());

        let pair = LocalChannelPair::new();
        let server_channel: Box<dyn crate::net::channel::Channel> =
            Box::new(pair.channel_a);
        let client_channel = pair.channel_b;

        client_channel.send(&[0xDE, 0xAD, 0xBE, 0xEF]).unwrap();
        let r = server.handle(server_channel);
        assert!(r.is_err(), "handle on bad magic must error");
        let msg = format!("{}", r.err().unwrap());
        assert!(
            msg.contains("bad restore magic"),
            "expected 'bad restore magic' in error, got: {msg}"
        );
    }

    #[test]
    fn test_service_handler_handle_rejects_short_magic() {
        use crate::net::channel::LocalChannelPair;

        let dir = make_env_home(&[]);
        let server = NetworkRestoreServer::new(dir.path());

        let pair = LocalChannelPair::new();
        let server_channel: Box<dyn crate::net::channel::Channel> =
            Box::new(pair.channel_a);
        let client_channel = pair.channel_b;

        client_channel.send(&[0xDE]).unwrap();
        let r = server.handle(server_channel);
        assert!(r.is_err(), "handle on short magic must error");
    }

    #[test]
    fn test_service_handler_handle_no_magic_received() {
        use crate::net::channel::LocalChannelPair;

        let dir = make_env_home(&[]);
        let server = NetworkRestoreServer::new(dir.path());

        let pair = LocalChannelPair::new();
        let server_channel: Box<dyn crate::net::channel::Channel> =
            Box::new(pair.channel_a);
        // Drop the client side without sending — server should fail
        // with "no magic bytes received".
        drop(pair.channel_b);
        let r = server.handle(server_channel);
        assert!(r.is_err(), "handle without magic must error");
    }

    #[test]
    fn test_service_handler_handle_with_unreadable_env_home() {
        use crate::net::channel::LocalChannelPair;

        // Point env_home at a path that doesn't exist — read_dir
        // fails inside handle() and we get a NetworkRestoreError.
        let server = NetworkRestoreServer::new("/nonexistent/path/xxx");

        let pair = LocalChannelPair::new();
        let server_channel: Box<dyn crate::net::channel::Channel> =
            Box::new(pair.channel_a);
        let client_channel = pair.channel_b;

        client_channel.send(&RESTORE_MAGIC.to_le_bytes()).unwrap();
        let r = server.handle(server_channel);
        assert!(r.is_err(), "unreadable env_home must error");
    }
}
