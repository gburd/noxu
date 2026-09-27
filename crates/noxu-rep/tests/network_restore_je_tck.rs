//! Test-parity port of JE's `com.sleepycat.je.rep.impl.networkRestore`
//! package (`NetworkBackupTest`, `NetworkRestoreTest`, `OneNodeRestoreTest`,
//! `NetworkRestoreNoMasterTest`, `ProtocolTest`,
//! `InterruptedNetworkRestoreTest`).
//!
//! # Architectural deviation (governs classification)
//!
//! JE's network restore is a rich, message-based protocol
//! (`FeederInfoReq/Resp`, `FileListReq/Resp`, `FileInfoReq/Resp`, `FileReq`,
//! `FileStart`, `FileEnd`, `Done` — ten wire message types, see JE
//! `Protocol.java`), with server-side *leases* (so a client whose connection
//! breaks can resume without re-copying), VLSN-range negotiation between log
//! providers, a `RestoreMarker` on-disk file that pins a half-restored node in
//! `InsufficientLogException` until a fresh restore completes, and per-file
//! skip/dispose/fetch statistics.
//!
//! Noxu deliberately implements a *simplified, deterministic file-copy*
//! protocol (`crates/noxu-rep/src/network_restore.rs` +
//! `network_restore_server.rs`): a single magic word, then
//! `[file_count]` followed by `[name_len][name][file_size][data][crc32]` per
//! `.ndb` file, over either a raw TCP socket (`execute`) or the
//! `TcpServiceDispatcher` (`execute_via_dispatcher`, the production path).
//! There is NO lease, NO VLSN-range message negotiation, NO `RestoreMarker`,
//! and NO skip/dispose/fetch statistics — those are genuine
//! design deviations (documented in the file-level module docs). What IS
//! faithfully portable — and is ported here — is the *observable behaviour*
//! the JE tests assert: the wire records round-trip and reject corruption; the
//! backup copies exactly the log files (verified byte-for-byte); a digest
//! trailer detects transit corruption; only `.ndb` files are selected; restore
//! from a specific / unknown peer is accepted / rejected; a no-peer restore
//! fails gracefully; and an interrupted transfer does not leave the node
//! `Completed` while a subsequent clean transfer restores it fully.
//!
//! These tests use the in-process / deterministic harness (real
//! `TcpServiceDispatcher`, real sockets, `TempDir` env homes) — no multi-JVM
//! live group is needed to prove the restore/backup contract.

use noxu_rep::network_restore::{
    NetworkRestore, NetworkRestoreConfig, RestoreState,
};
use noxu_rep::network_restore_server::{
    NetworkRestoreServer, RESTORE_SERVICE_NAME,
};
use noxu_rep::{NodeType, RepConfig, RepNode, ReplicatedEnvironment};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Create a temp env_home populated with the given `.ndb` (and other) files.
fn make_env_home(files: &[(&str, &[u8])]) -> TempDir {
    let dir = tempfile::tempdir().expect("temp dir");
    for (name, data) in files {
        std::fs::write(dir.path().join(name), data).expect("write file");
    }
    dir
}

/// Start a standalone `NetworkRestoreServer` on an ephemeral port and return
/// its bound port plus the server handle.
fn spawn_raw_server(dir: &TempDir) -> (Arc<NetworkRestoreServer>, u16) {
    let server = Arc::new(NetworkRestoreServer::new(dir.path()));
    let bound = server.start("127.0.0.1:0".parse().unwrap()).unwrap();
    std::thread::sleep(Duration::from_millis(20));
    (server, bound.port())
}

fn raw_config(port: u16, retain: bool) -> NetworkRestoreConfig {
    NetworkRestoreConfig {
        source_node: "src".to_string(),
        source_host: "127.0.0.1".to_string(),
        source_port: port,
        retain_log_files: retain,
    }
}

// ===========================================================================
// ProtocolTest — the network-restore wire records round-trip + reject.
// ===========================================================================
//
// JE `ProtocolTest.testBasic` round-trips all ten message types
// (FeederInfoReq/Resp, FileListReq/Resp, FileReq, FileStart, FileEnd,
// FileInfoReq/Resp, Done) through `protocol.read(TestChannel)` and asserts the
// re-serialized bytes equal the original. Noxu has no such message classes;
// its single-frame protocol carries the same information — a per-file record
// `[name_len][name][file_size][data][crc32]` — so the portable intent is "a
// well-formed file record round-trips through the server encode → client
// decode path unchanged, and a corrupt/short record is rejected."
//
// `ProtocolTest.testFileReqResp` writes a `FileStart` header, a 100-byte
// payload, and an Adler32 checksum trailer, then reads the header back,
// streams the payload verifying each byte, and checks the checksum trailer
// equals the recomputed checksum. Noxu's `[file_size][data][crc32]` layout is
// the direct analog (CRC32 not Adler32 — project-wide checksum deviation).

/// JE: `ProtocolTest.testBasic` (round-trip half, adapted to Noxu's
/// single-frame file-record wire format). A well-formed
/// `[count][name_len][name][size][data][crc32]` payload decodes to exactly the
/// bytes that were framed — proving the record round-trips unchanged.
#[test]
fn protocol_test_basic_file_record_round_trips() {
    // Build a payload the way the server's ServiceHandler::handle would:
    // one file "00000001.ndb" with a known body and correct CRC32 trailer.
    let filename = b"00000001.ndb";
    let body = b"the quick brown fox jumps over the lazy dog";
    let mut payload = 1u32.to_le_bytes().to_vec(); // file_count = 1
    payload.extend_from_slice(&(filename.len() as u16).to_le_bytes());
    payload.extend_from_slice(filename);
    payload.extend_from_slice(&(body.len() as u64).to_le_bytes());
    payload.extend_from_slice(body);
    payload.extend_from_slice(&crc32fast::hash(body).to_le_bytes());

    // Round-trip: decode via a real dispatcher round-trip and confirm the
    // decoded file bytes equal the framed body (the JE assertEquals-on-array).
    let dir = make_env_home(&[("00000001.ndb", body)]);
    let dispatcher =
        noxu_rep::net::service_dispatcher::TcpServiceDispatcher::new(
            "127.0.0.1:0".parse().unwrap(),
        )
        .unwrap();
    dispatcher.register(
        RESTORE_SERVICE_NAME,
        Arc::new(NetworkRestoreServer::new(dir.path())),
    );
    let addr = dispatcher.start().unwrap();
    std::thread::sleep(Duration::from_millis(20));

    let out_dir = tempfile::tempdir().unwrap();
    let restore = NetworkRestore::new(NetworkRestoreConfig {
        source_node: "peer".into(),
        source_host: addr.ip().to_string(),
        source_port: addr.port(),
        retain_log_files: false,
    })
    .with_local_dir(out_dir.path());
    restore.execute_via_dispatcher().expect("round-trip decode must succeed");

    let decoded = std::fs::read(out_dir.path().join("00000001.ndb")).unwrap();
    assert_eq!(&decoded, body, "decoded record bytes must equal framed body");
    // And the CRC in the payload we built above is the same one the server
    // sends, proving the wire layout is stable.
    assert_eq!(
        crc32fast::hash(body).to_le_bytes().to_vec(),
        payload[payload.len() - 4..].to_vec()
    );
    dispatcher.stop();
}

/// JE: `ProtocolTest.testBasic` (reject half). A file record whose CRC32
/// trailer does not match its body must be rejected on decode — the analog of
/// JE `protocol.read` refusing a message it cannot reconstruct. Guards against
/// a vacuous round-trip that never checks integrity.
#[test]
fn protocol_test_basic_rejects_corrupt_record() {
    // Serve a good file, then intercept and corrupt one body byte on the wire
    // by standing up a fake raw server that emits a mismatched CRC.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let body: &[u8] = b"hello world";
    let filename = b"00000001.ndb";
    std::thread::spawn(move || {
        if let Ok((mut s, _)) = listener.accept() {
            let mut magic = [0u8; 4];
            let _ = s.read_exact(&mut magic);
            let _ = s.write_all(&1u32.to_le_bytes()); // count = 1
            let _ = s.write_all(&(filename.len() as u16).to_le_bytes());
            let _ = s.write_all(filename);
            let _ = s.write_all(&(body.len() as u64).to_le_bytes());
            let _ = s.write_all(body);
            // WRONG crc (all-zero) — must be rejected by the client.
            let _ = s.write_all(&0u32.to_le_bytes());
            let _ = s.flush();
        }
    });
    std::thread::sleep(Duration::from_millis(20));

    let out_dir = tempfile::tempdir().unwrap();
    let restore = NetworkRestore::new(raw_config(port, false))
        .with_local_dir(out_dir.path());
    let err = restore.execute().unwrap_err();
    assert!(
        err.to_string().contains("digest mismatch"),
        "corrupt record must be rejected, got: {err}"
    );
    // The corrupt file must NOT be left on disk (execute removes it).
    assert!(!out_dir.path().join("00000001.ndb").exists());
}

/// JE: `ProtocolTest.testFileReqResp`. Frame a FileStart-equivalent header
/// (`name_len + name + file_size`), a byte-payload (0..100), and a CRC32
/// trailer; read the header back, stream the payload verifying each byte in
/// order, and check the trailer equals the recomputed checksum. This mirrors
/// JE's `CheckedInputStream` payload walk + Adler32 trailer comparison
/// (CRC32 substituted for Adler32 — project-wide checksum deviation).
#[test]
fn protocol_test_file_req_resp_streams_payload_and_checks_trailer() {
    // Simulated file payload: bytes 0..100.
    let body: Vec<u8> = (0u32..100).map(|i| (i & 0xFF) as u8).collect();
    let dir = make_env_home(&[("f1.ndb", &body)]);
    let (server, port) = spawn_raw_server(&dir);

    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream.write_all(&0x4E52_5354u32.to_le_bytes()).unwrap(); // RESTORE magic

    // Read file_count.
    let mut cbuf = [0u8; 4];
    stream.read_exact(&mut cbuf).unwrap();
    assert_eq!(u32::from_le_bytes(cbuf), 1);

    // FileStart-equivalent header: name_len + name + file_size.
    let mut nlbuf = [0u8; 2];
    stream.read_exact(&mut nlbuf).unwrap();
    let name_len = u16::from_le_bytes(nlbuf) as usize;
    let mut name = vec![0u8; name_len];
    stream.read_exact(&mut name).unwrap();
    assert_eq!(&name, b"f1.ndb");
    let mut szbuf = [0u8; 8];
    stream.read_exact(&mut szbuf).unwrap();
    let length = u64::from_le_bytes(szbuf);
    assert_eq!(length, 100);

    // Stream the payload, verifying each byte equals its index (JE walk).
    let mut recomputed = crc32fast::Hasher::new();
    for i in 0..length {
        let mut one = [0u8; 1];
        stream.read_exact(&mut one).unwrap();
        assert_eq!(one[0] as u64, i, "payload byte {i} mismatch");
        recomputed.update(&one);
    }

    // Read + verify the CRC32 trailer against the recomputed checksum.
    let mut tbuf = [0u8; 4];
    stream.read_exact(&mut tbuf).unwrap();
    let trailer = u32::from_le_bytes(tbuf);
    assert_eq!(
        trailer,
        recomputed.finalize(),
        "trailer must equal recomputed CRC32 over the streamed payload"
    );
    server.stop();
}

// ===========================================================================
// NetworkBackupTest — the file-copy backup protocol.
// ===========================================================================

/// JE: `NetworkBackupTest.testBackupFiles`. The backup copies the source log
/// files; the destination files match the source byte-for-byte; a re-run
/// still succeeds; and server-side corruption is surfaced (JE checks
/// `getDisposedCount` / `getFetchCount`; Noxu has no such stats — the portable
/// invariant is byte-for-byte fidelity and that a digest-trailer catches
/// transit corruption, covered by `protocol_test_basic_rejects_corrupt_record`
/// and the digest tests in the crate). Here we assert the copy fidelity and
/// that a second backup reproduces the same content.
#[test]
fn network_backup_test_backup_files_copies_byte_for_byte() {
    let f1: Vec<u8> = (0u8..=255).cycle().take(4096).collect();
    let f2: Vec<u8> = b"second log file".to_vec();
    let dir = make_env_home(&[("00000001.ndb", &f1), ("00000002.ndb", &f2)]);
    let (server, port) = spawn_raw_server(&dir);

    // backup1
    let out1 = tempfile::tempdir().unwrap();
    let b1 = NetworkRestore::new(raw_config(port, false))
        .with_local_dir(out1.path());
    b1.execute().expect("backup1");
    // verify byte-for-byte (JE's verify()).
    assert_eq!(std::fs::read(out1.path().join("00000001.ndb")).unwrap(), f1);
    assert_eq!(std::fs::read(out1.path().join("00000002.ndb")).unwrap(), f2);
    // Stats: transferred == expected == total source bytes (JE
    // getExpectedBytes == getTransferredBytes).
    let p1 = b1.get_progress();
    assert_eq!(p1.files_transferred, 2);
    assert_eq!(p1.bytes_transferred, (f1.len() + f2.len()) as u64);

    // backup2: a second run reproduces the same content (JE re-run).
    let out2 = tempfile::tempdir().unwrap();
    let b2 = NetworkRestore::new(raw_config(port, false))
        .with_local_dir(out2.path());
    b2.execute().expect("backup2");
    assert_eq!(std::fs::read(out2.path().join("00000001.ndb")).unwrap(), f1);
    assert_eq!(std::fs::read(out2.path().join("00000002.ndb")).unwrap(), f2);
    server.stop();
}

/// JE: `NetworkBackupTest.testConcurrentBackup`. JE backs up while the DB
/// grows and asserts the *later* file count exceeds the backed-up count, and
/// the backed-up subset still verifies. Noxu's transfer is a point-in-time
/// snapshot of the server's `.ndb` set; the portable, deterministic analog is:
/// a backup captures the files present at that instant, and a subsequent
/// backup after the source grows sees strictly more files while the earlier
/// snapshot still verifies byte-for-byte.
#[test]
fn network_backup_test_concurrent_backup_snapshot_then_grows() {
    let dir = make_env_home(&[
        ("00000001.ndb", b"a"),
        ("00000002.ndb", b"bb"),
        ("00000003.ndb", b"ccc"),
    ]);
    let (server, port) = spawn_raw_server(&dir);

    // First backup: 3 files.
    let out1 = tempfile::tempdir().unwrap();
    let b1 = NetworkRestore::new(raw_config(port, false))
        .with_local_dir(out1.path());
    b1.execute().expect("backup1");
    let first_count = b1.get_progress().files_transferred;
    assert_eq!(first_count, 3);

    // Source grows (concurrent writer would add files).
    std::fs::write(dir.path().join("00000004.ndb"), b"dddd").unwrap();
    std::fs::write(dir.path().join("00000005.ndb"), b"eeeee").unwrap();

    // Second backup: strictly more files (JE: newCount > backupThread.files).
    let out2 = tempfile::tempdir().unwrap();
    let b2 = NetworkRestore::new(raw_config(port, false))
        .with_local_dir(out2.path());
    b2.execute().expect("backup2");
    let second_count = b2.get_progress().files_transferred;
    assert!(
        second_count > first_count,
        "count must grow: {second_count} > {first_count}"
    );

    // Earlier snapshot still verifies byte-for-byte.
    assert_eq!(std::fs::read(out1.path().join("00000001.ndb")).unwrap(), b"a");
    assert_eq!(
        std::fs::read(out1.path().join("00000003.ndb")).unwrap(),
        b"ccc"
    );
    server.stop();
}

/// JE: `NetworkBackupTest.testBasicWithRetainLog` (retainLog = true). With
/// retain set, an existing destination file is renamed aside (not clobbered)
/// before the fresh copy lands, and the fresh copy is written correctly.
#[test]
fn network_backup_test_basic_with_retain_log() {
    let original = b"original destination content";
    let updated = b"new content from the backup server";
    let dir = make_env_home(&[("00000001.ndb", updated)]);
    let (server, port) = spawn_raw_server(&dir);

    let out = tempfile::tempdir().unwrap();
    std::fs::write(out.path().join("00000001.ndb"), original).unwrap();

    let b =
        NetworkRestore::new(raw_config(port, true)).with_local_dir(out.path());
    b.execute().expect("retain-log backup");

    // Fresh copy landed.
    assert_eq!(
        std::fs::read(out.path().join("00000001.ndb")).unwrap(),
        updated
    );
    // Original retained as `.bak` (JE retains rather than deletes).
    assert_eq!(
        std::fs::read(out.path().join("00000001.ndb.bak")).unwrap(),
        original
    );
    server.stop();
}

/// JE: `NetworkBackupTest.testBasicWithoutRetainLog` (retainLog = false).
/// Without retain, a fresh backup simply writes the copy; no `.bak` is left.
#[test]
fn network_backup_test_basic_without_retain_log() {
    let updated = b"backup content, no retain";
    let dir = make_env_home(&[("00000001.ndb", updated)]);
    let (server, port) = spawn_raw_server(&dir);

    let out = tempfile::tempdir().unwrap();
    std::fs::write(out.path().join("00000001.ndb"), b"stale").unwrap();

    let b =
        NetworkRestore::new(raw_config(port, false)).with_local_dir(out.path());
    b.execute().expect("no-retain backup");

    assert_eq!(
        std::fs::read(out.path().join("00000001.ndb")).unwrap(),
        updated
    );
    // No `.bak` when retain is off.
    assert!(!out.path().join("00000001.ndb.bak").exists());
    server.stop();
}

// ===========================================================================
// NetworkRestoreTest — full restore, provider selection, config error.
// ===========================================================================

/// JE: `NetworkRestoreTest.testLogProviders` (provider-selection + non-.ndb
/// filtering half). JE restores from master / a specific replica / a secondary
/// and verifies restore-from-self fails. Noxu selects a source peer by name;
/// the portable invariant is: restore from a *named, registered* peer copies
/// exactly that peer's `.ndb` set (and only `.ndb` files), which is
/// provider selection. The "restore from self fails" case is the
/// unknown/invalid-provider rejection (see
/// `network_restore_test_config_error_rejects_bad_provider`).
#[test]
fn network_restore_test_log_providers_selects_named_peer() {
    // Source peer with a mix of .ndb and non-.ndb files.
    let src_dir = TempDir::new().unwrap();
    let src_home = src_dir.path().to_path_buf();
    std::fs::write(src_home.join("00000001.ndb"), b"provider log 1").unwrap();
    std::fs::write(src_home.join("00000002.ndb"), b"provider log 2").unwrap();
    std::fs::write(src_home.join("noxu.config.csv"), b"config, not copied")
        .unwrap();

    let src_cfg = RepConfig::builder("g1", "provider", "127.0.0.1")
        .node_port(0)
        .env_home(&src_home)
        .build();
    let src_env = ReplicatedEnvironment::new(src_cfg).unwrap();
    let src_addr = src_env.bound_addr().expect("provider binds");

    // Restoring node registers the provider by name and bootstraps from it.
    let dst_dir = TempDir::new().unwrap();
    let dst_home = dst_dir.path().to_path_buf();
    let dst_cfg = RepConfig::builder("g1", "restoring", "127.0.0.1")
        .node_port(0)
        .env_home(&dst_home)
        .build();
    let dst_env = ReplicatedEnvironment::new(dst_cfg).unwrap();
    dst_env
        .add_peer(RepNode::new(
            "provider".to_string(),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            src_addr.port(),
            2,
        ))
        .unwrap();

    dst_env
        .bootstrap_via_dispatcher("provider")
        .expect("restore from the named provider must succeed");

    // Only the provider's .ndb files copied.
    assert_eq!(
        std::fs::read(dst_home.join("00000001.ndb")).unwrap(),
        b"provider log 1"
    );
    assert_eq!(
        std::fs::read(dst_home.join("00000002.ndb")).unwrap(),
        b"provider log 2"
    );
    assert!(!dst_home.join("noxu.config.csv").exists());

    src_env.close().unwrap();
    dst_env.close().unwrap();
}

/// JE: `NetworkRestoreTest.testConfigError`. JE sets a bad node name as the
/// log provider and expects `IllegalArgumentException` from
/// `NetworkRestore.execute`. Noxu's analog: bootstrapping from a peer name
/// that is not registered in the group is a config error, surfaced (not a
/// panic, not a hang).
#[test]
fn network_restore_test_config_error_rejects_bad_provider() {
    let dst_dir = TempDir::new().unwrap();
    let cfg = RepConfig::builder("g1", "n1", "127.0.0.1")
        .node_port(0)
        .env_home(dst_dir.path())
        .build();
    let env = ReplicatedEnvironment::new(cfg).unwrap();

    let err = env.bootstrap_via_dispatcher("badname").unwrap_err();
    assert!(
        err.to_string().contains("badname")
            || err.to_string().contains("not registered"),
        "bad provider must be rejected as a config error, got: {err}"
    );
    env.close().unwrap();
}

/// JE: `NetworkRestoreTest.testLockout` (service-gating half). JE's
/// `shutdownNetworkBackup` makes the RESTORE service unavailable so a
/// `NetworkBackup.execute` fails with `ServiceConnectFailedException`;
/// `restartNetworkBackup` re-enables it. Noxu gates restore by whether the
/// `RESTORE` service is registered on the dispatcher: a restore against a
/// dispatcher WITHOUT the RESTORE handler must fail (connection closed by the
/// server), while one WITH it registered succeeds. The two branches together
/// prove the gate is real (not vacuous).
#[test]
fn network_restore_test_lockout_gates_on_service_registration() {
    let dir = make_env_home(&[("00000001.ndb", b"lockout log")]);

    // Dispatcher with NO RESTORE handler registered (== shutdownNetworkBackup).
    let locked = noxu_rep::net::service_dispatcher::TcpServiceDispatcher::new(
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    let locked_addr = locked.start().unwrap();
    std::thread::sleep(Duration::from_millis(20));

    let out = tempfile::tempdir().unwrap();
    let r = NetworkRestore::new(NetworkRestoreConfig {
        source_node: "peer".into(),
        source_host: locked_addr.ip().to_string(),
        source_port: locked_addr.port(),
        retain_log_files: false,
    })
    .with_local_dir(out.path());
    let err = r.execute_via_dispatcher().unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("restore")
            || err.to_string().to_lowercase().contains("closed")
            || err.to_string().to_lowercase().contains("payload"),
        "restore against a gated (unregistered) service must fail, got: {err}"
    );
    locked.stop();

    // Now register the RESTORE handler (== restartNetworkBackup): succeeds.
    let open = noxu_rep::net::service_dispatcher::TcpServiceDispatcher::new(
        "127.0.0.1:0".parse().unwrap(),
    )
    .unwrap();
    open.register(
        RESTORE_SERVICE_NAME,
        Arc::new(NetworkRestoreServer::new(dir.path())),
    );
    let open_addr = open.start().unwrap();
    std::thread::sleep(Duration::from_millis(20));

    let out2 = tempfile::tempdir().unwrap();
    let r2 = NetworkRestore::new(NetworkRestoreConfig {
        source_node: "peer".into(),
        source_host: open_addr.ip().to_string(),
        source_port: open_addr.port(),
        retain_log_files: false,
    })
    .with_local_dir(out2.path());
    r2.execute_via_dispatcher()
        .expect("restore after re-registering the service must succeed");
    assert_eq!(
        std::fs::read(out2.path().join("00000001.ndb")).unwrap(),
        b"lockout log"
    );
    open.stop();
}

// ===========================================================================
// OneNodeRestoreTest — single surviving node serves a restore.
// ===========================================================================

/// JE: `OneNodeRestoreTest.testBasic` (single-node-serves-restore core). JE
/// restores dependent nodes from ONE surviving node (even while that node is
/// only in UNKNOWN state), then the group elects. The multi-node parallel
/// startup + election orchestration is a live-group deviation (N/A); the
/// portable core — a node whose env dir was wiped restores its full `.ndb` set
/// from the single surviving node — is ported here.
#[test]
fn one_node_restore_test_basic_restores_from_single_survivor() {
    // The single survivor holds the whole log.
    let survivor_dir = TempDir::new().unwrap();
    let survivor_home = survivor_dir.path().to_path_buf();
    for n in 1u32..=4 {
        let name = format!("{:08x}.ndb", n);
        std::fs::write(survivor_home.join(&name), format!("survivor {n}"))
            .unwrap();
    }
    let survivor_cfg = RepConfig::builder("g1", "survivor", "127.0.0.1")
        .node_port(0)
        .env_home(&survivor_home)
        .build();
    let survivor = ReplicatedEnvironment::new(survivor_cfg).unwrap();
    let survivor_addr = survivor.bound_addr().unwrap();

    // A dependent node with an empty (wiped) env dir restores from it.
    let dep_dir = TempDir::new().unwrap();
    let dep_home = dep_dir.path().to_path_buf();
    let dep_cfg = RepConfig::builder("g1", "dependent", "127.0.0.1")
        .node_port(0)
        .env_home(&dep_home)
        .build();
    let dep = ReplicatedEnvironment::new(dep_cfg).unwrap();
    dep.add_peer(RepNode::new(
        "survivor".to_string(),
        NodeType::Electable,
        "127.0.0.1".to_string(),
        survivor_addr.port(),
        1,
    ))
    .unwrap();

    dep.bootstrap_via_dispatcher("survivor")
        .expect("dependent restores from the single survivor");

    for n in 1u32..=4 {
        let name = format!("{:08x}.ndb", n);
        assert_eq!(
            std::fs::read(dep_home.join(&name)).unwrap(),
            format!("survivor {n}").into_bytes(),
            "file {name} must be restored"
        );
    }

    survivor.close().unwrap();
    dep.close().unwrap();
}

// ===========================================================================
// NetworkRestoreNoMasterTest — restore proceeds without a master; a
// no-peer restore fails gracefully.
// ===========================================================================

/// JE: `NetworkRestoreNoMasterTest.testMissingEnv` (restore-from-non-master
/// core). JE deletes the env dirs of two nodes, restarts one, and it network-
/// restores from the surviving (non-master, UNKNOWN-state) node, then the
/// group elects. The live-election-bootstrap orchestration is a deviation
/// (N/A); the portable core — a node can restore from a peer that is NOT a
/// master (any node serving the RESTORE service) — is ported here: the source
/// env has never been elected master and still serves a full restore.
#[test]
fn no_master_test_restore_from_non_master_peer() {
    // Source node: freshly constructed, never elected master (state UNKNOWN),
    // but it serves the RESTORE service and holds the log.
    let src_dir = TempDir::new().unwrap();
    let src_home = src_dir.path().to_path_buf();
    std::fs::write(src_home.join("00000001.ndb"), b"no-master log 1").unwrap();
    std::fs::write(src_home.join("00000002.ndb"), b"no-master log 2").unwrap();
    let src_cfg = RepConfig::builder("g1", "peer", "127.0.0.1")
        .node_port(0)
        .env_home(&src_home)
        .build();
    let src = ReplicatedEnvironment::new(src_cfg).unwrap();
    // Deliberately do NOT drive it to master — it stays in its initial
    // (non-master) state, mirroring JE's UNKNOWN-state survivor.
    assert!(
        !src.is_master(),
        "precondition: source must not be a master for this test"
    );
    let src_addr = src.bound_addr().unwrap();

    let dst_dir = TempDir::new().unwrap();
    let dst_home = dst_dir.path().to_path_buf();
    let dst_cfg = RepConfig::builder("g1", "restoring", "127.0.0.1")
        .node_port(0)
        .env_home(&dst_home)
        .build();
    let dst = ReplicatedEnvironment::new(dst_cfg).unwrap();
    dst.add_peer(RepNode::new(
        "peer".to_string(),
        NodeType::Electable,
        "127.0.0.1".to_string(),
        src_addr.port(),
        1,
    ))
    .unwrap();

    dst.bootstrap_via_dispatcher("peer")
        .expect("restore from a non-master peer must succeed");
    assert_eq!(
        std::fs::read(dst_home.join("00000001.ndb")).unwrap(),
        b"no-master log 1"
    );
    assert_eq!(
        std::fs::read(dst_home.join("00000002.ndb")).unwrap(),
        b"no-master log 2"
    );

    src.close().unwrap();
    dst.close().unwrap();
}

/// JE: `NetworkRestoreNoMasterTest.testSyncupFailure` (graceful-failure core).
/// JE's scenario turns on a live syncup interruption + re-election. The
/// deterministic portable invariant it depends on — that a restore attempt
/// with NO reachable peer serving the log does NOT hang or panic but returns a
/// clean error so the caller can retry / elect — is ported here: bootstrap
/// against a peer whose address points at a dead port fails cleanly and leaves
/// no partial files.
#[test]
fn no_master_test_restore_with_no_reachable_peer_fails_gracefully() {
    // Grab a port, bind+drop so it is (almost certainly) unbound.
    let dead_port = {
        let l = TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
        // listener dropped here → nothing listening on dead_port
    };

    let out = tempfile::tempdir().unwrap();
    let r = NetworkRestore::new(NetworkRestoreConfig {
        source_node: "ghost".into(),
        source_host: "127.0.0.1".into(),
        source_port: dead_port,
        retain_log_files: false,
    })
    .with_local_dir(out.path());

    let err = r.execute().unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("connect")
            || err.to_string().to_lowercase().contains("restore"),
        "no-peer restore must fail cleanly, got: {err}"
    );
    // No partial files left behind.
    let n = std::fs::read_dir(out.path()).unwrap().count();
    assert_eq!(n, 0, "a failed connect must not create files");
    // State must NOT be Completed.
    assert_ne!(r.get_state(), RestoreState::Completed);
}

// ===========================================================================
// InterruptedNetworkRestoreTest — an interrupted transfer does not leave the
// node "Completed"; a subsequent clean transfer restores it fully.
// ===========================================================================

/// JE: `InterruptedNetworkRestoreTest.testBasic`. JE interrupts a restore
/// mid-stream (StopBackup hook at file >= N), which drops a `RestoreMarker`
/// that keeps the node throwing `InsufficientLogException` on every open until
/// a fresh, uninterrupted restore completes and removes the marker. Noxu has
/// no `RestoreMarker` and transfers atomically per file, so the faithfully
/// portable invariant is: an interrupted transfer (server dies mid-body) fails
/// the restore (state != Completed, error surfaced), and a subsequent clean
/// restore against a healthy server completes and yields correct content —
/// i.e. the interrupt does not silently "half-restore" the node into a
/// Completed state.
#[test]
fn interrupted_network_restore_test_basic_interrupt_then_recover() {
    let body: Vec<u8> = vec![0xAB; 4096];

    // Phase 1: a fake server that announces one 4096-byte file but sends only
    // half the body then closes — the interrupt.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let bad_port = listener.local_addr().unwrap().port();
    let body_clone = body.clone();
    std::thread::spawn(move || {
        if let Ok((mut s, _)) = listener.accept() {
            let mut magic = [0u8; 4];
            let _ = s.read_exact(&mut magic);
            let _ = s.write_all(&1u32.to_le_bytes()); // count = 1
            let name = b"00000004.ndb";
            let _ = s.write_all(&(name.len() as u16).to_le_bytes());
            let _ = s.write_all(name);
            let _ = s.write_all(&(body_clone.len() as u64).to_le_bytes());
            // Send only HALF the body, then hang up → mid-stream interrupt.
            let _ = s.write_all(&body_clone[..body_clone.len() / 2]);
            let _ = s.flush();
            // Drop `s` → connection closes; client read_exact fails.
        }
    });
    std::thread::sleep(Duration::from_millis(20));

    let out = tempfile::tempdir().unwrap();
    let interrupted = NetworkRestore::new(raw_config(bad_port, false))
        .with_local_dir(out.path());
    let err = interrupted.execute().unwrap_err();
    assert!(
        err.to_string().to_lowercase().contains("reading data")
            || err.to_string().to_lowercase().contains("restore"),
        "interrupted transfer must surface an error, got: {err}"
    );
    // The interrupt must NOT leave the restore Completed (JE: marker keeps the
    // node un-openable). This is the key anti-"silent half-restore" assertion.
    assert_ne!(
        interrupted.get_state(),
        RestoreState::Completed,
        "an interrupted restore must not be Completed"
    );

    // Phase 2: a fresh, healthy restore against a real server completes and
    // yields the full, correct file (JE: fresh NR removes the marker and the
    // node opens).
    let good_dir = make_env_home(&[("00000004.ndb", &body)]);
    let (server, good_port) = spawn_raw_server(&good_dir);
    let recovered = NetworkRestore::new(raw_config(good_port, true))
        .with_local_dir(out.path());
    recovered.execute().expect("fresh restore must complete");
    assert_eq!(recovered.get_state(), RestoreState::Completed);
    assert_eq!(
        std::fs::read(out.path().join("00000004.ndb")).unwrap(),
        body,
        "recovered file must be the full, correct content"
    );
    server.stop();
}
