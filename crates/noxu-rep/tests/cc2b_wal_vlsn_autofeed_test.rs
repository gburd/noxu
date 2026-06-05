//! C-C2b integration tests: WAL-scanner auto-feed via VLSN-tagged entries.
//!
//! These tests close the qualification gap identified in the C-C2b design
//! doc (`docs/src/internal/deferred-blocker-designs-2026-06.md`).
//!
//! ## What is tested
//!
//! 1. **Convergence test** (`test_wal_scanner_autofeed_convergence`): the
//!    master performs real `EnvironmentImpl` commits (via `log_txn_commit`);
//!    the `EnvironmentLogScanner`-backed `FeederRunner` picks up the
//!    VLSN-tagged WAL entries and streams them to the replica automatically —
//!    no `replicate_entry` call needed.
//!
//! 2. **Standalone-no-VLSN regression test**
//!    (`test_standalone_env_writes_no_vlsn_header`): a non-replicated
//!    `EnvironmentImpl` (no `set_replication_vlsn_counter` called) writes
//!    14-byte headers.  The VLSN_PRESENT_MASK and REPLICATED_MASK bits must
//!    NOT be set.  Format regression proof.
//!
//! ## Fail-before / pass-after
//!
//! **Before this branch** (`origin/main`):
//! - `log_internal` always writes a 14-byte header; no entry ever has the
//!   `VLSN_PRESENT_MASK | REPLICATED_MASK` bits set.
//! - `EnvironmentLogScanner::next_entry` returns `None` on every call.
//! - The convergence test would hang waiting for entries and then time out.
//!
//! **After this branch**:
//! - `log_with_vlsn` writes the 22-byte header with the VLSN embedded.
//! - `EnvironmentImpl::log_txn_commit` uses `log_with_vlsn` when a VLSN
//!   counter is installed.
//! - `EnvironmentLogScanner` finds the entries and feeds them.
//! - The convergence test passes (entries received ≥ N_COMMITS).
//!
//! ## Coverage
//!
//! | Path | Exercised by |
//! |------|-------------|
//! | `LogManager::log_with_vlsn` 22-byte header write | both tests |
//! | `EnvironmentImpl::set_replication_vlsn_counter` | convergence test |
//! | `log_txn_commit` VLSN branch | convergence test |
//! | `EnvironmentLogScanner::next_entry` VLSN entry found | convergence test |
//! | `spawn_feeder_runner` WAL-scanner path | convergence test |
//! | Standalone 14-byte header invariant | standalone test |

use std::sync::Arc;
use std::time::{Duration, Instant};

use noxu_dbi::EnvironmentImpl;
use noxu_rep::net::channel::LocalChannelPair;
use noxu_rep::{NodeType, RepConfig, RepNode, ReplicatedEnvironment};

// Number of real commits to perform in the convergence test.
const N_COMMITS: usize = 5;

// How long to wait for all N_COMMITS to arrive on the replica side.
const RECV_TIMEOUT: Duration = Duration::from_secs(10);

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn master_cfg(
    group: &str,
    name: &str,
    env_home: &std::path::Path,
) -> RepConfig {
    RepConfig::builder(group, name, "127.0.0.1")
        .node_port(0)
        .env_home(env_home.to_path_buf())
        .build()
}

/// Parse one FeederRunner wire frame from `raw_frame`.
///
/// Frame layout (all LE):
///   `[vlsn:8][type:1][payload_len:4][crc32:4][payload:payload_len]`
fn parse_frame(frame: &[u8]) -> (u64, u8, Vec<u8>) {
    assert!(frame.len() >= 17, "frame too short: {} bytes", frame.len());
    let vlsn = u64::from_le_bytes(frame[0..8].try_into().unwrap());
    let entry_type = frame[8];
    let payload_len =
        u32::from_le_bytes(frame[9..13].try_into().unwrap()) as usize;
    let payload = frame[17..17 + payload_len].to_vec();
    (vlsn, entry_type, payload)
}

// ─── Test 1 (CONVERGENCE) ────────────────────────────────────────────────────
//
// FAILS on origin/main (scanner returns None; no entries arrive).
// PASSES with this branch (VLSN-tagged headers; scanner finds entries).
// ─────────────────────────────────────────────────────────────────────────────

/// WAL-scanner convergence: master commits via EnvironmentImpl; replica
/// receives entries through EnvironmentLogScanner auto-feed.
///
/// ## Fail-before (origin/main)
///
/// `log_internal` always uses `MIN_HEADER_SIZE` (14 bytes); the
/// `VLSN_PRESENT_MASK` / `REPLICATED_MASK` flags are never set.
/// `EnvironmentLogScanner::next_entry` returns `None` on every call.
/// The replica receives 0 frames; the assertion
/// `assert!(received.len() >= N_COMMITS)` fails.
///
/// ## Pass-after (this branch)
///
/// `log_with_vlsn` writes the 22-byte header; `log_txn_commit` calls it
/// when a VLSN counter is installed.  `EnvironmentLogScanner::next_entry`
/// finds the tagged entries and the FeederRunner streams them over the
/// channel.
#[test]
fn test_wal_scanner_autofeed_convergence() {
    // ── 1. Create a real EnvironmentImpl in a temp directory ──────────────
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let env_impl = Arc::new(
        EnvironmentImpl::new(tmp.path(), false, true)
            .expect("EnvironmentImpl::new"),
    );

    // ── 2. Create the ReplicatedEnvironment (master side) ─────────────────
    let cfg = master_cfg("cc2b_conv", "master", tmp.path());
    let rep_env = Arc::new(
        ReplicatedEnvironment::new(cfg).expect("ReplicatedEnvironment::new"),
    );
    rep_env
        .add_peer(RepNode::new(
            "replica".to_string(),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            0,
            2,
        ))
        .unwrap();

    // ── 3. Wire the env — installs VLSN counter on env_impl ──────────────
    rep_env.with_environment(Arc::clone(&env_impl));

    // ── 4. Set up the channel pair (master ↔ replica) ─────────────────────
    let pair = LocalChannelPair::new();
    let chan_master: Arc<dyn noxu_rep::net::Channel> = Arc::new(pair.channel_a);
    let chan_replica: Arc<dyn noxu_rep::net::Channel> =
        Arc::new(pair.channel_b);

    // ── 5. Register feeder channel + become_master ────────────────────────
    //    become_master calls spawn_feeder_runner, which sees env_impl is set
    //    and creates an EnvironmentLogScanner-backed FeederRunner.
    rep_env.register_feeder_channel("replica".to_string(), chan_master);
    rep_env.become_master(1).expect("become_master");

    // ── 6. Perform real EnvironmentImpl commits ───────────────────────────
    //    Each call to log_txn_commit (with flush=true) writes a VLSN-tagged
    //    22-byte header to the WAL, flushes it to disk, and increments the
    //    shared VLSN counter.  The EnvironmentLogScanner sees these entries.
    for txn_id in 1..=(N_COMMITS as i64) {
        env_impl
            .log_txn_commit(txn_id, /*fsync=*/ false, /*flush=*/ true)
            .expect("log_txn_commit");
    }

    // ── 7. Receive frames on the replica side (bounded timeout) ───────────
    let mut received: Vec<(u64, u8, Vec<u8>)> = Vec::new();
    let deadline = Instant::now() + RECV_TIMEOUT;

    while received.len() < N_COMMITS && Instant::now() < deadline {
        match chan_replica.receive(Duration::from_millis(200)) {
            Ok(Some(frame)) => received.push(parse_frame(&frame)),
            Ok(None) => {} // timeout, keep polling
            Err(_) => break,
        }
    }

    // Close the channel so the FeederRunner exits.
    drop(chan_replica);
    let _ = rep_env.close();

    // ── 8. Assert convergence ─────────────────────────────────────────────
    assert!(
        received.len() >= N_COMMITS,
        "Expected at least {} entries from WAL-scanner auto-feed, got {} \
         (FAIL-BEFORE if 0: scanner finds no VLSN-tagged entries on origin/main)",
        N_COMMITS,
        received.len(),
    );

    // Verify all VLSNs are positive and strictly increasing.
    let mut prev_vlsn = 0u64;
    for (vlsn, _etype, _payload) in &received {
        assert!(
            *vlsn > prev_vlsn,
            "VLSNs must be strictly increasing; got {} after {}",
            vlsn,
            prev_vlsn
        );
        prev_vlsn = *vlsn;
    }
}

// ─── Test 2 (STANDALONE FORMAT REGRESSION) ───────────────────────────────────
//
// Asserts that a non-replicated EnvironmentImpl still writes 14-byte headers
// with no VLSN bits set.  This proves the `log()` path is byte-unchanged.
// ─────────────────────────────────────────────────────────────────────────────

/// Standalone env writes 14-byte headers with no VLSN bits.
///
/// The `log()` (non-replicated) path must be byte-unchanged.  Verify by:
/// 1. Opening a fresh `EnvironmentImpl` WITHOUT calling
///    `set_replication_vlsn_counter`.
/// 2. Calling `log_txn_commit` (flush=true).
/// 3. Reading back the raw bytes from the WAL file.
/// 4. Asserting the flags byte has neither `VLSN_PRESENT_MASK` (0x08)
///    nor `REPLICATED_MASK` (0x20) set, and the header is exactly 14 bytes.
#[test]
fn test_standalone_env_writes_no_vlsn_header() {
    use noxu_log::entry_header::MIN_HEADER_SIZE;
    use noxu_log::file_manager::FileManager;

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let env =
        EnvironmentImpl::new(tmp.path(), false, true).expect("EnvironmentImpl");

    // Standalone commit — no VLSN counter installed.
    env.log_txn_commit(42, false, true).expect("log_txn_commit");

    // Read back the first log entry.  The file header occupies
    // `file_header::on_disk_size(LOG_VERSION)` bytes; the first log entry
    // starts immediately after.
    let fm = FileManager::new(tmp.path(), true, 64 * 1024 * 1024, 8)
        .expect("FileManager");

    let file_hdr_size =
        fm.file_header_size_for(0).expect("file_header_size_for");

    // Read MIN_HEADER_SIZE + some slack bytes starting at the first entry.
    let mut buf = vec![0u8; 32];
    let n = fm
        .read_from_file(0, file_hdr_size as u64, &mut buf)
        .expect("read_from_file");

    assert!(
        n >= MIN_HEADER_SIZE,
        "expected at least {} bytes, got {}",
        MIN_HEADER_SIZE,
        n
    );

    let flags = buf[5]; // flags byte in the on-disk header

    // VLSN_PRESENT_MASK = 0x08; REPLICATED_MASK = 0x20
    assert_eq!(
        flags & 0x08,
        0,
        "standalone entry must NOT have VLSN_PRESENT_MASK (0x08) set; \
         flags={:#04x}",
        flags
    );
    assert_eq!(
        flags & 0x20,
        0,
        "standalone entry must NOT have REPLICATED_MASK (0x20) set; \
         flags={:#04x}",
        flags
    );

    // Verify the header parser agrees the entry is non-replicated.
    let hdr = noxu_log::entry_header::LogEntryHeader::read_from_log(
        &buf[..MIN_HEADER_SIZE],
        noxu_util::Lsn::new(0, file_hdr_size as u32),
    )
    .expect("read_from_log");
    assert!(
        !hdr.vlsn_present(),
        "standalone header must not have vlsn_present"
    );
    assert!(
        !hdr.replicated(),
        "standalone header must not have replicated flag"
    );
    assert_eq!(
        hdr.size(),
        MIN_HEADER_SIZE,
        "standalone header size must be {} bytes",
        MIN_HEADER_SIZE
    );
}

// ─── Test 3 (LOG_WITH_VLSN HEADER FORMAT) ────────────────────────────────────
//
// Directly asserts that LogManager::log_with_vlsn writes a 22-byte header
// with the expected flags and VLSN value.
// ─────────────────────────────────────────────────────────────────────────────

/// `log_with_vlsn` writes a 22-byte header with VLSN_PRESENT + REPLICATED
/// flags and the correct VLSN value at offset 14.
#[test]
fn test_log_with_vlsn_header_format() {
    use noxu_log::LogEntryType;
    use noxu_log::entry_header::{LogEntryHeader, MAX_HEADER_SIZE};
    use noxu_log::file_manager::FileManager;

    let tmp = tempfile::TempDir::new().expect("tempdir");
    let env =
        EnvironmentImpl::new(tmp.path(), false, true).expect("EnvironmentImpl");

    let lm = env.get_log_manager().expect("log_manager");
    let test_vlsn: u64 = 77;

    // Write a VLSN-tagged entry directly.
    lm.log_with_vlsn(
        LogEntryType::TxnCommit,
        &[0xAA, 0xBB, 0xCC],
        test_vlsn,
        /*flush=*/ true,
        /*fsync=*/ false,
    )
    .expect("log_with_vlsn");

    // Read back raw bytes.
    let fm = FileManager::new(tmp.path(), true, 64 * 1024 * 1024, 8)
        .expect("FileManager");
    let file_hdr_size =
        fm.file_header_size_for(0).expect("file_header_size_for");

    let mut buf = vec![0u8; MAX_HEADER_SIZE + 16];
    let n = fm
        .read_from_file(0, file_hdr_size as u64, &mut buf)
        .expect("read_from_file");

    assert!(
        n >= MAX_HEADER_SIZE,
        "expected >= {} bytes, got {}",
        MAX_HEADER_SIZE,
        n
    );

    // Flags byte at offset 5: REPLICATED_MASK=0x20, VLSN_PRESENT_MASK=0x08.
    let flags = buf[5];
    assert_ne!(
        flags & 0x08,
        0,
        "VLSN_PRESENT_MASK must be set; flags={:#04x}",
        flags
    );
    assert_ne!(
        flags & 0x20,
        0,
        "REPLICATED_MASK must be set; flags={:#04x}",
        flags
    );

    // VLSN at offset 14, 8-byte little-endian i64.
    let raw_vlsn = i64::from_le_bytes(buf[14..22].try_into().unwrap());
    assert_eq!(
        raw_vlsn as u64, test_vlsn,
        "VLSN at offset 14 must match the written value"
    );

    // LogEntryHeader parser must agree.
    let hdr = LogEntryHeader::read_from_log(
        &buf[..MAX_HEADER_SIZE],
        noxu_util::Lsn::new(0, file_hdr_size as u32),
    )
    .expect("read_from_log");
    assert!(hdr.vlsn_present());
    assert!(hdr.replicated());
    assert_eq!(hdr.size(), MAX_HEADER_SIZE);
    assert_eq!(hdr.vlsn(), Some(noxu_util::vlsn::Vlsn::new(test_vlsn as i64)));
}
