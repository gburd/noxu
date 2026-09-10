//! Gap C: periodic DTVLSN flusher daemon.
//!
//! Port of JE `FeederManager.DTVLSNFlusher` (`FeederManager.java`
//! ~lines 930-1042): "Writes a null (no modifications) commit record when
//! it detects that the DTVLSN is ahead of the persistent DTVLSN and needs
//! to be updated... without this mechanism, the in-memory DTVLSN would
//! always be ahead of the persisted VLSN".
//!
//! ## Fail-before / pass-after
//!
//! **Before this branch**: `ReplicatedEnvironment::open` never spawns any
//! daemon that persists the DTVLSN independently of ordinary application
//! commits. A master that computes a DTVLSN (via `update_dtvlsn_from_feeders`
//! / ack processing) but then goes idle (no further application commits)
//! never writes that value to the WAL — `EnvironmentImpl::log_null_txn_commit`
//! did not exist, and nothing called it on a timer. This test asserts that,
//! after the in-memory DTVLSN advances and the master then goes idle, a
//! `TxnCommit` WAL entry carrying that DTVLSN appears **without any further
//! application-level commit**. On `origin/main` this never happens (the WAL
//! contains zero `TxnCommit` entries at all, since the test performs no data
//! writes), so the test fails.
//!
//! **After this branch**: `start_dtvlsn_flush_daemon` (spawned from `open()`)
//! observes the stable, unpersisted DTVLSN and calls
//! `EnvironmentImpl::log_null_txn_commit`, writing exactly the record this
//! test looks for.

use std::sync::Arc;
use std::time::{Duration, Instant};

use noxu_dbi::EnvironmentImpl;
use noxu_log::LogEntryType;
use noxu_log::entry::TxnEndEntry;
use noxu_log::entry_header::{
    LogEntryHeader, MAX_HEADER_SIZE, MIN_HEADER_SIZE,
};
use noxu_log::file_manager::FileManager;
use noxu_rep::{RepConfig, ReplicatedEnvironment};
use noxu_util::Lsn;

/// Scan every log file in `env_home` for `TxnCommit` entries; return the
/// `dtvlsn` sequence field of every one found, in file order.
fn scan_txn_commit_dtvlsns(env_home: &std::path::Path) -> Vec<i64> {
    let fm = FileManager::new(env_home, true, 64 * 1024 * 1024, 8)
        .expect("FileManager::new");
    let mut found = Vec::new();

    let file_nums = fm.list_file_numbers().expect("list_file_numbers");
    for file_num in file_nums {
        let file_len = fm.get_file_length(file_num).expect("get_file_length");
        let mut offset = fm
            .file_header_size_for(file_num)
            .expect("file_header_size_for") as u64;

        while offset < file_len {
            let mut hdr_buf = [0u8; MAX_HEADER_SIZE];
            let n = match fm.read_from_file(file_num, offset, &mut hdr_buf) {
                Ok(n) if n >= MIN_HEADER_SIZE => n,
                _ => break,
            };
            // Zero-fill region past the last written entry.
            if hdr_buf[4] == 0 {
                break;
            }
            let lsn = Lsn::new(file_num, offset as u32);
            let hdr = match LogEntryHeader::read_from_log(
                &hdr_buf[..n.min(MAX_HEADER_SIZE)],
                lsn,
            ) {
                Ok(h) => h,
                Err(_) => break,
            };
            let header_size = hdr.size();
            let item_size = hdr.item_size() as usize;
            let entry_size = header_size + item_size;

            if hdr.entry_type() == LogEntryType::TxnCommit {
                let mut payload = vec![0u8; item_size];
                let pn = fm
                    .read_from_file(
                        file_num,
                        offset + header_size as u64,
                        &mut payload,
                    )
                    .expect("read payload");
                if pn == item_size
                    && let Ok(e) = TxnEndEntry::read_from_log(&payload)
                {
                    found.push(e.dtvlsn.sequence());
                }
            }

            offset += entry_size as u64;
        }
    }

    found
}

fn master_cfg(
    group: &str,
    name: &str,
    env_home: &std::path::Path,
) -> RepConfig {
    RepConfig::builder(group, name, "127.0.0.1")
        .node_port(0)
        .env_home(env_home.to_path_buf())
        // Small heartbeat so the daemon's derived tick interval (a quarter
        // of the heartbeat, clamped to >= 25ms) is fast in this test without
        // going below the flake-avoidance floor called out in the task
        // brief (timers must be generous, not 100ms-tight).
        .heartbeat_interval(Duration::from_millis(200))
        .build()
}

/// Directly drives the DTVLSN up via a single-node "majority" (no peers ==
/// durable_ack_count == 0 branch of `update_dtvlsn_from_feeders`, which sets
/// DTVLSN to the current VLSN), then asserts the flush daemon persists it to
/// the WAL via a null TxnCommit within a generous timeout, with no further
/// application commit.
#[test]
fn dtvlsn_flush_daemon_persists_without_application_commit() {
    let tmp = tempfile::TempDir::new().expect("tempdir");
    let env_home = tmp.path().to_path_buf();

    let env_impl = Arc::new(
        EnvironmentImpl::new(&env_home, false, true)
            .expect("EnvironmentImpl::new"),
    );

    let cfg = master_cfg("dtvlsn_flush", "master", &env_home);
    let rep_env =
        ReplicatedEnvironment::open(cfg).expect("ReplicatedEnvironment::open");

    rep_env.with_environment(Arc::clone(&env_impl));
    rep_env.become_master(1).expect("become_master");

    // No peers registered: `update_dtvlsn_from_feeders`'s single-node branch
    // sets the DTVLSN to `get_current_vlsn()` directly. Advance the VLSN via
    // a real application commit (through `log_txn_commit`, which increments
    // the shared VLSN counter installed by `with_environment`) so there is a
    // non-zero VLSN to become durable. `log_txn_commit` bumps the shared
    // WAL-VLSN counter but does not itself register the mapping into the
    // master's own `vlsn_index` (that registration normally happens when a
    // live `EnvironmentLogScanner`-backed feeder streams the entry to a
    // replica); register it explicitly here so `get_current_vlsn()` — and
    // therefore `update_dtvlsn_from_feeders`'s single-node branch — observes
    // the advance without needing a live peer connection.
    env_impl
        .log_txn_commit(1, /* fsync = */ false, /* flush = */ true)
        .expect("log_txn_commit");
    rep_env.register_vlsn(1, 0, 1);

    // Drive the DTVLSN computation the same way `record_ack` would (single
    // node: durable_ack_count == 0, so DTVLSN tracks current VLSN directly).
    // This is exactly what the master-side ack path calls after every ack;
    // calling `record_ack` directly here means the test does not depend on
    // a live feeder/replica connection.
    rep_env.record_ack(0, "__no_such_replica__");

    let dtvlsn_before = rep_env.get_dtvlsn();
    assert!(
        dtvlsn_before > 0,
        "precondition: DTVLSN must have advanced past 0, got {dtvlsn_before}"
    );

    // Sanity: exactly one TxnCommit exists so far (the application commit
    // above). C-C2b's `log_txn_commit` embeds the assigned WAL-VLSN counter
    // value in the `dtvlsn` field of every VLSN-tagged commit (this is the
    // R-3 mechanism used for the X-14 VLSN-index rebuild after a crash — it
    // is NOT a persisted DTVLSN in the JE `DTVLSNFlusher` sense). The daemon
    // under test writes a SEPARATE null commit whose `dtvlsn` equals the
    // computed DTVLSN value once the master goes idle.
    let before = scan_txn_commit_dtvlsns(&env_home);
    assert_eq!(before, vec![1], "only the app commit should exist so far");

    // Now go idle: perform NO further application commits and wait for the
    // daemon to observe DTVLSN stability and flush it via a null commit.
    // Generous timeout per the task brief (2s+, not 100ms).  The signal we
    // wait for is a SECOND `TxnCommit` entry appearing in the WAL — the
    // daemon's null commit — distinct from the app commit already scanned
    // above (matching `dtvlsn_before` by value is not a distinguishing
    // signal here because the app commit's embedded R-3 dtvlsn already
    // happens to equal the DTVLSN value in this single-node scenario).
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut found_flush = false;
    let mut last_seen = before.clone();
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(200));
        let commits = scan_txn_commit_dtvlsns(&env_home);
        if commits.len() > before.len() {
            last_seen = commits;
            found_flush = true;
            break;
        }
    }

    let _ = rep_env.close();

    assert!(
        found_flush,
        "expected a SECOND TxnCommit (the daemon's null commit persisting \
         dtvlsn={dtvlsn_before}) to appear in the WAL without any further \
         application commit (FAIL-BEFORE: no such daemon exists on \
         origin/main; the WAL only ever contains the single app commit). \
         Found commits: {:?}",
        last_seen,
    );
    // The new commit's embedded dtvlsn must equal the stable DTVLSN value
    // observed before going idle (the daemon flushes exactly that value).
    assert_eq!(
        last_seen.last().copied(),
        Some(dtvlsn_before as i64),
        "the flushed null commit must carry dtvlsn={dtvlsn_before}"
    );
}
