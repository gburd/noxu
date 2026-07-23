// Copyright (C) 2024-2025 Greg Burd.  Apache-2.0 OR MIT.
//! Deterministic torn-write recovery policy test.
//!
//! This is the byte-level, in-process, no-timing-race companion to the
//! SIGKILL-timed torn-write coverage in `crash_recovery_test.rs`
//! (`test_torn_write_truncated_entry_recovered`) and the randomised
//! `power_loss_sweep.rs`. Where those rely on killing a subprocess at a
//! sampled moment (so the exact torn boundary is non-deterministic), this
//! test writes a known set of committed transactions, closes cleanly so the
//! `.ndb` WAL is well-formed, then **deterministically truncates the last N
//! bytes of the newest log file** to model a torn final write, reopens, and
//! asserts Noxu's documented recovery policy.
//!
//! ## Policy under test (JE `RecoveryManager` / `FileManagerLogScanner`
//! reference)
//!
//! JE's documented behavior at end-of-log discovery
//! (`LastFileReader` / `RecoveryManager.findEndOfLog`): a checksum error or a
//! header that runs past the physical end of the file near the tail of the
//! newest log is treated as an ordinary **torn write** from an interrupted
//! crash — the log is silently truncated at that point and every prior
//! complete, committed entry is recovered. Noxu mirrors this in
//! `FileManagerLogScanner::find_end_of_log`. Only when the
//! `halt_on_commit_after_checksum_exception` flag is set AND a committed
//! transaction is found *after* the corruption does Noxu refuse to mount
//! (that case is exercised by `halt_on_commit_after_checksum_test.rs`).
//!
//! Two cases are asserted here:
//!
//!  * **Case A — torn tail = recover.** Truncating the newest `.ndb` by a few
//!    bytes leaves a partial final record. Recovery must truncate that torn
//!    tail, open `Ok`, and surface every committed record written before it.
//!
//!  * **Case B — torn tail of increasing severity = still a prefix.** Sweeping
//!    the truncation length from 1 byte up to most of the file must NEVER
//!    silently drop an *earlier* committed record while keeping a later one:
//!    the recovered key set is always a prefix of the committed sequence
//!    (recovery truncates from the tail inward, never punches a hole).

use noxu_db::{DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig};

fn scratch(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "noxu-torn-write-{}-{}",
        tag,
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&p);
    std::fs::create_dir_all(&p).unwrap();
    p
}

/// Write `n` explicitly-committed txns (keys `k0000..`, value `"v"`) and close
/// cleanly, so the on-disk `.ndb` WAL is a well-formed, fully-flushed log.
fn write_committed(dir: &std::path::Path, n: u32) {
    let env = Environment::open(
        EnvironmentConfig::new(dir.to_path_buf())
            .with_transactional(true)
            .with_allow_create(true),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "d",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    for i in 0..n {
        let txn = env.begin_transaction(None).unwrap();
        db.put_in(&txn, format!("k{i:04}").as_bytes(), b"v").unwrap();
        txn.commit().unwrap();
    }
    db.close().unwrap();
    env.close().unwrap();
}

/// Newest `.ndb` file in `dir` (highest file number).
fn newest_ndb(dir: &std::path::Path) -> std::path::PathBuf {
    let mut files: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "ndb"))
        .collect();
    files.sort();
    files.pop().expect("at least one .ndb log file")
}

/// Truncate `path` by `n` bytes (removing the last `n` bytes), modelling a
/// torn final write where the tail of the newest log never reached disk.
fn truncate_tail(path: &std::path::Path, n: u64) {
    let len = std::fs::metadata(path).unwrap().len();
    let new_len = len.saturating_sub(n);
    let f = std::fs::OpenOptions::new().write(true).open(path).unwrap();
    f.set_len(new_len).unwrap();
    f.sync_all().unwrap();
}

/// Reopen and count how many of the `n` committed keys survive, asserting the
/// env opens `Ok` (torn-tail truncation, not a hard failure).
fn reopen_and_count_present(dir: &std::path::Path, n: u32) -> u32 {
    let env = Environment::open(
        EnvironmentConfig::new(dir.to_path_buf()).with_transactional(true),
    )
    .expect("torn-tail truncation must open Ok (recover), not fail");
    let db = env
        .open_database(
            None,
            "d",
            &DatabaseConfig::new().with_transactional(true),
        )
        .unwrap();
    let mut present = 0u32;
    for i in 0..n {
        let key = DatabaseEntry::from_bytes(format!("k{i:04}").as_bytes());
        let mut val = DatabaseEntry::new();
        if db.get_into(None, &key, &mut val).unwrap() {
            present += 1;
        }
    }
    db.close().unwrap();
    env.close().unwrap();
    present
}

/// Case A: a small torn tail recovers ALL committed records.
///
/// A clean-closed log ends with well-formed entries; shaving a few bytes off
/// the physical end tears the *last* thing on disk. Because the log was
/// clean-closed, the trailing bytes are checkpoint/close metadata, not a
/// committed data record, so recovery truncates the torn tail and every one of
/// the `N` committed keys must survive.
#[test]
fn torn_tail_recovers_all_prior_committed() {
    const N: u32 = 40;
    let dir = scratch("case-a");
    write_committed(&dir, N);

    let log = newest_ndb(&dir);
    let before = std::fs::metadata(&log).unwrap().len();
    // Shave 3 bytes: enough to tear the final record's checksum/size, small
    // enough not to reach into committed data records earlier in the file.
    truncate_tail(&log, 3);
    assert!(std::fs::metadata(&log).unwrap().len() < before);

    let present = reopen_and_count_present(&dir, N);
    assert_eq!(
        present, N,
        "torn tail must truncate and recover ALL {N} prior committed records; \
         got {present}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Case B: torn-tail severity sweep never drops an earlier commit while
/// keeping a later one (recovered set is always a PREFIX).
///
/// This is the strict-prefix invariant of torn-write recovery: whatever the
/// truncation length, recovery truncates from the tail inward and never
/// silently discards a committed record that lies *before* a surviving one.
/// If recovery ever "punched a hole" (kept key 39 but dropped key 10) this
/// test fails; a monotone-non-increasing present-count as truncation grows,
/// with a contiguous surviving prefix, is the correct behavior.
#[test]
fn torn_tail_severity_sweep_is_always_a_prefix() {
    const N: u32 = 60;
    // Sweep truncation lengths across the tail. Each length gets a fresh log
    // (rewritten + truncated) so the cases are independent and deterministic.
    //
    // The small cuts (<= a few hundred bytes) only trim trailing
    // checkpoint/close metadata, so all N keys survive. The large cuts reach
    // into committed *data* records and DO drop the newest keys — those are
    // the cases that make the prefix invariant non-trivial (verified
    // empirically: on a single ~7.6 KB log, a ~2.4 KB tail cut drops the last
    // ~7 keys as a clean prefix, never a hole). Including both regimes keeps
    // the test honest: it exercises the actual key-drop path, not just a
    // vacuous "nothing changed" assertion.
    let mut saw_drop = false;
    for &cut in &[1u64, 8, 64, 256, 1024, 2000, 2400, 3000, 4000] {
        let dir = scratch(&format!("case-b-{cut}"));
        write_committed(&dir, N);
        let log = newest_ndb(&dir);
        let len = std::fs::metadata(&log).unwrap().len();
        if cut >= len {
            let _ = std::fs::remove_dir_all(&dir);
            continue;
        }
        truncate_tail(&log, cut);

        // Reopen; the env must open Ok (torn-tail truncation).
        let env = Environment::open(
            EnvironmentConfig::new(dir.clone()).with_transactional(true),
        )
        .unwrap_or_else(|e| {
            panic!("cut={cut}: torn tail must recover, not fail: {e}")
        });
        let db = env
            .open_database(
                None,
                "d",
                &DatabaseConfig::new().with_transactional(true),
            )
            .unwrap();

        // Find the surviving contiguous prefix length: the first missing key
        // index. Every key BEFORE that must be present; the invariant is that
        // no key AFTER the first gap is present (no hole).
        let mut first_missing: Option<u32> = None;
        let mut present_after_gap = 0u32;
        let mut present_total = 0u32;
        for i in 0..N {
            let key = DatabaseEntry::from_bytes(format!("k{i:04}").as_bytes());
            let mut val = DatabaseEntry::new();
            let here = db.get_into(None, &key, &mut val).unwrap();
            if here {
                present_total += 1;
            }
            match first_missing {
                None if !here => first_missing = Some(i),
                Some(_) if here => present_after_gap += 1,
                _ => {}
            }
        }
        db.close().unwrap();
        env.close().unwrap();

        if present_total < N {
            saw_drop = true;
        }
        assert_eq!(
            present_after_gap, 0,
            "cut={cut}: recovery punched a hole — key(s) present AFTER the \
             first missing key {first_missing:?}; recovered set must be a \
             contiguous PREFIX of the committed sequence"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
    // Non-triviality guard: at least one cut in the sweep must have actually
    // dropped a committed key, so the prefix assertion is exercised against a
    // real truncation-into-data, not just against untouched logs.
    assert!(
        saw_drop,
        "sweep never truncated into committed data (all cuts left every key \
         present) — the prefix invariant would be vacuous; widen the cut range"
    );
}
