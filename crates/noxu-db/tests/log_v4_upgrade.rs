//! A2 (v7.11.1 backport): refuse a newer `LOG_VERSION` on every PUBLIC open
//! path (writable and read-only), and never mutate (truncate / delete /
//! recover) a future-version file before its header is validated.
//!
//! Trimmed from the `parity/integrate` v4 witness `log_v4_upgrade.rs` to be
//! **v3-only**: the valid environment is produced by 7.11.0 itself (not a v4
//! writer / golden fixture), and the only "future" bytes are a CRC-valid
//! header carrying `LOG_VERSION + 1`. Nothing here writes a newer version.
use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
use noxu_log::file_header::LOG_VERSION;
use std::fs;
use std::path::Path;

fn config(path: &Path, read_only: bool) -> EnvironmentConfig {
    let mut cfg = EnvironmentConfig::new(path.to_path_buf())
        .with_allow_create(!read_only)
        .with_read_only(read_only)
        .with_transactional(true);
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    cfg.set_run_evictor(false);
    cfg.set_run_in_compressor(false);
    cfg
}

/// Collect every `.ndb` file in `dir`, sorted ascending by name.
fn ndb_files(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut v: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "ndb"))
        .collect();
    v.sort();
    v
}

/// Build a valid v3 environment with 7.11.0 and leave one committed record.
/// Returns the highest existing log file number so a plant can be strictly
/// higher-numbered (the orphan-truncate / delete-during-recovery path).
fn seed_v3_env(dir: &Path) -> u32 {
    let env = Environment::open(config(dir, false)).unwrap();
    let db = env
        .open_database(
            None,
            "probe",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    db.put(b"key", b"old-v3-value").unwrap();
    db.close().unwrap();
    env.close().unwrap();

    // Every file written by 7.11.0 must carry LOG_VERSION (3).
    let files = ndb_files(dir);
    assert!(!files.is_empty(), "v3 env must have written at least one file");
    let mut highest = 0u32;
    for f in &files {
        let bytes = fs::read(f).unwrap();
        let ver =
            u32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
        assert_eq!(ver, LOG_VERSION, "7.11.0 must write v3 only");
        let name = f.file_name().unwrap().to_str().unwrap();
        let num: u32 = name.trim_end_matches(".ndb").parse().unwrap();
        highest = highest.max(num);
    }
    highest
}

/// A CRC-valid header carrying `version` for `file_num`, optionally with a
/// nonzero record tail appended after the 36-byte header.
fn future_file(version: u32, file_num: u32, with_records: bool) -> Vec<u8> {
    // Start from a real v3 header shape, overwrite version + file number,
    // recompute the CRC over bytes [0..32] so only the version gate can reject.
    let mut bytes = vec![
        78u8, 79, 88, 85, 68, 66, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ];
    bytes[8..12].copy_from_slice(&version.to_be_bytes());
    bytes[24..28].copy_from_slice(&file_num.to_be_bytes());
    let crc =
        noxu_log::checksum::ChecksumValidator::compute_range(&bytes, 0, 32);
    bytes[32..36].copy_from_slice(&crc.to_be_bytes());
    if with_records {
        // Arbitrary nonzero "record" bytes after the header. These must never
        // be parsed: the version gate must reject before any record scan.
        bytes.extend_from_slice(&[0xAB; 64]);
    }
    bytes
}

/// (a) writable open + (b) read-only open: a CRC-valid future-version file
/// (header-only AND header+records) must be refused, and NO existing file may
/// be mutated — the future file must stay byte-identical (not deleted, not
/// truncated) and the valid v3 files must be unchanged.
#[test]
fn unsupported_headers_rejected_before_mutating_recovery() {
    for read_only in [true, false] {
        for with_records in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let highest = seed_v3_env(dir.path());

            // Snapshot the valid v3 files for a byte-identical check later.
            let v3_snapshot: Vec<(std::path::PathBuf, Vec<u8>)> =
                ndb_files(dir.path())
                    .into_iter()
                    .map(|p| {
                        let b = fs::read(&p).unwrap();
                        (p, b)
                    })
                    .collect();

            // Plant a CRC-valid future-version file ABOVE the highest valid
            // file, so writable recovery would be tempted to delete it as an
            // "orphan".
            let future_num = highest + 1;
            let future_name = format!("{future_num:08}.ndb");
            let future_bytes =
                future_file(LOG_VERSION + 1, future_num, with_records);
            let future_path = dir.path().join(&future_name);
            fs::write(&future_path, &future_bytes).unwrap();

            // Open MUST fail (writable or read-only).
            let res = Environment::open(config(dir.path(), read_only));
            assert!(
                res.is_err(),
                "open (read_only={read_only}, with_records={with_records}) \
                 of a future-version file must fail",
            );

            // The future file must remain byte-identical: not deleted, not
            // truncated, not recovered.
            assert!(
                future_path.exists(),
                "future file must NOT be deleted (read_only={read_only}, \
                 with_records={with_records})",
            );
            assert_eq!(
                fs::read(&future_path).unwrap(),
                future_bytes,
                "future file must be byte-identical (not truncated/mutated)",
            );

            // The valid v3 files must be unchanged too.
            for (p, before) in &v3_snapshot {
                assert_eq!(
                    &fs::read(p).unwrap(),
                    before,
                    "valid v3 file {p:?} must be unchanged",
                );
            }
        }
    }
}

/// (d) A future-version file encountered on a scan/recovery path must be a
/// hard error — not a silent skip, not an empty result, not a truncation.
/// Here the file carries records (nonzero tail); the engine must refuse it on
/// its version, long before it would try to parse those records.
#[test]
fn malformed_v4_bin_is_error_not_absence_or_truncation() {
    for read_only in [true, false] {
        let dir = tempfile::tempdir().unwrap();
        let highest = seed_v3_env(dir.path());

        let future_num = highest + 1;
        let future_name = format!("{future_num:08}.ndb");
        let future_bytes = future_file(LOG_VERSION + 1, future_num, true);
        let future_path = dir.path().join(&future_name);
        fs::write(&future_path, &future_bytes).unwrap();
        let before = fs::read(&future_path).unwrap();

        assert!(
            Environment::open(config(dir.path(), read_only)).is_err(),
            "a future-version file with records must be an error \
             (read_only={read_only}), not silent absence/truncation",
        );
        assert_eq!(
            fs::read(&future_path).unwrap(),
            before,
            "future file must be byte-identical (not truncated)",
        );
    }
}

/// (f) A normal v3 environment still opens read-only, opens writable, reads
/// its committed record, accepts a new write, and reopens — all with
/// LOG_VERSION staying 3 (nothing is upgraded to a newer version on write).
#[test]
fn v3_env_opens_recovers_and_round_trips() {
    let dir = tempfile::tempdir().unwrap();
    seed_v3_env(dir.path());

    // Read-only open sees the committed record.
    {
        let env = Environment::open(config(dir.path(), true)).unwrap();
        let db = env
            .open_database(
                None,
                "probe",
                &DatabaseConfig::new().with_read_only(true),
            )
            .unwrap();
        assert_eq!(
            db.get(b"key").unwrap().as_deref(),
            Some(b"old-v3-value".as_slice()),
        );
        db.close().unwrap();
        env.close().unwrap();
    }

    // Writable open recovers, reads, and accepts a new write.
    {
        let env = Environment::open(config(dir.path(), false)).unwrap();
        let db = env
            .open_database(
                None,
                "probe",
                &DatabaseConfig::new().with_transactional(true),
            )
            .unwrap();
        assert_eq!(
            db.get(b"key").unwrap().as_deref(),
            Some(b"old-v3-value".as_slice()),
        );
        db.put(b"key2", b"new-value").unwrap();
        db.close().unwrap();
        env.close().unwrap();
    }

    // Every file on disk is still v3 — the engine never upgraded the format.
    for f in ndb_files(dir.path()) {
        let bytes = fs::read(&f).unwrap();
        let ver =
            u32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
        assert_eq!(ver, LOG_VERSION, "{f:?} must stay v3 (no upgrade)");
    }

    // Final reopen sees both records.
    {
        let env = Environment::open(config(dir.path(), false)).unwrap();
        let db = env
            .open_database(
                None,
                "probe",
                &DatabaseConfig::new().with_transactional(true),
            )
            .unwrap();
        assert_eq!(
            db.get(b"key").unwrap().as_deref(),
            Some(b"old-v3-value".as_slice()),
        );
        assert_eq!(
            db.get(b"key2").unwrap().as_deref(),
            Some(b"new-value".as_slice()),
        );
        db.close().unwrap();
        env.close().unwrap();
    }
}
