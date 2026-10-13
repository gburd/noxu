//! A2 (v7.11.1 backport): the engine must refuse any log file whose
//! `log_version` is outside the supported `[MIN_LOG_VERSION, LOG_VERSION]`
//! window on EVERY path that opens or memory-maps a log file.
//!
//! Trimmed from the `parity/integrate` v4 witness `log_v4.rs` to be
//! **v3-only**: nothing here asks the engine to WRITE a newer version. The
//! "future" marker is `LOG_VERSION + 1`, planted as a CRC-valid header so the
//! only thing that can reject it is the version-admission gate (not a CRC or
//! magic check).
use noxu_log::file_header::{FileHeader, LOG_VERSION, MIN_LOG_VERSION};
use noxu_log::{FileManager, LogError};

/// A literal v3 header (magic + version=3 + big-endian marker + zero payload),
/// with a valid trailing CRC32 over bytes [0..32]. No live writer participates.
const V3: [u8; 36] = [
    78, 79, 88, 85, 68, 66, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];

/// Build a CRC-valid 36-byte header carrying `version`.
fn header(version: u32) -> Vec<u8> {
    let mut bytes = V3.to_vec();
    bytes[8..12].copy_from_slice(&version.to_be_bytes());
    let crc = crc32fast::hash(&bytes[..32]);
    bytes[32..].copy_from_slice(&crc.to_be_bytes());
    bytes
}

/// Build a CRC-valid header carrying `version` for file number `file_num`.
fn header_for(version: u32, file_num: u32) -> Vec<u8> {
    let mut bytes = header(version);
    bytes[24..28].copy_from_slice(&file_num.to_be_bytes());
    let crc = crc32fast::hash(&bytes[..32]);
    bytes[32..].copy_from_slice(&crc.to_be_bytes());
    bytes
}

/// (e) A version below MIN_LOG_VERSION and (reject) a version above
/// LOG_VERSION must both be refused by the header reader — the lowest-level
/// gate every path funnels through.
#[test]
fn v4_rejects_unsupported_versions_at_read_boundary() {
    // At the tag: MIN_LOG_VERSION = 2, LOG_VERSION = 3.
    for version in [0, 1, LOG_VERSION + 1, LOG_VERSION + 2, u32::MAX] {
        assert!(
            !(MIN_LOG_VERSION..=LOG_VERSION).contains(&version),
            "test fixture version {version} must be outside the window"
        );
        assert!(
            matches!(
                FileHeader::read_from(&mut header(version).as_slice()),
                Err(LogError::VersionMismatch { .. })
            ),
            "version {version} must be rejected with VersionMismatch"
        );
    }
}

/// (a)+(b)+(e) Opening a FileManager — read-only OR read-write — must validate
/// EVERY header before anything else, and refuse a below-MIN or future version
/// with VersionMismatch. A header-only future file (36 bytes, no records) is
/// the strict case: there is nothing to parse, so only an admission gate can
/// catch it.
#[test]
fn v4_open_validates_every_header_even_read_only() {
    for read_only in [true, false] {
        // Below MIN_LOG_VERSION (1) and above LOG_VERSION (LOG_VERSION + 1).
        for version in [MIN_LOG_VERSION - 1, LOG_VERSION + 1] {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(
                dir.path().join("00000000.ndb"),
                header_for(version, 0),
            )
            .unwrap();
            assert!(
                matches!(
                    FileManager::new(dir.path(), read_only, 1_000_000, 8),
                    Err(LogError::VersionMismatch { .. })
                ),
                "open (read_only={read_only}) of version {version} must be \
                 VersionMismatch",
            );
        }
    }
}

/// (c) A future-version file that appears AFTER the FileManager was opened
/// (so the open-time preflight never saw it) must be refused the moment it is
/// memory-mapped — not silently mapped and its records ignored.
#[test]
fn v4_mmap_rejects_future_file_created_after_open() {
    let dir = tempfile::tempdir().unwrap();
    // Empty dir: the FileManager opens cleanly (no files to validate yet).
    let fm = FileManager::new(dir.path(), true, 1_000_000, 8).unwrap();
    // Now plant a CRC-valid future-version file 1.
    std::fs::write(
        dir.path().join("00000001.ndb"),
        header_for(LOG_VERSION + 1, 1),
    )
    .unwrap();
    assert!(
        matches!(fm.mmap_file(1), Err(LogError::VersionMismatch { .. })),
        "mmap of a future-version file must be VersionMismatch",
    );
}
