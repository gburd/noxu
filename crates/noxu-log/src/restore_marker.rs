//! RestoreMarker — a file that fences a half-restored environment.
//!
//! Faithful port of BDB-JE's `com.sleepycat.je.log.RestoreMarker`
//! (`RestoreMarker.java`).
//!
//! A RestoreMarker is a file that indicates a normal recovery is not possible,
//! because the log files are physically or semantically inconsistent in some
//! way.  The canonical case is an **interrupted, incomplete network restore**:
//! the restore copies log files from a source to a destination node; if it is
//! halted, the destination may contain files that are individually readable
//! and checksum-able, yet the set as a whole is not a complete and coherent
//! copy of the log.  Recovery must not run on such a log until a curative
//! action makes it consistent (for a network restore, re-running the copy to
//! completion).  The network restore creates the marker **just before** it
//! makes any change to the log that would render it inconsistent, and removes
//! it only after the full set has been transferred.
//!
//! JE stores the marker as `<MAXINT>.jdb`, holding a log-file header plus a
//! `RestoreRequired` log entry, and relies on recovery reading backwards from
//! the last file.  Noxu stores the marker as a distinctly-named file
//! ([`MARKER_FILE_NAME`]) holding the serialized [`RestoreRequired`] entry.
//! The distinct name (not a `*.ndb` log-file name) is deliberate and
//! **patch-safe**: an older reader's log-file enumeration (`parse_file_number`
//! requires a `.ndb` suffix) ignores it entirely, while the environment-open
//! path explicitly checks for it and refuses to open (or forces a restore)
//! when present — the same observable behaviour as JE.
//!
//! `create_marker_file` is idempotent and may be called repeatedly; it will
//! not overwrite an existing non-empty marker.  The handler that repairs the
//! log must delete the marker (via [`remove_marker_file`]) so future
//! recoveries can succeed.

use std::io::Write;
use std::path::Path;

use bytes::BytesMut;
use hashbrown::HashMap;

use crate::entry::restore_required::{FailureType, RestoreRequired};

/// The name of the restore marker file within the environment directory.
///
/// JE: `RestoreMarker.getMarkerFileName()` returns `<MAXINT>.jdb`.  Noxu uses
/// a distinct, non-`.ndb` name so an older reader's `.ndb` enumeration ignores
/// it (patch-safe); the open path checks for it by this exact name.
pub const MARKER_FILE_NAME: &str = "restore_required.marker";

/// Create the restore marker file in `env_dir`, recording `failure_type` and
/// the diagnostic `props`.
///
/// Idempotent: if a non-empty marker already exists it is left untouched
/// (JE `createMarkerFile`: "will not re-create the marker file if there's
/// already a non-zero length file").
///
/// JE: `RestoreMarker.createMarkerFile`
/// (`RestoreMarker.java`, `FailureType.NETWORK_RESTORE`).
pub fn create_marker_file(
    env_dir: &Path,
    failure_type: FailureType,
    props: HashMap<String, String>,
) -> std::io::Result<()> {
    let path = env_dir.join(MARKER_FILE_NAME);

    // Don't overwrite an existing non-empty marker.
    if let Ok(meta) = std::fs::metadata(&path)
        && meta.len() > 0
    {
        return Ok(());
    }

    let rr = RestoreRequired::new(failure_type, props);
    let mut buf = BytesMut::new();
    rr.write_to_log(&mut buf);

    // Write + fsync so the marker is durable before any log file is mutated.
    let mut f = std::fs::File::create(&path)?;
    f.write_all(&buf)?;
    f.sync_all()?;
    Ok(())
}

/// Read and parse the restore marker in `env_dir`, if present.
///
/// Returns `Ok(Some(rr))` when a marker exists and parses, `Ok(None)` when no
/// marker is present, and `Err` on an I/O or parse error for a present marker
/// (a present-but-corrupt marker is still a reason to refuse the environment).
pub fn read_marker_file(
    env_dir: &Path,
) -> std::io::Result<Option<RestoreRequired>> {
    let path = env_dir.join(MARKER_FILE_NAME);
    match std::fs::read(&path) {
        Ok(bytes) => {
            let rr = RestoreRequired::read_from_log(&bytes).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("corrupt restore marker: {e}"),
                )
            })?;
            Ok(Some(rr))
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Returns `true` if a restore marker is present in `env_dir`.
pub fn marker_present(env_dir: &Path) -> bool {
    env_dir.join(MARKER_FILE_NAME).exists()
}

/// Remove the restore marker from `env_dir` (if present).
///
/// JE: `RestoreMarker.removeMarkerFile`.  Called once the full restore has
/// completed and the log is coherent again.
pub fn remove_marker_file(env_dir: &Path) -> std::io::Result<()> {
    let path = env_dir.join(MARKER_FILE_NAME);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn create_read_remove_roundtrip() {
        let dir = tempdir().unwrap();
        assert!(!marker_present(dir.path()));
        assert!(read_marker_file(dir.path()).unwrap().is_none());

        let mut props = HashMap::new();
        props.insert("source".to_string(), "node2".to_string());
        create_marker_file(
            dir.path(),
            FailureType::NetworkRestore,
            props.clone(),
        )
        .unwrap();

        assert!(marker_present(dir.path()));
        let rr = read_marker_file(dir.path()).unwrap().unwrap();
        assert_eq!(rr.failure_type, FailureType::NetworkRestore);
        assert_eq!(rr.properties, props);

        remove_marker_file(dir.path()).unwrap();
        assert!(!marker_present(dir.path()));
        // Remove is idempotent.
        remove_marker_file(dir.path()).unwrap();
    }

    #[test]
    fn create_is_idempotent_does_not_overwrite() {
        let dir = tempdir().unwrap();
        let mut props = HashMap::new();
        props.insert("k".to_string(), "v1".to_string());
        create_marker_file(dir.path(), FailureType::NetworkRestore, props)
            .unwrap();
        let first = std::fs::read(dir.path().join(MARKER_FILE_NAME)).unwrap();

        // A second create with different props must NOT overwrite.
        let mut props2 = HashMap::new();
        props2.insert("k".to_string(), "v2-different".to_string());
        create_marker_file(dir.path(), FailureType::NetworkRestore, props2)
            .unwrap();
        let second = std::fs::read(dir.path().join(MARKER_FILE_NAME)).unwrap();
        assert_eq!(first, second, "marker must not be overwritten");
    }
}
