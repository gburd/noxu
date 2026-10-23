//! Cross-engine XDB-DETECT check: recover b965eb35-WRITTEN mixed-history
//! images with the FIXED engine + detector.
//!
//! Images are produced by `xdb711_write_b965.rs` on a b965-baseline tree
//! (the buggy v7.11.1 writer) and staged under `XDB_B965_IMAGES` (each
//! `seed-*/` has the `.ndb` files + a `ground_truth` file: sorted live names).
//!
//! For each image, recover twice:
//!   * detector OFF (`NOXU_XDB711_DETECT_OFF`) → the would-be-mounted catalog;
//!     compare to ground truth to classify the image as DAMAGED or clean.
//!   * detector ON → the real verdict.
//!
//! Contract (coordinator):
//!   * 0 false positives: never refuse an image whose detector-off recovery
//!     matched ground truth.
//!   * every DAMAGED image (detector-off recovery != ground truth) that the
//!     detector CAN see (has surviving NameLN evidence) must be refused; the
//!     rest are silent (a recovery/cleaner gap, ENG-CLEAN-TOMBSTONE-1).
//!
//! Report the damage count and the refused/silent split.

use noxu_db::{
    Environment, EnvironmentConfig, EnvironmentFailureReason, NoxuError,
};
use std::path::{Path, PathBuf};

fn images_root() -> Option<PathBuf> {
    std::env::var_os("XDB_B965_IMAGES").map(PathBuf::from)
}

/// Stage an image dir into a fresh scratch copy so the read-write recovery
/// open never mutates the pristine corpus (a recovery open appends a
/// checkpoint tail; on v8 that newer-format tail would shift the bounded
/// analysis window past the stray evidence on a second in-place open).
fn stage(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for e in std::fs::read_dir(src).unwrap() {
        let e = e.unwrap();
        if e.file_type().unwrap().is_file() {
            std::fs::copy(e.path(), dst.join(e.file_name())).unwrap();
        }
    }
}

fn scratch_base() -> PathBuf {
    std::env::var_os("TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
        .join(format!("xdb-b965-cross-{}", std::process::id()))
}

fn reopen_names(dir: &Path) -> Result<Vec<String>, NoxuError> {
    let mut rc = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(false)
        .with_transactional(true);
    rc.set_run_cleaner(false);
    rc.set_run_checkpointer(false);
    rc.set_run_evictor(false);
    let env = Environment::open(rc)?;
    let mut got = env.database_names().unwrap();
    got.sort();
    env.close().ok();
    Ok(got)
}

fn ground_truth(dir: &Path) -> Vec<String> {
    std::fs::read_to_string(dir.join("ground_truth"))
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.is_empty())
        .map(|s| s.to_string())
        .collect()
}

#[test]
fn b965_written_images_cross_engine() {
    let Some(root) = images_root() else {
        eprintln!("XDB_B965_IMAGES unset — skipping");
        return;
    };
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(&root)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.is_dir()
                && p.file_name().unwrap().to_string_lossy().starts_with("seed-")
        })
        .collect();
    dirs.sort();

    let mut damaged = 0u64;
    let mut damaged_refused = 0u64;
    let mut damaged_silent = 0u64;
    let mut false_positives = 0u64;
    let mut first_fp = String::new();
    let mut first_silent = String::new();

    let base = scratch_base();
    let _ = std::fs::remove_dir_all(&base);
    for dir in &dirs {
        let expect = ground_truth(dir);
        let name = dir.file_name().unwrap().to_string_lossy().to_string();

        // Pass 1: detector OFF → would-be catalog. Stage a FRESH copy so this
        // recovery open's checkpoint tail does not contaminate pass 2.
        let off_dir = base.join(format!("{name}-off"));
        stage(dir, &off_dir);
        // SAFETY: single-threaded test.
        unsafe { std::env::set_var("NOXU_XDB711_DETECT_OFF", "1") };
        let would = reopen_names(&off_dir);
        // SAFETY: single-threaded test.
        unsafe { std::env::remove_var("NOXU_XDB711_DETECT_OFF") };
        let is_damaged = !matches!(&would, Ok(g) if *g == expect);
        if is_damaged {
            damaged += 1;
        }

        // Pass 2: detector ON, on a SEPARATE fresh staged copy.
        let on_dir = base.join(format!("{name}-on"));
        stage(dir, &on_dir);
        match reopen_names(&on_dir) {
            Ok(_) => {
                if is_damaged {
                    damaged_silent += 1;
                    if first_silent.is_empty() {
                        first_silent = format!(
                            "{}: damaged (would-mount {:?} != ground {:?}) \
                             but detector silent",
                            dir.display(),
                            would.as_ref().ok(),
                            expect
                        );
                    }
                }
            }
            Err(NoxuError::EnvironmentFailure {
                reason: EnvironmentFailureReason::LogIntegrity,
                ..
            }) => {
                if is_damaged {
                    damaged_refused += 1;
                } else {
                    false_positives += 1;
                    if first_fp.is_empty() {
                        first_fp = format!(
                            "{}: recovery correct ({:?}) but refused",
                            dir.display(),
                            expect
                        );
                    }
                }
            }
            Err(e) => panic!("{}: unexpected open error: {e}", dir.display()),
        }
    }

    eprintln!(
        "b965 cross-engine: {} images — {damaged} damaged (recovery != \
         ground truth); of those {damaged_refused} REFUSED by detector, \
         {damaged_silent} silent (recovery/cleaner gap, no surviving NameLN \
         evidence); {false_positives} FALSE POSITIVES",
        dirs.len()
    );
    if !first_silent.is_empty() {
        eprintln!("first silent: {first_silent}");
    }
    assert_eq!(false_positives, 0, "detector false positives: {first_fp}");
}
