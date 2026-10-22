//! XDB-711 fail-closed detector — real damaged-image witnesses.
//!
//! Opens each of the 4 saved durable images written by the v7.11.0/.1 engine
//! that carry the stray catalog-relog NameLN (XDB-711). On the detector branch
//! each MUST refuse to open with `EnvironmentFailureReason::LogIntegrity`
//! rather than silently serve the wrong name->id binding.
//!
//! Image set (saved on d-x866:/data/artifacts/xdb-B/seed-*, copy on
//! d-x863:/data/work/xdb711/xdb-B-saved). Point the test at a directory of
//! `seed-*` subdirectories via `NOXU_XDB711_IMAGES`; the test is skipped (not
//! failed) when the variable is unset so it never breaks a contributor's
//! `cargo test` without the private images.
//!
//! Teeth: set `NOXU_XDB711_EXPECT=silent` to assert the OPPOSITE — that the
//! env opens and serves data — which is what a build with the detector
//! DISABLED does (the RED proof that the detector is load-bearing). See
//! `xdb-detect.md` §3.

use noxu_db::{
    Environment, EnvironmentConfig, EnvironmentFailureReason, NoxuError,
};
use std::fs;
use std::path::{Path, PathBuf};

fn images_root() -> Option<PathBuf> {
    std::env::var_os("NOXU_XDB711_IMAGES").map(PathBuf::from)
}

/// Copy an image dir into a fresh scratch dir so the open path may touch the
/// lock file without mutating the saved artifact.
fn stage(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).unwrap();
    for e in fs::read_dir(src).unwrap() {
        let e = e.unwrap();
        let name = e.file_name();
        // The lock / ready marker files are reconstructed by open; copy only
        // the log files (and anything else) verbatim.
        let to = dst.join(&name);
        if e.file_type().unwrap().is_file() {
            fs::copy(e.path(), &to).unwrap();
        }
    }
}

fn scratch_base() -> PathBuf {
    // Honour TMPDIR (coordinator rule: test scratch in /data/tmp-<name>).
    std::env::var_os("TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
}

fn open_image(dir: &Path) -> Result<Environment, NoxuError> {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(false)
        .with_transactional(true);
    // Daemons off: recovery (not background work) must be what observes the
    // damage, and we don't want a checkpoint/cleaner mutating the staged copy.
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    cfg.set_run_evictor(false);
    cfg.set_run_in_compressor(false);
    cfg.set_run_verifier(false);
    cfg.set_stats_collect(false);
    Environment::open(cfg)
}

#[test]
fn xdb711_saved_images_fail_closed() {
    let Some(root) = images_root() else {
        eprintln!(
            "NOXU_XDB711_IMAGES unset; skipping saved-image witness (private \
             artifacts)"
        );
        return;
    };
    let expect_silent = std::env::var("NOXU_XDB711_EXPECT")
        .map(|v| v == "silent")
        .unwrap_or(false);

    let base = scratch_base().join("xdb711-images");
    let _ = fs::remove_dir_all(&base);

    let mut seen = 0usize;
    let mut seeds: Vec<PathBuf> = fs::read_dir(&root)
        .expect("NOXU_XDB711_IMAGES is not a readable directory")
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            p.is_dir()
                && p.file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("seed-"))
                    .unwrap_or(false)
        })
        .collect();
    seeds.sort();
    assert!(!seeds.is_empty(), "no seed-* image dirs under {}", root.display());

    for seed in &seeds {
        let name = seed.file_name().unwrap().to_string_lossy().to_string();
        let staged = base.join(&name);
        stage(seed, &staged);
        seen += 1;

        // seed 167229452944 is a defect-#2 double-binding (charlie bound by
        // two committed txns; charlie->7 is the LATEST committed binding, so
        // the catalog is committed-consistent and the corruption is purely
        // data-level -- id 11 holds charlie's records). Not detectable from the
        // NameLN catalog without false-positiving on legitimate concurrent
        // renames (the dropped signal (b)), so this image OPENS (salvage only).
        // The other 3 images are stray-relog damage and MUST fail-closed.
        let known_undetectable = name.contains("167229452944");

        let r = open_image(&staged);
        let r_desc = match &r {
            Ok(_) => "Ok(Environment)".to_string(),
            Err(e) => format!("Err({e:?})"),
        };
        if expect_silent {
            // TEETH (detector disabled): the env opens silently (wrong data).
            assert!(
                r.is_ok(),
                "[teeth] {name}: expected a SILENT open (detector disabled) \
                 but got {r_desc}"
            );
            eprintln!(
                "[teeth] {name}: opened silently (detector disabled) — RED"
            );
        } else if known_undetectable {
            assert!(
                r.is_ok(),
                "{name}: defect-#2 double binding is not detectable; must OPEN \
                 (salvage only) but got {r_desc}"
            );
            eprintln!(
                "{name}: OPENS (defect-#2 double binding, undetectable -- \
                 salvage/backup only)"
            );
            r.map(|e| e.close().ok()).ok();
        } else {
            match r {
                Err(NoxuError::EnvironmentFailure {
                    reason: EnvironmentFailureReason::LogIntegrity,
                    ref msg,
                }) => {
                    // The message must name the affected DB and the recovery
                    // options, and must NOT name a 7.11.x-absent subcommand.
                    assert!(
                        msg.contains("XDB-711"),
                        "{name}: message missing XDB-711 attribution: {msg}"
                    );
                    assert!(
                        msg.contains("restore") && msg.contains("preserve"),
                        "{name}: message missing recovery options: {msg}"
                    );
                    assert!(
                        !msg.contains("scavenge"),
                        "{name}: message names a 7.11.x-absent subcommand: {msg}"
                    );
                    eprintln!("{name}: fail-closed LOG_INTEGRITY ✓");
                }
                other => panic!(
                    "{name}: expected EnvironmentFailure(LOG_INTEGRITY), got \
                     {}",
                    match other {
                        Ok(_) => "Ok(Environment)".to_string(),
                        Err(e) => format!("Err({e:?})"),
                    }
                ),
            }
        }
    }
    assert_eq!(seen, seeds.len());
    eprintln!("checked {seen} saved XDB-711 images");
}
