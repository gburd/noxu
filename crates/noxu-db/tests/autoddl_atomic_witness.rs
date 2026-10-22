//! ENG-AUTODDL-1 atomicity witness: a crash BETWEEN the two NameLN writes of
//! an auto-commit rename must NOT lose the database. Uses FaultDisk TornWrite
//! at a targeted write so the new-name NameLN (or its covering TxnCommit) is
//! torn off, then the process power-cuts (posio writes the prefix and calls
//! `faultdisk::power_cut()`).
//!
//! On rc-base the rename logs the old-name tombstone with flush+fsync and the
//! new-name NameLN lazily; a torn write anywhere in that window can leave "a"
//! tombstoned and "b" absent — the whole db lost. The JE-faithful fix runs the
//! rename in ONE auto-txn whose NameLN changes are only honoured by recovery
//! if the single TxnCommit is durable, so a torn write before that commit
//! leaves the ORIGINAL name intact (atomic: all-or-nothing).
//!
//! The child sweeps a target write index (AUTODDL_TORN_AT) chosen by the
//! parent; the parent runs several indices and asserts the recovered db is
//! never empty: either "a:k=v" (rename did not take) or "b:k=v" (rename took).
//! Never `[]` and never a db with the record lost.
use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
use noxu_log::faultdisk::{self, FaultController, FaultKind};
use std::process::Command;

fn cfg(p: &std::path::Path) -> EnvironmentConfig {
    EnvironmentConfig::new(p.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
}

fn dbcfg() -> DatabaseConfig {
    DatabaseConfig::new().with_allow_create(true).with_transactional(true)
}

#[test]
fn autoddl_atomic_torn_child() {
    let Ok(dir) = std::env::var("AUTODDL_DIR") else { return };
    let rel: u64 = std::env::var("AUTODDL_TORN_AT").unwrap().parse().unwrap();
    let env = Environment::open(cfg(dir.as_ref())).unwrap();
    let db = env.open_database(None, "a", &dbcfg()).unwrap();
    db.put(b"k", b"v").unwrap();
    db.close().unwrap();
    env.flush_log(true).unwrap();
    // Tear the write `rel` positions into the rename's log output. If the
    // rename issues fewer writes than `rel`, no fault fires and the child
    // exits 0 cleanly (that index simply has no torn point — the parent still
    // recovers and checks).
    let at = faultdisk::write_count() + rel;
    faultdisk::install(FaultController::for_test(FaultKind::TornWrite, at));
    // May power-cut inside here (exit 137) when the torn write fires.
    let _ = env.rename_database(None, "a", "b");
    // No fault fired at this index: clean up and exit 0.
    faultdisk::uninstall();
    std::process::exit(0);
}

fn recover_view(dir: &std::path::Path) -> Vec<String> {
    let env = Environment::open(cfg(dir)).unwrap();
    let mut names = env.database_names().unwrap();
    names.sort();
    let rdcfg = DatabaseConfig::new().with_transactional(true);
    let view: Vec<String> = names
        .iter()
        .map(|n| {
            let d = env.open_database(None, n, &rdcfg).unwrap();
            let v = d
                .get(b"k")
                .ok()
                .flatten()
                .map(|b| String::from_utf8_lossy(&b).to_string());
            d.close().ok();
            format!("{n}:k={v:?}")
        })
        .collect();
    env.close().ok();
    view
}

#[test]
fn autoddl_rename_atomic_under_torn_write() {
    if std::env::var("AUTODDL_DIR").is_ok() {
        return;
    }
    // Sweep the first several writes the rename emits. For each torn point the
    // recovered catalog must keep the db: "a:k=v" (not renamed) or "b:k=v"
    // (renamed) — never empty, never a record lost.
    for rel in 0u64..6 {
        let dir = tempfile::tempdir().unwrap();
        let st = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "autoddl_atomic_torn_child",
                "--test-threads=1",
                "--nocapture",
            ])
            .env("AUTODDL_DIR", dir.path())
            .env("AUTODDL_TORN_AT", rel.to_string())
            .status()
            .unwrap();
        // 0 = no fault fired at that index; 137 = simulated power loss.
        let code = st.code().unwrap_or(-1);
        assert!(
            code == 0 || code == 137,
            "child rel={rel} exited unexpectedly {st:?}"
        );
        let v = recover_view(dir.path());
        let ok = v == vec!["a:k=Some(\"v\")".to_string()]
            || v == vec!["b:k=Some(\"v\")".to_string()];
        assert!(
            ok,
            "ENG-AUTODDL-1 atomicity: torn rename at rel={rel} lost the db \
             (expected [a:k=v] or [b:k=v], got {v:?})"
        );
    }
}
