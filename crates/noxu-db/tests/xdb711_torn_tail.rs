//! XDB-DETECT torn-tail exactness (coordinator point 3).
//!
//! A torn log tail (crash / SIGKILL) can only drop a SUFFIX of the log — it
//! cannot remove a record in the middle. So it cannot delete a tombstone that
//! sits between two bindings (signal b) nor a legitimate authority that sits
//! below a stray relog (signal a) while keeping the later record: if the tail
//! is cut at offset X, everything at ≥ X is gone, including the relog burst
//! and the final CkptStart that make a stray positionally identifiable.
//!
//! This probe writes a legitimate mixed history (fixed writer), then
//! truncates the single log file at many byte offsets (simulating every
//! possible torn-tail cut), and asserts recovery NEVER fails closed with
//! LOG_INTEGRITY on any surviving prefix whose recovery is well-formed. A cut
//! can leave a shorter but still-legitimate catalog; it must never manufacture
//! a stray-relog or double-binding signature.

use noxu_db::{
    CheckpointConfig, DatabaseConfig, Environment, EnvironmentConfig,
    EnvironmentFailureReason, NoxuError,
};
use std::path::{Path, PathBuf};

fn scratch() -> PathBuf {
    std::env::var_os("TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
}
fn cfg(p: &Path) -> EnvironmentConfig {
    let mut c = EnvironmentConfig::new(p.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    // One big log file so the "torn tail" is a single-file truncation.
    c.set_log_file_max_bytes(1 << 30);
    c.set_run_cleaner(false);
    c.set_run_checkpointer(true);
    c.set_run_evictor(false);
    c
}
fn dbc() -> DatabaseConfig {
    DatabaseConfig::new().with_allow_create(true).with_transactional(true)
}

fn write_history(dir: &Path) {
    let env = Environment::open(cfg(dir)).unwrap();
    // A mix that produces committed rename / remove / recreate on shared names
    // plus checkpoints (relog bursts) so the tail contains the exact records
    // the detector keys on.
    let t = env.begin_transaction(None).unwrap();
    let d = env.open_database(Some(&t), "n", &dbc()).unwrap();
    d.put_in(&t, b"k".as_ref(), b"v".as_ref()).ok();
    d.close().ok();
    t.commit().unwrap();
    env.checkpoint(Some(&CheckpointConfig::new().with_force(true))).ok();
    env.rename_database(None, "n", "m").unwrap();
    env.checkpoint(Some(&CheckpointConfig::new().with_force(true))).ok();
    let d2 = env.open_database(None, "n", &dbc()).unwrap(); // recreate n (fresh id)
    d2.put(b"k", b"v2").ok();
    d2.close().ok();
    env.checkpoint(Some(&CheckpointConfig::new().with_force(true))).ok();
    env.remove_database(None, "m").unwrap();
    env.checkpoint(Some(&CheckpointConfig::new().with_force(true))).ok();
    env.close().unwrap();
}

fn log_file(dir: &Path) -> PathBuf {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|e| e == "ndb").unwrap_or(false))
        .collect();
    files.sort();
    files.pop().expect("a .ndb file")
}

#[test]
fn torn_tail_never_false_fires() {
    let base = scratch().join(format!("xdb-torn-{}", std::process::id()));
    std::fs::create_dir_all(&base).unwrap();

    // Produce the reference history once, capture the full log bytes.
    let src = base.join("src");
    std::fs::create_dir_all(&src).unwrap();
    write_history(&src);
    let full = std::fs::read(log_file(&src)).unwrap();
    let total = full.len() as u64;

    // Try many torn-tail cut points across the whole file.
    let mut false_fires = 0u64;
    let step = (total / 200).max(1);
    let mut cut = step;
    let mut tried = 0u64;
    while cut < total {
        let dir = base.join(format!("cut-{cut}"));
        std::fs::create_dir_all(&dir).unwrap();
        // Stage a truncated copy of the env (just the log file; open rebuilds
        // the lock).
        for e in std::fs::read_dir(&src).unwrap() {
            let e = e.unwrap();
            if e.file_type().unwrap().is_file() {
                let name = e.file_name();
                if name.to_string_lossy().ends_with(".ndb") {
                    std::fs::write(dir.join(&name), &full[..cut as usize])
                        .unwrap();
                } else {
                    std::fs::copy(e.path(), dir.join(&name)).ok();
                }
            }
        }
        let mut rc = EnvironmentConfig::new(dir.clone())
            .with_allow_create(false)
            .with_transactional(true);
        rc.set_run_cleaner(false);
        rc.set_run_checkpointer(false);
        rc.set_run_evictor(false);
        match Environment::open(rc) {
            Ok(env) => {
                env.close().ok();
            }
            Err(NoxuError::EnvironmentFailure {
                reason: EnvironmentFailureReason::LogIntegrity,
                msg,
            }) => {
                false_fires += 1;
                eprintln!(
                    "cut {cut}/{total}: LOG_INTEGRITY on torn tail: {msg}"
                );
            }
            // Other open errors (a truncated record / incomplete header) are
            // legitimate torn-tail outcomes, not a detector false positive.
            Err(_) => {}
        }
        tried += 1;
        cut += step;
    }
    eprintln!(
        "torn-tail: {tried} cut points, {false_fires} LOG_INTEGRITY false fires"
    );
    assert_eq!(false_fires, 0, "detector false-fired on a torn tail");
}
