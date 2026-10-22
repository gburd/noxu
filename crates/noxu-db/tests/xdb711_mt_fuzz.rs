//! XDB-DETECT multi-threaded mixed-history fuzz (coordinator point 1).
//!
//! Concurrent transactional + auto-commit DDL (create / remove / rename /
//! truncate) on shared names from several worker threads, with the env's
//! background checkpointer + cleaner + evictor running, a clean close, and a
//! reopen. This exercises the write-order-vs-commit-order interleavings that
//! made the dropped signal (b) false-fire; the surviving signal (a) must have
//! **0 false positives** (never refuse a correctly-recovered env).
//!
//! Because the op interleaving is OS-scheduled, the per-thread op sequence is
//! deterministic per seed but the resulting catalog is not known a priori; so
//! the ground truth is read from the live env at a quiescent instant (all
//! worker threads joined) BEFORE close, then recovery is compared to it. A
//! refusal is a false positive iff the detector-off recovery matched that
//! quiescent ground truth.
//!
//! `XDB_MT_ITERS` seeds (default 200).

use noxu_db::{
    DatabaseConfig, Environment, EnvironmentConfig, EnvironmentFailureReason,
    NoxuError,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn seeds() -> u64 {
    std::env::var("XDB_MT_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200)
}
fn scratch() -> PathBuf {
    std::env::var_os("TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
}
fn cfg(p: &Path) -> EnvironmentConfig {
    let mut c = EnvironmentConfig::new(p.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        .with_lock_timeout(500);
    c.set_log_file_max_bytes(8192);
    c.set_cleaner_min_utilization(80u8);
    c.set_cleaner_min_age(1);
    c.set_run_cleaner(true);
    c.set_run_checkpointer(true);
    c.set_run_evictor(true);
    c
}
fn dbc() -> DatabaseConfig {
    DatabaseConfig::new().with_allow_create(true).with_transactional(true)
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 16
    }
    fn pick(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

const NAMES: &[&str] = &["a", "b", "c", "d"];
const THREADS: usize = 4;
const OPS: usize = 20;

/// Run the concurrent fuzz; return the quiescent committed catalog (sorted
/// names) observed through the live handle after all workers joined.
fn run(env: &Arc<Environment>, seed: u64) -> Vec<String> {
    let mut handles = Vec::new();
    for t in 0..THREADS {
        let env = Arc::clone(env);
        let s = seed.wrapping_mul(97).wrapping_add(t as u64).wrapping_add(1);
        handles.push(std::thread::spawn(move || {
            let mut rng =
                Rng(s.wrapping_mul(2862933555777941757).wrapping_add(1));
            for _ in 0..OPS {
                let n = NAMES[rng.pick(NAMES.len())];
                match rng.pick(8) {
                    0 | 1 => {
                        let tx = env.begin_transaction(None).unwrap();
                        if let Ok(d) = env.open_database(Some(&tx), n, &dbc()) {
                            d.put_in(&tx, b"k".as_ref(), b"v".as_ref()).ok();
                            d.close().ok();
                            tx.commit().ok();
                        } else {
                            tx.abort().ok();
                        }
                    }
                    2 => {
                        if let Ok(d) = env.open_database(None, n, &dbc()) {
                            d.put(b"k", b"v").ok();
                            d.close().ok();
                        }
                    }
                    3 => {
                        let tx = env.begin_transaction(None).unwrap();
                        if env.remove_database(Some(&tx), n).is_ok() {
                            tx.commit().ok();
                        } else {
                            tx.abort().ok();
                        }
                    }
                    4 => {
                        let _ = env.remove_database(None, n);
                    }
                    5 => {
                        let _ = env.truncate_database(None, n);
                    }
                    6 => {
                        let m = NAMES[rng.pick(NAMES.len())];
                        if m != n {
                            let tx = env.begin_transaction(None).unwrap();
                            if env.rename_database(Some(&tx), n, m).is_ok() {
                                tx.commit().ok();
                            } else {
                                tx.abort().ok();
                            }
                        }
                    }
                    _ => {
                        let m = NAMES[rng.pick(NAMES.len())];
                        if m != n {
                            let _ = env.rename_database(None, n, m);
                        }
                    }
                }
            }
        }));
    }
    for h in handles {
        let _ = h.join();
    }
    // Quiescent ground truth.
    let mut names = env.database_names().unwrap();
    names.sort();
    names
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

#[test]
fn concurrent_mixed_no_false_positive() {
    let base = scratch().join(format!("xdb-mt-{}", std::process::id()));
    std::fs::create_dir_all(&base).unwrap();
    let n = seeds();
    let mut false_positives = 0u64;
    let mut refused = 0u64;
    let mut divergences = 0u64;
    for seed in 0..n {
        let dir = base.join(format!("s{seed}"));
        std::fs::create_dir_all(&dir).unwrap();
        let expect = {
            let env = Arc::new(Environment::open(cfg(&dir)).unwrap());
            let gt = run(&env, seed);
            // Drop the last Arc to close.
            match Arc::try_unwrap(env) {
                Ok(e) => e.close().unwrap(),
                Err(e) => e.close().unwrap(),
            }
            gt
        };
        // Pass 1: detector OFF.
        // SAFETY: single-threaded test loop.
        unsafe { std::env::set_var("NOXU_XDB711_DETECT_OFF", "1") };
        let would = reopen_names(&dir);
        // SAFETY: single-threaded test loop.
        unsafe { std::env::remove_var("NOXU_XDB711_DETECT_OFF") };
        let recovery_correct = matches!(&would, Ok(g) if *g == expect);

        match reopen_names(&dir) {
            Ok(got) => {
                if got != expect {
                    divergences += 1;
                }
            }
            Err(NoxuError::EnvironmentFailure {
                reason: EnvironmentFailureReason::LogIntegrity,
                msg,
            }) => {
                if recovery_correct {
                    false_positives += 1;
                    eprintln!(
                        "seed {seed}: FALSE POSITIVE — recovery correct \
                         ({expect:?}) but refused: {msg}"
                    );
                } else {
                    refused += 1;
                }
            }
            Err(e) => panic!("seed {seed}: unexpected open error: {e}"),
        }
    }
    eprintln!(
        "concurrent mixed fuzz: {n} seeds — {false_positives} false \
         positives, {refused} correct fail-closed, {divergences} silent \
         recovery divergences (recovery/cleaner gap, not a detector FP)"
    );
    assert_eq!(
        false_positives, 0,
        "{false_positives}/{n} correctly-recovered histories falsely refused"
    );
}
