//! XDB-DETECT mixed-history fuzz (coordinator exactness gate).
//!
//! Random transactional + auto-commit create / remove / rename / truncate /
//! recreate on 3–4 shared names, with checkpoints (incl. forced) and
//! `clean_log_forced()`, a clean close, and a reopen. Two writers:
//!   * the FIXED engine (this tree), and
//!   * (separately, via the b965-baseline tree) the buggy v7.11.1 writer.
//!
//! Exactness contract (coordinator): the detector must have **0 false
//! positives** — never refuse an environment whose recovery is CORRECT
//! (recovered catalog == ground truth). Each seed is recovered twice: once
//! with the detector disabled (`NOXU_XDB711_DETECT_OFF`) to read the
//! would-be-mounted catalog, once normally. A normal refusal counts as a
//! false positive **iff** the detector-off recovery matched ground truth.
//!
//! `XDB_MIXED_ITERS` seeds (default 600). Deterministic per seed.

use noxu_db::{
    CheckpointConfig, DatabaseConfig, Environment, EnvironmentConfig,
    EnvironmentFailureReason, NoxuError,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

fn seeds() -> u64 {
    std::env::var("XDB_MIXED_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(600)
}

fn scratch() -> PathBuf {
    std::env::var_os("TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(std::env::temp_dir)
}

fn cfg(p: &Path) -> EnvironmentConfig {
    let mut c = EnvironmentConfig::new(p.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    c.set_log_file_max_bytes(4096);
    c.set_cleaner_min_utilization(90u8);
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

/// Drive a legitimate mixed history on shared names; track the ground-truth
/// live name set. Returns the expected final sorted name set.
fn drive(env: &Environment, seed: u64) -> Vec<String> {
    let names = ["a", "b", "c", "d"];
    let mut live: BTreeMap<&str, ()> = BTreeMap::new();
    let mut rng =
        Rng(seed.wrapping_mul(2862933555777941757).wrapping_add(3037000493));
    let ops = 24 + (seed % 24) as usize;
    for _ in 0..ops {
        let n = names[rng.pick(names.len())];
        match rng.pick(9) {
            0 | 1 => {
                // txn create if absent
                if !live.contains_key(n) {
                    let t = env.begin_transaction(None).unwrap();
                    if let Ok(d) = env.open_database(Some(&t), n, &dbc()) {
                        d.put_in(&t, b"k".as_ref(), b"v".as_ref()).ok();
                        d.close().ok();
                        t.commit().unwrap();
                        live.insert(n, ());
                    } else {
                        t.abort().ok();
                    }
                }
            }
            2 => {
                // auto create if absent
                if !live.contains_key(n)
                    && let Ok(d) = env.open_database(None, n, &dbc())
                {
                    d.put(b"k", b"v").ok();
                    d.close().ok();
                    live.insert(n, ());
                }
            }
            3 => {
                // txn remove if present
                if live.contains_key(n) {
                    let t = env.begin_transaction(None).unwrap();
                    if env.remove_database(Some(&t), n).is_ok() {
                        t.commit().unwrap();
                        live.remove(n);
                    } else {
                        t.abort().ok();
                    }
                }
            }
            4 => {
                // auto remove if present
                if live.contains_key(n) && env.remove_database(None, n).is_ok()
                {
                    live.remove(n);
                }
            }
            5 => {
                // truncate (keeps the name live, clears records)
                if live.contains_key(n) {
                    let _ = env.truncate_database(None, n);
                }
            }
            6 => {
                // rename n -> m (m a different shared name) if n live and m absent
                let m = names[rng.pick(names.len())];
                if m != n && live.contains_key(n) && !live.contains_key(m) {
                    let t = env.begin_transaction(None).unwrap();
                    if env.rename_database(Some(&t), n, m).is_ok() {
                        t.commit().unwrap();
                        live.remove(n);
                        live.insert(m, ());
                    } else {
                        t.abort().ok();
                    }
                }
            }
            7 => {
                // auto rename n -> m
                let m = names[rng.pick(names.len())];
                if m != n
                    && live.contains_key(n)
                    && !live.contains_key(m)
                    && env.rename_database(None, n, m).is_ok()
                {
                    live.remove(n);
                    live.insert(m, ());
                }
            }
            _ => {
                env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
                    .ok();
                env.clean_log_forced().ok();
            }
        }
    }
    env.checkpoint(Some(&CheckpointConfig::new().with_force(true))).ok();
    live.keys().map(|s| s.to_string()).collect()
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
fn mixed_history_no_false_positive() {
    let base = scratch().join(format!("xdb-mixed-{}", std::process::id()));
    std::fs::create_dir_all(&base).unwrap();
    let n = seeds();
    let mut false_positives = 0u64;
    let mut refused_correctly = 0u64;
    let mut divergences = 0u64;
    for seed in 0..n {
        let dir = base.join(format!("s{seed}"));
        std::fs::create_dir_all(&dir).unwrap();
        let expect = {
            let env = Environment::open(cfg(&dir)).unwrap();
            let e = drive(&env, seed);
            env.close().unwrap();
            e
        };
        // Pass 1: detector OFF.
        // SAFETY: single-threaded test; set/remove around the open.
        unsafe { std::env::set_var("NOXU_XDB711_DETECT_OFF", "1") };
        let would = reopen_names(&dir);
        // SAFETY: single-threaded test.
        unsafe { std::env::remove_var("NOXU_XDB711_DETECT_OFF") };
        let recovery_correct = matches!(&would, Ok(g) if *g == expect);

        // Pass 2: detector ON.
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
                    refused_correctly += 1;
                }
            }
            Err(e) => panic!("seed {seed}: unexpected open error: {e}"),
        }
    }
    eprintln!(
        "mixed fuzz: {n} seeds — {false_positives} false positives, \
         {refused_correctly} correct fail-closed, {divergences} silent \
         recovery divergences (recovery/cleaner gap, not a detector FP)"
    );
    assert_eq!(
        false_positives, 0,
        "{false_positives}/{n} correctly-recovered histories falsely refused"
    );
}
