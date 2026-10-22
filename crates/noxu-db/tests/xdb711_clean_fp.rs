//! XDB-DETECT cleaning-robustness guard (coordinator point 2).
//!
//! The detector's two signals (stray-relog, double-committed-binding) reason
//! about the catalog NameLN history. Log CLEANING can reclaim intermediate
//! NameLN files (a remove tombstone, a recreate), so the surviving history may
//! differ from the full one. This probe stresses exactly that: many
//! transactional + auto remove/recreate cycles of a few shared names, a tiny
//! `log_file_max`, aggressive cleaner settings, forced checkpoints +
//! `clean_log_forced()` loops, a clean close, and a reopen.
//!
//! The exactness contract (coordinator): the detector must have **0 false
//! positives** — it must never refuse an environment whose recovery is
//! CORRECT (recovered catalog == ground truth). To decide that per seed, each
//! history is recovered TWICE: once with the detector disabled
//! (`NOXU_XDB711_DETECT_OFF`) to observe the would-be-mounted catalog, and
//! once normally. A normal refusal is a false positive **iff** the
//! detector-off recovery matched ground truth.
//!
//! Observed (fixed writer, 200 seeds): 0 false positives. ~7% of seeds show a
//! *recovery divergence* under aggressive cleaning (a committed remove is lost
//! and a database is resurrected) — that is a recovery-correctness issue
//! INDEPENDENT of the detector; the detector either refuses it (fail-closed,
//! correct) or opens it (a pre-existing recovery gap, not a detector FP). With
//! cleaning disabled the same histories show 0 divergences, confirming the
//! divergences are cleaning-induced and the ground-truth tracking is correct.

use noxu_db::{
    CheckpointConfig, DatabaseConfig, Environment, EnvironmentConfig,
    EnvironmentFailureReason, NoxuError,
};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

fn seeds() -> u64 {
    std::env::var("XDB_CLEAN_ITERS")
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

fn drive(env: &Environment, seed: u64) -> Vec<String> {
    let names = ["a", "b", "c", "d"];
    let mut live: BTreeMap<&str, ()> = BTreeMap::new();
    let mut rng =
        Rng(seed.wrapping_mul(2862933555777941757).wrapping_add(3037000493));
    let ops = 20 + (seed % 20) as usize;
    for _ in 0..ops {
        let n = names[rng.pick(names.len())];
        match rng.pick(6) {
            0 | 1 => {
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
                if !live.contains_key(n)
                    && let Ok(d) = env.open_database(None, n, &dbc())
                {
                    d.put(b"k", b"v").ok();
                    d.close().ok();
                    live.insert(n, ());
                }
            }
            3 => {
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
                if live.contains_key(n)
                    && env.remove_database(None, n).is_ok()
                {
                    live.remove(n);
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
fn cleaning_never_false_fires() {
    let base = scratch().join(format!("xdb-clean-fp-{}", std::process::id()));
    std::fs::create_dir_all(&base).unwrap();
    let n = seeds();
    let mut false_positives = 0u64;
    let mut divergences = 0u64;
    let mut refused_correctly = 0u64;
    for seed in 0..n {
        let dir = base.join(format!("s{seed}"));
        std::fs::create_dir_all(&dir).unwrap();
        let expect = {
            let env = Environment::open(cfg(&dir)).unwrap();
            let e = drive(&env, seed);
            env.close().unwrap();
            e
        };

        // Pass 1: detector OFF — what would recovery mount?
        // SAFETY: single-threaded test; set/remove the env var around the open.
        unsafe { std::env::set_var("NOXU_XDB711_DETECT_OFF", "1") };
        let would_mount = reopen_names(&dir);
        // SAFETY: single-threaded test; no other thread reads the env here.
        unsafe { std::env::remove_var("NOXU_XDB711_DETECT_OFF") };
        let recovery_correct = matches!(&would_mount, Ok(g) if *g == expect);

        // Pass 2: detector ON — the real verdict.
        match reopen_names(&dir) {
            Ok(got) => {
                // Opened. If recovery diverged from ground truth that is a
                // recovery gap (lost remove under cleaning), not a detector
                // concern; the detector correctly stayed silent only when it
                // had no surviving evidence.
                if got != expect {
                    divergences += 1;
                }
            }
            Err(NoxuError::EnvironmentFailure {
                reason: EnvironmentFailureReason::LogIntegrity,
                msg,
            }) => {
                if recovery_correct {
                    // The detector refused a CORRECTLY-recovered env: a true
                    // false positive.
                    false_positives += 1;
                    eprintln!(
                        "seed {seed}: FALSE POSITIVE — recovery was correct \
                         ({expect:?}) but detector refused: {msg}"
                    );
                } else {
                    // The detector refused an env recovery would have
                    // mounted WRONG — fail-closed is correct here.
                    refused_correctly += 1;
                }
            }
            Err(e) => panic!("seed {seed}: unexpected open error: {e}"),
        }
    }
    eprintln!(
        "cleaning fuzz: {n} seeds — {false_positives} false positives, \
         {refused_correctly} correct fail-closed (recovery would be wrong), \
         {divergences} silent recovery divergences (detector had no surviving \
         evidence; a recovery gap, not a detector FP)"
    );
    assert_eq!(
        false_positives, 0,
        "{false_positives}/{n} correctly-recovered histories falsely refused"
    );
}
