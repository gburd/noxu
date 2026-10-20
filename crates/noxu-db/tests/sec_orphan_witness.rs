//! NEW-SECMAINT-OLDDATA-TOCTOU — secondary-index integrity under concurrent
//! writes of the SAME primary key.
//!
//! Root cause (coordinator blast-radius probe): `Database::put`'s `do_put`
//! closure reads the pre-image (`old_data_for_secondaries`) via
//! `get_bytes(at, key)` BEFORE the primary write cursor takes its WRITE lock
//! on the key's slot.  Two concurrent writers of the SAME primary key both
//! observe `old_data = None` (neither sees the other's uncommitted write under
//! RC/serializable isolation).  The primary WRITE lock then serialises them,
//! so the second writer's `cursor.put` becomes an UPDATE of the first's
//! committed record — but the secondary maintenance for the second writer
//! still runs with the STALE `old_data = None`, so `update_secondary`:
//!
//!   * with a NON-identity key creator (distinct values → distinct sec keys):
//!     INSERTs the new sec key but NEVER DELETEs the first writer's old sec
//!     key → a SILENT ORPHAN secondary entry (the secondary returns a primary
//!     under a key the primary no longer has) = secondary-index corruption
//!     with NO error raised; or
//!   * with an identity key creator (same sec key): re-INSERTs the identical
//!     (sec_key, pri_key) dup → `KeyExists` → `SecondaryIntegrityException`.
//!
//! JE reads the old data UNDER the primary's write lock inside
//! `Cursor.putNotify` / `putNoNotify` and passes it to
//! `updateSecondary(oldData, newData)` (SecondaryDatabase.java), so the second
//! writer sees the first's committed value as `oldData` and correctly deletes
//! the stale sec key before inserting the new one.
//!
//! The TOCTOU lives in `database.rs` do_put, untouched by the noxu-tree
//! dup-seek Layer B fix, so the result is identical on the base (a8c9929c) and
//! the R-SECREAD tip (4325e6ae).  These witnesses are RED on the unfixed put
//! path (orphan sec key still resolves to the primary / SIE raised) and GREEN
//! once the old-data-under-write-lock fix lands.
//!
//! Orphan detection is done WITHOUT masking: the secondary key IS the primary
//! record's value, so after all writers commit we probe `sec.get_into(v)` for
//! EVERY value any writer used.  Exactly ONE value — the primary's final
//! committed value — may still resolve to `P`; any superseded value that still
//! resolves to `P` is an orphan (a sec key the primary no longer has).

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    Get, OperationStatus, SecondaryConfig, SecondaryDatabase,
    SecondaryKeyCreator,
};
use noxu_sync::Mutex;
use std::collections::BTreeSet;
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;

/// Fixed primary key under contention.
const P: &[u8] = b"pri-key-P";

/// sec key = the primary record's DATA (value).  Distinct values therefore map
/// to DISTINCT secondary keys — the orphan-producing shape.
struct ValueKeyCreator;
impl SecondaryKeyCreator for ValueKeyCreator {
    fn create_secondary_key(
        &self,
        _db: &Database,
        _key: &DatabaseEntry,
        data: &DatabaseEntry,
        result: &mut DatabaseEntry,
    ) -> bool {
        let d = data.data_opt().unwrap_or(&[]);
        if d.is_empty() {
            return false;
        }
        result.set_data(d);
        true
    }
}

struct Env {
    env: Arc<Environment>,
    primary_lf: Arc<Database>,
    sec: Arc<SecondaryDatabase>,
}

impl Env {
    fn open(dir: &TempDir, serializable: bool) -> Self {
        let mut env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            .with_txn_timeout(10_000)
            .with_lock_timeout(10_000);
        if serializable {
            env_cfg = env_cfg.with_txn_serializable_isolation(true);
        }
        let env = Arc::new(Environment::open(env_cfg).unwrap());

        let primary = Arc::new(Mutex::new(
            env.open_database(
                None,
                "foo",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .unwrap(),
        ));
        let inner = env
            .open_database(
                None,
                "fooSec",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true)
                    .with_sorted_duplicates(true),
            )
            .unwrap();
        let sec = Arc::new(
            SecondaryDatabase::open(
                Arc::clone(&primary),
                inner,
                SecondaryConfig::new()
                    .with_allow_create(true)
                    .with_allow_populate(true)
                    .with_sorted_duplicates(true)
                    .with_key_creator(Box::new(ValueKeyCreator)),
            )
            .unwrap(),
        );
        let primary_lf = Arc::new(primary.lock().handle_clone());
        Env { env, primary_lf, sec }
    }

    /// Does secondary key `v` resolve to primary `P`?
    fn sec_resolves_to_p(&self, v: &[u8]) -> bool {
        let txn = self.env.begin_transaction(None).unwrap();
        let mut pk = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        let found = self.sec.get_into(Some(&txn), v, &mut pk, &mut d).unwrap();
        let hit = found && pk.data_opt().unwrap_or(&[]) == P;
        txn.commit().unwrap();
        hit
    }

    fn primary_value(&self) -> Option<Vec<u8>> {
        let txn = self.env.begin_transaction(None).unwrap();
        let mut d = DatabaseEntry::new();
        let mut k = DatabaseEntry::from_bytes(P);
        let mut c = self.primary_lf.open_cursor_in(&txn, None).unwrap();
        let st = c.get(&mut k, &mut d, Get::Search, None).unwrap();
        let v = if st == OperationStatus::Success {
            Some(d.data_opt().unwrap_or(&[]).to_vec())
        } else {
            None
        };
        c.close().unwrap();
        txn.commit().unwrap();
        v
    }
}

fn is_retryable(e: &noxu_db::NoxuError) -> bool {
    let s = e.to_string().to_lowercase();
    s.contains("deadlock")
        || (s.contains("lock")
            && (s.contains("timeout")
                || s.contains("conflict")
                || s.contains("not available")))
}

/// For every value any writer used, the only value that may still resolve to
/// `P` is the primary's FINAL committed value.  Any superseded value that
/// still resolves to `P` is an ORPHAN secondary entry.
fn assert_no_orphans(
    env: &Env,
    values: &BTreeSet<Vec<u8>>,
    round: usize,
    ser: bool,
) {
    let pv = env.primary_value().expect("primary P must exist after puts");
    let mut orphans = Vec::new();
    for v in values {
        if v.as_slice() == pv.as_slice() {
            continue;
        }
        if env.sec_resolves_to_p(v) {
            orphans.push(String::from_utf8_lossy(v).into_owned());
        }
    }
    assert!(
        orphans.is_empty(),
        "round {round} ser={ser}: ORPHAN secondary entries resolve to P \
         (primary's final value is {:?}): {:?} — the superseded value's sec \
         key was never deleted (NEW-SECMAINT-OLDDATA-TOCTOU)",
        String::from_utf8_lossy(&pv),
        orphans
    );
    assert!(
        env.sec_resolves_to_p(&pv),
        "round {round} ser={ser}: the primary's final value {:?} does NOT \
         resolve through the secondary — missing sec entry",
        String::from_utf8_lossy(&pv)
    );
}

/// N auto-commit writers put (P, value_i) with DISTINCT values concurrently,
/// ×ROUNDS.  Orphan probe.
fn run_put_vs_put(threads: usize, rounds: usize, serializable: bool) {
    for round in 0..rounds {
        let dir = TempDir::new().unwrap();
        let s = Arc::new(Env::open(&dir, serializable));
        let seed = format!("v-seed-{round}");
        {
            let txn = s.env.begin_transaction(None).unwrap();
            s.primary_lf.put_in(&txn, P, seed.as_bytes()).unwrap();
            txn.commit().unwrap();
        }
        let mut values: BTreeSet<Vec<u8>> = BTreeSet::new();
        values.insert(seed.into_bytes());
        for t in 0..threads {
            values.insert(format!("v-{round}-t{t}").into_bytes());
        }
        let barrier = Arc::new(Barrier::new(threads));
        let mut hs = Vec::new();
        for t in 0..threads {
            let s = Arc::clone(&s);
            let barrier = Arc::clone(&barrier);
            hs.push(thread::spawn(move || {
                let val = format!("v-{round}-t{t}");
                barrier.wait();
                loop {
                    let txn = s.env.begin_transaction(None).unwrap();
                    match s.primary_lf.put_in(&txn, P, val.as_bytes()) {
                        Ok(_) => match txn.commit() {
                            Ok(_) => break,
                            Err(ref e) if is_retryable(e) => continue,
                            Err(e) => panic!("commit failed: {e:?}"),
                        },
                        Err(ref e) if is_retryable(e) => {
                            let _ = txn.abort();
                        }
                        Err(e) => panic!("put failed: {e:?}"),
                    }
                }
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
        assert_no_orphans(&s, &values, round, serializable);
    }
}

/// One writer deletes P while others put (P, value) concurrently, ×ROUNDS.
/// Whatever the final state, the secondary must be consistent with the
/// primary: if P is present, only its final value resolves; if P is absent, no
/// value resolves to P.
fn run_delete_vs_put(threads: usize, rounds: usize, serializable: bool) {
    for round in 0..rounds {
        let dir = TempDir::new().unwrap();
        let s = Arc::new(Env::open(&dir, serializable));
        let seed = format!("v-seed-{round}");
        {
            let txn = s.env.begin_transaction(None).unwrap();
            s.primary_lf.put_in(&txn, P, seed.as_bytes()).unwrap();
            txn.commit().unwrap();
        }
        let mut values: BTreeSet<Vec<u8>> = BTreeSet::new();
        values.insert(seed.into_bytes());
        for t in 1..threads {
            values.insert(format!("v-{round}-t{t}").into_bytes());
        }
        let barrier = Arc::new(Barrier::new(threads));
        let mut hs = Vec::new();
        for t in 0..threads {
            let s = Arc::clone(&s);
            let barrier = Arc::clone(&barrier);
            let deleter = t == 0;
            hs.push(thread::spawn(move || {
                let val = format!("v-{round}-t{t}");
                barrier.wait();
                loop {
                    let txn = s.env.begin_transaction(None).unwrap();
                    let r = if deleter {
                        s.primary_lf.delete_in(&txn, P).map(|_| ())
                    } else {
                        s.primary_lf.put_in(&txn, P, val.as_bytes())
                    };
                    match r {
                        Ok(_) => match txn.commit() {
                            Ok(_) => break,
                            Err(ref e) if is_retryable(e) => continue,
                            Err(e) => panic!("commit failed: {e:?}"),
                        },
                        Err(ref e) if is_retryable(e) => {
                            let _ = txn.abort();
                        }
                        Err(e) => panic!("op failed: {e:?}"),
                    }
                }
            }));
        }
        for h in hs {
            h.join().unwrap();
        }
        match s.primary_value() {
            Some(_) => assert_no_orphans(&s, &values, round, serializable),
            None => {
                let mut orphans = Vec::new();
                for v in &values {
                    if s.sec_resolves_to_p(v) {
                        orphans.push(String::from_utf8_lossy(v).into_owned());
                    }
                }
                assert!(
                    orphans.is_empty(),
                    "delete_vs_put round {round} ser={serializable}: P absent \
                     but ORPHAN secondary entries still resolve to P: {:?}",
                    orphans
                );
            }
        }
    }
}

// ── put-vs-put, distinct sec keys (ORPHAN probe) ───────────────────────

#[test]
fn sec_orphan_put_vs_put_autocommit_2t() {
    run_put_vs_put(2, 1000, false);
}

#[test]
fn sec_orphan_put_vs_put_autocommit_8t() {
    run_put_vs_put(8, 400, false);
}

#[test]
fn sec_orphan_put_vs_put_serializable_4t() {
    run_put_vs_put(4, 500, true);
}

// ── delete-vs-put (orphan / missing-entry probe) ───────────────────────

#[test]
fn sec_orphan_delete_vs_put_autocommit_4t() {
    run_delete_vs_put(4, 500, false);
}

#[test]
fn sec_orphan_delete_vs_put_serializable_4t() {
    run_delete_vs_put(4, 500, true);
}
