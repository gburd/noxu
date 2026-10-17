// Copyright (C) 2024-2026 Greg Burd.  Apache-2.0 OR MIT.
//! v7.11.1 Option B single-locker auto-commit witnesses.
//!
//! Each is deterministic and is RED on base 6ea18f9d (narrow-window
//! primary↔secondary gap + per-secondary fsync) and GREEN after the
//! single-locker fix.  Covers:
//!   (a) torn PUT: crash between primary and secondary -> after recovery
//!       NEITHER is present (uncommitted auto-txn); crash after the single
//!       commit -> BOTH present.
//!   (b) torn DELETE: no dangling secondary, no SecondaryIntegrityException.
//!   (c) a secondary-maintenance error (key-creator failure) aborts the
//!       PRIMARY write too.
//!   (e) the fsync count is 1 per auto-commit put with N secondaries.
//!   (f) lock-holding: the primary write lock is held until the single
//!       commit, so a concurrent reader cannot see the primary without its
//!       secondary at RC.
//! Witness (d) (a trigger sees the same txn as the primary write) is covered
//! by `db_trig_test::auto_commit_put_fires_with_some_txn_and_commit` and the
//! `kvops_auto` case.
// These witnesses re-exec this test binary as a child and rely on `noxu-db`'s
// `optb_crash_hook` being LINKED into the lib.  The crate's own test / bench
// targets get the `testing` feature via the self dev-dependency in
// `noxu-db/Cargo.toml` (`noxu-db = { path = ".", features = ["testing"] }`),
// so these run under a plain `cargo test` / `cargo test --workspace`; the hook
// is still compiled OUT of every production/consumer build (R-OBHOOK).
#![cfg(all(unix, not(noxu_shuttle)))]

use noxu_db::{
    CheckpointConfig, Database, DatabaseConfig, DatabaseEntry, Durability,
    Environment, EnvironmentConfig, Mutex, SecondaryConfig, SecondaryDatabase,
    SecondaryKeyCreator,
};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Secondary key = the first 9 data bytes.
struct DataKey;
impl SecondaryKeyCreator for DataKey {
    fn create_secondary_key(
        &self,
        _db: &Database,
        _key: &DatabaseEntry,
        data: &DatabaseEntry,
        result: &mut DatabaseEntry,
    ) -> bool {
        match data.data_opt() {
            Some(d) if d.len() >= 9 => {
                result.set_data(&d[..9]);
                true
            }
            _ => false,
        }
    }
}

/// Secondary key = the LAST 9 data bytes (a second, independent index).
struct DataKeyTail;
impl SecondaryKeyCreator for DataKeyTail {
    fn create_secondary_key(
        &self,
        _db: &Database,
        _k: &DatabaseEntry,
        data: &DatabaseEntry,
        result: &mut DatabaseEntry,
    ) -> bool {
        match data.data_opt() {
            Some(d) if d.len() >= 9 => {
                result.set_data(&d[d.len() - 9..]);
                true
            }
            _ => false,
        }
    }
}

fn env_cfg(p: &Path) -> EnvironmentConfig {
    let mut c = EnvironmentConfig::new(p.to_path_buf())
        .with_transactional(true)
        .with_allow_create(true)
        .with_durability(Durability::COMMIT_SYNC)
        .with_node_max_entries(8);
    c.set_run_cleaner(false);
    c.set_run_checkpointer(false);
    c.set_run_in_compressor(false);
    c
}

fn db_cfg(dups: bool) -> DatabaseConfig {
    DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(dups)
}

type Opened = (Environment, Arc<Mutex<Database>>, SecondaryDatabase);

fn open_pri_sec(p: &Path) -> Opened {
    let env = Environment::open(env_cfg(p)).unwrap();
    let pri = Arc::new(Mutex::new(
        env.open_database(None, "pri", &db_cfg(false)).unwrap(),
    ));
    let inner = env.open_database(None, "sec", &db_cfg(true)).unwrap();
    let sec = SecondaryDatabase::open(
        Arc::clone(&pri),
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(DataKey)),
    )
    .unwrap();
    (env, pri, sec)
}

fn image(dir: &Path) -> tempfile::TempDir {
    let img = tempfile::tempdir().unwrap();
    for f in std::fs::read_dir(dir).unwrap() {
        let f = f.unwrap();
        if f.file_type().unwrap().is_file() && f.file_name() != "noxu.lck" {
            std::fs::copy(f.path(), img.path().join(f.file_name())).unwrap();
        }
    }
    img
}

fn sec_present(sec: &SecondaryDatabase, key: &[u8]) -> bool {
    let (mut pk, mut d) = (DatabaseEntry::new(), DatabaseEntry::new());
    sec.get_into(None, key, &mut pk, &mut d).unwrap()
}

// Drives a child that crashes at the named hook on the `zz-after` key, takes
// the on-disk image, and returns it for recovery.  `hook_var` is one of
// NOXU_OPTB_CRASH_BEFORE_COMMIT / _AFTER_COMMIT; `child_mode`/`child_home`
// select the child entry point.
fn run_crash_child(
    test_name: &str,
    mode_env: &str,
    home_env: &str,
    hook_var: &str,
    hook_key: &str,
) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let mut proc = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", test_name, "--nocapture"])
        .env(mode_env, "1")
        .env(home_env, dir.path())
        .env(hook_var, hook_key)
        .spawn()
        .unwrap();
    let start = Instant::now();
    while !dir.path().join("ready").exists() {
        if let Some(st) = proc.try_wait().unwrap() {
            panic!("child exited before ready: {st:?}");
        }
        assert!(start.elapsed() < Duration::from_secs(60), "child timeout");
        std::thread::sleep(Duration::from_millis(5));
    }
    let st = loop {
        if let Some(st) = proc.try_wait().unwrap() {
            break st;
        }
        assert!(start.elapsed() < Duration::from_secs(60), "no abort");
        std::thread::sleep(Duration::from_millis(5));
    };
    eprintln!("child exit: {st:?} (expect SIGABRT, not success)");
    assert!(
        !dir.path().join("NOHOOK").exists(),
        "crash hook did not fire (ran to completion)"
    );
    image(dir.path())
}

// ───────────────────────────── Witness (a): torn PUT ────────────────────────

const A_MODE: &str = "NOXU_OPTB_A_CHILD";
const A_HOME: &str = "NOXU_OPTB_A_HOME";

fn child_put(home: &Path) -> ! {
    let (env, pri, _sec) = open_pri_sec(home);
    for i in 0..10u32 {
        pri.lock()
            .put(
                format!("k{i:05}").into_bytes(),
                format!("val-{i:05}00000").into_bytes(),
            )
            .unwrap();
    }
    env.checkpoint(Some(&CheckpointConfig::new().with_force(true))).unwrap();
    std::fs::write(home.join("ready"), b"").unwrap();
    // Auto-commit put: the engine aborts at the armed hook.
    pri.lock().put(b"zz-after", b"zzzzzzzzzz").unwrap();
    std::fs::write(home.join("NOHOOK"), b"").unwrap();
    env.close().unwrap();
    std::process::exit(0);
}

#[test]
fn a_torn_put_before_commit_neither_primary_nor_secondary() {
    if std::env::var(A_MODE).is_ok() {
        child_put(Path::new(&std::env::var_os(A_HOME).unwrap()));
    }
    let img = run_crash_child(
        "a_torn_put_before_commit_neither_primary_nor_secondary",
        A_MODE,
        A_HOME,
        "NOXU_OPTB_CRASH_BEFORE_COMMIT",
        "zz-after",
    );
    let (env, pri, sec) = open_pri_sec(img.path());
    let pri_present = pri.lock().get(b"zz-after").unwrap().is_some();
    let secp = sec_present(&sec, b"zzzzzzzzz");
    eprintln!(
        "OPTB-A-BEFORE: primary_present={pri_present} secondary_present={secp}"
    );
    sec.close().unwrap();
    pri.lock().close().unwrap();
    env.close().unwrap();
    // The auto-txn never committed: NEITHER the primary NOR the secondary is
    // durable (atomic all-or-nothing).  On the base this is RED (primary
    // present, secondary absent).
    assert!(
        !pri_present && !secp,
        "crash before the single commit must leave NEITHER present; \
         got pri={pri_present} sec={secp}"
    );
}

const A2_MODE: &str = "NOXU_OPTB_A2_CHILD";
const A2_HOME: &str = "NOXU_OPTB_A2_HOME";

fn child_put2(home: &Path) -> ! {
    let (env, pri, _sec) = open_pri_sec(home);
    env.checkpoint(Some(&CheckpointConfig::new().with_force(true))).unwrap();
    std::fs::write(home.join("ready"), b"").unwrap();
    pri.lock().put(b"zz-after", b"zzzzzzzzzz").unwrap();
    std::fs::write(home.join("NOHOOK"), b"").unwrap();
    env.close().unwrap();
    std::process::exit(0);
}

#[test]
fn a_torn_put_after_commit_both_primary_and_secondary() {
    if std::env::var(A2_MODE).is_ok() {
        child_put2(Path::new(&std::env::var_os(A2_HOME).unwrap()));
    }
    let img = run_crash_child(
        "a_torn_put_after_commit_both_primary_and_secondary",
        A2_MODE,
        A2_HOME,
        "NOXU_OPTB_CRASH_AFTER_COMMIT",
        "zz-after",
    );
    let (env, pri, sec) = open_pri_sec(img.path());
    let pri_present = pri.lock().get(b"zz-after").unwrap().as_deref()
        == Some(&b"zzzzzzzzzz"[..]);
    let secp = sec_present(&sec, b"zzzzzzzzz");
    eprintln!(
        "OPTB-A-AFTER: primary_present={pri_present} secondary_present={secp}"
    );
    sec.close().unwrap();
    pri.lock().close().unwrap();
    env.close().unwrap();
    // Crash AFTER the single commit: BOTH durable.
    assert!(
        pri_present && secp,
        "crash after the single commit must leave BOTH present; \
         got pri={pri_present} sec={secp}"
    );
}

// ──────────────────────────── Witness (b): torn DELETE ──────────────────────

const B_MODE: &str = "NOXU_OPTB_B_CHILD";
const B_HOME: &str = "NOXU_OPTB_B_HOME";

fn child_del(home: &Path) -> ! {
    let (env, pri, _sec) = open_pri_sec(home);
    // A durable record whose secondary entry exists.  Insert under an
    // EXPLICIT txn so the setup put does NOT go through
    // `with_auto_commit_transaction` and so does NOT trip the
    // `dd-key`-armed crash hook (only the auto-commit DELETE below should).
    {
        let txn = env.begin_transaction(None).unwrap();
        pri.lock().put_in(&txn, b"dd-key", b"ddddddddddX").unwrap();
        txn.commit().unwrap();
    }
    env.checkpoint(Some(&CheckpointConfig::new().with_force(true))).unwrap();
    std::fs::write(home.join("ready"), b"").unwrap();
    // Auto-commit DELETE: engine aborts BEFORE the single commit.
    pri.lock().delete(b"dd-key").unwrap();
    std::fs::write(home.join("NOHOOK"), b"").unwrap();
    env.close().unwrap();
    std::process::exit(0);
}

#[test]
fn b_torn_delete_no_dangling_secondary() {
    if std::env::var(B_MODE).is_ok() {
        child_del(Path::new(&std::env::var_os(B_HOME).unwrap()));
    }
    let img = run_crash_child(
        "b_torn_delete_no_dangling_secondary",
        B_MODE,
        B_HOME,
        "NOXU_OPTB_CRASH_BEFORE_COMMIT",
        "dd-key",
    );
    let (env, pri, sec) = open_pri_sec(img.path());
    let pri_present = pri.lock().get(b"dd-key").unwrap().is_some();
    // Read the secondary.  A torn delete on the base leaves a dangling
    // secondary -> SecondaryIntegrityException or a stale mapping.  After the
    // fix the delete never committed, so BOTH the primary AND its secondary
    // survive intact and the read-through succeeds cleanly.
    let (mut pk, mut d) = (DatabaseEntry::new(), DatabaseEntry::new());
    let readthrough = sec.get_into(None, b"ddddddddd", &mut pk, &mut d);
    let integrity_err = matches!(&readthrough, Err(e)
        if format!("{e:?}").contains("SecondaryIntegrity")
            || format!("{e:?}").contains("missing primary"));
    let found = readthrough.unwrap_or(false);
    eprintln!(
        "OPTB-B: primary_present={pri_present} sec_found={found} \
         integrity_err={integrity_err}"
    );
    sec.close().unwrap();
    pri.lock().close().unwrap();
    env.close().unwrap();
    // The delete's auto-txn never committed: the record is intact (primary
    // present, secondary resolves to it), and there is NO dangling secondary
    // and NO SecondaryIntegrityException.  On the base this is RED (primary
    // absent, dangling secondary / integrity error).
    assert!(
        pri_present && found && !integrity_err,
        "crash before the single delete-commit must leave the record intact \
         with no dangling secondary; got pri={pri_present} found={found} \
         integrity_err={integrity_err}"
    );
}

// ───────── Witness (c): a secondary-maintenance error aborts the primary ─────

/// A key-creator that fails (panics would abort; instead it signals "no key"
/// is NOT an error).  To force a secondary-maintenance *error* we use a
/// foreign-key constraint: inserting a secondary key absent from the foreign
/// db raises ForeignConstraintViolation, which must abort the PRIMARY write.
#[test]
fn c_secondary_error_aborts_primary_write() {
    let dir = tempfile::tempdir().unwrap();
    let env = Environment::open(env_cfg(dir.path())).unwrap();
    let pri = Arc::new(Mutex::new(
        env.open_database(None, "pri", &db_cfg(false)).unwrap(),
    ));
    // Foreign db holds the set of allowed secondary keys.
    let foreign = Arc::new(Mutex::new(
        env.open_database(None, "foreign", &db_cfg(false)).unwrap(),
    ));
    foreign.lock().put(b"GOODKEY-1", b"x").unwrap();

    let inner = env.open_database(None, "sec", &db_cfg(true)).unwrap();
    let sec = SecondaryDatabase::open(
        Arc::clone(&pri),
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(DataKey))
            .with_foreign_key_database_handle(Arc::clone(&foreign)),
    )
    .unwrap();

    // Insert whose secondary key ("BADKEY-99") is NOT in the foreign db:
    // secondary maintenance must raise ForeignConstraintViolation, and the
    // single-locker auto-txn must abort -> the PRIMARY write is undone too.
    let bad = pri.lock().put(b"p1", b"BADKEY-99ignored");
    eprintln!("OPTB-C: put result = {bad:?}");
    assert!(bad.is_err(), "FK-violating secondary insert must fail the put");

    // The primary must NOT be present (the whole auto-txn aborted).  On the
    // base, the primary auto-committed BEFORE secondary maintenance, so it
    // survived the secondary failure -> RED.
    let p = pri.lock().get(b"p1").unwrap();
    eprintln!("OPTB-C: primary after failed put = {p:?}");
    assert!(
        p.is_none(),
        "a secondary-maintenance error must abort (undo) the PRIMARY write; \
         primary is still present: {p:?}"
    );
    // A good insert still works.
    pri.lock().put(b"p2", b"GOODKEY-1ignored").unwrap();
    assert!(pri.lock().get(b"p2").unwrap().is_some());

    sec.close().unwrap();
    pri.lock().close().unwrap();
    foreign.lock().close().unwrap();
    env.close().unwrap();
}

// ───────── Witness (e): fsync count is 1 per auto-commit put, N secondaries ──

fn open_pri_two_sec(
    p: &Path,
) -> (Environment, Arc<Mutex<Database>>, SecondaryDatabase, SecondaryDatabase) {
    let env = Environment::open(env_cfg(p)).unwrap();
    let pri = Arc::new(Mutex::new(
        env.open_database(None, "pri", &db_cfg(false)).unwrap(),
    ));
    let in1 = env.open_database(None, "s1", &db_cfg(true)).unwrap();
    let s1 = SecondaryDatabase::open(
        Arc::clone(&pri),
        in1,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(DataKey)),
    )
    .unwrap();
    let in2 = env.open_database(None, "s2", &db_cfg(true)).unwrap();
    let s2 = SecondaryDatabase::open(
        Arc::clone(&pri),
        in2,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(DataKeyTail)),
    )
    .unwrap();
    (env, pri, s1, s2)
}

#[test]
fn e_fsync_count_is_one_per_auto_commit_put_with_two_secondaries() {
    use noxu_log::file_handle::fsync_probe;
    let dir = tempfile::tempdir().unwrap();
    let (env, pri, s1, s2) = open_pri_two_sec(dir.path());
    let n = 200u32;
    // Warm up (create files) before arming so file-creation flushes don't
    // skew the count.
    pri.lock().put(b"warm", b"warmwarmwarm").unwrap();
    fsync_probe::arm(0);
    for i in 0..n {
        pri.lock()
            .put(
                format!("b{i:06}").into_bytes(),
                format!("val-{i:06}-xyz").into_bytes(),
            )
            .unwrap();
    }
    let fsyncs =
        fsync_probe::COMPLETED.load(std::sync::atomic::Ordering::SeqCst);
    fsync_probe::disarm();
    let per_put = fsyncs as f64 / n as f64;
    eprintln!(
        "OPTB-E: n_puts={n} secondaries=2 completed_fdatasyncs={fsyncs} \
         per_put={per_put:.2}"
    );
    s2.close().unwrap();
    s1.close().unwrap();
    pri.lock().close().unwrap();
    env.close().unwrap();
    // JE fsyncs once per auto-commit op regardless of N secondaries.  The
    // base (6ea18f9d) fsyncs 1 (primary) + N (each secondary) = 3.00 per put;
    // this fix must be 1.00 (single TxnCommit fsync covers the whole batch).
    // Allow a hair over 1 for incidental file flips, but far below 2.
    assert!(
        per_put < 1.5,
        "auto-commit put with 2 secondaries must fsync ~1x (one commit); \
         got {per_put:.2} ({fsyncs} for {n} puts) -- base was 3.00"
    );
}

// ───────── Witness (f): primary write lock held until the single commit ─────

#[test]
fn f_primary_write_lock_held_until_single_commit() {
    // A concurrent reader at ReadCommitted must not see the primary without
    // its secondary: the single-locker fix holds the primary record write
    // lock until the one commit, so the reader either sees NEITHER (before
    // commit) or BOTH (after).  We assert the reader never observes a primary
    // whose secondary is missing.
    use noxu_db::{LockMode, ReadOptions};
    let dir = tempfile::tempdir().unwrap();
    let (env, pri, sec) = open_pri_sec(dir.path());
    let env = Arc::new(env);
    let sec = Arc::new(sec);

    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let violations = Arc::new(std::sync::atomic::AtomicU64::new(0));

    // Reader thread: repeatedly, for each writer key, if the primary is
    // visible at RC then its secondary entry MUST also be visible.
    let reader = {
        let pri = Arc::clone(&pri);
        let sec = Arc::clone(&sec);
        let stop = Arc::clone(&stop);
        let violations = Arc::clone(&violations);
        std::thread::spawn(move || {
            let rc = ReadOptions::new().with_lock_mode(LockMode::ReadCommitted);
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                for i in 0..50u32 {
                    let k = format!("w{i:05}").into_bytes();
                    let p = pri
                        .lock()
                        .get_with_options(None, &k, &rc)
                        .unwrap_or(None);
                    if let Some(v) = p {
                        // Primary visible -> its secondary key (first 9 bytes
                        // of the value) must resolve back to a primary.
                        if v.len() >= 9 {
                            let (mut pk, mut d) =
                                (DatabaseEntry::new(), DatabaseEntry::new());
                            let found = sec
                                .get_into(None, &v[..9], &mut pk, &mut d)
                                .unwrap_or(false);
                            if !found {
                                violations.fetch_add(
                                    1,
                                    std::sync::atomic::Ordering::Relaxed,
                                );
                            }
                        }
                    }
                }
            }
        })
    };

    // Writer: auto-commit puts (each a single-locker atomic unit).  The
    // value's first 9 bytes are the secondary key, distinct per record.
    for i in 0..50u32 {
        let k = format!("w{i:05}").into_bytes();
        let v = format!("sk{i:05}xx-{i:05}").into_bytes();
        pri.lock().put(k, v).unwrap();
    }
    // Let the reader observe the committed state for a bit.
    std::thread::sleep(Duration::from_millis(200));
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    reader.join().unwrap();

    let v = violations.load(std::sync::atomic::Ordering::Relaxed);
    eprintln!("OPTB-F: primary-without-secondary observations = {v}");
    sec.close().unwrap();
    pri.lock().close().unwrap();
    Arc::try_unwrap(env).ok().unwrap().close().unwrap();
    assert_eq!(
        v, 0,
        "a concurrent RC reader must never see a primary without its \
         secondary (the write lock is held until the single commit)"
    );
}
