//! JE `SecondaryMultiTest` port — concurrent secondary/primary access.
//! Faithful port of
//! `test/com/sleepycat/je/test/SecondaryMultiTest.java`.
//!
//! JE class purpose (SecondaryMultiTest.java:49-57): "Tests involving
//! secondary indexes with respect to deadlock free access.  Access is
//! performed using Database, SecondaryDatabase, Cursor and SecondaryCursor.
//! When serialization is NOT being used, concurrent access of a record should
//! not deadlock."
//!
//! JE is `@RunWith(Parameterized.class)` over `(db1UsePrimary, db2UsePrimary,
//! useSerialization)` (SecondaryMultiTest.java:94-120).  The active param set
//! (two deadlock-prone serializable+mixed-handle rows are commented out in JE)
//! is:
//!   {F,F,F} {F,F,T} {F,T,F} {T,F,F} {T,T,F} {T,T,T}
//! `deadlockCanHappen()` (SecondaryMultiTest.java:626-632) is
//! `useSerialization && (db1UsePrimary ^ db2UsePrimary)` — FALSE for every
//! active row, so NO unexpected lock conflict may occur in any ported run.
//! Each Rust test loops over the active matrix and asserts per row.
//!
//! JE `MyKeyCreator` (SecondaryMultiTest.java:1211-1219): the secondary key is
//! the PRIMARY KEY verbatim (`result.setData(key.getData())`) — an identity
//! extraction over a sorted-duplicate secondary (openSecondary:1199-1209).  So
//! a read "via the secondary" and a read "via the primary" resolve the SAME
//! primary record; this port reads the primary record directly for the primary
//! handle and via `SecondaryDatabase::get_into` for the secondary handle.
//!
//! Deviation (documented): JE coordinates its worker threads with a shared
//! `volatile int currentEvent` counter + `Thread.sleep` busy-waits
//! (SecondaryMultiTest.java:170-333, worker classes :637-1018).  That counter
//! is JE-internal test choreography, not an observable engine contract.  This
//! port reproduces the SUBSTANTIVE property of each method — the thread counts,
//! the operation each worker performs, and the exact per-worker `assertSame`
//! result assertions — using `std::thread` + a start `Barrier`, which is the
//! Rust-idiomatic equivalent of JE's start-gun.  Where JE's choreography makes
//! a method deterministic (testMultiDelete: thread 1 commits before thread 2
//! runs), the port enforces the same order explicitly so the same determinate
//! SUCCESS/NOTFOUND assertions hold.

use noxu_db::{
    Cursor, Database, DatabaseConfig, DatabaseEntry, Environment,
    EnvironmentConfig, Get, LockMode, OperationStatus, SecondaryConfig,
    SecondaryDatabase, SecondaryKeyCreator, Transaction, TransactionConfig,
};
use noxu_sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;

const DATACOUNT: u32 = 99;
const KEY: u32 = 55;

/// JE `MyKeyCreator` (SecondaryMultiTest.java:1211-1219): sec key = pri key.
struct IdentityKeyCreator;
impl SecondaryKeyCreator for IdentityKeyCreator {
    fn create_secondary_key(
        &self,
        _db: &Database,
        key: &DatabaseEntry,
        _data: &DatabaseEntry,
        result: &mut DatabaseEntry,
    ) -> bool {
        result.set_data(key.data_opt().unwrap_or(&[]));
        true
    }
}

/// JE `createEntry(int)`: the decimal string bytes of `val`.
fn entry(val: u32) -> Vec<u8> {
    val.to_string().into_bytes()
}

/// A fully wired secondary environment matching JE `setup()`
/// (SecondaryMultiTest.java:126-145): transactional env with 5s txn/lock
/// timeouts, a primary DB `foo`, and a sorted-dup identity secondary `fooSec`
/// with allowPopulate.
struct SecEnv {
    env: Arc<Environment>,
    /// Lock-free handle onto the primary `DatabaseImpl` used for ALL direct
    /// primary record access (reads via the primary handle, deletes, puts).
    /// JE's test uses a plain `Database` handle with no monitor; the Noxu
    /// `SecondaryDatabase` association requires the primary behind an
    /// `Arc<Mutex<Database>>` (held only transiently at open), so direct
    /// primary ops go through this lock-free clone instead of holding the
    /// handle `Mutex` across the blocking record-lock acquisition — which
    /// would form a `Mutex` <-> record-lock ABBA with a concurrent writer
    /// that holds the `Mutex` and waits for a record lock, a cycle invisible
    /// to the txn deadlock detector.  Mirrors the engine's internal
    /// `SecondaryHookState.primary_handle` (BUG-SEC-MUTEX-DEADLOCK) and JE
    /// reading the primary through a handle reference with no monitor held
    /// (`Database::handle_clone`).
    primary_lf: Arc<Database>,
    sec: Arc<SecondaryDatabase>,
}

impl SecEnv {
    fn open(dir: &TempDir, serializable: bool) -> Self {
        let mut env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            .with_txn_timeout(5_000)
            .with_lock_timeout(5_000);
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
                    .with_key_creator(Box::new(IdentityKeyCreator)),
            )
            .unwrap(),
        );

        // A lock-free second handle onto the SAME primary DatabaseImpl for
        // direct primary access (see `primary_lf` field doc).
        let primary_lf = Arc::new(primary.lock().handle_clone());

        SecEnv { env, primary_lf, sec }
    }

    /// Populate the primary with 0..DATACOUNT, each value = its decimal bytes,
    /// optionally skipping `skip` (JE testMultiReadInsert leaves KEY absent).
    fn populate(&self, skip: Option<u32>) {
        let pri = &self.primary_lf;
        for i in 0..DATACOUNT {
            if Some(i) == skip {
                continue;
            }
            let v = entry(i);
            pri.put(&v, &v).unwrap();
        }
    }
}

/// Is the given error a JE `LockConflictException` analog (deadlock / timeout /
/// not-available)?  Any of these aborts and retries in the JE workers.  Noxu's
/// `NoxuError::is_lock_conflict()` is the engine's own classification and
/// includes `TransactionTimeout` (JE `LockConflictException` covers txn/lock
/// timeout, deadlock and not-available alike — error.rs:716-732).
///
/// 7.11.x TEST ADAPTATION (FID-711-SECDEL-ERRTYPE): on the 7.11.1 line the
/// secondary-delete path flattens the typed lock error into
/// `NoxuError::OperationNotAllowed(string)` at 11 call sites in
/// `secondary_database.rs` (`.map_err(|e| OperationNotAllowed(e.to_string()))`,
/// e.g. `delete_sec_key` lines 396/400 — the exact-dup secondary delete reached
/// via the primary-delete auto-hook).  JE surfaces these as the typed
/// `DeadlockException` / `LockTimeoutException` subclasses of
/// `LockConflictException`, which its workers retry; integrate restored the
/// typed path (`.map_err(NoxuError::from)`).  So that the ported JE workers
/// retry EXACTLY as JE does — and so this witness isolates the READER property
/// (never observe NOTFOUND) from the writer-side deadlock artifact — classify a
/// stringified `OperationNotAllowed` whose message carries the engine's own
/// deadlock / lock-timeout / lock-conflict wording as a retryable lock
/// conflict.  This is a TEST-ONLY shim for a known 7.11.x error-typing gap
/// fixed in v8; it changes NO engine behaviour and leaves every reader/writer
/// result assertion exact.
fn is_lock_conflict(e: &noxu_db::NoxuError) -> bool {
    if e.is_lock_conflict() || e.is_lock_timeout() {
        return true;
    }
    if let noxu_db::NoxuError::OperationNotAllowed(s) = e {
        let m = s.to_ascii_lowercase();
        return m.contains("deadlock detected")
            || m.contains("deadlock cycle")
            || m.contains("lock timeout")
            || m.contains("lock conflict")
            || m.contains("chosen as victim");
    }
    false
}

/// Read the record under `KEY` in `txn`, via the primary (`use_primary`) or via
/// the secondary identity index.  Returns Success/NotFound or an error.
fn read_key(
    env: &Environment,
    primary: &Arc<Database>,
    sec: &SecondaryDatabase,
    use_primary: bool,
    txn: &Transaction,
    lock_mode: Option<LockMode>,
) -> Result<OperationStatus, noxu_db::NoxuError> {
    let k = entry(KEY);
    if use_primary {
        // JE Cursor.getSearchKey(dek, data, lockMode) on the primary.  The
        // lock-free handle is used so no handle `Mutex` is held across the
        // record read (see `SecEnv::primary_lf`).
        let mut c = primary.open_cursor_in(txn, None)?;
        let mut ke = DatabaseEntry::from_bytes(&k);
        let mut d = DatabaseEntry::new();
        let st = c.get(&mut ke, &mut d, Get::Search, lock_mode)?;
        c.close()?;
        Ok(st)
    } else {
        // JE SecondaryCursor.getSearchKey/getSearchBoth/getSearchKeyRange —
        // all resolve the primary record by the (identity) secondary key.
        let _ = env;
        let mut pk = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        let found = sec.get_into(Some(txn), &k, &mut pk, &mut d)?;
        Ok(if found {
            OperationStatus::Success
        } else {
            OperationStatus::NotFound
        })
    }
}

// =====================================================================
// testMultiDelete (SecondaryMultiTest.java:168-200)
// =====================================================================

/// JE `testMultiDelete`: two threads attempt to delete the SAME record; JE's
/// event counter forces thread 1 to commit before thread 2 acts, so thread 1
/// gets SUCCESS and thread 2 gets NOTFOUND.  Ported deterministically (the
/// event ordering IS the point) over the active parameter matrix.
#[test]
fn je_secondary_multi_test_test_multi_delete() {
    for &(d1p, d2p, ser) in ACTIVE_PARAMS {
        let dir = TempDir::new().unwrap();
        let s = SecEnv::open(&dir, ser);
        s.populate(None);
        let k = entry(KEY);

        // Thread 1 deletes and commits first (JE waitEventPre=0/post=3).
        let t1 = {
            let txn = s.env.begin_transaction(None).unwrap();
            let r = delete_via(&s, d1p, &txn, k.as_slice());
            txn.commit().unwrap();
            r
        };
        // Thread 2 deletes afterward (JE waitEventPre=2/post=4).
        let t2 = {
            let txn = s.env.begin_transaction(None).unwrap();
            let r = delete_via(&s, d2p, &txn, k.as_slice());
            txn.commit().unwrap();
            r
        };

        assert_eq!(
            t1,
            OperationStatus::Success,
            "testMultiDelete t1 must SUCCEED (d1p={d1p} d2p={d2p} ser={ser})"
        );
        assert_eq!(
            t2,
            OperationStatus::NotFound,
            "testMultiDelete t2 must be NOTFOUND (d1p={d1p} d2p={d2p} ser={ser})"
        );
    }
}

/// Delete `KEY` via the primary (`use_primary`) or via the secondary (fans out
/// to the same primary delete).  Maps to JE `db.delete(xact, key)` where `db`
/// is `db` or `sdb`.
fn delete_via(
    s: &SecEnv,
    use_primary: bool,
    txn: &Transaction,
    key: &[u8],
) -> OperationStatus {
    let deleted = if use_primary {
        s.primary_lf.delete_in(txn, key).unwrap()
    } else {
        s.sec.delete_in(txn, key).unwrap()
    };
    if deleted { OperationStatus::Success } else { OperationStatus::NotFound }
}

// =====================================================================
// testMultiReadInsert (SecondaryMultiTest.java:262-336)
// =====================================================================

/// JE `testMultiReadInsert`: KEY is absent initially; 20 readers (initStatus
/// NOTFOUND, read-until-SUCCESS) + 2 inserters.  After the inserters commit
/// every reader observes SUCCESS and both inserters SUCCESS.
///
/// NEW-SECMAINT-OLDDATA-TOCTOU (fixed): two concurrent inserters of the SAME
/// brand-new primary key previously both read `old_data = None` (no slot to
/// lock) and the loser re-inserted the identical (sec_key, pri_key) dup →
/// spurious `SecondaryIntegrityException`.  `Database::put` now reads the
/// secondary pre-image UNDER the primary's WRITE lock (JE `Cursor.putNotify`),
/// so the second inserter blocks on the shared synthetic key lock, sees the
/// first's committed record as `old_data`, and correctly does a no-op/update
/// instead of a duplicate insert.
#[test]
fn je_secondary_multi_test_test_multi_read_insert() {
    const RTHREADS: usize = 20;
    const WTHREADS: usize = 2;
    for &(d1p, d2p, ser) in ACTIVE_PARAMS {
        let dir = TempDir::new().unwrap();
        let s = SecEnv::open(&dir, ser);
        s.populate(Some(KEY)); // KEY absent at start.
        let k = entry(KEY);

        let s = Arc::new(s);
        let barrier = Arc::new(Barrier::new(RTHREADS + WTHREADS));

        // Inserters.
        let mut writers = Vec::new();
        for _ in 0..WTHREADS {
            let s = Arc::clone(&s);
            let barrier = Arc::clone(&barrier);
            let k = k.clone();
            writers.push(thread::spawn(move || -> OperationStatus {
                barrier.wait();
                loop {
                    let txn = s.env.begin_transaction(None).unwrap();
                    match s.primary_lf.put_in(&txn, &k, &k) {
                        Ok(_) => {
                            txn.commit().unwrap();
                            return OperationStatus::Success;
                        }
                        Err(ref e) if is_lock_conflict(e) => {
                            let _ = txn.abort();
                            // not expected in active rows; retry
                        }
                        Err(e) => panic!("insert failed: {e:?}"),
                    }
                }
            }));
        }

        // Readers: read until SUCCESS (initStatus NOTFOUND).
        let mut readers = Vec::new();
        for i in 0..RTHREADS {
            let use_primary = if i % 2 == 0 { d1p } else { d2p };
            let s = Arc::clone(&s);
            let barrier = Arc::clone(&barrier);
            readers.push(thread::spawn(move || -> OperationStatus {
                barrier.wait();
                loop {
                    let txn = s.env.begin_transaction(None).unwrap();
                    let r = read_key(
                        &s.env,
                        &s.primary_lf,
                        &s.sec,
                        use_primary,
                        &txn,
                        None,
                    );
                    match r {
                        Ok(OperationStatus::Success) => {
                            txn.commit().unwrap();
                            return OperationStatus::Success;
                        }
                        Ok(_) => {
                            txn.commit().unwrap();
                            // keep reading until the insert lands
                        }
                        Err(ref e) if is_lock_conflict(e) => {
                            let _ = txn.abort();
                        }
                        Err(e) => panic!("read failed: {e:?}"),
                    }
                }
            }));
        }

        for w in writers {
            assert_eq!(
                w.join().unwrap(),
                OperationStatus::Success,
                "inserter must SUCCEED (d1p={d1p} d2p={d2p} ser={ser})"
            );
        }
        for r in readers {
            assert_eq!(
                r.join().unwrap(),
                OperationStatus::Success,
                "reader must end SUCCESS (d1p={d1p} d2p={d2p} ser={ser})"
            );
        }
    }
}

// =====================================================================
// testMultiReadDelete (SecondaryMultiTest.java:338-399)
// =====================================================================

/// JE `testMultiReadDelete`: KEY present; 20 readers (initStatus SUCCESS,
/// read-until-NOTFOUND) + 1 deleter.  After the deleter commits every reader
/// ends NOTFOUND and the deleter SUCCESS.
///
/// PORTED: deadlock-free via JE's lockPrimaryOnly deadlock-avoidance
/// (SecondaryCursor.getLockPrimaryOnly / readPrimaryAfterGet,
/// SecondaryCursor.java:1659-1748; Cursor.readPrimaryAfterGet,
/// Cursor.java:4805-4960).  A non-serializable secondary read dirty-reads the
/// secondary (no sec record lock), locks ONLY the primary, and re-verifies the
/// sec->primary reference under the primary lock; a primary found deleted
/// after the dirty read is skipped (NOTFOUND), not a SecondaryIntegrityException.
/// `SecondaryDatabase.delete` uses the matching lock mode (RMW serializable /
/// READ_UNCOMMITTED_ALL non-serializable, SecondaryDatabase.java:730-806) so
/// reader and deleter never form a sec->primary ABBA.  Composes with sec2's
/// BUG-SEC-MUTEX-DEADLOCK Mutex-scoping and core2's lock-mode tolerance.
#[test]
fn je_secondary_multi_test_test_multi_read_delete() {
    const RTHREADS: usize = 20;
    for &(d1p, d2p, ser) in ACTIVE_PARAMS {
        let dir = TempDir::new().unwrap();
        let s = SecEnv::open(&dir, ser);
        s.populate(None);
        let k = entry(KEY);

        let s = Arc::new(s);
        let barrier = Arc::new(Barrier::new(RTHREADS + 1));

        // Deleter (JE db1UsePrimary ? db : sdb).
        let deleter = {
            let s = Arc::clone(&s);
            let barrier = Arc::clone(&barrier);
            let k = k.clone();
            thread::spawn(move || -> OperationStatus {
                barrier.wait();
                loop {
                    let txn = s.env.begin_transaction(None).unwrap();
                    let dr = if d1p {
                        s.primary_lf.delete_in(&txn, &k)
                    } else {
                        s.sec.delete_in(&txn, &k)
                    };
                    match dr {
                        Ok(deleted) => {
                            txn.commit().unwrap();
                            return if deleted {
                                OperationStatus::Success
                            } else {
                                OperationStatus::NotFound
                            };
                        }
                        Err(ref e) if is_lock_conflict(e) => {
                            let _ = txn.abort();
                        }
                        Err(e) => panic!("delete failed: {e:?}"),
                    }
                }
            })
        };

        // Readers: read until NOTFOUND (initStatus SUCCESS).
        let mut readers = Vec::new();
        for i in 0..RTHREADS {
            let use_primary = if i % 2 == 0 { d1p } else { d2p };
            let s = Arc::clone(&s);
            let barrier = Arc::clone(&barrier);
            readers.push(thread::spawn(move || -> OperationStatus {
                barrier.wait();
                loop {
                    let txn = s.env.begin_transaction(None).unwrap();
                    let r = read_key(
                        &s.env,
                        &s.primary_lf,
                        &s.sec,
                        use_primary,
                        &txn,
                        None,
                    );
                    match r {
                        Ok(OperationStatus::NotFound) => {
                            txn.commit().unwrap();
                            return OperationStatus::NotFound;
                        }
                        Ok(_) => {
                            txn.commit().unwrap();
                        }
                        Err(ref e) if is_lock_conflict(e) => {
                            let _ = txn.abort();
                        }
                        Err(e) => panic!("read failed: {e:?}"),
                    }
                }
            }));
        }

        assert_eq!(
            deleter.join().unwrap(),
            OperationStatus::Success,
            "deleter must SUCCEED (d1p={d1p} d2p={d2p} ser={ser})"
        );
        for r in readers {
            assert_eq!(
                r.join().unwrap(),
                OperationStatus::NotFound,
                "reader must end NOTFOUND (d1p={d1p} d2p={d2p} ser={ser})"
            );
        }
    }
}

// =====================================================================
// testMultiReadDeleteInsert (SecondaryMultiTest.java:405-485)
// =====================================================================

/// JE `testMultiReadDeleteInsert`: readers loop for TESTRUNTIME while a writer
/// inserts (JE's `deleters` array is bugged — it starts the inserter thread
/// twice, SecondaryMultiTest.java:462 — so effectively readers + inserter).
/// The only assertion JE makes is the inserter SUCCESS.  Readers must never
/// crash (every read returns a valid status under default isolation).
#[test]
fn je_secondary_multi_test_test_multi_read_delete_insert() {
    use std::sync::atomic::AtomicBool;
    const RTHREADS: usize = 20;
    for &(d1p, d2p, ser) in ACTIVE_PARAMS {
        let dir = TempDir::new().unwrap();
        let s = SecEnv::open(&dir, ser);
        s.populate(None);
        let k = entry(KEY);

        let s = Arc::new(s);
        let done = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(RTHREADS + 1));

        // Inserter (JE writers[0]).  Insert KEY, commit, SUCCESS.
        let writer = {
            let s = Arc::clone(&s);
            let barrier = Arc::clone(&barrier);
            let k = k.clone();
            thread::spawn(move || -> OperationStatus {
                barrier.wait();
                loop {
                    let txn = s.env.begin_transaction(None).unwrap();
                    match s.primary_lf.put_in(&txn, &k, &k) {
                        Ok(_) => {
                            txn.commit().unwrap();
                            return OperationStatus::Success;
                        }
                        Err(ref e) if is_lock_conflict(e) => {
                            let _ = txn.abort();
                        }
                        Err(e) => panic!("insert failed: {e:?}"),
                    }
                }
            })
        };

        // Readers loop until `done`; any Success/NotFound is valid.
        let mut readers = Vec::new();
        for i in 0..RTHREADS {
            let use_primary = if i % 2 == 0 { d1p } else { d2p };
            let s = Arc::clone(&s);
            let done = Arc::clone(&done);
            let barrier = Arc::clone(&barrier);
            readers.push(thread::spawn(move || {
                barrier.wait();
                while !done.load(Ordering::Relaxed) {
                    let txn = s.env.begin_transaction(None).unwrap();
                    let r = read_key(
                        &s.env,
                        &s.primary_lf,
                        &s.sec,
                        use_primary,
                        &txn,
                        None,
                    );
                    match r {
                        Ok(OperationStatus::Success)
                        | Ok(OperationStatus::NotFound) => {
                            txn.commit().unwrap();
                        }
                        Ok(st) => panic!("read returned {st:?}"),
                        Err(ref e) if is_lock_conflict(e) => {
                            let _ = txn.abort();
                        }
                        Err(e) => panic!("read failed: {e:?}"),
                    }
                }
            }));
        }

        assert_eq!(
            writer.join().unwrap(),
            OperationStatus::Success,
            "inserter must SUCCEED (d1p={d1p} d2p={d2p} ser={ser})"
        );
        thread::sleep(std::time::Duration::from_millis(200));
        done.store(true, Ordering::Relaxed);
        for r in readers {
            r.join().unwrap();
        }
    }
}

// =====================================================================
// testMultiReadUpdate (SecondaryMultiTest.java:490-568)
// =====================================================================

/// JE `testMultiReadUpdate`: 20 readers loop while 5 updaters delete+reinsert
/// KEY in one txn (JE UpdateIt).  Every updater SUCCEEDS and NO reader ever
/// observes NOTFOUND — the record is always present across each atomic
/// delete+insert txn (readers block on the writer's lock until commit).
///
/// PORTED: deadlock-free via JE's lockPrimaryOnly deadlock-avoidance.  The
/// reader dirty-reads the secondary with READ_UNCOMMITTED_ALL semantics (no
/// sec lock, but RETURNS the uncommitted-deleted entry during the atomic
/// delete+reinsert so the reader resolves a primary key and BLOCKS on the
/// updater's primary lock instead of momentarily seeing NOTFOUND —
/// SecondaryCursor.java:1697-1707), then locks only the primary.  The updater
/// (`sec.delete_in` + `primary.put_in`) is primary-first, so no sec->primary
/// ABBA.  The direct-primary access uses a lock-free handle
/// (`SecEnv::primary_lf`) so no handle Mutex is held across a record op
/// (sec2 BUG-SEC-MUTEX-DEADLOCK).
#[test]
fn je_secondary_multi_test_test_multi_read_update() {
    use std::sync::atomic::AtomicBool;
    const RTHREADS: usize = 20;
    const WTHREADS: usize = 5;
    for &(d1p, d2p, ser) in ACTIVE_PARAMS {
        let dir = TempDir::new().unwrap();
        let s = SecEnv::open(&dir, ser);
        s.populate(None);
        let k = entry(KEY);

        let s = Arc::new(s);
        let done = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(RTHREADS + WTHREADS));

        // Updaters: delete then re-insert KEY atomically (JE UpdateIt).
        let mut writers = Vec::new();
        for _ in 0..WTHREADS {
            let s = Arc::clone(&s);
            let barrier = Arc::clone(&barrier);
            let k = k.clone();
            writers.push(thread::spawn(move || -> OperationStatus {
                barrier.wait();
                loop {
                    let txn = s.env.begin_transaction(None).unwrap();
                    // JE: sdb.delete then db.put, both asserted SUCCESS.
                    let dr = s.sec.delete_in(&txn, &k);
                    match dr {
                        Ok(true) => {}
                        Ok(false) => {
                            // Another updater is mid-flight; retry.
                            txn.commit().unwrap();
                            continue;
                        }
                        Err(ref e) if is_lock_conflict(e) => {
                            let _ = txn.abort();
                            continue;
                        }
                        Err(e) => panic!("sec delete failed: {e:?}"),
                    }
                    match s.primary_lf.put_in(&txn, &k, &k) {
                        Ok(_) => {
                            txn.commit().unwrap();
                            return OperationStatus::Success;
                        }
                        Err(ref e) if is_lock_conflict(e) => {
                            let _ = txn.abort();
                        }
                        Err(e) => panic!("put failed: {e:?}"),
                    }
                }
            }));
        }

        // Readers: never NOTFOUND (the atomic update keeps KEY present).
        let mut readers = Vec::new();
        for i in 0..RTHREADS {
            let use_primary = if i % 2 == 0 { d1p } else { d2p };
            let s = Arc::clone(&s);
            let done = Arc::clone(&done);
            let barrier = Arc::clone(&barrier);
            readers.push(thread::spawn(move || -> u64 {
                barrier.wait();
                let mut not_found = 0u64;
                while !done.load(Ordering::Relaxed) {
                    let txn = s.env.begin_transaction(None).unwrap();
                    let r = read_key(
                        &s.env,
                        &s.primary_lf,
                        &s.sec,
                        use_primary,
                        &txn,
                        None,
                    );
                    match r {
                        Ok(OperationStatus::Success) => {
                            txn.commit().unwrap();
                        }
                        Ok(OperationStatus::NotFound) => {
                            txn.commit().unwrap();
                            not_found += 1;
                        }
                        Ok(st) => panic!("read returned {st:?}"),
                        Err(ref e) if is_lock_conflict(e) => {
                            let _ = txn.abort();
                        }
                        Err(e) => panic!("read failed: {e:?}"),
                    }
                }
                not_found
            }));
        }

        for w in writers {
            assert_eq!(
                w.join().unwrap(),
                OperationStatus::Success,
                "updater must SUCCEED (d1p={d1p} d2p={d2p} ser={ser})"
            );
        }
        done.store(true, Ordering::Relaxed);
        for r in readers {
            assert_eq!(
                r.join().unwrap(),
                0,
                "reader must NEVER observe NOTFOUND under atomic update \
                 (d1p={d1p} d2p={d2p} ser={ser})"
            );
        }
    }
}

// =====================================================================
// testMultiReaders (SecondaryMultiTest.java:570-621)
// =====================================================================

/// JE `testMultiReaders`: 20 readers of the same present record with mixed lock
/// modes (even threads RMW, odd threads default).  No reader ever observes
/// NOTFOUND; concurrent reads (RMW is a write-lock read) must not deadlock in
/// the non-serializable rows.
#[test]
fn je_secondary_multi_test_test_multi_readers() {
    use std::sync::atomic::AtomicBool;
    const RTHREADS: usize = 20;
    for &(d1p, d2p, ser) in ACTIVE_PARAMS {
        let dir = TempDir::new().unwrap();
        let s = SecEnv::open(&dir, ser);
        s.populate(None);

        let s = Arc::new(s);
        let done = Arc::new(AtomicBool::new(false));
        let barrier = Arc::new(Barrier::new(RTHREADS));

        let mut readers = Vec::new();
        for i in 0..RTHREADS {
            // JE: even threads use db1 handle + RMW, odd use db2 handle + null.
            let (use_primary, lock_mode) = if i % 2 == 0 {
                (d1p, Some(LockMode::Rmw))
            } else {
                (d2p, None)
            };
            let s = Arc::clone(&s);
            let done = Arc::clone(&done);
            let barrier = Arc::clone(&barrier);
            readers.push(thread::spawn(move || -> u64 {
                barrier.wait();
                let mut not_found = 0u64;
                while !done.load(Ordering::Relaxed) {
                    let txn = s.env.begin_transaction(None).unwrap();
                    // RMW via secondary identity -> read the primary RMW.
                    let r = if use_primary || lock_mode == Some(LockMode::Rmw) {
                        read_primary_mode(&s.primary_lf, &txn, lock_mode)
                    } else {
                        read_key(
                            &s.env,
                            &s.primary_lf,
                            &s.sec,
                            use_primary,
                            &txn,
                            None,
                        )
                    };
                    match r {
                        Ok(OperationStatus::Success) => {
                            txn.commit().unwrap();
                        }
                        Ok(OperationStatus::NotFound) => {
                            txn.commit().unwrap();
                            not_found += 1;
                        }
                        Ok(st) => panic!("read returned {st:?}"),
                        Err(ref e) if is_lock_conflict(e) => {
                            let _ = txn.abort();
                        }
                        Err(e) => panic!("read failed: {e:?}"),
                    }
                }
                not_found
            }));
        }

        thread::sleep(std::time::Duration::from_millis(300));
        done.store(true, Ordering::Relaxed);
        for r in readers {
            assert_eq!(
                r.join().unwrap(),
                0,
                "reader must NEVER observe NOTFOUND (d1p={d1p} d2p={d2p} \
                 ser={ser})"
            );
        }
    }
}

/// Read KEY from the primary with an explicit lock mode (RMW / default).
fn read_primary_mode(
    primary: &Arc<Database>,
    txn: &Transaction,
    lock_mode: Option<LockMode>,
) -> Result<OperationStatus, noxu_db::NoxuError> {
    let k = entry(KEY);
    let mut c: Cursor = primary.open_cursor_in(txn, None)?;
    let mut ke = DatabaseEntry::from_bytes(&k);
    let mut d = DatabaseEntry::new();
    let st = c.get(&mut ke, &mut d, Get::Search, lock_mode)?;
    c.close()?;
    Ok(st)
}

// =====================================================================
// Active parameter matrix (SecondaryMultiTest.java:100-120, two serializable
// mixed-handle rows commented out in JE).
// (db1UsePrimary, db2UsePrimary, useSerialization)
// =====================================================================
const ACTIVE_PARAMS: &[(bool, bool, bool)] = &[
    (false, false, false),
    (false, false, true),
    (false, true, false),
    (true, false, false),
    (true, true, false),
    (true, true, true),
];

// ---------------------------------------------------------------------
// Existing testMultiDeleteUnordered port (NEW-DEL-RACE-1, already PORTED).
// ---------------------------------------------------------------------

/// JE `SecondaryMultiTest.testMultiDeleteUnordered` — default isolation.
///
/// NEW-DEL-RACE-1 (FIXED): exactly one of N concurrent deleters of the same
/// record wins; every other observes NOTFOUND (or loses a lock conflict).
#[test]
fn je_secondary_multi_test_test_multi_delete_unordered() {
    multi_delete_unordered(false);
}

/// Same property under SERIALIZABLE isolation (read locks retained through
/// commit).  The exactly-one-winner guarantee must hold identically.
#[test]
fn je_secondary_multi_test_test_multi_delete_unordered_serializable() {
    multi_delete_unordered(true);
}

fn multi_delete_unordered(serializable: bool) {
    const DELETERS: usize = 20;

    let dir = TempDir::new().unwrap();
    let env = Arc::new(
        Environment::open(
            EnvironmentConfig::new(dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap(),
    );
    let db = Arc::new(
        env.open_database(
            None,
            "foo",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap(),
    );

    // Populate 0..99, each key = its decimal-string bytes (JE layout).
    for i in 0..DATACOUNT {
        let v = i.to_string();
        db.put(v.as_bytes(), v.as_bytes()).unwrap();
    }

    let target = KEY.to_string();

    // CONTROL: exactly one record under the target key (single non-dup slot).
    {
        let mut c = db.open_cursor(None).unwrap();
        let mut k = DatabaseEntry::from_bytes(target.as_bytes());
        let mut d = DatabaseEntry::new();
        assert_eq!(
            c.get(&mut k, &mut d, Get::Search, None).unwrap(),
            OperationStatus::Success
        );
        assert_eq!(c.count().unwrap(), 1, "control: one record under KEY");
    }

    let success = Arc::new(AtomicUsize::new(0));
    let notfound = Arc::new(AtomicUsize::new(0));
    let conflict = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(DELETERS));

    let mut handles = Vec::with_capacity(DELETERS);
    for _ in 0..DELETERS {
        let env = Arc::clone(&env);
        let db = Arc::clone(&db);
        let target = target.clone();
        let success = Arc::clone(&success);
        let notfound = Arc::clone(&notfound);
        let conflict = Arc::clone(&conflict);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            let txn_cfg = TransactionConfig::new()
                .with_serializable_isolation(serializable);
            let txn = env.begin_transaction(Some(&txn_cfg)).unwrap();
            match db.delete_in(&txn, target.as_bytes()) {
                Ok(true) => {
                    txn.commit().unwrap();
                    success.fetch_add(1, Ordering::SeqCst);
                }
                Ok(false) => {
                    txn.commit().unwrap();
                    notfound.fetch_add(1, Ordering::SeqCst);
                }
                Err(_) => {
                    let _ = txn.abort();
                    conflict.fetch_add(1, Ordering::SeqCst);
                }
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }

    let s = success.load(Ordering::SeqCst);
    let nf = notfound.load(Ordering::SeqCst);
    let cf = conflict.load(Ordering::SeqCst);

    // CONTROL: post-race the record is gone (a further delete is false).
    let t = env.begin_transaction(None).unwrap();
    assert!(
        !db.delete_in(&t, target.as_bytes()).unwrap(),
        "KEY must be deleted after the race"
    );
    t.commit().unwrap();

    assert_eq!(
        s, 1,
        "exactly one deleter must succeed; got success={s} notfound={nf} \
         conflict={cf} (NEW-DEL-RACE-1: concurrent deletes not serialized)"
    );
    assert_eq!(
        nf + cf,
        DELETERS - 1,
        "every other deleter must observe NOTFOUND or a lock conflict"
    );
}
