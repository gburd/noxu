//! JE `com.sleepycat.je.dbi` BIN-delta / embedded-LN *behavioural* ports.
//!
//! JE's BINDeltaOpsTest / BINDeltaOperationTest / EmbeddedOpsTest drive the
//! delta machinery through JE-INTERNAL entry points (`DbInternal.getCursorImpl`
//! → `bin.mutateToBINDelta()`, `bin.isBINDelta()`, `bin.getNumEmbeddedLNs()`,
//! `getNCachedBINDeltas`, `BINDeltaBloomFilter`).  Noxu's delta format differs
//! by design (no bloom filter; different on-disk layout; see AGENTS.md), so the
//! byte-layout / internal-count assertions are recorded N/A in tp-je-dbi.md.
//!
//! What IS portable is the OBSERVABLE behaviour the deltas must preserve:
//!   - `db.count()` is correct with on-disk deltas, including after reopen
//!     (BINDeltaOperationTest.testDbCount);
//!   - a delta written in normal mode can be fetched in deferred-write mode
//!     without an assertion failure (BINDeltaOperationTest.testTransitionTo-
//!     DeferredWrite, JE [#25999]);
//!   - `getPrev` works moving backwards across BIN-deltas
//!     (BINDeltaOpsTest.testPrevBin);
//!   - small (embedded-LN-eligible) records survive delta + abort + crash
//!     recovery (EmbeddedOpsTest.testNoDups, observable half).
//!
//! Delta creation is forced the same way as `bin_split_base_regression_test.rs`:
//! daemons OFF, insert to span multiple BINs, full-image checkpoint, sparse
//! UPDATE-ONLY change, delta checkpoint; `delta_in_flush` reports activation.

use noxu_db::{
    CheckpointConfig, DatabaseConfig, DatabaseEntry, Environment,
    EnvironmentConfig, Get, OperationStatus,
};
use std::path::Path;
use tempfile::TempDir;

fn open_env(dir: &Path) -> Environment {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    cfg.set_run_checkpointer(false);
    cfg.set_run_cleaner(false);
    cfg.set_run_in_compressor(false);
    cfg.set_run_evictor(false);
    Environment::open(cfg).unwrap()
}

fn open_db(
    env: &Environment,
    name: &str,
    deferred_write: bool,
) -> noxu_db::Database {
    env.open_database(
        None,
        name,
        &DatabaseConfig::new()
            .with_allow_create(true)
            // JE opens deferred-write DBs non-transactional.
            .with_transactional(!deferred_write)
            .with_deferred_write(deferred_write),
    )
    .unwrap()
}

fn checkpoint(env: &Environment) {
    env.checkpoint(Some(&CheckpointConfig::new().with_force(true))).unwrap();
}

fn delta_in_flush(env: &Environment) -> u64 {
    env.stats().unwrap().checkpoint.delta_in_flush
}

fn key(i: u32) -> Vec<u8> {
    format!("k_{i:07}").into_bytes()
}

// ===========================================================================
// BINDeltaOperationTest.testDbCount
// JE: write 5000 records, assert db.count() == 5000; reopen (recovery rebuilds
// the tree, BIN-deltas are read from the log), assert db.count() == 5000 again;
// a full cursor walk sees every key.  The point is that count() is correct with
// BIN-deltas both cached and on-disk-after-reopen.
// ===========================================================================
#[test]
fn bin_delta_operation_test_db_count() {
    const N: u32 = 5000;
    let dir = TempDir::new().unwrap();

    // Phase 1: write, checkpoint (durable full bases), sparse update, delta
    // checkpoint.  Then assert count and reopen.
    {
        let env = open_env(dir.path());
        let db = open_db(&env, "testDB", false);

        let txn = env.begin_transaction(None).unwrap();
        for i in 0..N {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(&key(i)),
                DatabaseEntry::from_bytes(&key(i)),
            )
            .unwrap();
        }
        txn.commit().unwrap();

        checkpoint(&env); // full bases

        // Sparse UPDATE-ONLY so a later checkpoint logs deltas over the bases.
        let txn = env.begin_transaction(None).unwrap();
        for i in (0..N).step_by(50) {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(&key(i)),
                DatabaseEntry::from_bytes(b"updated"),
            )
            .unwrap();
        }
        txn.commit().unwrap();
        checkpoint(&env); // deltas

        // count() must be exactly N with cached deltas.
        assert_eq!(N as u64, db.count().unwrap(), "count with cached deltas");

        // A full walk must see exactly N distinct keys.
        let mut c = db.open_cursor(None).unwrap();
        let mut k = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        let mut seen = 0u32;
        let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
        while s == OperationStatus::Success {
            seen += 1;
            s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
        }
        assert_eq!(N, seen, "cursor walk with cached deltas");
        drop(c);
        db.close().unwrap();
        env.close().unwrap();
    }

    // Phase 2: reopen (recovery reads deltas from the log); count still N.
    {
        let env = open_env(dir.path());
        let db = open_db(&env, "testDB", false);
        assert_eq!(
            N as u64,
            db.count().unwrap(),
            "count() must be N after reopen (deltas read from log)"
        );
        // Walk after reopen sees exactly N keys, all present.
        let mut c = db.open_cursor(None).unwrap();
        let mut k = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        let mut seen = 0u32;
        let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
        while s == OperationStatus::Success {
            seen += 1;
            s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
        }
        assert_eq!(N, seen, "cursor walk after reopen");
        drop(c);
        db.close().unwrap();
        env.close().unwrap();
    }
}

// ===========================================================================
// BINDeltaOperationTest.testTransitionToDeferredWrite  (JE [#25999])
// JE: write BIN-deltas in normal (transactional) mode, close.  Reopen the DB in
// DEFERRED-WRITE mode and search for an existing key: prior to the fix a cursor
// search that fetched a BIN-delta hit an assertion ("BIN-deltas aren't allowed
// with deferred-write").  Oracle: the search succeeds and every key is
// readable in deferred-write mode.
// ===========================================================================
#[test]
fn bin_delta_operation_test_transition_to_deferred_write() {
    const N: u32 = 800; // spans multiple BINs at fanout 256
    let dir = TempDir::new().unwrap();

    // Phase 1: normal mode; create on-disk BIN-deltas; close.
    {
        let env = open_env(dir.path());
        let db = open_db(&env, "testDB", false);
        let txn = env.begin_transaction(None).unwrap();
        for i in 0..N {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(&key(i)),
                DatabaseEntry::from_bytes(&key(i)),
            )
            .unwrap();
        }
        txn.commit().unwrap();
        checkpoint(&env);
        let txn = env.begin_transaction(None).unwrap();
        for i in (0..N).step_by(50) {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(&key(i)),
                DatabaseEntry::from_bytes(b"updated"),
            )
            .unwrap();
        }
        txn.commit().unwrap();
        checkpoint(&env);
        db.close().unwrap();
        env.close().unwrap();
    }

    // Phase 2: reopen in DEFERRED-WRITE mode; search must succeed for keys that
    // live behind an on-disk delta.
    {
        let env = open_env(dir.path());
        let db = open_db(&env, "testDB", true /*deferred_write*/);
        let mut c = db.open_cursor(None).unwrap();
        // Search several existing keys (some sparse-updated, some not).
        for i in [0u32, 25, 50, 100, 400, N - 1] {
            let mut k = DatabaseEntry::from_bytes(&key(i));
            let mut d = DatabaseEntry::new();
            let s = c.get(&mut k, &mut d, Get::Search, None).unwrap();
            assert_eq!(
                OperationStatus::Success,
                s,
                "deferred-write search for key {i} must succeed (JE [#25999])"
            );
        }
        drop(c);
        // Full walk sees exactly N keys.
        let mut c = db.open_cursor(None).unwrap();
        let mut k = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        let mut seen = 0u32;
        let mut s = c.get(&mut k, &mut d, Get::First, None).unwrap();
        while s == OperationStatus::Success {
            seen += 1;
            s = c.get(&mut k, &mut d, Get::Next, None).unwrap();
        }
        assert_eq!(N, seen, "deferred-write full walk");
        drop(c);
        db.close().unwrap();
        env.close().unwrap();
    }
}

// ===========================================================================
// BINDeltaOpsTest.testPrevBin
// JE: insert nRecs records; walk forward pinning each BIN; as the cursor
// crosses into a new BIN, mutate the PREVIOUS BIN to a delta.  Then walk
// BACKWARD from the last record: every record must come back, in order,
// crossing the delta'd BIN boundaries (getPrev across BIN-deltas).
// Oracle (public API): create real on-disk deltas by checkpoint + sparse
// update + evict, then a full BACKWARD cursor walk must return every key in
// descending order exactly once.
// ===========================================================================
#[test]
fn bin_delta_ops_test_prev_bin() {
    const N: u32 = 1000; // many BINs at fanout 256
    let dir = TempDir::new().unwrap();
    let env = open_env(dir.path());
    let db = open_db(&env, "prevbin", false);

    let txn = env.begin_transaction(None).unwrap();
    for i in 0..N {
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&key(i)),
            DatabaseEntry::from_bytes(&key(i)),
        )
        .unwrap();
    }
    txn.commit().unwrap();
    checkpoint(&env); // full bases for every BIN

    // Sparse UPDATE-ONLY change spread across BINs, then checkpoint → deltas.
    let txn = env.begin_transaction(None).unwrap();
    for i in (0..N).step_by(30) {
        db.put_in(
            &txn,
            DatabaseEntry::from_bytes(&key(i)),
            DatabaseEntry::from_bytes(b"updated"),
        )
        .unwrap();
    }
    txn.commit().unwrap();
    checkpoint(&env);
    let deltas = delta_in_flush(&env);
    // Push resident BINs toward the budget so the backward walk re-faults
    // (some as deltas) from the log.
    let _ = env.evict_memory().unwrap();

    // BACKWARD walk: every key, descending, exactly once.
    let mut c = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    let mut expect = N; // expect keys N-1 .. 0
    let mut s = c.get(&mut k, &mut d, Get::Last, None).unwrap();
    while s == OperationStatus::Success {
        expect -= 1;
        assert_eq!(
            key(expect),
            k.data_opt().unwrap_or(&[]).to_vec(),
            "getPrev across BIN-deltas must return keys in descending order"
        );
        s = c.get(&mut k, &mut d, Get::Prev, None).unwrap();
    }
    assert_eq!(0, expect, "backward walk must reach the first key");
    drop(c);

    // Evidence the delta path was actually exercised (not a vacuous pass).
    assert!(
        deltas > 0,
        "expected the checkpoint to log at least one BIN-delta (delta_in_flush)"
    );
    db.close().unwrap();
    env.close().unwrap();
}

// ===========================================================================
// EmbeddedOpsTest.testNoDups  (OBSERVABLE half)
// JE drives embedded-LN internals (getNumEmbeddedLNs, key-rep mode) which are
// recorded N/A in tp-je-dbi.md.  The portable, observable behaviour: small
// (embedded-eligible) records survive delta creation, an ABORT restores the
// pre-txn state, and a crash+recover leaves the committed set intact.
// TREE_MAX_EMBEDDED_LN keeps the small values embedded in the BIN slot.
// ===========================================================================
#[test]
fn embedded_ops_test_no_dups_observable_survives_delta_abort_recover() {
    const N: u32 = 400; // spans BINs; small values are embedded-eligible
    let dir = TempDir::new().unwrap();

    // Committed baseline: N small records, checkpoint (bases), sparse update,
    // checkpoint (deltas).
    let commit_baseline = |env: &Environment, db: &noxu_db::Database| {
        let txn = env.begin_transaction(None).unwrap();
        for i in 0..N {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(&key(i)),
                // small value -> embedded-LN eligible
                DatabaseEntry::from_bytes(&i.to_be_bytes()),
            )
            .unwrap();
        }
        txn.commit().unwrap();
        checkpoint(env);
        let txn = env.begin_transaction(None).unwrap();
        for i in (0..N).step_by(30) {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(&key(i)),
                DatabaseEntry::from_bytes(&(i + 1).to_be_bytes()),
            )
            .unwrap();
        }
        txn.commit().unwrap();
        checkpoint(env);
    };

    // The committed value for key i after the baseline.
    let expect_val = |i: u32| -> Vec<u8> {
        if i.is_multiple_of(30) {
            (i + 1).to_be_bytes().to_vec()
        } else {
            i.to_be_bytes().to_vec()
        }
    };

    // Phase 1: baseline + an ABORTED mutation; post-abort must equal baseline.
    {
        let env = open_env(dir.path());
        let db = open_db(&env, "embedded", false);
        commit_baseline(&env, &db);

        // Aborted txn: overwrite a swath of keys + insert new ones, then abort.
        let txn = env.begin_transaction(None).unwrap();
        for i in 0..N {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(&key(i)),
                DatabaseEntry::from_bytes(b"ABORTED"),
            )
            .unwrap();
        }
        for i in N..(N + 50) {
            db.put_in(
                &txn,
                DatabaseEntry::from_bytes(&key(i)),
                DatabaseEntry::from_bytes(b"ABORTEDNEW"),
            )
            .unwrap();
        }
        txn.abort().unwrap();

        // Post-abort: exactly the baseline set/values.
        assert_eq!(N as u64, db.count().unwrap(), "abort must restore count");
        for i in 0..N {
            let mut out = DatabaseEntry::new();
            assert!(
                db.get_into(None, DatabaseEntry::from_bytes(&key(i)), &mut out)
                    .unwrap()
            );
            assert_eq!(
                expect_val(i),
                out.data_opt().unwrap(),
                "abort must restore key {i} to its committed value"
            );
        }
        for i in N..(N + 50) {
            let mut out = DatabaseEntry::new();
            assert!(
                !db.get_into(
                    None,
                    DatabaseEntry::from_bytes(&key(i)),
                    &mut out
                )
                .unwrap(),
                "aborted insert {i} must not survive"
            );
        }
        checkpoint(&env);
        db.close().unwrap();
        env.close().unwrap();
    }

    // Phase 2: crash-recover (clean close already done; reopen runs recovery).
    {
        let env = open_env(dir.path());
        let db = open_db(&env, "embedded", false);
        assert_eq!(N as u64, db.count().unwrap(), "recovered count");
        for i in 0..N {
            let mut out = DatabaseEntry::new();
            assert!(
                db.get_into(None, DatabaseEntry::from_bytes(&key(i)), &mut out)
                    .unwrap()
            );
            assert_eq!(
                expect_val(i),
                out.data_opt().unwrap(),
                "recovered key {i} must hold its committed value"
            );
        }
        db.close().unwrap();
        env.close().unwrap();
    }
}
