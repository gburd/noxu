//! JE `PhantomTest` port — serializable phantom prevention via next-key /
//! range locking.
//!
//! JE: `test/com/sleepycat/je/test/PhantomTest.java` (3192 lines, 51 `@Test`
//! methods, `@RunWith(Parameterized)` over four `TransactionConfig`s:
//! ReadUncommitted, ReadCommitted, RepeatableRead, Serializable).
//!
//! JE's property: a SERIALIZABLE reader that positions a cursor with some read
//! operation acquires next-key / range read locks so that a concurrent
//! transaction inserting a key INTO the locked range (a phantom) is blocked;
//! a non-serializable reader (RU/RC/RR) allows the phantom.  JE races a writer
//! thread (`startInsert`/`waitForInsert`) and detects the block via
//! `EnvironmentStats.getNWaiters()`.
//!
//! Deviation (authorized, documented in `isolation_test.rs`): instead of the
//! two-thread `startInsert`/`waitForInsert` harness, we use a `no_wait=true`
//! inserter transaction which turns the range-lock conflict into an immediate
//! `LockNotAvailable` — the SAME conflict JE's reader's held range lock
//! causes, made deterministic and timing-free (so each `#[test]` can be timed
//! in isolation).  Each Rust `#[test]` covers all four JE parameter sets: the
//! SERIALIZABLE branch (insert blocked) and the single non-serializable branch
//! that RU==RC==RR all share (insert allowed — `txnSerializable` is the only
//! distinguisher in every JE method).
//!
//! ## Two engine bugs this port exposes (noxu-dbi; see parity-test-suite.md)
//!
//! * BUG-PHANTOM-1 (over-locking): under serializable, `noxu-dbi`'s `lock_ln`
//!   acquires `RangeRead` on EVERY read, including the search ANCHOR.  JE
//!   range-locks only the ADVANCE step (`searchLockType=READ`,
//!   `advanceLockType=RANGE_READ`, Cursor.java:4031-4032).  So Noxu wrongly
//!   blocks "insertions before the anchor" that JE's try/catch asserts never
//!   block.  Tests whose SUCCESS asserts such a "safe before" insert are
//!   `#[ignore]`d as `PORTED-FAILING`.
//! * BUG-PHANTOM-2 (under-locking): under serializable, a range search that
//!   ends in NOTFOUND does not range-lock the next-key / EOF, so the phantom
//!   insert is NOT blocked.  JE's `searchRangeSerializable` locks the next
//!   slot even on NOTFOUND.  The `getSearchKeyRange_NotFound` /
//!   `getSearchBothRange_NotFound` tests are `#[ignore]`d as `PORTED-FAILING`.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, Get,
    LockMode, NoxuError, OperationStatus, Put, TransactionConfig,
};
use tempfile::TempDir;

fn open(dups: bool) -> (TempDir, Environment, noxu_db::Database) {
    let dir = TempDir::new().unwrap();
    let env = Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            // JE PhantomTest disables all four daemons so they do not
            // interfere with the scan<->insert range-lock window
            // (PhantomTest.java:183-190: ENV_RUN_EVICTOR / ENV_RUN_CLEANER /
            // ENV_RUN_CHECKPOINTER / ENV_RUN_INCOMPRESSOR = false). In
            // particular the IN compressor merging/removing a KD slot between
            // positioning and the phantom insert changes the "next key" and
            // makes the serializable range lock non-deterministic.
            .with_run_evictor(false)
            .with_run_cleaner(false)
            .with_run_checkpointer(false)
            .with_run_in_compressor(false),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "PhantomTest",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true)
                .with_sorted_duplicates(dups),
        )
        .unwrap();
    (dir, env, db)
}

/// JE `IntegerBinding.intToEntry` encodes an `int` by XOR-ing the sign bit
/// (`TupleOutput.writeInt`: `val ^ 0x80000000`) so signed integers sort in
/// their natural signed order as unsigned big-endian bytes (e.g. `-1 < 0`).
/// The port MUST use the same encoding, because several PhantomTest methods
/// (e.g. `testGetSearchBothRange_NotFound_Dup`) insert a NEGATIVE value and
/// assert it sorts BEFORE 0 ("insertions before {3,0} are never blocked").
/// A plain `to_be_bytes()` would place `-1` (0xFFFFFFFF) AFTER every
/// non-negative value and break that assumption.
fn ik(i: i32) -> [u8; 4] {
    ((i as u32) ^ 0x8000_0000).to_be_bytes()
}

/// Inverse of [`ik`]: decode JE's sign-XOR'd integer binding.
fn dk(b: &[u8]) -> i32 {
    (u32::from_be_bytes(b.try_into().unwrap()) ^ 0x8000_0000) as i32
}

/// JE `insert(k[,d])`: new txn, putNoOverwrite / putNoDupData, commit.
fn insert(
    env: &Environment,
    db: &noxu_db::Database,
    dups: bool,
    k: i32,
    d: i32,
) {
    let t = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&t, None).unwrap();
    let mode = if dups { Put::NoDupData } else { Put::NoOverwrite };
    let st = c
        .put(
            &DatabaseEntry::from_bytes(&ik(k)),
            &DatabaseEntry::from_bytes(&ik(d)),
            mode,
        )
        .unwrap();
    assert_eq!(st, OperationStatus::Success, "seed insert ({k},{d})");
    drop(c);
    t.commit().unwrap();
}

/// Deterministic form of JE's `startInsert`/`waitForInsert`: a `no_wait=true`
/// inserter.  Ok => committed (phantom allowed); Err => range lock blocked it.
fn try_insert(
    env: &Environment,
    db: &noxu_db::Database,
    dups: bool,
    k: i32,
    d: i32,
) -> Result<(), NoxuError> {
    let nw = TransactionConfig::new().with_no_wait(true);
    let t = env.begin_transaction(Some(&nw)).unwrap();
    let mut c = db.open_cursor_in(&t, None).unwrap();
    let mode = if dups { Put::NoDupData } else { Put::NoOverwrite };
    let r = c.put(
        &DatabaseEntry::from_bytes(&ik(k)),
        &DatabaseEntry::from_bytes(&ik(d)),
        mode,
    );
    drop(c);
    match r {
        Ok(_) => {
            t.commit().unwrap();
            Ok(())
        }
        Err(e) => {
            let _ = t.abort();
            Err(e)
        }
    }
}

/// A cursor positioning step (the JE op that locks the range).
#[derive(Clone, Copy)]
enum Step {
    Search(i32),
    SearchBoth(i32, i32),
    SearchGte(i32),
    SearchBothRange(i32, i32),
    First,
    Last,
    Next,
    Prev,
    NextDup,
    PrevDup,
    NextNoDup,
    PrevNoDup,
}

fn run_step(
    c: &mut noxu_db::Cursor,
    step: Step,
) -> (OperationStatus, Option<(i32, i32)>) {
    let (mut key, mut data, get) = match step {
        Step::Search(k) => (
            DatabaseEntry::from_bytes(&ik(k)),
            DatabaseEntry::new(),
            Get::Search,
        ),
        Step::SearchBoth(k, d) => (
            DatabaseEntry::from_bytes(&ik(k)),
            DatabaseEntry::from_bytes(&ik(d)),
            Get::SearchBoth,
        ),
        Step::SearchGte(k) => (
            DatabaseEntry::from_bytes(&ik(k)),
            DatabaseEntry::new(),
            Get::SearchGte,
        ),
        Step::SearchBothRange(k, d) => (
            DatabaseEntry::from_bytes(&ik(k)),
            DatabaseEntry::from_bytes(&ik(d)),
            Get::SearchBothRange,
        ),
        Step::First => (DatabaseEntry::new(), DatabaseEntry::new(), Get::First),
        Step::Last => (DatabaseEntry::new(), DatabaseEntry::new(), Get::Last),
        Step::Next => (DatabaseEntry::new(), DatabaseEntry::new(), Get::Next),
        Step::Prev => (DatabaseEntry::new(), DatabaseEntry::new(), Get::Prev),
        Step::NextDup => {
            (DatabaseEntry::new(), DatabaseEntry::new(), Get::NextDup)
        }
        Step::PrevDup => {
            (DatabaseEntry::new(), DatabaseEntry::new(), Get::PrevDup)
        }
        Step::NextNoDup => {
            (DatabaseEntry::new(), DatabaseEntry::new(), Get::NextNoDup)
        }
        Step::PrevNoDup => {
            (DatabaseEntry::new(), DatabaseEntry::new(), Get::PrevNoDup)
        }
    };
    let st = c.get(&mut key, &mut data, get, Some(LockMode::Default)).unwrap();
    let kd = if st == OperationStatus::Success {
        Some((dk(key.data()), dk(data.data())))
    } else {
        None
    };
    (st, kd)
}

/// Faithful port of one PhantomTest method's serializable + non-serializable
/// branches, using the deterministic no_wait inserter.
///
/// * `dups`     — DB sorted-duplicates flag.
/// * `seeds`    — committed (k,d) inserts before the reader starts.
/// * `pos`      — positioning op sequence; last step's result is asserted.
/// * `expect`   — `Some((k,d))` SUCCESS or `None` NOTFOUND (JE pre-insert).
/// * `safe`     — (k,d) inserts JE asserts "never blocked" (outside the range).
/// * `phantom`  — the (k,d) insert into the locked range.
/// * `after`    — the (k,d) a non-serializable re-read sees after the phantom.
#[allow(clippy::too_many_arguments)]
fn phantom_case(
    dups: bool,
    seeds: &[(i32, i32)],
    pos: &[Step],
    expect: Option<(i32, i32)>,
    safe: &[(i32, i32)],
    phantom: (i32, i32),
    after: Option<(i32, i32)>,
) {
    // ---- SERIALIZABLE: phantom insert blocked. ----
    {
        let (_d, env, db) = open(dups);
        for &(k, v) in seeds {
            insert(&env, &db, dups, k, v);
        }
        let ser = TransactionConfig::new().with_serializable_isolation(true);
        let reader = env.begin_transaction(Some(&ser)).unwrap();
        let mut c = db.open_cursor_in(&reader, None).unwrap();
        let mut last = (OperationStatus::NotFound, None);
        for &step in pos {
            last = run_step(&mut c, step);
        }
        match expect {
            Some(kd) => {
                assert_eq!(
                    last.0,
                    OperationStatus::Success,
                    "serializable position SUCCESS"
                );
                assert_eq!(last.1, Some(kd), "serializable position result");
            }
            None => assert_eq!(
                last.0,
                OperationStatus::NotFound,
                "serializable position NOTFOUND"
            ),
        }
        for &(k, v) in safe {
            assert!(
                try_insert(&env, &db, dups, k, v).is_ok(),
                "serializable: safe insert ({k},{v}) must never block"
            );
        }
        let r = try_insert(&env, &db, dups, phantom.0, phantom.1);
        assert!(
            matches!(r, Err(NoxuError::LockNotAvailable)),
            "serializable: phantom insert {phantom:?} MUST block (range lock); got {r:?}"
        );
        drop(c);
        reader.commit().unwrap();
        assert!(
            try_insert(&env, &db, dups, phantom.0, phantom.1).is_ok(),
            "after serializable reader commits, phantom {phantom:?} must succeed"
        );
    }

    // ---- NON-SERIALIZABLE (RU==RC==RR): phantom insert allowed. ----
    {
        let (_d, env, db) = open(dups);
        for &(k, v) in seeds {
            insert(&env, &db, dups, k, v);
        }
        let reader = env.begin_transaction(None).unwrap();
        let mut c = db.open_cursor_in(&reader, None).unwrap();
        let mut last = (OperationStatus::NotFound, None);
        for &step in pos {
            last = run_step(&mut c, step);
        }
        match expect {
            Some(kd) => assert_eq!(last.1, Some(kd), "non-ser position result"),
            None => assert_eq!(
                last.0,
                OperationStatus::NotFound,
                "non-ser position NOTFOUND"
            ),
        }
        assert!(
            try_insert(&env, &db, dups, phantom.0, phantom.1).is_ok(),
            "non-serializable: phantom insert {phantom:?} must be allowed"
        );
        if let Some(exp) = after {
            let mut c2 = db.open_cursor_in(&reader, None).unwrap();
            let mut re = (OperationStatus::NotFound, None);
            for &step in pos {
                re = run_step(&mut c2, step);
            }
            assert_eq!(
                re.1,
                Some(exp),
                "non-ser re-read must see the phantom {exp:?}"
            );
            drop(c2);
        }
        drop(c);
        reader.commit().unwrap();
    }
}

// ===========================================================================
// getSearchKey
// ===========================================================================

/// JE: PhantomTest.testGetSearchKey_Success (PhantomTest.java:223).
/// Insert 2; searchKey(2)=SUCCESS; inserts of 1 and 3 are never blocked.
/// PORTED-FAILING (BUG-PHANTOM-1): Noxu range-locks the anchor key 2 under
/// serializable, so insert of 1 (before the anchor) is wrongly blocked.
#[test]
fn je_phantom_test_test_get_search_key_success() {
    let (_d, env, db) = open(false);
    insert(&env, &db, false, 2, 0);
    let ser = TransactionConfig::new().with_serializable_isolation(true);
    let reader = env.begin_transaction(Some(&ser)).unwrap();
    let mut c = db.open_cursor_in(&reader, None).unwrap();
    let (st, kd) = run_step(&mut c, Step::Search(2));
    assert_eq!(st, OperationStatus::Success);
    assert_eq!(kd, Some((2, 0)));
    assert!(
        try_insert(&env, &db, false, 1, 0).is_ok(),
        "insert 1 never blocked"
    );
    assert!(
        try_insert(&env, &db, false, 3, 0).is_ok(),
        "insert 3 never blocked"
    );
    drop(c);
    reader.commit().unwrap();
}

/// JE: PhantomTest.testGetSearchKey_Success_Dup (PhantomTest.java:250).
/// dups {1,2},{1,3}; searchKey(1)=>{1,2}; insert {1,4} never blocked; phantom
/// {1,1} blocks under serializable (inserted BEFORE {1,2} within the dup set —
/// but {1,1}<{1,2} so it's a phantom in the range the reader's dup read guards).
#[test]
fn je_phantom_test_test_get_search_key_success_dup() {
    // JE calls getSearchKey(1) (SET), which on a sorted-dup DB positions on the
    // FIRST duplicate {1,2} via a range search (RANGE_READ), so an insert of
    // {1,1} BEFORE that first dup is blocked.  (JE's `searchKey(cursor, 1, 2)`
    // helper passes keyVal=1 and merely ASSERTS the result data == 2 -- it is
    // getSearchKey(1), not getSearchBoth(1,2).)
    phantom_case(
        true,
        &[(1, 2), (1, 3)],
        &[Step::Search(1)],
        Some((1, 2)),
        &[(1, 4)],
        (1, 1),
        Some((1, 1)),
    );
}

/// JE: PhantomTest.testGetSearchKey_NotFound (PhantomTest.java:300).
/// insert 1; searchKey(2)=NOTFOUND; insert 0 never blocked; phantom 2 blocks.
#[test]
fn je_phantom_test_test_get_search_key_notfound() {
    phantom_case(
        false,
        &[(1, 0)],
        &[Step::Search(2)],
        None,
        &[(0, 0)],
        (2, 0),
        Some((2, 0)),
    );
}

/// JE: PhantomTest.testGetSearchKey_NotFound_Dup (PhantomTest.java:349).
/// dups {2,1},{2,2}; searchKey {1,1}=NOTFOUND; inserts {2,3},{3,0} never
/// blocked; phantom {1,1} blocks under serializable.
#[test]
fn je_phantom_test_test_get_search_key_notfound_dup() {
    phantom_case(
        true,
        &[(2, 1), (2, 2)],
        &[Step::Search(1)],
        None,
        &[(2, 3), (3, 0)],
        (1, 1),
        Some((1, 1)),
    );
}

// ===========================================================================
// getSearchBoth
// ===========================================================================

/// JE: PhantomTest.testGetSearchBoth_Success (PhantomTest.java:400).
/// non-dup; insert 2; searchBoth(2,0)=>{2,0}; inserts 1,3 never blocked
/// (no phantom insert in this method).
/// PORTED-FAILING (BUG-PHANTOM-1): insert 1 before anchor wrongly blocked.
#[test]
fn je_phantom_test_test_get_search_both_success() {
    let (_d, env, db) = open(false);
    insert(&env, &db, false, 2, 0);
    let ser = TransactionConfig::new().with_serializable_isolation(true);
    let reader = env.begin_transaction(Some(&ser)).unwrap();
    let mut c = db.open_cursor_in(&reader, None).unwrap();
    let (st, kd) = run_step(&mut c, Step::SearchBoth(2, 0));
    assert_eq!(st, OperationStatus::Success);
    assert_eq!(kd, Some((2, 0)));
    assert!(
        try_insert(&env, &db, false, 1, 0).is_ok(),
        "insert 1 never blocked"
    );
    assert!(
        try_insert(&env, &db, false, 3, 0).is_ok(),
        "insert 3 never blocked"
    );
    drop(c);
    reader.commit().unwrap();
}

/// JE: PhantomTest.testGetSearchBoth_Success_Dup (PhantomTest.java:438).
/// dups {1,1},{1,3}; searchBoth(1,3)=>{1,3}; inserts {0,0},{1,0},{1,2},{1,4},
/// {2,0} never blocked (exact match, no phantom insert).
/// PORTED-FAILING (BUG-PHANTOM-1): inserts before the anchor {1,3} wrongly
/// blocked under serializable.
#[test]
fn je_phantom_test_test_get_search_both_success_dup() {
    let (_d, env, db) = open(true);
    insert(&env, &db, true, 1, 1);
    insert(&env, &db, true, 1, 3);
    let ser = TransactionConfig::new().with_serializable_isolation(true);
    let reader = env.begin_transaction(Some(&ser)).unwrap();
    let mut c = db.open_cursor_in(&reader, None).unwrap();
    let (st, kd) = run_step(&mut c, Step::SearchBoth(1, 3));
    assert_eq!(st, OperationStatus::Success);
    assert_eq!(kd, Some((1, 3)));
    for &(k, d) in &[(0, 0), (1, 0), (1, 2), (1, 4), (2, 0)] {
        assert!(
            try_insert(&env, &db, true, k, d).is_ok(),
            "insert ({k},{d}) never blocked"
        );
    }
    drop(c);
    reader.commit().unwrap();
}

/// JE: PhantomTest.testGetSearchBoth_NotFound (PhantomTest.java:469 ->
/// doTestGetSearchBoth_NotFound(false)).  non-dup; insert 1; searchBoth(2,0)=
/// NOTFOUND; insert 0 never blocked; phantom 2 blocks under serializable.
#[test]
fn je_phantom_test_test_get_search_both_notfound() {
    phantom_case(
        false,
        &[(1, 0)],
        &[Step::SearchBoth(2, 0)],
        None,
        &[(0, 0)],
        (2, 0),
        Some((2, 0)),
    );
}

/// JE: PhantomTest.testGetSearchBoth_NotFound_Dup (PhantomTest.java:532).
/// dups {1,1},{1,3}; searchBoth(1,2)=NOTFOUND; inserts {1,0},{0,0} never
/// blocked; phantom {1,2} blocks under serializable.
#[test]
fn je_phantom_test_test_get_search_both_notfound_dup() {
    phantom_case(
        true,
        &[(1, 1), (1, 3)],
        &[Step::SearchBoth(1, 2)],
        None,
        &[(1, 0), (0, 0)],
        (1, 2),
        Some((1, 2)),
    );
}

// ===========================================================================
// getSearchKeyRange  (SearchGte)
// ===========================================================================

/// JE: PhantomTest.testGetSearchKeyRange_Success (PhantomTest.java:585).
/// insert 1,3; SearchGte(2)=>key 3; inserts 0,4 never blocked; phantom 2
/// blocks under serializable (into gap 1..3, successor = 3).
#[test]
fn je_phantom_test_test_get_search_key_range_success() {
    phantom_case(
        false,
        &[(1, 0), (3, 0)],
        &[Step::SearchGte(2)],
        Some((3, 0)),
        &[(0, 0), (4, 0)],
        (2, 0),
        Some((2, 0)),
    );
}

/// JE: PhantomTest.testGetSearchKeyRange_Success_Dup (PhantomTest.java:648).
/// dups {1,1},{1,2},{3,2},{3,3}; SearchGte(2)=>{3,2}; inserts {1,0},{0,0},
/// {3,4},{4,0} never blocked; phantom {3,1} blocks (data < {3,2} within key 3,
/// successor of the range).
#[test]
fn je_phantom_test_test_get_search_key_range_success_dup() {
    phantom_case(
        true,
        &[(1, 1), (1, 2), (3, 2), (3, 3)],
        &[Step::SearchGte(2)],
        Some((3, 2)),
        &[(1, 0), (0, 0), (3, 4), (4, 0)],
        (3, 1),
        Some((3, 1)),
    );
}

/// JE: PhantomTest.testGetSearchKeyRange_NotFound (PhantomTest.java:719).
/// insert 1; SearchGte(2)=NOTFOUND; insert 0 never blocked; phantom 3 blocks.
/// PORTED-FAILING (BUG-PHANTOM-2): Noxu does not range-lock the next-key/EOF
/// when the range search ends in NOTFOUND, so insert 3 is wrongly allowed.
#[test]
fn je_phantom_test_test_get_search_key_range_notfound() {
    phantom_case(
        false,
        &[(1, 0)],
        &[Step::SearchGte(2)],
        None,
        &[(0, 0)],
        (3, 0),
        Some((3, 0)),
    );
}

/// JE: PhantomTest.testGetSearchKeyRange_NotFound_Dup (PhantomTest.java:779).
/// dups {1,1},{1,2}; SearchGte(2)=NOTFOUND; inserts {1,0},{0,0} never blocked;
/// phantom {3,1} blocks.
/// PORTED-FAILING (BUG-PHANTOM-2).
#[test]
fn je_phantom_test_test_get_search_key_range_notfound_dup() {
    phantom_case(
        true,
        &[(1, 1), (1, 2)],
        &[Step::SearchGte(2)],
        None,
        &[(1, 0), (0, 0)],
        (3, 1),
        Some((3, 1)),
    );
}

// ===========================================================================
// getSearchBothRange
// ===========================================================================

/// JE: PhantomTest.testGetSearchBothRange_Success_Dup (PhantomTest.java:851).
/// dups {1,1},{1,2},{3,2},{3,3}; searchBothRange(3,0)=>{3,2}; inserts {1,0},
/// {0,0},{3,4} never blocked; phantom {3,1} blocks (data<{3,2} in key 3).
#[test]
fn je_phantom_test_test_get_search_both_range_success_dup() {
    phantom_case(
        true,
        &[(1, 1), (1, 2), (3, 2), (3, 3)],
        &[Step::SearchBothRange(3, 0)],
        Some((3, 2)),
        &[(1, 0), (0, 0), (3, 4)],
        (3, 1),
        Some((3, 1)),
    );
}

/// JE: PhantomTest.testGetSearchBothRange_NotFound (PhantomTest.java:924 ->
/// doTestGetSearchBoth_NotFound(true)).  non-dup; insert 1; searchBothRange
/// (2,0)=NOTFOUND; insert 0 never blocked; phantom 2 blocks under serializable.
/// For a non-dup DB, getSearchBothRange degrades to getSearchBoth, which here
/// is an exact search that advances to the next slot (key 2's gap) — not a
/// NOTFOUND *range* search over the EOF, so BUG-PHANTOM-2 does not apply.
#[test]
fn je_phantom_test_test_get_search_both_range_notfound() {
    phantom_case(
        false,
        &[(1, 0)],
        &[Step::SearchBothRange(2, 0)],
        None,
        &[(0, 0)],
        (2, 0),
        Some((2, 0)),
    );
}

/// JE: PhantomTest.testGetSearchBothRange_NotFound_Dup (PhantomTest.java:932).
/// dups {3,0},{3,1}; searchBothRange(3,2)=NOTFOUND; inserts {3,-1},{2,0} never
/// blocked; phantom {3,3} blocks.
/// PORTED-FAILING (BUG-PHANTOM-2): the NOTFOUND range search (data 2 beyond the
/// last dup {3,1}) does not range-lock the next-key/EOF, so {3,3} is allowed.
#[test]
fn je_phantom_test_test_get_search_both_range_notfound_dup() {
    phantom_case(
        true,
        &[(3, 0), (3, 1)],
        &[Step::SearchBothRange(3, 2)],
        None,
        &[(3, -1), (2, 0)],
        (3, 3),
        Some((3, 3)),
    );
}

// ===========================================================================
// getFirst / getLast
// ===========================================================================

/// JE: PhantomTest.testGetFirst_Success (PhantomTest.java:998).
/// insert 2; getFirst=>2; insert 3 never blocked; phantom 1 blocks (before
/// first — getFirst range-locks the first key / its predecessor gap).
#[test]
fn je_phantom_test_test_get_first_success() {
    phantom_case(
        false,
        &[(2, 0)],
        &[Step::First],
        Some((2, 0)),
        &[(3, 0)],
        (1, 0),
        Some((1, 0)),
    );
}

/// JE: PhantomTest.testGetFirst_Success_Dup (PhantomTest.java:1056).
/// dups {1,2},{1,3}; getFirst=>{1,2}; inserts {1,4},{2,0} never blocked;
/// phantom {1,1} blocks (before first dup).
#[test]
fn je_phantom_test_test_get_first_success_dup() {
    phantom_case(
        true,
        &[(1, 2), (1, 3)],
        &[Step::First],
        Some((1, 2)),
        &[(1, 4), (2, 0)],
        (1, 1),
        Some((1, 1)),
    );
}

/// JE: PhantomTest.testGetFirst_NotFound (PhantomTest.java:1121).
/// empty DB; getFirst=NOTFOUND; phantom 1 blocks under serializable
/// (getFirst on empty range-locks EOF).
#[test]
fn je_phantom_test_test_get_first_notfound() {
    phantom_case(false, &[], &[Step::First], None, &[], (1, 0), Some((1, 0)));
}

/// JE: PhantomTest.testGetFirst_NotFound_Dup (PhantomTest.java:1168).
/// empty dup DB; getFirst=NOTFOUND; phantom {1,1} blocks.
#[test]
fn je_phantom_test_test_get_first_notfound_dup() {
    phantom_case(true, &[], &[Step::First], None, &[], (1, 1), Some((1, 1)));
}

/// JE: PhantomTest.testGetLast_Success (PhantomTest.java:1216).
/// insert 1; getLast=>1; insert 0 never blocked; phantom 2 blocks (after last
/// — getLast range-locks EOF).
#[test]
fn je_phantom_test_test_get_last_success() {
    phantom_case(
        false,
        &[(1, 0)],
        &[Step::Last],
        Some((1, 0)),
        &[(0, 0)],
        (2, 0),
        Some((2, 0)),
    );
}

/// JE: PhantomTest.testGetLast_Success_Dup (PhantomTest.java:1274).
/// dups {1,0},{1,2}; getLast=>{1,2}; inserts {1,1},{0,0} never blocked;
/// phantom {1,3} blocks (after last dup).
#[test]
fn je_phantom_test_test_get_last_success_dup() {
    phantom_case(
        true,
        &[(1, 0), (1, 2)],
        &[Step::Last],
        Some((1, 2)),
        &[(1, 1), (0, 0)],
        (1, 3),
        Some((1, 3)),
    );
}

/// JE: PhantomTest.testGetLast_NotFound (PhantomTest.java:1338).
/// empty; getLast=NOTFOUND; phantom 1 blocks.
#[test]
fn je_phantom_test_test_get_last_notfound() {
    phantom_case(false, &[], &[Step::Last], None, &[], (1, 0), Some((1, 0)));
}

/// JE: PhantomTest.testGetLast_NotFound_Dup (PhantomTest.java:1385).
/// empty dup; getLast=NOTFOUND; phantom {1,1} blocks.
#[test]
fn je_phantom_test_test_get_last_notfound_dup() {
    phantom_case(true, &[], &[Step::Last], None, &[], (1, 1), Some((1, 1)));
}

// ===========================================================================
// getNext
// ===========================================================================

/// JE: PhantomTest.testGetNext_Success (PhantomTest.java:1434).
/// insert 1,3; search(1)+getNext=>3; inserts 0,4 never blocked; phantom 2
/// blocks (gap 1..3).
/// PORTED-FAILING (BUG-PHANTOM-1): the search anchor 1 is range-locked, so
/// insert 0 (before anchor) is wrongly blocked.
#[test]
fn je_phantom_test_test_get_next_success() {
    phantom_case(
        false,
        &[(1, 0), (3, 0)],
        &[Step::Search(1), Step::Next],
        Some((3, 0)),
        &[(0, 0), (4, 0)],
        (2, 0),
        Some((2, 0)),
    );
}

/// JE: PhantomTest.testGetNext_Success_Dup (PhantomTest.java:1497).
/// dups {1,1},{1,3}; searchBoth(1,1)+getNext=>{1,3}; inserts {1,0},{0,0},
/// {1,4},{2,0} never blocked; phantom {1,2} blocks.
/// PORTED-FAILING (BUG-PHANTOM-1).
#[test]
fn je_phantom_test_test_get_next_success_dup() {
    phantom_case(
        true,
        &[(1, 1), (1, 3)],
        &[Step::SearchBoth(1, 1), Step::Next],
        Some((1, 3)),
        &[(1, 0), (0, 0), (1, 4), (2, 0)],
        (1, 2),
        Some((1, 2)),
    );
}

/// JE: PhantomTest.testGetNext_NotFound (PhantomTest.java:1604).
/// insert 1; search(1)+getNext=NOTFOUND; insert 0 never blocked; phantom 2
/// blocks (gap after 1 to EOF).
/// PORTED-FAILING (BUG-PHANTOM-1).
#[test]
fn je_phantom_test_test_get_next_notfound() {
    phantom_case(
        false,
        &[(1, 0)],
        &[Step::Search(1), Step::Next],
        None,
        &[(0, 0)],
        (2, 0),
        Some((2, 0)),
    );
}

/// JE: PhantomTest.testGetNext_NotFound_Dup (PhantomTest.java:1645-ish).
/// dups {1,1},{1,2}; searchBoth(1,2)+getNext=NOTFOUND; inserts {1,0},{0,0}
/// never blocked; phantom {1,3} blocks.
/// PORTED-FAILING (BUG-PHANTOM-1).
#[test]
fn je_phantom_test_test_get_next_notfound_dup() {
    phantom_case(
        true,
        &[(1, 1), (1, 2)],
        &[Step::SearchBoth(1, 2), Step::Next],
        None,
        &[(1, 0), (0, 0)],
        (1, 3),
        Some((1, 3)),
    );
}

// ===========================================================================
// getNextDup
// ===========================================================================

/// JE: PhantomTest.testGetNextDup_Success_Dup (PhantomTest.java:1689).
/// dups {1,1},{1,3}; searchBoth(1,1)+getNextDup=>{1,3}; inserts {1,0},{0,0},
/// {1,4},{2,0} never blocked; phantom {1,2} blocks.
/// PORTED-FAILING (BUG-PHANTOM-1).
#[test]
fn je_phantom_test_test_get_next_dup_success_dup() {
    phantom_case(
        true,
        &[(1, 1), (1, 3)],
        &[Step::SearchBoth(1, 1), Step::NextDup],
        Some((1, 3)),
        &[(1, 0), (0, 0), (1, 4), (2, 0)],
        (1, 2),
        Some((1, 2)),
    );
}

/// JE: PhantomTest.testGetNextDup_NotFound_Dup (PhantomTest.java:1759).
/// dups {1,1},{1,2},{2,1},{2,2}; searchBoth(1,2)+getNextDup=NOTFOUND; inserts
/// {1,0},{0,0},{2,3},{3,0} never blocked; phantom {1,3} blocks.
/// PORTED-FAILING (BUG-PHANTOM-1).
#[test]
fn je_phantom_test_test_get_next_dup_notfound_dup() {
    phantom_case(
        true,
        &[(1, 1), (1, 2), (2, 1), (2, 2)],
        &[Step::SearchBoth(1, 2), Step::NextDup],
        None,
        &[(1, 0), (0, 0), (2, 3), (3, 0)],
        (1, 3),
        Some((1, 3)),
    );
}

// ===========================================================================
// getNextNoDup
// ===========================================================================

/// JE: PhantomTest.testGetNextNoDup_Success (PhantomTest.java:1831).
/// insert 1,3; search(1)+getNextNoDup=>3; inserts 0,4 never blocked; phantom 2
/// blocks.
/// PORTED-FAILING (BUG-PHANTOM-1).
#[test]
fn je_phantom_test_test_get_next_no_dup_success() {
    phantom_case(
        false,
        &[(1, 0), (3, 0)],
        &[Step::Search(1), Step::NextNoDup],
        Some((3, 0)),
        &[(0, 0), (4, 0)],
        (2, 0),
        Some((2, 0)),
    );
}

/// JE: PhantomTest.testGetNextNoDup_Success_Dup (PhantomTest.java:1890).
/// dups {1,1},{1,2},{3,1},{3,2}; searchBoth(1,1)+getNextNoDup=>{3,1}; inserts
/// {1,0},{0,0},{3,3},{4,0} never blocked; phantom {2,1} blocks.
/// PORTED-FAILING (BUG-PHANTOM-1).
#[test]
fn je_phantom_test_test_get_next_no_dup_success_dup() {
    phantom_case(
        true,
        &[(1, 1), (1, 2), (3, 1), (3, 2)],
        &[Step::SearchBoth(1, 1), Step::NextNoDup],
        Some((3, 1)),
        &[(1, 0), (0, 0), (3, 3), (4, 0)],
        (2, 1),
        Some((2, 1)),
    );
}

/// JE: PhantomTest.testGetNextNoDup_NotFound (PhantomTest.java:1961).
/// insert 1; search(1)+getNextNoDup=NOTFOUND; insert 0 never blocked; phantom 2
/// blocks.
/// PORTED-FAILING (BUG-PHANTOM-1).
#[test]
fn je_phantom_test_test_get_next_no_dup_notfound() {
    phantom_case(
        false,
        &[(1, 0)],
        &[Step::Search(1), Step::NextNoDup],
        None,
        &[(0, 0)],
        (2, 0),
        Some((2, 0)),
    );
}

/// JE: PhantomTest.testGetNextNoDup_NotFound_Dup (PhantomTest.java:2024).
/// dups {1,1},{1,2}; searchBoth(1,1)+getNextNoDup=NOTFOUND; inserts {1,0},{0,0}
/// never blocked; phantom {2,1} blocks.
/// PORTED-FAILING (BUG-PHANTOM-1).
#[test]
fn je_phantom_test_test_get_next_no_dup_notfound_dup() {
    phantom_case(
        true,
        &[(1, 1), (1, 2)],
        &[Step::SearchBoth(1, 1), Step::NextNoDup],
        None,
        &[(1, 0), (0, 0)],
        (2, 1),
        Some((2, 1)),
    );
}

// ===========================================================================
// getPrev  (reverse scans: anchor is the HIGH key, phantom inserted below it)
// ===========================================================================

/// JE: PhantomTest.testGetPrev_Success (PhantomTest.java:2096-ish).
/// insert 1,3; search(3)+getPrev=>1; inserts 0,4 never blocked; phantom 2
/// blocks (gap 1..3).
/// PORTED-FAILING (BUG-PHANTOM-1): the anchor 3 is range-locked, so insert 4
/// (after anchor) is wrongly blocked.
#[test]
fn je_phantom_test_test_get_prev_success() {
    phantom_case(
        false,
        &[(1, 0), (3, 0)],
        &[Step::Search(3), Step::Prev],
        Some((1, 0)),
        &[(0, 0), (4, 0)],
        (2, 0),
        Some((2, 0)),
    );
}

/// JE: PhantomTest.testGetPrev_Success_Dup.
/// dups {1,1},{1,3}; searchBoth(1,3)+getPrev=>{1,1}; inserts {1,0},{0,0},{1,4},
/// {2,0} never blocked; phantom {1,2} blocks.
/// PORTED-FAILING (BUG-PHANTOM-1).
#[test]
fn je_phantom_test_test_get_prev_success_dup() {
    phantom_case(
        true,
        &[(1, 1), (1, 3)],
        &[Step::SearchBoth(1, 3), Step::Prev],
        Some((1, 1)),
        &[(1, 0), (0, 0), (1, 4), (2, 0)],
        (1, 2),
        Some((1, 2)),
    );
}

/// JE: PhantomTest.testGetPrev_NotFound.
/// insert 3; search(3)+getPrev=NOTFOUND; insert 4 never blocked; phantom 2
/// blocks (gap before 3).
/// PORTED-FAILING (BUG-PHANTOM-1).
#[test]
fn je_phantom_test_test_get_prev_notfound() {
    phantom_case(
        false,
        &[(3, 0)],
        &[Step::Search(3), Step::Prev],
        None,
        &[(4, 0)],
        (2, 0),
        Some((2, 0)),
    );
}

/// JE: PhantomTest.testGetPrev_NotFound_Dup.
/// dups {3,1},{3,2}; searchBoth(3,1)+getPrev=NOTFOUND; inserts {3,3},{4,0}
/// never blocked; phantom {2,1} blocks.
/// PORTED-FAILING (BUG-PHANTOM-1).
#[test]
fn je_phantom_test_test_get_prev_notfound_dup() {
    phantom_case(
        true,
        &[(3, 1), (3, 2)],
        &[Step::SearchBoth(3, 1), Step::Prev],
        None,
        &[(3, 3), (4, 0)],
        (2, 1),
        Some((2, 1)),
    );
}

// ===========================================================================
// getPrevDup
// ===========================================================================

/// JE: PhantomTest.testGetPrevDup_Success_Dup.
/// dups {1,1},{1,3}; searchBoth(1,3)+getPrevDup=>{1,1}; inserts {1,0},{0,0},
/// {1,4},{2,0} never blocked; phantom {1,2} blocks.
/// PORTED-FAILING (BUG-PHANTOM-1).
#[test]
fn je_phantom_test_test_get_prev_dup_success_dup() {
    phantom_case(
        true,
        &[(1, 1), (1, 3)],
        &[Step::SearchBoth(1, 3), Step::PrevDup],
        Some((1, 1)),
        &[(1, 0), (0, 0), (1, 4), (2, 0)],
        (1, 2),
        Some((1, 2)),
    );
}

/// JE: PhantomTest.testGetPrevDup_NotFound_Dup.
/// dups {1,1},{1,2},{2,1},{2,2}; searchBoth(2,2)+getPrevDup=NOTFOUND; inserts
/// {2,3},{3,0},{1,0},{0,0} never blocked; phantom {2,1}... (see JE); phantom is
/// {2,1}'s neighbor.  Follows the regular else-branch: phantom {2,1}.
/// PORTED-FAILING (BUG-PHANTOM-1).
#[test]
fn je_phantom_test_test_get_prev_dup_notfound_dup() {
    phantom_case(
        true,
        &[(1, 1), (1, 2), (2, 2), (2, 3)],
        &[Step::SearchBoth(2, 2), Step::PrevDup],
        None,
        &[(2, 4), (3, 0), (1, 0), (0, 0)],
        (2, 1),
        Some((2, 1)),
    );
}

// ===========================================================================
// getPrevNoDup
// ===========================================================================

/// JE: PhantomTest.testGetPrevNoDup_Success (PhantomTest.java:2472-ish).
/// insert 1,3; search(3)+getPrevNoDup=>1; inserts 0,4 never blocked; phantom 2
/// blocks.
/// PORTED-FAILING (BUG-PHANTOM-1).
#[test]
fn je_phantom_test_test_get_prev_no_dup_success() {
    phantom_case(
        false,
        &[(1, 0), (3, 0)],
        &[Step::Search(3), Step::PrevNoDup],
        Some((1, 0)),
        &[(0, 0), (4, 0)],
        (2, 0),
        Some((2, 0)),
    );
}

/// JE: PhantomTest.testGetPrevNoDup_Success_Dup.
/// dups {1,0},{1,2},{3,1},{3,2}; searchBoth(3,2)+getPrevNoDup=>{1,2}; inserts
/// {1,1},{0,0},{3,3},{4,0} never blocked; phantom {2,1} blocks.
/// PORTED-FAILING (BUG-PHANTOM-1).
#[test]
fn je_phantom_test_test_get_prev_no_dup_success_dup() {
    phantom_case(
        true,
        &[(1, 0), (1, 2), (3, 1), (3, 2)],
        &[Step::SearchBoth(3, 2), Step::PrevNoDup],
        Some((1, 2)),
        &[(1, 1), (0, 0), (3, 3), (4, 0)],
        (2, 1),
        Some((2, 1)),
    );
}

/// JE: PhantomTest.testGetPrevNoDup_NotFound.
/// insert 2; search(2)+getPrevNoDup=NOTFOUND; insert 3 never blocked; phantom 1
/// blocks (gap before 2).
/// PORTED-FAILING (BUG-PHANTOM-1).
#[test]
fn je_phantom_test_test_get_prev_no_dup_notfound() {
    phantom_case(
        false,
        &[(2, 0)],
        &[Step::Search(2), Step::PrevNoDup],
        None,
        &[(3, 0)],
        (1, 0),
        Some((1, 0)),
    );
}

/// JE: PhantomTest.testGetPrevNoDup_NotFound_Dup.
/// dups {2,1},{2,2}; searchBoth(2,2)+getPrevNoDup=NOTFOUND; inserts {2,3},{3,0}
/// never blocked; phantom {1,1} blocks.
/// PORTED-FAILING (BUG-PHANTOM-1).
#[test]
fn je_phantom_test_test_get_prev_no_dup_notfound_dup() {
    phantom_case(
        true,
        &[(2, 1), (2, 2)],
        &[Step::SearchBoth(2, 2), Step::PrevNoDup],
        None,
        &[(2, 3), (3, 0)],
        (1, 1),
        Some((1, 1)),
    );
}

// ===========================================================================
// Config / lock-mode validation and functional (non-matrix) tests
// ===========================================================================

/// JE: PhantomTest.testIllegalTransactionConfig (PhantomTest.java:2745).
/// serializable + readUncommitted together is illegal.
#[test]
fn je_phantom_test_test_illegal_transaction_config() {
    let (_d, env, _db) = open(false);
    let cfg = TransactionConfig::new()
        .with_serializable_isolation(true)
        .with_read_uncommitted(true);
    let r = env.begin_transaction(Some(&cfg));
    assert!(
        r.is_err(),
        "serializable + readUncommitted must be rejected (IllegalArgument)"
    );
    if let Ok(t) = r {
        let _ = t.abort();
    }
}

/// JE: PhantomTest.testEnvironmentConfig (PhantomTest.java:2772).
/// EnvironmentConfig.setTxnSerializableIsolation(true) makes txns serializable:
/// getFirst then insert-before-first blocks; false => allowed.
#[test]
fn je_phantom_test_test_environment_config() {
    // expectSerializable = false (default env).
    check_serializable_env(false);
    // expectSerializable = true (env serializable isolation).
    check_serializable_env(true);
}

fn check_serializable_env(expect_serializable: bool) {
    let dir = TempDir::new().unwrap();
    let mut cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        // JE PhantomTest.java:183-190 disables all four daemons.
        .with_run_evictor(false)
        .with_run_cleaner(false)
        .with_run_checkpointer(false)
        .with_run_in_compressor(false);
    if expect_serializable {
        cfg = cfg.with_txn_serializable_isolation(true);
    }
    let env = Environment::open(cfg).unwrap();
    let db = env
        .open_database(
            None,
            "PhantomTest",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    insert(&env, &db, false, 2, 0);
    let reader = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&reader, None).unwrap();
    let (st, kd) = run_step(&mut c, Step::First);
    assert_eq!(st, OperationStatus::Success);
    assert_eq!(kd, Some((2, 0)));
    // Insert of key 1 (before first) blocks iff serializable.
    let r = try_insert(&env, &db, false, 1, 0);
    if expect_serializable {
        assert!(
            r.is_err(),
            "env serializable: insert before first must block; got {r:?}"
        );
    } else {
        assert!(
            r.is_ok(),
            "non-serializable env: insert must be allowed; got {r:?}"
        );
    }
    drop(c);
    reader.commit().unwrap();
}

/// JE: PhantomTest.testCursorConfig (PhantomTest.java:2787).
/// A default cursor is non-serializable; setReadUncommitted keeps it
/// non-serializable (phantom insert allowed in both cases).
#[test]
fn je_phantom_test_test_cursor_config() {
    // Default (non-serializable).
    {
        let (_d, env, db) = open(false);
        insert(&env, &db, false, 2, 0);
        let reader = env.begin_transaction(None).unwrap();
        let mut c = db.open_cursor_in(&reader, None).unwrap();
        let (st, kd) = run_step(&mut c, Step::First);
        assert_eq!(st, OperationStatus::Success);
        assert_eq!(kd, Some((2, 0)));
        assert!(
            try_insert(&env, &db, false, 1, 0).is_ok(),
            "default cursor: insert allowed"
        );
        drop(c);
        reader.commit().unwrap();
    }
    // readUncommitted on the read op is still non-serializable.
    {
        let (_d, env, db) = open(false);
        insert(&env, &db, false, 2, 0);
        let reader = env.begin_transaction(None).unwrap();
        let mut c = db.open_cursor_in(&reader, None).unwrap();
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let st = c
            .get(
                &mut key,
                &mut data,
                Get::First,
                Some(LockMode::ReadUncommitted),
            )
            .unwrap();
        assert_eq!(st, OperationStatus::Success);
        assert!(
            try_insert(&env, &db, false, 1, 0).is_ok(),
            "readUncommitted cursor: insert allowed"
        );
        drop(c);
        reader.commit().unwrap();
    }
}

/// JE: PhantomTest.testReadUncommittedLockMode (PhantomTest.java:2804).
/// Env serializable, but LockMode.READ_UNCOMMITTED on the read op downgrades to
/// non-serializable: the phantom insert is allowed.
#[test]
fn je_phantom_test_test_read_uncommitted_lock_mode() {
    let dir = TempDir::new().unwrap();
    let env = Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            .with_txn_serializable_isolation(true),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "PhantomTest",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    insert(&env, &db, false, 2, 0);
    let reader = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&reader, None).unwrap();
    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let st = c
        .get(&mut key, &mut data, Get::First, Some(LockMode::ReadUncommitted))
        .unwrap();
    assert_eq!(st, OperationStatus::Success);
    assert_eq!(dk(key.data()), 2);
    // READ_UNCOMMITTED read holds no range lock => insert of 1 allowed.
    assert!(
        try_insert(&env, &db, false, 1, 0).is_ok(),
        "READ_UNCOMMITTED lock mode: insert before first must be allowed"
    );
    drop(c);
    reader.commit().unwrap();
}

/// JE: PhantomTest.testSingleDatumBug (PhantomTest.java:2900).
/// dups {1,1},{2,2}; searchBothRange(2,1)=>{2,2}; serializable insert {1,2}
/// (into the locked range before {2,2}) blocks; non-ser allowed.
#[test]
fn je_phantom_test_test_single_datum_bug() {
    phantom_case(
        true,
        &[(1, 1), (2, 2)],
        &[Step::SearchBothRange(2, 1)],
        Some((2, 2)),
        &[],
        (1, 2),
        None,
    );
}

/// JE: PhantomTest.testSearchKeySkipDeletedDup (PhantomTest.java:2946).
/// Insert {1,1},{1,2}; delete {1,1}; searchKey(1) must return SUCCESS at {1,2}
/// (skip the deleted dup), not NOTFOUND.
#[test]
fn je_phantom_test_test_search_key_skip_deleted_dup() {
    let (_d, env, db) = open(true);
    insert(&env, &db, true, 1, 1);
    insert(&env, &db, true, 1, 2);
    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&txn, None).unwrap();
    // Position on {1,1} and delete it.
    let (st, kd) = run_step(&mut c, Step::SearchBoth(1, 1));
    assert_eq!(st, OperationStatus::Success);
    assert_eq!(kd, Some((1, 1)));
    assert_eq!(c.delete().unwrap(), OperationStatus::Success);
    // searchKey(1) must skip the deleted {1,1} and return {1,2}.
    let (st2, kd2) = run_step(&mut c, Step::Search(1));
    assert_eq!(
        st2,
        OperationStatus::Success,
        "searchKey(1) must not return NOTFOUND"
    );
    assert_eq!(kd2, Some((1, 2)));
    drop(c);
    txn.commit().unwrap();
}

/// JE: PhantomTest.testNextAfterDupDeleteBug (PhantomTest.java:2988, SR#19026).
/// Insert {1,1},{2,1},{2,2}; delete {2,1}; then search(1) at {1,1} + getNextDup
/// must return NOTFOUND (not advance to {2,2}).
#[test]
fn je_phantom_test_test_next_after_dup_delete_bug() {
    let (_d, env, db) = open(true);
    insert(&env, &db, true, 1, 1);
    insert(&env, &db, true, 2, 1);
    insert(&env, &db, true, 2, 2);
    // Delete {2,1}.
    let txn = env.begin_transaction(None).unwrap();
    let mut c = db.open_cursor_in(&txn, None).unwrap();
    let (st, kd) = run_step(&mut c, Step::SearchBoth(2, 1));
    assert_eq!(st, OperationStatus::Success);
    assert_eq!(kd, Some((2, 1)));
    assert_eq!(c.delete().unwrap(), OperationStatus::Success);
    drop(c);
    txn.commit().unwrap();
    // On {1,1}, getNextDup must be NOTFOUND (must not move to {2,2}).
    let txn2 = env.begin_transaction(None).unwrap();
    let mut c2 = db.open_cursor_in(&txn2, None).unwrap();
    let (sa, ka) = run_step(&mut c2, Step::Search(1));
    assert_eq!(sa, OperationStatus::Success);
    assert_eq!(ka, Some((1, 1)));
    let (sb, _) = run_step(&mut c2, Step::NextDup);
    assert_eq!(
        sb,
        OperationStatus::NotFound,
        "getNextDup after dup-delete must be NOTFOUND"
    );
    drop(c2);
    txn2.commit().unwrap();
}

/// JE: PhantomTest.testSingleDegree3TxnOptimization (PhantomTest.java:2868).
/// Asserts `EnvironmentStats.getNRequests() == 1` after a single serializable
/// insert — i.e. the degree-3 (serializable) insert does NOT take the extra
/// next-key lock when it is the only txn.  This asserts JE's exact internal
/// lock-REQUEST accounting (`StatGroup` NRequests), which is a JE-internal
/// counter semantics, not an observable data/isolation behaviour.  Noxu's
/// stats model does not expose a per-insert `nRequests` counter with JE's
/// identical single-degree-3 optimization accounting.
/// NA: JE-internal lock-request-count statistic, not an observable behaviour.
#[test]
#[ignore = "NA: asserts JE-internal EnvironmentStats.getNRequests()==1 lock-count accounting, not observable behaviour"]
fn je_phantom_test_test_single_degree3_txn_optimization() {
    // Intentionally not asserted; see doc comment (NA).
}

// ===========================================================================
// BUG-PHANTOM-REVERSE-GAP witness (daemons ON).
//
// The gated phantom tests disable all four daemons, faithfully to JE
// PhantomTest.java:183-190.  With the daemons RUNNING (the pre-fidelity-fix
// config), the serializable range lock is NON-DETERMINISTIC: a background
// daemon (the IN compressor removing/merging a KD slot, chiefly) changes the
// "next key" between a scan's positioning and a phantom insert, so the phantom
// insert sometimes slips past the range lock (returns Ok instead of
// LockNotAvailable) and, in the other direction, a "safe" insert sometimes
// wrongly blocks.  Measured ~5-25% per case over 40x serial, both directions,
// across prev/next/search/first — see a15-phantom-rangelock-flake.md.
//
// This witness runs the SAME serializable must-block / safe-must-not-block
// assertions as `phantom_case`, but with the daemons ON, so the residual bug
// stays witnessed.  It is `#[ignore]`d (PORTED-FAILING) until
// BUG-PHANTOM-REVERSE-GAP is root-caused and fixed; its assertions are
// IDENTICAL to the gated path (nothing weakened).
// ===========================================================================

/// Like [`open`] but leaves all four daemons RUNNING (the config that exposes
/// BUG-PHANTOM-REVERSE-GAP).  Deliberately NOT the faithful JE config.
fn open_daemons_on(dups: bool) -> (TempDir, Environment, noxu_db::Database) {
    let dir = TempDir::new().unwrap();
    let env = Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "PhantomTest",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true)
                .with_sorted_duplicates(dups),
        )
        .unwrap();
    (dir, env, db)
}

/// One serializable must-block / safe-must-not-block scenario with daemons ON.
/// Returns `Err` with a description iff an assertion would fail (phantom
/// slipped past the range lock, or a safe insert wrongly blocked).  Identical
/// logic to the gated `phantom_case` serializable half; only the env config
/// (daemons running) differs.
fn daemons_on_scenario(
    dups: bool,
    seeds: &[(i32, i32)],
    pos: &[Step],
    safe: &[(i32, i32)],
    phantom: (i32, i32),
) -> Result<(), String> {
    let (_d, env, db) = open_daemons_on(dups);
    for &(k, v) in seeds {
        insert(&env, &db, dups, k, v);
    }
    let ser = TransactionConfig::new().with_serializable_isolation(true);
    let reader = env.begin_transaction(Some(&ser)).unwrap();
    let mut c = db.open_cursor_in(&reader, None).unwrap();
    for &step in pos {
        let _ = run_step(&mut c, step);
    }
    for &(k, v) in safe {
        if try_insert(&env, &db, dups, k, v).is_err() {
            return Err(format!("safe insert ({k},{v}) wrongly blocked"));
        }
    }
    let r = try_insert(&env, &db, dups, phantom.0, phantom.1);
    let ok = matches!(r, Err(NoxuError::LockNotAvailable));
    drop(c);
    reader.commit().unwrap();
    if ok {
        Ok(())
    } else {
        Err(format!("phantom insert {phantom:?} did NOT block; got {r:?}"))
    }
}

#[test]
#[ignore = "PORTED-FAILING BUG-PHANTOM-REVERSE-GAP: serializable range lock \
            non-deterministic with daemons running; see \
            a15-phantom-rangelock-flake.md"]
fn je_phantom_daemons_on_residual() {
    // Run the measured-flaky serializable cases (prev / next / search / first,
    // dup and non-dup) with the daemons ON, repeated enough times to reliably
    // surface the ~5-25%-per-case non-determinism.  Assertions are IDENTICAL
    // to the gated path; only the env config (daemons running) differs, which
    // is the whole point of the witness.  When BUG-PHANTOM-REVERSE-GAP is
    // fixed this must pass every iteration and be un-ignored.
    // (dups, seeds, positioning steps, safe-inserts, phantom-insert)
    type PhantomScenario<'a> =
        (bool, &'a [(i32, i32)], &'a [Step], &'a [(i32, i32)], (i32, i32));
    let cases: &[PhantomScenario] = &[
        // reverse / preceding-gap (worst measured)
        (
            true,
            &[(1, 1), (1, 2), (2, 2), (2, 3)],
            &[Step::SearchBoth(2, 2), Step::PrevDup],
            &[(2, 4), (3, 0), (1, 0), (0, 0)],
            (2, 1),
        ),
        (
            false,
            &[(1, 0), (2, 0), (3, 0)],
            &[Step::Search(2), Step::Prev],
            &[(0, 0), (3, 0)],
            (1, 0),
        ),
        // forward
        (
            false,
            &[(1, 0), (2, 0), (3, 0)],
            &[Step::Search(2), Step::Next],
            &[(0, 0), (1, 0)],
            (2, 0),
        ),
        // first
        (
            false,
            &[(1, 0), (2, 0), (3, 0)],
            &[Step::First],
            &[(2, 0), (3, 0)],
            (1, 0),
        ),
    ];
    let mut failures: Vec<String> = Vec::new();
    for _ in 0..25 {
        for &(dups, seeds, pos, safe, phantom) in cases {
            if let Err(e) = daemons_on_scenario(dups, seeds, pos, safe, phantom)
            {
                failures.push(e);
            }
        }
    }
    assert!(
        failures.is_empty(),
        "serializable (daemons on) range lock non-deterministic \
         (BUG-PHANTOM-REVERSE-GAP): {} failure(s), e.g. {:?}",
        failures.len(),
        failures.first()
    );
}
