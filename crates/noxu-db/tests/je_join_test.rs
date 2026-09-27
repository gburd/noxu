//! JE `JoinTest` port — natural-join intersection over multiple secondary
//! indexes.  Faithful port of
//! `test/com/sleepycat/je/test/JoinTest.java`.
//!
//! JE citations:
//!   - `JoinTest.testJoin` — the 7 data-set / join-set matrix, both
//!     with-data and key-only, both default and no-sort JoinConfig.
//!   - `JoinTest.testWriteDuringJoin` — join does not block a concurrent
//!     writer inserting dups for the same main key (`[#11833]`: join uses
//!     READ_UNCOMMITTED to obtain per-cursor dup counts).
//!
//! Data model (verbatim from JE): the primary key is a single byte; the
//! primary data is a 3-byte array whose bytes at positions 0/1/2 are the
//! secondary key values for indexes 0/1/2.  A **zero** byte means "not
//! indexed" (the key creator returns `false`), so a zero value is never
//! used as a join search key.
//!
//! Intentional deviations from the JE test (documented):
//!   - JE asserts `DbInternal.getSortedCursors(jc)` ordering (cursors are
//!     re-ordered by ascending dup count unless no-sort).  Noxu applies the
//!     same sort inside `JoinCursor::new` but does **not** expose the sorted
//!     cursor array publicly (JE-internal `DbInternal` reflection).  The
//!     substantive assertion — that the join returns exactly the expected
//!     primary-key set in either config — is preserved for BOTH
//!     default (sort) and no-sort configs, which exercises both paths.
//!   - JE reuses one env and removes DBs between data sets; Noxu opens a
//!     fresh env per data set (same net effect, simpler with RAII TempDir).

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    JoinConfig, OperationStatus, SecondaryConfig, SecondaryDatabase,
    SecondaryKeyCreator,
};
use noxu_sync::Mutex;
use std::sync::Arc;
use tempfile::TempDir;

// ─── Key creator: extract data byte at `key_id`; 0 means "don't index" ──
struct ByteAtKeyCreator {
    key_id: usize,
}
impl SecondaryKeyCreator for ByteAtKeyCreator {
    fn create_secondary_key(
        &self,
        _db: &Database,
        _key: &DatabaseEntry,
        data: &DatabaseEntry,
        result: &mut DatabaseEntry,
    ) -> bool {
        // JE MyKeyCreator: byte val = data.getData()[keyId]; index iff != 0.
        if let Some(d) = data.data_opt()
            && self.key_id < d.len()
        {
            let val = d[self.key_id];
            if val != 0 {
                result.set_data(&[val]);
                return true;
            }
        }
        false
    }
}

fn open_env(dir: &TempDir) -> Environment {
    Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap()
}

fn open_primary(env: &Environment, name: &str) -> Arc<Mutex<Database>> {
    let db = env
        .open_database(
            None,
            name,
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    Arc::new(Mutex::new(db))
}

fn open_secondary(
    env: &Environment,
    primary: &Arc<Mutex<Database>>,
    name: &str,
    key_id: usize,
) -> SecondaryDatabase {
    // JE opens each secondary with sortedDuplicates=true (dups): a single
    // secondary key value maps to many primaries.  The inner index DB must
    // therefore allow duplicates (v1.6 sorted-dup secondaries).
    let inner = env
        .open_database(
            None,
            name,
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true)
                .with_sorted_duplicates(true),
        )
        .unwrap();
    SecondaryDatabase::open(
        Arc::clone(primary),
        inner,
        SecondaryConfig::new()
            .with_allow_create(true)
            .with_key_creator(Box::new(ByteAtKeyCreator { key_id })),
    )
    .unwrap()
}

/// One (data-set, join-set) pair from JE's `ALL` matrix.
struct JoinCase {
    /// Records to insert: (pri_key, [byte0, byte1, byte2]).
    data: &'static [(u8, [u8; 3])],
    /// Join queries: (search keys for sec 0/1/2, expected primary keys).
    joins: &'static [([u8; 3], &'static [u8])],
}

// JE ALL[] verbatim (7 data sets, each paired with its join set).
const CASES: &[JoinCase] = &[
    // Data set #1 - single match possible per record.
    JoinCase {
        data: &[(11, [1, 1, 1]), (12, [2, 2, 2]), (13, [3, 3, 3])],
        joins: &[
            ([1, 1, 1], &[11]),
            ([2, 2, 2], &[12]),
            ([3, 3, 3], &[13]),
            // Note: JE's join-set rows with empty expected are only issued
            // when all three search keys are non-zero; rows with a zero
            // search key can't position a cursor and JE never issues them.
            // The three non-empty rows above cover set #1's join intent.
        ],
    },
    // Data set #2 - no match possible when some indices are zero.
    JoinCase {
        data: &[
            (11, [1, 1, 0]),
            (12, [2, 0, 2]),
            (13, [0, 3, 3]),
            (14, [3, 2, 1]),
        ],
        joins: &[([1, 1, 1], &[]), ([2, 2, 2], &[]), ([3, 3, 3], &[])],
    },
    // Data set #3 - one match with non-matching (missing/zero) records.
    JoinCase {
        data: &[
            (11, [1, 0, 0]),
            (12, [1, 1, 0]),
            (13, [1, 1, 1]),
            (14, [0, 0, 0]),
        ],
        joins: &[([1, 1, 1], &[13])],
    },
    // Data set #4 - one match with non-matching (non-zero) records.
    JoinCase {
        data: &[
            (11, [1, 2, 3]),
            (12, [1, 1, 3]),
            (13, [1, 1, 1]),
            (14, [3, 2, 1]),
        ],
        joins: &[([1, 1, 1], &[13])],
    },
    // Data set #5 - two matches with non-matching records.
    JoinCase {
        data: &[
            (11, [1, 2, 3]),
            (12, [1, 1, 3]),
            (13, [1, 1, 1]),
            (14, [1, 2, 3]),
        ],
        joins: &[([1, 2, 3], &[11, 14])],
    },
    // Data set #6 - three matches with non-matching records; also verifies
    // cursor sort-by-count (2, 1, 0) internally.
    JoinCase {
        data: &[
            (11, [1, 2, 3]),
            (12, [1, 1, 3]),
            (13, [1, 1, 1]),
            (14, [1, 2, 3]),
            (15, [1, 1, 1]),
            (16, [1, 0, 0]),
            (17, [1, 1, 0]),
            (18, [1, 1, 1]),
            (19, [0, 0, 0]),
            (20, [3, 2, 1]),
        ],
        joins: &[([1, 1, 1], &[13, 15, 18])],
    },
    // Data set #7 - three matches by themselves.
    JoinCase {
        data: &[(11, [1, 2, 3]), (12, [1, 2, 3]), (13, [1, 2, 3])],
        joins: &[([1, 2, 3], &[11, 12, 13])],
    },
];

/// Runs one join query and returns the primary keys found, sorted.
fn run_join(
    primary: &Arc<Mutex<Database>>,
    sec: &[SecondaryDatabase; 3],
    search: [u8; 3],
    no_sort: bool,
    with_data: bool,
    // The inserted (pri_key -> data) map, to verify data when requested.
    data_by_pk: &[(u8, [u8; 3])],
) -> Vec<u8> {
    // Position one cursor per secondary at its search key.  All three
    // search keys are guaranteed non-zero for every join issued here.
    let mut c0 = sec[0].open_cursor(None).unwrap();
    let mut c1 = sec[1].open_cursor(None).unwrap();
    let mut c2 = sec[2].open_cursor(None).unwrap();
    for (c, k) in
        [(&mut c0, search[0]), (&mut c1, search[1]), (&mut c2, search[2])]
    {
        let mut p = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        let s = c
            .get_search_key(&DatabaseEntry::from_bytes(&[k]), &mut p, &mut d)
            .unwrap();
        assert_eq!(
            s,
            OperationStatus::Success,
            "cursor must position at search key {k}"
        );
    }

    let pri = primary.lock();
    let cfg =
        if no_sort { Some(JoinConfig::new().with_no_sort(true)) } else { None };
    // JE asserts `jc.getDatabase() == priDb`; Noxu's `join.database()`
    // returns the primary ref by construction.  We just confirm the call
    // is wired (a getter, not the substantive join assertion).
    let mut join = pri.join(vec![c0, c1, c2], cfg).unwrap();
    let _ = join.database();

    let mut got: Vec<u8> = Vec::new();
    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    loop {
        let st = if with_data {
            join.get_next(&mut key, &mut data).unwrap()
        } else {
            join.get_next_key(&mut key).unwrap()
        };
        if st != OperationStatus::Success {
            break;
        }
        let pk = key.data_opt().unwrap()[0];
        got.push(pk);
        if with_data {
            // JE: the returned data must be the primary record of pk.
            let expected = data_by_pk
                .iter()
                .find(|(k, _)| *k == pk)
                .map(|(_, d)| d)
                .expect("join returned a pk that was never inserted");
            assert_eq!(
                data.data_opt().unwrap(),
                &expected[..],
                "join with-data must return the primary record for pk={pk}"
            );
        }
    }
    got.sort_unstable();
    got
}

/// JE `JoinTest.testJoin`: for every data set, issue each join query in the
/// with-data × {default, no-sort} matrix and confirm the intersection is
/// exactly the expected primary-key set (and NOTFOUND terminates).
#[test]
fn je_join_test_test_join() {
    for (set_num, case) in CASES.iter().enumerate() {
        for with_data in [false, true] {
            let dir = TempDir::new().unwrap();
            let env = open_env(&dir);
            let primary = open_primary(&env, "pri");
            let sec = [
                open_secondary(&env, &primary, "sec0", 0),
                open_secondary(&env, &primary, "sec1", 1),
                open_secondary(&env, &primary, "sec2", 2),
            ];

            // Populate the primary; secondaries auto-maintain via hooks.
            {
                let pri = primary.lock();
                for (pk, d) in case.data {
                    // Database::put returns Result<()>; success == Ok.
                    pri.put([*pk], &d[..]).unwrap();
                }
            }

            for (search, expected) in case.joins {
                for no_sort in [false, true] {
                    let mut want: Vec<u8> = expected.to_vec();
                    want.sort_unstable();
                    let got = run_join(
                        &primary, &sec, *search, no_sort, with_data, case.data,
                    );
                    assert_eq!(
                        got,
                        want,
                        "set#{} search={:?} no_sort={} with_data={}: join \
                         intersection mismatch",
                        set_num + 1,
                        search,
                        no_sort,
                        with_data
                    );
                }
            }
        }
    }
}

/// JE `JoinTest.testWriteDuringJoin` (`[#11833]`): after `join()` obtains the
/// per-cursor dup counts (using READ_UNCOMMITTED internally), a concurrent
/// writer inserting a dup for the same main key must NOT deadlock, and the
/// in-flight join must still return the two records present when it started.
///
/// Noxu port: JE uses two transactions (a reader txn holding the cursors and a
/// separate writer txn) in one thread — the writer's put happening between
/// `join()` and `getNext()` proves join did not leave a blocking read lock on
/// the count-probe.  We reproduce the same interleaving without a background
/// thread: begin the join, then perform a writer put under a *separate*
/// transaction, then drain the join.
#[test]
fn je_join_test_test_write_during_join() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_primary(&env, "pri");
    let sec = [
        open_secondary(&env, &primary, "sec0", 0),
        open_secondary(&env, &primary, "sec1", 1),
        open_secondary(&env, &primary, "sec2", 2),
    ];

    // Insert pk 13 and 14, both with data {1,1,1}.
    {
        let pri = primary.lock();
        for pk in [13u8, 14u8] {
            pri.put([pk], [1u8, 1, 1]).unwrap();
        }
    }

    // Position each cursor at secondary key 1 (READ_UNCOMMITTED, matching JE).
    let cfg = noxu_db::CursorConfig::new().with_read_uncommitted(true);
    let mut c0 = sec[0].open_cursor(Some(&cfg)).unwrap();
    let mut c1 = sec[1].open_cursor(Some(&cfg)).unwrap();
    let mut c2 = sec[2].open_cursor(Some(&cfg)).unwrap();
    for c in [&mut c0, &mut c1, &mut c2] {
        let mut p = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        let s = c
            .get_search_key(&DatabaseEntry::from_bytes(&[1u8]), &mut p, &mut d)
            .unwrap();
        assert_eq!(s, OperationStatus::Success);
    }

    let pri = primary.lock();
    // join() gets the cursor counts (READ_UNCOMMITTED — no blocking locks).
    let mut join = pri.join(vec![c0, c1, c2], None).unwrap();

    // After join(), insert a dup for the same main key (12) — must not block.
    // Database::put returns Ok(()) on success; the fact it returns rather
    // than deadlocking is the assertion ([#11833]).
    pri.put([12u8], [1u8, 1, 1]).unwrap();

    // The join should retrieve exactly 13 and 14 (the snapshot at join start).
    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let mut got: Vec<u8> = Vec::new();
    while join.get_next(&mut key, &mut data).unwrap()
        == OperationStatus::Success
    {
        got.push(key.data_opt().unwrap()[0]);
    }
    got.sort_unstable();
    assert_eq!(
        got,
        vec![13u8, 14u8],
        "join must return the two pre-join records"
    );

    // Try writing again after draining — still must not block.
    pri.put([11u8], [1u8, 1, 1]).unwrap();
}

/// NEW-JOIN-1 deletion-safety control: `new()` must eagerly drain cursor[0]'s
/// ENTIRE duplicate set into the candidate list, so removing the (buggy)
/// refill in `next_matching_candidate` cannot drop legitimate candidates.
///
/// We build a secondary whose search key `sec0='1'` is shared by 500
/// primaries, and a second secondary whose search key `sec1='1'` is shared by
/// a KNOWN even-keyed subset of those.  The join intersection must be exactly
/// that subset (250 primaries), with cursor[0] sorted first (fewest dups) so
/// the default (sort) path is exercised.  If the deletion dropped candidates,
/// the intersection would come up short.
#[test]
fn je_join_test_new_join_1_large_dup_set_no_candidates_dropped() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_primary(&env, "pri");
    // sec0 keys on data byte 0; sec1 keys on data byte 1.
    let sec = [
        open_secondary(&env, &primary, "sec0", 0),
        open_secondary(&env, &primary, "sec1", 1),
    ];

    // 500 primaries, all with sec0 byte = 1.  Even-numbered pk get sec1
    // byte = 1 (in the intersection); odd get sec1 byte = 2 (excluded).
    // Primary keys are 2 bytes so all 500 fit distinctly.
    let mut expected: Vec<[u8; 2]> = Vec::new();
    {
        let pri = primary.lock();
        for i in 0u16..500 {
            let pk = i.to_be_bytes();
            let sec1_byte = if i % 2 == 0 { 1u8 } else { 2u8 };
            // data = [sec0=1, sec1=sec1_byte, filler]
            pri.put(pk, [1u8, sec1_byte, 0u8]).unwrap();
            if sec1_byte == 1 {
                expected.push(pk);
            }
        }
    }

    // Position both cursors at their '1' keys.
    let mut c0 = sec[0].open_cursor(None).unwrap();
    let mut c1 = sec[1].open_cursor(None).unwrap();
    for c in [&mut c0, &mut c1] {
        let mut p = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        assert_eq!(
            c.get_search_key(
                &DatabaseEntry::from_bytes(&[1u8]),
                &mut p,
                &mut d
            )
            .unwrap(),
            OperationStatus::Success
        );
    }

    // Default JoinConfig => sorts by count ascending.  c1 (sec1='1') has 250
    // dups; c0 (sec0='1') has 500 — so c1 sorts first as cursor[0].  This is
    // the exact configuration that exposed NEW-JOIN-1 (a large cursor[0] dup
    // set feeding the deleted refill path).
    let pri = primary.lock();
    let mut join = pri.join(vec![c0, c1], None).unwrap();

    let mut got: Vec<[u8; 2]> = Vec::new();
    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    while join.get_next(&mut key, &mut data).unwrap()
        == OperationStatus::Success
    {
        let b = key.data_opt().unwrap();
        got.push([b[0], b[1]]);
    }
    got.sort_unstable();
    expected.sort_unstable();
    assert_eq!(
        got.len(),
        250,
        "intersection must contain all 250 even-keyed primaries; a dropped \
         candidate would shorten this (deletion-safety proof for NEW-JOIN-1)"
    );
    assert_eq!(got, expected, "join must be exactly the even-keyed subset");
}

/// NEW-PNO-SEC-1 (secondary data-integrity): the JE `JoinTest.testJoin`
/// matrix run through `Database::put_no_overwrite` instead of `put`.
///
/// This is the primary repro for NEW-PNO-SEC-1.  Before the fix,
/// `put_no_overwrite_bytes` fired put-triggers on a successful insert but
/// never called `hook.maintain(...)`, so every registered secondary was left
/// WITHOUT the new entry — the join over those secondaries returned nothing
/// (or the wrong set).  After the fix, `put_no_overwrite` maintains
/// secondaries exactly like `put`, so this test matches
/// `je_join_test_test_join` (the via-`put` control) byte for byte.
///
/// Fails on base 0c54a122 (secondaries empty after put_no_overwrite → join
/// intersection mismatch); passes on fix.
#[test]
fn je_join_test_test_join_via_put_no_overwrite() {
    for (set_num, case) in CASES.iter().enumerate() {
        for with_data in [false, true] {
            let dir = TempDir::new().unwrap();
            let env = open_env(&dir);
            let primary = open_primary(&env, "pri");
            let sec = [
                open_secondary(&env, &primary, "sec0", 0),
                open_secondary(&env, &primary, "sec1", 1),
                open_secondary(&env, &primary, "sec2", 2),
            ];

            // Populate the primary via put_no_overwrite; secondaries must
            // auto-maintain via hooks (NEW-PNO-SEC-1).  Every pk in a case is
            // distinct, so each no-overwrite is a fresh insert (Ok(true)).
            {
                let pri = primary.lock();
                for (pk, d) in case.data {
                    let inserted = pri.put_no_overwrite([*pk], &d[..]).unwrap();
                    assert!(
                        inserted,
                        "set#{} pk={pk}: put_no_overwrite of a distinct key \
                         must insert",
                        set_num + 1
                    );
                }
            }

            for (search, expected) in case.joins {
                for no_sort in [false, true] {
                    let mut want: Vec<u8> = expected.to_vec();
                    want.sort_unstable();
                    let got = run_join(
                        &primary, &sec, *search, no_sort, with_data, case.data,
                    );
                    assert_eq!(
                        got,
                        want,
                        "set#{} search={:?} no_sort={} with_data={}: join \
                         intersection mismatch (secondaries not maintained by \
                         put_no_overwrite? — NEW-PNO-SEC-1)",
                        set_num + 1,
                        search,
                        no_sort,
                        with_data
                    );
                }
            }
        }
    }
}

/// NEW-PNO-SEC-1 focused control: a single `put_no_overwrite` insert must be
/// visible via the SECONDARY index (query by secondary key finds the record).
///
/// Fails on base (secondary empty → not found); passes on fix.
#[test]
fn je_join_test_pno_sec_1_put_no_overwrite_maintains_secondary() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_primary(&env, "pri");
    let sec = open_secondary(&env, &primary, "sec0", 0);

    // Insert pk=42 with data byte0=7 via put_no_overwrite (fresh key).
    {
        let pri = primary.lock();
        let inserted = pri.put_no_overwrite([42u8], [7u8, 0, 0]).unwrap();
        assert!(inserted, "fresh key must insert");
    }

    // The secondary index (keyed on data byte 0 = 7) must now contain the
    // entry — query by secondary key '7' must find primary key 42 and its
    // data.  Before the fix the secondary was empty, so this returned false.
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let found = sec.get_into(None, [7u8], &mut p_key, &mut data).unwrap();
    assert!(
        found,
        "secondary query by key must find the put_no_overwrite record \
         (NEW-PNO-SEC-1)"
    );
    assert_eq!(p_key.data_opt().unwrap(), &[42u8][..], "primary key mismatch");
    assert_eq!(
        data.data_opt().unwrap(),
        &[7u8, 0, 0][..],
        "primary data mismatch"
    );
}

/// NEW-PNO-SEC-1 dup-key control: a `put_no_overwrite` that FAILS because the
/// key already exists must NOT touch the secondary index (no change → no
/// maintain).  A successful `put` establishes the record and its secondary
/// entry; a subsequent `put_no_overwrite` with a DIFFERENT data value must
/// return false and leave both the primary AND the old secondary entry intact
/// (the new secondary key must NOT appear).
#[test]
fn je_join_test_pno_sec_1_dup_key_does_not_maintain_secondary() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let primary = open_primary(&env, "pri");
    let sec = open_secondary(&env, &primary, "sec0", 0);

    {
        let pri = primary.lock();
        // Establish pk=42 with secondary key '7'.
        let inserted = pri.put_no_overwrite([42u8], [7u8, 0, 0]).unwrap();
        assert!(inserted);
        // Now attempt a no-overwrite with a DIFFERENT data value whose
        // secondary key would be '9'.  The key already exists, so this must
        // FAIL (return false) and NOT insert / NOT maintain secondaries.
        let inserted2 = pri.put_no_overwrite([42u8], [9u8, 0, 0]).unwrap();
        assert!(
            !inserted2,
            "put_no_overwrite of an existing key must return false"
        );
    }

    // The original secondary key '7' is still present (the primary is
    // unchanged).
    assert!(
        sec.exists(None, &DatabaseEntry::from_bytes(&[7u8])).unwrap(),
        "the original secondary entry must survive a failed no-overwrite"
    );
    // The would-be new secondary key '9' must NOT exist — the failed
    // no-overwrite made no change, so it maintained nothing.
    assert!(
        !sec.exists(None, &DatabaseEntry::from_bytes(&[9u8])).unwrap(),
        "a failed no-overwrite must NOT create a secondary entry \
         (NEW-PNO-SEC-1: no change → no maintain)"
    );
    // And the primary data is unchanged (still byte0=7).
    let pri = primary.lock();
    let mut data = DatabaseEntry::new();
    assert!(pri.get_into(None, [42u8], &mut data).unwrap());
    assert_eq!(data.data_opt().unwrap(), &[7u8, 0, 0][..]);
}
