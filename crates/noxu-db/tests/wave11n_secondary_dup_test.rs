//! Wave 11-N regression tests for the four sorted-dup cursor bugs that
//! Wave 11 v2.3.1 surfaced.  Bugs 1 and 2 are exercised by
//! `je_db_cursor_test::db_cursor_duplicate_test_duplicate_count` /
//! `db_cursor_duplicate_test_get_next_dup`; bugs 3 and 4 are exercised
//! here against the public `SecondaryCursor` API.
//!
//! See the 2026 review for the
//! per-bug analysis.
//!
//! Both regressions follow the same shape as Wave 11-B's W13 benchmark
//! workload: a primary populated with N records, a sorted-dup secondary
//! that buckets primaries by `primary_key % BUCKETS` so each secondary
//! key owns ~N/BUCKETS primaries.

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    OperationStatus, SecondaryConfig, SecondaryDatabase, SecondaryKeyCreator,
};
use noxu_sync::Mutex;
use std::sync::Arc;
use tempfile::TempDir;

/// Buckets primary keys (4-byte big-endian u32) into a small number of
/// secondary keys (1-byte bucket id), so several primaries map to the
/// same secondary key — the multi-primary regime that surfaces bugs 3
/// and 4.
struct BucketKeyCreator {
    buckets: u32,
}

impl SecondaryKeyCreator for BucketKeyCreator {
    fn create_secondary_key(
        &self,
        _db: &Database,
        key: &DatabaseEntry,
        _data: &DatabaseEntry,
        result: &mut DatabaseEntry,
    ) -> bool {
        let bytes = key.get_data().unwrap_or(&[]);
        if bytes.len() != 4 {
            return false;
        }
        let n = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let bucket = (n % self.buckets) as u8;
        result.set_data(&[bucket]);
        true
    }
}

fn open_pri_sec(
    n: usize,
    buckets: u32,
) -> (TempDir, Environment, Arc<Mutex<Database>>, SecondaryDatabase) {
    let dir = TempDir::new().unwrap();
    let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(env_cfg).unwrap();

    let pri_cfg =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    let primary = Arc::new(Mutex::new(
        env.open_database(None, "wave11n_primary", &pri_cfg).unwrap(),
    ));

    {
        let pri = primary.lock();
        let value = DatabaseEntry::from_bytes(b"value");
        for i in 0..n as u32 {
            let k = DatabaseEntry::from_bytes(&i.to_be_bytes());
            pri.put(None, &k, &value).unwrap();
        }
    }

    let sec_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_sorted_duplicates(true);
    let sec_db =
        env.open_database(None, "wave11n_secondary", &sec_cfg).unwrap();
    let sec_config = SecondaryConfig::new()
        .with_allow_create(true)
        .with_allow_populate(true)
        .with_key_creator(Box::new(BucketKeyCreator { buckets }));
    let secondary =
        SecondaryDatabase::open(Arc::clone(&primary), sec_db, sec_config)
            .unwrap();

    (dir, env, primary, secondary)
}

/// Bug 3 regression: `get_search_key` + repeated `get_next_dup_full`
/// must yield every primary whose key falls into the requested bucket
/// without raising `SecondaryIntegrityException`.  Pre-fix the very
/// first `get_next_dup_full` after a `get_search_key` on a non-first
/// bucket either reported NotFound prematurely (because the same
/// `Search+NextDup` boundary bug from Bug 2 triggered through the
/// secondary layer) or surfaced as a `SecondaryIntegrityException`
/// when the underlying inner cursor stepped onto a foreign primary
/// whose data slot did not match a real primary record.
#[test]
fn wave11n_bug3_get_search_key_then_next_dup_full_yields_all() {
    const N: usize = 60;
    const BUCKETS: u32 = 6;
    let (_tmp, _env, _primary, secondary) = open_pri_sec(N, BUCKETS);

    // Iterate every bucket and confirm get_search_key + get_next_dup_full
    // visits exactly the expected primary keys for that bucket.
    for bucket in 0..BUCKETS as u8 {
        let mut cursor = secondary.open_cursor(None, None).unwrap();
        let search = DatabaseEntry::from_bytes(&[bucket]);
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let s = cursor.get_search_key(&search, &mut p_key, &mut data).expect(
            "get_search_key must not raise SecondaryIntegrityException",
        );
        assert_eq!(s, OperationStatus::Success, "bucket={bucket}");

        let mut seen: Vec<u32> = Vec::new();
        let pk_bytes = p_key.get_data().unwrap();
        seen.push(u32::from_be_bytes([
            pk_bytes[0],
            pk_bytes[1],
            pk_bytes[2],
            pk_bytes[3],
        ]));

        let mut sec_key_out = DatabaseEntry::new();
        loop {
            let s = cursor
                .get_next_dup_full(&mut sec_key_out, &mut p_key, &mut data)
                .expect("get_next_dup_full must not raise");
            if s == OperationStatus::NotFound {
                break;
            }
            assert_eq!(s, OperationStatus::Success);
            // Must stay inside the same bucket.
            assert_eq!(
                sec_key_out.get_data().unwrap(),
                &[bucket],
                "bucket={bucket}: get_next_dup_full crossed sec-key boundary",
            );
            let pk_bytes = p_key.get_data().unwrap();
            seen.push(u32::from_be_bytes([
                pk_bytes[0],
                pk_bytes[1],
                pk_bytes[2],
                pk_bytes[3],
            ]));
        }
        cursor.close().unwrap();

        let mut expected: Vec<u32> =
            (0..N as u32).filter(|i| (i % BUCKETS) as u8 == bucket).collect();
        expected.sort();
        let mut seen_sorted = seen.clone();
        seen_sorted.sort();
        assert_eq!(
            seen_sorted, expected,
            "bucket={bucket}: visited primaries differ from bucket members",
        );
    }
}

/// Bug 4 regression: `get_first` + repeated `get_next` must terminate
/// after visiting every (sec_key, primary_key, data) triple exactly
/// once — no revisits, no infinite loop, no `SecondaryIntegrityException`.
/// Pre-fix the walk could revisit primaries (because `current_index`
/// was wrong after sorted-dup positioning, so the BIN-internal step
/// landed on a stale slot) or fail to terminate altogether.
#[test]
fn wave11n_bug4_get_first_get_next_full_walk_terminates() {
    const N: usize = 200;
    const BUCKETS: u32 = 16;
    let (_tmp, _env, _primary, secondary) = open_pri_sec(N, BUCKETS);

    let mut cursor = secondary.open_cursor(None, None).unwrap();
    let mut sec_key = DatabaseEntry::new();
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();

    let mut seen: Vec<(u8, u32)> = Vec::new();
    let cap = N * 2 + 16;

    let mut s = cursor
        .get_first(&mut sec_key, &mut p_key, &mut data)
        .expect("get_first must not raise");
    while s == OperationStatus::Success {
        let sk = sec_key.get_data().unwrap();
        assert_eq!(sk.len(), 1, "secondary keys are 1-byte bucket ids");
        let pk = p_key.get_data().unwrap();
        assert_eq!(pk.len(), 4, "primary keys are 4-byte u32");
        let entry = (sk[0], u32::from_be_bytes([pk[0], pk[1], pk[2], pk[3]]));
        seen.push(entry);
        assert!(
            seen.len() <= cap,
            "walk did not terminate after {} steps",
            seen.len()
        );
        s = cursor
            .get_next(&mut sec_key, &mut p_key, &mut data)
            .expect("get_next must not raise");
    }
    cursor.close().unwrap();

    // Exactly N triples, no duplicates.
    assert_eq!(
        seen.len(),
        N,
        "expected exactly {N} triples, got {}",
        seen.len()
    );
    let mut sorted = seen.clone();
    sorted.sort();
    let before_dedup = sorted.len();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        before_dedup,
        "walk revisited {} (sec_key, primary_key) pair(s)",
        before_dedup - sorted.len(),
    );

    // Each primary appears exactly once with the correct bucket id.
    for (sk, pk) in &seen {
        assert_eq!(
            (pk % BUCKETS) as u8,
            *sk,
            "primary {pk} reported in bucket {sk}",
        );
    }
}
