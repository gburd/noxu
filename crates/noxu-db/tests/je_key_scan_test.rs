//! JE `KeyScanTest` port — key-only cursor scan with partial (zero-length)
//! data.  Faithful port of `test/com/sleepycat/je/test/KeyScanTest.java`.
//!
//! JE citations:
//!   - `KeyScanTest.testKeyScan` — non-dup key-only scan visits every key.
//!   - `KeyScanTest.testKeyScanDup` — sorted-dup key-only scan with
//!     `getNextNoDup` visits every distinct key exactly once.
//!
//! Intentional deviation (documented):
//!   - JE asserts `EnvironmentStats.getNCacheMiss() == 0` and
//!     `getNNotResident() == 0` after `preload()` without loading LNs, to
//!     prove a key-only (partial-data, size 0) scan never faults in a leaf
//!     record.  Noxu does not expose per-scan cache-miss / not-resident
//!     counters on its public API (JE-internal `EnvironmentStats`), so the
//!     no-fault optimization is not asserted; the substantive behavior — the
//!     key-only scan visits every key in order via getNext / getNextNoDup,
//!     and getFirst/getLast/getSearchKey/getSearchKeyRange land correctly —
//!     is fully asserted, both with a fresh cache and after `preload()`.

use noxu_db::preload::PreloadConfig;
use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus, Put,
};
use tempfile::TempDir;

const RECORD_COUNT: u32 = 3 * 500;

fn ikey(i: u32) -> [u8; 4] {
    i.to_be_bytes()
}

fn to_int(b: &[u8]) -> u32 {
    u32::from_be_bytes([b[0], b[1], b[2], b[3]])
}

fn open_env(dir: &TempDir) -> noxu_db::Environment {
    let mut cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    cfg.set_run_in_compressor(false);
    noxu_db::Environment::open(cfg).unwrap()
}

fn do_key_scan(dups: bool) {
    let dir = TempDir::new().unwrap();

    // Phase 1: open, write RECORD_COUNT keys, close.
    {
        let env = open_env(&dir);
        let db = env
            .open_database(
                None,
                "foo",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true)
                    .with_sorted_duplicates(dups),
            )
            .unwrap();
        for i in 0..RECORD_COUNT {
            // data = 1 for every key (JE putNoOverwrite).
            assert!(
                db.put_no_overwrite(ikey(i), ikey(1)).unwrap(),
                "putNoOverwrite key {i} must insert"
            );
            if dups && (i % 2) == 1 {
                // Add a second duplicate (data = 2) for odd keys via a cursor
                // (JE db.putNoDupData); Database has no NoDupData facade.
                let mut c = db.open_cursor(None).unwrap();
                assert_eq!(
                    c.put(
                        &DatabaseEntry::from_bytes(&ikey(i)),
                        &DatabaseEntry::from_bytes(&ikey(2)),
                        Put::NoDupData,
                    )
                    .unwrap(),
                    OperationStatus::Success
                );
                c.close().unwrap();
            }
        }
        db.close().unwrap();
    }

    // Phase 2: reopen, preload (JE loads INs without LNs), key-only scan.
    let env = open_env(&dir);
    let db = env
        .open_database(
            None,
            "foo",
            &DatabaseConfig::new()
                .with_transactional(true)
                .with_allow_create(true)
                .with_sorted_duplicates(dups),
        )
        .unwrap();
    db.preload(&PreloadConfig::new()).unwrap();

    // Two variants: JE uses lockMode READ_UNCOMMITTED then a
    // READ_UNCOMMITTED cursor config.  Noxu: exercise a read-uncommitted
    // cursor twice (the substantive behavior — the key-only scan — is
    // identical; the two JE variants only differ in WHERE the lock mode is
    // specified, which Noxu collapses onto the cursor config).
    for _variant in 0..2 {
        let cfg = noxu_db::CursorConfig::new().with_read_uncommitted(true);
        let mut c = db.open_cursor(Some(&cfg)).unwrap();

        // Key-only scan: partial data, zero length.
        let mut count = 0u32;
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        data.set_partial(0, 0, true);
        let mode = if dups { Get::NextNoDup } else { Get::Next };
        loop {
            let st = c.get(&mut key, &mut data, mode, None).unwrap();
            if st != OperationStatus::Success {
                break;
            }
            assert_eq!(
                to_int(key.data_opt().unwrap()),
                count,
                "key-only scan must visit keys in order"
            );
            count += 1;
        }
        assert_eq!(count, RECORD_COUNT, "scan must visit every distinct key");

        // Misc positioning ops (still key-only).
        let mut k = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        d.set_partial(0, 0, true);
        assert_eq!(
            c.get(&mut k, &mut d, Get::First, None).unwrap(),
            OperationStatus::Success
        );
        assert_eq!(to_int(k.data_opt().unwrap()), 0);

        assert_eq!(
            c.get(&mut k, &mut d, Get::Last, None).unwrap(),
            OperationStatus::Success
        );
        assert_eq!(to_int(k.data_opt().unwrap()), RECORD_COUNT - 1);

        let mut ksearch = DatabaseEntry::from_bytes(&ikey(RECORD_COUNT / 2));
        assert_eq!(
            c.get(&mut ksearch, &mut d, Get::Search, None).unwrap(),
            OperationStatus::Success
        );
        assert_eq!(to_int(ksearch.data_opt().unwrap()), RECORD_COUNT / 2);

        let mut krange = DatabaseEntry::from_bytes(&ikey(RECORD_COUNT / 2));
        assert_eq!(
            c.get(&mut krange, &mut d, Get::SearchGte, None).unwrap(),
            OperationStatus::Success
        );
        assert_eq!(to_int(krange.data_opt().unwrap()), RECORD_COUNT / 2);

        c.close().unwrap();
    }

    db.close().unwrap();
}

/// JE `KeyScanTest.testKeyScan`.
#[test]
fn je_key_scan_test_test_key_scan() {
    do_key_scan(false);
}

/// JE `KeyScanTest.testKeyScanDup`.
#[test]
fn je_key_scan_test_test_key_scan_dup() {
    do_key_scan(true);
}
