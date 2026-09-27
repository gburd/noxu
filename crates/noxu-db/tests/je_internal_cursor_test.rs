//! JE `InternalCursorTest` port — cursor put/positioning across BIN
//! boundaries.  Faithful port of
//! `test/com/sleepycat/je/test/InternalCursorTest.java`.
//!
//! JE citation:
//!   - `InternalCursorTest.testAddCursorFix` — regression for a cursor-add
//!     bug: after inserting enough records to create 2 BINs, repositioning
//!     the cursor to the first BIN (getFirst / getSearchKey) and then putting
//!     into the second BIN must not corrupt the tree; a full traversal must
//!     return every record in order.
//!
//! Intentional deviation (documented):
//!   - JE uses `DbInternal.makeCursor` to obtain a NON-sticky internal cursor
//!     (the object the original bug lived on).  Noxu does not expose a
//!     non-sticky-cursor constructor on its public API (JE-internal
//!     `DbInternal`); we exercise the same record-integrity behavior with a
//!     regular public cursor.  The substantive assertion — inserting across
//!     BIN boundaries after repositioning keeps all records reachable and
//!     ordered — is preserved.
//!   - JE keys via `IntegerBinding.intToEntry`; we use fixed-width ascending
//!     string keys so the tree splits deterministically at a small NODE_MAX.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus,
    Put, StatsConfig,
};
use tempfile::TempDir;

const NODE_MAX: u32 = 6; // JE SecondaryTest/InternalCursorTest use NODE_MAX=6.
const N_KEYS: u32 = 200;

fn open_env(dir: &TempDir) -> noxu_db::Environment {
    let mut cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    cfg.set_run_evictor(false);
    cfg.set_run_in_compressor(false);
    cfg.set_node_max_entries(NODE_MAX);
    noxu_db::Environment::open(cfg).unwrap()
}

fn ikey(i: u32) -> String {
    format!("{i:08}")
}

/// JE `InternalCursorTest.testAddCursorFix`.
#[test]
fn je_internal_cursor_test_test_add_cursor_fix() {
    let dir = TempDir::new().unwrap();
    let env = open_env(&dir);
    let db = env
        .open_database(
            None,
            "foo",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .unwrap();

    let data = DatabaseEntry::from_bytes(b"123");

    // Add records 1..=200 to create multiple BINs.
    let mut c = db.open_cursor(None).unwrap();
    for i in 1..=N_KEYS {
        let k = DatabaseEntry::from_bytes(ikey(i).as_bytes());
        assert_eq!(
            c.put(&k, &data, Put::Overwrite).unwrap(),
            OperationStatus::Success
        );
    }

    // Confirm the setup actually spans multiple BINs.
    let bins = db
        .stats(Some(&StatsConfig::new().with_fast(false)))
        .unwrap()
        .btree
        .bottom_internal_node_count;
    assert!(
        bins >= 2,
        "test setup: NODE_MAX={NODE_MAX} with {N_KEYS} keys should span \
         >= 2 BINs, got {bins}"
    );

    // Move to first BIN (getFirst).
    let mut k = DatabaseEntry::new();
    let mut d = DatabaseEntry::new();
    assert_eq!(
        c.get(&mut k, &mut d, Get::First, None).unwrap(),
        OperationStatus::Success
    );

    // Put into the second BIN (last key, overwrite).
    let k200 = DatabaseEntry::from_bytes(ikey(N_KEYS).as_bytes());
    assert_eq!(
        c.put(&k200, &data, Put::Overwrite).unwrap(),
        OperationStatus::Success
    );

    // Search in the first BIN (getSearchKey key=1).
    let mut k1 = DatabaseEntry::from_bytes(ikey(1).as_bytes());
    let mut dsearch = DatabaseEntry::new();
    assert_eq!(
        c.get(&mut k1, &mut dsearch, Get::Search, None).unwrap(),
        OperationStatus::Success
    );

    // Put in the second BIN again.
    assert_eq!(
        c.put(&k200, &data, Put::Overwrite).unwrap(),
        OperationStatus::Success
    );

    // Traverse ALL records: getFirst then getNext must visit 1..=200 in order.
    let mut kt = DatabaseEntry::new();
    let mut dt = DatabaseEntry::new();
    assert_eq!(
        c.get(&mut kt, &mut dt, Get::First, None).unwrap(),
        OperationStatus::Success
    );
    for i in 1..=N_KEYS {
        assert_eq!(
            kt.data_opt().unwrap(),
            ikey(i).as_bytes(),
            "record {i} must be reachable in order"
        );
        let st = c.get(&mut kt, &mut dt, Get::Next, None).unwrap();
        if i == N_KEYS {
            assert_eq!(st, OperationStatus::NotFound, "past last record");
        } else {
            assert_eq!(st, OperationStatus::Success, "next of {i}");
        }
    }

    // Put in the first BIN (key=1, overwrite) — must still succeed.
    let k1b = DatabaseEntry::from_bytes(ikey(1).as_bytes());
    let mut d1b = DatabaseEntry::new();
    assert_eq!(
        c.get(&mut k1b.clone(), &mut d1b, Get::Search, None).unwrap(),
        OperationStatus::Success
    );
    assert_eq!(
        c.put(&k1b, &data, Put::Overwrite).unwrap(),
        OperationStatus::Success
    );

    c.close().unwrap();
}
