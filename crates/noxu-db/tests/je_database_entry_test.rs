//! JE TCK port: `com.sleepycat.je.DatabaseEntryTest`.
//!
//! Behaviour-level ports.  Targets DatabaseEntry's offset / size /
//! partial accessors as they interact with `Database::put` and
//! `Cursor::get`.
//!
//! Adaptations
//!
//! - JE's `DatabaseEntry` exposes a settable byte array via `getData()`
//!   then in-place mutation; noxu's `DatabaseEntry` wraps a `bytes::Bytes`
//!   so we build the buffer up front and use `set_offset` / `set_size`
//!   in the same way JE callers do via `setOffset` / `setSize`.
//! - noxu's partial-put protocol returns
//!   `NoxuError::IllegalArgument` if the supplied data length does not
//!   match the partial length, so the partial-put test uses an
//!   explicit `Some(&[])` byte slice.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus,
};
use tempfile::TempDir;

fn open_env_db() -> (TempDir, noxu_db::Environment, noxu_db::Database) {
    let dir = TempDir::new().unwrap();
    let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_cfg).unwrap();
    let db_cfg =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    let db = env.open_database(None, "DatabaseEntryTest", &db_cfg).unwrap();
    (dir, env, db)
}

// ---------------------------------------------------------------------------
// DatabaseEntryTest.testBasic
// ---------------------------------------------------------------------------

/// Port of `DatabaseEntryTest.testBasic`.  Exercises constructors,
/// `set_data` (clears the offset), and `get_size`.
#[test]
fn database_entry_test_basic() {
    let foo = vec![1u8; 10];

    // Constructor that takes a byte array.
    let mut a = DatabaseEntry::from_bytes(&foo);
    assert_eq!(foo.len(), a.get_size());
    assert_eq!(foo.as_slice(), a.get_data().unwrap());

    // Set the data to empty (JE: setData(null)).  Noxu has no Option,
    // but set_data(&[]) gives us the same observable state.
    a.set_data(&[]);
    assert_eq!(0, a.get_size());

    // Constructor that sets the data later.
    let mut later = DatabaseEntry::new();
    assert_eq!(0, later.get_size());
    later.set_data(&foo);
    assert_eq!(foo.as_slice(), later.get_data().unwrap());

    // Set offset, then reset data and offset should be reset.
    let mut off = DatabaseEntry::from_bytes(&foo);
    off.set_offset(1);
    off.set_size(1);
    assert_eq!(1, off.get_offset());
    assert_eq!(1, off.get_size());
    off.set_data(&foo);
    assert_eq!(0, off.get_offset());
    assert_eq!(foo.len(), off.get_size());
}

// ---------------------------------------------------------------------------
// DatabaseEntryTest.testOffset
// ---------------------------------------------------------------------------

/// Port of `DatabaseEntryTest.testOffset`.  A 30-byte buffer with the
/// "interesting" 10 bytes at offset 10 round-trips through `Database::put`
/// and `Cursor::get`: the stored payload is the 10-byte window, and the
/// returned entry has offset=0, size=10.
#[test]
fn database_entry_test_offset() {
    let (_dir, env, db) = open_env_db();

    const N_BYTES: u8 = 30;
    let buf: Vec<u8> = (0..N_BYTES).collect();

    let mut original_key = DatabaseEntry::from_bytes(&buf);
    let mut original_data = DatabaseEntry::from_bytes(&buf);
    original_key.set_size(10);
    original_key.set_offset(10);
    original_data.set_size(10);
    original_data.set_offset(10);

    db.put( &original_key, &original_data).unwrap();

    let txn = env.begin_transaction(None).unwrap();
    let mut cursor = db.open_cursor_in(&txn, None).unwrap();
    let mut found_key = DatabaseEntry::new();
    let mut found_data = DatabaseEntry::new();
    let s =
        cursor.get(&mut found_key, &mut found_data, Get::First, None).unwrap();
    assert_eq!(OperationStatus::Success, s);

    // Returned entries always start at offset 0 with size = stored payload.
    assert_eq!(0, found_key.get_offset());
    assert_eq!(0, found_data.get_offset());
    assert_eq!(10, found_key.get_size());
    assert_eq!(10, found_data.get_size());

    let key_data = found_key.get_data().unwrap();
    let val_data = found_data.get_data().unwrap();
    for i in 0..10 {
        assert_eq!((i + 10) as u8, key_data[i]);
        assert_eq!((i + 10) as u8, val_data[i]);
    }

    drop(cursor);
    txn.commit().unwrap();
}
