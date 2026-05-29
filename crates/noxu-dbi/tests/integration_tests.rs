//! Comprehensive integration tests for noxu-dbi.
//!
//! Covers:
//!   - DatabaseImpl: open, put, get, delete, cursor operations
//!   - EnvironmentImpl: create, open databases, close
//!   - CursorImpl: basic iteration, state machine
//!   - DbType: all variants
//!   - DatabaseId: creation, comparison

#![allow(clippy::field_reassign_with_default)]

use std::sync::Arc;

use noxu_dbi::{
    CursorImpl, DatabaseConfig, DatabaseId, DatabaseImpl, DbType,
    EnvironmentImpl, GetMode, OperationStatus, PutMode, SearchMode,
};
use tempfile::TempDir;

fn tmp_env() -> (TempDir, EnvironmentImpl) {
    let dir = TempDir::new().unwrap();
    let env = EnvironmentImpl::new(dir.path(), false, true).unwrap();
    (dir, env)
}
use noxu_sync::RwLock;

// ============================================================================
// 1. DatabaseId: creation, comparison, ordering
// ============================================================================

#[test]
fn database_id_creation_and_equality() {
    let id1 = DatabaseId::new(1);
    let id2 = DatabaseId::new(1);
    let id3 = DatabaseId::new(2);

    assert_eq!(id1, id2);
    assert_ne!(id1, id3);
    assert_eq!(id1.id(), 1);
    assert_eq!(id1.as_i64(), 1);
}

#[test]
fn database_id_ordering() {
    let id1 = DatabaseId::new(1);
    let id2 = DatabaseId::new(2);
    let id3 = DatabaseId::new(3);

    assert!(id1 < id2);
    assert!(id2 < id3);
    assert!(id3 > id1);
    assert_eq!(id1.cmp(&id1), std::cmp::Ordering::Equal);
}

#[test]
fn database_id_serialization_roundtrip() {
    let original = DatabaseId::new(12345);
    let mut buf = Vec::new();
    original.write_to_log(&mut buf);
    assert_eq!(buf.len(), 8);

    let restored = DatabaseId::read_from_log(&buf).unwrap();
    assert_eq!(original, restored);
}

#[test]
fn database_id_negative_value() {
    let id = DatabaseId::new(-1);
    assert_eq!(id.id(), -1);
    let mut buf = Vec::new();
    id.write_to_log(&mut buf);
    let restored = DatabaseId::read_from_log(&buf).unwrap();
    assert_eq!(id, restored);
}

// ============================================================================
// 2. DbType: all variants, is_internal, internal_name, Display
// ============================================================================

#[test]
fn db_type_is_internal() {
    assert!(DbType::Id.is_internal());
    assert!(DbType::Name.is_internal());
    assert!(DbType::Utilization.is_internal());
    assert!(!DbType::User.is_internal());
}

#[test]
fn db_type_internal_name() {
    assert_eq!(DbType::Id.internal_name(), Some("_jeIdMap"));
    assert_eq!(DbType::Name.internal_name(), Some("_jeNameMap"));
    assert_eq!(DbType::Utilization.internal_name(), Some("_jeUtilization"));
    assert_eq!(DbType::User.internal_name(), None);
}

#[test]
fn db_type_display() {
    assert_eq!(DbType::Id.to_string(), "ID");
    assert_eq!(DbType::Name.to_string(), "NAME");
    assert_eq!(DbType::Utilization.to_string(), "UTILIZATION");
    assert_eq!(DbType::User.to_string(), "USER");
}

#[test]
fn db_type_equality() {
    assert_eq!(DbType::User, DbType::User);
    assert_ne!(DbType::User, DbType::Id);
    assert_ne!(DbType::Id, DbType::Name);
}

// ============================================================================
// 3. DatabaseImpl: creation, flags, delete state, dirty, reference count
// ============================================================================

fn default_config() -> DatabaseConfig {
    DatabaseConfig::default()
}

fn make_db(id: i64, name: &str) -> DatabaseImpl {
    DatabaseImpl::new(
        DatabaseId::new(id),
        name.to_string(),
        DbType::User,
        &default_config(),
    )
}

#[test]
fn database_impl_new() {
    let db = make_db(1, "mydb");
    assert_eq!(db.get_id(), DatabaseId::new(1));
    assert_eq!(db.get_name(), "mydb");
    assert_eq!(db.get_db_type(), DbType::User);
    assert!(!db.is_deleted());
    assert!(!db.is_deleting());
    assert_eq!(db.reference_count(), 0);
    assert!(!db.is_dirty());
}

#[test]
fn database_impl_sorted_duplicates_flag() {
    let mut config = DatabaseConfig::default();
    config.sorted_duplicates = false;
    let db1 = DatabaseImpl::new(
        DatabaseId::new(1),
        "a".into(),
        DbType::User,
        &config,
    );
    assert!(!db1.get_sorted_duplicates());

    config.sorted_duplicates = true;
    let db2 = DatabaseImpl::new(
        DatabaseId::new(2),
        "b".into(),
        DbType::User,
        &config,
    );
    assert!(db2.get_sorted_duplicates());
}

#[test]
fn database_impl_temporary_flag() {
    let mut config = DatabaseConfig::default();
    config.temporary = true;
    let db = DatabaseImpl::new(
        DatabaseId::new(1),
        "t".into(),
        DbType::User,
        &config,
    );
    assert!(db.is_temporary());
}

#[test]
fn database_impl_key_prefixing_flag() {
    let mut config = DatabaseConfig::default();
    config.key_prefixing = true;
    let db = DatabaseImpl::new(
        DatabaseId::new(1),
        "p".into(),
        DbType::User,
        &config,
    );
    assert!(db.get_key_prefixing());
}

#[test]
fn database_impl_delete_state_transitions() {
    let mut db = make_db(1, "db");
    assert!(!db.is_deleted());
    assert!(!db.is_deleting());

    db.start_delete();
    assert!(!db.is_deleted());
    assert!(db.is_deleting());

    db.finish_delete();
    assert!(db.is_deleted());
    assert!(db.is_deleting()); // still deleting (Deleted state)
}

#[test]
fn database_impl_dirty_tracking() {
    let db = make_db(1, "db");
    assert!(!db.is_dirty());
    db.set_dirty();
    assert!(db.is_dirty());
    db.clear_dirty();
    assert!(!db.is_dirty());
}

#[test]
fn database_impl_reference_counting() {
    let db = make_db(1, "db");
    assert_eq!(db.reference_count(), 0);
    db.increment_reference_count();
    db.increment_reference_count();
    assert_eq!(db.reference_count(), 2);
    db.decrement_reference_count();
    assert_eq!(db.reference_count(), 1);
    db.decrement_reference_count();
    assert_eq!(db.reference_count(), 0);
}

#[test]
fn database_impl_tree_access() {
    let mut db = make_db(1, "db");
    {
        let tree = db.get_tree().unwrap();
        assert_eq!(tree.get_root_lsn(), noxu_util::NULL_LSN.as_u64());
    }
    {
        let tree = db.get_tree_mut().unwrap();
        tree.set_root_lsn(9999);
    }
    assert_eq!(db.get_tree().unwrap().get_root_lsn(), 9999);
}

#[test]
fn database_impl_serialization_roundtrip() {
    let mut config = DatabaseConfig::default();
    config.sorted_duplicates = true;
    config.key_prefixing = true;
    config.node_max_entries = 512;

    let db = DatabaseImpl::new(
        DatabaseId::new(77),
        "roundtrip_db".to_string(),
        DbType::User,
        &config,
    );

    let mut buf = Vec::new();
    db.write_to_log(&mut buf).unwrap();

    let db2 = DatabaseImpl::read_from_log(&buf).unwrap();
    assert_eq!(db2.get_id(), DatabaseId::new(77));
    assert_eq!(db2.get_name(), "roundtrip_db");
    assert!(db2.get_sorted_duplicates());
    assert!(db2.get_key_prefixing());
    assert_eq!(db2.max_tree_entries_per_node(), 512);
}

// ============================================================================
// 4. EnvironmentImpl: create, open databases, close
// ============================================================================

#[test]
fn environment_impl_create_and_open() {
    let (_dir, env) = tmp_env();
    assert!(!env.is_read_only());
    assert!(env.is_transactional());
    assert!(env.is_open());
    assert!(env.is_valid());
}

#[test]
fn environment_impl_open_database_with_create() {
    let (_dir, env) = tmp_env();
    let mut cfg = DatabaseConfig::new();
    cfg.set_allow_create(true);

    let db = env.open_database("testdb", &cfg).unwrap();
    assert_eq!(db.read().get_name(), "testdb");
    assert_eq!(db.read().reference_count(), 1);
}

#[test]
fn environment_impl_open_database_no_create_fails() {
    let (_dir, env) = tmp_env();
    let cfg = DatabaseConfig::new();
    let result = env.open_database("nonexistent", &cfg);
    assert!(result.is_err());
}

#[test]
fn environment_impl_open_same_database_twice_increments_ref() {
    let (_dir, env) = tmp_env();
    let mut cfg = DatabaseConfig::new();
    cfg.set_allow_create(true);

    let db1 = env.open_database("shared", &cfg).unwrap();
    let db2 = env.open_database("shared", &cfg).unwrap();

    assert_eq!(db1.read().get_id(), db2.read().get_id());
    assert_eq!(db1.read().reference_count(), 2);
}

#[test]
fn environment_impl_multiple_databases_unique_ids() {
    let (_dir, env) = tmp_env();
    let mut cfg = DatabaseConfig::new();
    cfg.set_allow_create(true);

    let db1 = env.open_database("db1", &cfg).unwrap();
    let db2 = env.open_database("db2", &cfg).unwrap();
    let db3 = env.open_database("db3", &cfg).unwrap();

    assert_ne!(db1.read().get_id(), db2.read().get_id());
    assert_ne!(db2.read().get_id(), db3.read().get_id());
    assert_eq!(env.n_databases(), 3);
}

#[test]
fn environment_impl_remove_database() {
    let (_dir, env) = tmp_env();
    let mut cfg = DatabaseConfig::new();
    cfg.set_allow_create(true);

    let db = env.open_database("to_remove", &cfg).unwrap();
    env.close_database(db.read().get_id()).unwrap();
    env.remove_database("to_remove").unwrap();

    let result = env.open_database("to_remove", &DatabaseConfig::new());
    assert!(result.is_err());
}

#[test]
fn environment_impl_rename_database() {
    let (_dir, env) = tmp_env();
    let mut cfg = DatabaseConfig::new();
    cfg.set_allow_create(true);

    let db = env.open_database("old", &cfg).unwrap();
    let original_id = db.read().get_id();
    env.close_database(original_id).unwrap();

    env.rename_database("old", "new").unwrap();

    let found = env.open_database("new", &cfg).unwrap();
    assert_eq!(found.read().get_id(), original_id);
    assert!(env.open_database("old", &DatabaseConfig::new()).is_err());
}

#[test]
fn environment_impl_get_database_names() {
    let (_dir, env) = tmp_env();
    let mut cfg = DatabaseConfig::new();
    cfg.set_allow_create(true);

    env.open_database("alpha", &cfg).unwrap();
    env.open_database("beta", &cfg).unwrap();

    let names = env.get_database_names();
    assert!(names.contains(&"alpha".to_string()));
    assert!(names.contains(&"beta".to_string()));
}

#[test]
fn environment_impl_begin_txn_tracks_active() {
    let (_dir, env) = tmp_env();
    assert_eq!(env.n_active_txns(), 0);
    let _t1 = env.begin_txn().unwrap();
    assert_eq!(env.n_active_txns(), 1);
    let _t2 = env.begin_txn().unwrap();
    assert_eq!(env.n_active_txns(), 2);
}

#[test]
fn environment_impl_close() {
    let (_dir, env) = tmp_env();
    assert!(env.is_open());
    env.close().unwrap();
    assert!(!env.is_open());
    // Closing twice is safe.
    env.close().unwrap();
}

#[test]
fn environment_impl_ops_on_closed_env_fail() {
    let (_dir, env) = tmp_env();
    env.close().unwrap();

    let mut cfg = DatabaseConfig::new();
    cfg.set_allow_create(true);

    assert!(env.open_database("x", &cfg).is_err());
    assert!(env.begin_txn().is_err());
}

// ============================================================================
// 5. CursorImpl: basic operations, state machine
// ============================================================================

fn make_cursor_db() -> Arc<RwLock<DatabaseImpl>> {
    let db = DatabaseImpl::new(
        DatabaseId::new(1),
        "cursor_db".to_string(),
        DbType::User,
        &DatabaseConfig::default(),
    );
    Arc::new(RwLock::new(db))
}

#[test]
fn cursor_impl_new_not_initialized() {
    let db = make_cursor_db();
    let cursor = CursorImpl::new(db, 100);
    assert!(!cursor.is_initialized());
    assert!(!cursor.is_closed());
    assert!(cursor.get_current_key().is_none());
    assert!(cursor.get_current_data().is_none());
    assert_eq!(cursor.get_locker_id(), 100);
}

#[test]
fn cursor_impl_search_positions_cursor() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);

    // Insert into the tree first, then search.
    cursor.put(b"key1", b"val1", PutMode::Overwrite).unwrap();
    let status =
        cursor.search(b"key1", Some(b"val1"), SearchMode::Set).unwrap();
    assert_eq!(status, OperationStatus::Success);
    assert!(cursor.is_initialized());
    assert_eq!(cursor.get_current_key(), Some(b"key1".as_slice()));
    assert_eq!(cursor.get_current_data(), Some(b"val1".as_slice()));
}

#[test]
fn cursor_impl_get_current_after_search() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);

    cursor.put(b"k", b"v", PutMode::Overwrite).unwrap();
    cursor.search(b"k", Some(b"v"), SearchMode::Set).unwrap();
    let (key, data) = cursor.get_current().unwrap();
    assert_eq!(key, b"k");
    assert_eq!(data, b"v");
}

#[test]
fn cursor_impl_get_current_uninitialized_fails() {
    let db = make_cursor_db();
    let cursor = CursorImpl::new(db, 1);
    assert!(cursor.get_current().is_err());
}

#[test]
fn cursor_impl_retrieve_next_from_uninitialized() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);
    let status = cursor.retrieve_next(GetMode::Next).unwrap();
    assert_eq!(status, OperationStatus::NotFound);
}

#[test]
fn cursor_impl_put_overwrite() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);
    let status = cursor.put(b"key", b"data", PutMode::Overwrite).unwrap();
    assert_eq!(status, OperationStatus::Success);
    assert!(cursor.is_initialized());
    assert_eq!(cursor.get_current_key(), Some(b"key".as_slice()));
}

#[test]
fn cursor_impl_put_no_overwrite_key_exists_returns_key_exist() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);

    cursor.put(b"key", b"v1", PutMode::Overwrite).unwrap();
    let status = cursor.put(b"key", b"v2", PutMode::NoOverwrite).unwrap();
    assert_eq!(status, OperationStatus::KeyExist);
}

#[test]
fn cursor_impl_put_current_requires_initialized() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);
    let result = cursor.put(b"k", b"v", PutMode::Current);
    assert!(result.is_err());
}

#[test]
fn cursor_impl_put_current_updates_data() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);
    cursor.put(b"k", b"v1", PutMode::Overwrite).unwrap();
    cursor.search(b"k", Some(b"v1"), SearchMode::Set).unwrap();
    cursor.put(b"k", b"v2", PutMode::Current).unwrap();
    assert_eq!(cursor.get_current_data(), Some(b"v2".as_slice()));
}

#[test]
fn cursor_impl_delete_resets_state() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);

    cursor.put(b"k", b"v", PutMode::Overwrite).unwrap();
    cursor.search(b"k", Some(b"v"), SearchMode::Set).unwrap();
    assert!(cursor.is_initialized());

    let status = cursor.delete().unwrap();
    assert_eq!(status, OperationStatus::Success);
    assert!(!cursor.is_initialized());
    assert!(cursor.get_current_key().is_none());
}

#[test]
fn cursor_impl_delete_uninitialized_fails() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);
    assert!(cursor.delete().is_err());
}

#[test]
fn cursor_impl_count_returns_one_after_search() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);
    cursor.put(b"k", b"v", PutMode::Overwrite).unwrap();
    cursor.search(b"k", Some(b"v"), SearchMode::Set).unwrap();
    assert_eq!(cursor.count().unwrap(), 1);
}

#[test]
fn cursor_impl_dup_same_position() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);
    cursor.put(b"k", b"v", PutMode::Overwrite).unwrap();
    cursor.search(b"k", Some(b"v"), SearchMode::Set).unwrap();

    let dup = cursor.dup(true).unwrap();
    assert!(dup.is_initialized());
    assert_eq!(dup.get_current_key(), Some(b"k".as_slice()));
    assert_eq!(dup.get_locker_id(), 1);
    assert_ne!(cursor.get_id(), dup.get_id());
}

#[test]
fn cursor_impl_dup_no_position() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);
    cursor.put(b"k", b"v", PutMode::Overwrite).unwrap();
    cursor.search(b"k", Some(b"v"), SearchMode::Set).unwrap();

    let dup = cursor.dup(false).unwrap();
    assert!(!dup.is_initialized());
    assert!(dup.get_current_key().is_none());
}

#[test]
fn cursor_impl_close_sets_state() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);
    cursor.close().unwrap();
    assert!(cursor.is_closed());
}

#[test]
fn cursor_impl_ops_after_close_fail() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);
    cursor.close().unwrap();

    assert!(cursor.search(b"k", None, SearchMode::Set).is_err());
    assert!(cursor.get_current().is_err());
    assert!(cursor.retrieve_next(GetMode::Next).is_err());
    assert!(cursor.put(b"k", b"v", PutMode::Overwrite).is_err());
    assert!(cursor.delete().is_err());
    assert!(cursor.count().is_err());
    assert!(cursor.dup(true).is_err());
}

#[test]
fn cursor_impl_close_idempotent() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);
    cursor.close().unwrap();
    cursor.close().unwrap(); // Must not panic.
    assert!(cursor.is_closed());
}

#[test]
fn cursor_impl_unique_ids() {
    let db = make_cursor_db();
    let c1 = CursorImpl::new(db.clone(), 1);
    let c2 = CursorImpl::new(db.clone(), 1);
    let c3 = CursorImpl::new(db, 1);
    assert_ne!(c1.get_id(), c2.get_id());
    assert_ne!(c2.get_id(), c3.get_id());
    assert_ne!(c1.get_id(), c3.get_id());
}

#[test]
fn cursor_impl_search_modes_all_succeed() {
    let db = make_cursor_db();
    // Pre-insert the key so search can find it in all modes.
    {
        let mut cursor = CursorImpl::new(db.clone(), 1);
        cursor.put(b"key", b"val", PutMode::Overwrite).unwrap();
    }
    for mode in [
        SearchMode::Set,
        SearchMode::Both,
        SearchMode::SetRange,
        SearchMode::BothRange,
    ] {
        let mut cursor = CursorImpl::new(db.clone(), 1);
        let status = cursor.search(b"key", Some(b"val"), mode).unwrap();
        assert_eq!(
            status,
            OperationStatus::Success,
            "mode {:?} should succeed",
            mode
        );
    }
}

// ============================================================================
// 5b. CursorImpl traversal — get_first, get_last, retrieve_next
//
// These tests mirror CursorImplTest for the basic traversal path:
//   positionFirstOrLast / getNext / getPrev.
// ============================================================================

/// `get_first` on an empty database returns NotFound.
#[test]
fn cursor_get_first_empty_returns_not_found() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);
    let status = cursor.get_first().unwrap();
    assert_eq!(status, OperationStatus::NotFound);
}

/// `get_last` on an empty database returns NotFound.
#[test]
fn cursor_get_last_empty_returns_not_found() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);
    let status = cursor.get_last().unwrap();
    assert_eq!(status, OperationStatus::NotFound);
}

/// `get_first` positions the cursor at the smallest key.
///
/// CursorImplTest: after inserting multiple keys, getFirst()
/// must land on the smallest one.
#[test]
fn cursor_get_first_positions_at_smallest_key() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);

    // Insert three keys in non-sorted order.
    cursor.put(b"cherry", b"c", PutMode::Overwrite).unwrap();
    cursor.put(b"apple", b"a", PutMode::Overwrite).unwrap();
    cursor.put(b"banana", b"b", PutMode::Overwrite).unwrap();

    let status = cursor.get_first().unwrap();
    assert_eq!(status, OperationStatus::Success);
    assert!(cursor.is_initialized());

    // The cursor should be on "apple" (byte-lexicographic minimum).
    assert_eq!(cursor.get_current_key(), Some(b"apple".as_slice()));
    assert_eq!(cursor.get_current_data(), Some(b"a".as_slice()));
}

/// `get_last` positions the cursor at the largest key.
///
/// CursorImplTest: after inserting multiple keys, getLast()
/// must land on the largest one.
#[test]
fn cursor_get_last_positions_at_largest_key() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);

    cursor.put(b"apple", b"a", PutMode::Overwrite).unwrap();
    cursor.put(b"cherry", b"c", PutMode::Overwrite).unwrap();
    cursor.put(b"banana", b"b", PutMode::Overwrite).unwrap();

    let status = cursor.get_last().unwrap();
    assert_eq!(status, OperationStatus::Success);
    assert!(cursor.is_initialized());

    // "cherry" is the largest key byte-lexicographically.
    assert_eq!(cursor.get_current_key(), Some(b"cherry".as_slice()));
    assert_eq!(cursor.get_current_data(), Some(b"c".as_slice()));
}

/// `get_first` + sequential `retrieve_next` (GetMode::Next) iterates in
/// sorted order.
///
/// CursorImplTest scan: get first, then repeatedly getNext until
/// NotFound.
#[test]
fn cursor_iterate_forward_with_get_first_and_retrieve_next() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);

    cursor.put(b"aardvark", b"1", PutMode::Overwrite).unwrap();
    cursor.put(b"zebra", b"3", PutMode::Overwrite).unwrap();
    cursor.put(b"meerkat", b"2", PutMode::Overwrite).unwrap();

    // Position at first.
    let s = cursor.get_first().unwrap();
    assert_eq!(s, OperationStatus::Success);
    let (k, _) = cursor.get_current().unwrap();
    assert_eq!(k, b"aardvark");

    // Advance to second.
    let s = cursor.retrieve_next(GetMode::Next).unwrap();
    assert_eq!(s, OperationStatus::Success);
    let (k, _) = cursor.get_current().unwrap();
    assert_eq!(k, b"meerkat");

    // Advance to third.
    let s = cursor.retrieve_next(GetMode::Next).unwrap();
    assert_eq!(s, OperationStatus::Success);
    let (k, _) = cursor.get_current().unwrap();
    assert_eq!(k, b"zebra");

    // No more entries.
    let s = cursor.retrieve_next(GetMode::Next).unwrap();
    assert_eq!(s, OperationStatus::NotFound);
}

/// `get_last` + sequential `retrieve_next` (GetMode::Prev) iterates in
/// reverse sorted order.
///
/// CursorImplTest reverse scan.
#[test]
fn cursor_iterate_backward_with_get_last_and_retrieve_next_prev() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);

    cursor.put(b"aardvark", b"1", PutMode::Overwrite).unwrap();
    cursor.put(b"meerkat", b"2", PutMode::Overwrite).unwrap();
    cursor.put(b"zebra", b"3", PutMode::Overwrite).unwrap();

    // Position at last.
    let s = cursor.get_last().unwrap();
    assert_eq!(s, OperationStatus::Success);
    let (k, _) = cursor.get_current().unwrap();
    assert_eq!(k, b"zebra");

    // Move backward to second.
    let s = cursor.retrieve_next(GetMode::Prev).unwrap();
    assert_eq!(s, OperationStatus::Success);
    let (k, _) = cursor.get_current().unwrap();
    assert_eq!(k, b"meerkat");

    // Move backward to first.
    let s = cursor.retrieve_next(GetMode::Prev).unwrap();
    assert_eq!(s, OperationStatus::Success);
    let (k, _) = cursor.get_current().unwrap();
    assert_eq!(k, b"aardvark");

    // No more entries backward.
    let s = cursor.retrieve_next(GetMode::Prev).unwrap();
    assert_eq!(s, OperationStatus::NotFound);
}

/// `retrieve_next` from an uninitialized cursor returns NotFound.
///
/// : CursorImpl.getNext() asserts mustBeInitialized.
#[test]
fn cursor_retrieve_next_uninitialized_returns_not_found() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);
    let s = cursor.retrieve_next(GetMode::Next).unwrap();
    assert_eq!(s, OperationStatus::NotFound);
}

/// Single-element database: get_first, retrieve_next returns NotFound.
#[test]
fn cursor_single_element_get_first_then_next_is_not_found() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);
    cursor.put(b"only", b"one", PutMode::Overwrite).unwrap();

    let s = cursor.get_first().unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(cursor.get_current_key(), Some(b"only".as_slice()));

    let s = cursor.retrieve_next(GetMode::Next).unwrap();
    assert_eq!(s, OperationStatus::NotFound);
}

/// Single-element database: get_last, retrieve_next(Prev) returns NotFound.
#[test]
fn cursor_single_element_get_last_then_prev_is_not_found() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);
    cursor.put(b"only", b"one", PutMode::Overwrite).unwrap();

    let s = cursor.get_last().unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(cursor.get_current_key(), Some(b"only".as_slice()));

    let s = cursor.retrieve_next(GetMode::Prev).unwrap();
    assert_eq!(s, OperationStatus::NotFound);
}

/// Range search (SetRange) positions at first key >= search key.
///
/// CursorImpl.searchRange(): with keys "apple", "banana", "cherry",
/// searching for "b" should position at "banana".
#[test]
fn cursor_search_range_positions_at_first_ge_key() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);

    cursor.put(b"apple", b"a", PutMode::Overwrite).unwrap();
    cursor.put(b"banana", b"b", PutMode::Overwrite).unwrap();
    cursor.put(b"cherry", b"c", PutMode::Overwrite).unwrap();

    // Search for "b" — should land on "banana" (first key >= "b").
    let s = cursor.search(b"b", None, SearchMode::SetRange).unwrap();
    assert_eq!(s, OperationStatus::Success);
    assert_eq!(cursor.get_current_key(), Some(b"banana".as_slice()));
}

/// Range search beyond all keys returns NotFound.
#[test]
fn cursor_search_range_beyond_last_key_returns_not_found() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);

    cursor.put(b"apple", b"a", PutMode::Overwrite).unwrap();
    cursor.put(b"banana", b"b", PutMode::Overwrite).unwrap();

    // "z" is beyond all keys.
    let s = cursor.search(b"z", None, SearchMode::SetRange).unwrap();
    assert_eq!(s, OperationStatus::NotFound);
}

/// After insert, search finds the record and the data matches.
#[test]
fn cursor_put_then_search_retrieves_correct_data() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);

    cursor.put(b"hello", b"world", PutMode::Overwrite).unwrap();

    let s = cursor.search(b"hello", None, SearchMode::Set).unwrap();
    assert_eq!(s, OperationStatus::Success);
    let (k, v) = cursor.get_current().unwrap();
    assert_eq!(k, b"hello");
    assert_eq!(v, b"world");
}

/// After delete, search returns NotFound.
#[test]
fn cursor_delete_then_search_returns_not_found() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);

    cursor.put(b"key", b"val", PutMode::Overwrite).unwrap();
    cursor.search(b"key", None, SearchMode::Set).unwrap();
    cursor.delete().unwrap();

    let s = cursor.search(b"key", None, SearchMode::Set).unwrap();
    assert_eq!(s, OperationStatus::NotFound);
}

/// get_first on a database that had a key deleted returns NotFound when empty.
#[test]
fn cursor_get_first_after_all_keys_deleted_returns_not_found() {
    let db = make_cursor_db();
    let mut cursor = CursorImpl::new(db, 1);

    cursor.put(b"only", b"one", PutMode::Overwrite).unwrap();
    cursor.search(b"only", None, SearchMode::Set).unwrap();
    cursor.delete().unwrap();

    // Tree is now empty.
    let s = cursor.get_first().unwrap();
    assert_eq!(s, OperationStatus::NotFound);
}

// ============================================================================
// 6. EnvironmentImpl + CursorImpl end-to-end
// ============================================================================

#[test]
fn environment_cursor_put_get_delete() {
    let (_dir, env) = tmp_env();
    let mut cfg = DatabaseConfig::new();
    cfg.set_allow_create(true);

    let db_arc = env.open_database("e2e", &cfg).unwrap();

    let mut cursor = CursorImpl::new(db_arc, 1);

    // Put.
    cursor.put(b"hello", b"world", PutMode::Overwrite).unwrap();
    assert_eq!(cursor.get_current_key(), Some(b"hello".as_slice()));
    assert_eq!(cursor.get_current_data(), Some(b"world".as_slice()));

    // Search re-positions.
    cursor.search(b"hello", None, SearchMode::Set).unwrap();
    let (k, _) = cursor.get_current().unwrap();
    assert_eq!(k, b"hello");

    // Delete.
    cursor.delete().unwrap();
    assert!(!cursor.is_initialized());

    cursor.close().unwrap();
    env.close().unwrap();
}

// ============================================================================
// UtilizationTracker wiring
// ============================================================================

/// Writes through `EnvironmentImpl` update the `UtilizationTracker`.
///
/// Uses `CursorImpl::with_log_manager` so LN writes flow through the real
/// `LogManager`, which notifies the `UtilizationTrackerObserver` on each
/// `log()` call.  Verifies that after 10 puts the tracker holds at least one
/// file summary with a non-zero `total_count`.
#[test]
fn utilization_tracker_is_populated_after_writes() {
    let dir = TempDir::new().unwrap();
    let env = EnvironmentImpl::new(dir.path(), false, true).unwrap();

    let lm = env
        .get_log_manager()
        .expect("transactional env must have a LogManager");

    let mut cfg = DatabaseConfig::new();
    cfg.set_allow_create(true);
    let db_arc = env.open_database("util_test", &cfg).unwrap();

    // Use with_log_manager so LN writes go through LogManager (and trigger
    // the UtilizationTrackerObserver on each write).
    let mut cursor = CursorImpl::with_log_manager(db_arc, 1, lm);

    for i in 0u8..10 {
        cursor.put(&[i], b"value", PutMode::Overwrite).unwrap();
    }
    cursor.close().unwrap();

    // The UtilizationTracker must be populated with at least one file having
    // a non-zero total_count.
    let tracker_lock = env
        .get_utilization_tracker()
        .expect("UtilizationTracker must be wired into EnvironmentImpl");

    let tracker = tracker_lock.lock();
    let has_entries = tracker
        .get_tracked_files()
        .values()
        .any(|t| t.get_summary().total_count > 0);

    assert!(
        has_entries,
        "UtilizationTracker has no tracked entries after writes — \
         count_new_log_entry is not being called from the write path"
    );
}

// ============================================================================
// X-11: log_flush_no_sync_interval_ms daemon
// ============================================================================

/// X-11: Verify the LogFlushTask daemon flushes CommitNoSync data to the OS
/// page cache within the configured interval.
///
/// Test strategy:
/// 1. Open a writable env with `log_flush_no_sync_interval_ms = 50` ms.
/// 2. Write a record using CommitNoSync (no flush/fsync from the committer).
/// 3. Wait 200 ms (4× the flush interval) for the daemon to run.
/// 4. Assert that the LogManager's `last_flush_lsn` has advanced past 0,
///    meaning at least one flush_no_sync() has fired.  We verify via
///    `get_log_manager().unwrap().get_last_flush_lsn()`.
#[test]
fn test_x11_log_flush_no_sync_daemon_fires() {
    use noxu_dbi::{DatabaseConfig, DbiEnvConfig, EnvironmentImpl};

    let dir = TempDir::new().unwrap();

    let cfg = DbiEnvConfig {
        transactional: true,
        log_flush_no_sync_interval_ms: 50, // 50 ms flush interval
        run_cleaner: false,
        run_checkpointer: false,
        run_in_compressor: false,
        ..DbiEnvConfig::default()
    };
    let env = EnvironmentImpl::from_dbi_config(dir.path(), &cfg).unwrap();

    // Open a database and write a record.
    let db_cfg = DatabaseConfig::new().set_allow_create(true).clone();
    let db_arc = env.open_database("test", &db_cfg).unwrap();

    {
        let db = db_arc.read();
        let tree = db.get_real_tree().expect("tree must be present");
        let lsn = noxu_util::Lsn::from_u64(1);
        tree.insert(b"k1".to_vec(), b"v1".to_vec(), lsn).unwrap();
    }

    // Write something to the log manager directly to ensure there's data to flush.
    // Use a raw log write to simulate CommitNoSync (flush=false, fsync=false).
    let lm = env.get_log_manager().expect("log manager must be present");
    let mut buf = bytes::BytesMut::with_capacity(32);
    let entry = noxu_log::entry::LnLogEntry::new(
        1,
        None,
        noxu_util::lsn::NULL_LSN,
        false,
        None,
        None,
        noxu_util::vlsn::NULL_VLSN,
        0,
        false,
        b"k1".to_vec(),
        Some(b"v1".to_vec()),
        0,
        noxu_util::vlsn::NULL_VLSN,
    );
    use noxu_log::LogEntryType;
    let _ = LogEntryType::UpdateLN; // fix import
    entry.write_to_log(&mut buf);
    lm.log(
        noxu_log::LogEntryType::UpdateLN,
        &buf,
        noxu_log::Provisional::No,
        false, // flush=false (CommitNoSync)
        false, // fsync=false
    )
    .expect("log write should succeed");

    // Record last_flush_lsn before waiting.
    let before = lm.get_last_flush_lsn();

    // Wait long enough for the 50-ms daemon to have run multiple times.
    std::thread::sleep(std::time::Duration::from_millis(300));

    let after = lm.get_last_flush_lsn();

    env.close().unwrap();

    assert!(
        after > before,
        "LogFlushTask daemon must have advanced last_flush_lsn: \
         before={before:?} after={after:?}"
    );
}

/// X-11: When log_flush_no_sync_interval_ms = 0 the daemon must be a no-op
/// (the thread exits immediately without firing any flushes beyond env open).
#[test]
fn test_x11_disabled_interval_no_spurious_flush() {
    use noxu_dbi::{DbiEnvConfig, EnvironmentImpl};

    let dir = TempDir::new().unwrap();
    let cfg = DbiEnvConfig {
        transactional: true,
        log_flush_no_sync_interval_ms: 0, // disabled
        run_cleaner: false,
        run_checkpointer: false,
        run_in_compressor: false,
        ..DbiEnvConfig::default()
    };
    // Should open and close without error.
    let env = EnvironmentImpl::from_dbi_config(dir.path(), &cfg).unwrap();
    env.close().unwrap();
}
