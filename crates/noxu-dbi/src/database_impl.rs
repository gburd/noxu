//! Internal database implementation.
//!

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use noxu_tree::{KeyComparatorFn, Tree};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

use crate::dup_key_data;
use crate::throughput_stats::ThroughputStats;

use crate::{DatabaseConfig, DatabaseId, DbType};

/// Deletion processing states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DeleteState {
    NotDeleted,
    DeletedCleanupInListHarvest,
    DeletedCleanupLogHarvest,
    Deleted,
}

/// Flag bits for persistent database properties.
const DUPS_ENABLED: u8 = 0x01;
const TEMPORARY_BIT: u8 = 0x02;
const IS_REPLICATED_BIT: u8 = 0x04;
const NOT_REPLICATED_BIT: u8 = 0x08;
const PREFIXING_ENABLED: u8 = 0x10;

/// The underlying object for a given database.
///
/// 
pub struct DatabaseImpl {
    /// Unique database ID.
    id: DatabaseId,
    /// Database name (user databases) or internal type name.
    name: String,
    /// Database type.
    db_type: DbType,
    /// Persistent flag bits.
    flags: u8,
    /// Delete processing state.
    delete_state: DeleteState,
    /// Whether this database is dirty (needs to be written to log).
    dirty: AtomicBool,
    /// Maximum number of entries in a B-tree node.
    max_tree_entries_per_node: i32,
    /// Number of open database handles (user handles referencing this db).
    reference_count: AtomicI64,
    /// Persistent B-tree root metadata (root LSN, serialized with the database
    /// record in the ID database).  Populated from the log during recovery.
    tree: Option<DatabaseTree>,
    /// The in-memory B+tree backing cursor traversal (search, insert, delete).
    ///
    /// `None` only for read-only or freshly created databases before the first
    /// write; otherwise always `Some`.  Populated either from recovery via
    /// `set_recovered_tree()` or lazily on first write.
    real_tree: Option<Tree>,
    /// Whether writes are deferred (not WAL-logged immediately).
    ///
    /// 
    /// When true, `log_ln_write()` skips WAL logging and returns NULL_LSN;
    /// data is flushed to disk only at eviction or checkpoint.
    deferred_write: bool,
    /// Per-database entry count.
    ///
    /// Incremented on every new insert, decremented on every delete.
    /// Shared (Arc) so that CursorImpl can update it without holding the
    /// `DatabaseImpl` write lock — reads and writes are both O(1) atomics.
    ///
    /// `DatabaseImpl.count` (AtomicLong, updated in
    /// `BIN.insertEntry` / `BIN.deleteEntry`).
    entry_count: Arc<AtomicU64>,
    /// Key comparator (None = default byte comparison).
    bt_comparator:
        Option<Box<dyn Fn(&[u8], &[u8]) -> std::cmp::Ordering + Send + Sync>>,
    /// Duplicate comparator (None = default byte comparison).
    dup_comparator:
        Option<Box<dyn Fn(&[u8], &[u8]) -> std::cmp::Ordering + Send + Sync>>,
    /// Per-database operation throughput counters.
    ///
    /// Shared with every CursorImpl opened on this database so that insert,
    /// search, update, delete and position operations can be counted on the
    /// hot path without acquiring any mutex.
    pub throughput: Arc<ThroughputStats>,
}

/// Persistent B-tree root metadata stored alongside the database record.
///
/// Holds the root LSN so that recovery can locate the tree root on disk.
/// The live in-memory tree is `DatabaseImpl::real_tree`.
///
/// (the persistent `Tree` object stored as part
/// of the database record).
#[derive(Debug)]
pub struct DatabaseTree {
    /// Root LSN of the tree.
    root_lsn: u64,
}

impl Default for DatabaseTree {
    fn default() -> Self {
        Self::new()
    }
}

impl DatabaseTree {
    pub fn new() -> Self {
        DatabaseTree { root_lsn: noxu_util::NULL_LSN.as_u64() }
    }
    pub fn get_root_lsn(&self) -> u64 {
        self.root_lsn
    }
    pub fn set_root_lsn(&mut self, lsn: u64) {
        self.root_lsn = lsn;
    }
}

impl DatabaseImpl {
    /// Creates a new DatabaseImpl.
    pub fn new(
        id: DatabaseId,
        name: String,
        db_type: DbType,
        config: &DatabaseConfig,
    ) -> Self {
        let mut flags = 0u8;
        if config.sorted_duplicates {
            flags |= DUPS_ENABLED;
        }
        if config.temporary {
            flags |= TEMPORARY_BIT;
        }
        if config.key_prefixing {
            flags |= PREFIXING_ENABLED;
        }

        let max_entries = config.node_max_entries as usize;
        let real_tree = if config.sorted_duplicates {
            // Sorted-dup databases store (key, data) as two-part composite keys.
            // A custom comparator is required: pure lexicographic ordering fails
            // when a shorter primary key is a byte-prefix of a longer key's data.
            let dup_cmp: KeyComparatorFn = Arc::new(|a: &[u8], b: &[u8]| {
                dup_key_data::cmp_two_part_keys(a, b, |x, y| x.cmp(y), |x, y| x.cmp(y))
            });
            Tree::new_with_comparator(id.id() as u64, max_entries, dup_cmp)
        } else {
            Tree::new(id.id() as u64, max_entries)
        };
        DatabaseImpl {
            id,
            name,
            db_type,
            flags,
            delete_state: DeleteState::NotDeleted,
            dirty: AtomicBool::new(false),
            max_tree_entries_per_node: config.node_max_entries,
            reference_count: AtomicI64::new(0),
            tree: Some(DatabaseTree::new()),
            real_tree: Some(real_tree),
            deferred_write: config.deferred_write,
            bt_comparator: None,
            dup_comparator: None,
            entry_count: Arc::new(AtomicU64::new(0)),
            throughput: ThroughputStats::new(),
        }
    }

    // Getters
    pub fn get_id(&self) -> DatabaseId {
        self.id
    }
    pub fn get_name(&self) -> &str {
        &self.name
    }
    pub fn get_db_type(&self) -> DbType {
        self.db_type
    }

    /// Returns true if this database uses deferred write mode.
    ///
    /// 
    pub fn is_deferred_write(&self) -> bool {
        self.deferred_write
    }

    // Flag methods
    pub fn get_sorted_duplicates(&self) -> bool {
        self.flags & DUPS_ENABLED != 0
    }
    pub fn is_temporary(&self) -> bool {
        self.flags & TEMPORARY_BIT != 0
    }
    pub fn get_key_prefixing(&self) -> bool {
        self.flags & PREFIXING_ENABLED != 0
    }
    pub fn is_replicated(&self) -> bool {
        self.flags & IS_REPLICATED_BIT != 0
    }

    // Delete state
    pub fn is_deleted(&self) -> bool {
        self.delete_state == DeleteState::Deleted
    }
    pub fn is_deleting(&self) -> bool {
        self.delete_state != DeleteState::NotDeleted
    }
    pub fn start_delete(&mut self) {
        self.delete_state = DeleteState::DeletedCleanupInListHarvest;
    }
    pub fn finish_delete(&mut self) {
        self.delete_state = DeleteState::Deleted;
    }

    // Dirty tracking
    pub fn is_dirty(&self) -> bool {
        self.dirty.load(Ordering::Relaxed)
    }
    pub fn set_dirty(&self) {
        self.dirty.store(true, Ordering::Relaxed);
    }
    pub fn clear_dirty(&self) {
        self.dirty.store(false, Ordering::Relaxed);
    }

    // Reference counting (for open handles)
    pub fn increment_reference_count(&self) {
        self.reference_count.fetch_add(1, Ordering::Relaxed);
    }
    pub fn decrement_reference_count(&self) {
        self.reference_count.fetch_sub(1, Ordering::Relaxed);
    }
    pub fn reference_count(&self) -> i64 {
        self.reference_count.load(Ordering::Relaxed)
    }

    // Entry count (O(1) atomic counter)
    /// Returns the current entry count.
    ///
    /// In — reads an AtomicLong.
    pub fn entry_count(&self) -> u64 {
        self.entry_count.load(Ordering::Relaxed)
    }

    /// Increments the entry count by 1 (on new insert).
    pub fn increment_entry_count(&self) {
        self.entry_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrements the entry count by 1 (on delete), saturating at zero.
    pub fn decrement_entry_count(&self) {
        // Use a compare-and-swap loop to avoid underflow.
        loop {
            let cur = self.entry_count.load(Ordering::Relaxed);
            if cur == 0 {
                break;
            }
            if self.entry_count
                .compare_exchange_weak(cur, cur - 1, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
            {
                break;
            }
        }
    }

    // Tree access (stub for LSN tracking)
    pub fn get_tree(&self) -> Option<&DatabaseTree> {
        self.tree.as_ref()
    }
    pub fn get_tree_mut(&mut self) -> Option<&mut DatabaseTree> {
        self.tree.as_mut()
    }

    // Real B+tree access for cursor traversal and data operations.
    /// Returns a reference to the real B+tree.
    pub fn get_real_tree(&self) -> Option<&Tree> {
        self.real_tree.as_ref()
    }
    /// Returns a mutable reference to the real B+tree.
    pub fn get_real_tree_mut(&mut self) -> Option<&mut Tree> {
        self.real_tree.as_mut()
    }

    /// Sets the expiration time (absolute hours since Unix epoch) for the
    /// BIN slot holding `key`.
    ///
    /// Returns `true` if the key was found and updated.
    /// Delegates to `Tree::update_key_expiration()`.
    pub fn update_key_expiration(&self, key: &[u8], expiration_hours: u32) -> bool {
        self.real_tree
            .as_ref()
            .map(|t| t.update_key_expiration(key, expiration_hours))
            .unwrap_or(false)
    }

    /// Collects structural B-tree statistics.
    ///
    /// Walks the full tree (O(n) in node count) and returns node counts
    /// and maximum depth.  Mirrors JE `DatabaseImpl.getDbStats(fast=false)`.
    ///
    /// Returns `None` if this DatabaseImpl has no real tree (e.g. internal
    /// metadata databases).
    pub fn collect_btree_stats(&self) -> Option<noxu_tree::TreeStats> {
        self.real_tree.as_ref().map(|t| t.collect_stats())
    }

    /// Replace the real B+tree with a tree recovered from the log.
    ///
    /// Called by `EnvironmentImpl::open_database()` when a matching
    /// `recovered_trees` entry exists (Approach B of P1b wiring).
    pub fn set_recovered_tree(&mut self, tree: Tree) {
        // Synchronise the in-memory entry_count counter from the recovered
        // tree so that Database::count() returns the correct value after reopen.
        let count = tree.count_entries();
        self.entry_count.store(count, std::sync::atomic::Ordering::Relaxed);
        self.real_tree = Some(tree);
    }

    // Configuration
    pub fn max_tree_entries_per_node(&self) -> i32 {
        self.max_tree_entries_per_node
    }

    /// Sets a custom key comparator.
    pub fn set_bt_comparator<F>(&mut self, comparator: F)
    where
        F: Fn(&[u8], &[u8]) -> std::cmp::Ordering + Send + Sync + 'static,
    {
        self.bt_comparator = Some(Box::new(comparator));
    }

    /// Compares keys using the btree comparator or default byte comparison.
    pub fn compare_keys(&self, key1: &[u8], key2: &[u8]) -> std::cmp::Ordering {
        if let Some(ref cmp) = self.bt_comparator {
            cmp(key1, key2)
        } else {
            key1.cmp(key2)
        }
    }

    /// Serialization.
    pub fn log_size(&self) -> usize {
        8 + // id
        4 + self.name.len() + // name (length-prefixed)
        1 + // flags
        4 + // max entries
        8 // root LSN
    }

    pub fn write_to_log(&self, buf: &mut Vec<u8>) -> std::io::Result<()> {
        buf.write_i64::<BigEndian>(self.id.id())?;
        buf.write_u32::<BigEndian>(self.name.len() as u32)?;
        buf.extend_from_slice(self.name.as_bytes());
        buf.write_u8(self.flags)?;
        buf.write_i32::<BigEndian>(self.max_tree_entries_per_node)?;
        let root_lsn = self
            .tree
            .as_ref()
            .map_or(noxu_util::NULL_LSN.as_u64(), |t| t.root_lsn);
        buf.write_u64::<BigEndian>(root_lsn)?;
        Ok(())
    }

    pub fn read_from_log(buf: &[u8]) -> std::io::Result<Self> {
        // Helper:
        fn type_for_db_name(name: &str) -> DbType {
            match name {
                "_jeIdMap" | "_noxuIdMap" => DbType::Id,
                "_jeNameMap" | "_noxuNameMap" => DbType::Name,
                "_jeUtilization" | "_noxuUtilization" => DbType::Utilization,
                _ => DbType::User,
            }
        }
        use std::io::Cursor;

        let mut cursor = Cursor::new(buf);
        let id = cursor.read_i64::<BigEndian>()?;
        let name_len = cursor.read_u32::<BigEndian>()? as usize;

        // Read name bytes
        let name_start = cursor.position() as usize;
        let name_end = name_start + name_len;
        if name_end > buf.len() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Buffer too short for name",
            ));
        }
        let name = String::from_utf8(buf[name_start..name_end].to_vec())
            .map_err(|e| {
                std::io::Error::new(std::io::ErrorKind::InvalidData, e)
            })?;
        cursor.set_position(name_end as u64);

        let flags = cursor.read_u8()?;
        let max_entries = cursor.read_i32::<BigEndian>()?;
        let root_lsn = cursor.read_u64::<BigEndian>()?;

        let db_type = type_for_db_name(&name);

        let mut tree = DatabaseTree::new();
        tree.root_lsn = root_lsn;

        let real_tree =
            Tree::new(id as u64, max_entries as usize);
        Ok(DatabaseImpl {
            id: DatabaseId::new(id),
            name,
            db_type,
            flags,
            delete_state: DeleteState::NotDeleted,
            dirty: AtomicBool::new(false),
            max_tree_entries_per_node: max_entries,
            reference_count: AtomicI64::new(0),
            tree: Some(tree),
            real_tree: Some(real_tree),
            deferred_write: false, // not persisted in log record; set after open if needed
            bt_comparator: None,
            dup_comparator: None,
            entry_count: Arc::new(AtomicU64::new(0)),
            throughput: ThroughputStats::new(),
        })
    }
}

impl std::fmt::Debug for DatabaseImpl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseImpl")
            .field("id", &self.id)
            .field("name", &self.name)
            .field("db_type", &self.db_type)
            .field("flags", &self.flags)
            .field("delete_state", &self.delete_state)
            .finish()
    }
}

#[cfg(test)]
#[allow(clippy::field_reassign_with_default)]
mod tests {
    use super::*;

    fn make_config() -> DatabaseConfig {
        DatabaseConfig::default()
    }

    #[test]
    fn test_new_database() {
        let config = make_config();
        let db = DatabaseImpl::new(
            DatabaseId::new(100),
            "test_db".to_string(),
            DbType::User,
            &config,
        );

        assert_eq!(db.get_id(), DatabaseId::new(100));
        assert_eq!(db.get_name(), "test_db");
        assert_eq!(db.get_db_type(), DbType::User);
        assert!(!db.is_deleted());
        assert!(!db.is_deleting());
        assert_eq!(db.reference_count(), 0);
    }

    #[test]
    fn test_sorted_duplicates_flag() {
        let mut config = DatabaseConfig::default();
        config.sorted_duplicates = false;
        let db1 = DatabaseImpl::new(
            DatabaseId::new(1),
            "db1".to_string(),
            DbType::User,
            &config,
        );
        assert!(!db1.get_sorted_duplicates());

        config.sorted_duplicates = true;
        let db2 = DatabaseImpl::new(
            DatabaseId::new(2),
            "db2".to_string(),
            DbType::User,
            &config,
        );
        assert!(db2.get_sorted_duplicates());
    }

    #[test]
    fn test_temporary_flag() {
        let mut config = DatabaseConfig::default();
        config.temporary = false;
        let db1 = DatabaseImpl::new(
            DatabaseId::new(1),
            "db1".to_string(),
            DbType::User,
            &config,
        );
        assert!(!db1.is_temporary());

        config.temporary = true;
        let db2 = DatabaseImpl::new(
            DatabaseId::new(2),
            "db2".to_string(),
            DbType::User,
            &config,
        );
        assert!(db2.is_temporary());
    }

    #[test]
    fn test_key_prefixing_flag() {
        let mut config = DatabaseConfig::default();
        config.key_prefixing = false;
        let db1 = DatabaseImpl::new(
            DatabaseId::new(1),
            "db1".to_string(),
            DbType::User,
            &config,
        );
        assert!(!db1.get_key_prefixing());

        config.key_prefixing = true;
        let db2 = DatabaseImpl::new(
            DatabaseId::new(2),
            "db2".to_string(),
            DbType::User,
            &config,
        );
        assert!(db2.get_key_prefixing());
    }

    #[test]
    fn test_delete_state_transitions() {
        let config = make_config();
        let mut db = DatabaseImpl::new(
            DatabaseId::new(1),
            "db".to_string(),
            DbType::User,
            &config,
        );

        assert!(!db.is_deleted());
        assert!(!db.is_deleting());

        db.start_delete();
        assert!(!db.is_deleted());
        assert!(db.is_deleting());

        db.finish_delete();
        assert!(db.is_deleted());
        assert!(db.is_deleting());
    }

    #[test]
    fn test_dirty_tracking() {
        let config = make_config();
        let db = DatabaseImpl::new(
            DatabaseId::new(1),
            "db".to_string(),
            DbType::User,
            &config,
        );

        assert!(!db.is_dirty());

        db.set_dirty();
        assert!(db.is_dirty());

        db.clear_dirty();
        assert!(!db.is_dirty());
    }

    #[test]
    fn test_reference_counting() {
        let config = make_config();
        let db = DatabaseImpl::new(
            DatabaseId::new(1),
            "db".to_string(),
            DbType::User,
            &config,
        );

        assert_eq!(db.reference_count(), 0);

        db.increment_reference_count();
        assert_eq!(db.reference_count(), 1);

        db.increment_reference_count();
        assert_eq!(db.reference_count(), 2);

        db.decrement_reference_count();
        assert_eq!(db.reference_count(), 1);

        db.decrement_reference_count();
        assert_eq!(db.reference_count(), 0);
    }

    #[test]
    fn test_compare_keys_default() {
        let config = make_config();
        let db = DatabaseImpl::new(
            DatabaseId::new(1),
            "db".to_string(),
            DbType::User,
            &config,
        );

        assert_eq!(db.compare_keys(b"aaa", b"bbb"), std::cmp::Ordering::Less);
        assert_eq!(
            db.compare_keys(b"bbb", b"aaa"),
            std::cmp::Ordering::Greater
        );
        assert_eq!(db.compare_keys(b"aaa", b"aaa"), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_compare_keys_custom() {
        let config = make_config();
        let mut db = DatabaseImpl::new(
            DatabaseId::new(1),
            "db".to_string(),
            DbType::User,
            &config,
        );

        // Reverse comparator
        db.set_bt_comparator(|a, b| b.cmp(a));

        assert_eq!(
            db.compare_keys(b"aaa", b"bbb"),
            std::cmp::Ordering::Greater
        );
        assert_eq!(db.compare_keys(b"bbb", b"aaa"), std::cmp::Ordering::Less);
        assert_eq!(db.compare_keys(b"aaa", b"aaa"), std::cmp::Ordering::Equal);
    }

    #[test]
    fn test_serialization_round_trip() {
        let mut config = DatabaseConfig::default();
        config.sorted_duplicates = true;
        config.key_prefixing = true;
        config.node_max_entries = 256;

        let db = DatabaseImpl::new(
            DatabaseId::new(42),
            "my_database".to_string(),
            DbType::User,
            &config,
        );

        let mut buf = Vec::new();
        db.write_to_log(&mut buf).unwrap();

        let db2 = DatabaseImpl::read_from_log(&buf).unwrap();

        assert_eq!(db2.get_id(), DatabaseId::new(42));
        assert_eq!(db2.get_name(), "my_database");
        assert!(db2.get_sorted_duplicates());
        assert!(db2.get_key_prefixing());
        assert_eq!(db2.max_tree_entries_per_node(), 256);
    }

    #[test]
    fn test_tree_access() {
        let config = make_config();
        let mut db = DatabaseImpl::new(
            DatabaseId::new(1),
            "db".to_string(),
            DbType::User,
            &config,
        );

        // Default tree has NULL_LSN
        {
            let tree = db.get_tree().unwrap();
            assert_eq!(tree.get_root_lsn(), noxu_util::NULL_LSN.as_u64());
        }

        // Set root LSN
        {
            let tree = db.get_tree_mut().unwrap();
            tree.set_root_lsn(12345);
        }

        // Verify it was set
        {
            let tree = db.get_tree().unwrap();
            assert_eq!(tree.get_root_lsn(), 12345);
        }
    }

    #[test]
    fn test_log_size() {
        let config = make_config();
        let db = DatabaseImpl::new(
            DatabaseId::new(1),
            "test".to_string(),
            DbType::User,
            &config,
        );

        let expected_size = 8 + 4 + 4 + 1 + 4 + 8; // id + name_len + "test" + flags + max_entries + root_lsn
        assert_eq!(db.log_size(), expected_size);
    }
}
