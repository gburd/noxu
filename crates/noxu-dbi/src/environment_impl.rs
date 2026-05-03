//! Internal environment implementation.
//!
//! Port of `com.sleepycat.je.dbi.EnvironmentImpl`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use parking_lot::RwLock;

use crate::database_impl::DatabaseImpl;
use crate::{
    DatabaseConfig, DatabaseId, DbType, DbiError, EnvState,
    EnvironmentFailureReason, NodeSequence,
};
use noxu_txn::{LockManager, Txn, TxnManager};

/// The internal representation of an environment.
///
/// Owns all subsystems: log, tree, txn, lock, evictor, cleaner, etc.
/// This is a simplified initial implementation that wires together
/// the key components built in phases 0-3.
///
/// Port of `com.sleepycat.je.dbi.EnvironmentImpl`.
pub struct EnvironmentImpl {
    /// Path to the environment home directory.
    env_home: PathBuf,
    /// Current environment state.
    state: RwLock<EnvState>,
    /// Whether this is a read-only environment.
    is_read_only: bool,
    /// Whether transactions are enabled.
    is_transactional: bool,

    /// Node ID and transient LSN generator.
    node_sequence: NodeSequence,
    /// Next database ID.
    next_db_id: AtomicI64,

    /// The lock manager (shared across all lockers/txns).
    lock_manager: Arc<LockManager>,
    /// The transaction manager.
    txn_manager: TxnManager,

    /// All open databases, keyed by DatabaseId.
    db_map: RwLock<HashMap<DatabaseId, Arc<RwLock<DatabaseImpl>>>>,
    /// Name -> DatabaseId mapping.
    name_map: RwLock<HashMap<String, DatabaseId>>,

    /// Whether the environment has been invalidated.
    is_invalid: AtomicBool,
    /// If invalidated, the reason.
    invalid_reason: RwLock<Option<EnvironmentFailureReason>>,

    /// Creation time in milliseconds.
    creation_time_ms: u64,
}

impl EnvironmentImpl {
    /// Creates a new EnvironmentImpl.
    ///
    /// In a full implementation, this would:
    /// 1. Open/create the environment directory
    /// 2. Acquire the environment lock file
    /// 3. Initialize the log subsystem (FileManager, LogManager)
    /// 4. Run recovery
    /// 5. Open internal databases (id, name, utilization)
    /// 6. Start daemon threads (evictor, cleaner, checkpointer)
    pub fn new(
        env_home: impl Into<PathBuf>,
        read_only: bool,
        transactional: bool,
    ) -> Result<Self, DbiError> {
        let env_home = env_home.into();
        let lock_manager = Arc::new(LockManager::new());
        let txn_manager = TxnManager::new(lock_manager.clone());

        let env = EnvironmentImpl {
            env_home,
            state: RwLock::new(EnvState::Init),
            is_read_only: read_only,
            is_transactional: transactional,
            node_sequence: NodeSequence::new(),
            next_db_id: AtomicI64::new(1),
            lock_manager,
            txn_manager,
            db_map: RwLock::new(HashMap::new()),
            name_map: RwLock::new(HashMap::new()),
            is_invalid: AtomicBool::new(false),
            invalid_reason: RwLock::new(None),
            creation_time_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
        };

        // Mark as open
        *env.state.write() = EnvState::Open;

        Ok(env)
    }

    // Getters
    pub fn get_env_home(&self) -> &Path {
        &self.env_home
    }
    pub fn is_read_only(&self) -> bool {
        self.is_read_only
    }
    pub fn is_transactional(&self) -> bool {
        self.is_transactional
    }
    pub fn get_creation_time(&self) -> u64 {
        self.creation_time_ms
    }

    // State management
    pub fn get_state(&self) -> EnvState {
        *self.state.read()
    }
    pub fn is_open(&self) -> bool {
        self.state.read().is_open()
    }
    pub fn is_valid(&self) -> bool {
        !self.is_invalid.load(Ordering::Relaxed)
    }

    /// Checks that the environment is open and valid.
    pub fn check_open(&self) -> Result<(), DbiError> {
        // Check validity first - if invalidated, that takes precedence
        if !self.is_valid() {
            let reason = self
                .invalid_reason
                .read()
                .map(|r| format!("{:?}", r))
                .unwrap_or_else(|| "unknown".to_string());
            return Err(DbiError::EnvironmentFailure { reason });
        }
        if !self.is_open() {
            return Err(DbiError::EnvironmentNotOpen);
        }
        Ok(())
    }

    /// Invalidates the environment due to a failure.
    pub fn invalidate(&self, reason: EnvironmentFailureReason) {
        self.is_invalid.store(true, Ordering::Relaxed);
        *self.invalid_reason.write() = Some(reason);
        *self.state.write() = EnvState::Invalid;
    }

    // Database operations

    /// Creates or opens a database.
    pub fn open_database(
        &self,
        name: &str,
        config: &DatabaseConfig,
    ) -> Result<Arc<RwLock<DatabaseImpl>>, DbiError> {
        self.check_open()?;

        // Check if database already exists
        if let Some(db_id) = self.name_map.read().get(name)
            && let Some(db) = self.db_map.read().get(db_id)
        {
            db.read().increment_reference_count();
            return Ok(db.clone());
        }

        // Create new database
        if !config.allow_create {
            return Err(DbiError::DatabaseNotFound(name.to_string()));
        }

        let db_id =
            DatabaseId::new(self.next_db_id.fetch_add(1, Ordering::Relaxed));

        let db_impl =
            DatabaseImpl::new(db_id, name.to_string(), DbType::User, config);

        let db = Arc::new(RwLock::new(db_impl));
        db.read().increment_reference_count();

        self.db_map.write().insert(db_id, db.clone());
        self.name_map.write().insert(name.to_string(), db_id);

        Ok(db)
    }

    /// Closes a database handle.
    pub fn close_database(&self, db_id: DatabaseId) -> Result<(), DbiError> {
        if let Some(db) = self.db_map.read().get(&db_id) {
            db.read().decrement_reference_count();
            if db.read().reference_count() <= 0 {
                // Could remove from maps, but keep for now
            }
        }
        Ok(())
    }

    /// Removes (deletes) a database by name.
    pub fn remove_database(&self, name: &str) -> Result<(), DbiError> {
        self.check_open()?;

        let db_id = self
            .name_map
            .write()
            .remove(name)
            .ok_or_else(|| DbiError::DatabaseNotFound(name.to_string()))?;

        if let Some(db) = self.db_map.write().remove(&db_id) {
            db.write().start_delete();
            db.write().finish_delete();
        }

        Ok(())
    }

    /// Renames a database.
    pub fn rename_database(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> Result<(), DbiError> {
        self.check_open()?;

        let db_id =
            self.name_map.read().get(old_name).copied().ok_or_else(|| {
                DbiError::DatabaseNotFound(old_name.to_string())
            })?;

        if self.name_map.read().contains_key(new_name) {
            return Err(DbiError::DatabaseAlreadyExists(new_name.to_string()));
        }

        self.name_map.write().remove(old_name);
        self.name_map.write().insert(new_name.to_string(), db_id);

        // In a full implementation, would log the rename

        Ok(())
    }

    /// Returns the list of database names.
    pub fn get_database_names(&self) -> Vec<String> {
        self.name_map.read().keys().cloned().collect()
    }

    // Transaction operations

    /// Begins a new transaction.
    pub fn begin_txn(&self) -> Result<Txn, DbiError> {
        self.check_open()?;
        Ok(self.txn_manager.begin_txn())
    }

    /// Returns a reference to the lock manager.
    pub fn get_lock_manager(&self) -> &Arc<LockManager> {
        &self.lock_manager
    }

    /// Returns a reference to the txn manager.
    pub fn get_txn_manager(&self) -> &TxnManager {
        &self.txn_manager
    }

    /// Returns a reference to the node sequence generator.
    pub fn get_node_sequence(&self) -> &NodeSequence {
        &self.node_sequence
    }

    /// Returns the number of active transactions.
    pub fn n_active_txns(&self) -> usize {
        self.txn_manager.n_active_txns()
    }

    /// Returns the number of open databases.
    pub fn n_databases(&self) -> usize {
        self.db_map.read().len()
    }

    /// Closes the environment.
    pub fn close(&self) -> Result<(), DbiError> {
        let mut state = self.state.write();
        if state.is_closed() {
            return Ok(());
        }
        *state = EnvState::Closing;

        // In a full implementation:
        // 1. Run final checkpoint
        // 2. Stop daemon threads
        // 3. Close all databases
        // 4. Close log files
        // 5. Release env lock

        *state = EnvState::Closed;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_environment_creation() {
        let env = EnvironmentImpl::new("/tmp/test_env", false, true).unwrap();
        assert_eq!(env.get_env_home(), Path::new("/tmp/test_env"));
        assert!(!env.is_read_only());
        assert!(env.is_transactional());
        assert!(env.is_open());
        assert!(env.is_valid());
        assert!(matches!(env.get_state(), EnvState::Open));
    }

    #[test]
    fn test_open_database_with_create() {
        let env = EnvironmentImpl::new("/tmp/test_env", false, true).unwrap();

        let mut config = DatabaseConfig::new();
        config.set_allow_create(true);

        let db = env.open_database("test_db", &config).unwrap();
        assert_eq!(db.read().get_name(), "test_db");
        assert_eq!(db.read().reference_count(), 1);
    }

    #[test]
    fn test_open_database_without_create() {
        let env = EnvironmentImpl::new("/tmp/test_env", false, true).unwrap();

        let config = DatabaseConfig::new();
        let result = env.open_database("test_db", &config);

        assert!(matches!(result, Err(DbiError::DatabaseNotFound(_))));
    }

    #[test]
    fn test_open_same_database_twice() {
        let env = EnvironmentImpl::new("/tmp/test_env", false, true).unwrap();

        let mut config = DatabaseConfig::new();
        config.set_allow_create(true);

        let db1 = env.open_database("test_db", &config).unwrap();
        let db2 = env.open_database("test_db", &config).unwrap();

        // Should return the same database
        assert_eq!(db1.read().get_id(), db2.read().get_id());
        assert_eq!(db1.read().reference_count(), 2);
    }

    #[test]
    fn test_remove_database() {
        let env = EnvironmentImpl::new("/tmp/test_env", false, true).unwrap();

        let mut config = DatabaseConfig::new();
        config.set_allow_create(true);

        let _db = env.open_database("test_db", &config).unwrap();

        env.remove_database("test_db").unwrap();

        let result = env.open_database("test_db", &DatabaseConfig::new());
        assert!(matches!(result, Err(DbiError::DatabaseNotFound(_))));
    }

    #[test]
    fn test_rename_database() {
        let env = EnvironmentImpl::new("/tmp/test_env", false, true).unwrap();

        let mut config = DatabaseConfig::new();
        config.set_allow_create(true);

        let db = env.open_database("old_name", &config).unwrap();
        let db_id = db.read().get_id();

        env.rename_database("old_name", "new_name").unwrap();

        // Old name should not exist
        let result = env.open_database("old_name", &DatabaseConfig::new());
        assert!(matches!(result, Err(DbiError::DatabaseNotFound(_))));

        // New name should exist and point to same database
        let db2 = env.open_database("new_name", &config).unwrap();
        assert_eq!(db2.read().get_id(), db_id);
    }

    #[test]
    fn test_get_database_names() {
        let env = EnvironmentImpl::new("/tmp/test_env", false, true).unwrap();

        let mut config = DatabaseConfig::new();
        config.set_allow_create(true);

        env.open_database("db1", &config).unwrap();
        env.open_database("db2", &config).unwrap();
        env.open_database("db3", &config).unwrap();

        let names = env.get_database_names();
        assert_eq!(names.len(), 3);
        assert!(names.contains(&"db1".to_string()));
        assert!(names.contains(&"db2".to_string()));
        assert!(names.contains(&"db3".to_string()));
    }

    #[test]
    fn test_begin_txn() {
        let env = EnvironmentImpl::new("/tmp/test_env", false, true).unwrap();
        let _txn = env.begin_txn().unwrap();
        assert_eq!(env.n_active_txns(), 1);
    }

    #[test]
    fn test_invalidate_environment() {
        let env = EnvironmentImpl::new("/tmp/test_env", false, true).unwrap();

        assert!(env.is_valid());

        env.invalidate(EnvironmentFailureReason::LogChecksum);

        assert!(!env.is_valid());
        assert!(matches!(env.get_state(), EnvState::Invalid));

        let result = env.begin_txn();
        assert!(matches!(result, Err(DbiError::EnvironmentFailure { .. })));
    }

    #[test]
    fn test_close_environment() {
        let env = EnvironmentImpl::new("/tmp/test_env", false, true).unwrap();

        assert!(env.is_open());

        env.close().unwrap();

        assert!(!env.is_open());
        assert!(matches!(env.get_state(), EnvState::Closed));

        // Second close should be ok
        env.close().unwrap();
    }

    #[test]
    fn test_operations_on_closed_environment() {
        let env = EnvironmentImpl::new("/tmp/test_env", false, true).unwrap();

        env.close().unwrap();

        let mut config = DatabaseConfig::new();
        config.set_allow_create(true);

        let result = env.open_database("test_db", &config);
        assert!(matches!(result, Err(DbiError::EnvironmentNotOpen)));

        let result = env.begin_txn();
        assert!(matches!(result, Err(DbiError::EnvironmentNotOpen)));
    }

    #[test]
    fn test_read_only_mode() {
        let env = EnvironmentImpl::new("/tmp/test_env", true, true).unwrap();
        assert!(env.is_read_only());
    }

    #[test]
    fn test_multiple_databases_coexist() {
        let env = EnvironmentImpl::new("/tmp/test_env", false, true).unwrap();

        let mut config = DatabaseConfig::new();
        config.set_allow_create(true);

        let db1 = env.open_database("db1", &config).unwrap();
        let db2 = env.open_database("db2", &config).unwrap();
        let db3 = env.open_database("db3", &config).unwrap();

        assert_eq!(env.n_databases(), 3);
        assert_ne!(db1.read().get_id(), db2.read().get_id());
        assert_ne!(db2.read().get_id(), db3.read().get_id());
    }

    #[test]
    fn test_n_active_txns() {
        let env = EnvironmentImpl::new("/tmp/test_env", false, true).unwrap();

        assert_eq!(env.n_active_txns(), 0);

        let _txn1 = env.begin_txn().unwrap();
        assert_eq!(env.n_active_txns(), 1);

        let _txn2 = env.begin_txn().unwrap();
        assert_eq!(env.n_active_txns(), 2);
    }
}
