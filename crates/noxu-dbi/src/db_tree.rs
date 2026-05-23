use hashbrown::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};

use noxu_sync::RwLock;

use crate::database_config::DatabaseConfig;
use crate::database_impl::DatabaseImpl;
use crate::{DatabaseId, DbType, DbiError};

/// The database directory for this environment.
///
/// DbTree manages two internal databases:
/// - nameDatabase: maps name -> database ID
/// - idDatabase: maps database ID -> DatabaseImpl
///
///
pub struct DbTree {
    /// Name -> DatabaseId mapping.
    name_to_id: RwLock<HashMap<String, DatabaseId>>,
    /// DatabaseId -> DatabaseImpl mapping.
    id_to_db: RwLock<HashMap<DatabaseId, DatabaseImpl>>,
    /// Next available database ID.
    next_id: AtomicI64,
}

impl DbTree {
    pub fn new() -> Self {
        DbTree {
            name_to_id: RwLock::new(HashMap::new()),
            id_to_db: RwLock::new(HashMap::new()),
            next_id: AtomicI64::new(1),
        }
    }

    /// Allocates the next database ID.
    pub fn get_next_db_id(&self) -> DatabaseId {
        DatabaseId::new(self.next_id.fetch_add(1, Ordering::Relaxed))
    }

    /// Creates a new database in the directory.
    pub fn create_database(
        &self,
        name: &str,
        config: &DatabaseConfig,
    ) -> Result<DatabaseId, DbiError> {
        // Check if name already exists
        if self.name_to_id.read().contains_key(name) {
            return Err(DbiError::DatabaseAlreadyExists(name.to_string()));
        }

        let db_id = self.get_next_db_id();
        let db_impl =
            DatabaseImpl::new(db_id, name.to_string(), DbType::User, config);

        self.id_to_db.write().insert(db_id, db_impl);
        self.name_to_id.write().insert(name.to_string(), db_id);

        Ok(db_id)
    }

    /// Looks up a database by name.
    pub fn get_database_id(&self, name: &str) -> Option<DatabaseId> {
        self.name_to_id.read().get(name).copied()
    }

    /// Looks up a database by ID.
    pub fn get_database(&self, id: &DatabaseId) -> Option<DatabaseId> {
        if self.id_to_db.read().contains_key(id) { Some(*id) } else { None }
    }

    /// Returns the database name for a given ID.
    pub fn get_database_name(&self, id: &DatabaseId) -> Option<String> {
        self.id_to_db.read().get(id).map(|db| db.get_name().to_string())
    }

    /// Removes a database from the directory.
    pub fn remove_database(&self, name: &str) -> Result<DatabaseId, DbiError> {
        let db_id = self
            .name_to_id
            .write()
            .remove(name)
            .ok_or_else(|| DbiError::DatabaseNotFound(name.to_string()))?;

        if let Some(mut db) = self.id_to_db.write().remove(&db_id) {
            db.start_delete();
            db.finish_delete();
        }

        Ok(db_id)
    }

    /// Renames a database.
    pub fn rename_database(
        &self,
        old_name: &str,
        new_name: &str,
    ) -> Result<(), DbiError> {
        if self.name_to_id.read().contains_key(new_name) {
            return Err(DbiError::DatabaseAlreadyExists(new_name.to_string()));
        }

        let db_id =
            self.name_to_id.write().remove(old_name).ok_or_else(|| {
                DbiError::DatabaseNotFound(old_name.to_string())
            })?;

        self.name_to_id.write().insert(new_name.to_string(), db_id);

        Ok(())
    }

    /// Returns all database names.
    pub fn get_database_names(&self) -> Vec<String> {
        self.name_to_id.read().keys().cloned().collect()
    }

    /// Returns the number of databases.
    pub fn n_databases(&self) -> usize {
        self.id_to_db.read().len()
    }
}

impl Default for DbTree {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_lookup_database() {
        let tree = DbTree::new();
        let config = DatabaseConfig::default();

        let db_id = tree.create_database("test_db", &config).unwrap();

        // Lookup by name
        assert_eq!(tree.get_database_id("test_db"), Some(db_id));

        // Lookup by ID
        assert_eq!(tree.get_database(&db_id), Some(db_id));
    }

    #[test]
    fn test_create_duplicate_name_error() {
        let tree = DbTree::new();
        let config = DatabaseConfig::default();

        tree.create_database("test_db", &config).unwrap();

        // Try to create again with same name
        let result = tree.create_database("test_db", &config);
        assert!(matches!(result, Err(DbiError::DatabaseAlreadyExists(_))));
    }

    #[test]
    fn test_remove_database() {
        let tree = DbTree::new();
        let config = DatabaseConfig::default();

        let db_id = tree.create_database("test_db", &config).unwrap();

        // Remove database
        let removed_id = tree.remove_database("test_db").unwrap();
        assert_eq!(removed_id, db_id);

        // Verify it's gone
        assert_eq!(tree.get_database_id("test_db"), None);
        assert_eq!(tree.get_database(&db_id), None);
    }

    #[test]
    fn test_remove_nonexistent_database() {
        let tree = DbTree::new();

        let result = tree.remove_database("nonexistent");
        assert!(matches!(result, Err(DbiError::DatabaseNotFound(_))));
    }

    #[test]
    fn test_rename_database() {
        let tree = DbTree::new();
        let config = DatabaseConfig::default();

        let db_id = tree.create_database("old_name", &config).unwrap();

        // Rename
        tree.rename_database("old_name", "new_name").unwrap();

        // Verify old name is gone and new name works
        assert_eq!(tree.get_database_id("old_name"), None);
        assert_eq!(tree.get_database_id("new_name"), Some(db_id));
    }

    #[test]
    fn test_rename_to_existing_name_error() {
        let tree = DbTree::new();
        let config = DatabaseConfig::default();

        tree.create_database("db1", &config).unwrap();
        tree.create_database("db2", &config).unwrap();

        // Try to rename to existing name
        let result = tree.rename_database("db1", "db2");
        assert!(matches!(result, Err(DbiError::DatabaseAlreadyExists(_))));
    }

    #[test]
    fn test_get_database_names() {
        let tree = DbTree::new();
        let config = DatabaseConfig::default();

        tree.create_database("db1", &config).unwrap();
        tree.create_database("db2", &config).unwrap();
        tree.create_database("db3", &config).unwrap();

        let mut names = tree.get_database_names();
        names.sort();

        assert_eq!(names, vec!["db1", "db2", "db3"]);
    }

    #[test]
    fn test_n_databases() {
        let tree = DbTree::new();
        let config = DatabaseConfig::default();

        assert_eq!(tree.n_databases(), 0);

        tree.create_database("db1", &config).unwrap();
        assert_eq!(tree.n_databases(), 1);

        tree.create_database("db2", &config).unwrap();
        assert_eq!(tree.n_databases(), 2);

        tree.remove_database("db1").unwrap();
        assert_eq!(tree.n_databases(), 1);
    }

    #[test]
    fn test_get_next_db_id_sequential() {
        let tree = DbTree::new();

        let id1 = tree.get_next_db_id();
        let id2 = tree.get_next_db_id();
        let id3 = tree.get_next_db_id();

        assert_eq!(id1.id(), 1);
        assert_eq!(id2.id(), 2);
        assert_eq!(id3.id(), 3);
    }

    #[test]
    fn test_get_database_name() {
        let tree = DbTree::new();
        let config = DatabaseConfig::default();

        let db_id = tree.create_database("my_database", &config).unwrap();

        assert_eq!(
            tree.get_database_name(&db_id),
            Some("my_database".to_string())
        );

        // Test with nonexistent ID
        let bad_id = DatabaseId::new(999);
        assert_eq!(tree.get_database_name(&bad_id), None);
    }
}
