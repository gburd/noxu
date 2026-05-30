//! Persistent class catalog for entity stores (Wave 2C-2).
//!
//! The catalog is a hidden database, named `__noxu_persist_catalog__<store>`,
//! that records the most recent class version observed for each entity
//! class in the store.  It is used by the schema-evolution open path to
//! decide whether evolution is required when the user-supplied
//! [`Entity::class_version`] differs from the persisted value.
//!
//! On-disk shape:
//!
//! * **Key:** UTF-8 bytes of the entity class name (`Entity::entity_name()`).
//! * **Value:** little 6-byte fixed record:
//!   * `[0..2]`  catalog format version (currently `1u16` BE)
//!   * `[2..4]`  current class version (u16 BE)
//!   * `[4..6]`  reserved / flags (u16 BE, currently always `0`)
//!
//! Pre-v1.6 entity stores did not have a catalog database; opening such
//! a store transparently creates an empty catalog.  See the migration
//! guide.
//!
//! [`Entity::class_version`]: crate::persist::entity::Entity::class_version

use crate::db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, OperationStatus,
    Transaction,
};

use crate::persist::error::{PersistError, Result};

/// Format version of the catalog record itself (not the entity class
/// version).  Bumped if the catalog record layout changes.
const CATALOG_FORMAT_VERSION: u16 = 1;

/// Size of a catalog value record in bytes.
const RECORD_LEN: usize = 6;

/// One row in the persistent catalog database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CatalogEntry {
    /// The catalog record format version that produced this entry.
    pub format_version: u16,
    /// The most recent class version observed for the entity.
    pub class_version: u16,
}

impl CatalogEntry {
    /// Encodes this entry into its 6-byte on-disk shape.
    fn encode(&self) -> [u8; RECORD_LEN] {
        let mut out = [0u8; RECORD_LEN];
        out[0..2].copy_from_slice(&self.format_version.to_be_bytes());
        out[2..4].copy_from_slice(&self.class_version.to_be_bytes());
        // bytes[4..6] reserved for future flags, always zero today.
        out
    }

    /// Decodes a 6-byte on-disk record.
    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != RECORD_LEN {
            return Err(PersistError::SerializationError(format!(
                "catalog record has wrong length: {} (expected {})",
                bytes.len(),
                RECORD_LEN,
            )));
        }
        let format_version = u16::from_be_bytes([bytes[0], bytes[1]]);
        let class_version = u16::from_be_bytes([bytes[2], bytes[3]]);
        Ok(Self { format_version, class_version })
    }
}

/// Returns the catalog database name for the given store.
pub fn catalog_db_name(store_name: &str) -> String {
    format!("__noxu_persist_catalog__{}", store_name)
}

/// Wraps a hidden database and exposes the typed catalog operations.
///
/// The catalog is opened with `allow_create = true` whenever the parent
/// `EntityStore` was opened with `allow_create = true`.  Read-only stores
/// open an empty in-memory shim (no database created on disk) since they
/// cannot evolve anyway.
pub struct ClassCatalog {
    /// The backing database, or `None` for read-only stores that did not
    /// find an existing catalog database.
    db: Option<Database>,
}

impl ClassCatalog {
    /// Opens or creates the catalog database for `store_name`.
    ///
    /// * If `allow_create` is true, the database is created if it doesn't
    ///   exist (matches `EntityStore::open` semantics).
    /// * If `read_only` is true, the database is opened read-only.
    /// * If `transactional` is true, the database participates in
    ///   transactions; otherwise auto-commit is used.
    pub fn open(
        env: &Environment,
        store_name: &str,
        allow_create: bool,
        read_only: bool,
        transactional: bool,
    ) -> Result<Self> {
        let name = catalog_db_name(store_name);
        let mut cfg = DatabaseConfig::new();
        cfg.set_allow_create(allow_create);
        cfg.set_read_only(read_only);
        cfg.set_transactional(transactional);

        match env.open_database(None, &name, &cfg) {
            Ok(db) => Ok(Self { db: Some(db) }),
            Err(e) if read_only => {
                // Read-only opens may legitimately fail when the catalog
                // does not exist on disk yet; expose an empty catalog so
                // gets return None and writes are rejected.
                log::debug!(target: "crate::persist::evolve",
                    "no on-disk catalog for store '{}' (read-only): {}",
                    store_name, e);
                Ok(Self { db: None })
            }
            Err(e) => Err(PersistError::DatabaseError(e)),
        }
    }

    /// Looks up a catalog entry for `class_name`.
    ///
    /// Returns `Ok(None)` if no entry exists.
    pub fn get(
        &self,
        txn: Option<&Transaction>,
        class_name: &str,
    ) -> Result<Option<CatalogEntry>> {
        let Some(db) = &self.db else {
            return Ok(None);
        };
        let key = DatabaseEntry::from_vec(class_name.as_bytes().to_vec());
        let mut data = DatabaseEntry::new();
        match db.get(txn, &key, &mut data)? {
            OperationStatus::Success => {
                let bytes = data.get_data().ok_or_else(|| {
                    PersistError::SerializationError(
                        "catalog entry has empty data".to_string(),
                    )
                })?;
                Ok(Some(CatalogEntry::decode(bytes)?))
            }
            _ => Ok(None),
        }
    }

    /// Inserts or updates an entry for `class_name`.
    pub fn put(
        &self,
        txn: Option<&Transaction>,
        class_name: &str,
        class_version: u16,
    ) -> Result<()> {
        let db = self.db.as_ref().ok_or_else(|| {
            PersistError::DatabaseError(
                crate::db::NoxuError::OperationNotAllowed(
                    "catalog database is not writable (read-only or absent)"
                        .to_string(),
                ),
            )
        })?;
        let entry = CatalogEntry {
            format_version: CATALOG_FORMAT_VERSION,
            class_version,
        };
        let key = DatabaseEntry::from_vec(class_name.as_bytes().to_vec());
        let val = DatabaseEntry::from_vec(entry.encode().to_vec());
        db.put(txn, &key, &val)?;
        Ok(())
    }

    /// Removes the entry for `class_name`.  Used when a class is dropped
    /// via a class-level [`crate::persist::evolve::Deleter`].
    ///
    /// Returns `true` if the entry existed.
    pub fn remove(
        &self,
        txn: Option<&Transaction>,
        class_name: &str,
    ) -> Result<bool> {
        let Some(db) = &self.db else {
            return Ok(false);
        };
        let key = DatabaseEntry::from_vec(class_name.as_bytes().to_vec());
        match db.delete(txn, &key)? {
            OperationStatus::Success => Ok(true),
            _ => Ok(false),
        }
    }

    /// Closes the catalog database.
    pub fn close(&mut self) -> Result<()> {
        if let Some(db) = self.db.take() {
            db.close()?;
        }
        Ok(())
    }
}

impl Drop for ClassCatalog {
    fn drop(&mut self) {
        let _ = self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::db::EnvironmentConfig;
    use tempfile::TempDir;

    fn temp_env() -> (TempDir, Environment) {
        let td = TempDir::new().unwrap();
        let cfg = EnvironmentConfig::new(td.path().to_path_buf())
            .with_allow_create(true);
        (td, Environment::open(cfg).unwrap())
    }

    #[test]
    fn round_trip_record() {
        let entry = CatalogEntry { format_version: 1, class_version: 7 };
        let dec = CatalogEntry::decode(&entry.encode()).unwrap();
        assert_eq!(entry, dec);
    }

    #[test]
    fn decode_wrong_length_errors() {
        assert!(CatalogEntry::decode(&[1, 2, 3]).is_err());
    }

    #[test]
    fn open_creates_catalog_database() {
        let (_td, env) = temp_env();
        let cat =
            ClassCatalog::open(&env, "store", true, false, false).unwrap();
        assert!(cat.db.is_some());
    }

    #[test]
    fn put_get_round_trip() {
        let (_td, env) = temp_env();
        let cat = ClassCatalog::open(&env, "s", true, false, false).unwrap();
        cat.put(None, "User", 3).unwrap();
        let got = cat.get(None, "User").unwrap().unwrap();
        assert_eq!(got.class_version, 3);
    }

    #[test]
    fn get_missing_returns_none() {
        let (_td, env) = temp_env();
        let cat = ClassCatalog::open(&env, "s", true, false, false).unwrap();
        assert!(cat.get(None, "DoesNotExist").unwrap().is_none());
    }

    #[test]
    fn put_overwrites() {
        let (_td, env) = temp_env();
        let cat = ClassCatalog::open(&env, "s", true, false, false).unwrap();
        cat.put(None, "X", 1).unwrap();
        cat.put(None, "X", 2).unwrap();
        assert_eq!(cat.get(None, "X").unwrap().unwrap().class_version, 2);
    }

    #[test]
    fn remove_existing_returns_true() {
        let (_td, env) = temp_env();
        let cat = ClassCatalog::open(&env, "s", true, false, false).unwrap();
        cat.put(None, "X", 1).unwrap();
        assert!(cat.remove(None, "X").unwrap());
        assert!(cat.get(None, "X").unwrap().is_none());
    }

    #[test]
    fn remove_missing_returns_false() {
        let (_td, env) = temp_env();
        let cat = ClassCatalog::open(&env, "s", true, false, false).unwrap();
        assert!(!cat.remove(None, "Nope").unwrap());
    }

    #[test]
    fn catalog_db_name_is_distinct() {
        assert_eq!(catalog_db_name("foo"), "__noxu_persist_catalog__foo");
        assert_ne!(catalog_db_name("foo"), "foo_User");
    }
}
