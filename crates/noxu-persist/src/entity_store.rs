//! Entity store for managing databases of typed entities.
//!

use hashbrown::HashMap;

use noxu_db::{Database, DatabaseConfig, DatabaseEntry, Environment};

use crate::entity::{Entity, PrimaryKey};
use crate::error::{PersistError, Result};
use crate::evolve::evolve_config::EvolveConfig;
use crate::evolve::mutations::Mutations;
use crate::evolve::stats::EvolveStats;
use crate::primary_index::PrimaryIndex;
use crate::secondary_index::SecondaryIndex;
use crate::store_config::StoreConfig;

/// An entity store that manages databases for entity classes.
///
/// The `EntityStore` is the main entry point for the persistence layer. It
/// manages the mapping between entity types and their underlying databases.
/// Each entity type gets its own database, named using the store name as a
/// prefix and the entity name as a suffix.
///
/// 
///
/// # Example
///
/// ```ignore
/// use noxu_persist::{EntityStore, StoreConfig, PrimaryIndex, Entity, PrimaryKey};
/// use noxu_db::{Environment, EnvironmentConfig};
///
/// let env = Environment::open(config)?;
/// let store_config = StoreConfig::new("my_store").with_allow_create(true);
/// let mut store = EntityStore::open(&env, store_config)?;
///
/// let index: PrimaryIndex<u64, User> = store.get_primary_index()?;
/// ```
pub struct EntityStore<'env> {
    env: &'env Environment,
    config: StoreConfig,
    databases: HashMap<String, Database>,
    open: bool,
}

impl<'env> EntityStore<'env> {
    /// Opens or creates an entity store.
    ///
    /// The store manages databases within the given environment. Database
    /// names are prefixed with the store name to avoid collisions between
    /// multiple stores in the same environment.
    ///
    /// Constructor.
    ///
    /// # Arguments
    /// * `env` - The environment in which to open the store
    /// * `config` - Configuration for the store
    ///
    /// # Errors
    /// Returns an error if the environment is not valid.
    pub fn open(env: &'env Environment, config: StoreConfig) -> Result<Self> {
        if !env.is_valid() {
            return Err(PersistError::DatabaseError(
                noxu_db::NoxuError::EnvironmentClosed,
            ));
        }

        Ok(EntityStore { env, config, databases: HashMap::new(), open: true })
    }

    /// Gets or creates the primary index for an entity type.
    ///
    /// If the underlying database does not exist and `allow_create` is true
    /// in the store configuration, it will be created. The database name is
    /// formed as `"{store_name}_{entity_name}"`.
    ///
    /// 
    ///
    /// # Type Parameters
    /// * `K` - The primary key type
    /// * `E` - The entity type
    ///
    /// # Errors
    /// Returns an error if the store is not open, the database cannot be
    /// opened/created, or the entity configuration is invalid.
    pub fn get_primary_index<K, E>(&mut self) -> Result<PrimaryIndex<'_, K, E>>
    where
        K: PrimaryKey + Ord + Send + Sync + 'static,
        E: Entity<PrimaryKey = K> + Clone + Send + Sync + 'static,
    {
        if !self.open {
            return Err(PersistError::StoreNotOpen);
        }

        let db_name =
            format!("{}_{}", self.config.store_name, E::entity_name());

        if !self.databases.contains_key(&db_name) {
            let mut db_config = DatabaseConfig::new();
            db_config.set_allow_create(self.config.allow_create);
            db_config.set_read_only(self.config.read_only);
            db_config.set_transactional(self.config.transactional);

            let db = self.env.open_database(None, &db_name, &db_config)?;
            self.databases.insert(db_name.clone(), db);
        }

        let db = self
            .databases
            .get(&db_name)
            .ok_or_else(|| PersistError::IndexNotAvailable(db_name.clone()))?;

        Ok(PrimaryIndex::new(db))
    }

    /// Gets or creates a secondary index for an entity type.
    ///
    /// The caller must have already called `get_primary_index` for the entity
    /// type and must pass the resulting `PrimaryIndex` here so the secondary
    /// index registration can be deposited into it.
    ///
    /// 
    ///
    /// # Type Parameters
    /// * `SK` - The secondary key type
    /// * `K` - The primary key type
    /// * `E` - The entity type
    ///
    /// # Errors
    /// Returns an error if the store is not open.
    pub fn open_secondary_index<SK, K, E, F>(
        &mut self,
        primary: &mut PrimaryIndex<'_, K, E>,
        extractor: F,
    ) -> Result<SecondaryIndex<SK, K, E>>
    where
        SK: Ord + Clone + Send + Sync + 'static,
        K: PrimaryKey + Ord + Send + Sync + 'static,
        E: Entity<PrimaryKey = K> + Clone + Send + Sync + 'static,
        F: Fn(&E) -> Option<SK> + Send + Sync + 'static,
    {
        if !self.open {
            return Err(PersistError::StoreNotOpen);
        }
        Ok(primary.open_secondary_index(extractor))
    }

    /// Returns the store name.
    ///
    /// 
    pub fn get_store_name(&self) -> &str {
        &self.config.store_name
    }

    /// Returns the store configuration.
    ///
    /// 
    pub fn get_config(&self) -> &StoreConfig {
        &self.config
    }

    /// Returns whether the store is currently open.
    pub fn is_open(&self) -> bool {
        self.open
    }

    /// Returns the underlying environment.
    pub fn get_environment(&self) -> &'env Environment {
        self.env
    }

    /// Returns a list of entity database names managed by this store.
    pub fn get_database_names(&self) -> Vec<String> {
        self.databases.keys().cloned().collect()
    }

    /// Eagerly evolves all entities in the store according to the given
    /// mutations, optionally filtered to specific entity class names by the
    /// `EvolveConfig`.
    ///
    /// For every entity database managed by this store the method iterates
    /// all records and applies, in priority order:
    ///
    /// 1. **Deleter** — if a class-level deleter mutation is registered for the
    ///    entity class name at version 0 (the "current" delete marker), the
    ///    record is deleted from the database.
    /// 2. **Converter** — if a class-level converter mutation is registered,
    ///    the raw bytes are transformed by the conversion function.  If the
    ///    function returns `None` the record is deleted.
    ///
    /// The method returns cumulative [`EvolveStats`] describing how many
    /// records were read and how many were re-written.
    ///
    /// 
    ///
    /// # Errors
    /// Returns an error if the store is not open or if any database operation
    /// fails.
    pub fn evolve(
        &mut self,
        mutations: &Mutations,
        config: &EvolveConfig,
    ) -> Result<EvolveStats> {
        if !self.open {
            return Err(PersistError::StoreNotOpen);
        }

        let mut stats = EvolveStats::new();

        // Collect (db_name, entity_class_name) pairs we want to process.
        // The convention used by get_primary_index is "{store_name}_{entity_name}".
        let store_prefix = format!("{}_", self.config.store_name);
        let db_names: Vec<String> = self.databases.keys().cloned().collect();

        for db_name in &db_names {
            // Derive the entity class name from the database name.
            let entity_class = if let Some(suffix) = db_name.strip_prefix(&store_prefix) {
                suffix.to_string()
            } else {
                db_name.clone()
            };

            // Skip classes not targeted by the config.
            if !config.should_evolve(&entity_class) {
                continue;
            }

            let db = match self.databases.get(db_name) {
                Some(d) => d,
                None => continue,
            };

            let (n_read, n_converted) =
                evolve_database(db, &entity_class, mutations, config)?;
            stats.add(n_read, n_converted);
        }

        Ok(stats)
    }

    /// Closes the entity store and all of its databases.
    ///
    /// 
    ///
    /// # Errors
    /// Returns an error if the store is already closed or if any database
    /// close operation fails.
    pub fn close(&mut self) -> Result<()> {
        if !self.open {
            return Err(PersistError::StoreNotOpen);
        }

        // Close all databases
        let mut close_errors = Vec::new();
        for (name, db) in self.databases.drain() {
            if let Err(e) = db.close() {
                close_errors.push(format!("{}: {}", name, e));
            }
        }

        self.open = false;

        if !close_errors.is_empty() {
            return Err(PersistError::DatabaseError(
                noxu_db::NoxuError::OperationNotAllowed(format!(
                    "errors closing databases: {}",
                    close_errors.join(", ")
                )),
            ));
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Internal helper: evolve a single database
// ---------------------------------------------------------------------------

/// Iterates every record in `db`, applies mutations for `entity_class`, and
/// returns `(n_read, n_converted)`.
///
/// For each record:
/// * If a class-level **Deleter** exists at version 0, the record is deleted.
/// * If a class-level **Converter** exists at version 0:
///   - `convert()` returning `Some(new_bytes)` → re-write with new bytes.
///   - `convert()` returning `None` → delete the record.
///
/// Version 0 is used as the sentinel "current schema version" for eager
/// evolution because the store does not currently persist per-record version
/// metadata.  A full implementation would store the schema version alongside
/// each record and look up mutations by that version.
fn evolve_database(
    db: &Database,
    entity_class: &str,
    mutations: &Mutations,
    _config: &EvolveConfig,
) -> Result<(u64, u64)> {
    let cm = mutations.get_mutations_for_class(entity_class, 0);
    if cm.is_empty() {
        // Nothing to do for this class.
        return Ok((0, 0));
    }

    let has_deleter = cm.deleter.is_some();
    let converter = cm.converter;

    let mut n_converted: u64 = 0;

    // Collect all (key_bytes, data_bytes) pairs first so we can mutate the
    // database without holding a cursor open.  The public Cursor API does not
    // expose key bytes during iteration, so we use Database::scan_all_kv
    // which drops into the lower-level CursorImpl layer directly.
    let records = db.scan_all_kv()?;

    let n_read = records.len() as u64;

    for (key_bytes, data_bytes) in records {
        let key_entry_w = DatabaseEntry::from_vec(key_bytes.clone());

        if has_deleter {
            // Deleter: drop the record entirely.
            db.delete(None, &key_entry_w)?;
            n_converted += 1;
            continue;
        }

        if let Some(conv) = converter {
            match conv.convert(&data_bytes) {
                Some(new_bytes) => {
                    let new_data = DatabaseEntry::from_vec(new_bytes);
                    db.put(None, &key_entry_w, &new_data)?;
                    n_converted += 1;
                }
                None => {
                    // Converter signals deletion by returning None.
                    db.delete(None, &key_entry_w)?;
                    n_converted += 1;
                }
            }
        }
    }

    Ok((n_read, n_converted))
}

impl Drop for EntityStore<'_> {
    fn drop(&mut self) {
        if self.open {
            let _ = self.close();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::Entity;
    use crate::entity_serializer::EntitySerializer;
    use noxu_db::EnvironmentConfig;
    use tempfile::TempDir;

    // --- Test entity and serializer (duplicated here for self-contained tests) ---

    #[derive(Clone, Debug, PartialEq)]
    struct User {
        id: u64,
        name: String,
        email: String,
    }

    impl Entity for User {
        type PrimaryKey = u64;

        fn primary_key(&self) -> &u64 {
            &self.id
        }

        fn entity_name() -> &'static str {
            "User"
        }
    }

    struct UserSerializer;

    impl EntitySerializer<User> for UserSerializer {
        fn serialize(&self, entity: &User) -> Result<Vec<u8>> {
            let mut buf = Vec::new();
            buf.extend_from_slice(&entity.id.to_be_bytes());
            let name_bytes = entity.name.as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
            buf.extend_from_slice(name_bytes);
            let email_bytes = entity.email.as_bytes();
            buf.extend_from_slice(&(email_bytes.len() as u32).to_be_bytes());
            buf.extend_from_slice(email_bytes);
            Ok(buf)
        }

        fn deserialize(&self, bytes: &[u8]) -> Result<User> {
            if bytes.len() < 12 {
                return Err(PersistError::SerializationError(
                    "not enough bytes for User".to_string(),
                ));
            }
            let id = u64::from_be_bytes([
                bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5],
                bytes[6], bytes[7],
            ]);
            let name_len =
                u32::from_be_bytes([bytes[8], bytes[9], bytes[10], bytes[11]])
                    as usize;
            let name_start = 12;
            let name_end = name_start + name_len;
            if bytes.len() < name_end + 4 {
                return Err(PersistError::SerializationError(
                    "not enough bytes for User name/email".to_string(),
                ));
            }
            let name = String::from_utf8(bytes[name_start..name_end].to_vec())
                .map_err(|e| {
                    PersistError::SerializationError(format!("bad name: {}", e))
                })?;
            let email_len = u32::from_be_bytes([
                bytes[name_end],
                bytes[name_end + 1],
                bytes[name_end + 2],
                bytes[name_end + 3],
            ]) as usize;
            let email_start = name_end + 4;
            let email_end = email_start + email_len;
            if bytes.len() < email_end {
                return Err(PersistError::SerializationError(
                    "not enough bytes for User email".to_string(),
                ));
            }
            let email =
                String::from_utf8(bytes[email_start..email_end].to_vec())
                    .map_err(|e| {
                        PersistError::SerializationError(format!(
                            "bad email: {}",
                            e
                        ))
                    })?;
            Ok(User { id, name, email })
        }
    }

    #[derive(Clone, Debug, PartialEq)]
    struct Product {
        sku: String,
        name: String,
        price_cents: u32,
    }

    impl Entity for Product {
        type PrimaryKey = String;

        fn primary_key(&self) -> &String {
            &self.sku
        }

        fn entity_name() -> &'static str {
            "Product"
        }
    }

    struct ProductSerializer;

    impl EntitySerializer<Product> for ProductSerializer {
        fn serialize(&self, entity: &Product) -> Result<Vec<u8>> {
            let mut buf = Vec::new();
            let sku_bytes = entity.sku.as_bytes();
            buf.extend_from_slice(&(sku_bytes.len() as u32).to_be_bytes());
            buf.extend_from_slice(sku_bytes);
            let name_bytes = entity.name.as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as u32).to_be_bytes());
            buf.extend_from_slice(name_bytes);
            buf.extend_from_slice(&entity.price_cents.to_be_bytes());
            Ok(buf)
        }

        fn deserialize(&self, bytes: &[u8]) -> Result<Product> {
            if bytes.len() < 4 {
                return Err(PersistError::SerializationError(
                    "not enough bytes".to_string(),
                ));
            }
            let mut pos = 0;
            let sku_len = u32::from_be_bytes([
                bytes[pos],
                bytes[pos + 1],
                bytes[pos + 2],
                bytes[pos + 3],
            ]) as usize;
            pos += 4;
            let sku = String::from_utf8(bytes[pos..pos + sku_len].to_vec())
                .map_err(|e| PersistError::SerializationError(e.to_string()))?;
            pos += sku_len;
            let name_len = u32::from_be_bytes([
                bytes[pos],
                bytes[pos + 1],
                bytes[pos + 2],
                bytes[pos + 3],
            ]) as usize;
            pos += 4;
            let name = String::from_utf8(bytes[pos..pos + name_len].to_vec())
                .map_err(|e| {
                PersistError::SerializationError(e.to_string())
            })?;
            pos += name_len;
            let price_cents = u32::from_be_bytes([
                bytes[pos],
                bytes[pos + 1],
                bytes[pos + 2],
                bytes[pos + 3],
            ]);
            Ok(Product { sku, name, price_cents })
        }
    }

    fn temp_env() -> (TempDir, Environment) {
        let temp_dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true);
        let env = Environment::open(env_config).unwrap();
        (temp_dir, env)
    }

    #[test]
    fn test_open_store() {
        let (_td, env) = temp_env();
        let config = StoreConfig::new("test_store").with_allow_create(true);
        let store = EntityStore::open(&env, config).unwrap();
        assert!(store.is_open());
        assert_eq!(store.get_store_name(), "test_store");
    }

    #[test]
    fn test_close_store() {
        let (_td, env) = temp_env();
        let config = StoreConfig::new("test_store").with_allow_create(true);
        let mut store = EntityStore::open(&env, config).unwrap();
        store.close().unwrap();
        assert!(!store.is_open());
    }

    #[test]
    fn test_close_twice_fails() {
        let (_td, env) = temp_env();
        let config = StoreConfig::new("test_store").with_allow_create(true);
        let mut store = EntityStore::open(&env, config).unwrap();
        store.close().unwrap();
        let result = store.close();
        assert!(result.is_err());
    }

    #[test]
    fn test_get_primary_index() {
        let (_td, env) = temp_env();
        let config = StoreConfig::new("test_store").with_allow_create(true);
        let mut store = EntityStore::open(&env, config).unwrap();

        let index: PrimaryIndex<u64, User> = store.get_primary_index().unwrap();
        assert_eq!(index.count().unwrap(), 0);
    }

    #[test]
    fn test_get_primary_index_creates_database() {
        let (_td, env) = temp_env();
        let config = StoreConfig::new("mystore").with_allow_create(true);
        let mut store = EntityStore::open(&env, config).unwrap();

        let _index: PrimaryIndex<u64, User> =
            store.get_primary_index().unwrap();

        let db_names = store.get_database_names();
        assert!(db_names.contains(&"mystore_User".to_string()));
    }

    #[test]
    fn test_store_crud_operations() {
        let (_td, env) = temp_env();
        let config = StoreConfig::new("store").with_allow_create(true);
        let mut store = EntityStore::open(&env, config).unwrap();
        let ser = UserSerializer;

        let index: PrimaryIndex<u64, User> = store.get_primary_index().unwrap();

        // Create
        let user = User {
            id: 1,
            name: "Alice".to_string(),
            email: "alice@example.com".to_string(),
        };
        index.put(&ser, &user).unwrap();

        // Read
        let found = index.get(&ser, &1u64).unwrap().unwrap();
        assert_eq!(found, user);

        // Update
        let updated = User {
            id: 1,
            name: "Alice Updated".to_string(),
            email: "alice.new@example.com".to_string(),
        };
        index.put(&ser, &updated).unwrap();
        let found = index.get(&ser, &1u64).unwrap().unwrap();
        assert_eq!(found.name, "Alice Updated");

        // Delete
        let deleted = index.delete(&1u64).unwrap();
        assert!(deleted);
        assert_eq!(index.get(&ser, &1u64).unwrap(), None);
    }

    #[test]
    fn test_multiple_entity_types() {
        let (_td, env) = temp_env();
        let config = StoreConfig::new("store").with_allow_create(true);
        let mut store = EntityStore::open(&env, config).unwrap();

        let user_ser = UserSerializer;
        let product_ser = ProductSerializer;

        // Insert users
        {
            let user_index: PrimaryIndex<u64, User> =
                store.get_primary_index().unwrap();
            user_index
                .put(
                    &user_ser,
                    &User {
                        id: 1,
                        name: "Alice".to_string(),
                        email: "alice@test.com".to_string(),
                    },
                )
                .unwrap();
        }

        // Insert products
        {
            let product_index: PrimaryIndex<String, Product> =
                store.get_primary_index().unwrap();
            product_index
                .put(
                    &product_ser,
                    &Product {
                        sku: "SKU-001".to_string(),
                        name: "Widget".to_string(),
                        price_cents: 999,
                    },
                )
                .unwrap();
        }

        // Verify both
        {
            let user_index: PrimaryIndex<u64, User> =
                store.get_primary_index().unwrap();
            let found_user = user_index.get(&user_ser, &1u64).unwrap().unwrap();
            assert_eq!(found_user.name, "Alice");
        }
        {
            let product_index: PrimaryIndex<String, Product> =
                store.get_primary_index().unwrap();
            let found_product = product_index
                .get(&product_ser, &"SKU-001".to_string())
                .unwrap()
                .unwrap();
            assert_eq!(found_product.price_cents, 999);
        }

        let db_names = store.get_database_names();
        assert_eq!(db_names.len(), 2);
    }

    #[test]
    fn test_get_primary_index_when_closed() {
        let (_td, env) = temp_env();
        let config = StoreConfig::new("store").with_allow_create(true);
        let mut store = EntityStore::open(&env, config).unwrap();
        store.close().unwrap();

        let result: std::result::Result<PrimaryIndex<u64, User>, _> =
            store.get_primary_index();
        assert!(result.is_err());
    }

    #[test]
    fn test_get_config() {
        let (_td, env) = temp_env();
        let config = StoreConfig::new("test_store")
            .with_allow_create(true)
            .with_read_only(false);
        let store = EntityStore::open(&env, config).unwrap();
        let cfg = store.get_config();
        assert_eq!(cfg.store_name, "test_store");
        assert!(cfg.allow_create);
        assert!(!cfg.read_only);
    }

    #[test]
    fn test_get_environment() {
        let (_td, env) = temp_env();
        let config = StoreConfig::new("store").with_allow_create(true);
        let store = EntityStore::open(&env, config).unwrap();
        assert!(store.get_environment().is_valid());
    }

    #[test]
    fn test_store_with_string_key() {
        let (_td, env) = temp_env();
        let config = StoreConfig::new("store").with_allow_create(true);
        let mut store = EntityStore::open(&env, config).unwrap();
        let ser = ProductSerializer;

        let index: PrimaryIndex<String, Product> =
            store.get_primary_index().unwrap();

        let product = Product {
            sku: "ABC-123".to_string(),
            name: "Gadget".to_string(),
            price_cents: 1999,
        };
        index.put(&ser, &product).unwrap();

        let found = index.get(&ser, &"ABC-123".to_string()).unwrap().unwrap();
        assert_eq!(found, product);
    }

    #[test]
    fn test_store_iteration() {
        let (_td, env) = temp_env();
        let config = StoreConfig::new("store").with_allow_create(true);
        let mut store = EntityStore::open(&env, config).unwrap();
        let ser = UserSerializer;

        let index: PrimaryIndex<u64, User> = store.get_primary_index().unwrap();

        for i in 1..=5 {
            index
                .put(
                    &ser,
                    &User {
                        id: i,
                        name: format!("User{}", i),
                        email: format!("user{}@example.com", i),
                    },
                )
                .unwrap();
        }

        let entities: Vec<User> = index
            .entities(&ser)
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(entities.len(), 5);

        // Verify ordering
        for (i, user) in entities.iter().enumerate() {
            assert_eq!(user.id, (i + 1) as u64);
        }
    }

    // -----------------------------------------------------------------------
    // EntityStore::evolve tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_evolve_empty_mutations_returns_zero_stats() {
        let (_td, env) = temp_env();
        let config = StoreConfig::new("store").with_allow_create(true);
        let mut store = EntityStore::open(&env, config).unwrap();
        let ser = UserSerializer;

        let index: PrimaryIndex<u64, User> = store.get_primary_index().unwrap();
        index.put(&ser, &User { id: 1, name: "A".into(), email: "a@a.com".into() }).unwrap();
        drop(index);

        let mutations = crate::evolve::Mutations::new();
        let evolve_cfg = crate::evolve::EvolveConfig::new();
        let stats = store.evolve(&mutations, &evolve_cfg).unwrap();
        // No mutations registered → nothing read, nothing converted.
        assert_eq!(stats.n_read(), 0);
        assert_eq!(stats.n_converted(), 0);
    }

    #[test]
    fn test_evolve_converter_transforms_records() {
        use crate::evolve::{Converter, EvolveConfig, Mutations};

        let (_td, env) = temp_env();
        let config = StoreConfig::new("store").with_allow_create(true);
        let mut store = EntityStore::open(&env, config).unwrap();
        let ser = UserSerializer;

        // Insert three users.
        {
            let index: PrimaryIndex<u64, User> = store.get_primary_index().unwrap();
            for i in 1u64..=3 {
                index
                    .put(
                        &ser,
                        &User {
                            id: i,
                            name: format!("User{}", i),
                            email: format!("u{}@x.com", i),
                        },
                    )
                    .unwrap();
            }
        }

        // Register a converter for "User" at version 0 that appends a zero byte
        // (a trivial structural change for test purposes).
        let mut mutations = Mutations::new();
        mutations.add_converter(Converter::for_class("User", 0, |b: &[u8]| {
            let mut out = b.to_vec();
            out.push(0xFF); // append sentinel to detect conversion
            Some(out)
        }));

        let evolve_cfg = EvolveConfig::new();
        let stats = store.evolve(&mutations, &evolve_cfg).unwrap();
        assert_eq!(stats.n_read(), 3);
        assert_eq!(stats.n_converted(), 3);
    }

    #[test]
    fn test_evolve_deleter_removes_records() {
        use crate::evolve::{Deleter, EvolveConfig, Mutations};

        let (_td, env) = temp_env();
        let config = StoreConfig::new("store").with_allow_create(true);
        let mut store = EntityStore::open(&env, config).unwrap();
        let ser = UserSerializer;

        {
            let index: PrimaryIndex<u64, User> = store.get_primary_index().unwrap();
            for i in 1u64..=2 {
                index
                    .put(
                        &ser,
                        &User {
                            id: i,
                            name: format!("X{}", i),
                            email: format!("x{}@x.com", i),
                        },
                    )
                    .unwrap();
            }
        }

        let mut mutations = Mutations::new();
        mutations.add_deleter(Deleter::for_class("User", 0));

        let evolve_cfg = EvolveConfig::new();
        let stats = store.evolve(&mutations, &evolve_cfg).unwrap();
        assert_eq!(stats.n_read(), 2);
        assert_eq!(stats.n_converted(), 2);

        // Records should be gone.
        {
            let index: PrimaryIndex<u64, User> = store.get_primary_index().unwrap();
            assert_eq!(index.count().unwrap(), 0);
        }
    }

    #[test]
    fn test_evolve_config_class_filter_skips_unmatched() {
        use crate::evolve::{Converter, EvolveConfig, Mutations};

        let (_td, env) = temp_env();
        let config = StoreConfig::new("store").with_allow_create(true);
        let mut store = EntityStore::open(&env, config).unwrap();
        let ser = UserSerializer;

        {
            let index: PrimaryIndex<u64, User> = store.get_primary_index().unwrap();
            index
                .put(
                    &ser,
                    &User { id: 1, name: "A".into(), email: "a@a.com".into() },
                )
                .unwrap();
        }

        let mut mutations = Mutations::new();
        mutations.add_converter(Converter::for_class("User", 0, |b: &[u8]| Some(b.to_vec())));

        // Config targets a *different* class → User should be skipped.
        let evolve_cfg = EvolveConfig::new().with_class_to_evolve("SomeOtherClass");
        let stats = store.evolve(&mutations, &evolve_cfg).unwrap();
        assert_eq!(stats.n_read(), 0);
        assert_eq!(stats.n_converted(), 0);
    }

    #[test]
    fn test_evolve_on_closed_store_returns_error() {
        use crate::evolve::{EvolveConfig, Mutations};

        let (_td, env) = temp_env();
        let config = StoreConfig::new("store").with_allow_create(true);
        let mut store = EntityStore::open(&env, config).unwrap();
        store.close().unwrap();

        let mutations = Mutations::new();
        let evolve_cfg = EvolveConfig::new();
        let result = store.evolve(&mutations, &evolve_cfg);
        assert!(result.is_err());
    }

    #[test]
    fn test_drop_closes_store() {
        let (_td, env) = temp_env();
        let config = StoreConfig::new("store").with_allow_create(true);
        {
            let mut store = EntityStore::open(&env, config).unwrap();
            let _: PrimaryIndex<u64, User> = store.get_primary_index().unwrap();
            // store drops here, should close databases
        }
        // If we get here without a panic, drop worked correctly
    }
}
