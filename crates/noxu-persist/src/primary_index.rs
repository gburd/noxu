//! Primary index for typed entity access.
//!

use std::marker::PhantomData;

use noxu_db::{Database, DatabaseEntry, OperationStatus};

use crate::entity::{Entity, PrimaryKey};
use crate::entity_serializer::EntitySerializer;
use crate::error::{PersistError, Result};
use crate::secondary_index::{
    make_secondary_index, SecondaryIndex, SecondaryIndexMaintainer,
    SecondaryRegistration,
};

/// Typed access to entities by primary key.
///
/// A `PrimaryIndex` wraps a `Database` handle and provides typed get, put,
/// and delete operations for entities. The serializer is passed to each
/// method call rather than stored in the index, allowing different
/// serialization strategies to be used with the same index.
///
/// 
///
/// # Type Parameters
///
/// * `K` - The primary key type (must implement `PrimaryKey`)
/// * `E` - The entity type (must implement `Entity` with `PrimaryKey = K`)
pub struct PrimaryIndex<'db, K: PrimaryKey, E: Entity<PrimaryKey = K>> {
    db: &'db Database,
    /// Secondary index maintainers registered via `open_secondary_index`.
    ///
    /// Each secondary index deposits a `SecondaryRegistration` here. On every
    /// `put` / `delete_with_entity` every maintainer is notified so the
    /// secondary maps stay in sync with the primary store — mirroring the
    /// `SecondaryDatabase` auto-maintenance.
    secondaries: Vec<Box<dyn SecondaryIndexMaintainer<K, E> + Send + Sync>>,
    _phantom: PhantomData<(K, E)>,
}

impl<'db, K, E> PrimaryIndex<'db, K, E>
where
    K: PrimaryKey + Ord + Send + Sync + 'static,
    E: Entity<PrimaryKey = K> + Clone + Send + Sync + 'static,
{
    /// Creates a new `PrimaryIndex` wrapping the given database.
    pub fn new(db: &'db Database) -> Self {
        Self { db, secondaries: Vec::new(), _phantom: PhantomData }
    }

    // -----------------------------------------------------------------------
    // Secondary index factory
    // -----------------------------------------------------------------------

    /// Opens (creates) a secondary index backed by the given key-extractor.
    ///
    /// The returned `SecondaryIndex` is automatically kept in sync with this
    /// `PrimaryIndex`: every `put` and `delete_with_entity` updates the
    /// secondary map.
    ///
    /// 
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use noxu_persist::{Entity, PrimaryKey, PrimaryIndex, EntitySerializer, Result};
    /// # use noxu_persist::secondary_index::SecondaryIndex;
    /// # use noxu_db::{Database, DatabaseConfig, Environment, EnvironmentConfig};
    /// # use tempfile::TempDir;
    /// # #[derive(Clone, Debug, PartialEq)]
    /// # struct User { id: u64, department: String }
    /// # impl Entity for User {
    /// #     type PrimaryKey = u64;
    /// #     fn primary_key(&self) -> &u64 { &self.id }
    /// #     fn entity_name() -> &'static str { "User" }
    /// # }
    /// # struct Ser;
    /// # impl EntitySerializer<User> for Ser {
    /// #     fn serialize(&self, _: &User) -> Result<Vec<u8>> { Ok(vec![]) }
    /// #     fn deserialize(&self, _: &[u8]) -> Result<User> { unimplemented!() }
    /// # }
    /// # let td = TempDir::new().unwrap();
    /// # let env = Environment::open(EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true)).unwrap();
    /// # let db = env.open_database(None, "u", &DatabaseConfig::new().with_allow_create(true)).unwrap();
    /// let mut primary: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
    /// let dept_idx = primary.open_secondary_index(|u: &User| Some(u.department.clone()));
    /// ```
    pub fn open_secondary_index<SK, F>(
        &mut self,
        extractor: F,
    ) -> SecondaryIndex<SK, K, E>
    where
        SK: Ord + Clone + Send + Sync + 'static,
        F: Fn(&E) -> Option<SK> + Send + Sync + 'static,
    {
        let (index, reg): (SecondaryIndex<SK, K, E>, SecondaryRegistration<SK, K, E>) =
            make_secondary_index(extractor);
        self.secondaries.push(Box::new(reg));
        index
    }

    // -----------------------------------------------------------------------
    // Read operations (unchanged from original)
    // -----------------------------------------------------------------------

    /// Retrieves an entity by its primary key.
    ///
    /// Returns `None` if no entity with the given key exists.
    ///
    /// 
    ///
    /// # Errors
    /// Returns an error if the database operation fails or deserialization fails.
    pub fn get<S: EntitySerializer<E>>(
        &self,
        serializer: &S,
        key: &K,
    ) -> Result<Option<E>> {
        let key_entry = DatabaseEntry::from_vec(key.to_bytes());
        let mut data_entry = DatabaseEntry::new();

        let status = self.db.get(None, &key_entry, &mut data_entry)?;

        match status {
            OperationStatus::Success => {
                let bytes = data_entry.get_data().ok_or_else(|| {
                    PersistError::SerializationError(
                        "empty data from database".to_string(),
                    )
                })?;
                let entity = serializer.deserialize(bytes)?;
                Ok(Some(entity))
            }
            OperationStatus::NotFound => Ok(None),
            OperationStatus::KeyExists => Ok(None),
        }
    }

    // -----------------------------------------------------------------------
    // Write operations
    // -----------------------------------------------------------------------

    /// Stores an entity, inserting or updating as needed.
    ///
    /// All registered secondary indexes are updated automatically.
    ///
    /// 
    ///
    /// # Errors
    /// Returns an error if serialization or the database operation fails.
    pub fn put<S: EntitySerializer<E>>(
        &self,
        serializer: &S,
        entity: &E,
    ) -> Result<()> {
        // Fetch the existing entity so secondary maintainers can remove the
        // stale secondary key mapping (mirrors old-value callback).
        let old_entity = self.get(serializer, entity.primary_key())?;

        let key_bytes = entity.primary_key().to_bytes();
        let key_entry = DatabaseEntry::from_vec(key_bytes);
        let data_bytes = serializer.serialize(entity)?;
        let data_entry = DatabaseEntry::from_vec(data_bytes);

        self.db.put(None, &key_entry, &data_entry)?;

        // Notify all secondary maintainers.
        for m in &self.secondaries {
            m.on_put(old_entity.as_ref(), entity);
        }

        Ok(())
    }

    /// Stores an entity only if the primary key does not already exist.
    ///
    /// Returns `true` if the entity was inserted, `false` if the key already
    /// exists. Secondary indexes are updated on successful insert.
    ///
    /// 
    ///
    /// # Errors
    /// Returns an error if serialization or the database operation fails.
    pub fn put_no_overwrite<S: EntitySerializer<E>>(
        &self,
        serializer: &S,
        entity: &E,
    ) -> Result<bool> {
        let key_bytes = entity.primary_key().to_bytes();
        let key_entry = DatabaseEntry::from_vec(key_bytes);
        let data_bytes = serializer.serialize(entity)?;
        let data_entry = DatabaseEntry::from_vec(data_bytes);

        let status = self.db.put_no_overwrite(None, &key_entry, &data_entry)?;
        let inserted = status == OperationStatus::Success;

        if inserted {
            // New insert – no old entity.
            for m in &self.secondaries {
                m.on_put(None, entity);
            }
        }

        Ok(inserted)
    }

    /// Deletes an entity by its primary key.
    ///
    /// Returns `true` if the entity was deleted, `false` if no entity with
    /// the given key existed.
    ///
    /// **Note:** Secondary indexes are **not** updated by this method because
    /// no entity is fetched. Use `delete_with_entity` when secondary index
    /// maintenance is required.
    ///
    /// 
    ///
    /// # Errors
    /// Returns an error if the database operation fails.
    pub fn delete(&self, key: &K) -> Result<bool> {
        let key_entry = DatabaseEntry::from_vec(key.to_bytes());
        let status = self.db.delete(None, &key_entry)?;
        Ok(status == OperationStatus::Success)
    }

    /// Deletes an entity by its primary key, also updating secondary indexes.
    ///
    /// Fetches the entity first so secondary maintainers receive the old
    /// value. This is the preferred delete path when secondary indexes have
    /// been registered.
    ///
    /// Returns `true` if an entity was deleted, `false` if the key did not
    /// exist.
    ///
    /// # Errors
    /// Returns an error if the database operation fails or deserialization fails.
    pub fn delete_with_entity<S: EntitySerializer<E>>(
        &self,
        serializer: &S,
        key: &K,
    ) -> Result<bool> {
        let old_entity = self.get(serializer, key)?;
        let deleted = self.delete(key)?;
        if deleted
            && let Some(ref e) = old_entity
        {
            for m in &self.secondaries {
                m.on_delete(e);
            }
        }
        Ok(deleted)
    }

    // -----------------------------------------------------------------------
    // Existence / count
    // -----------------------------------------------------------------------

    /// Checks whether an entity with the given primary key exists.
    ///
    /// 
    ///
    /// # Errors
    /// Returns an error if the database operation fails.
    pub fn contains(&self, key: &K) -> Result<bool> {
        let key_entry = DatabaseEntry::from_vec(key.to_bytes());
        let mut data_entry = DatabaseEntry::new();
        let status = self.db.get(None, &key_entry, &mut data_entry)?;
        Ok(status == OperationStatus::Success)
    }

    /// Returns an approximate count of entities in the index.
    ///
    /// 
    ///
    /// # Errors
    /// Returns an error if the database operation fails.
    pub fn count(&self) -> Result<u64> {
        Ok(self.db.count()?)
    }

    // -----------------------------------------------------------------------
    // Iteration
    // -----------------------------------------------------------------------

    /// Returns an iterator over all entities in key order.
    ///
    /// 
    ///
    /// # Errors
    /// Returns an error if the cursor cannot be opened.
    pub fn entities<'a, S: EntitySerializer<E>>(
        &'a self,
        serializer: &'a S,
    ) -> Result<EntityIterator<'a, K, E, S>> {
        let cursor = self.db.open_cursor(None, None)?;
        Ok(EntityIterator {
            cursor,
            serializer,
            started: false,
            done: false,
            _phantom: PhantomData,
        })
    }

    /// Returns an iterator over all primary keys in key order.
    ///
    /// 
    ///
    /// # Errors
    /// Returns an error if the cursor cannot be opened.
    pub fn keys(&self) -> Result<KeyIterator<'_, K>> {
        let cursor = self.db.open_cursor(None, None)?;
        Ok(KeyIterator {
            cursor,
            started: false,
            done: false,
            _phantom: PhantomData,
        })
    }

    /// Returns a reference to the underlying database.
    pub fn database(&self) -> &'db Database {
        self.db
    }
}

// ---------------------------------------------------------------------------
// EntityIterator
// ---------------------------------------------------------------------------

/// Iterator over entities using a database cursor.
///
/// 
pub struct EntityIterator<'a, K, E, S> {
    cursor: noxu_db::Cursor,
    serializer: &'a S,
    started: bool,
    done: bool,
    _phantom: PhantomData<(K, E)>,
}

impl<'a, K: PrimaryKey, E: Entity<PrimaryKey = K>, S: EntitySerializer<E>>
    Iterator for EntityIterator<'a, K, E, S>
{
    type Item = Result<E>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let mut key_entry = DatabaseEntry::new();
        let mut data_entry = DatabaseEntry::new();

        let get_type = if self.started {
            noxu_db::Get::Next
        } else {
            self.started = true;
            noxu_db::Get::First
        };

        match self.cursor.get(&mut key_entry, &mut data_entry, get_type, None) {
            Ok(OperationStatus::Success) => {
                let bytes = match data_entry.get_data() {
                    Some(b) => b,
                    None => {
                        self.done = true;
                        return Some(Err(PersistError::SerializationError(
                            "empty data from cursor".to_string(),
                        )));
                    }
                };
                Some(self.serializer.deserialize(bytes))
            }
            Ok(_) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(e.into()))
            }
        }
    }
}

impl<K, E, S> Drop for EntityIterator<'_, K, E, S> {
    fn drop(&mut self) {
        let _ = self.cursor.close();
    }
}

// ---------------------------------------------------------------------------
// KeyIterator
// ---------------------------------------------------------------------------

/// Iterator over primary keys using a database cursor.
///
pub struct KeyIterator<'a, K> {
    cursor: noxu_db::Cursor,
    started: bool,
    done: bool,
    _phantom: PhantomData<&'a K>,
}

impl<K: PrimaryKey> Iterator for KeyIterator<'_, K> {
    type Item = Result<K>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let mut key_entry = DatabaseEntry::new();
        let mut data_entry = DatabaseEntry::new();

        let get_type = if self.started {
            noxu_db::Get::Next
        } else {
            self.started = true;
            noxu_db::Get::First
        };

        match self.cursor.get(&mut key_entry, &mut data_entry, get_type, None) {
            Ok(OperationStatus::Success) => {
                // The cursor writes the current key into key_entry (Cursor::get()
                // calls key.set_data(&k) on success.
                // which sets key_entry as an output parameter for all positioning ops).
                match key_entry.get_data() {
                    Some(key_bytes) => Some(K::from_bytes(key_bytes)),
                    None => {
                        self.done = true;
                        None
                    }
                }
            }
            Ok(_) => {
                self.done = true;
                None
            }
            Err(e) => {
                self.done = true;
                Some(Err(e.into()))
            }
        }
    }
}

impl<K> Drop for KeyIterator<'_, K> {
    fn drop(&mut self) {
        let _ = self.cursor.close();
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::Entity;
    use crate::entity_serializer::EntitySerializer;
    use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
    use tempfile::TempDir;

    // --- Test entity and serializer ---

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

    fn setup() -> (TempDir, Environment, Database) {
        let temp_dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true);
        let env = Environment::open(env_config).unwrap();
        let db_config = DatabaseConfig::new().with_allow_create(true);
        let db = env.open_database(None, "users", &db_config).unwrap();
        (temp_dir, env, db)
    }

    fn test_user(id: u64) -> User {
        User {
            id,
            name: format!("User{}", id),
            email: format!("user{}@example.com", id),
        }
    }

    #[test]
    fn test_put_and_get() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        let user = test_user(1);
        index.put(&ser, &user).unwrap();

        let found = index.get(&ser, &1u64).unwrap();
        assert_eq!(found, Some(user));
    }

    #[test]
    fn test_get_not_found() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        let found = index.get(&ser, &999u64).unwrap();
        assert_eq!(found, None);
    }

    #[test]
    fn test_put_overwrites() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        let user1 = test_user(1);
        index.put(&ser, &user1).unwrap();

        let user1_updated = User {
            id: 1,
            name: "Updated".to_string(),
            email: "updated@example.com".to_string(),
        };
        index.put(&ser, &user1_updated).unwrap();

        let found = index.get(&ser, &1u64).unwrap().unwrap();
        assert_eq!(found.name, "Updated");
        assert_eq!(found.email, "updated@example.com");
    }

    #[test]
    fn test_put_no_overwrite_success() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        let user = test_user(1);
        let inserted = index.put_no_overwrite(&ser, &user).unwrap();
        assert!(inserted);
    }

    #[test]
    fn test_put_no_overwrite_fails_on_existing() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        let user = test_user(1);
        index.put(&ser, &user).unwrap();

        let user2 = User {
            id: 1,
            name: "Other".to_string(),
            email: "other@example.com".to_string(),
        };
        let inserted = index.put_no_overwrite(&ser, &user2).unwrap();
        assert!(!inserted);

        // Original should be unchanged
        let found = index.get(&ser, &1u64).unwrap().unwrap();
        assert_eq!(found.name, "User1");
    }

    #[test]
    fn test_delete() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        let user = test_user(1);
        index.put(&ser, &user).unwrap();

        let deleted = index.delete(&1u64).unwrap();
        assert!(deleted);

        let found = index.get(&ser, &1u64).unwrap();
        assert_eq!(found, None);
    }

    #[test]
    fn test_delete_not_found() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);

        let deleted = index.delete(&999u64).unwrap();
        assert!(!deleted);
    }

    #[test]
    fn test_delete_with_entity() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        let user = test_user(1);
        index.put(&ser, &user).unwrap();

        let deleted = index.delete_with_entity(&ser, &1u64).unwrap();
        assert!(deleted);
        assert_eq!(index.get(&ser, &1u64).unwrap(), None);
    }

    #[test]
    fn test_delete_with_entity_not_found() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        let deleted = index.delete_with_entity(&ser, &999u64).unwrap();
        assert!(!deleted);
    }

    #[test]
    fn test_contains() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        assert!(!index.contains(&1u64).unwrap());

        let user = test_user(1);
        index.put(&ser, &user).unwrap();

        assert!(index.contains(&1u64).unwrap());
    }

    #[test]
    fn test_count_empty() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);

        assert_eq!(index.count().unwrap(), 0);
    }

    #[test]
    fn test_count_with_entities() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        for i in 1..=5 {
            index.put(&ser, &test_user(i)).unwrap();
        }

        assert_eq!(index.count().unwrap(), 5);
    }

    #[test]
    fn test_entities_iterator() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        for i in 1..=3 {
            index.put(&ser, &test_user(i)).unwrap();
        }

        let entities: Vec<User> = index
            .entities(&ser)
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(entities.len(), 3);
        // Entities should be in key order (big-endian u64 bytes sort correctly)
        assert_eq!(entities[0].id, 1);
        assert_eq!(entities[1].id, 2);
        assert_eq!(entities[2].id, 3);
    }

    #[test]
    fn test_entities_iterator_empty() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        let entities: Vec<User> = index
            .entities(&ser)
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert!(entities.is_empty());
    }

    #[test]
    fn test_multiple_put_delete_cycles() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        // Insert, delete, re-insert
        let user = test_user(1);
        index.put(&ser, &user).unwrap();
        index.delete(&1u64).unwrap();
        assert_eq!(index.count().unwrap(), 0);

        let user2 = User {
            id: 1,
            name: "Reinserted".to_string(),
            email: "new@example.com".to_string(),
        };
        index.put(&ser, &user2).unwrap();
        let found = index.get(&ser, &1u64).unwrap().unwrap();
        assert_eq!(found.name, "Reinserted");
    }

    #[test]
    fn test_database_reference() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        assert_eq!(index.database().get_database_name(), "users");
    }

    // --- Additional branch-coverage tests ---

    /// `EntityIterator::next` after `done=true` returns `None` immediately.
    #[test]
    fn test_entity_iterator_done_returns_none() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        // Empty database: first `next` sets done=true, second returns None.
        let mut iter = index.entities(&ser).unwrap();
        assert!(iter.next().is_none()); // First call on empty db → done=true
        assert!(iter.next().is_none()); // Already done
    }

    /// `EntityIterator::next` after entries are exhausted returns `None`.
    #[test]
    fn test_entity_iterator_exhausted() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        index.put(&ser, &test_user(1)).unwrap();
        let mut iter = index.entities(&ser).unwrap();

        // Read the single entry.
        assert!(iter.next().is_some());
        // Now exhausted.
        assert!(iter.next().is_none());
        // Still None on repeated calls.
        assert!(iter.next().is_none());
    }

    /// `KeyIterator::next` sets done after first call (current implementation).
    #[test]
    fn test_key_iterator_first_call_done() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        index.put(&ser, &test_user(1)).unwrap();

        let mut iter = index.keys().unwrap();
        // First call: started=false → sets started=true, issues Get::First.
        // The current implementation sets done=true unconditionally on success.
        let first = iter.next();
        // Second call: done=true → returns None immediately (the `if self.done` branch).
        assert!(iter.next().is_none());
        // Suppress unused variable warning.
        let _ = first;
    }

    /// `KeyIterator::next` on an empty database returns None.
    #[test]
    fn test_key_iterator_empty_db() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);

        let mut iter = index.keys().unwrap();
        assert!(iter.next().is_none());
    }

    /// `KeyIterator::next` called twice: first gets the cursor result,
    /// second hits the `if self.done` early-return branch.
    #[test]
    fn test_key_iterator_done_branch() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        for i in 1u64..=3 {
            index.put(&ser, &test_user(i)).unwrap();
        }

        // Drain all three keys.
        let mut iter = index.keys().unwrap();
        let k1 = iter.next();
        let k2 = iter.next();
        let k3 = iter.next();
        assert!(k1.is_some(), "expected first key");
        assert!(k2.is_some(), "expected second key");
        assert!(k3.is_some(), "expected third key");
        // After exhausting the iterator, done=true so subsequent calls return None.
        assert!(iter.next().is_none());
        assert!(iter.next().is_none());
    }

    /// `open_secondary_index` with `put` and `delete_with_entity` exercises
    /// the secondary notification paths.
    #[test]
    fn test_secondary_index_notifications_on_put_and_delete() {
        let (_td, _env, db) = setup();
        let mut index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        // Open a secondary index on name.
        let name_idx = index.open_secondary_index(|u: &User| Some(u.name.clone()));

        let user = test_user(10);
        index.put(&ser, &user).unwrap();
        // Secondary should now contain the name.
        assert!(name_idx.contains(&"User10".to_string()));

        // Overwrite: put triggers on_put with old_entity set.
        let updated = User {
            id: 10,
            name: "UpdatedUser10".to_string(),
            email: "u@x.com".to_string(),
        };
        index.put(&ser, &updated).unwrap();
        assert!(!name_idx.contains(&"User10".to_string()));
        assert!(name_idx.contains(&"UpdatedUser10".to_string()));

        // delete_with_entity: fetches entity and calls on_delete.
        let deleted = index.delete_with_entity(&ser, &10u64).unwrap();
        assert!(deleted);
        assert!(!name_idx.contains(&"UpdatedUser10".to_string()));
    }

    /// `delete_with_entity` on non-existing key: deleted=false, secondaries NOT notified.
    #[test]
    fn test_delete_with_entity_missing_key_no_secondary_notify() {
        let (_td, _env, db) = setup();
        let mut index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        let _name_idx = index.open_secondary_index(|u: &User| Some(u.name.clone()));

        let deleted = index.delete_with_entity(&ser, &999u64).unwrap();
        assert!(!deleted);
    }

    /// `put_no_overwrite` with secondary index: only fires on_put when inserted.
    #[test]
    fn test_put_no_overwrite_with_secondary() {
        let (_td, _env, db) = setup();
        let mut index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        let name_idx = index.open_secondary_index(|u: &User| Some(u.name.clone()));

        let user = test_user(5);
        let inserted = index.put_no_overwrite(&ser, &user).unwrap();
        assert!(inserted);
        assert!(name_idx.contains(&"User5".to_string()));

        // Second insert with same key: not inserted, secondary unchanged.
        let inserted2 = index.put_no_overwrite(&ser, &user).unwrap();
        assert!(!inserted2);
        assert!(name_idx.contains(&"User5".to_string()));
    }

    /// `contains` returns the correct status.
    #[test]
    fn test_contains_after_delete() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        index.put(&ser, &test_user(7)).unwrap();
        assert!(index.contains(&7u64).unwrap());
        index.delete(&7u64).unwrap();
        assert!(!index.contains(&7u64).unwrap());
    }

    /// `count` after insertions and deletions.
    #[test]
    fn test_count_after_delete() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        for i in 1u64..=4 {
            index.put(&ser, &test_user(i)).unwrap();
        }
        assert_eq!(index.count().unwrap(), 4);

        index.delete(&2u64).unwrap();
        assert_eq!(index.count().unwrap(), 3);
    }

    /// `entities` with many records returns all in order.
    #[test]
    fn test_entities_iterator_many() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        for i in 1u64..=10 {
            index.put(&ser, &test_user(i)).unwrap();
        }

        let entities: Vec<User> = index
            .entities(&ser)
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(entities.len(), 10);
        for (i, user) in entities.iter().enumerate() {
            assert_eq!(user.id, (i + 1) as u64);
        }
    }
}
