//! Primary index for typed entity access.
//!

use std::marker::PhantomData;
use std::sync::Arc;

use noxu_db::{
    Cursor, Database, DatabaseEntry, Mutex, OperationStatus, Transaction,
};

use crate::entity::{Entity, PrimaryKey};
use crate::entity_serializer::EntitySerializer;
use crate::error::{PersistError, Result};
use crate::evolve::envelope;
use crate::evolve::mutations::Mutations;

/// Typed access to entities by primary key.
///
/// A `PrimaryIndex` wraps a primary [`Database`] (shared via
/// `Arc<Mutex<Database>>`) and provides typed get, put, and delete
/// operations for entities.  The serializer is passed to each method call
/// rather than stored in the index, allowing different serialization
/// strategies to be used with the same index.
///
/// # Secondary index maintenance
///
/// Secondary indexes are real, persistent, transactional
/// [`noxu_db::SecondaryDatabase`]s opened against this primary via
/// [`crate::EntityStore::open_secondary_index`] (the JE
/// `Store.openSecondaryDatabase` model).  They are maintained
/// **automatically** by the primary database's `put` / `delete` within the
/// active transaction — there is no in-memory side index to keep in sync
/// and no `on_put` / `on_delete` callback list on `PrimaryIndex`.
///
/// # Type Parameters
///
/// * `K` - The primary key type (must implement `PrimaryKey`)
/// * `E` - The entity type (must implement `Entity` with `PrimaryKey = K`)
pub struct PrimaryIndex<K: PrimaryKey, E: Entity<PrimaryKey = K>> {
    /// The primary database, owned via a shared `Arc<Mutex<Database>>` so a
    /// [`noxu_db::SecondaryDatabase`] can be opened against the *same*
    /// handle and its automatic-maintenance fan-out fires on every `put` /
    /// `delete` performed here.  Owning (rather than borrowing) the `Arc`
    /// keeps the index independent of the `EntityStore`'s borrow, so a
    /// secondary can be opened while the primary index is alive.
    db: Arc<Mutex<Database>>,
    /// Schema-evolution mutations.
    ///
    /// Plumbed in from `EntityStore` via [`PrimaryIndex::with_mutations`].
    /// On every `get` / iteration, the per-record class version peeled
    /// from the on-disk envelope and `mutations` are passed to
    /// [`EntitySerializer::deserialize_versioned`] so user serializers
    /// can do field-level evolution on read.  Defaults to a shared
    /// empty `Mutations`, which makes
    /// `deserialize_versioned`'s default impl behave identically to
    /// `deserialize`.
    mutations: Arc<Mutations>,
    _phantom: PhantomData<(K, E)>,
}

impl<K, E> PrimaryIndex<K, E>
where
    K: PrimaryKey + Ord + Send + Sync + 'static,
    E: Entity<PrimaryKey = K> + Clone + Send + Sync + 'static,
{
    /// Creates a new `PrimaryIndex` over the given shared database.
    ///
    /// The new index has no [`Mutations`] registered; field-level
    /// schema evolution will not be available on read.  Use
    /// [`Self::with_mutations`] to attach a mutations set.
    pub fn new(db: Arc<Mutex<Database>>) -> Self {
        Self::with_mutations(db, Arc::new(Mutations::new()))
    }

    /// Creates a new `PrimaryIndex` over the given shared database and
    /// remembering the mutations set for read-side evolution.
    pub fn with_mutations(
        db: Arc<Mutex<Database>>,
        mutations: Arc<Mutations>,
    ) -> Self {
        Self { db, mutations, _phantom: PhantomData }
    }

    /// Returns a clone of the registered mutations Arc.
    pub fn mutations(&self) -> &Arc<Mutations> {
        &self.mutations
    }

    /// Returns the shared primary database handle.
    ///
    /// Used by [`crate::EntityStore::open_secondary_index`] to open a
    /// [`noxu_db::SecondaryDatabase`] against the *same* primary handle
    /// these writes go through, so automatic maintenance fires.
    pub fn database_shared(&self) -> &Arc<Mutex<Database>> {
        &self.db
    }

    // -----------------------------------------------------------------------
    // Read operations
    // -----------------------------------------------------------------------

    /// Retrieves an entity by its primary key.
    ///
    /// Returns `None` if no entity with the given key exists.
    ///
    /// # Transactions
    ///
    /// Pass `Some(&txn)` to perform the read inside a user-managed
    /// transaction (acquires shared locks via the txn's locker).  Pass
    /// `None` for an auto-commit read (the historical pre-v1.5 default,
    /// which still works).  This is a breaking change vs. v1.4: the
    /// `txn` parameter is now leading-positional to mirror the
    /// `noxu_db::Database::{get,put,delete}` shape.
    ///
    /// # Errors
    /// Returns an error if the database operation fails or deserialization fails.
    pub fn get<S: EntitySerializer<E>>(
        &self,
        txn: Option<&Transaction>,
        serializer: &S,
        key: &K,
    ) -> Result<Option<E>> {
        let key_bytes = key.to_bytes();
        let db = self.db.lock();
        let found = match txn {
            Some(t) => db.get_in(t, &key_bytes)?,
            None => db.get(&key_bytes)?,
        };

        match found {
            Some(bytes) => {
                let entity = self.decode_record(&bytes, serializer)?;
                Ok(Some(entity))
            }
            None => Ok(None),
        }
    }

    /// Decodes a raw on-disk record into an entity, peeling the
    /// per-record class-version envelope and dispatching to
    /// [`EntitySerializer::deserialize_versioned`].
    ///
    /// Pre-v1.6 records had no envelope; the migration guide
    /// describes the dump-and-reload procedure.  We fail loudly
    /// (rather than silently misinterpreting old bytes) when the
    /// embedded class tag does not match `E::entity_name()`,
    /// **unless** a [`crate::evolve::Renamer::for_class`] mutation
    /// remaps the on-disk tag to `E::entity_name()`.
    fn decode_record<S: EntitySerializer<E>>(
        &self,
        bytes: &[u8],
        serializer: &S,
    ) -> Result<E> {
        let dec = envelope::decode(bytes)?;
        let expected_tag = E::entity_name();
        if dec.class_tag != expected_tag {
            // Look for a class-level renamer that maps the on-disk tag to
            // the current entity name.  Any version is accepted (we walk
            // the registered renamers).
            let renamed = self.mutations.renamers().any(|r| {
                r.field_name().is_none()
                    && r.class_name() == dec.class_tag
                    && r.new_name() == expected_tag
            });
            if !renamed {
                return Err(PersistError::SerializationError(format!(
                    "entity class tag mismatch: on-disk '{}' != \
                     expected '{}' (no Renamer registered)",
                    dec.class_tag, expected_tag,
                )));
            }
        }
        serializer.deserialize_versioned(
            dec.payload,
            dec.class_version,
            self.mutations.as_ref(),
        )
    }

    // -----------------------------------------------------------------------
    // Write operations
    // -----------------------------------------------------------------------

    /// Stores an entity, inserting or updating as needed.
    ///
    /// All secondary indexes opened against this primary are maintained
    /// **automatically and transactionally** by the underlying
    /// [`noxu_db::Database::put`] fan-out (the JE associate()-style hook).
    ///
    /// # Transactions
    ///
    /// Pass `Some(&txn)` to participate in an explicit transaction.  The
    /// primary-database write **and every secondary index update** commit /
    /// abort atomically with the txn — aborting rolls back both.  Pass
    /// `None` for auto-commit.
    ///
    /// # Errors
    /// Returns an error if serialization or the database operation fails.
    pub fn put<S: EntitySerializer<E>>(
        &self,
        txn: Option<&Transaction>,
        serializer: &S,
        entity: &E,
    ) -> Result<()> {
        let key_bytes = entity.primary_key().to_bytes();
        let payload = serializer.serialize(entity)?;
        let envelope_bytes =
            envelope::encode(E::class_version(), E::entity_name(), &payload)?;

        // The Database::put fan-out drives every registered SecondaryDatabase
        // under the same `txn`, so secondaries are atomic with the primary.
        let db = self.db.lock();
        match txn {
            Some(t) => db.put_in(t, &key_bytes, &envelope_bytes)?,
            None => db.put(&key_bytes, &envelope_bytes)?,
        }

        Ok(())
    }

    /// Stores an entity only if the primary key does not already exist.
    ///
    /// Returns `true` if the entity was inserted, `false` if the key already
    /// exists.  Secondary indexes are maintained automatically and
    /// transactionally on a successful insert.
    ///
    /// See [`PrimaryIndex::put`] for the transactional semantics.
    ///
    /// # Errors
    /// Returns an error if serialization or the database operation fails.
    pub fn put_no_overwrite<S: EntitySerializer<E>>(
        &self,
        txn: Option<&Transaction>,
        serializer: &S,
        entity: &E,
    ) -> Result<bool> {
        let key_bytes = entity.primary_key().to_bytes();
        let payload = serializer.serialize(entity)?;
        let envelope_bytes =
            envelope::encode(E::class_version(), E::entity_name(), &payload)?;

        let db = self.db.lock();
        let inserted = match txn {
            Some(t) => {
                db.put_no_overwrite_in(t, &key_bytes, &envelope_bytes)?
            }
            None => db.put_no_overwrite(&key_bytes, &envelope_bytes)?,
        };
        Ok(inserted)
    }

    /// Deletes an entity by its primary key.
    ///
    /// Returns `true` if the entity was deleted, `false` if no entity with
    /// the given key existed.
    ///
    /// Secondary indexes are maintained automatically and transactionally
    /// by the underlying [`noxu_db::Database::delete`] fan-out — unlike the
    /// historical in-memory design, this method now keeps secondaries
    /// consistent without a separate fetch.  [`Self::delete_with_entity`]
    /// remains for callers that also want the removed entity returned.
    ///
    /// # Transactions
    ///
    /// Pass `Some(&txn)` to participate in an explicit transaction; `None`
    /// for auto-commit.
    ///
    /// # Errors
    /// Returns an error if the database operation fails.
    pub fn delete(&self, txn: Option<&Transaction>, key: &K) -> Result<bool> {
        let key_bytes = key.to_bytes();
        let db = self.db.lock();
        let deleted = match txn {
            Some(t) => db.delete_in(t, &key_bytes)?,
            None => db.delete(&key_bytes)?,
        };
        Ok(deleted)
    }

    /// Deletes an entity by its primary key.
    ///
    /// Equivalent to [`Self::delete`] (secondary indexes are now maintained
    /// automatically by the primary delete); retained for source
    /// compatibility with the historical in-memory API where a separate
    /// fetch was needed to notify secondary maintainers.
    ///
    /// Returns `true` if an entity was deleted, `false` if the key did not
    /// exist.
    ///
    /// See [`PrimaryIndex::put`] for the transactional semantics.
    ///
    /// # Errors
    /// Returns an error if the database operation fails.
    pub fn delete_with_entity<S: EntitySerializer<E>>(
        &self,
        txn: Option<&Transaction>,
        _serializer: &S,
        key: &K,
    ) -> Result<bool> {
        self.delete(txn, key)
    }

    // -----------------------------------------------------------------------
    // Existence / count
    // -----------------------------------------------------------------------

    /// Checks whether an entity with the given primary key exists.
    ///
    /// Pass `Some(&txn)` to read inside a transaction; `None` for
    /// auto-commit.
    ///
    /// # Errors
    /// Returns an error if the database operation fails.
    pub fn contains(&self, txn: Option<&Transaction>, key: &K) -> Result<bool> {
        let key_bytes = key.to_bytes();
        let db = self.db.lock();
        let found = match txn {
            Some(t) => db.get_in(t, &key_bytes)?,
            None => db.get(&key_bytes)?,
        };
        Ok(found.is_some())
    }

    /// Returns an approximate count of entities in the index.
    ///
    ///
    ///
    /// # Errors
    /// Returns an error if the database operation fails.
    pub fn count(&self) -> Result<u64> {
        Ok(self.db.lock().count()?)
    }

    // -----------------------------------------------------------------------
    // Iteration
    // -----------------------------------------------------------------------

    /// Returns an iterator over all entities in key order.
    ///
    /// Pass `Some(&txn)` to iterate inside a transaction (cursor reads
    /// acquire shared locks via the txn locker); `None` for auto-commit.
    ///
    /// # Errors
    /// Returns an error if the cursor cannot be opened.
    pub fn entities<'a, S: EntitySerializer<E>>(
        &'a self,
        txn: Option<&'a Transaction>,
        serializer: &'a S,
    ) -> Result<EntityIterator<'a, K, E, S>> {
        let cursor = {
            let db = self.db.lock();
            match txn {
                Some(t) => db.open_cursor_in(t, None)?,
                None => db.open_cursor(None)?,
            }
        };
        Ok(EntityIterator {
            cursor,
            serializer,
            mutations: Arc::clone(&self.mutations),
            started: false,
            done: false,
            _phantom: PhantomData,
        })
    }

    /// Returns an iterator over all primary keys in key order.
    ///
    /// Pass `Some(&txn)` to iterate inside a transaction; `None` for
    /// auto-commit.
    ///
    /// # Errors
    /// Returns an error if the cursor cannot be opened.
    pub fn keys<'a>(
        &'a self,
        txn: Option<&'a Transaction>,
    ) -> Result<KeyIterator<'a, K>> {
        let cursor = {
            let db = self.db.lock();
            match txn {
                Some(t) => db.open_cursor_in(t, None)?,
                None => db.open_cursor(None)?,
            }
        };
        Ok(KeyIterator {
            cursor,
            started: false,
            done: false,
            _phantom: PhantomData,
        })
    }

    /// Returns the shared primary database handle (clone of the `Arc`).
    ///
    /// (Conceptually renamed from the historical `database()` returning a
    /// bare `&Database`; the index now owns a shared
    /// `Arc<Mutex<Database>>`.)
    pub fn database(&self) -> Arc<Mutex<Database>> {
        Arc::clone(&self.db)
    }
}

// ---------------------------------------------------------------------------
// EntityIterator
// ---------------------------------------------------------------------------

/// Iterator over entities using a database cursor.
///
///
pub struct EntityIterator<'a, K, E, S> {
    cursor: Cursor<'a>,
    serializer: &'a S,
    /// Schema-evolution mutations cloned from the parent
    /// [`PrimaryIndex`].  Passed to
    /// [`EntitySerializer::deserialize_versioned`] for each record so
    /// users can do field-level evolution while iterating.
    mutations: Arc<Mutations>,
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
                Some(decode_iter_record::<E, S>(
                    bytes,
                    self.serializer,
                    self.mutations.as_ref(),
                ))
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

/// Free function variant of `PrimaryIndex::decode_record` used by
/// `EntityIterator`, which cannot easily borrow the `PrimaryIndex` due
/// to lifetime constraints on the iterator itself.
fn decode_iter_record<E, S>(
    bytes: &[u8],
    serializer: &S,
    mutations: &Mutations,
) -> Result<E>
where
    E: Entity,
    S: EntitySerializer<E>,
{
    let dec = envelope::decode(bytes)?;
    let expected_tag = E::entity_name();
    if dec.class_tag != expected_tag {
        let renamed = mutations.renamers().any(|r| {
            r.field_name().is_none()
                && r.class_name() == dec.class_tag
                && r.new_name() == expected_tag
        });
        if !renamed {
            return Err(PersistError::SerializationError(format!(
                "entity class tag mismatch: on-disk '{}' != expected '{}' \
                 (no Renamer registered)",
                dec.class_tag, expected_tag,
            )));
        }
    }
    serializer.deserialize_versioned(dec.payload, dec.class_version, mutations)
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
    cursor: Cursor<'a>,
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

    fn setup() -> (TempDir, Environment, Arc<Mutex<Database>>) {
        let temp_dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true);
        let env = Environment::open(env_config).unwrap();
        let db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        let db = env.open_database(None, "users", &db_config).unwrap();
        (temp_dir, env, Arc::new(Mutex::new(db)))
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
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(Arc::clone(&db));
        let ser = UserSerializer;

        let user = test_user(1);
        index.put(None, &ser, &user).unwrap();

        let found = index.get(None, &ser, &1u64).unwrap();
        assert_eq!(found, Some(user));
    }

    #[test]
    fn test_get_not_found() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(Arc::clone(&db));
        let ser = UserSerializer;

        let found = index.get(None, &ser, &999u64).unwrap();
        assert_eq!(found, None);
    }

    #[test]
    fn test_put_overwrites() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(Arc::clone(&db));
        let ser = UserSerializer;

        let user1 = test_user(1);
        index.put(None, &ser, &user1).unwrap();

        let user1_updated = User {
            id: 1,
            name: "Updated".to_string(),
            email: "updated@example.com".to_string(),
        };
        index.put(None, &ser, &user1_updated).unwrap();

        let found = index.get(None, &ser, &1u64).unwrap().unwrap();
        assert_eq!(found.name, "Updated");
        assert_eq!(found.email, "updated@example.com");
    }

    #[test]
    fn test_put_no_overwrite_success() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(Arc::clone(&db));
        let ser = UserSerializer;

        let user = test_user(1);
        let inserted = index.put_no_overwrite(None, &ser, &user).unwrap();
        assert!(inserted);
    }

    #[test]
    fn test_put_no_overwrite_fails_on_existing() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(Arc::clone(&db));
        let ser = UserSerializer;

        let user = test_user(1);
        index.put(None, &ser, &user).unwrap();

        let user2 = User {
            id: 1,
            name: "Other".to_string(),
            email: "other@example.com".to_string(),
        };
        let inserted = index.put_no_overwrite(None, &ser, &user2).unwrap();
        assert!(!inserted);

        // Original should be unchanged
        let found = index.get(None, &ser, &1u64).unwrap().unwrap();
        assert_eq!(found.name, "User1");
    }

    #[test]
    fn test_delete() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(Arc::clone(&db));
        let ser = UserSerializer;

        let user = test_user(1);
        index.put(None, &ser, &user).unwrap();

        let deleted = index.delete(None, &1u64).unwrap();
        assert!(deleted);

        let found = index.get(None, &ser, &1u64).unwrap();
        assert_eq!(found, None);
    }

    #[test]
    fn test_delete_not_found() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(Arc::clone(&db));

        let deleted = index.delete(None, &999u64).unwrap();
        assert!(!deleted);
    }

    #[test]
    fn test_delete_with_entity() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(Arc::clone(&db));
        let ser = UserSerializer;

        let user = test_user(1);
        index.put(None, &ser, &user).unwrap();

        let deleted = index.delete_with_entity(None, &ser, &1u64).unwrap();
        assert!(deleted);
        assert_eq!(index.get(None, &ser, &1u64).unwrap(), None);
    }

    #[test]
    fn test_delete_with_entity_not_found() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(Arc::clone(&db));
        let ser = UserSerializer;

        let deleted = index.delete_with_entity(None, &ser, &999u64).unwrap();
        assert!(!deleted);
    }

    #[test]
    fn test_contains() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(Arc::clone(&db));
        let ser = UserSerializer;

        assert!(!index.contains(None, &1u64).unwrap());

        let user = test_user(1);
        index.put(None, &ser, &user).unwrap();

        assert!(index.contains(None, &1u64).unwrap());
    }

    #[test]
    fn test_count_empty() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(Arc::clone(&db));

        assert_eq!(index.count().unwrap(), 0);
    }

    #[test]
    fn test_count_with_entities() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(Arc::clone(&db));
        let ser = UserSerializer;

        for i in 1..=5 {
            index.put(None, &ser, &test_user(i)).unwrap();
        }

        assert_eq!(index.count().unwrap(), 5);
    }

    #[test]
    fn test_entities_iterator() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(Arc::clone(&db));
        let ser = UserSerializer;

        for i in 1..=3 {
            index.put(None, &ser, &test_user(i)).unwrap();
        }

        let entities: Vec<User> = index
            .entities(None, &ser)
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
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(Arc::clone(&db));
        let ser = UserSerializer;

        let entities: Vec<User> = index
            .entities(None, &ser)
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert!(entities.is_empty());
    }

    #[test]
    fn test_multiple_put_delete_cycles() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(Arc::clone(&db));
        let ser = UserSerializer;

        // Insert, delete, re-insert
        let user = test_user(1);
        index.put(None, &ser, &user).unwrap();
        index.delete(None, &1u64).unwrap();
        assert_eq!(index.count().unwrap(), 0);

        let user2 = User {
            id: 1,
            name: "Reinserted".to_string(),
            email: "new@example.com".to_string(),
        };
        index.put(None, &ser, &user2).unwrap();
        let found = index.get(None, &ser, &1u64).unwrap().unwrap();
        assert_eq!(found.name, "Reinserted");
    }

    #[test]
    fn test_database_reference() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(Arc::clone(&db));
        assert_eq!(index.database().lock().get_database_name(), "users");
    }

    // --- Additional branch-coverage tests ---

    /// `EntityIterator::next` after `done=true` returns `None` immediately.
    #[test]
    fn test_entity_iterator_done_returns_none() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(Arc::clone(&db));
        let ser = UserSerializer;

        // Empty database: first `next` sets done=true, second returns None.
        let mut iter = index.entities(None, &ser).unwrap();
        assert!(iter.next().is_none()); // First call on empty db → done=true
        assert!(iter.next().is_none()); // Already done
    }

    /// `EntityIterator::next` after entries are exhausted returns `None`.
    #[test]
    fn test_entity_iterator_exhausted() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(Arc::clone(&db));
        let ser = UserSerializer;

        index.put(None, &ser, &test_user(1)).unwrap();
        let mut iter = index.entities(None, &ser).unwrap();

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
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(Arc::clone(&db));
        let ser = UserSerializer;

        index.put(None, &ser, &test_user(1)).unwrap();

        let mut iter = index.keys(None).unwrap();
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
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(Arc::clone(&db));

        let mut iter = index.keys(None).unwrap();
        assert!(iter.next().is_none());
    }

    /// `KeyIterator::next` called twice: first gets the cursor result,
    /// second hits the `if self.done` early-return branch.
    #[test]
    fn test_key_iterator_done_branch() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(Arc::clone(&db));
        let ser = UserSerializer;

        for i in 1u64..=3 {
            index.put(None, &ser, &test_user(i)).unwrap();
        }

        // Drain all three keys.
        let mut iter = index.keys(None).unwrap();
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

    /// `delete_with_entity` removes the record (secondary maintenance is
    /// now driven by the underlying `Database::delete` fan-out, so this
    /// behaves like `delete`).
    #[test]
    fn test_delete_with_entity_removes_record() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(Arc::clone(&db));
        let ser = UserSerializer;

        index.put(None, &ser, &test_user(10)).unwrap();
        let deleted = index.delete_with_entity(None, &ser, &10u64).unwrap();
        assert!(deleted);
        assert_eq!(index.get(None, &ser, &10u64).unwrap(), None);
    }

    /// `delete_with_entity` on a non-existing key returns false.
    #[test]
    fn test_delete_with_entity_missing_key_returns_false() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(Arc::clone(&db));
        let ser = UserSerializer;

        let deleted = index.delete_with_entity(None, &ser, &999u64).unwrap();
        assert!(!deleted);
    }

    /// `put_no_overwrite` inserts only when the key is absent.
    #[test]
    fn test_put_no_overwrite_insert_then_skip() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(Arc::clone(&db));
        let ser = UserSerializer;

        let user = test_user(5);
        assert!(index.put_no_overwrite(None, &ser, &user).unwrap());
        // Second insert with same key: not inserted.
        assert!(!index.put_no_overwrite(None, &ser, &user).unwrap());
    }

    /// `contains` returns the correct status.
    #[test]
    fn test_contains_after_delete() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(Arc::clone(&db));
        let ser = UserSerializer;

        index.put(None, &ser, &test_user(7)).unwrap();
        assert!(index.contains(None, &7u64).unwrap());
        index.delete(None, &7u64).unwrap();
        assert!(!index.contains(None, &7u64).unwrap());
    }

    /// `count` after insertions and deletions.
    #[test]
    fn test_count_after_delete() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(Arc::clone(&db));
        let ser = UserSerializer;

        for i in 1u64..=4 {
            index.put(None, &ser, &test_user(i)).unwrap();
        }
        assert_eq!(index.count().unwrap(), 4);

        index.delete(None, &2u64).unwrap();
        assert_eq!(index.count().unwrap(), 3);
    }

    /// `entities` with many records returns all in order.
    #[test]
    fn test_entities_iterator_many() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(Arc::clone(&db));
        let ser = UserSerializer;

        for i in 1u64..=10 {
            index.put(None, &ser, &test_user(i)).unwrap();
        }

        let entities: Vec<User> = index
            .entities(None, &ser)
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(entities.len(), 10);
        for (i, user) in entities.iter().enumerate() {
            assert_eq!(user.id, (i + 1) as u64);
        }
    }
}
