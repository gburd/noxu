//! Primary index for typed entity access.
//!

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use noxu_db::{Database, DatabaseEntry, OperationStatus, Transaction};

use crate::entity::{Entity, PrimaryKey};
use crate::entity_serializer::EntitySerializer;
use crate::error::{PersistError, Result};
use crate::evolve::envelope;
use crate::evolve::mutations::Mutations;
use crate::secondary_index::{
    SecondaryIndex, SecondaryIndexMaintainer, SecondaryRegistration,
    make_secondary_index,
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
    /// Schema-evolution mutations (Wave 2C-2).
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
    /// Secondary index maintainers registered via `open_secondary_index`.
    ///
    /// Each secondary index deposits a `SecondaryRegistration` here. On every
    /// `put` / `delete_with_entity` every maintainer is notified so the
    /// secondary maps stay in sync with the primary store — mirroring the
    /// `SecondaryDatabase` auto-maintenance.
    secondaries: Vec<Box<dyn SecondaryIndexMaintainer<K, E> + Send + Sync>>,
    /// One-shot guard: have we already warned the operator that secondary
    /// updates are not atomic with the user transaction?  Set the first
    /// time `put` / `delete_with_entity` is called with `Some(&txn)` while
    /// `secondaries` is non-empty.  See
    /// `PersistError::SecondariesNotTransactional`.
    warned_secondaries_non_txn: AtomicBool,
    _phantom: PhantomData<(K, E)>,
}

impl<'db, K, E> PrimaryIndex<'db, K, E>
where
    K: PrimaryKey + Ord + Send + Sync + 'static,
    E: Entity<PrimaryKey = K> + Clone + Send + Sync + 'static,
{
    /// Creates a new `PrimaryIndex` wrapping the given database.
    ///
    /// The new index has no [`Mutations`] registered; field-level
    /// schema evolution will not be available on read.  Use
    /// [`Self::with_mutations`] to attach a mutations set.
    pub fn new(db: &'db Database) -> Self {
        Self::with_mutations(db, Arc::new(Mutations::new()))
    }

    /// Creates a new `PrimaryIndex` wrapping the given database and
    /// remembering the mutations set for read-side evolution.
    pub fn with_mutations(
        db: &'db Database,
        mutations: Arc<Mutations>,
    ) -> Self {
        Self {
            db,
            mutations,
            secondaries: Vec::new(),
            warned_secondaries_non_txn: AtomicBool::new(false),
            _phantom: PhantomData,
        }
    }

    /// Returns a clone of the registered mutations Arc.
    pub fn mutations(&self) -> &Arc<Mutations> {
        &self.mutations
    }

    /// Emit a one-shot operator warning when a primary write occurs inside
    /// an explicit transaction while in-memory secondary indexes are
    /// registered.
    ///
    /// In v1.5 secondary mutations are applied immediately on the primary
    /// write regardless of the transaction's commit/abort outcome (see
    /// `PersistError::SecondariesNotTransactional`).  The warning is rate-
    /// limited to once per `PrimaryIndex` to keep logs sane in hot paths.
    fn warn_secondaries_not_txn_once(&self, txn: Option<&Transaction>) {
        if txn.is_none() || self.secondaries.is_empty() {
            return;
        }
        if self
            .warned_secondaries_non_txn
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            // Construct the typed error so the message is the canonical one
            // documented on PersistError, but emit it as a warning rather
            // than returning it (the call still succeeds).
            log::warn!(
                target: "noxu_persist",
                "{} (entity: {})",
                PersistError::SecondariesNotTransactional,
                E::entity_name(),
            );
            // The `debug_assert` makes the limitation surface in tests so
            // callers reviewing entity wiring see the issue at runtime,
            // matching the audit's request for a "warning in the
            // secondary-update path".  Tests that legitimately exercise
            // the txn + secondary path can opt out by setting
            // `NOXU_PERSIST_ALLOW_NON_TXN_SECONDARIES=1`.
            debug_assert!(
                std::env::var("NOXU_PERSIST_ALLOW_NON_TXN_SECONDARIES").is_ok(),
                "DPL: primary write inside an explicit transaction while \
                 in-memory secondary indexes are registered (entity {}). \
                 Set NOXU_PERSIST_ALLOW_NON_TXN_SECONDARIES=1 to silence \
                 this debug assertion. v1.6 will back secondaries with a \
                 real Database.",
                E::entity_name(),
            );
        }
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
        let (index, reg): (
            SecondaryIndex<SK, K, E>,
            SecondaryRegistration<SK, K, E>,
        ) = make_secondary_index(extractor);
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
        let key_entry = DatabaseEntry::from_vec(key.to_bytes());
        let mut data_entry = DatabaseEntry::new();

        let status = self.db.get(txn, &key_entry, &mut data_entry)?;

        match status {
            OperationStatus::Success => {
                let bytes = data_entry.get_data().ok_or_else(|| {
                    PersistError::SerializationError(
                        "empty data from database".to_string(),
                    )
                })?;
                let entity = self.decode_record(bytes, serializer)?;
                Ok(Some(entity))
            }
            OperationStatus::NotFound => Ok(None),
            OperationStatus::KeyExists => Ok(None),
        }
    }

    /// Decodes a raw on-disk record into an entity, peeling the
    /// per-record class-version envelope and dispatching to
    /// [`EntitySerializer::deserialize_versioned`].
    ///
    /// Pre-Wave-2C-2 records had no envelope; the migration guide
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
    /// All registered secondary indexes are updated automatically.
    ///
    /// # Transactions
    ///
    /// Pass `Some(&txn)` to participate in an explicit transaction (the
    /// primary-database write commits/aborts atomically with the txn).
    /// Pass `None` for auto-commit.
    ///
    /// **v1.5 limitation:** registered secondary indexes are in-memory
    /// only and are *not* atomic with the user transaction — the
    /// secondary-map updates are applied immediately on this call
    /// regardless of whether the caller later commits or aborts.  See
    /// `PersistError::SecondariesNotTransactional`.  Calling this method
    /// with `Some(&txn)` while at least one secondary index is
    /// registered emits a one-shot `log::warn!`.  v1.6 backs secondary
    /// indexes with a real `Database` to close this gap.
    ///
    /// # Errors
    /// Returns an error if serialization or the database operation fails.
    pub fn put<S: EntitySerializer<E>>(
        &self,
        txn: Option<&Transaction>,
        serializer: &S,
        entity: &E,
    ) -> Result<()> {
        self.warn_secondaries_not_txn_once(txn);

        // Fetch the existing entity so secondary maintainers can remove the
        // stale secondary key mapping (mirrors old-value callback).
        let old_entity = self.get(txn, serializer, entity.primary_key())?;

        let key_bytes = entity.primary_key().to_bytes();
        let key_entry = DatabaseEntry::from_vec(key_bytes);
        let payload = serializer.serialize(entity)?;
        let envelope_bytes =
            envelope::encode(E::class_version(), E::entity_name(), &payload)?;
        let data_entry = DatabaseEntry::from_vec(envelope_bytes);

        self.db.put(txn, &key_entry, &data_entry)?;

        // Notify all secondary maintainers.
        // NOTE: in v1.5 this happens *eagerly* and is NOT rolled back if
        // `txn` is later aborted — see `SecondariesNotTransactional`.
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
    /// See [`PrimaryIndex::put`] for the transactional semantics; the same
    /// v1.5 secondary-index limitation applies.
    ///
    /// # Errors
    /// Returns an error if serialization or the database operation fails.
    pub fn put_no_overwrite<S: EntitySerializer<E>>(
        &self,
        txn: Option<&Transaction>,
        serializer: &S,
        entity: &E,
    ) -> Result<bool> {
        self.warn_secondaries_not_txn_once(txn);

        let key_bytes = entity.primary_key().to_bytes();
        let key_entry = DatabaseEntry::from_vec(key_bytes);
        let payload = serializer.serialize(entity)?;
        let envelope_bytes =
            envelope::encode(E::class_version(), E::entity_name(), &payload)?;
        let data_entry = DatabaseEntry::from_vec(envelope_bytes);

        let status = self.db.put_no_overwrite(txn, &key_entry, &data_entry)?;
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
    /// # Transactions
    ///
    /// Pass `Some(&txn)` to participate in an explicit transaction; `None`
    /// for auto-commit.
    ///
    /// # Errors
    /// Returns an error if the database operation fails.
    pub fn delete(&self, txn: Option<&Transaction>, key: &K) -> Result<bool> {
        let key_entry = DatabaseEntry::from_vec(key.to_bytes());
        let status = self.db.delete(txn, &key_entry)?;
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
    /// See [`PrimaryIndex::put`] for the transactional semantics; the same
    /// v1.5 secondary-index limitation applies.
    ///
    /// # Errors
    /// Returns an error if the database operation fails or deserialization fails.
    pub fn delete_with_entity<S: EntitySerializer<E>>(
        &self,
        txn: Option<&Transaction>,
        serializer: &S,
        key: &K,
    ) -> Result<bool> {
        self.warn_secondaries_not_txn_once(txn);

        let old_entity = self.get(txn, serializer, key)?;
        let deleted = self.delete(txn, key)?;
        if deleted && let Some(ref e) = old_entity {
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
    /// Pass `Some(&txn)` to read inside a transaction; `None` for
    /// auto-commit.
    ///
    /// # Errors
    /// Returns an error if the database operation fails.
    pub fn contains(&self, txn: Option<&Transaction>, key: &K) -> Result<bool> {
        let key_entry = DatabaseEntry::from_vec(key.to_bytes());
        let mut data_entry = DatabaseEntry::new();
        let status = self.db.get(txn, &key_entry, &mut data_entry)?;
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
    /// Pass `Some(&txn)` to iterate inside a transaction (cursor reads
    /// acquire shared locks via the txn locker); `None` for auto-commit.
    ///
    /// # Errors
    /// Returns an error if the cursor cannot be opened.
    pub fn entities<'a, S: EntitySerializer<E>>(
        &'a self,
        txn: Option<&Transaction>,
        serializer: &'a S,
    ) -> Result<EntityIterator<'a, K, E, S>> {
        let cursor = self.db.open_cursor(txn, None)?;
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
    pub fn keys(
        &self,
        txn: Option<&Transaction>,
    ) -> Result<KeyIterator<'_, K>> {
        let cursor = self.db.open_cursor(txn, None)?;
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
        index.put(None, &ser, &user).unwrap();

        let found = index.get(None, &ser, &1u64).unwrap();
        assert_eq!(found, Some(user));
    }

    #[test]
    fn test_get_not_found() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        let found = index.get(None, &ser, &999u64).unwrap();
        assert_eq!(found, None);
    }

    #[test]
    fn test_put_overwrites() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
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
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        let user = test_user(1);
        let inserted = index.put_no_overwrite(None, &ser, &user).unwrap();
        assert!(inserted);
    }

    #[test]
    fn test_put_no_overwrite_fails_on_existing() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
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
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
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
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);

        let deleted = index.delete(None, &999u64).unwrap();
        assert!(!deleted);
    }

    #[test]
    fn test_delete_with_entity() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
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
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        let deleted = index.delete_with_entity(None, &ser, &999u64).unwrap();
        assert!(!deleted);
    }

    #[test]
    fn test_contains() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        assert!(!index.contains(None, &1u64).unwrap());

        let user = test_user(1);
        index.put(None, &ser, &user).unwrap();

        assert!(index.contains(None, &1u64).unwrap());
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
            index.put(None, &ser, &test_user(i)).unwrap();
        }

        assert_eq!(index.count().unwrap(), 5);
    }

    #[test]
    fn test_entities_iterator() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
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
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
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
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
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
        let mut iter = index.entities(None, &ser).unwrap();
        assert!(iter.next().is_none()); // First call on empty db → done=true
        assert!(iter.next().is_none()); // Already done
    }

    /// `EntityIterator::next` after entries are exhausted returns `None`.
    #[test]
    fn test_entity_iterator_exhausted() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
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
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
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
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);

        let mut iter = index.keys(None).unwrap();
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

    /// `open_secondary_index` with `put` and `delete_with_entity` exercises
    /// the secondary notification paths.
    #[test]
    fn test_secondary_index_notifications_on_put_and_delete() {
        let (_td, _env, db) = setup();
        let mut index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        // Open a secondary index on name.
        let name_idx =
            index.open_secondary_index(|u: &User| Some(u.name.clone()));

        let user = test_user(10);
        index.put(None, &ser, &user).unwrap();
        // Secondary should now contain the name.
        assert!(name_idx.contains(&"User10".to_string()));

        // Overwrite: put triggers on_put with old_entity set.
        let updated = User {
            id: 10,
            name: "UpdatedUser10".to_string(),
            email: "u@x.com".to_string(),
        };
        index.put(None, &ser, &updated).unwrap();
        assert!(!name_idx.contains(&"User10".to_string()));
        assert!(name_idx.contains(&"UpdatedUser10".to_string()));

        // delete_with_entity: fetches entity and calls on_delete.
        let deleted = index.delete_with_entity(None, &ser, &10u64).unwrap();
        assert!(deleted);
        assert!(!name_idx.contains(&"UpdatedUser10".to_string()));
    }

    /// `delete_with_entity` on non-existing key: deleted=false, secondaries NOT notified.
    #[test]
    fn test_delete_with_entity_missing_key_no_secondary_notify() {
        let (_td, _env, db) = setup();
        let mut index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        let _name_idx =
            index.open_secondary_index(|u: &User| Some(u.name.clone()));

        let deleted = index.delete_with_entity(None, &ser, &999u64).unwrap();
        assert!(!deleted);
    }

    /// `put_no_overwrite` with secondary index: only fires on_put when inserted.
    #[test]
    fn test_put_no_overwrite_with_secondary() {
        let (_td, _env, db) = setup();
        let mut index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
        let ser = UserSerializer;

        let name_idx =
            index.open_secondary_index(|u: &User| Some(u.name.clone()));

        let user = test_user(5);
        let inserted = index.put_no_overwrite(None, &ser, &user).unwrap();
        assert!(inserted);
        assert!(name_idx.contains(&"User5".to_string()));

        // Second insert with same key: not inserted, secondary unchanged.
        let inserted2 = index.put_no_overwrite(None, &ser, &user).unwrap();
        assert!(!inserted2);
        assert!(name_idx.contains(&"User5".to_string()));
    }

    /// `contains` returns the correct status.
    #[test]
    fn test_contains_after_delete() {
        let (_td, _env, db) = setup();
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
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
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
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
        let index: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
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
