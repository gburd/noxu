//! Secondary index for typed entity access by a non-primary key.
//!
//! A `SecondaryIndex<SK, PK, E>` maps a secondary key (SK) extracted from an
//! entity (E) back to its primary key (PK), and then looks up the entity in
//! the primary database.  This mirrors the BDB-JE `SecondaryDatabase →
//! PrimaryDatabase` join (`com.sleepycat.persist.SecondaryIndex`,
//! `com.sleepycat.persist.impl.Store.InternalSecondaryIndex`).
//!
//! # Design — persistent, transactional secondaries (v6.x)
//!
//! In JE, an `EntityStore` secondary index **is** a real, persistent
//! `SecondaryDatabase` that is associated with the primary database and
//! maintained automatically *inside the user transaction*
//! (`Store.openSecondaryDatabase` → `env.openSecondaryDatabase(txn, name,
//! priDb, config)`; `Store.getKeyCreator` installs a `PersistKeyCreator`).
//! When the surrounding transaction aborts, the primary write **and** the
//! secondary index update roll back together.
//!
//! This Rust port mirrors that exactly:
//!
//! * Each DPL secondary index is backed by a [`noxu_db::SecondaryDatabase`]
//!   opened against the primary `Database` and registered with it
//!   (`Database::register_secondary`).  Every primary `put` / `delete`
//!   fans out to the secondary under the **same** `Transaction`, so the
//!   secondary commits / aborts atomically with the primary — closing the
//!   former "DPL secondaries are in-memory and not transactional"
//!   correctness gap (audit #10 / #11).
//! * The secondary-key extraction (`Fn(&E) -> Option<SK>`) maps onto
//!   [`noxu_db::SecondaryKeyCreator`] (the analogue of JE's
//!   `PersistKeyCreator`): the creator deserialises the primary record,
//!   extracts the secondary key, and encodes it via
//!   [`PrimaryKey::to_bytes`] (the same tuple/byte encoding JE uses for
//!   secondary keys through `PersistKeyBinding`).
//! * Reads (`get` / `contains` / `iter` / `sub_index` / `keys_index`)
//!   delegate to the `SecondaryDatabase` cursor join — the secondary is on
//!   disk and survives restart; it is **not** rebuilt from a side map.
//!
//! There is no in-memory side `HashMap` any more: correctness comes from
//! the transactional `SecondaryDatabase`.
//!
//! # Fidelity
//!
//! | JE | Rust method |
//! |---|---|
//! | `SecondaryIndex.get(SK)` | `get(&sk)` |
//! | `EntityIndex.contains(SK)` | `contains(&sk)` |
//! | `EntityIndex.delete(SK)` | `delete(&sk)` |
//! | `entities()` cursor | `iter()` |
//! | range scan | `iter_from(&sk)` |
//! | `keysIndex()` | `keys_index()` |
//! | `subIndex(SK)` | `sub_index(&sk)` |

use std::sync::Arc;

use noxu_db::{DatabaseEntry, OperationStatus, SecondaryDatabase, Transaction};

use crate::entity::{Entity, PrimaryKey};
use crate::entity_serializer::EntitySerializer;
use crate::error::{PersistError, Result};
use crate::evolve::envelope;
use crate::evolve::mutations::Mutations;

// ---------------------------------------------------------------------------
// SecondaryIndex
// ---------------------------------------------------------------------------

/// Typed secondary index that maps a secondary key `SK` to entities `E`.
///
/// Backed by a persistent, transactional [`noxu_db::SecondaryDatabase`]
/// (the JE `SecondaryDatabase` model) associated with the primary database.
///
/// # Type Parameters
///
/// * `SK` – secondary key type (must implement [`PrimaryKey`] so it can be
///   byte-encoded for the on-disk index — the analogue of JE's
///   `PersistKeyBinding`).
/// * `PK` – primary key type (must implement [`PrimaryKey`]).
/// * `E` – entity type (must implement `Entity<PrimaryKey = PK>`).
///
/// # Maintenance
///
/// Do **not** put or delete entities directly through a `SecondaryIndex`;
/// use the `PrimaryIndex` for all writes.  The underlying
/// `SecondaryDatabase` is automatically maintained by the primary
/// database's `put` / `delete` within the active transaction (JE's
/// associate()-style auto-maintenance).
pub struct SecondaryIndex<SK, PK, E>
where
    SK: PrimaryKey + Ord + Send + Sync + 'static,
    PK: PrimaryKey + Ord + Send + Sync + 'static,
    E: Entity<PrimaryKey = PK> + Send + Sync + 'static,
{
    /// The persistent secondary database (shared via `Arc` so the handle
    /// stays registered with the primary for the lifetime of the index).
    secondary: Arc<SecondaryDatabase>,
    /// Schema-evolution mutations, cloned from the owning `PrimaryIndex`,
    /// so read-side `deserialize_versioned` can do field-level evolution.
    mutations: Arc<Mutations>,
    _phantom: std::marker::PhantomData<(SK, PK, E)>,
}

impl<SK, PK, E> SecondaryIndex<SK, PK, E>
where
    SK: PrimaryKey + Ord + Send + Sync + 'static,
    PK: PrimaryKey + Ord + Send + Sync + 'static,
    E: Entity<PrimaryKey = PK> + Clone + Send + Sync + 'static,
{
    /// Internal constructor — called from
    /// `EntityStore::open_secondary_index`.
    pub(crate) fn new(
        secondary: Arc<SecondaryDatabase>,
        mutations: Arc<Mutations>,
    ) -> Self {
        Self { secondary, mutations, _phantom: std::marker::PhantomData }
    }

    /// Decodes a primary record (envelope + payload) into an entity, using
    /// the registered mutations for field-level evolution.  Mirrors
    /// `PrimaryIndex::decode_record`.
    fn decode_primary<S: EntitySerializer<E>>(
        &self,
        bytes: &[u8],
        serializer: &S,
    ) -> Result<E> {
        let dec = envelope::decode(bytes)?;
        let expected_tag = E::entity_name();
        if dec.class_tag != expected_tag {
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
    // Read methods
    // -----------------------------------------------------------------------

    /// Returns the first entity whose secondary key equals `sk`, or `None`.
    ///
    /// When multiple primary keys map to the same secondary key
    /// (MANY_TO_ONE), the entity with the smallest primary key is returned,
    /// matching `SecondaryDatabase.get` (returns the first duplicate).
    ///
    /// Pass `Some(&txn)` to perform the lookup inside a user transaction;
    /// the secondary scan and the primary lookup both participate in `txn`.
    pub fn get<S: EntitySerializer<E>>(
        &self,
        txn: Option<&Transaction>,
        serializer: &S,
        _primary: &crate::primary_index::PrimaryIndex<PK, E>,
        sk: &SK,
    ) -> Result<Option<E>> {
        let key = sk.to_bytes();
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let found =
            self.secondary.get_into(txn, &key, &mut p_key, &mut data)?;
        if found {
            let bytes = data.get_data().ok_or_else(|| {
                PersistError::SerializationError(
                    "empty primary data from secondary join".to_string(),
                )
            })?;
            Ok(Some(self.decode_primary(bytes, serializer)?))
        } else {
            Ok(None)
        }
    }

    /// Returns `true` if at least one entity has the given secondary key.
    ///
    /// Mirrors `EntityIndex.contains` — checks the secondary index without
    /// fetching the primary record.
    ///
    /// Pass `Some(&txn)` to read inside a user transaction; `None` for
    /// auto-commit.
    pub fn contains_txn(
        &self,
        txn: Option<&Transaction>,
        sk: &SK,
    ) -> Result<bool> {
        let key = DatabaseEntry::from_vec(sk.to_bytes());
        Ok(self.secondary.exists(txn, &key)?)
    }

    /// Returns `true` if at least one entity has the given secondary key
    /// (auto-commit convenience; see [`Self::contains_txn`] for the
    /// transactional form).
    ///
    /// # Panics
    /// Never panics on a healthy database; an underlying error is mapped to
    /// `false` to preserve the historical infallible `contains` shape.  Use
    /// [`Self::contains_txn`] when you need to observe errors.
    pub fn contains(&self, sk: &SK) -> bool {
        self.contains_txn(None, sk).unwrap_or(false)
    }

    /// Deletes the entity (or entities) with the given secondary key.
    ///
    /// Returns `true` if at least one entity was deleted.  The deletion
    /// cascades to the primary record (and thence to every other secondary
    /// index) via `SecondaryDatabase::delete`.
    ///
    /// Pass `Some(&txn)` to perform the delete inside a user transaction —
    /// the primary delete and all secondary cleanup commit / abort
    /// atomically with `txn`.
    pub fn delete<S: EntitySerializer<E>>(
        &self,
        txn: Option<&Transaction>,
        _serializer: &S,
        _primary: &crate::primary_index::PrimaryIndex<PK, E>,
        sk: &SK,
    ) -> Result<bool> {
        let key = sk.to_bytes();
        let deleted = match txn {
            Some(t) => self.secondary.delete_in(t, &key)?,
            None => self.secondary.delete(&key)?,
        };
        Ok(deleted)
    }

    // -----------------------------------------------------------------------
    // Iteration methods
    // -----------------------------------------------------------------------

    /// Returns all `(secondary_key, entity)` pairs in secondary key order.
    ///
    /// Pass `Some(&txn)` to read inside a user transaction; `None` for
    /// auto-commit.
    ///
    /// The pairs are materialised eagerly (the secondary cursor is opened,
    /// scanned, and closed inside this call) so the returned iterator does
    /// not borrow the transaction.  This mirrors the historical
    /// `SecondaryIterator` shape.
    pub fn iter<'a, S: EntitySerializer<E>>(
        &'a self,
        txn: Option<&'a Transaction>,
        serializer: &'a S,
        _primary: &'a crate::primary_index::PrimaryIndex<PK, E>,
    ) -> SecondaryIterator<SK, E> {
        SecondaryIterator {
            pairs: self.collect_pairs(txn, serializer, None),
            pos: 0,
        }
    }

    /// Returns all `(secondary_key, entity)` pairs where
    /// `secondary_key >= from_sk`, in secondary key order.
    pub fn iter_from<'a, S: EntitySerializer<E>>(
        &'a self,
        txn: Option<&'a Transaction>,
        serializer: &'a S,
        _primary: &'a crate::primary_index::PrimaryIndex<PK, E>,
        from_sk: &SK,
    ) -> SecondaryIterator<SK, E> {
        SecondaryIterator {
            pairs: self.collect_pairs(txn, serializer, Some(from_sk)),
            pos: 0,
        }
    }

    /// Scans the secondary cursor and decodes every `(SK, E)` pair, starting
    /// at `from` (or the first key when `from` is `None`).  Errors mid-scan
    /// are captured as `Err` items so the iterator can surface them.
    fn collect_pairs<S: EntitySerializer<E>>(
        &self,
        txn: Option<&Transaction>,
        serializer: &S,
        from: Option<&SK>,
    ) -> Vec<Result<(SK, E)>> {
        let mut out = Vec::new();
        let mut cursor = match match txn {
            Some(t) => self.secondary.open_cursor_in(t, None),
            None => self.secondary.open_cursor(None),
        } {
            Ok(c) => c,
            Err(e) => {
                out.push(Err(e.into()));
                return out;
            }
        };
        let mut key = DatabaseEntry::new();
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        // Position to the first record we care about.
        let first = match from {
            Some(sk) => {
                key = DatabaseEntry::from_vec(sk.to_bytes());
                cursor.get_search_key_range(&mut key, &mut p_key, &mut data)
            }
            None => cursor.get_first(&mut key, &mut p_key, &mut data),
        };
        let mut status = match first {
            Ok(s) => s,
            Err(e) => {
                out.push(Err(e.into()));
                return out;
            }
        };

        while status == OperationStatus::Success {
            match (key.get_data(), data.get_data()) {
                (Some(sk_bytes), Some(data_bytes)) => {
                    let sk = SK::from_bytes(sk_bytes);
                    let ent = self.decode_primary(data_bytes, serializer);
                    match (sk, ent) {
                        (Ok(sk), Ok(ent)) => out.push(Ok((sk, ent))),
                        (Err(e), _) | (_, Err(e)) => out.push(Err(e)),
                    }
                }
                _ => {
                    // Dangling secondary entry (primary missing) — skip.
                }
            }
            status = match cursor.get_next(&mut key, &mut p_key, &mut data) {
                Ok(s) => s,
                Err(e) => {
                    out.push(Err(e.into()));
                    break;
                }
            };
        }
        let _ = cursor.close();
        out
    }

    /// Returns the `(SK, PK)` mappings without fetching full entities.
    ///
    /// Mirrors JE `keysIndex()`.  Auto-commit; use the cursor directly if
    /// you need a transactional read.
    pub fn keys_index(&self) -> Vec<(SK, PK)> {
        let mut out = Vec::new();
        let mut cursor = match self.secondary.open_cursor(None) {
            Ok(c) => c,
            Err(_) => return out,
        };
        let mut key = DatabaseEntry::new();
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let mut status = cursor
            .get_first(&mut key, &mut p_key, &mut data)
            .unwrap_or(OperationStatus::NotFound);
        while status == OperationStatus::Success {
            if let (Some(sk_b), Some(pk_b)) = (key.get_data(), p_key.get_data())
                && let (Ok(sk), Ok(pk)) =
                    (SK::from_bytes(sk_b), PK::from_bytes(pk_b))
            {
                out.push((sk, pk));
            }
            status = cursor
                .get_next(&mut key, &mut p_key, &mut data)
                .unwrap_or(OperationStatus::NotFound);
        }
        let _ = cursor.close();
        out
    }

    /// Returns all primary keys that map to `sk` (the JE `subIndex(SK)`
    /// duplicate run).  Auto-commit.
    pub fn sub_index(&self, sk: &SK) -> Vec<PK> {
        let mut out = Vec::new();
        let mut cursor = match self.secondary.open_cursor(None) {
            Ok(c) => c,
            Err(_) => return out,
        };
        let search = DatabaseEntry::from_vec(sk.to_bytes());
        let mut p_key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();
        let mut status = cursor
            .get_search_key(&search, &mut p_key, &mut data)
            .unwrap_or(OperationStatus::NotFound);
        while status == OperationStatus::Success {
            if let Some(pk_b) = p_key.get_data()
                && let Ok(pk) = PK::from_bytes(pk_b)
            {
                out.push(pk);
            }
            let mut key = DatabaseEntry::new();
            status = cursor
                .get_next_dup_full(&mut key, &mut p_key, &mut data)
                .unwrap_or(OperationStatus::NotFound);
        }
        let _ = cursor.close();
        out
    }

    /// Returns a reference to the underlying `SecondaryDatabase`.
    pub fn secondary_database(&self) -> &SecondaryDatabase {
        &self.secondary
    }
}

// ---------------------------------------------------------------------------
// SecondaryIterator
// ---------------------------------------------------------------------------

/// Iterator returned by `SecondaryIndex::iter` and
/// `SecondaryIndex::iter_from`.  Yields `(SK, E)` tuples in secondary key
/// order.  The pairs are materialised when the iterator is created (the
/// underlying secondary cursor has already been scanned and closed).
pub struct SecondaryIterator<SK, E> {
    pairs: Vec<Result<(SK, E)>>,
    pos: usize,
}

impl<SK, E> Iterator for SecondaryIterator<SK, E> {
    type Item = Result<(SK, E)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.pairs.len() {
            return None;
        }
        let item = std::mem::replace(
            &mut self.pairs[self.pos],
            Err(PersistError::SerializationError(String::new())),
        );
        self.pos += 1;
        Some(item)
    }
}

// ---------------------------------------------------------------------------
// SecondaryKeyCreator bridge (JE PersistKeyCreator analogue)
// ---------------------------------------------------------------------------

/// Bridges the DPL extractor `Fn(&E) -> Option<SK>` onto a
/// [`noxu_db::SecondaryKeyCreator`].  This is the Rust analogue of JE's
/// `com.sleepycat.persist.impl.PersistKeyCreator`: it deserialises the
/// primary record, extracts the secondary key, and encodes it to bytes.
///
/// The bridge owns:
/// * the entity deserializer (a boxed closure over the user's
///   `EntitySerializer<E>` + the store's [`Mutations`]); and
/// * the extractor closure.
///
/// It is `Send + Sync` because it is invoked from the primary database's
/// write path (potentially multiple threads), exactly like JE's
/// `PersistKeyCreator`.
pub(crate) struct ExtractorKeyCreator<SK, PK, E>
where
    SK: PrimaryKey + Send + Sync + 'static,
    PK: PrimaryKey + Send + Sync + 'static,
    E: Entity<PrimaryKey = PK> + Send + Sync + 'static,
{
    /// Deserialises a full primary record (envelope + payload) into `E`,
    /// peeling the class-version envelope and honouring schema evolution
    /// — the same logic as `PrimaryIndex::decode_record`.
    deserialize: Arc<dyn Fn(&[u8]) -> Result<E> + Send + Sync>,
    /// Extracts the secondary key from an entity.
    extractor: Arc<dyn Fn(&E) -> Option<SK> + Send + Sync>,
    _phantom: std::marker::PhantomData<(SK, PK)>,
}

impl<SK, PK, E> ExtractorKeyCreator<SK, PK, E>
where
    SK: PrimaryKey + Send + Sync + 'static,
    PK: PrimaryKey + Send + Sync + 'static,
    E: Entity<PrimaryKey = PK> + Send + Sync + 'static,
{
    pub(crate) fn new(
        deserialize: Arc<dyn Fn(&[u8]) -> Result<E> + Send + Sync>,
        extractor: Arc<dyn Fn(&E) -> Option<SK> + Send + Sync>,
    ) -> Self {
        Self { deserialize, extractor, _phantom: std::marker::PhantomData }
    }
}

impl<SK, PK, E> noxu_db::SecondaryKeyCreator for ExtractorKeyCreator<SK, PK, E>
where
    SK: PrimaryKey + Send + Sync + 'static,
    PK: PrimaryKey + Send + Sync + 'static,
    E: Entity<PrimaryKey = PK> + Send + Sync + 'static,
{
    fn create_secondary_key(
        &self,
        _secondary_db: &noxu_db::Database,
        _key: &DatabaseEntry,
        data: &DatabaseEntry,
        result: &mut DatabaseEntry,
    ) -> bool {
        let Some(bytes) = data.get_data() else { return false };
        // The deserialize closure peels the per-record class-version
        // envelope and dispatches to `deserialize_versioned` (honouring
        // schema evolution) — identical to `PrimaryIndex::decode_record`.
        let entity = match (self.deserialize)(bytes) {
            Ok(e) => e,
            Err(_) => return false,
        };
        match (self.extractor)(&entity) {
            Some(sk) => {
                result.set_data(&sk.to_bytes());
                true
            }
            None => false, // nullable secondary key — omit from the index.
        }
    }
}
