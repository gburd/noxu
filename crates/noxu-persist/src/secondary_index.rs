//! Secondary index for typed entity access by a non-primary key.
//!
//!
//! A `SecondaryIndex<SK, PK, E>` maps a secondary key (SK) extracted from an
//! entity (E) back to its primary key (PK), and then looks up the entity in
//! the `PrimaryIndex`.  This mirrors `SecondaryDatabase → PrimaryDatabase`
//! join.
//!
//! # Design
//!
//! The secondary database is a first-class on-disk store that is
//! automatically kept in sync with the primary database through the
//! `SecondaryDatabase` association.  In this Rust port the persistence layer
//! sits on top of `noxu-db`'s in-memory `HashMap` store, so we replicate the
//! same invariant in memory:
//!
//! * The mapping is stored in a `BTreeMap<SK, BTreeSet<PK>>` so that secondary
//!   keys can map to *one or more* primary keys (MANY_TO_ONE / MANY_TO_MANY
//!   patterns).
//! * The map is wrapped in `Arc<Mutex<…>>` and shared between the
//!   `SecondaryIndex` and the `SecondaryRegistration` handle that is
//!   registered with `PrimaryIndex`.  This way every `put`/`delete` on the
//!   `PrimaryIndex` automatically keeps all registered secondary indexes
//!   consistent.
//!
//! # Fidelity
//!
//! | method | Rust method |
//! |---|---|
//! | `get(SK)` | `get(&sk)` |
//! | `contains(SK)` | `contains(&sk)` |
//! | `delete(SK)` | `delete(&sk)` |
//! | `entities()` cursor | `iter()` |
//! | range scan | `iter_from(&sk)` |
//! | `keysIndex()` | `keys_index()` |
//! | `subIndex(SK)` | `sub_index(&sk)` |

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use crate::entity::{Entity, PrimaryKey};
use crate::entity_serializer::EntitySerializer;
use crate::error::Result;

// ---------------------------------------------------------------------------
// Trait that every secondary index must expose so PrimaryIndex can call it
// without knowing the concrete type parameters.
// ---------------------------------------------------------------------------

/// Internal trait implemented by every `SecondaryIndex`.
///
/// `PrimaryIndex` holds a `Vec<Box<dyn SecondaryIndexMaintainer<PK, E>>>` and
/// calls `on_put` / `on_delete` for every write.
pub(crate) trait SecondaryIndexMaintainer<PK: PrimaryKey, E: Entity<PrimaryKey = PK>>:
    Send + Sync
{
    /// Called after a successful `put`.
    ///
    /// * `old_entity` – the entity that was replaced, if any (needed to
    ///   remove the stale secondary key entry).
    /// * `new_entity` – the entity that was just stored.
    fn on_put(&self, old_entity: Option<&E>, new_entity: &E);

    /// Called after a successful `delete`.
    ///
    /// * `deleted_entity` – the entity that was removed.
    fn on_delete(&self, deleted_entity: &E);
}

// ---------------------------------------------------------------------------
// The shared secondary map
// ---------------------------------------------------------------------------

/// The actual `secondary_key → {primary_key, …}` map shared between
/// `SecondaryIndex` and `SecondaryRegistration`.
///
/// Using `BTreeSet<PK>` for the value side supports both ONE_TO_ONE and
/// MANY_TO_ONE relationships without API changes.
pub(crate) struct SecondaryMap<SK: Ord + Clone, PK: Ord + Clone> {
    map: BTreeMap<SK, BTreeSet<PK>>,
    /// Reverse map: primary_key → secondary_key, used to clean up stale
    /// entries on overwrite and delete without a full scan.
    reverse: BTreeMap<PK, SK>,
}

impl<SK: Ord + Clone, PK: Ord + Clone> SecondaryMap<SK, PK> {
    fn new() -> Self {
        Self { map: BTreeMap::new(), reverse: BTreeMap::new() }
    }

    /// Insert `(sk, pk)` into the forward and reverse maps.
    fn insert(&mut self, sk: SK, pk: PK) {
        // If the same primary key already had a different secondary key,
        // remove the stale forward entry first.
        if let Some(old_sk) = self.reverse.get(&pk)
            && old_sk != &sk
        {
            let old_sk_clone = old_sk.clone();
            if let Some(set) = self.map.get_mut(&old_sk_clone) {
                set.remove(&pk);
            }
            if self.map.get(&old_sk_clone).map(|s| s.is_empty()).unwrap_or(false) {
                self.map.remove(&old_sk_clone);
            }
        }
        self.map.entry(sk.clone()).or_default().insert(pk.clone());
        self.reverse.insert(pk, sk);
    }

    /// Remove the entry for `pk` from both maps.
    fn remove_by_pk(&mut self, pk: &PK) {
        if let Some(sk) = self.reverse.remove(pk) {
            if let Some(set) = self.map.get_mut(&sk) {
                set.remove(pk);
            }
            if self.map.get(&sk).map(|s| s.is_empty()).unwrap_or(false) {
                self.map.remove(&sk);
            }
        }
    }

    /// Look up the set of primary keys for a secondary key.
    fn get_pks(&self, sk: &SK) -> Option<&BTreeSet<PK>> {
        self.map.get(sk)
    }

    /// Returns true iff the secondary key maps to at least one primary key.
    fn contains(&self, sk: &SK) -> bool {
        self.map.get(sk).is_some_and(|s| !s.is_empty())
    }

    /// Iterate all `(sk, pk)` pairs in secondary key order.
    fn iter(&self) -> impl Iterator<Item = (&SK, &PK)> {
        self.map.iter().flat_map(|(sk, pks)| pks.iter().map(move |pk| (sk, pk)))
    }

    /// Iterate all `(sk, pk)` pairs where `sk >= from_sk`, in order.
    fn iter_from<'a>(&'a self, from_sk: &'a SK) -> impl Iterator<Item = (&'a SK, &'a PK)> {
        self.map
            .range(from_sk..)
            .flat_map(|(sk, pks)| pks.iter().map(move |pk| (sk, pk)))
    }
}

// ---------------------------------------------------------------------------
// SecondaryIndex
// ---------------------------------------------------------------------------

/// Typed secondary index that maps a secondary key `SK` to entities `E`.
///
/// 
///
/// # Type Parameters
///
/// * `SK` – secondary key type (must be `Ord + Clone + Send + Sync`)
/// * `PK` – primary key type (must implement `PrimaryKey` + `Ord`)
/// * `E` – entity type (must implement `Entity<PrimaryKey = PK>`)
///
/// # Maintenance
///
/// Do **not** put or delete entities directly through a `SecondaryIndex`; use
/// the `PrimaryIndex` for all writes.  The `PrimaryIndex` automatically calls
/// back into every registered `SecondaryIndex` to keep the mapping consistent.
///
/// # Example
///
/// ```no_run
/// # use noxu_persist::{Entity, PrimaryKey, PrimaryIndex, EntitySerializer, Result};
/// # use noxu_persist::secondary_index::SecondaryIndex;
/// # use noxu_db::{Database, DatabaseConfig, Environment, EnvironmentConfig};
/// # use tempfile::TempDir;
/// # #[derive(Clone, Debug, PartialEq)]
/// # struct User { id: u64, name: String, department: String }
/// # impl Entity for User {
/// #     type PrimaryKey = u64;
/// #     fn primary_key(&self) -> &u64 { &self.id }
/// #     fn entity_name() -> &'static str { "User" }
/// # }
/// # struct UserSerializer;
/// # impl EntitySerializer<User> for UserSerializer {
/// #     fn serialize(&self, _: &User) -> Result<Vec<u8>> { Ok(vec![]) }
/// #     fn deserialize(&self, _: &[u8]) -> Result<User> { unimplemented!() }
/// # }
/// # let td = TempDir::new().unwrap();
/// # let env = Environment::open(EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true)).unwrap();
/// # let db = env.open_database(None, "users", &DatabaseConfig::new().with_allow_create(true)).unwrap();
/// let mut primary: PrimaryIndex<u64, User> = PrimaryIndex::new(&db);
/// let dept_idx: SecondaryIndex<String, u64, User> =
///     primary.open_secondary_index(|u: &User| Some(u.department.clone()));
///
/// primary.put(&UserSerializer, &User { id: 1, name: "Alice".into(), department: "Eng".into() }).unwrap();
///
/// let eng = dept_idx.get(&UserSerializer, &primary, &"Eng".to_string()).unwrap();
/// assert!(eng.is_some());
/// ```
pub struct SecondaryIndex<SK, PK, E>
where
    SK: Ord + Clone + Send + Sync + 'static,
    PK: PrimaryKey + Ord + Send + Sync + 'static,
    E: Entity<PrimaryKey = PK> + Send + Sync + 'static,
{
    /// Shared secondary map (also held by the registration in PrimaryIndex).
    shared: Arc<Mutex<SecondaryMap<SK, PK>>>,
    /// The key-extractor closure: `entity → Option<SK>`.
    ///
    /// Returns `None` for entities where the secondary key is absent (nullable
    /// keys, equivalent to null secondary key which is simply omitted).
    extractor: Arc<dyn Fn(&E) -> Option<SK> + Send + Sync>,
}

impl<SK, PK, E> SecondaryIndex<SK, PK, E>
where
    SK: Ord + Clone + Send + Sync + 'static,
    PK: PrimaryKey + Ord + Send + Sync + 'static,
    E: Entity<PrimaryKey = PK> + Clone + Send + Sync + 'static,
{
    /// Internal constructor – called from `PrimaryIndex::open_secondary_index`.
    pub(crate) fn new(
        shared: Arc<Mutex<SecondaryMap<SK, PK>>>,
        extractor: Arc<dyn Fn(&E) -> Option<SK> + Send + Sync>,
    ) -> Self {
        Self { shared, extractor }
    }

    // -----------------------------------------------------------------------
    // Read methods
    // -----------------------------------------------------------------------

    /// Returns the first entity whose secondary key equals `sk`, or `None`.
    ///
    /// When multiple primary keys map to the same secondary key (MANY_TO_ONE)
    /// the entity with the smallest primary key is returned, matching the
    /// `SecondaryDatabase.get` behaviour (returns the first duplicate).
    ///
    /// 
    pub fn get<S: EntitySerializer<E>>(
        &self,
        serializer: &S,
        primary: &crate::primary_index::PrimaryIndex<'_, PK, E>,
        sk: &SK,
    ) -> Result<Option<E>> {
        let guard = self.shared.lock().unwrap();
        let pks = match guard.get_pks(sk) {
            Some(s) if !s.is_empty() => s.clone(),
            _ => return Ok(None),
        };
        // Drop the lock before doing the (potentially slow) primary lookup.
        drop(guard);
        // Return the first matching entity (smallest PK), mirroring the.
        for pk in &pks {
            if let Some(entity) = primary.get(serializer, pk)? {
                return Ok(Some(entity));
            }
        }
        Ok(None)
    }

    /// Returns `true` if at least one entity has the given secondary key.
    ///
    /// (via `EntityIndex.contains`).
    pub fn contains(&self, sk: &SK) -> bool {
        self.shared.lock().unwrap().contains(sk)
    }

    /// Deletes the entity (or entities) with the given secondary key.
    ///
    /// Returns `true` if at least one entity was deleted.
    ///
    /// – in this deletes via the
    /// secondary database which cascades a delete to the primary.
    pub fn delete<S: EntitySerializer<E>>(
        &self,
        serializer: &S,
        primary: &crate::primary_index::PrimaryIndex<'_, PK, E>,
        sk: &SK,
    ) -> Result<bool> {
        let pks: Vec<PK> = {
            let guard = self.shared.lock().unwrap();
            match guard.get_pks(sk) {
                Some(s) => s.iter().cloned().collect(),
                None => return Ok(false),
            }
        };
        let mut deleted = false;
        for pk in &pks {
            // Use delete_with_entity so that ALL registered secondary indexes
            // (not just this one) are notified of the deletion via their
            // maintainer callbacks.  This mirrors SecondaryDatabase
            // cascade behaviour.
            if primary.delete_with_entity(serializer, pk)? {
                deleted = true;
            }
        }
        Ok(deleted)
    }

    // -----------------------------------------------------------------------
    // Iteration methods
    // -----------------------------------------------------------------------

    /// Returns an iterator over all `(secondary_key, entity)` pairs in
    /// secondary key order.
    ///
    /// / `EntityCursor`.
    pub fn iter<'a, S: EntitySerializer<E>>(
        &'a self,
        serializer: &'a S,
        primary: &'a crate::primary_index::PrimaryIndex<'_, PK, E>,
    ) -> SecondaryIterator<'a, SK, PK, E, S> {
        let pairs: Vec<(SK, PK)> = {
            let guard = self.shared.lock().unwrap();
            guard.iter().map(|(sk, pk)| (sk.clone(), pk.clone())).collect()
        };
        SecondaryIterator {
            pairs,
            pos: 0,
            serializer,
            primary,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Returns an iterator over `(secondary_key, entity)` pairs where
    /// `secondary_key >= from_sk`, in secondary key order.
    ///
    /// Range-scan via the entity index.
    pub fn iter_from<'a, S: EntitySerializer<E>>(
        &'a self,
        serializer: &'a S,
        primary: &'a crate::primary_index::PrimaryIndex<'_, PK, E>,
        from_sk: &SK,
    ) -> SecondaryIterator<'a, SK, PK, E, S> {
        let pairs: Vec<(SK, PK)> = {
            let guard = self.shared.lock().unwrap();
            guard
                .iter_from(from_sk)
                .map(|(sk, pk)| (sk.clone(), pk.clone()))
                .collect()
        };
        SecondaryIterator {
            pairs,
            pos: 0,
            serializer,
            primary,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Returns an iterator over only the secondary key → primary key mappings,
    /// without fetching the full entities.
    ///
    /// 
    pub fn keys_index(&self) -> Vec<(SK, PK)> {
        let guard = self.shared.lock().unwrap();
        guard.iter().map(|(sk, pk)| (sk.clone(), pk.clone())).collect()
    }

    /// Returns all primary keys that map to `sk` (sub-index).
    ///
    /// 
    pub fn sub_index(&self, sk: &SK) -> Vec<PK> {
        let guard = self.shared.lock().unwrap();
        guard.get_pks(sk).map(|s| s.iter().cloned().collect()).unwrap_or_default()
    }
}

// ---------------------------------------------------------------------------
// SecondaryIterator
// ---------------------------------------------------------------------------

/// Iterator returned by `SecondaryIndex::iter` and `SecondaryIndex::iter_from`.
///
/// Yields `(SK, E)` tuples in secondary key order.  Missing primary records
/// (which can occur if the primary was deleted without the secondary being
/// properly maintained) are silently skipped, correct Noxu behaviour.
pub struct SecondaryIterator<'a, SK, PK, E, S>
where
    PK: PrimaryKey + Ord + Send + Sync + 'static,
    E: Entity<PrimaryKey = PK> + Clone + Send + Sync + 'static,
{
    pairs: Vec<(SK, PK)>,
    pos: usize,
    serializer: &'a S,
    primary: &'a crate::primary_index::PrimaryIndex<'a, PK, E>,
    _phantom: std::marker::PhantomData<(SK, E)>,
}

impl<'a, SK, PK, E, S> Iterator for SecondaryIterator<'a, SK, PK, E, S>
where
    SK: Clone,
    PK: PrimaryKey + Ord + Send + Sync + 'static,
    E: Entity<PrimaryKey = PK> + Clone + Send + Sync + 'static,
    S: EntitySerializer<E>,
{
    type Item = Result<(SK, E)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.pos >= self.pairs.len() {
                return None;
            }
            let (sk, pk) = self.pairs[self.pos].clone();
            self.pos += 1;

            match self.primary.get(self.serializer, &pk) {
                Ok(Some(entity)) => return Some(Ok((sk, entity))),
                Ok(None) => {
                    // Primary record missing – skip (dangling secondary entry).
                    continue;
                }
                Err(e) => return Some(Err(e)),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Registration object (held inside PrimaryIndex)
// ---------------------------------------------------------------------------

/// Opaque handle that `PrimaryIndex` stores to maintain a secondary index.
///
/// Each `SecondaryIndex` created via `PrimaryIndex::open_secondary_index`
/// deposits one of these into the primary index's registration list.
pub(crate) struct SecondaryRegistration<SK, PK, E>
where
    SK: Ord + Clone + Send + Sync + 'static,
    PK: PrimaryKey + Ord + Send + Sync + 'static,
    E: Entity<PrimaryKey = PK> + Send + Sync + 'static,
{
    shared: Arc<Mutex<SecondaryMap<SK, PK>>>,
    extractor: Arc<dyn Fn(&E) -> Option<SK> + Send + Sync>,
}

impl<SK, PK, E> SecondaryIndexMaintainer<PK, E> for SecondaryRegistration<SK, PK, E>
where
    SK: Ord + Clone + Send + Sync + 'static,
    PK: PrimaryKey + Ord + Send + Sync + 'static,
    E: Entity<PrimaryKey = PK> + Send + Sync + 'static,
{
    fn on_put(&self, old_entity: Option<&E>, new_entity: &E) {
        let pk = new_entity.primary_key().clone();
        if let Some(sk) = (self.extractor)(new_entity) {
            // SecondaryMap::insert handles removal of the stale entry for the
            // same pk via its reverse-map lookup.
            self.shared.lock().unwrap().insert(sk, pk);
        } else if old_entity.is_some() {
            // New entity has no secondary key – remove any existing mapping.
            self.shared.lock().unwrap().remove_by_pk(&pk);
        }
    }

    fn on_delete(&self, deleted_entity: &E) {
        let pk = deleted_entity.primary_key();
        self.shared.lock().unwrap().remove_by_pk(pk);
    }
}

// ---------------------------------------------------------------------------
// Factory function used by PrimaryIndex
// ---------------------------------------------------------------------------

/// Creates a linked `(SecondaryIndex, SecondaryRegistration)` pair from a
/// key-extractor closure.
///
/// The registration is deposited into `PrimaryIndex`; the `SecondaryIndex` is
/// returned to the caller.
pub(crate) fn make_secondary_index<SK, PK, E, F>(
    extractor: F,
) -> (SecondaryIndex<SK, PK, E>, SecondaryRegistration<SK, PK, E>)
where
    SK: Ord + Clone + Send + Sync + 'static,
    PK: PrimaryKey + Ord + Send + Sync + 'static,
    E: Entity<PrimaryKey = PK> + Clone + Send + Sync + 'static,
    F: Fn(&E) -> Option<SK> + Send + Sync + 'static,
{
    let shared = Arc::new(Mutex::new(SecondaryMap::new()));
    let extractor_arc: Arc<dyn Fn(&E) -> Option<SK> + Send + Sync> = Arc::new(extractor);
    let index = SecondaryIndex::new(Arc::clone(&shared), Arc::clone(&extractor_arc));
    let reg = SecondaryRegistration { shared, extractor: extractor_arc };
    (index, reg)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity::Entity;
    use crate::entity_serializer::EntitySerializer;
    use crate::error::Result;
    use crate::primary_index::PrimaryIndex;
    use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
    use tempfile::TempDir;

    // -----------------------------------------------------------------------
    // Test fixtures
    // -----------------------------------------------------------------------

    #[derive(Clone, Debug, PartialEq)]
    struct Employee {
        id: u64,
        name: String,
        department: String,
        email: Option<String>,
    }

    impl Entity for Employee {
        type PrimaryKey = u64;

        fn primary_key(&self) -> &u64 {
            &self.id
        }

        fn entity_name() -> &'static str {
            "Employee"
        }
    }

    struct EmpSerializer;

    impl EntitySerializer<Employee> for EmpSerializer {
        fn serialize(&self, e: &Employee) -> Result<Vec<u8>> {
            let mut buf = Vec::new();
            buf.extend_from_slice(&e.id.to_be_bytes());
            let name = e.name.as_bytes();
            buf.extend_from_slice(&(name.len() as u32).to_be_bytes());
            buf.extend_from_slice(name);
            let dept = e.department.as_bytes();
            buf.extend_from_slice(&(dept.len() as u32).to_be_bytes());
            buf.extend_from_slice(dept);
            // email: 0 = absent, 1 = present
            match &e.email {
                None => buf.push(0),
                Some(em) => {
                    buf.push(1);
                    let eb = em.as_bytes();
                    buf.extend_from_slice(&(eb.len() as u32).to_be_bytes());
                    buf.extend_from_slice(eb);
                }
            }
            Ok(buf)
        }

        fn deserialize(&self, bytes: &[u8]) -> Result<Employee> {
            let id = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
            let name_len = u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize;
            let name =
                String::from_utf8(bytes[12..12 + name_len].to_vec()).unwrap();
            let pos = 12 + name_len;
            let dept_len =
                u32::from_be_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
            let department = String::from_utf8(
                bytes[pos + 4..pos + 4 + dept_len].to_vec(),
            )
            .unwrap();
            let pos = pos + 4 + dept_len;
            let email = if bytes[pos] == 0 {
                None
            } else {
                let el =
                    u32::from_be_bytes(bytes[pos + 1..pos + 5].try_into().unwrap())
                        as usize;
                Some(
                    String::from_utf8(bytes[pos + 5..pos + 5 + el].to_vec())
                        .unwrap(),
                )
            };
            Ok(Employee { id, name, department, email })
        }
    }

    fn setup() -> (TempDir, Environment, noxu_db::Database) {
        let td = TempDir::new().unwrap();
        let env = Environment::open(
            EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true),
        )
        .unwrap();
        let db = env
            .open_database(None, "emp", &DatabaseConfig::new().with_allow_create(true))
            .unwrap();
        (td, env, db)
    }

    fn emp(id: u64, dept: &str) -> Employee {
        Employee {
            id,
            name: format!("Emp{}", id),
            department: dept.to_string(),
            email: None,
        }
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    /// Basic get via secondary key – mirrors SecondaryIndex.get(SK).
    #[test]
    fn test_secondary_get_found() {
        let (_td, _env, db) = setup();
        let mut primary: PrimaryIndex<u64, Employee> = PrimaryIndex::new(&db);
        let dept_idx: SecondaryIndex<String, u64, Employee> =
            primary.open_secondary_index(|e: &Employee| Some(e.department.clone()));
        let ser = EmpSerializer;

        primary.put(&ser, &emp(1, "Engineering")).unwrap();

        let found = dept_idx.get(&ser, &primary, &"Engineering".to_string()).unwrap();
        assert!(found.is_some());
        assert_eq!(found.unwrap().id, 1);
    }

    /// get for a key with no matching entity returns None.
    #[test]
    fn test_secondary_get_not_found() {
        let (_td, _env, db) = setup();
        let mut primary: PrimaryIndex<u64, Employee> = PrimaryIndex::new(&db);
        let dept_idx: SecondaryIndex<String, u64, Employee> =
            primary.open_secondary_index(|e: &Employee| Some(e.department.clone()));
        let ser = EmpSerializer;

        primary.put(&ser, &emp(1, "Engineering")).unwrap();

        let found = dept_idx
            .get(&ser, &primary, &"Marketing".to_string())
            .unwrap();
        assert!(found.is_none());
    }

    /// contains returns true iff the secondary key exists.
    #[test]
    fn test_secondary_contains() {
        let (_td, _env, db) = setup();
        let mut primary: PrimaryIndex<u64, Employee> = PrimaryIndex::new(&db);
        let dept_idx: SecondaryIndex<String, u64, Employee> =
            primary.open_secondary_index(|e: &Employee| Some(e.department.clone()));
        let ser = EmpSerializer;

        assert!(!dept_idx.contains(&"Engineering".to_string()));
        primary.put(&ser, &emp(1, "Engineering")).unwrap();
        assert!(dept_idx.contains(&"Engineering".to_string()));
        assert!(!dept_idx.contains(&"HR".to_string()));
    }

    /// delete via secondary key removes the entity from primary too.
    #[test]
    fn test_secondary_delete() {
        let (_td, _env, db) = setup();
        let mut primary: PrimaryIndex<u64, Employee> = PrimaryIndex::new(&db);
        let dept_idx: SecondaryIndex<String, u64, Employee> =
            primary.open_secondary_index(|e: &Employee| Some(e.department.clone()));
        let ser = EmpSerializer;

        primary.put(&ser, &emp(1, "Engineering")).unwrap();

        let deleted = dept_idx
            .delete(&ser, &primary, &"Engineering".to_string())
            .unwrap();
        assert!(deleted);

        // Primary record should be gone.
        assert_eq!(primary.get(&ser, &1u64).unwrap(), None);
        // Secondary map should be clean.
        assert!(!dept_idx.contains(&"Engineering".to_string()));
    }

    /// delete on a key that does not exist returns false.
    #[test]
    fn test_secondary_delete_not_found() {
        let (_td, _env, db) = setup();
        let mut primary: PrimaryIndex<u64, Employee> = PrimaryIndex::new(&db);
        let dept_idx: SecondaryIndex<String, u64, Employee> =
            primary.open_secondary_index(|e: &Employee| Some(e.department.clone()));
        let ser = EmpSerializer;

        let deleted = dept_idx
            .delete(&ser, &primary, &"NonExistent".to_string())
            .unwrap();
        assert!(!deleted);
    }

    /// MANY_TO_ONE: multiple employees in the same department.
    #[test]
    fn test_secondary_many_to_one() {
        let (_td, _env, db) = setup();
        let mut primary: PrimaryIndex<u64, Employee> = PrimaryIndex::new(&db);
        let dept_idx: SecondaryIndex<String, u64, Employee> =
            primary.open_secondary_index(|e: &Employee| Some(e.department.clone()));
        let ser = EmpSerializer;

        for i in 1u64..=5 {
            primary.put(&ser, &emp(i, "Engineering")).unwrap();
        }
        primary.put(&ser, &emp(6, "Marketing")).unwrap();

        // sub_index returns all PKs for "Engineering"
        let eng_pks = dept_idx.sub_index(&"Engineering".to_string());
        assert_eq!(eng_pks.len(), 5);

        let mkt_pks = dept_idx.sub_index(&"Marketing".to_string());
        assert_eq!(mkt_pks.len(), 1);
    }

    /// iter yields all entries in secondary key order.
    #[test]
    fn test_secondary_iter() {
        let (_td, _env, db) = setup();
        let mut primary: PrimaryIndex<u64, Employee> = PrimaryIndex::new(&db);
        let dept_idx: SecondaryIndex<String, u64, Employee> =
            primary.open_secondary_index(|e: &Employee| Some(e.department.clone()));
        let ser = EmpSerializer;

        primary.put(&ser, &emp(1, "Zebra")).unwrap();
        primary.put(&ser, &emp(2, "Alpha")).unwrap();
        primary.put(&ser, &emp(3, "Mango")).unwrap();

        let pairs: Vec<(String, Employee)> = dept_idx
            .iter(&ser, &primary)
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(pairs.len(), 3);
        // Must be in lexicographic secondary key order.
        assert_eq!(pairs[0].0, "Alpha");
        assert_eq!(pairs[1].0, "Mango");
        assert_eq!(pairs[2].0, "Zebra");
    }

    /// iter_from respects the lower bound.
    #[test]
    fn test_secondary_iter_from() {
        let (_td, _env, db) = setup();
        let mut primary: PrimaryIndex<u64, Employee> = PrimaryIndex::new(&db);
        let dept_idx: SecondaryIndex<String, u64, Employee> =
            primary.open_secondary_index(|e: &Employee| Some(e.department.clone()));
        let ser = EmpSerializer;

        primary.put(&ser, &emp(1, "Alpha")).unwrap();
        primary.put(&ser, &emp(2, "Beta")).unwrap();
        primary.put(&ser, &emp(3, "Gamma")).unwrap();
        primary.put(&ser, &emp(4, "Delta")).unwrap();

        let pairs: Vec<(String, Employee)> = dept_idx
            .iter_from(&ser, &primary, &"Beta".to_string())
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0].0, "Beta");
        assert_eq!(pairs[1].0, "Delta");
        assert_eq!(pairs[2].0, "Gamma");
    }

    /// Secondary map is updated when an entity is overwritten with a new dept.
    #[test]
    fn test_secondary_update_on_overwrite() {
        let (_td, _env, db) = setup();
        let mut primary: PrimaryIndex<u64, Employee> = PrimaryIndex::new(&db);
        let dept_idx: SecondaryIndex<String, u64, Employee> =
            primary.open_secondary_index(|e: &Employee| Some(e.department.clone()));
        let ser = EmpSerializer;

        primary.put(&ser, &emp(1, "Engineering")).unwrap();
        assert!(dept_idx.contains(&"Engineering".to_string()));

        // Move employee 1 to Marketing.
        let updated = Employee {
            id: 1,
            name: "Emp1".to_string(),
            department: "Marketing".to_string(),
            email: None,
        };
        primary.put(&ser, &updated).unwrap();

        // Old secondary key must be gone.
        assert!(!dept_idx.contains(&"Engineering".to_string()));
        // New secondary key must be present.
        assert!(dept_idx.contains(&"Marketing".to_string()));
    }

    /// Secondary map is cleaned up when entity is deleted via primary.
    #[test]
    fn test_secondary_cleanup_on_primary_delete() {
        let (_td, _env, db) = setup();
        let mut primary: PrimaryIndex<u64, Employee> = PrimaryIndex::new(&db);
        let dept_idx: SecondaryIndex<String, u64, Employee> =
            primary.open_secondary_index(|e: &Employee| Some(e.department.clone()));
        let ser = EmpSerializer;

        primary.put(&ser, &emp(1, "Engineering")).unwrap();
        assert!(dept_idx.contains(&"Engineering".to_string()));

        primary.delete_with_entity(&ser, &1u64).unwrap();
        assert!(!dept_idx.contains(&"Engineering".to_string()));
    }

    /// Nullable secondary key: entities with None are not indexed.
    #[test]
    fn test_secondary_nullable_key() {
        let (_td, _env, db) = setup();
        let mut primary: PrimaryIndex<u64, Employee> = PrimaryIndex::new(&db);
        // Index on email (which may be None).
        let email_idx: SecondaryIndex<String, u64, Employee> =
            primary.open_secondary_index(|e: &Employee| e.email.clone());
        let ser = EmpSerializer;

        let mut e1 = emp(1, "Eng");
        e1.email = Some("alice@example.com".to_string());
        let e2 = emp(2, "Eng"); // email = None

        primary.put(&ser, &e1).unwrap();
        primary.put(&ser, &e2).unwrap();

        assert!(email_idx.contains(&"alice@example.com".to_string()));
        // e2 has no email so it must not appear in the index.
        assert_eq!(email_idx.keys_index().len(), 1);
    }

    /// keys_index returns (SK, PK) pairs without fetching entities.
    #[test]
    fn test_secondary_keys_index() {
        let (_td, _env, db) = setup();
        let mut primary: PrimaryIndex<u64, Employee> = PrimaryIndex::new(&db);
        let dept_idx: SecondaryIndex<String, u64, Employee> =
            primary.open_secondary_index(|e: &Employee| Some(e.department.clone()));
        let ser = EmpSerializer;

        primary.put(&ser, &emp(1, "Eng")).unwrap();
        primary.put(&ser, &emp(2, "HR")).unwrap();
        primary.put(&ser, &emp(3, "Eng")).unwrap();

        let keys = dept_idx.keys_index();
        // 3 entries total (2 Eng + 1 HR)
        assert_eq!(keys.len(), 3);
        // All Eng entries come before HR in sorted order.
        let eng_pairs: Vec<_> =
            keys.iter().filter(|(sk, _)| sk == "Eng").collect();
        assert_eq!(eng_pairs.len(), 2);
    }

    /// Multiple secondary indexes on the same primary index.
    #[test]
    fn test_multiple_secondary_indexes() {
        let (_td, _env, db) = setup();
        let mut primary: PrimaryIndex<u64, Employee> = PrimaryIndex::new(&db);
        let dept_idx: SecondaryIndex<String, u64, Employee> =
            primary.open_secondary_index(|e: &Employee| Some(e.department.clone()));
        let email_idx: SecondaryIndex<String, u64, Employee> =
            primary.open_secondary_index(|e: &Employee| e.email.clone());
        let ser = EmpSerializer;

        let mut e1 = emp(1, "Eng");
        e1.email = Some("a@x.com".to_string());
        primary.put(&ser, &e1).unwrap();

        assert!(dept_idx.contains(&"Eng".to_string()));
        assert!(email_idx.contains(&"a@x.com".to_string()));

        // Delete via dept secondary – both indexes should be clean.
        dept_idx.delete(&ser, &primary, &"Eng".to_string()).unwrap();
        assert!(!dept_idx.contains(&"Eng".to_string()));
        // email secondary is cleaned by the delete callback.
        assert!(!email_idx.contains(&"a@x.com".to_string()));
    }

    /// iter on an empty index yields nothing.
    #[test]
    fn test_secondary_iter_empty() {
        let (_td, _env, db) = setup();
        let mut primary: PrimaryIndex<u64, Employee> = PrimaryIndex::new(&db);
        let dept_idx: SecondaryIndex<String, u64, Employee> =
            primary.open_secondary_index(|e: &Employee| Some(e.department.clone()));
        let ser = EmpSerializer;

        let pairs: Vec<_> = dept_idx
            .iter(&ser, &primary)
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert!(pairs.is_empty());
    }
}
