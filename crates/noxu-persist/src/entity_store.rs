//! Entity store for managing databases of typed entities.
//!
//! Schema evolution is wired into the open path:
//!
//! * `EntityStore::open` opens a hidden
//!   [`crate::evolve::ClassCatalog`] alongside the entity databases.
//! * `get_primary_index<E>()` consults the catalog; if
//!   `E::class_version()` differs from the persisted version and
//!   [`StoreConfig::with_mutations`] supplied a non-empty
//!   [`Mutations`] set, the entity database is **streamed through a
//!   single transaction**, applying class-level Renamer / Deleter /
//!   Converter mutations and rewriting per-record envelopes with the
//!   current version.
//! * `evolve(...)` is still available for callers that want to drive
//!   evolution explicitly, but it now uses the same transactional,
//!   streamed code path — no `scan_all_kv` materialisation.
//!

use std::sync::Arc;

use hashbrown::HashMap;

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, Get, Mutex,
    OperationStatus, Put, SecondaryConfig, SecondaryDatabase, Transaction,
};

use crate::entity::{Entity, PrimaryKey};
use crate::entity_serializer::EntitySerializer;
use crate::error::{PersistError, Result};
use crate::evolve::catalog::ClassCatalog;
use crate::evolve::envelope;
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
    /// Primary entity databases, shared via `Arc<Mutex<Database>>` so that a
    /// real persistent [`SecondaryDatabase`] can be opened against the same
    /// primary handle (its automatic-maintenance fan-out only fires for
    /// writes through that handle).  Mirrors JE's `Store.priIndexMap` where
    /// each primary is a `Database` the secondaries associate with.
    databases: HashMap<String, Arc<Mutex<Database>>>,
    /// Opened secondary index databases, keyed by their full DB name.
    /// Kept as strong `Arc`s so the secondaries stay **registered** with
    /// their primary (the primary holds only a `Weak`).  Mirrors JE's
    /// `Store.secIndexMap`.
    secondaries: HashMap<String, Arc<SecondaryDatabase>>,
    /// The persistent class-version catalog.
    ///
    /// Lazily opened on first access to avoid disturbing recovery of
    /// pre-existing entity databases.  Opening a NEW database (the
    /// catalog, with `allow_create=true`) before the existing entity
    /// databases have been touched can mask their on-disk state in
    /// some env recovery sequences; deferring the catalog open until
    /// after the first entity database has been opened sidesteps that
    /// quirk.
    catalog: Option<ClassCatalog>,
    /// Shared mutations Arc.  Cloned into each [`PrimaryIndex`] so
    /// read-side `deserialize_versioned` can do field-level evolution.
    /// Defaults to an empty `Mutations`.
    mutations: Arc<Mutations>,
    /// Shared evolve-config Arc.  Defaults to an empty config
    /// ("evolve all classes").
    evolve_config: Arc<EvolveConfig>,
    /// Tracks which entity-class names have already had their open-path
    /// evolution checked during this `EntityStore`'s lifetime.  Re-checks
    /// (e.g. a second `get_primary_index<E>` call) become idempotent
    /// no-ops without re-querying the catalog.
    evolved: hashbrown::HashSet<String>,
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

        let mutations = config
            .mutations
            .clone()
            .unwrap_or_else(|| Arc::new(Mutations::new()));
        let evolve_config = config
            .evolve_config
            .clone()
            .unwrap_or_else(|| Arc::new(EvolveConfig::new()));

        // The catalog is opened lazily on the first call to
        // `get_primary_index` / `evolve` so that pre-existing entity
        // databases are recovered before a new (catalog) database is
        // created.  See the comment on `EntityStore::catalog`.
        Ok(EntityStore {
            env,
            config,
            databases: HashMap::new(),
            secondaries: HashMap::new(),
            catalog: None,
            mutations,
            evolve_config,
            evolved: hashbrown::HashSet::new(),
            open: true,
        })
    }

    /// Returns a reference to the open catalog, opening it on demand.
    fn catalog_mut(&mut self) -> Result<&mut ClassCatalog> {
        if self.catalog.is_none() {
            self.catalog = Some(ClassCatalog::open(
                self.env,
                &self.config.store_name,
                self.config.allow_create,
                self.config.read_only,
                self.config.transactional,
            )?);
        }
        Ok(self.catalog.as_mut().unwrap())
    }

    /// Returns a shared reference to the open catalog, opening it on demand.
    fn catalog_ref(&mut self) -> Result<&ClassCatalog> {
        let _ = self.catalog_mut()?;
        Ok(self.catalog.as_ref().unwrap())
    }

    /// Gets or creates the primary index for an entity type.
    ///
    /// If the underlying database does not exist and `allow_create` is true
    /// in the store configuration, it will be created. The database name is
    /// formed as `"{store_name}_{entity_name}"`.
    ///
    /// The first call to `get_primary_index<E>` for a given
    /// `E` checks the persistent class catalog.  If the persisted class
    /// version differs from `E::class_version()` and a non-empty
    /// [`crate::evolve::Mutations`] set was attached to the
    /// [`StoreConfig`] via [`StoreConfig::with_mutations`], the records
    /// in the entity database are streamed through a single transaction
    /// (not materialised into RAM) and rewritten with class-level
    /// Renamer / Deleter / Converter mutations applied.  The catalog
    /// is then updated to `E::class_version()`.
    ///
    /// If the persisted version equals the current version, no work is
    /// done.  This makes the call idempotent across reopens.
    ///
    /// # Type Parameters
    /// * `K` - The primary key type
    /// * `E` - The entity type
    ///
    /// # Errors
    /// Returns an error if the store is not open, the database cannot be
    /// opened/created, or the entity configuration is invalid.
    pub fn get_primary_index<K, E>(&mut self) -> Result<PrimaryIndex<K, E>>
    where
        K: PrimaryKey + Ord + Send + Sync + 'static,
        E: Entity<PrimaryKey = K> + Clone + Send + Sync + 'static,
    {
        if !self.open {
            return Err(PersistError::StoreNotOpen);
        }

        let entity_name = E::entity_name();
        let db_name = format!("{}_{}", self.config.store_name, entity_name);

        if !self.databases.contains_key(&db_name) {
            let mut db_config = DatabaseConfig::new();
            // Wave 7 polish: align with JE's `EntityStore.open()` semantics
            // for pure read-only reopens.  JE permits opening an existing
            // entity store with `setReadOnly(true)` and *no*
            // `setAllowCreate(true)` — the entity DBs are simply re-opened
            // from the on-disk image.
            //
            // Noxu has no durable name->db_id catalog at recovery time, so
            // the env's `open_database()` cannot resolve an existing entity
            // DB by name without `allow_create=true`.  Recovery does
            // re-materialise the on-disk B-tree (keyed by db_id), and the
            // get_primary_index call sequence after reopen is deterministic
            // (catalog -> entity), so the freshly-allocated db_id matches
            // the recovered tree slot.
            //
            // We therefore force `allow_create=true` under the hood when
            // the store was opened read-only.  The `read_only=true` flag on
            // the underlying `DatabaseConfig` still rejects every write at
            // the `Database::put` / `delete` boundary via
            // `check_writable()` -> `NoxuError::ReadOnly`, so this is
            // observably equivalent to JE's behaviour for callers.  See
            // `tck_persist_read_only_store_reopens_without_allow_create`.
            //
            // The same pattern is already used by `ClassCatalog::open` for
            // the catalog DB.
            let effective_allow_create =
                self.config.allow_create || self.config.read_only;
            db_config.set_allow_create(effective_allow_create);
            db_config.set_read_only(self.config.read_only);
            db_config.set_transactional(self.config.transactional);

            let db = self.env.open_database(None, &db_name, &db_config)?;
            self.databases.insert(db_name.clone(), Arc::new(Mutex::new(db)));
        }

        // Run open-path evolution exactly once per entity-class per store
        // lifetime.  This is the wired-in counterpart to the `evolve()`
        // public method (which can still be called explicitly later).
        if !self.evolved.contains(entity_name) {
            self.evolve_open_path::<E>(&db_name)?;
            self.evolved.insert(entity_name.to_string());
        }

        let db = self
            .databases
            .get(&db_name)
            .ok_or_else(|| PersistError::IndexNotAvailable(db_name.clone()))?;

        Ok(PrimaryIndex::with_mutations(
            Arc::clone(db),
            Arc::clone(&self.mutations),
        ))
    }

    /// Open-path evolution: compares persisted class version vs the
    /// `Entity::class_version()` constant on the type and runs streamed
    /// transactional evolution if they differ.
    ///
    /// This is the wired-in counterpart to [`Self::evolve`] — it runs
    /// automatically the first time `get_primary_index<E>()` is called
    /// for a given `E`.  It is idempotent: running with persisted ==
    /// current version is a no-op.
    fn evolve_open_path<E>(&mut self, db_name: &str) -> Result<()>
    where
        E: Entity,
    {
        let entity_name = E::entity_name();
        let current_version = E::class_version();
        let cfg = Arc::clone(&self.evolve_config);
        if !cfg.should_evolve(entity_name) {
            return Ok(());
        }

        let persisted = self.catalog_ref()?.get(None, entity_name)?;

        // Run the streamed evolution if **either** the persisted version
        // differs from the user's current_version, or the user has
        // registered any class-level mutation against this entity name.
        // The latter handles the case where the catalog says "already
        // up to date" but the user wants to re-apply a Deleter /
        // Converter / Renamer.  Mutations that don't match any record
        // are no-ops (Skip), so this is safe and idempotent.
        let mutations_apply =
            mutations_apply_to(self.mutations.as_ref(), entity_name);

        match persisted {
            None => {
                // First time seeing this class.
                if mutations_apply && !self.config.read_only {
                    let db = self.databases.get(db_name).ok_or_else(|| {
                        PersistError::IndexNotAvailable(db_name.to_string())
                    })?;
                    let catalog = self.catalog.as_ref().unwrap();
                    let stats = stream_evolve_class(
                        self.env,
                        db,
                        entity_name,
                        current_version,
                        self.mutations.as_ref(),
                        cfg.as_ref(),
                        self.config.transactional,
                        catalog,
                    )?;
                    log::info!(
                        target: "noxu_persist::evolve",
                        "open-path evolved entity '{}' (no prior catalog entry) to v{}: {}",
                        entity_name, current_version, stats,
                    );
                } else if !self.config.read_only {
                    self.catalog_mut()?.put(
                        None,
                        entity_name,
                        current_version,
                    )?;
                }
            }
            Some(entry)
                if entry.class_version == current_version
                    && !mutations_apply =>
            {
                // Already up to date and no mutations to re-apply.
            }
            Some(entry) => {
                if self.config.read_only {
                    return Err(PersistError::DatabaseError(
                        noxu_db::NoxuError::OperationNotAllowed(format!(
                            "entity '{}' needs schema evolution from version \
                             {} to {}, but the store was opened read-only",
                            entity_name, entry.class_version, current_version,
                        )),
                    ));
                }

                let db = self.databases.get(db_name).ok_or_else(|| {
                    PersistError::IndexNotAvailable(db_name.to_string())
                })?;
                let catalog = self.catalog.as_ref().unwrap();
                let stats = stream_evolve_class(
                    self.env,
                    db,
                    entity_name,
                    current_version,
                    self.mutations.as_ref(),
                    cfg.as_ref(),
                    self.config.transactional,
                    catalog,
                )?;
                log::info!(
                    target: "noxu_persist::evolve",
                    "open-path evolved entity '{}' from v{} to v{}: {}",
                    entity_name,
                    entry.class_version,
                    current_version,
                    stats,
                );
            }
        }
        Ok(())
    }

    /// Opens (creating and populating if necessary) a persistent,
    /// transactional secondary index for an entity type.
    ///
    /// This is the Rust analogue of JE's
    /// `Store.openSecondaryDatabase` (`com.sleepycat.persist.impl.Store`):
    /// it opens a real [`SecondaryDatabase`] associated with the entity's
    /// primary database, installs a [`noxu_db::SecondaryKeyCreator`] built
    /// from the supplied serializer + extractor (the analogue of JE's
    /// `PersistKeyCreator`), and registers it with the primary so that
    /// every `put` / `delete` maintains the secondary **inside the active
    /// transaction**.  Aborting a transaction rolls the primary write and
    /// the secondary update back together.
    ///
    /// The secondary index DB is named `"{store}_{entity}_{name}"` and is
    /// opened with sorted duplicates (so MANY_TO_ONE secondary keys can
    /// map to multiple primaries) and `allow_populate` (so an existing
    /// primary is back-filled when the secondary is first created).  It is
    /// persistent: on store reopen it survives on disk and is **not**
    /// rebuilt from scratch.
    ///
    /// The caller must pass the [`PrimaryIndex`] obtained from
    /// [`Self::get_primary_index`] for the same entity, plus the
    /// [`EntitySerializer`] used to (de)serialise the entity and a closure
    /// extracting the secondary key from an entity.
    ///
    /// # Type Parameters
    /// * `SK` - The secondary key type (must implement [`PrimaryKey`] so it
    ///   can be byte-encoded for the on-disk index).
    /// * `K` - The primary key type.
    /// * `E` - The entity type.
    /// * `S` - The entity serializer (shared via `Arc` so the key creator
    ///   can deserialise primary records on the write path).
    ///
    /// # Errors
    /// Returns an error if the store is not open or the underlying
    /// databases cannot be opened.
    #[allow(clippy::type_complexity)]
    pub fn open_secondary_index<SK, K, E, S, F>(
        &mut self,
        primary: &mut PrimaryIndex<K, E>,
        name: &str,
        serializer: Arc<S>,
        extractor: F,
    ) -> Result<SecondaryIndex<SK, K, E>>
    where
        SK: PrimaryKey + Ord + Send + Sync + 'static,
        K: PrimaryKey + Ord + Send + Sync + 'static,
        E: Entity<PrimaryKey = K> + Clone + Send + Sync + 'static,
        S: EntitySerializer<E> + Send + Sync + 'static,
        F: Fn(&E) -> Option<SK> + Send + Sync + 'static,
    {
        if !self.open {
            return Err(PersistError::StoreNotOpen);
        }

        let sec_db_name =
            format!("{}_{}_{}", self.config.store_name, E::entity_name(), name);

        // Idempotent: a second open of the same logical secondary returns a
        // fresh typed view over the already-registered SecondaryDatabase.
        if let Some(existing) = self.secondaries.get(&sec_db_name) {
            return Ok(SecondaryIndex::new(
                Arc::clone(existing),
                Arc::clone(&self.mutations),
            ));
        }

        let primary_shared = Arc::clone(primary.database_shared());

        // Inner index DB: sorted-dup, mirrors the primary's flags.
        let mut inner_cfg = DatabaseConfig::new();
        let effective_allow_create =
            self.config.allow_create || self.config.read_only;
        inner_cfg.set_allow_create(effective_allow_create);
        inner_cfg.set_read_only(self.config.read_only);
        inner_cfg.set_transactional(self.config.transactional);
        inner_cfg = inner_cfg.with_sorted_duplicates(true);
        let inner_db =
            self.env.open_database(None, &sec_db_name, &inner_cfg)?;

        // Build the PersistKeyCreator-equivalent.  It deserialises the
        // primary record (peeling the class-version envelope, honouring
        // schema evolution) and extracts + encodes the secondary key.
        let mutations = Arc::clone(&self.mutations);
        let ser_for_creator = Arc::clone(&serializer);
        let deserialize: Arc<dyn Fn(&[u8]) -> Result<E> + Send + Sync> =
            Arc::new(move |bytes: &[u8]| {
                decode_entity_record::<E, S>(
                    bytes,
                    ser_for_creator.as_ref(),
                    mutations.as_ref(),
                )
            });
        let extractor_arc: Arc<dyn Fn(&E) -> Option<SK> + Send + Sync> =
            Arc::new(extractor);
        let key_creator =
            crate::secondary_index::ExtractorKeyCreator::<SK, K, E>::new(
                deserialize,
                extractor_arc,
            );

        let sec_config = SecondaryConfig::new()
            .with_allow_create(effective_allow_create)
            .with_allow_populate(true)
            .with_sorted_duplicates(true)
            .with_key_creator(Box::new(key_creator));

        let secondary = Arc::new(SecondaryDatabase::open(
            primary_shared,
            inner_db,
            sec_config,
        )?);
        self.secondaries.insert(sec_db_name, Arc::clone(&secondary));

        Ok(SecondaryIndex::new(secondary, Arc::clone(&self.mutations)))
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

    /// Returns the registered mutations (defaults to an empty `Mutations`).
    pub fn mutations(&self) -> &Arc<Mutations> {
        &self.mutations
    }

    /// Returns the registered evolve config (defaults to evolve-all).
    pub fn evolve_config(&self) -> &Arc<EvolveConfig> {
        &self.evolve_config
    }

    /// Eagerly evolves all entity databases known to this store, applying
    /// the supplied [`Mutations`] in a single transaction per database.
    ///
    /// Unlike the v1.5.1 implementation this method
    /// no longer materialises the entire database into RAM via
    /// `scan_all_kv`; it streams records through a transactional
    /// cursor, decoding the per-record class version envelope and
    /// applying the matching class-level mutation.  Class-level
    /// **Deleter** drops the record; class-level **Converter** runs the
    /// user conversion on the payload (returning `None` deletes the
    /// record); records with no matching mutation but a stale envelope
    /// version are stamped with the current version.  Class-level
    /// **Renamer** is applied lazily via the read-side tag check on
    /// `PrimaryIndex::get`.
    ///
    /// The method honours the `EvolveConfig::should_evolve` filter and
    /// reports progress via the registered listener.  On success it
    /// updates the persistent class catalog so subsequent reopens see
    /// the evolution as already-applied.
    ///
    /// # Errors
    /// Returns an error if the store is not open, if any underlying
    /// database operation fails, or if the listener requests an early
    /// stop.
    pub fn evolve(
        &mut self,
        mutations: &Mutations,
        config: &EvolveConfig,
    ) -> Result<EvolveStats> {
        if !self.open {
            return Err(PersistError::StoreNotOpen);
        }
        if self.config.read_only {
            return Err(PersistError::DatabaseError(
                noxu_db::NoxuError::OperationNotAllowed(
                    "cannot evolve a read-only entity store".to_string(),
                ),
            ));
        }

        let mut stats = EvolveStats::new();

        // The convention used by get_primary_index is
        // "{store_name}_{entity_name}".
        let store_prefix = format!("{}_", self.config.store_name);
        let db_names: Vec<String> = self.databases.keys().cloned().collect();

        for db_name in &db_names {
            // Derive the entity class name from the database name.
            let entity_class =
                if let Some(suffix) = db_name.strip_prefix(&store_prefix) {
                    suffix.to_string()
                } else {
                    db_name.clone()
                };

            // Skip classes not targeted by the config.
            if !config.should_evolve(&entity_class) {
                continue;
            }

            if !self.databases.contains_key(db_name) {
                continue;
            }

            // Touch the catalog before borrowing the entity DB so
            // `catalog_mut()` doesn't fight us for `&mut self`.
            let target_version = self
                .catalog_mut()?
                .get(None, &entity_class)?
                .map(|e| e.class_version)
                .unwrap_or(0);

            let db = self.databases.get(db_name).unwrap();
            let catalog = self.catalog.as_ref().unwrap();
            let class_stats = stream_evolve_class(
                self.env,
                db,
                &entity_class,
                target_version,
                mutations,
                config,
                self.config.transactional,
                catalog,
            )?;
            stats.add(class_stats.n_read(), class_stats.n_converted());
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

        // Close all secondary index databases first (they hold the only
        // strong `Arc<SecondaryDatabase>` references; dropping them also
        // unregisters their `Weak` from the primary).  Then close the
        // primaries.
        let mut close_errors = Vec::new();
        for (name, sec) in self.secondaries.drain() {
            if let Err(e) = sec.close() {
                close_errors.push(format!("{}: {}", name, e));
            }
        }
        for (name, db) in self.databases.drain() {
            // The store owns the only strong ref to each primary at this
            // point (every `PrimaryIndex` borrowed it).  Locking and
            // closing is safe.
            if let Err(e) = db.lock().close() {
                close_errors.push(format!("{}: {}", name, e));
            }
        }

        if let Some(mut catalog) = self.catalog.take()
            && let Err(e) = catalog.close()
        {
            close_errors.push(format!("catalog: {}", e));
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
// Internal helper: stream-evolve a single entity database
// ---------------------------------------------------------------------------

/// Decision applied to a single record during streamed evolution.
enum EvolveAction {
    /// Drop the record.
    Delete,
    /// Replace the payload via a Converter and stamp the envelope with
    /// `target_version` + `entity_class` tag.
    RewriteWithConverter { new_payload: Vec<u8> },
    /// Keep the payload but fix the on-disk class tag (for class-level
    /// Renamer); preserve the on-disk class_version so the read-side
    /// `deserialize_versioned` still sees the original vintage.
    RewriteRename,
    /// Skip — the record needs no rewrite.  This includes records that
    /// already match the target shape **and** records at older
    /// versions for which no Converter / Deleter mutation was
    /// registered (lazy field-level evolution).
    Skip,
}

/// Streams every record in `db` through a single transaction, decoding
/// the envelope, applying the matching class-level mutation, and
/// rewriting the envelope with `target_version`.  Returns aggregated
/// stats.
///
/// On success the catalog is updated to record `target_version` for
/// `entity_class` (or the entry is removed if a class-level Deleter
/// fired).
///
/// All cursor reads/writes happen inside one [`Transaction`] (or
/// auto-commit if `transactional` is false), so a failure mid-stream
/// rolls the database back to its prior state.
#[allow(clippy::too_many_arguments)]
fn stream_evolve_class(
    env: &Environment,
    db: &Arc<Mutex<Database>>,
    entity_class: &str,
    target_version: u16,
    mutations: &Mutations,
    config: &EvolveConfig,
    transactional: bool,
    catalog: &ClassCatalog,
) -> Result<EvolveStats> {
    let mut stats = EvolveStats::new();

    // Open a write transaction unless the env or store is non-transactional.
    let txn: Option<Transaction> =
        if transactional { Some(env.begin_transaction(None)?) } else { None };
    let txn_ref = txn.as_ref();

    let db_guard = db.lock();
    let mut cursor = match txn_ref {
        Some(t) => db_guard.open_cursor_in(t, None)?,
        None => db_guard.open_cursor(None)?,
    };

    // Class-level deleter fires once we see any record that matches.
    let mut class_deleter_seen = false;

    let mut started = false;
    let mut n_read: u64 = 0;
    let mut n_converted: u64 = 0;

    loop {
        let mut key_entry = DatabaseEntry::new();
        let mut data_entry = DatabaseEntry::new();
        let get_type = if started { Get::Next } else { Get::First };
        started = true;

        let status =
            cursor.get(&mut key_entry, &mut data_entry, get_type, None)?;
        match status {
            OperationStatus::Success => {}
            _ => break,
        }

        let data_bytes = match data_entry.data_opt() {
            Some(b) => b.to_vec(),
            None => continue,
        };

        n_read += 1;
        let action = compute_evolve_action(
            &data_bytes,
            entity_class,
            target_version,
            mutations,
        )?;

        match action {
            EvolveAction::Skip => {}
            EvolveAction::Delete => {
                cursor.delete()?;
                n_converted += 1;
                class_deleter_seen = true;
            }
            EvolveAction::RewriteWithConverter { new_payload } => {
                let new_envelope = envelope::encode(
                    target_version,
                    entity_class,
                    &new_payload,
                )?;
                let new_data = DatabaseEntry::from_vec(new_envelope);
                let key_bytes = key_entry.data_opt().unwrap_or(&[]).to_vec();
                let key_entry_w = DatabaseEntry::from_vec(key_bytes);
                cursor.put(&key_entry_w, &new_data, Put::Current)?;
                n_converted += 1;
            }
            EvolveAction::RewriteRename => {
                // Preserve the original class_version, but fix the tag.
                let dec = envelope::decode(&data_bytes)?;
                let new_envelope = envelope::encode(
                    dec.class_version,
                    entity_class,
                    dec.payload,
                )?;
                let new_data = DatabaseEntry::from_vec(new_envelope);
                let key_bytes = key_entry.data_opt().unwrap_or(&[]).to_vec();
                let key_entry_w = DatabaseEntry::from_vec(key_bytes);
                cursor.put(&key_entry_w, &new_data, Put::Current)?;
                n_converted += 1;
            }
        }

        // Progress reporting.
        if let Some(listener) = config.listener()
            && !listener.evolve_progress(entity_class, n_read, n_converted)
        {
            cursor.close()?;
            drop(cursor);
            if let Some(t) = txn {
                t.abort()?;
            }
            return Err(PersistError::DatabaseError(
                noxu_db::NoxuError::OperationNotAllowed(format!(
                    "evolution of '{}' aborted by listener",
                    entity_class
                )),
            ));
        }
    }

    cursor.close()?;
    drop(cursor);

    // Update the catalog inside the same transaction so it commits or
    // aborts atomically with the data writes.
    if class_deleter_seen {
        catalog.remove(txn_ref, entity_class)?;
    } else {
        catalog.put(txn_ref, entity_class, target_version)?;
    }

    if let Some(t) = txn {
        t.commit()?;
    }

    stats.add(n_read, n_converted);
    Ok(stats)
}

/// Decides what to do with a single record during streamed evolution.
///
/// Decoding the envelope yields the on-disk class version and tag.  We
/// look up class-level mutations for `(entity_class, on_disk_version)`
/// (rather than always version 0).
///
/// Priority of actions, modeled on JE:
///
/// 1. **Class-level Deleter** at on-disk version → [`EvolveAction::Delete`].
/// 2. **Class-level Converter** at on-disk version →
///    [`EvolveAction::RewriteWithConverter`] (or `Delete` if the
///    converter returned `None`).
/// 3. **Class-level Renamer** at on-disk version with a tag mismatch
///    → [`EvolveAction::RewriteRename`] (preserves on-disk version).
/// 4. Otherwise → [`EvolveAction::Skip`].  The lazy field-level
///    evolution path (a user `deserialize_versioned` switching on
///    `class_version`) handles records whose envelope version is
///    older than the catalog target.  Stamping the envelope to a
///    newer version without a real payload transform would break
///    that contract — the next `get` would call
///    `deserialize_versioned(_, target_version, ...)` and the user
///    serializer would treat the bytes as the new shape.
fn compute_evolve_action(
    record: &[u8],
    entity_class: &str,
    _target_version: u16,
    mutations: &Mutations,
) -> Result<EvolveAction> {
    let dec = envelope::decode(record)?;

    // Resolve the class name through any class-level Renamer chain so
    // mutations registered against either the old or new name match.
    let mut on_disk_class = dec.class_tag.to_string();
    let mut renamer_active = false;
    if let Some(renamer) =
        mutations.get_renamer(&on_disk_class, dec.class_version.into(), None)
    {
        on_disk_class = renamer.new_name().to_string();
        renamer_active = true;
    }

    // Look up class-level mutations for the on-disk version.
    let cm = mutations
        .get_mutations_for_class(&on_disk_class, dec.class_version.into());

    if cm.deleter.is_some() {
        return Ok(EvolveAction::Delete);
    }
    if let Some(conv) = cm.converter {
        return match conv.convert(dec.payload) {
            Some(new_payload) => {
                Ok(EvolveAction::RewriteWithConverter { new_payload })
            }
            None => Ok(EvolveAction::Delete),
        };
    }

    // No Deleter / Converter.  If a class-level Renamer brought us to
    // the current entity name and the on-disk tag still has the old
    // name, fix the tag.
    if renamer_active && dec.class_tag != entity_class {
        return Ok(EvolveAction::RewriteRename);
    }

    // Otherwise leave the record alone (lazy semantics).
    Ok(EvolveAction::Skip)
}

/// Returns true if `mutations` contains any class-level
/// (renamer / deleter / converter) entry whose class name matches
/// `entity_name` either directly or as the *target* of a class-level
/// renamer.  Used by [`EntityStore::evolve_open_path`] to decide
/// whether scanning the entity database is needed.
fn mutations_apply_to(mutations: &Mutations, entity_name: &str) -> bool {
    // Direct matches: mutation registered against the new (current) name.
    if mutations.renamers().any(|r| {
        r.field_name().is_none()
            && (r.class_name() == entity_name || r.new_name() == entity_name)
    }) {
        return true;
    }
    if mutations.deleters().any(|d| d.class_name() == entity_name) {
        return true;
    }
    if mutations.converters().any(|c| c.class_name() == entity_name) {
        return true;
    }
    // Class-level renamer chain: a registered renamer X -> Y means
    // records tagged "X" should also be evolved when the user is now
    // using entity_name() = "Y".
    mutations
        .renamers()
        .any(|r| r.field_name().is_none() && r.new_name() == entity_name)
}

/// Decodes a full primary record (class-version envelope + payload) into an
/// entity, honouring class-level Renamer mutations and dispatching to
/// [`EntitySerializer::deserialize_versioned`].  Shared by the secondary
/// key-creator bridge ([`crate::secondary_index::ExtractorKeyCreator`]) so
/// the secondary key is extracted with the same semantics as
/// `PrimaryIndex::get`.
fn decode_entity_record<E, S>(
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
        index.put(None, &ser, &user).unwrap();

        // Read
        let found = index.get(None, &ser, &1u64).unwrap().unwrap();
        assert_eq!(found, user);

        // Update
        let updated = User {
            id: 1,
            name: "Alice Updated".to_string(),
            email: "alice.new@example.com".to_string(),
        };
        index.put(None, &ser, &updated).unwrap();
        let found = index.get(None, &ser, &1u64).unwrap().unwrap();
        assert_eq!(found.name, "Alice Updated");

        // Delete
        let deleted = index.delete(None, &1u64).unwrap();
        assert!(deleted);
        assert_eq!(index.get(None, &ser, &1u64).unwrap(), None);
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
                    None,
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
                    None,
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
            let found_user =
                user_index.get(None, &user_ser, &1u64).unwrap().unwrap();
            assert_eq!(found_user.name, "Alice");
        }
        {
            let product_index: PrimaryIndex<String, Product> =
                store.get_primary_index().unwrap();
            let found_product = product_index
                .get(None, &product_ser, &"SKU-001".to_string())
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
        index.put(None, &ser, &product).unwrap();

        let found =
            index.get(None, &ser, &"ABC-123".to_string()).unwrap().unwrap();
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
                    None,
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
            .entities(None, &ser)
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
    fn test_evolve_empty_mutations_streams_records_no_converted() {
        // Wave 2C-2: the streamed evolve no longer takes the
        // pre-2C-2 "early-return when nothing matches" shortcut.  With
        // an empty `Mutations` set every record is *read* (n_read
        // counts the cursor scan) but none are *converted* because the
        // on-disk envelope already matches the catalog target version.
        let (_td, env) = temp_env();
        let config = StoreConfig::new("store").with_allow_create(true);
        let mut store = EntityStore::open(&env, config).unwrap();
        let ser = UserSerializer;

        let index: PrimaryIndex<u64, User> = store.get_primary_index().unwrap();
        index
            .put(
                None,
                &ser,
                &User { id: 1, name: "A".into(), email: "a@a.com".into() },
            )
            .unwrap();
        drop(index);

        let mutations = crate::evolve::Mutations::new();
        let evolve_cfg = crate::evolve::EvolveConfig::new();
        let stats = store.evolve(&mutations, &evolve_cfg).unwrap();
        assert_eq!(stats.n_read(), 1);
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
            let index: PrimaryIndex<u64, User> =
                store.get_primary_index().unwrap();
            for i in 1u64..=3 {
                index
                    .put(
                        None,
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
        mutations.add_converter(Converter::for_class(
            "User",
            0,
            |b: &[u8]| {
                let mut out = b.to_vec();
                out.push(0xFF); // append sentinel to detect conversion
                Some(out)
            },
        ));

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
            let index: PrimaryIndex<u64, User> =
                store.get_primary_index().unwrap();
            for i in 1u64..=2 {
                index
                    .put(
                        None,
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
            let index: PrimaryIndex<u64, User> =
                store.get_primary_index().unwrap();
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
            let index: PrimaryIndex<u64, User> =
                store.get_primary_index().unwrap();
            index
                .put(
                    None,
                    &ser,
                    &User { id: 1, name: "A".into(), email: "a@a.com".into() },
                )
                .unwrap();
        }

        let mut mutations = Mutations::new();
        mutations.add_converter(Converter::for_class(
            "User",
            0,
            |b: &[u8]| Some(b.to_vec()),
        ));

        // Config targets a *different* class → User should be skipped.
        let evolve_cfg =
            EvolveConfig::new().with_class_to_evolve("SomeOtherClass");
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
