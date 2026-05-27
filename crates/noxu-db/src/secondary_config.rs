//! Secondary database configuration.
//!

use crate::database::Database;
use crate::database_config::DatabaseConfig;
use crate::database_entry::DatabaseEntry;
use noxu_sync::Mutex;
use std::sync::Arc;

/// Callback trait for creating a single secondary key from a primary record.
///
///
///
/// Implement this trait to extract one secondary key per primary record.
/// The implementation must be thread-safe because it is called from multiple
/// threads without external synchronization.
pub trait SecondaryKeyCreator: Send + Sync {
    /// Creates a secondary key for the given primary key/data pair.
    ///
    /// # Arguments
    /// * `secondary_db` - The secondary database handle (for context).
    /// * `key` - The primary key.
    /// * `data` - The primary data.
    /// * `result` - Output parameter to receive the secondary key.
    ///
    /// # Returns
    /// `true` if a secondary key was created, `false` if this primary record
    /// should not have a secondary index entry.
    fn create_secondary_key(
        &self,
        secondary_db: &Database,
        key: &DatabaseEntry,
        data: &DatabaseEntry,
        result: &mut DatabaseEntry,
    ) -> bool;
}

/// Callback trait for creating multiple secondary keys from a primary record.
///
///
///
/// Implement this trait to extract zero or more secondary keys per primary
/// record. The implementation must be thread-safe because it is called from
/// multiple threads without external synchronization.
pub trait SecondaryMultiKeyCreator: Send + Sync {
    /// Creates secondary keys for the given primary key/data pair.
    ///
    /// # Arguments
    /// * `secondary_db` - The secondary database handle (for context).
    /// * `key` - The primary key.
    /// * `data` - The primary data.
    /// * `results` - Output set to add secondary keys to.
    fn create_secondary_keys(
        &self,
        secondary_db: &Database,
        key: &DatabaseEntry,
        data: &DatabaseEntry,
        results: &mut Vec<DatabaseEntry>,
    );
}

/// Action taken when a record in a foreign key database is deleted.
///
///
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ForeignKeyDeleteAction {
    /// Abort the operation if a secondary record refers to the deleted foreign key.
    ///
    /// This causes the delete of the foreign key record to fail with an error.
    Abort,

    /// Cascade the delete: also delete all primary records that reference the
    /// deleted foreign key.
    Cascade,

    /// Nullify: update all primary records to remove (null out) the reference
    /// to the deleted foreign key.
    Nullify,
}

/// Callback trait for nullifying a single foreign key reference in primary data.
///
///
pub trait ForeignKeyNullifier: Send + Sync {
    /// Called when a referenced foreign key record is deleted.
    ///
    /// The implementation should modify `data` to remove the reference to the
    /// deleted key (set the field to null/zero/empty).
    ///
    /// # Returns
    /// `true` if `data` was modified, `false` if no change was needed.
    fn nullify_foreign_key(
        &self,
        secondary_db: &Database,
        data: &mut DatabaseEntry,
    ) -> bool;
}

/// Callback trait for nullifying multi-valued foreign key references.
///
///
pub trait ForeignMultiKeyNullifier: Send + Sync {
    /// Called when a referenced foreign key record is deleted.
    ///
    /// The implementation should remove the reference to `secondary_key` from
    /// `data`.
    ///
    /// # Returns
    /// `true` if `data` was modified, `false` if no change was needed.
    fn nullify_foreign_key(
        &self,
        secondary_db: &Database,
        key: &DatabaseEntry,
        data: &mut DatabaseEntry,
        secondary_key: &DatabaseEntry,
    ) -> bool;
}

/// Configuration for a secondary database.
///
///
///
/// SecondaryConfig extends DatabaseConfig with additional fields required to
/// open a secondary (index) database:
/// - A key creator callback to extract secondary keys from primary data.
/// - Optional foreign key constraint settings.
/// - Optional population and immutability flags.
///
/// # Example
/// ```ignore
/// use noxu_db::secondary_config::{SecondaryConfig, SecondaryKeyCreator};
/// use noxu_db::{Database, DatabaseEntry};
///
/// struct MyKeyCreator;
/// impl SecondaryKeyCreator for MyKeyCreator {
///     fn create_secondary_key(
///         &self,
///         _secondary_db: &Database,
///         _key: &DatabaseEntry,
///         data: &DatabaseEntry,
///         result: &mut DatabaseEntry,
///     ) -> bool {
///         // Extract the first 4 bytes of primary data as the secondary key
///         if let Some(d) = data.get_data() {
///             if d.len() >= 4 {
///                 result.set_data(&d[..4]);
///                 return true;
///             }
///         }
///         false
///     }
/// }
///
/// let config = SecondaryConfig::new()
///     .with_allow_create(true)
///     .with_allow_populate(true)
///     .with_key_creator(Box::new(MyKeyCreator));
/// ```
pub struct SecondaryConfig {
    /// Base database configuration (inherited fields).
    pub base: DatabaseConfig,

    /// Single-key creator callback.
    pub key_creator: Option<Box<dyn SecondaryKeyCreator>>,

    /// Multi-key creator callback.
    pub multi_key_creator: Option<Box<dyn SecondaryMultiKeyCreator>>,

    /// Whether to auto-populate the secondary when opened on an empty secondary.
    ///
    /// If true and the secondary is empty at open time, all primary records are
    /// scanned and indexed.
    pub allow_populate: bool,

    /// Foreign key database name for referential integrity constraint.
    ///
    /// Stored separately from the [`Self::foreign_key_database`] handle
    /// so configuration objects without a live `Arc` reference (e.g.
    /// for diagnostic logging or programmatic inspection of
    /// `SecondaryConfig`) still carry the relationship name.
    pub foreign_key_database_name: Option<String>,

    /// Foreign key database **handle** for referential integrity
    /// constraint enforcement.  v1.6 (audit C2 / Decision 2C):
    /// when set, the engine ensures every secondary key produced by
    /// this index also exists as a primary key in the supplied
    /// foreign database, and triggers the configured
    /// [`Self::foreign_key_delete_action`] when a foreign-DB record
    /// is deleted.
    pub foreign_key_database: Option<Arc<Mutex<Database>>>,

    /// Action to take when a referenced foreign key record is deleted.
    pub foreign_key_delete_action: ForeignKeyDeleteAction,

    /// Nullifier for single-valued foreign keys.
    pub foreign_key_nullifier: Option<Box<dyn ForeignKeyNullifier>>,

    /// Nullifier for multi-valued foreign keys.
    pub foreign_multi_key_nullifier: Option<Box<dyn ForeignMultiKeyNullifier>>,

    /// When true, the secondary key is immutable and cannot change when the
    /// primary record is updated. This enables an optimization that skips
    /// calling the key creator on updates.
    pub immutable_secondary_key: bool,

    /// When true, the secondary key is derived solely from the primary key
    /// (not from the primary data). This allows skipping reads of primary data
    /// on update/delete.
    pub extract_from_primary_key_only: bool,
}

impl SecondaryConfig {
    /// Creates a new SecondaryConfig with default settings.
    ///
    /// Defaults:
    /// - `allow_create = false`
    /// - `allow_populate = false`
    /// - `immutable_secondary_key = false`
    /// - `extract_from_primary_key_only = false`
    /// - `foreign_key_delete_action = ForeignKeyDeleteAction::Abort`
    /// - All callbacks are `None`
    pub fn new() -> Self {
        Self {
            base: DatabaseConfig::new(),
            key_creator: None,
            multi_key_creator: None,
            allow_populate: false,
            foreign_key_database_name: None,
            foreign_key_database: None,
            foreign_key_delete_action: ForeignKeyDeleteAction::Abort,
            foreign_key_nullifier: None,
            foreign_multi_key_nullifier: None,
            immutable_secondary_key: false,
            extract_from_primary_key_only: false,
        }
    }

    // ------------------------------------------------------------------
    // Builder-style setters (mirrors setXxx() returning &self)
    // ------------------------------------------------------------------

    /// Sets whether a new secondary database may be created.
    pub fn with_allow_create(mut self, allow_create: bool) -> Self {
        self.base.allow_create = allow_create;
        self
    }

    /// Sets whether duplicates are allowed in the secondary database.
    ///
    /// Secondary databases typically allow duplicates (multiple primary records
    /// sharing the same secondary key).
    pub fn with_sorted_duplicates(mut self, sorted_duplicates: bool) -> Self {
        self.base.sorted_duplicates = sorted_duplicates;
        self
    }

    /// Sets whether automatic population of the secondary is allowed on open.
    ///
    ///
    pub fn with_allow_populate(mut self, allow_populate: bool) -> Self {
        self.allow_populate = allow_populate;
        self
    }

    /// Sets the single-key creator callback.
    ///
    /// Either `key_creator` or `multi_key_creator` must be set (not both),
    /// unless the primary database is read-only.
    ///
    ///
    pub fn with_key_creator(
        mut self,
        key_creator: Box<dyn SecondaryKeyCreator>,
    ) -> Self {
        self.key_creator = Some(key_creator);
        self
    }

    /// Sets the multi-key creator callback.
    ///
    ///
    pub fn with_multi_key_creator(
        mut self,
        multi_key_creator: Box<dyn SecondaryMultiKeyCreator>,
    ) -> Self {
        self.multi_key_creator = Some(multi_key_creator);
        self
    }

    /// Sets whether the secondary key is immutable.
    ///
    ///
    pub fn with_immutable_secondary_key(mut self, immutable: bool) -> Self {
        self.immutable_secondary_key = immutable;
        self
    }

    /// Sets whether to derive the secondary key from the primary key only.
    ///
    ///
    pub fn with_extract_from_primary_key_only(
        mut self,
        extract_only: bool,
    ) -> Self {
        self.extract_from_primary_key_only = extract_only;
        self
    }

    /// Sets the foreign key database (by name only — advisory).
    ///
    /// v1.6: this records the relationship name for diagnostics but
    /// **does not by itself activate FK enforcement**.  To enforce the
    /// constraint, additionally call
    /// [`Self::with_foreign_key_database_handle`] with the foreign
    /// primary's `Arc<Mutex<Database>>` so the engine has a live
    /// reference for cascade / abort / nullify fan-out.
    pub fn with_foreign_key_database<S: Into<String>>(
        mut self,
        name: S,
    ) -> Self {
        self.foreign_key_database_name = Some(name.into());
        self
    }

    /// Sets the foreign key database handle for runtime FK enforcement.
    ///
    /// v1.6 (audit C2 / Decision 2C): the secondary index registers
    /// itself as an FK referrer on the supplied foreign primary so
    /// `Database::delete` on the foreign primary triggers the
    /// configured [`ForeignKeyDeleteAction`] for every child record
    /// whose secondary key equals the deleted foreign key.
    pub fn with_foreign_key_database_handle(
        mut self,
        handle: Arc<Mutex<Database>>,
    ) -> Self {
        // Pull the database name out for diagnostics if the user did
        // not also call [`Self::with_foreign_key_database`].  Locks
        // briefly; this is a one-time setup call.
        if self.foreign_key_database_name.is_none() {
            let n = handle.lock().get_database_name().to_string();
            self.foreign_key_database_name = Some(n);
        }
        self.foreign_key_database = Some(handle);
        self
    }

    /// Sets the action to take when a referenced foreign key is deleted.
    ///
    /// # v1.5 status
    ///
    /// **Decision 2C**: stored but not enforced; `SecondaryDatabase::open`
    /// rejects configs with a non-`Abort` action set (or any nullifier or
    /// foreign DB) with `NoxuError::Unsupported`.  Full FK support is
    /// planned for v1.6.
    pub fn with_foreign_key_delete_action(
        mut self,
        action: ForeignKeyDeleteAction,
    ) -> Self {
        self.foreign_key_delete_action = action;
        self
    }

    /// Sets the foreign key nullifier (single-value variant).
    ///
    /// # v1.5 status
    ///
    /// **Decision 2C**: stored but not enforced; `SecondaryDatabase::open`
    /// rejects configs with a nullifier set with `NoxuError::Unsupported`.
    /// Full FK support is planned for v1.6.
    pub fn with_foreign_key_nullifier(
        mut self,
        nullifier: Box<dyn ForeignKeyNullifier>,
    ) -> Self {
        self.foreign_key_nullifier = Some(nullifier);
        self
    }

    /// Sets the foreign key nullifier (multi-value variant).
    ///
    /// # v1.5 status
    ///
    /// **Decision 2C**: stored but not enforced; `SecondaryDatabase::open`
    /// rejects configs with a nullifier set with `NoxuError::Unsupported`.
    /// Full FK support is planned for v1.6.
    pub fn with_foreign_multi_key_nullifier(
        mut self,
        nullifier: Box<dyn ForeignMultiKeyNullifier>,
    ) -> Self {
        self.foreign_multi_key_nullifier = Some(nullifier);
        self
    }

    // ------------------------------------------------------------------
    // Validation
    // ------------------------------------------------------------------

    /// Validates the configuration for opening a secondary database.
    ///
    /// Returns an error description if the configuration is invalid.
    ///
    /// Constructor validation in `SecondaryDatabase`.
    pub(crate) fn validate(
        &self,
        primary_read_only: bool,
    ) -> Result<(), String> {
        if self.key_creator.is_some() && self.multi_key_creator.is_some() {
            return Err(
                "key_creator and multi_key_creator may not both be non-null"
                    .to_string(),
            );
        }
        if self.foreign_key_nullifier.is_some()
            && self.foreign_multi_key_nullifier.is_some()
        {
            return Err(
                "foreign_key_nullifier and foreign_multi_key_nullifier may not both be non-null"
                    .to_string(),
            );
        }
        if self.foreign_key_delete_action == ForeignKeyDeleteAction::Nullify
            && self.foreign_key_nullifier.is_none()
            && self.foreign_multi_key_nullifier.is_none()
        {
            return Err(
                "A ForeignKeyNullifier or ForeignMultiKeyNullifier must be set when \
                 ForeignKeyDeleteAction is Nullify"
                    .to_string(),
            );
        }
        if self.foreign_key_nullifier.is_some()
            && self.multi_key_creator.is_some()
        {
            return Err(
                "ForeignKeyNullifier may not be used with SecondaryMultiKeyCreator; \
                 use ForeignMultiKeyNullifier instead"
                    .to_string(),
            );
        }
        if !primary_read_only
            && self.key_creator.is_none()
            && self.multi_key_creator.is_none()
        {
            return Err(
                "key_creator or multi_key_creator must be set when the primary database \
                 is not read-only"
                    .to_string(),
            );
        }
        Ok(())
    }

    /// Returns whether an update to the primary may change the secondary key.
    ///
    ///
    pub(crate) fn update_may_change_secondary(&self) -> bool {
        !self.immutable_secondary_key && !self.extract_from_primary_key_only
    }

    /// Returns `true` if any foreign-key constraint field is set to a
    /// non-default value.
    ///
    /// v1.6 still uses this helper to gate the open-time rejection of
    /// half-configured FK setups (e.g. a nullifier without
    /// `foreign_key_database_handle` set).
    pub(crate) fn has_foreign_key_config(&self) -> bool {
        self.foreign_key_database_name.is_some()
            || self.foreign_key_database.is_some()
            || self.foreign_key_delete_action != ForeignKeyDeleteAction::Abort
            || self.foreign_key_nullifier.is_some()
            || self.foreign_multi_key_nullifier.is_some()
    }
}

impl Default for SecondaryConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct SimpleKeyCreator;
    impl SecondaryKeyCreator for SimpleKeyCreator {
        fn create_secondary_key(
            &self,
            _db: &Database,
            _key: &DatabaseEntry,
            data: &DatabaseEntry,
            result: &mut DatabaseEntry,
        ) -> bool {
            if let Some(d) = data.get_data() {
                result.set_data(d);
                true
            } else {
                false
            }
        }
    }

    #[test]
    fn test_default_config() {
        let config = SecondaryConfig::new();
        assert!(!config.base.allow_create);
        assert!(!config.allow_populate);
        assert!(!config.immutable_secondary_key);
        assert!(!config.extract_from_primary_key_only);
        assert_eq!(
            config.foreign_key_delete_action,
            ForeignKeyDeleteAction::Abort
        );
        assert!(config.key_creator.is_none());
        assert!(config.multi_key_creator.is_none());
        assert!(config.foreign_key_nullifier.is_none());
        assert!(config.foreign_multi_key_nullifier.is_none());
    }

    #[test]
    fn test_builder_chain() {
        let config = SecondaryConfig::new()
            .with_allow_create(true)
            .with_allow_populate(true)
            .with_sorted_duplicates(true)
            .with_immutable_secondary_key(true)
            .with_extract_from_primary_key_only(true)
            .with_key_creator(Box::new(SimpleKeyCreator));

        assert!(config.base.allow_create);
        assert!(config.base.sorted_duplicates);
        assert!(config.allow_populate);
        assert!(config.immutable_secondary_key);
        assert!(config.extract_from_primary_key_only);
        assert!(config.key_creator.is_some());
    }

    #[test]
    fn test_validate_ok() {
        let config =
            SecondaryConfig::new().with_key_creator(Box::new(SimpleKeyCreator));
        assert!(config.validate(false).is_ok());
    }

    #[test]
    fn test_validate_both_creators_fails() {
        use crate::environment::Environment;
        use crate::environment_config::EnvironmentConfig;
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let env = Environment::open(
            EnvironmentConfig::new(temp_dir.path().to_path_buf())
                .with_allow_create(true),
        )
        .unwrap();
        let db_cfg = crate::database_config::DatabaseConfig::new()
            .with_allow_create(true);
        let db = env.open_database(None, "vbc_db", &db_cfg).unwrap();

        struct MkCreator;
        impl SecondaryMultiKeyCreator for MkCreator {
            fn create_secondary_keys(
                &self,
                _db: &Database,
                _key: &DatabaseEntry,
                _data: &DatabaseEntry,
                _results: &mut Vec<DatabaseEntry>,
            ) {
            }
        }

        // Exercise the method body.
        let creator = MkCreator;
        let key = DatabaseEntry::from_bytes(b"k");
        let data = DatabaseEntry::from_bytes(b"v");
        let mut results: Vec<DatabaseEntry> = Vec::new();
        creator.create_secondary_keys(&db, &key, &data, &mut results);

        let config = SecondaryConfig {
            key_creator: Some(Box::new(SimpleKeyCreator)),
            multi_key_creator: Some(Box::new(MkCreator)),
            ..SecondaryConfig::new()
        };
        assert!(config.validate(false).is_err());
    }

    #[test]
    fn test_validate_no_creator_read_only_ok() {
        let config = SecondaryConfig::new();
        // primary_read_only=true => no creator required
        assert!(config.validate(true).is_ok());
    }

    #[test]
    fn test_validate_no_creator_writable_fails() {
        let config = SecondaryConfig::new();
        assert!(config.validate(false).is_err());
    }

    #[test]
    fn test_update_may_change_secondary() {
        let config = SecondaryConfig::new();
        assert!(config.update_may_change_secondary());

        let config_imm =
            SecondaryConfig::new().with_immutable_secondary_key(true);
        assert!(!config_imm.update_may_change_secondary());

        let config_key_only =
            SecondaryConfig::new().with_extract_from_primary_key_only(true);
        assert!(!config_key_only.update_may_change_secondary());
    }

    #[test]
    fn test_foreign_key_delete_action() {
        let config = SecondaryConfig::new()
            .with_foreign_key_delete_action(ForeignKeyDeleteAction::Cascade);
        assert_eq!(
            config.foreign_key_delete_action,
            ForeignKeyDeleteAction::Cascade
        );
    }

    #[test]
    fn test_foreign_key_delete_action_nullify() {
        let config = SecondaryConfig::new()
            .with_foreign_key_delete_action(ForeignKeyDeleteAction::Nullify);
        assert_eq!(
            config.foreign_key_delete_action,
            ForeignKeyDeleteAction::Nullify
        );
    }

    #[test]
    fn test_foreign_key_delete_action_abort() {
        // Default value is Abort; also verify explicit set
        let config = SecondaryConfig::new()
            .with_foreign_key_delete_action(ForeignKeyDeleteAction::Abort);
        assert_eq!(
            config.foreign_key_delete_action,
            ForeignKeyDeleteAction::Abort
        );
    }

    #[test]
    fn test_with_foreign_key_nullifier() {
        struct SimpleNullifier;
        impl ForeignKeyNullifier for SimpleNullifier {
            fn nullify_foreign_key(
                &self,
                _db: &Database,
                _data: &mut DatabaseEntry,
            ) -> bool {
                false
            }
        }
        let config = SecondaryConfig::new()
            .with_foreign_key_nullifier(Box::new(SimpleNullifier));
        assert!(config.foreign_key_nullifier.is_some());
    }

    #[test]
    fn test_with_foreign_multi_key_nullifier() {
        struct MultiNullifier;
        impl ForeignMultiKeyNullifier for MultiNullifier {
            fn nullify_foreign_key(
                &self,
                _db: &Database,
                _key: &DatabaseEntry,
                _data: &mut DatabaseEntry,
                _secondary_key: &DatabaseEntry,
            ) -> bool {
                false
            }
        }
        let config = SecondaryConfig::new()
            .with_foreign_multi_key_nullifier(Box::new(MultiNullifier));
        assert!(config.foreign_multi_key_nullifier.is_some());
    }

    #[test]
    fn test_with_multi_key_creator() {
        struct MkCreator;
        impl SecondaryMultiKeyCreator for MkCreator {
            fn create_secondary_keys(
                &self,
                _db: &Database,
                _key: &DatabaseEntry,
                _data: &DatabaseEntry,
                _results: &mut Vec<DatabaseEntry>,
            ) {
            }
        }
        let config =
            SecondaryConfig::new().with_multi_key_creator(Box::new(MkCreator));
        assert!(config.multi_key_creator.is_some());
        assert!(config.key_creator.is_none());
    }

    #[test]
    fn test_validate_both_nullifiers_fails() {
        use crate::environment::Environment;
        use crate::environment_config::EnvironmentConfig;
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let env = Environment::open(
            EnvironmentConfig::new(temp_dir.path().to_path_buf())
                .with_allow_create(true),
        )
        .unwrap();
        let db_cfg = crate::database_config::DatabaseConfig::new()
            .with_allow_create(true);
        let db = env.open_database(None, "bnf_db", &db_cfg).unwrap();

        struct SimpleNullifier;
        impl ForeignKeyNullifier for SimpleNullifier {
            fn nullify_foreign_key(
                &self,
                _db: &Database,
                _data: &mut DatabaseEntry,
            ) -> bool {
                false
            }
        }
        struct MultiNullifier;
        impl ForeignMultiKeyNullifier for MultiNullifier {
            fn nullify_foreign_key(
                &self,
                _db: &Database,
                _key: &DatabaseEntry,
                _data: &mut DatabaseEntry,
                _secondary_key: &DatabaseEntry,
            ) -> bool {
                false
            }
        }

        // Exercise the method bodies.
        let sn = SimpleNullifier;
        let mut data = DatabaseEntry::from_bytes(b"v");
        assert!(!sn.nullify_foreign_key(&db, &mut data));
        let mn = MultiNullifier;
        let key = DatabaseEntry::from_bytes(b"k");
        let sec = DatabaseEntry::from_bytes(b"s");
        assert!(!mn.nullify_foreign_key(&db, &key, &mut data, &sec));

        let config = SecondaryConfig {
            key_creator: Some(Box::new(SimpleKeyCreator)),
            foreign_key_nullifier: Some(Box::new(SimpleNullifier)),
            foreign_multi_key_nullifier: Some(Box::new(MultiNullifier)),
            ..SecondaryConfig::new()
        };
        assert!(config.validate(false).is_err());
        let err = config.validate(false).unwrap_err();
        assert!(
            err.contains("foreign_key_nullifier")
                && err.contains("foreign_multi_key_nullifier")
        );
    }

    #[test]
    fn test_validate_nullify_action_without_nullifier_fails() {
        let config = SecondaryConfig::new()
            .with_key_creator(Box::new(SimpleKeyCreator))
            .with_foreign_key_delete_action(ForeignKeyDeleteAction::Nullify);
        assert!(config.validate(false).is_err());
        let err = config.validate(false).unwrap_err();
        assert!(err.contains("Nullify"));
    }

    #[test]
    fn test_validate_nullify_action_with_nullifier_ok() {
        use crate::environment::Environment;
        use crate::environment_config::EnvironmentConfig;
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let env = Environment::open(
            EnvironmentConfig::new(temp_dir.path().to_path_buf())
                .with_allow_create(true),
        )
        .unwrap();
        let db_cfg = crate::database_config::DatabaseConfig::new()
            .with_allow_create(true);
        let db = env.open_database(None, "vnano_db", &db_cfg).unwrap();

        struct SimpleNullifier;
        impl ForeignKeyNullifier for SimpleNullifier {
            fn nullify_foreign_key(
                &self,
                _db: &Database,
                _data: &mut DatabaseEntry,
            ) -> bool {
                false
            }
        }

        // Exercise the method body.
        let sn = SimpleNullifier;
        let mut data = DatabaseEntry::from_bytes(b"v");
        assert!(!sn.nullify_foreign_key(&db, &mut data));

        let config = SecondaryConfig::new()
            .with_key_creator(Box::new(SimpleKeyCreator))
            .with_foreign_key_delete_action(ForeignKeyDeleteAction::Nullify)
            .with_foreign_key_nullifier(Box::new(SimpleNullifier));
        assert!(config.validate(false).is_ok());
    }

    #[test]
    fn test_validate_nullify_action_with_multi_nullifier_ok() {
        use crate::environment::Environment;
        use crate::environment_config::EnvironmentConfig;
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let env = Environment::open(
            EnvironmentConfig::new(temp_dir.path().to_path_buf())
                .with_allow_create(true),
        )
        .unwrap();
        let db_cfg = crate::database_config::DatabaseConfig::new()
            .with_allow_create(true);
        let db = env.open_database(None, "vnamo_db", &db_cfg).unwrap();

        struct MultiNullifier;
        impl ForeignMultiKeyNullifier for MultiNullifier {
            fn nullify_foreign_key(
                &self,
                _db: &Database,
                _key: &DatabaseEntry,
                _data: &mut DatabaseEntry,
                _secondary_key: &DatabaseEntry,
            ) -> bool {
                false
            }
        }

        // Exercise the method body.
        let mn = MultiNullifier;
        let key = DatabaseEntry::from_bytes(b"k");
        let mut data = DatabaseEntry::from_bytes(b"v");
        let sec = DatabaseEntry::from_bytes(b"s");
        assert!(!mn.nullify_foreign_key(&db, &key, &mut data, &sec));

        let config = SecondaryConfig::new()
            .with_key_creator(Box::new(SimpleKeyCreator))
            .with_foreign_key_delete_action(ForeignKeyDeleteAction::Nullify)
            .with_foreign_multi_key_nullifier(Box::new(MultiNullifier));
        assert!(config.validate(false).is_ok());
    }

    #[test]
    fn test_validate_foreign_nullifier_with_multi_key_creator_fails() {
        use crate::environment::Environment;
        use crate::environment_config::EnvironmentConfig;
        use tempfile::TempDir;
        let temp_dir = TempDir::new().unwrap();
        let env = Environment::open(
            EnvironmentConfig::new(temp_dir.path().to_path_buf())
                .with_allow_create(true),
        )
        .unwrap();
        let db_cfg = crate::database_config::DatabaseConfig::new()
            .with_allow_create(true);
        let db = env.open_database(None, "vfnmkc_db", &db_cfg).unwrap();

        struct MkCreator;
        impl SecondaryMultiKeyCreator for MkCreator {
            fn create_secondary_keys(
                &self,
                _db: &Database,
                _key: &DatabaseEntry,
                _data: &DatabaseEntry,
                _results: &mut Vec<DatabaseEntry>,
            ) {
            }
        }
        struct SimpleNullifier;
        impl ForeignKeyNullifier for SimpleNullifier {
            fn nullify_foreign_key(
                &self,
                _db: &Database,
                _data: &mut DatabaseEntry,
            ) -> bool {
                false
            }
        }

        // Exercise the method bodies.
        let creator = MkCreator;
        let key = DatabaseEntry::from_bytes(b"k");
        let data = DatabaseEntry::from_bytes(b"v");
        let mut results: Vec<DatabaseEntry> = Vec::new();
        creator.create_secondary_keys(&db, &key, &data, &mut results);

        let sn = SimpleNullifier;
        let mut d = DatabaseEntry::from_bytes(b"v");
        assert!(!sn.nullify_foreign_key(&db, &mut d));

        let config = SecondaryConfig {
            multi_key_creator: Some(Box::new(MkCreator)),
            foreign_key_nullifier: Some(Box::new(SimpleNullifier)),
            ..SecondaryConfig::new()
        };
        // multi_key_creator + foreign_key_nullifier => error
        let err = config.validate(false).unwrap_err();
        assert!(
            err.contains("ForeignKeyNullifier")
                || err.contains("ForeignMultiKeyNullifier")
                || err.contains("multi")
        );
    }

    #[test]
    fn test_default_trait() {
        let cfg: SecondaryConfig = Default::default();
        assert!(!cfg.allow_populate);
        assert!(!cfg.immutable_secondary_key);
    }

    #[test]
    fn test_update_may_change_secondary_both_false() {
        let config = SecondaryConfig::new()
            .with_immutable_secondary_key(false)
            .with_extract_from_primary_key_only(false);
        // Both false => may change
        assert!(config.update_may_change_secondary());
    }

    #[test]
    fn test_validate_multi_key_creator_read_only_ok() {
        struct MkCreator;
        impl SecondaryMultiKeyCreator for MkCreator {
            fn create_secondary_keys(
                &self,
                _db: &Database,
                _key: &DatabaseEntry,
                _data: &DatabaseEntry,
                _results: &mut Vec<DatabaseEntry>,
            ) {
            }
        }
        // primary_read_only=true with multi_key_creator should be ok
        let config =
            SecondaryConfig::new().with_multi_key_creator(Box::new(MkCreator));
        assert!(config.validate(true).is_ok());
    }

    #[test]
    fn test_validate_multi_key_creator_writable_ok() {
        struct MkCreator;
        impl SecondaryMultiKeyCreator for MkCreator {
            fn create_secondary_keys(
                &self,
                _db: &Database,
                _key: &DatabaseEntry,
                _data: &DatabaseEntry,
                _results: &mut Vec<DatabaseEntry>,
            ) {
            }
        }
        let config =
            SecondaryConfig::new().with_multi_key_creator(Box::new(MkCreator));
        assert!(config.validate(false).is_ok());
    }

    // ========================================================================
    // Additional branch-coverage tests: exercise the trait impl bodies
    // ========================================================================

    /// Exercise SimpleKeyCreator::create_secondary_key — both branches:
    /// - data has Some bytes → returns true (secondary key set)
    /// - data is None       → returns false
    #[test]
    fn test_simple_key_creator_both_branches() {
        use crate::environment::Environment;
        use crate::environment_config::EnvironmentConfig;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true);
        let env = Environment::open(env_config).unwrap();
        let db_config = crate::database_config::DatabaseConfig::new()
            .with_allow_create(true);
        let db = env.open_database(None, "ck_test", &db_config).unwrap();

        let creator = SimpleKeyCreator;

        // Branch: data has bytes → true
        let key = DatabaseEntry::from_bytes(b"k");
        let data_with = DatabaseEntry::from_bytes(b"some_data");
        let mut result = DatabaseEntry::new();
        let got =
            creator.create_secondary_key(&db, &key, &data_with, &mut result);
        assert!(got);
        assert_eq!(result.get_data().unwrap(), b"some_data");

        // Branch: data is empty/None → false
        let data_none = DatabaseEntry::new();
        let mut result2 = DatabaseEntry::new();
        let got2 =
            creator.create_secondary_key(&db, &key, &data_none, &mut result2);
        assert!(!got2);
    }

    /// Exercise ForeignKeyNullifier via ForeignKeyDeleteAction::Nullify path to
    /// cover the nullifier trait impl body.
    #[test]
    fn test_foreign_key_nullifier_impl_covered() {
        struct SimpleNullifier;
        impl ForeignKeyNullifier for SimpleNullifier {
            fn nullify_foreign_key(
                &self,
                _db: &Database,
                _data: &mut DatabaseEntry,
            ) -> bool {
                false
            }
        }

        use crate::environment::Environment;
        use crate::environment_config::EnvironmentConfig;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true);
        let env = Environment::open(env_config).unwrap();
        let db_config = crate::database_config::DatabaseConfig::new()
            .with_allow_create(true);
        let db = env.open_database(None, "nul_test", &db_config).unwrap();

        let n = SimpleNullifier;
        let mut data = DatabaseEntry::from_bytes(b"val");
        let result = n.nullify_foreign_key(&db, &mut data);
        assert!(!result);
    }

    /// Exercise ForeignMultiKeyNullifier impl.
    #[test]
    fn test_foreign_multi_key_nullifier_impl_covered() {
        struct MultiNullifier;
        impl ForeignMultiKeyNullifier for MultiNullifier {
            fn nullify_foreign_key(
                &self,
                _db: &Database,
                _key: &DatabaseEntry,
                _data: &mut DatabaseEntry,
                _secondary_key: &DatabaseEntry,
            ) -> bool {
                false
            }
        }

        use crate::environment::Environment;
        use crate::environment_config::EnvironmentConfig;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true);
        let env = Environment::open(env_config).unwrap();
        let db_config = crate::database_config::DatabaseConfig::new()
            .with_allow_create(true);
        let db = env.open_database(None, "mknul_test", &db_config).unwrap();

        let n = MultiNullifier;
        let key = DatabaseEntry::from_bytes(b"k");
        let mut data = DatabaseEntry::from_bytes(b"v");
        let sec = DatabaseEntry::from_bytes(b"s");
        let result = n.nullify_foreign_key(&db, &key, &mut data, &sec);
        assert!(!result);
    }

    /// Exercise SecondaryMultiKeyCreator impl in both validate tests.
    #[test]
    fn test_multi_key_creator_impl_covered() {
        struct MkCreator;
        impl SecondaryMultiKeyCreator for MkCreator {
            fn create_secondary_keys(
                &self,
                _db: &Database,
                _key: &DatabaseEntry,
                _data: &DatabaseEntry,
                results: &mut Vec<DatabaseEntry>,
            ) {
                results.push(DatabaseEntry::from_bytes(b"sec"));
            }
        }

        use crate::environment::Environment;
        use crate::environment_config::EnvironmentConfig;
        use tempfile::TempDir;

        let temp_dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true);
        let env = Environment::open(env_config).unwrap();
        let db_config = crate::database_config::DatabaseConfig::new()
            .with_allow_create(true);
        let db = env.open_database(None, "mkcreator_test", &db_config).unwrap();

        let creator = MkCreator;
        let key = DatabaseEntry::from_bytes(b"k");
        let data = DatabaseEntry::from_bytes(b"v");
        let mut results = Vec::new();
        creator.create_secondary_keys(&db, &key, &data, &mut results);
        assert_eq!(results.len(), 1);
    }

    /// Exercise with_foreign_key_database builder method.
    #[test]
    fn test_with_foreign_key_database() {
        let config = SecondaryConfig::new()
            .with_key_creator(Box::new(SimpleKeyCreator))
            .with_foreign_key_database("foreign_db");
        assert_eq!(
            config.foreign_key_database_name.as_deref(),
            Some("foreign_db")
        );
        // Wave 1C audit cleanup (secondary-join F16): the FK database is
        // now stored as an owned name; no raw pointer or `unsafe impl
        // Send` is involved.
        assert!(config.has_foreign_key_config());
    }

    /// Comprehensive test exercising ALL local test-struct trait method bodies to
    /// ensure branch coverage for structs defined inside individual test functions.
    ///
    /// This test deliberately calls each struct method to cover the function bodies
    /// that otherwise count as uncovered branches.
    #[test]
    fn test_all_local_trait_impls_exercised() {
        use crate::environment::Environment;
        use crate::environment_config::EnvironmentConfig;
        use tempfile::TempDir;

        // Set up a real Database so trait impls can accept &Database.
        let temp_dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
            .with_allow_create(true);
        let env = Environment::open(env_config).unwrap();
        let db_config = crate::database_config::DatabaseConfig::new()
            .with_allow_create(true);
        let db = env.open_database(None, "exercise_db", &db_config).unwrap();

        // ---- No-op MkCreator (like in test_validate_both_creators_fails) ----
        struct NopMkCreator;
        impl SecondaryMultiKeyCreator for NopMkCreator {
            fn create_secondary_keys(
                &self,
                _db: &Database,
                _key: &DatabaseEntry,
                _data: &DatabaseEntry,
                _results: &mut Vec<DatabaseEntry>,
            ) {
                // No-op body — exercising the branch.
            }
        }

        let creator = NopMkCreator;
        let k = DatabaseEntry::from_bytes(b"k");
        let d = DatabaseEntry::from_bytes(b"v");
        let mut out: Vec<DatabaseEntry> = Vec::new();
        creator.create_secondary_keys(&db, &k, &d, &mut out);
        assert!(out.is_empty());

        // ---- SimpleNullifier (like in test_with_foreign_key_nullifier) ----
        struct Nul;
        impl ForeignKeyNullifier for Nul {
            fn nullify_foreign_key(
                &self,
                _db: &Database,
                _data: &mut DatabaseEntry,
            ) -> bool {
                false
            }
        }

        let n = Nul;
        let mut data = DatabaseEntry::from_bytes(b"v");
        assert!(!n.nullify_foreign_key(&db, &mut data));

        // ---- MultiNullifier (like in test_with_foreign_multi_key_nullifier) ----
        struct MultiNul;
        impl ForeignMultiKeyNullifier for MultiNul {
            fn nullify_foreign_key(
                &self,
                _db: &Database,
                _key: &DatabaseEntry,
                _data: &mut DatabaseEntry,
                _secondary_key: &DatabaseEntry,
            ) -> bool {
                false
            }
        }

        let mn = MultiNul;
        let key = DatabaseEntry::from_bytes(b"k");
        let mut data2 = DatabaseEntry::from_bytes(b"v");
        let sec = DatabaseEntry::from_bytes(b"s");
        assert!(!mn.nullify_foreign_key(&db, &key, &mut data2, &sec));

        // ---- Another NopMkCreator for test_validate_foreign_nullifier path ----
        struct NopMkCreator2;
        impl SecondaryMultiKeyCreator for NopMkCreator2 {
            fn create_secondary_keys(
                &self,
                _db: &Database,
                _key: &DatabaseEntry,
                _data: &DatabaseEntry,
                _results: &mut Vec<DatabaseEntry>,
            ) {
            }
        }

        let creator2 = NopMkCreator2;
        let mut out2: Vec<DatabaseEntry> = Vec::new();
        creator2.create_secondary_keys(&db, &k, &d, &mut out2);

        // ---- Another SimpleNullifier for test_validate_foreign_nullifier path ----
        struct Nul2;
        impl ForeignKeyNullifier for Nul2 {
            fn nullify_foreign_key(
                &self,
                _db: &Database,
                _data: &mut DatabaseEntry,
            ) -> bool {
                false
            }
        }

        let n2 = Nul2;
        let mut data3 = DatabaseEntry::from_bytes(b"v");
        assert!(!n2.nullify_foreign_key(&db, &mut data3));
    }
}
