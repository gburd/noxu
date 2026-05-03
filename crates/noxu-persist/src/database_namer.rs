//! Database naming strategy for entity stores.
//!
//! Port of `com.sleepycat.persist.DatabaseNamer`. Provides a trait and
//! default implementation for generating database names from entity and
//! store names.

/// Generates database names for entity stores.
///
/// In JE, each entity type and secondary index gets its own BDB database.
/// The `DatabaseNamer` controls the naming convention used to map entity
/// types and key names to underlying database names.
///
/// Port of `com.sleepycat.persist.DatabaseNamer`.
pub trait DatabaseNamer {
    /// Generate a database name for a primary index.
    ///
    /// # Arguments
    /// * `store_name` - The name of the entity store.
    /// * `entity_name` - The name of the entity type.
    fn primary_db_name(&self, store_name: &str, entity_name: &str) -> String;

    /// Generate a database name for a secondary index.
    ///
    /// # Arguments
    /// * `store_name` - The name of the entity store.
    /// * `entity_name` - The name of the entity type.
    /// * `key_name` - The name of the secondary key.
    fn secondary_db_name(
        &self,
        store_name: &str,
        entity_name: &str,
        key_name: &str,
    ) -> String;

    /// Generate a database name for the sequence database.
    ///
    /// # Arguments
    /// * `store_name` - The name of the entity store.
    fn sequence_db_name(&self, store_name: &str) -> String;
}

/// Default database namer using the JE convention: `"persist#StoreName#EntityName"`.
///
/// This matches the naming convention used by JE's `EntityStore`.
///
/// Port of the default naming logic in `com.sleepycat.persist.impl.Store`.
///
/// # Examples
///
/// ```
/// use noxu_persist::database_namer::{DatabaseNamer, DefaultDatabaseNamer};
///
/// let namer = DefaultDatabaseNamer;
/// assert_eq!(
///     namer.primary_db_name("MyStore", "User"),
///     "persist#MyStore#User"
/// );
/// assert_eq!(
///     namer.secondary_db_name("MyStore", "User", "email"),
///     "persist#MyStore#User#email"
/// );
/// assert_eq!(
///     namer.sequence_db_name("MyStore"),
///     "persist#MyStore#sequences"
/// );
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultDatabaseNamer;

impl DatabaseNamer for DefaultDatabaseNamer {
    fn primary_db_name(&self, store_name: &str, entity_name: &str) -> String {
        format!("persist#{}#{}", store_name, entity_name)
    }

    fn secondary_db_name(
        &self,
        store_name: &str,
        entity_name: &str,
        key_name: &str,
    ) -> String {
        format!("persist#{}#{}#{}", store_name, entity_name, key_name)
    }

    fn sequence_db_name(&self, store_name: &str) -> String {
        format!("persist#{}#sequences", store_name)
    }
}

/// A custom database namer that uses a configurable prefix and separator.
///
/// Useful when the default `"persist#"` prefix conflicts with existing
/// database names or when a different separator is preferred.
#[derive(Debug, Clone)]
pub struct CustomDatabaseNamer {
    /// Prefix for all generated names.
    prefix: String,
    /// Separator between name components.
    separator: String,
}

impl CustomDatabaseNamer {
    /// Creates a new custom namer with the given prefix and separator.
    ///
    /// # Arguments
    /// * `prefix` - The prefix to prepend to all database names.
    /// * `separator` - The separator between name components.
    pub fn new(
        prefix: impl Into<String>,
        separator: impl Into<String>,
    ) -> Self {
        Self { prefix: prefix.into(), separator: separator.into() }
    }
}

impl DatabaseNamer for CustomDatabaseNamer {
    fn primary_db_name(&self, store_name: &str, entity_name: &str) -> String {
        format!(
            "{}{}{}{}{}",
            self.prefix,
            self.separator,
            store_name,
            self.separator,
            entity_name
        )
    }

    fn secondary_db_name(
        &self,
        store_name: &str,
        entity_name: &str,
        key_name: &str,
    ) -> String {
        format!(
            "{}{}{}{}{}{}{}",
            self.prefix,
            self.separator,
            store_name,
            self.separator,
            entity_name,
            self.separator,
            key_name
        )
    }

    fn sequence_db_name(&self, store_name: &str) -> String {
        format!(
            "{}{}{}{}sequences",
            self.prefix, self.separator, store_name, self.separator
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_primary_db_name() {
        let namer = DefaultDatabaseNamer;
        assert_eq!(
            namer.primary_db_name("MyStore", "User"),
            "persist#MyStore#User"
        );
    }

    #[test]
    fn test_default_secondary_db_name() {
        let namer = DefaultDatabaseNamer;
        assert_eq!(
            namer.secondary_db_name("MyStore", "User", "email"),
            "persist#MyStore#User#email"
        );
    }

    #[test]
    fn test_default_sequence_db_name() {
        let namer = DefaultDatabaseNamer;
        assert_eq!(
            namer.sequence_db_name("MyStore"),
            "persist#MyStore#sequences"
        );
    }

    #[test]
    fn test_default_primary_db_name_with_spaces() {
        let namer = DefaultDatabaseNamer;
        assert_eq!(
            namer.primary_db_name("My Store", "My Entity"),
            "persist#My Store#My Entity"
        );
    }

    #[test]
    fn test_default_primary_db_name_empty_strings() {
        let namer = DefaultDatabaseNamer;
        assert_eq!(namer.primary_db_name("", ""), "persist##");
    }

    #[test]
    fn test_default_secondary_multiple_keys() {
        let namer = DefaultDatabaseNamer;
        let name1 = namer.secondary_db_name("store", "User", "email");
        let name2 = namer.secondary_db_name("store", "User", "name");
        assert_ne!(name1, name2);
    }

    #[test]
    fn test_default_primary_vs_secondary_names_differ() {
        let namer = DefaultDatabaseNamer;
        let primary = namer.primary_db_name("store", "User");
        let secondary = namer.secondary_db_name("store", "User", "email");
        assert_ne!(primary, secondary);
    }

    #[test]
    fn test_default_different_stores_different_names() {
        let namer = DefaultDatabaseNamer;
        let name1 = namer.primary_db_name("store1", "User");
        let name2 = namer.primary_db_name("store2", "User");
        assert_ne!(name1, name2);
    }

    #[test]
    fn test_custom_namer_primary() {
        let namer = CustomDatabaseNamer::new("db", ".");
        assert_eq!(namer.primary_db_name("store", "User"), "db.store.User");
    }

    #[test]
    fn test_custom_namer_secondary() {
        let namer = CustomDatabaseNamer::new("db", ".");
        assert_eq!(
            namer.secondary_db_name("store", "User", "email"),
            "db.store.User.email"
        );
    }

    #[test]
    fn test_custom_namer_sequence() {
        let namer = CustomDatabaseNamer::new("db", ".");
        assert_eq!(namer.sequence_db_name("store"), "db.store.sequences");
    }

    #[test]
    fn test_custom_namer_underscore_separator() {
        let namer = CustomDatabaseNamer::new("prefix", "_");
        assert_eq!(
            namer.primary_db_name("mystore", "User"),
            "prefix_mystore_User"
        );
    }

    #[test]
    fn test_custom_namer_empty_prefix() {
        let namer = CustomDatabaseNamer::new("", "/");
        assert_eq!(namer.primary_db_name("store", "User"), "/store/User");
    }

    #[test]
    fn test_default_namer_is_copy() {
        let namer1 = DefaultDatabaseNamer;
        let namer2 = namer1;
        assert_eq!(
            namer1.primary_db_name("s", "e"),
            namer2.primary_db_name("s", "e")
        );
    }

    #[test]
    fn test_custom_namer_clone() {
        let namer1 = CustomDatabaseNamer::new("p", "#");
        let namer2 = namer1.clone();
        assert_eq!(
            namer1.primary_db_name("s", "e"),
            namer2.primary_db_name("s", "e")
        );
    }
}
