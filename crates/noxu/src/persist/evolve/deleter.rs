//! Deleter mutation for schema evolution.
//!

use super::mutation::MutationKey;

/// A mutation that marks an entity class or field as deleted.
///
/// **Warning:** The data for the deleted class or field will be discarded.
/// If you need to convert the data to another format, use a [`Converter`]
/// instead.
///
/// # Examples
///
/// ```
/// use crate::persist::evolve::Deleter;
///
/// // Delete an entire entity class at version 0
/// let class_deleter = Deleter::for_class("my.package.Statistics", 0);
///
/// // Delete a field from a class at version 0
/// let field_deleter = Deleter::for_field("my.package.Person", 0, "favoriteColors");
/// ```
///
///
///
/// [`Converter`]: super::converter::Converter
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Deleter {
    key: MutationKey,
}

impl Deleter {
    /// Creates a mutation for deleting an entire entity class.
    ///
    ///
    ///
    /// # Arguments
    /// * `class_name` - Fully-qualified name of the class to delete.
    /// * `class_version` - Version of the class this mutation applies to.
    pub fn for_class(
        class_name: impl Into<String>,
        class_version: u32,
    ) -> Self {
        Self { key: MutationKey::for_class(class_name, class_version) }
    }

    /// Creates a mutation for deleting a field from all instances of a class.
    ///
    /// `Deleter(String declaringClass, int declaringClassVersion,
    ///                   String fieldName)`.
    ///
    /// # Arguments
    /// * `class_name` - Fully-qualified name of the declaring class.
    /// * `class_version` - Version of the class this mutation applies to.
    /// * `field_name` - Name of the field to delete.
    pub fn for_field(
        class_name: impl Into<String>,
        class_version: u32,
        field_name: impl Into<String>,
    ) -> Self {
        Self {
            key: MutationKey::for_field(class_name, class_version, field_name),
        }
    }

    /// Returns the mutation key (class name, version, optional field name).
    pub fn key(&self) -> &MutationKey {
        &self.key
    }

    /// Returns the class name this mutation applies to.
    pub fn class_name(&self) -> &str {
        self.key.class_name()
    }

    /// Returns the class version this mutation applies to.
    pub fn class_version(&self) -> u32 {
        self.key.class_version()
    }

    /// Returns the field name, or `None` for class-level deleters.
    pub fn field_name(&self) -> Option<&str> {
        self.key.field_name()
    }
}

impl std::fmt::Display for Deleter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[Deleter {}]", self.key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_class_deleter() {
        let d = Deleter::for_class("my.pkg.Stats", 0);
        assert_eq!(d.class_name(), "my.pkg.Stats");
        assert_eq!(d.class_version(), 0);
        assert_eq!(d.field_name(), None);
    }

    #[test]
    fn test_field_deleter() {
        let d = Deleter::for_field("my.pkg.Person", 0, "favoriteColors");
        assert_eq!(d.class_name(), "my.pkg.Person");
        assert_eq!(d.class_version(), 0);
        assert_eq!(d.field_name(), Some("favoriteColors"));
    }

    #[test]
    fn test_equality() {
        let a = Deleter::for_class("X", 1);
        let b = Deleter::for_class("X", 1);
        assert_eq!(a, b);
    }

    #[test]
    fn test_inequality_version() {
        let a = Deleter::for_class("X", 0);
        let b = Deleter::for_class("X", 1);
        assert_ne!(a, b);
    }

    #[test]
    fn test_display_class() {
        let d = Deleter::for_class("com.example.Stats", 0);
        let s = d.to_string();
        assert!(s.contains("Deleter"));
        assert!(s.contains("com.example.Stats"));
    }

    #[test]
    fn test_display_field() {
        let d = Deleter::for_field("com.example.Person", 2, "oldField");
        let s = d.to_string();
        assert!(s.contains("Deleter"));
        assert!(s.contains("oldField"));
    }
}
