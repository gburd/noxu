//! Renamer mutation for schema evolution.
//!
//! Port of `com.sleepycat.persist.evolve.Renamer`.

use super::mutation::MutationKey;

/// A mutation that renames a class or a field without changing instance data.
///
/// Use a class renamer when an entity class is renamed but the stored data
/// format is otherwise unchanged.  Use a field renamer when a field within a
/// class is renamed.
///
/// # Examples
///
/// ```
/// use noxu_persist::evolve::Renamer;
///
/// // Rename the class itself (version 0 -> new name)
/// let class_renamer = Renamer::for_class("my.package.Person", 0, "my.package.Human");
///
/// // Rename a field within the class
/// let field_renamer = Renamer::for_field("my.package.Human", 0, "name", "fullName");
/// ```
///
/// Port of `com.sleepycat.persist.evolve.Renamer`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Renamer {
    key: MutationKey,
    new_name: String,
}

impl Renamer {
    /// Creates a mutation for renaming the class itself.
    ///
    /// Port of `Renamer(String fromClass, int fromVersion, String toClass)`.
    ///
    /// # Arguments
    /// * `from_class` - Fully-qualified name of the class being renamed.
    /// * `from_version` - Version of the class this mutation applies to.
    /// * `to_class` - New fully-qualified class name.
    pub fn for_class(
        from_class: impl Into<String>,
        from_version: u32,
        to_class: impl Into<String>,
    ) -> Self {
        Self {
            key: MutationKey::for_class(from_class, from_version),
            new_name: to_class.into(),
        }
    }

    /// Creates a mutation for renaming a field within a class.
    ///
    /// Port of `Renamer(String declaringClass, int declaringClassVersion,
    ///                   String fromField, String toField)`.
    ///
    /// # Arguments
    /// * `class_name` - Fully-qualified name of the declaring class.
    /// * `class_version` - Version of the class this mutation applies to.
    /// * `from_field` - Existing field name in the given class version.
    /// * `to_field` - New field name.
    pub fn for_field(
        class_name: impl Into<String>,
        class_version: u32,
        from_field: impl Into<String>,
        to_field: impl Into<String>,
    ) -> Self {
        Self {
            key: MutationKey::for_field(class_name, class_version, from_field),
            new_name: to_field.into(),
        }
    }

    /// Returns the mutation key (class name, version, optional field name).
    pub fn key(&self) -> &MutationKey {
        &self.key
    }

    /// Returns the new class or field name.
    ///
    /// Port of `Renamer.getNewName()`.
    pub fn new_name(&self) -> &str {
        &self.new_name
    }

    /// Returns the class name this mutation applies to.
    pub fn class_name(&self) -> &str {
        self.key.class_name()
    }

    /// Returns the class version this mutation applies to.
    pub fn class_version(&self) -> u32 {
        self.key.class_version()
    }

    /// Returns the field name, or `None` for class-level renamers.
    pub fn field_name(&self) -> Option<&str> {
        self.key.field_name()
    }
}

impl std::fmt::Display for Renamer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[Renamer {} NewName: {}]", self.key, self.new_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_class_renamer() {
        let r = Renamer::for_class("my.pkg.Person", 0, "my.pkg.Human");
        assert_eq!(r.class_name(), "my.pkg.Person");
        assert_eq!(r.class_version(), 0);
        assert_eq!(r.field_name(), None);
        assert_eq!(r.new_name(), "my.pkg.Human");
    }

    #[test]
    fn test_field_renamer() {
        let r = Renamer::for_field("my.pkg.Human", 1, "name", "fullName");
        assert_eq!(r.class_name(), "my.pkg.Human");
        assert_eq!(r.class_version(), 1);
        assert_eq!(r.field_name(), Some("name"));
        assert_eq!(r.new_name(), "fullName");
    }

    #[test]
    fn test_equality() {
        let a = Renamer::for_class("C", 0, "D");
        let b = Renamer::for_class("C", 0, "D");
        assert_eq!(a, b);
    }

    #[test]
    fn test_inequality_different_new_name() {
        let a = Renamer::for_class("C", 0, "D");
        let b = Renamer::for_class("C", 0, "E");
        assert_ne!(a, b);
    }

    #[test]
    fn test_display_class() {
        let r = Renamer::for_class("com.example.Person", 0, "com.example.Human");
        let s = r.to_string();
        assert!(s.contains("Renamer"));
        assert!(s.contains("com.example.Person"));
        assert!(s.contains("com.example.Human"));
    }

    #[test]
    fn test_display_field() {
        let r = Renamer::for_field("com.example.Human", 1, "name", "fullName");
        let s = r.to_string();
        assert!(s.contains("Renamer"));
        assert!(s.contains("fullName"));
    }
}
