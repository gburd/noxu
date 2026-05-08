//! Base mutation type for schema evolution.
//!

/// A key identifying a mutation target: (class_name, class_version,
/// field_name).
///
/// `field_name` is `None` when the mutation targets the class itself rather
/// than a specific field.
///
/// 
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MutationKey {
    /// The fully-qualified class (entity) name.
    pub class_name: String,
    /// The class version this mutation applies to.
    pub class_version: u32,
    /// The field name, or `None` for class-level mutations.
    pub field_name: Option<String>,
}

impl MutationKey {
    /// Creates a class-level key (no field).
    pub fn for_class(class_name: impl Into<String>, class_version: u32) -> Self {
        Self {
            class_name: class_name.into(),
            class_version,
            field_name: None,
        }
    }

    /// Creates a field-level key.
    pub fn for_field(
        class_name: impl Into<String>,
        class_version: u32,
        field_name: impl Into<String>,
    ) -> Self {
        Self {
            class_name: class_name.into(),
            class_version,
            field_name: Some(field_name.into()),
        }
    }

    /// Returns the class name.
    pub fn class_name(&self) -> &str {
        &self.class_name
    }

    /// Returns the class version.
    pub fn class_version(&self) -> u32 {
        self.class_version
    }

    /// Returns the field name, or `None` for class-level mutations.
    pub fn field_name(&self) -> Option<&str> {
        self.field_name.as_deref()
    }
}

impl std::fmt::Display for MutationKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Class: {} Version: {}", self.class_name, self.class_version)?;
        if let Some(ref fname) = self.field_name {
            write!(f, " Field: {}", fname)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_class_key_equality() {
        let a = MutationKey::for_class("my.Class", 0);
        let b = MutationKey::for_class("my.Class", 0);
        assert_eq!(a, b);
    }

    #[test]
    fn test_field_key_equality() {
        let a = MutationKey::for_field("my.Class", 1, "fieldA");
        let b = MutationKey::for_field("my.Class", 1, "fieldA");
        assert_eq!(a, b);
    }

    #[test]
    fn test_class_vs_field_key_not_equal() {
        let a = MutationKey::for_class("my.Class", 0);
        let b = MutationKey::for_field("my.Class", 0, "someField");
        assert_ne!(a, b);
    }

    #[test]
    fn test_different_versions_not_equal() {
        let a = MutationKey::for_class("my.Class", 0);
        let b = MutationKey::for_class("my.Class", 1);
        assert_ne!(a, b);
    }

    #[test]
    fn test_display_class_level() {
        let k = MutationKey::for_class("com.example.Person", 2);
        assert_eq!(k.to_string(), "Class: com.example.Person Version: 2");
    }

    #[test]
    fn test_display_field_level() {
        let k = MutationKey::for_field("com.example.Person", 1, "fullName");
        assert_eq!(k.to_string(), "Class: com.example.Person Version: 1 Field: fullName");
    }

    #[test]
    fn test_hash_consistency() {
        use std::collections::HashMap;
        let mut map: HashMap<MutationKey, &str> = HashMap::new();
        let k = MutationKey::for_class("my.Class", 0);
        map.insert(k.clone(), "val");
        assert_eq!(map.get(&k), Some(&"val"));
    }
}
