//! Converter mutation for schema evolution.
//!
//! Port of `com.sleepycat.persist.evolve.Converter` and
//! `com.sleepycat.persist.evolve.Conversion`.

use super::mutation::MutationKey;

/// A conversion function that transforms the raw bytes of an old entity or
/// field value into bytes compatible with the current schema.
///
/// In JE this is the `Conversion` interface.  In Rust we use a trait object
/// (`Box<dyn ConversionFn>`) so the closure can be stored.
///
/// Port of `com.sleepycat.persist.evolve.Conversion`.
pub trait ConversionFn: Send + Sync {
    /// Converts old raw bytes to new raw bytes.
    ///
    /// Returns `None` if the record should be deleted (for class-level
    /// converters that wish to drop the entity entirely).
    fn convert(&self, old_bytes: &[u8]) -> Option<Vec<u8>>;
}

/// Blanket impl for closures `Fn(&[u8]) -> Option<Vec<u8>>`.
impl<F> ConversionFn for F
where
    F: Fn(&[u8]) -> Option<Vec<u8>> + Send + Sync,
{
    fn convert(&self, old_bytes: &[u8]) -> Option<Vec<u8>> {
        self(old_bytes)
    }
}

/// A mutation for converting an old version of an entity or field value to
/// conform to the current class definition.
///
/// # Examples
///
/// ```
/// use noxu_persist::evolve::Converter;
///
/// // Convert all version-0 instances of Person to the current layout.
/// let conv = Converter::for_class(
///     "my.package.Person",
///     0,
///     |old_bytes: &[u8]| {
///         // transform bytes...
///         Some(old_bytes.to_vec())
///     },
/// );
/// ```
///
/// Port of `com.sleepycat.persist.evolve.Converter`.
pub struct Converter {
    key: MutationKey,
    conversion: Box<dyn ConversionFn>,
}

impl Converter {
    /// Creates a mutation for converting all instances of the given class
    /// version to the current version.
    ///
    /// Port of `Converter(String className, int classVersion, Conversion conversion)`.
    pub fn for_class<F>(
        class_name: impl Into<String>,
        class_version: u32,
        conversion: F,
    ) -> Self
    where
        F: ConversionFn + 'static,
    {
        Self {
            key: MutationKey::for_class(class_name, class_version),
            conversion: Box::new(conversion),
        }
    }

    /// Creates a mutation for converting all values of the given field in the
    /// given class version.
    ///
    /// Port of `Converter(String declaringClassName, int declaringClassVersion,
    ///                     String fieldName, Conversion conversion)`.
    pub fn for_field<F>(
        class_name: impl Into<String>,
        class_version: u32,
        field_name: impl Into<String>,
        conversion: F,
    ) -> Self
    where
        F: ConversionFn + 'static,
    {
        Self {
            key: MutationKey::for_field(class_name, class_version, field_name),
            conversion: Box::new(conversion),
        }
    }

    /// Returns the mutation key.
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

    /// Returns the field name, or `None` for class-level converters.
    pub fn field_name(&self) -> Option<&str> {
        self.key.field_name()
    }

    /// Runs the conversion on `old_bytes`.
    ///
    /// Returns `None` if the record should be deleted.
    ///
    /// Port of `Conversion.convert(EntityInput, int, EntityInput)` (simplified
    /// to raw byte slice interface for Rust).
    pub fn convert(&self, old_bytes: &[u8]) -> Option<Vec<u8>> {
        self.conversion.convert(old_bytes)
    }
}

impl std::fmt::Debug for Converter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Converter").field("key", &self.key).finish_non_exhaustive()
    }
}

impl std::fmt::Display for Converter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[Converter {}]", self.key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_class_converter_identity() {
        let c = Converter::for_class("my.pkg.Person", 0, |b: &[u8]| Some(b.to_vec()));
        assert_eq!(c.class_name(), "my.pkg.Person");
        assert_eq!(c.class_version(), 0);
        assert_eq!(c.field_name(), None);
        let out = c.convert(b"hello");
        assert_eq!(out.as_deref(), Some(b"hello" as &[u8]));
    }

    #[test]
    fn test_field_converter() {
        let c = Converter::for_field("my.pkg.Person", 1, "age", |b: &[u8]| {
            // Example: double every byte
            Some(b.iter().map(|x| x.wrapping_mul(2)).collect())
        });
        assert_eq!(c.field_name(), Some("age"));
        let out = c.convert(&[1u8, 2, 3]).unwrap();
        assert_eq!(out, vec![2u8, 4, 6]);
    }

    #[test]
    fn test_converter_returns_none_for_delete() {
        let c = Converter::for_class("my.pkg.Obsolete", 0, |_: &[u8]| None);
        assert_eq!(c.convert(b"anything"), None);
    }

    #[test]
    fn test_display() {
        let c = Converter::for_class("com.example.Foo", 3, |b: &[u8]| Some(b.to_vec()));
        let s = c.to_string();
        assert!(s.contains("Converter"));
        assert!(s.contains("com.example.Foo"));
    }

    #[test]
    fn test_debug() {
        let c = Converter::for_class("X", 0, |b: &[u8]| Some(b.to_vec()));
        let s = format!("{:?}", c);
        assert!(s.contains("Converter"));
    }
}
