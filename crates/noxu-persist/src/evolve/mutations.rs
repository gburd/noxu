//! Collection of schema-evolution mutations.
//!
//! Port of `com.sleepycat.persist.evolve.Mutations`.

use std::collections::HashMap;

use super::converter::Converter;
use super::deleter::Deleter;
use super::mutation::MutationKey;
use super::renamer::Renamer;

/// A collection of [`Renamer`], [`Deleter`], and [`Converter`] mutations used
/// to configure how the persistence layer handles schema evolution.
///
/// Mutations cause data conversion to occur lazily as instances are read from
/// the store.  The [`EntityStore::evolve`] method may be used to perform
/// eager (batch) conversion.
///
/// # Example
///
/// ```
/// use noxu_persist::evolve::{Mutations, Renamer, Deleter};
///
/// let mut m = Mutations::new();
/// m.add_renamer(Renamer::for_class("my.pkg.Person", 0, "my.pkg.Human"));
/// m.add_deleter(Deleter::for_field("my.pkg.Human", 0, "nickname"));
/// assert!(!m.is_empty());
/// ```
///
/// Port of `com.sleepycat.persist.evolve.Mutations`.
///
/// [`EntityStore::evolve`]: crate::entity_store::EntityStore::evolve
#[derive(Debug, Default)]
pub struct Mutations {
    renamers: HashMap<MutationKey, Renamer>,
    deleters: HashMap<MutationKey, Deleter>,
    converters: HashMap<MutationKey, Converter>,
}

impl Mutations {
    /// Creates an empty set of mutations.
    ///
    /// Port of `Mutations()` constructor.
    pub fn new() -> Self {
        Self {
            renamers: HashMap::new(),
            deleters: HashMap::new(),
            converters: HashMap::new(),
        }
    }

    /// Returns `true` if no mutations are present.
    ///
    /// Port of `Mutations.isEmpty()`.
    pub fn is_empty(&self) -> bool {
        self.renamers.is_empty() && self.deleters.is_empty() && self.converters.is_empty()
    }

    // -----------------------------------------------------------------------
    // Renamers
    // -----------------------------------------------------------------------

    /// Adds a renamer mutation.
    ///
    /// Port of `Mutations.addRenamer(Renamer)`.
    pub fn add_renamer(&mut self, renamer: Renamer) {
        self.renamers.insert(renamer.key().clone(), renamer);
    }

    /// Returns the renamer for the given class version (class-level lookup).
    ///
    /// Pass `field_name = None` to look up a class renamer.
    ///
    /// Port of `Mutations.getRenamer(String, int, String)`.
    pub fn get_renamer(
        &self,
        class_name: &str,
        class_version: u32,
        field_name: Option<&str>,
    ) -> Option<&Renamer> {
        let key = make_key(class_name, class_version, field_name);
        self.renamers.get(&key)
    }

    /// Returns an iterator over all renamer mutations.
    ///
    /// Port of `Mutations.getRenamers()`.
    pub fn renamers(&self) -> impl Iterator<Item = &Renamer> {
        self.renamers.values()
    }

    // -----------------------------------------------------------------------
    // Deleters
    // -----------------------------------------------------------------------

    /// Adds a deleter mutation.
    ///
    /// Port of `Mutations.addDeleter(Deleter)`.
    pub fn add_deleter(&mut self, deleter: Deleter) {
        self.deleters.insert(deleter.key().clone(), deleter);
    }

    /// Returns the deleter for the given class/field, or `None`.
    ///
    /// Pass `field_name = None` to look up a class deleter.
    ///
    /// Port of `Mutations.getDeleter(String, int, String)`.
    pub fn get_deleter(
        &self,
        class_name: &str,
        class_version: u32,
        field_name: Option<&str>,
    ) -> Option<&Deleter> {
        let key = make_key(class_name, class_version, field_name);
        self.deleters.get(&key)
    }

    /// Returns an iterator over all deleter mutations.
    ///
    /// Port of `Mutations.getDeleters()`.
    pub fn deleters(&self) -> impl Iterator<Item = &Deleter> {
        self.deleters.values()
    }

    // -----------------------------------------------------------------------
    // Converters
    // -----------------------------------------------------------------------

    /// Adds a converter mutation.
    ///
    /// Port of `Mutations.addConverter(Converter)`.
    pub fn add_converter(&mut self, converter: Converter) {
        self.converters.insert(converter.key().clone(), converter);
    }

    /// Returns the converter for the given class/field, or `None`.
    ///
    /// Pass `field_name = None` to look up a class converter.
    ///
    /// Port of `Mutations.getConverter(String, int, String)`.
    pub fn get_converter(
        &self,
        class_name: &str,
        class_version: u32,
        field_name: Option<&str>,
    ) -> Option<&Converter> {
        let key = make_key(class_name, class_version, field_name);
        self.converters.get(&key)
    }

    /// Returns an iterator over all converter mutations.
    ///
    /// Port of `Mutations.getConverters()`.
    pub fn converters(&self) -> impl Iterator<Item = &Converter> {
        self.converters.values()
    }

    // -----------------------------------------------------------------------
    // Combined lookup
    // -----------------------------------------------------------------------

    /// Returns all mutations (renamer, deleter, or converter) that apply to
    /// the given class at the given version.
    ///
    /// This is a convenience method not present in JE.  It collects all
    /// class-level and field-level mutations for a class name + version pair,
    /// which is useful during eager evolution.
    pub fn get_mutations_for_class(
        &self,
        class_name: &str,
        class_version: u32,
    ) -> ClassMutations<'_> {
        ClassMutations {
            renamer: self.get_renamer(class_name, class_version, None),
            deleter: self.get_deleter(class_name, class_version, None),
            converter: self.get_converter(class_name, class_version, None),
        }
    }
}

/// The set of class-level mutations that apply to a specific class version.
#[derive(Debug)]
pub struct ClassMutations<'a> {
    /// A class-level renamer, if one exists.
    pub renamer: Option<&'a Renamer>,
    /// A class-level deleter, if one exists.
    pub deleter: Option<&'a Deleter>,
    /// A class-level converter, if one exists.
    pub converter: Option<&'a Converter>,
}

impl ClassMutations<'_> {
    /// Returns `true` if no class-level mutations were found.
    pub fn is_empty(&self) -> bool {
        self.renamer.is_none() && self.deleter.is_none() && self.converter.is_none()
    }
}

impl std::fmt::Display for Mutations {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_empty() {
            return write!(f, "[Empty Mutations]");
        }
        if !self.renamers.is_empty() {
            for r in self.renamers.values() {
                write!(f, "{}", r)?;
            }
        }
        if !self.deleters.is_empty() {
            for d in self.deleters.values() {
                write!(f, "{}", d)?;
            }
        }
        if !self.converters.is_empty() {
            for c in self.converters.values() {
                write!(f, "{}", c)?;
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

fn make_key(class_name: &str, class_version: u32, field_name: Option<&str>) -> MutationKey {
    match field_name {
        Some(f) => MutationKey::for_field(class_name, class_version, f),
        None => MutationKey::for_class(class_name, class_version),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::evolve::converter::Converter;
    use crate::evolve::deleter::Deleter;
    use crate::evolve::renamer::Renamer;

    #[test]
    fn test_empty_on_new() {
        let m = Mutations::new();
        assert!(m.is_empty());
    }

    #[test]
    fn test_not_empty_after_add_renamer() {
        let mut m = Mutations::new();
        m.add_renamer(Renamer::for_class("A", 0, "B"));
        assert!(!m.is_empty());
    }

    #[test]
    fn test_not_empty_after_add_deleter() {
        let mut m = Mutations::new();
        m.add_deleter(Deleter::for_class("A", 0));
        assert!(!m.is_empty());
    }

    #[test]
    fn test_not_empty_after_add_converter() {
        let mut m = Mutations::new();
        m.add_converter(Converter::for_class("A", 0, |b: &[u8]| Some(b.to_vec())));
        assert!(!m.is_empty());
    }

    #[test]
    fn test_get_renamer_class_level() {
        let mut m = Mutations::new();
        m.add_renamer(Renamer::for_class("pkg.Person", 0, "pkg.Human"));
        let r = m.get_renamer("pkg.Person", 0, None).unwrap();
        assert_eq!(r.new_name(), "pkg.Human");
    }

    #[test]
    fn test_get_renamer_field_level() {
        let mut m = Mutations::new();
        m.add_renamer(Renamer::for_field("pkg.Human", 1, "name", "fullName"));
        let r = m.get_renamer("pkg.Human", 1, Some("name")).unwrap();
        assert_eq!(r.new_name(), "fullName");
    }

    #[test]
    fn test_get_renamer_not_found() {
        let m = Mutations::new();
        assert!(m.get_renamer("Unknown", 0, None).is_none());
    }

    #[test]
    fn test_get_deleter_class_level() {
        let mut m = Mutations::new();
        m.add_deleter(Deleter::for_class("pkg.Stats", 2));
        assert!(m.get_deleter("pkg.Stats", 2, None).is_some());
        assert!(m.get_deleter("pkg.Stats", 0, None).is_none());
    }

    #[test]
    fn test_get_deleter_field_level() {
        let mut m = Mutations::new();
        m.add_deleter(Deleter::for_field("pkg.Person", 0, "favoriteColors"));
        assert!(m.get_deleter("pkg.Person", 0, Some("favoriteColors")).is_some());
        assert!(m.get_deleter("pkg.Person", 0, None).is_none());
    }

    #[test]
    fn test_get_converter_class_level() {
        let mut m = Mutations::new();
        m.add_converter(Converter::for_class("X", 0, |b: &[u8]| Some(b.to_vec())));
        let c = m.get_converter("X", 0, None).unwrap();
        assert_eq!(c.convert(b"hi").as_deref(), Some(b"hi" as &[u8]));
    }

    #[test]
    fn test_get_mutations_for_class_all_present() {
        let mut m = Mutations::new();
        m.add_renamer(Renamer::for_class("C", 0, "D"));
        m.add_deleter(Deleter::for_class("C", 1));
        m.add_converter(Converter::for_class("C", 2, |b: &[u8]| Some(b.to_vec())));

        let cm = m.get_mutations_for_class("C", 0);
        assert!(cm.renamer.is_some());
        assert!(cm.deleter.is_none());
        assert!(cm.converter.is_none());

        let cm1 = m.get_mutations_for_class("C", 1);
        assert!(cm1.deleter.is_some());

        let cm2 = m.get_mutations_for_class("C", 2);
        assert!(cm2.converter.is_some());
    }

    #[test]
    fn test_get_mutations_for_class_empty() {
        let m = Mutations::new();
        let cm = m.get_mutations_for_class("NoSuch", 0);
        assert!(cm.is_empty());
    }

    #[test]
    fn test_renamers_iter() {
        let mut m = Mutations::new();
        m.add_renamer(Renamer::for_class("A", 0, "B"));
        m.add_renamer(Renamer::for_class("C", 0, "D"));
        assert_eq!(m.renamers().count(), 2);
    }

    #[test]
    fn test_deleters_iter() {
        let mut m = Mutations::new();
        m.add_deleter(Deleter::for_class("A", 0));
        assert_eq!(m.deleters().count(), 1);
    }

    #[test]
    fn test_converters_iter() {
        let mut m = Mutations::new();
        m.add_converter(Converter::for_class("A", 0, |b: &[u8]| Some(b.to_vec())));
        m.add_converter(Converter::for_field("A", 1, "f", |b: &[u8]| Some(b.to_vec())));
        assert_eq!(m.converters().count(), 2);
    }

    #[test]
    fn test_later_add_overwrites_earlier() {
        // Adding a renamer with the same key should replace the old one.
        let mut m = Mutations::new();
        m.add_renamer(Renamer::for_class("A", 0, "B"));
        m.add_renamer(Renamer::for_class("A", 0, "C")); // overwrites
        let r = m.get_renamer("A", 0, None).unwrap();
        assert_eq!(r.new_name(), "C");
        assert_eq!(m.renamers().count(), 1);
    }

    #[test]
    fn test_display_empty() {
        let m = Mutations::new();
        assert_eq!(m.to_string(), "[Empty Mutations]");
    }

    #[test]
    fn test_display_non_empty() {
        let mut m = Mutations::new();
        m.add_renamer(Renamer::for_class("A", 0, "B"));
        let s = m.to_string();
        assert!(!s.is_empty());
        assert_ne!(s, "[Empty Mutations]");
    }
}
