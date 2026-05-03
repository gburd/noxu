//! Core binding traits for converting between database entries and Rust types.
//!
//! Port of `com.sleepycat.bind.EntryBinding` and `com.sleepycat.bind.EntityBinding`.

use noxu_db::DatabaseEntry;

use crate::error::Result;

/// Converts between a `DatabaseEntry` and a Rust type.
///
/// This is the fundamental binding trait, analogous to JE's `EntryBinding<T>`.
/// Implementations define how to serialize an object into a database entry
/// and how to deserialize it back.
///
/// Port of `com.sleepycat.bind.EntryBinding<T>`.
pub trait EntryBinding<T> {
    /// Converts a `DatabaseEntry` to an object.
    ///
    /// # Errors
    ///
    /// Returns `BindError` if the entry data cannot be deserialized.
    fn entry_to_object(&self, entry: &DatabaseEntry) -> Result<T>;

    /// Converts an object to a `DatabaseEntry`.
    ///
    /// # Errors
    ///
    /// Returns `BindError` if the object cannot be serialized.
    fn object_to_entry(
        &self,
        object: &T,
        entry: &mut DatabaseEntry,
    ) -> Result<()>;
}

/// Converts between key+data entries and an entity object.
///
/// This trait is used for entity bindings where the key and data are stored
/// separately but represent a single logical entity.
///
/// Port of `com.sleepycat.bind.EntityBinding<E>`.
pub trait EntityBinding<E> {
    /// Converts key and data entries to an entity object.
    ///
    /// # Errors
    ///
    /// Returns `BindError` if the entries cannot be deserialized.
    fn entry_to_object(
        &self,
        key: &DatabaseEntry,
        data: &DatabaseEntry,
    ) -> Result<E>;

    /// Extracts the key from an entity object and writes it to a `DatabaseEntry`.
    ///
    /// # Errors
    ///
    /// Returns `BindError` if the key cannot be serialized.
    fn object_to_key(&self, object: &E, key: &mut DatabaseEntry) -> Result<()>;

    /// Extracts the data from an entity object and writes it to a `DatabaseEntry`.
    ///
    /// # Errors
    ///
    /// Returns `BindError` if the data cannot be serialized.
    fn object_to_data(
        &self,
        object: &E,
        data: &mut DatabaseEntry,
    ) -> Result<()>;
}
