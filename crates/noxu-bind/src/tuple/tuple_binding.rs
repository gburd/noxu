//! TupleBinding trait for type-specific tuple serialization.
//!
//! Port of `com.sleepycat.bind.tuple.TupleBinding`.

use noxu_db::DatabaseEntry;

use crate::entry_binding::EntryBinding;
use crate::error::Result;
use crate::tuple::tuple_input::TupleInput;
use crate::tuple::tuple_output::TupleOutput;

/// A binding that uses `TupleInput` and `TupleOutput` to serialize/deserialize
/// values of type `T`.
///
/// Implementors define `tuple_to_object` and `object_to_tuple` to convert
/// between a type and its tuple-encoded byte representation.
///
/// Port of `com.sleepycat.bind.tuple.TupleBinding<T>`.
pub trait TupleBinding<T>: EntryBinding<T> {
    /// Creates a `TupleInput` from a `DatabaseEntry`.
    fn entry_to_input(entry: &DatabaseEntry) -> TupleInput {
        TupleInput::new(entry.data())
    }

    /// Converts tuple input to an object.
    ///
    /// # Errors
    ///
    /// Returns `BindError` if the data cannot be deserialized.
    fn tuple_to_object(&self, input: &mut TupleInput) -> Result<T>;

    /// Converts an object to tuple output.
    ///
    /// # Errors
    ///
    /// Returns `BindError` if the object cannot be serialized.
    fn object_to_tuple(
        &self,
        object: &T,
        output: &mut TupleOutput,
    ) -> Result<()>;
}
