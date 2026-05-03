//! Tuple binding subsystem for encoding Rust primitive types into sortable byte arrays.
//!
//! This module provides the primary binding mechanism for database keys. Values are
//! encoded in a way that their byte representation sorts in the same order as the
//! original values when compared lexicographically.
//!
//! Port of `com.sleepycat.bind.tuple`.

pub mod primitive_bindings;
pub mod tuple_binding;
pub mod tuple_input;
pub mod tuple_output;

pub use primitive_bindings::*;
pub use tuple_binding::TupleBinding;
pub use tuple_input::TupleInput;
pub use tuple_output::TupleOutput;
