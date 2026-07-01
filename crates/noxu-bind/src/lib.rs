#![forbid(unsafe_code)]
// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT
#![allow(clippy::type_complexity, clippy::too_many_arguments)]
//! > **Internal component of the [`noxu`](https://crates.io/crates/noxu) database.**
//! >
//! > This crate is published only so the `noxu` umbrella crate can depend on it.
//! > Use `noxu` (`noxu = "7"`) in applications; depend on this crate directly only
//! > if you are extending the engine internals. Its API may change without a major
//! > version bump.
//!
//! Serialization bindings for Noxu DB.
//!
//! bindings between database entries
//! and Rust types, including tuple and byte encoding.
//!
//! # Overview
//!
//! This crate provides the serialization layer for Noxu DB, converting between
//! Rust types and `DatabaseEntry` byte representations. The primary mechanism
//! is the **tuple binding** subsystem, which encodes primitive types into
//! sortable byte arrays suitable for database keys.
//!
//! # Modules
//!
//! - [`error`]  -  Error types for binding operations.
//! - [`entry_binding`]  -  Core `EntryBinding` and `EntityBinding` traits.
//! - [`byte_array_binding`]  -  Pass-through binding for raw byte arrays.
//! - [`record_number_binding`]  -  Big-endian u64 record number binding.
//! - [`tuple`][mod@tuple]  -  Tuple (compact binary) bindings for sortable keys.

pub mod byte_array_binding;
pub mod entry_binding;
pub mod error;
pub mod record_number_binding;
pub mod serial;
pub mod tuple;

// Re-export primary types at crate root for convenience.
pub use byte_array_binding::ByteArrayBinding;
pub use entry_binding::{EntityBinding, EntryBinding};
pub use error::{BindError, Result};
pub use record_number_binding::RecordNumberBinding;
pub use serial::serde_binding::SerdeBinding;
pub use serial::tuple_serde_binding::{
    TupleSerdeBinding, TupleSerdeKeyDataBinding,
};
pub use tuple::primitive_bindings::{
    BoolBinding, ByteBinding, CharBinding, DoubleBinding, FloatBinding,
    IntBinding, LongBinding, PackedIntBinding, PackedLongBinding, ShortBinding,
    SortedDoubleBinding, SortedFloatBinding, SortedPackedIntBinding,
    SortedPackedLongBinding, StringBinding,
};
pub use tuple::{SortKey, TupleBinding, TupleInput, TupleOutput};
