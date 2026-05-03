//! Schema evolution support for the persistence layer.
//!
//! This module provides the types required to configure how the DPL handles
//! incompatible changes to entity class definitions over time.  It is a port
//! of the `com.sleepycat.persist.evolve` Java package from Berkeley DB JE.
//!
//! # Overview
//!
//! When an entity class changes in a way that is incompatible with previously
//! stored data (e.g. a field is renamed, a class is renamed, or the data
//! format changes), you register *mutations* that tell the persistence layer
//! how to handle old data.  Three mutation types are supported:
//!
//! * **[`Renamer`]** — rename a class or field without changing stored bytes.
//! * **[`Deleter`]** — discard an old class or field.
//! * **[`Converter`]** — transform raw bytes from an old format to the new one.
//!
//! Mutations are collected in a [`Mutations`] object.  You can also perform
//! *eager* (batch) evolution by calling [`EntityStore::evolve`] with an
//! [`EvolveConfig`]; progress and statistics are reported via [`EvolveStats`].
//!
//! # Example
//!
//! ```
//! use noxu_persist::evolve::{Mutations, Renamer, Deleter, EvolveConfig};
//!
//! let mut mutations = Mutations::new();
//! mutations.add_renamer(Renamer::for_class("my.pkg.Person", 0, "my.pkg.Human"));
//! mutations.add_renamer(Renamer::for_field("my.pkg.Human", 0, "name", "fullName"));
//! mutations.add_deleter(Deleter::for_field("my.pkg.Human", 0, "nickname"));
//!
//! let config = EvolveConfig::new().with_class_to_evolve("my.pkg.Human");
//! ```
//!
//! [`EntityStore::evolve`]: crate::entity_store::EntityStore::evolve

pub mod converter;
pub mod deleter;
pub mod evolve_config;
pub mod mutation;
pub mod mutations;
pub mod renamer;
pub mod stats;

// Re-export the most commonly used types at the module root.
pub use converter::{ConversionFn, Converter};
pub use deleter::Deleter;
pub use evolve_config::{EvolveConfig, EvolveListener};
pub use mutation::MutationKey;
pub use mutations::{ClassMutations, Mutations};
pub use renamer::Renamer;
pub use stats::EvolveStats;
