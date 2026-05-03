//! Serde-based serialization bindings for Noxu DB.
//!
//! Port of `com.sleepycat.bind.serial`  -  replaces Java serialization with
//! Rust's serde framework and a compact binary encoding.
//!
//! ## Required dependencies (to be added to Cargo.toml)
//!
//! ```toml
//! serde = { version = "1", features = ["derive"] }
//! ```

pub mod serde_binding;
pub mod simple_serial;
pub mod tuple_serde_binding;
