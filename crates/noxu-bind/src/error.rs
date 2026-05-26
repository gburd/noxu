//! Error types for the noxu-bind crate.
//!

use thiserror::Error;

/// Errors that can occur during binding operations.
#[derive(Debug, Error)]
pub enum BindError {
    /// Buffer underflow: attempted to read more bytes than available.
    #[error("buffer underflow: needed {needed} bytes, got {available}")]
    BufferUnderflow {
        /// Number of bytes needed.
        needed: usize,
        /// Number of bytes available.
        available: usize,
    },

    /// Invalid data encountered during deserialization.
    #[error("invalid data: {0}")]
    InvalidData(String),

    /// String encoding/decoding error.
    #[error("string encoding error: {0}")]
    StringEncoding(String),

    /// Unsupported type encountered.
    #[error("unsupported type: {0}")]
    UnsupportedType(String),

    /// The on-disk payload was written by an older or newer version of
    /// the binding's wire format than this build understands.
    ///
    /// Returned by `SerdeBinding::entry_to_object` when the 2-byte
    /// header (`[magic, version]`) does not match this build's
    /// expectation.  See `docs/src/getting-started/bindings.md` for
    /// the format description and migration guidance.
    #[error(
        "binding version mismatch: header {{ magic: 0x{found_magic:02X}, \
         version: 0x{found_version:02X} }} but this build expects \
         {{ magic: 0x{expected_magic:02X}, version: 0x{expected_version:02X} }}"
    )]
    VersionMismatch {
        /// Expected magic byte.
        expected_magic: u8,
        /// Expected version byte.
        expected_version: u8,
        /// Magic byte found in the payload (or 0 if the payload was
        /// too short to contain one).
        found_magic: u8,
        /// Version byte found in the payload (or 0 if the payload was
        /// too short to contain one).
        found_version: u8,
    },
}

/// A specialized Result type for binding operations.
pub type Result<T> = std::result::Result<T, BindError>;
