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
}

/// A specialized Result type for binding operations.
pub type Result<T> = std::result::Result<T, BindError>;
