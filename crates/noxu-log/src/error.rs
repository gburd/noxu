//! Error types for the log layer.
//!

use std::io;
use thiserror::Error;

/// Errors that can occur in the log layer.
#[derive(Debug, Error)]
pub enum NoxuLogError {
    /// I/O error during file operations.
    #[error("Log I/O error: {0}")]
    Io(#[from] io::Error),

    /// File not found (specific log file).
    #[error("Log file not found: {0}")]
    FileNotFound(String),

    /// Checksum validation failed.
    #[error("Checksum validation failed at LSN {lsn}: {message}")]
    Checksum { lsn: noxu_util::lsn::Lsn, message: String },

    /// Invalid file header.
    #[error("Invalid file header in file {file_num:08x}: {message}")]
    InvalidHeader { file_num: u32, message: String },

    /// Version mismatch between log file and current version.
    #[error(
        "Version mismatch: expected {expected}, found {found} in file {file_num:08x}"
    )]
    VersionMismatch { expected: u32, found: u32, file_num: u32 },

    /// Environment is locked by another process.
    #[error("Environment locked: {0}")]
    EnvironmentLocked(String),

    /// Invalid environment directory.
    #[error("Invalid environment directory: {0}")]
    InvalidDirectory(String),

    /// Log write failed.
    #[error("Log write failed: {0}")]
    WriteFailed(String),

    /// Invalid entry type number.
    #[error("Invalid entry type {type_num} at LSN {lsn}")]
    InvalidEntryType { type_num: u8, lsn: noxu_util::lsn::Lsn },

    /// Invalid entry size.
    #[error("Invalid entry size {size} at LSN {lsn}")]
    InvalidEntrySize { size: i32, lsn: noxu_util::lsn::Lsn },

    /// Unexpected end of data.
    #[error("Unexpected EOF at LSN {lsn}: {message}")]
    UnexpectedEof { lsn: noxu_util::lsn::Lsn, message: String },

    /// Buffer overflow.
    #[error("Buffer overflow: {0}")]
    BufferOverflow(String),

    /// Log corruption detected.
    #[error("Log corrupt: {0}")]
    LogCorrupt(String),

    /// File header CRC32 checksum mismatch (torn header write).
    ///
    /// Returned when a v3 file header is opened and the trailing 4-byte
    /// CRC32 over bytes `[0..32]` does not match the stored value.  A torn
    /// header write can corrupt `file_number` or `last_entry_in_prev_file`
    /// while leaving magic + version intact; this error makes such corruption
    /// detectable rather than silently yielding wrong recovery metadata.
    #[error(
        "Header CRC32 mismatch in file {file_num:08x}: \
         expected {expected:#010x}, found {found:#010x}"
    )]
    HeaderChecksumMismatch { file_num: u32, expected: u32, found: u32 },

    /// Latch acquisition timed out (maps to EnvironmentFailure/LatchTimeout).
    #[error("Latch acquisition timed out: {0}")]
    LatchTimeout(String),

    /// A committed transaction was found AFTER a mid-file corruption point.
    ///
    /// Surfaced by [`crate::last_file_reader::LastFileReader`] during
    /// end-of-log discovery when the `haltOnCommitAfterChecksumException`
    /// param is enabled and a `TxnCommit` entry exists past a checksum
    /// failure.  This distinguishes real media corruption (with committed
    /// data beyond it) from a benign torn-tail write — recovery must REFUSE
    /// to silently truncate.  Recovery maps this to the env-invalidating
    /// `EnvironmentFailureReason::FoundCommittedTxn`.
    ///
    /// Faithful to JE `LastFileReader.readNextEntry`/`findCommittedTxn`
    /// (LastFileReader.java:313/394, [#18307]) which throws
    /// `EnvironmentFailureException(FOUND_COMMITTED_TXN, ...)`.
    #[error(
        "Found committed txn after the corruption point: \
         corrupt entry at LSN {corrupt_lsn}, committed txn at LSN {commit_lsn}"
    )]
    FoundCommittedTxn {
        corrupt_lsn: noxu_util::lsn::Lsn,
        commit_lsn: noxu_util::lsn::Lsn,
    },

    /// Internal consistency error.
    #[error("Internal error: {0}")]
    Internal(String),
}

/// Alias for backward compatibility with code using `LogError`.
pub type LogError = NoxuLogError;

pub type Result<T> = std::result::Result<T, NoxuLogError>;
