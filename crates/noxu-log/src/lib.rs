#![allow(dead_code, clippy::type_complexity, clippy::too_many_arguments)]
//! Log-structured storage engine for Noxu DB.
//!
//! Port of `com.sleepycat.je.log` - handles sequential logging/writing,
//! random reading/fetching, and sequential reading of the write-ahead log.
//!
//! # Architecture
//!
//! The log subsystem consists of several layers:
//!
//! - **Entry types** (`entry_type`, `entry/`): Catalog of all log entry types
//!   and their serialization format.
//! - **Entry header** (`entry_header`): Metadata prepended to every log entry
//!   (type, size, checksum, VLSN).
//! - **File management** (`file_manager`, `file_handle`): Log file creation,
//!   rotation, naming, and I/O.
//! - **Buffer management** (`log_buffer`, `log_buffer_pool`): Write buffering
//!   with pool-based recycling.
//! - **Log manager** (`log_manager`): Central coordinator for log writes and
//!   reads.
//! - **File readers** (`file_reader`, `last_file_reader`, etc.): Sequential
//!   log scanning for recovery, cleaning, etc.

// Core types
pub mod checksum;
pub mod entry_header;
pub mod entry_type;
pub mod error;
pub mod log_item;
pub mod log_utils;
pub mod loggable;
pub mod provisional;
pub mod stats;
pub mod whole_entry;

// Entry types
pub mod entry;

// File I/O
pub mod file_handle;
pub mod file_header;
pub mod file_manager;
pub mod fsync_manager;
pub mod log_source;

// Buffer and log management
pub mod log_buffer;
pub mod log_buffer_pool;
pub mod log_file_reader;
pub mod log_flusher;
pub mod log_manager;

// File readers
pub mod checkpoint_file_reader;
pub mod cleaner_file_reader;
pub mod file_reader;
pub mod in_file_reader;
pub mod last_file_reader;
pub mod ln_file_reader;
pub mod search_file_reader;
pub mod utilization_file_reader;

// Re-export main types
pub use checksum::ChecksumValidator;
pub use entry_header::LogEntryHeader;
pub use entry_type::LogEntryType;
pub use error::{LogError, NoxuLogError, Result};
pub use file_handle::{FileHandle, FileHandleGuard};
pub use file_header::FileHeader;
pub use file_manager::FileManager;
pub use log_buffer::LogBuffer;
pub use log_buffer_pool::LogBufferPool;
pub use log_file_reader::LogFileReader;
pub use log_manager::LogManager;
pub use loggable::Loggable;
pub use provisional::Provisional;
