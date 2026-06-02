// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

#![allow(dead_code, clippy::type_complexity, clippy::too_many_arguments)]
//! > **Internal component of the [`noxu`](https://crates.io/crates/noxu) database.**
//! >
//! > This crate is published only so the `noxu` umbrella crate can depend on it.
//! > Use `noxu` (`noxu = "3"`) in applications; depend on this crate directly only
//! > if you are extending the engine internals. Its API may change without a major
//! > version bump.
//!
//! Log-structured storage engine for Noxu DB.
//!
//! handles sequential logging/writing,
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
pub(crate) mod posio;

// Buffer and log management
pub mod log_buffer;
pub mod log_buffer_pool;
pub mod log_file_reader;
pub mod log_flusher;
pub mod log_manager;
pub mod write_observer;

// File readers
pub mod checkpoint_file_reader;
pub mod cleaner_file_reader;
pub mod file_reader;
pub mod in_file_reader;
pub mod last_file_reader;
pub mod ln_file_reader;
pub mod search_file_reader;
pub mod utilization_file_reader;

// ---------------------------------------------------------------------------
// Public constants
// ---------------------------------------------------------------------------

/// Maximum allowed payload size for a single log entry (header excluded).
///
/// Centralised so every reader/scanner uses the same upper bound when
/// validating an `item_size` field decoded from disk or off the wire.
/// A larger value would imply tens-of-MiB attacker-controlled allocations
/// during recovery / replication and is rejected as corruption.
///
/// 100 MiB is generous enough for any well-formed entry produced by the
/// engine (the largest synthetic entries seen in tests are ≤ a few MiB)
/// while still bounding memory consumed by a single bad header.
pub const MAX_ITEM_SIZE: usize = 100 * 1024 * 1024;

// Re-export main types
pub use checksum::ChecksumValidator;
pub use entry_header::LogEntryHeader;
pub use entry_type::LogEntryType;
pub use error::{LogError, NoxuLogError, Result};
pub use file_handle::{FileHandle, FileHandleGuard};
pub use file_header::FileHeader;
pub use file_manager::{FileManager, FileManagerIoStats};
pub use log_buffer::LogBuffer;
pub use log_buffer_pool::LogBufferPool;
pub use log_file_reader::LogFileReader;
pub use log_manager::{LogManager, LogManagerStats};
pub use loggable::Loggable;
pub use provisional::Provisional;
pub use write_observer::LogWriteObserver;
