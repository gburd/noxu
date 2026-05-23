#![allow(dead_code, clippy::type_complexity, clippy::too_many_arguments)]
//! Internal utilities for Noxu DB.
//!
//! Fundamental types and utilities used throughout the database engine.
//!
//! ## Platform Notes
//!
//! **AtomicU64 on 32-bit targets (armv7, i686, riscv32)**: Noxu uses
//! `std::sync::atomic::AtomicU64` for lock-free VLSN/LSN counters and
//! statistics.  On 32-bit targets without `LDREXD`/`STREXD` (ARMv7) or
//! equivalent instructions, Rust will silently emit a mutex-based fallback.
//! This is *correct* but adds locking overhead on the stats hot path.
//! All uses in Noxu are for stats counters and VLSN tracking; none are
//! called from signal handlers, so the mutex fallback is safe.
//!
//! **path separators**: All file paths use `std::path::PathBuf`/`Path` which
//! produces `/`-separated paths on Unix and `\`-separated on Windows.
//! Log file names embedded in wire frames are transmitted as `u32` file
//! numbers, not strings, so no cross-platform path issues arise on the wire.
//!
//! **fdatasync / fsync**: `FileHandle::sync_data()` calls `File::sync_data()`
//! which maps to `fdatasync(2)` on Linux/macOS and `FlushFileBuffers` on
//! Windows.  Semantics are equivalent: data is durable when it returns `Ok`.
//!
//! **tc netem**: Used in `noxu-rep` chaos tests.  Guarded by
//! `#[cfg(not(target_os = "linux"))]`; on non-Linux platforms the tests run
//! with software-only fault injection (`TcNetemGuard::active == false`).

pub mod daemon;
pub mod lsn;
pub mod packed;
pub mod stats;
pub mod ttl;
pub mod vlsn;

// Re-export commonly used types at crate root
pub use lsn::{Lsn, NULL_LSN};
pub use ttl::{
    SECS_PER_HOUR, current_time_hours, current_time_secs, is_expired,
    ttl_hours_to_expiration, ttl_secs_to_expiration,
};
pub use vlsn::{
    FIRST_VLSN, NULL_VLSN, NULL_VLSN_SEQUENCE, UNINITIALIZED_VLSN_SEQUENCE,
    VLSN_LOG_SIZE, Vlsn,
};
