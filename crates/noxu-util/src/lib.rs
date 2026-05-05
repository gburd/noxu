#![allow(dead_code, clippy::type_complexity, clippy::too_many_arguments)]
//! Internal utilities for Noxu DB.
//!
//! Port of `com.sleepycat.je.utilint` - provides fundamental types and
//! utilities used throughout the database engine.

pub mod daemon;
pub mod lsn;
pub mod packed;
pub mod stats;
pub mod ttl;
pub mod vlsn;

// Re-export commonly used types at crate root
pub use lsn::{Lsn, NULL_LSN};
pub use ttl::{
    current_time_hours, current_time_secs, is_expired, ttl_hours_to_expiration,
    ttl_secs_to_expiration, SECS_PER_HOUR,
};
pub use vlsn::{
    FIRST_VLSN, NULL_VLSN, NULL_VLSN_SEQUENCE, UNINITIALIZED_VLSN_SEQUENCE,
    VLSN_LOG_SIZE, Vlsn,
};
