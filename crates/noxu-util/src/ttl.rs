//! TTL (time-to-live) utility functions.
//!
//! TTL (time-to-live) utility functions for record expiration.

use std::time::{SystemTime, UNIX_EPOCH};

/// Number of seconds in one hour.
pub const SECS_PER_HOUR: u64 = 3600;

/// Returns the current time as packed hours since epoch (for hour-resolution TTL).
pub fn current_time_hours() -> u32 {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    (secs / SECS_PER_HOUR) as u32
}

/// Returns the current time as seconds since epoch (for second-resolution TTL).
pub fn current_time_secs() -> u32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as u32
}

/// Returns true if the given packed expiration time has passed.
///
/// `expiration_time == 0` means no expiration (never expires).
/// `in_hours`: if true, `expiration_time` is in hours; if false, in seconds.
pub fn is_expired(expiration_time: u32, in_hours: bool) -> bool {
    if expiration_time == 0 {
        return false;
    }
    let now = if in_hours {
        current_time_hours()
    } else {
        current_time_secs()
    };
    expiration_time <= now
}

/// Converts a TTL duration in hours to a packed expiration_time.
pub fn ttl_hours_to_expiration(ttl_hours: u32) -> u32 {
    if ttl_hours == 0 {
        return 0;
    }
    current_time_hours().saturating_add(ttl_hours)
}

/// Converts a TTL duration in seconds to a packed expiration_time (second resolution).
pub fn ttl_secs_to_expiration(ttl_secs: u64) -> u32 {
    if ttl_secs == 0 {
        return 0;
    }
    current_time_secs().saturating_add((ttl_secs / SECS_PER_HOUR).max(1) as u32)
}
