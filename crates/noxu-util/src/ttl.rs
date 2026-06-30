//! TTL (time-to-live) utility functions.
//!
//! TTL (time-to-live) utility functions for record expiration.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::clock::Clock;

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
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
        as u32
}

/// Clock-aware variant of [`is_expired`] for the DST control-flow path.
///
/// Identical to [`is_expired`] but reads "now" from an injectable [`Clock`]
/// instead of [`SystemTime::now`], so a [`crate::SimClock`] makes expiry a
/// pure function of the simulated timeline.  Used where TTL expiry is a
/// control-flow decision under simulation; the plain [`is_expired`] (real
/// wall clock) remains the default everywhere else.
///
/// `in_hours` matches [`is_expired`]: hours-since-epoch when `true`,
/// seconds-since-epoch when `false`.
pub fn is_expired_with(
    clock: &dyn Clock,
    expiration_time: u32,
    in_hours: bool,
) -> bool {
    if expiration_time == 0 {
        return false;
    }
    let now_secs = clock.now_unix_ms() / 1000;
    let now = if in_hours {
        (now_secs / SECS_PER_HOUR) as u32
    } else {
        now_secs as u32
    };
    expiration_time <= now
}

/// Returns true if the given packed expiration time has passed.
///
/// `expiration_time == 0` means no expiration (never expires).
///
/// `in_hours`: if `true`, `expiration_time` is hours since the Unix epoch
/// (the only granularity the public write API — `WriteOptions::with_ttl` /
/// `with_expiration` — produces).  If `false`, `expiration_time` is seconds
/// since the Unix epoch.  **The engine's `BinStub::expiration_in_hours` flag
/// must always match the granularity of the stored values**: mixing them
/// produces silent correctness failures (St-H6 — see `Tree::split_child`).
///
/// The seconds-granularity path (`in_hours = false`) exists for future use;
/// it is **not reachable from the current public API** which is hours-only.
pub fn is_expired(expiration_time: u32, in_hours: bool) -> bool {
    if expiration_time == 0 {
        return false;
    }
    let now = if in_hours { current_time_hours() } else { current_time_secs() };
    expiration_time <= now
}

/// Converts a TTL duration in hours to a packed expiration_time.
pub fn ttl_hours_to_expiration(ttl_hours: u32) -> u32 {
    if ttl_hours == 0 {
        return 0;
    }
    current_time_hours().saturating_add(ttl_hours)
}

/// Converts a TTL duration in seconds to a packed `expiration_time`
/// (seconds since the Unix epoch).
///
/// The returned value is comparable to [`current_time_secs`] and
/// works with [`is_expired`] when `in_hours = false`.
///
/// `ttl_secs == 0` is the "never expires" sentinel and returns 0.
///
/// Saturates at `u32::MAX` if either `ttl_secs` itself or
/// `current_time_secs() + ttl_secs` would not fit in `u32`.
pub fn ttl_secs_to_expiration(ttl_secs: u64) -> u32 {
    if ttl_secs == 0 {
        return 0;
    }
    let ttl_u32: u32 = ttl_secs.try_into().unwrap_or(u32::MAX);
    current_time_secs().saturating_add(ttl_u32)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn never_expires_when_zero() {
        // expiration_time == 0 is the "never expires" sentinel,
        // regardless of the resolution flag.
        assert!(!is_expired(0, true));
        assert!(!is_expired(0, false));
    }

    #[test]
    fn expired_when_in_the_past_hours() {
        // Hour 1 (1970-01-01 01:00 UTC) is far in the past.
        assert!(is_expired(1, true));
    }

    #[test]
    fn expired_when_in_the_past_seconds() {
        // 1 second after the epoch is in the past.
        assert!(is_expired(1, false));
    }

    #[test]
    fn not_expired_when_in_the_future_hours() {
        // current_time_hours() + 24 (one day from now) is in the future.
        let future = current_time_hours().saturating_add(24);
        assert!(!is_expired(future, true));
    }

    #[test]
    fn not_expired_when_in_the_future_seconds() {
        // current_time_secs() + 86_400 is in the future.
        let future = current_time_secs().saturating_add(86_400);
        assert!(!is_expired(future, false));
    }

    #[test]
    fn current_time_hours_is_nonzero_after_epoch() {
        // Hours since epoch should be > 0 for any time after
        // 1970-01-01 01:00 UTC. The test is checking that the
        // function returns a sane (non-zero, non-overflowing) value
        // — we don't assert a tight bound.
        let h = current_time_hours();
        assert!(h > 0, "current_time_hours() returned 0 after epoch");
        // Also assert it agrees with current_time_secs() within
        // half an hour.
        let s = current_time_secs();
        let from_secs = (s as u64) / SECS_PER_HOUR;
        assert!(
            (h as i64 - from_secs as i64).abs() <= 1,
            "current_time_hours() and current_time_secs() disagree: \
             {h} hours vs {from_secs} hours-from-secs",
        );
    }

    #[test]
    fn ttl_hours_to_expiration_zero_means_never() {
        assert_eq!(ttl_hours_to_expiration(0), 0);
    }

    #[test]
    fn ttl_hours_to_expiration_returns_future() {
        let now = current_time_hours();
        let exp = ttl_hours_to_expiration(24);
        assert!(exp >= now + 24 - 1 && exp <= now + 24 + 1);
    }

    #[test]
    fn ttl_hours_to_expiration_saturates_on_overflow() {
        // u32::MAX + 1 hour saturates at u32::MAX.
        let exp = ttl_hours_to_expiration(u32::MAX);
        assert_eq!(exp, u32::MAX);
    }

    #[test]
    fn ttl_secs_to_expiration_zero_means_never() {
        assert_eq!(ttl_secs_to_expiration(0), 0);
    }

    #[test]
    fn ttl_secs_to_expiration_30_seconds_in_30_seconds() {
        // Adding 30 seconds to current_time_secs() yields a value
        // 30 ahead — exact down to the second-resolution.
        let now = current_time_secs();
        let exp = ttl_secs_to_expiration(30);
        assert!(
            exp >= now + 30 && exp <= now + 31,
            "exp={exp} not in [{}, {}]",
            now + 30,
            now + 31
        );
    }

    #[test]
    fn ttl_secs_to_expiration_two_hours_uses_seconds() {
        // 7200 seconds == 2 hours. The expiration must be ~7200s in
        // the future, NOT 2s — that would be the legacy buggy
        // behaviour where the function divided by SECS_PER_HOUR.
        let now = current_time_secs();
        let exp = ttl_secs_to_expiration(7200);
        assert!(
            exp >= now + 7200 && exp <= now + 7201,
            "expected ~now+7200 seconds, got exp={exp} (now={now})"
        );
    }

    #[test]
    fn ttl_secs_to_expiration_saturates_on_u64_overflow() {
        // u64::MAX > u32::MAX, so the inner try_into clamps to
        // u32::MAX and the saturating_add then clamps again.
        assert_eq!(ttl_secs_to_expiration(u64::MAX), u32::MAX);
    }

    #[test]
    fn ttl_secs_to_expiration_saturates_on_addition_overflow() {
        // A TTL one less than u32::MAX always overflows when added
        // to any non-zero current_time_secs.
        let exp = ttl_secs_to_expiration(u32::MAX as u64 - 1);
        assert_eq!(exp, u32::MAX);
    }
}
