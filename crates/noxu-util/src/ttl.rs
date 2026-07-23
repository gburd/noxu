//! TTL (time-to-live) utility functions.
//!
//! TTL (time-to-live) utility functions for record expiration.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::clock::Clock;

/// Number of seconds in one hour.
pub const SECS_PER_HOUR: u64 = 3600;

/// Milliseconds in one hour (JE `TTL.MILLIS_PER_HOUR`).
pub const MILLIS_PER_HOUR: u64 = 1000 * 60 * 60;

/// Milliseconds in one day (JE `TTL.MILLIS_PER_DAY`).
pub const MILLIS_PER_DAY: u64 = MILLIS_PER_HOUR * 24;

/// TTL time unit supplied on a write, matching JE `WriteOptions.setTTL`'s
/// `TimeUnit` parameter.  Records expire on hour or day boundaries depending on
/// the unit; `Days` is recommended to minimize the per-slot expiration storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TtlUnit {
    /// Expire on hour boundaries.  Stored expiration values are hours since
    /// the Unix epoch.
    Hours,
    /// Expire on day boundaries.  Stored expiration values are hours since the
    /// Unix epoch (24 * days), so the engine keeps a single hours-since-epoch
    /// representation as JE does.
    Days,
}

/// Returns the current time as packed hours since epoch (for hour-resolution TTL).
pub fn current_time_hours() -> u32 {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    (secs / SECS_PER_HOUR) as u32
}

/// Returns the current wall-clock time in milliseconds since the Unix epoch.
///
/// JE `TTL.currentSystemTime` (System.currentTimeMillis).
pub fn current_system_time_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis()
        as u64
}

/// Translates the user-supplied TTL parameter into the expiration value stored
/// internally (hours since the Unix epoch), rounding the current time up to the
/// next hour or day boundary first.
///
/// Faithful port of JE `TTL.ttlToExpiration` (TTL.java): the current system
/// time is rounded UP to the next unit boundary, then the TTL is added.  When
/// the unit is `Days`, the day-granular result is multiplied by 24 so the
/// engine always stores hours-since-epoch (matching
/// `ExpirationTracker`/`BIN` which keep one hours representation and a
/// day-boundary flag).  A `ttl` of 0 returns 0 ("never expires").
pub fn ttl_to_expiration(ttl: u32, unit: TtlUnit) -> u32 {
    if ttl == 0 {
        return 0;
    }
    let now_ms = current_system_time_ms();
    match unit {
        TtlUnit::Days => {
            // JE: currentTime = ceil(now / MILLIS_PER_DAY); expiration in
            // days = currentTime + ttl; stored as hours = days * 24.
            let current_day = now_ms.div_ceil(MILLIS_PER_DAY);
            let exp_days = current_day.saturating_add(ttl as u64);
            exp_days.saturating_mul(24).min(u32::MAX as u64) as u32
        }
        TtlUnit::Hours => {
            // JE: currentTime = ceil(now / MILLIS_PER_HOUR); expiration in
            // hours = currentTime + ttl.
            let current_hour = now_ms.div_ceil(MILLIS_PER_HOUR);
            current_hour.saturating_add(ttl as u64).min(u32::MAX as u64) as u32
        }
    }
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

/// Returns whether the given expiration time is within `within_ms` of the
/// current system time — i.e. the record has expired, or will expire within
/// `within_ms`.  A negative `within_ms` shifts the check the other way (the
/// record expired at least `abs(within_ms)` ago).
///
/// Faithful port of JE `TTL.expiresWithin` (TTL.java).  Used by the purge /
/// cleaner path via the `ENV_TTL_CLOCK_TOLERANCE` grace window: a record is
/// only treated as safely purgeable once it expired at least the tolerance
/// ago, so a small backward clock adjustment cannot cause a live record to be
/// reclaimed.  `expiration_time == 0` means "never expires".
///
/// `in_hours` selects hours-since-epoch (`true`) vs seconds-since-epoch
/// (`false`), matching [`is_expired`].
pub fn expires_within(
    expiration_time: u32,
    in_hours: bool,
    within_ms: i64,
) -> bool {
    if expiration_time == 0 {
        return false;
    }
    let exp_ms = if in_hours {
        (expiration_time as u64).saturating_mul(MILLIS_PER_HOUR)
    } else {
        (expiration_time as u64).saturating_mul(1000)
    } as i128;
    let now_ms = current_system_time_ms() as i128;
    now_ms + within_ms as i128 > exp_ms
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

    #[test]
    fn ttl_to_expiration_zero_means_never() {
        assert_eq!(ttl_to_expiration(0, TtlUnit::Hours), 0);
        assert_eq!(ttl_to_expiration(0, TtlUnit::Days), 0);
    }

    #[test]
    fn ttl_to_expiration_hours_rounds_up_and_adds() {
        // JE ttlToExpiration: ceil(now/hour) + ttl.  With a 1-hour TTL the
        // result must be within [now_hours+1, now_hours+2] (rounding up the
        // current partial hour).
        let now = current_time_hours();
        let exp = ttl_to_expiration(1, TtlUnit::Hours);
        assert!(
            exp > now && exp <= now + 2,
            "exp={exp} not in ({}, {}]",
            now,
            now + 2
        );
    }

    #[test]
    fn ttl_to_expiration_days_stored_as_hours_on_day_boundary() {
        // A day-granular expiration is stored as hours-since-epoch and must
        // land exactly on a 24-hour boundary (JE stores days*24).
        let exp = ttl_to_expiration(1, TtlUnit::Days);
        assert_eq!(
            exp % 24,
            0,
            "day-granular expiration must be a multiple of 24 hours"
        );
        // 1-day TTL is at least ~24 hours in the future.
        assert!(exp as u64 >= current_time_hours() as u64 + 24);
    }

    #[test]
    fn expires_within_zero_never_expires() {
        assert!(!expires_within(0, true, i64::MAX));
    }

    #[test]
    fn expires_within_past_expiration_is_true() {
        // Hour 1 (1970) is long past; with a zero tolerance it is expired.
        assert!(expires_within(1, true, 0));
    }

    #[test]
    fn expires_within_tolerance_extends_future_check() {
        // A record expiring exactly one hour from now is NOT expired with a
        // zero grace window, but IS "expires within" a 2-hour grace window.
        let one_hour_out = current_time_hours() + 1;
        assert!(!expires_within(one_hour_out, true, 0));
        assert!(expires_within(one_hour_out, true, 3 * MILLIS_PER_HOUR as i64));
    }
}
