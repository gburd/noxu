//! TTL (time-to-live) utility functions.
//!
//! TTL (time-to-live) utility functions for record expiration.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::clock::Clock;

/// Number of seconds in one hour.
pub const SECS_PER_HOUR: u64 = 3600;

/// Process-global test override for the TTL system clock, in milliseconds
/// since the Unix epoch.
///
/// Faithful analog of JE's static `TTL.timeTestHook` / `fixedSystemTime`
/// (TTL.java:124-162): all TTL time reads flow through
/// [`current_system_time_ms`] (JE `TTL.currentSystemTime`), which consults
/// this override when it is set.  A value of 0 means "not set — use the real
/// wall clock", exactly as `timeTestHook == null` does in JE.  Because `0` is
/// also a (long-past) legal epoch time, 0 is reserved as the sentinel; tests
/// use a time well after the epoch (e.g. 2016), so this is never a problem in
/// practice and matches JE where `fixedSystemTime == 0` is the cleared state.
///
/// This is a **test-only hook**: production never calls
/// [`set_test_time_ms`], so `TEST_TIME_MS` stays 0 and
/// [`current_system_time_ms`] delegates straight to `SystemTime::now`.
static TEST_TIME_MS: AtomicU64 = AtomicU64::new(0);

/// Installs a fixed TTL system time (ms since the Unix epoch) for all TTL
/// processing in this process, or clears it when `ms == 0`.
///
/// Faithful analog of JE `TTL.setTimeTestHook` (TTL.java:153): once set, every
/// TTL expiry decision behaves as if the system clock reads `ms`, so a record
/// can be aged past its expiration deterministically (JE `TTLTest`
/// `setFixedTimeHook` / `fixedSystemTime`).  Pass 0 to restore the real clock
/// (JE `TTL.setTimeTestHook(null)`).
///
/// Intended for tests only; production code never calls it.
#[doc(hidden)] // test-only hook; not committed public API (v7.11.1 patch-safe)
pub fn set_test_time_ms(ms: u64) {
    TEST_TIME_MS.store(ms, Ordering::SeqCst);
}

/// Advances the fixed TTL system time by `delta_ms` milliseconds.
///
/// Must be called only while a fixed time is installed (JE's tests only mutate
/// `fixedSystemTime` after `setFixedTimeHook`); advancing from the cleared
/// state (0) is a no-op guard so a stray advance cannot silently install a
/// near-epoch fixed clock in production.
#[doc(hidden)] // test-only hook; not committed public API (v7.11.1 patch-safe)
pub fn advance_test_time_ms(delta_ms: u64) {
    let cur = TEST_TIME_MS.load(Ordering::SeqCst);
    if cur != 0 {
        TEST_TIME_MS.store(cur.saturating_add(delta_ms), Ordering::SeqCst);
    }
}

/// Returns the currently installed fixed TTL system time, or 0 if none.
#[doc(hidden)] // test-only hook; not committed public API (v7.11.1 patch-safe)
pub fn test_time_ms() -> u64 {
    TEST_TIME_MS.load(Ordering::SeqCst)
}

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
///
/// Reads "now" via [`current_system_time_ms`] so a test clock installed with
/// [`set_test_time_ms`] is honored (JE `TTL.currentSystemTime` is the single
/// time source for all TTL math).
pub fn current_time_hours() -> u32 {
    (current_system_time_ms() / 1000 / SECS_PER_HOUR) as u32
}

/// Returns the current wall-clock time in milliseconds since the Unix epoch.
///
/// JE `TTL.currentSystemTime` (TTL.java:157-163): returns the fixed test time
/// when one is installed via [`set_test_time_ms`] (JE `timeTestHook`),
/// otherwise `System.currentTimeMillis`.
pub fn current_system_time_ms() -> u64 {
    let fixed = TEST_TIME_MS.load(Ordering::SeqCst);
    if fixed != 0 {
        return fixed;
    }
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
///
/// Reads "now" via [`current_system_time_ms`] so a test clock installed with
/// [`set_test_time_ms`] is honored.
pub fn current_time_secs() -> u32 {
    (current_system_time_ms() / 1000) as u32
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
    use std::sync::Mutex;

    use super::*;

    // R-TTL-CLOCK-RACE: the TTL tests share one process-global clock
    // (`TEST_TIME_MS`).  `test_time_hook_overrides_the_clock` installs a fixed
    // time (2016), advances it, then clears it, while the other tests read
    // "now" via `current_time_hours()` / `current_system_time_ms()` on parallel
    // threads.  Without serialisation a wall-clock-relative reader can observe
    // the clock jump to 2016 (or back to the real clock) between its two "now"
    // reads and fail (e.g. `expires_within_tolerance_extends_future_check`).
    //
    // Every test that READS "now" and asserts a value relative to it, and the
    // one test that WRITES the clock, takes this guard, so the fixed-clock
    // window runs entirely before or entirely after any wall-clock-relative
    // reader, never interleaved.  Tests whose assertions hold for ANY "now"
    // (zero sentinels, saturation, hour-1-is-past) do not take it.
    static HOOK_GUARD: Mutex<()> = Mutex::new(());

    #[test]
    fn test_time_hook_overrides_the_clock() {
        // R-TTL-CLOCK-RACE: the writer MUST hold the guard for its whole
        // set/advance/clear window (see the module comment above).
        let _g = HOOK_GUARD.lock().unwrap();
        // Faithful analog of JE TTLTest.setFixedTimeHook / fixedSystemTime:
        // installing a fixed time makes every TTL time read return it, so a
        // record can be aged past its expiration deterministically.
        // 2016-02-03 in ms (matches JE TTLTest BASE_CAL year) — well after
        // the epoch, so 0 stays a safe "cleared" sentinel.
        let base_ms: u64 = 1_454_472_306_000; // ~2016-02-03 04:05:06 UTC
        set_test_time_ms(base_ms);
        assert_eq!(current_system_time_ms(), base_ms);
        let base_hours = (base_ms / 1000 / SECS_PER_HOUR) as u32;
        assert_eq!(current_time_hours(), base_hours);
        assert_eq!(current_time_secs(), (base_ms / 1000) as u32);

        // A record expiring exactly one hour out is not yet expired...
        let exp = base_hours + 1;
        assert!(!is_expired(exp, true));
        // ...but advancing the fixed clock two hours makes it expired, with
        // no real wall-clock wait (JE: fixedSystemTime += MILLIS_PER_HOUR).
        advance_test_time_ms(2 * MILLIS_PER_HOUR);
        assert!(is_expired(exp, true));

        // Clearing the hook restores the real clock (JE setTimeTestHook(null)).
        set_test_time_ms(0);
        assert_eq!(test_time_ms(), 0);
        assert!(current_system_time_ms() > base_ms); // real clock, now > 2016
    }

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
        let _g = HOOK_GUARD.lock().unwrap();
        // current_time_hours() + 24 (one day from now) is in the future.
        let future = current_time_hours().saturating_add(24);
        assert!(!is_expired(future, true));
    }

    #[test]
    fn not_expired_when_in_the_future_seconds() {
        let _g = HOOK_GUARD.lock().unwrap();
        // current_time_secs() + 86_400 is in the future.
        let future = current_time_secs().saturating_add(86_400);
        assert!(!is_expired(future, false));
    }

    #[test]
    fn current_time_hours_is_nonzero_after_epoch() {
        let _g = HOOK_GUARD.lock().unwrap();
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
        let _g = HOOK_GUARD.lock().unwrap();
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
        let _g = HOOK_GUARD.lock().unwrap();
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
        let _g = HOOK_GUARD.lock().unwrap();
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
        let _g = HOOK_GUARD.lock().unwrap();
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
        let _g = HOOK_GUARD.lock().unwrap();
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
        // R-TTL-CLOCK-RACE: serialised against the clock writer so the global
        // override cannot move "now" out from under this relative assertion.
        let _g = HOOK_GUARD.lock().unwrap();
        let one_hour_out = current_time_hours() + 1;
        assert!(!expires_within(one_hour_out, true, 0));
        assert!(expires_within(one_hour_out, true, 3 * MILLIS_PER_HOUR as i64));
    }
}
