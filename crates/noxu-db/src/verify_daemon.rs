// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Background B-tree verifier daemon (`VERIFY_SCHEDULE` / `ENV_RUN_VERIFIER`).
//!
//! A faithful analogue of BDB-JE's background `DataVerifier` /
//! `BtreeVerifier` daemon (gated by `ENV_RUN_VERIFIER`, scheduled by
//! `VERIFY_SCHEDULE`): a daemon runs the same structural B-tree verification
//! that [`crate::Environment::verify`] runs, on a cron-style schedule, and
//! logs any errors it finds.  It does **not** modify the internals of
//! `Environment::verify` — it re-uses the same public
//! `noxu_engine::verify_database_impl` +
//! `noxu_engine::check_lsns_against_tracker` walk against the shared
//! `EnvironmentImpl`, so it stays in lock-step with `verify` without
//! coupling.
//!
//! The daemon is env-owned (started in `Environment::open`, stopped in
//! `Environment::close`) using the same [`DaemonThread`] lifecycle as the
//! stats-file dumper, so it has NO coupling to the engine's `DaemonManager`
//! and therefore cannot perturb the daemon-manager shutdown ordering.
//!
//! JE ref: `EnvironmentParams.ENV_RUN_VERIFIER` / `VERIFY_SCHEDULE`,
//! `com.sleepycat.je.dbi.DataVerifier` / `BtreeVerifier`.

use noxu_dbi::EnvironmentImpl;
use noxu_engine::VerifyConfig;
use noxu_sync::Mutex;
use noxu_util::daemon::DaemonThread;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Handle to the background verifier daemon.
///
/// Dropping the handle signals shutdown without blocking; call [`stop`] to
/// join the thread.
///
/// [`stop`]: VerifyDaemon::stop
pub struct VerifyDaemon {
    daemon: DaemonThread,
}

impl VerifyDaemon {
    /// Spawn the verifier daemon.
    ///
    /// * `env_impl` — the shared environment the daemon verifies (the same
    ///   handle `Environment::verify` walks).
    /// * `schedule` — a cron-style schedule string (`VERIFY_SCHEDULE`,
    ///   e.g. `"0 0 * * *"` for daily at midnight).  Must be non-empty; the
    ///   caller (`Environment::open`) only starts the daemon when
    ///   `run_verifier` is true AND `schedule` is non-empty, so the default
    ///   (`run_verifier = false`) spawns nothing and behaviour is unchanged.
    /// * `config` — the [`VerifyConfig`] used for each run.
    pub fn start(
        env_impl: Arc<Mutex<EnvironmentImpl>>,
        schedule: CronSchedule,
        config: VerifyConfig,
    ) -> Self {
        Self::start_with_tick(
            env_impl,
            schedule,
            config,
            Duration::from_secs(60),
        )
    }

    /// Like [`start`] but with a caller-chosen wake tick (test seam so an
    /// integration test can observe a run without a 60 s wait).  The tick is
    /// only how often the daemon RE-EVALUATES the schedule; the schedule
    /// itself still governs when a run fires.
    ///
    /// [`start`]: VerifyDaemon::start
    pub fn start_with_tick(
        env_impl: Arc<Mutex<EnvironmentImpl>>,
        schedule: CronSchedule,
        config: VerifyConfig,
        tick: Duration,
    ) -> Self {
        // The daemon wakes on `tick` (60 s in production — cron granularity is
        // minutes) and runs verify only when the current wall-clock minute
        // matches the schedule.  `last_run_minute` prevents a double-run
        // within the same matching minute.
        let last_run_minute = Arc::new(AtomicU64::new(u64::MAX));
        let daemon = DaemonThread::spawn("noxu-verifier", tick, move || {
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0);
            if should_run_now(&schedule, now, &last_run_minute) {
                run_once(&env_impl, &config);
            }
            true
        });
        VerifyDaemon { daemon }
    }

    /// Request shutdown and join the daemon thread.
    pub fn stop(self) {
        self.daemon.shutdown();
    }
}

/// Tick decision (extracted so it is unit-testable without a 60 s wait):
/// true iff `now` matches `schedule` at minute granularity AND this minute
/// has not already fired (de-dupe via `last_run_minute`).
fn should_run_now(
    schedule: &CronSchedule,
    now_epoch_secs: u64,
    last_run_minute: &AtomicU64,
) -> bool {
    if !schedule.matches_epoch_secs(now_epoch_secs) {
        return false;
    }
    let minute = now_epoch_secs / 60;
    // swap returns the previous value; if it equals `minute` we already ran.
    last_run_minute.swap(minute, Ordering::Relaxed) != minute
}

/// Run one verification pass over the shared environment, logging errors.
///
/// This mirrors the read-only walk `Environment::verify` performs — it calls
/// the SAME public `noxu_engine` verification functions against the same
/// `EnvironmentImpl` — WITHOUT touching `Environment::verify`'s internals.
fn run_once(env_impl: &Arc<Mutex<EnvironmentImpl>>, config: &VerifyConfig) {
    // Skip a contended tick rather than block a user operation.
    let guard = match env_impl.try_lock() {
        Some(g) => g,
        None => return,
    };
    let all_dbs = guard.get_all_database_impls();
    let tracker_guard = guard.get_utilization_tracker().map(|t| t.lock());

    let mut merged = noxu_engine::VerifyResult::new();
    for db_arc in &all_dbs {
        let db_guard = db_arc.read();
        let result = noxu_engine::verify_database_impl(&db_guard, config);
        merged.databases_verified += result.databases_verified;
        merged.records_verified += result.records_verified;
        for err in result.errors {
            merged.add_error(err);
            if merged.error_count() >= config.max_errors as usize {
                break;
            }
        }
        if let Some(ref t) = tracker_guard {
            noxu_engine::check_lsns_against_tracker(&db_guard, t, &mut merged);
        }
        if merged.error_count() >= config.max_errors as usize {
            break;
        }
    }

    if merged.error_count() > 0 {
        log::warn!(
            "background verifier found {} error(s) across {} database(s) \
             ({} records verified): {:?}",
            merged.error_count(),
            merged.databases_verified,
            merged.records_verified,
            merged.errors,
        );
    } else {
        log::debug!(
            "background verifier: OK ({} database(s), {} records)",
            merged.databases_verified,
            merged.records_verified,
        );
    }
}

/// A minimal cron schedule (minute hour day-of-month month day-of-week).
///
/// Supports `*`, exact numbers, comma lists, ranges (`a-b`), and steps
/// (`*/n` or `a-b/n`) per field — enough to express JE's `VERIFY_SCHEDULE`
/// examples (`"0 0 * * *"` = daily midnight, `"0 */6 * * *"` = every 6h,
/// etc.).  Not a full cron implementation (no named months/days, no `L`/`#`),
/// which JE's schedule strings do not use.
#[derive(Debug, Clone)]
pub struct CronSchedule {
    minute: CronField,
    hour: CronField,
    dom: CronField,
    month: CronField,
    dow: CronField,
}

impl CronSchedule {
    /// Parse a 5-field cron string. Returns `None` if the string is not a
    /// well-formed 5-field expression (the caller then does not start the
    /// daemon and logs a warning).
    pub fn parse(s: &str) -> Option<CronSchedule> {
        let parts: Vec<&str> = s.split_whitespace().collect();
        if parts.len() != 5 {
            return None;
        }
        Some(CronSchedule {
            minute: CronField::parse(parts[0], 0, 59)?,
            hour: CronField::parse(parts[1], 0, 23)?,
            dom: CronField::parse(parts[2], 1, 31)?,
            month: CronField::parse(parts[3], 1, 12)?,
            dow: CronField::parse(parts[4], 0, 6)?, // 0 = Sunday
        })
    }

    /// True when the given epoch-seconds instant (in UTC) matches this
    /// schedule at minute granularity.
    pub fn matches_epoch_secs(&self, epoch_secs: u64) -> bool {
        let (min, hour, dom, month, dow) = utc_fields(epoch_secs);
        self.minute.matches(min)
            && self.hour.matches(hour)
            && self.month.matches(month)
            // cron OR semantics: when BOTH dom and dow are restricted, a match
            // in either fires; when either is `*`, the other governs.
            && cron_day_match(&self.dom, dom, &self.dow, dow)
    }
}

/// cron day-of-month / day-of-week OR semantics (POSIX crontab): if both are
/// restricted (not `*`), the command runs when EITHER matches.
fn cron_day_match(dom: &CronField, d: u32, dow: &CronField, w: u32) -> bool {
    match (dom.is_wildcard, dow.is_wildcard) {
        (true, true) => true,
        (false, true) => dom.matches(d),
        (true, false) => dow.matches(w),
        (false, false) => dom.matches(d) || dow.matches(w),
    }
}

/// One parsed cron field: a bitset of allowed values plus a wildcard flag.
#[derive(Debug, Clone)]
struct CronField {
    /// Allowed values (bit i set => value `min+i` allowed). Small (<=60), so
    /// a `u64` bitset relative to `min` is enough.
    allowed: u64,
    min: u32,
    is_wildcard: bool,
}

impl CronField {
    fn parse(field: &str, min: u32, max: u32) -> Option<CronField> {
        let mut allowed: u64 = 0;
        let is_wildcard = field == "*";
        for part in field.split(',') {
            let (range_part, step) = match part.split_once('/') {
                Some((r, s)) => (r, s.parse::<u32>().ok().filter(|n| *n > 0)?),
                None => (part, 1),
            };
            let (lo, hi) = if range_part == "*" {
                (min, max)
            } else if let Some((a, b)) = range_part.split_once('-') {
                (a.parse::<u32>().ok()?, b.parse::<u32>().ok()?)
            } else {
                let v = range_part.parse::<u32>().ok()?;
                (v, v)
            };
            if lo < min || hi > max || lo > hi {
                return None;
            }
            let mut v = lo;
            while v <= hi {
                allowed |= 1u64 << (v - min);
                v += step;
            }
        }
        Some(CronField { allowed, min, is_wildcard })
    }

    fn matches(&self, value: u32) -> bool {
        if value < self.min {
            return false;
        }
        let bit = value - self.min;
        bit < 64 && (self.allowed & (1u64 << bit)) != 0
    }
}

/// Convert epoch seconds (UTC) to (minute, hour, day-of-month, month,
/// day-of-week) — a compact civil-from-days algorithm (Howard Hinnant's
/// `civil_from_days`), avoiding a chrono dependency.
fn utc_fields(epoch_secs: u64) -> (u32, u32, u32, u32, u32) {
    let days = (epoch_secs / 86_400) as i64;
    let secs_of_day = epoch_secs % 86_400;
    let minute = ((secs_of_day / 60) % 60) as u32;
    let hour = (secs_of_day / 3600) as u32;
    // day-of-week: 1970-01-01 was a Thursday (=4); cron uses 0=Sunday.
    let dow = (((days % 7) + 4 + 7) % 7) as u32;

    // civil_from_days: days since 1970-01-01 -> (year, month, day).
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0,399]
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0,365]
    let mp = (5 * doy + 2) / 153; // [0,11]
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1,31]
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32; // [1,12]

    (minute, hour, day, month, dow)
}

#[cfg(test)]
mod tests {
    use super::*;

    // Known epoch instants (UTC) to validate the civil-date decode.
    // 2021-01-01 00:00:00 UTC = 1_609_459_200 (a Friday, dow=5).
    #[test]
    fn utc_fields_known_instant() {
        let (min, hour, dom, month, dow) = utc_fields(1_609_459_200);
        assert_eq!((min, hour, dom, month), (0, 0, 1, 1));
        assert_eq!(dow, 5, "2021-01-01 is a Friday (cron dow 5)");
    }

    #[test]
    fn daily_midnight_matches_only_at_midnight() {
        let s = CronSchedule::parse("0 0 * * *").unwrap();
        // 2021-01-01 00:00 UTC -> match.
        assert!(s.matches_epoch_secs(1_609_459_200));
        // +1 minute -> no match.
        assert!(!s.matches_epoch_secs(1_609_459_200 + 60));
        // +1 hour -> no match (hour != 0).
        assert!(!s.matches_epoch_secs(1_609_459_200 + 3600));
    }

    #[test]
    fn every_six_hours_step() {
        let s = CronSchedule::parse("0 */6 * * *").unwrap();
        let midnight = 1_609_459_200; // 00:00
        assert!(s.matches_epoch_secs(midnight)); // 0h
        assert!(!s.matches_epoch_secs(midnight + 3600)); // 1h
        assert!(s.matches_epoch_secs(midnight + 6 * 3600)); // 6h
        assert!(s.matches_epoch_secs(midnight + 12 * 3600)); // 12h
        assert!(!s.matches_epoch_secs(midnight + 7 * 3600)); // 7h
    }

    #[test]
    fn exact_minute_hour() {
        let s = CronSchedule::parse("30 2 * * *").unwrap();
        let midnight = 1_609_459_200;
        assert!(s.matches_epoch_secs(midnight + 2 * 3600 + 30 * 60)); // 02:30
        assert!(!s.matches_epoch_secs(midnight + 2 * 3600)); // 02:00
        assert!(!s.matches_epoch_secs(midnight + 3 * 3600 + 30 * 60)); // 03:30
    }

    #[test]
    fn range_and_list_fields() {
        // minutes 0 and 15, hours 9 through 17.
        let s = CronSchedule::parse("0,15 9-17 * * *").unwrap();
        let midnight = 1_609_459_200;
        assert!(s.matches_epoch_secs(midnight + 9 * 3600)); // 09:00
        assert!(s.matches_epoch_secs(midnight + 9 * 3600 + 15 * 60)); // 09:15
        assert!(!s.matches_epoch_secs(midnight + 9 * 3600 + 30 * 60)); // 09:30
        assert!(!s.matches_epoch_secs(midnight + 8 * 3600)); // 08:00 (< 9)
        assert!(s.matches_epoch_secs(midnight + 17 * 3600)); // 17:00
        assert!(!s.matches_epoch_secs(midnight + 18 * 3600)); // 18:00 (> 17)
    }

    #[test]
    fn rejects_malformed() {
        assert!(CronSchedule::parse("").is_none());
        assert!(CronSchedule::parse("0 0 * *").is_none()); // 4 fields
        assert!(CronSchedule::parse("60 0 * * *").is_none()); // minute > 59
        assert!(CronSchedule::parse("0 24 * * *").is_none()); // hour > 23
        assert!(CronSchedule::parse("x 0 * * *").is_none()); // non-numeric
    }

    #[test]
    fn dom_dow_or_semantics() {
        // Run on the 1st OR on a Monday (dow 1). Both restricted => OR.
        let s = CronSchedule::parse("0 0 1 * 1").unwrap();
        // 2021-01-01 is the 1st (and a Friday) -> matches via dom.
        assert!(s.matches_epoch_secs(1_609_459_200));
        // 2021-01-04 is a Monday (not the 1st) -> matches via dow.
        let jan4 = 1_609_459_200 + 3 * 86_400;
        assert!(s.matches_epoch_secs(jan4));
        // 2021-01-05 is a Tuesday, not the 1st -> no match.
        let jan5 = 1_609_459_200 + 4 * 86_400;
        assert!(!s.matches_epoch_secs(jan5));
    }

    #[test]
    fn should_run_now_fires_once_per_matching_minute() {
        // "* * * * *" matches every minute.
        let s = CronSchedule::parse("* * * * *").unwrap();
        let last = AtomicU64::new(u64::MAX);
        let t = 1_609_459_200; // 2021-01-01 00:00:00
        // First tick in the minute -> run.
        assert!(should_run_now(&s, t, &last));
        // Second tick in the SAME minute -> skip (de-dupe).
        assert!(!should_run_now(&s, t + 30, &last));
        // Next minute -> run again.
        assert!(should_run_now(&s, t + 60, &last));
    }

    #[test]
    fn should_run_now_skips_non_matching_minute() {
        // Only at 02:30.
        let s = CronSchedule::parse("30 2 * * *").unwrap();
        let last = AtomicU64::new(u64::MAX);
        let midnight = 1_609_459_200;
        // 00:00 does not match -> no run, and last is untouched.
        assert!(!should_run_now(&s, midnight, &last));
        // 02:30 matches -> run.
        assert!(should_run_now(&s, midnight + 2 * 3600 + 30 * 60, &last));
    }
}
