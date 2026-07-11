//! Adaptive cleaner throttle: cleaner-daemon sleep tuning and backlog-driven
//! write-path backpressure.
//!
//! Implements `CleanerThrottle`:
//!
//! - The **cleaner daemon** sleep interval and files-per-pass are tuned from
//!   an exponential moving average of the log write rate (backs off when idle,
//!   accelerates under write pressure).
//! - The **write path** backpressure ([`CleanerThrottle::should_throttle_writer`])
//!   is gated on the cleaner *backlog* — the count of files queued for cleaning
//!   that the cleaner has not caught up on — NOT on the raw write rate. This
//!   mirrors JE's `EnvironmentImpl.checkDiskLimitViolation()` write-path gate,
//!   which fires only when the cleaner cannot reclaim space fast enough. A
//!   workload that keeps the cleaner caught up (e.g. a fresh insert into empty
//!   space with nothing yet to clean) is never throttled.
//!
//! # Algorithm
//!
//! After each cleaning pass the caller invokes [`CleanerThrottle::update`]
//! with the total bytes written to the log so far (available from
//! `LogManagerStats::n_sequential_write_bytes`).  The throttle computes:
//!
//! 1. The instantaneous write rate since the previous update.
//! 2. Blends it into an EWMA (α = 0.3).
//! 3. Derives a sleep interval:
//!    `sleep = clamp(BASE_SLEEP * HIGH_WRITE_THRESHOLD / max(rate, 1), MIN, MAX)`
//!
//!    High write rate → shorter sleep (aggressive cleaning).
//!    Low/zero write rate → sleep up to MAX (back off).
//!
//! 4. Derives a recommended file count:
//!    `n_files = clamp(1 + rate / HIGH_WRITE_THRESHOLD_PER_FILE, 1, MAX_FILES)`

use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Base wakeup interval when cleaning pressure is normal (1 second).
pub const BASE_SLEEP_MS: u64 = 1_000;

/// Maximum sleep between passes when the log is idle (10 seconds).
pub const MAX_SLEEP_MS: u64 = 10_000;

/// Minimum sleep between passes at peak write pressure (100 ms).
pub const MIN_SLEEP_MS: u64 = 100;

/// Write-rate threshold above which cleaning accelerates (1 MB/s).
///
/// Implements `EnvironmentParams.CLEANER_BYTES_INTERVAL` default (10 MiB),
/// divided by the base wakeup interval to yield a per-second figure.
///
/// NOTE: this drives only the *cleaner daemon* sleep interval (how often the
/// daemon wakes to clean). It is **not** the write-path backpressure gate —
/// see [`CleanerThrottle::should_throttle_writer`], which is gated on the
/// cleaner *backlog*, not on raw write rate.
pub const HIGH_WRITE_THRESHOLD_BYTES_PER_SEC: u64 = 1_000_000;

/// Cleaner backlog (files queued but not yet cleaned) at or below which the
/// write path is **never** throttled.
///
/// JE gates write-path backpressure on the cleaner falling behind — see
/// `EnvironmentImpl.checkDiskLimitViolation()` (called from the write path in
/// `FileProcessor` / `Checkpointer` / `DirtyINMap`), which is driven by the
/// cleaner's inability to reclaim obsolete space (a real backlog), *not* by a
/// raw bytes/sec write rate. JE has no rate throttle. We model the same
/// gating signal with the count of files the cleaner is behind on
/// (`FileSelector.to_be_cleaned`): a fresh insert workload with nothing yet to
/// clean has a zero backlog and is never throttled.
pub const BACKLOG_THROTTLE_THRESHOLD: u64 = 8;

/// Maximum write-path throttle delay (ms) when the backlog is severe.
pub const MAX_WRITE_DELAY_MS: u64 = 50;

/// Write-rate (bytes/s) that adds one extra file per pass.
///
/// At 2× threshold the cleaner runs 2 files per pass, etc.
const BYTES_PER_SEC_PER_EXTRA_FILE: u64 = 500_000;

/// Maximum files per cleaning pass.
pub const MAX_FILES_PER_PASS: u32 = 8;

/// Minimum files per cleaning pass.
pub const MIN_FILES_PER_PASS: u32 = 1;

/// EWMA smoothing factor (α).  Larger values weight recent samples more.
const EWMA_ALPHA: f64 = 0.3;

/// Adaptive throttle for the log cleaner daemon.
///
/// Thread-safe; designed to be shared across the cleaner daemon and the
/// environment's metrics path via `Arc<CleanerThrottle>`.
pub struct CleanerThrottle {
    /// Total bytes written to the log at the time of the last `update()`.
    last_bytes: AtomicU64,

    /// Monotonic wall-clock timestamp (ms since Unix epoch) of the last
    /// `update()` call.  Initialised to the construction time.
    last_time_ms: AtomicU64,

    /// Exponential moving average of the write rate (bytes / second).
    ///
    /// Protected by a `Mutex` because floating-point EWMA update requires
    /// a read-modify-write that cannot be done with `AtomicU64` alone.
    write_rate_ewma: Mutex<f64>,

    /// Most-recently computed sleep interval (ms) — cached for reporting.
    sleep_interval_ms: AtomicU64,

    /// Most-recently recommended number of files to clean per pass.
    recommended_n_files: AtomicU64,

    /// Cleaner backlog: the number of log files queued for cleaning that the
    /// cleaner has **not yet** caught up on (`FileSelector.to_be_cleaned`).
    ///
    /// Published by the cleaner after each pass via [`Self::set_backlog`] and
    /// read by [`Self::should_throttle_writer`] to decide whether the write
    /// path should apply backpressure. This is the JE-faithful gating signal
    /// (the cleaner falling behind), replacing the old raw-write-rate gate.
    backlog: AtomicU64,
}

impl CleanerThrottle {
    /// Creates a new throttle seeded with `initial_bytes_written` (typically
    /// zero at environment open time).
    pub fn new(initial_bytes_written: u64) -> Self {
        CleanerThrottle {
            last_bytes: AtomicU64::new(initial_bytes_written),
            last_time_ms: AtomicU64::new(now_ms()),
            write_rate_ewma: Mutex::new(0.0),
            sleep_interval_ms: AtomicU64::new(BASE_SLEEP_MS),
            recommended_n_files: AtomicU64::new(MIN_FILES_PER_PASS as u64),
            backlog: AtomicU64::new(0),
        }
    }

    /// Updates the throttle with the current total bytes written to the log
    /// and returns the recommended `(sleep_ms, n_files)` for the next pass.
    ///
    /// # Arguments
    /// * `current_bytes_written` – cumulative bytes written to the log
    ///   (from `LogManagerStats::n_sequential_write_bytes`).
    /// * `cleaning_needed` – `true` when at least one file is below the
    ///   minimum utilisation threshold; forces a shorter sleep interval.
    pub fn update(
        &self,
        current_bytes_written: u64,
        cleaning_needed: bool,
    ) -> (u64, u32) {
        let now = now_ms();
        let prev_bytes =
            self.last_bytes.swap(current_bytes_written, Ordering::Relaxed);
        let prev_time = self.last_time_ms.swap(now, Ordering::Relaxed);

        let elapsed_ms = now.saturating_sub(prev_time).max(1);
        let delta_bytes = current_bytes_written.saturating_sub(prev_bytes);

        // Instantaneous write rate (bytes/sec).
        let instant_rate = (delta_bytes as f64 * 1_000.0) / elapsed_ms as f64;

        // Update EWMA.
        let rate = {
            let mut ewma =
                self.write_rate_ewma.lock().unwrap_or_else(|p| p.into_inner());
            *ewma = *ewma * (1.0 - EWMA_ALPHA) + instant_rate * EWMA_ALPHA;
            *ewma
        };

        // Compute sleep interval:
        //   sleep = BASE * HIGH_THRESHOLD / max(rate, 1)
        // Clamp to [MIN, MAX].  When cleaning is needed, cap at BASE.
        let rate_u64 = rate as u64;
        let sleep_ms = if rate_u64 == 0 {
            if cleaning_needed { BASE_SLEEP_MS } else { MAX_SLEEP_MS }
        } else {
            let computed = (BASE_SLEEP_MS as f64
                * HIGH_WRITE_THRESHOLD_BYTES_PER_SEC as f64
                / rate.max(1.0)) as u64;
            let capped = if cleaning_needed {
                computed.min(BASE_SLEEP_MS)
            } else {
                computed
            };
            capped.clamp(MIN_SLEEP_MS, MAX_SLEEP_MS)
        };

        // Compute recommended file count:
        //   n_files = 1 + rate / BYTES_PER_SEC_PER_EXTRA_FILE, clamped.
        let n_files = (1 + rate_u64 / BYTES_PER_SEC_PER_EXTRA_FILE.max(1))
            .clamp(MIN_FILES_PER_PASS as u64, MAX_FILES_PER_PASS as u64)
            as u32;

        self.sleep_interval_ms.store(sleep_ms, Ordering::Relaxed);
        self.recommended_n_files.store(n_files as u64, Ordering::Relaxed);

        (sleep_ms, n_files)
    }

    /// Returns the most recently computed sleep interval in milliseconds.
    pub fn current_sleep_ms(&self) -> u64 {
        self.sleep_interval_ms.load(Ordering::Relaxed)
    }

    /// Returns the most recently recommended files-per-pass count.
    pub fn current_n_files(&self) -> u32 {
        self.recommended_n_files.load(Ordering::Relaxed) as u32
    }

    /// Returns the EWMA write rate in bytes/second.
    pub fn write_rate_bytes_per_sec(&self) -> f64 {
        *self.write_rate_ewma.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Publishes the current cleaner backlog — the number of log files queued
    /// for cleaning that the cleaner has not yet caught up on
    /// (`FileSelector.to_be_cleaned`). Called by the cleaner after each pass.
    ///
    /// This is the signal [`Self::should_throttle_writer`] gates on. When the
    /// cleaner is keeping up the backlog is zero and the write path is never
    /// throttled.
    pub fn set_backlog(&self, files_behind: u64) {
        self.backlog.store(files_behind, Ordering::Relaxed);
    }

    /// Returns the most recently published cleaner backlog (files behind).
    pub fn current_backlog(&self) -> u64 {
        self.backlog.load(Ordering::Relaxed)
    }

    /// Returns a recommended write-path delay when the cleaner has fallen
    /// **behind** — i.e. when the backlog of files queued for cleaning exceeds
    /// [`BACKLOG_THROTTLE_THRESHOLD`] — or `None` when the cleaner is keeping
    /// up (the common case, including a fresh insert workload with nothing yet
    /// to clean).
    ///
    /// # JE-faithful gating (the fix)
    ///
    /// This replaces the previous defect: a fixed raw-write-**rate** gate
    /// (`rate > HIGH_WRITE_THRESHOLD_BYTES_PER_SEC`, 1 MB/s) that fired under
    /// *any* sustained write load regardless of whether the cleaner was
    /// behind, sleeping every committer and capping write throughput at just
    /// above 1 MB/s on devices doing GB/s.
    ///
    /// JE has no raw-rate write throttle. Its write-path backpressure is
    /// `EnvironmentImpl.checkDiskLimitViolation()` — a gate driven by the
    /// cleaner's inability to reclaim obsolete log space (a real backlog),
    /// checked on the write path (`FileProcessor.doClean`,
    /// `Checkpointer.checkpoint`, `DirtyINMap.selectDirtyINsForCheckpoint`).
    /// When the cleaner keeps up, JE does not throttle; when it genuinely
    /// cannot keep up, JE prohibits writes. We model the same *gating signal*
    /// — the cleaner falling behind — with the count of files queued for
    /// cleaning (`FileSelector.to_be_cleaned`), applying a graduated sleep
    /// (softer than JE's hard `DiskLimitException`) so writers slow to let the
    /// cleaner catch up before the log grows unboundedly.
    ///
    /// The delay scales linearly with how far past the threshold the backlog
    /// is, clamped to `[1 ms, MAX_WRITE_DELAY_MS]`.
    pub fn should_throttle_writer(&self) -> Option<std::time::Duration> {
        let backlog = self.backlog.load(Ordering::Relaxed);
        if backlog <= BACKLOG_THROTTLE_THRESHOLD {
            // Cleaner is keeping up — no backpressure.
            return None;
        }
        // Files past the threshold; 1 ms per extra file, clamped.
        let overshoot = backlog - BACKLOG_THROTTLE_THRESHOLD;
        let delay_ms = overshoot.clamp(1, MAX_WRITE_DELAY_MS);
        Some(std::time::Duration::from_millis(delay_ms))
    }
}

impl Default for CleanerThrottle {
    fn default() -> Self {
        Self::new(0)
    }
}

impl std::fmt::Debug for CleanerThrottle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CleanerThrottle")
            .field("sleep_interval_ms", &self.current_sleep_ms())
            .field("recommended_n_files", &self.current_n_files())
            .field(
                "write_rate_bytes_per_sec",
                &format!("{:.0}", self.write_rate_bytes_per_sec()),
            )
            .field("backlog", &self.current_backlog())
            .finish()
    }
}

/// Returns the current time in milliseconds since Unix epoch.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_defaults() {
        let t = CleanerThrottle::new(0);
        assert_eq!(t.current_sleep_ms(), BASE_SLEEP_MS);
        assert_eq!(t.current_n_files(), MIN_FILES_PER_PASS);
        assert_eq!(t.write_rate_bytes_per_sec(), 0.0);
    }

    #[test]
    fn test_zero_write_rate_backs_off() {
        let t = CleanerThrottle::new(0);
        // No new bytes written since last call.
        let (sleep_ms, n_files) = t.update(0, false);
        assert_eq!(sleep_ms, MAX_SLEEP_MS, "idle should sleep maximum");
        assert_eq!(n_files, MIN_FILES_PER_PASS);
    }

    #[test]
    fn test_zero_write_rate_cleaning_needed_sleeps_base() {
        let t = CleanerThrottle::new(0);
        // No new bytes but cleaning is needed.
        let (sleep_ms, n_files) = t.update(0, true);
        assert_eq!(
            sleep_ms, BASE_SLEEP_MS,
            "cleaning needed caps sleep at BASE"
        );
        assert_eq!(n_files, MIN_FILES_PER_PASS);
    }

    #[test]
    fn test_high_write_rate_accelerates() {
        // Seed with a previous byte count a second ago.
        let t = CleanerThrottle::new(0);
        // Force a tiny elapsed so we get an exaggerated rate; just test the
        // direction: more bytes → shorter sleep, more files.
        let (sleep_ms_low, n_files_low) = t.update(100, false); // 100 B written
        let t2 = CleanerThrottle::new(0);
        let (sleep_ms_high, n_files_high) = t2.update(50_000_000, false); // 50 MB written

        // Higher write load should produce shorter or equal sleep and more files.
        assert!(
            sleep_ms_high <= sleep_ms_low,
            "high write rate should not increase sleep: {sleep_ms_high} vs {sleep_ms_low}"
        );
        assert!(
            n_files_high >= n_files_low,
            "high write rate should recommend more files: {n_files_high} vs {n_files_low}"
        );
    }

    #[test]
    fn test_sleep_always_in_range() {
        let t = CleanerThrottle::new(0);
        for bytes in [0, 1_000, 1_000_000, 100_000_000, u64::MAX / 2] {
            let (sleep_ms, n_files) = t.update(bytes, bytes > 0);
            assert!(
                (MIN_SLEEP_MS..=MAX_SLEEP_MS).contains(&sleep_ms),
                "sleep out of range: {sleep_ms} for bytes={bytes}"
            );
            assert!(
                (MIN_FILES_PER_PASS..=MAX_FILES_PER_PASS).contains(&n_files),
                "n_files out of range: {n_files} for bytes={bytes}"
            );
        }
    }

    #[test]
    fn test_ewma_smooths_over_multiple_updates() {
        let t = CleanerThrottle::new(0);
        // Several large writes should push the EWMA upward.
        t.update(1_000_000, true);
        t.update(2_000_000, true);
        t.update(3_000_000, true);
        assert!(
            t.write_rate_bytes_per_sec() > 0.0,
            "EWMA should be positive after multiple writes"
        );
    }

    #[test]
    fn test_n_files_clamped_at_max() {
        let t = CleanerThrottle::new(0);
        // Enormous write volume.
        let (_, n_files) = t.update(u64::MAX / 2, true);
        assert_eq!(n_files, MAX_FILES_PER_PASS, "n_files must be clamped");
    }

    #[test]
    fn test_debug_impl() {
        let t = CleanerThrottle::default();
        let s = format!("{t:?}");
        assert!(s.contains("CleanerThrottle"));
        assert!(s.contains("sleep_interval_ms"));
    }

    #[test]
    fn test_cleaning_needed_caps_sleep() {
        let t = CleanerThrottle::new(0);
        let (sleep_no_pressure, _) = t.update(0, false);
        let t2 = CleanerThrottle::new(0);
        let (sleep_with_pressure, _) = t2.update(0, true);
        assert!(
            sleep_with_pressure <= sleep_no_pressure,
            "cleaning_needed should not increase sleep"
        );
    }

    #[test]
    fn test_no_backlog_no_throttle() {
        // A fresh throttle (backlog 0) must never throttle the write path,
        // regardless of write rate — this is the fix for the ~7k write ceiling:
        // a fresh insert workload with nothing to clean is not slowed.
        let t = CleanerThrottle::new(0);
        // Even after pushing a large write rate through the daemon EWMA:
        t.update(500_000_000, false); // ~500 MB "written", huge rate
        assert_eq!(t.current_backlog(), 0);
        assert!(
            t.should_throttle_writer().is_none(),
            "no backlog => no write-path throttle even at high write rate"
        );
    }

    #[test]
    fn test_backlog_at_threshold_no_throttle() {
        let t = CleanerThrottle::new(0);
        // Backlog exactly at the threshold: cleaner still deemed keeping up.
        t.set_backlog(BACKLOG_THROTTLE_THRESHOLD);
        assert!(
            t.should_throttle_writer().is_none(),
            "backlog at threshold must not throttle"
        );
    }

    #[test]
    fn test_backlog_over_threshold_throttles() {
        let t = CleanerThrottle::new(0);
        // Backlog past the threshold: cleaner is behind => backpressure.
        t.set_backlog(BACKLOG_THROTTLE_THRESHOLD + 1);
        let delay = t.should_throttle_writer();
        assert!(delay.is_some(), "real backlog must throttle the write path");
        assert_eq!(
            delay.unwrap(),
            std::time::Duration::from_millis(1),
            "one file over threshold => 1 ms"
        );
    }

    #[test]
    fn test_backlog_delay_scales_and_clamps() {
        let t = CleanerThrottle::new(0);
        // Well past threshold => larger delay, but clamped to MAX_WRITE_DELAY_MS.
        t.set_backlog(BACKLOG_THROTTLE_THRESHOLD + 5);
        assert_eq!(
            t.should_throttle_writer().unwrap(),
            std::time::Duration::from_millis(5)
        );
        // Enormous backlog clamps at the max.
        t.set_backlog(BACKLOG_THROTTLE_THRESHOLD + 10_000);
        assert_eq!(
            t.should_throttle_writer().unwrap(),
            std::time::Duration::from_millis(MAX_WRITE_DELAY_MS)
        );
    }

    #[test]
    fn test_write_rate_does_not_gate_write_throttle() {
        // Regression guard: the write-path throttle must be gated on backlog,
        // NOT on raw write rate. A high EWMA rate with zero backlog => no sleep.
        let t = CleanerThrottle::new(0);
        for b in [10_000_000u64, 50_000_000, 100_000_000] {
            t.update(b, false);
        }
        assert!(
            t.write_rate_bytes_per_sec()
                > HIGH_WRITE_THRESHOLD_BYTES_PER_SEC as f64,
            "precondition: EWMA rate well above the old 1 MB/s gate"
        );
        assert_eq!(t.current_backlog(), 0);
        assert!(
            t.should_throttle_writer().is_none(),
            "high write rate must not throttle when the cleaner is caught up"
        );
    }
}
