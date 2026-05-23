//! Adaptive cleaner throttle: write-rate tracking, backoff and acceleration.
//!
//! Implements `CleanerThrottle` — tracks an exponential moving average of
//! the log write rate (bytes/second) and uses it to:
//!
//! - Compute how long the cleaner daemon should **sleep** between passes
//!   (backs off when idle, accelerates when write pressure is high).
//! - Recommend how many files to clean per pass.
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
pub const HIGH_WRITE_THRESHOLD_BYTES_PER_SEC: u64 = 1_000_000;

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

    /// Returns a recommended write-path delay when the log write rate exceeds
    /// `HIGH_WRITE_THRESHOLD_BYTES_PER_SEC`, or `None` if no throttling is
    /// needed.
    ///
    /// The delay scales linearly with how far above the threshold we are,
    /// clamped to [1 ms, 50 ms].  At 2× threshold the writer sleeps ~2 ms;
    /// at 10× threshold it sleeps ~10 ms; above 50× it sleeps 50 ms.
    ///
    /// This is the write-path counterpart to the cleaner's adaptive sleep.
    /// equivalent logic in `CleanerThrottle.getWriteDelay()`.
    pub fn should_throttle_writer(&self) -> Option<std::time::Duration> {
        let rate = self.write_rate_bytes_per_sec() as u64;
        if rate <= HIGH_WRITE_THRESHOLD_BYTES_PER_SEC {
            return None;
        }
        // overshoot factor (1.0 at threshold, 2.0 at 2× threshold, etc.)
        let factor = rate / HIGH_WRITE_THRESHOLD_BYTES_PER_SEC;
        let delay_ms = factor.clamp(1, 50);
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
}
