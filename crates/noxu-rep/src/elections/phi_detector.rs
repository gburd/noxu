//! Phi accrual failure detector (Hayashibara et al., SRDS 2004).
//!
//! Instead of a binary "alive / dead" decision, the detector outputs a
//! continuous *suspicion level* φ(t):
//!
//! ```text
//! φ(t) = −log₁₀(P_later(t − T_last))
//! ```
//!
//! where `P_later(u)` is the probability that the *next* inter-arrival gap
//! will be at least `u`, estimated from the last `window_size` inter-arrival
//! samples as a Normal distribution.
//!
//! The process is considered **suspected** when `φ ≥ threshold`.
//!
//! Recommended production defaults (from the paper):
//! - `threshold = 8.0`  → mistake rate ≈ 10⁻⁸
//! - `window_size = 200` for LAN; `1000` for WAN
//!
//! Returns `phi = 0.0` when fewer than two samples have been collected
//! (window not yet populated → always available).
//!
//! The Normal CDF is approximated with the Abramowitz & Stegun 26.2.17
//! rational approximation — accurate to |ε| < 1.5 × 10⁻⁷, no external
//! dependencies required.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use noxu_sync::RwLock;

// ---------------------------------------------------------------------------
// Normal CDF helper
// ---------------------------------------------------------------------------

/// Abramowitz & Stegun 26.2.17 approximation for Φ(x) — the standard Normal CDF.
/// Max error: |ε| < 1.5 × 10⁻⁷.
fn phi_cdf(x: f64) -> f64 {
    let t = 1.0 / (1.0 + 0.2316419 * x.abs());
    let poly = t
        * (0.319_381_53
            + t * (-0.356_563_78
                + t * (1.781_477_94
                    + t * (-1.821_255_978 + t * 1.330_274_429))));
    let base = 1.0
        - ((-0.5 * x * x).exp()) / (2.0 * std::f64::consts::PI).sqrt() * poly;
    if x >= 0.0 { base } else { 1.0 - base }
}

/// Survival function: P(X > u) = 1 − Φ((u − μ) / σ).
///
/// Returns 1.0 (always available) when σ ≤ 0 (degenerate distribution).
fn p_later(u: f64, mean: f64, std_dev: f64) -> f64 {
    if std_dev <= 0.0 {
        return 1.0;
    }
    let z = (u - mean) / std_dev;
    1.0 - phi_cdf(z)
}

// ---------------------------------------------------------------------------
// PhiAccrualDetector
// ---------------------------------------------------------------------------

/// Phi accrual failure detector.
///
/// Call `record_heartbeat` whenever a heartbeat arrives from the monitored
/// process.  Query `phi` or `is_available` to assess liveness.
///
/// All methods are thread-safe.
pub struct PhiAccrualDetector {
    /// Sliding window of inter-arrival times (seconds).
    samples: RwLock<VecDeque<f64>>,
    /// Maximum number of samples to retain.
    window_size: usize,
    /// Timestamp of the most recent heartbeat.
    last_heartbeat: RwLock<Option<Instant>>,
    /// φ threshold above which the process is suspected.
    threshold: f64,
}

impl PhiAccrualDetector {
    /// Create a new detector.
    ///
    /// - `threshold`: suspicion level above which the process is considered
    ///   failed.  `8.0` is the paper's recommended production value.
    /// - `window_size`: number of inter-arrival samples to retain.  `200` is
    ///   adequate for LAN; use `1000` for WAN.
    pub fn new(threshold: f64, window_size: usize) -> Self {
        Self {
            samples: RwLock::new(VecDeque::with_capacity(window_size + 1)),
            window_size,
            last_heartbeat: RwLock::new(None),
            threshold,
        }
    }

    /// Record a heartbeat from the monitored process.
    ///
    /// On the **first** call there is no previous heartbeat, so only the
    /// `last_heartbeat` timestamp is set and the inter-arrival sample
    /// window is left untouched. On every **subsequent** call the
    /// elapsed time since the previous heartbeat is appended to the
    /// sample window (older samples are evicted once the window is full).
    pub fn record_heartbeat(&self) {
        let now = Instant::now();
        let mut last = self.last_heartbeat.write();
        if let Some(prev) = *last {
            let interval = now.duration_since(prev).as_secs_f64();
            let mut samples = self.samples.write();
            samples.push_back(interval);
            if samples.len() > self.window_size {
                samples.pop_front();
            }
        }
        *last = Some(now);
    }

    /// Compute the current suspicion level φ.
    ///
    /// Returns `0.0` when fewer than two samples exist (window not yet
    /// populated — process is always considered available).
    pub fn phi(&self) -> f64 {
        let last_opt = *self.last_heartbeat.read();
        let last = match last_opt {
            Some(t) => t,
            None => return 0.0,
        };

        let samples = self.samples.read();
        if samples.len() < 2 {
            return 0.0;
        }

        let n = samples.len() as f64;
        let mean = samples.iter().sum::<f64>() / n;
        let variance =
            samples.iter().map(|&x| (x - mean).powi(2)).sum::<f64>() / n;
        let std_dev = variance.sqrt();

        let elapsed = last.elapsed().as_secs_f64();
        let p = p_later(elapsed, mean, std_dev).max(f64::MIN_POSITIVE);
        -p.log10()
    }

    /// Returns `true` when φ < threshold (the process is NOT suspected).
    pub fn is_available(&self) -> bool {
        self.phi() < self.threshold
    }

    /// The configured suspicion threshold.
    pub fn threshold(&self) -> f64 {
        self.threshold
    }

    /// Number of inter-arrival samples currently in the window.
    pub fn sample_count(&self) -> usize {
        self.samples.read().len()
    }

    /// Mean heartbeat inter-arrival time in seconds.
    /// Returns None if fewer than 2 samples in the window.
    pub fn mean_interval(&self) -> Option<f64> {
        let samples = self.samples.read();
        if samples.len() < 2 {
            return None;
        }
        Some(samples.iter().sum::<f64>() / samples.len() as f64)
    }

    /// Standard deviation of heartbeat inter-arrival times in seconds.
    /// Returns None if fewer than 2 samples.
    pub fn stddev_interval(&self) -> Option<f64> {
        let samples = self.samples.read();
        if samples.len() < 2 {
            return None;
        }
        let mean = samples.iter().sum::<f64>() / samples.len() as f64;
        let variance = samples.iter().map(|x| (x - mean).powi(2)).sum::<f64>()
            / samples.len() as f64;
        Some(variance.sqrt())
    }

    /// Recommended election phase timeout: mean + k * stddev, clamped to [50ms, 5s].
    /// Returns the configured fallback duration if window has < 2 samples.
    pub fn suggested_phase_timeout(
        &self,
        k: f64,
        fallback: Duration,
    ) -> Duration {
        let mean = match self.mean_interval() {
            Some(m) => m,
            None => return fallback,
        };
        let std = self.stddev_interval().unwrap_or(0.0);
        let secs = (mean + k * std).clamp(0.05, 5.0);
        Duration::from_secs_f64(secs)
    }
}

// Safety: all interior mutability is behind noxu_sync RwLocks.
unsafe impl Send for PhiAccrualDetector {}
unsafe impl Sync for PhiAccrualDetector {}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_phi_zero_with_no_heartbeats() {
        let d = PhiAccrualDetector::new(8.0, 200);
        assert_eq!(d.phi(), 0.0);
        assert!(d.is_available());
    }

    #[test]
    fn test_phi_zero_with_one_sample() {
        let d = PhiAccrualDetector::new(8.0, 200);
        d.record_heartbeat();
        // Only one heartbeat → no inter-arrival interval yet.
        assert_eq!(d.phi(), 0.0);
        d.record_heartbeat();
        // Two heartbeats → one sample, but need ≥2 samples for a distribution.
        // (phi is still 0 because len < 2)
        // After two heartbeats we have 1 sample; need 2.
        assert_eq!(d.sample_count(), 1);
        assert_eq!(d.phi(), 0.0);
    }

    #[test]
    fn test_phi_available_just_after_heartbeat() {
        let d = PhiAccrualDetector::new(8.0, 200);
        // Prime the window with several 10 ms heartbeats.
        for _ in 0..10 {
            d.record_heartbeat();
            thread::sleep(Duration::from_millis(10));
        }
        // Immediately after a heartbeat, elapsed ≈ 0 → phi should be very low.
        d.record_heartbeat();
        assert!(d.phi() < 1.0, "phi={}", d.phi());
        assert!(d.is_available());
    }

    #[test]
    fn test_phi_rises_without_heartbeat() {
        let d = PhiAccrualDetector::new(8.0, 200);
        // Prime with ~10 ms heartbeats.
        for _ in 0..10 {
            d.record_heartbeat();
            thread::sleep(Duration::from_millis(10));
        }
        // Wait much longer than the mean interval (10 ms × 5 = 50 ms > 3σ).
        thread::sleep(Duration::from_millis(200));
        let phi = d.phi();
        assert!(phi > 1.0, "expected phi > 1.0 after long silence, got {phi}");
    }

    #[test]
    fn test_phi_resets_on_heartbeat() {
        let d = PhiAccrualDetector::new(8.0, 200);
        for _ in 0..10 {
            d.record_heartbeat();
            thread::sleep(Duration::from_millis(10));
        }
        thread::sleep(Duration::from_millis(200));
        let phi_before = d.phi();
        assert!(phi_before > 1.0);

        // Record a new heartbeat: resets the last_heartbeat timestamp.
        d.record_heartbeat();
        let phi_after = d.phi();
        assert!(
            phi_after < phi_before,
            "phi should drop after heartbeat: before={phi_before} after={phi_after}"
        );
    }

    #[test]
    fn test_is_available_below_threshold() {
        let d = PhiAccrualDetector::new(8.0, 200);
        for _ in 0..10 {
            d.record_heartbeat();
            thread::sleep(Duration::from_millis(10));
        }
        d.record_heartbeat(); // fresh heartbeat
        assert!(d.is_available());
    }

    #[test]
    fn test_suspected_above_threshold() {
        // Use a low threshold so we can trigger it quickly.
        let d = PhiAccrualDetector::new(1.0, 200);
        for _ in 0..20 {
            d.record_heartbeat();
            thread::sleep(Duration::from_millis(10));
        }
        // Sleep ~5× mean interval to push phi well above 1.0.
        thread::sleep(Duration::from_millis(150));
        assert!(
            !d.is_available(),
            "process should be suspected; phi={}",
            d.phi()
        );
    }

    #[test]
    fn test_window_size_capped() {
        let d = PhiAccrualDetector::new(8.0, 5);
        for _ in 0..20 {
            d.record_heartbeat();
            thread::sleep(Duration::from_millis(1));
        }
        // Window should cap at 5.
        assert!(d.sample_count() <= 5);
    }

    #[test]
    fn test_threshold_accessor() {
        let d = PhiAccrualDetector::new(12.5, 100);
        assert_eq!(d.threshold(), 12.5);
    }
}
