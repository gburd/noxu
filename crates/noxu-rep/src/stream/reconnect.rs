//! Replica-side reconnection with exponential backoff.
//!
//! When a network partition or master crash disconnects the replica's
//! replication stream, the [`catch_up_with_retry`] function wraps the
//! existing `catch_up_from_peer` call in a retry loop with configurable
//! exponential backoff and jitter.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::error::RepError;
use crate::stream::peer_feeder::catch_up_from_peer;
use crate::stream::replica_stream::LogWriter;

/// Configuration for replica reconnection backoff.
#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    /// Initial backoff duration in milliseconds (default: 100).
    pub initial_backoff_ms: u64,
    /// Maximum backoff duration in milliseconds (default: 30_000).
    pub max_backoff_ms: u64,
    /// Multiplicative backoff factor (default: 2.0).
    pub backoff_factor: f64,
    /// Maximum number of retry attempts. 0 means unlimited (default: 0).
    pub max_retries: u32,
    /// Fraction of the current backoff to use as jitter range (default: 0.25).
    ///
    /// The actual sleep is `backoff +/- (backoff * jitter_fraction / 2)`.
    pub jitter_fraction: f64,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            initial_backoff_ms: 100,
            max_backoff_ms: 30_000,
            backoff_factor: 2.0,
            max_retries: 0,
            jitter_fraction: 0.25,
        }
    }
}

impl ReconnectConfig {
    /// Calculate the backoff duration for the given attempt number (0-indexed).
    ///
    /// Applies exponential growth capped at `max_backoff_ms`, then adds
    /// symmetric jitter within `+/- jitter_fraction/2` of the base value.
    pub fn next_backoff(&self, attempt: u32) -> Duration {
        let base = (self.initial_backoff_ms as f64)
            * self.backoff_factor.powi(attempt as i32);
        let capped = base.min(self.max_backoff_ms as f64);

        // Deterministic jitter using a simple hash of the attempt number.
        // Not cryptographic, but sufficient for desynchronizing retries.
        let jitter_seed = attempt.wrapping_mul(2654435761); // Knuth multiplicative hash
        let jitter_norm = (jitter_seed % 1000) as f64 / 1000.0; // 0.0..1.0
        let jitter_range = capped * self.jitter_fraction;
        let jitter = (jitter_norm - 0.5) * jitter_range;

        let ms = (capped + jitter).max(1.0) as u64;
        Duration::from_millis(ms)
    }
}

/// Outcome of a `catch_up_with_retry` call.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconnectOutcome {
    /// Successfully caught up from the peer.
    CaughtUp,
    /// Peer indicated a full restore is required (VLSN too old).
    NeedsRestore,
    /// Maximum retries exceeded without a successful connection.
    MaxRetriesExceeded,
    /// Shutdown was signalled before a successful connection.
    Shutdown,
}

/// Retry wrapper around [`catch_up_from_peer`].
///
/// Calls `catch_up_from_peer` in a loop. On connection failure, sleeps
/// according to [`ReconnectConfig`] backoff and retries. The loop exits when:
///
/// - The catch-up succeeds (`Ok(true)`) — returns [`ReconnectOutcome::CaughtUp`].
/// - The peer requires a full restore (`Ok(false)`) — returns [`ReconnectOutcome::NeedsRestore`].
/// - `max_retries` is exceeded (and non-zero) — returns [`ReconnectOutcome::MaxRetriesExceeded`].
/// - `shutdown` flag is set — returns [`ReconnectOutcome::Shutdown`].
pub fn catch_up_with_retry(
    peer_addr: SocketAddr,
    start_vlsn: u64,
    log_writer: &mut dyn LogWriter,
    config: &ReconnectConfig,
    shutdown: &Arc<AtomicBool>,
) -> ReconnectOutcome {
    let mut attempt: u32 = 0;

    loop {
        // Check shutdown before each attempt.
        if shutdown.load(Ordering::Acquire) {
            log::info!(
                "reconnect: shutdown signalled before attempt {}; exiting",
                attempt
            );
            return ReconnectOutcome::Shutdown;
        }

        match catch_up_from_peer(peer_addr, start_vlsn, log_writer) {
            Ok(true) => {
                if attempt > 0 {
                    log::info!(
                        "reconnect: successfully caught up from {} after {} retries",
                        peer_addr, attempt
                    );
                }
                return ReconnectOutcome::CaughtUp;
            }
            Ok(false) => {
                log::warn!(
                    "reconnect: peer {} requires full restore (VLSN {} too old)",
                    peer_addr, start_vlsn
                );
                return ReconnectOutcome::NeedsRestore;
            }
            Err(e) => {
                // Check if this is a non-retryable error.
                if !is_retryable(&e) {
                    log::error!(
                        "reconnect: non-retryable error from {}: {}",
                        peer_addr, e
                    );
                    // Treat non-retryable errors like max retries exceeded.
                    return ReconnectOutcome::MaxRetriesExceeded;
                }

                // Check max_retries limit.
                if config.max_retries > 0 && attempt >= config.max_retries {
                    log::warn!(
                        "reconnect: max retries ({}) exceeded for {}; last error: {}",
                        config.max_retries, peer_addr, e
                    );
                    return ReconnectOutcome::MaxRetriesExceeded;
                }

                let backoff = config.next_backoff(attempt);
                log::warn!(
                    "reconnect: attempt {} to {} failed ({}); retrying in {:?}",
                    attempt, peer_addr, e, backoff
                );

                // Sleep in small increments to allow shutdown detection.
                let sleep_end = std::time::Instant::now() + backoff;
                while std::time::Instant::now() < sleep_end {
                    if shutdown.load(Ordering::Acquire) {
                        log::info!("reconnect: shutdown signalled during backoff");
                        return ReconnectOutcome::Shutdown;
                    }
                    let remaining = sleep_end.saturating_duration_since(std::time::Instant::now());
                    std::thread::sleep(remaining.min(Duration::from_millis(100)));
                }

                attempt = attempt.saturating_add(1);
            }
        }
    }
}

/// Determine whether an error is retryable (transient network issue).
fn is_retryable(err: &RepError) -> bool {
    matches!(
        err,
        RepError::NetworkError(_) | RepError::ChannelClosed(_) | RepError::FrameCorrupted { .. }
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;

    // -----------------------------------------------------------------------
    // ReconnectConfig tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_default_config() {
        let cfg = ReconnectConfig::default();
        assert_eq!(cfg.initial_backoff_ms, 100);
        assert_eq!(cfg.max_backoff_ms, 30_000);
        assert_eq!(cfg.backoff_factor, 2.0);
        assert_eq!(cfg.max_retries, 0);
        assert_eq!(cfg.jitter_fraction, 0.25);
    }

    #[test]
    fn test_backoff_exponential_growth() {
        let cfg = ReconnectConfig {
            initial_backoff_ms: 100,
            max_backoff_ms: 60_000,
            backoff_factor: 2.0,
            max_retries: 0,
            jitter_fraction: 0.0, // no jitter for predictable test
        };

        // With zero jitter, backoff should be exactly exponential.
        let b0 = cfg.next_backoff(0);
        let b1 = cfg.next_backoff(1);
        let b2 = cfg.next_backoff(2);
        let b3 = cfg.next_backoff(3);

        assert_eq!(b0.as_millis(), 100);
        assert_eq!(b1.as_millis(), 200);
        assert_eq!(b2.as_millis(), 400);
        assert_eq!(b3.as_millis(), 800);
    }

    #[test]
    fn test_backoff_capped_at_max() {
        let cfg = ReconnectConfig {
            initial_backoff_ms: 1000,
            max_backoff_ms: 5000,
            backoff_factor: 3.0,
            max_retries: 0,
            jitter_fraction: 0.0,
        };

        // attempt 0: 1000, attempt 1: 3000, attempt 2: 9000 -> capped to 5000
        assert_eq!(cfg.next_backoff(0).as_millis(), 1000);
        assert_eq!(cfg.next_backoff(1).as_millis(), 3000);
        assert_eq!(cfg.next_backoff(2).as_millis(), 5000);
        assert_eq!(cfg.next_backoff(3).as_millis(), 5000);
    }

    #[test]
    fn test_backoff_with_jitter_bounded() {
        let cfg = ReconnectConfig {
            initial_backoff_ms: 1000,
            max_backoff_ms: 60_000,
            backoff_factor: 2.0,
            max_retries: 0,
            jitter_fraction: 0.5,
        };

        // With 50% jitter, backoff(0) should be in [750, 1250]
        let b = cfg.next_backoff(0).as_millis();
        assert!(b >= 750, "backoff {} < 750", b);
        assert!(b <= 1250, "backoff {} > 1250", b);
    }

    #[test]
    fn test_backoff_never_zero() {
        let cfg = ReconnectConfig {
            initial_backoff_ms: 1,
            max_backoff_ms: 1,
            backoff_factor: 1.0,
            max_retries: 0,
            jitter_fraction: 1.0, // maximum jitter
        };

        // Even with extreme jitter the backoff should be at least 1ms.
        for attempt in 0..20 {
            let b = cfg.next_backoff(attempt);
            assert!(b.as_millis() >= 1);
        }
    }

    // -----------------------------------------------------------------------
    // catch_up_with_retry tests (using mock channel infrastructure)
    // -----------------------------------------------------------------------

    #[test]
    fn test_shutdown_before_first_attempt() {
        struct NeverWriter;
        impl LogWriter for NeverWriter {
            fn write_entry(&mut self, _: u64, _: u8, _: &[u8]) -> Result<()> {
                panic!("should not be called");
            }
        }

        let shutdown = Arc::new(AtomicBool::new(true));
        let cfg = ReconnectConfig::default();
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();

        let outcome = catch_up_with_retry(addr, 0, &mut NeverWriter, &cfg, &shutdown);
        assert_eq!(outcome, ReconnectOutcome::Shutdown);
    }

    #[test]
    fn test_max_retries_exceeded() {
        struct NeverWriter;
        impl LogWriter for NeverWriter {
            fn write_entry(&mut self, _: u64, _: u8, _: &[u8]) -> Result<()> {
                Ok(())
            }
        }

        let shutdown = Arc::new(AtomicBool::new(false));
        let cfg = ReconnectConfig {
            initial_backoff_ms: 1,
            max_backoff_ms: 1,
            backoff_factor: 1.0,
            max_retries: 2,
            jitter_fraction: 0.0,
        };
        // Use an address that will fail to connect (nothing listening on port 1).
        let addr: SocketAddr = "127.0.0.1:1".parse().unwrap();

        let outcome = catch_up_with_retry(addr, 0, &mut NeverWriter, &cfg, &shutdown);
        assert_eq!(outcome, ReconnectOutcome::MaxRetriesExceeded);
    }

    #[test]
    fn test_is_retryable_network_error() {
        assert!(is_retryable(&RepError::NetworkError("timeout".into())));
        assert!(is_retryable(&RepError::ChannelClosed("gone".into())));
        assert!(is_retryable(&RepError::FrameCorrupted {
            vlsn: 1,
            expected: 0,
            actual: 1,
        }));
        assert!(!is_retryable(&RepError::ProtocolError("bad".into())));
        assert!(!is_retryable(&RepError::DatabaseError("disk".into())));
    }
}
