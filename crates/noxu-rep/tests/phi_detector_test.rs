//! Tests for `PhiAccrualDetector` and its integration with `MasterTracker`.

use std::thread;
use std::time::Duration;

use noxu_rep::PhiAccrualDetector;
use noxu_rep::elections::MasterTracker;

// ---------------------------------------------------------------------------
// PhiAccrualDetector unit tests
// ---------------------------------------------------------------------------

#[test]
fn test_phi_zero_with_insufficient_samples() {
    // With fewer than 2 inter-arrival samples the detector returns phi=0.0
    // and considers the process available.
    let det = PhiAccrualDetector::new(8.0, 200);
    assert_eq!(det.phi(), 0.0, "phi must be 0.0 with no samples");
    assert!(det.is_available(), "must be available with no samples");

    // One heartbeat recorded but still only 0 inter-arrival samples.
    det.record_heartbeat();
    assert_eq!(det.phi(), 0.0, "phi must be 0.0 after first heartbeat (no interval yet)");
    assert!(det.is_available());
}

#[test]
fn test_phi_rises_over_time_without_heartbeat() {
    // Populate the window with regular 10 ms heartbeats.
    let det = PhiAccrualDetector::new(8.0, 200);
    for _ in 0..20 {
        det.record_heartbeat();
        thread::sleep(Duration::from_millis(10));
    }
    // Record a fresh heartbeat so last_heartbeat is current regardless of
    // OS scheduling jitter during the loop's final sleep.
    det.record_heartbeat();
    // After the heartbeats stop, phi should be zero or low immediately.
    let phi_now = det.phi();
    assert!(
        phi_now < 8.0,
        "phi must be below threshold right after last heartbeat (got {phi_now:.4})"
    );

    // Wait 3× the mean interval (≈ 30 ms) and check that phi has risen.
    thread::sleep(Duration::from_millis(80));
    let phi_later = det.phi();
    assert!(
        phi_later > phi_now,
        "phi must rise over time without heartbeats (before={phi_now:.4} after={phi_later:.4})"
    );
}

#[test]
fn test_phi_resets_after_heartbeat() {
    let det = PhiAccrualDetector::new(8.0, 200);
    for _ in 0..20 {
        det.record_heartbeat();
        thread::sleep(Duration::from_millis(10));
    }
    // Let phi build up.
    thread::sleep(Duration::from_millis(50));
    let phi_before = det.phi();

    // A new heartbeat resets the last-seen time so phi drops immediately.
    det.record_heartbeat();
    let phi_after = det.phi();
    assert!(
        phi_after < phi_before,
        "phi must drop after a heartbeat (before={phi_before:.4} after={phi_after:.4})"
    );
}

#[test]
fn test_is_available_below_threshold() {
    let threshold = 8.0_f64;
    let det = PhiAccrualDetector::new(threshold, 200);
    assert_eq!(det.threshold(), threshold);

    // Populate the window then check immediately — phi should be well below 8.0.
    for _ in 0..20 {
        det.record_heartbeat();
        thread::sleep(Duration::from_millis(10));
    }
    det.record_heartbeat(); // final heartbeat
    assert!(
        det.is_available(),
        "must be available immediately after last heartbeat"
    );
}

#[test]
fn test_suspected_above_threshold() {
    // Use a very low threshold (1.0) so we can reliably cross it in a short
    // test without very long sleeps.
    let det = PhiAccrualDetector::new(1.0, 50);

    // Establish a mean of 10 ms.
    for _ in 0..25 {
        det.record_heartbeat();
        thread::sleep(Duration::from_millis(10));
    }

    // Wait 150 ms (≈ 15× mean) — phi should far exceed 1.0.
    thread::sleep(Duration::from_millis(150));
    let phi = det.phi();
    assert!(
        phi >= 1.0,
        "phi must exceed threshold=1.0 after 150 ms silence (got {phi:.4})"
    );
    assert!(
        !det.is_available(),
        "process must be suspected after 150 ms silence"
    );
}

// ---------------------------------------------------------------------------
// MasterTracker + phi integration
// ---------------------------------------------------------------------------

#[test]
fn test_master_tracker_phi_mode() {
    let det = PhiAccrualDetector::new(1.0, 50);
    let tracker = MasterTracker::new(Duration::from_secs(10)).with_phi(det);

    tracker.set_master("node1", 1);

    // Send heartbeats to populate the phi detector's window.
    for _ in 0..20 {
        tracker.record_heartbeat();
        thread::sleep(Duration::from_millis(10));
    }

    // Immediately after heartbeats: should be alive.
    assert!(tracker.is_master_alive(), "master must be alive right after heartbeats");

    // Silence for 200 ms → phi rises above 1.0 → suspected.
    thread::sleep(Duration::from_millis(200));
    assert!(
        !tracker.is_master_alive(),
        "master must be suspected after extended silence with phi detector"
    );

    // phi() accessor returns Some(phi_value) in phi mode.
    let phi_val = tracker.phi();
    assert!(
        phi_val.is_some(),
        "tracker.phi() must return Some(...) in phi mode"
    );
    assert!(
        phi_val.unwrap() >= 1.0,
        "phi value must exceed threshold after silence"
    );
}

#[test]
fn test_master_tracker_timeout_mode_unchanged() {
    // Without a phi detector, the tracker falls back to binary heartbeat timeout.
    let timeout = Duration::from_millis(100);
    let tracker = MasterTracker::new(timeout);

    tracker.set_master("node2", 1);
    tracker.record_heartbeat();

    // Immediately alive.
    assert!(tracker.is_master_alive(), "must be alive right after heartbeat");

    // phi() returns None in timeout mode.
    assert_eq!(tracker.phi(), None, "phi() must be None in binary timeout mode");

    // After 150 ms (> timeout), master should be considered dead.
    thread::sleep(Duration::from_millis(150));
    assert!(
        !tracker.is_master_alive(),
        "master must be dead after timeout elapses"
    );
}
