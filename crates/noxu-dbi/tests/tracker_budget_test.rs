//! DBI-24: env-open wires CLEANER_DETAIL_MAX_MEMORY_PERCENTAGE into the
//! UtilizationTracker's byte budget.
//!
//! JE: MemoryBudget.reset computes
//!   trackerBudget = cachePortion
//!                   * CLEANER_DETAIL_MAX_MEMORY_PERCENTAGE / 100
//! (DbConfigManager.getInt(CLEANER_DETAIL_MAX_MEMORY_PERCENTAGE)).
//!
//! This asserts the live tracker constructed at env-open carries the
//! derived budget, so its obsolete-offset detail is bounded.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use noxu_dbi::EnvironmentImpl;
use tempfile::TempDir;

#[test]
fn tracker_budget_derived_from_cache_size_and_percentage() {
    let dir = TempDir::new().unwrap();
    let env = EnvironmentImpl::new(
        dir.path(),
        /*read_only=*/ false,
        /*transactional=*/ true,
    )
    .expect("open environment");

    let tracker = env
        .get_utilization_tracker()
        .expect("transactional env has a live tracker");
    let budget = tracker.lock().get_tracker_budget();

    // Defaults: cache_size 64 MiB, CLEANER_DETAIL_MAX_MEMORY_PERCENTAGE 2.
    let expected = (64 * 1024 * 1024_i64) * 2 / 100;
    assert_eq!(
        budget, expected,
        "tracker budget must be cache_size * pct / 100 (JE trackerBudget)"
    );
    assert!(budget > 0, "default config yields a positive budget");
}
