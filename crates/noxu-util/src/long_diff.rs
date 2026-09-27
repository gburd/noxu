//! `LongDiffStat`: difference of a base long stat against a set value.
//!
//! Port of JE's `LongDiffStat`, a stat component that computes the difference
//! between a base stat value and a specified value, reporting 0 when the
//! specified value exceeds the base.  The computed difference remains valid for
//! a specified time window; once that window elapses without an update, the
//! difference is recomputed from the current base value and the last specified
//! value (so a stat that stops updating is shown as falling behind).  In JE
//! this backs the replication `Feeder` lag stats.
//!
//! JE ref: `com.sleepycat.je.utilint.LongDiffStat`.
//!
//! Faithful reference port kept for test parity with JE; the eventual wiring
//! into Feeder-lag stats is tracked as a follow-up.

use crate::moving_avg::AtomicLongComponent;
use std::sync::Arc;

/// A long stat component that reports `max(base - prevValue, 0)`, where the
/// difference is treated as valid for `validity_millis` after each update.
#[derive(Debug, Clone)]
pub struct LongDiffStat {
    /// The base stat supplying the current value for computing differences.
    base: Arc<AtomicLongComponent>,
    /// The time (ms) a computed difference remains valid.
    validity_millis: i64,
    prev_value: i64,
    prev_time: i64,
    diff: i64,
}

impl LongDiffStat {
    /// Creates an instance using `base` as the difference source and a
    /// `validity_millis` window.
    pub fn new(base: Arc<AtomicLongComponent>, validity_millis: i64) -> Self {
        debug_assert!(validity_millis > 0);
        LongDiffStat {
            base,
            validity_millis,
            prev_value: 0,
            prev_time: 0,
            diff: 0,
        }
    }

    /// Returns the value of the stat for the specified time.
    ///
    /// Within the validity window, the cached difference is returned; past it,
    /// the difference is recomputed from the current base value.
    pub fn get_at(&self, time: i64) -> i64 {
        debug_assert!(time > 0);
        if self.prev_time == 0 {
            return 0;
        }
        if time < (self.prev_time + self.validity_millis) {
            return self.diff;
        }
        let base_value = self.base.get();
        (base_value - self.prev_value).max(0)
    }

    /// Specifies a new value for the specified time, snapshotting the current
    /// difference against the base.
    pub fn set_at(&mut self, new_value: i64, time: i64) {
        debug_assert!(time > 0);
        let base_value = self.base.get();
        self.prev_value = new_value;
        self.prev_time = time;
        self.diff = (base_value - new_value).max(0);
    }

    /// Clears the stat back to its unset state.
    pub fn clear(&mut self) {
        self.prev_value = 0;
        self.prev_time = 0;
        self.diff = 0;
    }

    /// Returns an independent copy (the base is shared via `Arc`, matching JE
    /// where the base stat's `copy()` is a live handle in the tests).
    pub fn copy(&self) -> LongDiffStat {
        LongDiffStat {
            base: Arc::clone(&self.base),
            validity_millis: self.validity_millis,
            prev_value: self.prev_value,
            prev_time: self.prev_time,
            diff: self.diff,
        }
    }

    /// Formats the value at `time`: `"Unknown"` when unset, else grouped or
    /// plain per `use_commas`.
    pub fn formatted_value_at(&self, time: i64, use_commas: bool) -> String {
        if self.is_not_set() {
            return "Unknown".to_string();
        }
        let v = self.get_at(time);
        if use_commas {
            crate::moving_avg::format_grouped_i64_pub(v)
        } else {
            v.to_string()
        }
    }

    /// True until the first value is set.
    pub fn is_not_set(&self) -> bool {
        self.prev_time == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_at(v: i64) -> Arc<AtomicLongComponent> {
        let b = Arc::new(AtomicLongComponent::new());
        b.set(v);
        b
    }

    // JE: LongDiffStatTest.testGet
    #[test]
    fn test_get() {
        let base = base_at(1000);
        let mut stat = LongDiffStat::new(Arc::clone(&base), 3000);
        assert_eq!(stat.get_at(1000), 0);
        stat.set_at(300, 1000);
        base.set(2000);
        assert_eq!(stat.get_at(2000), 700);
        assert_eq!(stat.get_at(5000), 1700);
        stat.set_at(3000, 6000);
        assert_eq!(stat.get_at(7000), 0);
    }

    // JE: LongDiffStatTest.testClear
    #[test]
    fn test_clear() {
        let base = base_at(1000);
        let mut stat = LongDiffStat::new(base, 3000);
        stat.set_at(10, 1000);
        assert_eq!(stat.get_at(1000), 990);
        assert!(!stat.is_not_set());
        stat.clear();
        assert_eq!(stat.get_at(1000), 0);
        assert!(stat.is_not_set());
    }

    // JE: LongDiffStatTest.testCopy
    #[test]
    fn test_copy() {
        let base = base_at(1000);
        let mut stat = LongDiffStat::new(Arc::clone(&base), 3000);
        stat.set_at(300, 1000);
        let mut copy = stat.copy();
        stat.set_at(350, 2000);
        base.set(2000);
        assert_eq!(copy.get_at(1000), 700);
        copy.set_at(400, 3000);
        assert_eq!(stat.get_at(3000), 650);
    }

    // JE: LongDiffStatTest.testGetFormattedValue
    #[test]
    fn test_get_formatted_value() {
        let base = base_at(123456790);
        let mut stat = LongDiffStat::new(base, 3000);
        // JE uses System.currentTimeMillis(); we use a fixed in-window time so
        // the cached diff is returned deterministically.
        stat.set_at(1, 1_000_000);
        assert_eq!(stat.formatted_value_at(1_000_000, true), "123,456,789");
        assert_eq!(stat.formatted_value_at(1_000_000, false), "123456789");
    }

    // JE: LongDiffStatTest.testIsNotSet
    #[test]
    fn test_is_not_set() {
        let base = base_at(1000);
        let mut stat = LongDiffStat::new(base, 3000);
        assert!(stat.is_not_set());
        stat.set_at(200, 1000);
        assert!(!stat.is_not_set());
        stat.clear();
        assert!(stat.is_not_set());
    }
}
