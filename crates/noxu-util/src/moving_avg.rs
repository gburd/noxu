//! Exponential-moving-average stat components.
//!
//! Port of JE's `DoubleExpMovingAvg` and `LongAvgRate` (and the
//! `AtomicLongComponent` scalar stat), the self-contained averaging
//! primitives JE uses for irregularly-sampled rate statistics (in JE these
//! back the replication `Feeder`'s replay/ack-rate stats).  These are pure
//! arithmetic utilities with no engine coupling, so they are ported here
//! faithfully — same EWMA formula, same MIN_PERIOD gating, same rounding and
//! time-unit conversion — together with JE's exact test vectors.
//!
//! JE ref: `com.sleepycat.je.utilint.DoubleExpMovingAvg`,
//! `com.sleepycat.je.utilint.LongAvgRate`,
//! `com.sleepycat.je.utilint.AtomicLongComponent`.

use std::sync::atomic::{AtomicI64, Ordering};

/// A double stat component: an exponential moving average over a specified
/// time period of values supplied with associated times, to support averaging
/// values generated at irregular intervals.
///
/// JE ref: `DoubleExpMovingAvg`.
#[derive(Debug, Clone)]
pub struct DoubleExpMovingAvg {
    name: String,
    /// The averaging period in milliseconds.
    period_millis: i64,
    /// The time (ms) of the previous value, or 0 if none have been provided.
    prev_time: i64,
    /// The current average, or 0 if no values have been provided.
    avg: f64,
}

impl DoubleExpMovingAvg {
    /// Creates an instance averaging over `period_millis` milliseconds.
    pub fn new(name: impl Into<String>, period_millis: i64) -> Self {
        let name = name.into();
        debug_assert!(period_millis > 0);
        DoubleExpMovingAvg { name, period_millis, prev_time: 0, avg: 0.0 }
    }

    /// Returns the name of this stat.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Adds a new value to the average, ignoring values that are not newer
    /// than the time of the previous call.
    pub fn add(&mut self, value: f64, time: i64) {
        debug_assert!(time > 0);
        if time <= self.prev_time {
            return;
        }
        if self.prev_time == 0 {
            self.avg = value;
        } else {
            // Exponential moving average.  See the Wikipedia "Application to
            // measuring computer performance" section referenced by JE.
            let m = (-((time - self.prev_time) as f64)
                / (self.period_millis as f64))
                .exp();
            self.avg = ((1.0 - m) * value) + (m * self.avg);
        }
        self.prev_time = time;
    }

    /// Adds the values from another average.
    pub fn add_avg(&mut self, other: &DoubleExpMovingAvg) {
        if other.is_not_set() {
            return;
        }
        self.add(other.avg, other.prev_time);
    }

    /// Returns the current average, or 0 if no values have been added.
    pub fn get(&self) -> f64 {
        self.avg
    }

    /// Clears the average back to its unset state.
    pub fn clear(&mut self) {
        self.prev_time = 0;
        self.avg = 0.0;
    }

    /// Formats the value: `"unknown"` when unset, `"NaN"` when NaN, otherwise
    /// grouped (thousands separators) when `use_commas`, else two decimals.
    pub fn formatted_value(&self, use_commas: bool) -> String {
        if self.is_not_set() {
            "unknown".to_string()
        } else if self.avg.is_nan() {
            "NaN".to_string()
        } else if use_commas {
            format_grouped_f64(self.avg)
        } else {
            format!("{:.2}", self.avg)
        }
    }

    /// True until the first value is added.
    pub fn is_not_set(&self) -> bool {
        self.prev_time == 0
    }
}

/// A long stat component: an exponential moving average of the rate of change
/// in a long value over time, reported in a chosen time unit.
///
/// JE ref: `LongAvgRate`.
#[derive(Debug, Clone)]
pub struct LongAvgRate {
    /// The averaged rate values (rate is in units per millisecond).
    avg: DoubleExpMovingAvg,
    report_time_unit: TimeUnit,
    prev_value: i64,
    prev_time: i64,
}

/// The time unit for reporting a [`LongAvgRate`], matching the subset of
/// `java.util.concurrent.TimeUnit` JE uses here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeUnit {
    /// Nanoseconds.
    Nanoseconds,
    /// Microseconds.
    Microseconds,
    /// Milliseconds (the base unit the average is computed in).
    Milliseconds,
    /// Seconds.
    Seconds,
    /// Minutes.
    Minutes,
}

impl TimeUnit {
    /// Number of milliseconds in one of this unit (for units >= millisecond).
    fn to_millis(self) -> i64 {
        match self {
            TimeUnit::Milliseconds => 1,
            TimeUnit::Seconds => 1_000,
            TimeUnit::Minutes => 60_000,
            // Sub-millisecond units are handled separately.
            TimeUnit::Microseconds | TimeUnit::Nanoseconds => 1,
        }
    }

    /// For sub-millisecond units, how many of this unit fit in one
    /// millisecond (JE: `reportTimeUnit.convert(1, MILLISECONDS)`).
    fn per_millis(self) -> i64 {
        match self {
            TimeUnit::Nanoseconds => 1_000_000,
            TimeUnit::Microseconds => 1_000,
            _ => 1,
        }
    }

    fn is_sub_millis(self) -> bool {
        matches!(self, TimeUnit::Nanoseconds | TimeUnit::Microseconds)
    }
}

impl LongAvgRate {
    /// The minimum number of milliseconds for computing rate changes, to avoid
    /// quantizing errors.
    pub const MIN_PERIOD: i64 = 200;

    /// Creates an instance averaging over `period_millis` and reporting in
    /// `report_time_unit`.
    pub fn new(
        name: impl Into<String>,
        period_millis: i64,
        report_time_unit: TimeUnit,
    ) -> Self {
        LongAvgRate {
            avg: DoubleExpMovingAvg::new(name, period_millis),
            report_time_unit,
            prev_value: 0,
            prev_time: 0,
        }
    }

    /// Returns the name of this stat.
    pub fn name(&self) -> &str {
        self.avg.name()
    }

    /// Adds a new value to the average, ignoring values that are less than
    /// [`Self::MIN_PERIOD`] milliseconds newer than the last entry.
    pub fn add(&mut self, value: i64, time: i64) {
        debug_assert!(time > 0);
        if self.prev_time != 0 {
            let delta_time = time - self.prev_time;
            if delta_time < Self::MIN_PERIOD {
                return;
            }
            self.avg.add(
                (value - self.prev_value) as f64 / delta_time as f64,
                time,
            );
        }
        self.prev_value = value;
        self.prev_time = time;
    }

    /// Updates with more recent values from another stat (only if the other is
    /// newer by more than [`Self::MIN_PERIOD`]).
    pub fn add_rate(&mut self, other: &LongAvgRate) {
        self.add_internal(other);
    }

    fn add_internal(&mut self, other: &LongAvgRate) {
        // Only use the other values if they are newer by more than the minimum.
        if (other.prev_time - self.prev_time) > Self::MIN_PERIOD {
            self.avg.add_avg(&other.avg);
            self.prev_value = other.prev_value;
            self.prev_time = other.prev_time;
        }
    }

    /// Creates and returns a new stat that includes the most recent values from
    /// this stat and another stat.
    pub fn copy_latest(&self, other: &LongAvgRate) -> LongAvgRate {
        let mut other_copy = other.clone();
        if self.prev_time > other_copy.prev_time {
            other_copy.add_internal(self);
            other_copy
        } else {
            let mut result = self.clone();
            result.add_internal(&other_copy);
            result
        }
    }

    /// Returns the current average rate (rounded, in the report time unit), or
    /// 0 if no rate has been computed.
    pub fn get(&self) -> i64 {
        let in_millis = self.avg.get();
        if self.report_time_unit == TimeUnit::Milliseconds {
            round_half_up(in_millis)
        } else if self.report_time_unit.is_sub_millis() {
            round_half_up(in_millis / self.report_time_unit.per_millis() as f64)
        } else {
            round_half_up(in_millis * self.report_time_unit.to_millis() as f64)
        }
    }

    /// Clears the stat back to its unset state.
    pub fn clear(&mut self) {
        self.avg.clear();
        self.prev_value = 0;
        self.prev_time = 0;
    }

    /// Formats the value: `"unknown"` when unset, else grouped or plain.
    pub fn formatted_value(&self, use_commas: bool) -> String {
        if self.is_not_set() {
            return "unknown".to_string();
        }
        let val = self.get();
        if use_commas { format_grouped_i64(val) } else { val.to_string() }
    }

    /// True until enough values have been added to compute an average.
    pub fn is_not_set(&self) -> bool {
        self.avg.is_not_set()
    }
}

/// A scalar stat component backed by an atomic long.
///
/// JE ref: `AtomicLongComponent`.
#[derive(Debug)]
pub struct AtomicLongComponent {
    val: AtomicI64,
}

impl Default for AtomicLongComponent {
    fn default() -> Self {
        Self::new()
    }
}

impl AtomicLongComponent {
    /// Creates a zero-valued component.
    pub fn new() -> Self {
        AtomicLongComponent { val: AtomicI64::new(0) }
    }

    /// Sets the stat to the specified value.
    pub fn set(&self, new_value: i64) {
        self.val.store(new_value, Ordering::SeqCst);
    }

    /// Adds the specified value.
    pub fn add(&self, inc: i64) {
        self.val.fetch_add(inc, Ordering::SeqCst);
    }

    /// Returns the current value.
    pub fn get(&self) -> i64 {
        self.val.load(Ordering::SeqCst)
    }

    /// Resets the value to zero.
    pub fn clear(&self) {
        self.val.store(0, Ordering::SeqCst);
    }

    /// Returns an independent copy.
    pub fn copy(&self) -> AtomicLongComponent {
        AtomicLongComponent { val: AtomicI64::new(self.get()) }
    }

    /// Formats the value: grouped (thousands separators) when `use_commas`,
    /// else plain.
    pub fn formatted_value(&self, use_commas: bool) -> String {
        if use_commas {
            format_grouped_i64(self.get())
        } else {
            self.get().to_string()
        }
    }

    /// True when the value is zero.
    pub fn is_not_set(&self) -> bool {
        self.get() == 0
    }
}

impl std::fmt::Display for AtomicLongComponent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.get())
    }
}

/// Rounds a double to the nearest long, rounding halves up toward positive
/// infinity — matching Java's `Math.round`.
fn round_half_up(x: f64) -> i64 {
    (x + 0.5).floor() as i64
}

/// Crate-internal accessor for the grouped-integer formatter, reused by
/// [`crate::long_diff::LongDiffStat`].
pub(crate) fn format_grouped_i64_pub(v: i64) -> String {
    format_grouped_i64(v)
}

/// Formats an integer with thousands separators (matching Java's grouping
/// `DecimalFormat`, e.g. `123456789 -> "123,456,789"`).
fn format_grouped_i64(v: i64) -> String {
    let neg = v < 0;
    let digits = v.unsigned_abs().to_string();
    let bytes = digits.as_bytes();
    let mut out = String::new();
    let len = bytes.len();
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 && (len - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(*b as char);
    }
    if neg { format!("-{out}") } else { out }
}

/// Formats a double with thousands separators on the integer part and no
/// fractional digits when the value is integral, matching JE's
/// `DecimalFormat("###,###,...###.##")` for the integer-valued cases the tests
/// exercise (e.g. `10000.0 -> "10,000"`).
fn format_grouped_f64(v: f64) -> String {
    // JE's pattern uses "##" for the fraction, which drops trailing zeros and
    // rounds to at most two decimals.
    let rounded = (v * 100.0).round() / 100.0;
    if rounded.fract() == 0.0 {
        format_grouped_i64(rounded as i64)
    } else {
        let int_part = rounded.trunc() as i64;
        let frac = ((rounded.abs().fract()) * 100.0).round() as i64;
        let frac_str = if frac % 10 == 0 {
            format!("{}", frac / 10)
        } else {
            format!("{frac:02}")
        };
        format!("{}.{}", format_grouped_i64(int_part), frac_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use TimeUnit::*;

    // ---- DoubleExpMovingAvg ----

    // JE: DoubleExpMovingAvgTest.testConstructorPeriodMillis
    #[test]
    fn double_exp_constructor_period_millis() {
        let mut avg = DoubleExpMovingAvg::new("stat", 3000);
        avg.add(1.0, 1000);
        avg.add(2.0, 2000);
        avg.add(4.0, 3000);
        avg.add(8.0, 4000);
        assert!((avg.get() - 3.7).abs() <= 0.1, "got {}", avg.get());

        // Shorter period skews result towards later entries.
        let mut avg = DoubleExpMovingAvg::new("stat", 2000);
        avg.add(1.0, 1000);
        avg.add(2.0, 2000);
        avg.add(4.0, 3000);
        avg.add(8.0, 4000);
        assert!((avg.get() - 4.6).abs() <= 0.1, "got {}", avg.get());
    }

    // JE: DoubleExpMovingAvgTest.testCopyConstructor
    #[test]
    fn double_exp_copy_constructor() {
        let mut avg = DoubleExpMovingAvg::new("stat", 3000);
        avg.add(2.0, 1000);
        assert_eq!(avg.get(), 2.0);
        let mut copy = avg.clone();
        assert_eq!(avg.get(), copy.get());
        copy.add(4.0, 2000);
        assert_eq!(avg.get(), 2.0);
        assert!((copy.get() - 2.5).abs() <= 0.1, "got {}", copy.get());
    }

    // JE: DoubleExpMovingAvgTest.testGetAndAdd
    #[test]
    fn double_exp_get_and_add() {
        let mut avg = DoubleExpMovingAvg::new("stat", 3000);
        assert_eq!(avg.get(), 0.0);

        avg.add(1.0, 1000);
        assert_eq!(avg.get(), 1.0);
        avg.add(4.2, 2000);
        assert!((avg.get() - 2.0).abs() <= 0.1);
        avg.add(5.5, 3000);
        assert!((avg.get() - 3.0).abs() <= 0.1);
        avg.add(3.0, 4000);
        assert!((avg.get() - 3.0).abs() <= 0.1);
        avg.add(-0.3, 5000);
        assert!((avg.get() - 2.0).abs() <= 0.1);
        avg.add(-1.3, 6000);
        assert!((avg.get() - 1.0).abs() <= 0.1);
        avg.add(-2.4, 7000);
        assert!((avg.get() - 0.0).abs() <= 0.1);
        avg.add(0.0, 8000);
        assert!((avg.get() - 0.0).abs() <= 0.1);

        // Ignore items at same and earlier times.
        avg.add(123.0, 8000);
        avg.add(456.0, 2000);
        assert!((avg.get() - 0.0).abs() <= 0.1);
    }

    // JE: DoubleExpMovingAvgTest.testGetFormattedValue
    #[test]
    fn double_exp_get_formatted_value() {
        let mut avg = DoubleExpMovingAvg::new("stat", 3000);
        assert_eq!(avg.formatted_value(true), "unknown");
        avg.add(10000.0, 1000);
        assert_eq!(avg.formatted_value(true), "10,000");
        assert_eq!(avg.formatted_value(false), "10000.00");

        avg.add(f64::NAN, 2000);
        assert_eq!(avg.formatted_value(true), "NaN");
    }

    // JE: DoubleExpMovingAvgTest.testIsNotSet
    #[test]
    fn double_exp_is_not_set() {
        let mut avg = DoubleExpMovingAvg::new("stat", 3000);
        assert!(avg.is_not_set());
        avg.add(1.0, 1000);
        assert!(!avg.is_not_set());
        avg.add(2.0, 2000);
        assert!(!avg.is_not_set());
    }

    // ---- LongAvgRate ----

    // JE: LongAvgRateTest.testConstructorPeriodMillis
    #[test]
    fn long_rate_constructor_period_millis() {
        let mut avg = LongAvgRate::new("stat", 3000, Milliseconds);
        avg.add(1000, 1000);
        avg.add(2000, 2000);
        avg.add(4000, 3000);
        avg.add(8000, 4000);
        assert_eq!(avg.get(), 2);

        let mut avg = LongAvgRate::new("stat", 2000, Milliseconds);
        avg.add(1000, 1000);
        avg.add(2000, 2000);
        avg.add(4000, 3000);
        avg.add(8000, 4000);
        assert_eq!(avg.get(), 2);
    }

    // JE: LongAvgRateTest.testConstructorReportTimeUnit
    #[test]
    fn long_rate_constructor_report_time_unit() {
        let mut avg = LongAvgRate::new("stat", 3000, Nanoseconds);
        avg.add(2_000_000_000, 1000);
        avg.add(4_000_000_000, 2000);
        assert_eq!(avg.get(), 2);

        let mut avg = LongAvgRate::new("stat", 3000, Microseconds);
        avg.add(2_000_000_000, 1000);
        avg.add(4_000_000_000, 2000);
        assert_eq!(avg.get(), 2000);

        let mut avg = LongAvgRate::new("stat", 3000, Milliseconds);
        avg.add(2_000_000_000, 1000);
        avg.add(4_000_000_000, 2000);
        assert_eq!(avg.get(), 2_000_000);

        let mut avg = LongAvgRate::new("stat", 3000, Seconds);
        avg.add(2_000_000_000, 1000);
        avg.add(4_000_000_000, 2000);
        assert_eq!(avg.get(), 2_000_000_000);
    }

    // JE: LongAvgRateTest.testMinPeriod
    #[test]
    fn long_rate_min_period() {
        let mut avg = LongAvgRate::new("stat", 3000, Milliseconds);
        avg.add(2000, 1000);
        // Entry with time delta less than 200 ms is ignored.
        avg.add(3000, 1100);
        assert_eq!(avg.get(), 0);
        // Computes back to the initial entry.
        avg.add(4000, 2000);
        assert_eq!(avg.get(), 2);
    }

    // JE: LongAvgRateTest.testAdd
    #[test]
    fn long_rate_add() {
        let mut avg = LongAvgRate::new("stat", 3000, Milliseconds);
        assert_eq!(avg.get(), 0);
        avg.add(2000, 1000);
        assert_eq!(avg.get(), 0);
        // Second value prior to MIN_PERIOD is ignored.
        avg.add(7700, 1100);
        assert_eq!(avg.get(), 0);
        avg.add(4000, 2000);
        avg.add(7700, 2100);
        assert_eq!(avg.get(), 2);
        avg.add(8000, 3000);
        avg.add(17700, 3100);
        assert_eq!(avg.get(), 3);
    }

    // JE: LongAvgRateTest.testAddAverage
    #[test]
    fn long_rate_add_average() {
        let mut avg = LongAvgRate::new("stat", 3000, Milliseconds);
        let mut other = LongAvgRate::new("stat", 3000, Seconds);
        avg.add_rate(&other);
        assert!(avg.is_not_set(), "Add empty on empty has no effect");

        avg.add(3000, 1000);
        avg.add(6000, 2000);
        avg.add_rate(&other);
        assert_eq!(avg.get(), 3, "Add empty has no effect");

        other.add(6000, 1000);
        other.add(12000, 2000);
        avg.add_rate(&other);
        assert_eq!(avg.get(), 3, "Add older has no effect");

        other.clear();
        other.add(6000, 3000);
        other.add(12000, 4000);
        avg.add_rate(&other);
        assert_eq!(avg.get(), 4, "Add newer has effect");

        avg.clear();
        avg.add_rate(&other);
        assert_eq!(avg.get(), 6, "Add to empty");
    }

    // JE: LongAvgRateTest.testCopyLatest
    // Also covers LongAvgRateStatTest.testComputeInterval: JE
    // LongAvgRateStat.computeInterval(base) == avg.copyLatest(baseAvg)
    // (LongAvgRateStat.java:108-112), identical vectors (3, 20, 13).
    #[test]
    fn long_rate_copy_latest() {
        let mut avg = LongAvgRate::new("stat", 3000, Milliseconds);
        let mut other = LongAvgRate::new("stat", 3000, Milliseconds);

        let latest = avg.copy_latest(&other);
        assert!(latest.is_not_set());

        avg.add(0, 1000);
        avg.add(3000, 2000);
        let latest = avg.copy_latest(&other);
        assert_eq!(latest.get(), 3);
        let latest = other.copy_latest(&avg);
        assert_eq!(latest.get(), 3);

        // The later rate is 30, so the result is closer to that.
        other.add(10000, 4000);
        other.add(40000, 5000);
        let latest = avg.copy_latest(&other);
        assert_eq!(latest.get(), 20);
        let latest = other.copy_latest(&avg);
        assert_eq!(latest.get(), 20);

        // The later rate is 3, so the result is smaller.
        avg.clear();
        other.clear();
        avg.add(10000, 1000);
        avg.add(40000, 2000);
        other.add(0, 4000);
        other.add(3000, 5000);
        let latest = avg.copy_latest(&other);
        assert_eq!(latest.get(), 13);
        let latest = other.copy_latest(&avg);
        assert_eq!(latest.get(), 13);
    }

    // JE: LongAvgRateTest.testClear
    #[test]
    fn long_rate_clear() {
        let mut avg = LongAvgRate::new("stat", 3000, Milliseconds);
        avg.add(3, 1000);
        avg.add(6, 2000);
        avg.clear();
        assert_eq!(avg.get(), 0);
        assert!(avg.is_not_set());
    }

    // JE: LongAvgRateTest.testCopy
    // Also covers LongAvgRateStatTest.testCopy: JE LongAvgRateStat.copy()
    // == avg.copy() (LongAvgRateStat.java:75-77), same underlying arithmetic.
    #[test]
    fn long_rate_copy() {
        let mut avg = LongAvgRate::new("stat", 3000, Milliseconds);
        avg.add(3000, 1000);
        avg.add(6000, 2000);
        let mut copy = avg.clone();
        assert_eq!(avg.name(), copy.name());
        avg.add(12000, 3000);
        copy.add(24000, 3000);
        assert_eq!(avg.get(), 4);
        assert_eq!(copy.get(), 7);
    }

    // JE: LongAvgRateTest.testGetFormattedValue
    #[test]
    fn long_rate_get_formatted_value() {
        let mut avg = LongAvgRate::new("stat", 3000, Microseconds);
        assert_eq!(avg.formatted_value(true), "unknown");
        avg.add(0, 1000);
        avg.add(987698769, 2000);
        assert_eq!(avg.formatted_value(true), "988");
        assert_eq!(avg.formatted_value(false), "988");

        let mut avg = LongAvgRate::new("stat", 3000, Milliseconds);
        assert_eq!(avg.formatted_value(true), "unknown");
        avg.add(0, 1000);
        avg.add(987698769, 2000);
        assert_eq!(avg.formatted_value(true), "987,699");
        assert_eq!(avg.formatted_value(false), "987699");

        let mut avg = LongAvgRate::new("stat", 3000, Seconds);
        assert_eq!(avg.formatted_value(true), "unknown");
        avg.add(0, 1000);
        avg.add(987698769, 2000);
        assert_eq!(avg.formatted_value(true), "987,698,769");
        assert_eq!(avg.formatted_value(false), "987698769");
    }

    // JE: LongAvgRateTest.testIsNotSet
    #[test]
    fn long_rate_is_not_set() {
        let mut avg = LongAvgRate::new("stat", 3000, Milliseconds);
        assert!(avg.is_not_set());
        avg.add(0, 1000);
        assert!(avg.is_not_set());
        avg.add(2000, 2000);
        assert!(!avg.is_not_set());
        avg.add(4000, 3000);
        assert!(!avg.is_not_set());
    }

    // JE: LongAvgRateTest.testAddAverageNullArg — N/A: Rust references cannot
    // be null; `add_rate` takes `&LongAvgRate`, so the NPE case is
    // unrepresentable (language/API deviation).

    // ---- AtomicLongComponent ----

    // JE: AtomicLongComponentTest.testConstructor
    #[test]
    fn atomic_long_constructor() {
        let comp = AtomicLongComponent::new();
        assert_eq!(comp.get(), 0);
    }

    // JE: AtomicLongComponentTest.testSet
    #[test]
    fn atomic_long_set() {
        let comp = AtomicLongComponent::new();
        comp.set(72);
        assert_eq!(comp.get(), 72);
    }

    // JE: AtomicLongComponentTest.testClear
    #[test]
    fn atomic_long_clear() {
        let comp = AtomicLongComponent::new();
        comp.set(37);
        comp.clear();
        assert_eq!(comp.get(), 0);
    }

    // JE: AtomicLongComponentTest.testCopy
    #[test]
    fn atomic_long_copy() {
        let comp = AtomicLongComponent::new();
        comp.set(70);
        let copy = comp.copy();
        comp.clear();
        assert_eq!(copy.get(), 70);
        copy.set(75);
        assert_eq!(comp.get(), 0);
    }

    // JE: AtomicLongComponentTest.testGetFormattedValue
    #[test]
    fn atomic_long_get_formatted_value() {
        let comp = AtomicLongComponent::new();
        comp.set(123456789);
        assert_eq!(comp.formatted_value(true), "123,456,789");
        assert_eq!(comp.formatted_value(false), "123456789");
    }

    // JE: AtomicLongComponentTest.testIsNotSet
    #[test]
    fn atomic_long_is_not_set() {
        let comp = AtomicLongComponent::new();
        assert!(comp.is_not_set());
        comp.set(3);
        assert!(!comp.is_not_set());
        comp.clear();
        assert!(comp.is_not_set());
    }

    // JE: AtomicLongComponentTest.testToString
    #[test]
    fn atomic_long_to_string() {
        let comp = AtomicLongComponent::new();
        comp.set(987654321);
        assert_eq!(comp.to_string(), "987654321");
    }

    // Guard: negative grouping (not directly a JE test, but exercises the
    // grouped-format helper used by the ports above with a negative value).
    #[test]
    fn grouped_format_negative() {
        assert_eq!(format_grouped_i64(-123456789), "-123,456,789");
        assert_eq!(format_grouped_i64(0), "0");
        assert_eq!(format_grouped_i64(999), "999");
        assert_eq!(format_grouped_i64(1000), "1,000");
    }
}
