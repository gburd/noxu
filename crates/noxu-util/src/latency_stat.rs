// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Latency histogram stat: min/max/avg/95th/99th percentile.
//!
//! Faithful reference port of JE's `com.sleepycat.utilint.LatencyStat` and its
//! result struct `com.sleepycat.utilint.Latency`.  A `LatencyStat` records
//! per-request latencies (in nanoseconds) into a millisecond-bucketed
//! histogram and, on demand, computes the min, max, average, 95th- and
//! 99th-percentile latencies (in milliseconds) plus the count of requests
//! whose latency exceeded the tracked maximum.
//!
//! This is pure self-contained arithmetic with no engine coupling — the same
//! faithful-reference-port precedent as [`crate::moving_avg`]
//! (`DoubleExpMovingAvg` / `LongAvgRate`) and [`crate::long_diff`]: the JE
//! algorithm (histogram bucketing, the `nTrackedRequests * .95` / `* .99`
//! truncation, the "never include the highest request" rule, the rollup
//! average blend) is ported byte-for-byte, together with JE's exact test
//! vectors from `LatencyStatTest`.
//!
//! Not yet wired into Noxu's stats framework (a flat [`crate::stats::StatGroup`]
//! of scalar counters, no percentile histograms).  Kept as a ready primitive
//! and a fidelity anchor for the JE latency-histogram arithmetic.
//!
//! JE ref: `com.sleepycat.utilint.LatencyStat`,
//! `com.sleepycat.utilint.Latency`.
//!
//! # Concurrency note (intentional deviation)
//!
//! JE's `LatencyStat` is thread-safe (atomic histogram, `synchronized`
//! calculate/clear) so `set` can be called concurrently with
//! `calculate`/`calculateAndClear` from many request threads.  This port is
//! single-threaded (`&mut self` for mutation).  Noxu's core is blocking I/O
//! with explicit threading (see AGENTS.md "Key Design Decisions": "No async");
//! a stat this fine-grained would be owned per-thread or behind the engine's
//! own latch when wired, so the interior atomics are not reproduced.  The JE
//! `testConcurrentSetCalculateClear` stress test asserts only that
//! concurrently-observed `Latency` snapshots are internally consistent; its
//! arithmetic invariants are ported single-threaded as `stress_invariants`.

/// A struct holding the min, max, avg, 95th, and 99th percentile measurements
/// for the collection of values held in a [`LatencyStat`].
///
/// JE ref: `com.sleepycat.utilint.Latency`.
#[derive(Debug, Clone, PartialEq)]
pub struct Latency {
    max_tracked_latency_millis: i32,
    min: i32,
    max: i32,
    avg: f32,
    total_ops: i32,
    total_requests: i32,
    percent95: i32,
    percent99: i32,
    /// Number of requests whose latency exceeded `max_tracked_latency_millis`.
    /// (JE names the serialized field `opsOverflow` for 5.0.69 compat; the
    /// accessor and meaning are "requests overflow".)
    requests_overflow: i32,
}

impl Latency {
    /// Creates a `Latency` with a `max_tracked_latency_millis` and all fields
    /// with zero values.
    pub fn empty(max_tracked_latency_millis: i32) -> Self {
        Latency {
            max_tracked_latency_millis,
            min: 0,
            max: 0,
            avg: 0.0,
            total_ops: 0,
            total_requests: 0,
            percent95: 0,
            percent99: 0,
            requests_overflow: 0,
        }
    }

    /// The number of operations recorded by this stat.
    pub fn total_ops(&self) -> i32 {
        self.total_ops
    }

    /// The number of requests recorded by this stat.
    pub fn total_requests(&self) -> i32 {
        self.total_requests
    }

    /// The number of requests which exceed the max expected latency.
    pub fn requests_overflow(&self) -> i32 {
        self.requests_overflow
    }

    /// The max expected latency for this kind of operation.
    pub fn max_tracked_latency_millis(&self) -> i32 {
        self.max_tracked_latency_millis
    }

    /// The fastest latency tracked (millis).
    pub fn min(&self) -> i32 {
        self.min
    }

    /// The slowest latency tracked (millis).
    pub fn max(&self) -> i32 {
        self.max
    }

    /// The average latency tracked (millis).
    pub fn avg(&self) -> f32 {
        self.avg
    }

    /// The 95th percentile latency tracked by the histogram (millis).
    pub fn percent_95(&self) -> i32 {
        self.percent95
    }

    /// The 99th percentile latency tracked by the histogram (millis).
    pub fn percent_99(&self) -> i32 {
        self.percent99
    }

    /// Add the measurements from `other` and recalculate the min, max, and
    /// average values.  The 95th and 99th percentile are **not** recalculated,
    /// because the histogram from `LatencyStat` is not available, and those
    /// values can't be regenerated — so they are cleared to 0.
    ///
    /// JE ref: `Latency.rollup`.
    ///
    /// # Panics
    ///
    /// Panics (JE throws `IllegalStateException`) if `other` has no data or a
    /// different `max_tracked_latency_millis`.
    pub fn rollup(&mut self, other: &Latency) {
        assert!(
            other.total_ops != 0 && other.total_requests != 0,
            "Can't rollup a Latency that doesn't have any data"
        );
        assert!(
            self.max_tracked_latency_millis == other.max_tracked_latency_millis,
            "Can't rollup a Latency whose maxTrackedLatencyMillis is different"
        );

        if self.min > other.min {
            self.min = other.min;
        }
        if self.max < other.max {
            self.max = other.max;
        }

        self.avg = ((self.total_requests as f32 * self.avg)
            + (other.total_requests as f32 * other.avg))
            / (self.total_requests + other.total_requests) as f32;

        // Clear out 95th and 99th.  They have become invalid.
        self.percent95 = 0;
        self.percent99 = 0;

        self.total_ops += other.total_ops;
        self.total_requests += other.total_requests;
        self.requests_overflow += other.requests_overflow;
    }
}

/// A stat that keeps track of latency in milliseconds and presents average,
/// min, max, 95th and 99th percentile values.
///
/// JE ref: `com.sleepycat.utilint.LatencyStat`.
#[derive(Debug, Clone)]
pub struct LatencyStat {
    /// The maximum tracked latency, in milliseconds; also the size of the
    /// histogram array which is used to save latencies.
    max_tracked_latency_millis: i32,
    values: Values,
}

/// The tracked values.  Cleared by assigning a fresh instance (mirrors JE's
/// `trackedValues` reset).
#[derive(Debug, Clone)]
struct Values {
    num_ops: i32,
    num_requests: i32,
    total_nanos: i64,
    /// Indexed by latency in millis; elements contain the number of ops for
    /// that latency.
    histogram: Vec<i32>,
    /// Min/max latency.  Both may exceed `max_tracked_latency_millis`.
    min_including_overflow: i32,
    max_including_overflow: i32,
    /// Number of requests whose latency exceeds `max_tracked_latency_millis`.
    requests_overflow: i32,
}

impl Values {
    fn new(max_tracked_latency_millis: i32) -> Self {
        Values {
            num_ops: 0,
            num_requests: 0,
            total_nanos: 0,
            histogram: vec![0; max_tracked_latency_millis.max(0) as usize],
            min_including_overflow: i32::MAX,
            max_including_overflow: 0,
            requests_overflow: 0,
        }
    }
}

impl LatencyStat {
    /// Creates a `LatencyStat` tracking latencies up to
    /// `max_tracked_latency_millis` in the histogram.
    pub fn new(max_tracked_latency_millis: i64) -> Self {
        let m = max_tracked_latency_millis as i32;
        LatencyStat { max_tracked_latency_millis: m, values: Values::new(m) }
    }

    /// Clears the accumulated measurements.
    pub fn clear(&mut self) {
        self.values = Values::new(self.max_tracked_latency_millis);
    }

    /// Record a single operation that took place in a request of `nano_latency`
    /// nanoseconds.
    pub fn set(&mut self, nano_latency: i64) {
        self.set_ops(1, nano_latency);
    }

    /// Record `num_recorded_ops` (one or more) operations that took place in a
    /// single request of `nano_latency` nanoseconds.
    ///
    /// JE ref: `LatencyStat.set(int numRecordedOps, long nanoLatency)`.
    pub fn set_ops(&mut self, num_recorded_ops: i32, nano_latency: i64) {
        // Ignore negative values [#22466].
        if nano_latency < 0 {
            return;
        }

        // Round the latency to determine where to mark the histogram.
        let millis_rounded =
            ((nano_latency + (1_000_000i64 / 2)) / 1_000_000i64) as i32;

        // Record this latency.
        if millis_rounded >= self.max_tracked_latency_millis {
            self.values.requests_overflow += 1;
        } else {
            self.values.histogram[millis_rounded as usize] += 1;
        }

        if self.values.max_including_overflow < millis_rounded {
            self.values.max_including_overflow = millis_rounded;
        }
        if self.values.min_including_overflow > millis_rounded {
            self.values.min_including_overflow = millis_rounded;
        }

        self.values.total_nanos += nano_latency;
        self.values.num_ops += num_recorded_ops;
        self.values.num_requests += 1;
    }

    /// Whether no requests/ops have been recorded.
    pub fn is_empty(&self) -> bool {
        self.values.num_ops == 0 || self.values.num_requests == 0
    }

    /// Generate the min, max, avg, 95th and 99th percentile for the collected
    /// measurements.  Does **not** clear the measurement collection.
    pub fn calculate(&mut self) -> Latency {
        self.calculate_internal(false)
    }

    /// Generate the min, max, avg, 95th and 99th percentile for the collected
    /// measurements, then clear the measurement collection.
    pub fn calculate_and_clear(&mut self) -> Latency {
        self.calculate_internal(true)
    }

    /// JE ref: `LatencyStat.calculate(boolean clear)`.
    fn calculate_internal(&mut self, clear: bool) -> Latency {
        // Snapshot the values (cloning if we are clearing, so the returned
        // computation is stable and the live histogram is reset).
        let values = if clear {
            std::mem::replace(
                &mut self.values,
                Values::new(self.max_tracked_latency_millis),
            )
        } else {
            self.values.clone()
        };

        let total_ops = values.num_ops;
        let total_requests = values.num_requests;
        if total_ops == 0 || total_requests == 0 {
            return Latency::empty(self.max_tracked_latency_millis);
        }

        let total_nanos = values.total_nanos;
        let n_overflow = values.requests_overflow;
        let max_including_overflow = values.max_including_overflow;
        let min_including_overflow = values.min_including_overflow;

        let avg_ms =
            ((total_nanos as f64 * 1e-6) / total_requests as f64) as f32;

        // The 95% and 99% values will be -1 if there are no recorded latencies
        // in the histogram.
        let mut percent95: i32 = -1;
        let mut percent99: i32 = -1;

        // Bound min/max to the (rounded) average, so they are sensible even
        // under concurrent updates (JE's rationale; harmless single-threaded).
        let avg_ms_int = round_half_up(avg_ms);
        let mut max = avg_ms_int.max(max_including_overflow);
        let mut min = avg_ms_int.min(min_including_overflow);

        let n_tracked_requests = total_requests - n_overflow;
        let (percent95_count, percent99_count) = if n_tracked_requests == 1 {
            // For one request, always include it in the 95% and 99%.
            (1, 1)
        } else {
            // Otherwise truncate: never include the last/highest request.
            (
                (n_tracked_requests as f64 * 0.95) as i32,
                (n_tracked_requests as f64 * 0.99) as i32,
            )
        };

        let mut num_requests_seen = 0i32;
        for latency in 0..values.histogram.len() as i32 {
            let count = values.histogram[latency as usize];
            if count == 0 {
                continue;
            }
            if min > latency {
                min = latency;
            }
            if max < latency {
                max = latency;
            }
            if num_requests_seen < percent95_count {
                percent95 = latency;
            }
            if num_requests_seen < percent99_count {
                percent99 = latency;
            }
            num_requests_seen += count;
        }

        Latency {
            max_tracked_latency_millis: self.max_tracked_latency_millis,
            min,
            max,
            avg: avg_ms,
            total_ops,
            total_requests,
            percent95,
            percent99,
            requests_overflow: n_overflow,
        }
    }
}

/// Round half up to the nearest integer, matching Java's `Math.round(float)`
/// (`floor(x + 0.5)`).  Reused from the same convention as
/// [`crate::moving_avg`].
fn round_half_up(x: f32) -> i32 {
    (x + 0.5).floor() as i32
}

#[cfg(test)]
mod tests {
    use super::*;

    const DELTA: f32 = 1e-6;

    /// Faithful port of JE `LatencyStatTest.checkResults`.
    #[allow(clippy::too_many_arguments)]
    fn check_results(
        results: &Latency,
        expected_req: i32,
        expected_ops: i32,
        expected_min: i32,
        expected_max: i32,
        expected_avg: f32,
        expected95: i32,
        expected99: i32,
        req_overflow: i32,
    ) {
        assert_eq!(expected_req, results.total_requests(), "totalRequests");
        assert_eq!(expected_ops, results.total_ops(), "totalOps");
        assert_eq!(expected_min, results.min(), "min");
        assert_eq!(expected_max, results.max(), "max");
        assert!(
            (expected_avg - results.avg()).abs() <= DELTA,
            "avg: expected {expected_avg}, got {}",
            results.avg()
        );
        assert_eq!(expected95, results.percent_95(), "95th");
        assert_eq!(expected99, results.percent_99(), "99th");
        assert_eq!(req_overflow, results.requests_overflow(), "reqOverflow");
    }

    /// JE: LatencyStatTest.testMillisLatency
    #[test]
    fn test_millis_latency() {
        let mut interval = LatencyStat::new(100);
        let mut accumulate = LatencyStat::new(100);

        for i in 0..=11 {
            interval.set(i * 10 * 1_000_000);
            accumulate.set(i * 10 * 1_000_000);
        }

        check_results(
            &interval.calculate_and_clear(),
            12,
            12,
            0,
            110,
            55.0,
            80,
            80,
            2,
        );
        check_results(&accumulate.calculate(), 12, 12, 0, 110, 55.0, 80, 80, 2);

        for _ in 0..20 {
            interval.set(92_000_000);
            accumulate.set(92_000_000);
        }

        check_results(
            &interval.calculate_and_clear(),
            20,
            20,
            92,
            92,
            92.0,
            92,
            92,
            0,
        );
        check_results(
            &accumulate.calculate(),
            32,
            32,
            0,
            110,
            78.125,
            92,
            92,
            2,
        );

        interval.clear();
        accumulate.clear();

        for i in 0..100 {
            interval.set(i * 1_000_000);
            accumulate.set(i * 1_000_000);
        }
        check_results(
            &interval.calculate_and_clear(),
            100,
            100,
            0,
            99,
            49.5,
            94,
            98,
            0,
        );
        check_results(
            &accumulate.calculate(),
            100,
            100,
            0,
            99,
            49.5,
            94,
            98,
            0,
        );
    }

    /// JE: LatencyStatTest.testNanoLatency
    #[test]
    fn test_nano_latency() {
        let mut interval = LatencyStat::new(100);
        let mut accumulate = LatencyStat::new(100);

        for i in 0..=11 {
            interval.set(i * 10_000);
            accumulate.set(i * 10_000);
        }

        check_results(
            &interval.calculate_and_clear(),
            12,
            12,
            0,
            0,
            0.055,
            0,
            0,
            0,
        );
        check_results(&accumulate.calculate(), 12, 12, 0, 0, 0.055, 0, 0, 0);

        for i in 1..=10 {
            interval.set((i * 1_000_000) + 500_000);
            accumulate.set((i * 1_000_000) + 500_000);
        }
        check_results(
            &interval.calculate_and_clear(),
            10,
            10,
            2,
            11,
            6.0,
            10,
            10,
            0,
        );
        check_results(
            &accumulate.calculate(),
            22,
            22,
            0,
            11,
            2.7572727,
            9,
            10,
            0,
        );
    }

    /// JE: LatencyStatTest.testRollup
    #[test]
    fn test_rollup() {
        let mut stat1 = LatencyStat::new(100);
        let mut stat2 = LatencyStat::new(100);

        for i in 0..=11 {
            stat1.set(i * 10 * 1_000_000);
            stat2.set_ops(5, i * 20 * 1_000_000);
        }

        let mut result1 = stat1.calculate();
        check_results(&result1, 12, 12, 0, 110, 55.0, 80, 80, 2);
        let result2 = stat2.calculate();
        check_results(&result2, 12, 60, 0, 220, 110.0, 60, 60, 7);

        // 95th and 99th become 0 because they are not preserved by rollup.
        result1.rollup(&result2);
        check_results(&result1, 24, 72, 0, 220, 82.5, 0, 0, 9);
    }

    /// JE: LatencyStatTest.testSmallNumberOfOps
    ///
    /// When there is only one op, the 95% and 99% numbers should be the
    /// latency for that op, not -1. [#21763]
    #[test]
    fn test_small_number_of_ops() {
        let mut stat = LatencyStat::new(100);

        stat.set(6_900_000);
        check_results(&stat.calculate_and_clear(), 1, 1, 7, 7, 6.9, 7, 7, 0);

        stat.set(7 * 1_000_000);
        check_results(&stat.calculate(), 1, 1, 7, 7, 7.0, 7, 7, 0);

        stat.set(8 * 1_000_000);
        check_results(&stat.calculate(), 2, 2, 7, 8, 7.5, 7, 7, 0);

        stat.set(9 * 1_000_000);
        check_results(&stat.calculate(), 3, 3, 7, 9, 8.0, 8, 8, 0);
    }

    /// JE: LatencyStatTest.testMultiOps
    ///
    /// Tests `set` when passing `numRecordedOps > 1`.
    #[test]
    fn test_multi_ops() {
        let mut stat = LatencyStat::new(100);

        // Basic check of a single request.
        stat.set_ops(10, 3 * 1_000_000);
        check_results(&stat.calculate_and_clear(), 1, 10, 3, 3, 3.0, 3, 3, 0);

        // Two requests, no overflow.
        stat.set_ops(5, 1_000_000);
        stat.set_ops(10, 3 * 1_000_000);
        check_results(&stat.calculate_and_clear(), 2, 15, 1, 3, 2.0, 1, 1, 0);

        // Three requests, one overflow.
        stat.set_ops(5, 3 * 1_000_000);
        stat.set_ops(10, 16 * 1_000_000);
        stat.set_ops(10, 101 * 1_000_000);
        check_results(
            &stat.calculate_and_clear(),
            3,
            25,
            3,
            101,
            40.0,
            3,
            3,
            1,
        );

        // Three requests, all overflows.
        stat.set_ops(5, 101 * 1_000_000);
        stat.set_ops(5, 102 * 1_000_000);
        stat.set_ops(5, 103 * 1_000_000);
        check_results(
            &stat.calculate_and_clear(),
            3,
            15,
            101,
            103,
            102.0,
            -1,
            -1,
            3,
        );

        // Check that when the very highest recorded latency is high, and the
        // rest (95% and 99%) are low, we don't report the high value.  Prior
        // to a bug fix (JE [#21763]) the high value was reported: both checks
        // reported 77 for the 95%/99% values, but 7 is correct.
        for _ in 0..100 {
            stat.set_ops(10, 7 * 1_000_000);
        }
        stat.set_ops(20, 77 * 1_000_000);
        check_results(
            &stat.calculate_and_clear(),
            101,
            1020,
            7,
            77,
            7.6930695,
            7,
            7,
            0,
        );
    }

    /// JE: LatencyStatTest.testConcurrentSetCalculateClear — arithmetic
    /// invariants half (see the module-level concurrency note for why the
    /// 20-second Java thread-stress harness itself is N/A).  This asserts the
    /// same per-snapshot consistency invariants JE's `DoCalc` asserts, over a
    /// deterministic single-threaded workload of randomized latencies.
    #[test]
    fn stress_invariants() {
        use crate::prng::Prng;
        let mut stat = LatencyStat::new(100);
        let mut rnd = Prng::new(123);
        // Interleave sets and calculate/calculate_and_clear like the JE
        // DoSet/DoCalc threads, checking invariants on each non-empty result.
        for round in 0..2000u32 {
            let nanos = ((rnd.next_u64() % 99) as i64 + 1) * 1_000_000;
            let n_ops = if round % 7 == 0 {
                (rnd.next_u64() % 10) as i32 + 1
            } else {
                1
            };
            stat.set_ops(n_ops, nanos);

            if round % 3 == 0 {
                let latency = if round % 6 == 0 {
                    stat.calculate_and_clear()
                } else {
                    stat.calculate()
                };
                if latency.total_ops() == 0 {
                    continue;
                }
                let s = format!("{latency:?}");
                assert!(latency.percent_95() >= 0, "{s}");
                assert!(latency.min() >= 0, "{s}");
                assert_ne!(latency.min(), i32::MAX, "{s}");
                assert!(latency.min() <= round_half_up(latency.avg()), "{s}");
                assert!(latency.min() <= latency.percent_95(), "{s}");
                assert!(latency.min() <= latency.percent_99(), "{s}");
                assert!(latency.max() >= latency.min(), "{s}");
                assert!(latency.max() >= round_half_up(latency.avg()), "{s}");
                assert!(latency.max() >= latency.percent_95(), "{s}");
                assert!(latency.max() >= latency.percent_99(), "{s}");
                assert!(latency.avg() > 0.0, "{s}");
                assert_eq!(latency.requests_overflow(), 0, "{s}");
            }
        }
    }

    /// Empty stat returns an all-zero Latency (JE: `calculate` when
    /// numOps/numRequests == 0).
    #[test]
    fn empty_stat_returns_empty_latency() {
        let mut stat = LatencyStat::new(100);
        assert!(stat.is_empty());
        let l = stat.calculate();
        assert_eq!(l, Latency::empty(100));
        assert_eq!(l.total_ops(), 0);
    }
}
