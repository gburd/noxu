//! TTL expiration tracking for log files.
//!
//! tracks expired bytes in time windows
//! (histogram) for each log file, used to calculate expired data during cleaning.

use hashbrown::HashMap;

/// Tracks the expired bytes in each time window (histogram) for a log file.
///
/// Each tracked file maintains a histogram of expiration times to byte counts.
/// This is used during cleaning to determine how much data in a file will
/// expire and when, allowing the cleaner to prioritize files with more
/// expired data.
///
/// The histogram uses expiration time buckets (in hours since epoch) as keys
/// and tracks both the count of records and total size for each bucket.
#[derive(Debug)]
pub struct ExpirationTracker {
    /// The log file number being tracked.
    file_number: u32,

    /// Histogram bins: expiration_time_hours -> (count, size in bytes)
    bins: HashMap<u64, ExpirationBin>,
}

/// A single bin in the expiration histogram.
#[derive(Debug, Clone, Default)]
pub struct ExpirationBin {
    /// Expiration time bucket (in hours since epoch, 0 = never expires).
    pub expiration_time: u64,

    /// Number of records expiring in this bucket.
    pub count: i32,

    /// Total size in bytes of records expiring in this bucket.
    pub size: i32,
}

impl ExpirationTracker {
    /// Creates a new expiration tracker for the given file.
    pub fn new(file_number: u32) -> Self {
        Self { file_number, bins: HashMap::new() }
    }

    /// Returns the file number being tracked.
    pub fn get_file_number(&self) -> u32 {
        self.file_number
    }

    /// Tracks an entry with the given expiration time and size.
    ///
    /// # Arguments
    /// * `expiration_time` - Expiration time in **hours since epoch**
    ///   (packed-hours unit from the log format; 0 = never expires)
    /// * `size` - Size of the entry in bytes
    pub fn track(&mut self, expiration_time: u64, size: i32) {
        if expiration_time == 0 {
            // 0 means never expires - don't track
            return;
        }

        self.bins
            .entry(expiration_time)
            .and_modify(|bin| {
                bin.count += 1;
                bin.size += size;
            })
            .or_insert(ExpirationBin { expiration_time, count: 1, size });
    }

    /// Returns the total size of expired bytes as of the given time.
    ///
    /// # Arguments
    /// * `current_time` - Current time in **hours since epoch** (same unit as
    ///   values passed to `track`)
    ///
    /// # Returns
    /// Total size in bytes of all entries that have expired by `current_time`
    pub fn get_expired_bytes(&self, current_time: u64) -> i64 {
        let mut expired_size = 0i64;

        for bin in self.bins.values() {
            if bin.expiration_time > 0 && bin.expiration_time <= current_time {
                expired_size += bin.size as i64;
            }
        }

        expired_size
    }

    /// Serializes the histogram to a compact byte array (CLN-24).
    ///
    /// Faithful port of JE `ExpirationTracker.serialize`: byte 0 is the
    /// `hours` flag (`1` iff any `exp % 24 != 0`, else `0`); the remainder is
    /// a run-length-encoded series of `{interval, byteSize}` varint pairs,
    /// ordered by expiration time.  `interval` is the delta from the previous
    /// expiration value.  When all values are day-aligned (`hours == 0`) the
    /// expiration values are stored in DAYS (`exp / 24`) to shrink the deltas,
    /// matching JE's packed-integer space optimisation.
    ///
    /// Returns an empty `Vec` if no data has an expiration time (JE returns
    /// `Key.EMPTY_KEY`).  The serialized byte counter per bucket mirrors JE's
    /// `AtomicInteger` byte counters.
    ///
    /// JE: `ExpirationTracker.serialize` / `isExpirationInHours`.
    pub fn serialize(&self) -> Vec<u8> {
        if self.bins.is_empty() {
            return Vec::new();
        }
        let mut exps: Vec<u64> = self.bins.keys().copied().collect();
        exps.sort_unstable();
        // JE: hours = true iff any exp % 24 != 0.
        let hours = exps.iter().any(|&exp| exp % 24 != 0);
        let mut out = Vec::with_capacity(1 + exps.len() * 4);
        out.push(if hours { 1u8 } else { 0u8 });
        let mut prev_exp: u32 = 0;
        for exp in exps {
            let size = self.bins[&exp].size as u32;
            // JE: if !hours, store the value in days (exp / 24).  Expiration
            // times are hours-since-epoch (< 2^32 for any realistic clock),
            // so the u64->u32 narrowing is lossless.
            let stored = (if hours { exp } else { exp / 24 }) as u32;
            write_varint(&mut out, stored - prev_exp);
            write_varint(&mut out, size);
            prev_exp = stored;
        }
        out
    }

    /// Reconstructs an `ExpirationTracker` for `file_number` from the
    /// serialized form produced by [`serialize`](Self::serialize) (CLN-24).
    ///
    /// The reconstructed tracker carries the same per-bucket byte counts and
    /// the same day/hour granularity as the original, so the recovered
    /// `get_expired_bytes_band` prediction matches what the live tracker
    /// would have produced before the restart.  The per-bin record `count`
    /// is not serialized (JE persists only the byte counter); it is
    /// reconstructed as `1` per bucket.
    ///
    /// An empty slice yields an empty tracker (no expiring data).
    ///
    /// JE: `ExpirationTracker.toString(byte[])` / `getExpiredBytes(byte[],..)`
    /// (the same `{interval,size}` packed-pair reader, day-or-hour by byte 0).
    pub fn deserialize(file_number: u32, serialized: &[u8]) -> Self {
        let mut t = Self::new(file_number);
        if serialized.is_empty() {
            return t;
        }
        let hours = serialized[0] == 1;
        let mut pos = 1usize;
        let mut prev_exp: u64 = 0;
        while pos < serialized.len() {
            let (delta, n1) = read_varint(&serialized[pos..]);
            pos += n1;
            if pos >= serialized.len() {
                break;
            }
            let (size, n2) = read_varint(&serialized[pos..]);
            pos += n2;
            let stored = prev_exp + delta as u64;
            prev_exp = stored;
            // JE stores in days when !hours; convert back to hours-since-epoch
            // (the unit ExpirationTracker tracks in).
            let exp_hours = if hours { stored } else { stored * 24 };
            t.track(exp_hours, size as i32);
        }
        t
    }

    /// Returns whether every tracked bin expires on a DAY boundary (its
    /// expiration hour is a multiple of 24).  When true, JE represents the
    /// histogram in days and prorates the gradual band over a whole day
    /// rather than an hour.
    ///
    /// JE `ExpirationTracker.serialize`: the `hours` flag is set iff any
    /// `exp % 24 != 0`; if all are day-aligned the serialized form uses days
    /// and `ExpirationProfile.getExpiredBytes` picks `MILLIS_PER_DAY` as the
    /// proration interval (the `anyExpirationInHours == false` branch).
    pub fn is_expiration_in_hours(&self) -> bool {
        self.bins.keys().any(|&exp| exp % 24 != 0)
    }

    /// Returns the (lower, upper) expired-bytes uncertainty band as of
    /// `current_time` (hours since epoch) and `current_sub_hour_ms` (millis
    /// elapsed within the current hour, 0..3_600_000).
    ///
    /// Mirrors JE `ExpirationProfile.getExpiredBytes` (which returns a
    /// `Pair<lower, gradual-upper>`):
    ///   - **lower** = bytes whose expiration window has FULLY passed (the
    ///     bin expired in a prior INTERVAL) — definitely obsolete.
    ///   - **upper (gradual)** = lower PLUS a prorated fraction of the bytes
    ///     expiring within the CURRENT interval:
    ///     `newly * elapsed_in_interval / interval_ms`.
    ///
    /// **Interval granularity (CLN-26).**  JE chooses the proration interval
    /// per-file based on the histogram's alignment
    /// (`ExpirationProfile.getExpiredBytes`: `intervalMs = anyExpirationInHours
    /// ? MILLIS_PER_HOUR : MILLIS_PER_DAY`).  When every bin is day-aligned
    /// (TTL set in days), the band prorates linearly over the **current day**
    /// (a wider, smoother band); otherwise over the **current hour**.  Noxu
    /// stores expiration in hours-since-epoch, so:
    ///   - hour granularity: the current interval is the current hour
    ///     (`expiration_time == current_time`), prorated by
    ///     `current_sub_hour_ms / HOUR_MS`.
    ///   - day granularity: the current interval is the current DAY-bucket
    ///     (`expiration_time / 24 == current_time / 24`), prorated by the
    ///     fraction of the day elapsed,
    ///     `((current_time % 24) * HOUR_MS + current_sub_hour_ms) / DAY_MS`.
    ///
    /// The width `upper - lower` is the two-pass uncertainty band JE's cleaner
    /// gates on (`CLEANER_TWO_PASS_GAP`).
    pub fn get_expired_bytes_band(
        &self,
        current_time: u64,
        current_sub_hour_ms: u64,
    ) -> (i64, i64) {
        const HOUR_MS: u64 = 3_600_000;
        const DAY_MS: u64 = HOUR_MS * 24;
        // JE: anyExpirationInHours decides the proration interval.
        let hours_granularity = self.is_expiration_in_hours();
        let sub_hour = current_sub_hour_ms.min(HOUR_MS);
        // Elapsed-within-current-interval and the interval width, per JE
        // `currentMs = time % intervalMs` (with the whole-interval cap).
        let (elapsed, interval_ms) = if hours_granularity {
            (sub_hour, HOUR_MS)
        } else {
            (((current_time % 24) * HOUR_MS + sub_hour).min(DAY_MS), DAY_MS)
        };
        // The current-interval bucket index: hour for hour-granularity, day
        // for day-granularity.
        let cur_bucket =
            if hours_granularity { current_time } else { current_time / 24 };
        let mut lower = 0i64;
        let mut newly = 0i64;
        for bin in self.bins.values() {
            if bin.expiration_time == 0 {
                continue;
            }
            let bin_bucket = if hours_granularity {
                bin.expiration_time
            } else {
                bin.expiration_time / 24
            };
            if bin_bucket < cur_bucket {
                // Expired in a prior interval: fully obsolete.
                lower += bin.size as i64;
            } else if bin_bucket == cur_bucket {
                // Expiring within the current interval: the uncertain part.
                newly += bin.size as i64;
            }
        }
        // gradual = lower + prorated fraction of the current-interval bytes.
        let gradual = lower + (newly * elapsed as i64) / interval_ms as i64;
        (lower, gradual)
    }

    /// Returns the total tracked size (all bins).
    pub fn get_total_tracked_size(&self) -> i64 {
        self.bins.values().map(|bin| bin.size as i64).sum()
    }

    /// Returns the number of bins in the histogram.
    pub fn get_bin_count(&self) -> usize {
        self.bins.len()
    }

    /// Returns a reference to all bins (for testing/inspection).
    pub fn get_bins(&self) -> &HashMap<u64, ExpirationBin> {
        &self.bins
    }

    /// Clears all tracked data.
    pub fn clear(&mut self) {
        self.bins.clear();
    }
}

/// LEB128-style varint writer (7 data bits + continuation bit per byte).
///
/// Local to the expiration serializer; the byte layout only needs to be
/// self-consistent across `serialize`/`deserialize` (it is the record "data"
/// in a FileSummaryLN, not an interop format).  Matches the encoding used by
/// `packed_offsets::write_varint`.
fn write_varint(buffer: &mut Vec<u8>, mut value: u32) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buffer.push(byte);
        if value == 0 {
            break;
        }
    }
}

/// LEB128-style varint reader; returns `(value, bytes_read)`.
fn read_varint(buffer: &[u8]) -> (u32, usize) {
    let mut value = 0u32;
    let mut shift = 0;
    let mut bytes_read = 0;
    for &byte in buffer {
        bytes_read += 1;
        value |= ((byte & 0x7F) as u32) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    (value, bytes_read)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expired_bytes_band_uncertainty() {
        let mut t = ExpirationTracker::new(0);
        t.track(5, 100);
        t.track(10, 200);
        t.track(20, 400);
        let (lower, upper) = t.get_expired_bytes_band(10, 1_800_000);
        assert_eq!(lower, 100, "lower = fully-elapsed bins only");
        assert_eq!(upper, 200, "upper = lower + prorated current-interval bin");
        assert_eq!(upper - lower, 100);
        let (lo0, up0) = t.get_expired_bytes_band(10, 0);
        assert_eq!(lo0, 100);
        assert_eq!(up0, 100);
        let (lo1, up1) = t.get_expired_bytes_band(10, 3_600_000);
        assert_eq!(lo1, 100);
        assert_eq!(up1, 300);
    }

    /// CLN-26 HEADLINE: a file of DAY-TTL (day-aligned) data prorates its
    /// gradual band over the whole DAY (a wider, smoother band), whereas an
    /// hour-TTL file prorates over the hour.  For the SAME elapsed fraction of
    /// the current interval the two bands match, but at a fixed wall-clock
    /// offset into the period the day-TTL gradual-upper differs from the
    /// hour-TTL case.
    ///
    /// FAIL-PRE (hour-only): both files would prorate over the hour, so the
    /// day-TTL band at hour boundaries would jump to full instead of being
    /// spread across 24 hours.
    ///
    /// JE: `ExpirationProfile.getExpiredBytes` (`intervalMs = anyExpirationIn
    /// Hours ? MILLIS_PER_HOUR : MILLIS_PER_DAY`) + `ExpirationTracker.
    /// serialize` (the `exp % 24` day-alignment test).
    #[test]
    fn test_cln26_day_vs_hour_proration() {
        const HOUR_MS: u64 = 3_600_000;
        // Day-TTL file: all bins land on the SAME day-bucket (day 1 = hours
        // 24..47), so they expire "within the current day" and prorate over
        // the whole day.  Hours 24, 36 are both multiples of 12 but only
        // multiples of 24 are day-aligned; pick 24 and 48 so the histogram is
        // day-aligned (24%24==0, 48%24==0).
        let mut day = ExpirationTracker::new(0);
        day.track(24, 1000); // day-bucket 1
        assert!(!day.is_expiration_in_hours(), "all bins day-aligned");

        // Hour-TTL file: a bin at an hour that is NOT day-aligned.
        let mut hour = ExpirationTracker::new(1);
        hour.track(25, 1000); // hour-bucket 25 (25%24 != 0)
        assert!(hour.is_expiration_in_hours(), "a non-day-aligned bin");

        // Evaluate both 30 minutes into the current period.
        //   - day file: current_time=24 (start of day 1), 30min in =>
        //     elapsed = (24%24)*HOUR + 30min = 30min of a 24h interval
        //     => fraction 0.5/24, gradual = 1000 * (1.8e6 / 86.4e6) ~= 20.
        //   - hour file: current_time=25, 30min in => fraction 0.5 of the
        //     hour => gradual = 1000 * 0.5 = 500.
        let (lo_d, up_d) = day.get_expired_bytes_band(24, HOUR_MS / 2);
        let (lo_h, up_h) = hour.get_expired_bytes_band(25, HOUR_MS / 2);
        assert_eq!(lo_d, 0, "day bin not yet in a prior day");
        assert_eq!(lo_h, 0, "hour bin not yet in a prior hour");
        // The day band is much narrower at the same wall-clock offset because
        // it is spread over 24x the interval.
        assert!(
            up_d < up_h,
            "day-TTL gradual ({up_d}) must be < hour-TTL gradual ({up_h}) at \
             the same wall-clock offset (day spreads over 24h)"
        );
        // Concrete proration check: 30min / 24h of 1000.
        let expect_day = 1000 * (HOUR_MS / 2) as i64 / (HOUR_MS * 24) as i64;
        assert_eq!(up_d, expect_day, "day proration over MILLIS_PER_DAY");
        assert_eq!(up_h, 500, "hour proration over MILLIS_PER_HOUR");

        // Half-way through the DAY (hour 12 of the day, i.e. current_time=36),
        // the day band should be ~half of the bin, matching what the hour band
        // would reach only at the END of its hour.
        let (_, up_d_mid) = day.get_expired_bytes_band(36, 0);
        assert_eq!(up_d_mid, 500, "day band at mid-day = half the bin");
    }

    /// CLN-24 roundtrip: serialize the histogram, deserialize, and assert the
    /// reconstructed tracker yields the SAME expired-bytes band — including
    /// the day-band proration math.
    ///
    /// Day-band math (the assertion the previous agent miscalculated): with
    /// `interval = DAY` and a bin on the current day-bucket, at wall-clock
    /// `current_time = T` hours / `sub_hour_ms` ms,
    ///   elapsed_in_day = (T % 24) * HOUR_MS + sub_hour_ms
    ///   gradual = lower + bin_size * elapsed_in_day / DAY_MS.
    #[test]
    fn test_cln24_serialize_roundtrip_days() {
        const HOUR_MS: u64 = 3_600_000;
        const DAY_MS: u64 = HOUR_MS * 24;
        // Day-aligned histogram: bins on day-buckets 1, 2, 3 (hours 24,48,72).
        let mut t = ExpirationTracker::new(7);
        t.track(24, 1000);
        t.track(48, 2000);
        t.track(72, 4000);
        assert!(!t.is_expiration_in_hours(), "all bins day-aligned");

        let bytes = t.serialize();
        // byte 0 must be the day flag (0 = days), non-empty payload follows.
        assert_eq!(bytes[0], 0, "day-aligned histogram serializes with days");
        assert!(bytes.len() > 1);

        let r = ExpirationTracker::deserialize(7, &bytes);
        assert_eq!(r.get_file_number(), 7);
        assert_eq!(r.get_bin_count(), 3, "all three buckets restored");
        assert!(!r.is_expiration_in_hours(), "granularity preserved (days)");
        assert_eq!(r.get_total_tracked_size(), 7000);
        // Per-bucket sizes restored.
        assert_eq!(r.get_bins().get(&24).unwrap().size, 1000);
        assert_eq!(r.get_bins().get(&48).unwrap().size, 2000);
        assert_eq!(r.get_bins().get(&72).unwrap().size, 4000);

        // Band MUST match the original at an arbitrary wall-clock point.
        // Evaluate at current_time = 49 (day-bucket 2, 1h into the day),
        // sub_hour = 30min.  day-bucket 1 (hours 24..47) is fully in a PRIOR
        // day => lower = 1000.  day-bucket 2 (the bin at hour 48) is the
        // current day => prorated.
        //   elapsed_in_day = (49 % 24) * HOUR_MS + 30min
        //                  = 1*HOUR_MS + HOUR_MS/2 = 1.5h of a 24h interval.
        let sub = HOUR_MS / 2;
        let (lo_orig, up_orig) = t.get_expired_bytes_band(49, sub);
        let (lo_r, up_r) = r.get_expired_bytes_band(49, sub);
        assert_eq!((lo_orig, up_orig), (lo_r, up_r), "band survives roundtrip");
        // Concrete: lower = 1000 (day 1 elapsed); gradual adds day-2 (2000)
        // prorated by 1.5h/24h.
        let elapsed = HOUR_MS + sub; // 1.5h in ms
        let expect_lo = 1000i64;
        let expect_up = 1000 + 2000 * elapsed as i64 / DAY_MS as i64;
        assert_eq!(lo_r, expect_lo, "day-1 fully obsolete");
        assert_eq!(up_r, expect_up, "day-2 prorated over MILLIS_PER_DAY");
    }

    /// CLN-24 roundtrip: an HOUR-granularity histogram (a non-day-aligned
    /// bin) serializes with the hours flag set and prorates over the hour.
    #[test]
    fn test_cln24_serialize_roundtrip_hours() {
        let mut t = ExpirationTracker::new(3);
        t.track(25, 1000); // 25 % 24 != 0 => hours
        t.track(100, 500);
        assert!(t.is_expiration_in_hours());

        let bytes = t.serialize();
        assert_eq!(
            bytes[0], 1,
            "hour-granular histogram serializes with hours"
        );

        let r = ExpirationTracker::deserialize(3, &bytes);
        assert!(r.is_expiration_in_hours(), "granularity preserved (hours)");
        assert_eq!(r.get_bin_count(), 2);
        assert_eq!(r.get_bins().get(&25).unwrap().size, 1000);
        assert_eq!(r.get_bins().get(&100).unwrap().size, 500);
        // Band matches: at hour 100, 30min in: hour 25 fully obsolete (1000),
        // hour 100 prorated by 0.5 => +250.
        let (lo, up) = r.get_expired_bytes_band(100, 1_800_000);
        assert_eq!(lo, 1000);
        assert_eq!(up, 1250);
    }

    /// CLN-24: an empty histogram serializes to an empty array (JE returns
    /// `Key.EMPTY_KEY`) and deserializes back to an empty tracker.
    #[test]
    fn test_cln24_serialize_empty() {
        let t = ExpirationTracker::new(0);
        assert!(t.serialize().is_empty());
        let r = ExpirationTracker::deserialize(0, &[]);
        assert_eq!(r.get_bin_count(), 0);
        assert_eq!(r.get_total_tracked_size(), 0);
    }

    #[test]
    fn test_new_tracker() {
        let tracker = ExpirationTracker::new(5);
        assert_eq!(tracker.get_file_number(), 5);
        assert_eq!(tracker.get_bin_count(), 0);
        assert_eq!(tracker.get_total_tracked_size(), 0);
    }

    #[test]
    fn test_track_single_entry() {
        let mut tracker = ExpirationTracker::new(1);

        tracker.track(100, 1024);

        assert_eq!(tracker.get_bin_count(), 1);
        assert_eq!(tracker.get_total_tracked_size(), 1024);
    }

    #[test]
    fn test_track_never_expires() {
        let mut tracker = ExpirationTracker::new(1);

        // Expiration time 0 means never expires - should not be tracked
        tracker.track(0, 1024);

        assert_eq!(tracker.get_bin_count(), 0);
        assert_eq!(tracker.get_total_tracked_size(), 0);
    }

    #[test]
    fn test_track_multiple_same_expiration() {
        let mut tracker = ExpirationTracker::new(1);

        tracker.track(100, 512);
        tracker.track(100, 256);
        tracker.track(100, 128);

        assert_eq!(tracker.get_bin_count(), 1);
        assert_eq!(tracker.get_total_tracked_size(), 896);

        let bin = tracker.get_bins().get(&100).unwrap();
        assert_eq!(bin.count, 3);
        assert_eq!(bin.size, 896);
    }

    #[test]
    fn test_track_different_expirations() {
        let mut tracker = ExpirationTracker::new(1);

        tracker.track(100, 1000);
        tracker.track(200, 2000);
        tracker.track(300, 3000);

        assert_eq!(tracker.get_bin_count(), 3);
        assert_eq!(tracker.get_total_tracked_size(), 6000);
    }

    #[test]
    fn test_get_expired_bytes_none_expired() {
        let mut tracker = ExpirationTracker::new(1);

        tracker.track(100, 1000);
        tracker.track(200, 2000);
        tracker.track(300, 3000);

        // Current time is before all expirations
        let expired = tracker.get_expired_bytes(50);
        assert_eq!(expired, 0);
    }

    #[test]
    fn test_get_expired_bytes_some_expired() {
        let mut tracker = ExpirationTracker::new(1);

        tracker.track(100, 1000);
        tracker.track(200, 2000);
        tracker.track(300, 3000);

        // Current time is after first two expirations
        let expired = tracker.get_expired_bytes(250);
        assert_eq!(expired, 3000); // 1000 + 2000
    }

    #[test]
    fn test_get_expired_bytes_all_expired() {
        let mut tracker = ExpirationTracker::new(1);

        tracker.track(100, 1000);
        tracker.track(200, 2000);
        tracker.track(300, 3000);

        // Current time is after all expirations
        let expired = tracker.get_expired_bytes(400);
        assert_eq!(expired, 6000);
    }

    #[test]
    fn test_get_expired_bytes_exact_boundary() {
        let mut tracker = ExpirationTracker::new(1);

        tracker.track(100, 1000);
        tracker.track(200, 2000);

        // Current time exactly at expiration
        let expired = tracker.get_expired_bytes(100);
        assert_eq!(expired, 1000);

        let expired = tracker.get_expired_bytes(200);
        assert_eq!(expired, 3000);
    }

    #[test]
    fn test_clear() {
        let mut tracker = ExpirationTracker::new(1);

        tracker.track(100, 1000);
        tracker.track(200, 2000);

        assert_eq!(tracker.get_bin_count(), 2);

        tracker.clear();

        assert_eq!(tracker.get_bin_count(), 0);
        assert_eq!(tracker.get_total_tracked_size(), 0);
        assert_eq!(tracker.get_expired_bytes(1000), 0);
    }

    #[test]
    fn test_large_values() {
        let mut tracker = ExpirationTracker::new(1);

        tracker.track(1_000_000, 100_000_000);
        tracker.track(2_000_000, 200_000_000);

        assert_eq!(tracker.get_total_tracked_size(), 300_000_000);
        assert_eq!(tracker.get_expired_bytes(1_500_000), 100_000_000);
        assert_eq!(tracker.get_expired_bytes(3_000_000), 300_000_000);
    }

    #[test]
    fn test_bins_independent() {
        let mut tracker = ExpirationTracker::new(1);

        tracker.track(100, 1000);
        tracker.track(200, 2000);
        tracker.track(300, 3000);

        // Each bin should maintain its own count and size
        let bin100 = tracker.get_bins().get(&100).unwrap();
        let bin200 = tracker.get_bins().get(&200).unwrap();
        let bin300 = tracker.get_bins().get(&300).unwrap();

        assert_eq!(bin100.size, 1000);
        assert_eq!(bin200.size, 2000);
        assert_eq!(bin300.size, 3000);

        assert_eq!(bin100.count, 1);
        assert_eq!(bin200.count, 1);
        assert_eq!(bin300.count, 1);
    }

    #[test]
    fn test_expiration_bin_default() {
        let bin = ExpirationBin::default();
        assert_eq!(bin.expiration_time, 0);
        assert_eq!(bin.count, 0);
        assert_eq!(bin.size, 0);
    }

    #[test]
    fn test_mixed_tracking() {
        let mut tracker = ExpirationTracker::new(1);

        // Mix of never-expires and timed entries
        tracker.track(0, 1000); // Should be ignored
        tracker.track(100, 500);
        tracker.track(0, 2000); // Should be ignored
        tracker.track(100, 500);
        tracker.track(200, 1000);

        assert_eq!(tracker.get_bin_count(), 2); // Only two bins (100 and 200)
        assert_eq!(tracker.get_total_tracked_size(), 2000); // Ignores never-expires entries
    }
}
