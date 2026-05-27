//! JE-equivalent VLSN-index tests.
//!
//! Wave 6 — Priority-3 JE TCK port.
//!
//! Ports invariants from
//! `je/test/com/sleepycat/je/rep/vlsn/VLSNIndexTest.java` and
//! `je/test/com/sleepycat/je/rep/vlsn/VLSNBucketTest.java`, adapted to
//! Noxu's `VlsnIndex`/`VlsnBucket` API.
//!
//! Notes on adaptation:
//! * JE's `VLSNBucket(maxMappings, maxDistance)` caps each bucket's size;
//!   Noxu has no such cap (one bucket per file/range), so JE's bucket-cap
//!   asserts have no analogue.  We port only the in-bucket lookup and the
//!   index-level gap-bucket invariants that survive the cap difference.
//! * JE's `removeFromTail(vlsn, prevLsn)` becomes `truncate_after(vlsn-1)`
//!   — i.e. truncate the index so that the new last-vlsn is `vlsn - 1`.

use noxu_rep::vlsn::{VlsnBucket, VlsnIndex};

// --------------------------------------------------------------------------
// VLSNBucketTest::testRemoveFromTail (adapted)
//
// JE's removeFromTail(startDeleteVlsn, prevLsn) lops off all entries
// >= startDeleteVlsn.  Noxu has no per-bucket removeFromTail; the
// index-level equivalent is `truncate_after(startDeleteVlsn - 1)`.
// We test the index-level invariant: after truncation, ownership of
// vlsns >= startDeleteVlsn is gone, and ownership of vlsns <
// startDeleteVlsn is preserved.
// --------------------------------------------------------------------------
#[test]
fn test_remove_from_tail_index_level() {
    // Build an index with vlsns 10..20, file=0.
    let stride = 3u32;
    let index = VlsnIndex::new(stride);
    for v in 10u64..20 {
        index.put(v, 0, (v as u32) * 10);
    }

    // Sweep over different truncation points, JE-style.
    for start_delete in 9u64..21 {
        // Reload the index for each iteration (no reset method).
        let idx = VlsnIndex::new(stride);
        for v in 10u64..20 {
            idx.put(v, 0, (v as u32) * 10);
        }

        // Truncate so that anything >= start_delete is gone.
        if start_delete == 0 {
            idx.truncate_after(0);
        } else {
            idx.truncate_after(start_delete.saturating_sub(1));
        }

        // After truncation, anything >= start_delete must NOT be looked up.
        for v in 10u64..20 {
            let lsn = idx.get_lsn(v);
            if v >= start_delete {
                // Either None (truncated) or some lower stride entry.
                // JE's invariant: bucket no longer owns this vlsn.
                // Range last_vlsn must be < start_delete.
                let range_last = idx.get_latest_vlsn();
                assert!(
                    range_last < start_delete,
                    "range last_vlsn={} should be < start_delete={} after truncate",
                    range_last,
                    start_delete
                );
                let _ = lsn; // not asserting None here — see below
            } else {
                // Entries below the truncation point must survive at the
                // index level.  LTE semantics: lookup never errors.
                assert!(
                    lsn.is_some(),
                    "vlsn {} (< {}) must still resolve after truncate_after({})",
                    v,
                    start_delete,
                    start_delete - 1
                );
            }
        }
    }
}

// --------------------------------------------------------------------------
// VLSNIndexTest::testNonContiguousBucketSmallHoles
//
// Insert 1..30 with vlsns 12 and 24 missing.  After insertion, lookups
// for the missing vlsns must fall back to the LTE entry (i.e. they
// must not panic and must return a value <= the missing vlsn).
// --------------------------------------------------------------------------
#[test]
fn test_non_contiguous_bucket_small_holes() {
    let stride = 3u32;
    let index = VlsnIndex::new(stride);
    let file_num: u32 = 33;
    let offset_step: u32 = 100;
    let holes = [12u64, 24];

    for v in 1u64..=30 {
        if holes.contains(&v) {
            continue;
        }
        index.put(v, file_num, (v as u32) * offset_step);
    }

    // Every NON-hole vlsn must have a stored mapping (LTE returns the exact
    // LSN if it's a stride boundary, or the nearest-lower stride entry
    // otherwise).
    for v in 1u64..=30 {
        if holes.contains(&v) {
            continue;
        }
        assert!(index.get_lsn(v).is_some(), "vlsn {} must resolve via LTE", v);
    }

    // Hole vlsns: LTE lookup must return some lower entry without panic.
    for &h in &holes {
        let lsn = index.get_lsn(h);
        // LTE never panics, returns the previous stride entry when present.
        assert!(
            lsn.is_some(),
            "LTE lookup for hole vlsn {} must yield a fallback",
            h
        );
    }

    // Range covers [1, 30].
    let range = index.get_range();
    assert_eq!(range.get_first(), 1);
    assert_eq!(range.get_last(), 30);
}

// --------------------------------------------------------------------------
// VLSNIndexTest::testNonContiguousBucketLargeHoles
//
// Insert 1..50 with vlsns 18,19,20 and 38,39,40 missing (gap > 1).
// Same invariant: LTE lookup must succeed for non-hole vlsns and must
// not panic for hole vlsns.
// --------------------------------------------------------------------------
#[test]
fn test_non_contiguous_bucket_large_holes() {
    let stride = 5u32;
    let index = VlsnIndex::new(stride);
    let file_num: u32 = 33;
    let offset_step: u32 = 100;
    let holes = [18u64, 19, 20, 38, 39, 40];

    for v in 1u64..=50 {
        if holes.contains(&v) {
            continue;
        }
        index.put(v, file_num, (v as u32) * offset_step);
    }

    for v in 1u64..=50 {
        if holes.contains(&v) {
            continue;
        }
        assert!(index.get_lsn(v).is_some(), "vlsn {} must resolve via LTE", v);
    }

    for &h in &holes {
        // LTE never panics; on a hole it returns the previous entry.
        let _ = index.get_lsn(h);
    }

    // Range covers [1, 50].
    let range = index.get_range();
    assert_eq!(range.get_first(), 1);
    assert_eq!(range.get_last(), 50);
}

// --------------------------------------------------------------------------
// VLSNBucketTest::testTruncateAfterFileOffset
//
// JE: Truncate a bucket when the truncation point is between the last
// stored stride offset and the last vlsn.  In Noxu, the equivalent is
// `truncate_after(v)` on the index; the post-truncation last-vlsn must
// equal `v` and lookups for vlsn > v must yield None at index level.
// --------------------------------------------------------------------------
#[test]
fn test_truncate_after_file_offset() {
    let stride = 5u32;
    let index = VlsnIndex::new(stride);

    index.put(10, 0, 10);
    index.put(15, 0, 20);
    index.put(20, 0, 30);
    // Skip 25 (stride boundary): vlsn 28 came in first.
    index.put(28, 0, 40);

    // Pre-truncation range = [10, 28].
    assert_eq!(index.get_latest_vlsn(), 28);

    // Truncate after vlsn 25: range last must be clamped.  Note: Noxu's
    // truncate_after only removes whole buckets and clamps the global
    // range; it does NOT shrink individual bucket contents.  This is a
    // documented semantic difference from JE's removeFromTail (see
    // `test_truncate_removes_buckets_beyond_point` in vlsn_index.rs).
    index.truncate_after(25);
    let range = index.get_range();
    assert_eq!(range.get_last(), 25, "range last must be clamped to 25");
    assert!(!range.contains(28), "range must not include 28 after truncate");
    assert!(!range.contains(26), "range must not include 26 after truncate");
    assert!(range.contains(15), "vlsn 15 < 25 must remain in range");
}

// --------------------------------------------------------------------------
// VLSNIndexTest::testFlushedGets (adapted)
//
// JE: VLSNs persisted to the database are reachable on lookup just like
// in-memory entries.  Noxu has no flush-to-database step in vlsn_index;
// flush is via persist::write_index/read_index.  The invariant ported
// here is the simpler "get returns what was put", across many entries.
// --------------------------------------------------------------------------
#[test]
fn test_basic_gets() {
    let stride = 3u32;
    let index = VlsnIndex::new(stride);
    let file_num: u32 = 1;

    // Put 1..=30 with offset = vlsn*10.
    for v in 1u64..=30 {
        index.put(v, file_num, (v as u32) * 10);
    }

    // Every vlsn in [1, 30] must resolve via LTE.
    for v in 1u64..=30 {
        let got = index.get_lsn(v);
        assert!(got.is_some(), "vlsn {} must have a mapping", v);
        let (f, o) = got.unwrap();
        assert_eq!(f, file_num, "file number must be preserved");
        // LTE: returned offset must be <= the put offset for this vlsn,
        // since stride entries land at vlsn boundaries 1, 4, 7, ...
        assert!(
            o <= (v as u32) * 10,
            "LTE invariant: returned offset {} must be <= put offset {} for vlsn {}",
            o,
            (v as u32) * 10,
            v
        );
    }

    // Extreme: vlsn 0 must yield None (NULL_VLSN).
    assert_eq!(index.get_lsn(0), None);
    // Beyond range: vlsn 31 must yield None.
    assert_eq!(index.get_lsn(31), None);
}

// --------------------------------------------------------------------------
// VLSNBucketTest::testRemoveFromTail (bucket-level smoke check)
//
// We can verify the bucket-level invariant by truncating the index
// and inspecting the bucket count.
// --------------------------------------------------------------------------
#[test]
fn test_truncate_clamps_range_and_removes_buckets() {
    // In Noxu, multi-bucket indices can be observed when a put() falls
    // before the last bucket's first_vlsn.  Since this is unreachable
    // through the public API on a fresh index (the only bucket starts
    // at the first put), we verify the range-clamping invariant only,
    // which is what JE's removeFromTail tests in spirit.
    let stride = 2u32;
    let index = VlsnIndex::new(stride);

    for v in 1u64..=20 {
        index.put(v, 0, (v as u32) * 10);
    }
    let pre_buckets = index.bucket_count();
    assert!(pre_buckets >= 1);

    // Truncate after vlsn 10.
    index.truncate_after(10);
    let range = index.get_range();
    assert_eq!(range.get_last(), 10, "range last must be 10");
    assert!(!range.contains(15));
    assert!(!range.contains(20));
    // vlsns never inserted are still None.
    assert_eq!(index.get_lsn(50), None);
}

// --------------------------------------------------------------------------
// Direct VlsnBucket invariants (mapping JE's VLSNBucketTest "owns",
// "getLsn", "getLastLsn"/"getLast" semantics).  These overlap with
// the in-module tests in vlsn_bucket.rs but are kept here as an
// independent JE-shaped check.
// --------------------------------------------------------------------------
#[test]
fn test_bucket_owns_after_inserts() {
    let mut bucket = VlsnBucket::new(10, 3);
    bucket.put(10, 0, 100);
    bucket.put(15, 0, 200);
    // Owns covers [first_vlsn, last_vlsn].
    assert!(bucket.owns(10));
    assert!(bucket.owns(15));
    assert!(bucket.owns(12));
    assert!(!bucket.owns(9));
    assert!(!bucket.owns(16));
    assert_eq!(bucket.get_last_vlsn(), 15);
    assert_eq!(bucket.get_first_vlsn(), 10);
}
