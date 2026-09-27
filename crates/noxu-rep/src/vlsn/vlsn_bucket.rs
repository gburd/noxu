//! VLSN-to-LSN mapping bucket.
//!
//! A bucket maps a
//! contiguous range of VLSNs to their log positions (LSNs). As a tradeoff
//! between space and time, a bucket only stores a sparse set of mappings
//! at stride intervals. The caller must use a log reader to scan for any
//! log entries not directly mapped by the bucket.
//!
//! Each bucket covers VLSNs from a single log file. The VLSN is not stored
//! directly; instead, only the file offset portion of the LSN is stored,
//! and the VLSN is deduced from the stride and first_vlsn.

/// A bucket mapping a contiguous range of VLSNs to their log positions (LSNs).
///
/// Stores sparse mappings at stride intervals. For example, with stride=4 and
/// first_vlsn=9, the bucket stores offsets for VLSNs 9, 13, 17, ... The last
/// VLSN mapping is always stored regardless of stride alignment.
///
///
#[derive(Debug, Clone)]
pub struct VlsnBucket {
    /// First VLSN covered by this bucket. 0 means uninitialized.
    first_vlsn: u64,
    /// Last VLSN covered by this bucket. 0 means uninitialized.
    last_vlsn: u64,
    /// Interval between VLSN values that are mapped in the offsets array.
    stride: u32,
    /// Sparse mapping: file offsets at stride boundaries.
    /// `offsets[i]` is the (file_number, file_offset) for VLSN
    /// `first_vlsn + i * stride`.
    /// A value of `(0, 0)` indicates an unpopulated slot (NO_OFFSET).
    offsets: Vec<(u32, u32)>,
    /// The LSN for the last VLSN, which may not be on a stride boundary.
    last_lsn: Option<(u32, u32)>,
}

/// Sentinel value indicating an unpopulated offset slot.
const NO_OFFSET: (u32, u32) = (0, 0);

impl VlsnBucket {
    /// Create a new bucket starting at the given VLSN with the given stride.
    ///
    /// The offsets array is initialized with a single empty slot for
    /// `first_vlsn`.
    pub fn new(first_vlsn: u64, stride: u32) -> Self {
        assert!(first_vlsn > 0, "first_vlsn must be > 0");
        assert!(stride > 0, "stride must be > 0");
        VlsnBucket {
            first_vlsn,
            last_vlsn: first_vlsn,
            stride,
            offsets: vec![NO_OFFSET],
            last_lsn: None,
        }
    }

    /// Return the first VLSN covered by this bucket.
    pub fn get_first_vlsn(&self) -> u64 {
        self.first_vlsn
    }

    /// Return the last VLSN covered by this bucket.
    pub fn get_last_vlsn(&self) -> u64 {
        self.last_vlsn
    }

    /// Return the stride interval.
    pub fn get_stride(&self) -> u32 {
        self.stride
    }

    /// Return true if this bucket has no actual LSN mappings stored.
    pub fn is_empty(&self) -> bool {
        self.last_lsn.is_none() && self.offsets.iter().all(|o| *o == NO_OFFSET)
    }

    /// Return true if the given VLSN falls on a stride boundary for this
    /// bucket.
    fn is_modulo(&self, vlsn: u64) -> bool {
        (vlsn - self.first_vlsn).is_multiple_of(self.stride as u64)
    }

    /// Return the index into the offsets array for the given VLSN.
    /// The VLSN must be on a stride boundary.
    fn get_index(&self, vlsn: u64) -> usize {
        debug_assert!(self.is_modulo(vlsn));
        ((vlsn - self.first_vlsn) / self.stride as u64) as usize
    }

    /// Put a VLSN->LSN mapping into this bucket.
    ///
    /// Returns `true` if the mapping was accepted, `false` if the VLSN does
    /// not belong in this bucket (e.g., it precedes `first_vlsn`).
    ///
    /// If the VLSN is on a stride boundary, it is stored in the offsets
    /// array. The last VLSN/LSN pair is always tracked regardless of stride.
    ///
    ///
    pub fn put(
        &mut self,
        vlsn: u64,
        file_number: u32,
        file_offset: u32,
    ) -> bool {
        if vlsn < self.first_vlsn {
            return false;
        }

        // Store at stride boundary if applicable.
        if self.is_modulo(vlsn) {
            let index = self.get_index(vlsn);
            let list_len = self.offsets.len();
            if index < list_len {
                self.offsets[index] = (file_number, file_offset);
            } else if index == list_len {
                self.offsets.push((file_number, file_offset));
            } else {
                // Pad with NO_OFFSET for any skipped slots.
                for _ in list_len..index {
                    self.offsets.push(NO_OFFSET);
                }
                self.offsets.push((file_number, file_offset));
            }
        }

        // Track the last VLSN/LSN.
        if vlsn >= self.last_vlsn {
            self.last_vlsn = vlsn;
            self.last_lsn = Some((file_number, file_offset));
        }

        true
    }

    /// Get the LSN for a VLSN.
    ///
    /// If the exact VLSN is the last VLSN, returns its LSN directly.
    /// If the VLSN is on a stride boundary and is stored, returns it.
    /// Otherwise, returns the nearest lower entry that is populated
    /// (less-than-or-equal lookup).
    ///
    /// Returns `None` if the VLSN is not owned by this bucket or no
    /// mapping can be found.
    ///
    /// And `VLSNBucket.getLsn()`.
    pub fn get_lsn(&self, vlsn: u64) -> Option<(u32, u32)> {
        if !self.owns(vlsn) {
            return None;
        }

        // Check if this is the last VLSN.
        if vlsn == self.last_vlsn {
            return self.last_lsn;
        }

        // If on a stride boundary, try exact lookup.
        if self.is_modulo(vlsn) {
            let index = self.get_index(vlsn);
            if index < self.offsets.len() {
                let entry = self.offsets[index];
                if entry != NO_OFFSET {
                    return Some(entry);
                }
            }
        }

        // Fall back to nearest lower (LTE) entry.
        let diff = vlsn - self.first_vlsn;
        let mut index = (diff / self.stride as u64) as usize;
        if index >= self.offsets.len() {
            index = self.offsets.len() - 1;
        }

        // Search backwards for a populated entry.
        for i in (0..=index).rev() {
            if self.offsets[i] != NO_OFFSET {
                return Some(self.offsets[i]);
            }
        }

        None
    }

    /// Check if this bucket owns the given VLSN (i.e., the VLSN falls
    /// within [first_vlsn, last_vlsn]).
    ///
    ///
    pub fn owns(&self, vlsn: u64) -> bool {
        if vlsn == 0 || self.first_vlsn == 0 {
            return false;
        }
        self.first_vlsn <= vlsn && vlsn <= self.last_vlsn
    }

    /// Return the number of offset entries stored (including NO_OFFSET
    /// placeholders).
    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    /// Append every populated (vlsn, file_number, file_offset) entry in this
    /// bucket to `out`, in vlsn-ascending order.
    ///
    /// Used by the VLSN-index persistence layer to snapshot bucket
    /// contents.  Stride-boundary entries are emitted in array order;
    /// the last vlsn (which may not be on a stride boundary) is
    /// emitted last when it differs from the highest stored stride entry.
    pub fn append_entries(&self, out: &mut Vec<(u64, u32, u32)>) {
        for (i, slot) in self.offsets.iter().enumerate() {
            if *slot != NO_OFFSET {
                let vlsn = self.first_vlsn + (i as u64) * (self.stride as u64);
                if vlsn <= self.last_vlsn {
                    out.push((vlsn, slot.0, slot.1));
                }
            }
        }
        if let Some(last) = self.last_lsn {
            // The last vlsn always has a recorded LSN; emit it if it isn't
            // already covered by a stride entry above.
            let already_emitted = (self.last_vlsn - self.first_vlsn)
                .is_multiple_of(self.stride as u64);
            if !already_emitted {
                out.push((self.last_vlsn, last.0, last.1));
            }
        }
    }
    /// Trim this bucket's head so it no longer owns any VLSN `< new_first`,
    /// **preserving the stride grid** so surviving stride-boundary mappings
    /// still resolve.
    ///
    /// Adapted from JE `VLSNBucket.removeFromHead`. JE advances `firstVLSN` to
    /// a stride boundary and spins any sub-stride prefix into a "remainder"
    /// bucket by scanning the log. The Noxu in-memory index has no log to
    /// scan and keeps a single expanding bucket per origin, so we advance
    /// `first_vlsn` only to the **largest stride-grid vlsn `<= new_first`**
    /// (keeping grid alignment intact) and drop the offset slots below it. Any
    /// vlsn in `[grid_first, new_first)` that the bucket still nominally
    /// covers is rejected by the authoritative `VlsnRange` at the index layer,
    /// which is why the index lookups are range-gated. Non-grid survivors
    /// between two stride boundaries resolve via LTE to the surviving lower
    /// boundary — Noxu does not reconstruct JE's per-vlsn ghost mapping (a
    /// documented single-bucket deviation).
    ///
    /// JE: VLSNBucket.removeFromHead (VLSNBucket.java:245-330), adapted.
    pub fn remove_from_head(&mut self, new_first: u64) {
        if new_first <= self.first_vlsn {
            return;
        }
        if new_first > self.last_vlsn {
            // Whole bucket is below the new head; collapse to empty.
            self.first_vlsn = new_first;
            self.last_vlsn = new_first;
            self.offsets = vec![NO_OFFSET];
            self.last_lsn = None;
            return;
        }
        // Largest stride-grid vlsn <= new_first (keeps grid alignment).
        let grid_first = self.first_vlsn
            + ((new_first - self.first_vlsn) / self.stride as u64)
                * self.stride as u64;
        let drop =
            ((grid_first - self.first_vlsn) / self.stride as u64) as usize;
        if drop > 0 && drop < self.offsets.len() {
            self.offsets.drain(0..drop);
        } else if drop >= self.offsets.len() {
            // All stride offsets fall below grid_first; keep just a
            // placeholder for grid_first.
            self.offsets = vec![NO_OFFSET];
        }
        self.first_vlsn = grid_first;
        // last_vlsn / last_lsn are unchanged (always >= new_first > grid_first
        // when the bucket straddles the delete point).
    }

    /// JE `VLSNBucket.getLsn(vlsn)`: return the LSN for an **exact** vlsn
    /// match, or `None` if this bucket has no stored mapping for exactly
    /// `vlsn`.
    ///
    /// Unlike [`Self::get_lsn`] (which is LTE — the nearest preceding
    /// mapping), this is the strict/precise lookup used by
    /// `ForwardVLSNScanner.getPreciseLsn`. It returns a value only when the
    /// vlsn is the last vlsn or falls on a populated stride boundary; an
    /// intermediate vlsn (or an out-of-order hole on a stride boundary)
    /// returns `None`.
    ///
    /// JE: VLSNBucket.getLsn (VLSNBucket.java:566-596).
    pub fn get_exact_lsn(&self, vlsn: u64) -> Option<(u32, u32)> {
        debug_assert!(self.owns(vlsn), "get_exact_lsn on non-owned vlsn");
        if vlsn == self.last_vlsn {
            return self.last_lsn;
        }
        if !self.is_modulo(vlsn) {
            return None;
        }
        let index = self.get_index(vlsn);
        if index >= self.offsets.len() {
            return None;
        }
        let offset = self.offsets[index];
        if offset == NO_OFFSET {
            return None;
        }
        Some(offset)
    }

    /// JE `VLSNBucket.getLTELsn(vlsn)`: the mapping whose vlsn is <= `vlsn`
    /// (the nearest preceding populated mapping). Never returns `None` for a
    /// vlsn this bucket owns, because the first offset is always populated.
    ///
    /// This is exactly the semantics of [`Self::get_lsn`]; provided under the
    /// JE-faithful name so ported tests read naturally.
    ///
    /// JE: VLSNBucket.getLTELsn (VLSNBucket.java:614-650).
    pub fn get_lte_lsn(&self, vlsn: u64) -> Option<(u32, u32)> {
        self.get_lsn(vlsn)
    }

    /// JE `VLSNBucket.getGTELsn(vlsn)`: the mapping whose vlsn is >= `vlsn`
    /// (the nearest following populated mapping). Never returns `None` for a
    /// vlsn this bucket owns, because the last vlsn is always mapped.
    ///
    /// Mirrors the JE algorithm: compute the ceiling stride index
    /// `ceil((vlsn - first_vlsn) / stride)`, clamp to `last_lsn` when the
    /// index runs past the offsets array, then scan **forward** for the next
    /// populated slot (accounting for out-of-order holes). A slot that is
    /// still `NO_OFFSET` after the forward scan falls back to `last_lsn`.
    ///
    /// JE: VLSNBucket.getGTELsn / getGTEIndex / findPopulatedIndex
    /// (VLSNBucket.java:391-471).
    pub fn get_gte_lsn(&self, vlsn: u64) -> Option<(u32, u32)> {
        if !self.owns(vlsn) {
            return None;
        }
        if vlsn == self.last_vlsn {
            return self.last_lsn;
        }
        // getGTEIndex: ceil division toward the next stride boundary. If
        // vlsn <= first_vlsn (can happen for a vlsn that falls between two
        // buckets), the index is 0.
        let index = if vlsn <= self.first_vlsn {
            0usize
        } else {
            let diff = vlsn - self.first_vlsn;
            diff.div_ceil(self.stride as u64) as usize
        };
        if index >= self.offsets.len() {
            return self.last_lsn;
        }
        // findPopulatedIndex forward.
        let mut use_index = index;
        while use_index < self.offsets.len() {
            if self.offsets[use_index] != NO_OFFSET {
                break;
            }
            use_index += 1;
        }
        if use_index >= self.offsets.len()
            || self.offsets[use_index] == NO_OFFSET
        {
            return self.last_lsn;
        }
        Some(self.offsets[use_index])
    }

    /// JE `VLSNBucket.getLastLsn()`: the LSN of the last (highest) vlsn.
    pub fn get_last_lsn(&self) -> Option<(u32, u32)> {
        self.last_lsn
    }

    /// JE `VLSNBucket.removeFromTail(startOfDelete, prevLsn)`: remove all
    /// mappings for vlsns `>= start_of_delete` from this bucket.
    ///
    /// `prev_lsn` is the LSN of `start_of_delete - 1` (i.e. the new last
    /// mapping's LSN). If `prev_lsn` is `None` (JE's `NULL_LSN`), we don't
    /// have a value to cap the bucket, so the bucket is cut back to the
    /// largest known populated stride mapping (which may drop mappings
    /// between the last stride offset and the old last vlsn).
    ///
    /// Mirrors the JE algorithm exactly (including the two
    /// "between last stride and last vlsn" cases and the out-of-order-hole
    /// handling via findPopulatedIndex).
    ///
    /// JE: VLSNBucket.removeFromTail (VLSNBucket.java:735-833).
    pub fn remove_from_tail(
        &mut self,
        start_of_delete: u64,
        prev_lsn: Option<(u32, u32)>,
    ) {
        if self.is_empty() {
            return;
        }
        // Nothing to delete: the whole bucket precedes the delete point.
        if self.last_vlsn < start_of_delete {
            return;
        }
        // Delete all mappings.
        if self.first_vlsn >= start_of_delete {
            self.last_vlsn = self.first_vlsn;
            self.last_lsn = None;
            self.offsets.clear();
            return;
        }

        // getGTEIndex(start_of_delete).
        let diff = start_of_delete - self.first_vlsn;
        let mut delete_index = diff.div_ceil(self.stride as u64) as usize;
        debug_assert!(delete_index > 0, "delete_index must be > 0");

        if delete_index < self.offsets.len() {
            // start_of_delete is between first_vlsn and the last file offset.
            if prev_lsn.is_none() {
                let last_populated =
                    self.find_populated_index_back(delete_index - 1);
                if last_populated != delete_index - 1 {
                    delete_index = last_populated + 1;
                }
            }
            self.offsets.truncate(delete_index);
        } else {
            // start_of_delete is between the last file offset and last vlsn.
            if prev_lsn.is_none() {
                let last_index = self.offsets.len() - 1;
                let last_populated = self.find_populated_index_back(last_index);
                if last_populated < last_index {
                    self.offsets.truncate(last_populated + 1);
                }
            }
        }

        // Set the new last vlsn -> last lsn mapping.
        match prev_lsn {
            None => {
                self.last_vlsn = ((self.offsets.len() - 1) as u64)
                    * (self.stride as u64)
                    + self.first_vlsn;
                let last_offset = self.offsets[self.offsets.len() - 1];
                debug_assert!(last_offset != NO_OFFSET);
                self.last_lsn = Some(last_offset);
            }
            Some(prev) => {
                self.last_vlsn = start_of_delete - 1;
                self.last_lsn = Some(prev);
            }
        }
    }

    /// JE `VLSNBucket.findPopulatedIndex(startIndex, forward=false)`: scan
    /// backward from `start_index` for the first populated offset; if none is
    /// found, return `start_index` unchanged.
    fn find_populated_index_back(&self, start_index: usize) -> usize {
        let mut i = start_index as isize;
        while i >= 0 {
            if self.offsets[i as usize] != NO_OFFSET {
                return i as usize;
            }
            i -= 1;
        }
        start_index
    }
}
impl std::fmt::Display for VlsnBucket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "<VlsnBucket numOffsets={} stride={} firstVlsn={} lastVlsn={} lastLsn={:?}>",
            self.offsets.len(),
            self.stride,
            self.first_vlsn,
            self.last_vlsn,
            self.last_lsn,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_bucket() {
        let bucket = VlsnBucket::new(10, 3);
        assert_eq!(bucket.get_first_vlsn(), 10);
        assert_eq!(bucket.get_last_vlsn(), 10);
        assert_eq!(bucket.get_stride(), 3);
        assert!(bucket.is_empty());
        assert_eq!(bucket.len(), 1);
    }

    #[test]
    fn test_put_first_vlsn() {
        let mut bucket = VlsnBucket::new(10, 3);
        assert!(bucket.put(10, 1, 100));
        assert!(!bucket.is_empty());
        assert_eq!(bucket.get_lsn(10), Some((1, 100)));
    }

    #[test]
    fn test_put_stride_boundary() {
        let mut bucket = VlsnBucket::new(10, 3);
        assert!(bucket.put(10, 1, 100));
        assert!(bucket.put(13, 1, 200));
        assert!(bucket.put(16, 1, 300));
        assert_eq!(bucket.get_lsn(10), Some((1, 100)));
        assert_eq!(bucket.get_lsn(13), Some((1, 200)));
        assert_eq!(bucket.get_lsn(16), Some((1, 300)));
        assert_eq!(bucket.len(), 3);
    }

    #[test]
    fn test_put_non_stride() {
        let mut bucket = VlsnBucket::new(10, 3);
        assert!(bucket.put(10, 1, 100));
        assert!(bucket.put(11, 1, 110));
        assert!(bucket.put(12, 1, 120));
        // VLSN 11 and 12 are not on stride boundaries, so only tracked
        // via last_vlsn.
        assert_eq!(bucket.get_lsn(10), Some((1, 100)));
        // VLSN 12 is the last, so it should be returned.
        assert_eq!(bucket.get_lsn(12), Some((1, 120)));
        // VLSN 11 is not on stride, so nearest lower is 10.
        assert_eq!(bucket.get_lsn(11), Some((1, 100)));
    }

    #[test]
    fn test_put_before_first_vlsn_rejected() {
        let mut bucket = VlsnBucket::new(10, 3);
        assert!(!bucket.put(9, 1, 50));
    }

    #[test]
    fn test_owns() {
        let mut bucket = VlsnBucket::new(10, 3);
        bucket.put(10, 1, 100);
        bucket.put(15, 1, 200);
        assert!(!bucket.owns(0));
        assert!(!bucket.owns(9));
        assert!(bucket.owns(10));
        assert!(bucket.owns(12));
        assert!(bucket.owns(15));
        assert!(!bucket.owns(16));
    }

    #[test]
    fn test_owns_empty() {
        let bucket = VlsnBucket::new(10, 3);
        // Even an empty bucket "owns" VLSNs in [first, last] = [10, 10].
        assert!(bucket.owns(10));
        assert!(!bucket.owns(11));
    }

    #[test]
    fn test_nearest_lower_lookup() {
        // Bucket with stride=5, starting at VLSN 1.
        let mut bucket = VlsnBucket::new(1, 5);
        bucket.put(1, 0, 100);
        bucket.put(6, 0, 200);
        bucket.put(11, 0, 300);
        bucket.put(14, 0, 350); // last VLSN, not on stride

        // VLSN 3 -> nearest lower stride entry is VLSN 1.
        assert_eq!(bucket.get_lsn(3), Some((0, 100)));
        // VLSN 8 -> nearest lower stride entry is VLSN 6.
        assert_eq!(bucket.get_lsn(8), Some((0, 200)));
        // VLSN 14 -> exact last VLSN.
        assert_eq!(bucket.get_lsn(14), Some((0, 350)));
    }

    #[test]
    fn test_out_of_order_puts() {
        // Simulate out-of-order insertion (as described in the equivalent VLSNBucket).
        let mut bucket = VlsnBucket::new(10, 3);
        // Insert VLSN 16 first (skipping 10 and 13).
        assert!(bucket.put(16, 1, 300));
        // The offset array should have been padded.
        assert_eq!(bucket.len(), 3); // indices 0, 1, 2
        // Index 0 and 1 are NO_OFFSET placeholders.
        assert_eq!(bucket.get_lsn(16), Some((1, 300)));

        // Now insert VLSN 10.
        assert!(bucket.put(10, 1, 100));
        assert_eq!(bucket.get_lsn(10), Some((1, 100)));

        // VLSN 13 is still a hole.
        // For VLSN 13, nearest lower is VLSN 10.
        assert_eq!(bucket.get_lsn(13), Some((1, 100)));
    }

    #[test]
    fn test_get_lsn_not_owned() {
        let mut bucket = VlsnBucket::new(10, 3);
        bucket.put(10, 1, 100);
        assert_eq!(bucket.get_lsn(5), None);
        assert_eq!(bucket.get_lsn(11), None);
    }

    #[test]
    fn test_large_stride() {
        let mut bucket = VlsnBucket::new(1, 100);
        bucket.put(1, 0, 10);
        bucket.put(50, 0, 500);
        // VLSN 50 is the last, and only the first stride slot is populated.
        assert_eq!(bucket.get_lsn(1), Some((0, 10)));
        assert_eq!(bucket.get_lsn(50), Some((0, 500)));
        // VLSN 25: nearest lower is VLSN 1.
        assert_eq!(bucket.get_lsn(25), Some((0, 10)));
    }

    #[test]
    fn test_stride_one() {
        let mut bucket = VlsnBucket::new(1, 1);
        for i in 1..=5 {
            bucket.put(i, 0, i as u32 * 100);
        }
        for i in 1..=5 {
            assert_eq!(bucket.get_lsn(i), Some((0, i as u32 * 100)));
        }
        assert_eq!(bucket.len(), 5);
    }

    #[test]
    fn test_display() {
        let bucket = VlsnBucket::new(1, 3);
        let s = format!("{}", bucket);
        assert!(s.contains("VlsnBucket"));
        assert!(s.contains("stride=3"));
    }

    #[test]
    fn test_different_file_numbers() {
        let mut bucket = VlsnBucket::new(1, 2);
        // Put entries from different files.
        bucket.put(1, 0, 100);
        bucket.put(3, 1, 200); // Different file number.
        bucket.put(5, 0, 300);
        // Noxu accepts entries from multiple files in one bucket; constrains
        // each bucket to a single file number for cleaner GC accounting.
        assert_eq!(bucket.get_lsn(1), Some((0, 100)));
        assert_eq!(bucket.get_lsn(3), Some((1, 200)));
        assert_eq!(bucket.get_lsn(5), Some((0, 300)));
    }

    // -------------------------------------------------------------------------
    // Ported from VLSNBucketTest.java
    // -------------------------------------------------------------------------

    // Helper: build the canonical six-entry test dataset.
    // vlsn=1..6, file=3, offset=i*10  → lsn=(3, i*10)
    fn init_data() -> Vec<(u64, u32, u32)> {
        (1u64..=6).map(|i| (i, 3u32, i as u32 * 10)).collect()
    }

    ///
    /// The Rust bucket has no hard maxMappings cap. The key invariants
    /// ported here are:
    ///   - put() returns false for vlsn < first_vlsn (rejected)
    ///   - owns() covers [first_vlsn, last_vlsn]
    ///   - get_lsn() uses LTE (less-than-or-equal) semantics
    ///   - stride boundaries get exact mappings; intermediate vlsns fall
    ///     back to the nearest lower stride entry
    ///   - the last vlsn in the bucket is always stored exactly
    // JE: VLSNBucketTest.testBasic (LTE-only view; the faithful port
    // asserting GTE too is `je_bucket_test_basic_faithful` below).
    #[test]
    fn test_basic() {
        let stride = 3u32;
        let vals = init_data(); // (vlsn, file, offset)

        // first_vlsn = vals[0].vlsn = 1
        let mut bucket = VlsnBucket::new(vals[0].0, stride);

        // Initially empty.
        assert!(bucket.is_empty());

        // Insert vlsn 1.
        assert!(bucket.put(vals[0].0, vals[0].1, vals[0].2));
        assert!(!bucket.is_empty());

        // Insert vlsn 2.
        assert!(bucket.put(vals[1].0, vals[1].1, vals[1].2));

        // Reject a VLSN before first_vlsn (i.e. 0).
        assert!(!bucket.put(0, 3, 99));

        // vlsn 1 and 2 are owned (in [first=1, last=2]);
        // vlsn 3 not yet inserted so last_vlsn=2.
        assert!(bucket.owns(vals[0].0)); // vlsn 1
        assert!(bucket.owns(vals[1].0)); // vlsn 2
        assert!(!bucket.owns(vals[2].0)); // vlsn 3 not yet inserted

        // Insert vlsn 3 — now it becomes last_vlsn.
        assert!(bucket.put(vals[2].0, vals[2].1, vals[2].2));

        // LTE semantics after inserting vlsns 1,2,3 with stride=3:
        //   stride boundaries: vlsn 1 (index 0) — that is all so far.
        //   last_vlsn = 3 (always stored).
        //   vlsn 1 → exact stride boundary
        assert_eq!(bucket.get_lsn(vals[0].0), Some((vals[0].1, vals[0].2)));
        //   vlsn 2 → not a boundary; last_vlsn is 3, so falls back to stride 1
        assert_eq!(bucket.get_lsn(vals[1].0), Some((vals[0].1, vals[0].2)));
        //   vlsn 3 → last_vlsn, always exact
        assert_eq!(bucket.get_lsn(vals[2].0), Some((vals[2].1, vals[2].2)));

        // Insert vlsns 4, 5, 6.
        assert!(bucket.put(vals[3].0, vals[3].1, vals[3].2));
        assert!(bucket.put(vals[4].0, vals[4].1, vals[4].2));
        assert!(bucket.put(vals[5].0, vals[5].1, vals[5].2));

        check_access(&bucket, stride as u64, &vals);
    }

    /// Verify LTE access semantics for a bucket holding vlsns 1-6 with
    /// stride=3. Stored stride boundaries are at vlsns 1 and 4; the last
    /// vlsn (6) is always stored exactly.
    ///
    /// LTE(v) = the LSN for the largest stride entry whose vlsn <= v,
    ///          or the last_vlsn entry if v == last_vlsn.
    fn check_access(
        bucket: &VlsnBucket,
        stride: u64,
        vals: &[(u64, u32, u32)],
    ) {
        // --- exact stride-boundary hits ---
        for i in (0..vals.len()).step_by(stride as usize) {
            let (vlsn, file, off) = vals[i];
            assert!(bucket.owns(vlsn));
            assert_eq!(bucket.get_lsn(vlsn), Some((file, off)));
        }

        // --- LTE checks (all 6 vlsns) ---
        // vlsn 1 → stride boundary 1
        assert_eq!(bucket.get_lsn(vals[0].0), Some((vals[0].1, vals[0].2)));
        // vlsn 2 → no stride entry; LTE falls back to stride 1
        assert_eq!(bucket.get_lsn(vals[1].0), Some((vals[0].1, vals[0].2)));
        // vlsn 3 → no stride entry; LTE falls back to stride 1
        assert_eq!(bucket.get_lsn(vals[2].0), Some((vals[0].1, vals[0].2)));
        // vlsn 4 → exact stride boundary 4
        assert_eq!(bucket.get_lsn(vals[3].0), Some((vals[3].1, vals[3].2)));
        // vlsn 5 → no stride entry; LTE falls back to stride 4
        assert_eq!(bucket.get_lsn(vals[4].0), Some((vals[3].1, vals[3].2)));
        // vlsn 6 → last_vlsn, always exact
        assert_eq!(bucket.get_lsn(vals[5].0), Some((vals[5].1, vals[5].2)));
    }

    /// Vlsns inserted in
    /// non-monotonic order; ownership and LTE lookup must still be correct.
    // JE: VLSNBucketTest.testOutOfOrderPuts (LTE-only view; faithful GTE
    // port is `je_bucket_test_out_of_order_puts_faithful` below).
    #[test]
    fn test_out_of_order_puts_with_stride() {
        let stride = 3u32;
        let vals = init_data();
        let mut bucket = VlsnBucket::new(vals[0].0, stride);

        // Insert vlsn 2, then 1 (out of order).
        assert!(bucket.is_empty());
        assert!(bucket.put(vals[1].0, vals[1].1, vals[1].2)); // vlsn 2 first
        assert!(!bucket.is_empty());

        // After inserting vlsn 2, bucket owns [1..2] (first=1, last=2).
        assert!(bucket.owns(vals[1].0)); // vlsn 2
        assert!(bucket.owns(vals[0].0)); // vlsn 1 (in [first,last])
        assert!(!bucket.owns(vals[2].0)); // vlsn 3 not yet

        assert!(bucket.put(vals[0].0, vals[0].1, vals[0].2)); // vlsn 1

        // Reject VLSN from before first_vlsn.
        assert!(!bucket.put(0, 4, 20));

        // Still only owns up to current last (vlsn 2).
        assert!(!bucket.owns(vals[2].0));

        // Insert the remaining vlsns out of order: 5,6,3,4.
        assert!(bucket.put(vals[4].0, vals[4].1, vals[4].2)); // vlsn 5
        assert!(bucket.put(vals[5].0, vals[5].1, vals[5].2)); // vlsn 6
        assert!(bucket.put(vals[2].0, vals[2].1, vals[2].2)); // vlsn 3
        assert!(bucket.put(vals[3].0, vals[3].1, vals[3].2)); // vlsn 4

        check_access(&bucket, stride as u64, &vals);
    }

    /// Bucket with holes
    /// (out-of-order puts that leave unpopulated stride slots), checking
    /// that LTE and GTE semantics work correctly around holes.
    // JE: VLSNBucketTest.testGetNonNullWithHoles (LTE-only view; faithful
    // GTE+LTE port is `je_bucket_test_get_non_null_with_holes_faithful`).
    #[test]
    fn test_get_non_null_with_holes() {
        // stride=2, file=0
        let mut bucket = VlsnBucket::new(1, 2);
        assert!(bucket.put(1, 0, 10));
        assert!(bucket.put(3, 0, 30));
        // Jump directly to vlsn 6 — creates holes at stride positions 2 and 4.
        assert!(bucket.put(6, 0, 60));

        // LTE checks: return the nearest at-or-below populated entry.
        assert_eq!(bucket.get_lsn(1), Some((0, 10)));
        assert_eq!(bucket.get_lsn(2), Some((0, 10))); // stride hole → fall back to 1
        assert_eq!(bucket.get_lsn(3), Some((0, 30)));
        assert_eq!(bucket.get_lsn(4), Some((0, 30))); // stride hole → fall back to 3
        assert_eq!(bucket.get_lsn(5), Some((0, 30))); // not owned if last=6 tracks 6 only;
        // 5 is between stride 5 (empty) and last 6
        assert_eq!(bucket.get_lsn(6), Some((0, 60)));
    }

    /// After truncation the
    /// VLSNs at or beyond the truncation point must no longer be owned.
    /// We simulate this by verifying ownership after truncating the bucket
    /// range through the index (VlsnIndex::truncate_after).
    #[test]
    fn test_ownership_boundary() {
        // Build a bucket with vlsns 10-19 at stride=3, file=0.
        let mut bucket = VlsnBucket::new(10, 3);
        for i in 10u64..20 {
            assert!(bucket.put(i, 0, i as u32 * 10));
        }
        // All 10-19 should be owned.
        for i in 10u64..20 {
            assert!(bucket.owns(i), "expected owns({})", i);
        }
        // 9 and 20 are outside.
        assert!(!bucket.owns(9));
        assert!(!bucket.owns(20));
    }

    /// Verifies that
    /// the last tracked vlsn after insert is correct, including when the
    /// last insert is not on a stride boundary.
    #[test]
    fn test_last_vlsn_tracking() {
        let mut bucket = VlsnBucket::new(10, 5);
        bucket.put(10, 0, 10);
        assert_eq!(bucket.get_last_vlsn(), 10);

        bucket.put(15, 0, 20);
        assert_eq!(bucket.get_last_vlsn(), 15);

        bucket.put(20, 0, 30);
        assert_eq!(bucket.get_last_vlsn(), 20);

        // Insert vlsn 28 before 25 (out of order); last should become 28.
        bucket.put(28, 0, 40);
        assert_eq!(bucket.get_last_vlsn(), 28);

        // Now insert 25 (back-fill); last stays 28.
        bucket.put(25, 0, 35);
        assert_eq!(bucket.get_last_vlsn(), 28);

        // LTE for vlsn 26 should be the entry at 25 (stride=5, boundary 25).
        assert_eq!(bucket.get_lsn(25), Some((0, 35)));
        // LTE for vlsn 26 still returns 25 entry.
        assert_eq!(bucket.get_lsn(26), Some((0, 35)));
        // LTE for vlsn 28 returns last.
        assert_eq!(bucket.get_lsn(28), Some((0, 40)));
    }

    /// First_vlsn is always returned exactly.
    #[test]
    fn test_first_vlsn_exact_lookup() {
        let mut bucket = VlsnBucket::new(1, 3);
        bucket.put(1, 3, 10);
        bucket.put(2, 3, 20);
        bucket.put(3, 3, 30);
        assert_eq!(bucket.get_lsn(1), Some((3, 10)));
    }

    /// Verify get_first_vlsn / get_last_vlsn match bucket contents.
    #[test]
    fn test_first_last_accessors() {
        let mut bucket = VlsnBucket::new(5, 2);
        assert_eq!(bucket.get_first_vlsn(), 5);
        assert_eq!(bucket.get_last_vlsn(), 5);
        bucket.put(5, 0, 50);
        bucket.put(7, 0, 70);
        bucket.put(9, 0, 90);
        assert_eq!(bucket.get_first_vlsn(), 5);
        assert_eq!(bucket.get_last_vlsn(), 9);
    }

    // =========================================================================
    // FAITHFUL ports of VLSNBucketTest.java asserting the full JE access
    // semantics: get_exact_lsn (JE getLsn), get_lte_lsn (JE getLTELsn) and
    // get_gte_lsn (JE getGTELsn). The Noxu bucket has no hard maxMappings /
    // maxDistance cap and accepts entries from any file (a documented
    // deviation — Noxu constrains a bucket to one file at the index layer,
    // not the bucket), so those refusal assertions from JE are not ported;
    // every mapping-semantics assertion IS ported verbatim.
    // =========================================================================

    // (vlsn, file, offset) for vlsn=1..6, file=3, offset=i*10  (JE initData).
    fn je_init_data() -> Vec<(u64, u32, u32)> {
        (1u64..=6).map(|i| (i, 3u32, i as u32 * 10)).collect()
    }

    /// JE VLSNBucketTest.checkAccess — full get/getLTE/getGTE table for a
    /// bucket holding vlsns 1..6 at stride 3 (stored strides: vlsn 1, 4;
    /// last vlsn 6 always stored).
    fn je_check_access(bucket: &VlsnBucket, vals: &[(u64, u32, u32)]) {
        // exact stride-boundary hits (i += stride).
        let stride = bucket.get_stride() as usize;
        for i in (0..vals.len()).step_by(stride) {
            let (vlsn, file, off) = vals[i];
            assert!(bucket.owns(vlsn));
            assert_eq!(bucket.get_exact_lsn(vlsn), Some((file, off)));
        }

        // getLTELsn table (JE checkAccess).
        assert_eq!(bucket.get_lte_lsn(vals[0].0), Some((vals[0].1, vals[0].2)));
        assert_eq!(bucket.get_lte_lsn(vals[1].0), Some((vals[0].1, vals[0].2)));
        assert_eq!(bucket.get_lte_lsn(vals[2].0), Some((vals[0].1, vals[0].2)));
        assert_eq!(bucket.get_lte_lsn(vals[3].0), Some((vals[3].1, vals[3].2)));
        assert_eq!(bucket.get_lte_lsn(vals[4].0), Some((vals[3].1, vals[3].2)));
        assert_eq!(bucket.get_lte_lsn(vals[5].0), Some((vals[5].1, vals[5].2)));

        // getGTELsn table (JE checkAccess).
        assert_eq!(bucket.get_gte_lsn(vals[0].0), Some((vals[0].1, vals[0].2)));
        assert_eq!(bucket.get_gte_lsn(vals[1].0), Some((vals[3].1, vals[3].2)));
        assert_eq!(bucket.get_gte_lsn(vals[2].0), Some((vals[3].1, vals[3].2)));
        assert_eq!(bucket.get_gte_lsn(vals[3].0), Some((vals[3].1, vals[3].2)));
        assert_eq!(bucket.get_gte_lsn(vals[4].0), Some((vals[5].1, vals[5].2)));
        assert_eq!(bucket.get_gte_lsn(vals[5].0), Some((vals[5].1, vals[5].2)));
    }

    /// JE: VLSNBucketTest.testBasic (faithful — exact/LTE/GTE).
    #[test]
    fn je_bucket_test_basic_faithful() {
        let vals = je_init_data();
        let mut bucket = VlsnBucket::new(vals[0].0, 3);

        assert!(bucket.is_empty());
        assert!(bucket.put(vals[0].0, vals[0].1, vals[0].2)); // vlsn 1
        assert!(!bucket.is_empty());
        assert!(bucket.put(vals[1].0, vals[1].1, vals[1].2)); // vlsn 2

        assert!(bucket.owns(vals[0].0));
        assert!(bucket.owns(vals[1].0));
        assert!(!bucket.owns(vals[2].0)); // vlsn 3 not yet in range

        assert!(bucket.put(vals[2].0, vals[2].1, vals[2].2)); // vlsn 3

        // JE: getGTELsn table before the bucket fills up.
        //   only stride entry so far is vlsn 1 (index 0); last vlsn is 3.
        assert_eq!(bucket.get_gte_lsn(vals[0].0), Some((vals[0].1, vals[0].2)));
        // GTE(2): ceil(1/3)=index1 (empty) -> forward none -> last (vlsn3).
        assert_eq!(bucket.get_gte_lsn(vals[1].0), Some((vals[2].1, vals[2].2)));
        // GTE(3) == last vlsn.
        assert_eq!(bucket.get_gte_lsn(vals[2].0), Some((vals[2].1, vals[2].2)));

        // Only one stride offset stored (vlsn 1).
        assert_eq!(bucket.len(), 1);

        // Fill up: 4, 5, 6.
        assert!(bucket.put(vals[3].0, vals[3].1, vals[3].2));
        assert!(bucket.put(vals[4].0, vals[4].1, vals[4].2));
        assert!(bucket.put(vals[5].0, vals[5].1, vals[5].2));

        je_check_access(&bucket, &vals);
    }

    /// JE: VLSNBucketTest.testOutOfOrderPuts (faithful — exact/LTE/GTE).
    #[test]
    fn je_bucket_test_out_of_order_puts_faithful() {
        let vals = je_init_data();
        let mut bucket = VlsnBucket::new(vals[0].0, 3);

        assert!(bucket.is_empty());
        assert!(bucket.put(vals[1].0, vals[1].1, vals[1].2)); // vlsn 2 first
        assert!(!bucket.is_empty());

        assert!(bucket.owns(vals[1].0)); // vlsn 2
        assert!(bucket.owns(vals[0].0)); // vlsn 1 (in [first,last])
        assert!(!bucket.owns(vals[2].0)); // vlsn 3

        assert!(bucket.put(vals[0].0, vals[0].1, vals[0].2)); // vlsn 1

        assert!(!bucket.owns(vals[2].0));
        assert_eq!(bucket.len(), 1);

        // Fill up out of order: 5,6,3,4.
        assert!(bucket.put(vals[4].0, vals[4].1, vals[4].2)); // 5
        assert!(bucket.put(vals[5].0, vals[5].1, vals[5].2)); // 6
        assert!(bucket.put(vals[2].0, vals[2].1, vals[2].2)); // 3
        assert!(bucket.put(vals[3].0, vals[3].1, vals[3].2)); // 4

        je_check_access(&bucket, &vals);
    }

    /// JE: VLSNBucketTest.testGetNonNullWithHoles (faithful — exact GTE+LTE
    /// table with out-of-order holes, stride=2, file=0).
    #[test]
    fn je_bucket_test_get_non_null_with_holes_faithful() {
        let mut bucket = VlsnBucket::new(1, 2);
        assert!(bucket.put(1, 0, 10));
        assert!(bucket.put(3, 0, 30));
        // Jump to vlsn 6 -> file-offset array stays size 2 (edge case for
        // the "too-small array" LTE/GTE handling).
        assert!(bucket.put(6, 0, 60));

        // getLTELsn table (JE).
        assert_eq!(bucket.get_lte_lsn(1), Some((0, 10)));
        assert_eq!(bucket.get_lte_lsn(2), Some((0, 10)));
        assert_eq!(bucket.get_lte_lsn(3), Some((0, 30)));
        assert_eq!(bucket.get_lte_lsn(4), Some((0, 30)));
        assert_eq!(bucket.get_lte_lsn(5), Some((0, 30)));
        assert_eq!(bucket.get_lte_lsn(6), Some((0, 60)));

        // getGTELsn table (JE).
        assert_eq!(bucket.get_gte_lsn(1), Some((0, 10)));
        assert_eq!(bucket.get_gte_lsn(2), Some((0, 30)));
        assert_eq!(bucket.get_gte_lsn(3), Some((0, 30)));
        assert_eq!(bucket.get_gte_lsn(4), Some((0, 60)));
        assert_eq!(bucket.get_gte_lsn(5), Some((0, 60)));
        assert_eq!(bucket.get_gte_lsn(6), Some((0, 60)));
    }

    // Build a bucket loaded with the given (vlsn,file,offset) mappings, like
    // JE VLSNBucketTest.loadBucket.
    fn je_load_bucket(
        mappings: &[(u64, u32, u32)],
        stride: u32,
        first_vlsn: u64,
    ) -> VlsnBucket {
        let mut bucket = VlsnBucket::new(first_vlsn, stride);
        for &(vlsn, f, off) in mappings {
            assert!(bucket.put(vlsn, f, off), "put {} rejected", vlsn);
        }
        bucket
    }

    /// JE: VLSNBucketTest.testRemoveFromTail. Load vlsns 10..19 at stride 3,
    /// file 0, offset=vlsn*10, then removeFromTail at every point in
    /// [start-1, end] and check the surviving mappings match JE's rules.
    #[test]
    fn je_bucket_test_remove_from_tail() {
        let stride = 3u32;
        let start = 10u64;
        let end = 20u64; // exclusive (vlsns 10..=19)
        let expected: Vec<(u64, u32, u32)> =
            (start..end).map(|i| (i, 0u32, i as u32 * 10)).collect();

        for start_delete in (start - 1)..(end + 1) {
            let mut bucket = je_load_bucket(&expected, stride, start);
            // prevLsn = (startDelete-1)*10 on file 0  (JE passes this).
            let prev = ((start_delete - 1) as u32) * 10;
            bucket.remove_from_tail(start_delete, Some((0, prev)));

            for &(vlsn, f, off) in &expected {
                let lsn = if bucket.owns(vlsn) {
                    bucket.get_exact_lsn(vlsn)
                } else {
                    None
                };

                if vlsn >= start_delete {
                    // Anything >= startDelete should be truncated.
                    assert_eq!(
                        lsn, None,
                        "startDelete={} vlsn={} should be truncated",
                        start_delete, vlsn
                    );
                } else if (vlsn - start).is_multiple_of(stride as u64) {
                    // On a stride boundary -> exact mapping present.
                    assert_eq!(
                        lsn,
                        Some((f, off)),
                        "stride vlsn {} should map (startDelete={})",
                        vlsn,
                        start_delete
                    );
                } else if vlsn == start_delete - 1 {
                    // It's the last mapping (capped by prevLsn).
                    assert_eq!(
                        bucket.get_last_lsn(),
                        Some((0, prev)),
                        "last mapping should be prevLsn"
                    );
                } else {
                    // Non-stride, non-last -> no exact mapping.
                    assert_eq!(
                        lsn, None,
                        "non-stride vlsn {} exact=None",
                        vlsn
                    );
                }
            }
        }
    }

    /// JE: VLSNBucketTest.testTruncateAfterFileOffset [#20796] — truncate when
    /// the truncation point lies between the last stored file offset and the
    /// last vlsn.
    #[test]
    fn je_bucket_test_truncate_after_file_offset() {
        // stride 5, first 10. Mappings: 10/0/10, 15/0/20, 20/0/30, and 28/0/40
        // (25 skipped: vlsn 28 arrived out of order before 25).
        let mappings: &[(u64, u32, u32)] =
            &[(10, 0, 10), (15, 0, 20), (20, 0, 30), (28, 0, 40)];

        // Case 1: prevLsn = NULL -> cut back to the largest stride mapping.
        let mut bucket = je_load_bucket(mappings, 5, 10);
        bucket.remove_from_tail(26, None);
        assert_eq!(bucket.get_last_vlsn(), 20);
        assert_eq!(bucket.get_last_lsn(), Some((0, 30)));

        // Case 2: prevLsn = 0/33 -> cap the bucket at vlsn 25 -> 0/33.
        let mut bucket = je_load_bucket(mappings, 5, 10);
        bucket.remove_from_tail(26, Some((0, 33)));
        assert_eq!(bucket.get_last_vlsn(), 25);
        assert_eq!(bucket.get_last_lsn(), Some((0, 33)));
    }
}
