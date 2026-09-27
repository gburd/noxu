//! Sparse long-indexed bitmap.
//!
//! Port of JE's `BitMap`, which supports indexing with `long` arguments by
//! keeping a map of fixed-size bitset segments (each covering 2^16 bits), so
//! the map may be sparse — a segment is only instantiated when needed.  JE
//! uses it in `DbScavenger` (log salvage).
//!
//! JE ref: `com.sleepycat.je.utilint.BitMap`.
//!
//! Note: like the JE original, this is not thread-safe.

use hashbrown::HashMap;

/// Each segment covers `2^SEGMENT_SIZE` (= 65536) bit indices.
const SEGMENT_SIZE: u32 = 16;
const SEGMENT_MASK: i64 = 0xffff;
/// Bits per `u64` word inside a segment.
const WORD_BITS: usize = 64;
/// Number of `u64` words needed to cover one segment (65536 / 64 = 1024).
const WORDS_PER_SEGMENT: usize = (1 << SEGMENT_SIZE) / WORD_BITS;

/// A bitmap indexed by non-negative `i64` values, stored as sparse segments.
#[derive(Debug, Default)]
pub struct BitMap {
    /// Map of segment id (`index >> 16`) -> bitset segment.
    bit_segments: HashMap<i64, Box<[u64; WORDS_PER_SEGMENT]>>,
}

impl BitMap {
    /// Creates an empty bitmap.
    pub fn new() -> Self {
        BitMap { bit_segments: HashMap::new() }
    }

    /// Sets the bit at `index`.
    ///
    /// # Panics
    ///
    /// Panics (JE throws `IndexOutOfBoundsException`) if `index` is negative.
    pub fn set(&mut self, index: i64) {
        assert!(index >= 0, "{index} is negative.");
        let segment_id = index >> SEGMENT_SIZE;
        let word = (index & SEGMENT_MASK) as usize / WORD_BITS;
        let bit = (index & SEGMENT_MASK) as usize % WORD_BITS;
        let seg = self
            .bit_segments
            .entry(segment_id)
            .or_insert_with(|| Box::new([0u64; WORDS_PER_SEGMENT]));
        seg[word] |= 1u64 << bit;
    }

    /// Returns whether the bit at `index` is set.
    ///
    /// # Panics
    ///
    /// Panics (JE throws `IndexOutOfBoundsException`) if `index` is negative.
    pub fn get(&self, index: i64) -> bool {
        assert!(index >= 0, "{index} is negative.");
        let segment_id = index >> SEGMENT_SIZE;
        match self.bit_segments.get(&segment_id) {
            None => false,
            Some(seg) => {
                let word = (index & SEGMENT_MASK) as usize / WORD_BITS;
                let bit = (index & SEGMENT_MASK) as usize % WORD_BITS;
                (seg[word] & (1u64 << bit)) != 0
            }
        }
    }

    /// Returns the number of instantiated segments (for testing).
    pub fn num_segments(&self) -> usize {
        self.bit_segments.len()
    }

    /// Returns the total number of set bits.
    pub fn cardinality(&self) -> usize {
        self.bit_segments
            .values()
            .map(|seg| {
                seg.iter().map(|w| w.count_ones() as usize).sum::<usize>()
            })
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // JE: BitMapTest.testSegments
    #[test]
    fn test_segments() {
        let mut bmap = BitMap::new();
        let start_bit = 15;
        let end_bit = 62;
        assert_eq!(bmap.cardinality(), 0);
        assert_eq!(bmap.num_segments(), 0);

        assert!(!bmap.get(1001));
        assert_eq!(bmap.num_segments(), 0);

        // Set a bit in different segments.
        for i in start_bit..=end_bit {
            let index = (1i64 << i) + 17;
            bmap.set(index);
        }

        assert_eq!(bmap.cardinality(), (end_bit - start_bit + 1) as usize);
        assert_eq!(bmap.num_segments(), (end_bit - start_bit + 1) as usize);

        // Should be set.
        for i in start_bit..=end_bit {
            let index = (1i64 << i) + 17;
            assert!(bmap.get(index));
        }

        // Should be clear.
        for i in start_bit..=end_bit {
            let index = 7 + (1i64 << i);
            assert!(!bmap.get(index));
        }

        // Checking for non-set bits should not create more segments.
        assert_eq!(bmap.cardinality(), (end_bit - start_bit + 1) as usize);
        assert_eq!(bmap.num_segments(), (end_bit - start_bit + 1) as usize);
    }

    // JE: BitMapTest.testNegative
    #[test]
    #[should_panic(expected = "negative")]
    fn test_negative() {
        let mut b_map = BitMap::new();
        b_map.set(-300);
    }
}
