//! Bloom filter for BIN-delta key membership testing.
//!
//! Port of `com.sleepycat.je.tree.BINDeltaBloomFilter` from JE.
//!
//! A Bloom filter implementation, highly specialized for use in BIN deltas.
//! Both space and computation times are minimized, with a potential small
//! loss in accuracy.
//!
//! A nice introduction to bloom filters can be found here:
//! http://en.wikipedia.org/wiki/Bloom_filter

/// Parameters for the Fowler-Noll-Vo (FNV) hash function.
const FNV_OFFSET_BASIS: u64 = 2166136261;
const FNV_PRIME: u64 = 16777619;

/// The m/n ratio, where m is the number of bits used by the bloom filter
/// and n is the number of keys in the set represented by the bloom filter.
const M_N_RATIO: usize = 8;

/// The number of hash values to generate per key, when a key is added to
/// the filter or when the key's membership is tested.
const K: usize = 3;

/// Context for optimized bloom filter creation.
///
/// Lets us avoid repeated (per key) hashing of the key prefix and repeated
/// allocations of the RNG and the hashes array.
pub struct HashContext {
    /// Pre-computed hash values for the current key.
    pub hashes: [usize; K],

    /// Seed for pseudo-random number generation.
    pub seed: u64,

    /// Initial FNV hash value (can include key prefix).
    pub init_fnv_value: u64,
}

impl HashContext {
    /// Creates a new hash context.
    pub fn new() -> Self {
        Self { hashes: [0; K], seed: 0, init_fnv_value: FNV_OFFSET_BASIS }
    }

    /// Hashes a key prefix and updates the initial FNV value.
    ///
    /// This allows prefix hashing to be amortized across multiple keys
    /// with the same prefix.
    pub fn hash_key_prefix(&mut self, prefix: &[u8]) {
        self.init_fnv_value = hash_fnv(prefix, self.init_fnv_value);
    }
}

impl Default for HashContext {
    fn default() -> Self {
        Self::new()
    }
}

/// BIN-delta bloom filter operations.
pub struct BinDeltaBloomFilter;

impl BinDeltaBloomFilter {
    /// Creates a bloom filter byte array for the given keys.
    pub fn create(keys: &[&[u8]]) -> Vec<u8> {
        if keys.is_empty() {
            return vec![0];
        }

        let num_bytes = Self::get_byte_size(keys.len());
        let mut filter = vec![0u8; num_bytes];
        let mut hc = HashContext::new();

        for key in keys {
            Self::add(&mut filter, key, &mut hc);
        }

        filter
    }

    /// Adds the given key to the given bloom filter.
    pub fn add(bf: &mut [u8], key: &[u8], hc: &mut HashContext) {
        hash(bf, key, hc);

        for &idx in &hc.hashes {
            set_bit(bf, idx);
        }
    }

    /// Tests if a key might exist in the set represented by this filter.
    ///
    /// May return false positives but never false negatives.
    pub fn contains(bf: &[u8], key: &[u8]) -> bool {
        let mut hc = HashContext::new();
        hash(bf, key, &mut hc);

        for &idx in &hc.hashes {
            if !get_bit(bf, idx) {
                return false;
            }
        }

        true
    }

    /// Gets the number of bytes needed to store the bitset of a bloom filter
    /// for the given number of keys.
    pub fn get_byte_size(num_keys: usize) -> usize {
        if num_keys == 0 {
            return 1;
        }
        let nbits = num_keys * M_N_RATIO;
        nbits.div_ceil(8)
    }

    /// Gets the total memory consumed by the given bloom filter.
    pub fn get_memory_size(bf: &[u8]) -> usize {
        // In Rust, Vec overhead is 24 bytes (ptr + len + cap)
        24 + bf.len()
    }
}

/// Generates K hash values for the given key.
fn hash(bf: &[u8], key: &[u8], hc: &mut HashContext) {
    debug_assert_eq!(K, 3);
    debug_assert_eq!(hc.hashes.len(), K);

    hc.seed = hash_fnv(key, hc.init_fnv_value);

    let num_bits = bf.len() * 8;

    // Use a simple LCG (Linear Congruential Generator) for speed.
    // This mimics Java's Random but is simpler.
    if num_bits <= 1024 {
        let hash = lcg_next(hc.seed);
        hc.hashes[0] = ((hash & 0x0000_03FF) as usize) % num_bits;
        let hash = hash >> 10;
        hc.hashes[1] = ((hash & 0x0000_03FF) as usize) % num_bits;
        let hash = hash >> 10;
        hc.hashes[2] = ((hash & 0x0000_03FF) as usize) % num_bits;
        hc.seed = hash;
    } else {
        hc.hashes[0] = (lcg_next(hc.seed) as usize) % num_bits;
        hc.seed = lcg_next(hc.seed);
        hc.hashes[1] = (lcg_next(hc.seed) as usize) % num_bits;
        hc.seed = lcg_next(hc.seed);
        hc.hashes[2] = (lcg_next(hc.seed) as usize) % num_bits;
        hc.seed = lcg_next(hc.seed);
    }
}

/// Fowler-Noll-Vo hash function.
fn hash_fnv(key: &[u8], init_value: u64) -> u64 {
    let mut hash = init_value;

    for &b in key {
        hash = hash.wrapping_mul(FNV_PRIME) & 0xFFFF_FFFF;
        hash ^= b as u64;
    }

    hash
}

/// Simple linear congruential generator (mimics Java's Random).
///
/// Uses constants from Knuth and Schrage.
fn lcg_next(seed: u64) -> u64 {
    seed.wrapping_mul(0x5DEECE66D).wrapping_add(0xB) & 0xFFFF_FFFF_FFFF
}

/// Sets a bit in the bloom filter at the given index.
fn set_bit(bf: &mut [u8], idx: usize) {
    bf[idx / 8] |= 1 << (idx % 8);
}

/// Gets a bit from the bloom filter at the given index.
fn get_bit(bf: &[u8], idx: usize) -> bool {
    (bf[idx / 8] & (1 << (idx % 8))) != 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bloom_filter_basic() {
        let keys = vec![b"key1".as_ref(), b"key2".as_ref(), b"key3".as_ref()];
        let filter = BinDeltaBloomFilter::create(&keys);

        // All inserted keys should be found (no false negatives)
        assert!(BinDeltaBloomFilter::contains(&filter, b"key1"));
        assert!(BinDeltaBloomFilter::contains(&filter, b"key2"));
        assert!(BinDeltaBloomFilter::contains(&filter, b"key3"));
    }

    #[test]
    fn test_bloom_filter_no_false_negatives() {
        let keys = vec![
            b"apple".as_ref(),
            b"banana".as_ref(),
            b"cherry".as_ref(),
            b"date".as_ref(),
            b"elderberry".as_ref(),
        ];
        let filter = BinDeltaBloomFilter::create(&keys);

        // All inserted keys must be found
        for key in &keys {
            assert!(
                BinDeltaBloomFilter::contains(&filter, key),
                "Key {:?} should be found (no false negatives allowed)",
                key
            );
        }
    }

    #[test]
    fn test_bloom_filter_false_positives() {
        let keys = vec![b"key1".as_ref(), b"key2".as_ref()];
        let filter = BinDeltaBloomFilter::create(&keys);

        // We might get false positives for keys that weren't inserted
        // but this is expected behavior for bloom filters.
        // This test just documents that behavior.
        let _ = BinDeltaBloomFilter::contains(&filter, b"key_not_inserted");
        // We can't assert false here because false positives are possible
    }

    #[test]
    fn test_get_byte_size() {
        assert_eq!(BinDeltaBloomFilter::get_byte_size(0), 1);
        assert_eq!(BinDeltaBloomFilter::get_byte_size(1), 1); // 8 bits / 8 = 1 byte
        assert_eq!(BinDeltaBloomFilter::get_byte_size(8), 8); // 64 bits / 8 = 8 bytes
        assert_eq!(BinDeltaBloomFilter::get_byte_size(100), 100); // 800 bits / 8 = 100 bytes
    }

    #[test]
    fn test_empty_filter() {
        let filter = BinDeltaBloomFilter::create(&[]);
        assert_eq!(filter.len(), 1);

        // Empty filter should not contain anything
        assert!(!BinDeltaBloomFilter::contains(&filter, b"any_key"));
    }

    #[test]
    fn test_hash_context() {
        let mut hc = HashContext::new();
        assert_eq!(hc.init_fnv_value, FNV_OFFSET_BASIS);

        hc.hash_key_prefix(b"prefix");
        // After hashing prefix, init_fnv_value should be different
        assert_ne!(hc.init_fnv_value, FNV_OFFSET_BASIS);
    }

    #[test]
    fn test_set_and_get_bit() {
        let mut bf = vec![0u8; 10];

        // Set some bits
        set_bit(&mut bf, 0);
        set_bit(&mut bf, 7);
        set_bit(&mut bf, 15);
        set_bit(&mut bf, 63);

        // Check they're set
        assert!(get_bit(&bf, 0));
        assert!(get_bit(&bf, 7));
        assert!(get_bit(&bf, 15));
        assert!(get_bit(&bf, 63));

        // Check others are not set
        assert!(!get_bit(&bf, 1));
        assert!(!get_bit(&bf, 8));
        assert!(!get_bit(&bf, 16));
        assert!(!get_bit(&bf, 64));
    }

    #[test]
    fn test_fnv_hash() {
        let h1 = hash_fnv(b"test", FNV_OFFSET_BASIS);
        let h2 = hash_fnv(b"test", FNV_OFFSET_BASIS);
        assert_eq!(h1, h2); // Same input should give same hash

        let h3 = hash_fnv(b"different", FNV_OFFSET_BASIS);
        assert_ne!(h1, h3); // Different input should (likely) give different hash
    }

    #[test]
    fn test_large_filter() {
        // Test with many keys
        let keys: Vec<Vec<u8>> =
            (0..1000).map(|i| format!("key_{}", i).into_bytes()).collect();
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();

        let filter = BinDeltaBloomFilter::create(&key_refs);

        // Verify all keys are found
        for key in &key_refs {
            assert!(
                BinDeltaBloomFilter::contains(&filter, key),
                "Key should be found in filter"
            );
        }
    }

    #[test]
    fn test_memory_size() {
        let filter = vec![0u8; 100];
        let mem_size = BinDeltaBloomFilter::get_memory_size(&filter);
        assert_eq!(mem_size, 24 + 100); // Vec overhead + data
    }
}
