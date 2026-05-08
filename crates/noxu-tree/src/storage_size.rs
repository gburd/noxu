//! Storage size estimation utilities.
//!
//!
//! Provides constants and utilities for estimating the disk storage size
//! of records and tree nodes. These estimates help applications understand
//! the storage footprint of their data.

/// Overhead bytes for a standalone LN log entry.
///
/// This includes the log entry header, checksums, key, and LN metadata,
/// but excludes the actual data bytes.
pub const LN_OVERHEAD: usize = 50;

/// Overhead bytes for a secondary database slot.
///
/// Secondary databases reference primary database records. This is the
/// overhead for each slot in a secondary BIN.
pub const SEC_SLOT_OVERHEAD: usize = 12;

/// Overhead bytes for a primary database BIN slot.
///
/// This is the per-slot overhead in a primary database BIN when the LN
/// is not embedded.
pub const PRI_SLOT_OVERHEAD: usize = 14;

/// Overhead bytes for a primary database BIN slot with an embedded LN.
///
/// When small LNs are embedded directly in the BIN (to avoid a separate
/// log entry), this is the per-slot overhead.
pub const PRI_EMBEDDED_LN_SLOT_OVERHEAD: usize = 20;

/// Estimates the storage size for a primary database record.
///
/// This includes the LN overhead plus the actual key and data sizes.
///
/// # Arguments
/// * `key_size` - Size of the key in bytes
/// * `data_size` - Size of the data in bytes
/// * `embedded` - True if the LN is embedded in the BIN
///
/// # Returns
/// Estimated storage size in bytes
pub fn estimate_primary_record_size(
    key_size: usize,
    data_size: usize,
    embedded: bool,
) -> usize {
    if embedded {
        // Embedded LN: no separate log entry, just BIN slot overhead
        PRI_EMBEDDED_LN_SLOT_OVERHEAD + key_size + data_size
    } else {
        // Non-embedded: LN log entry + BIN slot
        LN_OVERHEAD + key_size + data_size + PRI_SLOT_OVERHEAD
    }
}

/// Estimates the storage size for a secondary database record.
///
/// Secondary databases only store keys (which reference primary keys).
/// The actual data is in the primary database.
///
/// # Arguments
/// * `key_size` - Size of the secondary key in bytes
///
/// # Returns
/// Estimated storage size in bytes
pub fn estimate_secondary_record_size(key_size: usize) -> usize {
    SEC_SLOT_OVERHEAD + key_size
}

/// Estimates the total storage size for a set of records.
///
/// # Arguments
/// * `record_sizes` - Iterator over individual record sizes
///
/// # Returns
/// Total estimated storage size in bytes
pub fn estimate_total_size<I>(record_sizes: I) -> usize
where
    I: IntoIterator<Item = usize>,
{
    record_sizes.into_iter().sum()
}

/// Determines if a record should be embedded in the BIN based on size.
///
/// Small records are more efficient when embedded directly in the BIN,
/// avoiding a separate log entry fetch. uses a threshold around 16 bytes.
///
/// # Arguments
/// * `data_size` - Size of the data in bytes
///
/// # Returns
/// True if the record should be embedded
pub fn should_embed_ln(data_size: usize) -> bool {
    // Embed if data size is small (threshold of 16 bytes, matching behavior)
    data_size <= 16
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_constants() {
        assert_eq!(LN_OVERHEAD, 50);
        assert_eq!(SEC_SLOT_OVERHEAD, 12);
        assert_eq!(PRI_SLOT_OVERHEAD, 14);
        assert_eq!(PRI_EMBEDDED_LN_SLOT_OVERHEAD, 20);
    }

    #[test]
    fn test_estimate_primary_record_size_embedded() {
        let key_size = 10;
        let data_size = 5;

        let size = estimate_primary_record_size(key_size, data_size, true);

        // PRI_EMBEDDED_LN_SLOT_OVERHEAD + key + data
        assert_eq!(size, PRI_EMBEDDED_LN_SLOT_OVERHEAD + key_size + data_size);
        assert_eq!(size, 35);
    }

    #[test]
    fn test_estimate_primary_record_size_not_embedded() {
        let key_size = 10;
        let data_size = 100;

        let size = estimate_primary_record_size(key_size, data_size, false);

        // LN_OVERHEAD + key + data + PRI_SLOT_OVERHEAD
        assert_eq!(
            size,
            LN_OVERHEAD + key_size + data_size + PRI_SLOT_OVERHEAD
        );
        assert_eq!(size, 174);
    }

    #[test]
    fn test_estimate_secondary_record_size() {
        let key_size = 20;

        let size = estimate_secondary_record_size(key_size);

        // SEC_SLOT_OVERHEAD + key
        assert_eq!(size, SEC_SLOT_OVERHEAD + key_size);
        assert_eq!(size, 32);
    }

    #[test]
    fn test_estimate_total_size() {
        let sizes = vec![100, 200, 300];
        let total = estimate_total_size(sizes);

        assert_eq!(total, 600);
    }

    #[test]
    fn test_estimate_total_size_empty() {
        let sizes: Vec<usize> = vec![];
        let total = estimate_total_size(sizes);

        assert_eq!(total, 0);
    }

    #[test]
    fn test_should_embed_ln_small() {
        assert!(should_embed_ln(0));
        assert!(should_embed_ln(1));
        assert!(should_embed_ln(10));
        assert!(should_embed_ln(16));
    }

    #[test]
    fn test_should_embed_ln_large() {
        assert!(!should_embed_ln(17));
        assert!(!should_embed_ln(100));
        assert!(!should_embed_ln(1000));
    }

    #[test]
    fn test_should_embed_ln_boundary() {
        // Exactly at threshold
        assert!(should_embed_ln(16));
        // Just over threshold
        assert!(!should_embed_ln(17));
    }

    #[test]
    fn test_primary_record_size_zero_sizes() {
        let size_embedded = estimate_primary_record_size(0, 0, true);
        let size_not_embedded = estimate_primary_record_size(0, 0, false);

        assert_eq!(size_embedded, PRI_EMBEDDED_LN_SLOT_OVERHEAD);
        assert_eq!(size_not_embedded, LN_OVERHEAD + PRI_SLOT_OVERHEAD);
    }

    #[test]
    fn test_secondary_record_size_zero() {
        let size = estimate_secondary_record_size(0);
        assert_eq!(size, SEC_SLOT_OVERHEAD);
    }

    #[test]
    fn test_estimate_total_size_iterator() {
        let sizes = (1..=5).map(|i| i * 10);
        let total = estimate_total_size(sizes);

        // 10 + 20 + 30 + 40 + 50 = 150
        assert_eq!(total, 150);
    }
}
