//! BIN delta slot information.
//!
//!
//! Holds the delta information for one BIN entry in a partial BIN log entry.
//! BIN deltas are logged instead of full BINs when only a few slots have changed,
//! reducing the amount of data written to the log.
//!
//! ## Property tests
//!
//! Round-trip and reverse-direction encode/decode properties live in
//! `crates/noxu-tree/tests/prop_tests.rs` (see Wave 11-E):
//! `delta_info_roundtrip`, `delta_info_encode_deterministic`,
//! `delta_info_read_then_write_idempotent`.

use crate::tree::entry_states::SlotState;
use crate::util::Lsn;

/// Holds the delta for one BIN entry in a partial BIN log entry.
///
/// A BIN delta contains only the slots that have changed since the last
/// full BIN write. Each changed slot is represented by a DeltaInfo.
#[derive(Debug, Clone)]
pub struct DeltaInfo {
    /// The key for this slot.
    pub key: Vec<u8>,

    /// The LSN of the child node.
    pub lsn: Lsn,

    /// State flags for this slot.
    pub state: SlotState,
}

impl DeltaInfo {
    /// Creates a new DeltaInfo.
    ///
    /// # Arguments
    /// * `key` - The key for this slot
    /// * `lsn` - The LSN of the child node
    /// * `state` - State flags for this slot
    pub fn new(key: Vec<u8>, lsn: Lsn, state: SlotState) -> Self {
        DeltaInfo { key, lsn, state }
    }

    /// Returns true if the known-deleted flag is set.
    #[inline]
    pub fn is_known_deleted(&self) -> bool {
        self.state.is_known_deleted()
    }

    /// Returns true if the pending-deleted flag is set.
    #[inline]
    pub fn is_pending_deleted(&self) -> bool {
        self.state.is_pending_deleted()
    }

    /// Returns true if the dirty flag is set.
    #[inline]
    pub fn is_dirty(&self) -> bool {
        self.state.is_dirty()
    }

    /// Returns true if the embedded-LN flag is set.
    #[inline]
    pub fn is_embedded_ln(&self) -> bool {
        self.state.is_embedded_ln()
    }

    /// Computes the log size in bytes for this delta info.
    ///
    /// The size includes:
    /// - 2 bytes for key length (u16)
    /// - N bytes for the key data
    /// - 8 bytes for the LSN (u64)
    /// - 1 byte for the state flags
    pub fn log_size(&self) -> usize {
        2 +                    // key length (u16)
            self.key.len() +   // key bytes
            8 +                // LSN (u64)
            1 // state (u8)
    }

    /// Writes this delta info to a byte buffer.
    ///
    /// Format:
    /// - key_length: u16 (little-endian)
    /// - key_bytes: [u8; key_length]
    /// - lsn: u64 (little-endian)
    /// - state: u8
    ///
    /// # Arguments
    /// * `buffer` - The buffer to write to
    pub fn write_to_log(&self, buffer: &mut Vec<u8>) {
        // Write key length (u16 little-endian)
        let key_len = self.key.len() as u16;
        buffer.extend_from_slice(&key_len.to_le_bytes());

        // Write key bytes
        buffer.extend_from_slice(&self.key);

        // Write LSN (u64 little-endian)
        buffer.extend_from_slice(&self.lsn.as_u64().to_le_bytes());

        // Write state byte
        buffer.push(self.state.as_byte());
    }

    /// Reads a delta info from a byte slice.
    ///
    /// Returns the DeltaInfo and the number of bytes consumed.
    ///
    /// # Arguments
    /// * `data` - The byte slice to read from
    ///
    /// # Returns
    /// `Ok((DeltaInfo, bytes_consumed))` on success, or an error if the data is malformed.
    pub fn read_from_log(data: &[u8]) -> Result<(Self, usize), String> {
        let mut offset = 0;

        // Read key length (u16 little-endian)
        if data.len() < 2 {
            return Err("Not enough data for key length".to_string());
        }
        let key_len = u16::from_le_bytes([data[0], data[1]]) as usize;
        offset += 2;

        // Read key bytes
        if data.len() < offset + key_len {
            return Err("Not enough data for key".to_string());
        }
        let key = data[offset..offset + key_len].to_vec();
        offset += key_len;

        // Read LSN (u64 little-endian)
        if data.len() < offset + 8 {
            return Err("Not enough data for LSN".to_string());
        }
        let lsn_bytes: [u8; 8] = data[offset..offset + 8]
            .try_into()
            .map_err(|_| "Failed to read LSN bytes")?;
        let lsn = Lsn::from_u64(u64::from_le_bytes(lsn_bytes));
        offset += 8;

        // Read state byte
        if data.len() < offset + 1 {
            return Err("Not enough data for state".to_string());
        }
        let state = SlotState::from_byte(data[offset]);
        offset += 1;

        Ok((DeltaInfo::new(key, lsn, state), offset))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::{Lsn, NULL_LSN};

    #[test]
    fn test_new() {
        let key = b"test_key".to_vec();
        let lsn = Lsn::new(1, 1000);
        let state = SlotState::new();

        let delta = DeltaInfo::new(key.clone(), lsn, state);

        assert_eq!(delta.key, key);
        assert_eq!(delta.lsn, lsn);
        assert!(!delta.is_dirty());
    }

    #[test]
    fn test_state_flags() {
        let key = b"key".to_vec();
        let lsn = Lsn::new(5, 5000);
        let mut state = SlotState::new();
        state.set_dirty();
        state.set_embedded_ln();

        let delta = DeltaInfo::new(key, lsn, state);

        assert!(delta.is_dirty());
        assert!(delta.is_embedded_ln());
        assert!(!delta.is_known_deleted());
        assert!(!delta.is_pending_deleted());
    }

    #[test]
    fn test_log_size() {
        let key = b"test".to_vec();
        let lsn = NULL_LSN;
        let state = SlotState::new();

        let delta = DeltaInfo::new(key, lsn, state);

        // 2 (key_len) + 4 (key bytes) + 8 (LSN) + 1 (state) = 15
        assert_eq!(delta.log_size(), 15);
    }

    #[test]
    fn test_log_size_empty_key() {
        let key = Vec::new();
        let lsn = Lsn::new(1, 1);
        let state = SlotState::new();

        let delta = DeltaInfo::new(key, lsn, state);

        // 2 (key_len) + 0 (key bytes) + 8 (LSN) + 1 (state) = 11
        assert_eq!(delta.log_size(), 11);
    }

    #[test]
    fn test_write_and_read_round_trip() {
        let key = b"round_trip_key".to_vec();
        let lsn = Lsn::new(10, 20000);
        let mut state = SlotState::new();
        state.set_dirty();
        state.set_known_deleted();

        let original = DeltaInfo::new(key, lsn, state);

        let mut buffer = Vec::new();
        original.write_to_log(&mut buffer);

        let (decoded, bytes_consumed) =
            DeltaInfo::read_from_log(&buffer).unwrap();

        assert_eq!(decoded.key, original.key);
        assert_eq!(decoded.lsn, original.lsn);
        assert_eq!(decoded.state.as_byte(), original.state.as_byte());
        assert_eq!(bytes_consumed, buffer.len());
        assert_eq!(bytes_consumed, original.log_size());
    }

    #[test]
    fn test_write_and_read_empty_key() {
        let key = Vec::new();
        let lsn = NULL_LSN;
        let state = SlotState::new();

        let original = DeltaInfo::new(key, lsn, state);

        let mut buffer = Vec::new();
        original.write_to_log(&mut buffer);

        let (decoded, _) = DeltaInfo::read_from_log(&buffer).unwrap();

        assert!(decoded.key.is_empty());
        assert_eq!(decoded.lsn, NULL_LSN);
    }

    #[test]
    fn test_read_from_log_insufficient_data() {
        let data = &[0u8; 1]; // Only 1 byte, need at least 2 for key length

        let result = DeltaInfo::read_from_log(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_from_log_truncated_key() {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&5u16.to_le_bytes()); // key_len = 5
        buffer.extend_from_slice(&[1, 2, 3]); // Only 3 bytes of key

        let result = DeltaInfo::read_from_log(&buffer);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Not enough data for key"));
    }

    #[test]
    fn test_read_from_log_truncated_lsn() {
        let mut buffer = Vec::new();
        buffer.extend_from_slice(&2u16.to_le_bytes()); // key_len = 2
        buffer.extend_from_slice(&[1, 2]); // key bytes
        buffer.extend_from_slice(&[1, 2, 3]); // Only 3 bytes of LSN (need 8)

        let result = DeltaInfo::read_from_log(&buffer);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Not enough data for LSN"));
    }

    #[test]
    fn test_clone() {
        let key = b"clone_test".to_vec();
        let lsn = Lsn::new(7, 7777);
        let mut state = SlotState::new();
        state.set_embedded_ln();

        let delta1 = DeltaInfo::new(key, lsn, state);
        let delta2 = delta1.clone();

        assert_eq!(delta2.key, delta1.key);
        assert_eq!(delta2.lsn, delta1.lsn);
        assert_eq!(delta2.state.as_byte(), delta1.state.as_byte());
    }
}
