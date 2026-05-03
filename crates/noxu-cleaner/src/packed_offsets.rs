//! Packed offset list for efficient storage of obsolete log entry offsets.
//!
//! Port of `com.sleepycat.je.cleaner.PackedOffsets` - stores a sorted list of LSN offsets
//! in a packed representation. Each stored value is the difference between two consecutive
//! offsets, encoded as a variable-length integer.

/// Stores a sorted list of LSN offsets in a packed representation.
///
/// Offsets are stored as deltas between consecutive values, using variable-length encoding
/// where each value takes 1-5 bytes depending on magnitude. This achieves significant space
/// savings for closely-spaced offsets.
#[derive(Debug, Clone, Default)]
pub struct PackedOffsets {
    /// Packed delta-encoded offset data.
    data: Vec<u8>,
    /// Number of offsets stored.
    count: usize,
}

impl PackedOffsets {
    /// Creates an empty PackedOffsets.
    pub fn new() -> Self {
        Self::default()
    }

    /// Packs the given offsets, replacing any offsets stored in this object.
    ///
    /// The offsets are sorted and delta-encoded into a compact byte representation.
    pub fn pack(&mut self, offsets: &[u32]) {
        if offsets.is_empty() {
            self.data.clear();
            self.count = 0;
            return;
        }

        // Sort offsets
        let mut sorted = offsets.to_vec();
        sorted.sort_unstable();

        // Estimate size (worst case: 5 bytes per offset for varint encoding)
        let mut buffer = Vec::with_capacity(sorted.len() * 5);

        // Encode first offset and then deltas
        let mut prev = 0u32;
        for &offset in &sorted {
            let delta = offset - prev;
            write_varint(&mut buffer, delta);
            prev = offset;
        }

        self.data = buffer;
        self.count = sorted.len();
    }

    /// Returns the unpacked offsets as a vector.
    pub fn unpack(&self) -> Vec<u32> {
        if self.count == 0 {
            return Vec::new();
        }

        let mut offsets = Vec::with_capacity(self.count);
        let mut pos = 0;
        let mut current = 0u32;

        while pos < self.data.len() && offsets.len() < self.count {
            let (delta, bytes_read) = read_varint(&self.data[pos..]);
            current = current.wrapping_add(delta);
            offsets.push(current);
            pos += bytes_read;
        }

        offsets
    }

    /// Returns the number of offsets stored.
    pub fn get_count(&self) -> usize {
        self.count
    }

    /// Returns a reference to the packed data.
    pub fn get_data(&self) -> &[u8] {
        &self.data
    }

    /// Returns whether this object is empty.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Returns the memory size of the packed data.
    pub fn memory_size(&self) -> usize {
        self.data.len()
    }
}

/// Writes a u32 value as a variable-length integer.
///
/// Uses a simple varint encoding where each byte stores 7 bits of data and 1 continuation bit.
/// The continuation bit is set to 1 if more bytes follow, 0 for the last byte.
fn write_varint(buffer: &mut Vec<u8>, mut value: u32) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80; // Set continuation bit
        }
        buffer.push(byte);
        if value == 0 {
            break;
        }
    }
}

/// Reads a variable-length integer from the buffer.
///
/// Returns (value, bytes_read).
fn read_varint(buffer: &[u8]) -> (u32, usize) {
    let mut value = 0u32;
    let mut shift = 0;
    let mut pos = 0;

    loop {
        if pos >= buffer.len() {
            break;
        }

        let byte = buffer[pos];
        pos += 1;

        value |= ((byte & 0x7F) as u32) << shift;
        shift += 7;

        if (byte & 0x80) == 0 {
            break;
        }
    }

    (value, pos)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let packed = PackedOffsets::new();
        assert!(packed.is_empty());
        assert_eq!(packed.get_count(), 0);
        assert_eq!(packed.get_data().len(), 0);
    }

    #[test]
    fn test_pack_empty() {
        let mut packed = PackedOffsets::new();
        packed.pack(&[]);
        assert!(packed.is_empty());
        assert_eq!(packed.get_count(), 0);
    }

    #[test]
    fn test_pack_single() {
        let mut packed = PackedOffsets::new();
        packed.pack(&[100]);
        assert_eq!(packed.get_count(), 1);
        assert!(!packed.is_empty());

        let unpacked = packed.unpack();
        assert_eq!(unpacked, vec![100]);
    }

    #[test]
    fn test_pack_multiple() {
        let mut packed = PackedOffsets::new();
        let offsets = vec![100, 200, 300, 400, 500];
        packed.pack(&offsets);

        assert_eq!(packed.get_count(), 5);
        let unpacked = packed.unpack();
        assert_eq!(unpacked, offsets);
    }

    #[test]
    fn test_pack_unsorted() {
        let mut packed = PackedOffsets::new();
        let offsets = vec![500, 100, 300, 200, 400];
        packed.pack(&offsets);

        assert_eq!(packed.get_count(), 5);
        let unpacked = packed.unpack();
        // Should be sorted after unpacking
        assert_eq!(unpacked, vec![100, 200, 300, 400, 500]);
    }

    #[test]
    fn test_pack_with_duplicates() {
        let mut packed = PackedOffsets::new();
        let offsets = vec![100, 100, 200, 200, 300];
        packed.pack(&offsets);

        assert_eq!(packed.get_count(), 5);
        let unpacked = packed.unpack();
        assert_eq!(unpacked, vec![100, 100, 200, 200, 300]);
    }

    #[test]
    fn test_pack_large_offsets() {
        let mut packed = PackedOffsets::new();
        let offsets = vec![1000000, 2000000, 3000000];
        packed.pack(&offsets);

        assert_eq!(packed.get_count(), 3);
        let unpacked = packed.unpack();
        assert_eq!(unpacked, offsets);
    }

    #[test]
    fn test_varint_encoding_small() {
        let mut buffer = Vec::new();
        write_varint(&mut buffer, 0);
        assert_eq!(buffer, vec![0]);

        buffer.clear();
        write_varint(&mut buffer, 127);
        assert_eq!(buffer, vec![127]);
    }

    #[test]
    fn test_varint_encoding_medium() {
        let mut buffer = Vec::new();
        write_varint(&mut buffer, 128);
        assert_eq!(buffer, vec![0x80, 0x01]);

        buffer.clear();
        write_varint(&mut buffer, 300);
        assert_eq!(buffer, vec![0xAC, 0x02]);
    }

    #[test]
    fn test_varint_encoding_large() {
        let mut buffer = Vec::new();
        write_varint(&mut buffer, 1_000_000);
        let (decoded, bytes_read) = read_varint(&buffer);
        assert_eq!(decoded, 1_000_000);
        assert_eq!(bytes_read, buffer.len());
    }

    #[test]
    fn test_varint_roundtrip() {
        for value in [
            0,
            1,
            127,
            128,
            255,
            256,
            16383,
            16384,
            65535,
            65536,
            1_000_000,
            u32::MAX,
        ] {
            let mut buffer = Vec::new();
            write_varint(&mut buffer, value);
            let (decoded, _) = read_varint(&buffer);
            assert_eq!(decoded, value, "Failed roundtrip for {}", value);
        }
    }

    #[test]
    fn test_delta_encoding_efficiency() {
        let mut packed = PackedOffsets::new();
        // Closely spaced offsets should compress well
        let offsets: Vec<u32> = (1000..1100).collect();
        packed.pack(&offsets);

        // Each delta is 1, which should encode to 1 byte
        // First offset (1000) takes ~2 bytes, rest take 1 byte each
        assert!(packed.memory_size() < offsets.len() * 4); // Much smaller than 4 bytes per offset
    }

    #[test]
    fn test_memory_size() {
        let mut packed = PackedOffsets::new();
        let offsets = vec![100, 200, 300];
        packed.pack(&offsets);

        assert!(packed.memory_size() > 0);
        assert_eq!(packed.memory_size(), packed.get_data().len());
    }

    #[test]
    fn test_repack() {
        let mut packed = PackedOffsets::new();
        packed.pack(&[100, 200, 300]);
        assert_eq!(packed.get_count(), 3);

        // Repack with different data
        packed.pack(&[400, 500]);
        assert_eq!(packed.get_count(), 2);
        assert_eq!(packed.unpack(), vec![400, 500]);
    }

    #[test]
    fn test_clone() {
        let mut packed1 = PackedOffsets::new();
        packed1.pack(&[100, 200, 300]);

        let packed2 = packed1.clone();
        assert_eq!(packed2.get_count(), 3);
        assert_eq!(packed2.unpack(), vec![100, 200, 300]);
    }

    #[test]
    fn test_default() {
        let packed = PackedOffsets::default();
        assert!(packed.is_empty());
        assert_eq!(packed.get_count(), 0);
    }

    #[test]
    fn test_large_dataset() {
        let mut packed = PackedOffsets::new();
        let offsets: Vec<u32> = (0..10000).map(|i| i * 100).collect();
        packed.pack(&offsets);

        assert_eq!(packed.get_count(), 10000);
        let unpacked = packed.unpack();
        assert_eq!(unpacked, offsets);
    }

    #[test]
    fn test_max_u32_value() {
        let mut packed = PackedOffsets::new();
        let offsets = vec![u32::MAX - 2, u32::MAX - 1, u32::MAX];
        packed.pack(&offsets);

        let unpacked = packed.unpack();
        assert_eq!(unpacked, offsets);
    }
}
