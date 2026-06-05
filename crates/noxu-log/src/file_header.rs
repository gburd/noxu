//! File header for Noxu DB log files.
//!
//! Each log file starts with a self-describing header that allows any reader
//! to identify the file as a Noxu DB log, determine its format version, and
//! detect the byte order used when the file was written.
//!
//! # Header layout — v3 (current, 36 bytes)
//!
//! ```text
//! Offset  Size  Field
//! ------  ----  -----
//!      0     8  magic           "NOXUDB\0\0" (0x4E4F5855_44420000)
//!      8     4  log_version     u32 big-endian, currently LOG_VERSION (3)
//!     12     1  byte_order      0x00 = big-endian (default), 0x01 = little-endian
//!     13     3  _reserved       zero-filled, reserved for future use
//!     16     8  timestamp       u64 big-endian, Unix time in milliseconds
//!     24     4  file_number     u32 big-endian, 0-based sequential file index
//!     28     4  last_entry_in_prev_file  u32 big-endian, offset in previous file
//!                               (0 for the first file in an environment)
//!     32     4  header_crc      u32 big-endian, CRC32 of bytes [0..32]
//! ```
//!
//! # Header layout — v2 (legacy, 36 bytes)
//!
//! ```text
//! Offset  Size  Field
//! ------  ----  -----
//!      0    32  same as above offsets 0..32 (no trailing CRC)
//! ```
//!
//! v2 files are read-compatible: `read_from` detects the version field and
//! skips the CRC check, returning the header with `log_version == 2`.
//! The first log entry in a v2 file is at offset 32; in a v3 file at offset 36.
//! Use [`FileHeader::on_disk_size`] to resolve this per file.
//!
//! # Portability
//!
//! Files written by this implementation always use big-endian byte order
//! (`byte_order = 0x00`). The `byte_order` field is reserved for future
//! little-endian native format support; current readers reject files with
//! `byte_order != 0x00`.
//!
//! The magic bytes `NOXUDB\0\0` allow tools to identify Noxu DB log files
//! without relying on file extension. The `log_version` field allows format
//! evolution with a clear compatibility check at open time.
//!
//! # Mixed byte-order note
//!
//! The **file header** (both v2 and v3) is written in big-endian byte order.
//! The **log entry header** (`entry_header.rs`) is written in little-endian
//! byte order (matching the entry data path). Some entry payloads retain
//! big-endian fields inherited from the original format. New code should
//! follow the byte-order conventions of each existing layer rather than
//! attempting unification (see `docs/src/internal/checksum-selection.md`).

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{self, Read, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{LogError, Result};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Magic bytes at the start of every Noxu DB log file.
///
/// ASCII "NOXUDB" followed by two NUL bytes for alignment:
/// `0x4E 0x4F 0x58 0x55 0x44 0x42 0x00 0x00`.
pub const FILE_MAGIC: &[u8; 8] = b"NOXUDB\0\0";

/// Byte-order marker for big-endian files (the only supported byte order).
pub const BYTE_ORDER_BIG_ENDIAN: u8 = 0x00;

/// Byte-order marker for little-endian files (reserved, not yet supported).
pub const BYTE_ORDER_LITTLE_ENDIAN: u8 = 0x01;

/// Current log file format version.
///
/// Version history:
/// - 1: Original Noxu format (no magic, no byte-order marker)
/// - 2: Added 8-byte magic, 4-byte version, 1-byte byte-order, 3-byte padding
///   making the header self-describing and portable (32 bytes total).
/// - 3: Added 4-byte CRC32 over header bytes `[0..32]` as a torn-header
///   guard, growing the header to 36 bytes and advancing the first-entry
///   LSN offset from 32 to 36.  v2 files remain fully readable (backward
///   compatible); new files are always written as v3.
pub const LOG_VERSION: u32 = 3;

/// Minimum supported log file format version.
pub const MIN_LOG_VERSION: u32 = 2;

/// Size of a v2 (legacy) file header on disk (bytes).
///
/// Layout: magic(8) + log_version(4) + byte_order(1) + _pad(3)
///       + timestamp(8) + file_number(4) + last_entry_in_prev_file(4) = 32
///
/// Provided for backward-compatible reading of existing v2 log files.
/// Use [`on_disk_size`] to resolve the correct size for a given version.
pub const FILE_HEADER_SIZE_V2: usize = 8 + 4 + 1 + 3 + 8 + 4 + 4;

/// Size of the current (v3) file header on disk (bytes).
///
/// Layout: magic(8) + log_version(4) + byte_order(1) + _pad(3)
///       + timestamp(8) + file_number(4) + last_entry_in_prev_file(4)
///       + header_crc(4) = 36
///
/// This is the size written by new code.  When reading an existing file,
/// use [`FileHeader::on_disk_size`] with the file's actual `log_version`
/// to obtain the correct first-entry offset.
pub const FILE_HEADER_SIZE: usize = FILE_HEADER_SIZE_V2 + 4;

/// Returns the on-disk header size (bytes) for `version`.
///
/// - v2 → 32 bytes (no CRC)
/// - v3+ → 36 bytes (with CRC32)
///
/// This is the offset of the first log entry in any file of that version.
/// **Always use this instead of the bare `FILE_HEADER_SIZE` constant when
/// computing entry offsets for an existing file.**
#[inline]
pub fn on_disk_size(version: u32) -> usize {
    if version < LOG_VERSION { FILE_HEADER_SIZE_V2 } else { FILE_HEADER_SIZE }
}

// ---------------------------------------------------------------------------
// FileHeader
// ---------------------------------------------------------------------------

/// Header written at the beginning of each Noxu DB log file.
///
/// See the module-level documentation for the on-disk layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileHeader {
    /// Unix timestamp in milliseconds when this file was created.
    pub timestamp: u64,
    /// Sequential log file number (0-based).
    pub file_number: u32,
    /// Offset of the last log entry in the previous file, or 0 for file 0.
    pub last_entry_in_prev_file: u32,
    /// Log file format version (`LOG_VERSION`).
    pub log_version: u32,
}

impl FileHeader {
    /// Creates a new file header for `file_number` with `last_entry_in_prev_file`.
    pub fn new(file_number: u32, last_entry_in_prev_file: u32) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("System time before UNIX epoch")
            .as_millis() as u64;

        FileHeader {
            timestamp,
            file_number,
            last_entry_in_prev_file,
            log_version: LOG_VERSION,
        }
    }

    /// Returns the on-disk size (bytes) of a header with the given `version`.
    ///
    /// This is also the byte offset of the first log entry in a file of
    /// that version.  Use this instead of the bare `FILE_HEADER_SIZE`
    /// constant when computing entry offsets for an existing file.
    #[inline]
    pub fn on_disk_size(version: u32) -> usize {
        on_disk_size(version)
    }

    /// Reads and validates a file header from `reader`.
    ///
    /// The reader dispatches on `log_version` after reading the magic and
    /// version fields:
    ///
    /// - **v2** (32 bytes): reads the remaining 20 bytes; no CRC check.
    /// - **v3** (36 bytes): reads the remaining 24 bytes; verifies the
    ///   trailing 4-byte CRC32 (big-endian) over header bytes `[0..32]`.
    ///   Returns [`LogError::HeaderChecksumMismatch`] on mismatch.
    ///
    /// Returns [`LogError::InvalidHeader`] if the magic bytes are wrong or
    /// if the byte-order marker is not big-endian.
    /// Returns [`LogError::VersionMismatch`] if `log_version < MIN_LOG_VERSION`.
    pub fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        // --- Magic (8 bytes) ---
        let mut magic = [0u8; 8];
        reader.read_exact(&mut magic)?;
        if &magic != FILE_MAGIC {
            return Err(LogError::InvalidHeader {
                file_num: u32::MAX,
                message: format!(
                    "bad magic: expected {FILE_MAGIC:?}, found {magic:?}"
                ),
            });
        }

        // --- Version (4 bytes) ---
        let log_version = reader.read_u32::<BigEndian>()?;
        if log_version < MIN_LOG_VERSION {
            return Err(LogError::VersionMismatch {
                expected: LOG_VERSION,
                found: log_version,
                file_num: u32::MAX,
            });
        }

        // --- Byte-order marker (1 byte) ---
        let byte_order = reader.read_u8()?;
        if byte_order != BYTE_ORDER_BIG_ENDIAN {
            return Err(LogError::InvalidHeader {
                file_num: u32::MAX,
                message: format!(
                    "unsupported byte order: 0x{:02X} \
                     (only big-endian 0x00 is supported)",
                    byte_order
                ),
            });
        }

        // --- 3 reserved bytes ---
        let mut _reserved = [0u8; 3];
        reader.read_exact(&mut _reserved)?;

        // --- Payload (16 bytes) ---
        let timestamp = reader.read_u64::<BigEndian>()?;
        let file_number = reader.read_u32::<BigEndian>()?;
        let last_entry_in_prev_file = reader.read_u32::<BigEndian>()?;

        // --- v3: trailing CRC32 (4 bytes, big-endian) ---
        //
        // The CRC covers the first 32 bytes of the header (same layout as v2).
        // We reconstruct those bytes to verify, then consume the 4 CRC bytes.
        if log_version >= LOG_VERSION {
            // Reconstruct the 32-byte prefix that was checksummed.
            let mut covered = [0u8; FILE_HEADER_SIZE_V2];
            covered[..8].copy_from_slice(FILE_MAGIC);
            covered[8..12].copy_from_slice(&log_version.to_be_bytes());
            covered[12] = BYTE_ORDER_BIG_ENDIAN;
            covered[13..16].copy_from_slice(&[0u8; 3]);
            covered[16..24].copy_from_slice(&timestamp.to_be_bytes());
            covered[24..28].copy_from_slice(&file_number.to_be_bytes());
            covered[28..32]
                .copy_from_slice(&last_entry_in_prev_file.to_be_bytes());

            let expected_crc = crc32fast::hash(&covered);
            let stored_crc = reader.read_u32::<BigEndian>()?;

            if stored_crc != expected_crc {
                return Err(LogError::HeaderChecksumMismatch {
                    file_num: file_number,
                    expected: expected_crc,
                    found: stored_crc,
                });
            }
        }

        Ok(FileHeader {
            timestamp,
            file_number,
            last_entry_in_prev_file,
            log_version,
        })
    }

    /// Writes the file header to `writer`.
    ///
    /// Always emits a v3 header (36 bytes):
    /// the first 32 bytes match the v2 layout, followed by a 4-byte big-endian
    /// CRC32 over those 32 bytes.
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Build the 32-byte covered prefix first so we can CRC it.
        let mut covered = [0u8; FILE_HEADER_SIZE_V2];
        covered[..8].copy_from_slice(FILE_MAGIC);
        covered[8..12].copy_from_slice(&self.log_version.to_be_bytes());
        covered[12] = BYTE_ORDER_BIG_ENDIAN;
        covered[13..16].copy_from_slice(&[0u8; 3]);
        covered[16..24].copy_from_slice(&self.timestamp.to_be_bytes());
        covered[24..28].copy_from_slice(&self.file_number.to_be_bytes());
        covered[28..32]
            .copy_from_slice(&self.last_entry_in_prev_file.to_be_bytes());

        writer.write_all(&covered)?;

        // Append CRC32 of the covered prefix (big-endian, matching header
        // byte order).
        let crc = crc32fast::hash(&covered);
        writer.write_u32::<BigEndian>(crc)?;
        Ok(())
    }

    /// Validates that this header matches `expected_file_num`.
    ///
    /// Returns the log version on success.
    pub fn validate(&self, expected_file_num: u32) -> Result<u32> {
        if self.log_version > LOG_VERSION {
            return Err(LogError::VersionMismatch {
                expected: LOG_VERSION,
                found: self.log_version,
                file_num: self.file_number,
            });
        }

        if self.file_number != expected_file_num {
            return Err(LogError::InvalidHeader {
                file_num: self.file_number,
                message: format!(
                    "Expected file number {expected_file_num:08x}, found {:08x}",
                    self.file_number
                ),
            });
        }

        Ok(self.log_version)
    }

    /// Returns the offset of the last entry in the previous file.
    pub fn last_entry_in_prev_file_offset(&self) -> u32 {
        self.last_entry_in_prev_file
    }

    /// Returns the on-disk size of this header in bytes.
    ///
    /// Equivalent to `FileHeader::on_disk_size(self.log_version)`.
    pub const fn size() -> usize {
        FILE_HEADER_SIZE
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    // -----------------------------------------------------------------------
    // Basic constant / layout tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_log_version_is_3() {
        assert_eq!(LOG_VERSION, 3);
    }

    #[test]
    fn test_file_header_size_v2_is_32() {
        assert_eq!(FILE_HEADER_SIZE_V2, 32);
    }

    #[test]
    fn test_file_header_size_v3_is_36() {
        assert_eq!(FILE_HEADER_SIZE, 36);
        assert_eq!(FileHeader::size(), 36);
    }

    #[test]
    fn test_on_disk_size_dispatch() {
        assert_eq!(FileHeader::on_disk_size(2), 32, "v2 → 32 bytes");
        assert_eq!(FileHeader::on_disk_size(3), 36, "v3 → 36 bytes");
        assert_eq!(on_disk_size(2), 32);
        assert_eq!(on_disk_size(3), 36);
    }

    #[test]
    fn test_magic_bytes() {
        assert_eq!(FILE_MAGIC, b"NOXUDB\0\0");
        assert_eq!(FILE_MAGIC.len(), 8);
    }

    // -----------------------------------------------------------------------
    // v3 round-trip (PRIMARY NEW BEHAVIOUR)
    // -----------------------------------------------------------------------

    /// St-C3 primary test: write a v3 header, read it back, verify CRC.
    #[test]
    fn test_v3_header_roundtrip_crc_verified() {
        let header = FileHeader::new(42, 0x1000);
        assert_eq!(header.log_version, LOG_VERSION, "new header is v3");

        let mut buf = Vec::new();
        header.write_to(&mut buf).unwrap();

        // Must be exactly 36 bytes.
        assert_eq!(buf.len(), FILE_HEADER_SIZE, "v3 header is 36 bytes");

        // First 8 bytes: magic.
        assert_eq!(&buf[..8], FILE_MAGIC);
        // Bytes 8..12: LOG_VERSION big-endian.
        assert_eq!(&buf[8..12], &LOG_VERSION.to_be_bytes());
        // Byte 12: BYTE_ORDER_BIG_ENDIAN.
        assert_eq!(buf[12], BYTE_ORDER_BIG_ENDIAN);
        // Bytes 13..16: reserved zeros.
        assert_eq!(&buf[13..16], &[0u8; 3]);
        // Bytes 32..36: CRC of bytes [0..32], big-endian.
        let expected_crc = crc32fast::hash(&buf[..32]);
        let stored_crc =
            u32::from_be_bytes([buf[32], buf[33], buf[34], buf[35]]);
        assert_eq!(stored_crc, expected_crc, "stored CRC matches computed CRC");

        // Round-trip parse must succeed and recover all fields.
        let mut cursor = Cursor::new(&buf);
        let decoded = FileHeader::read_from(&mut cursor).unwrap();

        assert_eq!(decoded.file_number, 42);
        assert_eq!(decoded.last_entry_in_prev_file, 0x1000);
        assert_eq!(decoded.log_version, LOG_VERSION);
        assert!(decoded.timestamp > 0);
    }

    // -----------------------------------------------------------------------
    // Corrupt-header test (fails on pre-fix code, passes post-fix)
    // -----------------------------------------------------------------------

    /// St-C3 negative test: flipping one byte in the 32-byte covered prefix
    /// of a v3 header MUST return `HeaderChecksumMismatch`.
    ///
    /// On the pre-fix codebase (no CRC in the header) this test would FAIL
    /// because `read_from` would succeed and silently return a corrupted header.
    /// On the post-fix codebase it PASSES.
    #[test]
    fn test_corrupt_v3_header_byte_detected() {
        let header = FileHeader::new(7, 0);
        let mut buf = Vec::new();
        header.write_to(&mut buf).unwrap();

        // Corrupt one byte inside the covered prefix (byte 25 = file_number MSB+1).
        buf[25] ^= 0xFF;

        let mut cursor = Cursor::new(buf);
        let result = FileHeader::read_from(&mut cursor);
        assert!(
            result.is_err(),
            "corrupted v3 header must not parse successfully"
        );
        match result.unwrap_err() {
            LogError::HeaderChecksumMismatch { .. } => {} // expected
            other => panic!("expected HeaderChecksumMismatch, got {:?}", other),
        }
    }

    /// St-C3: corrupting the stored CRC bytes (not the covered data) also
    /// returns `HeaderChecksumMismatch`.
    #[test]
    fn test_corrupt_v3_crc_field_detected() {
        let header = FileHeader::new(3, 0);
        let mut buf = Vec::new();
        header.write_to(&mut buf).unwrap();

        // Flip all 4 CRC bytes.
        buf[32] ^= 0xFF;
        buf[33] ^= 0xFF;
        buf[34] ^= 0xFF;
        buf[35] ^= 0xFF;

        let mut cursor = Cursor::new(buf);
        match FileHeader::read_from(&mut cursor).unwrap_err() {
            LogError::HeaderChecksumMismatch { .. } => {}
            other => panic!("expected HeaderChecksumMismatch, got {:?}", other),
        }
    }

    // -----------------------------------------------------------------------
    // v2 backward-compatibility
    // -----------------------------------------------------------------------

    /// St-C3 backward-compat: a pre-written 32-byte v2 header parses cleanly.
    /// No CRC is checked (there is none).  This confirms that entries at
    /// offset 32 in a v2 file will be found correctly.
    #[test]
    fn test_v2_header_backward_compat_no_crc_check() {
        // Build a raw 32-byte v2 header (version = 2).
        let mut buf = Vec::new();
        buf.extend_from_slice(FILE_MAGIC); // 0..8
        buf.extend_from_slice(&2u32.to_be_bytes()); // 8..12 version=2
        buf.push(BYTE_ORDER_BIG_ENDIAN); // 12
        buf.extend_from_slice(&[0u8; 3]); // 13..16 reserved
        buf.extend_from_slice(&9999u64.to_be_bytes()); // 16..24 timestamp
        buf.extend_from_slice(&55u32.to_be_bytes()); // 24..28 file_number
        buf.extend_from_slice(&0x400u32.to_be_bytes()); // 28..32 last_entry

        assert_eq!(buf.len(), FILE_HEADER_SIZE_V2, "v2 header is 32 bytes");

        let mut cursor = Cursor::new(&buf);
        let header = FileHeader::read_from(&mut cursor)
            .expect("v2 header must parse without error");

        assert_eq!(header.log_version, 2);
        assert_eq!(header.file_number, 55);
        assert_eq!(header.last_entry_in_prev_file, 0x400);
        assert_eq!(header.timestamp, 9999);

        // on_disk_size for v2 must be 32, confirming first entry at offset 32.
        assert_eq!(
            FileHeader::on_disk_size(header.log_version),
            32,
            "v2 first-entry offset is 32"
        );
    }

    /// St-C3: verify the reader consumed exactly 32 bytes for a v2 header
    /// (nothing more), so the 4 bytes following the v2 header are not eaten.
    #[test]
    fn test_v2_header_consumes_exactly_32_bytes() {
        let mut buf = Vec::new();
        buf.extend_from_slice(FILE_MAGIC);
        buf.extend_from_slice(&2u32.to_be_bytes());
        buf.push(BYTE_ORDER_BIG_ENDIAN);
        buf.extend_from_slice(&[0u8; 3]);
        buf.extend_from_slice(&1u64.to_be_bytes());
        buf.extend_from_slice(&0u32.to_be_bytes());
        buf.extend_from_slice(&0u32.to_be_bytes());
        // Append 4 sentinel bytes that must NOT be consumed by the v2 read.
        buf.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);

        let mut cursor = Cursor::new(&buf);
        let _header = FileHeader::read_from(&mut cursor).unwrap();

        // Reader must be positioned at byte 32 (after the 32-byte v2 header).
        assert_eq!(
            cursor.position(),
            32,
            "v2 read must consume exactly 32 bytes"
        );
    }

    // -----------------------------------------------------------------------
    // Validate helper
    // -----------------------------------------------------------------------

    #[test]
    fn test_file_header_validate_ok() {
        let header = FileHeader::new(10, 500);
        assert!(header.validate(10).is_ok());
        assert_eq!(header.validate(10).unwrap(), LOG_VERSION);
    }

    #[test]
    fn test_file_header_validate_wrong_number() {
        let header = FileHeader::new(5, 0);
        assert!(header.validate(99).is_err());
    }

    #[test]
    fn test_file_header_validate_future_version_rejected() {
        let mut header = FileHeader::new(0, 0);
        header.log_version = LOG_VERSION + 1;
        assert!(header.validate(0).is_err());
    }

    // -----------------------------------------------------------------------
    // Error path tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_read_rejects_bad_magic() {
        let mut buf = vec![0u8; FILE_HEADER_SIZE];
        buf[..8].copy_from_slice(b"WRONGMAG");
        let mut cursor = Cursor::new(buf);
        assert!(FileHeader::read_from(&mut cursor).is_err());
    }

    #[test]
    fn test_read_rejects_little_endian_byte_order() {
        let header = FileHeader::new(0, 0);
        let mut buf = Vec::new();
        header.write_to(&mut buf).unwrap();
        // Byte 12 is the byte-order marker — change to little-endian.
        buf[12] = BYTE_ORDER_LITTLE_ENDIAN;
        // Recompute CRC so we hit the byte-order check, not the CRC check.
        let new_crc = crc32fast::hash(&buf[..32]);
        buf[32..36].copy_from_slice(&new_crc.to_be_bytes());
        let mut cursor = Cursor::new(buf);
        assert!(FileHeader::read_from(&mut cursor).is_err());
    }

    #[test]
    fn test_read_rejects_old_version() {
        // Write a header with version 1 (below MIN_LOG_VERSION).
        let mut buf = Vec::new();
        buf.extend_from_slice(FILE_MAGIC);
        buf.extend_from_slice(&1u32.to_be_bytes()); // version 1
        buf.push(BYTE_ORDER_BIG_ENDIAN);
        buf.extend_from_slice(&[0u8; 3]);
        buf.extend_from_slice(&42u64.to_be_bytes());
        buf.extend_from_slice(&0u32.to_be_bytes());
        buf.extend_from_slice(&0u32.to_be_bytes());
        let mut cursor = Cursor::new(buf);
        assert!(FileHeader::read_from(&mut cursor).is_err());
    }

    #[test]
    fn test_last_entry_in_prev_file_offset() {
        let header = FileHeader::new(3, 9999);
        assert_eq!(header.last_entry_in_prev_file_offset(), 9999);
    }
}
