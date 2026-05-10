//! File header for Noxu DB log files.
//!
//! Each log file starts with a self-describing 32-byte header that allows any
//! reader to identify the file as a Noxu DB log, determine its format version,
//! and detect the byte order used when the file was written.
//!
//! # Header layout (32 bytes)
//!
//! ```text
//! Offset  Size  Field
//! ------  ----  -----
//!      0     8  magic           "NOXUDB\0\0" (0x4E4F5855_44420000)
//!      8     4  log_version     u32 big-endian, currently LOG_VERSION (2)
//!     12     1  byte_order      0x00 = big-endian (default), 0x01 = little-endian
//!     13     3  _reserved       zero-filled, reserved for future use
//!     16     8  timestamp       u64 big-endian, Unix time in milliseconds
//!     24     4  file_number     u32 big-endian, 0-based sequential file index
//!     28     4  last_entry_in_prev_file  u32 big-endian, offset in previous file
//!                               (0 for the first file in an environment)
//! ```
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
///   making the header self-describing and portable (32 bytes total)
pub const LOG_VERSION: u32 = 2;

/// Minimum supported log file format version.
pub const MIN_LOG_VERSION: u32 = 2;

/// Size of the file header on disk (bytes).
///
/// Layout: magic(8) + log_version(4) + byte_order(1) + _pad(3)
///       + timestamp(8) + file_number(4) + last_entry_in_prev_file(4) = 32
pub const FILE_HEADER_SIZE: usize = 8 + 4 + 1 + 3 + 8 + 4 + 4;

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

    /// Reads and validates a file header from `reader`.
    ///
    /// Returns `LogError::InvalidHeader` if the magic bytes are wrong or if
    /// the byte-order marker is not big-endian.
    /// Returns `LogError::VersionMismatch` if `log_version < MIN_LOG_VERSION`.
    pub fn read_from<R: Read>(reader: &mut R) -> Result<Self> {
        // --- Magic ---
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

        // --- Version ---
        let log_version = reader.read_u32::<BigEndian>()?;
        if log_version < MIN_LOG_VERSION {
            return Err(LogError::VersionMismatch {
                expected: LOG_VERSION,
                found: log_version,
                file_num: u32::MAX,
            });
        }

        // --- Byte-order marker ---
        let byte_order = reader.read_u8()?;
        if byte_order != BYTE_ORDER_BIG_ENDIAN {
            return Err(LogError::InvalidHeader {
                file_num: u32::MAX,
                message: format!(
                    "unsupported byte order: 0x{:02X} (only big-endian 0x00 is supported)",
                    byte_order
                ),
            });
        }

        // --- 3 reserved bytes ---
        let mut _reserved = [0u8; 3];
        reader.read_exact(&mut _reserved)?;

        // --- Payload ---
        let timestamp = reader.read_u64::<BigEndian>()?;
        let file_number = reader.read_u32::<BigEndian>()?;
        let last_entry_in_prev_file = reader.read_u32::<BigEndian>()?;

        Ok(FileHeader {
            timestamp,
            file_number,
            last_entry_in_prev_file,
            log_version,
        })
    }

    /// Writes the file header to `writer`.
    pub fn write_to<W: Write>(&self, writer: &mut W) -> io::Result<()> {
        // Magic
        writer.write_all(FILE_MAGIC)?;
        // Version
        writer.write_u32::<BigEndian>(self.log_version)?;
        // Byte-order marker
        writer.write_u8(BYTE_ORDER_BIG_ENDIAN)?;
        // Reserved bytes
        writer.write_all(&[0u8; 3])?;
        // Payload
        writer.write_u64::<BigEndian>(self.timestamp)?;
        writer.write_u32::<BigEndian>(self.file_number)?;
        writer.write_u32::<BigEndian>(self.last_entry_in_prev_file)?;
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

    /// Returns the size of the file header in bytes.
    pub const fn size() -> usize {
        FILE_HEADER_SIZE
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_file_header_size_is_32() {
        assert_eq!(FILE_HEADER_SIZE, 32);
        assert_eq!(FileHeader::size(), 32);
    }

    #[test]
    fn test_log_version_is_2() {
        assert_eq!(LOG_VERSION, 2);
    }

    #[test]
    fn test_magic_bytes() {
        assert_eq!(FILE_MAGIC, b"NOXUDB\0\0");
        assert_eq!(FILE_MAGIC.len(), 8);
    }

    #[test]
    fn test_file_header_roundtrip() {
        let header = FileHeader::new(42, 0x1000);

        let mut buf = Vec::new();
        header.write_to(&mut buf).unwrap();

        assert_eq!(buf.len(), FILE_HEADER_SIZE);

        // First 8 bytes must be the magic.
        assert_eq!(&buf[..8], FILE_MAGIC);
        // Bytes 8..12 are LOG_VERSION big-endian.
        assert_eq!(&buf[8..12], &LOG_VERSION.to_be_bytes());
        // Byte 12 is BYTE_ORDER_BIG_ENDIAN.
        assert_eq!(buf[12], BYTE_ORDER_BIG_ENDIAN);
        // Bytes 13..16 are reserved zeros.
        assert_eq!(&buf[13..16], &[0u8, 0, 0]);

        let mut cursor = Cursor::new(buf);
        let decoded = FileHeader::read_from(&mut cursor).unwrap();

        assert_eq!(decoded.file_number, 42);
        assert_eq!(decoded.last_entry_in_prev_file, 0x1000);
        assert_eq!(decoded.log_version, LOG_VERSION);
        assert!(decoded.timestamp > 0);
    }

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

    #[test]
    fn test_read_rejects_bad_magic() {
        let mut buf = vec![0u8; FILE_HEADER_SIZE];
        // Write wrong magic in the first 8 bytes.
        buf[..8].copy_from_slice(b"WRONGMAG");
        let mut cursor = Cursor::new(buf);
        assert!(FileHeader::read_from(&mut cursor).is_err());
    }

    #[test]
    fn test_read_rejects_little_endian_byte_order() {
        // Build a valid header and then flip the byte-order byte.
        let header = FileHeader::new(0, 0);
        let mut buf = Vec::new();
        header.write_to(&mut buf).unwrap();
        // Byte 12 is the byte-order marker — change to little-endian.
        buf[12] = BYTE_ORDER_LITTLE_ENDIAN;
        let mut cursor = Cursor::new(buf);
        assert!(FileHeader::read_from(&mut cursor).is_err());
    }

    #[test]
    fn test_read_rejects_old_version() {
        // Write a header with version 1 (below MIN_LOG_VERSION).
        let mut buf = Vec::new();
        buf.extend_from_slice(FILE_MAGIC);
        buf.extend_from_slice(&1u32.to_be_bytes());   // version 1
        buf.push(BYTE_ORDER_BIG_ENDIAN);
        buf.extend_from_slice(&[0u8; 3]);             // reserved
        buf.extend_from_slice(&42u64.to_be_bytes());  // timestamp
        buf.extend_from_slice(&0u32.to_be_bytes());   // file_number
        buf.extend_from_slice(&0u32.to_be_bytes());   // last_entry
        let mut cursor = Cursor::new(buf);
        assert!(FileHeader::read_from(&mut cursor).is_err());
    }

    #[test]
    fn test_last_entry_in_prev_file_offset() {
        let header = FileHeader::new(3, 9999);
        assert_eq!(header.last_entry_in_prev_file_offset(), 9999);
    }
}
