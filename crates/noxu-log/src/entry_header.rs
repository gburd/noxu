//! Log entry header.
//!
//!
//! A LogEntryHeader embodies the header information at the beginning of each
//! log entry. The header contains: checksum, entry type, flags, previous
//! entry offset, item size, and optionally a VLSN.

use crate::entry_type::{LOG_VERSION, LogEntryType};
use crate::error::{NoxuLogError, Result};
use crate::provisional::Provisional;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use noxu_util::{Lsn, VLSN_LOG_SIZE, Vlsn};
use std::io::Cursor;

/// Minimum (invariant) size of a log entry header in bytes.
///
/// Layout:
/// - checksum: 4 bytes
/// - entry_type: 1 byte
/// - flags: 1 byte
/// - prev_offset: 4 bytes
/// - item_size: 4 bytes
///
/// Total: 14 bytes
pub const MIN_HEADER_SIZE: usize = 14;

/// Maximum size of a log entry header (when VLSN is present).
pub const MAX_HEADER_SIZE: usize = MIN_HEADER_SIZE + VLSN_LOG_SIZE;

/// Size of the checksum field in bytes.
pub const CHECKSUM_BYTES: usize = 4;

/// Byte offset of the entry type field.
const ENTRY_TYPE_OFFSET: usize = 4;

/// Byte offset of the flags field.
const FLAGS_OFFSET: usize = 5;

/// Byte offset of the previous offset field.
const PREV_OFFSET_OFFSET: usize = 6;

/// Byte offset of the item size field.
const ITEM_SIZE_OFFSET: usize = 10;

/// Byte offset of the VLSN field (if present).
pub const VLSN_OFFSET: usize = MIN_HEADER_SIZE;

// Flag bits in the flags byte
const PROVISIONAL_ALWAYS_MASK: u8 = 0x80;
const PROVISIONAL_BEFORE_CKPT_END_MASK: u8 = 0x40;
const REPLICATED_MASK: u8 = 0x20;
const INVISIBLE_MASK: u8 = 0x10;
const VLSN_PRESENT_MASK: u8 = 0x08;

/// A log entry header containing metadata about a log entry.
#[derive(Debug, Clone)]
pub struct LogEntryHeader {
    /// Checksum value (stored as u32 in the log).
    checksum: u32,

    /// Log entry type.
    entry_type: LogEntryType,

    /// Log version for this entry.
    version: u8,

    /// Size of the log entry payload (not including the header).
    item_size: u32,

    /// Offset of the previous log entry in the file.
    prev_offset: u32,

    /// Optional VLSN for replicated entries.
    vlsn: Option<Vlsn>,

    /// Provisional status.
    provisional: Provisional,

    /// Whether this entry is replicated.
    replicated: bool,

    /// Whether this entry is invisible (used during rollback).
    invisible: bool,

    /// Whether a VLSN is present in the header.
    vlsn_present: bool,
}

impl LogEntryHeader {
    /// Creates a new log entry header for writing.
    ///
    /// # Arguments
    /// * `entry_type` - The type of log entry.
    /// * `item_size` - The size of the entry payload (excluding header).
    /// * `provisional` - The provisional status.
    /// * `replicated` - Whether this entry is replicated.
    /// * `vlsn` - Optional VLSN for replicated entries.
    pub fn new(
        entry_type: LogEntryType,
        item_size: u32,
        provisional: Provisional,
        replicated: bool,
        vlsn: Option<Vlsn>,
    ) -> Self {
        let vlsn_present = vlsn.is_some() || replicated;

        LogEntryHeader {
            checksum: 0, // Will be computed later
            entry_type,
            version: LOG_VERSION,
            item_size,
            prev_offset: 0, // Will be set later
            vlsn,
            provisional,
            replicated,
            invisible: false,
            vlsn_present,
        }
    }

    /// Reads a log entry header from a buffer.
    ///
    /// # Arguments
    /// * `buf` - Buffer containing at least MIN_HEADER_SIZE bytes.
    /// * `lsn` - The LSN of this entry (for error reporting).
    pub fn read_from_log(buf: &[u8], lsn: Lsn) -> Result<Self> {
        if buf.len() < MIN_HEADER_SIZE {
            return Err(NoxuLogError::UnexpectedEof {
                lsn,
                message: format!("header too short: {} bytes", buf.len()),
            });
        }

        let mut cursor = Cursor::new(buf);

        // Read fixed fields
        let checksum = cursor.read_u32::<LittleEndian>()?;
        let entry_type_num = cursor.read_u8()?;
        let flags = cursor.read_u8()?;
        let prev_offset = cursor.read_u32::<LittleEndian>()?;
        let item_size = cursor.read_u32::<LittleEndian>()?;

        // Validate entry type
        let entry_type = LogEntryType::from_type_num(entry_type_num).ok_or(
            NoxuLogError::InvalidEntryType { type_num: entry_type_num, lsn },
        )?;

        // Validate item size
        if item_size > 100_000_000 {
            // Sanity check: 100MB limit
            return Err(NoxuLogError::InvalidEntrySize {
                lsn,
                size: item_size as i32,
            });
        }

        // Parse flags
        let provisional = if (flags & PROVISIONAL_ALWAYS_MASK) != 0 {
            Provisional::Yes
        } else if (flags & PROVISIONAL_BEFORE_CKPT_END_MASK) != 0 {
            Provisional::BeforeCkptEnd
        } else {
            Provisional::No
        };

        let replicated = (flags & REPLICATED_MASK) != 0;
        let invisible = (flags & INVISIBLE_MASK) != 0;
        let vlsn_present = (flags & VLSN_PRESENT_MASK) != 0 || replicated;

        // Read VLSN if present
        let vlsn = if vlsn_present {
            if buf.len() < MAX_HEADER_SIZE {
                return Err(NoxuLogError::UnexpectedEof {
                    lsn,
                    message: "VLSN field truncated".to_string(),
                });
            }
            let vlsn_val = cursor.read_i64::<LittleEndian>()?;
            Some(Vlsn::new(vlsn_val))
        } else {
            None
        };

        Ok(LogEntryHeader {
            checksum,
            entry_type,
            version: LOG_VERSION,
            item_size,
            prev_offset,
            vlsn,
            provisional,
            replicated,
            invisible,
            vlsn_present,
        })
    }

    /// Writes the log entry header to a buffer.
    ///
    /// The checksum and prev_offset fields are initially written as 0 and
    /// must be filled in later via `add_post_marshalling_info`.
    pub fn write_to_log(&self, buf: &mut Vec<u8>) -> Result<()> {
        // Reserve space for checksum (will be filled in later)
        buf.write_u32::<LittleEndian>(0)?;

        // Entry type
        buf.write_u8(self.entry_type.type_num())?;

        // Flags
        let mut flags = 0u8;
        match self.provisional {
            Provisional::Yes => flags |= PROVISIONAL_ALWAYS_MASK,
            Provisional::BeforeCkptEnd => {
                flags |= PROVISIONAL_BEFORE_CKPT_END_MASK
            }
            Provisional::No => {}
        }
        if self.replicated {
            flags |= REPLICATED_MASK;
        }
        if self.invisible {
            flags |= INVISIBLE_MASK;
        }
        if self.vlsn_present {
            flags |= VLSN_PRESENT_MASK;
        }
        buf.write_u8(flags)?;

        // Prev offset (will be filled in later)
        buf.write_u32::<LittleEndian>(0)?;

        // Item size
        buf.write_u32::<LittleEndian>(self.item_size)?;

        // VLSN (if present)
        if self.vlsn_present {
            // Reserve space for VLSN (will be filled in later if needed)
            buf.write_i64::<LittleEndian>(
                self.vlsn
                    .map_or(noxu_util::NULL_VLSN_SEQUENCE, |v| v.sequence()),
            )?;
        }

        Ok(())
    }

    /// Adds post-marshalling information to the header.
    ///
    /// This must be called after the entry has been fully marshalled to
    /// set the previous offset, VLSN, and compute the checksum.
    ///
    /// # Arguments
    /// * `buf` - The complete buffer containing header + entry data.
    /// * `prev_offset` - Offset of the previous log entry.
    /// * `vlsn` - The VLSN to assign (if applicable).
    /// * `checksum` - The computed checksum value.
    pub fn add_post_marshalling_info(
        &mut self,
        buf: &mut [u8],
        prev_offset: u32,
        vlsn: Option<Vlsn>,
        checksum: u32,
    ) -> Result<()> {
        self.prev_offset = prev_offset;
        self.vlsn = vlsn;
        self.checksum = checksum;

        // Write prev_offset at offset 6
        let mut cursor = Cursor::new(&mut buf[PREV_OFFSET_OFFSET..]);
        cursor.write_u32::<LittleEndian>(prev_offset)?;

        // Write VLSN if present
        if let Some(v) = vlsn.filter(|_| self.vlsn_present) {
            let mut cursor = Cursor::new(&mut buf[VLSN_OFFSET..]);
            cursor.write_i64::<LittleEndian>(v.sequence())?;
        }

        // Write checksum at offset 0
        let mut cursor = Cursor::new(&mut buf[0..]);
        cursor.write_u32::<LittleEndian>(checksum)?;

        Ok(())
    }

    /// Returns the size of this header in bytes.
    #[inline]
    pub fn size(&self) -> usize {
        if self.vlsn_present { MAX_HEADER_SIZE } else { MIN_HEADER_SIZE }
    }

    /// Returns the total entry size (header + item).
    #[inline]
    pub fn entry_size(&self) -> usize {
        self.size() + self.item_size as usize
    }

    // Accessors
    #[inline]
    pub fn checksum(&self) -> u32 {
        self.checksum
    }

    #[inline]
    pub fn entry_type(&self) -> LogEntryType {
        self.entry_type
    }

    #[inline]
    pub fn version(&self) -> u8 {
        self.version
    }

    #[inline]
    pub fn item_size(&self) -> u32 {
        self.item_size
    }

    #[inline]
    pub fn prev_offset(&self) -> u32 {
        self.prev_offset
    }

    #[inline]
    pub fn vlsn(&self) -> Option<Vlsn> {
        self.vlsn
    }

    #[inline]
    pub fn provisional(&self) -> Provisional {
        self.provisional
    }

    #[inline]
    pub fn replicated(&self) -> bool {
        self.replicated
    }

    #[inline]
    pub fn invisible(&self) -> bool {
        self.invisible
    }

    #[inline]
    pub fn vlsn_present(&self) -> bool {
        self.vlsn_present
    }

    /// Sets the invisible flag.
    pub fn set_invisible(&mut self, invisible: bool) {
        self.invisible = invisible;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_header_sizes() {
        assert_eq!(MIN_HEADER_SIZE, 14);
        assert_eq!(MAX_HEADER_SIZE, 14 + 8);
    }

    #[test]
    fn test_header_roundtrip_no_vlsn() {
        let header = LogEntryHeader::new(
            LogEntryType::BIN,
            1024,
            Provisional::No,
            false,
            None,
        );

        let mut buf = Vec::new();
        header.write_to_log(&mut buf).unwrap();

        assert_eq!(buf.len(), MIN_HEADER_SIZE);

        let lsn = Lsn::new(1, 100);
        let decoded = LogEntryHeader::read_from_log(&buf, lsn).unwrap();

        assert_eq!(header.entry_type(), decoded.entry_type());
        assert_eq!(header.item_size(), decoded.item_size());
        assert_eq!(header.provisional(), decoded.provisional());
        assert_eq!(header.replicated(), decoded.replicated());
    }

    #[test]
    fn test_header_roundtrip_with_vlsn() {
        let vlsn = Some(Vlsn::new(42));
        let header = LogEntryHeader::new(
            LogEntryType::InsertLNTxn,
            512,
            Provisional::Yes,
            true,
            vlsn,
        );

        let mut buf = Vec::new();
        header.write_to_log(&mut buf).unwrap();

        assert_eq!(buf.len(), MAX_HEADER_SIZE);

        let lsn = Lsn::new(2, 200);
        let decoded = LogEntryHeader::read_from_log(&buf, lsn).unwrap();

        assert_eq!(header.entry_type(), decoded.entry_type());
        assert_eq!(header.vlsn(), decoded.vlsn());
        assert!(decoded.vlsn_present());
    }

    #[test]
    fn test_provisional_flags() {
        for prov in
            [Provisional::No, Provisional::Yes, Provisional::BeforeCkptEnd]
        {
            let header =
                LogEntryHeader::new(LogEntryType::BIN, 100, prov, false, None);
            let mut buf = Vec::new();
            header.write_to_log(&mut buf).unwrap();

            let lsn = Lsn::new(1, 0);
            let decoded = LogEntryHeader::read_from_log(&buf, lsn).unwrap();
            assert_eq!(prov, decoded.provisional());
        }
    }

    #[test]
    fn test_invalid_entry_type() {
        let mut buf = vec![0u8; MIN_HEADER_SIZE];
        buf[ENTRY_TYPE_OFFSET] = 255; // Invalid type

        let lsn = Lsn::new(1, 0);
        let result = LogEntryHeader::read_from_log(&buf, lsn);
        assert!(matches!(result, Err(NoxuLogError::InvalidEntryType { .. })));
    }

    #[test]
    fn test_header_too_short() {
        let buf = vec![0u8; MIN_HEADER_SIZE - 1];
        let lsn = Lsn::new(1, 0);
        let result = LogEntryHeader::read_from_log(&buf, lsn);
        assert!(matches!(result, Err(NoxuLogError::UnexpectedEof { .. })));
    }

    #[test]
    fn test_vlsn_truncated_buffer() {
        // Build a valid header that claims VLSN is present but buffer is too short.
        let mut header = LogEntryHeader::new(
            LogEntryType::InsertLNTxn,
            64,
            Provisional::No,
            true, // replicated => vlsn_present
            None,
        );
        let mut buf = Vec::new();
        header.write_to_log(&mut buf).unwrap();
        // Truncate to MIN_HEADER_SIZE so VLSN field is missing.
        buf.truncate(MIN_HEADER_SIZE);

        let lsn = Lsn::new(1, 0);
        let result = LogEntryHeader::read_from_log(&buf, lsn);
        assert!(matches!(result, Err(NoxuLogError::UnexpectedEof { .. })));

        // suppress unused-mut warning
        let _ = &mut header;
    }

    #[test]
    fn test_invisible_flag_roundtrip() {
        let mut header = LogEntryHeader::new(
            LogEntryType::BIN,
            50,
            Provisional::No,
            false,
            None,
        );
        header.set_invisible(true);
        assert!(header.invisible());

        let mut buf = Vec::new();
        header.write_to_log(&mut buf).unwrap();

        // Rebuild the flags byte with invisible bit set so round-trip works.
        // write_to_log always writes current state; invisible is written via flags.
        let lsn = Lsn::new(1, 0);
        let decoded = LogEntryHeader::read_from_log(&buf, lsn).unwrap();
        assert!(decoded.invisible());

        header.set_invisible(false);
        assert!(!header.invisible());
    }

    #[test]
    fn test_add_post_marshalling_info() {
        let mut header = LogEntryHeader::new(
            LogEntryType::BIN,
            100,
            Provisional::No,
            false,
            None,
        );

        let mut buf = Vec::new();
        header.write_to_log(&mut buf).unwrap();
        assert_eq!(buf.len(), MIN_HEADER_SIZE);

        let prev = 42u32;
        let checksum = 0x1234_5678u32;
        header
            .add_post_marshalling_info(&mut buf, prev, None, checksum)
            .unwrap();

        // Re-read the header and verify the fields were written back.
        let lsn = Lsn::new(1, 0);
        let decoded = LogEntryHeader::read_from_log(&buf, lsn).unwrap();
        assert_eq!(decoded.prev_offset(), prev);
        assert_eq!(decoded.checksum(), checksum);
    }

    #[test]
    fn test_add_post_marshalling_info_with_vlsn() {
        let vlsn = Some(Vlsn::new(99));
        let mut header = LogEntryHeader::new(
            LogEntryType::InsertLNTxn,
            200,
            Provisional::No,
            true,
            vlsn,
        );

        let mut buf = Vec::new();
        header.write_to_log(&mut buf).unwrap();
        assert_eq!(buf.len(), MAX_HEADER_SIZE);

        let new_vlsn = Some(Vlsn::new(101));
        header
            .add_post_marshalling_info(&mut buf, 77, new_vlsn, 0xDEAD)
            .unwrap();

        let lsn = Lsn::new(1, 0);
        let decoded = LogEntryHeader::read_from_log(&buf, lsn).unwrap();
        assert_eq!(decoded.prev_offset(), 77);
        assert_eq!(decoded.checksum(), 0xDEAD);
        assert_eq!(decoded.vlsn(), new_vlsn);
    }

    #[test]
    fn test_oversized_entry_rejected() {
        // Build a buffer with item_size > 100_000_000.
        let mut buf = vec![0u8; MIN_HEADER_SIZE];
        // entry_type byte must be valid
        buf[ENTRY_TYPE_OFFSET] = LogEntryType::BIN.type_num();
        // item_size at ITEM_SIZE_OFFSET (offset 10), little-endian
        let big: u32 = 100_000_001;
        buf[ITEM_SIZE_OFFSET] = (big & 0xFF) as u8;
        buf[ITEM_SIZE_OFFSET + 1] = ((big >> 8) & 0xFF) as u8;
        buf[ITEM_SIZE_OFFSET + 2] = ((big >> 16) & 0xFF) as u8;
        buf[ITEM_SIZE_OFFSET + 3] = ((big >> 24) & 0xFF) as u8;

        let lsn = Lsn::new(1, 0);
        let result = LogEntryHeader::read_from_log(&buf, lsn);
        assert!(matches!(result, Err(NoxuLogError::InvalidEntrySize { .. })));
    }

    #[test]
    fn test_replicated_flag_roundtrip() {
        let header = LogEntryHeader::new(
            LogEntryType::InsertLNTxn,
            32,
            Provisional::No,
            true,
            Some(Vlsn::new(7)),
        );

        let mut buf = Vec::new();
        header.write_to_log(&mut buf).unwrap();

        let lsn = Lsn::new(1, 0);
        let decoded = LogEntryHeader::read_from_log(&buf, lsn).unwrap();
        assert!(decoded.replicated());
        assert!(decoded.vlsn_present());
        assert_eq!(decoded.vlsn(), Some(Vlsn::new(7)));
    }

    #[test]
    fn test_entry_size_and_header_size() {
        let h_no_vlsn = LogEntryHeader::new(
            LogEntryType::BIN,
            100,
            Provisional::No,
            false,
            None,
        );
        assert_eq!(h_no_vlsn.size(), MIN_HEADER_SIZE);
        assert_eq!(h_no_vlsn.entry_size(), MIN_HEADER_SIZE + 100);

        let h_with_vlsn = LogEntryHeader::new(
            LogEntryType::InsertLNTxn,
            50,
            Provisional::No,
            true,
            Some(Vlsn::new(1)),
        );
        assert_eq!(h_with_vlsn.size(), MAX_HEADER_SIZE);
        assert_eq!(h_with_vlsn.entry_size(), MAX_HEADER_SIZE + 50);
    }
}
