//! FileSummaryLN log entry.
//!
//! Records per-file utilization statistics used by the cleaner to determine
//! which log files are candidates for cleaning. Written periodically by the
//! cleaner and during checkpoints.
//!
//! # On-disk layout (C7)
//!
//! Faithful port of JE `FileSummaryLN.writeToLog`, which serializes the base
//! `FileSummary` (`FileSummary.writeToLog`, 11 ints) followed by the
//! `PackedOffsets` of obsolete-LN offsets (`PackedOffsets.writeToLog`).
//!
//! Originally this entry kept only five aggregate counters (total_count,
//! total_size, obsolete_count, obsolete_size, obsolete_size_counted) and
//! DROPPED the LN/IN breakdown, `max_ln_size`, and the obsolete-offset list
//! that the in-memory `TrackedFileSummary` tracks (census L-28 / T-15).  C7
//! restores the full breakdown so the persisted form is as faithful as the
//! in-memory one — this is required by CLN-4's recovery-time profile rebuild,
//! which must seed the cleaner with the same `FileSummary` it would have had
//! before the restart (including the average-size estimation that depends on
//! the LN/IN split and `max_ln_size`).
//!
//! The 11 breakdown ints mirror `FileSummary.writeToLog`:
//! `totalCount, totalSize, totalINCount, totalINSize, totalLNCount,
//!  totalLNSize, maxLNSize, obsoleteINCount, obsoleteLNCount, obsoleteLNSize,
//!  obsoleteLNSizeCounted`.  Following them is the obsolete-offset blob,
//! length-prefixed (the Noxu `PackedOffsets` delta-varint encoding, distinct
//! from JE's short-array encoding but round-trip equivalent).
//!
//! CLN-24 appends one more length-prefixed trailer after the obsolete-offset
//! blob: the serialized per-file expiration histogram
//! (`ExpirationTracker::serialize`).  JE keeps this in a separate EXPIRATION
//! DB (`FileExpirationLN`); Noxu folds it into the FileSummaryLN trailer so a
//! single record restores both utilization and the TTL expiration prediction
//! at recovery.  The trailer is optional on read — pre-CLN-24 entries decode
//! it as empty.

use byteorder::{BigEndian, ReadBytesExt};
use bytes::{BufMut, BytesMut};
use std::io::{self, Cursor, Read};
use thiserror::Error;

/// Error type for FileSummaryLN log entry operations.
#[derive(Debug, Error)]
pub enum FileSummaryLnEntryError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}

/// FileSummaryLN log entry.
///
/// Stores utilization statistics for a single log file. The cleaner reads
/// these entries to determine file utilization and select files for cleaning.
///
/// # Fields
///
/// - `file_number`: Log file number these statistics apply to.
/// - the 11 `FileSummary` breakdown counters (see field docs).
/// - `obsolete_offset_count` / `obsolete_offset_data`: the packed
///   obsolete-LN offset list (C7), carried verbatim from the in-memory
///   `PackedOffsets`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSummaryLnEntry {
    /// Log file number these statistics apply to.
    pub file_number: u64,
    /// Total number of log entries in the file.
    pub total_count: i32,
    /// Total byte size of all log entries in the file.
    pub total_size: i32,
    /// Number of IN log entries.
    pub total_in_count: i32,
    /// Byte size of IN log entries.
    pub total_in_size: i32,
    /// Number of LN log entries.
    pub total_ln_count: i32,
    /// Byte size of LN log entries.
    pub total_ln_size: i32,
    /// Byte size of the largest LN log entry (C7 / version 8 `maxLNSize`).
    pub max_ln_size: i32,
    /// Number of obsolete IN log entries.
    pub obsolete_in_count: i32,
    /// Number of obsolete LN log entries.
    pub obsolete_ln_count: i32,
    /// Byte size of obsolete LN log entries.
    pub obsolete_ln_size: i32,
    /// Number of obsolete LNs whose size was counted.
    pub obsolete_ln_size_counted: i32,
    /// Number of obsolete offsets in `obsolete_offset_data` (C7 PackedOffsets).
    pub obsolete_offset_count: u32,
    /// Packed (delta-varint) obsolete-LN offset bytes (C7 PackedOffsets).
    pub obsolete_offset_data: Vec<u8>,
    /// Serialized per-file expiration histogram (CLN-24).
    ///
    /// The byte form produced by `ExpirationTracker::serialize` (JE
    /// `ExpirationTracker.serialize` — byte 0 day/hour flag + RLE
    /// `{interval,size}` packed pairs).  Empty when the file has no
    /// expiring (TTL) data, or for FileSummaryLN entries written before
    /// CLN-24 (the trailing blob is absent and reads back as empty).
    ///
    /// JE keeps this in a separate EXPIRATION DB (`FileExpirationLN`);
    /// Noxu folds it into the FileSummaryLN trailer alongside the C7
    /// breakdown so a single record carries both the utilization and the
    /// expiration prediction the cleaner needs after restart.
    pub expiration_histogram: Vec<u8>,
}

impl FileSummaryLnEntry {
    /// Creates a new FileSummaryLN entry from the full breakdown.
    ///
    /// `obsolete_offset_count` / `obsolete_offset_data` carry the packed
    /// obsolete-offset list (pass `0` / empty if detail tracking is off).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        file_number: u64,
        total_count: i32,
        total_size: i32,
        total_in_count: i32,
        total_in_size: i32,
        total_ln_count: i32,
        total_ln_size: i32,
        max_ln_size: i32,
        obsolete_in_count: i32,
        obsolete_ln_count: i32,
        obsolete_ln_size: i32,
        obsolete_ln_size_counted: i32,
        obsolete_offset_count: u32,
        obsolete_offset_data: Vec<u8>,
        expiration_histogram: Vec<u8>,
    ) -> Self {
        Self {
            file_number,
            total_count,
            total_size,
            total_in_count,
            total_in_size,
            total_ln_count,
            total_ln_size,
            max_ln_size,
            obsolete_in_count,
            obsolete_ln_count,
            obsolete_ln_size,
            obsolete_ln_size_counted,
            obsolete_offset_count,
            obsolete_offset_data,
            expiration_histogram,
        }
    }

    /// Returns the serialized size in bytes.
    pub fn log_size(&self) -> usize {
        8 + // file_number
        (11 * 4) + // 11 FileSummary breakdown ints
        4 + // obsolete_offset_count
        4 + // obsolete_offset_data length prefix
        self.obsolete_offset_data.len() +
        4 + // CLN-24 expiration_histogram length prefix
        self.expiration_histogram.len()
    }

    /// Writes this entry to a buffer.
    ///
    /// JE: `FileSummaryLN.writeToLog` -> `baseSummary.writeToLog` (11 ints)
    /// then `obsoleteOffsets.writeToLog`.
    pub fn write_to_log(&self, buf: &mut BytesMut) {
        buf.put_u64(self.file_number);
        // FileSummary.writeToLog: 11 ints in JE field order.
        buf.put_i32(self.total_count);
        buf.put_i32(self.total_size);
        buf.put_i32(self.total_in_count);
        buf.put_i32(self.total_in_size);
        buf.put_i32(self.total_ln_count);
        buf.put_i32(self.total_ln_size);
        buf.put_i32(self.max_ln_size);
        buf.put_i32(self.obsolete_in_count);
        buf.put_i32(self.obsolete_ln_count);
        buf.put_i32(self.obsolete_ln_size);
        buf.put_i32(self.obsolete_ln_size_counted);
        // PackedOffsets.writeToLog: count, then length-prefixed packed bytes.
        buf.put_u32(self.obsolete_offset_count);
        buf.put_u32(self.obsolete_offset_data.len() as u32);
        buf.put_slice(&self.obsolete_offset_data);
        // CLN-24: length-prefixed serialized expiration histogram trailer.
        buf.put_u32(self.expiration_histogram.len() as u32);
        buf.put_slice(&self.expiration_histogram);
    }

    /// Reads an entry from a buffer.
    pub fn read_from_log(buf: &[u8]) -> Result<Self, FileSummaryLnEntryError> {
        let mut cursor = Cursor::new(buf);
        let file_number = cursor.read_u64::<BigEndian>()?;
        let total_count = cursor.read_i32::<BigEndian>()?;
        let total_size = cursor.read_i32::<BigEndian>()?;
        let total_in_count = cursor.read_i32::<BigEndian>()?;
        let total_in_size = cursor.read_i32::<BigEndian>()?;
        let total_ln_count = cursor.read_i32::<BigEndian>()?;
        let total_ln_size = cursor.read_i32::<BigEndian>()?;
        let max_ln_size = cursor.read_i32::<BigEndian>()?;
        let obsolete_in_count = cursor.read_i32::<BigEndian>()?;
        let obsolete_ln_count = cursor.read_i32::<BigEndian>()?;
        let obsolete_ln_size = cursor.read_i32::<BigEndian>()?;
        let obsolete_ln_size_counted = cursor.read_i32::<BigEndian>()?;
        let obsolete_offset_count = cursor.read_u32::<BigEndian>()?;
        let data_len = cursor.read_u32::<BigEndian>()? as usize;
        let mut obsolete_offset_data = vec![0u8; data_len];
        cursor.read_exact(&mut obsolete_offset_data)?;
        // CLN-24: the expiration-histogram trailer is optional.  Pre-CLN-24
        // entries have no trailing length prefix; treat EOF as "empty".
        let expiration_histogram = match cursor.read_u32::<BigEndian>() {
            Ok(hlen) => {
                let mut h = vec![0u8; hlen as usize];
                cursor.read_exact(&mut h)?;
                h
            }
            Err(_) => Vec::new(),
        };
        Ok(Self {
            file_number,
            total_count,
            total_size,
            total_in_count,
            total_in_size,
            total_ln_count,
            total_ln_size,
            max_ln_size,
            obsolete_in_count,
            obsolete_ln_count,
            obsolete_ln_size,
            obsolete_ln_size_counted,
            obsolete_offset_count,
            obsolete_offset_data,
            expiration_histogram,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_summary_ln_roundtrip() {
        let entry = FileSummaryLnEntry::new(
            5,
            1000,
            512000,
            100,
            51200,
            900,
            460800,
            4096,
            5,
            200,
            102400,
            200,
            3,
            vec![0xAC, 0x02, 0x01],
            vec![0, 0xAC, 0x02, 0x10], // CLN-24 expiration histogram blob
        );

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = FileSummaryLnEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
        assert_eq!(decoded.file_number, 5);
        assert_eq!(decoded.total_count, 1000);
        assert_eq!(decoded.total_size, 512000);
        assert_eq!(decoded.total_in_count, 100);
        assert_eq!(decoded.total_ln_count, 900);
        assert_eq!(decoded.max_ln_size, 4096);
        assert_eq!(decoded.obsolete_ln_count, 200);
        assert_eq!(decoded.obsolete_ln_size, 102400);
        assert_eq!(decoded.obsolete_ln_size_counted, 200);
        // C7: the packed obsolete-offset blob round-trips verbatim.
        assert_eq!(decoded.obsolete_offset_count, 3);
        assert_eq!(decoded.obsolete_offset_data, vec![0xAC, 0x02, 0x01]);
        // CLN-24: the expiration histogram trailer round-trips verbatim.
        assert_eq!(decoded.expiration_histogram, vec![0, 0xAC, 0x02, 0x10]);
    }

    #[test]
    fn test_file_summary_ln_empty_offsets() {
        let entry = FileSummaryLnEntry::new(
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            0,
            Vec::new(),
            Vec::new(),
        );

        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);

        let decoded = FileSummaryLnEntry::read_from_log(&buf).unwrap();
        assert_eq!(entry, decoded);
        assert_eq!(decoded.obsolete_offset_count, 0);
        assert!(decoded.obsolete_offset_data.is_empty());
        assert!(decoded.expiration_histogram.is_empty());
    }

    #[test]
    fn test_log_size() {
        let entry = FileSummaryLnEntry::new(
            1,
            10,
            100,
            2,
            20,
            8,
            80,
            16,
            1,
            5,
            50,
            5,
            2,
            vec![1, 2, 3, 4],
            vec![5, 6],
        );
        assert_eq!(entry.log_size(), 8 + (11 * 4) + 4 + 4 + 4 + 4 + 2);
        let mut buf = BytesMut::new();
        entry.write_to_log(&mut buf);
        assert_eq!(buf.len(), entry.log_size());
    }

    /// CLN-24: a FileSummaryLN written WITHOUT the expiration trailer (the
    /// pre-CLN-24 on-disk form) must still decode — the missing trailer
    /// reads back as an empty histogram.
    #[test]
    fn test_file_summary_ln_backward_compat_no_trailer() {
        // Hand-build the pre-CLN-24 byte layout: 8 + 11*4 ints + offset
        // count + length-prefixed offset blob, with NO trailing histogram.
        let mut buf = BytesMut::new();
        buf.put_u64(9);
        for _ in 0..11 {
            buf.put_i32(0);
        }
        buf.put_u32(0); // obsolete_offset_count
        buf.put_u32(0); // obsolete_offset_data length
        // (no expiration_histogram trailer)
        let decoded = FileSummaryLnEntry::read_from_log(&buf).unwrap();
        assert_eq!(decoded.file_number, 9);
        assert!(decoded.expiration_histogram.is_empty());
    }
}
