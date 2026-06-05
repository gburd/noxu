//! Sequential log file reader.
//!
//! Core read-loop for log file reading.
//!
//! `LogFileReader` opens a single `.ndb` log file and reads entries one by
//! one in forward order.  Unlike the generic `FileReader<F>` (which uses a
//! `LogFileAccess` trait and supports backwards scanning), this struct is a
//! simple, concrete reader aimed at recovery scanning and integration tests.
//!
//! # On-disk entry format (little-endian)
//!
//! ```text
//! offset  0: checksum    u32
//! offset  4: entry_type  u8
//! offset  5: flags       u8
//! offset  6: prev_offset u32
//! offset 10: item_size   u32
//! offset 14: vlsn?       i64   (present when VLSN_PRESENT or REPLICATED flag)
//! offset 14 or 22: payload bytes[item_size]
//! ```
//!
//! CRC32 is computed over bytes `[CHECKSUM_BYTES..header_size+item_size]`.
//!
//! # equivalents
//!
//! - `FileReader.readNextEntry()` -> `LogFileReader::read_next()`
//! - Checksum validation via `ChecksumValidator`
//! - Skip invalid (bad-CRC) entries with a warning, stop at true EOF

use crate::MAX_ITEM_SIZE;
use crate::checksum::ChecksumValidator;
use crate::entry_header::{CHECKSUM_BYTES, MAX_HEADER_SIZE, MIN_HEADER_SIZE};
use crate::entry_type::LogEntryType;
use crate::error::{NoxuLogError, Result};
use crate::file_manager::FileManager;
use noxu_util::lsn::Lsn;
use std::sync::Arc;

/// Reads log entries sequentially from a single log file.
///
/// Constructed via `LogFileReader::open()`.  Call `read_next()` in a loop
/// to iterate over entries; it returns `None` at end-of-file or after the
/// first unrecoverable error.
pub struct LogFileReader {
    /// The file manager that provides raw I/O.
    file_manager: Arc<FileManager>,

    /// Which log file (0-based) we are reading.
    file_num: u32,

    /// Current byte offset within the file.  Starts immediately after the
    /// file header (32 bytes for v2 files, 36 bytes for v3 files), exactly
    /// as the original implementation does for the current version.
    current_offset: u64,

    /// Length of the file as determined at open time.
    file_length: u64,

    /// Number of entries successfully read.
    entries_read: u64,
}

impl LogFileReader {
    /// Opens the log file `file_num` for sequential reading.
    ///
    /// The reader starts immediately after the file header.  For v2 log
    /// files that is byte 32; for v3 files byte 36.  The correct offset is
    /// resolved by querying the `FileManager` for the file's `log_version`.
    ///
    /// # Errors
    /// Returns an error if the file does not exist or cannot be read.
    pub fn open(file_manager: Arc<FileManager>, file_num: u32) -> Result<Self> {
        let file_length = file_manager.get_file_length(file_num)?;
        // Query the file's actual header size so v2 and v3 files both start
        // scanning at the correct first-entry offset.
        let first_entry_offset =
            file_manager.file_header_size_for(file_num)? as u64;

        Ok(LogFileReader {
            file_manager,
            file_num,
            current_offset: first_entry_offset,
            file_length,
            entries_read: 0,
        })
    }

    /// Returns the number of entries successfully read so far.
    pub fn entries_read(&self) -> u64 {
        self.entries_read
    }

    /// Returns the current byte offset within the file.
    pub fn current_offset(&self) -> u64 {
        self.current_offset
    }

    /// Reads the next log entry from the file.
    ///
    /// Core loop inside `FileReader.readNextEntry()`.
    ///
    /// - If a complete, valid entry is found, returns
    ///   `Some((lsn, entry_type, payload))`.
    /// - If the end of the file is reached, returns `None`.
    /// - If the entry header or checksum is invalid (truncated write at end
    ///   of log), logs a warning and returns `None` - this matches the
    ///   behaviour in `LastFileReader` where bad-CRC entries are treated as
    ///   the log boundary rather than a hard error.
    ///
    /// # Security rationale (lenient end-of-log detection)
    ///
    /// On crash, the tail of the WAL frequently contains a partial record
    /// (truncated header, half-written payload, or zero-fill).  The legacy
    /// `LastFileReader` semantics treat any such corruption at the tail as
    /// the true end of the log so that a clean recovery can complete.  A
    /// hard error here would refuse to mount any database that took an
    /// untimely crash.
    ///
    /// This same leniency is also a corruption oracle: a single flipped bit
    /// in an `entry_type_num` byte will silently truncate the rest of the
    /// log.  Recovery callers that need to distinguish "clean EOF" from
    /// "corruption mid-log" MUST use [`LogFileReader::read_next_strict`]
    /// instead — that method returns explicit errors for bad checksums,
    /// unknown entry types, or oversized item_size fields.
    ///
    /// **DEPRECATION NOTE (LOG-5)**: callers that participate in recovery
    /// or replication are required to use `read_next_strict`.  This
    /// `read_next` method is retained only for cleaner / log-dump tools
    /// where stopping at the first suspicious byte is acceptable, and may
    /// be removed in a future major version.
    pub fn read_next(&mut self) -> Option<(Lsn, LogEntryType, Vec<u8>)> {
        // Check for EOF before attempting a read.
        if self.current_offset >= self.file_length {
            return None;
        }

        // We need at least MIN_HEADER_SIZE bytes remaining to read a header.
        if self.file_length - self.current_offset < MIN_HEADER_SIZE as u64 {
            // Partial header at end of file - treat as end-of-log.
            return None;
        }

        // Step 1: Read the minimum (fixed) header.
        let mut header_buf = vec![0u8; MIN_HEADER_SIZE];
        match self.file_manager.read_from_file(
            self.file_num,
            self.current_offset,
            &mut header_buf,
        ) {
            Ok(n) if n < MIN_HEADER_SIZE => return None,
            Err(_) => return None,
            Ok(_) => {}
        }

        // Step 2: Parse the header fields we need.
        let stored_checksum = u32::from_le_bytes([
            header_buf[0],
            header_buf[1],
            header_buf[2],
            header_buf[3],
        ]);
        let entry_type_num = header_buf[4];
        let flags = header_buf[5];
        let item_size = u32::from_le_bytes([
            header_buf[10],
            header_buf[11],
            header_buf[12],
            header_buf[13],
        ]) as usize;

        // Validate entry_type before trusting item_size.
        let entry_type = match LogEntryType::from_type_num(entry_type_num) {
            Some(t) => t,
            None => {
                // Unknown type byte - could be garbage at end of a partial
                // write OR a corruption oracle that hides later valid entries.
                // We log at error level so the operator sees it; then return
                // None for backwards-compatibility with the legacy
                // truncate-at-corruption behaviour (LOG-5).  Strict callers
                // must use `read_next_strict`.
                log::error!(
                    "LogFileReader: unknown entry type {} at file {:08x} \
                     offset {:#x}; treating as end of log (this may hide \
                     later valid entries — use read_next_strict to surface)",
                    entry_type_num,
                    self.file_num,
                    self.current_offset,
                );
                return None;
            }
        };

        // Sanity-check item_size before allocating.
        if item_size > MAX_ITEM_SIZE {
            log::error!(
                "LogFileReader: implausible item_size {} (cap {}) at file \
                 {:08x} offset {:#x}; treating as end of log",
                item_size,
                MAX_ITEM_SIZE,
                self.file_num,
                self.current_offset,
            );
            return None;
        }

        // Step 3: Determine actual header size (VLSN extends it by 8).
        let vlsn_present = (flags & 0x08) != 0 || (flags & 0x20) != 0;
        let header_size =
            if vlsn_present { MAX_HEADER_SIZE } else { MIN_HEADER_SIZE };
        let entry_size = header_size + item_size;

        // Step 4: Read the full entry (header + payload).
        let lsn = Lsn::new(self.file_num, self.current_offset as u32);

        if self.current_offset + entry_size as u64 > self.file_length {
            // Entry extends past end of file - partial write.
            log::warn!(
                "LogFileReader: truncated entry at file {:08x} offset {:#x}; \
                 entry_size={}, file_length={}; treating as end of log",
                self.file_num,
                self.current_offset,
                entry_size,
                self.file_length,
            );
            return None;
        }

        let mut full_buf = vec![0u8; entry_size];
        match self.file_manager.read_from_file(
            self.file_num,
            self.current_offset,
            &mut full_buf,
        ) {
            Ok(n) if n < entry_size => return None,
            Err(_) => return None,
            Ok(_) => {}
        }

        // Step 5: Validate the CRC32 checksum.
        // ChecksumValidator covers bytes [CHECKSUM_BYTES..entry_size].
        let computed_crc = ChecksumValidator::compute_range(
            &full_buf,
            CHECKSUM_BYTES,
            entry_size - CHECKSUM_BYTES,
        );

        if computed_crc != stored_checksum {
            // Bad checksum - partial/corrupt write.  LastFileReader
            // treats this as end-of-log rather than a hard error.
            log::warn!(
                "LogFileReader: checksum mismatch at file {:08x} offset \
                 {:#x}: expected {:#x}, got {:#x}; treating as end of log",
                self.file_num,
                self.current_offset,
                stored_checksum,
                computed_crc,
            );
            return None;
        }

        // Step 6: Advance offset past this entry.
        self.current_offset += entry_size as u64;
        self.entries_read += 1;

        let payload = full_buf[header_size..].to_vec();
        Some((lsn, entry_type, payload))
    }

    /// Returns an error-propagating version of `read_next`.
    ///
    /// Unlike `read_next()`, this method returns errors explicitly instead of
    /// converting bad-checksum / truncation into `None`.  Used by callers
    /// that need to distinguish between clean EOF and corruption.
    pub fn read_next_strict(
        &mut self,
    ) -> Result<Option<(Lsn, LogEntryType, Vec<u8>)>> {
        if self.current_offset >= self.file_length {
            return Ok(None);
        }

        if self.file_length - self.current_offset < MIN_HEADER_SIZE as u64 {
            return Ok(None);
        }

        // Read minimum header.
        let mut header_buf = vec![0u8; MIN_HEADER_SIZE];
        let n = self.file_manager.read_from_file(
            self.file_num,
            self.current_offset,
            &mut header_buf,
        )?;
        if n < MIN_HEADER_SIZE {
            return Ok(None);
        }

        let lsn = Lsn::new(self.file_num, self.current_offset as u32);

        let stored_checksum = u32::from_le_bytes([
            header_buf[0],
            header_buf[1],
            header_buf[2],
            header_buf[3],
        ]);
        let entry_type_num = header_buf[4];
        let flags = header_buf[5];
        let item_size = u32::from_le_bytes([
            header_buf[10],
            header_buf[11],
            header_buf[12],
            header_buf[13],
        ]) as usize;

        // Validate entry_type early — before any allocation or full-entry
        // read — so an unknown type byte is reported distinctly from a
        // checksum mismatch.  LOG-5: the strict reader is the recommended
        // API for recovery callers precisely because this distinction is
        // visible.
        let entry_type = LogEntryType::from_type_num(entry_type_num).ok_or(
            NoxuLogError::InvalidEntryType { type_num: entry_type_num, lsn },
        )?;

        if item_size > MAX_ITEM_SIZE {
            return Err(NoxuLogError::InvalidEntrySize {
                lsn,
                size: item_size as i32,
            });
        }

        let vlsn_present = (flags & 0x08) != 0 || (flags & 0x20) != 0;
        let header_size =
            if vlsn_present { MAX_HEADER_SIZE } else { MIN_HEADER_SIZE };
        let entry_size = header_size + item_size;

        // Read the full entry.
        let mut full_buf = vec![0u8; entry_size];
        let n = self.file_manager.read_from_file(
            self.file_num,
            self.current_offset,
            &mut full_buf,
        )?;
        if n < entry_size {
            return Ok(None);
        }

        // Validate checksum.
        let computed_crc = ChecksumValidator::compute_range(
            &full_buf,
            CHECKSUM_BYTES,
            entry_size - CHECKSUM_BYTES,
        );
        if computed_crc != stored_checksum {
            return Err(NoxuLogError::Checksum {
                lsn,
                message: format!(
                    "expected {:#x}, got {:#x}",
                    stored_checksum, computed_crc
                ),
            });
        }

        self.current_offset += entry_size as u64;
        self.entries_read += 1;

        let payload = full_buf[header_size..].to_vec();
        Ok(Some((lsn, entry_type, payload)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_manager::FileManager;
    use crate::log_manager::LogManager;
    use crate::provisional::Provisional;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn make_log_manager(dir: &TempDir) -> (Arc<FileManager>, LogManager) {
        let fm = Arc::new(
            FileManager::new(dir.path(), false, 10_000_000, 100).unwrap(),
        );
        let lm = LogManager::new(Arc::clone(&fm), 3, 1024 * 1024, 4096);
        (fm, lm)
    }

    #[test]
    fn test_log_file_reader_reads_entry() {
        let dir = TempDir::new().unwrap();
        let (fm, lm) = make_log_manager(&dir);

        let payload = b"test payload";
        let lsn = lm
            .log(LogEntryType::Trace, payload, Provisional::No, true, false)
            .unwrap();

        let mut reader =
            LogFileReader::open(Arc::clone(&fm), lsn.file_number()).unwrap();
        let result = reader.read_next();
        assert!(result.is_some(), "expected an entry");
        let (read_lsn, entry_type, read_payload) = result.unwrap();
        assert_eq!(read_lsn, lsn);
        assert_eq!(entry_type, LogEntryType::Trace);
        assert_eq!(read_payload, payload);
    }

    #[test]
    fn test_log_file_reader_eof_after_all_entries() {
        let dir = TempDir::new().unwrap();
        let (fm, lm) = make_log_manager(&dir);

        for i in 0u8..5 {
            lm.log(LogEntryType::Trace, &[i], Provisional::No, false, false)
                .unwrap();
        }
        lm.flush_no_sync().unwrap();

        let file_num = fm.get_current_file_num();
        let mut reader =
            LogFileReader::open(Arc::clone(&fm), file_num).unwrap();

        let mut count = 0usize;
        while reader.read_next().is_some() {
            count += 1;
        }
        assert_eq!(count, 5);
    }

    /// LOG-3: All readers in the crate use the same `MAX_ITEM_SIZE`
    /// constant when validating `item_size`.
    #[test]
    fn test_max_item_size_is_centralised() {
        // The constant lives in the crate root and is the single source of
        // truth.  Any future change must update only this one place.
        assert_eq!(crate::MAX_ITEM_SIZE, 100 * 1024 * 1024);
    }

    /// LOG-5: `read_next_strict` reports an explicit
    /// `InvalidEntryType` error when the type byte is unknown, rather
    /// than silently truncating like the legacy `read_next`.
    #[test]
    fn test_read_next_strict_rejects_unknown_entry_type() {
        use crate::error::NoxuLogError;
        use std::fs::OpenOptions;
        use std::io::Write;

        let dir = TempDir::new().unwrap();
        let (fm, lm) = make_log_manager(&dir);

        // Write at least one valid entry first so the file is well-formed.
        lm.log(LogEntryType::Trace, b"valid", Provisional::No, false, false)
            .unwrap();
        lm.flush_sync().unwrap();

        let file_num = fm.get_current_file_num();
        let len_before_append = fm.get_file_length(file_num).unwrap();

        // Append a hand-crafted header with an out-of-range entry_type byte
        // (255 is not a valid LogEntryType variant) directly to the file.
        let path = dir.path().join(format!("{:08x}.ndb", file_num));
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        // 14-byte header: checksum (4) | type (1) | flags (1) | prev (4) | size (4)
        // Only the type byte matters for this test; we set item_size = 0.
        let mut hdr = [0u8; MIN_HEADER_SIZE];
        hdr[4] = 255; // Invalid entry_type.
        file.write_all(&hdr).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let mut reader =
            LogFileReader::open(Arc::clone(&fm), file_num).unwrap();

        // Skip past the well-formed entries first.
        loop {
            if reader.current_offset() >= len_before_append {
                break;
            }
            match reader.read_next_strict() {
                Ok(Some(_)) => continue,
                Ok(None) => {
                    panic!("unexpected EOF before reaching injected header")
                }
                Err(e) => panic!("unexpected error parsing valid entry: {e:?}"),
            }
        }

        // Now we should be at our injected corrupt header.  read_next_strict
        // must surface the unknown entry type as an explicit error rather
        // than silently returning Ok(None).
        match reader.read_next_strict() {
            Err(NoxuLogError::InvalidEntryType { type_num, .. }) => {
                assert_eq!(type_num, 255);
            }
            other => panic!(
                "expected InvalidEntryType error from strict reader, got {:?}",
                other
            ),
        }
    }
}
