//! Utilization file reader for tracking log file space usage.
//!
//!
//! Scans the entire log (or a range of it) and builds a per-file
//! `FileSummary` that separately counts LN and IN entries as total vs.
//! obsolete.  Unlike `CleanerFileReader`, which works on a *single* file,
//! this reader spans *all* files and accumulates statistics keyed by
//! file number.
//!
//! Because noxu-log does not depend on noxu-cleaner, this module provides its
//! own minimal `FileSummary` type that mirrors the fields required by the
//! `UtilizationFileReader`.  Callers that need the full noxu-cleaner
//! `FileSummary` can copy the fields across.

use crate::entry_header::{MAX_HEADER_SIZE, MIN_HEADER_SIZE};
use crate::entry_type::LogEntryType;
use crate::error::Result;
use crate::file_reader::LogFileAccess;
use noxu_util::lsn::{Lsn, NULL_LSN};
use std::collections::HashMap;

// Maximum plausible payload size (64 MiB).
const MAX_SANE_ITEM_SIZE: usize = 64 * 1024 * 1024;

/// Minimal per-file utilization summary produced by `UtilizationFileReader`.
///
#[derive(Debug, Clone, Default)]
pub struct FileSummary {
    /// Total number of log entries counted for this file.
    pub total_count: i32,
    /// Total bytes across all counted entries.
    pub total_size: i32,
    /// Number of LN (leaf-node) log entries.
    pub total_ln_count: i32,
    /// Byte size of LN log entries.
    pub total_ln_size: i32,
    /// Number of IN/BIN log entries.
    pub total_in_count: i32,
    /// Byte size of IN/BIN log entries.
    pub total_in_size: i32,
    /// Number of obsolete IN entries (BIN-deltas are all counted obsolete).
    pub obsolete_in_count: i32,
    /// Number of obsolete LN entries.
    pub obsolete_ln_count: i32,
}

impl FileSummary {
    /// Returns estimated utilization in the range `[0.0, 1.0]`.
    pub fn utilization(&self) -> f64 {
        if self.total_size == 0 {
            return 0.0;
        }
        let obsolete_bytes = self.total_ln_size * self.obsolete_ln_count
            / self.total_ln_count.max(1)
            + self.total_in_size * self.obsolete_in_count
                / self.total_in_count.max(1);
        let active = (self.total_size - obsolete_bytes).max(0);
        active as f64 / self.total_size as f64
    }
}

/// Scans the log and builds a per-file utilization map.
///
/// 
///
/// The simplest way to use this reader:
///
/// ```ignore
/// let mut reader = UtilizationFileReader::new(file_access, buf_size, start_lsn)?;
/// while reader.read_next_entry()? { /* all work done internally */ }
/// let map = reader.get_file_summary_map();
/// ```
pub struct UtilizationFileReader<F: LogFileAccess> {
    /// File access interface.
    file_access: F,
    /// Where to start scanning (`NULL_LSN` = from beginning of log).
    start_lsn: Lsn,
    /// Stop before this LSN (`NULL_LSN` = no limit).
    finish_lsn: Lsn,
    /// Current file number being scanned.
    current_file_num: u32,
    /// Current byte offset within the current file.
    current_offset: u64,
    /// Per-file summaries accumulated during the scan.
    summaries: HashMap<u32, FileSummary>,
    /// Whether we have reached the end of the log.
    eof: bool,
}

impl<F: LogFileAccess> UtilizationFileReader<F> {
    /// Create a new UtilizationFileReader.
    ///
    /// # Arguments
    /// * `file_access`      – file I/O provider
    /// * `_read_buffer_size`– ignored (kept for API compatibility)
    /// * `start_lsn`        – where to begin scanning (`NULL_LSN` = beginning)
    pub fn new(
        file_access: F,
        _read_buffer_size: usize,
        start_lsn: Lsn,
    ) -> Result<Self> {
        let (current_file_num, current_offset, eof) =
            if !start_lsn.is_null() {
                (start_lsn.file_number(), start_lsn.file_offset() as u64, false)
            } else if let Some(first) = file_access.get_first_file_num() {
                (first, 0u64, false)
            } else {
                (0u32, 0u64, true)
            };

        Ok(UtilizationFileReader {
            file_access,
            start_lsn,
            finish_lsn: NULL_LSN,
            current_file_num,
            current_offset,
            summaries: HashMap::new(),
            eof,
        })
    }

    /// Create a reader that stops before `finish_lsn`.
    pub fn new_with_finish(
        file_access: F,
        read_buffer_size: usize,
        start_lsn: Lsn,
        finish_lsn: Lsn,
    ) -> Result<Self> {
        let mut reader = Self::new(file_access, read_buffer_size, start_lsn)?;
        reader.finish_lsn = finish_lsn;
        Ok(reader)
    }

    /// Process the next log entry and update the per-file summary.
    ///
    /// Returns `Ok(true)` while entries remain; `Ok(false)` at end of log.
    ///
    /// every non-header
    /// entry is counted; invisible entries are skipped.
    pub fn read_next_entry(&mut self) -> Result<bool> {
        if self.eof {
            return Ok(false);
        }

        loop {
            // Move to next file if current is exhausted.
            let file_len =
                match self.file_access.get_file_length(self.current_file_num) {
                    Ok(l) => l,
                    Err(_) => {
                        self.eof = true;
                        return Ok(false);
                    }
                };

            if self.current_offset >= file_len {
                match self
                    .file_access
                    .get_following_file_num(self.current_file_num, true)
                {
                    None => {
                        self.eof = true;
                        return Ok(false);
                    }
                    Some(next) => {
                        self.current_file_num = next;
                        self.current_offset = 0;
                        continue;
                    }
                }
            }

            // Read the minimum header.
            let mut hdr = [0u8; MIN_HEADER_SIZE];
            let n = self.file_access.read_from_file(
                self.current_file_num,
                self.current_offset,
                &mut hdr,
            )?;
            if n < MIN_HEADER_SIZE {
                self.eof = true;
                return Ok(false);
            }

            // Zero type byte → past last written entry.
            if hdr[4] == 0 {
                self.eof = true;
                return Ok(false);
            }

            let entry_type_num = hdr[4];
            let flags = hdr[5];
            let item_size = u32::from_le_bytes([
                hdr[10], hdr[11], hdr[12], hdr[13],
            ]) as usize;

            if item_size > MAX_SANE_ITEM_SIZE {
                self.eof = true;
                return Ok(false);
            }

            let vlsn_present =
                (flags & 0x08) != 0 || (flags & 0x20) != 0;
            let header_size =
                if vlsn_present { MAX_HEADER_SIZE } else { MIN_HEADER_SIZE };
            let entry_size = header_size + item_size;

            if self.current_offset + entry_size as u64 > file_len {
                self.eof = true;
                return Ok(false);
            }

            let lsn =
                Lsn::new(self.current_file_num, self.current_offset as u32);

            // Enforce finish_lsn bound.
            if !self.finish_lsn.is_null() && lsn >= self.finish_lsn {
                self.eof = true;
                return Ok(false);
            }

            self.current_offset += entry_size as u64;

            // Skip file header (not tracked by UtilizationProfile).
            let entry_type = match LogEntryType::from_type_num(entry_type_num) {
                Some(t) => t,
                None => continue,
            };
            if entry_type == LogEntryType::FileHeader {
                continue;
            }

            // Accumulate into the per-file summary.
            let summary = self
                .summaries
                .entry(self.current_file_num)
                .or_default();

            let size = entry_size as i32;
            summary.total_count += 1;
            summary.total_size += size;

            match entry_type {
                t if is_ln_type(t) => {
                    summary.total_ln_count += 1;
                    summary.total_ln_size += size;
                }
                LogEntryType::BINDelta => {
                    // BIN-deltas are counted as IN and marked all-obsolete
                    // (same conservative assumption as UtilizationFileReader.java).
                    summary.total_in_count += 1;
                    summary.total_in_size += size;
                    summary.obsolete_in_count += 1;
                }
                t if t.is_in_type() => {
                    summary.total_in_count += 1;
                    summary.total_in_size += size;
                }
                _ => {}
            }

            return Ok(true);
        }
    }

    /// Returns the accumulated per-file summary map.
    ///
    /// Keys are log file numbers; values are the utilization counters.
    pub fn get_file_summary_map(&self) -> &HashMap<u32, FileSummary> {
        &self.summaries
    }

    /// Consumes the reader and returns the accumulated summary map.
    pub fn into_file_summary_map(self) -> HashMap<u32, FileSummary> {
        self.summaries
    }
}

/// Returns `true` if the given `LogEntryType` is an LN-family type.
fn is_ln_type(t: LogEntryType) -> bool {
    matches!(
        t,
        LogEntryType::InsertLN
            | LogEntryType::UpdateLN
            | LogEntryType::DeleteLN
            | LogEntryType::InsertLNTxn
            | LogEntryType::UpdateLNTxn
            | LogEntryType::DeleteLNTxn
            | LogEntryType::MapLN
            | LogEntryType::NameLN
            | LogEntryType::NameLNTxn
            | LogEntryType::FileSummaryLN
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::ln_log_entry::LnLogEntry;
    use crate::entry::in_log_entry::InLogEntry;
    use crate::entry_header::MIN_HEADER_SIZE;
    use crate::entry_type::LogEntryType;
    use bytes::BytesMut;
    use noxu_util::lsn::NULL_LSN;
    use noxu_util::vlsn::NULL_VLSN;
    use std::collections::HashMap;
    use std::io;

    // ------------------------------------------------------------------
    // Mock file access
    // ------------------------------------------------------------------

    struct MockFileAccess {
        files: HashMap<u32, Vec<u8>>,
    }

    impl MockFileAccess {
        fn new() -> Self {
            MockFileAccess { files: HashMap::new() }
        }

        fn add_file(&mut self, file_num: u32, data: Vec<u8>) {
            self.files.insert(file_num, data);
        }
    }

    impl LogFileAccess for MockFileAccess {
        fn read_from_file(
            &self,
            file_num: u32,
            offset: u64,
            buf: &mut [u8],
        ) -> Result<usize> {
            if let Some(data) = self.files.get(&file_num) {
                let start = offset as usize;
                if start >= data.len() {
                    return Ok(0);
                }
                let end = (start + buf.len()).min(data.len());
                let n = end - start;
                buf[..n].copy_from_slice(&data[start..end]);
                Ok(n)
            } else {
                Err(io::Error::new(io::ErrorKind::NotFound, "File not found")
                    .into())
            }
        }

        fn get_file_length(&self, file_num: u32) -> Result<u64> {
            self.files
                .get(&file_num)
                .map(|d| d.len() as u64)
                .ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, "File not found")
                        .into()
                })
        }

        fn get_first_file_num(&self) -> Option<u32> {
            self.files.keys().min().copied()
        }

        fn get_following_file_num(
            &self,
            file_num: u32,
            forward: bool,
        ) -> Option<u32> {
            let mut nums: Vec<u32> = self.files.keys().copied().collect();
            nums.sort();
            if forward {
                nums.iter().find(|&&n| n > file_num).copied()
            } else {
                nums.iter().rev().find(|&&n| n < file_num).copied()
            }
        }

        fn get_file_header_prev_offset(&self, _file_num: u32) -> Result<u64> {
            Ok(0)
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    fn make_raw_entry(entry_type: LogEntryType, payload: &[u8]) -> Vec<u8> {
        let item_size = payload.len() as u32;
        let mut buf = vec![0u8; MIN_HEADER_SIZE + payload.len()];
        buf[4] = entry_type.type_num();
        buf[10..14].copy_from_slice(&item_size.to_le_bytes());
        buf[MIN_HEADER_SIZE..].copy_from_slice(payload);
        buf
    }

    fn make_ln_payload() -> Vec<u8> {
        let e = LnLogEntry::new(
            1, None, NULL_LSN, false, None, None, NULL_VLSN, 0, false,
            b"k".to_vec(), Some(b"v".to_vec()), 0, NULL_VLSN,
        );
        let mut buf = BytesMut::new();
        e.write_to_log(&mut buf);
        buf.to_vec()
    }

    fn make_in_payload() -> Vec<u8> {
        let e = InLogEntry::new(1, NULL_LSN, NULL_LSN, b"n".to_vec());
        let mut buf = BytesMut::new();
        e.write_to_log(&mut buf);
        buf.to_vec()
    }

    // ------------------------------------------------------------------
    // Construction tests
    // ------------------------------------------------------------------

    #[test]
    fn test_utilization_file_reader_new_with_file() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 64]);
        let result = UtilizationFileReader::new(mock, 512, Lsn::new(0, 0));
        assert!(result.is_ok());
    }

    #[test]
    fn test_utilization_file_reader_new_empty_store() {
        let mock = MockFileAccess::new();
        let result = UtilizationFileReader::new(mock, 512, NULL_LSN);
        assert!(result.is_ok());
    }

    #[test]
    fn test_utilization_file_reader_multiple_files() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![1u8; 128]);
        mock.add_file(1, vec![2u8; 256]);
        let result = UtilizationFileReader::new(mock, 1024, Lsn::new(0, 0));
        assert!(result.is_ok());
    }

    #[test]
    fn test_utilization_file_reader_large_buffer() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 32]);
        let result = UtilizationFileReader::new(mock, 65536, Lsn::new(0, 0));
        assert!(result.is_ok());
    }

    #[test]
    fn test_utilization_file_reader_nonzero_start_offset() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 200]);
        let result = UtilizationFileReader::new(mock, 512, Lsn::new(0, 50));
        assert!(result.is_ok());
    }

    // ------------------------------------------------------------------
    // Functional tests
    // ------------------------------------------------------------------

    #[test]
    fn test_empty_file_produces_no_summaries() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![]);

        let mut reader =
            UtilizationFileReader::new(mock, 512, NULL_LSN).unwrap();
        while reader.read_next_entry().unwrap() {}

        assert!(reader.get_file_summary_map().is_empty());
    }

    #[test]
    fn test_no_files_produces_no_summaries() {
        let mock = MockFileAccess::new();
        let mut reader =
            UtilizationFileReader::new(mock, 512, NULL_LSN).unwrap();
        while reader.read_next_entry().unwrap() {}
        assert!(reader.get_file_summary_map().is_empty());
    }

    #[test]
    fn test_single_ln_entry_counted() {
        let payload = make_ln_payload();
        let raw = make_raw_entry(LogEntryType::InsertLN, &payload);
        let entry_size = raw.len() as i32;

        let mut mock = MockFileAccess::new();
        mock.add_file(0, raw);

        let mut reader =
            UtilizationFileReader::new(mock, 512, Lsn::new(0, 0)).unwrap();
        while reader.read_next_entry().unwrap() {}

        let map = reader.get_file_summary_map();
        let summary = map.get(&0).expect("should have entry for file 0");
        assert_eq!(summary.total_count, 1);
        assert_eq!(summary.total_ln_count, 1);
        assert_eq!(summary.total_ln_size, entry_size);
        assert_eq!(summary.total_in_count, 0);
    }

    #[test]
    fn test_single_in_entry_counted() {
        let payload = make_in_payload();
        let raw = make_raw_entry(LogEntryType::IN, &payload);
        let entry_size = raw.len() as i32;

        let mut mock = MockFileAccess::new();
        mock.add_file(0, raw);

        let mut reader =
            UtilizationFileReader::new(mock, 512, Lsn::new(0, 0)).unwrap();
        while reader.read_next_entry().unwrap() {}

        let map = reader.get_file_summary_map();
        let summary = map.get(&0).expect("summary for file 0");
        assert_eq!(summary.total_in_count, 1);
        assert_eq!(summary.total_in_size, entry_size);
        assert_eq!(summary.total_ln_count, 0);
    }

    #[test]
    fn test_bin_delta_counted_as_obsolete_in() {
        use crate::entry::bin_delta_log_entry::BinDeltaLogEntry;

        let e = BinDeltaLogEntry::new(1, NULL_LSN, NULL_LSN, b"dd".to_vec());
        let mut pbuf = BytesMut::new();
        e.write_to_log(&mut pbuf);
        let raw = make_raw_entry(LogEntryType::BINDelta, &pbuf);

        let mut mock = MockFileAccess::new();
        mock.add_file(0, raw);

        let mut reader =
            UtilizationFileReader::new(mock, 512, Lsn::new(0, 0)).unwrap();
        while reader.read_next_entry().unwrap() {}

        let map = reader.get_file_summary_map();
        let summary = map.get(&0).unwrap();
        assert_eq!(summary.total_in_count, 1);
        assert_eq!(summary.obsolete_in_count, 1);
    }

    #[test]
    fn test_multiple_entries_counted() {
        let p_ln = make_ln_payload();
        let r_ln = make_raw_entry(LogEntryType::InsertLN, &p_ln);
        let p_in = make_in_payload();
        let r_in = make_raw_entry(LogEntryType::BIN, &p_in);
        let mut data = r_ln;
        data.extend_from_slice(&r_in);

        let mut mock = MockFileAccess::new();
        mock.add_file(0, data);

        let mut reader =
            UtilizationFileReader::new(mock, 512, Lsn::new(0, 0)).unwrap();
        while reader.read_next_entry().unwrap() {}

        let map = reader.get_file_summary_map();
        let summary = map.get(&0).unwrap();
        assert_eq!(summary.total_count, 2);
        assert_eq!(summary.total_ln_count, 1);
        assert_eq!(summary.total_in_count, 1);
    }

    #[test]
    fn test_entries_across_two_files() {
        let p_ln = make_ln_payload();
        let r_ln = make_raw_entry(LogEntryType::InsertLN, &p_ln);
        let p_in = make_in_payload();
        let r_in = make_raw_entry(LogEntryType::BIN, &p_in);

        let mut mock = MockFileAccess::new();
        mock.add_file(0, r_ln);
        mock.add_file(1, r_in);

        let mut reader =
            UtilizationFileReader::new(mock, 512, Lsn::new(0, 0)).unwrap();
        while reader.read_next_entry().unwrap() {}

        let map = reader.get_file_summary_map();
        assert!(map.contains_key(&0), "should have summary for file 0");
        assert!(map.contains_key(&1), "should have summary for file 1");
        assert_eq!(map[&0].total_ln_count, 1);
        assert_eq!(map[&1].total_in_count, 1);
    }

    #[test]
    fn test_finish_lsn_limits_scan() {
        let p = make_ln_payload();
        let r = make_raw_entry(LogEntryType::InsertLN, &p);
        let entry_len = r.len();

        let mut data = r.clone();
        data.extend_from_slice(&r);

        let mut mock = MockFileAccess::new();
        mock.add_file(0, data);

        // finish_lsn = after the first entry only.
        let finish = Lsn::new(0, entry_len as u32);
        let mut reader = UtilizationFileReader::new_with_finish(
            mock,
            512,
            Lsn::new(0, 0),
            finish,
        )
        .unwrap();
        while reader.read_next_entry().unwrap() {}

        let map = reader.get_file_summary_map();
        let summary = map.get(&0).unwrap();
        assert_eq!(summary.total_count, 1, "only first entry should be counted");
    }

    #[test]
    fn test_into_file_summary_map() {
        let p = make_ln_payload();
        let r = make_raw_entry(LogEntryType::InsertLN, &p);

        let mut mock = MockFileAccess::new();
        mock.add_file(0, r);

        let mut reader =
            UtilizationFileReader::new(mock, 512, Lsn::new(0, 0)).unwrap();
        while reader.read_next_entry().unwrap() {}

        let map = reader.into_file_summary_map();
        assert!(map.contains_key(&0));
    }

    #[test]
    fn test_file_summary_utilization() {
        let mut summary = FileSummary::default();
        assert_eq!(summary.utilization(), 0.0);

        summary.total_count = 1;
        summary.total_size = 100;
        summary.total_ln_count = 1;
        summary.total_ln_size = 100;
        // 0 obsolete → full utilization
        assert!((summary.utilization() - 1.0).abs() < 1e-6);
    }

    // ------------------------------------------------------------------
    // Branch-coverage for MockFileAccess
    // ------------------------------------------------------------------

    #[test]
    fn test_mock_read_from_file_missing() {
        let mock = MockFileAccess::new();
        let mut buf = [0u8; 4];
        assert!(mock.read_from_file(99, 0, &mut buf).is_err());
    }

    #[test]
    fn test_mock_read_from_file_offset_at_end() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![1u8, 2, 3]);
        let mut buf = [0u8; 4];
        let n = mock.read_from_file(0, 3, &mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn test_mock_read_from_file_offset_past_end() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![1u8, 2]);
        let mut buf = [0u8; 2];
        let n = mock.read_from_file(0, 500, &mut buf).unwrap();
        assert_eq!(n, 0);
    }

    #[test]
    fn test_mock_get_file_length_missing() {
        let mock = MockFileAccess::new();
        assert!(mock.get_file_length(0).is_err());
    }

    #[test]
    fn test_mock_get_file_length_present() {
        let mut mock = MockFileAccess::new();
        mock.add_file(1, vec![0u8; 88]);
        assert_eq!(mock.get_file_length(1).unwrap(), 88);
    }

    #[test]
    fn test_mock_get_first_file_num_empty() {
        let mock = MockFileAccess::new();
        assert_eq!(mock.get_first_file_num(), None);
    }

    #[test]
    fn test_mock_get_first_file_num_returns_min() {
        let mut mock = MockFileAccess::new();
        mock.add_file(5, vec![]);
        mock.add_file(1, vec![]);
        mock.add_file(3, vec![]);
        assert_eq!(mock.get_first_file_num(), Some(1));
    }

    #[test]
    fn test_mock_get_following_file_backward() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![]);
        mock.add_file(1, vec![]);
        mock.add_file(2, vec![]);
        assert_eq!(mock.get_following_file_num(2, false), Some(1));
        assert_eq!(mock.get_following_file_num(0, false), None);
    }

    #[test]
    fn test_mock_get_following_file_forward_none() {
        let mut mock = MockFileAccess::new();
        mock.add_file(10, vec![]);
        assert_eq!(mock.get_following_file_num(10, true), None);
    }

    #[test]
    fn test_utilization_file_reader_null_lsn_with_files() {
        let mut mock = MockFileAccess::new();
        mock.add_file(0, vec![0u8; 64]);
        let result = UtilizationFileReader::new(mock, 512, NULL_LSN);
        assert!(result.is_ok());
    }
}
