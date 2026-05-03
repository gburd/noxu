//! FileSummaryLN  -  Leaf Node for per-file utilization tracking.
//!
//! Port of `com.sleepycat.je.tree.FileSummaryLN`.
//!
//! A FileSummaryLN stores utilization information for a single log file.
//! This data is used by the cleaner to determine which log files have
//! the most reclaimable space and should be cleaned first.

use crate::ln::Ln;

/// Utilization summary for a single log file.
///
/// Tracks total and obsolete counts/sizes for all entries, LN entries,
/// and IN entries separately. This detailed breakdown allows the cleaner
/// to make informed decisions about which files to clean.
#[derive(Debug, Clone, Default)]
pub struct FileSummary {
    /// Total number of log entries in the file.
    pub total_count: i32,
    /// Total size of all log entries in bytes.
    pub total_size: i64,
    /// Number of obsolete (reclaimable) entries.
    pub obsolete_count: i32,
    /// Total size of obsolete entries.
    pub obsolete_size: i64,
    /// Number of LN entries.
    pub total_ln_count: i32,
    /// Size of LN entries.
    pub total_ln_size: i64,
    /// Number of obsolete LN entries.
    pub obsolete_ln_count: i32,
    /// Size of obsolete LN entries.
    pub obsolete_ln_size: i64,
    /// Number of IN entries.
    pub total_in_count: i32,
    /// Size of IN entries.
    pub total_in_size: i64,
    /// Number of obsolete IN entries.
    pub obsolete_in_count: i32,
    /// Size of obsolete IN entries.
    pub obsolete_in_size: i64,
}

impl FileSummary {
    /// Creates a new empty FileSummary.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the utilization ratio (0.0 - 1.0).
    ///
    /// The utilization is the fraction of the file that contains active
    /// (non-obsolete) data. A file with 0.2 utilization means 80% of the
    /// file is reclaimable.
    pub fn utilization(&self) -> f64 {
        if self.total_size == 0 {
            return 1.0;
        }
        let active = self.total_size - self.obsolete_size;
        active as f64 / self.total_size as f64
    }

    /// Adds another summary to this one.
    ///
    /// This is used to aggregate multiple partial summaries into a
    /// complete file summary.
    pub fn add(&mut self, other: &FileSummary) {
        self.total_count += other.total_count;
        self.total_size += other.total_size;
        self.obsolete_count += other.obsolete_count;
        self.obsolete_size += other.obsolete_size;
        self.total_ln_count += other.total_ln_count;
        self.total_ln_size += other.total_ln_size;
        self.obsolete_ln_count += other.obsolete_ln_count;
        self.obsolete_ln_size += other.obsolete_ln_size;
        self.total_in_count += other.total_in_count;
        self.total_in_size += other.total_in_size;
        self.obsolete_in_count += other.obsolete_in_count;
        self.obsolete_in_size += other.obsolete_in_size;
    }

    /// Resets all counters to zero.
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// Returns the serialized size of this FileSummary.
    pub fn log_size(&self) -> usize {
        // 6 i32 fields + 6 i64 fields = 24 + 48 = 72 bytes
        6 * 4 + 6 * 8
    }

    /// Writes this FileSummary to a byte buffer.
    pub fn write_to_log(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.total_count.to_be_bytes());
        buf.extend_from_slice(&self.total_size.to_be_bytes());
        buf.extend_from_slice(&self.obsolete_count.to_be_bytes());
        buf.extend_from_slice(&self.obsolete_size.to_be_bytes());
        buf.extend_from_slice(&self.total_ln_count.to_be_bytes());
        buf.extend_from_slice(&self.total_ln_size.to_be_bytes());
        buf.extend_from_slice(&self.obsolete_ln_count.to_be_bytes());
        buf.extend_from_slice(&self.obsolete_ln_size.to_be_bytes());
        buf.extend_from_slice(&self.total_in_count.to_be_bytes());
        buf.extend_from_slice(&self.total_in_size.to_be_bytes());
        buf.extend_from_slice(&self.obsolete_in_count.to_be_bytes());
        buf.extend_from_slice(&self.obsolete_in_size.to_be_bytes());
    }

    /// Reads a FileSummary from a byte buffer.
    pub fn read_from_log(buf: &[u8]) -> std::io::Result<Self> {
        use byteorder::{BigEndian, ReadBytesExt};
        use std::io::Cursor;

        let mut cursor = Cursor::new(buf);

        Ok(FileSummary {
            total_count: cursor.read_i32::<BigEndian>()?,
            total_size: cursor.read_i64::<BigEndian>()?,
            obsolete_count: cursor.read_i32::<BigEndian>()?,
            obsolete_size: cursor.read_i64::<BigEndian>()?,
            total_ln_count: cursor.read_i32::<BigEndian>()?,
            total_ln_size: cursor.read_i64::<BigEndian>()?,
            obsolete_ln_count: cursor.read_i32::<BigEndian>()?,
            obsolete_ln_size: cursor.read_i64::<BigEndian>()?,
            total_in_count: cursor.read_i32::<BigEndian>()?,
            total_in_size: cursor.read_i64::<BigEndian>()?,
            obsolete_in_count: cursor.read_i32::<BigEndian>()?,
            obsolete_in_size: cursor.read_i64::<BigEndian>()?,
        })
    }
}

/// A FileSummaryLN stores utilization data for a single log file.
///
/// The FileSummaryLN is used by the cleaner to track which log files
/// have the most reclaimable space. Each log file has one FileSummaryLN
/// that is updated as entries in that file become obsolete.
#[derive(Debug, Clone)]
pub struct FileSummaryLn {
    /// The underlying LN.
    ln: Ln,
    /// The summary data.
    summary: FileSummary,
    /// Whether this summary has been modified.
    modified: bool,
}

impl FileSummaryLn {
    /// Creates a new FileSummaryLN with the given summary.
    pub fn new(summary: FileSummary) -> Self {
        FileSummaryLn {
            ln: Ln::new(None), // Data stored separately in summary
            summary,
            modified: false,
        }
    }

    /// Returns a reference to the summary.
    pub fn get_summary(&self) -> &FileSummary {
        &self.summary
    }

    /// Returns a mutable reference to the summary.
    ///
    /// Automatically marks the summary as modified.
    pub fn get_summary_mut(&mut self) -> &mut FileSummary {
        self.modified = true;
        self.ln.set_dirty();
        &mut self.summary
    }

    /// Returns true if the summary has been modified.
    pub fn is_modified(&self) -> bool {
        self.modified
    }

    /// Marks the summary as modified.
    pub fn set_modified(&mut self) {
        self.modified = true;
        self.ln.set_dirty();
    }

    /// Returns a reference to the underlying LN.
    pub fn get_ln(&self) -> &Ln {
        &self.ln
    }

    /// Returns a mutable reference to the underlying LN.
    pub fn get_ln_mut(&mut self) -> &mut Ln {
        &mut self.ln
    }

    /// Returns the serialized size of this FileSummaryLN.
    pub fn log_size(&self) -> usize {
        self.ln.log_size() + self.summary.log_size() + 1 // + modified flag
    }

    /// Writes this FileSummaryLN to a byte buffer.
    pub fn write_to_log(&self, buf: &mut Vec<u8>) {
        self.ln.write_to_log(buf);
        self.summary.write_to_log(buf);
        buf.push(if self.modified { 1 } else { 0 });
    }

    /// Reads a FileSummaryLN from a byte buffer.
    pub fn read_from_log(buf: &[u8]) -> std::io::Result<Self> {
        let ln = Ln::read_from_log(buf)?;
        let ln_size = ln.log_size();
        let remaining = &buf[ln_size..];

        let summary = FileSummary::read_from_log(remaining)?;
        let summary_size = summary.log_size();
        let remaining = &remaining[summary_size..];

        use byteorder::ReadBytesExt;
        use std::io::Cursor;
        let mut cursor = Cursor::new(remaining);
        let modified = cursor.read_u8()? != 0;

        Ok(FileSummaryLn { ln, summary, modified })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_summary_new() {
        let summary = FileSummary::new();

        assert_eq!(summary.total_count, 0);
        assert_eq!(summary.total_size, 0);
        assert_eq!(summary.obsolete_count, 0);
        assert_eq!(summary.obsolete_size, 0);
    }

    #[test]
    fn test_file_summary_utilization() {
        let mut summary = FileSummary::new();
        summary.total_size = 1000;
        summary.obsolete_size = 300;

        let util = summary.utilization();
        assert!((util - 0.7).abs() < 0.001);

        // Empty file has 100% utilization
        let empty = FileSummary::new();
        assert_eq!(empty.utilization(), 1.0);

        // Fully obsolete file
        let mut full_obsolete = FileSummary::new();
        full_obsolete.total_size = 1000;
        full_obsolete.obsolete_size = 1000;
        assert!((full_obsolete.utilization() - 0.0).abs() < 0.001);
    }

    #[test]
    fn test_file_summary_add() {
        let mut summary1 = FileSummary::new();
        summary1.total_count = 10;
        summary1.total_size = 1000;
        summary1.obsolete_count = 3;
        summary1.obsolete_size = 300;
        summary1.total_ln_count = 5;
        summary1.total_ln_size = 500;

        let mut summary2 = FileSummary::new();
        summary2.total_count = 20;
        summary2.total_size = 2000;
        summary2.obsolete_count = 5;
        summary2.obsolete_size = 500;
        summary2.total_ln_count = 10;
        summary2.total_ln_size = 1000;

        summary1.add(&summary2);

        assert_eq!(summary1.total_count, 30);
        assert_eq!(summary1.total_size, 3000);
        assert_eq!(summary1.obsolete_count, 8);
        assert_eq!(summary1.obsolete_size, 800);
        assert_eq!(summary1.total_ln_count, 15);
        assert_eq!(summary1.total_ln_size, 1500);
    }

    #[test]
    fn test_file_summary_reset() {
        let mut summary = FileSummary::new();
        summary.total_count = 100;
        summary.total_size = 10000;
        summary.obsolete_count = 50;

        summary.reset();

        assert_eq!(summary.total_count, 0);
        assert_eq!(summary.total_size, 0);
        assert_eq!(summary.obsolete_count, 0);
    }

    #[test]
    fn test_file_summary_serialization() {
        let mut summary = FileSummary::new();
        summary.total_count = 100;
        summary.total_size = 5000;
        summary.obsolete_count = 25;
        summary.obsolete_size = 1250;
        summary.total_ln_count = 60;
        summary.total_ln_size = 3000;
        summary.obsolete_ln_count = 15;
        summary.obsolete_ln_size = 750;
        summary.total_in_count = 40;
        summary.total_in_size = 2000;
        summary.obsolete_in_count = 10;
        summary.obsolete_in_size = 500;

        let mut buf = Vec::new();
        summary.write_to_log(&mut buf);

        let summary2 = FileSummary::read_from_log(&buf).unwrap();

        assert_eq!(summary2.total_count, 100);
        assert_eq!(summary2.total_size, 5000);
        assert_eq!(summary2.obsolete_count, 25);
        assert_eq!(summary2.obsolete_size, 1250);
        assert_eq!(summary2.total_ln_count, 60);
        assert_eq!(summary2.total_ln_size, 3000);
        assert_eq!(summary2.obsolete_ln_count, 15);
        assert_eq!(summary2.obsolete_ln_size, 750);
        assert_eq!(summary2.total_in_count, 40);
        assert_eq!(summary2.total_in_size, 2000);
        assert_eq!(summary2.obsolete_in_count, 10);
        assert_eq!(summary2.obsolete_in_size, 500);
    }

    #[test]
    fn test_file_summary_ln_new() {
        let summary = FileSummary::new();
        let fs_ln = FileSummaryLn::new(summary);

        assert!(!fs_ln.is_modified());
        assert_eq!(fs_ln.get_summary().total_count, 0);
    }

    #[test]
    fn test_file_summary_ln_modification() {
        let summary = FileSummary::new();
        let mut fs_ln = FileSummaryLn::new(summary);

        assert!(!fs_ln.is_modified());

        fs_ln.get_summary_mut().total_count = 10;

        assert!(fs_ln.is_modified());
        assert!(fs_ln.get_ln().is_dirty());
        assert_eq!(fs_ln.get_summary().total_count, 10);
    }

    #[test]
    fn test_file_summary_ln_roundtrip() {
        let mut summary = FileSummary::new();
        summary.total_count = 50;
        summary.total_size = 2500;
        summary.obsolete_count = 10;
        summary.obsolete_size = 500;

        let mut fs_ln = FileSummaryLn::new(summary);
        fs_ln.set_modified();

        let mut buf = Vec::new();
        fs_ln.write_to_log(&mut buf);

        let fs_ln2 = FileSummaryLn::read_from_log(&buf).unwrap();

        assert!(fs_ln2.is_modified());
        assert_eq!(fs_ln2.get_summary().total_count, 50);
        assert_eq!(fs_ln2.get_summary().total_size, 2500);
        assert_eq!(fs_ln2.get_summary().obsolete_count, 10);
        assert_eq!(fs_ln2.get_summary().obsolete_size, 500);
    }

    #[test]
    fn test_file_summary_ln_log_size() {
        let summary = FileSummary::new();
        let fs_ln = FileSummaryLn::new(summary);

        let size = fs_ln.log_size();

        let mut buf = Vec::new();
        fs_ln.write_to_log(&mut buf);

        assert_eq!(size, buf.len());
    }
}
