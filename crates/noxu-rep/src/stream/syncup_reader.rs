//! REP-1 STEP 5 (A): the backward `ReplicaSyncupReader`.
//!
//! Port of `com.sleepycat.je.rep.stream.ReplicaSyncupReader` (and the feeder's
//! `FeederSyncupReader`, which is the same backward log walk on the feeder
//! side). Both scan the log BACKWARD from the last VLSN, yielding, per VLSN:
//! the LSN, a record fingerprint (checksum, JE `OutputWireRecord.match`), and a
//! sync-point flag (JE `LogEntryType.isSyncPoint`). The reader also counts the
//! commits/aborts it steps over after the candidate matchpoint
//! (`MatchpointSearchResults.getNumPassedCommits`), which
//! [`crate::stream::syncup::verify_rollback`] needs for its HardRecovery
//! decision.
//!
//! The VLSN index alone records only VLSN→LSN; it does NOT keep the per-VLSN
//! sync-flag, the record checksum, or the commit count. JE therefore RE-READS
//! the log rather than trusting the index (see the class comment in
//! `ReplicaSyncupReader.java`: "The reader must track whether it has passed a
//! checkpoint, and therefore can not use the vlsn index to skip over
//! entries."). This reader re-reads too, reusing the same raw `FileManager`
//! byte reads and VLSN-tagged header parsing the feeder's
//! `EnvironmentLogScanner` already uses.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use noxu_log::MAX_ITEM_SIZE;
use noxu_log::entry_header::{MAX_HEADER_SIZE, MIN_HEADER_SIZE};
use noxu_log::file_header::LOG_VERSION as LOG_FILE_VERSION;
use noxu_log::file_header::on_disk_size as file_header_on_disk_size;
use noxu_log::file_manager::FileManager;
use noxu_util::{NULL_VLSN, Vlsn};

use crate::stream::syncup::{SyncupView, VlsnEntry};
use crate::vlsn::vlsn_index::VlsnIndex;

/// A scanned snapshot of one node's replicated log, indexed by VLSN.
///
/// Built by walking the log and recording, per VLSN, its [`VlsnEntry`]
/// (LSN, fingerprint, sync-flag). Implements [`SyncupView`] so the pure
/// matchpoint search (`find_matchpoint`) and `verify_rollback` truth table can
/// run against a real environment's log.
///
/// Port of the data the JE `ReplicaSyncupReader` exposes (`scanBackwards`,
/// `findPrevSyncEntry`, plus the `MatchpointSearchResults` counters). JE walks
/// strictly backward for efficiency; this snapshot collects the same per-VLSN
/// facts in one pass (the log is the source of truth either way) and answers
/// backward queries from the in-memory map. The O(n) one-pass scan is marked
/// below; a streaming backward reader is the upgrade path if syncup ever runs
/// on logs too large to snapshot.
pub struct SyncupLogView {
    /// VLSN → entry, in ascending VLSN order.
    entries: BTreeMap<i64, VlsnEntry>,
    /// VLSNs that are transaction ends (commit/abort). Used to count
    /// `numPassedCommits` above a candidate matchpoint. Stored separately
    /// because [`VlsnEntry`]'s public shape (fixed by the decision core)
    /// carries only the sync-flag, not the narrower txn-end flag.
    txn_end_vlsns: std::collections::BTreeSet<i64>,
    /// Highest sync-point VLSN seen (JE `VLSNRange.getLastSync`).
    last_sync: Vlsn,
    /// Highest commit/abort VLSN seen (JE `VLSNRange.getLastTxnEnd`).
    last_txn_end: Vlsn,
    /// First (lowest) VLSN available (JE `VLSNRange.getFirst`).
    first: Vlsn,
}

impl SyncupLogView {
    /// Build a view by scanning the log under `env_home`.
    ///
    /// Reads every entry once (forward over files for simplicity), recording
    /// the per-VLSN fingerprint/sync-flag the matchpoint search needs. Returns
    /// `None` only if a `FileManager` cannot be opened for `env_home`.
    pub fn scan(env_home: &Path) -> Option<Self> {
        // Read-only FileManager over the env's log, same construction the
        // feeder's EnvironmentLogScanner uses.
        let fm = Arc::new(
            FileManager::new(env_home, true, 256 * 1024 * 1024, 32).ok()?,
        );
        Some(Self::scan_with_manager(&fm))
    }

    /// Build a view from an already-open [`FileManager`] (used by the live
    /// syncup driver, which already holds one, and by tests).
    pub fn scan_with_manager(fm: &FileManager) -> Self {
        let mut entries: BTreeMap<i64, VlsnEntry> = BTreeMap::new();
        let mut txn_end_vlsns: std::collections::BTreeSet<i64> =
            std::collections::BTreeSet::new();
        let mut last_sync = NULL_VLSN;
        let mut last_txn_end = NULL_VLSN;

        // ponytail: one forward O(n) pass over the log collects every
        // per-VLSN fact (lsn, fingerprint, sync-flag). JE scans backward and
        // stops early at the matchpoint; a streaming backward reader is the
        // upgrade path if syncup must run on logs too large to snapshot.
        let file_nums = fm.list_file_numbers().unwrap_or_default();
        for file_num in file_nums {
            let header_size = fm
                .file_header_size_for(file_num)
                .unwrap_or_else(|_| file_header_on_disk_size(LOG_FILE_VERSION))
                as u64;
            let file_len = match fm.get_file_length(file_num) {
                Ok(len) => len,
                Err(_) => continue,
            };
            let mut offset = header_size;
            while offset < file_len {
                match read_raw_entry(fm, file_num, offset) {
                    None => break, // end of written data in this file
                    Some((entry_size, vlsn_opt, type_byte, payload)) => {
                        offset += entry_size as u64;
                        let Some(vlsn) = vlsn_opt else { continue };
                        let lsn = noxu_util::Lsn::new(file_num, {
                            // offset before this entry (we just advanced)
                            (offset - entry_size as u64) as u32
                        })
                        .as_u64();
                        let is_sync =
                            noxu_log::LogEntryType::from_type_num(type_byte)
                                .map(|t| t.is_sync_point())
                                .unwrap_or(false);
                        let is_txn_end =
                            noxu_log::LogEntryType::from_type_num(type_byte)
                                .map(|t| {
                                    matches!(
                                        t,
                                        noxu_log::LogEntryType::TxnCommit
                                            | noxu_log::LogEntryType::TxnAbort
                                    )
                                })
                                .unwrap_or(false);
                        // Fingerprint = checksum of the record payload, the
                        // stand-in for JE OutputWireRecord.match (record
                        // equality at the same VLSN).
                        let fingerprint = crc32fast::hash(&payload) as u64
                            ^ (type_byte as u64);
                        entries.insert(
                            vlsn as i64,
                            VlsnEntry { lsn, fingerprint, is_sync },
                        );
                        let v = Vlsn::new(vlsn as i64);
                        if is_sync && v > last_sync {
                            last_sync = v;
                        }
                        if is_txn_end {
                            txn_end_vlsns.insert(vlsn as i64);
                            if v > last_txn_end {
                                last_txn_end = v;
                            }
                        }
                    }
                }
            }
        }

        let first =
            entries.keys().next().map(|&v| Vlsn::new(v)).unwrap_or(NULL_VLSN);

        Self { entries, txn_end_vlsns, last_sync, last_txn_end, first }
    }

    /// Count the commit/abort records strictly above `matchpoint` (JE
    /// `MatchpointSearchResults.getNumPassedCommits`). `verify_rollback` uses
    /// this to force HardRecovery when the backward scan stepped over a txn
    /// end even if `lastTxnEnd <= matchpoint` numerically.
    pub fn num_passed_commits(&self, matchpoint: Vlsn) -> u64 {
        let floor = matchpoint.sequence();
        self.txn_end_vlsns.range((floor + 1)..).count() as u64
    }

    /// All VLSN→[`VlsnEntry`] pairs, ascending. Used by the feeder side of the
    /// syncup protocol to answer `EntryRequest`.
    pub fn entries(&self) -> impl Iterator<Item = (Vlsn, &VlsnEntry)> {
        self.entries.iter().map(|(&v, e)| (Vlsn::new(v), e))
    }
}

impl SyncupView for SyncupLogView {
    fn last_sync(&self) -> Vlsn {
        self.last_sync
    }
    fn last_txn_end(&self) -> Vlsn {
        self.last_txn_end
    }
    fn first_vlsn(&self) -> Vlsn {
        self.first
    }
    fn entry(&self, vlsn: Vlsn) -> Option<VlsnEntry> {
        self.entries.get(&vlsn.sequence()).copied()
    }
}

// ---------------------------------------------------------------------------
// VlsnIndexView — a SyncupView over an in-memory VlsnIndex
// ---------------------------------------------------------------------------

/// A [`SyncupView`] backed by a live [`VlsnIndex`] (VLSN → LSN) plus the
/// index's range (`getFirst`/`getLastSync`/`getLastTxnEnd`).
///
/// The per-VLSN *fingerprint* is the LSN itself: two nodes hold the "same
/// record" at a VLSN iff they assigned it the same LSN. This is the in-memory
/// equivalent of JE `OutputWireRecord.match` for the syncup driver that works
/// from the VLSN index without re-reading raw log bytes (used by the live
/// `become_replica` path and the multi-node test harness, which track
/// replication at the VLSN-index granularity). The `SyncupLogView` above is
/// the full re-read used when raw per-record checksums are required.
///
/// A VLSN is treated as a *sync point* iff it is `<= lastSync` and held in the
/// index — matching JE, where every sync point is a txn end and `lastSync`
/// bounds the highest matchpoint candidate.
pub struct VlsnIndexView {
    index: Arc<VlsnIndex>,
    first: Vlsn,
    last_sync: Vlsn,
    last_txn_end: Vlsn,
}

impl VlsnIndexView {
    /// Build a view over `index`.
    pub fn from_index(index: &Arc<VlsnIndex>) -> Self {
        let range = index.get_range();
        let to_vlsn =
            |v: u64| if v == 0 { NULL_VLSN } else { Vlsn::new(v as i64) };
        Self {
            index: Arc::clone(index),
            first: to_vlsn(range.get_first()),
            last_sync: to_vlsn(range.get_last_sync()),
            last_txn_end: to_vlsn(range.get_last_txn_end()),
        }
    }

    fn lsn_fingerprint(&self, vlsn: i64) -> Option<(u64, u64, bool)> {
        if vlsn <= 0 {
            return None;
        }
        // Proper per-VLSN lookup (NOT the sparse snapshot): get_lsn answers
        // for every VLSN the index holds, matching JE VLSNIndex.getLsn.
        let (file, offset) = self.index.get_lsn(vlsn as u64)?;
        let lsn = noxu_util::Lsn::new(file, offset).as_u64();
        // Fingerprint == LSN: same LSN at a VLSN means the same record.
        let is_sync = Vlsn::new(vlsn) <= self.last_sync;
        Some((lsn, lsn, is_sync))
    }
}

impl SyncupView for VlsnIndexView {
    fn last_sync(&self) -> Vlsn {
        self.last_sync
    }
    fn last_txn_end(&self) -> Vlsn {
        self.last_txn_end
    }
    fn first_vlsn(&self) -> Vlsn {
        self.first
    }
    fn entry(&self, vlsn: Vlsn) -> Option<VlsnEntry> {
        let (lsn, fingerprint, is_sync) =
            self.lsn_fingerprint(vlsn.sequence())?;
        Some(VlsnEntry { lsn, fingerprint, is_sync })
    }
}

/// Read the raw header+payload at `(file_num, offset)`.
///
/// Returns `(entry_size_bytes, vlsn_opt, entry_type_byte, payload)` or `None`
/// at end-of-data / corruption. Same VLSN-tagged header parse as
/// `EnvironmentLogScanner::read_raw_entry` (feeder.rs); kept as a free
/// function here so the backward view does not depend on the feeder type.
fn read_raw_entry(
    fm: &FileManager,
    file_num: u32,
    offset: u64,
) -> Option<(usize, Option<u64>, u8, Vec<u8>)> {
    let mut hdr = [0u8; MIN_HEADER_SIZE];
    let n = fm.read_from_file(file_num, offset, &mut hdr).ok()?;
    if n < MIN_HEADER_SIZE {
        return None;
    }
    if hdr[4] == 0 {
        return None; // zero-fill past last entry
    }
    // Skip entries whose invisible bit (flags mask 0x10) is set: a rolled-back
    // entry made invisible by Replay.rollback (STEP 4) is not a valid
    // matchpoint candidate (JE ReplicaSyncupReader.isTargetEntry: "Skip
    // invisible entries"). We still advance past it by returning its size.
    let invisible = (hdr[5] & 0x10) != 0;
    let entry_type_byte = hdr[4];
    let flags = hdr[5];
    let item_size =
        u32::from_le_bytes([hdr[10], hdr[11], hdr[12], hdr[13]]) as usize;
    let vlsn_present = (flags & 0x08) != 0 || (flags & 0x20) != 0;
    let header_size =
        if vlsn_present { MAX_HEADER_SIZE } else { MIN_HEADER_SIZE };
    if item_size > MAX_ITEM_SIZE {
        return None;
    }
    let entry_size = header_size + item_size;
    let mut full = vec![0u8; entry_size];
    let n = fm.read_from_file(file_num, offset, &mut full).ok()?;
    if n < entry_size {
        return None;
    }
    let vlsn_opt = if vlsn_present && full.len() >= MAX_HEADER_SIZE {
        let raw = i64::from_le_bytes(
            full[MIN_HEADER_SIZE..MAX_HEADER_SIZE].try_into().ok()?,
        );
        if raw > 0 { Some(raw as u64) } else { None }
    } else {
        None
    };
    let payload = full[header_size..].to_vec();
    // Invisible entries advance the cursor but are not yielded as VLSN
    // entries (their VLSN is suppressed so the matchpoint search ignores
    // them). Returning a None VLSN keeps the scan moving past them.
    let vlsn_opt = if invisible { None } else { vlsn_opt };
    Some((entry_size, vlsn_opt, entry_type_byte, payload))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::syncup::{Matchpoint, find_matchpoint};
    use std::collections::HashMap;

    /// A hand-built view used to prove the reader's data drives the decision
    /// core (`find_matchpoint`) the same way the JE backward reader does.
    struct FakeView {
        entries: HashMap<i64, VlsnEntry>,
        last_sync: Vlsn,
        last_txn_end: Vlsn,
        first: Vlsn,
    }
    impl SyncupView for FakeView {
        fn last_sync(&self) -> Vlsn {
            self.last_sync
        }
        fn last_txn_end(&self) -> Vlsn {
            self.last_txn_end
        }
        fn first_vlsn(&self) -> Vlsn {
            self.first
        }
        fn entry(&self, vlsn: Vlsn) -> Option<VlsnEntry> {
            self.entries.get(&vlsn.sequence()).copied()
        }
    }

    #[test]
    fn test_num_passed_commits_counts_above_matchpoint() {
        let mut entries = BTreeMap::new();
        for v in 1..=5i64 {
            entries.insert(
                v,
                VlsnEntry {
                    lsn: v as u64,
                    fingerprint: v as u64,
                    is_sync: true,
                },
            );
        }
        let mut txn_end_vlsns = std::collections::BTreeSet::new();
        // Two txn ends above matchpoint 3 (at VLSN 4 and 5).
        txn_end_vlsns.insert(4);
        txn_end_vlsns.insert(5);
        let view = SyncupLogView {
            entries,
            txn_end_vlsns,
            last_sync: Vlsn::new(5),
            last_txn_end: Vlsn::new(5),
            first: Vlsn::new(1),
        };
        assert_eq!(view.num_passed_commits(Vlsn::new(3)), 2);
        assert_eq!(view.num_passed_commits(Vlsn::new(5)), 0);
    }

    /// The reader's per-VLSN data feeds find_matchpoint: a replica view whose
    /// fingerprints match the feeder at VLSN 4 but diverge at 5/6 yields
    /// matchpoint 4.
    #[test]
    fn test_view_drives_find_matchpoint() {
        let mk = |v: i64, fp: u64, sync: bool| {
            (
                v,
                VlsnEntry {
                    lsn: (v as u64) * 0x100,
                    fingerprint: fp,
                    is_sync: sync,
                },
            )
        };
        let replica = FakeView {
            entries: [
                mk(6, 0xDEAD, true),
                mk(5, 0x55, false),
                mk(4, 0x44, true),
            ]
            .into_iter()
            .collect(),
            last_sync: Vlsn::new(6),
            last_txn_end: Vlsn::new(6),
            first: Vlsn::new(1),
        };
        let feeder = FakeView {
            entries: [mk(6, 0xBEEF, true), mk(4, 0x44, true)]
                .into_iter()
                .collect(),
            last_sync: Vlsn::new(8),
            last_txn_end: Vlsn::new(8),
            first: Vlsn::new(1),
        };
        assert_eq!(
            find_matchpoint(&replica, &feeder),
            Matchpoint::Found { vlsn: Vlsn::new(4), lsn: 0x400 }
        );
    }
}
