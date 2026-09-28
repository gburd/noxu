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
    /// VLSN → raw log entry type byte, for every VLSN in the scan.
    ///
    /// Feeds [`crate::stream::syncup::classify_tail`], the safety gate that
    /// decides whether a diverged tail may be discarded: the decision turns on
    /// the exact entry TYPE of each tail entry (a provisional transactional LN
    /// was never applied to the live tree; a non-transactional LN was). Kept
    /// out of [`VlsnEntry`] for the same reason as `txn_end_vlsns`.
    entry_types: BTreeMap<i64, u8>,
    /// Per-VLSN details of every replicated transaction-end scanned, so the
    /// syncup driver can report the earliest passed transaction (JE
    /// `MatchpointSearchResults.getEarliestPassedTxn` → `PassedTxnInfo`) and
    /// count passed commits by VLSN. Only replicated (VLSN-tagged)
    /// commit/abort records appear here, matching JE's reader, which counts
    /// only entries for which `entryIsReplicated()` is true.
    passed_txns: BTreeMap<i64, PassedTxn>,
    /// Whether the scan stepped over a checkpoint-end whose
    /// `cleaned_files_to_delete` flag is set (JE
    /// `MatchpointSearchResults.getPassedCheckpointEnd`, set by
    /// `notePassedCheckpointEnd` only when
    /// `CheckpointEnd.getCleanedFilesToDelete()` is true).
    ///
    /// Scan-wide (not matchpoint-relative): a checkpoint-end is not a
    /// replicated entry and carries no VLSN, so it cannot be filtered by a
    /// matchpoint VLSN. This mirrors the JE test's usage, where the backward
    /// scan runs down to the bottom of the newly populated region (matchpoint
    /// at/below the first populated VLSN) so every populated checkpoint-end is
    /// "passed".
    passed_checkpoint_end: bool,
    /// Highest sync-point VLSN seen (JE `VLSNRange.getLastSync`).
    last_sync: Vlsn,
    /// Highest commit/abort VLSN seen (JE `VLSNRange.getLastTxnEnd`).
    last_txn_end: Vlsn,
    /// First (lowest) VLSN available (JE `VLSNRange.getFirst`).
    first: Vlsn,
}

/// The details of a transaction-end record the syncup backward scan stepped
/// over, mirroring JE `MatchpointSearchResults.PassedTxnInfo` (`id`, `time`,
/// `lsn`). Used to report the *earliest* passed transaction, which the syncup
/// driver logs for the operator and JE's `verifyRollback` uses when adjusting
/// the passed-commit count at the matchpoint boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PassedTxn {
    /// Transaction id (JE `PassedTxnInfo.id`).
    pub id: i64,
    /// Commit/abort timestamp in milliseconds (JE `PassedTxnInfo.time`).
    pub time_ms: u64,
    /// LSN of the transaction-end record (JE `PassedTxnInfo.lsn`).
    pub lsn: u64,
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
        let mut entry_types: BTreeMap<i64, u8> = BTreeMap::new();
        let mut txn_end_vlsns: std::collections::BTreeSet<i64> =
            std::collections::BTreeSet::new();
        let mut last_sync = NULL_VLSN;
        let mut last_txn_end = NULL_VLSN;
        let mut passed_txns: BTreeMap<i64, PassedTxn> = BTreeMap::new();
        let mut passed_checkpoint_end = false;

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
                        // A checkpoint-end (JE `LOG_CKPT_END`) is not
                        // replicated and carries no VLSN, so it would be
                        // dropped by the `continue` below. But JE's reader
                        // reads it anyway (`isTargetEntry` returns true for
                        // CKPT_END) to note whether a rollback-blocking
                        // checkpoint was passed. Inspect its
                        // `cleaned_files_to_delete` flag here.
                        if noxu_log::LogEntryType::from_type_num(type_byte)
                            == Some(noxu_log::LogEntryType::CkptEnd)
                            && ckpt_end_cleaned_files(&payload)
                        {
                            passed_checkpoint_end = true;
                        }
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
                        entry_types.insert(vlsn as i64, type_byte);
                        let v = Vlsn::new(vlsn as i64);
                        if is_sync && v > last_sync {
                            last_sync = v;
                        }
                        if is_txn_end {
                            txn_end_vlsns.insert(vlsn as i64);
                            if v > last_txn_end {
                                last_txn_end = v;
                            }
                            // JE `notePassedCommits`/`notePassedAborts`: record
                            // the txn id + timestamp so the earliest passed
                            // txn can be reported. The payload layout is the
                            // `TxnEnd` header (id:i64 BE, timestamp:u64 BE,
                            // last_lsn:u64 BE, ...), identical for commit and
                            // abort.
                            if let Some((id, time_ms)) =
                                txn_end_id_and_time(&payload)
                            {
                                passed_txns.insert(
                                    vlsn as i64,
                                    PassedTxn { id, time_ms, lsn },
                                );
                            }
                        }
                    }
                }
            }
        }

        let first =
            entries.keys().next().map(|&v| Vlsn::new(v)).unwrap_or(NULL_VLSN);

        Self {
            entries,
            txn_end_vlsns,
            entry_types,
            passed_txns,
            passed_checkpoint_end,
            last_sync,
            last_txn_end,
            first,
        }
    }

    /// The log entry type of every VLSN strictly above `matchpoint`, in
    /// ascending VLSN order — the input to
    /// [`crate::stream::syncup::classify_tail`].
    ///
    /// An entry whose type byte does not decode to a known
    /// [`noxu_log::LogEntryType`] is reported as `None`; the gate treats an
    /// undecodable tail entry as unsafe (it cannot prove the entry was never
    /// applied to the tree).
    pub fn tail_types(
        &self,
        matchpoint: Vlsn,
    ) -> Vec<(Vlsn, Option<noxu_log::LogEntryType>)> {
        let floor = matchpoint.sequence();
        self.entry_types
            .range((floor + 1)..)
            .map(|(&v, &ty)| {
                (Vlsn::new(v), noxu_log::LogEntryType::from_type_num(ty))
            })
            .collect()
    }

    /// Count the commit/abort records strictly above `matchpoint` (JE
    /// `MatchpointSearchResults.getNumPassedCommits`). `verify_rollback` uses
    /// this to force HardRecovery when the backward scan stepped over a txn
    /// end even if `lastTxnEnd <= matchpoint` numerically.
    pub fn num_passed_commits(&self, matchpoint: Vlsn) -> u64 {
        let floor = matchpoint.sequence();
        self.txn_end_vlsns.range((floor + 1)..).count() as u64
    }

    /// Whether a checkpoint-end with `cleaned_files_to_delete` set was scanned
    /// (JE `MatchpointSearchResults.getPassedCheckpointEnd`). Scan-wide; see
    /// the field doc for why it is not matchpoint-relative.
    pub fn passed_checkpoint_end(&self) -> bool {
        self.passed_checkpoint_end
    }

    /// The earliest (lowest-VLSN) replicated transaction end scanned strictly
    /// above `matchpoint` (JE `MatchpointSearchResults.getEarliestPassedTxn`).
    /// `None` if no replicated txn end was passed.
    pub fn earliest_passed_txn(&self, matchpoint: Vlsn) -> Option<PassedTxn> {
        let floor = matchpoint.sequence();
        self.passed_txns.range((floor + 1)..).next().map(|(_, t)| *t)
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

/// Extract the transaction id and commit/abort timestamp from a `TxnEnd`
/// payload (JE `TxnCommit`/`TxnAbort` main item). Layout: `id:i64 BE`,
/// `timestamp_ms:u64 BE`, then `last_lsn`/`master_id`/`dtvlsn`. Returns `None`
/// if the payload is too short.
fn txn_end_id_and_time(payload: &[u8]) -> Option<(i64, u64)> {
    if payload.len() < 16 {
        return None;
    }
    let id = i64::from_be_bytes(payload[0..8].try_into().ok()?);
    let time_ms = u64::from_be_bytes(payload[8..16].try_into().ok()?);
    Some((id, time_ms))
}

/// Read the `cleaned_files_to_delete` flag from a `CheckpointEnd` payload by
/// decoding it with the owning crate's reader (JE
/// `CheckpointEnd.getCleanedFilesToDelete()`). The flag lives after the
/// variable-length invoker string, so it cannot be read at a fixed offset;
/// `CheckpointEnd::read_from_log` handles the layout (and the v1/v2 trailer).
/// Returns false if the payload does not decode as a checkpoint-end.
fn ckpt_end_cleaned_files(payload: &[u8]) -> bool {
    noxu_recovery::CheckpointEnd::read_from_log(payload)
        .map(|c| c.get_cleaned_files_to_delete())
        .unwrap_or(false)
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
            entry_types: BTreeMap::new(),
            passed_txns: BTreeMap::new(),
            passed_checkpoint_end: false,
            last_sync: Vlsn::new(5),
            last_txn_end: Vlsn::new(5),
            first: Vlsn::new(1),
        };
        assert_eq!(view.num_passed_commits(Vlsn::new(3)), 2);
        assert_eq!(view.num_passed_commits(Vlsn::new(5)), 0);
    }

    /// `tail_types` reports the entry type of every VLSN STRICTLY above the
    /// matchpoint, ascending — the exact input the safety gate
    /// (`classify_tail`) needs. Undecodable type bytes surface as `None` so the
    /// gate can refuse them.
    #[test]
    fn test_tail_types_reports_entries_above_matchpoint() {
        use noxu_log::LogEntryType as T;

        let mut entry_types = BTreeMap::new();
        entry_types.insert(4, T::TxnCommit as u8);
        entry_types.insert(5, T::InsertLNTxn as u8);
        entry_types.insert(6, T::InsertLN as u8);
        entry_types.insert(7, 0xFE); // not a known LogEntryType
        let view = SyncupLogView {
            entries: BTreeMap::new(),
            txn_end_vlsns: std::collections::BTreeSet::new(),
            entry_types,
            passed_txns: BTreeMap::new(),
            passed_checkpoint_end: false,
            last_sync: Vlsn::new(7),
            last_txn_end: Vlsn::new(4),
            first: Vlsn::new(1),
        };

        assert_eq!(
            view.tail_types(Vlsn::new(4)),
            vec![
                (Vlsn::new(5), Some(T::InsertLNTxn)),
                (Vlsn::new(6), Some(T::InsertLN)),
                (Vlsn::new(7), None),
            ],
            "matchpoint 4 is EXCLUDED; the tail is 5,6,7 ascending"
        );
        // A tail that stops at the last VLSN is empty (not diverged).
        assert!(view.tail_types(Vlsn::new(7)).is_empty());

        // And the gate refuses this tail (VLSN 6 is a non-txn LN).
        assert!(matches!(
            crate::stream::syncup::classify_tail(view.tail_types(Vlsn::new(4))),
            crate::stream::syncup::TailSafety::Refuse { .. }
        ));
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

    // ── VlsnIndexView ───────────────────────────────────────────────

    /// `VlsnIndexView::entry` must look up the real (file, offset) LSN for a
    /// VLSN the index holds, use that LSN as the fingerprint (same LSN at
    /// the same VLSN == same record, per this view's doc contract), and mark
    /// it a sync point iff its VLSN is <= the index's `last_sync`.
    #[test]
    fn vlsn_index_view_entry_reports_lsn_fingerprint_and_sync_flag() {
        let index = Arc::new(VlsnIndex::new(4));
        index.put_with_type(
            1,
            7,
            100,
            noxu_log::entry_type::LogEntryType::TxnCommit,
        );
        index.put(2, 7, 200);

        let view = VlsnIndexView::from_index(&index);

        let expected_lsn_1 = noxu_util::Lsn::new(7, 100).as_u64();
        let e1 = view.entry(Vlsn::new(1)).expect("vlsn 1 must be present");
        assert_eq!(e1.lsn, expected_lsn_1);
        assert_eq!(e1.fingerprint, expected_lsn_1, "fingerprint == lsn");
        assert!(e1.is_sync, "a commit is a sync point");

        let expected_lsn_2 = noxu_util::Lsn::new(7, 200).as_u64();
        let e2 = view.entry(Vlsn::new(2)).expect("vlsn 2 must be present");
        assert_eq!(e2.lsn, expected_lsn_2);
        assert!(
            !e2.is_sync,
            "vlsn 2 is above last_sync (only vlsn 1 was a commit)"
        );

        assert_eq!(view.last_sync(), Vlsn::new(1));
        assert_eq!(view.last_txn_end(), Vlsn::new(1));
        assert_eq!(view.first_vlsn(), Vlsn::new(1));
    }

    /// A VLSN the index has never seen must report `None`, not panic — the
    /// syncup driver probes candidate matchpoints that may be outside the
    /// range actually held.
    #[test]
    fn vlsn_index_view_entry_missing_vlsn_is_none() {
        let index = Arc::new(VlsnIndex::new(4));
        index.put(1, 7, 100);
        let view = VlsnIndexView::from_index(&index);
        assert!(view.entry(Vlsn::new(99)).is_none());
    }

    /// `lsn_fingerprint`'s `vlsn <= 0` guard must reject `NULL_VLSN` (0)
    /// rather than treat it as a valid lookup key.
    #[test]
    fn vlsn_index_view_entry_rejects_null_vlsn() {
        let index = Arc::new(VlsnIndex::new(4));
        index.put(1, 7, 100);
        let view = VlsnIndexView::from_index(&index);
        assert!(view.entry(NULL_VLSN).is_none());
    }

    /// An empty index must report the null range via `from_index`, proving
    /// the `range.get_first() == 0` → `NULL_VLSN` mapping in `from_index`.
    #[test]
    fn vlsn_index_view_from_empty_index_is_null_range() {
        let index = Arc::new(VlsnIndex::new(4));
        let view = VlsnIndexView::from_index(&index);
        assert_eq!(view.first_vlsn(), NULL_VLSN);
        assert_eq!(view.last_sync(), NULL_VLSN);
        assert_eq!(view.last_txn_end(), NULL_VLSN);
    }

    // ── read_raw_entry / scan edge cases ────────────────────────────

    /// `scan_with_manager` over a `FileManager` with no log files at all
    /// must yield an empty, null-range view rather than panicking on the
    /// `entries.keys().next()` fallback.
    #[test]
    fn scan_with_manager_on_empty_env_yields_null_range() {
        let dir = tempfile::TempDir::new().unwrap();
        let fm = FileManager::new(dir.path(), false, 256 * 1024 * 1024, 32)
            .expect("FileManager must open an empty, writable env dir");
        let view = SyncupLogView::scan_with_manager(&fm);
        assert_eq!(view.first_vlsn(), NULL_VLSN);
        assert_eq!(view.last_sync(), NULL_VLSN);
        assert_eq!(view.last_txn_end(), NULL_VLSN);
        assert!(view.entries().next().is_none());
    }

    /// `SyncupLogView::scan` (the `Path`-based entry point, as opposed to
    /// `scan_with_manager`) must succeed against a real (empty) env
    /// directory and return a usable, empty view.
    #[test]
    fn scan_opens_its_own_file_manager() {
        let dir = tempfile::TempDir::new().unwrap();
        let view = SyncupLogView::scan(dir.path())
            .expect("scan must open a FileManager for an existing directory");
        assert_eq!(view.first_vlsn(), NULL_VLSN);
    }
    // ── ReplicaSyncupReaderTest (JE) ────────────────────────────────────
    //
    // Port of com.sleepycat.je.rep.stream.ReplicaSyncupReaderTest. The JE test
    // populates a real replicated log with a controlled mix of checkpoint-end
    // and commit records (some replicated, some not), scans it backward with a
    // ReplicaSyncupReader, and asserts the MatchpointSearchResults counters:
    //   - getNumPassedCommits()   — only REPLICATED commits are counted;
    //   - getPassedCheckpointEnd()— true iff a CKPT_END with
    //                               cleanedFilesToDelete was passed;
    //   - getEarliestPassedTxn()  — {id, time, lsn} of the earliest passed txn.
    //
    // Noxu's `SyncupLogView` is the equivalent backward-scan bookkeeping
    // (num_passed_commits / passed_checkpoint_end / earliest_passed_txn). We
    // reproduce the JE fixture by writing the same entry mix to a real log via
    // the FileManager and scanning it, so the same invariants the JE test
    // guards (non-replicated commits ignored; checkpoint-end noted only when it
    // cleans files; earliest passed txn recorded) are exercised end to end.
    //
    // Deviation from the JE mechanism (documented): JE assigns VLSNs through the
    // live replication write path and bounds the backward scan with finishLSN.
    // Noxu writes a fresh log containing ONLY the fixture entries, so the whole
    // log IS the "newly populated" region and the matchpoint is the bottom
    // (NULL) — identical in effect to JE's finishLSN bound. Non-replicated
    // entries are written WITHOUT a VLSN (JE ReplicationContext.NO_REPLICATE),
    // which is exactly why the reader must count them out.

    use noxu_log::LogEntryType;
    use noxu_log::{LogEntryHeader, Provisional};
    use noxu_txn::TxnCommit;

    /// Append one log entry (header + payload) at `offset` in file 0, returning
    /// the offset past it. `vlsn = Some(_)` marks the entry replicated (JE
    /// ReplicationContext.MASTER); `None` marks it non-replicated
    /// (NO_REPLICATE) — no VLSN in the header, so the syncup reader ignores it
    /// for commit-counting, exactly as JE's `entryIsReplicated()` gate does.
    fn append_entry(
        fm: &FileManager,
        offset: u64,
        entry_type: LogEntryType,
        vlsn: Option<Vlsn>,
        payload: &[u8],
    ) -> u64 {
        let mut header = LogEntryHeader::new(
            entry_type,
            payload.len() as u32,
            Provisional::No,
            vlsn.is_some(),
            vlsn,
        );
        let mut buf = Vec::new();
        header.write_to_log(&mut buf).unwrap();
        buf.extend_from_slice(payload);
        // Fill in prev_offset/vlsn/checksum. The syncup reader does not
        // validate the checksum, but a real record carries one, so compute it.
        let checksum = crc32fast::hash(&buf[4..]);
        header.add_post_marshalling_info(&mut buf, 0, vlsn, checksum).unwrap();
        fm.write_buffer_to_file(0, &buf, offset).unwrap();
        offset + buf.len() as u64
    }

    /// Serialize a `TxnCommit` main-item payload (the `TxnEnd` header the
    /// syncup reader parses for id/timestamp).
    fn commit_payload(id: i64) -> Vec<u8> {
        let mut buf = Vec::new();
        // last_lsn/master_id/dtvlsn are irrelevant to the reader's id/time
        // extraction; use plausible values.
        TxnCommit::new(id, 1, 1, 1).write_to_log(&mut buf);
        buf
    }

    /// Serialize a `CheckpointEnd` payload with the given
    /// `cleaned_files_to_delete` flag (the flag JE keys `passedCheckpointEnd`
    /// on).
    fn ckpt_end_payload(cleaned_files_to_delete: bool) -> Vec<u8> {
        let ckpt = noxu_recovery::CheckpointEnd::new(
            1,
            "test",
            noxu_util::Lsn::from_u64(0),
            None,
            noxu_util::Lsn::from_u64(0),
            0u64,
            0i64,
            0u64,
            0i64,
            0u64,
            0i64,
            cleaned_files_to_delete,
        );
        let mut buf = Vec::new();
        ckpt.write_to_log(&mut buf).unwrap();
        buf
    }

    /// Open a writable FileManager over a fresh env dir and return it with the
    /// first-entry offset for file 0.
    fn fresh_log(dir: &Path) -> (FileManager, u64) {
        let fm = FileManager::new(dir, false, 256 * 1024 * 1024, 32)
            .expect("writable FileManager");
        // Force file 0 to exist with its header by writing a zero-length entry
        // region: the first real append creates + headers the file.
        let header_size =
            noxu_log::file_header::on_disk_size(LOG_FILE_VERSION) as u64;
        (fm, header_size)
    }

    /// JE ReplicaSyncupReaderTest.testRepAndNonRepCommits: a CKPT_END (cleans
    /// files) then a NON-replicated commit (txn 10) then a REPLICATED commit
    /// (txn 20). Expected: numPassedCommits=1 (only the replicated txn 20),
    /// passedCheckpointEnd=true, earliestTxnId=20.
    #[test]
    fn test_rep_and_non_rep_commits() {
        let dir = tempfile::TempDir::new().unwrap();
        let (fm, mut off) = fresh_log(dir.path());

        // CKPT_END, cleanedFilesToDelete=true, NOT replicated (no VLSN).
        off = append_entry(
            &fm,
            off,
            LogEntryType::CkptEnd,
            None,
            &ckpt_end_payload(true),
        );
        // Commit txn 10, NOT replicated → must be ignored by the reader.
        off = append_entry(
            &fm,
            off,
            LogEntryType::TxnCommit,
            None,
            &commit_payload(10),
        );
        // Commit txn 20, replicated (VLSN 1) → the only passed commit.
        let _ = append_entry(
            &fm,
            off,
            LogEntryType::TxnCommit,
            Some(Vlsn::new(1)),
            &commit_payload(20),
        );

        let view = SyncupLogView::scan_with_manager(&fm);

        // Only the replicated commit is counted (matchpoint at the bottom).
        assert_eq!(
            view.num_passed_commits(NULL_VLSN),
            1,
            "only the replicated commit (txn 20) is counted; the \
             non-replicated commit (txn 10) is ignored"
        );
        // The checkpoint-end cleaned files → passed-checkpoint-end noted.
        assert!(
            view.passed_checkpoint_end(),
            "a CKPT_END with cleaned_files_to_delete must be noted"
        );
        // Earliest (only) passed txn is txn 20.
        let earliest =
            view.earliest_passed_txn(NULL_VLSN).expect("one passed txn");
        assert_eq!(earliest.id, 20, "earliest passed txn id");
    }

    /// JE ReplicaSyncupReaderTest.testMultipleCkpts: two CKPT_END records that
    /// do NOT clean files, bracketing two REPLICATED commits (txn 10, txn 20).
    /// Expected: numPassedCommits=2, passedCheckpointEnd=false,
    /// earliestTxnId=10.
    #[test]
    fn test_multiple_ckpts() {
        let dir = tempfile::TempDir::new().unwrap();
        let (fm, mut off) = fresh_log(dir.path());

        // Ckpt A — does NOT clean files.
        off = append_entry(
            &fm,
            off,
            LogEntryType::CkptEnd,
            None,
            &ckpt_end_payload(false),
        );
        // Commit A (txn 10), replicated at VLSN 1 → earliest passed txn.
        off = append_entry(
            &fm,
            off,
            LogEntryType::TxnCommit,
            Some(Vlsn::new(1)),
            &commit_payload(10),
        );
        // Commit B (txn 20), replicated at VLSN 2.
        off = append_entry(
            &fm,
            off,
            LogEntryType::TxnCommit,
            Some(Vlsn::new(2)),
            &commit_payload(20),
        );
        // Ckpt B — does NOT clean files.
        let _ = append_entry(
            &fm,
            off,
            LogEntryType::CkptEnd,
            None,
            &ckpt_end_payload(false),
        );

        let view = SyncupLogView::scan_with_manager(&fm);

        assert_eq!(
            view.num_passed_commits(NULL_VLSN),
            2,
            "both replicated commits (txn 10 and txn 20) are counted"
        );
        assert!(
            !view.passed_checkpoint_end(),
            "neither CKPT_END cleaned files, so passed-checkpoint-end is false"
        );
        let earliest =
            view.earliest_passed_txn(NULL_VLSN).expect("two passed txns");
        assert_eq!(
            earliest.id, 10,
            "earliest passed txn is the lowest-VLSN one (txn 10)"
        );
    }

    /// Guard against a vacuous port: prove the reader ACTUALLY excludes
    /// non-replicated commits (would-count-them regression would flip this).
    /// A log of three commits, only the middle one replicated, must report
    /// exactly one passed commit — with the replicated one's id.
    #[test]
    fn test_non_replicated_commits_are_not_counted() {
        let dir = tempfile::TempDir::new().unwrap();
        let (fm, mut off) = fresh_log(dir.path());

        off = append_entry(
            &fm,
            off,
            LogEntryType::TxnCommit,
            None,
            &commit_payload(100),
        );
        off = append_entry(
            &fm,
            off,
            LogEntryType::TxnCommit,
            Some(Vlsn::new(1)),
            &commit_payload(200),
        );
        let _ = append_entry(
            &fm,
            off,
            LogEntryType::TxnCommit,
            None,
            &commit_payload(300),
        );

        let view = SyncupLogView::scan_with_manager(&fm);
        assert_eq!(view.num_passed_commits(NULL_VLSN), 1);
        assert_eq!(view.earliest_passed_txn(NULL_VLSN).unwrap().id, 200);
    }
}
