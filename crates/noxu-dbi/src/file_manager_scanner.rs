//! File-manager-backed log scanner for recovery.
//!
//! Implements `LogScanner` over a real `FileManager` + `LogManager` pair so
//! that `RecoveryManager::recover()` can be called during `Environment::open()`
//! on an existing database directory.
//!
//! Log file scanning utilities.
//! `RecoveryManager.recover()`.

use std::sync::Arc;

use bytes::Bytes;
use noxu_log::{
    FileManager,
    checksum::ChecksumValidator,
    entry::{
        BinDeltaLogEntry, InLogEntry, LnLogEntry, TxnEndEntry, TxnPrepareEntry,
    },
    entry_header::{CHECKSUM_BYTES, MAX_HEADER_SIZE, MIN_HEADER_SIZE},
    entry_type::LogEntryType,
    file_header::FILE_HEADER_SIZE,
};
use noxu_recovery::{
    CheckpointEnd, CheckpointStart, CkptEndRecord, CkptStartRecord, InRecord,
    LnOperation, LnRecord, LogEntry, LogScanner, PositionedEntry,
    TxnAbortRecord, TxnCommitRecord, TxnPrepareRecord,
};
use noxu_util::{Lsn, NULL_LSN};

// Maximum plausible payload size (64 MiB) for a sanity check while scanning.
const MAX_SANE_ITEM_SIZE: usize = 64 * 1024 * 1024;

/// Compute the byte range of `child` within `parent`.
///
/// Panics in debug builds if `child` is not a subslice of `parent`; in release
/// builds the range is silently clamped (unreachable in correct code).
#[inline]
fn subslice_range(parent: &[u8], child: &[u8]) -> std::ops::Range<usize> {
    let start = child.as_ptr() as usize - parent.as_ptr() as usize;
    start..start + child.len()
}

/// `LogScanner` implementation backed by a real `FileManager`.
///
/// Implements the three scan primitives required by `RecoveryManager`:
/// - `find_end_of_log` — scans log files to discover the true end of the log
///   and updates the `FileManager`'s LSN positions via `set_last_position()`.
/// - `scan_forward`  — sequential forward scan over a LSN range.
/// - `scan_backward` — reverse-order scan (implemented as forward + reverse).
pub struct FileManagerLogScanner {
    file_manager: Arc<FileManager>,

    /// When true, a checksum failure during `find_end_of_log` triggers a
    /// forward scan for a `TxnCommit` past the corruption; if one is found,
    /// `find_end_of_log` HALTS (returns a sentinel that recovery surfaces as
    /// `EnvironmentFailureReason::FoundCommittedTxn`) instead of silently
    /// truncating.  Wired from `noxu.haltOnCommitAfterChecksumException`.
    ///
    /// Faithful to JE `LastFileReader.readNextEntry`/`findCommittedTxn`
    /// (LastFileReader.java:313/394, [#18307]).
    halt_on_commit_after_checksum: bool,

    /// Set by `find_end_of_log` when `halt_on_commit_after_checksum` is on and
    /// a `TxnCommit` is found past a mid-file corruption point. Holds
    /// `(corrupt_lsn, commit_lsn)`.
    found_committed_txn: Option<(Lsn, Lsn)>,
}

impl FileManagerLogScanner {
    pub fn new(file_manager: Arc<FileManager>) -> Self {
        Self {
            file_manager,
            halt_on_commit_after_checksum: false,
            found_committed_txn: None,
        }
    }

    /// Enable halt-on-committed-txn-after-checksum during `find_end_of_log`.
    ///
    /// Recovery passes `cfg.halt_on_commit_after_checksum_exception` here.
    pub fn with_halt_on_commit(mut self, halt: bool) -> Self {
        self.halt_on_commit_after_checksum = halt;
        self
    }

    /// Take the `(corrupt_lsn, commit_lsn)` recorded by `find_end_of_log`
    /// when a committed txn was found past a mid-file corruption point.
    ///
    /// Recovery checks this after `find_end_of_log`/`recover_all` and, if
    /// `Some`, surfaces a fatal `EnvironmentFailureReason::FoundCommittedTxn`
    /// instead of accepting the silently-truncated log.
    ///
    /// Faithful to JE `LastFileReader.readNextEntry` throwing
    /// `EnvironmentFailureException(FOUND_COMMITTED_TXN, ...)`
    /// (LastFileReader.java:313).
    pub fn take_found_committed_txn(&mut self) -> Option<(Lsn, Lsn)> {
        self.found_committed_txn.take()
    }

    /// Load a log file as a [`Bytes`] buffer for sequential scanning.
    ///
    /// Tries `mmap` first via [`Bytes::from_owner`] so the OS-managed pages
    /// back the buffer without any heap copy.  Falls back to a single
    /// `pread64` into a `Vec<u8>` — owned by the `Bytes` — if mmap fails.
    ///
    /// Either way, callers get a `Bytes` that can be sliced zero-copy.
    fn load_file_bytes(&self, file_num: u32, file_len: u64) -> Option<Bytes> {
        // Try mmap first: Bytes::from_owner stores the Mmap as the owner,
        // keeping the pages mapped for as long as the Bytes (or any slice
        // derived from it) is alive.
        if let Ok(mmap) = self.file_manager.mmap_file(file_num) {
            return Some(Bytes::from_owner(mmap));
        }
        // Fallback: single pread64 into a heap Vec, then move into Bytes
        // (no copy — Bytes::from(Vec<u8>) takes ownership).
        let mut buf = vec![0u8; file_len as usize];
        match self.file_manager.read_from_file(file_num, 0, &mut buf) {
            Ok(n) if n < file_len as usize => buf.truncate(n),
            Err(_) => return None,
            Ok(_) => {}
        }
        Some(Bytes::from(buf))
    }

    /// Convert a raw (entry_type_num, payload_bytes, vlsn) triple to a
    /// `LogEntry`.
    ///
    /// `payload` is a `Bytes` slice pointing into the file buffer.  For LN
    /// entries the key/data fields are created as sub-slices of `payload`
    /// via `Bytes::slice` — zero heap allocation until the bytes are
    /// materialised into the B-tree at the redo/undo boundary.
    ///
    /// `vlsn` is `Some` when the original log entry header carried a VLSN
    /// (`vlsn_present` flag set) and the value is plausible.  It is
    /// attached to the resulting `LnRecord` so that recovery can verify
    /// VLSN ordering on the replicated-entry stream (security review
    /// LOG-6).
    ///
    /// Returns `None` for entry types recovery does not need to process
    /// (FileHeader, Trace, etc.).
    fn parse_payload(
        entry_type_num: u8,
        payload: Bytes,
        vlsn: Option<u64>,
        flags: u8,
    ) -> Option<LogEntry> {
        // JE Provisional enum bits (entry_header.rs):
        //   0x80 = PROVISIONAL_ALWAYS (always provisional, never replay)
        //   0x40 = PROVISIONAL_BEFORE_CKPT_END (provisional until CkptEnd)
        // Stage 2 (DRIFT-3): is_provisional is set for INs that must be
        // filtered by the recovery redo pass.
        let is_provisional = (flags & 0x80) != 0 || (flags & 0x40) != 0;
        let entry_type = LogEntryType::from_type_num(entry_type_num)?;

        match entry_type {
            // Transaction end ─────────────────────────────────────────
            LogEntryType::TxnCommit => {
                let e = TxnEndEntry::read_from_log(&payload).ok()?;
                // R-3: extract dtvlsn so the X-14 VLSN rebuild includes it.
                let dtvlsn_seq = e.dtvlsn.sequence();
                let dtvlsn =
                    if dtvlsn_seq > 0 { Some(dtvlsn_seq as u64) } else { None };
                Some(LogEntry::TxnCommit(TxnCommitRecord {
                    txn_id: e.txn_id as u64,
                    lsn: NULL_LSN, // Filled in by caller with actual LSN.
                    dtvlsn,
                }))
            }
            LogEntryType::TxnAbort => {
                let e = TxnEndEntry::read_from_log(&payload).ok()?;
                Some(LogEntry::TxnAbort(TxnAbortRecord {
                    txn_id: e.txn_id as u64,
                }))
            }
            LogEntryType::TxnPrepare => {
                // Wave 3-2: parse the XA prepare frame.  `lsn` is filled
                // in by the caller (parse_entry_from_bytes) since the
                // entry itself does not carry its own LSN.
                let e = TxnPrepareEntry::read_from_log(&payload).ok()?;
                Some(LogEntry::TxnPrepare(TxnPrepareRecord {
                    txn_id: e.txn_id as u64,
                    first_lsn: noxu_util::Lsn::from_u64(e.first_lsn),
                    last_lsn: noxu_util::Lsn::from_u64(e.last_lsn),
                    lsn: NULL_LSN,
                    xid_format_id: e.xid_format_id,
                    xid_gtrid: e.xid_gtrid,
                    xid_bqual: e.xid_bqual,
                }))
            }

            // LN entries ──────────────────────────────────────────────
            LogEntryType::InsertLN
            | LogEntryType::InsertLNTxn
            | LogEntryType::UpdateLN
            | LogEntryType::UpdateLNTxn
            | LogEntryType::DeleteLN
            | LogEntryType::DeleteLNTxn => {
                let is_txn = matches!(
                    entry_type,
                    LogEntryType::InsertLNTxn
                        | LogEntryType::UpdateLNTxn
                        | LogEntryType::DeleteLNTxn
                );
                // Zero-copy parse: LnEntryRef borrows &[u8] slices from
                // `payload`.  We then convert each slice to a Bytes::slice
                // of the same underlying mmap region — no heap allocation.
                let raw: &[u8] = &payload;
                let r = LnLogEntry::parse_from_slice(raw, is_txn).ok()?;
                let op = match entry_type {
                    LogEntryType::InsertLN | LogEntryType::InsertLNTxn => {
                        LnOperation::Insert
                    }
                    LogEntryType::UpdateLN | LogEntryType::UpdateLNTxn => {
                        LnOperation::Update
                    }
                    _ => LnOperation::Delete,
                };
                let key = payload.slice(subslice_range(raw, r.key));
                let data =
                    r.data.map(|s| payload.slice(subslice_range(raw, s)));
                let mut rec = LnRecord::new(
                    r.db_id,
                    r.txn_id.map(|id| id as u64),
                    op,
                    key,
                    data,
                    r.abort_lsn,
                    r.abort_known_deleted,
                );
                rec.abort_key =
                    r.abort_key.map(|s| payload.slice(subslice_range(raw, s)));
                rec.abort_data =
                    r.abort_data.map(|s| payload.slice(subslice_range(raw, s)));
                rec.vlsn = vlsn;
                Some(LogEntry::Ln(rec))
            }

            // IN / BIN entries ────────────────────────────────────────
            LogEntryType::IN | LogEntryType::BIN => {
                let e = InLogEntry::read_from_log(&payload).ok()?;
                // Extract node_id and level from the serialized node_data.
                //
                // BIN entries (LogEntryType::BIN): node_data is produced by
                // `BinStub::serialize_full()` — format: node_id(u64BE) |
                // num_entries(u32BE) | per-slot data.  Level = BIN_LEVEL.
                //
                // Upper-IN entries (LogEntryType::IN): node_data is produced by
                // `TreeNode::write_to_bytes()` — format: node_id(u64BE) |
                // level(i32BE) | n_entries(u32BE) | dirty(u8) | per-entry data.
                // Level is read from bytes[8..12].
                //
                // Recovery.recoverChildIN currency check (DRIFT-9) uses the level
                // to distinguish BIN vs upper-IN during recover_in_redo.
                // JE RecoveryManager.replayOneIN / IN.postRecoveryInit.
                let node_id = if e.node_data.len() >= 8 {
                    u64::from_be_bytes(e.node_data[0..8].try_into().ok()?)
                } else {
                    0
                };
                // noxu_tree::BIN_LEVEL = 0x10001; MAIN_LEVEL = 0x10000.
                // Upper-IN level bytes[8..12] will be >= MAIN_LEVEL (0x10000).
                // BIN serialize_full has num_entries there (always < 0x10000).
                let level = if entry_type == LogEntryType::BIN {
                    // BIN_LEVEL = 0x10001
                    0x10001i32
                } else if e.node_data.len() >= 12 {
                    i32::from_be_bytes(e.node_data[8..12].try_into().ok()?)
                } else {
                    // Upper IN, level unknown — use sentinel above BIN.
                    0x10002i32
                };
                Some(LogEntry::In(InRecord {
                    db_id: e.db_id,
                    node_id,
                    level,
                    is_root: false,
                    is_delta: false,
                    is_provisional,
                    node_data: Some(e.node_data),
                    prev_full_lsn: e.prev_full_lsn,
                }))
            }
            LogEntryType::BINDelta => {
                let e = BinDeltaLogEntry::read_from_log(&payload).ok()?;
                let node_id = if e.delta_data.len() >= 8 {
                    u64::from_be_bytes(e.delta_data[0..8].try_into().ok()?)
                } else {
                    0
                };
                Some(LogEntry::In(InRecord {
                    db_id: e.db_id,
                    node_id,
                    level: 0x10001i32, // BIN_LEVEL
                    is_root: false,
                    is_delta: true,
                    is_provisional,
                    node_data: Some(e.delta_data),
                    prev_full_lsn: e.prev_full_lsn,
                }))
            }

            // Checkpoint entries ──────────────────────────────────────
            LogEntryType::CkptStart => {
                let e = CheckpointStart::read_from_log(&payload).ok()?;
                Some(LogEntry::CkptStart(CkptStartRecord {
                    id: e.get_id(),
                    lsn: NULL_LSN, // Filled in by caller.
                }))
            }
            LogEntryType::CkptEnd => {
                let e = CheckpointEnd::read_from_log(&payload).ok()?;
                Some(LogEntry::CkptEnd(CkptEndRecord {
                    id: e.get_id(),
                    checkpoint_start_lsn: e.get_checkpoint_start_lsn(),
                    first_active_lsn: e.get_first_active_lsn(),
                    root_lsn: e.get_root_lsn().unwrap_or(NULL_LSN),
                    last_local_node_id: e.get_last_local_node_id(),
                    last_replicated_node_id: e.get_last_replicated_node_id(),
                    last_local_db_id: e.get_last_local_db_id(),
                    last_replicated_db_id: e.get_last_replicated_db_id(),
                    last_local_txn_id: e.get_last_local_txn_id(),
                    last_replicated_txn_id: e.get_last_replicated_txn_id(),
                }))
            }

            // NameLN / NameLNTxn: database name registration entries.
            // Parse using the LnLogEntry format: key = db_name bytes,
            // data = 8-byte little-endian db_id (or None for deletion).
            // db_id in the entry header is ignored (we use the data field).
            LogEntryType::NameLN | LogEntryType::NameLNTxn => {
                let is_txn = entry_type == LogEntryType::NameLNTxn;
                let raw: &[u8] = &payload;
                let r = LnLogEntry::parse_from_slice(raw, is_txn).ok()?;
                let name = String::from_utf8(r.key.to_vec()).ok()?;
                if let Some(data_bytes) = r.data {
                    if data_bytes.len() >= 8 {
                        let db_id = u64::from_le_bytes(
                            data_bytes[..8].try_into().ok()?,
                        );
                        // C-6: propagate txn_id from the LN entry so the
                        // mapping-tree undo pass can remove NameLNs whose
                        // creating transaction aborted.
                        let txn_id = r.txn_id.map(|id| id.unsigned_abs());
                        Some(LogEntry::NameLn(noxu_recovery::NameLnRecord {
                            name,
                            db_id,
                            is_deleted: false,
                            txn_id,
                        }))
                    } else {
                        None
                    }
                } else {
                    // Deletion: data is None.
                    // db_id 0 is a sentinel (unused for deletions).
                    Some(LogEntry::NameLn(noxu_recovery::NameLnRecord {
                        name,
                        db_id: 0,
                        is_deleted: true,
                        txn_id: None,
                    }))
                }
            }

            // HA rollback markers (REP-1) ─────────────────────────────
            LogEntryType::RollbackStart => {
                let e = noxu_log::entry::RollbackStartEntry::read_from_log(
                    &payload,
                )
                .ok()?;
                Some(LogEntry::RollbackStart(
                    noxu_recovery::RollbackStartRecord {
                        matchpoint_vlsn: e.matchpoint_vlsn,
                        matchpoint_lsn: e.matchpoint_lsn,
                        lsn: NULL_LSN, // Filled in by caller.
                        active_txn_ids: e.active_txn_ids,
                    },
                ))
            }
            LogEntryType::RollbackEnd => {
                let e =
                    noxu_log::entry::RollbackEndEntry::read_from_log(&payload)
                        .ok()?;
                Some(LogEntry::RollbackEnd(noxu_recovery::RollbackEndRecord {
                    matchpoint_lsn: e.matchpoint_lsn,
                    lsn: NULL_LSN, // Filled in by caller.
                }))
            }

            // Everything else (FileHeader, Trace, MapLN, etc.) ─
            _ => None,
        }
    }

    /// Parse a log entry from a `Bytes` file buffer at `offset`.
    ///
    /// Extracts the payload as a `Bytes::slice` of `file_bytes` — zero-copy
    /// for both the mmap and heap-owned fallback paths.
    ///
    /// Returns `(entry_size, Option<LogEntry>)` or `None` if the bytes are
    /// zero-filled (past the last write) or truncated.
    fn parse_entry_from_bytes(
        file_bytes: &Bytes,
        offset: usize,
    ) -> Option<(usize, Option<LogEntry>)> {
        let data: &[u8] = file_bytes;
        if offset + MIN_HEADER_SIZE > data.len() {
            return None;
        }
        let hdr = &data[offset..offset + MIN_HEADER_SIZE];

        // Zero-filled region past the last written byte.
        if hdr[4] == 0 {
            return None;
        }

        let entry_type_num = hdr[4];
        let flags = hdr[5];
        let item_size =
            u32::from_le_bytes([hdr[10], hdr[11], hdr[12], hdr[13]]) as usize;

        if item_size > MAX_SANE_ITEM_SIZE {
            return None;
        }

        let vlsn_present = (flags & 0x08) != 0 || (flags & 0x20) != 0;
        let header_size =
            if vlsn_present { MAX_HEADER_SIZE } else { MIN_HEADER_SIZE };
        let entry_size = header_size + item_size;

        if offset + entry_size > data.len() {
            return None; // Truncated write at end of log.
        }

        // Extract VLSN from the header extension (8-byte LE i64) so the
        // recovery redo pass can verify VLSN ordering on replicated
        // entries (security review LOG-6).  A negative or zero raw value
        // with vlsn_present set is a contradiction; treat as no-VLSN
        // rather than poisoning the scan.
        let vlsn_opt = if vlsn_present {
            let raw = i64::from_le_bytes(
                data[offset + MIN_HEADER_SIZE..offset + MAX_HEADER_SIZE]
                    .try_into()
                    .ok()?,
            );
            if raw > 0 { Some(raw as u64) } else { None }
        } else {
            None
        };

        // slice() is O(1): just bumps the Bytes Arc refcount.
        let payload =
            file_bytes.slice(offset + header_size..offset + entry_size);

        // C-3 (2026 audit F-3.5 / F-9.1): verify CRC32 before
        // returning a parsed entry.  The non-recovery reader (file_reader.rs)
        // already validates CRCs; skipping it here was an asymmetric gap that
        // silently injected corrupted entries into the recovered B-tree.
        //
        // Layout: checksum (4 bytes LE) at header[0..4] covers bytes [4..entry_size].
        // A stored checksum of 0 means "not computed" (synthetic test entries);
        // skip validation in that case, matching file_reader.rs behaviour.
        let stored_checksum =
            u32::from_le_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
        if stored_checksum != 0 {
            let entry_bytes = &data[offset..offset + entry_size];
            // REP-1 STEP 4 (JE LogEntryHeader.turnOffInvisible): cloak the
            // invisible bit (flags 0x10) before checksumming so an entry
            // flipped invisible in-place by recovery rollback still validates.
            let computed = if entry_bytes[5] & 0x10 != 0 {
                let mut cloaked = entry_bytes.to_vec();
                cloaked[5] &= !0x10u8;
                ChecksumValidator::compute_range(
                    &cloaked,
                    CHECKSUM_BYTES,
                    entry_size - CHECKSUM_BYTES,
                )
            } else {
                ChecksumValidator::compute_range(
                    entry_bytes,
                    CHECKSUM_BYTES,
                    entry_size - CHECKSUM_BYTES,
                )
            };
            if computed != stored_checksum {
                // Return a sentinel that the scanner loop interprets as a
                // hard parse failure; the caller in scan_files_forward / scan
                // treats this as end-of-valid-log for recovery purposes, which
                // is the safe conservative action (better than replaying
                // corrupted data into the B-tree).
                return None;
            }
        }

        let log_entry =
            Self::parse_payload(entry_type_num, payload, vlsn_opt, flags);

        Some((entry_size, log_entry))
    }

    /// [#18307] Scan FORWARD from a mid-file corruption point looking for a
    /// `TxnCommit` (entry type 30). Faithful to JE
    /// `LastFileReader.findCommittedTxn` (LastFileReader.java:394).
    ///
    /// `corrupt_offset` is the byte offset of the entry whose checksum failed
    /// (or which was otherwise unparseable). We first SKIP it by its claimed
    /// header item_size (JE case 2), then keep reading:
    /// - a CRC-valid `TxnCommit` → return `Some(lsn)` (JE case 5, HALT),
    /// - a second checksum/parse failure → `None` (JE case 2/3, truncate),
    /// - clean EOF with no commit → `None` (JE case 4, truncate).
    fn find_committed_txn_after(
        file_bytes: &Bytes,
        corrupt_offset: usize,
        file_num: u32,
    ) -> Option<Lsn> {
        let data: &[u8] = file_bytes;

        // JE: skipData(currentEntryHeader.getItemSize()). Read the corrupt
        // entry's header to learn its claimed size and step over it. If the
        // header itself is unreadable, there is nothing to scan → truncate.
        if corrupt_offset + MIN_HEADER_SIZE > data.len() {
            return None;
        }
        let chdr = &data[corrupt_offset..corrupt_offset + MIN_HEADER_SIZE];
        let cflags = chdr[5];
        let citem = u32::from_le_bytes([chdr[10], chdr[11], chdr[12], chdr[13]])
            as usize;
        if citem > MAX_SANE_ITEM_SIZE {
            return None;
        }
        let cvlsn = (cflags & 0x08) != 0 || (cflags & 0x20) != 0;
        let chdr_size = if cvlsn { MAX_HEADER_SIZE } else { MIN_HEADER_SIZE };
        let mut offset = corrupt_offset + chdr_size + citem;

        // 30 == LogEntryType::TxnCommit.
        const LOG_TXN_COMMIT: u8 = 30;

        while offset < data.len() {
            if offset + MIN_HEADER_SIZE > data.len() {
                return None;
            }
            let hdr = &data[offset..offset + MIN_HEADER_SIZE];
            // Zero-fill tail or unparseable type byte → case 4, truncate.
            if hdr[4] == 0 {
                return None;
            }
            let entry_type_num = hdr[4];
            let flags = hdr[5];
            let item_size =
                u32::from_le_bytes([hdr[10], hdr[11], hdr[12], hdr[13]])
                    as usize;
            if item_size > MAX_SANE_ITEM_SIZE {
                return None;
            }
            let vlsn_present = (flags & 0x08) != 0 || (flags & 0x20) != 0;
            let header_size =
                if vlsn_present { MAX_HEADER_SIZE } else { MIN_HEADER_SIZE };
            let entry_size = header_size + item_size;
            if offset + entry_size > data.len() {
                return None;
            }

            // Validate CRC; a second checksum failure is JE case 2/3 →
            // truncate at the first corruption.
            let stored = u32::from_le_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
            if stored != 0 {
                let entry_bytes = &data[offset..offset + entry_size];
                // REP-1 STEP 4: cloak the invisible bit before checksumming.
                let computed = if entry_bytes[5] & 0x10 != 0 {
                    let mut cloaked = entry_bytes.to_vec();
                    cloaked[5] &= !0x10u8;
                    ChecksumValidator::compute_range(
                        &cloaked,
                        CHECKSUM_BYTES,
                        entry_size - CHECKSUM_BYTES,
                    )
                } else {
                    ChecksumValidator::compute_range(
                        entry_bytes,
                        CHECKSUM_BYTES,
                        entry_size - CHECKSUM_BYTES,
                    )
                };
                if computed != stored {
                    return None;
                }
            }

            // Case 5: a CRC-valid committed txn after the corruption.
            if entry_type_num == LOG_TXN_COMMIT {
                return Some(Lsn::new(file_num, offset as u32));
            }

            offset += entry_size;
        }

        // Case 4: reached EOF without finding a commit.
        None
    }

    /// Scan forward through all log files collecting `PositionedEntry` items
    /// whose LSN falls in `[start_lsn, end_lsn)`.
    ///
    /// Each file is read into memory with a single `pread64` call before any
    /// entry parsing — eliminating 2 syscalls per entry (one for the header,
    /// one for the full entry) down to 1 syscall per file.  For a 100K-record
    /// log (~17 MB on one file) this reduces syscall count from ~200K to 1.
    ///
    /// `NULL_LSN` for `start_lsn` means "from the beginning of the first file".
    /// `NULL_LSN` for `end_lsn`   means "to the end of the log".
    fn scan_files_forward(
        &self,
        start_lsn: Lsn,
        end_lsn: Lsn,
    ) -> Vec<PositionedEntry> {
        let mut results = Vec::new();
        self.scan_files_forward_cb(start_lsn, end_lsn, &mut |pe| {
            results.push(pe)
        });
        results
    }

    /// Streaming core of the forward scan: walk every log file from
    /// `start_lsn` up to (but not including) `end_lsn`, invoking `cb` for
    /// each parsed entry **inline** — no intermediate `Vec`.
    ///
    /// Both `scan_files_forward` (which collects) and the `scan_forward_fn`
    /// trait override (which streams) drive off this single loop, so the
    /// parse/LSN-stamp/bounds logic stays in one place.  This mirrors JE's
    /// `LNFileReader`/`INFileReader` read loop (`FileReader.readNextEntry`):
    /// the recovery passes pull one entry at a time through a callback rather
    /// than materialising the whole bounded range.
    fn scan_files_forward_cb(
        &self,
        start_lsn: Lsn,
        end_lsn: Lsn,
        cb: &mut dyn FnMut(PositionedEntry),
    ) {
        let file_nums = match self.file_manager.list_file_numbers() {
            Ok(nums) => nums,
            Err(_) => return,
        };
        if file_nums.is_empty() {
            return;
        }

        let start_file = if start_lsn == NULL_LSN {
            *file_nums.first().unwrap()
        } else {
            start_lsn.file_number()
        };

        for &file_num in file_nums.iter().filter(|&&n| n >= start_file) {
            let file_len = match self.file_manager.get_file_length(file_num) {
                Ok(len) => len,
                Err(_) => continue,
            };
            if file_len == 0 {
                continue;
            }

            // Prefer mmap (zero heap allocation, sequential OS read-ahead);
            // fall back to a single pread64 into a heap buffer if mmap fails.
            let file_bytes = match self.load_file_bytes(file_num, file_len) {
                Some(b) => b,
                None => continue,
            };

            // Determine where in this file to start parsing.
            // Always start at the file's header size minimum: a start offset
            // of 0 would land inside the file header and break parsing.
            // For v2 files the header is 32 bytes; v3 files 36 bytes.
            let file_header_size = self
                .file_manager
                .file_header_size_for(file_num)
                .unwrap_or(FILE_HEADER_SIZE);
            let file_start_offset: usize = if start_lsn != NULL_LSN
                && file_num == start_lsn.file_number()
            {
                (start_lsn.file_offset() as usize).max(file_header_size)
            } else {
                file_header_size
            };

            let mut offset = file_start_offset;

            while offset < file_bytes.len() {
                // Enforce end_lsn upper bound.
                if end_lsn != NULL_LSN {
                    let cur_lsn = Lsn::new(file_num, offset as u32);
                    if cur_lsn >= end_lsn {
                        return;
                    }
                }

                match Self::parse_entry_from_bytes(&file_bytes, offset) {
                    None => break, // Zero-filled or truncated — end of data.
                    Some((entry_size, parsed)) => {
                        let entry_lsn = Lsn::new(file_num, offset as u32);

                        if let Some(mut log_entry) = parsed {
                            match &mut log_entry {
                                LogEntry::TxnCommit(r) => {
                                    r.lsn = entry_lsn;
                                }
                                LogEntry::CkptStart(r) => {
                                    r.lsn = entry_lsn;
                                }
                                LogEntry::TxnPrepare(r) => {
                                    r.lsn = entry_lsn;
                                }
                                LogEntry::RollbackStart(r) => {
                                    r.lsn = entry_lsn;
                                }
                                LogEntry::RollbackEnd(r) => {
                                    r.lsn = entry_lsn;
                                }
                                _ => {}
                            }
                            cb(PositionedEntry::new(entry_lsn, log_entry));
                        }

                        offset += entry_size;
                    }
                }
            }
        }
    }
}

impl LogScanner for FileManagerLogScanner {
    /// Find the true end of the log by scanning through all log files.
    ///
    /// Also calls `FileManager::set_last_position()` to restore the
    /// manager's LSN state so subsequent writes continue from the correct
    /// position rather than overwriting existing data.
    ///
    /// Returns `(last_used_lsn, next_available_lsn)`.
    fn find_end_of_log(&mut self) -> (Lsn, Lsn) {
        let file_nums = match self.file_manager.list_file_numbers() {
            Ok(nums) => nums,
            Err(_) => return (NULL_LSN, NULL_LSN),
        };

        if file_nums.is_empty() {
            // Fresh environment: no log files written yet.
            return (NULL_LSN, NULL_LSN);
        }

        // Scan files from the last one backward to find the last valid entry.
        // We scan each file fully in forward order and keep the last valid
        // entry we see.  Scanning in reverse-file order lets us stop as soon
        // as we find a non-empty file.
        let mut last_used_lsn = NULL_LSN;
        let mut next_available_lsn = NULL_LSN;

        'outer: for &file_num in file_nums.iter().rev() {
            let file_len = match self.file_manager.get_file_length(file_num) {
                Ok(l) => l,
                Err(_) => continue,
            };
            if file_len == 0 {
                continue;
            }

            // Prefer mmap; fall back to pread64 on failure.
            let file_bytes = match self.load_file_bytes(file_num, file_len) {
                Some(b) => b,
                None => continue,
            };

            let mut offset = self
                .file_manager
                .file_header_size_for(file_num)
                .unwrap_or(FILE_HEADER_SIZE);
            let mut last_valid_offset: Option<usize> = None;
            let mut last_entry_size = 0usize;

            while offset < file_bytes.len() {
                match Self::parse_entry_from_bytes(&file_bytes, offset) {
                    None => break,
                    Some((entry_size, _)) => {
                        last_valid_offset = Some(offset);
                        last_entry_size = entry_size;
                        offset += entry_size;
                    }
                }
            }

            // L-14 [#18307]: the scan stopped at `offset` because
            // parse_entry_from_bytes returned None — either a clean zero-fill
            // tail (benign) or a CHECKSUM failure / corruption mid-file. The
            // common case is a benign torn-tail write, and we truncate-and-
            // continue below. BUT if `haltOnCommitAfterChecksumException` is
            // set, scan FORWARD past the corruption for a TxnCommit; if one is
            // found we must REFUSE to silently truncate (real media
            // corruption with committed data beyond it). We record the LSNs
            // so recovery surfaces EnvironmentFailureReason::FoundCommittedTxn.
            //
            // Faithful to JE LastFileReader.readNextEntry/findCommittedTxn
            // (LastFileReader.java:313/394).
            if self.halt_on_commit_after_checksum
                && offset < file_bytes.len()
                && self.found_committed_txn.is_none()
                && let Some(commit_lsn) = Self::find_committed_txn_after(
                    &file_bytes,
                    offset,
                    file_num,
                )
            {
                let corrupt_lsn = Lsn::new(file_num, offset as u32);
                log::warn!(
                    "recovery: committed txn at LSN {commit_lsn} found AFTER \
                     corruption at LSN {corrupt_lsn}; halting recovery \
                     (haltOnCommitAfterChecksumException enabled)"
                );
                self.found_committed_txn = Some((corrupt_lsn, commit_lsn));
            }

            if let Some(valid_offset) = last_valid_offset {
                let end_offset = valid_offset + last_entry_size;
                last_used_lsn = Lsn::new(file_num, valid_offset as u32);

                // next_available is the byte immediately after the last entry.
                // If we're at the end of this file, the next write goes to
                // the start of the next file.
                next_available_lsn = if end_offset >= file_bytes.len()
                    && file_nums.last().copied() != Some(file_num)
                {
                    let next_file = file_num + 1;
                    // Use next file's actual header size for the first-entry
                    // offset (next_file always exists here since file_num is
                    // not the last file in the list).
                    let next_header_size = self
                        .file_manager
                        .file_header_size_for(next_file)
                        .unwrap_or(FILE_HEADER_SIZE);
                    Lsn::new(next_file, next_header_size as u32)
                } else {
                    Lsn::new(file_num, end_offset as u32)
                };

                // Restore the FileManager's LSN state so subsequent log()
                // calls write after the last valid entry rather than
                // overwriting it at offset FILE_HEADER_SIZE.
                self.file_manager
                    .set_last_position(next_available_lsn, last_used_lsn);

                // F-1 (JE RecoveryManager.setEndOfFile ->
                // FileManager.truncateLog): physically truncate the torn
                // trailing bytes after the last valid entry, and delete any
                // higher-numbered orphan files, so a half-written entry cannot
                // be misread on a later scan and no log-entry gap remains.
                // Only when there is something to truncate, and only R/W.
                //
                // L-14: do NOT truncate if a committed txn was found past the
                // corruption point — destroying it would lose durable data.
                // Recovery halts on found_committed_txn instead.
                let has_torn_tail = end_offset < file_bytes.len();
                let has_orphan_files =
                    file_nums.last().copied() != Some(file_num);
                if self.found_committed_txn.is_none()
                    && (has_torn_tail || has_orphan_files)
                    && !self.file_manager.is_read_only()
                    && let Err(e) = self
                        .file_manager
                        .truncate_log(file_num, end_offset as u64)
                {
                    log::warn!(
                        "recovery: truncate_log({}, {}) failed: {}",
                        file_num,
                        end_offset,
                        e
                    );
                }

                break 'outer;
            }
            // This file had no valid entries; check the previous file.
        }

        (last_used_lsn, next_available_lsn)
    }

    fn scan_forward(
        &self,
        start_lsn: Lsn,
        end_lsn: Lsn,
    ) -> Vec<PositionedEntry> {
        self.scan_files_forward(start_lsn, end_lsn)
    }

    /// Streaming forward scan: parse entries from the mmap'd/read file bytes
    /// and invoke `cb` per entry, never building the intermediate `Vec`.
    ///
    /// This is the recovery-analysis hot path (JE `LNFileReader`/`INFileReader`
    /// read loop): `RecoveryManager.run_analysis` drives a single forward pass
    /// through this callback, so the bounded scan range is no longer
    /// materialised into a `Vec<PositionedEntry>` (with cloned `LnRecord` /
    /// `Bytes`) only to be iterated once.
    fn scan_forward_fn(
        &self,
        start_lsn: Lsn,
        end_lsn: Lsn,
        cb: &mut dyn FnMut(PositionedEntry),
    ) {
        self.scan_files_forward_cb(start_lsn, end_lsn, cb);
    }

    fn scan_backward(
        &self,
        start_lsn: Lsn,
        stop_lsn: Lsn,
    ) -> Vec<PositionedEntry> {
        // scan_files_forward returns entries in ascending LSN order (files in
        // numeric order, offsets in forward order within each file).  Reversing
        // the resulting Vec is O(N) vs the previous sort_by() which was
        // O(N log N).
        let mut entries = self.scan_files_forward(stop_lsn, NULL_LSN);

        if start_lsn != NULL_LSN {
            entries.retain(|e| e.lsn <= start_lsn);
        }

        entries.reverse();
        entries
    }

    /// Read the single log entry at exactly `target_lsn` by scanning the
    /// appropriate log file.
    ///
    /// Used during the undo phase to fetch the before-image of a
    /// disk-resident LN at its `abort_lsn`.
    ///
    /// → `fetchTarget(...)`.
    fn read_at_lsn(&self, target_lsn: Lsn) -> Option<LogEntry> {
        if target_lsn == NULL_LSN {
            return None;
        }
        // scan_files_forward with end_lsn = next offset after target is
        // the cheapest way to read exactly one entry.
        let end_lsn =
            Lsn::new(target_lsn.file_number(), target_lsn.file_offset() + 1);
        let entries = self.scan_files_forward(target_lsn, end_lsn);
        entries.into_iter().find(|e| e.lsn == target_lsn).map(|e| e.entry)
    }

    /// Bounded backward search for the most recent `CkptEnd` (Finding 5, JE
    /// `CheckpointFileReader` reads backward from the end to the last
    /// checkpoint). Walks log files newest-first; the first file that contains
    /// any `CkptEnd` yields its LAST `CkptEnd`, and we stop — never
    /// materialising the whole log (the prior recovery path did a full forward
    /// scan just to find this).
    fn find_last_checkpoint_entry(&self) -> Option<PositionedEntry> {
        let mut file_nums = self.file_manager.list_file_numbers().ok()?;
        // Newest file first.
        file_nums.sort_unstable_by(|a, b| b.cmp(a));
        for file_num in file_nums {
            let file_len = self.file_manager.get_file_length(file_num).ok()?;
            if file_len == 0 {
                continue;
            }
            let file_bytes = match self.load_file_bytes(file_num, file_len) {
                Some(b) => b,
                None => continue,
            };
            let file_header_size = self
                .file_manager
                .file_header_size_for(file_num)
                .unwrap_or(FILE_HEADER_SIZE);
            // Scan this file forward, remembering the last CkptEnd seen.
            let mut offset = file_header_size;
            let mut last_ckpt: Option<PositionedEntry> = None;
            while offset < file_bytes.len() {
                match Self::parse_entry_from_bytes(&file_bytes, offset) {
                    None => break, // torn / end of valid data in this file
                    Some((entry_size, maybe_entry)) => {
                        if let Some(entry) = maybe_entry
                            && matches!(entry, LogEntry::CkptEnd(_))
                        {
                            last_ckpt = Some(PositionedEntry::new(
                                Lsn::new(file_num, offset as u32),
                                entry,
                            ));
                        }
                        offset += entry_size;
                    }
                }
            }
            // If this (newer-or-equal) file had a CkptEnd, it is the most
            // recent — return it and stop scanning older files.
            if last_ckpt.is_some() {
                return last_ckpt;
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use noxu_log::entry::TxnEndEntry;
    use noxu_log::{LogEntryType, LogManager, Provisional};
    use noxu_util::vlsn::NULL_VLSN;
    use tempfile::TempDir;

    fn make_manager(
        dir: &std::path::Path,
    ) -> (Arc<FileManager>, Arc<LogManager>) {
        let fm = Arc::new(
            FileManager::new(dir, false, 64 * 1024 * 1024, 100).unwrap(),
        );
        let lm =
            Arc::new(LogManager::new(Arc::clone(&fm), 3, 1024 * 1024, 65536));
        (fm, lm)
    }

    #[test]
    fn test_find_end_of_log_empty() {
        let dir = TempDir::new().unwrap();
        let (fm, _lm) = make_manager(dir.path());
        let mut scanner = FileManagerLogScanner::new(Arc::clone(&fm));
        let (last, next) = scanner.find_end_of_log();
        assert_eq!(last, NULL_LSN, "empty log: last_used should be NULL");
        assert_eq!(next, NULL_LSN, "empty log: next_available should be NULL");
    }

    #[test]
    fn test_find_end_of_log_after_write() {
        let dir = TempDir::new().unwrap();
        let (fm, lm) = make_manager(dir.path());

        // Write a commit entry.
        let entry = TxnEndEntry::new_commit(42, NULL_LSN, 0, 0, NULL_VLSN);
        let mut buf = BytesMut::with_capacity(entry.log_size());
        entry.write_to_log(&mut buf);
        lm.log(LogEntryType::TxnCommit, &buf, Provisional::No, true, false)
            .unwrap();
        lm.flush_sync().unwrap();
        drop(lm);

        // Re-open scanner and find end.
        let mut scanner = FileManagerLogScanner::new(Arc::clone(&fm));
        let (last, next) = scanner.find_end_of_log();
        assert_ne!(last, NULL_LSN, "after write: last_used should be non-null");
        assert!(next > last, "next_available must be after last_used");
    }

    #[test]
    fn test_find_end_of_log_physically_truncates_torn_tail() {
        // F-1 regression (JE RecoveryManager.setEndOfFile -> truncateLog): a
        // torn / half-written entry at the tail must be PHYSICALLY removed,
        // not merely skipped, so it cannot be misread on a later scan.
        let dir = TempDir::new().unwrap();
        let (fm, lm) = make_manager(dir.path());

        // Write one valid commit and sync.
        let entry = TxnEndEntry::new_commit(7, NULL_LSN, 0, 0, NULL_VLSN);
        let mut buf = BytesMut::with_capacity(entry.log_size());
        entry.write_to_log(&mut buf);
        lm.log(LogEntryType::TxnCommit, &buf, Provisional::No, true, false)
            .unwrap();
        lm.flush_sync().unwrap();
        let file_num = 0u32;
        drop(lm);

        // Determine the valid file length, then append garbage (a torn tail).
        let valid_len = fm.get_file_length(file_num).unwrap();
        {
            use std::io::Write;
            let path = dir.path().join(format!("{:08x}.ndb", file_num));
            let mut f =
                std::fs::OpenOptions::new().append(true).open(&path).unwrap();
            // 64 bytes of non-zero garbage that does not parse as a valid entry.
            f.write_all(&[0xABu8; 64]).unwrap();
            f.sync_all().unwrap();
        }
        let torn_len = fm.get_file_length(file_num).unwrap();
        assert!(torn_len > valid_len, "garbage must have grown the file");

        // Recover: find_end_of_log must physically truncate the torn tail.
        let mut scanner = FileManagerLogScanner::new(Arc::clone(&fm));
        let (_last, next) = scanner.find_end_of_log();

        let after_len = fm.get_file_length(file_num).unwrap();
        assert_eq!(
            after_len, valid_len,
            "F-1: torn tail must be physically truncated to the recovery point              (was {}, valid {}, after {})",
            torn_len, valid_len, after_len
        );
        assert_eq!(
            next,
            Lsn::new(file_num, valid_len as u32),
            "next_available must be at the truncation point"
        );
    }

    #[test]
    fn test_scan_forward_finds_commit() {
        let dir = TempDir::new().unwrap();
        let (fm, lm) = make_manager(dir.path());

        // Write a commit for txn 99.
        let entry = TxnEndEntry::new_commit(99, NULL_LSN, 0, 0, NULL_VLSN);
        let mut buf = BytesMut::with_capacity(entry.log_size());
        entry.write_to_log(&mut buf);
        lm.log(LogEntryType::TxnCommit, &buf, Provisional::No, true, false)
            .unwrap();
        lm.flush_sync().unwrap();
        drop(lm);

        let mut scanner = FileManagerLogScanner::new(Arc::clone(&fm));
        let (_, next) = scanner.find_end_of_log();

        let entries = scanner.scan_forward(NULL_LSN, next);
        assert!(!entries.is_empty(), "should find at least one entry");

        let commit = entries.iter().find(
            |e| matches!(e.entry, LogEntry::TxnCommit(ref r) if r.txn_id == 99),
        );
        assert!(commit.is_some(), "should find TxnCommit for txn 99");
    }

    #[test]
    fn test_scan_backward_reverses_order() {
        let dir = TempDir::new().unwrap();
        let (fm, lm) = make_manager(dir.path());

        // Write two commits.
        for txn_id in [10i64, 20i64] {
            let e = TxnEndEntry::new_commit(txn_id, NULL_LSN, 0, 0, NULL_VLSN);
            let mut buf = BytesMut::with_capacity(e.log_size());
            e.write_to_log(&mut buf);
            lm.log(LogEntryType::TxnCommit, &buf, Provisional::No, true, false)
                .unwrap();
        }
        lm.flush_sync().unwrap();
        drop(lm);

        let mut scanner = FileManagerLogScanner::new(Arc::clone(&fm));
        let (last, _) = scanner.find_end_of_log();

        let entries = scanner.scan_backward(last, NULL_LSN);
        assert!(entries.len() >= 2, "should find both commits");

        // Entries should be in descending LSN order.
        for w in entries.windows(2) {
            assert!(w[0].lsn >= w[1].lsn, "backward scan must be descending");
        }
    }

    #[test]
    fn test_find_end_restores_file_manager_position() {
        let dir = TempDir::new().unwrap();
        let (fm, lm) = make_manager(dir.path());

        let e = TxnEndEntry::new_commit(7, NULL_LSN, 0, 0, NULL_VLSN);
        let mut buf = BytesMut::with_capacity(e.log_size());
        e.write_to_log(&mut buf);
        lm.log(LogEntryType::TxnCommit, &buf, Provisional::No, true, false)
            .unwrap();
        lm.flush_sync().unwrap();
        drop(lm);

        let mut scanner = FileManagerLogScanner::new(Arc::clone(&fm));
        let (last, next) = scanner.find_end_of_log();

        // After find_end_of_log, the file manager should reflect those LSNs.
        assert_eq!(fm.get_last_used_lsn(), last);
        assert_eq!(fm.get_next_available_lsn(), next);
    }

    /// C-3 regression: the recovery scanner must reject entries with a bad
    /// CRC32.  Before this fix, `parse_entry_from_bytes` skipped checksum
    /// validation and silently loaded corrupted data into the B-tree.
    ///
    /// The test writes a real log entry (with a valid CRC), then overwrites
    /// one byte in its payload on disk and verifies the scanner skips the
    /// corrupted entry rather than returning it.
    #[test]
    fn test_recovery_scanner_rejects_corrupted_crc() {
        use noxu_log::entry_header::MIN_HEADER_SIZE;
        use std::io::{Seek, SeekFrom, Write as _};

        let dir = TempDir::new().unwrap();
        let (fm, lm) = make_manager(dir.path());

        // Write one commit entry with a real CRC.
        let entry = TxnEndEntry::new_commit(55, NULL_LSN, 0, 0, NULL_VLSN);
        let mut buf = BytesMut::with_capacity(entry.log_size());
        entry.write_to_log(&mut buf);
        let commit_lsn = lm
            .log(LogEntryType::TxnCommit, &buf, Provisional::No, true, false)
            .unwrap();
        lm.flush_sync().unwrap();
        let fm_for_path = Arc::clone(&fm);
        drop(lm);

        // Corrupt one byte in the entry payload (past the header).
        let file_name = format!("{:08x}.ndb", commit_lsn.file_number());
        let file_path = dir.path().join(&file_name);
        let mut f =
            std::fs::OpenOptions::new().write(true).open(&file_path).unwrap();
        let payload_byte_offset =
            commit_lsn.file_offset() as u64 + MIN_HEADER_SIZE as u64 + 1;
        f.seek(SeekFrom::Start(payload_byte_offset)).unwrap();
        f.write_all(&[0xDE]).unwrap(); // flip a byte
        f.sync_all().unwrap();
        drop(f);
        drop(fm_for_path);

        // Scanner must not return the corrupted entry.
        let scanner = FileManagerLogScanner::new(Arc::clone(&fm));
        let entries = scanner.scan_forward(NULL_LSN, NULL_LSN);
        let found = entries.iter().any(
            |e| matches!(e.entry, LogEntry::TxnCommit(ref r) if r.txn_id == 55),
        );
        assert!(
            !found,
            "corrupted entry must not appear in scan results (C-3 regression)"
        );
    }

    // ------------------------------------------------------------------
    // L-14 [#18307]: findCommittedTxn — refuse to silently truncate when a
    // committed txn exists AFTER a mid-file corruption point.
    // Faithful to JE LastFileReader.readNextEntry/findCommittedTxn
    // (LastFileReader.java:313/394).
    // ------------------------------------------------------------------

    /// Helper: write `n` commit entries, return their LSNs.
    fn write_commits(lm: &LogManager, txn_ids: &[i64]) -> Vec<Lsn> {
        let mut lsns = Vec::new();
        for &txn_id in txn_ids {
            let e = TxnEndEntry::new_commit(txn_id, NULL_LSN, 0, 0, NULL_VLSN);
            let mut buf = BytesMut::with_capacity(e.log_size());
            e.write_to_log(&mut buf);
            let lsn = lm
                .log(
                    LogEntryType::TxnCommit,
                    &buf,
                    Provisional::No,
                    true,
                    false,
                )
                .unwrap();
            lsns.push(lsn);
        }
        lm.flush_sync().unwrap();
        lsns
    }

    /// Corrupt one payload byte of the entry at `lsn` on disk.
    fn corrupt_entry_payload(dir: &std::path::Path, lsn: Lsn) {
        use noxu_log::entry_header::MIN_HEADER_SIZE;
        use std::io::{Seek, SeekFrom, Write as _};
        let path = dir.join(format!("{:08x}.ndb", lsn.file_number()));
        let mut f =
            std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        let off = lsn.file_offset() as u64 + MIN_HEADER_SIZE as u64 + 1;
        f.seek(SeekFrom::Start(off)).unwrap();
        f.write_all(&[0xDE]).unwrap();
        f.sync_all().unwrap();
    }

    /// Layout: [commit][CORRUPT commit][commit]. With halt_on_commit ENABLED,
    /// find_end_of_log must scan forward past the corruption, find the trailing
    /// committed txn, record found_committed_txn, and NOT physically truncate.
    ///
    /// FAILS pre-fix (silently truncates; found_committed_txn is None).
    #[test]
    fn test_l14_halts_on_committed_txn_after_corruption() {
        let dir = TempDir::new().unwrap();
        let (fm, lm) = make_manager(dir.path());
        let lsns = write_commits(&lm, &[1, 2, 3]);
        drop(lm);

        let valid_len_before = fm.get_file_length(0).unwrap();

        // Corrupt the MIDDLE entry (committed txns follow it).
        corrupt_entry_payload(dir.path(), lsns[1]);

        let mut scanner = FileManagerLogScanner::new(Arc::clone(&fm))
            .with_halt_on_commit(true);
        let _ = scanner.find_end_of_log();

        let found = scanner.take_found_committed_txn();
        assert!(
            found.is_some(),
            "L-14: a committed txn after mid-file corruption must be detected"
        );
        let (corrupt_lsn, commit_lsn) = found.unwrap();
        assert_eq!(corrupt_lsn, lsns[1], "corruption point is the 2nd entry");
        assert_eq!(commit_lsn, lsns[2], "committed txn is the 3rd entry");

        // Must NOT have truncated the file (would lose the committed data).
        assert_eq!(
            fm.get_file_length(0).unwrap(),
            valid_len_before,
            "L-14: must not truncate when a committed txn follows corruption"
        );
    }

    /// With halt_on_commit DISABLED (default), the same corruption keeps the
    /// common-case truncate-and-continue behavior: no detection, file is
    /// truncated at the first valid entry.
    #[test]
    fn test_l14_disabled_truncates_as_before() {
        let dir = TempDir::new().unwrap();
        let (fm, lm) = make_manager(dir.path());
        let lsns = write_commits(&lm, &[1, 2, 3]);
        drop(lm);

        corrupt_entry_payload(dir.path(), lsns[1]);

        // Default scanner (halt disabled).
        let mut scanner = FileManagerLogScanner::new(Arc::clone(&fm));
        let _ = scanner.find_end_of_log();

        assert!(
            scanner.take_found_committed_txn().is_none(),
            "halt disabled: no found_committed_txn"
        );
        // The torn-tail truncate keeps only the first valid entry.
        assert_eq!(
            fm.get_file_length(0).unwrap(),
            lsns[1].file_offset() as u64,
            "halt disabled: file truncated at first corruption"
        );
    }

    /// Common case (JE case 4): corruption at the tail with NO committed txn
    /// after it. Even with halt enabled, truncate-and-continue applies.
    #[test]
    fn test_l14_torn_tail_no_commit_does_not_halt() {
        use std::io::Write;
        let dir = TempDir::new().unwrap();
        let (fm, lm) = make_manager(dir.path());
        let lsns = write_commits(&lm, &[1]);
        drop(lm);
        let valid_len = lsns[0].file_offset() as u64
            + (fm.get_file_length(0).unwrap() - lsns[0].file_offset() as u64);
        // Append non-zero garbage (a torn tail, no commit after it).
        {
            let path = dir.path().join("00000000.ndb");
            let mut f =
                std::fs::OpenOptions::new().append(true).open(&path).unwrap();
            f.write_all(&[0xABu8; 64]).unwrap();
            f.sync_all().unwrap();
        }

        let mut scanner = FileManagerLogScanner::new(Arc::clone(&fm))
            .with_halt_on_commit(true);
        let _ = scanner.find_end_of_log();

        assert!(
            scanner.take_found_committed_txn().is_none(),
            "torn tail with no commit after: must not halt"
        );
        // Garbage is truncated.
        assert_eq!(fm.get_file_length(0).unwrap(), valid_len);
    }
}
