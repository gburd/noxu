//! Production-path IN-redo test: verify that `recover_all` (the path used by
//! `EnvironmentImpl::open`) applies dirty INs to the per-database trees and
//! reports `ins_replayed > 0`.
//!
//! This test exercises `run_redo_all` → `apply_in_redo_to_trees` →
//! `apply_in_redo_to_tree` → `Tree::recover_in_redo` — the exact path that
//! was broken before this fix.  `recover` (single-DB) is also verified as a
//! sanity check that the shared helper works for both callers.
//!
//! Stage 1 acceptance test for the `run_redo_all` fix.
//! JE RecoveryManager.buildINs / recoverIN (RecoveryManager.java ~915-1500).

use hashbrown::HashMap;
use noxu_recovery::{
    CkptEndRecord, CkptStartRecord, InRecord, LogEntry, LogScanner,
    PositionedEntry, RecoveryManager,
};
use noxu_tree::{BinEntry, BinStub, Tree};
use noxu_util::{Lsn, NULL_LSN};

// ─── helpers ─────────────────────────────────────────────────────────────────

fn lsn(file: u32, off: u32) -> Lsn {
    Lsn::new(file, off)
}

/// Serialise a one-entry BIN into the format written by
/// `BinStub::serialize_full()`.
fn make_bin_bytes(
    node_id: u64,
    key: &[u8],
    data: &[u8],
    entry_lsn: Lsn,
) -> Vec<u8> {
    let bin = BinStub {
        node_id,
        level: noxu_tree::BIN_LEVEL,
        entries: vec![BinEntry {
            data: Some(bytes::Bytes::copy_from_slice(data)),
            known_deleted: false,
            dirty: false,
            expiration_time: 0,
        }],
        key_prefix: vec![],
        dirty: false,
        is_delta: false,
        last_full_lsn: NULL_LSN,
        last_delta_lsn: NULL_LSN,
        generation: 0,
        parent: None,
        expiration_in_hours: true,
        cursor_count: 0,
        prohibit_next_delta: false,
        lsn_rep: noxu_tree::tree::LsnRep::from_lsns(&[entry_lsn]),
        keys: noxu_tree::tree::KeyRep::from_keys(vec![key.to_vec()]),
        compact_max_key_length:
            noxu_tree::tree::INKeyRep_DEFAULT_MAX_KEY_LENGTH,
    };
    bin.serialize_full()
}

/// Build an `InRecord` carrying serialised BIN bytes.
fn in_record(db_id: u64, node_id: u64, bytes: Vec<u8>) -> InRecord {
    InRecord {
        db_id,
        node_id,
        level: 0x10001, // BIN_LEVEL
        is_root: true,  // single-BIN tree
        is_delta: false,
        is_provisional: false,
        node_data: Some(bytes),
        prev_full_lsn: NULL_LSN,
    }
}

/// Minimal in-memory scanner that satisfies `LogScanner`.
struct SimpleScanner {
    entries: Vec<PositionedEntry>,
    last: Lsn,
    next: Lsn,
}

impl SimpleScanner {
    fn new() -> Self {
        Self { entries: vec![], last: NULL_LSN, next: NULL_LSN }
    }
    fn push(&mut self, lsn: Lsn, entry: LogEntry) {
        self.last = lsn;
        self.next = Lsn::new(lsn.file_number(), lsn.file_offset() + 1);
        self.entries.push(PositionedEntry::new(lsn, entry));
    }
}

impl LogScanner for SimpleScanner {
    fn find_end_of_log(&mut self) -> (Lsn, Lsn) {
        (self.last, self.next)
    }
    fn scan_forward(&self, start: Lsn, end: Lsn) -> Vec<PositionedEntry> {
        self.entries
            .iter()
            .filter(|pe| pe.lsn >= start && pe.lsn < end)
            .cloned()
            .collect()
    }
    fn scan_backward(&self, start: Lsn, stop: Lsn) -> Vec<PositionedEntry> {
        let mut v: Vec<_> = self
            .entries
            .iter()
            .filter(|pe| pe.lsn <= start && pe.lsn >= stop)
            .cloned()
            .collect();
        v.reverse();
        v
    }
    fn read_at_lsn(&self, lsn: Lsn) -> Option<LogEntry> {
        self.entries.iter().find(|pe| pe.lsn == lsn).map(|pe| pe.entry.clone())
    }
}

// ─── tests ───────────────────────────────────────────────────────────────────

/// `recover_all` (production path) applies a logged BIN to the per-database
/// tree and reports `ins_replayed > 0`.
///
/// Scenario:
///   1. CkptStart
///   2. IN record (BIN for db_id=1, node_id=42, key=b"hello", data=b"world")
///      — simulates the checkpoint flushing the BIN to WAL
///   3. CkptEnd
///
/// After `recover_all`, tree for db_id=1 must contain key b"hello" and
/// `ins_replayed` must be > 0 (confirming `run_redo_all` invoked
/// `recover_in_redo`).
#[test]
fn recover_all_applies_logged_bin_ins_replayed_gt_zero() {
    let bin_lsn = lsn(1, 100);
    let bin_bytes = make_bin_bytes(42, b"hello", b"world", bin_lsn);

    let mut scanner = SimpleScanner::new();
    scanner.push(
        lsn(1, 50),
        LogEntry::CkptStart(CkptStartRecord { id: 1, lsn: lsn(1, 50) }),
    );
    scanner.push(bin_lsn, LogEntry::In(in_record(1, 42, bin_bytes)));
    scanner.push(
        lsn(1, 200),
        LogEntry::CkptEnd(CkptEndRecord {
            id: 1,
            checkpoint_start_lsn: lsn(1, 50),
            first_active_lsn: lsn(1, 50),
            root_lsn: NULL_LSN,
            last_local_node_id: 42,
            last_replicated_node_id: 0,
            last_local_db_id: 1,
            last_replicated_db_id: 0,
            last_local_txn_id: 0,
            last_replicated_txn_id: 0,
            per_db_roots: Vec::new(),
        }),
    );

    let mut mgr = RecoveryManager::new();
    let mut trees: HashMap<u64, Tree> = HashMap::new();
    trees.insert(1, Tree::new(1, 128));

    // recover_all is the production path (EnvironmentImpl::open calls it).
    mgr.recover_all(&mut scanner, &mut trees, false).unwrap();

    let stats = mgr.get_stats();
    assert!(
        stats.ins_replayed > 0,
        "run_redo_all must apply the logged BIN via recover_in_redo \
         (ins_replayed={})",
        stats.ins_replayed
    );

    // The tree must contain the key that was in the BIN.
    let tree = trees.get(&1).expect("db_id=1 tree must exist");
    let slot = tree.search_with_data(b"hello");
    assert!(
        slot.map(|s| s.found).unwrap_or(false),
        "key b\"hello\" must be present in the recovered tree \
         (IN-redo path via run_redo_all)"
    );
}

/// `recover` (single-DB path) also applies a logged BIN via the same shared
/// helper — regression guard to ensure both paths continue to work.
#[test]
fn recover_single_db_applies_logged_bin() {
    let bin_lsn = lsn(1, 100);
    let bin_bytes = make_bin_bytes(99, b"key1", b"val1", bin_lsn);

    let mut scanner = SimpleScanner::new();
    scanner.push(
        lsn(1, 50),
        LogEntry::CkptStart(CkptStartRecord { id: 2, lsn: lsn(1, 50) }),
    );
    scanner.push(bin_lsn, LogEntry::In(in_record(5, 99, bin_bytes)));
    scanner.push(
        lsn(1, 200),
        LogEntry::CkptEnd(CkptEndRecord {
            id: 2,
            checkpoint_start_lsn: lsn(1, 50),
            first_active_lsn: lsn(1, 50),
            root_lsn: NULL_LSN,
            last_local_node_id: 99,
            last_replicated_node_id: 0,
            last_local_db_id: 5,
            last_replicated_db_id: 0,
            last_local_txn_id: 0,
            last_replicated_txn_id: 0,
            per_db_roots: Vec::new(),
        }),
    );

    let mut mgr = RecoveryManager::new();
    let mut tree = Tree::new(5, 128);

    mgr.recover(&mut scanner, Some(&mut tree), false).unwrap();

    let stats = mgr.get_stats();
    assert!(
        stats.ins_replayed > 0,
        "run_redo must apply the logged BIN (ins_replayed={})",
        stats.ins_replayed
    );

    let result = tree.search_with_data(b"key1");
    assert!(
        result.map(|r| r.found).unwrap_or(false),
        "key b\"key1\" must be present in the recovered tree"
    );
}

/// `recover_all` applies INs to MULTIPLE databases in one pass.
/// Both db_id=1 and db_id=2 must get their BINs replayed.
#[test]
fn recover_all_applies_bins_to_multiple_databases() {
    let bin1_lsn = lsn(1, 100);
    let bin2_lsn = lsn(1, 150);
    let bin1 = make_bin_bytes(10, b"db1key", b"db1val", bin1_lsn);
    let bin2 = make_bin_bytes(20, b"db2key", b"db2val", bin2_lsn);

    let mut scanner = SimpleScanner::new();
    scanner.push(
        lsn(1, 50),
        LogEntry::CkptStart(CkptStartRecord { id: 3, lsn: lsn(1, 50) }),
    );
    scanner.push(bin1_lsn, LogEntry::In(in_record(1, 10, bin1)));
    scanner.push(bin2_lsn, LogEntry::In(in_record(2, 20, bin2)));
    scanner.push(
        lsn(1, 300),
        LogEntry::CkptEnd(CkptEndRecord {
            id: 3,
            checkpoint_start_lsn: lsn(1, 50),
            first_active_lsn: lsn(1, 50),
            root_lsn: NULL_LSN,
            last_local_node_id: 20,
            last_replicated_node_id: 0,
            last_local_db_id: 2,
            last_replicated_db_id: 0,
            last_local_txn_id: 0,
            last_replicated_txn_id: 0,
            per_db_roots: Vec::new(),
        }),
    );

    let mut mgr = RecoveryManager::new();
    let mut trees: HashMap<u64, Tree> = HashMap::new();
    trees.insert(1, Tree::new(1, 128));
    trees.insert(2, Tree::new(2, 128));

    mgr.recover_all(&mut scanner, &mut trees, false).unwrap();

    let stats = mgr.get_stats();
    assert!(
        stats.ins_replayed >= 2,
        "both BINs must be replayed (ins_replayed={})",
        stats.ins_replayed
    );

    let t1 = trees.get(&1).unwrap();
    assert!(
        t1.search_with_data(b"db1key").map(|r| r.found).unwrap_or(false),
        "db1key must be in tree 1"
    );
    let t2 = trees.get(&2).unwrap();
    assert!(
        t2.search_with_data(b"db2key").map(|r| r.found).unwrap_or(false),
        "db2key must be in tree 2"
    );
}
