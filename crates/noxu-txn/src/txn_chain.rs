//! Partial transaction rollback for replication sync-up.
//!

use std::collections::HashMap;

use noxu_util::lsn::NULL_LSN;

/// Identifies a slot in a BIN (key + BIN node ID) for deduplication.
///
/// During partial rollback, multiple writes to the same record within a
/// transaction need to be collapsed to just the latest revert state.
///
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CompareSlot {
    /// Node ID of the BIN containing this slot.
    pub bin_node_id: u64,
    /// Key in the slot (used as secondary discriminant).
    pub key: Vec<u8>,
}

impl CompareSlot {
    pub fn new(bin_node_id: u64, key: Vec<u8>) -> Self {
        CompareSlot { bin_node_id, key }
    }
}

/// Revert information for a single log entry in the undo chain.
///
/// Records the state that a record should be reverted to during partial rollback.
///
#[derive(Debug, Clone)]
pub struct RevertInfo {
    /// The LSN to revert to (the before-image LSN).
    pub revert_lsn: u64,

    /// Whether the revert version is a known-deleted record.
    pub revert_kd: bool,

    /// Whether this is a phantom deletion (record existed before txn started).
    pub revert_pd: bool,

    /// Key at the revert version.
    pub revert_key: Option<Vec<u8>>,

    /// Data at the revert version.
    pub revert_data: Option<Vec<u8>>,

    /// VLSN of the revert version.
    pub revert_vlsn: i64,

    /// Expiration time of the revert version.
    pub revert_expiration: i32,
}

impl RevertInfo {
    pub fn new(revert_lsn: u64, revert_kd: bool) -> Self {
        RevertInfo {
            revert_lsn,
            revert_kd,
            revert_pd: false,
            revert_key: None,
            revert_data: None,
            revert_vlsn: -1,
            revert_expiration: 0,
        }
    }
}

/// A backward traversal of a transaction's log chain for partial rollback.
///
/// Used by the replication layer to undo a transaction from its last
/// logged LSN back to a specific rollback point (not all the way to the
/// beginning of the transaction).
///
/// # Algorithm
///
/// 1. Start at `last_logged_lsn` for the Txn.
/// 2. Follow the prev-entry chain (each LN log entry records the previous
///    LSN written by the same Txn).
/// 3. For each entry, record a `RevertInfo` keyed by `CompareSlot`.
/// 4. If the same slot appears multiple times (multiple writes to the same
///    record), keep only the first (latest) `RevertInfo` — that is the
///    before-image needed for partial rollback.
/// 5. Stop when the chain LSN <= `rollback_point`.
///
/// Used by `MasterTxn.rollbackOperations()` in HA.
///
/// 
#[derive(Debug)]
pub struct TxnChain {
    /// Ordered list of revert entries (in log-chain traversal order, newest first).
    reverts: Vec<(CompareSlot, RevertInfo)>,

    /// Dedup map: `CompareSlot` → index into `reverts` so later (older) writes
    /// to the same slot are ignored.
    slot_map: HashMap<CompareSlot, usize>,

    /// The LSN at which the chain traversal stopped.
    rollback_point: u64,

    /// The LSN of the TxnCommit or last entry, if the txn was committed.
    commit_lsn: u64,
}

impl TxnChain {
    /// Creates a new empty `TxnChain`.
    pub fn new(rollback_point: u64) -> Self {
        TxnChain {
            reverts: Vec::new(),
            slot_map: HashMap::new(),
            rollback_point,
            commit_lsn: NULL_LSN.as_u64(),
        }
    }

    /// Adds a revert entry for a BIN slot, if not already recorded.
    ///
    /// On each log entry in the chain, if the slot hasn't been seen yet,
    /// record the `RevertInfo` (the before-image).  If it has been seen, the
    /// earlier (older) write is ignored because the later write's before-image
    /// is what partial rollback needs.
    pub fn add_revert(
        &mut self,
        slot: CompareSlot,
        revert_lsn: u64,
        revert_kd: bool,
    ) {
        if self.slot_map.contains_key(&slot) {
            // Already have a (newer) revert for this slot — ignore older writes.
            return;
        }
        let idx = self.reverts.len();
        let revert = RevertInfo::new(revert_lsn, revert_kd);
        self.slot_map.insert(slot.clone(), idx);
        self.reverts.push((slot, revert));
    }

    /// Adds a revert entry with full `RevertInfo`.
    pub fn add_revert_info(&mut self, slot: CompareSlot, revert: RevertInfo) {
        if self.slot_map.contains_key(&slot) {
            return;
        }
        let idx = self.reverts.len();
        self.slot_map.insert(slot.clone(), idx);
        self.reverts.push((slot, revert));
    }

    /// Sets the commit LSN (if the txn was committed before partial rollback).
    pub fn set_commit_lsn(&mut self, lsn: u64) {
        self.commit_lsn = lsn;
    }

    /// Returns the rollback point LSN.
    pub fn rollback_point(&self) -> u64 {
        self.rollback_point
    }

    /// Returns the commit LSN (NULL_LSN if the txn was not committed).
    pub fn commit_lsn(&self) -> u64 {
        self.commit_lsn
    }

    /// Returns all revert entries in traversal order (newest first).
    pub fn reverts(&self) -> &[(CompareSlot, RevertInfo)] {
        &self.reverts
    }

    /// Returns the revert info for a given slot, if any.
    pub fn get_revert(&self, slot: &CompareSlot) -> Option<&RevertInfo> {
        self.slot_map
            .get(slot)
            .and_then(|&idx| self.reverts.get(idx).map(|(_, r)| r))
    }

    /// Returns the number of recorded revert entries.
    pub fn len(&self) -> usize {
        self.reverts.len()
    }

    /// Returns true if no revert entries have been recorded.
    pub fn is_empty(&self) -> bool {
        self.reverts.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_chain_empty() {
        let chain = TxnChain::new(100);
        assert!(chain.is_empty());
        assert_eq!(chain.rollback_point(), 100);
        assert_eq!(chain.commit_lsn(), NULL_LSN.as_u64());
    }

    #[test]
    fn test_add_revert_basic() {
        let mut chain = TxnChain::new(0);
        let slot = CompareSlot::new(1, b"key1".to_vec());
        chain.add_revert(slot.clone(), 500, false);
        assert_eq!(chain.len(), 1);
        let r = chain.get_revert(&slot).unwrap();
        assert_eq!(r.revert_lsn, 500);
        assert!(!r.revert_kd);
    }

    #[test]
    fn test_add_revert_dedup() {
        let mut chain = TxnChain::new(0);
        let slot = CompareSlot::new(1, b"key1".to_vec());
        // First (newest) write — revert_lsn 500
        chain.add_revert(slot.clone(), 500, false);
        // Second (older) write to same slot — should be ignored
        chain.add_revert(slot.clone(), 200, true);
        assert_eq!(chain.len(), 1);
        let r = chain.get_revert(&slot).unwrap();
        assert_eq!(r.revert_lsn, 500); // Newest before-image kept
    }

    #[test]
    fn test_add_multiple_slots() {
        let mut chain = TxnChain::new(0);
        let s1 = CompareSlot::new(1, b"k1".to_vec());
        let s2 = CompareSlot::new(1, b"k2".to_vec());
        let s3 = CompareSlot::new(2, b"k1".to_vec());
        chain.add_revert(s1.clone(), 100, false);
        chain.add_revert(s2.clone(), 200, false);
        chain.add_revert(s3.clone(), 300, true);
        assert_eq!(chain.len(), 3);
        assert_eq!(chain.get_revert(&s1).unwrap().revert_lsn, 100);
        assert_eq!(chain.get_revert(&s2).unwrap().revert_lsn, 200);
        assert!(chain.get_revert(&s3).unwrap().revert_kd);
    }

    #[test]
    fn test_commit_lsn() {
        let mut chain = TxnChain::new(0);
        chain.set_commit_lsn(9999);
        assert_eq!(chain.commit_lsn(), 9999);
    }
}
