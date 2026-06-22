//! Transaction chain for HA rollback version-revert.
//!
//! Port of `com.sleepycat.je.txn.TxnChain`.
//!
//! `TxnChain` supports "txn rollback", which undoes the write operations for a
//! given transaction back to an arbitrary point (the matchpoint). It is built
//! during recovery to process a rollback period (see [`crate::rollback_tracker`]).
//!
//! ## Why a chain is needed
//!
//! In the log, the logrecs that make up a txn are chained, but each logrec's
//! undo (`abort`) info refers to the *pre-txn* version of the record, which may
//! not be the *immediately previous* version if the txn wrote the same record
//! multiple times. JE's worked example (see `TxnChain.java`):
//!
//! ```text
//! lsn   key  data      abortlsn
//! 100   A    10        null   (pre-txn A)
//! 150   B    100       null   (pre-txn B)
//!  ... txn begins ...
//! 200   A    20        100
//! 300   A    deleted   100
//! 400   B    200       150
//! 500   A    30        100
//! ```
//!
//! The txn chain is `500 -> 400 -> 300 -> 200 -> null`. To roll back to an
//! arbitrary entry we need, for each BIN slot, the chain of versions that
//! occupied it during the txn. Reverting logrec 500 (A=30) must restore A=20
//! (logrec 300's deleted state was the immediately-previous A version, then
//! 200 A=20) — *not* jump straight to the pre-txn A=10. Only the *earliest*
//! in-window write of a slot reverts to the pre-txn (abort) version.
//!
//! This is the difference between rollback and abort: a rollback returns an LN
//! to its **previous version** (intra- or inter-txnal), an abort always returns
//! it to its **pre-txn version**.
//!
//! ## Algorithm (faithful to `TxnChain`'s constructor)
//!
//! Walk the txn's logrecs backward (highest LSN first). Maintain a
//! `records_map: CompareSlot -> RevertInfo` holding the `RevertInfo` of the
//! latest logrec seen so far for each slot. For each logrec `L` (record `R`,
//! slot `recId`):
//!
//! 1. If `records_map[recId]` exists (a *later* R-logrec `Ln` was already
//!    seen), update that earlier-created `RevertInfo` so `Ln` reverts to `L`:
//!    `revert_lsn = L.lsn`, `revert_kd = false`, `revert_pd = L.is_deleted`,
//!    key/data from `L`.
//! 2. If `L.lsn > matchpoint` (`L` will be rolled back), create a new
//!    `RevertInfo` pointing to `L`'s *abort* (pre-txn) version, push it on
//!    `revert_list`, and store it in `records_map[recId]` (assume for now `L`
//!    is the first R-logrec; step 1 of an earlier `L'` will fix it up).
//! 3. Otherwise (`L` at/before the matchpoint, preserved) remove `recId` from
//!    the map: `L` is the surviving previous version that a later logrec
//!    reverts to.
//!
//! `revert_list` ends up in reverse-LSN order, one `RevertInfo` per
//! rolled-back logrec. The undo pass pops them in that order
//! (`getChain(txnId).pop()`).

use bytes::Bytes;
use noxu_util::Lsn;
use std::cmp::Ordering;

use crate::log_scanner::{LnOperation, LnRecord};

/// A key comparison function: returns the ordering of two keys for a given
/// database. `None`-equivalent default is unsigned byte comparison.
///
/// The undo path passes the database's configured comparator (the tree's
/// `KeyComparatorFn`); recovery without a custom comparator passes a closure
/// that defers to unsigned byte order.
pub type KeyCmp<'a> = &'a dyn Fn(&[u8], &[u8]) -> Ordering;

/// A BIN-slot identity: the database id plus the key, compared with the DB's
/// comparator. Port of `TxnChain.CompareSlot`.
///
/// Two logrecs hash to the same slot iff they belong to the same database and
/// their keys compare equal under that database's key comparator. Records from
/// different databases are never equal.
#[derive(Clone)]
struct CompareSlot {
    db_id: u64,
    key: Bytes,
}

impl CompareSlot {
    fn new(db_id: u64, key: Bytes) -> Self {
        Self { db_id, key }
    }

    /// Compare two slots using the DB comparator for the keys. JE
    /// `CompareSlot.compareTo` compares the DB id first, then the keys.
    ///
    /// `cmp` compares keys within the same database; the caller only ever
    /// compares slots in the same database (db id is checked first).
    fn cmp_with(&self, other: &CompareSlot, cmp: KeyCmp<'_>) -> Ordering {
        match self.db_id.cmp(&other.db_id) {
            Ordering::Equal => cmp(&self.key, &other.key),
            non_eq => non_eq,
        }
    }
}

/// The record version to revert a rolled-back logrec to. Port of
/// `TxnChain.RevertInfo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevertInfo {
    /// LSN of the version to revert to (`NULL_LSN` = pre-txn / first write).
    pub revert_lsn: Lsn,
    /// Revert-to-known-deleted: the slot was known-deleted before this version
    /// (the rolled-back logrec was the first write — delete the slot).
    pub revert_kd: bool,
    /// Revert-to-pending-deleted: the version we revert to is itself a delete.
    pub revert_pd: bool,
    /// Key of the version to revert to (`None` = same key as the logrec).
    pub revert_key: Option<Bytes>,
    /// Data of the version to revert to (`None` = not embedded; read from log
    /// at `revert_lsn`, or the slot is deleted when `revert_kd`/`revert_pd`).
    pub revert_data: Option<Bytes>,
}

impl RevertInfo {
    /// Construct the initial RevertInfo for a logrec, pointing at its *abort*
    /// (pre-txn) version. JE: `new RevertInfo(abortLsn, abortKD, abortKey,
    /// abortData, ...)` with `revertPD = false` initially.
    fn from_abort(rec: &LnRecord) -> Self {
        Self {
            revert_lsn: rec.abort_lsn,
            revert_kd: rec.abort_known_deleted,
            revert_pd: false,
            revert_key: rec.abort_key.clone(),
            revert_data: rec.abort_data.clone(),
        }
    }
}

/// A built transaction chain: the ordered list of `RevertInfo` for each
/// rolled-back logrec, popped during the undo pass.
///
/// Port of `com.sleepycat.je.txn.TxnChain`.
pub struct TxnChain {
    /// `RevertInfo` for each rolled-back logrec, in reverse-LSN order
    /// (highest LSN first), matching the undo pass's backward scan.
    revert_list: std::collections::VecDeque<RevertInfo>,
    /// LSNs that remain locked / preserved (at or before the matchpoint).
    /// JE `getRemainingLockedNodes()`.
    remaining_locked_nodes: Vec<Lsn>,
}

impl TxnChain {
    /// Build the chain for a single transaction from its in-window logrecs.
    ///
    /// `logrecs` must be **all** of the transaction's LN logrecs that the undo
    /// scan can see, in any order; the chain sorts them descending by LSN
    /// (the backward-walk order JE uses, `currLsn = ...getLastLsn()`).
    /// `matchpoint` splits rolled-back (`lsn > matchpoint`) from preserved
    /// (`lsn <= matchpoint`) logrecs.
    ///
    /// `cmp` is the key comparison function for the database(s) involved.
    /// Recovery rollback in JE fetches the `DatabaseImpl` per logrec; here the
    /// undo pass supplies the comparator for the slot's database. All logrecs
    /// in one chain belong to one transaction but may span databases — when
    /// they do, `CompareSlot` orders by db id first so `cmp` is only ever
    /// applied to same-db key pairs.
    pub fn build(
        mut logrecs: Vec<(Lsn, LnRecord)>,
        matchpoint: Lsn,
        cmp: KeyCmp<'_>,
    ) -> Self {
        // Backward walk: highest LSN first (JE follows prevLsn pointers).
        logrecs.sort_by_key(|(lsn, _)| std::cmp::Reverse(lsn.as_u64()));

        // records_map: latest-seen RevertInfo per slot. We keep it as a Vec
        // of (slot, index-into-revert_list) and search with the comparator,
        // because CompareSlot intentionally has no hash (a partial comparator
        // would break hashing — JE throws from CompareSlot.hashCode).
        let mut records_map: Vec<(CompareSlot, usize)> = Vec::new();
        let mut revert_list: Vec<RevertInfo> = Vec::new();
        let mut remaining_locked_nodes: Vec<Lsn> = Vec::new();

        for (lsn, rec) in &logrecs {
            let rec_id = CompareSlot::new(rec.db_id, rec.key.clone());

            // 1. If a later logrec for this slot exists, point it at L.
            let existing = records_map
                .iter()
                .position(|(s, _)| s.cmp_with(&rec_id, cmp) == Ordering::Equal);

            if let Some(pos) = existing {
                let idx = records_map[pos].1;
                let ri = &mut revert_list[idx];
                ri.revert_lsn = *lsn;
                ri.revert_kd = false;
                ri.revert_pd = matches!(rec.operation, LnOperation::Delete);
                // Key only matters if the DB allows key updates; we always
                // record the logrec's key (safe: the slot is keyed by it).
                ri.revert_key = Some(rec.key.clone());
                ri.revert_data = rec.data.clone();
            }

            // 2/3. Split at the matchpoint.
            if *lsn > matchpoint {
                // L will be rolled back: assume it is the first R-logrec and
                // set its revert info to the pre-txn (abort) version.
                let ri = RevertInfo::from_abort(rec);
                revert_list.push(ri);
                let new_idx = revert_list.len() - 1;
                match existing {
                    Some(pos) => records_map[pos].1 = new_idx,
                    None => records_map.push((rec_id, new_idx)),
                }
            } else {
                // L is preserved (at/before matchpoint): it is the surviving
                // previous version, so remove the slot from the map.
                if let Some(pos) = existing {
                    records_map.swap_remove(pos);
                }
                remaining_locked_nodes.push(*lsn);
            }
        }

        Self { revert_list: revert_list.into(), remaining_locked_nodes }
    }

    /// Pop the next `RevertInfo` (reverse-LSN order). JE `TxnChain.pop()`.
    pub fn pop(&mut self) -> Option<RevertInfo> {
        self.revert_list.pop_front()
    }

    /// Number of rolled-back logrecs (RevertInfo entries).
    pub fn len(&self) -> usize {
        self.revert_list.len()
    }

    /// Whether the chain is empty.
    pub fn is_empty(&self) -> bool {
        self.revert_list.is_empty()
    }

    /// LSNs preserved (at/before matchpoint). JE `getRemainingLockedNodes()`.
    pub fn remaining_locked_nodes(&self) -> &[Lsn] {
        &self.remaining_locked_nodes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::log_scanner::LnOperation;
    use noxu_util::NULL_LSN;

    fn lsn(file: u32, off: u32) -> Lsn {
        Lsn::new(file, off)
    }

    fn ln(
        db_id: u64,
        txn_id: u64,
        op: LnOperation,
        key: &[u8],
        data: Option<&[u8]>,
        abort_lsn: Lsn,
        abort_known_deleted: bool,
        abort_data: Option<&[u8]>,
    ) -> LnRecord {
        let mut r = LnRecord::new(
            db_id,
            Some(txn_id),
            op,
            Bytes::copy_from_slice(key),
            data.map(Bytes::copy_from_slice),
            abort_lsn,
            abort_known_deleted,
        );
        r.abort_data = abort_data.map(Bytes::copy_from_slice);
        r
    }

    /// STEP 3 headline: an intra-txnal rollback (txn wrote v1 then v2, rolled
    /// back to v1) must REVERT to v1, not skip both.
    ///
    /// Chain (single txn, single slot A):
    /// ```text
    ///   100  A=v1  (abort_lsn=NULL, first write of A by this txn)
    ///   200  A=v2  (abort_lsn=NULL)   <- rolled back
    /// ```
    /// matchpoint = 150 (between 100 and 200). Logrec 200 must revert to the
    /// immediately-previous version, logrec 100 (A=v1) — NOT to the pre-txn
    /// version (which would delete A entirely, the "skip both" bug).
    #[test]
    fn test_intra_txnal_revert_to_v1_not_skip_both() {
        let recs = vec![
            (
                lsn(1, 100),
                ln(
                    7,
                    1,
                    LnOperation::Insert,
                    b"A",
                    Some(b"v1"),
                    NULL_LSN,
                    true,
                    None,
                ),
            ),
            (
                lsn(1, 200),
                ln(
                    7,
                    1,
                    LnOperation::Update,
                    b"A",
                    Some(b"v2"),
                    NULL_LSN,
                    true,
                    None,
                ),
            ),
        ];

        let mut chain =
            TxnChain::build(recs, lsn(1, 150), &|a: &[u8], b: &[u8]| a.cmp(b));

        // Exactly one logrec rolled back (200); 100 is preserved.
        assert_eq!(chain.len(), 1);
        let ri = chain.pop().unwrap();

        // It reverts to logrec 100 (the previous in-window version), NOT to
        // the pre-txn NULL/known-deleted state.
        assert_eq!(ri.revert_lsn, lsn(1, 100), "must revert to v1, not skip");
        assert!(!ri.revert_kd, "must NOT delete the slot (that is skip-both)");
        assert_eq!(ri.revert_data.as_deref(), Some(&b"v1"[..]));

        // 100 is the preserved/locked node.
        assert_eq!(chain.remaining_locked_nodes(), &[lsn(1, 100)]);
    }

    /// When the matchpoint precedes the txn's first write, the earliest
    /// in-window logrec reverts to the pre-txn (abort) version.
    #[test]
    fn test_first_write_reverts_to_pretxn() {
        // matchpoint=50 < 100, so both 100 and 200 are rolled back.
        let recs = vec![
            (
                lsn(1, 100),
                ln(
                    7,
                    1,
                    LnOperation::Insert,
                    b"A",
                    Some(b"v1"),
                    NULL_LSN,
                    true,
                    None,
                ),
            ),
            (
                lsn(1, 200),
                ln(
                    7,
                    1,
                    LnOperation::Update,
                    b"A",
                    Some(b"v2"),
                    NULL_LSN,
                    true,
                    None,
                ),
            ),
        ];

        let mut chain =
            TxnChain::build(recs, lsn(1, 50), &|a: &[u8], b: &[u8]| a.cmp(b));
        assert_eq!(chain.len(), 2);

        // Popped reverse-LSN: first 200 reverts to 100, then 100 reverts to
        // pre-txn (NULL/known-deleted → delete the slot).
        let ri200 = chain.pop().unwrap();
        assert_eq!(ri200.revert_lsn, lsn(1, 100));
        assert!(!ri200.revert_kd);

        let ri100 = chain.pop().unwrap();
        assert_eq!(ri100.revert_lsn, NULL_LSN);
        assert!(ri100.revert_kd, "first write reverts to pre-txn delete");
    }

    /// JE worked example: two slots (A, B), interleaved writes.
    /// ```text
    ///   200  A=20  abort=100
    ///   300  A=del abort=100
    ///   400  B=200 abort=150
    ///   500  A=30  abort=100
    /// ```
    /// matchpoint=150 (all rolled back). Expect:
    ///   500 -> 300 (A=del), 400 -> 150 (pre-txn B), 300 -> 200 (A=20),
    ///   200 -> 100 (pre-txn A).
    #[test]
    fn test_je_worked_example_two_slots() {
        let recs = vec![
            (
                lsn(1, 200),
                ln(
                    7,
                    1,
                    LnOperation::Update,
                    b"A",
                    Some(b"20"),
                    lsn(1, 100),
                    false,
                    Some(b"10"),
                ),
            ),
            (
                lsn(1, 300),
                ln(
                    7,
                    1,
                    LnOperation::Delete,
                    b"A",
                    None,
                    lsn(1, 100),
                    false,
                    Some(b"10"),
                ),
            ),
            (
                lsn(1, 400),
                ln(
                    7,
                    1,
                    LnOperation::Update,
                    b"B",
                    Some(b"200"),
                    lsn(1, 150),
                    false,
                    Some(b"100"),
                ),
            ),
            (
                lsn(1, 500),
                ln(
                    7,
                    1,
                    LnOperation::Update,
                    b"A",
                    Some(b"30"),
                    lsn(1, 100),
                    false,
                    Some(b"10"),
                ),
            ),
        ];

        let mut chain =
            TxnChain::build(recs, lsn(1, 150), &|a: &[u8], b: &[u8]| a.cmp(b));
        assert_eq!(chain.len(), 4);

        // reverse-LSN pop order: 500, 400, 300, 200
        let ri500 = chain.pop().unwrap();
        assert_eq!(ri500.revert_lsn, lsn(1, 300), "A=30 reverts to A=del @300");
        assert!(ri500.revert_pd, "the version at 300 is a delete");

        let ri400 = chain.pop().unwrap();
        assert_eq!(ri400.revert_lsn, lsn(1, 150), "B reverts to pre-txn B");

        let ri300 = chain.pop().unwrap();
        assert_eq!(ri300.revert_lsn, lsn(1, 200), "A=del reverts to A=20 @200");

        let ri200 = chain.pop().unwrap();
        assert_eq!(ri200.revert_lsn, lsn(1, 100), "A=20 reverts to pre-txn A");
    }
}
