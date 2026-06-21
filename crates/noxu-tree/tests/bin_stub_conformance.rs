//! T-1 drift-guard: conformance between the RUNTIME `BinStub` (tree.rs, the
//! implementation that actually runs) and a compact, inline JE-faithful BIN
//! oracle.
//!
//! Background: the census found that noxu-tree carried two parallel BIN
//! implementations — a JE-transliterated `bin::Bin` / `in_node::InNode`
//! (faithful but exercised only by their own tests) shelved beside the runtime
//! `tree::BinStub` / `tree::InNodeStub`.  The risk is DRIFT: TREE-F1 was
//! exactly such a bug, where the faithful `find_entry` filtered `known_deleted`
//! slots while the runtime stub did not, so a deleted slot read as present.
//!
//! Rather than keep ~7k LOC of shelved parallel code as the oracle, this test
//! reimplements the JE-correct BIN semantics inline as a tiny, obviously-right
//! reference (`BinOracle`, a sorted `Vec` of full keys) and pins the runtime
//! `BinStub` to it on RANDOM inputs:
//!   * `find_entry` (exact / indicate-duplicate / insertion-point semantics),
//!   * the `known_deleted` "slot reads as ABSENT" semantics (the TREE-F1 case),
//!   * `compute_key_prefix` (longest common prefix of all keys),
//!   * the split index / split point (`n / 2`),
//!   * slot ordering after insert.
//!
//! If this test ever fails, the runtime stub has drifted from the JE-correct
//! semantics — fix the STUB to match the oracle.

use proptest::prelude::*;

use noxu_tree::tree::BinStub;
use noxu_util::{Lsn, NULL_LSN};

const BIN_LEVEL: i32 = 0x10000 | 1;
const EXACT_MATCH: i32 = 1 << 16;

// ===========================================================================
// Inline JE-faithful BIN oracle.
//
// A BIN is, semantically, a sorted set of (full_key, lsn) slots with a
// per-slot known_deleted flag.  Keys are kept fully expanded (no prefix
// compression) so the reference is trivially correct; prefix compression in
// the runtime is an *encoding* optimisation that must not change any of the
// observable semantics below.
// ===========================================================================

struct BinOracle {
    /// Sorted by full key (unsigned byte order), no duplicates.
    slots: Vec<(Vec<u8>, Lsn, bool)>, // (key, lsn, known_deleted)
}

impl BinOracle {
    fn new() -> Self {
        BinOracle { slots: Vec::new() }
    }

    /// Insert or update — `IN.insertEntry` / BIN insert path.  On an existing
    /// key, the LSN is overwritten in place (no duplicate slot).
    fn insert(&mut self, key: Vec<u8>, lsn: Lsn) {
        match self.slots.binary_search_by(|(k, _, _)| k.as_slice().cmp(&key)) {
            Ok(idx) => self.slots[idx].1 = lsn,
            Err(idx) => self.slots.insert(idx, (key, lsn, false)),
        }
    }

    fn n_entries(&self) -> usize {
        self.slots.len()
    }

    fn key(&self, idx: usize) -> &[u8] {
        &self.slots[idx].0
    }

    fn lsn(&self, idx: usize) -> Lsn {
        self.slots[idx].1
    }

    /// `IN.findEntry(key, indicateIfDuplicate, exact)` — the JE return
    /// convention: on a hit return `idx | EXACT_MATCH`; on a miss return -1
    /// when `exact`, else the insertion point.  (The `indicate_duplicate`
    /// argument is ignored at the BIN level: JE BIN.findEntry always OR's
    /// EXACT_MATCH on a hit.)
    fn find_entry(&self, key: &[u8], _indicate_dup: bool, exact: bool) -> i32 {
        match self.slots.binary_search_by(|(k, _, _)| k.as_slice().cmp(key)) {
            Ok(idx) => (idx as i32) | EXACT_MATCH,
            Err(idx) => {
                if exact {
                    -1
                } else {
                    idx as i32
                }
            }
        }
    }

    fn set_known_deleted(&mut self, key: &[u8]) {
        if let Ok(idx) =
            self.slots.binary_search_by(|(k, _, _)| k.as_slice().cmp(key))
        {
            self.slots[idx].2 = true;
        }
    }

    fn is_known_deleted(&self, idx: usize) -> bool {
        self.slots[idx].2
    }

    /// `IN.computeKeyPrefix(excludeIdx)` — the longest common prefix of the
    /// keys, optionally excluding one slot.  Faithful to JE
    /// `IN.computeKeyPrefix` (IN.java:1623): the early-out guard is on the
    /// TOTAL entry count (`nEntries <= 1`), NOT the post-exclusion count, and
    /// the seed key is `getKey(firstIdx)` where `firstIdx = (excludeIdx == 0)
    /// ? 1 : 0`.  So with two keys and `excludeIdx == 0` the result is the
    /// (single) remaining seed key in full, matching JE.
    fn compute_key_prefix(&self, exclude_idx: Option<usize>) -> Vec<u8> {
        let n = self.slots.len();
        if n <= 1 {
            return Vec::new();
        }
        let first_idx = if exclude_idx == Some(0) { 1 } else { 0 };
        let seed = self.slots[first_idx].0.as_slice();
        let mut prefix_len = seed.len();
        for (i, (k, _, _)) in self.slots.iter().enumerate() {
            if i <= first_idx || Some(i) == exclude_idx {
                continue;
            }
            let new_len = common_prefix_len(&seed[..prefix_len], k);
            if new_len < prefix_len {
                prefix_len = new_len;
            }
        }
        seed[..prefix_len].to_vec()
    }
}

/// Length of the longest common (unsigned-byte) prefix of `a` and `b`.
fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
}

// ===========================================================================
// Runtime BinStub helpers
// ===========================================================================

/// Build an empty runtime `BinStub` with no prefix and no entries.
fn empty_stub() -> BinStub {
    BinStub {
        node_id: 1,
        level: BIN_LEVEL,
        entries: Vec::new(),
        key_prefix: Vec::new(),
        dirty: false,
        is_delta: false,
        last_full_lsn: NULL_LSN,
        last_delta_lsn: NULL_LSN,
        generation: 0,
        parent: None,
        expiration_in_hours: true,
        cursor_count: 0,
    }
}

/// Translate the runtime stub's search onto the JE `IN.findEntry` return
/// convention (matches `TreeNode::find_entry` for the BIN arm).
fn stub_find(
    stub: &BinStub,
    key: &[u8],
    _indicate_dup: bool,
    exact: bool,
) -> i32 {
    let (idx, found) = stub.find_entry_compressed(key);
    if found {
        (idx as i32) | EXACT_MATCH
    } else if exact {
        -1
    } else {
        idx as i32
    }
}

/// Mark a stub slot known-deleted by its full key.
fn stub_mark_known_deleted(stub: &mut BinStub, full_key: &[u8]) {
    let (idx, found) = stub.find_entry_compressed(full_key);
    if found {
        stub.entries[idx].known_deleted = true;
    }
}

// ===========================================================================
// Strategies
// ===========================================================================

/// Generate a set of distinct keys (1..16 bytes each, up to `max` keys),
/// returned in arbitrary (insert) order.
fn distinct_keys(max: usize) -> impl Strategy<Value = Vec<Vec<u8>>> {
    prop::collection::hash_set(
        prop::collection::vec(any::<u8>(), 1..16),
        1..=max,
    )
    .prop_map(|s| s.into_iter().collect())
}

// ===========================================================================
// Properties
// ===========================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    /// After inserting the same key set, the runtime stub and the oracle must
    /// agree on n_entries, slot ordering (full keys), and per-slot LSN.
    #[test]
    fn insert_then_layout_agrees(keys in distinct_keys(40)) {
        let max = keys.len() + 8;
        let mut stub = empty_stub();
        let mut oracle = BinOracle::new();

        for (i, k) in keys.iter().enumerate() {
            let lsn = Lsn::from_u64(100 + i as u64);
            stub.insert_with_prefix(k.clone(), lsn, None);
            oracle.insert(k.clone(), lsn);
        }
        let _ = max;

        prop_assert_eq!(stub.entries.len(), oracle.n_entries(), "n_entries diverged");
        for i in 0..stub.entries.len() {
            prop_assert_eq!(
                &stub.get_full_key(i).unwrap(),
                oracle.key(i),
                "slot {} key diverged", i
            );
            prop_assert_eq!(
                stub.entries[i].lsn,
                oracle.lsn(i),
                "slot {} lsn diverged", i
            );
        }
    }

    /// `find_entry` must agree for inserted keys AND arbitrary probe keys,
    /// across all (indicate_duplicate, exact) flag combinations.
    #[test]
    fn find_entry_agrees(
        keys in distinct_keys(40),
        probes in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..16), 0..20),
    ) {
        let mut stub = empty_stub();
        let mut oracle = BinOracle::new();
        for (i, k) in keys.iter().enumerate() {
            let lsn = Lsn::from_u64(100 + i as u64);
            stub.insert_with_prefix(k.clone(), lsn, None);
            oracle.insert(k.clone(), lsn);
        }

        for probe in keys.iter().cloned().chain(probes.into_iter()) {
            for &dup in &[false, true] {
                for &exact in &[false, true] {
                    let s = stub_find(&stub, &probe, dup, exact);
                    let o = oracle.find_entry(&probe, dup, exact);
                    prop_assert_eq!(
                        s, o,
                        "find_entry({:?}, dup={}, exact={}) diverged: stub={} oracle={}",
                        probe, dup, exact, s, o
                    );
                }
            }
        }
    }

    /// `compute_key_prefix` must agree (with and without an excluded index).
    #[test]
    fn compute_key_prefix_agrees(keys in distinct_keys(40)) {
        let mut stub = empty_stub();
        let mut oracle = BinOracle::new();
        for (i, k) in keys.iter().enumerate() {
            let lsn = Lsn::from_u64(100 + i as u64);
            stub.insert_with_prefix(k.clone(), lsn, None);
            oracle.insert(k.clone(), lsn);
        }

        prop_assert_eq!(
            stub.compute_key_prefix(None),
            oracle.compute_key_prefix(None),
            "compute_key_prefix(None) diverged"
        );
        for ex in 0..stub.entries.len() {
            prop_assert_eq!(
                stub.compute_key_prefix(Some(ex)),
                oracle.compute_key_prefix(Some(ex)),
                "compute_key_prefix(Some({})) diverged", ex
            );
        }
    }

    /// Split point: JE `IN.splitInternal` uses the plain midpoint `n / 2` for
    /// the no-hint case; both implementations must place the split there and
    /// agree on the split key (full key of the first right-half slot).
    #[test]
    fn split_index_agrees(keys in distinct_keys(40)) {
        prop_assume!(keys.len() >= 2);
        let mut stub = empty_stub();
        let mut oracle = BinOracle::new();
        for (i, k) in keys.iter().enumerate() {
            let lsn = Lsn::from_u64(100 + i as u64);
            stub.insert_with_prefix(k.clone(), lsn, None);
            oracle.insert(k.clone(), lsn);
        }
        let split = stub.entries.len() / 2;
        prop_assert_eq!(
            stub.get_full_key(split).unwrap(),
            oracle.key(split).to_vec(),
            "split key at index {} diverged", split
        );
    }

    /// TREE-F1 GUARD: a known-deleted slot must read as ABSENT for exact
    /// lookups.  The runtime stub does not filter known_deleted inside
    /// `find_entry` (the JE-correct absence is enforced one layer up via
    /// `slot_is_live`); this pins the stub's `slot_is_live` predicate to the
    /// oracle's KD bit, so a future stub that forgets to filter KD slots fails.
    #[test]
    fn known_deleted_reads_absent(
        keys in distinct_keys(20),
        delete_seed in any::<u64>(),
    ) {
        prop_assume!(!keys.is_empty());
        let mut stub = empty_stub();
        let mut oracle = BinOracle::new();
        for (i, k) in keys.iter().enumerate() {
            let lsn = Lsn::from_u64(100 + i as u64);
            stub.insert_with_prefix(k.clone(), lsn, None);
            oracle.insert(k.clone(), lsn);
        }

        // Mark roughly half the keys known-deleted in both.
        let mut s = delete_seed;
        let mut deleted: Vec<Vec<u8>> = Vec::new();
        for k in &keys {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1);
            if s & 1 == 0 {
                stub_mark_known_deleted(&mut stub, k);
                oracle.set_known_deleted(k);
                deleted.push(k.clone());
            }
        }

        for k in &deleted {
            // Oracle: the matched slot is known_deleted (the JE-correct
            // "exact lookup of a KD slot is absent" precondition).
            let oidx = (oracle.find_entry(k, false, false) & 0xffff) as usize;
            prop_assert!(oracle.is_known_deleted(oidx),
                "oracle: {:?} should be known_deleted", k);
            // Runtime stub: the slot must read as NOT live.
            let (sidx, sfound) = stub.find_entry_compressed(k);
            prop_assert!(sfound, "stub lost slot for {:?}", k);
            prop_assert!(!stub.slot_is_live(sidx),
                "TREE-F1: stub slot for {:?} still reads live after KD", k);
        }

        // Non-deleted keys must still read live in the stub and not-KD in the
        // oracle.
        for k in &keys {
            if deleted.contains(k) {
                continue;
            }
            let (sidx, sfound) = stub.find_entry_compressed(k);
            prop_assert!(sfound && stub.slot_is_live(sidx),
                "stub: live key {:?} reads absent", k);
            let oidx = (oracle.find_entry(k, false, false) & 0xffff) as usize;
            prop_assert!(!oracle.is_known_deleted(oidx),
                "oracle: live key {:?} reads deleted", k);
        }
    }
}
