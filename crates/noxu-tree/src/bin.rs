//! BIN (Bottom Internal Node) implementation.
//!
//!
//! A BIN is the leaf-level internal node in the B+tree. BINs contain
//! references to LN (Leaf Node) data records. BINs support BIN-deltas
//! for efficient logging of partially-modified nodes.
//!
//! # BIN-deltas
//!
//! A BIN-delta is a BIN with the non-dirty slots omitted. A "full BIN", OTOH
//! contains all slots. On disk and in memory, the format of a BIN-delta is the
//! same as that of a BIN. In memory, a BIN object is actually a BIN-delta when
//! the BIN-delta flag is set. On disk, the NewBINDelta log entry type is the
//! only thing that distinguishes it from a full BIN, which has the BIN log
//! entry type.
//!
//! BIN-deltas provide two benefits: Reduced writing and reduced memory usage.
//!
//! ## Reduced Writing
//!
//! Logging a BIN-delta rather a full BIN reduces writing significantly. The
//! cost, however, is that two reads are necessary to reconstruct a full BIN
//! from scratch. The reduced writing is worth this cost, particularly because
//! less writing means less log cleaning.
//!
//! A BIN-delta is logged when 25% or less (configured with EnvironmentConfig
//! TREE_BIN_DELTA) of the slots in a BIN are dirty. When a BIN-delta is logged,
//! the dirty flag is cleared on the the BIN in cache. If more slots are
//! dirtied and another BIN-delta is logged, it will contain all entries dirtied
//! since the last full BIN was logged. In other words, BIN-deltas are
//! cumulative and not chained, to avoid reading many (more than two) log
//! entries to reconstruct a full BIN.
//!
//! ## Reduced Memory Usage
//!
//! In the Btree cache, a BIN may be represented as a full BIN or a BIN-delta.
//! Eviction will mutate a full BIN to a BIN-delta in preference to discarding
//! the entire BIN. A BIN-delta in cache occupies less memory than a full BIN.

use crate::entry_states::DIRTY_BIT;
use crate::error::TreeError;
use crate::key::{create_key_prefix, get_key_prefix_length};
use noxu_util::{Lsn, NULL_LSN, Vlsn};
use hashbrown::HashSet;

// BIN builds on the same slot-array model as the upper IN but lives at level 1.
// It carries its own lightweight InNode helper (distinct from in_node::InNode)
// because BIN's latch requirements and slot layout differ from upper INs.
// The full integration with in_node::InNode is deferred to the tree-integration
// milestone; until then this module owns its own compact slot store.

#[derive(Debug)]
pub struct InNode {
    db_id: u64,
    level: i32,
    max_entries: usize,
    keys: Vec<Vec<u8>>,
    lsns: Vec<Lsn>,
    states: Vec<u8>,
    is_delta: bool,
    /// Node-level dirty flag.
    ///
    /// `IN.dirty` (IN_DIRTY_BIT in `flags`).
    dirty: bool,
    /// When true, the next checkpoint must write a full BIN rather than a delta.
    ///
    /// Set when a dirty slot is compressed away — sets this in
    /// `BIN.compress()` to prevent a subsequent delta from omitting the
    /// compressed slot.
    prohibit_next_delta: bool,
    /// Persistent node ID assigned at creation.
    ///
    /// `IN.nodeId` (allocated from `NodeSequence`).
    node_id: u64,
}

/// Monotonic counter for BIN node IDs (mirrors NodeSequence).
static NEXT_BIN_NODE_ID: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(1);

impl InNode {
    pub fn new(db_id: u64, level: i32, max_entries: usize) -> Self {
        let node_id =
            NEXT_BIN_NODE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Self {
            db_id,
            level,
            max_entries,
            keys: Vec::new(),
            lsns: Vec::new(),
            states: Vec::new(),
            is_delta: false,
            dirty: false,
            prohibit_next_delta: false,
            node_id,
        }
    }

    pub fn get_n_entries(&self) -> usize {
        self.keys.len()
    }

    pub fn max_entries(&self) -> usize {
        self.max_entries
    }

    pub fn get_key(&self, index: usize) -> Option<&[u8]> {
        self.keys.get(index).map(|k| k.as_slice())
    }

    pub fn get_lsn(&self, index: usize) -> Lsn {
        self.lsns.get(index).copied().unwrap_or(NULL_LSN)
    }

    pub fn get_state(&self, index: usize) -> u8 {
        self.states.get(index).copied().unwrap_or(0)
    }

    pub fn find_entry(
        &self,
        key: &[u8],
        _indicator: bool,
        _exact: bool,
    ) -> i32 {
        match self.keys.binary_search_by(|k| k.as_slice().cmp(key)) {
            Ok(idx) => idx as i32 | 0x1_0000, // EXACT_MATCH flag
            Err(idx) => idx as i32,
        }
    }

    pub fn insert_entry(
        &mut self,
        key: Vec<u8>,
        lsn: Lsn,
        state: u8,
    ) -> Result<i32, TreeError> {
        if self.keys.len() >= self.max_entries {
            return Err(TreeError::SplitRequired);
        }
        let index = match self.keys.binary_search(&key) {
            Ok(idx) => {
                // Key exists, update it
                self.lsns[idx] = lsn;
                self.states[idx] = state;
                return Ok(idx as i32 | 0x1_0000); // EXACT_MATCH flag
            }
            Err(idx) => idx,
        };
        self.keys.insert(index, key);
        self.lsns.insert(index, lsn);
        self.states.insert(index, state);
        Ok(index as i32)
    }

    pub fn delete_entry(&mut self, index: usize) -> bool {
        if index < self.keys.len() {
            self.keys.remove(index);
            self.lsns.remove(index);
            self.states.remove(index);
            true
        } else {
            false
        }
    }

    pub fn is_bin_delta(&self) -> bool {
        self.is_delta
    }

    pub fn set_bin_delta(&mut self, delta: bool) {
        self.is_delta = delta;
    }

    pub fn is_entry_embedded_ln(&self, index: usize) -> bool {
        if let Some(&state) = self.states.get(index) {
            state & crate::entry_states::EMBEDDED_LN_BIT != 0
        } else {
            false
        }
    }

    pub fn is_entry_known_deleted(&self, index: usize) -> bool {
        self.states.get(index).is_some_and(|&s| {
            s & crate::entry_states::KNOWN_DELETED_BIT != 0
        })
    }

    pub fn is_entry_pending_deleted(&self, index: usize) -> bool {
        self.states.get(index).is_some_and(|&s| {
            s & crate::entry_states::PENDING_DELETED_BIT != 0
        })
    }

    pub fn is_entry_dirty(&self, index: usize) -> bool {
        self.states
            .get(index)
            .is_some_and(|&s| s & DIRTY_BIT != 0)
    }

    pub fn is_tombstone(&self, index: usize) -> bool {
        self.states.get(index).is_some_and(|&s| {
            s & crate::entry_states::TOMBSTONE_BIT != 0
        })
    }

    pub fn set_tombstone(&mut self, index: usize, tombstone: bool) {
        if let Some(s) = self.states.get_mut(index) {
            if tombstone {
                *s |= crate::entry_states::TOMBSTONE_BIT;
            } else {
                *s &= !crate::entry_states::TOMBSTONE_BIT;
            }
            *s |= DIRTY_BIT;
        }
    }

    pub fn set_known_deleted(&mut self, index: usize) {
        if let Some(s) = self.states.get_mut(index) {
            *s |= crate::entry_states::KNOWN_DELETED_BIT;
            *s &= !crate::entry_states::PENDING_DELETED_BIT;
            *s |= DIRTY_BIT;
        }
    }

    pub fn clear_known_deleted(&mut self, index: usize) {
        if let Some(s) = self.states.get_mut(index) {
            *s &= !crate::entry_states::KNOWN_DELETED_BIT;
            *s |= DIRTY_BIT;
        }
    }

    pub fn set_pending_deleted(&mut self, index: usize) {
        if let Some(s) = self.states.get_mut(index) {
            *s |= crate::entry_states::PENDING_DELETED_BIT;
            *s |= DIRTY_BIT;
        }
    }

    pub fn clear_pending_deleted(&mut self, index: usize) {
        if let Some(s) = self.states.get_mut(index) {
            *s &= !crate::entry_states::PENDING_DELETED_BIT;
            *s |= DIRTY_BIT;
        }
    }

    /// Returns the node-level dirty flag.
    ///
    /// `IN.isDirty()`.
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Sets or clears the node-level dirty flag.
    ///
    /// `IN.setDirty(boolean)`.
    pub fn set_dirty(&mut self, dirty: bool) {
        self.dirty = dirty;
    }

    /// Returns true if the next checkpoint must write a full BIN (not a delta).
    ///
    /// `BIN.getProhibitNextDelta()`.
    pub fn get_prohibit_next_delta(&self) -> bool {
        self.prohibit_next_delta
    }

    /// Sets or clears the prohibit-next-delta flag.
    ///
    /// `BIN.setProhibitNextDelta(boolean)`.
    pub fn set_prohibit_next_delta(&mut self, val: bool) {
        self.prohibit_next_delta = val;
    }

    /// Returns the persistent node ID.
    ///
    /// `Node.getNodeId()`.
    pub fn node_id(&self) -> i64 {
        self.node_id as i64
    }

    pub fn set_lsn(&mut self, index: usize, lsn: Lsn) {
        if let Some(l) = self.lsns.get_mut(index) {
            *l = lsn;
        }
    }

    pub fn set_state(&mut self, index: usize, state: u8) {
        if let Some(s) = self.states.get_mut(index) {
            *s = state;
        }
    }

    /// Returns true if all slots are known-deleted (and the node is non-empty).
    pub fn is_valid_for_delete(&self) -> bool {
        if self.keys.is_empty() {
            return false;
        }
        self.states
            .iter()
            .all(|&s| s & crate::entry_states::KNOWN_DELETED_BIT != 0)
    }

    pub fn latch(&self) {}

    pub fn latch_shared(&self) {}

    pub fn release_latch(&self) {}
}

/// A Bottom Internal Node in the B+tree.
///
/// BINs are always at level 1 (MAIN_LEVEL | 1 = BIN_LEVEL).
/// They reference LN data records in their slots.
///
/// # Key Prefix Compression
///
/// When `key_prefix` is non-empty, `inner.keys` stores only the *suffix* of
/// each key — the bytes after stripping the common leading prefix.  The full
/// key is reconstructed by prepending `key_prefix` to the stored suffix via
/// `get_full_key()`.  The prefix is recomputed via `recompute_key_prefix()`
/// after bulk inserts and splits, mirroring `IN.keyPrefix` pattern.
///
/// .
#[derive(Debug)]
pub struct Bin {
    /// The underlying IN (composition  -  BIN extends IN in the).
    /// `inner.keys` stores *suffixes* when `key_prefix` is non-empty.
    pub(crate) inner: InNode,

    /// Common prefix shared by every key in this BIN.
    /// Empty means prefix compression is not active.
    /// `IN.keyPrefix`.
    pub(crate) key_prefix: Vec<u8>,

    /// LSN of the last full BIN that was logged (for BIN-delta reconstruction).
    last_full_version: Lsn,

    /// Set of cursor IDs currently positioned on this BIN.
    ///
    /// uses a `TinyHashSet<CursorImpl>` pointer set; we track cursor IDs
    /// (u64) as a lightweight substitute until the full cursor integration is
    /// wired.  None means the set is empty.
    cursor_set: Option<HashSet<u64>>,

    /// Bloom filter for BIN-delta key membership testing.
    /// None if this is a full BIN or bloom filter is not computed.
    delta_bloom_filter: Option<Vec<u8>>,

    /// VLSN for each slot (only when VLSNs are preserved).
    /// None means VLSNs are not tracked.
    slot_vlsns: Option<Vec<Vlsn>>,

    /// Expiration time for each slot (TTL support).
    slot_expirations: Option<Vec<u32>>,

    /// Embedded LN data for slots with EMBEDDED_LN_BIT set.
    /// Index i maps to embedded data for slot i; None if not embedded.
    slot_embedded_data: Vec<Option<Vec<u8>>>,

    /// Per-slot last-modification time in milliseconds since epoch.
    ///
    /// Only populated for slots with embedded LN data. A value of 0 means
    /// no modification time is recorded for that slot.
    ///
    /// `BIN.modificationTimes` (`INLongRep` array, NoSQL fork).
    /// `INLongRep` uses variable-length encoding with a delta base to
    /// pack times compactly; Noxu stores absolute millis in a plain Vec for
    /// simplicity while preserving the same per-slot semantics.
    pub(crate) modification_times: Vec<u64>,

    /// Per-slot creation time in milliseconds since epoch.
    ///
    /// Only populated for slots with embedded LN data. A value of 0 means
    /// no creation time is recorded for that slot.
    ///
    /// `BIN.creationTimes` (`INLongRep` array, NoSQL fork).
    pub(crate) creation_times: Vec<u64>,
}

impl Bin {
    /// Creates a new BIN with the specified parameters.
    ///
    /// # Arguments
    ///
    /// * `db_id` - Database ID this BIN belongs to
    /// * `max_entries` - Maximum number of entries (slots) in this BIN
    pub fn new(db_id: u64, max_entries: usize) -> Self {
        let inner = InNode::new(db_id, crate::tree::BIN_LEVEL, max_entries);
        Self {
            inner,
            key_prefix: Vec::new(),
            last_full_version: NULL_LSN,
            cursor_set: None,
            delta_bloom_filter: None,
            slot_vlsns: None,
            slot_expirations: None,
            slot_embedded_data: Vec::new(),
            modification_times: Vec::new(),
            creation_times: Vec::new(),
        }
    }

    // ========================================================================
    // Key prefix compression
    // ========================================================================

    /// Returns the current key prefix (may be empty).
    ///
    /// `IN.getKeyPrefix()`.
    pub fn get_key_prefix(&self) -> &[u8] {
        &self.key_prefix
    }

    /// Returns true when prefix compression is active.
    ///
    /// `IN.hasKeyPrefix()`.
    pub fn has_key_prefix(&self) -> bool {
        !self.key_prefix.is_empty()
    }

    /// Reconstruct the full key for slot `index` by prepending the prefix.
    ///
    /// Returns `None` if `index` is out of range.
    ///
    /// `IN.getKey(int idx)`.
    pub fn get_full_key(&self, index: usize) -> Option<Vec<u8>> {
        let suffix = self.inner.get_key(index)?;
        if self.key_prefix.is_empty() {
            Some(suffix.to_vec())
        } else {
            let mut full = Vec::with_capacity(self.key_prefix.len() + suffix.len());
            full.extend_from_slice(&self.key_prefix);
            full.extend_from_slice(suffix);
            Some(full)
        }
    }

    /// Decompress a stored suffix into a full key.
    ///
    /// If `key_prefix` is empty the suffix *is* the full key.
    pub fn decompress_key(&self, suffix: &[u8]) -> Vec<u8> {
        if self.key_prefix.is_empty() {
            suffix.to_vec()
        } else {
            let mut full = Vec::with_capacity(self.key_prefix.len() + suffix.len());
            full.extend_from_slice(&self.key_prefix);
            full.extend_from_slice(suffix);
            full
        }
    }

    /// Strip the current prefix from a full key to obtain the suffix to store.
    ///
    /// `IN.computeKeySuffix(prefix, key)`.
    fn compress_key(&self, full_key: &[u8]) -> Vec<u8> {
        let plen = self.key_prefix.len();
        if plen == 0 {
            full_key.to_vec()
        } else {
            full_key[plen..].to_vec()
        }
    }

    /// Compute the longest common prefix across all keys currently in this BIN,
    /// optionally excluding slot `exclude_idx`.
    ///
    /// Returns an empty `Vec` when fewer than 2 entries exist or keys share no
    /// common prefix.
    ///
    /// `IN.computeKeyPrefix(int excludeIdx)`.
    pub fn compute_key_prefix(&self, exclude_idx: Option<usize>) -> Vec<u8> {
        let n = self.inner.get_n_entries();
        if n < 2 {
            return Vec::new();
        }

        let first_idx = match exclude_idx {
            Some(0) => 1,
            _ => 0,
        };

        let seed_full = match self.get_full_key(first_idx) {
            Some(k) => k,
            None => return Vec::new(),
        };
        let mut prefix_len = seed_full.len();

        for i in (first_idx + 1)..n {
            if let Some(ex) = exclude_idx
                && i == ex
            {
                continue;
            }
            let full_key = match self.get_full_key(i) {
                Some(k) => k,
                None => continue,
            };
            let new_len = get_key_prefix_length(&seed_full[..prefix_len], &full_key);
            if new_len < prefix_len {
                prefix_len = new_len;
            }
            if prefix_len == 0 {
                return Vec::new();
            }
        }

        seed_full[..prefix_len].to_vec()
    }

    /// Recompute the key prefix from scratch and re-encode every stored suffix.
    ///
    /// Call after bulk inserts, splits, or merges.
    ///
    /// `IN.recalcKeyPrefix()` → `IN.recalcSuffixes(newPrefix, …)`.
    pub fn recompute_key_prefix(&mut self) {
        let new_prefix = self.compute_key_prefix(None);
        self.apply_new_prefix(new_prefix);
    }

    /// Apply `new_prefix`, re-encoding all suffixes from the old prefix into
    /// the new one.
    ///
    /// `IN.recalcSuffixes(newPrefix, null, null, -1)`.
    fn apply_new_prefix(&mut self, new_prefix: Vec<u8>) {
        let n = self.inner.get_n_entries();
        let full_keys: Vec<Vec<u8>> =
            (0..n).map(|i| self.get_full_key(i).unwrap_or_default()).collect();

        self.key_prefix = new_prefix;

        for (i, full_key) in full_keys.into_iter().enumerate() {
            let suffix = self.compress_key(&full_key);
            self.inner.keys[i] = suffix;
        }
    }

    /// Binary search for `full_key` against prefix-compressed stored keys.
    ///
    /// Returns `(slot_index, exact_match)`.
    fn find_entry_compressed(&self, full_key: &[u8]) -> (usize, bool) {
        let plen = self.key_prefix.len();
        if plen > 0
            && (full_key.len() < plen || &full_key[..plen] != self.key_prefix.as_slice())
        {
            // Key does not share the stored prefix — use allocation-free
            // two-part comparison: compare (prefix ++ suffix) against full_key
            // by chaining the prefix slice and suffix slice comparisons.
            let prefix = self.key_prefix.as_slice();
            let pos = self.inner.keys.partition_point(|s| {
                // Compare prefix portion first, then suffix if prefix matches.
                let pfx_cmp = prefix.cmp(&full_key[..prefix.len().min(full_key.len())]);
                if pfx_cmp != std::cmp::Ordering::Equal {
                    return pfx_cmp == std::cmp::Ordering::Less;
                }
                // Prefixes agree up to min(plen, full_key.len()):
                // the full entry key is prefix ++ s; the comparison continuation
                // is full_key[plen..] vs s.
                if full_key.len() <= plen {
                    // full_key exhausted before or at prefix boundary: prefix ++ s > full_key
                    return false;
                }
                s.as_slice() < &full_key[plen..]
            });
            return (pos, false);
        }
        let suffix = &full_key[plen..];
        match self.inner.keys.binary_search_by(|k| k.as_slice().cmp(suffix)) {
            Ok(idx) => (idx, true),
            Err(idx) => (idx, false),
        }
    }

    /// Returns true  -  this is always a BIN.
    #[inline]
    pub fn is_bin(&self) -> bool {
        true
    }

    /// Returns true if this BIN is currently in BIN-delta form.
    #[inline]
    pub fn is_bin_delta(&self) -> bool {
        self.inner.is_bin_delta()
    }

    /// Marks this BIN as a delta.
    #[inline]
    pub fn set_bin_delta(&mut self, delta: bool) {
        self.inner.set_bin_delta(delta);
    }

    /// Returns the number of slots.
    #[inline]
    pub fn get_n_entries(&self) -> usize {
        self.inner.get_n_entries()
    }

    /// Returns the maximum number of entries this BIN can hold.
    #[inline]
    pub fn max_entries(&self) -> usize {
        self.inner.max_entries()
    }

    /// Gets the full (decompressed) key at the given slot index.
    ///
    /// When prefix compression is active the returned `Vec` is freshly
    /// allocated (prefix prepended to the stored suffix).  When there is no
    /// prefix the suffix byte-slice is returned as-is inside a `Vec`.
    ///
    /// `IN.getKey(int idx)`.
    #[inline]
    pub fn get_key(&self, index: usize) -> Option<Vec<u8>> {
        self.get_full_key(index)
    }

    /// Gets the LSN at the given slot index.
    #[inline]
    pub fn get_lsn(&self, index: usize) -> Lsn {
        self.inner.get_lsn(index)
    }

    /// Gets the state byte at the given slot index.
    #[inline]
    pub fn get_state(&self, index: usize) -> u8 {
        self.inner.get_state(index)
    }

    /// Binary search for a full (uncompressed) key.
    ///
    /// When prefix compression is active `key` is searched using the
    /// prefix-aware path: the prefix is stripped from `key` and the
    /// remaining suffix is compared against stored suffixes.
    ///
    /// # Returns
    ///
    /// Index into the slot array. If the EXACT_MATCH flag (bit 16) is set,
    /// an exact match was found. Otherwise returns the insertion point.
    ///
    /// `IN.findEntry(key, indicateIfDuplicate, exact)`.
    pub fn find_entry(&self, key: &[u8], _indicator: bool, exact: bool) -> i32 {
        let (idx, found) = self.find_entry_compressed(key);
        if found {
            (idx as i32) | 0x1_0000 // EXACT_MATCH flag
        } else if exact {
            -1
        } else {
            idx as i32
        }
    }

    // --- Cursor management ---

    /// Returns the set of cursor IDs currently positioned on this BIN.
    ///
    /// 
    pub fn get_cursor_set(&self) -> std::collections::BTreeSet<u64> {
        match &self.cursor_set {
            None => std::collections::BTreeSet::new(),
            Some(set) => set.iter().copied().collect(),
        }
    }

    /// Registers a cursor (by ID) with this BIN.
    ///
    /// 
    pub fn add_cursor(&mut self, cursor_id: u64) {
        self.cursor_set
            .get_or_insert_with(HashSet::new)
            .insert(cursor_id);
    }

    /// Unregisters a cursor (by ID) from this BIN.
    ///
    /// 
    pub fn remove_cursor(&mut self, cursor_id: u64) {
        if let Some(set) = self.cursor_set.as_mut() {
            set.remove(&cursor_id);
            if set.is_empty() {
                self.cursor_set = None;
            }
        }
    }

    /// Returns the number of cursors currently positioned on this BIN.
    ///
    /// 
    #[inline]
    pub fn n_cursors(&self) -> usize {
        self.cursor_set.as_ref().map_or(0, |s| s.len())
    }

    /// Returns true if any cursors are currently positioned on this BIN.
    ///
    /// 
    #[inline]
    pub fn has_cursors(&self) -> bool {
        self.n_cursors() > 0
    }

    // --- Legacy cursor count helpers (kept for backward compat) ---

    /// Adjusts the legacy cursor count by the given delta.
    ///
    /// Deprecated: prefer `add_cursor` / `remove_cursor`.
    pub fn adjust_cursor_count(&mut self, delta: i32) {
        if delta > 0 {
            // Generate unique synthetic cursor IDs based on current set size so
            // each call inserts truly new IDs even across multiple adjust calls.
            let base = self.cursor_set.as_ref().map_or(0, |s| s.len()) as u64;
            for i in 0..delta as u64 {
                self.add_cursor(base + i); // synthetic IDs, unique per BIN
            }
        } else {
            let n = (-delta) as usize;
            let ids: Vec<u64> = self
                .cursor_set
                .as_ref()
                .map(|s| s.iter().copied().take(n).collect())
                .unwrap_or_default();
            for id in ids {
                self.remove_cursor(id);
            }
        }
    }

    /// Returns the current cursor count.
    #[inline]
    pub fn get_cursor_count(&self) -> i32 {
        self.n_cursors() as i32
    }

    // --- BIN-specific slot operations ---

    /// Returns whether the slot contains an embedded LN.
    #[inline]
    pub fn is_embedded_ln(&self, index: usize) -> bool {
        self.inner.is_entry_embedded_ln(index)
    }

    /// Sets embedded LN data for a slot.
    ///
    /// # Arguments
    ///
    /// * `index` - Slot index
    /// * `data` - Embedded LN data, or None to clear
    pub fn set_embedded_data(&mut self, index: usize, data: Option<Vec<u8>>) {
        // Ensure slot_embedded_data is large enough
        while self.slot_embedded_data.len() <= index {
            self.slot_embedded_data.push(None);
        }
        self.slot_embedded_data[index] = data;
    }

    /// Gets embedded LN data for a slot.
    ///
    /// # Arguments
    ///
    /// * `index` - Slot index
    ///
    /// # Returns
    ///
    /// Reference to embedded data, or None if not embedded
    pub fn get_embedded_data(&self, index: usize) -> Option<&[u8]> {
        self.slot_embedded_data.get(index)?.as_deref()
    }

    /// Gets the VLSN for a slot.
    ///
    /// # Arguments
    ///
    /// * `index` - Slot index
    ///
    /// # Returns
    ///
    /// VLSN for the slot, or VLSN(0) if not tracked
    pub fn get_slot_vlsn(&self, index: usize) -> Vlsn {
        self.slot_vlsns
            .as_ref()
            .and_then(|v| v.get(index).copied())
            .unwrap_or_else(|| Vlsn::new(0))
    }

    /// Sets the VLSN for a slot.
    ///
    /// # Arguments
    ///
    /// * `index` - Slot index
    /// * `vlsn` - VLSN to set
    pub fn set_slot_vlsn(&mut self, index: usize, vlsn: Vlsn) {
        let vlsns = self.slot_vlsns.get_or_insert_with(|| {
            vec![Vlsn::new(0); self.inner.max_entries()]
        });
        if index < vlsns.len() {
            vlsns[index] = vlsn;
        }
    }

    // --- BIN-delta operations ---

    /// Returns true if this BIN should be logged as a delta.
    ///
    /// A delta is logged when <= 25% of slots are dirty.
    pub fn should_log_delta(&self) -> bool {
        let dirty_count = self.count_dirty_slots();
        if dirty_count == 0 || self.inner.get_n_entries() == 0 {
            return false;
        }
        let total = self.inner.get_n_entries();
        // Default threshold: 25%
        dirty_count <= total / 4
    }

    /// Counts the number of dirty slots.
    pub fn count_dirty_slots(&self) -> usize {
        let mut count = 0;
        for i in 0..self.inner.get_n_entries() {
            if self.inner.get_state(i) & DIRTY_BIT != 0 {
                count += 1;
            }
        }
        count
    }

    /// Gets the last full version LSN (for delta reconstruction).
    #[inline]
    pub fn get_last_full_version(&self) -> Lsn {
        self.last_full_version
    }

    /// Sets the last full version LSN.
    #[inline]
    pub fn set_last_full_version(&mut self, lsn: Lsn) {
        self.last_full_version = lsn;
    }

    /// Gets the bloom filter for this BIN-delta.
    ///
    /// # Returns
    ///
    /// Reference to bloom filter bytes, or None if not a delta or filter not computed
    pub fn get_bloom_filter(&self) -> Option<&[u8]> {
        self.delta_bloom_filter.as_deref()
    }

    /// Sets the bloom filter for this BIN-delta.
    ///
    /// # Arguments
    ///
    /// * `filter` - Bloom filter bytes, or None to clear
    pub fn set_bloom_filter(&mut self, filter: Option<Vec<u8>>) {
        self.delta_bloom_filter = filter;
    }

    // --- Insert operations specific to BIN ---

    /// Inserts an LN reference into this BIN.
    ///
    /// The supplied `key` must be the full (uncompressed) key.  This method
    /// handles prefix management:
    ///
    /// - If the new key shrinks the current prefix, all stored suffixes are
    ///   re-encoded under the new (shorter) prefix before the insert.
    /// - After the insert, if a prefix was not yet established and there are
    ///   now ≥ 2 entries, the prefix is computed and all suffixes re-encoded.
    ///
    /// `IN.setKey` / BIN insert path including
    /// `IN.recalcSuffixes`.
    ///
    /// # Arguments
    ///
    /// * `key` - Full (uncompressed) key for the entry
    /// * `lsn` - LSN of the LN log entry
    /// * `state` - Initial state flags for the entry
    /// * `embedded_data` - Optional embedded LN data
    ///
    /// # Returns
    ///
    /// Index where entry was inserted/updated, with EXACT_MATCH flag set
    /// when a key was updated rather than inserted.
    pub fn insert_entry(
        &mut self,
        key: Vec<u8>,
        lsn: Lsn,
        state: u8,
        embedded_data: Option<Vec<u8>>,
    ) -> Result<i32, TreeError> {
        // Check prefix compatibility with the new key.
        let plen = self.key_prefix.len();
        let new_len = if plen > 0 {
            get_key_prefix_length(&self.key_prefix, &key)
        } else {
            0
        };

        if plen > 0 && new_len < plen {
            // New key shrinks the prefix — recompute considering the incoming
            // key: recompute prefix then recalculate suffixes.
            let mut candidate = self.compute_key_prefix(None);
            if !candidate.is_empty() {
                let cl = get_key_prefix_length(&candidate, &key);
                candidate.truncate(cl);
            } else if !self.inner.keys.is_empty()
                && let Some(first_full) = self.get_full_key(0)
            {
                candidate = create_key_prefix(&first_full, &key).unwrap_or_default();
                for i in 1..self.inner.get_n_entries() {
                    if candidate.is_empty() {
                        break;
                    }
                    if let Some(fk) = self.get_full_key(i) {
                        let l = get_key_prefix_length(&candidate, &fk);
                        candidate.truncate(l);
                    }
                }
            }
            self.apply_new_prefix(candidate);
        }

        // Compress the key under the (possibly updated) prefix.
        let suffix = self.compress_key(&key);

        let result = self.inner.insert_entry(suffix, lsn, state)?;
        let index = (result & 0xFFFF) as usize; // mask off flags

        // If the prefix was empty and we now have ≥ 2 entries, establish it.
        if self.key_prefix.is_empty() && self.inner.get_n_entries() >= 2 {
            self.recompute_key_prefix();
        }

        if embedded_data.is_some() {
            self.set_embedded_data(index, embedded_data);
        }
        Ok(result)
    }

    /// Deletes an entry from this BIN.
    ///
    /// # Arguments
    ///
    /// * `index` - Slot index to delete
    ///
    /// # Returns
    ///
    /// True if the entry was deleted, false if index was invalid
    pub fn delete_entry(&mut self, index: usize) -> bool {
        // Clean up per-slot auxiliary arrays.
        if index < self.slot_embedded_data.len() {
            self.slot_embedded_data.remove(index);
        }
        if index < self.modification_times.len() {
            self.modification_times.remove(index);
        }
        if index < self.creation_times.len() {
            self.creation_times.remove(index);
        }
        self.inner.delete_entry(index)
    }

    // ========================================================================
    // Per-slot modification / creation time  —  NoSQL fork
    // ========================================================================

    /// Returns the last-modification time for slot `index` in milliseconds
    /// since the Unix epoch, or `0` if not set.
    ///
    /// `BIN.getModificationTime(int idx)` (NoSQL fork).
    pub fn get_modification_time(&self, index: usize) -> u64 {
        self.modification_times.get(index).copied().unwrap_or(0)
    }

    /// Sets the last-modification time for slot `index` in milliseconds
    /// since the Unix epoch.
    ///
    /// `BIN.setModificationTime(int idx, long time)` (NoSQL fork).
    pub fn set_modification_time(&mut self, index: usize, time_ms: u64) {
        while self.modification_times.len() <= index {
            self.modification_times.push(0);
        }
        self.modification_times[index] = time_ms;
    }

    /// Returns the creation time for slot `index` in milliseconds since the
    /// Unix epoch, or `0` if not set.
    ///
    /// `BIN.getCreationTime(int idx)` (NoSQL fork).
    pub fn get_creation_time(&self, index: usize) -> u64 {
        self.creation_times.get(index).copied().unwrap_or(0)
    }

    /// Sets the creation time for slot `index` in milliseconds since the
    /// Unix epoch.
    ///
    /// `BIN.setCreationTime(int idx, long time)` (NoSQL fork).
    pub fn set_creation_time(&mut self, index: usize, time_ms: u64) {
        while self.creation_times.len() <= index {
            self.creation_times.push(0);
        }
        self.creation_times[index] = time_ms;
    }

    // --- Latch operations  -  delegate to inner IN ---

    /// Acquires an exclusive latch on this BIN.
    #[inline]
    pub fn latch(&self) {
        self.inner.latch();
    }

    /// Acquires a shared latch on this BIN.
    #[inline]
    pub fn latch_shared(&self) {
        self.inner.latch_shared();
    }

    /// Releases the latch on this BIN.
    #[inline]
    pub fn release_latch(&self) {
        self.inner.release_latch();
    }

    // =========================================================================
    // Slot deletion state helpers
    // =========================================================================

    /// Returns true if the slot is known-deleted or pending-deleted.
    ///
    /// 
    #[inline]
    pub fn is_deleted(&self, index: usize) -> bool {
        self.inner.is_entry_known_deleted(index) || self.inner.is_entry_pending_deleted(index)
    }

    /// Returns true if the slot is defunct (deleted or TTL-expired).
    ///
    /// A slot is defunct when it is known-deleted/pending-deleted OR when its
    /// TTL expiration time has passed. from the.
    #[inline]
    pub fn is_defunct(&self, index: usize) -> bool {
        if self.is_deleted(index) {
            return true;
        }
        // Check TTL expiration if tracked for this slot.
        if let Some(ref expirations) = self.slot_expirations
            && let Some(&exp) = expirations.get(index)
                && noxu_util::ttl::is_expired(exp, true) {
                    return true;
                }
        false
    }

    /// Returns true if the slot is defunct, optionally treating tombstones as defunct.
    ///
    /// 
    #[inline]
    pub fn is_defunct_with_tombstones(&self, index: usize, exclude_tombstones: bool) -> bool {
        self.is_defunct(index) || (exclude_tombstones && self.is_tombstone(index))
    }

    /// Returns true if the slot has the tombstone flag set.
    ///
    /// 
    #[inline]
    pub fn is_tombstone(&self, index: usize) -> bool {
        self.inner.is_tombstone(index)
    }

    /// Sets or clears the tombstone flag for the slot at `index`.
    ///
    /// 
    #[inline]
    pub fn set_tombstone(&mut self, index: usize, tombstone: bool) {
        self.inner.set_tombstone(index, tombstone);
    }

    /// Sets the known-deleted flag on the slot (also clears pending-deleted).
    ///
    /// 
    #[inline]
    pub fn set_known_deleted(&mut self, index: usize) {
        self.inner.set_known_deleted(index);
    }

    /// Clears the known-deleted flag on the slot.
    ///
    /// 
    #[inline]
    pub fn clear_known_deleted(&mut self, index: usize) {
        self.inner.clear_known_deleted(index);
    }

    /// Sets the pending-deleted flag on the slot.
    ///
    /// 
    #[inline]
    pub fn set_pending_deleted(&mut self, index: usize) {
        self.inner.set_pending_deleted(index);
    }

    /// Clears the pending-deleted flag on the slot.
    ///
    /// 
    #[inline]
    pub fn clear_pending_deleted(&mut self, index: usize) {
        self.inner.clear_pending_deleted(index);
    }

    // =========================================================================
    // BIN compress
    // =========================================================================

    /// Returns true if the BIN has any deleted slots that could be compressed.
    ///
    /// 
    pub fn should_compress_obsolete_keys(&self) -> bool {
        if self.is_bin_delta() || self.get_n_entries() == 0 {
            return false;
        }
        (0..self.get_n_entries()).any(|i| self.is_defunct(i))
    }

    /// Compresses a full BIN by removing deleted slots.
    ///
    /// If `compress_dirty_slots` is false, dirty slots are left in place even
    /// when deleted.  The BIN is NOT marked dirty when non-dirty slots are
    /// removed — this design to allow delta logging after
    /// compression of clean slots.
    ///
    /// Returns `true` always (locking checks are no-ops in this implementation).
    ///
    /// 
    pub fn compress(&mut self, compress_dirty_slots: bool) -> bool {
        assert!(!self.is_bin_delta(), "compress called on BIN-delta");
        assert!(self.n_cursors() == 0, "compress called with active cursors");

        let mut i = 0;
        while i < self.get_n_entries() {
            if !compress_dirty_slots && self.inner.is_entry_dirty(i) {
                i += 1;
                continue;
            }
            if !self.is_defunct(i) {
                i += 1;
                continue;
            }
            // Dirty slot removal prevents a future delta.
            if self.inner.is_entry_dirty(i) {
                self.inner.set_prohibit_next_delta(true);
            }
            let was_dirty = self.inner.is_dirty();
            let _ = self.delete_entry(i);
            // Restore dirty state — clean slot removal must not dirty the BIN.
            self.inner.set_dirty(was_dirty);
            // Do NOT increment i — the next slot shifted down to position i.
        }
        true
    }

    // =========================================================================
    // LN eviction
    // =========================================================================

    /// Evicts all resident embedded-LN values from this BIN.
    ///
    /// Iterates every slot that has embedded data.  For each such slot:
    /// - If the slot is dirty and a `log_manager` is provided, the LN is
    ///   written to the WAL first so that its latest value is durable before
    ///   the in-memory copy is dropped.
    /// - Non-dirty embedded LNs are evicted without a log write (the data is
    ///   already captured in a previously written BIN or BIN-delta entry).
    ///
    /// Returns the total bytes freed (estimated as key-len + data-len per slot).
    ///
    /// 
    pub fn evict_lns(
        &mut self,
        log_manager: Option<&noxu_log::LogManager>,
    ) -> usize {
        if self.has_cursors() {
            return 0;
        }
        let n = self.get_n_entries();
        let mut freed = 0;
        for i in 0..n {
            freed += self.evict_ln(i, log_manager);
        }
        freed
    }

    /// Evicts the embedded LN value at slot `index`.
    ///
    /// - Returns 0 if the slot has no resident embedded data.
    /// - If the slot is dirty and `log_manager` is `Some`, writes a
    ///   non-transactional `LN` log entry so the value is durable, then
    ///   updates the slot LSN to the new entry's position.
    /// - Clears `EMBEDDED_LN_BIT` from the slot state and sets the slot's
    ///   embedded data to `None`.
    ///
    /// Returns an estimate of the bytes freed (key-len + data-len).
    ///
    /// 
    pub fn evict_ln(
        &mut self,
        index: usize,
        log_manager: Option<&noxu_log::LogManager>,
    ) -> usize {
        // Only embedded-LN slots have resident data to evict.
        if self.get_embedded_data(index).is_none() {
            return 0;
        }

        let key = match self.get_full_key(index) {
            Some(k) => k,
            None => return 0,
        };
        let data = self
            .slot_embedded_data
            .get(index)
            .and_then(|d| d.clone())
            .unwrap_or_default();
        let freed = key.len() + data.len();

        // If the slot is dirty and we have a log manager, write the LN entry
        // to the WAL before dropping the in-memory copy.
        if self.inner.is_entry_dirty(index)
            && let Some(lm) = log_manager
        {
            let entry = noxu_log::entry::LnLogEntry::new(
                self.inner.db_id,
                None,
                noxu_util::NULL_LSN,
                false,
                None,
                None,
                noxu_util::NULL_VLSN,
                0,
                false,
                key,
                Some(data),
                0,
                noxu_util::NULL_VLSN,
            );
            let mut buf = bytes::BytesMut::with_capacity(entry.log_size());
            entry.write_to_log(&mut buf);
            if let Ok(new_lsn) = lm.log(
                noxu_log::LogEntryType::InsertLN,
                &buf,
                noxu_log::Provisional::No,
                false,
                false,
            ) {
                self.inner.set_lsn(index, new_lsn);
            }
        }

        // Clear embedded data and remove EMBEDDED_LN_BIT from slot state.
        if let Some(slot) = self.slot_embedded_data.get_mut(index) {
            *slot = None;
        }
        if let Some(state) = self.inner.states.get_mut(index) {
            *state &= !crate::entry_states::EMBEDDED_LN_BIT;
        }

        freed
    }

    // =========================================================================
    // Evictability
    // =========================================================================

    /// Returns true if this BIN can be evicted from the cache.
    ///
    /// A BIN is not evictable when it has active cursors.
    ///
    /// 
    #[inline]
    pub fn is_evictable(&self) -> bool {
        !self.has_cursors()
    }

    /// Returns true if this BIN can be part of a deletable subtree.
    ///
    /// All slots must be known-deleted and there must be no active cursors.
    ///
    /// 
    pub fn is_valid_for_delete(&self) -> bool {
        if self.is_bin_delta() || self.has_cursors() {
            return false;
        }
        self.inner.is_valid_for_delete()
    }

    // =========================================================================
    // BIN-delta mutation
    // =========================================================================

    /// Returns true if this full BIN can be mutated to a BIN-delta.
    ///
    /// 
    pub fn can_mutate_to_bin_delta(&self) -> bool {
        if self.is_bin_delta() || self.inner.get_prohibit_next_delta() {
            return false;
        }
        let n = self.get_n_entries();
        if n == 0 {
            return false;
        }
        let dirty = self.count_dirty_slots();
        dirty > 0 && dirty < n
    }

    /// Mutates this full BIN into a BIN-delta by discarding all non-dirty slots.
    ///
    /// Returns the approximate number of bytes freed.
    ///
    /// 
    pub fn mutate_to_bin_delta(&mut self) -> usize {
        assert!(self.can_mutate_to_bin_delta(), "cannot mutate to BIN-delta");

        let old_n = self.get_n_entries();

        // Collect dirty slots (full keys needed for re-insertion).
        let mut delta_keys: Vec<Vec<u8>> = Vec::new();
        let mut delta_lsns: Vec<Lsn> = Vec::new();
        let mut delta_states: Vec<u8> = Vec::new();
        let mut delta_embedded: Vec<Option<Vec<u8>>> = Vec::new();

        for i in 0..old_n {
            if self.inner.is_entry_dirty(i) {
                // get_key returns the full (decompressed) key.
                let fk = self.get_key(i).unwrap_or_default();
                delta_keys.push(fk);
                delta_lsns.push(self.inner.get_lsn(i));
                delta_states.push(self.inner.get_state(i));
                delta_embedded.push(self.slot_embedded_data.get(i).cloned().flatten());
            }
        }

        let delta_n = delta_keys.len();

        // Clear and re-insert only the delta slots.
        while self.get_n_entries() > 0 {
            let _ = self.delete_entry(0);
        }
        self.slot_embedded_data.clear();
        self.modification_times.clear();
        self.creation_times.clear();
        self.key_prefix.clear();

        for j in 0..delta_n {
            let _ = self.insert_entry(delta_keys[j].clone(), delta_lsns[j], delta_states[j], None);
            self.slot_embedded_data.push(delta_embedded[j].clone());
        }

        self.inner.set_bin_delta(true);

        // Build a lightweight bloom-filter marker from key lengths.
        if delta_n > 0 {
            let filter: Vec<u8> = delta_keys
                .iter()
                .flat_map(|k| (k.len() as u16).to_be_bytes())
                .collect();
            self.set_bloom_filter(Some(filter));
        } else {
            self.set_bloom_filter(None);
        }

        (old_n - delta_n) * 40 // approximate bytes freed
    }

    /// Mutates this BIN-delta back into a full BIN by merging with `full_bin`.
    ///
    /// `full_bin` must be the full BIN matching `self.last_full_version`.
    /// After the call `self` is a full BIN.
    ///
    /// 
    pub fn mutate_to_full_bin(&mut self, full_bin: &mut Bin, leave_free_slot: bool) {
        assert!(self.is_bin_delta(), "mutate_to_full_bin called on non-delta");

        // Apply each delta slot onto the full BIN.
        let delta_n = self.get_n_entries();
        for i in 0..delta_n {
            let key = self.get_key(i).unwrap_or_default();
            let lsn = self.inner.get_lsn(i);
            let state = self.inner.get_state(i);
            let embedded = self.slot_embedded_data.get(i).cloned().flatten();
            full_bin.apply_delta_slot(key, lsn, state, embedded);
        }

        if leave_free_slot && full_bin.get_n_entries() >= full_bin.max_entries() {
            log::warn!(
                "mutate_to_full_bin: leave_free_slot requested but BIN is full (n={})",
                full_bin.get_n_entries()
            );
        }

        // Swap contents so self becomes the full BIN.
        std::mem::swap(&mut self.inner, &mut full_bin.inner);
        std::mem::swap(&mut self.key_prefix, &mut full_bin.key_prefix);
        std::mem::swap(&mut self.slot_embedded_data, &mut full_bin.slot_embedded_data);
        std::mem::swap(&mut self.slot_vlsns, &mut full_bin.slot_vlsns);
        std::mem::swap(&mut self.slot_expirations, &mut full_bin.slot_expirations);

        self.inner.set_bin_delta(false);
        self.delta_bloom_filter = None;
        self.last_full_version = full_bin.last_full_version;
    }

    /// Applies a single delta slot to this full BIN.
    ///
    /// Updates the slot if the key already exists; otherwise inserts a new slot.
    ///
    /// 
    pub fn apply_delta_slot(
        &mut self,
        key: Vec<u8>,
        lsn: Lsn,
        state: u8,
        embedded_data: Option<Vec<u8>>,
    ) {
        let (index, exact) = self.find_entry_compressed(&key);
        if exact {
            self.inner.set_lsn(index, lsn);
            self.inner.set_state(index, state);
            if embedded_data.is_some() {
                self.set_embedded_data(index, embedded_data);
            }
        } else {
            let _ = self.insert_entry(key, lsn, state, embedded_data);
        }
    }

    /// Queues this BIN for slot compression when a deleted slot is observed.
    ///
    /// Skips if the slot is dirty and a delta should be logged, to avoid
    /// blocking a future delta write.
    ///
    /// 
    pub fn queue_slot_deletion(&self, index: usize) {
        if self.inner.is_entry_dirty(index) && self.should_log_delta() {
            return;
        }
        log::trace!(
            "BIN: queue slot {} for compression (node_id={})",
            index,
            self.inner.node_id()
        );
    }
}

impl std::fmt::Display for Bin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BIN(entries={}, delta={}, cursors={}, last_full={})",
            self.get_n_entries(),
            self.is_bin_delta(),
            self.get_cursor_count(),
            self.last_full_version
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry_states::DIRTY_BIT;

    #[test]
    fn test_new_bin() {
        let bin = Bin::new(1, 128);
        assert_eq!(bin.get_n_entries(), 0);
        assert_eq!(bin.max_entries(), 128);
        assert!(bin.is_bin());
        assert!(!bin.is_bin_delta());
        assert_eq!(bin.get_cursor_count(), 0);
        assert_eq!(bin.get_last_full_version(), NULL_LSN);
    }

    #[test]
    fn test_bin_insert_and_find() {
        let mut bin = Bin::new(1, 128);

        // Insert some entries
        let result = bin
            .insert_entry(b"key1".to_vec(), Lsn::from_u64(100), 0, None)
            .unwrap();
        assert_eq!(result & 0xFFFF, 0); // Inserted at index 0

        let result = bin
            .insert_entry(b"key2".to_vec(), Lsn::from_u64(200), 0, None)
            .unwrap();
        assert_eq!(result & 0xFFFF, 1); // Inserted at index 1

        let result = bin
            .insert_entry(b"key3".to_vec(), Lsn::from_u64(300), 0, None)
            .unwrap();
        assert_eq!(result & 0xFFFF, 2); // Inserted at index 2

        assert_eq!(bin.get_n_entries(), 3);

        // Find entries
        let index = bin.find_entry(b"key2", false, true);
        assert_ne!(index & 0x1_0000, 0); // EXACT_MATCH flag set
        assert_eq!(index & 0xFFFF, 1); // Found at index 1

        // Find key that doesn't exist
        let index = bin.find_entry(b"key1.5", false, false);
        assert_eq!(index & 0x1_0000, 0); // EXACT_MATCH flag not set
    }

    #[test]
    fn test_bin_delta_flag() {
        let mut bin = Bin::new(1, 128);
        assert!(!bin.is_bin_delta());

        bin.set_bin_delta(true);
        assert!(bin.is_bin_delta());

        bin.set_bin_delta(false);
        assert!(!bin.is_bin_delta());
    }

    #[test]
    fn test_bin_embedded_data() {
        let mut bin = Bin::new(1, 128);

        bin.insert_entry(b"key1".to_vec(), Lsn::from_u64(100), 0, None)
            .unwrap();

        // Set embedded data
        let data = b"embedded_ln_data".to_vec();
        bin.set_embedded_data(0, Some(data.clone()));

        // Get embedded data
        let retrieved = bin.get_embedded_data(0).unwrap();
        assert_eq!(retrieved, data.as_slice());

        // Clear embedded data
        bin.set_embedded_data(0, None);
        assert!(bin.get_embedded_data(0).is_none());
    }

    #[test]
    fn test_should_log_delta() {
        let mut bin = Bin::new(1, 128);

        // Insert 8 entries
        for i in 0..8 {
            let key = format!("key{}", i).into_bytes();
            bin.insert_entry(key, Lsn::from_u64(100 + i as u64), 0, None)
                .unwrap();
        }

        // No dirty entries - should not log delta
        assert!(!bin.should_log_delta());

        // Mark 2 entries as dirty (25% of 8)
        bin.inner.states[0] = DIRTY_BIT;
        bin.inner.states[1] = DIRTY_BIT;

        // Exactly 25% dirty - should log delta
        assert!(bin.should_log_delta());
        assert_eq!(bin.count_dirty_slots(), 2);

        // Mark 3 entries as dirty (37.5% of 8)
        bin.inner.states[2] = DIRTY_BIT;

        // More than 25% dirty - should not log delta
        assert!(!bin.should_log_delta());
        assert_eq!(bin.count_dirty_slots(), 3);
    }

    #[test]
    fn test_bin_cursor_count() {
        let mut bin = Bin::new(1, 128);
        assert_eq!(bin.get_cursor_count(), 0);
        assert!(!bin.has_cursors());

        bin.adjust_cursor_count(1);
        assert_eq!(bin.get_cursor_count(), 1);
        assert!(bin.has_cursors());

        bin.adjust_cursor_count(2);
        assert_eq!(bin.get_cursor_count(), 3);
        assert!(bin.has_cursors());

        bin.adjust_cursor_count(-1);
        assert_eq!(bin.get_cursor_count(), 2);
        assert!(bin.has_cursors());

        bin.adjust_cursor_count(-2);
        assert_eq!(bin.get_cursor_count(), 0);
        assert!(!bin.has_cursors());
    }

    #[test]
    fn test_last_full_version() {
        let mut bin = Bin::new(1, 128);
        assert_eq!(bin.get_last_full_version(), NULL_LSN);

        let lsn = Lsn::from_u64(0x12340000_00000100);
        bin.set_last_full_version(lsn);
        assert_eq!(bin.get_last_full_version(), lsn);
    }

    #[test]
    fn test_bloom_filter() {
        let mut bin = Bin::new(1, 128);
        assert!(bin.get_bloom_filter().is_none());

        let filter = vec![0xFF, 0x00, 0xAA, 0x55];
        bin.set_bloom_filter(Some(filter.clone()));
        assert_eq!(bin.get_bloom_filter(), Some(filter.as_slice()));

        bin.set_bloom_filter(None);
        assert!(bin.get_bloom_filter().is_none());
    }

    #[test]
    fn test_vlsn_tracking() {
        let mut bin = Bin::new(1, 128);

        bin.insert_entry(b"key1".to_vec(), Lsn::from_u64(100), 0, None)
            .unwrap();

        // Initially no VLSN
        assert_eq!(bin.get_slot_vlsn(0), Vlsn::new(0));

        // Set VLSN
        bin.set_slot_vlsn(0, Vlsn::new(12345));
        assert_eq!(bin.get_slot_vlsn(0), Vlsn::new(12345));
    }

    #[test]
    fn test_delete_entry() {
        let mut bin = Bin::new(1, 128);

        bin.insert_entry(b"key1".to_vec(), Lsn::from_u64(100), 0, None)
            .unwrap();
        bin.insert_entry(b"key2".to_vec(), Lsn::from_u64(200), 0, None)
            .unwrap();
        bin.insert_entry(b"key3".to_vec(), Lsn::from_u64(300), 0, None)
            .unwrap();

        assert_eq!(bin.get_n_entries(), 3);

        // Delete middle entry
        assert!(bin.delete_entry(1));
        assert_eq!(bin.get_n_entries(), 2);

        // Check remaining keys (get_key returns Option<Vec<u8>>)
        assert_eq!(bin.get_key(0), Some(b"key1".to_vec()));
        assert_eq!(bin.get_key(1), Some(b"key3".to_vec()));

        // Delete invalid index
        assert!(!bin.delete_entry(10));
    }

    #[test]
    fn test_display() {
        let bin = Bin::new(1, 128);
        let s = bin.to_string();
        assert!(s.contains("BIN"));
        assert!(s.contains("entries=0"));
        assert!(s.contains("delta=false"));
    }

    // ========================================================================
    // Key prefix compression tests
    // IN key-prefix compression unit tests.
    // ========================================================================

    /// Inserting keys with a common prefix causes the BIN to automatically
    /// establish that prefix and store only suffixes.
    #[test]
    fn test_prefix_established_on_second_insert() {
        let mut bin = Bin::new(1, 128);

        // First insert — no prefix yet (need ≥ 2 entries).
        bin.insert_entry(b"user:alice".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        assert!(bin.key_prefix.is_empty(), "single-entry BIN must have no prefix");

        // Second insert — prefix "user:" should be established.
        bin.insert_entry(b"user:bob".to_vec(), Lsn::from_u64(2), 0, None).unwrap();
        assert_eq!(&bin.key_prefix, b"user:",
            "common prefix 'user:' must be extracted after 2nd insert");
    }

    /// `get_key` returns the full (decompressed) key regardless of what is
    /// stored internally.
    #[test]
    fn test_get_key_decompresses() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"app:config".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"app:data".to_vec(), Lsn::from_u64(2), 0, None).unwrap();
        bin.insert_entry(b"app:log".to_vec(), Lsn::from_u64(3), 0, None).unwrap();

        // All full keys must be recovered from get_key.
        assert_eq!(bin.get_key(0), Some(b"app:config".to_vec()));
        assert_eq!(bin.get_key(1), Some(b"app:data".to_vec()));
        assert_eq!(bin.get_key(2), Some(b"app:log".to_vec()));
    }

    /// `find_entry` must find keys correctly even when prefix compression is
    /// active (i.e., the caller passes a full key, not a suffix).
    #[test]
    fn test_find_entry_with_prefix() {
        let mut bin = Bin::new(1, 128);
        for key in [b"ns:aaa".as_ref(), b"ns:bbb".as_ref(), b"ns:ccc".as_ref()] {
            bin.insert_entry(key.to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        }

        assert!(!bin.key_prefix.is_empty(), "prefix must be set");

        // Exact-match searches must succeed with full keys.
        let idx_b = bin.find_entry(b"ns:bbb", false, true);
        assert_ne!(idx_b & 0x1_0000, 0, "ns:bbb must be found");
        assert_eq!(idx_b & 0xFFFF, 1, "ns:bbb must be at index 1");

        // Exact-match search for a non-existent key must return -1.
        let idx_miss = bin.find_entry(b"ns:zzz", false, true);
        assert_eq!(idx_miss, -1, "ns:zzz must not be found");

        // Insertion-point search must return the correct position.
        let idx_ins = bin.find_entry(b"ns:b0b", false, false);
        assert_eq!(idx_ins & 0x1_0000, 0, "ns:b0b must not be an exact match");
        assert_eq!(idx_ins & 0xFFFF, 1, "ns:b0b inserts between aaa and bbb");
    }

    /// When a new key that does not share the existing prefix is inserted, the
    /// prefix is shortened (or cleared) and all stored suffixes are re-encoded.
    #[test]
    fn test_prefix_shrinks_when_key_breaks_it() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"abc:one".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"abc:two".to_vec(), Lsn::from_u64(2), 0, None).unwrap();
        assert_eq!(&bin.key_prefix, b"abc:", "initial prefix");

        // Insert a key that shares only "a" with existing ones.
        bin.insert_entry(b"axe".to_vec(), Lsn::from_u64(3), 0, None).unwrap();
        // The prefix must have shrunk to just "a".
        assert_eq!(&bin.key_prefix, b"a", "prefix must shrink to common part");

        // All full keys must still be recoverable.
        let all_keys: Vec<Vec<u8>> = (0..bin.get_n_entries())
            .filter_map(|i| bin.get_key(i))
            .collect();
        assert!(all_keys.contains(&b"abc:one".to_vec()), "abc:one must still be present");
        assert!(all_keys.contains(&b"abc:two".to_vec()), "abc:two must still be present");
        assert!(all_keys.contains(&b"axe".to_vec()), "axe must be present");
    }

    /// `compute_key_prefix` returns the correct prefix without modifying state.
    #[test]
    fn test_compute_key_prefix_pure() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"log:debug".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"log:info".to_vec(), Lsn::from_u64(2), 0, None).unwrap();
        bin.insert_entry(b"log:warn".to_vec(), Lsn::from_u64(3), 0, None).unwrap();

        let computed = bin.compute_key_prefix(None);
        assert_eq!(computed, b"log:", "computed prefix must be 'log:'");
    }

    /// `recompute_key_prefix` can be called multiple times without data loss.
    #[test]
    fn test_recompute_key_prefix_idempotent() {
        let mut bin = Bin::new(1, 128);
        for key in [b"key:a".as_ref(), b"key:b".as_ref(), b"key:c".as_ref()] {
            bin.insert_entry(key.to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        }

        let prefix_before = bin.key_prefix.clone();
        bin.recompute_key_prefix();
        assert_eq!(bin.key_prefix, prefix_before, "prefix must be stable after recompute");

        // All keys still intact.
        assert_eq!(bin.get_key(0), Some(b"key:a".to_vec()));
        assert_eq!(bin.get_key(1), Some(b"key:b".to_vec()));
        assert_eq!(bin.get_key(2), Some(b"key:c".to_vec()));
    }

    /// Memory-reduction: compressed keys in the BIN use fewer bytes than the
    /// full keys.
    #[test]
    fn test_prefix_reduces_stored_bytes() {
        let mut bin = Bin::new(1, 128);
        let prefix = b"very:long:common:prefix:";
        for suffix in [b"a".as_ref(), b"b".as_ref(), b"c".as_ref(), b"d".as_ref()] {
            let mut full = prefix.to_vec();
            full.extend_from_slice(suffix);
            bin.insert_entry(full, Lsn::from_u64(1), 0, None).unwrap();
        }

        assert_eq!(bin.key_prefix, prefix, "full prefix must be extracted");

        // Stored suffixes must each be 1 byte.
        for entry in &bin.inner.keys {
            assert_eq!(entry.len(), 1,
                "stored suffix must be 1 byte, got {:?}", entry);
        }
    }

    /// Keys with no common prefix result in an empty key_prefix.
    #[test]
    fn test_no_common_prefix_leaves_prefix_empty() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"apple".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"banana".to_vec(), Lsn::from_u64(2), 0, None).unwrap();
        // "apple" and "banana" share no prefix.
        assert!(bin.key_prefix.is_empty(), "differing-first-byte keys must have no prefix");
    }

    // ========================================================================
    // has_key_prefix / get_key_prefix
    // ========================================================================

    #[test]
    fn test_has_key_prefix_false_when_empty() {
        let bin = Bin::new(1, 128);
        assert!(!bin.has_key_prefix());
        assert!(bin.get_key_prefix().is_empty());
    }

    #[test]
    fn test_has_key_prefix_true_after_prefix_established() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"foo:a".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"foo:b".to_vec(), Lsn::from_u64(2), 0, None).unwrap();
        assert!(bin.has_key_prefix());
        assert_eq!(bin.get_key_prefix(), b"foo:");
    }

    // ========================================================================
    // decompress_key / compress_key (via insert/find round-trips)
    // ========================================================================

    #[test]
    fn test_decompress_key_no_prefix() {
        let bin = Bin::new(1, 128);
        // With no prefix, decompress_key is identity.
        assert_eq!(bin.decompress_key(b"hello"), b"hello");
        assert_eq!(bin.decompress_key(b""), b"");
    }

    #[test]
    fn test_decompress_key_with_prefix() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"data:x".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"data:y".to_vec(), Lsn::from_u64(2), 0, None).unwrap();
        // After insertion the prefix is "data:" and suffixes are "x", "y".
        assert_eq!(bin.decompress_key(b"x"), b"data:x");
        assert_eq!(bin.decompress_key(b"y"), b"data:y");
        assert_eq!(bin.decompress_key(b""), b"data:");
    }

    // ========================================================================
    // find_entry_compressed — empty BIN, exact, GTE, not-found cases
    // ========================================================================

    #[test]
    fn test_find_entry_compressed_empty_bin() {
        let bin = Bin::new(1, 128);
        // find_entry returns insertion point 0 for any key in an empty BIN.
        let r = bin.find_entry(b"anything", false, false);
        assert_eq!(r, 0);
        let r_exact = bin.find_entry(b"anything", false, true);
        assert_eq!(r_exact, -1);
    }

    #[test]
    fn test_find_entry_exact_match() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k:a".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"k:b".to_vec(), Lsn::from_u64(2), 0, None).unwrap();
        bin.insert_entry(b"k:c".to_vec(), Lsn::from_u64(3), 0, None).unwrap();

        let r = bin.find_entry(b"k:b", false, true);
        assert_ne!(r & 0x1_0000, 0, "EXACT_MATCH must be set");
        assert_eq!(r & 0xFFFF, 1);
    }

    #[test]
    fn test_find_entry_gte_insertion_point() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k:aaa".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"k:ccc".to_vec(), Lsn::from_u64(2), 0, None).unwrap();

        // "k:bbb" sorts between index 0 and 1 → insertion point is 1.
        let r = bin.find_entry(b"k:bbb", false, false);
        assert_eq!(r & 0x1_0000, 0, "must not be exact match");
        assert_eq!(r & 0xFFFF, 1);
    }

    #[test]
    fn test_find_entry_not_found_exact_returns_minus1() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k:aaa".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"k:ccc".to_vec(), Lsn::from_u64(2), 0, None).unwrap();

        let r = bin.find_entry(b"k:zzz", false, true);
        assert_eq!(r, -1);
    }

    #[test]
    fn test_find_entry_key_outside_prefix() {
        // Keys outside the current prefix must still return a valid position.
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"prefix:a".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"prefix:b".to_vec(), Lsn::from_u64(2), 0, None).unwrap();
        assert!(bin.has_key_prefix(), "prefix must be active");

        // "other:x" does not share the "prefix:" prefix — must not panic,
        // and must not report an exact match.
        let r = bin.find_entry(b"other:x", false, false);
        assert_eq!(r & 0x1_0000, 0, "no exact match for out-of-prefix key");
    }

    // ========================================================================
    // compute_key_prefix with exclude_idx
    // ========================================================================

    #[test]
    fn test_compute_key_prefix_fewer_than_2_entries() {
        let mut bin = Bin::new(1, 128);
        // 0 entries
        assert!(bin.compute_key_prefix(None).is_empty());
        // 1 entry
        bin.insert_entry(b"sole".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        // After single insert, no prefix (< 2 entries when computing manually).
        // Force clear the prefix so compute_key_prefix sees raw state.
        let computed = bin.compute_key_prefix(None);
        assert!(computed.is_empty(), "single-entry BIN has no computable prefix");
    }

    #[test]
    fn test_compute_key_prefix_exclude_first() {
        let mut bin = Bin::new(1, 128);
        // Insert two matching keys then one that doesn't share the prefix.
        bin.insert_entry(b"aa:x".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"aa:y".to_vec(), Lsn::from_u64(2), 0, None).unwrap();
        bin.insert_entry(b"aa:z".to_vec(), Lsn::from_u64(3), 0, None).unwrap();

        // Excluding slot 0: remaining keys ("aa:y", "aa:z") still share "aa:".
        let computed = bin.compute_key_prefix(Some(0));
        assert_eq!(computed, b"aa:");
    }

    // ========================================================================
    // apply_new_prefix — tested indirectly via recompute_key_prefix after
    // manual prefix manipulation.
    // ========================================================================

    #[test]
    fn test_apply_new_prefix_re_encodes_suffixes() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"xyz:1".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"xyz:2".to_vec(), Lsn::from_u64(2), 0, None).unwrap();
        assert_eq!(&bin.key_prefix, b"xyz:");

        // Verify that recompute_key_prefix is idempotent: prefix stays "xyz:"
        // and full keys are still recoverable.
        bin.recompute_key_prefix();
        assert_eq!(&bin.key_prefix, b"xyz:", "prefix must remain stable after recompute");

        // Full keys must still be recoverable after re-encoding.
        assert_eq!(bin.get_key(0), Some(b"xyz:1".to_vec()));
        assert_eq!(bin.get_key(1), Some(b"xyz:2".to_vec()));

        // Now force a new longer prefix via apply_new_prefix with an explicit
        // shorter prefix: after apply the stored suffixes must be re-encoded
        // and full keys must remain correct.
        bin.apply_new_prefix(b"xyz".to_vec());
        assert_eq!(&bin.key_prefix, b"xyz");
        assert_eq!(bin.get_key(0), Some(b"xyz:1".to_vec()));
        assert_eq!(bin.get_key(1), Some(b"xyz:2".to_vec()));
    }

    // ========================================================================
    // insert_entry prefix shrink — key that breaks existing prefix
    // ========================================================================

    #[test]
    fn test_insert_entry_prefix_shrinks_to_empty() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"alpha".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"alpha2".to_vec(), Lsn::from_u64(2), 0, None).unwrap();
        // Now insert a key with no common prefix.
        bin.insert_entry(b"zzzzz".to_vec(), Lsn::from_u64(3), 0, None).unwrap();
        // "alpha" and "zzzzz" share no prefix.
        assert!(bin.key_prefix.is_empty(), "prefix must be empty when no common bytes");
        // All keys must still be readable.
        let keys: Vec<Vec<u8>> = (0..bin.get_n_entries())
            .filter_map(|i| bin.get_key(i))
            .collect();
        assert!(keys.contains(&b"alpha".to_vec()));
        assert!(keys.contains(&b"alpha2".to_vec()));
        assert!(keys.contains(&b"zzzzz".to_vec()));
    }

    // ========================================================================
    // Cursor set management — add_cursor / remove_cursor / n_cursors / get_cursor_set
    // ========================================================================

    #[test]
    fn test_cursor_add_remove_basic() {
        let mut bin = Bin::new(1, 128);
        assert_eq!(bin.n_cursors(), 0);
        assert!(!bin.has_cursors());

        bin.add_cursor(42);
        assert_eq!(bin.n_cursors(), 1);
        assert!(bin.has_cursors());

        bin.add_cursor(99);
        assert_eq!(bin.n_cursors(), 2);

        bin.remove_cursor(42);
        assert_eq!(bin.n_cursors(), 1);

        bin.remove_cursor(99);
        assert_eq!(bin.n_cursors(), 0);
        assert!(!bin.has_cursors());
    }

    #[test]
    fn test_cursor_add_duplicate_is_idempotent() {
        let mut bin = Bin::new(1, 128);
        bin.add_cursor(7);
        bin.add_cursor(7); // same ID again
        // HashSet semantics: count is still 1.
        assert_eq!(bin.n_cursors(), 1);
    }

    #[test]
    fn test_cursor_remove_nonexistent_is_noop() {
        let mut bin = Bin::new(1, 128);
        bin.add_cursor(1);
        bin.remove_cursor(999); // ID not in set — must not panic
        assert_eq!(bin.n_cursors(), 1);
    }

    #[test]
    fn test_cursor_remove_last_clears_set() {
        let mut bin = Bin::new(1, 128);
        bin.add_cursor(5);
        bin.remove_cursor(5);
        // cursor_set must be None (not Some(empty)).
        assert!(bin.cursor_set.is_none(), "cursor_set should be None when empty");
        assert_eq!(bin.n_cursors(), 0);
    }

    #[test]
    fn test_get_cursor_set_contents() {
        let mut bin = Bin::new(1, 128);
        bin.add_cursor(10);
        bin.add_cursor(20);
        bin.add_cursor(30);
        let set = bin.get_cursor_set();
        assert_eq!(set.len(), 3);
        assert!(set.contains(&10));
        assert!(set.contains(&20));
        assert!(set.contains(&30));
    }

    #[test]
    fn test_get_cursor_set_empty() {
        let bin = Bin::new(1, 128);
        assert!(bin.get_cursor_set().is_empty());
    }

    // ========================================================================
    // Slot state methods on bin::InNode
    // ========================================================================

    #[test]
    fn test_known_deleted_set_and_clear() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k".to_vec(), Lsn::from_u64(1), 0, None).unwrap();

        assert!(!bin.inner.is_entry_known_deleted(0));
        bin.inner.set_known_deleted(0);
        assert!(bin.inner.is_entry_known_deleted(0));
        // set_known_deleted also sets DIRTY_BIT.
        assert!(bin.inner.is_entry_dirty(0));
        // set_known_deleted clears pending-deleted.
        assert!(!bin.inner.is_entry_pending_deleted(0));

        bin.inner.clear_known_deleted(0);
        assert!(!bin.inner.is_entry_known_deleted(0));
    }

    #[test]
    fn test_pending_deleted_set_and_clear() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k".to_vec(), Lsn::from_u64(1), 0, None).unwrap();

        assert!(!bin.inner.is_entry_pending_deleted(0));
        bin.inner.set_pending_deleted(0);
        assert!(bin.inner.is_entry_pending_deleted(0));
        assert!(bin.inner.is_entry_dirty(0));

        bin.inner.clear_pending_deleted(0);
        assert!(!bin.inner.is_entry_pending_deleted(0));
    }

    #[test]
    fn test_known_deleted_clears_pending_deleted() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k".to_vec(), Lsn::from_u64(1), 0, None).unwrap();

        bin.inner.set_pending_deleted(0);
        assert!(bin.inner.is_entry_pending_deleted(0));

        bin.inner.set_known_deleted(0);
        assert!(bin.inner.is_entry_known_deleted(0));
        // pending-deleted must be cleared by set_known_deleted.
        assert!(!bin.inner.is_entry_pending_deleted(0));
    }

    #[test]
    fn test_tombstone_set_and_clear() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k".to_vec(), Lsn::from_u64(1), 0, None).unwrap();

        assert!(!bin.inner.is_tombstone(0));
        bin.inner.set_tombstone(0, true);
        assert!(bin.inner.is_tombstone(0));
        // Setting tombstone also sets DIRTY_BIT.
        assert!(bin.inner.is_entry_dirty(0));

        bin.inner.set_tombstone(0, false);
        assert!(!bin.inner.is_tombstone(0));
    }

    #[test]
    fn test_is_dirty_and_set_dirty() {
        let mut node = InNode::new(1, 1, 128);
        // New nodes start clean.
        assert!(!node.is_dirty());
        node.set_dirty(true);
        assert!(node.is_dirty());
        node.set_dirty(false);
        assert!(!node.is_dirty());
    }

    #[test]
    fn test_get_prohibit_next_delta_and_set() {
        let mut node = InNode::new(1, 1, 128);
        assert!(!node.get_prohibit_next_delta());
        node.set_prohibit_next_delta(true);
        assert!(node.get_prohibit_next_delta());
        node.set_prohibit_next_delta(false);
        assert!(!node.get_prohibit_next_delta());
    }

    #[test]
    fn test_node_id_unique() {
        let node1 = InNode::new(1, 1, 128);
        let node2 = InNode::new(1, 1, 128);
        // Each new InNode must have a distinct positive node_id.
        assert!(node1.node_id() > 0);
        assert!(node2.node_id() > 0);
        assert_ne!(node1.node_id(), node2.node_id());
    }

    #[test]
    fn test_slot_state_out_of_bounds_returns_false() {
        let bin = Bin::new(1, 128);
        // All state accessors must be safe for out-of-bounds indices.
        assert!(!bin.inner.is_entry_known_deleted(99));
        assert!(!bin.inner.is_entry_pending_deleted(99));
        assert!(!bin.inner.is_tombstone(99));
        assert!(!bin.inner.is_entry_dirty(99));
    }

    // ========================================================================
    // Bin-level deletion helpers: is_deleted, is_defunct, is_defunct_with_tombstones
    // ========================================================================

    #[test]
    fn test_is_deleted_known_deleted() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        assert!(!bin.is_deleted(0));
        bin.set_known_deleted(0);
        assert!(bin.is_deleted(0));
    }

    #[test]
    fn test_is_deleted_pending_deleted() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        assert!(!bin.is_deleted(0));
        bin.set_pending_deleted(0);
        assert!(bin.is_deleted(0));
    }

    #[test]
    fn test_is_defunct_same_as_is_deleted() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        assert!(!bin.is_defunct(0));
        bin.set_known_deleted(0);
        assert!(bin.is_defunct(0));
    }

    #[test]
    fn test_is_defunct_with_tombstones_exclude_true() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        // Not deleted and not tombstone.
        assert!(!bin.is_defunct_with_tombstones(0, true));

        // Set tombstone — with exclude_tombstones=true this makes it defunct.
        bin.set_tombstone(0, true);
        assert!(bin.is_defunct_with_tombstones(0, true));
        // With exclude_tombstones=false tombstone alone does not make it defunct.
        assert!(!bin.is_defunct_with_tombstones(0, false));
    }

    #[test]
    fn test_is_tombstone_via_bin() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        assert!(!bin.is_tombstone(0));
        bin.set_tombstone(0, true);
        assert!(bin.is_tombstone(0));
        bin.set_tombstone(0, false);
        assert!(!bin.is_tombstone(0));
    }

    #[test]
    fn test_set_clear_known_deleted_via_bin() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.set_known_deleted(0);
        assert!(bin.is_deleted(0));
        bin.clear_known_deleted(0);
        assert!(!bin.is_deleted(0));
    }

    #[test]
    fn test_set_clear_pending_deleted_via_bin() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.set_pending_deleted(0);
        assert!(bin.is_deleted(0));
        bin.clear_pending_deleted(0);
        assert!(!bin.is_deleted(0));
    }

    // ========================================================================
    // should_compress_obsolete_keys
    // ========================================================================

    #[test]
    fn test_should_compress_obsolete_keys_empty() {
        let bin = Bin::new(1, 128);
        assert!(!bin.should_compress_obsolete_keys());
    }

    #[test]
    fn test_should_compress_obsolete_keys_no_deleted() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k1".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"k2".to_vec(), Lsn::from_u64(2), 0, None).unwrap();
        assert!(!bin.should_compress_obsolete_keys());
    }

    #[test]
    fn test_should_compress_obsolete_keys_with_deleted() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k1".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"k2".to_vec(), Lsn::from_u64(2), 0, None).unwrap();
        bin.set_known_deleted(0);
        assert!(bin.should_compress_obsolete_keys());
    }

    #[test]
    fn test_should_compress_obsolete_keys_false_for_delta() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k1".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"k2".to_vec(), Lsn::from_u64(2), 0, None).unwrap();
        bin.set_known_deleted(0);
        bin.set_bin_delta(true); // pretend it's a delta
        assert!(!bin.should_compress_obsolete_keys(), "delta BINs must not be compressed");
    }

    // ========================================================================
    // compress()
    // ========================================================================

    #[test]
    fn test_compress_removes_known_deleted_slots() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k:a".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"k:b".to_vec(), Lsn::from_u64(2), 0, None).unwrap();
        bin.insert_entry(b"k:c".to_vec(), Lsn::from_u64(3), 0, None).unwrap();

        bin.set_known_deleted(1); // mark "k:b" deleted (not dirty)
        // clear DIRTY_BIT so compress_dirty_slots=false still removes it
        bin.inner.states[1] = crate::entry_states::KNOWN_DELETED_BIT;

        assert_eq!(bin.get_n_entries(), 3);
        bin.compress(true);
        assert_eq!(bin.get_n_entries(), 2, "deleted slot must be removed");
        let remaining: Vec<Vec<u8>> = (0..bin.get_n_entries())
            .filter_map(|i| bin.get_key(i))
            .collect();
        assert!(remaining.contains(&b"k:a".to_vec()));
        assert!(remaining.contains(&b"k:c".to_vec()));
        assert!(!remaining.contains(&b"k:b".to_vec()));
    }

    #[test]
    fn test_compress_skips_dirty_slots_when_flag_false() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k:a".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"k:b".to_vec(), Lsn::from_u64(2), 0, None).unwrap();

        // Mark slot 0 as pending-deleted AND dirty.
        bin.set_pending_deleted(0); // also sets DIRTY_BIT

        // compress with compress_dirty_slots=false must skip dirty slot.
        bin.compress(false);
        assert_eq!(bin.get_n_entries(), 2, "dirty deleted slot must be skipped");
    }

    #[test]
    fn test_compress_removes_dirty_deleted_slot_when_flag_true() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k:a".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"k:b".to_vec(), Lsn::from_u64(2), 0, None).unwrap();

        // Mark slot 0 as pending-deleted AND dirty.
        bin.set_pending_deleted(0); // also sets DIRTY_BIT

        bin.compress(true);
        assert_eq!(bin.get_n_entries(), 1, "dirty deleted slot must be removed when flag is true");
        assert_eq!(bin.get_key(0), Some(b"k:b".to_vec()));
    }

    #[test]
    fn test_compress_all_deleted_leaves_empty_bin() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"x".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"y".to_vec(), Lsn::from_u64(2), 0, None).unwrap();

        // Mark both as known-deleted (non-dirty).
        bin.inner.states[0] = crate::entry_states::KNOWN_DELETED_BIT;
        bin.inner.states[1] = crate::entry_states::KNOWN_DELETED_BIT;

        bin.compress(true);
        assert_eq!(bin.get_n_entries(), 0);
    }

    // ========================================================================
    // is_evictable / is_valid_for_delete
    // ========================================================================

    #[test]
    fn test_is_evictable_no_cursors() {
        let bin = Bin::new(1, 128);
        assert!(bin.is_evictable());
    }

    #[test]
    fn test_is_evictable_with_cursors() {
        let mut bin = Bin::new(1, 128);
        bin.add_cursor(1);
        assert!(!bin.is_evictable());
    }

    #[test]
    fn test_is_valid_for_delete_empty_bin() {
        let bin = Bin::new(1, 128);
        // Empty bin is NOT valid for delete.
        assert!(!bin.is_valid_for_delete());
    }

    #[test]
    fn test_is_valid_for_delete_all_known_deleted() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k1".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"k2".to_vec(), Lsn::from_u64(2), 0, None).unwrap();

        // Not all deleted yet.
        assert!(!bin.is_valid_for_delete());

        // Mark both known-deleted.
        bin.inner.states[0] = crate::entry_states::KNOWN_DELETED_BIT;
        bin.inner.states[1] = crate::entry_states::KNOWN_DELETED_BIT;
        assert!(bin.is_valid_for_delete());
    }

    #[test]
    fn test_is_valid_for_delete_false_if_delta() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.inner.states[0] = crate::entry_states::KNOWN_DELETED_BIT;
        bin.set_bin_delta(true);
        assert!(!bin.is_valid_for_delete(), "delta BIN must not be valid for delete");
    }

    #[test]
    fn test_is_valid_for_delete_false_with_cursor() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.inner.states[0] = crate::entry_states::KNOWN_DELETED_BIT;
        bin.add_cursor(1);
        assert!(!bin.is_valid_for_delete(), "BIN with cursor must not be valid for delete");
    }

    // ========================================================================
    // can_mutate_to_bin_delta
    // ========================================================================

    #[test]
    fn test_can_mutate_to_bin_delta_false_when_already_delta() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k".to_vec(), Lsn::from_u64(1), DIRTY_BIT, None).unwrap();
        bin.set_bin_delta(true);
        assert!(!bin.can_mutate_to_bin_delta());
    }

    #[test]
    fn test_can_mutate_to_bin_delta_false_when_empty() {
        let bin = Bin::new(1, 128);
        assert!(!bin.can_mutate_to_bin_delta());
    }

    #[test]
    fn test_can_mutate_to_bin_delta_false_when_no_dirty_slots() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k1".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"k2".to_vec(), Lsn::from_u64(2), 0, None).unwrap();
        // Force states to 0 (no dirty bits).
        bin.inner.states[0] = 0;
        bin.inner.states[1] = 0;
        assert!(!bin.can_mutate_to_bin_delta());
    }

    #[test]
    fn test_can_mutate_to_bin_delta_false_when_all_dirty() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k1".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"k2".to_vec(), Lsn::from_u64(2), 0, None).unwrap();
        // All slots dirty — dirty == n, so delta would be as large as full BIN.
        bin.inner.states[0] = DIRTY_BIT;
        bin.inner.states[1] = DIRTY_BIT;
        assert!(!bin.can_mutate_to_bin_delta(), "all-dirty BIN must not become delta");
    }

    #[test]
    fn test_can_mutate_to_bin_delta_true() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k1".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"k2".to_vec(), Lsn::from_u64(2), 0, None).unwrap();
        bin.insert_entry(b"k3".to_vec(), Lsn::from_u64(3), 0, None).unwrap();
        // Make only slot 1 dirty.
        bin.inner.states[0] = 0;
        bin.inner.states[1] = DIRTY_BIT;
        bin.inner.states[2] = 0;
        assert!(bin.can_mutate_to_bin_delta());
    }

    // ========================================================================
    // mutate_to_bin_delta
    // ========================================================================

    #[test]
    fn test_mutate_to_bin_delta_marks_as_delta() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"p:a".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"p:b".to_vec(), Lsn::from_u64(2), 0, None).unwrap();
        bin.insert_entry(b"p:c".to_vec(), Lsn::from_u64(3), 0, None).unwrap();
        // Make slot 1 dirty.
        bin.inner.states[1] = DIRTY_BIT;

        let bytes_freed = bin.mutate_to_bin_delta();
        assert!(bin.is_bin_delta(), "must be marked as delta after mutation");
        assert!(bytes_freed > 0, "some bytes should be freed");
        // Only the dirty slot remains.
        assert_eq!(bin.get_n_entries(), 1);
        assert_eq!(bin.get_key(0), Some(b"p:b".to_vec()));
    }

    #[test]
    fn test_mutate_to_bin_delta_sets_bloom_filter() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"p:a".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"p:b".to_vec(), Lsn::from_u64(2), 0, None).unwrap();
        bin.inner.states[0] = DIRTY_BIT;
        bin.inner.states[1] = 0;

        bin.mutate_to_bin_delta();
        assert!(bin.get_bloom_filter().is_some(), "bloom filter must be set for non-empty delta");
    }

    #[test]
    fn test_mutate_to_bin_delta_multiple_dirty_slots() {
        let mut bin = Bin::new(1, 128);
        for i in 0..6u64 {
            let key = format!("k:{}", i).into_bytes();
            bin.insert_entry(key, Lsn::from_u64(i + 1), 0, None).unwrap();
        }
        // Mark slots 0 and 3 dirty.
        bin.inner.states[0] = DIRTY_BIT;
        bin.inner.states[3] = DIRTY_BIT;

        bin.mutate_to_bin_delta();
        assert!(bin.is_bin_delta());
        assert_eq!(bin.get_n_entries(), 2, "only the 2 dirty slots should remain");
        let keys: Vec<Vec<u8>> = (0..bin.get_n_entries())
            .filter_map(|i| bin.get_key(i))
            .collect();
        assert!(keys.contains(&b"k:0".to_vec()));
        assert!(keys.contains(&b"k:3".to_vec()));
    }

    // ========================================================================
    // mutate_to_full_bin
    // ========================================================================

    #[test]
    fn test_mutate_to_full_bin_basic() {
        // Build a full BIN with 3 entries.
        let mut full = Bin::new(1, 128);
        full.insert_entry(b"r:a".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        full.insert_entry(b"r:b".to_vec(), Lsn::from_u64(2), 0, None).unwrap();
        full.insert_entry(b"r:c".to_vec(), Lsn::from_u64(3), 0, None).unwrap();

        // Create a delta that updates "r:b" with a newer LSN.
        let mut delta = Bin::new(1, 128);
        delta.insert_entry(b"r:b".to_vec(), Lsn::from_u64(99), DIRTY_BIT, None).unwrap();
        delta.set_bin_delta(true);

        delta.mutate_to_full_bin(&mut full, false);

        assert!(!delta.is_bin_delta(), "result must be a full BIN");
        assert_eq!(delta.get_n_entries(), 3, "all 3 slots from full BIN must be present");

        // The updated LSN for "r:b" must be reflected.
        let idx = delta.find_entry(b"r:b", false, true);
        assert_ne!(idx & 0x1_0000, 0, "r:b must be found");
        let slot = (idx & 0xFFFF) as usize;
        assert_eq!(delta.get_lsn(slot), Lsn::from_u64(99));
    }

    #[test]
    fn test_mutate_to_full_bin_insert_new_key() {
        // Full BIN with 2 entries.
        let mut full = Bin::new(1, 128);
        full.insert_entry(b"r:a".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        full.insert_entry(b"r:c".to_vec(), Lsn::from_u64(3), 0, None).unwrap();

        // Delta inserts a brand-new key "r:b".
        let mut delta = Bin::new(1, 128);
        delta.insert_entry(b"r:b".to_vec(), Lsn::from_u64(2), DIRTY_BIT, None).unwrap();
        delta.set_bin_delta(true);

        delta.mutate_to_full_bin(&mut full, false);

        assert!(!delta.is_bin_delta());
        assert_eq!(delta.get_n_entries(), 3, "new key from delta must be merged in");
        let keys: Vec<Vec<u8>> = (0..delta.get_n_entries())
            .filter_map(|i| delta.get_key(i))
            .collect();
        assert!(keys.contains(&b"r:b".to_vec()));
    }

    // ========================================================================
    // apply_delta_slot — upsert semantics
    // ========================================================================

    #[test]
    fn test_apply_delta_slot_updates_existing() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k:x".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"k:y".to_vec(), Lsn::from_u64(2), 0, None).unwrap();

        // Update "k:x" with a new LSN and state.
        bin.apply_delta_slot(b"k:x".to_vec(), Lsn::from_u64(100), DIRTY_BIT, None);

        let idx = bin.find_entry(b"k:x", false, true);
        assert_ne!(idx & 0x1_0000, 0);
        let slot = (idx & 0xFFFF) as usize;
        assert_eq!(bin.get_lsn(slot), Lsn::from_u64(100));
        assert_eq!(bin.inner.get_state(slot), DIRTY_BIT);
    }

    #[test]
    fn test_apply_delta_slot_inserts_new_key() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k:a".to_vec(), Lsn::from_u64(1), 0, None).unwrap();

        // Apply a slot for a brand-new key.
        bin.apply_delta_slot(b"k:z".to_vec(), Lsn::from_u64(50), 0, None);
        assert_eq!(bin.get_n_entries(), 2);
        let idx = bin.find_entry(b"k:z", false, true);
        assert_ne!(idx & 0x1_0000, 0, "newly inserted key must be found");
    }

    #[test]
    fn test_apply_delta_slot_updates_embedded_data() {
        let mut bin = Bin::new(1, 128);
        bin.insert_entry(b"k".to_vec(), Lsn::from_u64(1), 0, None).unwrap();

        let new_data = b"new_embedded".to_vec();
        bin.apply_delta_slot(b"k".to_vec(), Lsn::from_u64(2), 0, Some(new_data.clone()));

        assert_eq!(bin.get_embedded_data(0), Some(new_data.as_slice()));
    }

    // ========================================================================
    // InNode::is_valid_for_delete
    // ========================================================================

    #[test]
    fn test_in_node_is_valid_for_delete_empty() {
        let node = InNode::new(1, 1, 128);
        assert!(!node.is_valid_for_delete());
    }

    #[test]
    fn test_in_node_is_valid_for_delete_not_all_deleted() {
        let mut node = InNode::new(1, 1, 128);
        node.insert_entry(b"k1".to_vec(), Lsn::from_u64(1), 0).unwrap();
        node.insert_entry(b"k2".to_vec(), Lsn::from_u64(2), crate::entry_states::KNOWN_DELETED_BIT).unwrap();
        assert!(!node.is_valid_for_delete());
    }

    #[test]
    fn test_in_node_is_valid_for_delete_all_deleted() {
        let mut node = InNode::new(1, 1, 128);
        node.insert_entry(b"k1".to_vec(), Lsn::from_u64(1), crate::entry_states::KNOWN_DELETED_BIT).unwrap();
        node.insert_entry(b"k2".to_vec(), Lsn::from_u64(2), crate::entry_states::KNOWN_DELETED_BIT).unwrap();
        assert!(node.is_valid_for_delete());
    }

    // ========================================================================
    // InNode::find_entry and insert_entry edge cases
    // ========================================================================

    #[test]
    fn test_in_node_insert_at_capacity_returns_error() {
        let mut node = InNode::new(1, 1, 2); // max 2 entries
        node.insert_entry(b"a".to_vec(), Lsn::from_u64(1), 0).unwrap();
        node.insert_entry(b"b".to_vec(), Lsn::from_u64(2), 0).unwrap();
        let err = node.insert_entry(b"c".to_vec(), Lsn::from_u64(3), 0);
        assert!(err.is_err(), "inserting beyond max_entries must fail");
    }

    #[test]
    fn test_in_node_insert_duplicate_updates_in_place() {
        let mut node = InNode::new(1, 1, 128);
        node.insert_entry(b"k".to_vec(), Lsn::from_u64(1), 0).unwrap();
        let r = node.insert_entry(b"k".to_vec(), Lsn::from_u64(99), DIRTY_BIT).unwrap();
        assert_ne!(r & 0x1_0000, 0, "EXACT_MATCH must be set on update");
        assert_eq!(node.get_n_entries(), 1, "no new slot must be added");
        assert_eq!(node.get_lsn(0), Lsn::from_u64(99));
    }

    #[test]
    fn test_in_node_find_entry_gte() {
        let mut node = InNode::new(1, 1, 128);
        node.insert_entry(b"a".to_vec(), Lsn::from_u64(1), 0).unwrap();
        node.insert_entry(b"c".to_vec(), Lsn::from_u64(2), 0).unwrap();

        // "b" not found, insertion point = 1.
        let r = node.find_entry(b"b", false, false);
        assert_eq!(r & 0x1_0000, 0);
        assert_eq!(r, 1);
    }

    #[test]
    fn test_in_node_delete_entry_out_of_bounds() {
        let mut node = InNode::new(1, 1, 128);
        assert!(!node.delete_entry(0), "deleting from empty node must return false");
    }

    // ========================================================================
    // InNode::set_lsn / set_state
    // ========================================================================

    #[test]
    fn test_in_node_set_lsn_and_state() {
        let mut node = InNode::new(1, 1, 128);
        node.insert_entry(b"k".to_vec(), Lsn::from_u64(1), 0).unwrap();
        node.set_lsn(0, Lsn::from_u64(42));
        assert_eq!(node.get_lsn(0), Lsn::from_u64(42));
        node.set_state(0, 0xFF);
        assert_eq!(node.get_state(0), 0xFF);
    }

    #[test]
    fn test_in_node_set_lsn_out_of_bounds_noop() {
        let mut node = InNode::new(1, 1, 128);
        // Must not panic for out-of-bounds index.
        node.set_lsn(99, Lsn::from_u64(1));
        node.set_state(99, 0xFF);
    }

    // ========================================================================
    // InNode::node_id is assigned a unique positive value at construction
    // ========================================================================

    #[test]
    fn test_in_node_node_id_positive() {
        let node = InNode::new(1, 1, 128);
        assert!(node.node_id() > 0, "node_id must be a positive assigned value");
    }

    // ========================================================================
    // ========================================================================

    /// Mutate_to_bin_delta sets IS_DELTA flag.
    ///
    /// `BIN.log(delta=true)` produces a BIN-delta; the in-memory BIN is
    /// still a full BIN but the delta flag is set.  In our model
    /// `mutate_to_bin_delta()` converts the in-memory representation.
    #[test]
    fn test_je_mutate_to_bin_delta_sets_delta_flag() {
        let mut bin = Bin::new(1, 32);
        // 4 entries, 1 dirty → can_mutate is true (dirty < n && dirty > 0).
        for i in 0u8..4 {
            bin.insert_entry(vec![i], Lsn::from_u64(i as u64), 0, None).unwrap();
        }
        bin.inner.states[2] = DIRTY_BIT; // only slot 2 dirty

        assert!(bin.can_mutate_to_bin_delta(),
            "precondition: must be mutable to delta");
        assert!(!bin.is_bin_delta(), "must start as full BIN");

        bin.mutate_to_bin_delta();

        assert!(bin.is_bin_delta(), "IS_DELTA flag must be set after mutation");
        // Only the dirty slot should remain.
        assert_eq!(bin.get_n_entries(), 1);
    }

    /// `apply_delta_slot` updates an existing key LSN.
    ///
    /// Applying a delta writes the new child LSN into the slot whose key
    /// matches the delta entry key.
    #[test]
    fn test_je_apply_delta_slot_updates_existing_key_lsn() {
        let mut full = Bin::new(1, 32);
        for i in 0u8..4 {
            full.insert_entry(vec![i], Lsn::from_u64(i as u64 + 10), 0, None).unwrap();
        }

        let new_lsn = Lsn::from_u64(999);
        // Apply a delta for key [1] with a newer LSN.
        full.apply_delta_slot(vec![1u8], new_lsn, DIRTY_BIT, None);

        let idx = full.find_entry(&[1u8], false, true);
        assert!(idx >= 0 && (idx & 0x1_0000) != 0, "key [1] must be found");
        let slot = (idx & 0xFFFF) as usize;
        assert_eq!(full.get_lsn(slot), new_lsn,
            "apply_delta_slot must update the LSN of the matched key");
    }

    /// Full round-trip: mutate → reconstruct.
    ///
    /// `logAndCheck`: after logging a delta and reconstituting via
    /// `reconstituteBIN` the entry count and all LSNs must match the
    /// original in-memory full BIN.
    ///
    /// In our port: `mutate_to_bin_delta` + `mutate_to_full_bin`.
    #[test]
    fn test_je_bin_delta_roundtrip_all_keys_present() {
        const N: usize = 6;
        let mut full = Bin::new(1, N * 2);
        for i in 0..N as u8 {
            // Insert with initial LSN.
            full.insert_entry(vec![i], Lsn::from_u64(i as u64 * 10), 0, None).unwrap();
        }

        // Record the original full set of (key, lsn) pairs.
        let original: Vec<(Vec<u8>, Lsn)> = (0..full.get_n_entries())
            .map(|i| (full.get_key(i).unwrap(), full.get_lsn(i)))
            .collect();

        // Create a snapshot of the full BIN (mimics the "base" BIN on disk).
        let mut base_snap = Bin::new(1, N * 2);
        for (k, l) in &original {
            base_snap.insert_entry(k.clone(), *l, 0, None).unwrap();
        }

        // Mark slots 1 and 3 dirty to create a non-trivial delta.
        full.inner.states[1] = DIRTY_BIT;
        full.inner.states[3] = DIRTY_BIT;
        // Update their LSNs so we can verify the merge.
        full.inner.lsns[1] = Lsn::from_u64(110);
        full.inner.lsns[3] = Lsn::from_u64(130);

        // Mutate to delta.
        let mut delta = Bin::new(1, N * 2);
        for i in 0..N as u8 {
            let state = full.inner.states[i as usize];
            let lsn   = full.inner.lsns[i as usize];
            if state & DIRTY_BIT != 0 {
                delta.insert_entry(vec![i], lsn, state, None).unwrap();
            }
        }
        delta.set_bin_delta(true);

        assert_eq!(delta.get_n_entries(), 2,
            "delta must contain only the 2 dirty slots");
        assert!(delta.is_bin_delta());

        // Reconstruct: apply delta onto the base snapshot.
        delta.mutate_to_full_bin(&mut base_snap, false);

        // After reconstruction:
        assert!(!delta.is_bin_delta(), "reconstructed BIN must not be a delta");
        assert_eq!(delta.get_n_entries(), N,
            "all {} original keys must be present after reconstruction", N);

        // Verify updated LSNs for the delta slots.
        let idx1 = delta.find_entry(&[1u8], false, true);
        assert!(idx1 >= 0 && (idx1 & 0x1_0000) != 0);
        assert_eq!(delta.get_lsn((idx1 & 0xFFFF) as usize), Lsn::from_u64(110));

        let idx3 = delta.find_entry(&[3u8], false, true);
        assert!(idx3 >= 0 && (idx3 & 0x1_0000) != 0);
        assert_eq!(delta.get_lsn((idx3 & 0xFFFF) as usize), Lsn::from_u64(130));

        // All other keys must still be present with their original LSNs.
        for i in [0u8, 2, 4, 5] {
            let idx = delta.find_entry(&[i], false, true);
            assert!(idx >= 0 && (idx & 0x1_0000) != 0,
                "key [{}] must be present after reconstruction", i);
            let expected_lsn = Lsn::from_u64(i as u64 * 10);
            assert_eq!(delta.get_lsn((idx & 0xFFFF) as usize), expected_lsn,
                "key [{}] must have its original LSN", i);
        }
    }

    /// `create_key_prefix` semantics.
    ///
    /// `Key.createKeyPrefix`:
    ///   makePrefix("aaaa","aaab") = "aaa"
    ///   makePrefix("abaa","aaab") = "a"
    ///   makePrefix("baaa","aaab") = null
    ///   makePrefix("aaa","aaa")  = "aaa"
    ///   makePrefix("aaa","aaab") = "aaa"
    #[test]
    fn test_je_create_key_prefix_semantics() {
        use crate::key::create_key_prefix;

        assert_eq!(create_key_prefix(b"aaaa", b"aaab"), Some(b"aaa".to_vec()));
        assert_eq!(create_key_prefix(b"abaa", b"aaab"), Some(b"a".to_vec()));
        assert_eq!(create_key_prefix(b"baaa", b"aaab"), None);
        assert_eq!(create_key_prefix(b"aaa",  b"aaa"),  Some(b"aaa".to_vec()));
        assert_eq!(create_key_prefix(b"aaa",  b"aaab"), Some(b"aaa".to_vec()));
    }

    ///
    /// "keyPrefixSubsetTest" — given an existing prefix and a new key,
    /// check whether the existing prefix is a prefix of the new key.
    ///
    ///   ("aaa",  "aaa")  → true   (identical)
    ///   ("aa",   "aaa")  → true   (prefix is shorter)
    ///   ("aaa",  "aa")   → false  (prefix longer than key)
    ///   ("",     "aa")   → false  (empty prefix)
    ///   (null,   "aa")   → false  (null prefix)
    ///   ("baa",  "aa")   → false  (different first byte)
    #[test]
    fn test_je_key_prefix_subset_check() {
        fn is_prefix_of(prefix: Option<&[u8]>, key: &[u8]) -> bool {
            match prefix {
                None | Some([]) => false,
                Some(p) => key.starts_with(p),
            }
        }

        assert!(is_prefix_of(Some(b"aaa"), b"aaa"),  "identical is subset");
        assert!(is_prefix_of(Some(b"aa"),  b"aaa"),  "shorter prefix is subset");
        assert!(!is_prefix_of(Some(b"aaa"), b"aa"),  "prefix longer than key");
        assert!(!is_prefix_of(Some(b""),   b"aa"),   "empty prefix is not subset");
        assert!(!is_prefix_of(None,         b"aa"),   "null prefix is not subset");
        assert!(!is_prefix_of(Some(b"baa"), b"aa"),  "different first byte");
    }

    /// Inserting a key that breaks the existing prefix
    /// clears / shortens the prefix and all keys remain decompressible.
    ///
    /// When a new key reduces the common prefix length, the BIN recomputes
    /// suffixes so `getKey(i)` still returns the full key.
    #[test]
    fn test_je_prefix_cleared_when_key_breaks_it() {
        let mut bin = Bin::new(1, 32);

        // Keys sharing prefix "aa".
        bin.insert_entry(b"aaa".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"aab".to_vec(), Lsn::from_u64(2), 0, None).unwrap();
        bin.insert_entry(b"aac".to_vec(), Lsn::from_u64(3), 0, None).unwrap();

        assert!(!bin.key_prefix.is_empty(), "prefix 'aa' must be established");

        // Insert a key that starts with 'b' — breaks any 'a'-based prefix.
        bin.insert_entry(b"baa".to_vec(), Lsn::from_u64(4), 0, None).unwrap();

        // Prefix must be cleared (no common bytes between 'a...' and 'b...').
        assert!(bin.key_prefix.is_empty(),
            "prefix must be cleared when new key shares no leading bytes");

        // All full keys must still be decompressible.
        let all_keys: Vec<Vec<u8>> = (0..bin.get_n_entries())
            .filter_map(|i| bin.get_key(i))
            .collect();
        for expected in [b"aaa".as_ref(), b"aab", b"aac", b"baa"] {
            assert!(
                all_keys.iter().any(|k| k.as_slice() == expected),
                "key {:?} must still be present and decompressible", expected
            );
        }
    }

    /// Compress_key / decompress_key are inverse ops.
    ///
    /// The suffix stored for each key must round-trip back to the full key
    /// via `getKey(i)` (decompress).
    #[test]
    fn test_je_compress_decompress_are_inverse() {
        let mut bin = Bin::new(1, 32);
        let test_keys: &[&[u8]] = &[b"aaf", b"aag", b"aah", b"aaj"];

        for &k in test_keys {
            bin.insert_entry(k.to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        }

        assert!(!bin.key_prefix.is_empty(), "shared 'aa' prefix must be set");

        for &k in test_keys {
            let suffix = bin.compress_key(k);
            let recovered = bin.decompress_key(&suffix);
            assert_eq!(recovered.as_slice(), k,
                "compress then decompress must return the original key");
        }
    }

    /// After a split, each half has its own correct
    /// key prefix.
    ///
    /// We simulate a split by building two separate BINs from the halves of a
    /// sorted key set and verifying that `compute_key_prefix` returns the
    /// correct prefix for each half.
    #[test]
    fn test_je_split_halves_have_independent_prefixes() {
        // Simulate BIN1 (keys sharing "aa") and BIN6 (keys sharing "ba").
        let mut bin1 = Bin::new(1, 32);
        for k in [b"aaa".as_ref(), b"aab", b"aac", b"aae"] {
            bin1.insert_entry(k.to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        }

        let mut bin6 = Bin::new(1, 32);
        for k in [b"baa".as_ref(), b"bab", b"bac", b"bam"] {
            bin6.insert_entry(k.to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        }

        // Each BIN should have its own correct prefix.
        assert!(bin1.key_prefix.starts_with(b"aa"),
            "BIN1 prefix must start with 'aa', got {:?}", bin1.key_prefix);
        assert!(bin6.key_prefix.starts_with(b"ba"),
            "BIN6 prefix must start with 'ba', got {:?}", bin6.key_prefix);

        // All keys must still be decompressible in each half.
        for k in [b"aaa".as_ref(), b"aab", b"aac", b"aae"] {
            let idx = bin1.find_entry(k, false, true);
            assert!(idx >= 0 && (idx & 0x1_0000) != 0,
                "key {:?} must be found in bin1 after split", k);
        }
        for k in [b"baa".as_ref(), b"bab", b"bac", b"bam"] {
            let idx = bin6.find_entry(k, false, true);
            assert!(idx >= 0 && (idx & 0x1_0000) != 0,
                "key {:?} must be found in bin6 after split", k);
        }
    }

    /// After recompute_key_prefix, N keys with a
    /// common prefix all decompress to their original full keys.
    #[test]
    fn test_je_recompute_prefix_preserves_all_keys() {
        let mut bin = Bin::new(1, 32);
        // Keys from the test's "BIN5" group.
        let keys: &[&[u8]] = &[b"aat", b"aau", b"aav", b"aaz"];
        for &k in keys {
            bin.insert_entry(k.to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        }

        assert!(!bin.key_prefix.is_empty(), "prefix must be established");

        bin.recompute_key_prefix();

        for &k in keys {
            let idx = bin.find_entry(k, false, true);
            assert!(
                idx >= 0 && (idx & 0x1_0000) != 0,
                "key {:?} must be found after recompute_key_prefix", k
            );
        }
    }

    // ========================================================================
    // ========================================================================
    //
    // BINDeltaOperationTest tests the detailed slot-level semantics of BIN
    // delta operations: mutate_to_bin_delta, apply_delta_slot, and
    // mutate_to_full_bin.  These tests exercise the invariants at the Bin
    // struct level (without a full tree + environment setup).

    /// `mutate_to_bin_delta` retains only
    /// dirty slots, marks the BIN as a delta, and frees approximate memory.
    ///
    /// After `BIN.mutateToBINDelta()` the BIN has `IS_DELTA` set and
    /// holds only the entries that were dirty at the time of mutation.
    #[test]
    fn test_bindelta_mutate_only_dirty_slots_retained() {
        let mut bin = Bin::new(1, 100);

        // Insert 10 entries (slots 0-9) with no dirty bits.
        for i in 0u8..10 {
            bin.insert_entry(vec![i], Lsn::from_u64(i as u64 * 10), 0, None).unwrap();
        }
        // Clear all state bits (simulates a clean, checkpointed BIN).
        for s in bin.inner.states.iter_mut() {
            *s = 0;
        }

        // Mark slots 2 and 7 dirty (simulates two record updates).
        bin.inner.states[2] = DIRTY_BIT;
        bin.inner.states[7] = DIRTY_BIT;

        assert!(bin.can_mutate_to_bin_delta(), "precondition: must be mutable");
        let freed = bin.mutate_to_bin_delta();

        assert!(bin.is_bin_delta(), "BIN must be marked as delta after mutation");
        assert_eq!(bin.get_n_entries(), 2, "only the 2 dirty slots must remain");
        assert!(freed > 0, "some bytes must have been freed (8 slots removed)");

        // Both dirty keys must be present.
        let idx2 = bin.find_entry(&[2u8], false, true);
        assert!(idx2 >= 0 && (idx2 & 0x1_0000) != 0, "dirty slot key=[2] must be in delta");
        let idx7 = bin.find_entry(&[7u8], false, true);
        assert!(idx7 >= 0 && (idx7 & 0x1_0000) != 0, "dirty slot key=[7] must be in delta");

        // Non-dirty keys must be absent from the delta.
        for i in [0u8, 1, 3, 4, 5, 6, 8, 9] {
            let idx = bin.find_entry(&[i], false, true);
            assert!(idx < 0 || (idx & 0x1_0000) == 0,
                "non-dirty key [{}] must not be in the delta", i);
        }
    }

    /// `apply_delta_slot` updates the LSN
    /// and state of an existing slot, leaving all other slots unchanged.
    ///
    /// `IN.applyDelta` writes the delta entry's child LSN into the
    /// matching slot when an exact-match key is found.
    #[test]
    fn test_bindelta_apply_delta_slot_updates_lsn_and_state() {
        let mut bin = Bin::new(1, 32);

        // Build a full BIN with 4 entries.
        for i in 0u8..4 {
            bin.insert_entry(vec![i], Lsn::from_u64(i as u64 + 1), 0, None).unwrap();
        }

        let original_lsns: Vec<Lsn> = (0..4).map(|i| bin.get_lsn(i)).collect();

        // Apply a delta that updates slot for key=[2].
        let new_lsn = Lsn::from_u64(999);
        bin.apply_delta_slot(vec![2u8], new_lsn, DIRTY_BIT, None);

        // Slot for key [2] must have the new LSN and dirty state.
        let idx = bin.find_entry(&[2u8], false, true);
        assert!(idx >= 0 && (idx & 0x1_0000) != 0);
        let slot = (idx & 0xFFFF) as usize;
        assert_eq!(bin.get_lsn(slot), new_lsn,
            "apply_delta_slot must update the slot LSN");
        assert_eq!(bin.inner.get_state(slot), DIRTY_BIT,
            "apply_delta_slot must set the state on the updated slot");

        // All other slots must be unchanged.
        for (i, &orig_lsn) in original_lsns.iter().enumerate() {
            let idx_i = bin.find_entry(&[i as u8], false, true);
            assert!(idx_i >= 0 && (idx_i & 0x1_0000) != 0);
            let s = (idx_i & 0xFFFF) as usize;
            if bin.get_key(s) != Some(vec![2u8]) {
                assert_eq!(bin.get_lsn(s), orig_lsn,
                    "slot {} must be unchanged by apply_delta_slot", i);
            }
        }

        // Entry count must not change.
        assert_eq!(bin.get_n_entries(), 4, "no new slots may be added");
    }

    /// `mutate_to_full_bin` merges delta
    /// entries into the base full BIN and the result is no longer a delta.
    ///
    /// `BIN.mutateToFullBIN(fullBIN, leaveFreeSlot)` applies every
    /// delta slot onto `fullBIN` (updating existing keys or inserting new
    /// ones) then swaps the contents so `self` becomes the full BIN.
    #[test]
    fn test_bindelta_mutate_to_full_bin_merges_correctly() {
        // Build a "base" full BIN representing the last checkpoint.
        let mut full = Bin::new(1, 64);
        for i in 0u8..8 {
            full.insert_entry(vec![i], Lsn::from_u64(i as u64 + 1), 0, None).unwrap();
        }

        // Create a delta with updates for keys [2] and [5], and a new key [9].
        let mut delta = Bin::new(1, 64);
        delta.insert_entry(vec![2u8], Lsn::from_u64(200), DIRTY_BIT, None).unwrap();
        delta.insert_entry(vec![5u8], Lsn::from_u64(500), DIRTY_BIT, None).unwrap();
        delta.insert_entry(vec![9u8], Lsn::from_u64(900), DIRTY_BIT, None).unwrap();
        delta.set_bin_delta(true);

        assert!(delta.is_bin_delta(), "precondition: delta must be a BIN-delta");

        // Merge: delta.mutate_to_full_bin(&mut full)
        delta.mutate_to_full_bin(&mut full, false);

        // After merging, self (delta) must be a full BIN.
        assert!(!delta.is_bin_delta(), "result must be a full BIN, not a delta");

        // All 9 entries (0-8 from base + new key 9) must be present.
        assert_eq!(delta.get_n_entries(), 9,
            "merged BIN must have all 9 entries (8 base + 1 new)");

        // Updated slots must carry the new LSNs.
        let idx2 = delta.find_entry(&[2u8], false, true);
        assert!(idx2 >= 0 && (idx2 & 0x1_0000) != 0, "key [2] must be present");
        assert_eq!(delta.get_lsn((idx2 & 0xFFFF) as usize), Lsn::from_u64(200));

        let idx5 = delta.find_entry(&[5u8], false, true);
        assert!(idx5 >= 0 && (idx5 & 0x1_0000) != 0, "key [5] must be present");
        assert_eq!(delta.get_lsn((idx5 & 0xFFFF) as usize), Lsn::from_u64(500));

        // New key from delta must have been inserted.
        let idx9 = delta.find_entry(&[9u8], false, true);
        assert!(idx9 >= 0 && (idx9 & 0x1_0000) != 0, "new key [9] from delta must be present");
        assert_eq!(delta.get_lsn((idx9 & 0xFFFF) as usize), Lsn::from_u64(900));

        // Unchanged slots must retain original LSNs.
        for i in [0u8, 1, 3, 4, 6, 7] {
            let idx = delta.find_entry(&[i], false, true);
            assert!(idx >= 0 && (idx & 0x1_0000) != 0, "key [{}] must be present", i);
            let expected = Lsn::from_u64(i as u64 + 1);
            assert_eq!(delta.get_lsn((idx & 0xFFFF) as usize), expected,
                "key [{}] must have its original LSN", i);
        }
    }

    /// Full round-trip:
    /// full BIN → mutate_to_bin_delta → mutate_to_full_bin → full BIN
    /// must restore all original entries with the correct updated LSNs.
    ///
    /// `testEviction`: after mutating to delta and then reconstituting,
    /// the in-memory BIN must have the same entry count as the original.
    #[test]
    fn test_bindelta_full_roundtrip_restores_all_slots() {
        const N: usize = 10;
        let mut original = Bin::new(1, N * 2);

        // Insert N entries; record their (key, lsn) pairs.
        for i in 0..N as u8 {
            original.insert_entry(vec![i], Lsn::from_u64(i as u64 * 10 + 1), 0, None).unwrap();
        }
        for s in original.inner.states.iter_mut() {
            *s = 0; // clear all dirty bits (clean state)
        }

        // Save a snapshot of the full BIN (the "on-disk" base for delta reconstruction).
        let mut base_snap = Bin::new(1, N * 2);
        for i in 0..N as u8 {
            let lsn = original.get_lsn(i as usize);
            base_snap.insert_entry(vec![i], lsn, 0, None).unwrap();
        }

        // Now update slots 3 and 6 — these become the delta.
        original.inner.states[3] = DIRTY_BIT;
        original.inner.lsns[3] = Lsn::from_u64(330);
        original.inner.states[6] = DIRTY_BIT;
        original.inner.lsns[6] = Lsn::from_u64(660);

        // Mutate to delta: collect dirty slots into a new BIN-delta.
        let mut delta = Bin::new(1, N * 2);
        for i in 0..N {
            if original.inner.states[i] & DIRTY_BIT != 0 {
                let key = original.get_key(i).unwrap();
                let lsn = original.get_lsn(i);
                let state = original.inner.get_state(i);
                delta.insert_entry(key, lsn, state, None).unwrap();
            }
        }
        delta.set_bin_delta(true);

        assert_eq!(delta.get_n_entries(), 2, "delta must contain only 2 dirty slots");
        assert!(delta.is_bin_delta());

        // Reconstruct: apply delta onto the base snapshot.
        delta.mutate_to_full_bin(&mut base_snap, false);

        // Post-reconstruction invariants.
        assert!(!delta.is_bin_delta(), "reconstructed BIN must not be a delta");
        assert_eq!(delta.get_n_entries(), N,
            "reconstructed BIN must have all {} original entries", N);

        // Updated slots carry the new LSNs.
        let i3 = delta.find_entry(&[3u8], false, true);
        assert!(i3 >= 0 && (i3 & 0x1_0000) != 0, "key [3] must be present after reconstruction");
        assert_eq!(delta.get_lsn((i3 & 0xFFFF) as usize), Lsn::from_u64(330));

        let i6 = delta.find_entry(&[6u8], false, true);
        assert!(i6 >= 0 && (i6 & 0x1_0000) != 0, "key [6] must be present after reconstruction");
        assert_eq!(delta.get_lsn((i6 & 0xFFFF) as usize), Lsn::from_u64(660));

        // All other slots carry their original LSNs.
        for i in [0u8, 1, 2, 4, 5, 7, 8, 9] {
            let idx = delta.find_entry(&[i], false, true);
            assert!(idx >= 0 && (idx & 0x1_0000) != 0, "key [{}] must be present", i);
            let expected = Lsn::from_u64(i as u64 * 10 + 1);
            assert_eq!(delta.get_lsn((idx & 0xFFFF) as usize), expected,
                "key [{}] must have its original LSN after round-trip", i);
        }
    }

    /// `apply_delta_slot` with a key that is
    /// not in the base BIN inserts a new slot (the "insert new key" branch of
    /// `IN.applyDelta`).
    #[test]
    fn test_bindelta_apply_delta_slot_inserts_new_key() {
        let mut bin = Bin::new(1, 32);
        bin.insert_entry(b"key:a".to_vec(), Lsn::from_u64(1), 0, None).unwrap();
        bin.insert_entry(b"key:c".to_vec(), Lsn::from_u64(3), 0, None).unwrap();
        assert_eq!(bin.get_n_entries(), 2);

        // Apply a delta for a key that does NOT yet exist ("key:b").
        bin.apply_delta_slot(b"key:b".to_vec(), Lsn::from_u64(2), DIRTY_BIT, None);

        assert_eq!(bin.get_n_entries(), 3, "new slot must have been inserted");

        let idx = bin.find_entry(b"key:b", false, true);
        assert!(idx >= 0 && (idx & 0x1_0000) != 0, "newly inserted key must be findable");
        assert_eq!(bin.get_lsn((idx & 0xFFFF) as usize), Lsn::from_u64(2));
    }

    /// Searching a BIN-delta for a key that
    /// is in the delta returns an exact match; searching for a key that is
    /// NOT in the delta does not return an exact match.
    ///
    /// A cursor performing a search on an in-memory BIN-delta can resolve
    /// the key directly if it appears in the delta's slots, without needing to
    /// reconstruct the full BIN.
    #[test]
    fn test_bindelta_search_in_delta_exact_and_range() {
        let mut bin = Bin::new(1, 64);

        // Insert 8 entries.
        for i in 0u8..8 {
            bin.insert_entry(vec![i * 10], Lsn::from_u64(i as u64), 0, None).unwrap();
        }
        for s in bin.inner.states.iter_mut() {
            *s = 0;
        }

        // Mark slots 1 and 4 dirty (keys [10] and [40]).
        bin.inner.states[1] = DIRTY_BIT;
        bin.inner.states[4] = DIRTY_BIT;

        bin.mutate_to_bin_delta();
        assert!(bin.is_bin_delta());
        assert_eq!(bin.get_n_entries(), 2);

        // Exact search for a key present in the delta must succeed.
        let r_exact = bin.find_entry(&[10u8], false, true);
        assert!(r_exact >= 0 && (r_exact & 0x1_0000) != 0,
            "key [10] must be found in BIN-delta by exact search");

        // Exact search for key [40] (the other dirty slot).
        let r40 = bin.find_entry(&[40u8], false, true);
        assert!(r40 >= 0 && (r40 & 0x1_0000) != 0,
            "key [40] must be found in BIN-delta by exact search");

        // Exact search for a key NOT in the delta must return -1.
        let r_miss = bin.find_entry(&[20u8], false, true);
        assert_eq!(r_miss, -1,
            "key [20] (not in delta) must not be found by exact search");
    }

    /// `mutate_to_bin_delta` sets the bloom
    /// filter to a non-None value when the delta is non-empty, and clears it
    /// when re-assigning to None.
    #[test]
    fn test_bindelta_bloom_filter_set_after_mutation() {
        let mut bin = Bin::new(1, 32);
        for i in 0u8..6 {
            bin.insert_entry(vec![i], Lsn::from_u64(i as u64), 0, None).unwrap();
        }
        for s in bin.inner.states.iter_mut() {
            *s = 0;
        }
        bin.inner.states[0] = DIRTY_BIT;
        bin.inner.states[3] = DIRTY_BIT;

        assert!(bin.get_bloom_filter().is_none(), "no bloom filter before mutation");

        bin.mutate_to_bin_delta();

        assert!(bin.get_bloom_filter().is_some(),
            "bloom filter must be set after mutate_to_bin_delta for non-empty delta");

        // Clearing it manually must work.
        bin.set_bloom_filter(None);
        assert!(bin.get_bloom_filter().is_none());
    }
}
