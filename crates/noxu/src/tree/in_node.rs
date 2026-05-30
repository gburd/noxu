//! Internal Node (IN) implementation for Noxu DB B-tree.
//!
//! the core B-tree node structure.
//!
//! INs hold references to child INs or (for BINs) to LNs/embedded data.
//! Slot data is stored in parallel arrays for memory compactness.

use crate::latch::SharedLatch;
use crate::util::{Lsn, NULL_LSN};
use std::cmp::Ordering as CmpOrdering;
use thiserror::Error;

/// Error types for IN operations.
#[derive(Error, Debug)]
pub enum InError {
    #[error("Node is full (should have been split): entries={0}, max={1}")]
    NodeFull(usize, usize),

    #[error("Invalid slot index: {0}, entries={1}")]
    InvalidIndex(usize, usize),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Deserialization error: {0}")]
    Deserialization(String),
}

/// Level constants.
///
/// The mapping tree has levels in the 0x20000 -> 0x2ffff number space.
/// The main tree has levels in the 0x10000 -> 0x1ffff number space.
/// The duplicate tree levels are in 0 -> 0xffff number space.
pub const DBMAP_LEVEL: i32 = 0x20000;
pub const MAIN_LEVEL: i32 = 0x10000;
pub const LEVEL_MASK: i32 = 0x0ffff;
pub const MIN_LEVEL: i32 = -1;
pub const BIN_LEVEL: i32 = MAIN_LEVEL | 1;

/// findEntry result flags.
pub const EXACT_MATCH: i32 = 1 << 16;
pub const INSERT_SUCCESS: i32 = 1 << 17;

/// IN flag bits (transient, not persisted).
/// Entry state flag constants.
const IN_DIRTY_BIT: u32 = 0x1;
const IN_RECALC_TOGGLE_BIT: u32 = 0x2;
const IN_IS_ROOT_BIT: u32 = 0x4;
const IN_HAS_CACHED_CHILDREN_BIT: u32 = 0x8;
const IN_PRI2_LRU_BIT: u32 = 0x10;
const IN_DELTA_BIT: u32 = 0x20;
/// The node was fetched from disk via CacheMode::Unchanged and has not been
/// accessed with any other cache mode since.  Such BINs are evicted eagerly.
const IN_FETCHED_COLD_BIT: u32 = 0x40;
/// The IN is currently registered on the INList (in the cache).
const IN_RESIDENT_BIT: u32 = 0x80;
/// The next log write of this BIN must be a full BIN, not a delta.
const IN_PROHIBIT_NEXT_DELTA_BIT: u32 = 0x100;
/// Expiration values for this BIN are stored in hours (not minutes).
const IN_EXPIRATION_IN_HOURS: u32 = 0x200;
/// All subtree slots reflect the least value (used for subtree scanning).
const IN_SUBTREE_SLOTS_REFLECT_LEAST_VALUE: u32 = 0x400;

/// Entry state flags (persistent).
///
///
/// These constants mirror `crate::tree::entry_states` but are kept here for
/// in-module use so that methods inside `InNode` can reference them without
/// a long path.
pub mod entry_states {
    pub const KNOWN_DELETED_BIT: u8 = 0x01;
    pub const DIRTY_BIT: u8 = 0x02;
    /// Formerly MIGRATE_BIT.  Always transient; 0x04 is reserved forever.
    pub const MIGRATE_BIT: u8 = 0x04;
    pub const PENDING_DELETED_BIT: u8 = 0x08;
    pub const EMBEDDED_LN_BIT: u8 = 0x10;
    pub const NO_DATA_LN_BIT: u8 = 0x20;
    /// Transient flag: key must be re-written when this slot is next logged.
    pub const UPDATE_KEY_WHEN_LOGGED: u8 = 0x40;
    /// Tombstone: blind-deletion marker (extended capability).
    pub const TOMBSTONE_BIT: u8 = 0x80;

    /// Bits that are transient (cleared before persisting to disk).
    ///
    pub const TRANSIENT_BITS: u8 = MIGRATE_BIT | UPDATE_KEY_WHEN_LOGGED;
}

/// Default maximum entries per IN.
pub const DEFAULT_MAX_ENTRIES: usize = 128;

/// An Internal Node in the B+tree.
///
///
///
/// INs hold references to child INs or (for BINs) to LNs/embedded data.
/// Slot data is stored in parallel arrays for memory compactness.
///
/// # Explanation of KD (KnownDeleted) and PD (PendingDeleted) Entry Flags
///
/// **PD**: Set for all LN entries that are deleted, even before the LN is
/// committed. Used as an authoritative (transactionally correct) indication
/// that an LN is deleted. PD will be cleared if the txn for the deleted LN is
/// aborted.
///
/// **KD**: Set under special conditions for entries containing LNs which are known
/// to be obsolete. Not used for entries in an active/uncommitted transaction.
///
/// Note that IN.fetchLN will allow a FileNotFoundException when the PD or KD
/// flag is set on the entry, and will allow a NULL_LSN when the KD flag is set.
///
/// KD is set when the cleaner attempts to migrate an LN and discovers it is
/// deleted. We need KD because the INCompressor may not have run, and may not
/// have compressed the BIN. There's the danger that we'll try to fetch that entry,
/// and that the file was deleted by the cleaner.
///
/// PD is closely related and came about because of a cleaner optimization we make.
/// The cleaner considers all deleted LN log entries to be obsolete, without doing
/// a tree lookup.
pub struct InNode {
    /// Unique node identifier.
    node_id: i64,

    /// Node latch for concurrency control.
    latch: SharedLatch,

    /// IN flag bits (dirty, root, delta, etc.)  -  transient, not persisted.
    flags: u32,

    /// LSN of the last full version of this node logged.
    last_full_lsn: Lsn,

    /// LSN of the last delta version logged (BINs only, NULL_LSN for upper INs).
    last_delta_lsn: Lsn,

    /// Level in the tree. BINs are level MAIN_LEVEL|1, upper INs are higher.
    level: i32,

    /// Key that identifies this IN in its parent.
    /// Initially the key of the zeroth slot, but insertions prior to slot
    /// zero make this no longer true. Always equal to some key in the IN.
    identifier_key: Option<Vec<u8>>,

    /// Number of valid entries (slots) in this node.
    n_entries: usize,

    /// Maximum number of entries this node can hold.
    max_entries: usize,

    /// Keys for each slot. Only indices 0..n_entries are valid.
    /// With key prefixing, these would be key suffixes. Without prefixing,
    /// these are complete keys. For embedded LNs, the key may be combined
    /// with data as a two-part key.
    entry_keys: Vec<Option<Vec<u8>>>,

    /// LSN for each child/LN slot.
    entry_lsns: Vec<Lsn>,

    /// State flags for each slot (known-deleted, dirty, pending-deleted, etc.).
    entry_states: Vec<u8>,

    /// Database ID this IN belongs to.
    database_id: u64,

    /// Whether this IN is on the INList (in-memory).
    in_list_resident: bool,

    /// Cached in-memory size (bytes).
    in_memory_size: usize,

    /// Accumulated delta between the true memory size and the last value
    /// reported to the memory budget.  Kept small to avoid expensive
    /// per-operation budget updates.
    accumulated_delta: i64,

    /// Generation counter for access tracking (for evictor).
    generation: u64,

    /// Pin count: number of active operations that have pinned this IN and
    /// therefore prevent it from being evicted.
    pin_count: u32,
}

impl InNode {
    /// Creates a new IN with the specified parameters.
    ///
    /// # Arguments
    ///
    /// * `database_id` - The database ID this IN belongs to
    /// * `level` - The tree level (BIN_LEVEL for BINs, higher for upper INs)
    /// * `max_entries` - Maximum number of slots this IN can hold
    pub fn new(database_id: u64, level: i32, max_entries: usize) -> Self {
        // Upper INs use shared latches, BINs use exclusive-only latches
        let exclusive_only = (level & LEVEL_MASK) == 1;

        Self {
            node_id: 0, // Set by caller or during recovery
            latch: SharedLatch::named("InNode", exclusive_only),
            flags: 0,
            last_full_lsn: NULL_LSN,
            last_delta_lsn: NULL_LSN,
            level,
            identifier_key: None,
            n_entries: 0,
            max_entries,
            entry_keys: vec![None; max_entries],
            entry_lsns: vec![NULL_LSN; max_entries],
            entry_states: vec![0; max_entries],
            database_id,
            in_list_resident: false,
            in_memory_size: 0,
            accumulated_delta: 0,
            generation: 0,
            pin_count: 0,
        }
    }

    // ========================================================================
    // Node ID
    // ========================================================================

    /// Returns the unique node identifier.
    #[inline]
    pub fn node_id(&self) -> i64 {
        self.node_id
    }

    /// Sets the unique node identifier.
    #[inline]
    pub fn set_node_id(&mut self, node_id: i64) {
        self.node_id = node_id;
    }

    // ========================================================================
    // Level Queries
    // ========================================================================

    /// Returns the tree level of this node.
    #[inline]
    pub fn level(&self) -> i32 {
        self.level
    }

    /// Returns the normalized level (stripping MAIN_LEVEL/DBMAP_LEVEL bits).
    #[inline]
    pub fn normalized_level(&self) -> i32 {
        self.level & LEVEL_MASK
    }

    /// Returns true if this is a BIN (Bottom Internal Node).
    #[inline]
    pub fn is_bin(&self) -> bool {
        self.normalized_level() == 1
    }

    /// Returns true if this is an upper IN (not a BIN).
    #[inline]
    pub fn is_upper_in(&self) -> bool {
        !self.is_bin()
    }

    /// Returns true if this is at the dbmap level.
    #[inline]
    pub fn is_dbmap_level(&self) -> bool {
        (self.level & DBMAP_LEVEL) != 0
    }

    // ========================================================================
    // Flag Operations
    // ========================================================================

    /// Returns true if this node is dirty (has been modified).
    #[inline]
    pub fn is_dirty(&self) -> bool {
        (self.flags & IN_DIRTY_BIT) != 0
    }

    /// Sets or clears the dirty flag.
    #[inline]
    pub fn set_dirty(&mut self, dirty: bool) {
        if dirty {
            self.flags |= IN_DIRTY_BIT;
        } else {
            self.flags &= !IN_DIRTY_BIT;
        }
    }

    /// Clears the dirty flag.
    #[inline]
    pub fn clear_dirty(&mut self) {
        self.set_dirty(false);
    }

    /// Returns true if this node is the root.
    #[inline]
    pub fn is_root(&self) -> bool {
        (self.flags & IN_IS_ROOT_BIT) != 0
    }

    /// Sets or clears the root flag.
    #[inline]
    pub fn set_is_root(&mut self, is_root: bool) {
        if is_root {
            self.flags |= IN_IS_ROOT_BIT;
        } else {
            self.flags &= !IN_IS_ROOT_BIT;
        }
    }

    /// Returns true if this is a BIN delta.
    #[inline]
    pub fn is_bin_delta(&self) -> bool {
        (self.flags & IN_DELTA_BIT) != 0
    }

    /// Sets or clears the BIN delta flag.
    #[inline]
    pub fn set_bin_delta(&mut self, is_delta: bool) {
        if is_delta {
            self.flags |= IN_DELTA_BIT;
        } else {
            self.flags &= !IN_DELTA_BIT;
        }
    }

    /// Returns true if this node has cached children.
    #[inline]
    pub fn has_cached_children(&self) -> bool {
        (self.flags & IN_HAS_CACHED_CHILDREN_BIT) != 0
    }

    /// Sets or clears the has cached children flag.
    #[inline]
    pub fn set_has_cached_children(&mut self, has: bool) {
        if has {
            self.flags |= IN_HAS_CACHED_CHILDREN_BIT;
        } else {
            self.flags &= !IN_HAS_CACHED_CHILDREN_BIT;
        }
    }

    /// Returns true if this node is in the priority-2 LRU list.
    ///
    ///
    #[inline]
    pub fn is_in_pri2_lru(&self) -> bool {
        (self.flags & IN_PRI2_LRU_BIT) != 0
    }

    /// Sets or clears the priority-2 LRU flag.
    ///
    ///
    #[inline]
    pub fn set_in_pri2_lru(&mut self, value: bool) {
        if value {
            self.flags |= IN_PRI2_LRU_BIT;
        } else {
            self.flags &= !IN_PRI2_LRU_BIT;
        }
    }

    /// Returns true if this node was fetched with CacheMode::Unchanged and
    /// has not been accessed with any other mode since.
    ///
    ///
    #[inline]
    pub fn get_fetched_cold(&self) -> bool {
        (self.flags & IN_FETCHED_COLD_BIT) != 0
    }

    /// Sets or clears the fetched-cold flag.
    ///
    ///
    #[inline]
    pub fn set_fetched_cold(&mut self, val: bool) {
        if val {
            self.flags |= IN_FETCHED_COLD_BIT;
        } else {
            self.flags &= !IN_FETCHED_COLD_BIT;
        }
    }

    /// Returns true if the next log write of this BIN must be a full BIN
    /// (not a delta).
    ///
    ///
    #[inline]
    pub fn get_prohibit_next_delta(&self) -> bool {
        (self.flags & IN_PROHIBIT_NEXT_DELTA_BIT) != 0
    }

    /// Sets or clears the prohibit-next-delta flag.
    ///
    /// Only meaningful for BINs. Setting to `true` forces the next log write
    /// to be a full BIN. This is set (a) when deleting a slot and (b) when
    /// the cleaner marks a BIN dirty for migration.
    ///
    ///
    #[inline]
    pub fn set_prohibit_next_delta(&mut self, val: bool) {
        if !self.is_bin() {
            return;
        }
        if val {
            self.flags |= IN_PROHIBIT_NEXT_DELTA_BIT;
        } else {
            self.flags &= !IN_PROHIBIT_NEXT_DELTA_BIT;
        }
    }

    /// Returns true if expiration values for this BIN are in hours.
    ///
    ///
    #[inline]
    pub fn is_expiration_in_hours(&self) -> bool {
        (self.flags & IN_EXPIRATION_IN_HOURS) != 0
    }

    /// Sets or clears the expiration-in-hours flag.
    #[inline]
    pub fn set_expiration_in_hours(&mut self, hours: bool) {
        if hours {
            self.flags |= IN_EXPIRATION_IN_HOURS;
        } else {
            self.flags &= !IN_EXPIRATION_IN_HOURS;
        }
    }

    /// Returns true if this node is registered on the INList (resident in
    /// the cache).
    ///
    ///
    #[inline]
    pub fn get_in_list_resident(&self) -> bool {
        self.in_list_resident
    }

    /// Sets whether this node is registered on the INList.
    ///
    ///
    #[inline]
    pub fn set_in_list_resident(&mut self, resident: bool) {
        self.in_list_resident = resident;
        if resident {
            self.flags |= IN_RESIDENT_BIT;
        } else {
            self.flags &= !IN_RESIDENT_BIT;
        }
    }

    // ========================================================================
    // LSN Operations
    // ========================================================================

    /// Returns the LSN of the last full version logged.
    #[inline]
    pub fn last_full_lsn(&self) -> Lsn {
        self.last_full_lsn
    }

    /// Sets the LSN of the last full version logged.
    #[inline]
    pub fn set_last_full_lsn(&mut self, lsn: Lsn) {
        self.last_full_lsn = lsn;
    }

    /// Returns the LSN of the last delta version logged.
    #[inline]
    pub fn last_delta_lsn(&self) -> Lsn {
        self.last_delta_lsn
    }

    /// Sets the LSN of the last delta version logged.
    #[inline]
    pub fn set_last_delta_lsn(&mut self, lsn: Lsn) {
        self.last_delta_lsn = lsn;
    }

    // ========================================================================
    // Entry/Slot Access
    // ========================================================================

    /// Returns the number of valid entries in this node.
    #[inline]
    pub fn n_entries(&self) -> usize {
        self.n_entries
    }

    /// Returns the maximum number of entries this node can hold.
    #[inline]
    pub fn max_entries(&self) -> usize {
        self.max_entries
    }

    /// Returns the key at the given index.
    ///
    /// # Panics
    ///
    /// Panics if index >= n_entries or the key at the index is None.
    #[inline]
    pub fn get_key(&self, index: usize) -> &[u8] {
        assert!(
            index < self.n_entries,
            "index {} >= n_entries {}",
            index,
            self.n_entries
        );
        self.entry_keys[index].as_ref().expect("key should exist")
    }

    /// Returns the LSN at the given index.
    ///
    /// # Panics
    ///
    /// Panics if index >= n_entries.
    #[inline]
    pub fn get_lsn(&self, index: usize) -> Lsn {
        assert!(
            index < self.n_entries,
            "index {} >= n_entries {}",
            index,
            self.n_entries
        );
        self.entry_lsns[index]
    }

    /// Returns the state byte at the given index.
    ///
    /// # Panics
    ///
    /// Panics if index >= n_entries.
    #[inline]
    pub fn get_state(&self, index: usize) -> u8 {
        assert!(
            index < self.n_entries,
            "index {} >= n_entries {}",
            index,
            self.n_entries
        );
        self.entry_states[index]
    }

    /// Sets the key at the given index.
    ///
    /// # Panics
    ///
    /// Panics if index >= max_entries.
    #[inline]
    pub fn set_key(&mut self, index: usize, key: Vec<u8>) {
        assert!(
            index < self.max_entries,
            "index {} >= max_entries {}",
            index,
            self.max_entries
        );
        self.entry_keys[index] = Some(key);
    }

    /// Sets the LSN at the given index.
    ///
    /// # Panics
    ///
    /// Panics if index >= max_entries.
    #[inline]
    pub fn set_lsn(&mut self, index: usize, lsn: Lsn) {
        assert!(
            index < self.max_entries,
            "index {} >= max_entries {}",
            index,
            self.max_entries
        );
        self.entry_lsns[index] = lsn;
    }

    /// Sets the state byte at the given index.
    ///
    /// # Panics
    ///
    /// Panics if index >= max_entries.
    #[inline]
    pub fn set_state(&mut self, index: usize, state: u8) {
        assert!(
            index < self.max_entries,
            "index {} >= max_entries {}",
            index,
            self.max_entries
        );
        self.entry_states[index] = state;
    }

    /// Returns the identifier key (the key that identifies this IN in its parent).
    #[inline]
    pub fn identifier_key(&self) -> Option<&[u8]> {
        self.identifier_key.as_deref()
    }

    /// Sets the identifier key.
    #[inline]
    pub fn set_identifier_key(&mut self, key: Vec<u8>) {
        self.identifier_key = Some(key);
    }

    // ========================================================================
    // Entry State Queries
    // ========================================================================

    /// Returns true if the entry at the given index is known deleted.
    #[inline]
    pub fn is_entry_known_deleted(&self, index: usize) -> bool {
        Self::is_state_known_deleted(self.get_state(index))
    }

    /// Returns true if the entry at the given index is pending deleted.
    #[inline]
    pub fn is_entry_pending_deleted(&self, index: usize) -> bool {
        Self::is_state_pending_deleted(self.get_state(index))
    }

    /// Returns true if the entry at the given index is an embedded LN.
    #[inline]
    pub fn is_entry_embedded_ln(&self, index: usize) -> bool {
        Self::is_state_embedded_ln(self.get_state(index))
    }

    /// Returns true if the entry at the given index is a no-data LN.
    #[inline]
    pub fn is_entry_no_data_ln(&self, index: usize) -> bool {
        Self::is_state_no_data_ln(self.get_state(index))
    }

    /// Returns true if the entry at the given index is dirty.
    #[inline]
    pub fn is_entry_dirty(&self, index: usize) -> bool {
        Self::is_state_dirty(self.get_state(index))
    }

    // ========================================================================
    // Entry State Manipulation (Static Helpers)
    // ========================================================================

    /// Returns true if the given state has the known deleted bit set.
    #[inline]
    pub fn is_state_known_deleted(state: u8) -> bool {
        (state & entry_states::KNOWN_DELETED_BIT) != 0
    }

    /// Returns true if the given state has the pending deleted bit set.
    #[inline]
    pub fn is_state_pending_deleted(state: u8) -> bool {
        (state & entry_states::PENDING_DELETED_BIT) != 0
    }

    /// Returns true if the given state has the embedded LN bit set.
    #[inline]
    pub fn is_state_embedded_ln(state: u8) -> bool {
        (state & entry_states::EMBEDDED_LN_BIT) != 0
    }

    /// Returns true if the given state has the no-data LN bit set.
    #[inline]
    pub fn is_state_no_data_ln(state: u8) -> bool {
        (state & entry_states::NO_DATA_LN_BIT) != 0
    }

    /// Returns true if the given state has the dirty bit set.
    #[inline]
    pub fn is_state_dirty(state: u8) -> bool {
        (state & entry_states::DIRTY_BIT) != 0
    }

    /// Sets the known-deleted (KD) flag on the slot at `index`.
    ///
    /// Also clears the pending-deleted flag and marks the slot dirty, exactly
    /// as in `IN.setKnownDeleted`.
    pub fn set_known_deleted(&mut self, index: usize) {
        debug_assert!(index < self.n_entries);
        self.entry_states[index] |= entry_states::KNOWN_DELETED_BIT;
        self.entry_states[index] &= !entry_states::PENDING_DELETED_BIT;
        self.entry_states[index] |= entry_states::DIRTY_BIT;
        self.set_dirty(true);
    }

    /// Clears the known-deleted flag on the slot at `index` and marks it dirty.
    ///
    ///
    pub fn clear_known_deleted(&mut self, index: usize) {
        debug_assert!(index < self.n_entries);
        self.entry_states[index] &= !entry_states::KNOWN_DELETED_BIT;
        self.entry_states[index] |= entry_states::DIRTY_BIT;
        self.set_dirty(true);
    }

    /// Sets the pending-deleted flag on the slot at `index` and marks dirty.
    ///
    ///
    pub fn set_pending_deleted(&mut self, index: usize) {
        debug_assert!(index < self.n_entries);
        self.entry_states[index] |= entry_states::PENDING_DELETED_BIT;
        self.entry_states[index] |= entry_states::DIRTY_BIT;
        self.set_dirty(true);
    }

    /// Clears the pending-deleted flag on the slot at `index` and marks dirty.
    ///
    ///
    pub fn clear_pending_deleted(&mut self, index: usize) {
        debug_assert!(index < self.n_entries);
        self.entry_states[index] &= !entry_states::PENDING_DELETED_BIT;
        self.entry_states[index] |= entry_states::DIRTY_BIT;
        self.set_dirty(true);
    }

    /// Sets the embedded LN bit on the entry at the given index and marks dirty.
    ///
    ///
    pub fn set_embedded_ln(&mut self, index: usize) {
        debug_assert!(index < self.n_entries);
        self.entry_states[index] |= entry_states::EMBEDDED_LN_BIT;
        self.entry_states[index] |= entry_states::DIRTY_BIT;
        self.set_dirty(true);
    }

    /// Clears the embedded LN bit on the entry at the given index and marks dirty.
    ///
    ///
    pub fn clear_embedded_ln(&mut self, index: usize) {
        debug_assert!(index < self.n_entries);
        self.entry_states[index] &= !entry_states::EMBEDDED_LN_BIT;
        self.entry_states[index] |= entry_states::DIRTY_BIT;
        self.set_dirty(true);
    }

    /// Sets the no-data-LN bit on the entry at the given index and marks dirty.
    ///
    ///
    pub fn set_no_data_ln(&mut self, index: usize) {
        debug_assert!(index < self.n_entries);
        self.entry_states[index] |= entry_states::NO_DATA_LN_BIT;
        self.entry_states[index] |= entry_states::DIRTY_BIT;
        self.set_dirty(true);
    }

    /// Clears the no-data-LN bit on the entry at the given index and marks dirty.
    ///
    ///
    pub fn clear_no_data_ln(&mut self, index: usize) {
        debug_assert!(index < self.n_entries);
        self.entry_states[index] &= !entry_states::NO_DATA_LN_BIT;
        self.entry_states[index] |= entry_states::DIRTY_BIT;
        self.set_dirty(true);
    }

    /// Sets the entry dirty bit on the entry at the given index.
    #[inline]
    pub fn set_entry_dirty(&mut self, index: usize) {
        self.entry_states[index] |= entry_states::DIRTY_BIT;
    }

    /// Clears the entry dirty bit on the entry at the given index.
    #[inline]
    pub fn clear_entry_dirty(&mut self, index: usize) {
        self.entry_states[index] &= !entry_states::DIRTY_BIT;
    }

    /// Returns true if the slot has a tombstone flag set.
    ///
    ///
    #[inline]
    pub fn is_tombstone(&self, index: usize) -> bool {
        debug_assert!(index < self.n_entries);
        (self.entry_states[index] & entry_states::TOMBSTONE_BIT) != 0
    }

    /// Sets or clears the tombstone flag for the slot at `index`.
    ///
    /// Also marks the slot and node dirty.
    ///
    pub fn set_tombstone(&mut self, index: usize, tombstone: bool) {
        debug_assert!(index < self.n_entries);
        if tombstone {
            self.entry_states[index] |= entry_states::TOMBSTONE_BIT;
        } else {
            self.entry_states[index] &= !entry_states::TOMBSTONE_BIT;
        }
        self.entry_states[index] |= entry_states::DIRTY_BIT;
        self.set_dirty(true);
    }

    /// Returns true if the slot has the update-key-when-logged transient flag.
    ///
    ///
    #[inline]
    pub fn is_update_key_when_logged(&self, index: usize) -> bool {
        debug_assert!(index < self.n_entries);
        (self.entry_states[index] & entry_states::UPDATE_KEY_WHEN_LOGGED) != 0
    }

    /// Sets the update-key-when-logged flag on the slot at `index`.
    ///
    /// This transient flag tells the logger to re-encode the key when writing.
    ///
    pub fn set_update_key_when_logged(&mut self, index: usize) {
        debug_assert!(index < self.n_entries);
        self.entry_states[index] |= entry_states::UPDATE_KEY_WHEN_LOGGED;
    }

    /// Returns true if the slot has a non-empty embedded data payload.
    ///
    /// Returns `false` if the data is zero-length even though EMBEDDED_LN_BIT
    /// is set (NO_DATA_LN_BIT is set in that case).
    ///
    #[inline]
    pub fn have_embedded_data(&self, index: usize) -> bool {
        self.is_entry_embedded_ln(index) && !self.is_entry_no_data_ln(index)
    }

    /// Returns the number of slots with the EMBEDDED_LN_BIT set.
    ///
    ///
    pub fn get_num_embedded_lns(&self) -> usize {
        (0..self.n_entries).filter(|&i| self.is_entry_embedded_ln(i)).count()
    }

    // ========================================================================
    // Pin / Eviction
    // ========================================================================

    /// Increments the pin count, preventing this node from being evicted.
    ///
    ///
    pub fn pin(&mut self) {
        self.pin_count += 1;
    }

    /// Decrements the pin count.
    ///
    ///
    ///
    /// # Panics
    ///
    /// Panics (debug) if pin_count is already 0.
    pub fn unpin(&mut self) {
        debug_assert!(self.pin_count > 0, "unpin called with pin_count == 0");
        self.pin_count = self.pin_count.saturating_sub(1);
    }

    /// Returns true if this node is pinned (pin_count > 0).
    ///
    ///
    #[inline]
    pub fn is_pinned(&self) -> bool {
        self.pin_count > 0
    }

    /// Returns true if this upper-IN is evictable.
    ///
    /// An upper-IN is always evictable (the evictor decides whether to do so).
    /// BINs override this logic in `Bin::is_evictable`.
    ///
    ///
    #[inline]
    pub fn is_evictable(&self) -> bool {
        // Upper INs are always considered evictable at the IN level.
        // BINs have more conditions checked by Bin::is_evictable.
        true
    }

    // ========================================================================
    // Memory Accounting
    // ========================================================================

    /// Returns the cached in-memory size of this node (bytes).
    ///
    ///
    #[inline]
    pub fn get_in_memory_size(&self) -> usize {
        self.in_memory_size
    }

    /// Sets the cached in-memory size.
    #[inline]
    pub fn set_in_memory_size(&mut self, size: usize) {
        self.in_memory_size = size;
    }

    /// Returns the memory size that has been reported to the budget.
    ///
    /// This is the cached size minus the accumulated (un-reported) delta.
    ///
    #[inline]
    pub fn get_budgeted_memory_size(&self) -> i64 {
        self.in_memory_size as i64 - self.accumulated_delta
    }

    /// Resets the accumulated delta and returns the total memory size.
    ///
    /// Called during a checkpoint to flush pending memory-budget updates.
    ///
    pub fn reset_and_get_memory_size(&mut self) -> usize {
        self.accumulated_delta = 0;
        self.in_memory_size
    }

    // ========================================================================
    // BIN-specific: validate-subtree / is-valid-for-delete
    // ========================================================================

    /// Returns true if this node can be part of a deletable subtree.
    ///
    /// For BINs: true when all slots are known-deleted and no cursors are
    /// registered.
    ///
    /// For upper INs: true when the node has at most one valid entry (which
    /// should be handled by the caller before deletion).
    ///
    /// / `BIN.isValidForDelete()`.
    pub fn is_valid_for_delete(&self) -> bool {
        if self.is_bin_delta() {
            return false;
        }
        let num_valid = (0..self.n_entries)
            .filter(|&i| !self.is_entry_known_deleted(i))
            .count();
        num_valid == 0
    }

    /// Returns true if the subtree rooted at slot `index` is safe to delete.
    ///
    /// For a BIN this always returns true (BINs have no sub-tree to validate).
    /// For upper INs we conservatively check the slot count.
    ///
    ///
    pub fn validate_subtree_before_delete(&self, index: usize) -> bool {
        if index >= self.n_entries {
            // No entry here — trivially deletable.
            return true;
        }
        if self.is_bin() {
            return true;
        }
        // Upper IN: the sub-tree must contain at most one valid slot.
        // Full validation requires descending the tree (requires cache access),
        // so here we only do the lightweight check.
        self.n_entries <= 1
    }

    /// Returns whether inserting `key` into this BIN-delta requires first
    /// mutating to a full BIN.
    ///
    /// Two cases require a full BIN:
    /// 1. The node is already full (no free slot).
    /// 2. The key might already be present in the full BIN (blind insertions
    ///    skip the check, so they never need to mutate just for that reason).
    ///
    ///
    pub fn insert_must_mutate_to_full_bin(
        &self,
        key: &[u8],
        blind_insertion: bool,
    ) -> bool {
        if self.n_entries >= self.max_entries {
            return true;
        }
        if blind_insertion {
            return false;
        }
        // Non-blind: the key might already exist in the full BIN; must mutate
        // before inserting to get the authoritative picture.
        // We use the bloom filter (if present) for a fast negative check; in
        // absence of a filter we conservatively say "might be present".
        !self.is_bin_delta() || self.find_entry(key, false, false) < 0
    }

    // ========================================================================
    // Binary Search
    // ========================================================================

    /// Finds the entry in this IN for which key is LTE the key arg.
    ///
    /// Currently uses a binary search. This method guarantees that the key
    /// parameter is always the left hand parameter to the comparison.
    ///
    /// Note that the 0'th entry's key is treated specially in an upper IN.
    /// It always compares lower than any other key (virtual key behavior).
    ///
    /// # Arguments
    ///
    /// * `key` - The key to search for
    /// * `indicate_if_duplicate` - If true, EXACT_MATCH is OR'd onto the return
    ///   value if key is already present in this node
    /// * `exact` - If true, an exact match must be found
    ///
    /// # Returns
    ///
    /// - Offset for the entry that has a key LTE the arg. 0 if key is less than
    ///   the 1st entry.
    /// - -1 if `exact` is true and no exact match is found.
    /// - If `indicate_if_duplicate` is true and an exact match was found, then
    ///   EXACT_MATCH is OR'd onto the return value.
    pub fn find_entry(
        &self,
        key: &[u8],
        indicate_if_duplicate: bool,
        exact: bool,
    ) -> i32 {
        let mut high = self.n_entries as i32 - 1;
        let mut low: i32 = 0;
        let mut middle: i32;

        // Special Treatment of 0th Entry
        // -------------------------------
        // Upper INs are special in that they have an entry[0] where the key is a
        // virtual key in that it always compares lower than any other key.
        // BINs don't treat key[0] specially. But if the caller asked for an
        // exact match or to indicate duplicates, then use the key[0] and
        // forget about the special entry zero comparison.
        let entry_zero_special_compare =
            self.is_upper_in() && !exact && !indicate_if_duplicate;

        while low <= high {
            middle = (high + low) / 2;
            let cmp = if middle == 0 && entry_zero_special_compare {
                // The virtual key at slot 0 is always considered less than any
                // real search key. compare_keys(search_key, virtual_key_0)
                // therefore returns Greater: the search key is to the right.
                CmpOrdering::Greater
            } else {
                // Compare unsigned bytes
                Self::compare_keys(key, self.get_key(middle as usize))
            };

            match cmp {
                CmpOrdering::Less => {
                    high = middle - 1;
                }
                CmpOrdering::Greater => {
                    low = middle + 1;
                }
                CmpOrdering::Equal => {
                    let ret = if indicate_if_duplicate {
                        middle | EXACT_MATCH
                    } else {
                        middle
                    };

                    // If exact match required and the entry is known deleted, return -1
                    if ret >= 0
                        && exact
                        && self.is_entry_known_deleted((ret & 0xffff) as usize)
                    {
                        return -1;
                    } else {
                        return ret;
                    }
                }
            }
        }

        // No match found. Either return -1 if caller wanted exact matches
        // only, or return entry whose key is < search key.
        if exact { -1 } else { high }
    }

    /// Compares two keys using unsigned byte comparison.
    ///
    /// This is the default key comparison used by the.
    fn compare_keys(key1: &[u8], key2: &[u8]) -> CmpOrdering {
        let min_len = key1.len().min(key2.len());

        for i in 0..min_len {
            match key1[i].cmp(&key2[i]) {
                CmpOrdering::Less => return CmpOrdering::Less,
                CmpOrdering::Greater => return CmpOrdering::Greater,
                CmpOrdering::Equal => continue,
            }
        }

        // If all compared bytes are equal, the shorter key is less
        key1.len().cmp(&key2.len())
    }

    // ========================================================================
    // Insert/Delete Operations
    // ========================================================================

    /// Inserts a slot with the given key, lsn, and state into this IN,
    /// maintaining sorted order.
    ///
    /// If a slot with the same key already exists, returns an error or the
    /// index of the duplicate (depending on implementation needs).
    ///
    /// The state of the new slot is set as provided (typically DIRTY_BIT).
    ///
    /// # Returns
    ///
    /// - Ok(index | INSERT_SUCCESS) if the entry was successfully inserted
    /// - Ok(index) if the entry already exists (duplicate)
    /// - Err(InError::NodeFull) if the node is full
    pub fn insert_entry(
        &mut self,
        key: Vec<u8>,
        lsn: Lsn,
        state: u8,
    ) -> Result<i32, InError> {
        // Search without requiring an exact match, but do let us know the
        // index of the match if there is one.
        let index = self.find_entry(&key, true, false);

        if index >= 0 && (index & EXACT_MATCH) != 0 {
            // There is an exact match. Return the index without INSERT_SUCCESS flag.
            return Ok(index & !EXACT_MATCH);
        }

        // Check if node is full
        if self.n_entries >= self.max_entries {
            return Err(InError::NodeFull(self.n_entries, self.max_entries));
        }

        // There was no key match, so insert to the right of this entry.
        let insert_index = (index + 1) as usize;

        // Shift entries to the right if needed
        if insert_index < self.n_entries {
            self.shift_entries_right(insert_index);
        } else {
            self.n_entries += 1;
        }

        // Insert the new entry
        self.entry_keys[insert_index] = Some(key);
        self.entry_lsns[insert_index] = lsn;
        self.entry_states[insert_index] = state;

        // Mark the node as dirty
        self.set_dirty(true);

        Ok(insert_index as i32 | INSERT_SUCCESS)
    }

    /// Shifts entries to the right starting at the given index.
    fn shift_entries_right(&mut self, index: usize) {
        // Move entries [index..n_entries) to [index+1..n_entries+1)
        for i in (index..self.n_entries).rev() {
            self.entry_keys[i + 1] = self.entry_keys[i].take();
            self.entry_lsns[i + 1] = self.entry_lsns[i];
            self.entry_states[i + 1] = self.entry_states[i];
        }
        self.n_entries += 1;
    }

    /// Deletes the entry at the given index.
    ///
    /// Shifts all entries after the deleted entry to the left.
    ///
    /// # Returns
    ///
    /// - true if the entry was successfully deleted
    /// - false if the index is out of bounds
    pub fn delete_entry(&mut self, index: usize) -> bool {
        if index >= self.n_entries {
            return false;
        }

        // Shift entries to the left
        for i in index..self.n_entries - 1 {
            self.entry_keys[i] = self.entry_keys[i + 1].take();
            self.entry_lsns[i] = self.entry_lsns[i + 1];
            self.entry_states[i] = self.entry_states[i + 1];
        }

        // Clear the last entry
        let last_idx = self.n_entries - 1;
        self.entry_keys[last_idx] = None;
        self.entry_lsns[last_idx] = NULL_LSN;
        self.entry_states[last_idx] = 0;

        self.n_entries -= 1;
        self.set_dirty(true);

        true
    }

    /// Updates only the LSN at the given slot index.
    ///
    ///
    ///
    /// # Panics
    ///
    /// Panics if index >= n_entries.
    #[inline]
    pub fn update_entry_lsn(&mut self, index: usize, lsn: Lsn) {
        assert!(
            index < self.n_entries,
            "index {} >= n_entries {}",
            index,
            self.n_entries
        );
        self.entry_lsns[index] = lsn;
        self.set_dirty(true);
    }

    /// Returns the generation counter for this node.
    ///
    /// Used by the LRU evictor: a higher generation means more recently accessed.
    #[inline]
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// Sets the generation counter.
    #[inline]
    pub fn set_generation(&mut self, generation: u64) {
        self.generation = generation;
    }

    /// Increments and returns the generation counter.
    #[inline]
    pub fn bump_generation(&mut self) -> u64 {
        self.generation += 1;
        self.generation
    }

    /// Returns an approximate in-memory footprint of this node in bytes.
    ///
    /// Used by the evictor to estimate memory pressure. This is intentionally
    /// approximate: it counts the parallel slot arrays plus the key bytes held
    /// in each occupied slot.
    ///
    ///
    pub fn get_memory_size(&self) -> usize {
        // Fixed-size fields: node_id (8), flags (4), last_full_lsn (8),
        // last_delta_lsn (8), level (4), n_entries (8), max_entries (8),
        // database_id (8), in_list_resident (1), in_memory_size (8),
        // generation (8) = ~73 bytes of value fields + struct overhead.
        let mut size: usize = 128;

        // identifier_key heap allocation
        if let Some(ref k) = self.identifier_key {
            size += k.len();
        }

        // Per-slot arrays: each slot contributes one Option<Vec<u8>> (24 bytes
        // on 64-bit), one Lsn (8 bytes), one u8 state, rounded up.
        size += self.max_entries * (24 + 8 + 1);

        // Key bytes actually stored
        for i in 0..self.n_entries {
            if let Some(ref k) = self.entry_keys[i] {
                size += k.len();
            }
        }

        size
    }

    // ========================================================================
    // Split Support
    // ========================================================================

    /// Returns the split index (midpoint) for splitting this node.
    #[inline]
    pub fn split_index(&self) -> usize {
        self.n_entries / 2
    }

    /// Returns the key at the split point.
    ///
    /// This key will be the identifier key for the new right sibling.
    pub fn get_split_key(&self, split_idx: usize) -> Option<Vec<u8>> {
        if split_idx < self.n_entries {
            self.entry_keys[split_idx].clone()
        } else {
            None
        }
    }

    // ========================================================================
    // Latch Operations
    // ========================================================================

    /// Returns a reference to the latch for this node.
    ///
    /// The latch uses RAII guards (SharedLatchReadGuard/WriteGuard) for
    /// automatic release. Call `latch.acquire_exclusive()` or
    /// `latch.acquire_shared()` to obtain a guard.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let guard = node.latch().acquire_exclusive();
    /// // ... perform operations ...
    /// // guard is automatically released when it goes out of scope
    /// ```
    #[inline]
    pub fn latch(&self) -> &SharedLatch {
        &self.latch
    }

    // ========================================================================
    // Serialization
    // ========================================================================

    /// Returns the size in bytes of this IN when serialized to the log.
    ///
    /// This is a NEW Rust-native format, not -compatible.
    ///
    /// Format:
    /// - node_id: 8 bytes
    /// - database_id: 8 bytes
    /// - level: 4 bytes
    /// - last_full_lsn: 8 bytes
    /// - last_delta_lsn: 8 bytes
    /// - n_entries: 2 bytes
    /// - identifier_key_len: 2 bytes
    /// - identifier_key: variable
    /// - For each entry:
    ///   - key_len: 2 bytes
    ///   - key: variable
    ///   - lsn: 8 bytes
    ///   - state: 1 byte
    pub fn log_size(&self) -> usize {
        let mut size = 8 + 8 + 4 + 8 + 8 + 2; // Fixed fields

        // Identifier key
        size += 2; // length
        if let Some(ref id_key) = self.identifier_key {
            size += id_key.len();
        }

        // Entry data
        for i in 0..self.n_entries {
            size += 2; // key length
            if let Some(ref key) = self.entry_keys[i] {
                size += key.len();
            }
            size += 8; // lsn
            size += 1; // state
        }

        size
    }

    /// Writes this IN to the log buffer.
    ///
    /// This is a NEW Rust-native format, not -compatible.
    pub fn write_to_log(&self, buf: &mut Vec<u8>) {
        // Write fixed fields
        buf.extend_from_slice(&self.node_id.to_be_bytes());
        buf.extend_from_slice(&self.database_id.to_be_bytes());
        buf.extend_from_slice(&self.level.to_be_bytes());
        buf.extend_from_slice(&self.last_full_lsn.as_u64().to_be_bytes());
        buf.extend_from_slice(&self.last_delta_lsn.as_u64().to_be_bytes());
        buf.extend_from_slice(&(self.n_entries as u16).to_be_bytes());

        // Write identifier key
        if let Some(ref id_key) = self.identifier_key {
            buf.extend_from_slice(&(id_key.len() as u16).to_be_bytes());
            buf.extend_from_slice(id_key);
        } else {
            buf.extend_from_slice(&0u16.to_be_bytes());
        }

        // Write entries
        for i in 0..self.n_entries {
            // Key
            if let Some(ref key) = self.entry_keys[i] {
                buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
                buf.extend_from_slice(key);
            } else {
                buf.extend_from_slice(&0u16.to_be_bytes());
            }

            // LSN
            buf.extend_from_slice(&self.entry_lsns[i].as_u64().to_be_bytes());

            // State (clear transient bits before persisting)
            let persistent_state =
                self.entry_states[i] & !entry_states::TRANSIENT_BITS;
            buf.push(persistent_state);
        }
    }

    /// Reads an IN from the log buffer.
    ///
    /// This is a NEW Rust-native format, not -compatible.
    pub fn read_from_log(buf: &[u8], _level: i32) -> Result<Self, InError> {
        if buf.len() < 38 {
            return Err(InError::Deserialization(format!(
                "buffer too small: {} bytes",
                buf.len()
            )));
        }

        let mut offset = 0;

        // Read fixed fields
        let node_id =
            i64::from_be_bytes(buf[offset..offset + 8].try_into().map_err(
                |e| InError::Deserialization(format!("node_id: {}", e)),
            )?);
        offset += 8;

        let database_id =
            u64::from_be_bytes(buf[offset..offset + 8].try_into().map_err(
                |e| InError::Deserialization(format!("database_id: {}", e)),
            )?);
        offset += 8;

        let level_val =
            i32::from_be_bytes(buf[offset..offset + 4].try_into().map_err(
                |e| InError::Deserialization(format!("level: {}", e)),
            )?);
        offset += 4;

        let last_full_lsn = Lsn::from_u64(u64::from_be_bytes(
            buf[offset..offset + 8].try_into().map_err(|e| {
                InError::Deserialization(format!("last_full_lsn: {}", e))
            })?,
        ));
        offset += 8;

        let last_delta_lsn = Lsn::from_u64(u64::from_be_bytes(
            buf[offset..offset + 8].try_into().map_err(|e| {
                InError::Deserialization(format!("last_delta_lsn: {}", e))
            })?,
        ));
        offset += 8;

        let n_entries =
            u16::from_be_bytes(buf[offset..offset + 2].try_into().map_err(
                |e| InError::Deserialization(format!("n_entries: {}", e)),
            )?) as usize;
        offset += 2;

        // Read identifier key
        let id_key_len =
            u16::from_be_bytes(buf[offset..offset + 2].try_into().map_err(
                |e| InError::Deserialization(format!("id_key_len: {}", e)),
            )?) as usize;
        offset += 2;

        let identifier_key = if id_key_len > 0 {
            if offset + id_key_len > buf.len() {
                return Err(InError::Deserialization(
                    "id_key extends past buffer".into(),
                ));
            }
            let key = buf[offset..offset + id_key_len].to_vec();
            offset += id_key_len;
            Some(key)
        } else {
            None
        };

        // Calculate max_entries (we need at least n_entries)
        let max_entries = n_entries.max(DEFAULT_MAX_ENTRIES);

        // Create the IN
        let mut in_node = Self::new(database_id, level_val, max_entries);
        in_node.node_id = node_id;
        in_node.last_full_lsn = last_full_lsn;
        in_node.last_delta_lsn = last_delta_lsn;
        in_node.identifier_key = identifier_key;
        in_node.n_entries = n_entries;

        // Read entries
        for i in 0..n_entries {
            // Key length
            if offset + 2 > buf.len() {
                return Err(InError::Deserialization(format!(
                    "entry {} key_len past buffer",
                    i
                )));
            }
            let key_len = u16::from_be_bytes(
                buf[offset..offset + 2].try_into().map_err(|e| {
                    InError::Deserialization(format!(
                        "entry {} key_len: {}",
                        i, e
                    ))
                })?,
            ) as usize;
            offset += 2;

            // Key
            let key = if key_len > 0 {
                if offset + key_len > buf.len() {
                    return Err(InError::Deserialization(format!(
                        "entry {} key past buffer",
                        i
                    )));
                }
                let k = buf[offset..offset + key_len].to_vec();
                offset += key_len;
                Some(k)
            } else {
                None
            };

            // LSN
            if offset + 8 > buf.len() {
                return Err(InError::Deserialization(format!(
                    "entry {} lsn past buffer",
                    i
                )));
            }
            let lsn = Lsn::from_u64(u64::from_be_bytes(
                buf[offset..offset + 8].try_into().map_err(|e| {
                    InError::Deserialization(format!("entry {} lsn: {}", i, e))
                })?,
            ));
            offset += 8;

            // State
            if offset + 1 > buf.len() {
                return Err(InError::Deserialization(format!(
                    "entry {} state past buffer",
                    i
                )));
            }
            let state = buf[offset];
            offset += 1;

            in_node.entry_keys[i] = key;
            in_node.entry_lsns[i] = lsn;
            in_node.entry_states[i] = state;
        }

        Ok(in_node)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_in_node() {
        let in_node = InNode::new(42, BIN_LEVEL, 128);

        assert_eq!(in_node.database_id, 42);
        assert_eq!(in_node.level(), BIN_LEVEL);
        assert_eq!(in_node.n_entries(), 0);
        assert_eq!(in_node.max_entries(), 128);
        assert!(in_node.is_bin());
        assert!(!in_node.is_upper_in());
        assert!(!in_node.is_dirty());
        assert!(!in_node.is_root());
    }

    #[test]
    fn test_level_queries() {
        let bin = InNode::new(1, BIN_LEVEL, 128);
        assert!(bin.is_bin());
        assert!(!bin.is_upper_in());
        assert_eq!(bin.normalized_level(), 1);

        let upper = InNode::new(1, MAIN_LEVEL | 2, 128);
        assert!(!upper.is_bin());
        assert!(upper.is_upper_in());
        assert_eq!(upper.normalized_level(), 2);

        let dbmap = InNode::new(1, DBMAP_LEVEL | 1, 128);
        assert!(dbmap.is_dbmap_level());
    }

    #[test]
    fn test_flag_operations() {
        let mut in_node = InNode::new(1, BIN_LEVEL, 128);

        assert!(!in_node.is_dirty());
        in_node.set_dirty(true);
        assert!(in_node.is_dirty());
        in_node.clear_dirty();
        assert!(!in_node.is_dirty());

        assert!(!in_node.is_root());
        in_node.set_is_root(true);
        assert!(in_node.is_root());
        in_node.set_is_root(false);
        assert!(!in_node.is_root());

        assert!(!in_node.is_bin_delta());
        in_node.set_bin_delta(true);
        assert!(in_node.is_bin_delta());
    }

    #[test]
    fn test_insert_and_find() {
        let mut in_node = InNode::new(1, BIN_LEVEL, 128);

        // Insert some keys
        let result = in_node.insert_entry(
            b"banana".to_vec(),
            Lsn::from_u64(100),
            entry_states::DIRTY_BIT,
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap() & !INSERT_SUCCESS, 0);

        let result = in_node.insert_entry(
            b"apple".to_vec(),
            Lsn::from_u64(200),
            entry_states::DIRTY_BIT,
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap() & !INSERT_SUCCESS, 0);

        let result = in_node.insert_entry(
            b"cherry".to_vec(),
            Lsn::from_u64(300),
            entry_states::DIRTY_BIT,
        );
        assert!(result.is_ok());
        assert_eq!(result.unwrap() & !INSERT_SUCCESS, 2);

        assert_eq!(in_node.n_entries(), 3);

        // Find exact matches
        let idx = in_node.find_entry(b"apple", true, true);
        assert_eq!(idx & 0xffff, 0);
        assert_eq!(idx & EXACT_MATCH, EXACT_MATCH);

        let idx = in_node.find_entry(b"banana", true, true);
        assert_eq!(idx & 0xffff, 1);
        assert_eq!(idx & EXACT_MATCH, EXACT_MATCH);

        let idx = in_node.find_entry(b"cherry", true, true);
        assert_eq!(idx & 0xffff, 2);
        assert_eq!(idx & EXACT_MATCH, EXACT_MATCH);

        // Find non-existent key (exact)
        let idx = in_node.find_entry(b"dog", false, true);
        assert_eq!(idx, -1);

        // Find non-existent key (inexact, should return entry < key)
        let idx = in_node.find_entry(b"blueberry", false, false);
        assert_eq!(idx, 1); // "banana" is < "blueberry"
    }

    #[test]
    fn test_insert_maintains_order() {
        let mut in_node = InNode::new(1, BIN_LEVEL, 128);

        // Insert out of order
        in_node
            .insert_entry(
                b"zebra".to_vec(),
                Lsn::from_u64(100),
                entry_states::DIRTY_BIT,
            )
            .unwrap();
        in_node
            .insert_entry(
                b"apple".to_vec(),
                Lsn::from_u64(200),
                entry_states::DIRTY_BIT,
            )
            .unwrap();
        in_node
            .insert_entry(
                b"monkey".to_vec(),
                Lsn::from_u64(300),
                entry_states::DIRTY_BIT,
            )
            .unwrap();

        assert_eq!(in_node.n_entries(), 3);

        // Verify sorted order
        assert_eq!(in_node.get_key(0), b"apple");
        assert_eq!(in_node.get_key(1), b"monkey");
        assert_eq!(in_node.get_key(2), b"zebra");

        assert_eq!(in_node.get_lsn(0), Lsn::from_u64(200));
        assert_eq!(in_node.get_lsn(1), Lsn::from_u64(300));
        assert_eq!(in_node.get_lsn(2), Lsn::from_u64(100));
    }

    #[test]
    fn test_delete_entry() {
        let mut in_node = InNode::new(1, BIN_LEVEL, 128);

        // Insert some entries
        in_node
            .insert_entry(
                b"apple".to_vec(),
                Lsn::from_u64(100),
                entry_states::DIRTY_BIT,
            )
            .unwrap();
        in_node
            .insert_entry(
                b"banana".to_vec(),
                Lsn::from_u64(200),
                entry_states::DIRTY_BIT,
            )
            .unwrap();
        in_node
            .insert_entry(
                b"cherry".to_vec(),
                Lsn::from_u64(300),
                entry_states::DIRTY_BIT,
            )
            .unwrap();

        assert_eq!(in_node.n_entries(), 3);

        // Delete the middle entry
        assert!(in_node.delete_entry(1));
        assert_eq!(in_node.n_entries(), 2);

        // Verify remaining entries are correct
        assert_eq!(in_node.get_key(0), b"apple");
        assert_eq!(in_node.get_key(1), b"cherry");
        assert_eq!(in_node.get_lsn(0), Lsn::from_u64(100));
        assert_eq!(in_node.get_lsn(1), Lsn::from_u64(300));
    }

    #[test]
    fn test_find_entry_exact_and_inexact() {
        let mut in_node = InNode::new(1, BIN_LEVEL, 128);

        in_node
            .insert_entry(
                b"b".to_vec(),
                Lsn::from_u64(100),
                entry_states::DIRTY_BIT,
            )
            .unwrap();
        in_node
            .insert_entry(
                b"d".to_vec(),
                Lsn::from_u64(200),
                entry_states::DIRTY_BIT,
            )
            .unwrap();
        in_node
            .insert_entry(
                b"f".to_vec(),
                Lsn::from_u64(300),
                entry_states::DIRTY_BIT,
            )
            .unwrap();

        // Exact match with flag
        let idx = in_node.find_entry(b"d", true, true);
        assert_eq!(idx & 0xffff, 1);
        assert_eq!(idx & EXACT_MATCH, EXACT_MATCH);

        // Exact match without flag
        let idx = in_node.find_entry(b"d", false, true);
        assert_eq!(idx, 1);

        // Inexact: search for "a" (less than all)
        let idx = in_node.find_entry(b"a", false, false);
        assert_eq!(idx, -1); // No entry less than "a"

        // Inexact: search for "c" (between b and d)
        let idx = in_node.find_entry(b"c", false, false);
        assert_eq!(idx, 0); // "b" is the entry < "c"

        // Inexact: search for "e" (between d and f)
        let idx = in_node.find_entry(b"e", false, false);
        assert_eq!(idx, 1); // "d" is the entry < "e"

        // Inexact: search for "z" (greater than all)
        let idx = in_node.find_entry(b"z", false, false);
        assert_eq!(idx, 2); // "f" is the entry < "z"
    }

    #[test]
    fn test_slot_state_operations() {
        let mut in_node = InNode::new(1, BIN_LEVEL, 128);

        in_node.insert_entry(b"key1".to_vec(), Lsn::from_u64(100), 0).unwrap();

        // Known deleted
        assert!(!in_node.is_entry_known_deleted(0));
        in_node.set_known_deleted(0);
        assert!(in_node.is_entry_known_deleted(0));
        in_node.clear_known_deleted(0);
        assert!(!in_node.is_entry_known_deleted(0));

        // Pending deleted
        assert!(!in_node.is_entry_pending_deleted(0));
        in_node.set_pending_deleted(0);
        assert!(in_node.is_entry_pending_deleted(0));
        in_node.clear_pending_deleted(0);
        assert!(!in_node.is_entry_pending_deleted(0));

        // Embedded LN
        assert!(!in_node.is_entry_embedded_ln(0));
        in_node.set_embedded_ln(0);
        assert!(in_node.is_entry_embedded_ln(0));
        in_node.clear_embedded_ln(0);
        assert!(!in_node.is_entry_embedded_ln(0));

        // Dirty — note: set_embedded_ln / clear_embedded_ln both set DIRTY_BIT
        // so the entry is already dirty at this point; we just verify that
        // clear_entry_dirty properly clears it.
        in_node.set_entry_dirty(0);
        assert!(in_node.is_entry_dirty(0));
        in_node.clear_entry_dirty(0);
        assert!(!in_node.is_entry_dirty(0));
    }

    #[test]
    fn test_serialization_roundtrip() {
        let mut in_node = InNode::new(42, BIN_LEVEL, 128);
        in_node.set_node_id(1234);
        in_node.set_last_full_lsn(Lsn::from_u64(5000));
        in_node.set_last_delta_lsn(Lsn::from_u64(5100));
        in_node.set_identifier_key(b"id_key".to_vec());

        // Insert some entries
        in_node
            .insert_entry(
                b"apple".to_vec(),
                Lsn::from_u64(100),
                entry_states::DIRTY_BIT,
            )
            .unwrap();
        in_node
            .insert_entry(
                b"banana".to_vec(),
                Lsn::from_u64(200),
                entry_states::KNOWN_DELETED_BIT,
            )
            .unwrap();
        in_node
            .insert_entry(
                b"cherry".to_vec(),
                Lsn::from_u64(300),
                entry_states::PENDING_DELETED_BIT,
            )
            .unwrap();

        // Serialize
        let mut buf = Vec::with_capacity(in_node.log_size());
        in_node.write_to_log(&mut buf);

        // Deserialize
        let restored = InNode::read_from_log(&buf, BIN_LEVEL).unwrap();

        // Verify
        assert_eq!(restored.node_id(), 1234);
        assert_eq!(restored.database_id, 42);
        assert_eq!(restored.level(), BIN_LEVEL);
        assert_eq!(restored.last_full_lsn(), Lsn::from_u64(5000));
        assert_eq!(restored.last_delta_lsn(), Lsn::from_u64(5100));
        assert_eq!(restored.identifier_key(), Some(b"id_key".as_slice()));
        assert_eq!(restored.n_entries(), 3);

        assert_eq!(restored.get_key(0), b"apple");
        assert_eq!(restored.get_lsn(0), Lsn::from_u64(100));
        assert!(InNode::is_state_dirty(restored.get_state(0)));

        assert_eq!(restored.get_key(1), b"banana");
        assert_eq!(restored.get_lsn(1), Lsn::from_u64(200));
        assert!(InNode::is_state_known_deleted(restored.get_state(1)));

        assert_eq!(restored.get_key(2), b"cherry");
        assert_eq!(restored.get_lsn(2), Lsn::from_u64(300));
        assert!(InNode::is_state_pending_deleted(restored.get_state(2)));
    }

    #[test]
    fn test_compare_keys() {
        use CmpOrdering::*;

        assert_eq!(InNode::compare_keys(b"apple", b"banana"), Less);
        assert_eq!(InNode::compare_keys(b"banana", b"apple"), Greater);
        assert_eq!(InNode::compare_keys(b"apple", b"apple"), Equal);

        // Prefix comparison
        assert_eq!(InNode::compare_keys(b"app", b"apple"), Less);
        assert_eq!(InNode::compare_keys(b"apple", b"app"), Greater);

        // Empty key
        assert_eq!(InNode::compare_keys(b"", b"apple"), Less);
        assert_eq!(InNode::compare_keys(b"apple", b""), Greater);
        assert_eq!(InNode::compare_keys(b"", b""), Equal);
    }

    #[test]
    fn test_node_full_error() {
        let mut in_node = InNode::new(1, BIN_LEVEL, 2); // Small capacity

        in_node
            .insert_entry(
                b"a".to_vec(),
                Lsn::from_u64(100),
                entry_states::DIRTY_BIT,
            )
            .unwrap();
        in_node
            .insert_entry(
                b"b".to_vec(),
                Lsn::from_u64(200),
                entry_states::DIRTY_BIT,
            )
            .unwrap();

        // Third insert should fail
        let result = in_node.insert_entry(
            b"c".to_vec(),
            Lsn::from_u64(300),
            entry_states::DIRTY_BIT,
        );
        assert!(result.is_err());
        match result {
            Err(InError::NodeFull(n, max)) => {
                assert_eq!(n, 2);
                assert_eq!(max, 2);
            }
            _ => panic!("Expected NodeFull error"),
        }
    }

    #[test]
    fn test_latch_operations() {
        let in_node = InNode::new(1, BIN_LEVEL, 128);

        // Test exclusive latch (RAII guard)
        {
            let _guard =
                in_node.latch().acquire_exclusive().expect("acquire_exclusive");
            // Latch is held here
        }
        // Latch is released when guard goes out of scope

        // For a BIN (level 1), shared and exclusive are the same (exclusive-only mode)
        {
            let _guard =
                in_node.latch().acquire_shared().expect("acquire_shared");
            // Latch is held here (as exclusive since BINs are exclusive-only)
        }
    }

    // ========================================================================
    // New method tests
    // ========================================================================

    #[test]
    fn test_update_entry_lsn() {
        let mut node = InNode::new(1, BIN_LEVEL, 128);
        node.insert_entry(b"key".to_vec(), Lsn::from_u64(100), 0).unwrap();

        assert_eq!(node.get_lsn(0), Lsn::from_u64(100));
        // Node should be dirty after insert already; clear it to test that
        // update_entry_lsn re-dirties the node.
        node.clear_dirty();
        assert!(!node.is_dirty());

        node.update_entry_lsn(0, Lsn::from_u64(999));
        assert_eq!(node.get_lsn(0), Lsn::from_u64(999));
        assert!(node.is_dirty(), "update_entry_lsn should mark node dirty");
    }

    #[test]
    #[should_panic]
    fn test_update_entry_lsn_out_of_bounds_panics() {
        let mut node = InNode::new(1, BIN_LEVEL, 128);
        // n_entries == 0, so any index is out of bounds.
        node.update_entry_lsn(0, Lsn::from_u64(1));
    }

    #[test]
    fn test_get_memory_size_empty_node() {
        let node = InNode::new(1, BIN_LEVEL, 128);
        let size = node.get_memory_size();
        // Must be positive and at least the per-slot-array overhead.
        assert!(size > 0);
    }

    #[test]
    fn test_get_memory_size_grows_with_keys() {
        let mut node_small = InNode::new(1, BIN_LEVEL, 32);
        let mut node_large = InNode::new(1, BIN_LEVEL, 32);

        node_small.insert_entry(b"k".to_vec(), Lsn::from_u64(1), 0).unwrap();
        node_large
            .insert_entry(b"very_long_key_here".to_vec(), Lsn::from_u64(1), 0)
            .unwrap();

        assert!(
            node_large.get_memory_size() > node_small.get_memory_size(),
            "larger key should produce larger memory estimate"
        );
    }

    #[test]
    fn test_get_memory_size_with_identifier_key() {
        let mut node_no_id = InNode::new(1, BIN_LEVEL, 8);
        let mut node_with_id = InNode::new(1, BIN_LEVEL, 8);
        node_with_id.set_identifier_key(b"identifier_key_bytes".to_vec());

        assert!(
            node_with_id.get_memory_size() > node_no_id.get_memory_size(),
            "identifier key should increase memory estimate"
        );
        // suppress unused-mut warning
        let _ = &mut node_no_id;
    }

    // ========================================================================
    // Edge case tests for insert/delete/find
    // ========================================================================

    #[test]
    fn test_insert_into_empty_node() {
        let mut node = InNode::new(1, BIN_LEVEL, 4);
        let result = node.insert_entry(b"only".to_vec(), Lsn::from_u64(1), 0);
        assert!(result.is_ok());
        let idx = result.unwrap();
        assert_ne!(idx & INSERT_SUCCESS, 0);
        assert_eq!(node.n_entries(), 1);
    }

    #[test]
    fn test_insert_duplicate_returns_index_without_success_flag() {
        let mut node = InNode::new(1, BIN_LEVEL, 4);
        node.insert_entry(b"dup".to_vec(), Lsn::from_u64(1), 0).unwrap();

        let result = node.insert_entry(b"dup".to_vec(), Lsn::from_u64(2), 0);
        assert!(result.is_ok());
        let idx = result.unwrap();
        // No INSERT_SUCCESS flag on duplicate.
        assert_eq!(idx & INSERT_SUCCESS, 0);
        // Entry count unchanged.
        assert_eq!(node.n_entries(), 1);
    }

    #[test]
    fn test_delete_only_entry() {
        let mut node = InNode::new(1, BIN_LEVEL, 4);
        node.insert_entry(b"solo".to_vec(), Lsn::from_u64(1), 0).unwrap();
        assert_eq!(node.n_entries(), 1);

        assert!(node.delete_entry(0));
        assert_eq!(node.n_entries(), 0);
    }

    #[test]
    fn test_delete_out_of_bounds_returns_false() {
        let mut node = InNode::new(1, BIN_LEVEL, 4);
        assert!(!node.delete_entry(0));
        assert!(!node.delete_entry(999));
    }

    #[test]
    fn test_find_entry_empty_node() {
        let node = InNode::new(1, BIN_LEVEL, 4);
        // Inexact search on empty node: high starts at -1, returns -1.
        let result = node.find_entry(b"any", false, false);
        assert_eq!(result, -1);

        // Exact search on empty node.
        let result = node.find_entry(b"any", false, true);
        assert_eq!(result, -1);
    }

    #[test]
    fn test_find_entry_known_deleted_exact_returns_minus_one() {
        let mut node = InNode::new(1, BIN_LEVEL, 4);
        node.insert_entry(b"ghost".to_vec(), Lsn::from_u64(1), 0).unwrap();
        node.set_known_deleted(0);

        // Exact search for a known-deleted key should return -1.
        let result = node.find_entry(b"ghost", true, true);
        assert_eq!(result, -1);
    }

    #[test]
    fn test_upper_in_entry_zero_virtual_key() {
        // Upper IN: the key at slot 0 is treated as -infinity (virtual).
        // Per IN.java: when middle==0 and entry_zero_special_compare is
        // true, the comparison is set to cmp=1 (search key > virtual key),
        // so the loop always moves right past slot 0.
        //
        // Three entries ["aaa"(0), "mmm"(1), "zzz"(2)].
        let mut node = InNode::new(1, MAIN_LEVEL | 2, 8);
        node.insert_entry(b"aaa".to_vec(), Lsn::from_u64(1), 0).unwrap();
        node.insert_entry(b"mmm".to_vec(), Lsn::from_u64(2), 0).unwrap();
        node.insert_entry(b"zzz".to_vec(), Lsn::from_u64(3), 0).unwrap();

        // Inexact search for "nnn" (between "mmm"=1 and "zzz"=2):
        // low=0, high=2 => middle=1; cmp("nnn" vs "mmm")=Greater => low=2
        // low=2, high=2 => middle=2; cmp("nnn" vs "zzz")=Less => high=1
        // loop ends, returns high=1.
        let result = node.find_entry(b"nnn", false, false);
        assert_eq!(result, 1);

        // Inexact search for "bbb" (between virtual slot 0 and "mmm"=1):
        // low=0, high=2 => middle=1; cmp("bbb" vs "mmm")=Less => high=0
        // low=0, high=0 => middle=0; slot 0 virtual => Greater => low=1
        // loop ends (low=1 > high=0), returns high=0.
        // Slot 0 is the leftmost child and covers all keys up to "mmm".
        let result2 = node.find_entry(b"bbb", false, false);
        assert_eq!(result2, 0);

        // indicate_if_duplicate disables the virtual-key path; exact match
        // on "zzz" should find slot 2 with EXACT_MATCH set.
        let result3 = node.find_entry(b"zzz", true, false);
        assert_eq!(result3 & !EXACT_MATCH, 2);
        assert_ne!(result3 & EXACT_MATCH, 0);
    }

    #[test]
    fn test_split_index_and_split_key() {
        let mut node = InNode::new(1, BIN_LEVEL, 8);
        for i in 0u8..6 {
            node.insert_entry(vec![b'a' + i], Lsn::from_u64(i as u64), 0)
                .unwrap();
        }
        // n_entries == 6, split_index == 3
        assert_eq!(node.split_index(), 3);
        let split_key = node.get_split_key(3).unwrap();
        assert_eq!(split_key, node.get_key(3).to_vec());

        // Out-of-range split key returns None.
        assert!(node.get_split_key(node.n_entries()).is_none());
    }

    #[test]
    fn test_in_list_resident_and_generation() {
        let mut node = InNode::new(1, BIN_LEVEL, 4);
        assert!(!node.in_list_resident);
        node.in_list_resident = true;
        assert!(node.in_list_resident);

        assert_eq!(node.generation, 0);
        node.generation = 42;
        assert_eq!(node.generation, 42);
    }

    #[test]
    fn test_set_lsn_and_state_direct() {
        let mut node = InNode::new(1, BIN_LEVEL, 4);
        node.insert_entry(b"k".to_vec(), Lsn::from_u64(1), 0).unwrap();

        node.set_lsn(0, Lsn::from_u64(777));
        assert_eq!(node.get_lsn(0), Lsn::from_u64(777));

        node.set_state(0, entry_states::KNOWN_DELETED_BIT);
        assert_eq!(node.get_state(0), entry_states::KNOWN_DELETED_BIT);
    }

    #[test]
    fn test_identifier_key_roundtrip() {
        let mut node = InNode::new(1, BIN_LEVEL, 4);
        assert!(node.identifier_key().is_none());

        node.set_identifier_key(b"myid".to_vec());
        assert_eq!(node.identifier_key(), Some(b"myid".as_ref()));
    }

    #[test]
    fn test_has_cached_children_flag() {
        let mut node = InNode::new(1, MAIN_LEVEL | 2, 4);
        assert!(!node.has_cached_children());
        node.set_has_cached_children(true);
        assert!(node.has_cached_children());
        node.set_has_cached_children(false);
        assert!(!node.has_cached_children());
    }

    #[test]
    fn test_last_full_and_delta_lsn() {
        let mut node = InNode::new(1, BIN_LEVEL, 4);
        assert!(node.last_full_lsn().is_null());
        assert!(node.last_delta_lsn().is_null());

        node.set_last_full_lsn(Lsn::new(1, 100));
        node.set_last_delta_lsn(Lsn::new(1, 200));

        assert_eq!(node.last_full_lsn(), Lsn::new(1, 100));
        assert_eq!(node.last_delta_lsn(), Lsn::new(1, 200));
    }

    #[test]
    fn test_no_data_ln_entry_state() {
        let mut node = InNode::new(1, BIN_LEVEL, 4);
        node.insert_entry(b"k".to_vec(), Lsn::from_u64(1), 0).unwrap();

        assert!(!node.is_entry_no_data_ln(0));
        node.entry_states[0] |= entry_states::NO_DATA_LN_BIT;
        assert!(node.is_entry_no_data_ln(0));
        assert!(InNode::is_state_no_data_ln(entry_states::NO_DATA_LN_BIT));
    }

    // ========================================================================
    // ========================================================================

    ///
    /// On an empty IN every `findEntry` variant returns -1.
    #[test]
    fn test_je_find_entry_empty_in_returns_minus_one() {
        let node = InNode::new(1, BIN_LEVEL, 6);

        let zero_key = [0x00u8; 3];
        let max_key = [0xFFu8; 3];

        // All four (indicate_if_duplicate × exact) combinations must return -1.
        assert_eq!(node.find_entry(&zero_key, false, false), -1);
        assert_eq!(node.find_entry(&max_key, false, false), -1);
        assert_eq!(node.find_entry(&zero_key, false, true), -1);
        assert_eq!(node.find_entry(&max_key, false, true), -1);
        assert_eq!(node.find_entry(&zero_key, true, false), -1);
        assert_eq!(node.find_entry(&max_key, true, false), -1);
        assert_eq!(node.find_entry(&zero_key, true, true), -1);
        assert_eq!(node.find_entry(&max_key, true, true), -1);
    }

    ///
    /// inserts keys of the form [0x01, i, 0x10] for i in 0..capacity.
    /// After each insert:
    ///  - zero_key (all 0x00) routes to slot 0 (LTE inexact)
    ///  - max_key  (all 0xFF) routes to the last inserted slot (LTE inexact)
    ///  - exact search for zero_key / max_key returns -1 (not present in BIN)
    ///  - each inserted key finds itself with EXACT_MATCH when indicate_if_duplicate=true
    #[test]
    fn test_je_find_entry_after_sequential_inserts() {
        const CAP: usize = 6;
        let mut node = InNode::new(1, BIN_LEVEL, CAP);

        let zero_key = [0x00u8; 3];
        let max_key = [0xFFu8; 3];

        for i in 0u8..CAP as u8 {
            // Key pattern: [0x01, i, 0x10]
            let key_bytes = vec![0x01u8, i, 0x10u8];
            let result =
                node.insert_entry(key_bytes, Lsn::from_u64(i as u64), 0);
            assert!(result.is_ok());
            let flags = result.unwrap();
            assert_ne!(flags & INSERT_SUCCESS, 0, "INSERT_SUCCESS must be set");
            // The slot index returned must equal i (keys inserted in order).
            assert_eq!(
                (flags & !INSERT_SUCCESS) as u8,
                i,
                "slot index must equal i"
            );

            // zero_key is below all inserted keys → -1 (no slot at or below it).
            // semantics: inexact returns the largest slot index whose key ≤
            // search key, or -1 if the search key is less than slot[0].
            assert_eq!(node.find_entry(&zero_key, false, false), -1);
            // max_key is above all inserted keys → last slot (LTE inexact).
            assert_eq!(node.find_entry(&max_key, false, false), i as i32);

            // exact=true for keys not present → -1.
            assert_eq!(node.find_entry(&zero_key, false, true), -1);
            assert_eq!(node.find_entry(&max_key, false, true), -1);

            // Each present key finds itself with and without EXACT_MATCH.
            // Note: slot 0 is virtual for upper INs but this is a BIN, so
            // all slots including 0 behave normally.
            for j in 0..=i as usize {
                let k = node.get_key(j);
                let idx_inexact = node.find_entry(k, false, false);
                assert_eq!(
                    idx_inexact, j as i32,
                    "inexact: key at {} must return {}",
                    j, j
                );

                let idx_exact = node.find_entry(k, false, true);
                assert_eq!(
                    idx_exact, j as i32,
                    "exact: key at {} must return {}",
                    j, j
                );

                let idx_dup = node.find_entry(k, true, false);
                assert_eq!(
                    idx_dup & !EXACT_MATCH,
                    j as i32,
                    "indicate_dup: slot must be {}",
                    j
                );
                assert_ne!(idx_dup & EXACT_MATCH, 0, "EXACT_MATCH must be set");

                let idx_both = node.find_entry(k, true, true);
                assert_eq!(idx_both & !EXACT_MATCH, j as i32);
                assert_ne!(idx_both & EXACT_MATCH, 0);
            }
        }
    }

    /// Unsigned comparison of 0xff bytes.
    ///
    /// comment: "Use FF since that sets the sign bit negative on a byte.
    /// This checks the Key.compareTo routine for proper unsigned comparisons."
    ///
    /// In the Rust port this is just `u8::cmp` which is already unsigned, but
    /// we add the explicit test for documentation parity.
    #[test]
    fn test_je_unsigned_byte_key_ordering() {
        let mut node = InNode::new(1, BIN_LEVEL, 8);
        // Insert a "middle" key: all 0x7F bytes.
        node.insert_entry(vec![0x7Fu8; 3], Lsn::from_u64(1), 0).unwrap();

        let high_key = [0xFFu8; 3]; // Would be negative in signed Java byte

        // 0xFF > 0x7F when treated as unsigned → high_key lands after slot 0.
        let idx = node.find_entry(&high_key, false, false);
        assert_eq!(idx, 0, "0xFF key must sort after 0x7F (slot 0)");

        // Insert the 0xFF key to confirm ordering.
        node.insert_entry(vec![0xFFu8; 3], Lsn::from_u64(2), 0).unwrap();
        assert_eq!(node.n_entries(), 2);
        assert_eq!(node.get_key(0), &[0x7Fu8; 3]);
        assert_eq!(node.get_key(1), &[0xFFu8; 3]);
    }

    /// Fill then empty a bin.
    ///
    /// Fill IN to capacity with random keys, then delete them one by one
    /// until only the first entry remains, then delete that too.
    /// We use deterministic keys for reproducibility.
    #[test]
    fn test_je_delete_entry_fill_then_empty() {
        const CAP: usize = 6;
        let mut node = InNode::new(1, BIN_LEVEL, CAP);

        // Insert CAP keys (sorted, so we know their indices).
        let keys: Vec<Vec<u8>> =
            (0..CAP as u8).map(|i| vec![0x01u8, i, 0x10u8]).collect();
        for k in &keys {
            node.insert_entry(k.clone(), Lsn::from_u64(0), 0).unwrap();
        }
        assert_eq!(node.n_entries(), CAP);

        // Delete from the end down to 1 entry.
        while node.n_entries() > 1 {
            let n = node.n_entries();
            // Delete the last entry.
            let last_key = node.get_key(n - 1).to_vec();
            let idx = node.find_entry(&last_key, false, true);
            assert!(idx >= 0, "must find key before deleting");
            assert!(node.delete_entry(idx as usize));
            assert_eq!(node.n_entries(), n - 1);
        }

        // One entry left: the zeroth key.
        assert_eq!(node.n_entries(), 1);
        assert_eq!(node.get_key(0), keys[0].as_slice());

        // Delete the last entry.
        assert!(node.delete_entry(0));
        assert_eq!(node.n_entries(), 0);

        // Deleting from an empty node returns false.
        assert!(!node.delete_entry(0));
    }

    /// Level constants.
    ///
    /// BINs are at level `MAIN_LEVEL | 1`; upper INs are at
    /// `MAIN_LEVEL | 2` and above; dbmap INs live in `DBMAP_LEVEL` space.
    #[test]
    fn test_je_level_constants() {
        assert_eq!(
            BIN_LEVEL,
            MAIN_LEVEL | 1,
            "BIN_LEVEL must equal MAIN_LEVEL | 1"
        );

        let bin = InNode::new(1, BIN_LEVEL, 4);
        assert!(bin.is_bin(), "BIN_LEVEL node must be is_bin()");
        assert!(!bin.is_upper_in(), "BIN_LEVEL node must not be is_upper_in()");
        assert_eq!(bin.normalized_level(), 1);

        let upper = InNode::new(1, MAIN_LEVEL | 2, 4);
        assert!(!upper.is_bin(), "upper IN must not be is_bin()");
        assert!(upper.is_upper_in(), "upper IN must be is_upper_in()");
        assert_eq!(upper.normalized_level(), 2);

        let dbmap = InNode::new(1, DBMAP_LEVEL | 1, 4);
        assert!(
            dbmap.is_dbmap_level(),
            "DBMAP_LEVEL node must be is_dbmap_level()"
        );
    }

    /// Virtual slot-0 key in an upper IN.
    ///
    /// "The 0'th entry's key is treated specially in an upper IN. It
    /// always compares lower than any other key (virtual key behavior)."
    ///
    /// When indicate_if_duplicate=false and exact=false, the virtual path is
    /// active. Any search key routed through slot 0 stays at slot 0 (because
    /// the virtual key is always ≤ any real key).
    ///
    /// When indicate_if_duplicate=true or exact=true the virtual path is
    /// disabled and the real key at slot 0 participates in the search.
    #[test]
    fn test_je_upper_in_virtual_slot0_routing() {
        let mut node = InNode::new(1, MAIN_LEVEL | 2, 8);
        // Insert three real keys.
        node.insert_entry(b"bbb".to_vec(), Lsn::from_u64(1), 0).unwrap();
        node.insert_entry(b"ddd".to_vec(), Lsn::from_u64(2), 0).unwrap();
        node.insert_entry(b"fff".to_vec(), Lsn::from_u64(3), 0).unwrap();
        // Slots: 0="bbb", 1="ddd", 2="fff"

        // A key less than "bbb" routes to slot 0 (virtual key wins).
        let idx_below = node.find_entry(b"aaa", false, false);
        assert_eq!(
            idx_below, 0,
            "key < first real key must route to slot 0 (virtual)"
        );

        // A key between "bbb" and "ddd" routes to slot 0.
        let idx_mid = node.find_entry(b"ccc", false, false);
        assert_eq!(
            idx_mid, 0,
            "key between slot-0 and slot-1 must stay at slot 0"
        );

        // A key between "ddd" and "fff" routes to slot 1.
        let idx_hi = node.find_entry(b"eee", false, false);
        assert_eq!(
            idx_hi, 1,
            "key between slot-1 and slot-2 must route to slot 1"
        );

        // A key greater than all routes to the last slot.
        let idx_max = node.find_entry(b"zzz", false, false);
        assert_eq!(
            idx_max, 2,
            "key greater than all entries must route to last slot"
        );

        // With indicate_if_duplicate=true the virtual path is disabled:
        // exact match on the real key at slot 0 must be found.
        let idx_dup = node.find_entry(b"bbb", true, false);
        assert_eq!(
            idx_dup & !EXACT_MATCH,
            0,
            "indicate_dup: exact match at slot 0 must return slot 0"
        );
        assert_ne!(
            idx_dup & EXACT_MATCH,
            0,
            "indicate_dup: EXACT_MATCH must be set for 'bbb'"
        );
    }

    /// Node-full error.
    ///
    /// Inserting into a full IN raises EnvironmentFailureException.
    /// Rust: `insert_entry` returns `Err(InError::NodeFull)`.
    #[test]
    fn test_je_insert_entry_node_full_returns_error() {
        const CAP: usize = 4;
        let mut node = InNode::new(1, BIN_LEVEL, CAP);

        for i in 0..CAP as u8 {
            node.insert_entry(vec![i], Lsn::from_u64(i as u64), 0).unwrap();
        }
        assert_eq!(node.n_entries(), CAP);

        let result =
            node.insert_entry(b"overflow".to_vec(), Lsn::from_u64(99), 0);
        assert!(
            matches!(result, Err(InError::NodeFull(n, m)) if n == CAP && m == CAP),
            "inserting into a full node must return InError::NodeFull"
        );
    }

    // ========================================================================
    // Additional branch-coverage tests
    // ========================================================================

    #[test]
    fn test_set_prohibit_next_delta_on_non_bin_is_noop() {
        // set_prohibit_next_delta has an early-return for non-BIN nodes.
        let mut upper = InNode::new(1, MAIN_LEVEL | 2, 8);
        upper.set_prohibit_next_delta(true);
        // Flag must NOT be set on a non-BIN.
        assert!(!upper.get_prohibit_next_delta());

        // Verify the flag CAN be set on a BIN.
        let mut bin = InNode::new(1, BIN_LEVEL, 8);
        bin.set_prohibit_next_delta(true);
        assert!(bin.get_prohibit_next_delta());
        bin.set_prohibit_next_delta(false);
        assert!(!bin.get_prohibit_next_delta());
    }

    #[test]
    fn test_is_valid_for_delete_bin_delta_returns_false() {
        // When the BIN-delta flag is set, is_valid_for_delete must return false
        // regardless of the slot states.
        let mut node = InNode::new(1, BIN_LEVEL, 4);
        node.insert_entry(b"k".to_vec(), Lsn::from_u64(1), 0).unwrap();
        node.set_known_deleted(0);
        assert!(
            node.is_valid_for_delete(),
            "all KD without delta flag => true"
        );

        node.set_bin_delta(true);
        assert!(!node.is_valid_for_delete(), "bin-delta flag => always false");
    }

    #[test]
    fn test_validate_subtree_before_delete_paths() {
        // Path 1: index >= n_entries (trivially deletable).
        let node = InNode::new(1, BIN_LEVEL, 4);
        assert!(node.validate_subtree_before_delete(0));

        // Path 2: BIN with valid entries => always true.
        let mut bin = InNode::new(1, BIN_LEVEL, 4);
        bin.insert_entry(b"a".to_vec(), Lsn::from_u64(1), 0).unwrap();
        bin.insert_entry(b"b".to_vec(), Lsn::from_u64(2), 0).unwrap();
        assert!(bin.validate_subtree_before_delete(0));
        assert!(bin.validate_subtree_before_delete(1));

        // Path 3: upper IN with exactly 1 entry => true.
        let mut upper1 = InNode::new(1, MAIN_LEVEL | 2, 8);
        upper1.insert_entry(b"x".to_vec(), Lsn::from_u64(1), 0).unwrap();
        assert!(upper1.validate_subtree_before_delete(0));

        // Path 4: upper IN with 2+ entries => false.
        let mut upper2 = InNode::new(1, MAIN_LEVEL | 2, 8);
        upper2.insert_entry(b"x".to_vec(), Lsn::from_u64(1), 0).unwrap();
        upper2.insert_entry(b"y".to_vec(), Lsn::from_u64(2), 0).unwrap();
        assert!(!upper2.validate_subtree_before_delete(0));
    }

    #[test]
    fn test_insert_must_mutate_to_full_bin() {
        // Case 1: node full => true regardless of blind_insertion.
        let mut node = InNode::new(1, BIN_LEVEL, 2);
        node.insert_entry(b"a".to_vec(), Lsn::from_u64(1), 0).unwrap();
        node.insert_entry(b"b".to_vec(), Lsn::from_u64(2), 0).unwrap();
        assert!(node.insert_must_mutate_to_full_bin(b"c", true));
        assert!(node.insert_must_mutate_to_full_bin(b"c", false));

        // Case 2: not full, blind_insertion=true => false.
        let node2 = InNode::new(1, BIN_LEVEL, 8);
        assert!(!node2.insert_must_mutate_to_full_bin(b"x", true));

        // Case 3: not full, blind_insertion=false, not a delta (find_entry < 0 path).
        let mut node3 = InNode::new(1, BIN_LEVEL, 8);
        node3.insert_entry(b"m".to_vec(), Lsn::from_u64(1), 0).unwrap();
        // "z" is not present => find_entry returns >= 0 without EXACT_MATCH,
        // but is_bin_delta() is false so the condition `!self.is_bin_delta()` is
        // true => returns true.
        assert!(node3.insert_must_mutate_to_full_bin(b"z", false));
    }

    #[test]
    fn test_set_in_list_resident_false_clears_resident_bit() {
        let mut node = InNode::new(1, BIN_LEVEL, 4);
        node.set_in_list_resident(true);
        assert!(node.get_in_list_resident());
        assert!((node.flags & 0x80) != 0, "RESIDENT bit should be set");

        node.set_in_list_resident(false);
        assert!(!node.get_in_list_resident());
        assert!((node.flags & 0x80) == 0, "RESIDENT bit should be cleared");
    }

    #[test]
    fn test_in_pri2_lru_and_fetched_cold_and_expiration_in_hours() {
        let mut node = InNode::new(1, BIN_LEVEL, 4);

        // is_in_pri2_lru
        assert!(!node.is_in_pri2_lru());
        node.set_in_pri2_lru(true);
        assert!(node.is_in_pri2_lru());
        node.set_in_pri2_lru(false);
        assert!(!node.is_in_pri2_lru());

        // get/set fetched cold
        assert!(!node.get_fetched_cold());
        node.set_fetched_cold(true);
        assert!(node.get_fetched_cold());
        node.set_fetched_cold(false);
        assert!(!node.get_fetched_cold());

        // expiration in hours
        assert!(!node.is_expiration_in_hours());
        node.set_expiration_in_hours(true);
        assert!(node.is_expiration_in_hours());
        node.set_expiration_in_hours(false);
        assert!(!node.is_expiration_in_hours());
    }

    #[test]
    fn test_have_embedded_data_and_get_num_embedded_lns() {
        let mut node = InNode::new(1, BIN_LEVEL, 8);
        node.insert_entry(b"k1".to_vec(), Lsn::from_u64(1), 0).unwrap();
        node.insert_entry(b"k2".to_vec(), Lsn::from_u64(2), 0).unwrap();

        // Neither embedded yet.
        assert_eq!(node.get_num_embedded_lns(), 0);
        assert!(!node.have_embedded_data(0));

        // Set k1 as embedded LN with data.
        node.set_embedded_ln(0);
        assert!(node.have_embedded_data(0));
        assert_eq!(node.get_num_embedded_lns(), 1);

        // Set k2 as embedded LN but also no-data.
        node.set_embedded_ln(1);
        node.set_no_data_ln(1);
        // have_embedded_data must be false when NO_DATA_LN_BIT is set.
        assert!(!node.have_embedded_data(1));
        assert_eq!(node.get_num_embedded_lns(), 2);

        // Clear no_data_ln from k2.
        node.clear_no_data_ln(1);
        assert!(node.have_embedded_data(1));

        // Clear embedded from k1.
        node.clear_embedded_ln(0);
        assert!(!node.have_embedded_data(0));
        assert_eq!(node.get_num_embedded_lns(), 1);
    }

    #[test]
    fn test_tombstone_operations() {
        let mut node = InNode::new(1, BIN_LEVEL, 4);
        node.insert_entry(b"t".to_vec(), Lsn::from_u64(1), 0).unwrap();

        assert!(!node.is_tombstone(0));
        node.set_tombstone(0, true);
        assert!(node.is_tombstone(0));
        assert!(node.is_entry_dirty(0));

        node.set_tombstone(0, false);
        assert!(!node.is_tombstone(0));
    }

    #[test]
    fn test_update_key_when_logged_flag() {
        let mut node = InNode::new(1, BIN_LEVEL, 4);
        node.insert_entry(b"k".to_vec(), Lsn::from_u64(1), 0).unwrap();

        assert!(!node.is_update_key_when_logged(0));
        node.set_update_key_when_logged(0);
        assert!(node.is_update_key_when_logged(0));
    }

    #[test]
    fn test_pin_unpin_is_pinned() {
        let mut node = InNode::new(1, BIN_LEVEL, 4);
        assert!(!node.is_pinned());
        assert_eq!(node.pin_count, 0);

        node.pin();
        assert!(node.is_pinned());
        assert_eq!(node.pin_count, 1);

        node.pin();
        assert_eq!(node.pin_count, 2);

        node.unpin();
        assert!(node.is_pinned());
        assert_eq!(node.pin_count, 1);

        node.unpin();
        assert!(!node.is_pinned());
        assert_eq!(node.pin_count, 0);
    }

    #[test]
    fn test_is_evictable_always_true() {
        let node = InNode::new(1, MAIN_LEVEL | 2, 4);
        assert!(node.is_evictable());
        let bin = InNode::new(1, BIN_LEVEL, 4);
        assert!(bin.is_evictable());
    }

    #[test]
    fn test_memory_accounting() {
        let mut node = InNode::new(1, BIN_LEVEL, 4);
        node.insert_entry(b"k".to_vec(), Lsn::from_u64(1), 0).unwrap();

        // Default in_memory_size is 0; set and get it.
        assert_eq!(node.get_in_memory_size(), 0);
        node.set_in_memory_size(512);
        assert_eq!(node.get_in_memory_size(), 512);

        // accumulated_delta starts at 0.
        assert_eq!(node.get_budgeted_memory_size(), 512);

        // Artificially set accumulated_delta.
        node.accumulated_delta = 100;
        assert_eq!(node.get_budgeted_memory_size(), 412);

        // reset_and_get_memory_size zeroes the delta and returns size.
        let sz = node.reset_and_get_memory_size();
        assert_eq!(sz, 512);
        assert_eq!(node.accumulated_delta, 0);
    }

    #[test]
    fn test_bump_generation() {
        let mut node = InNode::new(1, BIN_LEVEL, 4);
        assert_eq!(node.generation(), 0);

        let g1 = node.bump_generation();
        assert_eq!(g1, 1);
        assert_eq!(node.generation(), 1);

        node.set_generation(99);
        assert_eq!(node.generation(), 99);
        let g2 = node.bump_generation();
        assert_eq!(g2, 100);
    }

    #[test]
    fn test_read_from_log_small_buffer_error() {
        // Buffer too small (< 38 bytes).
        let buf = vec![0u8; 10];
        let result = InNode::read_from_log(&buf, BIN_LEVEL);
        assert!(result.is_err());
        match result {
            Err(InError::Deserialization(msg)) => {
                assert!(msg.contains("buffer too small"), "msg={}", msg);
            }
            _ => panic!("Expected Deserialization error"),
        }
    }

    #[test]
    fn test_read_from_log_id_key_past_buffer_error() {
        // Build a valid header but claim a large identifier key length that
        // extends past the buffer.
        let mut in_node = InNode::new(42, BIN_LEVEL, 4);
        in_node.set_node_id(1);
        let mut buf = Vec::new();
        in_node.write_to_log(&mut buf);

        // Find the identifier-key length field (bytes 38+2-1 = at offset 38)
        // Format: node_id(8)+db_id(8)+level(4)+full_lsn(8)+delta_lsn(8)+n_entries(2) = 38
        // Then id_key_len at offset 38.
        // Overwrite it with a huge value.
        buf[38] = 0xFF;
        buf[39] = 0xFF;
        // Truncate the buffer so the key can't fit.
        buf.truncate(40);

        let result = InNode::read_from_log(&buf, BIN_LEVEL);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_from_log_entry_key_past_buffer_error() {
        // Create a valid node with 1 entry, serialize it, then truncate
        // inside the entry to trigger the "key past buffer" path.
        let mut node = InNode::new(1, BIN_LEVEL, 4);
        node.insert_entry(b"hello".to_vec(), Lsn::from_u64(10), 0).unwrap();

        let mut buf = Vec::new();
        node.write_to_log(&mut buf);

        // Fixed header: 38 bytes + id_key_len(2) + 0 bytes of id key = 40 bytes.
        // Entry 0 key_len at offset 40 (bytes 40-41).  key_len == 5 normally.
        // Overwrite with a large value.
        buf[40] = 0xFF;
        buf[41] = 0x00;
        // Truncate so the key can't fit.
        buf.truncate(44);

        let result = InNode::read_from_log(&buf, BIN_LEVEL);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_from_log_entry_lsn_past_buffer_error() {
        // Truncate the buffer inside the LSN field of entry 0.
        let mut node = InNode::new(1, BIN_LEVEL, 4);
        node.insert_entry(b"hi".to_vec(), Lsn::from_u64(10), 0).unwrap();

        let mut buf = Vec::new();
        node.write_to_log(&mut buf);

        // Header=40, key_len(2)=2, key(2)=2 → LSN starts at 44.  Truncate mid-LSN.
        buf.truncate(47);

        let result = InNode::read_from_log(&buf, BIN_LEVEL);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_from_log_entry_state_past_buffer_error() {
        // Truncate the buffer just before the state byte of entry 0.
        let mut node = InNode::new(1, BIN_LEVEL, 4);
        node.insert_entry(b"hi".to_vec(), Lsn::from_u64(10), 0).unwrap();

        let mut buf = Vec::new();
        node.write_to_log(&mut buf);

        // Header=40, key_len(2)+key(2)=4, lsn(8)=8 → state at 52.  Truncate before it.
        buf.truncate(52);

        let result = InNode::read_from_log(&buf, BIN_LEVEL);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_from_log_entry_key_len_past_buffer_error() {
        // Build a node with n_entries=1 but truncate the buffer before the
        // key_len field of entry 0 (2 bytes at offset 40).
        let mut node = InNode::new(1, BIN_LEVEL, 4);
        node.insert_entry(b"x".to_vec(), Lsn::from_u64(1), 0).unwrap();

        let mut buf = Vec::new();
        node.write_to_log(&mut buf);

        // Truncate to 40 — the header is complete but entry 0's key_len is missing.
        buf.truncate(40);

        let result = InNode::read_from_log(&buf, BIN_LEVEL);
        assert!(result.is_err());
    }

    #[test]
    fn test_read_from_log_no_identifier_key_and_no_entry_keys() {
        // Roundtrip with no identifier key and entries with zero-length keys.
        let mut node = InNode::new(5, MAIN_LEVEL | 2, 4);
        node.set_node_id(77);
        // Insert an entry with an empty key (key_len=0 path in write/read).
        node.entry_keys[0] = None;
        node.entry_lsns[0] = Lsn::from_u64(7);
        node.entry_states[0] = 0;
        node.n_entries = 1;

        let mut buf = Vec::new();
        node.write_to_log(&mut buf);
        let restored = InNode::read_from_log(&buf, MAIN_LEVEL | 2).unwrap();
        assert_eq!(restored.node_id(), 77);
        assert_eq!(restored.n_entries(), 1);
        assert!(restored.identifier_key().is_none());
        assert_eq!(restored.get_lsn(0), Lsn::from_u64(7));
    }
}
