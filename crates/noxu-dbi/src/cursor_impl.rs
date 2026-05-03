//! Internal cursor implementation.
//!
//! Port of `com.sleepycat.je.dbi.CursorImpl`.
//!
//! The core traversal logic mirrors JE's `CursorImpl.getNext()` (line 2546):
//!
//! ```text
//! while (bin != null) {
//!     latchBIN();
//!     if (forward ? ++index < nEntries : --index >= 0) {
//!         if record is valid: return it
//!     } else {
//!         bin = tree.getNextBin(anchorBIN) or tree.getPrevBin(anchorBIN)
//!         index = -1  (or nEntries for backward)
//!     }
//! }
//! ```
//!
//! Cross-BIN traversal is implemented: when the current BIN is exhausted,
//! `retrieve_next` calls `Tree::get_next_bin` / `Tree::get_prev_bin` to move
//! to the adjacent BIN and continues iteration there.

use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
#[cfg(any(test, feature = "testing"))]
use std::cell::Cell;

use noxu_tree::Tree;
use noxu_util::Lsn;
use parking_lot::RwLock;

use crate::{
    DbiError, GetMode, OperationStatus, PutMode, SearchMode,
    database_impl::DatabaseImpl,
};

/// Cursor states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CursorState {
    NotInitialized,
    Initialized,
    Closed,
}

/// Result flags for cursor search operations.
pub const FOUND: u32 = 0x1;
pub const EXACT_KEY: u32 = 0x2;
pub const FOUND_LAST: u32 = 0x4;

/// Unique cursor ID generator.
static NEXT_CURSOR_ID: AtomicI64 = AtomicI64::new(1);

// Test-only hook: countdown to forced cursor failure.
//
// When the countdown is N (> 0), each `check_state`/`check_initialized` call
// decrements it by 1.  When it reaches 1 the decrement fires, it resets to 0,
// and the call returns `Err(DbiError::CursorClosed)`.
//
// `set_cursor_fail_after(1)` => fail on the next check (the 1st call).
// `set_cursor_fail_after(2)` => skip the 1st check, fail on the 2nd call.
//
// This lets `noxu-db` tests exercise both `map_err` closures inside a single
// `Database` method (e.g. `get()` has one closure on `search` and another on
// `get_current`).
#[cfg(any(test, feature = "testing"))]
thread_local! {
    static CURSOR_FAIL_COUNTDOWN: Cell<u32> = const { Cell::new(0) };
}

/// Set countdown so the Nth cursor-check call returns `DbiError::CursorClosed`.
/// `n = 1` → fail immediately on the next check.
/// Only available in test/testing builds.
#[cfg(any(test, feature = "testing"))]
pub fn set_cursor_fail_after(n: u32) {
    CURSOR_FAIL_COUNTDOWN.with(|c| c.set(n));
}

/// Clear the cursor fail countdown (idempotent).
#[cfg(any(test, feature = "testing"))]
pub fn clear_cursor_fail_flag() {
    CURSOR_FAIL_COUNTDOWN.with(|c| c.set(0));
}

/// Decrement the countdown and return `true` if this call should fail.
#[cfg(any(test, feature = "testing"))]
fn tick_fail() -> bool {
    CURSOR_FAIL_COUNTDOWN.with(|c| {
        let v = c.get();
        if v == 0 {
            false
        } else if v == 1 {
            c.set(0);
            true
        } else {
            c.set(v - 1);
            false
        }
    })
}

/// The internal implementation of a database cursor.
///
/// A CursorImpl tracks a position in a database and provides
/// get/put/delete operations. The cursor state machine ensures
/// proper initialization before operations.
///
/// In JE, a cursor tracks its position via a BIN reference and slot index.
/// This implementation wires cursor traversal to `noxu_tree::Tree`:
///
/// * `get_first` / `get_last` — use `Tree::get_first_node()` /
///   `Tree::get_last_node()` (port of `CursorImpl.positionFirstOrLast`).
/// * `retrieve_next` — increments `current_index` within the BIN and, when
///   the BIN is exhausted, calls `Tree::get_next_bin()` /
///   `Tree::get_prev_bin()` to cross BIN boundaries (port of
///   `CursorImpl.getNext()`).
/// * `search` — uses `Tree::search()` to locate the exact key.
/// * `put` / `delete` — mutate the tree in-place using `Tree::insert()` /
///   `Tree::delete()`.
///
/// Port of `com.sleepycat.je.dbi.CursorImpl` (4096 lines in JE 7.5.11).
pub struct CursorImpl {
    /// Unique cursor ID (for debugging and hashCode).
    id: i64,
    /// The database this cursor operates on.
    db_impl: Arc<RwLock<DatabaseImpl>>,
    /// The locker (transaction or auto-commit) for this cursor.
    locker_id: i64,
    /// Current cursor state.
    state: CursorState,

    /// Current position: the key at the cursor's position.
    current_key: Option<Vec<u8>>,
    /// Current position: the data at the cursor's position.
    current_data: Option<Vec<u8>>,
    /// Current position: the LSN of the record.
    current_lsn: u64,
    /// Current position: the BIN index (slot in the current BIN).
    ///
    /// In JE this is `CursorImpl.index`. -1 means "before first entry".
    current_index: i32,
}

impl CursorImpl {
    /// Creates a new CursorImpl for the given database.
    ///
    /// The cursor is initially in the NotInitialized state and must be
    /// positioned via a search operation before get/put/delete operations
    /// can be performed.
    ///
    /// # Arguments
    ///
    /// * `db_impl` - The database implementation this cursor operates on
    /// * `locker_id` - The locker (transaction) ID for this cursor
    pub fn new(db_impl: Arc<RwLock<DatabaseImpl>>, locker_id: i64) -> Self {
        CursorImpl {
            id: NEXT_CURSOR_ID.fetch_add(1, Ordering::Relaxed),
            db_impl,
            locker_id,
            state: CursorState::NotInitialized,
            current_key: None,
            current_data: None,
            current_lsn: noxu_util::NULL_LSN.as_u64(),
            current_index: -1,
        }
    }

    /// Returns the unique cursor ID.
    ///
    /// Used for debugging and cursor tracking.
    pub fn get_id(&self) -> i64 {
        self.id
    }

    /// Returns the database this cursor operates on.
    pub fn get_database(&self) -> &Arc<RwLock<DatabaseImpl>> {
        &self.db_impl
    }

    /// Returns the locker ID.
    pub fn get_locker_id(&self) -> i64 {
        self.locker_id
    }

    /// Returns true if the cursor is initialized (positioned on a record).
    pub fn is_initialized(&self) -> bool {
        self.state == CursorState::Initialized
    }

    /// Returns true if the cursor is closed.
    pub fn is_closed(&self) -> bool {
        self.state == CursorState::Closed
    }

    /// Returns the current key, if positioned.
    pub fn get_current_key(&self) -> Option<&[u8]> {
        self.current_key.as_deref()
    }

    /// Returns the current data, if positioned.
    pub fn get_current_data(&self) -> Option<&[u8]> {
        self.current_data.as_deref()
    }

    /// Returns the current LSN, if positioned.
    pub fn get_current_lsn(&self) -> u64 {
        self.current_lsn
    }

    /// Checks the cursor is not closed.
    fn check_state(&self) -> Result<(), DbiError> {
        #[cfg(any(test, feature = "testing"))]
        if tick_fail() {
            return Err(DbiError::CursorClosed);
        }
        match self.state {
            CursorState::Closed => Err(DbiError::CursorClosed),
            _ => Ok(()),
        }
    }

    /// Checks the cursor is initialized.
    fn check_initialized(&self) -> Result<(), DbiError> {
        #[cfg(any(test, feature = "testing"))]
        if tick_fail() {
            return Err(DbiError::CursorClosed);
        }
        match self.state {
            CursorState::Closed => Err(DbiError::CursorClosed),
            CursorState::NotInitialized => Err(DbiError::CursorNotInitialized),
            CursorState::Initialized => Ok(()),
        }
    }

    /// Positions the cursor at a specific key.
    ///
    /// Port of `CursorImpl.searchExact()` / `CursorImpl.searchRange()` from JE.
    ///
    /// Uses `Tree::search(key)` to locate the BIN slot for the key:
    ///
    /// * `SearchMode::Set` / `SearchMode::Both` — exact key match required.
    ///   Returns `NotFound` if the key is not present.
    /// * `SearchMode::SetRange` / `SearchMode::BothRange` — positions at the
    ///   first key >= the search key (range search).  Currently degrades to
    ///   an exact-match check; full range support requires iterating forward
    ///   until the key is >= the search key.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to search for
    /// * `data` - Optional data for Both/BothRange modes
    /// * `search_mode` - The search mode (Set, Both, SetRange, BothRange)
    ///
    /// # Returns
    ///
    /// * `Success` if the key was found and cursor positioned
    /// * `NotFound` if the key does not exist
    pub fn search(
        &mut self,
        key: &[u8],
        data: Option<&[u8]>,
        search_mode: SearchMode,
    ) -> Result<OperationStatus, DbiError> {
        self.check_state()?;

        // Use the real tree when available.
        let found = {
            let db = self.db_impl.read();
            if let Some(tree) = db.get_real_tree() {
                let result = tree.search(key);
                match result {
                    Some(sr) => sr.exact_parent_found,
                    None => false,
                }
            } else {
                // No tree yet — treat as not found.
                false
            }
        };

        match search_mode {
            SearchMode::Set | SearchMode::Both => {
                if found {
                    // Retrieve the data from the tree so current_data is accurate.
                    let data_from_tree: Option<Vec<u8>> = {
                        let db = self.db_impl.read();
                        if let Some(tree) = db.get_real_tree() {
                            Self::get_data_from_tree(tree, key)
                        } else {
                            None
                        }
                    };
                    self.current_key = Some(key.to_vec());
                    self.current_data = data_from_tree.or_else(|| data.map(|d| d.to_vec()));
                    self.current_lsn = noxu_util::NULL_LSN.as_u64();
                    self.current_index = 0;
                    self.state = CursorState::Initialized;
                    Ok(OperationStatus::Success)
                } else {
                    Ok(OperationStatus::NotFound)
                }
            }
            SearchMode::SetRange | SearchMode::BothRange => {
                // For range search, position at the first key >= search key.
                // If exact match, position there; otherwise search for the
                // smallest key that is >= the requested key.
                if found {
                    let data_from_tree: Option<Vec<u8>> = {
                        let db = self.db_impl.read();
                        if let Some(tree) = db.get_real_tree() {
                            Self::get_data_from_tree(tree, key)
                        } else {
                            None
                        }
                    };
                    self.current_key = Some(key.to_vec());
                    self.current_data = data_from_tree.or_else(|| data.map(|d| d.to_vec()));
                    self.current_lsn = noxu_util::NULL_LSN.as_u64();
                    self.current_index = 0;
                    self.state = CursorState::Initialized;
                    Ok(OperationStatus::Success)
                } else {
                    // Position at the first key in the tree that is >= key.
                    // Port of: CursorImpl.searchRange() + checking for NotFound
                    let next_entry: Option<(Vec<u8>, Vec<u8>)> = {
                        let db = self.db_impl.read();
                        if let Some(tree) = db.get_real_tree() {
                            Self::find_range_entry(tree, key)
                        } else {
                            None
                        }
                    };
                    match next_entry {
                        Some((k, v)) => {
                            self.current_key = Some(k);
                            self.current_data = Some(v);
                            self.current_lsn = noxu_util::NULL_LSN.as_u64();
                            self.current_index = 0;
                            self.state = CursorState::Initialized;
                            Ok(OperationStatus::Success)
                        }
                        None => Ok(OperationStatus::NotFound),
                    }
                }
            }
        }
    }

    /// Fetches the data associated with `key` from a tree (BIN-level lookup).
    ///
    /// Port of the data-read path in `CursorImpl.lockAndGetCurrent()`.
    fn get_data_from_tree(tree: &Tree, key: &[u8]) -> Option<Vec<u8>> {
        use noxu_tree::tree::TreeNode;
        let root = tree.get_root();
        let root = root.as_ref()?;
        // Descend to the BIN that should contain `key` (not always the leftmost).
        let bin_arc = Self::find_bin_for_key(root.clone(), key)?;
        let guard = bin_arc.read().ok()?;
        match &*guard {
            TreeNode::Bottom(bin) => {
                // BIN entries store compressed (suffix) keys under the BIN's
                // key_prefix. Compare against the suffix, not the full key.
                let suffix = bin.compress_key(key);
                bin.entries
                    .iter()
                    .find(|e| e.key.as_slice() == suffix.as_slice())
                    .and_then(|e| e.data.clone())
            }
            _ => None,
        }
    }

    /// Finds the first entry in the tree whose key >= `key`.
    ///
    /// Port of `Tree.searchRange()` — returns the first key in the BIN that
    /// compares >= the given search key.
    fn find_range_entry(tree: &Tree, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        use noxu_tree::tree::TreeNode;
        let root = tree.get_root();
        let root = root.as_ref()?;
        // Use find_bin_for_key so range searches also work for non-leftmost BINs.
        let bin_arc = Self::find_bin_for_key(root.clone(), key)?;
        let guard = bin_arc.read().ok()?;
        match &*guard {
            TreeNode::Bottom(bin) => {
                // BIN entries use compressed (suffix) keys; range-compare
                // against suffix and return the decompressed full key.
                let suffix = bin.compress_key(key);
                bin.entries
                    .iter()
                    .enumerate()
                    .find(|(_, e)| e.key.as_slice() >= suffix.as_slice())
                    .and_then(|(i, e)| {
                        bin.get_full_key(i)
                            .map(|fk| (fk, e.data.clone().unwrap_or_default()))
                    })
            }
            _ => None,
        }
    }

    /// Descends from the given node to the leftmost BIN, returning its Arc.
    fn descend_to_bin(
        node: std::sync::Arc<std::sync::RwLock<noxu_tree::tree::TreeNode>>,
    ) -> Option<std::sync::Arc<std::sync::RwLock<noxu_tree::tree::TreeNode>>>
    {
        use noxu_tree::tree::TreeNode;
        let mut current = node;
        loop {
            let (is_bin, child) = {
                let g = current.read().ok()?;
                let is_bin = g.is_bin();
                let child = if !is_bin {
                    match &*g {
                        TreeNode::Internal(n) => {
                            n.entries.first().and_then(|e| e.child.clone())
                        }
                        _ => None,
                    }
                } else {
                    None
                };
                (is_bin, child)
            };
            if is_bin {
                return Some(current);
            }
            current = child?;
        }
    }

    /// Descends from the given node to the rightmost BIN, returning its Arc.
    fn descend_to_last_bin(
        node: std::sync::Arc<std::sync::RwLock<noxu_tree::tree::TreeNode>>,
    ) -> Option<std::sync::Arc<std::sync::RwLock<noxu_tree::tree::TreeNode>>>
    {
        use noxu_tree::tree::TreeNode;
        let mut current = node;
        loop {
            let (is_bin, child) = {
                let g = current.read().ok()?;
                let is_bin = g.is_bin();
                let child = if !is_bin {
                    match &*g {
                        TreeNode::Internal(n) => {
                            n.entries.last().and_then(|e| e.child.clone())
                        }
                        _ => None,
                    }
                } else {
                    None
                };
                (is_bin, child)
            };
            if is_bin {
                return Some(current);
            }
            current = child?;
        }
    }

    /// Positions the cursor at the first (smallest) record in the database.
    ///
    /// Port of `CursorImpl.positionFirstOrLast(true)` from JE (line 1754).
    ///
    /// Uses `Tree::get_first_node()` to descend to the leftmost BIN, then
    /// positions the cursor at slot 0.
    ///
    /// # Returns
    ///
    /// * `Success` if the tree is non-empty
    /// * `NotFound` if the tree is empty
    pub fn get_first(&mut self) -> Result<OperationStatus, DbiError> {
        self.check_state()?;

        let entry: Option<(Vec<u8>, Vec<u8>, i32)> = {
            let db = self.db_impl.read();
            if let Some(tree) = db.get_real_tree() {
                if tree.is_empty() {
                    None
                } else {
                    use noxu_tree::tree::TreeNode;
                    let root = tree.get_root();
                    root.as_ref().and_then(|r| {
                        let bin_arc = Self::descend_to_bin(r.clone())?;
                        let g = bin_arc.read().ok()?;
                        match &*g {
                            TreeNode::Bottom(bin) => {
                                // Use get_full_key so prefix-compressed keys
                                // are correctly reconstructed.  Port of JE
                                // IN.getKey(int idx).
                                if bin.entries.is_empty() {
                                    None
                                } else {
                                    Some((
                                        bin.get_full_key(0).unwrap_or_default(),
                                        bin.entries[0].data.clone().unwrap_or_default(),
                                        0i32,
                                    ))
                                }
                            }
                            _ => None,
                        }
                    })
                }
            } else {
                None
            }
        };

        match entry {
            Some((key, data, idx)) => {
                self.current_key = Some(key);
                self.current_data = Some(data);
                self.current_lsn = noxu_util::NULL_LSN.as_u64();
                self.current_index = idx;
                self.state = CursorState::Initialized;
                Ok(OperationStatus::Success)
            }
            None => Ok(OperationStatus::NotFound),
        }
    }

    /// Positions the cursor at the last (largest) record in the database.
    ///
    /// Port of `CursorImpl.positionFirstOrLast(false)` from JE (line 1757).
    ///
    /// Uses `Tree::get_last_node()` to descend to the rightmost BIN, then
    /// positions the cursor at the last slot.
    ///
    /// # Returns
    ///
    /// * `Success` if the tree is non-empty
    /// * `NotFound` if the tree is empty
    pub fn get_last(&mut self) -> Result<OperationStatus, DbiError> {
        self.check_state()?;

        let entry: Option<(Vec<u8>, Vec<u8>, i32)> = {
            let db = self.db_impl.read();
            if let Some(tree) = db.get_real_tree() {
                if tree.is_empty() {
                    None
                } else {
                    use noxu_tree::tree::TreeNode;
                    let root = tree.get_root();
                    root.as_ref().and_then(|r| {
                        let bin_arc = Self::descend_to_last_bin(r.clone())?;
                        let g = bin_arc.read().ok()?;
                        match &*g {
                            TreeNode::Bottom(bin) => {
                                let n = bin.entries.len();
                                if n == 0 {
                                    None
                                } else {
                                    let last_idx = n - 1;
                                    // Use get_full_key to reconstruct
                                    // prefix-compressed keys correctly.
                                    Some((
                                        bin.get_full_key(last_idx).unwrap_or_default(),
                                        bin.entries[last_idx].data.clone().unwrap_or_default(),
                                        last_idx as i32,
                                    ))
                                }
                            }
                            _ => None,
                        }
                    })
                }
            } else {
                None
            }
        };

        match entry {
            Some((key, data, idx)) => {
                self.current_key = Some(key);
                self.current_data = Some(data);
                self.current_lsn = noxu_util::NULL_LSN.as_u64();
                self.current_index = idx;
                self.state = CursorState::Initialized;
                Ok(OperationStatus::Success)
            }
            None => Ok(OperationStatus::NotFound),
        }
    }

    /// Retrieves the current record.
    ///
    /// Returns the key and data at the cursor's current position.
    ///
    /// # Returns
    ///
    /// A tuple of (key, data) for the current record.
    ///
    /// # Errors
    ///
    /// * `CursorNotInitialized` if the cursor is not positioned on a record
    /// * `CursorClosed` if the cursor has been closed
    pub fn get_current(&self) -> Result<(Vec<u8>, Vec<u8>), DbiError> {
        self.check_initialized()?;

        let key =
            self.current_key.clone().ok_or(DbiError::CursorNotInitialized)?;
        let data = self.current_data.clone().unwrap_or_default();

        Ok((key, data))
    }

    /// Moves the cursor to the next/previous record.
    ///
    /// Port of `CursorImpl.getNext()` from JE (line 2546).
    ///
    /// Advances `current_index` within the current BIN.  When the BIN is
    /// exhausted (forward: `index >= nEntries`; backward: `index < 0`) the
    /// cursor moves to the adjacent BIN via `Tree::get_next_bin()` /
    /// `Tree::get_prev_bin()`, mirroring JE's call to
    /// `tree.getNextBin(anchorBIN)` / `tree.getPrevBin(anchorBIN)`.
    ///
    /// The GetMode parameter controls direction and duplicate handling:
    ///
    /// * `Next` / `NextNoDup` / `NextDup` — move forward
    /// * `Prev` / `PrevNoDup` / `PrevDup` — move backward
    ///
    /// # Returns
    ///
    /// * `Success` if positioned on a new record
    /// * `NotFound` if there are no more records in that direction
    pub fn retrieve_next(
        &mut self,
        mode: GetMode,
    ) -> Result<OperationStatus, DbiError> {
        self.check_state()?;

        // If not yet positioned, return NotFound (port of JE assertion
        // "mustBeInitialized" in getNext).
        if self.state == CursorState::NotInitialized {
            return Ok(OperationStatus::NotFound);
        }

        let forward = mode.is_forward();

        // Calculate the candidate next index within the current BIN.
        let next_index = if forward {
            self.current_index + 1
        } else {
            self.current_index - 1
        };

        // Try to read the entry at next_index from the BIN containing
        // current_key.
        let entry: Option<(Vec<u8>, Vec<u8>, i32)> = {
            let db = self.db_impl.read();
            if let Some(tree) = db.get_real_tree() {
                if tree.is_empty() {
                    None
                } else {
                    use noxu_tree::tree::TreeNode;
                    // Find the BIN that contains current_key by searching the
                    // tree for the key, then descend to that BIN.  We use
                    // descend_to_bin / descend_to_last_bin as a simplification:
                    // the search result tells us which BIN to look at.
                    let root = tree.get_root();
                    root.as_ref().and_then(|r| {
                        // Descend to the BIN that should contain current_key.
                        // For the correct BIN we use the search path rather
                        // than always going to the leftmost/rightmost BIN.
                        let current_key_slice = self.current_key.as_deref()?;
                        let bin_arc =
                            Self::find_bin_for_key(r.clone(), current_key_slice)?;
                        let g = bin_arc.read().ok()?;
                        match &*g {
                            TreeNode::Bottom(bin) => {
                                if next_index < 0
                                    || next_index >= bin.entries.len() as i32
                                {
                                    // BIN exhausted — signal caller to try
                                    // cross-BIN navigation.
                                    None
                                } else {
                                    let idx = next_index as usize;
                                    // Use get_full_key to reconstruct
                                    // prefix-compressed keys correctly.
                                    Some((
                                        bin.get_full_key(idx).unwrap_or_default(),
                                        bin.entries[idx].data.clone().unwrap_or_default(),
                                        next_index,
                                    ))
                                }
                            }
                            _ => None,
                        }
                    })
                }
            } else {
                None
            }
        };

        if let Some((key, data, idx)) = entry {
            self.current_key = Some(key);
            self.current_data = Some(data);
            self.current_lsn = noxu_util::NULL_LSN.as_u64();
            self.current_index = idx;
            return Ok(OperationStatus::Success);
        }

        // Current BIN is exhausted.  Port of JE CursorImpl.getNext() lines
        // 2605–2648: call tree.getNextBin(anchorBIN) / tree.getPrevBin().
        // `current_key` is the anchor: the last key we were positioned on in
        // the exhausted BIN (JE uses the BIN reference itself; we use the key
        // since our tree API is key-addressed).
        let anchor_key: Vec<u8> = match &self.current_key {
            Some(k) => k.clone(),
            None => return Ok(OperationStatus::NotFound),
        };

        let adjacent_entries: Option<Vec<noxu_tree::tree::BinEntry>> = {
            let db = self.db_impl.read();
            if let Some(tree) = db.get_real_tree() {
                if forward {
                    tree.get_next_bin(&anchor_key)
                } else {
                    tree.get_prev_bin(&anchor_key)
                }
            } else {
                None
            }
        };

        match adjacent_entries {
            Some(entries) if !entries.is_empty() => {
                // Position at the first entry of the next BIN (forward) or
                // the last entry of the previous BIN (backward).
                // Port of JE: index = -1 then ++index for forward;
                //             index = bin.getNEntries() then --index for backward.
                let (key, data, idx) = if forward {
                    let e = entries.into_iter().next().unwrap();
                    (e.key, e.data.unwrap_or_default(), 0i32)
                } else {
                    let last_idx = (entries.len() - 1) as i32;
                    let e = entries.into_iter().last().unwrap();
                    (e.key, e.data.unwrap_or_default(), last_idx)
                };
                self.current_key = Some(key);
                self.current_data = Some(data);
                self.current_lsn = noxu_util::NULL_LSN.as_u64();
                self.current_index = idx;
                Ok(OperationStatus::Success)
            }
            _ => Ok(OperationStatus::NotFound),
        }
    }

    /// Descends from `node` to the BIN whose key range contains `key`.
    ///
    /// This mirrors the search path in `Tree::search()` — at each upper IN
    /// we follow the child slot with the largest key <= `key`.  Returns the
    /// `Arc` of the matching BIN, or `None` if the tree is empty / malformed.
    fn find_bin_for_key(
        node: std::sync::Arc<std::sync::RwLock<noxu_tree::tree::TreeNode>>,
        key: &[u8],
    ) -> Option<std::sync::Arc<std::sync::RwLock<noxu_tree::tree::TreeNode>>>
    {
        use noxu_tree::tree::TreeNode;
        let mut current = node;
        loop {
            let (is_bin, child) = {
                let g = current.read().ok()?;
                let is_bin = g.is_bin();
                let child = if !is_bin {
                    match &*g {
                        TreeNode::Internal(n) => {
                            if n.entries.is_empty() {
                                return None;
                            }
                            // Slot 0 carries a virtual key (-infinity); follow
                            // the largest key <= search key (same logic as
                            // Tree::search and Tree::insert_recursive).
                            let mut idx = 0usize;
                            for (i, entry) in n.entries.iter().enumerate() {
                                if i == 0 {
                                    idx = 0;
                                } else if entry.key.as_slice() <= key {
                                    idx = i;
                                } else {
                                    break;
                                }
                            }
                            n.entries.get(idx).and_then(|e| e.child.clone())
                        }
                        _ => None,
                    }
                } else {
                    None
                };
                (is_bin, child)
            };
            if is_bin {
                return Some(current);
            }
            current = child?;
        }
    }

    /// Inserts or updates a record at the cursor position.
    ///
    /// Port of the write path in `CursorImpl.put()` from JE:
    ///
    /// 1. Checks state and, for `Current` mode, that the cursor is initialized.
    /// 2. For `NoOverwrite`: searches the tree; returns `KeyExist` if found.
    /// 3. Calls `Tree::insert(key, data, lsn)` to insert/update in the BIN.
    /// 4. Updates the cursor position to the newly written record.
    ///
    /// Note: locking (step 2 in JE) and WAL logging (step 3 in JE) are not
    /// yet wired here — they require LogManager integration (P0 gap).
    ///
    /// # Arguments
    ///
    /// * `key` - The key to insert/update
    /// * `data` - The data value
    /// * `put_mode` - The insertion mode
    ///
    /// # Returns
    ///
    /// * `Success` if the record was inserted/updated
    /// * `KeyExist` if NoOverwrite mode and key already exists
    pub fn put(
        &mut self,
        key: &[u8],
        data: &[u8],
        put_mode: PutMode,
    ) -> Result<OperationStatus, DbiError> {
        self.check_state()?;

        match put_mode {
            PutMode::Current => {
                // Current mode requires cursor to be positioned (JE: must be initialized).
                self.check_initialized()?;
                // Update data in-place in the tree.
                let current_key = self
                    .current_key
                    .clone()
                    .ok_or(DbiError::CursorNotInitialized)?;
                let mut db = self.db_impl.write();
                if let Some(tree) = db.get_real_tree_mut() {
                    let lsn = Lsn::from_u64(self.current_lsn);
                    // insert() with the same key will update the existing slot.
                    let _ = tree.insert(current_key, data.to_vec(), lsn);
                }
                self.current_data = Some(data.to_vec());
                Ok(OperationStatus::Success)
            }
            PutMode::NoOverwrite => {
                // Check if key already exists in the tree.
                let key_exists = {
                    let db = self.db_impl.read();
                    if let Some(tree) = db.get_real_tree() {
                        tree.search(key)
                            .map(|sr| sr.exact_parent_found)
                            .unwrap_or(false)
                    } else {
                        false
                    }
                };
                if key_exists {
                    return Ok(OperationStatus::KeyExist);
                }
                // Insert the new record.
                let new_lsn = Lsn::from_u64(noxu_util::NULL_LSN.as_u64());
                {
                    let mut db = self.db_impl.write();
                    if let Some(tree) = db.get_real_tree_mut() {
                        let _ = tree.insert(key.to_vec(), data.to_vec(), new_lsn);
                    }
                }
                self.current_key = Some(key.to_vec());
                self.current_data = Some(data.to_vec());
                self.current_lsn = noxu_util::NULL_LSN.as_u64();
                self.current_index = 0;
                self.state = CursorState::Initialized;
                Ok(OperationStatus::Success)
            }
            PutMode::Overwrite | PutMode::NoDupData => {
                // Insert or update the record unconditionally.
                let new_lsn = Lsn::from_u64(noxu_util::NULL_LSN.as_u64());
                {
                    let mut db = self.db_impl.write();
                    if let Some(tree) = db.get_real_tree_mut() {
                        let _ = tree.insert(key.to_vec(), data.to_vec(), new_lsn);
                    }
                }
                self.current_key = Some(key.to_vec());
                self.current_data = Some(data.to_vec());
                self.current_lsn = noxu_util::NULL_LSN.as_u64();
                self.current_index = 0;
                self.state = CursorState::Initialized;
                Ok(OperationStatus::Success)
            }
        }
    }

    /// Deletes the record at the cursor position.
    ///
    /// Port of the delete path in `CursorImpl.delete()` from JE:
    ///
    /// 1. Checks that the cursor is initialized.
    /// 2. Calls `Tree::delete(key)` to remove the entry from the BIN.
    /// 3. Resets cursor to NotInitialized (matching JE behaviour).
    ///
    /// Note: write locking and WAL log entry emission are not yet wired
    /// (P0 gap — requires LogManager integration).
    ///
    /// # Returns
    ///
    /// * `Success` if the record was deleted
    ///
    /// # Errors
    ///
    /// * `CursorNotInitialized` if cursor is not positioned
    /// * `CursorClosed` if cursor has been closed
    pub fn delete(&mut self) -> Result<OperationStatus, DbiError> {
        self.check_initialized()?;

        // Remove the record from the real tree.
        if let Some(key) = self.current_key.clone() {
            let mut db = self.db_impl.write();
            if let Some(tree) = db.get_real_tree_mut() {
                tree.delete(&key);
            }
        }

        self.current_key = None;
        self.current_data = None;
        self.current_lsn = noxu_util::NULL_LSN.as_u64();
        self.current_index = -1;
        self.state = CursorState::NotInitialized;

        Ok(OperationStatus::Success)
    }

    /// Counts the number of duplicates at the current key position.
    ///
    /// In a full implementation with duplicate support, this would
    /// traverse all records with the same key and count them.
    ///
    /// For now, always returns 1 since we don't have full duplicate
    /// support implemented.
    ///
    /// # Returns
    ///
    /// The count of duplicate records (currently always 1).
    ///
    /// # Errors
    ///
    /// * `CursorNotInitialized` if cursor is not positioned
    /// * `CursorClosed` if cursor has been closed
    pub fn count(&self) -> Result<i64, DbiError> {
        self.check_initialized()?;

        // Simplified: always 1 (no real dup counting)
        // In a full implementation:
        // 1. Save current position
        // 2. Search for first duplicate of current key
        // 3. Count all records until key changes
        // 4. Restore position
        Ok(1)
    }

    /// Creates a duplicate of this cursor at the same position.
    ///
    /// If `same_position` is true, the new cursor is positioned at the
    /// same record as this cursor. Otherwise, the new cursor is created
    /// in the NotInitialized state.
    ///
    /// The duplicated cursor shares the same locker (transaction) as
    /// the original cursor.
    ///
    /// # Arguments
    ///
    /// * `same_position` - Whether to copy the current position
    ///
    /// # Returns
    ///
    /// A new CursorImpl with the same or uninitialized position.
    ///
    /// # Errors
    ///
    /// * `CursorClosed` if the cursor has been closed
    pub fn dup(&self, same_position: bool) -> Result<CursorImpl, DbiError> {
        self.check_state()?;

        let mut new_cursor =
            CursorImpl::new(self.db_impl.clone(), self.locker_id);

        if same_position && self.state == CursorState::Initialized {
            new_cursor.current_key = self.current_key.clone();
            new_cursor.current_data = self.current_data.clone();
            new_cursor.current_lsn = self.current_lsn;
            new_cursor.current_index = self.current_index;
            new_cursor.state = CursorState::Initialized;
        }

        Ok(new_cursor)
    }

    /// Closes the cursor.
    ///
    /// Releases all resources held by the cursor, including any BIN latches
    /// and cursor-level locks. After closing, all operations on the cursor
    /// will return `CursorClosed` errors.
    ///
    /// Closing a cursor multiple times is safe and has no effect after the
    /// first close.
    ///
    /// # Returns
    ///
    /// `Ok(())` always (never fails).
    pub fn close(&mut self) -> Result<(), DbiError> {
        if self.state == CursorState::Closed {
            return Ok(());
        }

        // In a full implementation, would:
        // 1. Release BIN latch if held
        // 2. Release cursor-level locks
        // 3. Unregister from DatabaseImpl's cursor tracking

        self.current_key = None;
        self.current_data = None;
        self.current_lsn = noxu_util::NULL_LSN.as_u64();
        self.current_index = -1;
        self.state = CursorState::Closed;

        Ok(())
    }
}

impl Drop for CursorImpl {
    /// Ensures the cursor is closed when dropped.
    ///
    /// This provides automatic cleanup if the user forgets to explicitly
    /// close the cursor. Note that it's still better practice to call
    /// close() explicitly to handle potential errors.
    fn drop(&mut self) {
        if self.state != CursorState::Closed {
            let _ = self.close();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DatabaseConfig, DatabaseId, DbType};

    /// Creates a test DatabaseImpl for cursor testing.
    fn create_test_database() -> Arc<RwLock<DatabaseImpl>> {
        let db_id = DatabaseId::new(1);
        let config = DatabaseConfig::default();
        let db_impl = DatabaseImpl::new(
            db_id,
            "test_db".to_string(),
            DbType::User,
            &config,
        );
        Arc::new(RwLock::new(db_impl))
    }

    #[test]
    fn test_new_cursor_not_initialized() {
        let db = create_test_database();
        let cursor = CursorImpl::new(db, 100);

        assert!(!cursor.is_initialized());
        assert!(!cursor.is_closed());
        assert_eq!(cursor.get_locker_id(), 100);
        assert!(cursor.get_current_key().is_none());
        assert!(cursor.get_current_data().is_none());
    }

    #[test]
    fn test_search_positions_cursor() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        let key = b"test_key";
        let data = b"test_data";

        // Insert into tree first, then search.
        cursor.put(key, data, PutMode::Overwrite).unwrap();
        let status = cursor.search(key, Some(data), SearchMode::Set).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert!(cursor.is_initialized());
        assert_eq!(cursor.get_current_key(), Some(key.as_slice()));
        assert_eq!(cursor.get_current_data(), Some(data.as_slice()));
    }

    #[test]
    fn test_get_current_after_search() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        let key = b"my_key";
        let data = b"my_data";

        // Insert into tree first, then search.
        cursor.put(key, data, PutMode::Overwrite).unwrap();
        cursor.search(key, Some(data), SearchMode::Set).unwrap();
        let (ret_key, ret_data) = cursor.get_current().unwrap();

        assert_eq!(ret_key, key);
        assert_eq!(ret_data, data);
    }

    #[test]
    fn test_get_current_before_initialization() {
        let db = create_test_database();
        let cursor = CursorImpl::new(db, 100);

        let result = cursor.get_current();
        assert!(matches!(result, Err(DbiError::CursorNotInitialized)));
    }

    #[test]
    fn test_retrieve_next_from_uninitialized() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        let status = cursor.retrieve_next(GetMode::Next).unwrap();
        assert_eq!(status, OperationStatus::NotFound);
    }

    #[test]
    fn test_put_overwrite() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        let key = b"key1";
        let data = b"data1";

        let status = cursor.put(key, data, PutMode::Overwrite).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert!(cursor.is_initialized());
        assert_eq!(cursor.get_current_key(), Some(key.as_slice()));
    }

    #[test]
    fn test_put_no_overwrite_when_key_exists() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        let key = b"key1";
        let data1 = b"data1";
        let data2 = b"data2";

        // First put succeeds
        cursor.put(key, data1, PutMode::Overwrite).unwrap();

        // Second put with NoOverwrite should return KeyExist
        let status = cursor.put(key, data2, PutMode::NoOverwrite).unwrap();
        assert_eq!(status, OperationStatus::KeyExist);
    }

    #[test]
    fn test_put_current_requires_initialization() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        let key = b"key1";
        let data = b"data1";

        let result = cursor.put(key, data, PutMode::Current);
        assert!(matches!(result, Err(DbiError::CursorNotInitialized)));
    }

    #[test]
    fn test_put_current_after_initialization() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        let key = b"key1";
        let data1 = b"data1";
        let data2 = b"data2";

        // Insert first, then search to position cursor, then update with Current mode.
        cursor.put(key, data1, PutMode::Overwrite).unwrap();
        cursor.search(key, Some(data1), SearchMode::Set).unwrap();

        // Update with Current mode
        let status = cursor.put(key, data2, PutMode::Current).unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert_eq!(cursor.get_current_data(), Some(data2.as_slice()));
    }

    #[test]
    fn test_delete_requires_initialization() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        let result = cursor.delete();
        assert!(matches!(result, Err(DbiError::CursorNotInitialized)));
    }

    #[test]
    fn test_delete_resets_state() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        let key = b"key1";
        let data = b"data1";

        // Insert, search to position, then delete.
        cursor.put(key, data, PutMode::Overwrite).unwrap();
        cursor.search(key, Some(data), SearchMode::Set).unwrap();
        assert!(cursor.is_initialized());

        // Delete
        let status = cursor.delete().unwrap();
        assert_eq!(status, OperationStatus::Success);
        assert!(!cursor.is_initialized());
        assert!(cursor.get_current_key().is_none());
    }

    #[test]
    fn test_dup_with_same_position() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db.clone(), 100);

        let key = b"key1";
        let data = b"data1";

        // Insert, search to position, then dup.
        cursor.put(key, data, PutMode::Overwrite).unwrap();
        cursor.search(key, Some(data), SearchMode::Set).unwrap();

        // Duplicate with same position
        let dup_cursor = cursor.dup(true).unwrap();
        assert!(dup_cursor.is_initialized());
        assert_eq!(dup_cursor.get_current_key(), Some(key.as_slice()));
        assert_eq!(dup_cursor.get_current_data(), Some(data.as_slice()));
        assert_eq!(dup_cursor.get_locker_id(), 100);

        // Should have different IDs
        assert_ne!(cursor.get_id(), dup_cursor.get_id());
    }

    #[test]
    fn test_dup_without_same_position() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db.clone(), 100);

        let key = b"key1";
        let data = b"data1";

        // Insert, search to position, then dup without position.
        cursor.put(key, data, PutMode::Overwrite).unwrap();
        cursor.search(key, Some(data), SearchMode::Set).unwrap();

        // Duplicate without position
        let dup_cursor = cursor.dup(false).unwrap();
        assert!(!dup_cursor.is_initialized());
        assert!(dup_cursor.get_current_key().is_none());
        assert_eq!(dup_cursor.get_locker_id(), 100);
    }

    #[test]
    fn test_close_sets_state() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        cursor.close().unwrap();
        assert!(cursor.is_closed());
    }

    #[test]
    fn test_operations_after_close() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        cursor.close().unwrap();

        // All operations should return CursorClosed
        assert!(matches!(
            cursor.search(b"key", None, SearchMode::Set),
            Err(DbiError::CursorClosed)
        ));
        assert!(matches!(cursor.get_current(), Err(DbiError::CursorClosed)));
        assert!(matches!(
            cursor.retrieve_next(GetMode::Next),
            Err(DbiError::CursorClosed)
        ));
        assert!(matches!(
            cursor.put(b"key", b"data", PutMode::Overwrite),
            Err(DbiError::CursorClosed)
        ));
        assert!(matches!(cursor.delete(), Err(DbiError::CursorClosed)));
        assert!(matches!(cursor.count(), Err(DbiError::CursorClosed)));
        assert!(matches!(cursor.dup(true), Err(DbiError::CursorClosed)));
    }

    #[test]
    fn test_close_idempotent() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        cursor.close().unwrap();
        cursor.close().unwrap(); // Should not panic
        assert!(cursor.is_closed());
    }

    #[test]
    fn test_drop_calls_close() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db.clone(), 100);

        let key = b"key1";
        let data = b"data1";
        cursor.put(key, data, PutMode::Overwrite).unwrap();
        cursor.search(key, Some(data), SearchMode::Set).unwrap();

        // Drop without explicit close
        drop(cursor);

        // Create another cursor to verify no issues
        let cursor2 = CursorImpl::new(db, 200);
        assert!(!cursor2.is_closed());
    }

    #[test]
    fn test_count_returns_one() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        let key = b"key1";
        let data = b"data1";
        cursor.put(key, data, PutMode::Overwrite).unwrap();
        cursor.search(key, Some(data), SearchMode::Set).unwrap();

        let count = cursor.count().unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_unique_cursor_ids() {
        let db = create_test_database();
        let cursor1 = CursorImpl::new(db.clone(), 100);
        let cursor2 = CursorImpl::new(db.clone(), 100);
        let cursor3 = CursorImpl::new(db, 100);

        assert_ne!(cursor1.get_id(), cursor2.get_id());
        assert_ne!(cursor2.get_id(), cursor3.get_id());
        assert_ne!(cursor1.get_id(), cursor3.get_id());
    }

    // -----------------------------------------------------------------------
    // New unit tests for real B-tree traversal (get_first, get_last,
    // retrieve_next).
    // -----------------------------------------------------------------------

    /// get_first on an empty database returns NotFound.
    ///
    /// Port of JE CursorImplTest: positionFirstOrLast on an empty tree.
    #[test]
    fn test_get_first_empty_tree() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);
        let status = cursor.get_first().unwrap();
        assert_eq!(status, OperationStatus::NotFound);
    }

    /// get_last on an empty database returns NotFound.
    #[test]
    fn test_get_last_empty_tree() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);
        let status = cursor.get_last().unwrap();
        assert_eq!(status, OperationStatus::NotFound);
    }

    /// get_first positions at smallest key after multiple puts.
    #[test]
    fn test_get_first_after_multiple_puts() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        cursor.put(b"mango", b"m", PutMode::Overwrite).unwrap();
        cursor.put(b"apple", b"a", PutMode::Overwrite).unwrap();
        cursor.put(b"kiwi", b"k", PutMode::Overwrite).unwrap();

        let s = cursor.get_first().unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(cursor.get_current_key(), Some(b"apple".as_slice()));
        assert_eq!(cursor.get_current_data(), Some(b"a".as_slice()));
    }

    /// get_last positions at largest key after multiple puts.
    #[test]
    fn test_get_last_after_multiple_puts() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        cursor.put(b"apple", b"a", PutMode::Overwrite).unwrap();
        cursor.put(b"mango", b"m", PutMode::Overwrite).unwrap();
        cursor.put(b"kiwi", b"k", PutMode::Overwrite).unwrap();

        let s = cursor.get_last().unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(cursor.get_current_key(), Some(b"mango".as_slice()));
        assert_eq!(cursor.get_current_data(), Some(b"m".as_slice()));
    }

    /// retrieve_next(Next) advances forward through the BIN.
    ///
    /// Port of JE CursorImplTest.testGetNext().
    #[test]
    fn test_retrieve_next_forward() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        cursor.put(b"a", b"1", PutMode::Overwrite).unwrap();
        cursor.put(b"b", b"2", PutMode::Overwrite).unwrap();
        cursor.put(b"c", b"3", PutMode::Overwrite).unwrap();

        cursor.get_first().unwrap();
        assert_eq!(cursor.get_current_key(), Some(b"a".as_slice()));

        let s = cursor.retrieve_next(GetMode::Next).unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(cursor.get_current_key(), Some(b"b".as_slice()));

        let s = cursor.retrieve_next(GetMode::Next).unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(cursor.get_current_key(), Some(b"c".as_slice()));

        let s = cursor.retrieve_next(GetMode::Next).unwrap();
        assert_eq!(s, OperationStatus::NotFound, "should be exhausted");
    }

    /// retrieve_next(Prev) traverses backward through the BIN.
    ///
    /// Port of JE CursorImplTest.testGetPrev().
    #[test]
    fn test_retrieve_next_backward() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        cursor.put(b"a", b"1", PutMode::Overwrite).unwrap();
        cursor.put(b"b", b"2", PutMode::Overwrite).unwrap();
        cursor.put(b"c", b"3", PutMode::Overwrite).unwrap();

        cursor.get_last().unwrap();
        assert_eq!(cursor.get_current_key(), Some(b"c".as_slice()));

        let s = cursor.retrieve_next(GetMode::Prev).unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(cursor.get_current_key(), Some(b"b".as_slice()));

        let s = cursor.retrieve_next(GetMode::Prev).unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(cursor.get_current_key(), Some(b"a".as_slice()));

        let s = cursor.retrieve_next(GetMode::Prev).unwrap();
        assert_eq!(s, OperationStatus::NotFound, "should be exhausted");
    }

    /// A single key: get_first succeeds; retrieve_next(Next) returns NotFound.
    #[test]
    fn test_single_entry_traversal() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        cursor.put(b"only", b"val", PutMode::Overwrite).unwrap();

        let s = cursor.get_first().unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(cursor.get_current_key(), Some(b"only".as_slice()));

        let s = cursor.retrieve_next(GetMode::Next).unwrap();
        assert_eq!(s, OperationStatus::NotFound);
    }

    /// retrieve_next from NotInitialized state returns NotFound (not an error).
    ///
    /// Port of JE: getNext asserts mustBeInitialized; we convert this to
    /// NotFound per Rust convention.
    #[test]
    fn test_retrieve_next_from_not_initialized_returns_not_found() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        let s = cursor.retrieve_next(GetMode::Next).unwrap();
        assert_eq!(s, OperationStatus::NotFound);
    }

    /// put + NoOverwrite returns KeyExist when key is already in the tree.
    #[test]
    fn test_put_no_overwrite_tree_check() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        cursor.put(b"key", b"v1", PutMode::Overwrite).unwrap();
        let s = cursor.put(b"key", b"v2", PutMode::NoOverwrite).unwrap();
        assert_eq!(s, OperationStatus::KeyExist);

        // Verify original value is still there.
        cursor.search(b"key", None, SearchMode::Set).unwrap();
        let (_, data) = cursor.get_current().unwrap();
        assert_eq!(data, b"v1");
    }

    /// After delete the tree no longer contains the key (search returns NotFound).
    #[test]
    fn test_delete_removes_from_tree() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        cursor.put(b"key", b"val", PutMode::Overwrite).unwrap();
        cursor.search(b"key", None, SearchMode::Set).unwrap();
        cursor.delete().unwrap();

        let s = cursor.search(b"key", None, SearchMode::Set).unwrap();
        assert_eq!(s, OperationStatus::NotFound);
    }

    /// Range search: positions at the first key >= search key.
    #[test]
    fn test_search_set_range_finds_ge_key() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        cursor.put(b"aaa", b"1", PutMode::Overwrite).unwrap();
        cursor.put(b"bbb", b"2", PutMode::Overwrite).unwrap();
        cursor.put(b"ccc", b"3", PutMode::Overwrite).unwrap();

        // Search for "bb" (not present) — should land on "bbb".
        let s = cursor.search(b"bb", None, SearchMode::SetRange).unwrap();
        assert_eq!(s, OperationStatus::Success);
        assert_eq!(cursor.get_current_key(), Some(b"bbb".as_slice()));
    }

    /// Range search beyond all keys returns NotFound.
    #[test]
    fn test_search_set_range_beyond_all_keys() {
        let db = create_test_database();
        let mut cursor = CursorImpl::new(db, 100);

        cursor.put(b"aaa", b"1", PutMode::Overwrite).unwrap();
        cursor.put(b"bbb", b"2", PutMode::Overwrite).unwrap();

        let s = cursor.search(b"zzz", None, SearchMode::SetRange).unwrap();
        assert_eq!(s, OperationStatus::NotFound);
    }

    // -----------------------------------------------------------------------
    // Cross-BIN cursor traversal test
    // -----------------------------------------------------------------------

    /// Full forward scan visits all 200 entries across multiple BINs in sorted
    /// order.
    ///
    /// We use a DatabaseImpl whose underlying Tree is created with a small
    /// `max_entries_per_node` (4) so that 200 inserts force many splits and
    /// fill multiple BINs.  The cursor must cross every BIN boundary without
    /// losing any entry.
    ///
    /// Port of JE CursorImplTest multi-BIN scan: insert N records, open
    /// cursor at first, call getNext() until NotFound, assert count == N and
    /// keys are in ascending order.
    #[test]
    fn test_full_scan_crosses_multiple_bins() {
        // Build a database with a small node fanout (4) so 200 inserts force
        // many BIN splits.  DatabaseConfig::node_max_entries controls the
        // Tree::max_entries_per_node passed to Tree::new().
        let db_id = DatabaseId::new(42);
        let mut config = DatabaseConfig::default();
        config.set_node_max_entries(4); // tiny fanout → many BINs
        let db_impl = DatabaseImpl::new(
            db_id,
            "scan_test".to_string(),
            DbType::User,
            &config,
        );
        let db = Arc::new(RwLock::new(db_impl));

        const N: usize = 200;

        // Insert 200 entries with zero-padded decimal keys so lexicographic
        // order == numeric order.
        {
            let mut cursor = CursorImpl::new(db.clone(), 1);
            for i in 0..N {
                let key = format!("{:08}", i).into_bytes();
                let val = format!("v{}", i).into_bytes();
                let s = cursor.put(&key, &val, PutMode::Overwrite).unwrap();
                assert_eq!(s, OperationStatus::Success, "put {} failed", i);
            }
        }

        // Forward scan: get_first + repeated get_next.
        let mut cursor = CursorImpl::new(db.clone(), 2);
        let s = cursor.get_first().unwrap();
        assert_eq!(s, OperationStatus::Success, "get_first should succeed");

        let mut visited: Vec<Vec<u8>> = Vec::new();
        visited.push(cursor.get_current_key().unwrap().to_vec());

        loop {
            let s = cursor.retrieve_next(GetMode::Next).unwrap();
            match s {
                OperationStatus::Success => {
                    visited.push(cursor.get_current_key().unwrap().to_vec());
                }
                OperationStatus::NotFound => break,
                other => panic!("unexpected status {:?}", other),
            }
        }

        assert_eq!(
            visited.len(),
            N,
            "full scan must visit exactly {} entries, got {}",
            N,
            visited.len()
        );

        // Verify keys are in ascending (sorted) order.
        for i in 1..visited.len() {
            assert!(
                visited[i - 1] < visited[i],
                "keys out of order at position {}: {:?} >= {:?}",
                i,
                std::str::from_utf8(&visited[i - 1]).unwrap_or("?"),
                std::str::from_utf8(&visited[i]).unwrap_or("?"),
            );
        }

        // Backward scan: get_last + repeated get_prev.
        let mut cursor_back = CursorImpl::new(db.clone(), 3);
        let s = cursor_back.get_last().unwrap();
        assert_eq!(s, OperationStatus::Success, "get_last should succeed");

        let mut visited_back: Vec<Vec<u8>> = Vec::new();
        visited_back.push(cursor_back.get_current_key().unwrap().to_vec());

        loop {
            let s = cursor_back.retrieve_next(GetMode::Prev).unwrap();
            match s {
                OperationStatus::Success => {
                    visited_back
                        .push(cursor_back.get_current_key().unwrap().to_vec());
                }
                OperationStatus::NotFound => break,
                other => panic!("unexpected backward status {:?}", other),
            }
        }

        assert_eq!(
            visited_back.len(),
            N,
            "backward scan must visit exactly {} entries, got {}",
            N,
            visited_back.len()
        );

        // Backward scan should be the reverse of forward scan.
        let mut visited_back_rev = visited_back.clone();
        visited_back_rev.reverse();
        assert_eq!(
            visited_back_rev, visited,
            "backward scan reversed must equal forward scan"
        );
    }
}
