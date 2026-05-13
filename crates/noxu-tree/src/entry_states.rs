//! Slot state bit flags for BIN entries.
//!
//!
//! Each slot in a BIN (Bottom Internal Node) has associated state flags
//! that track metadata about the entry (deleted, dirty, embedded, etc.).

/// Known deleted bit - slot is known to be deleted (obsolete).
pub const KNOWN_DELETED_BIT: u8 = 0x01;

/// Dirty bit - slot has been modified since last log write.
pub const DIRTY_BIT: u8 = 0x02;

/// Transient migrate bit - formerly used for migration, always transient.
/// 0x04 is reserved as transient forever (it was accidentally persisted
/// historically, so it cannot be reused as a persistent bit).
pub const MIGRATE_BIT: u8 = 0x04;

/// Pending deleted bit - delete is pending (not yet committed).
pub const PENDING_DELETED_BIT: u8 = 0x08;

/// Embedded LN bit - LN data is embedded directly in the BIN slot.
pub const EMBEDDED_LN_BIT: u8 = 0x10;

/// No data LN bit - LN has no data (zero-length embedded data).
pub const NO_DATA_LN_BIT: u8 = 0x20;

/// Update key when logged bit - transient flag to update key on next log.
/// 
pub const UPDATE_KEY_WHEN_LOGGED: u8 = 0x40;

/// Tombstone bit - slot is a blind-deletion tombstone.
/// 
pub const TOMBSTONE_BIT: u8 = 0x80;

/// Mask for transient state bits (not logged to disk).
/// 
/// Bit 0x04 (MIGRATE_BIT) is always transient; UPDATE_KEY_WHEN_LOGGED is also transient.
pub const TRANSIENT_BITS: u8 = MIGRATE_BIT | UPDATE_KEY_WHEN_LOGGED;

/// A newtype wrapper around slot state flags.
///
/// Provides type-safe access to individual state bits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SlotState(u8);

impl SlotState {
    /// Creates a new slot state with all flags cleared.
    #[inline]
    pub fn new() -> Self {
        SlotState(0)
    }

    /// Creates a slot state from a raw byte value.
    #[inline]
    pub fn from_byte(byte: u8) -> Self {
        SlotState(byte)
    }

    /// Returns the raw byte value of the state flags.
    #[inline]
    pub fn as_byte(self) -> u8 {
        self.0
    }

    /// Returns true if the known-deleted bit is set.
    #[inline]
    pub fn is_known_deleted(self) -> bool {
        (self.0 & KNOWN_DELETED_BIT) != 0
    }

    /// Sets the known-deleted bit.
    #[inline]
    pub fn set_known_deleted(&mut self) {
        self.0 |= KNOWN_DELETED_BIT;
    }

    /// Clears the known-deleted bit.
    #[inline]
    pub fn clear_known_deleted(&mut self) {
        self.0 &= !KNOWN_DELETED_BIT;
    }

    /// Returns true if the dirty bit is set.
    #[inline]
    pub fn is_dirty(self) -> bool {
        (self.0 & DIRTY_BIT) != 0
    }

    /// Sets the dirty bit.
    #[inline]
    pub fn set_dirty(&mut self) {
        self.0 |= DIRTY_BIT;
    }

    /// Clears the dirty bit.
    #[inline]
    pub fn clear_dirty(&mut self) {
        self.0 &= !DIRTY_BIT;
    }

    /// Returns true if the migrate (transient) bit is set.
    #[inline]
    pub fn is_migrate(self) -> bool {
        (self.0 & MIGRATE_BIT) != 0
    }

    /// Sets the migrate bit.
    #[inline]
    pub fn set_migrate(&mut self) {
        self.0 |= MIGRATE_BIT;
    }

    /// Clears the migrate bit.
    #[inline]
    pub fn clear_migrate(&mut self) {
        self.0 &= !MIGRATE_BIT;
    }

    /// Returns true if the pending-deleted bit is set.
    #[inline]
    pub fn is_pending_deleted(self) -> bool {
        (self.0 & PENDING_DELETED_BIT) != 0
    }

    /// Sets the pending-deleted bit.
    #[inline]
    pub fn set_pending_deleted(&mut self) {
        self.0 |= PENDING_DELETED_BIT;
    }

    /// Clears the pending-deleted bit.
    #[inline]
    pub fn clear_pending_deleted(&mut self) {
        self.0 &= !PENDING_DELETED_BIT;
    }

    /// Returns true if the embedded-LN bit is set.
    #[inline]
    pub fn is_embedded_ln(self) -> bool {
        (self.0 & EMBEDDED_LN_BIT) != 0
    }

    /// Sets the embedded-LN bit.
    #[inline]
    pub fn set_embedded_ln(&mut self) {
        self.0 |= EMBEDDED_LN_BIT;
    }

    /// Clears the embedded-LN bit.
    #[inline]
    pub fn clear_embedded_ln(&mut self) {
        self.0 &= !EMBEDDED_LN_BIT;
    }

    /// Returns true if the no-data-LN bit is set.
    #[inline]
    pub fn is_no_data_ln(self) -> bool {
        (self.0 & NO_DATA_LN_BIT) != 0
    }

    /// Sets the no-data-LN bit.
    #[inline]
    pub fn set_no_data_ln(&mut self) {
        self.0 |= NO_DATA_LN_BIT;
    }

    /// Clears the no-data-LN bit.
    #[inline]
    pub fn clear_no_data_ln(&mut self) {
        self.0 &= !NO_DATA_LN_BIT;
    }

    /// Returns true if the update-key-when-logged (transient) bit is set.
    ///
    /// 
    #[inline]
    pub fn is_update_key_when_logged(self) -> bool {
        (self.0 & UPDATE_KEY_WHEN_LOGGED) != 0
    }

    /// Sets the update-key-when-logged bit.
    #[inline]
    pub fn set_update_key_when_logged(&mut self) {
        self.0 |= UPDATE_KEY_WHEN_LOGGED;
    }

    /// Clears the update-key-when-logged bit.
    #[inline]
    pub fn clear_update_key_when_logged(&mut self) {
        self.0 &= !UPDATE_KEY_WHEN_LOGGED;
    }

    /// Returns true if the tombstone bit is set.
    ///
    /// A tombstone slot is a blind-deletion marker (extended capability).
    /// 
    #[inline]
    pub fn is_tombstone(self) -> bool {
        (self.0 & TOMBSTONE_BIT) != 0
    }

    /// Sets the tombstone bit.
    #[inline]
    pub fn set_tombstone(&mut self) {
        self.0 |= TOMBSTONE_BIT;
    }

    /// Clears the tombstone bit.
    #[inline]
    pub fn clear_tombstone(&mut self) {
        self.0 &= !TOMBSTONE_BIT;
    }

    /// Clears all transient bits (not persisted to disk).
    ///
    /// Transient bits are: MIGRATE_BIT (0x04), UPDATE_KEY_WHEN_LOGGED (0x40).
    /// 
    #[inline]
    pub fn clear_transient_bits(&mut self) {
        self.0 &= !TRANSIENT_BITS;
    }

    /// Returns a copy with transient bits cleared.
    #[inline]
    pub fn with_transient_bits_cleared(self) -> Self {
        SlotState(self.0 & !TRANSIENT_BITS)
    }
}

impl From<u8> for SlotState {
    fn from(byte: u8) -> Self {
        SlotState(byte)
    }
}

impl From<SlotState> for u8 {
    fn from(state: SlotState) -> Self {
        state.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_state_is_clear() {
        let state = SlotState::new();
        assert_eq!(state.as_byte(), 0);
        assert!(!state.is_known_deleted());
        assert!(!state.is_dirty());
        assert!(!state.is_embedded_ln());
    }

    #[test]
    fn test_known_deleted() {
        let mut state = SlotState::new();
        assert!(!state.is_known_deleted());

        state.set_known_deleted();
        assert!(state.is_known_deleted());
        assert_eq!(state.as_byte(), KNOWN_DELETED_BIT);

        state.clear_known_deleted();
        assert!(!state.is_known_deleted());
        assert_eq!(state.as_byte(), 0);
    }

    #[test]
    fn test_dirty() {
        let mut state = SlotState::new();
        assert!(!state.is_dirty());

        state.set_dirty();
        assert!(state.is_dirty());
        assert_eq!(state.as_byte(), DIRTY_BIT);

        state.clear_dirty();
        assert!(!state.is_dirty());
    }

    #[test]
    fn test_multiple_flags() {
        let mut state = SlotState::new();

        state.set_dirty();
        state.set_embedded_ln();
        assert!(state.is_dirty());
        assert!(state.is_embedded_ln());
        assert!(!state.is_known_deleted());
        assert_eq!(state.as_byte(), DIRTY_BIT | EMBEDDED_LN_BIT);

        state.clear_dirty();
        assert!(!state.is_dirty());
        assert!(state.is_embedded_ln());
    }

    #[test]
    fn test_transient_bits() {
        let mut state = SlotState::new();
        state.set_migrate();
        state.set_update_key_when_logged();
        state.set_dirty();

        assert!(state.is_migrate());
        assert!(state.is_update_key_when_logged());
        assert!(state.is_dirty());

        state.clear_transient_bits();
        assert!(!state.is_migrate());
        assert!(!state.is_update_key_when_logged());
        assert!(state.is_dirty()); // Non-transient bit should remain
    }

    #[test]
    fn test_with_transient_bits_cleared() {
        let mut state = SlotState::new();
        state.set_migrate();
        state.set_dirty();
        state.set_embedded_ln();

        let cleared = state.with_transient_bits_cleared();
        assert!(!cleared.is_migrate());
        assert!(cleared.is_dirty());
        assert!(cleared.is_embedded_ln());

        // Original should be unchanged
        assert!(state.is_migrate());
    }

    #[test]
    fn test_tombstone() {
        let mut state = SlotState::new();
        assert!(!state.is_tombstone());

        state.set_tombstone();
        assert!(state.is_tombstone());
        assert_eq!(state.as_byte(), TOMBSTONE_BIT);

        state.clear_tombstone();
        assert!(!state.is_tombstone());
    }

    #[test]
    fn test_update_key_when_logged() {
        let mut state = SlotState::new();
        assert!(!state.is_update_key_when_logged());

        state.set_update_key_when_logged();
        assert!(state.is_update_key_when_logged());
        // It's a transient bit, so it clears with clear_transient_bits
        state.clear_transient_bits();
        assert!(!state.is_update_key_when_logged());
    }

    #[test]
    fn test_from_byte() {
        let state = SlotState::from_byte(DIRTY_BIT | EMBEDDED_LN_BIT);
        assert!(state.is_dirty());
        assert!(state.is_embedded_ln());
        assert!(!state.is_known_deleted());
    }

    #[test]
    fn test_conversions() {
        let byte: u8 = DIRTY_BIT | PENDING_DELETED_BIT;
        let state = SlotState::from(byte);
        assert!(state.is_dirty());
        assert!(state.is_pending_deleted());

        let back: u8 = state.into();
        assert_eq!(back, byte);
    }

    #[test]
    fn test_all_flags() {
        let mut state = SlotState::new();

        // Set all persistent + transient flags
        state.set_known_deleted();
        state.set_dirty();
        state.set_migrate();
        state.set_pending_deleted();
        state.set_embedded_ln();
        state.set_no_data_ln();
        state.set_update_key_when_logged();
        state.set_tombstone();

        // Check all are set
        assert!(state.is_known_deleted());
        assert!(state.is_dirty());
        assert!(state.is_migrate());
        assert!(state.is_pending_deleted());
        assert!(state.is_embedded_ln());
        assert!(state.is_no_data_ln());
        assert!(state.is_update_key_when_logged());
        assert!(state.is_tombstone());
    }
}
