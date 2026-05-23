//! TigerBeetle-compatible Account type (128 bytes, fixed layout).

/// Unique identifier for an account.
pub type AccountId = u128;

/// Account flags (u16 bitfield, TigerBeetle-compatible layout).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AccountFlags(pub u16);

impl AccountFlags {
    pub const LINKED: u16 = 1 << 0;
    pub const DEBITS_MUST_NOT_EXCEED_CREDITS: u16 = 1 << 1;
    pub const CREDITS_MUST_NOT_EXCEED_DEBITS: u16 = 1 << 2;
    pub const HISTORY: u16 = 1 << 3;

    pub fn linked(self) -> bool {
        self.0 & Self::LINKED != 0
    }
    pub fn debits_must_not_exceed_credits(self) -> bool {
        self.0 & Self::DEBITS_MUST_NOT_EXCEED_CREDITS != 0
    }
    pub fn credits_must_not_exceed_debits(self) -> bool {
        self.0 & Self::CREDITS_MUST_NOT_EXCEED_DEBITS != 0
    }
    pub fn history(self) -> bool {
        self.0 & Self::HISTORY != 0
    }
}

/// A double-entry bookkeeping account (128 bytes, TigerBeetle-compatible layout).
///
/// Field layout matches TigerBeetle exactly:
/// ```text
/// Offset  Size  Field
///   0      16   id
///  16      16   debits_pending
///  32      16   debits_posted
///  48      16   credits_pending
///  64      16   credits_posted
///  80      16   user_data_128
///  96       8   user_data_64
/// 104       4   user_data_32
/// 108       4   reserved
/// 112       4   ledger
/// 116       2   code
/// 118       2   flags
/// 120       8   timestamp
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct Account {
    pub id: u128,
    pub debits_pending: u128,
    pub debits_posted: u128,
    pub credits_pending: u128,
    pub credits_posted: u128,
    pub user_data_128: u128,
    pub user_data_64: u64,
    pub user_data_32: u32,
    pub reserved: u32,
    pub ledger: u32,
    pub code: u16,
    pub flags: AccountFlags,
    pub timestamp: u64,
}

const _: () = assert!(size_of::<Account>() == 128);

impl Account {
    pub const SIZE: usize = 128;

    /// Creates a new account with zero balances.
    pub fn new(id: u128, ledger: u32) -> Self {
        Self {
            id,
            debits_pending: 0,
            debits_posted: 0,
            credits_pending: 0,
            credits_posted: 0,
            user_data_128: 0,
            user_data_64: 0,
            user_data_32: 0,
            reserved: 0,
            ledger,
            code: 0,
            flags: AccountFlags(0),
            timestamp: 0,
        }
    }

    /// Net balance: credits_posted - debits_posted.
    pub fn balance(&self) -> i128 {
        self.credits_posted as i128 - self.debits_posted as i128
    }

    /// Available balance considering pending operations and flags.
    pub fn available_balance(&self) -> i128 {
        if self.flags.debits_must_not_exceed_credits() {
            self.credits_posted as i128
                - self.debits_posted as i128
                - self.debits_pending as i128
        } else if self.flags.credits_must_not_exceed_debits() {
            self.debits_posted as i128
                - self.credits_posted as i128
                - self.credits_pending as i128
        } else {
            self.balance()
        }
    }

    /// Returns true if the account can sustain a debit of `amount`.
    pub fn can_debit(&self, amount: u128) -> bool {
        if self.flags.debits_must_not_exceed_credits() {
            let total = self.debits_posted as i128
                + self.debits_pending as i128
                + amount as i128;
            total <= self.credits_posted as i128
        } else {
            true
        }
    }

    /// Returns true if the account can sustain a credit of `amount`.
    pub fn can_credit(&self, amount: u128) -> bool {
        if self.flags.credits_must_not_exceed_debits() {
            let total = self.credits_posted as i128
                + self.credits_pending as i128
                + amount as i128;
            total <= self.debits_posted as i128
        } else {
            true
        }
    }

    /// Applies a pending debit.
    pub fn apply_pending_debit(&mut self, amount: u128) {
        self.debits_pending = self.debits_pending.saturating_add(amount);
    }

    /// Applies a pending credit.
    pub fn apply_pending_credit(&mut self, amount: u128) {
        self.credits_pending = self.credits_pending.saturating_add(amount);
    }

    /// Posts a pending debit (moves from pending to posted).
    pub fn post_pending_debit(&mut self, amount: u128) {
        self.debits_pending = self.debits_pending.saturating_sub(amount);
        self.debits_posted = self.debits_posted.saturating_add(amount);
    }

    /// Posts a pending credit (moves from pending to posted).
    pub fn post_pending_credit(&mut self, amount: u128) {
        self.credits_pending = self.credits_pending.saturating_sub(amount);
        self.credits_posted = self.credits_posted.saturating_add(amount);
    }

    /// Voids a pending debit.
    pub fn void_pending_debit(&mut self, amount: u128) {
        self.debits_pending = self.debits_pending.saturating_sub(amount);
    }

    /// Voids a pending credit.
    pub fn void_pending_credit(&mut self, amount: u128) {
        self.credits_pending = self.credits_pending.saturating_sub(amount);
    }

    /// Serializes to a 128-byte little-endian buffer.
    pub fn to_bytes(&self) -> [u8; 128] {
        let mut buf = [0u8; 128];
        buf[0..16].copy_from_slice(&self.id.to_le_bytes());
        buf[16..32].copy_from_slice(&self.debits_pending.to_le_bytes());
        buf[32..48].copy_from_slice(&self.debits_posted.to_le_bytes());
        buf[48..64].copy_from_slice(&self.credits_pending.to_le_bytes());
        buf[64..80].copy_from_slice(&self.credits_posted.to_le_bytes());
        buf[80..96].copy_from_slice(&self.user_data_128.to_le_bytes());
        buf[96..104].copy_from_slice(&self.user_data_64.to_le_bytes());
        buf[104..108].copy_from_slice(&self.user_data_32.to_le_bytes());
        buf[108..112].copy_from_slice(&self.reserved.to_le_bytes());
        buf[112..116].copy_from_slice(&self.ledger.to_le_bytes());
        buf[116..118].copy_from_slice(&self.code.to_le_bytes());
        buf[118..120].copy_from_slice(&self.flags.0.to_le_bytes());
        buf[120..128].copy_from_slice(&self.timestamp.to_le_bytes());
        buf
    }

    /// Deserializes from a 128-byte little-endian buffer.
    pub fn from_bytes(buf: &[u8; 128]) -> Self {
        Self {
            id: u128::from_le_bytes(buf[0..16].try_into().unwrap()),
            debits_pending: u128::from_le_bytes(
                buf[16..32].try_into().unwrap(),
            ),
            debits_posted: u128::from_le_bytes(buf[32..48].try_into().unwrap()),
            credits_pending: u128::from_le_bytes(
                buf[48..64].try_into().unwrap(),
            ),
            credits_posted: u128::from_le_bytes(
                buf[64..80].try_into().unwrap(),
            ),
            user_data_128: u128::from_le_bytes(buf[80..96].try_into().unwrap()),
            user_data_64: u64::from_le_bytes(buf[96..104].try_into().unwrap()),
            user_data_32: u32::from_le_bytes(buf[104..108].try_into().unwrap()),
            reserved: u32::from_le_bytes(buf[108..112].try_into().unwrap()),
            ledger: u32::from_le_bytes(buf[112..116].try_into().unwrap()),
            code: u16::from_le_bytes(buf[116..118].try_into().unwrap()),
            flags: AccountFlags(u16::from_le_bytes(
                buf[118..120].try_into().unwrap(),
            )),
            timestamp: u64::from_le_bytes(buf[120..128].try_into().unwrap()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_is_128_bytes() {
        assert_eq!(size_of::<Account>(), 128);
    }

    #[test]
    fn test_roundtrip() {
        let mut acct = Account::new(0xDEAD_BEEF, 42);
        acct.credits_posted = 1_000_000;
        acct.debits_posted = 300_000;
        acct.user_data_128 = 0xFF;
        acct.flags = AccountFlags(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS);
        let bytes = acct.to_bytes();
        let restored = Account::from_bytes(&bytes);
        assert_eq!(acct, restored);
    }

    #[test]
    fn test_balance() {
        let mut acct = Account::new(1, 1);
        acct.credits_posted = 1000;
        acct.debits_posted = 300;
        assert_eq!(acct.balance(), 700);
    }

    #[test]
    fn test_can_debit_constraint() {
        let mut acct = Account::new(1, 1);
        acct.flags = AccountFlags(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS);
        acct.credits_posted = 500;
        assert!(acct.can_debit(500));
        assert!(!acct.can_debit(501));
    }
}
