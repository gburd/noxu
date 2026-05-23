//! TigerBeetle-compatible Transfer type (128 bytes, fixed layout).

use crate::account::AccountId;

/// Unique identifier for a transfer.
pub type TransferId = u128;

/// Transfer flags (u16 bitfield, TigerBeetle-compatible layout).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransferFlags(pub u16);

impl TransferFlags {
    pub const LINKED: u16 = 1 << 0;
    pub const PENDING: u16 = 1 << 1;
    pub const POST_PENDING_TRANSFER: u16 = 1 << 2;
    pub const VOID_PENDING_TRANSFER: u16 = 1 << 3;
    pub const BALANCING_DEBIT: u16 = 1 << 4;
    pub const BALANCING_CREDIT: u16 = 1 << 5;

    pub fn linked(self) -> bool {
        self.0 & Self::LINKED != 0
    }
    pub fn pending(self) -> bool {
        self.0 & Self::PENDING != 0
    }
    pub fn post_pending_transfer(self) -> bool {
        self.0 & Self::POST_PENDING_TRANSFER != 0
    }
    pub fn void_pending_transfer(self) -> bool {
        self.0 & Self::VOID_PENDING_TRANSFER != 0
    }
}

/// A financial transfer between two accounts (128 bytes, TigerBeetle-compatible layout).
///
/// ```text
/// Offset  Size  Field
///   0      16   id
///  16      16   debit_account_id
///  32      16   credit_account_id
///  48      16   amount
///  64      16   pending_id
///  80      16   user_data_128
///  96       8   user_data_64
/// 104       4   user_data_32
/// 108       4   timeout
/// 112       4   ledger
/// 116       2   code
/// 118       2   flags
/// 120       8   timestamp
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
pub struct Transfer {
    pub id: u128,
    pub debit_account_id: AccountId,
    pub credit_account_id: AccountId,
    pub amount: u128,
    pub pending_id: u128,
    pub user_data_128: u128,
    pub user_data_64: u64,
    pub user_data_32: u32,
    pub timeout: u32,
    pub ledger: u32,
    pub code: u16,
    pub flags: TransferFlags,
    pub timestamp: u64,
}

const _: () = assert!(size_of::<Transfer>() == 128);

impl Transfer {
    pub const SIZE: usize = 128;

    /// Creates an immediate (non-pending) transfer.
    pub fn new(
        id: u128,
        debit_account_id: u128,
        credit_account_id: u128,
        amount: u128,
    ) -> Self {
        Self {
            id,
            debit_account_id,
            credit_account_id,
            amount,
            pending_id: 0,
            user_data_128: 0,
            user_data_64: 0,
            user_data_32: 0,
            timeout: 0,
            ledger: 0,
            code: 0,
            flags: TransferFlags(0),
            timestamp: 0,
        }
    }

    /// Creates a two-phase pending transfer.
    pub fn new_pending(
        id: u128,
        debit_account_id: u128,
        credit_account_id: u128,
        amount: u128,
    ) -> Self {
        let mut t = Self::new(id, debit_account_id, credit_account_id, amount);
        t.flags = TransferFlags(TransferFlags::PENDING);
        t
    }

    /// Creates a post-pending request.
    pub fn post_pending(id: u128, pending_id: u128) -> Self {
        Self {
            id,
            debit_account_id: 0,
            credit_account_id: 0,
            amount: 0,
            pending_id,
            user_data_128: 0,
            user_data_64: 0,
            user_data_32: 0,
            timeout: 0,
            ledger: 0,
            code: 0,
            flags: TransferFlags(TransferFlags::POST_PENDING_TRANSFER),
            timestamp: 0,
        }
    }

    /// Creates a void-pending request.
    pub fn void_pending(id: u128, pending_id: u128) -> Self {
        Self {
            id,
            debit_account_id: 0,
            credit_account_id: 0,
            amount: 0,
            pending_id,
            user_data_128: 0,
            user_data_64: 0,
            user_data_32: 0,
            timeout: 0,
            ledger: 0,
            code: 0,
            flags: TransferFlags(TransferFlags::VOID_PENDING_TRANSFER),
            timestamp: 0,
        }
    }

    pub fn is_pending(&self) -> bool {
        self.flags.pending()
    }
    pub fn is_post_request(&self) -> bool {
        self.flags.post_pending_transfer()
    }
    pub fn is_void_request(&self) -> bool {
        self.flags.void_pending_transfer()
    }

    /// Serializes to a 128-byte little-endian buffer.
    pub fn to_bytes(&self) -> [u8; 128] {
        let mut buf = [0u8; 128];
        buf[0..16].copy_from_slice(&self.id.to_le_bytes());
        buf[16..32].copy_from_slice(&self.debit_account_id.to_le_bytes());
        buf[32..48].copy_from_slice(&self.credit_account_id.to_le_bytes());
        buf[48..64].copy_from_slice(&self.amount.to_le_bytes());
        buf[64..80].copy_from_slice(&self.pending_id.to_le_bytes());
        buf[80..96].copy_from_slice(&self.user_data_128.to_le_bytes());
        buf[96..104].copy_from_slice(&self.user_data_64.to_le_bytes());
        buf[104..108].copy_from_slice(&self.user_data_32.to_le_bytes());
        buf[108..112].copy_from_slice(&self.timeout.to_le_bytes());
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
            debit_account_id: u128::from_le_bytes(
                buf[16..32].try_into().unwrap(),
            ),
            credit_account_id: u128::from_le_bytes(
                buf[32..48].try_into().unwrap(),
            ),
            amount: u128::from_le_bytes(buf[48..64].try_into().unwrap()),
            pending_id: u128::from_le_bytes(buf[64..80].try_into().unwrap()),
            user_data_128: u128::from_le_bytes(buf[80..96].try_into().unwrap()),
            user_data_64: u64::from_le_bytes(buf[96..104].try_into().unwrap()),
            user_data_32: u32::from_le_bytes(buf[104..108].try_into().unwrap()),
            timeout: u32::from_le_bytes(buf[108..112].try_into().unwrap()),
            ledger: u32::from_le_bytes(buf[112..116].try_into().unwrap()),
            code: u16::from_le_bytes(buf[116..118].try_into().unwrap()),
            flags: TransferFlags(u16::from_le_bytes(
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
        assert_eq!(size_of::<Transfer>(), 128);
    }

    #[test]
    fn test_roundtrip() {
        let t = Transfer::new_pending(0xABCD, 1, 2, 1_000_000);
        let bytes = t.to_bytes();
        let restored = Transfer::from_bytes(&bytes);
        assert_eq!(t, restored);
    }

    #[test]
    fn test_flags() {
        let t = Transfer::post_pending(10, 5);
        assert!(t.is_post_request());
        assert!(!t.is_pending());
        assert!(!t.is_void_request());
        assert_eq!(t.pending_id, 5);
    }
}
