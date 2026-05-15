//! Error types and TigerBeetle-compatible result codes.

use thiserror::Error;

/// Errors that can occur during FTDB operations.
#[derive(Debug, Error)]
pub enum FtdbError {
    #[error("storage error: {0}")]
    Storage(#[from] noxu_db::NoxuError),

    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("protocol error: {0}")]
    Protocol(String),
}

/// TigerBeetle-compatible result codes for create_accounts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum CreateAccountResult {
    Ok = 0,
    LinkedEventFailed = 1,
    LinkedEventChainOpen = 2,
    IdMustNotBeZero = 18,
    IdMustNotBeMax = 19,
    FlagsAreMutuallyExclusive = 22,
    LedgerMustNotBeZero = 26,
    CodeMustNotBeZero = 27,
    Exists = 32,
    ExistsWithDifferentFlags = 33,
    ExistsWithDifferentLedger = 35,
    ExistsWithDifferentCode = 36,
}

/// TigerBeetle-compatible result codes for create_transfers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum CreateTransferResult {
    Ok = 0,
    LinkedEventFailed = 1,
    LinkedEventChainOpen = 2,
    IdMustNotBeZero = 18,
    IdMustNotBeMax = 19,
    DebitAccountIdMustNotBeZero = 22,
    CreditAccountIdMustNotBeZero = 24,
    AccountsMustBeDifferent = 26,
    AmountMustNotBeZero = 30,
    PendingIdMustBeZero = 27,
    PendingIdMustNotBeZero = 28,
    DebitAccountNotFound = 33,
    CreditAccountNotFound = 34,
    ExceedsCredits = 38,
    ExceedsDebits = 39,
    Exists = 42,
    PendingTransferNotFound = 44,
    PendingTransferNotPending = 45,
    PendingTransferAlreadyPosted = 46,
    PendingTransferAlreadyVoided = 47,
}

/// A per-record result returned in batch responses.
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct BatchResult {
    pub index: u32,
    pub result: u32,
}

impl BatchResult {
    pub const SIZE: usize = 8;

    pub fn to_bytes(self) -> [u8; 8] {
        let mut buf = [0u8; 8];
        buf[0..4].copy_from_slice(&self.index.to_le_bytes());
        buf[4..8].copy_from_slice(&self.result.to_le_bytes());
        buf
    }
}
