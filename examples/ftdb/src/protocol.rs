//! Binary wire protocol for FTDB.
//!
//! Message format:
//! ```text
//! [size: u32 LE]     — total message size (header + body)
//! [header: 32 bytes] — request/response metadata
//! [body: N bytes]    — array of fixed-size records
//! ```
//!
//! Header layout (32 bytes):
//! ```text
//! Offset  Size  Field
//!   0       1   operation
//!   1       1   status (0 in requests, result in responses)
//!   2       2   reserved
//!   4       4   request_id
//!   8       4   batch_count (number of records)
//!  12       4   checksum (CRC32C of body)
//!  16      16   reserved
//! ```
//!
//! Operations use TigerBeetle-compatible codes:
//! - 128 (0x80): create_accounts
//! - 129 (0x81): create_transfers
//! - 130 (0x82): lookup_accounts
//! - 131 (0x83): lookup_transfers

/// Wire protocol operation codes (TigerBeetle-compatible).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Operation {
    CreateAccounts = 128,
    CreateTransfers = 129,
    LookupAccounts = 130,
    LookupTransfers = 131,
}

impl Operation {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            128 => Some(Self::CreateAccounts),
            129 => Some(Self::CreateTransfers),
            130 => Some(Self::LookupAccounts),
            131 => Some(Self::LookupTransfers),
            _ => None,
        }
    }
}

/// Message header (32 bytes).
#[derive(Debug, Clone, Copy)]
pub struct Header {
    pub operation: u8,
    pub status: u8,
    pub reserved1: u16,
    pub request_id: u32,
    pub batch_count: u32,
    pub checksum: u32,
    pub reserved2: [u8; 16],
}

impl Header {
    pub const SIZE: usize = 32;

    pub fn new(operation: Operation, request_id: u32, batch_count: u32) -> Self {
        Self {
            operation: operation as u8,
            status: 0,
            reserved1: 0,
            request_id,
            batch_count,
            checksum: 0,
            reserved2: [0; 16],
        }
    }

    pub fn response(operation: Operation, request_id: u32, batch_count: u32, status: u8) -> Self {
        Self {
            operation: operation as u8,
            status,
            reserved1: 0,
            request_id,
            batch_count,
            checksum: 0,
            reserved2: [0; 16],
        }
    }

    pub fn to_bytes(&self) -> [u8; 32] {
        let mut buf = [0u8; 32];
        buf[0] = self.operation;
        buf[1] = self.status;
        buf[2..4].copy_from_slice(&self.reserved1.to_le_bytes());
        buf[4..8].copy_from_slice(&self.request_id.to_le_bytes());
        buf[8..12].copy_from_slice(&self.batch_count.to_le_bytes());
        buf[12..16].copy_from_slice(&self.checksum.to_le_bytes());
        buf[16..32].copy_from_slice(&self.reserved2);
        buf
    }

    pub fn from_bytes(buf: &[u8; 32]) -> Self {
        Self {
            operation: buf[0],
            status: buf[1],
            reserved1: u16::from_le_bytes([buf[2], buf[3]]),
            request_id: u32::from_le_bytes(buf[4..8].try_into().unwrap()),
            batch_count: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            checksum: u32::from_le_bytes(buf[12..16].try_into().unwrap()),
            reserved2: buf[16..32].try_into().unwrap(),
        }
    }
}

/// Maximum batch size (TigerBeetle uses 8190).
pub const MAX_BATCH_SIZE: u32 = 8190;

/// Computes CRC32C checksum of a body.
pub fn checksum(body: &[u8]) -> u32 {
    crc32fast::hash(body)
}
