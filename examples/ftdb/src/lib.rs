//! FTDB: A TigerBeetle-compatible financial transactions database.
//!
//! Implements double-entry bookkeeping with ACID guarantees using TigerBeetle's
//! exact data model (128-byte Account/Transfer structs) and a binary wire protocol
//! with TigerBeetle-compatible operation codes.
//!
//! Backed by Noxu DB for persistent storage with full transaction support.

pub mod account;
pub mod engine;
pub mod error;
pub mod protocol;
pub mod server;
pub mod storage;
pub mod transfer;

pub use account::{Account, AccountFlags};
pub use engine::Engine;
pub use error::FtdbError;
pub use protocol::{Header, Operation};
pub use server::Server;
pub use storage::Storage;
pub use transfer::{Transfer, TransferFlags};
