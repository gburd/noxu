//! Storage layer: thin wrapper around Noxu DB for accounts and transfers.

use crate::account::Account;
use crate::error::FtdbError;
use crate::transfer::Transfer;
use noxu::{
    Database, DatabaseConfig, Environment, EnvironmentConfig, Transaction,
};
use std::path::Path;

/// Persistent storage backed by two Noxu DB databases (accounts + transfers).
pub struct Storage {
    env: Environment,
    accounts_db: Database,
    transfers_db: Database,
}

impl Storage {
    /// Opens (or creates) the storage at the given directory path.
    pub fn open(path: &Path) -> Result<Self, FtdbError> {
        let env_config = EnvironmentConfig::new(path.to_path_buf())
            .with_allow_create(true)
            .with_transactional(true);

        let env = Environment::open(env_config)?;

        let db_config = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);

        let accounts_db = env.open_database(None, "accounts", &db_config)?;
        let transfers_db = env.open_database(None, "transfers", &db_config)?;

        Ok(Self { env, accounts_db, transfers_db })
    }

    /// Begins a new transaction.
    pub fn begin_transaction(&self) -> Result<Transaction, FtdbError> {
        Ok(self.env.begin_transaction(None)?)
    }

    // ── Account operations ──────────────────────────────────────────────────

    /// Retrieves an account by ID (no explicit transaction).
    pub fn get_account(&self, id: u128) -> Result<Option<Account>, FtdbError> {
        match self.accounts_db.get(id.to_le_bytes())? {
            Some(bytes) if bytes.len() == 128 => {
                Ok(Some(Account::from_bytes(bytes[..].try_into().unwrap())))
            }
            _ => Ok(None),
        }
    }

    /// Retrieves an account by ID within a transaction.
    pub fn get_account_txn(
        &self,
        txn: &Transaction,
        id: u128,
    ) -> Result<Option<Account>, FtdbError> {
        match self.accounts_db.get_in(txn, id.to_le_bytes())? {
            Some(bytes) if bytes.len() == 128 => {
                Ok(Some(Account::from_bytes(bytes[..].try_into().unwrap())))
            }
            _ => Ok(None),
        }
    }

    /// Stores an account (no explicit transaction).
    pub fn put_account(&self, account: &Account) -> Result<(), FtdbError> {
        self.accounts_db.put(account.id.to_le_bytes(), account.to_bytes())?;
        Ok(())
    }

    /// Stores an account within a transaction.
    pub fn put_account_txn(
        &self,
        txn: &Transaction,
        account: &Account,
    ) -> Result<(), FtdbError> {
        self.accounts_db.put_in(
            txn,
            account.id.to_le_bytes(),
            account.to_bytes(),
        )?;
        Ok(())
    }

    // ── Transfer operations ─────────────────────────────────────────────────

    /// Retrieves a transfer by ID (no explicit transaction).
    pub fn get_transfer(
        &self,
        id: u128,
    ) -> Result<Option<Transfer>, FtdbError> {
        match self.transfers_db.get(id.to_le_bytes())? {
            Some(bytes) if bytes.len() == 128 => {
                Ok(Some(Transfer::from_bytes(bytes[..].try_into().unwrap())))
            }
            _ => Ok(None),
        }
    }

    /// Retrieves a transfer by ID within a transaction.
    pub fn get_transfer_txn(
        &self,
        txn: &Transaction,
        id: u128,
    ) -> Result<Option<Transfer>, FtdbError> {
        match self.transfers_db.get_in(txn, id.to_le_bytes())? {
            Some(bytes) if bytes.len() == 128 => {
                Ok(Some(Transfer::from_bytes(bytes[..].try_into().unwrap())))
            }
            _ => Ok(None),
        }
    }

    /// Stores a transfer within a transaction.
    pub fn put_transfer_txn(
        &self,
        txn: &Transaction,
        transfer: &Transfer,
    ) -> Result<(), FtdbError> {
        self.transfers_db.put_in(
            txn,
            transfer.id.to_le_bytes(),
            transfer.to_bytes(),
        )?;
        Ok(())
    }
}
