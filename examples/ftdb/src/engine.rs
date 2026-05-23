//! Transaction engine: validates and executes financial operations atomically.
//!
//! Returns TigerBeetle-compatible result codes for batch processing.

use crate::account::Account;
use crate::error::{
    BatchResult, CreateAccountResult, CreateTransferResult, FtdbError,
};
use crate::storage::Storage;
use crate::transfer::Transfer;

/// The financial transaction engine.
pub struct Engine {
    storage: Storage,
}

impl Engine {
    pub fn new(storage: Storage) -> Self {
        Self { storage }
    }

    /// Creates accounts in a batch. Returns results only for failed entries.
    pub fn create_accounts(
        &self,
        accounts: &[Account],
    ) -> Result<Vec<BatchResult>, FtdbError> {
        let mut results = Vec::new();

        for (i, account) in accounts.iter().enumerate() {
            let result = self.create_one_account(account);
            if result != CreateAccountResult::Ok {
                results.push(BatchResult {
                    index: i as u32,
                    result: result as u32,
                });
            }
        }

        Ok(results)
    }

    /// Creates transfers in a batch. Returns results only for failed entries.
    pub fn create_transfers(
        &self,
        transfers: &[Transfer],
    ) -> Result<Vec<BatchResult>, FtdbError> {
        let mut results = Vec::new();

        for (i, transfer) in transfers.iter().enumerate() {
            let result = self.create_one_transfer(transfer);
            match result {
                Ok(code) => {
                    if code != CreateTransferResult::Ok {
                        results.push(BatchResult {
                            index: i as u32,
                            result: code as u32,
                        });
                    }
                }
                Err(_) => {
                    results.push(BatchResult {
                        index: i as u32,
                        result: CreateTransferResult::DebitAccountNotFound
                            as u32,
                    });
                }
            }
        }

        Ok(results)
    }

    /// Looks up accounts by ID. Returns found accounts (missing IDs are omitted).
    pub fn lookup_accounts(
        &self,
        ids: &[u128],
    ) -> Result<Vec<Account>, FtdbError> {
        let mut results = Vec::new();
        for &id in ids {
            if let Some(acct) = self.storage.get_account(id)? {
                results.push(acct);
            }
        }
        Ok(results)
    }

    /// Looks up transfers by ID. Returns found transfers (missing IDs are omitted).
    pub fn lookup_transfers(
        &self,
        ids: &[u128],
    ) -> Result<Vec<Transfer>, FtdbError> {
        let mut results = Vec::new();
        for &id in ids {
            if let Some(t) = self.storage.get_transfer(id)? {
                results.push(t);
            }
        }
        Ok(results)
    }

    // ── Single-record operations ────────────────────────────────────────────

    fn create_one_account(&self, account: &Account) -> CreateAccountResult {
        // Validate
        if account.id == 0 {
            return CreateAccountResult::IdMustNotBeZero;
        }
        if account.id == u128::MAX {
            return CreateAccountResult::IdMustNotBeMax;
        }
        if account.ledger == 0 {
            return CreateAccountResult::LedgerMustNotBeZero;
        }
        if account.code == 0 {
            return CreateAccountResult::CodeMustNotBeZero;
        }

        // Check for mutually exclusive flags
        let flags = account.flags;
        if flags.debits_must_not_exceed_credits()
            && flags.credits_must_not_exceed_debits()
        {
            return CreateAccountResult::FlagsAreMutuallyExclusive;
        }

        // Check existence
        match self.storage.get_account(account.id) {
            Ok(Some(existing)) => {
                if existing.flags != account.flags {
                    return CreateAccountResult::ExistsWithDifferentFlags;
                }
                if existing.ledger != account.ledger {
                    return CreateAccountResult::ExistsWithDifferentLedger;
                }
                if existing.code != account.code {
                    return CreateAccountResult::ExistsWithDifferentCode;
                }
                return CreateAccountResult::Exists;
            }
            Ok(None) => {}
            Err(_) => return CreateAccountResult::Exists,
        }

        // Persist with server-assigned timestamp
        let mut to_store = *account;
        to_store.timestamp = now_nanos();

        match self.storage.put_account(&to_store) {
            Ok(()) => CreateAccountResult::Ok,
            Err(_) => CreateAccountResult::Exists,
        }
    }

    fn create_one_transfer(
        &self,
        transfer: &Transfer,
    ) -> Result<CreateTransferResult, FtdbError> {
        // Validate
        if transfer.id == 0 {
            return Ok(CreateTransferResult::IdMustNotBeZero);
        }
        if transfer.id == u128::MAX {
            return Ok(CreateTransferResult::IdMustNotBeMax);
        }

        // Dispatch based on flags
        if transfer.is_post_request() {
            return self.execute_post_pending(transfer);
        }
        if transfer.is_void_request() {
            return self.execute_void_pending(transfer);
        }

        // Normal or pending transfer
        if transfer.debit_account_id == 0 {
            return Ok(CreateTransferResult::DebitAccountIdMustNotBeZero);
        }
        if transfer.credit_account_id == 0 {
            return Ok(CreateTransferResult::CreditAccountIdMustNotBeZero);
        }
        if transfer.debit_account_id == transfer.credit_account_id {
            return Ok(CreateTransferResult::AccountsMustBeDifferent);
        }
        if transfer.amount == 0 {
            return Ok(CreateTransferResult::AmountMustNotBeZero);
        }
        if !transfer.is_pending() && transfer.pending_id != 0 {
            return Ok(CreateTransferResult::PendingIdMustBeZero);
        }

        // Check duplicate
        if self.storage.get_transfer(transfer.id)?.is_some() {
            return Ok(CreateTransferResult::Exists);
        }

        let txn = self.storage.begin_transaction()?;

        let mut debit_acct = match self
            .storage
            .get_account_txn(&txn, transfer.debit_account_id)?
        {
            Some(a) => a,
            None => {
                txn.abort()?;
                return Ok(CreateTransferResult::DebitAccountNotFound);
            }
        };

        let mut credit_acct = match self
            .storage
            .get_account_txn(&txn, transfer.credit_account_id)?
        {
            Some(a) => a,
            None => {
                txn.abort()?;
                return Ok(CreateTransferResult::CreditAccountNotFound);
            }
        };

        // Balance constraints
        if !debit_acct.can_debit(transfer.amount) {
            txn.abort()?;
            return Ok(CreateTransferResult::ExceedsCredits);
        }
        if !credit_acct.can_credit(transfer.amount) {
            txn.abort()?;
            return Ok(CreateTransferResult::ExceedsDebits);
        }

        // Apply
        if transfer.is_pending() {
            debit_acct.apply_pending_debit(transfer.amount);
            credit_acct.apply_pending_credit(transfer.amount);
        } else {
            debit_acct.debits_posted =
                debit_acct.debits_posted.saturating_add(transfer.amount);
            credit_acct.credits_posted =
                credit_acct.credits_posted.saturating_add(transfer.amount);
        }

        // Persist with server-assigned timestamp
        let mut to_store = *transfer;
        to_store.timestamp = now_nanos();

        self.storage.put_account_txn(&txn, &debit_acct)?;
        self.storage.put_account_txn(&txn, &credit_acct)?;
        self.storage.put_transfer_txn(&txn, &to_store)?;

        txn.commit()?;
        Ok(CreateTransferResult::Ok)
    }

    fn execute_post_pending(
        &self,
        post: &Transfer,
    ) -> Result<CreateTransferResult, FtdbError> {
        if post.pending_id == 0 {
            return Ok(CreateTransferResult::PendingIdMustNotBeZero);
        }

        // Check duplicate
        if self.storage.get_transfer(post.id)?.is_some() {
            return Ok(CreateTransferResult::Exists);
        }

        let txn = self.storage.begin_transaction()?;

        let pending =
            match self.storage.get_transfer_txn(&txn, post.pending_id)? {
                Some(t) => t,
                None => {
                    txn.abort()?;
                    return Ok(CreateTransferResult::PendingTransferNotFound);
                }
            };

        if !pending.is_pending() {
            txn.abort()?;
            return Ok(CreateTransferResult::PendingTransferNotPending);
        }

        let mut debit_acct = match self
            .storage
            .get_account_txn(&txn, pending.debit_account_id)?
        {
            Some(a) => a,
            None => {
                txn.abort()?;
                return Ok(CreateTransferResult::DebitAccountNotFound);
            }
        };

        let mut credit_acct = match self
            .storage
            .get_account_txn(&txn, pending.credit_account_id)?
        {
            Some(a) => a,
            None => {
                txn.abort()?;
                return Ok(CreateTransferResult::CreditAccountNotFound);
            }
        };

        // Post: move from pending to posted
        let amount = if post.amount != 0 {
            post.amount.min(pending.amount)
        } else {
            pending.amount
        };
        debit_acct.post_pending_debit(amount);
        credit_acct.post_pending_credit(amount);

        let mut to_store = *post;
        to_store.timestamp = now_nanos();

        self.storage.put_account_txn(&txn, &debit_acct)?;
        self.storage.put_account_txn(&txn, &credit_acct)?;
        self.storage.put_transfer_txn(&txn, &to_store)?;

        txn.commit()?;
        Ok(CreateTransferResult::Ok)
    }

    fn execute_void_pending(
        &self,
        void: &Transfer,
    ) -> Result<CreateTransferResult, FtdbError> {
        if void.pending_id == 0 {
            return Ok(CreateTransferResult::PendingIdMustNotBeZero);
        }

        // Check duplicate
        if self.storage.get_transfer(void.id)?.is_some() {
            return Ok(CreateTransferResult::Exists);
        }

        let txn = self.storage.begin_transaction()?;

        let pending =
            match self.storage.get_transfer_txn(&txn, void.pending_id)? {
                Some(t) => t,
                None => {
                    txn.abort()?;
                    return Ok(CreateTransferResult::PendingTransferNotFound);
                }
            };

        if !pending.is_pending() {
            txn.abort()?;
            return Ok(CreateTransferResult::PendingTransferNotPending);
        }

        let mut debit_acct = match self
            .storage
            .get_account_txn(&txn, pending.debit_account_id)?
        {
            Some(a) => a,
            None => {
                txn.abort()?;
                return Ok(CreateTransferResult::DebitAccountNotFound);
            }
        };

        let mut credit_acct = match self
            .storage
            .get_account_txn(&txn, pending.credit_account_id)?
        {
            Some(a) => a,
            None => {
                txn.abort()?;
                return Ok(CreateTransferResult::CreditAccountNotFound);
            }
        };

        // Void: remove from pending
        debit_acct.void_pending_debit(pending.amount);
        credit_acct.void_pending_credit(pending.amount);

        let mut to_store = *void;
        to_store.timestamp = now_nanos();

        self.storage.put_account_txn(&txn, &debit_acct)?;
        self.storage.put_account_txn(&txn, &credit_acct)?;
        self.storage.put_transfer_txn(&txn, &to_store)?;

        txn.commit()?;
        Ok(CreateTransferResult::Ok)
    }
}

fn now_nanos() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account::AccountFlags;
    use tempfile::TempDir;

    fn setup() -> (Engine, TempDir) {
        let dir = TempDir::new().unwrap();
        let storage = Storage::open(dir.path()).unwrap();
        (Engine::new(storage), dir)
    }

    #[test]
    fn test_create_and_lookup_account() {
        let (engine, _dir) = setup();
        let acct = Account { code: 1, ..Account::new(1, 100) };
        let results = engine.create_accounts(&[acct]).unwrap();
        assert!(results.is_empty());

        let found = engine.lookup_accounts(&[1]).unwrap();
        assert_eq!(found.len(), 1);
        assert_eq!(found[0].id, 1);
        assert_eq!(found[0].ledger, 100);
    }

    #[test]
    fn test_duplicate_account() {
        let (engine, _dir) = setup();
        let acct = Account { code: 1, ..Account::new(1, 100) };
        engine.create_accounts(&[acct]).unwrap();
        let results = engine.create_accounts(&[acct]).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].result, CreateAccountResult::Exists as u32);
    }

    #[test]
    fn test_immediate_transfer() {
        let (engine, _dir) = setup();

        let mut sender = Account { code: 1, ..Account::new(1, 100) };
        sender.credits_posted = 1_000_000;
        let receiver = Account { code: 1, ..Account::new(2, 100) };
        engine.create_accounts(&[sender, receiver]).unwrap();

        let transfer = Transfer::new(100, 1, 2, 500_000);
        let results = engine.create_transfers(&[transfer]).unwrap();
        assert!(results.is_empty());

        let accounts = engine.lookup_accounts(&[1, 2]).unwrap();
        assert_eq!(accounts[0].debits_posted, 500_000);
        assert_eq!(accounts[1].credits_posted, 500_000);
    }

    #[test]
    fn test_insufficient_funds() {
        let (engine, _dir) = setup();

        let mut sender = Account { code: 1, ..Account::new(1, 100) };
        sender.flags =
            AccountFlags(AccountFlags::DEBITS_MUST_NOT_EXCEED_CREDITS);
        sender.credits_posted = 100;
        let receiver = Account { code: 1, ..Account::new(2, 100) };
        engine.create_accounts(&[sender, receiver]).unwrap();

        let transfer = Transfer::new(100, 1, 2, 200);
        let results = engine.create_transfers(&[transfer]).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(
            results[0].result,
            CreateTransferResult::ExceedsCredits as u32
        );
    }

    #[test]
    fn test_pending_post_lifecycle() {
        let (engine, _dir) = setup();

        let mut sender = Account { code: 1, ..Account::new(1, 100) };
        sender.credits_posted = 1_000_000;
        let receiver = Account { code: 1, ..Account::new(2, 100) };
        engine.create_accounts(&[sender, receiver]).unwrap();

        // Create pending
        let pending = Transfer::new_pending(10, 1, 2, 300_000);
        let results = engine.create_transfers(&[pending]).unwrap();
        assert!(results.is_empty());

        let accounts = engine.lookup_accounts(&[1]).unwrap();
        assert_eq!(accounts[0].debits_pending, 300_000);
        assert_eq!(accounts[0].debits_posted, 0);

        // Post it
        let post = Transfer::post_pending(11, 10);
        let results = engine.create_transfers(&[post]).unwrap();
        assert!(results.is_empty());

        let accounts = engine.lookup_accounts(&[1, 2]).unwrap();
        assert_eq!(accounts[0].debits_pending, 0);
        assert_eq!(accounts[0].debits_posted, 300_000);
        assert_eq!(accounts[1].credits_posted, 300_000);
    }

    #[test]
    fn test_void_pending() {
        let (engine, _dir) = setup();

        let mut sender = Account { code: 1, ..Account::new(1, 100) };
        sender.credits_posted = 1_000_000;
        let receiver = Account { code: 1, ..Account::new(2, 100) };
        engine.create_accounts(&[sender, receiver]).unwrap();

        let pending = Transfer::new_pending(20, 1, 2, 400_000);
        engine.create_transfers(&[pending]).unwrap();

        let void = Transfer::void_pending(21, 20);
        let results = engine.create_transfers(&[void]).unwrap();
        assert!(results.is_empty());

        let accounts = engine.lookup_accounts(&[1]).unwrap();
        assert_eq!(accounts[0].debits_pending, 0);
    }

    #[test]
    fn test_batch_create() {
        let (engine, _dir) = setup();

        let accounts: Vec<Account> = (1..=100)
            .map(|i| Account { code: 1, ..Account::new(i, 1) })
            .collect();
        let results = engine.create_accounts(&accounts).unwrap();
        assert!(results.is_empty());

        let ids: Vec<u128> = (1..=100).collect();
        let found = engine.lookup_accounts(&ids).unwrap();
        assert_eq!(found.len(), 100);
    }
}
