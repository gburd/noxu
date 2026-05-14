//! XA Environment — wraps a Noxu Environment to provide XA resource management.

use std::sync::Mutex;

use hashbrown::HashMap;
use noxu_db::{Environment, Transaction, TransactionConfig};

use crate::error::{PrepareResult, XaError, XaResult};
use crate::flags::XaFlags;
use crate::prepared_log::PreparedLog;
use crate::resource::XaResource;
use crate::xid::Xid;

/// State of an XA transaction branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BranchState {
    /// xa_start called; work is being performed.
    Active,
    /// xa_end called with TMSUSPEND.
    Suspended,
    /// xa_end called with TMSUCCESS; ready for prepare/one-phase commit.
    Idle,
    /// xa_end called with TMFAIL; must be rolled back.
    RollbackOnly,
    /// xa_prepare succeeded; waiting for commit or rollback.
    Prepared,
}

/// Internal branch tracking.
struct Branch {
    state: BranchState,
    txn: Transaction,
    has_writes: bool,
}

/// XA-enabled wrapper around a Noxu Environment.
///
/// Manages the lifecycle of distributed transaction branches, implementing
/// the full X/Open XA two-phase commit protocol.
///
/// If a `PreparedLog` is configured (via `with_prepared_log`), prepared
/// branches are persisted to disk for crash recovery.
pub struct XaEnvironment {
    env: Environment,
    branches: Mutex<HashMap<Xid, Branch>>,
    prepared_log: Option<PreparedLog>,
}

impl XaEnvironment {
    /// Creates a new XaEnvironment wrapping the given environment.
    pub fn new(env: Environment) -> Self {
        Self {
            env,
            branches: Mutex::new(HashMap::new()),
            prepared_log: None,
        }
    }

    /// Returns a reference to the underlying Environment.
    pub fn inner(&self) -> &Environment {
        &self.env
    }

    /// Returns the transaction for an active branch (for use by application code).
    ///
    /// The transaction is only accessible while the branch is Active.
    pub fn get_transaction(&self, xid: &Xid) -> XaResult<&Transaction> {
        // Safety: we return a reference tied to &self, and the branch map
        // holds the Transaction for the lifetime of the XaEnvironment.
        // This is sound because branches are only removed after commit/rollback.
        let branches = self.branches.lock().unwrap();
        let branch = branches.get(xid).ok_or(XaError::NotFound)?;
        if branch.state != BranchState::Active {
            return Err(XaError::Protocol(
                "transaction not active".to_string(),
            ));
        }
        // SAFETY: The Transaction lives in the HashMap which lives as long as
        // `self`. We return a reference bounded by `&self`.
        let txn_ptr: *const Transaction = &branch.txn;
        Ok(unsafe { &*txn_ptr })
    }

    /// Mark the branch as having performed writes.
    pub fn mark_write(&self, xid: &Xid) -> XaResult<()> {
        let mut branches = self.branches.lock().unwrap();
        let branch = branches.get_mut(xid).ok_or(XaError::NotFound)?;
        branch.has_writes = true;
        Ok(())
    }

    /// Enable persistent prepared-transaction logging for crash recovery.
    ///
    /// When enabled, `xa_prepare` writes the Xid to a persistent database,
    /// and `xa_commit`/`xa_rollback`/`xa_forget` remove it. After a crash,
    /// `xa_recover` returns XIDs from both the in-memory map and the
    /// persistent log.
    pub fn with_prepared_log(mut self) -> Result<Self, noxu_db::NoxuError> {
        let log = PreparedLog::open(&self.env)?;
        self.prepared_log = Some(log);
        Ok(self)
    }
}

impl XaResource for XaEnvironment {
    fn xa_start(&self, xid: &Xid, flags: XaFlags) -> XaResult<()> {
        let mut branches = self.branches.lock().unwrap();

        if flags.contains(XaFlags::RESUME) {
            // Resume a suspended branch.
            let branch = branches.get_mut(xid).ok_or(XaError::NotFound)?;
            if branch.state != BranchState::Suspended {
                return Err(XaError::Protocol(
                    "cannot resume: branch not suspended".to_string(),
                ));
            }
            branch.state = BranchState::Active;
            return Ok(());
        }

        if flags.contains(XaFlags::JOIN) {
            // Join an existing branch — just verify it exists and is active.
            let branch = branches.get(xid).ok_or(XaError::NotFound)?;
            if branch.state != BranchState::Active {
                return Err(XaError::Protocol(
                    "cannot join: branch not active".to_string(),
                ));
            }
            return Ok(());
        }

        // New branch.
        if branches.contains_key(xid) {
            return Err(XaError::DuplicateXid);
        }

        let config = TransactionConfig::new();
        let txn = self.env.begin_transaction(None, Some(&config))
            .map_err(XaError::Db)?;

        branches.insert(xid.clone(), Branch {
            state: BranchState::Active,
            txn,
            has_writes: false,
        });

        log::debug!("xa_start: {xid:?}");
        Ok(())
    }

    fn xa_end(&self, xid: &Xid, flags: XaFlags) -> XaResult<()> {
        let mut branches = self.branches.lock().unwrap();
        let branch = branches.get_mut(xid).ok_or(XaError::NotFound)?;

        if branch.state != BranchState::Active {
            return Err(XaError::Protocol(
                "xa_end: branch not active".to_string(),
            ));
        }

        if flags.contains(XaFlags::TMSUSPEND) {
            branch.state = BranchState::Suspended;
        } else if flags.contains(XaFlags::TMFAIL) {
            branch.state = BranchState::RollbackOnly;
        } else {
            // TMSUCCESS or NOFLAGS
            branch.state = BranchState::Idle;
        }

        log::debug!("xa_end: {xid:?} -> {:?}", branch.state);
        Ok(())
    }

    fn xa_prepare(&self, xid: &Xid, flags: XaFlags) -> XaResult<PrepareResult> {
        let _ = flags;
        let mut branches = self.branches.lock().unwrap();
        let branch = branches.get_mut(xid).ok_or(XaError::NotFound)?;

        if branch.state != BranchState::Idle {
            return Err(XaError::Protocol(format!(
                "xa_prepare: expected Idle state, got {:?}",
                branch.state
            )));
        }

        if !branch.has_writes {
            // Read-only optimization: no need for second phase.
            // Abort the internal transaction (releases locks) and remove branch.
            let _ = branch.txn.abort();
            branches.remove(xid);
            log::debug!("xa_prepare: {xid:?} -> ReadOnly");
            return Ok(PrepareResult::ReadOnly);
        }

        // Persist prepared record for crash recovery.
        if let Some(ref log) = self.prepared_log {
            log.record_prepare(xid).map_err(XaError::Db)?;
        }
        branch.state = BranchState::Prepared;
        log::debug!("xa_prepare: {xid:?} -> Prepared");
        Ok(PrepareResult::Ok)
    }

    fn xa_commit(&self, xid: &Xid, flags: XaFlags) -> XaResult<()> {
        let mut branches = self.branches.lock().unwrap();
        let branch = branches.get_mut(xid).ok_or(XaError::NotFound)?;

        if flags.contains(XaFlags::ONEPHASE) {
            // One-phase commit: skip prepare.
            if branch.state != BranchState::Idle {
                return Err(XaError::Protocol(format!(
                    "xa_commit(ONEPHASE): expected Idle, got {:?}",
                    branch.state
                )));
            }
        } else if branch.state != BranchState::Prepared {
            return Err(XaError::Protocol(format!(
                "xa_commit: expected Prepared, got {:?}",
                branch.state
            )));
        }

        // Remove branch and commit the underlying transaction.
        let branch = branches.remove(xid).unwrap();
        branch.txn.commit().map_err(XaError::Db)?;
        if let Some(ref log) = self.prepared_log {
            let _ = log.remove(xid);
        }
        log::debug!("xa_commit: {xid:?}");
        Ok(())
    }

    fn xa_rollback(&self, xid: &Xid, flags: XaFlags) -> XaResult<()> {
        let _ = flags;
        let mut branches = self.branches.lock().unwrap();
        let branch = branches.get(xid).ok_or(XaError::NotFound)?;

        match branch.state {
            BranchState::Idle | BranchState::Prepared | BranchState::RollbackOnly => {}
            _ => {
                return Err(XaError::Protocol(format!(
                    "xa_rollback: unexpected state {:?}",
                    branch.state
                )));
            }
        }

        let branch = branches.remove(xid).unwrap();
        branch.txn.abort().map_err(XaError::Db)?;
        if let Some(ref log) = self.prepared_log {
            let _ = log.remove(xid);
        }
        log::debug!("xa_rollback: {xid:?}");
        Ok(())
    }

    fn xa_recover(&self, _flags: XaFlags) -> XaResult<Vec<Xid>> {
        // In-memory prepared branches
        let branches = self.branches.lock().unwrap();
        let mut prepared: Vec<Xid> = branches
            .iter()
            .filter(|(_, b)| b.state == BranchState::Prepared)
            .map(|(xid, _)| xid.clone())
            .collect();

        // Add any from persistent log (crash recovery — not in memory)
        if let Some(ref log) = self.prepared_log {
            if let Ok(persisted) = log.recover_all() {
                for xid in persisted {
                    if !prepared.contains(&xid) {
                        prepared.push(xid);
                    }
                }
            }
        }
        Ok(prepared)
    }

    fn xa_forget(&self, xid: &Xid, _flags: XaFlags) -> XaResult<()> {
        let mut branches = self.branches.lock().unwrap();
        if branches.remove(xid).is_none() {
            // Check persistent log (may be from crash recovery)
            if let Some(ref log) = self.prepared_log {
                let recovered = log.recover_all().unwrap_or_default();
                if !recovered.contains(xid) {
                    return Err(XaError::NotFound);
                }
            } else {
                return Err(XaError::NotFound);
            }
        }
        if let Some(ref log) = self.prepared_log {
            let _ = log.remove(xid);
        }
        log::debug!("xa_forget: {xid:?}");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use noxu_db::{DatabaseConfig, DatabaseEntry, EnvironmentConfig};
    use tempfile::TempDir;

    fn make_xa_env() -> (XaEnvironment, TempDir) {
        let dir = TempDir::new().unwrap();
        let env_config = EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true);
        let env = Environment::open(env_config).unwrap();
        (XaEnvironment::new(env), dir)
    }

    #[test]
    fn test_full_2pc() {
        let (xa, _dir) = make_xa_env();
        let db_config = DatabaseConfig::new().with_allow_create(true);
        let db = xa.inner().open_database(None, "test", &db_config).unwrap();

        let xid = Xid::new(1, b"gtrid1", b"bqual1").unwrap();

        // Phase 1: start + work + end
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid).unwrap();
            let key = DatabaseEntry::from_bytes(b"k1");
            let val = DatabaseEntry::from_bytes(b"v1");
            db.put(Some(txn), &key, &val).unwrap();
        }
        xa.mark_write(&xid).unwrap();
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();

        // Phase 2: prepare + commit
        let prep = xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
        assert_eq!(prep, PrepareResult::Ok);
        xa.xa_commit(&xid, XaFlags::NOFLAGS).unwrap();

        // Verify data committed
        let key = DatabaseEntry::from_bytes(b"k1");
        let mut val = DatabaseEntry::new();
        let status = db.get(None, &key, &mut val).unwrap();
        assert_eq!(status, noxu_db::OperationStatus::Success);
        assert_eq!(val.get_data(), Some(b"v1".as_slice()));
    }

    #[test]
    fn test_rollback() {
        let (xa, _dir) = make_xa_env();
        let db_config = DatabaseConfig::new().with_allow_create(true);
        let db = xa.inner().open_database(None, "test", &db_config).unwrap();

        let xid = Xid::new(1, b"gtrid2", b"bqual2").unwrap();

        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid).unwrap();
            let key = DatabaseEntry::from_bytes(b"k2");
            let val = DatabaseEntry::from_bytes(b"v2");
            db.put(Some(txn), &key, &val).unwrap();
        }
        xa.mark_write(&xid).unwrap();
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
        xa.xa_rollback(&xid, XaFlags::NOFLAGS).unwrap();

        // Verify data NOT committed
        let key = DatabaseEntry::from_bytes(b"k2");
        let mut val = DatabaseEntry::new();
        let status = db.get(None, &key, &mut val).unwrap();
        assert_eq!(status, noxu_db::OperationStatus::NotFound);
    }

    #[test]
    fn test_read_only_optimization() {
        let (xa, _dir) = make_xa_env();

        let xid = Xid::new(1, b"readonly", b"branch").unwrap();
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        // No writes performed
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();

        let prep = xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
        assert_eq!(prep, PrepareResult::ReadOnly);
        // No commit needed — branch already cleaned up
    }

    #[test]
    fn test_duplicate_xid_rejected() {
        let (xa, _dir) = make_xa_env();
        let xid = Xid::new(1, b"dup", b"dup").unwrap();

        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        let result = xa.xa_start(&xid, XaFlags::NOFLAGS);
        assert!(matches!(result, Err(XaError::DuplicateXid)));

        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_rollback(&xid, XaFlags::NOFLAGS).unwrap();
    }

    #[test]
    fn test_protocol_error_prepare_before_end() {
        let (xa, _dir) = make_xa_env();
        let xid = Xid::new(1, b"proto", b"err").unwrap();

        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        // Try to prepare while still Active (not yet ended)
        let result = xa.xa_prepare(&xid, XaFlags::NOFLAGS);
        assert!(matches!(result, Err(XaError::Protocol(_))));

        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_rollback(&xid, XaFlags::NOFLAGS).unwrap();
    }

    #[test]
    fn test_one_phase_commit() {
        let (xa, _dir) = make_xa_env();
        let db_config = DatabaseConfig::new().with_allow_create(true);
        let db = xa.inner().open_database(None, "test", &db_config).unwrap();

        let xid = Xid::new(1, b"onephase", b"branch").unwrap();
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid).unwrap();
            let key = DatabaseEntry::from_bytes(b"k3");
            let val = DatabaseEntry::from_bytes(b"v3");
            db.put(Some(txn), &key, &val).unwrap();
        }
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();

        // One-phase commit (skip prepare)
        xa.xa_commit(&xid, XaFlags::ONEPHASE).unwrap();

        let key = DatabaseEntry::from_bytes(b"k3");
        let mut val = DatabaseEntry::new();
        let status = db.get(None, &key, &mut val).unwrap();
        assert_eq!(status, noxu_db::OperationStatus::Success);
    }

    #[test]
    fn test_suspend_resume() {
        let (xa, _dir) = make_xa_env();
        let xid = Xid::new(1, b"suspend", b"resume").unwrap();

        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        xa.xa_end(&xid, XaFlags::TMSUSPEND).unwrap();

        // Resume
        xa.xa_start(&xid, XaFlags::RESUME).unwrap();
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_rollback(&xid, XaFlags::NOFLAGS).unwrap();
    }

    #[test]
    fn test_recover_returns_prepared() {
        let (xa, _dir) = make_xa_env();
        let db_config = DatabaseConfig::new().with_allow_create(true);
        let db = xa.inner().open_database(None, "test", &db_config).unwrap();

        let xid = Xid::new(1, b"recover", b"test").unwrap();
        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        {
            let txn = xa.get_transaction(&xid).unwrap();
            db.put(Some(txn), &DatabaseEntry::from_bytes(b"rk"), &DatabaseEntry::from_bytes(b"rv")).unwrap();
        }
        xa.mark_write(&xid).unwrap();
        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();

        // Recover should show this xid
        let recovered = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0], xid);

        // Clean up
        xa.xa_commit(&xid, XaFlags::NOFLAGS).unwrap();
    }
}
