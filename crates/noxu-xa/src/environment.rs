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
///
/// `txn` is boxed so its address is stable across `branches` HashMap
/// rehashes. `get_transaction` hands out a `&Transaction` that may be
/// used by application code while another thread inserts a *different*
/// branch into the map; without the heap allocation, that insert could
/// rehash and move the `Branch` (and the `Transaction` it contains),
/// invalidating the outstanding reference.
struct Branch {
    state: BranchState,
    txn: Box<Transaction>,
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
    /// Recovery-scan cursor state for `xa_recover` (audit
    /// persist-xa F5, Wave 2C-4).  X/Open requires `STARTRSCAN` to
    /// rewind the cursor and `ENDRSCAN` to release it; calls
    /// without `STARTRSCAN` resume from the saved cursor.  Stored
    /// here so a paginating TM no longer sees duplicates.
    /// `None` = no scan in progress.
    recover_cursor: Mutex<Option<RecoverScan>>,
}

/// Saved state of a paginating `xa_recover` scan.
#[derive(Debug, Clone)]
struct RecoverScan {
    /// Full snapshot of XIDs at scan start; the cursor walks this
    /// list rather than re-querying the backing maps each call so
    /// pagination is stable across concurrent prepare / forget
    /// calls.
    xids: Vec<Xid>,
    /// Index of the next XID to return.
    next: usize,
}

impl XaEnvironment {
    /// Creates a new XaEnvironment wrapping the given environment.
    pub fn new(env: Environment) -> Self {
        Self {
            env,
            branches: Mutex::new(HashMap::new()),
            prepared_log: None,
            recover_cursor: Mutex::new(None),
        }
    }

    /// Returns a reference to the underlying Environment.
    pub fn inner(&self) -> &Environment {
        &self.env
    }

    /// Returns the transaction for an active branch (for use by application code).
    ///
    /// The transaction is only accessible while the branch is Active.
    /// `Branch::txn` is boxed, so the returned reference remains valid
    /// even if a concurrent `xa_start` rehashes the underlying HashMap;
    /// the caller is responsible for not invalidating the reference by
    /// rolling back / committing this same `xid` from another thread
    /// (the X/Open XA protocol forbids that anyway).
    pub fn get_transaction(&self, xid: &Xid) -> XaResult<&Transaction> {
        let branches = self.branches.lock().unwrap();
        let branch = branches.get(xid).ok_or(XaError::NotFound)?;
        if branch.state != BranchState::Active {
            return Err(XaError::Protocol(
                "transaction not active".to_string(),
            ));
        }
        // SAFETY: The Transaction is heap-allocated via Box, so its
        // address is stable across HashMap rehashes triggered by other
        // xa_start calls. The reference's lifetime is bounded by `&self`.
        // We deliberately drop the `branches` lock guard at the end of
        // this function — callers serialize their own xid through the
        // XA state machine, so no other thread will remove this branch
        // while the caller is using the returned reference.
        let txn_ptr: *const Transaction = &*branch.txn;
        Ok(unsafe { &*txn_ptr })
    }

    /// Mark the branch as having performed writes.
    ///
    /// Historically, this had to be called before `xa_prepare` for any
    /// branch that performed writes; otherwise `xa_prepare` would take the
    /// read-only optimisation and silently abort the inner transaction.
    ///
    /// As of v1.5, `xa_prepare` auto-detects writes by inspecting the inner
    /// `Transaction`'s log-entry chain via `Txn::has_logged_entries()`, so
    /// calling `mark_write` is **no longer required** for correctness when
    /// writes go through `get_transaction(&xid)`'s `Transaction`.
    ///
    /// `mark_write` remains supported as a backwards-compatible no-op
    /// override: callers who plan to perform writes through some side
    /// channel that `Txn::has_logged_entries()` cannot observe (today
    /// there is no such channel in the public API) can still force the
    /// branch into the writes-present prepare path.
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
    ///
    /// # v1.5 limitation
    ///
    /// In v1.5, the persistent log records that an XID was prepared but the
    /// engine does not durably record the underlying `Transaction`'s state
    /// (write locks, undo chain, dirty pages). On a fresh process, XIDs from
    /// the persistent log appear in `xa_recover` but cannot be committed or
    /// rolled back — attempting to do so returns
    /// [`XaError::CrashDurabilityNotSupported`]. Use `xa_forget` to clear
    /// the persistent record. Crash-durable XA is planned for v2.0.
    pub fn with_prepared_log(mut self) -> Result<Self, noxu_db::NoxuError> {
        let log = PreparedLog::open(&self.env)?;
        self.prepared_log = Some(log);
        Ok(self)
    }

    /// Classify the cause of a `branches.get(xid) == None` lookup.
    ///
    /// If the XID also appears in the persistent prepared log, the caller is
    /// trying to operate on a branch that was prepared in a previous process
    /// and lost on restart; surface the v1.5 limitation as a typed error
    /// instead of the generic `NotFound`. Otherwise the XID was simply never
    /// seen — return `NotFound`.
    fn classify_missing_branch(&self, xid: &Xid) -> XaError {
        if let Some(ref log) = self.prepared_log {
            if let Ok(persisted) = log.recover_all() {
                if persisted.contains(xid) {
                    return XaError::CrashDurabilityNotSupported;
                }
            }
        }
        XaError::NotFound
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
        let txn = self
            .env
            .begin_transaction(None, Some(&config))
            .map_err(XaError::Db)?;

        branches.insert(
            xid.clone(),
            Branch {
                state: BranchState::Active,
                txn: Box::new(txn),
                has_writes: false,
            },
        );

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

        // Auto-detect writes performed via the inner Transaction. Resolves
        // the `mark_write` footgun (Sprint 3, audit Finding 3): if the
        // application performs writes through the inner Transaction but
        // forgets to call `mark_write`, we must NOT take the read-only
        // optimisation — doing so silently aborts those writes.
        //
        // `Txn::has_logged_entries()` returns true as soon as any LN log
        // record was emitted under this transaction id, which is exactly
        // the condition that makes the read-only optimisation unsafe.
        let inner_has_writes = branch
            .txn
            .get_inner_txn()
            .map(|t| t.lock().unwrap().has_logged_entries())
            .unwrap_or(false);
        let has_writes = branch.has_writes || inner_has_writes;

        if !has_writes {
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
        let branch = match branches.get_mut(xid) {
            Some(b) => b,
            None => {
                // Not in memory — distinguish "never seen this XID" from
                // "prepared in a previous process and lost on restart".
                drop(branches);
                return Err(self.classify_missing_branch(xid));
            }
        };

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
        let branch = match branches.get(xid) {
            Some(b) => b,
            None => {
                drop(branches);
                return Err(self.classify_missing_branch(xid));
            }
        };

        match branch.state {
            BranchState::Idle
            | BranchState::Prepared
            | BranchState::RollbackOnly => {}
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

    fn xa_recover(&self, flags: XaFlags) -> XaResult<Vec<Xid>> {
        // Audit persist-xa F5 (Wave 2C-4): honour the X/Open
        // STARTRSCAN / ENDRSCAN flags so that paginating TMs no
        // longer see duplicates.
        //
        //   * STARTRSCAN: rewind — build a fresh snapshot of every
        //     prepared XID and reset the cursor to 0.
        //   * ENDRSCAN:   release — drop the saved snapshot.
        //   * Neither flag: resume — return the next page of the
        //     existing snapshot, or build one on demand if no scan
        //     is currently in progress (matches "first call rewinds
        //     implicitly" behaviour expected by simple TMs).
        //
        // We continue to return *all* remaining XIDs in a single
        // call rather than enforcing a TM-supplied page size; XA
        // does not actually require pagination, only that the same
        // XID never appears twice across resumed calls.  Re-paging
        // can be added later by surfacing a `Vec<Xid>` chunk size.
        let start_rscan = flags.contains(XaFlags::STARTRSCAN);
        let end_rscan = flags.contains(XaFlags::ENDRSCAN);

        let mut cursor_guard = self.recover_cursor.lock().unwrap();

        if start_rscan || cursor_guard.is_none() {
            // Build a fresh snapshot.
            let branches = self.branches.lock().unwrap();
            let mut prepared: Vec<Xid> = branches
                .iter()
                .filter(|(_, b)| b.state == BranchState::Prepared)
                .map(|(xid, _)| xid.clone())
                .collect();
            drop(branches);

            // Add any from persistent log.  Storage-level read errors
            // are now surfaced (was: silently dropped — audit XA F7).
            if let Some(ref log) = self.prepared_log {
                let persisted = log.recover_all().map_err(|e| {
                    XaError::RmFail(format!(
                        "xa_recover: prepared-log scan failed: {e}"
                    ))
                })?;
                for xid in persisted {
                    if !prepared.contains(&xid) {
                        prepared.push(xid);
                    }
                }
            }

            *cursor_guard = Some(RecoverScan { xids: prepared, next: 0 });
        }

        let result = if let Some(ref mut scan) = *cursor_guard {
            // Drain everything from `next..` and advance the cursor.
            let out: Vec<Xid> = scan.xids[scan.next..].to_vec();
            scan.next = scan.xids.len();
            out
        } else {
            Vec::new()
        };

        if end_rscan {
            *cursor_guard = None;
        }

        Ok(result)
    }

    fn xa_forget(&self, xid: &Xid, _flags: XaFlags) -> XaResult<()> {
        let mut branches = self.branches.lock().unwrap();
        if branches.remove(xid).is_none() {
            // Check persistent log (may be from crash recovery).
            // Audit XA F7 (Wave 2C-4): surface storage-level read
            // errors as XAER_RMFAIL instead of pretending the log is
            // empty.
            if let Some(ref log) = self.prepared_log {
                let recovered = log.recover_all().map_err(|e| {
                    XaError::RmFail(format!(
                        "xa_forget: prepared-log scan failed: {e}"
                    ))
                })?;
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
            db.put(
                Some(txn),
                &DatabaseEntry::from_bytes(b"rk"),
                &DatabaseEntry::from_bytes(b"rv"),
            )
            .unwrap();
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

    /// Audit persist-xa F5 (Wave 2C-4): STARTRSCAN / ENDRSCAN
    /// pagination. A second `xa_recover` call without STARTRSCAN
    /// (i.e. resume) returns the empty list because the previous
    /// call drained the cursor.
    #[test]
    fn test_recover_pagination_no_duplicates() {
        let (xa, _dir) = make_xa_env();
        let db_config = DatabaseConfig::new().with_allow_create(true);
        let _db = xa.inner().open_database(None, "test", &db_config).unwrap();

        // Prepare two branches.
        let mut xids = Vec::new();
        for i in 0..2u32 {
            let bqual = format!("bq{i}");
            let xid = Xid::new(1, b"gtrid", bqual.as_bytes()).unwrap();
            xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
            xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
            xa.mark_write(&xid).unwrap();
            xa.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
            xids.push(xid);
        }

        // STARTRSCAN drains everything.
        let first = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
        assert_eq!(first.len(), 2);

        // Resume (no flag): no duplicates — empty list.
        let second = xa.xa_recover(XaFlags::NOFLAGS).unwrap();
        assert!(
            second.is_empty(),
            "second xa_recover (resume) must not duplicate XIDs: {second:?}",
        );

        // ENDRSCAN releases the cursor; the next implicit-rewind call
        // sees the snapshot afresh.
        let _ = xa.xa_recover(XaFlags::ENDRSCAN).unwrap();
        let again = xa.xa_recover(XaFlags::NOFLAGS).unwrap();
        assert_eq!(
            again.len(),
            2,
            "after ENDRSCAN the next call rebuilds and returns all XIDs",
        );

        // Clean up
        for xid in &xids {
            xa.xa_rollback(xid, XaFlags::NOFLAGS).unwrap();
        }
    }
}
