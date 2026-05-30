//! XA Environment — wraps a Noxu Environment to provide XA resource management.

use std::sync::Mutex;

use hashbrown::HashMap;
use hashbrown::HashSet;
use crate::db::{Environment, Transaction, TransactionConfig};

use crate::xa::error::{PrepareResult, XaError, XaResult};
use crate::xa::flags::XaFlags;
use crate::xa::prepared_log::PreparedLog;
use crate::xa::resource::XaResource;
use crate::xa::xid::Xid;

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

/// Branch reconstructed from a recovered (post-crash) prepared
/// transaction.
///
/// Used to
/// resolve `xa_commit(xid)` / `xa_rollback(xid)` calls for XIDs that
/// were prepared in a previous process and survived the crash via the
/// `TxnPrepare` WAL frame.
///
/// There is no in-memory `Transaction` for these branches — the original
/// process crashed before commit / rollback, and the recovery layer
/// surfaced only the (xid, txn_id, first_lsn, last_lsn) tuple plus the
/// list of LNs to replay on commit.
#[derive(Debug, Clone)]
struct RecoveredBranch {
    /// Transaction id from the original process.
    txn_id: u64,
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
    /// Branches restored from the WAL `TxnPrepare` frames
    /// during the most recent `Environment::open()` recovery pass.
    /// Resolved via `xa_commit(xid)` / `xa_rollback(xid)`.
    recovered_branches: Mutex<HashMap<Xid, RecoveredBranch>>,
    /// X-4: sentinel set tracking XIDs currently being resolved from a
    /// recovered state (removed from `recovered_branches` but I/O not yet
    /// complete).  A concurrent `xa_start(JOIN, xid)` during this window
    /// must receive `XaError::Protocol` (retryable) rather than
    /// `XaError::NotFound` (silently dropped join).
    ///
    /// Locking order: `recovered_branches` → `resolving_xids`.
    /// `xa_start` acquires `branches` → `resolving_xids` → `recovered_branches`
    /// (each held briefly, released before the next, so no cycle).
    resolving_xids: Mutex<HashSet<Xid>>,
    prepared_log: Option<PreparedLog>,
    /// Recovery-scan cursor state for `xa_recover` (audit
    /// X/Open requires `STARTRSCAN` to
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
    ///
    /// Also seeds the `recovered_branches` map from the
    /// engine's recovered prepared-txn list (the durable WAL
    /// `TxnPrepare` frames are the source of truth for crash
    /// durability), so `xa_recover()` returns in-doubt XIDs even when
    /// no `PreparedLog` is configured.
    pub fn new(env: Environment) -> Self {
        let recovered = Self::seed_recovered_branches(&env);
        Self {
            env,
            branches: Mutex::new(HashMap::new()),
            recovered_branches: Mutex::new(recovered),
            resolving_xids: Mutex::new(HashSet::new()),
            prepared_log: None,
            recover_cursor: Mutex::new(None),
        }
    }

    /// Pull the recovered prepared-txn list out of the
    /// engine's `EnvironmentImpl` and rebuild the (Xid →
    /// RecoveredBranch) map.
    ///
    /// Encoding: the XID format_id / gtrid / bqual fields stored in
    /// the WAL `TxnPrepare` frame are the same components used by
    /// `crate::xa::Xid`, so this round-trip is byte-exact.
    fn seed_recovered_branches(
        env: &Environment,
    ) -> HashMap<Xid, RecoveredBranch> {
        let mut map = HashMap::new();
        for info in env.recovered_prepared_txns() {
            let xid = match Xid::new(
                info.xid_format_id,
                &info.xid_gtrid,
                &info.xid_bqual,
            ) {
                Ok(xid) => xid,
                Err(e) => {
                    log::error!(
                        "noxu-xa: skipping recovered prepared txn {}: \
                         malformed XID in WAL: {e}",
                        info.txn_id
                    );
                    continue;
                }
            };
            map.insert(xid, RecoveredBranch { txn_id: info.txn_id });
        }
        map
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
    /// and `xa_commit`/`xa_rollback`/`xa_forget` remove it.
    ///
    /// As of wave 3-2, the `PreparedLog` is OPTIONAL: the WAL
    /// `TxnPrepare` frame written by `Transaction::prepare` is the
    /// durable source of truth, and `xa_recover()` populates its
    /// return value from the engine's recovered prepared-txn list
    /// regardless of whether a `PreparedLog` is configured.  The
    /// `PreparedLog` is retained as a convenience for operators who
    /// want to enumerate in-doubt XIDs without scanning the WAL
    /// (e.g. via a maintenance tool that doesn't open a full
    /// environment).
    pub fn with_prepared_log(mut self) -> Result<Self, crate::db::NoxuError> {
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
            //
            // X-4: also guard the window where xa_commit/xa_rollback has
            // already removed the XID from `recovered_branches` but I/O is
            // still in-flight (XID is in `resolving_xids`).  A JOIN on a
            // recovered or in-resolution XID is always a protocol violation
            // per X/Open: recovered branches are PREPARED, not Active.
            if self.resolving_xids.lock().unwrap().contains(xid) {
                return Err(XaError::Protocol(
                    "cannot join: XID is being resolved from a recovered \
                     state; retry after xa_commit/xa_rollback completes"
                        .to_string(),
                ));
            }
            if self.recovered_branches.lock().unwrap().contains_key(xid) {
                return Err(XaError::Protocol(
                    "cannot join: XID is a recovered (prepared) branch; \
                     xa_commit or xa_rollback must be called instead"
                        .to_string(),
                ));
            }
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
        // Wave 3-2: also reject if the XID is already pending
        // resolution from a previous process — starting fresh work
        // under it would be ambiguous.
        if self.recovered_branches.lock().unwrap().contains_key(xid) {
            return Err(XaError::DuplicateXid);
        }

        let config = TransactionConfig::new();
        let txn =
            self.env.begin_transaction(Some(&config)).map_err(XaError::Db)?;

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

    #[allow(deprecated)] // uses Transaction::get_inner_txn — internal wiring
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
        // the `mark_write` footgun (Sprint 3, audit Finding 3).
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

        // Wave 3-2: write the durable TxnPrepare WAL frame.  The
        // `Transaction::prepare` call serializes (txn_id, first_lsn,
        // last_lsn, xid_format_id, xid_gtrid, xid_bqual) into the WAL
        // and fsyncs before returning, so a crash immediately after
        // this point still yields the XID via xa_recover() on the
        // next environment open.
        branch
            .txn
            .prepare(
                xid.format_id,
                &xid.global_transaction_id,
                &xid.branch_qualifier,
            )
            .map_err(XaError::Db)?;

        // Persist prepared record for operator-facing recovery (the
        // WAL frame is the durable source of truth; this database is
        // a convenience).
        if let Some(ref log) = self.prepared_log {
            if let Err(e) = log.record_prepare(xid) {
                log::warn!(
                    "noxu-xa: PreparedLog.record_prepare failed for {xid:?}: \
                     {e}; WAL TxnPrepare frame is still durable so xa_recover() \
                     will surface this XID after a crash"
                );
            }
        }
        branch.state = BranchState::Prepared;
        log::debug!("xa_prepare: {xid:?} -> Prepared");
        Ok(PrepareResult::Ok)
    }

    fn xa_commit(&self, xid: &Xid, flags: XaFlags) -> XaResult<()> {
        // Fast path: in-memory branch (this process prepared the XID).
        {
            let mut branches = self.branches.lock().unwrap();
            if let Some(branch) = branches.get_mut(xid) {
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

                let branch = branches.remove(xid).unwrap();
                if branch.state == BranchState::Prepared {
                    branch
                        .txn
                        .resolved_commit_after_prepare()
                        .map_err(XaError::Db)?;
                } else {
                    // ONEPHASE: ordinary commit.
                    branch.txn.commit().map_err(XaError::Db)?;
                }
                if let Some(ref log) = self.prepared_log {
                    let _ = log.remove(xid);
                }
                log::debug!("xa_commit: {xid:?}");
                return Ok(());
            }
        }

        // Wave 3-2: recovered branch path.
        if flags.contains(XaFlags::ONEPHASE) {
            return Err(XaError::Protocol(
                "xa_commit(ONEPHASE): cannot one-phase commit a \
                 recovered branch"
                    .into(),
            ));
        }
        let mut recovered = self.recovered_branches.lock().unwrap();
        let recovered_branch =
            recovered.remove(xid).ok_or(XaError::NotFound)?;
        // X-4: insert into resolving_xids BEFORE dropping recovered_branches
        // so a concurrent xa_start(JOIN, xid) never sees a window where the
        // XID has been removed from recovered_branches but I/O is not yet
        // complete and returns XaError::NotFound.  With the sentinel in place
        // xa_start(JOIN) returns XaError::Protocol (retryable) instead.
        self.resolving_xids.lock().unwrap().insert(xid.clone());
        drop(recovered);

        // Replay the prepared txn's LNs into the in-memory tree so
        // subsequent reads see the committed data without waiting for
        // the next environment open.
        let lns = self.env.take_recovered_prepared_lns(recovered_branch.txn_id);
        let apply_result =
            self.env.apply_recovered_prepared_lns(&lns).map_err(XaError::Db);

        // Write the durable TxnCommit WAL frame.
        let commit_result = if apply_result.is_ok() {
            self.env
                .write_txn_commit_for_recovered(recovered_branch.txn_id)
                .map_err(XaError::Db)
        } else {
            apply_result
        };

        // Drop from the engine's recovered list and from the
        // operator-facing PreparedLog.
        self.env.forget_recovered_prepared_txn(recovered_branch.txn_id);
        if let Some(ref log) = self.prepared_log {
            let _ = log.remove(xid);
        }

        // X-4: remove the in-resolution sentinel.  At this point any
        // concurrent xa_start(JOIN) will correctly receive NotFound.
        self.resolving_xids.lock().unwrap().remove(xid);

        commit_result?;
        log::debug!("xa_commit: {xid:?} (recovered)");
        Ok(())
    }

    fn xa_rollback(&self, xid: &Xid, flags: XaFlags) -> XaResult<()> {
        let _ = flags;
        // Fast path: in-memory branch.
        {
            let mut branches = self.branches.lock().unwrap();
            if let Some(branch) = branches.get(xid) {
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
                if branch.state == BranchState::Prepared {
                    branch
                        .txn
                        .resolved_abort_after_prepare()
                        .map_err(XaError::Db)?;
                } else {
                    branch.txn.abort().map_err(XaError::Db)?;
                }
                if let Some(ref log) = self.prepared_log {
                    let _ = log.remove(xid);
                }
                log::debug!("xa_rollback: {xid:?}");
                return Ok(());
            }
        }

        // Wave 3-2: recovered branch path.
        let mut recovered = self.recovered_branches.lock().unwrap();
        let recovered_branch =
            recovered.remove(xid).ok_or(XaError::NotFound)?;
        // X-4: same sentinel pattern as xa_commit's recovered path.
        self.resolving_xids.lock().unwrap().insert(xid.clone());
        drop(recovered);

        // Discard the prepared LN replay list — nothing to apply.
        let _ = self.env.take_recovered_prepared_lns(recovered_branch.txn_id);

        // Write the durable TxnAbort WAL frame.
        let abort_result = self
            .env
            .write_txn_abort_for_recovered(recovered_branch.txn_id)
            .map_err(XaError::Db);

        self.env.forget_recovered_prepared_txn(recovered_branch.txn_id);
        if let Some(ref log) = self.prepared_log {
            let _ = log.remove(xid);
        }

        // X-4: remove the in-resolution sentinel.
        self.resolving_xids.lock().unwrap().remove(xid);

        abort_result?;
        log::debug!("xa_rollback: {xid:?} (recovered)");
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

            // Wave 3-2: in-doubt branches surfaced by recovery.  These are
            // backed by durable WAL `TxnPrepare` frames; xa_commit /
            // xa_rollback resolve them through the recovered_branches
            // map.
            for xid in self.recovered_branches.lock().unwrap().keys().cloned() {
                if !prepared.contains(&xid) {
                    prepared.push(xid);
                }
            }

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
        let mut found = false;
        // In-memory branch: if it was prepared, write a durable
        // TxnAbort so the next recovery does NOT surface the XID
        // again from the still-fsync'd TxnPrepare WAL frame.
        let in_mem_branch = self.branches.lock().unwrap().remove(xid);
        if let Some(branch) = in_mem_branch {
            if branch.state == BranchState::Prepared {
                // Equivalent to an implicit rollback: discard the
                // prepared writes (which are still in the in-memory
                // tree), apply undo, and write a TxnAbort frame.
                branch
                    .txn
                    .resolved_abort_after_prepare()
                    .map_err(XaError::Db)?;
            }
            // Other states (Idle, RollbackOnly, Active, Suspended): the
            // X/Open spec only allows forget on Prepared / heuristically-
            // completed branches, but we accept the legacy behaviour of
            // dropping the Branch silently for backwards compatibility
            // with v1.5.  The Transaction's Drop will best-effort abort
            // the inner txn.
            found = true;
        }
        if let Some(rec) = self.recovered_branches.lock().unwrap().remove(xid) {
            // The XA TM has decided to forget this in-doubt branch
            // without resolving it.  Treat it as an implicit rollback
            // for durability: write a TxnAbort frame so a subsequent
            // recovery does not surface the XID again.  The data was
            // never applied (prepared LNs are not redone), so there is
            // nothing to undo in the tree.
            let _ = self.env.take_recovered_prepared_lns(rec.txn_id);
            self.env
                .write_txn_abort_for_recovered(rec.txn_id)
                .map_err(XaError::Db)?;
            self.env.forget_recovered_prepared_txn(rec.txn_id);
            found = true;
        }
        if let Some(ref log) = self.prepared_log {
            // Persistent-log entries always succeed at forget; an
            // entry without a matching in-memory or recovered branch
            // is treated as already-cleaned (idempotent).
            let recovered = log.recover_all().unwrap_or_default();
            if recovered.contains(xid) {
                found = true;
            }
            let _ = log.remove(xid);
        }
        if !found {
            return Err(XaError::NotFound);
        }
        log::debug!("xa_forget: {xid:?}");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{DatabaseConfig, DatabaseEntry, EnvironmentConfig};
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
        assert_eq!(status, crate::db::OperationStatus::Success);
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
        assert_eq!(status, crate::db::OperationStatus::NotFound);
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
        assert_eq!(status, crate::db::OperationStatus::Success);
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

    /// STARTRSCAN / ENDRSCAN
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

    /// X-4: Recovered XA branch TOCTOU — xa_start(JOIN) during resolution
    /// must return Protocol (retryable), never NotFound.
    ///
    /// Simulates the race by directly injecting a synthetic entry into
    /// `recovered_branches` and `resolving_xids` (via the Mutex fields),
    /// then verifying that xa_start(JOIN, xid) returns the correct errors.
    ///
    /// The full concurrent scenario (xa_commit racing xa_start) would require
    /// carefully interleaved threads; this test verifies the invariants that
    /// the sentinel-based fix relies on.
    #[test]
    fn test_xa4_join_on_recovered_xid_returns_protocol_not_notfound() {
        let (xa, _dir) = make_xa_env();
        let xid = Xid::new(1, b"xa4-gtrid", b"xa4-bqual").unwrap();

        // Phase A: XID is in recovered_branches (before resolution starts).
        // xa_start(JOIN) must return Protocol, not NotFound.
        {
            let mut rec = xa.recovered_branches.lock().unwrap();
            rec.insert(xid.clone(), RecoveredBranch { txn_id: 9999 });
        }
        let result = xa.xa_start(&xid, XaFlags::JOIN);
        assert!(
            matches!(result, Err(XaError::Protocol(_))),
            "xa_start(JOIN) on a recovered XID must return Protocol: {result:?}"
        );
        // Clean up
        xa.recovered_branches.lock().unwrap().remove(&xid);

        // Phase B: XID is in resolving_xids (mid-resolution I/O window).
        // xa_start(JOIN) must return Protocol, not NotFound.
        {
            xa.resolving_xids.lock().unwrap().insert(xid.clone());
        }
        let result = xa.xa_start(&xid, XaFlags::JOIN);
        assert!(
            matches!(result, Err(XaError::Protocol(_))),
            "xa_start(JOIN) while XID is being resolved must return Protocol: \
             {result:?}"
        );
        xa.resolving_xids.lock().unwrap().remove(&xid);

        // Phase C: XID is gone from both maps (resolution complete).
        // xa_start(JOIN) must return NotFound (correct: no active branch).
        let result = xa.xa_start(&xid, XaFlags::JOIN);
        assert!(
            matches!(result, Err(XaError::NotFound)),
            "xa_start(JOIN) after resolution must return NotFound: {result:?}"
        );
    }

    /// X-4: Verify that xa_start(JOIN) on an active in-memory branch still works.
    #[test]
    fn test_xa4_join_active_branch_still_works() {
        let (xa, _dir) = make_xa_env();
        let xid = Xid::new(1, b"xa4join", b"active").unwrap();

        xa.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
        // JOIN from another logical thread should succeed.
        xa.xa_start(&xid, XaFlags::JOIN).unwrap();

        xa.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
        xa.xa_rollback(&xid, XaFlags::NOFLAGS).unwrap();
    }
}
