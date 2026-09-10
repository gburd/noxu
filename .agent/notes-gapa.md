# Gap A — session notes (checkpoint, no code changes yet)

Working tree: `/tmp/w-gapa` (branch `feat/ha-gap-a-txnchain`). No `~/ws/je`
checkout in *this* environment was assumed present per the task instructions,
but a JE checkout WAS actually found at `/home/gburd/ws/je` and used for every
citation below (confirmed present, contra the "if absent" fallback wording in
the brief).

## Headline finding: most of "step 2" already exists — just not wired for syncup

`crates/noxu-recovery/src/txn_chain.rs` (`TxnChain::build`, `RevertInfo`,
`CompareSlot`) is **already a faithful, unit-tested, pure port of JE
`TxnChain`'s constructor** (`~/ws/je/src/com/sleepycat/je/txn/TxnChain.java`).
It was added in commit `b70cb456` ("feat(recovery): TxnChain version-revert for
rollback periods (REP-1 STEP 3)") to serve **crash-recovery's** rollback-period
undo pass (`RecoveryManager::build_rollback_chains` /
`run_undo_all` / `apply_revert_info`), NOT the live syncup path this task is
about.

Critically, `TxnChain::build(logrecs: Vec<(Lsn, LnRecord)>, matchpoint: Lsn, cmp:
KeyCmp) -> Self` takes a **pre-filtered list of one transaction's logrecs** —
it never reads `rec.txn_id` internally (confirmed: zero references in the
file). It is txn-agnostic and already exactly the "pure function, given a
txn's records above a matchpoint, produce the per-(db_id,key) revert set"
that the brief's step 2 asks for. Its test suite already includes the exact
tricky case named in the brief:

- `test_intra_txnal_revert_to_v1_not_skip_both` — a record written twice in
  the rolled-back span must revert to the **first post-matchpoint version's**
  abort info (JE's "intermediate versions are skipped, but a partial rollback
  must not skip past an intra-window intermediate" case), not the pre-txn
  version.
- `test_je_worked_example_two_slots` — reproduces JE's own worked example in
  `TxnChain.java`'s class doc verbatim (interleaved writes to two slots, chain
  of 4 revert-infos, exact expected LSNs).
- `test_first_write_reverts_to_pretxn` — matchpoint before the txn's first
  write: reverts all the way to the abort (pre-txn) version.

So the "revert-info walk, as a pure function with unit tests" that step 2
asks for is **already done, already tested, already correct** — for the
recovery-time caller. What is NOT done is feeding it from the **live syncup**
path (`ReplicatedEnvironment::execute_rollback` /
`crate::stream::syncup::classify_tail`), which today has no active `Txn`
table to walk at all (see below) and refuses instead.

## Where the ordinary-abort undo machinery lives, and whether it can be driven by matchpoint LSN

Two parallel undo paths already exist in the codebase, both already reusable
in principle:

1. **Live ordinary txn abort** — `noxu-txn::Txn::abort_collect_undo` (txn.rs
   ~1333) collects `UndoRecord { current_lsn, abort_lsn, abort_known_deleted,
   abort_data, abort_key, database_id }` from `WriteLockInfo` WITHOUT
   releasing locks; the caller (`noxu-db::Transaction::abort`,
   transaction.rs ~695-787) then applies each `UndoRecord` directly to the
   live `Tree` via `tree.delete(&abort_key)` / `tree.insert(abort_key,
   abort_data, lsn)`, sorted newest-LSN-first, THEN releases write locks.
   This is **per-txn**, driven by the txn's own `write_locks` map (i.e. by
   the aborting txn's own write-lock set) — it has no notion of "matchpoint",
   it always reverts every write-locked record all the way to its
   `abort_lsn` (the txn's *pre-txn* version), not to an arbitrary
   intermediate point. **This is NOT directly reusable for the syncup
   partial-rollback case** without generalisation, because a live syncup
   rollback needs the `TxnChain`-style intermediate-stop behaviour (revert to
   the last version *at or before the matchpoint*, not the pre-txn version) —
   exactly the distinction `TxnChain::build` exists to make. `UndoRecord` /
   `WriteLockInfo` carry only ONE abort version per record (the txn's
   original pre-txn image), so they cannot express "stop at an intermediate
   in-txn version" at all. Driving `abort_collect_undo` by matchpoint LSN is
   not a matter of passing a different LSN in — the underlying data
   structure does not hold the intermediate versions needed.

2. **Recovery-time rollback-period undo** — `RecoveryManager::run_undo_all`
   (recovery_manager.rs ~1358) + `build_rollback_chains` (~1093) +
   `apply_revert_info` (~1238). THIS is the one that already IS driven by
   matchpoint LSN (`period.matchpoint_lsn`) rather than by an aborting txn's
   write-lock set: it re-scans the WAL forward from `NULL_LSN` to
   `rollback_start_lsn`, groups logrecs by (period, txn_id), builds a
   `TxnChain` per (matchpoint_lsn, txn_id) pair, and pops `RevertInfo`s in
   reverse-LSN order during the backward undo scan, applying each via
   `apply_revert_info` which does the SAME `tree.delete` / `tree.insert`
   primitive operations the ordinary-abort path uses (both ultimately bottom
   out in `noxu_tree::Tree::insert` / `Tree::delete` — there is exactly ONE
   BIN-mutation primitive pair in the codebase, which is good: whichever path
   we drive, we are not hand-rolling BIN mutation).

**Conclusion for step 3 (not attempted this session): the live-syncup undo
apply step should mirror `RecoveryManager::apply_revert_info` (matchpoint-LSN
driven, TxnChain-based), NOT `Txn::abort_collect_undo`/`Transaction::abort`
(write-lock-driven, single-abort-version). The two are structurally
different because the recovery path already had to solve "revert to an
intermediate point," which is exactly Gap A's problem; ordinary abort never
had to.** The brief's phrasing ("Reuse the machinery ordinary txn abort
already uses / the inverse of `apply_redo_ln`") is partially right in spirit
(reuse `Tree::insert`/`Tree::delete`, don't hand-roll) but the specific
function to imitate is `apply_revert_info` + `TxnChain`, already built for
recovery, not `abort_collect_undo`.

## Is `rollback_tracker.rs` reusable for the live-syncup case?

Yes, structurally, but it is currently ANALYSIS-TIME/RECOVERY-time-only in
how it gets populated (`record_rollback_start`/`record_rollback_end`, called
from `RecoveryManager::run_analysis` while backward-scanning the WAL after a
restart). `RollbackTracker::register_rollback_start_with_txns` /
`register_rollback_end` are pure data-structure ops with no I/O dependency,
so they COULD be called live (i.e., right after
`noxu_recovery::rollback_steps_1_to_4` writes the `RollbackStart` record in
the live syncup path) to populate a live equivalent — but nothing in
`ReplicatedEnvironment::execute_rollback` (replicated_environment.rs
~2130-2180) does this today: it calls `noxu_recovery::rollback(...)`
(replay.rs) which writes RollbackStart/End + make-invisible + fsync, and
never touches a `RollbackTracker` at all. The live rollback path currently
has NO in-memory notion of "which txns were active at the matchpoint" — it
passes `active_txn_ids: Vec::new()` unconditionally (see the comment at
replicated_environment.rs ~2170: "the harness/VLSN-index model has no live
txn table here"). This is the real blocker for step 2/3 on the LIVE (not
recovery) path: **there is currently no live analogue of JE's
`Replay.activeTxns` (`ReplayTxn` map) on the noxu-rep side** — the closest
thing, `noxu_dbi::ReplicaReplay::active_txns: HashMap<u64, Vec<(LnRecord,
Lsn)>>`, only holds **uncommitted, never-applied** transactional LNs (exactly
the case `classify_tail` already handles safely by dropping the buffer). It
does NOT track applied/committed txns' logrec chains at all, because once a
txn commits, `ReplicaReplay::commit_txn` drains and discards the buffer
(replica_replay.rs `commit_txn`, ~200-207) — there is nothing left in memory
to walk backward through after commit. **This means the live TxnChain walk
cannot be built from `ReplicaReplay`'s in-memory state for the cases
`classify_tail` currently refuses (committed txns, non-transactional LNs);
it would have to re-scan the replica's own WAL** (exactly as
`SyncupLogView::scan` / `build_rollback_chains` already do), which is
possible in principle (the replica's WAL is the byte-shadow source of truth)
but is new plumbing, not a rewire of existing structures.

## What `classify_tail` refuses today (unchanged this session)

Confirmed by re-reading `crates/noxu-rep/src/stream/syncup.rs`
`classify_tail` (lines ~245-330) and its 2 pinned tests
(`crates/noxu-rep/tests/syncup_matchpoint_rollback_test.rs::
test_diverged_replica_with_applied_tail_is_refused`,
`crates/noxu-rep/tests/rep1_step5_live_syncup_test.rs::
test_rollback_past_commit_needs_restore`) plus the in-file unit tests:

- SAFE (truncated, no tree work): every tail entry is a provisional
  transactional LN (`is_ln_type() && is_transactional()`), because
  `ReplicaReplay` buffers these and never applies them without a commit.
- REFUSED: undecodable type byte; `TxnCommit`/`TxnAbort` in the tail (should
  already have been routed to `HardRecovery` by `verify_rollback`, refuse if
  seen anyway rather than assume); a non-transactional LN (already applied to
  the live tree, `ReplicaReplay::apply_ln`); any structural/other entry
  (IN/BIN-delta/etc., "may be referenced by a live parent").

I did **not** touch `classify_tail` or `verify_rollback` this session. No
line of either function changed. Confirmed via `git diff main --stat`
against this worktree at checkpoint time: zero modified files, zero commits
made yet.

## Gap B interaction (informational, no action needed)

Told at checkpoint that Gap B merged to main with a new test asserting
re-syncup still refuses an unsafely-divergent tail. I have not looked at that
test's exact name/location yet (out of scope for Gap A investigation so far,
and this worktree has not pulled the Gap B merge commit — `git status` shows
`behind 'origin/main' by 4 commits` from before this session started, i.e.
unrelated to Gap B). Treating it as a guardrail: any future step-4 change in
this worktree must keep that test green, same as
`test_diverged_replica_with_applied_tail_is_refused` and
`test_rollback_past_commit_needs_restore`.

## What remains for a future session (in order)

1. **Step 1 (RollbackStart/RollbackEnd bracketing)**: already fully done for
   the live path — `noxu_recovery::rollback` / `rollback_steps_1_to_4`
   (replay.rs) already write RollbackStart, make-invisible, fsync,
   RollbackEnd, and are already called from
   `ReplicatedEnvironment::execute_rollback`. Nothing to add here; this was
   apparently completed in a prior session (REP-1 STEP 5, see
   `rep1_step5_live_syncup_test.rs`). The brief's step 1 is a NO-OP for the
   syncup-rollback path specifically (it already exists); what is genuinely
   missing is feeding `RollbackTracker` from the LIVE path the way analysis
   does from recovery (see above) — needed once step 2/3 need it.
2. **Step 2 (pure revert-info walk)**: the algorithm exists and is tested
   (`TxnChain::build` in noxu-recovery). What's missing is a *live* source of
   "a txn's records above a matchpoint" to feed it — i.e., a live-WAL variant
   of `RecoveryManager::build_rollback_chains` that runs against the
   replica's own on-disk log (via `SyncupLogView`-style forward scan, which
   already exists and already exposes `entries()` / raw type bytes) rather
   than against `AnalysisResult`'s recovery-only active-txn bookkeeping. This
   is new glue code, not new algorithm.
3. **Step 3 (undo apply)**: mirror `apply_revert_info`
   (`RecoveryManager::apply_revert_info`), NOT `Txn::abort_collect_undo` (see
   above) — same `Tree::insert`/`Tree::delete` primitives, driven by
   `RevertInfo` popped from a live-built `TxnChain`.
4. **Step 4 (narrow the refusal)**: only after 2+3 are real and tested. NOT
   attempted. `classify_tail`/`verify_rollback` are UNCHANGED from main.

No code was written this session; this is pure investigation banked as notes
per the checkpoint request. The next session should start by writing the
live-WAL-scan glue for step 2 (item 2 above) reusing `SyncupLogView`'s
existing per-VLSN type/LSN data rather than `AnalysisResult`.
