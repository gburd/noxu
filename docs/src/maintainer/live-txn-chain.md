# Live transaction-chain reconstruction for HA syncup rollback

This note documents how a *live* syncup rollback on a replica reconstructs a
transaction's logrec chain, and how that differs from JE's live path and from
Noxu's own recovery-time rollback. It is the design context behind
`crates/noxu-rep/src/stream/live_txn_chain.rs`.

Background facts (confirmed by direct reading of the current code):

- `TxnChain::build` (`crates/noxu-recovery/src/txn_chain.rs`) is a faithful,
  unit-tested pure port of JE `TxnChain`'s constructor
  (`TxnChain.java:109-123` for the two constructor overloads, the
  backward-walk body at lines ~148-260). It takes `Vec<(Lsn, LnRecord)>` (a
  txn's logrecs, any order — it sorts descending by LSN itself) +
  `matchpoint: Lsn` + a `KeyCmp` closure, and returns a `TxnChain` you
  `pop()` in reverse-LSN order for `RevertInfo`s.
- `RecoveryManager::apply_revert_info` (recovery_manager.rs:1238-1300) already
  applies a popped `RevertInfo` to a live `Tree` via `tree.insert`/`tree.delete`
  — the exact tree-mutation primitives ordinary redo/undo already use. It is
  currently a **private `fn` on the `impl RecoveryManager` block**, not
  exported from the crate (`crates/noxu-recovery/src/lib.rs`'s `pub use
  recovery_manager::{...}` does NOT list it). Any live-syncup caller in
  noxu-rep either needs this made `pub`/free-standing, or needs an equivalent
  written next to the live chain source (same 15 lines, trivial either way).
- `RollbackTracker` / `rollback_tracker.rs` machinery is recovery-time-only
  (populated by `RecoveryManager::run_analysis`'s backward scan); nothing in
  the live syncup path populates it. It is not needed for live rollback: the
  durable RollbackStart/End bracketing already works — see
  `noxu_recovery::rollback`/`rollback_steps_1_to_4` in `replay.rs`, already
  wired from `ReplicatedEnvironment::execute_rollback`.

## What `TxnChain::build` needs as input (the contract the live source must satisfy)

```rust
pub fn build(
    logrecs: Vec<(Lsn, LnRecord)>,   // ONE txn's LN logrecs, ANY order
    matchpoint: Lsn,
    cmp: KeyCmp<'_>,                  // key comparator for the txn's db(s)
) -> TxnChain
```

Where `LnRecord` (`crates/noxu-recovery/src/log_scanner.rs:36-90`) needs, per
logrec, exactly these fields (everything else is unused by `build`):

- `db_id: u64`
- `operation: LnOperation` (Insert/Update/Delete)
- `key: Bytes`, `data: Option<Bytes>`
- `abort_lsn: Lsn`, `abort_known_deleted: bool`, `abort_key: Option<Bytes>`,
  `abort_data: Option<Bytes>` — the pre-txn before-image, read straight off
  the on-disk `LnLogEntry`'s abort fields (see below — these are ONLY present
  for transactional LN types).
- `txn_id` is read by the CALLER to group logrecs per txn BEFORE calling
  `build` (matching `RecoveryManager::build_rollback_chains`'s `per_txn:
  HashMap<i64, Vec<(Lsn, LnRecord)>>` grouping) — `TxnChain::build` itself
  never reads `rec.txn_id` (confirmed zero references in `txn_chain.rs`).

So a live chain source's job, precisely: **given a txn id, its last-logged
LSN, and a matchpoint LSN, walk backward through the WAL along that txn's
own chain of LN logrecs (or equivalently forward-scan-then-filter, see below)
collecting `(Lsn, LnRecord)` pairs for that txn id whose LSN is in
`(matchpoint, last_logged_lsn]` UNION enough pre-matchpoint history for
`TxnChain::build`'s revert targets to resolve.** `TxnChain::build`'s body
needs the FULL set of the txn's logrecs on both sides of the matchpoint (not
just the rolled-back ones), because a rolled-back logrec's revert target can
be a PRESERVED (at-or-below-matchpoint) logrec of the same txn (see
`test_intra_txnal_revert_to_v1_not_skip_both` — matchpoint sits BETWEEN two
same-txn writes to slot A, and the in-window write must revert to the
preserved one, not to `abort_lsn`). This matches
`RecoveryManager::build_rollback_chains`'s own comment (recovery_manager.rs
~1113-1119): "JE's TxnChain walks the txn's ENTIRE logrec chain
(lastLoggedLsn -> NULL via prevLsn); the matchpoint only decides rolled-back
vs preserved."

## How JE gets "the txn's entire logrec chain" vs. how Noxu must

JE's live path (`ReplayTxn.undoWrites`, `ReplayTxn.java:496-524`) walks
`currLsn = lastLoggedLsn`, then at each step
`undoLsn = undo.logEntry.getUserTxn().getLastLsn()` — i.e. it follows an
explicit **prev-LSN-of-same-txn pointer embedded in each on-disk `LNLogEntry`**
(see the class doc in `LNLogEntry.java:58-59`: "txn id -- if transactional /
prev LSN of same txn -- if transactional" is part of EVERY on-disk LN format
version). JE's live `ReplayTxn` (a `Txn` subclass) keeps `lastLoggedLsn` as
in-memory per-txn state (`Txn.java:178`, updated via
`Txn.addLogInfo`/`writeToLog`), so `ReplayTxn.rollback` can start the backward
walk from there directly, with NO forward re-scan needed at all — it just
chases `prevLsn` pointers backward through the log until `NULL_LSN` or the
matchpoint (whichever it needs).

**Noxu's on-disk `LnLogEntry` format has NO such prev-LSN-of-same-txn field.**
Confirmed by direct reading of `crates/noxu-log/src/entry/ln_log_entry.rs`
`write_to_log`/`parse_from_slice` (lines ~370-520): the transactional payload
is `flags | db_id | abort_lsn? | txn_id | abort_key? | abort_data? |
abort_vlsn? | abort_expiration? | expiration? | data | key` — `txn_id` is
there, but there is no chained prev-LSN. `abort_lsn` always points to the
**pre-txn** version (and that is exactly why `TxnChain` exists at all — to
reconstruct the missing intermediate links). This is also exactly why live
rollback is not a mere rewire of existing in-memory state — even if Noxu
tracked `last_lsn` per live txn (which `noxu-txn::Txn::last_lsn()` DOES,
confirmed at `crates/noxu-txn/src/txn.rs:977-991`, but `ReplicaReplay` has NO
`Txn` object at all for a streamed txn — it just buffers raw `(LnRecord, Lsn)`
pairs in a plain `HashMap`, see `replica_replay.rs:90`), there is still no
on-disk backward pointer to chase. **The only way to reconstruct "this txn's
other logrecs" from the WAL is a forward scan of the log collecting every
entry whose `txn_id` matches, exactly what `RecoveryManager::
build_rollback_chains` already does** (`scanner.scan_forward(NULL_LSN,
period.rollback_start_lsn)` then filters by `rec.txn_id` — recovery_manager.rs
~1119-1131). A live chain source must do the SAME kind of forward scan, just
driven by `(txn_id, matchpoint_lsn, last_logged_lsn)` instead of by a
`RollbackPeriod` from `RollbackTracker`.

## What the live re-scan must decode

A forward scan from `NULL_LSN` (or, as a bounded optimisation, from
`matchpoint_lsn` — see the next section) to `last_logged_lsn + 1`, decoding
every entry and keeping only `LogEntry::Ln(rec)` where `rec.txn_id ==
Some(target_txn_id)`. This needs a `LogScanner` (the `noxu-recovery` trait) or
equivalent over the replica's real `FileManager`. Two candidates already
exist in the codebase and BOTH already do this exact decode:

1. **`noxu_dbi::file_manager_scanner::FileManagerLogScanner`** — implements
   `noxu_recovery::LogScanner` fully (`scan_forward`, `scan_backward`,
   `read_at_lsn`), already used by `RecoveryManager` in-process. Currently
   `mod file_manager_scanner;` is **private** in `crates/noxu-dbi/src/lib.rs`
   (line 34) — not exported. To reuse it from `noxu-rep` (which already
   depends on both `noxu-dbi` and `noxu-recovery`, confirmed in
   `crates/noxu-rep/Cargo.toml` lines 39-44), either (a) make the module
   `pub` in noxu-dbi's `lib.rs` and re-export `FileManagerLogScanner`, or (b)
   write a second, smaller `LogScanner`-shaped reader directly in noxu-rep
   using the same raw-entry-read pattern `SyncupLogView::scan_with_manager`
   / `read_raw_entry` in `crates/noxu-rep/src/stream/syncup_reader.rs`
   already implements (that file already parses VLSN-tagged headers off a
   `noxu_log::FileManager` — it just doesn't currently decode LN payloads
   into `LnRecord`, only fingerprints/types). Route (a) is less code (reuse,
   don't duplicate) but requires a one-line visibility change in a crate this
   work doesn't otherwise touch; route (b) keeps the change contained to
   noxu-rep. Route (a) is preferred: it is a pure visibility widening (no
   behaviour change to noxu-dbi), and it avoids a second LN-payload decoder to
   keep in sync with `FileManagerLogScanner::parse_payload` (the same decoder
   `ReplicaReplay::apply_entry` already reuses via `parse_payload`).

2. Two log-entry types matter for the scan: `LogEntryType::InsertLNTxn /
   UpdateLNTxn / DeleteLNTxn` (the only transactional LN types — a
   non-transactional LN by definition has `txn_id = None` and can never
   belong to a chain). Everything else (structural IN/BIN, TxnCommit/Abort,
   checkpoint markers) is skipped by the txn_id filter automatically once
   decoded via `FileManagerLogScanner::parse_payload`/`scan_forward`, which
   already discriminates all of these into the `LogEntry` enum
   (`crates/noxu-recovery/src/log_scanner.rs:319-345`).

## Bounding the scan (performance, not correctness)

`RecoveryManager::build_rollback_chains` scans from `NULL_LSN` (the whole log)
because recovery's rollback periods are assumed small and it already has to
touch the whole log for other passes anyway. A live syncup rollback is a
narrower, more latency-sensitive operation (it blocks a replica reconnecting).
The forward scan can safely start at `matchpoint_lsn` instead of `NULL_LSN`:
any of the txn's PRE-matchpoint logrecs needed as revert targets are, by
definition, AT OR BEFORE the matchpoint LSN, so starting exactly at
`matchpoint_lsn` (not after it) captures them too. This is a real
optimisation over the recovery path's `NULL_LSN` start and is the preferred
default for the live source. A test should assert the bound doesn't drop a
needed preserved-slot logrec: a case where the preserved version is written
AT the matchpoint LSN itself, not before it — the off-by-one the bound could
otherwise introduce. (See
`test_bounded_scan_from_matchpoint_still_sees_preserved_target` in
`live_txn_chain.rs`.)

## Where the syncup rollback path calls it

`ReplicatedEnvironment::syncup_with_feeder`
(`crates/noxu-rep/src/replicated_environment.rs` ~2071-2124) is the call
site. After computing `tail_types` and BEFORE the `classify_tail` gate, the
live chain(s) for whichever txn ids appear in the tail are computed. The
per-txn call-site inputs are:

- `log_manager.file_manager()` (`LogManager::file_manager(&self) ->
  &Arc<FileManager>`, `noxu-log/src/log_manager.rs:1394`) — the SAME
  `FileManager` `SyncupLogView::scan_with_manager` already uses for its own
  re-read, so no new file handle/lock contention;
- `txn_id` (from the tail entry's decoded `LnRecord::txn_id`). Note
  `SyncupLogView::tail_types` only returns `(Vlsn, Option<LogEntryType>)` — it
  does NOT decode the LN payload, only the type byte. A live chain source
  needs the ACTUAL `LnRecord`s, so it does its own
  `FileManagerLogScanner::scan_forward` rather than reusing `SyncupLogView`'s
  lighter-weight scan;
- `last_logged_lsn` — the LSN of the tail entry itself (or the txn's highest
  LSN in the tail, if a txn logged more than one entry in the diverged tail —
  the walk needs to start from the TRUE last-logged LSN of the txn);
- `matchpoint_lsn` — already computed at this call site (`let matchpoint_lsn =
  match &matchpoint {...}`, line ~2058).

## Scope note: why this does not narrow `classify_tail`

Under noxu-rep's current architecture, `verify_rollback` already forces
`HardRecovery` whenever any committed/aborted txn-end exists above the
matchpoint (globally, not per-txn) — so a tail containing a COMMITTED txn's
LNs can never reach `classify_tail`'s `RollbackToMatchpoint` branch at all.
The only tails that reach `classify_tail` after `RollbackToMatchpoint` are:
(a) still-ACTIVE-txn LNs, which `ReplicaReplay` never applies to the tree
(nothing to revert — dropping the buffer already is a complete, correct
rollback); (b) non-transactional LNs, which carry NO on-disk abort info at
all (`LnLogEntry::write_to_log` only serialises `abort_lsn`/`abort_data` `if
self.is_transactional()`) so no revert can be built for them regardless of
chain machinery; (c) structural entries, whose hazard ("may be referenced by
a live parent") a value-revert doesn't address. The live chain source is
useful as a verified building block, but it should NOT be expected to narrow
`classify_tail` in this codebase's current form.
