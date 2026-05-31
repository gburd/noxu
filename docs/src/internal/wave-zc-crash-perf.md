# Wave ZC — crash-safety + performance fixes (re-audit follow-up)

**Target**: v3.1.0. Addresses Keith's 2026-05 re-audit findings
(`reaudit-2026-05-keith.md`) in the log / cleaner / recovery / replication
layers.

## Items

| Finding | Severity | Status | Summary |
|---|---|---|---|
| **R-2** | High (regression) | Fixed | `LogFlushTask` (added by X-11) held the log-write-latch across `pwrite64`, stalling all foreground commits while the background flush ran. `flush_no_sync` now snapshots the dirty buffers + positions under the LWL, releases it, and performs the `pwrite` I/O without the latch held. Eliminates periodic commit-latency spikes when `log_flush_no_sync_interval_ms > 0`. |
| **R-1** | Medium | Partial | `collect_dirty_buffers` reuses the outer buffer collection across `flush_sync` calls instead of reallocating it. The inner per-buffer `to_vec()` copy is retained by design — once R-2 releases the LWL before I/O, the snapshotted bytes must be owned. Net: one fewer allocation per flush. |
| **P-1** | High (perf) | Fixed | `FSyncGroup` thundering-herd: added an `AtomicBool` fast-path so waiters observe completion without all re-acquiring the inner mutex (the minimal fix designed in wave-11-J but never shipped). Verified against `test_fsync_before_commit_invariant`. |
| **R-7** | High (crash-safety) | Fixed | The cleaner's `migrate_ln_slot` previously fell back to the stale `log_lsn` if `write_migration_ln` failed, leaving a slot pointing at a file the cleaner then deleted (silent data loss on recovery). Migration of that slot is now aborted on WAL-write failure and the source file is retained. |
| **R-3** | High (crash-safety) | Fixed | A recovered XA `TxnCommit` was written with `NULL_VLSN`; the X-14 VLSN rebuild ignored `TxnCommit` records, so after a second crash post-XA-resolution the commit was invisible to replication. Recovered-XA commits now carry a real VLSN in replicated mode, and the recovery VLSN rebuild includes `TxnCommit`-derived VLSNs. |
| **R-5** | Medium | Documented + tested | Non-transactional `open_database(None, …)` writes a plain `NameLN` with `txn_id = None`. This is *correct*: a non-transactional create is durably committed at write time (no wrapping transaction to abort), so recovery's undo predicate correctly treats it as committed. Invariant documented in `run_mapping_tree_undo_pass` and covered by a test. |
| **P-2** | High (perf) | Scoped (design note) | W11 recovery remains ~2.9× JE at 100K records. The path that would close most of the gap — restoring BINs directly from the analysis-pass `dirty_in_map` instead of replaying every LN — is a sizeable change (it requires materialising BINs from the dirty-IN map and reconciling with the redo pass). Deferred to a dedicated follow-up wave; see the design note below. |

## P-2 design note (deferred — see Wave FC investigation)

Recovery currently rebuilds the tree by replaying every committed LN from the
last checkpoint forward (`redo_ln`). At 100K records this dominates re-open
time (~2.9× JE). JE avoids most of this by reconstructing BINs from the
checkpoint's dirty-IN map and applying only the deltas after it.

**Wave FC** (branch `fix/fc-p2-recovery-binrestore`) conducted a complete
investigation and found a fundamental correctness blocker.  Full findings are
in `docs/src/internal/wave-fc-p2-recovery.md`.  Summary:

### Why P-2 cannot be implemented safely today

Noxu's checkpointer only flushes **dirty** BINs (those modified since the
last checkpoint).  Stable BINs that had no writes since the previous
checkpoint are not re-logged.  This means:

- `dirty_ins` (the analysis-pass map) only contains BINs touched in the
  current recovery interval.
- Keys in **stable** BINs (modified before the last checkpoint) are not in
  `dirty_ins` and not in `redo_entries` if the scan is narrowed to
  `checkpoint_start_lsn`.
- Narrowing the scan would silently lose those keys.

The current `first_active_lsn = Lsn::new(0, 0)` (scan from beginning of log)
is intentionally conservative and correct.  A "conservative P-2" that keeps
the full scan but adds BIN pre-population provides ≤ 20 % improvement — not
the 1.5× target — because the LN-redo loop still iterates all N entries.

### What is needed for a correct full P-2

1. **DbTree (BIN-version index)** — a mapping tree written at `CkptEnd` that
   records the current-version LSN for every BIN (stable and dirty alike),
   mirroring JE's `_jeNameTree` / `DbTree`.  Without this index there is no
   way to fetch stable BINs at recovery time.
2. **Correct `first_active_lsn`** — once the DbTree can supply all BINs,
   the checkpoint writes `first_active_lsn = CkptStart` (not `Lsn::new(0,0)`),
   and recovery replays only post-checkpoint LNs (providing the 3–5× speedup).
3. **BINDelta chain tracking** — `AnalysisResult.dirty_ins` must keep the
   full chain per node (last full BIN + subsequent deltas) and `InRecord`
   must carry `prev_full_lsn` from `BinDeltaLogEntry`.
4. **LSN-aware `redo_insert`** — skip writes where the BIN already has the
   key at the same or newer LSN (overlap case from the CkptStart–BIN-lock
   window).
5. **Tree bulk-load API** — `Tree::build_from_checkpoint_bins(Vec<BinStub>)`
   for O(N) BIN insertion instead of O(N log N) individual-key inserts.

Estimated effort for the full correct P-2: **large** (new DbTree subsystem +
all items above; touches `noxu-log`, `noxu-recovery`, `noxu-tree`, `noxu-dbi`).
Risk: **high** (recovery correctness).  Acceptance: W11 within ~1.5× JE
without regressing any existing recovery/crash test.  Tracked as a future wave
requiring the DbTree prerequisite.

## Gate

`cargo fmt`, `clippy -D warnings`, `doc -D warnings`, `cargo test --workspace`,
and `make docs-check` all pass (verified at integration into `main`).
