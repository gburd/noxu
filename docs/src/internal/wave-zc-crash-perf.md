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

### Current P-2 status (Wave GB outcome)

**Wave GB** implemented the DbTree foundation and applied the escape hatch.
See `docs/src/internal/wave-gb-dbtree-recovery.md` for the full analysis.

Summary of Wave GB STEP-0 findings:

| Property | Result |
|---|---|
| All BINs in memory at checkpoint (eviction) | Safe (fragile — child arcs never nulled) |
| BINDelta chains | Handled via force-full-flush pass |
| `InEntry.lsn` top-down walk | Does NOT work (slot LSNs not updated on BIN re-log) |
| Scan-reduction (`first_active_lsn = CkptStart`) | DEFERRED — open-txn-at-crash correctness gap |

**What Wave GB prototyped** (on the `fix/gb-dbtree-recovery` branch — **not
merged to main**):

- `DbTreeEntry` BIN-version index written at each `CkptEnd` (foundation only;
  `first_active_lsn` unchanged; full scan preserved).

- LSN-aware `redo_insert` (correctness fix).

- Wave GB equality harness: 11 tests, all PASS.

**Why it was not merged**: the write-side alone force-flushes delta BINs to
full and walks the whole tree at every checkpoint, yet recovery still
full-scans and never reads the index — pure overhead until the scan-reduction
that consumes it is correct.

**Remaining prerequisite for full P-2**:

- `first_active_lsn = min(earliest_open_txn_lsn, CkptStart)` requires the
  checkpointer to have access to the transaction manager (not yet wired).

- Once implemented, the BIN pre-loading path in recovery can be enabled and
  the W11 speedup target (~1.5×) becomes achievable.

## Gate

`cargo fmt`, `clippy -D warnings`, `doc -D warnings`, `cargo test --workspace`,
and `make docs-check` all pass (verified at integration into `main`).
