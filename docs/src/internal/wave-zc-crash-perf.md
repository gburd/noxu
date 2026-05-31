# Wave ZC — crash-safety + performance fixes (re-audit follow-up)

**Target**: v3.1.0. Addresses Keith's 2026-05 re-audit findings
(`reaudit-2026-05-keith.md`) in the log / cleaner / recovery / replication
layers.

## Items

| Finding | Severity | Status | Summary |
|---|---|---|---|
| **R-2** | High (regression) | Fixed | `LogFlushTask` (added by X-11) held the log-write-latch across `pwrite64`, stalling all foreground commits while the background flush ran. `flush_no_sync` now snapshots the dirty buffers + positions under the LWL, releases it, and performs the `pwrite` I/O without the latch held. Eliminates periodic commit-latency spikes when `log_flush_no_sync_interval_ms > 0`. |
| **R-1** | Medium | Fixed | `collect_dirty_buffers` reuses a flush buffer `Vec` instead of allocating a fresh `Vec<(Vec<u8>, u64)>` with a `to_vec()` copy per dirty buffer on every flush. |
| **P-1** | High (perf) | Fixed | `FSyncGroup` thundering-herd: added an `AtomicBool` fast-path so waiters observe completion without all re-acquiring the inner mutex (the minimal fix designed in wave-11-J but never shipped). Verified against `test_fsync_before_commit_invariant`. |
| **R-7** | High (crash-safety) | Fixed | The cleaner's `migrate_ln_slot` previously fell back to the stale `log_lsn` if `write_migration_ln` failed, leaving a slot pointing at a file the cleaner then deleted (silent data loss on recovery). Migration of that slot is now aborted on WAL-write failure and the source file is retained. |
| **R-3** | High (crash-safety) | Fixed | A recovered XA `TxnCommit` was written with `NULL_VLSN`; the X-14 VLSN rebuild ignored `TxnCommit` records, so after a second crash post-XA-resolution the commit was invisible to replication. Recovered-XA commits now carry a real VLSN in replicated mode, and the recovery VLSN rebuild includes `TxnCommit`-derived VLSNs. |
| **R-5** | Medium | Documented + tested | Non-transactional `open_database(None, …)` writes a plain `NameLN` with `txn_id = None`. This is *correct*: a non-transactional create is durably committed at write time (no wrapping transaction to abort), so recovery's undo predicate correctly treats it as committed. Invariant documented in `run_mapping_tree_undo_pass` and covered by a test. |
| **P-2** | High (perf) | Scoped (design note) | W11 recovery remains ~2.9× JE at 100K records. The path that would close most of the gap — restoring BINs directly from the analysis-pass `dirty_in_map` instead of replaying every LN — is a sizeable change (it requires materialising BINs from the dirty-IN map and reconciling with the redo pass). Deferred to a dedicated follow-up wave; see the design note below. |

## P-2 design note (deferred)

Recovery currently rebuilds the tree by replaying every committed LN from the
last checkpoint forward (`redo_ln`). At 100K records this dominates re-open
time (~2.9× JE). JE avoids most of this by reconstructing BINs from the
checkpoint's dirty-IN map and applying only the deltas after it.

Proposed approach for a future wave:

1. During the analysis pass, retain the per-BIN dirty-slot map already built
   for the cleaner-barrier / utilization work.
2. In the redo pass, for each BIN present in the checkpoint, materialise it
   from its last full-BIN log entry + subsequent BIN-deltas, then apply only
   the post-checkpoint LN redo records that fall in that BIN — rather than
   inserting every LN into an empty tree.
3. Reconcile with the C-6 mapping-tree undo pass (catalog entries must be
   resolved before BIN materialisation references their database roots).

Estimated effort: medium-large (touches `recovery_manager`, `noxu-tree`
BIN deserialisation, and the analysis/redo boundary). Risk: high (recovery
correctness). Acceptance: W11 within ~1.5× JE without regressing any
existing recovery/crash test. Tracked as a standalone wave.

## Gate

`cargo fmt`, `clippy -D warnings`, `doc -D warnings`, `cargo test --workspace`,
and `make docs-check` all pass (verified at integration into `main`).
