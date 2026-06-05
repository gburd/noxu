# v3.x Production-Readiness Review — Synthesis

**Date**: 2026-06-03
**Code reviewed**: `origin/main` at v3.2.0 (commit `34171f6` and follow-ups).
**Reviewers (persona lenses)**: Justin Sheehy, Michael Cahill, Margo Seltzer,
Keith Bostic, Linda Lee, Charlie Lamb, Jon Gjengset (jonhoo).
**Method**: four parallel domain reviews, each finding cross-checked against
`origin/main` (an initial pass against a stale local checkout was re-validated;
findings already fixed on main were dropped).

Source reports (full evidence, file:line, JE references, suggested fixes):

- `review-txn-isolation-2026-06.md` — transactions, isolation, locking, recovery
- `review-storage-core-2026-06.md` — tree, log, cleaner, evictor, on-disk format
- `review-rust-api-2026-06.md` — `unsafe`, concurrency, public API soundness
- `review-claims-docs-tooling-2026-06.md` — claims, docs, tests, config, tooling

## Verdict

**`main` is not yet ready to be promoted as a production major release.** The
review surfaced genuine Critical correctness and memory-safety defects, several
documented guarantees the code does not deliver, and a body of stale
documentation/comments. None are "scale/soak" concerns — they are functional
correctness, soundness, and honesty issues that must be closed (or the claims
withdrawn) before advertising production readiness.

This document is the canonical blocker list. Items fixed in the same change set
that introduced this document are marked **FIXED**; the rest are the prioritized
backlog.

## Remediation status (2026-06-03 follow-up)

Worked through the blocker list after the initial review. Outcome per item:

**Fixed and merged** (each with a regression test, full local gate green):

- S-C1 (WAL fsync fast-path durability) — `defef9f`
- T-F1 (recovery undo currency check, defensive/JE alignment) — `a4a7d5c`
- R-F03 (mmap of live write file) + R-F04 (XA use-after-free; `noxu-xa` now
  `#![forbid(unsafe_code)]`) — `9d02b28`
- R-F05 (latch 0-hash panic) + honest-claim/doc corrections — `b1e6e69`
- T-F5 (explicit txns leaked `TxnManager` entries) — `ddafb96`
- **St-H4** (binary upper-IN floor-search, unified across all 8 descent
  sites; also fixed `search_with_coupling` ignoring a custom comparator) —
  `5fa49c3`
- **R-F01** (`LogBufferSegment` move-UB: latch/pin-count moved into a shared
  `Arc<LogBufferControl>`) — `d16c1c1`
- **St-H5** (`TreeNode::find_entry` non-exact Internal now returns the floor) —
  `b9b1d44`

That is every contained and medium-sized blocker from the review.

**Remaining — large dedicated efforts** (designs in
`deferred-blocker-designs-2026-06.md`; not rushed because a half-correct
version of each is *worse* than the current honest state):

- **T-F2** — SERIALIZABLE next-key range locking — **FIXED** (fix/tf2-range-locks).
- **C-C2** — `become_master` feeder / log-streaming threads (replication
  feature). **FULLY FIXED** in v3.2.0 (push-feeder threads) + v3.3.0 branch
  `fix/cc2b-wal-vlsn-autofeed` (C-C2b WAL-scanner auto-feed). `with_environment`
  now installs a VLSN counter; every `log_txn_commit` writes a VLSN-tagged
  22-byte WAL entry; `EnvironmentLogScanner` discovers and streams these to
  replicas without `replicate_entry` calls. Convergence test passes end-to-end.
- **St-C3 / St-H1 / St-H3** — **DONE**: St-H1/H3 docs corrected earlier; St-C3
  shipped LOG_VERSION 2→3 (v3 header CRC32, version-aware first-entry offset,
  v2 backward-compat).
- **St-H6** — **CLOSED (live bug, fixed).** `Tree::split_child` hardcoded
  `expiration_in_hours: false` on the right-half sibling BIN, causing
  hours-granularity TTL records in the right half of every split to be
  silently treated as expired on read (128/256 keys missing in the
  benchmark scenario).  Fix: inherit the flag from the splitting BIN;
  add three ancillary `true`-corrections and a `debug_assert!` guard.
  Regression test: `test_ttl_records_survive_bin_split_right_sibling_256`
  (FAIL-PRE / PASS-POST confirmed).  The original latent concern
  (serialization omission) is confirmed harmless: `deserialize_full`
  hardcodes `true` which is correct for the hours-only public API.
- **T-F3 / T-F4** — checkpoint `first_active_lsn` + `update_first_lsn` wiring;
  prerequisites for the deferred P-2 recovery-scan optimization, which has no
  production consumer yet (`get_first_active_lsn()` is unused; the checkpointer
  hardcodes `first_active_lsn = 0`). Land with P-2.

### Superseded notes

The per-finding status tables above are authoritative. Earlier
"deferred with rationale" notes for St-H4 / St-H5 / R-F01 are superseded —
those are now **fixed** (see the list above).

**Remaining dedicated efforts** (larger; each its own change):

- **St-C3 / St-H1 / St-H3** — file-header checksum + on-disk endianness; an
  on-disk-format-version decision (do not rush at scale).
- **T-F2** — SERIALIZABLE range locks — **FIXED** (fix/tf2-range-locks; next-key
  locking; phantom-prevention tests pass; isolation docs restored).
- **C-C2** — `become_master` feeder/log-streaming threads — **FULLY FIXED**
  (v3.2.0 push threads + v3.3.0 WAL-scanner auto-feed, branch
  `fix/cc2b-wal-vlsn-autofeed`): real `EnvironmentImpl` commits write
  VLSN-tagged WAL entries; `EnvironmentLogScanner` auto-feeds them to
  replicas; end-to-end convergence test passes. C-C2b qualification gap
  closed.

## Critical

| ID | Area | Issue | Status |
|----|------|-------|--------|
| S-C1 | noxu-log durability | `flush_no_sync` advanced the same `last_flush_lsn` that `flush_sync_if_needed` uses to skip fsyncs → a SYNC commit after a WRITE_NO_SYNC write (or the no-sync flush daemon) could skip its `fdatasync` and be lost on power failure | **FIXED** (separate `last_synced_lsn` durable watermark + regression test) |
| T-F1 | noxu-recovery | Undo pass applied before-images with **no `logLsn == slotLsn` currency check** (the code comment falsely claimed the check was "delegated to the tree layer"). Theoretically an aborted txn's before-image could overwrite a later committed write of the same key during recovery. | **MITIGATED** — the JE currency check is now enforced in `run_undo`/`run_undo_all`; the false comment is corrected. The specific interleaving could **not** be reproduced as a live failure on main (masked by runtime-abort reversion + redo-only-committed + the no-active-txns fast path), so this is defensive alignment with JE, not a demonstrated live-corruption fix. |
| T-F2 | noxu-dbi / docs / noxu-txn | `cursor_impl::lock_ln` now acquires `LockType::RangeRead` for SERIALIZABLE; new-key inserts acquire `RangeInsert` on successor; EOF sentinel protection added. `WaitRestart` path fixed to return `RangeRestart`. All phantom tests pass. | **FIXED** (fix/tf2-range-locks) |
| R-F01 | noxu-log | `LogBufferSegment` stored raw pointers into inline `LogBuffer` fields → moving the buffer dangled them (UB) | **FIXED** (latch + pin-count moved into an `Arc<LogBufferControl>` shared with each segment; only the heap-backed `data_ptr` remains, which survives moves; move-safety regression test) |
| R-F03 | noxu-log / noxu-dbi | `mmap_file` SAFETY claims it is only used on complete files during recovery; the disk-ordered cursor maps the **current write file** during live operation, violating `memmap2`'s no-concurrent-modification contract (UB) | **FIXED** (`mmap_file` refuses the current write file; the scanner falls back to `pread`) |
| St-C3 | noxu-log format | The 32-byte file header was written with **no checksum**; a torn write of the header was undetectable | **FIXED** (LOG_VERSION 2→3: v3 header carries a CRC32; version-aware first-entry offset via `on_disk_size`; v2 files still readable; `HeaderChecksumMismatch` on corrupt v3 header) |

## High

| ID | Area | Issue | Status |
|----|------|-------|--------|
| R-F04 | noxu-xa | `get_transaction` returned `&Transaction` after dropping the `Mutex` guard; a concurrent `xa_rollback` frees the `Box` → use-after-free. | **FIXED** (returns `Arc<Transaction>`; the handle keeps the txn alive independently of the map; `unsafe` removed and noxu-xa now carries `#![forbid(unsafe_code)]`) |
| R-F05 | noxu-latch | `thread_id()` lacked `\| 1`; a thread whose `DefaultHasher` output is 0 collides with the "unowned" sentinel and false-panics "latch already held" on first acquire | **FIXED** |
| T-F3 | noxu-recovery | `CkptEnd.first_active_lsn` is hard-coded to `Lsn::new(0,0)` → recovery always scans the whole log from file 0 (O(total log), not O(checkpoint interval)). Correct but unbounded. Depends on T-F4. | OPEN (documented; see `wave-gb-dbtree-recovery.md`) |
| T-F4 | noxu-txn/dbi | `TxnManager::update_first_lsn` is never called from the cursor layer → `get_first_active_lsn()` always returns NULL_LSN (the documented contract is unimplemented) | OPEN |
| T-F5 | noxu-txn/db | `TxnManager::commit_txn`/`abort_txn`/`unregister_*` were never called for **explicit** transactions → `all_txns` + locker-label maps grew unbounded; `n_active_txns()`/`n_commits`/`n_aborts` stats wrong | **FIXED** (`Transaction::unregister_inner_txn` called on commit/abort + both XA resolved paths; regression test `f5_explicit_txns_unregister_from_txn_manager`) |
| St-H1 | noxu-log format | File header `byte_order = 0x00` claims big-endian, but log entry headers are little-endian — an external reader following the documented contract misparses | OPEN |
| St-H2 | noxu-evictor | `real_node_size()` walks the entire tree per eviction decision (O(n) per node, O(n·batch) per batch) — no per-node in-memory-size tracking | OPEN |
| St-H3 | noxu-log format | Entry headers little-endian, entry payloads (BINDelta, BinStub) big-endian — mixed on-disk endianness, undocumented | OPEN |
| St-H4 | noxu-tree | Upper-IN descent used an O(n) linear scan instead of binary search | **FIXED** (unified `Tree::upper_in_floor_index` binary floor-search applied to all 8 descent sites; also fixed `search_with_coupling` ignoring a custom comparator; property test vs linear scan) |
| St-H5 | noxu-tree | `TreeNode::find_entry` returned the insertion point, not the floor, for Internal nodes (non-exact) | **FIXED** (returns `(idx-1).max(0)` floor, consistent with `upper_in_floor_index` + JE; test `test_find_entry_internal_nonexact_returns_floor`) |
| St-H6 | noxu-tree | `Tree::split_child` hardcoded `expiration_in_hours: false` on the right-half sibling BIN → hours-granularity TTL records silently expired on read (128/256 lost in benchmark); original finding (deserialization default) was latent, the split bug was live | **FIXED** (`bin_expiration_in_hours` captured before `drop(child_guard)` and inherited by sibling; three ancillary `false→true` corrections; `debug_assert!` guard; regression tests `test_ttl_records_survive_bin_split_right_sibling_256` FAIL-PRE/PASS-POST) |
| C-C2 | noxu-rep | `become_master` doc promised a `FeederRunner`/`EnvironmentLogScanner` thread per replica; the body only created in-memory tracker structs → a master did not actively feed replicas | **FULLY FIXED** (v3.2.0 + v3.3.0 branch `fix/cc2b-wal-vlsn-autofeed`): push-feeder threads via `register_feeder_channel` (v3.2.0); WAL-scanner auto-feed via `with_environment` + `log_with_vlsn` + `EnvironmentLogScanner` (v3.3.0, C-C2b). Convergence test `test_wal_scanner_autofeed_convergence` proves end-to-end propagation with real `EnvironmentImpl` commits. Standalone format regression test confirms 14-byte headers unchanged. |
| C-H4 | noxu-rep | (stale-branch finding) `peer_allowlist` no-op — **re-validated on main: FIXED** by mTLS Phase 2/3 (`PeerAllowlistVerifier` wired through the TLS listener, dispatcher, and QUIC) | RESOLVED on main |

## Medium / Low (summary — see source reports)

- **Honest-claim corrections (FIXED in this change set):** "400+ config
  parameters" → ~165 (C-C3); "21/19 crates" → 22 (C-H2); CRC32 "15+ GiB/s"
  qualified as x86-64-only with the AArch64 software-fallback caveat (C-C1);
  README unsafe table cited a removed `noxu-db` block (C-H8); AGENTS.md
  `noxu-log` unsafe inventory 6 → 8 (R-F15); SERIALIZABLE "range locks /
  prevents phantoms" docs downgraded to repeatable-read with an explicit
  not-yet-enforced caveat (T-F8).
- **Soundness hardening (FIXED):** the `log_source.rs` transmute SAFETY comment
  now documents the load-bearing field-drop-order invariant (R-F02).
- **Concurrency (OPEN):** non-fair RwLock writer starvation undocumented in the
  public type (R-F08); `Condvar` missing the same-mutex requirement (R-F09);
  `LogBuffer::release()` callable by a non-owner (R-F06); `reinit()` clobbers
  pin count without an assert (R-F07); `write_buffer` file-size TOCTOU (R-F10).
- **Recovery/locking (OPEN):** `retains_locks_on_commit()` dead and
  inconsistent with commit behavior (T-F6); deadlock re-check skipped on
  spurious wakeup in `lock_with_sharing_and_timeout` (T-F7); buffer-hit
  `read_entry` skips CRC (St-M1); `log_internal` never writes VLSN into the
  header (St-M2); `BinStub::apply_delta` dead-code corruption trap (St-M3 /
  St-C2) — **docstring corrected** to remove the misleading `reconstituteBIN`
  claim.
- **Engine/verify (OPEN):** `Engine::close()` skips EnvironmentImpl close
  (M-1); `verify_environment`/`verify_database` are stubs returning
  `passed: true` (M-2).
- **Config params accepted-and-ignored (OPEN):** 7 `EnvironmentConfig` fields
  (`env_latch_timeout_ms`, `env_expiration_enabled`, `env_db_eviction`,
  `env_fair_latches`, `env_check_leaks`, `env_forced_yield`,
  `env_ttl_clock_tolerance_ms`) have behavioral docs but no production reads
  (H-3) — Wave ZA added a registry + warn-at-open but the docs still read as
  if functional.
- **Feature/test gaps (OPEN):** `JoinCursor` only test is `#[ignore]`d (M-5);
  sorted-dup `SecondaryCursor` W13 correctness bugs documented as present
  (M-6); `noxu-observe` unpublished so the `observability` feature fails to
  resolve for crates.io users, undocumented in known-limitations (M-7);
  `Get::SearchLte`/`FirstDup`/`LastDup` return `Unsupported` with no
  `#[deprecated]` (L-4).
- **Stale docs/comments (partly FIXED):** capability matrix says "v2.2
  (current)" (H-1); `known-limitations.md` says HA stubs "not functional in
  v1.3.0" (H-6); spec stamp claim "all eleven" but 3 unstamped (H-7);
  `record_active_txn` TODO describes an already-fixed bug (T-F9 / R-F11);
  `TODO(bug)` headers on fixed cursor tests (R-F16); integer-cast/overflow
  papercuts in `noxu-sync`/`noxu-log` (R-F12/13/14).

## Recommended release gating order

1. **T-F1** (recovery currency check) — **mitigated** (JE currency check now
   enforced; could not be reproduced as a live failure — defensive). Verify the
   analysis above (runtime-abort reversion + redo-only-committed) is complete.
2. **R-F01, R-F03** (noxu-log `unsafe` soundness) — R-F03 **fixed**; R-F01
   (`LogBufferSegment` move-safety) remains.
3. **R-F04** (XA use-after-free) — **fixed** (Arc<Transaction>).
4. **T-F2** (range locks) — **FIXED** in fix/tf2-range-locks; SERIALIZABLE
   now prevents phantoms via next-key locking; docs restored.
5. **St-C3 / St-H1 / St-H3** (on-disk format: header checksum + endianness) —
   format-version decision; fix before committing to on-disk stability.
6. **T-F4 / T-F5** (txn manager wiring) — correctness of stats, eviction
   signal, and a prerequisite for T-F3 (bounded recovery).
7. The remaining High/Medium items and the config-param honesty pass.

Until items 1–4 are closed and qualified, Noxu should be described as
pre-production / preview, not as a production-ready release.
