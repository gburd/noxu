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

## Critical

| ID | Area | Issue | Status |
|----|------|-------|--------|
| S-C1 | noxu-log durability | `flush_no_sync` advanced the same `last_flush_lsn` that `flush_sync_if_needed` uses to skip fsyncs → a SYNC commit after a WRITE_NO_SYNC write (or the no-sync flush daemon) could skip its `fdatasync` and be lost on power failure | **FIXED** (separate `last_synced_lsn` durable watermark + regression test) |
| T-F1 | noxu-recovery | Undo pass applied before-images with **no `logLsn == slotLsn` currency check** (the code comment falsely claimed the check was "delegated to the tree layer"). Theoretically an aborted txn's before-image could overwrite a later committed write of the same key during recovery. | **MITIGATED** — the JE currency check is now enforced in `run_undo`/`run_undo_all`; the false comment is corrected. The specific interleaving could **not** be reproduced as a live failure on main (masked by runtime-abort reversion + redo-only-committed + the no-active-txns fast path), so this is defensive alignment with JE, not a demonstrated live-corruption fix. |
| T-F2 | noxu-dbi / docs | `cursor_impl::lock_ln` always acquires `LockType::Read`, never `RangeRead`; the range-lock conflict matrix is dead code at the operational level. SERIALIZABLE does not prevent phantoms despite being documented to. | **OPEN** (docs corrected to stop claiming phantom prevention; code fix pending) |
| R-F01 | noxu-log | `LogBufferSegment` stores raw pointers into inline `LogBuffer` fields; `unsafe impl Send` + no `Pin`/`!Unpin` → moving a stack `LogBuffer` after `allocate()` dangles the pointers (UB) | **OPEN** |
| R-F03 | noxu-log / noxu-dbi | `mmap_file` SAFETY claims it is only used on complete files during recovery; the disk-ordered cursor maps the **current write file** during live operation, violating `memmap2`'s no-concurrent-modification contract (UB) | **OPEN** |
| St-C3 | noxu-log format | The 32-byte file header is written with **no checksum**; a torn write of the header is undetectable (JE wraps it as an Adler32-protected log entry) | **OPEN** |

## High

| ID | Area | Issue | Status |
|----|------|-------|--------|
| R-F04 | noxu-xa | `get_transaction` returns `&Transaction` after dropping the `Mutex` guard; a concurrent `xa_rollback` frees the `Box` → use-after-free. Invariant not enforced by the type system. | OPEN (fix: RAII guard wrapper) |
| R-F05 | noxu-latch | `thread_id()` lacked `\| 1`; a thread whose `DefaultHasher` output is 0 collides with the "unowned" sentinel and false-panics "latch already held" on first acquire | **FIXED** |
| T-F3 | noxu-recovery | `CkptEnd.first_active_lsn` is hard-coded to `Lsn::new(0,0)` → recovery always scans the whole log from file 0 (O(total log), not O(checkpoint interval)). Correct but unbounded. Depends on T-F4. | OPEN (documented; see `wave-gb-dbtree-recovery.md`) |
| T-F4 | noxu-txn/dbi | `TxnManager::update_first_lsn` is never called from the cursor layer → `get_first_active_lsn()` always returns NULL_LSN (the documented contract is unimplemented) | OPEN |
| T-F5 | noxu-txn/db | `TxnManager::commit_txn`/`abort_txn`/`unregister_*` are never called for **explicit** transactions → `all_txns` + locker-label maps grow unbounded; `n_active_txns()` stat and the evictor's serializable signal are wrong | OPEN |
| St-H1 | noxu-log format | File header `byte_order = 0x00` claims big-endian, but log entry headers are little-endian — an external reader following the documented contract misparses | OPEN |
| St-H2 | noxu-evictor | `real_node_size()` walks the entire tree per eviction decision (O(n) per node, O(n·batch) per batch) — no per-node in-memory-size tracking | OPEN |
| St-H3 | noxu-log format | Entry headers little-endian, entry payloads (BINDelta, BinStub) big-endian — mixed on-disk endianness, undocumented | OPEN |
| St-H4 | noxu-tree | Upper-IN descent uses an O(n) linear scan instead of binary search (JE uses binary search) | OPEN |
| St-H5 | noxu-tree | `TreeNode::find_entry` returns the insertion point, not the floor, for Internal nodes (non-exact) — wrong child routing if ever wired to descent (currently latent) | OPEN |
| St-H6 | noxu-tree | `BinStub::deserialize_full` hardcodes `expiration_in_hours = true` regardless of what was logged → TTL read back 3600× wrong for seconds-granularity BINs | OPEN |
| C-C2 | noxu-rep | `become_master` doc promises a `FeederRunner`/`EnvironmentLogScanner` thread per replica; the body only creates in-memory tracker structs → a master does not actively feed replicas | OPEN |
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
2. **R-F01, R-F03** (noxu-log `unsafe` soundness) — UB under documented use.
3. **R-F04** (XA use-after-free) — RAII guard.
4. **T-F2** (range locks) — either implement, or keep the corrected docs and
   drop SERIALIZABLE from the advertised guarantees until implemented.
5. **St-C3 / St-H1 / St-H3** (on-disk format: header checksum + endianness) —
   format-version decision; fix before committing to on-disk stability.
6. **T-F4 / T-F5** (txn manager wiring) — correctness of stats, eviction
   signal, and a prerequisite for T-F3 (bounded recovery).
7. The remaining High/Medium items and the config-param honesty pass.

Until items 1–4 are closed and qualified, Noxu should be described as
pre-production / preview, not as a production-ready release.
