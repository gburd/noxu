# Audit synthesis — 2026-05-29

Four parallel persona-based code reviews of Noxu DB at `v2.4.1` (post-Wave 11-M merge):

- [JE-team audit (Sleepycat)](audit-2026-05-je-team.md) — 9 critical, 11 high, 10 medium, 6 low, 2 informational (38 total).
- [Margo audit (algorithms + docs)](audit-2026-05-margo.md) — 1 critical, 4 high, 15 medium, 7 low (27 total).
- [Keith audit (perf + correctness)](audit-2026-05-keith.md) — 3 critical, 12 high, 14 medium, 8 low, 1 informational (38 total).
- [Jonhoo audit (Rust idiom + soundness)](audit-2026-05-jonhoo.md) — 6 critical-equiv, 12 high-equiv, ~30 medium / nitpick.

This document distills those four reports into a single prioritised task list.
Severity assignments here are **the maximum of the four reviewers** — i.e. if
anyone called it critical, it's critical. Cross-confirmation across reviewers
raises confidence; a single-reviewer finding is still listed but flagged as
such.

## Cross-confirmed critical bugs (high confidence)

These are findings that show up in two or more audit reports. They are the
priority-1 fixes for v3.0.0.

### C-1. Parent directory not fsynced after new log file creation

**Reviewers**: JE-team (1-G / 4-A), Keith (F-3.1 / F-8.1).
**Severity**: Critical.
**File**: `crates/noxu-log/src/file_manager.rs:416` (`create_file_internal`).
**Bug**: `file.sync_all()` is called on the new file but the parent directory
is never `sync_all()`'d. POSIX requires the parent directory to be fsync'd
after a `creat`/`rename` for the directory entry itself to be durable; without
it, a power loss between file creation and the next directory write loses the
file from the directory entirely, taking all data written to it with it.
**Fix**: ~3 lines. Open the parent dir; `dir.sync_all()` after the file is
created and its first write has been fsync'd.
**Test**: a power-loss test that creates a log file, writes records, kills
power before the next file rotation, and then asserts the file is present in
the directory after recovery.

### C-2. fsync failure does not invalidate the environment

**Reviewers**: Keith (F-3.2 / F-8.4 / F-9.4).
**Severity**: Critical.
**File**: `crates/noxu-log/src/fsync_manager.rs:113`,
`crates/noxu-log/src/file_manager.rs:437`,
`crates/noxu-log/src/file_manager.rs:663` (`sync_log_end`).
**Bug**: After `EIO` from `fdatasync`, the database continues accepting
writes on what may be a permanently unflushable page-cache. This is the
"fsyncgate" class of vulnerability — once a kernel fsync returns an error
once, subsequent fsyncs may not retry the same dirty pages.
**Fix**: on every I/O error in `sync_data` / `sync_all`, call
`env_impl.invalidate(LogWrite)` and refuse all further commits. Match JE's
`EnvironmentFailureException` propagation.
**Test**: a fault-injection test using `fail::cfg("fsync", "return")` style
that asserts the env transitions to invalid and rejects subsequent commits.

### C-3. CRC32 validation skipped in the recovery scanner

**Reviewers**: Keith (F-3.5 / F-9.1).
**Severity**: Critical.
**File**: `crates/noxu-dbi/src/file_manager_scanner.rs::parse_entry_from_bytes`.
**Bug**: The recovery code path's log-entry parser does not verify the entry's
CRC32 checksum. A bit-flip in a log file from a bad sector silently injects
garbage into the recovered B-tree — recovery's last-line-of-defence is
non-existent. The non-recovery reader (`file_reader.rs`) does verify CRCs;
this is an asymmetric gap.
**Fix**: compute and verify CRC32 in `parse_entry_from_bytes` before
returning a parsed entry. Mismatches should return `Err(LogCorrupted)` and
trigger env invalidation.
**Test**: a test that writes a log entry, corrupts a single byte in its
payload, attempts recovery, and asserts the env reports corruption (currently
this test would pass because recovery would silently load the corrupted
data).

### C-4. `open_database` ignores transaction parameter

**Reviewers**: JE-team (1-I / 1-J).
**Severity**: Critical.
**File**: `crates/noxu-dbi/src/database_impl.rs::open_database` (and callers).
**Bug**: The `txn: Option<&Transaction>` parameter on `EnvironmentImpl::
open_database` is `_txn` — silently ignored. Database creation is therefore
non-transactional and non-rollbackable, even if the caller passed a txn that
later aborts. `get_database_names()` returns databases created in still-
uncommitted transactions, violating JE's committed-only semantics.
**Note**: the v2.3.2 bug-fix wave added WAL persistence of the name
registry (closing one symptom: registry lost across reopen) but did *not*
make the txn-parameter actually rollbackable. This is the deeper bug.
**Fix**: implement NameLN locking under the provided locker, write the
NameLN as part of the txn's redo set, and undo it on abort.
**Test**: open a database inside an explicit txn, abort, reopen the env,
assert the database does not exist. Currently this fails silently
(database persists despite abort).

### C-5. BIN delta logging missing JE guard clauses

**Reviewers**: JE-team (1-A).
**Severity**: Critical.
**File**: `crates/noxu-tree/src/tree.rs::BIN::should_log_delta`.
**Bug**: JE's `BIN.shouldLogDelta()` checks three guard predicates Noxu
omits: `prohibitNextDelta` flag (set by `compress()`), `lastFullLsn ==
NULL_LSN` (no full BIN logged yet, can't reference one), and DeferredWrite
mode. Noxu's omission means a BIN that was just compressed (so the slot
indices have shifted) can have its next checkpoint write a delta that
references stale slot positions, producing an unrecoverable BIN on
recovery.
**Fix**: add the three predicates. Each is a one-line guard.
**Test**: compress a BIN, immediately checkpoint, verify the checkpoint
writes a full BIN (not a delta).

### C-6. Recovery missing dedicated MapLN two-pass

**Reviewers**: JE-team (1-C).
**Severity**: Critical.
**File**: `crates/noxu-recovery/src/recovery_manager.rs`.
**Bug**: JE's recovery runs a separate undo+redo pass over the mapping tree
(NameLNs and MapLNs that locate per-database root INs) before replaying the
main data LNs. Noxu collapses both passes into one. This directly correlates
with the v2.3.2 bug-fix wave's "non-transactional db registration is lost
across reopen" bug — that was a partial fix; the underlying ordering issue
remains for any failure where the mapping tree itself needs undo.
**Fix**: split recovery into mapping-tree-pass-then-data-pass. Add a
mapping-tree-only undo phase between analysis and main redo.
**Test**: a recovery test where a NameLN's owning txn aborted mid-checkpoint,
and the mapping-tree must be undone before main redo can find the right
database root.

### C-7. `Ordering::Relaxed` on log-buffer pin-count decrement

**Reviewers**: Jonhoo (4.4).
**Severity**: Critical.
**File**: `crates/noxu-log/src/log_buffer.rs::LogBufferSegment::release` (or
similar — Jonhoo cites the `pin_count.fetch_sub` site).
**Bug**: The pin-count decrement uses `Ordering::Relaxed`. Under the C++/Rust
memory model, the readers' view of "buffer is now free" can be reordered
before the readers' view of "I'm done writing into the buffer", causing data
written after the relax-ordered fetch_sub to be lost. The reader's zero-check
needs `Acquire`; the decrement needs `Release`.
**Fix**: change the `fetch_sub` to `Ordering::Release` and the corresponding
load to `Ordering::Acquire`. Run `cargo +nightly miri test -p noxu-log` to
verify.
**Test**: existing tests likely don't exercise this race; a stress test with
many concurrent buffer-pin/release pairs would be needed to demonstrate
correctness post-fix.

## Single-reviewer critical findings (medium confidence — verify first)

### C-8. Tests with documented live bugs are not `#[ignore]`'d

**Reviewers**: JE-team (2-B / 2-C / 7-C).
**Severity**: Critical (if confirmed).
**Files**: search for SR9465 / SR9752 in `crates/noxu-db/tests/`.
**Claim**: Three tests documented in the JE TCK port enumeration TSV as
surfacing live bugs ("aborted delete+reinsert corrupts BIN", "aborted dup
inserts persist") currently run in CI as `#[test]` without `#[ignore]`.
Either (a) the bugs were silently fixed and the TSV is stale, or (b) the
tests are silently passing because they don't actually exercise the bug
(false positives in CI).
**Fix**: read each test, verify it actually exercises the documented bug. If
the bug is live, `#[ignore]` it with a fresh TODO. If the bug is silently
fixed, update the TSV. If the test is tautological, fix it to actually
test the invariant.

### C-9. `unsafe` claim in AGENTS.md is a lie (`std::mem::transmute` in

`noxu-log` not inventoried)

**Reviewers**: Jonhoo (4.2).
**Severity**: Critical (truthfulness).
**File**: `crates/noxu-log/src/...` (Jonhoo cites the location).
**Bug**: AGENTS.md claims `noxu-log` has `unsafe` for "memory-mapped I/O" —
but a `std::mem::transmute` exists in noxu-log that is not part of the
mmap API and is not documented anywhere. Either the transmute is
unsafely-soundness-broken (in which case it must be fixed) or it's
sound (in which case the AGENTS.md unsafe inventory must mention it).
**Fix**: locate every `unsafe` in noxu-log, audit each for soundness, and
update AGENTS.md to accurately describe what's there.

## Cross-confirmed high findings (priority-2)

### H-1. EnvironmentImpl lock held across abort undo loop

**Reviewers**: Keith (F-2.2).
**Severity**: High.
**File**: `crates/noxu-db/src/transaction.rs::abort` (and inner_txn::abort).
**Bug**: A long-running transaction abort applies undo records to the B-tree
while holding the EnvironmentImpl-level lock, blocking all other threads
(including readers) for the entire abort duration. O(records-in-txn) latency
spike visible to all concurrent users.
**Fix**: drop the EnvironmentImpl lock around the per-record undo
application. Acquire it only at txn-state transitions.

### H-2. Lock manager: waiter-graph mutex acquired while shard mutex held

**Reviewers**: Keith (F-6.2).
**Severity**: High.
**File**: `crates/noxu-txn/src/lock_manager.rs:339`.
**Bug**: Lock-ordering inversion. The `waiter_graph` mutex is acquired
while a per-shard mutex is already held. A dual-thread sequence can deadlock
the lock manager itself — process hangs with no recovery path.
**Fix**: define a global lock ordering (e.g. waiter_graph always before
shards). Document and enforce.

### H-3. Per-log-entry allocation pressure

**Reviewers**: Keith (F-1.1, F-1.2), JE-team (loosely 6).
**Severity**: High.
**File**: `crates/noxu-log/src/log_manager.rs:261, 542`.
**Bug**: `vec![0u8; entry_size]` allocated per log-write call;
`Vec<(Vec<u8>, u64)>` allocated per `collect_dirty_buffers()` call.
Together account for the majority of allocator pressure visible in all four
Wave 11-H profiles.
**Fix**: per-thread scratch `Vec<u8>` reused across calls; per-thread
`Vec<(Bytes, u64)>` for buffer collection.

### H-4. Deadlock victim selection always passes `HashMap::new()` for

`lock_counts`

**Reviewers**: Margo (1.4 / 5.6), Keith (F-4.4).
**Severity**: High.
**File**: `crates/noxu-txn/src/lock_manager.rs:986`
(`check_deadlock_for_waiter` → `select_victim`).
**Bug**: The documented primary victim-selection criterion ("fewest locks
held") is dead code. The effective criterion is always "youngest locker",
which preferentially aborts large transactions in high-write workloads.
**Fix**: actually populate the `lock_counts` HashMap before calling
`select_victim`. Compute it once during the cycle-detection DFS (it's free
since DFS visits every locker anyway).

### H-5. Comment-vs-code drift: waiter graph direction

**Reviewers**: Margo (1.5 / 5.1).
**Severity**: High (documentation accuracy).
**File**: `docs/src/maintainer/algorithms.md:65`.
**Bug**: The doc says `waiter_graph` maps "blocker → [waiters]" but the code
maps "waiter → [owner_ids it is blocked by]". Direction is opposite.
**Fix**: update the doc to match the code.

### H-6. On-disk format hex codes wrong in docs

**Reviewers**: Margo (3.1).
**Severity**: Critical (per Margo) — but documentation-only, so demoted to
high here.
**File**: `docs/src/reference/on-disk-format.md`.
**Bug**: Every hex code in the entry-type table is wrong (e.g. doc says BIN
= 0x10, code says BIN = 3). Anyone trying to write a fsck-like tool from
this doc would produce garbage.
**Fix**: regenerate the table from `crates/noxu-log/src/entry_type.rs`. One
script run; permanent fix.

### H-7. On-disk format endianness wrong in docs

**Reviewers**: Margo (3.2).
**Severity**: High (documentation accuracy).
**File**: `docs/src/reference/on-disk-format.md:56-57`.
**Bug**: The doc says "most payload fields use little-endian" but BIN and
IN payloads use **big-endian** (`to_be_bytes()`, `BytesMut::put_u64()`).
Only entry headers are LE.
**Fix**: rewrite the endianness section with payload-by-payload accuracy.

### H-8. README example references non-existent methods

**Reviewers**: Jonhoo (1.4).
**Severity**: High (UX).
**File**: `README.md` lines 63 and 80.
**Bug**: The Quick Start example references a 4-arg `db.get` and a
`cursor.get_next()` that don't exist in the current API. First impression
is broken code.
**Fix**: update the example to compile against the v2.4.1 API. Convert
`lib.rs` doc-tests from `ignore` to `no_run` so they compile every CI run.

### H-9. `PartialEvict` does not actually evict

**Reviewers**: Margo (5.7).
**Severity**: High (correctness — eviction stats lie about reality).
**File**: `crates/noxu-evictor/src/...` (the PartialEvict path).
**Bug**: The evictor increments `nodes_stripped` / `lns_evicted` stats and
credits `node_size_fn(id)` bytes to the memory budget, but never sets
`data = None` on any `BinEntry`. The heap is not actually reclaimed; the
memory budget tracker drifts below reality, causing the evictor to
under-fire under pressure (it thinks it's evicted memory that's still
held).
**Fix**: actually clear the slot data field. Two lines in the
`PartialEvict::apply` path.

### H-10. Three unnecessary `unsafe impl Send + Sync` in `noxu-rep`

**Reviewers**: Jonhoo (4.1).
**Severity**: High (soundness).
**Files**: `crates/noxu-rep/src/elections/election.rs:302`,
`crates/noxu-rep/src/elections/master_tracker.rs:163`,
`crates/noxu-rep/src/elections/phi_detector.rs:212`.
**Bug**: Each of these types has fields that are already `Send + Sync` and
should auto-derive. Manually-asserted `unsafe impl Send + Sync` is a hand-
maintained invariant that the compiler would otherwise check for free —
removing the `unsafe impl` lets the compiler do its job.
**Fix**: delete the three `unsafe impl` blocks; verify `cargo check` still
passes (if it doesn't, there's a real soundness bug to find).

## High-priority cleanup tasks (priority-3)

### Q-1. Cursor doesn't implement `Iterator` (UX-critical)

**Reviewers**: Jonhoo (2.1).
**Severity**: High (UX).
**Bug**: A Rust user expects `for (k, v) in db.iter(&txn)? { ... }`. Noxu's
Cursor is a stateful object with `next()`, `prev()`, `get_search_key()`,
etc., and does not implement `Iterator`.
**Fix**: add an `IterCursor<'a>: Iterator<Item = Result<(Bytes, Bytes), …>>`
adapter; expose `Database::iter()` and `Database::range(...)`. Keep the
stateful Cursor for advanced uses.

### Q-2. Tests with bare `#[ignore]` (no reason string)

**Reviewers**: JE-team (7-A).
**Severity**: Low individually; medium collectively.
**Files**: `isolation_test.rs:630, 725, 837`, `sustained_load_test.rs:86, 200`,
`torture_test.rs:1133`, `xa_chaos_test.rs:795, 904`.
**Fix**: add `#[ignore = "..."]` reason strings, or gate behind a
`slow-tests` cargo feature so they can be run in nightly CI.

### Q-3. JE features Noxu silently lacks

**Reviewers**: JE-team (3-A through 3-G).
**Severity**: Medium (each individually); high collectively (capability
matrix lies).
**Missing**: `Environment::compress()` (explicit BIN compression),
`Environment::evict_memory()` (explicit eviction),
`Environment::get_lock_stats()` / `get_transaction_stats()`,
`Get::SearchLte` / `FirstDup` / `LastDup`,
`Environment::verify()` with `VerifyConfig`,
`LogFlushTask` (periodic background log flush),
`Database::truncate()` return value (record count).
**Fix**: either implement (preferred for `verify`, `evict_memory`,
`compress` since they enable JE-port test fidelity) or document the
divergence in `docs/src/getting-started/migrating.md`.

### Q-4. Recovery test ports don't actually exercise recovery

**Reviewers**: JE-team (2-D, 2-E, 2-F).
**Severity**: High (test fidelity).
**Bug**: Many JE recovery test ports omit the explicit checkpoint /
INCompressorQueue drain / `env.compress()` calls that JE uses to set up
the pre-crash state. The Noxu ports therefore test "recovery from a clean
shutdown that just happened to be called crash recovery", not actual
recovery from a forced state.
**Fix**: audit the 49 Wave 11-G port and the dozen-or-so other recovery
ports; add the missing setup calls. Mark each with a comment citing the JE
version's setup.

### Q-5. Add `#![forbid(unsafe_code)]` to 12 zero-unsafe crates

**Reviewers**: Jonhoo (4.5).
**Severity**: Low (defense in depth).
**Files**: `lib.rs` of `noxu-tree`, `noxu-txn`, `noxu-evictor`,
`noxu-cleaner`, `noxu-recovery`, `noxu-dbi`, `noxu-engine`, `noxu-bind`,
`noxu-collections`, `noxu-persist`, `noxu-config`, `noxu-util`.
**Fix**: 1 line per file; makes the zero-unsafe claim machine-enforced.

### Q-6. AGENTS.md unsafe inventory accuracy

**Reviewers**: Jonhoo (4.2 — see C-9).
**Fix**: walk every `unsafe` block in the workspace; confirm it matches
the AGENTS.md inventory; update either the code or AGENTS.md as needed.

### Q-7. Comment drift cleanup

**Reviewers**: Margo (Section 5).
**Files**: multiple — see Margo's audit for the full list of 50+
comment-vs-code drifts.
**Fix**: per-file mechanical updates.

## Wave plan

The findings above split naturally into three follow-up waves:

### Wave 11-Q (correctness fixes — non-breaking) — target v2.4.2

C-1, C-2, C-3, C-7, H-2, H-3, H-4, H-9 (the priority-1 correctness fixes
that don't break public API). C-7 is the most-bug-like; C-1 is the
easiest. Plus C-9 (AGENTS.md unsafe inventory accuracy) and Q-5
(`#![forbid(unsafe_code)]`).

### Wave 11-R (semantic correctness — breaking) — target v3.0.0

C-4 (open_database txn semantics), C-5 (BIN delta guard clauses), C-6
(MapLN two-pass), C-8 (live-bug tests), Q-3 (missing JE features), Q-4
(recovery test fidelity).

### Wave 11-S (UX + cleanup — non-breaking) — target v3.0.0

H-1, H-5, H-6, H-7, H-8, H-10, Q-1, Q-2, Q-6, Q-7. Documentation accuracy
fixes and the Cursor-implements-Iterator UX win.

After 11-Q, 11-R, 11-S all merge: **tag v3.0.0** and execute the
crates.io publish runbook from `docs/src/contributing/publishing.md`.

## Cross-cutting commitments

Every fix in these waves must:

- Add a regression test that would have caught the bug (especially C-1, C-2,
  C-3, C-7 — these have crash-safety / concurrency components that demand
  fault-injection or stress tests).
- Update the relevant documentation (Margo's drift findings will be
  incorporated as we touch each subsystem).
- Update `CHANGELOG.md` `[Unreleased]` section with the fix and a citation
  to the audit finding.
- Run `cargo +nightly miri test` for any concurrency-related fix (C-7
  especially).

## Confidence calibration

- Findings cross-confirmed by ≥2 reviewers: very high confidence. Address
  in 11-Q immediately.
- Single-reviewer findings: medium confidence. Verify before addressing
  (C-8, C-9 explicitly).
- Single-reviewer ergonomics / Rust-idiom findings (most of Jonhoo's
  Section 1, 2, 3): subjective. Address per a separate API review.
