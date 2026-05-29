# Noxu DB Algorithmic and Documentation Audit

**Reviewer**: Margo Seltzer (channelled)  
**Date**: 2026-05-29  
**Branch under review**: `fix/wave11-l-api-stability`  
**Repository**: `/home/gburd/ws/noxu`

---

## Preamble

I've spent the better part of a day working through the code, the docs, and the
specs. The overall picture is of a project written by people who understand
database internals — the algorithms are largely correct, the latch discipline
is thoughtfully documented, and the Stateright model-checked specs are a
genuinely good practice. That said, the *documentation* has drifted badly in
several places, and there are a handful of real algorithmic concerns that need
attention before this can be cited as a reference implementation.

I'm filing findings as I would in a systems-paper review: what you wrote vs
what you built, and why the gap matters.

---

## Section 1 — Algorithm Correctness

### 1.1 B-tree Split — `split_child`

| Attribute | Value |
|---|---|
| **Severity** | Medium |
| **Subsystem** | noxu-tree |
| **File:line** | `crates/noxu-tree/src/tree.rs:2290–2504` |

**What the comment claims** (`tree.rs:2293–2302`):

```
1. splitIndex = child.nEntries / 2
   (idKeyIndex determines which half keeps the identifier key)
2. Create newSibling at the same level.
3. Move entries [low..high) from child to newSibling.
4. If low == 0: replace parent slot childIndex -> newSibling,
   insert child (now right half) with its new first key.
   Else:        update parent slot childIndex -> child (left half),
   insert newSibling with newIdKey.
```

**What the code does** (`tree.rs:2380–2500`):

- `split_index = n_entries / 2` always
- `left_entries = all_entries.slice(0, split_index)` — always installed into the original `child_arc`
- `right_entries = all_entries.slice(split_index, n_entries)` — always placed in `new_sibling`
- Sibling is inserted at `child_index + 1` in the parent

**The drift**: Step 4's "if low == 0" branch (where the original node becomes the right half) is
documented but **never executed**. Noxu always uses the "else" path. In BDB-JE this
conditional existed because JE preserved the `idKeyIndex` in the original node to avoid
rewriting all parent pointer chains. Noxu's parent model doesn't require this, so the "else"
path is the correct and complete implementation — but the `if low == 0` arm is dead comment
that references a JE detail that has no Rust analogue. A reader trying to verify the split
against the comment will be confused and may falsely conclude the left-half/right-half
assignment is conditional.

**Verdict**: `DIVERGES (intentional, unjustified)` — the code is correct; the comment is
misleading. Delete the "if low == 0" branch from the doc comment and replace with a plain
description of the actual split.

---

### 1.2 WAL Group Commit — `fsync_manager.rs`

| Attribute | Value |
|---|---|
| **Severity** | Low (informational) |
| **Subsystem** | noxu-log |
| **File:line** | `crates/noxu-log/src/fsync_manager.rs:1–440` |

**What the comment claims** (`fsync_manager.rs:8–27`): leader/waiter two-cohort pattern
matching `FSyncManager.flushAndSync`. A leader fsyncs for all current waiters; then wakes one
member of the *next* cohort.

**What the code does**: Matches precisely, including the group-commit threshold wait via
`grpc_wait`. The `test_fsync_before_commit_invariant` property test validates the core
safety invariant.

One subtle issue: when a waiting thread receives `DoLeaderFsync` but finds
`state.work_in_progress` is already `true` on re-entry, it sets `do_work = true` but NOT
`is_leader = true`. This means it performs a solo fsync without waking the cohort it just
joined (`in_progress_group` is `None`). The embedded comment says "Ensure that an fsync
is done before returning" — this is correct, but the scenario is not described in the
module-level algorithm comment, leaving it as an undocumented edge case. A future reviewer
may treat this as a bug.

**Verdict**: `MATCHES` — algorithmically correct. Add one sentence to the module comment
describing the "DoLeaderFsync but work_in_progress race" path.

---

### 1.3 Recovery — 3-Phase vs 5-Phase Labelling

| Attribute | Value |
|---|---|
| **Severity** | Low |
| **Subsystem** | noxu-recovery |
| **File:line** | `crates/noxu-recovery/src/recovery_manager.rs:1–30` |

**What the module comment claims**:
> "Performs 3-phase recovery when an Environment is opened: Phase 1 — Analysis, Phase 2 — Redo, Phase 3 — Undo"

**What `recover()` says** (`recovery_manager.rs:303`):
> "orchestrating all five sub-phases"

**What the code does**: Five distinct phases: FindEndOfLog, FindLastCheckpoint, Analysis
(BuildTree), ReplayLNs, UndoLNs. The "3-phase" label matches the ARIES/JE tradition of
naming only the database-logic phases (analysis, redo, undo); find-end and find-checkpoint
are "pre-processing". But the inline comment in `recover()` contradicts the module header
by saying "five sub-phases" — the same document uses both terms.

**Verdict**: `DIVERGES (intentional)` — the "3-phase" label is an accurate ARIES reference.
Fix by removing "all five sub-phases" from the `recover()` doc comment and instead saying
"phases A and B (find end of log, find last checkpoint) precede the three ARIES phases."

---

### 1.4 Lock Manager — Deadlock Detection Victim Selection

| Attribute | Value |
|---|---|
| **Severity** | Medium |
| **Subsystem** | noxu-txn |
| **File:line (code)** | `crates/noxu-txn/src/deadlock_detector.rs:87–131` |
| **File:line (call site)** | `crates/noxu-txn/src/lock_manager.rs:619–623` |

**What `select_victim` claims** (`deadlock_detector.rs:90–99`):

```
1. Select the locker with the fewest locks held.
   A transaction with fewer locks has done less work...
2. On tie, select the youngest transaction (highest locker ID).
victim = locker with min(n_owners), ties broken by max(locker_id)
```

**What the call site does** (`lock_manager.rs:619–623`):

```rust
// Pass empty lock_counts: select_victim falls back to youngest (highest ID).
let victim = DeadlockDetector::select_victim(&cycle, &HashMap::new());
```

The `lock_manager` **always passes an empty `lock_counts` map**. The "fewest locks held"
primary selection criterion is therefore **never evaluated**. The effective behaviour is
always "youngest locker (highest locker_id)" — which is the tiebreaker, elevated to primary.

This is a ghost feature: the primary criterion exists only in the doc comment and in unit
tests that construct artificial `lock_counts`; it has no effect in production.

**Verdict**: `DIVERGES (unjustified)`. Either (a) wire in actual lock counts (the lock
manager has all the data needed) or (b) remove the primary criterion from `select_victim`'s
doc comment to match actual behaviour.

---

### 1.5 Deadlock Detection — Waiter Graph Direction

| Attribute | Value |
|---|---|
| **Severity** | High |
| **Subsystem** | noxu-txn + docs |
| **File:line (code)** | `crates/noxu-txn/src/lock_manager.rs:64–72` |
| **File:line (doc)** | `docs/src/maintainer/algorithms.md:65` |

**What `algorithms.md` says** (`algorithms.md:65`):
> "`waiter_graph: Mutex<HashMap<i64, Vec<i64>>>` maps blocker→[waiters]."

**What the code field says** (`lock_manager.rs:64–72`):

```rust
/// Incremental waits-for graph for O(1) deadlock detection.
///
/// Maps waiting_locker_id → [owner_locker_ids it is blocked by].
```

**What `record_wait` does** (`lock_manager.rs:637–640`):

```rust
fn record_wait(&self, locker_id: i64, owner_ids: &[i64]) {
    let mut graph = self.waiter_graph.lock();
    graph.insert(locker_id, owner_ids.to_vec());
}
```

The graph maps **waiter → [owners it is blocked by]** — i.e., the standard waits-for
direction. The `algorithms.md` describes the **inverse** ("blocker→[waiters]"), which is a
blocked-by graph. These are dual representations; only one is implemented.

**Verdict**: `DIVERGES (unjustified)` — the code and the field-level comment are consistent
with each other. The `algorithms.md` description is wrong. Fix `algorithms.md` line 65 to
say "maps `waiting_locker_id` → `[owner_ids it waits on]`."

---

### 1.6 Cleaner Utilization Formula

| Attribute | Value |
|---|---|
| **Severity** | Informational |
| **Subsystem** | noxu-cleaner |
| **File:line** | `crates/noxu-cleaner/src/file_selector.rs:1–30` |

**What the comment claims**: TTL-adjusted utilization formula is fully documented:
`adjustedUtil = (active_bytes - expired_bytes) / total` (0–100 integer %). File with
lowest adjusted utilization is selected.

**What the code does**: `adjusted_utilization_pct()` matches the formula exactly. The
two-pass cleaning logic (required utilization threshold escalation) is also documented inline.

**Verdict**: `MATCHES` — this is one of the better-documented algorithms in the project.

---

### 1.7 Eviction Policy Selection

| Attribute | Value |
|---|---|
| **Severity** | Low |
| **Subsystem** | noxu-evictor |
| **File:line** | `crates/noxu-evictor/src/evictor.rs:1–50, 310–380` |

**What the comments claim**: Two independent policy slots (primary / scan), plus pri2 dirty
staging. Scan pages evicted preferentially. `decide_eviction` uses a documented decision
tree matching `processTarget()`.

**What the code does**: Matches. The `PartialEvict` decision for BINs calls `node_size_fn`
to estimate freed bytes but does **not** actually strip LN data from the BIN — it returns
a byte count for memory budget accounting while the node stays in the policy list. The
comment says "LN data evicted" but no LN data is removed; only the memory counter is
adjusted. This is a semantic imprecision: BIN partial eviction in JE actually clears the
data from BIN slots; Noxu's implementation accounts for it without performing it.

**Verdict**: `DIVERGES (intentional, partially)` — the comment says "stripped" but the
`PartialEvict` path only updates the memory counter via a callback. The actual slot-clearing
(setting `data = None` in BinEntry) is absent. This means the evictor believes it freed
`node_size_fn(id)` bytes but those bytes remain in the heap. Flag for investigation.

---

### 1.8 Flexible Paxos Election

| Attribute | Value |
|---|---|
| **Severity** | Informational |
| **Subsystem** | noxu-rep |
| **File:line** | `crates/noxu-rep/src/elections/paxos.rs:1–50` |

**What the comment claims**: Two-phase Paxos, Phase 1 (Prepare/Promise), Phase 2
(Accept/Accepted). Proposer picks the best Phase-1 candidate as the Phase-2 value.

**What the Stateright spec says**: `flexible_paxos.rs` models `PersistentAcceptor`
(post-Wave-4-A) and `EphemeralAcceptor` (pre-fix, regression bait). The spec correctly
cites the implementation files.

**Verdict**: `MATCHES` — algorithm and spec are consistent.

---

### 1.9 VLSN Streaming

| Attribute | Value |
|---|---|
| **Severity** | Low |
| **Subsystem** | noxu-rep + noxu-spec |
| **File:line** | `crates/noxu-spec/src/vlsn_streaming.rs:13–15` |

**What the spec cites**: `crates/noxu-rep/src/vlsn.rs`

**What exists**: `crates/noxu-rep/src/vlsn/` (directory), `crates/noxu-rep/src/vlsn/mod.rs`

The file path cited in the spec (`vlsn.rs`) does not exist; the actual entry point is
`vlsn/mod.rs`. This is cosmetic but causes a dead reference.

**Verdict**: `DIVERGES (unjustified)` — update the spec comment.

---

## Section 2 — Invariants Stated and Tested

| Data structure | Invariant | Stated (doc)? | Tested (assert/PBT)? |
|---|---|---|---|
| BinStub | Entries are sorted by full key | ❌ Not stated | ✅ Implicitly via binary_search usage |
| BinStub | `key_prefix` is a valid common prefix of all entries | ⚠️ Stated in prose, not as `INVARIANT:` | ✅ `debug_assert!` in `compress_key` |
| BinStub | No two slots share the same full key | ❌ Not stated | ❌ No explicit test |
| BinStub | `cursor_count >= 0` | ❌ Not stated | ❌ No assertion (is `i32`, can go negative) |
| InNodeStub | Children sorted by key | ❌ Not stated | ❌ No explicit test |
| LockManager | `waiter_graph` edges are removed on grant/timeout/deadlock | ✅ Implied by `clear_wait` call sites | ⚠️ Covered by deadlock tests, not invariant tests |
| LockManager | Every waiter has at most one entry per LSN in the graph | ❌ Not stated | ❌ No assertion |
| FsyncManager | `work_in_progress` is false when no leader thread is executing | ✅ Implicit | ✅ `test_multiple_threads_one_fsync` |
| UtilizationTracker | `tracked_bytes == sum(memory_size())` | ✅ Stated informally | ✅ `prop_tracker_total_size_matches_writes` |
| RecoveryManager | `redo_entries` grows monotonically during analysis, never shrinks | ❌ Not stated | ❌ No assertion |
| RepGroup | Quorum intersection: ∀ Q1 ∈ QS1, Q2 ∈ QS2: Q1 ∩ Q2 ≠ ∅ | ✅ `flexible_paxos.rs` `QuorumIntersection` property | ✅ Stateright model |
| VlsnIndex | VLSN values are monotonically increasing | ✅ `vlsn_streaming.rs` `VlsnMonotone` property | ✅ Stateright model |

**Findings**:

1. `BinStub` has no struct-level `# Invariants` section. For a data structure this central
   to correctness, the sorted-keys and unique-keys invariants should be explicitly stated in
   the type comment and asserted in a `validate()` method called from tests.

2. `BinStub::cursor_count` is `i32` but logically should be `u32` — nothing prevents
   decrement to negative, which would cause the evictor to incorrectly skip otherwise-clean
   BINs (since the check is `cursor_count > 0`, a negative count also satisfies this and
   locks the BIN in cache).

3. The deadlock detector's waiter-graph cleanup is not asserted after each test — a missed
   `clear_wait` call could leave a phantom edge that causes false deadlock detection later.

---

## Section 3 — On-Disk Format Documentation

### 3.1 Entry Type Codes — CRITICAL Mismatch

| Attribute | Value |
|---|---|
| **Severity** | Critical |
| **Subsystem** | docs/reference |
| **File:line (doc)** | `docs/src/reference/on-disk-format.md:63–71` |
| **File:line (code)** | `crates/noxu-log/src/entry_type.rs:33–70` |

**What the doc says** (`on-disk-format.md`):

| Code | Name |
|------|------|
| 0x01 | LN |
| 0x02 | DEL_LN |
| 0x10 | BIN |
| 0x11 | BIN_DELTA |
| 0x12 | IN |
| 0x20 | COMMIT |
| 0x21 | ABORT |
| 0x30 | CHECKPOINT_START |
| 0x31 | CHECKPOINT_END |

**What the code says** (`entry_type.rs`, decimal then hex):

| Code | Name |
|------|------|
| 1 (0x01) | FileHeader |
| 2 (0x02) | IN |
| 3 (0x03) | BIN |
| 4 (0x04) | BINDelta |
| 10 (0x0A) | InsertLN |
| 30 (0x1E) | TxnCommit |
| 31 (0x1F) | TxnAbort |
| 40 (0x28) | CkptStart |
| 41 (0x29) | CkptEnd |

**The drift**: Every single entry type code in the documentation is wrong. The doc appears
to have been written with notional hex codes that were never reconciled with the actual enum
values. Someone attempting to write a hex-editor decoder or an `ndb-fsck` tool from the
docs would produce a completely non-functional parser.

The doc says "IN=0x12" but the code assigns IN=2. The doc says "COMMIT=0x20" but the code
assigns TxnCommit=30 (0x1E). There is no overlap between the documented set and the actual
set.

**Verdict**: `DIVERGES (unjustified)` — CRITICAL. The on-disk-format.md entry type table
must be regenerated from `entry_type.rs`. Additionally:

- `TxnPrepare` (32, 0x20, XA, added in v2) is absent from both reference docs.
- `RollbackStart` (62, 0x3E) and `RollbackEnd` (63, 0x3F) are absent from both docs.
- `ImmutableFile` (70, 0x46) is absent.
- `NameLN`, `NameLNTxn`, `FileSummaryLN` are absent.

---

### 3.2 Payload Endianness — Big-endian Payload vs. Little-endian Header

| Attribute | Value |
|---|---|
| **Severity** | High |
| **Subsystem** | docs/reference |
| **File:line (doc)** | `docs/src/reference/on-disk-format.md:56–57` |
| **File:line (code)** | `crates/noxu-tree/src/tree.rs:793–826`, `crates/noxu-log/src/entry/in_log_entry.rs:80–85` |

**What the doc claims** (`on-disk-format.md:56–57`):
> "All multi-byte integers in entry headers are **little-endian**. Most payload fields also use little-endian."

**What the code does**:

- Entry **headers** (`entry_header.rs`): little-endian via `byteorder::LittleEndian` ✓
- BIN/IN **payload** (`BinStub::serialize_full`, `in_log_entry.rs`): **big-endian**. Uses `.to_be_bytes()` and `BytesMut::put_u64()` (which writes big-endian).

`in_log_entry.rs:80–85`:

```rust
buf.put_u64(self.db_id);           // big-endian
buf.put_u64(self.prev_full_lsn.as_u64()); // big-endian
buf.put_u64(self.prev_delta_lsn.as_u64()); // big-endian
buf.put_u32(self.node_data.len() as u32);  // big-endian
```

`BinStub::serialize_full` (`tree.rs:800–820`):

```rust
buf.extend_from_slice(&self.node_id.to_be_bytes()); // big-endian
buf.extend_from_slice(&(self.entries.len() as u32).to_be_bytes()); // big-endian
```

The documentation claiming "most payload fields use little-endian" is incorrect. BIN and IN
payloads are big-endian. This inconsistency within the format itself (LE header, BE payload)
is worth its own design note explaining *why*, if intentional.

**Verdict**: `DIVERGES (unjustified)` — HIGH. Fix `on-disk-format.md` to state that entry
headers are little-endian but BIN/IN payloads use big-endian encoding. Consider adding a
per-entry-type payload format section.

---

### 3.3 BIN/IN Payload Format Not Documented

| Attribute | Value |
|---|---|
| **Severity** | High |
| **Subsystem** | docs/reference |
| **File:line (doc)** | `docs/src/reference/on-disk-format.md`, `docs/src/reference/log-format.md` |
| **File:line (code)** | `crates/noxu-tree/src/tree.rs:793–826, 822–843` |

Neither reference document describes the **payload** format of BIN, BIN_DELTA, or IN
entries. The header layout is documented; the payload is not. From `tree.rs:793–826`, the
actual BIN full-write format is:

```
[node_id: u64BE] [num_entries: u32BE]
  per slot: [key_len: u32BE] [key: bytes]
            [lsn: u64BE] [has_data: u8]
            if has_data: [data_len: u32BE] [data: bytes]
            [known_deleted: u8]
```

The BIN-delta format (`serialize_delta`):

```
[node_id: u64BE] [num_dirty: u32BE]
  per dirty slot: [slot_idx: u32BE] [key_len: u32BE] [key: bytes]
                  [lsn: u64BE] [has_data: u8]
                  if has_data: [data_len: u32BE] [data: bytes]
                  [known_deleted: u8]
```

These formats are undocumented. Someone writing a recovery verifier from the docs cannot
decode a `.ndb` file.

**Verdict**: `DIVERGES (unjustified)` — add a per-type payload section to `on-disk-format.md`.

---

### 3.4 Log Version History Not Documented

| Attribute | Value |
|---|---|
| **Severity** | Medium |
| **Subsystem** | docs/reference |
| **File:line (code)** | `crates/noxu-log/src/entry_type.rs:10–15` |

The code has a documented version history:

- v1 — initial format
- v2 — added `TxnPrepare` (type 32)

Neither `log-format.md` nor `on-disk-format.md` mentions this version history, the file
header version field, or how a reader should handle a version mismatch (forward/backward
compatibility rules are entirely absent from the docs).

**Verdict**: `DIVERGES (unjustified)` — add a "Version History" section.

---

### 3.5 CRC32 Scope Accurate

| Attribute | Value |
|---|---|
| **Severity** | Informational |
| **Subsystem** | docs/reference |

Both docs correctly state CRC32 covers `bytes[4..end]`. Code (`log_buffer.rs:317–323`)
confirms. `MATCHES`.

---

## Section 4 — Design-Decisions Doc Honesty

### 4.1 Decision 1 — Lock-Based Isolation

| Attribute | Value |
|---|---|
| **Severity** | Medium |
| **Subsystem** | docs/maintainer |
| **File:line** | `docs/src/maintainer/design-decisions.md:7–22` |

**What it says**:
> "Under high write concurrency, readers can block."

**What it omits**: The decision doesn't quantify the loss scenario. Readers
block not only under high write concurrency but under any write — a single long-running
write transaction blocks all readers on the written records for the transaction's duration,
with no timeout or retryable fallback (only `ReadUncommitted` avoids blocking, which
sacrifices isolation guarantees). This is a workload category where Noxu will consistently
lose to MVCC systems.

Missing:

- Concrete loss scenario: "read-heavy workloads with long-running write transactions will
  see dramatically higher read latency than MVCC alternatives"
- No citation to a reference that quantifies the difference (e.g., the classic Faleiro &
  Abadi "Rethinking Serializable MVCC" paper)
- The `txn_timeout_ms` mention is good but the word "readers can block" undersells it

**Verdict**: Partially honest. The decision should include a "When you should choose
something else" paragraph.

---

### 4.2 Decision 2 — CRC32 Title Scope

| Attribute | Value |
|---|---|
| **Severity** | Medium |
| **Subsystem** | docs/maintainer |
| **File:line** | `docs/src/maintainer/design-decisions.md:24–39` |

**What it says**: "CRC32 Not CRC32C for **Replication Feeder Protocol**"

**What is true**: CRC32 (via `crc32fast`) is used for **all log entries** (see
`noxu-log/src/checksum.rs`), not just the replication feeder. The title implies a
narrow, per-subsystem decision, when in fact it is a project-wide choice applied to the WAL.

The `checksum-selection.md` document is excellent and accurate; the design-decisions
title just undersells the scope.

**Verdict**: `DIVERGES (unjustified)` — rename to "CRC32 Not CRC32C (Project-Wide)" and
note that this applies to both WAL checksums and the replication feeder frame header.

---

### 4.3 Decision 3 — Confusing Self-Reference ("Noxu and Noxu")

| Attribute | Value |
|---|---|
| **Severity** | Medium |
| **Subsystem** | docs/maintainer |
| **File:line** | `docs/src/maintainer/design-decisions.md:52–53` |

**What it says**:
> "Noxu tools cannot read Noxu log files. Migration between Noxu and Noxu requires
> an export/import step at the application level."

The sentence uses "Noxu" for both the project itself and the BDB-JE reference source.
This reads as "Noxu cannot read its own log files", which is nonsensically wrong. It should
say something like:
> "Noxu DB tools cannot read BDB-JE (`.jdb`) log files. Migration from BDB-JE to Noxu
> requires an export/import step at the application layer."

**Verdict**: `DIVERGES (unjustified)` — fix to clarify the two entities being contrasted.

---

### 4.4 Decision 7 — Async Ecosystem Impedance Undocumented

| Attribute | Value |
|---|---|
| **Severity** | Medium |
| **Subsystem** | docs/maintainer |
| **File:line** | `docs/src/maintainer/design-decisions.md:94–107` |

**What it says**: "Async would require pervasive `await` throughout the codebase."

**What it omits**: The practical consequence for users: callers from async Rust code must
wrap every Noxu operation in `tokio::task::spawn_blocking`. This is the primary ecosystem
impedance. Failure to mention it means users in `axum` / `actix-web` / `tower` contexts
will encounter it as a surprise. The JE-to-Noxu port was justified by assuming single-app
embedded use; modern Rust applications are far more likely to be async than Java applications
were at JE's design time.

**Verdict**: Partially honest. Add: "Applications using async Rust runtimes (tokio, async-std)
must call Noxu operations from a synchronous context, typically via `spawn_blocking`. This
adds a thread-pool hop per database operation; latency-sensitive async workloads may prefer
a different database engine."

---

### 4.5 Missing Decision — No Nested Transactions

| Attribute | Value |
|---|---|
| **Severity** | Medium |
| **Subsystem** | docs/maintainer |
| **File:line** | `docs/src/internal/wave-3-1-nested-txn-removal.md` (internal only) |

BDB-JE supports nested transactions (savepoints). Noxu explicitly removed the
`parent: Option<&Transaction>` parameter in v2.0. This is a **user-visible API change
and a capability loss** from the reference, but there is no entry in `design-decisions.md`
for it. It exists only in an internal wave note.

Applications that relied on JE's nested transactions cannot be ported without architectural
changes. This deserves a top-level design decision, not a buried internal note.

**Verdict**: Missing decision entry. Add "No Nested Transactions" to `design-decisions.md`.

---

### 4.6 Decision 8 — Stale Unsafe Table Entry

| Attribute | Value |
|---|---|
| **Severity** | Low |
| **Subsystem** | docs/maintainer |
| **File:line (doc)** | `docs/src/maintainer/design-decisions.md:125` |
| **File:line (code)** | `crates/noxu-evictor/src/off_heap.rs` |

**What the doc says**: Table row listing `crates/noxu-evictor/src/off_heap.rs` as a
location where unsafe is allowed ("Off-heap BIN storage").

**What `AGENTS.md` says** (line 122–123):
> "`noxu-evictor::off_heap` originally used raw `mmap` ops but has been refactored to go
> through `memmap2` and `lru` safe wrappers and now contains no `unsafe`."

Verified in code: `off_heap.rs` uses `memmap2::MmapMut` and `lru::LruCache` with no
`unsafe` blocks. The table entry in `design-decisions.md` is stale.

**Verdict**: `DIVERGES (unjustified)` — remove `off_heap.rs` from the unsafe table.

---

## Section 5 — Comments That Drift from Code

### 5.1 `algorithms.md` Waiter Graph Direction (HIGH)

- **File:line**: `docs/src/maintainer/algorithms.md:65`
- **Says**: "`waiter_graph` maps blocker→[waiters]"
- **Does**: Maps `waiter → [owner_ids it is blocked by]` (standard waits-for direction)
- **Action**: Fix `algorithms.md` line 65

### 5.2 `algorithms.md` Victim Selection (MEDIUM)

- **File:line**: `docs/src/maintainer/algorithms.md:69`
- **Says**: "Youngest transaction (by txn_id) in the cycle is selected as victim"
- **Does**: This is only the tiebreaker. Primary criterion (fewest locks) is documented
  in `select_victim` but never evaluated (empty `lock_counts` always passed).
- **Action**: Change to "Effective behaviour: youngest locker (highest ID). Primary
  fewest-locks criterion documented but not wired in production."

### 5.3 `split_child` Dead "if low == 0" Branch (MEDIUM)

- **File:line**: `crates/noxu-tree/src/tree.rs:2293–2302`
- **Says**: Documents JE's "if low == 0" and "else" cases for which half the original
  node retains
- **Does**: Always uses the "else" case (child = left half, sibling = right half)
- **Action**: Delete the "if low == 0" arm from the doc comment

### 5.4 Recovery Spec Cites Non-Existent Files (HIGH)

- **File:line**: `crates/noxu-spec/src/recovery_three_phase.rs:9–11`
- **Says**:
  - `crates/noxu-recovery/src/transaction_table.rs` (does not exist)
  - `crates/noxu-recovery/src/dirty_page_table.rs` (does not exist)
- **Does**: The actual files are `analysis_result.rs` (commit/abort tracking, ARIES
  "transaction table") and `dirty_in_map.rs` (dirty-IN map, ARIES "dirty page table")
- **Action**: Update the spec comment to cite the correct filenames

### 5.5 VLSN Streaming Spec Cites Non-Existent File (LOW)

- **File:line**: `crates/noxu-spec/src/vlsn_streaming.rs:13`
- **Says**: `crates/noxu-rep/src/vlsn.rs`
- **Does**: File is at `crates/noxu-rep/src/vlsn/mod.rs`
- **Action**: Update spec comment

### 5.6 `select_victim` Primary Criterion Is Dead Code (MEDIUM)

- **File:line**: `crates/noxu-txn/src/deadlock_detector.rs:90–99`
- **Says**: Primary criterion is "fewest locks held"; tiebreaker is "youngest"
- **Does**: Lock manager passes empty `lock_counts`; primary criterion never fires
- **Action**: Either wire in lock counts or simplify the doc to describe actual behaviour

### 5.7 BIN `PartialEvict` Does Not Strip Data (MEDIUM)

- **File:line**: `crates/noxu-evictor/src/evictor.rs:PartialEvict` arm
- **Says** (via `decide_eviction` comment and stats counter `nodes_stripped` / `lns_evicted`):
  partial eviction strips LN data from BIN slots
- **Does**: Calls `node_size_fn(node_id)` and counts bytes freed, but does NOT actually
  clear `BinEntry::data` fields. The BIN stays in memory unchanged.
- **Action**: Either implement actual slot-data stripping (matching JE's BIN partial
  eviction) or rename the decision to "MeasureButSkip" and document why.

### 5.8 `design-decisions.md` "Noxu and Noxu" Confusion (MEDIUM)

- **File:line**: `docs/src/maintainer/design-decisions.md:52–53`
- **Says**: "Migration between Noxu and Noxu"
- **Does**: Means "migration from BDB-JE to Noxu DB"
- **Action**: Rewrite to name both systems explicitly

### 5.9 `Environment::open_database` — "currently ignored" txn param (LOW)

- **File:line**: `crates/noxu-db/src/environment.rs:434, 562, 624`
- **Says**: `txn - Optional transaction handle (currently ignored)`
- **Does**: Ignores the txn argument silently — this is a JE compatibility API where
  transactional database-open is significant
- **Action**: Either implement transactional open or add a NOTE explaining this is a
  permanent divergence, not a TODO

---

## Section 6 — Public API Documentation Completeness

Walk of `noxu-db` public surface:

| Function | Brief | Params | Returns | Errors | Panics | Example |
|---|---|---|---|---|---|---|
| `Environment::open` | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ (`ignore`) |
| `Environment::close` | ✅ | ✅ | ❌ | ✅ | ❌ | ❌ |
| `Environment::begin_transaction` | ✅ | ✅ | ❌ | ⚠️ (partial) | ❌ | ❌ |
| `Database::get` | ⚠️ (1 line only) | ✅ | ❌ | ⚠️ (1 line only) | ❌ | ❌ |
| `Database::put` | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ |
| `Cursor::get` | ❌ (no function doc) | ✅ | ❌ | ❌ | ❌ | ✅ (struct-level only) |
| `Cursor::put` | ❌ (no function doc) | ❌ | ❌ | ❌ | ❌ | ❌ |
| `Cursor::delete` | ❌ (no function doc) | ❌ | ❌ | ❌ | ❌ | ❌ |

**Key findings**:

1. **`Cursor::get` has no method-level doc comment** — only the struct has an example.
   The method signature doesn't document what `get_type` values are valid, what the
   `lock_mode` parameter does (it is currently ignored: `_lock_mode`), or when
   `OperationStatus::KeyEmpty` can be returned.

2. **All examples use `#[ignore]`** — they are excluded from `cargo test`. They may have
   silently drifted from the API (e.g., `Environment::open` example does not use the
   v2.0 `begin_transaction` signature).

3. **`Database::get` says `Returns an error if the database is closed`** as the only
   doc line before the function body — the `/// # Returns` and `/// # Errors` sections
   are missing from the function doc.

4. **`_lock_mode` in `Cursor::get` is silently ignored** — the parameter name has a
   leading underscore indicating intent to suppress the unused warning. Callers passing
   `LockMode::ReadCommitted` or `LockMode::Rmw` get no effect without knowing it.
   This should be documented prominently.

---

## Section 7 — Stateright Spec ↔ Implementation

| Spec | Cites implementation? | Files exist? | State ≈ impl? | Properties stated? |
|---|---|---|---|---|
| `btree_latching.rs` | ✅ file:function level | ✅ All exist | ✅ Close match | ✅ `ParentIsParent`, `SearchAlwaysFinds`, etc. |
| `wal_commit.rs` | ✅ | ✅ All exist | ✅ | ✅ `AllCommitsSeenAfterCrash`, etc. |
| `recovery_three_phase.rs` | ⚠️ Partial | ❌ `transaction_table.rs`, `dirty_page_table.rs` DNE | ✅ Model logic is correct | ✅ `AllAndOnlyCommitted`, `IdempotentReplay` |
| `flexible_paxos.rs` | ✅ | ✅ All exist | ✅ | ✅ `ElectionSafety`, `PromiseHonoured`, `QuorumIntersection` |
| `vlsn_streaming.rs` | ⚠️ Partial | ❌ `vlsn.rs` DNE (is `vlsn/mod.rs`) | ✅ | ✅ `VlsnMonotone`, `NoOverflow`, `AckTracksReceived` |
| `lock_manager_deadlock.rs` | ✅ | ✅ All exist | ✅ | ✅ |
| `cleaner_safety.rs` | ✅ | ✅ All exist | ✅ | ✅ |
| `cache_vs_cleaner.rs` | ✅ | ✅ All exist | ✅ | ✅ |
| `master_transfer.rs` | ✅ | ✅ All exist | ✅ | ✅ |
| `network_restore.rs` | ✅ | ✅ All exist | ✅ | ✅ |
| `xa_two_phase_commit.rs` | ✅ | ✅ All exist | ✅ | ✅ |

**Key findings**:

1. **`recovery_three_phase.rs` cites `transaction_table.rs` and `dirty_page_table.rs`** —
   ARIES terminology that doesn't match Noxu's naming. The actual abstractions are
   `AnalysisResult` (in `analysis_result.rs`) and `DirtyINMap` (in `dirty_in_map.rs`).
   The spec's state model (`TxnTable`, `DirtyPageTable`) is a reasonable abstraction but
   the stale file citations undermine trustworthiness.

2. **`vlsn_streaming.rs` cites `crates/noxu-rep/src/vlsn.rs`** which does not exist as
   a plain file (it's a module directory). A reviewer following the citation will find
   nothing.

3. **Properties are consistently stated in plain English** in the spec module doc comments
   — this is one of the strongest parts of the project. The `PersistentAcceptor` /
   `EphemeralAcceptor` variant pattern for documenting pre/post-fix correctness is
   exemplary and should be replicated in other specs that validate bug fixes.

4. **`VALIDATED-AS-OF: v2.4.0`** annotation in `recovery_three_phase.rs` is excellent
   practice — other specs should adopt it.

---

## Summary Table

| # | Severity | Subsystem | Description |
|---|---|---|---|
| 3.1 | **Critical** | docs/reference | Entry type codes in `on-disk-format.md` are completely wrong (none match actual enum values) |
| 1.5 | **High** | docs/maintainer | `algorithms.md` describes waiter graph direction in reverse |
| 3.2 | **High** | docs/reference | BIN/IN payload endianness documented as little-endian; code is big-endian |
| 3.3 | **High** | docs/reference | BIN/IN/BIN-delta payload wire format not documented at all |
| 5.4 | **High** | noxu-spec | `recovery_three_phase.rs` spec cites non-existent source files |
| 1.4 | **Medium** | noxu-txn | `select_victim` primary criterion (fewest locks) never evaluated; empty map always passed |
| 1.7 | **Medium** | noxu-evictor | `PartialEvict` does not actually strip BIN slot data; only updates memory counter |
| 3.4 | **Medium** | docs/reference | Log format version history not documented; no version-mismatch handling described |
| 4.1 | **Medium** | docs/maintainer | Lock-based isolation decision omits concrete loss scenario vs. MVCC |
| 4.2 | **Medium** | docs/maintainer | CRC32 decision title scoped to "replication feeder" but applies project-wide |
| 4.3 | **Medium** | docs/maintainer | "Noxu and Noxu" self-reference confusion in Decision 3 |
| 4.4 | **Medium** | docs/maintainer | Async ecosystem impedance (`spawn_blocking` requirement) not documented |
| 4.5 | **Medium** | docs/maintainer | No nested transactions decision missing from `design-decisions.md` |
| 5.3 | **Medium** | noxu-tree | `split_child` comment documents dead JE "if low == 0" code path |
| 5.6 | **Medium** | noxu-txn | `select_victim` primary criterion is dead code |
| 5.7 | **Medium** | noxu-evictor | `PartialEvict` comment and stats counters imply data stripping that doesn't happen |
| 5.8 | **Medium** | docs/maintainer | "Noxu and Noxu" confuses two separate systems |
| 1.1 | **Medium** | noxu-tree | `split_child` comment describes JE "if low == 0" path not in Noxu code |
| 2.1 | **Medium** | noxu-tree | `BinStub` lacks explicit invariant list; `cursor_count` is `i32` (can go negative) |
| 1.2 | **Low** | noxu-log | `fsync_manager` `DoLeaderFsync`+`work_in_progress` race path undocumented |
| 1.3 | **Low** | noxu-recovery | Module says "3-phase" but `recover()` doc says "five sub-phases" |
| 1.9 | **Low** | noxu-spec | `vlsn_streaming.rs` cites `vlsn.rs` which doesn't exist |
| 4.6 | **Low** | docs/maintainer | `off_heap.rs` listed as unsafe in design-decisions table; was refactored to safe |
| 5.5 | **Low** | noxu-spec | `vlsn_streaming.rs` cites `vlsn.rs` (should be `vlsn/mod.rs`) |
| 5.9 | **Low** | noxu-db | `open_database` ignores `txn` param silently without documentation |
| 6.1 | **Low** | noxu-db | `Cursor::get` has no method-level doc; `_lock_mode` silently ignored |
| 6.2 | **Low** | noxu-db | All doc examples use `#[ignore]`; may have drifted from current API |

**Totals**: 1 Critical, 4 High, 15 Medium, 7 Low

---

## Top 5 Most-Actionable Comment/Doc Fixes

1. **[CRITICAL] Regenerate the entry type code table** in `on-disk-format.md` from
   `entry_type.rs`. Add `TxnPrepare`, `RollbackStart/End`, `NameLN`, `FileSummaryLN`,
   `ImmutableFile`. This is a one-file change with a `for` loop or `grep`.

2. **[HIGH] Fix endianness statement** in `on-disk-format.md:56–57`: change "Most payload
   fields also use little-endian" to "BIN and IN payloads use **big-endian** encoding;
   only entry headers use little-endian."

3. **[HIGH] Fix waiter graph direction** in `algorithms.md:65`: change "maps blocker→[waiters]"
   to "maps `waiting_locker_id` → `[owner_ids it waits on]`."

4. **[MEDIUM] Add "No Nested Transactions" design decision** to `design-decisions.md`,
   pointing to `wave-3-1-nested-txn-removal.md` for rationale.

5. **[MEDIUM] Dead `select_victim` primary criterion**: either wire `lock_counts` from
   `LockManager::release` (trivially: call `get_owned_lock_count(victim_id)` before
   clearing) or simplify `select_victim` to take no `lock_counts` argument.

---

## Top 5 Most-Actionable Algorithmic Concerns

1. **[MEDIUM] `PartialEvict` does not evict**: The `decide_eviction` function returns
   `PartialEvict` for BINs, increments `nodes_stripped` and `lns_evicted` stats, and
   accounts for `node_size_fn(id)` bytes freed — but never clears `BinEntry::data`. The
   memory budget accounting will drift below real heap usage, potentially causing the
   evictor daemon to under-evict under memory pressure. Verify whether JE's BIN partial
   eviction is intended to be implemented or permanently deferred.

2. **[MEDIUM] `BinStub::cursor_count` can go negative**: The field is `i32`, decremented
   without a floor check. A cursor that closes twice (or a bug in CursorImpl) can produce
   a negative count that the evictor interprets as "cursors present" (since the check
   is `> 0`), permanently locking the BIN in cache. Change to `u32` or add
   `debug_assert!(self.cursor_count >= 0)` in the decrement path.

3. **[MEDIUM] `select_victim` primary criterion is dead**: The "fewest locks held"
   heuristic (JE's `LockManager.selectVictim`) is implemented but never evaluated. In
   a workload with long-running write transactions holding many locks, always aborting
   the youngest may abort the wrong transaction. Wire in the actual lock counts.

4. **[LOW] `DoLeaderFsync` + `work_in_progress` race in `fsync_manager`**: When a
   waiting thread is woken as `DoLeaderFsync` but finds `work_in_progress` still set,
   it performs a solo fsync without waking the `in_progress_group`. Any threads that
   joined `in_progress_group` after the new leader started but before our solo fsync
   will be left waiting. This is an edge case that needs a `wakeup_all` before the
   solo path returns, or explicit documentation that this scenario cannot arise.

5. **[INFORMATIONAL] Split algorithm needs a JE-divergence note**: The `split_child`
   comment describes both the JE "if low == 0" and "else" cases. The Noxu implementation
   permanently uses the "else" case. This is algorithmically correct for Noxu's parent
   model (no identifier-key preservation needed). Document this explicitly as a
   "JE divergence: we always assign left to child, right to sibling."

---

*Report path: `/tmp/noxu-audit-margo.md`*
