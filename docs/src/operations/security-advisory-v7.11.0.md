# Security / Data-Integrity Advisory — v7.11.0

## Noxu DB v7.11.0 — silent data-loss, isolation, and availability defects fixed in v7.11.1

**Status:** Published with the v7.11.1 release. All defects below are **fixed in
v7.11.1**, released **2026-10-20**.
**Affected:** Noxu DB **v7.11.0** (tag `b17f8258`) and earlier v7.11.x
pre-release build lineages.
**Fixed in:** **v7.11.1** (released 2026-10-20).
**Recommendation:** **Upgrade to v7.11.1.** Several defects below cause silent
data loss — including whole-database loss — reachable with **default settings**
and the background daemons alone, with no explicit maintenance call.

Every defect listed here is a **confirmed v7.11.0 defect** (present at tag
`b17f8258`). Defects that were introduced and fixed *within* the v7.11.1
development cycle (never shipped) are deliberately excluded from this advisory
and are recorded only in the CHANGELOG. Reproduced on x86-64 and aarch64; the
two whole-DB / value-loss defects were also reproduced on **riscv64** (RVWMO
weak memory). Architecture only changes the reproduction *rate*, not whether the
defect is present.

---

## Severity summary

| ID | Class | Worst outcome | Default-config exposure |
|----|-------|---------------|-------------------------|
| BUG-CLEANER-LIVE-FILE-DELETE | Data loss | **Whole database lost** | Yes — background cleaner alone |
| R-UNOPENED-RECLAIM | Data loss | **Whole recovered-but-unopened database lost** | Yes — cleaner runs before the db is opened after reopen |
| R-FIRSTLSN | Data loss | Committed DDL (create/remove/rename) in a long-open txn lost on crash | Yes — open DDL txn + cleaning + checkpoints + crash |
| R-RELOG-PENDING-DDL | Data loss | Committed DB remove/rename **resurrected** after commit + crash | Yes — background checkpointer alone |
| R-XARECOVERED | Data loss | **Whole committed prepared XA branch lost** | XA 2PC: in-doubt window + cleaner barrier + second crash |
| BUG-CLEANER-LIVE-LN-RECLAIM | Data loss | Committed value reads EMPTY / env invalidated | Yes — cache pressure + churn |
| BUG-CLEANER-IN-FETCH-DEAD | Data loss | Cascading reclaim of live data when an interior node's file is unreadable | Yes — cache pressure + cleaning |
| BUG-EVICT-UIN-WITH-CHILDREN (zombie parent) | Data loss | Live data orphaned; stale image resurrected by recovery | Yes — cache pressure (eviction) |
| BUG-EVICT-REFAULT-DATALOSS | Data loss | Committed value lost on re-fault | Yes — cache pressure |
| BUG-SPLIT-KD-RESURRECT (A5b) | Data loss | Committed delete resurrected | Yes — split + eviction + cleaning |
| BUG-DUP-DELETEALL-SPAN | Data loss | `delete(k)` leaves duplicates | Yes |
| A5 (KD-slot retention) | Data loss | **An uncommitted delete becomes permanent after a crash** (once a checkpoint has run); aborted txn visible / committed record lost | Yes — any crash during an in-flight delete |
| BUG-DW-SYNC-1 | Durability | Deferred-write `sync()` not durable | Deferred-write DBs |
| BUG-DDL-DURABILITY-1 | Durability | Committed DB remove/rename lost on crash | Yes — on crash recovery |
| NEW-DEL-RACE-1 | Isolation | Two deleters of one record both commit | Concurrent deletes |
| BUG-LOCKER-ID-COLLISION | Isolation | A cursor read or write without a transaction can share a lock owner with a live transaction: dirty read, lost update, a lock released early, spurious `DeadlockDetected` | Yes: mixing non-transactional cursor operations with transactions |
| BUG-BARE-RMW-LOCK-LEAK | Availability | A cursor without a transaction that takes a write lock for read-modify-write never releases it, even after close: later writers of that record time out | Yes: read-modify-write through a cursor without a transaction |
| FID-ELECT-1 (proposer self-vote split brain) | Availability / safety | **Two masters** admitted under a concurrent-election / partition-rejoin race | Replicated group with concurrent elections at the same term |
| R-LATCHLOCK | Isolation | **READ_COMMITTED / weaker read returns an uncommitted insert's value** (dirty read, S2) | Yes — concurrent insert + read-committed-or-weaker reader on ≥ 2 cores |
| R-SECREAD-NOTFOUND | Isolation | A reader (direct or via a sorted-dup secondary) returns `NOTFOUND` for a record present throughout a concurrent atomic update (S2); transient, not data loss | Yes — concurrent update + reader of the same key on ≥ 2 cores |
| NEW-SECMAINT-OLDDATA-TOCTOU | Isolation | Spurious `SecondaryIntegrityException` on concurrent same-new-key inserts into a DB with secondaries (S3); txn aborts cleanly, no corruption, env usable | Yes — concurrent inserts of one brand-new key with a secondary index |
| R-XADDL-711 | Isolation / data integrity | **DDL in a prepared XA transaction wrongly resolved across a crash** (S2): a committed REMOVE/RENAME resurrected or double-named, a multi-op branch's removals silently undone, and a concurrent op on an in-doubt name accepted as committed (an in-doubt-remove then truncated loses records) | XA 2PC: a CREATE/REMOVE/RENAME in a prepared branch resolved across a crash |
| R-LIVEDDL-711 | Isolation / data integrity | **Concurrent catalog DDL loses committed data** (S2): a transactional REMOVE/RENAME/TRUNCATE took no name-record lock, so a concurrent auto-commit op on the same name was not serialised; the transaction's abort then could not undo it — database gone or empty | Yes — a transactional catalog DDL concurrent with an auto-commit op on the same name |
| BUG-RC-SCAN-TORN-READ | Isolation | Read-committed scan returns uncommitted value | RC scans under contention |
| RMW-not-blocking | Isolation (lost update) | RMW read fails to block a concurrent writer | RMW under cleaning |
| BUG-PHANTOM-REVERSE-GAP | Isolation | Serializable range lock stranded; phantom insert allowed | Serializable + cleaner |
| BUG-TTL-REPEATABLE-READ-1 | Isolation | TTL-expired record NOTFOUND on 2nd same-txn read | TTL records |
| BUG-SYNC-RWLOCK-LOST-WAKE (A1) | Availability | Hang (lost wakeup) | aarch64 |
| BUG-VERIFY-TRACKER-LATCH-ABBA (R-VERIFY) | Availability | Verifier/checkpoint deadlock | Verifier + checkpoint |
| BUG-CKPT-FLUSHLEVELS-ABBA (R-FLUSHLVL) | Availability | Checkpoint/evictor deadlock | Checkpoint + eviction |
| BUG-LOGBUF-LATCH-1 | Availability | Log-buffer read/flush deadlock | Concurrent log read + flush |
| BUG-REP-STATE-TOCTOU (R-REPSTATE) | Availability | Spurious `InvalidStateTransition` on become_master | Replication startup |
| BUG-REP-DISPATCHER-LEAK (R-DISPATCH) | Availability | Thread+fd leak; same-port reopen comes up with NO dispatcher | Replication env churn |
| Checkpoint/Cleaner "already running" | Availability | Spurious error from the public API | Manual checkpoint/clean vs daemon |
| BUG-FEEDER-SCAN-SKIP (R-FEEDER) | Data loss (replica) | Feeder ships a VLSN gap (lost committed txn) to the replica | Replication + cleaning |
| **BUG-REP-USER-WRITES-NOT-REPLICATED (R-REPUSER)** | **Replication non-functional** | **No application write is ever replicated**; a replicated commit either silently succeeds locally (ack policy `None`) or times out (`InsufficientReplicas`). **v7.11.1 fails closed** (refuses the write with `NoxuError::ReplicationNotSupported`); full fix in v8.0.0 (see §0) | **Every replicated deployment** |

---

## 0. BUG-REP-USER-WRITES-NOT-REPLICATED — replication does not replicate application writes (CRITICAL; refused fail-closed in v7.11.1, full fix in v8.0.0)

**Symptom.** In a replicated group, a `put` / `delete` / commit on the master
through the public API is never sent to any replica. A commit with a replica-ack
durability policy (for example `SimpleMajority`) blocks for the full ack timeout
and fails with `InsufficientReplicas { available: 0 }`. A commit without an ack
policy succeeds locally, but the replicas never see the data. Failover therefore
loses every application write since the replica was created.
`ReplicatedEnvironment::commit_token()` and the master's current VLSN never
advance.

**Cause.** The application commit path writes its commit record without a
replication sequence number (VLSN). The replication feeder only ships entries
that carry one. The only code that tags a commit with a VLSN is reachable only
from tests, and the bridge that connects a data `Environment` to a
`ReplicatedEnvironment` (`with_environment`) needs an accessor that is
available only to tests. The replication test suite passed because those tests
tag commits by hand or treat the timeout as the expected result.

**Exposure.** Every v7.x release that offered replication. Reproduced on
v7.11.0 (`b17f8258`), the v7.11.1 release candidate, and the development
branch, on x86-64 and aarch64, in debug and release builds.

**Fix in v7.11.1 (fail-closed; owner decision #9).** v7.11.1 does **not** make
replication of user writes work — the complete JE-faithful fix (one write path,
a `ReplicationContext` on every log call, a per-txn commit VLSN, and DDL replay)
is **v8.0.0**. Instead, v7.11.1 **fails closed**: a data-logging write/commit on
an `Environment` that has a `ReplicatedEnvironment` installed as its
replica-ack coordinator is refused **at the operation, before any log record**,
with `NoxuError::ReplicationNotSupported`, at every write choke point
(`Database`/`Environment` `check_writable`, `open_database` create, the commit
path, the cursor backstop, sequences, collections auto-commit, and the XA layer
including a recovered prepared branch). This removes the silent-success and the
misleading-timeout failure modes: an application can no longer be fooled into
believing a write was replicated. Elections, group membership, master transfer,
and the feeder/ack machinery are unaffected.

**Workaround.** On v7.11.0: do not rely on replication for durability or
failover. On v7.11.1: the refusal **is** the signal — a `ReplicationNotSupported`
error on a write/commit means the write was NOT replicated and NOT committed;
run a **single node**, or **wait for v8.0.0** for replicated application writes.

## 1. BUG-CLEANER-LIVE-FILE-DELETE — whole-database loss (HIGH)

**Symptom.** On a **single-log-file environment** — every small or freshly
created database — the background cleaner can select, migrate, and **unlink the
log file that is still the active write target**. Because the database-mapping
(NameDB) record also lives in that file, the loss is not a few missing keys: on
reopen the database itself is reported absent (*"Database '…' does not exist and
allow_create is false"*). While the environment is still open, a later
`put`/`clean_log`/`checkpoint` that re-reads the dangling slot instead throws
`OperationNotAllowed("environment failure: Invalid entry type 0 at LSN 0x0/0x24")`
and poisons the environment. The same root cause also manifested as RMW failing
to block a writer (see §11), `cursor.delete()` on a live dup returning
`KeyEmpty`, and lost catalog (DDL) updates.

**Preconditions.** A single log file (default `cleaner.minAge = 2` is enough);
the **background cleaner daemon alone** triggers it (no explicit `clean_log()`;
default `run_cleaner = on`). A multi-file environment is **not** corrupted.

**Reproduction (measured).** riscv64 at v7.11.0: `cleaner_live_file_dbloss_test`
**FAIL 10/10** (deterministic); the batch-D churn reproducer
`forced_clean_terminates_under_migration_churn` **FAIL 10/10**. Reproduces
identically on x86-64 (d-x863) and aarch64 (d-arm3).

---

## 2. BUG-CLEANER-LIVE-LN-RECLAIM — committed value reads EMPTY / env invalidated (HIGH)

**Symptom.** Under a sustained **working-set ≫ cache** workload with the
cleaner, evictor, and checkpointer active, a point read of a **committed** key
can return an **empty value** (v7.11.0, fail-open) or a fatal
`LOG_FILE_NOT_FOUND` that **invalidates the environment**. In both cases the
committed value is not returned.

**Preconditions (all together).** Working set ≫ cache (continuous eviction +
re-fault); non-embedded LN values (> `TREE_MAX_EMBEDDED_LN`, default 16 bytes);
the cleaner actively reclaiming (record churn); concurrent readers. The
**background cleaner + evictor + checkpointer (defaults) are sufficient** — no
explicit `clean_log()` needed.

**Reproduction (measured).** v7.11.0 value-correctness witness
(`cleaner_ln_reclaim_witness`, 1 MiB cache, 40 000 × 400-byte records ≈ 16.8×
over cache): x86-64 **1/50 pass** (fails ~98 %); aarch64 **0/30 pass** (fails
every run); **riscv64 FAIL 30/30**. The witness originally asserted only
presence, not value — an empty read passed; it now asserts byte-equality.

---

## 3. BUG-CLEANER-IN-FETCH-DEAD — cascading reclaim when an interior node is unreadable (HIGH)

**Symptom.** The cleaner's slot-currency check descended with a hand-rolled
`get_slot_lsn_from_root` that did **not** re-fault evicted interior nodes and
returned `None` for a key whose path crossed any non-resident (evicted)
interior IN. A `None` was read as *slot-mismatch → Obsolete → Dead*, so the
cleaner reclaimed the LN's log file **while a live BIN slot still referenced
it**. If an interior node's log file was unreadable, the failure propagated as a
`None` that was likewise treated as Dead — a **cascading reclaim** of live data
below the unreadable node, rather than failing closed.

**Reachability at v7.11.0: YES — a superset of the evicted-IN case.** At the tag
`get_slot_lsn_from_root` returned `None` for **every** evicted interior IN
(whether merely non-resident or genuinely unreadable), so any key whose path
crossed an evicted IN under cleaning was exposed; the unreadable-file case is a
strict subset of that already-reachable "`None` → Dead" path. Fixed by using the
real re-faulting `search_with_data` descent, plus an IN-fetch-failure guard that
treats an unreadable interior IN as **Unknown** (fail-closed, retain), never
Dead. **Precondition:** cache pressure (evicted interior INs) + the cleaner
reclaiming.

---

## 4. BUG-EVICT-UIN-WITH-CHILDREN — zombie parent / orphaned live data (HIGH)

**Symptom.** The evictor could **detach an interior (upper) node that still had
resident children**, orphaning live data below it; a detached parent could then
pass the flush identity check so a **stale image was logged and stamped into the
parent slot**, and recovery redo (newer-LSN-wins) could **resurrect that stale
image over the committed value**. Net effect: live data unreachable and/or a
committed value overwritten by a stale one after recovery.

**Root cause.** The sole production non-root upper-IN detach site
(`detach_node_by_id`) had a BIN cursor/dirty re-check but **no upper-IN
childlessness re-check**, while `decide_eviction` read `has_cached_children`
from a *pre-latch* snapshot — a TOCTOU: a child could be faulted in under the
upper IN between the snapshot and the detach (vs JE's under-latch
`hasCachedChildren` re-check in `Evictor.processTarget`). Fixed with a
childlessness re-check under the parent write latch in `detach_node_by_id`
(covers the evictor's background and critical-eviction paths), plus the
root-down flush-parent resolution (so a detached parent is never flushed).

**Reachability at v7.11.0: YES — confirmed by direct probe** (d-x864 root-cause
report, STEP 6): on a fresh v7.11.0 (`b17f8258`) worktree, `detach_node_by_id`
(tree.rs:7772) has the BIN never-logged + BIN cursor/dirty guards but **no**
`TreeNode::Internal` childlessness re-check before `take_child` — the identical
gap; the probe builds a 3-level tree, makes the lowest upper IN childless,
re-faults a BIN under it, and `detach_node_by_id(uin)` succeeds (detaches the
upper IN from the live tree with a resident child = zombie). **Precondition:**
cache pressure that drives upper-IN eviction while a child is (re)faulted.

**Reproduction (measured).** Real-evictor TOCTOU witness
(`real_evictor_uin_toctou`): on base the real evictor detaches an upper IN with
a resident child (zombie); the durability witness
`committed_child_write_survives_log_roundtrip_after_refusal` shows the root-down
search returns `None` (orphaned, value unreachable) on base; GREEN after the
fix.

---

## 5. BUG-EVICT-REFAULT-DATALOSS — committed value lost on re-fault

**Symptom.** A dirty BIN stripped by the evictor and later re-faulted could come
back with a stale or empty image, losing a committed value. **Precondition:**
cache pressure causing eviction + re-fault of dirty BINs.

---

## 6. BUG-SPLIT-KD-RESURRECT — a committed delete resurrected (A5b)

**Symptom.** A **committed delete could be resurrected** when the cleaner
validated a reused BIN slot after a concurrent split. The cleaner's liveness
check routed to a parent slot latch-free, then took the parent write latch in a
separate acquisition and compared the logged image against whatever node sat at
that slot; a concurrent split/merge/eviction could relocate or reuse the slot in
that window, so a known-deleted (tombstone) slot could be resurrected over a
committed-live value. **Precondition:** concurrent split + eviction + cleaning.
**Reachability at v7.11.0: YES** (base cleaner liveness-check code).

---

## 7. BUG-DUP-DELETEALL-SPAN — `delete(k)` leaves duplicates

**Symptom.** A non-cursor `delete(k)` on a key with duplicates silently removed
only part of the duplicate set across a BIN span, leaving some duplicates
behind.

---

## 8. A5 — aborted transaction visible / committed record lost after a crash

**Symptom.** Recovery undo of an in-flight / aborted transaction could lose a
committed record or resurrect an aborted one (known-deleted slots were not
retained for the recovery scanner to follow). **Precondition:** a crash with an
in-flight transaction that had deleted records.

**Most common form: a crash makes an uncommitted delete permanent.** v7.11.0
physically removes a record's slot at `delete()` time, before the transaction
commits. If a checkpoint writes that BIN while the delete is still uncommitted
and the process then crashes, recovery has no slot to roll back to, and **the
record is permanently lost** although its delete never committed. Reproduced
deterministically, 10/10 on x86-64 and aarch64 (independent review, 2026-10-16).
An explicit `abort()` before the crash is handled correctly. **Logs written by
v7.11.0 cannot be repaired by v7.11.1:** the slot is already gone from the
checkpointed BIN, so only crashes that happen after upgrading are protected.
v7.11.1 keeps the slot (marked pending-deleted, as JE does) until the deleter
commits; witness
`fid_cursor1_crash_recovery_undoes_uncommitted_delete_deterministic`.

---

## 9. BUG-DW-SYNC-1 — deferred-write `sync()` not durable

**Symptom.** On a deferred-write database, `sync()` followed by a crash lost the
synced records; evicting a deferred-write BIN with a never-logged slot wrote a
NULL-LSN image. **Precondition:** deferred-write databases.

---

## 10. BUG-DDL-DURABILITY-1 — committed DB remove/rename lost on crash

**Symptom.** A committed transactional database `remove`/`rename` followed by a
crash could resurrect the old name; an aborted one could leave the new name.
**Precondition:** a crash after a committed/aborted DDL transaction.

---

## 11. Isolation defects

- **R-LATCHLOCK (S2 dirty read):** On v7.11.0, a `READ_COMMITTED` (or
  weaker, incl. the default lock mode) read of a key that a concurrent
  transaction is inserting could return the *uncommitted* inserted value if the
  read and the insert run on **two or more cores** at the same instant. The
  value may subsequently abort, so the reader could act on data that never
  committed. **Trigger:** a concurrent insert + a reader at read-committed or
  weaker on ≥ 2 CPU cores. **Impact:** the reader may observe (and return) an
  uncommitted value that may then be rolled back. **Affected:** v7.11.0.
  **Fixed:** v7.11.1 (R-LATCHLOCK): the record lock is now acquired while the
  BIN is latched, so a reader that sees a published-but-uncommitted record also
  sees the inserter's write lock and blocks until it commits or aborts.
  **Mitigation until upgrade:** use serializable isolation, or confine reads of
  concurrently-inserted keys to a single thread, until you upgrade to v7.11.1.
- **R-SECREAD-NOTFOUND (S2):** On v7.11.0, a reader — either an exact primary
  read or a read via a sorted-duplicate secondary index — could return
  `NOTFOUND` for a record that exists throughout a concurrent atomic
  update (a delete followed by a reinsert of the same key in one transaction),
  even though the key is never logically absent. During the updater's
  delete-and-reinsert the record's slot (or, via a secondary, the secondary's
  duplicate slot) is momentarily pending-deleted and write-locked by the
  updater; the reader's positioning seek skipped that pending-deleted slot
  **without taking the record lock** and reported the record as absent instead
  of blocking on the in-flight update. **Trigger:** a concurrent in-progress
  update (delete + reinsert of one key) plus a reader of that key, direct or
  through a sorted-dup secondary, on ≥ 2 CPU cores. **Impact:** a transient,
  wrong `NOTFOUND` for a record that is in fact present — not data loss and not
  corruption; the correct value is returned once the updater commits.
  **Affected:** v7.11.0. **Fixed:** v7.11.1 (R-SECREAD-NOTFOUND): the reader
  now lands on the pending-deleted slot, takes the record lock, and **blocks**
  on the updater exactly as it would for any other write-locked record,
  observing the committed reinsert (or the restored record on abort) rather
  than a spurious miss. **Mitigation until upgrade:** use serializable
  isolation, or confine reads of a concurrently-updated key to the updating
  thread, until you upgrade to v7.11.1.
- **NEW-SECMAINT-OLDDATA-TOCTOU (S3):** On v7.11.0, two transactions inserting
  the **same brand-new primary key** at the same time into a database that has
  a secondary index could raise a spurious
  `SecondaryIntegrityException("duplicate (sec_key, pri_key)")`. Each inserter
  read the secondary-maintenance pre-image of the key as absent **before** the
  primary write took its slot lock; once the lock serialised them, the loser's
  secondary maintenance re-inserted an identical `(sec_key, pri_key)` entry and
  raised the integrity error. **Trigger:** concurrent inserts of the same
  brand-new primary key into a database with one or more secondaries.
  **Impact:** the enclosing transaction **aborts cleanly** — the environment is
  not invalidated, the secondary is not marked corrupt, no orphan secondary
  entry is produced, and the environment stays usable. It is a loud, transient,
  non-corrupting error. **Affected:** v7.11.0. **Fixed:** v7.11.1
  (NEW-SECMAINT-OLDDATA-TOCTOU): the secondary-maintenance pre-image is now
  read **under the primary's write lock**, so the second inserter blocks on the
  key, observes the first inserter's committed record as the old data, and
  performs a correct update with no duplicate secondary insert.
  **Mitigation until upgrade:** serialise inserts of the same new key, or retry
  the cleanly-aborted transaction, until you upgrade to v7.11.1.
- **NEW-DEL-RACE-1:** two concurrent deleters of the same record both committed a
  delete. Precondition: concurrent deletes of one record.
- **BUG-RC-SCAN-TORN-READ:** a read-committed scan whose relock livelocked fell
  back to an **unlocked** read and could return an uncommitted value mid-write.
  Precondition: read-committed scans under write contention.
- **RMW not blocking a concurrent writer (lost update):** a read-modify-write
  reader's write lock sat on an LN in the live log file; the cleaner deleting
  that file (BUG-CLEANER-LIVE-FILE-DELETE) dropped the lock, so a concurrent
  writer was not blocked. Precondition: RMW under active cleaning. (Root cause =
  the live-file delete; listed here as the isolation symptom.)
- **BUG-PHANTOM-REVERSE-GAP:** under serializable isolation a next-key /
  previous-key range lock was stranded when the cleaner migrated an LN (the IN
  compressor changed the "next key" between scan positioning and the phantom
  insert), so a phantom insert into the locked range was wrongly **allowed**.
  Precondition: serializable isolation with the cleaner/compressor running.
- **BUG-TTL-REPEATABLE-READ-1:** a second read of a TTL-expired record in the
  same transaction that locked it returned NOTFOUND (repeatable-read violation).

---

## 12. Availability defects

- **BUG-SYNC-RWLOCK-LOST-WAKE (A1):** on aarch64 the `NoxuRawRwLock` could lose a
  wakeup and **hang** a parked waiter indefinitely.
- **BUG-VERIFY-TRACKER-LATCH-ABBA (R-VERIFY):** the verifier's utilization
  tracker and the checkpointer could **deadlock** on an ABBA latch order.
- **BUG-CKPT-FLUSHLEVELS-ABBA (R-FLUSHLVL):** the evictor's BIN-latch →
  flush-levels-mutex order raced the checkpointer's flush-levels-mutex →
  node-latch order — a **deadlock**.
- **BUG-LOGBUF-LATCH-1:** a log-buffer latch held across a flush could
  **deadlock** a concurrent log reader.
- **BUG-REP-STATE-TOCTOU (R-REPSTATE):** a contended `become_master` /
  `become_replica` observed a stale state in a released-lock gap and drove an
  invalid self-transition — a spurious `InvalidStateTransition` ("MASTER ->
  MASTER") on replication startup under load.
- **BUG-REP-DISPATCHER-LEAK (R-DISPATCH):** the replication service-dispatcher
  accept thread, its listener fd, and its bound port survived env drop/close and
  leaked until process exit (**a thread + fd per env**); a same-port reopen then
  came up with **no dispatcher** (`EADDRINUSE` swallowed) — a node that reports
  healthy but **cannot serve RESTORE / SYNCUP / feeder / election**.
- **Spurious "Checkpoint already in progress" / "Cleaner is already running":** a
  manual `checkpoint()` or `clean_log()` overlapping the background daemon
  returned an **error from the public API** instead of waiting.

---

## 13. Replica data-loss defect

- **BUG-FEEDER-SCAN-SKIP (R-FEEDER):** the replication feeder log scanner could
  silently **skip a log file cleaned ahead of the cursor**, shipping a VLSN gap
  (a lost committed transaction) to the replica. Precondition: replication with
  the cleaner reclaiming log files below the feeder.

---

## 14. Defects fixed in the v7.11.1 RC v3 assembly (per-item detail)

The following defects were confirmed on v7.11.0 (tag `b17f8258`) and fixed in
v7.11.1. Each gives affected versions, trigger, impact, fix version, and
mitigation.

### 14.1 R-UNOPENED-RECLAIM — a recovered-but-unopened database can be reclaimed (S1 data loss)

- **Affected versions:** v7.11.0 and earlier v7.11.x build lineages.
- **Trigger:** reopen an environment (after clean shutdown or crash) and let the
  log cleaner run — the background cleaner, or an explicit `clean_log()` /
  `clean_log_forced()` followed by two checkpoints — **before** the application
  opens a database that was created in a previous session. Databases opened
  before cleaning are unaffected.
- **Impact:** the cleaner can permanently delete the log file(s) holding that
  database's records and B-tree — whole-database loss; on reopen the database is
  reported absent.
- **Fix version:** v7.11.1 (the cleaner treats an LN/IN/BIN whose `db_id` is
  live in the catalog but has no registered tree as live and retains its file
  until the db is opened; owner-approved `D-ADDITIVE-SAFETY`, 7.11.x only).
  Superseded in v8.0.0 by the mapping-tree `get_db` fault path + comparator
  registry.
- **Mitigation on v7.11.0:** open every database you intend to use **before**
  any cleaning or checkpoint runs on a freshly reopened environment; or disable
  the cleaner (`run_cleaner = false`) until all databases are opened.

### 14.2 R-FIRSTLSN — a long-open DDL transaction's file can be reclaimed (S1 data loss)

- **Affected versions:** v7.11.0 and earlier v7.11.x build lineages.
- **Trigger:** a database **create / remove / rename inside an explicit
  transaction** that stays open across a log-cleaning pass and at least two
  checkpoints, then commits, then the process crashes before the next
  checkpoint. Data written through a cursor (ordinary put/delete, truncate,
  sequences, DPL, collections) was never affected.
- **Impact:** the committed DDL is lost on the crash — recovery cannot find the
  NameLN because the cleaner deleted the file holding the transaction's first
  (NameLN) entry, which the first-active-LSN clamp failed to protect.
- **Fix version:** v7.11.1 (record the first-logged LSN at the single
  transactional-log-write chokepoint, so every transactional entry — including
  a leading NameLN — is clamp-protected).
- **Mitigation on v7.11.0:** do not keep a DDL transaction open across cleaning
  and checkpoints; commit DDL promptly and checkpoint after.

### 14.3 R-RELOG-PENDING-DDL — a committed DB remove/rename can be resurrected (S1 data loss)

- **Affected versions:** v7.11.0 and earlier v7.11.x build lineages.
- **Trigger:** a transactional `remove`/`rename` of a database; a checkpoint
  (the **background checkpointer alone** suffices) runs while the txn is open
  and re-logs a live NameLN past the delete tombstone; then commit + crash.
- **Impact:** recovery's latest-NameLN-per-name rule resurrects the removed /
  old name after the commit + crash — the removed database reappears, or a
  rename's old name comes back.
- **Fix version:** v7.11.1 (skip pending-DDL names in the catalog relog +
  re-log one pre-tombstone live copy kept alive by the R-FIRSTLSN clamp; the
  `pending_ddl_names` marker is set before relog-once/tombstone to close a
  narrow TOCTOU window against a background relog). Owner-approved
  `D-ADDITIVE-SAFETY`, 7.11.x only; superseded in v8.0.0.
- **Mitigation on v7.11.0:** force a checkpoint immediately after committing a
  DDL `remove`/`rename` and before any crash exposure; avoid long-open DDL txns.

### 14.4 R-XARECOVERED — a committed prepared XA branch can be lost (S1 data loss)

- **Affected versions:** v7.11.0 and earlier v7.11.x build lineages.
- **Trigger:** an XA transaction that completed `xa_prepare` and survived a
  crash (recovered in-doubt), **then** a cleaner deletion barrier (two
  checkpoints — the background cleaner, or explicit `clean_log_forced()` +
  `checkpoint()`) runs **before** `xa_commit` resolves it, **then** a second
  crash after `xa_commit` but before the next checkpoint.
- **Impact:** on the final reopen the committed branch's records are silently
  absent — whole committed-branch loss. A clean resolution without an
  intervening barrier, or a resolution followed by a checkpoint, was unaffected.
- **Fix version:** v7.11.1 (redo **and** re-lock a recovered prepared txn's LNs
  during the first recovery, JE `RecoveryManager` redoLNs/eligibleForRedo/
  redoOneLN/relock, so the branch's slots are materialised and lock-protected
  until resolution). **Behaviour change (owner #19):** after upgrade a reader of
  an unresolved in-doubt record **blocks** until the branch is resolved and, on
  timeout, receives the typed `NoxuError::LockTimeout` / `LockConflict` — JE XA
  isolation parity.
- **Mitigation on v7.11.0:** resolve recovered in-doubt XA transactions
  (`xa_recover` → `xa_commit`/`xa_rollback`) **immediately** after opening the
  environment, before any cleaning or checkpoint, and force a checkpoint after
  resolving.

### 14.5 FID-ELECT-1 — proposer self-vote split brain (S1 safety)

- **Affected versions:** the entire v7.11.x line, including v7.11.0 (tag
  `b17f8258`). v7.11.0 is affected **more broadly** (free synthetic self-vote,
  no acceptor value guard, bare-`term` keying); an rc2/7.11.1 interim value
  guard narrowed exposure to the JE topology, and `d5e1e448` closes it entirely.
- **Trigger:** a replicated group with concurrent elections at the same term
  (partition / rejoin race).
- **Impact:** two nodes can each conclude they won and both become master
  (split brain), risking divergent writes.
- **Fix version:** v7.11.1 (route the proposer's self-vote through its own
  acceptor so its prior promise/accept correctly bounds its proposal; preserves
  "at most one master" across partition/rejoin). Patch-safe, no wire/on-disk
  change.
- **Mitigation on v7.11.0:** treat concurrent elections at the same term as
  unsafe; the fix is backportable with the same patch-safe shape.

### 14.6 BUG-LOCKER-ID-COLLISION — non-txn locker shares a transaction's lock owner (S2 isolation)

- **Affected versions:** v7.11.0 and earlier.
- **Trigger:** mixing non-transactional cursor operations with transactions.
- **Impact:** a cursor read/write without a transaction could share a lock owner
  with a live transaction — a dirty read, a lost update, a lock released early,
  or a spurious `DeadlockDetected`.
- **Fix version:** v7.11.1 (non-transactional lockers get a disjoint id
  namespace).
- **Mitigation on v7.11.0:** do not mix bare-cursor operations with explicit
  transactions on the same records; use an explicit transaction throughout.

### 14.7 BUG-BARE-RMW-LOCK-LEAK — a non-txn cursor's RMW write lock leaks forever (S3 availability)

- **Affected versions:** v7.11.0 and earlier.
- **Trigger:** a read-modify-write through a cursor without a transaction.
- **Impact:** the write lock is never released, even after the cursor is closed,
  so later writers of that record time out across threads for the life of the
  environment.
- **Fix version:** v7.11.1 (release a bare cursor's non-transactional locks on
  close and on move — at the MOVE, not deferred to the RMW upgrade — narrowing
  the lock to the cursor's positioned lifetime). A related same-thread
  non-sharing limitation (`FID-THREADLOCKER-SHARE`, S3) remains, fixed in v8.0.0.
- **Mitigation on v7.11.0:** use an explicit transaction for read-modify-write
  so the lock is released at commit/abort.

### 14.8 R-XADDL-711 — DDL inside a prepared XA transaction wrongly resolved across a crash (S2 isolation / data integrity)

- **Affected versions:** v7.11.0 and earlier v7.11.x build lineages. On
  v7.11.0 the failure mode was worse: a transactional remove/rename logged
  nothing at prepare time, so the prepared branch vanished entirely across a
  crash — an XA prepare-durability violation.
- **Trigger:** a database CREATE, REMOVE or RENAME performed inside an XA
  transaction that reached `xa_prepare` (phase 1) and was then resolved across
  a crash — the mandatory `xa_recover()` reopen followed by `xa_commit` /
  `xa_rollback`. Also: any concurrent CREATE/REMOVE/RENAME/TRUNCATE/OPEN of an
  affected name while the branch was in doubt.
- **Impact:** a committed REMOVE left the database resurrected with all its
  records; a committed RENAME left it under both the old and new names; a
  prepared branch carrying more than one DDL op had its committed
  removals/renames silently undone; and a concurrent op on an in-doubt name was
  accepted as though the branch had already committed — a concurrent truncate of
  an in-doubt-remove database then lost that database's records on a subsequent
  rollback.
- **Fix version:** v7.11.1 (collect a prepared transaction's NameLNs during
  recovery and reconstruct the in-doubt catalog state **per NameLN**, rename
  paired by database id; mark the removed/renamed-away names so the
  post-recovery checkpoint relog cannot resurrect them past their tombstone;
  and make every catalog operation on an in-doubt name conflict until the
  branch resolves. Resolution runs the normal DDL commit/abort over all the
  branch's operations). This mirrors JE's resurrected-`PreparedTxn` handling —
  `RecoveryManager` resurrects and WRITE-relocks each prepared LN (NameLN
  included) during recovery, and `Txn.commit`/`abort` cleans up every database
  in the branch. Patch-safe: no on-disk format change (NameLNs are already
  logged), no public-API change.
- **Mitigation on v7.11.0:** resolve recovered in-doubt XA transactions
  (`xa_recover` → `xa_commit`/`xa_rollback`) **immediately** after opening the
  environment and force a checkpoint afterward; avoid concurrent catalog
  operations on a name a prepared branch is removing/renaming until the branch
  resolves.

### 14.9 R-LIVEDDL-711 — concurrent catalog DDL loses committed data on abort (S2 isolation / data integrity)

- **Affected versions:** v7.11.0 and the whole v7.11.x line (pre-existing; not
  a regression).
- **Trigger:** a database REMOVE, RENAME or TRUNCATE performed inside a
  transaction `T`, concurrent with an auto-commit REMOVE, RENAME, TRUNCATE or
  OPEN of the **same** database, where `T` then aborts.
- **Impact:** the transactional DDL took no lock on the catalog name record, so
  the concurrent auto-commit op was not serialised against the uncommitted
  operation; when `T` aborted its effect was already applied and the rollback
  could not undo it — the database was left gone or empty, losing committed
  records.
- **Fix version:** v7.11.1 (take a lock on the name record for the lifetime of
  the DDL — a **blocking WRITE** for remove/rename/truncate, so a concurrent
  catalog op blocks or fails with a typed, retryable lock error; a
  **non-blocking (fail-fast) READ** for open, so a concurrent open of a name a
  transaction is removing/renaming fails immediately with a lock conflict.
  `database_names()` stays lock-free. A transaction holding such a DDL and a
  recovered in-doubt prepared-XA DDL share the one name-record lock). This
  matches JE's `DbTree.lockNameLN` WRITE lock for remove/rename/truncate and
  the resurrected prepared transaction's NameLN relock; JE instead takes a
  **blocking** READ for open (`DbTree.getDb`), which v8 restores — so the
  wait-vs-refuse behaviour of open is a documented 7.11.x deviation
  (**FID-711-OPEN-FAILFAST**). The lock id is a per-name synthetic id; two
  distinct names with equal 32-bit hashes may false-contend (a typed,
  retryable conflict, never data loss — **FID-711-NAMELOCK-HASH**, fixed to a
  collision-free NameLN-record id in v8). Patch-safe: no on-disk format change,
  no public-API change.
- **Mitigation on v7.11.0:** do not run an auto-commit catalog operation on a
  name that is being removed/renamed/truncated inside an open transaction;
  serialise catalog DDL on a given database name, or keep all catalog
  operations on that name inside one transaction.

### 14.10 ENG-CLEAN-TOMBSTONE-1 — a removed database can be resurrected by log cleaning (S1 data loss)

- **Affected versions:** v7.11.0 and v7.11.1 (and the v7.11.2 candidate before
  this fix).
- **Trigger:** a committed `remove_database` — transactional or
  auto-commit — or a rename's old-name removal, followed by log cleaning
  (the background
  cleaner, or `clean_log()`) that reclaims the log file holding the removal's
  record while an older surviving file still holds the database's create
  record. Databases that were never removed/renamed, or environments without
  cleaning, are unaffected.
- **Impact:** the cleaner classifies a database-name record (`NameLN`) as
  obsolete and never migrates it, and the per-checkpoint catalog relog
  re-materialises only *live* names — never removals. So the reclaimed removal
  record is lost while an older create record survives, and recovery's
  latest-record-wins catalog replay picks the stale create: **the removed
  database (with its records) reappears on reopen**, silently (recovery returns
  success, `verify` passes).
- **Fix version:** v7.11.2 (a cleaner retain-guard keeps a removal's file until
  nothing older can resurrect the name — the same two-checkpoint
  deletion-barrier mechanism used for a recovered-but-unopened database,
  §14.1). The guard retains only a removed name's **latest** on-disk removal
  record while an older create for that name still survives, so it retains **at
  most one log file per removed name that still has an older surviving create**
  — a bound independent of how many times the name was removed and recreated.
  (This bounds only what the guard itself retains; it does not bound total log
  size, which is governed by the cleaner's ordinary utilization policy.)
  Owner-approved `D-ADDITIVE-SAFETY`, 7.11.x
  only, superseded in v8.0.0 by the mapping-tree / MapLN-tombstone model. No
  on-disk format change; the fix is forward-acting, so a dump/reload is **not**
  required — once running v7.11.2 a committed remove stays removed. The
  XDB-711 detector does **not** reliably flag an already-resurrected
  environment: a
  resurrection produced purely by this cleaner-reclaim path leaves no surviving
  stray-relog evidence once the removal's file is gone.
- **Mitigation on v7.11.0/.1:** force a checkpoint immediately after a committed
  `remove`/`rename` and keep the database's create file from being cleaned (open
  the database before cleaning, or avoid aggressive cleaning) until you upgrade.

---

## How to tell if you're affected

| Defect | On-disk signature / error string / counter |
|--------|---------------------------------------------|
| BUG-CLEANER-LIVE-FILE-DELETE | A single `.ndb` file that never rolled, with an internal zero-filled hole (logical content starts far into the file; header `[0..0x24]` then zeros). Error `Invalid entry type 0 at LSN 0x0/0x24` (`0x24` = v3 first-entry offset). **`cleaner.deletions >= 1` on an environment that has only ever had one log file is itself diagnostic** (a healthy single-file env never deletes a file). On reopen: *"Database '…' does not exist and allow_create is false"*. |
| BUG-CLEANER-LIVE-LN-RECLAIM / IN-FETCH | A point read of a committed key returns `Ok(None)` / empty bytes (fail-open), **or** a fatal `LOG_FILE_NOT_FOUND` ("LN fetch at Lsn(…): Log file not found: …/NNNNNNNN.ndb") that **invalidates the environment** (subsequent ops fail with the env-invalid error). |
| BUG-EVICT-UIN-WITH-CHILDREN / EVICT-REFAULT | A committed value read back empty/stale after cache pressure, or `LOG_FILE_NOT_FOUND` on re-fetch of a slot whose parent was detached; after recovery, a committed value replaced by an older one. |
| BUG-SPLIT-KD-RESURRECT | A key you deleted (and committed) reappears after a period of eviction + cleaning. |
| BUG-DUP-DELETEALL-SPAN | After `delete(k)` on a key with duplicates, a subsequent `get`/cursor scan still returns some duplicate values for `k`. |
| A5 / BUG-DDL-DURABILITY-1 | After a crash: an aborted transaction's records present, or a committed record missing; a committed DB remove/rename not reflected (old name resurrected / new name lost). |
| BUG-FEEDER-SCAN-SKIP | A replica missing a contiguous VLSN range that the master committed (a gap in the applied stream), with no feeder error. |
| ENG-CLEAN-TOMBSTONE-1 | A database you removed (or a rename's old name) reappears on reopen after log cleaning, with its old records; in-memory it was absent before the clean close. No error; `verify` passes. Most likely when a removed name's create record shared a log file with another database's long-lived data while cleaning reclaimed the removal's (lower-utilization) file. |

A clean single-file environment that has never had `cleaner.deletions > 0`, a
cache that holds the live working set, all-small (≤ 16-byte, embedded) values,
or a read-mostly no-churn workload, is not exposed to the cleaner/evictor
data-loss class.

---

## Recommendation

**Upgrade to Noxu DB v7.11.1.** v7.11.1 is a patch release (bug fixes only; no
on-disk format change — `LOG_VERSION` stays 3; no public API change in the
Stable-tier crates), so it is a drop-in upgrade: no data migration, no code
change required. There is no complete pre-upgrade mitigation for the whole-DB
and value-loss defects other than disabling the cleaner (`run_cleaner = false`)
and/or ensuring the cache holds the live set — neither is a real fix.

Databases already damaged by BUG-CLEANER-LIVE-FILE-DELETE (a single `.ndb` file
with an internal zero hole, or a reopen reporting the database absent) cannot be
recovered by upgrading — the data in the unlinked file is gone. Restore from a
backup taken before the damage, then upgrade before resuming writes.

---

*Published 2026-10-20 with the v7.11.1 release.*
