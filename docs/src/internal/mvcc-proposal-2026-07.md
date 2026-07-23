# Proposal: Multi-Version Concurrency Control (MVCC) for Noxu DB

**Status:** Research / Design proposal — *for human decision.* NOT approved,
NOT scheduled, NOT implemented.
**Date:** 2026-07
**Author:** research/mvcc-proposal
**Decision requested:** Do we add MVCC to Noxu, and if so in what form?

> **Read this first.** This document analyses a change that would trade one of
> Noxu's measured *strengths* for one of its measured *weaknesses*. It is
> written to inform a decision, not to sell a feature. The recommendation
> (§9) includes "don't" and "narrow opt-in" as first-class outcomes.

---

## 1. Motivation and the honest tradeoff

### 1.1 What the benchmark actually said

A cross-engine benchmark put Noxu against WiredTiger (WT), BDB-JE, and
RocksDB/TidesDB. Two findings frame this proposal, and they point in
*opposite* directions:

| Workload | Noxu | WiredTiger | Verdict |
|---|---|---|---|
| **Pure read** (peak) | ~715K ops/s | ~2.8–3.4M ops/s | **Noxu ~4–5× behind** |
| **Mixed read/write** | **0 aborts** | ~10% conflict-abort tax | **Noxu wins 2–5×** |

The read gap is Noxu's *one structural weakness*. It is not a tuning artefact:
a lock-based read in Noxu pays a **per-record read lock** (`lock_ln` — see
`crates/noxu-dbi/src/cursor_impl.rs:1441`) plus **hand-over-hand shared latches**
down the B+tree. WiredTiger's reads are lock-free (MVCC snapshot + hazard
pointers), so they scale on read-heavy fan-out where Noxu's per-record lock and
latch traffic become the bottleneck.

But the mixed-workload result is the mirror image. Noxu's lock-based writer
serialization means a mixed workload commits **without aborts**: a writer that
would conflict *waits* (and deadlock detection resolves genuine cycles), rather
than optimistically racing and then aborting on a write-write conflict at
commit. WT's MVCC pays a ~10% conflict-abort tax on the same workload — every
aborted transaction is wasted work plus a client-side retry. On mixed load,
Noxu beats WT by 2–5×.

**MVCC would attack the weakness by sacrificing the strength.** A snapshot read
is lock-free (that is the entire point), so it closes the read gap. But MVCC's
natural companion — snapshot-isolation *writes* validated at commit — is exactly
what introduces the conflict-abort tax that Noxu currently does not pay. Even if
we keep lock-based writes and add *only* snapshot reads (which is the design we
actually recommend considering, §6a), we take on a large new subsystem — version
visibility and version garbage collection — whose failure mode is silent data
corruption.

### 1.2 The north star this touches

Noxu is a faithful Rust port of BDB-JE. Its north-star design elements are
deliberate:

- **Lock-based isolation, NOT MVCC** (`AGENTS.md`: "Isolation model: Lock-based,
  NOT MVCC. Writers lock BIN slots; readers block on write-locked records.")
- Log-structured B+tree, single WAL, checkpoint recovery.
- Faithful JE transliteration; no-async core.

MVCC is a **departure from JE** — JE has no MVCC (§4). Adopting it means
choosing a design element BDB-JE explicitly does not have. That is allowed
(BDB-C, the sibling engine, *does* have MVCC — §3), but it must be a conscious
choice, and the safest framing keeps the lock-based path as the default so the
north star stays intact for every workload that isn't read-bound (§7).

### 1.3 Is the read gap worth it?

The honest answer up front: **only for read-dominated deployments, and only via
the narrowest, most Noxu-native design.** The read gap is real but it is the
*only* structural gap, and Noxu already *wins* the workload most applications
actually run (mixed). Full SI-with-write-validation would be a bad trade. A
narrow, opt-in, snapshot-read-only feature that reuses the log Noxu already
has (§6a) is defensible *if* read throughput is a business requirement. The
even-lazier alternative — just make the existing lock-based read cheaper (§6c)
— may capture much of the same read win with none of the version-GC risk, and
is where this proposal ultimately points first (§9).

---

## 2. Background: how Noxu reads and writes today

### 2.1 The read path (the cost the benchmark measured)

Every positioned read in a locking cursor calls `lock_ln`
(`crates/noxu-dbi/src/cursor_impl.rs:1441`):

- Read-uncommitted: `lock(lsn, LockType::None, …)` — no real lock, but still
  runs `checkState`/`checkPreempted`.
- Serializable: `LockType::RangeRead` (phantom protection).
- Everything else: `LockType::Read`.
- Read-committed releases the read lock immediately after the operation;
  serializable holds it to commit (tracked in `Txn.read_locks`).

The lock is keyed on the LN's **LSN** (`crates/noxu-txn` — record-level locking
on LSN). Reaching the slot to get that LSN is a hand-over-hand shared-latch
descent through the INs/BIN. Two costs, both per-read:

1. A lock-manager hash lookup + grant (even when uncontended).
2. Shared-latch acquire/release on each tree level.

This is faithful to JE (`CursorImpl.lockLN`). It is also precisely what a
lock-free MVCC read avoids.

### 2.2 The write path — and the fact that old versions already exist

Noxu is **log-structured**. Every update **appends a new LN** to the single WAL;
the BIN slot is repointed at the new LSN; the old LN becomes *obsolete* and is
eventually reclaimed by the cleaner. This is the single most important fact for
this proposal:

> **Noxu already retains old versions.** Unlike an in-place engine (which
> overwrites the row and must *manufacture* an old version to do MVCC — exactly
> what BDB-C's page-versioning mpool does, §3), Noxu's old versions physically
> exist in the log until the cleaner removes them.

Each transactional LN log entry carries an `abort_lsn`
(`crates/noxu-log/src/entry/ln_log_entry.rs:110,265`): the LSN of the version
this write superseded (the before-image, used today for undo-on-abort;
`crates/noxu-txn/src/write_lock_info.rs:13`). The write path sets it to "the
current slot LSN before this write" (`cursor_impl.rs:3361`).

Two nuances that matter for feasibility (§6a):

- `abort_lsn` is the version as of *this txn's* first write to the record — it
  is the before-image for **undo**, not a general "previous committed version"
  pointer. But because *every* committed LN records the LSN it superseded, the
  `abort_lsn` links do form a chain back through the log: version *N*'s
  `abort_lsn` points at *N−1*, whose own `abort_lsn` points at *N−2*, and so on.
- The chain is only walkable while those older LNs still physically exist —
  i.e. while the **cleaner** has not reclaimed them. This is the crux of the
  version-GC coupling (§6a, §5.2).

### 2.3 How the cleaner already decides a version is dead

The cleaner declares an LN obsolete when the BIN slot no longer points at it:
`tree_lsn != log_lsn ⇒ Dead` (`crates/noxu-cleaner/src/file_processor.rs:2296`
and the `test_process_found_ln_dead_when_lsns_differ` case at line ~2303). In
other words, **the exact LNs the cleaner reclaims are exactly the old versions
an MVCC snapshot would want to read.** Today it is always safe to reclaim them
because no reader can see anything but the current slot version. Under MVCC that
stops being true, and that is the single biggest new coupling this proposal
introduces (§5.2).

---

## 3. Reference: how BDB-C (libdb) does MVCC

BDB-C *does* have MVCC, and it is the API/impl reference the design should be
measured against. Its scheme is **page-level, copy-on-write, in the buffer pool
(mpool)** — a good contrast to what Noxu could do.

### 3.1 The API surface

- `DB_MULTIVERSION` — env/db **open** flag; enables versioning for a file
  (`build_unix/db.h:2915`; `DB_ENV_MULTIVERSION` internal bit `db.in:2409`).
- `DB_TXN_SNAPSHOT` — **txn** flag; this txn reads a snapshot
  (`db.h:3011`; internal `TXN_SNAPSHOT 0x08000`, `db.in:957`).
- `DB_READ_COMMITTED` (= `DB_DEGREE_2`, `db.in:214`) — degree-2 isolation.
- `DB_TXN_SNAPSHOT_SAFE` / `TXN_SNAPSHOT_SAFE` (`db.in:961`) — serializable
  snapshot isolation (SSI).

### 3.2 How a snapshot resolves the visible version

The read view is a **read LSN** stored on the transaction detail:

```c
/* src/dbinc/txn.h:75 */
typedef struct __txn_detail {
    ...
    DB_LSN read_lsn;     /* Read LSN for MVCC. */
    DB_LSN visible_lsn;  /* LSN at which this transaction's changes are visible. */
    ...
};
```

On fetch (`src/mp/mp_fget.c`), if the file is multiversion and the txn is a
snapshot txn, BDB sets `read_lsnp = &td->read_lsn`, lazily filling it with the
current log LSN on first use (`mp_fget.c:173–186`). Then it walks the buffer's
**version chain** to the version visible at that read LSN
(`mp_fget.c:263–280`):

```c
/* Snapshot reads -- get the version visible at read_lsn. */
if (read_lsnp != NULL) {
    while (bhp != NULL &&
        !BH_OWNED_BY(env, bhp, txn) &&
        !BH_VISIBLE(env, bhp, read_lsnp, vlsn))
        bhp = SH_CHAIN_PREV(bhp, vc, __bh);
    if (bhp == NULL) { ret = DB_PAGE_NOTFOUND; goto err; }  /* created after snapshot */
}
```

Visibility is a single LSN comparison against each version's **commit LSN**
(`src/dbinc/mp.h:686`):

```c
#define BH_VISIBLE(env, bhp, read_lsnp, vlsn) \
    (bhp->td_off == INVALID_ROFF || \
    ((vlsn).file = VISIBLE_LSN(env, bhp)->file, \
     (vlsn).offset = VISIBLE_LSN(env, bhp)->offset, \
     LOG_COMPARE((read_lsnp), &(vlsn)) >= 0))
```

`visible_lsn` starts at `MAX_LSN` (uncommitted → invisible to everyone) and is
set to the commit LSN at commit/abort (`mp.h:674–690`). So: **a version is
visible iff `read_lsn >= version.commit_lsn`, or the reader owns it.** This is
textbook snapshot isolation and is directly portable in concept.

### 3.3 Copy-on-write and freezing

- A dirty write to a page owned by another txn triggers `makecopy`
  (`mp_fget.c:282`): BDB *copies the whole page* to create a new version,
  chaining the old one. This is the page-level cost Noxu would **not** pay in
  the log-native design (§6a) but **would** pay in the page-versioning design
  (§6b).
- Under memory pressure, old versions are **frozen** to temp storage rather than
  discarded (`__memp_bh_freeze`, `mp_mvcc.c`), because a snapshot might still
  need them. Noxu's analogue is "the cleaner must not reclaim the log file"
  (§5.2) — cheaper, because the version is already on disk in the WAL.

### 3.4 Version garbage collection

BDB reclaims old versions when no open snapshot can see them, using an
**oldest-reader LSN** low-water-mark. `mp_alloc.c` walks MVCC chains
(`mp_alloc.c:360`) and, when it needs a buffer, computes `oldest_reader` via
`__txn_oldest_reader` (`mp_alloc.c:391–400`) and frees versions older than it.
`BH_OBSOLETE` (`mp.h:692`) decides whether the *next* version being visible to
the oldest reader makes *this* one reclaimable.

**This is the exact shape of the mechanism Noxu already has** for two other
purposes: the cleaner's obsolescence test (§2.3) and replication's **CBVLSN**
(Cleaner Barrier VLSN, `crates/noxu-rep/src/group_service.rs:31`) — a global
minimum VLSN below which the cleaner must not reclaim. An MVCC "oldest open
snapshot read-LSN" is the same kind of low-water-mark, wired into the same
cleaner (§5.2, §6a).

---

## 4. Reference: JE is lock-based (the parent Noxu ports)

BDB-JE has **no MVCC**. Reads take read locks via the `LockManager`, and
`CursorImpl.lockLN` acquires a `LockType.READ`/`RANGE_READ` on the LN before
returning data — exactly what Noxu ports in `cursor_impl.rs::lock_ln` (§2.1).
JE's isolation levels (Serializable / RepeatableRead / ReadCommitted /
ReadUncommitted) are all **lock-degree** levels, not versioned snapshots. Noxu
mirrors this: `Locker.isSerializableIsolation` / `isReadCommittedIsolation`
(`crates/noxu-txn/src/locker.rs:198,205`), and the cursor's `LockType` selection
(§2.1).

**Consequence:** adding MVCC has *no JE precedent to transliterate.* The design
reference is BDB-C (§3) and WT, not JE. This is why the faithfulness posture
(§7) matters so much — MVCC is the point where Noxu would stop being "JE in
Rust" for the opted-in path.

---

## 5. Interaction with Noxu's invariants

### 5.1 Single WAL

No change to the log *format's* durability contract. The log-native design
(§6a) needs **no new log record type** for reads (a snapshot read-LSN is
in-memory only, §5.3). Writes still append new LNs with `abort_lsn` as today.
If we ever added SI *write validation*, we would need a conflict-detection
structure but still no new durable log record for reads.

### 5.2 The cleaner — the biggest new coupling (and the biggest risk)

Today: the cleaner reclaims any LN whose slot LSN no longer matches
(`tree_lsn != log_lsn`, §2.3). Under MVCC that LN might be **the version an open
snapshot must still read.** So the invariant becomes:

> **The cleaner must not reclaim a log file containing an LN version that is
> still visible to some open snapshot.**

The mechanism is a **snapshot read-LSN low-water-mark**: the minimum read-LSN
over all open snapshot transactions. The cleaner must retain any file whose LNs
could be reached by a snapshot at or above that mark. This is *structurally
identical* to the CBVLSN the cleaner already honours for replication
(`group_service.rs:31–38`, `getCBVLSN`), which exists precisely so "the log
cleaner must not remove log files at VLSNs ≤ CBVLSN." We would add a second
barrier (call it the **MVCC cleaner barrier**) and take `min(CBVLSN, MVCC
barrier)`.

Failure mode if this is wrong: **the cleaner reclaims a version a snapshot then
reads → read of freed/overwritten data → silent corruption.** This is where
databases lose data. It is the single most safety-critical piece and demands a
model check (§8).

### 5.3 Recovery — snapshots are in-memory, do not survive crash

Confirmed by the design's own shape and BDB precedent: a snapshot read-LSN lives
on the in-memory `Txn`, not in the log. On crash, all open transactions
(snapshot or not) are aborted/undone by checkpoint recovery exactly as today; no
snapshot state needs to survive, because a crashed reader has no client to
return to. **Recovery is unchanged.** The MVCC cleaner barrier is also in-memory
and simply resets to "no open snapshots" (fully permissive) after recovery,
which is safe because there are no open snapshots after a crash.

### 5.4 Replication (VLSN / feeder)

A snapshot **read** changes nothing about what is replicated. The feeder streams
committed log entries by VLSN; a reader resolving an old version from the log
neither writes nor commits, so it produces no VLSN and no feeder traffic. The
only interaction is the shared cleaner barrier (§5.2): the MVCC barrier and the
CBVLSN both constrain the cleaner, and the cleaner already takes a minimum over
such barriers. **No replication protocol change.**

### 5.5 Memory budget

Snapshot reads reuse the existing WAL + cache; the marginal memory cost is
retained *log files* the cleaner would otherwise have deleted (disk, not heap)
plus a small per-snapshot read-LSN and the barrier bookkeeping (heap, tiny).
This is far cheaper than BDB-C's page copies (§3.3) or the page-versioning
design (§6b), which hold whole extra pages in the cache and must account them in
`MemoryBudget`. The real budget concern is **disk**: long-lived snapshots keep
old log files un-reclaimed, growing on-disk footprint and lowering effective
utilization — the classic MVCC "long-running-reader bloats the store" problem,
here manifesting as "the cleaner falls behind." Mitigation: a max-snapshot-age
config that forces old snapshots to error (`SnapshotTooOld`) rather than pin the
log forever — exactly analogous to Postgres `old_snapshot_threshold`.

---

## 6. Design options

### 6a. Snapshot isolation via log-version reads (the Noxu-native option)

**Idea.** A snapshot txn records a **read-LSN** at first read (the current log
end, à la BDB `mp_fget.c:180`). A read resolves the visible version by starting
at the BIN slot's current LSN and walking `abort_lsn` links backward through the
log until it finds the newest version whose commit-LSN `<= read_lsn`. **No
copy-on-write, no page versioning** — it reuses the versions already in the log
(§2.2).

**Why this fits Noxu.** The versions physically exist (log-structured), the
visibility test is one LSN comparison (§3.2), the GC is a low-water-mark the
cleaner already knows how to honour (§5.2/CBVLSN). This is the design BDB-C
*couldn't* have because its store is in-place; Noxu gets it nearly for free
structurally.

**The hard parts — do not gloss these:**

1. **Commit-LSN of an old version.** BDB stores `visible_lsn` (commit LSN) per
   buffer version. Noxu's LN log entry does **not** today carry the *committing
   txn's commit LSN*; it carries the LN's own write LSN and its `abort_lsn`. To
   test `read_lsn >= version.commit_lsn` we need each committed version's commit
   point. Options: (i) resolve it from the txn's commit record (extra log reads
   on the read path — slow, defeats the purpose); (ii) add a commit-LSN/commit-
   VLSN stamp to the LN or maintain an in-memory committed-txn→commit-LSN map
   for recently committed txns (bounded by the barrier); (iii) approximate using
   the LN's own LSN, which is *close* but not exactly the commit point and can
   misjudge visibility for the window between a write and its commit. **This is
   the central design question of option 6a and must be resolved before
   implementation.** Getting it wrong is a visibility bug = wrong query results.
2. **`abort_lsn` is a before-image, not a clean version chain.** It points at
   the version as of the writing txn's first touch (§2.2). For a record updated
   many times *within one txn*, intermediate versions are not separately
   chained — but those are never visible to another snapshot anyway (they belong
   to an uncommitted txn), so the *committed* chain is what matters and it is
   walkable. This needs careful proof, not assertion (§8).
3. **Walk cost.** A read that must walk *k* old versions does *k* log reads
   (cache-resident if hot, disk if cold). For the read-heavy workload MVCC
   targets, most reads hit the current version (`k=0`, no walk) — the win is
   that even that read is **lock-free and latch-light**. Cold historical reads
   are slower, but that is the correct tradeoff (rare) and matches every MVCC
   engine.

**Public API sketch** (illustrative — NOT compiled engine code):

```rust
// Env open: opt in to versioning (default OFF — north star preserved).
let env = Environment::builder(path)
    .with_multiversion(true)          // DB_MULTIVERSION analogue
    .max_snapshot_age(Duration::from_secs(300)) // bound cleaner pinning
    .open()?;

// A snapshot (read-only) transaction: lock-free reads at a fixed read-LSN.
let snap = env.begin_txn(
    TxnConfig::new().isolation(Isolation::Snapshot) // DB_TXN_SNAPSHOT analogue
)?;
let db = env.open_database(&snap, "orders", DbConfig::default())?;
// Reads resolve the version visible at snap's read-LSN, no lock_ln, no per-record read lock.
let v = db.get(&snap, b"order-42")?;
// Writes under a snapshot txn are still possible but validated at commit
// (SI write-write conflict -> Err(Conflict)); OR restrict snapshot txns to
// read-only (recommended first cut -- avoids the abort tax entirely).
snap.commit()?;
```

> **Recommended first cut:** snapshot txns are **read-only**. This gives the
> lock-free read win with **zero** conflict-abort tax (there are no snapshot
> writes to validate), keeping Noxu's mixed-workload strength fully intact for
> the write path. SI writes can come later if ever needed.

**Effort:** medium-large. New: snapshot read path in the cursor (bypassing
`lock_ln`), version-walk-by-`abort_lsn`, commit-LSN resolution (the hard part),
the MVCC cleaner barrier + its wiring into `file_selector`/`file_processor`, the
config flags, and the model check (§8). Reuses: the log, the cleaner-barrier
pattern (CBVLSN), the `abort_lsn` chain.

### 6b. Page/BIN-level versioning (the BDB-C mp_mvcc.c approach)

Copy-on-write BIN versions in the cache, chained like BDB's buffer headers
(§3.3). A snapshot read walks the BIN version chain in the cache.

**Assessment: reject for Noxu.** This manufactures versions Noxu already has in
the log, doubling memory pressure (page copies in cache, accounted in
`MemoryBudget`), adding a freeze/thaw mechanism (`__memp_bh_freeze`) Noxu
doesn't need because its versions are already durable on disk, and importing the
`mprotect`-based sharing hazards (`MVCC_MPROTECT`, `mp.h:731`). It is the right
design for an in-place engine and the wrong one for a log-structured one. Listed
only for completeness and to justify choosing 6a over it.

### 6c. Keep lock-based, optimize the read path directly (the lazy alternative)

The benchmark's read gap is *largely `lock_ln` + hand-over-hand latches* (§2.1),
**not** the absence of versioning. So attack that directly, no MVCC:

- **Lock-free read-committed fast path.** For read-committed / read-uncommitted
  (which release the lock immediately or take none), skip the lock-manager hash
  round-trip entirely and read the slot under the shared latch, validating the
  slot LSN didn't change (optimistic). This is a much smaller change with **no
  version-GC risk** and no north-star departure — reads are still lock-based in
  semantics, just cheaper in mechanism.
- **Latch-lite descent** for point reads: reduce hand-over-hand overhead on the
  hot path (e.g. optimistic latch-coupling with a version/seqlock check on INs,
  a well-trodden B+tree technique).
- **Optimistic read validation** for repeatable-read: read without holding the
  per-record lock, take the lock only at commit / re-validate — closer to
  OCC than MVCC, keeps zero-version-GC.

**Assessment: strongest risk-adjusted option.** It targets the measured cost,
carries a fraction of MVCC's risk (no chance of reading a reclaimed version,
because there are no old versions to reclaim), and preserves the lock-based
north star entirely. It likely captures a large share of the read win. Its
ceiling is lower than true lock-free MVCC reads under extreme read fan-out, but
Noxu's actual gap may not need the full ceiling.

---

## 7. Faithfulness posture

MVCC has **no JE precedent** (§4). Two framings:

1. **BDB-C-inspired opt-in feature** (`with_multiversion(true)`, default OFF).
   The lock-based path remains the default for every environment that does not
   opt in — so the north star ("lock-based, NOT MVCC") stays true for the
   default engine, and MVCC becomes a *sibling-engine-inspired* capability the
   way BDB-C is a sibling of BDB-JE. This is the honest and almost-certainly-
   correct framing: it mirrors exactly how BDB-C exposes `DB_MULTIVERSION` (a
   per-env/db opt-in, not the default).
2. **North-star change** (MVCC becomes the default isolation substrate). **Do
   not recommend.** It would make Noxu no-longer-"JE-in-Rust," impose the design
   risk on *every* deployment including the mixed workloads Noxu wins today, and
   throw away the zero-abort property for users who never asked for MVCC.

The proposal adopts framing (1) unconditionally if MVCC is pursued at all.

---

## 8. Risk, test surface, and effort

**Highest-risk areas (in order):**

1. **Version GC too early** (§5.2): cleaner reclaims a version a snapshot then
   reads → **silent corruption / wrong results.** The catastrophic failure.
2. **Visibility misjudgement** (§6a.1): wrong commit-LSN comparison → a snapshot
   sees a version it shouldn't (or misses one) → **wrong query results,**
   possibly non-deterministic.
3. **`abort_lsn` chain assumptions** (§6a.2): if the walk ever lands on the wrong
   predecessor → wrong version returned.
4. **Cleaner-barrier / CBVLSN interaction** (§5.2): the two barriers must
   compose correctly (`min`), and neither may be starved by a stuck snapshot.

**Test surface (new):**

- **Stateright / shuttle model of version visibility + GC safety.** This is
  non-negotiable for MVCC. Model the two safety properties: (a) *no snapshot
  ever reads a version older than its read-LSN's visible set or newer than
  read-LSN*; (b) *the cleaner never reclaims a version reachable by an open
  snapshot* (the barrier is a correct low-water-mark). Noxu already has a spec
  culture (`noxu-spec`, `make spec`) including cleaner-safety and
  cache↔cleaner-ordering models — the MVCC barrier is a natural addition and
  should anchor to production types where possible (like the existing
  `lock_manager_deadlock → LockType` anchor).
- Property tests: random interleavings of writes + snapshot reads vs a
  reference single-version history oracle.
- Long-running-snapshot bloat + `SnapshotTooOld` expiry tests (§5.5).
- Crash-recovery tests confirming snapshots simply vanish (§5.3).

**Effort estimate (option 6a, read-only snapshots):** medium-large — dominated
by (i) the commit-LSN resolution design (§6a.1) and (ii) the model check.
Option 6c is **small-medium** with dramatically lower risk.

---

## 9. Recommendation

**Primary recommendation: do NOT add MVCC now. Pursue §6c (direct read-path
optimization) first.**

Reasoning:

- The read gap is Noxu's *only* structural weakness, but Noxu **wins the mixed
  workload most applications run** — and it wins it *because* it is lock-based
  (zero aborts vs WT's ~10% tax). MVCC's natural companion (SI write validation)
  would import that tax; even read-only snapshots take on a large,
  corruption-class-risk subsystem (version GC) to fix a gap that §6c can
  substantially close with a fraction of the risk and **no north-star
  departure.**
- The measured read cost is `lock_ln` + hand-over-hand latches (§2.1), not the
  absence of versioning. §6c attacks that cost directly. Measure §6c's win
  first; it may make the MVCC question moot.

**Secondary recommendation (only if §6c proves insufficient AND read throughput
is a hard business requirement): pursue §6a as a strictly opt-in,
read-only-snapshot feature** (`with_multiversion(true)`, default OFF, snapshot
txns read-only in the first cut). This is the most Noxu-native, lowest-risk MVCC
path: it reuses the log's existing versions, the `abort_lsn` chain, and the
CBVLSN cleaner-barrier pattern; it preserves the lock-based default (north star
intact); and by keeping snapshots read-only it takes the lock-free read win with
**zero** new abort tax. It must ship with the Stateright/shuttle GC-safety model
(§8) as a gating requirement.

**Reject: §6b (page versioning)** — wrong design for a log-structured engine.
**Reject: MVCC-as-default (§7 framing 2)** — throws away the mixed-workload win
for every user.

**Bottom line.** MVCC is where databases lose data, and Noxu is currently
*winning* the workload that matters most. The read gap is worth closing, but the
first tool for that job is a cheaper lock-based read (§6c), not a new
multi-version subsystem. Reserve MVCC (§6a, opt-in, read-only) for the specific
case where read throughput is a stated requirement and §6c has been shown to
fall short — and only with the safety model in place.

---

## Appendix: source references

| Claim | Source |
|---|---|
| Per-read lock cost | `crates/noxu-dbi/src/cursor_impl.rs:1441` (`lock_ln`) |
| LN carries `abort_lsn` (before-image) | `crates/noxu-log/src/entry/ln_log_entry.rs:110,265`; `crates/noxu-txn/src/write_lock_info.rs:13` |
| Write path sets `abort_lsn` = prior slot LSN | `crates/noxu-dbi/src/cursor_impl.rs:3361` |
| Cleaner: `tree_lsn != log_lsn ⇒ obsolete` | `crates/noxu-cleaner/src/file_processor.rs:2296` |
| CBVLSN cleaner barrier (low-water-mark precedent) | `crates/noxu-rep/src/group_service.rs:31–38` |
| Isolation is lock-degree (JE-faithful) | `crates/noxu-txn/src/locker.rs:198,205` |
| BDB read-LSN / visible-LSN | `~/ws/libdb/src/dbinc/txn.h:75` (`read_lsn`, `visible_lsn`) |
| BDB snapshot version walk | `~/ws/libdb/src/mp/mp_fget.c:263–280` |
| BDB visibility test | `~/ws/libdb/src/dbinc/mp.h:686` (`BH_VISIBLE`) |
| BDB copy-on-write | `~/ws/libdb/src/mp/mp_fget.c:282` (`makecopy`) |
| BDB version GC / oldest-reader | `~/ws/libdb/src/mp/mp_alloc.c:360,391`; `mp.h:692` (`BH_OBSOLETE`) |
| BDB API flags | `~/ws/libdb/build_unix/db.h:2915,3011,2941`; `src/dbinc/db.in:214,957,961,2409` |
| JE is lock-based (no MVCC) | `~/ws/je` — `LockManager`, `CursorImpl.lockLN` |
