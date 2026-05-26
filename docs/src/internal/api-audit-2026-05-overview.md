# Public API Audit — May 2026

A read-only behavioural audit of the Noxu DB public API surface,
focused on whether the implementation matches the published
documentation and whether each method honours the BDB / BDB-JE
semantics it is modeled on.  Triggered by two correctness bugs
fixed in v1.4.2 / v1.4.3 (Cursor `SearchGte` panic on short prefix,
silent `NotFound` on cross-BIN gap), both of which had been live in
the cursor public API since at least v1.4.0.  The premise of the
audit is that bugs of that shape rarely arrive alone.

This is an **independent companion** to two existing audits:

* `claim-audit-2026-05.md` — doc-vs-implementation drift (23
  findings, primarily missing-error-mode and stale-rationale).
* `security-review-2026-05.md` — security posture (memory safety,
  panic surface, input validation).

This audit is **behavioural correctness vs the BDB-JE contract**.
The findings rarely overlap with the prior two; they describe
configuration that is accepted but not consumed, methods that are
shaped right but wired wrong, and operations that look correct on
the BDB-shaped happy path but diverge under the contracts BDB
users would consider load-bearing (transactional cursors,
foreign-key actions, sorted-duplicate secondary indexes,
cross-restart durability).

---

## Scope

The audited surface is every public function in:

| Crate | Surface | Public fns (approx.) |
|---|---|---|
| `noxu-db` | `Environment`, `Database`, `Cursor`, `Transaction`, `Sequence`, `SecondaryDatabase`, `SecondaryCursor`, `JoinCursor` plus `*Config` / `*Stats` | 460 |
| `noxu-collections` | `StoredMap` / `StoredSortedMap` / `StoredKeySet` / `StoredValueSet` / `StoredList` / `StoredIterator` / `TransactionRunner` | 81 |
| `noxu-bind` | `EntryBinding` / `TupleBinding` / `SerialBinding` and the per-primitive bindings | 72 |
| `noxu-persist` | DPL (`EntityStore` / `PrimaryIndex` / `SecondaryIndex`) | 153 |
| `noxu-xa` | X/Open XA (`XaResource`, `Xid`, `PreparedLog`) | 14 |

`noxu-rep` (replication) was deliberately **excluded** from this
pass — its public surface is large and concerns a different blast
radius (HA / quorum), and merits its own audit phase.

The audit was a static, read-only review.  No code, configs, or
tests were modified.  No tests were executed.  No concurrent or
recovery paths were exercised live; concurrency findings are
reasoned from the source.

The reference archives at `_/je/` and `_/nosql/` were not
available in the audit environment, so cross-references to BDB-JE
behaviour rely on the well-documented public BDB / BDB-JE API
contract rather than line-level comparison with the reference
source.

---

## Methodology

For each public method or trait the auditing agent:

1. Read the rustdoc claim on the method.
2. Read the matching narrative in `docs/src/` (the published
   mdBook).
3. Read the implementation, following the call chain into
   `noxu-dbi`, `noxu-engine`, `noxu-txn`, `noxu-tree` as needed.
4. Cross-referenced against the BDB / BDB-JE contract for the
   equivalent operation.
5. Flagged each divergence with a severity classification:

   * **Critical** — silently wrong observable behaviour where a
     user following the published docs would lose data, lose
     transactional isolation, or land in an unrecoverable state.
   * **High** — silently wrong behaviour with smaller blast
     radius, or behaviour that contradicts the published docs but
     is not data-losing.
   * **Medium** — config-not-plumbed, missing surface, doc
     inaccuracy that misleads but does not break correctness.
   * **Low** — cosmetic, dead code, missing observability,
     polish.
   * **Info** — informational, not a defect.

Each finding carries a specific `file:line` citation so a
reader can verify it against the source and so a follow-up commit
can address it directly.

---

## Aggregate severity (~141 findings across 6 reports)

| Subsystem | Critical | High | Medium | Low | Info | Total |
|---|---:|---:|---:|---:|---:|---:|
| Cursor | 1 | 8 | 7 | 3 | 3 | 22 |
| Database CRUD | 0 | 5 | 9 | 8 | 1 | 23 |
| Transaction / Environment / Sequence | 0 | 3 | 9 | 11 | 1 | 24 |
| Secondary / Join | 3 | 4 | 8 | 4 | 1 | 20 |
| Collections / Bind | 0 | 8 | 13 | 10 | 0 | 31 |
| Persist / XA | 2 | 6 | 7 | 5 | 1 | 21 |
| **Total** | **6** | **34** | **53** | **41** | **7** | **141** |

> Severity counts are aggregated from the per-subsystem reports;
> see those reports for the authoritative classifications.  The
> Transaction / Environment numbers include the `MAJOR / MINOR`
> categories used in that report mapped to High / Low.

---

## The six Critical findings

These are the items that should be addressed before recommending
v1.4.x for new transactional / multi-record workloads, and ideally
before the next release.

### C1 — `Database::open_cursor(Some(&txn), …)` silently drops the txn

**File**: `crates/noxu-db/src/database.rs:648`
**Subsystem**: Cursor (#1 in `api-audit-2026-05-cursor.md`)

The signature accepts `txn: Option<&Transaction>` but the
parameter is named `_txn` and never read.  Cursor reads do not
take locks in the txn's lock set; cursor writes auto-commit.  The
rustdoc on the method honestly says "currently ignored", but
`docs/src/transactions/cursors.md` recommends `Some(&txn)` as the
canonical pattern for transactional iteration.  Users following
the mdBook lose transactional isolation silently.

### C2 — Foreign-key delete actions are unimplemented

**Files**: `crates/noxu-db/src/secondary_config.rs`,
`crates/noxu-db/src/secondary_database.rs`
**Subsystem**: Secondary / Join (F1)

`ForeignKeyDeleteAction` (`Abort` / `Cascade` / `Nullify`),
`foreign_key_database`, and the two nullifier traits are stored
on `SecondaryConfig` but never read by any code outside that
file.  `Database::delete` of a foreign-key referent does nothing
to the referencing primary.  The user-facing docs describe these
as supported.

### C3 — No `associate()`-style hook on `Database::put` / `delete`

**Files**: `crates/noxu-db/src/database.rs`,
`crates/noxu-db/src/secondary_database.rs`
**Subsystem**: Secondary / Join (F2)

In BDB-JE, primary writes automatically maintain registered
secondaries.  In Noxu the user must manually call
`update_secondary` after every primary write.  The rustdoc states
"On every primary `put` the secondary is updated via
`update_secondary`" — this is aspirational; `update_secondary`
is the user's responsibility.  `Database::put_and_update_secondaries`
is referenced by docs but does not exist.  Users who follow the
"primary write only" pattern get silent index drift.

### C4 — `insert_sec_key` uses `Put::Overwrite`

**File**: `crates/noxu-db/src/secondary_database.rs:441`
**Subsystem**: Secondary / Join (F3)

The canonical many-primary→one-secondary-key BDB use case
(several primary records sharing a secondary index value, e.g.
multiple users in the same city) silently loses all but the
most-recently-written primary, because the inner index is
written with `Put::Overwrite` rather than as a sorted-duplicate
entry.  The inline comment on the function admits the issue
(`"If the secondary allows duplicates, use NoOverwrite-equivalent
(NO_DUP_DATA)"`) and rationalises the current behaviour as
"safe for the fully-populated path" — which holds only for the
unique-key model.  `JoinCursor` cannot return BDB-shaped
equality-join cardinality on top of this.

### C5 — `xa_prepare` is not crash-durable end-to-end

**Files**: `crates/noxu-xa/src/resource.rs`,
`crates/noxu-xa/src/prepared_log.rs`,
`crates/noxu-txn/src/txn.rs:70`
**Subsystem**: Persist / XA (#1)

`xa_prepare` records the XID in a fsync'd `PreparedLog` but
never tells the underlying `noxu-db::Transaction` that the
branch is prepared.  No `TxnPrepare` log record is emitted,
`Transaction::prepare()` does not exist (a stale `IS_PREPARED`
constant is unused), and on `Environment::open` after a crash
recovery rolls the txn back unconditionally.  `xa_recover`
returns the XID but `xa_commit(xid)` then fails with
`XaError::NotFound` because the in-memory branch map is empty.
Two-phase commit is therefore non-functional across a crash.

### C6 — Persist primary writes cannot participate in a user transaction

**Files**: `crates/noxu-persist/src/primary_index.rs`,
`crates/noxu-persist/src/secondary_index.rs`
**Subsystem**: Persist / XA (#10, #11, #18)

`PrimaryIndex::put` always calls `db.put(None, …)`.  Combined
with secondary indexes that are in-memory only, this means an
entity write cannot be made atomic with anything else under DPL
— a critical departure from BDB-JE, where DPL writes are
expected to participate in `EntityStore.beginTransaction()`'s
txn and where secondary index maintenance is atomic with the
primary write.

---

## Cross-cutting themes

The 141 individual findings collapse onto a small number of
recurring patterns:

### Theme 1 — "Config accepted but never read"

By far the most common pattern.  Config fields and builder
methods exist on the public API, the rustdoc describes them as
supported, but nothing in the implementation reads them.

Concrete instances:

* `EnvironmentConfig::durability` is never consulted on commit
  (Transaction-Env F3).
* `TransactionConfig::read_uncommitted` silently dropped
  (Transaction-Env F2).
* `CheckpointConfig` accepted but ignored (Transaction-Env F6).
* `EnvironmentMutableConfig::set_*` writes only to the local
  config and never pushes to the live subsystems
  (Transaction-Env F7).
* `Durability::replica_sync` / `replica_ack` unused
  (Transaction-Env F8).
* `LockMode::Rmw` and `ReadCommitted` are no-ops in
  `get_with_options` (Database #?).
* `DatabaseConfig::override_btree_comparator`, `key_prefixing`,
  `replicated`, `bin_delta`, `cache_mode`, `exclusive`,
  `use_existing_config` stored but never plumbed (Database).
* `SecondaryConfig`: `immutable_secondary_key`,
  `extract_from_primary_key_only`,
  `update_may_change_secondary` are config sinks
  (Secondary F8, F9).
* `CursorConfig`: `read_committed`, `non_sticky`, `evict_ln`,
  `prefix_constraint` are never read (Cursor F20).

This pattern is dangerous because the user has no way to detect
the no-op except by reading the implementation.  A consistent
remediation is needed: either wire each field in, or reject
unknown values at config-build time with a clear error.

### Theme 2 — "Transaction not threaded"

Several public methods take a `txn: Option<&Transaction>`
argument that the implementation either drops on the floor or
forwards only partially:

* `Database::open_cursor(Some(&txn), …)` drops `txn` (Cursor C1).
* `SecondaryDatabase::open_cursor` drops both `txn` and `config`
  (Secondary F4).
* `SecondaryDatabase::delete` only forwards `txn` to the primary;
  the secondary cleanup runs auto-commit (Secondary F5).
* `EnvironmentImpl::truncate_database` ignores its `txn`
  parameter (Database F?).
* `Transaction::begin` accepts a `parent: Option<&Transaction>`
  for nested transactions and silently drops it
  (Transaction-Env F11).
* Auto-commit writes bypass the lock manager entirely, so they
  do not isolate against explicit transactions
  (Transaction-Env F12).
* `PrimaryIndex` writes (DPL) always pass `None`
  (Persist #10, #18).
* All `Stored*` collection operations hard-code `None`
  (Collections-Bind #1).

This is the most concentrated cluster of correctness risk.  A
user who reads the docs and threads `Some(&txn)` through their
code reasonably expects ACID isolation; the current
implementation gives them auto-commit + per-op locking, which
in many cases is observably wrong (e.g. read-then-write race
windows that the user believed they had eliminated).

### Theme 3 — "Sorted-dup gaps"

The sorted-duplicates path is silently incomplete:

* `Database::count()` returns 0 because `put_dup` bypasses the
  entry counter (Database).
* `Database::delete(key)` deletes only one of N duplicates
  instead of all (Database).
* `Get::NextDup` / `Get::PrevDup` degenerate into plain
  `Next` / `Prev` on a non-dup database — there is no error to
  warn the user (Cursor F4).
* `Get::SearchBoth` ignores the user-supplied data on a non-dup
  database (Cursor F5).
* Secondary indexes use `Put::Overwrite` instead of duplicate
  insertion (C4).

### Theme 4 — "Doc shows API that doesn't exist"

The published mdBook describes APIs the implementation never
shipped:

* `env.open_secondary(...)` (used in `secondary-with-txn.md`).
* `SecondaryConfig::with_transactional(true)`
  (used in `secondary-with-txn.md`).
* `Database::put_and_update_secondaries` (referenced in
  `database.rs` rustdoc).
* Typed `StoredMap<K,V>` / `StoredSet<K>` / `StoredList<V>`
  parameterised by bindings (the actual API is `&[u8]`-keyed).
* `Cursor::open` semantics around an explicit `txn` argument
  (`docs/src/transactions/cursors.md`).

### Theme 5 — "Cross-restart durability gap"

* XA prepare durability is broken end-to-end (C5).
* `WriteOptions::ttl` writes are not durable
  (Database).
* `StoredList::next_index` is a process-local `Mutex<usize>`
  that resets to 0 on reopen, so pushes after restart will
  overwrite existing records (Collections-Bind #6).
* `SerdeBinding` / `simple_serial` lacks schema versioning, so
  adding/removing/reordering a struct field silently corrupts
  on-disk records (Collections-Bind #19).
* DPL `EntityStore::evolve` is non-transactional, hardcodes
  class version 0, and materializes the whole DB into RAM
  (Persist #12, #13, #16).
* DPL secondary indexes are in-memory only (Persist #11).

### Theme 6 — Confusion between "v1.4.x" and "BDB-JE"

The published docs (rustdoc + mdBook) consistently describe
the BDB-JE feature set, but several of those features are
explicitly out of scope for the current implementation (e.g.
nested transactions, foreign-key actions, sorted-dup
secondaries, durable XA).  Either the implementation should
match the docs, or the docs should be qualified with
"unsupported in v1.4.x" markers.  The current state silently
misleads users who reach for those features.

---

## Per-subsystem report index

| Report | Findings | Headline |
|---|---|---|
| [Cursor](api-audit-2026-05-cursor.md) | 22 (1C/8H) | `open_cursor(Some(&txn))` drops txn; cursor put corrupts position; three documented `Get` variants fall through to NotFound; `NextDup` is plain `Next` on non-dup |
| [Database CRUD](api-audit-2026-05-database.md) | 23 (5H) | `count()` returns 0 on sorted-dup; `delete(key)` removes only one of N dups; `LockMode::Rmw` / `ReadCommitted` are no-ops; partial-put under txn self-deadlocks; many `DatabaseConfig` fields are config sinks |
| [Transaction / Env / Sequence](api-audit-2026-05-transaction-env.md) | 24 (3H) | `env.close()` errors after `txn.commit()` (active_txns leak); `EnvironmentConfig::durability` ignored on explicit-txn commit; `read_uncommitted` silently dropped; auto-commit writes bypass lock manager |
| [Secondary / Join](api-audit-2026-05-secondary-join.md) | 20 (3C/4H) | Foreign-key actions unimplemented; no `associate()` hook; secondary uses `Put::Overwrite`; `SecondaryDatabase::open_cursor` drops txn |
| [Collections / Bind](api-audit-2026-05-collections-bind.md) | 31 (8H) | `Stored*` API hard-codes auto-commit; `StoredList::next_index` non-persistent; `SerdeBinding` no schema versioning; `TransactionRunner` cannot thread its txn |
| [Persist / DPL / XA](api-audit-2026-05-persist-xa.md) | 21 (2C/6H) | XA prepare not durable across crash; `PrimaryIndex` writes always `None` txn; DPL secondaries in-memory only; `EntityStore::evolve` non-transactional |

---

## What is *not* covered

* **`noxu-rep`** (replication public surface) — explicitly
  out of scope for this audit pass.  Recommended for a
  follow-up phase.
* **Concurrent / racy paths** — every report notes that no
  concurrent execution was performed.  The cursor and txn
  reports flag potential issues by code reading; live
  exercising is needed.
* **Recovery / crash paths** — only the XA path was reasoned
  about end-to-end (and found broken).  General Environment
  recovery, cleaner safety, and replica-restore were not
  exercised live.
* **Oversized inputs** — no fuzz against billion-byte keys,
  saturated cache, or dirty-disk conditions.
* **The reference archives** (`_/je/`, `_/nosql/`) — not
  available in the audit environment.  Cross-references rely
  on documented BDB / BDB-JE contracts rather than line-level
  diff.

---

## Recommended next steps

In rough priority order:

1. **Address C1** — `Database::open_cursor(Some(&txn), …)` is
   the cheapest critical to fix and the most user-visible
   (the published mdBook recommends it).  Either route through
   `make_cursor_for_txn()` or remove the `_txn` parameter and
   update the docs.

2. **Address C5** — XA prepare durability is the highest-
   impact silent failure (data loss across a TM-coordinated
   crash).  Either implement the prepared-state log record
   and recovery path, or document XA as "in-process only" and
   reject `xa_prepare` calls when the underlying environment
   is not configured for crash-durable prepare.

3. **Address Theme 2** systematically — every method that
   takes a `txn` parameter must either thread it correctly or
   drop the parameter and document the auto-commit semantics.
   Mixed behaviour silently breaks ACID.

4. **Address C2 / C3 / C4** as a single workstream — the
   secondary-database surface needs a coherent decision: do
   we ship sorted-dup secondaries (BDB-JE shape) or commit to
   one-to-one and rename / re-document the feature?  Without
   that decision the rest of the secondary surface is
   undefined.

5. **Address Theme 1 (config-not-plumbed)** with a single
   pattern: each `*Config` builder method either wires the
   value into a runtime decision or rejects it at
   construction time.  No silent config sinks.

6. **Reconcile the published mdBook with shipped reality**
   (Theme 4).  Several user-facing examples reference APIs
   that don't exist; new users hit these first.

7. **Add the regression tests** flagged in each report's
   "Coverage gaps" section.  The pattern from v1.4.3 (write a
   property test against a brute-force oracle) caught two
   bugs the deterministic tests missed; the same approach
   should be applied to: secondary index sync after primary
   put, XA round-trip after recovery, transactional cursor
   isolation, sorted-dup count semantics.

8. **Plan the `noxu-rep` audit phase** before recommending
   replicated deployments for new workloads.

---

## Audit limits — please read

This audit is a **reading audit**, not an exercising audit.
Where the source is consistent and the BDB contract is
well-known, the findings are high-confidence.  Where the
behaviour depends on concurrent timing, recovery edge cases,
or the actual contents of the reference source we did not
have access to, findings are reasoned-from-source — credible
but not proven.  Each finding's "Recommendation" should be
treated as a starting point: implement the fix, then write
the failing-then-passing test that demonstrates the bug
existed.  v1.4.3's `cursor_search_gte_oracle_brute_force_*`
test is the model — it caught bugs that deterministic
scenario tests missed.

The auditing agents were instructed to be honest about audit
limits in each report; those sections should be consulted in
parallel with the findings.

---

*Audit performed 2026-05-25 against the tip of
`fix/cursor-search-gte-cross-bin-walk` (v1.4.3).
Aggregated from six per-subsystem reports written by parallel
read-only auditing agents.*
