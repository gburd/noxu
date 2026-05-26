# API Audit — `noxu-persist` and `noxu-xa` (May 2026)

## Scope

Read-only audit of two higher-level subsystems sitting on top of the core
Noxu engine:

* **`noxu-persist`** — Direct Persistence Layer (DPL): trait-based entity
  store with primary and secondary indexes, schema-evolution mutations, and
  sequences.  Source under `crates/noxu-persist/src/`.
* **`noxu-xa`** — X/Open XA two-phase commit: `Xid`, flags, `XaResource`
  trait, `XaEnvironment` implementation, and `PreparedLog` for crash
  recovery.  Source under `crates/noxu-xa/src/`.

Companion documentation reviewed:

* `docs/src/collections/entity-persistence.md`
* `docs/src/transactions/xa-distributed.md`

The audit cross-references the implementation against BDB-JE DPL semantics
(`EntityStore`, `PrimaryIndex.put` / `putNoOverwrite`, `SecondaryIndex`,
`@PrimaryKey` / `@SecondaryKey` annotation contract, mutation evolution) and
against the X/Open XA specification (xa_open / xa_close / xa_start / xa_end /
xa_prepare / xa_commit / xa_rollback / xa_recover / xa_forget; XID format;
durability of the prepared state across a crash).

No code, configuration, or tests were modified.

## Methodology

1. Enumerated every public item in the two crates by reading `lib.rs`,
   the module roots, and grepping for `pub fn` / `pub struct` / `pub trait`.
2. Read all rustdoc on the public surface plus the two mdBook chapters.
3. Read the implementation of every public method.  Followed cross-crate
   calls into `noxu-db` / `noxu-txn` only as far as needed to verify
   semantic claims (e.g. `Database::put(None, …)` durability).
4. Inspected the `tests/` integration suites for both crates
   (`xa_protocol_test.rs`, `xa_chaos_test.rs`, `xa_adversarial_test.rs`,
   `noxu_persist_tests.rs`, `integration_tests.rs`) to understand what is
   actually tested vs. only documented.
5. Compared each finding against the BDB-JE / X/Open XA reference contract.

Severity legend used in the table below:

| Severity | Meaning |
|---|---|
| **CRITICAL** | The implementation does not deliver a guarantee its API/docs claim, in a way that can silently corrupt or lose committed data. |
| **MAJOR** | A documented or expected feature is unimplemented or substantially divergent; correctness suffers in realistic scenarios. |
| **MEDIUM** | Behaviour is technically correct for in-process use but violates the spirit of the BDB-JE / XA contract, or causes scaling / footgun problems. |
| **MINOR** | Inconsistency, dead code, swallowed error, or ergonomics issue. |
| **INFO** | Acknowledged limitation or stylistic note; no fix required. |

## Findings table

| # | Crate | Severity | Area | One-line summary |
|---:|---|---|---|---|
|  1 | xa | CRITICAL | `xa_prepare` durability | The underlying noxu-db `Transaction` is **not** prepared/durably staged before `xa_prepare` returns. After a crash, recovery rolls the txn back, so a recovered XID has no surviving data to commit. |
|  2 | xa | CRITICAL | `xa_commit` / `xa_rollback` after restart | After an env reopen, prepared XIDs survive only in `PreparedLog`; they are **not** re-instantiated in the in-memory `branches` map.  `xa_commit(xid)` / `xa_rollback(xid)` therefore fail with `NotFound`, leaving recovered XIDs unresolvable. |
|  3 | xa | MAJOR | `mark_write` footgun | `xa_prepare` only treats a branch as writable if `XaEnvironment::mark_write` was called.  A caller that performs writes via `db.put(Some(txn), …)` but forgets `mark_write` silently gets `PrepareResult::ReadOnly` and the writes are aborted. |
|  4 | xa | MAJOR | XID ordering vs. data durability | `PreparedLog::record_prepare` is fsync'd via auto-commit, but no ordering / barrier ensures the *transaction's* data writes are durable before the prepared marker is durable.  Combined with #1 this is moot, but it is the second half of the same problem and would matter for a fixed implementation. |
|  5 | xa | MEDIUM | `xa_recover` flag handling | `STARTRSCAN` / `ENDRSCAN` are accepted but ignored (`environment.rs:271`); the entire prepared list is returned on every call.  Acceptable as a simplification, but undocumented — TM code that calls multiple times to paginate will see duplicates. |
|  6 | xa | MEDIUM | Soundness of `get_transaction` | Returns `&Transaction` derived from a raw pointer (`environment.rs:88`) outliving the `branches` lock.  Documented invariant relies on caller never racing `xa_rollback` from another thread on the same XID; sound only under that contract. |
|  7 | xa | MINOR | Swallowed errors | `xa_recover` swallows `recover_all()` errors with `if let Ok(persisted)` (`environment.rs:284`); `xa_forget` does the same (`environment.rs:303`).  A failing prepared-log read silently reports an empty / inconsistent set. |
|  8 | xa | MINOR | XAER_RBROLLBACK family unmapped | `xa_end(TMFAIL) → RollbackOnly` then `xa_prepare` returns `XaError::Protocol`, not the spec's `XAER_RB*` family; downstream TMs cannot tell "branch ready to rollback" from a generic protocol violation. |
|  9 | xa | MINOR | `XaResource` trait omits `mark_write` | `mark_write` is an inherent method on `XaEnvironment`, not part of the trait, so polymorphic TM code cannot mark writes through `dyn XaResource`. |
| 10 | persist | CRITICAL | Atomicity of secondary updates | `PrimaryIndex::put` always uses `db.put(None, …)` (auto-commit) and only afterwards notifies in-memory secondary maintainers.  A crash between the two leaves the on-disk primary written and the in-memory secondary unupdated — but since secondaries are in-memory only, every restart is "post-crash" relative to them.  No user transaction can be threaded through. |
| 11 | persist | MAJOR | Secondary indexes not persisted | `SecondaryIndex` is a `BTreeMap<SK, BTreeSet<PK>>` behind `Arc<Mutex<…>>`.  Restart loses all secondary state; the `PrimaryIndex` does not rebuild it.  Documented in `entity-persistence.md` "Limitations" but a major divergence from BDB-JE `SecondaryDatabase`. |
| 12 | persist | MAJOR | `EntityStore::evolve` is non-transactional | `evolve_database` (`entity_store.rs:268`) reads every record into memory, then issues `db.put(None, …)` / `db.delete(None, …)` per record.  An interrupted evolve leaves the store in a half-converted state with no rollback. |
| 13 | persist | MAJOR | Schema-version sentinel hardcoded | `evolve_database` looks up mutations only at `class_version = 0` (`entity_store.rs:294`) with an inline comment that "a full implementation would store the schema version alongside each record".  Per-record class versioning is **not** implemented. |
| 14 | persist | MAJOR | `DatabaseNamer` is dead code | `DefaultDatabaseNamer` produces `persist#Store#Entity`, `CustomDatabaseNamer` is configurable, both are exported from `lib.rs:63-65`, but `EntityStore::get_primary_index` hardcodes `format!("{}_{}", store_name, entity_name)` (`entity_store.rs:99`).  The trait is never plumbed through the store. |
| 15 | persist | MEDIUM | `KeySelector` family is dead code | `AllKeysSelector`, `RangeKeySelector`, `PredicateKeySelector`, `SetKeySelector`, `NotKeySelector` are exported (`lib.rs:70-73`) but no public method on `PrimaryIndex` / `SecondaryIndex` / `EntityIterator` accepts a `KeySelector`.  Pure docs/test surface. |
| 16 | persist | MEDIUM | `evolve_database` reads everything into RAM | `db.scan_all_kv()?` materialises the entire database before iterating (`entity_store.rs:289`).  OOMs on large stores; comment ("the public Cursor API does not expose key bytes during iteration") explains the workaround but not its cost. |
| 17 | persist | MEDIUM | "MANY_TO_MANY" claim is incorrect | `secondary_index.rs:78-86` documents support for "MANY_TO_MANY"; the reverse map is `BTreeMap<PK, SK>` (singular SK), so a single primary key can map to at most one secondary key per index.  MANY_TO_ONE works; MANY_TO_MANY in the BDB-JE sense does not. |
| 18 | persist | MEDIUM | `PrimaryIndex::put` cannot use a user transaction | All write paths use `db.put(None, …)` / `db.delete(None, …)`.  Docs in `secondary-with-txn.md` (chapter exists) cannot apply to entities — there is no API to thread a `Transaction` through the entity layer. |
| 19 | persist | MINOR | `Sequence::new` re-reads the limit, not the current | `Sequence::with_cache_size` reads the persisted *limit* (`sequence.rs:87`) and starts the in-memory counter at it, then writes `limit + cache_size`.  After a normal restart this burns one cache window of IDs.  Monotonic but creates gaps; the doc-comment "starts from 1" omits this detail. |
| 20 | persist | MINOR | Unused error variants | `PersistError::EntityNotFound`, `DuplicateKey`, `StoreAlreadyOpen`, `InvalidEntity` are defined and have Display tests but are never constructed by production code (`error.rs:16-37`). |
| 21 | persist | MINOR | `SimpleSerializer` doc claims "no schema evolution mechanism" but evolve module exists | The simple_serializer comment (`simple_serializer.rs:6`) says it's "suitable for applications that do not need a schema evolution mechanism", which contradicts the existence of `noxu-persist::evolve` operating at the byte level.  Cosmetic. |

## Detailed findings

### XA — Finding 1 (CRITICAL): `xa_prepare` does not durably prepare the underlying transaction

`XaEnvironment::xa_prepare` (`crates/noxu-xa/src/environment.rs:188-217`)
performs only three actions when the branch has writes:

1. Calls `PreparedLog::record_prepare(xid)` which writes `xid → timestamp`
   into the `_xa_prepared` hidden database via `self.db.put(None, …)`
   (auto-commit, fsync'd by `Database::auto_commit_sync`).
2. Sets `branch.state = BranchState::Prepared`.
3. Returns `PrepareResult::Ok`.

Critically, **the underlying `noxu_db::Transaction` is left in its normal
in-flight state**.  No `prepare()` is invoked on it, and grep across the
workspace confirms no such method exists on `Transaction` or the inner
`Txn`:

* `crates/noxu-txn/src/txn.rs:70` defines `const IS_PREPARED: u8 = 1;`
  but the constant is **unused** anywhere in the workspace.
* No `pub fn prepare` exists in `noxu-txn`, `noxu-dbi`, or `noxu-db`.
* No `prepared` / `in-doubt` handling exists in `noxu-recovery`.

Consequence: on a crash between `xa_prepare` and `xa_commit`, recovery
will see the underlying transaction as "not committed" and roll back its
write set.  When the operator subsequently calls `xa_recover`, the
`PreparedLog` returns the XID, but the data the TM wants to commit is
already gone.

The X/Open XA specification (§6.2 *xa_prepare*) requires the RM to
"force the log records [needed to commit or rollback] to durable storage
before returning."  The current implementation forces a separate
*marker*, not the transaction's own log records relative to a prepared
state.  Auto-commit does fsync the log up through the marker write
(via `Database::auto_commit_sync`), so the transaction's data writes
do happen to be on disk — but recovery will undo them because nothing
tells it the txn was prepared.

The integration test `tests/xa_adversarial_test.rs:74-113`
(`test_crash_recovery_prepared_log_persists`) verifies that
`xa_recover` returns the prepared XIDs after a simulated crash, but
**no test calls `xa_commit(xid)` on a recovered XID and verifies the
data survived**.  Adding such a test would expose this bug.

### XA — Finding 2 (CRITICAL): `xa_commit` / `xa_rollback` cannot resolve recovered XIDs

`xa_commit` (`environment.rs:219-247`) starts with:

```rust
let branch = branches.get_mut(xid).ok_or(XaError::NotFound)?;
```

After an environment reopen, `branches` is empty.  Even though
`PreparedLog::recover_all` can reconstitute the XID list, no code path
re-creates the corresponding `Branch { state: Prepared, txn: …, … }`
entries in the in-memory map.  The `Box<Transaction>` is gone, so
`xa_commit` and `xa_rollback` both return `XaError::NotFound`.

The doc workflow (`docs/src/transactions/xa-distributed.md`,
"Recovery workflow", lines 138-153) instructs callers to do exactly
this:

```rust
let prepared_xids = xa.xa_recover(XaFlags::STARTRSCAN).unwrap();
for xid in &prepared_xids {
    xa.xa_commit(xid, XaFlags::NOFLAGS).unwrap();
}
```

That `unwrap()` will panic with `XaError::NotFound` against the current
implementation.  The only operation that does work post-crash is
`xa_forget`, which intentionally consults the prepared log on the
NotFound path (`environment.rs:296-308`).

### XA — Finding 3 (MAJOR): `mark_write` footgun

The `XaResource` trait does not include `mark_write`; it is an
inherent method on `XaEnvironment` (`environment.rs:96-101`).  The
read-only optimisation in `xa_prepare` (`environment.rs:201-211`):

```rust
if !branch.has_writes {
    let _ = branch.txn.abort();
    branches.remove(xid);
    return Ok(PrepareResult::ReadOnly);
}
```

aborts the transaction and removes the branch unconditionally when
`has_writes == false`.  A caller that performs writes via the natural
API — `db.put(Some(xa.get_transaction(&xid)?), …)` — but forgets to
also call `xa.mark_write(&xid)` will silently see all of those writes
discarded, with `Ok(PrepareResult::ReadOnly)` returned.  No XA spec
analogue exists; in BDB-JE the resource manager tracks write set on
its own.

### XA — Finding 4 (MAJOR): no ordering between prepared marker and txn data

Independently of #1, `xa_prepare` writes the prepared marker via
`Database::put(None, …)` which calls `auto_commit_sync` and fsyncs the
log up through that put's LSN.  The transaction's own writes have
LSNs *earlier* than the marker, so they are durable by the time the
marker is.  This is benign for now, but if #1 is fixed (e.g. by adding
a real `Transaction::prepare()` that writes a TxnPrepare log entry and
fsyncs), this implicit ordering must be made explicit.

### XA — Finding 5 (MEDIUM): `xa_recover` flag handling is a no-op

`xa_recover` (`environment.rs:269-292`) ignores `_flags`:

```rust
fn xa_recover(&self, _flags: XaFlags) -> XaResult<Vec<Xid>> {
```

Per X/Open §6.2, `STARTRSCAN` should rewind the cursor and `ENDRSCAN`
should release it; calling without `STARTRSCAN` should resume.  The
current implementation always returns the full list.  TMs that paginate
recovery will see duplicates with no way to detect end-of-scan.

### XA — Finding 6 (MEDIUM): `get_transaction` raw-pointer soundness

`get_transaction` (`environment.rs:74-94`) acquires the branches mutex,
takes a raw pointer to the boxed `Transaction`, drops the guard, and
returns a `&Transaction` synthesised from the pointer with lifetime
`&self`.  The doc-comment correctly enumerates the obligation: callers
must not concurrently `xa_rollback` / `xa_commit` the same XID from
another thread.  The XA spec does forbid this, so the soundness
contract is reasonable, but it is unenforceable from Rust's type
system and the `unsafe` is currently the only way to expose the txn to
application code.  Listed in `AGENTS.md` as the documented `unsafe`
block.

### XA — Finding 7 (MINOR): swallowed errors

`xa_recover` (`environment.rs:283-289`):

```rust
if let Ok(persisted) = log.recover_all() {
    for xid in persisted { … }
}
```

silently drops errors from the cursor scan over `_xa_prepared`.  Same
in `xa_forget` (`environment.rs:301-303`) via `.unwrap_or_default()`.
A storage-level read failure is reported as an empty list.

### XA — Finding 8 (MINOR): XAER_RB* family not modeled

`xa_end(TMFAIL)` puts the branch in `RollbackOnly`.  The next
`xa_prepare` returns `XaError::Protocol(...)` (`environment.rs:194-197`)
rather than mapping to one of the spec's `XAER_RB*` codes.
`XaError::HeuristicCommit` / `HeuristicRollback` are defined in
`error.rs:32-37` but never returned anywhere in the implementation.

### XA — Finding 9 (MINOR): `mark_write` not on `XaResource` trait

Defined as `impl XaEnvironment` (`environment.rs:96-101`) only.
Polymorphic `&dyn XaResource` cannot mark writes; this is a usability
issue more than a correctness one.

### Persist — Finding 10 (CRITICAL): no atomicity primary↔secondary

`PrimaryIndex::put` (`primary_index.rs:138-175`) does:

1. `let old_entity = self.get(serializer, entity.primary_key())?;`
   — auto-commit get.
2. `self.db.put(None, &key_entry, &data_entry)?;` — auto-commit put.
3. `for m in &self.secondaries { m.on_put(old_entity.as_ref(), entity); }`
   — purely in-memory mutation.

There is no transaction option; `Database::put` is called with `None`
unconditionally.  Step 3 is a `Mutex<BTreeMap>` update, not a database
write, so it is "atomic" only in the trivial sense.  A crash between
steps 2 and 3 is unobservable because step 3's state is gone after
restart anyway (Finding 11).  The architectural problem is that there
is **no API by which a user could thread an outer
`noxu_db::Transaction` through `PrimaryIndex::put` to make the primary
write transactional with anything else** (e.g. with another DPL store
or with a non-DPL `Database::put`).

This is a critical divergence from BDB-JE, where `PrimaryIndex.put`
accepts a `Transaction` and the secondary maintenance is automatic
under that transaction.

### Persist — Finding 11 (MAJOR): secondary indexes are in-memory only

`SecondaryMap<SK, PK>` (`secondary_index.rs:71-89`) is a `BTreeMap`
plus reverse map, wrapped in `Arc<Mutex<…>>`.  No on-disk
representation exists.  `EntityStore::open` does not iterate the
primary database to rebuild secondaries.  Therefore on every process
restart the secondary indexes are empty until the application
re-issues a `put` for each entity (which never happens automatically).

`docs/src/collections/entity-persistence.md` ("Limitations and
roadmap") acknowledges this:

> Secondary indexes are in-memory `BTreeMap`s rebuilt by the
> `PrimaryIndex` registration. They are not persisted independently.

But the chapter elsewhere ("Secondary indexes" section) describes
behaviour identical to BDB-JE `SecondaryDatabase`, which is misleading
for a reader who skims past the limitations list.

### Persist — Finding 12 (MAJOR): `EntityStore::evolve` is non-transactional

`evolve_database` (`entity_store.rs:268-323`) iterates every key/value
pair returned by `Database::scan_all_kv()` and rewrites them via
`db.put(None, …)` or `db.delete(None, …)`.  No outer transaction wraps
the conversion.  An evolve interrupted by a crash, panic, or
out-of-disk leaves the store in a state where some records have been
converted to the new format and others have not.  Re-running evolve
will not detect the partial state because the converter sees raw
bytes and is expected to handle every input.

### Persist — Finding 13 (MAJOR): per-record class version not stored

`evolve_database` (`entity_store.rs:294`):

```rust
let cm = mutations.get_mutations_for_class(entity_class, 0);
```

The class version is hardcoded to `0`.  The function-level comment
states:

> Version 0 is used as the sentinel "current schema version" for
> eager evolution because the store does not currently persist
> per-record version metadata.  A full implementation would store
> the schema version alongside each record and look up mutations by
> that version.

This is a significant divergence from BDB-JE DPL where each record
carries its class version in the catalog and the engine looks up the
mutation chain dynamically on read.  The `Mutations` API supports
per-version mutations (the `MutationKey` has a `class_version` field),
but the store cannot use it.  Schema migrations are therefore
single-step and one-shot.

### Persist — Finding 14 (MAJOR): `DatabaseNamer` trait wired but unused

`crates/noxu-persist/src/database_namer.rs` defines `DatabaseNamer`,
`DefaultDatabaseNamer` (formats `"persist#{store}#{entity}"`), and
`CustomDatabaseNamer`.  All three are re-exported (`lib.rs:63-65`).

`EntityStore::get_primary_index` (`entity_store.rs:99`) hardcodes:

```rust
let db_name = format!("{}_{}", self.config.store_name, E::entity_name());
```

That is, `mystore_User`, not `persist#mystore#User`.  No constructor
accepts a `DatabaseNamer`, no field stores one, and the trait is
never invoked outside its own unit tests.  The chapter
`docs/src/collections/entity-persistence.md` documents the actual
underscore form (line 41 of the source: `mystore_User`), so the docs
are consistent with the implementation but inconsistent with
`DefaultDatabaseNamer`.

### Persist — Finding 15 (MEDIUM): `KeySelector` is dead public API

The selector hierarchy in `key_selector.rs` (369 lines, fully
unit-tested) is exported (`lib.rs:70-73`) but no method on
`PrimaryIndex`, `SecondaryIndex`, `EntityIterator`, `KeyIterator`, or
`SecondaryIterator` accepts one.  `grep KeySelector
crates/noxu-persist/src` confirms there are no consumers outside
`key_selector.rs` itself.  Removing or wiring it up would be a
visible cleanup.

### Persist — Finding 16 (MEDIUM): `evolve` materialises the whole DB

`Database::scan_all_kv()` (`crates/noxu-db/src/database.rs:743`)
returns `Vec<(Vec<u8>, Vec<u8>)>`.  `evolve_database` calls it
unconditionally (`entity_store.rs:289`).  For a multi-GiB store this
is unacceptable.  The inline comment notes the workaround was used
because "the public Cursor API does not expose key bytes during
iteration" — but it does, via `DatabaseEntry::get_data()` on the key
output of `Cursor::get(.., Get::First/Next, ..)` (used elsewhere in
this very crate, e.g. `EntityIterator::next`).

### Persist — Finding 17 (MEDIUM): MANY_TO_MANY not actually supported

`secondary_index.rs:78-86` documents:

> Using `BTreeSet<PK>` for the value side supports both ONE_TO_ONE
> and MANY_TO_ONE relationships without API changes.

That part is accurate.  Earlier the same file (line 26) and the
chapter `docs/src/collections/entity-persistence.md` mention
"MANY_TO_MANY patterns".  The reverse map (`secondary_index.rs:88`)
is `BTreeMap<PK, SK>` — singular SK per PK — so a single primary key
cannot have multiple secondary keys in the same index.  MANY_TO_MANY
in the BDB-JE sense (where the secondary key is a `Set<SK>` annotated
on the entity) is not supported; the extractor returns at most one
`SK` per entity.

### Persist — Finding 18 (MEDIUM): no transaction on entity writes

Reiteration of #10's mechanism: there is no API surface that allows
threading a `noxu_db::Transaction` through `PrimaryIndex::put`,
`put_no_overwrite`, `delete`, or `delete_with_entity`.  Mentioned
separately because the chapter `docs/src/transactions/secondary-with-txn.md`
exists and presumably covers transactional secondary maintenance for
non-DPL `SecondaryDatabase` users (not audited here).  DPL users have
no equivalent.

### Persist — Finding 19 (MINOR): sequence reads "limit" as initial

`Sequence::with_cache_size` (`sequence.rs:71-103`) reads the persisted
value as `initial_value`, computes `limit = initial_value + cache_size`,
and persists the new limit.  The persisted value is in fact the
*previous* limit.  Effect: every `Sequence::new` call (including the
one after a normal restart) burns one full cache window of IDs.
Monotonicity is preserved; gaps are guaranteed.  The doc-comment "If
the sequence already exists in the database, its current value is
read.  Otherwise, it starts from 1" obscures this.  Counter values can
also "skip backward by `cache_size`" if `with_cache_size` is called
twice in the same process before any `next()`.

### Persist — Finding 20 (MINOR): unused error variants

`PersistError::EntityNotFound`, `DuplicateKey`, `StoreAlreadyOpen`, and
`InvalidEntity(String)` are defined (`error.rs:16-37`) and tested for
their `Display` output but never constructed in production code.
`PrimaryIndex::get` returns `Ok(None)` rather than `EntityNotFound`,
and `put_no_overwrite` returns `Ok(false)` rather than `DuplicateKey`.
Either remove the variants or wire them in for a more idiomatic API.

### Persist — Finding 21 (MINOR): `SimpleSerializer` evolution comment

`simple_serializer.rs:6` says it is "suitable for testing and simple
applications that do not need a schema evolution mechanism."  The
`evolve` module operates on raw bytes and works fine with
`SimpleSerializer`; the comment is misleading.

## Coverage gaps

* **No XA crash-then-resolve test.**  `tests/xa_adversarial_test.rs`
  has multiple "prepare, drop env, reopen, recover" tests, but none
  call `xa_commit(recovered_xid, …)` and re-query the database to
  confirm the data survived.  Adding one would immediately surface
  Findings 1 and 2.
* **No persist-restart test for secondary indexes.**  The two
  `tests/*.rs` files in `noxu-persist` open one `Environment` per
  test; none drop and reopen and verify secondary lookups.
* **No persist-evolve crash test.**  `evolve` is tested on small
  in-memory stores but not for resumability after an interrupted
  conversion.
* **`xa_recover` flag-driven scan** is not exercised; protocol tests
  always pass `STARTRSCAN`.
* **`mark_write` omission** is not exercised by any negative test —
  there is no test that performs a write via `db.put(Some(txn), …)`
  but skips `mark_write`, which would pass today (silently aborting
  the data) and fail as a bug-for-bug surprise.
* **`KeySelector`** has unit tests but no integration with any
  persist iterator.
* **`DatabaseNamer`** has unit tests but `EntityStore` is not tested
  with a custom namer (because it cannot accept one).

## Summary

The `noxu-xa` crate ships an XA façade that handles the in-process
cases (single-process 2PC, suspend/resume, one-phase commit, the
read-only optimisation, basic protocol-error handling) cleanly and
has reasonable adversarial coverage, but **its prepared-state
durability story is broken end-to-end**.  The `PreparedLog`
remembers XIDs, but the underlying `Transaction` is never told it is
prepared, no `TxnPrepare` log record exists in the WAL, recovery
cannot reconstitute prepared transactions, and `xa_commit` /
`xa_rollback` against a recovered XID fail with `NotFound`.  The
documentation and the stateright spec describe a 2PC implementation
that the production code does not deliver.  Finding 1 and Finding 2
together turn a feature labelled "X/Open XA two-phase commit
(complete)" into a feature that is correct only when no crash
occurs between `xa_prepare` and `xa_commit`.

Several smaller XA issues — the `mark_write` footgun (#3),
`xa_recover` flag handling (#5), and silently-swallowed log read
errors (#7) — should be addressed independently, but the central
work item is to add a real `Transaction::prepare()` to noxu-db / -txn,
log a `TxnPrepare` record, surface prepared transactions through
`noxu-recovery`, and reconstitute them in `XaEnvironment::with_prepared_log`
so post-crash `xa_commit` / `xa_rollback` actually work.

The `noxu-persist` crate is in the opposite shape: the in-process
read/write surface is clean, well-tested, and matches the chapter's
expectations, but **multiple visible features are dead code or stubs**:

* `DatabaseNamer` is exported but unwired (#14).
* `KeySelector` family is exported but unconsumed (#15).
* Secondary indexes are in-memory only (#11) — documented, but a
  major divergence from BDB-JE that limits the layer to single-process
  deployments (the docs hint at a future fix; nothing in the current
  code is on that path).
* Schema-evolution per-record class versions are not stored (#13);
  evolve is hardcoded to version 0 and is non-transactional (#12).
* Entity writes cannot participate in a user transaction (#10, #18),
  which silently breaks the BDB-JE invariant that secondary index
  maintenance is atomic with the primary write.

`PersistError` carries four variants (#20) the implementation never
returns, and `MANY_TO_MANY` claims (#17) overstate what the data
structure supports.  None of these are immediately corrupting in a
single-process workload; in aggregate they mean the persist layer is
better described as "a typed key/value façade with in-memory secondary
indexes" than as a port of BDB-JE DPL.

Recommended priorities, highest first:

1. **xa_prepare durability** — add a real prepare path to
   noxu-db/noxu-txn/noxu-recovery so `xa_commit` after recovery
   actually resolves the branch (Findings 1 & 2).  Add a positive
   crash-resolve integration test.
2. **mark_write footgun** — either auto-detect writes from
   `Transaction` (preferred), or move `mark_write` onto the
   `XaResource` trait and document it in the chapter (Finding 3).
3. **Persist transactions + secondary persistence** — add a
   `txn: Option<&Transaction>` parameter to `PrimaryIndex` writes
   and back secondary indexes with a real `Database` so the layer
   survives restart (Findings 10, 11, 18).
4. **`EntityStore::evolve` correctness** — stream rather than
   materialise (Finding 16), wrap in a transaction (Finding 12),
   and store per-record class versions (Finding 13).
5. **Dead-code cleanup** — either wire in or remove `DatabaseNamer`,
   `KeySelector`, and the unused `PersistError` variants (Findings
   14, 15, 20).
6. **XA polish** — flag handling in `xa_recover` (Finding 5),
   non-swallowed errors (Finding 7), and `XAER_RB*` mapping
   (Finding 8).

The audit found no `panic!`, `todo!`, `unimplemented!`, or
`unreachable!` calls in production code.  All `unwrap()` occurrences
in `noxu-xa` production code are either lock-poisoning (`Mutex::lock`)
or guarded post-condition removals after an explicit existence check
(`environment.rs:239`, `:265`).  The single documented `unsafe` block
in `XaEnvironment::get_transaction` is correctly justified though it
relies on caller discipline for soundness (Finding 6).
