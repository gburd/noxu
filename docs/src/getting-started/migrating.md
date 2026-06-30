# Migrating from v1.4.x

This page lists every observable behaviour change between v1.4.x and
v1.5 (and later releases) that is likely to surface in user code.

> **Capability matrix:** see
> [Introduction → capability matrix](../introduction.md#capability-matrix-v15--v22)
> for the canonical "what is supported in which release" table.

## v6.x → 7.0 — idiomatic-Rust API reshape + cleanups

7.0 reshapes the `noxu-db` public surface to read as ordinary Rust. The core
read/write/cursor changes are described in the
[CHANGELOG](https://codeberg.org/gregburd/noxu) under *7.0 core API reshape*;
this section covers the mechanical cleanups that layer on top.

### Getter renames (C-GETTER)

`get_x()` field getters are now `x()`. `get_` survives only where a key lookup
happens (`Database::get`/`get_in`, cursor `get_next`/`get_first`, …).

| Before | After |
|---|---|
| `entry.get_data()` | `entry.data_opt()` (the `Option<&[u8]>`; `entry.data()` still gives `&[u8]`) |
| `entry.get_size()` | `entry.len()` |
| `entry.get_offset()` | `entry.offset()` |
| `entry.get_partial_offset()` / `get_partial_length()` | `entry.partial_offset()` / `partial_length()` |
| `db.get_database_name()` | `db.name()` |
| `db.get_config()` | `db.config()` |
| `db.get_sorted_duplicates()` | `db.sorted_duplicates()` |
| `db.get_stats(cfg)` | `db.stats(cfg)` |
| `txn.get_id()` / `get_name()` / `get_state()` | `txn.id()` / `name()` / `state()` |
| `txn.get_durability()` / `get_lock_timeout()` / `get_txn_timeout()` | `txn.durability()` / `lock_timeout()` / `txn_timeout()` |
| `env.get_database_names()` / `get_home()` / `get_config()` | `env.database_names()` / `home()` / `config()` |
| `env.get_mutable_config()` / `get_stats()` / `get_replica_ack_timeout()` | `env.mutable_config()` / `stats()` / `replica_ack_timeout()` |
| `cursor.get_state()` | `cursor.state()` |
| `join_cursor.get_database()` / `get_config()` | `join_cursor.database()` / `config()` |
| `scan_result.get_include()` / `get_stop()` | `scan_result.included()` / `stops()` |
| `seq.get_stats()` | `seq.stats()` |
| `write_opts.get_expiration_time()` | `write_opts.expiration_time()` |
| `env_cfg.get_exception_listener()` | `env_cfg.exception_listener()` |

The redundant `DatabaseStats`/`BtreeStats`/`JoinConfig` getters over `pub`
fields were removed — read the fields directly (e.g. `stats.btree.leaf_node_count`).

### Error chains (`NoxuError`)

`NoxuError` and `EnvironmentFailureReason` are now `#[non_exhaustive]`: any
`match` on them in your code must add a wildcard arm:

```rust,ignore
match err {
    NoxuError::NotFound => { /* … */ }
    NoxuError::DeadlockDetected => { /* … */ }
    // required now:
    _ => { /* … */ }
}
```

Sub-crate causes are no longer flattened to a string — `err.source()` now chains
to the originating log/B-tree/comparator/DBI error (via the new
`NoxuError::OperationFailed { msg, source }` variant). `anyhow`/`?` users get
the real cause; classification (`is_retryable`/`is_fatal_to_environment`) and
Display text are unchanged.

### Lazy collection iterators

`StoredMap`/`StoredSortedMap::iter`/`keys`/`values` (and the `StoredKeySet`/
`StoredValueSet`/`StoredList::iter`, `iter_from`/`iter_reverse`) are now **lazy**
(cursor-backed, O(1) to create). If you relied on the old eager snapshot
semantics — iterating a point-in-time view that ignores mutations made after
the iterator was created — switch to the explicit `snapshot()` /
`keys_snapshot()` / `values_snapshot()`:

```rust,ignore
// Before (eager): iter() materialised everything.
let snap = map.iter(None)?;
map.put(None, &k, &v)?;            // not seen by `snap`

// After: name the eagerness explicitly.
let snap = map.snapshot(None)?;    // point-in-time, ignores the put below
map.put(None, &k, &v)?;
```

When iterating under an explicit transaction the lazy iterator borrows the
transaction for its whole lifetime — drop it (or finish iterating) before
committing/aborting.

### Uniform `with_*` config builders

Every `EnvironmentConfig` / `DatabaseConfig` parameter now has a consuming
`with_*` builder, so the whole config builds in one chained expression. The
`set_*(&mut self)` setters are retained, so existing code is unaffected; you
can now also write `EnvironmentConfig::new(path).with_run_cleaner(false)` for
parameters that previously only had `set_*`.

### Deprecated inert knobs

Settable-but-inert knobs are now `#[deprecated(note = "not yet implemented …")]`
so the compiler warns that they do nothing: `DatabaseConfig`'s `exclusive` /
`replicated` / `cache_mode` / `bin_delta` / `use_existing_config`, and the
per-op `WriteOptions::with_cache_mode` / `with_update_ttl` / `evict_after_write`
and `ReadOptions::with_cache_mode` / `evict_after_read`. Remove these calls;
they had no effect. `WriteOptions::with_ttl` is **not** deprecated — TTL is
honoured.

### Removed `Transaction` constructors

`Transaction::new` and the `with_log_manager`/`with_env_impl`/`with_inner_txn`
wiring methods are no longer public (they exposed engine internals and could
build a non-functional handle). Obtain a transaction via
`Environment::begin_transaction(...)`.

## v3.2 → v4.0 — XA `get_transaction` returns `Arc<Transaction>`

v4.0.0 is a major release driven by a single source-incompatible change
(review item R-F04, a soundness fix).

### Source-level breaking change

* **`XaEnvironment::get_transaction()` now returns `Arc<Transaction>`
  instead of `&Transaction`.** The previous borrow pointed into the XA
  branch map and could dangle if a protocol-violating
  `xa_rollback`/`xa_commit` freed the transaction on another thread. The
  `Arc<Transaction>` keeps the transaction alive independently of the map,
  and removes the only `unsafe` block in `noxu-xa` (the crate now carries
  `#![forbid(unsafe_code)]`).

  ```rust,ignore
  // v3.2: get_transaction returned XaResult<&Transaction>
  let txn = xa_env.get_transaction(&xid)?;         // &Transaction
  db.get(Some(txn), &key, &mut data)?;

  // v4.0: get_transaction returns XaResult<Arc<Transaction>>
  let txn = xa_env.get_transaction(&xid)?;         // Arc<Transaction>
  db.get(Some(&*txn), &key, &mut data)?;           // deref the Arc
  ```

  Mechanically: `get_transaction` still returns a `Result`; where you
  previously passed the `&Transaction` as `Some(txn)`, dereference the
  `Arc` — `Some(&*txn)`.

### On-disk format (backward compatible — no migration)

v4.0 adds the v3 log file-header CRC32 (St-C3) and an optional VLSN-tagged
entry header for replicated commits (C-C2b). Both are backward compatible:
standalone, non-replicated environments write byte-unchanged 14-byte entry
headers, and legacy v2 files remain fully readable. No data migration is
required.

## Collections API (v1.5 → v1.6)

### Source-level breaking changes

* **`StoredMap<'db>` is now `StoredMap<'db, K, V, KB, VB>`.**  The map
  is parameterised by `EntryBinding` implementations for keys and
  values.

  ```rust,ignore
  // v1.5
  let map = StoredMap::new(&db, /* read_only = */ false);
  map.put(b"key", b"value")?;

  // v1.6
  use noxu::bind::ByteArrayBinding;
  let map: StoredMap<Vec<u8>, Vec<u8>, _, _> =
      StoredMap::new(&db, ByteArrayBinding, ByteArrayBinding);
  map.put(None, &b"key".to_vec(), &b"value".to_vec())?;
  ```

  Same shape for `StoredSortedMap<K, V, KB, VB>`,
  `StoredKeySet<K, KB>`, `StoredValueSet<V, VB>`, and
  `StoredList<V, VB>`.

* **Every `Stored*` method now takes `txn: Option<&Transaction>` as
  the leading argument.**

  ```rust,ignore
  // v1.5
  map.put(b"k", b"v")?;            // auto-commit
  map.get(b"k")?;
  map.iter()?;

  // v1.6
  map.put(None, &k, &v)?;          // auto-commit
  map.get(None, &k)?;
  map.iter(None)?;
  // ...or pass Some(&txn) to participate in a user txn:
  map.put(Some(&txn), &k, &v)?;
  ```

  This applies to `get`, `put`, `remove`, `contains_key`, `len`,
  `is_empty`, `iter`, `keys`, `values`, `clear`, `iter_from`,
  `iter_reverse`, `first_key`, `last_key`, `first_entry`,
  `last_entry`, `higher_key` (StoredSortedMap), `add`, `contains`,
  `remove` (StoredKeySet), and every `StoredList` method.

* **`StoredMap::len` returns `usize` instead of `u64`.**  The on-disk
  count is bounded by `Database::count() -> u64` but Rust callers
  almost always want `usize` (matching `BTreeMap::len`); the
  collections layer truncates to `usize::MAX` at the boundary.

* **The internal `BTreeSet` key index is removed.**
  `register_key`, `register_keys`, `known_keys` are deleted.  Pre-
  existing data that was visible in v1.5 only after a
  `register_keys` call is now visible automatically because
  `iter` / `keys` / `values` walk the database directly via a
  cursor:

  ```rust,ignore
  // v1.5: iteration only saw keys you'd registered.
  let map = StoredMap::new(&db, true);
  map.register_keys(&[b"a", b"b"]);
  for entry in map.iter()? { /* sees a, b */ }

  // v1.6: iteration sees every record in the database.
  for entry in map.iter(None)? { /* sees every record */ }
  ```

* **`StoredList::remove` now compacts.**  Removing index `i` shifts
  every record at index `j > i` down to `j - 1` and decrements
  `next_index`.  Code that relied on the v1.5 "remove leaves a hole"
  contract will see different `get(idx)` results after `remove`.
  The whole compaction is issued under the supplied txn; pass
  `Some(&txn)` for crash-atomic semantics.

* **`StoredList::new` / `StoredList::open` now take a value
  binding.**

  ```rust,ignore
  // v1.5
  let list = StoredList::new(&db);
  let list = StoredList::open(&db)?;

  // v1.6
  use noxu::bind::ByteArrayBinding;
  let list: StoredList<Vec<u8>, _> =
      StoredList::new(&db, ByteArrayBinding);
  let list: StoredList<Vec<u8>, _> =
      StoredList::open(&db, ByteArrayBinding)?;
  ```

### Behavioural breaking changes

* **`TransactionRunner` now drives `Stored*` methods.**  In v1.5 the
  `&Transaction` it supplied could not be threaded into any `Stored*`
  call (every `Stored*` method ignored its txn argument because there
  *was* no txn argument).  In v1.6 the runner-supplied `&Transaction`
  is the canonical way to make a sequence of `Stored*` writes
  transactional:

  ```rust,ignore
  let runner = TransactionRunner::new(&env);
  runner.run(|txn| {
      map.put(Some(txn), &k1, &v1)?;
      map.put(Some(txn), &k2, &v2)?;
      list.remove(Some(txn), 0)?;          // shift-compaction inside the txn
      Ok(())
  })?;
  ```

* **`TransactionRunner` retries on every retryable error, with
  jittered exponential backoff.**  v1.5 retried only on
  `DeadlockDetected`.  v1.6 retries on every variant returned by
  `NoxuError::is_retryable()` (`LockConflict`, `DeadlockDetected`,
  `LockTimeout`, `LockNotAvailable`, `TransactionTimeout`,
  `LockPreempted`).  Defaults: 10 retries, 10 ms base, 1 s ceiling,
  ±25% jitter.  Configure via:

  ```rust,ignore
  TransactionRunner::new(&env)
      .with_max_retries(20)
      .with_base_backoff(Duration::from_millis(5))
      .with_max_backoff(Duration::from_secs(2))
      .with_jitter(0.1);
  ```

* **`StoredKeySet::add` returns `bool` (newly inserted).**  v1.5 had
  no `add` method; the v1.6 `add` matches `java.util.Set.add`
  semantics (returns `true` on first insert, `false` if already
  present).

* **`TransactionRunner::run`'s closure signature relaxed from `Fn`
  to `FnMut`.**  Closures may now capture mutable state (e.g. retry
  counters).

## Transaction wiring (v1.4.x → v1.5)

These are previously-broken paths that the engine now executes
correctly. Code that *depended* on the v1.4.x bug will break.

* **`Database::open_cursor(Some(&txn), ...)` now threads `txn` through
  to the cursor.** Cursors opened on a transactional database
  participate in the transaction as documented. v1.4.x silently
  ignored the argument - every cursor was effectively auto-commit.
  The change can surface as new lock conflicts on workloads that were
  accidentally racing against themselves.
* **`SecondaryDatabase::open_cursor(Some(&txn), ...)`** - same fix.
* **`Database::count()` on a sorted-dup database** is now correct;
  v1.4.x returned 0.
* **`Database::delete(key)` on a sorted-dup database** now removes
  every duplicate value for `key`. v1.4.x removed only the first
  duplicate.
* **`Environment::close()` after `txn.commit()` succeeds.** v1.4.x's
  active-transactions gate fired even after every txn was already
  committed.
* **`EnvironmentConfig::durability` is honoured.** v1.4.x stored the
  policy on the config but never threaded it into the txn manager.
* **`TransactionConfig::read_uncommitted` is honoured.** Same shape.

## Cursor `Get` variants (v1.4.x → v1.5)

* **`Get::SearchBoth` on a non-duplicates database now validates the
  data argument.** A non-matching data returns `NotFound` instead of
  succeeding on the key alone.
* **`Get::NextDup` / `Get::PrevDup` on a non-duplicates database** now
  return `NotFound` (consistent with the no-dups invariant). v1.4.x's
  behaviour was undefined.
* **`Get::SearchLte`, `Get::FirstDup`, `Get::LastDup`** now return
  `NoxuError::Unsupported`. These variants were never wired in v1.4.x
  (the stub paths returned `NotFound` or panicked depending on the
  db shape); v1.5 surfaces a typed error so callers can match against
  it. Planned for v1.6.

## Architectural decisions (v1.5)

* **`Environment::begin_transaction(Some(&parent), ...)` returns
  `NoxuError::Unsupported` (v1.5) - and the `parent` parameter has been
  removed entirely in v2.0.**  See the v1.5 →
  v2.0 section below for the source-compatibility break.
* **`SecondaryConfig::with_foreign_key_database` /
  `with_foreign_key_delete_action` /
  `with_foreign_key_nullifier` /
  `with_foreign_multi_key_nullifier` are rejected at
  `SecondaryDatabase::open` with `NoxuError::Unsupported`.**
  Decision 2C. The setters are still chainable on
  `SecondaryConfig` so source written against v1.6 keeps compiling
  on v1.5; the rejection fires only when an FK-configured config
  reaches `open`.

  > **v1.6 update.** Foreign-key constraints are now
  > enforced.  Use the new
  > `SecondaryConfig::with_foreign_key_database_handle(Arc<Mutex<Database>>)`
  > setter to register the foreign primary's runtime handle; the
  > legacy `with_foreign_key_database(name)` setter is retained as
  > advisory but combining `name` *without* `handle` is rejected with
  > `NoxuError::IllegalArgument`.  All three actions - Abort,
  > Cascade (transitive, with cycle detection), Nullify (single-key
  > and multi-key) - work end-to-end under the caller's txn.  See
  > Secondary database unification.

* **`SecondaryDatabase` cross-primary collisions return
  `NoxuError::Unsupported`.** Decision 1B. v1.4.x silently overwrote
  the first primary's secondary entry when a second primary produced
  the same secondary key. v1.5 rejects the second insert with a typed
  error and leaves the first primary's mapping intact. Idempotent
  re-inserts of the same `(sec_key, pri_key)` pair remain a no-op so
  v1.4 callers that relied on `update_secondary(pk, None, Some(d))`
  twice for the same primary keep working.

  > **v1.6 update.** v1.6 secondaries are sorted-dup, so
  > many primaries may share a secondary key.  The inner secondary
  > database **must** be opened with
  > `DatabaseConfig::with_sorted_duplicates(true)`; without it,
  > `SecondaryDatabase::open` returns `NoxuError::IllegalArgument`.
  > Cross-primary inserts succeed and the cursor's new
  > `get_next_dup_full` / `get_prev_dup_full` walk the duplicate run.
  > Additionally, `Database::put` / `Database::delete` now drive
  > every registered secondary automatically under the caller's
  > txn - manual `update_secondary` calls are no longer required
  > (but still supported for population paths).

## XA in-process only (v1.5)

See the 2026 review.

* **`xa_commit(xid)` / `xa_rollback(xid)` on an XID that exists in
  the persistent `_xa_prepared` log but not in the in-memory
  `branches` map return `XaError::CrashDurabilityNotSupported`.**
  v1.4.x returned the misleading `XaError::NotFound` for the same
  case. The XID is still surfaced by `xa_recover` so operators can
  see what is in doubt; clear it with `xa_forget`.
* **`xa_prepare` no longer requires `mark_write`.** v1.5 auto-detects
  writes via `Transaction::has_logged_entries`. `mark_write` is kept
  as a no-op for source compatibility.

## DPL transaction threading (v1.5)

This is the only v1.5 change with a non-trivial source-level
migration. See
the 2026 review.

* **`PrimaryIndex::{put, put_no_overwrite, get, delete,
  delete_with_entity, contains, entities, keys}` now take
  `txn: Option<&Transaction>` as the leading argument.**
  Pass `None` for the historical auto-commit semantics.
* **`SecondaryIndex::{get, delete, iter, iter_from}` take
  `txn: Option<&Transaction>` as the leading argument.**
* **DPL secondary indexes are now transactional and persistent.** They
  are backed by real `noxu-db` `SecondaryDatabase`s opened against the
  primary and maintained inside the active transaction (the JE
  `Store.openSecondaryDatabase` model). Aborting a transaction rolls the
  primary write and the secondary index update back together; the
  secondary survives store reopen. The former one-shot
  `PersistError::SecondariesNotTransactional` warning and the
  `NOXU_PERSIST_ALLOW_NON_TXN_SECONDARIES` opt-in have been removed.
  **Breaking:** `EntityStore::open_secondary_index` now takes
  `(primary, name, serializer, extractor)`, and the
  `#[derive(SecondaryKey)]`-generated `open_<name>_index` helper now
  takes `(&mut store, &mut primary, serializer)`. The secondary-key type
  must implement `PrimaryKey`.

## Collections and bind (v1.5)

* **`SerdeBinding<T>` payloads now carry a 2-byte
  `[0xCB, 0x01]` magic + version header.** Records written by
  earlier 1.5 release candidates do **not** carry the header and will
  fail to decode under v1.5 with
  `BindError::VersionMismatch`. Migrate by re-`put`-ing the data
  under the v1.5 build, or by staying on the pre-3C build until you
  have a maintenance window. The plain tuple bindings
  (`IntBinding`, `LongBinding`, `StringBinding`,
  `SortedDoubleBinding`) are unaffected.
* **`StoredList::next_index` is now persistent.** Use
  `StoredList::open(&db)` (new) when reopening a database that
  already contains entries; it recovers `next_index` from the
  largest existing 8-byte big-endian key.
  `StoredList::new(&db)` is preserved for empty / fresh databases
  but **does not recover** `next_index`; using it against an existing
  list re-uses slot 0 and overwrites the first record.

## DPL entity record envelope (v1.6)

* **Every entity record stored by `noxu-persist::PrimaryIndex` now
  carries a per-record class-version envelope.**  Pre-v1.6
  records were the raw output of
  `EntitySerializer::serialize`; v1.6 records prepend

  ```text
  [2-byte class_version BE]
  [1-byte entity_class_tag_len]
  [entity_class_tag bytes]    (UTF-8, length = tag_len)
  [payload bytes]             (your EntitySerializer's output)
  ```

  This is **not backward-compatible** with pre-v1.6 entity stores.
  Reading a pre-v1.6 record under v1.6 fails with
  `PersistError::SerializationError("record too short for entity
  envelope: ...")` or `"entity class tag mismatch: on-disk '...' !=
  expected '...'"`.

  **Migration procedure (one-shot dump and reload):**

  1. While still on v1.5.x, run a dump utility that walks every
     entity database with the user's existing `EntitySerializer`
     and writes the deserialised entities to a sidecar file (any
     format - JSON, ndjson, custom binary; the format is local to
     your migration).
  2. Take the application offline and bump to v1.6.
  3. Open the v1.6 environment with
     `EntityStore::open(&env, StoreConfig::new(...).with_allow_create(true))`,
     iterate the sidecar, and `index.put(None, &ser, &entity)` each
     record.  v1.6's `put` writes the new envelope.
  4. Drop the v1.5 entity database files.

  Stores that opened the entity DBs **only** under v1.6 are
  unaffected - the envelope is universal under v1.6.

* **`Entity` trait gained a default `class_version() -> u16` method.**
  Existing implementations need no change (the default is `0`).
  Bump `class_version()` whenever you change the on-disk shape of an
  entity and supply matching `noxu::persist::evolve::Mutations` via
  `StoreConfig::with_mutations(...)` so the open path can run
  schema evolution for older records.

* **`EntitySerializer` trait gained a default `deserialize_versioned`
  method.**  Existing implementations work as-is.  Override
  `deserialize_versioned` when you want field-level evolution that
  reads old records lazily without rewriting them.  See
  [Schema evolution](../collections/entity-persistence.md#schema-evolution).

* **A hidden catalog database
  `__noxu_persist_catalog__<store_name>` is now created in every
  environment that opens an `EntityStore`.**  It records the most
  recent class version observed for each entity name and is
  consulted by the open-path schema-evolution flow.  The catalog is
  opened lazily on the first `get_primary_index<E>()` /
  `evolve()` call, so existing pre-v1.6 environments that have
  already had their data dump-and-reloaded will gain the catalog
  the next time they are opened.

## Documented v1.5 limitations (no source change required)

These are not breakages; they are clarifications. They affect the
shape of patterns we recommend rather than the source-level signature
of any method.

* **`secondary.update_secondary(...)` runs auto-committed** even when
  the surrounding primary write is under a user txn. v1.5 has no
  `associate()` hook; the `update_secondary` call itself does not
  take a transaction. Atomic primary + secondary writes are planned
  for v1.6 alongside Decision 1's sorted-dup + `associate` work. See
  [Secondary Indices with Transactions](../transactions/secondary-with-txn.md).
* **`Stored*` collection methods now thread `Option<&Transaction>`
  through every operation** - v1.5's auto-commit-only restriction is
  closed by v1.6 (see [Collections API (v1.5 → v1.6)](#collections-api-v15--v16)
  above).  `TransactionRunner` is now the recommended way to drive
  multi-statement `Stored*` sequences.
* **Replication is GA in v2.0.** All ten pre-v2.0 blockers were
  closed.  See the
  Wave 4-A report for
  per-finding resolution notes.

## Quick before/after summary

```rust
// v1.4.x (broken)
let cursor = db.open_cursor(Some(&txn), None)?;  // txn was ignored
secondary.update_secondary(&pk, None, Some(&v))?; // silently overwrote
                                                  // existing secondary
                                                  // for cross-primary
                                                  // key collisions
let txn2 = env.begin_transaction(Some(&txn), None)?; // accepted, no-op (v1.4.x)

// v1.5 (correct)
let cursor = db.open_cursor(Some(&txn), None)?;  // honours txn
secondary.update_secondary(&pk, None, Some(&v))?; // returns
                                                  // NoxuError::Unsupported
                                                  // on cross-primary
                                                  // collision
let txn2 = env.begin_transaction(Some(&txn), None)?; // returns
                                                     // NoxuError::Unsupported
                                                     // (v1.5; in v2.0 this
                                                     //  is a compile error
                                                     //  - see the v1.5 →
                                                     //  v2.0 section below)

// DPL (breaking source-level signature change)
// v1.4.x:
//   index.put(&ser, &user)?;
//   let u = index.get(&ser, &id)?;
// v1.5:
index.put(None, &ser, &user)?;          // explicit auto-commit
let u = index.get(None, &ser, &id)?;
// or, to participate in a user txn:
index.put(Some(&txn), &ser, &user)?;
```

---

## v1.5 → v2.0 — nested-transaction parameter removed

The `parent` parameter to `Environment::begin_transaction` was rejected
at runtime in v1.5 and removed from the signature entirely in v2.0 —
the type system now enforces the constraint and the misuse is a
compile error.

### Breaking signature change

```rust
// v1.4.x and v1.5 / v1.6
fn begin_transaction(
    &self,
    parent: Option<&Transaction>,
    config: Option<&TransactionConfig>,
) -> Result<Transaction>;

// v2.0
fn begin_transaction(
    &self,
    config: Option<&TransactionConfig>,
) -> Result<Transaction>;
```

### Mechanical migration

```rust
// before
let txn  = env.begin_transaction(None, None)?;
let txn2 = env.begin_transaction(None, Some(&cfg))?;
// (and the v1.5-rejected misuse, which now will not compile)
let bad  = env.begin_transaction(Some(&parent), None)?;

// after
let txn  = env.begin_transaction(None)?;
let txn2 = env.begin_transaction(Some(&cfg))?;
// no v2.0 equivalent for nested txns - they remain unsupported, and the
// type system now enforces it.
```

See
the 2026 review
for details.

---

## v2.x → v3.0 — Wave 11-R breaking changes

### C-4: `Environment::open_database` — `txn` parameter is now honoured

The `_txn` parameter was silently ignored in v2.x.  In v3.0 it is renamed
to `txn` and is functional:

* When `txn: Some(&txn)` is supplied and `config.allow_create = true`, the
  database creation is **transactional**.  If the transaction is subsequently
  aborted, the database is rolled back and does not appear in the WAL.
* `Environment::get_database_names()` now returns **committed names only**.
  A database created inside an uncommitted transaction is not visible to
  other callers until the transaction commits.

#### Mechanical migration (no code change required for most users)

If you pass `None` as the transaction argument, behaviour is unchanged.
If you previously passed `Some(&txn)` expecting the parameter to be
ignored (i.e. relied on non-transactional creation inside a txn scope),
you must either:

1. Pass `None` to preserve the old non-transactional semantics.
2. Accept the new transactional semantics: call `txn.commit()` to persist
   the database creation, or `txn.abort()` to roll it back.

```rust
// v2.x — txn ignored; database always created immediately
let db = env.open_database(Some(&txn), "mydb", &cfg)?;
// ...
txn.abort(); // database still existed despite abort!

// v3.0 — txn honoured; abort rolls back the creation
let db = env.open_database(Some(&txn), "mydb", &cfg)?;
// ...
txn.abort(); // database registration is rolled back
```

### C-5: BIN delta log behaviour changes in checkpoint traces

`BIN::should_log_delta()` is now a faithful port of JE `BIN.shouldLogDelta`
(BIN.java:1892).  The delta-vs-full decision is COUNT-based (the number of
delta slots vs `nEntries * binDeltaPercent / 100`, integer math) using the
configurable `TREE_BIN_DELTA` percent (0–75, default 25, set via
`EnvironmentConfig::set_tree_bin_delta_percent`) — not the former dirty-fraction
heuristic against a hardcoded 0.25.  It also carries three JE-equivalent guard
clauses:

1. BINs already in delta form always re-log as a delta (no change for
   users; previously a spurious full BIN could be written).
2. After `compress()` removes a dirty slot (`prohibit_next_delta = true`),
   the next checkpoint writes a full BIN instead of a delta.
3. A BIN whose full version has never been written (`last_full_lsn ==
   NULL_LSN`) always writes a full BIN.

On-disk format is unchanged; recovery is strictly safer.  Checkpoint
output may differ (more full BINs in specific compress-then-checkpoint
scenarios).  No application code changes are required.

### Q-3: New API — `Environment::compress()` and `Environment::evict_memory()`

Two new methods mirror JE's `Environment.compress()` and
`Environment.evictMemory()`:

```rust
// Synchronously compress BINs with known-deleted slots.
let n_bins_compressed: usize = env.compress()?;

// Trigger the memory evictor.
let bytes_freed: usize = env.evict_memory()?;
```

These are additive (non-breaking) additions to the public API.

---

## v3.0.0 — Wave 11-T Cross-Feature Correctness Fixes

### X-5: `CleanResult::files_deleted` semantics changed

Previously `Cleaner::do_clean()` deleted files immediately in the same pass,
so `CleanResult::files_deleted` was always equal to `files_cleaned` (minus
protected files).

After the X-5 checkpoint-barrier fix, files are only deleted **after two
successive checkpoints** have captured the migration.  During the cleaning pass
itself, `files_deleted` will be **0** (or a small non-zero value if files from
a prior cleaning cycle have now passed the barrier).

**Migration**: if your code asserts `result.files_deleted > 0` immediately after
`do_clean()`, update it to call `cleaner.delete_safe_files()` explicitly after
triggering two checkpoints, or rely on the background checkpointer to advance
the barrier automatically.

### X-13: `EnvironmentImpl::is_invalid` type changed to `Arc<AtomicBool>`

Internal API only (`noxu-dbi`).  If you directly access
`EnvironmentImpl::is_invalid` (e.g. in integration-test mocks), change the
field access to use the new `is_invalid_flag()` method which returns an
`Arc<AtomicBool>`.

### X-3: `ReplicaAckCoordinator` trait: new default method

The `ReplicaAckCoordinator` trait gained
`alloc_vlsn_for_recovered_commit(&self, lsn: Lsn) -> u64` with a default
implementation that returns 0 (NULL_VLSN — correct for non-replicated envs).
No action is required unless you have a custom `ReplicaAckCoordinator` impl
that should assign VLSNs to recovered XA commits.

### X-7: `DatabaseImpl::get_real_tree()` return type changed

Internal API only (`noxu-dbi`).  `get_real_tree()` previously returned
`Option<&Tree>`.  It now returns `Option<std::sync::RwLockReadGuard<'_, Tree>>`.

The guard implements `Deref<Target=Tree>`, so most call sites (method calls,
deref coercions) require no change.  The only sites that need updating are
where `tree` is passed to a function expecting `&Tree` explicitly — change
`f(tree)` to `f(&tree)`.

A new method `get_real_tree_arc()` returns `Option<Arc<RwLock<Tree>>>` for
callers that need the shared Arc (e.g. the cleaner registry).

### X-12: `cache_size` is now the total memory budget (BREAKING)

**Previously**: `cache_size` bounded only the BIN tree Arbiter pool.
Log write buffers (`log_num_buffers × log_buffer_size`, default 3 MiB)
and off-heap cache (`max_off_heap_memory`, default 0) were independent
pools.  Actual memory use = `cache_size + log_buffers + off_heap`.

**Now**: `cache_size` is the **total** ceiling.  The Arbiter receives
`cache_size − log_buf_total − off_heap_reserved` (floored at 1 MiB).

**Impact**: If your `cache_size` was sized to give the BIN tree a specific
budget, the BIN tree pool is now smaller by `log_buf_total + off_heap_reserved`.

**Migration**:

```rust
// Before (v2.x): cache_size=256 MiB meant BIN tree got 256 MiB,
// plus 3 MiB log buffers on top = 259 MiB total.
.with_cache_size(256 * MiB)

// After (v3.0): to give the BIN tree 256 MiB, increase cache_size:
// cache_size = 256 MiB + 3 MiB (log buffers) = 259 MiB
.with_cache_size(259 * MiB)

// Or equivalently, check the formula:
// bin_tree = cache_size - log_num_buffers * log_buffer_size - max_off_heap_memory
```

If you are using the default `log_num_buffers = 3` and `log_buffer_size = 1 MiB`
and `max_off_heap_memory = 0`, add **3 MiB** to your `cache_size` to maintain
the same BIN tree allocation.

### X-11: `log_flush_no_sync_interval_ms` now active

Previously this parameter was stored but never consumed.  As of v3.0.0,
setting a non-zero value starts the `noxu-log-flusher` background daemon.
This is a **behaviour change** (not a source-level breaking change): code
that previously set this parameter expecting it to be ignored will now see
background flushes.  Default is 0 (disabled, same as before).

### C-6: Transactional database creation WAL behavior changed (Wave 11-Y)

**Previously**: `open_database_transactional` deferred the `NameLN` WAL write
to commit time (`commit_pending_database` called `log_name_ln`).  The WAL
entry had `txn_id = None` and was written as a plain `NameLN` after the
`TxnCommit` record.

**Now**: The `NameLNTxn` WAL entry is written **inside** the creating
transaction (`Provisional::Yes`, `LogEntryType::NameLNTxn`) with the live
`txn_id`.  `commit_pending_database` no longer writes a WAL entry; the
`TxnCommit` record makes the provisional entry durable.  An aborted or
crashed transaction leaves no committed `NameLNTxn` in the log, and recovery
removes it.

**Source-level impact**: `EnvironmentImpl::open_database_transactional` now
requires a `txn_id: u64` parameter (internal API, `noxu-dbi` crate).
External callers use `Environment::open_database(txn, name, config)` which
is unchanged.

**On-disk compatibility**: Existing WAL files with `NameLN` entries
(txn_id=None, written at commit time) recover correctly — the undo predicate
treats `txn_id=None` as committed.  No migration or log version bump is
required for files created by earlier versions.

---

## Wave ZA — v3.1.0 (fix/za-config-api)

### DbIter/DbRange acquire a `'txn` lifetime parameter (Item 4)

`Database::iter` and `Database::range` now return `DbIter<'txn>` and
`DbRange<'txn>` respectively.  Code that stores a `DbIter` or `DbRange` in a
named variable with an explicit type annotation must add the lifetime:

```rust
// Before (v3.0):
let it: DbIter = db.iter(None)?;

// After (v3.1):
let it: DbIter<'_> = db.iter(None)?;
// or: let it = db.iter(None)?;  (inferred — no change needed)
```

The borrow checker will now reject any code that commits or drops a
transaction while an iterator created from that transaction is still alive.
This is a compile-time safety improvement; no runtime behaviour changes.

If inference is sufficient (most cases), no source change is needed.

### `commit_pending_database` atomic transition (Item 5)

`EnvironmentImpl::commit_pending_database` in `noxu-dbi` now holds the
`pending_names` write lock across the entire pending→committed transition.
The field `pending_names` has changed from `HashSet<String>` to
`HashMap<String, DatabaseId>`.  This is an internal API change; user-facing
code using only `noxu-db` / `noxu` is unaffected.

If you depend on `noxu-dbi` directly and call `commit_pending_database` or
`abort_pending_database`, no call-site changes are required.

### `noxu::PreparedTxnInfo`, `PreparedLnReplay`, `SharedReplicaAckCoordinator` (Item 3)

These types previously required a direct dependency on `noxu-recovery` or
`noxu-dbi` to name.  They are now re-exported from `noxu-db` and accessible
as `noxu::PreparedTxnInfo`, `noxu::PreparedLnReplay`, `noxu::PreparedLnOperation`,
`noxu::SharedReplicaAckCoordinator`, `noxu::ReplicaAckCoordinator`,
`noxu::AckWaitError`, and `noxu::AckWaitErrorKind`.

Remove any direct `noxu-recovery` / `noxu-dbi` dependency that was added
solely to name these types.

### Reserved `EnvironmentConfig` parameters now warn (Item 1)

Seven parameters (`env_latch_timeout_ms`, `env_expiration_enabled`,
`env_db_eviction`, `env_fair_latches`, `env_check_leaks`, `env_forced_yield`,
`env_ttl_clock_tolerance_ms`) are accepted but not implemented.  Setting any
to a non-default value now emits a `WARN`-level log message at
`Environment::open` time.

Previously these settings were silently ignored.  If your code sets any of
them, expect a log warning until the underlying feature is implemented.
See `docs/src/operations/known-limitations.md` for the full list.

### `RepConfig::peer_allowlist` mTLS warn (Item 2)

Setting a non-empty `peer_allowlist` now emits a `WARN`-level log at
`ReplicatedEnvironment::new` time.  The allowlist has never been enforced
(Phase 2 pending); the warning ensures operators are not misled.

### Log format v2 → v3 (file-header checksum)

`LOG_VERSION` is now **3**. v3 log files write a CRC32 over the file header
(growing it from 32 to 36 bytes) so a torn header write is detected at open
time. This is backward-compatible: existing **v2** files are read unchanged —
the engine resolves each file's first-entry offset from that file's own
version (`FileHeader::on_disk_size`), so v2 entries are still found at offset
32 and v3 entries at offset 36. No migration step or dump/reload is required;
new files are written as v3, old files continue to be read in place. A v3 file
whose header fails its CRC is rejected with `LogError::HeaderChecksumMismatch`.
