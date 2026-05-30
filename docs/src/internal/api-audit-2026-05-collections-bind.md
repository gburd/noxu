# API Audit — `noxu-collections` and `noxu-bind`

Date: 2026-05
Auditor: read-only review against the BDB-JE collections/bind reference
contracts described in the project AGENTS.md.
Scope: `crates/noxu-collections/src/`, `crates/noxu-bind/src/`,
`docs/src/collections/`, `docs/src/getting-started/bindings.md`.

This is a **read-only** audit; no source files were modified.

---

## Scope

This document audits two crates that together form the typed-access /
collection-style layer above `noxu-db`:

| Crate | Public surface audited |
|---|---|
| `noxu-collections` | `StoredMap`, `StoredSortedMap`, `StoredKeySet`, `StoredValueSet`, `StoredList`, `StoredIterator`, `StoredKeyIterator`, `StoredValueIterator`, `TransactionRunner`, `CollectionError` |
| `noxu-bind` | `EntryBinding`, `EntityBinding`, `TupleBinding`, `SortKey`, `TupleInput`, `TupleOutput`, `ByteArrayBinding`, `RecordNumberBinding`, primitive bindings (`Bool/Byte/Char/Short/Int/Long/Float/Double/Sorted{Float,Double}/Packed{Int,Long}/SortedPacked{Int,Long}/StringBinding`), `SerdeBinding`, `TupleSerdeBinding`, `TupleSerdeKeyDataBinding`, `simple_serial::{to_bytes, from_bytes, SimpleSerializer, SimpleDeserializer}`, `BindError` |

The reference contract is the BDB-JE collections / bind module, which the
project's `AGENTS.md` establishes as the conceptual model (`Map<K,V>`-like
view, sorted-map variant, record-number list, `StoredClassCatalog`,
deadlock-retrying `TransactionRunner`, sortable tuple encoding).

---

## Methodology

1. Enumerated every public method on each `Stored*` type and every public
   trait/binding type in `noxu-bind`.
2. Read crate-level rustdoc, per-item rustdoc, and the user-facing mdbook
   chapters (`docs/src/collections/*`, `docs/src/getting-started/bindings.md`).
3. Read the implementation, including in-module unit tests.
4. Compared the surface and observable behaviour against the BDB-JE contract:
   - `StoredMap` / `StoredSortedMap` map semantics (return value of `put`, key
     ordering, iteration);
   - `StoredKeySet` / `StoredValueSet` (collection of keys / values backed by
     a database);
   - `StoredList` (record-number-keyed sequence backed by a `Sequence` /
     RECNO database, no key gaps);
   - `StoredIterator` lifecycle (cursor must be closed);
   - `TransactionRunner` (retry budget, deadlock classification, backoff);
   - `TupleBinding` (sortable byte encodings, sign-bit flip for signed
     integers, sortable IEEE-754 encoding for floats);
   - `SerialBinding` (serde-based replacement for Java serialization, role of
     a stored class catalog);
   - `ByteArrayBinding` (claimed zero-copy / pass-through);
   - empty-value vs missing-value semantics.
5. Spot-checked for `unwrap` / `panic!` / `todo!` on user input,
   `unsafe` blocks, and mismatches between rustdoc and code.

### Limits of this audit

- I did not run the test suite, only read it.
- I did not exercise concurrent or crash-recovery scenarios.
- The mdbook source is canonical; I did not re-verify the rendered HTML.
- I did not audit `noxu-persist` (it is referenced from
  `docs/src/collections/entity-persistence.md`, but per the user prompt only
  the listed files were in scope).

---

## Findings table

Severity legend:

- **High** — observable correctness gap, contract violation, or doc lying
  about reality.
- **Medium** — semantic divergence from the BDB-JE contract or a foot-gun
  that real users will hit.
- **Low** — polish, documentation cleanup, missing tests, or minor
  ergonomic issue.

| # | Sev. | Area | Summary |
|---|---|---|---|
| 1 | **High** | `StoredMap` | `iter()` / `keys()` / `values()` walk an in-memory `BTreeSet` of keys this map view has happened to touch, not the database; pre-existing rows are silently skipped |
| 2 | **High** | `StoredMap` | `len()` and `is_empty()` use `db.count()` while `iter()` uses the in-memory key index — the two disagree as soon as a `register_key` is forgotten |
| 3 | **High** | `StoredMap` / collections | No collection method accepts a `&Transaction`; `db.get/put/delete` is always called with `None`, so collection operations never participate in a user transaction |
| 4 | **High** | `TransactionRunner` | The closure receives `&Transaction` but there is no API surface to thread that handle into any `Stored*` operation, so retries do not actually wrap a collection's reads/writes in a transaction |
| 5 | **High** | `StoredList` | `remove(index)` rustdoc claims "re-numbers all higher-indexed entries so that gaps are never left in the list"; the body just deletes the key. Doc and impl directly contradict, and a comment inside the body says the opposite of the rustdoc |
| 6 | **High** | `StoredList` | `next_index` is a process-local `Mutex<usize>`; on environment reopen it resets to 0 and `push()` will overwrite existing records at index 0, 1, … |
| 7 | **High** | docs vs. code | All four `docs/src/collections/*.md` chapters describe a typed `StoredMap<K,V>`/`StoredSet<K>`/`StoredList<V>` API parameterised by bindings and threading `txn` through every call — an API that does not exist in the codebase |
| 8 | **High** | docs | `docs/src/collections/stored-list.md` references `env.open_sequence(...)` and a `Sequence` parameter to `StoredList::new`; neither exists (sequences live on `Database`, not `Environment`, and `StoredList::new` takes only `&Database`) |
| 9 | **Medium** | `StoredMap::put` | Pre-read of the previous value and the `db.put` are not atomic — even if a future revision wires up transactions, today the "previous value" returned by `put`/`remove` is racy under concurrent writers |
| 10 | **Medium** | `StoredMap::clear` | Iterates the in-memory key index and calls `db.delete` per key with `_ = ...`; rows added by other writers, or rows in the DB but never registered with this view, are silently skipped, and per-key delete errors are swallowed |
| 11 | **Medium** | `TransactionRunner` | `is_deadlock` only matches `NoxuError::DeadlockDetected`; lock-conflict / lock-timeout errors and any other transient-but-retryable conditions exhaust no retries (BDB-JE retries on the broader `LockConflictException` family) |
| 12 | **Medium** | `TransactionRunner` | Retry loop has no backoff, no jitter, and no cap on elapsed time — under contention this is a tight retry-storm |
| 13 | **Medium** | `TransactionRunner` | Closure bound is `Fn`, not `FnMut`; mutable per-attempt state requires interior mutability (the test for retry uses `AtomicU32`) |
| 14 | **Medium** | `TransactionRunner::run_without_txn` | Just calls `f()` with no transaction at all; does not use `self.env`, has no retry behaviour, and is indistinguishable from a direct call. Documented as the auto-commit / non-transactional path but provides nothing |
| 15 | **Medium** | `StoredKeySet` / `StoredValueSet` | Same in-memory key-index pattern as `StoredMap` (findings 1–2): iteration only sees keys discovered via `contains` / `register_key{,s}` |
| 16 | **Medium** | iterator lifecycle | `StoredIterator` is documented as "wraps a live cursor" but holds no cursor; nothing requires explicit close, and there is no `Drop` impl. The contrast with the BDB-JE `StoredIterator.close()` contract is not explained |
| 17 | **Medium** | `StoredIterator::next` | On `OperationStatus::NotFound` (key disappeared between snapshot and fetch) and on any unexpected status the iterator silently recurses to skip the entry; an unexpected status is logged nowhere and is not distinguishable from a clean "deleted between snapshot and fetch" |
| 18 | **Medium** | `EntryBinding` | The trait surface cannot represent the `Some(empty)` vs `None` distinction that `DatabaseEntry` itself carries — every binding goes through `entry.data()` (which collapses the two) |
| 19 | **Medium** | `SerdeBinding` / `simple_serial` | No class catalog and no schema/version tag at all. Adding, removing, or reordering a struct field silently corrupts on-disk records. `entity-persistence.md` warns about this for the DPL but the audited `SerdeBinding` does not advertise the limitation |
| 20 | **Medium** | `simple_serial` | `serialize_str` and `serialize_bytes` cast `usize` length to `u32` (`v.len() as u32`) without bounds-checking; payloads larger than 4 GiB silently truncate |
| 21 | **Medium** | docs (bindings) | `docs/src/getting-started/bindings.md` lists only `IntBinding`, `LongBinding`, `SortedDoubleBinding`, `StringBinding`. The crate also exports `Bool/Byte/Char/Short/Float/Double/Sorted{Float}/Packed{Int,Long}/SortedPacked{Int,Long}` and `ByteArrayBinding`/`RecordNumberBinding`/`SerdeBinding`/`TupleSerdeBinding`/`TupleSerdeKeyDataBinding`. None are documented |
| 22 | **Low** | `noxu-collections/Cargo.toml` | Declares a dependency on `noxu-bind`; `grep` finds zero usages in `crates/noxu-collections/src` |
| 23 | **Low** | `ByteArrayBinding` | Pass-through, but `entry_to_object` calls `entry.data().to_vec()` and `object_to_entry` calls `entry.set_data(object)` (which copies). Despite using `bytes::Bytes` underneath, the binding is not zero-copy. Rustdoc is silent on the cost |
| 24 | **Low** | `StoredList::pop` | If the highest-index key was deleted via `remove()` (not `pop`), the in-memory `next_index` is not adjusted, and `pop()` returns `None` despite the list still containing earlier entries |
| 25 | **Low** | `StoredList::len` | Delegates to `map.len()` which reports the database `count()`; this can disagree with `next_index` when `remove()` has been called, leading to surprising results in user code that mixes the two |
| 26 | **Low** | `StoredList::index_to_key` | Uses fixed 8-byte big-endian; `index_to_key` is `pub` but lacks any rustdoc cross-reference to `RecordNumberBinding`, and the two encodings are independently maintained |
| 27 | **Low** | `RecordNumberBinding::record_number_to_entry` | Inherent method skips the trait error path (returns `()`); only the trait `object_to_entry` is fallible. Inconsistent with the rest of the binding API |
| 28 | **Low** | `TupleOutput::write_sorted_packed_{int,long}` | Routes through `noxu_util::packed::write_sorted_i{32,64}` and uses `.expect("write_sorted_i32 to Vec is infallible")`; the assertion is fine, but the rest of the file inlines its packed encoders, so the dual codepath is undocumented |
| 29 | **Low** | `TupleInput::set_offset` | Accepts any `usize`, including past `buf.len()`; subsequent reads will fail with `BufferUnderflow`, but a plain bounds check at `set_offset` would be friendlier and matches the JE marker discipline |
| 30 | **Low** | `StoredIterator::new_from` | Does inclusive lower bound only; no exclusive variant, no upper bound — `BTreeMap::range` semantics are absent. Documented as such, but the user-facing chapter promises range scans |
| 31 | **Low** | `CollectionError::ConcurrentModification` | Defined and re-exported but never constructed anywhere in the crate. Either dead code, or the iterator is meant to detect concurrent mutation but does not |

---

## Detailed findings

### 1 (High) — `StoredMap` iteration walks an in-memory key index, not the database

`crates/noxu-collections/src/stored_map.rs:48-52`

```rust
pub struct StoredMap<'db> {
    db: &'db Database,
    read_only: bool,
    key_index: Mutex<BTreeSet<Vec<u8>>>,
}
```

`iter()`, `keys()`, `values()` all build the iterator from
`self.known_keys()` (lines 200, 209, 218), which is a snapshot of
`key_index`. The index is only populated by `put`, `remove` (which
removes), `get` on hit, `contains_key` on hit, and the explicit
`register_key{,s}`. Pre-existing rows in the database that have not been
touched through this exact `StoredMap` instance are not visible to
iteration. The crate-level rustdoc (`stored_map.rs:14-23`) does call this
out, but the user-facing mdbook does not, and the JE contract is
"iteration walks the database".

The crate-level lib.rs comment at `lib.rs:21-32` admits this and tells
users to call `register_key()` for pre-existing data. This is a documented
divergence from the JE contract, but it makes the type unsuitable as a
drop-in `BTreeMap`-like view for any database that already contains data.

### 2 (High) — `len`/`is_empty` use `db.count()`, iteration uses the index

`stored_map.rs:154-167`:

```rust
pub fn len(&self) -> Result<u64> {
    Ok(self.db.count()?)
}
pub fn is_empty(&self) -> Result<bool> {
    Ok(self.len()? == 0)
}
```

`iter()` (line 199) and `keys()` (line 207) read `self.known_keys()`. So
`len() == 5` while `iter().count() == 0` is reachable on first use
against a populated DB — the canonical Map-invariant
`len() == iter().count()` does not hold. This is the same shape of bug as
finding 1 but visible without ever calling iteration: `for_each` on a
fresh handle silently does nothing while `len()` looks fine.

The same delegation pattern appears in `StoredKeySet`
(`stored_key_set.rs:64-72`) and `StoredValueSet`
(`stored_value_set.rs:46-54`).

### 3 (High) — Collection operations cannot participate in a transaction

Every `Stored*` operation calls `self.db.{get,put,delete}(None, ...)`.
Examples:

- `stored_map.rs:71` — `self.db.get(None, &key_entry, &mut data_entry)`
- `stored_map.rs:101` — `self.db.put(None, &key_entry, &data_entry)`
- `stored_map.rs:124` — `self.db.delete(None, &key_entry)`
- `stored_iterator.rs:101` — `self.db.get(None, &key_entry, ...)`
- `stored_key_set.rs:54` — `self.db.get(None, ...)`

`grep` over `crates/noxu-collections/src` for `Transaction|begin_transaction|txn:`
returns matches only inside `transaction_runner.rs`. There is no public
constructor or method on any `Stored*` type that accepts a `&Transaction`.
This is a substantial divergence from the JE collections contract, where
the underlying operations honour a thread-bound or explicit transaction.

### 4 (High) — `TransactionRunner` produces a `Transaction` that nothing accepts

`transaction_runner.rs:75-101`:

```rust
pub fn run<F, R>(&self, f: F) -> Result<R>
where
    F: Fn(&Transaction) -> Result<R>,
{
    ...
    let txn = self.env.begin_transaction(None, None)?;
    match f(&txn) { ... }
}
```

The closure receives `&Transaction`, but the in-tree test at
`transaction_runner.rs:179-198` shows the only available pattern: the
closure does `db.put(None, &key, &val)`, with `None` instead of
`Some(&txn)`. Combined with finding 3, this means `TransactionRunner` is
useful only for callers who bypass the collection types and call
`Database::{get,put,delete}` themselves with `Some(txn)` — exactly the
pattern the collections layer is supposed to abstract away.

### 5 (High) — `StoredList::remove` rustdoc directly contradicts its body

`stored_list.rs:91-112`:

```rust
/// Removes the value at the given index and re-indexes all higher-indexed
/// elements so the list remains contiguous.
///
/// After removing the element at `index`, every element stored at indices
/// `index+1 .. next_index` is read and re-written at the decremented key
/// (`old_index - 1`), then the original key is removed.  `next_index` is
/// decremented by 1.
///
/// `StoredList.remove(int index)`: the re-numbers all higher-
/// indexed entries so that gaps are never left in the list.
///
/// Returns the removed value, or `None` if no value was at that index.
pub fn remove(&self, index: usize) -> Result<Option<Vec<u8>>> {
    // remove deletes the element at the given index
    // but does NOT compact / re-index remaining elements.  Gaps are left
    // in the index, consistent with behaviour.
    let key = Self::index_to_key(index);
    self.map.remove(&key)
}
```

The rustdoc paragraph promises compaction; the inline `//` comment two
lines below admits there is no compaction; the body never reads the
higher indices. Tests at `stored_list.rs:217-229` assert the
non-compacting behaviour (`list.get(1).unwrap() == None`). One of the two
contracts is wrong; both ship in the same function.

### 6 (High) — `StoredList::next_index` is volatile

`stored_list.rs:38-49`:

```rust
pub struct StoredList<'db> {
    map: StoredMap<'db>,
    next_index: std::sync::Mutex<usize>,
}
impl<'db> StoredList<'db> {
    pub fn new(db: &'db Database) -> Self {
        StoredList {
            map: StoredMap::new(db, false),
            next_index: std::sync::Mutex::new(0),
        }
    }
}
```

`new` always seeds `next_index` to 0. There is no recovery path that
scans the database for the maximum existing big-endian-keyed record. On a
restart with existing data, `push()` will start writing at key
`be_bytes(0u64)`, overwriting whatever lived there. JE solves this by
backing list semantics on a `Sequence` (atomically persisted on the
database itself); the docs in `stored-list.md:13` even reference exactly
this design — a `Sequence` parameter — but the implementation does not
take one.

### 7 (High) — `docs/src/collections/*.md` describes a different API

The user-facing chapters describe a generic, binding-parameterised,
transaction-threading API that is not in the source.

`docs/src/collections/stored-map.md:14-19`:

```rust
let map: StoredMap<u64, String> = StoredMap::new(
    db,
    TupleBinding::<u64>::new(),
    EntryBinding::<String>::new(),
);
```

Actual signature (`stored_map.rs:55`):

```rust
pub fn new(db: &'db Database, read_only: bool) -> Self
```

`stored-map.md:25-39` shows:

```rust
map.put(txn, &42u64, &"Alice".to_string())?;
let value: Option<String> = map.get(txn, &42u64)?;
for (k, v) in map.range(txn, &10u64..&50u64)? { ... }
```

Actual: `put(&[u8], &[u8])`, `get(&[u8])`, no `range`. Same problem in
`stored-set.md` (no `StoredSet` type exists; the crate has `StoredKeySet`
and `StoredValueSet`, neither of which has `add`/`contains`/`remove` in
the documented form), and in `stored-list.md` (no `Sequence` parameter,
no typed `V`, no `iter()` returning `(idx, value)` — `StoredList` has no
`iter` at all).

### 8 (High) — Sequence shape in `stored-list.md` is wrong twice

`docs/src/collections/stored-list.md:13`:

```rust
let seq = env.open_sequence(None, "events_seq", SequenceConfig::default())?;
```

`open_sequence` is defined on `Database`, not `Environment`
(`crates/noxu-db/src/database.rs:678`); the signature is
`fn open_sequence(&self, key: &DatabaseEntry, config: SequenceConfig)`,
not `(Option<&Transaction>, &str, SequenceConfig)`. And then:

```rust
let list: StoredList<String> = StoredList::new(db, seq, EntryBinding::<String>::new());
```

`StoredList::new` takes a single `&Database` (`stored_list.rs:46`). Two
distinct fabrications in three lines.

### 9 (Medium) — `put`/`remove` "previous value" is racy

`stored_map.rs:91-105` and `stored_map.rs:117-130` implement the
"previous value" semantics by issuing a separate `get` followed by
`put`/`delete` outside any shared transaction. Even setting aside the
no-txn problem of finding 3, two threads racing `put` against the same
key can both observe `None` and both report inserts as "new", or both
observe each other's value and report inconsistent prior values. The
`Map.put`/`Map.remove` previous-value contract requires a single atomic
operation; the JE implementation issues a single cursor put/delete that
returns the prior LN. This implementation cannot.

### 10 (Medium) — `clear()` swallows errors and only clears registered keys

`stored_map.rs:236-256`:

```rust
pub fn clear(&self) -> Result<()> {
    if self.read_only { return Err(CollectionError::ReadOnly); }
    let keys: Vec<Vec<u8>> = self.key_index.lock().unwrap().iter().cloned().collect();
    for key in &keys {
        let key_entry = DatabaseEntry::from_vec(key.clone());
        let _ = self.db.delete(None, &key_entry);
    }
    self.key_index.lock().unwrap().clear();
    Ok(())
}
```

Two problems:

1. Only registered keys are deleted. A `StoredMap::clear` against a
   freshly opened populated database is a no-op.
2. `let _ = self.db.delete(...)` silently discards every per-key error.
   A user who calls `clear()` and then `put()` may discover later that
   half the rows were never deleted (e.g. lock timeout, IO error).

The intended primitive — `Database::truncate_database` — is reachable
from `noxu-db` and would be the correct delegate.

### 11 (Medium) — `is_deadlock` matches a single error variant

`transaction_runner.rs:120-128`:

```rust
fn is_deadlock(err: &CollectionError) -> bool {
    match err {
        CollectionError::DatabaseError(db_err) => {
            matches!(db_err, noxu_db::NoxuError::DeadlockDetected)
        }
        _ => false,
    }
}
```

The JE `TransactionRunner` retries on the entire `LockConflictException`
hierarchy, which includes lock-conflict, lock-not-granted, and
lock-timeout. The test at `transaction_runner.rs:285-300` (`test_non_deadlock_error_no_retry`)
uses `NoxuError::Timeout` and asserts the call is **not** retried; the
test enshrines the bug.

### 12 (Medium) — Retry loop has no backoff

`transaction_runner.rs:80-103` is a `loop { ... continue; }` that
immediately re-runs the closure. No `thread::sleep`, no exponential
backoff, no jitter, no cap on total elapsed time. Under sustained
contention this becomes a tight retry-storm and starves the conflicting
transaction.

### 13 (Medium) — Retry closure must be `Fn`

`transaction_runner.rs:77`: `F: Fn(&Transaction) -> Result<R>`. JE's
`TransactionWorker` is a single `run()` method on a stateful object;
the `Fn` bound forces Rust callers to push state into `Cell` /
`AtomicU32` / `Mutex`. The in-tree retry test
(`transaction_runner.rs:206-225`) demonstrates this: it uses
`std::sync::atomic::AtomicU32` to count attempts. `FnMut` would be a
strict superset and is the standard Rust idiom for "may be invoked
several times".

### 14 (Medium) — `run_without_txn` is a no-op wrapper

`transaction_runner.rs:107-116`:

```rust
pub fn run_without_txn<F, R>(&self, f: F) -> Result<R>
where
    F: Fn() -> Result<R>,
{
    f()
}
```

Does not use `self`, does not use `self.env`, does not retry, does
nothing the bare call doesn't. The rustdoc claims it is for non-
transactional environments, but it cannot detect or enforce
non-transactionality.

### 15 (Medium) — `StoredKeySet` / `StoredValueSet` inherit the index problem

Same shape as findings 1–2:
`stored_key_set.rs:34-37`, `stored_value_set.rs:34-37`. Iteration walks
the in-memory `BTreeSet`, but `len()` is `db.count()`. Neither has any
mutating method (`add` / `remove`), so they are pure read-only views,
which makes the index-vs-DB mismatch the more visible footgun.

### 16 (Medium) — `StoredIterator` lifecycle

`stored_iterator.rs:13-19` documents the type as if it wraps a cursor:

> Provides iterators over database records. Unlike the StoredIterator
> which wraps a live cursor, these iterators work from a snapshot of
> sorted keys and fetch values on demand from the database.

Inside the type itself there is no cursor — `next()` issues
`db.get(None, &key_entry, ...)` per element (`stored_iterator.rs:99`).
There is no `Drop` impl, no `close()` method, and no resource that needs
releasing. The JE contract requires `StoredIterator.close()`; here, the
documented "must close" guidance does not apply, but the rustdoc is
written to mention it (in the negative) and confuses the reader.

### 17 (Medium) — Iterator silently swallows unexpected statuses

`stored_iterator.rs:103-118`:

```rust
match self.db.get(None, &key_entry, &mut data_entry) {
    Ok(OperationStatus::Success) => { Some(Ok((key_bytes, value))) }
    Ok(OperationStatus::NotFound) => { self.next() }
    Ok(_) => { self.next() }
    Err(e) => Some(Err(CollectionError::DatabaseError(e))),
}
```

The catchall `Ok(_)` silently advances. If `OperationStatus` ever grows a
new variant (`KeyExists`, `KeyEmpty`, `BufferTooSmall`, …) the iterator
will silently drop those records.

### 18 (Medium) — `EntryBinding` cannot represent missing-vs-empty data

`DatabaseEntry::data` (`crates/noxu-db/src/database_entry.rs:128`) is

```rust
pub fn data(&self) -> &[u8] {
    self.get_data().unwrap_or(&[])
}
```

`is_empty` (line 187) returns true for both `data == None` and `size ==
0`. Every binding routes through `entry.data()` (e.g.
`byte_array_binding.rs:26`, `record_number_binding.rs:23`,
`tuple_binding.rs:21`, `serde_binding.rs:88`). There is no way for a
binding to express "the entry was unset". This is a real distinction in
JE (`DatabaseEntry.partial` / `getData() == null`); none of the rustdoc
mentions it.

`StoredMap::get` at `stored_map.rs:69-83` similarly collapses the
distinction: a successful read with `data: None` is reported as
`Some(b"".to_vec())`.

### 19 (Medium) — `SerdeBinding` has no schema management

`crates/noxu-bind/src/serial/simple_serial.rs` is non-self-describing:
struct fields are written in declaration order with no prefix
(`simple_serial.rs:342-348`, `serialize_struct`), and structs are read
by walking the same field list (`simple_serial.rs:786-789`). Adding a
field, removing a field, or reordering fields silently desyncs reader
from writer. JE's `SerialBinding` solved this by storing a class
descriptor in a `StoredClassCatalog`; this crate has no catalog
(`grep -i 'catalog'` over `crates/noxu-bind` returns no hits).

`docs/src/collections/entity-persistence.md` already calls this out for
the DPL ("the persistence layer does not store a schema with each
record"), but `SerdeBinding`'s rustdoc
(`crates/noxu-bind/src/serial/serde_binding.rs:1-13`) does not, and
neither does `docs/src/getting-started/bindings.md`.

### 20 (Medium) — `simple_serial` truncates lengths over 4 GiB

`simple_serial.rs:223-229`:

```rust
fn serialize_str(self, v: &str) -> std::result::Result<(), SerError> {
    let len = v.len() as u32;
    self.output.extend_from_slice(&len.to_be_bytes());
    self.output.extend_from_slice(v.as_bytes());
    Ok(())
}
```

`v.len()` is `usize`. `as u32` truncates for any `len >= 2^32`, the
length prefix lies, and the next field is misaligned. Same problem in
`serialize_bytes` (line 230). This is unlikely in practice but the
serializer should fail rather than silently corrupt.

### 21 (Medium) — `bindings.md` covers a quarter of the binding surface

`docs/src/getting-started/bindings.md` shows a 4-row table:

| Type | Binding |
|---|---|
| `i32` | `IntBinding` |
| `i64` | `LongBinding` |
| `f64` | `SortedDoubleBinding` |
| `String` | `StringBinding` |

`crates/noxu-bind/src/lib.rs:30-39` actually re-exports
`BoolBinding, ByteBinding, CharBinding, DoubleBinding, FloatBinding,
IntBinding, LongBinding, PackedIntBinding, PackedLongBinding,
ShortBinding, SortedDoubleBinding, SortedFloatBinding,
SortedPackedIntBinding, SortedPackedLongBinding, StringBinding,
ByteArrayBinding, RecordNumberBinding, SerdeBinding, TupleSerdeBinding,
TupleSerdeKeyDataBinding, SortKey, TupleBinding, TupleInput,
TupleOutput`. None of the 19 omitted types is mentioned anywhere in
the user docs, including the distinction between sortable and
non-sortable encodings (which is the most common foot-gun: `FloatBinding`
silently produces unsorted keys).

### 22 (Low) — `noxu-bind` is an unused dependency of `noxu-collections`

`crates/noxu-collections/Cargo.toml` lines:

```toml
noxu-bind = { workspace = true }
```

`grep -r 'noxu_bind\|EntryBinding\|TupleBinding'` in
`crates/noxu-collections/src` returns nothing. The two crates are linked
by Cargo but not by code; this is consistent with the docs (which
expect bindings to flow through collections) describing an aspirational
API that the source has not yet caught up with.

### 23 (Low) — `ByteArrayBinding` is documented as "pass-through" but copies

`byte_array_binding.rs:14-37`:

```rust
pub struct ByteArrayBinding;
impl EntryBinding<Vec<u8>> for ByteArrayBinding {
    fn entry_to_object(&self, entry: &DatabaseEntry) -> Result<Vec<u8>> {
        Ok(entry.data().to_vec())
    }
    fn object_to_entry(&self, object: &Vec<u8>, entry: &mut DatabaseEntry)
        -> Result<()>
    {
        entry.set_data(object);
        Ok(())
    }
}
```

`entry.data().to_vec()` allocates on read; `entry.set_data` calls
`Bytes::copy_from_slice` (`database_entry.rs:99`). `DatabaseEntry`
already wraps `bytes::Bytes`, which makes a true zero-copy binding
possible (return `&[u8]` borrowed from the entry, or `Bytes::clone()`),
but the trait shape (`fn entry_to_object(&self, &DatabaseEntry) -> Result<T>`)
forces an owned `T`. Worth either documenting the inevitable copy or
adding a sibling trait that returns a borrow.

### 24, 25 (Low) — `StoredList::pop` / `len` interactions with `remove`

`stored_list.rs:75-89`:

```rust
pub fn pop(&self) -> Result<Option<Vec<u8>>> {
    let mut next = self.next_index.lock().unwrap();
    if *next == 0 { return Ok(None); }
    let index = *next - 1;
    let key = Self::index_to_key(index);
    let val = self.map.remove(&key)?;
    if val.is_some() { *next = index; }
    Ok(val)
}
```

Sequence: `push("a"); push("b"); remove(1); pop()`. After `remove(1)`,
key 1 is gone, but `next_index` is still 2. `pop()` looks for key 1,
finds nothing, returns `None`. The list still contains "a" at index 0
but `pop()` reports empty.

`len()` (`stored_list.rs:114-116`) returns `self.map.len()`, which is
`db.count()` — so after the same sequence, `len() == 1` while
`pop()` returns `None` and `next_index() == 2`. Three accessors, three
different "sizes".

### 26 (Low) — `StoredList::index_to_key` independent of `RecordNumberBinding`

Both `stored_list.rs:51-53` and
`crates/noxu-bind/src/record_number_binding.rs:38-40` independently
implement `(u64).to_be_bytes()`-as-key. Identical encoding, two
unconnected APIs, no doc cross-reference. If the encoding ever changes
in one, the other will silently disagree.

### 27 (Low) — `RecordNumberBinding` has an inherent infallible companion

`record_number_binding.rs:33-37`:

```rust
pub fn record_number_to_entry(number: u64, entry: &mut DatabaseEntry) {
    entry.set_data(&number.to_be_bytes());
}
```

The trait method (`object_to_entry`) returns `Result<()>`. The inherent
method returns `()`. Both wrap exactly the same code. The duplication
exists only to provide a fallible-vs-infallible split that the trait
contract doesn't actually need.

### 28 (Low) — Sorted packed encoders dispatch to `noxu-util`, others inline

`tuple_output.rs:381-406`:

```rust
pub fn write_sorted_packed_int(&mut self, val: i32) {
    write_sorted_i32(&mut self.buf, val)
        .expect("write_sorted_i32 to Vec is infallible");
}
```

The `expect` is fine (writing into a `Vec` is infallible), but `write_packed_int`
just above (`tuple_output.rs:215-258`) inlines the encoder by hand. There
are two different policies for "where lives the bit-twiddling": packed
in this file, sorted-packed in `noxu-util`. A short note explaining
why (or moving everything one way) would help maintenance.

### 29 (Low) — `TupleInput::set_offset` is unchecked

`tuple_input.rs:48-50`:

```rust
pub fn set_offset(&mut self, offset: usize) {
    self.off = offset;
}
```

Setting an offset past `buf.len()` does not error; subsequent reads
will fail with `BufferUnderflow`. JE's `TupleInput` markers are
checked. Either trim to `buf.len()`, or return `Result`.

### 30 (Low) — `iter_from` is the only range primitive

`stored_iterator.rs:64-72` and `stored_sorted_map.rs:65-79` provide
`new_from(start_key)` — inclusive lower bound only. There is no
`iter_to`, no `iter_range`, and no exclusive variant. The user-facing
chapter (`stored-map.md:35-39`) advertises `map.range(txn, &10u64..&50u64)`
which the type does not have.

### 31 (Low) — `CollectionError::ConcurrentModification` is dead

Defined at `error.rs:24-25`, re-exported at `lib.rs:64`, and `grep`
finds no constructor anywhere in the crate. Either dead, or the
iterator was meant to detect concurrent index mutation and never grew
the check.

---

## Coverage gaps

This audit did not exercise:

- The behavioural test suite — only read tests; no `cargo test`/`cargo nextest run`.
- Concurrent-iterator semantics under live writers in another thread.
- Crash-recovery interaction (what happens to a `StoredList` whose backing
  database is reopened after a crash; see finding 6).
- `noxu-persist` (DPL), beyond noting that `entity-persistence.md`
  partially overlaps with `SerdeBinding`'s schema-evolution gap (finding 19).
- Performance: encoded-byte sizes, allocation counts, `bytes::Bytes`
  zero-copy claims under realistic loads.
- The proptest suite under `dev-dependencies` — only listed, not read.
- Property-based tests for the tuple encoding round-trip (random `(i64,
  i64) → encode → decode → ==` and `a < b ↔ encode(a) < encode(b)`). The
  in-module unit tests cover hand-picked extremes and boundaries; they do
  not exhaust the encoding space.
- `XaTransaction` interaction with collections (`noxu-xa` was not in scope).
- Replication: are collection writes streamed to replicas correctly when
  performed without a transaction (finding 3)?

---

## Summary

`noxu-bind`'s low-level encoding primitives — `TupleInput` / `TupleOutput`,
`SortKey`, the per-primitive bindings — are the strongest part of the
audited surface. The encoding contracts (sign-bit-flipped big-endian,
sortable IEEE-754, escaped-null strings, sorted packed integers) are
consistent between the writer and reader, well-documented in rustdoc,
and exercised by sort-order tests.

Above that layer, the gap between what is shipped and what the
user-facing documentation describes is wide. The mdbook chapters under
`docs/src/collections/` describe a typed, binding-parameterised,
transaction-threading API that the source does not implement; the
implemented API is `&[u8]`-keyed, locks every operation into
auto-commit (`None`), and tracks "what is in the map" through an
in-memory `BTreeSet` that diverges from `db.count()` the moment any
other writer (or a fresh restart) is involved (findings 1–8).

`TransactionRunner` is shaped like the JE class but cannot in practice
provide retry-around-collection because the closure-supplied
`&Transaction` is not threadable into any `Stored*` operation
(findings 3, 4); even within its own scope it lacks backoff and
classifies only one of several retryable lock errors (findings 11, 12).

`StoredList` carries a documented-vs-implemented contradiction inside
the same function body (finding 5) and has no persistent
`next_index` recovery (finding 6), which makes it actively unsafe to
reopen against existing data.

`SerdeBinding` / `simple_serial` lacks the schema-catalog discipline
that JE's `SerialBinding` provides (finding 19); the user gets no
warning that adding a struct field will silently corrupt their
existing records.

The recommended order of operations, if a future change is in scope, is
roughly: pick whether the docs or the code is the spec, fix one to match
the other (findings 7, 8, 21), then close the transaction-threading and
key-index gaps (3, 4, 1, 2), then schema/version the serial binding (19).
The encoding layer itself does not need work.
