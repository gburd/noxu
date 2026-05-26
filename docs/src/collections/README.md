# Collections and Persistence

> **v1.5 capability matrix:** see
> [Introduction → v1.5 capability matrix](../introduction.md#v15-capability-matrix).

This chapter covers the higher-level APIs built on top of the core Noxu DB
key-value store:

- **`noxu-collections`** — iterator-based `StoredMap`, `StoredSet`, and
  `StoredList` views that wrap databases with idiomatic Rust collection
  semantics. Corresponds to `noxu_collections` in Noxu DB.

- **`noxu-persist`** — the Direct Persistence Layer (DPL), which lets
  you store Rust structs in Noxu databases through a typed
  `PrimaryIndex<K, E>` instead of hand-written `DatabaseEntry` byte
  slices. The wiring is explicit (you implement `Entity` and an
  `EntitySerializer` for your type) — there are no derive macros today.

## v1.5 collections — what's in scope

The v1.5 collections surface is intentionally narrower than the
BDB-JE `com.sleepycat.collections` contract.  These constraints are
tracked by the May 2026 collections/bind API audit and are scheduled
for revisit in v1.6.

1. **`Stored*` operations are auto-commit only.**  Every `get` /
   `put` / `remove` / `iter` call on `StoredMap`, `StoredSortedMap`,
   `StoredList`, `StoredKeySet`, and `StoredValueSet` issues the
   underlying `Database` call with `txn = None`.  There is no way
   to thread an externally-begun `noxu_db::Transaction` into a
   collection method in v1.5.  (Audit findings #1, #3, #4.)

2. **`TransactionRunner` does not drive `Stored*` calls.**  The
   `&Transaction` it supplies cannot be passed into any `Stored*`
   method in v1.5.  Use the runner with the raw `Database` /
   `Cursor` API (passing `Some(&txn)` to `db.put` / `db.delete` /
   `db.open_cursor`), or use auto-commit `Stored*` ops without a
   runner.  See the per-chapter notes below.

3. **`StoredList::new` does not recover the next-index counter.**
   Use [`StoredList::open`](stored-list.md#creating-a-storedlist)
   when reopening a database that already contains entries.
   (Audit finding #6.)

4. **`StoredList::remove` does not compact.**  It is a single-key
   delete; it leaves a hole at the removed index and does not
   re-number higher indices.  (Audit finding #5.)

5. **`SerdeBinding` is now version-prefixed.**  Every payload
   begins with two bytes (`0xCB` magic + `0x01` version).
   This is a breaking change: data written by earlier 1.5 release
   candidates is not readable without migration.  See
   [the bindings chapter](../getting-started/bindings.md#serdebinding-version-prefix-v15).
   (Audit finding #19.)

## In this chapter

1. [StoredMap](stored-map.md) — `BTreeMap`-like view backed by a primary database
2. [StoredSet](stored-set.md) — `&[u8]`-keyed `StoredKeySet` / `StoredValueSet`
3. [StoredList](stored-list.md) — sequence-indexed list (with reopen support)
4. [Entity Persistence (DPL)](entity-persistence.md) — `Entity` /
   `EntitySerializer` traits, `EntityStore`, primary and secondary
   indexes
