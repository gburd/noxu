# Collections and Persistence

> **v1.6 capability matrix:** see
> [Introduction → v1.6 capability matrix](../introduction.md#v16-capability-matrix).

This chapter covers the higher-level APIs built on top of the core
Noxu DB key-value store:

- **`noxu::collections`** (via the `noxu` umbrella crate) — typed `StoredMap<K, V>`, `StoredSortedMap<K, V>`,
  `StoredKeySet<K>`, `StoredValueSet<V>`, and `StoredList<V>` views
  parameterised by `noxu::bind::EntryBinding` implementations.  Every
  method takes `txn: Option<&Transaction>` so the views compose with
  user-driven transactions.

- **`noxu::persist`** (via the `noxu` umbrella crate) — the Direct Persistence Layer (DPL), which lets
  you store Rust structs in Noxu databases through a typed
  `PrimaryIndex<K, E>` instead of hand-written `DatabaseEntry` byte
  slices.  The wiring is explicit (you implement `Entity` and an
  `EntitySerializer` for your type) — there are no derive macros today.

## v1.6 collections — what's in scope

The v1.6 collections API provides:

1. **Typed `Stored*` surface.**  `StoredMap<K, V, KB, VB>` is now
   parameterised by `EntryBinding`s for keys and values.  Same for
   `StoredSortedMap`, `StoredKeySet`, `StoredValueSet`, and
   `StoredList`.  Construct as:

   ```rust,ignore
   let map: StoredMap<i32, String, _, _> =
       StoredMap::new(&db, IntBinding, StringBinding);
   ```

2. **`Option<&Transaction>` threaded through every method.**  Pass
   `None` for auto-commit semantics, `Some(&t)` to participate in a
   user transaction:

   ```rust,ignore
   map.put(None, &1, &"alpha".to_string())?;            // auto-commit
   map.put(Some(&txn), &2, &"beta".to_string())?;       // user txn
   ```

3. **`TransactionRunner` drives `Stored*` methods.**  The
   `&Transaction` the runner supplies can now be threaded straight
   into any `Stored*` method.  The runner retries on every
   `is_retryable()` error (deadlock, lock conflict, lock timeout,
   transaction timeout, lock preempted) with jittered exponential
   backoff (default: 10 retries, 10 ms base, 1 s ceiling, ±25%
   jitter; all configurable).

4. **`StoredList::remove` compacts.**  Removing index `i` shifts every
   record at index `j > i` down by one slot and decrements
   `next_index`.  Run under the supplied txn, so passing a
   `Some(&txn)` makes the compaction crash-atomic.

5. **`StoredList::open` recovers `next_index` across reopen.**  Use
   `StoredList::open(&db, value_binding)` when reopening a database
   with existing entries.  `StoredList::new(&db, value_binding)` is
   preserved for empty / fresh databases (it does not scan).

## In this chapter

1. [StoredMap](stored-map.md) — typed map view with sorted-map navigation
2. [StoredSet](stored-set.md) — typed `StoredKeySet<K>` /
   `StoredValueSet<V>`
3. [StoredList](stored-list.md) — typed indexed list with shift-down
   compaction
4. [Entity Persistence (DPL)](entity-persistence.md) — `Entity` /
   `EntitySerializer` traits, `EntityStore`, primary and secondary
   indexes
