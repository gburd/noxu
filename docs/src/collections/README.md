# Collections and Persistence

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

## In This Chapter

1. [StoredMap](stored-map.md) — `BTreeMap`-like view backed by a primary database
2. [StoredSet](stored-set.md) — set semantics over a sorted-duplicate database
3. [StoredList](stored-list.md) — sequence-indexed list backed by a `Sequence`
4. [Entity Persistence (DPL)](entity-persistence.md) — `Entity` /
   `EntitySerializer` traits, `EntityStore`, primary and secondary
   indexes
