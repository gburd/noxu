# Collections and Persistence

This chapter covers the higher-level APIs built on top of the core Noxu DB
key-value store:

- **`noxu-collections`** — iterator-based `StoredMap`, `StoredSet`, and
  `StoredList` views that wrap databases with idiomatic Rust collection
  semantics. Corresponds to `noxu_collections` in Noxu DB.

- **`noxu-persist`** — the Direct Persistence Layer (DPL), which provides
  derive macros for serialising Rust structs directly into Noxu databases
  without manual key/value encoding. Corresponds to `noxu_persist`
  in Noxu DB.

## In This Chapter

1. [StoredMap](stored-map.md) — `BTreeMap`-like view backed by a primary database
2. [StoredSet](stored-set.md) — set semantics over a sorted-duplicate database
3. [StoredList](stored-list.md) — sequence-indexed list backed by a `Sequence`
4. [Entity Persistence (DPL)](entity-persistence.md) — derive macros, `EntityStore`, annotations
