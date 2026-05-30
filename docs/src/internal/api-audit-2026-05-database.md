# Database Subsystem API Audit — May 2026

**Auditor:** automated read-only review (sub-agent)
**Date:** 2026-05-25
**Branch reviewed:** `fix/cursor-search-gte-cross-bin-walk`
**Trigger:** Two recent SearchGte cursor bugs (v1.4.2 / v1.4.3) prompted a
parallel review of the *non-cursor* `Database` data-path API for similar
latent issues.

This audit is read-only — no source files were modified.

---

## Scope

The audit covers the public `Database` CRUD API and its supporting types
in `noxu-db`, plus the underlying `DatabaseImpl` in `noxu-dbi` to the
extent that it shapes user-visible semantics:

- `crates/noxu-db/src/database.rs`
- `crates/noxu-db/src/database_entry.rs`
- `crates/noxu-db/src/database_config.rs`
- `crates/noxu-db/src/database_stats.rs`
- `crates/noxu-db/src/byte_comparator.rs`
- `crates/noxu-db/src/cache_mode.rs`
- `crates/noxu-db/src/durability.rs`
- `crates/noxu-db/src/read_options.rs`
- `crates/noxu-db/src/write_options.rs`
- `crates/noxu-db/src/lock_mode.rs`
- `crates/noxu-db/src/environment.rs` (only the `open_database`,
  `truncate_database`, `remove_database` plumbing)
- `crates/noxu-dbi/src/database_impl.rs`
- `crates/noxu-dbi/src/cursor_impl.rs` (only the `put` / `put_dup` /
  `delete` paths called from `Database::put` and `Database::delete`)
- `crates/noxu-dbi/src/environment_impl.rs` (only `truncate_database`)
- `docs/src/getting-started/databases.md`
- `docs/src/getting-started/reading-writing.md`
- `docs/src/getting-started/records.md`

Out of scope (covered by other audits or unrelated to this review):
recovery, replication, secondary databases / `SecondaryCursor`, join
cursors, sequence semantics, the cleaner, the evictor.

---

## Methodology

1. Enumerated every `pub fn` on `Database`, `DatabaseEntry`,
   `DatabaseConfig`, `DatabaseStats`, `ReadOptions`, `WriteOptions`,
   `LockMode`, `CacheMode`, `Durability`, `ByteComparator`.
2. Read the rustdoc and the mdbook chapters referenced above.
3. Read each implementation, walking into `noxu-dbi` where the public API
   forwards.
4. Compared each method against Berkeley DB / BDB-JE semantics
   (`Database.put` / `putNoOverwrite` / `putNoDupData` / `get` /
   `getSearchBoth` / `delete` / `count` / `truncate` / `sync` /
   `getStats` / `preload` / `verify` / `openSequence`).
5. Cross-checked the `SearchGte cross-BIN walk` style of bug —
   correctness of the cursor-driven primitive used by `get` /
   `put` / `delete`, particularly on sorted-duplicate databases.

Tests in the audited crates were *not* run; this is a static review.

---

## Findings table

| # | Severity | Area | Summary |
|---|---|---|---|
| 1 | High | sorted-dup | `Database::count()` always returns 0 on sorted-duplicate DBs — `put_dup` bypasses `apply_tree_insert` so the entry counter is never incremented |
| 2 | High | sorted-dup | `Database::delete(key)` removes only **one** duplicate on sorted-dup DBs, not all records with that key (BDB-JE deletes all) |
| 3 | High | locking | `Database::put` partial-put pre-fetch uses a non-txn cursor inside a transactional caller — self-deadlock if the same key was already write-locked by the caller's txn |
| 4 | High | LockMode | `Database::get_with_options(LockMode::Rmw)` does **not** acquire a write lock; `LockMode::ReadCommitted` is also indistinguishable from `Default` |
| 5 | High | durability | `Environment::truncate_database` is non-WAL and non-transactional — truncation can be lost on crash even when the API accepts a `txn` argument |
| 6 | Medium | config | `DatabaseConfig::override_btree_comparator` / `override_duplicate_comparator` flags exist but no API attaches a comparator function — silently ignored |
| 7 | Medium | config | `DatabaseConfig::key_prefixing`, `replicated`, `bin_delta`, `cache_mode`, `exclusive`, `use_existing_config` are stored but **never plumbed** into `noxu_dbi::DatabaseConfig` by `Environment::open_database` |
| 8 | Medium | TTL | `put_with_options` writes the record durably **before** applying the TTL via `update_key_expiration`, which itself is not WAL-logged — TTL is lost on crash; also TTL update happens regardless of `update_ttl` flag |
| 9 | Medium | preload | `Database::preload` does not actually load LNs — it only walks BIN/IN structure via `collect_btree_stats`; `lns_loaded` is reported as `n_entries` even though no LN was touched |
| 10 | Medium | preload | `PreloadConfig::max_millis` is silently ignored (`reserved for future time-bounded preload`) |
| 11 | Medium | empty-key | `put` / `put_no_overwrite` accept a `DatabaseEntry` whose `data == None` and silently coerce it to an empty `&[]` key, while `get` / `delete` return `NotFound` for the same input — inconsistent |
| 12 | Medium | truncate | `truncate_database` does not enforce "no open handles" (unlike `remove_database` / `rename_database`) and replaces the live `Tree` underneath any cursors that may already be positioned in it |
| 13 | Medium | truncate | After truncation the new tree is created with `noxu_tree::Tree::new(...)` and is **not** wired to the environment's memory counter (the prior tree's counter is gone); subsequent inserts into the truncated DB are not budget-tracked |
| 14 | Medium | docs | Most `Database` methods that go beyond `get`/`put`/`delete` are missing from the user-facing mdbook (`truncate_database`, `sync`, `preload`, `verify`, `get_stats`, `put_no_overwrite`, `get_with_options`, `put_with_options`, `open_sequence`, `scan_all_kv`) |
| 15 | Low | docs | `Database::count` rustdoc says "approximate" but the implementation is **exact** for non-dup DBs (atomic counter incremented per LN insert/delete) |
| 16 | Low | docs | `Database::sync` rustdoc says "Flushes all pending writes for **this database**" but the call goes to the global `LogManager::flush_sync` — the granularity claim is misleading |
| 17 | Low | docs | Several rustdoc strings still contain stale BDB-JE template fragments (`": TXN_NO_SYNC"`, leading colons with no class name) — a cosmetic artefact of the porting-from-Java workflow |
| 18 | Low | API | `txn` parameters on `Environment::remove_database`, `truncate_database`, `rename_database` and `Database::open_cursor` are accepted but documented/named `_txn` — they are unused. The signature suggests transactional semantics that are not provided |
| 19 | Low | dead code | `noxu_db::ByteComparator` trait, `DefaultByteComparator`, and `compare_unsigned` are re-exported from the crate root but unreachable from any user path; `DatabaseImpl::compare_keys` / `set_bt_comparator` are also unused on the data path |
| 20 | Low | partial put | Partial `put` semantics differ subtly from BDB-JE: when `new_bytes.len() < partial_length` the implementation copies only `new_bytes.len()` bytes (silently truncating); when `new_bytes.len() > partial_length` excess bytes are silently dropped — neither case is documented |
| 21 | Low | API | `DatabaseConfig::node_max_entries` is documented as `0 = use default` but `Environment::open_database` only forwards it when `> 0`; the `noxu_tree::Tree::new(_, max_entries: usize)` contract is undocumented (it accepts `0` itself but no test covers `max_entries = 0`) |
| 22 | Low | API | `Database::put` accepts `data` that is itself partial (`is_partial() == true`) and synthesises a record from a non-existent key by zero-filling `[0..off]` — there is no documentation that partial puts to a missing key auto-create a zero-prefixed record |
| 23 | Info | observability | `put_with_options` and `get_with_options` do **not** emit the `observe_*` spans/counters that `put`/`get`/`delete` do — observability is silently asymmetric |

Severity legend:

- **High** — incorrect results or durability loss for a documented user-facing operation.
- **Medium** — feature is exposed but does not do what its API/docs claim.
- **Low** — cosmetic, documentation, or dead-code issue with no user-visible
  correctness impact in the common path.
- **Info** — observation, not a defect.

---

## Detailed findings

### 1. (High) `Database::count()` returns 0 on sorted-duplicate databases

**Where:**
`crates/noxu-dbi/src/cursor_impl.rs:1852-1949` (`put_dup`),
`crates/noxu-dbi/src/cursor_impl.rs:303-313` (`apply_tree_insert`),
`crates/noxu-db/src/database.rs:711-715` (`Database::count`).

**Symptom.** `Database::count()` reads `db_impl.entry_count()` which is an
atomic counter incremented by `apply_tree_insert`. For sorted-dup DBs the
put path goes through `cursor_impl::put_dup` (line 1745: `if
self.is_sorted_dup() { return self.put_dup(...) }`) and that function
calls `tree.insert(...)` directly — it never goes through
`apply_tree_insert`, so the counter is never bumped.

```rust
// cursor_impl.rs:1909-1916  (PutMode::NoDupData / NoOverwrite branch)
{
    let db = self.db_impl.read();
    if let Some(tree) = db.get_real_tree() {
        let _ = tree.insert(two_part_key.clone(), vec![], new_lsn);
    }
}
// (no call to apply_tree_insert; no increment_entry_count)
```

The same is true of the `PutMode::Overwrite` and `PutMode::Current`
branches at `cursor_impl.rs:1930-1949` and `cursor_impl.rs:1881-1898`.

Deletes still go through `apply_tree_delete` (`cursor_impl.rs:2056`),
which decrements the counter. `decrement_entry_count` saturates at 0
(`database_impl.rs:236-258`), so:

- inserts on sorted-dup → counter unchanged
- deletes on sorted-dup → counter unchanged (already 0)
- `Database::count()` → returns 0 forever

**Risk.** `count()` is the documented O(1) record-count API
(`docs/src/getting-started/databases.md:84`); applications that branch on
`db.count() == 0` will incorrectly conclude that a sorted-dup database is
empty.

**Suggested fix.** In each `put_dup` arm, replace the inline
`tree.insert(...)` with `self.apply_tree_insert(...)`, or have `put_dup`
call a shared helper that respects the "is_new" return from `tree.insert`.

---

### 2. (High) `Database::delete(key)` deletes only one duplicate on sorted-dup DBs

**Where:**
`crates/noxu-db/src/database.rs:534-583` (`Database::delete`),
`crates/noxu-dbi/src/cursor_impl.rs:2040-2068` (`CursorImpl::delete`),
`crates/noxu-dbi/src/cursor_impl.rs:516-770` (`CursorImpl::search` /
`search_dup`).

**Symptom.** `Database::delete` issues
`cursor.search(key_bytes, None, SearchMode::Set)` and then
`cursor.delete()`. On a sorted-dup DB, `search_dup` positions the cursor
on the *first* (key, data) two-part key for that primary key, and
`cursor.delete()` removes only that single slot. BDB-JE's
`Database.delete(txn, key)` removes **all** records with that key.

**Risk.** Silent partial deletes on duplicate databases. After
`db.delete(key)`, the DB still contains every duplicate except one,
which is contrary to documented BDB-JE semantics.

**Suggested fix.** Loop in `Database::delete` while `cursor.search(...,
SearchMode::Set)` returns `Success`, deleting each positioned record;
or push the loop into `CursorImpl::delete_all_for_key` and call from
`Database::delete`.

---

### 3. (High) Partial `put` pre-fetch self-deadlocks under a transaction

**Where:** `crates/noxu-db/src/database.rs:386-416` (`Database::put`,
partial branch).

**Symptom.** When `data.is_partial()` is true and a transaction `txn`
is provided, the partial-put read-modify-write path creates a
**non-transactional** cursor for the pre-fetch:

```rust
// database.rs:392-410
let mut tmp_cursor = self.make_cursor();   // <-- not make_cursor_for_txn(t)
match tmp_cursor
    .search(key_bytes, None, noxu_dbi::SearchMode::Set)
    ...
```

If the same transaction has previously written `key` (and therefore
holds a WRITE lock on the slot's LSN via the txn locker), the
non-transactional cursor will try to acquire a READ lock on the same
LSN under a fresh thread-locker. The lock manager will block the read
because the WRITE lock is held by a different locker — the transaction
deadlocks against itself.

**Risk.** Hard hang or deadlock-detector abort for any user code that
does a partial put on a record they wrote earlier in the same
transaction — a common pattern.

**Suggested fix.** Use `make_cursor_for_txn(t)` for the pre-fetch when
`txn` is `Some`, mirroring the txn cursor used for the actual write at
line 419.

---

### 4. (High) `LockMode::Rmw` and `LockMode::ReadCommitted` are no-ops in `get_with_options`

**Where:**
`crates/noxu-db/src/database.rs:325-339` (`Database::get_with_options`),
`crates/noxu-db/src/lock_mode.rs:32-39` (`LockMode::Rmw` rustdoc).

**Symptom.** The match in `get_with_options` only special-cases
`ReadUncommitted`:

```rust
// database.rs:329-336
let mut cursor = match opts.lock_mode {
    LockMode::ReadUncommitted => self.make_cursor_no_lock(),
    _ => match txn {
        Some(t) => self.make_cursor_for_txn(t),
        None => self.make_cursor(),
    },
};
```

`LockMode::Rmw` is documented (`lock_mode.rs:32-39`) as "Acquires a
write lock immediately, even though the operation is a read." — the
implementation does no such thing. `LockMode::ReadCommitted` is
documented (`lock_mode.rs:23-29`) as releasing the read lock when the
cursor moves; the implementation makes no distinction from `Default`.
Cursor probes (`SearchMode::Set` path at `cursor_impl.rs:543-590`)
acquire a regular READ lock via `lock_ln`.

**Risk.** Any user relying on `Rmw` to avoid lock-upgrade deadlocks
(the documented use case) will hit the deadlock anyway. A user relying
on `ReadCommitted` semantics gets whichever isolation the default txn
provides, with no API contract that the read lock is released early.

**Suggested fix.** Wire the lock mode through to
`CursorImpl::search`/`lock_ln` or reject the unsupported variants with
`NoxuError::OperationNotAllowed("LockMode::Rmw not yet supported")`.

---

### 5. (High) `truncate_database` is non-WAL, non-txn, ignores its `txn` argument

**Where:**
`crates/noxu-db/src/environment.rs:511-525`,
`crates/noxu-dbi/src/environment_impl.rs:865-890`.

**Symptom.** `Environment::truncate_database(txn, name)` accepts a
`txn` argument but the parameter is named `_txn` and dropped. The
inner `EnvironmentImpl::truncate_database` performs:

```rust
// environment_impl.rs:881-887
let new_tree =
    noxu_tree::Tree::new(db_id.as_i64() as u64, max_entries);
db_guard.set_recovered_tree(new_tree); // resets entry_count to 0
```

No log record is written. There is no checkpoint. On a crash between
the truncate and the next checkpoint, the records that were "deleted"
will reappear after recovery because no LN-delete log entries exist
for them.

The rustdoc references `Environment.truncateDatabase(txn, dbName,
returnCount)` from BDB-JE; in JE this operation **is** logged (it
writes a `DbOperationType.TRUNCATE` log entry under the txn) and is
recovery-safe. The Noxu DB implementation is not.

**Risk.** Data loss (or rather, undeleted-data reappearance) on crash.

**Suggested fix.** Either (a) implement truncate as a logged operation
under a real txn, or (b) call `LogManager::flush_sync` after replacing
the tree and clearly mark this as best-effort, non-replicated.

---

### 6. (Medium) `override_btree_comparator` / `override_duplicate_comparator` flags are dead

**Where:**
`crates/noxu-db/src/database_config.rs:36-40, 127-141`,
`crates/noxu-db/src/byte_comparator.rs` (entire file),
`crates/noxu-db/src/environment.rs:411-420` (open_database plumbing).

**Symptom.** `DatabaseConfig` exposes:

```rust
pub override_btree_comparator: bool,
pub override_duplicate_comparator: bool,
```

…but provides **no setter for an actual comparator function**.
`open_database` does not even read these flags. The
`ByteComparator` trait at `byte_comparator.rs:45` is re-exported from
`noxu_db::lib.rs:78-80` but no public API consumes it.

The only path that installs a custom comparator is
`DatabaseImpl::set_bt_comparator(F)` (`database_impl.rs:367-373`),
which is reachable from `noxu-dbi` integration tests but not from
`Database`.

**Risk.** Users who set `override_btree_comparator(true)` get default
byte order silently — no error, no comparator. This is the kind of
silent-divergence-from-config problem that the BDB world actively
discourages (BDB-JE throws `IllegalStateException` if you set the
flag without supplying a comparator).

**Suggested fix.** Either remove the flags + the `ByteComparator`
trait from the public API, or add `DatabaseConfig::with_btree_comparator(
Arc<dyn Fn(&[u8],&[u8]) -> Ordering + Send + Sync>)` and plumb it
through `Environment::open_database` into
`DatabaseImpl::set_bt_comparator`.

---

### 7. (Medium) Multiple `DatabaseConfig` flags are silently ignored

**Where:** `crates/noxu-db/src/environment.rs:411-420` translates
`noxu_db::DatabaseConfig` to `noxu_dbi::DatabaseConfig` but only
forwards a subset:

```rust
dbi_config.set_allow_create(config.allow_create);
dbi_config.set_sorted_duplicates(config.sorted_duplicates);
dbi_config.set_read_only(config.read_only);
dbi_config.set_temporary(config.temporary);
dbi_config.set_transactional(config.transactional);
dbi_config.deferred_write = config.deferred_write;
if config.node_max_entries > 0 {
    dbi_config.set_node_max_entries(config.node_max_entries as i32);
}
```

**Not forwarded:**

| `noxu_db::DatabaseConfig` field | Status |
|---|---|
| `key_prefixing` | Field exists in `noxu_dbi::DatabaseConfig` (line 14) but is never set from outer config |
| `replicated` | No `noxu_dbi::DatabaseConfig` field at all |
| `bin_delta` | No `noxu_dbi::DatabaseConfig` field at all |
| `cache_mode` | No `noxu_dbi::DatabaseConfig` field at all |
| `exclusive` | No `noxu_dbi::DatabaseConfig` field at all |
| `override_btree_comparator` | Dead (see finding 6) |
| `override_duplicate_comparator` | Dead (see finding 6) |
| `use_existing_config` | No `noxu_dbi::DatabaseConfig` field at all |

**Risk.** `DatabaseConfig::with_key_prefixing(true)` looks like it
enables the BIN key-prefix optimisation but in fact has zero effect
on the underlying `Tree`. Same for `bin_delta`, `replicated`, the
per-database `cache_mode`, etc.

**Suggested fix.** For each field, either (a) plumb it through and
honour it in `noxu-dbi`, or (b) remove the field from
`DatabaseConfig` and document any environment-level alternative.

---

### 8. (Medium) TTL writes are not durable and ignore `update_ttl`

**Where:** `crates/noxu-db/src/database.rs:483-503`
(`put_with_options`).

**Symptom.**

```rust
let result = self.put(txn, key, data)?;          // logs + fsyncs the LN
if opts.ttl > 0 {
    let key_bytes = key.get_data().unwrap_or(&[]);
    let expiration_hours =
        noxu_util::current_time_hours().saturating_add(opts.ttl as u32);
    self.db_impl.read().update_key_expiration(key_bytes, expiration_hours);
}
```

Three issues:

1. The fsync happens **before** the TTL is applied, so on a crash the
   record is durable but the expiration is lost — the record would
   never expire post-crash.
2. `update_key_expiration` writes only the in-memory BIN slot
   (`database_impl.rs:325-333`) — no log record is emitted, so even
   without a crash, the TTL is lost on the next eviction/recovery
   cycle if the slot is logged again.
3. `WriteOptions::update_ttl` (defined at `write_options.rs:21`) is
   never read. The TTL is unconditionally applied whenever
   `opts.ttl > 0`, even for an existing record that the caller wanted
   to keep with its original expiration.

**Risk.** TTL is silently best-effort and the `update_ttl` flag is a
no-op.

**Suggested fix.** Apply the TTL inside the same WAL-logged write
operation (extend `CursorImpl::put` to take an optional
`expiration_hours`), and gate the expiration update on
`!exists || opts.update_ttl`.

---

### 9. (Medium) `Database::preload` does not actually preload LNs

**Where:** `crates/noxu-db/src/database.rs:821-840`.

**Symptom.**

```rust
let guard = self.db_impl.read();
if let Some(tree_stats) = guard.collect_btree_stats() {
    stats.bins_loaded = tree_stats.n_bins;
    if config.load_lns {
        stats.lns_loaded = tree_stats.n_entries;  // <-- count, not actual loads
    }
}
```

`collect_btree_stats` walks the BIN/IN structure (which does pull
those nodes into the cache as a side effect), but it does **not**
follow the LN LSN of any slot — no LN is read off disk. The reported
`lns_loaded` is the count of LN slots in the tree, not the number of
LNs that were actually fetched into cache.

**Risk.** `preload(load_lns=true)` does not warm the LN cache as
documented. Users running it before a benchmark will not get the
cache priming they expect, and the stats they get back are
misleading.

**Suggested fix.** Either iterate every BIN slot calling
`fetch_target_node` to materialise the LN, or update the rustdoc to
say preload only warms BINs/INs and rename `load_lns` to a no-op
flag.

---

### 10. (Medium) `PreloadConfig::max_millis` is silently ignored

**Where:** `crates/noxu-db/src/database.rs:837`:

```rust
let _ = config.max_millis; // reserved for future time-bounded preload
```

The configuration field is exposed, accepted, and discarded. There is
no warning, no error, and no rustdoc on the field admitting the gap.

**Suggested fix.** Either implement the time bound (poll
`start.elapsed()` inside the BIN walk and break when over budget) or
remove the field with a deprecation note.

---

### 11. (Medium) Empty-key handling is inconsistent across `get` / `put` / `delete`

**Where:**

- `database.rs:255-258` (`get`): `key.get_data()` returns `None` →
  `Ok(NotFound)` early.
- `database.rs:540-543` (`delete`): `None` → `Ok(NotFound)` early.
- `database.rs:367` (`put`): `let key_bytes = key.get_data().unwrap_or(&[]);`
  — `None` is silently coerced to an empty key.
- `database.rs:457` (`put_no_overwrite`): same `unwrap_or(&[])`.

**Symptom.** A `DatabaseEntry::new()` (no data set) is treated as
"key not present" by reads and as "empty key" by writes. The two
behaviours do not round-trip:

```rust
let none_key = DatabaseEntry::new();
db.put(None, &none_key, &v)?;           // OK, writes record under empty-key
db.get(None, &none_key, &mut out)?;     // returns NotFound (not Success!)
```

There is no documentation in
`docs/src/getting-started/records.md` distinguishing `None` from
`Some(&[])`, and BDB-JE rejects empty-data DatabaseEntry on writes
(`IllegalArgumentException: zero length data`).

**Risk.** Surprising user-visible asymmetry; possible data
"black-holing" if user code accidentally writes with a `None` key
and then can never read it back through the public API (cursor scans
would still find it).

**Suggested fix.** Reject `None`-data keys in `put` /
`put_no_overwrite` with `IllegalArgument`, and explicitly state in
rustdoc whether empty-but-`Some(&[])` keys are supported.

---

### 12. (Medium) `truncate_database` doesn't enforce "no open handles" and yanks the tree from under cursors

**Where:**
`crates/noxu-dbi/src/environment_impl.rs:865-890`,
contrast with `remove_database` (`environment_impl.rs:800-820`) and
`rename_database` (`environment_impl.rs:830-855`).

**Symptom.** `remove_database` and `rename_database` both check
`reference_count() > 0` and reject the operation if any handle is
open. `truncate_database` does not perform this check; it acquires
the `db_impl.write()` lock and calls `set_recovered_tree(new_tree)`,
which replaces the live `Tree` inside the same `DatabaseImpl`.

The rustdoc claims this preserves "any open handles valid"
(`environment.rs:511-518`), and indeed the `Database` and any
`Cursor` keep their `Arc<RwLock<DatabaseImpl>>` — but a cursor that
was already positioned via `update_bin_pin` on the **old** tree's
BIN now holds an Arc to a node that is no longer reachable from the
new tree root. Subsequent navigation (`get_next`, etc.) will either
hit `NotFound` or, depending on the search path, walk the new tree
and produce inconsistent results.

**Risk.** Silent positioning loss and "ghost" reads on cursors that
were live during a truncate.

**Suggested fix.** Either (a) reject truncate when there are open
cursors / handles (matching JE's stricter contract), or (b) explicitly
document the cursor-invalidation behaviour and add a method on
`Cursor` that resets state when the underlying tree is replaced.

---

### 13. (Medium) Truncated DB loses memory-budget wiring

**Where:** `crates/noxu-dbi/src/environment_impl.rs:881-887` builds
`noxu_tree::Tree::new(db_id, max_entries)` and hands it to
`set_recovered_tree`. `DatabaseImpl::set_memory_counter`
(`database_impl.rs:339-348`) is **not** called on the new tree.

`set_recovered_tree` does forward an existing comparator, but it does
not forward an existing memory counter — `Tree::set_memory_counter`
is a separate API and is wired only at `EnvironmentImpl::open_database`
time.

**Risk.** Inserts into a truncated database stop incrementing the
environment's memory-budget Arbiter. Cache eviction and the cleaner
will under-account for that DB until it is closed and re-opened.

**Suggested fix.** Have `truncate_database` also forward the memory
counter:

```rust
db_guard.set_memory_counter(env_memory_counter.clone());
```

(or capture it at the call site).

---

### 14. (Medium) Most `Database` methods are missing from user-facing docs

**Where:** `docs/src/getting-started/databases.md` and `reading-writing.md`.

The mdbook chapters cover `open_database`, `get`, `put`, `delete`,
`count`, `close`, `is_valid`, `remove_database`. The following public
methods are entirely absent from the user-facing docs:

| Method | Location |
|---|---|
| `Database::truncate_database` (via `Environment`) | not in `databases.md` |
| `Database::sync` | `database.rs:778-792` |
| `Database::preload` | `database.rs:821-840` |
| `Database::verify` | `database.rs:861-883` |
| `Database::get_stats` | `database.rs:842-866` |
| `Database::put_no_overwrite` | `database.rs:436-470` |
| `Database::get_with_options` | `database.rs:300-345` |
| `Database::put_with_options` | `database.rs:481-503` |
| `Database::open_sequence` | `database.rs:678-686` |
| `Database::open_cursor` | `database.rs:592-625` |
| `Database::join` | `database.rs:903-915` |
| `Database::scan_all_kv` | `database.rs:732-758` |

**Suggested fix.** Add a "Database operations reference" section to
`docs/src/getting-started/databases.md`, or split out
`reading-writing.md` into a complete API tour.

---

### 15. (Low) `count()` rustdoc says "approximate" but is exact

**Where:** `database.rs:701-710`:

> "Returns an approximate count of records in the database."

The implementation is an `AtomicU64` updated on every successful LN
insert/delete (`apply_tree_insert` / `apply_tree_delete`). For non-dup
DBs this is exact under serialised execution; the only approximation
is races between `count()` and concurrent writers (which a user
expects). The "approximate" wording is a porting artefact from JE,
where `Database.count()` was an O(n) scan with snapshot semantics.

(Note: this is **separate** from finding 1, which says the counter
is wrong on sorted-dup DBs. For non-dup DBs, the counter is exact
and the docstring is just stale.)

---

### 16. (Low) `Database::sync` flushes the global log, not just one database

**Where:** `database.rs:778-792`:

```rust
pub fn sync(&self) -> Result<()> {
    ...
    if let Some(lm) = &self.log_manager {
        lm.flush_sync()
    }
}
```

The doc text says "Flushes all pending writes for **this database**"
but `LogManager::flush_sync` flushes the entire WAL for the
environment. There is no per-database WAL.

**Suggested fix.** Reword to "Flushes all pending writes in the
environment to stable storage" (matching JE's `Database.sync()`,
which also delegates to the env-level fsync).

---

### 17. (Low) Stale BDB-JE template fragments in rustdoc

**Where:** Throughout `database.rs`, e.g.:

- line 168: `"Port of`LogManager.flushTo(lsn)`"` (note the missing
  space between `Port of` and the backtick).
- line 174: `"// : TXN_NO_SYNC — skip log flush entirely"` — leading
  colon with no class name; this used to read `je: TXN_NO_SYNC`
  before the `je`/`JE` references were stripped.
- line 178: `": TXN_WRITE_NO_SYNC — flush to OS buffer, no fdatasync"`.
- line 182: `": flushTo(lsn) — skip if already covered by another flush."`.
- line 519: `": Environment.truncateDatabase(txn, dbName, returnCount)"`.
- line 893: `"Mirrors `Database.join(SecondaryCursor[], JoinConfig)` from ."`.

Cosmetic but pervasive. The same pattern appears in
`database_config.rs`, `database_stats.rs`, and `database_impl.rs`.

**Suggested fix.** Sweep `crates/noxu-db/src/*.rs` for `: ` at
sentence starts and `from .` at sentence ends and rewrite, or replace
with explicit `// Equivalent to BDB-JE Foo.bar()`.

---

### 18. (Low) `_txn` parameters on `remove_database` / `truncate_database` / `rename_database` / `open_cursor`

**Where:**

- `environment.rs:481-489`, `:511-525`, `:546-560`
- `database.rs:592-625` (`open_cursor`)

The signatures accept `txn: Option<&Transaction>` but the parameter
is named `_txn` and ignored. JE makes these operations
transactional. Either the API should be reduced to
`fn remove_database(&self, name: &str)` or the txn argument should
actually be honoured.

For `Database::open_cursor`, the txn is also dropped — a cursor
opened "under a txn" is no different from one opened with `None`
(the cursor takes its txn from the call site of each cursor op).
This is a subtle but real semantic departure from JE, where the
cursor inherits the txn for its lifetime.

---

### 19. (Low) Dead `ByteComparator` / `compare_keys` API surface

**Where:**

- `byte_comparator.rs:45-95` (trait + default impl + helper).
- `database_impl.rs:367-381` (`set_bt_comparator`, `compare_keys`).
- Re-exported at `lib.rs:78-80`.

`compare_keys` is never called on the data path; the tree uses
`KeyComparatorFn` set at construction (only by the sorted-dup path
in `database_impl.rs:135-145`). Custom user comparators have no
plumbing at all. See finding 6.

**Suggested fix.** Either wire it up or clearly mark these symbols
as "for future use, not yet supported" in the rustdoc, and remove
the misleading example in `byte_comparator.rs:27-44`.

---

### 20. (Low) Partial-put length mismatches are silent

**Where:** `database.rs:386-413`.

```rust
let total_len = (off + len).max(existing.len());
let mut patched = existing;
patched.resize(total_len, 0);
let copy_len = new_bytes.len().min(len);
patched[off..off + copy_len].copy_from_slice(&new_bytes[..copy_len]);
```

- If `new_bytes.len() < len`: only `new_bytes.len()` bytes are
  written; the remaining `[off + new_bytes.len() .. off + len]` keeps
  whatever was there (existing data or zero pad).
- If `new_bytes.len() > len`: excess bytes after `len` are silently
  discarded.

BDB-JE in both cases either rejects the operation
(`IllegalArgumentException`) or has tightly defined semantics
documented on `DatabaseEntry.setPartial`. None of this is
documented in `database_entry.rs:148-161`.

**Suggested fix.** Document the truncation semantics, or reject
mismatched lengths with `NoxuError::IllegalArgument`.

---

### 21. (Low) `node_max_entries: 0` plumbing edge case

**Where:** `environment.rs:418-420`:

```rust
if config.node_max_entries > 0 {
    dbi_config.set_node_max_entries(config.node_max_entries as i32);
}
```

If `node_max_entries == 0`, the inner config keeps its default (which
is also 0, see `noxu_dbi::DatabaseConfig`). Then
`DatabaseImpl::new` passes `0 as usize` to `Tree::new` (line 130).
Whether `Tree::new(_, 0)` is well-defined is not asserted by any
test in this audit's scope. Probably correct (defaults inside
`Tree`), but the contract is undocumented.

---

### 22. (Low) Partial put on a missing key auto-creates a zero-prefixed record

**Where:** `database.rs:391-410` — when the search returns
`NotFound`, the code creates `existing = vec![0u8; off + len]` and
proceeds to write. So a partial put with `offset=10, length=4,
data=b"abcd"` against a non-existent key creates a 14-byte record
`\0\0\0\0\0\0\0\0\0\0abcd`.

This may be intentional (BDB-JE has similar behaviour) but it is
not documented. If the application expected partial-put-to-missing-key
to error (and it has no out-of-band check), it gets a zero-prefixed
record instead.

---

### 23. (Info) Observability is asymmetric between basic and `_with_options` paths

**Where:**

- `database.rs:227-238` (`get`) — `observe_span!`, `observe_timer_*`,
  `observe_counter!` blocks present.
- `database.rs:300-345` (`get_with_options`) — none of these macros.
- `database.rs:481-503` (`put_with_options`) — wraps `put` so the
  counters fire under op="put", not "put_with_options".

Not a correctness issue; just means metrics dashboards under-count
or mis-attribute the `_with_options` paths.

---

## Coverage gaps

Areas not exhaustively covered by this audit but adjacent and worth a
follow-up review:

- **Duplicate-data semantics on `Cursor`.** This audit traced
  `Database::put`/`delete` into `put_dup` but did not exhaustively
  verify `Cursor::get(Get::SearchBoth)` / `Get::SearchKey` /
  `Get::NextDup` against JE for sorted-dup DBs. Given finding 1 and 2
  there are probably more.
- **`Database::verify`.** Walked enough to confirm it does not take a
  txn and is documented as structural. Did not verify that all
  invariants checked by JE's `BtreeVerifier` are implemented (e.g.
  duplicate-tree separator key consistency).
- **Recovery of `truncate_database`.** Beyond the durability gap in
  finding 5, this audit did not exercise the recovery path itself.
- **`Database::open_sequence` and the `Sequence` API.** Only verified
  that `open_sequence` exists; did not audit Sequence semantics.
- **`Database::join`.** Only verified the entry point exists.
- **`SecondaryDatabase` / `SecondaryCursor`.** Out of scope per the
  audit prompt.
- **Sorted-dup recovery.** `set_recovered_tree` uses
  `tree.count_entries()` — for a freshly-built recovered tree this
  may also miscount under sorted-dup if recovery uses
  `apply_tree_insert` semantics that the dup path does not. Not
  audited.
- **`DatabaseConfig::transactional`.** Not enforced when a
  non-transactional DB is used inside a transaction; out of scope
  here but worth checking.
- **Oversized key/value behaviour.** No `MAX_KEY_SIZE` /
  `MAX_DATA_SIZE` constant exists in `noxu-db` or `noxu-dbi`. Tests
  cover up to 10 KiB values; the absolute upper bound (limited by
  log-record size) is undocumented.
- **Auto-commit `delete` durability.** Briefly verified
  (`auto_commit_sync` is called); did not measure under load.

---

## Summary by severity

| Severity | Count |
|---|---|
| High | 5 |
| Medium | 9 |
| Low | 8 |
| Info | 1 |
| **Total** | **23** |

The five High-severity findings cluster in two themes:

1. **Sorted-duplicate database support is incomplete.** `count()`
   returns the wrong number (#1), `delete(key)` removes only one of
   N duplicates (#2). Both look like the original BDB-JE
   `Database` semantics were ported assuming non-dup behaviour and
   the `put_dup` path was retrofitted in `noxu-dbi` without back-
   propagating the counter increment / multi-record delete to the
   public API.

2. **The "options" surface (LockMode, WriteOptions TTL,
   transactional truncate, partial put under txn) is partially
   wired.** Several option fields look like they do something but
   silently do not (#3, #4, #5, #8). This is the same general class
   of latent bug that produced the SearchGte cursor issues — a
   path that *appears* to honour a configuration but in fact does
   not.

The Medium findings are dominated by `DatabaseConfig` flags that are
declared but unused (#6, #7) and documentation gaps (#9, #10, #11,
#12, #13, #14). The Low findings are mostly cosmetic / dead-code.

Recommended immediate follow-up in priority order:

1. Fix #1 (sorted-dup counter increment) — one-liner-equivalent fix.
2. Fix #3 (partial-put self-deadlock under txn) — single-line fix
   (`make_cursor` → `make_cursor_for_txn`).
3. Either implement or remove the `LockMode::Rmw` /
   `LockMode::ReadCommitted` plumbing in `get_with_options` (#4).
4. Fix #2 (multi-dup delete) — needs a small loop in
   `Database::delete`.
5. Decide on truncate semantics (#5, #12, #13) — these probably
   need a coordinated design change, not a quick fix.
6. Sweep dead config flags (#6, #7) and the `_txn` arguments (#18).

Honest limitations of this audit:

- No tests were run; all findings are static.
- Only the call paths exercised by `Database`'s public methods were
  walked. `cursor_impl.rs` (~3000 lines) was sampled, not read in full.
- The interaction with replication (`noxu-rep`) was not examined,
  so any "replicated" semantics on `Database` operations are out of
  scope.
- Reference archives in `_/je/src/` and `_/nosql/kvmain/src/` were
  not consulted directly during this audit (they are noted as
  guidance-only by `AGENTS.md`); the BDB-JE comparisons here come
  from rustdoc cross-references already in the source and from
  publicly known BDB-JE semantics.
