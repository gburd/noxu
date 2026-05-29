# Noxu DB — Rust Idiom, Soundness, and API Ergonomics Audit

**Reviewer persona**: Jon Gjengset (jonhoo)  
**Date**: 2026-05-29  
**Codebase**: `/home/gburd/ws/noxu` — branch `fix/wave11-l-api-stability`  
**Version**: 2.4.1

---

> "This is a serious piece of work — a full ACID embedded database in Rust
> with replication, XA, schema evolution, and 5,600 tests. But the *user
> experience* is still trapped in 2006 Java. The engine is 8/10; the API
> surface is 4/10."

---

## Table of Contents

1. [Public API Ergonomics](#1-public-api-ergonomics)
2. [Iterator and Stream Conventions](#2-iterator-and-stream-conventions)
3. [Type-state, Generics, and the Gospel of Monomorphization](#3-type-state-generics-and-the-gospel-of-monomorphization)
4. [The `unsafe` Audit](#4-the-unsafe-audit)
5. [Soundness Through the Type System](#5-soundness-through-the-type-system)
6. [Build / Cargo Experience](#6-build--cargo-experience)
7. [Documentation Quality](#7-documentation-quality)
8. [Would I Actually Use This?](#8-would-i-actually-use-this)
9. [Clippy / fmt / Nursery / Pedantic Pass](#9-clippy--fmt--nursery--pedantic-pass)
10. [Concurrency Primitives](#10-concurrency-primitives)
11. [Summary Table](#summary-table)
12. [Top 5 Actionable Items](#top-5-actionable-items)
13. [Elevator Pitch](#elevator-pitch)

---

## 1. Public API Ergonomics

### Finding 1.1 — `OperationStatus` is a Java transplant that Rust doesn't need

**Severity**: high  
**Topic**: API ergonomics  
**File:line**: `crates/noxu-db/src/database.rs`, `crates/noxu-db/src/operation_status.rs`

Every read/write returns `Result<OperationStatus>` where `OperationStatus ∈
{Success, NotFound, KeyExists}`. This forces callers to write two-level error
handling and creates verbosity that no idiomatic Rust library uses:

```rust
// Before (current Noxu API)
let mut data = DatabaseEntry::new();
match db.get(txn, &key, &mut data)? {
    OperationStatus::Success => {
        let val = data.get_data().unwrap();
        // use val
    }
    OperationStatus::NotFound => { /* ... */ }
    OperationStatus::KeyExists => unreachable!(),
}
```

```rust
// After (idiomatic Rust)
match db.get(txn, key)? {
    Some(val) => { /* val: Bytes */ }
    None      => { /* not found */ }
}
// put_no_overwrite becomes:
if db.insert(txn, key, value)? == InsertResult::Existed { … }
```

The `OperationStatus::KeyExists` variant belongs only on `put_no_overwrite`;
mixing it into the `get` return type is a design smell.

**Suggested fix**: Return `Result<Option<Bytes>>` from `get`. Return
`Result<bool>` (or a small `InsertStatus` enum) from upsert variants.
Keep `OperationStatus` if needed for `Cursor::get` which has more
state transitions, but don't export it as the primary result type.

---

### Finding 1.2 — `DatabaseEntry` as an out-parameter is not Rust

**Severity**: high  
**Topic**: API ergonomics  
**File:line**: `crates/noxu-db/src/database.rs:478–488`

```rust
// Before (current API — callers must pre-allocate DatabaseEntry output slots)
let mut data = DatabaseEntry::new();
let status = db.get(txn, &key, &mut data)?;
if status == OperationStatus::Success {
    let bytes = data.get_data().unwrap();
}
```

This is a direct port of the Java `DatabaseEntry` in/out convention. Rust has
zero-cost returns; there's no reason to use out-parameters. The internal
implementation already uses `bytes::Bytes` (reference-counted, O(1) clone)
for zero-copy, which is good — but nothing surfaces that to the caller.

```rust
// After
let bytes: Option<bytes::Bytes> = db.get(txn, key)?;
// or, with the key also being typed:
let bytes: Option<bytes::Bytes> = db.get(txn, b"hello")?;
```

Note: `DatabaseEntry` also holds `offset`, `size`, `partial_offset`,
`partial_length` — partial-read semantics. Those can move to
`ReadOptions` (which already exists) and be removed from the entry type.

---

### Finding 1.3 — Double builder API doubles the surface for no gain

**Severity**: medium  
**Topic**: API ergonomics  
**File:line**: `crates/noxu-db/src/environment_config.rs:918–1080` (representative)

Every config has *both* `set_x(&mut self) -> &mut Self` AND `with_x(self) ->
Self`. This doubles the API surface (150+ parameters × 2 = 300+ methods on
`EnvironmentConfig`), confuses users about which to prefer, and produces
incoherent usage like:

```rust
config.set_allow_create(true).set_transactional(true)
// vs
EnvironmentConfig::new(path).with_allow_create(true).with_transactional(true)
```

**Suggested fix**: Provide only the consuming builder `with_x(self) -> Self`.
Mutation-in-place via `set_x` is needed only for `EnvironmentMutableConfig`
(parameters that may change after the env is open). The current design is
inconsistent: some parameters only have `set_*`, some only have `with_*`,
some have both.

---

### Finding 1.4 — README API examples reference non-existent methods

**Severity**: high  
**Topic**: API ergonomics / documentation  
**File:line**: `README.md:63,80`

The README Quick Start block contains **two** API bugs that would cause any
reader who tried to copy-paste it to get a compile error immediately:

```rust
// README line 63 — WRONG: Database::get takes 3 args, not 4
let status = db.get(None, &key, &mut result, None)?;
//                                           ^^^^  extraneous 4th arg

// README line 80 — WRONG: Cursor has no get_next() method
while cursor.get_next(&mut k, &mut v, None)? == OperationStatus::Success {
//          ^^^^^^^^^ does not exist; should be cursor.get(&mut k, &mut v, Get::Next, None)?
```

The actual `Database::get` signature is:

```rust
pub fn get(&self, txn: Option<&Transaction>, key: &DatabaseEntry,
           data: &mut DatabaseEntry) -> Result<OperationStatus>
```

The README was written (or edited) against a hypothetical future API
(`get_with_options` which takes 4 args) or was never compile-checked.
For a database library, this is a critical trust-breaker.

---

### Finding 1.5 — All doc-tests use `ignore`; nothing is compile-tested

**Severity**: high  
**Topic**: Documentation / API drift  
**File:line**: `crates/noxu-db/src/lib.rs:24`, `database.rs:41`, `cursor.rs:32`, etc.

Every code example in the public API documentation uses `` ```ignore ``
instead of `` ```rust `` or `` ```no_run ``. This means `cargo test
--workspace` never validates a single documentation example. The README
bug (Finding 1.4) is a direct consequence of this.

```
$ grep -rn '```ignore\|```rust' crates/noxu-db/src/ | grep '```rust'
(no output)
```

All 30 doc-test blocks in `noxu-db` are marked `ignore`. Same pattern holds
in `noxu-bind`, `noxu-collections`, `noxu-persist`.

**Suggested fix**: Convert to `` ```no_run `` (compiles but doesn't execute,
safe for tests that need disk I/O) or provide a `#[doc = include_str!(...)]`
macro referencing the working `examples/` files. At minimum, fix the README.

---

### Finding 1.6 — `LockMode` passed as `Option<LockMode>` everywhere, always `None`

**Severity**: low  
**Topic**: API ergonomics  
**File:line**: `crates/noxu-db/src/cursor.rs:94`

The `Cursor::get` signature takes `_lock_mode: Option<LockMode>` as a 4th
argument that is documented as "reserved" and "currently ignored". In
practice 100% of callers pass `None`. This is dead interface weight that
adds to call-site verbosity.

```rust
// Before — every call site carries dead weight
cursor.get(&mut key, &mut data, Get::Next, None)?
// After (until LockMode is actually implemented)
cursor.get(&mut key, &mut data, Get::Next)?
```

Removing the parameter now costs a breaking change later; keeping it costs
everyone a perpetual `None` argument today. Recommendation: add a comment
on the argument indicating it's intentionally reserved and will not be
removed until the per-operation lock mode feature ships.

---

## 2. Iterator and Stream Conventions

### Finding 2.1 — `Cursor` does not implement `Iterator`

**Severity**: high  
**Topic**: Iterator conventions  
**File:line**: `crates/noxu-db/src/cursor.rs`

The `Cursor` type is a stateful JE-style object. To iterate all records a
user must write:

```rust
// Current API — verbose, manual, error-prone
let mut cursor = db.open_cursor(None, None)?;
let mut key = DatabaseEntry::new();
let mut data = DatabaseEntry::new();
while cursor.get(&mut key, &mut data, Get::Next, None)? == OperationStatus::Success {
    let k = key.get_data().unwrap();
    let v = data.get_data().unwrap();
    // do something
}
cursor.close()?;
```

This does not compose with `for`, `.map()`, `.filter()`, `.collect()`, or
any other iterator combinator. Contrast with `redb` or `heed` where:

```rust
// What users expect
for entry in db.range(txn, ..)? {
    let (k, v) = entry?;
}
```

**Recommended shape**: Add an adapter that implements `Iterator`:

```rust
impl Database {
    /// Forward-scanning iterator. Lazy; holds a live cursor.
    pub fn iter<'txn>(&self, txn: Option<&'txn Transaction>)
        -> Result<impl Iterator<Item = Result<(Bytes, Bytes)>> + 'txn>;

    /// Range scan. Keys must serialize to comparable byte slices.
    pub fn range<'txn, K: AsRef<[u8]>>(
        &self, txn: Option<&'txn Transaction>, range: impl RangeBounds<K>,
    ) -> Result<impl Iterator<Item = Result<(Bytes, Bytes)>> + 'txn>;
}
```

The `StoredMap` / `StoredIterator` layer materializes the entire scan into
a `Vec` upfront, which is strictly worse for large datasets.

---

### Finding 2.2 — `StoredIterator` is eager-only; large scans OOM

**Severity**: medium  
**Topic**: Iterator conventions  
**File:line**: `crates/noxu-collections/src/stored_iterator.rs`,
`crates/noxu-collections/src/stored_map.rs:214`

`StoredMap::iter()` calls `scan_records()` which walks the entire database
into a `Vec<T>` before returning. For a database with millions of records
this is a guaranteed out-of-memory. The design comment says it "avoids
holding a live cursor across the iteration's lifetime", but that is exactly
what a lazy iterator should do.

```rust
// Current — O(n) memory before first element is yielded
let items: Vec<(i32, String)> = map.iter(None)?.map(Result::unwrap).collect();

// What would be expected:
for (k, v) in map.iter(None)? {
    let (k, v) = (k?, v);  // lazy; never buffers more than one record
}
```

The lifetimes *are* manageable. A lazy cursor iterator needs a `'db` and a
`'txn` lifetime, both of which the `StoredMap` already holds:

```rust
pub struct StoredMapIter<'db, 'txn, K, V, KB, VB> {
    cursor: Cursor<'db>,
    txn:    PhantomData<&'txn ()>,
    // ...
}
```

This is the natural design. Eager materialization as a `StoredIterator` can
remain as a `collect_all()` method for callers who want it.

---

### Finding 2.3 — No `Database::range()` / key-range scan shorthand

**Severity**: medium  
**Topic**: Iterator conventions  
**File:line**: `crates/noxu-db/src/database.rs`

There is no `range()` method on `Database`. To do a bounded key scan the
user must open a cursor, `Get::SearchGte` to the lower bound, then loop
`Get::Next` until the key exceeds the upper bound. Every other Rust embedded
KV library (`redb`, `sled`, `heed`) provides this as a first-class API.

```rust
// What should exist:
for entry in db.range(txn, b"user:1000"..=b"user:1999")? {
    let (k, v) = entry?;
}
```

---

## 3. Type-state, Generics, and the Gospel of Monomorphization

### Finding 3.1 — `Arc<dyn Channel>` is hot-path dynamic dispatch in `noxu-rep`

**Severity**: medium  
**Topic**: Generics / monomorphization  
**File:line**: `crates/noxu-rep/src/stream/feeder.rs:307`,
`crates/noxu-rep/src/stream/replica_stream.rs:162`

The replication stream feeder holds `channel: Arc<dyn Channel>`. Every
byte sent over the wire involves a vtable dispatch: `channel.send(msg)`.
The `Channel` trait is implemented by `TcpChannel`, `QuicChannel`,
`InMemoryChannel` — a closed set known at `RepEnvironment` construction
time. This should be a generic:

```rust
// Before
pub struct Feeder {
    channel: Arc<dyn Channel>,
    // ...
}

// After
pub struct Feeder<C: Channel> {
    channel: Arc<C>,
    // ...
}
pub type TcpFeeder = Feeder<TcpChannel>;
```

The monomorphized version eliminates vtable dispatch on every frame write,
enables inlining of `TcpChannel::send`, and allows the compiler to
optimize the hot loop. The QUIC and in-memory variants are used for tests
and separate code paths anyway, so the increase in binary size is trivial.

---

### Finding 3.2 — `KeyComparatorFn = Arc<dyn Fn(…)>` on every key comparison

**Severity**: medium  
**Topic**: Generics / monomorphization  
**File:line**: `crates/noxu-tree/src/tree.rs:50–51`

```rust
pub type KeyComparatorFn =
    Arc<dyn Fn(&[u8], &[u8]) -> std::cmp::Ordering + Send + Sync>;
```

This is the comparator used for every B-tree key comparison. At 100 K
operations per second each comparison invokes `Arc::deref()` + vtable
dispatch. The type alias also hides that this is dynamic dispatch from
readers of struct definitions.

The comparator is set once at database open time. A generic parameter on
`Tree<C: Comparator>` with a monomorphized default would eliminate the
dispatch entirely. Alternatively, at minimum, the `Arc` can be removed
and replaced with a `Box<dyn Fn(…)>` (no ref-count overhead since the
tree uniquely owns it) or a plain `fn` pointer for the common case.

---

### Finding 3.3 — `Box<dyn EvictionPolicy>` in `Evictor` on every eviction decision

**Severity**: low  
**Topic**: Generics / monomorphization  
**File:line**: `crates/noxu-evictor/src/evictor.rs:150,154`

```rust
primary_policy: Box<dyn EvictionPolicy>,
scan_policy:    Box<dyn EvictionPolicy>,
```

Eviction is a background operation, so vtable dispatch here is not
latency-critical. However, `Box<dyn NodeEvictionInfo>` is allocated on
every eviction candidate (see `evictor.rs:563,780,942`). At high cache
pressure this is many short-lived heap allocations. A simple struct with
optional fields would avoid the allocation.

**Concrete typestate proposals**:

**Typestate 1 — `Transaction<S: TxnState>`**:

```rust
pub struct Transaction<S: sealed::TxnState> { inner: Arc<TxnInner>, _s: PhantomData<S> }
pub struct Open;
pub struct Committed;
pub struct Aborted;
impl Transaction<Open> {
    pub fn commit(self) -> Result<Transaction<Committed>> { … }
    pub fn abort(self)  -> Result<Transaction<Aborted>>  { … }
}
// Calling commit() on Transaction<Committed> is a compile error.
```

**Typestate 2 — `Database<S: DbState>`**:

```rust
pub struct Database<S: sealed::DbState = Open> { … }
impl Database<Open> {
    pub fn close(self) -> Result<Database<Closed>> { … }
    pub fn get(&self, …) -> …;  // only available while Open
}
// Calling get() after close() is a compile error, not a runtime check.
```

**Typestate 3 — `Cursor<'txn, 'db, S>`** where `'txn: 'db` enforces that
the cursor is dropped before the transaction commits:

```rust
pub fn open_cursor<'txn>(&'db self, txn: &'txn Transaction) -> Cursor<'txn, 'db>
// The lifetime borrow prevents the transaction from being committed
// while the cursor is live — compile error instead of runtime
// "cursor outlived transaction" checks.
```

Note: all three typestates would require `Arc<Database>` / `Arc<Environment>`
patterns (or RAII handles), which is a significant refactor but would
eliminate large swaths of runtime state checking.

---

## 4. The `unsafe` Audit

### Finding 4.1 — Three unnecessary `unsafe impl Send + Sync` in `noxu-rep`

**Severity**: high  
**Topic**: Soundness  
**File:line**:

- `crates/noxu-rep/src/elections/election.rs:302–303`
- `crates/noxu-rep/src/elections/master_tracker.rs:163–164`
- `crates/noxu-rep/src/elections/phi_detector.rs:212–213`

```rust
// Safety: all interior mutability is behind noxu_sync Mutexes.
unsafe impl Send for Election {}
unsafe impl Sync for Election {}
```

**The problem**: `Election` contains only `noxu_sync::Mutex<T>` fields wrapping
`Send` types (`ElectionState`, `u64`, `HashMap<String, bool>`, `Proposal`,
`ElectionOutcome`). `noxu_sync::Mutex<T>` is `Send + Sync` when `T: Send`.
Therefore `Election` would *automatically* derive `Send + Sync` without any
`unsafe impl` — the compiler would verify this. Same analysis applies to
`MasterTracker` (all `noxu_sync::RwLock<T>` fields) and `PhiAccrualDetector`
(also `noxu_sync::RwLock<T>` fields).

Using `unsafe impl Send` when the type already satisfies the auto-trait
bounds is **not just unnecessary — it is misleading and hides the actual
soundness story.** If someone later adds a `*mut T` field, the manual `unsafe
impl` masks the regression; the compiler's auto-trait derivation would have
caught it.

**Action**: Remove all three `unsafe impl Send + Sync` blocks from
`Election`, `MasterTracker`, and `PhiAccrualDetector`. `cargo test`
should pass without them; if it does not, there is a hidden non-Send field
that *needs* to be investigated rather than suppressed.

---

### Finding 4.2 — `std::mem::transmute` in `noxu-log` not in the AGENTS.md unsafe inventory

**Severity**: high  
**Topic**: Soundness / incomplete unsafe inventory  
**File:line**: `crates/noxu-log/src/log_source.rs:59–63`

```rust
// SAFETY: We extend the lifetime of the guard to 'static because we keep
// the Arc<FileHandle> alive for as long as the guard exists.
let guard = unsafe {
    let guard = handle.acquire()?;
    std::mem::transmute::<FileHandleGuard<'_>, FileHandleGuard<'static>>(guard)
};
```

AGENTS.md claims `noxu-log` contains only mmap `unsafe`. This is a
second, undocumented `unsafe` block using `std::mem::transmute` to
conjure a `'static` lifetime. This is one of the most dangerous Rust
patterns: lifetime transmutation.

The argument "we keep the `Arc<FileHandle>` alive via the `_handle` field"
is logically correct *today*, but it is entirely unenforceable: nothing in
the type system stops someone from calling `drop(_handle)` (fields are
public within the module), reordering the struct fields (drop order), or
moving `guard` out of the struct (invalidating the `_handle` protection).

**Safer alternative**: Restructure `FileLogSource` so the `FileHandleGuard`
is held behind a reference tied to the actual `Arc<FileHandle>` lifetime,
or use a self-referential struct via `ouroboros`/`rental`/`yoke`. If
transmute is truly necessary, add a `SAFETY` comment that explains each of
the above failure modes and why they cannot occur.

---

### Finding 4.3 — `LogBufferSegment` raw-pointer SAFETY argument is insufficient

**Severity**: high  
**Topic**: Soundness  
**File:line**: `crates/noxu-log/src/log_buffer.rs:329–376`

```rust
pub struct LogBufferSegment {
    data_ptr:   *mut u8,
    pin_count:  *const AtomicI32,
    latch:      *const RawMutex,
    latch_held: *const AtomicBool,
    size:       usize,
}
// SAFETY: The LogBuffer's latch and pin count protocol ensures safe concurrent access.
// The raw pointers point into a LogBuffer that is kept alive by the caller (typically
// wrapped in Arc<Mutex<LogBuffer>> in the pool).
unsafe impl Send for LogBufferSegment {}
```

The SAFETY comment says "typically wrapped in Arc<Mutex<LogBuffer>> in the
pool". The word "typically" is doing a lot of work. There is no lifetime or
ownership relationship *in the type* that enforces this invariant. If a
`LogBufferSegment` outlives its `LogBuffer`, the raw pointers become
dangling — undefined behaviour.

**Concrete gap**: `LogBufferSegment` is a `pub` type. Nothing prevents a
caller from dropping the `LogBuffer` (or its owning pool) and then calling
`.put()` on an outstanding segment. The latch pointer will dereference freed
memory.

**Suggested fix**: Add a `PhantomData<&'pool LogBuffer>` lifetime to
`LogBufferSegment<'pool>` so the borrow checker enforces the invariant.
If the pool lifetimes are too complex, wrap `LogBufferSegment` in a
newtype that is not `pub` and document the invariant at the module boundary
with a `#[doc(hidden)]` marker.

---

### Finding 4.4 — `Ordering::Relaxed` on `pin_count.fetch_sub` is incorrect

**Severity**: critical  
**Topic**: Soundness / memory ordering  
**File:line**: `crates/noxu-log/src/log_buffer.rs:374–376`

```rust
// In LogBufferSegment::put():
(*self.latch_held).store(false, Ordering::Relaxed);
(*self.latch).unlock();
(*self.pin_count).fetch_sub(1, Ordering::Relaxed);  // <-- WRONG
```

And at the reader side (`LogBuffer::wait_for_writers`):

```rust
if self.write_pin_count.load(Ordering::Relaxed) == 0 {
    // proceed to read the buffer contents
}
```

The `fetch_sub(1, Relaxed)` decrement does **not** provide a Release fence,
meaning the buffer writes that happened *before* the decrement may not be
visible to another thread that observes `pin_count == 0` via a Relaxed load.
The reader thread has no guarantee that it sees the writes into the buffer.

This is a classic "Relaxed publish" bug. The correct ordering is:

- Writer: `fetch_sub(1, Release)` (publishes the buffer writes)
- Reader: `load(Acquire)` after seeing zero (acquires the buffer writes)

This is *not* guarded by the latch because the latch is released
*before* the `fetch_sub`, so the latch release does not help here.

**Severity upgraded to critical** because this is a memory ordering bug
in the write-ahead log's write-staging buffer — incorrect visibility
here means that flushed data may be read in an uninitialized state,
violating the WAL correctness guarantee.

---

### Finding 4.5 — Missing `#[forbid(unsafe_code)]` on zero-unsafe crates

**Severity**: medium  
**Topic**: Soundness / guard rails  
**File:line**: `crates/noxu-tree/src/lib.rs`, `crates/noxu-txn/src/lib.rs`,
`crates/noxu-bind/src/lib.rs`, etc. (12 crates claiming zero unsafe)

AGENTS.md lists 12 crates as targeting zero `unsafe`. None of them have
`#![forbid(unsafe_code)]`. This means the claim is only maintained by
code review — a future contributor can add `unsafe` code to
`noxu-tree` without any build-time error. Only `noxu-rep/quoracle`
has this attribute.

```rust
// Add to lib.rs of every zero-unsafe crate:
#![forbid(unsafe_code)]
```

---

### Summary of `unsafe` blocks

| Location | Purpose | SAFETY comment? | Comment quality | Necessary? |
|---|---|---|---|---|
| `noxu-latch/exclusive.rs:118` | force_unlock RAII | ✓ | adequate | yes |
| `noxu-log/file_manager.rs:615` | mmap | none | — | yes |
| `noxu-log/log_buffer.rs:242–376` | raw ptr write, unsafe impl Send | ✓ | thin | partially |
| `noxu-log/log_source.rs:59` | lifetime transmute | ✓ | insufficient | no (restructure) |
| `noxu-rep/channel.rs:469` | `libc::setsockopt` | none | — | yes |
| `noxu-rep/elections/election.rs:302–303` | `unsafe impl Send+Sync` | ✓ (weak) | thin | **NO** |
| `noxu-rep/elections/master_tracker.rs:163–164` | `unsafe impl Send+Sync` | ✓ (weak) | thin | **NO** |
| `noxu-rep/elections/phi_detector.rs:212–213` | `unsafe impl Send+Sync` | ✓ (weak) | thin | **NO** |
| `noxu-sync/condvar.rs:65,73,88,112` | raw lock/unlock | ✓ | adequate | yes |
| `noxu-sync/futex.rs:43,75` | libc futex | none | — | yes |
| `noxu-sync/raw_mutex.rs:51,101,115` | RawMutex impl | none (trait reqs) | — | yes |
| `noxu-sync/raw_rwlock.rs:42,69,122,146` | RawRwLock impl | none (trait reqs) | — | yes |
| `noxu-sync/lib.rs:148,154,160,166` | raw state inspection | ✓ | adequate | yes |
| `noxu-xa/environment.rs:177` | ptr deref for txn ref | ✓ | good | yes (restructure later) |

**Total unsafe blocks**: ~25  
**Unnecessary**: 3 (the three `unsafe impl Send+Sync` in elections)  
**Under-documented**: 5+ (futex, setsockopt, mmap, raw_mutex trait bodies)  
**Incorrect**: 1 (Relaxed ordering on pin_count — Finding 4.4)  
**Incomplete AGENTS.md inventory**: 1 (log_source transmute)

---

## 5. Soundness Through the Type System

### Finding 5.1 — `TransactionState` checked at runtime where compile-time is possible

**Severity**: medium  
**Topic**: Soundness via types  
**File:line**: `crates/noxu-db/src/transaction.rs`

`Transaction` has a `state: Mutex<TransactionState>` that is checked on
every operation. `commit()` on an already-committed transaction returns a
runtime `Err(TransactionAborted(...))`. This is correct but permits misuse
at the call site. See Finding 3.3, Typestate 1 for the proposed fix.

Current code also mixes `std::sync::Mutex` and `noxu_sync::Mutex` within
the same struct:

```rust
use noxu_sync::Mutex as SyncMutex;  // for some fields
use std::sync::Mutex;               // for state: Mutex<TransactionState>
                                    // and name: Mutex<Option<String>>
```

This is inconsistent and likely accidental — the `noxu_sync::Mutex` was
introduced to get `get_n_waiters()` and similar diagnostics; the `std::sync`
uses are probably oversights.

---

### Finding 5.2 — `PhantomData<fn() -> (K, V)>` is correct; document why

**Severity**: informational  
**Topic**: Soundness  
**File:line**: `crates/noxu-collections/src/stored_map.rs:65`

```rust
pub(crate) _marker: PhantomData<fn() -> (K, V)>,
```

This is the right choice: `fn() -> (K, V)` is covariant in `K` and `V`
(unlike `*const (K, V)` which would add `!Send + !Sync`). The author
clearly knew what they were doing. A brief comment explaining the
invariance/variance choice would be educational for future contributors
who might "simplify" it to `PhantomData<(K, V)>`.

---

### Finding 5.3 — `Send + Sync` bounds on user-facing trait objects are over-broad

**Severity**: low  
**Topic**: Soundness / ergonomics  
**File:line**: `crates/noxu-db/src/secondary_config.rs:17,45`,
`crates/noxu-db/src/error.rs:294`

```rust
pub trait SecondaryKeyCreator: Send + Sync { … }
pub trait ExceptionListener: Send + Sync { … }
```

`Send + Sync` on these traits prevents users from implementing them with
`Rc<…>` or other non-thread-safe types. For `ExceptionListener`, the
`Send + Sync` is justified since the callback runs in background daemon
threads. For `SecondaryKeyCreator`, it is also justified since it may be
called from any writer thread.

However, the bounds are not documented. A new user who wants to implement
`SecondaryKeyCreator` with a closure that captures local data might be
surprised by the `Send + Sync` requirement. Adding a sentence explaining
*why* these bounds are necessary would prevent confusion.

---

## 6. Build / Cargo Experience

### Finding 6.1 — Workspace lints are vestigial; no real enforcement

**Severity**: high  
**Topic**: Build/Cargo  
**File:line**: `Cargo.toml:175–180`

```toml
[workspace.lints.clippy]
or_fun_call = "warn"
redundant_clone = "warn"
large_stack_frames = "warn"
large_types_passed_by_value = "warn"
```

Four clippy lints, all at `warn`. Missing:

- `rust.warnings = "deny"` — the most important: treat all `rustc` warnings
  as errors so warnings don't accumulate silently.
- `rust.unsafe_op_in_unsafe_fn = "deny"` — requires explicit `unsafe` blocks
  inside `unsafe fn`, preventing "inheriting" unsafety from the function
  signature.
- `clippy::missing_safety_doc = "warn"` — flags `unsafe fn` without a
  `# Safety` doc section.
- `clippy::undocumented_unsafe_blocks = "warn"` — flags `unsafe { }` blocks
  without a `// SAFETY:` comment (Finding 4.5's futex/setsockopt blocks would
  be caught by this).
- `rust.missing_docs = "warn"` — Finding 7.

The CI commands in AGENTS.md run `cargo clippy --workspace --all-targets
--all-features -- -D warnings` externally, which is correct. But not
encoding this in `[workspace.lints]` means contributors without the CI
command memorized won't see these failures locally during development.

Additionally, **every** crate's `lib.rs` begins with:

```rust
#![allow(dead_code, clippy::type_complexity, clippy::too_many_arguments)]
```

This silences three valuable lints globally across the entire workspace:

- `dead_code` — hides unused API surface that could be removed
- `clippy::type_complexity` — hides signatures that should be type-aliased
- `clippy::too_many_arguments` — hides functions that should take a config struct

---

### Finding 6.2 — No `rust-version` (MSRV) declared in workspace

**Severity**: medium  
**Topic**: Build/Cargo  
**File:line**: `Cargo.toml` (workspace package section)

The workspace uses Rust `edition = "2024"` (Rust 1.85+) but there is no
`rust-version` field in `[workspace.package]`. The `rust-toolchain.toml`
pins to `channel = "1.95"`, but `Cargo.toml` doesn't declare an MSRV.
This means `cargo check` on an older toolchain will give cryptic errors
instead of "this crate requires Rust 1.85+".

```toml
[workspace.package]
rust-version = "1.85"  # edition 2024 minimum
```

---

### Finding 6.3 — All crates marked `publish = false` with path deps

**Severity**: informational  
**Topic**: Build/Cargo  

All 19 crates use `publish = false`. The comments say this is because of
path deps during development. This is correct practice and noted in the
crate comments. No finding here beyond noting that the `wave-10-e-cratesio-prep.md`
document should be the reference for the publish plan when the time comes.

---

## 7. Documentation Quality

### Finding 7.1 — `lib.rs` crate-level doc is a one-liner

**Severity**: medium  
**Topic**: Documentation  
**File:line**: `crates/noxu-db/src/lib.rs:13–35`

```rust
//! Noxu DB - An embedded transactional database engine.
//!
//! Public API : Environment, Database,
//! Cursor, Transaction, DatabaseEntry, SecondaryDatabase, Sequence, etc.
//!
//! This crate provides the public API for Noxu DB.
//! It is designed to be familiar to embedded database users while being
//! idiomatic Rust.
```

The crate-level doc does not explain:

- What a "Noxu environment" is (contrast: a directory on disk with WAL files)
- The ownership model (Environment owns Databases, both are `Arc`-like handles)
- Transaction semantics (explicit vs auto-commit)
- Concurrency model (multiple readers + one writer? MVCC? Lock-based?)
- Minimum example that actually compiles (```` ```no_run``` ``)

Compare with `redb`'s crate doc, which gives a working example in 10 lines
and explains the ownership model up front.

---

### Finding 7.2 — Many `///` doc-comments end mid-sentence from Java porting

**Severity**: low  
**Topic**: Documentation  
**File:line**: `crates/noxu-db/src/database.rs:36` (representative)

```rust
/// Database handles provide methods for inserting, retrieving, and
/// deleting records. A database belongs to a single environment.
///
/// # Example
/// ```ignore
/// use noxu_db::{Environment, EnvironmentConfig, DatabaseConfig, DatabaseEntry};
/// use std::path::PathBuf;
///
/// let env_config = EnvironmentConfig::new(PathBuf::from("/tmp/mydb"))
///     .allow_create(true);  // <-- WRONG: should be .with_allow_create(true)
```

The doc-example uses `.allow_create(true)` which doesn't exist (it's
`.with_allow_create(true)`). Another example of API drift enabled by
````ignore`.

---

### Finding 7.3 — No per-crate README in most crates

**Severity**: low  
**Topic**: Documentation  

Most crates under `crates/` have no `README.md`. When someone lands on
`docs.rs` for `noxu-bind` or `noxu-collections` they get the lib.rs
doc only. The workspace `README.md` is good but doesn't describe the
individual crate APIs.

---

## 8. Would I Actually Use This?

### Context

I'm starting a new Rust project and need an embedded transactional KV store.
My requirements: ACID transactions, no external process, pure Rust preferred,
decent ergonomics, production-stable.

### Comparison

| | Noxu | redb | sled | heed/lmdb | rust-rocksdb |
|---|---|---|---|---|---|
| ACID transactions | ✓ full | ✓ | ✗ (eventual) | ✓ | ✓ |
| Pure Rust | ✓ | ✓ | ✓ | ✗ (FFI) | ✗ (FFI) |
| Replication | ✓ built-in HA | ✗ | ✗ | ✗ | partial |
| Idiomatic API | ✗ Java-ported | ✓ | ✓ | ✓ | ✓ |
| Crates.io | ✗ not published | ✓ | ✓ | ✓ | ✓ |
| Iterator impl | ✗ | ✓ | ✓ | ✓ | ✓ |
| Production battle-tested | unknown | growing | yes | yes (lmdb) | yes |
| XA / 2PC | ✓ | ✗ | ✗ | ✗ | ✗ |
| Schema evolution / DPL | ✓ | ✗ | ✗ | ✗ | ✗ |
| Secondary indexes | ✓ | partial | ✗ | ✗ | ✗ |

### "Why Noxu?" Elevator Pitch

Noxu is the only pure-Rust embedded database that combines full ACID
transactions, built-in master-replica replication with automatic leader
elections, secondary indexes, XA two-phase commit, and a schema-evolution
persistence layer — all without an external process or a C FFI boundary.
If you need BDB-JE semantics in Rust with HA replication, nothing else
comes close.

### "Why Not Noxu?" Honest List

1. **Not published to crates.io.** You must depend via a git URL. This is a
   hard stop for many projects and package audits.

2. **API is not idiomatic Rust.** `DatabaseEntry` out-params, `OperationStatus`
   returns, no `Iterator` impl on `Cursor`, no `range()` shorthand. The
   API clearly descends from Java and requires a mental Java-to-Rust
   translation layer at every call site.

3. **Documentation examples don't compile.** All `///` examples are
   ````ignore`. The README has at least two API bugs. Confidence that
   the documentation matches the code is low.

4. **Memory ordering bug in the WAL buffer** (Finding 4.4). The
   `pin_count.fetch_sub(1, Relaxed)` in `LogBufferSegment::put` is
   incorrect — a reader seeing `pin_count == 0` via a Relaxed load may
   not see the buffer writes. This needs to be verified under Miri and
   fixed before the library can be considered production-safe.

5. **No Miri story.** The AGENTS.md build instructions don't mention
   `cargo +nightly miri test` for the unsafe-containing crates. Given
   Finding 4.3 and 4.4, running Miri on `noxu-log` is non-optional.

---

## 9. Clippy / fmt / Nursery / Pedantic Pass

### Finding 9.1 — `#![allow(dead_code)]` blanket-suppressed across workspace

**Severity**: medium  
**Topic**: Code quality  
**File:line**: All `lib.rs` files  

Every crate has `#![allow(dead_code, ...)]` in its lib.rs. This is a
global silencer that prevents any unused-code warning from surfacing.
A quick experiment: there is `crates/noxu-db/src/get.rs:Get::SearchLte`
and `Get::FirstDup` that are public variants returning `Unsupported` —
these are dead in the sense that callers never write correct code using
them. The blanket `allow(dead_code)` means any truly dead internal code
also hides silently.

**Recommendation**: Replace the blanket `allow` with targeted
`#[allow(dead_code)]` attributes on specific items that are intentionally
kept for future use, with a comment explaining when they'll be activated.

---

### Finding 9.2 — Mixed `std::sync::Mutex` and `noxu_sync::Mutex` usage

**Severity**: medium  
**Topic**: Code quality / consistency  
**File:line**: `crates/noxu-collections/src/stored_list.rs:24`,
`crates/noxu-db/src/sequence.rs:32`,
`crates/noxu-dbi/src/cursor_impl.rs:25`,
`crates/noxu-recovery/src/checkpointer.rs:157`

```rust
// Some files:
use noxu_sync::Mutex;  // noxu's custom futex-backed Mutex

// Other files in the same workspace:
use std::sync::Mutex;  // stdlib Mutex (can poison!)
```

The codebase uses `noxu_sync::Mutex` as the primary primitive for its
diagnostics capabilities (`get_n_waiters()`, `get_owner()`). But eight
call sites still use `std::sync::Mutex`, which can poison on panic. The
`noxu_sync::Mutex` is specifically designed to be poison-free.

This inconsistency means some code paths need `unwrap()` or
`.unwrap_or_else(|p| p.into_inner())` to handle mutex poisoning, while
others don't. See `database.rs:281` for the auto-txn unlock:

```rust
let mut auto_txn = match Arc::try_unwrap(auto_txn_arc) {
    Ok(m) => m.into_inner().unwrap_or_else(|p| p.into_inner()),
    …
}
```

---

### Finding 9.3 — `match … None` pattern where `Option::map` would be cleaner

**Severity**: nitpick  
**Topic**: Clippy/idiom  
**File:line**: `crates/noxu-db/src/database.rs:489–493` (representative)

```rust
// Before
let key_bytes = match key.get_data() {
    Some(k) => k,
    None => return Ok(OperationStatus::NotFound),
};

// This is fine — early return is idiomatic. But in other places:
key.get_data().map_or(0, |k| k.len())  // correct idiom in observe_span
```

The pattern is inconsistent. In `get()` the early-return is correct. But
in `observe_span!` the same `map_or` idiom is used inline. This is
a style inconsistency rather than a bug.

---

### Finding 9.4 — `pub mod` everywhere, nothing is `pub(crate)`

**Severity**: medium  
**Topic**: Visibility / API stability  
**File:line**: `crates/noxu-db/src/lib.rs:46–80`

```rust
pub mod cache_mode;
pub mod checkpoint_config;
pub mod cursor;
// ... (every module is pub)
```

All modules are `pub`. This is intentional (re-exported at crate root), but
it also means every struct, enum, and function defined in those modules is
public-by-default even if not intended for external use. Internal
implementation helpers in `database.rs` (e.g., `apply_auto_txn_undo`,
`make_cursor_with_locker`, `make_cursor_no_lock`, `check_open`,
`check_writable`) are accessible to any user via `noxu_db::database::Database`.

This is not immediately harmful (they're not re-exported at crate root) but
it means users can write `use noxu_db::database::Database` and call internal
methods, and semver requires those methods to remain stable. The
`pub(crate)` visibility modifier should be used for internal helpers.

---

### Finding 9.5 — Duplicate builder methods on all 150+ config parameters

**Severity**: medium  
**Topic**: Clippy/idiom  
**File:line**: `crates/noxu-db/src/environment_config.rs` (entire file)

As noted in Finding 1.3, having both `set_x(&mut self)` and `with_x(self)`
on every parameter means the EnvironmentConfig type has 300+ methods.
`cargo doc` renders this as an overwhelming wall. The `set_*` variants
returning `&mut Self` follow a pattern from before Rust's builder ergonomics
settled on consuming `self`-returning builders. With clippy pedantic,
the `set_*` variants that take `&mut self` but are never used as
`config.set_x(...); config.set_y(...)` (i.e. never chained in `&mut` style)
would be flagged by `clippy::return_self_not_must_use`.

---

## 10. Concurrency Primitives

### Finding 10.1 — Inconsistent Mutex selection (`std::sync` vs `noxu_sync`)

**Severity**: medium  
**Topic**: Concurrency primitives  
**File:line**: Multiple (see Finding 9.2)

Noxu introduced its own futex-backed `noxu_sync::Mutex` to get diagnostic
capabilities. But several production-path files still import
`std::sync::Mutex`:

- `noxu-cleaner/src/throttle.rs` — the writer backpressure throttle
- `noxu-dbi/src/cursor_impl.rs` — the cursor implementation
- `noxu-recovery/src/checkpointer.rs` — the checkpoint daemon

The `std::sync::Mutex` can poison (propagating panics across thread
boundaries). The project's stated intent is to use `noxu_sync::Mutex`
throughout. These are likely unintentional oversights from the porting
work.

---

### Finding 10.2 — `tokio::sync::Mutex` not used when it should be

**Severity**: low  
**Topic**: Concurrency primitives  
**File:line**: `crates/noxu-rep/src/net/channel.rs:634`

```rust
stream: Arc<std::sync::Mutex<Box<dyn TlsStreamOps>>>,
```

In `TlsTcpChannel`, the TLS stream is behind a `std::sync::Mutex`. Since
this is in `noxu-rep` which uses `tokio` for QUIC networking, and if
`TlsTcpChannel::send` were to be called from async code, holding a
`std::sync::Mutex` across a `.await` would cause issues. The comment on
line 224 acknowledges this: "noxu_sync::Mutex is used rather than
std::sync::Mutex". The TLS channel doesn't follow this guidance.

---

### Finding 10.3 — Memory ordering at `LogBufferSegment` (critical — see §4)

**Severity**: critical  
**Topic**: Concurrency primitives  
**File:line**: `crates/noxu-log/src/log_buffer.rs:374–376`

See Finding 4.4. The `pin_count.fetch_sub(1, Ordering::Relaxed)` should be
`fetch_sub(1, Ordering::Release)` and the corresponding load should be
`load(Ordering::Acquire)`. This is not a style issue — it is a correctness
issue that can cause stale data to be read from the log buffer.

---

### Finding 10.4 — `Ordering::SeqCst` in tests (informational)

**Severity**: informational  
**Topic**: Concurrency primitives  
**File:line**: `crates/noxu-collections/src/transaction_runner.rs:328`,
`crates/noxu-latch/src/exclusive.rs:282`

Test code uses `Ordering::SeqCst` for correctness. This is fine in tests
(it's the safest ordering), but it means test atomics don't validate
whether the production code's weaker orderings are sufficient. Using
`SeqCst` in tests can mask ordering bugs in production code that uses
`Relaxed`. Not a production bug but worth noting.

---

## Summary Table

| # | Severity | Topic | File / Location | Short Description |
|---|---|---|---|---|
| 1.1 | high | API ergonomics | `database.rs` | `OperationStatus` instead of `Option<V>` |
| 1.2 | high | API ergonomics | `database.rs:478` | `DatabaseEntry` out-param instead of return value |
| 1.3 | medium | API ergonomics | `environment_config.rs` | Duplicate `set_x` / `with_x` builder pattern |
| 1.4 | **high** | API doc bugs | `README.md:63,80` | Two API bugs in Quick Start — won't compile |
| 1.5 | **high** | Doc / drift | All `lib.rs` | All examples `ignore`d — never compile-tested |
| 1.6 | low | API ergonomics | `cursor.rs:94` | Dead `Option<LockMode>` arg always `None` |
| 2.1 | high | Iterator | `cursor.rs` | `Cursor` doesn't implement `Iterator` |
| 2.2 | medium | Iterator | `stored_iterator.rs` | Eager materialization — OOMs on large scans |
| 2.3 | medium | Iterator | `database.rs` | No `Database::range()` shorthand |
| 3.1 | medium | Generics | `feeder.rs:307` | `Arc<dyn Channel>` should be generic type param |
| 3.2 | medium | Generics | `tree.rs:50` | `Arc<dyn Fn>` comparator on every key compare |
| 3.3 | low | Generics | `evictor.rs:150` | `Box<dyn NodeEvictionInfo>` per eviction |
| 4.1 | **high** | Soundness | `elections/*.rs` | 3 unnecessary `unsafe impl Send+Sync` |
| 4.2 | **high** | Soundness | `log_source.rs:59` | Undocumented `transmute` lifetime extension |
| 4.3 | **high** | Soundness | `log_buffer.rs:329` | Raw-ptr `LogBufferSegment` without lifetime |
| 4.4 | **critical** | Soundness | `log_buffer.rs:374` | `Relaxed` ordering on WAL pin_count decrement |
| 4.5 | medium | Soundness | 12 zero-unsafe crates | Missing `#![forbid(unsafe_code)]` |
| 5.1 | medium | Type system | `transaction.rs` | Runtime TxnState check; typestate possible |
| 5.2 | informational | Type system | `stored_map.rs:65` | `PhantomData<fn()>` correct but undocumented |
| 5.3 | low | Type system | `secondary_config.rs` | `Send+Sync` bounds undocumented on user traits |
| 6.1 | high | Build/Cargo | `Cargo.toml:175` | Workspace lints vestigial; global `allow` silences |
| 6.2 | medium | Build/Cargo | `Cargo.toml` | No `rust-version` MSRV declared |
| 7.1 | medium | Documentation | `lib.rs:13` | Crate-level doc too thin |
| 7.2 | low | Documentation | `database.rs:36` | Doc examples have API bugs + wrong method names |
| 7.3 | low | Documentation | `crates/*/` | No per-crate README |
| 8.1 | — | Usability | — | Not on crates.io |
| 8.2 | — | Usability | — | No Miri test story |
| 9.1 | medium | Code quality | All lib.rs | `allow(dead_code)` global silencer |
| 9.2 | medium | Code quality | Multiple | Mixed Mutex types |
| 9.3 | nitpick | Clippy | `database.rs:489` | Inconsistent `match` vs `map_or` |
| 9.4 | medium | Visibility | `lib.rs:46–80` | All modules `pub`; no `pub(crate)` |
| 9.5 | medium | Clippy | `environment_config.rs` | 300+ methods from duplicate builder pattern |
| 10.1 | medium | Concurrency | Multiple | `std::sync::Mutex` in production paths |
| 10.2 | low | Concurrency | `channel.rs:634` | `std::sync::Mutex` around TLS stream |
| 10.3 | **critical** | Concurrency | `log_buffer.rs:374` | (same as 4.4) |
| 10.4 | informational | Concurrency | Test code | `SeqCst` in tests masks ordering bugs |

### Counts per severity

| Severity | Count |
|---|---|
| critical | 1 |
| high | 9 |
| medium | 13 |
| low | 7 |
| informational | 3 |
| nitpick | 1 |
| **Total** | **34** |

---

## Top 5 Most Actionable Improvements

### #1 — Fix `Ordering::Relaxed` → `Ordering::Release` on WAL pin_count (critical, ~1h)

`crates/noxu-log/src/log_buffer.rs:374–376`. Change:

```rust
(*self.pin_count).fetch_sub(1, Ordering::Relaxed);
```

to:

```rust
(*self.pin_count).fetch_sub(1, Ordering::Release);
```

And the reader's zero-check to `Acquire`. Run `cargo +nightly miri test -p
noxu-log` to verify. This is a correctness bug in the WAL; everything else
is ergonomics.

### #2 — Fix the README Quick Start (30 minutes)

Lines 63 and 80 of `README.md` reference a non-existent 4-arg `db.get` and
a non-existent `cursor.get_next()`. Fix them to use the actual API. Then
convert the `lib.rs` example from ```` ```ignore ```` to ```` ```no_run ````
so it compiles on every `cargo test`. These are the first two things any
potential user will see.

### #3 — Remove the three unnecessary `unsafe impl Send+Sync` in `noxu-rep/elections`

`election.rs:302`, `master_tracker.rs:163`, `phi_detector.rs:212`. These
types should derive `Send + Sync` automatically because all their fields
are `Send + Sync`. Removing the `unsafe impl` lets the compiler verify the
invariant instead of requiring humans to. If the `unsafe impl` was masking
a real problem, the compiler will surface it.

### #4 — Add `#![forbid(unsafe_code)]` to the 12 zero-unsafe crates (~15 minutes)

Add to the `lib.rs` of `noxu-tree`, `noxu-txn`, `noxu-evictor`,
`noxu-cleaner`, `noxu-recovery`, `noxu-dbi`, `noxu-engine`, `noxu-bind`,
`noxu-collections`, `noxu-persist`, `noxu-config`, `noxu-util`. This makes
the zero-unsafe claim machine-enforced rather than honor-based.

### #5 — Add lazy `Database::iter()` and `Database::range()` (1–2 days)

These two methods are what every user of an embedded KV library looks for
first. Without them, Noxu cannot be used in a `for` loop, cannot compose
with `Iterator::filter`, and cannot do streaming range scans. The
`StoredMap::iter()` eager materialization is not a substitute — it OOMs on
large tables and requires the collections layer. A lazy cursor-backed
`Iterator` at the `Database` level is the minimum viable API.

---

## Elevator Pitch

### "Why Noxu?"

Noxu is the only pure-Rust embedded database with built-in master-replica
HA replication, automatic leader elections, full XA two-phase commit, and a
typed schema-evolution persistence layer — features that would otherwise
require gluing together three separate libraries. For a Rust service that
needs embedded transactional storage with predictable crash recovery and
replication without the operational cost of a separate database process,
Noxu is the only game in town.

### "Why Not Noxu?" (honest list)

1. **Not on crates.io.** Git-only dependency — a blocker for most
   production-ready Rust projects and for automated supply chain audits.

2. **The API is a Java BDB port, not a Rust API.** `DatabaseEntry` out-params,
   `OperationStatus` results, `Get` enum navigation, no `Iterator` impl.
   Every line of user code requires consulting the docs to know which of 40
   methods to call.

3. **README examples don't work.** The library's first impression is
   broken code. `db.get` has the wrong arity; `cursor.get_next` doesn't
   exist. This signals API instability even if the engine is solid.

4. **Potential WAL correctness bug.** The `Relaxed` memory ordering on the
   log buffer pin-count decrement (Finding 4.4) is wrong under the C++/Rust
   memory model. Until this is verified under Miri and corrected, the
   library's ACID correctness claims are unverified.

5. **No published crate, no crates.io CI badge, no Miri story.** The
   barrier to adoption is high. Other pure-Rust alternatives (`redb`,
   `sled`) have ergonomic APIs, are on crates.io, and have seen real-world
   production use.

---

*End of audit. Path to this report: `/tmp/noxu-audit-jonhoo.md`*
