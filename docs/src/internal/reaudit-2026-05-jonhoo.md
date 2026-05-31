# Noxu DB — Re-Audit (Jonhoo, 2026-05-30)

**Reviewer persona**: Jon Gjengset (jonhoo)  
**Date**: 2026-05-30  
**Codebase**: `/tmp/reaudit-jonhoo` (`origin/main`, commit `8f63f6e`, v3.0.2)  
**Prior audit**: `docs/src/internal/audit-2026-05-jonhoo.md` (v2.4.1, 2026-05-29)  
**Scope**: Umbrella crate review, lingering ergonomics, new code soundness, Cargo hygiene.

---

> "Substantial progress. The engine is still 8/10. The API surface improved from 4/10
> to a credible 6/10 — the new umbrella is a real front door, the critical WAL ordering
> bug is fixed, the elections `unsafe` is gone, and `Database::iter()`/`range()` land.
> But the umbrella's own Quick-start example is *still wrong*, there's a proc-macro
> packaging trap that will bite every user who finds noxu-persist on docs.rs, and
> the `StoredMap` eager-materialize OOM is still sitting there. One critical README bug
> survived."

---

## What was fixed (do not re-report)

| Prior finding | Status | Notes |
|---|---|---|
| C-7 / 4.4 — Relaxed pin_count in WAL | ✓ Fixed | `fetch_sub(Release)` + `load(Acquire)` at `log_buffer.rs:298, 306` |
| 4.1 / H-10 — `unsafe impl Send+Sync` in elections | ✓ Fixed | All three blocks removed |
| 4.5 / Q-5 — `#![forbid(unsafe_code)]` on 12 zero-unsafe crates | ✓ Fixed | Compiler-enforced |
| 1.4 / H-8 — README `cursor.get_next` (first half) | ✓ Fixed | Changed to `cursor.get(…, Get::Next, None)` |
| 2.1 / Q-1 — `Cursor` does not implement `Iterator` | ✓ Fixed | `DbIter` / `DbRange` in `db_iter.rs`, lazy, 12 tests |
| H-1 — EnvironmentImpl lock held across abort undo | ✓ Fixed | Lock dropped before undo loop |
| H-3 — Per-log-entry alloc pressure | ✓ Partially fixed | Scratch `Vec<u8>` embedded in LWL |
| H-4 — Victim selection dead code | ✓ Fixed | `lock_counts` populated |
| H-9 — `PartialEvict` does not free data | ✓ Fixed | `strip_lns` clears slot data |
| Q-2 — Bare `#[ignore]` no reason strings | ✓ Fixed | Reason strings added |
| C-1/C-2/C-3/C-4/C-5/C-6 | ✓ Fixed | Waves 11-Q and 11-R |

---

## Section 1 — The Umbrella Crate (`crates/noxu`)

### Finding U-1 — Umbrella Quick-start example has **two** API bugs (still `ignore`d)

**Severity**: high  
**Topic**: Documentation / API bugs  
**File:line**: `crates/noxu/src/lib.rs:19–30`

```rust
// What the umbrella's ```ignore Quick-start says:
let db = env.open_database(None, "kv", true)?;         // (A)
db.put(&txn, b"hello", b"world")?;                     // (B)
```

**Bug A** — `open_database(None, "kv", true)`:  
The actual signature is `open_database(txn, name, config: &DatabaseConfig)`.  
The third argument is `&DatabaseConfig`, not `bool`.  This won't compile.

```rust
// Actual signature (environment.rs:448)
pub fn open_database(
    &self,
    txn: Option<&Transaction>,
    name: &str,
    config: &DatabaseConfig,      // ← &DatabaseConfig, not bool
) -> Result<Database>
```

**Bug B** — `db.put(&txn, b"hello", b"world")`:  
The actual signature is `put(txn: Option<&Transaction>, key: &DatabaseEntry, data: &DatabaseEntry)`.  
Two problems: `&txn` is `&Transaction`, not `Option<&Transaction>` (should be `Some(&txn)`);  
and `b"hello"` is `&[u8; 5]`, which does not coerce to `&DatabaseEntry`  
(despite `impl From<&[u8]> for DatabaseEntry` existing, the coercion requires an explicit conversion).

These examples are behind `` ```ignore `` so `cargo test` never catches them.  
The smoke test in `crates/noxu/tests/smoke.rs` uses the **correct** API;  
the doc example is simply wrong.

**Suggested fix**:

```rust
// Change to no_run with the correct API:
//! ```no_run
//! use noxu::{DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig};
//! use std::path::PathBuf;
//!
//! let env = Environment::open(
//!     EnvironmentConfig::new(PathBuf::from("/tmp/mydb"))
//!         .with_allow_create(true)
//!         .with_transactional(true),
//! )?;
//! let db_config = DatabaseConfig::new().with_allow_create(true).with_transactional(true);
//! let db = env.open_database(None, "kv", &db_config)?;
//! let txn = env.begin_transaction(None)?;
//! db.put(Some(&txn), &DatabaseEntry::from_bytes(b"hello"), &DatabaseEntry::from_bytes(b"world"))?;
//! txn.commit()?;
//! # Ok::<(), noxu::NoxuError>(())
//! ```
```

---

### Finding U-2 — README Quick-start still has a 4-arg `db.get()` call

**Severity**: high  
**Topic**: Documentation / API bugs  
**File:line**: `README.md:68`

```rust
let status = db.get(None, &key, &mut result, None)?;  // ← 4 args WRONG
```

The actual `Database::get` signature has **three** arguments:

```rust
pub fn get(
    &self,
    txn: Option<&Transaction>,
    key: &DatabaseEntry,
    data: &mut DatabaseEntry,
) -> Result<OperationStatus>
```

The 4-arg variant is `get_with_options` which takes a `&ReadOptions` as the fourth argument,
not `None`. Wave 11-S fixed the `cursor.get_next` bug in the README but missed this separate bug
on line 68. The README code block is marked `` ```rust `` (not `` ```ignore ``), so it appears
as a copyable example to users. Pasting it gives a compiler error.

**[prior: 1.4 / H-8]** — partially fixed (cursor.get_next was addressed; this db.get() call was not.

**Suggested fix**: change to `db.get(None, &key, &mut result)?`  
or, if you want to show `ReadOptions`, write `db.get_with_options(None, &key, &mut result, &ReadOptions::default())?`.

---

### Finding U-3 — Persist-derive hard-codes `::noxu::persist::` — no escape hatch

**Severity**: high  
**Topic**: Packaging trap  
**File:line**: `crates/noxu-persist-derive/src/lib.rs:127, 190, 227, 232, 245, 274–288, 331–345, 457–464`

The derive macros (`Entity`, `PrimaryKey`, `SecondaryKey`) emit code containing **hard-coded**
`::noxu::persist::` paths:

```rust
// Generated by #[derive(Entity)]:
impl ::noxu::persist::Entity for User { … }
```

This means the derives are **only usable when the `noxu` umbrella crate is in the dependency
graph**, because `::noxu` refers to the crate root of the `noxu` dependency.

The trap: `noxu-persist` is published as a crate and its own crate-level doc example says:

```rust
// noxu-persist/src/lib.rs:22–29 — this DOES NOT COMPILE without `noxu` in Cargo.toml:
use noxu_persist::{Entity, SecondaryKey};
#[derive(Clone, Debug, Entity, SecondaryKey)]
struct User { … }
```

A user who follows `noxu-persist`'s own example and writes `noxu-persist = "3"` in their
`Cargo.toml` — without `noxu = "3"` — gets a compile error from generated code referencing
the missing `::noxu` crate.

The warning "This crate is published only so the `noxu` umbrella crate can depend on it"
is present in the doc header, but the example code **contradicts it** by using the direct
crate path.

**The idiomatic fix**: follow the `serde_derive` pattern — generate a configurable crate
path, defaulting to the crate where the macro is called from.  Minimum fix:

```rust
// In the derive macro, emit:
impl #crate_path::Entity for #struct_ident { … }
// where `crate_path` defaults to `::noxu::persist` but is overridable via:
// #[entity(crate = "noxu_persist")]   ← serde uses #[serde(crate = "...")]
```

**Near-term minimum fix** without the escape hatch: change the `noxu-persist` doc example
to use `use noxu::persist::{Entity, SecondaryKey}` and add a boxed WARNING that the derive
requires the `noxu` umbrella, not `noxu-persist` directly.

---

### Finding U-4 — `recovered_prepared_txns` returns an unnamed type

**Severity**: medium  
**Topic**: API type leak  
**File:line**: `crates/noxu-db/src/environment.rs:1226–1228, 1240–1243, 1318–1320`

Three public methods on `Environment` return `noxu_recovery::` types that are not
re-exported anywhere in the `noxu` umbrella or `noxu-db`:

```rust
// environment.rs:1226–1228
pub fn recovered_prepared_txns(&self) -> Vec<noxu_recovery::PreparedTxnInfo>

// environment.rs:1240–1243
pub fn take_recovered_prepared_lns(&self, txn_id: u64) -> Vec<noxu_recovery::PreparedLnReplay>

// environment.rs:1318–1320
pub fn apply_recovered_prepared_lns(&self, lns: &[noxu_recovery::PreparedLnReplay])
```

`PreparedTxnInfo` and `PreparedLnReplay` are defined in `noxu-recovery/src/analysis_result.rs`
and re-exported by `noxu-recovery/src/lib.rs`. They are **not** re-exported by `noxu-db`
(grep confirms nothing in `noxu-db/src/lib.rs` mentions them), and `noxu-recovery` is not
exposed through the umbrella at all.

A user calling `env.recovered_prepared_txns()` cannot name the return type without adding
`noxu-recovery` as a direct dependency — exactly what the umbrella is supposed to prevent.

**Suggested fix**:

```rust
// In noxu-db/src/lib.rs, add:
pub use noxu_recovery::{PreparedTxnInfo, PreparedLnReplay};
```

Or, preferably, wrap these in XA-specific types in `noxu-xa` so they only appear when
the `xa` feature is enabled. XA is the only caller context for these methods.

---

### Finding U-5 — Umbrella exposes internal module hierarchy via `pub use noxu_db::*`

**Severity**: low  
**Topic**: API surface / docs pollution  
**File:line**: `crates/noxu/src/lib.rs:57` (`pub use noxu_db::*`)

`noxu-db` declares all its sub-modules as `pub mod`:

```rust
pub mod cursor;
pub mod database;
pub mod environment;
// … (28 modules total)
```

Since the umbrella does `pub use noxu_db::*`, every module is re-exported under `noxu::`.
A user can write:

```rust
use noxu::cursor::CursorState;      // module path
use noxu::CursorState;              // re-export path — both work
use noxu::database::Database;       // exposes pub fn check_open(), check_writable(), etc.
```

The `check_open()` and `check_writable()` private helpers on `Database` are `fn` (private)
and not accessible, but public struct fields and enum variants in those modules are. This creates
a wider semver surface than intended: any symbol added to any of those 28 modules becomes part of
the `noxu` semver contract. `docs.rs` will also show the full module hierarchy alongside the
clean re-exports, making the docs page harder to navigate.

**Suggested fix**: In `noxu-db/src/lib.rs`, change internal-use modules to `pub(crate)`.
Only keep `pub mod` for modules that contain types meant to be navigated via the module path
(`db_iter`, `error`, etc.). In the umbrella, replace the blanket `pub use noxu_db::*` with
explicit item imports.

---

### Finding U-6 — Umbrella crate is missing `#![forbid(unsafe_code)]`

**Severity**: low  
**Topic**: Soundness / guard rails  
**File:line**: `crates/noxu/src/lib.rs` (first line absent)

The umbrella crate contains only re-exports (`pub use`, `pub mod`) — no `unsafe` code. The 12
core engine crates all gained `#![forbid(unsafe_code)]` in Wave 11-Q, but `noxu`, `noxu-db`,
`noxu-xa`, and `noxu-rep` do not carry the attribute. Only `noxu-bind` does.

For the umbrella in particular, there is no reason for `unsafe` code to ever appear. Add:

```rust
#![forbid(unsafe_code)]
```

at the top of `crates/noxu/src/lib.rs`.

---

## Section 2 — API Ergonomics (Lingering)

### Finding E-1 — `OperationStatus` / `DatabaseEntry` out-params are still the primary API

**Severity**: medium (unchanged from prior audit)  
**Topic**: API ergonomics  
**File:line**: `crates/noxu-db/src/database.rs:491–562`

The original findings 1.1 and 1.2 — `Result<OperationStatus>` instead of `Result<Option<Bytes>>`
and `DatabaseEntry` out-params — were deliberately not addressed in the wave plan.
Wave 11-L catalogued them as known pre-v3.0 stability items, with v3.0 locking the current
shape. This is a deliberate choice, not an oversight.

Reporting here for completeness as the single biggest ergonomics gap vs. `redb`/`heed`:

```rust
// Current Noxu API (v3.0.2):
let mut out = DatabaseEntry::new();
let status = db.get(None, &key_entry, &mut out)?;
if status == OperationStatus::Success {
    let val: &[u8] = out.get_data().unwrap();
}

// redb equivalent:
let read_txn = db.begin_read()?;
let table = read_txn.open_table(TABLE)?;
let val: Option<AccessGuard<'_, &[u8]>> = table.get(key)?;
```

The `DatabaseEntry::from_bytes(b"key")` constructor makes the API workable, but the
two-layer result (check `OperationStatus`, then extract data) is not idiomatic Rust.
This is a known v4.0 candidate item, not a regression.

---

### Finding E-2 — `DbIter` / `DbRange` carry no `'txn` lifetime — use-after-commit is silent

**Severity**: medium  
**Topic**: API ergonomics / type safety  
**File:line**: `crates/noxu-db/src/db_iter.rs:73–119`

The new `DbIter` and `DbRange` types do not carry a lifetime parameter tied to the `Transaction`:

```rust
pub struct DbIter {
    cursor: Cursor,
    started: bool,
    done: bool,
}
```

`Database::iter` takes `txn: Option<&Transaction>` but returns `Result<DbIter>` (no lifetime).
This means the borrow checker cannot prevent:

```rust
let txn = env.begin_transaction(None)?;
let iter = db.iter(Some(&txn))?;
txn.commit()?;                          // txn dropped / committed
for result in iter { … }               // cursor now orphaned — runtime error, not compile error
```

The runtime path will raise `OperationNotAllowed` on the orphaned cursor, so this isn't a
soundness issue. But it's a missed opportunity: the canonical API for a transactional
iterator is:

```rust
pub struct DbIter<'txn> {
    cursor: Cursor,
    _txn: PhantomData<&'txn Transaction>,
}

pub fn iter<'txn>(&self, txn: Option<&'txn Transaction>) -> Result<DbIter<'txn>>
```

Without the lifetime, `for r in iter` after `txn.commit()` is a logic bug that the
compiler will not catch. Every other transactional Rust KV library (`redb`, `heed`)
enforces this with a lifetime.

---

### Finding E-3 — No reverse (`DoubleEndedIterator`) support in `DbIter`/`DbRange`

**Severity**: low  
**Topic**: API completeness  
**File:line**: `crates/noxu-db/src/db_iter.rs`

`DbIter` and `DbRange` implement `Iterator` but not `DoubleEndedIterator`. Common use cases
like "last N entries", "range in reverse", or `iter().rev()` require opening a `Cursor` and
manually using `Get::Last`/`Get::Prev`. The cursor supports backward navigation
(`Get::moves_backward()` exists); it just isn't exposed through the iterator API.

```rust
// What users want:
for result in db.iter(None)?.rev() { … }
```

This is a natural extension, not a critical gap, but it's notable given that `redb`, `sled`,
and `heed` all support reverse iteration.

---

### Finding E-4 — `StoredMap::iter()` is *still* eager — O(n) memory, documented as such

**Severity**: medium  
**Topic**: Iterator conventions  
**File:line**: `crates/noxu-collections/src/stored_map.rs:240–256`

The prior finding 2.2 flagged `StoredMap::iter()` as eager (materializes the entire DB to a `Vec`).
It is **still eager** in v3.0.2, and the doc now confirms this explicitly:

```rust
/// The iterator is materialised eagerly: at the call to `iter()`
/// the cursor walks every record under `txn` and decodes every
/// pair into the returned `Vec`-backed iterator.
pub fn iter(&self, txn: Option<&Transaction>) -> Result<StoredIterator<(K, V)>>
```

The wave plan deferred this to a future release, and being honest in the docs is better than
silent OOM. Still, any user with a `StoredMap` over more than a few hundred thousand entries
will be surprised by this. The lazy `Database::iter()`/`range()` added at the lower layer
(Finding E-2) is the workaround, but it bypasses the typed bindings.

This is a known gap, not a regression. But it remains the biggest performance footgun in the
collections layer.

---

## Section 3 — Soundness in New Code

### Finding S-1 — `noxu-xa` uses `std::sync::Mutex` with `.unwrap()` throughout

**Severity**: medium  
**Topic**: Concurrency consistency / panic propagation  
**File:line**: `crates/noxu-xa/src/environment.rs:3, 174, 209, 260, 290, 312, 337, 400, 443, 451, 479, 521, 525, 543, 571`

The XA environment imports `std::sync::Mutex` (not `noxu_sync::Mutex`) and calls `.unwrap()`
on every lock acquisition:

```rust
use std::sync::Mutex;   // ← std, can poison on panic
// …
let branches = self.branches.lock().unwrap();     // propagates across thread boundaries
self.resolving_xids.lock().unwrap().insert(xid);  // 9 more similar sites
```

The project's stated intent is to use `noxu_sync::Mutex` throughout (which is poison-free).
A panic inside any `branches` or `resolving_xids` critical section will poison the mutex,
causing every subsequent XA operation to panic. This is a consistency gap with the rest of
the codebase, not a new pattern — but it means the XA environment is the only place where
a panic in one operation (e.g., during `xa_prepare`) can permanently break all subsequent
XA operations in the same process.

**Suggested fix**: Replace `use std::sync::Mutex` with `use noxu_sync::Mutex` in
`noxu-xa/src/environment.rs`. Remove the 15 `.unwrap()` calls; `noxu_sync::Mutex::lock()`
does not return a `Result`.

---

### Finding S-2 — `db_trees_registry` and `primary_tree` use `std::sync::RwLock`

**Severity**: medium  
**Topic**: Concurrency consistency  
**File:line**: `crates/noxu-dbi/src/environment_impl.rs:7, 554, 617`

New code in `EnvironmentImpl` introduces `std::sync::RwLock` and `std::sync::Mutex` for
the `db_trees_registry` (the cleaner's per-DB tree dispatch map) and `primary_tree`:

```rust
// environment_impl.rs:617
let db_trees_registry: Arc<
    std::sync::Mutex<HashMap<i64, Arc<std::sync::RwLock<noxu_tree::Tree>>>>
> = Arc::new(std::sync::Mutex::new(HashMap::new()));

// environment_impl.rs:554
Arc::new(std::sync::RwLock::new(primary_tree_inner));
```

The rest of `EnvironmentImpl` uses `noxu_sync::RwLock` (imported on line 10). These two
usages create a mixed mutex story and carry the same `unwrap()`-on-poison risk as S-1.
The `db_trees_registry` is shared with the cleaner — a panic inside the cleaner could
poison this lock and break all subsequent database operations.

**Suggested fix**: Replace `std::sync::Mutex` with `noxu_sync::Mutex` and
`std::sync::RwLock` with `noxu_sync::RwLock` at these sites. Change the
`db_trees_registry` to `Arc<NoxuMutex<HashMap<…>>>`.

---

### Finding S-3 — `get_transaction` in XA uses a raw-pointer dereference that the `SAFETY` comment understates

**Severity**: low  
**Topic**: Soundness  
**File:line**: `crates/noxu-xa/src/environment.rs:173–189`

```rust
// SAFETY: The Transaction is heap-allocated via Box, so its address is stable
// across HashMap rehashes triggered by other xa_start calls. The reference's
// lifetime is bounded by `&self`.
// We deliberately drop the `branches` lock guard at the end of this function —
// callers serialize their own xid through the XA state machine, so no other
// thread will remove this branch while the caller is using the returned reference.
let txn_ptr: *const Transaction = &*branch.txn;
Ok(unsafe { &*txn_ptr })
```

The SAFETY argument is: (1) `Box` gives a stable address, (2) the lock is dropped and
the caller "serializes" their xid. Point (2) is the dangerous assumption: nothing in the
*type system* prevents a concurrent `xa_rollback(xid)` from removing `branch.txn` from the
HashMap while the caller holds the `&Transaction` reference. The XA protocol requires that
the TM serializes these operations, but this is a documentation invariant, not a compiler
invariant.

This is an acknowledged block in AGENTS.md's unsafe inventory and is not a new finding,
but the SAFETY comment should explicitly state the race that it relies on the caller to
prevent, and should note that this is a candidate for restructuring (e.g., returning
`Arc<Transaction>` or holding the lock guard in the returned type).

---

## Section 4 — Cargo / Workspace Hygiene

### Finding C-1 — No `rust-version` (MSRV) in `[workspace.package]`

**Severity**: medium  
**Topic**: Build/Cargo  
**File:line**: `Cargo.toml` — `[workspace.package]` section

The workspace uses `edition = "2024"` (Rust 1.85+, `rust-toolchain.toml` pins to 1.95),
but there is no `rust-version` field:

```toml
[workspace.package]
version = "3.0.2"
edition = "2024"
# ← no rust-version!
```

A user on Rust 1.80 will get cryptic edition-2024 syntax errors rather than a clear
"this crate requires Rust 1.85+". Add:

```toml
rust-version = "1.85"   # edition 2024 minimum; toolchain pins 1.95
```

**[prior: 6.2]** — not fixed between original audit and now.

---

### Finding C-2 — Workspace lints still vestigial; no `rust.warnings = "deny"`

**Severity**: medium  
**Topic**: Build/Cargo  
**File:line**: `Cargo.toml:180–184`

```toml
[workspace.lints.clippy]
or_fun_call = "warn"
redundant_clone = "warn"
large_stack_frames = "warn"
large_types_passed_by_value = "warn"
```

Still only four clippy lints at `warn`, unchanged from the original audit. Still missing:

- `rust.warnings = "deny"` — treat compiler warnings as errors in the workspace
- `clippy::undocumented_unsafe_blocks = "warn"` — catches new `unsafe { }` without `// SAFETY:`
- `clippy::missing_safety_doc = "warn"` — flags `unsafe fn` without a `# Safety` section
- `rust.unsafe_op_in_unsafe_fn = "deny"` — requires explicit `unsafe { }` inside `unsafe fn`

The CI command `cargo clippy … -D warnings` does catch warnings, but that flag is only
in AGENTS.md, not in the workspace lint table. Contributors who don't run the full CI command
don't see failures locally.

**[prior: 6.1]** — not fixed.

---

### Finding C-3 — `noxu-persist` dev-dep cycle is real but undocumented

**Severity**: low (informational)  
**Topic**: Build/Cargo  
**File:line**: `crates/noxu-persist/Cargo.toml:25–27`

```toml
[dev-dependencies]
noxu = { workspace = true, features = ["persist"] }  # required because derive emits ::noxu::persist::
```

`noxu-persist` → (prod dep) `noxu-persist-derive` → (emit code with) `::noxu::persist::`
← (umbrella) `noxu`.

So to test `noxu-persist` you need `noxu` in dev-deps. Cargo allows dev-dep cycles as
long as there are no *production* dep cycles. This is the correct workaround for the
hardcoded crate path problem (Finding U-3), but it means:

1. Any developer who adds a test to `noxu-persist` that uses a derive macro must know
   to import `noxu::persist::Entity` (not `noxu_persist::Entity`).
2. Publishing `noxu-persist` before `noxu` to crates.io is not possible, which means
   the Layer 5 publish order in the runbook must publish `noxu` before `noxu-persist`'s
   dev-dep test suite can run. The runbook in `docs/src/contributing/publishing.md` should
   call this out explicitly.

---

## Section 5 — Documentation That Compiles

### Finding D-1 — Umbrella lib.rs examples still `ignore`d (two of them)

**Severity**: high  
**Topic**: Documentation quality  
**File:line**: `crates/noxu/src/lib.rs:19, 49`

Both doc examples in the umbrella's `lib.rs` use `` ```ignore ``:

- Line 19: the Quick-start (which contains API bugs per U-1).
- Line 49: the `#[derive(Entity)]` example.

The `lib.rs` example in `noxu-db` was converted to `` ```no_run `` in Wave 11-S.
The umbrella's own examples were not. `cargo test` on the `noxu` crate does not
compile-check either doc example.

The smoke test in `crates/noxu/tests/smoke.rs` correctly exercises the API — but
the *documentation* first impressions are still wrong and unguarded.

**Count**: After Wave 11-S, 11 `` ```ignore `` blocks remain in `noxu-db/src/*.rs` alone
(cursor, database, disk_ordered_cursor, environment, join_cursor, secondary_config,
secondary_cursor, secondary_database, sequence, environment_mutable_config, environment).
Most of these predate the umbrella and haven't been touched.

---

### Finding D-2 — `noxu-persist` lib.rs doc example would break on direct use

**Severity**: medium  
**Topic**: Documentation quality / misleading  
**File:line**: `crates/noxu-persist/src/lib.rs:22–29`

```rust
/// ```ignore
/// use noxu_persist::{Entity, SecondaryKey};
///
/// #[derive(Clone, Debug, Entity, SecondaryKey)]
/// struct User { … }
/// ```
```

This example uses `noxu_persist::` import paths. As established in Finding U-3, a user
who has `noxu-persist = "3"` in their `Cargo.toml` and copies this example will get a
compiler error because the generated code references `::noxu::persist::Entity`.

The example should either be removed (since direct `noxu-persist` use is not supported)
or corrected to `use noxu::persist::{Entity, SecondaryKey}` with an explicit note that
the `noxu` umbrella crate must be in scope.

---

### Finding D-3 — `noxu-db` lib.rs `no_run` example creates configs but doesn't use them

**Severity**: low  
**Topic**: Documentation quality  
**File:line**: `crates/noxu-db/src/lib.rs:31–43`

```rust
//! ```no_run
//! use noxu_db::{EnvironmentConfig, DatabaseConfig};
//! use std::path::PathBuf;
//!
//! let env_config = EnvironmentConfig::new(PathBuf::from("/tmp/mydb"))
//!     .with_allow_create(true)
//!     .with_transactional(true);
//!
//! let db_config = DatabaseConfig::new()
//!     .with_allow_create(true)
//!     .with_transactional(true);
//! ```
```

The doc example compiles (it's `` ```no_run ``), but it only creates two config objects
and does nothing with them. There's no `Environment::open(env_config)`, no database open,
no transaction, no put/get. A first-time user reading `noxu-db`'s own doc page learns
nothing useful from this. The proper example is in `db_iter.rs` and `database.rs`; the
crate-level doc should show at least the minimal round-trip.

---

## Section 6 — "Would I Use `noxu = "3"` Today?"

### Comparison table (updated for v3.0.2)

| Criterion | Noxu 3.0.2 | redb 2.x | sled | heed/lmdb | rust-rocksdb |
|---|---|---|---|---|---|
| ACID transactions | ✓ full | ✓ | ✗ (eventual) | ✓ | ✓ |
| Pure Rust | ✓ | ✓ | ✓ | ✗ (FFI) | ✗ (FFI) |
| Replication | ✓ built-in HA | ✗ | ✗ | ✗ | partial |
| Umbrella crate | ✓ `noxu = "3"` | ✓ | ✓ | ✓ | ✓ |
| Idiomatic API | ~ (better but not clean) | ✓ | ✓ | ✓ | ✓ |
| `Iterator` on DB scan | ✓ (`DbIter`, `DbRange`) | ✓ | ✓ | ✓ | ✓ |
| crates.io published | ~ (version in Cargo.toml, actual publish pending) | ✓ | ✓ | ✓ | ✓ |
| README compiles | ~ (one remaining API bug) | ✓ | ✓ | ✓ | ✓ |
| XA / 2PC | ✓ | ✗ | ✗ | ✗ | ✗ |
| Schema evolution / DPL | ✓ | ✗ | ✗ | ✗ | ✗ |
| Secondary indexes | ✓ | partial | ✗ | ✗ | ✗ |
| Production battle-tested | unknown | growing | yes | yes (lmdb) | yes |

### Why Noxu? (updated pitch)

Noxu 3.0 ships as a **single crate** (`noxu = "3"`) — the umbrella finally exists and
works. The critical WAL ordering bug is fixed, the elections `unsafe` is gone, all twelve
zero-unsafe crates are compiler-enforced, and users now have a real lazy `Iterator` over
the database without materializing it in memory. The `#[derive(Entity)]` story is functional
when used through the umbrella.

For a Rust service that needs embedded transactional storage with predictable crash recovery
*and* built-in master-replica HA replication, nothing else in the ecosystem comes close.

### Why Not Yet? (honest list, updated)

1. **The umbrella's own Quick-start is still wrong.**  `open_database(None, "kv", true)` in
   `lib.rs` and `db.get(…, None)` with a 4th argument in `README.md` will produce compile
   errors for anyone who copies the examples. For an important release milestone (`noxu = "3"`),
   this is the first thing a user sees, and it's broken.

2. **The `#[derive(Entity)]` proc-macro has a packaging trap.**  The derives emit
   `::noxu::persist::` hard-coded paths. If you follow `noxu-persist`'s own doc example
   and use it directly, your code won't compile. The only documented escape is the umbrella,
   but the escape isn't made obvious enough and there's no `#[entity(crate = "…")]` override
   as `serde`'s `#[serde(crate = "…")]` provides.

3. **API shape is still Java-descended at the core.** `OperationStatus`, `DatabaseEntry`
   out-params, the `Get` enum navigation on raw cursors — these are all present in the
   stable surface locked at v3.0. You get the nicer `db.iter()`/`range()` on top, but every
   direct `Database` operation still returns `Result<OperationStatus>` with an out-param.

4. **`StoredMap::iter()` OOMs on large collections.** The typed persistence layer silently
   materializes everything. The doc says "eagerly" now — that's honest — but the fix is not
   coming until a later release.

5. **`noxu-xa` / `noxu-dbi` mix `std::sync::Mutex` with the project's `noxu_sync::Mutex`.**
   A panic in an XA critical section poisons locks and breaks all subsequent XA operations
   in the process. Minor for most users, but XA correctness is a selling point.

---

## Summary Table — New Findings

| # | Severity | Topic | File:line | Short description |
|---|---|---|---|---|
| U-1 | **high** | Doc / umbrella | `noxu/src/lib.rs:25–27` | Quick-start has 2 API bugs (`ignore`d, wrong `open_database` arg + wrong `put` signature) |
| U-2 | **high** | Doc / README | `README.md:68` | `db.get` called with 4 args; takes 3 [prior: 1.4, partially fixed] |
| U-3 | **high** | Packaging | `noxu-persist-derive/src/lib.rs:127+` | Derive emits `::noxu::persist::` hard-coded; no `crate=` escape hatch; direct `noxu-persist` use breaks |
| U-4 | medium | Type leak | `noxu-db/src/environment.rs:1228,1243,1320` | `PreparedTxnInfo`/`PreparedLnReplay` in public API, not re-exported through umbrella |
| U-5 | low | API surface | `noxu/src/lib.rs:57` | `pub use noxu_db::*` exposes 28 internal modules; semver surface wider than intended |
| U-6 | low | Soundness | `noxu/src/lib.rs` | Umbrella missing `#![forbid(unsafe_code)]` |
| E-1 | medium | Ergonomics | `database.rs:491` | `OperationStatus` / `DatabaseEntry` out-params still primary API [prior: 1.1/1.2, deferred] |
| E-2 | medium | Ergonomics | `db_iter.rs:73` | `DbIter`/`DbRange` have no `'txn` lifetime; use-after-commit is silent (runtime only) |
| E-3 | low | API completeness | `db_iter.rs` | No `DoubleEndedIterator`; reverse scans require manual `Cursor` |
| E-4 | medium | Iterator | `stored_map.rs:240` | `StoredMap::iter()` still eager (O(n) memory) [prior: 2.2, deferred] |
| S-1 | medium | Soundness | `noxu-xa/src/environment.rs:3` | `std::sync::Mutex` with 15 `.unwrap()` calls in XA; panic poisons locks |
| S-2 | medium | Soundness | `noxu-dbi/src/environment_impl.rs:617` | `db_trees_registry` uses `std::sync::Mutex`/`RwLock`; same poison risk |
| S-3 | low | Soundness | `noxu-xa/src/environment.rs:189` | `get_transaction` raw-ptr: SAFETY comment understates race; structural fix deferred |
| C-1 | medium | Build/Cargo | `Cargo.toml` | No `rust-version` in `[workspace.package]` [prior: 6.2, unfixed] |
| C-2 | medium | Build/Cargo | `Cargo.toml:180` | Workspace lints still vestigial (4 warns only) [prior: 6.1, unfixed] |
| C-3 | low | Build/Cargo | `noxu-persist/Cargo.toml:27` | Dev-dep cycle with umbrella undocumented in publish runbook |
| D-1 | high | Documentation | `noxu/src/lib.rs:19,49` | Both umbrella doc examples use `ignore`; not compile-checked |
| D-2 | medium | Documentation | `noxu-persist/src/lib.rs:22` | Persist doc example uses `noxu_persist::` import; breaks on direct use |
| D-3 | low | Documentation | `noxu-db/src/lib.rs:31` | Crate-level `no_run` example creates configs but does nothing |

### Counts per severity (new findings only)

| Severity | Count |
|---|---|
| high | 4 (U-1, U-2, U-3, D-1) |
| medium | 8 (U-4, E-1, E-2, E-4, S-1, S-2, C-1, C-2) |
| low | 7 (U-5, U-6, E-3, S-3, C-3, D-2, D-3) |
| **Total** | **19** |

---

## Top 5 Actionable

### #1 — Fix the umbrella Quick-start and README API bugs (~45 minutes)

Three wrong examples across two files. None require design decisions — just matching
the actual API:

**`crates/noxu/src/lib.rs:25–27`** — change from `ignore` to `no_run`, fix both bugs:

```rust
// Before (inside ```ignore):
let db = env.open_database(None, "kv", true)?;
db.put(&txn, b"hello", b"world")?;

// After (inside ```no_run):
let db_cfg = DatabaseConfig::new().with_allow_create(true).with_transactional(true);
let db = env.open_database(None, "kv", &db_cfg)?;
db.put(Some(&txn), &DatabaseEntry::from_bytes(b"hello"), &DatabaseEntry::from_bytes(b"world"))?;
# Ok::<(), noxu::NoxuError>(())
```

**`README.md:68`** — change `db.get(None, &key, &mut result, None)?` to `db.get(None, &key, &mut result)?`.

### #2 — Add `crate=` escape hatch to the persist derive (~3–4 hours)

Following the `serde` pattern, allow users to override the generated crate path:

```rust
#[derive(Entity)]                                // generates ::noxu::persist:: paths (default)
struct User { … }

#[derive(Entity)]
#[entity(crate = "noxu_persist")]                // generates ::noxu_persist:: paths
struct Widget { … }
```

In the macro, read the `#[entity(crate = "…")]` attribute (defaulting to `::noxu::persist`)
and substitute it in every `quote!` site. Also update `noxu-persist/src/lib.rs` doc example to
use `use noxu::persist::` with a callout explaining why.

This unblocks users of `noxu-persist` directly (e.g., extension crates that re-wrap the DPL).

### #3 — Re-export `PreparedTxnInfo` / `PreparedLnReplay` (15 minutes)

In `crates/noxu-db/src/lib.rs`, add:

```rust
pub use noxu_recovery::{PreparedLnReplay, PreparedTxnInfo};
```

Until then, any user of the XA `xa_commit`/`xa_rollback` recovery path has to add
`noxu-recovery` to their `Cargo.toml` to name the return type.

### #4 — Add `'txn` lifetime to `DbIter` / `DbRange` (~1 hour)

```rust
pub struct DbIter<'txn> {
    cursor: Cursor,
    started: bool,
    done: bool,
    _txn: std::marker::PhantomData<&'txn crate::transaction::Transaction>,
}

impl Database {
    pub fn iter<'txn>(
        &self,
        txn: Option<&'txn Transaction>,
    ) -> Result<DbIter<'txn>> { … }
}
```

This is a breaking change (adds a lifetime parameter) but it makes commit-while-iterating
a compiler error rather than a runtime error. Given that v3.0.0 is the API stability
boundary, doing this now (before anyone depends on the signature) is far less painful
than doing it later.

### #5 — Add `rust-version = "1.85"` and three workspace lint rules (~10 minutes)

```toml
[workspace.package]
rust-version = "1.85"   # add this

[workspace.lints.rust]
warnings = "deny"                    # add this
unsafe_op_in_unsafe_fn = "deny"     # add this

[workspace.lints.clippy]
or_fun_call = "warn"                 # existing
redundant_clone = "warn"             # existing
large_stack_frames = "warn"          # existing
large_types_passed_by_value = "warn" # existing
undocumented_unsafe_blocks = "warn"  # add this
```

These two changes (MSRV + three lint lines) require zero code changes but prevent whole
classes of future issues from accumulating silently.

---

## Elevator Pitch (updated for v3.0.2)

### Why Noxu?

**`noxu = "3"` is now the single dependency** — the umbrella works, the engine is sound
(WAL ordering fixed, `unsafe` inventory accurate), the 12 core crates are compiler-enforced
safe, and `Database::iter()`/`range()` give you lazy, composable iteration without
materializing the database into memory. The `#[derive(Entity)]` story works correctly when
you use the umbrella. For teams that need ACID + replication + XA in a single embedded
Rust library, there is nothing else.

### Why Not Yet?

1. The first example a user sees (umbrella Quick-start) doesn't compile.
2. `#[derive(Entity)]` has a packaging trap that breaks direct `noxu-persist` users.
3. The core `Database` API is still Java-shaped; `OperationStatus` and `DatabaseEntry`
   out-params are locked in at v3.0.
4. `StoredMap::iter()` OOMs silently on large collections (now documented, still unfixed).
5. `noxu-xa` panics permanently on mutex poison; inconsistent Mutex choice throughout the
   new XA and cleaner registry code.

**Single biggest remaining barrier**: The umbrella Quick-start example is wrong.
"First impression" code that doesn't compile is a trust-breaker disproportionate to
the effort required to fix it (~45 minutes). Fix U-1 and U-2 and the "first contact"
experience becomes credible.

---

*Path: `/tmp/noxu-reaudit-jonhoo.md`*
