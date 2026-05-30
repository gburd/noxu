# Transaction / Environment / Sequence public-API audit — 2026-05

**Auditor:** read-only audit by automated agent
**Date:** 2026-05-25
**Branch:** `fix/cursor-search-gte-cross-bin-walk`
**Scope:** Public surface of `Environment`, `Transaction`, `Sequence`, and
their `*Config` / `*Stats` types in `noxu-db`, cross-referenced against
the `EnvironmentImpl` / `Txn` implementations in `noxu-dbi` /
`noxu-txn` and the published mdBook chapters.

This is a read-only paper audit — no source, configs, or tests were
modified, and no tests were executed. All findings derive from reading
the listed files.

---

## 1. Scope

### Audited

Public API surface in `noxu-db`:

* `crates/noxu-db/src/environment.rs` (1545 lines)
* `crates/noxu-db/src/environment_config.rs` (2220 lines)
* `crates/noxu-db/src/environment_mutable_config.rs` (120 lines)
* `crates/noxu-db/src/transaction.rs` (705 lines)
* `crates/noxu-db/src/transaction_config.rs` (373 lines)
* `crates/noxu-db/src/sequence.rs` (776 lines)
* `crates/noxu-db/src/sequence_config.rs` (316 lines)
* `crates/noxu-db/src/sequence_stats.rs` (35 lines)
* `crates/noxu-db/src/durability.rs` (208 lines)
* `crates/noxu-db/src/checkpoint_config.rs` (58 lines)

Implementation cross-references:

* `crates/noxu-dbi/src/environment_impl.rs` (~1600 lines) —
  `open_database`, `remove_database`, `rename_database`,
  `truncate_database`, `begin_txn`, `run_checkpoint`, `close`,
  `log_txn_commit`, `log_txn_abort`.
* `crates/noxu-txn/src/txn.rs` —
  `commit_with_durability`, `abort`, `abort_collect_undo`,
  `release_all_locks`, `set_serializable_isolation`,
  `set_read_committed_isolation`, `set_lock_timeout`,
  `set_txn_timeout`, `set_no_wait`, `set_importunate`.
* `crates/noxu-db/src/database.rs` for the auto-commit fsync path
  (`auto_commit_sync`, `check_writable`).

Documentation:

* `docs/src/getting-started/environments.md`
* `docs/src/transactions/{basics,concurrency,durability,isolation,deadlocks,transaction-config,backup-recovery}.md`

Reference shape: BDB-JE (`Environment`, `Transaction`, `Sequence`,
`EnvironmentConfig`, `EnvironmentMutableConfig`, `TransactionConfig`,
`SequenceConfig`, `Durability`, `CheckpointConfig`).

### Explicitly **not** exercised

* No code was modified; no tests were run.
* I did not audit replication-side durability (`noxu-rep`); however I
  observed that `Durability::replica_sync` and `replica_ack` fields are
  declared but **never consumed** anywhere under
  `crates/noxu-db/`, `crates/noxu-dbi/`, or `crates/noxu-engine/`.
* I did not audit XA / `noxu-xa` interactions.
* I did not audit secondary-database transactional behaviour beyond
  noting that `Environment::open_secondary_database` does not exist
  (secondary DBs are opened via `SecondaryDatabase::open(env, primary,
  …)` instead).

---

## 2. Methodology

1. Enumerated every `pub fn` on `Environment`, `Transaction`,
   `Sequence`, and the `*Config` / `*Stats` types (using `grep "pub fn"`).
2. Read every public method body and the rustdoc above it in full.
3. For each method, walked into the `noxu-dbi` / `noxu-txn`
   implementation it delegates to.
4. Read the corresponding mdBook chapter end-to-end and compared the
   contract documented to the contract enforced by the code.
5. Cross-referenced against BDB-JE 7.5.11 method names (the project's
   stated reference) where the surface should match.
6. Categorised each divergence by severity:
   * **High** — silently wrong observable behaviour, ACID risk,
     resource leak, or memory-safety edge.
   * **Medium** — doc/impl mismatch, missing surface vs. JE that is
     implementable today, or behaviour visible to careful users.
   * **Low** — cosmetic, missing-but-equivalent surface, or stale
     comment.

---

## 3. Findings table

| ID | Severity | Area | One-liner |
|---|---|---|---|
| F1 | **High** | `Environment` lifecycle | `Environment::active_txns` is never pruned; `mark_transaction_complete` is dead code, so explicit `env.close()` after `txn.commit()` returns `OperationNotAllowed`. |
| F2 | **High** | `TransactionConfig::read_uncommitted` | The `read_uncommitted` flag set in `TransactionConfig` is silently ignored by `Environment::begin_transaction` (no `set_read_uncommitted` call on the inner `Txn`). |
| F3 | **High** | `EnvironmentConfig::durability` | The environment-default `durability` field is never read when committing an explicit transaction; every commit falls back to `Durability::COMMIT_SYNC` unless the per-`TransactionConfig` durability differs from the default. |
| F4 | **Medium** | `EnvironmentConfig::txn_no_sync` / `txn_write_no_sync` | These flags are honoured **only on the auto-commit path** (`Database::auto_commit_sync`), not on explicit `Transaction::commit()`. JE applies them globally. |
| F5 | **Medium** | `Environment::read_only` | `read_only=true` is *not* checked at the env layer for `open_database(allow_create=true)`, `remove_database`, `rename_database`, `truncate_database`, or `begin_transaction`; only `Database`-level writes are blocked. |
| F6 | **Medium** | `Environment::checkpoint` | `CheckpointConfig` is accepted but every field (`force`, `k_bytes`, `minutes`, `minimize_recovery_time`) is ignored — checkpoint is always run with the label `"manual"`. |
| F7 | **Medium** | `Environment::set_mutable_config` | The setter records new daemon-enable flags into `self.config` but never tells the running daemons; the inline comment confirms this is "advisory at runtime". `cache_size`, `lock_timeout_ms`, and `txn_timeout_ms` are likewise *recorded* but not pushed to the live evictor / lock manager / txn manager. |
| F8 | **Medium** | `Transaction::commit_with_durability` | `replica_sync` and `replica_ack` policies on `Durability` are never consumed even when the engine is built with replication; only `local_sync` controls fsync/flush. |
| F9 | **Medium** | `Transaction::commit_with_durability` (direct call) | When user code calls `commit_with_durability` directly (instead of via `commit()`), the `noxu_db_active_transactions` gauge is **not** decremented (the dec is in `commit()` only). |
| F10 | **Medium** | `Transaction` Drop | `Drop` only `log::warn!`s; it does **not** abort, does **not** decrement the active-transactions gauge, and does **not** release inner-`Txn` write locks or apply undo. Locks are leaked until the underlying `EnvironmentImpl` is dropped. |
| F11 | **Medium** | Nested transactions | `Environment::begin_transaction(_parent: Option<&Transaction>, …)` accepts a parent argument that is silently ignored. JE returns an error or creates a child txn; here a non-`None` parent is a no-op. Not documented. |
| F12 | **Medium** | Auto-commit transactional semantics | "Auto-commit" (`txn = None`) writes apply to the in-memory tree directly without acquiring per-record write locks via a `Txn`; only the WAL fsync is gated. Concurrent auto-commit + explicit-txn workloads can therefore observe non-isolated states even on a transactional environment. |
| F13 | **Low** | `Environment` missing JE methods | No `Environment::sync()`, `Environment::flush_log()`, `Environment::open_secondary_database()`, `Environment::compress()`, `Environment::evict_memory()`, `Environment::clean_log()`. (`Database::sync()` exists; secondaries open via `SecondaryDatabase::open`.) |
| F14 | **Low** | `Transaction` missing JE methods | No `commit_sync()`, `commit_no_sync()`, `commit_write_no_sync()` convenience wrappers, no `set_name()` / `get_name()`. Functionally equivalent via `commit_with_durability`. |
| F15 | **Low** | `Sequence` missing JE methods | No `get_database()`, `get_key()`, `get_config()` accessors. JE exposes them. |
| F16 | **Low** | `SequenceConfig` default `cache_size` | Default is **20**, but JE's default is **0** (no caching). The change is intentional per the inline comment but is not advertised in `docs/src/`. |
| F17 | **Low** | `TransactionConfig` defaults vs docs | Docs claim defaults are "serializable, env lock timeout, full durability"; the struct defaults to all three isolation flags `false` (= repeatable read, NOT serializable) and `lock_timeout_ms = 0` (= env default). The "serializable default" wording in `transaction-config.md` is wrong. |
| F18 | **Low** | `Transaction` Drop & state | `Drop` checks `state` while inner `state.lock().unwrap()` could panic on a poisoned mutex; recoverable but inconsistent with the project's "use `.expect("invariant: …")` rule" guideline. |
| F19 | **Low** | `Sequence` `n_cache_hits` semantics | The accounting increments `n_cache_hits` whenever `!need_refill`, including the trivial "delta == 1, fresh batch" hit — same as JE, but the doc for `SequenceStats::n_cache_hits` should clarify that the *first* `get` after a refill is also counted as a cache hit. |
| F20 | **Low** | `Sequence::close` is a no-op | `Sequence::close()` returns `Ok(())` unconditionally and the rustdoc admits "Nothing to flush". A second call after `close()` is therefore not detected; subsequent `get()` continues to work. JE's `Sequence.close()` *invalidates* the handle. |
| F21 | **Low** | `Environment::open` write-test side effect | `open()` writes a `.noxu_write_test` file in the home directory whenever `read_only == false`. On a read-write env that races with another process this is benign, but the file is written *before* recovery and *before* `EnvironmentImpl::new()` validates the home, so a partial open leaves a stray file behind. |
| F22 | **Low** | Recovery surface | `Environment::open` runs recovery (via `EnvironmentImpl::from_dbi_config`) but does not document the failure mode: a corrupt log produces `NoxuError::environment(...)` rather than a typed `RecoveryFailure` variant. The mdBook docs claim "no application action is required" but never mention what error type is returned on recovery failure. |
| F23 | **Low** | Deadlock detection — doc claim vs. config | `docs/src/transactions/deadlocks.md` says deadlock is reported via `NoxuError::DeadlockDetected` *or* `NoxuError::LockConflict`, but the env config field `lock_deadlock_detect` (default `true`) and `lock_deadlock_detect_delay_ms` are not surfaced in any doc page. |
| F24 | **Low** | `EnvironmentMutableConfig` "0 means unchanged" sentinel | The "0 = unchanged" rule for `lock_timeout_ms` / `txn_timeout_ms` makes it impossible to *clear* a timeout (e.g. set txn_timeout to "no timeout") via `set_mutable_config`. JE uses `null`/`Long.MAX_VALUE` sentinels. |
| F25 | **Info** | No `Sequence` removal API | Neither `Database::remove_sequence()` nor `Environment::remove_sequence()` exists. JE exposes `Database.removeSequence(Transaction, DatabaseEntry)`. Today the only way to remove a sequence is to delete its key with `Database::delete`. |

---

## 4. Detailed findings

### F1 (High) — `Environment::active_txns` is never pruned; explicit `env.close()` after `txn.commit()` fails

**Location:**
* `crates/noxu-db/src/environment.rs:50-51` — declaration
  ```rust
  /// Active transactions
  active_txns: Mutex<HashMap<u64, Arc<TransactionState>>>,
  ```
* `crates/noxu-db/src/environment.rs:617-623` — insertion in
  `begin_transaction`:
  ```rust
  let mut active_txns = self.active_txns.lock();
  active_txns.insert(txn_id, txn_state);
  ```
* `crates/noxu-db/src/environment.rs:920-929` — removal:
  ```rust
  pub(crate) fn mark_transaction_complete(&self, txn_id: u64) {
      let mut active_txns = self.active_txns.lock();
      active_txns.remove(&txn_id);
  }
  ```
* `crates/noxu-db/src/environment.rs:355-362` — close logic:
  ```rust
  let active_txns = self.active_txns.lock();
  if !active_txns.is_empty() {
      return Err(NoxuError::OperationNotAllowed(format!(
          "Cannot close environment with {} active transactions",
          active_txns.len()
      )));
  }
  ```

**Symptom.** The only call site of `mark_transaction_complete` is in
the unit test `test_mark_transaction_complete_allows_env_close`
(`environment.rs:1369-1382`), which explicitly says *"Without removing
the txn, close would fail."* — i.e. the test acknowledges the bug
exists in the production path.

`Transaction::commit` and `Transaction::abort`
(`crates/noxu-db/src/transaction.rs:180-296` and `:308-379`) never
call back into the `Environment`. The `Transaction` struct only holds
`env_impl: Option<Arc<SyncMutex<EnvironmentImpl>>>` (used for undo
application), not an `Arc<Environment>`.

**Why it has not been caught in CI.** Every test in
`crates/noxu-db/tests/sorted_dup_test.rs` writes `let _ = env.close();`,
swallowing the error. Tests that exercise `txn.commit()` rely on `Drop`
(`Environment::drop` on `environment.rs:946-951` ignores the close
error). End users following the published examples in
`docs/src/transactions/basics.md`, which all chain `db.close()?` →
`env.close()?`, will receive `OperationNotAllowed("Cannot close
environment with N active transactions")` whenever they begin and
commit a transaction.

**Suggested fix:** make `Transaction::commit` /
`Transaction::commit_with_durability` / `Transaction::abort` notify
the environment by holding a weak/strong reference back to it (or
restructure ownership so the truth-of-record for "active txns" lives
in `EnvironmentImpl::txn_manager`, which is already kept in sync via
`begin_txn` / inner-`Txn::commit`).

---

### F2 (High) — `TransactionConfig::read_uncommitted` is silently dropped

**Location:** `crates/noxu-db/src/environment.rs:634-659`

```rust
let inner_txn = env_guard
    .begin_txn()
    .map(|mut t| {
        if txn_config.read_committed {
            t.set_read_committed_isolation(true);
        }
        if txn_config.serializable_isolation {
            t.set_serializable_isolation(true);
        }
        if txn_config.importunate { t.set_importunate(true); }
        if txn_config.no_wait { t.set_no_wait(true); }
        if txn_config.lock_timeout_ms > 0 { … }
        if txn_config.txn_timeout_ms > 0 { … }
        Arc::new(std::sync::Mutex::new(t))
    })
```

There is **no branch** for `txn_config.read_uncommitted`. The inner
`Txn` keeps `read_uncommitted_default = false`
(`crates/noxu-txn/src/txn.rs:200`). The published documentation in
`docs/src/transactions/isolation.md` (the `with_read_uncommitted(true)`
example) shows users explicitly setting this flag and expecting dirty
reads. The flag is propagated nowhere.

The per-operation `LockMode::ReadUncommitted` path (the alternative
documented at `isolation.md:75-83`) is unaffected — that flows through
the cursor's lock-mode argument, not through `Txn`. So users who set
the txn-level flag silently get repeatable-read.

**Suggested fix:** add `if txn_config.read_uncommitted { … }`. Today
`Txn` has no public `set_read_uncommitted_default` method; it would
need to be added (the field exists at
`crates/noxu-txn/src/txn.rs:115`).

---

### F3 (High) — `EnvironmentConfig::durability` is dead config

**Location:**
* Declaration: `crates/noxu-db/src/environment_config.rs:557-559`
  ```rust
  /// Default durability policy for transactions.
  /// : `TXN_DURABILITY`.
  pub durability: Durability,
  ```
* Default initialisation: `environment_config.rs:859`
* Builder: `environment_config.rs:1498-1504`
* Total uses in commit path:
  ```text
  $ grep -n "config\.durability\|self\.config\.durability" \
       crates/noxu-db/src/environment.rs
  (no matches)
  ```

`Transaction::with_log_manager` and `Transaction::new`
(`transaction.rs:94-129`) initialise the per-txn `durability` field
from `config.durability` only — i.e. from the **`TransactionConfig`**,
not from the `EnvironmentConfig`. If the user calls
`begin_transaction(None, None)` the config is `TransactionConfig::default()`,
whose `durability` is `Durability::default() == COMMIT_SYNC`.
The env-level `EnvironmentConfig::durability` is therefore never
consulted.

`docs/src/transactions/basics.md` claims:

> Set a default durability of WRITE_NO_SYNC for the entire environment.
> ```rust
> EnvironmentConfig::new(home).with_durability(no_sync)
> ```

This example is incorrect — every `begin_transaction(None, None)` will
still fsync.

**Suggested fix:** `Environment::begin_transaction` must merge the
env-level `Durability` into a default `TransactionConfig` when the
caller passes `None` and the per-txn default has not been overridden.

---

### F4 (Medium) — `txn_no_sync` / `txn_write_no_sync` apply only to auto-commit

**Location:**
* `crates/noxu-db/src/environment.rs:455-465` — values are forwarded
  into `Database::new(no_sync, write_no_sync)` only.
* `crates/noxu-db/src/database.rs:160-194` — `auto_commit_sync`
  branches on `self.no_sync` / `self.write_no_sync`.
* `crates/noxu-db/src/transaction.rs:206-225` — explicit-txn commit
  branches **only** on `durability.local_sync`. There is no read of
  the env's `txn_no_sync` / `txn_write_no_sync`.

JE behaviour: `EnvironmentConfig.setTxnNoSync(true)` makes **all**
commits non-sync, whether issued via `Transaction.commit()` or via
auto-commit. Combined with F3 above, an explicit transaction in a
"non-durable" environment will still fsync.

---

### F5 (Medium) — `Environment::read_only` is not enforced at the env layer

**Location:**
* `crates/noxu-db/src/environment.rs:115-141` — `open()` checks the
  on-disk directory is *writable* unless `read_only`, but never stores
  a "no-write" guard for later operations.
* `crates/noxu-db/src/environment.rs:384-468` (`open_database`) —
  forwards `config.allow_create` to `EnvironmentImpl::open_database`
  without checking `self.config.read_only`. A read-only env that
  receives `DatabaseConfig::new().with_allow_create(true)` will create
  the database in the in-memory map and (eventually) write a log
  entry once the WAL is wired (today the WAL is not wired for
  read-only envs, so the create is silently in-memory only —
  see `crates/noxu-dbi/src/environment_impl.rs:291` "log_manager =
  None for read-only").
* `crates/noxu-db/src/environment.rs:480-509` (`remove_database`),
  `:511-528` (`truncate_database`), `:544-580` (`rename_database`)
  — none check `self.config.read_only`.
* `crates/noxu-db/src/environment.rs:600-614` (`begin_transaction`) —
  allows beginning a transaction on a read-only env.

The only enforcement is at the *Database* level
(`database.rs:951-955`, `check_writable`). This means a `read_only`
environment can still mutate its name-map (with no WAL backing) and
hand out transaction handles that will silently no-op on commit
(`EnvironmentImpl::log_txn_commit` returns `Ok(())` when
`log_manager` is `None`, see `environment_impl.rs:1016-1042`).

The user-facing docs (`docs/src/getting-started/environments.md`
"Read-Only Environments") promise *"No write operations are
permitted."* — this is materially overstated.

---

### F6 (Medium) — `CheckpointConfig` is accepted but ignored

**Location:** `crates/noxu-db/src/environment.rs:773-779`

```rust
pub fn checkpoint(&self, _config: Option<&CheckpointConfig>) -> Result<()> {
    self.check_open()?;
    let env_impl = self.env_impl.lock();
    env_impl.run_checkpoint()
        .map_err(|e| NoxuError::environment(e.to_string()))
}
```

The argument name is `_config` (intentionally ignored). The downstream
`EnvironmentImpl::run_checkpoint` (`environment_impl.rs:973-989`) calls
`ckpt.do_checkpoint("manual")` with no parameters. Therefore
`CheckpointConfig::force`, `k_bytes`, `minutes`, and
`minimize_recovery_time` are dead fields.

The published API in `crates/noxu-db/src/checkpoint_config.rs` documents
each field with non-trivial semantics. They should either be plumbed
through or removed.

---

### F7 (Medium) — `set_mutable_config` is largely advisory

**Location:** `crates/noxu-db/src/environment.rs:731-770`

```rust
pub fn set_mutable_config(&mut self, cfg: EnvironmentMutableConfig) -> Result<()> {
    …
    if let Some(sz) = cfg.cache_size { self.config.cache_size = sz as u64; }
    if cfg.lock_timeout_ms > 0 { self.config.lock_timeout_ms = cfg.lock_timeout_ms; }
    if cfg.txn_timeout_ms > 0 { self.config.txn_timeout_ms = cfg.txn_timeout_ms; }
    self.config.txn_no_sync = cfg.txn_no_sync;
    self.config.txn_write_no_sync = cfg.txn_write_no_sync;
    // Daemon enable/disable flags are advisory at runtime; …
    if let Some(v) = cfg.run_cleaner { self.config.run_cleaner = v; }
    if let Some(v) = cfg.run_checkpointer { self.config.run_checkpointer = v; }
    if let Some(v) = cfg.run_evictor { self.config.run_evictor = v; }
    Ok(())
}
```

Every assignment writes only to `self.config` — the in-memory
`EnvironmentConfig`. Nothing is pushed to:

* `EnvironmentImpl.evictor.set_target_bytes(…)` (cache size)
* `EnvironmentImpl.lock_manager.set_default_timeout(…)` (lock timeout)
* `EnvironmentImpl.txn_manager.set_default_timeout(…)` (txn timeout)
* The daemon shutdown/start switches.

Once an `Environment` is open, these knobs cannot actually be changed
at runtime. The inline comment is honest about the daemons; for
`cache_size` and the timeouts the docstring claims more than the code
does. Note also that `cfg.durability` is declared on the struct
(`environment_mutable_config.rs:33`) but is never copied into
`self.config.durability` — combined with F3, that path has no effect
either way.

---

### F8 (Medium) — `Durability::replica_sync` and `replica_ack` are not wired

**Location:**
* `crates/noxu-db/src/durability.rs:60-99` — fields and named constants.
* `crates/noxu-db/src/transaction.rs:213-220` —
  ```rust
  let (fsync, flush) = match durability.local_sync {
      SyncPolicy::Sync => (true, true),
      SyncPolicy::WriteNoSync => (false, true),
      SyncPolicy::NoSync => (false, false),
  };
  ```
  No reference to `durability.replica_sync` or `durability.replica_ack`.
* Repository-wide consumption:
  ```text
  $ grep -rn "replica_sync\|replica_ack" crates/noxu-db crates/noxu-dbi crates/noxu-engine
  (only struct declarations and tests in noxu-db/src/durability.rs)
  ```

In a single-node / non-replicated build this is moot. In a build with
`noxu-rep` enabled the replica sides of the policy may still not be
consulted from the public-API path (the actual replica wait path is in
`noxu-rep`; this audit did not verify whether `noxu-rep` re-consults
the user-supplied `Durability` or substitutes its own policy).

---

### F9 (Medium) — `commit_with_durability` skips the active-transactions gauge dec

**Location:**
* `crates/noxu-db/src/transaction.rs:180-189` — `commit()` calls
  `commit_with_durability(durability)` then unconditionally
  `observe_gauge_dec!("noxu_db_active_transactions");`
* `crates/noxu-db/src/transaction.rs:206-296` — `commit_with_durability`
  itself does **not** call `observe_gauge_dec!`.
* `crates/noxu-db/src/transaction.rs:374-376` — `abort()` does call it.

Both `commit` and `commit_with_durability` are public, so a user
calling `txn.commit_with_durability(Durability::COMMIT_NO_SYNC)`
directly will leak the metric. The increment side
(`new`/`with_log_manager`) is unconditional.

---

### F10 (Medium) — `Drop` does not abort, release locks, or decrement the gauge

**Location:** `crates/noxu-db/src/transaction.rs:493-505`

```rust
impl Drop for Transaction {
    fn drop(&mut self) {
        let state = *self.state.lock().unwrap();
        if matches!(state, TransactionState::Open | TransactionState::MustAbort) {
            log::warn!(
                "Transaction {} dropped without commit or abort, implicitly aborting",
                self.id
            );
        }
    }
}
```

The log message says *"implicitly aborting"*, but no abort is
performed:

* The inner `Txn` (`self.inner_txn`) is not aborted, so its read /
  write locks remain registered with the lock manager until the
  containing `EnvironmentImpl` is dropped.
* The active-transactions gauge is not decremented (counterpart
  to F9).
* No `TxnAbort` WAL entry is written; recovery will treat the
  transaction as in-doubt until the next checkpoint.
* `Environment::active_txns` (see F1) is not pruned either, so this
  compounds F1.

JE's `Transaction.finalize()` calls `abort()` if the txn is still open.

---

### F11 (Medium) — Nested transactions silently dropped

**Location:** `crates/noxu-db/src/environment.rs:600-614`

```rust
pub fn begin_transaction(
    &self,
    _parent: Option<&Transaction>,
    config: Option<&TransactionConfig>,
) -> Result<Transaction> {
    …
}
```

The `parent` argument has the leading-underscore convention indicating
it is intentionally unused. JE's `Environment.beginTransaction(Transaction
parent, TransactionConfig)` either creates a child transaction (when
the env supports nested txns) or throws `IllegalArgumentException`.
Noxu silently produces a top-level transaction regardless of `parent`.

The mdBook (`docs/src/transactions/basics.md` line ~166) tells the user
*"Pass `None` for the parent unless you want a nested (child)
transaction."* — but passing `Some(&parent)` does **not** produce a
nested transaction.

---

### F12 (Medium) — Auto-commit does not acquire per-record locks

**Location:**
* `crates/noxu-db/src/database.rs:160-194` — `auto_commit_sync`
  is the *entire* "auto-commit" surface; it only gates the WAL fsync.
* `crates/noxu-db/src/database.rs:455-635` — `put` / `put_no_overwrite`
  / `delete` apply directly to the in-memory tree before
  `auto_commit_sync` is called; only the explicit-txn cursor path
  (`make_cursor_for_txn`) plumbs an inner `Txn` for record locking.

Effect: when a thread issues `db.put(None, k, v)` concurrently with an
explicit transaction `txn` that holds a read or write lock on `k`, the
auto-commit write **does not consult the lock manager**. It bypasses
isolation entirely.

This is consistent with the current cleaner / write-path design (the
data path mutates the tree directly; the lock manager is only used by
explicit transactions). However the published guarantee in
`docs/src/transactions/basics.md` ("All ACID … Isolation …") is
claimed for the entire "Transactions" chapter, with auto-commit
documented as just "single-write convenience". Users who interleave
auto-commit and explicit txns will see ACID violations.

The explicit warning in `basics.md` ("Never have more than one active
transaction in your thread at a time. Mixing an explicit transaction
with an auto-commit operation in the same thread can result in
undetectable deadlocks.") is correct in spirit but understates the
scope: cross-*thread* interleaving has the same problem.

---

### F13 (Low) — Missing JE `Environment` methods

`Environment::sync()`, `Environment::flush_log()`,
`Environment::open_secondary_database()`,
`Environment::compress()`, `Environment::evict_memory()`,
`Environment::clean_log()` are absent. Workarounds today:

* `Database::sync()` exists (`database.rs:802`) — flushes the log
  manager.
* Secondary databases are opened by free function
  `SecondaryDatabase::open(env, primary_arc, name, config)`
  (`secondary_database.rs:84`) rather than via the env handle.

These are surface gaps, not correctness bugs, but they break the
"familiar to BDB-JE users" promise in the crate-root rustdoc.

---

### F14 (Low) — Missing JE `Transaction` convenience methods

JE exposes `commitSync()`, `commitNoSync()`, `commitWriteNoSync()` and
`setName()` / `getName()`. Noxu only has `commit()` and
`commit_with_durability(Durability)`. Trivial to add.

---

### F15 (Low) — Missing `Sequence` accessors

JE's `Sequence` exposes `getDatabase()`, `getKey(DatabaseEntry)`,
`getConfig()`. Noxu's `Sequence` (`crates/noxu-db/src/sequence.rs:76-84`)
exposes only `get`, `get_stats`, `close`. The `Database` reference and
`key` are stored in the struct (lines 95-99) but not surfaced.

---

### F16 (Low) — `SequenceConfig::cache_size` default is **20**, not 0

**Location:** `crates/noxu-db/src/sequence_config.rs:11-13, 38-49`

```rust
/// Number of elements cached in the sequence handle (default 20).
///
/// default is 0 but the task specifies 20 as the noxu default for
/// pre-fetching.
pub cache_size: i32,
```

The deviation is intentional and documented in source, but the user-facing
mdBook does not mention sequences at all. A user porting from BDB-JE
expecting `cache_size = 0` (i.e. every `get()` hits the database) will
observe sequence values jumping forward by 20 per process restart.

---

### F17 (Low) — Doc claim "Default isolation: Serializable" is wrong

**Location:** `docs/src/transactions/transaction-config.md`

```text
| Serializable (default) | with_serializable_isolation(true) | …
```

and

```text
If you pass `None` for the config, the transaction uses defaults
(serializable reads, environment lock timeout, full durability).
```

But the actual default in `crates/noxu-db/src/transaction_config.rs:50-62`
is `read_committed = false`, `read_uncommitted = false`,
`serializable_isolation = false` — i.e. **repeatable read**, the JE
default and what `docs/src/transactions/isolation.md` correctly
documents in its level table. The two doc pages contradict each other.

---

### F18 (Low) — `Drop` may panic on poisoned mutex

`crates/noxu-db/src/transaction.rs:495` — `let state = *self.state.lock().unwrap();` inside `Drop`. A panic in `Drop` is
double-panic territory. The project guideline (`AGENTS.md`)
permits `unwrap()` on lock acquisition because mutex poisoning is
considered fatal, so this is policy-compliant; flagged for visibility.

---

### F19 (Low) — `Sequence::n_cache_hits` accounting

`crates/noxu-db/src/sequence.rs:415-417`:

```rust
state.n_gets += 1;
if !need_refill {
    state.n_cache_hits += 1;
}
```

A `get()` that triggers a refill does *not* count as a cache hit;
a `get()` immediately after the refill (which serves from the just-loaded
batch) **does**. For very-cold-handle workloads this overstates the hit
rate vs. JE, where `nCacheHits` only counts hits that *avoided* a DB
read. Verify against the JE source the project archives at `_/je/src`.

---

### F20 (Low) — `Sequence::close` is a no-op; handle remains usable

`crates/noxu-db/src/sequence.rs:476-484`:

```rust
pub fn close(&self) -> Result<()> {
    Ok(())
}
```

JE's `Sequence.close()` invalidates the handle. In Noxu, a `Sequence`
held after `close()` continues to function (still serves cached
values, still writes to the DB on refill). For a correctness audit the
risk is low — there is no resource leak — but the rustdoc on
`close()` says *"After calling this method the handle must not be used
again"*, which the implementation does not enforce.

---

### F21 (Low) — `Environment::open` write-test side effect

**Location:** `crates/noxu-db/src/environment.rs:135-145`

```rust
let test_file = home.join(".noxu_write_test");
std::fs::write(&test_file, b"test").map_err(|e| { … })?;
let _ = std::fs::remove_file(&test_file);
```

This runs **before** `EnvironmentImpl::from_dbi_config`, i.e. before
recovery. If the directory contains existing `.ndb` files but recovery
later fails (returning an error from `open()`), the user is left with
a stray `.noxu_write_test` file (the `let _ = … remove_file` succeeds
in the happy path; if write succeeded but a panic before remove is
hit, the file stays). Cosmetic.

---

### F22 (Low) — Recovery failure typing

`Environment::open` (`environment.rs:110-296`) maps every error from
`EnvironmentImpl::from_dbi_config` to `NoxuError::environment(e.to_string())`.
There is no typed `RecoveryFailure` variant — even though
`EnvironmentFailureReason::LogChecksum` /
`EnvironmentFailureReason::BtreeCorruption` exist
(`crates/noxu-db/src/error.rs`).

`docs/src/transactions/backup-recovery.md` "Normal Recovery" promises
that *"This is run automatically every time a Noxu DB environment is
opened; no application action is required"* but never tells the user
how to distinguish a recovery failure from any other env-open failure.

---

### F23 (Low) — Deadlock detection knobs not surfaced in docs

`crates/noxu-db/src/environment_config.rs:541-548`:

```rust
pub lock_deadlock_detect: bool,            // default true
pub lock_deadlock_detect_delay_ms: u64,    // default 0
pub txn_deadlock_stack_trace: bool,
pub txn_dump_locks: bool,
```

None of these appear in `docs/src/transactions/deadlocks.md`,
`docs/src/transactions/concurrency.md`, or
`docs/src/getting-started/environments.md`. Users who want to disable
deadlock detection (e.g. for ordered-lock workloads) cannot find the
flag from the docs.

---

### F24 (Low) — "0 means unchanged" sentinel is too broad

`crates/noxu-db/src/environment_mutable_config.rs:54-58`,
`crates/noxu-db/src/environment.rs:739-743`:

`lock_timeout_ms = 0` and `txn_timeout_ms = 0` mean "use env default" or
"no timeout" depending on context, **and** they mean "do not change the
running value" in `set_mutable_config`. A user cannot use
`set_mutable_config` to express "switch this transaction timeout to
zero (no timeout)" — the change is silently dropped.

JE uses `Long.MAX_VALUE` (or null) for "no timeout", separating the
two meanings.

---

### F25 (Info) — No `remove_sequence` API

JE: `Database.removeSequence(Transaction, DatabaseEntry)`.

Noxu: `Database` exposes `open_sequence` only. To remove a sequence the
user must call `db.delete(None, &key)`. This works but is undocumented.

---

## 5. Coverage gaps

* **Replication-side durability path** was not exercised. The dead
  `replica_sync` / `replica_ack` fields (F8) need a follow-up audit
  inside `crates/noxu-rep/`.
* **Concurrent auto-commit + explicit-txn isolation** (F12) was not
  reproduced; finding is from code reading. A property-based test
  (`hegel`) that interleaves both write paths against the same key
  would either confirm or refute the isolation gap.
* **Recovery failure surface** (F22) was not tested — no contrived
  log-corruption test was constructed. The audit only checked which
  error variants the public API emits.
* **`Environment::close()` after explicit txn** (F1) was not run; the
  conclusion is from the code path and the existence of the
  `test_mark_transaction_complete_allows_env_close` "test as
  documentation" pattern.
* **`Drop` ordering** of `Transaction` vs `Environment` was not
  explored. F10's "lock leak until env drop" claim should be confirmed
  with a `drop` order test.
* **JE `Transaction.setName` / lock-stat reporting** (F14) was not
  audited for downstream consumers (`noxu-engine::env_stats` exposes
  `LockStatsSnapshot` but not by-txn breakdowns).
* **Sequence overflow in decrement+wrap mode** was code-read only; the
  inline comments in `sequence.rs:300-374` walk through all four
  edge-cases (incr/decr × wrap/no-wrap) but no targeted test was found
  for the i64::MIN / i64::MAX boundary cases.

---

## 6. Summary

The transaction / environment / sequence surface is *visibly*
JE-shaped — every public name, the `*Config` builder pattern, the
`Durability` constants, the `Sequence` 26-byte record format — but
several **wiring** issues mean the published behaviour diverges from
the docs and from the BDB-JE reference:

* **F1 / F3 / F2** are the three findings that affect every
  transactional user: explicit `env.close()` after a committed
  transaction errors, env-level durability is ignored, and
  per-transaction read-uncommitted is silently dropped. None of these
  are visible in the existing test suite because the tests either
  ignore the `env.close()` error or never exercise the configuration
  path.
* **F4 / F5 / F6 / F7** are wiring gaps for env-level configuration:
  several `EnvironmentConfig` fields are accepted but consumed only
  partially (auto-commit only, in-memory only, or not at all).
* **F8 / F11 / F12** are doc-vs-impl mismatches whose blast radius
  depends on whether users believe the docs over the code. F12 in
  particular is a real isolation hole that should be either fixed or
  *prominently* documented (a single-line warning in `basics.md` is not
  enough).
* **F9 / F10** are smaller bugs around `commit_with_durability` /
  `Drop` that should be cleaned up to keep observability and
  lock-manager state consistent.
* The remaining **F13–F25** are missing-but-equivalent surface, doc
  bugs, or cosmetic issues.

There are **no panics, unwraps on user input, or `unsafe` blocks** in
the audited files; the project's "no `unsafe`" guarantee for these
crates holds. All `.unwrap()` calls observed are on `Mutex::lock()`
results, which the project policy permits.

The single most important follow-up is to make `Transaction::commit` /
`Transaction::abort` notify the owning `Environment` so that
`active_txns` is pruned (F1). Once that is in place, the
durability-default and read-uncommitted plumbing fixes (F3, F2) are
small mechanical changes.
