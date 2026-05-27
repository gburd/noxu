# Transaction Configuration

`TransactionConfig` controls per-transaction behavior: isolation level, lock
handling, timeouts, and durability. It uses a builder pattern and is passed to
`env.begin_transaction()`.

```rust
use noxu_db::TransactionConfig;

let config = TransactionConfig::new()
    .with_read_committed(true)
    .with_lock_timeout_ms(500);

let txn = env.begin_transaction(Some(&config))?;
```

If you pass `None` for the config, the transaction uses defaults (serializable
reads, environment lock timeout, full durability).

---

## Isolation Levels

Noxu DB supports three isolation levels. Only one can be active per transaction;
setting one clears the others.

| Level | Method | Reads see | Locks held | Use when |
|-------|--------|-----------|------------|----------|
| Serializable (default) | `with_serializable_isolation(true)` | Only committed data; read set is stable | Read + write locks held until commit | Correctness is paramount |
| Read Committed | `with_read_committed(true)` | Only committed data, but re-reads may differ | Read locks released after each operation | Long-running reads that must not block writers |
| Read Uncommitted | `with_read_uncommitted(true)` | Dirty (uncommitted) data from other transactions | No read locks acquired | Approximate counts, monitoring, non-critical scans |

**Serializable** (the default) prevents phantom reads — if you read a range
twice within the same transaction, you get the same results. The cost is that
read locks are retained until commit, increasing contention.

**Read Committed** releases read locks immediately after each get/cursor
operation. Another transaction's commit between two reads of the same key may
produce different values (non-repeatable read). This is the best choice for
long-lived reader transactions that would otherwise block writers.

**Read Uncommitted** acquires no read locks at all, so it never blocks and never
causes deadlocks. The trade-off is that you may observe partial or rolled-back
writes. Use this only when approximate results are acceptable.

```rust
// Long-running analytics query that should not block writers
let config = TransactionConfig::new().with_read_committed(true);
let txn = env.begin_transaction(Some(&config))?;
// ... scan large ranges without holding locks ...
txn.commit()?;
```

---

## Lock Management

These options control what happens when a transaction cannot immediately acquire
a lock.

| Option | Method | Behavior |
|--------|--------|----------|
| Lock timeout | `with_lock_timeout_ms(ms)` | Wait up to `ms` milliseconds for a lock; 0 means use the environment default |
| No wait | `with_no_wait(true)` | Fail immediately with `LockNotAvailable` if any lock is held by another transaction |
| Importunate | `with_importunate(true)` | Steal locks from other transactions rather than waiting (the victim is forced to abort) |

### Lock timeout

Set a per-transaction lock timeout to bound how long a transaction will wait
before returning a `LockConflict` error. This is useful for latency-sensitive
operations.

```rust
let config = TransactionConfig::new().with_lock_timeout_ms(100);
let txn = env.begin_transaction(Some(&config))?;

match db.put(Some(&txn), &key, &val) {
    Ok(_) => txn.commit()?,
    Err(NoxuError::LockConflict(_)) => {
        txn.abort()?;
        // handle timeout — retry or return error to caller
    }
    Err(e) => { txn.abort()?; return Err(e.into()); }
}
```

### No wait

`no_wait` is the extreme case of a zero-millisecond timeout. The transaction
fails instantly on any lock conflict. This is ideal for try-lock patterns where
you would rather skip an operation than wait.

```rust
let config = TransactionConfig::new().with_no_wait(true);
let txn = env.begin_transaction(Some(&config))?;
// If any key is already write-locked, this returns LockNotAvailable.
db.put(Some(&txn), &key, &val)?;
txn.commit()?;
```

### Importunate

An importunate transaction never waits — it forcibly takes locks from other
holders, causing those victims to receive a `LockConflict` error on their next
operation. Use this sparingly, typically for administrative or high-priority
repair operations.

```rust
// Administrative cleanup that must not be blocked
let config = TransactionConfig::new().with_importunate(true);
let txn = env.begin_transaction(Some(&config))?;
db.delete(Some(&txn), &stale_key)?;
txn.commit()?;
```

---

## Transaction Boundaries

| Option | Method | Purpose |
|--------|--------|---------|
| Read only | `with_read_only(true)` | Declare the transaction will perform no writes; enables internal optimizations |
| Transaction timeout | `with_txn_timeout_ms(ms)` | Abort the transaction if it runs longer than `ms` milliseconds (0 = no limit) |
| Local write | `with_local_write(true)` | Writes stay local to this replica and are not replicated |

**Read only** transactions skip write-lock bookkeeping and undo-log allocation.
They are cheaper to commit and never contribute to deadlocks. Use them whenever
you know a transaction will only read.

```rust
let config = TransactionConfig::new().with_read_only(true);
let txn = env.begin_transaction(Some(&config))?;
db.get(Some(&txn), &key, &mut val)?;
txn.commit()?;
```

**Transaction timeout** guards against runaway transactions that hold locks
indefinitely. If the timeout fires, the transaction is marked timed-out and
subsequent operations return an error.

**Local write** is used on read-only replicas when you need to store local
metadata or session state that should not be replicated to other nodes.

---

## Durability

Each transaction inherits the environment's default `Durability` policy. You can
override it per-transaction to trade durability for performance on
latency-sensitive writes.

```rust
use noxu_db::{Durability, TransactionConfig};

// Fast commit — data is buffered in the OS page cache but not fsynced.
let config = TransactionConfig::new()
    .with_durability(Durability::COMMIT_WRITE_NO_SYNC);
let txn = env.begin_transaction(Some(&config))?;
db.put(Some(&txn), &key, &val)?;
txn.commit()?;
```

See [Durability Policies](durability.md) for the full list of sync policies and
their trade-offs.

---

## Quick Reference

### Factory methods

| Method | Equivalent to |
|--------|--------------|
| `TransactionConfig::new()` | Default (serializable, env lock timeout, full durability) |
| `TransactionConfig::read_committed()` | `new().with_read_committed(true)` |
| `TransactionConfig::read_uncommitted()` | `new().with_read_uncommitted(true)` |
| `TransactionConfig::read_only()` | `new().with_read_only(true)` |

### Builder methods

All builder methods consume and return `Self`, so they chain naturally:

```rust
let config = TransactionConfig::new()
    .with_durability(Durability::COMMIT_NO_SYNC)
    .with_read_committed(true)
    .with_lock_timeout_ms(200)
    .with_no_wait(false);
```

The `set_*` variants take `&mut self` and return `&mut Self` for cases where you
need to configure an existing instance in place.

### All fields at a glance

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `durability` | `Durability` | `COMMIT_SYNC` | Sync policy for this transaction |
| `read_committed` | `bool` | `false` | Release read locks after each operation |
| `read_uncommitted` | `bool` | `false` | Allow dirty reads, no read locks |
| `read_only` | `bool` | `false` | No writes permitted |
| `no_wait` | `bool` | `false` | Fail immediately on lock conflict |
| `lock_timeout_ms` | `u64` | `0` (env default) | Per-lock wait timeout |
| `txn_timeout_ms` | `u64` | `0` (no limit) | Whole-transaction lifetime limit |
| `serializable_isolation` | `bool` | `false` | Retain read locks through commit |
| `importunate` | `bool` | `false` | Steal locks from other holders |
| `local_write` | `bool` | `false` | Writes not replicated |
