# Durability Policies

Noxu DB provides fine-grained control over how writes are persisted to disk
and acknowledged. The key types are `SyncPolicy`, `DurabilityPolicy`, and
`ReplicaAckPolicy` (for replicated environments).

## Performance Tuning

The use of transactions introduces overhead that non-transactional applications
do not incur. Key tuning considerations:

### Durability vs. Throughput

The default `SyncPolicy::Sync` performs a full fsync on every commit. This is the
most durable option but also the slowest. If you can tolerate the possibility of
losing the most recent committed transactions in the event of an OS crash, use
`SyncPolicy::WriteNoSync`. If you can tolerate losing committed transactions on
application crash as well, use `SyncPolicy::NoSync`. See
[Non-Durable Transactions](basics.md#non-durable-transactions).

### Commit Lock-Release Ordering

Commit is a two-phase operation: an **append** phase writes the commit record
to the write-ahead log buffer (which assigns the commit LSN), and a **durable**
phase performs the fsync that makes it durable. A transaction's write locks are
released *between* these two phases — after the commit record is in the WAL
buffer, but before the fsync completes.

The committer still blocks on the fsync in the durable phase before `commit()`
returns, so the durability contract is unchanged: **a `Sync` commit that
returned successfully is durable.** Releasing the write locks earlier only
shrinks the window in which they are held from the whole fsync (100µs–2ms) to
microseconds, which dissolves the lock convoy that otherwise forms on a hot key
under high write contention.

This is safe because Noxu uses a single write-ahead log with a monotonic
durable point: a transaction that acquires a just-released lock and commits is
assigned a strictly-higher LSN, and a single fsync makes everything up to a
point durable. A later, dependent commit can therefore never become durable
ahead of the commit it depends on — if the earlier commit is lost to a crash,
any later commit that read its released lock is lost too, and recovery replays
in LSN order. The framing is: *locks guard logical conflict; durability is a
separate barrier the committer still waits on.*

> This ordering is a deliberate tail-latency optimization specific to Noxu. It
> trades a small window in which a not-yet-durable value is visible to a
> concurrent read-committed reader (whose own commit is ordered after it in the
> same log) for a large reduction in p99 latency under hot-key contention.

### Reduce Lock Contention

- Keep transactions short. Shorter transactions hold locks for less time, reducing
  the probability of blocking other threads.
- Minimize the lifetime of transactional cursors; read locks are held by default
  until the transaction ends.
- Access heavily-accessed records toward the end of the transaction to reduce the
  time that popular records are locked.
- Use committed-read isolation (`TransactionConfig::with_read_committed(true)`) for
  forward-scanning cursors. This releases read locks as the cursor advances rather
  than holding them for the entire transaction.
- Use uncommitted-read isolation where tolerable — this avoids taking read locks
  entirely for read operations.

### Isolation Level Trade-offs

| Level | Locking Cost | Anomalies Possible |
|-------|--------------|--------------------|
| Read Uncommitted | Lowest (no read locks) | Dirty reads, non-repeatable reads, phantoms |
| Read Committed | Low (read locks released early) | Non-repeatable reads, phantoms |
| Repeatable Read (default) | Medium | Phantoms |
| Serializable | Highest (range locks) | None |

Choose the weakest isolation level that your application's correctness requirements
allow.

### Data Access Patterns

If threads can be designed to operate on non-overlapping portions of the database,
lock contention is naturally minimized. Partition your data and assign disjoint
key ranges to different threads where possible.

### Deadlock Avoidance

- Apply the same strategies as for reducing blocking.
- Ensure all threads lock records in the same order. Consistent lock ordering
  eliminates deadlocks (though not blocking).
- Use the read-modify-write pattern (`LockMode::Rmw`) when you know a read will be
  followed by a write, to avoid read-lock-to-write-lock upgrades that can deadlock.
- Expect deadlocks when using secondary databases in a concurrent application.
  Always implement the retry loop shown in [Aborting a Transaction](basics.md#aborting-a-transaction).

### Checkpointing

Run checkpoints regularly to bound recovery time on restart. The checkpointer
daemon runs in the background by default (`EnvironmentConfig::with_run_checkpointer(true)`).
For high-write workloads you can tune the checkpoint interval to balance write
amplification against recovery time.

### Summary of Key Configuration Points

| Goal | Configuration |
|------|--------------|
| Enable transactions | `EnvironmentConfig::with_transactional(true)` |
| Auto-commit a single write | Pass `None` for `txn` to `db.put()` / `db.delete()` |
| Relax durability for throughput | `Durability::COMMIT_WRITE_NO_SYNC` or `COMMIT_NO_SYNC` |
| Reduce read lock pressure | `TransactionConfig::with_read_committed(true)` |
| Allow dirty reads | `TransactionConfig::with_read_uncommitted(true)` |
| Prevent phantom reads | `TransactionConfig::with_serializable_isolation(true)` — next-key range locking prevents concurrent inserts into the scanned range |
| Avoid lock-upgrade deadlocks | `LockMode::Rmw` on the initial read |
| Set environment lock timeout | `EnvironmentConfig::with_lock_timeout(ms)` |
| Set per-transaction lock timeout | `txn.set_lock_timeout(ms)` |
| Fail immediately on lock conflict | `TransactionConfig::with_no_wait(true)` |
| Set per-txn lock timeout | `TransactionConfig::with_lock_timeout_ms(50)` |
| Set per-txn transaction timeout | `TransactionConfig::with_txn_timeout_ms(5000)` |
| Steal locks from waiters | `TransactionConfig::with_importunate(true)` |
| Bound recovery time | Keep checkpointer enabled (`with_run_checkpointer(true)`) |
