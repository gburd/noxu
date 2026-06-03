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
| Serializable | Highest (intended: range locks) | Phantoms still possible — range locking not yet wired (see [isolation](isolation.md)) |

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
| Prevent phantom reads | *(intended via* `TransactionConfig::with_serializable_isolation(true)`*; not yet enforced — see [isolation](isolation.md))* |
| Avoid lock-upgrade deadlocks | `LockMode::Rmw` on the initial read |
| Set environment lock timeout | `EnvironmentConfig::with_lock_timeout(ms)` |
| Set per-transaction lock timeout | `txn.set_lock_timeout(ms)` |
| Fail immediately on lock conflict | `TransactionConfig::with_no_wait(true)` |
| Set per-txn lock timeout | `TransactionConfig::with_lock_timeout_ms(50)` |
| Set per-txn transaction timeout | `TransactionConfig::with_txn_timeout_ms(5000)` |
| Steal locks from waiters | `TransactionConfig::with_importunate(true)` |
| Bound recovery time | Keep checkpointer enabled (`with_run_checkpointer(true)`) |
