# Transaction Processing

This chapter covers Noxu DB's transaction model in depth. It corresponds to the
**Noxu DB Getting Started with Transaction Processing** guide, adapted
for the Rust API.

Noxu DB uses **record-level locking** (not MVCC). Writers hold locks until
commit or abort; readers block on write-locked records. This is faithful to
Noxu's isolation model and is the basis for all concurrency guarantees
described here.

## In This Chapter

1. [Transaction Basics](basics.md) — `begin_transaction`, commit, abort, `TransactionConfig`
2. [Cursors and Transactions](cursors.md) — cursor lifetime and transactional iteration
3. [Secondary Indexes with Transactions](secondary-with-txn.md) — secondary DB ops under transactions
4. [Concurrency](concurrency.md) — thread safety, the lock model, blocking semantics
5. [Isolation Levels](isolation.md) — read-committed, serializable, and default isolation
6. [Deadlock Handling](deadlocks.md) — detection, retry patterns, victim selection
7. [Durability Policies](durability.md) — `SyncPolicy`, `DurabilityPolicy`, group commit
8. [Backup and Recovery](backup-recovery.md) — live backup, normal vs catastrophic recovery
