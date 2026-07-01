# Using Noxu from Async Code

**Noxu is synchronous by design.** Every operation — `get`, `put`, `commit`,
cursor navigation, `Environment::open` — is blocking: it does real disk I/O,
acquires locks, and may park the calling thread. There is no `async` API and
none is planned. The engine uses explicit threads and blocking I/O throughout;
only the optional `replication` feature's networking uses `tokio` internally.

If you call Noxu from inside a `tokio` (or other async) runtime, do **not**
call it directly on an async worker thread — a blocking call there stalls every
other task sharing that worker. Move the work onto a blocking thread instead:

```rust,ignore
async fn lookup(
    env: std::sync::Arc<noxu::Environment>,
) -> Result<Option<bytes::Bytes>, Box<dyn std::error::Error>> {
    // `Environment` is Send + Sync; clone the Arc into the blocking task.
    let value = tokio::task::spawn_blocking(move || {
        let db_cfg = noxu::DatabaseConfig::new().with_allow_create(true);
        let db = env.open_database(None, "users", &db_cfg)?;
        db.put(b"k", b"v")?;
        db.get(b"k")
    })
    .await??; // first `?`: JoinError; second `?`: NoxuError
    Ok(value)
}
```

## Guidelines

- Wrap each unit of Noxu work in `tokio::task::spawn_blocking` (or a dedicated
  blocking thread pool), not the async path.
- **Never hold a `Transaction` (or an open `Cursor`) across an `.await`.** A
  transaction holds locks; suspending the task while it is open can block other
  writers indefinitely, and the borrow on the transaction would also prevent
  the future from being `Send`. Open, use, and commit or abort a transaction
  entirely within one `spawn_blocking` closure.
- `Environment` is `Send + Sync`, so share it across tasks via
  `Arc<Environment>` and open per-task databases and transactions inside the
  blocking closure.
