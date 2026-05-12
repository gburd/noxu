# Network Restore

If a replica's log has been partially cleaned and it has fallen so far behind
that it cannot recover from the master's VLSN stream, it needs a **network
restore** — a full copy of the master's environment.

## When Network Restore Triggers

- Replica VLSN is below the master's `first_active_lsn`
- Required log files have been cleaned and deleted
- `RepError::RollbackRequired` is returned to the application

## Restore Process

1. The replica contacts the master and requests a restore.
2. The master's `NetworkRestoreProvider` (wired into `TcpServiceDispatcher`)
   accepts the connection.
3. The master streams a consistent snapshot of its log files over TCP.
4. The replica replaces its local log with the received files.
5. The replica performs normal recovery on the received files.
6. The replica reconnects to the master and resumes streaming.

## RollbackRequired

`RepError::RollbackRequired { from: Vlsn, to: Vlsn }` indicates that the
replica must roll back to VLSN `to` before it can rejoin. This occurs when
the master's term changes and the replica has entries that were never
committed in the new term.

The `TxnChain` mechanism handles partial rollback within a running replica
stream.

## Recovery from Restore

After restore, the replica opens the environment normally:

```rust
// The environment was replaced by the restore; re-open it
let env = Environment::open(Path::new("./data"), EnvironmentConfig::default())?;
let rep_env = ReplicatedEnvironment::new(env, rep_config)?;
```
