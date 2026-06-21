# Network Restore

> **v1.5 status — broken on the dispatcher path.** See
> [Introduction → v1.5 capability matrix](../introduction.md#v15-capability-matrix)
> and [the chapter overview](index.html). The standalone `serve_raw`
> path used by the unit tests works; the path used by
> `ReplicatedEnvironment` (via `TcpServiceDispatcher`) misinterprets
> the 4-byte `NRST` magic as a 1.31 GiB length prefix and never
> succeeds. New replicas cannot bootstrap through the documented
> path. (Audit findings 2 and 4.)

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

Partial rollback within a running replica stream (rolling back uncommitted
entries after a term change, without a full network restore) is **not yet
implemented**.  An earlier `TxnChain` container was removed (TXN-7) because it
did not perform JE's backward-log-walk and was unused; a correct
syncup-rollback path will be added when the HA syncup workstream lands.  Until
then, a replica that cannot fast-forward falls back to a full network restore.

## Recovery from Restore

After restore, the replica opens the environment normally:

```rust
// The environment was replaced by the restore; re-open it
let env = Environment::open(Path::new("./data"), EnvironmentConfig::default())?;
let rep_env = ReplicatedEnvironment::new(env, rep_config)?;
```
