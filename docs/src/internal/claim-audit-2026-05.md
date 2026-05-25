# Claim Audit — May 2026

A read-only pass over every public function in `noxu-db`,
`noxu-engine`, and `noxu-rep`, comparing the doc comment
against the body of the function. The intent is to find
documentation that has drifted out of sync with code: stub
implementations claiming to do something, error surfaces that
omit real failure modes, references to APIs that no longer
exist.

This was the first systematic claim audit of the public
surface and uncovered 23 findings. Items marked **high** are
behavioural — the code does meaningfully less than the doc
claims. **Medium** items are missing-error-mode or
stale-rationale issues. **Low** items are minor wording or
edge-case mismatches.

## Findings by crate

### noxu-db (5)

- `environment.rs:382` `open_database` (medium): documented
  errors omit `DatabaseAlreadyExists`, but the body returns it
  when a handle for that name is already open in this
  Environment.
- `database.rs:744` `scan_all_kv` (medium): stale rationale
  "the public Cursor interface does not expose key bytes
  during iteration" — `Cursor::get` (`cursor.rs:84`) takes
  `key: &mut DatabaseEntry` and writes back the current key on
  every successful position.
- `database.rs:795` `sync` (low): doc claims `fdatasync`
  ensures durability before returning; body silently no-ops
  when `log_manager` is `None` (non-transactional / in-memory
  env).
- `transaction.rs:172` `commit` (medium): doc says only "not
  in Open state" errors; body can also return
  `EnvironmentFailure(LogWrite)` from `write_txn_end` and
  propagate `inner_txn.commit()` errors. Same for
  `commit_with_durability` (line 190).
- `transaction.rs:287` `abort` (medium): doc says only
  "already committed/aborted" errors; body can also fail from
  `write_txn_end` (`LogWrite`) when `log_manager` is set and
  txn is not read-only.

### noxu-engine (5)

- `engine.rs:174` `close` (high): doc lists "3. Close
  EnvironmentImpl" as a step; body has explicit comment
  `(EnvironmentImpl doesn't have explicit close yet — would be
  added in full implementation)` and skips it.
- `engine.rs:223` `checkpoint` (low): has `# Returns` but no
  `# Errors` despite returning `EnvironmentClosed` and
  `InvalidConfig` (read-only). Same pattern in `clean` (line
  248), `evict`, `clean_adaptive`.
- `verify.rs:453` `verify_environment` (high): doc claims
  "Performs structural verification". Body only logs and
  returns an empty passing `VerifyResult{}`; no verification
  work is performed.
- `verify.rs:478` `verify_database` (high): doc claims
  "Verify a specific database by name". Body only logs and
  returns `VerifyResult{databases_verified: 1, passed: true}`;
  no verification regardless of `db_name` or `config`.
- `daemon_manager.rs:277` `is_running` (medium): doc says
  "Returns true if any daemon threads are running"; body
  returns `!self.shutdown.load(...)`, which is true after
  `new()` even before `start_daemons()` is called.

### noxu-rep (13)

- `replicated_environment.rs:181` `new` (high): doc claims
  "Creates a replicated environment handle and starts
  participating in the replication group", "When `new()`
  returns, the node will have established contact with the
  other members of the group", "creation will trigger an
  election", "A brand new node will always join an existing
  group as a Replica…". Body does NONE of this — only
  constructs state and starts a TCP service dispatcher; state
  remains `NodeState::Detached` after `new()`
  (`test_initial_state_is_detached` confirms).
- `replicated_environment.rs:594` `become_master` (high):
  doc claims "a `FeederRunner` + `EnvironmentLogScanner`
  background thread is spawned for each currently-registered
  replica". Body iterates feeders but only emits a log
  message; neither `FeederRunner` nor `EnvironmentLogScanner`
  is constructed, no thread is spawned.
- `replicated_environment.rs:788` `transfer_master` (high):
  doc says "Transfers the current master state… ensures that
  all changes at this node are available at the new master
  upon conclusion of the operation." Body has explicit inline
  comment "In a full implementation, this would coordinate
  with the target replica… For now, we record the intent."
  Only logs.
- `replicated_environment.rs:959` `shutdown_group` (high):
  doc says "The Master waits for all active Replicas to catch
  up so that they have a current set of logs, and then shuts
  them down." Body validates `is_master()`, logs, then calls
  `self.close()`. No wait-for-replicas, no catch-up
  coordination, replicas are not shut down.
- `network_restore.rs:293` `start` (medium): stale doc.
  Implementation is in `execute()` (line ~140); `start()` is
  currently only a state-transition helper.
- `stream/feeder.rs:13` (module doc, medium): references
  "Output and input thread pair inside the feeder". Actual
  `FeederRunner` is single-threaded — one `run()` loop
  combining log scanning, sending, and ack polling.
- `stream/feeder.rs:465` `queue_entry` (low): doc says "The
  current VLSN is advanced to one past the queued VLSN." Body
  only advances when `vlsn >= *current`; lower VLSNs leave
  current unchanged.
- `stream/peer_feeder.rs:83` `push` (medium): doc says
  "Entries must be pushed in VLSN order; out-of-order pushes
  update only the bounds." Body unconditionally enqueues every
  entry regardless of order.
- `stream/replica_stream.rs:404` `is_caught_up` (low): doc
  says "Returns true if applied VLSN equals or exceeds
  master's latest known VLSN and there are no pending
  entries." Body adds undocumented `master > 0` precondition.
- `rep_group.rs:139` `quorum_size` (low): opens with "Returns
  the quorum size: a simple majority of electable nodes."
  Body returns `phase2_quorum() as u32`, which is
  policy-dependent — under Flexible or Expression policies the
  result is not a simple majority.
- `elections/phi_detector.rs:101` `record_heartbeat` (low):
  doc says "Updates the inter-arrival sample window." On the
  first call there is no previous heartbeat so no sample is
  added.
- `ack_tracker.rs:137` `check_timeouts` (low): doc says
  "Check for timed-out acks and return their VLSNs" — implies
  pure read. Body also increments
  `*self.total_timeouts.lock()`.

## Summary

| Crate | High | Medium | Low | Total |
|---|---|---|---|---|
| noxu-db | 0 | 4 | 1 | 5 |
| noxu-engine | 3 | 1 | 1 | 5 |
| noxu-rep | 4 | 4 | 5 | 13 |
| **Total** | **7** | **9** | **7** | **23** |

## Top issues

1. **Engine and verification stubs documented as if
   functional.** `Engine::close()` skips closing
   `EnvironmentImpl`; `verify_environment` and
   `verify_database` return empty passing `VerifyResult`s
   without performing verification.
2. **`ReplicatedEnvironment::new()` makes broad protocol-level
   promises that the body doesn't implement.** No election, no
   group contact; node remains `Detached`.
3. **`become_master`, `transfer_master`, `shutdown_group` are
   partially or entirely stubbed.**
4. **`DaemonManager::is_running()` does not answer "are
   daemons running"** — it answers "has shutdown been called",
   which is true for a freshly-constructed manager with zero
   threads.
5. **Public Database/Transaction operations under-document
   their error surface.**
6. **`PeerLogScanner::push()` doc contradicts behaviour.**

## Disposition

This audit is published as evidence; the per-finding
remediation is tracked separately. Some of the high-severity
items (verify, transfer_master, shutdown_group) describe
**features that are not actually implemented** and the
production claim should be retracted or the feature
implemented before the next minor release. The lower-severity
items can be incorporated into a documentation-cleanup PR.
