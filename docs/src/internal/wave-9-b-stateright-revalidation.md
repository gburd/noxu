# Wave 9-B: Stateright spec re-validation against post-Wave-4-A code

**Branch**: `fix/wave9-b-stateright-revalidation`
**Base**: `sprint/v2.2.0-base` (e41bfc6, built on v2.1.0).
**Status**: complete.

## Background

Wave 4-A (v2.0.0) shipped four major changes to `noxu-rep` that were
not reflected in the Stateright executable specifications under
`crates/noxu-spec/`. Wave 4-A's report explicitly flagged this as a
follow-up:

> Stateright specs were not re-validated against the new
> persistent-acceptor implementation in this wave; that's flagged in
> the audit's closing prose for a follow-up sprint.

Wave 9-B is that follow-up sprint. Each Wave 4-A change is matched
to the spec module that models the same protocol, and the
abstractions are extended so the spec exercises the new
crash-durable behaviour.

## Per-model audit

| Spec module                       | Diverged from production? | Action                                                                                                  | Counterexample? |
|-----------------------------------|---------------------------|---------------------------------------------------------------------------------------------------------|-----------------|
| `flexible_paxos`                  | yes (F5/F31)              | added `Variant::PersistentAcceptor` / `EphemeralAcceptor`, `Crash` action, regression test              | no (production) |
| `vlsn_streaming`                  | yes (F11)                 | added `Variant::PersistentVlsnIndex` / `EphemeralVlsnIndex`, `ReplicaRestart` action; fixed apply/drain | no (production) |
| `master_transfer`                 | yes (F9)                  | added `current_master_feeders` state and `MasterHasFeeders` invariant                                   | no (production) |
| `network_restore`                 | partial (F2/F4)           | preamble updated; added `EnableStreamFeeder` post-restore transition                                    | no (production) |
| `btree_latching`                  | no                        | validated unchanged against `noxu-tree::Tree::insert` / `split_child`                                   | n/a             |
| `wal_commit`                      | no                        | validated unchanged against `noxu-log::LogManager` + `noxu-txn::commit_with_durability`                 | n/a             |
| `recovery_three_phase`            | no                        | validated unchanged against `noxu-recovery::recovery_manager`                                           | n/a             |
| `lock_manager_deadlock`           | no                        | validated unchanged; compile-time anchor on `noxu_txn::LockType` keeps it honest                        | n/a             |
| `cleaner_safety`                  | no                        | validated unchanged against `noxu-cleaner::file_processor`                                              | n/a             |
| `cache_vs_cleaner`                | no                        | validated unchanged against `noxu-evictor` ↔ `noxu-cleaner` ordering                                    | n/a             |
| `xa_two_phase_commit`             | no                        | validated unchanged; compile-time anchor on `noxu_xa::XaFlags` keeps it honest                          | n/a             |

### `flexible_paxos` — F5/F31 (persistent acceptor)

**What changed in production.** `noxu-rep::elections::acceptor_state`
introduces `PersistentAcceptorState`, which writes the
`(promised_term, accepted_term, accepted_master)` triple atomically
to `<env_home>/acceptor.state` on every promise/accept and reloads
it on startup.  Without this, a node that restarts forgets every
promise, and an old proposer can win a fresh majority at the same
term — split-brain.

**Spec divergence.** The pre-Wave-9-B model already carried
`promised_term[n]` / `accepted_term[n]` / `accepted_leader[n]` as
part of the abstract state, but it had **no `Crash` transition**.
Persistence was therefore only validated implicitly: the model
trivially preserved the triple across all transitions because no
transition ever cleared it.

**Wave 9-B fix.** Following the convention already established in
[`btree_latching`][btree-conv] (single model parameterised on a
`Variant` enum, two tests — one for the fixed protocol, one for
regression bait), the model now exposes:

- `Variant::PersistentAcceptor` — the post-Wave-4-A behaviour.
  `Crash { node }` is a no-op on the triple.
- `Variant::EphemeralAcceptor` — the pre-Wave-4-A behaviour.
  `Crash { node }` zeroes `promised_term[node]`,
  `accepted_term[node]`, and `accepted_leader[node]`.

Two `#[test]` cases:

- `paxos_safety_holds` — `assert_properties` on the persistent
  variant. ElectionSafety / PromiseHonoured / QuorumIntersection
  all hold across arbitrary crash sequences.
- `ephemeral_promises_allow_split_brain` — `assert_discovery` on
  the ephemeral variant. The counterexample is a 14-step trace
  where leader 0 wins quorum {0,1} at term 1; acceptor 1 then
  crashes (losing its in-memory promise); leader 2 collects a
  fresh quorum {1,2} at the same term and is also declared
  elected. **This is exactly the F5/F31 split-brain that the
  `acceptor.state` file closes.**

[btree-conv]: ../../../crates/noxu-spec/src/btree_latching.rs

### `vlsn_streaming` — F11 (persistent VLSN index)

**What changed in production.** `noxu-rep::vlsn::persist` writes the
in-memory `VlsnIndex` to `<env_home>/vlsn.idx` on a clean shutdown
and reloads it on startup, so a restarted replica resumes from its
last persisted vlsn instead of forcing a full network restore.

**Spec divergence.** Same shape as the Paxos case: the pre-Wave-9-B
model had `replica_applied_high` in state but no `Restart` transition.
The model also contained a **pre-existing latent bug**: `ReplicaApply`
drained `in_flight`, which made `MasterReceiveAck` unreachable from
any non-zero state. The result was that `master_acked_high` could
never advance past 0, and the `AckTracksReceived` invariant
(`master_acked_high <= replica_applied_high`) was a trivial truth.

**Wave 9-B fixes.**

1. **Apply/drain semantics** — `ReplicaApply` now advances
   `replica_applied_high` without dropping the entry from
   `in_flight`; `MasterReceiveAck` is what removes it.  This matches
   the wire protocol: the master holds the entry in its retry buffer
   until it observes the replica's ack.  With this fix, BFS actually
   exercises the ack path.
2. **Variant + Restart** — `Variant::PersistentVlsnIndex` /
   `Variant::EphemeralVlsnIndex` plus a `ReplicaRestart` action.
   Persistent: `applied_high` survives.  Ephemeral: it snaps back to
   0.
3. **Regression test** — `ephemeral_vlsn_index_loses_applied_progress`
   discovers a 4-step counterexample: send vlsn=1, apply, master
   receives ack, replica restarts.  After the restart,
   `master_acked_high=1` but `replica_applied_high=0` —
   `AckTracksReceived` violated.

### `master_transfer` — F9 (become_master spawns feeders)

**What changed in production.**
`replicated_environment::become_master` now spawns a `Feeder`
tracker for every electable peer, so AckTracker bookkeeping can
attribute replica acks correctly and the `peer_scanner` actually
pushes writes for replicas pulling from `PEER_FEEDER`.  Without
this, a node could become master in the role-state sense but be
silently unable to serve replicas.

**Spec divergence.** The pre-Wave-9-B model checked only role-state
safety (`AtMostOneMaster`, `AtMostOneDraining`,
`MasterTermsMonotone`).  It would have stayed green even if
production had stopped spawning feeders entirely — F9 has no
visible effect on the role state alone.

**Wave 9-B fix.** Added `current_master_feeders: [bool; N_NODES]` to
the abstract state and a new invariant `MasterHasFeeders`: whenever
some node is in `MasterActive` or `MasterDraining`, the feeder map
must contain every other peer; when no node holds the role, the map
must be empty.  Both `BecomeMaster` and `HandoffComplete` now
populate the map; `StartDrain` does **not** clear it (drain keeps
feeders alive until the successor takes over).

### `network_restore` — F2/F4 (dispatcher integration)

**What changed in production.** `network_restore::execute_via_dispatcher`
exchanges framed payloads through `connect_to_service(RESTORE)` on
the `TcpServiceDispatcher`, instead of the legacy raw-TCP
`execute()` path.  The dispatcher is a transport detail — the donor
still ships `[count][file_records...]` in a single payload and the
recipient still applies entries in vlsn order — but the integration
adds a post-restore handover where the replica re-enables the
streaming feeder.

**Spec divergence.** The pre-Wave-9-B model captured the abstract
protocol correctly (StartRestore → ApplyEntry…→ CompleteRestore,
plus Fail/Resume).  However, `stream_feeder_active` was set to
`false` at `StartRestore` and **never flipped back to `true`** — the
post-restore handover was modelled as a cliff.

**Wave 9-B fix.** Documented the dispatcher integration in the
module preamble, then added an `EnableStreamFeeder` transition that
fires only after `CompleteRestore`.  The `NoConcurrentCorruption`
invariant now exercises the full restore→stream handover.

### Specs validated unchanged

The remaining seven specs were inspected against current production
code and confirmed accurate.  None of the Wave 4-A changes touched
their subject areas:

- **`btree_latching`** — `noxu-tree::Tree::insert` / `split_child`
  still follow hand-over-hand latching with the BIN write taken
  before the parent read is released.  The `Variant::HandOverHand`
  / `Variant::DropParentEarly` pair remains accurate regression
  bait.
- **`wal_commit`** — `noxu-log::LogManager` + `noxu-txn::commit_with_durability`
  semantics unchanged.  DurableImpliesLogged / LsnMonotone /
  FsyncedNeverDecreases hold.
- **`recovery_three_phase`** — analysis → redo → undo pipeline
  unchanged.
- **`lock_manager_deadlock`** — automatically kept honest by the
  compile-time anchor `spec_lock_kind(noxu_txn::LockType)`; an
  exhaustive match would break the build if a new `LockType`
  variant were added.
- **`cleaner_safety`** — cleaner's pre-deletion live-check is still
  the same protocol.
- **`cache_vs_cleaner`** — evictor↔cleaner ordering unchanged.
- **`xa_two_phase_commit`** — automatically kept honest by the
  compile-time `_FLAG_ANCHOR` referencing every `XaFlags`
  constant.

## Production code bugs surfaced

**None.** Every newly introduced invariant — `MasterHasFeeders`,
`AckTracksReceived` after the apply/drain fix, ElectionSafety
across `Crash` under `PersistentAcceptor` — passes.  The only
counterexamples found are the deliberate ephemeral-variant
regression baits, which match the pre-Wave-4-A behaviour.

The one bug Wave 9-B *did* fix is in the spec itself: the
`vlsn_streaming` model previously drained `in_flight` on apply,
silently weakening `AckTracksReceived` to a trivial truth.  This
was a pre-existing model bug, not a production regression.

## CI integration

Already complete from prior waves:

- **`make spec`** target in the root Makefile runs
  `cargo test -p noxu-spec --release`.
- **`.github/workflows/spec.yml`** runs the same target on every
  push to `main` and every pull request.

Total runtime on a development machine after Wave 9-B's additions:
**~24 seconds** in release mode (up from ~0.02 s pre-fix).  The
state-space increase is mostly from `flexible_paxos` (Crash adds 3
extra binary dimensions to the per-node state) and `vlsn_streaming`
(Restart × WAL_LEN × applied/sent combinations).  Both are well
within the CI workflow's default 360-minute job timeout.

If a future spec change pushes the runtime past ~5 minutes, the
recommended escape hatch is a `make spec-quick` target that runs
the suite with reduced `MAX_TERM` / `MASTER_WAL_LEN` / `N_NODES`
constants via `#[cfg(feature = "spec-quick")]` gating — but that is
not needed today.

## Known divergences that remain

These are intentional and documented; not action items for this
wave:

1. **`master_transfer` does not model VLSN catch-up.**  The model
   tracks role state and feeder presence, not commit-point
   propagation across the handoff.  A liveness property
   "post-handoff, the new master eventually catches up to the old
   master's commit point" would require modelling the data plane
   and is out of scope for this spec.
2. **`network_restore` does not model the `NeedsRestore` signal
   path.**  `StartRestore` is unconditional in the model.  In
   production, `StartRestore` is gated on the master signaling
   `NeedsRestore` (stream returns `Ok(false)` from
   `catch_up_from_peer`).  Adding a `MasterSignalsNeedsRestore`
   precondition would be a small extension and is a candidate for
   a future spec wave.
3. **`flexible_paxos` runs at `MAX_TERM = 1`.**  Increasing to
   `MAX_TERM = 2` would let the model exercise term escalation
   after a stale-promise rejection.  The state-space cost was
   judged not worth it for this wave; the F5/F31 split-brain is
   already captured at `MAX_TERM = 1`.
4. **`xa_two_phase_commit` does not check `RecoveryConsistent` as
   a 2-state predicate.**  Previously flagged in the module's own
   TODO; out of scope for Wave 9-B.

## Commits

| Commit  | Subject                                                                |
|---------|------------------------------------------------------------------------|
| eaefb9b | wave9-b(docs): placeholder for Stateright spec re-validation           |
| fab3238 | feat(spec)!: model persistent acceptor in flexible_paxos (F5/F31)      |
| 0cff25f | feat(spec)!: model persistent vlsn.idx in vlsn_streaming (F11)         |
| 4449528 | feat(spec): model F9 feeder spawning in master_transfer                |
| 39476b0 | feat(spec): model dispatcher-mediated network restore (F2/F4)          |

The two `feat(spec)!` commits introduce breaking changes to the
spec-module APIs (`FlexiblePaxosModel::persistent()` /
`VlsnStreamingModel::persistent()` constructors replace the old
unit-struct usage) but are internal to `noxu-spec` and have no
external consumers — `noxu-spec` is `publish = false`.
