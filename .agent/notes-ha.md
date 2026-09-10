# HA remaining-gaps investigation notes

Working tree: `/tmp/w-ha` (branch `feat/ha-remaining`). JE checkout present at
`~/ws/je` (confirmed — used for all citations below).

## Where things live

- Syncup driver (replica side): `crates/noxu-rep/src/stream/syncup.rs`
  - `find_matchpoint`, `verify_rollback`, `RollbackDecision`,
    `SyncupAction::{RollbackToMatchpoint, DivergedRefused, NoActionNeeded}`.
  - Called from `ReplicatedEnvironment::syncup_with_feeder` /
    `syncup_with_feeder_at` in `src/replicated_environment.rs` (~line 1773
    onward).
- `classify_tail`-style default-deny logic: the decision is made in
  `verify_rollback` (in `syncup.rs`) based on:
  - `matchpoint` (found or none),
  - `last_txn_end` vs `matchpoint_vlsn` (JE: rollback interval must never
    include a **committed** txn — if `last_txn_end > matchpoint`, i.e. a
    commit/abort record exists above the matchpoint, JE would need
    `Replay.rollback`'s in-memory `TxnChain` revert; without it we must
    refuse),
  - `num_passed_commits` (count of txn-end records strictly above the
    matchpoint, computed by re-scanning the log when a `SyncupLogView` is
    available).
  - Concretely: today's decision only allows `RollbackToMatchpoint` when the
    divergent tail contains **provisional transactional LN records that were
    never committed** (i.e., they were logged but the matching TxnCommit
    never streamed in — JE's classic "uncommitted tail" case). Any tail that
    contains an applied **non-transactional LN**, a structural entry (IN/BIN),
    or any **TxnCommit/TxnAbort above the matchpoint** causes
    `SyncupAction::DivergedRefused`, because rolling those back would require
    reverting live B-tree mutations / in-flight transaction chain state that
    we do not track (see Gap A below) — refusing and forcing a network
    restore is the safe choice already baked in.
  - Doc comments in `syncup.rs` (around line 287-331) already explain this:
    "a transactional LN above the matchpoint therefore has, by construction,
    no committed `TxnCommit` above the matchpoint" is the ONLY case currently
    truncated; everything else is refused.

- Master-change / mid-stream syncup renegotiation (Gap B): I found NO
  `MasterChangeListener`-equivalent hook that re-triggers syncup on a
  replica already streaming. `ReplicaReceiver::run`/`run_until` in
  `src/stream/replica_stream.rs` just keeps reading frames from whatever
  channel it was given; nothing watches `master_tracker` /
  `election_state` for a term change and tears down + re-syncs the stream.
  `MasterTracker` (`src/elections/master_tracker.rs`) *does* track
  `set_master(name, term)` and is updated by `become_master`, and the
  replica thread captures `master_name` once at spawn time
  (`replicated_environment.rs` ~2600-2670, `become_replica`) — but there is
  no loop that notices `master_tracker`'s master changing while the
  `noxu-replica-*` thread's blocking `ReplicaReceiver::run` is already
  in progress. JE equivalent: `Replica` listens for a `MasterChangeListener`
  callback (`RepNode`/`Replica.java`) and re-enters `Replica.runReplicaLoop`
  → fresh `ReplicaFeederSyncup` on a master change. We do not have that
  loop; the current replica thread runs syncup exactly ONCE at startup, then
  streams forever against the channel it was handed, even if `become_replica`
  is later called again with a different master name (that spawns a whole
  NEW thread + channel, but nothing forces the OLD thread to notice its
  master is stale and exit on its own — it relies on the channel being
  explicitly closed by the caller).

- Daemon-thread lifecycle pattern used throughout `noxu-rep`: NOT
  `noxu-engine::daemon_manager` (that manager is specific to
  evictor/cleaner/checkpointer inside `noxu-engine`/`noxu-dbi`). `noxu-rep`
  has its own convention: `ReplicatedEnvironment` keeps
  `io_threads: StdMutex<Vec<JoinHandle<()>>>` and `io_shutdown: Arc<AtomicBool>`;
  each daemon is `std::thread::Builder::new().name("noxu-<kind>-<node>").spawn(...)`,
  loops `while !io_shutdown && !is_shutdown() { sleep(interval); ...work... }`,
  does a final flush/action on exit, and pushes its `JoinHandle` onto
  `io_threads`. `close()` sets `io_shutdown`, then joins every thread in
  `io_threads`. Idempotent-spawn guard: check `io_threads` for a thread whose
  `.name()` already starts with the daemon's prefix before spawning again.
  Existing examples to copy: `start_vlsn_persistence_daemon` (F11, ~line
  789-880) and `start_election_driver` (~line 900+). `open()` currently calls
  `start_election_driver()` + `start_vlsn_persistence_daemon()` +
  `register_admin_service()`.

- DTVLSN state machine already exists and is wired for RANKING purposes:
  `dtvlsn: AtomicU64` field on `ReplicatedEnvironment`
  (`replicated_environment.rs` ~line 240), `get_dtvlsn`/`update_dtvlsn`/
  `set_dtvlsn` (~3184-3214), master-side computation
  `update_dtvlsn_from_feeders` (~3221, called from `record_ack`) using
  SIMPLE_MAJORITY over qualifying electable feeders' `Feeder::acked_vlsn`
  high-water marks. This is JE `RepNode.dtvlsn` + `FeederManager.updateDTVLSN`
  faithfully ported EXCEPT for:
  1. JE seeds `dtvlsn.updateMax(repImpl.getLoggedDTVLSN())` at RepNode
     startup (`RepNode.java:386`) — reads the last commit/abort's persisted
     dtvlsn field from the log. Our `dtvlsn` field starts at `0` always; no
     equivalent seed-from-log call exists.
  2. JE's `FeederManager.DTVLSNFlusher.flush()` — called every feeder-manager
     poll tick (`FeederManager.java` ~line 636) — persists the DTVLSN to disk
     by writing a **null MasterTxn commit** (no tree changes, just a
     TxnCommit WAL record) once the in-memory DTVLSN has been stable for
     `targetStableTicks` ticks and exceeds the last-persisted value. This is
     the piece that is completely absent from noxu-rep: **Gap C**. Nothing in
     `noxu-rep` ever writes a commit record purely to flush the DTVLSN;
     `log_txn_commit`/`TxnEndEntry.dtvlsn` machinery exists on the WAL side
     (`crates/noxu-log/src/entry/commit_abort_entry.rs`,
     `crates/noxu-dbi/src/environment_impl.rs::log_txn_commit`,
     `crates/noxu-recovery` R-3/X-14 VLSN-rebuild paths that read
     `TxnEndEntry.dtvlsn` back), but nothing ever POPULATES a non-zero
     dtvlsn field on an ordinary commit (`write_txn_end` in
     `crates/noxu-db/src/transaction.rs` always passes `NULL_VLSN` for
     `dtvlsn`), and no timer ever forces a commit purely to persist the
     current DTVLSN during a quiet period.
  - `docs/src/operations/known-limitations.md` (row: "Replication HA protocol
    is incomplete...") ALREADY documents both missing pieces ("there is no
    periodic `DTVLSNFlusher` daemon" and the `Replay.rollback` TxnChain gap)
    — so this was a known, honestly-recorded gap before this session; my job
    is to close what I can and keep the doc honest about what's left.

## Gap-by-gap: what closing each one requires

### Gap A — JE `Replay.rollback` step 2 (in-memory `TxnChain` revert)

JE reference: `Replay.rollback(VLSN matchpointVLSN, long matchpointLsn)`
(`~/ws/je/src/com/sleepycat/je/rep/impl/node/Replay.java:1060`), which for
every locally-active `ReplayTxn` calls `replayTxn.rollback(matchpointLsn)`
(`ReplayTxn.java:390`), which in turn builds a `TxnChain`
(`~/ws/je/src/com/sleepycat/je/txn/TxnChain.java`) by walking the txn's
logrec chain BACKWARD from its last-logged LSN, and for every logrec above
the matchpoint records a `RevertInfo` (the pre-image key/data/abortLsn to
revert each BIN slot to) using a `TreeMap<CompareSlot, RevertInfo>` keyed by
(dbId, key) so that if the same record was written more than once in the
rolled-back span, only the FIRST post-matchpoint version's abort-info
survives as the true revert target (skipping intermediate versions). Then
`ReplayTxn.undoWrites` applies each `RevertInfo` in reverse-LSN order,
directly mutating the live in-memory tree via the same cursor-based
put/delete path normal aborts use, and marks the rolled-back on-disk LSNs
"invisible" (obsolete) — `Replay.rollback` wraps the whole thing between a
durably-fsynced `RollbackStart` record and a `RollbackEnd` record for crash
recoverability of the rollback itself.

**What porting this faithfully requires in noxu-rep/noxu-dbi:**
1. A Rust `TxnChain`-equivalent: given a txn's last-logged LSN and a
   matchpoint LSN, walk backward through the WAL (or through
   `ReplicaReplay`'s already-buffered `active_txns` map, which is a much
   smaller and already-in-memory version of the same LN chain — this may be
   the leaner path since Noxu's replica already buffers uncommitted
   transactional LNs entirely in memory in `ReplicaReplay::active_txns`,
   never applying them to the tree until commit) and build the revert-info
   per (db_id, key).
2. A revert/undo apply step that mutates the live replica tree back to the
   pre-divergence state for any record that WAS already applied (i.e., was
   part of a committed txn, or a non-transactional LN) above the matchpoint.
   This is the genuinely hard, correctness-critical part: it requires
   reaching into `noxu-tree`'s BIN mutation path with the SAME undo
   machinery `Txn::abort_collect_undo` / `apply_redo_ln`'s inverse already
   uses for ordinary txn abort, but driven by matchpoint LSN rather than by
   an aborting txn's own write-lock set.
3. Marking the rolled-back on-disk LSNs invisible/obsolete (parallel to
   what the existing syncup rollback already does for the provisional-LN
   case — `RollbackTracker`-equivalent logic already exists in
   `crates/noxu-recovery/src/rollback_tracker.rs` for the recovery-time
   rollback case and could likely be reused/extended for the live-syncup
   case).

**Why I did not attempt this in the current session:** `ReplicaReplay`'s
current model (see `crates/noxu-dbi/src/replica_replay.rs` module doc,
"Transaction model (provisional-apply, resolved at commit)") ALREADY buffers
transactional LNs in memory until commit and discards them cleanly on
abort — this is why today's `classify_tail`/`verify_rollback` can safely
truncate a tail of provisional (uncommitted) transactional LNs: they were
NEVER applied to the live tree, so "rollback" is just dropping the buffer
entry, which is already what `abort_txn` does. The genuinely unimplemented
case is a tail containing a COMMITTED txn or a non-transactional LN — i.e.,
something that WAS applied to the live B-tree — where reverting requires the
TxnChain-style pre-image walk + a real tree-mutation undo. Given the
project's explicit safety instruction ("if widening the truncate window
creates ANY doubt, keep the refusal"), and that a tree-mutation undo bug
here would silently corrupt a replica (worse than the current safe refusal +
network-restore path), I judged this not implementable correctly and
tested inside the remaining session budget. **I did not change
`classify_tail`/`verify_rollback`'s behavior; the default-deny refusal
stays exactly as it was.** This is the right call per the stated safety bar
and is left as documented future work.

### Gap B — syncup not renegotiated on mid-stream master change

Requires: (1) the `noxu-replica-*` thread (spawned in
`ReplicatedEnvironment::become_replica`, `replicated_environment.rs`
~2600-2670) to hold a live reference to `master_tracker` and poll/subscribe
for a master-name or term change while `ReplicaReceiver::run_until` blocks;
(2) on detecting a change, break out of the receive loop, close the stale
channel, and re-run the SYNCUP handshake (`syncup_with_feeder`/
`syncup_with_feeder_at`) against the NEW master's address before resuming
`ReplicaReceiver::run`. JE equivalent: `Replica` registers a
`MasterChangeListener` with `RepNode`; on notification it breaks
`runReplicaLoop`'s inner streaming loop and re-enters
`ReplicaFeederSyncup` against the new master. The cleanest Rust shape is
probably: give the replica thread's loop an outer `'reconnect: loop` that
(a) resolves the current master address from `group_service`/
`master_tracker` fresh each iteration, (b) runs syncup against it, (c) runs
`ReplicaReceiver::run_until` with a `shutdown`-style flag that ALSO trips
when `master_tracker`'s term/name changes underneath it (this needs a new
atomic "generation" counter bumped by `become_master`/`master_tracker.
set_master`, checked between frames in the receive loop, similar to how
`io_shutdown` is already checked). Not attempted this session — no working
code — kept as an open, documented gap.

### Gap C — periodic DTVLSN flusher daemon

Requires: a new `noxu-<kind>-<node>` daemon thread on the existing
`io_threads`/`io_shutdown` lifecycle (see pattern above), spawned from
`open()` alongside `start_election_driver`/`start_vlsn_persistence_daemon`,
that on a master node periodically (a stability-debounced interval,
matching JE's `targetStableTicks * FEEDER_MANAGER_POLL_TIMEOUT` semantics —
Noxu's config exposes `heartbeat_interval`, used as the tick base) checks
whether `get_dtvlsn()` has advanced since the last flush and, if so, and the
value has been stable for a debounce window, writes a null commit (a
`TxnCommit` WAL record referencing an internal txn id, with `dtvlsn` field
populated) purely to persist the value — mirroring JE's
`FeederManager.DTVLSNFlusher`. This is implemented in this session; see the
commit that follows these notes.
