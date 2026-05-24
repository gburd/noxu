# Operational Runbooks

Actionable procedures for the four most common production-incident shapes
in a deployed Noxu DB. Each runbook follows the same structure:

- **Symptoms** — what triggers attention (alerts, log lines, metric trends)
- **Diagnose** — commands and queries to confirm the diagnosis
- **Mitigate** — immediate intervention to stop the bleeding
- **Resolve** — follow-up to remove the underlying cause
- **Escalate** — when to wake another human up

The runbooks assume you have the standard monitoring set up
([Monitoring](monitoring.md)) and have configured the
`ExceptionListener` to forward `NoxuError::EnvironmentFailure` events
to your alerting system.

---

## Runbook 1 — Recovery loop

The environment opens, runs WAL recovery, then panics or returns a
fatal error during the redo/undo phase. On the operator's reopen the
same thing happens. The environment never reaches the "open and
serving" state.

### Symptoms

- `Environment::open()` returns `NoxuError::EnvironmentFailure {
  reason: LogChecksum | BtreeCorruption | LogReadError, .. }`
- Process restart loop (systemd, k8s) is restarting the service every
  few seconds with the same error in the log
- `log::error!` lines from `noxu-recovery` saying "redo failed at
  lsn=…" or "undo … failed at abort_lsn=…", repeatedly, in the same
  region of the log
- Disk free space is OK (recovery is not running out of room)

### Diagnose

1. **Capture the corruption scope.** Tail the recovery log and
   identify the first failing LSN and the last successful LSN. If
   the failures cluster around a single file number, the corruption
   is localised to one log file.

   ```text
   noxu-recovery: redo failed at lsn=Lsn(file=42, offset=…), …
   noxu-recovery: undo (embedded before-image) failed at lsn=Lsn(file=42, offset=…), …
   ```

2. **Check the file integrity.** Each `.ndb` file in the
   environment directory has its own integrity. List file sizes:
   ```sh
   ls -lS /path/to/env/*.ndb
   ```
   A file truncated to 0 bytes or much smaller than its peers is
   the most common cause.

3. **Check the file's checksum directly** using the standalone
   tool (when shipped):
   ```sh
   noxu-fsck /path/to/env/00000042.ndb
   ```

### Mitigate

The environment is unwriteable. Stop the restart loop first
(`systemctl stop noxu` / scale the deployment to 0) before
attempting any repair. A restart-loop service will keep the disk
under load and prevent forensics.

### Resolve

Three paths in order of preference:

1. **Restore from a replica** (if you have one):
   - Stop the broken node.
   - Configure the environment for restore via
     `RepConfig::with_network_restore_source(source_node, addr)`.
   - Start the node; it will pull the entire env_home from the
     source. Recovery time is bounded by network bandwidth × env
     size.

2. **Restore from backup**: replace the env directory with the
   most recent `BackupManager` snapshot. The
   `transaction_id_at_backup_time` field on the snapshot tells you
   how much committed data you're dropping.

3. **Manual log truncation** (last resort, no replica, no
   backup):
   - Copy the env directory to a forensics location.
   - Identify the first corrupted file (e.g. `00000042.ndb`).
   - Move that file and all later files (`00000043.ndb`,
     `00000044.ndb`, …) out of the env directory.
   - Reopen. Recovery will replay up to the boundary and start
     over. **All transactions committed in the moved files are
     lost**.

### Escalate

- If three rounds of `Mitigate → Resolve` produce different fatal
  errors each time → the environment is corrupted across multiple
  files; only restore-from-replica or restore-from-backup will
  work. Wake the on-call.
- If recovery succeeds but reads then return `BtreeCorruption`,
  the on-disk B-tree is inconsistent at the page level → wake
  noxu-tree's owner.

---

## Runbook 2 — Cleaner backlog

Disk usage is growing faster than the cleaner can reclaim, even
though logical data volume is stable. Free space trends downward
over hours/days. Without intervention the disk will fill and the
environment will fail with
`EnvironmentFailureReason::DiskLimitExceeded`.

### Symptoms

- Disk free space dropping at a sustained rate
- `s.cleaner.deletions` plateauing or growing slower than
  `s.log.n_log_writes`
- `s.cleaner.bytes_pending_to_delete` (when exposed) growing
- File count in env directory (`ls *.ndb | wc -l`) growing
- The cleaner's util-tracker shows the lowest-utilisation file
  staying above `cleaner_min_utilization` (no targets to clean)

### Diagnose

1. **Compute the cleaner gap.** Snapshot
   `s.log.n_log_writes` and `s.cleaner.deletions` 60 seconds apart;
   if writes outpace deletions by more than 2:1 sustained, the
   cleaner is behind.

2. **Inspect file utilisation.** Each `.ndb` file's utilisation is
   logged at cleaner-run boundaries:
   ```text
   cleaner: file 00000042.ndb utilisation = 87.3%, threshold = 50%, skip
   ```
   If most files are above the threshold but the disk is filling,
   the cleaner is correctly idle and the writer rate is the
   problem.

3. **Check for cleaner shutdown / disabled state.**
   `s.cleaner.runs == 0` over a 5-minute window with active
   writes means the cleaner thread is dead or disabled. Check the
   environment config for `noxu.cleaner.enabled` and
   `noxu.cleaner.threads`.

### Mitigate

1. **Throttle writers.** If you control the writer (your
   application), back off — even briefly — so the cleaner can
   close the gap.
2. **Lower `cleaner_min_utilization`** (e.g. from 50 → 30) at
   runtime via the environment-mutable config to make MORE files
   eligible for cleaning. The cleaner will work harder.
3. **Bump `cleaner_threads`** if a single thread is the
   bottleneck — observable as 100% CPU on one of the cleaner
   worker threads.

### Resolve

- If symptoms recur, the writer rate is sustained above the
  cleaner's throughput. Either:
  - Increase `cleaner_threads` permanently
  - Provision a faster disk (cleaner is read-modify-write bound on
    log file I/O)
  - Reduce write rate
- If utilisation stays stuck at high values across all files (so
  cleaner has no targets), the data set is genuinely write-heavy
  with low overwrite — cleaner will never gain ground. Plan for a
  larger disk.

### Escalate

- Disk free space below 10% **and** cleaner gap > 5:1: page the
  on-call. Forty minutes of head room before
  `DiskLimitExceeded` triggers.
- `s.cleaner.runs == 0` for more than 30 minutes with active
  writes: the cleaner has crashed; wake the on-call.

---

## Runbook 3 — Election thrash

Replicated cluster cannot settle on a master. Elections complete
but the elected master loses its term within seconds, triggering a
new election. Throughput is near zero because the master's role is
constantly being re-acquired.

### Symptoms

- `s.rep.elections_initiated` increments every few seconds
- `RepNode::get_state()` cycling between `Master` ↔ `Replica` /
  `Detached`
- `phi_detector` log lines reporting `phi >
  suspicion_threshold` for the current master from multiple
  observers
- Group_service log messages: `master {} demoted by quorum,
  starting new election`
- Application throughput collapses; commit latency spikes to
  multiple seconds

### Diagnose

1. **Identify whether the network is the cause.**
   ```sh
   for peer in node1 node2 node3; do
     ping -c 5 $peer
   done
   ```
   Sustained packet loss > 5% or RTT variance > 100ms is enough to
   trip phi_detector at default thresholds.

2. **Check phi values directly.** The `MasterTracker` exposes
   `phi(peer_name)`:
   ```rust
   for peer in cluster.peers() {
       eprintln!("{}: phi = {:.2}", peer, tracker.phi(peer));
   }
   ```
   If most pairs show phi > 8 (default suspicion threshold), the
   network is the problem.

3. **Check master CPU saturation.** A master under sustained CPU
   pressure can't serve heartbeats fast enough. Look for the
   master thread at 100% CPU.

4. **Check disk pressure on the master.** A master with a slow
   fsync (full disk, slow underlying storage, encryption layer
   sync delay) will appear unresponsive. Compare
   `s.log.fsync_p99_us` between candidate masters.

### Mitigate

1. **Raise the suspicion threshold** (`phi_threshold`) on all
   nodes from the default 8.0 to 12.0 — buys ~2× the time before
   declaring a master suspect. Survives transient network jitter.
2. **Pin the desired master** by setting
   `RepConfig::set_designated_primary(true)` on one node and
   `false` on the others. The designated primary will resist
   demotion under flapping conditions.
3. **Reduce replication payload** by raising the master's
   `noxu.txn.commit_sync_buffer_size` so commits are batched
   harder — gives the master more headroom for heartbeats.

### Resolve

- If phi values were the cause: investigate the network. Likely
  causes: a flapping switch, a misconfigured QoS rule, an MTU
  mismatch on a replication link, or a noisy neighbour saturating
  the link.
- If master CPU was the cause: profile what's holding the master
  thread. Common culprits: oversized commit batches, eviction
  storm, checkpoint running too long.
- If master disk was the cause: investigate fsync latency. Likely
  causes: filesystem barrier, backup/snapshot in progress on the
  same volume, encryption layer.

### Escalate

- More than 3 elections in 60 seconds with phi values steady
  below threshold: page the noxu-rep on-call. There may be a real
  bug in the election protocol.
- A master is elected but cannot serve writes (commits return
  `MasterTransferInProgress` or `NotCurrentMaster`): the
  cluster's view of master is out of sync — wake on-call.

---

## Runbook 4 — Slow checkpoints

Checkpoint duration grows over time. Eventually checkpoints take
longer than the interval between them, the dirty page set grows
unbounded, and recovery time after an unclean shutdown explodes
from minutes to hours.

### Symptoms

- `s.checkpoint.last_duration_ms` consistently increasing
- `s.checkpoint.checkpoints` not advancing during a snapshot
  window (running long)
- `EnvironmentFailureReason::CheckpointFailed` — checkpointer
  thread errored out
- Application latency spikes during checkpoint: writes block on
  flush
- Recovery on a recent restart took noticeably longer than the
  same workload's recovery a month ago

### Diagnose

1. **Read recent checkpoint stats.**
   ```rust
   let s = env.get_stats()?;
   eprintln!("last={}ms, count={}, dirty_in_count={}",
       s.checkpoint.last_duration_ms,
       s.checkpoint.checkpoints,
       s.evictor.in_count_at_last_checkpoint);
   ```
   `in_count_at_last_checkpoint` is the size of the in-memory IN
   set the checkpointer had to walk. If this is growing while
   throughput is steady, evictor isn't keeping up with cache
   pressure and is leaving dirty INs in the cache.

2. **Look at evictor pressure.**
   `s.evictor.nodes_evicted` rising fast while
   `s.cache_utilization_percent()` stays above 85% means the
   evictor is constantly working but cache is full.

3. **Check disk write throughput during the slow checkpoint.**
   `iostat -x 1` on the env volume; if `await` (millis per I/O)
   spikes during checkpoint, the disk is the bottleneck.

### Mitigate

1. **Reduce checkpoint scope** by lowering
   `noxu.checkpointer.kbytes_interval` or
   `noxu.checkpointer.txn_count_interval`. Smaller checkpoints
   take less time but happen more often. Net effect on disk
   throughput: roughly the same. Net effect on recovery time
   after crash: shorter.
2. **Bump cache size** (`noxu.cache.size`). The fastest fix when
   evictor is the bottleneck.
3. **Reduce write rate** temporarily.

### Resolve

- If `in_count_at_last_checkpoint` is the root cause: the
  evictor + checkpointer are fundamentally not keeping pace with
  the writer. Permanent fix is more cache, faster disk, or a
  lower writer rate.
- If disk is the root cause: investigate. Common: noisy
  neighbour on shared storage, a snapshot/backup running on the
  same volume, encryption layer pulling its weight only during
  fsync.
- If a single checkpoint takes hours but the next is fast: the
  workload is bursty (one tx generated MASSIVE dirty IN set).
  Either scope the workload differently or accept the burst.

### Escalate

- Recovery time at the next unclean shutdown crosses your SLA
  (e.g. > 5 minutes for a service that promises 99.9% uptime):
  page the noxu-engine on-call.
- Checkpoint duration > checkpoint interval (the checkpointer
  never finishes a cycle): wake on-call. Risk of disk fill
  because the cleaner can't reclaim files held by an in-progress
  checkpoint.

---

## See also

- [Monitoring](monitoring.md) — what fields to scrape and at what
  intervals
- [Performance Tuning](tuning.md) — config knobs referenced
  throughout these runbooks
- [Recovery Procedures](recovery-ops.md) — for the corruption /
  fatal-error case (Runbook 1's deepest path)
- [Backup](backup.md) — how to take a `BackupManager` snapshot
  for the Runbook 1 fallback
