# Chaos and Soak Testing

Noxu DB's replication correctness is validated under adversarial network
conditions using Linux's `tc netem` kernel module.

## Infrastructure

### tc_netem_helper

A small setuid C binary (`scripts/tc_netem_helper.c`) that wraps `tc qdisc`
commands. It runs as root (setuid) so test code can apply netem rules without
needing sudo.

Install:

```bash
cd scripts && make tc_netem_helper
sudo install -m 4755 tc_netem_helper /usr/local/bin/
```

### TcNetemGuard

`crates/noxu-rep/src/net/netem.rs` provides `TcNetemGuard`, an RAII guard
that applies netem rules on construction and removes them on drop.

Key methods:

- `overlay(opts)` — add/replace netem rules (packet loss, delay, corruption, duplicate)
- `overlay_calm()` — remove all netem rules (clear faults)
- `overlay_burst_loss(p13, p31)` — Gilbert-Elliott correlated loss model
- `overlay_bandwidth_cap(kbps)` — throttle bandwidth
- `overlay_slot(min_ms, max_ms)` — slotted delivery (burst delivery timing)
- `overlay_queue_bloat(delay_ms, limit_pkts)` — simulate bufferbloat

## Torture Test (`torture_test.rs`)

**File**: `crates/noxu-rep/tests/torture_test.rs`

The torture test runs multi-round replication elections under continuous chaos
injection. Each round:

1. Starts a 3 or 5 node cluster
2. Injects a `ChaosPhase`
3. Runs a configurable number of elections
4. Checks invariants after each election

### ChaosPhase Enum

| Variant | Description |
|---|---|
| `Calm` | No network faults (baseline) |
| `PacketLoss5pct` | 5% random packet loss |
| `PacketLoss20pct` | 20% random packet loss |
| `Delay50ms` | 50ms fixed latency |
| `Corrupt1pct` | 1% packet corruption |
| `Duplicate2pct` | 2% packet duplication |
| `Reorder10pct` | 10% reordering |
| `CombinedFault` | Loss + delay + corrupt combined |
| `PeerJoin` | Add a new node mid-run |
| `PeerLeave` | Remove a non-master node mid-run |
| `CapacityChange` | Update read/write capacity for a random node |
| `ClusterGrow` | Grow cluster from 3→5 nodes |
| `ClusterShrink` | Shrink cluster from 5→3 nodes |
| `BurstLoss` | Gilbert-Elliott correlated burst loss |
| `BandwidthCap` | 10 kbps bandwidth cap |
| `SlottedDelivery` | 20–40ms slotted delivery |
| `QueueBloat` | 50ms delay + 100-packet queue |

### Invariants Checked

After every election:

1. **Safety**: at most one winner per term (no split-brain)
2. **VLSN monotonicity**: each node's applied VLSN only increases
3. **No panic**: test runner catches `SIGABRT` and marks as violation
4. **Quorum validity**: after membership changes, `phase1+phase2>n` holds

### Running the Torture Test

```bash
# Default run (60 seconds)
cargo nextest run -p noxu-rep -- torture

# Extended run
TORTURE_SECS=1800 cargo nextest run -p noxu-rep -- torture

# Specific transport
TRANSPORTS=tcp cargo nextest run -p noxu-rep -- torture
TRANSPORTS=quic cargo nextest run -p noxu-rep -- torture
TRANSPORTS="tcp quic mix" scripts/torture_all.sh
```

### Interpreting Violations

A violation means an invariant failed. The torture test outputs:

```text
Round 42: violations=1 (split_brain detected at term=5)
```

Zero violations across 10,000+ rounds is the target. Any violation is a
critical bug and must be fixed before merging.

## Soak Test (`soak.sh`)

**File**: `scripts/soak.sh`

Runs `torture_all.sh` for 6 hours across all transport configurations. Used
before releases to validate correctness under extended chaos.

```bash
SOAK_SECS=21600 scripts/soak.sh
```

The soak test checks for violations with the regex `violations=[1-9]` (not
just `violations=0`) to avoid false positives from log lines containing
`violations=0` text.

## Known Soak Bugs Fixed

| Bug | Root cause | Fix |
|---|---|---|
| TCP hang under 5% loss | `set_read_timeout(None)` before payload read | `timeout.max(30s)` in channel.rs |
| QUIC PMTUD assertion abort | netem corrupts PMTUD probes → quinn `mtud.rs:88` assert | `mtu_discovery_config(None)` |
| TCP SYN hang under loss | `TcpStream::connect()` no timeout → OS SYN retry ≤127s | `connect_timeout(30s)` |

All three were found in the 6-hour soak (commit `018d314`) with zero
replication correctness violations across ~6,500 rounds.

## Adding New Chaos Phases

1. Add a variant to `ChaosPhase` in `torture_test.rs`
2. Update `rng.gen_range(0u32..<N>)` to include the new variant
3. Add a match arm calling the appropriate `TcNetemGuard` method
4. If the phase changes cluster membership, update the `members: Vec<usize>`
   tracking variable and verify invariants hold after the change

## HA soak + fault-injection plan (Jepsen-style)

The external review's standing recommendation for the replication stack is
weeks-long soaks plus Jepsen-style fault injection with a
linearizability-class checker. This section is the documented **plan** and the
**honest current status** — not something run per-PR.

### Honest current status

| Item | Status |
|---|---|
| tc-netem chaos torture (`torture_test.rs`) | **Done, in CI.** 16 fault phases, safety + VLSN-monotonicity + no-panic + quorum invariants after every election. |
| 6-hour soak (`soak.sh`) across all transports | **Done once** (commit `018d314`): ~6,500 rounds, **found 3 real bugs** (TCP hang, QUIC PMTUD abort, TCP SYN hang), zero replication-correctness violations after the fixes. |
| Weeks-long continuous soak | **Pending.** Not yet run. JE HA has orders of magnitude more exposure; a single 6-hour run is a floor, not a ceiling. |
| Jepsen-style linearizability checker on acked writes | **Pending / planned** (this section). The torture test checks *election* safety (no split-brain) and VLSN monotonicity; it does not yet run an external key-value workload through a Knossos/Elle-class history checker. |

Until the weeks-long soak + linearizability checker run, the honest claim is:
*"replication validated by a 6-hour multi-transport chaos soak (3 bugs found
and fixed) plus continuous per-PR election-safety torture; extended-duration
and linearizability-history validation are planned, not yet done."*

### Faults to inject

The tc-netem phases already cover the network-degradation axis. The
Jepsen-style plan adds **node/process/clock** faults driven against a running
key-value workload:

| Fault | How | Already have? |
|---|---|---|
| Network partition (symmetric + asymmetric) | `TcNetemGuard` drop-all between node subsets; iptables for asymmetric | partial (loss %, not clean partitions) |
| Packet loss / delay / corruption / reorder / dup | `TcNetemGuard` (`overlay_*`) | **yes** (`ChaosPhase`) |
| Master kill (SIGKILL) + failover | kill the master process mid-workload, force re-election | partial (election churn; not a killed master under load) |
| Replica kill + rejoin (network restore) | kill a replica, restart, verify catch-up | partial |
| Clock skew | per-node offset via `libfaketime` or a `SimClock` seam | **no** (planned) |
| Disk-full / WAL write error on a node | `noxu-log` `faultdisk` `DiskFull` on one node | **no on rep path** (planned) |
| Pause (SIGSTOP) / GC-pause simulation | SIGSTOP the master for > election timeout | **no** (planned) |

### Invariants to check (against an external client history)

A Jepsen-style run records the real-time history of client operations
(`invoke`/`ok`/`fail`/`info`) against the cluster and checks:

1. **No lost acked commit after failover.** Every write whose `commit()`
   returned `Ok` (the client got an ack) must be readable on the surviving
   quorum after any master kill / partition / rejoin. This is the headline
   durability invariant — an acked commit is never lost.
2. **Linearizability of acked operations.** The observed history of
   acked reads and writes must be linearizable w.r.t. a single register /
   key-value model (Knossos/Elle-class checker). No stale read of a value
   older than one a prior acked read already returned on the same key.
3. **No split-brain.** At most one master per term accepts writes (already
   checked structurally by `torture_test.rs`; the Jepsen run confirms it
   under a live write workload, not just elections).
4. **Monotonic VLSN per node** (already checked): a node's applied VLSN
   never regresses.
5. **Clean fail-stop on unrecoverable fault.** A node hitting a WAL write
   error (fsyncgate stance) invalidates itself and drops out of the quorum
   rather than serving possibly-non-durable data (cross-refs `SAFETY.md`
   § "WAL write-error handling").

### Harness plan

- **Workload:** a multi-client key-value workload (N clients doing
  `put`/`get` on a keyspace with `COMMIT_SYNC`) recording a real-time
  history with per-op invoke/complete timestamps and the ack outcome.
- **Nemesis:** a fault scheduler that composes the faults above (reuse
  `TcNetemGuard` for network faults; add process-kill and
  `libfaketime`/`SimClock` for node/clock faults).
- **Checker:** feed the recorded history to an Elle/Knossos-class
  linearizability checker (external tool) for invariants 1–2; assert
  invariants 3–5 inline from the cluster state as the torture test already
  does.
- **Duration:** run continuously for the target soak window (start at 24 h,
  extend toward the review's weeks-long recommendation) with the nemesis
  cycling faults; any invariant violation is a release blocker.

### Running the (current) soak today

```bash
# 6-hour multi-transport chaos soak (the run that found the 3 bugs):
SOAK_SECS=21600 scripts/soak.sh

# Extend the torture duration (single transport) toward a longer soak:
TORTURE_SECS=86400 cargo nextest run -p noxu-rep -- torture
```

The linearizability-checker workload + node/clock nemesis above are the
not-yet-built additions; the network-fault torture + duration knobs exist
today.
