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
```
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
