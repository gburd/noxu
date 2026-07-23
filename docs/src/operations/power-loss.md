# Power-Loss Testing

Noxu DB ships two power-loss test layers. The first runs in
ordinary `cargo test`; the second requires a dedicated VM and
documents itself as a hand-run procedure rather than an
auto-executed test.

## Layer 1 — `pkill -9` torn-write sweep (in-tree, automated)

`crates/noxu-db/tests/power_loss_sweep.rs` runs the same
`crash_worker` subprocess used by `crash_recovery_test.rs`,
SIGKILLs it at randomised times across a 1000-iteration sweep,
and asserts:

- recovery returns `Ok` and does not panic
- every committed key is present with its original value
- no uncommitted key is visible
- the recovered key set is a **prefix** of the committed
  sequence (recovery never drops an earlier commit while keeping
  a later one)

A short smoke variant (20 iterations) runs in CI; the full sweep
is `#[ignore]` because it takes 30-60 minutes:

```sh
# Smoke (CI-friendly, ~30s):
cargo test -p noxu-db --test power_loss_sweep --release \
    power_loss_sweep_smoke

# Full 1000-iteration sweep:
cargo test -p noxu-db --test power_loss_sweep --release \
    -- --ignored --nocapture
```

### What this layer **does not** test

`SIGKILL` (`kill -9`) on a process from within the same OS kills
the process but does **not** kill the OS. The kernel's page
cache continues to flush dirty pages after the process is dead,
so a process that called `write()` but not `fsync()` will still
have its bytes hit the disk eventually. A real power loss drops
the page cache mid-flight and exposes recovery to entries that
were `write()`'d but never reached disk.

For that, see Layer 2.

## Layer 2 — qemu whole-VM kill (manual procedure)

This procedure simulates a real power loss by killing the qemu
process hosting the test VM. The host kernel does not flush the
guest's page cache when the qemu process dies — it just
discards the qemu memory. Result: recovery faces a disk image
with exactly the bytes that were `fsync`'d up to the kill
moment, no more.

### Prerequisites

- `qemu-system-x86_64` installed on the host
- A bootable Linux VM image with a recent Rust toolchain
- The VM image's disk uses `cache=none,aio=native` so writes go
  through the host page cache only on `fsync` boundaries

### Procedure

1. **Start the test VM with deterministic disk image:**

   ```sh
   qemu-system-x86_64 \
       -name noxu-power-loss \
       -machine q35,accel=kvm \
       -smp 2 -m 2G \
       -drive file=disk.qcow2,if=virtio,cache=none,aio=native \
       -monitor unix:/tmp/qemu-mon.sock,server,nowait \
       -netdev user,id=n0,hostfwd=tcp:127.0.0.1:2222-:22 \
       -device virtio-net-pci,netdev=n0
   ```

2. **Build and copy `crash_worker` into the VM:**

   ```sh
   cargo build -p noxu-db --bin crash_worker --release
   scp -P 2222 \
       target/release/crash_worker \
       root@127.0.0.1:/usr/local/bin/
   ```

3. **Inside the VM**, start the worker:

   ```sh
   ssh -p 2222 root@127.0.0.1 \
       'NOXU_CRASH_DIR=/var/lib/noxu \
        NOXU_CRASH_MODE=committed_then_uncommitted \
        /usr/local/bin/crash_worker'
   ```

4. **From the host**, after a delay sampled from
   `[0, 250]ms`, send a `quit` to the qemu monitor (kills the
   qemu process, does NOT shut the guest down cleanly):

   ```sh
   echo 'quit' | nc -U /tmp/qemu-mon.sock
   ```

5. **Restart the VM** with the same disk image:

   ```sh
   # Same qemu command line as step 1
   qemu-system-x86_64 ...
   ```

6. **Inside the restarted VM**, reopen the env and run the
   recovery checks (use `power_loss_sweep`'s `reopen_db` +
   verification logic, packaged as a `#[bin]` for in-VM use).

### Why we don't auto-run this

Running 1000 iterations of qemu start/kill/restart against a
prepared disk image is ~3 hours of wall time per run, requires
~10 GB of intermediate disk space, and has dependencies that
fall outside the workspace's normal `cargo test` reach (qemu,
ssh, a prepared VM image). It is the right test to run as a
quarterly release-gate for v1.x.0 versions, not as a per-PR
gate.

### What we test instead

Layer 1's `power_loss_sweep_smoke` runs in CI on every PR and
catches the most common torn-write classes. Layer 1's full
1000-iteration sweep runs as a `#[ignore]` test that should be
exercised by the developer running a release candidate.

The qemu Layer 2 is what would make Noxu DB *attest* to power
loss correctness; without that step, the README and
documentation should not claim "survives power loss" — only
"survives process crash."

## Result interpretation

| Failure mode | Layer that catches it |
|---|---|
| Worker calls `write()` then is killed before `fsync()`; recovery sees a partial entry | Layer 1 — exercised |
| Worker calls `commit()` (which fsyncs the WAL) then is killed; recovery must materialise the commit | Layer 1 — exercised |
| Page cache contains a dirty WAL entry that has not been `fsync`'d when the OS dies; recovery must NOT see that entry | **Layer 2 only** |
| Filesystem journal replay produces a partial-sector write; recovery must detect the bad CRC | **Layer 2 only** |
| `fdatasync()` ordering: a commit's WAL entry made it to disk but the file metadata that lets the recovery tool find that file did not | **Layer 2 only** |

Items only catchable by Layer 2 are real and matter. The Layer 1
sweep + the architectural choice of always-fsync-before-commit
gives a strong correctness baseline; Layer 2 closes the loop.

## Layer 3 — `dm-log-writes` / CrashMonkey block-level replay (methodology)

The qemu Layer 2 kills the VM at *wall-clock* moments; it cannot
replay **every** crash point or check the invariant at each one. The
gold standard for filesystem/WAL crash-consistency is the Linux
`dm-log-writes` device-mapper target (as used by `xfstests` and the
CrashMonkey / ACE tooling from the OSDI'18 "Barrier-Enabled IO Stack"
line of work). It records every block write and every flush/FUA
barrier to a log device, then lets you **replay the block stream up to
any barrier** and mount the result — so you can check recovery at
*every* durability boundary the WAL emitted, not just at random times.

### What it needs (why it is not in `cargo test`)

- **root** (to create device-mapper targets)
- a **real block device** (or loopback file) for the data + log devices
- kernel `dm-log-writes` support (`CONFIG_DM_LOG_WRITES`)
- the `dm-log-writes` userspace tools (`src/log-writes/replay-log`
  from `xfstests`, or the CrashMonkey harness)

This is a CI-infrastructure project (dedicated privileged runner), not
a per-PR unit test. It is documented here as the plan.

### Procedure

1. **Create the logging device over the data device:**

   ```sh
   # $DATA = the block device Noxu's env directory lives on
   # $LOG  = a separate device to record the write stream
   sectors=$(blockdev --getsz "$DATA")
   dmsetup create noxu-logwrites --table \
     "0 $sectors log-writes $DATA $LOG"
   mkfs.ext4 /dev/mapper/noxu-logwrites   # or xfs
   mount /dev/mapper/noxu-logwrites /mnt/noxu
   ```

2. **Run a committing workload against `/mnt/noxu`:**

   ```sh
   NOXU_CRASH_DIR=/mnt/noxu \
   NOXU_CRASH_MODE=committed_then_uncommitted \
   crash_worker &
   # let it run through several commit/fsync cycles, then stop it
   ```

3. **Snapshot the write log, then replay to every flush boundary:**

   ```sh
   umount /mnt/noxu
   dmsetup remove noxu-logwrites
   # For each recorded flush/FUA mark i:
   for i in $(seq 0 $NMARKS); do
     replay-log --log "$LOG" --replay "$DATA_COPY" --limit-mark $i --fsck \
       'mount + reopen Noxu env + run the recovery check below'
   done
   ```

4. **Invariant checked at every replayed boundary** (the whole point):

   > Reopen the Noxu env on the partially-replayed image. Recovery must
   > succeed (or fail-stop cleanly), and **every transaction whose
   > `commit()` returned before the replayed flush boundary must be
   > present and readable, with no committed transaction lost and no
   > uncommitted transaction visible.** The recovered committed set must
   > be a *prefix* of the commit order (recovery never keeps a later
   > commit while dropping an earlier one).

   This is the same invariant the in-tree torn-write tests
   (`torn_write_policy_test.rs`, `crash_recovery_test.rs`) assert at the
   application layer — Layer 3 asserts it at *every* block-level
   durability boundary the kernel actually emitted.

### Relationship to the runnable subset

The torn-write + write-error tests that DO run in CI
(`torn_write_policy_test.rs`, `crash_recovery_test.rs`'s
`test_torn_write_truncated_entry_recovered`, `power_loss_sweep_smoke`,
and `noxu-log`'s `test_real_write_error_invalidates_and_is_not_swallowed`)
are the runnable subset of this methodology: they exercise the
torn-tail-truncate and write-error-invalidate paths deterministically
without needing root or a block device. Layer 3 is the full rig that
would let Noxu DB *attest* to power-fail correctness across every
barrier; until it runs on a privileged CI runner, the honest claim is
"survives process crash and torn final write, with a documented
fail-stop stance on WAL write errors" — not a general power-loss
attestation. See the current honest status in
[Known Limitations](known-limitations.md).

### Write-error (fsyncgate) stance

What Noxu does when the `fdatasync`/`pwrite` **itself returns an
error** (as opposed to a torn write from an abrupt stop) is a separate
question from crash replay. Noxu's stance is **fail-stop**: the
environment is permanently invalidated and refuses further writes; it
never retries the sync (retrying is unsafe on Linux — a failed
`fsync` may have already dropped the dirty page). The policy, its
rationale, and its test coverage are documented in
[SAFETY.md](https://codeberg.org/gregburd/noxu/src/branch/main/SAFETY.md)
§ "WAL write-error handling" and summarised in
[Known Limitations](known-limitations.md).
