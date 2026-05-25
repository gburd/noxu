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
