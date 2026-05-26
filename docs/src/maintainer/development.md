# Development Workflow

## Prerequisites

```bash
# Rust (MSRV 1.85)
rustup toolchain install stable
rustup component add clippy rustfmt

# nextest (faster test runner)
cargo install cargo-nextest --locked

# Documentation
cargo install mdbook --version 0.4.40 --locked
cargo install mdbook-mermaid --version 0.13.0 --locked
npm install -g markdownlint-cli2
cargo install typos-cli --locked

# For chaos/soak tests (Linux only)
# tc and netem are in iproute2 (usually pre-installed)
# The setuid tc_netem_helper binary must be compiled and installed:
cd scripts && make tc_netem_helper && sudo install -m 4755 tc_netem_helper /usr/local/bin/
```

## Build Commands

```bash
cargo build                            # build all crates
cargo build -p noxu-rep --features quic  # build with QUIC transport
cargo test                             # run all tests
cargo nextest run                      # faster test runner (parallel)
cargo nextest run -p noxu-rep          # test a single crate
cargo clippy --workspace --all-targets --all-features -- -D warnings  # full CI lint
cargo fmt -- --check                   # check formatting
cargo doc --workspace --no-deps        # build Rust API docs
make docs                              # build mdBook
```

## Environment Variables

| Variable | Effect |
|---|---|
| `RUST_LOG=noxu_db=debug` | Enable debug logging for the db crate |
| `RUST_LOG=noxu_rep=trace` | Trace-level replication logs |
| `RUST_BACKTRACE=1` | Short backtraces on panic |
| `RUST_BACKTRACE=full` | Full backtraces |
| `TORTURE_SECS=60` | Run torture test for N seconds |
| `TRANSPORTS=tcp` | Constrain torture test to one transport |

## Debugging Tips

### Finding which test is flaky

```bash
cargo nextest run -p noxu-rep --test-threads=1 --retries=0 2>&1 | grep -E "FAIL|PASS"
```

### Adding trace logging

```rust
log::trace!("enter BIN slot search: key={:?} prefix_len={}", key, prefix_len);
```

Then run with `RUST_LOG=noxu_tree=trace cargo test`.

### Reproducing a race condition

```bash
for i in $(seq 1 100); do
    cargo test -p noxu-txn -- test_concurrent_put --nocapture 2>&1 | tail -3
done
```

### Profiling with flamegraph

```bash
cargo install flamegraph
CARGO_PROFILE_BENCH_DEBUG=true cargo flamegraph --bench write_bench -- --bench
```

### Memory profiling

```bash
valgrind --tool=massif --pages-as-heap=yes target/debug/examples/basic
ms_print massif.out.<pid>
```

## Common Patterns

### Pattern: background daemon thread

```rust
// In EnvironmentImpl::start_daemons():
let handle = noxu_util::DaemonThread::spawn("cleaner", move || {
    while !shutdown.load(Ordering::Relaxed) {
        cleaner.work();
        std::thread::sleep(cleaner_interval);
    }
});
self.cleaner_handle = Some(handle);
```

### Pattern: latch coupling descent

```rust
let root = self.root.read();
let bin = {
    let child = root.find_child(key);
    let child_guard = child.read();
    drop(root);  // release parent
    child_guard.find_bin(key)
};
let mut bin_guard = bin.write();
bin_guard.insert(key, value);
```

### Pattern: undo record on write

```rust
// Before modifying, save the before-image for undo:
let before_lsn = slot.ln_lsn;
let before_data = slot.data.clone();
txn.add_undo_record(UndoRecord { db_id, key, before_lsn, before_data });
// Then apply the modification
slot.data = new_data;
slot.dirty = true;
```

## IDE Setup

### rust-analyzer (VS Code / Neovim)

Add to `settings.json`:

```json
{
    "rust-analyzer.cargo.features": "all",
    "rust-analyzer.checkOnSave.command": "clippy"
}
```

### IntelliJ IDEA / CLion (via IntelliJ Rust plugin)

Enable "Use cargo check" in Rust plugin settings. Add `--features quic` to
the additional arguments field for full feature coverage.
