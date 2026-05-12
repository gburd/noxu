# Testing Guide

## Test Categories

### Unit Tests

Unit tests live inside the source file they test, in a `#[cfg(test)]` module at
the bottom of the file. They test a single function or struct in isolation.

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lsn_round_trip() {
        let lsn = Lsn::new(3, 1024);
        assert_eq!(Lsn::from_u64(lsn.as_u64()), lsn);
    }
}
```

### Integration Tests

Integration tests live in `tests/` inside each crate (e.g.,
`crates/noxu-db/tests/`). They test the public API from the perspective of an
external caller. Each test file is a separate compilation unit.

All integration tests that touch the filesystem must create a temporary
directory using the `TempDir` isolation pattern:

```rust
use tempfile::TempDir;

fn open_test_env() -> (TempDir, Environment) {
    let dir = TempDir::new().unwrap();
    let env = Environment::open(dir.path(), EnvironmentConfig::default()).unwrap();
    (dir, env)  // caller must hold TempDir alive for the test's duration
}
```

Never use a fixed path like `/tmp/noxu-test` — tests run in parallel and will
collide.

### Property-Based Tests

Property tests use the `proptest` crate. They live in `#[cfg(test)]` modules
with a `proptest!` macro block. See `crates/noxu-log/tests/` for examples.

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn round_trip_packed_int(v in 0u64..=u64::MAX) {
        let encoded = PackedInt::encode(v);
        assert_eq!(PackedInt::decode(&encoded).unwrap(), v);
    }
}
```

### Fuzz Tests

Fuzz targets live in `tests/fuzz/`. They use `cargo-fuzz` and require nightly:

```bash
cargo +nightly fuzz list
cargo +nightly fuzz run fuzz_log_entry -- -max_total_time=3600
```

The six fuzz targets are: `fuzz_log_entry`, `fuzz_bin_entry`, `fuzz_lsn`,
`fuzz_packed_int`, `fuzz_recovery`, `fuzz_replication`.

## Test Runner

Use `cargo nextest` for all test runs. It is faster than `cargo test`, shows
cleaner output, and respects per-test timeouts from `.config/nextest.toml`:

```bash
cargo nextest run --workspace              # all tests
cargo nextest run -p noxu-txn             # one crate
cargo nextest run -p noxu-rep --no-fail-fast  # keep going past first failure
```

## Naming Conventions

- Unit test functions: `test_<what>_<condition>` (e.g., `test_lsn_overflow_returns_error`)
- Integration test files: `<subsystem>_test.rs` (e.g., `concurrency_test.rs`)
- Property test functions: `round_trip_*`, `invariant_*`, or describe the property

## Test Isolation

Key rules:

1. **No shared state** — each test creates its own `TempDir` and opens a fresh
   `Environment`.
2. **No fixed ports** — replication tests bind to port `0` (OS assigns an
   ephemeral port).
3. **No `sleep`** — use channels, condvars, or retry loops with a deadline.
4. **Always close before asserting** — WAL is flushed on `env.close()` (or when
   the `Environment` is dropped). Do not assert file contents while the env is
   still open.
5. **Drop order matters** — drop `Database` handles before dropping the
   `Environment`. The environment's WAL flush happens on its drop.

## Running the Full Test Suite

```bash
# Matches the CI command exactly
cargo nextest run --workspace --all-features
```

For replication tests (noxu-rep), the suite takes approximately 90 seconds on a
modern workstation due to the election timeout and chaos test durations.

## Debugging a Failing Test

```bash
# Show all stdout/stderr from the test
cargo test -p noxu-rep -- test_name --nocapture

# Enable debug logging
RUST_LOG=noxu_rep=debug cargo test -p noxu-rep -- test_name --nocapture

# Enable full backtraces
RUST_BACKTRACE=full cargo test -p noxu-rep -- test_name --nocapture

# Run the test in isolation (nextest runs each test in its own process)
cargo nextest run -p noxu-rep -E 'test(test_name)'
```

## Adding Tests for a JE Feature

When porting a JE feature, locate its Java tests in `_/je/` under the same
package path as the source (e.g., `_/je/src/com/sleepycat/je/tree/`). Port
each `@Test` method to a Rust `#[test]`. Preserve the test names (translated to
snake_case) and the intent of each assertion.
