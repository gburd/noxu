# Testing

Noxu DB has 5,000+ tests across 19 crates. This page explains the testing
approach, conventions, and how to run different test categories.

## Test Runner

Use `cargo-nextest` for faster parallel execution:

```bash
cargo install cargo-nextest --locked
cargo nextest run                        # all tests
cargo nextest run -p noxu-rep           # one crate
cargo nextest run stream::feeder         # filter by test name
cargo nextest run --no-fail-fast        # continue after first failure
```

Configuration: `.config/nextest.toml` sets timeouts and retry behaviour.

## Test Categories

### Unit Tests

In-module `#[cfg(test)]` blocks. Located at the bottom of each source file.
Test a single function or type in isolation.

```rust
// Example: crates/noxu-txn/src/lock_manager.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_read_compatible() {
        let lm = LockManager::new(LockManagerConfig::default());
        lm.lock(1, 0, b"key", LockType::Read, Duration::from_secs(1)).unwrap();
        lm.lock(2, 0, b"key", LockType::Read, Duration::from_secs(1)).unwrap();
    }
}
```

### Integration Tests

In `crates/<crate>/tests/` directory. Test crate-level behaviour using the
public API.

```bash
cargo nextest run -p noxu-db             # all noxu-db integration tests
cargo nextest run -p noxu-rep            # all noxu-rep tests (623 tests)
```

### Property-Based Tests

Uses `proptest` for randomized inputs. Located in `tests/` directories.
Example: `crates/noxu-rep/tests/chaos_test.rs` uses proptest for election
property verification.

```bash
cargo nextest run -p noxu-rep chaos_test
```

### Fuzz Tests

Located in `tests/fuzz/`. Uses `cargo-fuzz` / `libFuzzer`.

```bash
cargo fuzz list                          # list fuzz targets
cargo fuzz run fuzz_log_entry            # run a specific fuzz target
```

## Test Naming Conventions

- Unit tests: `test_<what_is_being_tested>_<condition>`
  - `test_read_read_compatible`
  - `test_write_blocks_reader_until_commit`
- Integration tests: `test_<scenario>_<expected_outcome>`
  - `test_basic_crud_roundtrip`
  - `test_crash_recovery_preserves_committed_data`
- Property tests: `prop_<invariant_holds>`
  - `prop_election_produces_single_winner_per_term`

## Test Isolation

- Each test that uses a database opens a `tempfile::TempDir` environment.
- Never use a shared environment across tests (thread-safety issues).
- Use `drop(db); drop(env)` (not `env.close()`) for proper WAL flush in tests.
  `env.close()` fails if db handles are still open.

```rust
// Correct pattern:
let dir = tempfile::tempdir().unwrap();
let env = Environment::open(dir.path(), cfg).unwrap();
let db  = env.open_database(None, Some("test"), DatabaseConfig::default()).unwrap();
// ... do test ...
drop(db);
drop(env);  // or env.close() after drop(db)
```

## Replication Tests

`crates/noxu-rep/tests/` contains:
- `chaos_test.rs` — election correctness under network faults
- `torture_test.rs` — full soak test with tc netem chaos injection

See [Chaos and Soak Testing](chaos-soak-testing.md).

## CI Test Command

```bash
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace
cargo doc --workspace --no-deps
make docs-check
```

All must pass (zero warnings, zero failures) before a PR can merge.
