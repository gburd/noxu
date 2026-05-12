# Release Checklist

Complete all items before publishing a release.

## Code Quality

- [ ] `cargo fmt --check` passes with no differences
- [ ] `cargo clippy --workspace` produces zero warnings
- [ ] `cargo deny check licenses` passes
- [ ] No `TODO` or `FIXME` comments in shipped code paths
- [ ] All `#[allow(...)]` annotations are justified

## Testing

- [ ] `cargo test --workspace`  -  all tests pass
- [ ] `cargo test --workspace --release`  -  tests pass in release mode
- [ ] Fuzz tests run for at least 1 hour with no crashes
- [ ] Property-based tests pass with increased iterations
- [ ] No performance regressions in `cargo bench` vs. previous release

## Recovery Verification

- [ ] Crash recovery test: write data, kill process, reopen, verify data
- [ ] Checkpoint recovery: verify recovery from various checkpoint states
- [ ] Log cleaning: verify data survives cleaner operations

## Replication Verification

- [ ] Master election completes correctly with 3-node group
- [ ] Replica catches up after master writes
- [ ] Master transfer completes without data loss
- [ ] Network restore recovers from VLSN gaps

## Documentation

- [ ] CHANGELOG.md updated with all notable changes
- [ ] README.md examples compile and run
- [ ] API documentation builds without warnings: `cargo doc --no-deps`
- [ ] ARCHITECTURE.md reflects any structural changes
- [ ] `make docs-check` passes — zero spelling errors, lint violations, broken links
- [ ] mdBook docs reflect any new public API, config parameters, or architectural changes

## Release Process

1. Update version in workspace `Cargo.toml`
2. Update CHANGELOG.md
3. Create git tag: `git tag -a v0.x.y -m "Release v0.x.y"`
4. Push tag: `git push origin v0.x.y`
5. CI builds and publishes crates
6. Verify crates appear on crates.io
