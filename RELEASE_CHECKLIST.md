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
2. Bump the internal `noxu-*` entries under `[workspace.dependencies]` to an
   exact pin of the new version (for example `version = "=X.Y.Z"`). The internal
   crates are lock-step versioned; a caret requirement (for example `"7.10.1"`)
   lets a published crate resolve an older sibling from crates.io that lacks the
   new release's internal APIs, so the exact pin is required. Leave the external
   `quoracle` requirement unchanged.
3. Update CHANGELOG.md
4. Create git tag: `git tag -a v0.x.y -m "Release v0.x.y"`
5. Push tag: `git push origin v0.x.y`
6. CI builds and publishes crates
7. Verify crates appear on crates.io
