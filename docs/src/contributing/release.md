# Release Process

## Versioning

Noxu DB uses [Semantic Versioning](https://semver.org/):

- **MAJOR**: breaking public API change (rare until 1.0)
- **MINOR**: new backwards-compatible functionality
- **PATCH**: backwards-compatible bug fix

All 19 crates share a single version number. The workspace root `Cargo.toml`
sets the version; each crate inherits it.

## Pre-Release Gates

All items in `RELEASE_CHECKLIST.md` at the project root must be completed
before creating a release tag.

The critical gate is the full test suite in release mode:

```bash
cargo test --workspace --release
```

And the docs quality gate:

```bash
make docs-check
```

## Release Steps

### 1. Update Version

Edit the workspace `Cargo.toml`:

```toml
[workspace.package]
version = "0.X.Y"
```

### 2. Update Changelog

Add an entry to `CHANGELOG.md` following the [Keep a Changelog](https://keepachangelog.com/)
format:

```markdown
## [0.X.Y] — YYYY-MM-DD

### Added
- ...

### Changed
- ...

### Fixed
- ...
```

Move unreleased items from `[Unreleased]` to the new version section.

### 3. Run the Full Checklist

```bash
# Code quality
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo deny check licenses

# Tests
cargo nextest run --workspace
cargo nextest run --workspace --release

# Docs
cargo doc --workspace --no-deps
make docs-check
```

### 4. Commit and Tag

```bash
git add Cargo.toml CHANGELOG.md
git commit -m "chore: release v0.X.Y"
git tag -a v0.X.Y -m "Release v0.X.Y"
git push origin main
git push origin v0.X.Y
```

### 5. Verify CI

After pushing the tag, verify the CI pipeline passes. The release tag push
triggers the publish workflow if one is configured.

### 6. Verify Published Docs

After the tag push, verify the GitHub Pages deployment at
`https://gburd.github.io/lamdb/` reflects the release content.

## Hotfix Process

For critical bug fixes on a released version:

1. Create a branch from the release tag: `git checkout -b fix/v0.X.Y-critical v0.X.Y`
2. Apply the minimal fix.
3. Bump the patch version (0.X.Y → 0.X.Z).
4. Follow the full release process from step 2 above.
5. Cherry-pick the fix onto `main` if it applies.
