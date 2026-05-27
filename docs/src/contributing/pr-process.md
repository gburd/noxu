# PR Process

## Branch Naming

```text
feat/<short-description>       # new feature
fix/<short-description>        # bug fix
docs/<short-description>       # documentation only
refactor/<short-description>   # code restructuring, no behaviour change
perf/<short-description>       # performance improvement
ci/<short-description>         # CI/CD changes
```

Example: `feat/group-commit-wiring`, `fix/bin-delta-chaining`

## Commit Format

Follow the [Conventional Commits](https://www.conventionalcommits.org/)
specification:

```text
<type>(<scope>): <short summary in imperative mood>

<optional body explaining the why, not the what>

<optional footer: breaking changes, issue refs>
```

Types: `feat`, `fix`, `docs`, `refactor`, `perf`, `test`, `ci`, `chore`

Scope (optional): crate name or subsystem, e.g. `noxu-rep`, `cleaner`, `btree`

Examples:

```text
feat(noxu-rep): add phi accrual failure detector (Hayashibara 2004)

fix(noxu-txn): abort correctly undoes new inserts in undo log

docs: write maintainer's guide — algorithms, design decisions, crate guide

perf(noxu-log): release LWL before fsync for group commit coalescing
```

Keep the summary line under 72 characters. Use the body for the "why": cite Noxu
source files, reference algorithm papers, explain non-obvious choices.

## Before Submitting

Run the full local CI suite (see [Build and Test](build-and-test.md)):

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace
cargo doc --workspace --no-deps
make docs-check   # if docs were changed
```

All must pass with zero warnings/errors.

## Review Checklist

The PR author is responsible for the following before requesting review:

### Code

- [ ] Zero clippy warnings with `-D warnings`
- [ ] All tests pass on Linux, macOS, and Windows (CI will verify)
- [ ] New public API has Rust doc comments
- [ ] Noxu fidelity: logic matches `_/je/` source (or deviation is documented)
- [ ] No `unwrap()` in library code paths (use `?` or explicit error handling)
- [ ] No `unsafe` added without comment citing why it is sound

### Tests

- [ ] New logic has unit tests
- [ ] New integration behaviour has integration tests
- [ ] Tests use `TempDir` isolation — no fixed paths or ports

### Documentation

- [ ] `docs/src/` updated if public API, architecture, or config changed
- [ ] `CHANGELOG.md` entry added (for user-visible changes)
- [ ] `make docs-check` passes if docs changed

### Noxu Fidelity (porting PRs)

- [ ] Java source cited in commit message or code comment
- [ ] Preserved Noxu method names, doc comments, algorithm structure
- [ ] Rust-specific deviations (error types, ownership) documented in comments

## Review Process

1. Open a PR against `main` with the completed checklist above.
2. At least one maintainer approval is required.
3. CI must be green (all jobs in `test.yml` and `docs.yml`).
4. Squash-merge is preferred for feature branches; merge commits for
   significant milestones.

## After Merge

- Delete the branch.
- If the change affects documented behaviour, verify the deployed docs at
  `https://codeberg.page/gregburd/noxu/` after the `docs.yml` deploy job completes.
