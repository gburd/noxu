# Build and Test

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Rust | stable (≥ 1.82) | `rustup toolchain install stable` |
| cargo-nextest | latest | `cargo install cargo-nextest --locked` |
| mdBook | 0.4.40 | `cargo install mdbook --version 0.4.40 --locked` |
| mdbook-mermaid | 0.13.0 | `cargo install mdbook-mermaid --version 0.13.0 --locked` |
| typos | latest | `cargo install typos-cli --locked` |
| markdownlint-cli2 | latest | `npm install -g markdownlint-cli2` |

The project uses a `rust-toolchain.toml` at the root; `rustup` will activate the
correct toolchain automatically when you enter the workspace directory.

## Building

```bash
# First-time setup: initialize the quoracle submodule used by noxu-rep.
git submodule update --init --recursive

# Build all crates
cargo build --workspace

# Build in release mode (needed for benchmarks)
cargo build --workspace --release

# Build with all features enabled
cargo build --workspace --all-features
```

## Running Tests

```bash
# Run all tests with nextest (recommended — parallel, better output)
cargo nextest run --workspace

# Run all tests with cargo test (fallback)
cargo test --workspace

# Run tests for a single crate
cargo nextest run -p noxu-txn

# Run a specific test by name
cargo nextest run -p noxu-rep -- test_phi_accrual_detector

# Run tests with output captured (useful for debugging)
cargo test -p noxu-log -- test_name --nocapture

# Run integration tests only
cargo nextest run --workspace --test '*'
```

### Test Configuration

Tests are configured in `.config/nextest.toml`. Key settings:

- Default test timeout: 30 seconds
- Replication tests timeout: 120 seconds
- Fuzz targets excluded from normal runs

## Checking Code Quality

```bash
# Format check (CI-style — fails if reformatting needed)
cargo fmt --all -- --check

# Apply formatting
cargo fmt --all

# Lint with all warnings as errors (CI-exact command)
cargo clippy --workspace --all-targets --all-features -- -D warnings

# Check licenses and dependency policy
cargo deny check licenses
```

## Building Documentation

```bash
# Rust API docs (opens in browser)
cargo doc --workspace --no-deps --open

# mdBook user documentation
make docs-serve        # Live-reload at http://localhost:3000
make docs              # One-shot build to docs/book/
make docs-check        # Full quality gate: spell + lint + build
```

## Full Local CI Run

This mirrors what the CI pipeline runs before accepting a PR:

```bash
# All gates in sequence — must all pass before submitting a PR
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace
cargo doc --workspace --no-deps
make docs-check
```

Or using Make:

```bash
make check   # fmt-check + clippy
make test    # cargo test --workspace
make doc     # cargo doc
make docs-check  # docs quality gates
```

## Cross-Compilation

The CI cross-compiles to three additional targets. To reproduce locally:

```bash
rustup target add aarch64-unknown-linux-gnu
# Install cross-linker (Ubuntu: sudo apt install gcc-aarch64-linux-gnu)
cargo check --workspace --target aarch64-unknown-linux-gnu
```

Supported targets: `aarch64-unknown-linux-gnu`, `armv7-unknown-linux-gnueabihf`,
`riscv64gc-unknown-linux-gnu`.

## Continuous Integration

CI runs on every push and PR to `main`. The pipeline is defined in
`.github/workflows/test.yml` (code) and `.github/workflows/docs.yml`
(documentation). A PR cannot be merged until all jobs are green.

Jobs:

| Job | What it checks |
|-----|----------------|
| `check` | `cargo fmt` + `cargo clippy` |
| `test` | `cargo test` on Linux, macOS, Windows |
| `cross` | `cargo check` on AArch64, ARMv7, RISC-V |
| `doc` | `cargo doc` + `mdbook build docs/` |
| `spell` (docs.yml) | `typos docs/src/` |
| `prose` (docs.yml) | `markdownlint-cli2` |
| `build` (docs.yml) | `mdbook build` + lychee link check |
