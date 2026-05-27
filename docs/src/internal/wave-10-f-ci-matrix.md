# Wave 10-F — CI matrix expansion

Status: complete (sprint v2.3.0).
Branch: `fix/wave10-f-ci-matrix`.
Touches: `.github/workflows/test.yml`, `.forgejo/workflows/test.yml`,
docs only.

## The problem

Before Wave 10-F, the CI matrix declared more coverage than it actually
provided.

`.github/workflows/test.yml` had a `test` job with
`matrix: [ubuntu-latest, macos-latest, windows-latest]`, but every cell
of the matrix ran the same command:

```bash
cargo test --workspace
```

i.e. **default features only**, **no-fail-fast missing**, and
**`submodules: recursive` missing on `actions/checkout`**. The latter
is a latent bug: `noxu-rep` depends on the in-tree `quoracle`
submodule via `quoracle = { path = "crates/noxu-rep/quoracle" }` in
the workspace root `Cargo.toml`. Without the submodule checked out,
the workspace cannot resolve and `cargo test --workspace` would fail
to build `noxu-rep` at all. The Forgejo workflow had it right; the
GitHub workflow did not.

The `clippy` step in the `check` job did pass `--all-features`, so
the type-checker saw the rep TLS, QUIC, and observability paths.
But `cargo test` never saw them, which meant any regression in those
feature-gated code paths (e.g. broken `tls-native` build, broken
`opentelemetry` glue) would land silently on `main`.

The `cross` job ran `cargo check` only on aarch64 / armv7 / riscv64,
which is the right call (no qemu in CI), but it too only saw the
default feature set.

## The matrix, post-Wave 10-F

### `.github/workflows/test.yml`

| Job                  | OSes                                       | Command                                                            |
|----------------------|--------------------------------------------|--------------------------------------------------------------------|
| `check`              | `ubuntu-latest`                            | `cargo fmt --check` and `cargo clippy --workspace --all-features`  |
| `test`               | `ubuntu-latest`, `macos-latest`, `windows-latest` | `cargo test --workspace --no-fail-fast` (default features) |
| `test-all-features`  | `ubuntu-latest`                            | `cargo test --workspace --all-features --no-fail-fast`             |
| `cross`              | `ubuntu-latest` (cross-toolchain)          | `cargo check --workspace --target {aarch64,armv7,riscv64}-…`       |
| `doc`                | `ubuntu-latest`                            | `cargo doc --workspace --no-deps` and `mdbook build docs/`         |

### `.forgejo/workflows/test.yml`

Codeberg only offers Linux runners (`lxc-bookworm`), so the Forgejo
mirror covers exactly the same feature dimensions but only the Linux
OS row:

| Job                  | OSes           | Command                                                          |
|----------------------|----------------|------------------------------------------------------------------|
| `check`              | `lxc-bookworm` | fmt + clippy `--all-features`                                    |
| `test`               | `lxc-bookworm` | `cargo test --workspace --no-fail-fast` (default features)       |
| `test-all-features`  | `lxc-bookworm` | `cargo test --workspace --all-features --no-fail-fast`           |
| `doc`                | `lxc-bookworm` | `cargo doc --workspace --no-deps`                                |

(macOS / Windows coverage lives only on the GitHub side.)

## Design rationale

### Why ubuntu-only for `--all-features`?

Enabling every feature simultaneously pulls in the union of
`tls-rustls` + `tls-native` + `quic` + `observability` + the rep
`test-harness`. `tls-native` builds against the system OpenSSL,
which on:

- **Linux** is `libssl-dev` from apt — already installed for clippy.
- **macOS** would need `brew install openssl@3` and `OPENSSL_DIR`
  exported, with the Apple Silicon vs. Intel path split.
- **Windows** would need either vcpkg
  (`vcpkg install openssl:x64-windows-static-md`) or a separate
  pre-built OpenSSL binary, and would still need
  `OPENSSL_DIR`/`OPENSSL_LIB_DIR`/`OPENSSL_INCLUDE_DIR` glued
  through.

Two of those three are non-trivial CI plumbing for code paths
(`tls-native`) that are tested adequately on Linux. The `tls-rustls`
path is what most users will deploy (pure Rust, no system deps), and
its build is exercised on Linux already. We chose to keep the OS
matrix on default features and put the feature-flag matrix on a
single Linux row.

If a future change makes a feature genuinely OS-sensitive (e.g. a
Windows-specific TLS implementation), we will revisit and add a
matrix dimension there.

### Why no qemu in `cross`?

`cargo check` catches build-time portability bugs (size_t mismatches,
endian assumptions in `byteorder` calls, syscall numbers in the
`noxu-sync::futex` Linux gate). Running tests on aarch64 / armv7 /
riscv64 would require qemu-user setup, which is:

- slow (the noxu-rep test suite alone takes minutes on native
  ubuntu),
- brittle (qemu's syscall translation occasionally differs from
  native, generating false positives),
- and largely redundant — the algorithms are not architecture-
  sensitive once the build passes.

Native ARM CI on `actions/runner` is becoming available
(`ubuntu-24.04-arm`); a future wave can flip aarch64 to a real
runner without changing the matrix shape.

### Distinct cache keys

Each job uses a distinct cache key:

| Job                  | Cache key prefix                          |
|----------------------|-------------------------------------------|
| `check`              | `${{ runner.os }}-cargo-check-`           |
| `test`               | `${{ runner.os }}-cargo-test-default-`    |
| `test-all-features`  | `${{ runner.os }}-cargo-test-allfeat-`    |
| `cross`              | `cross-${{ matrix.target }}-`             |
| `doc`                | `${{ runner.os }}-cargo-doc-`             |

The default-features and all-features jobs would otherwise alternate
overwriting each other's `target/` directory because their feature
graphs differ.

### Cross-platform code is already gated

A pre-Wave-10-F audit confirmed the code base is portability-clean:

- `noxu-sync::futex` gates the Linux-only `SYS_futex` syscall behind
  `#[cfg(target_os = "linux")]` with a parking_lot fallback for
  non-Linux platforms.
- `noxu-log::file_handle` and `noxu-log::file_manager` use `#[cfg(unix)]`
  / `#[cfg(windows)]` for the `AsRawFd` vs. `AsRawHandle` split.
- `noxu-rep::net::channel` gates `setsockopt(SO_RCVTIMEO)` behind
  `#[cfg(unix)]` (the `tokio::net::TcpStream` path on Windows uses
  the standard tokio API instead).
- `noxu-rep::tests::torture_test` gates `tc netem` chaos injection
  behind `#[cfg(target_os = "linux")]`; non-Linux runs use software-
  only fault injection.
- File paths everywhere use `std::path::PathBuf` / `Path`, not raw
  `/`-strings. `tempfile::TempDir` is used in tests instead of
  hard-coded `/tmp`.

No new `#[cfg]` gates were needed in Wave 10-F.

## What changed mechanically

1. `.github/workflows/test.yml`:
   - Added `with: submodules: recursive` to every `actions/checkout@v4`
     step (closes the latent `quoracle` build bug).
   - Added `--no-fail-fast` to the `test` job (parity with Forgejo).
   - Added new `test-all-features` job (ubuntu-latest).
   - Distinct cache key for the default-features test job
     (`-cargo-test-default-` vs. `-cargo-test-allfeat-`).
   - `check` job now installs `pkg-config` + `libssl-dev` so the
     clippy run with `--all-features` (which already enabled
     `tls-native`) has its system dependencies present and consistent
     with `test-all-features`.

2. `.forgejo/workflows/test.yml`:
   - Renamed the `test` job's display name to "Test default features".
   - Distinct cache key (`-cargo-test-default-`).
   - Added a parallel `test-all-features` job mirroring the GitHub
     side.
   - Updated the file-header comment to describe the matrix rationale
     and link to this document.

3. Documentation:
   - This file (`docs/src/internal/wave-10-f-ci-matrix.md`).
   - Added to `docs/src/SUMMARY.md`.
   - Updated the "Continuous Integration" jobs table in
     `docs/src/contributing/build-and-test.md`.

## Verification

Local: `make docs-check` passes (typos + markdownlint + mdbook build).

Remote: the `test-all-features` jobs will surface any regressions in
`noxu-rep::net::tls`, `noxu-observe`, or the `quic` stack the next
time a PR touches them.
