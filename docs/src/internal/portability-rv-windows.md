# Portability validation — RISC-V 64 + Windows on ARM64

**Branch**: `fix/portability-rv-windows`
**Platforms exercised**:

| Platform | Triple | Result |
|---|---|---|
| RISC-V 64 Linux (`rv`) | `riscv64gc-unknown-linux-gnu` | Full workspace builds; 170 test-suites pass, 0 failures — **no code changes required** |
| Windows on ARM64 (`santorini`) | `aarch64-pc-windows-msvc` | Full workspace builds + all tests pass (incl. `tls-rustls`/ring + mTLS enforcement) — **3 small fixes** |

x86_64 Linux remains the primary CI target.

## Windows portability fixes

1. **Cross-platform positioned I/O** (`crates/noxu-log/src/posio.rs`, new).
   `noxu-log` used `std::os::unix::fs::FileExt::{read_at, read_exact_at,
   write_all_at}` behind a `#[cfg]` alias, but Windows'
   `std::os::windows::fs::FileExt` exposes only `seek_read`/`seek_write`
   (no `*_exact`/`*_all`), so the aliased calls didn't compile on
   `aarch64-pc-windows-msvc`. The new `posio` module maps the three
   operations to `pread`/`pwrite` on Unix and to `seek_read`/`seek_write`
   retry-loops on Windows. Used by `file_handle.rs` and `file_manager.rs`.
   No behavior change on Unix.

2. **Cross-platform directory fsync** (`posio::sync_dir`).
   The C-1 crash-durability fix did `File::open(dir).sync_all()`, which is
   correct on POSIX but fails on Windows with "Access is denied" (a
   directory handle requires `FILE_FLAG_BACKUP_SEMANTICS`). `sync_dir` does
   a real directory fsync on Unix (preserving C-1) and a best-effort
   backup-semantics open + `FlushFileBuffers` on Windows (treating
   unsupported/denied as success, since NTFS journals the directory entry).
   This fixed ~18 `noxu-log` file-create-path test failures on Windows.

3. **Cross-platform "unbindable address" test**
   (`noxu-rep::network_restore_server`). The test bound `127.0.0.1:1`
   expecting failure (port 1 is privileged on Unix), but Windows lets
   unprivileged users bind low ports, so the test failed there. Switched to
   `192.0.2.1:9999` (RFC 5737 TEST-NET-1), which is not on any local
   interface and therefore fails to bind with `EADDRNOTAVAIL` on both Unix
   and Windows.

## Notes

- `noxu-sync`'s `libc` futex FFI is already `#[cfg(target_os = "linux")]`
  gated with a cross-platform fallback, so it compiled on Windows ARM64
  unchanged.
- `memmap2`, `fs2`, `crc32fast`, and `ring`/`rustls` all build and run on
  `aarch64-pc-windows-msvc`.
- The slow `noxu-spec::flexible_paxos::ephemeral_promises_allow_split_brain`
  Stateright model is the same long-running spec on every platform (not a
  portability issue).

## How to reproduce

```bash
# RISC-V (rv): native
ssh rv 'cd noxu && cargo test --workspace'
# Windows ARM64 (santorini): native (rustup 1.95.0 aarch64-pc-windows-msvc)
ssh santorini "cd %USERPROFILE%\\noxu-port & cargo test --workspace"
```
