# Security Policy

## Reporting a Vulnerability

Please report security vulnerabilities through
[Codeberg Security Advisories](https://codeberg.org/gregburd/noxu/security/advisories/new)
or by contacting [Greg Burd](https://github.com/gburd) directly.

All reports will be investigated promptly. We will coordinate disclosure
with an expedited release including the fix.

## `unsafe` code inventory

Noxu DB is **not** zero-`unsafe`. Thirteen core data-path crates
(`noxu-tree`, `noxu-txn`, `noxu-evictor`, `noxu-cleaner`, `noxu-recovery`,
`noxu-dbi`, `noxu-engine`, `noxu-bind`, `noxu-collections`, `noxu-persist`,
`noxu-config`, `noxu-util`, `noxu-xa`) carry `#![forbid(unsafe_code)]` and
contain zero `unsafe`. The crates that *do* contain production `unsafe` are:

| Crate | Production `unsafe` blocks | Reason |
|---|---:|---|
| `noxu-sync` | ~20 (mostly small) | FFI to `libc` futex and the `lock_api`-shaped raw mutex/rwlock/condvar primitives that are the engine's production locks. |
| `noxu-log` | 7 | Memory-mapped I/O (`Mmap::map`); raw-pointer ops in `log_buffer.rs`; one `unsafe impl Send`; one lifetime-extending `transmute` in `log_source.rs`. |
| `noxu-rep` | 1 | Single FFI in `net/channel.rs` for socket-option setup. |
| `noxu-latch` | 1 | RAII force-unlock for poison recovery. |

Every production `unsafe` block carries a `// SAFETY:` comment, and this is
enforced in CI: every crate opts into the workspace lint set
(`[lints] workspace = true`), which sets
`clippy::undocumented_unsafe_blocks`, so
`cargo clippy --workspace --all-targets --all-features -- -D warnings` fails
on any undocumented `unsafe`. Adding any new `unsafe` requires review.

The concurrency primitives are the futex-based `noxu-sync` crate
(`lock_api`-shaped `Mutex`/`RwLock`/`Condvar`), **not** `parking_lot`
(`parking_lot` is a dev-only A/B-benchmark dependency and is not in any
shipped code path). See `AGENTS.md` for the authoritative, per-crate
inventory and the test/bench-only `unsafe` accounting.

## Threat Model

Noxu DB is an **embedded** database — it runs in-process with the
application. It does not listen on network ports except `noxu-rep` for
replication. The primary security considerations are:

1. **Memory safety** — no undefined behavior, use-after-free, or data races.
   Enforced by Rust's type system, the `noxu-sync` synchronization
   primitives, and the reviewed, SAFETY-documented `unsafe` inventory above.

2. **Data integrity** — CRC32 checksums on all log entries prevent silent
   corruption (`log.checksum.read` and `log.checksum.fatal` both default
   `true`). On-disk (`.ndb`) parsing during recovery is bounds-checked before
   allocation and treats a checksum mismatch as end-of-valid-log rather than
   injecting corrupt data. The `Verify` subsystem can validate B-tree
   consistency.

3. **Denial of service** — network and on-disk length-prefixed parsing is
   bounds-checked before allocation (replication frames capped at 64 MiB, log
   items at 100 MiB, the service-name handshake at 256 bytes). The plain-TCP
   service dispatcher bounds the handshake read (slow-loris) and caps
   concurrent connections. `MemoryBudget` and the evictor enforce cache
   limits.

4. **Replication authentication** — `noxu-rep` is the only network-exposed
   surface, and it is **fail-closed by default**:
   `ReplicatedEnvironment::new` refuses to start on a plain-TCP / QUIC
   transport in a production build unless the operator explicitly sets
   `insecure_no_auth = true` (which logs a warning). The recommended
   configuration is mTLS (`transport_kind = Tls`) with a non-empty
   `peer_allowlist`; an empty allowlist is a fail-closed `ConfigError`. Peer
   certificates are validated against the trusted CA and their subject
   CN/DNS-SANs matched against the allowlist before any application data is
   exchanged.

## Areas of Elevated Risk

- `noxu-sync` — the crate carrying the bulk of the `unsafe` inventory (futex
  FFI + raw lock primitives).
- `noxu-log` `memmap2` usage and raw-pointer log-buffer paths.
- **`noxu-rep` authorization gaps (tracked, honestly documented):**
  - Election/heartbeat traffic is authenticated at the *transport* layer
    (mTLS) but **not** at the message level — mTLS does not bind an individual
    proposal/vote to the cert that completed the handshake, and self-reported
    `node_name` is not cross-checked against the verified peer identity
    (NA-5/NA-6/NA-7 / audit F3b/S1).
  - The ADMIN RPC (shutdown-group / master-transfer / step-down) grants full
    group administrative authority to **any** transport-admitted peer — under
    mTLS, any allowlisted peer; under `insecure_no_auth`, any host that can
    reach the port (audit F5/S1).
  - `insecure_no_auth` and `TrustedCerts::SkipVerification` are intentional
    escape hatches that provide **zero** peer authentication of any kind; use
    only on a network trusted by other means.

  See `docs/src/operations/known-limitations.md` (Replication security) and
  `docs/src/internal/auth-mtls-design-2026-05.md` for the full, current
  posture and the tracked remediations.
- Log file parsing during recovery (processes untrusted on-disk data).
