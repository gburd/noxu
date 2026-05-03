# Security Policy

## Reporting a Vulnerability

Noxu DB targets zero `unsafe` code in its core crates. The only uses of
unsafe are in external dependencies (`parking_lot`, `memmap2`) and
potentially in a future off-heap cache implementation.

Please report security vulnerabilities through
[GitHub Security Advisories](https://github.com/gburd/lamdb/security/advisories/new)
or by contacting [Greg Burd](https://github.com/gburd) directly.

All reports will be investigated promptly. We will coordinate disclosure
with an expedited release including the fix.

## Threat Model

Noxu DB is an **embedded** database  -  it runs in-process with the
application. It does not listen on network ports (except `noxu-rep` for
replication). The primary security considerations are:

1. **Memory safety**  -  no undefined behavior, use-after-free, or data races.
   Enforced by Rust's type system and `parking_lot` synchronization.

2. **Data integrity**  -  CRC32 checksums on all log entries prevent silent
   corruption. The `Verify` subsystem can validate B-tree consistency.

3. **Denial of service**  -  unbounded input could cause excessive memory
   or disk usage. The `MemoryBudget` and evictor enforce limits.

4. **Replication**  -  `noxu-rep` opens network connections for master-replica
   communication. This is the only network-exposed surface.

## Areas of Elevated Risk

- `memmap2` usage for memory-mapped file access
- Any future off-heap cache implementation
- `noxu-rep` network protocol (unauthenticated in current implementation)
- Log file parsing during recovery (processes untrusted on-disk data)
