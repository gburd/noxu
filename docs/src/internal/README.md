# Internal Documents

This section contains technical reference written during the development of
Noxu DB. It is intended for contributors and maintainers, not end users.

These are design and research notes that explain decisions still reflected in
the code. Per-iteration development logs and point-in-time audit transcripts
are intentionally not kept here.

## Documents

- [Serialization Research](serialization-research.md) — zero-copy log entry
  parsing research and recommendations for `noxu-log`.
- [Checksum Selection](checksum-selection.md) — CRC32 selection rationale for
  the log and replication feeder protocol (referenced by `file_header.rs`).
- [mTLS-by-default design (2026-05)](auth-mtls-design-2026-05.md) — the
  replication transport-security design.
- [noxu umbrella crate](noxu-umbrella.md) — rationale for the single `noxu`
  facade crate and its feature flags.
- [Portability validation — RISC-V 64 + Windows on ARM64](portability-rv-windows.md)
  — cross-platform validation notes.
- [Wave GB — DbTree foundation + P-2 recovery investigation](wave-gb-dbtree-recovery.md)
  — the recovery-scan / checkpoint-user-BIN-flush investigation.
  - [Deferred-blocker implementation designs](deferred-blocker-designs-2026-06.md)
    — designs and status for deferred review items (T-F3/T-F4, P-2).
