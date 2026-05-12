# Internal Documents

This section contains technical analysis documents written during the
development of Noxu DB. They are intended for contributors and maintainers,
not end users.

> **Note:** These documents reflect the state of the codebase at the time
> they were written. Check the date and the git log to understand whether
> specific findings have been addressed since the document was produced.

## Documents

- [JE Fidelity Review](je-fidelity-review.md) — code-verified fidelity comparison
  against BDB JE 7.5.11 (754 production classes). Last updated: Session 36.
- [Audit Report](audit-report.md) — consolidated findings across three
  independent audits (Foundation/Storage, Core/Background, API/Extensions).
- [Rust Code Review](rust-review.md) — Rust quality review of all 16 crates
  (simulated expert review). Grade: B+.
- [Serialization Research](serialization-research.md) — zero-copy log entry
  parsing research and recommendations for `noxu-log`.
- [Checksum Selection](checksum-selection.md) — CRC32 vs CRC32C benchmark and
  selection rationale for the replication feeder protocol.
