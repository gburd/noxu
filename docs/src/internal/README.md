# Internal Documents

This section contains technical analysis documents written during the
development of Noxu DB. They are intended for contributors and maintainers,
not end users.

> **Note:** These documents reflect the state of the codebase at the time
> they were written. Check the date and the git log to understand whether
> specific findings have been addressed since the document was produced.

## Documents

- [Noxu Fidelity Review](je-fidelity-review.md) — code-verified fidelity comparison
  against Noxu DB 7.5.11 (754 production classes). Last updated: Session 36.
- [Audit Report](audit-report.md) — historical snapshot of three
  independent audits taken when the workspace had 16 crates and the
  public API was still backed by an in-memory HashMap. Most of its
  "Critical" findings have since been resolved; preserved for the
  record. Do not treat its subsystem-completeness claims as current.
- [Rust Code Review](rust-review.md) — historical Rust quality review (originally covered the 16-crate workspace state; current workspace has 19 crates). Grade: B+.
- [Serialization Research](serialization-research.md) — zero-copy log entry
  parsing research and recommendations for `noxu-log`.
- [Checksum Selection](checksum-selection.md) — CRC32 vs CRC32C benchmark and
  selection rationale for the replication feeder protocol.
