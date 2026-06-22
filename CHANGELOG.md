# Changelog

All notable changes to Noxu DB are documented here.

## [Unreleased]

### Changed

- **C7 (on-disk format, additive): `FileSummaryLN` now persists the full
  utilization breakdown.** The `FileSummaryLN` WAL entry previously kept only
  five aggregate counters (total count/size, obsolete count/size, and a
  size-counted flag) and dropped the LN/IN total+obsolete split, `maxLNSize`,
  and the packed obsolete-LN offset list. It now serializes the complete
  `FileSummary` breakdown (the 11 ints of JE `FileSummary.writeToLog`) plus the
  packed obsolete-offset blob (JE `PackedOffsets.writeToLog`), so the persisted
  form is as faithful as the in-memory `TrackedFileSummary`. This is an additive
  on-disk format change: `FileSummaryLN` entries are written only by
  `Checkpointer::persist_file_summaries` (added in v6.2.0) and read back only by
  the new recovery-time profile rebuild (CLN-4); no pre-existing reader is
  affected.
