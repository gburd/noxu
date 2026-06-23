# Changelog

All notable changes to Noxu DB are documented here.

## [Unreleased]

### Added

- **CLN-4: cleaner relies on persisted utilization immediately after restart.**
  Recovery now reads the `FileSummaryLN` records (C7 form) back during the
  env-open analysis scan and seeds the cleaner's `UtilizationProfile`
  (`EnvironmentImpl` → `Cleaner::seed_profile`). After a restart the cleaner's
  `get_profile_summary` / `get_file_summary_map` shows real per-file
  utilization with no re-warm-from-live-writes lag, so an under-utilized file
  is selectable for cleaning right away. Recovery also counts obsolete the LN
  supersessions written after each file's last `FileSummaryLN` (the
  `abort_lsn` → `isFileUncounted(oldFile, newLsn)` gate), so obsolete bytes
  produced after the last checkpoint are preserved. Faithful port of JE
  `UtilizationProfile.populateCache` + `RecoveryUtilizationTracker`
  (`countObsoleteIfUncounted`). Resolves the read-back half of the
  CLN-4/CLN-11 known limitation. Fixed a reentrant-lock deadlock:
  `persist_file_summaries` now snapshots the tracked summaries and drops the
  `UtilizationTracker` lock before writing each `FileSummaryLN` (the WAL write
  observer re-enters the same tracker to `countNewLogEntry`).

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
