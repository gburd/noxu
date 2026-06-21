# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Fixed

- **T-17**: the checkpointer's BIN-delta-vs-full logging decision drifted from
  JE `BIN.shouldLogDelta` (BIN.java:1892). It used a dirty *fraction*
  (`dirty_count / total <= 0.25`) against a hardcoded 0.25, with no
  `isBINDelta` fast path, no `numDeltas <= 0` guard, and no
  `isDeltaProhibited` delta-chain bound. The decision is now a faithful
  count-based port in `BinStub::should_log_delta`: `numDeltas` (dirty slots)
  compared against `nEntries * binDeltaPercent / 100` (integer math), with the
  already-a-delta fast path, the empty-delta guard, and the
  `prohibit_next_delta` / `lastFullLsn == NULL` bound. The percent is now the
  configurable `TREE_BIN_DELTA` / `BIN_DELTA_PERCENT` param (0–75, default 25),
  exposed as `EnvironmentConfig::set_tree_bin_delta_percent` and threaded to
  the checkpointer. Removing a dirty slot during `compress` now sets
  `prohibit_next_delta` (JE `IN.deleteEntry`, IN.java:3466), forcing a full BIN
  on the next log; a full-BIN log clears it (JE `IN.afterLog`, IN.java:5557).
