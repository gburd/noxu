# CleanerStats dead-field determination (fix/cleaner-stats-wiring)

Six `CleanerStats` fields (`crates/noxu-cleaner/src/cleaner_stat.rs`) are
never written by production code — only by `#[cfg(test)]` in that same file.
This note records the per-field decision before wiring, per
`docs/src/internal/space-amplification-2026-09.md` (which already flagged
this gap) and cross-referencing `~/ws/je` (present, checked).

| Field | Doc meaning (our doc comment / JE) | Decision | Production source (if WIRED) |
|---|---|---|---|
| `total_log_size` | "Bytes used by data files on disk: activeLogSize + reservedLogSize." JE `Cleaner.totalLogSize` (`CLEANER_TOTAL_LOG_SIZE`), refreshed by `recalcLogSizeStats`. | **WIRE** | `UtilizationProfile::get_total_log_size()` (already correct, used internally for selection) — sum of `FileSummary.total_size` across all files known to the profile/tracker at `do_clean` time. Alternative equally-honest source: `FileManager::total_log_size()` (used by `DiskLimitTracker`); profile-based is preferred since Noxu has no reserved-file tier so `total == active` and the profile is what selection itself trusts. |
| `active_log_size` | "Bytes used by all active data files: files required for basic operation." JE `CLEANER_ACTIVE_LOG_SIZE`, part of `FileProtector.LogSizeStats`. | **WIRE** | `UtilizationProfile::get_active_log_size()` — sum of `FileSummary.get_active_size()`. Noxu has no reserved/protected file tier (cleaner deletes outright, see `disk_limit.rs` doc comment), so `active_log_size == total_log_size` for us; that equivalence is the honest value, not a fabricated split. |
| `min_utilization` | "The current minimum (lower bound) log utilization as a percentage." JE `CLEANER_MIN_UTILIZATION` = `UtilizationCalculator.getCurrentMinUtilization()` — a **computed statistic**, NOT the config threshold (`Cleaner.minUtilization` AtomicU32, already a separate field on our `Cleaner` struct — do not conflate). | **WIRE** | Compute at `do_clean` time from the same aggregate formula JE uses (`FileSummary.utilization(currentMaxObsoleteSize, currentTotalSize)`): aggregate obsolete/total across the current file_summary_map, matching `FileSelector::compute_predicted_min_util` shape but using the *current* (not predicted/gradual) obsolete bound. |
| `max_utilization` | "The current maximum (upper bound) log utilization as a percentage." JE `CLEANER_MAX_UTILIZATION` = `getCurrentMaxUtilization()`. | **WIRE** | Same aggregate pass as `min_utilization`, using the other (min-obsolete) bound — `FileSummary.utilization(currentMinObsoleteSize, currentTotalSize)`. Computed together with `min_utilization` in one pass over `current_summary_map` inside `do_clean`. |
| `probe_runs` | Our doc: "Number of cleaner probing runs (without any cleaning) to update utilization information." | **REMOVE** (field + Prometheus gauge, if any) | JE's own `EnvironmentStats.getNCleanerProbeRuns()` is `@deprecated since JE 6.3, always returns zero` — there is no live "probe run" concept in current JE, and Noxu has no separate probe-only code path either (confirmed: no call site increments anything resembling this). Wiring a fabricated counter would be inventing a number dressed as fidelity. Removing this is the honest call. `noxu-observe` does not currently export a gauge for it (checked `export.rs`), so removal is stat-struct-only. |
| `repeat_iterator_reads` | Our doc: "Number of repeat-reads through the log caused by the repeat-iterator cleaning strategy." JE `nRepeatIteratorReads` = `FileReader.Window.adjustReadBufferSize` counter: incremented when a single log entry is larger than the reader's (growable, capped) read buffer and a second read is needed. | **REMOVE** (for now) — see caveat | Noxu's cleaner-side reader (`noxu-log::CleanerFileReader`) does NOT implement a growable/capped read-buffer-with-regrow strategy at all: it reads exact per-entry buffers (`vec![0u8; entry_size]`) sized from the header it already parsed, so the JE failure mode (entry bigger than current buffer, buffer regrows, and *if regrowing still isn't enough* a second/"repeat" read happens) structurally cannot occur in our implementation. Fabricating a count against a mechanism we don't have would be dishonest. Documented as N/A-to-current-design; recommend closing/removing rather than wiring 0-forever. |

## Caveat / open question on `repeat_iterator_reads`

`noxu-log::LogManager` DOES have a real, distinct, already-wired counter
`n_repeat_fault_reads` (`log_manager.rs`) for its own point-lookup fault-read
path (2-read fallback when the initial 2KB fault-read buffer is too small).
That is a different subsystem (general log fault reads, not cleaner
file-scan reads) and is already reported via `LogStatsSnapshot`. It is
**not** the same statistic as JE's cleaner-specific `nRepeatIteratorReads`
and should not be silently substituted for it — doing so would misrepresent
what the gauge measures. Decision stands: remove `repeat_iterator_reads`
from `CleanerStats` rather than repurpose an unrelated counter.

## Status

- [ ] `total_log_size` — wire
- [ ] `active_log_size` — wire
- [ ] `min_utilization` — wire
- [ ] `max_utilization` — wire
- [ ] `probe_runs` — remove
- [ ] `repeat_iterator_reads` — remove

Each will get its own commit per the task's incremental-commit requirement.
