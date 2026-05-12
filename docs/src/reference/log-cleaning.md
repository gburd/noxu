# Log Cleaning

Over time, updates and deletions leave log entries obsolete. The cleaner
(`noxu-cleaner`) reclaims disk space by migrating live entries from old files
to new ones, then deleting the now-empty old files.

## Cleaning Pipeline

### 1. Utilization Tracking

`UtilizationProfile` maintains a `FileSummary` per log file tracking live,
obsolete, and expired (TTL) bytes. Summaries are updated **incrementally** —
not by re-scanning files.

`FileSummary` also supports TTL-adjusted utilization via
`ExtinctionFilter`-marked records.

### 2. File Selection

`FileSelector` chooses the file to clean:
1. Compute `adjusted_utilization_pct` (incorporating TTL expiry)
2. Files below `min_utilization` (default 50%) are candidates
3. Lowest utilization file is selected first

### 3. File Processing

`FileProcessor`:
1. Reads live entries from the candidate file
2. Verifies each against the tree (still current version?)
3. Migrates confirmed live entries by logging them again (new LSN)
4. Marks the file as "fully processed"

### 4. File Deletion

A processed file is deleted only after the next checkpoint completes.
This invariant ensures recovery never needs a deleted file.

## Cleaner Throttling

`CleanerThrottle::should_throttle_writer()` returns `Option<Duration>` that
writers should sleep to let the cleaner catch up. Wired into:
- `Transaction::commit_with_durability()`
- `Database::put()` auto-commit path

## NoSQL Extensions

### DataEraser
Securely erases obsolete record bytes using `pwrite64` zero-overwrite.
Enable: `EnvironmentConfig::with_data_eraser_enabled(true)`.

### ExtinctionScanner
Periodically scans the B+tree for TTL-expired and manually extinct records
and removes them asynchronously.
Enable: `EnvironmentConfig::with_extinction_scanner_enabled(true)`.
