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

### 4a. Database catalog safety

Noxu's database catalog (name → id) is an in-memory `HashMap` rebuilt from
`NameLN` WAL entries during recovery, not a checkpointed mapping tree (unlike
JE). The cleaner does not migrate `NameLN` entries (it treats them as
unrecognised `Other` entries), so a forced cleaning pass could otherwise
reclaim the log file holding a database's only `NameLN` — after which recovery
could not find the database (`DatabaseNotFound`). To preserve the
"recovery never needs a deleted file" invariant for the catalog, the
checkpointer **re-logs the live catalog (one fresh `NameLN` per open
database) at the start of every checkpoint**. Combined with the two-checkpoint
deletion barrier above, a fresh `NameLN` for every live database always exists
in a file newer than any file the barrier can make deletable, so recovery's
full-log scan always finds it. This is Noxu's analog of JE flushing the
mapping-tree root at checkpoint (`Checkpointer.flushRoot`).

## Cleaner Throttling

`CleanerThrottle::should_throttle_writer()` returns `Option<Duration>` that
writers should sleep to let the cleaner catch up. Wired into:

- `Transaction::commit_with_durability()`
- `Database::put()` auto-commit path

## Extended-Fork Entry Types

### DataEraser

Securely erases obsolete record bytes using `pwrite64` zero-overwrite.
Enable: `EnvironmentConfig::with_data_eraser_enabled(true)`.

### ExtinctionScanner

Periodically scans the B+tree for TTL-expired and manually extinct records
and removes them asynchronously.
Enable: `EnvironmentConfig::with_extinction_scanner_enabled(true)`.
