# Dedicated-effort designs — deferred v3.x review blockers

**Date**: 2026-06-03

The v3.x production-readiness review
(`production-readiness-review-2026-06.md`) left a set of blockers that are
**not** safe quick fixes: they change the on-disk format, alter the isolation
contract, implement a replication feature, or refactor a hot data path. Each
is designed here so it can be implemented as a focused, individually-qualified
change rather than rushed at the tail of a remediation session. A
half-correct version of several of these (range locking, header format,
feeder threads) is *worse* than the current honest state.

All the contained, currently-active correctness/soundness bugs from the review
were already fixed (S-C1, T-F1, R-F03, R-F04, R-F05, T-F5).

## St-C3 / St-H1 / St-H3 — on-disk format v3 (header checksum + endianness)

**Problem.** The 32-byte file header is written without a checksum; a torn
header write that corrupts `file_number`/`last_entry_in_prev_file` while
leaving magic+version intact passes validation and yields wrong recovery
metadata. Separately, the header is big-endian while entry headers are
little-endian and some payloads big-endian — undocumented mixed endianness
(now at least documented in `file_header.rs`).

**Why it is not a tweak.** The header has only 3 reserved bytes; a CRC32 needs
4, so the header must grow. `FILE_HEADER_SIZE` is the **first-entry offset**
and is therefore part of the `(file_number, file_offset)` LSN space. It is
read as a fixed `32` in at least: `noxu-log/file_manager.rs`
(`first_log_entry_offset`), `noxu-cleaner/cleaner.rs` (×3),
`noxu-dbi/file_manager_scanner.rs` (×4), `noxu-dbi/environment_impl.rs`. A
size change requires every "where does entry 0 start in file N" computation to
become **version-aware**, and a wrong offset is silent recovery corruption.

**Design (LOG_VERSION 2 → 3).**

1. `FileHeader::read_from`: read magic(8) + version(4) first; branch on
   version: v2 ⇒ 32-byte header, no CRC; v3 ⇒ 36-byte header, verify a
   trailing CRC32 over bytes `[0..32]`. Reject v3 on CRC mismatch with a
   distinct `LogError::HeaderChecksumMismatch`.
2. Add `FileHeader::on_disk_size(version) -> usize` (32 for v2, 36 for v3) and
   replace the const `FILE_HEADER_SIZE` first-entry-offset uses with a
   per-file, version-aware lookup (the `FileManager` already reads the header
   on open, so it can cache the version → size per open file).
3. Writes always emit v3.
4. Backward compatibility: existing v2 files remain readable (entries at
   offset 32); v3 files put entries at offset 36. No data migration needed —
   the offset is resolved from the file's own version.
5. Endianness: keep the existing byte layouts (do not re-encode); the
   `file_header.rs` doc now states the per-section byte order accurately.

**Qualification.** Round-trip v3 (CRC verified); corrupt one header byte and
assert open fails with the checksum error; open a pre-written v2 file and
confirm entries are still found at offset 32; full crash-recovery suite green;
a file-flip across the v2→v3 boundary recovers correctly.

## St-H4 / St-H5 — upper-IN descent: unified binary floor-search

**Problem.** Internal-node descent uses an O(n) linear floor scan; JE uses
binary search. `TreeNode::find_entry` also returns the insertion point rather
than the floor for Internal nodes (latent — live callers pass `exact=true`).

**Why it is not a one-liner.** There are ~5 near-duplicate floor-descent loops
(`Tree::search`, `search_with_data`, `search_with_coupling`,
`update_key_expiration`, `get_parent_bin_for_child_ln`) with **inconsistent**
comparison: most use `self.key_cmp` (custom-comparator-aware), but
`search_with_coupling` uses raw `entry.key <= key` (ignores a custom
comparator — a separate latent bug). The `first_entry_at_or_after*` loops are
ceiling, not floor, and must NOT be folded in. A piecemeal conversion leaves
an inconsistent state on the most critical data path (attempted once, verified
equivalent over the full suite, then reverted for this reason).

**Design.** Add one helper
`Tree::upper_in_floor_index(&self, n: &InNodeStub, key: &[u8]) -> usize` that
binary-searches `entries[1..]` (slot 0 = virtual −∞) with `self.key_cmp` and
returns the floor index, then replace all five inline scans with a call.
Unifying on `key_cmp` also fixes the `search_with_coupling` comparator bug.
Fix `find_entry`'s Internal non-exact arm to return the floor consistently.

**Qualification.** The full `noxu-tree` + `noxu-db` suites (thousands of
searches) must stay green; add a property test comparing the helper's result
to a reference linear floor scan over random sorted IN key sets, including a
custom comparator.

## T-F2 — SERIALIZABLE range (next-key) locking — **FIXED**

**Status**: Implemented and merged in `fix/tf2-range-locks`.

**Summary of changes:**
- `lock_manager.rs`: `WaitRestart` wakeup now returns `Err(RangeRestart)` instead
  of incorrectly granting the lock as `New`.
- `locker.rs` / `txn.rs`: Added `owns_any_lock(lsn)` to guard against an illegal
  `RangeRead`→`RangeInsert` upgrade when the same SERIALIZABLE transaction both
  scans and inserts.
- `lsn.rs`: Added `Lsn::eof_lock_lsn(db_id)` for per-database EOF sentinel.
- `cursor_impl.rs`:
  - `lock_ln` acquires `RangeRead` when `is_serializable_isolation()`, else `Read`.
  - New `lock_range_insert`: acquires `RangeInsert` on the successor key's LSN
    for all new-key txn inserts (regardless of inserter's isolation level).
  - New `lock_eof_for_scan`: acquires `RangeRead` on the EOF sentinel when a
    SERIALIZABLE forward scan reaches the end of the key space.
- `database.rs`: `put` and `put_no_overwrite` now use `NoxuError::from(e)` for
  proper lock-error surfacing (previously mapped all errors to
  `OperationNotAllowed`).
- `error.rs`: `NoxuError::LockTimeout` gains a `detail` field preserving the
  full owner/requester diagnostic from `TxnError::LockTimeout`.

**Qualification**: Five new isolation tests all pass:
- `test_serializable_prevents_phantom_insert` (acceptance)
- `test_serializable_prevents_phantom_eof_insert` (EOF acceptance)
- `test_default_isolation_allows_phantom_insert` (regression: no over-locking)
- `test_read_committed_allows_phantom_insert` (regression: RC unaffected)
- `test_serializable_scan_then_insert_same_txn_no_panic` (same-txn guard)

**Original design** (kept for reference below):

**Problem.** `cursor_impl::lock_ln` always acquires `LockType::Read`, never
`RangeRead`, so SERIALIZABLE does not prevent phantoms (docs have been
corrected to stop claiming it does).

**Why it is not a flag flip.** Phantom prevention is next-key locking, not a
single lock-type swap. Making reads acquire `RangeRead` does nothing unless
inserts also acquire `RangeInsert` on the appropriate next key so the
`RangeRead`↔`RangeInsert` conflict fires; the scan must also `lockEof` the
end-of-range. A partial implementation would *appear* to prevent phantoms
while leaving gaps — strictly worse than the current honest "not prevented".

**Design.**

1. `lock_ln`: choose `RangeRead` when `txn.is_serializable_isolation()`, else
   `Read` (mirror JE `Cursor.getLockType(rangeLock)`).
2. Insert/put path: acquire `RangeInsert` on the next key (the slot that would
   follow the inserted key), matching JE's next-key protocol.
3. End-of-range: `lock_eof(RangeRead)` at scan end (JE `CursorImpl.lockEof`).
4. Restart handling: surface the `RangeRead`↔`RangeInsert` `RESTART` conflict
   as a cursor restart to the caller.

**Qualification.** A serializable cursor scans `[a, z]`; a concurrent committed
insert of `m` must be observed as a restart / not appear on re-scan within the
txn. Add the phantom-prevention test the review flagged as missing, plus
non-serializable regression (phantoms still allowed under REPEATABLE_READ).
Only after this lands should the SERIALIZABLE docs be restored to claim
phantom prevention.

## C-C2 — `become_master` feeder / log-streaming threads

**Status (v3.2.0, branch `fix/cc2-become-master-feeders`):**
**Push-feeder path SHIPPED; WAL-scanner auto-discovery DEFERRED.**

### What was implemented (v3.2.0)

- `ReplicatedEnvironment::register_feeder_channel(replica_name, channel)` —
  a new method that lets callers inject a `Channel` for a replica.  When
  `become_master` is called (or when the node is already master), a
  `FeederRunner` thread is spawned per registered channel.  The thread reads
  from a dedicated `PeerLogScanner` queue populated by `replicate_entry` /
  `apply_entry` fan-out and streams framed log entries to the replica.
- `replicate_entry` / `apply_entry` now fan out to all registered per-replica
  queues in addition to the shared `peer_scanner` (no competing-consumer
  problem between push and pull paths).
- `shutdown_group` now waits up to half the timeout for each `FeederRunner`
  replica to ack the master’s current VLSN before sending `SHUTDOWN_GROUP`
  (closes M-4 for the push path).
- 6 integration tests in `crates/noxu-rep/tests/cc2_feeder_integration_test.rs`
  demonstrate convergence, ack advancement, shutdown catch-up wait, late channel
  registration, `apply_entry` fan-out, and a 50-entry batch.

### What was NOT implemented (deferred as C-C2b)

The original design called for an `EnvironmentLogScanner`-backed thread that
automatically discovers replicated entries from the WAL **without requiring
`replicate_entry` calls**.  This requires:

1. `LogManager::log()` writing entries with VLSN tags (the `vlsn_present` flag
   in the entry header, i.e. a `Provisional::Replicated` variant or similar).
   Today `log()` always uses `MIN_HEADER_SIZE` (no VLSN field) because
   standalone environments are non-replicated.
2. `EnvironmentImpl`’s commit path calling `replicate_entry` or setting the
   VLSN on committed log entries — analogous to JE’s `RepContext` integration.
3. Possibly a `FeederReceiverService` on the replica side if pure push
   (master-initiated connections) is desired rather than the existing pull
   model.

Without (1), `EnvironmentLogScanner::next_entry` always returns `None` because
no WAL entries carry the `0x08 | 0x20` VLSN-present flags.

**Qualification gap for C-C2b**: a convergence test that uses `EnvironmentImpl`
commits (not `replicate_entry` calls) and asserts data propagation to the
replica cannot be written until (1)+(2) land.  The push-feeder test in
`cc2_feeder_integration_test.rs` demonstrates the channel / thread / ack
infrastructure works; it uses `replicate_entry` as the entry source.

### Ack-policy integration (partial)

`await_replica_acks` (via `ReplicaAckCoordinator`) uses a synthetic
`commit_ack_seq` counter tracked in `AckTracker`, not real VLSNs.  Wiring
the `FeederRunner`’s VLSN-based acks into the `AckTracker` seq requires a
`seq → vlsn` mapping.  This is deferred; the existing `record_ack(vlsn,
replica)` API still works when the application calls it manually after
receiving an ack.

### Original problem statement

`become_master` created in-memory `Feeder` tracker structs but spawned no
`FeederRunner`/`EnvironmentLogScanner` thread, so a master did not actively
stream log entries to replicas. (The pull-based `PEER_FEEDER` service existed;
the push/active-feed path did not.) Replication HA, ack policies, and VLSN
streaming all depend on this.

### Remaining design (C-C2b)

1. Add `Provisional::Replicated(vlsn: u64)` variant to `noxu-log`; thread
   the VLSN through `LogManager::log_internal` to write the 8-byte VLSN
   extension when replicated.
2. Wire the `ReplicatedEnvironment` (or `noxu_db` layer) to call
   `log_with_vlsn` at commit time, assigning the next VLSN from a shared
   monotone counter.
3. Then `EnvironmentLogScanner::next_entry` will find entries and the
   background scanner thread can auto-populate the feeder queues.
4. Optionally add a `FeederReceiverService` on the replica side for
   fully master-initiated (push-only) topology if the pull path is deprecated.

## Reaffirmed latent deferrals

- **R-F01** (`LogBufferSegment` raw pointers into a movable `LogBuffer`):
  production `LogBuffer`s live in `Arc<Mutex<…>>` in the pool and are never
  moved while a segment is alive (only test code stack-allocates them). Fix
  with a `PhantomPinned`/`Pin<Box<…>>` boundary or by moving the three control
  fields into an `Arc<SegmentControl>` the segment clones — a focused
  log-buffer change, not bundled with unrelated work.
- **St-H6** (`expiration_in_hours` not serialized): production only ever
  writes hours-granularity TTL, so the hardcoded `true` on deserialize is
  correct today; serialize the flag when seconds-granularity TTL is added
  (itself a BIN wire-format change → version-gated).
- **T-F3 / T-F4** (`first_active_lsn` + `update_first_lsn`): prerequisites for
  the deferred P-2 recovery-scan optimization; `get_first_active_lsn()` has no
  production consumer today. Land with P-2 (see `wave-gb-dbtree-recovery.md`).
