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

## T-F2 — SERIALIZABLE range (next-key) locking

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

**Problem.** `become_master` creates in-memory `Feeder` tracker structs but
spawns no `FeederRunner`/`EnvironmentLogScanner` thread, so a master does not
actively stream log entries to replicas. (The pull-based `PEER_FEEDER` service
exists; the push/active-feed path does not.) Replication HA, ack policies, and
VLSN streaming all depend on this.

**Design.** Spawn, per registered replica, a `FeederRunner` that owns an
`EnvironmentLogScanner` positioned at the replica's acked VLSN, reads committed
log entries in VLSN order, and pushes them over the established channel;
integrate with `AckTracker` for `ReplicaAckPolicy` and with `shutdown_group`
(which must then actually wait for replica catch-up — review M-4). Gate on
`with_environment`. This is a replication feature, not a stub tweak.

**Qualification.** Multi-node integration test: node A `become_master`, node B
reads via the feed and converges to A's data; an ack-policy commit blocks until
B acks; `shutdown_group` waits for B to reach A's VLSN. Until this lands,
`become_master` / HA must be described as preview (see `known-limitations.md`).

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
