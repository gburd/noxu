# Dedicated-effort designs — deferred v3.x review blockers

**Date**: 2026-06-03

The v3.x production-readiness review
(the 2026 review) left a set of blockers that are
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

### Remaining design (C-C2b) — **IMPLEMENTED** (branch `fix/cc2b-wal-vlsn-autofeed`)

**Status: CLOSED.**  All four steps landed:

1. `LogManager::log_with_vlsn(entry_type, payload, vlsn, flush, fsync)` added
   to `noxu-log`.  `log_internal` accepts `opt_vlsn: Option<u64>`; when
   `Some(vlsn)` it writes the 22-byte header with `REPLICATED_MASK |
   VLSN_PRESENT_MASK` flags and the 8-byte VLSN at offset 14.  The standalone
   `log()` path is byte-unchanged (14-byte header, no flag bits set).
2. `EnvironmentImpl::set_replication_vlsn_counter` (`noxu-dbi`) installs a
   shared `Arc<AtomicU64>`.  `log_txn_commit` increments it atomically and
   calls `log_with_vlsn` when set; standalone envs take the unchanged
   `log()` branch.
3. `ReplicatedEnvironment::with_environment` (`noxu-rep`) calls
   `env.set_replication_vlsn_counter(wal_vlsn_counter)` so every subsequent
   `log_txn_commit` is auto-tagged.  `spawn_feeder_runner` now uses
   `EnvironmentLogScanner` as the feeder source when env is wired;
   `EnvironmentLogScanner::next_entry` finds VLSN-tagged entries and
   returns them to the `FeederRunner::run` loop automatically.
4. `FeederReceiverService` on the replica side remains out of scope; the
   existing pull path (`PeerFeederService` / `catch_up_from_peer`) handles
   replica-initiated connections.

**Convergence test**: `test_wal_scanner_autofeed_convergence` in
`crates/noxu-rep/tests/cc2b_wal_vlsn_autofeed_test.rs` — performs real
`EnvironmentImpl::log_txn_commit` calls and asserts all committed entries
are received by the replica via WAL-scanner auto-feed.  The test **fails on
`origin/main`** (scanner returns None; 0 entries received) and **passes with
this branch** (≥ N_COMMITS entries received, VLSNs strictly increasing).

**Standalone regression test**: `test_standalone_env_writes_no_vlsn_header`
proves non-replicated envs still write 14-byte headers with no VLSN bits set.

## Reaffirmed latent deferrals

- **R-F01** (`LogBufferSegment` raw pointers into a movable `LogBuffer`):
  production `LogBuffer`s live in `Arc<Mutex<…>>` in the pool and are never
  moved while a segment is alive (only test code stack-allocates them). Fix
  with a `PhantomPinned`/`Pin<Box<…>>` boundary or by moving the three control
  fields into an `Arc<SegmentControl>` the segment clones — a focused
  log-buffer change, not bundled with unrelated work.
- **St-H6** (`expiration_in_hours` not serialized AND BIN split flag
  inheritance bug): **CLOSED — two live bugs, both fixed in this branch**.
  Three distinct defects were present:
  1. **Site 1 — Split-path data-loss** (NEW finding): `Tree::split_child`
     hardcoded `expiration_in_hours: false` on the right-half sibling BIN.
     Any hours-granularity `expiration_time` value (~495 000 in 2026) was
     then compared against `current_time_secs()` (~1.78 billion), returning
     `true` — silent data loss on every get of a right-sibling key.
     Regression test: `test_ttl_records_survive_bin_split_right_sibling_256`
     (FAIL-PRE: 128/256 keys missing; PASS-POST: 0 missing).
  2. **Site 2 — Recovery-path data-loss** (NEW finding): `eligible_for_redo`
     applied an `after_ckpt_start` guard to non-transactional LNs.  The
     background checkpointer thread writes CkptStart between inserts;
     `with_auto_txn` writes `locker_id = 0` (non-transactional) LNs;
     pre-checkpoint LNs were skipped.  Variable 33–194/256 records missing
     after close+reopen.
     Regression test: `test_ttl_records_survive_close_and_reopen`
     (FAIL-PRE: intermittent 33–194/256 missing; PASS-POST: stable 0).
  3. **Latent deserialization concern** (original finding — confirmed
     harmless): `expiration_in_hours` is not serialized in BIN records;
     `deserialize_full` hardcodes `true` (the safe direction).  No bug.
  Fix: inherit `b.expiration_in_hours` from the splitting BIN; set the
  other three hardcoded-`false` sites to `true`; add a `debug_assert!`
  guard at the split site; always replay non-transactional LNs in
  `eligible_for_redo`.
  See CHANGELOG.md `[Unreleased] Fixed (St-H6, two sites)`.
- **T-F3 / T-F4** (`first_active_lsn` + `update_first_lsn`): **PARTIALLY SHIPPED in `fix/checkpoint-user-bins`.**
  - **T-F4 (update_first_lsn wiring)**: SHIPPED. `CursorImpl` now accepts
    `with_txn_manager(Arc<TxnManager>)` (wired from `Database::make_cursor_with_locker`)
    and calls `txn_manager.update_first_lsn(txn_id, lsn)` alongside
    `Txn::note_log_entry` on every first transactional write. `get_first_active_lsn()`
    now returns a real LSN for active transactions.
  - **T-F3 (scan bounding)**: NOT YET ACTIVE. `CkptEnd.first_active_lsn` still
    emits `Lsn::new(0,0)` (full scan). Setting a non-zero value requires P-2
    BIN-preload during recovery: without pre-populating the in-memory tree from
    checkpointed BINs before the LN redo pass, starting from `first_active_lsn`
    would skip pre-checkpoint committed LNs — silent data loss. The `txn_manager`
    is wired into the checkpointer via `with_txn_manager()` and
    `get_first_active_lsn()` is queried; the result is discarded until P-2 lands.
    See `wave-gb-dbtree-recovery.md` for the P-2 redesign prerequisites.

## EV-14 / EV-27 — evictor-completeness MED deferrals (2026-06)

The evictor-completeness MED wave fixed the load-bearing safety guards
EV-6 (skip upper INs with cached children), EV-7 (skip root INs), and the
write-path back-pressure EV-15 (synchronous critical eviction in writer
threads). Two lower-priority items in the same wave are deferred:

- **EV-14 — `evictRoot` (user-DB root eviction): DEFERRED.** JE
  `Evictor.evictRoot` (Evictor.java:3050) CAN evict a *non-internal* DB's
  root IN under specific conditions: it logs the dirty root first, calls
  `rootRef.setLsn(newLsn)` to update the `DbTree` root reference, then
  `rootRef.clearTarget()` and increments `nRootNodesEvicted`. Noxu's
  `root_nodes_evicted` stat is consequently dead (the root is never evicted).
  This is **lower priority than EV-6/EV-7**, which *prevent* a wrong eviction
  that EV-13's detach made dangerous; EV-14 merely *enables* an additional
  (rare) eviction. Implementing it faithfully requires a separate root-LRU
  selection path (`RootEvictor`), logging the root before clearing it, and a
  version-aware `DbTree` root-LSN update — a focused change, not a tail-end
  tweak. Until it lands, `decide_eviction`'s EV-7 guard keeps **every** root
  resident (the simplest faithful rule), so the engine is correct but slightly
  more memory-conservative than JE for very large single-DB working sets whose
  root could otherwise be evicted. The `root_nodes_evicted` stat stays at 0.

- **EV-27 — off-heap budget subtraction: NO CHANGE NEEDED (off-by-default,
  not net-negative).** The triage note suggested removing a "net-negative
  budget subtraction" so enabling off-heap can't hurt. On inspection there is
  no such net-negative path: `OffHeapCache` is a self-contained write-back
  tier with its own mmap/LRU budget and **never touches the on-heap arbiter
  counter**; every method is gated on `is_enabled()`. The only on-heap
  interaction is `EnvironmentImpl` partitioning the cache ceiling at open:
  `arbiter_budget = cache_bytes - log_buf_total - off_heap_reserved`
  (environment_impl.rs). `off_heap_reserved = cfg.max_off_heap_memory`, which
  **defaults to 0** (dbi_config.rs:249), so with off-heap off-by-default the
  subtraction is a no-op. When off-heap IS enabled the subtraction is the
  *correct* partitioning (JE treats `cache_size` as the ceiling for the sum of
  the on-heap + off-heap + log-buffer pools). There is therefore nothing to
  remove; off-heap is genuinely inert by default and harmless when enabled.
