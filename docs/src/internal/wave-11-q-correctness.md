# Wave 11-Q — correctness fixes from the 2026-05 audit

**Target release**: v2.4.2 (non-breaking patch).
**Audit context**: [`audit-2026-05-synthesis.md`](audit-2026-05-synthesis.md).

This wave closes the cross-confirmed critical and high-severity findings from
the 2026-05 audit that do not require breaking changes. The remaining
findings (C-4, C-5, C-6, C-8, H-1, H-5..H-8, H-10, Q-1, Q-2, Q-3, Q-4, Q-6,
Q-7) are deferred to wave 11-R (breaking-OK) and wave 11-S (UX cleanup).

## Items addressed

| Audit finding | File:line | Fix | Test |
|---|---|---|---|
| **C-1** parent-dir fsync | `noxu-log/src/file_manager.rs` `create_file_internal` | Open parent dir, `sync_all`, drop. | `test_c1_parent_dir_synced_after_file_create` |
| **C-2** fsync error invalidates env | `noxu-log/src/{fsync_manager,log_manager}.rs`, `noxu-dbi/src/environment_impl.rs` | `LogManager::io_invalid: Arc<AtomicBool>`. Set on any fdatasync error; checked at every `log()` entry. `EnvironmentImpl::is_valid()` checks the flag. | `test_c2_fsync_error_invalidates_env` |
| **C-3** CRC32 in recovery scanner | `noxu-dbi/src/file_manager_scanner.rs` `parse_entry_from_bytes` | Compute and verify CRC32 over `[4..entry_size]`. Mismatch returns `None` (treated as end-of-valid-log, safest recovery posture). | `test_c3_corrupted_entry_rejected_by_scanner` |
| **C-7** `Release`/`Acquire` on log-buffer pin-count | `noxu-log/src/log_buffer.rs` `LogBuffer::free`, `LogBufferSegment::put`, `wait_for_zero_and_latch` | `fetch_sub(.., Release)` + zero-check `load(.., Acquire)`. | `test_c7_release_acquire_pin_count_visibility` |
| **H-2** lock-ordering shard-before-waiter-graph | `noxu-txn/src/lock_manager.rs` | Documented canonical order, added `flush_and_clear_waiter()` helper used by all six victim-cleanup paths. | `test_lock_ordering_no_internal_deadlock` |
| **H-3** per-log-entry allocation | `noxu-log/src/log_manager.rs:286` | TODO marker for follow-up. The deeper refactor (per-thread scratch buffer with careful LWL lifetime handling) is deferred to wave 11-S. | _(none — deferred)_ |
| **H-4** victim selection populated `lock_counts` | `noxu-txn/src/lock_manager.rs::compute_lock_counts` | New helper walks all shards, tallies locks held by every locker_id in the cycle. Called only on the rare cycle path; no cost on the common no-cycle path. | `test_h4_victim_selection_uses_lock_counts` |
| **H-9** PartialEvict actually frees data | `noxu-tree/src/tree.rs::BinStub::strip_lns`, `noxu-evictor/src/evictor.rs::strip_lns_from_node` | New `BinStub::strip_lns` clears `data: Option<Vec<u8>>` on non-dirty slots, returns bytes freed. PartialEvict path now calls it via `strip_lns_from_node`. | `test_h9_strip_lns_actually_frees_data` |
| **C-9** AGENTS.md `unsafe` inventory | `AGENTS.md` | Reorganized the unsafe-block inventory as a per-crate table. Added `std::mem::transmute` in `noxu-log/log_source.rs:61` (sound). Added `unsafe impl Send for LogBufferSegment`. Removed three stale `unsafe impl Send + Sync` blocks in `noxu-rep::elections::{election, master_tracker, phi_detector}` whose interior types auto-derive the bounds. Removed stale claim about `noxu-db unsafe impl Send for SecondaryConfig` (already removed). | _(no test — documentation accuracy)_ |
| **Q-5** `#![forbid(unsafe_code)]` | 12 crate `lib.rs` files | All 12 zero-unsafe crates now carry `#![forbid(unsafe_code)]`: `noxu-tree`, `noxu-txn`, `noxu-evictor`, `noxu-cleaner`, `noxu-recovery`, `noxu-dbi`, `noxu-engine`, `noxu-bind`, `noxu-collections`, `noxu-persist`, `noxu-config`, `noxu-util`. | _(compiler-enforced)_ |

## Test gate at v2.4.2

| Check | Result |
|---|---|
| `cargo fmt --all -- --check` | clean |
| `cargo clippy --workspace --all-targets -- -D warnings` | clean |
| `RUSTDOCFLAGS=-D warnings cargo doc --workspace --no-deps` | clean |
| `cargo test --workspace --no-fail-fast` | **5774 passed, 0 failed** (+8 from v2.4.1) |
| `make docs-check` (typos + markdownlint + mdbook) | clean |

## Notable design decisions

- **C-2 invalidation**: chose to add `io_invalid: Arc<AtomicBool>` to
  `LogManager` rather than a callback into `EnvironmentImpl` because the log
  manager is the lowest layer that observes the I/O error and needs the
  fastest path to refuse subsequent writes. The flag is checked at every
  `LogManager::log()` entry; subsequent commits fail fast with a typed
  error.

- **C-3 corruption posture**: a CRC mismatch causes the scanner to treat
  the entry as end-of-valid-log rather than aborting recovery. Most
  recovery systems (including JE) take this conservative posture: a
  corruption mid-log is either a torn write at the tail (where stopping is
  correct) or a deeper corruption (where stopping prevents propagating the
  damage into the recovered state).

- **H-3 deferred**: the obvious per-thread scratch buffer is at odds with
  the LWL-protected encoding region: the encoded bytes need to live until
  copied into the log buffer / file, and the LWL is held during that copy,
  so a naive thread-local trampling another thread's still-in-flight buffer
  is a real concern. A correct fix needs either (a) a small per-LogManager
  pool guarded by the LWL, or (b) a `BytesMut` reused with reference
  counting. Either is more invasive than the v2.4.2 scope allows.

- **H-9 strip semantics**: `BinStub::strip_lns` skips dirty slots. A dirty
  slot's data has not yet been written to the log; dropping the in-memory
  copy would lose the update. JE's equivalent (`evict_lns` in `Bin`) writes
  dirty slots to the log first, then clears them. The Rust `BinStub` is a
  simpler structure that does not embed the log-write path; the dirty-skip
  behavior is the conservative choice. A future enhancement could write
  dirty slots to the log inside `strip_lns` if the evictor passes the
  `LogManager` reference.

## Items deferred to follow-up waves

- **H-3** (per-log-entry alloc reduction) — wave 11-S.
- **C-4, C-5, C-6, C-8** (breaking semantic fixes) — wave 11-R.
- **H-1** (EnvironmentImpl lock held across abort undo) — wave 11-S.
- **H-5..H-8** (documentation accuracy fixes) — wave 11-S.
- **H-10** (already addressed by C-9 work in this wave).
- **Q-1, Q-2, Q-3, Q-4, Q-6, Q-7** (UX + cleanup) — wave 11-S.

## Cross-cutting

- The `noxu-rep::elections` `unsafe impl Send + Sync` cleanup (3 sites)
  was both a C-9 audit item and a Jonhoo finding 4.1; addressed once
  in the C-9 commit.
- `cargo +nightly miri test -p noxu-log --tests` was not run because miri
  is not installable in the dev environment for this round; the C-7 fix
  is documented and the corresponding regression test is in place.
