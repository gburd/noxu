# Wave 11-Q — Correctness Fixes (v2.4.2)

**Branch**: `fix/wave11-q-correctness`
**Target**: v2.4.2 (non-breaking)
**Audit source**: `docs/src/internal/audit-2026-05-synthesis.md`

This wave addresses the non-breaking, correctness-critical items surfaced by the
2026-05 four-persona audit. Each item below has a regression test that would have
caught the original bug.

> **Status**: IN PROGRESS — placeholder committed, fixes landing in order.

## Items

| ID | Severity | File(s) | Test name | Status |
|----|----------|---------|-----------|--------|
| C-1 | Critical | `noxu-log/src/file_manager.rs` | `test_parent_dir_fsynced_after_file_create` | TODO |
| C-2 | Critical | `noxu-log/src/fsync_manager.rs`, `file_manager.rs` | `test_fsync_failure_invalidates_env` | TODO |
| C-3 | Critical | `noxu-dbi/src/file_manager_scanner.rs` | `test_recovery_scanner_rejects_corrupted_crc` | TODO |
| C-7 | Critical | `noxu-log/src/log_buffer.rs` | `test_pin_count_release_acquire_ordering` | TODO |
| H-2 | High | `noxu-txn/src/lock_manager.rs` | `test_lock_ordering_no_internal_deadlock` | TODO |
| H-3 | High | `noxu-log/src/log_manager.rs` | (no new test — perf change) | TODO |
| H-4 | High | `noxu-txn/src/lock_manager.rs` | `test_deadlock_victim_fewest_locks` | TODO |
| H-9 | High | `noxu-evictor/src/evictor.rs` | `test_partial_evict_actually_clears_data` | TODO |
| C-9 | Critical | `AGENTS.md` | (documentation) | TODO |
| Q-5 | Low | 12 × `lib.rs` | (compile-time gate) | TODO |

## Audit cross-references

- C-1: audit-2026-05-keith.md F-3.1, audit-2026-05-je-team.md 1-G
- C-2: audit-2026-05-keith.md F-3.2 / F-8.4 / F-9.4
- C-3: audit-2026-05-keith.md F-3.5 / F-9.1
- C-7: audit-2026-05-jonhoo.md 4.4
- H-2: audit-2026-05-keith.md F-6.2
- H-3: audit-2026-05-keith.md F-1.1 / F-1.2
- H-4: audit-2026-05-margo.md 1.4 / 5.6, audit-2026-05-keith.md F-4.4
- H-9: audit-2026-05-margo.md 5.7
- C-9: audit-2026-05-jonhoo.md 4.2
- Q-5: audit-2026-05-jonhoo.md 4.5
