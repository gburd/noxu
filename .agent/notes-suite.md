# Debug-suite timing notes (fix/debug-suite-runnable)

Measured in isolation (`cargo test -p <crate> --test <binary> <test> -- --exact`),
debug profile unless noted, on this workstation (8 cores, btrfs-on-LUKS —
`fsync`/`fdatasync` measured at ~2.9ms each here, considerably worse than
NVMe-direct; that is the dominant factor for the two auto-commit-heavy tests).

| Test | Debug time (isolation) | Verdict |
|---|---|---|
| `noxu-db::shared_cache_test shared_cache_balances_one_budget_across_envs` | 194.7s | Genuinely slow in isolation (not just contention). Root cause: TBD — needs read before fixing (evictor `drive_eviction` loop is bounded 300 iters, so likely the 8k-record fill under auto-commit COMMIT_SYNC, one fsync per put x 16,000 puts). Candidate fix: batch into explicit txns like `eviction_pressure_test::large_dataset_sync_load_and_checkpoint_completes` already does (that test is 20.5s despite 200k puts, because it batches 1000 puts/txn). |
| `noxu-db::sustained_load_test test_cleaner_reduces_log_files_under_load` | 79.9s debug / 55.2s release | Same root cause suspected: 500 + 500*49 = 24,990 `db.put()` auto-commit calls, each COMMIT_SYNC (1 fsync each). Release doesn't help much (55s) because fsync latency dominates, not CPU — confirms it's I/O-bound, not compute-bound. Batching into txns is the fix, need to preserve "many small log files -> cleaner has work" intent (64KB log_file_max_bytes already forces many files independent of txn batching). |
| `noxu-spec flexible_paxos::tests::ephemeral_promises_allow_split_brain` | not yet measured | TODO |
| `noxu-db::eviction_pressure_test` (7 tests, `eviction_bounds_cache_and_preserves_data`, `delete_heavy_does_not_inflate_cache_usage`, `cursor_scan_under_eviction_returns_all_data`, `large_dataset_sync_load_and_checkpoint_completes`, `repopulated_read_is_consistent_and_budget_bounded`, `default_cache_mode_keeps_hot_lns_resident`, `stripped_ln_refetch_roundtrips`) | see per-test below | Mixed — some already batch txns and are fast; others use auto-commit `db.put()` in tight loops of tens of thousands and are the slow ones. |
| `noxu-db::read_fault_rss_leak_test read_only_workload_rss_stays_bounded` | 321.6s (release; did not attempt debug, would be worse) | Genuinely does 80,000 writes (auto-commit, no batching) + 700,000 reads. The 700k reads over an 8MiB cache vs ~80MiB dataset are the actual point of the RSS-leak regression test (must fault repeatedly to prove RSS doesn't leak) — read count is core to the property. The 80k unbatched auto-commit writes are setup, not the property; candidate fix batches those writes only. Read count may still need profile-scaling (700k -> smaller in debug) since release already takes 5+ min. |
| `noxu-db::evictor_reclaim_multitree_test evictor_reclaims_to_budget_across_user_dbs` | not yet measured | TODO |

## Per-test breakdown of `eviction_pressure_test` (isolated, debug, timeout 400s each)

| Test | Time | Write pattern |
|---|---|---|
| `eviction_bounds_cache_and_preserves_data` | 156.1s | 50,000x `db.put()` auto-commit, no batching |
| `delete_heavy_does_not_inflate_cache_usage` | 278.8s | 20 rounds x (2000 put + 2000 delete) auto-commit = 80,000 auto-commit ops |
| `cursor_scan_under_eviction_returns_all_data` | 154.6s | 20,000x `db.put()` auto-commit |
| `large_dataset_sync_load_and_checkpoint_completes` | 20.5s | 200,000 puts, but batched 1000/txn — ALREADY FAST despite 4x the record count of the slow tests above. This is the existing precedent inside the same file. |
| `repopulated_read_is_consistent_and_budget_bounded` | 159.5s | 30,000x `db.put()` auto-commit, then 8 cycles of read/evict over a 566-key sample |
| `default_cache_mode_keeps_hot_lns_resident` | 243.3s (release-build binary, but ran under debug harness — was mid-build during first `time`, re-measured clean at 240s+) | 60,000x `db.put()` auto-commit + read-heavy loop |
| `stripped_ln_refetch_roundtrips` | 84.3s | 20,001x `db.put()` auto-commit |

**Pattern confirmed**: every slow test in this whole list does thousands of
auto-commit (`db.put()`, no explicit txn) writes, each of which is
`COMMIT_SYNC` by default -> one `fdatasync` per put. The one fast test in the
same file (`large_dataset_sync_load_and_checkpoint_completes`, 20.5s for
200k records) already batches puts into 1000-record explicit transactions
with one commit (and one fsync) per batch. This is the same class of bug
notify_parent flagged for the xa/dst fixes already on main: a hard-coded
per-operation cost assumption (implicit "fsync is cheap") that was true on
the original dev's NVMe but is not true here (or under load) — not that the
property being tested needs O(n) individual syncs.

**Planned fix per test**: switch the auto-commit write loops to batched
explicit-transaction writes (1000/txn, matching the existing in-file
precedent), which changes debug wall-clock from ~150-280s to an expected
low single digits to ~20s, while keeping record counts (the actual property
under test — working-set-vs-cache-ratio) unchanged. This is priority-1
"scale the work" in spirit but note: it is not shrinking the workload size,
it's removing an unnecessary per-record fsync — the correct fix is a batching
fix, not a smaller-N fix, so record counts stay release-identical (there is
no release/debug split needed for most of these, since the fix is not
size-dependent).

Exception: `read_only_workload_rss_stays_bounded`'s 700k-read loop is the
actual property (must fault many times to prove no leak) and is a genuine
"profile-scale the read count" candidate on top of the write-batching fix.

## Next steps (in order)
1. Fix `eviction_pressure_test::eviction_bounds_cache_and_preserves_data` (batch writes), measure, commit.
2. Repeat for the other 5 slow tests in `eviction_pressure_test.rs`, one commit per test (or one commit for the whole file if the mechanical change is identical across all six — TBD, lean toward one commit per test per task instructions).
3. `sustained_load_test::test_cleaner_reduces_log_files_under_load` — batch writes, verify cleaner-work intent (many small files) still holds with batching.
4. `shared_cache_test::shared_cache_balances_one_budget_across_envs` — same batching fix.
5. `read_fault_rss_leak_test::read_only_workload_rss_stays_bounded` — batch the 80k setup writes; profile-scale the 700k read count if still slow after that.
6. Measure `evictor_reclaim_multitree_test` and `flexible_paxos::ephemeral_promises_allow_split_brain` (not yet looked at).
7. Remove the hand-exclusion filter once all pass under nextest's 120s cap.
8. Update docs/CHANGELOG, run full gates, full nextest run.
