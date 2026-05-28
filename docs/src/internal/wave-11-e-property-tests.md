# Wave 11-E: Property test expansion

Status: **merged**.  Target release: **v2.4.0**.

## Goal

Acceptance gate from `post-v2.3.0-roadmap.md`:

> Net new ≥20 `proptest!` blocks across crates that have light property-test
> coverage today.  Bias toward:
>
> * `noxu-tree` BIN-delta encoding/decoding properties
> * `noxu-recovery` ARIES-style replay invariants
> * `noxu-cleaner` utilization-tracking properties
> * `noxu-rep` Paxos / VLSN streaming properties
> * `noxu-bind` tuple format reverse properties
>
> Any property test that surfaces a real bug commits the failing
> test as `#[ignore]` with a TODO; bug fixes routed to a separate
> wave.

## Outcome

**+39 new properties** across 5 crates (target was ≥20; the wave doubled
that).  **1 surfaced behavior** committed as `#[ignore]`.

| Crate | New properties | Notes |
|---|---|---|
| `noxu-tree` | 7 | BIN-delta merge round-trip + DeltaInfo encode/decode |
| `noxu-bind` | 6 | SortKey reverse round-trip + lex-order preservation |
| `noxu-cleaner` | 10 | Utilization tracker oracle + FileSummary arithmetic |
| `noxu-recovery` | 9 (+1 ignored) | Rollback periods + AnalysisResult txn state |
| `noxu-rep` | 7 | Paxos acceptor + VLSN streaming feeder/replica |
| **Total** | **39 (+1)** | |

## Per-crate detail

### noxu-tree (`crates/noxu-tree/tests/prop_tests.rs`)

DeltaInfo (3):

* `delta_info_roundtrip` — encode/decode round-trip preserves
  (key, lsn, state); consumed exactly `log_size()` bytes.
* `delta_info_encode_deterministic` — encoding the same DeltaInfo
  twice produces byte-identical buffers.
* `delta_info_read_then_write_idempotent` — for any byte sequence
  that successfully decodes, re-encoding produces the same bytes
  (the reverse-direction property the wave brief asked for).

BIN-delta (4):

* `bin_delta_full_roundtrip` — building a delta-shaped BIN with a
  set of dirty (key, lsn, state) updates and merging it into a
  base via `mutate_to_full_bin` produces the same visible
  (key → lsn) mapping as calling `apply_delta_slot` directly on
  the base for each entry.  Oracle test.
* `bin_apply_delta_slot_idempotent` — applying the same delta
  twice equals applying it once.
* `bin_apply_delta_slot_no_duplicates` — `n_entries` grows by
  exactly the number of *new* keys; existing-key updates are
  in-place.
* `bin_apply_delta_slot_writes_lsn_and_state` — the (lsn, state)
  written equals the (lsn, state) supplied.

### noxu-bind (`crates/noxu-bind/tests/prop_tests.rs`)

`SortKey` reverse / order properties (6):

* `prop_sort_key_u32_decode_then_encode` — round-trip + reverse.
* `prop_sort_key_i64_decode_then_encode` — round-trip + reverse.
* `prop_sort_key_i32_order_iff` — `a.cmp(b) == encode(a).cmp(encode(b))`.
* `prop_sort_key_i16_order_iff` — same for i16.
* `prop_sort_key_bytes_roundtrip` — exercises null-byte escaping in
  the variable-length `Vec<u8>` encoding.
* `prop_sort_key_bytes_order_preserving` — byte-wise lex order matches
  `Vec<u8>` order.

### noxu-cleaner (`crates/noxu-cleaner/tests/prop_tests.rs`, new file)

UtilizationTracker oracle (4):

* `prop_tracker_total_size_matches_writes` — per-file
  `total_ln_count`, `total_ln_size` agree with a brute-force scan
  over the Write events.
* `prop_tracker_obsolete_count_matches_oracle` — per-file
  `obsolete_ln_count` agrees with the count of Delete events for
  that file.
* `prop_tracker_file_set_is_union` — the set of tracked file
  numbers equals the union of files referenced by any event.
* `prop_tracker_clear_resets` — `clear()` always restores
  zero-tracking state.

FileSummary arithmetic (4):

* `prop_active_plus_obsolete_eq_total` — `active + obsolete = total_size`.
* `prop_utilization_in_unit_interval` — `get_utilization()` in `[0, 1]`
  for any consistent summary.
* `prop_adjusted_utilization_le_utilization` — TTL-adjusted utilization
  is always ≤ unadjusted (because expired LNs reduce the active
  numerator).
* `prop_summary_add_totals_are_additive` — `FileSummary.add(b)` produces
  the per-field sum (max for `max_ln_size`).

TrackedFileSummary detail accounting (2):

* `prop_tracked_summary_offset_count_matches` — `track_detail = true`
  records every `add_obsolete_offset` call.
* `prop_tracked_summary_no_detail_no_offsets` — `track_detail = false`
  is a strict no-op for offset count.

Also adds `proptest` as a `noxu-cleaner` dev-dependency.

### noxu-recovery (`crates/noxu-recovery/tests/prop_tests.rs`, new file)

RollbackPeriod / RollbackTracker / RollbackScanner (6):

* `prop_rollback_period_boundaries_excluded` — `contains(matchpoint)`
  and `contains(start)` are both false (half-open interval).
* `prop_rollback_period_interior_contained` — every LSN strictly
  between matchpoint and start is contained.
* `prop_rollback_tracker_matches_oracle` — `is_in_rollback_period`
  agrees with a brute-force scan over completed periods.
* `prop_rollback_tracker_period_count` — period count equals the
  number of distinct matchpoints registered with end events.
* `prop_rollback_tracker_periods_sorted` — periods are always
  returned sorted by matchpoint LSN, regardless of insertion order.
* `prop_rollback_scanner_matches_oracle` — RollbackScanner agrees
  with the same oracle.

AnalysisResult txn state machine (3):

* `prop_analysis_txn_state_partition` — for any well-formed event
  trace, the (active, committed, aborted) partition matches the
  oracle reading the events in order.  This is the "applying-then-
  aborting-uncommitted" equivalence the recovery design asserts.
* `prop_analysis_has_active_iff_oracle` — `has_active_txns()` is
  true iff at least one txn never saw a terminal event.  Drives
  the "skip undo phase" optimization.
* `prop_analysis_max_txn_id_monotone` — `max_txn_id` only grows.

#### Surfaced behavior (committed `#[ignore]`)

`prop_active_txn_after_terminal_resurrects_phantom_active` documents
that `record_active_txn` does not defensively check
`committed_txns` / `aborted_txns`.  Calling it after `record_commit`
re-adds the txn to `active_txn_ids`, so `has_active_txns()` reports
a phantom active txn that the undo phase will then attempt to undo.

Counterexample: events = `[Commit(1, lsn), SawActive(1)]`.  Oracle
says `has_active_txns` should be false (the only txn committed); the
impl says it's true.

In production the analysis pass avoids this by ordering events
chronologically; the docstring on `record_active_txn` states the
precondition.  TODO in the test describes the proposed remediation
(defensive check or `debug_assert!`).  Bug fix is routed to a
post-v2.4.0 wave per the Wave 11-E discipline.

Also adds `proptest` as a `noxu-recovery` dev-dependency.

### noxu-rep (`crates/noxu-rep/tests/prop_tests.rs`)

Paxos acceptor (4):

* `prop_acceptor_promised_term_monotone` — `promised_term()` only ever
  grows under arbitrary message arrival order.
* `prop_acceptor_promise_contract` — `try_promise(t)` returning `true`
  yields `promised_term = max(prev, t)`; returning `false` leaves
  state unchanged AND requires `t < prev`.
* `prop_acceptor_accept_contract` — `try_accept(t, m)` returning
  `true` implies `t >= prev_promised`, sets accepted state, and
  bumps promised; returning `false` leaves accepted state unchanged.
* `prop_acceptor_persistence_restart_preserves_promise` — the F5/F31
  invariant: a restart cannot unmake a promise.  Exercises the on-
  disk persistence path end-to-end via tempfile.

VLSN streaming (3):

* `prop_vlsn_index_latest_is_max` — `get_latest_vlsn` is the running
  max of all `put()` arguments.
* `prop_vlsn_index_get_lsn_returns_what_was_put` — at `stride = 1`,
  every registered VLSN looks up its exact (file, offset).
* `prop_vlsn_replica_last_never_exceeds_master` — across any feeder/
  replica interleaving, replica.last ≤ master.last (only VLSNs that
  the master has already written can be observed by the replica).

## Discipline

Per the wave brief:

* Each property is bounded by `ProptestConfig::with_cases(48..256)`.
  All tests complete well under 30s on the developer workstation
  (full prop_tests suite per crate runs in < 1s).
* Production code unchanged.  Dev-dependency additions limited to
  `proptest` for `noxu-cleaner` and `noxu-recovery`.
* Test files extended in place (no parallel files).
* One `#[ignore]` test commit for the surfaced precondition gap.

## Gate status

Final-gate commands run on the wave branch:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS=-D warnings cargo doc --workspace --no-deps
cargo test --workspace --no-fail-fast
make docs-check
```
