# Wave ZB — Stale Docs, Comments, and Cargo Hygiene

**Branch**: `fix/zb-stale-docs`
**Target release**: v3.1.0
**Date**: 2026-05-30
**Status**: Complete

## Motivation

Four independent re-auditors reviewed Noxu DB at `origin/main` (v3.0.2) on
2026-05-30. Their reports (`reaudit-2026-05-{je,margo,keith,jonhoo}.md`) surfaced
a cluster of documentation drift, stale comments, broken examples, and Cargo
hygiene gaps that are safe to fix without touching production logic. Wave ZB
addresses all items in this cluster.

## Items Completed

### Item 1 — Broken front-door examples (HIGH)

**Sources**: reaudit-jonhoo U-1/U-2, reaudit-margo 5.4

- `crates/noxu/src/lib.rs` Quick-start: fixed two API bugs
  (`open_database` third arg `bool` → `&DatabaseConfig`; `db.put` arg types),
  changed `` ```ignore `` → `` ```no_run `` so the example is compile-checked.
- `crates/noxu/src/lib.rs` derive example: changed `` ```ignore `` →
  `` ```no_run ``.
- `README.md` line 68: `db.get(None, &key, &mut result, None)` → 3-arg form.
- `crates/noxu-persist/src/lib.rs`: fixed doc example to use
  `use noxu::persist::…` (not `noxu_persist::…`) and added a boxed warning
  that derive macros require the `noxu` umbrella.

### Item 2 — Stale TODO(bug) comments (MEDIUM)

**Source**: reaudit-je F-3

Five tests in `crates/noxu-db/tests/` had `TODO(bug)` comments claiming
active bugs that were fixed in commits 90918c5–b947b34:

- `je_database_test.rs`: 4 comments updated to "regression guard" framing
- `je_truncate_test.rs`: 1 comment updated

Test logic is unchanged; only the comment text was updated.

### Item 3 — C-6 residual TODOs (MEDIUM)

**Sources**: reaudit-je F-4, reaudit-margo 1.1/1.2/2.5/5.2/5.3

`crates/noxu-recovery/src/recovery_manager.rs`:

- Module preamble: replaced "3-phase" with accurate description of single-DB
  vs multi-DB phases.
- `RecoveryManager` struct doc: updated to describe both recovery paths.
- `mapping_tree_db_names` field doc: replaced stale C-6 TODO with accurate
  completion status (wave-11-y NameLNTxn write-path done; MapLN B-tree undo
  is the known remaining gap). Updated tracking link from wave-11-r to
  wave-11-y.
- `run_mapping_tree_undo_pass` TODO: split into "completed" and "known gap"
  sections with accurate descriptions.
- `recover()` fn doc: replaced "five sub-phases" with accurate 3-phase
  description; added explicit note documenting the intentional asymmetry
  with `recover_all()` (no catalog entries in single-DB path).
- `recover_all()` fn doc: updated to describe all 4 logical phases including
  the C-6 mapping-tree undo pass.
- `RecoveryScratch` doc: removed "Wave 11-K optimisation (Fix 2)" label;
  restructured as forward-compatibility hook description.
- `RecoveryStats::prepared_txns` doc: removed "Wave 3-2" label.
- Multiple inline comments: replaced "Wave 3-2:" / "Wave 11-K (Fix N):"
  with descriptive text.

### Item 4 — verify() stubs silently passing (LOW)

**Source**: reaudit-je F-8

`crates/noxu-engine/src/verify.rs`:

- `verify_environment`: added `log::warn!` + updated rustdoc to say "Stub —
  not yet implemented; result does not reflect a real integrity check."
- `verify_database`: same treatment.

### Item 5 — Documentation drift (HIGH/MEDIUM)

**Source**: reaudit-margo

- `docs/src/reference/recovery.md`: added Phase 2b (Mapping-Tree Undo Pass)
  section describing C-6, when it runs (multi-DB only), what invariant it
  enforces, and how it relates to JE's `_jeNameTree`.

- `docs/src/maintainer/crate-guide.md`:
  - Updated "All 19 crates" → "All 22 crates".
  - Replaced "no derive macros today" with accurate description of
    `noxu-persist-derive`.
  - Added `noxu-persist-derive` section.
  - Added `noxu` (umbrella) section.
  - Added `noxu-spec` section.

- `docs/src/maintainer/algorithms.md`:
  - Victim selection: updated to "fewest locks held (primary); youngest on
    ties" to reflect the H-4 (wave-11-Q) fix.
  - Recovery section: renamed to "Recovery Protocol" and added step 2b
    (mapping-tree undo, multi-DB only).

- `docs/src/maintainer/design-decisions.md`:
  - Fixed "Noxu and Noxu" in Decision 3 → "Noxu DB tools cannot read
    BDB-JE log files."
  - Removed stale `off_heap.rs` row from the unsafe table (off_heap.rs is
    now zero-unsafe; AGENTS.md was already fixed in a prior wave).
  - Added Decision 9: Single Umbrella Crate + derive path coupling.
  - Added Decision 10: `cache_size` = total memory budget (X-12).
  - Added Decision 11: mTLS Phase 1 landed, Phase 2 not yet wired.

### Item 6 — Stateright spec stamps + missing property note (MEDIUM)

**Source**: reaudit-margo 3.1/3.2/3.3

All 7 specs with `VALIDATED-AS-OF: v2.4.0` stamps updated to `v3.1.0`:

| Spec | Change |
|---|---|
| `btree_latching.rs` | Re-stamped; protocol unchanged |
| `cache_vs_cleaner.rs` | Re-stamped; added NOTE: X-7 per-db dispatch not modelled |
| `cleaner_safety.rs` | Re-stamped; added NOTE: X-7 per-db dispatch not modelled |
| `lock_manager_deadlock.rs` | Re-stamped; added NOTE: H-4 changes victim selection (VictimIsYoungest over-specified) |
| `recovery_three_phase.rs` | Re-stamped; fixed stale file citations; added C-6 CatalogConsistency TODO |
| `wal_commit.rs` | Re-stamped; protocol unchanged |
| `xa_two_phase_commit.rs` | Re-stamped; X-4 fix noted |
| `vlsn_streaming.rs` | Fixed `vlsn.rs` → `vlsn/mod.rs` citation; added NOTE: X-2 checkpoint-cap not modelled |

### Item 7 — Cargo/idiom hygiene (MEDIUM)

**Source**: reaudit-jonhoo C-1/C-2

`Cargo.toml` (`[workspace.package]`):

- Added `rust-version = "1.85"` (edition 2024 minimum; toolchain pins 1.95).

`Cargo.toml` (`[workspace.lints]`):

- Added `[workspace.lints.rust]` section with `unsafe_op_in_unsafe_fn = "deny"`.
- Added `clippy::undocumented_unsafe_blocks = "warn"`.

### Item 8 — persist derive doc fix (minimal)

**Source**: reaudit-jonhoo U-3

The full `#[entity(crate = "…")]` escape hatch is out of scope for Wave ZB.
Instead:

- `crates/noxu-persist/src/lib.rs`: corrected both doc examples to use
  `use noxu::persist::…` import paths; added a boxed `> **Note on derive macros**`
  warning explaining that derive macros emit `::noxu::persist::` paths and
  require the `noxu` umbrella crate.
- The escape hatch (`#[entity(crate = "…")]` following the serde pattern) is
  documented as a follow-up in design-decisions.md Decision 9.

### Reaudit reports archived

Four re-audit reports copied into `docs/src/internal/`:

- `reaudit-2026-05-je.md`
- `reaudit-2026-05-margo.md`
- `reaudit-2026-05-keith.md`
- `reaudit-2026-05-jonhoo.md`
- `reaudit-2026-05-synthesis.md` (this synthesis + Wave ZB disposition table)

All five added to `docs/src/SUMMARY.md`.

## Items Deferred

| Item | Finding | Reason |
|---|---|---|
| derive `crate=` escape hatch | jonhoo U-3 | Scope/risk; tracked in Decision 9 |
| PreparedTxnInfo re-export | jonhoo U-4 | Wave ZA scope |
| `pub use noxu_db::*` surface | jonhoo U-5 | Wave ZA scope |
| `forbid(unsafe_code)` on umbrella | jonhoo U-6 | Wave ZA scope |
| `'txn` lifetime on DbIter | jonhoo E-2 | v4.0 API stability candidate |
| std::sync::Mutex in XA/dbi | jonhoo S-1/S-2 | Wave ZC scope |
| 7 ignored config params | JE F-1 | Wave ZC scope |
| peer_allowlist security | JE F-2 | known-limitations already updated; Wave ZC |
| 1526 NOT-PORTED tests | JE F-5 | Ongoing porting effort |
| SharedReplicaAckCoordinator export | JE F-6 | Wave ZA scope |
| Keith R-1..R-7, P-1..P-3 | Performance/crash-safety | Wave ZC scope |
| LogFlushTask in sizing.md | margo 2.6 | Low priority; follow-up |
| wave-refs in known-limitations.md | margo 2.7 | Low priority; follow-up |
| NameLnRecord invariant doc | margo 6.1 | Wave ZC scope |
| debug_assert for arbiter floor | margo 6.2 | Wave ZC scope |
| CatalogConsistency spec property | margo 3.3 | Documented as TODO in spec |
| VlsnMonotone checkpoint-cap | margo 3.3 | Documented as NOTE in spec |

## Gate Results

```
cargo fmt --all -- --check          ✓
cargo clippy --workspace --all-targets -- -D warnings   ✓
RUSTDOCFLAGS=-D warnings cargo doc --workspace --no-deps  ✓
timeout 900 cargo test --workspace --no-fail-fast       ✓
make docs-check                     ✓
```
