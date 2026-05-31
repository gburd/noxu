# Re-Audit Synthesis — May 2026

**Date**: 2026-05-30
**Codebase**: `origin/main` @ `8f63f6e` (v3.0.2)
**Status**: Wave ZB addresses the items listed below.

Four independent re-auditors reviewed Noxu DB after the v3.0.2 release.
This document indexes their reports and maps each finding to its disposition.

## Reports

| Report | Reviewer persona | Focus |
|---|---|---|
| [reaudit-2026-05-je.md](reaudit-2026-05-je.md) | BDB-JE original team | Correctness, JE parity, test quality |
| [reaudit-2026-05-margo.md](reaudit-2026-05-margo.md) | Margo Seltzer | Algorithms, doc drift, Stateright specs |
| [reaudit-2026-05-keith.md](reaudit-2026-05-keith.md) | Keith Bostic | Performance, crash-safety correctness |
| [reaudit-2026-05-jonhoo.md](reaudit-2026-05-jonhoo.md) | Jon Gjengset (jonhoo) | Rust idiom, API ergonomics, Cargo hygiene |

## Wave ZB disposition (fix/zb-stale-docs)

Items handled in Wave ZB (this wave):

| Finding | Source | Action |
|---|---|---|
| JE F-3 | Stale TODO(bug) comments in test files | Fixed: updated 5 comments to "regression guard" |
| JE F-4 / margo 1.2 | C-6 TODO drift in recovery_manager.rs | Fixed: updated TODOs to reflect wave-11-y completion |
| JE F-8 | verify_environment/verify_database stubs | Fixed: added `log::warn!` + rustdoc stub notice |
| margo 2.1 | recovery.md omits C-6 mapping-tree undo pass | Fixed: added Phase 2b section |
| margo 2.3A/B | crate-guide.md says "19 crates", "no derive macros" | Fixed: updated to 22 crates, added derive crate entries |
| margo 1.3 | algorithms.md victim selection understates H-4 | Fixed: "fewest locks" as primary criterion |
| margo 4.1 | No design decision for umbrella crate architecture | Fixed: added Decision 9 |
| margo 4.2 | No design decision for cache_size total budget | Fixed: added Decision 10 |
| margo 4.3 | No design decision for mTLS Phase 2 not enforced | Fixed: added Decision 11 |
| margo 4.4 | "Noxu and Noxu" confusion in design-decisions.md | Fixed |
| margo 4.5 | off_heap.rs in unsafe table (removed in prior wave) | Fixed: removed row |
| margo 3.1 | recovery_three_phase.rs cites non-existent files | Fixed: corrected to analysis_result.rs / dirty_in_map.rs |
| margo 3.2 | vlsn_streaming.rs cites vlsn.rs (DNE) | Fixed: corrected to vlsn/mod.rs |
| margo 3.3 | All 11 specs stamped v2.4.0 | Fixed: re-stamped to v3.1.0 with per-spec notes |
| margo 5.4 / jonhoo U-1 | Umbrella quick-start API bugs (open_database + put) | Fixed: corrected + changed ignore→no_run |
| jonhoo U-2 | README db.get called with 4 args | Fixed |
| jonhoo U-3 (minimal) | noxu-persist doc contradicts umbrella requirement | Fixed: updated doc example + added warning notice |
| jonhoo C-1 | No rust-version in [workspace.package] | Fixed: added rust-version = "1.85" |
| jonhoo C-2 | Workspace lints vestigial | Fixed: added unsafe_op_in_unsafe_fn + undocumented_unsafe_blocks |
| margo 2.5 | recovery_manager.rs "3-phase" / "five sub-phases" | Fixed: updated module preamble + fn docs |
| margo 1.1 | recover() omits catalog undo — undocumented | Fixed: documented intentional asymmetry in fn doc |
| margo 5.2/5.3 | Wave-reference comments in recovery_manager.rs | Fixed: replaced wave labels with descriptive text |

Items **not** addressed in Wave ZB (deferred):

| Finding | Reason deferred |
|---|---|
| jonhoo U-3 full (crate= escape hatch in derive) | Scope risk; tracked as Wave ZC candidate |
| jonhoo U-4 (PreparedTxnInfo not re-exported) | Wave ZA scope (API re-exports) |
| jonhoo U-5 (pub use noxu_db::* exposes modules) | Wave ZA scope |
| jonhoo U-6 (umbrella missing forbid(unsafe_code)) | Wave ZA scope |
| jonhoo E-2 ('txn lifetime on DbIter) | API stability boundary (v4.0 candidate) |
| jonhoo S-1/S-2 (std::sync::Mutex in XA/dbi) | Wave ZC scope (correctness) |
| JE F-1 (7 config params ignored) | Wave ZC scope |
| JE F-2 (peer_allowlist security trap) | Wave ZC scope (already has known-limitations entry) |
| JE F-5 (1526 NOT-PORTED tests) | Ongoing porting effort |
| JE F-6 (SharedReplicaAckCoordinator not re-exported) | Wave ZA scope |
| Keith R-1..R-7, P-1..P-3 | Wave ZC scope (performance + crash-safety) |
| margo 2.6 (LogFlushTask missing from sizing.md) | Low priority; follow-up |
| margo 2.7 (wave-reference in known-limitations.md) | Low priority; follow-up |
| margo 6.1/6.2 (NameLnRecord invariant doc, debug_assert) | Wave ZC scope |
| spec CatalogConsistency property | Tracked in recovery_three_phase.rs TODO |
| spec VlsnMonotone checkpoint-cap modelling | Tracked in vlsn_streaming.rs NOTE |
| AGENTS.md crate count | Will update separately |

## Cross-references

- Wave ZA (`fix/za-config-api`): API re-exports, environment_impl TOCTOU, db_iter
- Wave ZB (`fix/zb-stale-docs`): this wave — docs, comments, examples, Cargo hygiene
- Wave ZC (planned): performance, crash-safety internals, config no-ops, XA Mutex
