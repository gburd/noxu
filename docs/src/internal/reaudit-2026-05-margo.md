# Noxu DB — Fresh Re-Audit: Lingering Drift at `origin/main`

**Reviewer**: Margo Seltzer (re-audit)
**Date**: 2026-05-30
**Branch under review**: `origin/main` (8f63f6e) — post-wave-11-U, 11-V, 11-X, 11-Y, umbrella
**Worktree**: `/tmp/reaudit-margo`
**Prior audits read**: `audit-2026-05-margo.md`, `audit-2026-05-synthesis.md`, wave docs Q/R/S/U/V/X/Y and noxu-umbrella

---

## Methodology

Prior findings from the May-2026 Margo audit and synthesis that were addressed
in waves 11-Q (C-1…C-9, H-2…H-4, H-9, Q-5), 11-R (C-4, C-5, C-6 partial,
C-8), 11-S (H-1, H-3, H-5, H-6, H-7, H-8, Q-1, Q-2, Q-6, Q-7), 11-U (X-2,
X-7, X-8, C-6 completion), 11-V (voice cleanup), 11-X (X-4, X-10, X-11,
X-12), 11-Y (C-6 end-to-end), and the umbrella wave are **not re-reported
below** except where a wave explicitly did NOT complete its claimed fix or where
new drift was introduced by the fix itself.

This audit focuses on: (1) doc drift from the major refactors, (2) algorithm
partials and correctness concerns in the new code, (3) invariants stated vs.
tested for new structures, (4) Stateright spec staleness, (5) comment drift in
churned files, and (6) design-decision honesty for new architectural choices.

---

## Section 1 — Algorithm Correctness and Partials

### 1.1 C-6 Mapping-Tree Undo Pass Omitted from `recover()` Single-DB Path

| Attribute | Value |
|---|---|
| **Severity** | Low |
| **Subsystem** | noxu-recovery |
| **File:line** | `crates/noxu-recovery/src/recovery_manager.rs:329, 419` |

**What the code says**: `recover()` (single-DB) and `recover_all()` (multi-DB)
are documented as equivalent except for the tree-dispatch model. But only
`recover_all()` calls `run_mapping_tree_undo_pass()` between analysis and redo.

**The gap**: `recover()` does not run the catalog undo pass. For single-DB
environments with no catalog entries this is harmless (nothing to undo), but
the omission is undocumented. A future caller using `recover()` for a scenario
with NameLN entries would silently skip catalog undo.

**Suggested action**: Add a comment to `recover()` explicitly stating "this
path has no catalog (NameLN) entries, so the mapping-tree undo pass is omitted;
multi-DB recovery uses `recover_all()` which does run the pass."

---

### 1.2 `run_mapping_tree_undo_pass` TODO Comment is Stale After Wave 11-Y

| Attribute | Value |
|---|---|
| **Severity** | Medium |
| **Subsystem** | noxu-recovery |
| **File:line** | `crates/noxu-recovery/src/recovery_manager.rs:591–597` |

**What the comment says**:

```
# TODO (C-6 full JE parity)
- Store NameLN `txn_id` in the WAL entry so recovery can distinguish
  committed vs. aborted NameLNs.  Currently NameLNs are logged with
  `txn_id = None` (non-transactional), so this pass can only remove
  entries that C-4 deferred (post-v3.0.0 WAL) — pre-C4 WAL entries
  remain unconditionally accepted.

```

**What wave 11-Y actually did**: `environment_impl.rs:1083–1087` now calls
`log_name_ln_txn(lm, name, db_id, txn_id)` inside the creating transaction
(`NameLNTxn`, `Provisional::Yes`). `commit_pending_database` no longer writes
a second `NameLN`. This TODO is therefore fulfilled — the NameLN is written
with a `txn_id` inside the transaction.

**The live TODO**: Only the second bullet (MapLN B-tree undo) remains valid.

**Suggested action**: Remove the first bullet point from the TODO. Rewrite to:

```
# TODO (C-6 partial — MapLN B-tree undo not yet implemented)
A full MapLN undo pass (JE phases A–D on the mapping tree B-tree) requires
a dedicated on-disk mapping tree, not a HashMap.  The current implementation
covers only NameLNTxn undo; tracked as a future follow-up.

```

---

### 1.3 `algorithms.md` Victim Selection Understates H-4 Fix

| Attribute | Value |
|---|---|
| **Severity** | Medium |
| **Subsystem** | docs/maintainer |
| **File:line** | `docs/src/maintainer/algorithms.md:68–69` |

**What the doc says**:
> "Cycle detection: DFS from each node. Youngest transaction (by txn_id) in the
> cycle is selected as victim."

**What the code does** (after wave 11-Q H-4 fix, `lock_manager.rs:963–995`):
`compute_lock_counts()` tallies all locks held by each locker in the cycle by
walking every shard. `select_victim()` then uses fewest-locks-held as primary
criterion, youngest locker-ID as tiebreaker. The empty-map (youngest-only)
behaviour no longer occurs in production.

**The drift**: The doc says ONLY "youngest transaction" but the actual primary
criterion is "fewest locks held." The doc was written before H-4 was fixed and
was not updated when the fix landed in wave 11-Q.

**Suggested action**: Change `algorithms.md:68–69` to:
> "Cycle detection: DFS from each node. Victim is the locker holding the
> fewest locks; ties are broken by youngest locker ID (highest ID).
> Lock counts are tallied via `compute_lock_counts()` — O(shards) on the rare
> cycle path."

---

### 1.4 mTLS Phase 2 and 3 Not Landed; `known-limitations.md` Doesn't Reflect Phase 1

| Attribute | Value |
|---|---|
| **Severity** | Medium |
| **Subsystem** | noxu-rep + docs/operations |
| **File:line** | `docs/src/operations/known-limitations.md:12`, `crates/noxu-rep/src/auth.rs:16–19` |

**What `known-limitations.md` says**:
> "The replication wire protocol has no authentication. A peer identity
> (`group_name`, `node_name`) is self-claimed plaintext and not verified."

**What the code does**: mTLS Phase 1 (`auth.rs`, `TlsConfig::for_replication`,
`RepConfig::with_peer_allowlist`) has landed. `PeerAllowlistVerifier` implements
`ClientCertVerifier` and `ServerCertVerifier`. But `tls.rs:305` shows the
`to_rustls_server_config()` still calls `.with_no_client_auth()` — Phase 2
(wiring the verifier to the dispatcher) and Phase 3 (running client cert
verification) have NOT landed as explicitly confirmed in `auth.rs:16–19`:
"Phase 2 has not landed yet."

**The drift**: `known-limitations.md` says "no authentication" (pre-Phase-1
language). The actual state is: certificate-based mTLS infrastructure exists
but peer allowlist enforcement is not wired to the server config. The gap is
real but more nuanced than stated.

**Suggested action**: Update the mTLS limitation bullet in `known-limitations.md`:
> "Peer authentication (mTLS) infrastructure (Phase 1) is in place:
> `TlsConfig::for_replication()` and `RepConfig::with_peer_allowlist()` are
> implemented. However, Phase 2 (wiring `PeerAllowlistVerifier` to the
> `TcpServiceDispatcher` server config) has not landed. The server still accepts
> unauthenticated connections at the transport layer (`with_no_client_auth()`).
> Deploy only on trusted networks until Phase 2 is merged."

---

## Section 2 — Documentation Drift After Major Refactors

### 2.1 `recovery.md` Doesn't Describe C-6 Mapping-Tree Undo Pass

| Attribute | Value |
|---|---|
| **Severity** | High |
| **Subsystem** | docs/reference |
| **File:line** | `docs/src/reference/recovery.md:1–35` |

**What the doc says**: "three-phase crash recovery" — Phase 1: Find End, Phase
2: Build Tree from Checkpoint, Phase 3: Replay and Undo LNs.

**What `recover_all()` actually does** (post-wave-11-U and 11-Y):

1. Find end of log
2. Find last checkpoint
3. Analysis (build dirty-IN map and transaction sets)
4. **Mapping-tree undo pass** — removes aborted NameLNTxn entries from
   `recovered_db_names` before data-LN redo begins (C-6 fix)
5. Redo committed LNs
6. Undo uncommitted LNs

The mapping-tree undo pass is entirely absent from `recovery.md`. A reader of
the reference docs will not know the catalog is corrected before data redo, nor
understand why NameLNTxn entries must carry a `txn_id`.

**Suggested action**: Add a "Mapping-Tree Undo Pass (v3.0.0)" section to
`recovery.md` between Phase 2 and Phase 3, describing the C-6 pass and its
relationship to `open_database(txn)` transactional semantics.

---

### 2.2 `algorithms.md` Three-Phase Recovery Doesn't Mention Mapping-Tree Pass

| Attribute | Value |
|---|---|
| **Severity** | Medium |
| **Subsystem** | docs/maintainer |
| **File:line** | `docs/src/maintainer/algorithms.md` (Three-Phase Recovery section) |

**What the doc says**: "3. Redo committed / undo uncommitted: scan from
`first_active_lsn`"

**What is missing**: The mapping-tree undo pass between analysis and main redo.
The section currently makes no mention of the catalog consistency step.

**Suggested action**: Expand step 3 to read:
> "2b. Mapping-tree undo (multi-DB only): for each NameLNTxn entry whose
> `txn_id` did not commit, remove the database registration before data-LN
> redo. Prevents data recovery for databases whose creation was rolled back."
>
> "3. Redo committed / undo uncommitted LNs: scan from `first_active_lsn`."

---

### 2.3 `crate-guide.md` Says "19 crates" and "No Derive Macros"

| Attribute | Value |
|---|---|
| **Severity** | High |
| **Subsystem** | docs/maintainer |
| **File:line** | `docs/src/maintainer/crate-guide.md:3, 196–197` |

**Issue A** — crate count wrong:
The document says "All 19 crates in the Noxu DB workspace". The actual
workspace `Cargo.toml` has 22 `[workspace.members]` entries:
`noxu-sync`, `noxu-util`, `noxu-latch`, `noxu-config`, `noxu-log`,
`noxu-tree`, `noxu-txn`, `noxu-evictor`, `noxu-cleaner`, `noxu-recovery`,
`noxu-dbi`, `noxu-engine`, `noxu-db`, `noxu-bind`, `noxu-collections`,
`noxu-persist`, `noxu-persist-derive`, `noxu-rep`, `noxu-xa`, `noxu-observe`,
`noxu` (umbrella), `noxu-spec`. The guide has no entries for `noxu`,
`noxu-persist-derive`, or `noxu-spec`.

**Issue B** — `noxu-persist` entry says (lines 196–197):
> "There are no derive macros today — all wiring is by trait impl."

This is false: `noxu-persist-derive` provides `#[derive(Entity)]`,
`#[derive(PrimaryKey)]`, and `#[derive(SecondaryKey)]`. The umbrella crate
re-exports them at `noxu::persist::*`. This statement was accurate before the
`noxu-persist-derive` crate was added but was never updated.

**Suggested action**: Update crate count to 22, add entries for `noxu`,
`noxu-persist-derive`, `noxu-spec`. Remove "no derive macros" sentence from the
`noxu-persist` section.

---

### 2.4 `AGENTS.md` Still Says "19 crates under crates/"

| Attribute | Value |
|---|---|
| **Severity** | Low |
| **Subsystem** | AGENTS.md |
| **File:line** | `AGENTS.md:20` |

**What it says**: "a Cargo workspace with 19 crates under `crates/`"
**Actual count**: 22 crates (see 2.3 above).

**Suggested action**: Update to "22 crates" and add rows for `noxu`,
`noxu-persist-derive`, `noxu-spec` to the crate table.

---

### 2.5 `recovery_manager.rs` Module/Function Comments Still Say "3-Phase" After C-6

| Attribute | Value |
|---|---|
| **Severity** | Medium |
| **Subsystem** | noxu-recovery |
| **File:line** | `crates/noxu-recovery/src/recovery_manager.rs:4, 205, 329, 332, 419` |

**Lines 4, 205**: Module-level: "Performs 3-phase recovery when an Environment
is opened."

**Line 329**: `recover()` fn doc: "Perform full 3-phase recovery…"

**Line 332**: Same fn doc: "orchestrating all five sub-phases."

**Line 419**: `recover_all()` fn doc: "Multi-database 3-phase recovery."

**The drift**: After C-6, `recover_all()` has six sub-phases (find-end,
find-checkpoint, analysis, mapping-tree-undo, redo, undo). The module and
function docs use inconsistent labels ("3-phase" vs "five sub-phases") that
are both now wrong for the multi-DB path.

**Suggested action**:

- Module preamble: "Performs crash recovery (analysis → redo → undo) when an
  Environment is opened. Multi-DB environments also run a mapping-tree undo
  pass (C-6) between analysis and main redo."
- `recover()` fn doc: "Single-database 3-phase recovery (analysis, redo, undo)."
- `recover_all()` fn doc: "Multi-database recovery with 4 logical phases:
  analysis, mapping-tree undo (catalog consistency), redo, undo."

---

### 2.6 `sizing.md` Thread Pool Table Missing `LogFlushTask` Daemon

| Attribute | Value |
|---|---|
| **Severity** | Low |
| **Subsystem** | docs/operations |
| **File:line** | `docs/src/operations/sizing.md` (thread pool table) |

**What the doc says**: 5 daemon threads: Checkpointer, Cleaner, Evictor,
INCompressor, FsyncManager.

**What the code does**: Wave 11-X (X-11) added a `LogFlushTask` background
daemon thread in `EnvironmentImpl`. When `log_flush_no_sync_interval_ms > 0`,
a 6th daemon thread `noxu-log-flusher` starts and wakes on the configured
interval.

**Suggested action**: Add a row:

```
| LogFlushTask | 0 or 1 | conditional on log_flush_no_sync_interval_ms > 0 |

```

---

### 2.7 `known-limitations.md` Contains Wave-Reference Language

| Attribute | Value |
|---|---|
| **Severity** | Low |
| **Subsystem** | docs/operations |
| **File:line** | `docs/src/operations/known-limitations.md:58–60` |

**What the doc says** (three rows in the limitations table):

- "These `Get` enum variants return `NoxuError::Unsupported` at runtime
  (**Wave 11-R audit finding 3-D**)."
- "Noxu has only `get_stats()` (**Wave 11-R audit finding 3-C**)."
- "not exposed as a public API (**Wave 11-R audit finding 3-F**)."

These are process-artifact references in a user-facing operations doc. Wave
11-V cleaned similar language from other user-facing docs but missed
`known-limitations.md`.

**Suggested action**: Remove the "(Wave 11-R audit finding X-Y)" parentheticals.
The limitation itself is correctly stated; the process provenance is internal.

---

## Section 3 — Stateright Specs vs Current Code

### 3.1 `recovery_three_phase.rs` Cites Non-Existent Files

| Attribute | Value |
|---|---|
| **Severity** | Medium |
| **Subsystem** | noxu-spec |
| **File:line** | `crates/noxu-spec/src/recovery_three_phase.rs:10–11` |
| **[prior]** | [prior: Margo findings 5.4; synthesis H-6] |

**What the spec says**:

```
//!   - `crates/noxu-recovery/src/transaction_table.rs`
//!   - `crates/noxu-recovery/src/dirty_page_table.rs`

```

**What exists**: `ls crates/noxu-recovery/src/` shows:
`analysis_result.rs`, `dirty_in_map.rs`, `recovery_manager.rs`, etc.
`transaction_table.rs` and `dirty_page_table.rs` do not exist.

**Status**: This was flagged in the prior Margo audit (finding 5.4) and the
synthesis (noting "ARIES terminology that doesn't match Noxu's naming"). It was
NOT fixed in any subsequent wave (checked 11-Q, 11-R, 11-S, 11-U, 11-V).

**Suggested action**: Update lines 10–11 to:

```
//!   - `crates/noxu-recovery/src/analysis_result.rs`   (≈ ARIES transaction table)
//!   - `crates/noxu-recovery/src/dirty_in_map.rs`       (≈ ARIES dirty page table)

```

---

### 3.2 `vlsn_streaming.rs` Spec Still Cites Dead File Path

| Attribute | Value |
|---|---|
| **Severity** | Low |
| **Subsystem** | noxu-spec |
| **File:line** | `crates/noxu-spec/src/vlsn_streaming.rs:12` |
| **[prior]** | [prior: Margo findings 1.9 and 5.5] |

**What the spec says**: `crates/noxu-rep/src/vlsn.rs`

**What exists**: The VLSN entry point is a module directory:
`crates/noxu-rep/src/vlsn/mod.rs`. The spec correctly cites
`crates/noxu-rep/src/vlsn/persist.rs` on the next line but the first
citation points to a plain file that doesn't exist.

**Status**: Carry-over from prior audit; never fixed.

**Suggested action**: Change line 12 to `crates/noxu-rep/src/vlsn/mod.rs`.

---

### 3.3 All `VALIDATED-AS-OF: v2.4.0` Stamps Are Stale After Multiple Behavioral Waves

| Attribute | Value |
|---|---|
| **Severity** | Medium |
| **Subsystem** | noxu-spec (all 11 specs) |
| **File:line** | `crates/noxu-spec/src/{btree_latching,cache_vs_cleaner,cleaner_safety,lock_manager_deadlock,recovery_three_phase,wal_commit,xa_two_phase_commit}.rs` |

**What the specs say**: `VALIDATED-AS-OF: v2.4.0 — Wave 11-F audit confirmed…`

**What has changed since v2.4.0**: Waves 11-Q, 11-R, 11-U, 11-X, 11-Y
introduced significant behavioral changes that these specs should be re-validated
against:

| Spec | Behavioral change since v2.4.0 |
|---|---|
| `recovery_three_phase` | C-6 (waves 11-U, 11-Y): mapping-tree undo pass added; not modelled |
| `lock_manager_deadlock` | H-4 (wave 11-Q): fewest-locks victim selection now wired in production |
| `cleaner_safety` | X-7 (wave 11-U): per-db tree dispatch added to cleaner migration; not modelled |
| `vlsn_streaming` | X-2 (wave 11-U): VLSN persistence now capped at checkpoint end LSN; not modelled |

**Additional gap**: `recovery_three_phase.rs` has an `AllAndOnlyCommitted`
property that verifies data LNs. After C-6 there is a new catalog invariant:
"after recovery, the db name registry contains only databases whose creating
transactions committed." No spec property covers this.

**Suggested action**:

1. Update all 11 `VALIDATED-AS-OF` stamps to the current version after
   re-validation with `make spec`.
2. Add a `CatalogConsistency` property to `recovery_three_phase.rs` modelling
   the NameLNTxn undo predicate.
3. Add X-2 VLSN-cap modelling to `vlsn_streaming.rs`.

---

## Section 4 — Design-Decision Honesty

### 4.1 No Design-Decision Entry for Umbrella Crate Architecture

| Attribute | Value |
|---|---|
| **Severity** | Medium |
| **Subsystem** | docs/maintainer |
| **File:line** | `docs/src/maintainer/design-decisions.md` (missing entry) |

**Decision not documented**: The `noxu` umbrella crate was added in v3.0.1 as
the primary user-facing crate. `noxu-persist-derive` was retargeted to emit
`::noxu::persist::…` paths (rather than `::noxu_persist::…`). This has a
significant consequence: any crate using `#[derive(Entity)]` must now have
`noxu` (not just `noxu-persist`) as a dependency.

This is a user-visible coupling decision with non-obvious implications for
downstream consumers who depended on individual crates. It exists only in
`docs/src/internal/noxu-umbrella.md` (internal doc), not in the maintained
design-decisions page.

**Suggested action**: Add a design decision:
> "Single Umbrella Crate (`noxu = "3"`): All component crates are accessible
> through one umbrella. `noxu-persist-derive` emits `::noxu::persist::…` paths
> so derive-macro users must depend on `noxu` directly. Components are still
> individually publishable for engine-internal users."

---

### 4.2 No Design-Decision Entry for `cache_size` = Total Budget (X-12)

| Attribute | Value |
|---|---|
| **Severity** | Medium |
| **Subsystem** | docs/maintainer |
| **File:line** | `docs/src/maintainer/design-decisions.md` (missing entry) |

**Decision not documented**: X-12 (wave 11-X) changed `cache_size` semantics
from "BIN tree pool ceiling" to "total memory ceiling." This is a breaking
change documented in `configuration.md` and `sizing.md` (good) but not in
`design-decisions.md`. Users upgrading from v2.x who encounter changed behaviour
under memory pressure need to understand *why* the semantics changed, not just
the migration recipe.

**Suggested action**: Add a design decision documenting the X-12 choice and the
rationale (JE semantics, eliminate surprise with log-buffer+off-heap expansion).

---

### 4.3 No Design-Decision Entry for mTLS Not Yet Enforced

| Attribute | Value |
|---|---|
| **Severity** | Medium |
| **Subsystem** | docs/maintainer |
| **File:line** | `docs/src/maintainer/design-decisions.md` (missing entry) |

**Decision not documented**: mTLS Phase 1 has landed but Phase 2 (wiring
`PeerAllowlistVerifier` to the dispatcher) and Phase 3 (enabling client cert
verification on the server side) have not. This is a deliberate staged
approach, but the design rationale (why Phase 2 was deferred, what triggers
landing it) is only in the internal `auth-mtls-design-2026-05.md`. A user
who enables `TlsConfig::for_replication()` and `with_peer_allowlist()` will
find the allowlist is not actually enforced at the connection level.

**Suggested action**: Add a decision entry explicitly stating the current
deployment posture and the Phase 2 trigger conditions.

---

### 4.4 "Noxu and Noxu" Confusion in Decision 3 Not Yet Fixed

| Attribute | Value |
|---|---|
| **Severity** | Medium |
| **Subsystem** | docs/maintainer |
| **File:line** | `docs/src/maintainer/design-decisions.md:52` |
| **[prior]** | [prior: Margo findings 4.3 and 5.8] |

**What the doc says**:
> "Noxu tools cannot read Noxu log files. Migration between Noxu and Noxu
> requires an export/import step at the application level."

This reads as "Noxu cannot read its own log files." Wave 11-S Q-7 only fixed
occurrences in `crates/noxu-db/src/database.rs`, not this `design-decisions.md`
entry.

**Suggested action**: Change to:
> "Noxu DB tools cannot read BDB-JE (`.jdb`) log files. Migration from
> BDB-JE to Noxu DB requires an export/import step at the application layer."

---

### 4.5 Stale Unsafe Table Entry for `off_heap.rs` (Carry-Over Unfixed)

| Attribute | Value |
|---|---|
| **Severity** | Low |
| **Subsystem** | docs/maintainer |
| **File:line** | `docs/src/maintainer/design-decisions.md:125` |
| **[prior]** | [prior: Margo finding 4.6] |

**What the doc says** (Decision 8 unsafe table):

```
| `crates/noxu-evictor/src/off_heap.rs` | Off-heap BIN storage |

```

**What the code does**: `noxu-evictor/src/lib.rs` carries
`#![forbid(unsafe_code)]`. `off_heap.rs` has zero `unsafe` blocks. Wave 11-Q
updated `AGENTS.md` to remove the stale entry but `design-decisions.md` was
not updated.

**Suggested action**: Remove the `off_heap.rs` row from the unsafe table.

---

### 4.6 Missing Decisions: No Nested Transactions and `spawn_blocking` (Carry-Over)

| Attribute | Value |
|---|---|
| **Severity** | Low |
| **Subsystem** | docs/maintainer |
| **File:line** | `docs/src/maintainer/design-decisions.md` (missing entries) |
| **[prior]** | [prior: Margo findings 4.4 and 4.5] |

Neither the "No Nested Transactions" decision nor the "`spawn_blocking`
requirement for async callers" note was added to `design-decisions.md` in any
subsequent wave. Both were flagged Medium in the original audit and deferred
to wave 11-S Q-7, which only fixed `database.rs` comment drift, not the
doc-level entries.

**Suggested action**: Add both entries (see prior audit findings 4.4 and 4.5
for the suggested text).

---

## Section 5 — Comment Drift in High-Churn Files

### 5.1 `rep_config.rs` and `auth.rs` Reference Stale Version Numbers

| Attribute | Value |
|---|---|
| **Severity** | Low |
| **Subsystem** | noxu-rep |
| **File:lines** | `crates/noxu-rep/src/rep_config.rs:138–148, 327–331`, `crates/noxu-rep/src/auth.rs:2–26` |

**What the comments say**: "v1.4.x", "v1.5.0+", "in v1.4.x this field is
`*unused*`", "Wildcards are NOT supported in v1.5.0."

**What the workspace version is**: `3.0.2` (see `Cargo.toml`).

The version scheme changed from v1.x to v3.x during the semantic-correctness
waves. These comments predate that change and now reference phantom version
numbers.

**Suggested action**: Replace "v1.4.x" with "pre-v3.0" and "v1.5.0+" with
"v3.0+" throughout both files.

---

### 5.2 `environment_impl.rs` and `recovery_manager.rs` Have Uncleaned Wave References

| Attribute | Value |
|---|---|
| **Severity** | Low |
| **Subsystem** | noxu-dbi, noxu-recovery |
| **File:lines** | `crates/noxu-dbi/src/environment_impl.rs:114, 374, 445, 1498, 1519, 1541` and `crates/noxu-recovery/src/recovery_manager.rs:153, 174, 405, 516, 557, 731, 963, 1148, 1260, 1394, 1565` |

**What the comments say**: "Wave 3-2: XA in-doubt transactions…", "Wave 3-2 of
the v1.5+ remediation plan.", "Wave 11-K optimisation (Fix 2).", "Wave 11-K
(Fix 3): call hint_redo_capacity…"

**Status**: Wave 11-V cleaned these from user-facing docs and several crate
`lib.rs` files but explicitly left `crates/noxu-dbi/src/` and
`crates/noxu-recovery/src/` untouched (not listed in the 11-V cleanup table).
The wave references are now process artifacts with no informational value —
they don't identify bugs, they only cite the implementing wave. They will
mislead future readers who try to verify what "Wave 3-2" changed.

**Suggested action**: Replace "Wave 3-2:" with "XA in-doubt recovery:" and
"Wave 11-K (Fix N):" with the actual optimization description (e.g. "Recovery
alloc optimisation: ...").

---

### 5.3 `RecoveryScratch` Doc Comment References "Wave 11-K"

| Attribute | Value |
|---|---|
| **Severity** | Low |
| **Subsystem** | noxu-recovery |
| **File:line** | `crates/noxu-recovery/src/recovery_manager.rs:174` |

**What the comment says**:
> "Wave 11-K optimisation (Fix 2). In the current implementation the redo
> loop passes `Bytes`-backed `&[u8]` slices directly to `Tree::redo_insert`
> without materialising intermediate owned buffers, so the scratch is
> primarily a hook for future use…"

The comment says "Wave 11-K optimisation" — wave-artifact language. More
importantly, the comment partially describes the implementation ("the redo loop
passes Bytes-backed slices directly") but then says "the scratch is primarily a
hook for future use" — meaning the struct exists for intent documentation only.
This needs to either be removed or clearly marked as a design placeholder.

**Suggested action**: Remove the wave reference; restructure as:
> "Pre-allocated scratch buffers for LN parsing. Currently the redo loop uses
> `Bytes`-backed slices without materialising owned buffers; this struct is
> a forward-compatibility hook and zero-copy intent marker."

---

### 5.4 `noxu/src/lib.rs` Quick-Start Example Passes Wrong Type to `open_database`

| Attribute | Value |
|---|---|
| **Severity** | Medium |
| **Subsystem** | noxu (umbrella) |
| **File:line** | `crates/noxu/src/lib.rs:25` |

**What the example says** (in a `//! ```ignore` block):

```rust
let db = env.open_database(None, "kv", true)?;

```

**What the signature requires**:

```rust
pub fn open_database(&self, txn: Option<&Transaction>, name: &str, config: &DatabaseConfig) -> Result<Database>

```

The example passes `true` (a `bool`) where `&DatabaseConfig` is required. The
`ignore` annotation means this won't fail `cargo test`, but it's the first code
a new user reads in the umbrella crate docs. Wave 11-S fixed the `README.md`
and `transaction.rs` examples but the new umbrella crate's quick-start was
introduced after that wave and was never validated.

**Suggested action**: Replace with:

```rust
let db = env.open_database(None, "kv", &DatabaseConfig::default().with_allow_create(true))?;

```

and change `ignore` to `no_run` so it at least compiles on `cargo test`.

---

## Section 6 — Invariant Coverage for New/Changed Structures

### 6.1 `NameLnRecord.txn_id` Recovery Invariants Not Explicitly Documented

| Attribute | Value |
|---|---|
| **Severity** | Low |
| **Subsystem** | noxu-recovery |
| **File:line** | `crates/noxu-recovery/src/log_scanner.rs` (`NameLnRecord` struct) |

The C-6 fix introduced `txn_id: Option<u64>` on `NameLnRecord`. The recovery
invariants are documented in inline comments inside `run_mapping_tree_undo_pass`
but not on the struct itself:

| WAL content | Expected outcome |
|---|---|
| `NameLN` (txn_id=None, old format) | Always kept (backward compat) |
| `NameLNTxn` + `TxnCommit` | Kept (txn_id in committed_txns) |
| `NameLNTxn` + `TxnAbort` | Removed |
| `NameLNTxn` only (crash-before-commit) | Removed |

These invariants should appear as `# Invariants` in the `NameLnRecord` struct
doc or the `AnalysisResult` struct doc, not only as buried inline comments.

**Suggested action**: Add a doc-comment `# Invariants` block to `NameLnRecord`
stating the `txn_id=None` → committed interpretation and the
`!committed_txns.contains_key(&tid)` → remove predicate.

---

### 6.2 Cache-Budget Split Invariants Not Asserted

| Attribute | Value |
|---|---|
| **Severity** | Low |
| **Subsystem** | noxu-dbi |
| **File:line** | `crates/noxu-dbi/src/environment_impl.rs` (Arbiter budget calculation, X-12) |

The X-12 fix computes:

```
arbiter_budget = max(cache_size − log_buf_total − off_heap_reserved, 1 MiB)

```

The 1 MiB floor prevents a non-positive Arbiter budget when user configuration
is pathological (e.g. `log_num_buffers × log_buffer_size > cache_size`). But
there is no `debug_assert!` or test that the computed budget is positive before
it is passed to the Arbiter constructor. A negative or zero value would silently
initialize the Arbiter with an invalid budget.

**Suggested action**: Add:

```rust
debug_assert!(arbiter_budget >= 1, "arbiter budget must be positive after floor");

```

at the calculation site, plus a test that triggers the floor condition.

---

## Summary Table

| # | Severity | Subsystem | Description | Prior? |
|---|---|---|---|---|
| 2.1 | **High** | docs/reference | `recovery.md` entirely omits C-6 mapping-tree undo pass | New |
| 2.3A | **High** | docs/maintainer | `crate-guide.md` says "19 crates" — workspace has 22 | New |
| 2.3B | **High** | docs/maintainer | `crate-guide.md` `noxu-persist` says "no derive macros" — derive macros exist | New |
| 3.1 | **Medium** | noxu-spec | `recovery_three_phase.rs` cites `transaction_table.rs`/`dirty_page_table.rs` (DNE) | Carry-over (5.4/H-6) |
| 1.2 | **Medium** | noxu-recovery | `run_mapping_tree_undo_pass` TODO comment is stale — wave 11-Y completed the work | New |
| 1.3 | **Medium** | docs/maintainer | `algorithms.md` victim selection says "youngest" only — H-4 fix adds "fewest locks" as primary | New |
| 1.4 | **Medium** | noxu-rep | `known-limitations.md` says "no authentication" — mTLS Phase 1 is in place but Phase 2 not wired | New |
| 2.2 | **Medium** | docs/maintainer | `algorithms.md` Three-Phase Recovery section omits mapping-tree undo pass | New |
| 2.5 | **Medium** | noxu-recovery | `recovery_manager.rs` comments say "3-phase" / "five sub-phases" — both inaccurate after C-6 | New |
| 3.3 | **Medium** | noxu-spec | All 11 specs have stale `VALIDATED-AS-OF: v2.4.0` stamps after waves 11-Q/U/X | New |
| 4.1 | **Medium** | docs/maintainer | No design-decision entry for umbrella crate architecture | New |
| 4.2 | **Medium** | docs/maintainer | No design-decision entry for `cache_size` = total budget (X-12) | New |
| 4.3 | **Medium** | docs/maintainer | No design-decision entry for mTLS Phase 2/3 not yet enforced | New |
| 4.4 | **Medium** | docs/maintainer | "Noxu and Noxu" confusion in Decision 3 still not fixed | Carry-over (4.3/5.8) |
| 5.4 | **Medium** | noxu (umbrella) | `lib.rs` quick-start example passes `true` instead of `&DatabaseConfig` to `open_database` | New |
| 2.4 | **Low** | AGENTS.md | Still says "19 crates" | New |
| 2.6 | **Low** | docs/operations | `sizing.md` thread pool table missing `LogFlushTask` daemon | New |
| 2.7 | **Low** | docs/operations | `known-limitations.md` contains wave-reference language ("Wave 11-R finding") | New |
| 3.2 | **Low** | noxu-spec | `vlsn_streaming.rs` spec cites `vlsn.rs` (DNE — should be `vlsn/mod.rs`) | Carry-over (1.9/5.5) |
| 4.5 | **Low** | docs/maintainer | `design-decisions.md` unsafe table still lists `off_heap.rs` | Carry-over (4.6) |
| 4.6 | **Low** | docs/maintainer | Missing "No Nested Transactions" and `spawn_blocking` design decisions | Carry-over (4.4/4.5) |
| 5.1 | **Low** | noxu-rep | `rep_config.rs`/`auth.rs` reference stale version numbers (v1.4.x, v1.5.0) | New |
| 5.2 | **Low** | noxu-dbi, noxu-recovery | Uncleaned wave-reference comments ("Wave 3-2:", "Wave 11-K") | New |
| 5.3 | **Low** | noxu-recovery | `RecoveryScratch` doc says "Wave 11-K optimisation" | New |
| 6.1 | **Low** | noxu-recovery | `NameLnRecord.txn_id` invariants not documented on struct | New |
| 6.2 | **Low** | noxu-dbi | Arbiter budget floor (X-12) not asserted with `debug_assert!` | New |
| 1.1 | **Low** | noxu-recovery | `recover()` single-DB path omits mapping-tree undo — undocumented intentional skip | New |

**Totals**: 3 High, 12 Medium, 12 Low = **27 findings**
(3 High and 5 Medium are new; 5 are carry-overs never fixed by any wave)

---

## Top 5 Doc Fixes (Most Actionable)

1. **[HIGH] Add mapping-tree undo pass to `recovery.md`** — Insert a
   "Mapping-Tree Undo Pass (v3.0.0)" section between Phase 2 and Phase 3,
   describing C-6. This is a single-file addition to the reference docs that
   makes the catalog-consistency guarantee visible to operators.

2. **[HIGH] Update `crate-guide.md`**: change "19 crates" to "22", remove "no
   derive macros" from the `noxu-persist` entry, add sections for `noxu`,
   `noxu-persist-derive`, `noxu-spec`. One file, ~20 lines of additions.

3. **[MEDIUM] Fix `noxu/src/lib.rs` umbrella quick-start** — change
   `open_database(None, "kv", true)` to use `&DatabaseConfig`, change `ignore`
   to `no_run`. Two-line fix; first code a user sees.

4. **[MEDIUM] Fix `algorithms.md` victim selection** — add "fewest locks held
   (primary); youngest on ties" to the Deadlock Detection section. Accurately
   reflects the H-4 fix without which the doc actively misleads readers about
   which transactions get aborted under load.

5. **[MEDIUM] Add three missing design decisions to `design-decisions.md`**:
   umbrella crate coupling (4.1), `cache_size` total budget (4.2), mTLS Phase
   2/3 not yet enforced (4.3). These are architectural and security decisions
   that should be explicitly documented for future maintainers and users.

---

## Top 5 Algorithm Concerns

1. **[MEDIUM] Stale TODO in `run_mapping_tree_undo_pass`** — The comment says
   the write-path txn_id plumbing hasn't been done, but wave 11-Y completed it.
   This causes a reader auditing the C-6 fix to incorrectly believe the undo
   predicate can never fire on real WAL files. The stale TODO also inflates the
   apparent incompleteness of C-6: the real remaining gap is only the MapLN
   B-tree undo (not the txn_id write-path).

2. **[MEDIUM] `recovery_three_phase.rs` spec doesn't model C-6 invariant** —
   The `AllAndOnlyCommitted` property checks data consistency but not catalog
   consistency. After C-6, an aborted database creation that survives recovery
   (due to a bug in the undo predicate) would pass the spec's model check but
   violate the system's correctness guarantee. The spec needs a
   `CatalogConsistency` property.

3. **[MEDIUM] All 11 Stateright specs are stamped `VALIDATED-AS-OF: v2.4.0`**
   after waves 11-Q/U/X changed four protocols materially (victim selection,
   VLSN persistence cap, secondary cleaner dispatch, recovery). The specs are
   still correct as models but the validation timestamp is a trust anchor — a
   stale anchor undermines confidence that the specs track production.

4. **[MEDIUM] mTLS Phase 2 not landed, not documented as a decision** — The
   `PeerAllowlistVerifier` is implemented but `to_rustls_server_config()` still
   calls `.with_no_client_auth()`. A user who reads the `RepConfig::peer_allowlist`
   doc and sets up an allowlist will find their allowlist is never checked.
   This is a security-correctness gap with no user-visible warning except the
   `auth.rs:19` inline comment.

5. **[LOW] `recover()` single-DB path silently lacks the C-6 catalog undo** —
   `recover_all()` calls `run_mapping_tree_undo_pass()` but `recover()` does
   not. For the current use of `recover()` (single-DB fresh environments with
   no NameLN entries) this is safe. But if `recover()` is ever used in a
   context that can have NameLNTxn entries, the catalog will not be corrected.
   The asymmetry is undocumented and could cause subtle correctness bugs in
   future refactors that repurpose `recover()`.

---

## Notes on What Was Fixed Correctly

The wave series (11-Q through 11-Y plus umbrella) closed all the critical and
most high-severity findings from the prior audit cleanly:

- C-1 through C-9 are all verifiably fixed in the code.
- H-4 (lock counts for victim selection) is properly wired in `lock_manager.rs`.
- H-5 (waiter graph direction in `algorithms.md`) is correctly fixed.
- H-6/H-7 (on-disk format hex codes and endianness) are correctly fixed in
  `on-disk-format.md`.
- H-9 (`PartialEvict` not stripping data) is fixed via `BinStub::strip_lns`.
- X-12 (cache_size total budget) is implemented and documented in
  `configuration.md` and `sizing.md`.
- X-11 (LogFlushTask daemon) is implemented and documented in
  `configuration.md`.
- C-6 (mapping-tree undo pass) is functionally complete in `recover_all()`;
  the remaining stale TODO comment is the only artifact.

The project is in substantially better shape than at the prior audit. The
remaining findings are largely documentation/comment housekeeping, not
algorithmic correctness issues.

---

*Report written by Margo Seltzer (re-audit), 2026-05-30*
*Report path: `/tmp/noxu-reaudit-margo.md`*
