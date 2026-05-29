# Wave 11-V — Voice Cleanup

**Branch**: `cleanup/wave11-v-voice`  
**Status**: Complete

## Overview

This wave audits and rewrites user-facing documentation and public-crate
rustdocs to remove agent-process artifacts, inappropriate provenance
references, and boastful language. The target voice is the direct,
declarative style of the Berkeley DB C reference documentation.

## Audit Findings

### Category A — Internal process artifacts in user-facing files

| File | Line(s) | Issue |
|---|---|---|
| `docs/src/getting-started/cursors.md` | 11, 47, 51 | "Sprint 1C", "Sprint 1 / F12" |
| `docs/src/getting-started/disk-ordered-cursors.md` | 10 | "Wave 2C-3" |
| `docs/src/getting-started/migrating.md` | 13–420 | Entire doc structured by "Wave 2B", "Sprint 1", "Sprint 3D", "Sprint 3A", "Sprint 3B", "Sprint 3C", "Wave 3-1" section titles |
| `docs/src/transactions/basics.md` | 11, 218, 431 | "Wave 3-1", "Sprint 1C / F12" |
| `docs/src/transactions/secondary-with-txn.md` | 23–24 | "Sprint 1C", "Wave 1B" |
| `docs/src/transactions/xa-distributed.md` | 8 | "wave 3-2 of the v1.5+ remediation plan" |
| `docs/src/replication/README.md` | 6–9 | "Wave 3-3 closed F1 … Wave 4-A closed …" |
| `docs/src/replication/durability.md` | 4 | "Wave 3-3, F1" |
| `docs/src/replication/dynamic-membership.md` | 5 | "Wave 4-A, F9" |
| `docs/src/replication/elections.md` | 4–6 | "Wave 3-3, F6", "Wave 4-A, F5/F31" |
| `docs/src/replication/in-memory-transport.md` | 3–4, 110, 123, 148 | "Wave 11-D", "Wave 8" |
| `docs/src/replication/transport.md` | 4–8, 19, 109 | "Wave 3-3, F3", "Wave 4-A, F2/F4", "Wave 11-D" |
| `docs/src/collections/README.md` | 24 | "Wave 2B (v1.6) closes the v1.5 collections audit" |
| `docs/src/collections/entity-persistence.md` | 8, 11, 13, 234, 372 | "Sprint 3B", "Wave 2C-1", "Wave 2C-2" |
| `docs/src/collections/stored-list.md` | 60 | "Compaction (Wave 2B / v1.6)" section title |
| `docs/src/operations/benchmarks.md` | 189–268 | "Wave 11-B", "Wave 10-D", "Wave 11-C" section titles and body |
| `docs/src/introduction.md` | 17–22 | "Wave 4-A", "Wave 2A–4-A series", "Wave 7–9 follow-ups" in capability matrix preamble |
| `README.md` | 16–22 | "Status:" paragraph with audit/wave language; "sprint/v2.2.0-base" branch name |
| `crates/noxu-rep/tests/inmem_transport_test.rs` | 1 | "Wave 11-D integration tests for the production-grade" |

### Category B — Inappropriate JE/BDB-Java references in user docs

| File | Line(s) | Issue |
|---|---|---|
| `docs/src/getting-started/disk-ordered-cursors.md` | 10–11 | "matches the shape of BDB JE's `DiskOrderedCursor`" |
| `docs/src/collections/README.md` | 13 | "Corresponds to BDB-JE's `com.sleepycat.collections`" |
| `docs/src/collections/entity-persistence.md` | 372 | "matches the JE `EntityStore.evolve(EvolveConfig)` shape" |
| `crates/noxu-collections/src/lib.rs` | 25 | "This is the BDB-JE shape" |
| `crates/noxu-persist-derive/src/lib.rs` | 19, 35 | "analogue of `@KeyField` from BDB-JE", "BDB-JE has three annotations" — **kept**; useful mapping table for maintainers, not user-facing tutorial |

### Category C — Boastful / self-promoting language

| File | Line(s) | Issue |
|---|---|---|
| `docs/src/contributing/porting-guidelines.md` | 12 | "battle-tested in production systems for over 20 years" (FALSE — Noxu is new) |
| `docs/src/maintainer/project-history.md` | 8–10 | "battle-tested, with 20+ years of production use", "production-grade" |
| `docs/src/reference/architecture.md` | 13 | "mature, production-grade embedded database" |
| `docs/src/maintainer/README.md` | 32 | "production-grade, dependency-light" |
| `docs/src/operations/benchmarks.md` | 57, 61–65 | "Noxu **2.7× faster**", "Noxu **1.6× faster**", etc. — bold-emphasis editorializing |

### Category F — Wave-organized user docs

| File | Issue |
|---|---|
| `docs/src/getting-started/migrating.md` | Entire structure is "Wave 2B", "Sprint 1", "Sprint 3D" — user sees versions, not sprints |
| `docs/src/introduction.md` | Capability matrix preamble cross-references internal wave docs |

### Category G — TODO / regression markers in production source

| File | Line(s) | Issue |
|---|---|---|
| `crates/noxu-db/tests/je_db_cursor_test.rs` | 448, 492 | `TODO(noxu-bug, wave-11-A):` |
| `crates/noxu-db/tests/je_database_test.rs` | 601, 637, 804, 921 | `TODO(noxu-db bug, wave-11-G):` |
| `crates/noxu-db/tests/je_truncate_test.rs` | 124 | `TODO(noxu-engine bug, wave-11-G):` |
| `crates/noxu-recovery/tests/prop_tests.rs` | 376 | `TODO(wave 11-E followup):` |
| `crates/noxu-bind/tests/prop_tests.rs` | 172 | `// Wave 11-E: SortKey reverse properties.` |
| `crates/noxu-bind/tests/tck_serial_binding.rs` | 19 | `Sprint 3C, see …` |

### Crate-level lib.rs and module docs

| File | Line(s) | Category | Issue |
|---|---|---|---|
| `crates/noxu-collections/src/lib.rs` | 13, 25, 74 | A+B | "v1.6 API shape (Wave 2B)", "BDB-JE shape", "Wave 2B redesign" |
| `crates/noxu-persist/src/lib.rs` | 10 | A | "v1.6 (Wave 2C-1)" |
| `crates/noxu-rep/src/lib.rs` | 113 | A | "Wave 11-D" comment |
| `crates/noxu-spec/src/lib.rs` | 20–24 | A | "Wave 9-B", "Wave 11-F" |
| `crates/noxu-collections/src/internal.rs` | 3 | A | "Wave 2B redesign (v1.6)" |
| `crates/noxu-collections/src/stored_iterator.rs` | 3 | A | "Wave 2B redesign (v1.6)" |
| `crates/noxu-collections/src/stored_list.rs` | 3 | A | "Wave 2B redesign (v1.6)" |
| `crates/noxu-collections/src/stored_value_set.rs` | 3 | A | "Wave 2B redesign (v1.6)" |
| `crates/noxu-collections/src/stored_key_set.rs` | 3 | A | "Wave 2B redesign (v1.6)" |
| `crates/noxu-collections/src/stored_map.rs` | 3 | A | "Wave 2B redesign (v1.6)" |
| `crates/noxu-collections/src/stored_sorted_map.rs` | 3 | A | "Wave 2B redesign (v1.6)" |
| `crates/noxu-collections/src/transaction_runner.rs` | 4 | A | "Wave 2B redesign (v1.6)" |
| `crates/noxu-persist/src/entity_store.rs` | 3 | A | "Wave 2C-2 wires schema evolution" |

## Rewrites

### README.md

Removed the "Status:" paragraph (internal-process audit-closure language).
Removed "sprint/v2.2.0-base" branch name from version line.
Retained all technical content.

### docs/src/introduction.md

Dropped wave-report cross-references from the capability matrix preamble.
Kept version labels (v1.5, v1.6, v2.0, v2.2) because those are
user-visible release identifiers. Replaced "Wave N-X" cell annotations
with plain version markers or removed them where redundant with the
column header. Dropped the preamble paragraph that pointed to internal
wave docs as the source of the matrix.

### docs/src/getting-started/migrating.md

Reorganized from sprint/wave section titles to topic/version titles:

- "Wave 2B — Collections typed API and txn threading (v1.5 → v1.6)" →
  "Collections API (v1.5 → v1.6)"
- "Behaviour changes (Sprint 1 — txn wiring)" →
  "Transaction wiring (v1.4.x → v1.5)"
- "Behaviour changes (Sprint 1 — cursor `Get` variants)" →
  "Cursor `Get` variants (v1.4.x → v1.5)"
- "Behaviour changes (Sprint 3D — v1.5 architectural decisions)" →
  "Architectural decisions (v1.5)"
- "Behaviour changes (Sprint 3A — XA in-process only)" →
  "XA in-process only (v1.5)"
- "Source-level breaking changes (Sprint 3B — DPL `txn` threading)" →
  "DPL transaction threading (v1.5)"
- "On-disk breaking changes (Sprint 3C — collections & bind)" →
  "Collections and bind (v1.5)"
- "On-disk breaking changes (Wave 2C-2 — DPL entity record envelope)" →
  "DPL entity record envelope (v1.6)"

Dropped internal audit-finding citations. Kept user-visible behavior
change descriptions. Inline wave-code annotations replaced with version
labels: "v1.6 (Wave 2A) update" → "v1.6 update", "Wave 3-1" → "v2.0".

### docs/src/getting-started/cursors.md

Removed "Sprint 1C" from the advisory callout (3 references).
Kept the factual version label "v1.5".

### docs/src/getting-started/disk-ordered-cursors.md

Replaced "Wave 2C-3" with "v1.6".
Replaced "BDB JE's `DiskOrderedCursor` plus a small Rust-idiomatic
extension (`dedup_keys`)" with a description of what the feature does.

### docs/src/transactions/basics.md

Removed "Wave 3-1" and "Sprint 1C / F12" references from the opening
callout and from inline prose. Kept "v2.0" version labels.

### docs/src/transactions/secondary-with-txn.md

Removed "Sprint 1C threaded the txn through; Wave 1B extended the same
plumbing" from the method description. Described the current behavior
factually.

### docs/src/transactions/xa-distributed.md

Replaced "wave 3-2 of the v1.5+ remediation plan" with "v2.0".

### docs/src/replication/README.md

Replaced the GA-status callout's wave/finding list with a factual
description of what is supported as of v2.0.

### docs/src/replication/elections.md

Removed "(Wave 3-3, F6)" and "(Wave 4-A, F5/F31)" from the GA callout.

### docs/src/replication/durability.md

Removed "(Wave 3-3, F1)" from the GA callout.

### docs/src/replication/dynamic-membership.md

Removed "(Wave 4-A, F9)" from the GA callout.

### docs/src/replication/transport.md

Removed "Wave 3-3, F3", "Wave 4-A, F2/F4", "Wave 11-D" references.
Kept "v2.0", "v2.4" version labels.

### docs/src/replication/in-memory-transport.md

Removed "Wave 11-D", "Wave 8" references from the title callout,
"Public API surface" section title, and "Tests" section prose.
Described the feature factually.

### docs/src/collections/README.md

Replaced "Wave 2B (v1.6) closes the v1.5 collections audit by:" with
"The v1.6 collections API provides:".
Removed BDB-JE `com.sleepycat.collections` reference from user-facing
description.

### docs/src/collections/entity-persistence.md

Removed "Sprint 3B", "Wave 2C-1", "Wave 2C-2" from the opening callout
and from section titles. Kept "v1.5", "v1.6" version labels.
Replaced "(matches the JE `EntityStore.evolve(EvolveConfig)` shape)"
with a factual description.

### docs/src/collections/stored-list.md

Changed section title "Compaction (Wave 2B / v1.6)" to "Compaction".

### docs/src/operations/benchmarks.md

Replaced bold-emphasis editorials in the Notes column:

- "Noxu **2.7× faster** — fewer per-commit fsyncs" →
  "Noxu favors fewer per-commit fsyncs"
- "Noxu **1.6× faster**" → "Noxu range scan stays inside same BIN"
- "Noxu **2.5× faster**" → "Noxu favors fewer per-commit fsyncs"
- "Noxu **1.3× faster** — `WritePromote` upgrade path" →
  "Noxu `WritePromote` upgrade path avoids lock re-acquisition"

Replaced "Wave 11-B", "Wave 10-D", "Wave 11-C" section titles and body
references with factual descriptions.

### docs/src/reference/architecture.md

Replaced "Noxu DB is a mature, production-grade embedded database"
with "Noxu DB is an embedded transactional key-value database engine
written in Rust, derived from the design of Berkeley DB Java Edition."

### docs/src/contributing/porting-guidelines.md

Replaced "Noxu's implementation has been battle-tested in production
systems for over 20 years" with an accurate statement: the reference
implementations in `_/je/` and `_/nosql/` carry decades of production
history; Noxu's approach is to inherit that algorithmic maturity
through faithful porting and test-suite porting.

### docs/src/maintainer/project-history.md

Replaced "Noxu DB is battle-tested, with 20+ years of production use"
with an accurate description of provenance.
Replaced "production-grade embedded database" with "embedded database".

### docs/src/maintainer/README.md

Replaced "production-grade, dependency-light" with "dependency-light".

### Crate-level lib.rs and module docs

- `crates/noxu-collections/src/lib.rs`: removed "v1.6 API shape (Wave
  2B)" heading, replaced "BDB-JE shape" with "same convention as
  `noxu_db::Database`", removed "Wave 2B redesign" migration note.
- `crates/noxu-collections/src/{internal,stored_iterator,stored_list,
  stored_value_set,stored_key_set,stored_map,stored_sorted_map,
  transaction_runner}.rs`: replaced "Wave 2B redesign (v1.6)." module
  preamble with a one-sentence factual description.
- `crates/noxu-persist/src/lib.rs`: removed "v1.6 (Wave 2C-1)".
- `crates/noxu-persist/src/entity_store.rs`: removed "Wave 2C-2".
- `crates/noxu-rep/src/lib.rs`: removed "Wave 11-D" comment.
- `crates/noxu-spec/src/lib.rs`: removed "Wave 9-B", "Wave 11-F"
  process language; kept version labels.

### Test file TODO normalization (Category G)

- `crates/noxu-db/tests/je_db_cursor_test.rs`: normalized
  `TODO(noxu-bug, wave-11-A):` → `// TODO(bug):`.
- `crates/noxu-db/tests/je_database_test.rs`: normalized four
  `TODO(noxu-db bug, wave-11-G):` → `// TODO(bug):`.
- `crates/noxu-db/tests/je_truncate_test.rs`: normalized
  `TODO(noxu-engine bug, wave-11-G):` → `// TODO(bug):`.
- `crates/noxu-recovery/tests/prop_tests.rs`: normalized
  `TODO(wave 11-E followup):` → `// TODO:`.
- `crates/noxu-bind/tests/prop_tests.rs`: replaced `// Wave 11-E:`
  with a factual comment.
- `crates/noxu-bind/tests/tck_serial_binding.rs`: removed "Sprint 3C"
  reference from test preamble.
- `crates/noxu-rep/tests/inmem_transport_test.rs`: removed "Wave 11-D"
  and "production-grade" from test preamble.

## Intentionally Untouched

- `docs/src/internal/` — all wave documents are appropriate in this
  directory. The only new file written here is this document.
- `docs/src/maintainer/algorithms.md` — JE references document algorithm
  provenance; appropriate for the maintainer audience.
- `docs/src/maintainer/design-decisions.md` — JE references are design
  rationale; appropriate.
- `docs/src/contributing/porting-guidelines.md` — JE/BDB-C references
  in the naming-conventions table are appropriate (this is the porting
  guide).
- `crates/noxu-persist-derive/src/lib.rs` — the JE annotation mapping
  table (`@Entity` → `#[derive(Entity)]`, etc.) is useful design
  rationale for the derive-macro author; kept.
- Test files whose `//! Wave N-X` header identifies the JE TCK port
  batch (e.g. `//! Wave 6 — Priority-3 JE TCK port.`) — these are
  internal test-registry notes, not user-facing docs. Left in place to
  preserve traceability.
- `AGENTS.md` — already task-oriented and direct. No wave/sprint
  language found outside the description of development phases, which
  is appropriate context for contributors.
