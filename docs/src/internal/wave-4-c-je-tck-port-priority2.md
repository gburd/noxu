# Wave 4-C — JE TCK Port, Priority-2 Packages

Status: in progress.

This wave ports JE `@Test` invariants from priority-2 packages onto Noxu DB:

- `bind/tuple` — TupleInput / TupleOutput round-trips, sort-key encoding.
- `bind/serial` — SerialBinding + class-catalog (now relevant after Sprint 3C's
  2-byte version header).
- `collections` — StoredMap / StoredSet / StoredList semantics (now relevant
  after Wave 2B's typed API).
- `persist` — DPL evolve / devolve / convert-and-add (now relevant after Wave
  2C-2 schema evolution).
- `je.config` — EnvironmentConfig parsing, mutation, defaults.

Target: 25-50 ports; commit per logical batch; per-package TSV updates.

This file is a placeholder so the branch exists; final narrative is added at
the end of the wave.
