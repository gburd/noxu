# For Future Maintainers

If you are reading this as the new maintainer of Noxu DB, this chapter is
written for you. It provides the full context that cannot be inferred from the
code alone: why decisions were made, how the codebase evolved, which algorithms
are implemented and where, and how to develop, test, and benchmark the system.

The goal is that a knowledgeable Rust developer who has never seen Noxu DB
before can read this chapter and become productive — able to add features, debug
subtle issues, and reason about correctness — within a day.

## In This Chapter

1. [Project History](project-history.md) — why the project exists, major milestones, session-by-session evolution
2. [Crate Guide](crate-guide.md) — all 16 crates: purpose, key types, JE correspondence, critical files
3. [Algorithms](algorithms.md) — every named algorithm with source file locations and academic references
4. [Design Decisions](design-decisions.md) — the "why" behind non-obvious implementation choices
5. [JE Source Navigation](je-source-guide.md) — navigating `_/je/` and `_/nosql/`, Java-to-Rust naming rules
6. [Development Workflow](development.md) — setup, build, debug, profile
7. [Testing](testing.md) — unit tests, nextest, proptest, fuzz targets
8. [Chaos and Soak Testing](chaos-soak-testing.md) — tc netem, torture test, soak scripts
9. [Benchmarking](benchmarking.md) — benchmark suites, JE comparison baseline, interpreting results

## Quick Orientation

**What is Noxu DB?** A Rust port of Oracle's Berkeley DB Java Edition 7.5.11,
including all 10 Oracle NoSQL JE enhancements. It is an embedded, serverless,
transactional key-value store — the same architecture as SQLite, but with
BDB-style B-tree storage, record-level locking, and optional multi-node
replication.

**Why does it exist?** To provide a production-grade, dependency-light,
idiomatically Rust embedded database with the same API contract, algorithm
fidelity, and operational characteristics as BDB JE — without a JVM.

**Where does the code live?**

```
/home/gburd/ws/lamdb/       ← this repository
/home/gburd/ws/lamdb/_/je/  ← BDB JE 7.5.11 Java source (read-only reference)
/home/gburd/ws/lamdb/_/nosql/ ← Oracle NoSQL JE fork (read-only reference)
```

**Current fidelity** (as of the last audit):
- Named-algorithm fidelity: ~92%
- Operational completeness: ~85%
- Production hardening: ~100% (all EnvironmentConfig parameters, ExceptionListener, is_valid())
