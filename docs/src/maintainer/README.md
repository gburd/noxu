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
2. [Crate Guide](crate-guide.md) — all 19 crates: purpose, key types, crate purpose, critical files
3. [Algorithms](algorithms.md) — every named algorithm with source file locations and academic references
4. [Design Decisions](design-decisions.md) — the "why" behind non-obvious implementation choices
5. [Codebase Navigation](reference-source-guide.md) — navigating reference archives and Noxu naming conventions
6. [Development Workflow](development.md) — setup, build, debug, profile
7. [Testing](testing.md) — unit tests, nextest, proptest, fuzz targets
8. [Chaos and Soak Testing](chaos-soak-testing.md) — tc netem, torture test, soak scripts
9. [Benchmarking](benchmarking.md) — benchmark suites, performance baseline, interpreting results

## Quick Orientation

**What is Noxu DB?** An embedded transactional key-value database engine written in Rust.
with 10 extended-fork capabilities built in. It is an embedded, serverless,
transactional key-value store — the same architecture as SQLite, but with
Noxu DB-style B-tree storage, record-level locking, and optional multi-node
replication.

**Why does it exist?** To provide a production-grade, dependency-light,
idiomatically Rust embedded database with the same API contract, algorithm
fidelity, and operational characteristics as Noxu DB — without a JVM.

**Where does the code live?**

```
<repo-root>/                 ← this repository (your local clone)
<repo-root>/_/je/            ← core reference archive (read-only, optional)
<repo-root>/_/nosql/         ← extended fork reference (read-only, optional)
```

The `_/` directory is gitignored. The reference archives are not committed
to the repository; they are guidance for porting work, not a build
prerequisite.

**Current fidelity** (as of the last audit):

- Named-algorithm fidelity: ~92%
- Operational completeness: ~85%
- Production hardening: ~100% (all EnvironmentConfig parameters, ExceptionListener, is_valid())
