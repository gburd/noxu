# TLA+ specifications for noxu

This directory contains TLA+ specs for safety-critical state machines in
the noxu codebase. Specs are intentionally smaller than the Rust they
model — TLA+ is most useful for proving the *protocol* is correct, not
for catching every implementation bug. The Rust tests still need to
verify that the implementation faithfully realises the protocol.

## Specs

| Spec | Models | Status |
|---|---|---|
| `BTreeLatching.tla` / `.cfg` | The B+tree concurrent insert/split protocol after the Stream F latch-coupling fixes. Two threads + one root + two BINs. Invariants: `LockInvariant`, `AtMostOneSplit`, `NoLostWrites`. | TLC clean (~300 distinct states, <1 s) |
| `BTreeLatchingBuggy.tla` / `.cfg` | The pre-fix variant that drops the parent's read lock before taking the BIN's write lock. **Intentionally** fails `NoLostWrites`; the counterexample trace is exactly the descender-vs-splitter race that Stream F closed. | TLC produces counterexample in ~8 k states, <1 s |
| `FlexiblePaxos.tla` / `.cfg` | The election protocol in `crates/noxu-rep/src/elections/paxos.rs`. Three nodes, two terms, Q1=Q2=2. Invariants: `QuorumIntersection`, `ElectionSafety` (one leader per term), `PromiseHonoured`. | TLC clean (~1.4 M distinct states, ~13 s) |

The "buggy" companion is the regression artefact for the fix: if a future
change to either `BTreeLatching.tla` or the Rust code causes the buggy
spec to *stop* failing, that's a signal that the lock discipline has
silently regressed (the bug is no longer reachable from the modelled
state space, but the underlying race may have re-opened).

## Running the specs locally

The TLA+ Toolbox bundles `tla2tools.jar`. On macOS the path is

```
/Applications/TLA+ Toolbox.app/Contents/Eclipse/tla2tools.jar
```

Set `TLA_JAR` to point at it, then:

```bash
make tla         # runs every spec, fails on any TLC violation in the
                 # non-buggy specs and on any *non-violation* in the
                 # buggy ones (i.e. the buggy spec must still find the bug)
```

The `Makefile` target invokes `tla/run.sh`, which is a thin wrapper around
`java -cp "$TLA_JAR" tlc2.TLC ...` so it works the same in CI.

## What is *not* modelled here

Worth doing as follow-ups:

- WAL commit + group commit — the durability protocol that
  `noxu-log::LogManager` and `noxu-txn::Txn::commit_with_durability`
  implement together.
- Recovery 3-phase analysis → redo → undo.
- Cleaner safety: file deletion vs in-flight references.
- Lock manager + deadlock detection: cycle-detection termination and
  no-false-positive abort.

Each of these is a tractable spec target; the time cost is the
domain-specific modelling, not running TLC. See the `Makefile` for the
slot they would each plug into.

## Sync between Rust and TLA+

Each spec carries a header comment listing the Rust source files it
models. On every CR that touches one of those Rust files, reviewers
must:

1. Re-check that the spec still matches the implementation.
2. If the spec is now stale, either update the spec in the same CR or
   land a follow-up that does so before the next release.

`scripts/check_tla_in_sync.sh` is a CI helper that walks every spec,
reads the Rust paths from its header, and `git -P diff`-s the touched
files in the current PR. If a tracked file changed without the spec
being touched, the script prints a warning. It does *not* fail the
build by default — TLA+ stay-in-sync is an advisory check today —
but the warning surfaces in CI logs.
