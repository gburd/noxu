# Wave 3-1 — nested-transaction `parent` parameter removed

> **Branch:** `fix/wave3-1-nested-txn-removal`
> **Base:** `sprint/v1.6.0-rc1-base`
> **Inputs:**
> [`v1.5-decisions-2026-05.md`](v1.5-decisions-2026-05.md) (Decision 3B),
> [`sprint-3-decisions-enforced.md`](sprint-3-decisions-enforced.md)
> (Decision 3B v1.5 path).
> **Output:** breaking signature change behind a `feat(db)!` commit;
> approved as a v2.0 SemVer break by the project owner.

## Purpose

Decision 3B in `v1.5-decisions-2026-05.md` was staged in two phases:

1. **v1.5 (Sprint 3D):** keep the `parent: Option<&Transaction>`
   parameter on `Environment::begin_transaction`, but reject
   `Some(_)` at runtime with `NoxuError::Unsupported`.  This avoided
   a SemVer break while making the audit-F11 misuse loud instead of
   silent.
2. **v2.0 (Wave 3-1, this note):** remove the parameter from the
   signature entirely so the misuse is a compile error.

The v1.5 path landed in Sprint 3D; this note covers the v2.0 path.

## Signature change

```rust
// v1.5 / v1.6
pub fn begin_transaction(
    &self,
    parent: Option<&Transaction>,
    config: Option<&TransactionConfig>,
) -> Result<Transaction>;

// v2.0 (Wave 3-1)
pub fn begin_transaction(
    &self,
    config: Option<&TransactionConfig>,
) -> Result<Transaction>;
```

The Sprint 3D `parent.is_some()` rejection block in
`crates/noxu-db/src/environment.rs` is gone — it would be unreachable
now that the parameter does not exist.  The `NoxuError::Unsupported`
variant itself is **kept**: cursor `Get::*Dup` arms (audit F3) and
several secondary-config / DPL paths still produce it.

## Mechanical migration

```rust
// before
let txn  = env.begin_transaction(None, None)?;
let txn2 = env.begin_transaction(None, Some(&cfg))?;
// the v1.5-rejected misuse
let bad  = env.begin_transaction(Some(&parent), None)?;

// after
let txn  = env.begin_transaction(None)?;
let txn2 = env.begin_transaction(Some(&cfg))?;
// no v2.0 equivalent for nested txns — they remain unsupported, and
// the type system now enforces it.  Nested-transaction API design
// is tracked in Decision 3B for a future major release.
```

The blast radius is small: Sprint 3D already rejected the
`Some(parent)` form at runtime, so there is no production code in
this repository (and, by Decision 3B's audit, none expected
downstream) that relied on the no-op behaviour.

## Files touched

```text
crates/noxu-db/src/environment.rs                       (signature + rustdoc + remove rejection block)
crates/noxu-db/src/transaction.rs                       (doctest)
crates/noxu-db/src/bin/crash_worker.rs                  (call sites)
crates/noxu-db/benches/api_bench.rs                     (call site)
crates/noxu-db/tests/*.rs                               (call sites)
crates/noxu-collections/src/*.rs                        (call sites + doctests)
crates/noxu-collections/tests/wave2b_tests.rs           (call sites)
crates/noxu-persist/src/entity_store.rs                 (call sites)
crates/noxu-persist/tests/*.rs                          (call sites)
crates/noxu-xa/src/environment.rs                       (internal call site)
crates/noxu-xa/tests/xa_chaos_test.rs                   (call sites)
benches/noxu-bench/src/{concurrent,workloads}.rs        (call sites)
examples/*.rs                                           (call sites)
tests/fuzz/fuzz_targets/*.rs                            (call sites)
docs/src/transactions/*.md                              (prose + code blocks)
docs/src/collections/*.md                               (code blocks)
docs/src/internal/sprint-3-dpl-restriction.md           (code block)
docs/src/getting-started/migrating.md                   (v1.5 → v2.0 section)
docs/src/introduction.md                                (capability matrix wording)
docs/src/transactions/basics.md                         (v1.5 limitation note rewritten)
docs/src/internal/sprint-3-decisions-enforced.md        (Wave 3-1 postscript)
docs/src/internal/wave-3-1-nested-txn-removal.md (NEW)  (this file)
README.md                                               (one stray code block)
```

`crates/noxu-dbi/src/environment_impl.rs` is unchanged: the v1.5
implementation never threaded `parent` past the public `Environment`
entry point, so the inner `EnvironmentImpl::begin_txn` continues to
take no parent argument.

## Test changes

* **Deleted:**
  `crates/noxu-db/tests/txn_wiring_test.rs::f11_nested_transaction_returns_unsupported`.
  The misuse it asserted is no longer representable: the call
  `env.begin_transaction(Some(&parent), None)` is a compile error in
  v2.0, so the runtime-error assertion has nothing to test.
* **Retained:**
  `crates/noxu-db/tests/txn_wiring_test.rs::f11_nested_transaction_none_still_works`
  is kept (and renamed in spirit) as a smoke test that the v2.0
  signature still accepts `None` and `Some(&cfg)` cleanly.

## Audit findings closed

* **F11 (`txn-env`):** the `_parent` parameter that originally
  dropped its argument on the floor.  Sprint 3D closed the v1.5 path
  by surfacing the misuse as a typed error; Wave 3-1 closes the v2.0
  path by making the same misuse a compile error.

## Why this is a v2.0 SemVer break

Removing a positional public-API parameter is a hard source-level
break for any caller that passed `None` (or `Some(_)`) for `parent`.
The user has explicitly authorised this break as part of the v2.0
batch.  Conventional commits flag it with `feat(db)!:` and the
`migrating.md` "v1.5 → v2.0" section gives downstream callers the
exact mechanical rewrite.
