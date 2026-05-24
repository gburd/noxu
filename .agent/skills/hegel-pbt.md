---
name: hegel-pbt
description: >
  Write property-based tests for noxu using the Hegel testing protocol
  (https://hegel.dev). Use this skill whenever you want to add coverage,
  evolve an existing example-based test, or test a function with
  invariants/contracts/round-trips that hold across many inputs.
  Hegel is built on Hypothesis-style shrinking, so failing tests
  produce minimal counterexamples automatically.
---

# Hegel property-based testing for noxu

`hegel-rust` is a property-based testing library with imperative
generators — your test takes a `TestCase` handle and calls
`tc.draw(...)` whenever it needs a value. Failures shrink to minimal
counterexamples without the test author writing any shrink logic.

This skill gives you the noxu-specific patterns. For the underlying
methodology, see the upstream [hegel-skill][1] (workflow, property
catalogue, generator discipline, common mistakes). The rest of this
file describes **how to wire Hegel into noxu** and **which noxu
properties are worth testing**.

[1]: https://github.com/hegeldev/hegel-skill

## Setup

The `hegeltest` crate (the published name of `hegel-rust`, see
<https://hegel.dev>) is a workspace dev-dependency. Pull it into a
crate that needs property tests:

```toml
[dev-dependencies]
hegeltest = { workspace = true }
hegeltest-macros = { workspace = true }
```

Tests live alongside existing tests in `tests/` or `src/`:

```rust
use hegeltest::generators as gs;
use hegeltest::TestCase;

#[hegeltest_macros::test]
fn put_get_roundtrip(tc: TestCase) {
    let env = open_test_env();
    let key  = tc.draw(gs::vec_of(gs::any::<u8>(), 0..=64));
    let val  = tc.draw(gs::vec_of(gs::any::<u8>(), 0..=4096));
    env.put(&key, &val).unwrap();
    let got = env.get(&key).unwrap();
    assert_eq!(got.as_deref(), Some(val.as_slice()));
}
```

`#[hegeltest_macros::test]` integrates with `cargo test` — no
separate runner.

## Where to put noxu Hegel tests

| Crate | What to test |
|---|---|
| `noxu-bind` | tuple/entry serialisation round-trips, sort-order preservation, Unicode edge cases |
| `noxu-tree` | `insert/get/delete` round-trips against a `BTreeMap` model; `search()` returns `None` for absent keys; key-prefix invariants survive splits |
| `noxu-db` | `Environment::open → put → close → reopen → get` round-trip; `Database::truncate` clears all keys but preserves the database; `commit/abort` semantics |
| `noxu-persist` | `Entity → bytes → Entity` round-trip via `EntitySerializer`; primary index agrees with `BTreeMap`; secondary index reflects every primary write |
| `noxu-collections` | `StoredMap`/`StoredSet`/`StoredList` agree with `std::collections` after arbitrary operation sequences |
| `noxu-rep::vlsn` | `VlsnIndex` `put_vlsn(v) → get_lsn(v)` round-trip; range scans return entries in VLSN order |
| `noxu-xa` | `Xid` round-trips through bytes; state-machine reachability matches the X/Open spec |

## Property catalogue (noxu-flavoured)

The upstream skill's catalogue is the right reference. The patterns
that have historically caught noxu bugs the most:

- **Model tests against `BTreeMap`/`HashMap`** — for any `noxu-tree`,
  `noxu-collections`, or `noxu-persist` operation, the strongest
  property is that a sequence of writes leaves the noxu data
  structure equal to the same sequence applied to a known-good
  in-memory map.

- **Concurrent-stress + invariant** — pair Hegel with a thread pool
  to drive concurrent operations. The invariant after every operation
  is the same model-equivalence test. This is how the lost-write
  races in `Tree::insert` were caught (see
  `crates/noxu-db/tests/concurrent_commits_stress.rs`).

- **Round-trip through serialisation** — every `noxu-bind` binding
  and every `EntitySerializer` should satisfy
  `deserialize(serialize(x)) == x` over the full input domain. Past
  bugs hid in: u64 values past 2^53 (precision loss when casting
  through f64), strings containing embedded NULs, sort-key encodings
  for negative integers.

- **Commit/abort semantics** — `db.put(Some(&txn), k, v); txn.abort()
  ⇒ db.get(None, k) is NotFound`. `db.put(Some(&txn), k, v);
  txn.commit() ⇒ db.get(None, k) sees v`.

- **Cursor monotonicity** — under any sequence of inserts/deletes, a
  forward cursor over the same database visits keys in non-decreasing
  order; a reverse cursor in non-increasing order.

## Generator discipline (noxu-specific)

Three places where it's tempting to over-constrain:

1. **Key length.** Don't cap keys at 32 bytes "just in case" — noxu
   handles keys up to ~4 KiB and the BIN-prefix code has bugs hiding
   at the prefix-recompute boundary. Generate `0..=4096` byte keys
   and let Hegel shrink.

2. **Concurrent thread count.** A thread count of `2..=8` is fine for
   most properties; keep the upper bound large enough that BIN
   splits are reachable in a single test.

3. **Transaction operation count.** Don't cap at 10 ops per txn —
   real txn bugs only fire at 50+ operations because that's when the
   inner Txn's `write_locks` map exceeds its initial capacity.

## When NOT to use Hegel for a noxu test

- **Replication chaos / network failures** — those tests
  (`crates/noxu-rep/tests/torture_test.rs`,
  `crates/noxu-xa/tests/xa_chaos_test.rs`) drive their own
  pseudo-random schedules over real TCP/QUIC sockets and mocked
  partitions. Hegel can't shrink across an external state machine
  like a network partition.

- **Crash recovery** — the `crash_recovery_test.rs` suite uses a
  process-spawning crash worker (`crates/noxu-db/src/bin/
  crash_worker.rs`) that Hegel can't drive directly. Use unit tests
  instead.

- **Performance benchmarks** — `criterion` is the right tool.
