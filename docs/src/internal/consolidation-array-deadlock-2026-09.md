# `noxu-log` consolidation-array LWL: deterministic self-deadlock (2026-09)

**Status**: **RESOLVED — the feature was RETIRED.** This bug settled the
long-standing "keep or retire the consolidation array" decision: the feature was
not merely unproven-faster (it had previously measured ~100x slower on spread
arrivals), it was *incorrect*. Rather than fix a deadlocking alternative to a
working default path, `consolidation.rs`, its shuttle model, the hanging stress
test, the `noxu.log.consolidationArray` config knob, and all of its config
threading were removed outright (a BREAKING public-knob removal; see the
CHANGELOG `### Removed` entry). That also deleted 5 production `unsafe` blocks
(`noxu-log` 12 -> 7) and made `cargo nextest run -p noxu-log` complete in ~6s
where it had previously hung forever.

The analysis below is retained because it documents the failure mode, the gdb
evidence, and *why* retirement was the right call — not because any fix is
pending. A candidate fix was written during the investigation but was
deliberately superseded by the removal.

**Severity**: hung the entire WAL write path. Gated behind
`noxu.log.consolidationArray`, which **defaulted to `false`**, so the shipped
default path (classic mutex LWL) is unaffected. Any deployment that opts in
hangs under concurrent write load. `noxu-dbi/src/environment_impl.rs:831`
wires the config straight through, so opting in is a one-line config change.

## How it was found

It was NOT found by reading the code. The stated premise going in was that
`cargo llvm-cov -p noxu-log` "exceeds 800s and does not finish" — i.e. a
tooling/performance problem. That premise was wrong: there is no tooling
problem. `noxu-log`'s
`log_manager::tests::test_consolidation_array_stress_64t_prev_offset_chain`
deadlocks, and every "timeout" was that test hanging.

The evidence was a still-live process from the *previous* agent's abandoned
attempt:

```
PID 854134  /tmp/w-cov1/target/llvm-cov-target/debug/deps/noxu_log-4c506bed57a4a2a2
ELAPSED 21:05:09   %CPU 211   Threads 66
```

21 hours of wall time at 211% CPU — a spinning livelock, not a slow test. That
one orphan was consuming ~2 of the box's 8 cores for the better part of a day,
which is also why *other* measurements on the box looked slow (load average was
99–130 on 8 cores; killing the orphan dropped it to ~30).

`gdb --batch -p 854134 -ex "thread apply all bt"` on all 66 threads:

| count | where |
|---:|---|
| 62 | `consolidation::wait_as_follower` (`consolidation.rs:300`) — spinning in the `yield_now` loop; this is the 211% CPU |
| 1 | the batch **leader**, blocked in `LogBuffer::wait_for_zero_and_latch` (`log_buffer.rs:433`), reached via `run_as_leader` → `assign_slot` (`log_manager.rs:385`) → `LogBufferPool::get_write_buffer` (`log_buffer_pool.rs:183`) → `bump_and_write_dirty` (`:228`) → `write_dirty` (`:334`) |
| 1 | a late joiner that became the leader of a *new* batch, blocked on the `log_write_latch` mutex (`log_manager.rs:805`) that the first leader still holds |
| 2 | main thread in `JoinHandle::join`, plus one bookkeeping thread |

All 64 workers accounted for, none able to progress. `grep -c run_as_leader` on
the full backtrace dump returned **0 additional** leaders — so the one leader in
`wait_for_zero_and_latch` was the only thread that could have made progress.

Reproduced independently of coverage instrumentation: a plain
`cargo test -p noxu-log --lib -- --exact
log_manager::tests::test_consolidation_array_stress_64t_prev_offset_chain`
hung until a 900s timeout killed it. So this is not an llvm-cov artifact.

## Mechanism (deterministic, not a race)

The deadlock is a **self-deadlock on the leader's own buffer pin**.

1. `ConsolidationArray::join` makes a committer the **leader** exactly when its
   CAS-push observes `head == null`. That means the leader is, by definition,
   the *earliest arrival* of its batch.
2. `run_as_leader` swaps the whole LIFO stack out, then calls `chain.reverse()`
   to get **arrival order** — which places the leader **first**.
3. The loop calls `assign` (= `LogManager::assign_slot`) per request, in that
   order. `assign_slot` reserves a log-buffer slot via `LogBuffer::allocate`,
   which takes a `write_pin_count` **pin** (`log_buffer.rs:381`,
   `fetch_add`).
4. That pin is released by the committer's own `LogBufferSegment::put`
   (`log_buffer.rs:529`, `fetch_sub`) — which runs back in `log_internal`
   **after `run_as_leader` returns**.
5. A **follower's** pin drains fine: the leader publishes its result and
   Release-stores `done` *inside* the loop, so the follower wakes and `put`s
   while the leader is still working.
6. The **leader's own** pin is different: nothing can release it until
   `run_as_leader` returns. So the leader is stamped first, takes a pin, and
   then keeps processing up to 63 more requests while holding it.
7. The moment any *later* member of the same batch needs a buffer flip,
   `get_write_buffer` → `bump_and_write_dirty` → `write_dirty` →
   `wait_for_zero_and_latch` (`log_buffer.rs:423`) waits for the pin count to
   reach **zero** — but the outstanding pin is the leader's own, and it cannot
   drain until the batch ends, and the batch cannot end until this wait
   returns. Hard cycle.
8. Every follower in the batch then spins in `wait_as_follower` forever, and any
   late joiner that becomes a new-batch leader blocks on `log_write_latch`.

Trigger condition: **any batch of ≥2 committers that causes a buffer flush.**
Not timing-dependent once that happens. The reason it isn't seen constantly is
that batches usually stay size-1 under light load; anything that slows the
leader (coverage instrumentation, a loaded box, 64 threads) grows batch sizes
and makes it near-certain. That makes it a latent CI time bomb independent of
the coverage work.

## The verified fix (in commit `0f69de83`, NOT applied on any merged branch)

One-line ordering change: the leader defers its OWN `assign` until after every
follower has been assigned and published, so its pin is live only across the
return into the caller's `put`.

Consequence: within a batch the leader receives the batch's **highest** LSN
instead of its lowest, so LSN order equals arrival order only among the
followers. That is sound — all members of a batch are concurrently inside
`log()` with no happens-before between them, so no observer can demand a
particular order among them — and the properties the log actually depends on
are untouched, because `assign` still runs exactly once per request, serially:

- LSN uniqueness
- strict monotonicity
- intra-file contiguity (no gaps that would break `prev_offset` chaining)
- the `prev_offset` back-chain

Those four are exactly what the existing stress test's oracle already asserts,
and it passes under the fix.

Results with the fix applied:

| | before | after |
|---|---|---|
| `test_consolidation_array_stress_64t_prev_offset_chain` | hangs forever (>21h observed) | **3.4s**, passes |
| full `noxu-log` lib suite (493 tests) | never completes | **5.2s**, 493 passed / 0 failed |
| `cargo llvm-cov -p noxu-log --lib` | "exceeds 800s, does not finish" | completes; 92.48% region / 91.50% function / 91.37% line |

The commit also adds a deterministic regression guard,
`consolidation::tests::leader_assigns_itself_last_so_its_pin_never_blocks_the_batch`.
Because `join` never blocks, a leader plus N followers can be assembled on a
single thread and the batch driven with zero scheduling luck. It asserts
*directly* that no `assign` runs while the leader's own pin is outstanding
(modelled with a `Cell<bool>`), rather than waiting on a hang — so a
regression fails in **0.00s** instead of wedging CI. Verified to have teeth by
reverting the ordering fix: it fails with

```
assign(f1) ran while the leader's own buffer pin was outstanding: a later
member needing a buffer flip would block forever on a pin that cannot drain
until the batch ends -> funnel self-deadlock
```

## Note for whoever owns the fix

The shuttle model (`crates/noxu-log/tests/shuttle_consolidation.rs`) did **not**
catch this, and would not: it models `assign` as a pure monotonic counter
bump, with no log-buffer pin and no `wait_for_zero_and_latch`. Its oracle is
order-agnostic (uniqueness / contiguity / prev-chain / watermark
monotonicity), so it stays valid under the reordering — but the deadlock is
invisible to it by construction. If the consolidation array is kept, the model
needs a pin/flush abstraction before it can claim to cover this class of bug.
Its one stale comment ("stamps LSNs in arrival order") is corrected in
`0f69de83`.
