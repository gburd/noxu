# `NoxuRawRwLock` writer-reservation state table (2026-09, attempt 3)

Mandated by the task brief: write this table BEFORE touching
`raw_rwlock.rs`. Two prior attempts (one sub-agent, one hands-on) patched
`lock_exclusive_slow` locally and introduced four distinct
deadlocks/livelocks because reservation changes what `WRITE_LOCKED` means,
and every reader of that bit has to be re-derived together. This document is
that re-derivation. See
`parking-lot-removal-2026-09.md` ("The honest remaining gap", "Reservation
was implemented and backed out", "Second attempt... also backed out") for the
measured problem and the four known failure modes this table must close.

## Design decision made while building this table

**Delete the `WRITE_WAITING` advisory gate and the `FAIRNESS_THRESHOLD`
time-based escalation. Do not bolt reservation onto them.**

This is the load-bearing decision, and it is very likely the reason attempt
\#2 hit a fourth, unidentified hang: it kept the old eventual-fairness
machinery (`WRITE_WAITING`, `FAIRNESS_THRESHOLD`, `give_up_waiting`,
`stopped_waiting_after_acquire`) running *in addition to* reservation, so two
independent mechanisms were both trying to answer "should new readers be
admitted right now?" and their reconciliation paths could disagree.

Why deletion is safe, not just simpler:

`WRITE_WAITING` existed because the pre-reservation writer's only path to
acquire was `CAS(state == 0 -> WRITE_LOCKED)`, which requires the reader
count to already be zero. A sustained reader stream that never let the count
hit zero starved the writer forever, so admission had to be gated to force
the count toward zero.

Reservation's CAS is different: `CAS(state & WRITE_LOCKED == 0 -> state |
WRITE_LOCKED)`, conditioned **only** on no other writer already holding the
bit — it does not require the reader count to be zero. The instant that CAS
succeeds, `try_lock_shared_fast`'s existing admission check
(`state & WRITE_LOCKED != 0` refuses) already excludes every future reader,
with no separate gate bit needed. A reader hand-over-hand chain cannot block
the reservation CAS at all (unlike the old plain CAS), so the starvation
`WRITE_WAITING` was invented to prevent is structurally impossible once
reservation exists. Keeping the gate around after that point is pure
redundant complexity and exactly the kind of surface where two mechanisms'
edge cases collide.

The one thing `WRITE_WAITING` and `FAIRNESS_THRESHOLD` legitimately bought —
**not** paying the reader-exclusion cost for a writer that would have gotten
in via the very next CAS anyway (measured 2.9x throughput cost for
"gate fires immediately", `raw_rwlock.rs` `WRITE_SPIN_ATTEMPTS` doc comment)
— is preserved by keeping the existing `WRITE_SPIN_ATTEMPTS` barging spin
**before** a writer commits to reserving. Spin depth is unchanged (400,
already tuned and measured at 649k ops/s, matching `parking_lot`'s 657k). The
escalation after spin exhausts changes from "arm an advisory bit and
re-race" to "reserve unconditionally" — no time threshold needed, because the
spin count already IS the "try before committing" phase, and reservation
does not need a second opinion.

Net effect: fewer bits, fewer fields, fewer reconciliation call sites than
either prior attempt, and the starvation property upgrades from *eventual*
fairness (bounded by a threshold that traded off against throughput) to
*structural* fairness (a reservation makes the reader stream itself
self-terminating).

## State definitions

Four states over `state: AtomicU32` (`READERS_MASK` = bits 0-29,
`WRITE_LOCKED` = bit 30). `WRITE_WAITING` (bit 31) is deleted by this design;
bit 31 becomes unused/reserved.

| State | `WRITE_LOCKED` | reader count | Meaning |
|---|---|---:|---|
| `free` | 0 | 0 | Nobody holds or wants the lock |
| `read_held(n)` | 0 | n > 0 | n readers hold shared access; barging admission open |
| `reserved_draining(n)` | 1 | n > 0 | A writer has reserved (owns `WRITE_LOCKED`) but n readers that were already in are still draining; new readers refused |
| `write_held` | 1 | 0 | The reserving writer now has full exclusive ownership |

Auxiliary (not part of `state`, but part of the model):

- `write_waiters: usize` — writers that have **not yet** obtained
  `WRITE_LOCKED` in either form (reserved or held). Incremented on entry to
  the slow path; decremented at the instant a writer succeeds at *any* CAS
  that sets `WRITE_LOCKED` (plain zero-readers CAS during the spin, or the
  reserve CAS after spin exhausts), or on give-up. **Single invariant,
  single decrement rule** — this is what attempt \#2's "writers stay counted
  in `write_waiters` for their whole drain" bug violated.
- `read_waiters: usize` — unchanged from the shipped design: readers parked
  because `WRITE_LOCKED` was set when they tried.
- At most one writer can ever be in `reserved_draining`/be the writer that
  transitions to `write_held`, by construction of the reserve-CAS (condition
  is exclusive on `WRITE_LOCKED`).

### A third futex word is required (found while building the shuttle model, not assumed up front)

The first draft of this table had the reserving writer park on `&state`
and receive a targeted `futex_wake(&state, 1)` from the last reader out,
reasoning that "at most one reservation-drain waiter exists at a time, so a
targeted wake can only reach the reserving writer." **That reasoning is
wrong and is exactly the kind of interaction this task's brief warned
would be easy to miss.** Readers are *also* parked on `&state` — they are
refused admission and park there during both `reserved_draining` and
`write_held`. A targeted `nr_wake=1` on a shared address has no way to
prefer the reserving writer over a parked reader; the OS can wake the
reader instead, which rechecks, finds itself still refused, and re-parks —
**swallowing the wakeup meant for the writer**, stranding it asleep with the
lock already drained and nobody left to notice.

Reusing the existing `write_futex` word for the reserving writer does not
work either, for a different reason: other **non-reserving** writers also
park on `write_futex` while waiting for a turn, and in the pre-reservation
design any parked writer is an equally valid target for a wake (they all
race the same acquire condition, so waking an arbitrary one and letting the
rest stay parked is correct by design). That fungibility does not hold for
the reserving writer: it is the *unique* thread that owns `WRITE_LOCKED` and
is waiting only to notice `READERS_MASK == 0`; no other queued writer can
substitute for it, because every other writer's own reserve/acquire CAS
fails as long as `WRITE_LOCKED` stays set.

Fix: a third, dedicated futex word, `drain_futex: AtomicU32`, that **only**
the current reservation-holder ever parks on. Since at most one writer is
ever `reserved_draining` (the reserve-CAS's own exclusivity), a targeted
wake on `drain_futex` is unambiguous by construction — there is structurally
nobody else who could be asleep on that address to swallow it. It uses the
same generation-counter idiom already proven for `write_futex` (sample the
counter before testing `state`, pass the sample to `futex_wait`, so a
release landing between the test and the park cannot be missed): the last
reader releasing to zero does `drain_futex.fetch_add(1, Release)` then
`futex_wake(&drain_futex, 1)`.

Three disjoint futex words, so no wakeup class can be swallowed by another:

- `state` — readers park here waiting for `WRITE_LOCKED` to clear. Woken in
  full (`ALL`) whenever that becomes possible, never targeted, so no reader
  can ever swallow a wake meant for someone else parked here.
- `write_futex` — writers not yet holding/reserving `WRITE_LOCKED` park here,
  unchanged from the shipped targeted-wakeup design; any one of them is a
  fungible target for a `nr_wake=1`.
- `drain_futex` — **only** the current reservation-holder parks here, so a
  targeted `nr_wake=1` from the last draining reader is guaranteed to reach
  it and nothing else.

## Transition table

| Current state | Event | Guard condition | Successor state | Who must be woken |
|---|---|---|---|---|
| `free` | reader acquire | `state & WRITE_LOCKED == 0`, CAS `state -> state + ONE_READER` succeeds | `read_held(1)` | none (acquirer needed nobody) |
| `free` | writer acquire (fast path / spin) | CAS `0 -> WRITE_LOCKED` succeeds | `write_held` | none |
| `free` | writer reserve | N/A — reserve is only attempted after spin fails while `state != 0`; at `free`, the plain CAS above always succeeds first, so this row cannot occur | — | — |
| `read_held(n)` | reader acquire | `state & WRITE_LOCKED == 0`, CAS `state -> state + ONE_READER` succeeds | `read_held(n+1)` | none |
| `read_held(n)` | reader acquire, gate check fails | never — there is no admission gate once `WRITE_LOCKED` is clear; barging is unconditional in `read_held` | `read_held(n+1)` | none |
| `read_held(n)`, n > 1 | reader release | fetch_sub always succeeds; resulting count > 0 | `read_held(n-1)` | none |
| `read_held(1)` | reader release | fetch_sub leaves count == 0, `prev & WRITE_LOCKED == 0` (no reservation raced in — see note below) | `free` | if `write_waiters > 0`: `notify_one_writer()` (targeted, `write_futex`). Else none. |
| `read_held(n)` | writer acquire (spin CAS) | `state & (READERS_MASK\|WRITE_LOCKED) == 0` fails (n > 0) — spin CAS cannot succeed while readers present | `read_held(n)` (no transition; writer keeps spinning) | none |
| `read_held(n)` | writer reserve | spin exhausted; CAS `state & WRITE_LOCKED == 0 -> state \| WRITE_LOCKED` (preserves reader bits) succeeds | `reserved_draining(n)` | none directly; `write_waiters` decremented now (this writer is no longer "waiting to reserve") |
| `read_held(n)` | writer timeout (during spin, before reserving) | deadline elapsed, no CAS ever succeeded | `read_held(n)` (unchanged) | none — this writer never touched `state`; decrement `write_waiters` only |
| `reserved_draining(n)`, n > 1 | reader release | fetch_sub leaves count > 0 | `reserved_draining(n-1)` | none — the reserving writer only cares about count == 0 |
| `reserved_draining(1)` | reader release | fetch_sub leaves count == 0, `prev & WRITE_LOCKED != 0` | `write_held` | wake the reserving writer: targeted `drain_futex.fetch_add(1, Release)` + `futex_wake(&drain_futex, 1)`. **This is the fix for the whole tail-latency problem**: the reserving writer needs no CAS, no re-race — it already owns `WRITE_LOCKED`; the wakeup is purely "stop waiting and notice you're done." |
| `reserved_draining(n)` | reader acquire attempt | `state & WRITE_LOCKED != 0` → refused unconditionally | `reserved_draining(n)` (reader parks) | none (reader becomes a waiter, nothing to wake) |
| `reserved_draining(n)` | second writer acquire/reserve attempt | reserve-CAS is conditional on `WRITE_LOCKED == 0`; it is 1, so CAS fails | `reserved_draining(n)` (that writer parks on `write_futex`) | none |
| `reserved_draining(n)` | writer timeout (the reserving writer itself gives up) | its own deadline elapsed, reader count still > 0 | `read_held(n)` — `fetch_and(!WRITE_LOCKED)` (**must not** touch reader bits) | wake BOTH unconditionally, not `else if`: if `write_waiters > 0`, `notify_one_writer()`; if `read_waiters > 0`, `futex_wake(&state, ALL)`. Unconditional-both is required — see "Bug 4" mapping below. |
| `write_held` | writer release (`unlock_exclusive`) | always (writer holds it) | `free` — `fetch_and(!WRITE_LOCKED)` (no reader bits to preserve; count is 0 here so nothing else can be set except bit 31, which no longer exists) | wake BOTH unconditionally: if `write_waiters > 0`, `notify_one_writer()`; if `read_waiters > 0`, `futex_wake(&state, ALL)`. |
| `write_held` | reader acquire attempt | `state & WRITE_LOCKED != 0` → refused | `write_held` (reader parks) | none |
| `write_held` | writer acquire/reserve attempt (another writer) | reserve-CAS fails (`WRITE_LOCKED` already 1) | `write_held` (that writer parks on `write_futex`) | none |

### Note on the `read_held(1) -> free` reader-release race

Between the `fetch_sub` in `unlock_shared` and its read of `prev`, a
concurrent writer's reserve-CAS could in principle interleave. This is safe
because `unlock_shared`'s branch on `prev & WRITE_LOCKED` is evaluated
against the **snapshot returned by its own `fetch_sub`**, which is atomic: if
a writer's reserve-CAS is concurrently in flight, exactly one of the two
orderings holds and both are handled by rows already in this table:

- Writer's CAS happens-before the reader's `fetch_sub`: the reader's `prev`
  already shows `WRITE_LOCKED` set, so the reader is actually draining a
  reservation (state was really `reserved_draining(1)`, not `read_held(1)`);
  it takes the `reserved_draining(1) -> write_held` row, not this one.
- Reader's `fetch_sub` happens-before the writer's CAS: the reader correctly
  sees `WRITE_LOCKED` clear (state was genuinely `read_held(1)`) and takes
  this row; the writer's CAS is a separate, later event evaluated against
  the post-release state.

No interleaving produces a third case, so no cell here needs "UNRESOLVED" —
the atomicity of `fetch_sub`'s return value is what makes the branch
well-defined.

## The four known failure modes, mapped to this table

1. **Blind `fetch_or` sets `WRITE_LOCKED` while another writer holds it.**
   Closed by construction: every reservation and every plain acquire in this
   table is a `compare_exchange`/`compare_exchange_weak` conditioned on
   `WRITE_LOCKED` (or the whole word) being clear first — never a
   `fetch_or`. See the `read_held(n) -> reserved_draining(n)` and
   `free -> write_held` rows.

2. **`is_locked_exclusive`/`is_write_locked` conflate reserved with held.**
   Both states have `WRITE_LOCKED == 1`; only `write_held` also has reader
   count `== 0`. The predicates must test
   `state & WRITE_LOCKED != 0 && state & READERS_MASK == 0`, which is exactly
   "current state is `write_held`" per the state definitions above — never
   true for `reserved_draining`.

3. **A reserved writer re-runs the acquire CAS on reaching readers == 0,
   trivially "succeeds", and clears the gate while holding the lock.**
   Closed by deleting the gate (design decision above) *and* by the
   `reserved_draining(1) -> write_held` row never performing a CAS at all —
   the writer already owns `WRITE_LOCKED` from the reserve step; reaching
   reader-count-zero is an **observation**, not an acquisition, so there is
   nothing left to reconcile and nothing to accidentally clear.

4. **`unlock_exclusive` woke writers OR readers via `else if`, and reserving
   writers staying counted in `write_waiters` for their whole drain meant a
   queued writer was almost always present, so readers were never woken.**
   Two independent fixes, both required:
   - `write_waiters` is decremented at the **reserve** transition (the
     `read_held(n) -> reserved_draining(n)` row), not at final acquisition —
     so a writer mid-drain no longer inflates the count seen by
     `unlock_exclusive` or the timeout-give-up row.
   - Both wake-worthy rows (`write_held` release, and the reserving writer's
     own timeout give-up) wake **both** populations unconditionally, not
     `else if`. This is necessary even with the `write_waiters` fix above,
     because it is legitimate for both a queued writer and queued readers to
     exist simultaneously and both deserve a chance to race for the
     newly-freed lock.

   The brief states a fourth, unidentified interaction remained even after
   the unconditional-both fix in attempt \#2. This table's answer is that
   attempt \#2 likely still had `WRITE_WAITING`/`FAIRNESS_THRESHOLD` active
   *concurrently* with reservation — i.e., it patched bug 4 without removing
   the redundant gate mechanism that bug 4's fix and the gate's own
   reconciliation (`give_up_waiting`, `stopped_waiting_after_acquire`) could
   still race against. This document's design decision (delete the gate
   entirely) removes that mechanism rather than trying to further patch its
   interaction with reservation. **This is a hypothesis, not yet proven** —
   the shuttle model (next step) must specifically hunt for any remaining
   lost-wakeup or double-acquire interleaving in the gate-free design before
   it is trusted.

## What the shuttle model must check

- **Mutual exclusion**: never two threads observe themselves as the
  exclusive owner simultaneously (`write_held` is never entered twice
  concurrently).
- **No lost wakeup**: every waiter (reader or writer) that is parked
  eventually gets woken and makes progress — model both wake paths
  (`reserved_draining(1) -> write_held` targeted wake, and the two
  unconditional-both wake rows).
- **No livelock**: a reserving writer that reaches reader-count-zero
  transitions to `write_held` without looping back through any CAS that
  could fail and re-park it.
- **Give-up correctness**: a writer that times out while `reserved_draining`
  correctly clears only `WRITE_LOCKED` (never touches reader bits) and wakes
  both populations.
- **No starvation reintroduction**: with the gate deleted, confirm a reader
  hand-over-hand chain cannot prevent a writer from ever reserving (the
  central claim of the design decision above) — this is the one property a
  pure state-transition model checker can prove that the two prior hands-on
  attempts could only observe empirically after the fact.

See `docs/src/internal/rwlock-shuttle-model-2026-09.md` (next commit) for the
model itself and what it found.
