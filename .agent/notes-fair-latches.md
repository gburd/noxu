# `env_fair_latches` — JE semantics and Noxu design

## JE reference (read at `~/ws/je`, present in this environment)

- `EnvironmentConfig.java:1071-1094` — doc comment on `ENV_FAIR_LATCHES`
  (`je.env.fairLatches`): "If true, use latches instead of synchronized
  blocks... Latches require that threads queue to obtain the mutex... and
  therefore guarantee that there will be no mutex starvation... In a Java 5
  JVM, where `java.util.concurrent.locks.ReentrantLock` is used for the latch
  implementation, this parameter will determine whether they are 'fair' or
  not. This parameter is 'static' across all environments." Default `false`,
  not mutable (`EnvironmentParams.java:276-279`).
- `LatchImpl.java` (exclusive-only latch): `extends ReentrantLock`, ctor
  `LatchImpl(LatchContext context) { this.context = context; }` — calls the
  **no-arg** `ReentrantLock()` superclass constructor, which is non-fair.
  There is no code path in `LatchImpl` that ever constructs a fair
  `ReentrantLock`.
- `SharedLatchImpl.java:26-34` (shared/exclusive latch):
  `extends ReentrantReadWriteLock`, ctor
  `SharedLatchImpl(boolean fair, LatchContext context) { super(fair); ... }`
  — this is the ONE class shape in JE that is structurally capable of fair
  behaviour, by delegating to `ReentrantReadWriteLock(fair)`.
- `LatchFactory.java:32` and `:48-49` — **both call sites that construct a
  `SharedLatchImpl` hardcode `new SharedLatchImpl(false /*fair*/, context)`.**
  Neither reads `EnvironmentParams.ENV_FAIR_LATCHES` / the env config.
- Exhaustive grep of `~/ws/je/src` and `~/ws/je/test` for
  `ENV_FAIR_LATCHES|fairLatches|FairLatches|getFairLatches|isFairLatches`
  finds **only the two definition sites** (`EnvironmentConfig.java:1094`,
  `EnvironmentParams.java:276-277`). No `configManager.getBoolean(...)` call
  for this param anywhere, unlike its sibling `ENV_FORCED_YIELD`
  (`EnvironmentImpl.java:733`, which IS read).

**Finding:** in the JE source tree available here, `ENV_FAIR_LATCHES` is
itself settable-but-inert — it is documented as controlling latch fairness
but is never threaded from config into any `SharedLatchImpl`/`LatchImpl`
constructor. There is no "faithful JE runtime behaviour" to port; only the
*class-shape intent* (a shared latch that would, if wired, become a fair
`ReentrantReadWriteLock`) and Java's own documented semantics for what that
would have meant.

## The two semantic questions, resolved

**Q1: Does fair mode still permit concurrent readers?**
Java's `ReentrantReadWriteLock(true)` semantics (JDK docs): fair mode uses an
"approximately arrival-order" policy — a contiguous *group* of readers that
arrived before the next queued writer may still share the lock together;
what fairness forbids is a *new* arrival (reader or writer) jumping the
queue ahead of an earlier-arrived, still-waiting thread. So JE's *intended*
fair shared latch: yes, contiguous readers can still run concurrently;
fairness is about admission order, not serialization.

**Q2: Strict FIFO or merely no-barging?**
Per-thread FIFO is not literally promised (the JDK docs deliberately say
"approximately arrival-order" and batch contiguous readers as one unit), so
the real guarantee is **no-barging**: no waiter may be granted ahead of an
earlier-arrived waiter still in the queue. `ReentrantLock(true)` (relevant to
a hypothetical fair `LatchImpl`, though JE never builds one) *is* strict FIFO
via the AQS CLH queue.

## Noxu design decision (deliberate simplification, documented)

Given there is no live JE behaviour to reproduce byte-for-byte, and given
this repository's hard-won lesson from `raw_rwlock.rs` (seven
deadlocks/livelocks across three attempts at a hand-rolled fair/reservation
protocol — every one caught by a test or shuttle, never by review), the
Noxu implementation intentionally chooses the **simpler, strictly stronger**
guarantee over the more complex batched-reader one:

> When `env_fair_latches` is on, **every** acquisition (`acquire_shared` and
> `acquire_exclusive` alike) is granted in strict FIFO arrival order via an
> explicit wait queue. No concurrent-reader batching. This is a superset
> restriction of JE's documented "no-barging" guarantee — it never grants a
> later arrival ahead of an earlier one (satisfying no-barging), and it
> additionally serializes readers that JE's fair mode would have allowed to
> run in parallel. This trade is acceptable because (a) the flag is a
> diagnostic/anti-starvation ordering aid, not a throughput feature — like
> its sibling `env_forced_yield` — and (b) building batched-reader admission
> ordering (tracking each queued waiter's kind and computing "front
> contiguous-reader run") is exactly the class of extra state-machine
> complexity that caused the `raw_rwlock.rs` incidents, for a property no
> test in this task requires.

### Mechanism: FIFO wait queue (not a bare ticket counter)

A bare `next_ticket`/`now_serving` pair cannot cleanly support **give-up on
timeout** (a queued waiter that times out mid-queue must not leave a
permanent gap that strands every ticket behind it). Instead: an explicit
`VecDeque<u64>` of waiter ids protected by `noxu_sync::Mutex` +
`noxu_sync::Condvar` (both already-safe, already-tested primitives — no new
`unsafe`):

- `enter(deadline) -> Result<id, ()>`: push a fresh id to the back; wait
  (`cv.wait` / `cv.wait_for`) until this id is at the FRONT of the queue.
  On timeout before reaching the front: remove this id from wherever it sits
  in the queue (never the front, since reaching the front is exactly the
  success condition) and return `Err`. Removing a **non-front** entry never
  changes who is at the front, so no other waiter is starved or stuck by a
  give-up.
- `leave(id)`: pop the front (must equal `id` — the caller is the current
  holder), `notify_all` so the new front (if any) re-checks and proceeds.

This is ported into both `ExclusiveLatch` and `SharedLatch`, gated behind a
new process-global `FAIR_LATCHES: AtomicBool` in `noxu_latch::config`
(mirrors `FORCED_YIELD`/`TIMEOUT_MS` exactly — same `configure()` extension
pattern, same "single relaxed atomic load when off" zero-cost shape). The
fair path wraps around the EXISTING inner `Mutex`/`RwLock` acquisition
(never replaces it): a caller must reach the front of the fair queue before
attempting the real lock at all, so contention on the real lock is
eliminated while fair mode is on (only the front-of-queue thread ever tries
it), and all existing timeout/latch-ordering/forced-yield/owner-tracking
logic is unchanged.

### Verification plan (per the raw_rwlock.rs lesson)

Even though this queue is far simpler than the rwlock reservation state
machine, it gets the same treatment: a standalone shuttle model of exactly
`enter`/`leave`/give-up-on-timeout over the `VecDeque` queue, checked for
(1) no two threads ever hold concurrently, (2) FIFO grant order, (3) a
give-up mid-queue does not strand later entries, before trusting the ported
version in `noxu-latch`.
