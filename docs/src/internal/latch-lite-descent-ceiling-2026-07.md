# latch-lite / optimistic tree descent: does it clear the ceiling? (measured 2026-07)

## The lever under test

The MVCC proposal (`mvcc-proposal-2026-07.md` §6c option 3) lists **latch-lite /
optimistic latch-coupling** (OLC) as the next lower-risk read lever after §6c
(cheaper lock-based reads, merged, ~+7% ycsb_c) and before MVCC. The idea:
descend the B+tree *without* taking a shared latch on each interior node. Add a
version/seqlock counter to every IN/BIN (bumped under the existing write latch),
read the counter before and after reading the child pointer, and if it did not
change (and the node was not write-locked) the read was consistent — else
restart the descent. This is the well-trodden Bw-tree / OLC technique.

It is a **high-risk** change: it touches the tree's hand-over-hand
latch-coupling — the heart of the read path — in a `#![forbid(unsafe_code)]`
crate, and would need fresh shuttle coverage of concurrent readers vs
splits/eviction. So the discipline is **measure the ceiling first**: latch-lite
can only ever remove the shared-latch acquire/release cost. It cannot touch the
binary-search / key-compare / data-clone / tree-walk cost, nor the record lock
(`lock_ln`, which lives a layer above the tree). If the latch fraction of a
warm point read is small, a *perfect* latch-lite descent yields at most that —
and building a risky concurrency change for a single-digit ceiling is not worth
it.

The question was answered by **direct microbench decomposition**, not by
re-reasoning about the code.

## Instrumentation

`crates/noxu-tree/benches/descent_bench.rs` (criterion) builds a real tree
(fanout 128, the production `NODE_MAX_ENTRIES` default; 200 K and 1 M dense
8-byte keys, 100-byte values → a 3-level tree) and times the read's primitives
in tight loops so the *ratios within one run* are robust even on a busy host:

- `full_read` — the real `Tree::search_with_data` hot path (latch + descent +
  BIN slot lookup + data clone), the whole warm read **minus** `lock_ln`.
- `latch_pair` — one `read()` acquire + `drop` on a resident node (borrow guard).
- `latch_pair_arc` — `read()`+`drop` **plus** an `Arc::clone`/drop, modelling
  the real descent's `read_arc()` owned-guard cost per level (this is the
  primitive latch-lite would remove).
- `bin_search` — one `find_entry_compressed` (BIN binary search).
- `olc_version_check` — two relaxed atomic version loads (read-before,
  re-read-after) + an Arc-pointer read: exactly what an OLC descent does *per
  level instead of* a latch pair. This is the cost latch-lite **keeps**.

`crates/noxu-db/benches/api_bench.rs::db_get_hit` gives the honest end-to-end
denominator: a full `Database::get_into` including `lock_ln` + cursor + txn.

## Measurement (repro box: 8 physical cores, load avg ~15 — busy)

Primitives (stable across repeated runs even under load; `full_read` swings with
host load but is not what drives the ceiling):

| primitive | cost | notes |
|---|---:|---|
| `latch_pair_arc` (real `read_arc()`+drop) | **~38 ns** | removed by latch-lite |
| `latch_pair` (borrow guard, no Arc) | ~28 ns | lower bound of latch cost |
| `olc_version_check` (2 atomic loads + ptr) | **~0.8 ns** | kept by latch-lite |
| `bin_search` (`find_entry_compressed`) | ~42 ns | untouchable |
| `full_read` (depth-3 descent, no `lock_ln`) | ~327 ns | — |
| `db_get_hit` (full end-to-end, **with** `lock_ln`) | ~638 ns | honest denominator |

The single decisive number: **`olc_version_check` (~0.8 ns) is ~50× cheaper than
`latch_pair_arc` (~38 ns).** So latch-lite genuinely *can* remove nearly the
whole per-level latch cost — the technique works. The question is only whether
the latch cost is a big enough slice of the read to matter.

## Ceiling computation

Descent latch cost latch-lite could remove, per read =
`depth × (latch_pair_arc − olc_version_check)`:

- depth 3 (≤ 2 M keys at fanout 128): `3 × (38 − 0.8) ≈ 112 ns`
- depth 4 (YCSB's 10 M records → 128⁴ = 268 M capacity): `4 × 37 ≈ 148 ns`

As a fraction of the read:

| denominator | depth 3 | depth 4 (YCSB shape) |
|---|---:|---:|
| tree descent only (`full_read`) | 112/327 ≈ **34 %** | ~148/436 ≈ **34 %** |
| full end-to-end read (**incl. `lock_ln`**) | 112/~875 ≈ **~13 %** | 148/~984 ≈ **~15 %** |

The end-to-end denominator is the honest one — it is what a real `Database::get`
pays and what ycsb_c measures. `db_get_hit` (638 ns) is a *depth-1* single-key
DB, so its descent is only one level (~90 ns of latch+bin+clone) and its
non-descent stack (`lock_ln` + cursor + txn) is ~548 ns. A realistic depth-3/4
end-to-end read is therefore ~875–984 ns, of which latch-lite could shave
~112–148 ns.

**Ceiling: ~13–15 % of the end-to-end warm point read**, and that is the
*theoretical maximum* assuming a zero-overhead OLC descent with no retry cost.
The *achievable* win is lower: an OLC descent still pays the (small) version
check, adds an optimistic-retry branch on every level, and adds a restart path
that costs a full re-descent when a concurrent split is observed (rare on reads,
but not free). Net realistic win: **~10–14 %.**

## Decision: DO NOT BUILD (this cycle)

A ~13–15 % theoretical / ~10–14 % achievable ceiling, on the **highest-risk part
of the engine** (hand-over-hand coupling in a `forbid(unsafe_code)` crate,
requiring new shuttle proofs of reader-vs-split/eviction safety), against the
reference point that §6c — which also targeted the read path and was far lower
risk — delivered ~+7 % in practice, does not clear the bar. The proposal's own
go/no-go rule: *"if the ceiling is small (< ~10–15 %), recommend NOT building it
… do NOT build a risky concurrency change for a < 10 % ceiling."* This lands
right on that boundary, and the risk asymmetry (a subtle reader-vs-split
concurrency bug in the read core vs a low-teens best case) resolves it to NO-GO.

More importantly, the decomposition tells us **where the read cost actually
lives**: the tree descent (latch + traversal) is only ~327 ns of a ~875 ns
end-to-end read; the majority (~548 ns) is `lock_ln` + cursor + txn machinery.
Even removing *all* latch cost caps the descent-side win at ~13–15 %. The read
gap to WiredTiger (~4–5×) is therefore **structurally traversal- and
lock-bound, not latch-bound** — it cannot be closed by cheaper latching.

This strengthens the "MVCC or nothing" conclusion for reads: the only lever that
attacks the dominant ~548 ns `lock_ln`/cursor cost — rather than the ~112 ns of
latch cost — is a **lock-free read path**, i.e. MVCC (opt-in read-only snapshot,
per the proposal §6a). Latch-lite optimises the wrong ~13 %.

## Faithfulness note

OLC latch-coupling has **no JE precedent** — JE uses latch-coupling on the read
descent, exactly as Noxu does. Had this measured well it would have been a
justified Noxu-only enhancement (citing the OLC technique). It did not, so the
descent stays faithful to JE's latch-coupling.

## Repro

```bash
cargo bench -p noxu-tree --bench descent_bench            # primitives + ceiling
cargo bench -p noxu-db   --bench api_bench -- db_get_hit   # end-to-end denominator
```

The `[ceiling]` line printed at the head of each config states the measured
tree depth (= latch pairs per read) so the ceiling arithmetic is
self-documenting.
