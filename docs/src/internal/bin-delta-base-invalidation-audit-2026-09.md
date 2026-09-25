# BIN-delta base-invalidation CLASS sweep — investigation

- Worktree: `/tmp/noxu-sweep`, branch `audit/base-invalidation-sweep`,
  baseline `ff19ddd0` (carries all three fixes: physical-delete `remove_slot`
  a1061397, split left half 1c817fbd, compress-merge BIN survivor d5dd2a25).
- JE reference: v7.5.11 (`/home/gburd/src/je`, confirmed from README line 3).
- Goal: close the CLASS, not one instance. Root cause of the class: a bulk
  BIN entry-replacement that changes which keys a node holds, while leaving
  `last_full_lsn` on a PRE-change full image AND not setting
  `prohibit_next_delta`, lets a later sparse `BINDelta` overlay the stale base
  and lose/resurrect keys on recovery.

## CONCLUSION: CLASS CLOSED. No remaining REAL PRODUCTION GAP

Every reachable bulk BIN entry-replacement in production either (a) sets
`prohibit_next_delta` (split left half, compress BIN survivor, `remove_slot`),
(b) operates on an upper IN which has NO delta machinery and is always logged
full, (c) is a read/merge path that reconstructs base+delta and preserves
`last_full_lsn` (no key-set change a delta cannot express), or (d) is
`#[cfg(test)]`/`#[cfg(noxu_shuttle)]` only. No code change made; no fix
invented for unreachable code.

## The decisive structural fact (kills the whole IN half of the map)

`InNodeStub` (upper INs, `TreeNode::Internal`) has **NO** `is_delta`,
`last_full_lsn`, `last_delta_lsn`, or `prohibit_next_delta` fields — those
live ONLY on `BinStub` (tree.rs:1055 vs 1140-1187). `should_log_delta` is a
`BinStub` method (tree.rs:1877, uses `self.is_delta`). So **an upper IN can
never be logged as a delta in noxu**, and there is no stale-base-over-delta
failure mode for any IN entry mutation.

This is faithful to JE: `shouldLogDelta()` is defined only in `BIN.java:1892`,
NOT `IN.java`. `IN.logInternal` (IN.java:5437-5479) computes
`isDelta = bin.isBINDelta() || (allowDeltas && bin.shouldLogDelta())` **only
inside `if (isBin)`**; the `else` branch for upper INs does
`isDelta = false; logEntry = new INLogEntry<>(node);` (IN.java:5474-5477).
Upper INs are always logged full. Delta-logging is a BIN-only concept in both
codebases.

## Per-site table

| # | Site (tree.rs) | Line | Node kind | Prod-reachable? | Delta-eligible over stale base? | Guarded? | JE cite | Verdict |
|---|---|---|---|---|---|---|---|---|
| 1 | `n.entries = le.clone()` (split, Internal arm) | 4514 | upper IN | yes | **no** — IN has no delta fields, always logged full | n/a (no delta path) | IN.logInternal else-branch IN.java:5474-5477 `isDelta=false`; shouldLogDelta only in BIN.java:1892 | **SAFE** |
| 2 | `b.entries = left` (split, Bottom arm) | 4532 | BIN (left half) | yes | would be, but fixed | **yes**: `b.prohibit_next_delta = true` at 4568 (fix 1c817fbd) | IN.splitInternal IN.java:4154 → logInternal(allowDeltas=false) → setLastFullLsn+setProhibitNextDelta IN.java:5545 | **SAFE (fixed)** |
| 3 | new right sibling BIN | 4608 | BIN (right half) | yes | **no** — created `last_full_lsn = NULL_LSN` → forced full on first log | inherent (NULL base) | isDeltaProhibited: lastFullLsn==NULL BIN.java:1867 | **SAFE** |
| 4 | `rb.entries = combined` (compress, BIN survivor) | 5871/5877 | BIN survivor | **no** (test/shuttle only) | would be, but fixed | **yes**: `rb.prohibit_next_delta = true` at 5922 (fix d5dd2a25) | IN.deleteEntry IN.java:3466 setProhibitNextDelta; INCompressor.java:80-88 | **SAFE (fixed; path is TEST-GATED)** |
| 5 | `rn.entries = combined` (compress, Internal survivor) | 5957/5994 | upper IN survivor | **no** (test/shuttle only) | **no** — IN has no delta fields, always logged full | n/a (no delta path) | same as #1; IN.logInternal else IN.java:5474-5477 | **SAFE** |
| 6 | `lb.entries.clear()` (compress, merged-away left BIN) | 5935 | BIN, then REMOVED | **no** (test/shuttle only) | **no** — parent slot `i+1` removed via `p.remove_entry(i+1)` (6062-6067); node unreachable | node removed from tree | INCompressor pruneBIN → tree.delete(idKey) | **SAFE** |
| 7 | `ln.entries.clear()` (compress, merged-away left IN) | 6037 | IN, then REMOVED | **no** (test/shuttle only) | **no** — IN + removed from tree | node removed; IN has no delta | as #5/#6 | **SAFE** |
| 8 | `delta.entries = base.entries` (`mutate_to_full_bin`) | 7156 | BIN reconstitution | yes (recovery/refault READ) | **no** — READ/merge path, reconstructs base+delta, sets `is_delta=false`; no bulk key change a delta can't express | last_full_lsn preserved by callers (7340, 7410) | BINDelta.reconstituteBIN; BINDeltaLogEntry.readEntry sets lastFullVersion IN.java:323-332,5553 | **SAFE (read path)** |
| 9 | `bin.last_full_lsn = base_full_lsn` (`fetch_node_from_log` BINDelta arm) | 7340 | BIN reconstitution | yes (recovery/refault READ) | **no** — correctly PRESERVES base full LSN | preserves last_full_lsn | BINDeltaLogEntry.readEntry IN.java:5553 setLastFullLsn(item.lsn) | **SAFE (read path)** |
| 10 | `base.last_full_lsn = delta.last_full_lsn` (`mutate_to_full_bin_from_log`) | 7410 | BIN reconstitution | yes (refault READ) | **no** — preserves base full LSN into merged result | preserves last_full_lsn | BIN.fetchFullBIN + mutateToFullBIN | **SAFE (read path)** |
| 11 | `base ... is_delta=false` (`reconstitute_bin_delta`) | 8349 | BIN reconstitution | yes (recovery READ) | **no** — recovery merge from bytes; `is_delta=false, dirty=false` | read path | BINDelta.reconstituteBIN | **SAFE (read path)** |
| 12 | `remove_slot` | 2092-2098 | BIN | yes | root of the class | **yes**: sets `prohibit_next_delta=true` FIRST (fix a1061397) | IN.deleteEntry IN.java:3466 | **SAFE (fixed)** |
| 13 | `InNodeStub::remove_entry` | 1326 | upper IN | yes | **no** — IN has no delta fields | n/a | as #1 | **SAFE** |
| 14 | `shuttle_clear_child` entries.clear() | 5586/5591 | either | **no** — `#[cfg(noxu_shuttle)]` | n/a | DST harness only | — | **TEST-GATED-ONLY (shuttle)** |
| 15 | test-module entries.clear() | 10541/10546 | either | **no** — inside `#[cfg(test)] mod tests` (>8667) | n/a | test only | — | **TEST-GATED-ONLY** |

## Reachability proof (compress-merge is test-only)

`compress_node` (tree.rs:5725) — the only site with the sibling-MERGE bulk
replacement (rows 4-7) — is called ONLY from `Tree::compress()` (tree.rs:5531,
the bare recursive sibling-merge). Exhaustive grep: `Tree::compress()` (bare)
is called ONLY from `#[cfg(test)] mod tests` (tree.rs 10203/10241/10266/10376/
10596/13082, all > line 8667) and the `--cfg noxu_shuttle` harness. It has NO
caller in `crates/*/src/` production code.

Production compression takes a DIFFERENT path that never merges non-empty
siblings:
- Public `Environment::compress()` (noxu-db environment.rs:1841) →
  `EnvironmentImpl::compress_all` (environment_impl.rs:3258) →
  `Tree::compress_bin_with_lock_check` — *slot* compression only (removes
  `known_deleted` slots via `remove_slot`, row 12, already guarded) and prunes
  EMPTY BINs; it never merges two under-full non-empty siblings.
- Background INCompressor daemon (environment_impl.rs:1661) → same
  `compress_bin_with_lock_check`.

So rows 4-7 are latent (defense-in-depth). The compress-merge fix (d5dd2a25)
is correct JE-parity hardening kept in place; the
`compress_merge_base_regression_test` BIN-count assertion
(`bins_before == bins_after`) is the tripwire that flips it to a live
reproduction if a future change ever wires `compress_node` into production.

## Bulk-mutation completeness check

Every `.entries = …` / `.entries.remove` / `.entries.clear` on a `BinStub` in
production (< line 8667) was enumerated:

- `self.entries.remove` appears twice: `InNodeStub::remove_entry` (1326, IN)
  and `BinStub::remove_slot` (2094, guarded). No `truncate`/`retain`/`drain`/
  `pop`/`split_off` on BIN entries anywhere.
- All THREE BIN slot-removal callers route through `remove_slot`:
  `delete_cmp` (2121), delete-by-key (5458), `compress_bin` known-deleted
  removal (6247). None bypass the guard.
- All BIN bulk `keys =`/`lsn_rep = from_…` rebuild sites < 8667 belong to
  split_child (4534/4537, guarded) or compress_node (5877, guarded) or the
  shuttle/removed-sibling clears. No other bulk BIN key-set change exists.
- `recompute_key_prefix`/`apply_new_prefix` are prefix-compression only
  (key-set preserving), not entry-set changes.

## Verdict per the brief's three questions

For every remaining site: it is SAFE or TEST-GATED-ONLY. The three fixes
(remove_slot / split left half / compress BIN survivor) plus the structural
fact that upper INs are never delta-logged and that reconstitution paths are
read-only-and-LSN-preserving together cover the entire class. **CLASS CLOSED.**

No new failing regression written and no code changed: there is no REAL
PRODUCTION GAP to reproduce. Inventing a fix for the test-only sibling-merge
survivor beyond the existing hardening, or for the IN arms (which have no delta
machinery), would be fixing unreachable code — explicitly out of scope per the
brief.

## Commands run (read-only investigation; no build needed — no code changed)

Structural greps against tree.rs and JE, verifying:
- `InNodeStub` has no delta fields; `should_log_delta`/`shouldLogDelta` are
  BIN-only (noxu tree.rs:1055/1140/1877; JE IN.java has no shouldLogDelta,
  BIN.java:1892 does).
- JE `IN.logInternal` upper-IN else-branch forces `isDelta=false`
  (IN.java:5474-5477).
- `compress_node`/`Tree::compress()` bare have no production caller
  (grep across `crates/*/src/`).
- All BIN slot removals route through `remove_slot`; the merged-away sibling's
  parent slot is removed (`p.remove_entry(i+1)`, tree.rs:6062-6067).

No `cargo`/build invocation required — the sweep changed no source. (If a
reviewer wants a smoke check, the existing regressions still cover the fixed
sites: `bin_split_base_regression_test`, `compress_merge_base_regression_test`,
and the tree unit test
`compress_merge_survivor_full_base_is_invalidated_for_delta`.)
