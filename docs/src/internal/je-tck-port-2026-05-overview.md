# JE TCK Port (2026-05) — Overview & Status

This document tracks the cross-package port of Berkeley DB Java Edition's
`@Test` methods into Noxu's Rust test suite.  See the per-package TSVs in
this same directory (`je-tck-port-2026-05-enumeration-<package>.tsv`) for
the row-by-row status, and see the per-wave narrative documents
(`wave-4-b-je-tck-port-priority1.md`, …) for what changed in each wave.

## Aggregate status (2026-05-28, after wave 11-G)

| Bucket | Count |
|---|---:|
| **Total** JE @Test methods enumerated | 2114 |
| PORTED-EQUIVALENT | 306 |
| PORTED-PARTIAL | 105 |
| OUT-OF-SCOPE | 127 |
| NOT-PORTED | 1531 |

"PORTED-EQUIVALENT" means a Rust test exists that asserts the same
invariant as the JE original.  "PORTED-PARTIAL" means the Rust test
captures only a subset of the invariant (typically because Noxu's API
surface is narrower) or is committed `#[ignore]`d to document a Noxu bug.
"OUT-OF-SCOPE" rows are tests that depend on JE-internal classes Noxu
does not expose, on features Noxu has dropped (custom byte comparators,
JMX, JE-specific log versions, Java BigInteger/BigDecimal bindings,
WeakHashMap GC semantics), or on the JE-specific deployment
topology (e.g. some replication tests).

## Per-package counts

| package | total | ported | partial | oos | not-ported |
|---|---:|---:|---:|---:|---:|
| `bind.serial.test`                             |      7 |      7 |      0 |      0 |          0 |
| `bind.test`                                    |      1 |      0 |      0 |      0 |          1 |
| `bind.tuple.test`                              |     51 |     41 |      0 |     10 |          0 |
| `collections`                                  |      3 |      0 |      0 |      0 |          3 |
| `collections.test`                             |     23 |     12 |      0 |      3 |          8 |
| `collections.test.serial`                      |      4 |      0 |      0 |      0 |          4 |
| `je`                                           |    199 |     51 |     29 |      1 |        118 |
| `je.cleaner`                                   |    158 |     10 |     17 |      0 |        131 |
| `je.config`                                    |      2 |      2 |      0 |      0 |          0 |
| `je.dbi`                                       |    138 |     27 |      2 |      1 |        108 |
| `je.evictor`                                   |     51 |      2 |      5 |      0 |         44 |
| `je.incomp`                                    |     29 |      0 |      0 |      0 |         29 |
| `je.jmx`                                       |      8 |      0 |      0 |      8 |          0 |
| `je.latch`                                     |      7 |      0 |      0 |      7 |          0 |
| `je.log`                                       |     94 |     14 |      0 |      1 |         79 |
| `je.logversion`                                |     15 |      0 |      0 |     15 |          0 |
| `je.recovery`                                  |     66 |     17 |      4 |      0 |         45 |
| `je.rep`                                       |    197 |     15 |      4 |      0 |        178 |
| `je.rep.arb`                                   |     21 |      0 |      0 |     21 |          0 |
| `je.rep.dual.trigger`                          |      1 |      0 |      0 |      1 |          0 |
| `je.rep.dupconvert`                            |      5 |      0 |      0 |      5 |          0 |
| `je.rep.elections`                             |     32 |      7 |      0 |      0 |         25 |
| `je.rep.impl`                                  |     38 |      1 |      0 |      0 |         37 |
| `je.rep.impl.networkRestore`                   |     20 |      5 |      0 |      0 |         15 |
| `je.rep.impl.node`                             |     61 |      4 |      0 |      0 |         57 |
| `je.rep.monitor`                               |     17 |      0 |      0 |      0 |         17 |
| `je.rep.node.replica`                          |      3 |      0 |      0 |      3 |          0 |
| `je.rep.persist.test`                          |      9 |      0 |      0 |      0 |          9 |
| `je.rep.stream`                                |     18 |      1 |      8 |      0 |          9 |
| `je.rep.subscription`                          |     18 |      0 |      0 |      0 |         18 |
| `je.rep.txn`                                   |     41 |      4 |      7 |      0 |         30 |
| `je.rep.util`                                  |     36 |      1 |      0 |      0 |         35 |
| `je.rep.util.ldiff`                            |     37 |      2 |      0 |      0 |         35 |
| `je.rep.utilint`                               |     13 |      3 |      0 |      0 |         10 |
| `je.rep.utilint.net`                           |     14 |      0 |      0 |      0 |         14 |
| `je.rep.vlsn`                                  |     38 |      8 |      3 |      0 |         27 |
| `je.serializecompatibility`                    |      2 |      0 |      0 |      2 |          0 |
| `je.test`                                      |    163 |     10 |      1 |      0 |        152 |
| `je.tree`                                      |     73 |     20 |      0 |      0 |         53 |
| `je.trigger`                                   |     22 |      1 |      0 |      0 |         21 |
| `je.txn`                                       |     74 |     13 |     21 |      0 |         40 |
| `je.util`                                      |     81 |      6 |      0 |      3 |         72 |
| `je.util.dbfilterstats`                        |      6 |      0 |      0 |      0 |          6 |
| `je.utilint`                                   |     58 |     13 |      0 |     45 |          0 |
| `persist.test`                                 |     97 |      8 |      4 |      0 |         85 |
| `util.test`                                    |      7 |      0 |      0 |      0 |          7 |
| `utilint`                                      |     10 |      1 |      0 |      0 |          9 |

## Wave summaries

* `wave-4-b-je-tck-port-priority1.md` — wave 4-B: added 27
  PORTED-EQUIVALENT, 5 PORTED-PARTIAL, 1 OUT-OF-SCOPE rows across `je`,
  `je.dbi`, `je.recovery`, `je.txn`.  Surfaced 3 real Noxu bugs as
  `#[ignore]`-d tests.
* `wave-6-je-tck-port-priority-3-4.md` — wave 6: added 14
  PORTED-EQUIVALENT, 8 PORTED-PARTIAL, 1 OUT-OF-SCOPE rows across
  `je.rep.elections`, `je.rep.vlsn`, `je.evictor`, `je.tree`, `je.dbi`.
  No real Noxu bugs surfaced; one documented semantic difference
  (Noxu's `VlsnIndex::truncate_after` only removes whole buckets and
  clamps the range; JE's `VLSNBucket::removeFromTail` partially trims).
* `wave-8-rep-testbase.md` — wave 8: added the
  `RepTestBase` / `RepEnvInfo` in-memory test harness
  (`crates/noxu-rep/src/test_harness.rs`) and ported 36 heavy tests
  on top of it across `je.rep` (13), `je.rep.txn` (14 + 1 #[ignore]),
  and `je.rep.stream` (9).  Surfaced 1 real Noxu bug:
  `become_master` accepts Secondary nodes (tracked as #[ignore]'d
  wave-8 follow-up).  Net counts: PORTED-EQUIVALENT 196 → 205
  (+9), PORTED-PARTIAL 70 → 89 (+19), NOT-PORTED 1738 → 1710
  (-28).  Some pre-existing PORTED-EQUIVALENT rows (ProtocolTest.
  testBasic, CommitTokenTest.testBasic, ReplicationGroupTest.testBasic)
  were re-tagged PORTED-PARTIAL because Wave 8's harness-level analog
  is a subset of the JE original; this is honest accounting, not a
  regression.
* `wave-9-c-je-tck-ports.md` — wave 9-C: added 34
  substantive new ports across 6 test files plus 11 docs-only
  re-tags of pre-existing analogues that the wave-1D name-match
  heuristic had missed.  Coverage: 18 tuple binding/format/ordering
  ports in `noxu-bind`, 7 cursor-edge / database-config / atomic-put
  ports in `noxu-db`, 2 recovery ports, 3 deadlock / lock-conflict
  ports in `noxu-txn`, 4 file-manager ports in `noxu-log`.  No real
  Noxu bugs surfaced.  Net counts: PORTED-EQUIVALENT 205 → 243
  (+38), PORTED-PARTIAL 89 → 96 (+7), OUT-OF-SCOPE 64 → 77 (+13),
  NOT-PORTED 1710 → 1653 (-57).
* `wave-11-g-je-tck-longtail.md` — wave 11-G (this wave): added 49
  substantive new ports across 7 new/extended test files.  Coverage:
  9 DatabaseTest/EnvironmentTest invariants (incl. 4 #[ignore]'d real
  Noxu bugs); 7 SR-numbered + DupSlotReuse regression tests; 5
  TruncateTest invariants (1 #[ignore]'d real bug); 6
  GetSearchBothRangeTest range-query corner cases; 5 recovery
  invariants (RecoveryDuplicates / Checkpoint / Delete / EdgeTxnId);
  7 tree-level invariants (Split / TreeBalance / KeyPrefix); 9 dup
  cursor invariants (DbCursorDuplicate{,Delete}Test).  Surfaced 5
  real Noxu bugs (committed as #[ignore]'d tests; routed to a
  follow-up bug-fix wave): txn cursor on non-txn db, put_no_overwrite
  semantics on sorted-dups, db-name registry across read-only reopen,
  checkpoint-after-commit loses data, truncate not durable across
  reopen.  Net counts: PORTED-EQUIVALENT 263 → 306 (+43),
  PORTED-PARTIAL 99 → 105 (+6), NOT-PORTED 1580 → 1531 (-49).

## Methodology

Each row in the per-package TSVs is in one of these states:

* **NOT-PORTED** — no Rust test exists.  Default state for new rows.
* **PORTED-EQUIVALENT** — a Rust test asserts the same invariant as the
  JE original.  Names need not match; the test may be in any
  `crates/<crate>/tests/*.rs` file.  The TSV records the file path and
  Rust function name.
* **PORTED-PARTIAL** — a Rust test captures part of the JE invariant.
  The `notes` column documents the gap.  This includes `#[ignore]`-d
  tests that document a Noxu bug.
* **OUT-OF-SCOPE** — the JE test depends on something Noxu does not
  support (custom byte comparators, JE-internal classes, JMX, JE-specific
  replication topologies, …) and there is no behaviour-equivalent port.

## Enumeration source

The enumeration TSVs were generated by `wave1d_enumerate.py` (in this
same directory) from the JE source tree under
`$JE_HOME/test/com/sleepycat/`.  Wave-4-B updates were applied
manually as ports were completed.
