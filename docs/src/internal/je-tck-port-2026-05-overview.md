# JE TCK Port (2026-05) — Overview & Status

This document tracks the cross-package port of Berkeley DB Java Edition's
`@Test` methods into Noxu's Rust test suite.  See the per-package TSVs in
this same directory (`je-tck-port-2026-05-enumeration-<package>.tsv`) for
the row-by-row status, and see the per-wave narrative documents
(`wave-4-b-je-tck-port-priority1.md`, …) for what changed in each wave.

## Aggregate status (2026-05-27)

| Bucket | Count |
|---|---:|
| **Total** JE @Test methods enumerated | 2068 |
| PORTED-EQUIVALENT | 147 |
| PORTED-PARTIAL | 62 |
| OUT-OF-SCOPE | 63 |
| NOT-PORTED | 1796 |

"PORTED-EQUIVALENT" means a Rust test exists that asserts the same
invariant as the JE original.  "PORTED-PARTIAL" means the Rust test
captures only a subset of the invariant (typically because Noxu's API
surface is narrower) or is committed `#[ignore]`d to document a Noxu bug.
"OUT-OF-SCOPE" rows are tests that depend on JE-internal classes Noxu
does not expose, on features Noxu has dropped (custom byte comparators,
JMX, JE-specific log versions), or on the JE-specific deployment
topology (e.g. some replication tests).

## Per-package counts

| package | total | ported | partial | oos | not-ported |
|---|---:|---:|---:|---:|---:|
| `bind.serial.test`                            |      7 |      0 |      0 |      0 |      7 |
| `bind.test`                                   |      1 |      0 |      0 |      0 |      1 |
| `bind.tuple.test`                             |     51 |      0 |      0 |      0 |     51 |
| `collections.test.serial`                     |      4 |      0 |      0 |      0 |      4 |
| `collections.test`                            |     23 |      2 |      0 |      0 |     21 |
| `collections`                                 |      3 |      0 |      0 |      0 |      3 |
| `je.cleaner`                                  |    158 |     10 |     17 |      0 |    131 |
| `je.config`                                   |      2 |      0 |      0 |      0 |      2 |
| `je.dbi`                                      |    138 |      9 |      0 |      0 |    129 |
| `je.evictor`                                  |     51 |      2 |      0 |      0 |     49 |
| `je.incomp`                                   |     29 |      0 |      0 |      0 |     29 |
| `je.jmx`                                      |      8 |      0 |      0 |      8 |      0 |
| `je.latch`                                    |      7 |      0 |      0 |      7 |      0 |
| `je.log`                                      |     94 |      9 |      0 |      0 |     85 |
| `je.logversion`                               |     15 |      0 |      0 |     15 |      0 |
| `je.recovery`                                 |     66 |      9 |      3 |      0 |     54 |
| `je.rep.arb`                                  |     21 |      0 |      0 |     21 |      0 |
| `je.rep.dual.trigger`                         |      1 |      0 |      0 |      1 |      0 |
| `je.rep.dupconvert`                           |      5 |      0 |      0 |      5 |      0 |
| `je.rep.elections`                            |     32 |      1 |      0 |      0 |     31 |
| `je.rep.impl.networkRestore`                  |     20 |      5 |      0 |      0 |     15 |
| `je.rep.impl.node`                            |     61 |      4 |      0 |      0 |     57 |
| `je.rep.impl`                                 |     38 |      1 |      0 |      0 |     37 |
| `je.rep.monitor`                              |     17 |      0 |      0 |      0 |     17 |
| `je.rep.node.replica`                         |      3 |      0 |      0 |      3 |      0 |
| `je.rep.persist.test`                         |      9 |      0 |      0 |      0 |      9 |
| `je.rep.stream`                               |     18 |      1 |      0 |      0 |     17 |
| `je.rep.subscription`                         |     18 |      0 |      0 |      0 |     18 |
| `je.rep`                                      |    197 |      9 |      0 |      0 |    188 |
| `je.rep.txn`                                  |     41 |      1 |      0 |      0 |     40 |
| `je.rep.utilint.net`                          |     14 |      0 |      0 |      0 |     14 |
| `je.rep.utilint`                              |     13 |      3 |      0 |      0 |     10 |
| `je.rep.util.ldiff`                           |     37 |      2 |      0 |      0 |     35 |
| `je.rep.util`                                 |     36 |      1 |      0 |      0 |     35 |
| `je.rep.vlsn`                                 |     38 |      6 |      0 |      0 |     32 |
| `je.serializecompatibility`                   |      2 |      0 |      0 |      2 |      0 |
| `je.test`                                     |    163 |      7 |      0 |      0 |    156 |
| `je.tree`                                     |     73 |      8 |      0 |      0 |     65 |
| `je.trigger`                                  |     22 |      1 |      0 |      0 |     21 |
| `je`                                          |    199 |     29 |     22 |      1 |    147 |
| `je.txn`                                      |     74 |      6 |     20 |      0 |     48 |
| `je.util.dbfilterstats`                       |      6 |      0 |      0 |      0 |      6 |
| `je.utilint`                                  |     58 |     13 |      0 |      0 |     45 |
| `je.util`                                     |     81 |      4 |      0 |      0 |     77 |
| `persist.test`                                |     97 |      3 |      0 |      0 |     94 |
| `utilint`                                     |     10 |      1 |      0 |      0 |      9 |
| `util.test`                                   |      7 |      0 |      0 |      0 |      7 |

## Wave summaries

* `wave-4-b-je-tck-port-priority1.md` — wave 4-B (this wave): added 27
  PORTED-EQUIVALENT, 5 PORTED-PARTIAL, 1 OUT-OF-SCOPE rows across `je`,
  `je.dbi`, `je.recovery`, `je.txn`.  Surfaced 3 real Noxu bugs as
  `#[ignore]`-d tests.

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
`/home/gburd/ws/je/test/com/sleepycat/`.  Wave-4-B updates were applied
manually as ports were completed.
