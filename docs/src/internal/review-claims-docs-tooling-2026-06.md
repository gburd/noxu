# Noxu DB — Production-Readiness Claim & Documentation Review

**Reviewer perspective**: Justin Sheehy (operational honesty), Margo Seltzer (rigor), Keith Bostic (precision)  
**Review date**: 2026-06-03  
**Scope**: claims-validation, documentation, tests-as-evidence, tooling/CI, configuration, replication/XA feature surface  
**Branch under review**: `fix/zb-stale-docs` (targeting v3.1.0), covering v3.0.2 public release state

---

## Summary Table

| ID | Severity | Area | Short description |
|----|----------|------|-------------------|
| C-1 | **Critical** | Docs / README | "15+ GiB/s" CRC32 claim is x86-64-only; untrue on the three cross-compiled CI targets |
| C-2 | **Critical** | Docs / Replication | `become_master` doc promises FeederRunner I/O threads; body only creates in-memory Feeder trackers — no log-streaming to replicas |
| C-3 | **Critical** | Config | "400+ configuration parameters" claim; actual count is 166 |
| H-1 | **High** | Docs / Introduction | Capability matrix header says "v2.2 (current)"; current release is v3.0.2 — matrix is stale by a full major version |
| H-2 | **High** | README | Claims "21 crates"; actual count is 22; AGENTS.md says "19" — three files disagree |
| H-3 | **High** | Config / Security | 7 `EnvironmentConfig` params documented as controlling behavior but never read in production — deferred to Wave ZC, NOT remediated in current branch |
| H-4 | **High** | Security / Replication | `RepConfig::peer_allowlist` / `with_peer_allowlist()` accepted and validated but has **zero effect** on connection acceptance — silent security no-op |
| H-5 | **High** | Benchmarks | Benchmark table in `operations/benchmarks.md` is from Noxu v2.2.1; presented as current capability without version caveat in headline |
| H-6 | **High** | Replication docs | `known-limitations.md` "`become_master`…not functional in **v1.3.0**" — version reference stale by 1.7 major versions |
| H-7 | **High** | Spec / Docs | `noxu-spec/src/lib.rs` claims "All eleven specs carry a `VALIDATED-AS-OF` stamp"; 3 of 11 have no such stamp |
| H-8 | **High** | README unsafe table | README:228 claims `noxu-db` has "`unsafe impl Send for SecondaryConfig`"; this block was removed — zero such unsafe in the current codebase |
| M-1 | **Medium** | Engine stubs | `Engine::close()` doc lists "3. Close EnvironmentImpl" as a step; body skips it with an explicit TODO comment |
| M-2 | **Medium** | Engine stubs | `verify_environment()` / `verify_database()` stubs return `passed: true` unconditionally (wave-zb added `log::warn!` but body is still a no-op) |
| M-3 | **Medium** | Replication | `ReplicatedEnvironment::new()` doc: "starts participating…creates election…establishes contact with group"; body does none of this — only `open()` starts the election driver |
| M-4 | **Medium** | Replication | `shutdown_group` doc: "The Master waits for all active Replicas to catch up…"; no such wait implemented |
| M-5 | **Medium** | Tests / Feature | `JoinCursor` (README featured, `operations/known-limitations.md` documented) — only test is `#[ignore]`; no functional test evidence |
| M-6 | **Medium** | Benchmarks | `benchmarks.md` W13 sorted-dup walk documents two open correctness bugs ("yields only 1–2 records before error"); bugs are still open as of v3.0.2 |
| M-7 | **Medium** | Observability | `noxu-observe` not published to crates.io; users enabling the `observability` feature of `noxu` will get a dep resolution failure — not mentioned in `known-limitations.md` |
| M-8 | **Medium** | CI / Tooling | Forgejo CI (`lxc-bookworm` runner) is a self-hosted runner with no fallback documented; its unavailability silently drops the Codeberg gate |
| M-9 | **Medium** | Spec | `recovery_three_phase.rs` carries a `TODO: model CatalogConsistency (C-6)` — acknowledged in Wave ZB but still open |
| M-10 | **Medium** | Recovery bug | `record_active_txn` in `noxu-recovery` prop_test has an open TODO describing a confirmed counterexample where `has_active_txns()` reports a phantom committed txn |
| L-1 | **Low** | CI / SemVer | `semver-checks` is `continue-on-error: true` (advisory) in both GitHub and Forgejo workflows; API breaks cannot fail CI |
| L-2 | **Low** | CI / Spec | Forgejo CI has no `spec.yml` equivalent; Stateright specs only run in GitHub Actions |
| L-3 | **Low** | Docs staleness | `design-review.md:22` says "5,002 unit/integration tests"; current count is ≥5,625 (capability matrix) |
| L-4 | **Low** | Cursor API | `Get::SearchLte`, `Get::FirstDup`, `Get::LastDup` exist in the public `Get` enum but return `Unsupported` at runtime; no `#[deprecated]` annotation |

**Severity counts**: Critical: 3 | High: 8 | Medium: 10 | Low: 4 | **Total: 25**

---

## 1. Unvalidatable Claims

### C-1 — "15+ GiB/s" CRC32 claim is architecture-specific [Critical]

**File**: `README.md:111`, `AGENTS.md` (CRC32 section), `docs/src/internal/checksum-selection.md`

**Claim** (README.md:111):
> "Write-ahead log…with CRC32 checksums (15+ GiB/s on CLMUL hardware)"

**AGENTS.md** says: "CRC32: Uses `crc32fast` (CLMUL/PCLMULQDQ hardware acceleration, 15.8 GiB/s at 1KiB)."

**Reality**: The `checksum-selection.md` document (same repo) explicitly states:

| Platform | CRC32 (crc32fast) | CRC32C |
|----------|-------------------|--------|
| x86-64 | PCLMULQDQ → ~18 GiB/s | ~4 GiB/s |
| **AArch64** | **Software fallback → ~500 MB/s** | `crc32cx` → ~4–8 GiB/s |
| ARMv7 | Software fallback → ~300 MB/s | ~300 MB/s |

The CI matrix cross-compiles three targets: `aarch64-unknown-linux-gnu`, `armv7-unknown-linux-gnueabihf`, `riscv64gc-unknown-linux-gnu`. On these targets, `crc32fast` performs at 300–500 MB/s — **30–60× slower than advertised**. The 15+ GiB/s applies only to x86-64 with CLMUL/PCLMULQDQ.

**Why it matters**: Anyone deploying on AWS Graviton (AArch64) — a primary cloud target — will see radically different WAL throughput. The AGENTS.md statement is used as an unqualified design-level claim without the architecture caveat that exists only in an internal document.

**Remediation**: Qualify the claim: "15+ GiB/s on x86-64 with PCLMULQDQ; ~500 MB/s on AArch64 (software fallback)."

---

### C-2 — `become_master` promises FeederRunner I/O threads; none are spawned [Critical]

**File**: `crates/noxu-rep/src/replicated_environment.rs:1253–1256`

**Claim** (function-level doc comment, lines ~1253–1256):
> "If a live `EnvironmentImpl` has been wired in via `with_environment`, a `FeederRunner` + `EnvironmentLogScanner` background thread is spawned for each currently-registered replica (feeder entries in `feeders`)."

**Reality**: The body of `become_master` (lines ~1290–1355) creates in-memory `Feeder` tracker structs per replica and logs a message. No `FeederRunner`, no `EnvironmentLogScanner`, no thread spawn of any kind — regardless of whether `with_environment` was called. The comment in the body says "F9: spawn Feeder trackers" and notes that the architecture is "pull-based: replicas pull from the master's PEER_FEEDER service." But without I/O threads pushing entries into `peer_scanner`, replicas cannot stream log entries from the master over an established connection either.

The `known-limitations.md` acknowledges `become_master` is stubbed but still uses "v1.3.0" as the reference version (see H-6 below). The doc comment on the function contradicts both the known-limitations note and the actual code.

**Why it matters**: This is the core replication mechanism. A production deployment that calls `become_master` (or uses `open()` which triggers the election driver which calls `become_master`) gets a node that believes it is master but does not actively feed replicas. VLSN streaming, ReplicaAckPolicy enforcement, and HA failover all depend on this path.

**Evidence gap**: No integration test exercises a multi-node topology where one node calls `become_master` and a second node subsequently reads from it via `PEER_FEEDER` service and verifies data was replicated. The torture test (`tests/torture_test.rs`) is `#[ignore]`d.

**Remediation**: Either implement the FeederRunner spawn path (close the gap), or update the function doc to say "Feeder thread spawning is not yet implemented; the pull-based PEER_FEEDER service must be contacted by the replica directly."

---

### C-3 — "400+ configuration parameters" claim; actual count is 166 [Critical]

**Files**: `README.md:134`, `AGENTS.md:29`, `crates/noxu-config/src/lib.rs`

**Claim**:
> README.md:134: "**400+ configuration parameters** with typed validation."  
> AGENTS.md:29: "`noxu-config` | 400+ configuration parameters with validation"  
> `noxu-config/src/lib.rs`: "Approximately 400 configuration parameters are defined here"

**Reality**: `grep -c "pub static.*ConfigParam" crates/noxu-config/src/params.rs` = **166**. The `all_params()` function returns a `vec![]` with **166 entries** (confirmed by counting `&` entries in the function body). The manager test confirms "at least 130" — not 400.

**Why it matters**: This is a 2.4× overstatement of a specific numeric claim. The "400+" figure appears to originate from the BDB-JE reference (which has over 400 parameters). Noxu currently implements approximately 41% of those parameters. This is a measurable, verifiable fact that directly contradicts user-facing documentation.

**Remediation**: Update to "166 configuration parameters" (or the actual count at release time). Do not reuse the JE reference count as a Noxu claim.

---

### H-1 — Capability matrix is stale: says "v2.2 (current)" at v3.0.2 [High]

**File**: `docs/src/introduction.md:8–85`

**Claim** (header):
> "## Capability matrix (v1.5 → v2.2)  
> This matrix states what each released line delivers. Columns are git tags (`v1.5.0`, `v1.6.0`, `v2.0.0`, `v2.2.1`)."

The rightmost column is labeled `v2.2 (current)`.

**Reality**: `README.md` says "**Current version**: 3.0.2." v3.0.0 was a major release that introduced SemVer stability guarantees, removed deprecated APIs, and committed ~135 breaking changes (per CHANGELOG). The capability matrix has no v3.0.x column and falsely labels v2.2.1 as "current."

**Why it matters**: A user reading `docs/src/introduction.md` to understand what v3.0.2 supports will see a matrix that is one full major version out of date. Feature decisions (e.g. Stateright spec validation added in v2.0+v2.4, `become_master` election driver wired in v2.0, `with_peer_allowlist` added in v3.0.2) are not reflected.

**Remediation**: Add a v3.0 / v3.0.2 column (even if it just inherits all v2.2 ✅ marks plus the v3.0 additions). Update the "v2.2 (current)" label.

---

### H-5 — Benchmark table is from v2.2.1, not v3.0.2 [High]

**File**: `docs/src/operations/benchmarks.md` (Methodology section, first bullet)

**Claim** (headline):
> "This page reports an end-to-end A/B comparison between Noxu DB (v2.2.1) and the reference implementation, Oracle Berkeley DB Java Edition 7.5.11."

The document is served as `docs/src/operations/benchmarks.md` and linked from the intro as an `operations/` guide without a prominent "these numbers are from a prior version" headline caveat.

**Reality**: The benchmarks were run at v2.2.1. The current release is v3.0.2. The v3.0.0 wave included a `recovery_scan_reduction` fix and `open-txn correctness fix` that materially affect W11 (recovery). Sorted-dup secondary bugs (W13) are documented as still open in the benchmark harness itself. The W13 section says:

> "on noxu the walk currently yields only the first 1–2 records before the engine returns an error."

This bug is described in present tense in a published operational benchmark page, implying it exists in the current release.

**Why it matters**: A prospective user reading the operations guide sees a benchmark claiming Noxu outperforms JE on sequential writes (W01: 1,709 vs 628 ops/s) without knowing these are v2.2.1 numbers, that the test substrate is `tmpfs` (fdatasync is instant), and that a known correctness bug is embedded in the same document.

**Remediation**: Add a version banner at the top of `benchmarks.md` stating the numbers are from v2.2.1. Mark W13 numbers as invalid until the cursor bugs are fixed. Either re-run or remove the broken benchmark.

---

### H-7 — "All eleven specs carry a VALIDATED-AS-OF stamp" is false [High]

**File**: `crates/noxu-spec/src/lib.rs:19`

**Claim**:
> "All eleven specs carry a `VALIDATED-AS-OF` stamp in their module preamble."

**Reality**: Of the 11 spec files, only 8 carry a `VALIDATED-AS-OF` stamp (all at `v3.1.0` after Wave ZB). The following three do **not**:

- `crates/noxu-spec/src/flexible_paxos.rs` — no stamp
- `crates/noxu-spec/src/master_transfer.rs` — no stamp
- `crates/noxu-spec/src/network_restore.rs` — no stamp

`lib.rs` further says "The four replication specs (`flexible_paxos`, `vlsn_streaming`, `master_transfer`, `network_restore`) were re-validated at v2.0.0." `vlsn_streaming` was re-stamped in Wave ZB; the other three were not.

**Why it matters**: The stamp mechanism is the project's own evidence chain that each spec corresponds to the current production code. Without stamps, these three specs may have drifted relative to v3.0.2 code changes (election driver wiring, become_master Feeder changes, network restore dispatcher changes in waves 9-A through 11).

**Remediation**: Either add `VALIDATED-AS-OF` stamps to all three (after verifying no divergence), or update `lib.rs` to accurately state which specs carry stamps.

---

### H-8 — README claims `noxu-db` has `unsafe impl Send for SecondaryConfig`; block does not exist [High]

**File**: `README.md:228`

**Claim**:
> "The exceptions are `noxu-sync` (FFI to libc futex / `parking_lot` raw locking), `noxu-log` (memory-mapped I/O), `noxu-rep` (network I/O glue + `parking_lot` raw locking), and one `unsafe` block each in `noxu-latch` (RAII force-unlock), **`noxu-db` (`unsafe impl Send for SecondaryConfig`)**, and `noxu-xa`…"

**Reality**: `grep -rn "unsafe impl Send" crates/noxu-db/src/` returns nothing. `AGENTS.md` correctly states: "`secondary_config.rs`'s former `unsafe impl Send` was removed when the secondary key creator was changed to an owned name." The README was not updated when this block was removed. The actual `unsafe impl Send` in `noxu-log` is for `LogBufferSegment`, not `SecondaryConfig`.

**Why it matters**: The `unsafe` inventory is a safety claim. Citing a removed `unsafe` block erodes trust in the accuracy of the unsafe surface documentation. More importantly, the actual `noxu-log` `unsafe impl Send` is not documented in the README at all (only in AGENTS.md).

**Remediation**: Remove `noxu-db` from the README unsafe list; confirm `noxu-log::LogBufferSegment` is present.

---

## 2. Configuration Parameters Silently Ignored

### H-3 — Seven EnvironmentConfig parameters accepted-and-ignored [High]

**File**: `crates/noxu-db/src/environment_config.rs:159–186`  
**Source**: JE reaudit finding F-1; deferred in `docs/src/internal/wave-zb-stale-docs.md` "Items Deferred" to Wave ZC

The following seven parameters are defined as public fields on `EnvironmentConfig`, have builder methods, have transfer code to `DbiEnvConfig`, and carry doc comments asserting real behavior — but are **never read** in any production code path:

| Parameter | Documented behavior | Verified production reads |
|-----------|-------------------|--------------------------|
| `env_latch_timeout_ms` | "A timeout causes `EnvironmentFailure`" | None — `LatchContext::new` ignores it |
| `env_expiration_enabled` | "Enable TTL-based record expiration" | None — cursor/read path ignores it |
| `env_db_eviction` | "Enable per-database node eviction" | None — evictor ignores it |
| `env_fair_latches` | "FIFO-ordered latches — prevents starvation" | None — always `LatchContext::new` |
| `env_check_leaks` | "Check for lock leaks on database close" | None — close path ignores it |
| `env_forced_yield` | "Force thread yields in critical sections" | None — no yield points read it |
| `env_ttl_clock_tolerance_ms` | "TTL clock tolerance for expiration" | None — expiration logic ignores it |

Verification: `grep -rn "\.env_latch_timeout_ms\|\.env_expiration_enabled\|\.env_db_eviction\|\.env_fair_latches\|\.env_check_leaks\|\.env_forced_yield" crates/` finds only assignment sites and test assertions — zero production reads.

**Why it matters**: `env_expiration_enabled` and `env_latch_timeout_ms` are correctness and safety relevant. Users setting `env_expiration_enabled = true` will get no TTL behavior; users setting `env_latch_timeout_ms` to avoid hung processes will get no timeout. These are described in present-tense docs without any "not yet implemented" caveat.

**Remediation**: Add `/// **Not yet implemented — this parameter is accepted but has no effect in the current release. See [known-limitations](../operations/known-limitations.md).**` to each setter, AND add all seven to `docs/src/operations/known-limitations.md`. Do not leave present-tense behavioral claims on no-op setters.

---

## 3. Feature-vs-Test Gaps

### H-4 — `RepConfig::peer_allowlist` / `with_peer_allowlist()` is a security no-op [High]

**Files**: `crates/noxu-rep/src/auth.rs:1–20`, `crates/noxu-rep/src/rep_config.rs`, `docs/src/maintainer/design-decisions.md:163`

The `auth.rs` module preamble says: "Phase 2 wires the verifier through the dispatcher and the rustls `ServerConfig` / `ClientConfig`. **Phase 2 has not landed yet.**"

`design-decisions.md` Decision 11:
> "**Consequence**: Setting `with_peer_allowlist(…)` currently has **no effect on connection acceptance**. The allowlist is stored and validated structurally, but the verifier is not wired to the TLS stack."

However, `grep -rn "PeerAllowlist" crates/noxu-rep/src/` returns only `auth.rs` — the `PeerAllowlist` type is completely absent from `replicated_environment.rs`, `channel.rs`, `quic_channel.rs`, and `TcpServiceDispatcher`. Any peer can connect to the replication group regardless of what `with_peer_allowlist` is set to.

The API surface (`RepConfig::peer_allowlist`) is public, carries no `#[deprecated]` or `#[doc = "not yet functional"]` annotation, and reads as if it provides peer authentication.

**Why it matters**: `known-limitations.md` documents six auth gaps (authentication, path traversal, unbounded allocation, etc.) and says "deploy only across a trusted network boundary." But a user who reads only the API docs will believe `peer_allowlist` closes the authentication gap. This is a security trap that survives `cargo doc`.

**Remediation**: Add a rustdoc notice to `RepConfig::with_peer_allowlist` and `RepConfig::peer_allowlist` field: "**Not yet enforced — this is Phase 1 foundation only. Connections are not filtered by this allowlist in v3.0.x; see docs/src/operations/known-limitations.md.**" Add to `known-limitations.md` as a named entry.

---

### M-5 — `JoinCursor` advertised in README/docs; sole functional test is `#[ignore]`d [Medium]

**Files**: `crates/noxu-db/src/join_cursor.rs:399`, `docs/src/operations/known-limitations.md`

The `known-limitations.md` entry says: "`JoinCursor` over sorted-dup secondaries — `test_join_intersection_finds_single_match` is `#[ignore]`; `JoinCursor` requires sorted-dup secondary indexes which are a v1.6 feature (Decision 1B). Planned for a dedicated follow-up wave."

`README.md` features `JoinCursor` under higher-level APIs. The single test covering its correctness is `#[ignore = "requires v1.6 sorted-dup secondaries; see Decision 1B / audit F7"]`.

**Why it matters**: There is no passing functional test for `JoinCursor`. Users depending on join queries get either an `Unsupported` error or silent incorrect results. This is not a scale/soak gap — it is a basic correctness gap.

---

### M-6 — `benchmarks.md` W13 documents open correctness bugs in sorted-dup secondary cursor [Medium]

**File**: `docs/src/operations/benchmarks.md:211–225`

The W13 section explicitly states two correctness bugs are present and unfixed:

1. `SecondaryCursor::get_search_key` followed by `get_next_dup_full` returns `SecondaryIntegrityException` for every primary except the lexicographically smallest.
2. `get_first` + repeated `get_next` walks revisit primaries and fail to terminate.

These bugs are described in present tense in the published `operations/` documentation. The W13 benchmark "yields only the first 1–2 records before the engine returns an error." No tracking issue number or resolution target is cited.

**Why it matters**: The operations guide is a user-facing document. Embedding a description of two active correctness bugs — without a "this is fixed in v3.0.2" note — tells users these bugs are current. If they are fixed, the document is wrong. If they are not fixed, the `SecondaryCursor` API is actively broken for multi-primary key ranges, which contradicts the capability matrix showing sorted-dup secondaries as ✅ since v1.6.

**Remediation**: Verify whether the bugs are fixed in v3.0.2. If fixed, update W13 with the fix citation. If not fixed, add to `known-limitations.md` and remove the sorted-dup ✅ from the capability matrix.

---

### M-7 — `noxu-observe` not published to crates.io; `observability` feature silently unusable [Medium]

**File**: `docs/src/contributing/publishing.md:166`, `docs/src/internal/wave-11-m-cratesio-prep.md:40`

`publishing.md:166`:
> "`noxu-observe` | Optional observability glue. The `observability` feature of `noxu-db` will not work for crates.io users until `noxu-observe` is also published. Publish decision deferred to a future release."

`known-limitations.md` does not contain any entry about this.

**Why it matters**: `README.md` documents `tracing`/`metrics`/OpenTelemetry via the `observability` feature. A user who adds `noxu = { version = "3", features = ["observability"] }` will get a dependency resolution error ("package `noxu-observe` not found on crates.io"). There is no warning in the README, in the API docs, or in `known-limitations.md`.

**Remediation**: Add to `known-limitations.md`. Add a rustdoc notice to the `observability` feature gate in `noxu/Cargo.toml` and/or to `noxu-observe/src/lib.rs`.

---

## 4. Replication / XA Correctness-Claim Review

### M-3 — `ReplicatedEnvironment::new()` doc contradicts behavior [Medium]

**File**: `crates/noxu-rep/src/replicated_environment.rs:215–233`

**Claim** (function doc):
> "Creates a replicated environment handle and starts participating in the replication group. The node's state is determined when it joins the group, and mastership is not preconfigured. If the group has no current master, creation will trigger an election to determine whether this node will participate as a Master or a Replica."

**Reality**: `new()` constructs state and starts a TCP service dispatcher. The node remains in `NodeState::Detached` after `new()` returns. The election driver is started by `open()`, not `new()`. The test `test_initial_state_is_detached` confirms: after `new()`, the state is `Detached`.

The `claim-audit-2026-05.md` (which is committed to this repo under `docs/src/internal/`) explicitly lists this as a HIGH finding: "Body does NONE of this — only constructs state and starts a TCP service dispatcher." The CHANGELOG for v3.0.0 does not mark this finding as closed; `known-limitations.md` acknowledges it with: "`ReplicatedEnvironment::new` does not start the replication group."

**Why it matters**: The `new()` doc comment is the most prominent description of the replication system's behavior for most users. It remains incorrect in v3.0.2 despite being flagged in the May 2026 audit.

**Remediation**: Update `new()` doc to accurately describe what it does (constructs state, starts TCP dispatcher, remains `Detached`); update `open()` doc to say "this is the recommended entry point; it starts the election driver."

---

### M-4 — `shutdown_group` doc promises replica catch-up wait; not implemented [Medium]

**File**: `crates/noxu-rep/src/replicated_environment.rs:1882`

**Claim** (function doc and `known-limitations.md`):
> `shutdown_group` doc: "The Master waits for all active Replicas to catch up so that they have a current set of logs, and then shuts them down."

**Reality**: The body sends `SHUTDOWN_GROUP` to each peer with a deadline, then calls `self.close()`. There is no VLSN catch-up check, no `AckTracker` wait, no acknowledgment that all replicas are at the master's LSN before shutdown. Replicas that haven't received the last few entries will restart in a state that needs network restore.

**known-limitations.md** still says "not functional in v1.3.0" (see H-6).

---

### H-6 — `known-limitations.md` stale version reference "v1.3.0" for HA stubs [High]

**File**: `docs/src/operations/known-limitations.md` (row in the limitations table)

**Claim**:
> "**`become_master`, `transfer_master`, `shutdown_group` are partially or entirely stubbed** | Identified by May-2026 claim audit. None of these run the full HA semantics described in their docs. | These APIs should be considered design placeholders, not functional in **v1.3.0**."

**Reality**: This entry says "not functional in v1.3.0" — but the current version is v3.0.2. `transfer_master` was substantially implemented (sends network messages, demotes self to replica). `shutdown_group` was substantially implemented (sends network SHUTDOWN_GROUP to peers). `become_master` partially addresses the Feeder tracking gap. The "v1.3.0" reference was copied verbatim from the original May 2026 claim audit and was never updated.

**Why it matters**: A user reading this limitation thinks these APIs were broken as of a pre-v2 release and may have been fixed since. The stale version number actively misleads about the current state.

**Remediation**: Update to reference the accurate current state: `transfer_master` and `shutdown_group` have been implemented (v2.0+); `become_master` does not yet spawn feeder I/O threads (still incomplete as of v3.0.2).

---

## 5. Tooling, CI, and Repo Hygiene

### H-2 — Crate count disagrees across three files [High]

**Files**: `README.md:152`, `AGENTS.md:20`, `docs/src/maintainer/crate-guide.md:3`

| File | Claim |
|------|-------|
| `README.md:152` | "Noxu DB is a Cargo workspace of **21 crates**" |
| `AGENTS.md:20` | "The 19 crates are organized by implementation layer" |
| `docs/src/maintainer/crate-guide.md:3` | "All 22 crates in the Noxu DB workspace" (updated by Wave ZB) |

**Reality**: Actual count: 22 crates (`noxu`, `noxu-bind`, `noxu-cleaner`, `noxu-collections`, `noxu-config`, `noxu-db`, `noxu-dbi`, `noxu-engine`, `noxu-evictor`, `noxu-latch`, `noxu-log`, `noxu-observe`, `noxu-persist`, `noxu-persist-derive`, `noxu-recovery`, `noxu-rep`, `noxu-spec`, `noxu-sync`, `noxu-tree`, `noxu-txn`, `noxu-util`, `noxu-xa`).

Wave ZB updated `crate-guide.md` but did not update README.md (still 21) or AGENTS.md (still 19). Three authoritative documents contain three different counts.

**Remediation**: Update README.md and AGENTS.md to say 22 crates.

---

### M-8 — Forgejo CI uses `lxc-bookworm` self-hosted runner with no fallback [Medium]

**File**: `.forgejo/workflows/test.yml`, `.forgejo/workflows/docs.yml`, `.forgejo/workflows/spec.yml`

All Forgejo CI jobs use `runs-on: lxc-bookworm`. This is a self-hosted runner. If the runner is unavailable (unregistered, offline, hardware failure), all Codeberg CI jobs will queue indefinitely or fail to start — without any indication in the PR status. The GitHub workflow uses `ubuntu-latest` (GitHub-managed), which always exists.

**Why it matters**: The repo is published to both GitHub and Codeberg. The CONTRIBUTING docs describe "run the full CI suite locally" but do not document that the Forgejo CI depends on an operator-maintained runner. A contributor submitting a PR to Codeberg may see permanently-pending checks.

**Remediation**: Document in `CONTRIBUTING.md` that Codeberg CI requires the `lxc-bookworm` self-hosted runner. Consider a fallback `ubuntu-latest` label or Forgejo's built-in Docker container runner.

---

### L-1 — SemVer gate is advisory (`continue-on-error: true`) [Low]

**Files**: `.github/workflows/test.yml` (semver-checks job), `.forgejo/workflows/test.yml` (semver-checks job)

Both workflows declare:
```yaml
semver-checks:
  name: SemVer checks (advisory)
  continue-on-error: true
```

The comment says "will be promoted to blocking after the first full minor-release cycle post-v3.0.0." The current version is v3.0.2 — no minor release has yet occurred — so the advisory status is arguably still correct. But with `continue-on-error: true`, a PR that breaks a public API will merge without CI failure.

**Remediation**: Document the promotion criteria explicitly (e.g., "promoted to blocking when v3.1.0 ships"), and track it as a known open gate rather than leaving it implicit.

---

### L-2 — No Forgejo equivalent of `spec.yml` (Stateright specs only run on GitHub) [Low]

**File**: `.github/workflows/spec.yml` exists; `.forgejo/workflows/spec.yml` exists.

Wait — the Forgejo spec.yml does exist (confirmed by `ls .forgejo/workflows/`). This item is a false alarm. Both CI systems run the spec workflow. *Retracted.*

---

## 6. Unfinished Work (Code-Level)

### M-1 — `Engine::close()` body has explicit TODO for EnvironmentImpl close [Medium]

**File**: `crates/noxu-engine/src/engine.rs:196`

```rust
// (EnvironmentImpl doesn't have explicit close yet - would be added in full implementation)
```

The doc comment for `close()` (line 171) lists step 3 as "Close EnvironmentImpl." The body skips this step with the above comment. `known-limitations.md` correctly documents this, but the in-code TODO is a maintenance trap — it suggests this is scaffolding code that awaits a "full implementation."

---

### M-2 — `verify_environment()` / `verify_database()` stubs return `passed: true` unconditionally [Medium]

**File**: `crates/noxu-engine/src/verify.rs:469–484`, `513–520`

After Wave ZB, both functions emit `log::warn!` at call time and carry rustdoc saying "Stub — not yet implemented; result does not reflect a real integrity check." This is an improvement. However, the public API still presents `VerifyResult { passed: true, errors: vec![] }` unconditionally, so calling code that checks `result.is_passed()` will always see success. Any operator tooling that calls `env.verify()` is silently given a false pass signal.

**known-limitations.md** documents this. The concern is that the warning requires a `log` subscriber to be observed; an application without a log subscriber gets no indication whatsoever.

**Remediation**: Consider returning a `VerifyResult` with a warning in the `warnings` field, or returning `Err(NoxuError::NotYetImplemented(...))`, so callers cannot silently rely on the false pass.

---

### M-9 — `recovery_three_phase.rs` has an open TODO for CatalogConsistency (C-6) property [Medium]

**File**: `crates/noxu-spec/src/recovery_three_phase.rs:23`

```rust
//! TODO: model CatalogConsistency (C-6) — a `CatalogConsistency` property
```

Wave ZB acknowledged this as deferred. The spec is stamped `VALIDATED-AS-OF: v3.1.0` despite the C-6 property being unmodelled. The recovery_manager.rs C-6 documentation says the MapLN B-tree undo pass is a known remaining gap. The spec therefore does not cover the full recovery protocol.

---

### M-10 — `record_active_txn` bug: confirmed counterexample, open TODO in prop_tests [Medium]

**File**: `crates/noxu-recovery/tests/prop_tests.rs:352–390`

A property test has a detailed inline comment describing a **confirmed counterexample**:

> "Counterexample: events = [Commit(1, lsn), SawActive(1)]. Oracle says `has_active_txns` should be false (the only txn committed); the impl says it's true."

The test is annotated `#[allow(dead_code)]` and has a TODO: decide whether `record_active_txn` should be hardened — tracked under the post-v2.3.0 roadmap.

**Why it matters**: `has_active_txns()` affects the undo phase decision. A false `true` result causes the recovery manager to attempt to undo a txn that is already committed, which can silently drop data or raise a spurious error during recovery. This is a correctness concern even if the in-production analysis pass enforces chronological order — the defense in depth is absent.

---

### L-3 — `design-review.md` says 5,002 tests; capability matrix says 5,625 [Low]

**File**: `docs/src/internal/design-review.md:22`

> "Noxu passes 5,002 unit/integration tests including a 6-hour constant-chaos replication soak"

The capability matrix in `docs/src/introduction.md:77` shows 5,625 passing tests at v2.2.1. The current version is v3.0.2 with additional tests. `design-review.md` is an internal document but it is surfaced in `SUMMARY.md` and cited in wave notes.

**Remediation**: Minor — note in `design-review.md` that the 5,002 number reflects the state at the time of that session; current count is higher.

---

### L-4 — `Get::SearchLte`, `Get::FirstDup`, `Get::LastDup` return `Unsupported` at runtime without deprecation [Low]

**File**: `crates/noxu-db/src/cursor.rs` (implied by `known-limitations.md`), `docs/src/introduction.md` (capability matrix shows ❌)

**Claim** (`docs/src/operations/known-limitations.md`):
> "`Get::SearchLte`, `Get::FirstDup`, `Get::LastDup` — These `Get` enum variants return `NoxuError::Unsupported` at runtime (Wave 11-R audit finding 3-D)."

These variants appear in the public `Get` enum but fail at runtime. There is no `#[deprecated]` annotation, no compile-time warning, and no documentation on the enum variants themselves to indicate they are unimplemented.

**Remediation**: Add a `#[doc]` note ("**Not yet implemented** — returns `NoxuError::Unsupported` at runtime") or a `#[deprecated]` hint to each variant until implemented.

---

## Cross-cutting: ACID and Durability Claims

The high-level claims — "ACID transactions," "crash recovery," "serializable isolation," "deadlock detection" — are all backed by code and tests at functional scope:

- `ACID`: the WAL, lock manager, and three-phase recovery are all present and tested via the JE TCK suite.
- `Crash recovery`: `power_loss_sweep.rs` smoke tests run in CI; Layer 2 (qemu VM kill) is documented but manual.
- `Serializable isolation`: implemented via range locking; covered in `isolation_test.rs` (though the stress variants are `#[ignore]`d).
- `Deadlock detection`: implemented in `noxu-txn::deadlock_detector`; covered by unit tests.

These claims are supportable from the test evidence. The concern is not that they are false, but that:

1. The durability guarantee for `SyncPolicy::NoSync` (tolerate losing committed data on crash) is explained in `transactions/durability.md` but not prominently in `README.md`'s ACID claim.
2. `database.rs:795 sync` silently no-ops when `log_manager` is `None` (documented in claim audit but not in the public `sync()` doc).

These are lower-priority documentation gaps, not evidence of missing functionality.

---

## Appendix: Files and Lines Cited

| ID | File | Line(s) |
|----|------|---------|
| C-1 | README.md | 111 |
| C-1 | docs/src/internal/checksum-selection.md | AArch64 row |
| C-2 | crates/noxu-rep/src/replicated_environment.rs | 1253–1356 |
| C-3 | README.md | 134 |
| C-3 | AGENTS.md | 29 |
| C-3 | crates/noxu-config/src/lib.rs | 18 |
| C-3 | crates/noxu-config/src/manager.rs | 325–328 |
| H-1 | docs/src/introduction.md | 8–13, 104 |
| H-2 | README.md | 152 |
| H-2 | AGENTS.md | 20 |
| H-2 | docs/src/maintainer/crate-guide.md | 3 |
| H-3 | crates/noxu-db/src/environment_config.rs | 159–186, 773–779 |
| H-3 | docs/src/internal/reaudit-2026-05-je.md | F-1 section |
| H-4 | crates/noxu-rep/src/auth.rs | 1–20 |
| H-4 | docs/src/maintainer/design-decisions.md | 163–180 |
| H-5 | docs/src/operations/benchmarks.md | Methodology, W13 |
| H-6 | docs/src/operations/known-limitations.md | become_master row |
| H-7 | crates/noxu-spec/src/lib.rs | 19 |
| H-7 | crates/noxu-spec/src/{flexible_paxos,master_transfer,network_restore}.rs | no stamp |
| H-8 | README.md | 228 |
| M-1 | crates/noxu-engine/src/engine.rs | 174, 196 |
| M-2 | crates/noxu-engine/src/verify.rs | 469–484, 493–520 |
| M-3 | crates/noxu-rep/src/replicated_environment.rs | 215–233 |
| M-4 | crates/noxu-rep/src/replicated_environment.rs | 1882–1955 |
| M-5 | crates/noxu-db/src/join_cursor.rs | 399 |
| M-6 | docs/src/operations/benchmarks.md | 211–225 |
| M-7 | docs/src/contributing/publishing.md | 166 |
| M-8 | .forgejo/workflows/test.yml | all jobs |
| M-9 | crates/noxu-spec/src/recovery_three_phase.rs | 23 |
| M-10 | crates/noxu-recovery/tests/prop_tests.rs | 352–390 |
| L-1 | .github/workflows/test.yml, .forgejo/workflows/test.yml | semver-checks |
| L-3 | docs/src/internal/design-review.md | 22 |
| L-4 | docs/src/operations/known-limitations.md | SearchLte row |

---

*Report generated 2026-06-03. Read-only review; no source files were modified.*
