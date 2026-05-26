# Checksum Selection for Noxu Replication Feeder Protocol

## Decision: CRC32 (crc32fast / Ethernet polynomial)

Noxu uses **CRC32 via `crc32fast`** for replication frame integrity verification.

---

## Benchmark Results

Measured on x86-64 Linux (NixOS, kernel 6.12, Noxu bench host) using Criterion.
Both variants checksummed randomized byte payloads; 100 samples each.

| Payload | CRC32 `crc32fast` | CRC32C `crc32c` | CRC32 speedup |
|---------|-------------------|-----------------|---------------|
| 64 B    | 2.16 GiB/s        | 2.48 GiB/s      | CRC32C +15%   |
| 256 B   | 12.03 GiB/s       | 3.87 GiB/s      | **3.1×**      |
| 1 KB    | 15.77 GiB/s       | 4.15 GiB/s      | **3.8×**      |
| 4 KB    | 18.04 GiB/s       | 4.12 GiB/s      | **4.4×**      |
| 64 KB   | 18.81 GiB/s       | 4.26 GiB/s      | **4.4×**      |

Benchmark: `cargo bench --bench util_bench -p noxu-util -- checksum`

### Why CRC32 is faster here

`crc32fast` uses **PCLMULQDQ** (carry-less multiply, SSE4.2) which processes 64 bytes
per clock cycle in parallel — reaching ~20 GiB/s at large payloads.

The `crc32c` crate (v0.6) uses the **`CRC32` SSE4.2 instruction** which processes
8 bytes per instruction, yielding ~4 GiB/s peak regardless of payload size.
The small-payload advantage of CRC32C (64 B: +15%) is explained by lower function-call
overhead in its hot path.

---

## Platform Considerations

| Platform         | CRC32 (crc32fast)                | CRC32C (crc32c)                      |
|------------------|----------------------------------|--------------------------------------|
| x86-64           | PCLMULQDQ → ~18 GiB/s           | SSE4.2 `crc32` → ~4 GiB/s           |
| AArch64          | Software fallback → ~500 MB/s   | `crc32cx` instruction → ~4–8 GiB/s  |
| ARMv7 (32-bit)   | Software fallback → ~300 MB/s   | Software or optional hw → ~300 MB/s |
| RISC-V           | Software fallback                | Software fallback                    |
| macOS / Windows  | Same as above per arch           | Same as above per arch               |

**Key observation**: on ARM platforms CRC32C has hardware acceleration (`crc32cx`
instruction) while CRC32/PCLMULQDQ does not. For a cross-platform project targeting
AArch64 (Raspberry Pi, Apple Silicon, AWS Graviton), CRC32C would be the faster choice.

---

## Rationale for Choosing CRC32

1. **Already a workspace dependency** — `crc32fast` is used in `noxu-util` and `noxu-log`
   for log-entry checksums. Adding a second checksum algorithm in the replication path
   would introduce a dep for parity on one platform.

2. **Dominant deployment target is x86-64** — Noxu's initial production targets are
   x86-64 servers. CRC32 is 4× faster there.

3. **Same error-detection strength** — Both CRC32 and CRC32C detect all 1/2-bit errors,
   all burst errors ≤ 32 bits, and ~99.998% of random error patterns for ≤ 32 KB frames.
   Neither is a cryptographic hash; both are adequate for corruption detection in TCP/QUIC
   transports which already provide their own checksums.

4. **Consistent hashing** — Using the same CRC32 in log entries and replication frames
   simplifies tooling (log scanners, debug dumps, checksums in crash reports are comparable).

---

## If the Decision Were Revisited

CRC32C would be preferred if:

- AArch64 becomes the primary deployment target (AWS Graviton, ARM servers)
- NVMe/filesystem interoperability is required (ext4, iSCSI, NVMe all use CRC32C)
- The `crc32c` crate gains a PCLMULQDQ path for x86-64 (closing the throughput gap)

To switch: replace `crc32fast::Hasher` with `crc32c::crc32c()` in
`noxu-util/src/checksum.rs` and update the feeder frame format version.

---

## Feeder Frame Format (with checksum)

```text
┌──────────────────────────────────────────────────────────┐
│  magic     : u32  (0x4E584D58 = "NXMX")                 │
│  version   : u8                                          │
│  frame_len : u32  (payload length, not including header) │
│  checksum  : u32  (CRC32 of payload bytes)               │
├──────────────────────────────────────────────────────────┤
│  payload   : [u8; frame_len]                             │
└──────────────────────────────────────────────────────────┘
```

Verification: compute `crc32fast::hash(payload)`, compare to `checksum` field.
A mismatch causes the receiver to close the stream with `FrameChecksumError`.

---

## References

- `crc32fast` crate: PCLMULQDQ-accelerated CRC32 — <https://crates.io/crates/crc32fast>
- `crc32c` crate: SSE4.2/ARM hardware CRC32C — <https://crates.io/crates/crc32c>
- Castagnoli, G. et al. (1993). "Optimization of Cyclic Redundancy-Check Codes."
  *IEEE Transactions on Communications* 41(6):883–892.
- Intel. (2009). "Streaming SIMD Extensions — Implementing CRC32C in Software."
  Application Note AP-942.
- Benchmark source: `crates/noxu-util/benches/util_bench.rs` (`checksum` group)
