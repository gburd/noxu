# LWL append critical-section rework (feat/lwl-scaling)

## MEASURED PROBLEM
Noxu NO_SYNC plateaus at ~277 MB/s = ~10% of the 2,858 MB/s device write ceiling;
128->1024 clients = 8x clients but only 1.4x throughput. perf @512 clients:
99% idle CPU, 27% in futex_wait -> the global LWL (log_write_latch) serializes
all WAL appends. NOT I/O-bound, NOT buffer-count-bound (3/16/64 buffers all ~21K).

## THE LWL CRITICAL SECTION TODAY (crates/noxu-log/src/log_manager.rs, fn log ~455-650)
Under log_write_latch.lock() it does, PER WRITE:
  1. header encode into scratch entry_buf
  2. entry_buf[header_size..].copy_from_slice(payload)   <-- PAYLOAD MEMCPY (2KB)
  3. file-flip check + LSN compute
  4. prev_offset patch + CRC32 over the WHOLE entry (2KB)
  5. buffer_pool.lock() + get_write_buffer                <-- NESTED LOCK
  6. buffer.lock() + latch_for_write + allocate + register_lsn  <-- NESTED LOCK
  7. entry_buf.clone()                                    <-- CLONE under LWL
The segment.put (final copy into the buffer) is already OUTSIDE the LWL (good).

## JE'S APPROACH (LogManager.java:327-337, LogEntryHeader.addPostMarshallingInfo)
JE MARSHALLS THE PAYLOAD OUTSIDE logWriteMutex (marshallOutsideLatch=true for
LN entries -- the hot path). Under the latch JE does ONLY: prevOffset + VLSN
writes + checksum-over-the-already-marshalled-buffer + LSN assign + buffer slot.
The expensive payload serialization is outside the latch.

## FIX (JE-faithful): marshall the payload OUTSIDE the LWL
Move steps 1-2 (header encode + payload memcpy into a per-call/thread-local
buffer) BEFORE taking the LWL. Under the LWL do ONLY: file-flip check, LSN
assign, prev_offset patch, CRC32 (needs prev_offset+VLSN which need the LSN --
JE checksums under the latch too, so keep CRC under LWL), and buffer-slot
reservation. Eliminate:
  - the payload memcpy under the LWL (do it outside into the per-call buffer)
  - the entry_buf.clone() under the LWL (the per-call buffer IS the owned bytes)
  - if possible, the nested buffer_pool.lock()/buffer.lock() -- reserve the
    slot with a tighter/lock-free protocol, or at least ensure they are not
    held longer than the slot reservation.
NOTE: CRC over 2KB stays under the LWL (JE does too) -- if that itself is the
serialization ceiling, a SECOND step is to move prev_offset OUT of the checksum
region so CRC can be precomputed outside (a format change -- do NOT do this
without explicit approval; it changes on-disk layout). First just move the
payload memcpy + clone + nested locks out and re-measure.

## HARD GUARDRAILS (all must pass)
- LSN ordering + monotonicity preserved (recovery depends on it).
- Isolation/transactional semantics unchanged.
- crash_recovery_test (12), recovery_correctness_test (17), sigkill tests,
  stepwise_truncation_test, shuttle_fsync_manager -- all green.
- noxu-log lib + noxu-log integration + noxu-txn + noxu-db (heavy excluded) green.
- cargo build --workspace --all-targets + clippy --workspace --all-targets -D warnings.
- No new unsafe. #![forbid(unsafe_code)] preserved where present (noxu-log has
  limited unsafe already; do not add more).
- The checksum must still cover the SAME bytes (correctness of on-disk format).

## VALIDATION (empirical, on EC2 after)
Re-run the NO_SYNC client sweep 128/256/512/1024 + measure device write MB/s.
Goal: move from ~277 MB/s (10%) toward the device ceiling. Also SYNC sweep +
p99 (must stay steady). Report before/after bandwidth + p99 at each client count.
