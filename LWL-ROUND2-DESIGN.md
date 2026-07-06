# LWL round 2: atomic buffer-slot reservation (remove nested locks from the LWL)

## WHERE WE ARE (measured)
Round 1 (marshall payload outside LWL) = +19% (277->330 MB/s), still ~11.5% of
the 2,858 MB/s ceiling. Re-profile @512t NO_SYNC: 99% idle CPU, 26% futex on the
LWL (16% wait + 9% WAKE = lock-handoff churn from 512 threads on one mutex).
Trimming the critical section isn't enough; the LWL hold is now dominated by the
nested buffer_pool.lock() + buffer.lock() + allocate (Vec::resize under &mut) +
register_lsn, all serialized inside the LWL.

## THE ROUND-2 CHANGE: lock-free buffer-slot reservation
Today log_buffer::allocate does self.data.resize(offset+size) under a &mut self
buffer lock (needs the mutex). Change to a pre-sized ring/append buffer with an
ATOMIC write position:
  - LogBuffer.data is pre-allocated to full capacity (buffer_size) ONCE.
  - allocate(size): let off = write_position.fetch_add(size); if off+size >
    capacity -> return None (buffer full, caller rolls to next buffer). Else
    return a segment pointing at [off..off+size] + bump pin_count (already
    atomic). NO &mut, NO buffer lock, NO Vec resize.
  - The copy into [off..off+size] (segment.put) already happens OUTSIDE the LWL.
Then the LWL critical section shrinks to: file-flip check + LSN assign
(get_next_available_lsn + set_last_position) + prev_offset + CRC. The
buffer-slot reservation becomes a single atomic fetch_add, NOT a nested mutex.

## CORRECTNESS (must preserve)
- pin_count protocol: wait_for_zero_and_latch in write_dirty still works (the
  flush waits for all in-flight segment.put to finish). The atomic position +
  atomic pin_count are the coordination; the flush latches the buffer, reads
  write_position (the high-water mark), writes [0..position] to disk, resets
  position=0 after all pins drain.
- LSN ordering: still assigned under the LWL (unchanged). The buffer offset and
  the LSN advance must stay consistent -- the LSN offset within a file must
  match the buffer offset region. VERIFY the mapping (file_offset <-> buffer
  position) stays exact so recovery reads the right bytes.
- Buffer roll (full buffer -> next): when fetch_add overflows capacity, the
  writer must roll to a new buffer; this transition still needs coordination
  (only one writer triggers the roll). Keep that minimal (a short lock or CAS
  on current_buffer_index), but OUT of the per-write LWL hot path.
- On-disk format BYTE-IDENTICAL. No unsafe beyond what's already there (the
  existing data_ptr unsafe is fine; do not add more without SAFETY comments).

## GUARDRAILS (same as round 1): crash_recovery 12, recovery_correctness 17,
## sigkill, stepwise_truncation, shuttle_fsync_manager, the 32-thread LWL stress
## test, noxu-log/txn/db suites, clippy, no new unsafe. LSN uniqueness+monotonic
## stress test MUST still pass.

## SUCCESS BAR (empirical, EC2): round 2 must show a SUBSTANTIAL jump (target
## toward 50%+ of the 2,858 MB/s ceiling at 512-1024 clients). If it plateaus
## again near ~350-400 MB/s, STOP and report -- that means the ceiling is
## elsewhere (the single LWL LSN-assign mutex itself, needing sharded logs = a
## from-scratch redesign, out of scope). Do NOT iterate endlessly.
