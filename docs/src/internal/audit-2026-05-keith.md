# Noxu DB Audit — Performance Issues and Correctness Bugs

**Reviewer:** Keith Bostic (hat)
**Date:** 2026-05-29
**Branch audited:** `fix/wave11-l-api-stability`
**Reference docs consulted:** Wave 11-H perf investigation, JE reference at `_/je/`

---

## Preamble

Read-only audit.  No code was modified.  All file:line citations are from the current HEAD of the branch above.  Findings marked "Wave 11-H known" were already identified in the wave doc and are included here for completeness with additional severity context or new detail not captured in the wave.

---

## Section 1 — Hot-Path Allocations on the Data Plane

### F-1.1 — `vec![0u8; entry_size]` allocation on every log write

- **Severity:** High
- **Subsystem:** noxu-log write path
- **File:line:** `crates/noxu-log/src/log_manager.rs:261`
- **What's wrong:** `log_internal()` opens with `let mut entry_buf = vec![0u8; entry_size]`.  This is a heap allocation on **every** log entry write — every `db.put()`, every `txn.commit()`, every `txn.abort()`, every IN/BIN checkpoint flush.  The allocation size varies (14 bytes for a small LN header + tiny key, up to megabytes for large values), so the allocator cannot easily recycle it.
- **Failure mode:** Allocation churn measured at ~3–5 % of total CPU in W10 and W11 profiles (Wave 11-H).  Gets worse with more writers because all threads contend on the same heap arena.
- **Suggested fix:** Pass a caller-supplied `&mut Vec<u8>` scratch buffer (cleared and reused per thread) into `log_internal`.  For the common case where the entry fits in a fixed-size stack frame (small keys/values ≤ 128 bytes), write the header into a stack array and skip the heap allocation entirely.

---

### F-1.2 — `Vec<(Vec<u8>, u64)>` allocation in `collect_dirty_buffers()` on every flush

- **Severity:** High
- **Subsystem:** noxu-log flush path
- **File:line:** `crates/noxu-log/src/log_manager.rs:542–573`
- **What's wrong:** `collect_dirty_buffers()` is called inside the LWL on every `flush_sync()` / `flush_no_sync()` — once per `txn.commit()` in the durable commit path.  It builds a fresh `Vec<(Vec<u8>, u64)>` and clones (`to_vec()`) each dirty buffer's unflushed byte range into a new `Vec<u8>` per buffer.  With 3 log buffers and megabyte-sized buffers in flight, each clone is 1–4 MB.  This allocation happens under the LWL, blocking all other writers.
- **Failure mode:** Under concurrent write load, `malloc` + memcpy inside the LWL serializes all threads.  Wave 11-H profiler shows `__memmove_avx_unaligned_erms` at 2.76 % in W10 8r8w; a significant fraction traces here.
- **Suggested fix:** Pass the pending data to `file_manager.write_buffer()` directly via a borrowed slice (`&[u8]`) without materialising an owned `Vec`.  Use a fixed-size stack array of `(offset, len)` pairs and keep the buffer locked for the duration of the pwrite, or use `pwritev` with iovecs.

---

### F-1.3 — `BytesMut::with_capacity` + `TxnEndEntry` allocation on every `txn.commit()`

- **Severity:** Medium
- **Subsystem:** noxu-db transaction commit
- **File:line:** `crates/noxu-db/src/transaction.rs:867–899` (`write_txn_end`)
- **What's wrong:** `write_txn_end` allocates a `BytesMut` buffer and a `TxnEndEntry` struct on every call.  Since this is called on every commit and every abort, it adds two heap allocations to the commit hot path.  The `TxnEndEntry` wire format is fixed-size (~40 bytes); there is no need for a heap buffer.
- **Failure mode:** Visible in all write-path profiles.  Small but compounding with F-1.1 and F-1.2.
- **Suggested fix:** Use a fixed-size stack array (e.g., `[u8; 64]`) to serialise the commit entry and pass `&stack_buf[..entry.log_size()]` to `lm.log()`.

---

### F-1.4 — Multiple `.to_vec()` and `.clone()` per cursor operation (Wave 11-H known, still present on non-hot paths)

- **Severity:** Medium
- **Subsystem:** noxu-dbi cursor
- **File:line:** `crates/noxu-dbi/src/cursor_impl.rs:765, 800, 1396–1397, 1989–2029, 2448–2449`
- **What's wrong:** After every successful `search()`, `put()`, or `retrieve_next()` the cursor stores the key as `Some(key.to_vec())` and the data as `Some(data.to_vec())`.  For `put`, this is two `to_vec()` calls per write operation.  `clone_position` (used by `dup_cursor()`) clones both `current_key` and `current_data`.  The Wave 11-H report noted this cross-workload at ~1–3 % per workload.  The main redo/search path was fixed (11-I); the `put` path at lines 1989–2029 and the `range_entry` path at line 1819 were not.
- **Failure mode:** Allocation pressure on every write and range-scan.  Gets worse when values are large (e.g., 1 KB+ stored per record).
- **Suggested fix:** Store `current_data` as `Bytes` (zero-copy slice of the BIN entry) rather than an owned `Vec<u8>`.  Only materialise an owned buffer when the user calls `get_current_data()` and wants an owned copy.

---

### F-1.5 — `xid_gtrid.to_vec()` and `xid_bqual.to_vec()` in XA prepare per call

- **Severity:** Low
- **Subsystem:** noxu-db XA
- **File:line:** `crates/noxu-db/src/transaction.rs:819–842` (`write_txn_prepare`)
- **What's wrong:** Each XA `prepare()` call clones the GTRID and BQUAL slices into owned `Vec<u8>` to construct a `TxnPrepareEntry`.  These slices are then serialised into a `Vec<u8>` buffer and written once to the WAL.  The intermediate owned copies are unnecessary.
- **Failure mode:** Two small heap allocations per XA prepare.  Low severity since XA prepare is not called in a tight loop.
- **Suggested fix:** Change `TxnPrepareEntry::new` to accept `&[u8]` references and serialise directly without intermediate owned vectors.

---

### F-1.6 — Recovery `collect_prepared_txn_lns` allocates per-LN for prepared transactions (new, not in Wave 11-H)

- **Severity:** Low
- **Subsystem:** noxu-recovery
- **File:line:** `crates/noxu-recovery/src/recovery_manager.rs:1025–1026`
- **What's wrong:** `collect_prepared_txn_lns()` calls `rec.key.to_vec()` and `rec.data.as_ref().map(|b| b.to_vec())` for every LN belonging to a prepared transaction.  Wave 11-K (Fix 1) eliminated `to_vec()` from the main redo loop, but this post-redo collection still allocates.  For a large prepared transaction (e.g., 10K writes in one XA branch) this adds 20K+ allocations post-recovery.
- **Failure mode:** Slow environment open when there are in-doubt XA transactions.
- **Suggested fix:** Use `Bytes::clone()` (refcount bump only) instead of `to_vec()` since the `LnRecord` carries `Bytes`-backed slices.

---

## Section 2 — Locks Held Across I/O

### F-2.1 — LWL held through `pwrite64` syscall (design choice — documented but flagged)

- **Severity:** Informational
- **Subsystem:** noxu-log
- **File:line:** `crates/noxu-log/src/log_manager.rs:281–395`
- **What's wrong:** The `log_write_latch` (LWL) is held from LSN assignment all the way through `file_manager.write_buffer()` (which calls `guard.write_at(file_offset, data)` — a `pwrite64` syscall on the open file handle).  On most kernels, `pwrite64` blocks in-kernel if the page is dirty in page cache.  All other writers are blocked for the duration.
- **Failure mode:** Under concurrent write load with a spinning disk or a slow kernel page-reclaim cycle, this serialises all committers behind a single `pwrite64`.  On tmpfs the cost is negligible; on real NVMe it is the dominant latency term.  Wave 11-H correctly identifies this as intentional JE design (enables group-commit); it is not a bug but the consequence is that a single slow pwrite serialises all writers.
- **Suggested fix:** This is the documented JE pattern.  The 11-J wave proposes a lock-free queue variant that decouples pwrite from fsync.  Accept as-is until 11-J lands; document the LATENCY SPIKE risk under page-cache pressure.

---

### F-2.2 — EnvironmentImpl lock held across entire abort undo loop (HIGH / new)

- **Severity:** High
- **Subsystem:** noxu-db transaction abort
- **File:line:** `crates/noxu-db/src/transaction.rs:529–562`
- **What's wrong:** `Transaction::abort()` acquires `env.lock()` (a `noxu_sync::Mutex` on the entire `EnvironmentImpl`) and holds it while iterating over **all** undo records and calling `tree.insert()` / `tree.delete()` on each one.  For a large transaction (say 100K writes), this loop runs 100K in-memory B-tree operations under a single global environment mutex.  During this time, every other thread that needs `env.lock()` — including threads serving `get()` requests, cursor opens, and checkpoint operations — is blocked.
- **Failure mode:** Under load with large transactions or long-running aborts, throughput collapses for all other users.  Manifests as intermittent latency spikes proportional to the aborting transaction's write set.
- **Suggested fix:** Look up the database `Arc<RwLock<DatabaseImpl>>` while holding `env.lock()` (which is a fast lookup), then drop `env.lock()` and apply the undo records without the env lock.  BDB-JE's `Txn.undoLNs()` does exactly this: it takes the env lock briefly to get the database handle, releases it, then applies undo to the BIN directly via the tree's own latch.

---

### F-2.3 — `write_buffer` calls `path.metadata()` (stat syscall) after every write

- **Severity:** Medium
- **Subsystem:** noxu-log file manager
- **File:line:** `crates/noxu-log/src/file_manager.rs:530–532`
- **What's wrong:** After every call to `write_buffer()`, the implementation calls `path.metadata().map(|m| m.len())` to check whether the file has exceeded `max_file_size` and should be flipped.  This is a `stat()` syscall after every pwrite, i.e., two syscalls per log entry written.  JE tracks the current file position in-memory (via `next_available_lsn.file_offset()`) and compares to `max_file_size` without a syscall.
- **Failure mode:** One extra syscall per write.  Doubles the syscall count on the write data plane.  Under high write throughput (say 200K ops/s) this adds ~200K stat() calls/s.
- **Suggested fix:** The in-memory `next_available_lsn.file_offset()` already tracks the current write position.  Compare `file_offset + entry_size > max_file_size` inside `log_internal` (where the flip decision is already made in the LSN-assignment step at line 288–293) and do not call `path.metadata()` in `write_buffer`.

---

### F-2.4 — File-handle cache lock (`Mutex<LruCache>`) serialises all file opens (informational at current scale, high at 10x)

- **Severity:** Low / informational
- **Subsystem:** noxu-log file manager
- **File:line:** `crates/noxu-log/src/file_manager.rs:75, 297–333`
- **What's wrong:** The LRU file-handle cache is protected by a single `noxu_sync::Mutex`.  Every call to `get_file_handle()` (recovery scan, cleaner, disk-ordered cursor) holds this lock for the full cache lookup + potential `File::open()` + header read.  With many concurrent cleaner threads or recovery parallelism, this becomes a single-point bottleneck.
- **Failure mode:** At 10x cleaner parallelism or parallel recovery, all file-open operations serialise.
- **Suggested fix:** Use a sharded cache (e.g., shard by `file_num % N_SHARDS`) or move file opens outside the lock (check-then-insert with double-checked locking via a concurrent map).

---

## Section 3 — Crash Safety on the Write Path

### F-3.1 — Parent directory not fsynced after new log file creation

- **Severity:** Critical
- **Subsystem:** noxu-log file manager
- **File:line:** `crates/noxu-log/src/file_manager.rs:416–443` (`create_file_internal`)
- **What's wrong:** When a new log file is created, `create_file_internal` calls `file.sync_all()` on the new file to flush its header and data pages.  It does **not** call `fsync()` on the parent directory (`env_dir`).  POSIX requires that the directory entry itself be fsynced to guarantee that the new file survives a crash.  Without it:
  1. `sync_all()` ensures the file's data pages reach stable storage.
  2. But the directory entry (the dentry mapping filename → inode) may not reach stable storage.
  3. On a crash and remount, the directory entry may not be present, leaving the file orphaned or invisible.
  4. Recovery looks up files by scanning `read_dir()`.  If the new file's directory entry is lost, recovery will use the previous file as end-of-log and discard all commits that were written to the new file.
- **Failure mode:** Any crash in the window between `create_file_internal` returning and the next filesystem-level `sync()` can silently lose all log data written to that new file.  Affects every log file flip.  Triggered simply by running under high write load on a journaling FS with `data=writeback` mount option, or after an unexpected power cycle.
- **Suggested fix:**

  ```rust
  // After create_file_internal:
  let dir = File::open(&self.env_dir)?;
  dir.sync_all()?;
  ```

  JE calls `fsync()` on the directory via `FileManager.syncDir()` after every new file is created.

---

### F-3.2 — fsync failure does not invalidate the environment

- **Severity:** Critical
- **Subsystem:** noxu-db commit path
- **File:line:** `crates/noxu-db/src/transaction.rs:318` (`commit_with_durability` calls `write_txn_end`)
- **What's wrong:** When `write_txn_end()` returns `Err` (which happens when `lm.log(..., fsync=true)` fails, i.e., the fdatasync returns EIO or similar), `commit_with_durability` propagates that error to the caller — but **never calls `env_impl.invalidate(EnvironmentFailureReason::LogWrite)`**.  The `EnvironmentImpl::invalid_reason` flag remains unset.  Subsequent transactions can open, write to the WAL, and commit, all on an environment whose storage is in an unknown state.

  On Linux, after a successful `fsync()` followed by an error on a subsequent `fsync()`, the kernel is not required to retry flushing the dirty pages — they may be lost silently (the "fsyncgate" issue, CVE-related).  An EIO from fdatasync leaves the environment in a state where we do not know which of our durably-committed records actually made it to stable storage.

- **Failure mode:** A crash after an EIO-from-fsync can produce a log with committed transactions that were never actually fsynced, but the code treats the environment as usable and continues writing new transactions.  Recovery will replay committed records up to the last good entry, potentially seeing commits interleaved with uncommitted data in unpredictable ways.  The result is silent data loss for the transactions that committed after the first EIO.
- **Suggested fix:**

  ```rust
  // In commit_with_durability, on write_txn_end Err:
  if let Some(env) = &self.env_impl {
      env.lock().invalidate(EnvironmentFailureReason::LogWrite);
  }
  return Err(e);
  ```

  JE calls `envImpl.invalidateEnvironment(EnvironmentFailureException)` on any unhandled I/O error from the LogManager, preventing all further writes.

---

### F-3.3 — TxnCommit record written before state is `Committed`; double-TxnCommit possible on retry

- **Severity:** High
- **Subsystem:** noxu-db transaction commit
- **File:line:** `crates/noxu-db/src/transaction.rs:307–430`
- **What's wrong:** `commit_with_durability` writes the `TxnCommit` WAL entry (line 318), then marks `state = Committed` (line 425).  If `inner.commit()` fails after the WAL write (lines 407–421), the error is propagated to the caller.  The outer state is set to `Committed` before propagating (correct), but the comment on line 397 acknowledges: "a retried `commit()` would append a *second* `TxnCommit` record to the WAL."  The current code guards against this because `check_open()` will fail on the second call.  However, the transition to `Committed` happens AFTER the WAL write, so if the program crashes between the WAL write and the state update, the in-memory state is lost — but that is recoverable from the WAL.

  The bigger concern: between the WAL fsync (line 318) and the `state = Committed` update (line 425), there is a replica-ack wait (lines 333–370) and a cleaner throttle sleep (lines 372–378).  During the cleaner throttle sleep, the env lock is acquired (`env.lock().get_cleaner_throttle()`) and released, but the inner Txn's locks are still held.  Blocked readers are waiting on the write-lock release.  This correctly delays their unblocking until `inner.commit()` releases the locks, but it means committed data is visible to recovery but not to live readers for the duration of the throttle sleep.  This is not a crash-safety issue per se, but it is a reader-starvation window.

- **Failure mode:** Reader starvation under heavy write load when the cleaner throttle fires. Not data loss.
- **Suggested fix:** Keep the replica-ack wait AFTER inner commit (lock release), not before. This minimises the window where the data is committed in the WAL but locks are still held. Note: for replication correctness the ack wait may need to remain before lock release (to prevent reads of un-replicated data); if so, document the reader-starvation window explicitly.

---

### F-3.4 — `sync_data` (fdatasync) used instead of `sync_all` (fsync) for log data; new file header needs both

- **Severity:** Medium
- **Subsystem:** noxu-log file manager
- **File:line:** `crates/noxu-log/src/file_manager.rs:663–666` (`sync_log_end`)
- **What's wrong:** `sync_log_end()` calls `guard.sync_data()` (fdatasync).  For an **existing** file that is being extended, `fdatasync` is sufficient: it guarantees the data pages are stable without syncing metadata (file size, modification time).  However, when this function is called on a *newly created* file (the first write after `create_file_internal`), fdatasync alone is not sufficient to guarantee the file header (written by `file.flush()` + `file.sync_all()` in `create_file_internal`) and the new data pages are both stable in the correct order.  JE uses `force(true)` (full fsync) on the first write to a new file and `force(false)` (fdatasync) on subsequent writes.
- **Failure mode:** A crash on the first write to a new file after the file header has been fsynced but before the log data sync could result in a file that looks valid (has a good header) but has empty/zero data pages — recovery would see it as an empty file.
- **Suggested fix:** Add a `new_file: bool` flag to `sync_log_end` (or detect first-write from `last_used_lsn` == `first_log_entry_offset`) and use `sync_all()` on the first write.

---

### F-3.5 — `FileManagerLogScanner::parse_entry_from_bytes` skips CRC32 validation (new, not in Wave 11-H)

- **Severity:** Critical
- **Subsystem:** noxu-dbi file manager scanner / recovery
- **File:line:** `crates/noxu-dbi/src/file_manager_scanner.rs:290–354`
- **What's wrong:** `parse_entry_from_bytes()` — the function used by the recovery path's `FileManagerLogScanner` — reads the entry header, parses the entry type, item size, and flags, and builds a `LogEntry` from the payload bytes.  It does **not validate the CRC32 checksum**.  The `file_reader.rs::FileReader` does perform checksum validation when `validate_checksum=true`, but `FileManagerLogScanner` never calls `FileReader`; it parses the mmap'd bytes directly.

  Consequence: if a disk returns a bad sector and `mmap` reads corrupt bytes, recovery will parse the garbage as valid log entries.  A silently corrupt LN record (e.g., a CRC-failing entry with wrong key/data bytes) will be redone into the B-tree, producing a database with silently wrong data.  No checksum error is raised; no environment invalidation occurs.

- **Failure mode:** Bit corruption on the storage device → garbage injected into the recovered B-tree → silent data corruption.  Worst case: a committed record's data is corrupted and the user sees wrong values; second-worst case: the parsed entry_size is garbage and the scanner walks off the end of the file, panicking or producing further corrupt parses.
- **Suggested fix:** Add CRC validation to `parse_entry_from_bytes`. The checksum bytes are already in the header (bytes 0–3); the CRC covers bytes 4 onwards.  If CRC fails, return `None` (treat as end-of-log) or return a `ParseError::Checksum` variant that the caller can log as an environment failure.  Mirror the `FileReader` behaviour: `validate_checksum = true` on all recovery readers.

---

### F-3.6 — Partial log entry at end of log parsed as valid (end-of-log detection gap)

- **Severity:** High
- **Subsystem:** noxu-dbi file manager scanner
- **File:line:** `crates/noxu-dbi/src/file_manager_scanner.rs:325–334`
- **What's wrong:** `parse_entry_from_bytes` returns `None` if `offset + entry_size > data.len()` (truncated write).  This correctly handles the case where the entire entry does not fit.  However, if exactly `MIN_HEADER_SIZE` bytes are present (the header is complete but the payload is missing), the function will parse the header, compute `item_size`, and then fail the bounds check on the payload — returning `None`.  This is correct.  **But**: if the header itself is partially written (e.g., 7 of 14 header bytes written before a crash), the first 7 bytes look valid (non-zero entry type, plausible flags) and the remaining 7 are zero.  The `item_size` field at bytes 10–13 is all-zero (= item_size 0), making `entry_size = header_size + 0 = 14`.  The bounds check passes.  The function will try to build a `LogEntry` from an entry with a zero-byte payload — which for most log entry types will fail gracefully (`parse_payload` returns `None`), but for `LogEntryType::TxnCommit` or `TxnAbort` with a zero-byte payload, the result depends on the entry's deserialiser.

  Without CRC validation (F-3.5), a partial header is indistinguishable from a valid empty entry.

- **Failure mode:** A crash mid-write of a commit record → recovery treats the partial commit record as present → the transaction is incorrectly treated as committed → data corruption.  This is the classic end-of-log detection failure that JE's `ReadWindow` / `LastFileReader` / `FindLastRecord` handles carefully.
- **Suggested fix:** CRC validation (F-3.5) subsumes this: a partial header will fail CRC and be treated as end-of-log.  As a belt-and-suspenders fix, `parse_entry_from_bytes` should also reject entries where `entry_type_num` is not a valid `LogEntryType` variant.

---

### F-3.7 — `set_last_position` makes three non-atomic stores; readers outside LWL may see torn state

- **Severity:** Medium
- **Subsystem:** noxu-log file manager
- **File:line:** `crates/noxu-log/src/file_manager.rs:277–285`
- **What's wrong:** `set_last_position` stores `last_used_lsn`, then `per_file_last_lsn` (under a write lock), then `next_available_lsn`, then `current_file_num` — four separate operations.  A reader that calls `get_next_available_lsn()` outside the LWL (e.g., the cleaner's utilization tracker) can observe `next_available_lsn` updated to a new file's first offset while `current_file_num` still reflects the old file number.  At that point, `Lsn::from_u64(next_available_lsn)` and `current_file_num` are inconsistent.
- **Failure mode:** Cleaner or checkpoint code that reads both atomics without holding the LWL can compute an LSN pointing to the wrong file.  In normal operation this is an intermittent read of the `current_file_num` at the wrong moment; it causes the cleaner to under-count or over-count entries in a file.  Can lead to premature file deletion.
- **Suggested fix:** Publish the new LSN position as a single `AtomicU64` (the packed `Lsn` already encodes both file number and offset), or acquire a read barrier between the stores. Since `set_last_position` is only called under the LWL, external readers that also hold the LWL are safe; expose a `get_position_under_lwl()` accessor and document that external readers must use it under the LWL.

---

## Section 4 — Performance Pathologies

### F-4.1 — `find_bin_for_key` linear scan for internal-node descent (Wave 11-H known; still present on multiple paths)

- **Severity:** High
- **Subsystem:** noxu-dbi cursor
- **File:line:** `crates/noxu-dbi/src/cursor_impl.rs:1906–1919`
- **What's wrong:** `find_bin_for_key()` traverses from the root to the BIN using a linear scan (`for (i, entry) in n.entries.iter().enumerate()`) at each internal node level.  The Wave 11-I fix added `search_with_data()` for the `SearchMode::Set` path, but `find_bin_for_key` is still called on the `SearchMode::SetRange` path (line 825), on the `put` path (lines 1681, 1778), on the `delete` path (line 1556), and on `update_bin_pin` (line 2481).  For a tree with fanout 128, three internal levels, and 100K records, this is ~3×64 = 192 comparisons per operation on average where binary search would need ~21.

  The `BinStub::find_entry_compressed` function already implements binary search; `Tree::search` and `Tree::search_with_data` use it.  The cursor's own descent code re-implements the descent without it.

- **Failure mode:** 15–16 % of CPU in W03/W04/W10 profiles is `__memcmp_avx2_movbe` tracing to `find_bin_for_key`.  The Wave 11-I fix covers the read path but the write path is unaffected.  Every `db.put()` still pays the linear scan cost.
- **Suggested fix:** Replace the linear scan inside `find_bin_for_key` with `entries.binary_search_by(|e| e.key.as_slice().cmp(key))` with the slot-0 "virtual −∞ key" treatment that `IN.findEntry` in JE uses.  Wave 11-I already demonstrates the feasibility.

---

### F-4.2 — `thread_id()` calls `std::thread::current()` on every mutex fast path (Wave 11-H known)

- **Severity:** Medium
- **Subsystem:** noxu-sync
- **File:line:** `crates/noxu-sync/src/raw_mutex.rs:30–35`
- **What's wrong:** The `thread_id()` function hashes `std::thread::current().id()` via `DefaultHasher::new()`.  On the stable toolchain, `std::thread::current()` increments an `Arc<Thread>` refcount (clone) on every call.  This is called on **every** successful `lock()` fast path (the `owner.store(thread_id(), ...)` at line 75 of `raw_mutex.rs`).  Wave 11-H measured `std::thread::current::current` at 1.41 % in the W10 8r8w profile.  The hash computation itself (`DefaultHasher::new()` + `.hash()` + `.finish()`) adds further overhead.

  Additionally, `DefaultHasher` is not guaranteed to be collision-resistant.  Two threads whose `ThreadId` values hash to the same u64 (after `| 1`) would both claim ownership of the mutex, breaking the owner-tracking invariant.  The collision probability is negligible at small thread counts but non-zero.

- **Failure mode:** ~1.5 % CPU overhead per lock on every mutex acquisition on the data plane.  At 10x thread counts, `Arc<Thread>` clone contention becomes visible.  Collision: misidentification of mutex owner in diagnostic code; does not affect correctness of the mutex itself (the state machine is CAS-based) but can falsely report "mutex owned by thread X" when thread Y holds it.
- **Suggested fix:** Use `thread_local! { static TID: u64 = NEXT_TID.fetch_add(1, Relaxed); }` with a global `AtomicU64` counter.  Assign once at thread creation.  No Arc clone, no hash computation, guaranteed unique.

---

### F-4.3 — `HashMap<u64, Lock>` in lock tables uses `hashbrown` default hasher; potential adversarial input attack vector at scale

- **Severity:** Low / informational
- **Subsystem:** noxu-txn lock manager
- **File:line:** `crates/noxu-txn/src/lock_manager.rs:57`
- **What's wrong:** Lock tables use `hashbrown::HashMap<u64, Lock>` with the default hasher.  LSNs (the key) are structured (file_num << 32 | file_offset) and not adversarially chosen, so hash DoS is not a practical concern.  However, `hashbrown`'s default hasher (AHasher) has different performance characteristics than a simple identity hash for `u64` keys.  For a lockless lookup in a hot shard, a multiplicative hash (e.g., integer multiplication mod prime) would be faster.  Not critical.
- **Failure mode:** Marginal per-lock overhead.
- **Suggested fix:** Consider `HashMap<u64, Lock, BuildHasherDefault<FxHasher>>` for the lock tables since keys are u64 LSNs.

---

### F-4.4 — Deadlock victim selection always passes empty `lock_counts` to `select_victim` (semantic bug)

- **Severity:** Medium
- **Subsystem:** noxu-txn lock manager
- **File:line:** `crates/noxu-txn/src/lock_manager.rs:986`
- **What's wrong:** `check_deadlock_for_waiter` calls `DeadlockDetector::select_victim(&cycle, &HashMap::new())`.  `select_victim` with empty `lock_counts` falls back to "youngest transaction = highest locker ID" for all ties.  JE's documented algorithm (`LockManager.selectVictim()`) selects the transaction with the **fewest locks held** as the primary criterion, falling back to youngest on ties.  With an empty map, "fewest locks" is always 0 for all participants, so the tiebreaker (youngest) always fires — meaning the most recently started transaction is always chosen as the victim, regardless of how much work it has done.

  This is observationally correct for deadlock resolution (a victim is still chosen), but it violates JE's efficiency objective: the victim should be the transaction that has done the least work.  Under heavy write load, a recently-started 100K-write transaction can be victimised in favour of a long-running read transaction that holds fewer locks.

- **Failure mode:** Sub-optimal deadlock victim selection under load.  Large, recently-started write transactions are disproportionately aborted.
- **Suggested fix:** Pass the actual per-locker lock counts.  These are available from the lock tables (iterate each shard once, count entries owned by each locker in the cycle).  Or pass the count directly from the `Txn::write_lock_count()` accessor if the Txn is reachable from the lock manager.

---

### F-4.5 — `list_file_numbers()` calls `read_dir()` on every invocation (O(N) directory scan)

- **Severity:** Medium
- **Subsystem:** noxu-log file manager
- **File:line:** `crates/noxu-log/src/file_manager.rs:222–238`
- **What's wrong:** `get_first_file_num()` and `get_last_file_num()` both call `list_file_numbers()`, which performs a full `fs::read_dir()` scan of the environment directory and sorts the results.  `list_file_numbers()` is called during recovery initialisation, by the cleaner file selector, and by `disk_ordered_cursor_impl` at cursor open.  With 10,000 log files (a realistic long-running production environment), this is an O(N log N) directory scan plus sort on every call.
- **Failure mode:** Recovery startup time and disk-ordered cursor open time grow linearly with the number of log files.  At 10x scale (100K log files after months of operation without checkpointing), recovery scans the directory thousands of times.
- **Suggested fix:** Cache the sorted file-number list in `FileManager`; invalidate the cache on `create_file`, `delete_file`, and `flip_file`.  JE's `FileManager` maintains an in-memory `SortedMap<Long, FileStatus>` that is only rebuilt on demand or after a delete.

---

## Section 5 — Resource Leaks Under Error Paths

### F-5.1 — `Transaction::abort` undo loop silently discards errors from `tree.insert`

- **Severity:** Medium
- **Subsystem:** noxu-db transaction abort
- **File:line:** `crates/noxu-db/src/transaction.rs:539–559`
- **What's wrong:** In `abort()`'s undo loop, `tree.insert(abort_key, abort_data, lsn)` is called with `if let Ok(is_new) = tree.insert(...) && is_new`.  A `tree.insert` that returns `Err(...)` is silently swallowed — the `if let Ok` pattern simply skips the body without incrementing the entry count or logging the error.  If a B-tree insert fails during undo (e.g., due to a split failure or out-of-memory), the before-image is not restored and the tree is left with the dirty post-abort value.  The abort then proceeds to release write locks.  Subsequent readers of that key will see the aborted (wrong) value.
- **Failure mode:** Silent data corruption after an abort that encounters a B-tree error.  Readers see the aborted transaction's data rather than the before-image.
- **Suggested fix:** Log an error and invalidate the environment when `tree.insert` fails during undo.  If the before-image cannot be restored, the database is in an undefined state that must be treated as fatal.

---

### F-5.2 — WAL log entry already written if `inner.commit()` fails after fsync

- **Severity:** Medium
- **Subsystem:** noxu-db commit
- **File:line:** `crates/noxu-db/src/transaction.rs:407–421`
- **What's wrong:** If `inner.commit()` fails after the `TxnCommit` WAL entry has already been fsynced (lines 311–318), the transaction is durably committed on disk.  The code correctly sets `state = Committed` before propagating the inner error.  However, the error propagation says "lock_manager locks may be leaked" — meaning callers can receive an error from `commit()` on a transaction that is actually durable.  Applications that retry on error will attempt a second commit, which `check_open` will correctly reject as `OperationNotAllowed`.  But applications that treat any commit error as "transaction not committed" will have a divergence between application state and database state.
- **Failure mode:** Application data loss or double-apply depending on error handling semantics in the caller.
- **Suggested fix:** Clearly document (already somewhat done) that a non-`OperationNotAllowed` error from `commit()` means the commit **is** durable on disk, and distinguish in the `NoxuError` type between "commit durable, inner error" and "commit failed, not durable".

---

### F-5.3 — Prepared transactions dropped without xa_commit still hold WAL resources indefinitely

- **Severity:** Low
- **Subsystem:** noxu-db XA
- **File:line:** `crates/noxu-db/src/transaction.rs:1048–1065` (`Drop`)
- **What's wrong:** The `Drop` impl correctly notes that a prepared transaction dropped without resolution is intentionally left in-doubt for recovery.  However, the `TxnPrepare` WAL frame causes the log files containing that frame to be "pinned" against cleaning (the cleaner's `FileProtector` must retain all files from `first_lsn` to `last_lsn`).  There is no timeout or operator notification path to flag stale in-doubt transactions.  In a long-running process where XA branches fail silently, these accumulate indefinitely.
- **Failure mode:** Log files accumulate (cleaner cannot reclaim them) until operator manually calls `xa_recover()` and resolves or forces an abort.  Not data loss but operational risk.
- **Suggested fix:** Add a `log::warn!` when an in-doubt transaction has been outstanding for more than a configurable duration.  Expose a metric (`noxu_db_indoubt_txns_total`).

---

## Section 6 — Concurrency Hazards

### F-6.1 — `std::sync::Mutex::lock().unwrap()` used on `inner_txn` and `state` in hot paths

- **Severity:** Medium
- **Subsystem:** noxu-db transaction
- **File:line:** `crates/noxu-db/src/transaction.rs:410, 425, 468, 506, 562, 565, 920, 927`
- **What's wrong:** `Transaction` uses `std::sync::Mutex` (not `noxu_sync::Mutex`) for its `state`, `name`, `lock_timeout_ms`, `txn_timeout_ms` fields and for `inner_txn`.  These are locked with `.lock().unwrap()`.  AGENTS.md acknowledges this is acceptable for `parking_lot::Mutex` (which cannot poison), but `std::sync::Mutex` **can** be poisoned: if a thread panics while holding the lock, the next call to `.lock()` returns `Err(PoisonError)` and `.unwrap()` propagates a panic across thread boundaries.  Seven separate `std::sync::Mutex` objects per `Transaction` means seven potential poison points.
- **Failure mode:** One panicking thread touching any transaction field can cascade poison panics to every other thread that touches any transaction, killing the process.
- **Suggested fix:** Replace the per-field `std::sync::Mutex<T>` fields (`state`, `lock_timeout_ms`, `txn_timeout_ms`, `name`) with either `std::sync::atomic` types (for scalar fields) or `parking_lot::Mutex` (non-poisoning).  This also eliminates 4–5 of the 7 Mutex objects per transaction, reducing per-transaction overhead.

---

### F-6.2 — `waiter_graph` mutex (noxu_sync::Mutex) acquired while shard mutex is held during deadlock check

- **Severity:** High
- **Subsystem:** noxu-txn lock manager
- **File:line:** `crates/noxu-txn/src/lock_manager.rs:339, 963–984`
- **What's wrong:** The lock acquisition sequence in `lock_with_timeout` is:
  1. Acquire shard mutex (one of 64 `lock_tables[idx]`) — registers the waiter.
  2. Release shard mutex.
  3. Call `record_wait()` — acquires `waiter_graph` mutex.
  4. Call `check_deadlock_for_waiter()` — acquires `waiter_graph` mutex again.

  In `check_deadlock_for_waiter` (line 963), the `waiter_graph` is locked and a `HashMap<i64, HashSet<i64>>` snapshot is cloned.  Then (line 984) `DeadlockDetector::detect()` runs a DFS on the cloned snapshot.  The `waiter_graph` lock is held only for the snapshot clone; the DFS runs outside the lock.  This is correct.

  However, at lines 359 and 366, after detecting a deadlock, the code acquires the **shard mutex** again while the `waiter_graph` lock has already been released.  If another thread is simultaneously locking the same LSN and entering the deadlock check path, the two threads can interleave:
  - Thread A: waiter_graph lock → release → shard lock (flush waiter)
  - Thread B: shard lock → waiter_graph lock → shard lock (same shard as A)
  This creates a **lock ordering inversion** between `waiter_graph` mutex and the shard mutex, which is a potential deadlock between the lock manager's own internal locks.

- **Failure mode:** Rare internal deadlock in the lock manager itself under extreme contention with simultaneous deadlock detection on the same lock.  Manifests as a hung process with all threads waiting on `waiter_graph` or a shard mutex.
- **Suggested fix:** Establish a strict lock ordering: always acquire shard mutex before `waiter_graph` (or use a try-lock protocol).  Restructure `lock_with_timeout` to do all state changes to the shard's waiter list while holding only the shard mutex, and call `waiter_graph` update separately with consistent ordering.

---

### F-6.3 — `Arc<Mutex<Txn>>` in `Transaction::inner_txn` locked multiple times per abort

- **Severity:** Low
- **Subsystem:** noxu-db transaction abort
- **File:line:** `crates/noxu-db/src/transaction.rs:506, 562`
- **What's wrong:** `abort()` locks `inner_txn` twice: once to call `abort_collect_undo()` (line 506) and once to call `release_all_locks()` (line 562).  The intermediate undo application does not hold the `inner_txn` lock.  This is safe since write locks are held (readers can't observe the in-flight state), but it means two lock acquisitions per abort.  With `std::sync::Mutex` (not parking_lot), each acquisition requires a syscall if there is any contention.
- **Failure mode:** Minor overhead.  Not a correctness issue.
- **Suggested fix:** Hold the `inner_txn` lock for the full undo-collect + apply + release sequence, or refactor `Txn` to expose a single `abort_and_release(undo_applier: impl FnMut(&UndoRecord))` method.

---

### F-6.4 — `FSyncGroup::take_error()` clones the error `String` inside the mutex

- **Severity:** Low / informational
- **Subsystem:** noxu-log fsync manager
- **File:line:** `crates/noxu-log/src/fsync_manager.rs:113–116`
- **What's wrong:** `take_error()` calls `self.inner.lock().unwrap().error.clone()`.  This allocates a new `String` while holding the `FSyncGroup` mutex.  Under a failed fsync where all N waiters call `take_error()`, N `String` clones happen serially under the mutex.
- **Failure mode:** Minor: slow error propagation after a failed fsync, proportional to the number of concurrent waiters.
- **Suggested fix:** Use `Option<Arc<str>>` for `error`; `clone()` becomes a reference-count increment.

---

## Section 7 — What Does This Do at 10x Scale?

### F-7.1 — Thread-ID hash collision at 10x threads is undocumented and unmitigated

- **Severity:** Medium
- **Subsystem:** noxu-sync, noxu-txn
- **File:line:** `crates/noxu-sync/src/raw_mutex.rs:30–35`, `crates/noxu-txn/src/thread_locker.rs:216–224`
- **What's wrong:** Both `raw_mutex.rs::thread_id()` and `thread_locker.rs::get_thread_id()` use `DefaultHasher::new()` to hash `std::thread::current().id()`.  `DefaultHasher` is not guaranteed to be collision-resistant (it is SipHash in current Rust, which has a 64-bit output, so birthday-collision probability for N threads is N²/2^64).  With 512 threads (10x of the current 64-thread design point), the collision probability is still negligible (~1.4×10⁻¹¹).  However:
  1. The `thread_id()` function is not documented as "may return duplicate values for distinct threads."
  2. If two threads collide, the mutex owner field (`NoxuRawMutex::owner`) will show the wrong thread as owner.  The mutex correctness is not affected (it's CAS-based), but diagnostic output will be wrong.
  3. In `ThreadLocker`, a collision means two distinct threads get the same `thread_id` and are treated as the same locker — they would bypass lock-conflict detection (`shares_locks_with` returning true), allowing a write-write conflict to go undetected.  **This is a correctness bug at scale.**
- **Failure mode:** At 10x threads: near-zero probability of `ThreadLocker` collision, but if it occurs, a write-write conflict is silently permitted, producing last-writer-wins corruption.
- **Suggested fix:** Use `thread_local! { static TID: u64 = NEXT_TID.fetch_add(1, Relaxed); }` as noted in F-4.2.  Zero collision probability, faster than hashing.

---

### F-7.2 — `Vec<BinEntry>` backing store for BINs: insertion cost is O(n) at high fill factor

- **Severity:** Medium
- **Subsystem:** noxu-tree
- **File:line:** `crates/noxu-tree/src/bin.rs` (BinStub::insert_with_prefix)
- **What's wrong:** Each BIN stores its entries as a sorted `Vec<BinEntry>`.  Inserting a new key into a sorted position requires a `Vec::insert(idx, ...)` which is O(n) (shifts all entries after `idx` one position right via memmove).  For a BIN with 128 entries, each insert is ~127 element moves.  At 10x the current key count per BIN (e.g., a wider fanout or larger keys that reduce prefix compression benefit), this becomes 1270 element moves per insert.
- **Failure mode:** Write throughput degrades as BINs fill.  The memmove cost appears as `__memmove_avx_unaligned_erms` at 2.76 % in Wave 11-H W10 profile.  At 10x scale this scales linearly.
- **Suggested fix:** JE's `IN` backing store uses a packed byte array with a pre-allocated slot count, reducing insert cost to a range-copy within a fixed-size slab.  As a near-term fix, consider `VecDeque` or a hybrid skip-list for BINs with high churn.  Longer term, BIN-delta compression means most inserts are appended to a delta list, not inserted mid-sorted-array.

---

### F-7.3 — Deadlock detection DFS is O(V+E) on waiter graph; at 10x concurrent transactions this is O(10K)

- **Severity:** Medium
- **Subsystem:** noxu-txn deadlock detector
- **File:line:** `crates/noxu-txn/src/deadlock_detector.rs:31–85`
- **What's wrong:** `DeadlockDetector::detect()` runs a DFS from each owner node in the cycle.  The `waiter_graph` contains one entry per blocked locker.  At 10x concurrent transactions (e.g., 1,000 concurrent writers), the `waiter_graph` can have up to 1,000 entries.  The DFS is O(V+E) where V = blocked lockers, E = edges in the waits-for graph.  Under pathological contention (a hot key that every writer wants), V = 999 and E = 999.  This runs inside the `check_deadlock_for_waiter` call, which acquires the `waiter_graph` mutex to clone the graph snapshot.  With 1,000 concurrent waiters, each calling `check_deadlock_for_waiter` on entry, the `waiter_graph` mutex becomes a serial bottleneck.
- **Failure mode:** Deadlock detection latency scales with the number of concurrent blocked transactions.  Under extreme contention on a hot key, deadlock detection takes O(N²) total work across all waiters.
- **Suggested fix:** Cap the DFS search depth (JE uses a depth limit).  Alternatively, run deadlock detection asynchronously from a single dedicated detector thread (as JE does with `DeadlockChecker.run()`), rather than inline on the waiter path.

---

### F-7.4 — `Arc<RwLock<IN>>` per-INode model: reference count overhead at 10x INodes

- **Severity:** Low / informational
- **Subsystem:** noxu-tree
- **File:line:** `crates/noxu-tree/src/tree.rs` (general)
- **What's wrong:** Every internal node and BIN in the tree is wrapped in `Arc<RwLock<TreeNode>>`.  At 10x the current dataset (e.g., 100M records, ~800K BINs), there are ~800K Arcs in the tree.  Each Arc is 16 bytes of header plus the RwLock word plus the payload.  800K × 40 bytes = ~32 MB just in Arc+RwLock headers.  More critically, every tree traversal clones 3–5 Arcs (root → IN → BIN), each requiring an atomic increment.  Under concurrent read load, these atomic increments contend on the same cache lines as the Arc reference counts.
- **Failure mode:** Memory overhead scales with dataset; at 1B records the Arc headers alone consume ~400 MB.  Cache-line contention on Arc reference counts is visible in high-concurrency profiles.
- **Suggested fix:** For the hot read path, use an "epoch-based" or "hazard pointer" read scheme to avoid Arc clones.  For the write path, the BIN latch already provides mutual exclusion; the Arc is only needed for shared ownership across the evictor and the tree.  This is a significant structural change; document for a future architectural wave.

---

## Section 8 — The fsync-and-Friends Audit

### F-8.1 — No directory fsync after file creation (Critical — see F-3.1 above)

Covered in F-3.1.

---

### F-8.2 — `sync_all()` vs `sync_data()` misuse in `create_file_internal` vs `sync_log_end`

- **Severity:** Medium (covered partially in F-3.4)
- **File:line:** `crates/noxu-log/src/file_manager.rs:437–438` vs `663–666`
- **What's wrong:** `create_file_internal` calls `file.flush()` then `file.sync_all()` (fsync with metadata).  `sync_log_end` calls `guard.sync_data()` (fdatasync without metadata).  The inconsistency is intentional: the header write needs metadata sync; data writes use fdatasync.  This is correct.  What is missing is the `fcntl(O_CLOEXEC)` flag — new log files are opened with `OpenOptions::new().read(true).write(true).create_new(true)` which does not set `O_CLOEXEC` on Unix.  If the process forks (unlikely but possible in some embedding scenarios), the file descriptor is inherited.
- **Failure mode:** Inherited file descriptors in child processes can cause double-writes or delayed file deletion.
- **Suggested fix:** Add `.custom_flags(libc::O_CLOEXEC)` to the `OpenOptions` when opening log files on Unix.

---

### F-8.3 — `fsync` error from `FsyncManager` is propagated but does not invalidate the environment (Critical — see F-3.2)

Covered in F-3.2.

---

### F-8.4 — `fsync` error from `file_handle.sync_all()` in `create_file_internal` not mapped to env failure

- **Severity:** High
- **File:line:** `crates/noxu-log/src/file_manager.rs:437–438`
- **What's wrong:** If `file.sync_all()` fails inside `create_file_internal`, the `Result<Err>` propagates up through `create_file`, `flip_file`, `write_buffer`, `log_internal`, and eventually returns from `lm.log()` as a `NoxuLogError::Io`.  The caller in `commit_with_durability` maps this to `NoxuError::EnvironmentFailure { reason: LogWrite }`.  The error is correctly returned to the user.  **But**, as noted in F-3.2, `env_impl.invalidate()` is never called, so the environment remains "valid" for subsequent operations.
- **Failure mode:** Same as F-3.2 — subsequent operations on a file system that failed to fsync continue writing, leading to silent data loss.
- **Suggested fix:** Same as F-3.2.

---

## Section 9 — The "What About a Bad Disk?" Audit

### F-9.1 — CRC mismatch during recovery does not halt or mark env invalid (Critical — see F-3.5)

Covered in F-3.5.  The `FileManagerLogScanner` (used by recovery) skips CRC validation entirely.

---

### F-9.2 — Short read detection in `parse_entry_from_bytes`: returns `None`, not `Err`

- **Severity:** Medium
- **Subsystem:** noxu-dbi file manager scanner
- **File:line:** `crates/noxu-dbi/src/file_manager_scanner.rs:325–334`
- **What's wrong:** When `parse_entry_from_bytes` detects that the remaining bytes are insufficient for the entry (truncated write), it returns `None`.  The caller treats `None` as end-of-log.  This is correct for the last entry in the log (which may be partially written before a crash).  However, if a file is genuinely corrupted mid-file (not just at end-of-log), a short/corrupted entry returns `None` and the scanner stops scanning — silently dropping all entries after the corrupted one.  There is no way to distinguish "we've hit the valid end of log" from "we've hit a mid-file corruption."
- **Failure mode:** A single corrupted byte in the middle of a log file causes recovery to silently truncate all log entries after that point, losing commits that were already written and fsynced.
- **Suggested fix:** Return `Err(ParseError::Truncated { offset, expected, available })` rather than `None` for mid-file truncation.  Callers can decide whether to treat it as end-of-log (normal) or as a corruption requiring operator intervention (abnormal for entries not at end-of-log).  JE's `FileReader.readNextEntry()` distinguishes `LogFileCorruptedException` from normal end-of-log.

---

### F-9.3 — `ENOSPC` from `write_buffer` returns `Err` but does not invalidate or flush partial state

- **Severity:** High
- **Subsystem:** noxu-log write path
- **File:line:** `crates/noxu-log/src/file_manager.rs:504–527`
- **What's wrong:** If `write_at()` returns `ENOSPC` (disk full), `write_buffer` returns `Err(LogError::Io(e))`.  This propagates up to `log_internal`, which returns `Err`.  The caller's `lm.log()` fails.  The LSN for this entry was already assigned and `set_last_position` was already called with the new LSN.  The write buffer pool may have registered the LSN as belonging to a buffer segment that was not actually written.  Subsequent writes will continue assigning LSNs from the (now incorrect) `next_available_lsn`.  On a subsequent flush attempt, `collect_dirty_buffers` will attempt to pwrite this uncommitted data to the disk — which may fail again if still out of space.
- **Failure mode:** Disk full mid-write leaves the LSN bookkeeping in an inconsistent state.  The WAL may have a gap at the position of the failed write.  Recovery will likely scan through a zero-filled hole and stop, treating all transactions after the gap as uncommitted.
- **Suggested fix:** On `ENOSPC` from `write_buffer`, immediately invalidate the environment (`EnvironmentFailureReason::DiskLimit`), stop all further writes, and flush what can be flushed.  JE calls `environmentImpl.invalidate(IOException)` on any unhandled disk exception.

---

### F-9.4 — `EIO` from `guard.sync_data()` in `sync_log_end` propagated but not escalated to env invalidation

- **Severity:** Critical (see F-3.2 and F-8.4)
Covered in F-3.2 and F-8.4.

---

### F-9.5 — `file_reader.rs` correctly handles short reads; `file_manager_scanner.rs` does not (asymmetric)

- **Severity:** High
- **Subsystem:** noxu-log / noxu-dbi scanner
- **File:line:** `crates/noxu-log/src/file_reader.rs:397–500` vs `crates/noxu-dbi/src/file_manager_scanner.rs:290–354`
- **What's wrong:** `FileReader` (used by checkpoint reader, IN reader, cleaner reader) detects short reads and CRC mismatches and returns typed errors (`NoxuLogError::UnexpectedEof`, `NoxuLogError::Checksum`).  `FileManagerLogScanner::parse_entry_from_bytes` (used by the main recovery path) silently returns `None` for any parse failure, including CRC mismatches.  There are two independent implementations of log parsing with different safety properties, and the **less safe one** is used on the critical recovery path.
- **Failure mode:** See F-3.5, F-9.2.
- **Suggested fix:** Unify behind `FileReader` or add a `parse_entry_validated(bytes, offset) -> Result<(size, Option<LogEntry>), ParseError>` that includes CRC validation, used by all callers.

---

## Section 10 — The Benchmark Gap

Wave 11-H profiled W03 (sequential read), W04 (random read), W10 (concurrent 4r4w/8r8w), and W11 (recovery).  The following standard workloads are not benchmarked:

| Workload | Gap | Impact |
|---|---|---|
| **YCSB-A** (50% read, 50% update) | Not benchmarked | Primary workload for most databases; closest to production use |
| **YCSB-B** (95% read, 5% update) | Not benchmarked | Important for read-heavy microservices |
| **YCSB-C** (100% read) | Partially covered by W04 (random) and W03 (sequential) | Not identical: YCSB-C uses Zipfian key distribution |
| **YCSB-E** (range scans) | Not benchmarked | Exercises the `GetNext`/`SetRange` path; F-4.1 (linear scan) hits hardest here |
| **YCSB-F** (read-modify-write) | Not benchmarked | Exercises cursor.current() put path |
| **TPC-C analog** (order entry + inventory + payment) | Not benchmarked | Multi-table transactional workload; exercises lock contention and deadlock detection |
| **Large-value workload** (1 KB–1 MB values) | Not benchmarked | Exercises the large-entry `write_buffer` path (temp buffer writes, F-1.1 amortisation) |
| **Deeply nested key workload** | Not benchmarked | Exercises prefix compression depth; binary-search benefit depends on key structure |
| **Secondary index concurrent update** | Not benchmarked | Exercises `noxu-db::secondary_database` + join cursors under concurrency |
| **Restart/recovery throughput** | Covered by W11 for clean close only | **Crash recovery** (dirty shutdown, truncated log) not benchmarked |

The most notable gap is **crash recovery** benchmarking.  W11 measures re-open after a clean close (which writes a checkpoint before shutdown).  A dirty recovery (crash with no checkpoint, full log scan, CRC validation per entry) is both the most common production scenario (process killed by OOM, kernel panic) and the scenario most affected by F-3.5 (no CRC in scanner) and the allocations identified in Wave 11-K.

---

## Summary Table

| Finding | Severity | Subsystem | File:line | Key Failure Mode |
|---|---|---|---|---|
| F-3.1 No directory fsync after new log file | **CRITICAL** | noxu-log | file_manager.rs:416 | Crash silently loses all data in new file |
| F-3.2 fsync failure does not invalidate env | **CRITICAL** | noxu-db | transaction.rs:318 | Silent data loss after EIO from fdatasync |
| F-3.5 No CRC validation in recovery scanner | **CRITICAL** | noxu-dbi | file_manager_scanner.rs:290 | Bit flip produces silent data corruption post-recovery |
| F-2.2 EnvironmentImpl lock held across all undo | **HIGH** | noxu-db | transaction.rs:529 | All threads stall during large-transaction abort |
| F-3.3 Committed before state=Committed; starvation window | **HIGH** | noxu-db | transaction.rs:307 | Reader starvation under cleaner throttle |
| F-3.6 Partial header at end-of-log not detected without CRC | **HIGH** | noxu-dbi | file_manager_scanner.rs:325 | Crash mid-commit record → spurious commit visible to recovery |
| F-4.1 Linear scan in find_bin_for_key on write path | **HIGH** | noxu-dbi | cursor_impl.rs:1906 | 16% CPU on write path; grows with BIN fill factor |
| F-5.1 tree.insert Err silently dropped in abort loop | **HIGH** | noxu-db | transaction.rs:539 | Aborted data visible to readers after BTree error |
| F-6.2 Lock ordering inversion: waiter_graph + shard mutex | **HIGH** | noxu-txn | lock_manager.rs:339 | Internal lock manager deadlock under extreme contention |
| F-8.4 fsync error in create_file not mapped to env failure | **HIGH** | noxu-log | file_manager.rs:437 | Same as F-3.2 on file creation path |
| F-9.3 ENOSPC leaves LSN bookkeeping inconsistent | **HIGH** | noxu-log | file_manager.rs:504 | Disk-full silently corrupts WAL |
| F-9.5 Two log parsers with different safety properties | **HIGH** | noxu-dbi | file_manager_scanner.rs | Recovery uses unsafe parser; normal reading uses safe one |
| F-1.1 vec![0u8; entry_size] per log write | **HIGH** | noxu-log | log_manager.rs:261 | Allocation churn 3–5% CPU on all write paths |
| F-1.2 Vec<(Vec<u8>, u64)> in collect_dirty_buffers | **HIGH** | noxu-log | log_manager.rs:542 | Allocation + memcpy under LWL on every commit |
| F-2.3 path.metadata() stat syscall per write | **MEDIUM** | noxu-log | file_manager.rs:530 | Double syscalls on write hot path |
| F-3.4 sync_data vs sync_all on first write to new file | **MEDIUM** | noxu-log | file_manager.rs:663 | Partial durability on first write after file flip |
| F-3.7 Three non-atomic stores in set_last_position | **MEDIUM** | noxu-log | file_manager.rs:277 | Torn state readable by cleaner; premature file deletion |
| F-4.2 thread_id() clones Arc<Thread> on every lock | **MEDIUM** | noxu-sync | raw_mutex.rs:30 | 1.4% CPU overhead per lock on all data paths |
| F-4.4 Deadlock victim selection passes empty lock_counts | **MEDIUM** | noxu-txn | lock_manager.rs:986 | Wrong victim chosen; large txns disproportionately aborted |
| F-4.5 list_file_numbers O(N) directory scan per call | **MEDIUM** | noxu-log | file_manager.rs:222 | O(N) at startup; O(N log N) at 10x file count |
| F-5.2 Commit Err but durable; caller semantics unclear | **MEDIUM** | noxu-db | transaction.rs:407 | Application may double-apply or silently lose committed data |
| F-6.1 std::sync::Mutex::lock().unwrap() poisoning risk | **MEDIUM** | noxu-db | transaction.rs:410 | Panic in one thread cascades to all transaction users |
| F-7.1 Thread-ID hash collision: ThreadLocker bypass at 10x | **MEDIUM** | noxu-txn | thread_locker.rs:216 | Write-write conflict undetected → last-writer-wins corruption |
| F-7.2 Vec<BinEntry> O(n) insert at high fill factor | **MEDIUM** | noxu-tree | bin.rs | Write throughput degrades as BINs fill; 2.76% memmove in W10 |
| F-7.3 Deadlock DFS O(V+E) at 10x concurrent transactions | **MEDIUM** | noxu-txn | deadlock_detector.rs | Deadlock detection latency scales with blocked-txn count |
| F-9.2 Short read returns None not Err; silently truncates log | **MEDIUM** | noxu-dbi | file_manager_scanner.rs:325 | Mid-file corruption truncates recovery silently |
| F-1.3 BytesMut + TxnEndEntry alloc per commit | **MEDIUM** | noxu-db | transaction.rs:867 | Two heap allocations per commit |
| F-1.4 to_vec/clone on cursor put path | **MEDIUM** | noxu-dbi | cursor_impl.rs:1989 | Allocation per write operation (wave 11-H partial fix) |
| F-6.3 inner_txn locked twice per abort | **LOW** | noxu-db | transaction.rs:506 | Minor: two lock acquisitions per abort |
| F-6.4 FSyncGroup::take_error clones String under mutex | **LOW** | noxu-log | fsync_manager.rs:113 | Minor: String allocation under mutex on fsync failure |
| F-7.4 Arc<RwLock<IN>> overhead at 10x INodes | **LOW** | noxu-tree | tree.rs | Memory + cache-line contention scales with dataset |
| F-1.5 xid_gtrid/bqual to_vec in XA prepare | **LOW** | noxu-db | transaction.rs:819 | Two allocs per XA prepare (low frequency) |
| F-1.6 collect_prepared_txn_lns allocs per prepared LN | **LOW** | noxu-recovery | recovery_manager.rs:1025 | Recovery allocs for prepared txns |
| F-4.3 HashMap default hasher for lock tables | **LOW** | noxu-txn | lock_manager.rs:57 | Marginal per-lock overhead |
| F-5.3 Prepared txns hold cleaner pins indefinitely | **LOW** | noxu-db | transaction.rs:1048 | Log files accumulate on XA failure |
| F-8.2 O_CLOEXEC not set on log file opens | **LOW** | noxu-log | file_manager.rs:420 | FD leak to forked children in embedded scenarios |
| Section 10: Benchmark gaps | **INFORMATIONAL** | all | — | Missing YCSB-A/E/F, TPC-C analog, crash recovery benchmarks |

---

## Severity Counts

| Severity | Count |
|---|---|
| Critical | 3 |
| High | 12 |
| Medium | 14 |
| Low | 8 |
| Informational | 1 |
| **Total** | **38** |

---

## Top 5 Critical/High Findings

1. **F-3.1 (Critical)** — No directory fsync after new log file creation (`file_manager.rs:416`).  A crash after the first log file flip silently loses all data written to that file.  Standard POSIX crash-safety 101.  Fix is three lines.

2. **F-3.2 / F-8.4 (Critical)** — `fsync` failure does not invalidate the environment.  After EIO from fdatasync, the database continues accepting writes on what may be a permanently unflushable page-cache.  This is the "fsyncgate" class of vulnerability.  JE invalidates the environment on every unhandled I/O exception.

3. **F-3.5 (Critical)** — `FileManagerLogScanner::parse_entry_from_bytes` skips CRC32 validation.  The primary recovery code path has no checksum checking.  A single bad sector on the disk silently injects garbage into the recovered B-tree.

4. **F-2.2 (High)** — EnvironmentImpl lock held across the entire abort undo loop.  Every large-transaction abort blocks all other threads for the duration of the B-tree undo application.  O(N_writes_in_txn) latency spike visible to all concurrent users.

5. **F-6.2 (High)** — Lock ordering inversion between `waiter_graph` mutex and shard mutexes in the lock manager.  Can produce an internal deadlock in the lock manager itself under extreme concurrent contention — a process hang with no recovery path.

---

## Top 3 Performance Pathologies

1. **F-1.1 + F-1.2** — Per-log-entry `vec![0u8; entry_size]` allocation plus `Vec<(Vec<u8>, u64)>` buffer clone in `collect_dirty_buffers`.  These two together account for the majority of allocator pressure visible in all four Wave 11-H profiles.  The fix is a scratch-buffer pattern (one `Vec<u8>` per thread, reused).

2. **F-4.1** — `find_bin_for_key` linear scan on the write path.  The Wave 11-I fix addressed the read `Search` mode but the `put`, `SetRange`, and `delete` paths still use the O(n) descent.  This is the same `__memcmp_avx2_movbe` cost measured at 15–16% CPU in W03/W04/W10 but now on the write path.

3. **F-2.2** — EnvironmentImpl lock held across undo loop.  Under write-heavy workloads with transaction aborts (deadlock victims, application-layer retries), this serialises all users behind each aborting transaction's undo work.  Not visible in the Wave 11-H profiles (which were clean commit workloads) but critical for real applications.

---

## Top 3 Crash-Safety Concerns

1. **F-3.1** — No directory fsync.  Every log file flip is a window where a crash loses all data written to the new file.  This is the single highest-priority fix.

2. **F-3.5** — No CRC in recovery scanner.  The entire recovery code path can be silently misled by bit-flip corruption.  Recovery is the last line of defence; it must validate every byte.

3. **F-3.2** — fsync failure does not invalidate env.  After EIO from fdatasync, the database is in an unknown durability state but continues operating.  All commits after the first EIO may be silently lost.  The fix is to call `env_impl.invalidate(LogWrite)` on any I/O error that affects durability.

---

*Report written to: `/tmp/noxu-audit-keith.md`*
