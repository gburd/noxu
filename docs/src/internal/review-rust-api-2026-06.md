# Noxu DB — Rust Correctness & API Review
**Reviewer**: jonhoo-style audit (Greg B / pi agent)  
**Date**: 2026-06-03  
**Source**: `/tmp/noxu-review` (origin/main, v3.2.0, commit `34171f6`)  
**Scope**: `noxu-sync`, `noxu-latch`, `noxu-log` (all `unsafe`), `noxu-xa`, `noxu-db` public API, `noxu-persist-derive`, concurrency primitives workspace-wide.  
**Method**: grep-first audit of every `unsafe` block; cross-check against AGENTS.md inventory; concurrency, API-soundness, and idiom review.

---

## Summary Table

| ID | Severity | Crate / File | Issue |
|----|----------|--------------|-------|
| F-01 | **Critical** | `noxu-log/log_buffer.rs:279-281` | `LogBufferSegment` stores raw pointers to inline `LogBuffer` fields; moves the buffer UB |
| F-02 | **Critical** | `noxu-log/log_source.rs:59-63` | `transmute` lifetime extension — SAFETY comment omits the load-bearing drop-order invariant |
| F-03 | **Critical** | `noxu-log/file_manager.rs:619` + `noxu-dbi/file_manager_scanner.rs:70` | `mmap_file` SAFETY claim is false: mapped during live writes, violating `memmap2` UB contract |
| F-04 | **High** | `noxu-xa/environment.rs:188-189` | `get_transaction` returns `&Transaction` after dropping Mutex guard; use-after-free if XA protocol violated |
| F-05 | **High** | `noxu-latch/exclusive.rs:147-153`, `shared.rs:226-232` | `thread_id()` missing `\| 1`; if `DefaultHasher` returns 0, every latch acquisition false-panics with "already held" |
| F-06 | **Medium** | `noxu-log/log_buffer.rs:239-244` | `release()` callable by non-owner thread; `latch_held` AtomicBool is not ownership-enforced |
| F-07 | **Medium** | `noxu-log/log_buffer.rs:106-115` | `reinit()` clobbers `write_pin_count` without asserting / waiting for zero |
| F-08 | **Medium** | `noxu-sync/raw_rwlock.rs` | Non-fair RwLock: writers can starve permanently under sustained reader load; undocumented in public type |
| F-09 | **Medium** | `noxu-sync/condvar.rs:60-75` | `Condvar` missing doc/check for "same-mutex" requirement; using with a different mutex is silent UB |
| F-10 | **Medium** | `noxu-log/file_manager.rs:534-536` | `write_buffer()` file-size check races: metadata read outside `file_latch` → double-flip possible |
| F-11 | **Medium** | `noxu-recovery/tests/prop_tests.rs:352-395` | Stale test comment describes an active bug that was already fixed; assertion now passes, comment says it fails |
| F-12 | **Low** | `noxu-sync/futex.rs:48` | `expected as i32` silently sign-wraps if bit 31 ever set in futex word |
| F-13 | **Low** | `noxu-log/log_buffer.rs:216` | `contains_lsn`: unchecked `u32` add wraps silently in release builds |
| F-14 | **Low** | `noxu-log/log_buffer.rs:324` | `get_bytes`: unchecked `u32` subtraction panics in debug / wrong index in release on pre-first-LSN input |
| F-15 | **Low** | `AGENTS.md` | Unsafe inventory for `noxu-log` claims "6 blocks"; actual count is 8 items |
| F-16 | **Low** | `noxu-db/tests/je_db_cursor_test.rs:448,492` | Stale `TODO(bug)` doc-comments describe bugs as active; "Fixed in Wave 11-N" text present but `TODO(bug)` heading not removed |

**Counts**: Critical 3 · High 2 · Medium 5 · Low 4  
**Total findings**: 16

---

## Critical

### F-01 — `LogBufferSegment` raw pointers into inline struct fields; stack-move → dangling

**File**: `noxu-log/src/log_buffer.rs:279-281`  
**Evidence**:
```rust
// allocate() — &mut self, but returns a value the caller can Send across threads
Some(LogBufferSegment {
    data_ptr,
    pin_count: &self.write_pin_count as *const AtomicI32,  // ← into struct field
    latch:     &self.read_latch    as *const RawMutex,      // ← into struct field
    latch_held:&self.latch_held    as *const AtomicBool,    // ← into struct field
    size,
})
```
```rust
// line 351
unsafe impl Send for LogBufferSegment {}
```

**Issue**: `pin_count`, `latch`, and `latch_held` point to fields *inside* the `LogBuffer` value.  `LogBuffer` is a plain `struct` — no `Pin`, no `PhantomPinned`, no `!Unpin`.  `LogBuffer::new()` and `LogBuffer::wrap()` are public; callers can and do create stack-local `LogBuffer` values (all tests do this).  If the `LogBuffer` is moved — or if `LogBufferSegment` is sent to another thread and the original thread drops or moves the buffer — all three pointers dangle.

`data_ptr` points into `BytesMut`'s heap allocation and survives a move safely.  The three struct-field pointers do not.

**SAFETY comment** (line 348-350) says "raw pointers point into a LogBuffer that is kept alive by the caller (typically wrapped in Arc<Mutex<LogBuffer>>)."  The word "typically" is the gap: nothing in the type system enforces heap allocation.

**Concrete UB path**:
```rust
let mut buf = LogBuffer::new(1024);
buf.latch_for_write();
let seg = buf.allocate(64).unwrap();
buf.release();
let buf2 = buf;            // MOVE — seg.pin_count etc. now dangle
std::thread::spawn(move || seg.put(&[0u8; 64])).join().unwrap(); // UB
```

**Suggested fix**: Either (a) make `LogBuffer` `!Unpin` via `PhantomPinned`, require callers to work through `Pin<Box<LogBuffer>>`, and document the invariant; or (b) replace the three struct-field raw pointers with `Arc`-wrapped atomics extracted from `LogBuffer` before `allocate()` is called.  Option (b) matches how the pool already works (`Arc<Mutex<LogBuffer>>`).

---

### F-02 — `FileLogSource::new` transmute: SAFETY comment omits load-bearing drop-order invariant

**File**: `noxu-log/src/log_source.rs:59-63`  
**Evidence**:
```rust
// SAFETY: We extend the lifetime of the guard to 'static because we keep
// the Arc<FileHandle> alive for as long as the guard exists.
let guard = unsafe {
    let guard = handle.acquire()?;
    std::mem::transmute::<FileHandleGuard<'_>, FileHandleGuard<'static>>(guard)
};

Ok(FileLogSource {
    guard: Some(guard),   // ← field 1
    _handle: handle,      // ← field 2
    ...
})
```

**Issue**: The SAFETY comment is incomplete and therefore misleading to future maintainers.

The transmute is *only sound* because of a second invariant the comment does not mention: **Rust drops struct fields in declaration order**, so `guard` (field 1) is dropped before `_handle` (field 2), releasing the borrow before the `Arc` refcount falls.

What the comment says — "we keep the Arc alive for as long as the guard exists" — is true only by accident of field ordering.  If someone:
- reorders `_handle` before `guard`, or
- adds a custom `Drop` that explicitly drops `_handle` first, or
- adds any field between them that itself has a `Drop` impl that reaches `_handle`,

…the `FileHandleGuard<'static>` would outlive the `FileHandle`, producing a dangling reference.

**Suggested fix**: Add the drop-order reasoning to the SAFETY comment and add a compile-time assertion or `static_assertions::assert_fields_order!` check.  Also consider `ManuallyDrop<FileHandleGuard<'static>>` + explicit ordering in a custom `Drop` implementation so the invariant is self-documenting in code, not just a comment.

---

### F-03 — `mmap_file` SAFETY invariant violated at runtime by disk-ordered cursor

**Files**: `noxu-log/src/file_manager.rs:619`, `noxu-dbi/src/file_manager_scanner.rs:70`  
**Evidence**:
```rust
// file_manager.rs — SAFETY comment
// SAFETY: We only mmap files that are complete (not the current
// active write file) during recovery, so the underlying bytes do
// not change while the mapping is alive.
let mmap = unsafe { Mmap::map(&file) }...
```
```rust
// file_manager_scanner.rs:66-71 — called during live disk-ordered cursor
fn load_file_bytes(&self, file_num: u32, file_len: u64) -> Option<Bytes> {
    if let Ok(mmap) = self.file_manager.mmap_file(file_num) {
        return Some(Bytes::from_owner(mmap));
    }
    ...
}
```
```rust
// disk_ordered_cursor_impl.rs:336 — scanner used during normal operation
let scanner = FileManagerLogScanner::new(fm);
```

**Issue**: The SAFETY comment in `mmap_file` asserts the mapping is safe because the file is not being written concurrently.  This is true during recovery.  It is **false** during live operation.

`FileManagerLogScanner` is instantiated from the disk-ordered cursor producer (`disk_ordered_cursor_impl.rs:336`) — a live operation unrelated to recovery — and calls `load_file_bytes` on every file returned by `list_file_numbers()`, which includes the **current active write file**.  That file is concurrently written via `write_at` (`pwrite64`) on the log-manager thread while the disk-ordered cursor reads it through the `Mmap`.

The `memmap2` crate documentation explicitly states: *"Files must not be modified during the lifetime of the mapping."* Violating this is undefined behaviour in the Rust/C++ abstract machine (the underlying `*const u8` slice could alias a concurrent `&mut [u8]` write path).

**Suggested fix**: Either (a) exclude the current write file in `file_manager_scanner.rs` (pass `file_manager.get_current_file_num()` and skip it), relying on the in-memory buffer pool for in-flight entries; or (b) add a runtime gate in `mmap_file` that returns an error when called on the current write file, forcing the fallback `pread64` path.  Update the SAFETY comment to accurately reflect which files are safe to map.

---

## High

### F-04 — `XaEnvironment::get_transaction` returns `&Transaction` after releasing Mutex guard

**File**: `noxu-xa/src/environment.rs:173-189`  
**Evidence**:
```rust
pub fn get_transaction(&self, xid: &Xid) -> XaResult<&Transaction> {
    let branches = self.branches.lock().unwrap();     // Mutex acquired
    let branch = branches.get(xid).ok_or(XaError::NotFound)?;
    ...
    let txn_ptr: *const Transaction = &*branch.txn;
    Ok(unsafe { &*txn_ptr })                          // Mutex released on return
}   // ← `branches` guard dropped here
```

**Issue**: `branch.txn` is a `Box<Transaction>` inside a `HashMap` guarded by a `Mutex`.  The SAFETY comment acknowledges the mutex is dropped on return and justifies soundness by appealing to the XA protocol invariant "no other thread will remove this branch while the caller is using the returned reference."

This is **not enforced by the Rust type system**.  The borrow checker sees the returned `&Transaction` as having lifetime `'_` (bounded by `&self`), which permits the following:
- Thread A: calls `get_transaction(xid)` → holds `&Transaction`
- Thread B: calls `xa_rollback(xid)` → acquires `branches` lock, removes the `Branch`, drops `Box<Transaction>`
- Thread A: dereferences the now-dangling `&Transaction` → use-after-free

The X/Open XA protocol requires callers to serialize; the current implementation trusts callers in a way that unsoundly sidesteps the borrow checker.

**Suggested fix**: Return an RAII wrapper that holds the `MutexGuard` alive, e.g.:
```rust
pub struct TxnRef<'a> {
    _guard: MutexGuard<'a, HashMap<Xid, Branch>>,
    txn:    *const Transaction,
}
impl<'a> Deref for TxnRef<'a> { type Target = Transaction; ... }
```
This is zero-cost, prevents the UB, and removes the need for `unsafe`.

---

### F-05 — `thread_id()` in `noxu-latch` missing `| 1`; any thread whose hash is 0 always panics

**Files**: `noxu-latch/src/exclusive.rs:147-153`, `noxu-latch/src/shared.rs:226-232`  
**Evidence**:
```rust
// exclusive.rs — sentinel value for "unowned":
owner: AtomicU64::new(0),
// reentrancy check:
if self.owner.load(Ordering::Relaxed) == current { panic!(...) }

// thread_id() — can return 0:
fn thread_id() -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    thread::current().id().hash(&mut hasher);
    hasher.finish()       // ← no | 1
}
```
```rust
// raw_mutex.rs — correctly ensures non-zero:
hasher.finish() | 1
```

**Issue**: The sentinel "no owner" value is `0`.  The reentrancy check panics when `owner == current`.  If `DefaultHasher` returns `0` for a thread's `ThreadId` (which is a valid hash output), then `current = 0 = owner_initial_value`, and the check **fires on every first acquisition attempt** by that thread, even when no thread holds the latch.  That thread can never acquire any `ExclusiveLatch` or `SharedLatch`.

`raw_mutex.rs` already knows about this problem and correctly does `hasher.finish() | 1`.  The latch crate duplicates the function without the fix.  Hash collisions between two distinct non-zero thread IDs are a separate, lower-probability reliability concern; the zero case is the certain path.

**Suggested fix**:
```rust
fn thread_id() -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    thread::current().id().hash(&mut hasher);
    hasher.finish() | 1   // same fix as raw_mutex.rs
}
```
Apply to both `exclusive.rs` and `shared.rs`.

---

## Medium

### F-06 — `LogBuffer::release()` can be called by any thread; `latch_held` is not ownership-enforced

**File**: `noxu-log/src/log_buffer.rs:239-244`  
**Evidence**:
```rust
pub fn release(&self) {                                   // pub, takes &self
    if self.latch_held.swap(false, Ordering::Relaxed) {
        // SAFETY: We hold the lock (verified by latch_held flag).
        unsafe { self.read_latch.unlock(); }
    }
}
```

**Issue**: `release()` is `pub` and takes `&self`, meaning any thread with a reference to the buffer can call it.  The SAFETY comment says "we hold the lock (verified by `latch_held` flag)", but `latch_held` is a shared `AtomicBool` that any code path — including `LogBufferSegment::put()` on a different thread — can read and set.

If `put()` on thread B has set `latch_held = true` and thread A concurrently calls `release()`, thread A would see `latch_held = true`, swap it to false, and call `unlock()` without holding the underlying mutex.  This is unsound (`NoxuRawMutex::unlock` is `unsafe` and requires the caller to hold the lock).

In the current pool usage pattern this is prevented by the ring-buffer protocol (no two code paths overlap on the same buffer at the same time), but the public API offers no such guarantee.

**Suggested fix**: Make `release()` `pub(crate)` or restructure into an RAII guard (a `LogBufferWriteGuard` that calls `unlock` on drop and is the only path through which the latch is held).

---

### F-07 — `reinit()` resets `write_pin_count` to 0 without asserting / waiting

**File**: `noxu-log/src/log_buffer.rs:106-115`  
**Evidence**:
```rust
pub fn reinit(&mut self) {
    self.latch_for_write();
    self.data.clear();
    ...
    self.write_pin_count.store(0, Ordering::Relaxed);  // ← no assert_eq!(old, 0)
    ...
    self.release();
}
```

**Issue**: If `reinit()` is called while segments are still alive (pin count > 0), those segments hold raw pointers into `self.data: BytesMut`.  `data.clear()` sets the length to zero but does not reallocate; subsequent `data.resize(…)` calls in the next allocation cycle may overwrite the memory the live segment pointers reference.  This could produce silent data corruption or memory safety issues depending on `BytesMut` internals.

The buffer-pool ring protocol in `log_buffer_pool.rs` relies on the `dirty_start`/`dirty_end` invariant to ensure a buffer's pin count is zero before `reinit()` is called, but there is **no defensive runtime assertion** in `reinit()` itself.

**Suggested fix**: Add `debug_assert_eq!(self.write_pin_count.load(Ordering::Acquire), 0, "reinit called with live segments")` at the top of `reinit()`.

---

### F-08 — Non-fair `NoxuRawRwLock` writer starvation undocumented in public type

**File**: `noxu-sync/src/raw_rwlock.rs`, `noxu-sync/src/lib.rs:88`  
**Evidence**:
```rust
// raw_rwlock.rs module doc:
//! Non-fair design: new readers are not blocked by pending writers, which
//! maximises read throughput.
```
```rust
// lib.rs:88 (public RwLock doc):
/// Non-fair: new readers are not blocked by waiting writers.
```
`try_lock_shared_fast()` never checks `write_waiters`, so under sustained reader load a writer may block in `lock_exclusive_slow` indefinitely regardless of how long it has been waiting.

**Issue**: The one-line warning in the public `RwLock` doc is easy to miss.  More importantly:
1. The `WRITE_WAITING` bit (bit 31) is reserved in the state word specifically for implementing writer preference, but "not currently used."  Callers cannot enable fairness.
2. The `noxu-db` and `noxu-dbi` crates use `Arc<RwLock<IN>>` for B-tree internal nodes.  Checkpoint and cleaner threads hold write locks; if reader threads (cursors) run continuously, checkpointing can starve indefinitely.

**Suggested fix**: Implement the `WRITE_WAITING` bit: set it when a writer is waiting, check it in `try_lock_shared_fast()` (fast path), and document clearly that writer preference is active.  This is a standard futex-based mutex pattern.  Alternatively, accept the current semantics but add explicit documentation at the crate level.

---

### F-09 — `Condvar::wait` / `wait_for` missing "same-mutex" requirement; different-mutex use is silent UB

**File**: `noxu-sync/src/condvar.rs:60-75`  
**Evidence**:
```rust
pub fn wait<T>(&self, guard: &mut MutexGuard<'_, T>) {
    let seq = self.seq.load(Ordering::SeqCst);
    let mutex = lock_api::MutexGuard::mutex(guard);
    unsafe { mutex.force_unlock() };
    futex_wait(&self.seq, seq, None);
    unsafe { mutex.raw().lock() };
}
```

**Issue**: `std::sync::Condvar::wait` panics if the guard comes from a different mutex than was used on the previous call.  `parking_lot::Condvar::wait` documents this requirement.  This implementation silently re-acquires whatever mutex was passed in — if a caller accidentally uses two different mutexes across two `wait` calls on the same condvar, the sequence counter works correctly but **the second `MutexGuard` returned to the caller will be holding a lock it never released**, while the first mutex has been silently unlocked and re-locked without the caller's knowledge.  The net effect is that lock-ordering invariants can be violated without any diagnostic.

**Suggested fix**: Store the address of the first mutex used (`*const NoxuRawMutex`) in the `Condvar` and `debug_assert!` on every subsequent call that the same mutex is passed, matching the `parking_lot::Condvar` contract.

---

### F-10 — `write_buffer()` file-size check is outside `file_latch`; double-flip possible

**File**: `noxu-log/src/file_manager.rs:534-536`  
**Evidence**:
```rust
pub fn write_buffer(&self, data: &[u8], file_offset: u64) -> Result<u32> {
    ...
    // Check whether we need to flip to a new file.
    let path = self.file_path(file_num);
    let file_len = path.metadata().map(|m| m.len()).unwrap_or(0);  // ← outside file_latch
    if file_len >= self.max_file_size {
        self.flip_file()?;     // ← acquires file_latch inside
    }
    ...
}
```

**Issue**: `file_len` is read via `fs::metadata` outside any latch.  If two threads both see `file_len >= max_file_size` before either calls `flip_file()`, both threads will call `flip_file()`.  Inside `flip_file()`, each sees a different value of `current_file_num` (after the first flip, the second thread reads the updated value), so they create two successive files unnecessarily.  `create_file_internal` uses `OpenOptions::create_new(true)`, so the second creation of the *same* file would return an `AlreadyExists` I/O error — propagated as an unexpected `LogError::WriteFailed`.

In practice `write_buffer()` is called from `LogManager` under the LWL, so concurrent calls are prevented by protocol.  The method is nonetheless `pub`, making this a latent API footgun.

**Suggested fix**: Move the file-size check inside the `file_latch` acquisition in `flip_file()`, or read `next_available_lsn` (which is already maintained correctly) instead of going to the filesystem.

---

### F-11 — Stale test comment describes active bug; bug is fixed but comment was not cleaned up

**File**: `noxu-recovery/tests/prop_tests.rs:352-395`  
**Evidence**:
```rust
// Bug observation surfaced by Wave 11-E (committed `#[ignore]` per the
// wave's discipline; bug fixes are routed to a separate wave).
//
/// `record_active_txn` re-introduces a txn into `active_txn_ids` even after
/// `record_commit` … has been called for the same id. …
/// TODO: decide whether `record_active_txn` should be hardened …
#[test]                                     // ← no #[ignore]
fn prop_active_txn_after_terminal_resurrects_phantom_active() {
    ...
    assert!(
        !a.has_active_txns(),  // ← passes today because fix is in analysis_result.rs
        "phantom active txn after Commit then record_active_txn"
    );
}
```

**Issue**: The comment says the test was "committed `#[ignore]`" and describes the assertion as documenting a failure.  In fact:
1. There is no `#[ignore]` attribute on the test.
2. The fix (`defensive precondition` guard in `record_active_txn()`) was shipped in `analysis_result.rs:293-308`.
3. The test now passes.

The comment misleads anyone reading it into thinking this is a known-failing / skipped test or that the assertion is expected to fail.  A CI reader may spend time investigating a "bug" that does not exist.

**Suggested fix**: Replace the header comment with a regression-test comment: `// Regression for Wave 11-E: record_active_txn after commit/abort must not re-activate.`  Remove the `TODO` and the `#[ignore]` description.

---

## Low

### F-12 — `futex_wait`: `expected as i32` silently sign-wraps if bit 31 ever set

**File**: `noxu-sync/src/futex.rs:48`  
**Evidence**:
```rust
libc::syscall(SYS_futex, futex_word, FUTEX_WAIT_PRIVATE, expected as i32, ...)
//                                                         ^^^^^^^^^^^^^^^^
```

**Issue**: The Linux `futex(2)` `val` argument is `int` (signed 32-bit).  If `expected` has bit 31 set, the `as i32` cast wraps to a negative value.  The kernel compares `*uaddr` (read as unsigned) with `val` (cast back to unsigned internally), so the comparison itself is not wrong — but the cast is implicit and undocumented, and future state encodings that use bit 31 (e.g., if `WRITE_WAITING` in `NoxuRawRwLock` is ever activated) would silently produce wrong comparisons if `expected` is passed without first verifying bit 31 is clear.

**Suggested fix**: Use an explicit conversion with a comment: `(expected as i32)  // bit 31 must be 0; current state encodings guarantee this`.  Add a `debug_assert!(expected < 0x8000_0000)` at the call site.

---

### F-13 — `contains_lsn`: unchecked `u32` addition wraps in release builds

**File**: `noxu-log/src/log_buffer.rs:216`  
**Evidence**:
```rust
let last_content_offset = first_lsn_offset + content_size as u32;
```

**Issue**: Both operands are `u32`.  If `first_lsn_offset + content_size` overflows `u32::MAX` (possible if a log buffer somehow starts near the end of the 4 GiB file-offset space), the result wraps silently in release builds, making `last_content_offset < first_lsn_offset`, so `contains_lsn` returns `false` for all LSNs — silently breaking reads.

In practice `max_file_size` is far below `u32::MAX` and buffer capacity is bounded, but there is no assertion.

**Suggested fix**: Use `first_lsn_offset.checked_add(content_size as u32).unwrap_or(u32::MAX)` or assert at `reinit()` / `register_lsn()` that the buffer will not overflow the 32-bit file-offset space.

---

### F-14 — `get_bytes()`: unchecked `u32` subtraction; panics in debug, wrong index in release

**File**: `noxu-log/src/log_buffer.rs:322-325`  
**Evidence**:
```rust
pub fn get_bytes(&self, file_offset: u32) -> &[u8] {
    let buffer_offset =
        (file_offset - self.first_lsn.file_offset()) as usize;  // wraps if file_offset < first_lsn_offset
    &self.data[buffer_offset..]
}
```

**Issue**: If `file_offset < self.first_lsn.file_offset()`, the `u32` subtraction wraps to a very large `usize`, then `self.data[huge_index..]` either panics (in debug or release with bounds checking) or silently accesses beyond the end of `data` (in release with unsafe bounds bypassing, which does not happen here since `BytesMut::Index` panics — but the huge `usize` will always panic).

The `pub` method has a comment saying "The LWL or read_latch must be held" but no precondition documentation about the offset range.  Any future caller passing an unvalidated LSN file-offset can trigger a panic in a path that should be non-panicking.

**Suggested fix**: Add a precondition: `debug_assert!(file_offset >= self.first_lsn.file_offset())`, and document the valid range in the method's doc comment.

---

### F-15 — AGENTS.md unsafe inventory undercounts `noxu-log`

**File**: `AGENTS.md` (lines documenting `noxu-log | 6`)  
**Evidence**:
```
grep -c "^unsafe\|unsafe {" /tmp/noxu-review/crates/noxu-log/src/**/*.rs
→ 8 items
```

Actual blocks (confirmed by line search):
1. `file_manager.rs:619` — `Mmap::map`
2. `log_source.rs:59` — `transmute`
3. `log_buffer.rs:242` — `read_latch.unlock()` inside `release()`
4. `log_buffer.rs:276` — `as_mut_ptr().add()` inside `allocate()`
5. `log_buffer.rs:351` — `unsafe impl Send for LogBufferSegment`
6. `log_buffer.rs:364` — `(*self.latch).lock()` inside `put()`
7. `log_buffer.rs:371` — `copy_nonoverlapping` inside `put()`
8. `log_buffer.rs:382` — `(*self.latch_held).store / (*self.latch).unlock / (*self.pin_count).fetch_sub` inside `put()`

AGENTS.md says "6."  The actual count is 8 (or 7 `unsafe { }` blocks + 1 `unsafe impl`).  The three separate blocks in `put()` (lines 364, 371, 382) were not individually counted.

**Impact**: Future auditors reading AGENTS.md will not check the last two `put()` blocks.

**Suggested fix**: Update AGENTS.md to say "8" and list all blocks including the three in `put()`.

---

### F-16 — Stale `TODO(bug)` doc-comments in cursor tests; bugs fixed, headers not cleaned up

**File**: `noxu-db/tests/je_db_cursor_test.rs:448-463, 492-507`  
**Evidence**:
```
/// TODO(bug): on a multi-primary sorted-dup DB,
/// `Cursor::count()` over-counts …
/// Fixed in Wave 11-N (Bug 1): the count formula is now `forward + 1` …
#[test]
fn db_cursor_duplicate_test_duplicate_count() { ... }
```

**Issue**: Two test functions carry `TODO(bug)` headings that describe bugs.  The same doc-comment body then says "Fixed in Wave 11-N (Bug 1 / Bug 2)" — so the bugs are fixed and the tests pass.  The `TODO(bug)` prefix is misleading: it signals an open defect to anyone reading the test file, running `grep TODO`, or using a dashboard that surfaces open TODOs.

**Suggested fix**: Replace `TODO(bug)` with `Regression:` and remove the description of the incorrect behaviour; keep the "Fixed in Wave 11-N" reference so git-blame is traceable.

---

## Additional Observations (not findings per se)

**Condvar `wait()` panic safety**: `force_unlock()` is called before `futex_wait`, which means a panic inside the (currently non-panicking) `futex_wait` would leave the `MutexGuard` in a permanently-unlocked state while the guard's `Drop` will try to unlock again.  In practice `futex_wait` cannot panic; worth a `// SAFETY: futex_wait does not panic` comment.

**Non-Linux fallback `futex_wake` is a no-op**: Threads wake only via timeout (1 ms).  This is documented but means all `Condvar` and `Mutex` waiters burn a 1 ms spin on non-Linux targets.  If Noxu is ever ported to macOS/Windows, performance will be poor.  An `unpark`-based implementation would fix this.

**`DefaultHasher` hash collision (non-zero threads)**: If two distinct threads hash to the same non-zero value, the reentrancy check in `ExclusiveLatch` / `SharedLatch` could fire a false panic.  With 64-bit outputs from `DefaultHasher` the probability is negligible in practice, but the latch crate should document this as a known limitation.

**`write_buffer()` stat counter overflow**: `n_sequential_write_bytes.fetch_add(data.len() as u64, Relaxed)` — the `as u64` cast of `data.len()` (a `usize`) is fine on all 64-bit targets but silently truncates on 32-bit targets if `data.len() > u32::MAX`, which is impossible in practice.

---

## Findings Verified on `origin/main` (`/tmp/noxu-review`)

All 16 findings were confirmed present at the line numbers cited.  No finding from the stale branch (`fix/zb-stale-docs`) was dropped as already fixed on main.
