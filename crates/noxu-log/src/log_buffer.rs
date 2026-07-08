//! Log buffer for staging writes before flushing to disk.
//!
//! `LogBufferSegment`.
//!
//! LogBuffers hold outgoing, newly written log entries. Space is allocated
//! via the `allocate()` method that returns a `LogBufferSegment`. The
//! `write_pin_count` is incremented each time space is allocated. Once the
//! caller copies data into the log buffer, the pin count is decremented via
//! the `free()` method. Readers of a log buffer wait until the pin count is
//! zero.
//!
//! The pin count is incremented under the read_latch. The pin count is
//! decremented without holding the latch. Holding the read_latch will prevent
//! the pin count from being incremented.
//!
//! Apart from the pin count, access to the buffer is protected by the
//! read_latch and the LWL:
//! - Write access requires holding both the LWL and the read_latch.
//! - Read access requires holding either the LWL or the read_latch.

use bytes::BytesMut;
use noxu_sync::RawMutex;
use noxu_sync::futex::{futex_wait, futex_wake};
use noxu_sync::lock_api::RawMutex as RawMutexTrait;
use noxu_util::lsn::{Lsn, NULL_LSN};
use std::sync::Arc;
use std::sync::atomic::{
    AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering,
};
use std::time::Duration;

/// A write buffer backed by `BytesMut`.
///
///
///
/// Uses a RawMutex with manual lock/unlock to match explicit
/// latch_for_write/release pattern (not RAII).
pub struct LogBuffer {
    /// The actual buffer storage.
    ///
    /// **Round-2 change (lock-free slot reservation):** `data` is pre-sized to
    /// the full `capacity` ONCE at construction (and after `reinit`) and is
    /// never grown per-write.  The number of bytes actually written is tracked
    /// by the atomic `control.write_position`, NOT by `data.len()` (which is
    /// always `capacity`).  This lets `allocate()` reserve a slot with a single
    /// `fetch_add` — no `&mut self`, no buffer latch, no `Vec::resize` — so the
    /// reservation no longer needs the nested `Mutex<LogBuffer>` +
    /// `read_latch` acquisitions inside the LWL hot path.
    data: BytesMut,

    /// LSN of the first entry in this buffer (round-2: atomic so `register_lsn`
    /// can run on a shared `&LogBuffer` without the `Mutex<LogBuffer>` write
    /// lock).  Stored as the raw `Lsn::as_u64()`; `u64::MAX` (== `NULL_LSN`)
    /// means "empty".
    first_lsn: AtomicU64,

    /// LSN of the last entry registered in this buffer (round-2: atomic, see
    /// `first_lsn`).
    last_lsn: AtomicU64,

    /// Total capacity of the buffer.
    capacity: usize,

    /// Latch + pin-count control block, shared (via `Arc`) with every
    /// [`LogBufferSegment`] this buffer hands out. Holding it behind an `Arc`
    /// (rather than inline) means a segment keeps the control block alive
    /// independently of the `LogBuffer` value, so moving the `LogBuffer` (it
    /// is a plain non-`Pin` struct) does not dangle a segment's references
    /// (review R-F01).
    control: Arc<LogBufferControl>,

    /// Buffer may be rewritten because an IOException previously occurred.
    rewrite_allowed: bool,

    /// Number of bytes already written to disk from this buffer.
    ///
    /// `LogBuffer.lastFlushedPosition`: only `data[flushed_len..]`
    /// needs to be written on the next flush.  Advancing this watermark after
    /// each write prevents successive commits from rewriting previously
    /// persisted bytes (eliminating the O(N²) I/O pattern).
    flushed_len: usize,
}

/// Latch + pin-count control block for a [`LogBuffer`].
///
/// Stored behind an `Arc` so that a [`LogBufferSegment`] (which may be sent to
/// another thread to perform its write) shares ownership of these
/// synchronization primitives rather than holding raw pointers into the
/// `LogBuffer`'s inline fields. This makes segment access sound even if the
/// owning `LogBuffer` is moved.
struct LogBufferControl {
    /// Protects buffer modifications and read access when the LWL is not held.
    read_latch: RawMutex,
    /// Whether the latch is currently held.
    latch_held: AtomicBool,
    /// Number of writers currently pinning this buffer.
    ///
    /// Always `>= 0` (incremented in `allocate`, decremented in `free`/`put`);
    /// stored as `AtomicU32` so readers in `wait_for_zero_and_latch` can
    /// `futex_wait` directly on this word and be woken the instant a writer
    /// decrements it to zero — the unparkable analogue of JE's
    /// `LockSupport.parkNanos(this, 100)` (LogBuffer.java:326), replacing the
    /// un-unparkable `thread::park_timeout(100ns)` spin (Stage A read fix).
    write_pin_count: AtomicU32,
    /// High-water mark of bytes reserved in this buffer (round-2 change).
    ///
    /// This is the authoritative "content length" of the buffer — the sum of
    /// all `allocate(size)` reservations since the last `reinit`.  It replaces
    /// the old `data.len()` (which is now always `capacity`).
    ///
    /// Reservation happens under the LWL (all `allocate` callers hold it), so
    /// `fetch_add` never races another writer.  It is atomic so that the
    /// flush/read paths (which read it WITHOUT the LWL, only under the
    /// `read_latch` after `wait_for_zero_and_latch`) observe a consistent
    /// value with the correct happens-before ordering.
    write_position: AtomicUsize,
}

impl LogBufferControl {
    fn new() -> Self {
        LogBufferControl {
            read_latch: RawMutex::INIT,
            latch_held: AtomicBool::new(false),
            write_pin_count: AtomicU32::new(0),
            write_position: AtomicUsize::new(0),
        }
    }
}

impl LogBuffer {
    /// Creates a new LogBuffer with the specified capacity.
    ///
    /// The backing `data` is pre-sized to the full `capacity` (round-2 change)
    /// so `allocate()` never needs to grow it.  `write_position` starts at 0.
    pub fn new(capacity: usize) -> Self {
        let mut data = BytesMut::with_capacity(capacity);
        data.resize(capacity, 0);
        LogBuffer {
            data,
            first_lsn: AtomicU64::new(NULL_LSN.as_u64()),
            last_lsn: AtomicU64::new(NULL_LSN.as_u64()),
            capacity,
            control: Arc::new(LogBufferControl::new()),
            rewrite_allowed: false,
            flushed_len: 0,
        }
    }

    /// Creates a temporary LogBuffer wrapping existing data at a specific LSN.
    ///
    /// Used by LogManager when an entry is too large for the buffer pool.
    /// The wrapped `data` is treated as fully written, so `write_position` is
    /// initialised to its length.
    pub fn wrap(data: BytesMut, first_lsn: Lsn) -> Self {
        let capacity = data.capacity();
        let control = Arc::new(LogBufferControl::new());
        control.write_position.store(data.len(), Ordering::Relaxed);
        LogBuffer {
            data,
            first_lsn: AtomicU64::new(first_lsn.as_u64()),
            last_lsn: AtomicU64::new(first_lsn.as_u64()),
            capacity,
            control,
            rewrite_allowed: false,
            flushed_len: 0,
        }
    }

    /// Reinitializes the buffer for reuse.
    ///
    /// The LWL and buffer pool latch must be held.
    ///
    /// `data` stays pre-sized to `capacity`; only the `write_position`
    /// high-water mark is reset to 0 (round-2 change) so the buffer starts
    /// empty again without reallocating.
    pub fn reinit(&mut self) {
        self.latch_for_write();
        self.first_lsn.store(NULL_LSN.as_u64(), Ordering::Relaxed);
        self.last_lsn.store(NULL_LSN.as_u64(), Ordering::Relaxed);
        self.rewrite_allowed = false;
        self.control.write_pin_count.store(0, Ordering::Relaxed);
        self.control.write_position.store(0, Ordering::Relaxed);
        self.flushed_len = 0;
        self.release();
    }

    /// Returns the number of bytes written to this buffer so far.
    ///
    /// Round-2 change: this is the atomic `write_position` high-water mark, the
    /// authoritative content length (the old `data.len()`, which is now always
    /// `capacity`).  Callers must hold the LWL or the `read_latch` (the same
    /// rule as the fields it replaces); the `Relaxed` load is sufficient
    /// because that lock (or, for the flush/read path, the preceding
    /// `wait_for_zero_and_latch` Acquire on `write_pin_count`) already
    /// establishes the happens-before ordering with the writers.
    fn content_len(&self) -> usize {
        self.control.write_position.load(Ordering::Relaxed)
    }

    /// Returns the data that has not yet been flushed to disk.
    ///
    /// `LogBuffer` dirty-range tracking.  The caller should write
    /// this slice to disk at `flushed_file_offset()` and then call
    /// `mark_flushed()` to advance the watermark.
    pub fn get_unflushed_data(&self) -> &[u8] {
        &self.data[self.flushed_len..self.content_len()]
    }

    /// Returns the file offset at which `get_unflushed_data()` should be written.
    ///
    /// Equals `first_lsn.file_offset() + flushed_len`.
    pub fn flushed_file_offset(&self) -> u64 {
        self.first_lsn().file_offset() as u64 + self.flushed_len as u64
    }

    /// Advances the flush watermark to the current buffer length.
    ///
    /// Must be called after a successful `write_buffer()` for the unflushed
    /// slice so that the next flush only writes new data.
    pub fn mark_flushed(&mut self) {
        self.flushed_len = self.content_len();
    }

    /// Returns the first LSN held in this buffer.
    ///
    /// The LWL or read_latch must be held.
    pub fn get_first_lsn(&self) -> Lsn {
        self.first_lsn()
    }

    /// Internal accessor: current `first_lsn` from the atomic field.
    fn first_lsn(&self) -> Lsn {
        Lsn::from_u64(self.first_lsn.load(Ordering::Relaxed))
    }

    /// Registers the LSN for a buffer segment that has been allocated in this buffer.
    ///
    /// The LWL must be held (round-2 change: takes `&self` and the `read_latch`
    /// is no longer required).  `first_lsn`/`last_lsn` are atomic and are
    /// protected against other writers and against `bump_current` by the LWL
    /// (all of those run under it), and against `contains_lsn` readers by the
    /// pin-count protocol — a reader's `wait_for_zero_and_latch` blocks while
    /// this segment's pin is outstanding, which spans the whole `allocate` →
    /// `register_lsn` → `put` window.
    pub fn register_lsn(&self, lsn: Lsn) {
        let last = Lsn::from_u64(self.last_lsn.load(Ordering::Relaxed));
        if !last.is_null() {
            assert!(lsn > last, "lsn={:?} must be > last_lsn={:?}", lsn, last);
        }

        self.last_lsn.store(lsn.as_u64(), Ordering::Relaxed);

        // first_lsn is set once (on the first register in this buffer).  Only
        // one writer runs here at a time (LWL-serialised), so a plain
        // load-then-store is race-free.
        if Lsn::from_u64(self.first_lsn.load(Ordering::Relaxed)).is_null() {
            self.first_lsn.store(lsn.as_u64(), Ordering::Relaxed);
        }
    }

    /// Checks if this buffer has room for the specified number of bytes.
    ///
    /// The LWL or read_latch must be held.
    pub fn has_room(&self, num_bytes: usize) -> bool {
        num_bytes <= (self.capacity - self.content_len())
    }

    /// Returns the buffer's written data for read access.
    ///
    /// Round-2 change: bounded to `content_len()` (the written high-water
    /// mark) rather than the full pre-sized allocation, so callers still see
    /// exactly the bytes that were logged.
    ///
    /// The LWL or read_latch must be held.
    pub fn get_data(&self) -> &[u8] {
        &self.data[..self.content_len()]
    }

    /// Returns the capacity of this buffer in bytes.
    ///
    /// The LWL or read_latch must be held.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Checks if an LSN is contained in this buffer.
    ///
    /// This method must wait until the buffer's pin count goes to zero. When
    /// writing is active and this is the currentWriteBuffer, it may have to
    /// wait until the buffer is full.
    ///
    /// Returns true if this buffer holds the data at this LSN location. If true
    /// is returned, the buffer will be latched for read. Returns false if LSN
    /// is not here, and releases the read latch.
    pub fn contains_lsn(&self, lsn: Lsn) -> bool {
        assert!(!lsn.is_null());

        // Latch before we look at the LSNs. We need to have the count zero
        // for a reader to read the buffer.
        self.wait_for_zero_and_latch();

        let first = self.first_lsn();
        let found = if !first.is_null()
            && first.file_number() == lsn.file_number()
        {
            let file_offset = lsn.file_offset();
            let content_size = self.content_len();
            let first_lsn_offset = first.file_offset();
            let last_content_offset = first_lsn_offset + content_size as u32;

            first_lsn_offset <= file_offset && last_content_offset > file_offset
        } else {
            false
        };

        if !found {
            self.release();
        }
        found
    }

    /// Acquires the read_latch, providing exclusive access to the buffer.
    ///
    /// When modifying the buffer, both the LWL and buffer latch must be held.
    /// Call `release()` to release the latch.
    pub fn latch_for_write(&self) {
        self.control.read_latch.lock();
        self.control.latch_held.store(true, Ordering::Relaxed);
    }

    /// Releases the read_latch if held.
    pub fn release(&self) {
        if self.control.latch_held.swap(false, Ordering::Relaxed) {
            // SAFETY: We hold the lock (verified by latch_held flag).
            unsafe {
                self.control.read_latch.unlock();
            }
        }
    }

    /// Returns whether this buffer can be rewritten.
    pub fn get_rewrite_allowed(&self) -> bool {
        self.rewrite_allowed
    }

    /// Marks this buffer as allowing rewrites.
    pub fn set_rewrite_allowed(&mut self) {
        self.rewrite_allowed = true;
    }

    /// Allocates a segment out of the buffer via a lock-free atomic reservation.
    ///
    /// **Round-2 change:** takes `&self` (not `&mut self`) and reserves the
    /// slot with a single `write_position.fetch_add(size)` — NO `read_latch`,
    /// NO `Mutex<LogBuffer>` write-lock, NO `Vec::resize`.  This removes the
    /// nested buffer-lock acquisitions from the LWL hot path.
    ///
    /// The reservation is serialised by the LWL (every production caller holds
    /// it), so the `fetch_add` never races another writer; it is atomic purely
    /// so the flush/read paths observe the high-water mark with the correct
    /// ordering.
    ///
    /// Returns `None` if the reservation would overflow `capacity` (the buffer
    /// is full — the caller rolls to the next buffer or takes the oversized
    /// direct-write path).  On overflow the reservation is undone so the
    /// high-water mark is not corrupted.
    pub fn allocate(&self, size: usize) -> Option<LogBufferSegment> {
        // Reserve [off .. off+size) atomically.  Acquire/Release so the
        // reserving thread's view is ordered; a losing (overflow) reservation
        // is rolled back below.
        let off = self.control.write_position.fetch_add(size, Ordering::AcqRel);

        if off + size > self.capacity {
            // Buffer full: undo the reservation and report no room.  Because
            // allocate() runs under the LWL, this fetch_sub cannot race another
            // writer's fetch_add, so the position is restored exactly.
            self.control.write_position.fetch_sub(size, Ordering::AcqRel);
            return None;
        }

        self.control.write_pin_count.fetch_add(1, Ordering::Relaxed);
        // SAFETY: `off + size <= capacity` (checked above) and `data` is
        // pre-sized to `capacity`, so `[off .. off+size)` is within the
        // allocation.  The pin-count protocol keeps the buffer alive and
        // un-reused while this segment is outstanding.
        let data_ptr = unsafe { self.data.as_ptr().add(off) as *mut u8 };
        Some(LogBufferSegment {
            data_ptr,
            // Share the control block (latch + pin count) so the segment
            // is independent of the LogBuffer's location in memory.
            control: Arc::clone(&self.control),
            size,
        })
    }

    /// Decrements the pin count (called when a segment write completes).
    ///
    /// Called without holding the latch.
    pub fn free(&self) {
        // C-7 (2026 audit 4.4): use Release so that the writes
        // into the buffer segment (visible to this thread) are ordered
        // before the decrement.  The Acquire load in wait_for_zero_and_latch
        // then guarantees the reader sees the completed writes before it
        // re-uses the buffer.
        let prev = self.control.write_pin_count.fetch_sub(1, Ordering::Release);
        // Stage A: wake any reader parked in wait_for_zero_and_latch the
        // instant the pin count reaches zero (JE's parkNanos is unparkable;
        // this futex_wake is Noxu's faithful equivalent).  The wake targets
        // the same word the reader futex_waits on, so the kernel serialises
        // the change-then-wake against a concurrent register-then-wait — no
        // missed wakeup: a reader whose load raced this decrement sees the
        // new (< expected) value and futex_wait returns immediately.
        if prev == 1 {
            futex_wake(&self.control.write_pin_count, i32::MAX as u32);
        }
    }

    /// Acquires the buffer latched and with the buffer pin count equal to zero.
    pub fn wait_for_zero_and_latch(&self) {
        loop {
            // C-7: Acquire pairs with the Release in free() / LogBufferSegment::put()
            // to ensure we see all completed segment writes before re-using the buffer.
            let pins = self.control.write_pin_count.load(Ordering::Acquire);
            if pins > 0 {
                // Stage A: instead of the old un-unparkable
                // `thread::park_timeout(100ns)` spin, wait ON the pin-count
                // word.  futex_wait returns early (EAGAIN) if the count already
                // changed away from `pins`, and is woken by free()/put()'s
                // futex_wake when the count hits zero.  The 100ns timeout is
                // kept as a backstop so a lost wake (e.g. on the non-Linux
                // fallback path) still makes progress — JE keeps the same 100ns
                // parkNanos bound.
                futex_wait(
                    &self.control.write_pin_count,
                    pins,
                    Some(Duration::from_nanos(100)),
                );
            } else {
                self.latch_for_write();
                if self.control.write_pin_count.load(Ordering::Acquire) == 0 {
                    return;
                } else {
                    self.release();
                }
            }
        }
    }

    /// Returns a slice of the buffer positioned at the given file offset.
    ///
    /// Round-2 change: bounded to `content_len()` so it returns only written
    /// bytes (the read path parses the entry size from the header within this
    /// slice; `contains_lsn` has already verified the offset lies inside the
    /// written region).
    ///
    /// The LWL or read_latch must be held.
    pub fn get_bytes(&self, file_offset: u32) -> &[u8] {
        let buffer_offset =
            (file_offset - self.first_lsn().file_offset()) as usize;
        &self.data[buffer_offset..self.content_len()]
    }
}

/// A segment allocated within a LogBuffer for writing.
///
///
///
/// Holds a raw pointer into the LogBuffer's data region. The LogBuffer's
/// latch and pin count protocol ensures the pointer remains valid for the
/// lifetime of the segment.
pub struct LogBufferSegment {
    /// Raw pointer to the start of this segment's region in the buffer's heap
    /// allocation. Survives a `LogBuffer` move (the `BytesMut` heap buffer is
    /// not relocated by a move); validity for the segment's lifetime relies on
    /// the pin-count protocol preventing reuse/realloc while pinned, and on
    /// the pool keeping the owning `LogBuffer` alive.
    data_ptr: *mut u8,
    /// Shared latch + pin-count control block (see [`LogBufferControl`]).
    /// Owning a clone here is what makes the segment independent of the
    /// `LogBuffer`'s memory location (review R-F01).
    control: Arc<LogBufferControl>,
    size: usize,
}

// SAFETY: `data_ptr` is a raw pointer (not auto-`Send`); the LogBuffer
// latch + pin-count protocol (held via the `Arc<LogBufferControl>`) serializes
// access to the pointed-to bytes, and the pool keeps the owning buffer alive
// while any segment is pinned. The control block is itself `Send + Sync`
// (atomics + raw mutex) and carried by `Arc`, so only `data_ptr` requires the
// manual impl.
unsafe impl Send for LogBufferSegment {}

impl LogBufferSegment {
    /// Copies data into the underlying LogBuffer and decrements the pin count.
    pub fn put(&self, data: &[u8]) {
        assert_eq!(
            data.len(),
            self.size,
            "data size must match allocated segment size"
        );

        // Acquire the latch to guarantee happens-before semantics. The
        // control block (latch, latch_held, pin_count) is owned via Arc, so
        // these accesses are safe regardless of the owning LogBuffer's
        // location — only the raw `data_ptr` copy below needs `unsafe`.
        self.control.read_latch.lock();
        self.control.latch_held.store(true, Ordering::Relaxed);

        // SAFETY: We allocated this segment, so we know the pointer and range are valid.
        // The latch ensures no concurrent modification.
        unsafe {
            std::ptr::copy_nonoverlapping(
                data.as_ptr(),
                self.data_ptr,
                data.len(),
            );
        }

        // Release latch and decrement pin count.
        self.control.latch_held.store(false, Ordering::Relaxed);
        // SAFETY: we hold the latch (set latch_held=true above), so unlock is
        // sound here.
        unsafe {
            self.control.read_latch.unlock();
        }
        // C-7: Release ensures the copy_nonoverlapping above is visible
        // to any thread that Acquire-loads the pin_count in
        // wait_for_zero_and_latch() and observes zero.
        let prev = self.control.write_pin_count.fetch_sub(1, Ordering::Release);
        // Stage A: wake a reader parked on the pin-count word (see
        // LogBuffer::free for the missed-wakeup argument).
        if prev == 1 {
            futex_wake(&self.control.write_pin_count, i32::MAX as u32);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_buffer() {
        let buffer = LogBuffer::new(1024);
        assert_eq!(buffer.capacity(), 1024);
        assert!(buffer.get_first_lsn().is_null());
        assert!(buffer.has_room(1024));
    }

    #[test]
    fn test_allocate_and_put() {
        let buffer = LogBuffer::new(1024);
        buffer.latch_for_write();

        let segment = buffer.allocate(100).expect("should allocate");
        let data = vec![42u8; 100];

        buffer.release();

        segment.put(&data);

        // Verify data was written
        buffer.latch_for_write();
        assert_eq!(buffer.get_data()[0..100], data[..]);
        buffer.release();
    }

    // R-F01 regression: a LogBufferSegment must remain valid after the owning
    // LogBuffer value is MOVED. Pre-fix, the segment held raw pointers into
    // the LogBuffer's inline fields, so moving the buffer dangled them (UB).
    // Now the control block is shared via Arc and data_ptr targets the
    // (non-relocating) heap allocation, so this is sound.
    #[test]
    fn test_segment_survives_buffer_move() {
        let buffer = LogBuffer::new(1024);
        buffer.latch_for_write();
        let segment = buffer.allocate(64).expect("should allocate");
        buffer.release();

        // Move the buffer to a new location (and behind a Box, a second move).
        let moved = buffer;
        let boxed = Box::new(moved);

        // Use the segment AFTER the buffer moved: writes via the shared
        // control block + heap data_ptr.
        let data = vec![7u8; 64];
        segment.put(&data);

        // The moved/boxed buffer observes the written bytes and a settled
        // pin count (put decremented it back to zero).
        boxed.latch_for_write();
        assert_eq!(boxed.get_data()[0..64], data[..]);
        boxed.release();
    }

    #[test]
    fn test_register_lsn() {
        let buffer = LogBuffer::new(1024);
        buffer.latch_for_write();

        let lsn1 = Lsn::new(0, 100);
        buffer.register_lsn(lsn1);
        assert_eq!(buffer.get_first_lsn(), lsn1);

        let lsn2 = Lsn::new(0, 200);
        buffer.register_lsn(lsn2);
        assert_eq!(buffer.get_first_lsn(), lsn1);

        buffer.release();
    }

    #[test]
    fn test_has_room() {
        let buffer = LogBuffer::new(100);
        buffer.latch_for_write();

        assert!(buffer.has_room(100));
        assert!(buffer.has_room(50));
        assert!(!buffer.has_room(101));

        let _seg = buffer.allocate(50);
        assert!(buffer.has_room(50));
        assert!(!buffer.has_room(51));

        buffer.release();
        // Free the pin count (segment not used for actual writes in this test)
        buffer.free();
    }

    #[test]
    fn test_reinit() {
        let mut buffer = LogBuffer::new(1024);
        buffer.latch_for_write();

        let _seg = buffer.allocate(100);
        buffer.register_lsn(Lsn::new(0, 100));

        buffer.release();
        buffer.free(); // Free the pin count before reinit
        buffer.reinit();

        buffer.latch_for_write();
        assert!(buffer.get_first_lsn().is_null());
        assert_eq!(buffer.get_data().len(), 0);
        assert!(buffer.has_room(1024));
        buffer.release();
    }

    #[test]
    fn test_contains_lsn() {
        let buffer = LogBuffer::new(1024);
        buffer.latch_for_write();

        let seg = buffer.allocate(100).unwrap();
        let lsn = Lsn::new(5, 1000);
        buffer.register_lsn(lsn);
        buffer.release();

        // Complete the write to decrement pin count
        seg.put(&[0u8; 100]);

        // LSN at start of buffer
        assert!(buffer.contains_lsn(Lsn::new(5, 1000)));
        buffer.release();

        // LSN in middle of buffer
        assert!(buffer.contains_lsn(Lsn::new(5, 1050)));
        buffer.release();

        // LSN just past end
        assert!(!buffer.contains_lsn(Lsn::new(5, 1100)));

        // Different file
        assert!(!buffer.contains_lsn(Lsn::new(6, 1000)));
    }

    #[test]
    fn test_wrap_constructor() {
        let mut data = bytes::BytesMut::with_capacity(256);
        data.resize(64, 0xAB);
        let lsn = Lsn::new(2, 400);
        let buffer = LogBuffer::wrap(data, lsn);

        assert_eq!(buffer.get_first_lsn(), lsn);
        assert_eq!(buffer.capacity(), 256);
    }

    #[test]
    fn test_multiple_allocations() {
        let buffer = LogBuffer::new(1024);
        buffer.latch_for_write();

        // Allocate two segments.
        let seg1 = buffer.allocate(100).expect("first allocation");
        let seg2 = buffer.allocate(200).expect("second allocation");
        assert!(!buffer.has_room(725)); // 1024 - 300 = 724 remaining
        assert!(buffer.has_room(724));

        buffer.release();

        seg1.put(&[1u8; 100]);
        seg2.put(&[2u8; 200]);

        buffer.latch_for_write();
        let data = buffer.get_data();
        assert_eq!(&data[0..100], &[1u8; 100]);
        assert_eq!(&data[100..300], &[2u8; 200]);
        buffer.release();
    }

    #[test]
    fn test_allocate_exactly_capacity() {
        let buffer = LogBuffer::new(256);
        buffer.latch_for_write();

        let seg = buffer.allocate(256).expect("should fill exactly");
        assert!(!buffer.has_room(1));

        buffer.release();
        seg.put(&[0xCCu8; 256]);

        buffer.latch_for_write();
        let data = buffer.get_data();
        assert_eq!(data.len(), 256);
        assert!(data.iter().all(|&b| b == 0xCC));
        buffer.release();
    }

    #[test]
    fn test_allocate_too_large_returns_none() {
        let buffer = LogBuffer::new(128);
        buffer.latch_for_write();

        let result = buffer.allocate(129);
        assert!(result.is_none());
        // Pin count must not have been incremented.
        assert!(buffer.has_room(128));

        buffer.release();
    }

    #[test]
    fn test_get_bytes_after_write() {
        let buffer = LogBuffer::new(512);
        buffer.latch_for_write();

        let lsn = Lsn::new(7, 2000);
        let seg = buffer.allocate(50).unwrap();
        buffer.register_lsn(lsn);
        buffer.release();

        seg.put(&[0xAAu8; 50]);

        // get_bytes should return data starting at the correct offset.
        buffer.latch_for_write();
        let slice = buffer.get_bytes(lsn.file_offset());
        assert_eq!(&slice[..50], &[0xAAu8; 50]);
        buffer.release();
    }

    #[test]
    fn test_rewrite_allowed_flag() {
        let mut buffer = LogBuffer::new(64);
        assert!(!buffer.get_rewrite_allowed());
        buffer.set_rewrite_allowed();
        assert!(buffer.get_rewrite_allowed());
    }

    /// C-7 regression: the pin_count Release/Acquire ordering must guarantee
    /// that a reader waiting in `wait_for_zero_and_latch()` sees the complete
    /// segment write once the pin count reaches zero.
    ///
    /// The test spawns a writer thread that allocates a segment, writes a
    /// known pattern, then calls `put()` (which decrements pin_count with
    /// Release).  The main thread calls `wait_for_zero_and_latch()` (which
    /// Acquire-loads pin_count) and then asserts the pattern is visible.
    ///
    /// Without Release/Acquire (i.e. with pure Relaxed), the Rust/C++
    /// memory model does NOT guarantee this and Miri / hardware could
    /// observe a stale value.  With Release/Acquire the guarantee is
    /// unconditional.
    #[test]
    fn test_pin_count_release_acquire_ordering() {
        use std::sync::{Arc, Mutex};
        use std::thread;

        let buf = Arc::new(Mutex::new(LogBuffer::new(256)));

        // Allocate a segment while holding the latch.
        let segment = {
            let b = buf.lock().unwrap();
            b.latch_for_write();
            let seg = b.allocate(64).expect("must allocate 64 bytes");
            b.release(); // release latch; writer can now copy data
            seg
        };

        // Spawn a writer that fills the segment and calls put().
        let written_pattern = [0xABu8; 64];
        let t = thread::spawn(move || {
            segment.put(&written_pattern);
        });

        // Wait for writer to complete and latch the buffer.
        {
            let b = buf.lock().unwrap();
            b.wait_for_zero_and_latch(); // Acquire-loads pin_count
            let data = b.get_data();
            assert_eq!(
                &data[..64],
                &[0xABu8; 64],
                "C-7: writer's data must be visible after pin_count reaches zero"
            );
            b.release();
        }

        t.join().unwrap();
    }

    /// Stage A missed-wakeup regression: a reader parked in
    /// `wait_for_zero_and_latch` while a writer holds the pin MUST be woken
    /// (or make progress via the 100ns backstop) when the writer's `put`
    /// drops the pin to zero — it must never hang.  Runs many rounds where
    /// the reader's wait and the writer's decrement race, which is exactly
    /// the window a missed wakeup would deadlock.  A watchdog thread fails
    /// the test if any round stalls, rather than hanging CI forever.
    #[test]
    fn test_wait_for_zero_no_missed_wakeup() {
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
        use std::sync::{Arc, Mutex};
        use std::thread;
        use std::time::{Duration, Instant};

        const ROUNDS: usize = 2_000;
        let progress = Arc::new(AtomicUsize::new(0));
        let done = Arc::new(AtomicBool::new(false));

        // Watchdog: if `progress` stops advancing for 10s, the reader is
        // wedged on a missed wakeup — abort loudly.
        let wd_progress = Arc::clone(&progress);
        let wd_done = Arc::clone(&done);
        let watchdog = thread::spawn(move || {
            let mut last = 0usize;
            let mut stalls = 0;
            while !wd_done.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(500));
                let now = wd_progress.load(Ordering::Relaxed);
                if now == last {
                    stalls += 1;
                    assert!(
                        stalls < 20,
                        "wait_for_zero_and_latch wedged at round {now}: \
                         missed wakeup"
                    );
                } else {
                    stalls = 0;
                    last = now;
                }
            }
        });

        let start = Instant::now();
        for _ in 0..ROUNDS {
            let buf = Arc::new(Mutex::new(LogBuffer::new(256)));
            let segment = {
                let b = buf.lock().unwrap();
                b.latch_for_write();
                let seg = b.allocate(64).expect("allocate");
                b.release();
                seg
            };
            // Writer: drop the pin (decrement to zero, futex_wake) on another
            // thread, racing the reader's wait below.
            let writer = thread::spawn(move || {
                segment.put(&[0x5Au8; 64]);
            });
            // Reader: block until pin count is zero.  Must not hang.
            {
                let b = buf.lock().unwrap();
                b.wait_for_zero_and_latch();
                b.release();
            }
            writer.join().unwrap();
            progress.fetch_add(1, Ordering::Relaxed);
        }
        done.store(true, Ordering::Relaxed);
        watchdog.join().unwrap();
        // Sanity: 2000 tiny rounds should finish well under the watchdog
        // budget; a per-round 100ns-spin regression would blow this out.
        assert!(
            start.elapsed() < Duration::from_secs(30),
            "reader progress too slow: {:?}",
            start.elapsed()
        );
    }
}
