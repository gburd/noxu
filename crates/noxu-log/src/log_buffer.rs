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
use noxu_sync::lock_api::RawMutex as RawMutexTrait;
use noxu_util::lsn::{Lsn, NULL_LSN};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::thread;
use std::time::Duration;

/// A write buffer backed by `BytesMut`.
///
///
///
/// Uses a RawMutex with manual lock/unlock to match explicit
/// latch_for_write/release pattern (not RAII).
pub struct LogBuffer {
    /// The actual buffer storage.
    data: BytesMut,

    /// LSN of the first entry in this buffer.
    first_lsn: Lsn,

    /// LSN of the last entry registered in this buffer.
    last_lsn: Lsn,

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
    write_pin_count: AtomicI32,
}

impl LogBufferControl {
    fn new() -> Self {
        LogBufferControl {
            read_latch: RawMutex::INIT,
            latch_held: AtomicBool::new(false),
            write_pin_count: AtomicI32::new(0),
        }
    }
}

impl LogBuffer {
    /// Creates a new LogBuffer with the specified capacity.
    pub fn new(capacity: usize) -> Self {
        LogBuffer {
            data: BytesMut::with_capacity(capacity),
            first_lsn: NULL_LSN,
            last_lsn: NULL_LSN,
            capacity,
            control: Arc::new(LogBufferControl::new()),
            rewrite_allowed: false,
            flushed_len: 0,
        }
    }

    /// Creates a temporary LogBuffer wrapping existing data at a specific LSN.
    ///
    /// Used by LogManager when an entry is too large for the buffer pool.
    pub fn wrap(data: BytesMut, first_lsn: Lsn) -> Self {
        let capacity = data.capacity();
        LogBuffer {
            data,
            first_lsn,
            last_lsn: first_lsn,
            capacity,
            control: Arc::new(LogBufferControl::new()),
            rewrite_allowed: false,
            flushed_len: 0,
        }
    }

    /// Reinitializes the buffer for reuse.
    ///
    /// The LWL and buffer pool latch must be held.
    pub fn reinit(&mut self) {
        self.latch_for_write();
        self.data.clear();
        self.first_lsn = NULL_LSN;
        self.last_lsn = NULL_LSN;
        self.rewrite_allowed = false;
        self.control.write_pin_count.store(0, Ordering::Relaxed);
        self.flushed_len = 0;
        self.release();
    }

    /// Returns the data that has not yet been flushed to disk.
    ///
    /// `LogBuffer` dirty-range tracking.  The caller should write
    /// this slice to disk at `flushed_file_offset()` and then call
    /// `mark_flushed()` to advance the watermark.
    pub fn get_unflushed_data(&self) -> &[u8] {
        &self.data[self.flushed_len..]
    }

    /// Returns the file offset at which `get_unflushed_data()` should be written.
    ///
    /// Equals `first_lsn.file_offset() + flushed_len`.
    pub fn flushed_file_offset(&self) -> u64 {
        self.first_lsn.file_offset() as u64 + self.flushed_len as u64
    }

    /// Advances the flush watermark to the current buffer length.
    ///
    /// Must be called after a successful `write_buffer()` for the unflushed
    /// slice so that the next flush only writes new data.
    pub fn mark_flushed(&mut self) {
        self.flushed_len = self.data.len();
    }

    /// Returns the first LSN held in this buffer.
    ///
    /// The LWL or read_latch must be held.
    pub fn get_first_lsn(&self) -> Lsn {
        self.first_lsn
    }

    /// Registers the LSN for a buffer segment that has been allocated in this buffer.
    ///
    /// The LWL and read_latch must be held.
    pub fn register_lsn(&mut self, lsn: Lsn) {
        assert!(
            self.control.latch_held.load(Ordering::Relaxed),
            "read_latch must be held"
        );

        if !self.last_lsn.is_null() {
            assert!(
                lsn > self.last_lsn,
                "lsn={:?} must be > last_lsn={:?}",
                lsn,
                self.last_lsn
            );
        }

        self.last_lsn = lsn;

        if self.first_lsn.is_null() {
            self.first_lsn = lsn;
        }
    }

    /// Checks if this buffer has room for the specified number of bytes.
    ///
    /// The LWL or read_latch must be held.
    pub fn has_room(&self, num_bytes: usize) -> bool {
        num_bytes <= (self.capacity - self.data.len())
    }

    /// Returns the buffer's data for read access.
    ///
    /// The LWL or read_latch must be held.
    pub fn get_data(&self) -> &[u8] {
        &self.data
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

        let found = if !self.first_lsn.is_null()
            && self.first_lsn.file_number() == lsn.file_number()
        {
            let file_offset = lsn.file_offset();
            let content_size = self.data.len();
            let first_lsn_offset = self.first_lsn.file_offset();
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

    /// Allocates a segment out of the buffer.
    ///
    /// The LWL and read_latch must be held.
    ///
    /// Returns `None` if not enough room, otherwise returns a `LogBufferSegment`
    /// for the data.
    pub fn allocate(&mut self, size: usize) -> Option<LogBufferSegment> {
        assert!(
            self.control.latch_held.load(Ordering::Relaxed),
            "read_latch must be held"
        );

        if self.has_room(size) {
            let offset = self.data.len();
            // Reserve space in the buffer
            self.data.resize(offset + size, 0);
            self.control.write_pin_count.fetch_add(1, Ordering::Relaxed);
            // SAFETY: offset is within the buffer we just resized.
            let data_ptr = unsafe { self.data.as_mut_ptr().add(offset) };
            Some(LogBufferSegment {
                data_ptr,
                // Share the control block (latch + pin count) so the segment
                // is independent of the LogBuffer's location in memory.
                control: Arc::clone(&self.control),
                size,
            })
        } else {
            None
        }
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
        self.control.write_pin_count.fetch_sub(1, Ordering::Release);
    }

    /// Acquires the buffer latched and with the buffer pin count equal to zero.
    pub fn wait_for_zero_and_latch(&self) {
        loop {
            // C-7: Acquire pairs with the Release in free() / LogBufferSegment::put()
            // to ensure we see all completed segment writes before re-using the buffer.
            if self.control.write_pin_count.load(Ordering::Acquire) > 0 {
                thread::park_timeout(Duration::from_nanos(100));
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
    /// The LWL or read_latch must be held.
    pub fn get_bytes(&self, file_offset: u32) -> &[u8] {
        let buffer_offset =
            (file_offset - self.first_lsn.file_offset()) as usize;
        &self.data[buffer_offset..]
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
        self.control.write_pin_count.fetch_sub(1, Ordering::Release);
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
        let mut buffer = LogBuffer::new(1024);
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
        let mut buffer = LogBuffer::new(1024);
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
        let mut buffer = LogBuffer::new(1024);
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
        let mut buffer = LogBuffer::new(100);
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
        let mut buffer = LogBuffer::new(1024);
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
        let mut buffer = LogBuffer::new(1024);
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
        let mut buffer = LogBuffer::new(256);
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
        let mut buffer = LogBuffer::new(128);
        buffer.latch_for_write();

        let result = buffer.allocate(129);
        assert!(result.is_none());
        // Pin count must not have been incremented.
        assert!(buffer.has_room(128));

        buffer.release();
    }

    #[test]
    fn test_get_bytes_after_write() {
        let mut buffer = LogBuffer::new(512);
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
            let mut b = buf.lock().unwrap();
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
}
