//! Platform-level futex abstraction.
//!
//! On Linux, uses the `futex(2)` syscall directly with `FUTEX_PRIVATE_FLAG`
//! for process-private (intra-process) locks — faster than the default
//! since the kernel can skip hash-table lookup for cross-process sharing.
//!
//! On non-Linux platforms, falls back to a spin-park loop using
//! `std::thread::park_timeout`, which is semantically correct but less
//! efficient.

use std::sync::atomic::AtomicU32;
use std::time::Duration;

/// Wait until `*futex_word != expected` or timeout elapses.
///
/// Returns `true` if we slept (or the value was already different).
/// Returns `false` only if the timeout expired without a notification.
#[cfg(target_os = "linux")]
pub fn futex_wait(
    futex_word: &AtomicU32,
    expected: u32,
    timeout: Option<Duration>,
) -> bool {
    use std::sync::atomic::Ordering;

    // FUTEX_WAIT | FUTEX_PRIVATE_FLAG = 0 | 128
    const FUTEX_WAIT_PRIVATE: i32 = 128;

    // Check before issuing syscall — if value already changed, return immediately.
    if futex_word.load(Ordering::Relaxed) != expected {
        return true;
    }

    let timeout_ts = timeout.map(|d| libc::timespec {
        tv_sec: d.as_secs() as libc::time_t,
        tv_nsec: d.subsec_nanos() as libc::c_long,
    });
    let timeout_ptr = match &timeout_ts {
        Some(ts) => ts as *const libc::timespec,
        None => std::ptr::null(),
    };

    // SAFETY: a raw `SYS_futex` syscall with `FUTEX_WAIT_PRIVATE`. The
    // arguments satisfy the kernel's contract:
    // - `futex_word` is a live `&AtomicU32` (borrow held for this call), so the
    //   pointer is non-null, aligned to 4 bytes, and points at 4 readable bytes
    //   for the whole syscall. FUTEX_WAIT only ever *reads* it.
    // - `timeout_ptr` is either null (block indefinitely) or points at
    //   `timeout_ts`, which is a live local that outlives this call.
    // - `FUTEX_PRIVATE_FLAG` keeps the wait process-local, so no cross-process
    //   mapping requirements apply.
    // The syscall cannot violate Rust aliasing: the kernel only compares the
    // word against `expected` and parks/wakes this thread. All failure modes
    // (EAGAIN/EINTR/ETIMEDOUT) are returned as `ret` and handled below, never
    // as UB.
    let ret = unsafe {
        libc::syscall(
            libc::SYS_futex,
            futex_word as *const AtomicU32 as *const u32,
            FUTEX_WAIT_PRIVATE,
            expected as i32,
            timeout_ptr,
            std::ptr::null::<u32>(),
            0i32,
        )
    };

    if ret == 0 {
        return true; // woken by futex_wake
    }
    let e = std::io::Error::last_os_error().raw_os_error().unwrap_or(0);
    // EAGAIN = value changed (not a timeout), EINTR = signal, ETIMEDOUT = timed out
    e != libc::ETIMEDOUT
}

/// Wake up to `count` threads waiting on `futex_word`.
///
/// `count` should be 1 to wake one thread, or `i32::MAX as u32` to wake all.
/// The kernel `nr_wake` argument is a signed int; passing `u32::MAX` would
/// truncate to -1, waking at most one thread — always use `i32::MAX as u32`
/// when the intent is "wake all".
#[cfg(target_os = "linux")]
pub fn futex_wake(futex_word: &AtomicU32, count: u32) {
    // FUTEX_WAKE | FUTEX_PRIVATE_FLAG
    const FUTEX_WAKE_PRIVATE: i32 = 128 | 1;
    // Kernel nr_wake is signed; clamp to i32::MAX to avoid sign wrap.
    let nr_wake = count.min(i32::MAX as u32) as i32;
    // SAFETY: a raw `SYS_futex` syscall with `FUTEX_WAKE_PRIVATE`. The
    // arguments satisfy the kernel's contract:
    // - `futex_word` is a live `&AtomicU32` (borrow held for this call), so the
    //   pointer is non-null, aligned to 4 bytes, and points at 4 readable bytes
    //   for the whole syscall. FUTEX_WAKE only *reads* it to locate the wait
    //   queue; it never writes through the pointer.
    // - `nr_wake` is clamped to `i32::MAX` above, so it cannot sign-wrap into a
    //   negative count (which the kernel would treat as "wake one").
    // - The timeout/uaddr2 arguments are unused by FUTEX_WAKE and passed as
    //   null, and `FUTEX_PRIVATE_FLAG` keeps the wake process-local.
    // Waking is idempotent and cannot fail in a way that affects memory safety:
    // a spurious or lost wake is a liveness concern the callers handle with a
    // predicate re-check, never UB. The return value is intentionally ignored.
    unsafe {
        libc::syscall(
            libc::SYS_futex,
            futex_word as *const AtomicU32 as *const u32,
            FUTEX_WAKE_PRIVATE,
            nr_wake,
            std::ptr::null::<libc::timespec>(),
            std::ptr::null::<u32>(),
            0i32,
        );
    }
}

/// Non-Linux fallback: spin briefly then park the thread.
///
/// This is correct but not as efficient as futex. Threads will be woken
/// by a combination of `park_timeout` expiry and spurious wakeups.
#[cfg(not(target_os = "linux"))]
pub fn futex_wait(
    futex_word: &AtomicU32,
    expected: u32,
    timeout: Option<Duration>,
) -> bool {
    use std::sync::atomic::Ordering;

    if futex_word.load(Ordering::Relaxed) != expected {
        return true;
    }
    let sleep = timeout
        .unwrap_or(Duration::from_millis(1))
        .min(Duration::from_millis(1));
    std::thread::park_timeout(sleep);
    let timed_out = timeout.map(|d| sleep >= d).unwrap_or(false);
    !timed_out
}

/// Non-Linux fallback: no-op — threads will wake via park_timeout expiry.
#[cfg(not(target_os = "linux"))]
pub fn futex_wake(_futex_word: &AtomicU32, _count: u32) {
    // On non-Linux, threads wake via timeout. This is correct but busy-waits.
}
