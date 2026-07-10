//! Property-based tests for noxu-latch (Hegel / hegeltest).

use hegel::generators;
use noxu_latch::{ExclusiveLatch, LatchContext, SharedLatch};
use std::sync::Arc;

// =============================================================================
// ExclusiveLatch property tests
// =============================================================================

/// An exclusive latch can be acquired and released repeatedly.
/// After release (guard drop), re-acquisition always succeeds.
#[hegel::test]
fn exclusive_latch_acquire_release_cycle(tc: hegel::TestCase) {
    let iterations =
        tc.draw(generators::integers::<u32>().min_value(1).max_value(19));
    let latch = ExclusiveLatch::named("prop-test");
    for _ in 0..iterations {
        {
            let _guard = latch.acquire().expect("acquire");
            assert!(latch.is_locked());
            assert!(latch.is_owner());
        }
        assert!(!latch.is_locked());
    }
}

/// try_acquire succeeds when the latch is not held.
#[test]
fn exclusive_latch_try_acquire_succeeds() {
    let latch = ExclusiveLatch::named("prop-test-try");
    let guard = latch.try_acquire();
    assert!(guard.is_some());
    assert!(latch.is_locked());
    drop(guard);
    assert!(!latch.is_locked());
}

/// Latch context name is preserved.
#[hegel::test]
fn exclusive_latch_preserves_name(tc: hegel::TestCase) {
    let name =
        tc.draw(generators::from_regex(r"[a-zA-Z0-9_-]{1,64}").fullmatch(true));
    let latch = ExclusiveLatch::new(LatchContext::new(name.clone()));
    assert_eq!(&latch.context().name, &name);
}

// =============================================================================
// SharedLatch property tests
// =============================================================================

/// Multiple shared acquires succeed simultaneously from different threads.
#[hegel::test]
fn shared_latch_multiple_readers(tc: hegel::TestCase) {
    let num_readers =
        tc.draw(generators::integers::<u32>().min_value(2).max_value(7));
    let latch = Arc::new(SharedLatch::named("prop-shared", false));

    // Acquire a shared lock from the main thread
    let _main_guard = latch.acquire_shared().expect("acquire_shared");

    // Spawn additional reader threads that should all succeed
    let handles: Vec<_> = (0..num_readers)
        .map(|_| {
            let latch = latch.clone();
            std::thread::spawn(move || {
                let _guard = latch.acquire_shared().expect("acquire_shared");
                true
            })
        })
        .collect();

    for handle in handles {
        let result = handle.join().unwrap();
        assert!(result);
    }
}

/// Exclusive acquisition after release always succeeds.
#[hegel::test]
fn shared_latch_exclusive_acquire_release(tc: hegel::TestCase) {
    let iterations =
        tc.draw(generators::integers::<u32>().min_value(1).max_value(19));
    let latch = SharedLatch::named("prop-exclusive", false);
    for _ in 0..iterations {
        {
            let _guard = latch.acquire_exclusive().expect("acquire_exclusive");
            assert!(latch.is_exclusive_owner());
        }
        assert!(!latch.is_exclusive_owner());
    }
}

/// In exclusive-only mode, acquire_shared acquires exclusive ownership.
#[test]
fn shared_latch_exclusive_only_mode() {
    let latch = SharedLatch::named("prop-excl-only", true);
    assert!(latch.is_exclusive_only());

    let _guard = latch.acquire_shared().expect("acquire_shared");
    // In exclusive-only mode, shared acquisition becomes exclusive
    assert!(latch.is_exclusive_owner());
}

/// SharedLatch context name is preserved.
#[hegel::test]
fn shared_latch_preserves_name(tc: hegel::TestCase) {
    let name =
        tc.draw(generators::from_regex(r"[a-zA-Z0-9_-]{1,64}").fullmatch(true));
    let latch = SharedLatch::new(LatchContext::new(name.clone()), false);
    assert_eq!(&latch.context().name, &name);
}
