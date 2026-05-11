//! Property-based tests for noxu-latch.

use noxu_latch::{ExclusiveLatch, LatchContext, SharedLatch};
use proptest::prelude::*;
use std::sync::Arc;

// =============================================================================
// ExclusiveLatch property tests
// =============================================================================

proptest! {
    /// An exclusive latch can be acquired and released repeatedly.
    /// After release (guard drop), re-acquisition always succeeds.
    #[test]
    fn exclusive_latch_acquire_release_cycle(iterations in 1u32..20u32) {
        let latch = ExclusiveLatch::named("prop-test");
        for _ in 0..iterations {
            {
                let _guard = latch.acquire().expect("acquire");
                prop_assert!(latch.is_locked());
                prop_assert!(latch.is_owner());
            }
            prop_assert!(!latch.is_locked());
        }
    }

    /// try_acquire succeeds when the latch is not held.
    #[test]
    fn exclusive_latch_try_acquire_succeeds(_dummy in 0u32..1u32) {
        let latch = ExclusiveLatch::named("prop-test-try");
        let guard = latch.try_acquire();
        prop_assert!(guard.is_some());
        prop_assert!(latch.is_locked());
        drop(guard);
        prop_assert!(!latch.is_locked());
    }

    /// Latch context name is preserved.
    #[test]
    fn exclusive_latch_preserves_name(name in "[a-zA-Z0-9_-]{1,64}") {
        let latch = ExclusiveLatch::new(LatchContext::new(name.clone()));
        prop_assert_eq!(&latch.context().name, &name);
    }
}

// =============================================================================
// SharedLatch property tests
// =============================================================================

proptest! {
    /// Multiple shared acquires succeed simultaneously from different threads.
    #[test]
    fn shared_latch_multiple_readers(num_readers in 2u32..8u32) {
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
            prop_assert!(result);
        }
    }

    /// Exclusive acquisition after release always succeeds.
    #[test]
    fn shared_latch_exclusive_acquire_release(iterations in 1u32..20u32) {
        let latch = SharedLatch::named("prop-exclusive", false);
        for _ in 0..iterations {
            {
                let _guard = latch.acquire_exclusive().expect("acquire_exclusive");
                prop_assert!(latch.is_exclusive_owner());
            }
            prop_assert!(!latch.is_exclusive_owner());
        }
    }

    /// In exclusive-only mode, acquire_shared acquires exclusive ownership.
    #[test]
    fn shared_latch_exclusive_only_mode(_dummy in 0u32..1u32) {
        let latch = SharedLatch::named("prop-excl-only", true);
        prop_assert!(latch.is_exclusive_only());

        let _guard = latch.acquire_shared().expect("acquire_shared");
        // In exclusive-only mode, shared acquisition becomes exclusive
        prop_assert!(latch.is_exclusive_owner());
    }

    /// SharedLatch context name is preserved.
    #[test]
    fn shared_latch_preserves_name(name in "[a-zA-Z0-9_-]{1,64}") {
        let latch = SharedLatch::new(LatchContext::new(name.clone()), false);
        prop_assert_eq!(&latch.context().name, &name);
    }
}
