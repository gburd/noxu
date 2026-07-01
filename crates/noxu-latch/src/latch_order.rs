// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Debug-build latch-ordering assertion (L-3).
//!
//! A faithful analogue of BDB-JE's **debug-only** latch-ordering enforcement
//! (`LatchSupport` / the per-thread `LatchTable`).  On each thread a stack of
//! currently-held latch ranks is maintained; acquiring a latch whose
//! [`rank`](crate::LatchContext::rank) is **not strictly greater** than the
//! top of the stack is a lock-ordering bug and panics.
//!
//! Like JE's, this check is **compiled out entirely in release builds**
//! (`#[cfg(debug_assertions)]`), so it has **zero release-build cost** — it is
//! a developer/test-time invariant, not a production mechanism.
//!
//! A rank of `0` (the default) opts out: a rank-0 latch neither records itself
//! on the stack nor is checked, so the millions of unranked B-tree node
//! latches are completely unaffected.  A subsystem that wants ordering
//! enforcement assigns strictly-increasing ranks to the latches it acquires in
//! a fixed order (e.g. `parent < child`, or `tree < lock-table`).
//!
//! The public surface ([`enter`] / [`leave`]) is present in all builds so the
//! latch guards can call it unconditionally; in release builds both are inlined
//! to nothing.

/// Guard against the ordering check re-entering itself during a violation
/// report; also lets tests observe the "would-panic" decision without
/// unwinding.  Only referenced by the debug-build code path.
#[cfg(debug_assertions)]
mod imp {
    use std::cell::RefCell;

    thread_local! {
        /// Per-thread stack of `(rank, name)` for currently-held ranked
        /// latches.  Rank-0 latches are never pushed.
        static HELD: RefCell<Vec<(u32, String)>> = const { RefCell::new(Vec::new()) };
    }

    /// Record acquisition of a ranked latch and assert ordering.
    ///
    /// Panics if `rank` is not strictly greater than the rank of the
    /// most-recently-acquired still-held ranked latch on this thread.
    pub fn enter(rank: u32, name: &str) {
        if rank == 0 {
            return; // opt-out sentinel
        }
        HELD.with(|h| {
            let mut stack = h.borrow_mut();
            if let Some((top_rank, top_name)) = stack.last() {
                assert!(
                    rank > *top_rank,
                    "latch-ordering violation: acquiring '{name}' (rank {rank}) \
                     while holding '{top_name}' (rank {top_rank}); latches must \
                     be acquired in strictly increasing rank order",
                );
            }
            stack.push((rank, name.to_string()));
        });
    }

    /// Record release of a ranked latch (LIFO).
    pub fn leave(rank: u32) {
        if rank == 0 {
            return;
        }
        HELD.with(|h| {
            let mut stack = h.borrow_mut();
            // Remove the newest entry with this rank (guards drop in LIFO order
            // in normal code; be tolerant of an unwinding drop that skipped a
            // push after a caught panic in tests).
            if let Some(pos) = stack.iter().rposition(|(r, _)| *r == rank) {
                stack.remove(pos);
            }
        });
    }

    /// Test-only: number of ranked latches currently recorded as held on this
    /// thread.  Lets a test assert the stack is balanced after a caught panic.
    pub fn held_depth() -> usize {
        HELD.with(|h| h.borrow().len())
    }
}

/// Record acquisition of a ranked latch and assert consistent ordering.
///
/// See the [module docs](self).  Compiled to a no-op in release builds.
#[inline(always)]
pub fn enter(_rank: u32, _name: &str) {
    #[cfg(debug_assertions)]
    imp::enter(_rank, _name);
}

/// Record release of a ranked latch (LIFO).
///
/// Compiled to a no-op in release builds.
#[inline(always)]
pub fn leave(_rank: u32) {
    #[cfg(debug_assertions)]
    imp::leave(_rank);
}

/// Test-only: currently-held ranked-latch depth on this thread (debug builds
/// only; returns 0 in release builds).
#[inline(always)]
pub fn held_depth() -> usize {
    #[cfg(debug_assertions)]
    {
        imp::held_depth()
    }
    #[cfg(not(debug_assertions))]
    {
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // These tests only assert meaningful behaviour in debug builds (where the
    // check is active).  In a release build `enter`/`leave` are no-ops, so the
    // ordering assertions are trivially skipped.

    #[test]
    fn in_order_ranks_ok() {
        enter(10, "tree");
        enter(20, "lock-table");
        assert_eq!(held_depth(), if cfg!(debug_assertions) { 2 } else { 0 });
        leave(20);
        leave(10);
        assert_eq!(held_depth(), 0);
    }

    #[test]
    fn rank_zero_never_recorded() {
        enter(0, "unranked");
        enter(0, "also-unranked");
        assert_eq!(held_depth(), 0);
        leave(0);
    }

    #[cfg(debug_assertions)]
    #[test]
    fn out_of_order_panics() {
        // Acquire a high rank, then attempt a lower/equal rank — a lock-
        // ordering bug — which must panic in a debug build.
        let result = std::panic::catch_unwind(|| {
            enter(20, "lock-table");
            enter(10, "tree"); // 10 <= 20: ordering violation
        });
        assert!(
            result.is_err(),
            "out-of-order rank acquisition must panic in debug builds"
        );
        // Clean up the successfully-pushed entry so this thread's stack does
        // not leak into sibling tests (tests share the thread pool worker).
        leave(20);
        // catch_unwind swallowed the panic before the second push, so depth is
        // back to 0 after the single leave.
        assert_eq!(held_depth(), 0);
    }

    #[cfg(debug_assertions)]
    #[test]
    fn equal_rank_panics() {
        let result = std::panic::catch_unwind(|| {
            enter(15, "a");
            enter(15, "b"); // equal rank is not strictly increasing
        });
        assert!(
            result.is_err(),
            "equal rank must panic (not strictly greater)"
        );
        leave(15);
        assert_eq!(held_depth(), 0);
    }
}
