// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Shared DST invariant assertions (DST Milestone 2, Phase 4 slice).
//!
//! These are the SAME safety properties the `noxu-spec` stateright models
//! check against the *abstract protocol*, expressed here as runnable asserts so
//! the [`shuttle`](https://docs.rs/shuttle) concurrency tests can check them
//! against the **real code** at every explored interleaving.  Writing each
//! invariant once (here) is the "specs become the DST oracle" synergy from the
//! DST plan: the spec proves the design; these asserts prove the implementation
//! matches it under N seeds of thread interleavings.
//!
//! The helpers are cheap (a couple of comparisons) and carry no dependency on
//! shuttle, so they live in `noxu-util` where every DST test can reach them.
//! Each panics with the offending values on violation; under a shuttle test
//! that panic aborts the schedule and shuttle prints the reproducing seed +
//! the shrunk interleaving.
//!
//! # Spec mapping
//!
//! | helper | `noxu-spec` model / property |
//! |---|---|
//! | [`assert_lsn_monotone`] | `wal_commit::LsnMonotone` |
//! | [`assert_fsynced_never_decreases`] | `wal_commit::FsyncedNeverDecreases` |
//! | [`assert_durable_covers_commit`] | `wal_commit::DurableImpliesLogged` (fsync-before-commit) |

/// `wal_commit::LsnMonotone` — assigned LSNs strictly increase.
///
/// Asserts `prev < next` for two LSNs a caller claims were assigned in order.
/// A stall (`prev == next`) or regression (`prev > next`) is a violation.
#[inline]
#[track_caller]
pub fn assert_lsn_monotone(prev: u64, next: u64) {
    assert!(
        prev < next,
        "LsnMonotone violated: prev LSN {prev} not strictly < next LSN {next}"
    );
}

/// `wal_commit::FsyncedNeverDecreases` — the durable high-water mark is
/// monotonically non-decreasing.
///
/// Asserts `new_fsynced >= old_fsynced`.  A durable watermark that moved
/// *backwards* would mean recovery could resurrect a stale prefix.
#[inline]
#[track_caller]
pub fn assert_fsynced_never_decreases(old_fsynced: u64, new_fsynced: u64) {
    assert!(
        new_fsynced >= old_fsynced,
        "FsyncedNeverDecreases violated: fsynced watermark went backwards \
         {old_fsynced} -> {new_fsynced}"
    );
}

/// `wal_commit::DurableImpliesLogged` — the fsync-before-commit invariant.
///
/// After a committer's `flush_and_sync` returns `Ok`, the durable watermark it
/// observed (`durable_lsn`) must cover its own commit LSN (`commit_lsn`).
/// Returning `Ok` with `durable_lsn < commit_lsn` would mean the caller was
/// told its write is durable before it actually reached stable storage.
#[inline]
#[track_caller]
pub fn assert_durable_covers_commit(durable_lsn: u64, commit_lsn: u64) {
    assert!(
        durable_lsn >= commit_lsn,
        "DurableImpliesLogged violated: durable watermark {durable_lsn} does \
         not cover committed LSN {commit_lsn} (commit reported durable before \
         fsync covered it)"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monotone_ok_and_violation() {
        assert_lsn_monotone(1, 2);
        assert!(
            std::panic::catch_unwind(|| assert_lsn_monotone(2, 2)).is_err(),
            "equal LSNs must be rejected (not strictly increasing)"
        );
        assert!(
            std::panic::catch_unwind(|| assert_lsn_monotone(3, 2)).is_err(),
            "decreasing LSN must be rejected"
        );
    }

    #[test]
    fn fsynced_never_decreases_ok_and_violation() {
        assert_fsynced_never_decreases(5, 5);
        assert_fsynced_never_decreases(5, 9);
        assert!(
            std::panic::catch_unwind(|| assert_fsynced_never_decreases(9, 5))
                .is_err(),
            "a backwards fsync watermark must be rejected"
        );
    }

    #[test]
    fn durable_covers_commit_ok_and_violation() {
        assert_durable_covers_commit(10, 10);
        assert_durable_covers_commit(11, 10);
        assert!(
            std::panic::catch_unwind(|| assert_durable_covers_commit(9, 10))
                .is_err(),
            "durable watermark below commit LSN must be rejected"
        );
    }
}
