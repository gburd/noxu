// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! DST concurrency seam: `std::sync` / `std::thread` re-exports that are
//! swapped for [`shuttle`](https://docs.rs/shuttle) equivalents under
//! `--cfg noxu_shuttle` (DST Milestone 2, Phase 2a).
//!
//! # Why this exists
//!
//! `shuttle` is a deterministic concurrency-permutation tester: it replaces
//! the `std::sync` synchronisation primitives (`Mutex`, `Condvar`, atomics,
//! …) and `std::thread` with instrumented look-alikes, then explores thread
//! interleavings under a seed and *shrinks* any failing schedule.  To test the
//! **real** engine code (not a re-implementation) shuttle must intercept the
//! exact primitives the code acquires, so the production code has to acquire
//! its locks through a swappable path.
//!
//! Rather than sprinkle `#[cfg(noxu_shuttle)]` across every subsystem, the
//! concurrency-critical modules import their `Mutex` / `Condvar` / thread
//! spawn from **this module**.  When `noxu_shuttle` is off (the default and
//! every production / released build) `dst_sync` is a transparent re-export of
//! `std::sync` + `std::thread`, so the swap is a **zero-cost, zero-behaviour**
//! type alias — the compiler sees the identical `std` types it always did.
//! When `noxu_shuttle` is on (a dev/test-only cfg set via `RUSTFLAGS`) the same
//! names resolve to `shuttle::sync` / `shuttle::thread`, and the module's
//! callers become schedulable by shuttle's cooperative scheduler.
//!
//! # Shape compatibility
//!
//! `shuttle::sync` mirrors the **`std::sync`** API shape (`Mutex::lock()`
//! returns a `LockResult`, `Condvar::wait` takes an owned guard, …), *not* the
//! `parking_lot` shape that [`crate`]'s sibling `noxu-sync` crate exposes.
//! Only modules that already use `std::sync` (the [`FsyncManager`] group-commit
//! protocol and the [`DaemonManager`] shutdown coordinator) can therefore route
//! through this shim.  `noxu-sync`-based modules (`lock_manager`) keep the
//! `parking_lot` shape and cannot be shuttle-swapped without a separate
//! parking_lot-over-shuttle wrapper — see the DST plan for that limitation.
//!
//! [`FsyncManager`]: ../../noxu_log/fsync_manager/struct.FsyncManager.html
//! [`DaemonManager`]: ../../noxu_engine/daemon_manager/struct.DaemonManager.html

// ── Production / default: transparent std re-export ─────────────────────────
#[cfg(not(noxu_shuttle))]
mod imp {
    pub use std::sync::{
        Arc, Condvar, Mutex, MutexGuard, RwLock, WaitTimeoutResult,
    };
    pub use std::thread;
    pub mod atomic {
        pub use std::sync::atomic::{
            AtomicBool, AtomicI32, AtomicI64, AtomicU64, AtomicUsize, Ordering,
        };
    }
}

// ── DST: shuttle-instrumented primitives (dev/test only) ────────────────────
#[cfg(noxu_shuttle)]
mod imp {
    // shuttle::sync re-exports std's Arc/guard types but instruments Mutex,
    // Condvar, RwLock and the atomics so its scheduler can preempt at every
    // acquire/release/notify point.
    pub use shuttle::sync::{
        Arc, Condvar, Mutex, MutexGuard, RwLock, WaitTimeoutResult,
    };
    pub use shuttle::thread;
    pub mod atomic {
        pub use shuttle::sync::atomic::{
            AtomicBool, AtomicI32, AtomicI64, AtomicU64, AtomicUsize, Ordering,
        };
    }
}

pub use imp::*;
