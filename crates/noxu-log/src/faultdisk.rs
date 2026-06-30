// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT
#![forbid(unsafe_code)]

//! Storage-fault injection for deterministic simulation testing (DST).
//!
//! This is the fault layer that sits over the positioned-I/O chokepoint
//! ([`crate::posio`]'s four functions) and over [`crate::fsync_manager`]'s
//! fsync.  It injects, **per seed**, the faults a real power loss or failing
//! disk would cause:
//!
//! * **Torn write** — a write completes only a prefix of its buffer, then the
//!   process "loses power" (exits) so the tail bytes (and everything after)
//!   never reach disk.  This is the byte-precise, reproducible power-loss the
//!   `power_loss_sweep` SIGKILL approach cannot do (SIGKILL leaves dirty pages
//!   for the kernel to flush; this drops them at an exact, seed-chosen write).
//! * **Fsync drop** — `fsync` is acknowledged without flushing, then the
//!   process loses power, so writes the engine *believed* durable vanish.
//! * **Disk full** — a write returns `ENOSPC`/`StorageFull` at a seed-chosen
//!   write, exercising the engine's out-of-space error path.
//! * **Corruption** — bytes are flipped in a just-written region, exercising
//!   the checksum/verification path on the next read.
//!
//! # Production safety (the load-bearing invariant)
//!
//! The fault layer is gated behind a single process-global [`AtomicBool`]
//! ([`is_active`]).  It is `false` until [`FaultController::install`] is
//! called, and is **never** installed by production code — only by the DST
//! harness (`crates/noxu-db/src/bin/crash_worker.rs` under `NOXU_DST_SEED`,
//! and the DST tests).  When inactive, every posio call does one relaxed
//! atomic load and takes the real path: **zero behavior change in
//! production**.
//!
//! All fault decisions are drawn from one seeded [`Prng`], so a given
//! `NOXU_DST_SEED` reproduces the exact same fault at the exact same write.

use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use noxu_util::Prng;

/// Process-global active flag.  `false` = no fault layer (production default).
static ACTIVE: AtomicBool = AtomicBool::new(false);

/// Monotonic counter of write operations seen by the fault layer, used so the
/// harness can report *where* a fault fired and so decisions are stable.
static WRITE_COUNT: AtomicU64 = AtomicU64::new(0);

/// The installed controller (only present while DST is active).
static CONTROLLER: Mutex<Option<FaultController>> = Mutex::new(None);

/// Returns the number of writes the fault layer has seen so far (diagnostic;
/// stable per seed, useful for sizing the target-write range).
pub fn write_count() -> u64 {
    WRITE_COUNT.load(Ordering::SeqCst)
}

/// Returns `true` if the fault layer is active.  One relaxed atomic load;
/// `false` (and therefore free) in production.
#[inline]
pub fn is_active() -> bool {
    ACTIVE.load(Ordering::Relaxed)
}

/// Which fault this run injects.  One run injects at most one fault kind so a
/// failure maps cleanly to a single cause; the seed picks the kind, the write
/// index, and (for torn/corruption) the size.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultKind {
    /// No fault — a control run (recovery must be a perfect no-op power loss).
    None,
    /// Write only a prefix of the target buffer, then lose power.
    TornWrite,
    /// Acknowledge fsync without flushing, then lose power.
    FsyncDrop,
    /// Return `StorageFull` from the target write.
    DiskFull,
    /// Flip bytes in the just-written region (bit-rot).
    Corruption,
}

/// Outcome of consulting the controller for a write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteFault {
    /// No fault; perform the write normally.
    None,
    /// Write only this many leading bytes, then the process must power-cut.
    Torn(usize),
    /// Fail the write with `StorageFull`.
    DiskFull,
    /// Perform the write, then flip `len` bytes starting at `offset_in_buf`.
    Corrupt { offset_in_buf: usize, len: usize },
}

/// Per-seed fault plan.
#[derive(Debug)]
pub struct FaultController {
    kind: FaultKind,
    /// The write index (0-based, over all posio writes) at which the fault
    /// fires.
    target_write: u64,
    /// For TornWrite: keep this fraction (numer/16) of the buffer; for
    /// Corruption: how many bytes to flip.
    magnitude: u64,
    /// Drawn from the seed; retained for reproducibility / debugging.
    prng: Prng,
    /// Set once a power-cut fault has fired, so the harness can detect it.
    fired: bool,
}

impl FaultController {
    /// Build a controller from a seed.  The seed alone determines the fault
    /// kind, the target write, and the magnitude.
    pub fn from_seed(seed: u64) -> Self {
        let mut prng = Prng::new(seed);
        // 0 = None (control), 1..=4 = the four fault kinds.  ~1-in-5 runs are
        // clean controls, the rest spread over the four faults.
        let kind = match prng.below(5) {
            0 => FaultKind::None,
            1 => FaultKind::TornWrite,
            2 => FaultKind::FsyncDrop,
            3 => FaultKind::DiskFull,
            _ => FaultKind::Corruption,
        };
        // Fire within the first ~64 writes.  The crash-sweep workload issues
        // ~50 committed-and-synced writes (phase 1) plus a few buffered
        // uncommitted writes (phase 2) plus header writes, so 0..64 reliably
        // lands the fault inside the workload (often right at a committed-txn
        // boundary, which is the interesting power-loss point).  below() keeps
        // it in range and seed-stable.
        let target_write = prng.below(64);
        let magnitude = 1 + prng.below(15); // 1..=15
        FaultController { kind, target_write, magnitude, prng, fired: false }
    }

    /// The fault kind this controller will inject.
    pub fn kind(&self) -> FaultKind {
        self.kind
    }

    /// The write index at which the fault fires.
    pub fn target_write(&self) -> u64 {
        self.target_write
    }

    /// Whether a power-cut fault has fired.
    pub fn fired(&self) -> bool {
        self.fired
    }
}

/// Install the controller and activate the fault layer.
///
/// **Only called by the DST harness.**  After this, posio/fsync consult the
/// controller.  Idempotent-ish: a second install replaces the controller.
pub fn install(controller: FaultController) {
    *CONTROLLER.lock().expect("faultdisk mutex") = Some(controller);
    ACTIVE.store(true, Ordering::SeqCst);
}

/// Convenience: install a fresh controller built from `seed`.
pub fn install_seed(seed: u64) {
    install(FaultController::from_seed(seed));
}

/// Deactivate the fault layer and drop the controller (test cleanup).
pub fn uninstall() {
    ACTIVE.store(false, Ordering::SeqCst);
    *CONTROLLER.lock().expect("faultdisk mutex") = None;
    WRITE_COUNT.store(0, Ordering::SeqCst);
}

/// Consulted by `posio::write_all_at` on every write while active.
///
/// Returns the action to take.  Advances the global write counter.  When the
/// configured fault fires at the target write, this returns the corresponding
/// [`WriteFault`]; for power-cut faults (torn) the caller writes the prefix
/// then calls [`power_cut`].
pub fn on_write(buf_len: usize) -> WriteFault {
    if !is_active() {
        return WriteFault::None;
    }
    let idx = WRITE_COUNT.fetch_add(1, Ordering::SeqCst);
    let mut guard = CONTROLLER.lock().expect("faultdisk mutex");
    let Some(ctrl) = guard.as_mut() else {
        return WriteFault::None;
    };
    if idx != ctrl.target_write {
        return WriteFault::None;
    }
    match ctrl.kind {
        FaultKind::TornWrite => {
            // Keep magnitude/16 of the buffer (at least 0, less than full so
            // the write is genuinely torn).  A 0-length prefix is a valid
            // power loss (nothing of this write survives).
            let keep = ((buf_len as u64 * ctrl.magnitude) / 16) as usize;
            let keep = keep.min(buf_len.saturating_sub(1));
            ctrl.fired = true;
            WriteFault::Torn(keep)
        }
        FaultKind::DiskFull => WriteFault::DiskFull,
        FaultKind::Corruption => {
            let len = (ctrl.magnitude as usize).min(buf_len);
            WriteFault::Corrupt { offset_in_buf: 0, len }
        }
        // FsyncDrop and None do not alter writes.
        FaultKind::FsyncDrop | FaultKind::None => WriteFault::None,
    }
}

/// Consulted by the fsync path (`FsyncManager`/`sync_data`) while active.
///
/// Returns `true` if this fsync should be *dropped* (acknowledged without
/// flushing).  When it drops, it also marks the fault as fired and the caller
/// should power-cut so the unsynced bytes vanish.
pub fn on_fsync() -> bool {
    if !is_active() {
        return false;
    }
    let mut guard = CONTROLLER.lock().expect("faultdisk mutex");
    let Some(ctrl) = guard.as_mut() else {
        return false;
    };
    if ctrl.kind == FaultKind::FsyncDrop && !ctrl.fired {
        // Drop the very next fsync once we've passed the target write count.
        if WRITE_COUNT.load(Ordering::SeqCst) >= ctrl.target_write {
            ctrl.fired = true;
            return true;
        }
    }
    false
}

/// Simulate a power loss: exit the process *now*, dropping any not-yet-synced
/// kernel buffers.  Called by the worker after a torn write or dropped fsync.
///
/// Uses `process::exit` (not panic) so destructors do NOT run — a real power
/// loss does not run `Drop`, does not flush buffers, does not close files
/// cleanly.  Exit code 137 mirrors SIGKILL (128 + 9) so callers/parents can
/// recognise the simulated crash.
pub fn power_cut() -> ! {
    // Best-effort note to stderr for debugging a failing seed.
    eprintln!("[faultdisk] simulated power loss (process exit)");
    std::process::exit(137);
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests mutate process-global state; serialise them.
    static GUARD: Mutex<()> = Mutex::new(());

    #[test]
    fn inactive_by_default_zero_cost_path() {
        let _g = GUARD.lock().unwrap();
        uninstall();
        assert!(!is_active());
        assert_eq!(on_write(4096), WriteFault::None);
        assert!(!on_fsync());
    }

    #[test]
    fn same_seed_same_plan() {
        let a = FaultController::from_seed(12345);
        let b = FaultController::from_seed(12345);
        assert_eq!(a.kind(), b.kind());
        assert_eq!(a.target_write(), b.target_write());
    }

    #[test]
    fn torn_write_fires_at_target() {
        let _g = GUARD.lock().unwrap();
        // Find a seed whose kind is TornWrite with a small target.
        let mut seed = 0u64;
        let ctrl = loop {
            let c = FaultController::from_seed(seed);
            if c.kind() == FaultKind::TornWrite && c.target_write() < 5 {
                break c;
            }
            seed += 1;
        };
        let target = ctrl.target_write();
        install(ctrl);
        // Writes before the target are untouched.
        for _ in 0..target {
            assert_eq!(on_write(4096), WriteFault::None);
        }
        // The target write is torn (a strict prefix).
        match on_write(4096) {
            WriteFault::Torn(keep) => assert!(keep < 4096),
            other => panic!("expected Torn, got {other:?}"),
        }
        uninstall();
    }

    #[test]
    fn disk_full_fires_at_target() {
        let _g = GUARD.lock().unwrap();
        let mut seed = 0u64;
        let ctrl = loop {
            let c = FaultController::from_seed(seed);
            if c.kind() == FaultKind::DiskFull && c.target_write() < 3 {
                break c;
            }
            seed += 1;
        };
        let target = ctrl.target_write();
        install(ctrl);
        for _ in 0..target {
            assert_eq!(on_write(100), WriteFault::None);
        }
        assert_eq!(on_write(100), WriteFault::DiskFull);
        uninstall();
    }
}
