// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Arc-owning read guard over the `dst_sync_pl` shuttle `RwLock` (DST only).
//!
//! # Why this exists
//!
//! `noxu-tree`'s hand-over-hand read descent (`search`, `first_entry_at_or_after`,
//! `get_prev_bin`, …) holds a chain of read locks along the path, each guard
//! *owning* its `Arc` so it can outlive the borrow of the node it came from.
//! Under the default build that guard is `parking_lot::ArcRwLockReadGuard`,
//! parking_lot's own inherent `Arc::read_arc()` API \u2014 zero cost, no shim.
//!
//! Under `--cfg noxu_shuttle` the node latch is the shuttle-instrumented
//! `noxu_util::dst_sync_pl::RwLock`, whose `RwLockReadGuard<'a, T>` *borrows*
//! the lock (shuttle 0.9 has no Arc-owning guard).  An Arc-owning wrapper is
//! self-referential (the guard borrows the lock the `Arc` keeps alive), which
//! cannot be expressed in safe Rust.  `noxu-tree` and `noxu-util` are both
//! `#![forbid(unsafe_code)]`, so the shim lives here in `noxu-latch`, which
//! already contains reviewed `unsafe` (the RAII force-unlock).
//!
//! This is **DST scaffolding**: it is compiled ONLY under `--cfg noxu_shuttle`,
//! never in production, so no release binary contains this `unsafe`.

use std::ops::Deref;
use std::sync::Arc;

use noxu_util::dst_sync_pl::{RwLock, RwLockReadGuard};

/// An `Arc`-owning read guard mirroring `parking_lot::ArcRwLockReadGuard`.
///
/// Field order matters for soundness: `guard` is declared before `_arc` so it
/// is dropped first (Rust drops struct fields in declaration order), releasing
/// the read lock before the `Arc` that keeps the lock alive is dropped.
pub struct ArcRwLockReadGuard<T: 'static> {
    // SAFETY: the guard borrows `*_arc`'s inner lock; the lifetime is extended
    // to `'static` below.  Sound because `_arc` (an owned strong reference)
    // keeps that lock alive for at least as long as this struct, and `guard`
    // drops before `_arc` (declaration order), so the borrow never dangles.
    // This mirrors the reviewed `'static`-guard pattern in
    // `noxu-log/src/log_source.rs` (`FileHandleGuard`), sound for the same
    // field-drop-order reason.
    guard: RwLockReadGuard<'static, T>,
    _arc: Arc<RwLock<T>>,
}

impl<T: 'static> ArcRwLockReadGuard<T> {
    /// Acquire an Arc-owning read guard \u2014 the shuttle analogue of
    /// `parking_lot`'s `Arc<RwLock<T>>::read_arc()`.
    pub fn read_arc(arc: &Arc<RwLock<T>>) -> Self {
        let arc = Arc::clone(arc);
        let guard = arc.read();
        // SAFETY: extend the guard's borrow of `*arc`'s inner lock to
        // `'static`.  `arc` is moved into the returned struct (`_arc`) and the
        // `guard` field is declared first, so it is dropped strictly before
        // `_arc`; the borrowed lock therefore outlives the guard.  No aliasing
        // hazard: a read guard only shares `&T`.
        let guard: RwLockReadGuard<'static, T> =
            unsafe { std::mem::transmute(guard) };
        ArcRwLockReadGuard { guard, _arc: arc }
    }

    /// The backing `Arc<RwLock<T>>` \u2014 the shuttle analogue of
    /// `parking_lot::ArcRwLockReadGuard::rwlock(&guard)`.
    pub fn rwlock(g: &Self) -> &Arc<RwLock<T>> {
        &g._arc
    }
}

impl<T: 'static> Deref for ArcRwLockReadGuard<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        &self.guard
    }
}

/// Extension trait giving `Arc<RwLock<T>>::read_arc()` the parking_lot shape
/// under shuttle.
pub trait ReadArc<T: 'static> {
    fn read_arc(&self) -> ArcRwLockReadGuard<T>;
}

impl<T: 'static> ReadArc<T> for Arc<RwLock<T>> {
    #[inline]
    fn read_arc(&self) -> ArcRwLockReadGuard<T> {
        ArcRwLockReadGuard::read_arc(self)
    }
}
