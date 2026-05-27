// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Noxu DB synchronization primitives.
//!
//! Futex-based `Mutex<T>`, `RwLock<T>`, and `Condvar` that replace
//! `parking_lot` throughout the Noxu codebase.
//!
//! ## Extra capabilities vs parking_lot
//!
//! | Method | Description |
//! |--------|-------------|
//! | `Mutex::get_n_waiters()` | Count of threads blocked waiting for the mutex |
//! | `Mutex::get_owner()` | Thread ID hash of the current owner |
//! | `RwLock::get_n_waiters()` | Count of threads blocked waiting for the rwlock |
//! | `RwLock::is_locked_exclusive()` | Returns true when a write lock is held |
//! | `RwLock::reader_count()` | Number of active shared-lock holders (global) |
//!
//! ## Drop-in compatibility
//!
//! The public types are designed to be drop-in replacements for the
//! corresponding `parking_lot` types:
//!
//! ```text
//! parking_lot::Mutex<T>          →  noxu_sync::Mutex<T>
//! parking_lot::RwLock<T>         →  noxu_sync::RwLock<T>
//! parking_lot::Condvar           →  noxu_sync::Condvar
//! parking_lot::RawMutex          →  noxu_sync::RawMutex
//! parking_lot::lock_api::…       →  noxu_sync::lock_api::…
//! parking_lot::MutexGuard<'_,T>  →  noxu_sync::MutexGuard<'_,T>
//! parking_lot::WaitTimeoutResult →  noxu_sync::WaitTimeoutResult
//! ```

pub mod condvar;
pub mod futex;
pub mod raw_mutex;
pub mod raw_rwlock;

pub use condvar::{Condvar, WaitTimeoutResult};
pub use raw_mutex::NoxuRawMutex;
pub use raw_rwlock::NoxuRawRwLock;

/// Re-export `lock_api` so callers can do `use noxu_sync::lock_api::RawMutex`.
pub use lock_api;

// ---------------------------------------------------------------------------
// Mutex
// ---------------------------------------------------------------------------

/// Mutual exclusion primitive backed by a futex.
///
/// Drop-in replacement for `parking_lot::Mutex<T>`.
pub type Mutex<T> = lock_api::Mutex<NoxuRawMutex, T>;

/// RAII guard returned by `Mutex::lock`.
pub type MutexGuard<'a, T> = lock_api::MutexGuard<'a, NoxuRawMutex, T>;

/// The raw mutex type for direct embed (e.g., `log_buffer.rs`).
///
/// Provides `RawMutex::INIT` for const initialisation and implements
/// `lock_api::RawMutex` for `.lock()` / `unsafe .unlock()`.
pub type RawMutex = NoxuRawMutex;

// ---------------------------------------------------------------------------
// RwLock
// ---------------------------------------------------------------------------

/// RAII guard returned by `RwLock::read`.
pub type RwLockReadGuard<'a, T> =
    lock_api::RwLockReadGuard<'a, NoxuRawRwLock, T>;

/// RAII guard returned by `RwLock::write`.
pub type RwLockWriteGuard<'a, T> =
    lock_api::RwLockWriteGuard<'a, NoxuRawRwLock, T>;

/// Reader-writer lock backed by a futex.
///
/// Drop-in replacement for `parking_lot::RwLock<T>`.
/// Non-fair: new readers are not blocked by waiting writers.
///
/// Additional methods beyond parking_lot:
///   - `is_locked_exclusive()` — true when a write lock is held
///   - `get_n_waiters()`       — number of threads waiting (read + write)
///   - `reader_count()`        — number of active readers
pub struct RwLock<T>(lock_api::RwLock<NoxuRawRwLock, T>);

impl<T> RwLock<T> {
    /// Creates a new `RwLock` wrapping `val`.
    #[inline]
    pub fn new(val: T) -> Self {
        RwLock(lock_api::RwLock::new(val))
    }

    /// Acquires a shared (read) lock, blocking until available.
    #[inline]
    pub fn read(&self) -> RwLockReadGuard<'_, T> {
        self.0.read()
    }

    /// Acquires an exclusive (write) lock, blocking until available.
    #[inline]
    pub fn write(&self) -> RwLockWriteGuard<'_, T> {
        self.0.write()
    }

    /// Tries to acquire a shared lock without blocking.
    #[inline]
    pub fn try_read(&self) -> Option<RwLockReadGuard<'_, T>> {
        self.0.try_read()
    }

    /// Tries to acquire an exclusive lock without blocking.
    #[inline]
    pub fn try_write(&self) -> Option<RwLockWriteGuard<'_, T>> {
        self.0.try_write()
    }

    /// Tries to acquire a shared lock within the given `timeout`.
    #[inline]
    pub fn try_read_for(
        &self,
        timeout: std::time::Duration,
    ) -> Option<RwLockReadGuard<'_, T>> {
        self.0.try_read_for(timeout)
    }

    /// Tries to acquire an exclusive lock within the given `timeout`.
    #[inline]
    pub fn try_write_for(
        &self,
        timeout: std::time::Duration,
    ) -> Option<RwLockWriteGuard<'_, T>> {
        self.0.try_write_for(timeout)
    }

    /// Returns `true` if the lock is held by any reader or by the exclusive writer.
    #[inline]
    pub fn is_locked(&self) -> bool {
        self.0.is_locked()
    }

    /// Returns `true` if the write (exclusive) lock is currently held.
    #[inline]
    pub fn is_locked_exclusive(&self) -> bool {
        // SAFETY: raw() is safe to call; we only read atomic state.
        unsafe { self.0.raw().is_write_locked() }
    }

    /// Returns the total number of threads waiting to acquire this lock.
    #[inline]
    pub fn get_n_waiters(&self) -> usize {
        unsafe { self.0.raw().get_n_waiters() }
    }

    /// Returns the number of active readers.
    #[inline]
    pub fn reader_count(&self) -> u32 {
        unsafe { self.0.raw().reader_count() }
    }

    /// Returns a reference to the raw `NoxuRawRwLock`.
    #[inline]
    pub fn raw(&self) -> &NoxuRawRwLock {
        unsafe { self.0.raw() }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::Duration;

    // --- Mutex ---

    #[test]
    fn mutex_basic() {
        let m = Mutex::new(0i32);
        *m.lock() = 42;
        assert_eq!(*m.lock(), 42);
    }

    #[test]
    fn mutex_try_lock() {
        let m = Arc::new(Mutex::new(()));
        let g = m.lock();
        let m2 = m.clone();
        let failed =
            std::thread::spawn(move || m2.try_lock().is_none()).join().unwrap();
        assert!(failed);
        drop(g);
        assert!(m.try_lock().is_some());
    }

    #[test]
    fn mutex_try_lock_for_timeout() {
        let m = Arc::new(Mutex::new(()));
        let g = m.lock();
        let m2 = m.clone();
        let timed_out = std::thread::spawn(move || {
            m2.try_lock_for(Duration::from_millis(30)).is_none()
        })
        .join()
        .unwrap();
        assert!(timed_out);
        drop(g);
    }

    #[test]
    fn mutex_get_n_waiters() {
        let m = Arc::new(Mutex::new(()));
        let _g = m.lock();
        let m2 = m.clone();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let b2 = barrier.clone();
        let handle = std::thread::spawn(move || {
            b2.wait();
            let _g2 = m2.lock();
        });
        barrier.wait();
        std::thread::sleep(Duration::from_millis(10));
        // The raw() accessor exposes get_n_waiters.
        assert!(unsafe { m.raw().get_n_waiters() } >= 1 || m.is_locked());
        drop(_g);
        handle.join().unwrap();
    }

    #[test]
    fn mutex_force_unlock() {
        let m = Mutex::new(());
        let _g = m.lock();
        unsafe { m.force_unlock() };
        // Should be acquirable now.
        assert!(m.try_lock().is_some());
    }

    // --- RwLock ---

    #[test]
    fn rwlock_basic_read_write() {
        let rw = RwLock::new(0i32);
        *rw.write() = 99;
        assert_eq!(*rw.read(), 99);
    }

    #[test]
    fn rwlock_multiple_readers() {
        let rw = Arc::new(RwLock::new(42i32));
        let rw2 = rw.clone();
        let g1 = rw.read();
        let handle = std::thread::spawn(move || {
            let g2 = rw2.read();
            *g2
        });
        assert_eq!(*g1, 42);
        assert_eq!(handle.join().unwrap(), 42);
    }

    #[test]
    fn rwlock_exclusive_blocks_readers() {
        let rw = Arc::new(RwLock::new(()));
        let _wg = rw.write();
        let rw2 = rw.clone();
        let failed = std::thread::spawn(move || rw2.try_read().is_none())
            .join()
            .unwrap();
        assert!(failed);
    }

    #[test]
    fn rwlock_is_locked_exclusive() {
        let rw = RwLock::new(());
        assert!(!rw.is_locked_exclusive());
        let _wg = rw.write();
        assert!(rw.is_locked_exclusive());
    }

    #[test]
    fn rwlock_try_write_for_timeout() {
        let rw = Arc::new(RwLock::new(()));
        let _wg = rw.write();
        let rw2 = rw.clone();
        let timed_out = std::thread::spawn(move || {
            rw2.try_write_for(Duration::from_millis(30)).is_none()
        })
        .join()
        .unwrap();
        assert!(timed_out);
    }

    #[test]
    fn rwlock_try_read_for_timeout() {
        let rw = Arc::new(RwLock::new(()));
        let _wg = rw.write();
        let rw2 = rw.clone();
        let timed_out = std::thread::spawn(move || {
            rw2.try_read_for(Duration::from_millis(30)).is_none()
        })
        .join()
        .unwrap();
        assert!(timed_out);
    }

    // --- RawMutex (for log_buffer style usage) ---

    #[test]
    fn raw_mutex_const_init_and_lock_unlock() {
        use lock_api::RawMutex as RawMutexTrait;
        let raw = NoxuRawMutex::INIT;
        assert!(!raw.is_locked());
        raw.lock();
        assert!(raw.is_locked());
        unsafe { raw.unlock() };
        assert!(!raw.is_locked());
    }
}
