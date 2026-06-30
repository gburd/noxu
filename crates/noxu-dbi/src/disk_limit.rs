// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Disk-limit enforcement: refuse new user writes before the disk fills so
//! recovery stays possible, and resume once the cleaner/checkpointer free
//! space.
//!
//! This is a faithful port of JE's disk-limit machinery, which lives in
//! `je/cleaner/Cleaner.java` (the `recalcLogSizeStats` computation and the
//! cached volatile `diskUsageViolationMessage`) gated at the write path by
//! `je/dbi/EnvironmentImpl.java` `checkDiskLimitViolation()` and
//! `je/Cursor.java` `checkUpdatesAllowed()`.
//!
//! # JE mapping
//!
//! JE's general formula (`Cleaner.recalcLogSizeStats`) is:
//!
//! ```text
//!   totalSize  = activeSize + reservedSize
//!   freeBytes1 = diskFreeSpace - freeLimit
//!   maxOverage = (adjustedMax > 0) ? totalSize - adjustedMax : 0
//!   freeBytes2 = (adjustedMax > 0) ? min(freeBytes1, adjustedMax - totalSize)
//!                                  : freeBytes1
//!   availBytes = freeBytes2 + reservedSize - protectedSize
//!   violation  = availBytes <= 0
//! ```
//!
//! Noxu has no reserved-file machinery (the cleaner deletes files outright
//! rather than parking them as "reserved" for later deletion under disk
//! pressure), so `reservedSize == 0` and `protectedSize == 0`. JE's
//! `adjustedMaxDiskLimit` subtracts `freeDisk` from `maxDisk` only under
//! specific conditions (large maxDisk, explicit freeDisk, or HA); for the
//! common non-HA case `adjustedMax == maxDisk`. The formula reduces to:
//!
//! ```text
//!   totalSize  = total log bytes on disk
//!   freeBytes1 = diskFreeSpace - freeDisk
//!   availBytes = (maxDisk > 0) ? min(freeBytes1, maxDisk - totalSize)
//!                              : freeBytes1
//!   violation  = availBytes <= 0
//! ```
//!
//! When both `maxDisk == 0` and `freeDisk == 0` the limit is disabled
//! (`availBytes` is effectively positive infinity) and the check is a single
//! atomic load — no `statvfs`, no directory scan. This matches JE's behaviour
//! where `MAX_DISK == 0` and `FREE_DISK == 0` disable enforcement.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use noxu_log::file_manager::FileManager;

/// Cached disk-usage / disk-limit violation state.
///
/// JE: the cached fields on `Cleaner` (`diskUsageViolationMessage`,
/// `availableLogSize`, `totalLogSize`) plus the `maxDiskLimit`/`freeDiskLimit`
/// configuration. The violation flag is volatile and refreshed periodically
/// (and after each cleaner/checkpointer run); the write path only reads the
/// cached flag, never probes the disk synchronously.
pub struct DiskLimitTracker {
    /// `MAX_DISK`: absolute cap on total log size in bytes. 0 = disabled.
    max_disk: u64,
    /// `FREE_DISK`: keep-this-much-free reserve in bytes. 0 = disabled.
    free_disk: u64,
    /// FileManager used to probe total log size + free space. `None` for
    /// in-memory or test environments that cannot probe a filesystem.
    file_manager: Option<Arc<FileManager>>,
    /// Cached "is a limit currently violated" flag. The write path reads this
    /// with a single relaxed atomic load (JE: volatile `diskUsageViolationMessage
    /// != null`).
    violated: AtomicBool,
    /// Cached `availableLogSize` (bytes), for stats / diagnostics. Signed value
    /// stored as bits; only used for the violation message.
    available_log_size: AtomicU64,
    /// Cached total log size at the last refresh (bytes), for the error message.
    total_log_size: AtomicU64,
    /// Cached free disk space at the last refresh (bytes), for the error message.
    disk_free_space: AtomicU64,
}

impl DiskLimitTracker {
    /// Creates a tracker. If both limits are zero, enforcement is disabled and
    /// `refresh()` is a cheap no-op (the write path never probes the disk).
    pub fn new(
        max_disk: u64,
        free_disk: u64,
        file_manager: Option<Arc<FileManager>>,
    ) -> Self {
        DiskLimitTracker {
            max_disk,
            free_disk,
            file_manager,
            violated: AtomicBool::new(false),
            available_log_size: AtomicU64::new(0),
            total_log_size: AtomicU64::new(0),
            disk_free_space: AtomicU64::new(0),
        }
    }

    /// Returns true when disk-limit enforcement is configured (either limit
    /// non-zero). When false the tracker is inert: `refresh()` does nothing and
    /// `is_violated()` is always false, so the write-path check is a single
    /// branch with no atomic contention.
    #[inline]
    pub fn is_enabled(&self) -> bool {
        self.max_disk > 0 || self.free_disk > 0
    }

    /// Cheap read of the cached violation flag (JE: read volatile
    /// `diskUsageViolationMessage`). Called on every user write; must not probe
    /// the disk.
    #[inline]
    pub fn is_violated(&self) -> bool {
        self.is_enabled() && self.violated.load(Ordering::Relaxed)
    }

    /// Total log bytes recorded at the last refresh (for the error / stats).
    pub fn last_total_log_size(&self) -> u64 {
        self.total_log_size.load(Ordering::Relaxed)
    }

    /// Effective limit value to report in the error: `max_disk` when set, else
    /// the free-disk-derived ceiling is not a single number, so report
    /// `disk_free_space` reserve target. Used only for the error payload.
    pub fn effective_limit(&self) -> u64 {
        if self.max_disk > 0 { self.max_disk } else { self.free_disk }
    }

    /// Re-probes the disk and recomputes the cached violation flag.
    ///
    /// JE: `Cleaner.freshenLogSizeStats()` -> `recalcLogSizeStats(stats,
    /// getDiskFreeSpace())`. Called periodically by a daemon and after each
    /// cleaner/checkpointer run (when files may have been deleted), NOT on the
    /// write path.
    ///
    /// A probe error (e.g. directory transiently unreadable) leaves the cached
    /// flag unchanged rather than spuriously blocking or unblocking writes.
    pub fn refresh(&self) {
        if !self.is_enabled() {
            return;
        }
        let Some(fm) = self.file_manager.as_ref() else {
            return;
        };
        let total_size = match fm.total_log_size() {
            Ok(v) => v,
            Err(_) => return,
        };
        let disk_free = match fm.disk_free_space() {
            Ok(v) => v,
            Err(_) => return,
        };
        self.recalc(total_size, disk_free);
    }

    /// Pure violation computation (JE `Cleaner.recalcLogSizeStats`), separated
    /// from probing so it can be unit-tested without a filesystem.
    ///
    /// `reservedSize` and `protectedSize` are zero for Noxu (no reserved-file
    /// machinery), and `adjustedMax == maxDisk` for the non-HA case.
    pub fn recalc(&self, total_size: u64, disk_free_space: u64) {
        // freeBytes1 = diskFreeSpace - freeLimit   (JE; signed)
        let free_bytes1: i64 = disk_free_space as i64 - self.free_disk as i64;

        // availBytes (with reservedSize == protectedSize == 0):
        //   adjustedMax > 0 -> min(freeBytes1, adjustedMax - totalSize)
        //   else            -> freeBytes1
        let avail_bytes: i64 = if self.max_disk > 0 {
            let max_room = self.max_disk as i64 - total_size as i64;
            free_bytes1.min(max_room)
        } else {
            free_bytes1
        };

        // JE: violation when availBytes <= 0.
        let violated = avail_bytes <= 0;

        self.total_log_size.store(total_size, Ordering::Relaxed);
        self.disk_free_space.store(disk_free_space, Ordering::Relaxed);
        self.available_log_size.store(avail_bytes as u64, Ordering::Relaxed);
        // Store the flag last (release) so a reader that sees `violated`
        // observes the matching stats (JE updates the volatile field last).
        self.violated.store(violated, Ordering::Release);
    }

    /// Builds the diagnostic message for a `DiskLimitExceeded` error (JE's
    /// `diskUsageViolationMessage`).
    pub fn violation_message(&self) -> String {
        format!(
            "Disk usage is not within maxDisk or freeDisk limits and write \
             operations are prohibited: maxDisk={}, freeDisk={}, \
             totalLogSize={}, diskFreeSpace={}, availableLogSize={}",
            self.max_disk,
            self.free_disk,
            self.total_log_size.load(Ordering::Relaxed),
            self.disk_free_space.load(Ordering::Relaxed),
            self.available_log_size.load(Ordering::Relaxed) as i64,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // The violation computation is the correctness crux; this self-check
    // exercises the JE formula across the documented cases.
    #[test]
    fn disabled_never_violates() {
        let t = DiskLimitTracker::new(0, 0, None);
        assert!(!t.is_enabled());
        t.recalc(1_000_000, 0); // even with zero free space
        assert!(!t.is_violated());
    }

    #[test]
    fn max_disk_cap() {
        // maxDisk=100, freeDisk=0, plenty of free space.
        let t = DiskLimitTracker::new(100, 0, None);
        t.recalc(50, 1_000_000);
        assert!(!t.is_violated(), "50 < 100 cap, ok");
        t.recalc(100, 1_000_000);
        assert!(t.is_violated(), "100 == cap -> availBytes 0 -> violated");
        t.recalc(150, 1_000_000);
        assert!(t.is_violated(), "over cap");
        // Free space recovers below cap -> resumes.
        t.recalc(50, 1_000_000);
        assert!(!t.is_violated(), "back under cap -> resume");
    }

    #[test]
    fn free_disk_reserve() {
        // freeDisk=25, no maxDisk. Mirrors JE example rows.
        let t = DiskLimitTracker::new(0, 25, None);
        t.recalc(75, 20); // diskFS=20 < freeDisk=25 -> freeBytes1=-5 -> violated
        assert!(t.is_violated());
        t.recalc(75, 30); // diskFS=30 > 25 -> freeBytes1=5 -> ok
        assert!(!t.is_violated());
    }

    #[test]
    fn both_limits_min_governs() {
        // JE row: freeDL=25 maxDL=80 diskFS=20 totalLS=50 -> avail 0 -> violated
        let t = DiskLimitTracker::new(80, 25, None);
        t.recalc(50, 20);
        assert!(t.is_violated());
        // freeDL=5 maxDL=80 diskFS=20 totalLS=50 -> freeB1=15, maxRoom=30,
        // avail=min(15,30)=15 -> ok
        let t2 = DiskLimitTracker::new(80, 5, None);
        t2.recalc(50, 20);
        assert!(!t2.is_violated());
    }
}
