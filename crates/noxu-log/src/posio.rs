// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT
#![forbid(unsafe_code)]

//! Cross-platform positioned file I/O.
//!
//! On Unix these map directly to `pread64`/`pwrite64` via
//! `std::os::unix::fs::FileExt` (`read_at`, `read_exact_at`, `write_all_at`).
//! On Windows `std::os::windows::fs::FileExt` exposes only `seek_read` /
//! `seek_write` with no `*_exact` / `*_all` variants, so the exact/all forms
//! are emulated with retry loops that preserve the same semantics.

use std::fs::File;
use std::io;
use std::path::Path;

/// Fsyncs a directory so that file creations/renames within it are durable.
///
/// On Unix this opens the directory and `sync_all()`s it (POSIX requires a
/// directory fsync after `creat`/`rename` for the entry to survive a crash —
/// the C-1 durability fix). On Windows a directory handle must be opened with
/// `FILE_FLAG_BACKUP_SEMANTICS`; the flush is best-effort because NTFS journals
/// metadata and not all volumes support `FlushFileBuffers` on a directory
/// handle. A directory-flush failure must NOT fail the surrounding file
/// creation.
#[cfg(unix)]
pub(crate) fn sync_dir(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

/// See the Unix variant. Best-effort on Windows.
#[cfg(windows)]
pub(crate) fn sync_dir(path: &Path) -> io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt;
    // FILE_FLAG_BACKUP_SEMANTICS — required to obtain a handle to a directory.
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
    let dir = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
        .open(path)?;
    match dir.sync_all() {
        Ok(()) => Ok(()),
        // Some filesystems / Windows versions reject FlushFileBuffers on a
        // directory handle (ERROR_ACCESS_DENIED / ERROR_INVALID_FUNCTION).
        // NTFS metadata journaling already orders the directory entry, so
        // treat these as a successful best-effort flush rather than failing
        // file creation.
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::PermissionDenied | io::ErrorKind::Unsupported
            ) =>
        {
            Ok(())
        }
        Err(e) => Err(e),
    }
}

/// Reads up to `buf.len()` bytes at `offset`; returns the number read.
#[cfg(unix)]
pub(crate) fn read_at(
    file: &File,
    buf: &mut [u8],
    offset: u64,
) -> io::Result<usize> {
    use std::os::unix::fs::FileExt;
    file.read_at(buf, offset)
}

/// Reads up to `buf.len()` bytes at `offset`; returns the number read.
#[cfg(windows)]
pub(crate) fn read_at(
    file: &File,
    buf: &mut [u8],
    offset: u64,
) -> io::Result<usize> {
    use std::os::windows::fs::FileExt;
    file.seek_read(buf, offset)
}

/// Reads exactly `buf.len()` bytes at `offset`.
#[cfg(unix)]
pub(crate) fn read_exact_at(
    file: &File,
    buf: &mut [u8],
    offset: u64,
) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.read_exact_at(buf, offset)
}

/// Reads exactly `buf.len()` bytes at `offset` (retry loop over `seek_read`).
#[cfg(windows)]
pub(crate) fn read_exact_at(
    file: &File,
    mut buf: &mut [u8],
    mut offset: u64,
) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        match file.seek_read(buf, offset) {
            Ok(0) => break,
            Ok(n) => {
                let tmp = buf;
                buf = &mut tmp[n..];
                offset += n as u64;
            }
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    if buf.is_empty() {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "failed to fill whole buffer",
        ))
    }
}

/// Writes all of `buf` at `offset`.
#[cfg(unix)]
pub(crate) fn write_all_at(
    file: &File,
    buf: &[u8],
    offset: u64,
) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    // Fault layer (DST only; one relaxed atomic load in production -> no-op).
    match crate::faultdisk::on_write(buf.len()) {
        crate::faultdisk::WriteFault::None => {}
        crate::faultdisk::WriteFault::DiskFull => {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "faultdisk: simulated ENOSPC",
            ));
        }
        crate::faultdisk::WriteFault::Torn(keep) => {
            // Write only the surviving prefix, then lose power so neither the
            // dropped tail nor any later write reaches disk.
            file.write_all_at(&buf[..keep], offset)?;
            crate::faultdisk::power_cut();
        }
        crate::faultdisk::WriteFault::Corrupt { offset_in_buf, len } => {
            // Write the buffer, then flip bytes on disk to model bit-rot.
            file.write_all_at(buf, offset)?;
            let mut flipped = buf[offset_in_buf..offset_in_buf + len].to_vec();
            for b in &mut flipped {
                *b = !*b;
            }
            file.write_all_at(&flipped, offset + offset_in_buf as u64)?;
            return Ok(());
        }
    }
    file.write_all_at(buf, offset)
}

/// Writes all of `buf` at `offset` (retry loop over `seek_write`).
#[cfg(windows)]
pub(crate) fn write_all_at(
    file: &File,
    buf: &[u8],
    offset: u64,
) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    // Fault layer (DST only; inactive in production). The torn/disk-full/
    // corruption faults are modelled the same way as on Unix.
    match crate::faultdisk::on_write(buf.len()) {
        crate::faultdisk::WriteFault::None => {}
        crate::faultdisk::WriteFault::DiskFull => {
            return Err(io::Error::new(
                io::ErrorKind::StorageFull,
                "faultdisk: simulated ENOSPC",
            ));
        }
        crate::faultdisk::WriteFault::Torn(keep) => {
            win_write_all(file, &buf[..keep], offset)?;
            crate::faultdisk::power_cut();
        }
        crate::faultdisk::WriteFault::Corrupt { offset_in_buf, len } => {
            win_write_all(file, buf, offset)?;
            let mut flipped = buf[offset_in_buf..offset_in_buf + len].to_vec();
            for b in &mut flipped {
                *b = !*b;
            }
            return win_write_all(
                file,
                &flipped,
                offset + offset_in_buf as u64,
            );
        }
    }
    win_write_all(file, buf, offset)
}

/// Windows `write_all_at` core (retry loop over `seek_write`).
#[cfg(windows)]
fn win_write_all(
    file: &File,
    mut buf: &[u8],
    mut offset: u64,
) -> io::Result<()> {
    use std::os::windows::fs::FileExt;
    while !buf.is_empty() {
        match file.seek_write(buf, offset) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "failed to write whole buffer",
                ));
            }
            Ok(n) => {
                buf = &buf[n..];
                offset += n as u64;
            }
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
