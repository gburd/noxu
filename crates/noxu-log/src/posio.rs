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

/// Reads up to `buf.len()` bytes at `offset`; returns the number read.
#[cfg(unix)]
pub(crate) fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    use std::os::unix::fs::FileExt;
    file.read_at(buf, offset)
}

/// Reads up to `buf.len()` bytes at `offset`; returns the number read.
#[cfg(windows)]
pub(crate) fn read_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<usize> {
    use std::os::windows::fs::FileExt;
    file.seek_read(buf, offset)
}

/// Reads exactly `buf.len()` bytes at `offset`.
#[cfg(unix)]
pub(crate) fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> io::Result<()> {
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
pub(crate) fn write_all_at(file: &File, buf: &[u8], offset: u64) -> io::Result<()> {
    use std::os::unix::fs::FileExt;
    file.write_all_at(buf, offset)
}

/// Writes all of `buf` at `offset` (retry loop over `seek_write`).
#[cfg(windows)]
pub(crate) fn write_all_at(
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
