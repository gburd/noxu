// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! R-LIVEDDL-711 — LIVE (no crash, no XA) catalog-concurrency data-loss matrix.
//!
//! Pre-existing on 7.11.x (base 223a643a and tag v7.11.0): an UNCOMMITTED
//! deferred DDL op under a user txn T takes NO name-record lock
//! (D-ADDITIVE-SAFETY #18), so a concurrent AUTO-COMMIT catalog op on the same
//! name is not blocked.  When T then ABORTS, the concurrent op's effect is
//! already durable and T's rollback cannot undo it -> database / records lost.
//!
//! Matrix: T-op in {remove, truncate, rename}, concurrent-op racing, T in
//! {abort, commit}.  RED (assert fires) = data loss observed.

#[allow(unused_imports)]
use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
use std::path::Path;
use tempfile::TempDir;

fn run_clean(dir: &Path, mode: &str, extra: &[(&str, &str)]) {
    let _ = std::fs::remove_file(dir.join("firstlsn_done"));
    let mut cmd =
        std::process::Command::new(env!("CARGO_BIN_EXE_crash_worker"));
    cmd.env("NOXU_CRASH_DIR", dir).env("NOXU_CRASH_MODE", mode);
    for (k, v) in extra {
        cmd.env(k, v);
    }
    let status = cmd.spawn().expect("spawn").wait().expect("wait");
    assert!(status.success(), "clean mode {mode} must exit 0");
}

fn read_flag_suffix(dir: &Path, prefix: &str) -> Option<String> {
    for e in std::fs::read_dir(dir).ok()?.flatten() {
        let n = e.file_name().to_string_lossy().to_string();
        if let Some(rest) = n.strip_prefix(prefix) {
            return Some(rest.to_string());
        }
    }
    None
}

struct Outcome {
    concurrent: String,
    x_ex: String,
    xr: String,
    y_ex: String,
    yr: String,
}

fn run_case(op: &str, decision: &str) -> Outcome {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_clean(&p, "rvxl_matrix", &[("RVXL_OP", op), ("RVXL_T", decision)]);
    let g = |pre: &str| read_flag_suffix(&p, pre).unwrap_or_default();
    let o = Outcome {
        concurrent: g("rvxl_concurrent_"),
        x_ex: g("rvxl_x_ex_"),
        xr: g("rvxl_xr_"),
        y_ex: g("rvxl_y_ex_"),
        yr: g("rvxl_yr_"),
    };
    eprintln!(
        "RVXL op={op} T={decision} concurrent={} x_ex={} xr={} y_ex={} yr={}",
        o.concurrent, o.x_ex, o.xr, o.y_ex, o.yr
    );
    o
}

// --- ABORT cases: after T aborts, the pre-txn state (x with 8 recs) must be
//     fully restored.  A concurrent op that already committed destroys it.

#[test]
fn rvxl_remove_vs_remove_abort() {
    let o = run_case("remove", "abort");
    assert!(
        o.x_ex == "1" && o.xr == "8",
        "LIVE remove-vs-remove + abort: x must survive with 8 recs; got x_ex={} xr={} (concurrent={})",
        o.x_ex,
        o.xr,
        o.concurrent
    );
}

#[test]
fn rvxl_truncate_vs_truncate_abort() {
    let o = run_case("truncate", "abort");
    assert!(
        o.x_ex == "1" && o.xr == "8",
        "LIVE truncate-vs-truncate + abort: x must survive with 8 recs; got x_ex={} xr={} (concurrent={})",
        o.x_ex,
        o.xr,
        o.concurrent
    );
}

#[test]
fn rvxl_rename_vs_remove_abort() {
    let o = run_case("rename", "abort");
    // T aborted the rename x->y, concurrent removed x.  x must survive with 8.
    assert!(
        o.x_ex == "1" && o.xr == "8",
        "LIVE rename-vs-remove + abort: x must survive with 8 recs; got x_ex={} xr={} y_ex={} (concurrent={})",
        o.x_ex,
        o.xr,
        o.y_ex,
        o.concurrent
    );
}

// --- COMMIT cases: documents the committed outcome (may or may not lose data;
//     recorded for the matrix, not all asserted).

#[test]
fn rvxl_remove_vs_remove_commit() {
    let o = run_case("remove", "commit");
    // Both want x gone; final x must be absent (no resurrection).
    assert!(
        o.x_ex == "0",
        "LIVE remove-vs-remove + commit: x must be gone; got x_ex={} (concurrent={})",
        o.x_ex,
        o.concurrent
    );
}

#[test]
fn rvxl_truncate_vs_truncate_commit() {
    let o = run_case("truncate", "commit");
    // T's truncate commits; x empty is the intended outcome.
    assert!(
        o.x_ex == "1" && o.xr == "0",
        "LIVE truncate-vs-truncate + commit: x present but empty; got x_ex={} xr={} (concurrent={})",
        o.x_ex,
        o.xr,
        o.concurrent
    );
}

#[test]
fn rvxl_rename_vs_remove_commit() {
    let o = run_case("rename", "commit");
    eprintln!(
        "RVXL rename-vs-remove commit documented: x_ex={} y_ex={} yr={} concurrent={}",
        o.x_ex, o.y_ex, o.yr, o.concurrent
    );
    // Documentary: a committed rename racing a committed remove is inherently
    // ambiguous; record, don't assert a single right answer here.
}
