// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! R-XADDL-711 v2 — INDEPENDENT RE-REVIEW adversarial witnesses.
//!
//! New, non-reused witnesses written for the v7.11.1 patch re-review of
//! `fix/xa-ddl-711` @ e53f0a41.  Each drives the public XA + catalog API
//! through the crash_worker harness (SIGKILL only via `child.kill()` on a
//! recorded child; never pkill/killall).
//!
//!   RVX-1  a prepared branch touching the SAME name more than once
//!          (create a / rename a->b / create a again / remove e), commit.
//!   RVX-2  TWO in-doubt branches on DIFFERENT names, resolved in OPPOSITE
//!          order (remove p / remove q; commit q then p).
//!   RVX-3  an in-doubt branch + clean env.close() + reopen + resolve.
//!   RVX-4  resolve a committed in-doubt remove WHILE another thread
//!          repeatedly opens the in-doubt name (no panic/deadlock; InUse
//!          before, NotFound after, never a live handle on a removed db).
//!   RVX-5  LIVE (no crash, no XA) FID-RF1V2-1 analogue: uncommitted
//!          remove("x") + concurrent truncate/remove/open + ABORT; x must
//!          retain its 8 records (no live-case data loss).

use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
use std::path::Path;
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn wait_for_flag(dir: &Path, name: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    let flag = dir.join(name);
    while Instant::now() < deadline {
        if flag.exists() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    false
}

/// Run a crash_worker mode that LOOPS (signals `firstlsn_done` then spins),
/// then SIGKILL it (crash).  Mirrors `xa_ddl_711.rs::run_phase`.
fn run_phase(dir: &Path, mode: &str) {
    let _ = std::fs::remove_file(dir.join("firstlsn_done"));
    let mut child =
        std::process::Command::new(env!("CARGO_BIN_EXE_crash_worker"))
            .env("NOXU_CRASH_DIR", dir)
            .env("NOXU_CRASH_MODE", mode)
            .spawn()
            .expect("spawn crash_worker");
    assert!(
        wait_for_flag(dir, "firstlsn_done", Duration::from_secs(180)),
        "worker never finished (mode {mode})"
    );
    child.kill().expect("SIGKILL");
    child.wait().expect("wait");
}

/// Run a crash_worker mode that EXITS cleanly (no loop), with optional env.
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
    assert!(
        dir.join("firstlsn_done").exists(),
        "clean mode {mode} must signal done"
    );
}

fn recover_phase(dir: &Path, multi: bool) {
    let _ = std::fs::remove_file(dir.join("firstlsn_done"));
    let mode = if multi { "xaddl_fuzz_recover" } else { "xaddl_recover_open" };
    let status = std::process::Command::new(env!("CARGO_BIN_EXE_crash_worker"))
        .env("NOXU_CRASH_DIR", dir)
        .env("NOXU_CRASH_MODE", mode)
        .spawn()
        .expect("spawn recover")
        .wait()
        .expect("wait recover");
    assert!(
        status.success() && dir.join("firstlsn_done").exists(),
        "recover_phase failed"
    );
}

fn open_env(dir: &Path) -> Environment {
    Environment::open(
        EnvironmentConfig::new(dir.to_path_buf())
            .with_allow_create(false)
            .with_transactional(true),
    )
    .expect("environment must reopen")
}

fn db_records(env: &Environment, name: &str) -> (bool, u32) {
    let names = env.database_names().expect("database_names");
    if !names.iter().any(|n| n == name) {
        return (false, 0);
    }
    let db = match env.open_database(
        None,
        name,
        &DatabaseConfig::new().with_transactional(true),
    ) {
        Ok(db) => db,
        Err(_) => return (true, 0),
    };
    let txn = env.begin_transaction(None).expect("read txn");
    let mut present = 0u32;
    for i in 0u32..8 {
        let want = format!("xa_payload_{i}").into_bytes();
        match db.get_in(&txn, i.to_be_bytes()).expect("get") {
            Some(v) if &v[..] == want.as_slice() => present += 1,
            _ => {}
        }
    }
    txn.commit().unwrap();
    db.close().unwrap();
    (true, present)
}

fn read_flag_suffix(dir: &Path, prefix: &str) -> Option<String> {
    let rd = std::fs::read_dir(dir).ok()?;
    for e in rd.flatten() {
        let n = e.file_name().to_string_lossy().to_string();
        if let Some(rest) = n.strip_prefix(prefix) {
            return Some(rest.to_string());
        }
    }
    None
}

// RVX-1: same name touched more than once in one prepared branch.
// FIDELITY witness: the recovered in-doubt resolution must match what the
// IDENTICAL live (direct-commit) txn produces.  On 7.11.x recreating a name
// freed by a same-txn rename is a PRE-EXISTING deferred-DDL limitation on the
// LIVE path (the recreated "a" is dropped), so the faithful recovered outcome
// is the SAME: b(8) present, a absent, e gone.  (Not a finding against this
// fix; see the live control rvx_same_name_twice_live.)
#[test]
fn rvx1_same_name_twice_commit_matches_live() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "rvx_same_name_twice_prepare");
    recover_phase(&p, false);
    run_phase(&p, "xaddl_commit");
    let env = open_env(&p);
    let (a_ex, _ar) = db_records(&env, "a");
    let (b_ex, br) = db_records(&env, "b");
    let (e_ex, _er) = db_records(&env, "e");
    env.close().unwrap();
    // b = renamed-away a (keeps 8 recs); e removed; recreated "a" is dropped by
    // the pre-existing live deferred-DDL limitation (recovered == live).
    assert!(
        b_ex && br == 8 && !e_ex && !a_ex,
        "RVX-1 fidelity: recovered must match live (b(8), e gone, a dropped); \
         got a_ex={a_ex} b_ex={b_ex} br={br} e_ex={e_ex}"
    );
}

// RVX-2: two branches, different names, resolved in opposite order.
#[test]
fn rvx2_two_branches_opposite_order_commit() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "rvx_two_branches_prepare");
    recover_phase(&p, true);
    run_phase(&p, "rvx_two_branches_resolve_qp");
    let env = open_env(&p);
    let (p_ex, _) = db_records(&env, "p");
    let (q_ex, _) = db_records(&env, "q");
    env.close().unwrap();
    assert!(
        !p_ex && !q_ex,
        "RVX-2: both removes (resolved q-then-p) must take effect; \
         got p_ex={p_ex} q_ex={q_ex}"
    );
}

// RVX-3: in-doubt branch survives clean close+reopen, then resolves.
#[test]
fn rvx3_indoubt_close_reopen_resolve_commit() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "xaddl_remove_prepare"); // x(8) remove prepared, crash
    recover_phase(&p, false);
    // First invocation: clean close while in doubt.
    run_clean(&p, "rvx_close_reopen_resolve", &[]);
    // Second invocation: branch still in doubt -> commit it (loops -> kill).
    run_phase(&p, "rvx_close_reopen_resolve");
    let env = open_env(&p);
    let (x_ex, _) = db_records(&env, "x");
    env.close().unwrap();
    assert!(
        !x_ex,
        "RVX-3: a prepared remove that survived a clean close+reopen in doubt \
         must still commit and remove x; got x_ex={x_ex}"
    );
}

// RVX-4: resolve while another thread hammers open() on the in-doubt name.
#[test]
fn rvx4_resolve_vs_concurrent_open() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "xaddl_remove_prepare");
    recover_phase(&p, false);
    run_clean(&p, "rvx_resolve_vs_concurrent_open", &[]);
    let before_inuse =
        read_flag_suffix(&p, "rvx4_before_inuse_").unwrap_or_default();
    let opened_ok = read_flag_suffix(&p, "rvx4_opened_ok_").unwrap_or_default();
    let other = read_flag_suffix(&p, "rvx4_other_").unwrap_or_default();
    let notfound =
        read_flag_suffix(&p, "rvx4_notfound_").unwrap_or_default();
    eprintln!(
        "RVX4 before_inuse={before_inuse} opened_ok={opened_ok} \
         other={other} notfound={notfound}"
    );
    // XDB-711 #2b (FID-711-OPEN-FAILFAST resolved): a concurrent open of an
    // in-doubt name now takes a BLOCKING NameLN READ lock and WAITS behind the
    // in-doubt branch's WRITE lock -- JE `DbTree.getDb` blocks behind the
    // NameLN WRITE lock rather than failing fast (the former non-blocking
    // fail-fast `LockConflict` was the retired `FID-711-OPEN-FAILFAST`
    // deviation).  So the correct oracle is no longer "at least one open is
    // refused WHILE in doubt" (an open blocks instead of being refused, and
    // whether it times out depends on the lock timeout vs the in-doubt
    // window): it is (1) NO opener ever gets a live handle on the removed /
    // in-doubt db, (2) every failure is a typed, expected error
    // (LockConflict / LockTimeout / InUse / NotFound), never a surprise, and
    // (3) after the branch resolves (committed remove), opens see the removed
    // state -- a typed `NotFound`, or a `LockTimeout` for an opener whose wait
    // exceeded the lock timeout before resolution.  A race-through (the bug
    // this guards) would surface as `opened_ok=1` or an `other` error.
    assert_eq!(
        opened_ok, "0",
        "RVX-4: no concurrent open may ever get a live handle on the in-doubt \
         / removed db (opened_ok={opened_ok})"
    );
    assert_eq!(
        other, "0",
        "RVX-4: every refusal must be a typed LockConflict/InUse/LockTimeout/\
         NotFound, never a surprising error (other={other})"
    );
    // At least one open must have been refused SOMEHOW -- blocked past the
    // lock timeout (LockTimeout, counted in before_inuse) OR seen the removed
    // state after resolution (NotFound).  Both are JE-correct; what must never
    // happen is a silent race-through (caught by opened_ok==0 above).
    assert!(
        before_inuse == "1" || notfound != "0",
        "RVX-4: a concurrent open of the in-doubt/removed db must be refused \
         (blocked->LockTimeout) or see NotFound after resolution \
         (before_inuse={before_inuse}, notfound={notfound})"
    );
    // Final state: x removed.
    let env = open_env(&p);
    let (x_ex, _) = db_records(&env, "x");
    env.close().unwrap();
    assert!(!x_ex, "RVX-4: committed remove must have removed x");
}

// RVX-5 (LIVE FID-RF1V2-1 analogue) lives in the dedicated R-LIVEDDL-711
// witness files (rvx5_liveddl.rs / rvxl_liveddl_matrix.rs): those are RED on
// 7.11.x (base, tag, and this fix) and belong to the separately-dispatched
// live-DDL fix, not to this in-doubt-XA fix's validation set.  Keeping them
// here would make this file RED for a defect outside e53f0a41's scope.
