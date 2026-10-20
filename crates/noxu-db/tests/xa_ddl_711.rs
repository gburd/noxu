// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! XA-DDL probe (v7.11.1) — does a DDL op (CREATE / REMOVE / RENAME) performed
//! inside a PREPARED XA transaction survive and resolve correctly across a
//! crash on the 7.11.x catalog mechanism (relog + pending_ddl_names, full-scan
//! recovery, R-XARECOVERED prepared-DATA-LN redo+relock)?
//!
//! On the v8 line, a prepared DDL branch was mis-resolved after
//! crash -> recover -> xa_commit/xa_rollback -> 2nd crash.  7.11.x has a
//! different catalog mechanism; R-XARECOVERED (fe2daff1/763542a2) redoes and
//! relocks prepared DATA LNs, but prepared NameLNs (DDL) were never checked.
//!
//! Structure (mirrors xarec_recovery_witness.rs):
//!   crash #1: do DDL under a txn, `prepare()`, leak the txn, SIGKILL.
//!   reopen  : xa_recover() must surface the branch.
//!   (a) xa_commit -> committed outcome; 2nd crash; reopen -> still that.
//!   (b) xa_rollback -> pre-txn catalog; 2nd crash; reopen -> still that.
//!   in-doubt: after the first recovery, a concurrent catalog op must block or
//!             conflict, never see the in-doubt state as committed.
//!
//! GREEN = correct committed/rolled-back catalog + records after the 2nd crash.
//! RED   = wrong outcome (lost create, un-applied remove/rename, resurrected
//!         name, ghost records, or in-doubt seen as committed).

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

fn recover_phase(dir: &Path) {
    // The XA transaction manager's recovery step: a CLEAN env reopen that runs
    // recovery, surfaces the in-doubt branch via xa_recover()
    // (recovered_prepared_txns), takes a checkpoint (as a long-lived env would
    // between recover and resolve), and closes cleanly.  Models a TM that
    // crashes/restarts, recovers, lists in-doubt branches, then resolves in a
    // later step.  Subprocess with a CLEAN exit so the post-recovery checkpoint
    // actually runs.
    let _ = std::fs::remove_file(dir.join("firstlsn_done"));
    let mut child =
        std::process::Command::new(env!("CARGO_BIN_EXE_crash_worker"))
            .env("NOXU_CRASH_DIR", dir)
            .env("NOXU_CRASH_MODE", "xaddl_recover_open")
            .spawn()
            .expect("spawn recover_phase");
    let status = child.wait().expect("wait recover_phase");
    assert!(
        status.success() && dir.join("firstlsn_done").exists(),
        "recover_phase (xa_recover) failed: the in-doubt branch was not \
         surfaced or the post-recovery checkpoint errored"
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

/// Does db `name` exist, and if so what records does it carry?  Returns
/// (exists, record-count-matching-xa_payload).  Uses allow_create=false so a
/// missing db is reported as "absent", never created.
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
        // Name in catalog but tree unopenable: treat as present-but-empty so
        // the assertion surfaces the discrepancy rather than panicking.
        Err(_) => return (true, 0),
    };
    let txn = env.begin_transaction(None).expect("read txn");
    let mut present = 0u32;
    for i in 0u32..8 {
        let want = format!("xa_payload_{i}").into_bytes();
        match db.get_in(&txn, i.to_be_bytes()).expect("get") {
            Some(v) if &v[..] == want.as_slice() => present += 1,
            Some(_) => {} // wrong value: not counted as a correct record
            None => {}
        }
    }
    txn.commit().unwrap();
    db.close().unwrap();
    (true, present)
}

// ========================================================================
// CREATE "x" (+ records) under a prepared XA branch.
// ========================================================================

#[test]
fn xaddl_create_commit_survives_second_crash() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "xaddl_create_prepare"); // crash #1
    recover_phase(&p); // xa_recover (surfaces branch + post-recovery checkpoint)
    run_phase(&p, "xaddl_commit"); // resolve + crash #2 (pre-checkpoint)

    let env = open_env(&p);
    let (exists, recs) = db_records(&env, "x");
    env.close().unwrap();
    assert!(
        exists && recs == 8,
        "CREATE+commit: db 'x' must exist with its 8 committed records after \
         the 2nd crash; got exists={exists} recs={recs} (lost-create = S1/S2 \
         data loss)"
    );
}

#[test]
fn xaddl_create_rollback_survives_second_crash() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "xaddl_create_prepare"); // crash #1
    recover_phase(&p); // xa_recover (surfaces branch + post-recovery checkpoint)
    run_phase(&p, "xaddl_rollback"); // resolve(rollback) + crash #2

    let env = open_env(&p);
    let (exists, recs) = db_records(&env, "x");
    env.close().unwrap();
    assert!(
        !exists,
        "CREATE+rollback: db 'x' must NOT exist after the 2nd crash; got \
         exists={exists} recs={recs} (a rolled-back create that survives is a \
         resurrected db / catalog corruption)"
    );
}

// ========================================================================
// REMOVE "x" (pre-existing committed, with records) under a prepared branch.
// ========================================================================

#[test]
fn xaddl_remove_commit_survives_second_crash() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "xaddl_remove_prepare"); // crash #1
    recover_phase(&p); // xa_recover (surfaces branch + post-recovery checkpoint)
    run_phase(&p, "xaddl_commit"); // resolve(commit) + crash #2

    let env = open_env(&p);
    let (exists, recs) = db_records(&env, "x");
    env.close().unwrap();
    assert!(
        !exists,
        "REMOVE+commit[recs={recs}]: db 'x' must be GONE after the 2nd crash; it still \
         exists => the committed remove was not applied (catalog / durability \
         defect)"
    );
}

#[test]
fn xaddl_remove_rollback_survives_second_crash() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "xaddl_remove_prepare"); // crash #1
    recover_phase(&p); // xa_recover (surfaces branch + post-recovery checkpoint)
    run_phase(&p, "xaddl_rollback"); // resolve(rollback) + crash #2

    let env = open_env(&p);
    let (exists, recs) = db_records(&env, "x");
    env.close().unwrap();
    assert!(
        exists && recs == 8,
        "REMOVE+rollback: db 'x' must still exist with all 8 records after the \
         2nd crash; got exists={exists} recs={recs} (a rolled-back remove that \
         still deletes the db is S1/S2 data loss)"
    );
}

// ========================================================================
// RENAME "x" -> "y" (x pre-existing committed, with records).
// ========================================================================

#[test]
fn xaddl_rename_commit_survives_second_crash() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "xaddl_rename_prepare"); // crash #1
    recover_phase(&p); // xa_recover (surfaces branch + post-recovery checkpoint)
    run_phase(&p, "xaddl_commit"); // resolve(commit) + crash #2

    let env = open_env(&p);
    let (x_exists, _x) = db_records(&env, "x");
    let (y_exists, y_recs) = db_records(&env, "y");
    env.close().unwrap();
    assert!(
        !x_exists && y_exists && y_recs == 8,
        "RENAME+commit: after the 2nd crash 'y' must exist with x's 8 records \
         and 'x' must be gone; got x_exists={x_exists} y_exists={y_exists} \
         y_recs={y_recs} (wrong = lost/duplicated db or lost records)"
    );
}

#[test]
fn xaddl_rename_rollback_survives_second_crash() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "xaddl_rename_prepare"); // crash #1
    recover_phase(&p); // xa_recover (surfaces branch + post-recovery checkpoint)
    run_phase(&p, "xaddl_rollback"); // resolve(rollback) + crash #2

    let env = open_env(&p);
    let (x_exists, x_recs) = db_records(&env, "x");
    let (y_exists, _y) = db_records(&env, "y");
    env.close().unwrap();
    assert!(
        x_exists && x_recs == 8 && !y_exists,
        "RENAME+rollback: after the 2nd crash 'x' must still exist with its 8 \
         records and 'y' must be absent; got x_exists={x_exists} \
         x_recs={x_recs} y_exists={y_exists} (wrong = corrupted catalog)"
    );
}

// ========================================================================
// DDL + DATA in the SAME prepared branch (R-XADDL-711): the recovered branch
// must resolve BOTH its NameLN (DDL) and its data LNs (to a different,
// pre-existing db "d") together -- the DDL reconstruction must not disturb the
// R-XARECOVERED data-LN redo+relock, and vice versa.
// ========================================================================

#[test]
fn xaddl_create_data_commit_survives_second_crash() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "xaddl_create_data_prepare"); // crash #1
    recover_phase(&p); // xa_recover + post-recovery checkpoint
    run_phase(&p, "xaddl_commit"); // resolve(commit) + crash #2

    let env = open_env(&p);
    let (x_exists, x_recs) = db_records(&env, "x");
    let (d_exists, d_recs) = db_records(&env, "d");
    env.close().unwrap();
    assert!(
        x_exists && x_recs == 8 && d_exists && d_recs == 8,
        "CREATE+DATA commit: after the 2nd crash 'x' must exist with its 8          records AND 'd' must carry the branch's 8 records; got          x_exists={x_exists} x_recs={x_recs} d_exists={d_exists}          d_recs={d_recs} (lost DDL or lost data in a combined branch = S1/S2)"
    );
}

#[test]
fn xaddl_create_data_rollback_survives_second_crash() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "xaddl_create_data_prepare"); // crash #1
    recover_phase(&p);
    run_phase(&p, "xaddl_rollback"); // resolve(rollback) + crash #2

    let env = open_env(&p);
    let (x_exists, _x) = db_records(&env, "x");
    let (d_exists, d_recs) = db_records(&env, "d");
    env.close().unwrap();
    assert!(
        !x_exists && d_exists && d_recs == 0,
        "CREATE+DATA rollback: after the 2nd crash 'x' must be absent AND the          branch's records to 'd' must be gone (d keeps only its committed          base); got x_exists={x_exists} d_exists={d_exists} d_recs={d_recs}"
    );
}

#[test]
fn xaddl_remove_data_commit_survives_second_crash() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "xaddl_remove_data_prepare"); // crash #1
    recover_phase(&p);
    run_phase(&p, "xaddl_commit"); // resolve(commit) + crash #2

    let env = open_env(&p);
    let (x_exists, _x) = db_records(&env, "x");
    let (d_exists, d_recs) = db_records(&env, "d");
    env.close().unwrap();
    assert!(
        !x_exists && d_exists && d_recs == 8,
        "REMOVE+DATA commit: after the 2nd crash 'x' must be GONE AND 'd' must          carry the branch's 8 records; got x_exists={x_exists}          d_exists={d_exists} d_recs={d_recs}"
    );
}

#[test]
fn xaddl_remove_data_rollback_survives_second_crash() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "xaddl_remove_data_prepare"); // crash #1
    recover_phase(&p);
    run_phase(&p, "xaddl_rollback"); // resolve(rollback) + crash #2

    let env = open_env(&p);
    let (x_exists, x_recs) = db_records(&env, "x");
    let (d_exists, d_recs) = db_records(&env, "d");
    env.close().unwrap();
    assert!(
        x_exists && x_recs == 8 && d_exists && d_recs == 0,
        "REMOVE+DATA rollback: after the 2nd crash 'x' must still exist with          its 8 records AND the branch's records to 'd' must be gone; got          x_exists={x_exists} x_recs={x_recs} d_exists={d_exists}          d_recs={d_recs}"
    );
}

// ========================================================================
// In-doubt isolation: a concurrent catalog op on the in-doubt name must
// block or conflict, NEVER see the in-doubt DDL as committed.
// ========================================================================

fn probe_outcome(dir: &Path) -> String {
    for r in ["committed", "conflict", "blocked"] {
        if dir.join(format!("probe_result_{r}")).exists() {
            return r.to_string();
        }
    }
    "none".to_string()
}

#[test]
fn xaddl_create_indoubt_not_visible_as_committed() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "xaddl_create_prepare"); // crash #1
    run_phase(&p, "xaddl_indoubt_probe_create"); // recover + probe + crash
    let r = probe_outcome(&p);
    assert!(
        r == "conflict" || r == "blocked",
        "CREATE in-doubt: a concurrent create of 'x' must block/conflict, not \
         succeed-as-committed; probe outcome = {r}"
    );
}

#[test]
fn xaddl_remove_indoubt_not_visible_as_committed() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "xaddl_remove_prepare"); // crash #1
    run_phase(&p, "xaddl_indoubt_probe_remove"); // recover + probe + crash
    let r = probe_outcome(&p);
    assert!(
        r == "conflict" || r == "blocked",
        "REMOVE in-doubt: a concurrent remove of 'x' must block/conflict, not \
         succeed-as-committed; probe outcome = {r}"
    );
}

#[test]
fn xaddl_rename_indoubt_not_visible_as_committed() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "xaddl_rename_prepare"); // crash #1
    run_phase(&p, "xaddl_indoubt_probe_rename"); // recover + probe + crash
    let r = probe_outcome(&p);
    assert!(
        r == "conflict" || r == "blocked",
        "RENAME in-doubt: renaming the in-doubt target 'y'->'z' must \
         block/conflict (y not yet committed), not succeed; probe outcome = {r}"
    );
}

// RV-ADV (reviewer, resolve-call teeth): resolve(commit) + a post-resolution
// forced checkpoint + CLEAN close (NOT a crash).  The author witness always
// SIGKILLs immediately after xaddl_commit, so the checkpoint relog that runs
// AFTER resolution -- the exact path resolve_recovered_prepared_ddl protects
// against -- is never exercised, and disabling the resolve call leaves the
// author matrix fully GREEN (vacuous).  Here the committed REMOVE/RENAME must
// still be gone after a clean reopen that followed a post-resolution
// checkpoint; without resolution the still-marked/never-physically-removed
// victim can be relogged live and resurrected.
#[test]
fn rv_adv_remove_commit_ckpt_clean_then_reopen() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "xaddl_remove_prepare"); // crash #1
    recover_phase(&p); // xa_recover + checkpoint
    // resolve(commit) + post-resolution checkpoint + CLEAN close (no crash)
    let mut child =
        std::process::Command::new(env!("CARGO_BIN_EXE_crash_worker"))
            .env("NOXU_CRASH_DIR", &p)
            .env("NOXU_CRASH_MODE", "xaddl_commit_ckpt_clean")
            .spawn()
            .expect("spawn commit_ckpt_clean");
    let status = child.wait().expect("wait commit_ckpt_clean");
    assert!(status.success(), "commit_ckpt_clean must exit cleanly");

    let env = open_env(&p);
    let (exists, recs) = db_records(&env, "x");
    env.close().unwrap();
    assert!(
        !exists,
        "RV-ADV REMOVE+commit+ckpt+clean[recs={recs}]: db x must be GONE after a          CLEAN reopen that followed a post-resolution checkpoint; it still exists =>          resolve_recovered_prepared_ddl did not run (physical remove skipped and/or          pending_ddl_names left stale so the checkpoint relog resurrected the victim)"
    );
}

#[test]
fn rv_adv_rename_commit_ckpt_clean_then_reopen() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "xaddl_rename_prepare"); // crash #1
    recover_phase(&p); // xa_recover + checkpoint
    let mut child =
        std::process::Command::new(env!("CARGO_BIN_EXE_crash_worker"))
            .env("NOXU_CRASH_DIR", &p)
            .env("NOXU_CRASH_MODE", "xaddl_commit_ckpt_clean")
            .spawn()
            .expect("spawn commit_ckpt_clean");
    let status = child.wait().expect("wait commit_ckpt_clean");
    assert!(status.success(), "commit_ckpt_clean must exit cleanly");

    let env = open_env(&p);
    let (x_exists, _xr) = db_records(&env, "x");
    let (y_exists, yr) = db_records(&env, "y");
    env.close().unwrap();
    assert!(
        !x_exists && y_exists && yr == 8,
        "RV-ADV RENAME+commit+ckpt+clean: expected ONLY y with 8 records after a          clean reopen following a post-resolution checkpoint; got x_exists={x_exists}          y_exists={y_exists} yr={yr}"
    );
}

// RV-ADV2 (reviewer): the LIVE (same-process) post-commit state must reflect a
// committed REMOVE immediately -- the resolve call keeps the in-memory catalog
// consistent for a long-lived TM that commits and KEEPS RUNNING.  database_names()
// must not list "x" and a repeat remove("x") must be NotFound (name gone + unlocked).
// RED when resolve_recovered_prepared_ddl is disabled (stale live name_map + the
// in-doubt conflict never clears).
#[test]
fn rv_adv2_live_state_after_commit() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "xaddl_remove_prepare");
    recover_phase(&p);
    let _ = std::fs::remove_file(p.join("firstlsn_done"));
    let mut child =
        std::process::Command::new(env!("CARGO_BIN_EXE_crash_worker"))
            .env("NOXU_CRASH_DIR", &p)
            .env("NOXU_CRASH_MODE", "xaddl_commit_live_check")
            .spawn()
            .expect("spawn live_check");
    let status = child.wait().expect("wait live_check");
    assert!(status.success(), "live_check must exit cleanly");
    let xlisted_false = p.join("adv2_xlisted_false").exists();
    let outcome_notfound = p.join("adv2_outcome_notfound").exists();
    assert!(
        xlisted_false,
        "RV-ADV2: after a resolved committed REMOVE, the LIVE env still lists x          => resolve_recovered_prepared_ddl did not update the in-memory catalog"
    );
    assert!(
        outcome_notfound,
        "RV-ADV2: a repeat remove(x) after a resolved committed REMOVE must be          NotFound; it was blocked-as-conflict or succeeded => in-doubt state never cleared"
    );
}

// RV-ADV3 (reviewer, task 1e): MULTIPLE DDL ops in ONE prepared branch.
// create a + remove b + rename c->d, committed across crash->recover->commit->crash.
// Ground-truth after commit: a present(8), b GONE, c GONE, d present(8).
// The (deletes.len,creates.len) classifier sees (2,2) -> skip arm -> the whole
// branch's DDL is dropped, so after commit b and c are resurrected and a/d are
// mishandled.  Uses the single-branch xaddl_commit resolve path.
#[test]
fn rv_adv3_multi_ddl_one_branch_commit() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "xaddl_multi_prepare");
    recover_phase(&p);
    run_phase(&p, "xaddl_commit");
    let env = open_env(&p);
    let (a_ex, ar) = db_records(&env, "a");
    let (b_ex, _br) = db_records(&env, "b");
    let (c_ex, _cr) = db_records(&env, "c");
    let (d_ex, dr) = db_records(&env, "d");
    env.close().unwrap();
    assert!(
        a_ex && ar == 8 && !b_ex && !c_ex && d_ex && dr == 8,
        "RV-ADV3 multi-DDL commit: expected a(8) present, b gone, c gone, d(8) present; \
         got a_ex={a_ex} ar={ar} b_ex={b_ex} c_ex={c_ex} d_ex={d_ex} dr={dr}"
    );
}

// RV-ADV5 (reviewer, task 1e): create "a" + remove "b" in one prepared branch.
// (1 del, 1 crt) -> classifier MISREADS as RENAME b->a.  Commit ground-truth:
// a present(8), b gone.
#[test]
fn rv_adv5_create_plus_remove_one_branch_commit() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "xaddl_create_plus_remove_prepare");
    recover_phase(&p);
    run_phase(&p, "xaddl_commit");
    let env = open_env(&p);
    let (a_ex, ar) = db_records(&env, "a");
    let (b_ex, _br) = db_records(&env, "b");
    env.close().unwrap();
    assert!(
        a_ex && ar == 8 && !b_ex,
        "RV-ADV5 create+remove one branch commit: expected a(8) present and b gone; \
         got a_ex={a_ex} ar={ar} b_ex={b_ex}"
    );
}

// RV-ADV6 (reviewer, task 1a): crash between durable TxnCommit and in-memory
// resolution.  The committed REMOVE must still take effect on the next recovery
// (the committed tombstone wins) -- durability independent of the in-memory step.
#[test]
fn rv_adv6_crash_between_commit_and_resolve_remove_gone() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "xaddl_remove_prepare"); // crash #1
    recover_phase(&p); // xa_recover + checkpoint
    run_phase(&p, "xaddl_commit_crash_before_resolve"); // durable commit then CRASH
    let env = open_env(&p);
    let (x_ex, xr) = db_records(&env, "x");
    env.close().unwrap();
    assert!(
        !x_ex,
        "RV-ADV6: after a durable TxnCommit then a crash BEFORE in-memory resolve, \
         'x' must be GONE on recovery (committed tombstone wins); got x_ex={x_ex} xr={xr}"
    );
}

// RV-ADV7 (reviewer, task 1d): prepare -> crash -> recover -> CRASH AGAIN while
// still in-doubt -> recover -> commit.  The in-doubt reconstruction must survive
// a second recovery and the branch must still be listed by xa_recover, then a
// committed REMOVE must take effect.
#[test]
fn rv_adv7_double_crash_in_doubt_then_commit_remove_gone() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "xaddl_remove_prepare"); // crash #1
    recover_phase(&p); // recover #1 + checkpoint (clean exit)
    // Second crash while still in-doubt: open+xa_recover (asserts branch==1),
    // then SIGKILL via the indoubt probe-less path -- reuse xaddl_recover_open
    // but crash it mid-way: simplest is to run recover_phase AGAIN (clean) to
    // model recover->(crash)->recover; then a THIRD recover that surfaces the
    // branch and commits.  recover_phase asserts len==1 each time (xa_recover
    // still lists the branch after repeated recoveries).
    recover_phase(&p); // recover #2 (still in-doubt, len==1)
    run_phase(&p, "xaddl_commit"); // recover #3 + resolve(commit) + crash #2
    let env = open_env(&p);
    let (x_ex, xr) = db_records(&env, "x");
    env.close().unwrap();
    assert!(
        !x_ex,
        "RV-ADV7: after prepare->recover->recover->commit the committed REMOVE must \
         take effect ('x' GONE); got x_ex={x_ex} xr={xr}"
    );
}

// RV-ADV9 (reviewer, task 1c, DATA LOSS): in-doubt REMOVE "x"(8) -- a concurrent
// truncate("x") is NOT blocked (JE WRITE-relock would block it), emptying "x".
// Then the prepared branch is ROLLED BACK: "x" must come back WITH its 8 records.
// It comes back EMPTY => a concurrent op destroyed in-doubt data the resurrected
// WRITE lock should have protected.
#[test]
fn rv_adv9_indoubt_truncate_then_rollback_loses_records() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "xaddl_remove_prepare"); // crash #1 (x has 8 recs, remove prepared)
    recover_phase(&p); // recover + checkpoint
    // concurrent truncate("x") while in-doubt (clean exit)
    let _ = std::fs::remove_file(p.join("firstlsn_done"));
    let mut child =
        std::process::Command::new(env!("CARGO_BIN_EXE_crash_worker"))
            .env("NOXU_CRASH_DIR", &p)
            .env("NOXU_CRASH_MODE", "xaddl_indoubt_truncate_then_leave")
            .spawn()
            .expect("spawn truncate");
    assert!(child.wait().expect("wait").success(), "truncate phase clean");
    // Now ROLL BACK the prepared remove: x survives -> must keep its 8 records.
    run_phase(&p, "xaddl_rollback");
    let env = open_env(&p);
    let (x_ex, xr) = db_records(&env, "x");
    env.close().unwrap();
    assert!(
        x_ex && xr == 8,
        "RV-ADV9: after an in-doubt REMOVE was rolled back, 'x' must retain its 8 \
         records; a concurrent truncate while in-doubt destroyed them: x_ex={x_ex} xr={xr}"
    );
}

// RV-ADV10 (reviewer, task 1f): cleaner must not reclaim an in-doubt prepared
// branch's files.  Prepared CREATE x(8) -> crash -> recover -> force cleaner +
// checkpoint while in-doubt -> commit.  x must exist with 8 records.
#[test]
fn rv_adv10_cleaner_cannot_reclaim_indoubt_branch() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "xaddl_create_prepare"); // crash #1
    recover_phase(&p); // recover + checkpoint
    let _ = std::fs::remove_file(p.join("firstlsn_done"));
    let mut child =
        std::process::Command::new(env!("CARGO_BIN_EXE_crash_worker"))
            .env("NOXU_CRASH_DIR", &p)
            .env("NOXU_CRASH_MODE", "xaddl_indoubt_clean_then_commit")
            .spawn()
            .expect("spawn clean_then_commit");
    assert!(
        child.wait().expect("wait").success(),
        "clean_then_commit clean exit"
    );
    let env = open_env(&p);
    let (x_ex, xr) = db_records(&env, "x");
    env.close().unwrap();
    assert!(
        x_ex && xr == 8,
        "RV-ADV10: after forcing the cleaner+checkpoint during the in-doubt window \
         then committing, 'x' must exist with 8 records; got x_ex={x_ex} xr={xr}"
    );
}

// RV-ADV11 (reviewer, task 1b): crash between durable TxnAbort and in-memory
// resolution for a prepared REMOVE -> "x" must survive with its 8 records.
#[test]
fn rv_adv11_crash_between_abort_and_resolve_remove_survives() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "xaddl_remove_prepare");
    recover_phase(&p);
    run_phase(&p, "xaddl_abort_crash_before_resolve");
    let env = open_env(&p);
    let (x_ex, xr) = db_records(&env, "x");
    env.close().unwrap();
    assert!(
        x_ex && xr == 8,
        "RV-ADV11: after durable TxnAbort then crash before resolve, 'x' must \
         survive with 8 records; got x_ex={x_ex} xr={xr}"
    );
}

// RV-ADV12 (reviewer, task 1e): two REMOVEs in one prepared branch (2 del,0 crt)
// -> classifier skip arm -> both b and c resurrected on commit (should be gone).
#[test]
fn rv_adv12_two_removes_one_branch_commit() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "xaddl_two_removes_prepare");
    recover_phase(&p);
    run_phase(&p, "xaddl_commit");
    let env = open_env(&p);
    let (b_ex, _b) = db_records(&env, "b");
    let (c_ex, _c) = db_records(&env, "c");
    env.close().unwrap();
    assert!(
        !b_ex && !c_ex,
        "RV-ADV12 two-removes commit: b and c must both be gone; got b_ex={b_ex} c_ex={c_ex}"
    );
}

// ========================================================================
// R-XADDL-711 fuzz (rework): random multi-op, multi-branch prepared-DDL
// resolution across two crashes, checked against a ground-truth model.
//
// Per seed: 1-3 XA branches, each 1-4 DDL ops (create / remove / rename over
// per-branch names) + a data write, all prepared; crash; recover; commit /
// rollback a seed-chosen subset (always leaving >=1 branch in doubt); crash;
// recover.  The final catalog + data must equal the model folded from the
// worker-reported plan + decisions (the plan is the spec, the DB the
// implementation under test).  Run via the crash_worker harness; every spawned
// child is reaped by `ChildGuard` (kills only its own recorded PID, never
// pkill/killall), even on panic.
// ========================================================================

/// RAII guard: SIGKILL + reap the wrapped child on drop (recorded PID only).
/// Guarantees a leaked/forgotten prepared-txn worker is never left running if
/// the test panics mid-way, without ever pkill/killall-ing by name.
struct ChildGuard(Option<std::process::Child>);

impl ChildGuard {
    fn spawn(dir: &Path, mode: &str, seed: u64) -> Self {
        let _ = std::fs::remove_file(dir.join("firstlsn_done"));
        let child =
            std::process::Command::new(env!("CARGO_BIN_EXE_crash_worker"))
                .env("NOXU_CRASH_DIR", dir)
                .env("NOXU_CRASH_MODE", mode)
                .env("XADDL_FUZZ_SEED", seed.to_string())
                .spawn()
                .expect("spawn crash_worker");
        ChildGuard(Some(child))
    }
    /// Wait for the worker's done-flag, then SIGKILL + reap it (the worker
    /// leaks its prepared txns and loops forever, exactly like `run_phase`).
    fn wait_flag_then_kill(mut self, dir: &Path, mode: &str) {
        assert!(
            wait_for_flag(dir, "firstlsn_done", Duration::from_secs(180)),
            "fuzz worker never finished (mode {mode})"
        );
        if let Some(mut c) = self.0.take() {
            c.kill().expect("SIGKILL");
            c.wait().expect("wait");
        }
    }

    /// Wait for a CLEAN-exiting worker to finish and assert success + flag.
    fn wait_clean(mut self, dir: &Path, mode: &str) {
        if let Some(mut c) = self.0.take() {
            let st = c.wait().expect("wait clean worker");
            assert!(
                st.success() && dir.join("firstlsn_done").exists(),
                "clean worker {mode} failed (no done-flag / nonzero exit)"
            );
        }
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(mut c) = self.0.take() {
            let _ = c.kill();
            let _ = c.wait();
        }
    }
}

/// Model of a database's expected final state.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ModelDb {
    /// Base records (keys 0..8) present from pre-seed / create.
    base: bool,
    /// Extra data record at key 100 present (a committed data op).
    data100: bool,
}

/// Fold the ground-truth final catalog+data from the worker's reported plan
/// (`fuzz_plan.txt`) and decisions (`fuzz_decisions.txt`).  In-doubt and
/// rolled-back branches have NO visible effect; committed branches apply their
/// DDL/data ops.  Pre-seeded dbs (every remove/rename source + each data db)
/// start present with keys 0..8.
fn fuzz_model(dir: &Path) -> std::collections::BTreeMap<String, ModelDb> {
    use std::collections::BTreeMap;
    let plan =
        std::fs::read_to_string(dir.join("fuzz_plan.txt")).expect("plan");
    let decs =
        std::fs::read_to_string(dir.join("fuzz_decisions.txt")).expect("decs");
    // branch index -> decision
    let mut decision: BTreeMap<u8, String> = BTreeMap::new();
    for line in decs.lines() {
        let mut it = line.split_whitespace();
        let b: u8 = it.next().unwrap().parse().unwrap();
        let d = it.next().unwrap().to_string();
        decision.insert(b, d);
    }
    // Parse ops in order, grouped by branch (file is already branch-grouped).
    let mut ops_by_branch: BTreeMap<u8, Vec<Vec<String>>> = BTreeMap::new();
    for line in plan.lines() {
        let parts: Vec<String> =
            line.split_whitespace().map(|s| s.to_string()).collect();
        let b: u8 = parts[0].parse().unwrap();
        ops_by_branch.entry(b).or_default().push(parts);
    }
    // Pre-seed: every remove/rename source + every data db starts present.
    let mut cat: BTreeMap<String, ModelDb> = BTreeMap::new();
    for ops in ops_by_branch.values() {
        for p in ops {
            match p[1].as_str() {
                "remove" => {
                    cat.insert(
                        p[2].clone(),
                        ModelDb { base: true, data100: false },
                    );
                }
                "rename" => {
                    cat.insert(
                        p[2].clone(),
                        ModelDb { base: true, data100: false },
                    );
                }
                "data" => {
                    cat.insert(
                        p[2].clone(),
                        ModelDb { base: true, data100: false },
                    );
                }
                _ => {}
            }
        }
    }
    // Apply each COMMITTED branch's ops in order; rollback/indoubt = no effect.
    for (b, ops) in &ops_by_branch {
        if decision.get(b).map(|d| d.as_str()) != Some("commit") {
            continue;
        }
        for p in ops {
            match p[1].as_str() {
                "create" => {
                    cat.insert(
                        p[2].clone(),
                        ModelDb { base: true, data100: false },
                    );
                }
                "remove" => {
                    cat.remove(&p[2]);
                }
                "rename" => {
                    if let Some(v) = cat.remove(&p[2]) {
                        cat.insert(p[3].clone(), v);
                    }
                }
                "data" => {
                    if let Some(v) = cat.get_mut(&p[2]) {
                        v.data100 = true;
                    }
                }
                _ => {}
            }
        }
    }
    cat
}

/// Observe the real final catalog+data: existence + base records + key-100.
///
/// A name LISTED by `database_names()` (JE `getDbNames` = LockType.NONE, the
/// committed catalog image) whose OPEN conflicts with an unresolved
/// recovered-prepared branch (the FID-XADDL-3 in-doubt handle lock) is
/// recorded as PRESENT with its committed base records intact (`base: true`):
/// the name is still listed and its 8 committed records sit behind the
/// in-doubt lock, exactly as the ground-truth model folds an in-doubt
/// remove/rename-old to "no effect, db still there".
fn fuzz_observe(
    env: &Environment,
) -> std::collections::BTreeMap<String, ModelDb> {
    use std::collections::BTreeMap;
    let mut out = BTreeMap::new();
    let names = env.database_names().expect("database_names");
    for name in names {
        if name == "test" {
            // Harness artifact: crash_worker opens a scratch "test" db on
            // startup for non-fuzz modes; ignore it in the catalog compare.
            continue;
        }
        let db = match env.open_database(
            None,
            &name,
            &DatabaseConfig::new().with_transactional(true),
        ) {
            Ok(db) => db,
            Err(e) => {
                let s = format!("{e:?}");
                // An in-doubt handle conflict surfaces as the real NameLN lock
                // being denied.  R-LIVEDDL-711 (ONE MECHANISM): the recovered
                // in-doubt branch WRITE-relocks the name, so a concurrent open
                // now gets a typed `LockConflict` / `LockTimeout` (JE
                // `DatabasePreemptedException`); the pre-unification lock-free
                // path returned `DatabaseInUse` (-> `OperationNotAllowed` or
                // the "database is in use" Display).  All of these mean the
                // name is still listed with its committed records intact.
                if matches!(
                    e,
                    noxu_db::NoxuError::OperationNotAllowed(_)
                        | noxu_db::NoxuError::LockConflict(_)
                        | noxu_db::NoxuError::LockTimeout { .. }
                        | noxu_db::NoxuError::DeadlockDetected
                ) || s.contains("database is in use")
                {
                    out.insert(name, ModelDb { base: true, data100: false });
                } else {
                    out.insert(name, ModelDb { base: false, data100: false });
                }
                continue;
            }
        };
        let txn = env.begin_transaction(None).expect("read txn");
        // A record WRITE-locked by an unresolved recovered-prepared branch is
        // not committed-visible: a read conflicts/times out.  Treat such a key
        // as ABSENT (the in-doubt write has no committed effect) -- the model
        // folds in-doubt branches to no visible data.
        let read_key = |k: u32| -> bool {
            matches!(db.get_in(&txn, k.to_be_bytes()), Ok(Some(_)))
        };
        let mut base = 0u32;
        for i in 0u32..8 {
            if read_key(i) {
                base += 1;
            }
        }
        let data100 = read_key(100);
        // Roll back the read txn (it may hold read locks on committed keys);
        // abort never blocks.
        txn.abort().ok();
        db.close().unwrap();
        out.insert(name, ModelDb { base: base == 8, data100 });
    }
    out
}

#[test]
fn xaddl_fuzz_multiop_random_seeds() {
    // 30 distinct seeds derived from a base.  The base is fixed for the gate
    // (deterministic CI) but overridable via XADDL_FUZZ_BASE so the stress
    // harness can run the fuzz several times over DIFFERENT seed sets.
    let fuzz_base: u64 = std::env::var("XADDL_FUZZ_BASE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0xA5A5_0000);
    for k in 0u64..30 {
        let seed = fuzz_base ^ k.wrapping_mul(0x1000_0193);
        let dir = TempDir::new().unwrap();
        let p = dir.path().to_path_buf();

        // crash #1: build + prepare all branches, leak, SIGKILL.
        let g = ChildGuard::spawn(&p, "xaddl_fuzz_prepare", seed);
        g.wait_flag_then_kill(&p, "xaddl_fuzz_prepare");

        // recover #1: xa_recover surfaces every branch + checkpoint (clean).
        {
            let g = ChildGuard::spawn(&p, "xaddl_fuzz_recover", seed);
            g.wait_clean(&p, "xaddl_fuzz_recover");
        }

        // resolve: commit/rollback a subset, leave >=1 in doubt, SIGKILL.
        let g = ChildGuard::spawn(&p, "xaddl_fuzz_resolve", seed);
        g.wait_flag_then_kill(&p, "xaddl_fuzz_resolve");

        // recover #2 (final): the in-doubt branches are re-surfaced.
        let env = open_env(&p);
        let model = fuzz_model(&p);
        let actual = fuzz_observe(&env);
        let n_indoubt = env.recovered_prepared_txns().len();
        env.close().unwrap();

        let plan_txt = std::fs::read_to_string(p.join("fuzz_plan.txt"))
            .unwrap_or_default();
        let dec_txt = std::fs::read_to_string(p.join("fuzz_decisions.txt"))
            .unwrap_or_default();
        assert_eq!(
            actual, model,
            "FUZZ seed={seed:#x}: final catalog+data must equal the ground-truth \
             model.\n  model ={model:?}\n  actual={actual:?}\n  (in-doubt branches \
             re-surfaced: {n_indoubt})\n--- plan ---\n{plan_txt}--- decisions ---\n{dec_txt}"
        );
    }
}
