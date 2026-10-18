//! XA-RECOVERED control: resolve a recovered prepared txn WITHOUT a cleaner
//! barrier, then crash before checkpoint.  If keys survive here (control
//! GREEN) but are lost with the barrier (xarec_recovery_witness RED), the loss
//! is caused by the cleaner reclaiming the prepared LNs' file.
use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
use std::path::Path;
use std::time::{Duration, Instant};
use tempfile::TempDir;
fn wait_for_flag(dir: &Path, name: &str, t: Duration) -> bool {
    let d = Instant::now() + t;
    let f = dir.join(name);
    while Instant::now() < d {
        if f.exists() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    false
}
fn run_phase(dir: &Path, mode: &str) {
    let _ = std::fs::remove_file(dir.join("firstlsn_done"));
    let mut c = std::process::Command::new(env!("CARGO_BIN_EXE_crash_worker"))
        .env("NOXU_CRASH_DIR", dir)
        .env("NOXU_CRASH_MODE", mode)
        .spawn()
        .expect("spawn");
    assert!(
        wait_for_flag(dir, "firstlsn_done", Duration::from_secs(120)),
        "worker never finished ({mode})"
    );
    c.kill().expect("SIGKILL");
    c.wait().expect("wait");
}
#[test]
fn xa_recovered_commit_noclean_control() {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_phase(&p, "xarec_prepare");
    run_phase(&p, "xarec_commit_noclean");
    let env = Environment::open(
        EnvironmentConfig::new(p)
            .with_allow_create(false)
            .with_transactional(true),
    )
    .expect("reopen");
    let db = env
        .open_database(
            None,
            "test",
            &DatabaseConfig::new().with_transactional(true),
        )
        .expect("db");
    let txn = env.begin_transaction(None).expect("txn");
    let mut present = 0u32;
    for i in 0u32..8 {
        let want = format!("xa_payload_{i}").into_bytes();
        match db.get_in(&txn, (100_000u32 + i).to_be_bytes()).expect("get") {
            Some(v) if &v[..] == want.as_slice() => present += 1,
            Some(v) => {
                panic!("CONTROL: key {i} WRONG value: {:?} != {:?}", v, want)
            }
            None => {}
        }
    }
    txn.commit().unwrap();
    db.close().unwrap();
    env.close().unwrap();
    assert_eq!(
        present, 8,
        "CONTROL: without the cleaner barrier the 8 XA keys must survive the \
         second crash"
    );
}
