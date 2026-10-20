// R-LIVEDDL-711 — LIVE: uncommitted remove("x") + concurrent op + ABORT.
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
    assert!(cmd.spawn().expect("spawn").wait().expect("wait").success());
}

fn rd(dir: &Path, pre: &str) -> String {
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let n = e.file_name().to_string_lossy().to_string();
        if let Some(r) = n.strip_prefix(pre) {
            return r.to_string();
        }
    }
    String::new()
}

fn case(op: &str) {
    let dir = TempDir::new().unwrap();
    let p = dir.path().to_path_buf();
    run_clean(&p, "rvx_live_remove_concurrent_then_abort", &[("RVX5_OP", op)]);
    let c = rd(&p, "rvx5_concurrent_");
    let x_ex = rd(&p, "rvx5_x_ex_");
    let xr = rd(&p, "rvx5_xr_");
    eprintln!("RVX5[{op}] concurrent={c} x_ex={x_ex} xr={xr}");
    assert!(
        x_ex == "1" && xr == "8",
        "LIVE remove+concurrent-{op}+abort: x must retain 8 recs; x_ex={x_ex} xr={xr} concurrent={c}"
    );
}

#[test]
fn rvx5_remove_concurrent_truncate_abort() {
    case("truncate");
}
#[test]
fn rvx5_remove_concurrent_remove_abort() {
    case("remove");
}
#[test]
fn rvx5_remove_concurrent_open_abort() {
    case("open");
}

#[allow(dead_code)]
fn _refs() {
    let _ = (Environment::open as fn(_) -> _, DatabaseConfig::new());
    let _ = EnvironmentConfig::new(std::path::PathBuf::new());
}
