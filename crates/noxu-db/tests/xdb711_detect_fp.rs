//! XDB-DETECT-FP-1 regression: the detector must NOT refuse a HEALTHY env
//! whose catalog history mixes transactional and auto-commit (non-txn NameLN)
//! DDL on one name.
//!
//! This is the coordinator's `coord_xdbdetect_fp_probe` committed as a
//! witness. Each variant writes a legitimate history with the fixed engine,
//! closes cleanly, and reopens; every reopen MUST succeed with the expected
//! catalog. Before the FP-1 fix (predicate (1)+(2)+(3) treating any non-txn
//! live NameLN above a disagreeing committed-txnal event as a stray relog),
//! V1–V4 were refused with LOG_INTEGRITY. See `xdb-detect.md` (FP-1).

use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};

fn cfg(p: &std::path::Path) -> EnvironmentConfig {
    EnvironmentConfig::new(p.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
}
fn dbc() -> DatabaseConfig {
    DatabaseConfig::new().with_allow_create(true).with_transactional(true)
}

fn txn_create(env: &Environment, n: &str) {
    let t = env.begin_transaction(None).unwrap();
    let d = env.open_database(Some(&t), n, &dbc()).unwrap();
    d.put_in(&t, b"k".as_ref(), b"txn".as_ref()).unwrap();
    d.close().unwrap();
    t.commit().unwrap();
}
fn auto_create(env: &Environment, n: &str) {
    let d = env.open_database(None, n, &dbc()).unwrap();
    d.put(b"k", b"auto").unwrap();
    d.close().unwrap();
}
fn txn_remove(env: &Environment, n: &str) {
    let t = env.begin_transaction(None).unwrap();
    env.remove_database(Some(&t), n).unwrap();
    t.commit().unwrap();
}
fn txn_rename(env: &Environment, a: &str, b: &str) {
    let t = env.begin_transaction(None).unwrap();
    env.rename_database(Some(&t), a, b).unwrap();
    t.commit().unwrap();
}

/// Write history `f`, close, reopen; assert the reopen SUCCEEDS and the
/// recovered catalog equals `expect` (sorted names).
fn run(label: &str, expect: &[&str], f: impl Fn(&Environment)) {
    let dir = tempfile::tempdir().unwrap();
    {
        let env = Environment::open(cfg(dir.path())).unwrap();
        f(&env);
        env.close().unwrap();
    }
    match Environment::open(cfg(dir.path())) {
        Ok(env) => {
            let mut names = env.database_names().unwrap();
            names.sort();
            let mut want: Vec<String> =
                expect.iter().map(|s| s.to_string()).collect();
            want.sort();
            assert_eq!(
                names, want,
                "{label}: reopened OK but catalog {names:?} != expected {want:?}"
            );
            env.close().ok();
        }
        Err(e) => panic!("{label}: REOPEN REFUSED (false positive): {e}"),
    }
}

#[test]
fn xdb_detect_false_positive_probe() {
    run("V1 txn-create,auto-remove,auto-create", &["n"], |e| {
        txn_create(e, "n");
        e.remove_database(None, "n").unwrap();
        auto_create(e, "n");
    });
    run("V2 txn-create,txn-remove,auto-create", &["n"], |e| {
        txn_create(e, "n");
        txn_remove(e, "n");
        auto_create(e, "n");
    });
    run("V3 txn-create,txn-rename-away,auto-create", &["m", "n"], |e| {
        txn_create(e, "n");
        txn_rename(e, "n", "m");
        auto_create(e, "n");
    });
    run("V4 auto-create m,txn n,txn-remove n,auto-rename m->n", &["n"], |e| {
        auto_create(e, "m");
        txn_create(e, "n");
        txn_remove(e, "n");
        e.rename_database(None, "m", "n").unwrap();
    });
    run("V5 txn-create,auto-rename n->m,m->n", &["n"], |e| {
        txn_create(e, "n");
        e.rename_database(None, "n", "m").unwrap();
        e.rename_database(None, "m", "n").unwrap();
    });
    run("V0 control: txn-create only", &["n"], |e| {
        txn_create(e, "n");
    });
}
