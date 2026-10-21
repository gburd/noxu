#![cfg(all(unix, not(noxu_shuttle)))]
use noxu_db::{DatabaseConfig, Durability, Environment, EnvironmentConfig};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
const T: u64 = 3000;
#[test]
fn locktimeout_explicit_txn_reaches_configured_value() {
    let dir = TempDir::new().unwrap();
    let env = Arc::new(Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true).with_transactional(true)
            .with_durability(Durability::COMMIT_NO_SYNC).with_lock_timeout(T)).unwrap());
    let db = Arc::new(env.open_database(None, "t",
        &DatabaseConfig::new().with_allow_create(true).with_transactional(true)).unwrap());
    db.put(b"k", b"v0").unwrap();
    let holding = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let (eh, dh, h, r) = (Arc::clone(&env), Arc::clone(&db), Arc::clone(&holding), Arc::clone(&release));
    let holder = std::thread::spawn(move || {
        let txn = eh.begin_transaction(None).unwrap();
        dh.put_in(&txn, b"k", b"held").unwrap();
        h.store(true, Ordering::SeqCst);
        while !r.load(Ordering::SeqCst) { std::thread::sleep(Duration::from_millis(10)); }
        txn.commit().unwrap();
    });
    while !holding.load(Ordering::SeqCst) { std::thread::yield_now(); }
    // EXPLICIT txn waiter
    let txn2 = env.begin_transaction(None).unwrap();
    let start = Instant::now();
    let res = db.put_in(&txn2, b"k", b"explicit");
    let waited = start.elapsed();
    release.store(true, Ordering::SeqCst);
    holder.join().unwrap();
    let _ = txn2.abort();
    eprintln!("LOCKTIMEOUT-EXPLICIT: configured={}ms waited={}ms result={:?}",
        T, waited.as_millis(), res.as_ref().map(|_| "Ok").map_err(|e| e.to_string()));
    assert!(!(res.is_err() && waited < Duration::from_millis(800)),
        "explicit txn timed out at ~{}ms despite with_lock_timeout({})", waited.as_millis(), T);
    db.close().unwrap();
    Arc::try_unwrap(env).ok().unwrap().close().unwrap();
}
