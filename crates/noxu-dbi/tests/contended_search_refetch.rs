//! D1 residual: after a contended read lock, `CursorImpl::search` re-reads
//! the slot.  A non-resident LN (slot data `None`: a >16 B value after reopen
//! or an evictor strip) must then be fetched from the log, exactly as on the
//! uncontended path.  JE `CursorImpl.lockLN` re-reads the slot after a
//! blocking lock (CursorImpl.java:3636-3680) and `getCurrent` fetches a
//! non-resident LN via `bin.fetchLN` (CursorImpl.java:2250-2251,
//! IN.java:2767-2768).  Pre-fix the re-read turned `None` into an empty value:
//! `Set`/`SetRange` returned `Success` with 0 bytes and `Both` returned
//! `NotFound` for a present record.
//!
//! Deterministic schedule: a foreign locker holds WRITE on the slot LSN; the
//! reader blocks in `lock_ln`; once it is queued as a waiter the foreign lock
//! is released, so `lock_ln` reports contention and the re-read path runs.
use noxu_dbi::{
    CursorImpl, DatabaseConfig, DbiEnvConfig, EnvironmentImpl, OperationStatus,
    PutMode, SearchMode,
};
use noxu_txn::LockType;
use std::sync::Arc;
use std::time::{Duration, Instant};

const FOREIGN: i64 = 4_242_424;

fn val(i: u32) -> Vec<u8> {
    let mut v = format!("contended-value-{i:04}-").into_bytes();
    v.resize(300, b'x');
    v
}

/// Uncontended control read of record `i` (full value expected).
fn cur_value(
    db: &Arc<noxu_util::dst_sync_pl::RwLock<noxu_dbi::DatabaseImpl>>,
    lm: &Arc<noxu_log::LogManager>,
    locks: &Arc<noxu_txn::LockManager>,
    i: u32,
    mode: SearchMode,
) -> Vec<u8> {
    let mut c = CursorImpl::with_log_manager(Arc::clone(db), 0, lm.clone())
        .with_lock_manager(Arc::clone(locks));
    let data = matches!(mode, SearchMode::Both).then(|| val(i));
    let st = c.search(&i.to_be_bytes(), data.as_deref(), mode).unwrap();
    assert_eq!(st, OperationStatus::Success, "control {mode:?}");
    let v = c.get_current().unwrap().1.to_vec();
    let _ = c.close();
    v
}

#[test]
fn contended_search_refetches_non_resident_ln() {
    let dir = tempfile::TempDir::new().unwrap();
    let cfg = DbiEnvConfig {
        transactional: true,
        run_cleaner: false,
        run_checkpointer: false,
        run_in_compressor: false,
        run_evictor: false,
        lock_timeout_ms: 20_000,
        ..DbiEnvConfig::default()
    };
    {
        let env = EnvironmentImpl::from_dbi_config(dir.path(), &cfg).unwrap();
        let lm = env.get_log_manager().unwrap();
        let mut dc = DatabaseConfig::new();
        dc.set_allow_create(true);
        let db = env.open_database("foo", &dc).unwrap();
        let mut c = CursorImpl::with_log_manager(db, 0, lm.clone());
        for i in 0..20u32 {
            c.put(&i.to_be_bytes(), &val(i), PutMode::Overwrite).unwrap();
        }
        c.close().unwrap();
        lm.flush_sync().unwrap();
        env.run_checkpoint().unwrap();
        env.close().unwrap();
    }
    let env = EnvironmentImpl::from_dbi_config(dir.path(), &cfg).unwrap();
    let lm = env.get_log_manager().unwrap();
    let locks = Arc::clone(env.get_lock_manager());
    let db = env.open_database("foo", &DatabaseConfig::new()).unwrap();
    // Control: the uncontended path fetches a non-resident LN.
    for (i, mode) in [
        (15u32, SearchMode::Set),
        (16, SearchMode::SetRange),
        (17, SearchMode::Both),
    ] {
        let d = db.read();
        let tree = d.get_real_tree().unwrap();
        let slot = tree.search_with_data(&i.to_be_bytes()).unwrap();
        assert!(slot.found && slot.data.is_none(), "control fixture {i}");
        drop(tree);
        drop(d);
        assert_eq!(cur_value(&db, &lm, &locks, i, mode), val(i), "{mode:?}");
    }
    let mut bad = Vec::new();
    for (i, mode) in [
        (3u32, SearchMode::Set),
        (7, SearchMode::SetRange),
        (11, SearchMode::Both),
    ] {
        let key = i.to_be_bytes().to_vec();
        let lsn = {
            let d = db.read();
            let slot =
                d.get_real_tree().unwrap().search_with_data(&key).unwrap();
            assert!(slot.found, "fixture {mode:?}");
            assert!(slot.data.is_none(), "fixture {mode:?}: slot resident");
            slot.lsn
        };
        locks.lock(lsn, FOREIGN, LockType::Write, false, false).unwrap();
        let mut c =
            CursorImpl::with_log_manager(Arc::clone(&db), 0, lm.clone())
                .with_lock_manager(Arc::clone(&locks));
        let want = val(i);
        let reader = std::thread::spawn(move || {
            let data = matches!(mode, SearchMode::Both).then_some(want);
            let st = c.search(&key, data.as_deref(), mode);
            let cur = c.get_current().map(|(_, v)| v.to_vec());
            let _ = c.close();
            (st, cur)
        });
        let deadline = Instant::now() + Duration::from_secs(20);
        while locks.get_lock_info(lsn).1 == 0 {
            assert!(Instant::now() < deadline, "{mode:?}: reader never queued");
            std::thread::sleep(Duration::from_millis(1));
        }
        locks.release(lsn, FOREIGN).unwrap();
        let (st, cur) = reader.join().unwrap();
        let got = (
            st.map_err(|e| e.to_string()),
            cur.map(|v| v.len()).map_err(|e| e.to_string()),
        );
        if got != (Ok(OperationStatus::Success), Ok(300)) {
            bad.push(format!("{mode:?}: {got:?}"));
        } else {
            assert_eq!(cur_value(&db, &lm, &locks, i, mode), val(i));
        }
    }
    assert!(bad.is_empty(), "contended re-read lost the LN value: {bad:?}");
    assert!(env.is_valid());
    env.close().unwrap();
}
