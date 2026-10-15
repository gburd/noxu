//! A23 RO env-lock (fetch site): once `EnvironmentImpl::close` has closed the
//! FileManager, a cursor retained past close that must fetch a non-resident LN
//! from the log must return an error, not `Success` with empty data.
//!
//! JE: a cursor operation after the environment is closed throws
//! IllegalStateException ("Environment is closed"); and even reaching
//! `IN.fetchLN` on a closed/failed log throws rather than yielding an empty
//! record (IN.java:2826-2833 maps a failed LN read to
//! EnvironmentFailureException, never an empty value).  Here the fetch is
//! refused at the FileManager (closed), which `fetch_ln_data_from_log` must
//! surface as `EnvironmentNotOpen`, never swallow into empty data.
use noxu_dbi::{
    CursorImpl, DatabaseConfig, DbiEnvConfig, EnvironmentImpl, GetMode,
    OperationStatus, PutMode,
};

fn wcfg() -> DbiEnvConfig {
    DbiEnvConfig {
        transactional: true,
        run_cleaner: false,
        run_checkpointer: false,
        run_in_compressor: false,
        ..DbiEnvConfig::default()
    }
}

#[test]
fn closed_file_manager_ln_fetch_is_an_error() {
    let dir = tempfile::TempDir::new().unwrap();
    let cfg = wcfg();
    {
        let env = EnvironmentImpl::from_dbi_config(dir.path(), &cfg).unwrap();
        let lm = env.get_log_manager().unwrap();
        let mut dc = DatabaseConfig::new();
        dc.set_allow_create(true);
        let db = env.open_database("foo", &dc).unwrap();
        let mut c = CursorImpl::with_log_manager(db, 0, lm.clone());
        // 300-byte values are non-embedded: after a reopen their LN data is
        // not resident in the BIN and must be fetched from the log.
        for i in 0..50u32 {
            c.put(&i.to_be_bytes(), &[7u8; 300], PutMode::Overwrite).unwrap();
        }
        c.close().unwrap();
        lm.flush_sync().unwrap();
        env.run_checkpoint().unwrap();
        env.close().unwrap();
    }

    // Reopen writable so the env has a LogManager and the cursor can fetch
    // non-resident LNs from the log (a read-only env has no LogManager, so it
    // never exercises the fetch path this guards).
    let env = EnvironmentImpl::from_dbi_config(dir.path(), &cfg).unwrap();
    let lm = env.get_log_manager().unwrap();
    let db = env.open_database("foo", &DatabaseConfig::new()).unwrap();
    let mut c = CursorImpl::with_log_manager(db, 0, lm.clone());

    // Control: before close, stepping onto a non-resident record fetches the
    // full value from the log.
    assert!(matches!(c.get_first().unwrap(), OperationStatus::Success));
    assert_eq!(c.get_current().unwrap().1.len(), 300, "control: full value");
    let s = c.retrieve_next(GetMode::Next).unwrap();
    assert!(matches!(s, OperationStatus::Success), "control: {s:?}");
    assert_eq!(
        c.get_current().unwrap().1.len(),
        300,
        "control: full value after a log fetch"
    );

    // Close the environment: `EnvironmentImpl::close` closes the FileManager.
    // The cursor is retained (holds its own `Arc<LogManager>`).
    env.close().unwrap();
    assert!(lm.file_manager().is_closed(), "env close must close the FM");

    // Stepping onto the next non-resident record now forces a fetch through
    // the closed FileManager.  It must be an error, never `Success` with an
    // empty / stale value.
    match c.retrieve_next(GetMode::Next) {
        Err(_) => {}
        Ok(st) => panic!(
            "fetch through a closed FileManager returned {st:?}, value len {:?}",
            c.get_current().map(|(_, v)| v.len()).ok()
        ),
    }
}
