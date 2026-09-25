//! Fail-stop acceptance for eviction-log-failure WAL accounting (A-prime).
//!
//! SUPERSEDED EXPECTATION: the predecessor at evidence commit e3cbc0f1
//! asserted a retry-compatible contract (`!is_io_invalid` + a SECOND real
//! failed append + unchanged obsolete count across same-environment retries).
//! Per the WAL fail-stop design review and the explicit user decision to
//! implement JE's environment fail-stop, that contract is INCOMPATIBLE with
//! the fix: the first critical append failure permanently invalidates the log
//! (and the environment), so a same-instance retry is refused rather than
//! re-attempting a real write. This test is the reviewed replacement asserting
//! the NEW contract, retaining the mandatory controls: real production
//! observer accounting, first-failure ZERO obsolete credit, dirty BIN
//! retention, and no-error-exactly-once.
#![cfg(not(noxu_shuttle))]

use noxu_dbi::{
    CursorImpl, DatabaseConfig, DbiEnvConfig, EnvironmentImpl, PutMode,
};
use noxu_log::entry::in_log_entry::InLogEntry;
use noxu_log::faultdisk::{self, FaultController, FaultKind};
use noxu_log::{LogEntryType, ObsoleteLsn, Provisional};
use noxu_tree::tree::TreeNode;
use noxu_util::NULL_LSN;
use std::sync::Arc;

struct FaultReset;
impl Drop for FaultReset {
    fn drop(&mut self) {
        faultdisk::uninstall();
    }
}

// Sole test in its executable: no concurrent faultdisk users or env daemons.
#[test]
fn failed_bin_log_fail_stops_and_credits_no_obsolete() {
    let _reset = FaultReset;
    faultdisk::uninstall();
    let dir = tempfile::tempdir().unwrap();
    let cfg = DbiEnvConfig {
        run_evictor: false,
        run_checkpointer: false,
        run_cleaner: false,
        run_in_compressor: false,
        log_flush_no_sync_interval_ms: 0,
        log_buffer_size: 256,
        ..DbiEnvConfig::default()
    };
    let env = EnvironmentImpl::from_dbi_config(dir.path(), &cfg).unwrap();
    let mut db_cfg = DatabaseConfig::new();
    db_cfg.set_allow_create(true);
    let db = env.open_database("retry", &db_cfg).unwrap();
    let mut cursor = CursorImpl::new(Arc::clone(&db), 1);
    cursor.put(b"key", &[1; 1024], PutMode::Overwrite).unwrap();
    cursor.close().unwrap();
    env.run_checkpoint().unwrap();
    let tree = db.read().get_real_tree_arc().unwrap();
    let bin_arc =
        tree.read().unwrap().search_with_data(b"key").unwrap().bin_arc;
    let old_full = {
        let guard = bin_arc.read();
        let TreeNode::Bottom(bin) = &*guard else { panic!("BIN required") };
        assert!(!bin.dirty);
        assert!(!bin.last_full_lsn.is_null());
        bin.last_full_lsn
    };
    let mut cursor = CursorImpl::new(Arc::clone(&db), 2);
    cursor.put(b"key", &[2; 1024], PutMode::Overwrite).unwrap();
    cursor.close().unwrap();
    let db_id = db.read().get_id().id() as u64;
    let entry = {
        let guard = bin_arc.read();
        let TreeNode::Bottom(bin) = &*guard else { panic!("BIN required") };
        assert!(bin.dirty);
        assert_eq!(bin.last_full_lsn, old_full);
        InLogEntry::new(db_id, old_full, NULL_LSN, bin.serialize_full())
    };
    let mut payload = bytes::BytesMut::with_capacity(entry.log_size());
    entry.write_to_log(&mut payload);
    assert!(payload.len() > cfg.log_buffer_size);
    let lm = env.get_log_manager().unwrap();
    lm.flush_sync().unwrap();
    assert_eq!(lm.read_entry(old_full).unwrap().0, LogEntryType::BIN);
    let tracker = env.get_utilization_tracker().unwrap();
    let obsolete_count = || {
        tracker
            .lock()
            .get_tracked_summary(old_full.file_number())
            .unwrap()
            .get_summary()
            .obsolete_in_count
    };
    let before = obsolete_count();

    // First failed oversized replacement: real DiskFull at pwrite zero.
    faultdisk::install(FaultController::for_test(FaultKind::DiskFull, 0));
    let result = lm.log_tracked(
        LogEntryType::BIN,
        &payload,
        Provisional::No,
        false,
        false,
        Some(db_id as u32),
        Some(ObsoleteLsn::exact(old_full, Some(db_id as u32), 0, false)),
        false,
    );
    assert!(result.is_err(), "pwrite must fail");
    assert_eq!(faultdisk::write_count(), 1);
    faultdisk::uninstall();

    // NEW CONTRACT — first failure permanently invalidates the log:
    assert!(
        lm.is_io_invalid(),
        "a failed critical append fatally invalidates the environment"
    );
    // Zero obsolete credit: no replacement reached the log, so the live full
    // BIN image must not be marked obsolete.
    assert_eq!(
        obsolete_count(),
        before,
        "failed replacement must credit zero obsolete for the live full BIN"
    );
    // Dirty BIN retained: the resident replacement is still the only durable
    // path to the new value; the old durable image at old_full is unchanged.
    {
        let guard = bin_arc.read();
        let TreeNode::Bottom(bin) = &*guard else { panic!("BIN required") };
        assert!(bin.dirty, "dirty BIN must be retained after failed log");
        assert_eq!(bin.last_full_lsn, old_full);
    }
    assert_eq!(
        lm.read_entry(old_full).unwrap().0,
        LogEntryType::BIN,
        "old durable image must remain readable"
    );

    // Subsequent writes are rejected without another kernel write (no
    // same-instance retry). faultdisk is uninstalled, so a real write would
    // succeed; the invalid gate must refuse it before any pwrite.
    let before_writes = faultdisk::write_count();
    let retry = lm.log_tracked(
        LogEntryType::BIN,
        &payload,
        Provisional::No,
        false,
        false,
        Some(db_id as u32),
        Some(ObsoleteLsn::exact(old_full, Some(db_id as u32), 0, false)),
        false,
    );
    assert!(retry.is_err(), "subsequent writes must be rejected after fail-stop");
    assert_eq!(
        faultdisk::write_count(),
        before_writes,
        "rejected write must not touch the kernel"
    );
    assert_eq!(
        obsolete_count(),
        before,
        "rejected write must not credit obsolete either"
    );

    // Public fail-stop surfaces through the environment.
    assert!(
        !env.is_valid(),
        "a fatal log write failure must invalidate the environment"
    );
    // close() must still release resources despite the invalid log.
    let _ = env.close();
}

// Positive control: a successful oversized replacement credits obsolete
// EXACTLY ONCE (no-error-exactly-once), in a fresh healthy environment.
#[test]
fn successful_bin_log_credits_obsolete_exactly_once() {
    let _reset = FaultReset;
    faultdisk::uninstall();
    let dir = tempfile::tempdir().unwrap();
    let cfg = DbiEnvConfig {
        run_evictor: false,
        run_checkpointer: false,
        run_cleaner: false,
        run_in_compressor: false,
        log_flush_no_sync_interval_ms: 0,
        log_buffer_size: 256,
        ..DbiEnvConfig::default()
    };
    let env = EnvironmentImpl::from_dbi_config(dir.path(), &cfg).unwrap();
    let mut db_cfg = DatabaseConfig::new();
    db_cfg.set_allow_create(true);
    let db = env.open_database("ok", &db_cfg).unwrap();
    let mut cursor = CursorImpl::new(Arc::clone(&db), 1);
    cursor.put(b"key", &[1; 1024], PutMode::Overwrite).unwrap();
    cursor.close().unwrap();
    env.run_checkpoint().unwrap();
    let tree = db.read().get_real_tree_arc().unwrap();
    let bin_arc =
        tree.read().unwrap().search_with_data(b"key").unwrap().bin_arc;
    let old_full = {
        let guard = bin_arc.read();
        let TreeNode::Bottom(bin) = &*guard else { panic!("BIN required") };
        bin.last_full_lsn
    };
    let mut cursor = CursorImpl::new(Arc::clone(&db), 2);
    cursor.put(b"key", &[2; 1024], PutMode::Overwrite).unwrap();
    cursor.close().unwrap();
    let db_id = db.read().get_id().id() as u64;
    let entry = {
        let guard = bin_arc.read();
        let TreeNode::Bottom(bin) = &*guard else { panic!("BIN required") };
        InLogEntry::new(db_id, old_full, NULL_LSN, bin.serialize_full())
    };
    let mut payload = bytes::BytesMut::with_capacity(entry.log_size());
    entry.write_to_log(&mut payload);
    assert!(payload.len() > cfg.log_buffer_size);
    let lm = env.get_log_manager().unwrap();
    lm.flush_sync().unwrap();
    let tracker = env.get_utilization_tracker().unwrap();
    let obsolete_count = || {
        tracker
            .lock()
            .get_tracked_summary(old_full.file_number())
            .unwrap()
            .get_summary()
            .obsolete_in_count
    };
    let before = obsolete_count();
    let lsn = lm
        .log_tracked(
            LogEntryType::BIN,
            &payload,
            Provisional::No,
            false,
            false,
            Some(db_id as u32),
            Some(ObsoleteLsn::exact(old_full, Some(db_id as u32), 0, false)),
            false,
        )
        .expect("healthy oversized append must succeed");
    assert!(!lm.is_io_invalid());
    lm.flush_sync().unwrap();
    assert_eq!(lm.read_entry(lsn).unwrap().0, LogEntryType::BIN);
    assert_eq!(
        obsolete_count() - before,
        1,
        "successful replacement credits obsolete exactly once"
    );
    env.close().unwrap();
}
