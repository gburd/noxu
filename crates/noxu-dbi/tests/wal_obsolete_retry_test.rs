//! NOT MERGE-READY: accounting follow-up blocker for eviction-log-failure.
//! Real observer + failed oversized BIN writes must not obsolete the live base.
//! Debug currently panics on retry; release overcounts obsolete INs instead.
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
fn failed_bin_log_retry_preserves_live_obsolete_accounting() {
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
    for attempt in 0..2 {
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
        assert!(result.is_err(), "attempt {attempt}: pwrite must fail");
        assert_eq!(faultdisk::write_count(), 1);
        faultdisk::uninstall();
        assert!(!lm.is_io_invalid());
        eprintln!(
            "attempt {attempt}: old_full={old_full:?}, obsolete before={before}, now={}",
            obsolete_count()
        );
    }
    // No successful replacement exists. Release must not hide the debug
    // duplicate-offset assertion by silently increasing obsolete counts.
    assert_eq!(
        obsolete_count(),
        before,
        "failed replacements cannot obsolete the live full BIN"
    );
    env.close().unwrap();
}
