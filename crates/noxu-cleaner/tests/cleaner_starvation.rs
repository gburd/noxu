//! A retained file must not starve unrelated candidates (including one-file
//! daemon passes). Adapted from the independent cleaner-types review probe.
use noxu_cleaner::{Cleaner, UtilizationTracker, UtilizationTrackerObserver};
use noxu_log::{FileManager, LogEntryType, LogManager, Provisional};
use noxu_sync::Mutex;
use std::sync::Arc;

fn prepare_does_not_block_progress(include_prepare: bool, budget: u32) {
    let dir = tempfile::tempdir().unwrap();
    let fm = Arc::new(FileManager::new(dir.path(), false, 4096, 100).unwrap());
    let tracker = Arc::new(Mutex::new(UtilizationTracker::new(true)));
    let mut lm = LogManager::new(Arc::clone(&fm), 3, 8192, 4096);
    lm.set_write_observer(Arc::new(UtilizationTrackerObserver::new(
        Arc::clone(&tracker),
    )));
    let mut payload = Vec::new();
    noxu_log::entry::TxnPrepareEntry::new(7, 0, 0, 0, 1, vec![1], vec![2])
        .unwrap()
        .write_to_log(&mut payload);
    // Control changes only the type, not the serialized Prepare payload.
    let kind = if include_prepare {
        LogEntryType::TxnPrepare
    } else {
        LogEntryType::Trace
    };
    lm.log(kind, &payload, Provisional::No, true, true).unwrap();
    let commit = noxu_log::entry::TxnEndEntry::new_commit(
        7,
        noxu_util::NULL_LSN,
        0,
        0,
        noxu_util::NULL_VLSN,
    );
    let mut buf = bytes::BytesMut::new();
    commit.write_to_log(&mut buf);
    lm.log(LogEntryType::TxnCommit, &buf, Provisional::No, true, true)
        .unwrap();
    for _ in 0..2 {
        fm.flip_file().unwrap();
        // A fresh pool prevents an old buffer from writing into the prior file.
        lm = LogManager::new(Arc::clone(&fm), 3, 8192, 4096);
        lm.set_write_observer(Arc::new(UtilizationTrackerObserver::new(
            Arc::clone(&tracker),
        )));
        let lsn = lm
            .log(LogEntryType::Trace, b"supported", Provisional::No, true, true)
            .unwrap();
        assert_eq!(lsn.file_number(), fm.get_current_file_num());
    }
    let retained_path = dir.path().join("00000000.ndb");
    let retained_bytes = std::fs::read(&retained_path).unwrap();
    let active_path = dir.path().join("00000002.ndb");
    let active_bytes = std::fs::read(&active_path).unwrap();
    let cleaner = Cleaner::with_file_manager_and_tree(
        50,
        0,
        1,
        Arc::clone(&fm),
        Arc::new(std::sync::RwLock::new(noxu_tree::Tree::new(1, 128))),
        Arc::new(lm),
    )
    .with_utilization_tracker(Arc::clone(&tracker));
    cleaner.add_file_to_clean(0);
    let mut deleted = 0;
    for pass in 0..3 {
        let result = cleaner.do_clean(budget, true);
        eprintln!("prepare={include_prepare}, budget={budget}, pass={pass}: {result:?}");
        if include_prepare {
            let error = result.expect_err("unsupported Prepare must remain visible");
            assert!(error.contains("unsupported Prepare"), "{error}");
            let selector = cleaner.get_file_selector_stats();
            assert_eq!(selector.to_be_cleaned, 1, "retry remains visible");
            assert_eq!(selector.being_cleaned, 0, "no stuck file");
            assert_eq!(std::fs::read(&retained_path).unwrap(), retained_bytes);
            assert!(tracker.lock().get_tracked_files().contains_key(&0));
        } else {
            assert!(result.is_ok(), "Trace control: {result:?}");
        }
        for _ in 0..2 {
            let state = cleaner.get_checkpoint_start_state();
            cleaner.after_checkpoint(&state);
        }
        deleted += cleaner.delete_safe_files();
        assert_eq!(std::fs::read(&active_path).unwrap(), active_bytes);
        if include_prepare || budget > 1 || pass > 0 {
            assert!(
                !dir.path().join("00000001.ndb").exists(),
                "independently eligible file 1 must be physically reclaimed"
            );
        }
    }
    assert_eq!(deleted, if include_prepare { 1 } else { 2 });
    assert_eq!(cleaner.get_stats().snapshot().deletions, deleted as u64);
    assert_eq!(
        cleaner.get_stats().snapshot().entries_read,
        if include_prepare { 1 } else { 3 },
        "real file processing, not the no-entry fallback"
    );
    assert_eq!(
        fm.list_file_numbers().unwrap(),
        if include_prepare { vec![0, 2] } else { vec![2] }
    );
    assert!(!tracker.lock().get_tracked_files().contains_key(&1));
}

#[test]
fn resolved_prepare_retained_without_starving_supported_file() {
    prepare_does_not_block_progress(true, 20);
}

#[test]
fn resolved_prepare_does_not_starve_one_file_passes() {
    prepare_does_not_block_progress(true, 1);
}

#[test]
fn trace_substitution_reclaims_the_same_candidate() {
    prepare_does_not_block_progress(false, 20);
    prepare_does_not_block_progress(false, 1);
}
