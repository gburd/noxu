//! Dirty BIN eviction must fail closed when its new WAL image cannot be logged.
//!
//! Two distinct refusal contracts (A-prime fail-stop):
//! - missing logger: no critical append began, so eviction refusal is
//!   RETRYABLE and the environment stays valid (attach a logger and retry).
//! - real logger, failed oversized pwrite: a critical append failure PERMANENTLY
//!   invalidates the environment (JE serialLog fail-stop); the dirty BIN is
//!   retained, no obsolete is credited, and no same-instance retry is allowed.
#![cfg(not(noxu_shuttle))]

use noxu_dbi::{
    CursorImpl, DatabaseConfig, DbiEnvConfig, EnvironmentImpl, PutMode,
};
use noxu_evictor::{Arbiter, CacheMode, EvictionSource, Evictor};
use noxu_log::faultdisk::{self, FaultController, FaultKind};
use noxu_tree::tree::TreeNode;
use std::sync::{Arc, Mutex, atomic::AtomicI64};

// faultdisk is process-global. Every test in this integration-test executable
// takes this lock; no environment daemons run while the controller is installed.
static FAULT_LOCK: Mutex<()> = Mutex::new(());
struct FaultReset;
impl Drop for FaultReset {
    fn drop(&mut self) {
        faultdisk::uninstall();
    }
}

fn exercise_refusal(missing_logger: bool, dirty_lru: bool, dirty: bool) {
    let _lock = FAULT_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    let _reset = FaultReset;
    faultdisk::uninstall();
    let dir = tempfile::tempdir().unwrap();
    let cfg = DbiEnvConfig {
        run_evictor: false,
        run_checkpointer: false,
        run_cleaner: false,
        run_in_compressor: false,
        log_flush_no_sync_interval_ms: 0,
        evictor_mutate_bins: false,
        max_off_heap_memory: 0,
        // A full BIN is larger than a buffer: eviction must perform a real
        // positioned write inside log_tracked, rather than just buffer bytes.
        log_buffer_size: 256,
        ..DbiEnvConfig::default()
    };
    let env = EnvironmentImpl::from_dbi_config(dir.path(), &cfg).unwrap();
    let mut db_cfg = DatabaseConfig::new();
    db_cfg.set_allow_create(true);
    let db = env.open_database("log_failure", &db_cfg).unwrap();
    let mut cursor = CursorImpl::new(Arc::clone(&db), 1);
    for i in 0..20u32 {
        cursor
            .put(&i.to_be_bytes(), &vec![i as u8; 1024], PutMode::Overwrite)
            .unwrap();
    }
    cursor.close().unwrap();
    drop(cursor);
    env.run_checkpoint().unwrap();
    let tree = db.read().get_real_tree_arc().unwrap();
    let (id, old_full) = {
        let t = tree.read().unwrap();
        let result = t.search_with_data(&0u32.to_be_bytes()).unwrap();
        let guard = result.bin_arc.read();
        let TreeNode::Bottom(bin) = &*guard else { panic!("BIN required") };
        assert!(!bin.last_full_lsn.is_null());
        assert!(!bin.dirty);
        assert!(bin.serialize_full().len() > cfg.log_buffer_size);
        (bin.node_id, bin.last_full_lsn)
    };
    if dirty {
        let mut cursor = CursorImpl::new(Arc::clone(&db), 2);
        for i in 0..20u32 {
            cursor
                .put(
                    &i.to_be_bytes(),
                    &vec![100 + i as u8; 1024],
                    PutMode::Overwrite,
                )
                .unwrap();
        }
        cursor.close().unwrap();
    }
    let lm = env.get_log_manager().unwrap();
    lm.flush_sync().unwrap();
    let usage = Arc::new(AtomicI64::new(100_000));
    // Use the real tree and logger, but isolate the candidate and budget from
    // unrelated nodes. do_evict supplies production lookup/detach callbacks.
    let mut evictor = Evictor::new(Arbiter::new(1, usage, 1, 0), 8, false)
        .with_tree(Arc::clone(&tree), db.read().get_id().id() as u64)
        .with_mutate_bins(false)
        .with_use_dirty_lru(dirty_lru);
    if !missing_logger {
        evictor = evictor.with_log_manager(Arc::clone(&lm));
    }
    evictor.note_ins_added(id, CacheMode::Default);
    if dirty_lru {
        // Real repopulation can leave a candidate in both primary and pri2.
        evictor.pri2_insert_for_test(id);
    }
    let assert_values = || {
        for i in 0..20u32 {
            let expected =
                vec![if dirty { 100 + i as u8 } else { i as u8 }; 1024];
            assert_eq!(
                tree.read()
                    .unwrap()
                    .search_with_data(&i.to_be_bytes())
                    .unwrap()
                    .data,
                Some(expected.into()),
                "key {i}"
            );
        }
    };
    if dirty {
        let dirty_count = {
            let t = tree.read().unwrap();
            let found = t.search_with_data(&0u32.to_be_bytes()).unwrap();
            let guard = found.bin_arc.read();
            let TreeNode::Bottom(bin) = &*guard else { panic!("BIN required") };
            assert!(bin.dirty);
            assert!(bin.dirty_count() > 0);
            bin.dirty_count()
        };
        // missing_logger: eviction refuses because there is NO logger — no
        // critical append ever began, so the refusal stays RETRYABLE and the
        // environment is never invalidated (repeat many times to prove it).
        // real-logger: the first failed oversized pwrite is a critical append
        // failure, so under A-prime fail-stop it PERMANENTLY invalidates the
        // environment; there is exactly ONE attempt and no same-instance retry.
        let attempts = if missing_logger { 32 } else { 1 };
        for attempt in 0..attempts {
            let oversized_before = lm.get_stats().n_temp_buffer_writes;
            if !missing_logger {
                faultdisk::install(FaultController::for_test(
                    FaultKind::DiskFull,
                    0,
                ));
            }
            let result = evictor.do_evict(EvictionSource::Manual);
            if !missing_logger {
                assert_eq!(
                    faultdisk::write_count(),
                    1,
                    "one actual failing pwrite before fail-stop"
                );
                assert_eq!(
                    lm.get_stats().n_temp_buffer_writes,
                    oversized_before + 1
                );
                faultdisk::uninstall();
                // A-prime fail-stop: the failed critical append fatally
                // invalidates the environment (JE invalidates on any
                // serialLog failure).
                assert!(
                    lm.is_io_invalid(),
                    "a failed oversized append fatally invalidates the log"
                );
            }
            // Check data first: on the unfixed code this refaults the OLD image
            // and reports the exact lost value, without close/checkpoint repair.
            assert_values();
            assert!(
                tree.read().unwrap().get_parent_in_for_child_in(id).is_some(),
                "resident after refusal {attempt}"
            );
            let t = tree.read().unwrap();
            let found = t.search_with_data(&0u32.to_be_bytes()).unwrap();
            let guard = found.bin_arc.read();
            let TreeNode::Bottom(bin) = &*guard else { panic!("BIN required") };
            assert_eq!(bin.node_id, id);
            assert!(bin.dirty);
            assert_eq!(bin.dirty_count(), dirty_count);
            assert_eq!(bin.last_full_lsn, old_full);
            assert_eq!(result.nodes_evicted, 0);
            assert_eq!(result.bytes_evicted, 0);
            assert_eq!(evictor.get_arbiter().get_cache_usage(), 100_000);
            assert_eq!(
                evictor.get_policy_sizes(),
                if dirty_lru { (0, 0, 1) } else { (1, 0, 0) }
            );
            let stats = evictor.get_stats();
            assert_eq!(stats.get(&stats.nodes_evicted), 0);
            assert_eq!(stats.get(&stats.dirty_nodes_evicted), 0);
            assert_eq!(stats.get(&stats.bytes_evicted_manual), 0);
            assert!(stats.get(&stats.nodes_put_back) > attempt);
        }
        if !missing_logger {
            // Fail-stop is terminal: a second eviction attempt must be refused
            // by the invalidated log without another pwrite, and must not
            // double-count obsolete or evict the dirty BIN.
            let writes_before = faultdisk::write_count();
            let result = evictor.do_evict(EvictionSource::Manual);
            assert_eq!(result.nodes_evicted, 0, "no eviction after fail-stop");
            assert_eq!(
                faultdisk::write_count(),
                writes_before,
                "rejected retry must not touch the kernel"
            );
            assert!(
                tree.read().unwrap().get_parent_in_for_child_in(id).is_some(),
                "dirty BIN retained after fail-stop"
            );
            assert!(
                !env.is_valid(),
                "environment invalid after fatal log write"
            );
            assert_values();
            let _ = env.close();
            return;
        }
        if missing_logger {
            // Existing builder preserves the candidate lists; no reinsertion.
            evictor = evictor.with_log_manager(Arc::clone(&lm));
        }
    }
    let result = evictor.do_evict(EvictionSource::Manual);
    assert_eq!(result.nodes_evicted, 1);
    assert!(result.bytes_evicted > 0);
    assert_eq!(
        evictor.get_arbiter().get_cache_usage(),
        100_000 - result.bytes_evicted as i64
    );
    assert_eq!(evictor.get_policy_sizes(), (0, 0, 0));
    assert!(
        tree.read().unwrap().get_parent_in_for_child_in(id).is_none(),
        "must really detach before refault"
    );
    assert_eq!(
        evictor.get_stats().get(&evictor.get_stats().dirty_nodes_evicted),
        u64::from(dirty)
    );
    lm.flush_sync().unwrap();
    assert_values(); // actual disk refault, no checkpoint or close before this
    env.close().unwrap();
}

#[test]
fn eviction_log_error_primary_retains_updates_and_retries() {
    exercise_refusal(false, false, true);
}

#[test]
fn eviction_log_error_pri2_retains_updates_and_retries() {
    exercise_refusal(false, true, true);
}

#[test]
fn eviction_missing_logger_primary_retains_updates_and_retries() {
    exercise_refusal(true, false, true);
}

#[test]
fn eviction_missing_logger_pri2_retains_updates_and_retries() {
    exercise_refusal(true, true, true);
}

#[test]
fn eviction_clean_bin_needs_no_logger() {
    exercise_refusal(true, false, false);
}
