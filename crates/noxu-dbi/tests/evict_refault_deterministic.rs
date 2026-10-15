//! REVIEWER deterministic single-threaded witnesses for
//! BUG-EVICT-REFAULT-DATALOSS (A28), independent of the author's concurrent
//! `evict_refault_witness.rs`.
//!
//! Mechanism under test (bde/cee317/476): the cleaner must not reclaim a log
//! file holding the ONLY on-disk image of a live BIN that the evictor has
//! DETACHED (non-resident).  Deterministic sequence, ZERO concurrency:
//!   1. open a NAMED user DB, all daemons OFF, SMALL EMBEDDED values (so a key
//!      read needs only the BIN image, not a separate LN fetch)
//!   2. fill -> several BINs; checkpoint -> full BIN images logged
//!   3. pick a target BIN; record node_id + current last_full_lsn -> file F
//!   4. force-EVICT the target BIN (arbiter max_memory=1 + evict_memory until
//!      the resident DFS can no longer find it: DETACHED / non-resident)
//!   5. force-CLEAN *only* file F (set_force_clean_files([F]) + do_clean(1,
//!      force=false)); drive the two-checkpoint deletion barrier;
//!      delete_safe_files
//!   6. CLOSE the env, REOPEN the directory, then read every key in the
//!      target BIN's range through a fresh cursor (the close+reopen is
//!      what makes the loss observable: an in-process re-read is masked by
//!      the in-memory log buffer that can still serve a just-deleted file).
//!
//! RED at C1a (42e55cbb): the cleaner's resident-only parent DFS cannot see
//! the evicted BIN, counts F's image dead, F is deleted; after close+reopen
//! the refault of every key in the BIN reads Ok(None) -- 127/127 keys
//! VANISH durably (verified for all three defects, x86 and aarch64).
//! GREEN at A28: get_parent_in_for_logged_child routes by key, faults the
//! evicted child, finds it live, F is migrated/retained.
#![cfg(not(noxu_shuttle))]

use noxu_dbi::{
    CursorImpl, DatabaseConfig, DatabaseImpl, DbiEnvConfig, EnvironmentImpl,
    NodeRwLock, OperationStatus, PutMode, SearchMode, TreeNode,
};
use noxu_tree::tree::Tree;
use noxu_util::dst_sync_pl::RwLock as DbRwLock;
use std::sync::{Arc, RwLock};

// Embedded LN: <= TREE_MAX_EMBEDDED_LN (16). A read only needs the BIN image.
const VAL_LEN: usize = 12;

fn mk_env(dir: &std::path::Path) -> EnvironmentImpl {
    let cfg = DbiEnvConfig {
        run_evictor: false,
        run_checkpointer: false,
        run_cleaner: false,
        run_in_compressor: false,
        log_flush_no_sync_interval_ms: 0,
        evictor_mutate_bins: false,
        max_off_heap_memory: 0,
        log_file_max_bytes: 64 * 1024,
        ..DbiEnvConfig::default()
    };
    EnvironmentImpl::from_dbi_config(dir, &cfg).unwrap()
}

fn put_range(db: &Arc<DbRwLock<DatabaseImpl>>, lo: u32, hi: u32, tag: u8) {
    let mut cursor = CursorImpl::new(Arc::clone(db), 100);
    let val = vec![tag; VAL_LEN];
    for i in lo..hi {
        cursor
            .put(format!("{i:010}").as_bytes(), &val, PutMode::Overwrite)
            .unwrap();
    }
    cursor.close().unwrap();
}

fn enumerate_bins(
    tree: &Arc<RwLock<Tree>>,
) -> Vec<(u64, u64, Vec<u8>, Vec<u8>)> {
    let mut out = Vec::new();
    let root = match tree.read().unwrap().get_root() {
        Some(r) => r,
        None => return out,
    };
    fn walk(
        node: &Arc<NodeRwLock<TreeNode>>,
        out: &mut Vec<(u64, u64, Vec<u8>, Vec<u8>)>,
    ) {
        let g = node.read();
        match &*g {
            TreeNode::Bottom(b) => {
                let first = b.get_full_key(0);
                let last = b.get_full_key(b.keys.len().saturating_sub(1));
                if let (Some(f), Some(l)) = (first, last) {
                    out.push((b.node_id, b.last_full_lsn.as_u64(), f, l));
                }
            }
            TreeNode::Internal(n) => {
                for i in 0..n.entries.len() {
                    if let Some(c) = n.get_child(i) {
                        walk(&c, out);
                    }
                }
            }
        }
    }
    walk(&root, &mut out);
    out
}

fn collect_bin_keys(tree: &Arc<RwLock<Tree>>, want: u64) -> Vec<Vec<u8>> {
    let mut ks = Vec::new();
    let root = tree.read().unwrap().get_root().unwrap();
    fn collect(
        node: &Arc<NodeRwLock<TreeNode>>,
        want: u64,
        out: &mut Vec<Vec<u8>>,
    ) {
        let g = node.read();
        match &*g {
            TreeNode::Bottom(b) => {
                if b.node_id == want {
                    for i in 0..b.keys.len() {
                        if let Some(k) = b.get_full_key(i) {
                            out.push(k);
                        }
                    }
                }
            }
            TreeNode::Internal(n) => {
                for i in 0..n.entries.len() {
                    if let Some(c) = n.get_child(i) {
                        collect(&c, want, out);
                    }
                }
            }
        }
    }
    collect(&root, want, &mut ks);
    ks
}

/// Returns (checked, missing).
fn run_deterministic(
    db_name: &str,
    also_open_primary: bool,
    log_deltas_first: bool,
) -> (usize, usize) {
    let dir = tempfile::tempdir().unwrap();
    let env = mk_env(dir.path());

    if also_open_primary {
        let mut pc = DatabaseConfig::new();
        pc.set_allow_create(true);
        let _primary = env.open_database("primary_small", &pc).unwrap();
        // give the primary a few BINs too so its tree is non-trivial
        let p = env.open_database("primary_small", &pc).unwrap();
        put_range(&p, 0, 2000, 0x11);
        env.run_checkpoint().unwrap();
    }

    let mut db_cfg = DatabaseConfig::new();
    db_cfg.set_allow_create(true);
    let db = env.open_database(db_name, &db_cfg).unwrap();

    let n: u32 = 6000;
    put_range(&db, 0, n, 0xAA);
    env.run_checkpoint().unwrap();

    if log_deltas_first {
        // 476: dirty a few slots per BIN then checkpoint -> BINs logged as
        // DELTAS; the current image under test is a delta / full base.
        let mut cursor = CursorImpl::new(Arc::clone(&db), 101);
        let val = vec![0xAAu8; VAL_LEN];
        let mut i = 0u32;
        while i < n {
            cursor
                .put(format!("{i:010}").as_bytes(), &val, PutMode::Overwrite)
                .unwrap();
            i += 40;
        }
        cursor.close().unwrap();
        env.run_checkpoint().unwrap();
    }

    let tree = db.read().get_real_tree_arc().unwrap();

    let bins = enumerate_bins(&tree);
    assert!(bins.len() >= 3, "need several BINs, got {}", bins.len());
    let mut candidates: Vec<_> =
        bins.iter().filter(|(_, lsn, _, _)| *lsn != 0).cloned().collect();
    candidates.sort_by_key(|(_, lsn, _, _)| *lsn);
    // pick a middle BIN (avoid edges)
    let (bin_id, bin_lsn, first_key, last_key) =
        candidates[candidates.len() / 2].clone();
    let target_file = (bin_lsn >> 32) as u32;

    let range_keys = collect_bin_keys(&tree, bin_id);
    assert!(!range_keys.is_empty(), "target BIN must have keys");

    // Preflight: every target key reads fine BEFORE we touch anything.
    {
        let mut c = CursorImpl::new(Arc::clone(&db), 99);
        for k in &range_keys {
            assert_eq!(
                c.search(k, None, SearchMode::Set).unwrap(),
                OperationStatus::Success,
                "preflight key {:?}",
                String::from_utf8_lossy(k)
            );
            // VALUE CORRECTNESS: the committed value (0xAA x VAL_LEN) must come
            // back intact, not merely present -- a fail-open refetch over a
            // reclaimed/missing file would pass a bare `is_some()` while
            // silently losing the bytes.
            assert_eq!(
                c.get_current_data(),
                Some([0xAAu8; VAL_LEN].as_slice()),
                "preflight key {:?} came back with the WRONG value",
                String::from_utf8_lossy(k)
            );
        }
        c.close().unwrap();
    }

    eprintln!(
        "target BIN node_id={bin_id} lsn={bin_lsn:#x} file={target_file:#x} \
         keys={} first={:?} last={:?}",
        range_keys.len(),
        String::from_utf8_lossy(&first_key),
        String::from_utf8_lossy(&last_key),
    );

    // Step 4: force-EVICT the target BIN until non-resident (detached).
    let evictor = env.get_evictor();
    evictor.get_arbiter().set_max_memory(1);
    let mut detached = false;
    for _ in 0..300 {
        env.evict_memory();
        if tree.read().unwrap().get_parent_in_for_child_in(bin_id).is_none() {
            detached = true;
            break;
        }
    }
    assert!(detached, "target BIN {bin_id} must detach (evicted/non-resident)");

    // Step 5: force-CLEAN *only* file F (force tier, not force=true sweep),
    // drive the two-checkpoint barrier, delete.
    let cleaner = env.get_cleaner().expect("cleaner available");
    cleaner.clear_force_clean_files();
    cleaner.set_force_clean_files([target_file]);
    let res = env.run_cleaner(1, false);
    env.run_checkpoint().unwrap();
    env.run_checkpoint().unwrap();
    let deleted = cleaner.delete_safe_files();
    let target_path = dir.path().join(format!("{target_file:08x}.ndb"));
    let st = cleaner.get_stats();
    eprintln!(
        "clean_res={:?} deleted={deleted} target_file={target_file:#x} \
         F_exists_after={} ins_migrated={} ins_dead={}",
        res.as_ref().map(|r| (r.files_cleaned, r.files_deleted)),
        target_path.exists(),
        st.ins_migrated.load(std::sync::atomic::Ordering::Relaxed),
        st.ins_dead.load(std::sync::atomic::Ordering::Relaxed),
    );

    // Step 6 (DURABILITY): close the env and REOPEN the same directory, then
    // read.  The in-memory log buffer pool can still serve a just-deleted
    // file's bytes (LogManager::read_entry checks the write buffers before the
    // on-disk file, log_manager.rs ~1386), so an in-process re-read would be
    // MASKED by that cache and falsely pass.  A clean close + reopen
    // flushes/recycles those buffers and re-opens the on-disk log, so a BIN
    // whose only durable image lived in the reclaimed file now reads Ok(None).
    // This is exactly the durability the C1 repro notes require
    // (evict-refault-datalosss-repro.md: survives a clean close + reopen).
    //
    // Every strong handle to the first env MUST drop before close or the
    // FileManager keeps the directory lock and the reopen fails with
    // "Environment is locked".
    drop(db);
    drop(tree);
    drop(evictor);
    drop(cleaner);
    env.close().unwrap();
    drop(env);

    let env = mk_env(dir.path());
    let mut reopen_cfg = DatabaseConfig::new();
    reopen_cfg.set_allow_create(false);
    let db = env.open_database(db_name, &reopen_cfg).unwrap();

    let mut cursor = CursorImpl::new(Arc::clone(&db), 103);
    let mut missing = 0usize;
    let checked = range_keys.len();
    for k in &range_keys {
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            cursor.search(k, None, SearchMode::Set)
        }));
        match r {
            // VALUE CORRECTNESS: require the exact committed bytes, not just
            // presence; an empty/wrong value served by a fail-open refetch is
            // the silent form of the data-loss this witness guards.
            Ok(Ok(OperationStatus::Success))
                if cursor.get_current_data()
                    == Some([0xAAu8; VAL_LEN].as_slice()) => {}
            _ => missing += 1,
        }
    }
    let _ = cursor.close();
    eprintln!(
        "RESULT db={db_name} checked={checked} missing={missing} env_valid={}",
        env.is_valid()
    );
    (checked, missing)
}

/// bde (dominant): evicted non-resident live BIN image must survive cleaning.
#[test]
fn det_bde_evicted_live_bin_image_survives_clean() {
    let (checked, missing) = run_deterministic("user_bde", false, false);
    assert!(checked > 0);
    assert_eq!(missing, 0, "{missing}/{checked} committed keys VANISHED (bde)");
}

/// cee317: a NAMED user-DB BIN (with a primary DB also open) must be classified
/// live via the owning tree, not the primary tree.
#[test]
fn det_cee317_user_db_bin_live() {
    let (checked, missing) = run_deterministic("user_cee", true, false);
    assert!(checked > 0);
    assert_eq!(
        missing, 0,
        "{missing}/{checked} committed keys VANISHED (cee317)"
    );
}

/// 476: a BIN logged as a delta (and the full base a newer delta chains onto)
/// must be classified live.
#[test]
fn det_476_delta_chain_image_live() {
    let (checked, missing) = run_deterministic("user_476", false, true);
    assert!(checked > 0);
    assert_eq!(missing, 0, "{missing}/{checked} committed keys VANISHED (476)");
}
