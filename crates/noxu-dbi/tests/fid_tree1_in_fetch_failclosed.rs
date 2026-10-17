//! FID-TREE-1: the USER READ path must NOT swallow an interior IN fetch
//! error.  When a committed key's descent must re-fault an interior IN whose
//! log image is unreadable (checksum failure), `CursorImpl::search` MUST fail
//! closed (Err + environment invalidated), NEVER return Ok(NotFound).
//!
//! JE `IN.fetchTarget` / `fetchIN` (IN.java:2825-2843) throw
//! `EnvironmentFailureException(LOG_CHECKSUM / LOG_FILE_NOT_FOUND)` for an
//! unreadable node and invalidate the environment.  Noxu's pre-fix READ path
//! did `fetch_node_from_log().ok()?` -> `None` -> "node absent" -> NOTFOUND
//! for a committed key (a silent wrong result).
//!
//! Deterministic witness: build a deep tree, checkpoint (log all INs), capture
//! a NON-root interior IN's on-disk LSN, evict the whole cache so that IN is
//! non-resident, CRC-corrupt the IN's bytes in its log file, then read a key
//! whose descent must re-fault it.
#![cfg(not(noxu_shuttle))]

use noxu_dbi::{
    CursorImpl, DatabaseConfig, DbiEnvConfig, EnvironmentImpl, OperationStatus,
    PutMode, SearchMode,
};
use noxu_tree::tree::TreeNode;
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn log_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "ndb"))
        .collect();
    v.sort();
    v
}

fn key(i: u32) -> Vec<u8> {
    format!("k{i:06}").into_bytes()
}

/// Walk the leftmost spine; return `(root_id, interior_id, interior_lsn,
/// probe_key)` where `interior` is a NON-root level-2 IN (children are BINs)
/// and `probe_key` is the first key of its leftmost BIN.  `interior_lsn` is the
/// IN's own on-disk LSN as recorded in its parent's slot.
fn find_nonroot_interior_in(
    tree_arc: &Arc<std::sync::RwLock<noxu_tree::Tree>>,
) -> Option<(u64, u64, noxu_util::Lsn, Vec<u8>)> {
    let t = tree_arc.read().unwrap();
    let root = t.get_root()?;
    let root_id = match &*root.read() {
        TreeNode::Internal(n) => n.node_id,
        TreeNode::Bottom(b) => b.node_id,
    };
    // Descend the leftmost spine, tracking each node and its LSN as recorded
    // in its PARENT's slot 0.  `(node, node_id, lsn_in_parent)`.
    let mut cur = Arc::clone(&root);
    let mut cur_id = root_id;
    let mut cur_lsn_in_parent: Option<noxu_util::Lsn> = None;
    loop {
        // Inspect `cur`: is it an IN whose first child is a BIN (=> `cur` is a
        // level-2 interior IN above the BIN layer)?
        let (child0, child0_lsn, child0_is_bin, probe) = {
            let g = cur.read();
            match &*g {
                TreeNode::Internal(n) => {
                    let c0 = n.get_child(0)?;
                    let is_bin = matches!(&*c0.read(), TreeNode::Bottom(_));
                    let probe = if is_bin {
                        match &*c0.read() {
                            TreeNode::Bottom(b) => b.get_full_key(0),
                            _ => None,
                        }
                    } else {
                        None
                    };
                    (Some(c0), n.get_lsn(0), is_bin, probe)
                }
                TreeNode::Bottom(_) => (None, noxu_util::NULL_LSN, false, None),
            }
        };
        if child0_is_bin {
            // `cur` is a level-2 interior IN.  Use it if it is NOT the root.
            if cur_id != root_id
                && let (Some(lsn), Some(p)) = (cur_lsn_in_parent, probe)
            {
                return Some((root_id, cur_id, lsn, p));
            }
            // `cur` is the root (tree only 2 levels) OR missing data: give up
            // on this spine (the fixture must build a deeper tree).
            return None;
        }
        // Descend to child0, recording its LSN (from cur's slot 0) and id.
        let c0 = child0?;
        let c0_id = match &*c0.read() {
            TreeNode::Internal(n) => n.node_id,
            TreeNode::Bottom(b) => b.node_id,
        };
        cur = c0;
        cur_id = c0_id;
        cur_lsn_in_parent = Some(child0_lsn);
    }
}

/// CRC-corrupt the interior IN's log entry: flip a byte a little past its
/// offset (inside the entry body), so `read_entry` fails the checksum.
fn corrupt_in_entry(dir: &Path, lsn: noxu_util::Lsn) {
    let fnum = lsn.file_number();
    let off = lsn.file_offset() as usize;
    for f in log_files(dir) {
        let this = f
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| u32::from_str_radix(s, 16).ok());
        if this == Some(fnum) {
            let mut bytes = std::fs::read(&f).unwrap();
            // Corrupt a wide range starting just past the entry header so the
            // IN body's checksum fails on read, regardless of the exact entry
            // length.  Bounded to the file size.
            let start = (off + 20).min(bytes.len());
            let end = (off + 220).min(bytes.len());
            for b in &mut bytes[start..end] {
                *b ^= 0xFF;
            }
            std::fs::write(&f, bytes).unwrap();
            return;
        }
    }
    panic!("fixture: no log file for IN LSN {lsn:?} (file {fnum:#x})");
}

#[test]
fn fid_tree1_read_committed_key_fails_closed_on_interior_in_fetch_error() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = DbiEnvConfig {
        run_evictor: false,
        run_checkpointer: false,
        run_cleaner: false,
        run_in_compressor: false,
        log_file_max_bytes: 4096,
        node_max_entries: 4,
        ..DbiEnvConfig::default()
    };
    let env = EnvironmentImpl::from_dbi_config(dir.path(), &cfg).unwrap();
    let mut db_cfg = DatabaseConfig::new();
    db_cfg.set_allow_create(true);
    // Small BIN/IN fanout so 600 keys build a tree several levels deep (with a
    // NON-root interior IN above the BIN layer).  The per-DB tree takes its
    // fanout from DatabaseConfig, not DbiEnvConfig.
    db_cfg.set_node_max_entries(4);
    let db = env.open_database("pri", &db_cfg).unwrap();

    {
        let mut cursor = CursorImpl::new(Arc::clone(&db), 1);
        for i in 0..600u32 {
            cursor.put(&key(i), b"v", PutMode::Overwrite).unwrap();
        }
        cursor.close().unwrap();
    }
    env.run_checkpoint().unwrap();
    let lm = env.get_log_manager().unwrap();
    lm.flush_sync().unwrap();

    let tree_arc = db.read().get_real_tree_arc().unwrap();
    let (_root_id, in_id, in_lsn, probe_key) =
        find_nonroot_interior_in(&tree_arc)
            .expect("tree must have a NON-root interior IN above the BINs");
    assert!(!in_lsn.is_null(), "interior IN needs a durable LSN");

    // Close the environment so nothing is resident, then CRC-corrupt the
    // interior IN's on-disk image.  On reopen every node is faulted lazily
    // from the log, so the probe read must re-fetch the corrupt interior IN.
    drop(tree_arc);
    drop(db);
    env.close().unwrap();
    drop(env);
    corrupt_in_entry(dir.path(), in_lsn);

    let env = EnvironmentImpl::from_dbi_config(dir.path(), &cfg).unwrap();
    let db = env.open_database("pri", &db_cfg).unwrap();

    // Read the committed probe key: its descent must re-fault the corrupt
    // interior IN.  This MUST fail closed, never Ok(NotFound).  Wire the
    // env-invalid flag on the cursor exactly as `Database::open_cursor` does
    // in production, so the fail-closed read also invalidates the env.
    let mut cursor = CursorImpl::new(Arc::clone(&db), 2)
        .with_env_invalid(env.is_invalid_flag());
    let r = cursor.search(&probe_key, None, SearchMode::Set);
    eprintln!("FID-TREE-1: in_id={in_id} in_lsn={in_lsn:?} read -> {r:?}");

    match r {
        Ok(OperationStatus::NotFound) => panic!(
            "FID-TREE-1 RED: committed key read as NotFound -- the READ path \
             swallowed the interior IN fetch failure into \"absent\""
        ),
        Ok(OperationStatus::Success) => panic!(
            "FID-TREE-1 fixture: read succeeded; the interior IN re-fault did \
             not hit the corrupted entry (adjust the fixture)"
        ),
        Ok(other) => panic!("unexpected status: {other:?}"),
        Err(e) => {
            let s = format!("{e:?}");
            assert!(
                s.contains("Fetch")
                    || s.contains("Checksum")
                    || s.contains("LogFileNotFound")
                    || s.contains("invalid")
                    || s.contains("Environment")
                    || s.contains("Log"),
                "FID-TREE-1: expected a fail-closed fetch error, got {e:?}"
            );
            assert!(
                !env.is_valid(),
                "FID-TREE-1: an undecidable interior IN fetch failure must \
                 invalidate the environment (JE LOG_CHECKSUM / \
                 LOG_FILE_NOT_FOUND)"
            );
        }
    }
}

/// FID-TREE-1 (invalidation scoping): the TREE's fetch-failure-aware checked
/// path (`search_with_data_checked`, used by the cleaner's slot-currency
/// descent over a POSSIBLY-STALE snapshot LSN) must surface the failure as
/// `Err` but MUST NOT itself invalidate the environment.  JE invalidates only
/// for a fetch through the CURRENT slot under the parent latch
/// (`child_at_or_fetch` / `fetch_root_from_log`); the cleaner reads a snapshot
/// LSN not under the live latch, where an unreadable file (cleaned after the
/// slot moved on) is EXPECTED and must only RETAIN.  The CALLER (the cleaner's
/// file processor, for a live slot) decides whether to invalidate -- not the
/// tree.
#[test]
fn fid_tree1_checked_fetch_path_does_not_invalidate_env() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = DbiEnvConfig {
        run_evictor: false,
        run_checkpointer: false,
        run_cleaner: false,
        run_in_compressor: false,
        log_file_max_bytes: 4096,
        node_max_entries: 4,
        ..DbiEnvConfig::default()
    };
    let env = EnvironmentImpl::from_dbi_config(dir.path(), &cfg).unwrap();
    let mut db_cfg = DatabaseConfig::new();
    db_cfg.set_allow_create(true);
    db_cfg.set_node_max_entries(4);
    let db = env.open_database("pri", &db_cfg).unwrap();
    {
        let mut cursor = CursorImpl::new(Arc::clone(&db), 1);
        for i in 0..600u32 {
            cursor.put(&key(i), b"v", PutMode::Overwrite).unwrap();
        }
        cursor.close().unwrap();
    }
    env.run_checkpoint().unwrap();
    env.get_log_manager().unwrap().flush_sync().unwrap();

    let tree_arc = db.read().get_real_tree_arc().unwrap();
    let (_root_id, _in_id, in_lsn, probe_key) =
        find_nonroot_interior_in(&tree_arc)
            .expect("tree must have a NON-root interior IN above the BINs");

    drop(tree_arc);
    drop(db);
    env.close().unwrap();
    drop(env);
    corrupt_in_entry(dir.path(), in_lsn);

    let env = EnvironmentImpl::from_dbi_config(dir.path(), &cfg).unwrap();
    let db = env.open_database("pri", &db_cfg).unwrap();
    let tree_arc = db.read().get_real_tree_arc().unwrap();

    // The cleaner's checked slot-currency descent surfaces the undecidable
    // fetch failure as `Err` -- but the TREE does not invalidate the env.
    let r = tree_arc.read().unwrap().search_with_data_checked(&probe_key);
    eprintln!(
        "FID-TREE-1 (scoping): search_with_data_checked is_err={}",
        r.is_err()
    );
    assert!(
        r.is_err(),
        "the checked path must surface the fetch failure as Err"
    );
    assert!(
        env.is_valid(),
        "FID-TREE-1 scoping: the TREE's checked (stale-snapshot) fetch path \
         must NOT invalidate the environment -- only a current-slot descent \
         (child_at_or_fetch / fetch_root_from_log) or the cleaner's own \
         live-slot decision may"
    );
}
