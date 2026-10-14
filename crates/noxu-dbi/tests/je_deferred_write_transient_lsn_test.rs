// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! JE `DeferredWriteTest.testTransientLsn`
//! (test/com/sleepycat/je/test/DeferredWriteTest.java:1678-1713) ported at the
//! crate-internal level.
//!
//! JE asserts, through `DbInternal.getCursorImpl(cursor).getBIN().getLsn(idx)`:
//!   - after `cursor.put(key, data)` on a deferred-write DB, the slot LSN is
//!     TRANSIENT (`DbLsn.isTransient`) — the LN was not logged;
//!   - re-`put`ting the same key reuses the slot and does NOT allocate a new
//!     transient LSN (the slot LSN is unchanged);
//!   - after `db.sync()`, the slot LSN is a REAL (non-transient) LSN.
//!
//! Noxu exposes no public `getLsn`/`isTransient` probe, so this is ported at
//! the noxu-dbi level: the deferred-write put leaves a NULL slot LSN (Noxu's
//! "transient" representation — the LN is never logged), and
//! `EnvironmentImpl::sync_database` (JE `Checkpointer.syncDatabase`) logs the
//! deferred-write LN before the BIN (`log_dirty_lns_in_bin` with the per-DB
//! deferred-write force rule, JE `BIN.logDirtyLN`), stamping a REAL LSN into
//! the slot.  JE's record is a short string ("herococo"), i.e. an EMBEDDED LN,
//! so this exercises the DW-mode force rule that logs even embedded slots.

use noxu_dbi::{
    CursorImpl, DatabaseConfig, DbiEnvConfig, EnvironmentImpl, PutMode,
};
use noxu_tree::tree::TreeNode;
use std::sync::Arc;

#[test]
fn je_deferred_write_test_test_transient_lsn() {
    let dir = tempfile::TempDir::new().unwrap();
    let cfg = DbiEnvConfig {
        transactional: true,
        run_cleaner: false,
        run_checkpointer: false,
        run_in_compressor: false,
        run_evictor: false,
        ..DbiEnvConfig::default()
    };
    let env = EnvironmentImpl::from_dbi_config(dir.path(), &cfg).unwrap();
    let lm = env.get_log_manager().unwrap();

    // Open a DEFERRED-WRITE database.
    let mut dc = DatabaseConfig::new();
    dc.set_allow_create(true);
    dc.deferred_write = true;
    let db = env.open_database("dw", &dc).unwrap();
    let db_id = db.read().get_id().id() as u64;

    let key = b"k1";

    // Read the raw slot LSN for `key` directly from the resident BIN
    // (JE `cursorImpl.getBIN().getLsn(cursorImpl.getIndex())`).
    let slot_lsn = || -> u64 {
        let d = db.read();
        let tree = d.get_real_tree().unwrap();
        let s = tree.search_with_data(key).expect("tree has a root");
        assert!(s.found, "key must be present");
        let g = s.bin_arc.read();
        let TreeNode::Bottom(bin) = &*g else { panic!("not a BIN") };
        bin.get_lsn(s.slot_index).as_u64()
    };

    let mut c = CursorImpl::with_log_manager(Arc::clone(&db), 0, lm);

    // put -> embedded, never-logged: slot LSN is transient (NULL in Noxu).
    c.put(key, b"herococo", PutMode::Overwrite).unwrap();
    assert_eq!(
        slot_lsn(),
        noxu_util::NULL_LSN.as_u64(),
        "deferred-write put must leave a transient (NULL) slot LSN — the LN is \
         not logged until sync"
    );

    // Reuse the slot: must NOT allocate a new transient LSN (JE: newLsn ==
    // currentLsn).  Still transient/NULL.
    c.put(key, b"coco", PutMode::Overwrite).unwrap();
    assert_eq!(
        slot_lsn(),
        noxu_util::NULL_LSN.as_u64(),
        "reusing a deferred-write slot must not assign a new transient LSN"
    );
    let _ = c.close();

    // After sync, the slot LSN must be a REAL (non-transient) LSN — the
    // deferred-write LN was logged by syncDatabase (JE assertFalse(isTransient)).
    env.sync_database(db_id).unwrap();
    assert_ne!(
        slot_lsn(),
        noxu_util::NULL_LSN.as_u64(),
        "after db.sync() an embedded deferred-write slot must carry a REAL \
         (non-transient) LSN (JE testTransientLsn)"
    );

    let _ = env.close();
}
