//! A23 witness: the cleaner must perform a latched currency recheck before it
//! republishes a migrated LN, so it never overwrites the live slot with a
//! stale version (JE `FileProcessor.processFoundLN`,
//! FileProcessor.java:1343-1376,1448-1468: migrate only when `treeLsn ==
//! logLsn` under the BIN latch; a changed slot is Obsolete and nothing is
//! logged).
//!
//! Integrate source: 535e16b9 ("fix(cleaner): latched currency recheck before
//! LN migration append"), test lineage 468b59488e.
//!
//! Deterministic, single-threaded, hook-free: the slot references
//! `cur_lsn`, but the cleaner is asked to migrate the log entry at
//! `old_lsn != cur_lsn` (as if it had sampled an older version and a writer
//! has since advanced the slot). JE requires Obsolete.
//!
//! RED ON BASE (pre-535e16b9): the migrate path only checks `slot == tree_lsn`
//! and then does an unconditional `tree.insert(..)`, so it republishes the
//! stale version over the live slot and returns Migrated.
//! GREEN WITH FIX: the latched publish re-verifies `bin.get_lsn(idx) ==
//! log_lsn` and returns Obsolete, leaving the live slot untouched.

use noxu_cleaner::{MigrationOutcome, RealTreeLookup, TreeLookup};
use noxu_txn::LockManager;
use noxu_util::Lsn;
use std::sync::{Arc, RwLock};

const KEY: &[u8] = b"stale-republish-key";

fn make_tree_with_key(key: &[u8], lsn: Lsn) -> noxu_tree::Tree {
    let tree = noxu_tree::Tree::new(1, 128);
    tree.insert(key.to_vec(), b"live-value".to_vec(), lsn)
        .expect("seed insert should succeed");
    tree
}

/// The cleaner samples an LN at `old_lsn`; by the time it tries to publish the
/// migrated copy the live slot already references `cur_lsn`. The latched
/// currency recheck (log_lsn == tree slot LSN) must make this Obsolete.
#[test]
fn cleaner_stale_republish_is_obsolete_not_migrated() {
    let old_lsn = Lsn::new(3, 300);
    let cur_lsn = Lsn::new(3, 400);
    assert_ne!(old_lsn, cur_lsn);

    // The live slot references cur_lsn (the writer's current version).
    let tree = make_tree_with_key(KEY, cur_lsn);
    let lookup = RealTreeLookup::new(
        Arc::new(RwLock::new(tree)),
        Arc::new(LockManager::new()),
    );

    // The cleaner believes it is migrating the log entry at old_lsn, and (per
    // JE) must only migrate when that still equals the live slot LSN. Here the
    // slot has moved on, so the correct outcome is Obsolete — the stale copy
    // must never be republished over the live slot.
    let outcome = lookup.migrate_ln_slot(1, KEY, old_lsn, cur_lsn);
    assert_eq!(
        outcome,
        MigrationOutcome::Obsolete,
        "cleaner migrated a stale LN (log_lsn {old_lsn:?} != live slot \
         {cur_lsn:?}) over the live slot — latched currency recheck missing"
    );
}

/// Control: when the slot still references the exact log entry being cleaned
/// (`log_lsn == tree_lsn == slot`), migration proceeds as before.
#[test]
fn cleaner_current_slot_still_migrates() {
    let lsn = Lsn::new(3, 300);
    let tree = make_tree_with_key(KEY, lsn);
    let lookup = RealTreeLookup::new(
        Arc::new(RwLock::new(tree)),
        Arc::new(LockManager::new()),
    );

    let outcome = lookup.migrate_ln_slot(1, KEY, lsn, lsn);
    assert_eq!(
        outcome,
        MigrationOutcome::Migrated,
        "a current slot (log_lsn == tree_lsn == slot) must still migrate"
    );
}
