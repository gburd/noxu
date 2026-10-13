//! Sorted-duplicate lookups across BIN boundaries (live engine, no recovery).
//!
//! Every sorted-dup "Set"-style lookup (`Get::Search`, `Database::get`,
//! `put_no_overwrite`, `Get::SearchBothRange`, `Get::SearchGte`,
//! `Cursor::count`) seeks `lower_bound(pk)`, which sorts below the
//! identifier key of the BIN holding pk's first duplicate when that
//! duplicate starts a BIN.  The floor BIN then holds nothing >= the seek key
//! and the seek must move on to the next BIN, as JE `Cursor.searchRange`
//! does (Cursor.java:4223-4300 -> `searchRangeAdvanceAndCheckKey`, :4523).
//!
//! The same miss made `je_recovery_ports_tp::complete_removal_dups_recovers`
//! fail once recovery built sorted-dup trees under the two-part comparator
//! (the 56c59f6c guard); the open-time re-sort had only been hiding it.

#![allow(clippy::unwrap_used)]

use noxu_db::{
    CheckpointConfig, DatabaseConfig, DatabaseEntry, Environment,
    EnvironmentConfig, Get, OperationStatus,
};
use std::path::Path;
use tempfile::TempDir;

fn env(dir: &Path) -> Environment {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    cfg.set_run_evictor(false);
    cfg.set_run_in_compressor(false);
    cfg.set_node_max_entries(4);
    Environment::open(cfg).unwrap()
}

fn dup_db(e: &Environment) -> noxu_db::Database {
    let c = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(true);
    e.open_database(None, "dups", &c).unwrap()
}

fn pk(i: u32) -> Vec<u8> {
    format!("p{i:04}").into_bytes()
}

#[test]
fn sorted_dup_lookups_at_bin_boundaries() {
    let tmp = TempDir::new().unwrap();
    let e = env(tmp.path());
    let db = dup_db(&e);
    for i in 0..60 {
        for d in [b"v0", b"v1"] {
            db.put(
                DatabaseEntry::from_bytes(&pk(i)),
                DatabaseEntry::from_bytes(d),
            )
            .unwrap();
        }
    }
    let mut bad: Vec<String> = Vec::new();
    for i in 0..60 {
        let mut c = db.open_cursor(None).unwrap();
        let mut k = DatabaseEntry::from_bytes(&pk(i));
        let mut d = DatabaseEntry::new();
        if c.get(&mut k, &mut d, Get::Search, None).unwrap()
            != OperationStatus::Success
        {
            bad.push(format!("Search {i}"));
        } else {
            match c.count() {
                Ok(2) => {}
                r => bad.push(format!("count {i}: {r:?}")),
            }
        }
        let mut k = DatabaseEntry::from_bytes(&pk(i));
        let mut d = DatabaseEntry::from_bytes(b"v");
        if c.get(&mut k, &mut d, Get::SearchBothRange, None).unwrap()
            != OperationStatus::Success
        {
            bad.push(format!("SearchBothRange {i}"));
        }
        let mut k = DatabaseEntry::from_bytes(&pk(i));
        let mut d = DatabaseEntry::new();
        let s = c.get(&mut k, &mut d, Get::SearchGte, None).unwrap();
        if s != OperationStatus::Success || k.data_opt() != Some(&pk(i)[..]) {
            bad.push(format!("SearchGte {i}"));
        }
        c.close().unwrap();
        if db.get(pk(i)).unwrap().is_none() {
            bad.push(format!("get {i}"));
        }
        if db.put_no_overwrite(pk(i), b"zz").unwrap() {
            bad.push(format!("put_no_overwrite inserted under existing {i}"));
        }
    }
    assert!(bad.is_empty(), "{} wrong lookups: {bad:?}", bad.len());
}

/// Live form of `complete_removal_dups_recovers`: empty the leading BINs of
/// one key's dup chain, insert new dups, then Search/count the key.
fn emptied_chain(checkpoint_and_compress: bool) {
    let tmp = TempDir::new().unwrap();
    let e = env(tmp.path());
    let db = dup_db(&e);
    let key = b"dupkey".to_vec();
    let dk = |i: u32| format!("k{i:08}").into_bytes();
    for i in 0..12 {
        db.put(
            DatabaseEntry::from_bytes(&key),
            DatabaseEntry::from_bytes(&dk(i)),
        )
        .unwrap();
    }
    {
        let mut c = db.open_cursor(None).unwrap();
        let (mut k, mut d) = (DatabaseEntry::new(), DatabaseEntry::new());
        while c.get(&mut k, &mut d, Get::First, None).unwrap()
            == OperationStatus::Success
        {
            assert_eq!(c.delete().unwrap(), OperationStatus::Success);
        }
        c.close().unwrap();
    }
    if checkpoint_and_compress {
        e.checkpoint(Some(&CheckpointConfig::new().with_force(true))).unwrap();
        let _ = e.compress().unwrap();
    }
    for i in 24..29 {
        db.put(
            DatabaseEntry::from_bytes(&key),
            DatabaseEntry::from_bytes(&dk(i)),
        )
        .unwrap();
    }
    let mut c = db.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::from_bytes(&key);
    let mut d = DatabaseEntry::new();
    assert_eq!(
        c.get(&mut k, &mut d, Get::Search, None).unwrap(),
        OperationStatus::Success,
        "Search(key) after re-insert"
    );
    assert_eq!(d.data_opt(), Some(&dk(24)[..]));
    assert_eq!(c.count().unwrap(), 5);
    c.close().unwrap();
}

#[test]
fn sorted_dup_count_after_emptying_leading_bins() {
    emptied_chain(false);
}

#[test]
fn sorted_dup_count_after_emptying_leading_bins_ckpt_compress() {
    emptied_chain(true);
}
