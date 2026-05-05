//! Integration tests for LN (Leaf Node) types.
//!
//! These tests verify that all LN implementations work correctly
//! without depending on other tree components that may not be complete.

use noxu_tree::{FileSummary, FileSummaryLn, Ln, MapLn, NameLn};
use noxu_tree::{
    make_uncached_ln, make_uncached_ln_from_bytes, make_versioned_ln,
};
use noxu_util::Vlsn;

#[test]
fn test_ln_basic() {
    let data = b"test data".to_vec();
    let ln = Ln::new(Some(data.clone()));

    assert_eq!(ln.get_data(), Some(data.as_slice()));
    assert!(!ln.is_deleted());
    assert!(ln.is_dirty());
}

#[test]
fn test_ln_deleted() {
    let ln = Ln::new_deleted();

    assert!(ln.is_deleted());
    assert_eq!(ln.get_data(), None);
}

#[test]
fn test_ln_roundtrip() {
    let data = b"roundtrip test".to_vec();
    let mut ln = Ln::new(Some(data.clone()));
    ln.set_vlsn(Vlsn::new(12345));

    let mut buf = Vec::new();
    ln.write_to_log(&mut buf);

    let ln2 = Ln::read_from_log(&buf).unwrap();

    assert_eq!(ln2.get_data(), Some(data.as_slice()));
    assert_eq!(ln2.get_vlsn().sequence(), 12345);
}

#[test]
fn test_versioned_ln() {
    let data = b"versioned".to_vec();
    let vlsn = Vlsn::new(999);
    let ln = make_versioned_ln(Some(data.clone()), vlsn);

    assert_eq!(ln.get_data(), Some(data.as_slice()));
    assert_eq!(ln.get_vlsn().sequence(), 999);
}

#[test]
fn test_uncached_ln() {
    let data = b"uncached".to_vec();
    let ln = make_uncached_ln(Some(data.clone()));

    assert_eq!(ln.get_data(), Some(data.as_slice()));
    assert!(ln.is_fetched_cold());
}

#[test]
fn test_uncached_ln_from_bytes() {
    let data = b"uncached bytes";
    let ln = make_uncached_ln_from_bytes(data);

    assert_eq!(ln.get_data(), Some(data.as_slice()));
    assert!(ln.is_fetched_cold());
}

#[test]
fn test_map_ln_basic() {
    let config = b"config data".to_vec();
    let map_ln = MapLn::new(42, config);

    assert_eq!(map_ln.get_db_id(), 42);
    assert!(!map_ln.is_deleted());
    assert!(!map_ln.is_transient());
}

#[test]
fn test_map_ln_roundtrip() {
    let config = b"test config".to_vec();
    let mut map_ln = MapLn::new(100, config.clone());
    map_ln.get_ln_mut().set_vlsn(Vlsn::new(50));

    let mut buf = Vec::new();
    map_ln.write_to_log(&mut buf);

    let map_ln2 = MapLn::read_from_log(&buf).unwrap();

    assert_eq!(map_ln2.get_db_id(), 100);
    assert_eq!(map_ln2.get_ln().get_data(), Some(config.as_slice()));
    assert_eq!(map_ln2.get_ln().get_vlsn().sequence(), 50);
}

#[test]
fn test_map_ln_flags() {
    let config = b"config".to_vec();
    let mut map_ln = MapLn::new(200, config);

    map_ln.set_deleted(true);
    map_ln.set_transient(true);

    assert!(map_ln.is_deleted());
    assert!(map_ln.is_transient());

    let mut buf = Vec::new();
    map_ln.write_to_log(&mut buf);

    let map_ln2 = MapLn::read_from_log(&buf).unwrap();

    assert!(map_ln2.is_deleted());
    assert!(map_ln2.is_transient());
}

#[test]
fn test_name_ln_basic() {
    let name_ln = NameLn::new(42);

    assert_eq!(name_ln.get_db_id(), 42);
}

#[test]
fn test_name_ln_roundtrip() {
    let mut name_ln = NameLn::new(12345);
    name_ln.get_ln_mut().set_vlsn(Vlsn::new(100));

    let mut buf = Vec::new();
    name_ln.write_to_log(&mut buf);

    let name_ln2 = NameLn::read_from_log(&buf).unwrap();

    assert_eq!(name_ln2.get_db_id(), 12345);
    assert_eq!(name_ln2.get_ln().get_vlsn().sequence(), 100);
}

#[test]
fn test_name_ln_set_db_id() {
    let mut name_ln = NameLn::new(100);
    name_ln.set_db_id(200);

    assert_eq!(name_ln.get_db_id(), 200);
    assert!(name_ln.get_ln().is_dirty());
}

#[test]
fn test_file_summary_utilization() {
    let mut summary = FileSummary::new();
    summary.total_size = 1000;
    summary.obsolete_size = 300;

    let util = summary.utilization();
    assert!((util - 0.7).abs() < 0.001);
}

#[test]
fn test_file_summary_add() {
    let mut summary1 = FileSummary::new();
    summary1.total_count = 10;
    summary1.total_size = 1000;
    summary1.obsolete_count = 3;
    summary1.obsolete_size = 300;

    let mut summary2 = FileSummary::new();
    summary2.total_count = 20;
    summary2.total_size = 2000;

    summary1.add(&summary2);

    assert_eq!(summary1.total_count, 30);
    assert_eq!(summary1.total_size, 3000);
}

#[test]
fn test_file_summary_ln_basic() {
    let summary = FileSummary::new();
    let fs_ln = FileSummaryLn::new(summary);

    assert!(!fs_ln.is_modified());
}

#[test]
fn test_file_summary_ln_modification() {
    let summary = FileSummary::new();
    let mut fs_ln = FileSummaryLn::new(summary);

    fs_ln.get_summary_mut().total_count = 10;

    assert!(fs_ln.is_modified());
    assert_eq!(fs_ln.get_summary().total_count, 10);
}

#[test]
fn test_file_summary_ln_roundtrip() {
    let mut summary = FileSummary::new();
    summary.total_count = 50;
    summary.total_size = 2500;
    summary.obsolete_count = 10;
    summary.obsolete_size = 500;

    let mut fs_ln = FileSummaryLn::new(summary);
    fs_ln.set_modified();

    let mut buf = Vec::new();
    fs_ln.write_to_log(&mut buf);

    let fs_ln2 = FileSummaryLn::read_from_log(&buf).unwrap();

    assert!(fs_ln2.is_modified());
    assert_eq!(fs_ln2.get_summary().total_count, 50);
    assert_eq!(fs_ln2.get_summary().total_size, 2500);
}
