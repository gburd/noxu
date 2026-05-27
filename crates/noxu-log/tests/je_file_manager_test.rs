//! JE FileManagerTest ports — log-file management invariants reachable
//! via the public `FileManager` API.
//!
//! Each test below corresponds to a method in
//! `test/com/sleepycat/je/log/FileManagerTest.java`.  Because Noxu's
//! file-name format differs (`.ndb` vs JE's `.jdb`) and `FileManager`
//! does not expose `getFollowingFileNum`, several JE tests are ported
//! by-spirit only.

use noxu_log::FileManager;
use std::fs::File;
use tempfile::TempDir;

const FILE_SIZE: u64 = 4096;
const CACHE_SIZE: usize = 16;

fn make_fm(dir: &TempDir) -> FileManager {
    FileManager::new(dir.path(), false, FILE_SIZE, CACHE_SIZE).unwrap()
}

// ─────────────────────────────────────────────────────────────────────────────
// FileManagerTest.testLastFile (wave 9-C)
//
// JE invariant: with no log files, getLastFileNum() returns null.  With
// log files {0, 1, 2} present alongside non-`.jdb` decoy files, the
// largest legitimate file number is returned.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn je_file_manager_last_file_no_files() {
    let dir = TempDir::new().unwrap();
    let fm = make_fm(&dir);
    assert!(fm.get_last_file_num().unwrap().is_none());
    assert!(fm.get_first_file_num().unwrap().is_none());
}

#[test]
fn je_file_manager_last_file_skips_decoys() {
    let dir = TempDir::new().unwrap();

    // Create some legitimate-looking `.ndb` files.
    File::create(dir.path().join("00000000.ndb")).unwrap();
    File::create(dir.path().join("00000001.ndb")).unwrap();
    File::create(dir.path().join("00000002.ndb")).unwrap();

    // Create decoy files that should be ignored.
    File::create(dir.path().join("108.cif")).unwrap(); // wrong extension
    File::create(dir.path().join("00000abx.ndb")).unwrap(); // non-hex
    File::create(dir.path().join("10.10.ndb")).unwrap(); // bad format
    File::create(dir.path().join("00000003.jdb")).unwrap(); // JE format, not noxu

    let fm = make_fm(&dir);
    assert_eq!(Some(2), fm.get_last_file_num().unwrap());
    assert_eq!(Some(0), fm.get_first_file_num().unwrap());
}

// ─────────────────────────────────────────────────────────────────────────────
// FileManagerTest.testFileNameFormat (wave 9-C, adapted)
//
// JE invariant: file names are the file number formatted as 8 hex
// digits followed by the suffix.  JE asserts `1L -> "00000001.jdb"`
// and `123L -> "0000007b.jdb"`.  Noxu's format is the same shape with
// `.ndb` instead of `.jdb`; we assert that `list_file_numbers` parses
// back to the original numeric value, which exercises the same encoding.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn je_file_manager_file_name_format_round_trips_via_listing() {
    let dir = TempDir::new().unwrap();

    for &n in &[0u32, 1, 0x7b, 0xff, 0x12345678] {
        File::create(dir.path().join(format!("{n:08x}.ndb"))).unwrap();
    }

    let fm = make_fm(&dir);
    let mut nums = fm.list_file_numbers().unwrap();
    nums.sort_unstable();
    assert_eq!(vec![0u32, 1, 0x7b, 0xff, 0x12345678], nums);
}

// ─────────────────────────────────────────────────────────────────────────────
// FileManagerTest.testFileCreation (wave 9-C, adapted)
//
// JE invariant: after creating two `.jdb` files (and a couple of
// confusingly-named non-`.jdb` files), `listFileNames(JE_SUFFIXES)`
// returns exactly the two `.jdb` files.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn je_file_manager_list_only_returns_ndb_files() {
    let dir = TempDir::new().unwrap();

    File::create(dir.path().join("00000000.ndb")).unwrap();
    File::create(dir.path().join("00000001.ndb")).unwrap();

    // Decoys.
    File::create(dir.path().join("00000abx.ndb")).unwrap();
    File::create(dir.path().join("10.10.ndb")).unwrap();
    File::create(dir.path().join("00000002.jdb")).unwrap(); // wrong suffix
    File::create(dir.path().join("00000003.txt")).unwrap();

    let fm = make_fm(&dir);
    let nums = fm.list_file_numbers().unwrap();
    assert_eq!(2, nums.len(), "expected exactly the two .ndb files: {nums:?}");
}
