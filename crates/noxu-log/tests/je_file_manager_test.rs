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

// ─────────────────────────────────────────────────────────────────────────────
// FileManagerTest.testFlipFile  (wave 10-A)
//
// JE invariant: `FileManager.flipFile()` advances the current file
// number by exactly one and the new file number is observable via the
// listing.
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn je_file_manager_flip_file_creates_next_file() {
    let dir = TempDir::new().unwrap();
    let fm = make_fm(&dir);

    // Create initial file 0 (empty header) so flip_file has something to
    // flip from.
    fm.create_file(0).unwrap();
    let initial = fm.get_current_file_num();
    let next = fm.flip_file().unwrap();
    assert_eq!(initial + 1, next, "flip_file should advance by 1");
    assert_eq!(next, fm.get_current_file_num());

    // Both files appear in the listing.
    let mut nums = fm.list_file_numbers().unwrap();
    nums.sort_unstable();
    assert!(nums.contains(&initial));
    assert!(nums.contains(&next));
}

// ─────────────────────────────────────────────────────────────────────────────
// FileManagerTest.testTruncatedHeader  (wave 10-A)
//
// JE invariant: opening a log file whose header has been truncated
// short of the FileHeader length must fail rather than silently return
// a half-initialized handle.  JE throws ChecksumException; noxu surfaces
// a `LogError` (the exact variant depends on whether the truncation
// trips the EOF read or the header validation, both of which produce
// an Err return).
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn je_file_manager_get_handle_rejects_truncated_header() {

    let dir = TempDir::new().unwrap();
    {
        let fm = make_fm(&dir);
    let _fh = fm.create_file(0).unwrap();
        let _ = _fh; // mark used; don't depend on Debug
        // Drop the FileManager to release the cached handle so we can
        // truncate the underlying file out from under any open Fd.
    }

    // Truncate file 0 to half its header length.
    let path = dir.path().join("00000000.ndb");
    let truncated_len: u64 = noxu_log::file_manager::first_log_entry_offset() as u64 / 2;
    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    f.set_len(truncated_len).unwrap();
    drop(f);

    // Re-open FileManager and try to get the handle.
    let fm = make_fm(&dir);
    let result = fm.get_file_handle(0);
    assert!(
        result.is_err(),
        "get_file_handle on truncated header must fail",
    );
}
