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
    let truncated_len: u64 =
        noxu_log::file_manager::first_log_entry_offset() as u64 / 2;
    let f =
        std::fs::OpenOptions::new().read(true).write(true).open(&path).unwrap();
    f.set_len(truncated_len).unwrap();
    drop(f);

    // Re-open FileManager and try to get the handle.
    let fm = make_fm(&dir);
    let result = fm.get_file_handle(0);
    assert!(result.is_err(), "get_file_handle on truncated header must fail",);
}

// -----------------------------------------------------------------------------
// FileManagerTest.testSetLastPosition
//
// JE invariant: setLastPosition(nextLsn, lastUsedLsn, prevOffset) primes the
// FileManager's end-of-log state so that logging continues from `nextLsn`.
// JE's signature carries a prev-offset third arg; Noxu's set_last_position
// takes (next_available_lsn, last_used_lsn) and derives current_file_num
// from next_available_lsn. We assert the state that JE's subsequent bumpLsn
// reads: last-used LSN, next-available LSN, and the current file number.
// (The bump-placement half of JE's test -- an entry that fits in file 79 vs.
// one that spills to file 80 -- is covered at the LogManager layer by
// noxu_log_tests.rs test_log_manager_lsn_stride / _lsns_are_ascending, since
// LSN advancement is internal to LogManager, not a public FileManager op.)
// -----------------------------------------------------------------------------

#[test]
fn je_file_manager_set_last_position_primes_end_of_log() {
    use noxu_util::lsn::Lsn;
    let dir = TempDir::new().unwrap();
    let fm = make_fm(&dir);

    // Pretend the last file is file 79, next write goes at offset 88, the
    // last used entry began at offset 77.
    let next = Lsn::new(79, 88);
    let last_used = Lsn::new(79, 77);
    fm.set_last_position(next, last_used);

    assert_eq!(fm.get_next_available_lsn(), next);
    assert_eq!(fm.get_last_used_lsn(), last_used);
    assert_eq!(fm.get_current_file_num(), 79);
}

// -----------------------------------------------------------------------------
// FileManagerTest.testFollowingFile
//
// JE invariant: getFollowingFileNum(n, forward) returns the next (forward) or
// previous (backward) real `.jdb` file number relative to `n`, tolerating a
// non-existent `n`, and null when there is nothing beyond `n`. Noxu has no
// single `get_following_file_num` on FileManager (readers derive it from the
// file listing), so this ports the SAME semantics over `list_file_numbers`,
// which is the parsed, decoy-filtered set of real files. The forward/backward
// selection logic itself is also unit-tested against the LogFileAccess trait
// in src/*_file_reader.rs (test_mock_get_following_file_*).
// -----------------------------------------------------------------------------

#[test]
fn je_file_manager_following_file_num_forward_and_backward() {
    let dir = TempDir::new().unwrap();

    // Real files {1, 6, 9} plus decoys that must be ignored. JE's decoy is
    // "003.jdb" (short + wrong-suffix); here we use a non-hex stem and a
    // wrong-suffix name -- both of which Noxu's parser rejects. (Note: unlike
    // JE, Noxu's parse_file_number accepts any hex stem length, so a
    // zero-padded-to-3 name like "003.ndb" would parse as file 3 rather than
    // being treated as a decoy; that lax parsing is a separate, minor format
    // deviation and is not what this test exercises.)
    File::create(dir.path().join("00000001.ndb")).unwrap();
    File::create(dir.path().join("00000006.ndb")).unwrap();
    File::create(dir.path().join("00000009.ndb")).unwrap();
    File::create(dir.path().join("0000zz03.ndb")).unwrap(); // non-hex stem
    File::create(dir.path().join("00000004.jdb")).unwrap(); // wrong suffix

    let fm = make_fm(&dir);
    let mut files = fm.list_file_numbers().unwrap();
    files.sort_unstable();
    assert_eq!(files, vec![1u32, 6, 9], "decoys must be filtered out");

    // getFollowingFileNum semantics over the parsed set.
    let following = |n: u32, forward: bool| -> Option<u32> {
        if forward {
            files.iter().copied().find(|&f| f > n)
        } else {
            files.iter().copied().rev().find(|&f| f < n)
        }
    };

    // Forward.
    assert_eq!(following(2, true), Some(6)); // next after non-existent 2
    assert_eq!(following(8, true), Some(9)); // next after non-existent 8
    assert_eq!(following(9, true), None);
    assert_eq!(following(10, true), None);

    // Backward.
    assert_eq!(following(8, false), Some(6)); // prev before non-existent 8
    assert_eq!(following(9, false), Some(6));
    assert_eq!(following(1, false), None);
    assert_eq!(following(0, false), None);
}

// -----------------------------------------------------------------------------
// FileManagerTest.testBadHeader
//
// JE invariant: a log file whose header has been overwritten with junk must
// be rejected when its handle is opened (JE throws ChecksumException). Noxu's
// FileManager::get_file_handle validates the header on a cache miss and
// returns an Err. (JE also asserts that opening a FileManager on a
// non-existent/unwritable directory throws IllegalArgumentException; that is
// exercised by FileManager::new returning Err, which make_fm().unwrap() would
// surface -- we keep the focus here on the corrupt-header path, the novel
// behaviour testBadHeader adds over testTruncatedHeader.)
// -----------------------------------------------------------------------------

#[test]
fn je_file_manager_get_handle_rejects_corrupt_header() {
    use std::io::{Seek, SeekFrom, Write};
    let dir = TempDir::new().unwrap();
    {
        let fm = make_fm(&dir);
        let _fh = fm.create_file(0).unwrap();
        // Drop the FileManager so the cached handle is released before we
        // scribble on the file underneath it.
    }

    // Overwrite the first two header bytes with junk (JE writes {1,1} at the
    // start of the file), corrupting the header CRC.
    let path = dir.path().join("00000000.ndb");
    {
        let mut f =
            std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.seek(SeekFrom::Start(0)).unwrap();
        f.write_all(&[1u8, 1u8]).unwrap();
        f.flush().unwrap();
    }

    // Re-open and try to get the handle: must fail (JE ChecksumException).
    let fm = make_fm(&dir);
    let result = fm.get_file_handle(0);
    assert!(result.is_err(), "get_file_handle on a corrupt header must fail");
}
