// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! End-to-end tests for the `noxu-admin` CLI (dump / load / print-log).
//!
//! These drive the real built binary as a subprocess, so they exercise arg
//! parsing, read-only env opening, the on-disk dump format, and error
//! handling exactly as a user would.  The binary path is injected by cargo
//! as `CARGO_BIN_EXE_noxu-admin`.
//!
//! Faithful to JE `DbDump` / `DbLoad` / `DbPrintLog` semantics.

use std::collections::BTreeSet;
use std::path::Path;
use std::process::Command;

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    OperationStatus,
};

fn admin_bin() -> &'static str {
    env!("CARGO_BIN_EXE_noxu-admin")
}

/// A spread of records that stresses binary-safety:
/// - ordinary ASCII keys,
/// - keys/values containing non-printable bytes (0x00, 0x0a newline, 0xff),
/// - a backslash (the escape char) in both key and value,
/// - duplicate keys (same key, different data) in a dup-sort DB.
fn sample_records() -> Vec<(Vec<u8>, Vec<u8>)> {
    vec![
        (b"alpha".to_vec(), b"first".to_vec()),
        (b"beta".to_vec(), b"second".to_vec()),
        // Non-printable bytes in the value.
        (b"binval".to_vec(), vec![0x00, 0x0a, 0xff, 0x7f, 0x80]),
        // Non-printable bytes in the key.
        (vec![0x00, 0x01, 0x02, 0xfe], b"binkey".to_vec()),
        // Backslash and newline (the two characters JE's escape mechanism
        // treats specially) in both halves.
        (b"back\\slash".to_vec(), b"val\\with\\back".to_vec()),
        (vec![b'k', b'\n', b'e', b'y'], vec![b'v', b'\n', b'l']),
        // A long-ish value with the full byte range to be thorough.
        (b"allbytes".to_vec(), (0u8..=255).collect()),
    ]
}

fn populate(dir: &Path, db_name: &str, dup_sort: bool) {
    let env = Environment::open(
        EnvironmentConfig::new(dir.to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .expect("open env");
    let db = env
        .open_database(
            None,
            db_name,
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true)
                .with_sorted_duplicates(dup_sort),
        )
        .expect("open db");

    let txn = env.begin_transaction(None).expect("begin");
    for (k, v) in sample_records() {
        db.put(
            Some(&txn),
            &DatabaseEntry::from_bytes(&k),
            &DatabaseEntry::from_bytes(&v),
        )
        .expect("put");
    }
    if dup_sort {
        // Add duplicate data for an existing key.
        db.put(
            Some(&txn),
            &DatabaseEntry::from_bytes(b"alpha"),
            &DatabaseEntry::from_bytes(b"first-dup"),
        )
        .expect("put dup");
        db.put(
            Some(&txn),
            &DatabaseEntry::from_bytes(b"alpha"),
            &DatabaseEntry::from_bytes(b"first-dup-2"),
        )
        .expect("put dup 2");
    }
    txn.commit().expect("commit");
    drop(db);
    env.close().expect("close");
}

/// Read every (key, data) pair from a database into a multiset so we can
/// compare two databases for exact equality regardless of any incidental
/// ordering differences (dup-sort iteration order is deterministic but we
/// compare as a set-of-pairs to be safe).
fn read_all(
    dir: &Path,
    db_name: &str,
    dup_sort: bool,
) -> BTreeSet<(Vec<u8>, Vec<u8>)> {
    let env = Environment::open(
        EnvironmentConfig::new(dir.to_path_buf()).with_read_only(true),
    )
    .expect("reopen env");
    let db = env
        .open_database(
            None,
            db_name,
            &DatabaseConfig::new()
                .with_read_only(true)
                .with_sorted_duplicates(dup_sort),
        )
        .expect("reopen db");
    let mut out = BTreeSet::new();
    for r in db.iter(None).expect("iter") {
        let (k, v) = r.expect("read");
        out.insert((k, v));
    }
    drop(db);
    env.close().expect("close");
    out
}

/// HEADLINE: dump | load round-trip must reproduce the database exactly,
/// for both printable and hex formats, including binary and duplicate keys.
fn round_trip(printable: bool, dup_sort: bool) {
    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();
    let dump_file = src.path().join("dump.txt");

    populate(src.path(), "data", dup_sort);

    // dump
    let mut dump_cmd = Command::new(admin_bin());
    dump_cmd
        .arg("dump")
        .arg("-h")
        .arg(src.path())
        .arg("-s")
        .arg("data")
        .arg("-f")
        .arg(&dump_file);
    if printable {
        dump_cmd.arg("-p");
    }
    if dup_sort {
        dump_cmd.arg("-D");
    }
    let dump_out = dump_cmd.output().expect("run dump");
    assert!(
        dump_out.status.success(),
        "dump failed: {}",
        String::from_utf8_lossy(&dump_out.stderr)
    );

    // Sanity: the dump file carries the right header for the chosen format.
    let dump_text = std::fs::read_to_string(&dump_file).unwrap();
    assert!(dump_text.starts_with("VERSION=3\n"));
    assert!(dump_text.contains(if printable {
        "format=print\n"
    } else {
        "format=bytevalue\n"
    }));
    assert!(
        dump_text
            .contains(&format!("dupsort={}\n", if dup_sort { 1 } else { 0 }))
    );
    assert!(dump_text.trim_end().ends_with("DATA=END"));

    // load into a fresh env
    let load_out = Command::new(admin_bin())
        .arg("load")
        .arg("-h")
        .arg(dst.path())
        .arg("-s")
        .arg("data")
        .arg("-f")
        .arg(&dump_file)
        .output()
        .expect("run load");
    assert!(
        load_out.status.success(),
        "load failed: {}",
        String::from_utf8_lossy(&load_out.stderr)
    );

    let original = read_all(src.path(), "data", dup_sort);
    let loaded = read_all(dst.path(), "data", dup_sort);
    assert_eq!(
        original, loaded,
        "round-trip mismatch (printable={printable}, dup_sort={dup_sort})"
    );
    // The all-bytes record proves binary safety end-to-end.
    assert!(loaded.contains(&(b"allbytes".to_vec(), (0u8..=255).collect())));
}

#[test]
fn dump_load_round_trip_printable_no_dups() {
    round_trip(true, false);
}

#[test]
fn dump_load_round_trip_hex_no_dups() {
    round_trip(false, false);
}

#[test]
fn dump_load_round_trip_printable_with_dups() {
    round_trip(true, true);
}

#[test]
fn dump_load_round_trip_hex_with_dups() {
    round_trip(false, true);
}

/// HEADLINE: print-log on an env with known writes emits entries for those
/// writes — TxnCommit and LN puts — with their LSNs and types.
#[test]
fn print_log_shows_commits_and_lns() {
    let dir = tempfile::tempdir().unwrap();
    populate(dir.path(), "data", false);

    let out = Command::new(admin_bin())
        .arg("print-log")
        .arg("-h")
        .arg(dir.path())
        .output()
        .expect("run print-log");
    assert!(
        out.status.success(),
        "print-log failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8_lossy(&out.stdout);

    // Every line is "lsn=... type=... size=...".
    assert!(text.contains("lsn="), "no lsn fields in output:\n{text}");
    // LogEntryType's Display uses short names ("Commit", "INS_LN_TX").
    assert!(text.contains("type=Commit"), "expected a commit entry:\n{text}");
    // The committed put is a transactional insert LN.
    assert!(
        text.contains("type=INS_LN_TX") || text.contains("type=INS_LN"),
        "expected an insert LN entry:\n{text}"
    );
    // LN lines carry key/data sizes.
    assert!(text.contains("keylen="), "LN entries should show keylen=");
}

/// print-log -S prints a per-type summary including a TxnCommit count.
#[test]
fn print_log_summary() {
    let dir = tempfile::tempdir().unwrap();
    populate(dir.path(), "data", false);

    let out = Command::new(admin_bin())
        .arg("print-log")
        .arg("-h")
        .arg(dir.path())
        .arg("-S")
        .output()
        .expect("run print-log -S");
    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("Log summary:"), "summary header missing:\n{text}");
    assert!(text.contains("total entries:"));
    assert!(text.contains("Commit"), "summary should tally commit entries");
}

/// dump -l lists database names.
#[test]
fn dump_list_databases() {
    let dir = tempfile::tempdir().unwrap();
    populate(dir.path(), "data", false);
    populate(dir.path(), "other", false);

    let out = Command::new(admin_bin())
        .arg("dump")
        .arg("-h")
        .arg(dir.path())
        .arg("-l")
        .output()
        .expect("run dump -l");
    assert!(out.status.success());
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(text.contains("data"), "missing 'data' in list:\n{text}");
    assert!(text.contains("other"), "missing 'other' in list:\n{text}");
}

// ── Graceful error handling: bad path, missing db, malformed dump ──────────

#[test]
fn dump_missing_env_fails_cleanly() {
    let out = Command::new(admin_bin())
        .arg("dump")
        .arg("-h")
        .arg("/nonexistent/path/to/env")
        .arg("-s")
        .arg("data")
        .output()
        .expect("run dump");
    assert!(!out.status.success(), "should fail on missing env");
    assert!(
        out.stdout.is_empty() || !out.stderr.is_empty(),
        "expected an error message on stderr"
    );
    // Must be a clean message, not a panic backtrace.
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("noxu-admin:"), "expected clean error, got:\n{err}");
    assert!(!err.contains("panicked"), "must not panic:\n{err}");
}

#[test]
fn load_malformed_dump_fails_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let bad = dir.path().join("bad.txt");
    // Header without HEADER=END terminator.
    std::fs::write(&bad, "VERSION=3\nformat=print\n").unwrap();

    let out = Command::new(admin_bin())
        .arg("load")
        .arg("-h")
        .arg(dir.path())
        .arg("-s")
        .arg("data")
        .arg("-f")
        .arg(&bad)
        .output()
        .expect("run load");
    assert!(!out.status.success(), "should fail on malformed dump");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("noxu-admin:"), "expected clean error, got:\n{err}");
    assert!(!err.contains("panicked"));
}

#[test]
fn load_missing_db_name_fails_cleanly() {
    let dir = tempfile::tempdir().unwrap();
    let dump = dir.path().join("d.txt");
    // Valid header + one record, but no -s and no database= header line.
    std::fs::write(
        &dump,
        "VERSION=3\nformat=print\ntype=btree\ndupsort=0\nHEADER=END\n k\n v\nDATA=END\n",
    )
    .unwrap();

    let out = Command::new(admin_bin())
        .arg("load")
        .arg("-h")
        .arg(dir.path())
        .arg("-f")
        .arg(&dump)
        .output()
        .expect("run load");
    assert!(!out.status.success(), "should fail without a db name");
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("noxu-admin:"));
}

/// load with `database=` header line (no -s) picks up the name from the dump.
#[test]
fn load_db_name_from_header() {
    let dir = tempfile::tempdir().unwrap();
    let dump = dir.path().join("d.txt");
    std::fs::write(
        &dump,
        "VERSION=3\nformat=print\ntype=btree\ndupsort=0\ndatabase=fromheader\nHEADER=END\n key1\n val1\nDATA=END\n",
    )
    .unwrap();

    let out = Command::new(admin_bin())
        .arg("load")
        .arg("-h")
        .arg(dir.path())
        .arg("-f")
        .arg(&dump)
        .output()
        .expect("run load");
    assert!(
        out.status.success(),
        "load failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let loaded = read_all(dir.path(), "fromheader", false);
    assert!(loaded.contains(&(b"key1".to_vec(), b"val1".to_vec())));
}

/// no-overwrite mode (-n) reports key-exists rather than clobbering.
#[test]
fn load_no_overwrite_keeps_existing() {
    let dir = tempfile::tempdir().unwrap();
    // Pre-populate with a different value for "alpha".
    {
        let env = Environment::open(
            EnvironmentConfig::new(dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
        let db = env
            .open_database(
                None,
                "data",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .unwrap();
        let txn = env.begin_transaction(None).unwrap();
        db.put(
            Some(&txn),
            &DatabaseEntry::from_bytes(b"alpha"),
            &DatabaseEntry::from_bytes(b"PRESERVE"),
        )
        .unwrap();
        txn.commit().unwrap();
        drop(db);
        env.close().unwrap();
    }

    let dump = dir.path().join("d.txt");
    std::fs::write(
        &dump,
        "VERSION=3\nformat=print\ntype=btree\ndupsort=0\nHEADER=END\n alpha\n CLOBBER\nDATA=END\n",
    )
    .unwrap();

    let out = Command::new(admin_bin())
        .arg("load")
        .arg("-h")
        .arg(dir.path())
        .arg("-s")
        .arg("data")
        .arg("-f")
        .arg(&dump)
        .arg("-n")
        .output()
        .expect("run load -n");
    assert!(out.status.success());

    // The existing value must survive.
    let env = Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf()).with_read_only(true),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "data",
            &DatabaseConfig::new().with_read_only(true),
        )
        .unwrap();
    let key = DatabaseEntry::from_bytes(b"alpha");
    let mut val = DatabaseEntry::new();
    let status = db.get(None, &key, &mut val).unwrap();
    assert_eq!(status, OperationStatus::Success);
    assert_eq!(val.get_data(), Some(b"PRESERVE".as_ref()));
    drop(db);
    env.close().unwrap();
}
