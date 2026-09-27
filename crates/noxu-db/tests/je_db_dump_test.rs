//! Faithful ports of JE `com.sleepycat.je.util.DbDumpTest` (DbDump.java /
//! DbLoad.java) that complement the round-trip coverage in `admin_cli_test.rs`.
//!
//! `admin_cli_test.rs::round_trip` already ports
//! `DbDumpTest.doDumpLoadTest(printable, nDumps=1)` (i.e.
//! `testDumpLoadPrintable` / `testDumpLoadBinary`).  This file adds:
//!   - `dump_matches_core_fixed_format`  → JE `testMatchCore`
//!   - `dump_load_multiple_databases`    → JE `testDumpLoadTwo` / `testDumpLoadThree`
//!
//! Adaptation note (language/API): Noxu's `noxu-admin dump` writes one database
//! per invocation and does not emit a `database=<name>` header line, whereas
//! JE's `DbDump` can dump several databases into one stream that `DbLoad`
//! demultiplexes by `database=`.  The `nDumps` intent of JE's test —
//! *multiple databases each dump/load/verify losslessly, and a re-dump equals
//! the first dump* — is preserved by dumping/loading each database with its
//! own `-s <name>` invocation.  The demultiplex-by-header path is separately
//! covered by `admin_cli_test.rs::load_db_name_from_header`.

use std::path::Path;
use std::process::Command;

fn admin_bin() -> &'static str {
    env!("CARGO_BIN_EXE_noxu-admin")
}

fn run(args: &[&std::ffi::OsStr]) -> std::process::Output {
    Command::new(admin_bin()).args(args).output().expect("run noxu-admin")
}

use std::ffi::OsStr;

fn dump(env_home: &Path, db: &str, file: &Path, printable: bool) {
    let mut a: Vec<&OsStr> = vec![
        OsStr::new("dump"),
        OsStr::new("-h"),
        env_home.as_os_str(),
        OsStr::new("-s"),
        OsStr::new(db),
        OsStr::new("-f"),
        file.as_os_str(),
    ];
    if printable {
        a.push(OsStr::new("-p"));
    }
    let out = run(&a);
    assert!(
        out.status.success(),
        "dump {db} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn load(env_home: &Path, db: &str, file: &Path) {
    let a: Vec<&OsStr> = vec![
        OsStr::new("load"),
        OsStr::new("-h"),
        env_home.as_os_str(),
        OsStr::new("-s"),
        OsStr::new(db),
        OsStr::new("-f"),
        file.as_os_str(),
    ];
    let out = run(&a);
    assert!(
        out.status.success(),
        "load {db} failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

fn read_all(dir: &Path, db_name: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};
    let env = Environment::open(
        EnvironmentConfig::new(dir.to_path_buf()).with_read_only(true),
    )
    .expect("reopen env");
    let db = env
        .open_database(
            None,
            db_name,
            &DatabaseConfig::new().with_read_only(true),
        )
        .expect("reopen db");
    let mut out = Vec::new();
    for r in db.iter(None).expect("iter") {
        out.push(r.expect("read"));
    }
    drop(db);
    env.close().expect("close");
    out
}

/// JE: `DbDumpTest.testMatchCore` (DbDump.java) — JE loads a *fixed*
/// hand-written dump stream (VERSION=3, format=print, type=btree, dupsort=0,
/// two key/data pairs), verifies the two records read back in order, then
/// re-dumps the database and asserts the re-dump is byte-identical to the
/// input stream.  This proves the dump format is the classic `db_dump`/Core
/// format and that dump/load are exact inverses.
#[test]
fn dump_matches_core_fixed_format() {
    let dir = tempfile::tempdir().unwrap();
    let dump_in = dir.path().join("in.txt");

    // A dump in the exact fixed format JE's testMatchCore uses.
    let input = "VERSION=3\n\
                 format=print\n\
                 type=btree\n\
                 dupsort=0\n\
                 HEADER=END\n\
                 abc\n\
                 firstLetters\n\
                 xyz\n\
                 lastLetters\n\
                 DATA=END\n";
    // JE prefixes each key/data line with a single leading space (DbDump.dump
    // and DbLoad both use the leading-space convention); write it that way.
    let input: String = input
        .lines()
        .map(|l| {
            // Data/key lines are the four content lines between HEADER=END
            // and DATA=END; JE writes them with a single leading space.
            if matches!(l, "abc" | "firstLetters" | "xyz" | "lastLetters") {
                format!(" {l}\n")
            } else {
                format!("{l}\n")
            }
        })
        .collect();
    std::fs::write(&dump_in, &input).unwrap();

    // load it into database "foobar"
    load(dir.path(), "foobar", &dump_in);

    // verify the two records read back in key order
    let records = read_all(dir.path(), "foobar");
    assert_eq!(
        records,
        vec![
            (b"abc".to_vec(), b"firstLetters".to_vec()),
            (b"xyz".to_vec(), b"lastLetters".to_vec()),
        ],
        "loaded records must match the fixed input in key order"
    );

    // re-dump and assert byte-equality with the input (JE: dump2 == dumpInfo)
    let dump_out = dir.path().join("out.txt");
    dump(dir.path(), "foobar", &dump_out, true);
    let redumped = std::fs::read_to_string(&dump_out).unwrap();
    assert_eq!(
        redumped, input,
        "re-dump must be byte-identical to the fixed input stream \
         (JE testMatchCore: dump2.toString() == dumpInfo.toString())"
    );
}

/// JE: `DbDumpTest.testDumpLoadTwo` (nDumps=2) and `testDumpLoadThree`
/// (nDumps=3) — several databases each dump/load/verify losslessly, and a
/// second dump of each equals the first (JE: `Key.compareKeys(baosba,
/// baos2) == 0`).  Ported here by dumping/loading each database with its own
/// `-s <name>` invocation (see module note on the `database=` header path).
#[test]
fn dump_load_multiple_databases() {
    use noxu_db::{
        DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    };

    const N_DBS: usize = 3; // covers both testDumpLoadTwo and testDumpLoadThree
    const N_KEYS: u32 = 100;

    let src = tempfile::tempdir().unwrap();
    let dst = tempfile::tempdir().unwrap();

    // Build N sorted-duplicate databases with distinct data (JE initDbs).
    let expected: Vec<Vec<(Vec<u8>, Vec<u8>)>> = {
        let env = Environment::open(
            EnvironmentConfig::new(src.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true),
        )
        .expect("open src");
        let mut per_db = Vec::new();
        for d in 0..N_DBS {
            let name = format!("testDB{d}");
            let db = env
                .open_database(
                    None,
                    &name,
                    &DatabaseConfig::new()
                        .with_allow_create(true)
                        .with_transactional(true),
                )
                .expect("open db");
            let txn = env.begin_transaction(None).expect("begin");
            let mut recs = Vec::new();
            for i in 0..N_KEYS {
                // db-specific key so each db has distinct content
                let k = format!("db{d}-key{i:04}").into_bytes();
                let v = format!("{i}").into_bytes();
                db.put_in(
                    &txn,
                    DatabaseEntry::from_bytes(&k),
                    DatabaseEntry::from_bytes(&v),
                )
                .expect("put");
                recs.push((k, v));
            }
            txn.commit().expect("commit");
            recs.sort();
            per_db.push(recs);
            drop(db);
        }
        env.close().expect("close src");
        per_db
    };

    for (d, expected_recs) in expected.iter().enumerate() {
        let name = format!("testDB{d}");
        let dump1 = src.path().join(format!("dump{d}.txt"));
        dump(src.path(), &name, &dump1, false);

        // load into a fresh env, verify data matches
        load(dst.path(), &name, &dump1);
        let mut loaded = read_all(dst.path(), &name);
        loaded.sort();
        assert_eq!(
            &loaded, expected_recs,
            "db {name} round-trip mismatch (JE testDumpLoadTwo/Three verifyDb)"
        );

        // re-dump the loaded db and assert dump == re-dump
        // (JE: Key.compareKeys(baosba, baos2) == 0)
        let dump2 = dst.path().join(format!("redump{d}.txt"));
        dump(dst.path(), &name, &dump2, false);
        let a = std::fs::read(&dump1).unwrap();
        let b = std::fs::read(&dump2).unwrap();
        assert_eq!(
            a, b,
            "re-dump of db {name} must equal the first dump \
             (JE Key.compareKeys == 0)"
        );
    }
}
