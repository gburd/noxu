// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT

//! Noxu DB administrative command-line tool.
//!
//! Provides three read-mostly utilities, each a faithful port of the
//! corresponding Berkeley DB JE utility:
//!
//! - `dump`      — export a database's records to a portable text format
//!   (JE `com.sleepycat.je.util.DbDump`).
//! - `load`      — import records from a dump file into a database
//!   (JE `com.sleepycat.je.util.DbLoad`).
//! - `print-log` — human-readable walk of the write-ahead log
//!   (JE `com.sleepycat.je.util.DbPrintLog`).
//!
//! The dump format is byte-for-byte the classic `db_dump`/`DbLoad` text
//! format (VERSION=3 header, `format=print`/`format=bytevalue`, alternating
//! key/data lines, `DATA=END` terminator), so `dump | load` round-trips any
//! database including binary (non-UTF-8) and duplicate keys.
//!
//! Argument parsing is a tiny hand-rolled parser rather than `clap`: the core
//! engine deliberately keeps its dependency set small, and the JE-style flag
//! grammar (`-h <dir> -s <db> [-f file] [-p]`) is trivial to parse by hand.

use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use noxu_db::{DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig};
use noxu_log::entry::{LnLogEntry, TxnEndEntry};
use noxu_log::{FileManager, LogEntryType, LogFileReader};
use noxu_util::Lsn;

// ─────────────────────────────────────────────────────────────────────────
// Dump-format encoding (faithful to JE CmdUtil.formatEntry / DbLoad.loadLine)
// ─────────────────────────────────────────────────────────────────────────

const DUMP_VERSION: u32 = 3;

/// The printable ASCII run JE indexes into: code points 33..=126 ('!'..'~').
/// `CmdUtil.printableChars` is exactly this string, indexed by `b - 33`.
const PRINTABLE_LO: u8 = 33; // '!'  (JE isPrint: 040 < b)
const PRINTABLE_HI: u8 = 126; // '~'  (JE isPrint: b < 0177)
const BACKSLASH: u8 = b'\\';

/// Encode one byte slice as a JE dump line (no leading space, no newline).
///
/// Mirrors `CmdUtil.formatEntry`:
/// - `format=print`: printable bytes are emitted literally (backslash doubled);
///   non-printable bytes become `\HH` (lowercase 2-digit hex).
/// - `format=bytevalue`: every byte is 2-digit lowercase hex.
fn format_entry(out: &mut String, bytes: &[u8], printable: bool) {
    for &b in bytes {
        if printable && (PRINTABLE_LO..=PRINTABLE_HI).contains(&b) {
            if b == BACKSLASH {
                out.push('\\');
            }
            out.push(b as char);
        } else {
            // Non-printable, or hex (bytevalue) mode.
            if printable {
                out.push('\\');
            }
            out.push(hex_digit(b >> 4));
            out.push(hex_digit(b & 0x0f));
        }
    }
}

#[inline]
fn hex_digit(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        10..=15 => (b'a' + (nibble - 10)) as char,
        _ => unreachable!("nibble is masked to 0..=15"),
    }
}

#[inline]
fn hex_value(c: u8) -> Result<u8, String> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(format!("invalid hex digit '{}'", c as char)),
    }
}

/// Decode a single dump line back to bytes.
///
/// Mirrors `DbLoad.loadLine` / `DbLoad.readPrintableLine`:
/// - hex mode: pairs of hex digits.
/// - printable mode: literal bytes, `\\` -> backslash, `\HH` -> hex byte.
fn parse_entry(line: &str, printable: bool) -> Result<Vec<u8>, String> {
    let bytes = line.as_bytes();
    if printable {
        let mut out = Vec::with_capacity(bytes.len());
        let mut i = 0;
        while i < bytes.len() {
            let c = bytes[i];
            if c == BACKSLASH {
                i += 1;
                if i >= bytes.len() {
                    return Err("corrupted line: trailing backslash".into());
                }
                if bytes[i] == BACKSLASH {
                    out.push(BACKSLASH);
                    i += 1;
                } else {
                    if i + 1 >= bytes.len() {
                        return Err(
                            "corrupted line: truncated \\HH escape".into()
                        );
                    }
                    let hi = hex_value(bytes[i])?;
                    let lo = hex_value(bytes[i + 1])?;
                    out.push((hi << 4) | lo);
                    i += 2;
                }
            } else {
                out.push(c);
                i += 1;
            }
        }
        Ok(out)
    } else {
        if !bytes.len().is_multiple_of(2) {
            return Err(format!(
                "hex line has odd length {} (expected pairs of digits)",
                bytes.len()
            ));
        }
        let mut out = Vec::with_capacity(bytes.len() / 2);
        let mut i = 0;
        while i < bytes.len() {
            let hi = hex_value(bytes[i])?;
            let lo = hex_value(bytes[i + 1])?;
            out.push((hi << 4) | lo);
            i += 2;
        }
        Ok(out)
    }
}

// ─────────────────────────────────────────────────────────────────────────
// dump  (faithful to DbDump)
// ─────────────────────────────────────────────────────────────────────────

struct DumpArgs {
    env_home: PathBuf,
    db_name: Option<String>,
    out_file: Option<PathBuf>,
    printable: bool,
    list: bool,
    dup_sort: bool,
}

fn run_dump(args: DumpArgs) -> Result<(), String> {
    // DbDump.openEnv: read-only environment so a live env is not perturbed.
    let env_cfg =
        EnvironmentConfig::new(args.env_home.clone()).with_read_only(true);
    let env = Environment::open(env_cfg)
        .map_err(|e| format!("cannot open environment read-only: {e}"))?;

    if args.list {
        // DbDump.listDbs
        let names = env
            .database_names()
            .map_err(|e| format!("cannot list databases: {e}"))?;
        let stdout = io::stdout();
        let mut w = stdout.lock();
        for name in names {
            writeln!(w, "{name}").map_err(|e| e.to_string())?;
        }
        return Ok(());
    }

    let db_name = args
        .db_name
        .as_deref()
        .ok_or("dump: -s <database> is required (or use -l to list)")?;

    // Noxu does not persist the sorted-duplicates flag across a reopen, so a
    // dup-sort database read back without declaring it returns corrupted
    // two-part slot keys.  The operator declares dup-sort with `-D`; we open
    // the source database with that flag so iteration decodes correctly.
    let db_cfg = DatabaseConfig::new()
        .with_read_only(true)
        .with_sorted_duplicates(args.dup_sort);
    let db = env
        .open_database(None, db_name, &db_cfg)
        .map_err(|e| format!("cannot open database '{db_name}': {e}"))?;
    let dup_sort = db.sorted_duplicates() || args.dup_sort;

    // Route output to a file or stdout, both behind a BufWriter.
    let mut writer: Box<dyn Write> = match &args.out_file {
        Some(path) => {
            Box::new(BufWriter::new(File::create(path).map_err(|e| {
                format!("cannot create '{}': {e}", path.display())
            })?))
        }
        None => Box::new(BufWriter::new(io::stdout())),
    };

    write_dump(&mut writer, &db, dup_sort, args.printable)?;
    writer.flush().map_err(|e| e.to_string())?;
    Ok(())
}

/// Write the JE DbDump format to `writer`.  Split out so the round-trip test
/// can drive it directly without spawning a process.
///
/// `dup_sort_hint` is what the reopened handle reports.  Noxu does not persist
/// the dup-sort flag across a read-only reopen, so we also *detect* duplicates
/// from the record stream (consecutive equal keys) and emit `dupsort=1` if
/// either source says so — this keeps `dump | load` lossless for duplicate
/// keys even though the reopened config reports `false`.
fn write_dump(
    writer: &mut dyn Write,
    db: &noxu_db::Database,
    dup_sort_hint: bool,
    printable: bool,
) -> Result<(), String> {
    // DbDump.dump: cursor.getNext over the whole database, key then data,
    // each line prefixed by a single space.  We buffer the body so the
    // header's dupsort field can reflect duplicates discovered while
    // scanning (a dup-sort DB yields consecutive equal keys).  Admin dump
    // tools are not latency-critical; JE's DbDump/DbScavenger likewise hold
    // working structures in memory.
    let mut body = String::new();
    let mut line = String::new();
    let mut prev_key: Option<Vec<u8>> = None;
    let mut dup_detected = false;

    let mut iter =
        db.iter(None).map_err(|e| format!("cannot open cursor: {e}"))?;
    loop {
        match iter.next() {
            None => break,
            Some(Err(e)) => return Err(format!("cursor read failed: {e}")),
            Some(Ok((key, data))) => {
                if prev_key.as_deref() == Some(key.as_slice()) {
                    dup_detected = true;
                }
                line.clear();
                line.push(' ');
                format_entry(&mut line, &key, printable);
                line.push('\n');
                line.push(' ');
                format_entry(&mut line, &data, printable);
                line.push('\n');
                body.push_str(&line);
                prev_key = Some(key);
            }
        }
    }
    let dup_sort = dup_sort_hint || dup_detected;

    // DbDump.printHeader
    let mut header = String::new();
    header.push_str(&format!("VERSION={DUMP_VERSION}\n"));
    header.push_str(if printable {
        "format=print\n"
    } else {
        "format=bytevalue\n"
    });
    header.push_str("type=btree\n");
    header.push_str(&format!("dupsort={}\n", if dup_sort { 1 } else { 0 }));
    header.push_str("HEADER=END\n");
    writer.write_all(header.as_bytes()).map_err(|e| e.to_string())?;
    writer.write_all(body.as_bytes()).map_err(|e| e.to_string())?;
    writer.write_all(b"DATA=END\n").map_err(|e| e.to_string())?;
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────
// load  (faithful to DbLoad)
// ─────────────────────────────────────────────────────────────────────────

struct LoadArgs {
    env_home: PathBuf,
    db_name: Option<String>,
    in_file: Option<PathBuf>,
    no_overwrite: bool,
}

fn run_load(args: LoadArgs) -> Result<(), String> {
    // DbLoad: the environment is opened read-write with allow-create so the
    // target database can be created if it does not already exist.
    let env_cfg = EnvironmentConfig::new(args.env_home.clone())
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(env_cfg)
        .map_err(|e| format!("cannot open environment: {e}"))?;

    let reader: Box<dyn BufRead> = match &args.in_file {
        Some(path) => {
            Box::new(BufReader::new(File::open(path).map_err(|e| {
                format!("cannot open '{}': {e}", path.display())
            })?))
        }
        None => Box::new(BufReader::new(io::stdin())),
    };

    load_dump(&env, args.db_name.as_deref(), args.no_overwrite, reader)
}

/// Parse a dump stream and insert into the named database.  Split out so the
/// round-trip test can call it directly.
///
/// The database name may come from the `-s` argument or from a
/// `database=<name>` header line (DbLoad.loadConfigLine).
fn load_dump(
    env: &Environment,
    db_name_arg: Option<&str>,
    no_overwrite: bool,
    mut reader: Box<dyn BufRead>,
) -> Result<(), String> {
    // DbLoad.loadHeader: read header lines until HEADER=END, learning the
    // format (print vs bytevalue), dupsort, and possibly the database name.
    let mut printable = false;
    let mut dup_sort = false;
    let mut header_db_name: Option<String> = None;

    let mut buf = String::new();
    loop {
        buf.clear();
        let n = reader.read_line(&mut buf).map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("dump file ended before HEADER=END".into());
        }
        let line = buf.trim_end_matches(['\n', '\r']);
        if line == "HEADER=END" {
            break;
        }
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| format!("invalid header line: '{line}'"))?;
        match key.trim().to_ascii_lowercase().as_str() {
            "version" => {
                if value.trim() != "3" {
                    return Err(format!(
                        "version {} is not supported (only 3)",
                        value.trim()
                    ));
                }
            }
            "format" => match value.trim().to_ascii_lowercase().as_str() {
                "print" => printable = true,
                "bytevalue" => printable = false,
                other => {
                    return Err(format!("unknown format value '{other}'"));
                }
            },
            "dupsort" => match value.trim().to_ascii_lowercase().as_str() {
                "true" | "1" => dup_sort = true,
                "false" | "0" => dup_sort = false,
                other => {
                    return Err(format!("unknown dupsort value '{other}'"));
                }
            },
            "type" => {
                if !value.trim().eq_ignore_ascii_case("btree") {
                    return Err(format!(
                        "unsupported database type '{}'",
                        value.trim()
                    ));
                }
            }
            "database" => header_db_name = Some(value.trim().to_string()),
            other => {
                return Err(format!("unknown header keyword '{other}'"));
            }
        }
    }

    let db_name = db_name_arg.map(str::to_string).or(header_db_name).ok_or(
        "load: a database name is required (-s or 'database=' header)",
    )?;

    let db_cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(dup_sort);
    let db = env
        .open_database(None, &db_name, &db_cfg)
        .map_err(|e| format!("cannot open database '{db_name}': {e}"))?;

    // DbLoad.loadData: alternating key/data lines until DATA=END.  All puts
    // run in one transaction so a malformed record leaves the DB unchanged.
    let txn = env
        .begin_transaction(None)
        .map_err(|e| format!("cannot begin transaction: {e}"))?;

    let mut key_line = String::new();
    let mut data_line = String::new();
    let mut count: u64 = 0;
    loop {
        key_line.clear();
        let n = reader.read_line(&mut key_line).map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("dump data ended without DATA=END".into());
        }
        let key_trim = key_line.trim();
        if key_trim == "DATA=END" {
            break;
        }
        data_line.clear();
        let n = reader.read_line(&mut data_line).map_err(|e| e.to_string())?;
        if n == 0 {
            return Err(format!("no data line to match key '{key_trim}'"));
        }
        let key_bytes = parse_entry(key_trim, printable)?;
        let data_bytes = parse_entry(data_line.trim(), printable)?;

        let key = DatabaseEntry::from_bytes(&key_bytes);
        let data = DatabaseEntry::from_bytes(&data_bytes);

        if no_overwrite {
            let status = db
                .put_no_overwrite_in(&txn, &key, &data)
                .map_err(|e| format!("put failed: {e}"))?;
            if !status {
                eprintln!("noxu-admin: key exists (skipped): {key_trim}");
            }
        } else {
            db.put_in(&txn, &key, &data)
                .map_err(|e| format!("put failed: {e}"))?;
        }
        count += 1;
    }

    txn.commit().map_err(|e| format!("commit failed: {e}"))?;
    eprintln!("noxu-admin: loaded {count} records into '{db_name}'");
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────
// print-log  (faithful to DbPrintLog)
// ─────────────────────────────────────────────────────────────────────────

struct PrintLogArgs {
    env_home: PathBuf,
    summary: bool,
}

fn run_print_log(args: PrintLogArgs) -> Result<(), String> {
    // DbPrintLog reads the raw log files; we do the same via a read-only
    // FileManager rather than opening a full Environment (which would run
    // recovery).  This lets print-log work even on a closed env.
    if !args.env_home.is_dir() {
        return Err(format!(
            "environment home '{}' is not a directory",
            args.env_home.display()
        ));
    }
    let fm = Arc::new(
        FileManager::new(&args.env_home, true, 1 << 30, 100)
            .map_err(|e| format!("cannot open log files: {e}"))?,
    );
    let file_nums = fm
        .list_file_numbers()
        .map_err(|e| format!("cannot list log files: {e}"))?;

    let stdout = io::stdout();
    let mut w = BufWriter::new(stdout.lock());

    // Summary mode (DbPrintLog -S): per-type counts, total bytes, entry count.
    let mut total_count: u64 = 0;
    let mut total_bytes: u64 = 0;
    // Small fixed-size tally keyed by type number (0..=70 covers all types).
    let mut type_counts: std::collections::BTreeMap<String, u64> =
        std::collections::BTreeMap::new();

    for file_num in file_nums {
        let mut reader = LogFileReader::open(Arc::clone(&fm), file_num)
            .map_err(|e| format!("cannot read log file {file_num:08x}: {e}"))?;
        while let Some((lsn, entry_type, payload)) = reader.read_next() {
            total_count += 1;
            // Header overhead is small and version-dependent; for the summary
            // we count the payload size (item_size), which is what matters for
            // utilization.  Faithful enough to DbPrintLog's per-type sizing.
            total_bytes += payload.len() as u64;
            *type_counts.entry(entry_type.to_string()).or_insert(0) += 1;

            if !args.summary {
                print_entry(&mut w, lsn, entry_type, &payload)
                    .map_err(|e| e.to_string())?;
            }
        }
    }

    if args.summary {
        writeln!(w, "Log summary:").map_err(|e| e.to_string())?;
        writeln!(w, "  total entries: {total_count}")
            .map_err(|e| e.to_string())?;
        writeln!(w, "  total payload bytes: {total_bytes}")
            .map_err(|e| e.to_string())?;
        writeln!(w, "  by type:").map_err(|e| e.to_string())?;
        for (ty, n) in &type_counts {
            writeln!(w, "    {ty:<14} {n}").map_err(|e| e.to_string())?;
        }
    }
    w.flush().map_err(|e| e.to_string())?;
    Ok(())
}

/// Print one log entry in human-readable form (DbPrintLog verbose mode).
///
/// For LN and Txn-end entries we decode the payload enough to show the txn id
/// and key/value sizes; other types print type + LSN + payload size only.
fn print_entry(
    w: &mut dyn Write,
    lsn: Lsn,
    entry_type: LogEntryType,
    payload: &[u8],
) -> io::Result<()> {
    write!(w, "lsn={lsn} type={entry_type} size={}", payload.len())?;

    match entry_type {
        LogEntryType::TxnCommit | LogEntryType::TxnAbort => {
            if let Ok(e) = TxnEndEntry::read_from_log(payload) {
                write!(
                    w,
                    " txn={} {}",
                    e.txn_id,
                    if e.is_commit() { "commit" } else { "abort" }
                )?;
            }
        }
        LogEntryType::InsertLN
        | LogEntryType::UpdateLN
        | LogEntryType::DeleteLN
        | LogEntryType::InsertLNTxn
        | LogEntryType::UpdateLNTxn
        | LogEntryType::DeleteLNTxn => {
            let is_txn = matches!(
                entry_type,
                LogEntryType::InsertLNTxn
                    | LogEntryType::UpdateLNTxn
                    | LogEntryType::DeleteLNTxn
            );
            if let Ok(r) = LnLogEntry::parse_from_slice(payload, is_txn) {
                write!(w, " db={}", r.db_id)?;
                if let Some(txn) = r.txn_id {
                    write!(w, " txn={txn}")?;
                }
                write!(w, " keylen={}", r.key.len())?;
                match r.data {
                    Some(d) => write!(w, " datalen={}", d.len())?,
                    None => write!(w, " (deleted)")?,
                }
            }
        }
        _ => {}
    }
    writeln!(w)
}

// ─────────────────────────────────────────────────────────────────────────
// Argument parsing (hand-rolled; JE-style single-letter flags)
// ─────────────────────────────────────────────────────────────────────────

const USAGE: &str = "\
noxu-admin — Noxu DB administrative utilities

USAGE:
    noxu-admin dump      -h <env> [-s <db>] [-f <file>] [-p] [-l] [-D]
    noxu-admin load      -h <env> [-s <db>] [-f <file>] [-n]
    noxu-admin print-log -h <env> [-S]

SUBCOMMANDS:
    dump        Export a database to portable text (JE DbDump format)
    load        Import a dump file into a database (JE DbLoad)
    print-log   Walk the write-ahead log (JE DbPrintLog)

COMMON FLAGS:
    -h <dir>    Environment home directory (required)
    -s <db>     Database name
    -f <file>   Output file (dump) / input file (load); default stdin/stdout

DUMP FLAGS:
    -p          Output printable characters where possible (else hex)
    -l          List database names and exit
    -D          The database has sorted duplicates (Noxu does not persist
                this flag across a reopen, so it must be declared for dump)

LOAD FLAGS:
    -n          No-overwrite mode (skip keys that already exist)

PRINT-LOG FLAGS:
    -S          Print a summary (per-type counts) instead of per-entry detail
";

/// Pull the value following a flag, advancing the iterator.
fn next_value(
    iter: &mut std::vec::IntoIter<String>,
    flag: &str,
) -> Result<String, String> {
    iter.next().ok_or_else(|| format!("{flag} requires an argument"))
}

fn main() -> ExitCode {
    let mut args: std::vec::IntoIter<String> =
        std::env::args().skip(1).collect::<Vec<_>>().into_iter();

    let subcommand = match args.next() {
        Some(s) => s,
        None => {
            eprint!("{USAGE}");
            return ExitCode::FAILURE;
        }
    };

    let result = match subcommand.as_str() {
        "dump" => parse_and_run_dump(args),
        "load" => parse_and_run_load(args),
        "print-log" => parse_and_run_print_log(args),
        "-h" | "--help" | "help" => {
            print!("{USAGE}");
            return ExitCode::SUCCESS;
        }
        other => Err(format!("unknown subcommand '{other}'")),
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(msg) => {
            eprintln!("noxu-admin: {msg}");
            ExitCode::FAILURE
        }
    }
}

fn parse_and_run_dump(
    mut iter: std::vec::IntoIter<String>,
) -> Result<(), String> {
    let mut env_home: Option<PathBuf> = None;
    let mut db_name: Option<String> = None;
    let mut out_file: Option<PathBuf> = None;
    let mut printable = false;
    let mut list = false;
    let mut dup_sort = false;
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "-h" => env_home = Some(next_value(&mut iter, "-h")?.into()),
            "-s" => db_name = Some(next_value(&mut iter, "-s")?),
            "-f" => out_file = Some(next_value(&mut iter, "-f")?.into()),
            "-p" => printable = true,
            "-l" => list = true,
            "-D" => dup_sort = true,
            other => return Err(format!("dump: unknown flag '{other}'")),
        }
    }
    let env_home = env_home.ok_or("dump: -h <env> is required")?;
    run_dump(DumpArgs {
        env_home,
        db_name,
        out_file,
        printable,
        list,
        dup_sort,
    })
}

fn parse_and_run_load(
    mut iter: std::vec::IntoIter<String>,
) -> Result<(), String> {
    let mut env_home: Option<PathBuf> = None;
    let mut db_name: Option<String> = None;
    let mut in_file: Option<PathBuf> = None;
    let mut no_overwrite = false;
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "-h" => env_home = Some(next_value(&mut iter, "-h")?.into()),
            "-s" => db_name = Some(next_value(&mut iter, "-s")?),
            "-f" => in_file = Some(next_value(&mut iter, "-f")?.into()),
            "-n" => no_overwrite = true,
            other => return Err(format!("load: unknown flag '{other}'")),
        }
    }
    let env_home = env_home.ok_or("load: -h <env> is required")?;
    run_load(LoadArgs { env_home, db_name, in_file, no_overwrite })
}

fn parse_and_run_print_log(
    mut iter: std::vec::IntoIter<String>,
) -> Result<(), String> {
    let mut env_home: Option<PathBuf> = None;
    let mut summary = false;
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "-h" => env_home = Some(next_value(&mut iter, "-h")?.into()),
            "-S" => summary = true,
            other => return Err(format!("print-log: unknown flag '{other}'")),
        }
    }
    let env_home = env_home.ok_or("print-log: -h <env> is required")?;
    run_print_log(PrintLogArgs { env_home, summary })
}
