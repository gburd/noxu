//! D1: a read-write LN fetch failure must never be reported as success with
//! empty or missing data.
//!
//! JE `IN.fetchLN` / `IN.fetchTarget` (IN.java:2826-2843) throws
//! `EnvironmentFailureException` when a non-resident LN cannot be read:
//! `LOG_FILE_NOT_FOUND` for a missing file (unless the slot is KD/PD or
//! probably expired), `LOG_CHECKSUM` for a checksum failure or short read
//! (LogManager.java:927-939, 1055-1113), `LOG_READ` for other I/O errors.
//! These reasons invalidate the environment
//! (EnvironmentFailureReason.java:61-77, EnvironmentFailureException.java:
//! 159-166).
//!
//! Every value here is 64 bytes (> TREE_MAX_EMBEDDED_LN = 16), so the BIN
//! slot holds only the LN's LSN after reopen and every read must fetch the
//! LN from its log file.  The faults keep the tree intact (the checkpointed
//! BINs live in newer files) and damage only the LN's file.
use noxu_db::{
    CheckpointConfig, Database, DatabaseConfig, DatabaseEntry, Environment,
    EnvironmentConfig, EnvironmentFailureReason, Get, NoxuError,
    OperationStatus, SecondaryConfig, SecondaryDatabase, SecondaryKeyCreator,
};
use noxu_sync::Mutex;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::TempDir;

const VICTIM: &[u8] = b"v";
const MARKER: &[u8] = b"VICTIM-VALUE-";

fn val(i: u32) -> Vec<u8> {
    let mut v = format!("a-value-{i:05}-").into_bytes();
    v.resize(64, b'x');
    v
}

fn victim_val() -> Vec<u8> {
    let mut v = MARKER.to_vec();
    v.resize(64, b'v');
    v
}

/// sec_key = first byte of the primary value.
struct FirstByte;
impl SecondaryKeyCreator for FirstByte {
    fn create_secondary_key(
        &self,
        _db: &Database,
        _key: &DatabaseEntry,
        data: &DatabaseEntry,
        result: &mut DatabaseEntry,
    ) -> bool {
        match data.data_opt() {
            Some(d) if !d.is_empty() => {
                result.set_data(&d[..1]);
                true
            }
            _ => false,
        }
    }
}

fn env_cfg(dir: &Path, ro: bool) -> EnvironmentConfig {
    let mut cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(!ro)
        .with_read_only(ro)
        .with_transactional(true)
        .with_log_file_max_bytes(4096);
    cfg.set_run_cleaner(false);
    cfg.set_run_checkpointer(false);
    cfg.set_run_evictor(false);
    cfg.set_run_in_compressor(false);
    cfg.set_run_verifier(false);
    cfg.set_stats_collect(false);
    cfg
}

fn db_cfg(ro: bool, dups: bool) -> DatabaseConfig {
    DatabaseConfig::new()
        .with_allow_create(!ro)
        .with_read_only(ro)
        .with_transactional(true)
        .with_sorted_duplicates(dups)
}

struct Opened {
    env: Environment,
    pri: Arc<Mutex<Database>>,
    sec: SecondaryDatabase,
    dups: Database,
}

fn open_all(dir: &Path, ro: bool) -> Opened {
    let env = Environment::open(env_cfg(dir, ro))
        .unwrap_or_else(|e| panic!("open env ro={ro}: {e}"));
    let pri = env.open_database(None, "pri", &db_cfg(ro, false)).unwrap();
    let pri = Arc::new(Mutex::new(pri));
    let sec_db = env.open_database(None, "sec", &db_cfg(ro, true)).unwrap();
    let sec = SecondaryDatabase::open(
        Arc::clone(&pri),
        sec_db,
        SecondaryConfig::new()
            .with_allow_create(!ro)
            .with_key_creator(Box::new(FirstByte)),
    )
    .unwrap();
    let dups = env.open_database(None, "dups", &db_cfg(ro, true)).unwrap();
    Opened { env, pri, sec, dups }
}

fn log_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "ndb"))
        .collect();
    v.sort();
    v
}

/// First (oldest) log file containing `needle`, and the offset in it.
fn locate(dir: &Path, needle: &[u8]) -> (PathBuf, usize) {
    let files = log_files(dir);
    for f in &files {
        let bytes = std::fs::read(f).unwrap();
        if let Some(off) = bytes.windows(needle.len()).position(|w| w == needle)
        {
            assert!(
                f != files.last().unwrap(),
                "fixture: {needle:?} first appears in the newest file"
            );
            return (f.clone(), off);
        }
    }
    panic!("fixture: {needle:?} not found in any log file");
}

/// Builds the RW fixture: 60 records, the victim (64-byte value), a 0-byte
/// value, a dup set and a deleted key, then filler so the checkpointed BINs
/// land in newer files.  Clean close.
fn build(dir: &Path) {
    let o = open_all(dir, false);
    {
        let pri = o.pri.lock();
        for i in 0..60u32 {
            pri.put(format!("a{i:04}"), val(i)).unwrap();
        }
        pri.put(VICTIM, victim_val()).unwrap();
        pri.put(b"vzero", b"").unwrap();
        pri.put(b"vdel", val(9999)).unwrap();
        assert!(pri.delete(b"vdel").unwrap());
    }
    o.dups.put(b"DUPKEY-MARKER", b"one").unwrap();
    o.dups.put(b"DUPKEY-MARKER", b"two").unwrap();
    {
        let pri = o.pri.lock();
        for i in 0..150u32 {
            pri.put(format!("w{i:04}"), val(i)).unwrap();
        }
    }
    o.env.checkpoint(Some(&CheckpointConfig::new().with_force(true))).unwrap();
    let Opened { env, pri, sec, dups } = o;
    sec.close().unwrap();
    drop(sec);
    dups.close().unwrap();
    pri.lock().close().unwrap();
    drop(pri);
    env.close().unwrap();
}

fn copy_dir(src: &Path) -> TempDir {
    let dst = TempDir::new().unwrap();
    for f in log_files(src) {
        std::fs::copy(&f, dst.path().join(f.file_name().unwrap())).unwrap();
    }
    dst
}

#[derive(Clone, Copy, Debug)]
enum Fault {
    /// Flip one byte of the LN's value: CRC mismatch, file header intact.
    Corrupt,
    /// Remove the file holding the LN (the BINs live in newer files).
    Remove,
    /// Truncate the file in the middle of the LN entry (short read).
    Truncate,
}

impl Fault {
    fn reason(self) -> EnvironmentFailureReason {
        match self {
            Fault::Corrupt | Fault::Truncate => {
                EnvironmentFailureReason::LogChecksum
            }
            Fault::Remove => EnvironmentFailureReason::LogFileNotFound,
        }
    }
    fn apply(self, dir: &Path) {
        let (file, off) = locate(dir, MARKER);
        match self {
            Fault::Corrupt => {
                let mut b = std::fs::read(&file).unwrap();
                b[off + 3] ^= 0xFF;
                std::fs::write(&file, b).unwrap();
            }
            Fault::Remove => std::fs::remove_file(&file).unwrap(),
            Fault::Truncate => {
                let f = std::fs::OpenOptions::new()
                    .write(true)
                    .open(&file)
                    .unwrap();
                f.set_len((off + 4) as u64).unwrap();
            }
        }
    }
}

const FAULTS: [Fault; 3] = [Fault::Corrupt, Fault::Remove, Fault::Truncate];

fn fixture() -> TempDir {
    let d = TempDir::new().unwrap();
    build(d.path());
    d
}

fn assert_env_failure<T: std::fmt::Debug>(
    r: noxu_db::Result<T>,
    want: &EnvironmentFailureReason,
    what: &str,
) {
    match r {
        Err(NoxuError::EnvironmentFailure { ref reason, .. })
            if reason == want => {}
        other => {
            panic!("{what}: expected EnvironmentFailure({want}), got {other:?}")
        }
    }
}

/// After an invalidating fetch failure the environment must be invalid and
/// later operations must fail (JE `checkIfInvalid`).
fn assert_invalidated(o: &Opened, what: &str) {
    assert!(
        !o.env.is_valid(),
        "{what}: env still valid after LN fetch failure"
    );
    let r = o.pri.lock().get(b"a0000");
    assert!(r.is_err(), "{what}: later get on an invalid env returned {r:?}");
}

/// Runs `op` on a freshly faulted copy of the fixture, for every fault.
fn for_each_fault(op: impl Fn(&Opened, Fault)) {
    let base = fixture();
    for fault in FAULTS {
        let d = copy_dir(base.path());
        fault.apply(d.path());
        let o = open_all(d.path(), false);
        op(&o, fault);
    }
}

#[test]
fn db_get_ln_fetch_failure_is_typed_env_failure() {
    for_each_fault(|o, f| {
        let what = format!("db.get {f:?}");
        assert_env_failure(o.pri.lock().get(VICTIM), &f.reason(), &what);
        assert_invalidated(o, &what);
    });
}

fn cursor_op(
    o: &Opened,
    get: Get,
    key: &[u8],
    data: &[u8],
) -> noxu_db::Result<(OperationStatus, Vec<u8>)> {
    let pri = o.pri.lock();
    let mut c = pri.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::from_bytes(key);
    let mut d = DatabaseEntry::from_bytes(data);
    let r = c.get(&mut k, &mut d, get, None);
    let out = r.map(|s| (s, d.data_opt().unwrap_or(&[]).to_vec()));
    let _ = c.close();
    out
}

#[test]
fn cursor_search_ln_fetch_failure_is_typed_env_failure() {
    let victim = victim_val();
    for (get, key, data) in [
        (Get::Search, VICTIM, &b""[..]),
        (Get::SearchBoth, VICTIM, &victim[..]),
        (Get::SearchGte, VICTIM, &b""[..]),
        // Range miss: positions on the victim via the find_range_entry path.
        (Get::SearchGte, &b"u"[..], &b""[..]),
    ] {
        for_each_fault(|o, f| {
            let what = format!("cursor {get:?}({key:?}) {f:?}");
            assert_env_failure(
                cursor_op(o, get, key, data),
                &f.reason(),
                &what,
            );
            assert_invalidated(o, &what);
        });
    }
}

#[test]
fn cursor_scan_ln_fetch_failure_is_typed_env_failure() {
    for_each_fault(|o, f| {
        let what = format!("scan {f:?}");
        let pri = o.pri.lock();
        let mut c = pri.open_cursor(None).unwrap();
        let mut k = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        let err = loop {
            match c.get(&mut k, &mut d, Get::Next, None) {
                Ok(OperationStatus::Success) => {
                    assert_ne!(
                        k.data(),
                        VICTIM,
                        "{what}: victim returned as Success with {:?}",
                        d.data_opt()
                    );
                }
                Ok(s) => panic!("{what}: scan ended with {s:?}, no error"),
                Err(e) => break e,
            }
        };
        let _ = c.close();
        drop(pri);
        assert_env_failure::<()>(Err(err), &f.reason(), &what);
        assert_invalidated(o, &what);
    });
}

#[test]
fn get_current_and_count_after_fetch_failure_error() {
    for_each_fault(|o, f| {
        let what = format!("Get::Current {f:?}");
        let pri = o.pri.lock();
        let mut c = pri.open_cursor(None).unwrap();
        let mut k = DatabaseEntry::from_bytes(b"a0001");
        let mut d = DatabaseEntry::new();
        assert_eq!(
            c.get(&mut k, &mut d, Get::Search, None).unwrap(),
            OperationStatus::Success
        );
        assert_eq!(d.data(), &val(1)[..], "control: full value");
        assert_env_failure(pri.get(VICTIM), &f.reason(), &what);
        let cur = c.get(&mut k, &mut d, Get::Current, None);
        assert!(cur.is_err(), "{what}: Get::Current on invalid env: {cur:?}");
        let n = c.count();
        assert!(n.is_err(), "{what}: count on invalid env: {n:?}");
        let _ = c.close();
    });
}

#[test]
fn secondary_get_ln_fetch_failure_is_typed_env_failure() {
    for_each_fault(|o, f| {
        let what = format!("secondary get {f:?}");
        let mut pk = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        let r = o.sec.get_into(None, b"V", &mut pk, &mut d);
        assert_env_failure(r, &f.reason(), &what);
        assert_invalidated(o, &what);
    });
}

/// JE `Cursor.checkOpenAndState(true)` (Cursor.java:5381-5385) runs
/// `checkEnv` for `getCurrent` and `count` (Cursor.java:2136-2138).
#[test]
fn get_current_and_count_check_env_validity() {
    let d = TempDir::new().unwrap();
    let o = open_all(d.path(), false);
    let pri = o.pri.lock();
    pri.put(b"k", val(1)).unwrap();
    let mut c = pri.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::from_bytes(b"k");
    let mut v = DatabaseEntry::new();
    assert_eq!(
        c.get(&mut k, &mut v, Get::Search, None).unwrap(),
        OperationStatus::Success
    );
    o.env.invalidate();
    let cur = c.get(&mut k, &mut v, Get::Current, None);
    assert!(
        matches!(cur, Err(NoxuError::EnvironmentFailure { .. })),
        "Get::Current on an invalidated env: {cur:?}"
    );
    let n = c.count();
    assert!(
        matches!(n, Err(NoxuError::EnvironmentFailure { .. })),
        "count on an invalidated env: {n:?}"
    );
    let _ = c.close();
}

/// Controls: genuinely absent (deleted) records are NotFound; a 0-byte value
/// and a duplicates DB round-trip even when their LN file is gone (a 0-byte
/// value is embedded; JE never fetches LNs of a duplicates DB,
/// IN.java:2780-2783).  The environment stays valid through all of them.
///
/// Last step: `Get::Prev` from the 0-byte record lands on the victim.  With
/// the files intact that returns the full value.  When the removed file also
/// holds the victim's LN it MUST fail with LOG_FILE_NOT_FOUND: JE
/// `CursorImpl.getNext` (backward) -> `lockAndGetCurrent` -> `getCurrent` ->
/// `bin.fetchLN` (CursorImpl.java:2546-2600, 2250-2251) throws
/// `EnvironmentFailureException(LOG_FILE_NOT_FOUND)` for a live slot whose
/// file is missing (IN.java:2826-2833), which invalidates the env.
#[test]
fn controls_deleted_zero_byte_and_dups_need_no_fetch() {
    let base = fixture();
    for remove in [false, true] {
        let d = copy_dir(base.path());
        if remove {
            let (zf, _) = locate(d.path(), b"vzero");
            let (df, _) = locate(d.path(), b"DUPKEY-MARKER");
            let (vf, _) = locate(d.path(), MARKER);
            assert_eq!(zf, df, "fixture: 0-byte LN and dup LNs share a file");
            assert_eq!(zf, vf, "fixture: the victim LN shares that file");
            std::fs::remove_file(&zf).unwrap();
        }
        let o = open_all(d.path(), false);
        let what = format!("controls remove={remove}");
        let mut dc = o.dups.open_cursor(None).unwrap();
        let mut k = DatabaseEntry::from_bytes(b"DUPKEY-MARKER");
        let mut v = DatabaseEntry::new();
        let mut got = Vec::new();
        let mut s = dc.get(&mut k, &mut v, Get::Search, None).unwrap();
        while s == OperationStatus::Success {
            got.push(v.data().to_vec());
            assert_eq!(dc.count().unwrap(), 2, "{what}: dup count");
            s = dc.get(&mut k, &mut v, Get::NextDup, None).unwrap();
        }
        assert_eq!(got, vec![b"one".to_vec(), b"two".to_vec()], "{what}");
        let _ = dc.close();
        let pri = o.pri.lock();
        assert_eq!(pri.get(b"vdel").unwrap(), None, "{what}: deleted");
        assert_eq!(pri.get(b"nosuch").unwrap(), None, "{what}: absent");
        assert_eq!(
            pri.get(b"vzero").unwrap().as_deref(),
            Some(&b""[..]),
            "{what}: 0-byte get"
        );
        // Position on the 0-byte record through the scan/range paths.
        let mut c = pri.open_cursor(None).unwrap();
        let mut k = DatabaseEntry::from_bytes(b"vz");
        let mut v = DatabaseEntry::from_bytes(b"junk");
        assert_eq!(
            c.get(&mut k, &mut v, Get::SearchGte, None).unwrap(),
            OperationStatus::Success,
            "{what}"
        );
        assert_eq!(k.data(), b"vzero", "{what}");
        assert_eq!(v.data_opt().unwrap_or(&[]), b"", "{what}: 0-byte gte");
        assert!(o.env.is_valid(), "{what}: env invalidated by a control");
        let prev = c.get(&mut k, &mut v, Get::Prev, None);
        if remove {
            assert_env_failure(
                prev,
                &EnvironmentFailureReason::LogFileNotFound,
                &what,
            );
            assert!(
                !o.env.is_valid(),
                "{what}: env valid after LOG_FILE_NOT_FOUND"
            );
        } else {
            assert_eq!(prev.unwrap(), OperationStatus::Success, "{what}");
            assert_eq!(k.data(), VICTIM, "{what}");
            assert_eq!(v.data(), &victim_val()[..], "{what}: victim");
            assert!(o.env.is_valid(), "{what}");
        }
        let _ = c.close();
    }
}

/// Control (929decd1): a read-only environment still returns full values.
#[test]
fn readonly_reads_return_full_values() {
    let base = fixture();
    let o = open_all(base.path(), true);
    let pri = o.pri.lock();
    assert_eq!(pri.get(VICTIM).unwrap().as_deref(), Some(&victim_val()[..]));
    let mut c = pri.open_cursor(None).unwrap();
    let mut k = DatabaseEntry::new();
    let mut v = DatabaseEntry::new();
    let mut n = 0;
    while c.get(&mut k, &mut v, Get::Next, None).unwrap()
        == OperationStatus::Success
    {
        if k.data() != b"vzero" {
            assert_eq!(v.data().len(), 64, "RO scan {:?}", k.data());
        }
        n += 1;
    }
    assert_eq!(n, 60 + 2 + 150);
    let _ = c.close();
}
