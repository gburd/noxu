//! Regression test for the REG-CLEANER-DISKLIMIT / Decision-1A cleaner
//! LN-migration data-loss defect.
//!
//! # The bug (reproduced by this test)
//!
//! Decision 1A makes `Tree::serialize_full` write an LSN-pointer-only slot
//! (`has_data = 0`, `data = None`) for any LN whose value length exceeds
//! `TREE_MAX_EMBEDDED_LN` (default 16 bytes) — the authoritative copy of a
//! large value is the standalone LN log entry, not the BIN slot.  Seven of the
//! eight read paths correctly fetch a `data = None` slot from its LSN, but the
//! CLEANER LN-MIGRATION path did not: `SharedTreeLookup::migrate_ln_slot` read
//! `bin.entries[idx].data` with no LSN fallback and `.unwrap_or_default()`.
//! For a reopen-materialised (or evictor-stripped) `> 16 B` slot (`data =
//! None`), migration wrote an EMPTY migration LN and re-inserted empty data,
//! PERMANENTLY DESTROYING the value (the empty migration LN becomes the
//! authoritative copy).
//!
//! JE migrates `lnFromLog` read from the log entry being cleaned
//! (`FileProcessor.processFoundLN`); Noxu's `LnInfo` does not carry the value,
//! so the cleaner must re-read it from the log at the slot's `tree_lsn`.
//!
//! # The scenario
//!
//! Write 2000 never-rewritten 512-byte records, churn 1000 keys x4, force a
//! checkpoint, close, then REOPEN (BINs materialise `data = None` for the
//! large / non-embedded slots), then call `env.clean_log()` + checkpoint x4
//! WITHOUT reading any key, then reopen and `get` all keys, asserting the FULL
//! value round-trips byte-for-byte.
//!
//! Pre-fix: the 1000 churned keys read back EMPTY (their surviving live
//! versions were migrated from `data = None` slots).  Control (no
//! `clean_log()`): no loss.  Post-fix: no loss with cleaning either.

use noxu_db::{
    CheckpointConfig, DatabaseConfig, Environment, EnvironmentConfig,
};
use tempfile::TempDir;

/// A 512-byte (`> TREE_MAX_EMBEDDED_LN`) value that encodes the key + a
/// generation byte, so a truncated or stale read is detectable.
fn value(k: u32, generation: u8) -> Vec<u8> {
    let mut v = vec![generation; 512];
    v[..4].copy_from_slice(&k.to_be_bytes());
    v
}

fn run(clean: bool) {
    let tmp = TempDir::new().unwrap();
    let dir = tmp.path().to_path_buf();
    let config = || {
        let mut cfg = EnvironmentConfig::new(dir.clone())
            .with_allow_create(true)
            .with_transactional(true)
            .with_log_file_max_bytes(64 * 1024)
            .with_cache_size(32 * 1024 * 1024);
        cfg.set_run_cleaner(false);
        cfg.set_run_checkpointer(false);
        cfg.set_run_evictor(false);
        cfg.set_run_in_compressor(false);
        cfg
    };
    let checkpoint = CheckpointConfig::new().with_force(true);

    // Phase 1: load 2000 never-rewritten large records + churn 1000 keys x4
    // so whole files of obsolete (superseded) versions accumulate.  Force a
    // checkpoint and close cleanly.
    {
        let env = Environment::open(config()).unwrap();
        let db_cfg = DatabaseConfig::new()
            .with_allow_create(true)
            .with_transactional(true);
        let db = env.open_database(None, "large", &db_cfg).unwrap();
        for k in 0u32..2000 {
            db.put(k.to_be_bytes(), value(k, 0)).unwrap();
        }
        for generation in 1..=4u8 {
            for k in 2000u32..3000 {
                db.put(k.to_be_bytes(), value(k, generation)).unwrap();
            }
        }
        env.checkpoint(Some(&checkpoint)).unwrap();
        db.close().unwrap();
        env.close().unwrap();
    }

    // Phase 2: REOPEN — recovery / checkpoint materialises the > 16 B slots as
    // `data = None` (LSN-pointer only).  Then clean_log() + checkpoint WITHOUT
    // reading any key, so the only source for a migrated value is the log.
    let peak;
    let mut cleaned: u32 = 0;
    {
        let env = Environment::open(config()).unwrap();
        // hold the db open so the cleaner can classify its LNs live/obsolete.
        let _db = env
            .open_database(
                None,
                "large",
                &DatabaseConfig::new().with_transactional(true),
            )
            .unwrap();
        peak = std::fs::read_dir(&dir)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|x| x == "ndb")
            })
            .count();
        for _ in 0..4 {
            if clean {
                cleaned += env.clean_log().unwrap();
            }
            env.checkpoint(Some(&checkpoint)).unwrap();
        }
        _db.close().unwrap();
        env.close().unwrap();
    }
    if clean {
        assert!(
            cleaned > 0,
            "cleaner must actually migrate/reclaim (else the test is vacuous)"
        );
    }

    // Phase 3: reopen and read back EVERY key.  Assert the FULL 512-byte value
    // round-trips byte-for-byte (not merely key presence — the defect returns
    // an EMPTY value for a migrated large record).
    let env = Environment::open(config()).unwrap();
    let db = env
        .open_database(
            None,
            "large",
            &DatabaseConfig::new().with_transactional(true),
        )
        .unwrap();
    let mut missing = 0;
    let mut empties = 0;
    let mut mismatches = Vec::new();
    for k in 0u32..3000 {
        let expected = value(k, if k < 2000 { 0 } else { 4 });
        match db.get(k.to_be_bytes()).unwrap() {
            None => missing += 1,
            Some(actual) if actual.is_empty() => empties += 1,
            Some(actual) if actual.as_ref() != expected.as_slice() => {
                mismatches.push(k)
            }
            Some(_) => {}
        }
    }
    eprintln!(
        "clean={clean} peak_files={peak} cleaned={cleaned}: \
         missing={missing} empties={empties} mismatches={}",
        mismatches.len()
    );
    assert_eq!(missing, 0, "no key may be lost (missing={missing})");
    assert_eq!(
        empties, 0,
        "a migrated large value must NOT be truncated to empty \
         (empties={empties}) — this is the 1A migration data-loss defect"
    );
    assert!(
        mismatches.is_empty(),
        "large value must round-trip byte-for-byte through cleaner migration; \
         {} mismatched, first: {:?}",
        mismatches.len(),
        &mismatches[..mismatches.len().min(20)]
    );
    db.close().unwrap();
    env.close().unwrap();
}

/// FAILS before the fix (the 1000 churned keys read back empty), PASSES after.
#[test]
fn cleaner_migration_large_value_roundtrip() {
    run(true);
}

/// Control: identical scenario without `clean_log()` — no migration, no loss.
/// Isolates the cause to cleaner migration.
#[test]
fn cleaner_migration_large_value_roundtrip_control() {
    run(false);
}
