//! Regression test for St-H6: silent data-loss when a BIN split sets
//! `expiration_in_hours = false` on the new right-half sibling instead of
//! inheriting the flag from the splitting BIN.
//!
//! # The bug (pre-fix)
//!
//! Every public TTL write goes through `WriteOptions::with_ttl` /
//! `with_expiration`, which stores `expiration_time` as **hours** since the
//! Unix epoch (via `noxu_util::ttl_hours_to_expiration`).  After insertion
//! `Database::put_with_options` calls `update_key_expiration`, which sets
//! `bin.expiration_in_hours = true` on the BIN that received the entry.
//! `is_expired(t, true)` then correctly compares `t` against
//! `current_time_hours()`.
//!
//! `Tree::split_child` (pre-fix) always hardcodes `expiration_in_hours: false`
//! on the newly-created right-half sibling BIN.  After the split:
//!
//! * The **left** half inherits the original BIN, which still has
//!   `expiration_in_hours = true` — correct.
//! * The **right** sibling is created fresh with `expiration_in_hours = false`
//!   — **wrong**.
//!
//! A subsequent `get` of any key in the right sibling calls
//! `is_expired(expiration_time_hours, false)`.  The hours-since-epoch value
//! (~495 000 as of 2026) is treated as *seconds* since epoch, which
//! corresponds to roughly 1970-01-06.  `is_expired` returns `true` →
//! the engine silently discards the record and returns `NotFound`.
//!
//! # Reproduction strategy
//!
//! The default `node_max_entries` for a freshly-opened database is 256 (the
//! `EnvironmentImpl` pre-seeds a recovered tree at `db_id=1` with 256 slots).
//! We fill a BIN to capacity by inserting all 256 single-byte keys (0x00–0xFF),
//! each with a 1 000-hour TTL.  The 257th insert uses a two-byte key
//! `[0x7f, 0xff]` whose sort position falls between `[0x7f]` and `[0x80]` —
//! **before** the split midpoint `[0x80]` — so after the split:
//!
//! - trigger key lands in the **left** half; `update_key_expiration` fixes
//!   `expiration_in_hours` on the left BIN only.
//! - keys `[0x80]`–`[0xff]` land in the **right** sibling, which retains
//!   `expiration_in_hours = false` (the pre-fix bug).
//!
//! Pre-fix: `get([0x80])` … `get([0xff])` return `NotFound` (128 keys lost).
//! Post-fix: all 257 keys are readable with correct data.
//!
//! # References
//! - JE: `BIN.java::setExpiration` always calls `setExpirationInHours(hours)`
//!   to propagate granularity; JE's `split` / `clone` carry the flag.
//! - St-H6 2026 review entry.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use noxu_db::{DatabaseConfig, DatabaseEntry, EnvironmentConfig};
use tempfile::TempDir;

/// Open an env + database with the default node size (256 entries per BIN).
fn open_env_and_db(dir: &TempDir) -> (noxu_db::Environment, noxu_db::Database) {
    let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = noxu_db::Environment::open(env_cfg).unwrap();
    let db_cfg = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, "ttl_split", &db_cfg).unwrap();
    (env, db)
}

// ──────────────────────────────────────────────────────────────────────────────
// St-H6 primary regression — FAIL-PRE / PASS-POST.
// ──────────────────────────────────────────────────────────────────────────────

/// **FAIL-PRE / PASS-POST** regression for St-H6.
///
/// Fills the first BIN (256 entries) with TTL records, then inserts a trigger
/// key that forces a split with the trigger landing in the left half.
///
/// Pre-fix: keys `[0x80]`–`[0xff]` (right sibling, `expiration_in_hours = false`)
/// are falsely treated as expired and return `NotFound` — 128 out of 256
/// records are silently lost.
///
/// Post-fix: all 257 keys return `Success` with correct data.
#[test]
fn test_ttl_records_survive_bin_split_right_sibling_256() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    let ttl_opts = noxu_db::WriteOptions::with_expiration(1_000); // 1 000 h TTL

    // Fill the BIN to capacity (256 entries) with TTL records.
    // Keys are all 256 single-byte values [0x00..=0xFF].
    for k in 0u8..=255u8 {
        let key = DatabaseEntry::from_bytes(&[k]);
        let val = DatabaseEntry::from_bytes(&[k, k]);
        db.put_with_options(None, &key, &val, &ttl_opts).unwrap();
    }

    // Insert the split-trigger key [0x7f, 0xff].
    //
    // Sort order: [0x7f, 0xff] > [0x7f] (shorter prefix rule), and
    // [0x7f, 0xff] < [0x80] (first byte 0x7f < 0x80).
    // So this key sorts BEFORE the split midpoint [0x80].
    //
    // After the BIN splits at index 128:
    //   left  = {[0x00]…[0x7f]}: expiration_in_hours = true  (original BIN)
    //   right = {[0x80]…[0xff]}: expiration_in_hours = false (pre-fix bug)
    //
    // The trigger key lands in the LEFT half → update_key_expiration fixes
    // the left BIN only.  The right sibling retains expiration_in_hours = false.
    let trigger_key = DatabaseEntry::from_bytes(b"\x7f\xff");
    let trigger_val = DatabaseEntry::from_bytes(b"trigger");
    db.put_with_options(None, &trigger_key, &trigger_val, &ttl_opts).unwrap();

    // Check right-sibling keys — these fail pre-fix.
    let mut missing: Vec<u8> = Vec::new();
    for k in 0u8..=255u8 {
        let key = DatabaseEntry::from_bytes(&[k]);
        let mut out = DatabaseEntry::new();
        match db.get_into(None, &key, &mut out).unwrap() {
            true => {
                assert_eq!(
                    out.data(),
                    &[k, k],
                    "get({k:02x}) returned wrong value"
                );
            }
            false => {
                missing.push(k);
            }
        }
    }
    // Trigger key must also be present.
    {
        let key = DatabaseEntry::from_bytes(b"\x7f\xff");
        let mut out = DatabaseEntry::new();
        if !(db.get_into(None, &key, &mut out).unwrap()) {
            panic!("trigger key [0x7f, 0xff] is missing after split");
        }
    }

    assert!(
        missing.is_empty(),
        "St-H6 BIN-split data-loss regression: {}/{} keys disappeared \
         after split (expiration_in_hours not inherited by right sibling).\n\
         First 10 missing: {:02x?}",
        missing.len(),
        256,
        &missing[..missing.len().min(10)]
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Variant: mixed TTL / no-TTL keys — FAIL-PRE / PASS-POST.
// ──────────────────────────────────────────────────────────────────────────────

/// Inserts 256 keys where even-byte keys have TTL and odd-byte keys do not.
/// Forces a BIN split.  Asserts ALL 256 keys are readable.
///
/// Pre-fix: even-byte keys in the right half `[0x80..0xff]` are falsely expired
/// (64 keys lost).  Odd-byte keys in the right half are fine (expiration_time = 0).
/// Post-fix: all 256 keys survive.
#[test]
fn test_ttl_and_no_ttl_keys_both_survive_bin_split() {
    let dir = TempDir::new().unwrap();
    let (_env, db) = open_env_and_db(&dir);

    let ttl_opts = noxu_db::WriteOptions::with_expiration(1_000);
    let no_ttl = noxu_db::WriteOptions::new();

    // Even keys have TTL; odd keys do not.
    for k in 0u8..=255u8 {
        let key = DatabaseEntry::from_bytes(&[k]);
        let val = DatabaseEntry::from_bytes(&[k]);
        let opts = if k % 2 == 0 { &ttl_opts } else { &no_ttl };
        db.put_with_options(None, &key, &val, opts).unwrap();
    }

    // Trigger key that splits with left-half landing.
    let trigger_key = DatabaseEntry::from_bytes(b"\x7f\xff");
    db.put_with_options(
        None,
        &trigger_key,
        DatabaseEntry::from_bytes(b"t"),
        &ttl_opts,
    )
    .unwrap();

    let mut missing = Vec::new();
    for k in 0u8..=255u8 {
        let key = DatabaseEntry::from_bytes(&[k]);
        let mut out = DatabaseEntry::new();
        match db.get_into(None, &key, &mut out).unwrap() {
            true => {}
            false => missing.push(k),
        }
    }

    assert!(
        missing.is_empty(),
        "St-H6 mixed-TTL split: {} keys missing (first 10: {:02x?})",
        missing.len(),
        &missing[..missing.len().min(10)]
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// Recovery guard (PASS-pre / PASS-post).
//
// This is a correctness guard, not a fail-pre test: after a clean close+reopen
// cycle, `expiration_time` is reset to 0 (LN records in the WAL do not carry
// expiration — it is in-memory only, see put_with_options audit note F8).
// `is_expired(0, …) = false` so records are visible regardless of
// `expiration_in_hours`.  This guard ensures that no other code path
// accidentally makes the right-sibling records appear expired after recovery.
//
// NOTE: uses `drop(env)` (exit-checkpoint), NOT `env.checkpoint(None)`.
// The explicit-checkpoint API has a known pre-existing limitation with
// multi-tree environments (not related to St-H6) that causes records inserted
// before the explicit checkpoint to be missing after reopen.
// ──────────────────────────────────────────────────────────────────────────────

/// Inserts TTL records, closes the environment (exit checkpoint), then reopens
/// and confirms all records are present.
///
/// Post-recovery, `expiration_time = 0` (never expires) for all entries because
/// the LN WAL records do not carry expiration times.  The test verifies that
/// records from the right-sibling BIN (which has `expiration_in_hours = false`
/// in its in-memory state before close) are correctly visible after reopen.
#[test]
fn test_ttl_records_survive_close_and_reopen() {
    let dir = TempDir::new().unwrap();

    // Phase 1: insert + split + close.
    {
        let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true);
        let env = noxu_db::Environment::open(env_cfg).unwrap();
        let db_cfg = DatabaseConfig::new().with_allow_create(true);
        let db = env.open_database(None, "ttl_recover", &db_cfg).unwrap();

        let ttl_opts = noxu_db::WriteOptions::with_expiration(1_000);
        for k in 0u8..=255u8 {
            let key = DatabaseEntry::from_bytes(&[k]);
            let val = DatabaseEntry::from_bytes(&[k]);
            db.put_with_options(None, &key, &val, &ttl_opts).unwrap();
        }
        // Trigger split with left-half landing.
        let trigger_key = DatabaseEntry::from_bytes(b"\x7f\xff");
        db.put_with_options(
            None,
            &trigger_key,
            DatabaseEntry::from_bytes(b"t"),
            &ttl_opts,
        )
        .unwrap();

        drop(db);
        drop(env); // triggers exit checkpoint + WAL fsync
    }

    // Phase 2: reopen and verify.
    {
        let env_cfg = EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true);
        let env = noxu_db::Environment::open(env_cfg).unwrap();
        let db_cfg = DatabaseConfig::new().with_allow_create(true);
        let db = env.open_database(None, "ttl_recover", &db_cfg).unwrap();

        let mut missing_count = 0usize;
        for k in 0u8..=255u8 {
            let key = DatabaseEntry::from_bytes(&[k]);
            let mut out = DatabaseEntry::new();
            match db.get_into(None, &key, &mut out).unwrap() {
                true => {
                    assert_eq!(
                        out.data(),
                        &[k],
                        "wrong value for key {:02x} after recovery",
                        k
                    );
                }
                false => {
                    missing_count += 1;
                }
            }
        }

        assert_eq!(
            missing_count, 0,
            "St-H6: {missing_count}/256 TTL records missing after \
             close+reopen"
        );
    }
}
