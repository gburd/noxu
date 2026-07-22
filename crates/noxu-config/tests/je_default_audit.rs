//! JE 7.5.11 default-value regression guard (constant audit, 2026-07).
//!
//! This test locks the *default values* of the correctness-affecting
//! configuration parameters to the values in BDB-JE 7.5.11
//! `com.sleepycat.je.config.EnvironmentParams`. It is the durable artifact of
//! the systematic constant/default/threshold audit
//! (`docs/src/internal/je-constant-audit-2026-07.md`).
//!
//! Rationale: the audit was motivated by a real semantic-drift bug in
//! transaction durability where a constant's *value* silently diverged from
//! JE while its type signature stayed intact. A signature-level review misses
//! that; a value assertion does not. If a future edit silently changes one of
//! these defaults away from the JE value, this test fails and forces a
//! deliberate decision (change + document + update the audit report).
//!
//! Each assertion cites the JE source (`EnvironmentParams.java` line, JE
//! 7.5.11). Durations are compared in their native `Duration` form.

use noxu_config::param::ParamValue;
use noxu_config::params;
use std::time::Duration;

/// Helper: extract an i32 default or panic.
fn int_default(p: &noxu_config::ConfigParam) -> i32 {
    match p.default {
        ParamValue::Int(v) => v,
        ref other => panic!("expected Int default, got {other:?}"),
    }
}

fn long_default(p: &noxu_config::ConfigParam) -> i64 {
    match p.default {
        ParamValue::Long(v) => v,
        ref other => panic!("expected Long default, got {other:?}"),
    }
}

fn bool_default(p: &noxu_config::ConfigParam) -> bool {
    match p.default {
        ParamValue::Bool(v) => v,
        ref other => panic!("expected Bool default, got {other:?}"),
    }
}

fn dur_default(p: &noxu_config::ConfigParam) -> Duration {
    match p.default {
        ParamValue::Duration(v) => v,
        ref other => panic!("expected Duration default, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Memory / cache
// ---------------------------------------------------------------------------

#[test]
fn je_default_max_memory_percent() {
    // EnvironmentParams.java: MAX_MEMORY_PERCENT default = 60.
    assert_eq!(int_default(&params::MAX_MEMORY_PERCENT), 60);
}

// ---------------------------------------------------------------------------
// Log
// ---------------------------------------------------------------------------

#[test]
fn je_default_log_num_buffers() {
    // EnvironmentParams.java: NUM_LOG_BUFFERS_DEFAULT = 3.
    assert_eq!(int_default(&params::LOG_NUM_BUFFERS), 3);
}

#[test]
fn je_default_log_buffer_size() {
    // EnvironmentParams.java: LOG_BUFFER_SIZE default = 1 << 20 (1 MiB).
    assert_eq!(int_default(&params::LOG_BUFFER_SIZE), 1 << 20);
}

#[test]
fn je_default_log_file_max() {
    // EnvironmentParams.java: LOG_FILE_MAX default = 10_000_000L.
    assert_eq!(long_default(&params::LOG_FILE_MAX), 10_000_000);
}

// ---------------------------------------------------------------------------
// Tree fanout / BIN-delta
// ---------------------------------------------------------------------------

#[test]
fn je_default_node_max_entries() {
    // EnvironmentParams.java: NODE_MAX_ENTRIES default = 128.
    assert_eq!(int_default(&params::NODE_MAX_ENTRIES), 128);
}

#[test]
fn je_default_tree_bin_delta() {
    // EnvironmentParams.java: TREE_BIN_DELTA default = 25.
    assert_eq!(int_default(&params::TREE_BIN_DELTA), 25);
}

#[test]
fn je_default_tree_max_embedded_ln() {
    // EnvironmentParams.java: TREE_MAX_EMBEDDED_LN default = 16.
    assert_eq!(int_default(&params::TREE_MAX_EMBEDDED_LN), 16);
}

#[test]
fn je_default_tree_min_memory() {
    // EnvironmentParams.java: TREE_MIN_MEMORY default = 500L * 1024.
    assert_eq!(long_default(&params::TREE_MIN_MEMORY), 500 * 1024);
}

// ---------------------------------------------------------------------------
// Evictor
// ---------------------------------------------------------------------------

#[test]
fn je_default_evictor_evict_bytes() {
    // EnvironmentParams.java: EVICTOR_EVICT_BYTES default = 524288L.
    assert_eq!(long_default(&params::EVICTOR_EVICT_BYTES), 524_288);
}

// ---------------------------------------------------------------------------
// Checkpointer
// ---------------------------------------------------------------------------

#[test]
fn je_default_checkpointer_bytes_interval() {
    // EnvironmentParams.java: CHECKPOINTER_BYTES_INTERVAL default = 20_000_000L.
    assert_eq!(long_default(&params::CHECKPOINTER_BYTES_INTERVAL), 20_000_000);
}

// ---------------------------------------------------------------------------
// Cleaner (correctness-affecting thresholds)
// ---------------------------------------------------------------------------

#[test]
fn je_default_cleaner_min_utilization() {
    // EnvironmentParams.java: CLEANER_MIN_UTILIZATION default = 50.
    assert_eq!(int_default(&params::CLEANER_MIN_UTILIZATION), 50);
}

#[test]
fn je_default_cleaner_min_file_utilization() {
    // EnvironmentParams.java: CLEANER_MIN_FILE_UTILIZATION default = 5.
    assert_eq!(int_default(&params::CLEANER_MIN_FILE_UTILIZATION), 5);
}

#[test]
fn je_default_cleaner_min_age() {
    // EnvironmentParams.java: CLEANER_MIN_AGE default = 2.
    assert_eq!(int_default(&params::CLEANER_MIN_AGE), 2);
}

#[test]
fn je_default_cleaner_threads() {
    // EnvironmentParams.java: CLEANER_THREADS default = 1.
    assert_eq!(int_default(&params::CLEANER_THREADS), 1);
}

// ---------------------------------------------------------------------------
// Lock / txn timeouts (durability/isolation-adjacent)
// ---------------------------------------------------------------------------

#[test]
fn je_default_lock_timeout() {
    // EnvironmentParams.java: LOCK_TIMEOUT default = "500 ms".
    assert_eq!(dur_default(&params::LOCK_TIMEOUT), Duration::from_millis(500));
}

#[test]
fn je_default_txn_timeout() {
    // EnvironmentParams.java: TXN_TIMEOUT default = "0" (no timeout).
    assert_eq!(dur_default(&params::TXN_TIMEOUT), Duration::ZERO);
}

// ---------------------------------------------------------------------------
// Booleans that gate durability/recovery behavior
// ---------------------------------------------------------------------------

#[test]
fn je_default_env_is_locking() {
    // EnvironmentParams.java: ENV_IS_LOCKING default = true.
    assert!(bool_default(&params::ENV_IS_LOCKING));
}

#[test]
fn je_default_env_recovery() {
    // EnvironmentParams.java: ENV_RECOVERY default = true.
    assert!(bool_default(&params::ENV_RECOVERY));
}

#[test]
fn je_default_txn_serializable_isolation() {
    // EnvironmentParams.java: TXN_SERIALIZABLE_ISOLATION default = false
    // (read-committed is the default isolation, not serializable).
    assert!(!bool_default(&params::TXN_SERIALIZABLE_ISOLATION));
}

#[test]
fn je_default_log_verify_checksums() {
    // EnvironmentParams.java: LOG_VERIFY_CHECKSUMS default = false.
    assert!(!bool_default(&params::LOG_VERIFY_CHECKSUMS));
}
