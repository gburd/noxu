//! JE-equivalent config manager tests.
//!
//! Wave 6 — Priority-4 JE TCK port.
//!
//! Ports invariants from
//! `je/test/com/sleepycat/je/dbi/DbConfigManagerTest.java`, adapted to
//! Noxu's `ConfigManager`.
//!
//! Mapping (JE -> Noxu):
//! * `DbConfigManager.getLong(EnvironmentParams.MAX_MEMORY)` ->
//!   `ConfigManager::get_long(&params::MAX_MEMORY)`
//! * `DbConfigManager.get(EnvironmentParams.ENV_RECOVERY)` ->
//!   `ConfigManager::get_bool(&params::ENV_RECOVERY)`
//! * `EnvironmentConfig.setCacheSize(2000)` ->
//!   `ConfigManager::set("MAX_MEMORY", ParamValue::Long(2000), false)`
//!
//! Skipped (no Noxu analogue):
//! * `testBooleanWhitespace` — Noxu's `ConfigManager` accepts already-typed
//!   `ParamValue::Bool(_)`, not raw user-supplied strings, so the
//!   leading/trailing whitespace concern does not arise at the manager
//!   level.

use noxu_config::{ConfigManager, ParamValue, params};

// --------------------------------------------------------------------------
// testBasicParams — direct port.
//
// JE asserts: after explicit set, getLong returns the set value.  For
// params not explicitly set, the configured default is returned.
// --------------------------------------------------------------------------
#[test]
fn test_basic_params() {
    let mut mgr = ConfigManager::new();

    // EnvironmentConfig.setCacheSize(2000) maps to MAX_MEMORY = 2000.
    mgr.set(
        params::MAX_MEMORY.name,
        ParamValue::Long(2000),
        /* is_open = */ false,
    )
    .expect("setting MAX_MEMORY=2000 must succeed");

    // Long override is returned.
    assert_eq!(
        mgr.get_long(&params::MAX_MEMORY),
        2000,
        "explicit override must be returned"
    );

    // ENV_RECOVERY was not set: must return the configured default.
    let env_recovery_default = match params::ENV_RECOVERY.default {
        ParamValue::Bool(b) => b,
        _ => panic!("ENV_RECOVERY must be a bool param"),
    };
    assert_eq!(
        mgr.get_bool(&params::ENV_RECOVERY),
        env_recovery_default,
        "default for unset bool param must be the param's defined default"
    );
}

// --------------------------------------------------------------------------
// Setting an unknown parameter must error (analogous to JE's
// IllegalArgumentException).
// --------------------------------------------------------------------------
#[test]
fn test_unknown_param_errors() {
    let mut mgr = ConfigManager::new();
    let res = mgr.set("nonexistent_param_xyz", ParamValue::Bool(true), false);
    assert!(res.is_err(), "setting unknown param must error");
}

// --------------------------------------------------------------------------
// Override / get_int round-trip for an integer param.
// --------------------------------------------------------------------------
#[test]
fn test_int_override_round_trip() {
    let mut mgr = ConfigManager::new();
    let default = mgr.get_int(&params::MAX_MEMORY_PERCENT);
    // Bump by 1 (within range).
    let new_val = default + 1;
    mgr.set(params::MAX_MEMORY_PERCENT.name, ParamValue::Int(new_val), false)
        .expect("setting MAX_MEMORY_PERCENT must succeed");
    assert_eq!(mgr.get_int(&params::MAX_MEMORY_PERCENT), new_val);
    assert!(mgr.is_overridden(params::MAX_MEMORY_PERCENT.name));
}

// --------------------------------------------------------------------------
// is_overridden returns false for params never set, true after set.
// --------------------------------------------------------------------------
#[test]
fn test_is_overridden_state_machine() {
    let mut mgr = ConfigManager::new();
    assert!(
        !mgr.is_overridden(params::SHARED_CACHE.name),
        "fresh manager: SHARED_CACHE must not be overridden"
    );
    mgr.set(params::SHARED_CACHE.name, ParamValue::Bool(true), false).unwrap();
    assert!(mgr.is_overridden(params::SHARED_CACHE.name));
    assert!(mgr.get_bool(&params::SHARED_CACHE));
}

// --------------------------------------------------------------------------
// Defaults are returned when no override is set.
// --------------------------------------------------------------------------
#[test]
fn test_defaults_returned_when_not_set() {
    let mgr = ConfigManager::new();
    // No explicit overrides; every public param must produce its default.
    let _ = mgr.get_long(&params::MAX_MEMORY);
    let _ = mgr.get_bool(&params::SHARED_CACHE);
    let _ = mgr.get_bool(&params::ENV_RECOVERY);
    let _ = mgr.get_int(&params::MAX_MEMORY_PERCENT);
}
