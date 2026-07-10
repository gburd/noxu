//! Property-based tests for noxu-config (Hegel / hegeltest).

use hegel::generators;
use noxu_config::manager::ConfigManager;
use noxu_config::param::ParamValue;
use noxu_config::params;

// =============================================================================
// ConfigManager property tests
// =============================================================================

/// Setting an integer parameter within its valid range and reading it back
/// yields the same value. MAX_MEMORY_PERCENT has range [1, 90].
#[hegel::test]
fn config_int_set_get_roundtrip(tc: hegel::TestCase) {
    let val = tc.draw(generators::integers::<i32>().min_value(1).max_value(90));
    let mut mgr = ConfigManager::new();
    mgr.set("noxu.maxMemoryPercent", ParamValue::Int(val), false).unwrap();
    let result = mgr.get_int(&params::MAX_MEMORY_PERCENT);
    assert_eq!(result, val);
}

/// Setting an integer parameter out of range fails validation.
#[hegel::test]
fn config_int_out_of_range_rejected(tc: hegel::TestCase) {
    let val = tc
        .draw(generators::integers::<i32>().min_value(91).max_value(i32::MAX));
    let mut mgr = ConfigManager::new();
    let result = mgr.set("noxu.maxMemoryPercent", ParamValue::Int(val), false);
    assert!(result.is_err());
}

/// Setting a long parameter within its valid range and reading it back.
/// LOG_FILE_MAX has range [1_000_000, 1_073_741_824].
#[hegel::test]
fn config_long_set_get_roundtrip(tc: hegel::TestCase) {
    let val = tc.draw(
        generators::integers::<i64>()
            .min_value(1_000_000)
            .max_value(1_073_741_824),
    );
    let mut mgr = ConfigManager::new();
    mgr.set("noxu.log.fileMax", ParamValue::Long(val), false).unwrap();
    let result = mgr.get_long(&params::LOG_FILE_MAX);
    assert_eq!(result, val);
}

/// Setting a long parameter below minimum is rejected.
#[hegel::test]
fn config_long_below_min_rejected(tc: hegel::TestCase) {
    let val =
        tc.draw(generators::integers::<i64>().min_value(0).max_value(999_999));
    let mut mgr = ConfigManager::new();
    let result = mgr.set("noxu.log.fileMax", ParamValue::Long(val), false);
    assert!(result.is_err());
}

/// Boolean parameters set/get round-trip.
#[hegel::test]
fn config_bool_set_get_roundtrip(tc: hegel::TestCase) {
    let val = tc.draw(generators::booleans());
    let mut mgr = ConfigManager::new();
    mgr.set("noxu.env.recovery", ParamValue::Bool(val), false).unwrap();
    let result = mgr.get_bool(&params::ENV_RECOVERY);
    assert_eq!(result, val);
}

/// Type mismatch is rejected: setting a bool parameter with an int value.
#[hegel::test]
fn config_type_mismatch_rejected(tc: hegel::TestCase) {
    let val = tc.draw(generators::integers::<i32>());
    let mut mgr = ConfigManager::new();
    let result = mgr.set("noxu.env.recovery", ParamValue::Int(val), false);
    assert!(result.is_err());
}

/// Unknown parameter names are rejected.
#[hegel::test]
fn config_unknown_param_rejected(tc: hegel::TestCase) {
    let name = tc.draw(
        generators::from_regex(r"je\.unknown\.[a-z]{1,20}").fullmatch(true),
    );
    let mut mgr = ConfigManager::new();
    let result = mgr.set(&name, ParamValue::Bool(true), false);
    assert!(result.is_err());
}

/// Non-mutable parameters cannot be set when is_open is true.
/// LOG_FILE_MAX is not mutable.
#[hegel::test]
fn config_mutability_enforced(tc: hegel::TestCase) {
    let val = tc.draw(
        generators::integers::<i64>()
            .min_value(1_000_000)
            .max_value(1_073_741_824),
    );
    let mut mgr = ConfigManager::new();
    let result = mgr.set("noxu.log.fileMax", ParamValue::Long(val), true);
    assert!(result.is_err());
}

/// Mutable parameters can be set when is_open is true.
/// MAX_MEMORY_PERCENT is mutable.
#[hegel::test]
fn config_mutable_param_set_when_open(tc: hegel::TestCase) {
    let val = tc.draw(generators::integers::<i32>().min_value(1).max_value(90));
    let mut mgr = ConfigManager::new();
    let result = mgr.set("noxu.maxMemoryPercent", ParamValue::Int(val), true);
    assert!(result.is_ok());
}
