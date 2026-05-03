//! Property-based tests for noxu-config.

use noxu_config::manager::ConfigManager;
use noxu_config::param::ParamValue;
use noxu_config::params;
use proptest::prelude::*;

// =============================================================================
// ConfigManager property tests
// =============================================================================

proptest! {
    /// Setting an integer parameter within its valid range and reading it back
    /// yields the same value. MAX_MEMORY_PERCENT has range [1, 90].
    #[test]
    fn config_int_set_get_roundtrip(val in 1i32..=90i32) {
        let mut mgr = ConfigManager::new();
        mgr.set("je.maxMemoryPercent", ParamValue::Int(val), false).unwrap();
        let result = mgr.get_int(&params::MAX_MEMORY_PERCENT);
        prop_assert_eq!(result, val);
    }

    /// Setting an integer parameter out of range fails validation.
    #[test]
    fn config_int_out_of_range_rejected(val in 91i32..=i32::MAX) {
        let mut mgr = ConfigManager::new();
        let result = mgr.set("je.maxMemoryPercent", ParamValue::Int(val), false);
        prop_assert!(result.is_err());
    }

    /// Setting a long parameter within its valid range and reading it back.
    /// LOG_FILE_MAX has range [1_000_000, 1_073_741_824].
    #[test]
    fn config_long_set_get_roundtrip(val in 1_000_000i64..=1_073_741_824i64) {
        let mut mgr = ConfigManager::new();
        mgr.set("je.log.fileMax", ParamValue::Long(val), false).unwrap();
        let result = mgr.get_long(&params::LOG_FILE_MAX);
        prop_assert_eq!(result, val);
    }

    /// Setting a long parameter below minimum is rejected.
    #[test]
    fn config_long_below_min_rejected(val in 0i64..1_000_000i64) {
        let mut mgr = ConfigManager::new();
        let result = mgr.set("je.log.fileMax", ParamValue::Long(val), false);
        prop_assert!(result.is_err());
    }

    /// Boolean parameters set/get round-trip.
    #[test]
    fn config_bool_set_get_roundtrip(val: bool) {
        let mut mgr = ConfigManager::new();
        mgr.set("je.env.recovery", ParamValue::Bool(val), false).unwrap();
        let result = mgr.get_bool(&params::ENV_RECOVERY);
        prop_assert_eq!(result, val);
    }

    /// Type mismatch is rejected: setting a bool parameter with an int value.
    #[test]
    fn config_type_mismatch_rejected(val: i32) {
        let mut mgr = ConfigManager::new();
        let result = mgr.set("je.env.recovery", ParamValue::Int(val), false);
        prop_assert!(result.is_err());
    }

    /// Unknown parameter names are rejected.
    #[test]
    fn config_unknown_param_rejected(name in "je\\.unknown\\.[a-z]{1,20}") {
        let mut mgr = ConfigManager::new();
        let result = mgr.set(&name, ParamValue::Bool(true), false);
        prop_assert!(result.is_err());
    }

    /// Non-mutable parameters cannot be set when is_open is true.
    /// LOG_FILE_MAX is not mutable.
    #[test]
    fn config_mutability_enforced(val in 1_000_000i64..=1_073_741_824i64) {
        let mut mgr = ConfigManager::new();
        let result = mgr.set("je.log.fileMax", ParamValue::Long(val), true);
        prop_assert!(result.is_err());
    }

    /// Mutable parameters can be set when is_open is true.
    /// MAX_MEMORY_PERCENT is mutable.
    #[test]
    fn config_mutable_param_set_when_open(val in 1i32..=90i32) {
        let mut mgr = ConfigManager::new();
        let result = mgr.set("je.maxMemoryPercent", ParamValue::Int(val), true);
        prop_assert!(result.is_ok());
    }
}
