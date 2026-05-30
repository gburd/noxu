//! Configuration manager.
//!
//! Manages the active configuration for an environment, supporting parameter
//! lookup, default values, and runtime mutation of mutable parameters.

use crate::config::param::{ConfigError, ConfigParam, ParamValue};
use hashbrown::HashMap;

/// Manages configuration parameters for a database environment.
///
/// Holds overridden parameter values and falls back to defaults for
/// parameters that have not been explicitly set.
///
/// Configuration manager for a database environment.
pub struct ConfigManager {
    /// Map from parameter name to overridden value.
    overrides: HashMap<String, ParamValue>,
    /// Known parameter definitions, keyed by name.
    definitions: HashMap<&'static str, &'static ConfigParam>,
}

impl ConfigManager {
    /// Creates a new ConfigManager with all known parameter definitions.
    pub fn new() -> Self {
        let mut definitions = HashMap::new();
        for param in crate::config::params::all_params() {
            definitions.insert(param.name, param);
        }
        ConfigManager { overrides: HashMap::new(), definitions }
    }

    /// Returns the effective value of a parameter (override or default).
    pub fn get(&self, name: &str) -> Option<&ParamValue> {
        if let Some(val) = self.overrides.get(name) {
            return Some(val);
        }
        self.definitions.get(name).map(|def| &def.default)
    }

    /// Returns a boolean parameter value.
    pub fn get_bool(&self, param: &ConfigParam) -> bool {
        self.get(param.name).and_then(|v| v.as_bool()).unwrap_or(false)
    }

    /// Returns an i32 parameter value.
    pub fn get_int(&self, param: &ConfigParam) -> i32 {
        self.get(param.name).and_then(|v| v.as_i32()).unwrap_or(0)
    }

    /// Returns an i64 parameter value.
    pub fn get_long(&self, param: &ConfigParam) -> i64 {
        self.get(param.name).and_then(|v| v.as_i64()).unwrap_or(0)
    }

    /// Returns a Duration parameter value.
    pub fn get_duration(&self, param: &ConfigParam) -> std::time::Duration {
        self.get(param.name).and_then(|v| v.as_duration()).unwrap_or_default()
    }

    /// Sets a parameter value, validating it first.
    ///
    /// For runtime changes, set `is_open` to true to enforce mutability checks.
    pub fn set(
        &mut self,
        name: &str,
        value: ParamValue,
        is_open: bool,
    ) -> Result<(), ConfigError> {
        let def = self.definitions.get(name).ok_or_else(|| {
            ConfigError::UnknownParam { name: name.to_string() }
        })?;

        if is_open && !def.mutable {
            return Err(ConfigError::NotMutable { name: def.name });
        }

        def.validate(&value)?;
        self.overrides.insert(name.to_string(), value);
        Ok(())
    }

    /// Returns true if a parameter has been explicitly overridden.
    pub fn is_overridden(&self, name: &str) -> bool {
        self.overrides.contains_key(name)
    }

    /// Returns the definition of a parameter by name, if known.
    pub fn get_definition(&self, name: &str) -> Option<&&'static ConfigParam> {
        self.definitions.get(name)
    }
}

impl Default for ConfigManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::params;

    #[test]
    fn test_defaults() {
        let mgr = ConfigManager::new();
        assert_eq!(mgr.get_int(&params::MAX_MEMORY_PERCENT), 60);
        assert!(mgr.get_bool(&params::ENV_RECOVERY));
        assert!(!mgr.get_bool(&params::LOG_MEM_ONLY));
    }

    #[test]
    fn test_override() {
        let mut mgr = ConfigManager::new();
        mgr.set("noxu.maxMemoryPercent", ParamValue::Int(75), false).unwrap();
        assert_eq!(mgr.get_int(&params::MAX_MEMORY_PERCENT), 75);
    }

    #[test]
    fn test_validation_rejects_out_of_range() {
        let mut mgr = ConfigManager::new();
        let result =
            mgr.set("noxu.maxMemoryPercent", ParamValue::Int(95), false);
        assert!(result.is_err());
    }

    #[test]
    fn test_mutability_enforcement() {
        let mut mgr = ConfigManager::new();
        // LOG_FILE_MAX is not mutable
        let result = mgr.set(
            "noxu.log.fileMax",
            ParamValue::Long(20_000_000),
            true, // is_open = true
        );
        assert!(matches!(result, Err(ConfigError::NotMutable { .. })));

        // But it can be set before open
        let result = mgr.set(
            "noxu.log.fileMax",
            ParamValue::Long(20_000_000),
            false, // is_open = false
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_unknown_param() {
        let mut mgr = ConfigManager::new();
        let result = mgr.set("noxu.nonexistent", ParamValue::Bool(true), false);
        assert!(matches!(result, Err(ConfigError::UnknownParam { .. })));
    }

    #[test]
    fn test_is_overridden() {
        let mut mgr = ConfigManager::new();
        assert!(!mgr.is_overridden("noxu.maxMemoryPercent"));
        mgr.set("noxu.maxMemoryPercent", ParamValue::Int(75), false).unwrap();
        assert!(mgr.is_overridden("noxu.maxMemoryPercent"));
    }

    #[test]
    fn test_get_definition() {
        let mgr = ConfigManager::new();
        let def = mgr.get_definition("noxu.maxMemoryPercent");
        assert!(def.is_some());
        assert_eq!(def.unwrap().name, "noxu.maxMemoryPercent");
        assert!(mgr.get_definition("noxu.nonexistent").is_none());
    }

    #[test]
    fn test_get_duration_param() {
        let mgr = ConfigManager::new();
        use std::time::Duration;
        let timeout = mgr.get_duration(&params::LOCK_TIMEOUT);
        assert_eq!(timeout, Duration::from_millis(500));
    }

    #[test]
    fn test_get_long_param() {
        let mgr = ConfigManager::new();
        assert_eq!(mgr.get_long(&params::LOG_FILE_MAX), 10_000_000);
        assert_eq!(mgr.get_long(&params::MAX_MEMORY), 0);
        assert_eq!(mgr.get_long(&params::FREE_DISK), 5_368_709_120);
    }

    #[test]
    fn test_get_bool_defaults() {
        let mgr = ConfigManager::new();
        assert!(mgr.get_bool(&params::ENV_RECOVERY));
        assert!(mgr.get_bool(&params::ENV_IS_LOCKING));
        assert!(!mgr.get_bool(&params::ENV_IS_TRANSACTIONAL));
        assert!(!mgr.get_bool(&params::ENV_IS_READ_ONLY));
        assert!(!mgr.get_bool(&params::LOG_MEM_ONLY));
        assert!(mgr.get_bool(&params::ENV_RUN_CHECKPOINTER));
        assert!(mgr.get_bool(&params::ENV_RUN_CLEANER));
        assert!(mgr.get_bool(&params::ENV_RUN_EVICTOR));
        assert!(mgr.get_bool(&params::ENV_RUN_IN_COMPRESSOR));
    }

    #[test]
    fn test_get_int_defaults() {
        let mgr = ConfigManager::new();
        assert_eq!(mgr.get_int(&params::MAX_MEMORY_PERCENT), 60);
        assert_eq!(mgr.get_int(&params::CLEANER_MIN_UTILIZATION), 50);
        assert_eq!(mgr.get_int(&params::CLEANER_MIN_AGE), 2);
        assert_eq!(mgr.get_int(&params::CLEANER_THREADS), 1);
        assert_eq!(mgr.get_int(&params::NODE_MAX_ENTRIES), 128);
        assert_eq!(mgr.get_int(&params::TREE_MAX_EMBEDDED_LN), 16);
        assert_eq!(mgr.get_int(&params::EVICTOR_CORE_THREADS), 1);
        assert_eq!(mgr.get_int(&params::EVICTOR_MAX_THREADS), 10);
        assert_eq!(mgr.get_int(&params::LOG_NUM_BUFFERS), 3);
    }

    #[test]
    fn test_set_get_roundtrip_all_types() {
        let mut mgr = ConfigManager::new();
        use std::time::Duration;

        // Bool
        mgr.set("noxu.env.runCleaner", ParamValue::Bool(false), false).unwrap();
        assert!(!mgr.get_bool(&params::ENV_RUN_CLEANER));

        // Int
        mgr.set("noxu.maxMemoryPercent", ParamValue::Int(80), false).unwrap();
        assert_eq!(mgr.get_int(&params::MAX_MEMORY_PERCENT), 80);

        // Long
        mgr.set("noxu.maxMemory", ParamValue::Long(512 * 1024 * 1024), false)
            .unwrap();
        assert_eq!(mgr.get_long(&params::MAX_MEMORY), 512 * 1024 * 1024);

        // Duration
        mgr.set(
            "noxu.lock.timeout",
            ParamValue::Duration(Duration::from_secs(2)),
            false,
        )
        .unwrap();
        assert_eq!(
            mgr.get_duration(&params::LOCK_TIMEOUT),
            Duration::from_secs(2)
        );
    }

    #[test]
    fn test_type_mismatch_rejected() {
        let mut mgr = ConfigManager::new();
        // Bool param given int
        let result = mgr.set("noxu.env.recovery", ParamValue::Int(1), false);
        assert!(matches!(result, Err(ConfigError::TypeMismatch { .. })));

        // Int param given bool
        let result =
            mgr.set("noxu.maxMemoryPercent", ParamValue::Bool(true), false);
        assert!(matches!(result, Err(ConfigError::TypeMismatch { .. })));

        // Long param given int
        let result = mgr.set("noxu.maxMemory", ParamValue::Int(1024), false);
        assert!(matches!(result, Err(ConfigError::TypeMismatch { .. })));
    }

    #[test]
    fn test_int_out_of_range_min() {
        let mut mgr = ConfigManager::new();
        // MAX_MEMORY_PERCENT min is 1
        let result =
            mgr.set("noxu.maxMemoryPercent", ParamValue::Int(0), false);
        assert!(matches!(result, Err(ConfigError::OutOfRange { .. })));
    }

    #[test]
    fn test_int_out_of_range_max() {
        let mut mgr = ConfigManager::new();
        // MAX_MEMORY_PERCENT max is 90
        let result =
            mgr.set("noxu.maxMemoryPercent", ParamValue::Int(91), false);
        assert!(matches!(result, Err(ConfigError::OutOfRange { .. })));
    }

    #[test]
    fn test_long_out_of_range_min() {
        let mut mgr = ConfigManager::new();
        // MAX_DISK min is 0 (je.maxMemory has no minimum)
        let result = mgr.set("noxu.maxDisk", ParamValue::Long(-1), false);
        assert!(matches!(result, Err(ConfigError::OutOfRange { .. })));
    }

    #[test]
    fn test_mutable_param_can_be_changed_when_open() {
        let mut mgr = ConfigManager::new();
        // MAX_MEMORY_PERCENT is mutable
        let result =
            mgr.set("noxu.maxMemoryPercent", ParamValue::Int(75), true);
        assert!(result.is_ok());
        assert_eq!(mgr.get_int(&params::MAX_MEMORY_PERCENT), 75);
    }

    #[test]
    fn test_immutable_param_cannot_be_changed_when_open() {
        let mut mgr = ConfigManager::new();
        // LOG_FILE_MAX is not mutable
        let result = mgr.set(
            "noxu.log.fileMax",
            ParamValue::Long(5_000_000),
            true, // is_open
        );
        assert!(matches!(result, Err(ConfigError::NotMutable { .. })));
    }

    #[test]
    fn test_all_params_registered() {
        // All params in all_params() should be discoverable by name
        let mgr = ConfigManager::new();
        for param in params::all_params() {
            assert!(
                mgr.get_definition(param.name).is_some(),
                "param '{}' not found in manager",
                param.name
            );
        }
    }

    #[test]
    fn test_all_params_count() {
        // We should have at least 130 params now
        let count = params::all_params().len();
        assert!(count >= 130, "expected at least 130 params, got {}", count);
    }

    #[test]
    fn test_new_checkpointer_params_defaults() {
        let mgr = ConfigManager::new();
        use std::time::Duration;
        assert_eq!(
            mgr.get_long(&params::CHECKPOINTER_BYTES_INTERVAL),
            20_000_000
        );
        assert_eq!(
            mgr.get_duration(&params::CHECKPOINTER_WAKEUP_INTERVAL),
            Duration::ZERO
        );
        assert_eq!(mgr.get_int(&params::CHECKPOINTER_DEADLOCK_RETRY), 3);
        assert!(!mgr.get_bool(&params::CHECKPOINTER_HIGH_PRIORITY));
    }

    #[test]
    fn test_new_tree_params_defaults() {
        let mgr = ConfigManager::new();
        assert_eq!(mgr.get_long(&params::TREE_MIN_MEMORY), 500 * 1024);
        assert_eq!(mgr.get_int(&params::TREE_COMPACT_MAX_KEY_LENGTH), 16);
        assert_eq!(mgr.get_int(&params::TREE_BIN_DELTA), 25);
        assert_eq!(mgr.get_int(&params::NODE_DUP_TREE_MAX_ENTRIES), 128);
    }

    #[test]
    fn test_new_evictor_params_defaults() {
        let mgr = ConfigManager::new();
        assert_eq!(mgr.get_long(&params::EVICTOR_EVICT_BYTES), 524_288);
        assert_eq!(mgr.get_int(&params::EVICTOR_N_LRU_LISTS), 4);
        assert_eq!(mgr.get_int(&params::EVICTOR_CRITICAL_PERCENTAGE), 0);
    }

    #[test]
    fn test_default_fallback_when_not_overridden() {
        let mgr = ConfigManager::new();
        // get() for non-overridden param returns default
        let val = mgr.get("noxu.maxMemoryPercent");
        assert_eq!(val, Some(&ParamValue::Int(60)));
    }

    #[test]
    fn test_get_returns_none_for_unknown() {
        let mgr = ConfigManager::new();
        assert!(mgr.get("noxu.nonexistent.param").is_none());
    }
}
