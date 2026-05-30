//! Configuration parameter types.
//!
//! Configuration parameter types including boolean, integer, long, duration,
//! and string variants with optional min/max bounds and mutability flags.

use std::fmt;
use std::time::Duration;

/// A typed configuration parameter value.
#[derive(Debug, Clone, PartialEq)]
pub enum ParamValue {
    Bool(bool),
    Int(i32),
    Long(i64),
    Duration(Duration),
    /// Owned string value (used at runtime).
    String(String),
    /// Static string literal — zero-cost, enables `const` construction.
    Str(&'static str),
}

impl ParamValue {
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            ParamValue::Bool(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_i32(&self) -> Option<i32> {
        match self {
            ParamValue::Int(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            ParamValue::Long(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_duration(&self) -> Option<Duration> {
        match self {
            ParamValue::Duration(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            ParamValue::String(v) => Some(v),
            ParamValue::Str(v) => Some(v),
            _ => None,
        }
    }
}

impl fmt::Display for ParamValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParamValue::Bool(v) => write!(f, "{}", v),
            ParamValue::Int(v) => write!(f, "{}", v),
            ParamValue::Long(v) => write!(f, "{}", v),
            ParamValue::Duration(v) => write!(f, "{} ms", v.as_millis()),
            ParamValue::String(v) => write!(f, "{}", v),
            ParamValue::Str(v) => write!(f, "{}", v),
        }
    }
}

/// The type of a configuration parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamType {
    Bool,
    Int,
    Long,
    Duration,
    String,
}

/// Definition of a single configuration parameter.
///
/// Definition of a single configuration parameter.
///
/// Each parameter has a name (following the "noxu.xxx.yyy" naming convention),
/// a type, a default value, optional min/max bounds, and flags indicating
/// whether it can be changed at runtime (mutable) and whether it's a
/// replication parameter.
#[derive(Debug, Clone)]
pub struct ConfigParam {
    /// Parameter name (e.g., "noxu.maxMemory").
    pub name: &'static str,
    /// Parameter type.
    pub param_type: ParamType,
    /// Default value.
    pub default: ParamValue,
    /// Minimum value (for numeric types).
    pub min: Option<ParamValue>,
    /// Maximum value (for numeric types).
    pub max: Option<ParamValue>,
    /// Whether this parameter can be changed after Environment open.
    pub mutable: bool,
    /// Whether this is a replication parameter.
    pub for_replication: bool,
}

impl ConfigParam {
    /// Creates a boolean parameter definition.
    pub const fn bool_param(
        name: &'static str,
        default: bool,
        mutable: bool,
        for_replication: bool,
    ) -> Self {
        ConfigParam {
            name,
            param_type: ParamType::Bool,
            default: ParamValue::Bool(default),
            min: None,
            max: None,
            mutable,
            for_replication,
        }
    }

    /// Creates an integer parameter definition.
    pub const fn int_param(
        name: &'static str,
        min: Option<i32>,
        max: Option<i32>,
        default: i32,
        mutable: bool,
        for_replication: bool,
    ) -> Self {
        ConfigParam {
            name,
            param_type: ParamType::Int,
            default: ParamValue::Int(default),
            min: match min {
                Some(v) => Some(ParamValue::Int(v)),
                None => None,
            },
            max: match max {
                Some(v) => Some(ParamValue::Int(v)),
                None => None,
            },
            mutable,
            for_replication,
        }
    }

    /// Creates a string parameter definition.
    ///
    /// Uses `ParamValue::Str` so the default is a `&'static str` — zero-cost,
    /// no heap allocation, and allows `const` static construction.
    pub const fn string_param(
        name: &'static str,
        default: &'static str,
        mutable: bool,
        for_replication: bool,
    ) -> Self {
        ConfigParam {
            name,
            param_type: ParamType::String,
            default: ParamValue::Str(default),
            min: None,
            max: None,
            mutable,
            for_replication,
        }
    }

    /// Creates a long parameter definition.
    pub const fn long_param(
        name: &'static str,
        min: Option<i64>,
        max: Option<i64>,
        default: i64,
        mutable: bool,
        for_replication: bool,
    ) -> Self {
        ConfigParam {
            name,
            param_type: ParamType::Long,
            default: ParamValue::Long(default),
            min: match min {
                Some(v) => Some(ParamValue::Long(v)),
                None => None,
            },
            max: match max {
                Some(v) => Some(ParamValue::Long(v)),
                None => None,
            },
            mutable,
            for_replication,
        }
    }

    /// Validates a value against this parameter's constraints.
    pub fn validate(&self, value: &ParamValue) -> Result<(), ConfigError> {
        // Type check
        match (&self.param_type, value) {
            (ParamType::Bool, ParamValue::Bool(_)) => {}
            (ParamType::Int, ParamValue::Int(v)) => {
                if let Some(ParamValue::Int(min)) = &self.min
                    && v < min
                {
                    return Err(ConfigError::OutOfRange {
                        name: self.name,
                        value: value.to_string(),
                        min: min.to_string(),
                        max: self
                            .max
                            .as_ref()
                            .map(|m| m.to_string())
                            .unwrap_or_default(),
                    });
                }
                if let Some(ParamValue::Int(max)) = &self.max
                    && v > max
                {
                    return Err(ConfigError::OutOfRange {
                        name: self.name,
                        value: value.to_string(),
                        min: self
                            .min
                            .as_ref()
                            .map(|m| m.to_string())
                            .unwrap_or_default(),
                        max: max.to_string(),
                    });
                }
            }
            (ParamType::Long, ParamValue::Long(v)) => {
                if let Some(ParamValue::Long(min)) = &self.min
                    && v < min
                {
                    return Err(ConfigError::OutOfRange {
                        name: self.name,
                        value: value.to_string(),
                        min: min.to_string(),
                        max: self
                            .max
                            .as_ref()
                            .map(|m| m.to_string())
                            .unwrap_or_default(),
                    });
                }
                if let Some(ParamValue::Long(max)) = &self.max
                    && v > max
                {
                    return Err(ConfigError::OutOfRange {
                        name: self.name,
                        value: value.to_string(),
                        min: self
                            .min
                            .as_ref()
                            .map(|m| m.to_string())
                            .unwrap_or_default(),
                        max: max.to_string(),
                    });
                }
            }
            (ParamType::Duration, ParamValue::Duration(_)) => {}
            (ParamType::String, ParamValue::String(_) | ParamValue::Str(_)) => {
            }
            _ => {
                return Err(ConfigError::TypeMismatch {
                    name: self.name,
                    expected: self.param_type,
                    got: value.clone(),
                });
            }
        }
        Ok(())
    }
}

impl fmt::Display for ConfigParam {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

/// Errors in configuration parameter handling.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("Unknown parameter: {name}")]
    UnknownParam { name: String },

    #[error(
        "Parameter {name}: type mismatch, expected {expected:?}, got {got}"
    )]
    TypeMismatch { name: &'static str, expected: ParamType, got: ParamValue },

    #[error("Parameter {name}: value {value} out of range [{min}, {max}]")]
    OutOfRange { name: &'static str, value: String, min: String, max: String },

    #[error("Parameter {name} is not mutable after Environment open")]
    NotMutable { name: &'static str },
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    // -----------------------------------------------------------------------
    // ParamValue tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_param_value_bool_accessors() {
        let v = ParamValue::Bool(true);
        assert_eq!(v.as_bool(), Some(true));
        assert_eq!(v.as_i32(), None);
        assert_eq!(v.as_i64(), None);
        assert_eq!(v.as_duration(), None);
        assert_eq!(v.as_str(), None);
    }

    #[test]
    fn test_param_value_int_accessors() {
        let v = ParamValue::Int(42);
        assert_eq!(v.as_i32(), Some(42));
        assert_eq!(v.as_bool(), None);
        assert_eq!(v.as_i64(), None);
    }

    #[test]
    fn test_param_value_long_accessors() {
        let v = ParamValue::Long(i64::MAX);
        assert_eq!(v.as_i64(), Some(i64::MAX));
        assert_eq!(v.as_i32(), None);
        assert_eq!(v.as_bool(), None);
    }

    #[test]
    fn test_param_value_duration_accessors() {
        let d = Duration::from_secs(5);
        let v = ParamValue::Duration(d);
        assert_eq!(v.as_duration(), Some(d));
        assert_eq!(v.as_bool(), None);
    }

    #[test]
    fn test_param_value_string_accessors() {
        let v = ParamValue::String("hello".to_string());
        assert_eq!(v.as_str(), Some("hello"));
        assert_eq!(v.as_bool(), None);
    }

    #[test]
    fn test_param_value_display() {
        assert_eq!(ParamValue::Bool(true).to_string(), "true");
        assert_eq!(ParamValue::Int(42).to_string(), "42");
        assert_eq!(ParamValue::Long(100).to_string(), "100");
        assert_eq!(ParamValue::String("x".to_string()).to_string(), "x");
        let d = Duration::from_millis(500);
        assert!(ParamValue::Duration(d).to_string().contains("500"));
    }

    // -----------------------------------------------------------------------
    // ConfigParam construction
    // -----------------------------------------------------------------------

    #[test]
    fn test_bool_param_construction() {
        let p = ConfigParam::bool_param("noxu.test.bool", true, true, false);
        assert_eq!(p.name, "noxu.test.bool");
        assert_eq!(p.param_type, ParamType::Bool);
        assert_eq!(p.default, ParamValue::Bool(true));
        assert!(p.mutable);
        assert!(!p.for_replication);
        assert!(p.min.is_none());
        assert!(p.max.is_none());
    }

    #[test]
    fn test_int_param_construction() {
        let p = ConfigParam::int_param(
            "noxu.test.int",
            Some(0),
            Some(100),
            50,
            false,
            false,
        );
        assert_eq!(p.default, ParamValue::Int(50));
        assert_eq!(p.min, Some(ParamValue::Int(0)));
        assert_eq!(p.max, Some(ParamValue::Int(100)));
        assert!(!p.mutable);
    }

    #[test]
    fn test_long_param_construction() {
        let p = ConfigParam::long_param(
            "noxu.test.long",
            Some(0),
            None,
            1024,
            true,
            false,
        );
        assert_eq!(p.default, ParamValue::Long(1024));
        assert_eq!(p.min, Some(ParamValue::Long(0)));
        assert!(p.max.is_none());
        assert!(p.mutable);
    }

    #[test]
    fn test_int_param_no_bounds() {
        let p = ConfigParam::int_param(
            "noxu.test.unbounded",
            None,
            None,
            5,
            false,
            false,
        );
        assert!(p.min.is_none());
        assert!(p.max.is_none());
    }

    // -----------------------------------------------------------------------
    // Validation tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_validate_bool_ok() {
        let p = ConfigParam::bool_param("noxu.test", false, false, false);
        assert!(p.validate(&ParamValue::Bool(true)).is_ok());
        assert!(p.validate(&ParamValue::Bool(false)).is_ok());
    }

    #[test]
    fn test_validate_bool_type_mismatch() {
        let p = ConfigParam::bool_param("noxu.test", false, false, false);
        let err = p.validate(&ParamValue::Int(1));
        assert!(matches!(err, Err(ConfigError::TypeMismatch { .. })));
    }

    #[test]
    fn test_validate_int_in_range() {
        let p = ConfigParam::int_param(
            "noxu.test",
            Some(0),
            Some(100),
            50,
            false,
            false,
        );
        assert!(p.validate(&ParamValue::Int(0)).is_ok());
        assert!(p.validate(&ParamValue::Int(50)).is_ok());
        assert!(p.validate(&ParamValue::Int(100)).is_ok());
    }

    #[test]
    fn test_validate_int_below_min() {
        let p = ConfigParam::int_param(
            "noxu.test",
            Some(1),
            Some(100),
            50,
            false,
            false,
        );
        let err = p.validate(&ParamValue::Int(0));
        assert!(matches!(err, Err(ConfigError::OutOfRange { .. })));
    }

    #[test]
    fn test_validate_int_above_max() {
        let p = ConfigParam::int_param(
            "noxu.test",
            Some(0),
            Some(90),
            50,
            false,
            false,
        );
        let err = p.validate(&ParamValue::Int(91));
        assert!(matches!(err, Err(ConfigError::OutOfRange { .. })));
    }

    #[test]
    fn test_validate_long_in_range() {
        let p = ConfigParam::long_param(
            "noxu.test",
            Some(0),
            Some(1000),
            500,
            false,
            false,
        );
        assert!(p.validate(&ParamValue::Long(0)).is_ok());
        assert!(p.validate(&ParamValue::Long(1000)).is_ok());
    }

    #[test]
    fn test_validate_long_below_min() {
        let p = ConfigParam::long_param(
            "noxu.test",
            Some(0),
            None,
            100,
            false,
            false,
        );
        let err = p.validate(&ParamValue::Long(-1));
        assert!(matches!(err, Err(ConfigError::OutOfRange { .. })));
    }

    #[test]
    fn test_validate_long_above_max() {
        let p = ConfigParam::long_param(
            "noxu.test",
            None,
            Some(100),
            50,
            false,
            false,
        );
        let err = p.validate(&ParamValue::Long(101));
        assert!(matches!(err, Err(ConfigError::OutOfRange { .. })));
    }

    #[test]
    fn test_validate_duration_ok() {
        let p = ConfigParam {
            name: "noxu.test.dur",
            param_type: ParamType::Duration,
            default: ParamValue::Duration(Duration::from_secs(1)),
            min: None,
            max: None,
            mutable: false,
            for_replication: false,
        };
        assert!(
            p.validate(&ParamValue::Duration(Duration::from_secs(5))).is_ok()
        );
    }

    #[test]
    fn test_validate_duration_type_mismatch() {
        let p = ConfigParam {
            name: "noxu.test.dur",
            param_type: ParamType::Duration,
            default: ParamValue::Duration(Duration::from_secs(1)),
            min: None,
            max: None,
            mutable: false,
            for_replication: false,
        };
        let err = p.validate(&ParamValue::Int(1000));
        assert!(matches!(err, Err(ConfigError::TypeMismatch { .. })));
    }

    #[test]
    fn test_config_param_display() {
        let p =
            ConfigParam::bool_param("noxu.test.display", false, false, false);
        assert_eq!(format!("{}", p), "noxu.test.display");
    }

    #[test]
    fn test_config_error_display() {
        let e = ConfigError::UnknownParam { name: "noxu.foo".to_string() };
        assert!(format!("{}", e).contains("noxu.foo"));

        let e2 = ConfigError::NotMutable { name: "noxu.log.fileMax" };
        assert!(format!("{}", e2).contains("noxu.log.fileMax"));

        let e3 = ConfigError::OutOfRange {
            name: "noxu.maxMemoryPercent",
            value: "95".to_string(),
            min: "1".to_string(),
            max: "90".to_string(),
        };
        assert!(format!("{}", e3).contains("95"));
        assert!(format!("{}", e3).contains("90"));
    }
}
