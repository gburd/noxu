//! JE TCK port: `je.config` parameter validation, mutation, and defaults.
//!
//! Ports invariants from JE
//! `com.sleepycat.je.config.EnvironmentParamsTest` onto noxu's
//! `ConfigManager` / `ConfigParam` / `ParamValue`.
//!
//! Mapping JE -> noxu:
//!
//! | JE                                | Noxu                              |
//! |-----------------------------------|-----------------------------------|
//! | `IntConfigParam`                  | `ConfigParam::int_param`          |
//! | `LongConfigParam`                 | `ConfigParam::long_param`         |
//! | `EnvironmentConfig.setConfigParam`| `ConfigManager::set`              |
//! | `IllegalArgumentException`        | `ConfigError::OutOfRange` /       |
//! |                                   | `ConfigError::TypeMismatch` /     |
//! |                                   | `ConfigError::UnknownParam`       |
//! | `EnvironmentFailureException`     | (returned as `ConfigError`)       |
//!
//! JE's `IntConfigParam(name, min, max, default, mutable, forReplication)`
//! constructor maps onto noxu's `int_param(name, Some(min), Some(max),
//! default, mutable, for_replication)`.

use noxu_config::manager::ConfigManager;
use noxu_config::param::{ConfigError, ConfigParam, ParamType, ParamValue};
use noxu_config::params;

// ---------------------------------------------------------------------------
// EnvironmentParamsTest.testValidation -- bounds enforcement
// ---------------------------------------------------------------------------
//
// JE constructs `IntConfigParam("param.int", 2, 10, 5, false, false)` and
// then asserts that values "1" and "11" fail validation while "5" succeeds.
// We exercise the same bounds-check logic via a known noxu int parameter
// (MAX_MEMORY_PERCENT, range [1, 90]).

#[test]
fn tck_config_int_value_within_bounds_accepted() {
    let mut mgr = ConfigManager::new();
    mgr.set(
        "noxu.maxMemoryPercent",
        ParamValue::Int(50),
        /* is_open */ false,
    )
    .unwrap();
    assert_eq!(50, mgr.get_int(&params::MAX_MEMORY_PERCENT));
}

#[test]
fn tck_config_int_value_below_min_rejected() {
    let mut mgr = ConfigManager::new();
    let res = mgr.set(
        "noxu.maxMemoryPercent",
        ParamValue::Int(0), // below min=1
        false,
    );
    assert!(
        matches!(res, Err(ConfigError::OutOfRange { .. })),
        "expected OutOfRange below min, got {res:?}",
    );
}

#[test]
fn tck_config_int_value_above_max_rejected() {
    let mut mgr = ConfigManager::new();
    let res = mgr.set(
        "noxu.maxMemoryPercent",
        ParamValue::Int(91), // above max=90
        false,
    );
    assert!(
        matches!(res, Err(ConfigError::OutOfRange { .. })),
        "expected OutOfRange above max, got {res:?}",
    );
}

// ---------------------------------------------------------------------------
// EnvironmentParamsTest.testInvalidVsMultiValue -- unknown param rejected
// ---------------------------------------------------------------------------
//
// JE rejects `setConfigParam("je.maxMemory.stuff", "true")` because the
// suffix is not a registered param.  Noxu's analogue: setting an unknown
// parameter returns `ConfigError::UnknownParam`, not `OutOfRange` or
// silent acceptance.

#[test]
fn tck_config_unknown_param_rejected() {
    let mut mgr = ConfigManager::new();
    let res = mgr.set(
        "noxu.maxMemory.stuff", // not a registered param
        ParamValue::Bool(true),
        false,
    );
    assert!(
        matches!(res, Err(ConfigError::UnknownParam { .. })),
        "expected UnknownParam, got {res:?}",
    );
}

// ---------------------------------------------------------------------------
// JE TCK: type mismatch rejected
// ---------------------------------------------------------------------------
//
// `IntConfigParam.validateValue("not-an-int")` throws.  noxu's analogue
// is setting a `ParamValue::Bool` against an int-typed parameter.

#[test]
fn tck_config_type_mismatch_rejected() {
    let mut mgr = ConfigManager::new();
    // SHARED_CACHE is bool; pass an Int.
    let res =
        mgr.set("noxu.sharedCache", ParamValue::Int(42), false);
    assert!(
        matches!(res, Err(ConfigError::TypeMismatch { .. })),
        "expected TypeMismatch, got {res:?}",
    );
}

// ---------------------------------------------------------------------------
// JE TCK: mutability after open
// ---------------------------------------------------------------------------
//
// JE's `EnvironmentMutableConfig` allows changing parameters marked
// `mutable=true` *after* the env has been opened, and rejects changes
// to immutable params.  noxu's `ConfigManager::set(name, val, is_open)`
// honours the same rule with the `is_open` flag.

#[test]
fn tck_config_immutable_param_rejected_after_open() {
    // Pick a known immutable param: ENV_IS_TRANSACTIONAL is set at
    // construction time and not changeable after open.
    let mgr_def = &params::ENV_IS_TRANSACTIONAL;
    assert!(
        !mgr_def.mutable,
        "this test is built around an immutable param; \
         {} is now marked mutable, pick a different one",
        mgr_def.name,
    );

    let mut mgr = ConfigManager::new();

    // Before open: any value (even an immutable param) is settable.
    mgr.set(
        mgr_def.name,
        ParamValue::Bool(true),
        /* is_open */ false,
    )
    .unwrap();

    // After open: setting an immutable param fails.
    let res = mgr.set(
        mgr_def.name,
        ParamValue::Bool(false),
        /* is_open */ true,
    );
    assert!(
        matches!(res, Err(ConfigError::NotMutable { .. })),
        "expected NotMutable when setting {} post-open, got {res:?}",
        mgr_def.name,
    );
}

#[test]
fn tck_config_mutable_param_accepted_after_open() {
    // MAX_MEMORY_PERCENT is mutable.
    let p = &params::MAX_MEMORY_PERCENT;
    assert!(p.mutable);

    let mut mgr = ConfigManager::new();
    mgr.set(p.name, ParamValue::Int(50), /* is_open */ true).unwrap();
    assert_eq!(50, mgr.get_int(p));
}

// ---------------------------------------------------------------------------
// Defaults present until overridden -- standard JE config invariant
// ---------------------------------------------------------------------------

#[test]
fn tck_config_defaults_returned_when_not_overridden() {
    let mgr = ConfigManager::new();
    // MAX_MEMORY_PERCENT default is 60 (per crates/noxu-config/src/params.rs).
    assert_eq!(60, mgr.get_int(&params::MAX_MEMORY_PERCENT));
    assert!(!mgr.is_overridden("noxu.maxMemoryPercent"));
}

#[test]
fn tck_config_overridden_value_takes_precedence_over_default() {
    let mut mgr = ConfigManager::new();
    mgr.set("noxu.maxMemoryPercent", ParamValue::Int(75), false)
        .unwrap();
    assert_eq!(75, mgr.get_int(&params::MAX_MEMORY_PERCENT));
    assert!(mgr.is_overridden("noxu.maxMemoryPercent"));
}

// ---------------------------------------------------------------------------
// Custom ConfigParam -- mirror of EnvironmentParamsTest's `intParam` /
// `longParam` set-up.  Confirms noxu's validation of custom parameters
// agrees with JE's bounds-check semantics for ad-hoc params not in
// `params.rs`.
// ---------------------------------------------------------------------------

#[test]
fn tck_config_custom_int_param_bounds_enforced_directly() {
    let p = ConfigParam::int_param(
        "noxu.test.tck.int",
        Some(2),
        Some(10),
        5,
        /* mutable */ false,
        /* for_replication */ false,
    );

    // In-range values pass.
    assert!(p.validate(&ParamValue::Int(2)).is_ok());
    assert!(p.validate(&ParamValue::Int(5)).is_ok());
    assert!(p.validate(&ParamValue::Int(10)).is_ok());

    // Out-of-range values fail with OutOfRange.
    assert!(matches!(
        p.validate(&ParamValue::Int(1)),
        Err(ConfigError::OutOfRange { .. }),
    ));
    assert!(matches!(
        p.validate(&ParamValue::Int(11)),
        Err(ConfigError::OutOfRange { .. }),
    ));

    // Wrong type fails with TypeMismatch.
    assert!(matches!(
        p.validate(&ParamValue::Bool(true)),
        Err(ConfigError::TypeMismatch { expected: ParamType::Int, .. }),
    ));
}
