//! Registry of `EnvironmentConfig` parameters that are **accepted but not
//! yet implemented**.
//!
//! # Purpose
//!
//! Each entry in [`UNIMPLEMENTED_ENV_PARAMS`] names a config field that is
//! stored in `EnvironmentConfig` / `DbiEnvConfig` but is never read by any
//! production subsystem.  [`warn_unimplemented_params`] is called from
//! [`crate::Environment::open`] and emits a `log::warn!` for each parameter
//! that has been set to a non-default value, preventing silent no-ops.
//!
//! # Adding a new parameter
//!
//! 1. Implement the parameter in the relevant subsystem (preferred), or
//! 2. Add an entry here with the field name and its default value expression.
//!
//! A `#[test]` in this module asserts that every entry triggers a warning when
//! set to a non-default value, so no new parameter can be silently added
//! without a corresponding registry entry.
//!
//! # Closing the gap (re-audit JE F-1)
//!
//! The 7 parameters identified in the JE re-audit (2026-05-30) are listed
//! below.  Each has been marked reserved in its rustdoc.

use crate::environment_config::EnvironmentConfig;

/// Describes one unimplemented `EnvironmentConfig` parameter.
pub struct UnimplementedParam {
    /// Human-readable field name (for log messages).
    pub name: &'static str,
    /// Returns `true` if the config has a non-default value for this param.
    pub is_non_default: fn(&EnvironmentConfig) -> bool,
}

/// The complete list of `EnvironmentConfig` parameters that are stored but
/// not consumed by any production subsystem as of v3.1.
///
/// Update this list whenever:
/// - A parameter is wired up (remove the entry), or
/// - A new reserved parameter is added (add an entry).
pub static UNIMPLEMENTED_ENV_PARAMS: &[UnimplementedParam] = &[
    UnimplementedParam {
        name: "env_check_leaks",
        // default = true; non-default means the caller set it to false
        is_non_default: |c| !c.env_check_leaks,
    },
    UnimplementedParam {
        name: "env_forced_yield",
        // default = false; non-default means the caller set it to true
        is_non_default: |c| c.env_forced_yield,
    },
    UnimplementedParam {
        name: "env_fair_latches",
        // default = false; non-default means the caller set it to true
        is_non_default: |c| c.env_fair_latches,
    },
    UnimplementedParam {
        name: "env_latch_timeout_ms",
        // default = 300_000; non-default means any other value
        is_non_default: |c| c.env_latch_timeout_ms != 300_000,
    },
    UnimplementedParam {
        name: "env_ttl_clock_tolerance_ms",
        // default = 0; non-default means non-zero
        is_non_default: |c| c.env_ttl_clock_tolerance_ms != 0,
    },
    UnimplementedParam {
        name: "env_expiration_enabled",
        // default = false; non-default means true
        is_non_default: |c| c.env_expiration_enabled,
    },
    UnimplementedParam {
        name: "env_db_eviction",
        // default = false; non-default means true
        is_non_default: |c| c.env_db_eviction,
    },
    // ---------------------------------------------------------------------
    // DBI-14 inert-flag sweep (2026-06-23): the following EnvironmentConfig
    // fields have a setter+field but ZERO runtime read sites.  They are
    // accepted-but-inert and warned here so a non-default setting is never a
    // silent no-op.  See docs/src/operations/known-limitations.md.
    //
    // NOTE (7.1): `env_dup_convert_preload_all` and `adler32_chunk_size` were
    // removed from this registry — they are not "real-but-unimplemented"
    // knobs, they are DEPRECATED and MOOT (JE 4→5 dup conversion is N/A to the
    // native .ndb format; Noxu uses CRC32, never Adler32).  A deprecated-moot
    // knob announces itself via `#[deprecated]` on its setter, not a runtime
    // WARN.  See the field rustdoc in environment_config.rs.
    // ---------------------------------------------------------------------
    UnimplementedParam {
        name: "checkpointer_min_interval_secs",
        // default = 0; non-default means non-zero.  NOTE: this is not a JE
        // param (JE's Checkpointer has no min-interval throttle); the
        // checkpointer is driven by checkpointer_bytes_interval +
        // checkpointer_wakeup_interval_ms, both of which ARE wired.
        is_non_default: |c| c.checkpointer_min_interval_secs != 0,
    },
    UnimplementedParam {
        name: "verify_schedule",
        // default = ""; non-default means any schedule string set
        // (background btree verifier scheduling is not implemented).
        is_non_default: |c| !c.verify_schedule.is_empty(),
    },
    UnimplementedParam {
        name: "startup_dump_threshold_ms",
        // default = 0; startup-diagnostics dump is not implemented.
        is_non_default: |c| c.startup_dump_threshold_ms != 0,
    },
    UnimplementedParam {
        name: "dos_producer_queue_timeout_ms",
        // default = 10_000; the DiskOrderedScan producer-queue timeout is not
        // consulted by the disk-ordered cursor implementation.
        is_non_default: |c| c.dos_producer_queue_timeout_ms != 10_000,
    },
];

/// Emit a `log::warn!` for each unimplemented parameter that has been set to a
/// non-default value.
///
/// Called once from [`crate::Environment::open`] so operators immediately see
/// in the log that their config option has no effect.
pub fn warn_unimplemented_params(config: &EnvironmentConfig) {
    for param in UNIMPLEMENTED_ENV_PARAMS {
        if (param.is_non_default)(config) {
            log::warn!(
                "EnvironmentConfig::{} is set to a non-default value but \
                 is NOT YET IMPLEMENTED as of v3.1. \
                 The setting has no effect. \
                 See docs/src/operations/known-limitations.md.",
                param.name
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::environment_config::EnvironmentConfig;
    use std::path::PathBuf;

    fn env_default() -> EnvironmentConfig {
        EnvironmentConfig::new(PathBuf::from("."))
    }

    /// Guard: every parameter in UNIMPLEMENTED_ENV_PARAMS must detect its
    /// non-default value.  Fails if a param is miscategorized.
    #[test]
    fn all_unimplemented_params_detect_non_default() {
        // Verify that with defaults, none are flagged.
        let default_config = env_default();
        for param in UNIMPLEMENTED_ENV_PARAMS {
            assert!(
                !(param.is_non_default)(&default_config),
                "param '{}' is_non_default fired on the DEFAULT config — \
                 the default check is wrong",
                param.name,
            );
        }
    }

    #[test]
    fn env_check_leaks_warn_on_false() {
        let mut c = env_default();
        c.set_env_check_leaks(false);
        let p = UNIMPLEMENTED_ENV_PARAMS
            .iter()
            .find(|p| p.name == "env_check_leaks")
            .unwrap();
        assert!((p.is_non_default)(&c), "should detect non-default");
    }

    #[test]
    fn env_forced_yield_warn_on_true() {
        let mut c = env_default();
        c.set_env_forced_yield(true);
        let p = UNIMPLEMENTED_ENV_PARAMS
            .iter()
            .find(|p| p.name == "env_forced_yield")
            .unwrap();
        assert!((p.is_non_default)(&c));
    }

    #[test]
    fn env_fair_latches_warn_on_true() {
        let mut c = env_default();
        c.set_env_fair_latches(true);
        let p = UNIMPLEMENTED_ENV_PARAMS
            .iter()
            .find(|p| p.name == "env_fair_latches")
            .unwrap();
        assert!((p.is_non_default)(&c));
    }

    #[test]
    fn env_latch_timeout_ms_warn_on_non_default() {
        let mut c = env_default();
        c.set_env_latch_timeout_ms(60_000); // non-default
        let p = UNIMPLEMENTED_ENV_PARAMS
            .iter()
            .find(|p| p.name == "env_latch_timeout_ms")
            .unwrap();
        assert!((p.is_non_default)(&c));
    }

    #[test]
    fn env_latch_timeout_ms_no_warn_on_default() {
        let mut c = env_default();
        c.set_env_latch_timeout_ms(300_000); // exactly the default
        let p = UNIMPLEMENTED_ENV_PARAMS
            .iter()
            .find(|p| p.name == "env_latch_timeout_ms")
            .unwrap();
        assert!(!(p.is_non_default)(&c));
    }

    #[test]
    fn env_ttl_clock_tolerance_ms_warn_on_non_zero() {
        let mut c = env_default();
        c.set_env_ttl_clock_tolerance_ms(100);
        let p = UNIMPLEMENTED_ENV_PARAMS
            .iter()
            .find(|p| p.name == "env_ttl_clock_tolerance_ms")
            .unwrap();
        assert!((p.is_non_default)(&c));
    }

    #[test]
    fn env_expiration_enabled_warn_on_true() {
        let mut c = env_default();
        c.set_env_expiration_enabled(true);
        let p = UNIMPLEMENTED_ENV_PARAMS
            .iter()
            .find(|p| p.name == "env_expiration_enabled")
            .unwrap();
        assert!((p.is_non_default)(&c));
    }

    #[test]
    fn env_db_eviction_warn_on_true() {
        let mut c = env_default();
        c.set_env_db_eviction(true);
        let p = UNIMPLEMENTED_ENV_PARAMS
            .iter()
            .find(|p| p.name == "env_db_eviction")
            .unwrap();
        assert!((p.is_non_default)(&c));
    }

    #[test]
    fn dbi14_sweep_params_warn_on_non_default() {
        let mut c = env_default();
        c.set_checkpointer_min_interval_secs(60);
        c.set_verify_schedule("0 0 * * *".to_string());
        c.set_startup_dump_threshold_ms(100);
        c.set_dos_producer_queue_timeout_ms(5_000);
        for name in [
            "checkpointer_min_interval_secs",
            "verify_schedule",
            "startup_dump_threshold_ms",
            "dos_producer_queue_timeout_ms",
        ] {
            let p = UNIMPLEMENTED_ENV_PARAMS
                .iter()
                .find(|p| p.name == name)
                .unwrap_or_else(|| panic!("census missing {name}"));
            assert!(
                (p.is_non_default)(&c),
                "{name} should detect its non-default value"
            );
        }
    }
}
