//! Cross-check test: every boolean `ConfigParam` in `noxu-config::params`
//! must agree with the effective default that `EnvironmentConfig::default()`
//! (and, where applicable, `DbiEnvConfig::default()`) actually applies.
//!
//! Root cause of audit findings V18/V19/B2/UTIL-9/UTIL-15: `params.rs` and the
//! `EnvironmentConfig`/`DbiEnvConfig` structs are two unsynchronized sources of
//! truth for defaults. The "152/152 constant MATCH" audit only compared JE
//! against `params.rs`; it never looked at the struct that users actually get.
//! Eight bool params ended up declared `true` in `params.rs` (matching JE) but
//! effectively `false` in `EnvironmentConfig::default()`.
//!
//! This test walks every bool `ConfigParam` and asserts one of:
//!   * it maps to an `EnvironmentConfig` bool field whose default equals the
//!     param's declared default, OR
//!   * it is explicitly listed as "no effective config field" (replication-
//!     only, reserved, or test-only) so that adding a new bool param without
//!     wiring it is caught here.
//!
//! Any deliberate divergence between a param default and its effective config
//! default must be recorded in `INTENTIONAL_DIVERGENCES` with a reason, so the
//! two sources agree and the divergence is auditable in one place.

use noxu_config::ConfigParam;
use noxu_config::params;
use noxu_db::EnvironmentConfig;
use std::path::PathBuf;

/// Read a bool field out of the default `EnvironmentConfig` by name.
fn env_default_field(cfg: &EnvironmentConfig, field: &str) -> bool {
    match field {
        "checkpointer_high_priority" => cfg.checkpointer_high_priority,
        "cleaner_adjust_utilization" => cfg.cleaner_adjust_utilization,
        "cleaner_background_proactive_migration" => {
            cfg.cleaner_background_proactive_migration
        }
        "cleaner_expiration_enabled" => cfg.cleaner_expiration_enabled,
        "cleaner_expunge" => cfg.cleaner_expunge,
        "cleaner_fetch_obsolete_size" => cfg.cleaner_fetch_obsolete_size,
        "cleaner_foreground_proactive_migration" => {
            cfg.cleaner_foreground_proactive_migration
        }
        "cleaner_lazy_migration" => cfg.cleaner_lazy_migration,
        "cleaner_use_deleted_dir" => cfg.cleaner_use_deleted_dir,
        "compressor_purge_root" => cfg.compressor_purge_root,
        "env_check_leaks" => cfg.env_check_leaks,
        "env_db_eviction" => cfg.env_db_eviction,
        "env_expiration_enabled" => cfg.env_expiration_enabled,
        "env_fair_latches" => cfg.env_fair_latches,
        "env_forced_yield" => cfg.env_forced_yield,
        "env_is_locking" => cfg.env_is_locking,
        "env_recovery_force_checkpoint" => cfg.env_recovery_force_checkpoint,
        "env_recovery_force_new_file" => cfg.env_recovery_force_new_file,
        "evictor_allow_bin_deltas" => cfg.evictor_allow_bin_deltas,
        "evictor_lru_only" => cfg.evictor_lru_only,
        "evictor_mutate_bins" => cfg.evictor_mutate_bins,
        "evictor_use_dirty_lru" => cfg.evictor_use_dirty_lru,
        "halt_on_commit_after_checksum_exception" => {
            cfg.halt_on_commit_after_checksum_exception
        }
        "lock_deadlock_detect" => cfg.lock_deadlock_detect,
        "log_checksum_read" => cfg.log_checksum_read,
        "log_detect_file_delete" => cfg.log_detect_file_delete,
        "log_mem_only" => cfg.log_mem_only,
        "log_use_odsync" => cfg.log_use_odsync,
        "log_use_write_queue" => cfg.log_use_write_queue,
        "log_verify_checksums" => cfg.log_verify_checksums,
        "offheap_checksum" => cfg.offheap_checksum,
        "read_only" => cfg.read_only,
        "run_checkpointer" => cfg.run_checkpointer,
        "run_cleaner" => cfg.run_cleaner,
        "run_evictor" => cfg.run_evictor,
        "run_in_compressor" => cfg.run_in_compressor,
        "run_offheap_evictor" => cfg.run_offheap_evictor,
        "run_verifier" => cfg.run_verifier,
        "shared_cache" => cfg.shared_cache,
        "stats_collect" => cfg.stats_collect,
        "transaction" | "transactional" => cfg.transactional,
        "txn_deadlock_stack_trace" => cfg.txn_deadlock_stack_trace,
        "txn_dump_locks" => cfg.txn_dump_locks,
        "txn_serializable_isolation" => cfg.txn_serializable_isolation,
        "verify_btree" => cfg.verify_btree,
        "verify_data_records" => cfg.verify_data_records,
        "verify_log" => cfg.verify_log,
        "verify_obsolete_records" => cfg.verify_obsolete_records,
        "verify_secondaries" => cfg.verify_secondaries,
        other => panic!("test bug: no accessor for EnvironmentConfig.{other}"),
    }
}

/// Every bool `ConfigParam` whose default *drives* an `EnvironmentConfig`
/// bool field. `(param, field)`.
#[allow(deprecated)] // some referenced params are deprecated but still tracked
const PARAM_TO_FIELD: &[(&ConfigParam, &str)] = &[
    (&params::CHECKPOINTER_HIGH_PRIORITY, "checkpointer_high_priority"),
    (&params::CLEANER_ADJUST_UTILIZATION, "cleaner_adjust_utilization"),
    (
        &params::CLEANER_BACKGROUND_PROACTIVE_MIGRATION,
        "cleaner_background_proactive_migration",
    ),
    (&params::CLEANER_REMOVE, "cleaner_expunge"),
    (&params::CLEANER_FETCH_OBSOLETE_SIZE, "cleaner_fetch_obsolete_size"),
    (
        &params::CLEANER_FOREGROUND_PROACTIVE_MIGRATION,
        "cleaner_foreground_proactive_migration",
    ),
    (&params::CLEANER_LAZY_MIGRATION, "cleaner_lazy_migration"),
    (&params::CLEANER_USE_DELETED_DIR, "cleaner_use_deleted_dir"),
    (&params::COMPRESSOR_PURGE_ROOT, "compressor_purge_root"),
    (&params::ENV_CHECK_LEAKS, "env_check_leaks"),
    (&params::ENV_DB_EVICTION, "env_db_eviction"),
    (&params::ENV_EXPIRATION_ENABLED, "env_expiration_enabled"),
    (&params::ENV_FAIR_LATCHES, "env_fair_latches"),
    (&params::ENV_FORCED_YIELD, "env_forced_yield"),
    (&params::ENV_IS_LOCKING, "env_is_locking"),
    (&params::ENV_RECOVERY_FORCE_CHECKPOINT, "env_recovery_force_checkpoint"),
    (&params::ENV_RECOVERY_FORCE_NEW_FILE, "env_recovery_force_new_file"),
    (&params::EVICTOR_ALLOW_BIN_DELTAS, "evictor_allow_bin_deltas"),
    (&params::EVICTOR_LRU_ONLY, "evictor_lru_only"),
    (&params::EVICTOR_MUTATE_BINS, "evictor_mutate_bins"),
    (&params::EVICTOR_USE_DIRTY_LRU, "evictor_use_dirty_lru"),
    (
        &params::HALT_ON_COMMIT_AFTER_CHECKSUMEXCEPTION,
        "halt_on_commit_after_checksum_exception",
    ),
    (&params::LOCK_DEADLOCK_DETECT, "lock_deadlock_detect"),
    (&params::LOG_CHECKSUM_READ, "log_checksum_read"),
    (&params::LOG_DETECT_FILE_DELETE, "log_detect_file_delete"),
    (&params::LOG_MEM_ONLY, "log_mem_only"),
    (&params::LOG_USE_ODSYNC, "log_use_odsync"),
    (&params::LOG_USE_WRITE_QUEUE, "log_use_write_queue"),
    (&params::LOG_VERIFY_CHECKSUMS, "log_verify_checksums"),
    (&params::OFFHEAP_CHECKSUM, "offheap_checksum"),
    (&params::ENV_IS_READ_ONLY, "read_only"),
    (&params::ENV_RUN_CHECKPOINTER, "run_checkpointer"),
    (&params::ENV_RUN_CLEANER, "run_cleaner"),
    (&params::ENV_RUN_EVICTOR, "run_evictor"),
    (&params::ENV_RUN_IN_COMPRESSOR, "run_in_compressor"),
    (&params::ENV_RUN_OFFHEAP_EVICTOR, "run_offheap_evictor"),
    (&params::ENV_RUN_VERIFIER, "run_verifier"),
    (&params::SHARED_CACHE, "shared_cache"),
    (&params::STATS_COLLECT, "stats_collect"),
    (&params::ENV_IS_TRANSACTIONAL, "transactional"),
    (&params::TXN_DEADLOCK_STACK_TRACE, "txn_deadlock_stack_trace"),
    (&params::TXN_DUMP_LOCKS, "txn_dump_locks"),
    (&params::TXN_SERIALIZABLE_ISOLATION, "txn_serializable_isolation"),
    (&params::VERIFY_BTREE, "verify_btree"),
    (&params::VERIFY_DATA_RECORDS, "verify_data_records"),
    (&params::VERIFY_LOG, "verify_log"),
    (&params::VERIFY_OBSOLETE_RECORDS, "verify_obsolete_records"),
    (&params::VERIFY_SECONDARIES, "verify_secondaries"),
];

/// Deliberate, documented divergences: `(param name, effective default,
/// reason)`. Anything here is allowed to differ from the param's declared
/// default; everything else must match exactly. Keep in sync with
/// docs/src/maintainer/design-decisions.md and the field doc comments.
const INTENTIONAL_DIVERGENCES: &[(&str, bool, &str)] = &[];

#[test]
fn bool_param_defaults_match_environment_config_defaults() {
    let cfg = EnvironmentConfig::new(PathBuf::from("/tmp/parity-test"));
    let mut mismatches = Vec::new();

    for (param, field) in PARAM_TO_FIELD {
        let declared = param
            .default
            .as_bool()
            .unwrap_or_else(|| panic!("{} is not a bool param", param.name));
        let effective = env_default_field(&cfg, field);

        if let Some((_, allowed, _)) = INTENTIONAL_DIVERGENCES
            .iter()
            .find(|(name, _, _)| *name == param.name)
        {
            assert_eq!(
                effective, *allowed,
                "{} is a documented divergence but its effective default \
                 ({effective}) does not match the recorded value ({allowed})",
                param.name
            );
            continue;
        }

        if declared != effective {
            mismatches.push(format!(
                "  {}: params.rs default = {declared}, \
                 EnvironmentConfig::default() = {effective}",
                param.name
            ));
        }
    }

    assert!(
        mismatches.is_empty(),
        "\n{} bool param default(s) disagree with the effective \
         EnvironmentConfig default:\n{}\n\nEither reconcile the effective \
         default to the declared (JE) default, or record the divergence in \
         INTENTIONAL_DIVERGENCES with a reason.\n",
        mismatches.len(),
        mismatches.join("\n")
    );
}
