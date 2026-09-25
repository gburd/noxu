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
//! # Completeness guard (config-defaults-review.md, follow-up B)
//!
//! The classification is EXHAUSTIVE and cannot silently skip a param.  The
//! `bool_param_classification_is_complete` test enumerates every bool
//! `ConfigParam` from `noxu_config::params::all_params()` and asserts each is
//! in EXACTLY one of [`PARAM_TO_FIELD`] (mapped to an `EnvironmentConfig`
//! field) or [`KNOWN_UNMAPPED`] (with a documented reason), and that
//! `mapped + known_unmapped == total bool param count`.  A newly-added bool
//! param therefore FAILS this test until someone classifies it — the earlier
//! version walked only the 48 mapped entries and silently skipped the other 21.
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

/// Bool `ConfigParam`s that have NO effective `EnvironmentConfig` bool field,
/// with the reason each is genuinely unmappable.  Together with
/// [`PARAM_TO_FIELD`] this must partition every bool param (enforced by
/// `bool_param_classification_is_complete`): a param is mapped OR known-
/// unmapped, never neither and never both.
///
/// `(param name, reason)`.  Keep the param NAME (the `noxu.*` key) here, since
/// that is what `ConfigParam::name` reports.
const KNOWN_UNMAPPED: &[(&str, &str)] = &[
    // Tree-internal knobs consumed inside noxu-tree; no EnvironmentConfig
    // bool field (EnvironmentConfig exposes the int TREE_BIN_DELTA percent,
    // not these blind-op toggles).
    ("noxu.tree.binDeltaBlindOps", "tree-internal; no EnvironmentConfig field"),
    (
        "noxu.tree.binDeltaBlindPuts",
        "tree-internal; no EnvironmentConfig field",
    ),
    // Cleaner-internal knobs consumed inside noxu-cleaner; no EnvironmentConfig
    // bool field.
    (
        "noxu.cleaner.trackDetail",
        "cleaner-internal; no EnvironmentConfig field",
    ),
    (
        "noxu.cleaner.gradualExpiration",
        "cleaner-internal; no EnvironmentConfig field",
    ),
    ("noxu.cleaner.rmwFix", "cleaner-internal; no EnvironmentConfig field"),
    // Utility / diagnostics flags with no runtime EnvironmentConfig field.
    (
        "noxu.env.comparatorsRequired",
        "utility-only (DbScavenger-style tools); no EnvironmentConfig field",
    ),
    (
        "noxu.env.exposeUserData",
        "diagnostics (include user data in messages); no EnvironmentConfig field",
    ),
    (
        "noxu.env.recovery",
        "recovery is always enabled in Noxu; no opt-out EnvironmentConfig field",
    ),
    (
        "noxu.env.setupLogger",
        "logging routes through the `log` crate; no EnvironmentConfig field",
    ),
    (
        "noxu.env.logTrace",
        "logging routes through the `log` crate / noxu-observe; no EnvironmentConfig field",
    ),
    (
        "noxu.lock.oldLockExceptions",
        "legacy exception-type compat; no EnvironmentConfig field",
    ),
    (
        "noxu.log.checksumFatal",
        "log-internal (checksum-error fatality); no EnvironmentConfig field",
    ),
    (
        "noxu.tree.secondaryIntegrityFatal",
        "tree-internal (secondary-integrity fatality); no EnvironmentConfig field",
    ),
    // Test-only flags.
    ("noxu.testMode", "test-only; no EnvironmentConfig field"),
    (
        "noxu.evictor.forcedYield",
        "test-only yield hook; no EnvironmentConfig field",
    ),
    // Deprecated / moot compatibility flags kept only so old config strings
    // still parse; the underlying feature is N/A to Noxu.
    (
        "noxu.env.sharedLatches",
        "deprecated in JE ('no longer used'); no EnvironmentConfig field",
    ),
    (
        "noxu.env.dupConvertPreloadAll",
        "deprecated-moot (JE 4->5 dup conversion N/A to .ndb); no EnvironmentConfig field",
    ),
    (
        "noxu.log.useNIO",
        "deprecated-moot (Java NIO N/A to Noxu); no EnvironmentConfig field",
    ),
    (
        "noxu.log.directNIO",
        "deprecated-moot (Java NIO direct buffers N/A to Noxu); no EnvironmentConfig field",
    ),
    (
        "noxu.deferredWrite.temp",
        "deprecated (per-DB deferred-write via DatabaseConfig); no EnvironmentConfig field",
    ),
    (
        "noxu.rep.runLogFlushTask",
        "replication-managed (flush task always controlled by the rep layer); no EnvironmentConfig field",
    ),
];

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

/// Completeness guard (follow-up B): every bool `ConfigParam` must be
/// classified as EXACTLY one of mapped ([`PARAM_TO_FIELD`]) or known-unmapped
/// ([`KNOWN_UNMAPPED`]).  This CANNOT silently skip a param: it enumerates the
/// full param set from `noxu_config::params::all_params()`, so a newly-added
/// bool param that is in neither table fails here until someone classifies it.
#[test]
fn bool_param_classification_is_complete() {
    // The full universe of bool params, straight from the params module.
    let all_bool: Vec<&'static str> = params::all_params()
        .into_iter()
        .filter(|p| p.default.as_bool().is_some())
        .map(|p| p.name)
        .collect();
    let total_bool = all_bool.len();

    // Mapped param NAMES (the `noxu.*` keys) from PARAM_TO_FIELD.
    let mapped_names: Vec<&'static str> =
        PARAM_TO_FIELD.iter().map(|(p, _)| p.name).collect();
    let unmapped_names: Vec<&'static str> =
        KNOWN_UNMAPPED.iter().map(|(n, _)| *n).collect();

    // 1. No param may be in BOTH tables (the partition must be disjoint).
    let both: Vec<&str> = mapped_names
        .iter()
        .filter(|n| unmapped_names.contains(n))
        .copied()
        .collect();
    assert!(
        both.is_empty(),
        "param(s) appear in BOTH PARAM_TO_FIELD and KNOWN_UNMAPPED (must be \
         one or the other): {both:?}"
    );

    // 2. Neither table may reference a name that is not a real bool param
    //    (catches typos / stale entries after a param is renamed/removed).
    for n in mapped_names.iter().chain(unmapped_names.iter()) {
        assert!(
            all_bool.contains(n),
            "'{n}' is listed in PARAM_TO_FIELD/KNOWN_UNMAPPED but is not a \
             bool ConfigParam in params::all_params() (stale or misspelled)"
        );
    }

    // 3. Every bool param must be classified (in exactly one table). Report
    //    the unclassified ones by name so a newcomer knows what to add.
    let unclassified: Vec<&str> = all_bool
        .iter()
        .filter(|n| !mapped_names.contains(n) && !unmapped_names.contains(n))
        .copied()
        .collect();
    assert!(
        unclassified.is_empty(),
        "\n{} bool ConfigParam(s) are unclassified — add each to PARAM_TO_FIELD \
         (if it drives an EnvironmentConfig field) or to KNOWN_UNMAPPED (with \
         a reason):\n  {}\n",
        unclassified.len(),
        unclassified.join("\n  ")
    );

    // 4. The count identity: mapped + known_unmapped == total. This is the
    //    load-bearing assertion — with (1)-(3) already holding, an equal count
    //    proves the two tables exactly partition the bool-param universe, so
    //    the classification cannot silently skip anything.
    assert_eq!(
        mapped_names.len() + unmapped_names.len(),
        total_bool,
        "PARAM_TO_FIELD ({}) + KNOWN_UNMAPPED ({}) != total bool params ({}) — \
         the classification is not exhaustive",
        mapped_names.len(),
        unmapped_names.len(),
        total_bool,
    );
}
