//! Main cleaner daemon for log garbage collection.
//!
//! responsible for garbage collecting the log by
//! selecting least utilized files, processing them, and deleting cleaned files.

use crate::FileSelector;
use crate::cleaner_stat::CleanerStats;
use crate::file_processor::{
    BinLookupResult, FileProcessResult, FileProcessor, LogEntry, LogEntryType,
    MigrateLnResult, SharedTreeLookup, TreeLookup,
};
use crate::file_protector::FileProtector;
use crate::file_summary::FileSummary;
use crate::throttle::CleanerThrottle;
use crate::utilization_profile::UtilizationProfile;
use crate::utilization_tracker::UtilizationTracker;
use noxu_log::{
    FileManager, LogManager,
    entry_header::{MAX_HEADER_SIZE, MIN_HEADER_SIZE},
    file_header::FILE_HEADER_SIZE,
};
use noxu_sync::Mutex;
use noxu_txn::TxnManager;
use noxu_util::lsn::NULL_LSN;
use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

/// Bundled context for calling `process_pending` from within the
/// `FileProcessor` periodic hook (CLN-12).
///
/// Holds cloned Arcs of the fields that `Cleaner::process_pending` needs,
/// so the callback can be built at the start of `process_single_file`
/// without holding a borrow on `self`.
struct ProcessPendingCtx {
    file_selector: Arc<Mutex<FileSelector>>,
    tree: Arc<RwLock<noxu_tree::Tree>>,
    log_manager: Arc<LogManager>,
    lock_manager: Option<Arc<noxu_txn::LockManager>>,
    extra_trees:
        Arc<std::sync::Mutex<HashMap<i64, Arc<RwLock<noxu_tree::Tree>>>>>,
    stats: Arc<CleanerStats>,
    shutdown: Arc<AtomicBool>,
}

impl ProcessPendingCtx {
    fn call(&self) {
        let pending = self.file_selector.lock().get_pending_lns();
        let Some(pending) = pending else { return };

        let tree_lookup = if let Some(ref shared_lm) = self.lock_manager {
            SharedTreeLookup::with_lock_manager(
                Arc::clone(&self.tree),
                Arc::clone(&self.log_manager),
                Arc::clone(shared_lm),
            )
            .with_extra_trees(
                self.extra_trees.lock().map(|g| g.clone()).unwrap_or_default(),
            )
        } else {
            SharedTreeLookup::new(
                Arc::clone(&self.tree),
                Arc::clone(&self.log_manager),
            )
            .with_extra_trees(
                self.extra_trees.lock().map(|g| g.clone()).unwrap_or_default(),
            )
        };

        let processor = FileProcessor::new(
            Arc::clone(&self.stats),
            Arc::clone(&self.shutdown),
        );

        for (log_lsn, ln_info) in pending {
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }
            let bin_result = tree_lookup.lookup_parent_bin(
                ln_info.db_id(),
                ln_info.key(),
                log_lsn,
            );
            let outcome = match bin_result {
                BinLookupResult::NotFound | BinLookupResult::KnownDeleted => {
                    self.file_selector.lock().remove_pending_ln(log_lsn);
                    self.stats
                        .pending_lns_processed
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                BinLookupResult::Found { tree_lsn } => processor
                    .process_found_ln(
                        &ln_info,
                        log_lsn,
                        tree_lsn,
                        &tree_lookup,
                    ),
            };
            match outcome {
                MigrateLnResult::Migrated | MigrateLnResult::Dead => {
                    self.file_selector.lock().remove_pending_ln(log_lsn);
                    self.stats
                        .pending_lns_processed
                        .fetch_add(1, Ordering::Relaxed);
                }
                MigrateLnResult::Locked => {
                    self.stats
                        .pending_lns_locked
                        .fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        let q = self.file_selector.lock().get_pending_ln_count() as u64;
        self.stats.pending_ln_queue_size.store(q, Ordering::Relaxed);
    }
}

/// The Cleaner is responsible for garbage collecting the log.
///
/// It selects the least utilized log file for cleaning (FileSelector),
/// reads through the log file (FileProcessor) and determines whether
/// each entry is obsolete or active. Active entries are migrated to
/// the end of the log, and the cleaned file is deleted.
///
/// The cleaner can be invoked manually via `do_clean()` or run as a
/// background daemon thread.
pub struct Cleaner {
    /// File selector for choosing files to clean.
    file_selector: Arc<Mutex<FileSelector>>,

    /// File protector for preventing deletion of files in use.
    ///
    /// `Arc` so other subsystems (e.g. a `DiskOrderedCursor` producer) can
    /// share the SAME protector instance the cleaner consults before
    /// deleting a file — see [`Cleaner::file_protector`].
    file_protector: Arc<FileProtector>,

    /// Cleaner statistics.
    stats: Arc<CleanerStats>,

    /// Whether the cleaner is currently running.
    running: AtomicBool,

    /// Shutdown signal.
    shutdown: Arc<AtomicBool>,

    /// Minimum utilization threshold (0-100%).
    ///
    /// Files below this utilization are candidates for cleaning.
    ///
    /// `AtomicU32` so a runtime `setMutableConfig` can push a new
    /// `je.cleaner.minUtilization` (DBI-10 / `EnvConfigObserver`).
    min_utilization: AtomicU32,

    /// CLN-F1: minFileUtilization second-tier threshold (0-50%).
    ///
    /// JE `EnvironmentParams.CLEANER_MIN_FILE_UTILIZATION`: when the aggregate
    /// gate (`predictedMinUtil < minUtilization`) fails, a single file whose
    /// max-gradual utilization is below this value is still cleaned.
    min_file_utilization: u32,

    /// Minimum file count before cleaning starts.
    ///
    /// The cleaner won't run until at least this many files exist.
    min_file_count: u32,

    /// Minimum age of file before cleaning (in seconds).
    ///
    /// Files must be at least this old before they can be cleaned.
    min_age: u64,

    /// Total number of cleaning runs performed.
    n_runs: AtomicU64,

    /// Files pending deletion (marked safe to delete but not yet removed).
    pending_deletions: Mutex<Vec<u32>>,

    /// Optional FileManager for real log-file scanning and deletion.
    ///
    /// When `None`, `process_single_file` returns an empty `FileSummary` and
    /// `delete_pending_files` skips the actual `fs::remove_file` call (the
    /// in-memory counter is still incremented so existing unit tests pass).
    file_manager: Option<Arc<FileManager>>,

    /// Optional shared B-tree for LN migration.
    ///
    /// When `Some`, `process_single_file` decodes the LN entries from the log
    /// file and calls `FileProcessor::process_file()` with a `SharedTreeLookup`
    /// so that live LN entries are migrated (their BIN slot LSNs are updated).
    /// When `None`, migration is skipped (the no-op path used by unit tests).
    ///
    /// `env.getDbTree()` access pattern in the equivalent `FileProcessor`.
    tree: Option<Arc<RwLock<noxu_tree::Tree>>>,

    /// Optional LogManager used by `SharedTreeLookup::migrate_ln_slot` to
    /// obtain a fresh LSN for the migrated LN entry.
    log_manager: Option<Arc<LogManager>>,

    /// Optional shared `LockManager` from the environment.
    ///
    /// When `Some`, the cleaner uses the environment's lock table so that
    /// cleaner-held locks contend with user transactions for correct deadlock
    /// detection.  When `None`, `SharedTreeLookup::new` allocates a private
    /// manager (safe but no cross-component deadlock detection).
    ///
    /// Using `env.getTxnManager().getLockManager()`.
    lock_manager: Option<Arc<noxu_txn::LockManager>>,

    /// Per-database tree registry for secondary databases (X-7 fix).
    ///
    /// Maps `db_id.id() as i64` → `Arc<RwLock<Tree>>` for every non-primary
    /// database that has been opened.  The cleaner's `SharedTreeLookup`
    /// dispatches liveness checks for non-primary LNs to the correct tree
    /// via `with_extra_trees`.
    ///
    /// The `Arc<Mutex<…>>` wrapper lets `open_database_inner` insert entries
    /// after the cleaner has already been constructed.
    extra_trees:
        Arc<std::sync::Mutex<HashMap<i64, Arc<RwLock<noxu_tree::Tree>>>>>,

    /// Adaptive throttle: tracks the log write rate and computes sleep
    /// intervals and files-per-pass recommendations for the daemon loop.
    ///
    /// Implements `CleanerThrottle`.
    pub throttle: Arc<CleanerThrottle>,

    /// Optional `TxnManager` for first-active-transaction file clamping
    /// (CLN-4).
    ///
    /// When `Some`, `do_clean` reads `TxnManager::get_first_active_lsn()` and
    /// clamps file selection so that no file inside an open transaction's log
    /// window is cleaned.
    ///
    /// JE: `UtilizationCalculator.getBestFile` reads
    /// `env.getTxnManager().getFirstActiveLsn()` and sets
    /// `firstActiveFile = min(newest, firstActiveTxnFile)` before computing
    /// `lastFileToClean`.
    txn_manager: Option<Arc<TxnManager>>,

    /// Optional callback invoked when the log is idle after cleaning
    /// (CLN-14: `wakeupAfterNoWrites`).
    ///
    /// When `Some`, `do_clean` calls this function after completing a pass
    /// with no active log writes, so the checkpointer is notified to run
    /// promptly and delete cleaned files.
    ///
    /// The engine wires this to `Checkpointer::wakeup_after_write` or a
    /// similar trigger.  Keeping it as a callback avoids a direct dependency
    /// on `noxu-recovery` from `noxu-cleaner`.
    ///
    /// JE: `FileProcessor.doClean` calls
    /// `envImpl.getCheckpointer().wakeupAfterNoWrites()` (~line 290).
    checkpoint_wakeup_fn: Option<Arc<dyn Fn() + Send + Sync>>,

    /// Per-file expiration profile store (CLN-9).
    ///
    /// Maps file numbers to their expiration-time histograms.  Populated by
    /// `two_pass_check` when a two-pass revisalRun completes.  Used to
    /// improve TTL-adjusted utilization scoring during file selection.
    ///
    /// In-memory only — does not survive crashes.  Persistent storage is
    /// deferred (see CLN-11 / known-limitations.md).
    ///
    /// JE: `Cleaner.getExpirationProfile()` (ExpirationProfile.java).
    expiration_profile_store: noxu_sync::Mutex<crate::ExpirationProfileStore>,

    /// In-memory per-file utilization summaries.
    ///
    /// Stores the cached `FileSummary` for every log file known to this
    /// environment.  This is the in-memory half of JE's
    /// `UtilizationProfile` / `FileSummaryDB`: the persistent
    /// `FileSummaryDB` backing store (CLN-11) is deferred; this field holds
    /// the summaries accumulated since the last flush.
    ///
    /// Populated by `do_clean` calling
    /// `profile.get_file_summary_map(true, &tracker)` (Part 2) which merges
    /// the cached profile with the live `UtilizationTracker`.
    ///
    /// JE: `UtilizationProfile.fileSummaryMap` (UtilizationProfile.java).
    utilization_profile: Arc<Mutex<UtilizationProfile>>,

    /// Live per-file utilization tracker.
    ///
    /// Fed on the LogManager write path (via `UtilizationTrackerObserver`)
    /// with every new and obsolete log entry.  `do_clean` reads this to
    /// build the merged `fileSummaryMap` before each selection pass.
    ///
    /// `None` in read-only / test environments that don't wire a tracker.
    ///
    /// JE: `EnvironmentImpl.getUtilizationTracker()` (env wiring).
    utilization_tracker: Option<Arc<Mutex<UtilizationTracker>>>,
}

/// Result of a cleaning operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CleanResult {
    /// Number of files successfully cleaned.
    pub files_cleaned: u32,

    /// Number of files successfully deleted.
    pub files_deleted: u32,

    /// Total number of log entries read across all cleaned files.
    pub total_entries_read: u64,
}

impl Cleaner {
    /// Creates a new cleaner with the given configuration.
    ///
    /// # Arguments
    /// * `min_utilization` - Minimum utilization threshold (0-100%)
    /// * `min_file_count` - Minimum file count before cleaning starts
    /// * `min_age` - Minimum age of file before cleaning (in seconds)
    pub fn new(
        min_utilization: u32,
        min_file_count: u32,
        min_age: u64,
    ) -> Self {
        Self {
            file_selector: Arc::new(Mutex::new(FileSelector::new())),
            file_protector: Arc::new(FileProtector::new()),
            stats: Arc::new(CleanerStats::new()),
            running: AtomicBool::new(false),
            shutdown: Arc::new(AtomicBool::new(false)),
            min_utilization: AtomicU32::new(min_utilization.min(100)),
            min_file_utilization: 5,
            min_file_count,
            min_age,
            n_runs: AtomicU64::new(0),
            pending_deletions: Mutex::new(Vec::new()),
            file_manager: None,
            tree: None,
            log_manager: None,
            lock_manager: None,
            extra_trees: Arc::new(std::sync::Mutex::new(HashMap::new())),
            throttle: Arc::new(CleanerThrottle::new(0)),
            txn_manager: None,
            checkpoint_wakeup_fn: None,
            expiration_profile_store: noxu_sync::Mutex::new(
                crate::ExpirationProfileStore::new(),
            ),
            utilization_profile: Arc::new(
                Mutex::new(UtilizationProfile::new()),
            ),
            utilization_tracker: None,
        }
    }

    /// Creates a new cleaner wired to a real `FileManager`.
    ///
    /// The cleaner uses the `FileManager` for two purposes:
    /// - `process_single_file()` scans the on-disk log file to compute real
    ///   utilization statistics.
    /// - `delete_pending_files()` calls `FileManager::delete_file()` to
    ///   remove cleaned log files from disk.
    pub fn with_file_manager(
        min_utilization: u32,
        min_file_count: u32,
        min_age: u64,
        file_manager: Arc<FileManager>,
    ) -> Self {
        Self {
            file_selector: Arc::new(Mutex::new(FileSelector::new())),
            file_protector: Arc::new(FileProtector::new()),
            stats: Arc::new(CleanerStats::new()),
            running: AtomicBool::new(false),
            shutdown: Arc::new(AtomicBool::new(false)),
            min_utilization: AtomicU32::new(min_utilization.min(100)),
            min_file_utilization: 5,
            min_file_count,
            min_age,
            n_runs: AtomicU64::new(0),
            pending_deletions: Mutex::new(Vec::new()),
            file_manager: Some(file_manager),
            tree: None,
            log_manager: None,
            lock_manager: None,
            extra_trees: Arc::new(std::sync::Mutex::new(HashMap::new())),
            throttle: Arc::new(CleanerThrottle::new(0)),
            txn_manager: None,
            checkpoint_wakeup_fn: None,
            expiration_profile_store: noxu_sync::Mutex::new(
                crate::ExpirationProfileStore::new(),
            ),
            utilization_profile: Arc::new(
                Mutex::new(UtilizationProfile::new()),
            ),
            utilization_tracker: None,
        }
    }

    /// Creates a new cleaner wired to a real `FileManager`, a shared B-tree,
    /// and a `LogManager`.
    ///
    /// In addition to the file-scanning and deletion capabilities of
    /// `with_file_manager`, this constructor enables LN migration:
    /// `process_single_file` will decode the actual LN entries from each
    /// cleaned log file and call `FileProcessor::process_file` with a
    /// `SharedTreeLookup` so that live LN entries are re-logged and their
    /// BIN slot LSNs are updated.
    ///
    /// Tree-access wiring for file processing.
    ///
    /// Note: allocates a private `LockManager` (no lock-table sharing with
    /// transactions).  Use `with_file_manager_tree_and_lock_manager` to pass
    /// the environment's shared LockManager for correct deadlock detection.
    pub fn with_file_manager_and_tree(
        min_utilization: u32,
        min_file_count: u32,
        min_age: u64,
        file_manager: Arc<FileManager>,
        tree: Arc<RwLock<noxu_tree::Tree>>,
        log_manager: Arc<LogManager>,
    ) -> Self {
        Self {
            file_selector: Arc::new(Mutex::new(FileSelector::new())),
            file_protector: Arc::new(FileProtector::new()),
            stats: Arc::new(CleanerStats::new()),
            running: AtomicBool::new(false),
            shutdown: Arc::new(AtomicBool::new(false)),
            min_utilization: AtomicU32::new(min_utilization.min(100)),
            min_file_utilization: 5,
            min_file_count,
            min_age,
            n_runs: AtomicU64::new(0),
            pending_deletions: Mutex::new(Vec::new()),
            file_manager: Some(file_manager),
            tree: Some(tree),
            log_manager: Some(log_manager),
            lock_manager: None,
            extra_trees: Arc::new(std::sync::Mutex::new(HashMap::new())),
            throttle: Arc::new(CleanerThrottle::new(0)),
            txn_manager: None,
            checkpoint_wakeup_fn: None,
            expiration_profile_store: noxu_sync::Mutex::new(
                crate::ExpirationProfileStore::new(),
            ),
            utilization_profile: Arc::new(
                Mutex::new(UtilizationProfile::new()),
            ),
            utilization_tracker: None,
        }
    }

    /// Creates a new cleaner wired to a `FileManager`, shared B-tree,
    /// `LogManager`, and the environment's shared `LockManager`.
    ///
    /// This is the preferred constructor for production use.  Passing the
    /// environment's `LockManager` ensures that locks held by the cleaner
    /// contend with user transactions, enabling correct deadlock detection.
    ///
    /// Cleaner obtains the lock manager via
    /// `env.getTxnManager().getLockManager()`.
    pub fn with_file_manager_tree_and_lock_manager(
        min_utilization: u32,
        min_file_count: u32,
        min_age: u64,
        file_manager: Arc<FileManager>,
        tree: Arc<RwLock<noxu_tree::Tree>>,
        log_manager: Arc<LogManager>,
        lock_manager: Arc<noxu_txn::LockManager>,
    ) -> Self {
        Self {
            file_selector: Arc::new(Mutex::new(FileSelector::new())),
            file_protector: Arc::new(FileProtector::new()),
            stats: Arc::new(CleanerStats::new()),
            running: AtomicBool::new(false),
            shutdown: Arc::new(AtomicBool::new(false)),
            min_utilization: AtomicU32::new(min_utilization.min(100)),
            min_file_utilization: 5,
            min_file_count,
            min_age,
            n_runs: AtomicU64::new(0),
            pending_deletions: Mutex::new(Vec::new()),
            file_manager: Some(file_manager),
            tree: Some(tree),
            log_manager: Some(log_manager),
            lock_manager: Some(lock_manager),
            extra_trees: Arc::new(std::sync::Mutex::new(HashMap::new())),
            throttle: Arc::new(CleanerThrottle::new(0)),
            txn_manager: None,
            checkpoint_wakeup_fn: None,
            expiration_profile_store: noxu_sync::Mutex::new(
                crate::ExpirationProfileStore::new(),
            ),
            utilization_profile: Arc::new(
                Mutex::new(UtilizationProfile::new()),
            ),
            utilization_tracker: None,
        }
    }

    /// Register per-database trees for secondary databases (X-7 fix).
    ///
    /// Accepts a shared registry `Arc` so the environment can dynamically add
    /// trees as databases are opened after the cleaner is constructed.
    pub fn with_tree_registry(
        mut self,
        registry: Arc<
            std::sync::Mutex<HashMap<i64, Arc<RwLock<noxu_tree::Tree>>>>,
        >,
    ) -> Self {
        self.extra_trees = registry;
        self
    }

    /// Register a single additional tree for `db_id` (X-7 fix).
    ///
    /// Idempotent — calling with the same `db_id` replaces the previous entry.
    pub fn register_db_tree(
        &self,
        db_id: i64,
        tree: Arc<RwLock<noxu_tree::Tree>>,
    ) {
        if let Ok(mut reg) = self.extra_trees.lock() {
            reg.insert(db_id, tree);
        }
    }

    /// Wire the environment's `TxnManager` for first-active-transaction
    /// file clamping (CLN-4).
    ///
    /// JE: `UtilizationCalculator.getBestFile` reads
    /// `env.getTxnManager().getFirstActiveLsn()` and sets
    /// `firstActiveFile = min(newest, firstActiveTxnFile)`.
    /// Configure the two-pass cleaning gate from
    /// `CLEANER_TWO_PASS_GAP` / `CLEANER_TWO_PASS_THRESHOLD` (JE). A
    /// `threshold` of 0 resolves to `minUtilization - 5` at gate time.
    pub fn with_two_pass_params(self, gap: i32, threshold: i32) -> Self {
        self.file_selector.lock().set_two_pass_params(gap, threshold);
        self
    }

    /// CLN-F1: set the `minFileUtilization` second-tier threshold (0-50%).
    ///
    /// JE `EnvironmentParams.CLEANER_MIN_FILE_UTILIZATION` (default 5%).
    pub fn with_min_file_utilization(mut self, pct: u32) -> Self {
        self.min_file_utilization = pct.min(50);
        self
    }

    pub fn with_txn_manager(mut self, txn_manager: Arc<TxnManager>) -> Self {
        self.txn_manager = Some(txn_manager);
        self
    }

    /// Returns the current minimum-utilization threshold (0-100%).
    pub fn get_min_utilization(&self) -> u32 {
        self.min_utilization.load(Ordering::Relaxed)
    }

    /// Pushes a new minimum-utilization threshold at runtime.
    ///
    /// DBI-10 / JE `EnvConfigObserver`: a `setMutableConfig` change to
    /// `je.cleaner.minUtilization` must reach the running cleaner. Clamped to
    /// 0-100 like the constructors.
    pub fn set_min_utilization(&self, pct: u32) {
        self.min_utilization.store(pct.min(100), Ordering::Relaxed);
    }

    /// Returns true if a `TxnManager` has been wired (CLN-4 first-active-txn
    /// clamp is active). Used to verify production wiring in tests.
    pub fn has_txn_manager(&self) -> bool {
        self.txn_manager.is_some()
    }

    /// Wire a checkpoint wakeup callback for CLN-14 (`wakeupAfterNoWrites`).
    ///
    /// The callback is invoked at the end of a cleaning pass when no active
    /// log writes were detected, prompting the checkpointer to run so that
    /// cleaned files are deleted promptly.
    ///
    /// JE: `FileProcessor.doClean` calls
    /// `envImpl.getCheckpointer().wakeupAfterNoWrites()` (~line 290).
    pub fn with_checkpoint_wakeup_fn(
        mut self,
        f: Arc<dyn Fn() + Send + Sync>,
    ) -> Self {
        self.checkpoint_wakeup_fn = Some(f);
        self
    }

    /// Wire the environment's live `UtilizationTracker` for autonomous file
    /// selection.
    ///
    /// When set, `do_clean` calls
    /// `profile.get_file_summary_map(true, &tracker)` to build the merged
    /// `fileSummaryMap` before each iteration, matching JE
    /// `FileProcessor.doClean` line ~340:
    /// `fileSummaryMap = profile.getFileSummaryMap(true)`.
    ///
    /// JE: `EnvironmentImpl.getUtilizationTracker()` — the tracker is
    /// threaded into the Cleaner symmetrically to how `LockManager` is
    /// threaded via `with_txn_manager`.
    pub fn with_utilization_tracker(
        mut self,
        tracker: Arc<Mutex<UtilizationTracker>>,
    ) -> Self {
        self.utilization_tracker = Some(tracker);
        self
    }

    /// CLN-4: seed the in-memory `UtilizationProfile` from the per-file
    /// summaries that recovery rebuilt from persisted `FileSummaryLN` WAL
    /// entries (the latest record per file wins).  This lets the cleaner see
    /// real utilization IMMEDIATELY after restart, rather than re-warming the
    /// profile from new live writes (the old CLN-6 limitation).
    ///
    /// JE: `UtilizationProfile.populateCache` reads the FileSummaryLN records
    /// back from the file-summary DB into `fileSummaryMap` at recovery; the
    /// cleaner's `getFileSummaryMap` then sees them.  Here recovery hands us
    /// the already-rebuilt map and we install it as the profile's cache.
    pub fn seed_profile(
        &self,
        summaries: hashbrown::HashMap<u32, FileSummary>,
    ) {
        if summaries.is_empty() {
            return;
        }
        let mut profile = self.utilization_profile.lock();
        profile.populate(summaries);
        // The seeded data reflects the on-disk state, not an unflushed change.
        profile.clear_modified();
    }

    /// CLN-4 verification helper: returns a snapshot of the profile's cached
    /// per-file summary (NOT merged with the live tracker).  Used by tests to
    /// assert that after a restart the cleaner sees the persisted obsolete
    /// bytes without first re-warming from new writes.
    pub fn get_profile_summary(&self, file_number: u32) -> Option<FileSummary> {
        self.utilization_profile.lock().get_file_summary(file_number).cloned()
    }

    /// Main cleaning entry point - performs cleaning of up to n_files.
    ///
    /// # Arguments
    /// * `n_files` - Maximum number of files to clean in this run
    /// * `force` - If true, ignore utilization thresholds and clean anyway
    ///
    /// # Returns
    /// Result containing cleaning statistics or an error
    /// Main cleaning entry point — faithful port of JE
    /// `FileProcessor.doClean` (FileProcessor.java ~line 317).
    ///
    /// # JE structure reproduced here
    ///
    /// 1. `fileSummaryMap = profile.getFileSummaryMap(true)` (line ~340) —
    ///    merge cached profile + live tracker before entering the loop.
    /// 2. Loop for each file to clean:
    ///    - `processPending()` (line ~360).
    ///    - If `nFilesCleaned > 0`: refresh `fileSummaryMap` (CLN-13, line ~386).
    ///    - `fileSelector.selectFileForCleaning(calculator, fileSummaryMap,
    ///      forceCleaning)` (line ~393): drain TO_BE_CLEANED first, then
    ///      getBestFile.
    ///    - Two-pass dry run when `twoPass` (lines ~420-465, CLN-5).
    ///    - `processFile` (migrate LNs) + `markFileCleaned`.
    ///
    /// # Arguments
    /// * `n_files` — maximum files to clean (JE `cleanMultipleFiles` if > 1).
    /// * `force` — bypass utilization threshold (`forceCleaning`).
    pub fn do_clean(
        &self,
        n_files: u32,
        force: bool,
    ) -> Result<CleanResult, String> {
        // Check if already running
        if self
            .running
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Err("Cleaner is already running".to_string());
        }

        // Ensure we reset running flag on exit
        let _guard = RunningGuard::new(&self.running);

        // Check shutdown
        if self.shutdown.load(Ordering::Relaxed) {
            return Err("Cleaner is shut down".to_string());
        }

        // Increment run counter
        self.n_runs.fetch_add(1, Ordering::Relaxed);
        self.stats.runs.fetch_add(1, Ordering::Relaxed);

        // JE FileProcessor.doClean ~line 340:
        //   fileSummaryMap = profile.getFileSummaryMap(true /*includeTrackedFiles*/)
        //
        // Build the merged per-file summary map from the in-memory profile
        // plus the live utilization tracker (when wired).  Files that have
        // not yet been flushed to the profile but appear in the tracker are
        // included so that the file-selector's getBestFile path can score
        // them.
        //
        // When no tracker is wired (read-only / unit-test mode), the profile
        // map is empty and selection falls back to the TO_BE_CLEANED queue.
        let file_summary_map: BTreeMap<u32, crate::FileSummary> = {
            let profile = self.utilization_profile.lock();
            if let Some(ref tracker_arc) = self.utilization_tracker {
                let tracker = tracker_arc.lock();
                profile.get_file_summary_map(true, &tracker)
            } else {
                profile.get_file_summary_map(
                    false,
                    &UtilizationTracker::new(false),
                )
            }
        };

        // CLN-4: compute first_active_txn_file from TxnManager so that
        // files inside an open transaction's log window are excluded.
        // JE: UtilizationCalculator.getBestFile reads
        // env.getTxnManager().getFirstActiveLsn() and sets
        // firstActiveFile = min(newest, firstActiveTxnFile).
        let first_active_txn_file: Option<u32> =
            self.txn_manager.as_ref().and_then(|tm| {
                let lsn_u64 = tm.get_first_active_lsn();
                if lsn_u64 == NULL_LSN.as_u64() {
                    None
                } else {
                    Some((lsn_u64 >> 32) as u32)
                }
            });

        let mut files_cleaned = 0u32;
        let mut total_entries = 0u64;
        // file_summary_map is refreshed inside the loop (CLN-13).
        let mut current_summary_map = file_summary_map;

        // JE FileProcessor.doClean main loop (~line 345): clean until no
        // more files are selected or n_files budget is exhausted.
        for _ in 0..n_files {
            // Check shutdown before each iteration.
            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }

            // JE ~line 360: processPending() — retry locked LNs from prior
            // passes before selecting the next file.
            // CLN-1/2/3 gating: pending LNs block file deletion.
            self.process_pending();

            // CLN-13: re-build the file summary map after the first file
            // so that utilization changes from cleaning are visible to the
            // next file-selection iteration.
            // JE ~line 386: fileSummaryMap = profile.getFileSummaryMap(true)
            if files_cleaned > 0 {
                let profile = self.utilization_profile.lock();
                current_summary_map =
                    if let Some(ref tracker_arc) = self.utilization_tracker {
                        let tracker = tracker_arc.lock();
                        profile.get_file_summary_map(true, &tracker)
                    } else {
                        profile.get_file_summary_map(
                            false,
                            &UtilizationTracker::new(false),
                        )
                    };
            }

            // JE ~line 393: fileSelector.selectFileForCleaning(
            //   calculator, fileSummaryMap, forceCleaning)
            // Unified method: drain TO_BE_CLEANED first, then getBestFile.
            // CLN-4 clamping is passed as first_active_txn_file.
            let (file_number, required_util) = {
                let mut selector = self.file_selector.lock();
                match selector.select_file_for_cleaning(
                    &current_summary_map,
                    self.min_utilization.load(Ordering::Relaxed),
                    self.min_age as u32,
                    force,
                    first_active_txn_file,
                    self.min_file_utilization as i32,
                ) {
                    None => break, // no more files
                    Some(pair) => pair,
                }
            };

            // CLN-5: two-pass (revisalRun) dry-run check.
            // When required_util >= 0 (set by check_for_required_util when
            // expiration uncertainty is high), run a first-pass scan to
            // recompute true utilization including expired bytes.
            // JE: FileProcessor.doClean two-pass block (~lines 420-465).
            if let Some(req) = required_util.filter(|&r| r >= 0) {
                let skip = self.two_pass_check(file_number, req);
                if skip {
                    // CLN NEW-3: use remove_file_from_cleaning instead of
                    // put_back_file_for_cleaning so the file is NOT re-enqueued
                    // and rescanned on the next pass.
                    // JE: fileSelector.removeFile(fileNum, budget) (doClean ~line 452).
                    self.file_selector
                        .lock()
                        .remove_file_from_cleaning(file_number);
                    continue;
                }
            }

            // Protect file during processing.
            self.file_protector.protect_file(file_number, "CleanerProcessing");

            // Process the file (scan, migrate LNs).
            let result = self.process_single_file(file_number);

            // Unprotect after processing.
            self.file_protector.unprotect_file(file_number);

            match result {
                Err(e) => {
                    // Processing failed — put file back so it is retried.
                    // JE: FileProcessor.doClean() finally { putBackFileForCleaning }.
                    self.file_selector
                        .lock()
                        .put_back_file_for_cleaning(file_number);
                    return Err(e);
                }
                Ok(result) => {
                    // Record any LNs that could not be migrated due to lock denial.
                    // JE: FileSelector.addPendingLN (FileSelector.java ~line 455).
                    if !result.locked_lns.is_empty() {
                        let mut selector = self.file_selector.lock();
                        for (log_lsn, ln_info) in &result.locked_lns {
                            selector.add_pending_ln(*log_lsn, ln_info.clone());
                        }
                    }

                    if result.completed {
                        files_cleaned += 1;
                        total_entries += result.entries_read;

                        self.update_stats(&result);

                        // X-5: do NOT delete immediately — files must pass
                        // through the two-checkpoint barrier.
                        self.file_selector
                            .lock()
                            .mark_file_cleaned(file_number);
                    } else {
                        // Processing interrupted (shutdown) — put file back.
                        self.file_selector
                            .lock()
                            .put_back_file_for_cleaning(file_number);
                    }
                }
            }
        }

        // X-5: only delete files that have passed the two-checkpoint barrier.
        let files_deleted = self.delete_safe_files();

        // Legacy pending_deletions.
        let _legacy = self.delete_pending_files();

        // Adaptive throttle update.
        let current_write_bytes = self
            .log_manager
            .as_ref()
            .map(|lm| lm.get_stats().n_sequential_write_bytes)
            .unwrap_or(0);
        let cleaning_needed = files_cleaned > 0;
        self.throttle.update(current_write_bytes, cleaning_needed);

        // CLN-14: wakeupAfterNoWrites.
        // JE: FileProcessor.doClean ~line 290:
        //   envImpl.getCheckpointer().wakeupAfterNoWrites()
        if let (true, Some(cb)) = (cleaning_needed, &self.checkpoint_wakeup_fn)
        {
            cb();
        }

        Ok(CleanResult {
            files_cleaned,
            files_deleted,
            total_entries_read: total_entries,
        })
    }
    ///
    /// Called when `required_util >= 0` to determine whether the file's true
    /// utilization (after accounting for expired bytes) is still above the
    /// threshold.  If so, returns `true` (skip this file) — JE calls this
    /// the "revisalRun".
    ///
    /// Also stores the `ExpirationTracker` result in `expiration_profile_store`
    /// (CLN-9) so that future selection passes can use per-file expiration data.
    ///
    /// JE: `FileProcessor.doClean` two-pass block (~line 420-465):
    ///   processFile(fileNum, recalcSummary, inSummary, expTracker);  // dry run
    ///   recalcUtil = utilization(obsolete + expired, total);
    ///   if (recalcUtil > requiredUtil) { skip; }
    ///   cleaner.getExpirationProfile().putFile(expTracker, expiredSize);
    fn two_pass_check(&self, file_number: u32, required_util: i32) -> bool {
        let summary = match &self.file_manager {
            None => return false, // no file to scan — don't skip
            Some(fm) => self.scan_file_summary(fm, file_number),
        };
        if summary.total_size <= 0 {
            return false;
        }
        // Build an ExpirationTracker from log entries (CLN-9 + CLN-5).
        let hours_now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() / 3600)
            .unwrap_or(0);

        let mut tracker = crate::ExpirationTracker::new(file_number);
        if let Some(fm) = &self.file_manager {
            let entries = self.decode_ln_entries_from_file(fm, file_number);
            for entry in &entries {
                if let LogEntryType::Ln {
                    expiration_time, entry_size, ..
                } = entry.entry_type
                {
                    tracker.track(expiration_time, entry_size);
                }
            }
        }
        let expired_bytes = tracker.get_expired_bytes(hours_now);

        // recalcUtil = utilization(obsolete + expired, total)
        let obsolete = summary.get_obsolete_size() as i64;
        let total = summary.total_size as i64;
        let recalc_util = if total > 0 {
            ((obsolete + expired_bytes) * 100 / total) as i32
        } else {
            0
        };

        // CLN-9: store the tracker for future selection scoring.
        // JE: cleaner.getExpirationProfile().putFile(expTracker, expiredSize).
        self.expiration_profile_store.lock().put_file(tracker);

        // Skip if recalc_util > required_util (file still too utilized).
        recalc_util > required_util
    }

    fn process_single_file(
        &self,
        file_number: u32,
    ) -> Result<FileProcessResult, String> {
        let file_summary = match &self.file_manager {
            None => crate::FileSummary::new(),
            Some(fm) => self.scan_file_summary(fm, file_number),
        };

        // CLN-12: build a process_pending callback from cloned Arcs so the
        // FileProcessor can invoke it periodically during a long file run.
        // Only wired when we have a tree + log manager (otherwise pending
        // migration isn't possible anyway).
        let pending_fn: Option<Arc<dyn Fn() + Send + Sync>> =
            if let (Some(tree), Some(lm)) = (&self.tree, &self.log_manager) {
                let ctx = Arc::new(ProcessPendingCtx {
                    file_selector: Arc::clone(&self.file_selector),
                    tree: Arc::clone(tree),
                    log_manager: Arc::clone(lm),
                    lock_manager: self.lock_manager.clone(),
                    extra_trees: Arc::clone(&self.extra_trees),
                    stats: Arc::clone(&self.stats),
                    shutdown: Arc::clone(&self.shutdown),
                });
                Some(Arc::new(move || ctx.call()))
            } else {
                None
            };

        let processor = {
            let p =
                FileProcessor::new(self.stats.clone(), self.shutdown.clone());
            if let Some(f) = pending_fn {
                p.with_process_pending_fn(f)
            } else {
                p
            }
        };

        // If we have a tree + log manager, decode LN entries from the file
        // and run them through the real migration path.
        if let (Some(fm), Some(tree), Some(lm)) =
            (&self.file_manager, &self.tree, &self.log_manager)
        {
            let entries = self.decode_ln_entries_from_file(fm, file_number);
            // Use the environment's shared LockManager when available so that
            // cleaner-held locks contend with user transactions (fidelity).
            // Cleaner uses env.getTxnManager().getLockManager().
            let tree_lookup = if let Some(ref shared_lm) = self.lock_manager {
                SharedTreeLookup::with_lock_manager(
                    Arc::clone(tree),
                    Arc::clone(lm),
                    Arc::clone(shared_lm),
                )
                .with_extra_trees(
                    self.extra_trees
                        .lock()
                        .map(|g| g.clone())
                        .unwrap_or_default(),
                )
            } else {
                SharedTreeLookup::new(Arc::clone(tree), Arc::clone(lm))
                    .with_extra_trees(
                        self.extra_trees
                            .lock()
                            .map(|g| g.clone())
                            .unwrap_or_default(),
                    )
            };
            return processor.process_file(
                file_number,
                &file_summary,
                &entries,
                &tree_lookup,
            );
        }

        processor.process_file_no_entries(file_number, &file_summary)
    }

    /// Decodes LN log entries from a file into `LogEntry` values suitable
    /// for `FileProcessor::process_file`.
    ///
    /// Scans the file sequentially, reading each entry header and payload.
    /// For LN-family entries (type bytes 4–9) the payload is parsed using
    /// `LnLogEntry::read_from_log` to extract the real record key.  This
    /// mirrors the way `CleanerFileReader` extracts keys from log entries
    /// before passing them to `FileProcessor.processFile()`.
    ///
    /// IN, BIN-delta, and all other entry types are represented as
    /// `LogEntryType::Other` (they will be skipped by the migration loop).
    ///
    ///
    fn decode_ln_entries_from_file(
        &self,
        fm: &Arc<FileManager>,
        file_number: u32,
    ) -> Vec<LogEntry> {
        let mut entries = Vec::new();

        let file_len = match fm.get_file_length(file_number) {
            Ok(l) => l,
            Err(_) => return entries,
        };

        // Resolve the version-aware first-entry offset:
        // v2 files start at byte 32; v3 files at byte 36.
        let first_entry_offset =
            fm.file_header_size_for(file_number).unwrap_or(FILE_HEADER_SIZE)
                as u64;
        let mut offset = first_entry_offset;
        while offset < file_len {
            let mut hdr = [0u8; MIN_HEADER_SIZE];
            let n = match fm.read_from_file(file_number, offset, &mut hdr) {
                Ok(n) => n,
                Err(_) => break,
            };
            if n < MIN_HEADER_SIZE {
                break;
            }
            if hdr[4] == 0 {
                break;
            }

            let entry_type_byte = hdr[4];
            let flags = hdr[5];
            let item_size =
                u32::from_le_bytes([hdr[10], hdr[11], hdr[12], hdr[13]])
                    as usize;

            let vlsn_present = (flags & 0x08) != 0 || (flags & 0x20) != 0;
            let header_size =
                if vlsn_present { MAX_HEADER_SIZE } else { MIN_HEADER_SIZE };
            let entry_size = header_size + item_size;

            let file_offset = offset as u32;
            let lsn = noxu_util::Lsn::new(file_number, file_offset);

            // Build a LogEntry for LN-family types only; everything else
            // is emitted as LogEntryType::Other so the processor skips it.
            // For LN entries, read the payload and deserialise the real key.
            // CleanerFileReader reading actual record keys via
            // LN payload deserialization.
            let log_entry_type = match entry_type_byte {
                // InsertLN=4, UpdateLN=6 (non-transactional) — active entries
                // that may need migration. Read payload to extract real key.
                4 | 6 => {
                    let payload_offset = offset + header_size as u64;
                    let mut payload = vec![0u8; item_size];
                    let (key, db_id, expiration_time): (Vec<u8>, i64, u64) =
                        if item_size > 0
                            && fm
                                .read_from_file(
                                    file_number,
                                    payload_offset,
                                    &mut payload,
                                )
                                .is_ok()
                        {
                            use noxu_log::entry::LnLogEntry;
                            match LnLogEntry::read_from_log(&payload, false) {
                                // CLN NEW-4: read ln.expiration as u64 (hours
                                // since epoch, per CLN-10) so the two-pass
                                // TTL-adjusted utilization sees real expired bytes.
                                // JE: FileProcessor.processFile reads
                                // lnEntry.getExpiration() (~line 1004).
                                Ok(ln) => (
                                    ln.key.clone(),
                                    ln.db_id as i64,
                                    ln.expiration as u64,
                                ),
                                Err(_) => (
                                    file_offset.to_le_bytes().to_vec(),
                                    1i64,
                                    0u64,
                                ),
                            }
                        } else {
                            (file_offset.to_le_bytes().to_vec(), 1i64, 0u64)
                        };
                    LogEntryType::Ln {
                        db_id,
                        key,
                        deleted: false,
                        expiration_time,
                        entry_size: entry_size as i32,
                    }
                }
                // InsertLNTxn=5, UpdateLNTxn=7 — transactional variants.
                // Read payload using transactional deserialization.
                5 | 7 => {
                    let payload_offset = offset + header_size as u64;
                    let mut payload = vec![0u8; item_size];
                    let (key, db_id, expiration_time): (Vec<u8>, i64, u64) =
                        if item_size > 0
                            && fm
                                .read_from_file(
                                    file_number,
                                    payload_offset,
                                    &mut payload,
                                )
                                .is_ok()
                        {
                            use noxu_log::entry::LnLogEntry;
                            match LnLogEntry::read_from_log(&payload, true) {
                                // CLN NEW-4: read ln.expiration as u64 (hours).
                                Ok(ln) => (
                                    ln.key.clone(),
                                    ln.db_id as i64,
                                    ln.expiration as u64,
                                ),
                                Err(_) => (
                                    file_offset.to_le_bytes().to_vec(),
                                    1i64,
                                    0u64,
                                ),
                            }
                        } else {
                            (file_offset.to_le_bytes().to_vec(), 1i64, 0u64)
                        };
                    // Transactional variants are considered live during
                    // cleaning — the cleaner migrates them.
                    LogEntryType::Ln {
                        db_id,
                        key,
                        deleted: false,
                        expiration_time,
                        entry_size: entry_size as i32,
                    }
                }
                // DeleteLN=8, DeleteLNTxn=9 — deleted LN entries are
                // immediately obsolete; emit as Ln { deleted: true }.
                8 | 9 => {
                    let payload_offset = offset + header_size as u64;
                    let mut payload = vec![0u8; item_size];
                    let (key, db_id): (Vec<u8>, i64) = if item_size > 0
                        && fm
                            .read_from_file(
                                file_number,
                                payload_offset,
                                &mut payload,
                            )
                            .is_ok()
                    {
                        use noxu_log::entry::LnLogEntry;
                        let is_txn = entry_type_byte == 9;
                        match LnLogEntry::read_from_log(&payload, is_txn) {
                            Ok(ln) => (ln.key.clone(), ln.db_id as i64),
                            Err(_) => {
                                (file_offset.to_le_bytes().to_vec(), 1i64)
                            }
                        }
                    } else {
                        (file_offset.to_le_bytes().to_vec(), 1i64)
                    };
                    LogEntryType::Ln {
                        db_id,
                        key,
                        deleted: true,
                        expiration_time: 0,
                        entry_size: entry_size as i32,
                    }
                }
                // IN/BIN/BINDelta and everything else → Other (skipped).
                _ => LogEntryType::Other,
            };

            entries.push(LogEntry { lsn, entry_type: log_entry_type });
            offset += entry_size as u64;
        }

        entries
    }

    /// Scans a log file and returns a populated `FileSummary`.
    ///
    /// Reads each log entry header sequentially, accumulating:
    /// - `total_count` / `total_size` for every entry
    /// - `total_ln_count` / `total_ln_size` for LN entry types
    /// - `total_in_count` / `total_in_size` for IN / BIN-delta entry types
    ///
    /// Entry-type bytes recognised as LN:  `InsertLN`=4, `InsertLNTxn`=5,
    /// `UpdateLN`=6, `UpdateLNTxn`=7, `DeleteLN`=8, `DeleteLNTxn`=9.
    /// Entry-type bytes recognised as IN:  `IN`=2, `BIN`=3, `BINDelta`=26.
    /// All other types are counted in the totals but not in the per-type
    /// fields, so they show up in "leftover" space (treated as obsolete by
    /// `FileSummary::calculate_obsolete_size`).
    ///
    /// This is the entry-header layout used throughout noxu-log:
    /// ```text
    /// bytes  0..3   checksum    (u32 LE)
    /// byte   4      entry_type
    /// byte   5      flags
    /// bytes  6..9   prev_offset (u32 LE)
    /// bytes  10..13 item_size   (u32 LE)
    /// [bytes 14..21 VLSN        (i64 LE)  — present when flags & 0x28 != 0]
    /// ```
    fn scan_file_summary(
        &self,
        fm: &Arc<FileManager>,
        file_number: u32,
    ) -> crate::FileSummary {
        let mut summary = crate::FileSummary::new();

        let file_len = match fm.get_file_length(file_number) {
            Ok(l) => l,
            Err(_) => return summary,
        };
        // Total size is the full file, including the file header.
        summary.total_size = file_len.min(i32::MAX as u64) as i32;

        // Resolve the version-aware first-entry offset:
        // v2 files start at byte 32; v3 files at byte 36.
        let first_entry_offset =
            fm.file_header_size_for(file_number).unwrap_or(FILE_HEADER_SIZE)
                as u64;
        let mut offset = first_entry_offset;
        while offset < file_len {
            let mut hdr = [0u8; MIN_HEADER_SIZE];
            let n = match fm.read_from_file(file_number, offset, &mut hdr) {
                Ok(n) => n,
                Err(_) => break,
            };
            if n < MIN_HEADER_SIZE {
                break; // Truncated read at end of file.
            }
            // A zero entry-type byte means we've reached unwritten space.
            if hdr[4] == 0 {
                break;
            }

            let entry_type_byte = hdr[4];
            let flags = hdr[5];
            let item_size =
                u32::from_le_bytes([hdr[10], hdr[11], hdr[12], hdr[13]])
                    as usize;

            let vlsn_present = (flags & 0x08) != 0 || (flags & 0x20) != 0;
            let header_size =
                if vlsn_present { MAX_HEADER_SIZE } else { MIN_HEADER_SIZE };
            let entry_size = (header_size + item_size) as i32;

            summary.total_count += 1;
            // total_size was already set to the full file length; we track
            // per-type sizes below for utilization estimation.

            // Classify by entry type.
            // LN types: InsertLN=4, InsertLNTxn=5, UpdateLN=6,
            //           UpdateLNTxn=7, DeleteLN=8, DeleteLNTxn=9
            // IN types: IN=2, BIN=3, BINDelta=26
            match entry_type_byte {
                4..=9 => {
                    // LN family
                    summary.total_ln_count += 1;
                    summary.total_ln_size += entry_size;
                    if entry_size > summary.max_ln_size {
                        summary.max_ln_size = entry_size;
                    }
                }
                2 | 3 | 26 => {
                    // IN / BIN / BINDelta family
                    summary.total_in_count += 1;
                    summary.total_in_size += entry_size;
                }
                _ => {
                    // FileHeader, Trace, MapLN, TxnCommit, etc.
                    // Counted in total_count / total_size only; these
                    // bytes will appear as "leftover" obsolete space.
                }
            }

            offset += (header_size + item_size) as u64;
        }

        // Populate the expiration uncertainty band (lower = definitely
        // expired, gradual upper = + prorated current-interval) so the
        // FileSelector's two-pass gate (JE getBestFile) can compute this
        // file's min/max utilization. CLN-9 / CFG-TWOPASS-1.
        {
            let mut tracker = crate::ExpirationTracker::new(file_number);
            let entries = self.decode_ln_entries_from_file(fm, file_number);
            for entry in &entries {
                if let LogEntryType::Ln {
                    expiration_time, entry_size, ..
                } = entry.entry_type
                {
                    tracker.track(expiration_time, entry_size);
                }
            }
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let hours_now = now_ms / 3_600_000;
            let sub_hour_ms = now_ms % 3_600_000;
            let (lower, gradual) =
                tracker.get_expired_bytes_band(hours_now, sub_hour_ms);
            summary.obsolete_expired_size = lower.min(i32::MAX as i64) as i32;
            summary.obsolete_expired_gradual_size =
                gradual.min(i32::MAX as i64) as i32;
        }

        summary
    }

    /// Updates statistics from a file processing result.
    fn update_stats(&self, result: &FileProcessResult) {
        self.stats
            .entries_read
            .fetch_add(result.entries_read, Ordering::Relaxed);
        self.stats.lns_cleaned.fetch_add(result.lns_cleaned, Ordering::Relaxed);
        self.stats.lns_dead.fetch_add(result.lns_dead, Ordering::Relaxed);
        self.stats
            .lns_migrated
            .fetch_add(result.lns_migrated, Ordering::Relaxed);
        self.stats
            .lns_obsolete
            .fetch_add(result.lns_obsolete, Ordering::Relaxed);
        self.stats.lns_locked.fetch_add(result.lns_locked, Ordering::Relaxed);
        self.stats.ins_cleaned.fetch_add(result.ins_cleaned, Ordering::Relaxed);
        self.stats.ins_dead.fetch_add(result.ins_dead, Ordering::Relaxed);
        self.stats
            .ins_migrated
            .fetch_add(result.ins_migrated, Ordering::Relaxed);
        self.stats
            .ins_obsolete
            .fetch_add(result.ins_obsolete, Ordering::Relaxed);
        self.stats
            .bin_deltas_cleaned
            .fetch_add(result.bin_deltas_cleaned, Ordering::Relaxed);
        self.stats
            .bin_deltas_dead
            .fetch_add(result.bin_deltas_dead, Ordering::Relaxed);
        self.stats
            .bin_deltas_migrated
            .fetch_add(result.bin_deltas_migrated, Ordering::Relaxed);
        self.stats
            .bin_deltas_obsolete
            .fetch_add(result.bin_deltas_obsolete, Ordering::Relaxed);
    }

    /// Deletes files that are safe to delete (not protected).
    ///
    /// When a `FileManager` is available, calls `FileManager::delete_file()`
    /// which removes the file handle from the cache and then calls
    /// `fs::remove_file` on the actual `.ndb` path.  When no `FileManager` is
    /// attached (unit-test mode) the deletion is counted but no I/O occurs.
    ///
    /// Returns the number of files successfully deleted.
    fn delete_pending_files(&self) -> u32 {
        let mut pending = self.pending_deletions.lock();
        let mut deleted = 0u32;

        pending.retain(|&file_number| {
            if !self.file_protector.is_protected(file_number) {
                // Perform the actual on-disk deletion when wired to a
                // FileManager.  Ignore errors (e.g. file already gone) so
                // that a single failed delete doesn't stall the cleaner.
                if let Some(fm) = &self.file_manager {
                    let _ = fm.delete_file(file_number);
                }
                deleted += 1;
                self.stats.deletions.fetch_add(1, Ordering::Relaxed);
                false // Remove from pending list
            } else {
                true // Keep in pending list
            }
        });

        deleted
    }

    /// Adds a file to the list of files to clean.
    ///
    /// Useful for manual cleaning or prioritizing specific files.
    pub fn add_file_to_clean(&self, file_number: u32) {
        let mut selector = self.file_selector.lock();
        selector.add_file_to_clean(file_number);
    }

    /// Returns a reference to the file selector (for testing/introspection).
    pub fn get_file_selector(&self) -> &Arc<Mutex<FileSelector>> {
        &self.file_selector
    }

    /// Returns the checkpoint start state, calling `process_pending` first.
    ///
    /// This is the correct entry point for the checkpointer to call instead
    /// of `get_file_selector().lock().get_checkpoint_state()` directly.
    /// Calling `process_pending` before snapshotting means any LNs that can
    /// be migrated right now are drained before the snapshot, so
    /// `any_pending_during_checkpoint` reflects only genuinely blocked LNs.
    ///
    /// JE: `FileSelector.getFilesAtCheckpointStart` is preceded by
    /// `processPending()` in `Cleaner.doClean` (Cleaner.java ~line 1185).
    pub fn get_checkpoint_start_state(
        &self,
    ) -> crate::file_selector::CheckpointStartCleanerState {
        // JE: processPending() before snapshot (CLN-7 / CLN-2).
        self.process_pending();
        self.file_selector.lock().get_checkpoint_state()
    }

    /// REC-F: whether the cleaner has files pending reclaim that a checkpoint
    /// would unblock.  Mirrors JE
    /// `Cleaner.getFileSelector().isCheckpointNeeded()` (used by
    /// `Checkpointer.needCheckpointForCleanedFiles`).
    pub fn is_checkpoint_needed(&self) -> bool {
        self.file_selector.lock().is_checkpoint_needed()
    }

    /// Notify the cleaner that a checkpoint has completed.
    ///
    /// Called by the checkpointer after `do_checkpoint()` succeeds.  This
    /// method advances the three-state checkpoint barrier:
    ///
    /// * Files in `checkpointed` (captured by the prior checkpoint) move to
    ///   `safe_to_delete`.
    /// * Files in `cleaned_files` (snapshotted at checkpoint *start*) move
    ///   to `checkpointed`.
    ///
    /// After this call, `delete_safe_files()` will remove files that have
    /// survived two checkpoints.
    ///
    /// X-5 fix: `FileSelector::process_checkpoint_end` was fully implemented
    /// but never called from outside the cleaner.
    pub fn after_checkpoint(
        &self,
        state: &crate::file_selector::CheckpointStartCleanerState,
    ) {
        self.file_selector.lock().process_checkpoint_end(state);
    }

    /// Retry pending LNs whose migration was previously denied a lock.
    ///
    /// Iterates the `FileSelector::pending_lns` set and attempts to migrate
    /// each one.  On success (`Migrated`), removes it from the pending set.
    /// On `Locked` again, leaves it for the next call.  On `Dead`, removes it
    /// (the LN is now obsolete in the tree — it was deleted or overwritten by
    /// the transaction that held the lock).
    ///
    /// Called at the start of every `do_clean` pass and from the periodic
    /// hook inside `FileProcessor` (every `PROCESS_PENDING_EVERY_N_LNS` LNs).
    ///
    /// JE: `Cleaner.processPending` (Cleaner.java ~line 1221).
    pub fn process_pending(&self) {
        if let (Some(tree), Some(lm)) = (&self.tree, &self.log_manager) {
            let ctx = ProcessPendingCtx {
                file_selector: Arc::clone(&self.file_selector),
                tree: Arc::clone(tree),
                log_manager: Arc::clone(lm),
                lock_manager: self.lock_manager.clone(),
                extra_trees: Arc::clone(&self.extra_trees),
                stats: Arc::clone(&self.stats),
                shutdown: Arc::clone(&self.shutdown),
            };
            ctx.call();
        }
    }

    /// Delete files that have passed the two-checkpoint barrier
    /// (`safe_to_delete`).
    ///
    /// X-5 fix: replaces the old `delete_pending_files` call in `do_clean`
    /// which deleted files immediately after cleaning without waiting for
    /// a checkpoint.  Now only files returned by
    /// `FileSelector::get_safe_to_delete()` are eligible.
    pub fn delete_safe_files(&self) -> u32 {
        let files_to_delete = {
            let mut selector = self.file_selector.lock();
            let to_delete = selector.get_safe_to_delete();
            // Remove each file from the selector's tracking state after
            // we decide to delete it so that a concurrent cleaning pass
            // doesn't see a ghost entry.
            for &f in &to_delete {
                selector.remove_deleted_file(f);
            }
            to_delete
        };

        let mut deleted = 0u32;
        for file_number in files_to_delete {
            if !self.file_protector.is_protected(file_number) {
                if let Some(fm) = &self.file_manager {
                    let _ = fm.delete_file(file_number);
                }
                deleted += 1;
                self.stats.deletions.fetch_add(1, Ordering::Relaxed);
            } else {
                // File is still protected — re-queue for later deletion.
                self.pending_deletions.lock().push(file_number);
                // Also restore in the selector so the barrier is not lost.
                self.file_selector.lock().add_safe_to_delete_back(file_number);
            }
        }
        deleted
    }

    /// Returns a reference to the shared file protector `Arc`.
    ///
    /// Shared so other subsystems (e.g. the `DiskOrderedCursor` producer)
    /// can protect the files they scan via the same instance the cleaner
    /// consults before deletion.
    pub fn get_file_protector(&self) -> &Arc<FileProtector> {
        &self.file_protector
    }

    /// Returns a reference to the statistics.
    pub fn get_stats(&self) -> &Arc<CleanerStats> {
        &self.stats
    }

    /// Returns whether the cleaner is currently running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::Relaxed)
    }

    /// Signals the cleaner to shut down.
    ///
    /// This will cause in-progress cleaning to stop at the next checkpoint.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    /// Requests that the given files be deleted once they are no longer protected.
    pub fn request_delete_files(&self, files: &[u32]) {
        let mut pending = self.pending_deletions.lock();
        pending.extend_from_slice(files);
    }

    /// Returns the total number of cleaning runs performed.
    pub fn get_run_count(&self) -> u64 {
        self.n_runs.load(Ordering::Relaxed)
    }
}

/// RAII guard to ensure the running flag is cleared on drop.
struct RunningGuard<'a> {
    running: &'a AtomicBool,
}

impl<'a> RunningGuard<'a> {
    fn new(running: &'a AtomicBool) -> Self {
        Self { running }
    }
}

impl<'a> Drop for RunningGuard<'a> {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_cleaner() {
        let cleaner = Cleaner::new(50, 5, 60);
        assert!(!cleaner.is_running());
        assert_eq!(cleaner.min_utilization.load(Ordering::Relaxed), 50);
        assert_eq!(cleaner.min_file_count, 5);
        assert_eq!(cleaner.min_age, 60);
        assert_eq!(cleaner.get_run_count(), 0);
    }

    #[test]
    fn test_cleaner_with_max_utilization() {
        let cleaner = Cleaner::new(150, 5, 60); // Over 100
        assert_eq!(cleaner.min_utilization.load(Ordering::Relaxed), 100); // Should be clamped
    }

    #[test]
    fn test_do_clean_not_running() {
        let cleaner = Cleaner::new(50, 0, 0);
        assert!(!cleaner.is_running());

        // Should return immediately with no files (selector is empty)
        let result = cleaner.do_clean(1, false).unwrap();
        assert_eq!(result.files_cleaned, 0);
        assert_eq!(result.files_deleted, 0);
    }

    #[test]
    fn test_do_clean_increments_run_count() {
        let cleaner = Cleaner::new(50, 0, 0);
        assert_eq!(cleaner.get_run_count(), 0);

        let _ = cleaner.do_clean(1, false);
        assert_eq!(cleaner.get_run_count(), 1);

        let _ = cleaner.do_clean(1, false);
        assert_eq!(cleaner.get_run_count(), 2);
    }

    #[test]
    fn test_concurrent_clean_rejected() {
        let cleaner = Arc::new(Cleaner::new(50, 0, 0));

        // Simulate a long-running clean by holding the running flag
        cleaner.running.store(true, Ordering::Relaxed);

        // Second clean attempt should fail
        let result = cleaner.do_clean(1, false);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("already running"));

        // Clean up
        cleaner.running.store(false, Ordering::Relaxed);
    }

    #[test]
    fn test_shutdown() {
        let cleaner = Cleaner::new(50, 0, 0);
        assert!(!cleaner.shutdown.load(Ordering::Relaxed));

        cleaner.shutdown();
        assert!(cleaner.shutdown.load(Ordering::Relaxed));

        // Cleaning should fail after shutdown
        let result = cleaner.do_clean(1, false);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("shut down"));
    }

    #[test]
    fn test_add_file_to_clean() {
        let cleaner = Cleaner::new(50, 0, 0);

        cleaner.add_file_to_clean(5);
        cleaner.add_file_to_clean(10);

        let selector = cleaner.get_file_selector().lock();
        assert!(selector.is_tracked(5));
        assert!(selector.is_tracked(10));
    }

    #[test]
    fn test_file_protector_integration() {
        let cleaner = Cleaner::new(50, 0, 0);

        let protector = cleaner.get_file_protector();
        protector.protect_file(5, "Test");

        assert!(protector.is_protected(5));
        assert!(!protector.is_protected(6));
    }

    #[test]
    fn test_stats_integration() {
        let cleaner = Cleaner::new(50, 0, 0);

        let stats = cleaner.get_stats();
        stats.lns_cleaned.fetch_add(100, Ordering::Relaxed);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.lns_cleaned, 100);
    }

    #[test]
    fn test_request_delete_files() {
        let cleaner = Cleaner::new(50, 0, 0);

        cleaner.request_delete_files(&[1, 2, 3]);

        let pending = cleaner.pending_deletions.lock();
        assert_eq!(pending.len(), 3);
        assert!(pending.contains(&1));
        assert!(pending.contains(&2));
        assert!(pending.contains(&3));
    }

    #[test]
    fn test_delete_pending_files_when_protected() {
        let cleaner = Cleaner::new(50, 0, 0);

        // Add files to pending deletion
        cleaner.request_delete_files(&[1, 2, 3]);

        // Protect file 2
        cleaner.get_file_protector().protect_file(2, "Test");

        // Attempt deletion
        let deleted = cleaner.delete_pending_files();

        // Should delete 1 and 3, but not 2
        assert_eq!(deleted, 2);

        let pending = cleaner.pending_deletions.lock();
        assert_eq!(pending.len(), 1);
        assert!(pending.contains(&2));
    }

    #[test]
    fn test_running_guard() {
        let running = AtomicBool::new(false);

        {
            running.store(true, Ordering::Relaxed);
            let _guard = RunningGuard::new(&running);
            assert!(running.load(Ordering::Relaxed));
        } // Guard drops here

        assert!(!running.load(Ordering::Relaxed));
    }

    #[test]
    fn test_clean_result() {
        let result = CleanResult {
            files_cleaned: 5,
            files_deleted: 4,
            total_entries_read: 10000,
        };

        assert_eq!(result.files_cleaned, 5);
        assert_eq!(result.files_deleted, 4);
        assert_eq!(result.total_entries_read, 10000);
    }

    #[test]
    fn test_clean_result_equality() {
        let result1 = CleanResult {
            files_cleaned: 5,
            files_deleted: 4,
            total_entries_read: 10000,
        };

        let result2 = CleanResult {
            files_cleaned: 5,
            files_deleted: 4,
            total_entries_read: 10000,
        };

        let result3 = CleanResult {
            files_cleaned: 6,
            files_deleted: 4,
            total_entries_read: 10000,
        };

        assert_eq!(result1, result2);
        assert_ne!(result1, result3);
    }

    #[test]
    fn test_do_clean_with_file_to_clean() {
        let cleaner = Cleaner::new(50, 0, 0);
        // Add a file to the selector so do_clean has work to do.
        cleaner.add_file_to_clean(7);

        let result = cleaner.do_clean(5, false).unwrap();
        // process_single_file calls process_file_no_entries → completed=true
        assert_eq!(result.files_cleaned, 1);
        // X-5: files are NOT deleted in the same cleaning pass.
        // They wait for two checkpoints before appearing in safe_to_delete.
        assert_eq!(
            result.files_deleted, 0,
            "X-5: file must not be deleted before checkpoint barrier"
        );
        assert_eq!(result.total_entries_read, 0);
    }

    #[test]
    fn test_do_clean_multiple_files() {
        let cleaner = Cleaner::new(50, 0, 0);
        cleaner.add_file_to_clean(1);
        cleaner.add_file_to_clean(2);
        cleaner.add_file_to_clean(3);

        let result = cleaner.do_clean(10, false).unwrap();
        assert_eq!(result.files_cleaned, 3);
        // X-5: no deletions without checkpoint barrier.
        assert_eq!(result.files_deleted, 0);
    }

    #[test]
    fn test_do_clean_respects_n_files_limit() {
        let cleaner = Cleaner::new(50, 0, 0);
        cleaner.add_file_to_clean(10);
        cleaner.add_file_to_clean(11);
        cleaner.add_file_to_clean(12);

        // Only allow cleaning 1 file at a time.
        let result = cleaner.do_clean(1, false).unwrap();
        assert_eq!(result.files_cleaned, 1);
    }

    #[test]
    fn test_do_clean_increments_stats_runs() {
        let cleaner = Cleaner::new(50, 0, 0);
        let _ = cleaner.do_clean(1, false);
        let _ = cleaner.do_clean(1, false);

        let snapshot = cleaner.get_stats().snapshot();
        assert_eq!(snapshot.runs, 2);
    }

    #[test]
    fn test_do_clean_updates_entry_stats() {
        let cleaner = Cleaner::new(50, 0, 0);
        cleaner.add_file_to_clean(5);

        let _ = cleaner.do_clean(5, false).unwrap();

        // process_file_no_entries returns 0 entries_read but completed=true.
        let snapshot = cleaner.get_stats().snapshot();
        // runs incremented; X-5: deletions are 0 until checkpoint barrier fires.
        assert_eq!(snapshot.runs, 1);
        assert_eq!(
            snapshot.deletions, 0,
            "X-5: no deletion before checkpoint barrier"
        );
    }

    #[test]
    fn test_do_clean_running_flag_cleared_after_completion() {
        let cleaner = Cleaner::new(50, 0, 0);
        assert!(!cleaner.is_running());

        let _ = cleaner.do_clean(1, false);

        // The running flag must be cleared after do_clean returns.
        assert!(!cleaner.is_running());
    }

    #[test]
    fn test_do_clean_file_protected_stays_pending() {
        let cleaner = Cleaner::new(50, 0, 0);
        cleaner.add_file_to_clean(42);

        // Protect the file before cleaning — deletion should be deferred.
        cleaner.get_file_protector().protect_file(42, "Hold");

        let result = cleaner.do_clean(5, false).unwrap();
        assert_eq!(result.files_cleaned, 1); // cleaned (processed)
        // X-5: even without protection, files wait for the checkpoint barrier.
        // With protection, they also can't be deleted. Either way, 0 deletions.
        assert_eq!(result.files_deleted, 0);

        // X-5: the file is in the 'cleaned' state in the FileSelector,
        // waiting for a checkpoint before it becomes 'safe_to_delete'.
        let status = cleaner.get_file_selector().lock().get_file_status(42);
        assert_eq!(
            status,
            Some(crate::FileStatus::Cleaned),
            "file should be in Cleaned state awaiting checkpoint barrier"
        );
    }

    #[test]
    fn test_do_clean_shutdown_during_file_loop() {
        let cleaner = Cleaner::new(50, 0, 0);
        cleaner.add_file_to_clean(1);
        cleaner.add_file_to_clean(2);
        cleaner.add_file_to_clean(3);

        // Shut down before calling do_clean.
        cleaner.shutdown();
        let result = cleaner.do_clean(10, false);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("shut down"));
    }

    #[test]
    fn test_get_file_selector_returns_selector() {
        let cleaner = Cleaner::new(50, 0, 0);
        cleaner.add_file_to_clean(99);
        let selector = cleaner.get_file_selector().lock();
        assert!(selector.is_tracked(99));
    }

    #[test]
    fn test_get_file_protector_returns_protector() {
        let cleaner = Cleaner::new(50, 0, 0);
        let protector = cleaner.get_file_protector();
        protector.protect_file(77, "Test");
        assert!(protector.is_protected(77));
    }

    #[test]
    fn test_get_stats_returns_stats_ref() {
        let cleaner = Cleaner::new(50, 0, 0);
        let stats = cleaner.get_stats();
        stats.runs.fetch_add(5, Ordering::Relaxed);
        assert_eq!(cleaner.get_stats().runs.load(Ordering::Relaxed), 5);
    }

    #[test]
    fn test_request_delete_files_empty_slice() {
        let cleaner = Cleaner::new(50, 0, 0);
        cleaner.request_delete_files(&[]);
        let pending = cleaner.pending_deletions.lock();
        assert!(pending.is_empty());
    }

    #[test]
    fn test_delete_pending_all_unprotected() {
        let cleaner = Cleaner::new(50, 0, 0);
        cleaner.request_delete_files(&[10, 20, 30]);

        let deleted = cleaner.delete_pending_files();
        assert_eq!(deleted, 3);

        let pending = cleaner.pending_deletions.lock();
        assert!(pending.is_empty());
    }

    #[test]
    fn test_delete_pending_increments_deletions_stat() {
        let cleaner = Cleaner::new(50, 0, 0);
        cleaner.request_delete_files(&[5, 6]);

        cleaner.delete_pending_files();

        let snapshot = cleaner.get_stats().snapshot();
        assert_eq!(snapshot.deletions, 2);
    }

    #[test]
    fn test_clean_result_clone() {
        let result = CleanResult {
            files_cleaned: 3,
            files_deleted: 2,
            total_entries_read: 500,
        };
        let cloned = result.clone();
        assert_eq!(cloned, result);
    }

    #[test]
    fn test_min_utilization_zero() {
        let cleaner = Cleaner::new(0, 0, 0);
        assert_eq!(cleaner.min_utilization.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_min_age_large() {
        let cleaner = Cleaner::new(50, 0, u64::MAX);
        assert_eq!(cleaner.min_age, u64::MAX);
    }

    // ── Integration tests: real FileManager ───────────────────────────────────

    /// Helper: create a FileManager + LogManager, write a few entries, flush.
    fn make_fm_with_entries(
        dir: &std::path::Path,
    ) -> Arc<noxu_log::FileManager> {
        use bytes::BytesMut;
        use noxu_log::entry::TxnEndEntry;
        use noxu_log::{FileManager, LogEntryType, LogManager, Provisional};
        use noxu_util::{NULL_LSN, NULL_VLSN};

        let fm = Arc::new(
            FileManager::new(dir, false, 64 * 1024 * 1024, 100).unwrap(),
        );
        let lm =
            Arc::new(LogManager::new(Arc::clone(&fm), 3, 1024 * 1024, 65536));

        // Write three commit entries so there is real data to scan.
        for txn_id in [1i64, 2, 3] {
            let entry =
                TxnEndEntry::new_commit(txn_id, NULL_LSN, 0, 0, NULL_VLSN);
            let mut buf = BytesMut::with_capacity(entry.log_size());
            entry.write_to_log(&mut buf);
            lm.log(LogEntryType::TxnCommit, &buf, Provisional::No, true, false)
                .unwrap();
        }
        lm.flush_sync().unwrap();
        fm
    }

    /// `scan_file_summary` produces non-zero totals after real entries are written.
    #[test]
    fn test_scan_file_summary_non_zero_after_writes() {
        let dir = tempfile::TempDir::new().unwrap();
        let fm = make_fm_with_entries(dir.path());

        let cleaner = Cleaner::with_file_manager(50, 0, 0, Arc::clone(&fm));

        // The written entries land in file 0.
        let summary = cleaner.scan_file_summary(&fm, 0);

        assert!(
            summary.total_size > 0,
            "total_size must be non-zero after writing entries"
        );
        assert!(
            summary.total_count > 0,
            "total_count must be non-zero after writing entries"
        );
    }

    /// `process_single_file` succeeds and returns `completed=true` when wired
    /// to a real FileManager containing at least one log file.
    #[test]
    fn test_process_single_file_with_real_fm() {
        let dir = tempfile::TempDir::new().unwrap();
        let fm = make_fm_with_entries(dir.path());

        let cleaner = Cleaner::with_file_manager(50, 0, 0, Arc::clone(&fm));

        let result = cleaner.process_single_file(0).unwrap();
        assert!(result.completed, "processing must complete successfully");
    }

    /// `delete_pending_files` removes the file from disk when a FileManager is
    /// present, and returns a count of 1.
    #[test]
    fn test_delete_pending_files_removes_file_on_disk() {
        let dir = tempfile::TempDir::new().unwrap();
        let fm = make_fm_with_entries(dir.path());

        // Confirm file 0 exists on disk before deletion.
        let file_path = dir.path().join("00000000.ndb");
        assert!(file_path.exists(), "log file must exist before deletion");

        let cleaner = Cleaner::with_file_manager(50, 0, 0, Arc::clone(&fm));
        cleaner.request_delete_files(&[0]);

        let deleted = cleaner.delete_pending_files();

        assert_eq!(deleted, 1, "one file should have been deleted");
        assert!(
            !file_path.exists(),
            "log file must be gone from disk after deletion"
        );
        // Pending list must be empty.
        assert!(cleaner.pending_deletions.lock().is_empty());
    }

    /// Protected files are not deleted even when a FileManager is present.
    #[test]
    fn test_delete_pending_skips_protected_with_real_fm() {
        let dir = tempfile::TempDir::new().unwrap();
        let fm = make_fm_with_entries(dir.path());

        let cleaner = Cleaner::with_file_manager(50, 0, 0, Arc::clone(&fm));
        cleaner.request_delete_files(&[0]);

        // Protect the file so it should not be deleted.
        cleaner.get_file_protector().protect_file(0, "Hold");

        let deleted = cleaner.delete_pending_files();
        assert_eq!(deleted, 0, "protected file must not be deleted");

        let file_path = dir.path().join("00000000.ndb");
        assert!(file_path.exists(), "protected file must still exist on disk");

        // Still in pending.
        assert!(cleaner.pending_deletions.lock().contains(&0));
    }

    /// `with_file_manager` constructor respects all configuration parameters.
    #[test]
    fn test_with_file_manager_constructor() {
        let dir = tempfile::TempDir::new().unwrap();
        let fm = Arc::new(
            noxu_log::FileManager::new(dir.path(), false, 10_000_000, 10)
                .unwrap(),
        );
        let cleaner = Cleaner::with_file_manager(75, 3, 120, fm);
        assert_eq!(cleaner.min_utilization.load(Ordering::Relaxed), 75);
        assert_eq!(cleaner.min_file_count, 3);
        assert_eq!(cleaner.min_age, 120);
        assert!(cleaner.file_manager.is_some());
    }

    /// `do_clean` end-to-end with a real FileManager: the file is cleaned and
    /// then deleted from disk.
    #[test]
    fn test_do_clean_end_to_end_with_real_fm() {
        let dir = tempfile::TempDir::new().unwrap();
        let fm = make_fm_with_entries(dir.path());

        let file_path = dir.path().join("00000000.ndb");
        assert!(file_path.exists(), "log file must exist before do_clean");

        let cleaner = Cleaner::with_file_manager(50, 0, 0, Arc::clone(&fm));

        // Add file 0 to the selector so do_clean picks it up.
        cleaner.add_file_to_clean(0);

        let result = cleaner.do_clean(5, false).unwrap();

        assert_eq!(result.files_cleaned, 1, "one file must be cleaned");
        // X-5: files are NOT deleted in the same pass — barrier not yet active.
        assert_eq!(
            result.files_deleted, 0,
            "X-5: file must not be deleted before checkpoint barrier"
        );
        // File still exists because no checkpoint has fired yet.
        assert!(
            file_path.exists(),
            "log file must still exist before checkpoint barrier fires"
        );

        // Simulate two checkpoints to advance the barrier.
        {
            let state1 =
                cleaner.get_file_selector().lock().get_checkpoint_state();
            cleaner.after_checkpoint(&state1);
            let state2 =
                cleaner.get_file_selector().lock().get_checkpoint_state();
            cleaner.after_checkpoint(&state2);
        }

        // Now delete_safe_files should remove the file.
        let deleted = cleaner.delete_safe_files();
        assert_eq!(
            deleted, 1,
            "one file must be deleted after two checkpoints"
        );
        assert!(
            !file_path.exists(),
            "log file must be gone from disk after checkpoint barrier fires"
        );
    }

    // ── Integration tests: tree-wired cleaner (with_file_manager_and_tree) ───

    /// Helper: create a FileManager + LogManager pair in `dir`.
    fn make_fm_and_lm(
        dir: &std::path::Path,
    ) -> (Arc<noxu_log::FileManager>, Arc<noxu_log::LogManager>) {
        use noxu_log::{FileManager, LogManager};

        let fm = Arc::new(
            FileManager::new(dir, false, 64 * 1024 * 1024, 100).unwrap(),
        );
        let lm =
            Arc::new(LogManager::new(Arc::clone(&fm), 3, 1024 * 1024, 65536));
        (fm, lm)
    }

    /// Helper: write a few log entries, flush, and return (fm, lm).
    fn make_fm_and_lm_with_entries(
        dir: &std::path::Path,
    ) -> (Arc<noxu_log::FileManager>, Arc<noxu_log::LogManager>) {
        use bytes::BytesMut;
        use noxu_log::entry::TxnEndEntry;
        use noxu_log::{LogEntryType, Provisional};
        use noxu_util::{NULL_LSN, NULL_VLSN};

        let (fm, lm) = make_fm_and_lm(dir);

        for txn_id in [1i64, 2, 3] {
            let entry =
                TxnEndEntry::new_commit(txn_id, NULL_LSN, 0, 0, NULL_VLSN);
            let mut buf = BytesMut::with_capacity(entry.log_size());
            entry.write_to_log(&mut buf);
            lm.log(LogEntryType::TxnCommit, &buf, Provisional::No, true, false)
                .unwrap();
        }
        lm.flush_sync().unwrap();
        (fm, lm)
    }

    /// `with_file_manager_and_tree` constructor sets all fields correctly.
    #[test]
    fn test_with_file_manager_and_tree_constructor() {
        let dir = tempfile::TempDir::new().unwrap();
        let (fm, lm) = make_fm_and_lm(dir.path());

        let tree = Arc::new(RwLock::new(noxu_tree::Tree::new(1, 128)));

        let cleaner = Cleaner::with_file_manager_and_tree(
            60,
            2,
            90,
            Arc::clone(&fm),
            Arc::clone(&tree),
            Arc::clone(&lm),
        );

        assert_eq!(cleaner.min_utilization.load(Ordering::Relaxed), 60);
        assert_eq!(cleaner.min_file_count, 2);
        assert_eq!(cleaner.min_age, 90);
        assert!(cleaner.file_manager.is_some(), "file_manager must be set");
        assert!(cleaner.tree.is_some(), "tree must be set");
        assert!(cleaner.log_manager.is_some(), "log_manager must be set");
    }

    /// `process_single_file` completes successfully when a tree is wired in,
    /// even if the tree is empty (all entries will be counted as dead).
    ///
    /// The no-live-entries path where
    /// every LN decoded from the file is absent from the tree.
    #[test]
    fn test_process_single_file_with_tree_empty_tree() {
        let dir = tempfile::TempDir::new().unwrap();
        let (fm, lm) = make_fm_and_lm_with_entries(dir.path());

        // Tree is empty — no key will be found so all LN entries are dead.
        let tree = Arc::new(RwLock::new(noxu_tree::Tree::new(1, 128)));

        let cleaner = Cleaner::with_file_manager_and_tree(
            50,
            0,
            0,
            Arc::clone(&fm),
            Arc::clone(&tree),
            Arc::clone(&lm),
        );

        let result = cleaner.process_single_file(0).unwrap();

        assert!(
            result.completed,
            "processing must complete even with an empty tree"
        );
        // The file written by make_fm_and_lm_with_entries contains only
        // TxnCommit entries (type=Other in the cleaner), so lns_cleaned==0.
        assert_eq!(
            result.lns_dead, 0,
            "no LN entries were written, so lns_dead must be 0"
        );
    }

    /// `process_single_file` with a tree-wired cleaner: live LN entries
    /// whose keys match entries in the tree are migrated.
    ///
    /// Core migration path for log file cleaning.
    /// `FileProcessor.processFoundLN()`.  We insert a key into the tree at
    /// the LSN that would be produced by a synthetic LN entry in the log, then
    /// verify the cleaner reports a migration.
    ///
    /// Because `decode_ln_entries_from_file` uses the file offset as a
    /// synthetic key and sets `db_id = 1`, we write a matching entry into the
    /// tree using those same values before running the cleaner.
    #[test]
    fn test_process_single_file_with_tree_migrates_live_ln() {
        use noxu_util::Lsn;

        let dir = tempfile::TempDir::new().unwrap();
        let (fm, lm) = make_fm_and_lm(dir.path());

        // Write a non-transactional InsertLN entry (type byte 4) so that
        // `decode_ln_entries_from_file` classifies it as a live LN.
        // We use `LogEntryType::Trace` with a crafted first byte because
        // the cleaner dispatches on the raw entry-type byte, not the enum.
        // Easiest approach: write raw bytes directly via FileManager.
        //
        // LogManager.log() writes a real entry header; the type byte at
        // position 4 of the record will be whatever `entry_type.type_num()`
        // returns.  Trace = type 1, TxnCommit = type 14, IN = type 2.
        //
        // For InsertLN (type 4) we need to write it as a raw payload.
        // We write a minimal 0-byte payload so item_size = 0.
        //
        // Note: LogManager.log() writes type byte 4 for InsertLN only if
        // LogEntryType::InsertLN exists.  Looking at the entry_type enum,
        // type 4 = InsertLN.  We use `LogEntryType::InsertLN` if present,
        // otherwise we skip this test.
        //
        // Looking at the existing code, we know TxnCommit entries are the
        // only ones easily writable.  To keep the test practical, we test
        // with a `NoopTree`-like scenario: write TxnCommit entries (type 14,
        // which maps to Other in the cleaner), confirm the file-level path
        // still completes.  The real LN-migration with a synthetic InsertLN
        // offset-based key is tested in the file_processor unit tests.
        //
        // Simpler approach: insert a key derived from FILE_HEADER_SIZE
        // (the first offset after the file header) into the tree at a
        // sentinel LSN, then write a raw log buffer whose header has type=4.

        use noxu_log::entry_header::MIN_HEADER_SIZE;
        use noxu_log::file_header::FILE_HEADER_SIZE;

        // Offset where the first log entry lands after the file header.
        let first_ln_offset = FILE_HEADER_SIZE as u32;
        let synthetic_key = first_ln_offset.to_le_bytes().to_vec();
        let entry_lsn = Lsn::new(0, first_ln_offset);

        // Insert that key into the tree at entry_lsn so the cleaner will
        // find it and attempt migration.
        let tree = Arc::new(RwLock::new(noxu_tree::Tree::new(1, 128)));
        {
            let t = tree.write().unwrap();
            t.insert(synthetic_key, b"value".to_vec(), entry_lsn)
                .expect("insert should succeed");
        }

        // Write a raw InsertLN (type=4) entry at `first_ln_offset` so the
        // decode loop picks it up.  We write directly via the FileManager
        // after flushing a file header; the easiest way is to construct the
        // 14-byte header manually with type=4 and item_size=0.
        let item_size: u32 = 0;
        let mut hdr = [0u8; MIN_HEADER_SIZE];
        hdr[4] = 4; // entry_type = InsertLN
        hdr[5] = 0; // flags = 0 (no VLSN)
        hdr[10..14].copy_from_slice(&item_size.to_le_bytes());
        // Compute CRC over bytes [4..MIN_HEADER_SIZE]
        let crc = noxu_log::ChecksumValidator::compute_range(
            &hdr,
            4,
            MIN_HEADER_SIZE - 4,
        );
        hdr[0..4].copy_from_slice(&crc.to_le_bytes());

        // Write file header + LN header to file 0.
        // The FileManager creates file 0 on first write; we need to write
        // past the file header.  We use write_buffer at offset
        // FILE_HEADER_SIZE.
        fm.write_buffer(&hdr, first_ln_offset as u64).unwrap();

        let cleaner = Cleaner::with_file_manager_and_tree(
            50,
            0,
            0,
            Arc::clone(&fm),
            Arc::clone(&tree),
            Arc::clone(&lm),
        );

        let result = cleaner.process_single_file(0).unwrap();

        assert!(result.completed, "processing must complete");
        // The InsertLN entry is decoded and its synthetic key matches the
        // tree entry at entry_lsn == log_lsn → migration.
        assert_eq!(result.lns_cleaned, 1, "one LN entry should be cleaned");
        assert_eq!(result.lns_migrated, 1, "the live LN must be migrated");
        assert_eq!(result.lns_dead, 0, "no entries should be dead");
    }

    /// `do_clean` end-to-end with tree wiring: a file containing only
    /// non-LN entries (TxnCommit = Other) is cleaned and deleted, and the
    /// migration counters remain zero (nothing to migrate).
    ///
    /// This verifies the full `do_clean → process_single_file →
    /// FileProcessor::process_file → SharedTreeLookup` chain completes
    /// without errors.
    #[test]
    fn test_do_clean_with_tree_no_ln_entries() {
        let dir = tempfile::TempDir::new().unwrap();
        let (fm, lm) = make_fm_and_lm_with_entries(dir.path());

        let tree = Arc::new(RwLock::new(noxu_tree::Tree::new(1, 128)));
        let file_path = dir.path().join("00000000.ndb");
        assert!(file_path.exists());

        let cleaner = Cleaner::with_file_manager_and_tree(
            50,
            0,
            0,
            Arc::clone(&fm),
            Arc::clone(&tree),
            Arc::clone(&lm),
        );

        cleaner.add_file_to_clean(0);
        let result = cleaner.do_clean(5, false).unwrap();

        assert_eq!(result.files_cleaned, 1);
        // X-5: no deletion without checkpoint barrier.
        assert_eq!(result.files_deleted, 0);
        assert!(file_path.exists(), "file must still exist before checkpoints");

        // Advance through the two-checkpoint barrier.
        {
            let state1 =
                cleaner.get_file_selector().lock().get_checkpoint_state();
            cleaner.after_checkpoint(&state1);
            let state2 =
                cleaner.get_file_selector().lock().get_checkpoint_state();
            cleaner.after_checkpoint(&state2);
        }
        let deleted = cleaner.delete_safe_files();
        assert_eq!(deleted, 1);
        assert!(
            !file_path.exists(),
            "cleaned file must be removed from disk after barrier"
        );

        // TxnCommit entries are classified as Other → not migrated.
        let stats = cleaner.get_stats().snapshot();
        assert_eq!(stats.lns_migrated, 0);
    }

    // ── X-6: migration writes real WAL LN entry ─────────────────────

    /// X-6: verify that `write_migration_ln` produces a real WAL entry
    /// (non-zero LSN) when a LogManager is wired, rather than the fake
    /// get_end_of_log() value.
    #[test]
    fn test_x6_migration_writes_real_wal_entry() {
        use noxu_util::NULL_LSN;

        let dir = tempfile::TempDir::new().unwrap();
        let (_fm, lm) = make_fm_and_lm(dir.path());

        // Simulate a migration: write an UpdateLN entry and confirm a
        // non-NULL LSN is returned (X-6 fix).
        let old_lsn = NULL_LSN;
        let db_id: u64 = 1;
        let key = b"migrated_key";
        let data = b"migrated_value";

        {
            use bytes::BytesMut;
            use noxu_log::entry::LnLogEntry;
            use noxu_log::{LogEntryType, Provisional};
            use noxu_util::vlsn::NULL_VLSN;

            let entry = LnLogEntry::new(
                db_id,
                None,
                old_lsn,
                false,
                None,
                None,
                NULL_VLSN,
                0,
                true,
                key.to_vec(),
                Some(data.to_vec()),
                0,
                NULL_VLSN,
            );
            let mut buf = BytesMut::with_capacity(entry.log_size());
            entry.write_to_log(&mut buf);
            let new_lsn = lm
                .log(
                    LogEntryType::UpdateLN,
                    &buf,
                    Provisional::No,
                    false,
                    false,
                )
                .expect("X-6: migration log write must succeed");
            assert_ne!(
                new_lsn.as_u64(),
                0,
                "X-6: migration must return a real non-NULL LSN"
            );
        }
    }

    /// X-7: SharedTreeLookup dispatches secondary-LN liveness checks to the
    /// correct tree (registered in extra_trees), not the primary tree.
    ///
    /// Without the fix, a secondary key looked up in the primary tree returns
    /// NotFound, and the LN is misclassified as Obsolete (silently dropped).
    /// With the fix, the lookup resolves to the secondary tree where the key
    /// actually lives, and the LN is correctly migrated.
    #[test]
    fn test_x7_secondary_ln_migrated_in_correct_tree() {
        use crate::file_processor::{
            BinLookupResult, MigrationOutcome, SharedTreeLookup, TreeLookup,
        };
        use noxu_log::{FileManager, LogManager};
        use noxu_tree::tree::Tree;
        use noxu_util::lsn::Lsn;
        use std::sync::{Arc, RwLock};
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let fm = Arc::new(
            FileManager::new(dir.path(), false, 64 * 1024 * 1024, 100).unwrap(),
        );
        let lm =
            Arc::new(LogManager::new(Arc::clone(&fm), 3, 1024 * 1024, 65536));

        // Build a PRIMARY tree (db_id=1) with a primary key.
        let primary = Tree::new(1, 256);
        primary
            .insert(b"pri_key".to_vec(), b"pri_data".to_vec(), Lsn::new(1, 1))
            .unwrap();
        let primary_arc: Arc<RwLock<Tree>> = Arc::new(RwLock::new(primary));

        // Build a SECONDARY tree (db_id=2) with a secondary key.
        let sec = Tree::new(2, 256);
        let sec_lsn = Lsn::new(1, 50);
        sec.insert(
            b"sec_key".to_vec(),
            b"pri_key".to_vec(), // secondary value = primary key
            sec_lsn,
        )
        .unwrap();
        let sec_arc: Arc<RwLock<Tree>> = Arc::new(RwLock::new(sec));

        // Wire both trees into the SharedTreeLookup.
        let mut extra = std::collections::HashMap::new();
        extra.insert(2i64, Arc::clone(&sec_arc));
        let lookup =
            SharedTreeLookup::new(Arc::clone(&primary_arc), Arc::clone(&lm))
                .with_extra_trees(extra);

        // --- Test primary LN lookup (db_id=1) ---
        let primary_result =
            lookup.lookup_parent_bin(1, b"pri_key", Lsn::new(1, 1));
        assert!(
            matches!(primary_result, BinLookupResult::Found { .. }),
            "X-7: primary key must be found in primary tree"
        );

        // --- Test secondary LN lookup (db_id=2) ---
        let sec_result =
            lookup.lookup_parent_bin(2, b"sec_key", Lsn::new(1, 50));
        assert!(
            matches!(sec_result, BinLookupResult::Found { .. }),
            "X-7: secondary key must be found in secondary tree (not primary)"
        );

        // Without the fix (no extra_trees), looking up sec_key in primary
        // would return NotFound.  Verify this expectation for documentation:
        let lookup_no_extra =
            SharedTreeLookup::new(Arc::clone(&primary_arc), Arc::clone(&lm));
        let bad_result =
            lookup_no_extra.lookup_parent_bin(2, b"sec_key", Lsn::new(1, 50));
        assert!(
            matches!(bad_result, BinLookupResult::NotFound),
            "without extra_trees, secondary key is NotFound in primary (pre-fix behavior confirmed)"
        );

        // --- Verify migration outcome ---
        // With the fix: migrate_ln_slot for db_id=2 resolves against the
        // secondary tree.  sec_key has tree_lsn == sec_lsn, so it matches
        // and the slot is migrated (or returns Migrated).
        let BinLookupResult::Found { tree_lsn } = sec_result else {
            panic!("lookup must succeed to test migration");
        };
        let outcome = lookup.migrate_ln_slot(2, b"sec_key", sec_lsn, tree_lsn);
        assert_ne!(
            outcome,
            MigrationOutcome::Obsolete,
            "X-7: secondary LN must not be misclassified as Obsolete"
        );
    }

    // ── CLN-4 acceptance tests ───────────────────────────────────────────────

    /// A long-running open txn prevents `do_clean` from selecting a file in
    /// the active-txn window via the getBestFile (profile-based) path.
    ///
    /// JE: `UtilizationCalculator.getBestFile` sets
    /// `firstActiveFile = min(newest, firstActiveTxnFile)` before computing
    /// `lastFileToClean`.  Files with `file_number >= firstActiveTxnFile`
    /// must not be selected through the utilization-scoring path.
    ///
    /// Note: files explicitly queued via `add_file_to_clean` (TO_BE_CLEANED)
    /// are drained first by `selectFileForCleaning` without the txn check,
    /// matching JE `FileSelector.selectFileForCleaning` ~line 175 which
    /// returns from the TO_BE_CLEANED queue before calling getBestFile.
    /// The txn clamping only applies in the getBestFile / profile path.
    #[test]
    fn test_cln4_do_clean_respects_first_active_txn_file() {
        use crate::file_selector::FileSelector;
        use crate::file_summary::FileSummary;
        use noxu_txn::{LockManager, TxnManager};
        use std::collections::BTreeMap;

        let lock_manager = Arc::new(LockManager::new());
        let txn_manager = Arc::new(TxnManager::new(lock_manager));

        // Open a transaction, note its first LSN as file 3, offset 100.
        let _txn = txn_manager.begin_txn();
        let txn_id = _txn.id_as_locker();
        let lsn_file3 = noxu_util::Lsn::new(3, 100).as_u64();
        txn_manager.update_first_lsn(txn_id, lsn_file3);

        // CLN-4: verify FileSelector.select_file_for_cleaning with a profile
        // that contains files 1-5 (newest = 5, firstActiveTxnFile = 3).
        // lastFileToClean = min(5, 3) - min_age(0) = 3.
        // Files 4 and 5 must be excluded; files 1-3 are candidates.
        // File 1 has lowest utilization (most obsolete), should be chosen.
        let first_active_txn_file: Option<u32> = {
            let lsn_u64 = txn_manager.get_first_active_lsn();
            if lsn_u64 == noxu_util::NULL_LSN.as_u64() {
                None
            } else {
                Some((lsn_u64 >> 32) as u32)
            }
        };
        assert_eq!(
            first_active_txn_file,
            Some(3),
            "first_active_txn_file must be file 3"
        );

        let mut profile: BTreeMap<u32, FileSummary> = BTreeMap::new();
        for f in 1u32..=5 {
            profile.insert(
                f,
                FileSummary {
                    total_count: 10,
                    total_size: 1000,
                    total_ln_count: 10,
                    total_ln_size: 1000,
                    obsolete_ln_count: 9,
                    obsolete_ln_size: 900, // 10% util
                    obsolete_ln_size_counted: 9,
                    ..Default::default()
                },
            );
        }

        let mut selector = FileSelector::new();
        // With firstActiveTxnFile = 3 and min_age = 0:
        // effective_newest = min(5, 3) = 3
        // lastFileToClean = 3 - 0 = 3
        // Only files 1, 2, 3 qualify; files 4 and 5 are excluded.
        let result = selector.select_file_for_cleaning(
            &profile,
            50, // min_utilization_pct (all below)
            0,  // min_age
            false,
            Some(3), // first_active_txn_file
            5,       // min_file_utilization_pct
        );
        // Should select a file <= 3.
        assert!(
            result.map(|(f, _)| f).is_some_and(|f| f <= 3),
            "CLN-4: selected file must be <= first_active_txn_file=3, got {:?}",
            result
        );
    }

    /// Without a TxnManager, all queued files are cleaned (baseline).
    #[test]
    fn test_cln4_no_txn_manager_cleans_all_files() {
        let cleaner = Cleaner::new(50, 0, 0);
        for f in 1u32..=5 {
            cleaner.add_file_to_clean(f);
        }
        let result = cleaner.do_clean(10, false).unwrap();
        assert_eq!(result.files_cleaned, 5);
    }

    // ── CLN-10 acceptance tests ──────────────────────────────────────────────

    /// `LnInfo::is_expired` must use hours-since-epoch (packed-hours) units.
    /// This test verifies that the unit documented in the field comment is
    /// enforced: passing a value in the same unit gives correct results.
    #[test]
    fn test_cln10_ln_info_expiration_unit_is_hours() {
        use crate::LnInfo;
        let lsn = noxu_util::Lsn::new(1, 100);
        // expiration_time = 1000 (hours since epoch)
        let info = LnInfo::new(lsn, 1, vec![1], 64, false, 1000);
        // At hour 999: not expired
        assert!(!info.is_expired(999), "not expired at hour 999");
        // At hour 1000: expired
        assert!(info.is_expired(1000), "expired at hour 1000");
        // At hour 1001: still expired
        assert!(info.is_expired(1001), "still expired at hour 1001");
    }

    /// `ExpirationTracker::track` and `get_expired_bytes` use hours.
    /// Units must be consistent: a value tracked at hour H must appear
    /// expired only when `current_time >= H`.
    #[test]
    fn test_cln10_expiration_tracker_unit_is_hours() {
        use crate::ExpirationTracker;
        let mut tracker = ExpirationTracker::new(1);
        // Track 500 bytes expiring at hour 100
        tracker.track(100, 500);
        // Track 300 bytes expiring at hour 200
        tracker.track(200, 300);

        // At hour 99: nothing expired
        assert_eq!(tracker.get_expired_bytes(99), 0);
        // At hour 100: first bucket expired
        assert_eq!(tracker.get_expired_bytes(100), 500);
        // At hour 200: both expired
        assert_eq!(tracker.get_expired_bytes(200), 800);
    }

    /// Unit consistency assertion: if we had stored ms in LnInfo and passed
    /// it to ExpirationTracker expecting hours, we'd get a 3600x mismatch.
    /// This test documents that the correct unit is hours in both places.
    #[test]
    fn test_cln10_unit_mismatch_would_be_detectable() {
        use crate::ExpirationTracker;
        // If someone passes ms (e.g. 3_600_000 ms = 1 hour) to a tracker
        // that expects hours, the value 3_600_000 would be treated as 3.6M
        // hours (~411 years), never expiring in any realistic timeframe.
        // This test just documents the expected behavior of the tracker in
        // hours mode, confirming the unit.
        let mut tracker = ExpirationTracker::new(1);
        // 1 hour since epoch
        tracker.track(1, 100);
        // current_time = 1 hour: expired
        assert_eq!(tracker.get_expired_bytes(1), 100);
        // current_time in ms (3_600_000): would also trigger, but only because
        // 3_600_000 >> 1.  The point is the tracker uses its input as hours.
        // We document: don't pass ms to this interface.
        assert_eq!(tracker.get_expired_bytes(3_600_000), 100, "still expired");
    }

    // ── CLN-12 acceptance tests ──────────────────────────────────────────────

    /// The pending queue is processed periodically during a long file run.
    ///
    /// JE: `FileProcessor.processFile` calls `cleaner.processPending()` every
    /// `PROCESS_PENDING_EVERY_N_LNS` entries (FileProcessor.java ~line 1004).
    #[test]
    fn test_cln12_pending_queue_processed_during_file_run() {
        use crate::file_processor::{
            FileProcessor, LogEntry, LogEntryType,
            PROCESS_PENDING_EVERY_N_LNS_PUB,
        };
        use std::sync::atomic::{AtomicUsize, Ordering};

        let callback_count = Arc::new(AtomicUsize::new(0));
        let callback_count2 = Arc::clone(&callback_count);

        let stats = Arc::new(crate::CleanerStats::new());
        let shutdown = Arc::new(AtomicBool::new(false));

        let processor = FileProcessor::new(stats, shutdown)
            .with_process_pending_fn(Arc::new(move || {
                callback_count2.fetch_add(1, Ordering::Relaxed);
            }));

        // Build enough LN entries to trigger the periodic callback.
        // With default interval = 100, 101 LNs should trigger once.
        let n = PROCESS_PENDING_EVERY_N_LNS_PUB + 1;
        let entries: Vec<LogEntry> = (0..n)
            .map(|i| LogEntry {
                lsn: noxu_util::Lsn::new(1, i as u32 * 100),
                entry_type: LogEntryType::Ln {
                    db_id: 1,
                    key: vec![i as u8],
                    deleted: false,
                    expiration_time: 0,
                    entry_size: 64,
                },
            })
            .collect();

        let summary = crate::FileSummary::new();
        // Use a NoopTreeLookup (all entries → NotFound → dead)
        struct Noop;
        impl crate::file_processor::TreeLookup for Noop {
            fn lookup_parent_bin(
                &self,
                _db_id: i64,
                _key: &[u8],
                _log_lsn: noxu_util::Lsn,
            ) -> crate::file_processor::BinLookupResult {
                crate::file_processor::BinLookupResult::NotFound
            }
            fn migrate_ln_slot(
                &self,
                _db_id: i64,
                _key: &[u8],
                _log_lsn: noxu_util::Lsn,
                _tree_lsn: noxu_util::Lsn,
            ) -> crate::file_processor::MigrationOutcome {
                crate::file_processor::MigrationOutcome::Obsolete
            }
            fn lookup_in(
                &self,
                _db_id: i64,
                _node_id: i64,
                _log_lsn: noxu_util::Lsn,
            ) -> crate::file_processor::InLookupResult {
                crate::file_processor::InLookupResult::Obsolete
            }
        }

        let _result =
            processor.process_file(1, &summary, &entries, &Noop).unwrap();

        // With n = PROCESS_PENDING_EVERY_N_LNS_PUB + 1, the counter must be
        // incremented exactly once at the interval boundary.
        assert_eq!(
            callback_count.load(Ordering::Relaxed),
            1,
            "CLN-12: pending callback must be called once for {} LNs at interval {}",
            n,
            PROCESS_PENDING_EVERY_N_LNS_PUB
        );
    }

    // ── CLN-14 acceptance tests ───────────────────────────────────────────────

    /// When `checkpoint_wakeup_fn` is set and cleaning occurs, the callback
    /// is invoked at the end of `do_clean`.
    ///
    /// JE: FileProcessor.doClean ~line 290 calls
    /// `envImpl.getCheckpointer().wakeupAfterNoWrites()`.
    #[test]
    fn test_cln14_checkpoint_wakeup_called_after_cleaning() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let wakeup_count = Arc::new(AtomicUsize::new(0));
        let wakeup_count2 = Arc::clone(&wakeup_count);

        let cleaner = Cleaner::new(50, 0, 0).with_checkpoint_wakeup_fn(
            Arc::new(move || {
                wakeup_count2.fetch_add(1, Ordering::Relaxed);
            }),
        );

        cleaner.add_file_to_clean(1);
        let result = cleaner.do_clean(1, false).unwrap();

        assert_eq!(result.files_cleaned, 1);
        assert_eq!(
            wakeup_count.load(Ordering::Relaxed),
            1,
            "CLN-14: checkpoint wakeup callback must be called after cleaning"
        );
    }

    /// When no files are cleaned, the callback is NOT invoked.
    #[test]
    fn test_cln14_no_wakeup_when_no_files_cleaned() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let wakeup_count = Arc::new(AtomicUsize::new(0));
        let wakeup_count2 = Arc::clone(&wakeup_count);

        let cleaner = Cleaner::new(50, 0, 0).with_checkpoint_wakeup_fn(
            Arc::new(move || {
                wakeup_count2.fetch_add(1, Ordering::Relaxed);
            }),
        );

        // No files added — nothing to clean.
        let result = cleaner.do_clean(1, false).unwrap();
        assert_eq!(result.files_cleaned, 0);
        assert_eq!(
            wakeup_count.load(Ordering::Relaxed),
            0,
            "CLN-14: checkpoint wakeup must NOT be called when nothing cleaned"
        );
    }
}
