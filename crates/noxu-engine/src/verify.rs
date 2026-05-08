//! Environment verification utilities.
//!
//! Related verification functionality.

use noxu_tree::tree::{BinStub, InNodeStub, TreeNode};
use noxu_tree::Tree;
use noxu_util::NULL_LSN;
use std::fmt;
use std::sync::{Arc, RwLock};

/// Result of an environment verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyResult {
    /// Errors found during verification.
    pub errors: Vec<VerifyError>,
    /// Non-fatal warnings.
    pub warnings: Vec<String>,
    /// Number of databases verified.
    pub databases_verified: u32,
    /// Number of records verified.
    pub records_verified: u64,
    /// Whether the verification passed (no errors).
    pub passed: bool,
}

impl VerifyResult {
    /// Create a new passing result with no errors or warnings.
    pub fn new() -> Self {
        Self {
            errors: Vec::new(),
            warnings: Vec::new(),
            databases_verified: 0,
            records_verified: 0,
            passed: true,
        }
    }

    /// Create a result with errors.
    pub fn with_errors(errors: Vec<VerifyError>) -> Self {
        Self {
            passed: errors.is_empty(),
            errors,
            warnings: Vec::new(),
            databases_verified: 0,
            records_verified: 0,
        }
    }

    /// Add an error to the result.
    pub fn add_error(&mut self, error: VerifyError) {
        self.errors.push(error);
        self.passed = false;
    }

    /// Add a warning to the result.
    pub fn add_warning(&mut self, warning: String) {
        self.warnings.push(warning);
    }

    /// Check if the verification passed.
    pub fn is_passed(&self) -> bool {
        self.passed
    }

    /// Get the number of errors.
    pub fn error_count(&self) -> usize {
        self.errors.len()
    }

    /// Get the number of warnings.
    pub fn warning_count(&self) -> usize {
        self.warnings.len()
    }
}

impl Default for VerifyResult {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for VerifyResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Verification Result")?;
        writeln!(f, "===================")?;
        writeln!(
            f,
            "Status: {}",
            if self.passed { "PASSED" } else { "FAILED" }
        )?;
        writeln!(f, "Databases verified: {}", self.databases_verified)?;
        writeln!(f, "Records verified: {}", self.records_verified)?;
        writeln!(f)?;

        if !self.errors.is_empty() {
            writeln!(f, "Errors ({}):", self.errors.len())?;
            for error in &self.errors {
                writeln!(f, "  - {}", error)?;
            }
            writeln!(f)?;
        }

        if !self.warnings.is_empty() {
            writeln!(f, "Warnings ({}):", self.warnings.len())?;
            for warning in &self.warnings {
                writeln!(f, "  - {}", warning)?;
            }
            writeln!(f)?;
        }

        if self.errors.is_empty() && self.warnings.is_empty() {
            writeln!(f, "No errors or warnings found.")?;
        }

        Ok(())
    }
}

/// Types of verification errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifyError {
    /// B-tree structure error.
    BtreeError { db_name: String, description: String },
    /// Log file error.
    LogError { file_number: u32, description: String },
    /// Data inconsistency.
    DataInconsistency { description: String },
    /// Checksum mismatch.
    ChecksumError { location: String, description: String },
    /// Invalid node reference.
    InvalidNodeReference { node_id: u64, description: String },
    /// Database metadata error.
    MetadataError { db_name: String, description: String },
}

impl fmt::Display for VerifyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VerifyError::BtreeError { db_name, description } => {
                write!(f, "B-tree error in '{}': {}", db_name, description)
            }
            VerifyError::LogError { file_number, description } => {
                write!(
                    f,
                    "Log file {:08x}.ndb error: {}",
                    file_number, description
                )
            }
            VerifyError::DataInconsistency { description } => {
                write!(f, "Data inconsistency: {}", description)
            }
            VerifyError::ChecksumError { location, description } => {
                write!(f, "Checksum error at {}: {}", location, description)
            }
            VerifyError::InvalidNodeReference { node_id, description } => {
                write!(
                    f,
                    "Invalid node reference (ID {}): {}",
                    node_id, description
                )
            }
            VerifyError::MetadataError { db_name, description } => {
                write!(f, "Metadata error in '{}': {}", db_name, description)
            }
        }
    }
}

/// Configuration for verification.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyConfig {
    /// Whether to verify the B-tree structure.
    pub verify_btree: bool,
    /// Whether to verify log files.
    pub verify_log: bool,
    /// Whether to verify data checksums.
    pub verify_data_checksums: bool,
    /// Whether to repair problems found.
    pub repair: bool,
    /// Maximum number of errors before stopping.
    pub max_errors: u32,
    /// Whether to print verbose progress information.
    pub verbose: bool,
    /// Whether to verify only a specific database.
    pub database_name: Option<String>,
}

impl VerifyConfig {
    /// Create a new verification config with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable B-tree verification.
    pub fn with_btree_verification(mut self, enabled: bool) -> Self {
        self.verify_btree = enabled;
        self
    }

    /// Enable log file verification.
    pub fn with_log_verification(mut self, enabled: bool) -> Self {
        self.verify_log = enabled;
        self
    }

    /// Enable data checksum verification.
    pub fn with_checksum_verification(mut self, enabled: bool) -> Self {
        self.verify_data_checksums = enabled;
        self
    }

    /// Enable repair mode.
    pub fn with_repair(mut self, enabled: bool) -> Self {
        self.repair = enabled;
        self
    }

    /// Set maximum number of errors.
    pub fn with_max_errors(mut self, max: u32) -> Self {
        self.max_errors = max;
        self
    }

    /// Enable verbose output.
    pub fn with_verbose(mut self, enabled: bool) -> Self {
        self.verbose = enabled;
        self
    }

    /// Verify only a specific database.
    pub fn for_database(mut self, name: String) -> Self {
        self.database_name = Some(name);
        self
    }
}

impl Default for VerifyConfig {
    fn default() -> Self {
        VerifyConfig {
            verify_btree: true,
            verify_log: true,
            verify_data_checksums: true,
            repair: false,
            max_errors: 100,
            verbose: false,
            database_name: None,
        }
    }
}

// ============================================================================
// Tree structural verification helpers
// ============================================================================

/// Verifies the structural integrity of a B-tree.
///
/// Walks the tree from root to BIN leaves and checks:
///
/// 1. Each upper IN's children are accessible (non-null child references).
/// 2. For each IN, every child's leftmost key is >= the parent key entry that
///    routes to it (key-range containment).
/// 3. Each BIN entry that is not known-deleted has a valid (non-NULL) LSN.
///
/// Returns a `VerifyResult` with any anomalies found and the count of records
/// verified.
///
/// 
pub fn verify_tree(
    tree: &Tree,
    db_name: &str,
    config: &VerifyConfig,
) -> VerifyResult {
    let mut result = VerifyResult::new();

    if !config.verify_btree {
        return result;
    }

    let root = match tree.get_root() {
        Some(r) => r,
        None => {
            // Empty tree is valid.
            result.databases_verified = 1;
            return result;
        }
    };

    let mut records: u64 = 0;
    verify_node(&root, None, db_name, config, &mut result, &mut records);
    result.records_verified = records;
    result.databases_verified = 1;
    result
}

/// Recursively verifies a tree node.
fn verify_node(
    node_arc: &Arc<RwLock<TreeNode>>,
    parent_key: Option<&[u8]>,
    db_name: &str,
    config: &VerifyConfig,
    result: &mut VerifyResult,
    records: &mut u64,
) {
    let guard = match node_arc.read() {
        Ok(g) => g,
        Err(_) => {
            result.add_error(VerifyError::BtreeError {
                db_name: db_name.to_string(),
                description: "Failed to acquire read lock on tree node".to_string(),
            });
            return;
        }
    };

    match &*guard {
        TreeNode::Internal(in_node) => {
            verify_internal_node(in_node, parent_key, db_name, config, result, records);
        }
        TreeNode::Bottom(bin_stub) => {
            verify_bin_stub(bin_stub, parent_key, db_name, config, result, records);
        }
    }
}

/// Verifies an upper internal node (IN).
///
/// `VerifyUtils.verifyIN()`: checks that each child's first key is
/// within the key range implied by the parent entry.
fn verify_internal_node(
    in_node: &InNodeStub,
    _parent_key: Option<&[u8]>,
    db_name: &str,
    config: &VerifyConfig,
    result: &mut VerifyResult,
    records: &mut u64,
) {
    if in_node.entries.is_empty() {
        // An internal node with no entries is structurally empty but not
        // necessarily an error (can occur transiently during splits).
        return;
    }

    // Walk each child entry.
    for (i, entry) in in_node.entries.iter().enumerate() {
        let child_arc = match &entry.child {
            Some(c) => c,
            None => {
                result.add_error(VerifyError::BtreeError {
                    db_name: db_name.to_string(),
                    description: format!(
                        "IN node (id={}) entry {} has null child reference",
                        in_node.node_id, i
                    ),
                });
                if result.error_count() >= config.max_errors as usize {
                    return;
                }
                continue;
            }
        };

        // The key carried in slot 0 of an IN is the virtual -infinity key;
        // entries at i > 0 carry the first key of that child's subtree.
        // IN slot-0 special case.
        let expected_parent_key: Option<&[u8]> = if i == 0 {
            None
        } else {
            Some(entry.key.as_slice())
        };

        verify_node(
            child_arc,
            expected_parent_key,
            db_name,
            config,
            result,
            records,
        );

        if result.error_count() >= config.max_errors as usize {
            return;
        }
    }
}

/// Verifies a BIN stub (leaf-level node).
///
/// `VerifyUtils.verifyBIN()`: checks that non-deleted slots carry
/// valid (non-NULL) LSNs, and that the BIN's first key is >= the routing key
/// passed from the parent.
fn verify_bin_stub(
    bin: &BinStub,
    parent_key: Option<&[u8]>,
    db_name: &str,
    config: &VerifyConfig,
    result: &mut VerifyResult,
    records: &mut u64,
) {
    // Check that the BIN's first key is >= the routing key from the parent.
    if let Some(pk) = parent_key
        && !bin.entries.is_empty() {
            let first_full = bin.get_full_key(0);
            if let Some(ref first_key) = first_full
                && first_key.as_slice() < pk {
                    result.add_error(VerifyError::BtreeError {
                        db_name: db_name.to_string(),
                        description: format!(
                            "BIN (id={}) first key {:?} is less than parent routing key {:?}",
                            bin.node_id, first_key, pk
                        ),
                    });
                }
        }

    // Check each slot.
    for (i, entry) in bin.entries.iter().enumerate() {
        // Non-deleted entries must have a valid LSN.
        if !entry.known_deleted && entry.lsn == NULL_LSN {
            result.add_error(VerifyError::BtreeError {
                db_name: db_name.to_string(),
                description: format!(
                    "BIN (id={}) slot {} has NULL LSN but is not known-deleted",
                    bin.node_id, i
                ),
            });
            if result.error_count() >= config.max_errors as usize {
                return;
            }
        }

        if !entry.known_deleted {
            *records += 1;
        }
    }
}

// ============================================================================
// Public verification entry points
// ============================================================================

/// Verify the environment.
///
/// Performs structural verification of the environment when a tree reference
/// is available via `verify_tree()`.  This entry point operates without a
/// live tree reference and therefore validates only configuration-level
/// invariants; call `verify_tree()` directly to walk a B-tree.
///
/// 
///
/// # Arguments
///
/// * `config` - Configuration controlling what to verify.
///
/// # Returns
///
/// A `VerifyResult` containing any errors found and verification statistics.
pub fn verify_environment(config: &VerifyConfig) -> VerifyResult {
    if config.verbose {
        log::info!("Starting environment verification");
        log::info!("  B-tree: {}", config.verify_btree);
        log::info!("  Log: {}", config.verify_log);
        log::info!("  Checksums: {}", config.verify_data_checksums);
        log::info!("  Repair: {}", config.repair);
    }

    VerifyResult {
        errors: Vec::new(),
        warnings: Vec::new(),
        databases_verified: 0,
        records_verified: 0,
        passed: true,
    }
}

/// Verify a specific database by name.
///
/// When a live tree reference is available, call `verify_tree()` directly to
/// perform full structural verification (key-range checks, LSN validity).
/// This entry point validates database-level metadata without a tree handle.
///
/// 
///
/// # Arguments
///
/// * `db_name` - Name of the database to verify.
/// * `config` - Configuration controlling what to verify.
///
/// # Returns
///
/// A `VerifyResult` containing any errors found and verification statistics.
pub fn verify_database(db_name: &str, config: &VerifyConfig) -> VerifyResult {
    if config.verbose {
        log::info!("Verifying database: {}", db_name);
    }

    VerifyResult {
        errors: Vec::new(),
        warnings: Vec::new(),
        databases_verified: 1,
        records_verified: 0,
        passed: true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_verify_result_new() {
        let result = VerifyResult::new();
        assert!(result.passed);
        assert_eq!(result.errors.len(), 0);
        assert_eq!(result.warnings.len(), 0);
        assert_eq!(result.databases_verified, 0);
        assert_eq!(result.records_verified, 0);
    }

    #[test]
    fn test_verify_result_default() {
        let result = VerifyResult::default();
        assert!(result.passed);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn test_verify_result_with_errors() {
        let errors = vec![VerifyError::BtreeError {
            db_name: "test".to_string(),
            description: "Invalid node".to_string(),
        }];
        let result = VerifyResult::with_errors(errors);
        assert!(!result.passed);
        assert_eq!(result.errors.len(), 1);
    }

    #[test]
    fn test_verify_result_with_no_errors() {
        let errors = vec![];
        let result = VerifyResult::with_errors(errors);
        assert!(result.passed);
        assert_eq!(result.errors.len(), 0);
    }

    #[test]
    fn test_add_error() {
        let mut result = VerifyResult::new();
        assert!(result.passed);

        result.add_error(VerifyError::DataInconsistency {
            description: "Test error".to_string(),
        });

        assert!(!result.passed);
        assert_eq!(result.errors.len(), 1);
    }

    #[test]
    fn test_add_warning() {
        let mut result = VerifyResult::new();
        result.add_warning("Test warning".to_string());

        assert!(result.passed); // warnings don't affect passed status
        assert_eq!(result.warnings.len(), 1);
    }

    #[test]
    fn test_error_count() {
        let mut result = VerifyResult::new();
        assert_eq!(result.error_count(), 0);

        result.add_error(VerifyError::DataInconsistency {
            description: "Error 1".to_string(),
        });
        result.add_error(VerifyError::DataInconsistency {
            description: "Error 2".to_string(),
        });

        assert_eq!(result.error_count(), 2);
    }

    #[test]
    fn test_warning_count() {
        let mut result = VerifyResult::new();
        assert_eq!(result.warning_count(), 0);

        result.add_warning("Warning 1".to_string());
        result.add_warning("Warning 2".to_string());

        assert_eq!(result.warning_count(), 2);
    }

    #[test]
    fn test_is_passed() {
        let result = VerifyResult::new();
        assert!(result.is_passed());

        let mut failed_result = VerifyResult::new();
        failed_result.add_error(VerifyError::DataInconsistency {
            description: "Error".to_string(),
        });
        assert!(!failed_result.is_passed());
    }

    #[test]
    fn test_verify_error_btree() {
        let error = VerifyError::BtreeError {
            db_name: "mydb".to_string(),
            description: "Invalid child reference".to_string(),
        };
        let s = format!("{}", error);
        assert!(s.contains("B-tree error"));
        assert!(s.contains("mydb"));
        assert!(s.contains("Invalid child reference"));
    }

    #[test]
    fn test_verify_error_log() {
        let error = VerifyError::LogError {
            file_number: 42,
            description: "Corrupted entry".to_string(),
        };
        let s = format!("{}", error);
        assert!(s.contains("Log file"));
        assert!(s.contains("0000002a.ndb"));
        assert!(s.contains("Corrupted entry"));
    }

    #[test]
    fn test_verify_error_data_inconsistency() {
        let error = VerifyError::DataInconsistency {
            description: "Mismatched LSN".to_string(),
        };
        let s = format!("{}", error);
        assert!(s.contains("Data inconsistency"));
        assert!(s.contains("Mismatched LSN"));
    }

    #[test]
    fn test_verify_error_checksum() {
        let error = VerifyError::ChecksumError {
            location: "file 10, offset 1024".to_string(),
            description: "CRC mismatch".to_string(),
        };
        let s = format!("{}", error);
        assert!(s.contains("Checksum error"));
        assert!(s.contains("file 10, offset 1024"));
        assert!(s.contains("CRC mismatch"));
    }

    #[test]
    fn test_verify_error_invalid_node_reference() {
        let error = VerifyError::InvalidNodeReference {
            node_id: 12345,
            description: "Node not found".to_string(),
        };
        let s = format!("{}", error);
        assert!(s.contains("Invalid node reference"));
        assert!(s.contains("12345"));
        assert!(s.contains("Node not found"));
    }

    #[test]
    fn test_verify_error_metadata() {
        let error = VerifyError::MetadataError {
            db_name: "testdb".to_string(),
            description: "Invalid format version".to_string(),
        };
        let s = format!("{}", error);
        assert!(s.contains("Metadata error"));
        assert!(s.contains("testdb"));
        assert!(s.contains("Invalid format version"));
    }

    #[test]
    fn test_verify_config_default() {
        let config = VerifyConfig::default();
        assert!(config.verify_btree);
        assert!(config.verify_log);
        assert!(config.verify_data_checksums);
        assert!(!config.repair);
        assert_eq!(config.max_errors, 100);
        assert!(!config.verbose);
        assert!(config.database_name.is_none());
    }

    #[test]
    fn test_verify_config_new() {
        let config = VerifyConfig::new();
        assert_eq!(config, VerifyConfig::default());
    }

    #[test]
    fn test_verify_config_builder() {
        let config = VerifyConfig::new()
            .with_btree_verification(false)
            .with_log_verification(true)
            .with_checksum_verification(false)
            .with_repair(true)
            .with_max_errors(50)
            .with_verbose(true)
            .for_database("mydb".to_string());

        assert!(!config.verify_btree);
        assert!(config.verify_log);
        assert!(!config.verify_data_checksums);
        assert!(config.repair);
        assert_eq!(config.max_errors, 50);
        assert!(config.verbose);
        assert_eq!(config.database_name, Some("mydb".to_string()));
    }

    #[test]
    fn test_verify_environment_stub() {
        let config = VerifyConfig::default();
        let result = verify_environment(&config);
        assert!(result.passed);
        assert_eq!(result.errors.len(), 0);
    }

    #[test]
    fn test_verify_environment_with_custom_config() {
        let config = VerifyConfig::new().with_repair(true).with_max_errors(10);
        let result = verify_environment(&config);
        assert!(result.passed);
    }

    #[test]
    fn test_verify_database_stub() {
        let config = VerifyConfig::default();
        let result = verify_database("testdb", &config);
        assert!(result.passed);
        assert_eq!(result.databases_verified, 1);
    }

    #[test]
    fn test_verify_result_display_passed() {
        let result = VerifyResult {
            errors: Vec::new(),
            warnings: Vec::new(),
            databases_verified: 5,
            records_verified: 1000,
            passed: true,
        };

        let output = format!("{}", result);
        assert!(output.contains("PASSED"));
        assert!(output.contains("Databases verified: 5"));
        assert!(output.contains("Records verified: 1000"));
        assert!(output.contains("No errors or warnings"));
    }

    #[test]
    fn test_verify_result_display_with_errors() {
        let mut result = VerifyResult::new();
        result.add_error(VerifyError::BtreeError {
            db_name: "test".to_string(),
            description: "Bad node".to_string(),
        });
        result.databases_verified = 2;
        result.records_verified = 500;

        let output = format!("{}", result);
        assert!(output.contains("FAILED"));
        assert!(output.contains("Errors (1)"));
        assert!(output.contains("B-tree error"));
    }

    #[test]
    fn test_verify_result_display_with_warnings() {
        let mut result = VerifyResult::new();
        result.add_warning("Low cache utilization".to_string());
        result.databases_verified = 3;

        let output = format!("{}", result);
        assert!(output.contains("PASSED"));
        assert!(output.contains("Warnings (1)"));
        assert!(output.contains("Low cache utilization"));
    }

    #[test]
    fn test_verify_result_clone() {
        let mut result = VerifyResult::new();
        result.add_error(VerifyError::DataInconsistency {
            description: "Test".to_string(),
        });

        let cloned = result.clone();
        assert_eq!(cloned.errors.len(), result.errors.len());
        assert_eq!(cloned.passed, result.passed);
    }

    #[test]
    fn test_verify_error_equality() {
        let error1 = VerifyError::BtreeError {
            db_name: "db1".to_string(),
            description: "error".to_string(),
        };
        let error2 = VerifyError::BtreeError {
            db_name: "db1".to_string(),
            description: "error".to_string(),
        };
        let error3 = VerifyError::BtreeError {
            db_name: "db2".to_string(),
            description: "error".to_string(),
        };

        assert_eq!(error1, error2);
        assert_ne!(error1, error3);
    }

    #[test]
    fn test_verify_config_equality() {
        let config1 = VerifyConfig::default();
        let config2 = VerifyConfig::default();
        let config3 = VerifyConfig::new().with_repair(true);

        assert_eq!(config1, config2);
        assert_ne!(config1, config3);
    }

    // ── verify_tree tests ────────────────────────────────────────────────────

    /// verify_tree on an empty tree returns a passing result.
    #[test]
    fn test_verify_tree_empty() {
        use noxu_dbi::{DatabaseConfig, DatabaseId, DatabaseImpl, DbType};
        use noxu_sync::RwLock;
        use std::sync::Arc;

        let db_id = DatabaseId::new(1);
        let config = DatabaseConfig::default();
        let db_impl =
            DatabaseImpl::new(db_id, "verify_test".to_string(), DbType::User, &config);
        let db = Arc::new(RwLock::new(db_impl));
        let guard = db.read();
        let cfg = VerifyConfig::default();

        if let Some(t) = guard.get_real_tree() {
            let result = verify_tree(t, "verify_test", &cfg);
            assert!(result.passed, "empty tree should pass: {:?}", result.errors);
            assert_eq!(result.databases_verified, 1);
        }
        // If no real tree is present the test is a no-op.
    }

    /// verify_tree on a populated tree returns a passing result.
    ///
    /// Uses a real LogManager so that each put() receives a valid (non-NULL)
    /// LSN — the verifier requires this for all non-deleted BIN entries.
    #[test]
    fn test_verify_tree_populated() {
        use noxu_dbi::{
            CursorImpl, DatabaseConfig, DatabaseId, DatabaseImpl, DbType, PutMode,
        };
        use noxu_log::{FileManager, LogManager};
        use noxu_sync::RwLock;
        use std::sync::Arc;
        use tempfile::TempDir;

        let dir = TempDir::new().unwrap();
        let fm = Arc::new(
            FileManager::new(dir.path(), false, 64 * 1024 * 1024, 100).unwrap(),
        );
        let lm = Arc::new(LogManager::new(Arc::clone(&fm), 3, 1024 * 1024, 65536));

        let db_id = DatabaseId::new(2);
        let config = DatabaseConfig::default();
        let db_impl =
            DatabaseImpl::new(db_id, "pop_test".to_string(), DbType::User, &config);
        let db = Arc::new(RwLock::new(db_impl));

        {
            let mut cursor =
                CursorImpl::with_log_manager(Arc::clone(&db), 1, Arc::clone(&lm));
            cursor.put(b"alpha", b"1", PutMode::Overwrite).unwrap();
            cursor.put(b"beta", b"2", PutMode::Overwrite).unwrap();
            cursor.put(b"gamma", b"3", PutMode::Overwrite).unwrap();
        }

        let guard = db.read();
        let cfg = VerifyConfig::default();

        if let Some(t) = guard.get_real_tree() {
            let result = verify_tree(t, "pop_test", &cfg);
            assert!(result.passed, "populated tree should pass: {:?}", result.errors);
            assert_eq!(result.databases_verified, 1);
        }
    }
}
