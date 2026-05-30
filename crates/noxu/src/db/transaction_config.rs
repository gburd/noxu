//! Transaction configuration.
//!

use crate::db::durability::Durability;

/// Configuration for transactions.
///
/// Specifies the configuration parameters used when beginning a transaction.
///
///
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionConfig {
    /// Durability for this transaction.
    pub durability: Durability,

    /// Use read-committed isolation.
    pub read_committed: bool,

    /// Use read-uncommitted isolation (dirty reads).
    pub read_uncommitted: bool,

    /// The transaction is read-only.
    pub read_only: bool,

    /// Don't wait for locks (fail immediately if lock unavailable).
    pub no_wait: bool,

    /// Lock timeout in milliseconds (0 = use environment default).
    pub lock_timeout_ms: u64,

    /// Transaction timeout in milliseconds (0 = no timeout).
    pub txn_timeout_ms: u64,

    /// Use serializable (repeatable-read) isolation.
    /// Read locks are retained through commit/abort.
    pub serializable_isolation: bool,

    /// Importunate transactions can steal locks from other lockers
    /// rather than waiting.
    pub importunate: bool,

    /// Writes stay local (used on read-only replicas for local modifications
    /// that are not replicated).
    pub local_write: bool,
}

impl TransactionConfig {
    /// Creates a new TransactionConfig with default settings.
    pub fn new() -> Self {
        Self {
            durability: Durability::default(),
            read_committed: false,
            read_uncommitted: false,
            read_only: false,
            no_wait: false,
            lock_timeout_ms: 0,
            txn_timeout_ms: 0,
            serializable_isolation: false,
            importunate: false,
            local_write: false,
        }
    }

    /// Sets the durability for this transaction.
    pub fn set_durability(&mut self, durability: Durability) -> &mut Self {
        self.durability = durability;
        self
    }

    /// Sets read-committed isolation.
    pub fn set_read_committed(&mut self, read_committed: bool) -> &mut Self {
        self.read_committed = read_committed;
        if read_committed {
            self.read_uncommitted = false;
        }
        self
    }

    /// Sets read-uncommitted isolation.
    pub fn set_read_uncommitted(
        &mut self,
        read_uncommitted: bool,
    ) -> &mut Self {
        self.read_uncommitted = read_uncommitted;
        if read_uncommitted {
            self.read_committed = false;
        }
        self
    }

    /// Sets whether the transaction is read-only.
    pub fn set_read_only(&mut self, read_only: bool) -> &mut Self {
        self.read_only = read_only;
        self
    }

    /// Sets whether to fail immediately if a lock is unavailable.
    pub fn set_no_wait(&mut self, no_wait: bool) -> &mut Self {
        self.no_wait = no_wait;
        self
    }

    /// Sets the lock timeout in milliseconds (0 = use environment default).
    pub fn set_lock_timeout_ms(&mut self, ms: u64) -> &mut Self {
        self.lock_timeout_ms = ms;
        self
    }

    /// Sets the transaction timeout in milliseconds (0 = no timeout).
    pub fn set_txn_timeout_ms(&mut self, ms: u64) -> &mut Self {
        self.txn_timeout_ms = ms;
        self
    }

    /// Sets serializable isolation (read locks retained through commit).
    pub fn set_serializable_isolation(&mut self, v: bool) -> &mut Self {
        self.serializable_isolation = v;
        self
    }

    /// Sets importunate mode (steal locks rather than wait).
    pub fn set_importunate(&mut self, v: bool) -> &mut Self {
        self.importunate = v;
        self
    }

    /// Sets local-write mode (writes not replicated).
    pub fn set_local_write(&mut self, v: bool) -> &mut Self {
        self.local_write = v;
        self
    }

    /// Builder-style method to set durability.
    pub fn with_durability(mut self, durability: Durability) -> Self {
        self.durability = durability;
        self
    }

    /// Builder-style method to set read_committed.
    pub fn with_read_committed(mut self, read_committed: bool) -> Self {
        self.set_read_committed(read_committed);
        self
    }

    /// Builder-style method to set read_uncommitted.
    pub fn with_read_uncommitted(mut self, read_uncommitted: bool) -> Self {
        self.set_read_uncommitted(read_uncommitted);
        self
    }

    /// Builder-style method to set read_only.
    pub fn with_read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    /// Builder-style method to set no_wait.
    pub fn with_no_wait(mut self, no_wait: bool) -> Self {
        self.no_wait = no_wait;
        self
    }

    /// Builder-style method to set lock timeout.
    pub fn with_lock_timeout_ms(mut self, ms: u64) -> Self {
        self.lock_timeout_ms = ms;
        self
    }

    /// Builder-style method to set transaction timeout.
    pub fn with_txn_timeout_ms(mut self, ms: u64) -> Self {
        self.txn_timeout_ms = ms;
        self
    }

    /// Builder-style method to set serializable isolation.
    pub fn with_serializable_isolation(mut self, v: bool) -> Self {
        self.serializable_isolation = v;
        self
    }

    /// Builder-style method to set importunate mode.
    pub fn with_importunate(mut self, v: bool) -> Self {
        self.importunate = v;
        self
    }

    /// Builder-style method to set local-write mode.
    pub fn with_local_write(mut self, v: bool) -> Self {
        self.local_write = v;
        self
    }

    /// Creates a TransactionConfig for read-committed isolation.
    pub fn read_committed() -> Self {
        Self::new().with_read_committed(true)
    }

    /// Creates a TransactionConfig for read-uncommitted isolation.
    pub fn read_uncommitted() -> Self {
        Self::new().with_read_uncommitted(true)
    }

    /// Creates a TransactionConfig for read-only transactions.
    pub fn read_only() -> Self {
        Self::new().with_read_only(true)
    }
}

impl Default for TransactionConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let config = TransactionConfig::new();
        assert_eq!(config.durability, Durability::default());
        assert!(!config.read_committed);
        assert!(!config.read_uncommitted);
        assert!(!config.read_only);
        assert!(!config.no_wait);
    }

    #[test]
    fn test_set_durability() {
        let mut config = TransactionConfig::new();
        config.set_durability(Durability::COMMIT_NO_SYNC);
        assert_eq!(config.durability, Durability::COMMIT_NO_SYNC);
    }

    #[test]
    fn test_set_read_committed() {
        let mut config = TransactionConfig::new();
        config.set_read_committed(true);
        assert!(config.read_committed);
        assert!(!config.read_uncommitted);
    }

    #[test]
    fn test_set_read_uncommitted() {
        let mut config = TransactionConfig::new();
        config.set_read_uncommitted(true);
        assert!(config.read_uncommitted);
        assert!(!config.read_committed);
    }

    #[test]
    fn test_isolation_mutual_exclusion() {
        let mut config = TransactionConfig::new();
        config.set_read_committed(true);
        assert!(config.read_committed);

        config.set_read_uncommitted(true);
        assert!(config.read_uncommitted);
        assert!(!config.read_committed);

        config.set_read_committed(true);
        assert!(config.read_committed);
        assert!(!config.read_uncommitted);
    }

    #[test]
    fn test_set_read_only() {
        let mut config = TransactionConfig::new();
        config.set_read_only(true);
        assert!(config.read_only);
    }

    #[test]
    fn test_set_no_wait() {
        let mut config = TransactionConfig::new();
        config.set_no_wait(true);
        assert!(config.no_wait);
    }

    #[test]
    fn test_with_durability() {
        let config = TransactionConfig::new()
            .with_durability(Durability::COMMIT_WRITE_NO_SYNC);
        assert_eq!(config.durability, Durability::COMMIT_WRITE_NO_SYNC);
    }

    #[test]
    fn test_with_read_committed() {
        let config = TransactionConfig::new().with_read_committed(true);
        assert!(config.read_committed);
    }

    #[test]
    fn test_with_read_uncommitted() {
        let config = TransactionConfig::new().with_read_uncommitted(true);
        assert!(config.read_uncommitted);
    }

    #[test]
    fn test_with_read_only() {
        let config = TransactionConfig::new().with_read_only(true);
        assert!(config.read_only);
    }

    #[test]
    fn test_with_no_wait() {
        let config = TransactionConfig::new().with_no_wait(true);
        assert!(config.no_wait);
    }

    #[test]
    fn test_read_committed_factory() {
        let config = TransactionConfig::read_committed();
        assert!(config.read_committed);
        assert!(!config.read_uncommitted);
    }

    #[test]
    fn test_read_uncommitted_factory() {
        let config = TransactionConfig::read_uncommitted();
        assert!(config.read_uncommitted);
        assert!(!config.read_committed);
    }

    #[test]
    fn test_read_only_factory() {
        let config = TransactionConfig::read_only();
        assert!(config.read_only);
    }

    #[test]
    fn test_default() {
        let config = TransactionConfig::default();
        assert!(!config.read_committed);
        assert!(!config.read_uncommitted);
    }

    #[test]
    fn test_clone() {
        let config1 = TransactionConfig::read_committed();
        let config2 = config1.clone();
        assert_eq!(config1, config2);
    }

    #[test]
    fn test_equality() {
        let config1 = TransactionConfig::new();
        let config2 = TransactionConfig::default();
        assert_eq!(config1, config2);

        let config3 = TransactionConfig::read_only();
        assert_ne!(config1, config3);
    }

    #[test]
    fn test_builder_chain() {
        let config = TransactionConfig::new()
            .with_durability(Durability::COMMIT_NO_SYNC)
            .with_read_committed(true)
            .with_no_wait(true);
        assert_eq!(config.durability, Durability::COMMIT_NO_SYNC);
        assert!(config.read_committed);
        assert!(config.no_wait);
    }

    #[test]
    fn test_debug() {
        let config = TransactionConfig::read_only();
        let debug = format!("{:?}", config);
        assert!(debug.contains("read_only"));
    }
}
