//! Election configuration.
//!
//! Port of `com.sleepycat.je.rep.elections.ElectionsConfig`  -  tunables that
//! govern how elections are conducted: timeouts, retry limits, node priority,
//! and designated-primary behaviour.

use std::time::Duration;

/// Configuration parameters for the election subsystem.
///
/// Use the builder pattern to construct:
///
/// ```
/// use noxu_rep::elections::ElectionConfig;
/// use std::time::Duration;
///
/// let config = ElectionConfig::builder()
///     .election_timeout(Duration::from_secs(5))
///     .max_retries(2)
///     .priority(10)
///     .designated_primary(true)
///     .build();
///
/// assert_eq!(config.priority(), 10);
/// assert!(config.designated_primary());
/// ```
#[derive(Debug, Clone)]
pub struct ElectionConfig {
    /// Maximum time to wait for an election to complete before timing out.
    election_timeout: Duration,
    /// Maximum number of election retries before giving up.
    max_retries: u32,
    /// Node's election priority. Higher values make the node more likely to
    /// become master. A priority of zero means the node will never volunteer
    /// as master (it can still vote).
    priority: u32,
    /// When `true` and the replication group has exactly two electable nodes,
    /// this node may elect itself master without a quorum. This avoids a
    /// two-node group becoming stuck when one node is down.
    designated_primary: bool,
}

impl ElectionConfig {
    /// Returns a new [`ElectionConfigBuilder`] with default values.
    pub fn builder() -> ElectionConfigBuilder {
        ElectionConfigBuilder::default()
    }

    /// Returns a config with all default values.
    pub fn new() -> Self {
        ElectionConfigBuilder::default().build()
    }

    /// Maximum time to wait for an election to complete.
    pub fn election_timeout(&self) -> Duration {
        self.election_timeout
    }

    /// Maximum number of election retries.
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }

    /// Node's election priority (higher = more likely to become master).
    pub fn priority(&self) -> u32 {
        self.priority
    }

    /// Whether this node can self-elect in a two-node group.
    pub fn designated_primary(&self) -> bool {
        self.designated_primary
    }
}

impl Default for ElectionConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for [`ElectionConfig`].
#[derive(Debug, Clone)]
pub struct ElectionConfigBuilder {
    election_timeout: Duration,
    max_retries: u32,
    priority: u32,
    designated_primary: bool,
}

impl Default for ElectionConfigBuilder {
    fn default() -> Self {
        Self {
            election_timeout: Duration::from_secs(10),
            max_retries: 3,
            priority: 1,
            designated_primary: false,
        }
    }
}

impl ElectionConfigBuilder {
    /// Set the election timeout.
    pub fn election_timeout(mut self, timeout: Duration) -> Self {
        self.election_timeout = timeout;
        self
    }

    /// Set the maximum number of retries.
    pub fn max_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }

    /// Set the node's election priority.
    pub fn priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }

    /// Set whether this node is the designated primary.
    pub fn designated_primary(mut self, designated: bool) -> Self {
        self.designated_primary = designated;
        self
    }

    /// Build the [`ElectionConfig`].
    pub fn build(self) -> ElectionConfig {
        ElectionConfig {
            election_timeout: self.election_timeout,
            max_retries: self.max_retries,
            priority: self.priority,
            designated_primary: self.designated_primary,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_defaults() {
        let config = ElectionConfig::new();
        assert_eq!(config.election_timeout(), Duration::from_secs(10));
        assert_eq!(config.max_retries(), 3);
        assert_eq!(config.priority(), 1);
        assert!(!config.designated_primary());
    }

    #[test]
    fn test_default_trait() {
        let config = ElectionConfig::default();
        assert_eq!(config.election_timeout(), Duration::from_secs(10));
    }

    #[test]
    fn test_builder_all_fields() {
        let config = ElectionConfig::builder()
            .election_timeout(Duration::from_secs(30))
            .max_retries(5)
            .priority(100)
            .designated_primary(true)
            .build();

        assert_eq!(config.election_timeout(), Duration::from_secs(30));
        assert_eq!(config.max_retries(), 5);
        assert_eq!(config.priority(), 100);
        assert!(config.designated_primary());
    }

    #[test]
    fn test_builder_partial() {
        let config = ElectionConfig::builder().priority(42).build();

        // Non-specified fields keep defaults.
        assert_eq!(config.election_timeout(), Duration::from_secs(10));
        assert_eq!(config.max_retries(), 3);
        assert_eq!(config.priority(), 42);
        assert!(!config.designated_primary());
    }

    #[test]
    fn test_builder_chaining_order_independent() {
        let a = ElectionConfig::builder().priority(5).max_retries(2).build();
        let b = ElectionConfig::builder().max_retries(2).priority(5).build();

        assert_eq!(a.priority(), b.priority());
        assert_eq!(a.max_retries(), b.max_retries());
    }

    #[test]
    fn test_clone() {
        let config = ElectionConfig::builder()
            .priority(7)
            .designated_primary(true)
            .build();
        let cloned = config;

        assert_eq!(cloned.priority(), 7);
        assert!(cloned.designated_primary());
    }

    #[test]
    fn test_zero_priority() {
        let config = ElectionConfig::builder().priority(0).build();
        assert_eq!(config.priority(), 0);
    }

    #[test]
    fn test_zero_timeout() {
        let config =
            ElectionConfig::builder().election_timeout(Duration::ZERO).build();
        assert_eq!(config.election_timeout(), Duration::ZERO);
    }
}
