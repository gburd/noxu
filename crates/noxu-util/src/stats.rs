//! Statistics framework.
//!
//! Statistics framework: `StatDefinition`, `StatGroup`, and related types.
//!
//! Provides a framework for defining, collecting, and reporting statistics
//! across all database subsystems.

use std::collections::HashMap;
use std::fmt;
use std::sync::atomic::{AtomicI64, Ordering};

/// Describes a single statistic: its name, description, and type.
#[derive(Debug, Clone)]
pub struct StatDefinition {
    /// Short programmatic name used as a key.
    pub name: &'static str,
    /// Human-readable description.
    pub description: &'static str,
    /// The type/interpretation of this statistic.
    pub stat_type: StatType,
}

/// How a statistic value should be interpreted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatType {
    /// A cumulative counter that increases over time.
    Cumulative,
    /// A point-in-time value (gauge).
    Current,
    /// A value that is always increasing (never reset).
    Incremental,
}

/// A named group of related statistics.
#[derive(Debug)]
pub struct StatGroup {
    /// Name of this statistics group (e.g., "Cache", "Log", "Cleaner").
    pub name: &'static str,
    /// Description of what this group measures.
    pub description: &'static str,
    /// Statistics in this group, keyed by definition name.
    stats: HashMap<&'static str, AtomicI64>,
}

impl StatGroup {
    /// Creates a new statistics group.
    pub fn new(name: &'static str, description: &'static str) -> Self {
        StatGroup { name, description, stats: HashMap::new() }
    }

    /// Registers a stat definition and initializes its value to zero.
    pub fn register(&mut self, def: &StatDefinition) {
        self.stats.entry(def.name).or_insert_with(|| AtomicI64::new(0));
    }

    /// Gets the current value of a stat.
    pub fn get(&self, name: &str) -> i64 {
        self.stats.get(name).map(|v| v.load(Ordering::Relaxed)).unwrap_or(0)
    }

    /// Sets the value of a stat.
    pub fn set(&self, name: &str, value: i64) {
        if let Some(stat) = self.stats.get(name) {
            stat.store(value, Ordering::Relaxed);
        }
    }

    /// Atomically increments a stat by the given amount.
    pub fn increment(&self, name: &str, delta: i64) {
        if let Some(stat) = self.stats.get(name) {
            stat.fetch_add(delta, Ordering::Relaxed);
        }
    }

    /// Resets all stats in this group to zero.
    pub fn clear(&self) {
        for stat in self.stats.values() {
            stat.store(0, Ordering::Relaxed);
        }
    }
}

impl fmt::Display for StatGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}:", self.name)?;
        for (name, value) in &self.stats {
            writeln!(f, "  {} = {}", name, value.load(Ordering::Relaxed))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    static TEST_STAT: StatDefinition = StatDefinition {
        name: "nOperations",
        description: "Number of operations performed",
        stat_type: StatType::Cumulative,
    };

    #[test]
    fn test_stat_group_basic() {
        let mut group = StatGroup::new("Test", "Test statistics");
        group.register(&TEST_STAT);

        assert_eq!(group.get("nOperations"), 0);
        group.increment("nOperations", 5);
        assert_eq!(group.get("nOperations"), 5);
        group.increment("nOperations", 3);
        assert_eq!(group.get("nOperations"), 8);
    }

    #[test]
    fn test_stat_group_clear() {
        let mut group = StatGroup::new("Test", "Test statistics");
        group.register(&TEST_STAT);
        group.set("nOperations", 42);
        assert_eq!(group.get("nOperations"), 42);
        group.clear();
        assert_eq!(group.get("nOperations"), 0);
    }
}
