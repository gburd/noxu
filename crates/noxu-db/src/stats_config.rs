//! Configuration for statistics retrieval operations.
//!
//! Implements `StatsConfig`.

/// Specifies the attributes of a statistics retrieval operation.
///
/// Pass to [`Environment::get_stats`][crate::environment::Environment::get_stats] or
/// [`Database::get_stats`][crate::database::Database::get_stats].
///
/// # Defaults
///
/// - `fast = false` — collect all statistics, including those that require an
///   expensive action such as a tree traversal or lock-table scan.
/// - `clear = false` — do not reset counters after reading them.
#[derive(Clone, Debug, Default)]
pub struct StatsConfig {
    /// If `true`, return only values that do not require expensive actions
    /// (e.g. skip B-tree traversal counts).  Implements `StatsConfig.setFast(true)`.
    pub fast: bool,
    /// If `true`, reset all counters to zero after reading them.
    /// Implements `StatsConfig.setClear(true)`.
    pub clear: bool,
}

impl StatsConfig {
    /// Creates a `StatsConfig` with all default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Convenience constructor: `fast = false`, `clear = true`.
    ///
    /// Implements `StatsConfig.CLEAR` constant.
    pub fn clear() -> Self {
        Self { fast: false, clear: true }
    }

    /// Builder: set `fast`.
    pub fn with_fast(mut self, fast: bool) -> Self {
        self.fast = fast;
        self
    }

    /// Builder: set `clear`.
    pub fn with_clear(mut self, clear: bool) -> Self {
        self.clear = clear;
        self
    }

    /// Sets `fast` and returns `&mut self` for chaining.
    pub fn set_fast(&mut self, fast: bool) -> &mut Self {
        self.fast = fast;
        self
    }

    /// Sets `clear` and returns `&mut self` for chaining.
    pub fn set_clear(&mut self, clear: bool) -> &mut Self {
        self.clear = clear;
        self
    }
}
