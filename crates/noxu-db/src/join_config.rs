//! Configuration for JoinCursor.
//!
//! Mirrors JE's `JoinConfig` with one field: `no_sort`.

/// Configuration properties for a [`JoinCursor`][crate::join_cursor::JoinCursor].
///
/// Pass to [`Database::join`][crate::database::Database::join] to control join
/// cursor behaviour.
///
/// # Default
///
/// * `no_sort = false` — cursors are automatically sorted by estimated
///   duplicate count (smallest set first) so that the join algorithm can
///   prune candidates as early as possible.  Set `no_sort = true` if you
///   have already ordered the cursor array optimally.
#[derive(Clone, Debug, Default)]
pub struct JoinConfig {
    /// Disables automatic cursor sorting when `true`.
    pub no_sort: bool,
}

impl JoinConfig {
    /// Creates a new `JoinConfig` with all defaults.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets whether automatic cursor sorting is disabled.
    pub fn set_no_sort(&mut self, no_sort: bool) {
        self.no_sort = no_sort;
    }

    /// Builder-style `no_sort` setter.
    pub fn with_no_sort(mut self, no_sort: bool) -> Self {
        self.no_sort = no_sort;
        self
    }

    /// Returns `true` if automatic cursor sorting is disabled.
    pub fn get_no_sort(&self) -> bool {
        self.no_sort
    }
}
