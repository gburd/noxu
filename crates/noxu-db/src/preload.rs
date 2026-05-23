//! Database preloading configuration and statistics.

/// Configuration for database preloading.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreloadConfig {
    /// Maximum bytes to load (0 = unlimited).
    pub max_bytes: u64,
    /// Maximum time in milliseconds (0 = unlimited).
    pub max_millis: u64,
    /// Whether to also load leaf node data (not just BINs).
    pub load_lns: bool,
}

impl PreloadConfig {
    /// Creates a new PreloadConfig with default settings (load everything).
    pub fn new() -> Self {
        Self { max_bytes: 0, max_millis: 0, load_lns: false }
    }

    /// Builder-style: set max_bytes.
    pub fn with_max_bytes(mut self, max_bytes: u64) -> Self {
        self.max_bytes = max_bytes;
        self
    }

    /// Builder-style: set max_millis.
    pub fn with_max_millis(mut self, max_millis: u64) -> Self {
        self.max_millis = max_millis;
        self
    }

    /// Builder-style: set load_lns.
    pub fn with_load_lns(mut self, load_lns: bool) -> Self {
        self.load_lns = load_lns;
        self
    }
}

impl Default for PreloadConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics returned from a database preload operation.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PreloadStats {
    /// Number of BIN (bottom internal) nodes loaded.
    pub bins_loaded: u64,
    /// Number of leaf nodes (LNs) loaded.
    pub lns_loaded: u64,
    /// Total elapsed time in milliseconds.
    pub elapsed_ms: u64,
}
