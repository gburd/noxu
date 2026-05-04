//! Sequence configuration.
//!
//! Port of `com.sleepycat.je.SequenceConfig`.

/// Specifies the attributes of a sequence.
///
/// Port of `com.sleepycat.je.SequenceConfig`.
#[derive(Debug, Clone)]
pub struct SequenceConfig {
    /// Number of elements cached in the sequence handle (default 20).
    ///
    /// JE default is 0 but the task specifies 20 as the noxu default for
    /// pre-fetching.  A value of 0 disables caching (every get hits the DB).
    pub cache_size: i32,

    /// Minimum value for the sequence (default i64::MIN).
    pub range_min: i64,

    /// Maximum value for the sequence (default i64::MAX).
    pub range_max: i64,

    /// Initial value when the sequence is first created (default 0).
    pub initial_value: i64,

    /// Create the sequence if it does not exist (default false).
    pub allow_create: bool,

    /// The sequence counts downward when true (default false — counts upward).
    pub decrement: bool,

    /// Fail if the sequence already exists (default false).
    pub exclusive_create: bool,

    /// Wrap around when the range limit is reached (default false).
    pub wrap: bool,
}

impl SequenceConfig {
    /// Creates a `SequenceConfig` with all defaults.
    pub fn new() -> Self {
        Self {
            cache_size: 20,
            range_min: i64::MIN,
            range_max: i64::MAX,
            initial_value: 0,
            allow_create: false,
            decrement: false,
            exclusive_create: false,
            wrap: false,
        }
    }

    /// Sets the number of cached elements.
    pub fn with_cache_size(mut self, cache_size: i32) -> Self {
        self.cache_size = cache_size;
        self
    }

    /// Sets the sequence range.
    pub fn with_range(mut self, min: i64, max: i64) -> Self {
        self.range_min = min;
        self.range_max = max;
        self
    }

    /// Sets the minimum value of the range.
    pub fn with_range_min(mut self, min: i64) -> Self {
        self.range_min = min;
        self
    }

    /// Sets the maximum value of the range.
    pub fn with_range_max(mut self, max: i64) -> Self {
        self.range_max = max;
        self
    }

    /// Sets the initial value (only effective on creation).
    pub fn with_initial_value(mut self, initial_value: i64) -> Self {
        self.initial_value = initial_value;
        self
    }

    /// Configures whether the sequence is created if missing.
    pub fn with_allow_create(mut self, allow_create: bool) -> Self {
        self.allow_create = allow_create;
        self
    }

    /// Configures whether the sequence decrements instead of incrementing.
    pub fn with_decrement(mut self, decrement: bool) -> Self {
        self.decrement = decrement;
        self
    }

    /// Configures whether to fail when the sequence already exists.
    pub fn with_exclusive_create(mut self, exclusive_create: bool) -> Self {
        self.exclusive_create = exclusive_create;
        self
    }

    /// Configures whether to wrap around at the range boundary.
    pub fn with_wrap(mut self, wrap: bool) -> Self {
        self.wrap = wrap;
        self
    }
}

impl Default for SequenceConfig {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_values() {
        let cfg = SequenceConfig::new();
        assert_eq!(cfg.cache_size, 20);
        assert_eq!(cfg.range_min, i64::MIN);
        assert_eq!(cfg.range_max, i64::MAX);
        assert_eq!(cfg.initial_value, 0);
        assert!(!cfg.allow_create);
        assert!(!cfg.decrement);
        assert!(!cfg.exclusive_create);
        assert!(!cfg.wrap);
    }

    #[test]
    fn test_default_trait() {
        let cfg = SequenceConfig::default();
        assert_eq!(cfg.cache_size, 20);
        assert_eq!(cfg.initial_value, 0);
    }

    #[test]
    fn test_with_cache_size() {
        let cfg = SequenceConfig::new().with_cache_size(100);
        assert_eq!(cfg.cache_size, 100);
    }

    #[test]
    fn test_with_cache_size_zero() {
        // Zero means no caching
        let cfg = SequenceConfig::new().with_cache_size(0);
        assert_eq!(cfg.cache_size, 0);
    }

    #[test]
    fn test_with_range() {
        let cfg = SequenceConfig::new().with_range(1, 1000);
        assert_eq!(cfg.range_min, 1);
        assert_eq!(cfg.range_max, 1000);
    }

    #[test]
    fn test_with_range_min() {
        let cfg = SequenceConfig::new().with_range_min(42);
        assert_eq!(cfg.range_min, 42);
        // range_max should remain default
        assert_eq!(cfg.range_max, i64::MAX);
    }

    #[test]
    fn test_with_range_max() {
        let cfg = SequenceConfig::new().with_range_max(999);
        assert_eq!(cfg.range_max, 999);
        // range_min should remain default
        assert_eq!(cfg.range_min, i64::MIN);
    }

    #[test]
    fn test_with_range_negative() {
        let cfg = SequenceConfig::new().with_range(-100, -1);
        assert_eq!(cfg.range_min, -100);
        assert_eq!(cfg.range_max, -1);
    }

    #[test]
    fn test_with_initial_value() {
        let cfg = SequenceConfig::new().with_initial_value(500);
        assert_eq!(cfg.initial_value, 500);
    }

    #[test]
    fn test_with_initial_value_negative() {
        let cfg = SequenceConfig::new().with_initial_value(-50);
        assert_eq!(cfg.initial_value, -50);
    }

    #[test]
    fn test_with_allow_create_true() {
        let cfg = SequenceConfig::new().with_allow_create(true);
        assert!(cfg.allow_create);
    }

    #[test]
    fn test_with_allow_create_false() {
        let cfg = SequenceConfig::new()
            .with_allow_create(true)
            .with_allow_create(false);
        assert!(!cfg.allow_create);
    }

    #[test]
    fn test_with_decrement_true() {
        let cfg = SequenceConfig::new().with_decrement(true);
        assert!(cfg.decrement);
    }

    #[test]
    fn test_with_decrement_false() {
        let cfg = SequenceConfig::new()
            .with_decrement(true)
            .with_decrement(false);
        assert!(!cfg.decrement);
    }

    #[test]
    fn test_with_exclusive_create_true() {
        let cfg = SequenceConfig::new().with_exclusive_create(true);
        assert!(cfg.exclusive_create);
    }

    #[test]
    fn test_with_exclusive_create_false() {
        let cfg = SequenceConfig::new()
            .with_exclusive_create(true)
            .with_exclusive_create(false);
        assert!(!cfg.exclusive_create);
    }

    #[test]
    fn test_with_wrap_true() {
        let cfg = SequenceConfig::new().with_wrap(true);
        assert!(cfg.wrap);
    }

    #[test]
    fn test_with_wrap_false() {
        let cfg = SequenceConfig::new()
            .with_wrap(true)
            .with_wrap(false);
        assert!(!cfg.wrap);
    }

    #[test]
    fn test_builder_chain_full() {
        let cfg = SequenceConfig::new()
            .with_cache_size(50)
            .with_range(0, 10_000)
            .with_initial_value(1)
            .with_allow_create(true)
            .with_wrap(true)
            .with_decrement(false)
            .with_exclusive_create(false);

        assert_eq!(cfg.cache_size, 50);
        assert_eq!(cfg.range_min, 0);
        assert_eq!(cfg.range_max, 10_000);
        assert_eq!(cfg.initial_value, 1);
        assert!(cfg.allow_create);
        assert!(cfg.wrap);
        assert!(!cfg.decrement);
        assert!(!cfg.exclusive_create);
    }

    #[test]
    fn test_builder_chain_decrement_sequence() {
        // Typical decrementing sequence: high-to-low range, starts at max
        let cfg = SequenceConfig::new()
            .with_range(-1000, 0)
            .with_initial_value(0)
            .with_decrement(true)
            .with_wrap(true)
            .with_allow_create(true);

        assert_eq!(cfg.range_min, -1000);
        assert_eq!(cfg.range_max, 0);
        assert_eq!(cfg.initial_value, 0);
        assert!(cfg.decrement);
        assert!(cfg.wrap);
        assert!(cfg.allow_create);
    }

    #[test]
    fn test_with_range_overrides_individual_setters() {
        // with_range sets both min and max atomically
        let cfg = SequenceConfig::new()
            .with_range_min(5)
            .with_range_max(50)
            .with_range(100, 200); // overrides both
        assert_eq!(cfg.range_min, 100);
        assert_eq!(cfg.range_max, 200);
    }

    #[test]
    fn test_clone() {
        let original = SequenceConfig::new()
            .with_cache_size(10)
            .with_range(1, 100)
            .with_allow_create(true);
        let cloned = original.clone();
        assert_eq!(cloned.cache_size, 10);
        assert_eq!(cloned.range_min, 1);
        assert_eq!(cloned.range_max, 100);
        assert!(cloned.allow_create);
    }

    #[test]
    fn test_debug() {
        let cfg = SequenceConfig::new();
        let s = format!("{:?}", cfg);
        assert!(s.contains("SequenceConfig"));
        assert!(s.contains("cache_size"));
    }
}
