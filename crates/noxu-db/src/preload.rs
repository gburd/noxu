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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database_config::DatabaseConfig;
    use crate::environment::Environment;
    use crate::environment_config::EnvironmentConfig;
    use tempfile::TempDir;

    #[test]
    fn defaults_are_unlimited_and_bins_only() {
        let c = PreloadConfig::new();
        assert_eq!(c.max_bytes, 0, "0 means unlimited");
        assert_eq!(c.max_millis, 0, "0 means unlimited");
        assert!(!c.load_lns, "LN warming is opt-in");
        assert_eq!(c, PreloadConfig::default());
    }

    #[test]
    fn builders_are_independent() {
        let d = PreloadConfig::new();
        let only_bytes = PreloadConfig::new().with_max_bytes(4096);
        assert_eq!(only_bytes.max_bytes, 4096);
        assert_eq!(only_bytes.max_millis, d.max_millis);
        assert_eq!(only_bytes.load_lns, d.load_lns);

        let only_millis = PreloadConfig::new().with_max_millis(250);
        assert_eq!(only_millis.max_millis, 250);
        assert_eq!(only_millis.max_bytes, d.max_bytes);

        let only_lns = PreloadConfig::new().with_load_lns(true);
        assert!(only_lns.load_lns);
        assert_eq!(only_lns.max_bytes, d.max_bytes);
        assert_eq!(only_lns.max_millis, d.max_millis);
    }

    fn seeded(n: u16) -> (TempDir, Environment, crate::database::Database) {
        let dir = TempDir::new().unwrap();
        let env = Environment::open(
            EnvironmentConfig::new(dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
        let db = env
            .open_database(
                None,
                "preload",
                &DatabaseConfig::new()
                    .with_allow_create(true)
                    .with_transactional(true),
            )
            .unwrap();
        for i in 0..n {
            db.put(i.to_be_bytes(), b"v").unwrap();
        }
        (dir, env, db)
    }

    /// `load_lns` is what distinguishes the two preload shapes: without it
    /// `lns_loaded` must stay 0, with it `lns_loaded` must equal the record
    /// count. `bins_loaded` is reported either way.
    ///
    /// (Documented limitation: with `load_lns` this is the LN *slot* count,
    /// not a count of LNs actually faulted in from disk. The assertion below
    /// deliberately pins the slot count, which is what the API currently
    /// promises — if true LN warming lands, this test must be updated on
    /// purpose.)
    #[test]
    fn load_lns_controls_whether_lns_are_reported() {
        let (_d, _e, db) = seeded(48);

        let bins_only = db.preload(&PreloadConfig::new()).unwrap();
        assert!(bins_only.bins_loaded > 0, "the BIN walk must report BINs");
        assert_eq!(
            bins_only.lns_loaded, 0,
            "without load_lns, no LNs may be reported"
        );

        let with_lns =
            db.preload(&PreloadConfig::new().with_load_lns(true)).unwrap();
        assert_eq!(
            with_lns.bins_loaded, bins_only.bins_loaded,
            "load_lns must not change the BIN count"
        );
        assert_eq!(
            with_lns.lns_loaded, 48,
            "with load_lns, every record's LN slot must be reported \
             (upper-IN routing slots must NOT inflate this)"
        );
    }

    /// Preload of an empty database must succeed and report nothing, rather
    /// than failing on the absent root.
    #[test]
    fn preload_of_an_empty_database_reports_nothing() {
        let (_d, _e, db) = seeded(0);
        let stats =
            db.preload(&PreloadConfig::new().with_load_lns(true)).unwrap();
        assert_eq!(stats.lns_loaded, 0);
    }

    /// `max_millis` is documented as advisory (the BIN walker is not yet
    /// interruptible), so an impossibly small budget must NOT truncate the
    /// results or fail — it only warns. Pinning this so the day the walker
    /// becomes interruptible, this test has to change deliberately.
    #[test]
    fn max_millis_is_advisory_and_does_not_truncate_results() {
        let (_d, _e, db) = seeded(48);
        let unbounded = db.preload(&PreloadConfig::new()).unwrap();
        let bounded =
            db.preload(&PreloadConfig::new().with_max_millis(1)).unwrap();
        assert_eq!(
            bounded.bins_loaded, unbounded.bins_loaded,
            "max_millis is advisory today: it must not truncate the walk"
        );
    }

    #[test]
    fn preload_is_rejected_on_a_closed_database() {
        let (_d, _e, db) = seeded(4);
        db.close().unwrap();
        assert!(db.preload(&PreloadConfig::new()).is_err());
    }

    #[test]
    fn preload_stats_default_is_all_zero() {
        let s = PreloadStats::default();
        assert_eq!((s.bins_loaded, s.lns_loaded, s.elapsed_ms), (0, 0, 0));
    }
}
