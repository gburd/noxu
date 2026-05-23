//! Statistics for eager schema evolution.
//!

/// Statistics accumulated during eager entity evolution.
///
/// Returned from [`EntityStore::evolve`] and also passed to progress
/// listeners during evolution.
///
///
///
/// [`EntityStore::evolve`]: crate::entity_store::EntityStore::evolve
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EvolveStats {
    /// Total number of entities read during evolution.
    n_read: u64,
    /// Total number of entities written (converted) during evolution.
    n_converted: u64,
}

impl EvolveStats {
    /// Creates a new zeroed-out stats object.
    pub fn new() -> Self {
        Self { n_read: 0, n_converted: 0 }
    }

    /// Accumulates counts from processing one batch/class.
    ///
    ///
    pub fn add(&mut self, n_read: u64, n_converted: u64) {
        self.n_read += n_read;
        self.n_converted += n_converted;
    }

    /// Returns the total number of entities read during eager evolution.
    ///
    ///
    pub fn n_read(&self) -> u64 {
        self.n_read
    }

    /// Returns the total number of entities converted (written) during eager
    /// evolution.
    ///
    ///
    pub fn n_converted(&self) -> u64 {
        self.n_converted
    }
}

impl std::fmt::Display for EvolveStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EvolveStats {{ n_read: {}, n_converted: {} }}",
            self.n_read, self.n_converted
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_zeros() {
        let s = EvolveStats::new();
        assert_eq!(s.n_read(), 0);
        assert_eq!(s.n_converted(), 0);
    }

    #[test]
    fn test_add_accumulates() {
        let mut s = EvolveStats::new();
        s.add(10, 7);
        s.add(5, 3);
        assert_eq!(s.n_read(), 15);
        assert_eq!(s.n_converted(), 10);
    }

    #[test]
    fn test_add_zero() {
        let mut s = EvolveStats::new();
        s.add(0, 0);
        assert_eq!(s.n_read(), 0);
        assert_eq!(s.n_converted(), 0);
    }

    #[test]
    fn test_equality() {
        let mut a = EvolveStats::new();
        a.add(5, 3);
        let mut b = EvolveStats::new();
        b.add(5, 3);
        assert_eq!(a, b);
    }

    #[test]
    fn test_inequality() {
        let mut a = EvolveStats::new();
        a.add(5, 3);
        let mut b = EvolveStats::new();
        b.add(5, 4);
        assert_ne!(a, b);
    }

    #[test]
    fn test_clone() {
        let mut s = EvolveStats::new();
        s.add(100, 50);
        let cloned = s.clone();
        assert_eq!(s, cloned);
    }

    #[test]
    fn test_display() {
        let mut s = EvolveStats::new();
        s.add(42, 21);
        let d = s.to_string();
        assert!(d.contains("42"));
        assert!(d.contains("21"));
    }
}
