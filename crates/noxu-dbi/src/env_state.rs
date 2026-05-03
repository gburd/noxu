//! Environment state.
//!
//! Port of `com.sleepycat.je.dbi.DbEnvState`.

/// States of the environment lifecycle.
///
/// Port of `com.sleepycat.je.dbi.DbEnvState`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvState {
    /// Environment is being initialized.
    Init,
    /// Environment is open and operational.
    Open,
    /// Environment is closing.
    Closing,
    /// Environment is closed.
    Closed,
    /// Environment has been invalidated due to a failure.
    Invalid,
}

impl EnvState {
    /// Returns true if the environment is open.
    pub fn is_open(&self) -> bool {
        *self == EnvState::Open
    }

    /// Returns true if the environment is valid (open or initializing).
    pub fn is_valid(&self) -> bool {
        matches!(self, EnvState::Open | EnvState::Init)
    }

    /// Returns true if the environment is closed or invalid.
    pub fn is_closed(&self) -> bool {
        matches!(self, EnvState::Closed | EnvState::Invalid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_open() {
        assert!(!EnvState::Init.is_open());
        assert!(EnvState::Open.is_open());
        assert!(!EnvState::Closing.is_open());
        assert!(!EnvState::Closed.is_open());
        assert!(!EnvState::Invalid.is_open());
    }

    #[test]
    fn test_is_valid() {
        assert!(EnvState::Init.is_valid());
        assert!(EnvState::Open.is_valid());
        assert!(!EnvState::Closing.is_valid());
        assert!(!EnvState::Closed.is_valid());
        assert!(!EnvState::Invalid.is_valid());
    }

    #[test]
    fn test_is_closed() {
        assert!(!EnvState::Init.is_closed());
        assert!(!EnvState::Open.is_closed());
        assert!(!EnvState::Closing.is_closed());
        assert!(EnvState::Closed.is_closed());
        assert!(EnvState::Invalid.is_closed());
    }

    #[test]
    fn test_all_states() {
        let states = vec![
            EnvState::Init,
            EnvState::Open,
            EnvState::Closing,
            EnvState::Closed,
            EnvState::Invalid,
        ];

        assert_eq!(states.len(), 5);

        // Verify all predicates for all states
        for state in &states {
            let _ = state.is_open();
            let _ = state.is_valid();
            let _ = state.is_closed();
        }
    }

    #[test]
    fn test_equality() {
        assert_eq!(EnvState::Open, EnvState::Open);
        assert_ne!(EnvState::Open, EnvState::Closed);
    }
}
