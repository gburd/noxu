//! Environment failure reasons.
//!
//! Port of `com.sleepycat.je.dbi.EnvironmentFailureReason`.

/// Reasons why an environment might fail and become invalid.
///
/// Port of `com.sleepycat.je.dbi.EnvironmentFailureReason`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnvironmentFailureReason {
    /// Environment is locked by another process.
    EnvLocked,
    /// Environment directory not found.
    EnvNotFound,
    /// Log file checksum error.
    LogChecksum,
    /// Log file not found.
    LogFileNotFound,
    /// Error reading log file.
    LogRead,
    /// Error writing log file.
    LogWrite,
    /// Log integrity violation.
    LogIntegrity,
    /// Incomplete log entry.
    LogIncomplete,
    /// B-tree structure corruption.
    BtreeCorruption,
    /// Java-specific error (included for completeness).
    JavaError,
    /// Latch already held by current thread.
    LatchAlreadyHeld,
    /// Latch not held when expected.
    LatchNotHeld,
    /// Thread was interrupted.
    ThreadInterrupted,
    /// Uncaught exception occurred.
    UncaughtException,
    /// Unexpected state (non-fatal).
    UnexpectedState,
    /// Unexpected state (fatal).
    UnexpectedStateFatal,
    /// Unexpected exception (non-fatal).
    UnexpectedException,
    /// Unexpected exception (fatal).
    UnexpectedExceptionFatal,
    /// Test-induced invalidation.
    TestInvalidate,
    /// Hard recovery required.
    HardRecovery,
    /// Shutdown was requested.
    ShutdownRequested,
    /// Environment is wedged (unrecoverable).
    Wedged,
}

impl EnvironmentFailureReason {
    /// Returns true if this failure reason invalidates the environment.
    ///
    /// Some failures are recoverable and don't require invalidation.
    pub fn invalidates_environment(&self) -> bool {
        !matches!(
            self,
            EnvironmentFailureReason::EnvLocked
                | EnvironmentFailureReason::EnvNotFound
                | EnvironmentFailureReason::LatchAlreadyHeld
                | EnvironmentFailureReason::LatchNotHeld
                | EnvironmentFailureReason::LogIntegrity
                | EnvironmentFailureReason::UnexpectedState
                | EnvironmentFailureReason::UnexpectedException
        )
    }
}

impl std::fmt::Display for EnvironmentFailureReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let msg = match self {
            EnvironmentFailureReason::EnvLocked => {
                "environment locked by another process"
            }
            EnvironmentFailureReason::EnvNotFound => {
                "environment directory not found"
            }
            EnvironmentFailureReason::LogChecksum => "log file checksum error",
            EnvironmentFailureReason::LogFileNotFound => "log file not found",
            EnvironmentFailureReason::LogRead => "error reading log file",
            EnvironmentFailureReason::LogWrite => "error writing log file",
            EnvironmentFailureReason::LogIntegrity => "log integrity violation",
            EnvironmentFailureReason::LogIncomplete => "incomplete log entry",
            EnvironmentFailureReason::BtreeCorruption => {
                "B-tree structure corruption"
            }
            EnvironmentFailureReason::JavaError => "Java-specific error",
            EnvironmentFailureReason::LatchAlreadyHeld => {
                "latch already held by current thread"
            }
            EnvironmentFailureReason::LatchNotHeld => {
                "latch not held when expected"
            }
            EnvironmentFailureReason::ThreadInterrupted => {
                "thread was interrupted"
            }
            EnvironmentFailureReason::UncaughtException => {
                "uncaught exception occurred"
            }
            EnvironmentFailureReason::UnexpectedState => {
                "unexpected state (non-fatal)"
            }
            EnvironmentFailureReason::UnexpectedStateFatal => {
                "unexpected state (fatal)"
            }
            EnvironmentFailureReason::UnexpectedException => {
                "unexpected exception (non-fatal)"
            }
            EnvironmentFailureReason::UnexpectedExceptionFatal => {
                "unexpected exception (fatal)"
            }
            EnvironmentFailureReason::TestInvalidate => {
                "test-induced invalidation"
            }
            EnvironmentFailureReason::HardRecovery => "hard recovery required",
            EnvironmentFailureReason::ShutdownRequested => {
                "shutdown was requested"
            }
            EnvironmentFailureReason::Wedged => {
                "environment is wedged (unrecoverable)"
            }
        };
        write!(f, "{}", msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_invalidates_environment() {
        // Non-invalidating failures
        assert!(!EnvironmentFailureReason::EnvLocked.invalidates_environment());
        assert!(
            !EnvironmentFailureReason::EnvNotFound.invalidates_environment()
        );
        assert!(
            !EnvironmentFailureReason::LatchAlreadyHeld
                .invalidates_environment()
        );
        assert!(
            !EnvironmentFailureReason::LatchNotHeld.invalidates_environment()
        );
        assert!(
            !EnvironmentFailureReason::LogIntegrity.invalidates_environment()
        );
        assert!(
            !EnvironmentFailureReason::UnexpectedState
                .invalidates_environment()
        );
        assert!(
            !EnvironmentFailureReason::UnexpectedException
                .invalidates_environment()
        );

        // Invalidating failures
        assert!(
            EnvironmentFailureReason::LogChecksum.invalidates_environment()
        );
        assert!(EnvironmentFailureReason::LogWrite.invalidates_environment());
        assert!(
            EnvironmentFailureReason::BtreeCorruption.invalidates_environment()
        );
        assert!(
            EnvironmentFailureReason::UnexpectedStateFatal
                .invalidates_environment()
        );
        assert!(EnvironmentFailureReason::Wedged.invalidates_environment());
    }

    #[test]
    fn test_display() {
        assert_eq!(
            EnvironmentFailureReason::EnvLocked.to_string(),
            "environment locked by another process"
        );
        assert_eq!(
            EnvironmentFailureReason::LogChecksum.to_string(),
            "log file checksum error"
        );
        assert_eq!(
            EnvironmentFailureReason::BtreeCorruption.to_string(),
            "B-tree structure corruption"
        );
    }

    #[test]
    fn test_all_variants_display() {
        // Ensure all variants have display strings
        let reasons = vec![
            EnvironmentFailureReason::EnvLocked,
            EnvironmentFailureReason::EnvNotFound,
            EnvironmentFailureReason::LogChecksum,
            EnvironmentFailureReason::LogFileNotFound,
            EnvironmentFailureReason::LogRead,
            EnvironmentFailureReason::LogWrite,
            EnvironmentFailureReason::LogIntegrity,
            EnvironmentFailureReason::LogIncomplete,
            EnvironmentFailureReason::BtreeCorruption,
            EnvironmentFailureReason::JavaError,
            EnvironmentFailureReason::LatchAlreadyHeld,
            EnvironmentFailureReason::LatchNotHeld,
            EnvironmentFailureReason::ThreadInterrupted,
            EnvironmentFailureReason::UncaughtException,
            EnvironmentFailureReason::UnexpectedState,
            EnvironmentFailureReason::UnexpectedStateFatal,
            EnvironmentFailureReason::UnexpectedException,
            EnvironmentFailureReason::UnexpectedExceptionFatal,
            EnvironmentFailureReason::TestInvalidate,
            EnvironmentFailureReason::HardRecovery,
            EnvironmentFailureReason::ShutdownRequested,
            EnvironmentFailureReason::Wedged,
        ];

        for reason in &reasons {
            let display = reason.to_string();
            assert!(!display.is_empty());
        }
    }

    #[test]
    fn test_equality() {
        assert_eq!(
            EnvironmentFailureReason::LogChecksum,
            EnvironmentFailureReason::LogChecksum
        );
        assert_ne!(
            EnvironmentFailureReason::LogChecksum,
            EnvironmentFailureReason::LogWrite
        );
    }
}
