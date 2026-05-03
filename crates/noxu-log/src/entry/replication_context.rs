//! Replication context.
//!
//! Port of `com.sleepycat.je.log.ReplicationContext`.
//!
//! Provides context about high-level operations so the logging level can
//! determine which replication-related actions are required for a given
//! log entry.

use noxu_util::vlsn::Vlsn;

/// Replication context.
///
/// Indicates whether a log entry is part of the replication stream and
/// provides the VLSN (Version Log Sequence Number) context for replication.
///
/// # Variants
///
/// - `NoReplicate`: This operation will not be replicated (read-only, local,
///   or entry type never replicated)
/// - `Master`: This operation is on a replication master and should generate
///   a new VLSN
/// - `Client`: This operation is on a replica applying a replicated entry
///   with the given VLSN
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum ReplicationContext {
    /// Not replicated.
    #[default]
    NoReplicate,
    /// Replicated master operation (will generate VLSN).
    Master,
    /// Replicated client operation (applying entry with given VLSN).
    Client {
        /// The VLSN from the replication message.
        vlsn: Vlsn,
    },
}

impl ReplicationContext {
    /// Creates a context for a non-replicated operation.
    pub const fn no_replicate() -> Self {
        ReplicationContext::NoReplicate
    }

    /// Creates a context for a master operation.
    pub const fn master() -> Self {
        ReplicationContext::Master
    }

    /// Creates a context for a client operation with the given VLSN.
    pub fn client(vlsn: Vlsn) -> Self {
        ReplicationContext::Client { vlsn }
    }

    /// Returns true if this operation is in the replication stream.
    pub fn in_replication_stream(&self) -> bool {
        matches!(
            self,
            ReplicationContext::Master | ReplicationContext::Client { .. }
        )
    }

    /// Returns true if this node should generate a VLSN for this operation.
    pub fn must_generate_vlsn(&self) -> bool {
        matches!(self, ReplicationContext::Master)
    }

    /// Returns the client VLSN if this is a client operation.
    pub fn client_vlsn(&self) -> Option<Vlsn> {
        match self {
            ReplicationContext::Client { vlsn } => Some(*vlsn),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_replicate() {
        let ctx = ReplicationContext::no_replicate();
        assert!(!ctx.in_replication_stream());
        assert!(!ctx.must_generate_vlsn());
        assert_eq!(ctx.client_vlsn(), None);
    }

    #[test]
    fn test_master() {
        let ctx = ReplicationContext::master();
        assert!(ctx.in_replication_stream());
        assert!(ctx.must_generate_vlsn());
        assert_eq!(ctx.client_vlsn(), None);
    }

    #[test]
    fn test_client() {
        let vlsn = Vlsn::new(42);
        let ctx = ReplicationContext::client(vlsn);
        assert!(ctx.in_replication_stream());
        assert!(!ctx.must_generate_vlsn());
        assert_eq!(ctx.client_vlsn(), Some(vlsn));
    }

    #[test]
    fn test_default() {
        let ctx = ReplicationContext::default();
        assert_eq!(ctx, ReplicationContext::NoReplicate);
    }
}
