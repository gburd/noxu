//! Commit tokens for commit-point read consistency.
//!
//! Port of `com.sleepycat.je.CommitToken` and `MasterTxn.getCommitToken`.
//!
//! A `CommitToken` is a bookmark into the master's serialized transaction
//! schedule: the VLSN of a committed transaction, tagged with the identity of
//! the replication environment that produced it.  A client that performs a
//! write on the master receives the token (`Transaction.getCommitToken`) and
//! can hand it to a replica read via
//! [`crate::ConsistencyPolicy::CommitPointConsistency`]; the replica then
//! blocks the read until it has replayed up to that VLSN (see
//! [`crate::ConsistencyTracker`]).
//!
//! JE keys the token on the replication-environment UUID
//! (`CommitToken.repenvUUID`) so a token minted by one group is rejected by
//! another.  We use the replication *group name* as the stable rep-env
//! identity for that same mismatch check (Noxu identifies a group by name; it
//! has no per-env UUID).

/// A bookmark identifying a specific committed transaction in the master's
/// replication stream.
///
/// Port of `com.sleepycat.je.CommitToken` (`{ repenvUUID, vlsn }`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CommitToken {
    /// Identity of the replication environment that produced this token.
    ///
    /// Port of `CommitToken.repenvUUID`; here the replication group name.
    group: String,
    /// The commit VLSN this token marks.
    ///
    /// Port of `CommitToken.vlsn`.
    vlsn: u64,
}

impl CommitToken {
    /// Create a commit token for `vlsn` produced by replication `group`.
    ///
    /// Port of `new CommitToken(envUUID, commitVLSN.getSequence())`
    /// (`MasterTxn.getCommitToken`).  Mirrors JE's invariant that the VLSN
    /// must not be NULL (0): a token with no commit VLSN is meaningless, so
    /// returns `None` rather than minting a bogus bookmark.
    pub fn new(group: impl Into<String>, vlsn: u64) -> Option<Self> {
        if vlsn == 0 {
            // CommitToken ctor: "the vlsn must not be null".
            return None;
        }
        Some(Self { group: group.into(), vlsn })
    }

    /// The replication-group identity that produced this token.
    ///
    /// Port of `CommitToken.getRepenvUUID`.
    pub fn group(&self) -> &str {
        &self.group
    }

    /// The commit VLSN this token marks.
    ///
    /// Port of `CommitToken.getVLSN`.
    pub fn vlsn(&self) -> u64 {
        self.vlsn
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_token() {
        let t = CommitToken::new("g1", 42).unwrap();
        assert_eq!(t.group(), "g1");
        assert_eq!(t.vlsn(), 42);
    }

    #[test]
    fn test_null_vlsn_rejected() {
        // CommitToken ctor rejects a NULL (0) VLSN.
        assert!(CommitToken::new("g1", 0).is_none());
    }

    #[test]
    fn test_eq_and_clone() {
        let a = CommitToken::new("g1", 7).unwrap();
        let b = a.clone();
        assert_eq!(a, b);
        let c = CommitToken::new("g2", 7).unwrap();
        assert_ne!(a, c);
    }
}
