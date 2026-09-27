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

    /// Order this token against `other` by commit VLSN, but ONLY when both
    /// were minted by the same replication group.
    ///
    /// Port of `com.sleepycat.je.CommitToken.compareTo` (which compares by
    /// `vlsn` and throws `IllegalArgumentException` when the `repenvUUID`s
    /// differ). JE's checked-exception contract maps to `Option<Ordering>`:
    /// `Some(ordering)` when the group identities match, `None` when they
    /// differ. A `None` result means the tokens are not comparable -- the
    /// same "you cannot order tokens from different groups" invariant JE
    /// enforces by throwing.
    ///
    /// We deliberately do NOT implement [`Ord`]: a total order would have
    /// to fabricate a result for the cross-group case JE rejects.
    pub fn try_compare(
        &self,
        other: &CommitToken,
    ) -> Option<std::cmp::Ordering> {
        if self.group != other.group {
            // JE: comparisons across environments are not meaningful.
            return None;
        }
        Some(self.vlsn.cmp(&other.vlsn))
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

    /// JE: `CommitTokenTest.testBasic`.
    ///
    /// Within one replication group, commit tokens are totally ordered by
    /// their VLSN (t1<t2<t3), equal tokens compare equal, and (JE's
    /// `IllegalArgumentException`) comparing tokens from DIFFERENT groups
    /// yields `None` (not comparable). Deviation: JE keys on a per-env
    /// `repenvUUID`; Noxu keys on the replication group name (documented in
    /// this module's header). The serialization round-trip JE also checks
    /// (java.io.Serializable) is N/A -- Noxu has no Java object
    /// serialization; `Clone`/`Eq` (test_eq_and_clone) cover value identity.
    #[test]
    fn commit_token_test_basic_ordering() {
        use std::cmp::Ordering;
        let t1 = CommitToken::new("g1", 1).unwrap();
        let t2 = CommitToken::new("g1", 2).unwrap();
        let t3 = CommitToken::new("g1", 3).unwrap();

        // t1<t2 && t2>t1, t2<t3 && t3>t2, t1<t3 && t3>t1.
        assert_eq!(t1.try_compare(&t2), Some(Ordering::Less));
        assert_eq!(t2.try_compare(&t1), Some(Ordering::Greater));
        assert_eq!(t2.try_compare(&t3), Some(Ordering::Less));
        assert_eq!(t3.try_compare(&t2), Some(Ordering::Greater));
        assert_eq!(t1.try_compare(&t3), Some(Ordering::Less));
        assert_eq!(t3.try_compare(&t1), Some(Ordering::Greater));

        // Equal tokens compare Equal (JE assertEquals + compareTo==0).
        let t1b = CommitToken::new("g1", 1).unwrap();
        assert_eq!(t1, t1b);
        assert_eq!(t1.try_compare(&t1b), Some(Ordering::Equal));

        // Cross-group comparison is NOT meaningful: JE throws
        // IllegalArgumentException; Noxu returns None.
        let other_group = CommitToken::new("g2", 1).unwrap();
        assert_eq!(
            t1.try_compare(&other_group),
            None,
            "tokens from different groups must be incomparable (JE throws)"
        );
    }
}
