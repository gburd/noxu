//! Peer authentication and authorisation for replication.
//!
//! Replication in v1.4.x and earlier had no authentication on
//! the wire (see `docs/src/internal/security-review-2026-05.md`,
//! finding NA-1). The mTLS-by-default plan
//! (`docs/src/internal/auth-mtls-design-2026-05.md`) closes
//! NA-1 / NA-2 / NA-3 / NA-4 / NA-8 / TLS-1 by:
//!
//!   1. Requiring TLS on the dispatcher's transport.
//!   2. Using rustls's standard chain verification (CA-rooted).
//!   3. **Plus this module's `PeerAllowlistVerifier`** (Phase 2),
//!      which runs after chain validation succeeds and confirms
//!      that the peer's leaf-certificate subject names are in a
//!      configured allowlist.
//!
//! This file is **Phase 1**: the allowlist matching logic in
//! isolation, with unit tests over plain Rust. Phase 2 wires
//! the verifier through the dispatcher and the rustls
//! `ServerConfig` / `ClientConfig`. Phase 2 has not landed yet.
//!
//! ## What goes in the allowlist
//!
//! Each allowlist entry is matched against the peer cert's
//! subject Common Name (CN) AND each Subject Alternative Name
//! (SAN) DNS entry. Matching is case-insensitive. Wildcards
//! (`*.cluster.example`) are NOT supported in v1.5.0 — the
//! allowlist is a literal set of expected names; a wildcard
//! would weaken the security boundary by accepting any cert
//! the operator's CA happens to sign with a name in that
//! domain.
//!
//! ## Why subject-based and not pinning the cert hash
//!
//! Cert pinning (storing a SHA-256 of the peer's leaf cert) is
//! more restrictive but breaks rotation: rotating any peer's
//! cert requires updating every other peer's pinned-hash list.
//! Subject-based authorisation lets the operator rotate certs
//! freely under the same CA without touching the allowlist.

use std::collections::BTreeSet;

/// Membership policy: which peer subject names are allowed to
/// participate in the replication group.
///
/// Construct via [`PeerAllowlist::new`] from a list of
/// expected subject names. Names are normalised to lowercase
/// at construction time so [`PeerAllowlist::contains`] is
/// case-insensitive.
#[derive(Clone, Debug, Default)]
pub struct PeerAllowlist {
    /// Lowercased, deduplicated subject names.
    allowed: BTreeSet<String>,
}

impl PeerAllowlist {
    /// Build an allowlist from any iterable of subject-name
    /// strings. Names are stored lowercased; duplicates and
    /// empty strings are filtered out.
    ///
    /// An allowlist with zero entries means "no peer is
    /// authorised", which is a valid (if useless) state — the
    /// caller should treat zero-entry allowlists as a
    /// configuration error before constructing the verifier.
    pub fn new<I, S>(names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let allowed = names
            .into_iter()
            .filter_map(|s| {
                let s = s.as_ref().trim();
                if s.is_empty() { None } else { Some(s.to_ascii_lowercase()) }
            })
            .collect();
        Self { allowed }
    }

    /// Number of unique entries in the allowlist.
    pub fn len(&self) -> usize {
        self.allowed.len()
    }

    /// `true` iff the allowlist is empty (no peers
    /// authorised).
    pub fn is_empty(&self) -> bool {
        self.allowed.is_empty()
    }

    /// `true` iff `name` is exactly equal to some entry,
    /// case-insensitive. Wildcards are NOT supported.
    pub fn contains(&self, name: &str) -> bool {
        self.allowed.contains(&name.trim().to_ascii_lowercase())
    }

    /// `true` iff ANY of `names` is in the allowlist. The
    /// caller passes every name extracted from the peer cert
    /// (subject CN + each SAN DNS entry); membership is
    /// granted if at least one matches.
    pub fn contains_any<I, S>(&self, names: I) -> bool
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        names.into_iter().any(|n| self.contains(n.as_ref()))
    }

    /// Read-only iterator over the lowercased entries. Order
    /// is `BTreeSet` order (lexicographic).
    pub fn iter(&self) -> impl Iterator<Item = &str> {
        self.allowed.iter().map(String::as_str)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_allowlist_admits_no_one() {
        let al = PeerAllowlist::default();
        assert!(al.is_empty());
        assert!(!al.contains("anyone"));
        assert!(!al.contains_any(["a", "b", "c"]));
    }

    #[test]
    fn case_insensitive_match() {
        let al = PeerAllowlist::new(["node-1.cluster.example"]);
        assert!(al.contains("node-1.cluster.example"));
        assert!(al.contains("Node-1.Cluster.Example"));
        assert!(al.contains("NODE-1.CLUSTER.EXAMPLE"));
        assert!(!al.contains("node-2.cluster.example"));
    }

    #[test]
    fn whitespace_and_empties_filtered() {
        let al = PeerAllowlist::new(["  node-1  ", "", "   ", "node-2"]);
        assert_eq!(al.len(), 2);
        assert!(al.contains("node-1"));
        assert!(al.contains("node-2"));
    }

    #[test]
    fn no_wildcard_match() {
        // *.cluster.example must NOT match node-7.cluster.example —
        // wildcards are deliberately unsupported.
        let al = PeerAllowlist::new(["*.cluster.example"]);
        assert!(!al.contains("node-7.cluster.example"));
        // The literal "*.cluster.example" string still matches
        // itself, which is fine (and useless): the rustls cert
        // verifier never produces a SAN that contains a literal
        // asterisk.
        assert!(al.contains("*.cluster.example"));
    }

    #[test]
    fn duplicates_collapsed() {
        let al = PeerAllowlist::new(["node-1", "NODE-1", " node-1 "]);
        assert_eq!(al.len(), 1);
    }

    #[test]
    fn contains_any_admits_first_matching() {
        let al = PeerAllowlist::new(["node-2"]);
        assert!(al.contains_any(["nope", "node-2", "another"]));
        assert!(!al.contains_any(["nope", "another"]));
    }

    #[test]
    fn iter_yields_sorted_lowercase_entries() {
        let al = PeerAllowlist::new(["beta", "ALPHA", "Charlie"]);
        let v: Vec<&str> = al.iter().collect();
        assert_eq!(v, vec!["alpha", "beta", "charlie"]);
    }

    #[test]
    fn contains_trims_input_whitespace() {
        let al = PeerAllowlist::new(["node-1"]);
        assert!(al.contains("  node-1  "));
        assert!(al.contains("\tnode-1\n"));
    }

    #[test]
    fn allowlist_clone_is_independent() {
        let al1 = PeerAllowlist::new(["a", "b"]);
        let al2 = al1.clone();
        assert_eq!(al1.len(), al2.len());
        assert!(al1.contains("a") && al2.contains("a"));
    }
}
