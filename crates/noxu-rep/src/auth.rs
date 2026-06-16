//! Peer authentication and authorisation for replication.
//!
//! Replication in v1.4.x and earlier had no authentication on
//! the wire (see the 2026 review,
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
//! **Phase 1** added the allowlist matching logic in isolation.
//! **Phase 2** (v3.1.0) wires the verifier through the rustls
//! `ServerConfig` via `PeerAllowlistVerifier` and updates the
//! client config to present a client certificate.  Both changes
//! are guarded by the `tls-rustls` feature flag.
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

// ─── Cert name extraction (tls-rustls only) ─────────────────────────────────

/// Minimal X.509 DER parser: decode a tag-length prefix.
///
/// Returns `(length, bytes_consumed_for_length_encoding)` or `None` if
/// the slice is too short or uses an unsupported form.
#[cfg(feature = "tls-rustls")]
fn der_decode_len(data: &[u8]) -> Option<(usize, usize)> {
    let first = *data.first()? as usize;
    if first < 0x80 {
        Some((first, 1))
    } else {
        let n = first & 0x7F;
        // Reject indefinite-length (n==0) and lengths > 4 bytes (> 4 GiB).
        if n == 0 || n > 4 || data.len() < 1 + n {
            return None;
        }
        let mut len = 0usize;
        for &b in &data[1..1 + n] {
            len = (len << 8) | (b as usize);
        }
        Some((len, 1 + n))
    }
}

/// Parse one DER TLV: returns `(tag, value_slice, remaining_after_TLV)`.
#[cfg(feature = "tls-rustls")]
fn der_tlv(data: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    if data.is_empty() {
        return None;
    }
    let tag = data[0];
    let (len, consumed) = der_decode_len(&data[1..])?;
    let start = 1 + consumed;
    if data.len() < start + len {
        return None;
    }
    Some((tag, &data[start..start + len], &data[start + len..]))
}

/// Extract lowercase subject names from a leaf certificate's DER bytes.
///
/// Returns every name the verifier should check against the allowlist:
/// - Subject Common Name (OID 2.5.4.3, any DirectoryString encoding).
/// - DNS Subject Alternative Names (GeneralName `[2] IMPLICIT IA5String`).
///
/// This is a focused, conservative parser that only touches the fields it
/// needs and ignores everything else.  Malformed input silently yields
/// whatever names have been collected so far — an unparseable cert
/// produces an empty list and therefore **fails** the allowlist check
/// (fail-closed).
#[cfg(feature = "tls-rustls")]
pub(crate) fn extract_cert_names(cert_der: &[u8]) -> Vec<String> {
    try_extract_cert_names(cert_der).unwrap_or_default()
}

/// Public re-export of `extract_cert_names` for integration tests.
///
/// This function is only available under the `tls-rustls` feature and is
/// intended for use in `tests/` integration tests that verify the DER
/// cert-name parser in isolation.
#[cfg(feature = "tls-rustls")]
pub fn extract_cert_names_for_test(cert_der: &[u8]) -> Vec<String> {
    extract_cert_names(cert_der)
}

#[cfg(feature = "tls-rustls")]
fn try_extract_cert_names(cert_der: &[u8]) -> Option<Vec<String>> {
    let mut names: Vec<String> = Vec::new();

    // Certificate is SEQUENCE { TBSCertificate, signatureAlg, signatureBits }.
    let (0x30, cert_body, _) = der_tlv(cert_der)? else {
        return Some(names);
    };
    // First element of Certificate body is TBSCertificate (SEQUENCE).
    let (0x30, tbs, _) = der_tlv(cert_body)? else {
        return Some(names);
    };

    let mut p = tbs;

    // Skip optional version [0] EXPLICIT (tag 0xA0).
    if let Some((0xA0, _, rest)) = der_tlv(p) {
        p = rest;
    }
    // Skip serialNumber INTEGER (tag 0x02).
    let (0x02, _, rest) = der_tlv(p)? else {
        return Some(names);
    };
    p = rest;
    // Skip signature AlgorithmIdentifier (SEQUENCE, tag 0x30).
    let (0x30, _, rest) = der_tlv(p)? else {
        return Some(names);
    };
    p = rest;
    // Skip issuer Name (SEQUENCE, tag 0x30).
    let (0x30, _, rest) = der_tlv(p)? else {
        return Some(names);
    };
    p = rest;
    // Skip validity (SEQUENCE, tag 0x30).
    let (0x30, _, rest) = der_tlv(p)? else {
        return Some(names);
    };
    p = rest;

    // Parse subject Name (SEQUENCE, tag 0x30) — extract CN.
    let (0x30, subject, rest) = der_tlv(p)? else {
        return Some(names);
    };
    p = rest;
    // Walk RDNs: each RDN is SET (0x31) containing ATVs.
    let mut rdns = subject;
    while let Some((0x31, rdn_val, rest2)) = der_tlv(rdns) {
        rdns = rest2;
        let mut atvs = rdn_val;
        while let Some((0x30, atv, rest3)) = der_tlv(atvs) {
            atvs = rest3;
            // ATV: OID (0x06) + DirectoryString value.
            if let Some((0x06, oid_bytes, val_rest)) = der_tlv(atv)
                && oid_bytes == [0x55, 0x04, 0x03]
            {
                // Accept any DirectoryString variant:
                // UTF8String(0x0C), PrintableString(0x13),
                // TeletexString(0x14), IA5String(0x16), BMPString(0x1E).
                if let Some((_vtag, vval, _)) = der_tlv(val_rest)
                    && let Ok(s) = std::str::from_utf8(vval)
                    && !s.is_empty()
                {
                    names.push(s.to_ascii_lowercase());
                }
            }
        }
    }

    // Skip subjectPublicKeyInfo (SEQUENCE, tag 0x30).
    let (0x30, _, rest) = der_tlv(p)? else {
        return Some(names);
    };
    p = rest;

    // Skip optional issuerUniqueID [1] and subjectUniqueID [2].
    if let Some((0x81, _, rest2)) = der_tlv(p) {
        p = rest2;
    }
    if let Some((0x82, _, rest2)) = der_tlv(p) {
        p = rest2;
    }

    // Look for [3] EXPLICIT Extensions (tag 0xA3).
    while let Some((tag, val, rest)) = der_tlv(p) {
        p = rest;
        if tag != 0xA3 {
            continue;
        }
        // Extensions are a SEQUENCE inside the [3] wrapper.
        let (0x30, exts_body, _) = der_tlv(val)? else {
            break;
        };
        let mut ext_p = exts_body;
        while let Some((0x30, ext, rest2)) = der_tlv(ext_p) {
            ext_p = rest2;
            // Each Extension: SEQUENCE { OID, [critical BOOLEAN,] OCTET STRING }.
            let (0x06, oid_bytes, ext_rest) = der_tlv(ext)? else {
                continue;
            };
            // OID 2.5.29.17 (id-ce-subjectAltName) = 0x55 0x1D 0x11.
            if oid_bytes != [0x55, 0x1D, 0x11] {
                continue;
            }
            // Skip optional critical BOOLEAN (tag 0x01).
            let san_octet_rest = if ext_rest.first() == Some(&0x01) {
                der_tlv(ext_rest).map(|(_, _, r)| r).unwrap_or(ext_rest)
            } else {
                ext_rest
            };
            // OCTET STRING wrapping the actual SAN value.
            let (0x04, octet_val, _) = der_tlv(san_octet_rest)? else {
                continue;
            };
            // SubjectAltName ::= SEQUENCE OF GeneralName.
            let (0x30, san_seq, _) = der_tlv(octet_val)? else {
                continue;
            };
            let mut san_p = san_seq;
            while let Some((gtag, gval, rest3)) = der_tlv(san_p) {
                san_p = rest3;
                // dNSName = [2] IMPLICIT IA5String, tag byte = 0x82.
                if gtag == 0x82
                    && let Ok(s) = std::str::from_utf8(gval)
                    && !s.is_empty()
                {
                    names.push(s.to_ascii_lowercase());
                }
            }
        }
        break; // parsed extensions, stop
    }
    let _ = p; // silence unused-variable warning after the loop

    Some(names)
}

// ─── PeerAllowlistVerifier ───────────────────────────────────────────────────

/// A rustls [`ClientCertVerifier`] that enforces the `peer_allowlist`.
///
/// # Enforcement model
///
/// 1. **Chain validation** — delegates to rustls's built-in
///    `WebPkiClientVerifier` which validates the client certificate chain
///    against the configured CA trust anchors.  An expired, self-signed, or
///    wrong-CA cert is rejected before the allowlist check runs.
///
/// 2. **Allowlist check** — extracts the leaf certificate's Subject Common
///    Name (CN) and every DNS Subject Alternative Name (SAN).  At least one
///    of those names must match an entry in the configured
///    [`PeerAllowlist`] (case-insensitive, no wildcards).
///
/// # Construction
///
/// Returns an error if `allowlist` is empty.  An empty allowlist means "no
/// peer is authorised", which is almost certainly a misconfiguration.
/// Callers should validate the allowlist before calling `new`.
///
/// # Feature gate
///
/// Only available under the `tls-rustls` feature.
///
/// [`ClientCertVerifier`]: rustls::server::danger::ClientCertVerifier
#[cfg(feature = "tls-rustls")]
pub(crate) struct PeerAllowlistVerifier {
    inner: std::sync::Arc<dyn rustls::server::danger::ClientCertVerifier>,
    allowlist: PeerAllowlist,
}

#[cfg(feature = "tls-rustls")]
impl PeerAllowlistVerifier {
    /// Build a verifier from a root cert store and a non-empty allowlist.
    ///
    /// # Errors
    ///
    /// - `RepError::ConfigError` if `allowlist` is empty.
    /// - `RepError::ConfigError` if the `WebPkiClientVerifier` builder fails
    ///   (e.g. the root store is empty or malformed).
    pub(crate) fn new(
        root_store: std::sync::Arc<rustls::RootCertStore>,
        allowlist: PeerAllowlist,
    ) -> crate::error::Result<Self> {
        if allowlist.is_empty() {
            return Err(crate::error::RepError::ConfigError(
                "PeerAllowlistVerifier requires a non-empty allowlist; an \
                 empty allowlist means no peer is authorised, which is almost \
                 certainly a misconfiguration. Add at least one expected peer \
                 subject name."
                    .into(),
            ));
        }
        let provider =
            std::sync::Arc::new(rustls::crypto::ring::default_provider());
        let inner =
            rustls::server::WebPkiClientVerifier::builder_with_provider(
                root_store, provider,
            )
            .build()
            .map_err(|e| {
                crate::error::RepError::ConfigError(format!(
                    "PeerAllowlistVerifier: WebPkiClientVerifier build \
                     failed: {e}"
                ))
            })?;
        Ok(Self { inner, allowlist })
    }
}

#[cfg(feature = "tls-rustls")]
impl std::fmt::Debug for PeerAllowlistVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerAllowlistVerifier")
            .field("allowlist_len", &self.allowlist.len())
            .finish()
    }
}

#[cfg(feature = "tls-rustls")]
impl rustls::server::danger::ClientCertVerifier for PeerAllowlistVerifier {
    fn offer_client_auth(&self) -> bool {
        true
    }

    fn client_auth_mandatory(&self) -> bool {
        true
    }

    fn root_hint_subjects(&self) -> &[rustls::DistinguishedName] {
        self.inner.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &rustls::pki_types::CertificateDer<'_>,
        intermediates: &[rustls::pki_types::CertificateDer<'_>],
        now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<
        rustls::server::danger::ClientCertVerified,
        rustls::Error,
    > {
        // Step 1: CA-rooted chain validation via rustls WebPki.
        self.inner.verify_client_cert(end_entity, intermediates, now)?;

        // Step 2: extract CN + SAN DNS names from the leaf cert.
        let names = extract_cert_names(end_entity.as_ref());

        // Step 3: allowlist check — at least one name must match.
        if !self.allowlist.contains_any(&names) {
            let peer_names = if names.is_empty() {
                "<no names found in cert>".to_string()
            } else {
                names.join(", ")
            };
            log::warn!(
                "mTLS: rejecting peer — cert names [{}] not in allowlist",
                peer_names
            );
            return Err(rustls::Error::General(format!(
                "peer certificate names [{peer_names}] do not match any \
                 entry in the configured peer_allowlist"
            )));
        }

        log::debug!("mTLS: peer cert names {:?} admitted by allowlist", names);
        Ok(rustls::server::danger::ClientCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<
        rustls::client::danger::HandshakeSignatureValid,
        rustls::Error,
    > {
        self.inner.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &rustls::pki_types::CertificateDer<'_>,
        dss: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<
        rustls::client::danger::HandshakeSignatureValid,
        rustls::Error,
    > {
        self.inner.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.inner.supported_verify_schemes()
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
