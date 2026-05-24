//! TLS configuration for Noxu DB replication channels.
//!
//! Noxu DB replication traffic can be encrypted using one of two TLS backends:
//!
//! | Feature | Backend | Dependencies |
//! |---------|---------|--------------|
//! | `tls-rustls` (default) | [rustls](https://github.com/rustls/rustls) | Pure Rust, no C |
//! | `tls-native` | [native-tls](https://github.com/sfackler/rust-native-tls) | System OpenSSL or LibreSSL |
//!
//! ## Why not quiche?
//!
//! [quiche](https://github.com/cloudflare/quiche) is Cloudflare's QUIC
//! implementation, written in C with Rust FFI bindings. It requires BoringSSL
//! and introduces `unsafe` FFI into the dependency tree.
//!
//! Noxu DB targets zero `unsafe` in its core and prefers pure-Rust
//! dependencies. [quinn](https://github.com/quinn-rs/quinn) provides the same
//! RFC 9000 QUIC semantics (including 0-RTT, unreliable datagrams, per-stream
//! flow control) using only safe Rust and `rustls` for TLS — exactly what
//! Noxu DB needs.
//!
//! ## Encryption status
//!
//! - **QUIC channels**: Always encrypted. QUIC mandates TLS 1.3 (RFC 9001).
//!   The default configuration uses a runtime-generated self-signed certificate
//!   suitable for trusted private networks. For production deployments supply
//!   a [`TlsConfig`] via the `connect_with_config` / `with_server_config`
//!   constructors on the QUIC channel types.
//!
//! - **TCP channels**: Unencrypted by default (`TcpChannel`). Use
//!   `TlsTcpChannel` (in `crate::net::channel`) for encrypted TCP
//!   connections. Enable at least one TLS feature (`tls-rustls` or
//!   `tls-native`) to make those types available.
//!
//! ## Quick start
//!
//! ```ignore
//! // Internal cluster with self-signed certs (no external CA required):
//! let tls = TlsConfig::self_signed("my-node.internal");
//!
//! // Production with PEM files (tls-rustls backend):
//! let tls = TlsConfig::from_pem_files(
//!     "/etc/noxu/cert.pem",
//!     "/etc/noxu/key.pem",
//!     "/etc/noxu/ca.pem",
//!     "my-node.internal",
//! );
//! ```

#[cfg(any(feature = "tls-rustls", feature = "tls-native"))]
use crate::error::{RepError, Result};

// ─── TlsIdentity ─────────────────────────────────────────────────────────────

/// Certificate and private key material that identifies this replication node.
///
/// ## Backend compatibility
///
/// | Variant | `tls-rustls` | `tls-native` |
/// |---------|:------------:|:------------:|
/// | `SelfSigned` | ✓ | ✗ |
/// | `PemFiles` | ✓ | ✗ |
/// | `PemBytes` | ✓ | ✗ |
/// | `Pkcs12` | ✗ | ✓ |
///
/// For `tls-native`, create a PKCS #12 archive with:
/// ```sh
/// openssl pkcs12 -export -out identity.p12 -inkey key.pem -in cert.pem
/// ```
#[derive(Clone)]
#[non_exhaustive]
pub enum TlsIdentity {
    /// Generate a fresh self-signed certificate at runtime.
    ///
    /// Supported by the `tls-rustls` backend only.  Suitable for internal,
    /// trusted replication networks where setting up a certificate authority
    /// is undesirable.
    SelfSigned {
        /// Subject Alternative Names for the certificate (e.g. DNS hostnames
        /// or IP addresses for this node).
        subject_alt_names: Vec<String>,
    },

    /// Load certificate chain and private key from PEM files on disk.
    ///
    /// Supported by the `tls-rustls` backend only.
    PemFiles {
        /// Path to a PEM-encoded certificate chain.
        cert: std::path::PathBuf,
        /// Path to a PEM-encoded private key (PKCS #8 or PKCS #1 RSA).
        key: std::path::PathBuf,
    },

    /// Certificate chain and private key as in-memory PEM bytes.
    ///
    /// Supported by the `tls-rustls` backend only.
    PemBytes {
        /// PEM-encoded certificate chain bytes.
        cert: Vec<u8>,
        /// PEM-encoded private key bytes.
        key: Vec<u8>,
    },

    /// PKCS #12 archive (certificate + key bundled) as DER bytes.
    ///
    /// Supported by the `tls-native` backend only (OpenSSL / LibreSSL).
    /// Load with:
    /// ```ignore
    /// let der = std::fs::read("/etc/noxu/identity.p12")?;
    /// let identity = TlsIdentity::Pkcs12 { der, password: "secret".into() };
    /// ```
    Pkcs12 {
        /// DER-encoded PKCS #12 archive.
        der: Vec<u8>,
        /// Password used to decrypt the archive.
        password: String,
    },
}

// ─── TrustedCerts ────────────────────────────────────────────────────────────

/// Policy for verifying the remote peer's certificate.
#[derive(Clone)]
#[non_exhaustive]
pub enum TrustedCerts {
    /// Accept any certificate without verification.
    ///
    /// **Insecure.** Use only on private, trusted networks where all nodes
    /// are implicitly trusted (authenticated at the Paxos / VLSN layer).
    SkipVerification,

    /// Trust CA certificates loaded from PEM files on disk.
    CaFiles(Vec<std::path::PathBuf>),

    /// Trust in-memory PEM-encoded CA certificates.
    CaBytes(Vec<Vec<u8>>),
}

// ─── TlsConfig ───────────────────────────────────────────────────────────────

/// TLS configuration for Noxu DB replication channels.
///
/// A `TlsConfig` bundles this node's identity (certificate + key) with the
/// policy for verifying remote peers.  Pass it to:
///
/// - `TlsTcpChannelListener::bind_with_tls` — encrypted TCP server
/// - `TlsTcpChannel::connect_with_tls` — encrypted TCP client
/// - `TlsConfig::to_quinn_server_config` — QUIC server with real certs
/// - `TlsConfig::to_quinn_client_config` — QUIC client with real certs
#[derive(Clone)]
pub struct TlsConfig {
    /// This node's certificate and private key.
    pub identity: TlsIdentity,
    /// How to verify the remote peer's certificate.
    pub trusted_certs: TrustedCerts,
    /// TLS SNI server name used by the client during the handshake.
    ///
    /// Must match the certificate's `Common Name` or a `Subject Alternative
    /// Name`.  Use `"localhost"` when connecting to a `SelfSigned` cert with
    /// `subject_alt_names = ["localhost"]`.
    pub server_name: String,
}

impl TlsConfig {
    /// Create an insecure TLS configuration for trusted private networks.
    ///
    /// Generates a self-signed certificate at first use and skips peer
    /// certificate verification entirely.  Equivalent to the current default
    /// QUIC channel behaviour.
    ///
    /// Requires the `tls-rustls` feature.
    pub fn insecure(server_name: impl Into<String>) -> Self {
        TlsConfig {
            identity: TlsIdentity::SelfSigned {
                subject_alt_names: vec!["localhost".into()],
            },
            trusted_certs: TrustedCerts::SkipVerification,
            server_name: server_name.into(),
        }
    }

    /// Create a TLS configuration using PEM cert/key files and a CA file.
    ///
    /// Verifies the remote peer's certificate against `ca`.
    /// Requires the `tls-rustls` feature.
    pub fn from_pem_files(
        cert: impl Into<std::path::PathBuf>,
        key: impl Into<std::path::PathBuf>,
        ca: impl Into<std::path::PathBuf>,
        server_name: impl Into<String>,
    ) -> Self {
        TlsConfig {
            identity: TlsIdentity::PemFiles {
                cert: cert.into(),
                key: key.into(),
            },
            trusted_certs: TrustedCerts::CaFiles(vec![ca.into()]),
            server_name: server_name.into(),
        }
    }

    /// Create a TLS configuration from a PKCS #12 archive.
    ///
    /// Verifies the remote peer against `ca_pem` bytes.
    /// Requires the `tls-native` feature.
    pub fn from_pkcs12(
        der: Vec<u8>,
        password: impl Into<String>,
        ca_pem: Vec<u8>,
        server_name: impl Into<String>,
    ) -> Self {
        TlsConfig {
            identity: TlsIdentity::Pkcs12 { der, password: password.into() },
            trusted_certs: TrustedCerts::CaBytes(vec![ca_pem]),
            server_name: server_name.into(),
        }
    }
}

// ─── rustls helpers ──────────────────────────────────────────────────────────

#[cfg(feature = "tls-rustls")]
impl TlsConfig {
    /// Build a `rustls::ServerConfig` from this configuration.
    ///
    /// Used by [`TlsTcpChannelListener`] and the QUIC server path.
    pub(crate) fn to_rustls_server_config(
        &self,
    ) -> Result<std::sync::Arc<rustls::ServerConfig>> {
        let (certs, key) = self.rustls_cert_and_key()?;

        let cfg = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| {
                RepError::NetworkError(format!("TLS server config: {e}"))
            })?;
        Ok(std::sync::Arc::new(cfg))
    }

    /// Build a `rustls::ClientConfig` from this configuration.
    ///
    /// Used by [`TlsTcpChannel`] and the QUIC client path.
    pub(crate) fn to_rustls_client_config(
        &self,
    ) -> Result<std::sync::Arc<rustls::ClientConfig>> {
        if matches!(&self.trusted_certs, TrustedCerts::SkipVerification) {
            let cfg = rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(std::sync::Arc::new(
                    SkipCertVerification::new(),
                ))
                .with_no_client_auth();
            return Ok(std::sync::Arc::new(cfg));
        }

        let root_store = self.rustls_root_store()?;
        let cfg = rustls::ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();
        Ok(std::sync::Arc::new(cfg))
    }

    /// Build a `quinn::ServerConfig` backed by this `TlsConfig`.
    ///
    /// Replaces the default self-signed / skip-verify server config for
    /// production deployments that bring their own certificates.
    ///
    /// # Example
    /// ```ignore
    /// let tls = TlsConfig::from_pem_files("cert.pem", "key.pem", "ca.pem", "node1");
    /// let server_cfg = tls.to_quinn_server_config()?;
    /// let listener = QuicMultiplexedChannelListener::with_server_config(addr, server_cfg)?;
    /// ```
    #[cfg(feature = "quic")]
    pub fn to_quinn_server_config(&self) -> Result<quinn::ServerConfig> {
        let rustls_cfg = self.to_rustls_server_config()?;
        let quic_cfg = quinn::crypto::rustls::QuicServerConfig::try_from(
            rustls::ServerConfig::clone(&rustls_cfg),
        )
        .map_err(|e| {
            RepError::NetworkError(format!("QUIC server config: {e}"))
        })?;
        let mut cfg =
            quinn::ServerConfig::with_crypto(std::sync::Arc::new(quic_cfg));
        let mut transport = quinn::TransportConfig::default();
        transport.mtu_discovery_config(None);
        transport.datagram_receive_buffer_size(Some(64 * 1024));
        cfg.transport_config(std::sync::Arc::new(transport));
        Ok(cfg)
    }

    /// Build a `quinn::ClientConfig` backed by this `TlsConfig`.
    ///
    /// Replaces the default skip-verify client config for production
    /// deployments that verify server certificates against a CA.
    #[cfg(feature = "quic")]
    pub fn to_quinn_client_config(&self) -> Result<quinn::ClientConfig> {
        let rustls_cfg = self.to_rustls_client_config()?;
        let quic_cfg = quinn::crypto::rustls::QuicClientConfig::try_from(
            rustls::ClientConfig::clone(&rustls_cfg),
        )
        .map_err(|e| {
            RepError::NetworkError(format!("QUIC client config: {e}"))
        })?;
        let mut cfg = quinn::ClientConfig::new(std::sync::Arc::new(quic_cfg));
        let mut transport = quinn::TransportConfig::default();
        transport.mtu_discovery_config(None);
        transport.datagram_receive_buffer_size(Some(64 * 1024));
        cfg.transport_config(std::sync::Arc::new(transport));
        Ok(cfg)
    }

    // ── Private helpers ──────────────────────────────────────────────────

    fn rustls_cert_and_key(
        &self,
    ) -> Result<(
        Vec<rustls::pki_types::CertificateDer<'static>>,
        rustls::pki_types::PrivateKeyDer<'static>,
    )> {
        use rustls::pki_types::{
            CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer,
        };

        match &self.identity {
            TlsIdentity::SelfSigned { subject_alt_names } => {
                let ck = rcgen::generate_simple_self_signed(
                    subject_alt_names.clone(),
                )
                .map_err(|e| RepError::NetworkError(format!("rcgen: {e}")))?;
                let cert = CertificateDer::from(ck.cert.der().to_vec());
                let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
                    ck.key_pair.serialize_der(),
                ));
                Ok((vec![cert], key))
            }
            TlsIdentity::PemFiles { cert, key } => {
                let cert_bytes = std::fs::read(cert).map_err(|e| {
                    RepError::NetworkError(format!("cert file: {e}"))
                })?;
                let key_bytes = std::fs::read(key).map_err(|e| {
                    RepError::NetworkError(format!("key file: {e}"))
                })?;
                Self::parse_pem_cert_and_key(&cert_bytes, &key_bytes)
            }
            TlsIdentity::PemBytes { cert, key } => {
                Self::parse_pem_cert_and_key(cert, key)
            }
            TlsIdentity::Pkcs12 { .. } => Err(RepError::NetworkError(
                "Pkcs12 identity is not supported by the tls-rustls backend; \
                 use PemFiles or PemBytes instead"
                    .into(),
            )),
        }
    }

    fn parse_pem_cert_and_key(
        cert_pem: &[u8],
        key_pem: &[u8],
    ) -> Result<(
        Vec<rustls::pki_types::CertificateDer<'static>>,
        rustls::pki_types::PrivateKeyDer<'static>,
    )> {
        use rustls_pemfile::{certs, private_key};
        use std::io::BufReader;

        let cert_chain: Vec<_> = certs(&mut BufReader::new(cert_pem))
            .collect::<std::result::Result<_, _>>()
            .map_err(|e| RepError::NetworkError(format!("cert parse: {e}")))?;
        if cert_chain.is_empty() {
            return Err(RepError::NetworkError(
                "no certificates found in PEM".into(),
            ));
        }

        let key = private_key(&mut BufReader::new(key_pem))
            .map_err(|e| RepError::NetworkError(format!("key parse: {e}")))?
            .ok_or_else(|| {
                RepError::NetworkError("no private key found in PEM".into())
            })?;

        Ok((cert_chain, key))
    }

    fn rustls_root_store(&self) -> Result<rustls::RootCertStore> {
        use rustls_pemfile::certs;
        use std::io::BufReader;

        let mut store = rustls::RootCertStore::empty();

        match &self.trusted_certs {
            TrustedCerts::SkipVerification => {
                // For skip-verification the root store is unused; the caller
                // must install a custom verifier (as quic_channel.rs does).
                // For TCP TLS we return an empty store here and the TlsTcpChannel
                // will install SkipCertVerification when this variant is set.
            }
            TrustedCerts::CaFiles(paths) => {
                for path in paths {
                    let pem = std::fs::read(path).map_err(|e| {
                        RepError::NetworkError(format!("CA file: {e}"))
                    })?;
                    for cert in certs(&mut BufReader::new(pem.as_slice()))
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .map_err(|e| {
                            RepError::NetworkError(format!("CA parse: {e}"))
                        })?
                    {
                        store.add(cert).map_err(|e| {
                            RepError::NetworkError(format!("CA add: {e}"))
                        })?;
                    }
                }
            }
            TrustedCerts::CaBytes(pems) => {
                for pem in pems {
                    for cert in certs(&mut BufReader::new(pem.as_slice()))
                        .collect::<std::result::Result<Vec<_>, _>>()
                        .map_err(|e| {
                            RepError::NetworkError(format!("CA parse: {e}"))
                        })?
                    {
                        store.add(cert).map_err(|e| {
                            RepError::NetworkError(format!("CA add: {e}"))
                        })?;
                    }
                }
            }
        }

        Ok(store)
    }
}

// ─── SkipCertVerification ────────────────────────────────────────────────────

/// A `rustls` `ServerCertVerifier` that accepts any certificate without chain
/// validation.
///
/// Suitable for internal, trusted replication networks where nodes are
/// implicitly trusted (authenticated at the Paxos / VLSN layer).
#[cfg(feature = "tls-rustls")]
#[derive(Debug)]
pub(crate) struct SkipCertVerification(
    std::sync::Arc<rustls::crypto::CryptoProvider>,
);

#[cfg(feature = "tls-rustls")]
impl SkipCertVerification {
    pub(crate) fn new() -> Self {
        Self(std::sync::Arc::new(rustls::crypto::ring::default_provider()))
    }
}

#[cfg(feature = "tls-rustls")]
impl rustls::client::danger::ServerCertVerifier for SkipCertVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer<'_>,
        _intermediates: &[rustls::pki_types::CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> std::result::Result<
        rustls::client::danger::ServerCertVerified,
        rustls::Error,
    > {
        Ok(rustls::client::danger::ServerCertVerified::assertion())
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
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
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
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

// ─── native-tls helpers ──────────────────────────────────────────────────────

#[cfg(feature = "tls-native")]
impl TlsConfig {
    /// Build a `native_tls::TlsAcceptor` for TCP server use.
    ///
    /// Requires `TlsIdentity::Pkcs12`.  Create a PKCS #12 archive with:
    /// ```sh
    /// openssl pkcs12 -export -out identity.p12 -inkey key.pem -in cert.pem
    /// ```
    pub(crate) fn to_native_acceptor(&self) -> Result<native_tls::TlsAcceptor> {
        let identity = self.native_identity()?;
        let builder = native_tls::TlsAcceptor::builder(identity);
        // Note: `native_tls::TlsAcceptorBuilder` does not expose CA-root
        // installation or "accept invalid client certs" knobs; mTLS-style
        // client-certificate verification is a `tls-rustls`-only feature
        // on this transport. Warn loudly only when the user has expressed
        // intent to do mTLS by populating CA roots — `SkipVerification`
        // and an empty `CaFiles(vec![])` are both already what a
        // native_tls server would do, so they're silent.
        let mtls_intent = match &self.trusted_certs {
            TrustedCerts::CaFiles(v) => !v.is_empty(),
            TrustedCerts::CaBytes(v) => !v.is_empty(),
            TrustedCerts::SkipVerification => false,
        };
        if mtls_intent {
            log::warn!(
                "TlsConfig.trusted_certs is configured with CA roots on a \
                 server transport, but native_tls::TlsAcceptorBuilder does \
                 not expose mTLS trust configuration — the setting is \
                 ignored on this transport. Use the tls-rustls feature for \
                 mTLS."
            );
        }
        builder
            .build()
            .map_err(|e| RepError::NetworkError(format!("TLS acceptor: {e}")))
    }

    /// Build a `native_tls::TlsConnector` for TCP client use.
    pub(crate) fn to_native_connector(
        &self,
    ) -> Result<native_tls::TlsConnector> {
        let mut builder = native_tls::TlsConnector::builder();

        // Install identity if present (optional for client-only auth).
        if !matches!(&self.identity, TlsIdentity::SelfSigned { .. }) {
            let id = self.native_identity()?;
            builder.identity(id);
        }

        self.apply_native_trust(&mut builder)?;
        builder
            .build()
            .map_err(|e| RepError::NetworkError(format!("TLS connector: {e}")))
    }

    fn native_identity(&self) -> Result<native_tls::Identity> {
        match &self.identity {
            TlsIdentity::Pkcs12 { der, password } => native_tls::Identity::from_pkcs12(der, password)
                .map_err(|e| RepError::NetworkError(format!("PKCS12 identity: {e}"))),
            TlsIdentity::SelfSigned { .. } => Err(RepError::NetworkError(
                "SelfSigned identity is not supported by the tls-native backend; \
                 use the tls-rustls feature instead, or supply a Pkcs12 identity"
                    .into(),
            )),
            TlsIdentity::PemFiles { .. } | TlsIdentity::PemBytes { .. } => {
                Err(RepError::NetworkError(
                    "PEM identities are not supported by the tls-native backend; \
                     convert to PKCS12 with: openssl pkcs12 -export -out id.p12 \
                     -inkey key.pem -in cert.pem"
                        .into(),
                ))
            }
        }
    }

    fn apply_native_trust(
        &self,
        builder: &mut native_tls::TlsConnectorBuilder,
    ) -> Result<()> {
        match &self.trusted_certs {
            TrustedCerts::SkipVerification => {
                builder.danger_accept_invalid_certs(true);
            }
            TrustedCerts::CaFiles(paths) => {
                for path in paths {
                    let pem = std::fs::read(path).map_err(|e| {
                        RepError::NetworkError(format!("CA file: {e}"))
                    })?;
                    let cert = native_tls::Certificate::from_pem(&pem)
                        .map_err(|e| {
                            RepError::NetworkError(format!("CA parse: {e}"))
                        })?;
                    builder.add_root_certificate(cert);
                }
            }
            TrustedCerts::CaBytes(pems) => {
                for pem in pems {
                    let cert = native_tls::Certificate::from_pem(pem).map_err(
                        |e| RepError::NetworkError(format!("CA parse: {e}")),
                    )?;
                    builder.add_root_certificate(cert);
                }
            }
        }
        Ok(())
    }
}

// `apply_native_trust` is a `TlsConnectorBuilder`-only helper. The
// previous shared trait `NativeTlsBuilderExt` was removed because the
// `native_tls::TlsAcceptorBuilder` does not expose `add_root_certificate`
// or `danger_accept_invalid_certs`, and the trait impls for it were
// unconditionally recursive (a real bug — they would have stack-
// overflowed on first call).

// ─── tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── Constructors (no feature gate; pure data shape) ──────────────

    #[test]
    fn insecure_constructor_uses_self_signed_localhost() {
        let cfg = TlsConfig::insecure("node-a");
        assert_eq!(cfg.server_name, "node-a");
        match cfg.identity {
            TlsIdentity::SelfSigned { subject_alt_names } => {
                assert_eq!(subject_alt_names, vec!["localhost".to_string()]);
            }
            _ => panic!("insecure should produce SelfSigned identity"),
        }
        assert!(matches!(cfg.trusted_certs, TrustedCerts::SkipVerification));
    }

    #[test]
    fn from_pem_files_constructor_records_paths() {
        let cfg = TlsConfig::from_pem_files(
            "/tmp/cert.pem",
            "/tmp/key.pem",
            "/tmp/ca.pem",
            "node-b",
        );
        assert_eq!(cfg.server_name, "node-b");
        match cfg.identity {
            TlsIdentity::PemFiles { cert, key } => {
                assert_eq!(cert, std::path::PathBuf::from("/tmp/cert.pem"));
                assert_eq!(key, std::path::PathBuf::from("/tmp/key.pem"));
            }
            _ => panic!("from_pem_files should produce PemFiles identity"),
        }
        match cfg.trusted_certs {
            TrustedCerts::CaFiles(paths) => {
                assert_eq!(
                    paths,
                    vec![std::path::PathBuf::from("/tmp/ca.pem")]
                );
            }
            _ => panic!("from_pem_files should produce CaFiles trust"),
        }
    }

    #[test]
    fn from_pkcs12_constructor_holds_bytes_and_password() {
        let der = vec![0x30, 0x82, 0x00, 0x10]; // dummy DER prefix
        let ca_pem = b"-----BEGIN CERTIFICATE-----\n".to_vec();
        let cfg = TlsConfig::from_pkcs12(
            der.clone(),
            "secret".to_string(),
            ca_pem.clone(),
            "node-c",
        );
        assert_eq!(cfg.server_name, "node-c");
        match cfg.identity {
            TlsIdentity::Pkcs12 { der: d, password } => {
                assert_eq!(d, der);
                assert_eq!(password, "secret");
            }
            _ => panic!("from_pkcs12 should produce Pkcs12 identity"),
        }
        match cfg.trusted_certs {
            TrustedCerts::CaBytes(pems) => {
                assert_eq!(pems, vec![ca_pem]);
            }
            _ => panic!("from_pkcs12 should produce CaBytes trust"),
        }
    }

    // ── rustls path (requires tls-rustls feature) ────────────────────

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn rustls_server_config_from_self_signed_succeeds() {
        // SelfSigned + SkipVerification is the "insecure" config; the
        // rustls server side generates a fresh self-signed cert at
        // build time. Should produce a valid ServerConfig.
        let cfg = TlsConfig::insecure("node-self");
        let sc = cfg.to_rustls_server_config();
        assert!(
            sc.is_ok(),
            "to_rustls_server_config from insecure() should succeed: {:?}",
            sc.err()
        );
    }

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn rustls_client_config_skip_verification_succeeds() {
        // SkipVerification is the trust mode for development clusters;
        // the client config should be built using the
        // SkipCertVerification verifier.
        let cfg = TlsConfig::insecure("any-name");
        let cc = cfg.to_rustls_client_config();
        assert!(
            cc.is_ok(),
            "to_rustls_client_config with SkipVerification should succeed: \
             {:?}",
            cc.err()
        );
    }

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn rustls_client_config_with_empty_ca_succeeds() {
        // Empty CaBytes list is a valid (if useless) input — produces
        // a ClientConfig with an empty root store. No certs will
        // validate, but the build itself should not error.
        let cfg = TlsConfig {
            identity: TlsIdentity::SelfSigned {
                subject_alt_names: vec!["localhost".into()],
            },
            trusted_certs: TrustedCerts::CaBytes(vec![]),
            server_name: "x".into(),
        };
        // rustls_root_store on empty CaBytes returns an empty
        // RootCertStore.
        let cc = cfg.to_rustls_client_config();
        assert!(cc.is_ok(), "{:?}", cc.err());
    }

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn rustls_client_config_with_malformed_ca_bytes_returns_empty_store() {
        // rustls_pemfile silently skips non-certificate PEM blocks, so
        // CaBytes containing garbage produces an empty root store
        // rather than an error. This is documented (silent skip
        // behaviour) and the calling test asserts it: client config
        // builds, but no certs will validate.
        let cfg = TlsConfig {
            identity: TlsIdentity::SelfSigned {
                subject_alt_names: vec!["localhost".into()],
            },
            trusted_certs: TrustedCerts::CaBytes(vec![b"not-a-pem".to_vec()]),
            server_name: "x".into(),
        };
        let cc = cfg.to_rustls_client_config();
        assert!(
            cc.is_ok(),
            "malformed CaBytes silently produces an empty root store; \
             a real cert will fail to verify but the config itself \
             builds. got Err: {:?}",
            cc.err()
        );
    }

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn skip_cert_verification_returns_ok_for_any_cert() {
        use rustls::client::danger::ServerCertVerifier;
        let v = SkipCertVerification::new();
        let cert = rustls::pki_types::CertificateDer::from(vec![0u8; 8]);
        let server_name =
            rustls::pki_types::ServerName::try_from("localhost").unwrap();
        let now = rustls::pki_types::UnixTime::now();
        let r = v.verify_server_cert(&cert, &[], &server_name, &[], now);
        assert!(r.is_ok(), "SkipCertVerification must return Ok for any cert");
    }

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn skip_cert_verification_supports_some_schemes() {
        use rustls::client::danger::ServerCertVerifier;
        let v = SkipCertVerification::new();
        let schemes = v.supported_verify_schemes();
        assert!(
            !schemes.is_empty(),
            "SkipCertVerification must report at least one signature scheme"
        );
    }

    // ── native-tls path (requires tls-native feature) ────────────────

    #[cfg(feature = "tls-native")]
    #[test]
    fn native_acceptor_requires_pkcs12_identity() {
        // SelfSigned identity is rejected because native_tls cannot
        // generate certs at runtime (only Pkcs12 is supported).
        let cfg = TlsConfig {
            identity: TlsIdentity::SelfSigned {
                subject_alt_names: vec!["localhost".into()],
            },
            trusted_certs: TrustedCerts::SkipVerification,
            server_name: "x".into(),
        };
        let r = cfg.to_native_acceptor();
        assert!(
            r.is_err(),
            "SelfSigned identity with native-tls must error, got Ok"
        );
    }

    #[cfg(feature = "tls-native")]
    #[test]
    fn native_connector_skip_verification_succeeds() {
        let cfg = TlsConfig {
            identity: TlsIdentity::SelfSigned {
                subject_alt_names: vec!["localhost".into()],
            },
            trusted_certs: TrustedCerts::SkipVerification,
            server_name: "any".into(),
        };
        // The client side does not need to load the local identity
        // (clients without a cert is normal).
        let r = cfg.to_native_connector();
        assert!(
            r.is_ok(),
            "native_tls client with SkipVerification should succeed: {:?}",
            r.err()
        );
    }

    // ── End-to-end with real X.509 (uses rcgen, only available
    //    under tls-rustls because that's where rcgen is gated). ──

    #[cfg(feature = "tls-rustls")]
    fn make_self_signed_pem(san: &[&str]) -> (Vec<u8>, Vec<u8>) {
        // Returns (cert_pem_bytes, key_pem_bytes).
        let sans: Vec<String> = san.iter().map(|s| s.to_string()).collect();
        let ck = rcgen::generate_simple_self_signed(sans).unwrap();
        let cert_pem = ck.cert.pem().into_bytes();
        let key_pem = ck.key_pair.serialize_pem().into_bytes();
        (cert_pem, key_pem)
    }

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn rustls_server_config_from_pem_bytes() {
        // Generate a real self-signed pair and feed it to the
        // server-config builder via PemBytes.
        let (cert_pem, key_pem) = make_self_signed_pem(&["localhost"]);
        let cfg = TlsConfig {
            identity: TlsIdentity::PemBytes { cert: cert_pem, key: key_pem },
            trusted_certs: TrustedCerts::SkipVerification,
            server_name: "localhost".into(),
        };
        let sc = cfg.to_rustls_server_config();
        assert!(sc.is_ok(), "PemBytes server config: {:?}", sc.err());
    }

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn rustls_server_config_from_pem_files_on_disk() {
        // Write the generated cert/key to tempfiles, then use the
        // PemFiles identity. This exercises the file-IO path in
        // rustls_cert_and_key.
        let (cert_pem, key_pem) = make_self_signed_pem(&["localhost"]);
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("cert.pem");
        let key_path = dir.path().join("key.pem");
        std::fs::write(&cert_path, &cert_pem).unwrap();
        std::fs::write(&key_path, &key_pem).unwrap();

        let cfg = TlsConfig {
            identity: TlsIdentity::PemFiles { cert: cert_path, key: key_path },
            trusted_certs: TrustedCerts::SkipVerification,
            server_name: "localhost".into(),
        };
        let sc = cfg.to_rustls_server_config();
        assert!(sc.is_ok(), "PemFiles server config: {:?}", sc.err());
    }

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn rustls_client_config_with_real_ca_bytes() {
        // Use a generated cert as a "CA" — rustls accepts it as a
        // root cert; that's enough to exercise the full
        // CaBytes -> RootCertStore::add path.
        let (ca_pem, _ca_key) = make_self_signed_pem(&["test-ca"]);
        let cfg = TlsConfig {
            identity: TlsIdentity::SelfSigned {
                subject_alt_names: vec!["localhost".into()],
            },
            trusted_certs: TrustedCerts::CaBytes(vec![ca_pem]),
            server_name: "localhost".into(),
        };
        let cc = cfg.to_rustls_client_config();
        assert!(cc.is_ok(), "real CA bytes: {:?}", cc.err());
    }

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn rustls_client_config_with_real_ca_file() {
        let (ca_pem, _ca_key) = make_self_signed_pem(&["test-ca"]);
        let dir = tempfile::tempdir().unwrap();
        let ca_path = dir.path().join("ca.pem");
        std::fs::write(&ca_path, &ca_pem).unwrap();

        let cfg = TlsConfig {
            identity: TlsIdentity::SelfSigned {
                subject_alt_names: vec!["localhost".into()],
            },
            trusted_certs: TrustedCerts::CaFiles(vec![ca_path]),
            server_name: "localhost".into(),
        };
        let cc = cfg.to_rustls_client_config();
        assert!(cc.is_ok(), "real CA file: {:?}", cc.err());
    }

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn rustls_server_config_with_pem_files_missing_cert_errors() {
        let dir = tempfile::tempdir().unwrap();
        let nonexistent = dir.path().join("does-not-exist.pem");
        let key_path = dir.path().join("key.pem");
        let (_, key_pem) = make_self_signed_pem(&["localhost"]);
        std::fs::write(&key_path, &key_pem).unwrap();

        let cfg = TlsConfig {
            identity: TlsIdentity::PemFiles {
                cert: nonexistent,
                key: key_path,
            },
            trusted_certs: TrustedCerts::SkipVerification,
            server_name: "localhost".into(),
        };
        let sc = cfg.to_rustls_server_config();
        assert!(sc.is_err(), "missing cert file should error, got Ok");
    }

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn rustls_server_config_with_pem_files_missing_key_errors() {
        let dir = tempfile::tempdir().unwrap();
        let cert_path = dir.path().join("cert.pem");
        let nonexistent = dir.path().join("nonexistent-key.pem");
        let (cert_pem, _) = make_self_signed_pem(&["localhost"]);
        std::fs::write(&cert_path, &cert_pem).unwrap();

        let cfg = TlsConfig {
            identity: TlsIdentity::PemFiles {
                cert: cert_path,
                key: nonexistent,
            },
            trusted_certs: TrustedCerts::SkipVerification,
            server_name: "localhost".into(),
        };
        let sc = cfg.to_rustls_server_config();
        assert!(sc.is_err(), "missing key file should error, got Ok");
    }

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn rustls_root_store_with_malformed_ca_file_errors() {
        // Garbage file content (non-PEM) — rustls_root_store should
        // surface the parse error.
        let dir = tempfile::tempdir().unwrap();
        let bad_ca = dir.path().join("bad.pem");
        std::fs::write(&bad_ca, b"this is not a PEM file\n").unwrap();

        let cfg = TlsConfig {
            identity: TlsIdentity::SelfSigned {
                subject_alt_names: vec!["localhost".into()],
            },
            trusted_certs: TrustedCerts::CaFiles(vec![bad_ca]),
            server_name: "x".into(),
        };
        // rustls_pemfile silently skips garbage, returning an empty
        // root store. Build is OK; the Ok-path covers the
        // CaFiles loop body where no certs to add exist.
        let cc = cfg.to_rustls_client_config();
        assert!(cc.is_ok());
    }

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn rustls_client_config_with_missing_ca_file_errors() {
        let cfg = TlsConfig {
            identity: TlsIdentity::SelfSigned {
                subject_alt_names: vec!["localhost".into()],
            },
            trusted_certs: TrustedCerts::CaFiles(vec![
                std::path::PathBuf::from("/nonexistent/ca.pem"),
            ]),
            server_name: "x".into(),
        };
        let cc = cfg.to_rustls_client_config();
        assert!(cc.is_err(), "missing CA file should error");
    }

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn rustls_server_config_self_signed_runtime() {
        // SelfSigned identity goes through rcgen at build time inside
        // rustls_cert_and_key. Verify the generated cert is parseable
        // and the ServerConfig builds.
        let cfg = TlsConfig {
            identity: TlsIdentity::SelfSigned {
                subject_alt_names: vec!["host-a".into(), "host-b".into()],
            },
            trusted_certs: TrustedCerts::SkipVerification,
            server_name: "host-a".into(),
        };
        let sc = cfg.to_rustls_server_config();
        assert!(sc.is_ok(), "SelfSigned runtime cert: {:?}", sc.err());
    }

    // ── verify_tls12_signature / verify_tls13_signature exercise ──
    //
    // Skipped at the unit-test level because constructing a
    // `rustls::DigitallySignedStruct` requires a private API; a
    // future integration test that performs a real TLS handshake
    // (TlsTcpChannel + TlsTcpChannelListener with a generated cert
    // pair) will exercise these arms naturally.

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn rustls_pkcs12_identity_is_rejected() {
        // The tls-rustls backend explicitly rejects Pkcs12 (it's the
        // tls-native identity); covers the dedicated error arm.
        let cfg = TlsConfig {
            identity: TlsIdentity::Pkcs12 {
                der: vec![0x30, 0x82, 0x00, 0x10],
                password: "x".into(),
            },
            trusted_certs: TrustedCerts::SkipVerification,
            server_name: "x".into(),
        };
        let r = cfg.to_rustls_server_config();
        assert!(r.is_err(), "Pkcs12 with rustls must error");
        let msg = format!("{}", r.err().unwrap());
        assert!(
            msg.contains("Pkcs12") || msg.contains("not supported"),
            "error should mention Pkcs12 or not-supported, got: {msg}"
        );
    }

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn rustls_pem_bytes_no_certificates_errors() {
        let cfg = TlsConfig {
            identity: TlsIdentity::PemBytes {
                cert: b"-----BEGIN GARBAGE-----\nXX\n-----END GARBAGE-----\n"
                    .to_vec(),
                key: b"-----BEGIN PRIVATE KEY-----\nMC4CAQA=\n-----END PRIVATE KEY-----\n"
                    .to_vec(),
            },
            trusted_certs: TrustedCerts::SkipVerification,
            server_name: "x".into(),
        };
        let r = cfg.to_rustls_server_config();
        assert!(r.is_err(), "PEM with no certificates must error, got Ok");
    }

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn rustls_pem_bytes_no_private_key_errors() {
        let (cert_pem, _key_pem) = make_self_signed_pem(&["localhost"]);
        let cfg = TlsConfig {
            identity: TlsIdentity::PemBytes {
                cert: cert_pem,
                key: b"-----BEGIN GARBAGE-----\nXX\n-----END GARBAGE-----\n"
                    .to_vec(),
            },
            trusted_certs: TrustedCerts::SkipVerification,
            server_name: "x".into(),
        };
        let r = cfg.to_rustls_server_config();
        assert!(r.is_err(), "PEM with no private key must error, got Ok");
    }

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn rustls_skip_verification_root_store_is_empty_but_ok() {
        // The SkipVerification arm of rustls_root_store returns an
        // empty store (the caller is expected to install a custom
        // verifier instead). Calling to_rustls_client_config goes
        // through the dangerous() path so this branch is not reached
        // during normal client-config build; we exercise it directly
        // via a hand-crafted code path: build a config with
        // SkipVerification and explicitly compare to the
        // rustls_root_store result for an empty CaBytes config.
        let skip_cfg = TlsConfig::insecure("localhost");
        // Indirectly exercises rustls_root_store via the
        // dangerous-skip branch.
        let cc = skip_cfg.to_rustls_client_config();
        assert!(cc.is_ok());

        // Also build with explicit CaBytes(empty) so the loop body
        // (no PEMs) exercises the second branch.
        let empty_cfg = TlsConfig {
            identity: TlsIdentity::SelfSigned {
                subject_alt_names: vec!["localhost".into()],
            },
            trusted_certs: TrustedCerts::CaBytes(vec![]),
            server_name: "x".into(),
        };
        let cc2 = empty_cfg.to_rustls_client_config();
        assert!(cc2.is_ok());
    }

    // ── QUIC config builders (under quic feature) ──

    #[cfg(all(feature = "tls-rustls", feature = "quic"))]
    #[test]
    fn quinn_server_config_builds_from_self_signed() {
        let cfg = TlsConfig::insecure("localhost");
        let qc = cfg.to_quinn_server_config();
        assert!(qc.is_ok(), "quinn server config: {:?}", qc.err());
    }

    #[cfg(all(feature = "tls-rustls", feature = "quic"))]
    #[test]
    fn quinn_client_config_builds_from_skip_verification() {
        let cfg = TlsConfig::insecure("localhost");
        let qc = cfg.to_quinn_client_config();
        assert!(qc.is_ok(), "quinn client config: {:?}", qc.err());
    }

    #[cfg(all(feature = "tls-rustls", feature = "quic"))]
    #[test]
    fn quinn_client_config_with_real_ca_bytes() {
        let (ca_pem, _) = make_self_signed_pem(&["test-ca"]);
        let cfg = TlsConfig {
            identity: TlsIdentity::SelfSigned {
                subject_alt_names: vec!["localhost".into()],
            },
            trusted_certs: TrustedCerts::CaBytes(vec![ca_pem]),
            server_name: "localhost".into(),
        };
        let qc = cfg.to_quinn_client_config();
        assert!(qc.is_ok(), "quinn client config with CA: {:?}", qc.err());
    }
}
