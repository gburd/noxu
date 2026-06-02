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

impl std::fmt::Debug for TlsIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SelfSigned { subject_alt_names } => f
                .debug_struct("SelfSigned")
                .field("subject_alt_names", subject_alt_names)
                .finish(),
            Self::PemFiles { cert, key } => f
                .debug_struct("PemFiles")
                .field("cert", cert)
                .field("key", key)
                .finish(),
            Self::PemBytes { cert, .. } => f
                .debug_struct("PemBytes")
                .field("cert_len", &cert.len())
                .field("key", &"<redacted>")
                .finish(),
            Self::Pkcs12 { .. } => f
                .debug_struct("Pkcs12")
                .field("der", &"<redacted>")
                .field("password", &"<redacted>")
                .finish(),
        }
    }
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

impl std::fmt::Debug for TrustedCerts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SkipVerification => write!(f, "SkipVerification"),
            Self::CaFiles(paths) => {
                f.debug_tuple("CaFiles").field(paths).finish()
            }
            Self::CaBytes(pems) => {
                let sizes: Vec<usize> =
                    pems.iter().map(|p| p.len()).collect();
                f.debug_struct("CaBytes")
                    .field("blob_sizes", &sizes)
                    .finish()
            }
        }
    }
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

impl std::fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsConfig")
            .field("server_name", &self.server_name)
            .field("identity", &self.identity)
            .field("trusted_certs", &self.trusted_certs)
            .finish()
    }
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

    /// Create a TLS configuration intended for **replication**:
    /// requires both a non-self-signed identity and a non-empty
    /// CA list. Returns `Err` for any input that would
    /// produce a configuration where the peer cannot be
    /// authenticated.
    ///
    /// This is the documented path for production replication
    /// per `docs/src/internal/auth-mtls-design-2026-05.md`. It
    /// is stricter than the `from_pem_files` and `from_pkcs12`
    /// constructors:
    ///
    ///   - Rejects `TlsIdentity::SelfSigned` (a runtime-generated
    ///     cert has no consistent subject across restarts —
    ///     incompatible with subject-based authorisation).
    ///   - Rejects `TrustedCerts::SkipVerification` (skip-verify
    ///     is a development-only path).
    ///   - Rejects empty `CaFiles` / `CaBytes` lists.
    ///
    /// `[`TlsConfig::insecure`]` remains available for tests and
    /// trusted-network deployments where the operator
    /// explicitly accepts an unauthenticated transport.
    pub fn for_replication(
        identity: TlsIdentity,
        trusted_certs: TrustedCerts,
        server_name: impl Into<String>,
    ) -> Result<Self> {
        // Validate identity.
        if matches!(identity, TlsIdentity::SelfSigned { .. }) {
            return Err(RepError::ConfigError(
                "TlsConfig::for_replication rejects TlsIdentity::SelfSigned: \
                 runtime-generated certs have no stable subject and cannot \
                 be matched against a peer allowlist. Use PemFiles, \
                 PemBytes, or Pkcs12 with a real CA-issued cert."
                    .into(),
            ));
        }
        // Validate trust.
        match &trusted_certs {
            TrustedCerts::SkipVerification => {
                return Err(RepError::ConfigError(
                    "TlsConfig::for_replication rejects \
                     TrustedCerts::SkipVerification: replication peer \
                     authentication requires CA-rooted chain validation. \
                     Use TrustedCerts::CaFiles or CaBytes with the \
                     replication CA's certificate."
                        .into(),
                ));
            }
            TrustedCerts::CaFiles(v) if v.is_empty() => {
                return Err(RepError::ConfigError(
                    "TlsConfig::for_replication rejects empty CaFiles: \
                     at least one CA must be provided for peer cert \
                     validation."
                        .into(),
                ));
            }
            TrustedCerts::CaBytes(v) if v.is_empty() => {
                return Err(RepError::ConfigError(
                    "TlsConfig::for_replication rejects empty CaBytes: \
                     at least one CA must be provided for peer cert \
                     validation."
                        .into(),
                ));
            }
            _ => {}
        }
        Ok(TlsConfig {
            identity,
            trusted_certs,
            server_name: server_name.into(),
        })
    }
}

// ─── rustls helpers ──────────────────────────────────────────────────────────

#[cfg(feature = "tls-rustls")]
impl TlsConfig {
    /// Build a `rustls::ServerConfig` from this configuration.
    ///
    /// The server does **not** request a client certificate (no mTLS).
    /// Use [`to_rustls_server_config_with_allowlist`] when
    /// `RepConfig::peer_allowlist` is configured.
    ///
    /// Used by [`TlsTcpChannelListener`] and the QUIC server path.
    ///
    /// [`to_rustls_server_config_with_allowlist`]: Self::to_rustls_server_config_with_allowlist
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

    /// Build a `rustls::ServerConfig` that enforces `peer_allowlist`.
    ///
    /// This is the **mTLS enforcement path** introduced in Phase 2 (v3.1.0).
    /// Compared to [`to_rustls_server_config`]:
    ///
    /// - The server requests a client certificate (`client_auth_mandatory = true`).
    /// - The client certificate chain is validated against the CA roots in
    ///   this `TlsConfig`.
    /// - The peer's Subject CN and DNS SANs are extracted and checked against
    ///   `allowlist`; the TLS handshake is aborted if no name matches.
    ///
    /// # Errors
    ///
    /// - `RepError::ConfigError` if `trusted_certs` is
    ///   [`TrustedCerts::SkipVerification`] (no CA to validate client certs).
    /// - `RepError::ConfigError` if `allowlist` is empty (fail-closed per
    ///   design doc: an empty allowlist admits no peer).
    /// - `RepError::NetworkError` if the cert/key material fails to parse.
    ///
    /// [`to_rustls_server_config`]: Self::to_rustls_server_config
    pub(crate) fn to_rustls_server_config_with_allowlist(
        &self,
        allowlist: crate::auth::PeerAllowlist,
    ) -> Result<std::sync::Arc<rustls::ServerConfig>> {
        if matches!(&self.trusted_certs, TrustedCerts::SkipVerification) {
            return Err(RepError::ConfigError(
                "to_rustls_server_config_with_allowlist requires a                  CA-rooted TrustedCerts configuration (CaFiles or CaBytes);                  SkipVerification cannot be used for mTLS enforcement because                  there is no CA to validate peer certificates against."
                    .into(),
            ));
        }
        let (certs, key) = self.rustls_cert_and_key()?;
        let root_store = self.rustls_root_store()?;
        let verifier = crate::auth::PeerAllowlistVerifier::new(
            std::sync::Arc::new(root_store),
            allowlist,
        )?;
        let cfg = rustls::ServerConfig::builder()
            .with_client_cert_verifier(std::sync::Arc::new(verifier))
            .with_single_cert(certs, key)
            .map_err(|e| {
                RepError::NetworkError(format!("TLS server config (mTLS): {e}"))
            })?;
        Ok(std::sync::Arc::new(cfg))
    }

    /// Build a `rustls::ClientConfig` from this configuration.
    ///
    /// **mTLS client-auth behaviour (Phase 2, v3.1.0)**:  When the identity
    /// is [`TlsIdentity::PemFiles`] or [`TlsIdentity::PemBytes`] and
    /// `trusted_certs` is not [`TrustedCerts::SkipVerification`], the client
    /// presents its own certificate during the TLS handshake so that a server
    /// running [`crate::auth::PeerAllowlistVerifier`] can verify it.
    ///
    /// [`TlsIdentity::SelfSigned`] always uses `with_no_client_auth` — a
    /// runtime-generated self-signed cert would fail CA chain validation on
    /// the server side regardless.
    ///
    /// Used by [`TlsTcpChannel`] and the QUIC client path.
    pub(crate) fn to_rustls_client_config(
        &self,
    ) -> Result<std::sync::Arc<rustls::ClientConfig>> {
        // Insecure (dev) path: skip server-cert verification, no client cert.
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

        // Production mTLS path: present client cert for PemFiles / PemBytes.
        // SelfSigned identity stays with_no_client_auth — a runtime-generated
        // self-signed cert would fail CA chain validation on the server side
        // anyway; keeping the dev-convenience path functional without change.
        match &self.identity {
            TlsIdentity::SelfSigned { .. } => {
                let cfg = rustls::ClientConfig::builder()
                    .with_root_certificates(root_store)
                    .with_no_client_auth();
                Ok(std::sync::Arc::new(cfg))
            }
            TlsIdentity::PemFiles { .. } | TlsIdentity::PemBytes { .. } => {
                let (certs, key) = self.rustls_cert_and_key()?;
                let cfg = rustls::ClientConfig::builder()
                    .with_root_certificates(root_store)
                    .with_client_auth_cert(certs, key)
                    .map_err(|e| {
                        RepError::NetworkError(format!(
                            "TLS client auth cert: {e}"
                        ))
                    })?;
                Ok(std::sync::Arc::new(cfg))
            }
            TlsIdentity::Pkcs12 { .. } => Err(RepError::NetworkError(
                "Pkcs12 identity is not supported by the tls-rustls                  backend; use PemFiles or PemBytes instead"
                    .into(),
            )),
        }
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

    /// Build a `quinn::ServerConfig` that enforces `peer_allowlist` via mTLS.
    ///
    /// This is the **QUIC mTLS enforcement path** introduced in Phase 3.
    /// Compared to [`to_quinn_server_config`]:
    ///
    /// - The server requests a client certificate (`client_auth_mandatory = true`).
    /// - The client certificate chain is validated against the CA roots in this
    ///   `TlsConfig`.
    /// - The peer's Subject CN and DNS SANs are checked against `allowlist`;
    ///   the QUIC (TLS 1.3) handshake is aborted if no name matches.
    ///
    /// The empty-allowlist fail-closed policy applies: constructing with an
    /// empty `PeerAllowlist` returns `Err(RepError::ConfigError)`.
    ///
    /// # Errors
    ///
    /// Same as [`to_rustls_server_config_with_allowlist`] plus QUIC config
    /// conversion errors.
    ///
    /// [`to_rustls_server_config_with_allowlist`]: Self::to_rustls_server_config_with_allowlist
    /// [`to_quinn_server_config`]: Self::to_quinn_server_config
    #[cfg(feature = "quic")]
    pub fn to_quinn_server_config_with_allowlist(
        &self,
        allowlist: crate::auth::PeerAllowlist,
    ) -> Result<quinn::ServerConfig> {
        let rustls_cfg =
            self.to_rustls_server_config_with_allowlist(allowlist)?;
        let quic_cfg = quinn::crypto::rustls::QuicServerConfig::try_from(
            rustls::ServerConfig::clone(&rustls_cfg),
        )
        .map_err(|e| {
            RepError::NetworkError(format!(
                "QUIC server config (mTLS): {e}"
            ))
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
                // TLS-2: an empty CaFiles list is a misconfiguration. It
                // would silently produce an empty trust store, which validates
                // nothing without the explicit `SkipVerification` opt-out.
                if paths.is_empty() {
                    return Err(RepError::ConfigError(
                        "TrustedCerts::CaFiles configured with no paths; \
                         this is a misconfiguration. Use \
                         TrustedCerts::SkipVerification to explicitly opt \
                         out of CA verification."
                            .into(),
                    ));
                }
                for path in paths {
                    let pem = std::fs::read(path).map_err(|e| {
                        RepError::NetworkError(format!("CA file: {e}"))
                    })?;
                    let parsed: Vec<_> =
                        certs(&mut BufReader::new(pem.as_slice()))
                            .collect::<std::result::Result<Vec<_>, _>>()
                            .map_err(|e| {
                                RepError::NetworkError(format!("CA parse: {e}"))
                            })?;
                    // TLS-3: rustls_pemfile silently skips non-cert PEM blocks
                    // (and any non-PEM bytes). If the file had bytes but
                    // produced zero certificates, treat that as a parse error
                    // rather than silently building an empty trust store.
                    if !pem.is_empty() && parsed.is_empty() {
                        return Err(RepError::ConfigError(format!(
                            "CA file {} parsed but contained 0 certificates",
                            path.display()
                        )));
                    }
                    for cert in parsed {
                        store.add(cert).map_err(|e| {
                            RepError::NetworkError(format!("CA add: {e}"))
                        })?;
                    }
                }
            }
            TrustedCerts::CaBytes(pems) => {
                // TLS-2: empty CaBytes list — same misconfiguration as
                // empty CaFiles. Reject explicitly.
                if pems.is_empty() {
                    return Err(RepError::ConfigError(
                        "TrustedCerts::CaBytes configured with no PEM blobs; \
                         this is a misconfiguration. Use \
                         TrustedCerts::SkipVerification to explicitly opt \
                         out of CA verification."
                            .into(),
                    ));
                }
                for (idx, pem) in pems.iter().enumerate() {
                    let parsed: Vec<_> =
                        certs(&mut BufReader::new(pem.as_slice()))
                            .collect::<std::result::Result<Vec<_>, _>>()
                            .map_err(|e| {
                                RepError::NetworkError(format!("CA parse: {e}"))
                            })?;
                    // TLS-3: bytes provided but no certs decoded.
                    if !pem.is_empty() && parsed.is_empty() {
                        return Err(RepError::ConfigError(format!(
                            "CA bytes (index {idx}) parsed but contained 0 \
                             certificates"
                        )));
                    }
                    for cert in parsed {
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
        // TLS-4: `native_tls::TlsAcceptorBuilder` exposes no CA-root or
        // accept-invalid-client-cert knobs, so any non-empty CA list on
        // this server transport is an mTLS configuration the runtime
        // cannot honour. A `log::warn!` here is not a security boundary;
        // proceeding silently would build an acceptor with NO client-cert
        // verification despite the operator's expressed intent. Refuse
        // up front (before identity parsing) so the misconfiguration is
        // loud and surfaces independently of any identity-format errors.
        let mtls_intent = match &self.trusted_certs {
            TrustedCerts::CaFiles(v) => !v.is_empty(),
            TrustedCerts::CaBytes(v) => !v.is_empty(),
            TrustedCerts::SkipVerification => false,
        };
        if mtls_intent {
            return Err(RepError::ConfigError(
                "mTLS is configured (TrustedCerts has CA roots) but the \
                 tls-native server transport does not support it: \
                 native_tls::TlsAcceptorBuilder exposes no client-cert \
                 verification knobs. Use the tls-rustls feature for mTLS, \
                 or set TrustedCerts::SkipVerification on this transport."
                    .into(),
            ));
        }
        let identity = self.native_identity()?;
        let builder = native_tls::TlsAcceptor::builder(identity);
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
    fn rustls_client_config_with_empty_ca_bytes_errors() {
        // TLS-2: empty CaBytes is a misconfiguration. Without an explicit
        // SkipVerification opt-out, an empty trust store would validate
        // nothing — refuse at config-build time.
        let cfg = TlsConfig {
            identity: TlsIdentity::SelfSigned {
                subject_alt_names: vec!["localhost".into()],
            },
            trusted_certs: TrustedCerts::CaBytes(vec![]),
            server_name: "x".into(),
        };
        let cc = cfg.to_rustls_client_config();
        assert!(
            cc.is_err(),
            "empty CaBytes must be a misconfiguration error, got Ok"
        );
        let msg = format!("{}", cc.err().unwrap());
        assert!(
            msg.contains("CaBytes") && msg.contains("misconfiguration"),
            "error should mention CaBytes/misconfiguration, got: {msg}"
        );
    }

    #[cfg(feature = "tls-rustls")]
    #[test]
    fn rustls_client_config_with_malformed_ca_bytes_errors() {
        // TLS-3: bytes were provided but rustls_pemfile produced 0 certs.
        // Refuse rather than silently build an empty trust store.
        let cfg = TlsConfig {
            identity: TlsIdentity::SelfSigned {
                subject_alt_names: vec!["localhost".into()],
            },
            trusted_certs: TrustedCerts::CaBytes(vec![b"not-a-pem".to_vec()]),
            server_name: "x".into(),
        };
        let cc = cfg.to_rustls_client_config();
        assert!(
            cc.is_err(),
            "malformed CaBytes must error rather than build an empty store, \
             got Ok"
        );
        let msg = format!("{}", cc.err().unwrap());
        assert!(
            msg.contains("0 certificates"),
            "error should mention 0 certificates, got: {msg}"
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
        // TLS-3: the file has bytes but rustls_pemfile decodes 0
        // certificates. Surface this as a structured error rather
        // than silently producing an empty trust store.
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
        let cc = cfg.to_rustls_client_config();
        assert!(
            cc.is_err(),
            "garbage CA file must error rather than yield empty trust"
        );
        let msg = format!("{}", cc.err().unwrap());
        assert!(
            msg.contains("0 certificates"),
            "error should mention 0 certificates, got: {msg}"
        );
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
    fn rustls_skip_verification_client_config_succeeds() {
        // SkipVerification is the explicit "skip-verify" opt-out; it must
        // continue to build a client config without requiring CA roots.
        // (TLS-2 enforces the empty-CaBytes/CaFiles path errors instead.)
        let skip_cfg = TlsConfig::insecure("localhost");
        let cc = skip_cfg.to_rustls_client_config();
        assert!(cc.is_ok());
    }

    // ── Security-review hardening: TLS-2, TLS-3, TLS-4 ──
    //
    // Each test below targets a specific finding from
    // docs/src/internal/security-review-2026-05.md. They fail before the
    // hardening change and pass after it.

    // TLS-2: empty `CaFiles(vec![])` is a misconfiguration, not a silent
    // empty trust store.
    #[cfg(feature = "tls-rustls")]
    #[test]
    fn tls2_empty_ca_files_errors() {
        let cfg = TlsConfig {
            identity: TlsIdentity::SelfSigned {
                subject_alt_names: vec!["localhost".into()],
            },
            trusted_certs: TrustedCerts::CaFiles(vec![]),
            server_name: "x".into(),
        };
        let cc = cfg.to_rustls_client_config();
        assert!(
            cc.is_err(),
            "empty CaFiles must be a misconfiguration error, got Ok"
        );
        let msg = format!("{}", cc.err().unwrap());
        assert!(
            msg.contains("CaFiles") && msg.contains("misconfiguration"),
            "error should mention CaFiles/misconfiguration, got: {msg}"
        );
    }

    // TLS-2: empty `CaBytes(vec![])` is a misconfiguration.
    #[cfg(feature = "tls-rustls")]
    #[test]
    fn tls2_empty_ca_bytes_errors() {
        let cfg = TlsConfig {
            identity: TlsIdentity::SelfSigned {
                subject_alt_names: vec!["localhost".into()],
            },
            trusted_certs: TrustedCerts::CaBytes(vec![]),
            server_name: "x".into(),
        };
        let cc = cfg.to_rustls_client_config();
        assert!(
            cc.is_err(),
            "empty CaBytes must be a misconfiguration error, got Ok"
        );
        let msg = format!("{}", cc.err().unwrap());
        assert!(
            msg.contains("CaBytes") && msg.contains("misconfiguration"),
            "error should mention CaBytes/misconfiguration, got: {msg}"
        );
    }

    // TLS-2: `SkipVerification` is preserved as the explicit opt-out.
    #[cfg(feature = "tls-rustls")]
    #[test]
    fn tls2_skip_verification_still_works() {
        let cfg = TlsConfig::insecure("localhost");
        let cc = cfg.to_rustls_client_config();
        assert!(
            cc.is_ok(),
            "SkipVerification must remain the supported opt-out, got Err: \
             {:?}",
            cc.err()
        );
    }

    // TLS-3: a non-empty PEM byte-blob that decodes to zero certificates
    // is a parse error, not a silent empty trust store.
    #[cfg(feature = "tls-rustls")]
    #[test]
    fn tls3_ca_bytes_with_zero_decoded_certs_errors() {
        let cfg = TlsConfig {
            identity: TlsIdentity::SelfSigned {
                subject_alt_names: vec!["localhost".into()],
            },
            trusted_certs: TrustedCerts::CaBytes(vec![
                b"this looks like text but is not a PEM certificate\n".to_vec(),
            ]),
            server_name: "x".into(),
        };
        let cc = cfg.to_rustls_client_config();
        assert!(
            cc.is_err(),
            "non-empty PEM with zero certs must error, got Ok"
        );
        let msg = format!("{}", cc.err().unwrap());
        assert!(
            msg.contains("0 certificates"),
            "error should mention 0 certificates, got: {msg}"
        );
    }

    // TLS-3: a non-empty CA file that decodes to zero certificates errors.
    #[cfg(feature = "tls-rustls")]
    #[test]
    fn tls3_ca_file_with_zero_decoded_certs_errors() {
        let dir = tempfile::tempdir().unwrap();
        let bad_ca = dir.path().join("bad.pem");
        // PEM-shaped wrapper around a non-cert label — rustls_pemfile
        // accepts no certificates.
        std::fs::write(
            &bad_ca,
            b"-----BEGIN GARBAGE-----\nAAAA\n-----END GARBAGE-----\n",
        )
        .unwrap();

        let cfg = TlsConfig {
            identity: TlsIdentity::SelfSigned {
                subject_alt_names: vec!["localhost".into()],
            },
            trusted_certs: TrustedCerts::CaFiles(vec![bad_ca.clone()]),
            server_name: "x".into(),
        };
        let cc = cfg.to_rustls_client_config();
        assert!(cc.is_err(), "CA file with 0 certificates must error");
        let msg = format!("{}", cc.err().unwrap());
        assert!(
            msg.contains("0 certificates")
                && msg.contains(&bad_ca.display().to_string()),
            "error should mention 0 certificates and the file path, got: \
             {msg}"
        );
    }

    // TLS-4: a `tls-native` server with mTLS intent (non-empty CA roots)
    // must error with an mTLS-specific message, since
    // native_tls::TlsAcceptorBuilder cannot enforce client-cert
    // verification. A warning is not a security boundary. The check
    // runs before identity parsing so that the misconfiguration
    // surfaces independently of any identity-format issues.
    #[cfg(feature = "tls-native")]
    #[test]
    fn tls4_native_acceptor_with_ca_files_intent_errors() {
        let cfg = TlsConfig {
            identity: TlsIdentity::Pkcs12 {
                der: vec![0x30, 0x82, 0x00, 0x10],
                password: "x".into(),
            },
            trusted_certs: TrustedCerts::CaFiles(vec![
                std::path::PathBuf::from("/etc/ssl/certs/ca.pem"),
            ]),
            server_name: "x".into(),
        };
        let r = cfg.to_native_acceptor();
        assert!(
            r.is_err(),
            "mTLS intent on tls-native server must error rather than warn"
        );
        let msg = format!("{}", r.err().unwrap());
        assert!(
            msg.contains("mTLS")
                && msg.contains("tls-native")
                && msg.contains("tls-rustls"),
            "error must point at mTLS / tls-native / tls-rustls remediation, \
             got: {msg}"
        );
    }

    #[cfg(feature = "tls-native")]
    #[test]
    fn tls4_native_acceptor_with_ca_bytes_intent_errors() {
        let cfg = TlsConfig {
            identity: TlsIdentity::Pkcs12 {
                der: vec![0x30, 0x82, 0x00, 0x10],
                password: "x".into(),
            },
            trusted_certs: TrustedCerts::CaBytes(vec![
                b"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n"
                    .to_vec(),
            ]),
            server_name: "x".into(),
        };
        let r = cfg.to_native_acceptor();
        assert!(
            r.is_err(),
            "non-empty CaBytes on tls-native server must error"
        );
        let msg = format!("{}", r.err().unwrap());
        assert!(msg.contains("mTLS"), "error must mention mTLS, got: {msg}");
    }

    // TLS-4: SkipVerification (no mTLS intent) must remain functional on
    // the tls-native server path — the Refusal is conditional on intent.
    #[cfg(feature = "tls-native")]
    #[test]
    fn tls4_native_acceptor_skip_verification_unaffected() {
        // SkipVerification = no mTLS intent, so the new TLS-4 check
        // must not fire. The dummy DER will fail at native_identity,
        // but the failure must NOT come from the mTLS misconfiguration
        // check (verified by message).
        let cfg = TlsConfig {
            identity: TlsIdentity::Pkcs12 {
                der: vec![0x30, 0x82, 0x00, 0x10],
                password: "x".into(),
            },
            trusted_certs: TrustedCerts::SkipVerification,
            server_name: "x".into(),
        };
        let r = cfg.to_native_acceptor();
        if let Err(e) = r {
            let msg = format!("{e}");
            assert!(
                !msg.contains("mTLS"),
                "SkipVerification must not trigger mTLS check, got: {msg}"
            );
        }
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

    // ── for_replication: stricter constructor for production
    //    replication. Rejects every shape that would produce
    //    an unauthenticated transport.

    #[test]
    fn for_replication_rejects_self_signed_identity() {
        let r = TlsConfig::for_replication(
            TlsIdentity::SelfSigned { subject_alt_names: vec!["x".into()] },
            TrustedCerts::CaBytes(vec![
                b"-----BEGIN CERTIFICATE-----".to_vec(),
            ]),
            "x",
        );
        assert!(r.is_err(), "self-signed identity must be rejected");
        let msg = format!("{}", r.err().unwrap());
        assert!(
            msg.contains("SelfSigned") || msg.contains("self-signed"),
            "error must mention SelfSigned, got: {msg}"
        );
    }

    #[test]
    fn for_replication_rejects_skip_verification() {
        let r = TlsConfig::for_replication(
            TlsIdentity::PemBytes { cert: vec![], key: vec![] },
            TrustedCerts::SkipVerification,
            "x",
        );
        assert!(r.is_err());
        let msg = format!("{}", r.err().unwrap());
        assert!(
            msg.contains("SkipVerification") || msg.contains("skip"),
            "error must mention SkipVerification, got: {msg}"
        );
    }

    #[test]
    fn for_replication_rejects_empty_ca_files() {
        let r = TlsConfig::for_replication(
            TlsIdentity::PemFiles {
                cert: "/tmp/cert.pem".into(),
                key: "/tmp/key.pem".into(),
            },
            TrustedCerts::CaFiles(vec![]),
            "x",
        );
        assert!(r.is_err());
        let msg = format!("{}", r.err().unwrap());
        assert!(msg.contains("empty CaFiles"));
    }

    #[test]
    fn for_replication_rejects_empty_ca_bytes() {
        let r = TlsConfig::for_replication(
            TlsIdentity::PemFiles {
                cert: "/tmp/cert.pem".into(),
                key: "/tmp/key.pem".into(),
            },
            TrustedCerts::CaBytes(vec![]),
            "x",
        );
        assert!(r.is_err());
        let msg = format!("{}", r.err().unwrap());
        assert!(msg.contains("empty CaBytes"));
    }

    #[test]
    fn for_replication_accepts_pem_files_with_real_ca() {
        let r = TlsConfig::for_replication(
            TlsIdentity::PemFiles {
                cert: "/etc/noxu/cert.pem".into(),
                key: "/etc/noxu/key.pem".into(),
            },
            TrustedCerts::CaFiles(vec!["/etc/noxu/ca.pem".into()]),
            "node-1.cluster.example",
        );
        assert!(r.is_ok());
        let cfg = r.unwrap();
        assert_eq!(cfg.server_name, "node-1.cluster.example");
    }

    #[test]
    fn for_replication_accepts_pkcs12_with_ca() {
        let r = TlsConfig::for_replication(
            TlsIdentity::Pkcs12 { der: vec![0; 128], password: "p".into() },
            TrustedCerts::CaBytes(vec![
                b"-----BEGIN CERTIFICATE-----".to_vec(),
            ]),
            "node-2",
        );
        assert!(r.is_ok());
    }
}
