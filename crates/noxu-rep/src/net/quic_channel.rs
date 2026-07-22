//! QUIC transport for Noxu DB replication.
//!
//! Provides [`QuicChannel`] and [`QuicChannelListener`] that implement the
//! same [`Channel`] trait as [`super::channel::TcpChannel`], so any code that
//! accepts `Box<dyn Channel>` can switch between TCP and QUIC transparently.
//!
//! ## Feature flag
//!
//! Compiled only when the `quic` cargo feature is enabled:
//!
//! ```toml
//! noxu-rep = { ..., features = ["quic"] }
//! ```
//!
//! ## TLS / certificates
//!
//! QUIC mandates TLS 1.3.  Two paths exist:
//!
//! - **Authenticated (recommended):**
//!   [`QuicChannelListener::bind_with_tls_and_allowlist`] requires a client
//!   certificate and enforces the `peer_allowlist` — mutual TLS, the QUIC
//!   analogue of BDB-JE HA's `SSLAuthenticator`
//!   (`com.sleepycat.je.rep.net`).  Pair it with
//!   [`QuicChannel::connect_with_config`] built from a CA-rooted
//!   [`crate::tls::TlsConfig`].
//! - **Explicit-insecure (trusted network only):**
//!   [`QuicChannel::connect`] / [`default_server_config`] use a self-signed
//!   cert and a no-op `ServerCertVerifier` that skips chain validation.
//!   This path performs **no peer authentication** and is gated at the
//!   environment level by [`crate::RepConfig::insecure_no_auth`] —
//!   `ReplicatedEnvironment::new` refuses to start a `Quic` transport
//!   unless the operator has explicitly opted out of authentication.
//!
//! ## Wire framing
//!
//! Identical to `TcpChannel`: every message is prefixed with a 4-byte
//! little-endian length so that the receiver knows exactly how many bytes to
//! read: `[payload_len: u32 LE][payload bytes]`.
//!
//! ## Synchronous bridge
//!
//! Quinn is an async library built on Tokio.  We bridge the async API to the
//! synchronous `Channel` trait by holding a `Arc<tokio::runtime::Runtime>` and
//! calling `runtime.block_on(future)` for each operation.  Streams are
//! protected by `tokio::sync::Mutex` so that guards can be held across `.await`
//! points without violating borrow rules.

use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use quinn::{Connection, Endpoint, ReadExactError, RecvStream, SendStream};
use rcgen::generate_simple_self_signed;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use tokio::runtime::Runtime;

use crate::error::{RepError, Result};
use crate::net::channel::Channel;

// ---------------------------------------------------------------------------
// TLS helpers
// ---------------------------------------------------------------------------

/// A `ServerCertVerifier` that accepts any certificate without chain
/// validation.
///
/// **INSECURE.**  This performs no authentication of the server — it accepts
/// any presented certificate.  It backs only the explicit-insecure QUIC
/// client path ([`insecure_client_config`] / [`QuicChannel::connect`]),
/// which `ReplicatedEnvironment` refuses to use unless
/// [`crate::RepConfig::insecure_no_auth`] is set.  Never used on the
/// authenticated ([`QuicChannel::connect_with_config`] + CA-rooted
/// [`crate::tls::TlsConfig`]) path.
#[derive(Debug)]
struct SkipCertVerification(Arc<rustls::crypto::CryptoProvider>);

impl rustls::client::danger::ServerCertVerifier for SkipCertVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &rustls::pki_types::ServerName<'_>,
        _ocsp: &[u8],
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
        cert: &CertificateDer<'_>,
        dh: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<
        rustls::client::danger::HandshakeSignatureValid,
        rustls::Error,
    > {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dh,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dh: &rustls::DigitallySignedStruct,
    ) -> std::result::Result<
        rustls::client::danger::HandshakeSignatureValid,
        rustls::Error,
    > {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dh,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}

/// Generate a fresh self-signed certificate for "localhost".
fn self_signed_cert()
-> Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let ck = generate_simple_self_signed(vec!["localhost".to_string()])
        .map_err(|e| RepError::NetworkError(format!("rcgen: {e}")))?;
    let cert = CertificateDer::from(ck.cert.der().to_vec());
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        ck.key_pair.serialize_der(),
    ));
    Ok((vec![cert], key))
}

/// Build a `quinn::ServerConfig` backed by a self-signed certificate.
///
/// **INSECURE — no client authentication.** The server does not request a
/// client certificate (`with_no_client_auth`), so any peer can connect. This
/// is the *explicit-insecure* QUIC path, permitted only when the operator has
/// set [`crate::RepConfig::insecure_no_auth`] (or is driving `QuicChannel`
/// directly on a trusted network).  For mutually-authenticated QUIC use
/// [`QuicChannelListener::bind_with_tls_and_allowlist`], which requires a
/// client certificate and enforces the `peer_allowlist` (the analogue of
/// BDB-JE HA's `SSLAuthenticator`).
pub fn default_server_config() -> Result<quinn::ServerConfig> {
    let (certs, key) = self_signed_cert()?;
    let tls = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| RepError::NetworkError(format!("TLS: {e}")))?;
    let quic_tls = quinn::crypto::rustls::QuicServerConfig::try_from(tls)
        .map_err(|e| {
            RepError::NetworkError(format!("QUIC server config: {e}"))
        })?;
    let mut cfg = quinn::ServerConfig::with_crypto(Arc::new(quic_tls));
    // Disable PMTUD: on loopback the MTU is fixed at 65535, and quinn-proto's
    // MTUD state machine can assert under netem duplicate/corrupt injection.
    let mut transport = quinn::TransportConfig::default();
    transport.mtu_discovery_config(None);
    cfg.transport_config(Arc::new(transport));
    Ok(cfg)
}

/// Build a `quinn::ClientConfig` that skips certificate verification.
///
/// **INSECURE — no server-certificate verification.** Installs a no-op
/// [`SkipCertVerification`] verifier, so the client accepts *any* server
/// certificate (no chain validation, no name check). This is the
/// *explicit-insecure* QUIC client path, permitted only on a trusted network
/// / under [`crate::RepConfig::insecure_no_auth`]. For an authenticated QUIC
/// client use [`QuicChannel::connect_with_config`] with a `quinn::ClientConfig`
/// built from a real CA-rooted [`crate::tls::TlsConfig`].
pub fn insecure_client_config() -> quinn::ClientConfig {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let verifier = SkipCertVerification(Arc::clone(&provider));
    let tls = rustls::ClientConfig::builder()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_no_client_auth();
    let mut cfg = quinn::ClientConfig::new(Arc::new(
        quinn::crypto::rustls::QuicClientConfig::try_from(tls)
            .expect("valid insecure client config"),
    ));
    // Disable PMTUD for the same reason as the server config above.
    let mut transport = quinn::TransportConfig::default();
    transport.mtu_discovery_config(None);
    cfg.transport_config(Arc::new(transport));
    cfg
}

/// 4-byte connection handshake sent by the client immediately after opening
/// the bidirectional stream.  Receiving this header is what makes the stream
/// "visible" on the server side (QUIC sends STREAM frames lazily).
const MAGIC: &[u8; 4] = b"NXUR";

// ---------------------------------------------------------------------------
// QuicChannel
// ---------------------------------------------------------------------------

/// A bidirectional channel backed by a single QUIC stream.
///
/// Wire framing is `[payload_len: u32 LE][payload]`, identical to
/// [`super::channel::TcpChannel`].
///
/// Obtain instances via:
/// - [`QuicChannel::connect`] — client side, insecure (skip-verify) TLS
/// - [`QuicChannel::connect_with_config`] — client side, custom TLS
/// - [`QuicChannelListener::accept`] — server side
pub struct QuicChannel {
    /// Keep the client endpoint alive.  The server side stores `None`
    /// because `QuicChannelListener` owns the server endpoint.
    _endpoint: Option<Endpoint>,
    /// Keep the connection alive for the lifetime of this channel.
    _connection: Connection,
    send: Arc<tokio::sync::Mutex<SendStream>>,
    recv: Arc<tokio::sync::Mutex<RecvStream>>,
    runtime: Arc<Runtime>,
    open: AtomicBool,
}

impl QuicChannel {
    /// Wrap an already-open bidirectional QUIC stream (server-accepted side).
    pub fn from_streams(
        connection: Connection,
        send: SendStream,
        recv: RecvStream,
        runtime: Arc<Runtime>,
    ) -> Self {
        Self {
            _endpoint: None,
            _connection: connection,
            send: Arc::new(tokio::sync::Mutex::new(send)),
            recv: Arc::new(tokio::sync::Mutex::new(recv)),
            runtime,
            open: AtomicBool::new(true),
        }
    }

    /// Connect to a QUIC endpoint using the **insecure** (skip-verify)
    /// client config.  `server_name` must match the SNI in the server
    /// certificate (use `"localhost"` for the self-signed cert produced by
    /// [`default_server_config`]).
    ///
    /// **INSECURE — no server authentication.** See [`insecure_client_config`].
    /// For a mutually-authenticated channel use
    /// [`QuicChannel::connect_with_config`] with a CA-rooted client config.
    pub fn connect(addr: SocketAddr, server_name: &str) -> Result<Self> {
        Self::connect_with_config(addr, server_name, insecure_client_config())
    }

    /// Connect by hostname (or IP string) and port with DNS resolution.
    ///
    /// Happy Eyeballs: IPv6 addresses are tried before IPv4. The first
    /// address that accepts a QUIC connection wins.  `server_name` is passed
    /// as the TLS SNI; use `"localhost"` when connecting to a self-signed cert.
    pub fn connect_host(
        host: &str,
        port: u16,
        server_name: &str,
    ) -> Result<Self> {
        let addrs: Vec<SocketAddr> = (host, port)
            .to_socket_addrs()
            .map_err(|e| {
                RepError::NetworkError(format!(
                    "DNS resolution failed for {host}:{port}: {e}"
                ))
            })?
            .collect();

        if addrs.is_empty() {
            return Err(RepError::NetworkError(format!(
                "no addresses resolved for {host}:{port}"
            )));
        }

        // Happy Eyeballs: prefer IPv6.
        let mut sorted = addrs;
        sorted.sort_by_key(|a| if a.is_ipv6() { 0u8 } else { 1u8 });

        let mut last_err = None;
        for addr in &sorted {
            match Self::connect(*addr, server_name) {
                Ok(ch) => return Ok(ch),
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or_else(|| {
            RepError::NetworkError(format!(
                "could not connect to {host}:{port}"
            ))
        }))
    }

    /// Connect to a QUIC endpoint with a caller-supplied `ClientConfig`.
    pub fn connect_with_config(
        addr: SocketAddr,
        server_name: &str,
        client_cfg: quinn::ClientConfig,
    ) -> Result<Self> {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|e| RepError::NetworkError(format!("tokio: {e}")))?,
        );
        let server_name = server_name.to_string();
        // Keep the endpoint alive; dropping it shuts down the UDP socket background task.
        let (endpoint, conn, send, recv) = runtime.block_on(async move {
            let mut endpoint =
                Endpoint::client("0.0.0.0:0".parse().expect("valid bind addr"))
                    .map_err(|e| RepError::NetworkError(e.to_string()))?;
            endpoint.set_default_client_config(client_cfg);
            let conn = endpoint
                .connect(addr, &server_name)
                .map_err(|e| RepError::NetworkError(e.to_string()))?
                .await
                .map_err(|e| RepError::NetworkError(e.to_string()))?;
            let (mut send, recv) = conn
                .open_bi()
                .await
                .map_err(|e| RepError::NetworkError(e.to_string()))?;
            // Send the connection handshake.  In QUIC, stream frames are sent
            // lazily (only when data is written), so the server's accept_bi()
            // never wakes up unless the client writes at least one byte.
            // A 4-byte magic header opens the stream immediately and lets the
            // server validate the connection before any protocol messages.
            send.write_all(MAGIC)
                .await
                .map_err(|e| RepError::NetworkError(e.to_string()))?;
            Ok::<_, RepError>((endpoint, conn, send, recv))
        })?;
        Ok(Self {
            _endpoint: Some(endpoint),
            _connection: conn,
            send: Arc::new(tokio::sync::Mutex::new(send)),
            recv: Arc::new(tokio::sync::Mutex::new(recv)),
            runtime,
            open: AtomicBool::new(true),
        })
    }
}

impl Channel for QuicChannel {
    /// Send a message.  Writes `[len: u32 LE][payload]` atomically.
    fn send(&self, data: &[u8]) -> Result<()> {
        if !self.is_open() {
            return Err(RepError::ChannelClosed(
                "QuicChannel is closed".into(),
            ));
        }
        let len_prefix = (data.len() as u32).to_le_bytes();
        let payload = data.to_vec();
        // Rust 2024: precise field capture — borrow self.send independently of self.runtime.
        self.runtime.block_on(async {
            let mut stream = self.send.lock().await;
            stream
                .write_all(&len_prefix)
                .await
                .map_err(|e| RepError::NetworkError(e.to_string()))?;
            stream
                .write_all(&payload)
                .await
                .map_err(|e| RepError::NetworkError(e.to_string()))
        })
    }

    /// Receive a message, blocking until data arrives or `timeout` expires.
    ///
    /// Returns `Ok(None)` on timeout.  Returns `Err(ChannelClosed)` if the
    /// peer closed the stream cleanly (EOF).
    fn receive(&self, timeout: Duration) -> Result<Option<Vec<u8>>> {
        if !self.is_open() {
            return Err(RepError::ChannelClosed(
                "QuicChannel is closed".into(),
            ));
        }
        self.runtime.block_on(async {
            let mut stream = self.recv.lock().await;

            // Read the 4-byte length prefix, with a timeout.
            let mut len_buf = [0u8; 4];
            match tokio::time::timeout(timeout, stream.read_exact(&mut len_buf))
                .await
            {
                Err(_elapsed) => return Ok(None),
                Ok(Ok(_n)) => {}
                Ok(Err(ReadExactError::FinishedEarly(_))) => {
                    return Err(RepError::ChannelClosed(
                        "QUIC stream closed by peer".into(),
                    ));
                }
                Ok(Err(ReadExactError::ReadError(e))) => {
                    return Err(RepError::NetworkError(e.to_string()));
                }
            }

            let payload_len = u32::from_le_bytes(len_buf) as usize;
            if payload_len > crate::net::channel::MAX_FRAME_PAYLOAD {
                return Err(RepError::ProtocolError(format!(
                    "frame payload too large: {} > {}",
                    payload_len,
                    crate::net::channel::MAX_FRAME_PAYLOAD
                )));
            }
            let mut payload = vec![0u8; payload_len];
            stream.read_exact(&mut payload).await.map_err(|e| match e {
                ReadExactError::FinishedEarly(_) => RepError::ChannelClosed(
                    "QUIC stream closed mid-payload".into(),
                ),
                ReadExactError::ReadError(re) => {
                    RepError::NetworkError(re.to_string())
                }
            })?;
            Ok(Some(payload))
        })
    }

    /// Close the channel.
    ///
    /// Sends a QUIC stream FIN (marks end-of-data to the peer) and waits
    /// briefly so that any buffered writes are transmitted before the
    /// connection is torn down.
    ///
    /// **Note**: `Connection::close()` is intentionally NOT called here
    /// because it is an immediate/forceful termination that discards any
    /// data still in the local send buffer.  Instead we rely on the
    /// multi-thread Tokio runtime's background workers to flush the QUIC
    /// send buffer while we sleep.
    fn close(&self) -> Result<()> {
        // Swap to false; if it was already false, nothing to do.
        if !self.open.swap(false, Ordering::SeqCst) {
            return Ok(());
        }
        self.runtime.block_on(async {
            let mut stream = self.send.lock().await;
            // FIN the send side.  The QUIC background task will piggyback the
            // FIN on the next STREAM frame or send it standalone.
            let _ = stream.finish();
            // Sleep briefly to allow the background task to transmit all
            // buffered QUIC packets before the Connection is dropped.
            tokio::time::sleep(Duration::from_millis(50)).await;
        });
        Ok(())
    }

    fn is_open(&self) -> bool {
        self.open.load(Ordering::SeqCst)
    }
}

impl Drop for QuicChannel {
    /// Ensure buffered data is flushed before the connection is torn down.
    fn drop(&mut self) {
        if self.is_open() {
            // Ignore errors — best-effort flush on implicit drop.
            let _ = self.close();
        }
    }
}

// ---------------------------------------------------------------------------
// QuicChannelListener
// ---------------------------------------------------------------------------

/// Accepts incoming QUIC connections and wraps each first bidirectional stream
/// in a [`QuicChannel`].
///
/// The shared [`Runtime`] is passed to every accepted channel so that all
/// channels originating from this listener share the same Tokio event loop.
pub struct QuicChannelListener {
    endpoint: Endpoint,
    runtime: Arc<Runtime>,
}

impl QuicChannelListener {
    /// Bind to `addr` using a freshly-generated self-signed certificate.
    pub fn bind(addr: SocketAddr) -> Result<Self> {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|e| RepError::NetworkError(format!("tokio: {e}")))?,
        );
        let server_cfg = default_server_config()?;
        let endpoint = runtime.block_on(async move {
            Endpoint::server(server_cfg, addr)
                .map_err(|e| RepError::NetworkError(e.to_string()))
        })?;
        Ok(Self { endpoint, runtime })
    }

    /// Bind using a caller-supplied `ServerConfig`.
    pub fn with_server_config(
        addr: SocketAddr,
        server_cfg: quinn::ServerConfig,
    ) -> Result<Self> {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|e| RepError::NetworkError(format!("tokio: {e}")))?,
        );
        let endpoint = runtime.block_on(async move {
            Endpoint::server(server_cfg, addr)
                .map_err(|e| RepError::NetworkError(e.to_string()))
        })?;
        Ok(Self { endpoint, runtime })
    }

    /// Bind to `addr` and enforce `allowlist` via mTLS.
    ///
    /// This is the **QUIC mTLS enforcement constructor** introduced in
    /// Phase 3.  The QUIC server will:
    ///
    /// 1. Require a client certificate on every incoming connection.
    /// 2. Validate the chain against the CA roots in `tls`.
    /// 3. Reject the connection if the peer's Subject CN / DNS SANs are
    ///    not in `allowlist` (before any application data is exchanged).
    ///
    /// The empty-allowlist fail-closed policy applies: an empty `allowlist`
    /// returns `Err(RepError::ConfigError)`.
    ///
    /// `tls` must not use `TrustedCerts::SkipVerification` (same restriction
    /// as the TCP-TLS path — `SkipVerification` provides no CA for chain
    /// validation).
    pub fn bind_with_tls_and_allowlist(
        addr: SocketAddr,
        tls: &crate::tls::TlsConfig,
        allowlist: crate::auth::PeerAllowlist,
    ) -> Result<Self> {
        let server_cfg =
            tls.to_quinn_server_config_with_allowlist(allowlist)?;
        Self::with_server_config(addr, server_cfg)
    }

    /// Return the local address the endpoint is bound to.
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.endpoint
            .local_addr()
            .map_err(|e| RepError::NetworkError(e.to_string()))
    }

    /// Accept the next incoming QUIC connection, blocking until one arrives.
    ///
    /// Waits for the connecting peer to open a bidirectional stream and send
    /// the 4-byte connection handshake (`NXUR`).  Returns a `QuicChannel`
    /// wrapping the accepted stream.
    pub fn accept(&self) -> Result<QuicChannel> {
        // Rust 2024: self.endpoint and self.runtime are separate field borrows.
        // Clone runtime once inside the future (instead of twice outside).
        self.runtime.block_on(async {
            let incoming = self.endpoint.accept().await.ok_or_else(|| {
                RepError::NetworkError("QUIC endpoint closed".into())
            })?;
            let conn = incoming
                .await
                .map_err(|e| RepError::NetworkError(e.to_string()))?;
            let (send, mut recv) = conn
                .accept_bi()
                .await
                .map_err(|e| RepError::NetworkError(e.to_string()))?;
            // Read and validate the 4-byte connection handshake.
            let mut magic = [0u8; 4];
            recv.read_exact(&mut magic).await.map_err(|e| {
                RepError::NetworkError(format!("handshake read: {e}"))
            })?;
            if &magic != MAGIC {
                return Err(RepError::NetworkError(format!(
                    "invalid QUIC handshake magic: {magic:02x?}"
                )));
            }
            Ok(QuicChannel::from_streams(
                conn,
                send,
                recv,
                Arc::clone(&self.runtime),
            ))
        })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn loopback_listener() -> QuicChannelListener {
        QuicChannelListener::bind("127.0.0.1:0".parse().unwrap())
            .expect("bind QUIC listener")
    }

    #[test]
    fn test_quic_basic_send_receive() {
        let listener = loopback_listener();
        let addr = listener.local_addr().unwrap();

        let server_thread = std::thread::spawn(move || {
            let ch = listener.accept().unwrap();
            let msg = ch.receive(Duration::from_secs(5)).unwrap();
            assert_eq!(msg, Some(b"hello quic".to_vec()));
            ch.send(b"world").unwrap();
        });

        let client = QuicChannel::connect(addr, "localhost").unwrap();
        client.send(b"hello quic").unwrap();
        let reply = client.receive(Duration::from_secs(5)).unwrap();
        assert_eq!(reply, Some(b"world".to_vec()));

        server_thread.join().unwrap();
    }

    #[test]
    fn test_quic_empty_message() {
        let listener = loopback_listener();
        let addr = listener.local_addr().unwrap();

        let server_thread = std::thread::spawn(move || {
            let ch = listener.accept().unwrap();
            let msg = ch.receive(Duration::from_secs(5)).unwrap();
            assert_eq!(msg, Some(vec![]));
        });

        let client = QuicChannel::connect(addr, "localhost").unwrap();
        client.send(b"").unwrap();
        server_thread.join().unwrap();
    }

    #[test]
    fn test_quic_large_message() {
        let listener = loopback_listener();
        let addr = listener.local_addr().unwrap();
        let payload: Vec<u8> = (0u32..65536).map(|i| (i % 256) as u8).collect();
        let expected = payload.clone();

        let server_thread = std::thread::spawn(move || {
            let ch = listener.accept().unwrap();
            let msg = ch.receive(Duration::from_secs(5)).unwrap().unwrap();
            assert_eq!(msg, expected);
        });

        let client = QuicChannel::connect(addr, "localhost").unwrap();
        client.send(&payload).unwrap();
        server_thread.join().unwrap();
    }

    #[test]
    fn test_quic_multiple_messages_fifo() {
        let listener = loopback_listener();
        let addr = listener.local_addr().unwrap();

        let server_thread = std::thread::spawn(move || {
            let ch = listener.accept().unwrap();
            for i in 0u8..5 {
                let msg = ch.receive(Duration::from_secs(5)).unwrap().unwrap();
                assert_eq!(msg, vec![i]);
            }
        });

        let client = QuicChannel::connect(addr, "localhost").unwrap();
        for i in 0u8..5 {
            client.send(&[i]).unwrap();
        }
        server_thread.join().unwrap();
    }

    #[test]
    fn test_quic_receive_timeout() {
        let listener = loopback_listener();
        let addr = listener.local_addr().unwrap();

        // Server accepts but never sends; client receive should time out.
        let server_thread =
            std::thread::spawn(move || listener.accept().unwrap());

        let client = QuicChannel::connect(addr, "localhost").unwrap();
        // 300 ms — long enough to avoid flakiness under test-parallelism load
        // but short enough that the test completes quickly.
        let result = client.receive(Duration::from_millis(300)).unwrap();
        assert_eq!(result, None, "expected timeout → None");

        drop(server_thread.join().unwrap());
    }

    #[test]
    fn test_quic_is_open_and_close() {
        let listener = loopback_listener();
        let addr = listener.local_addr().unwrap();
        let server_thread = std::thread::spawn(move || {
            listener.accept().unwrap();
        });

        let client = QuicChannel::connect(addr, "localhost").unwrap();
        assert!(client.is_open());
        client.close().unwrap();
        assert!(!client.is_open());

        server_thread.join().unwrap();
    }

    #[test]
    fn test_quic_send_after_close_fails() {
        let listener = loopback_listener();
        let addr = listener.local_addr().unwrap();
        let server_thread = std::thread::spawn(move || {
            listener.accept().unwrap();
        });

        let client = QuicChannel::connect(addr, "localhost").unwrap();
        client.close().unwrap();
        assert!(client.send(b"should fail").is_err());

        server_thread.join().unwrap();
    }

    #[test]
    fn test_quic_bidirectional() {
        let listener = loopback_listener();
        let addr = listener.local_addr().unwrap();

        let server_thread = std::thread::spawn(move || {
            let ch = listener.accept().unwrap();
            let from_client = ch.receive(Duration::from_secs(5)).unwrap();
            assert_eq!(from_client, Some(b"ping".to_vec()));
            ch.send(b"pong").unwrap();
            let from_client2 = ch.receive(Duration::from_secs(5)).unwrap();
            assert_eq!(from_client2, Some(b"done".to_vec()));
        });

        let client = QuicChannel::connect(addr, "localhost").unwrap();
        client.send(b"ping").unwrap();
        let reply = client.receive(Duration::from_secs(5)).unwrap();
        assert_eq!(reply, Some(b"pong".to_vec()));
        client.send(b"done").unwrap();

        server_thread.join().unwrap();
    }

    #[test]
    fn test_quic_local_addr() {
        let listener = loopback_listener();
        let addr = listener.local_addr().unwrap();
        assert_eq!(addr.ip().to_string(), "127.0.0.1");
        assert_ne!(addr.port(), 0);
    }

    #[test]
    fn test_quic_channel_implements_channel_trait() {
        let listener = loopback_listener();
        let addr = listener.local_addr().unwrap();
        let server_thread = std::thread::spawn(move || {
            let ch = listener.accept().unwrap();
            let _: &dyn Channel = &ch; // verify it's object-safe
            ch.receive(Duration::from_secs(5)).unwrap()
        });

        let client = QuicChannel::connect(addr, "localhost").unwrap();
        let ch: Box<dyn Channel> = Box::new(client); // verify boxable
        ch.send(b"trait test").unwrap();
        let msg = server_thread.join().unwrap();
        assert_eq!(msg, Some(b"trait test".to_vec()));
    }

    /// LOG-2: QuicChannel must reject a peer-supplied `payload_len` that
    /// exceeds [`crate::net::channel::MAX_FRAME_PAYLOAD`] before allocating the
    /// payload buffer.
    #[test]
    fn test_quic_rejects_oversize_frame() {
        let listener = loopback_listener();
        let addr = listener.local_addr().unwrap();

        // Server sends an oversized payload via the normal `send` path,
        // which writes a length prefix that exceeds the cap.  The client
        // must reject after reading the 4-byte header.
        let server_thread = std::thread::spawn(move || {
            let ch = listener.accept().unwrap();
            let oversized =
                vec![0u8; crate::net::channel::MAX_FRAME_PAYLOAD + 1];
            // Result is intentionally ignored: when the client tears the
            // stream down after rejecting the header, this `send` may fail.
            let _ = ch.send(&oversized);
        });

        let client = QuicChannel::connect(addr, "localhost").unwrap();
        let result = client.receive(Duration::from_secs(10));
        let _ = client.close();
        let err = result.expect_err("oversize QUIC frame must be rejected");
        match err {
            RepError::ProtocolError(msg) => {
                assert!(
                    msg.contains("frame payload too large"),
                    "unexpected protocol-error message: {}",
                    msg
                );
            }
            other => panic!("expected ProtocolError, got {:?}", other),
        }
        let _ = server_thread.join();
    }
}
