//! QUIC multiplexed transport for Noxu DB replication.
//!
//! Extends the single-stream [`QuicChannel`] with true QUIC stream
//! multiplexing: one QUIC connection carries four independent streams, one
//! per replication concern, plus unreliable datagrams for CBVLSN broadcasts.
//!
//! ## Feature flag
//!
//! Compiled only when the `quic` cargo feature is enabled.
//!
//! ## Stream layout
//!
//! | Stream | ID | Direction | Purpose                       |
//! |--------|----|-----------|-------------------------------|
//! | Heartbeat | 0 | Bidirectional | Elections and heartbeats   |
//! | Log       | 1 | M → R     | Log-entry shipping            |
//! | Ack       | 2 | R → M     | Commit acknowledgements       |
//! | Restore   | 3 | M → R     | Network restore file xfer     |
//!
//! ## Why separate streams matter
//!
//! QUIC enforces per-stream flow control.  On TCP, a large log-shipping burst
//! fills the socket send buffer and delays the next heartbeat packet — the
//! classic head-of-line blocking problem.  With separate QUIC streams, log
//! back-pressure on stream 1 has no effect on stream 0 heartbeats, so the
//! `PhiAccrualDetector` sees a tighter inter-arrival distribution and is less
//! prone to false elections.
//!
//! ## CBVLSN datagrams
//!
//! CBVLSN (Cluster-Based VLSN) heartbeat values are broadcast as 8-byte
//! unreliable QUIC datagrams (`RFC 9221`).  A lost datagram is immediately
//! superseded by the next broadcast (~10 ms later), so reliability is
//! unnecessary and the overhead of retransmission is avoided.
//!
//! ## Wire handshake
//!
//! On connection the client opens four bidirectional QUIC streams (in order
//! 0–3) and writes a 5-byte handshake on each:
//! `[NXMX: 4 bytes][stream_type: 1 byte]`.
//! The server accepts four streams and validates the magic + type.
//!
//! ## 0-RTT reconnect
//!
//! Because `QuicMultiplexedChannel::connect` stores the underlying `Endpoint`,
//! TLS session tickets from the initial connection are cached.  On master
//! failover, extract the endpoint via [`QuicMultiplexedChannel::take_endpoint`]
//! and call [`QuicMultiplexedChannel::connect_with_endpoint`]; Quinn uses the
//! cached session ticket and issues a 0-RTT / early-data reconnect, cutting
//! reconnect latency from ~3 RTT (TCP+TLS) to ~1 RTT.

use hashbrown::HashMap;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use bytes::Bytes;
use quinn::{Connection, Endpoint, ReadExactError, RecvStream, SendStream};
use tokio::runtime::Runtime;

use super::quic_channel::{default_server_config, insecure_client_config};
use crate::rep::error::{RepError, Result};
use crate::rep::net::channel::Channel;

// ---------------------------------------------------------------------------
// Wire protocol constants
// ---------------------------------------------------------------------------

/// Magic bytes identifying a multiplexed QUIC connection.
/// Distinguishes from single-stream [`QuicChannel`] (which uses `b"NXUR"`).
const MULTIPLEXED_MAGIC: &[u8; 4] = b"NXMX";

/// Stream type identifiers sent as the 5th byte of the per-stream handshake.
const STREAM_HEARTBEAT: u8 = 0;
const STREAM_LOG: u8 = 1;
const STREAM_ACK: u8 = 2;
const STREAM_RESTORE: u8 = 3;

/// Total number of streams opened per multiplexed connection.
const NUM_STREAMS: usize = 4;

// ---------------------------------------------------------------------------
// Transport config helpers
// ---------------------------------------------------------------------------

/// Build a `quinn::ServerConfig` with datagrams enabled (64 KiB receive
/// buffer).  Used by [`QuicMultiplexedChannelListener::bind`].
pub fn mux_server_config() -> Result<quinn::ServerConfig> {
    let mut cfg = default_server_config()?;
    let mut transport = quinn::TransportConfig::default();
    // Disable PMTUD: loopback MTU is fixed; MTUD asserts under netem
    // duplicate/corrupt injection (quinn-proto mtud.rs:88).
    transport.mtu_discovery_config(None);
    transport.datagram_receive_buffer_size(Some(64 * 1024));
    cfg.transport_config(Arc::new(transport));
    Ok(cfg)
}

/// Build a skip-verify `quinn::ClientConfig` with datagrams enabled.
/// Used by [`QuicMultiplexedChannel::connect`].
pub fn mux_insecure_client_config() -> quinn::ClientConfig {
    let mut cfg = insecure_client_config();
    let mut transport = quinn::TransportConfig::default();
    // Disable PMTUD: same reason as mux_server_config.
    transport.mtu_discovery_config(None);
    transport.datagram_receive_buffer_size(Some(64 * 1024));
    cfg.transport_config(Arc::new(transport));
    cfg
}

// ---------------------------------------------------------------------------
// QuicSubChannel  (one QUIC stream, implements Channel)
// ---------------------------------------------------------------------------

/// A [`Channel`] backed by a single QUIC bidirectional stream.
///
/// Wire framing: `[payload_len: u32 LE][payload bytes]`, identical to
/// [`TcpChannel`](super::channel::TcpChannel).
///
/// All four streams of a [`QuicMultiplexedChannel`] are `QuicSubChannel`
/// instances that share the same Tokio runtime.
pub(super) struct QuicSubChannel {
    pub(super) send: Arc<tokio::sync::Mutex<SendStream>>,
    recv: Arc<tokio::sync::Mutex<RecvStream>>,
    runtime: Arc<Runtime>,
    pub(super) open: AtomicBool,
}

impl QuicSubChannel {
    fn new(send: SendStream, recv: RecvStream, runtime: Arc<Runtime>) -> Self {
        Self {
            send: Arc::new(tokio::sync::Mutex::new(send)),
            recv: Arc::new(tokio::sync::Mutex::new(recv)),
            runtime,
            open: AtomicBool::new(true),
        }
    }
}

impl Channel for QuicSubChannel {
    fn send(&self, data: &[u8]) -> Result<()> {
        if !self.is_open() {
            return Err(RepError::ChannelClosed(
                "QuicSubChannel is closed".into(),
            ));
        }
        let len_prefix = (data.len() as u32).to_le_bytes();
        let payload = data.to_vec();
        // Rust 2024 edition: async block captures self.send by field (not all of self),
        // so borrowing self.runtime for block_on and self.send inside the future is fine.
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

    fn receive(&self, timeout: Duration) -> Result<Option<Vec<u8>>> {
        if !self.is_open() {
            return Err(RepError::ChannelClosed(
                "QuicSubChannel is closed".into(),
            ));
        }
        self.runtime.block_on(async {
            let mut stream = self.recv.lock().await;

            let mut len_buf = [0u8; 4];
            match tokio::time::timeout(timeout, stream.read_exact(&mut len_buf))
                .await
            {
                Err(_elapsed) => return Ok(None),
                Ok(Ok(_)) => {}
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
            if payload_len > crate::rep::net::channel::MAX_FRAME_PAYLOAD {
                return Err(RepError::ProtocolError(format!(
                    "frame payload too large: {} > {}",
                    payload_len,
                    crate::rep::net::channel::MAX_FRAME_PAYLOAD
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

    fn close(&self) -> Result<()> {
        if !self.open.swap(false, Ordering::SeqCst) {
            return Ok(());
        }
        self.runtime.block_on(async {
            let mut stream = self.send.lock().await;
            let _ = stream.finish();
            tokio::time::sleep(Duration::from_millis(50)).await;
        });
        Ok(())
    }

    fn is_open(&self) -> bool {
        self.open.load(Ordering::SeqCst)
    }
}

// ---------------------------------------------------------------------------
// ReplicationChannel trait
// ---------------------------------------------------------------------------

/// Stream-typed channel interface for Noxu replication over QUIC.
///
/// Provides per-concern stream accessors and CBVLSN datagram support.
/// Each stream operates independently under QUIC flow control: log-shipping
/// back-pressure on [`log_channel`] cannot delay heartbeats on
/// [`heartbeat_channel`].
///
/// [`log_channel`]: ReplicationChannel::log_channel
/// [`heartbeat_channel`]: ReplicationChannel::heartbeat_channel
pub trait ReplicationChannel: Send + Sync {
    /// Bidirectional stream for heartbeats and elections (stream 0).
    fn heartbeat_channel(&self) -> &dyn Channel;

    /// Stream for log shipping, master → replica (stream 1).
    fn log_channel(&self) -> &dyn Channel;

    /// Stream for ack / commit feedback, replica → master (stream 2).
    fn ack_channel(&self) -> &dyn Channel;

    /// Stream for network restore file transfer (stream 3).
    fn restore_channel(&self) -> &dyn Channel;

    /// Broadcast a CBVLSN value as an unreliable 8-byte datagram.
    ///
    /// Datagrams are fresh-only: a lost datagram is superseded by the next
    /// broadcast in ~10 ms, so reliability is unnecessary.  This is a
    /// non-blocking call; the QUIC stack enqueues the datagram immediately.
    fn send_vlsn_datagram(&self, vlsn: i64) -> Result<()>;

    /// Receive a CBVLSN datagram, blocking until one arrives or `timeout`
    /// expires.
    ///
    /// Returns `Ok(None)` on timeout.
    fn recv_vlsn_datagram(&self, timeout: Duration) -> Result<Option<i64>>;
}

// ---------------------------------------------------------------------------
// ReconnectToken
// ---------------------------------------------------------------------------

/// Token produced by [`QuicMultiplexedChannel::into_reconnect_token`] that
/// bundles the QUIC `Endpoint` and its Tokio `Runtime` for a subsequent
/// 0-RTT reconnect.
///
/// The `Runtime` **must** be kept alive alongside the `Endpoint`: Quinn's
/// endpoint background task runs on the runtime that created the endpoint, so
/// dropping the runtime invalidates the endpoint with "endpoint stopping".
pub struct ReconnectToken {
    /// The QUIC endpoint with cached TLS session tickets.
    pub endpoint: Endpoint,
    /// The Tokio runtime driving the endpoint's UDP socket background task.
    pub runtime: Arc<Runtime>,
}

// ---------------------------------------------------------------------------
// QuicMultiplexedChannel
// ---------------------------------------------------------------------------

/// A QUIC connection multiplexed into four independent per-concern streams
/// plus unreliable datagrams for CBVLSN broadcasts.
///
/// Obtain instances via:
/// - [`QuicMultiplexedChannel::connect`] — client side, insecure TLS
/// - [`QuicMultiplexedChannelListener::accept`] — server side
pub struct QuicMultiplexedChannel {
    /// Keep the client endpoint alive (None for server-accepted channels).
    endpoint: Option<Endpoint>,
    /// Named connection handle so we can call `send_datagram` / `read_datagram`.
    connection: Connection,
    heartbeat: QuicSubChannel,
    log: QuicSubChannel,
    ack: QuicSubChannel,
    restore: QuicSubChannel,
    runtime: Arc<Runtime>,
    open: AtomicBool,
}

impl QuicMultiplexedChannel {
    fn build_runtime() -> Result<Arc<Runtime>> {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .map(Arc::new)
            .map_err(|e| RepError::NetworkError(format!("tokio: {e}")))
    }

    /// Connect to a multiplexed QUIC endpoint using insecure (skip-verify)
    /// TLS.  `server_name` must match the server's SNI (use `"localhost"` for
    /// the self-signed cert from [`mux_server_config`]).
    pub fn connect(addr: SocketAddr, server_name: &str) -> Result<Self> {
        let runtime = Self::build_runtime()?;
        Self::connect_inner(
            addr,
            server_name,
            mux_insecure_client_config(),
            runtime,
            None,
        )
    }

    /// Connect using a caller-supplied `ClientConfig`.
    pub fn connect_with_config(
        addr: SocketAddr,
        server_name: &str,
        client_cfg: quinn::ClientConfig,
    ) -> Result<Self> {
        let runtime = Self::build_runtime()?;
        Self::connect_inner(addr, server_name, client_cfg, runtime, None)
    }

    /// Connect by hostname (or IP string) and port with DNS resolution.
    ///
    /// Happy Eyeballs: IPv6 addresses are tried before IPv4. The first address
    /// that completes the QUIC handshake wins.
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

    /// Reconnect using a [`ReconnectToken`] produced by
    /// [`into_reconnect_token`].
    ///
    /// The token's `Runtime` is reused so that the endpoint's UDP background
    /// task remains alive, and the endpoint's cached TLS session ticket enables
    /// a 0-RTT / early-data handshake.
    ///
    /// [`into_reconnect_token`]: QuicMultiplexedChannel::into_reconnect_token
    pub fn connect_with_token(
        token: ReconnectToken,
        addr: SocketAddr,
        server_name: &str,
    ) -> Result<Self> {
        Self::connect_inner(
            addr,
            server_name,
            mux_insecure_client_config(),
            token.runtime,
            Some(token.endpoint),
        )
    }

    /// Reconnect reusing an existing `Endpoint`.
    ///
    /// Because the `Endpoint` retains TLS session tickets from the previous
    /// connection, Quinn will use 0-RTT / early data when the server's session
    /// ticket has not yet expired — cutting reconnect latency to ~1 RTT.
    ///
    /// Obtain the endpoint from a prior channel with [`take_endpoint`].
    ///
    /// [`take_endpoint`]: QuicMultiplexedChannel::take_endpoint
    pub fn connect_with_endpoint(
        endpoint: Endpoint,
        addr: SocketAddr,
        server_name: &str,
    ) -> Result<Self> {
        let runtime = Self::build_runtime()?;
        Self::connect_inner(
            addr,
            server_name,
            mux_insecure_client_config(),
            runtime,
            Some(endpoint),
        )
    }

    fn connect_inner(
        addr: SocketAddr,
        server_name: &str,
        client_cfg: quinn::ClientConfig,
        runtime: Arc<Runtime>,
        existing_endpoint: Option<Endpoint>,
    ) -> Result<Self> {
        let server_name = server_name.to_string();

        // Block on all async QUIC setup; returns the endpoint, connection, and
        // a HashMap of (SendStream, RecvStream) keyed by stream type byte.
        let (endpoint, conn, mut stream_map): (
            Endpoint,
            Connection,
            HashMap<u8, (SendStream, RecvStream)>,
        ) = runtime.block_on(async move {
            let mut endpoint = match existing_endpoint {
                Some(ep) => ep,
                None => Endpoint::client(
                    "0.0.0.0:0".parse().expect("valid bind addr"),
                )
                .map_err(|e| RepError::NetworkError(e.to_string()))?,
            };
            endpoint.set_default_client_config(client_cfg);
            let conn = endpoint
                .connect(addr, &server_name)
                .map_err(|e| RepError::NetworkError(e.to_string()))?
                .await
                .map_err(|e| RepError::NetworkError(e.to_string()))?;

            // Open streams in canonical order: heartbeat, log, ack, restore.
            let stream_types =
                [STREAM_HEARTBEAT, STREAM_LOG, STREAM_ACK, STREAM_RESTORE];
            let mut map: HashMap<u8, (SendStream, RecvStream)> =
                HashMap::with_capacity(NUM_STREAMS);

            for stream_type in stream_types {
                let (mut send, recv) = conn
                    .open_bi()
                    .await
                    .map_err(|e| RepError::NetworkError(e.to_string()))?;
                // 5-byte handshake: magic (4) + stream type (1).
                // Sending data immediately forces a QUIC STREAM frame, making
                // the stream visible to the server's accept_bi().
                let mut handshake = [0u8; 5];
                handshake[..4].copy_from_slice(MULTIPLEXED_MAGIC);
                handshake[4] = stream_type;
                send.write_all(&handshake)
                    .await
                    .map_err(|e| RepError::NetworkError(e.to_string()))?;
                map.insert(stream_type, (send, recv));
            }

            Ok::<_, RepError>((endpoint, conn, map))
        })?;

        let (hb_s, hb_r) =
            stream_map.remove(&STREAM_HEARTBEAT).ok_or_else(|| {
                RepError::NetworkError("missing heartbeat stream".into())
            })?;
        let (log_s, log_r) =
            stream_map.remove(&STREAM_LOG).ok_or_else(|| {
                RepError::NetworkError("missing log stream".into())
            })?;
        let (ack_s, ack_r) =
            stream_map.remove(&STREAM_ACK).ok_or_else(|| {
                RepError::NetworkError("missing ack stream".into())
            })?;
        let (rst_s, rst_r) =
            stream_map.remove(&STREAM_RESTORE).ok_or_else(|| {
                RepError::NetworkError("missing restore stream".into())
            })?;

        let (hb_rt, log_rt, ack_rt, rst_rt) = (
            Arc::clone(&runtime),
            Arc::clone(&runtime),
            Arc::clone(&runtime),
            Arc::clone(&runtime),
        );

        Ok(Self {
            endpoint: Some(endpoint),
            connection: conn,
            heartbeat: QuicSubChannel::new(hb_s, hb_r, hb_rt),
            log: QuicSubChannel::new(log_s, log_r, log_rt),
            ack: QuicSubChannel::new(ack_s, ack_r, ack_rt),
            restore: QuicSubChannel::new(rst_s, rst_r, rst_rt),
            runtime,
            open: AtomicBool::new(true),
        })
    }

    /// Consume this channel and return a [`ReconnectToken`] for 0-RTT reconnect.
    ///
    /// The token bundles the QUIC `Endpoint` **and** its Tokio `Runtime`.
    /// Both must stay alive together: the runtime drives the endpoint's UDP
    /// socket background task, and dropping it would invalidate the endpoint
    /// with "endpoint stopping".
    ///
    /// Pass the token to [`connect_with_token`] to open a new multiplexed
    /// connection to the new master, reusing the cached TLS session ticket for
    /// 0-RTT / early-data.
    ///
    /// Returns `None` for server-accepted channels (the listener owns their
    /// endpoint).
    ///
    /// [`connect_with_token`]: QuicMultiplexedChannel::connect_with_token
    pub fn into_reconnect_token(mut self) -> Option<ReconnectToken> {
        let endpoint = self.endpoint.take()?;
        let runtime = Arc::clone(&self.runtime);
        Some(ReconnectToken { endpoint, runtime })
    }

    /// Returns `true` if the channel has not been closed.
    pub fn is_open(&self) -> bool {
        self.open.load(Ordering::SeqCst)
    }

    /// Close all four streams simultaneously.
    ///
    /// Sends a QUIC stream FIN on every stream and waits 50 ms for the
    /// background task to flush any buffered packets before the connection
    /// reference is dropped.
    pub fn close_all(&self) -> Result<()> {
        if !self.open.swap(false, Ordering::SeqCst) {
            return Ok(());
        }
        // Mark every sub-channel as closed so in-flight operations fail fast.
        self.heartbeat.open.store(false, Ordering::SeqCst);
        self.log.open.store(false, Ordering::SeqCst);
        self.ack.open.store(false, Ordering::SeqCst);
        self.restore.open.store(false, Ordering::SeqCst);

        // Rust 2024: async block captures each sub-channel's send field precisely.
        self.runtime.block_on(async {
            let _ = self.heartbeat.send.lock().await.finish();
            let _ = self.log.send.lock().await.finish();
            let _ = self.ack.send.lock().await.finish();
            let _ = self.restore.send.lock().await.finish();
            tokio::time::sleep(Duration::from_millis(50)).await;
        });
        Ok(())
    }
}

impl ReplicationChannel for QuicMultiplexedChannel {
    fn heartbeat_channel(&self) -> &dyn Channel {
        &self.heartbeat
    }

    fn log_channel(&self) -> &dyn Channel {
        &self.log
    }

    fn ack_channel(&self) -> &dyn Channel {
        &self.ack
    }

    fn restore_channel(&self) -> &dyn Channel {
        &self.restore
    }

    /// Enqueue a CBVLSN datagram on the connection.  Non-blocking; the QUIC
    /// stack sends the 8-byte datagram as soon as the congestion window allows.
    fn send_vlsn_datagram(&self, vlsn: i64) -> Result<()> {
        self.connection
            .send_datagram(Bytes::from(vlsn.to_le_bytes().to_vec()))
            .map_err(|e| RepError::NetworkError(format!("datagram send: {e}")))
    }

    fn recv_vlsn_datagram(&self, timeout: Duration) -> Result<Option<i64>> {
        self.runtime.block_on(async {
            match tokio::time::timeout(timeout, self.connection.read_datagram())
                .await
            {
                Err(_elapsed) => Ok(None),
                Ok(Ok(data)) => {
                    if data.len() != 8 {
                        return Err(RepError::NetworkError(format!(
                            "VLSN datagram: expected 8 bytes, got {}",
                            data.len()
                        )));
                    }
                    let bytes: [u8; 8] =
                        data[..8].try_into().expect("length checked above");
                    Ok(Some(i64::from_le_bytes(bytes)))
                }
                Ok(Err(e)) => {
                    Err(RepError::NetworkError(format!("datagram recv: {e}")))
                }
            }
        })
    }
}

impl Drop for QuicMultiplexedChannel {
    fn drop(&mut self) {
        if self.is_open() {
            let _ = self.close_all();
        }
    }
}

// ---------------------------------------------------------------------------
// QuicMultiplexedChannelListener
// ---------------------------------------------------------------------------

/// Accepts incoming multiplexed QUIC connections and returns
/// [`QuicMultiplexedChannel`] instances.
///
/// The server endpoint is configured with datagrams enabled so that CBVLSN
/// broadcasts sent by clients are not silently discarded.
pub struct QuicMultiplexedChannelListener {
    endpoint: Endpoint,
    runtime: Arc<Runtime>,
}

impl QuicMultiplexedChannelListener {
    /// Bind to `addr` using a self-signed certificate with datagrams enabled.
    pub fn bind(addr: SocketAddr) -> Result<Self> {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .map_err(|e| RepError::NetworkError(format!("tokio: {e}")))?,
        );
        let server_cfg = mux_server_config()?;
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

    /// Return the local address the listener is bound to.
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.endpoint
            .local_addr()
            .map_err(|e| RepError::NetworkError(e.to_string()))
    }

    /// Accept the next incoming multiplexed QUIC connection.
    ///
    /// Waits for the client to open four bidirectional streams and validates
    /// the 5-byte handshake on each stream before returning.
    pub fn accept(&self) -> Result<QuicMultiplexedChannel> {
        // Rust 2024: borrow self.endpoint and self.runtime as separate fields;
        // the runtime Arc is cloned once inside the future (not twice outside).
        self.runtime.block_on(async {
            let incoming = self.endpoint.accept().await.ok_or_else(|| {
                RepError::NetworkError("QUIC endpoint closed".into())
            })?;
            let conn = incoming
                .await
                .map_err(|e| RepError::NetworkError(e.to_string()))?;

            // Accept NUM_STREAMS streams; the client opens them in type order
            // but we key by the type byte from the handshake, so any ordering
            // is accepted safely.
            let mut stream_map: HashMap<u8, (SendStream, RecvStream)> =
                HashMap::with_capacity(NUM_STREAMS);

            for _ in 0..NUM_STREAMS {
                let (send, mut recv) = conn
                    .accept_bi()
                    .await
                    .map_err(|e| RepError::NetworkError(e.to_string()))?;

                let mut handshake = [0u8; 5];
                recv.read_exact(&mut handshake).await.map_err(|e| {
                    RepError::NetworkError(format!("mux handshake: {e}"))
                })?;

                if &handshake[..4] != MULTIPLEXED_MAGIC {
                    return Err(RepError::NetworkError(format!(
                        "invalid mux magic: {:02x?}",
                        &handshake[..4]
                    )));
                }
                let stream_type = handshake[4];
                if stream_map.contains_key(&stream_type) {
                    return Err(RepError::NetworkError(format!(
                        "duplicate stream type {stream_type}"
                    )));
                }
                stream_map.insert(stream_type, (send, recv));
            }

            let (hb_s, hb_r) =
                stream_map.remove(&STREAM_HEARTBEAT).ok_or_else(|| {
                    RepError::NetworkError("missing heartbeat stream".into())
                })?;
            let (log_s, log_r) =
                stream_map.remove(&STREAM_LOG).ok_or_else(|| {
                    RepError::NetworkError("missing log stream".into())
                })?;
            let (ack_s, ack_r) =
                stream_map.remove(&STREAM_ACK).ok_or_else(|| {
                    RepError::NetworkError("missing ack stream".into())
                })?;
            let (rst_s, rst_r) =
                stream_map.remove(&STREAM_RESTORE).ok_or_else(|| {
                    RepError::NetworkError("missing restore stream".into())
                })?;

            // One clone of the runtime Arc, distributed to each sub-channel and
            // to the channel itself (five Arc::clone total, down from six before).
            let rt = Arc::clone(&self.runtime);
            Ok(QuicMultiplexedChannel {
                endpoint: None, // server side: listener owns the endpoint
                connection: conn,
                heartbeat: QuicSubChannel::new(hb_s, hb_r, Arc::clone(&rt)),
                log: QuicSubChannel::new(log_s, log_r, Arc::clone(&rt)),
                ack: QuicSubChannel::new(ack_s, ack_r, Arc::clone(&rt)),
                restore: QuicSubChannel::new(rst_s, rst_r, Arc::clone(&rt)),
                runtime: rt,
                open: AtomicBool::new(true),
            })
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

    fn loopback_mux_listener() -> QuicMultiplexedChannelListener {
        QuicMultiplexedChannelListener::bind("127.0.0.1:0".parse().unwrap())
            .expect("bind mux QUIC listener")
    }

    // -----------------------------------------------------------------------
    // Basic connectivity
    // -----------------------------------------------------------------------

    #[test]
    fn test_mux_connect_and_close() {
        let listener = loopback_mux_listener();
        let addr = listener.local_addr().unwrap();

        let server_thread = std::thread::spawn(move || {
            let ch = listener.accept().unwrap();
            assert!(ch.is_open());
            // Server drops naturally, which calls close_all().
        });

        let client =
            QuicMultiplexedChannel::connect(addr, "localhost").unwrap();
        assert!(client.is_open());
        client.close_all().unwrap();
        assert!(!client.is_open());

        server_thread.join().unwrap();
    }

    // -----------------------------------------------------------------------
    // Per-stream send / receive
    // -----------------------------------------------------------------------

    #[test]
    fn test_mux_heartbeat_send_receive() {
        let listener = loopback_mux_listener();
        let addr = listener.local_addr().unwrap();

        let server_thread = std::thread::spawn(move || {
            let ch = listener.accept().unwrap();
            let msg =
                ch.heartbeat_channel().receive(Duration::from_secs(5)).unwrap();
            assert_eq!(msg, Some(b"hb-ping".to_vec()));
            ch.heartbeat_channel().send(b"hb-pong").unwrap();
        });

        let client =
            QuicMultiplexedChannel::connect(addr, "localhost").unwrap();
        client.heartbeat_channel().send(b"hb-ping").unwrap();
        let reply =
            client.heartbeat_channel().receive(Duration::from_secs(5)).unwrap();
        assert_eq!(reply, Some(b"hb-pong".to_vec()));

        server_thread.join().unwrap();
    }

    #[test]
    fn test_mux_log_send_receive() {
        let listener = loopback_mux_listener();
        let addr = listener.local_addr().unwrap();

        let server_thread = std::thread::spawn(move || {
            let ch = listener.accept().unwrap();
            let msg = ch.log_channel().receive(Duration::from_secs(5)).unwrap();
            assert_eq!(msg, Some(b"log-entry".to_vec()));
        });

        let client =
            QuicMultiplexedChannel::connect(addr, "localhost").unwrap();
        client.log_channel().send(b"log-entry").unwrap();

        server_thread.join().unwrap();
    }

    #[test]
    fn test_mux_ack_send_receive() {
        let listener = loopback_mux_listener();
        let addr = listener.local_addr().unwrap();

        let server_thread = std::thread::spawn(move || {
            let ch = listener.accept().unwrap();
            let msg = ch.ack_channel().receive(Duration::from_secs(5)).unwrap();
            assert_eq!(msg, Some(b"ack-42".to_vec()));
        });

        let client =
            QuicMultiplexedChannel::connect(addr, "localhost").unwrap();
        client.ack_channel().send(b"ack-42").unwrap();

        server_thread.join().unwrap();
    }

    #[test]
    fn test_mux_restore_send_receive() {
        let listener = loopback_mux_listener();
        let addr = listener.local_addr().unwrap();

        let server_thread = std::thread::spawn(move || {
            let ch = listener.accept().unwrap();
            let msg =
                ch.restore_channel().receive(Duration::from_secs(5)).unwrap();
            assert_eq!(msg, Some(b"restore-block".to_vec()));
        });

        let client =
            QuicMultiplexedChannel::connect(addr, "localhost").unwrap();
        client.restore_channel().send(b"restore-block").unwrap();

        server_thread.join().unwrap();
    }

    // -----------------------------------------------------------------------
    // Stream independence
    // -----------------------------------------------------------------------

    #[test]
    fn test_mux_all_streams_independent() {
        // Each of the four streams carries a distinct message.
        // None blocks any other — the QUIC stack routes them independently.
        let listener = loopback_mux_listener();
        let addr = listener.local_addr().unwrap();

        let server_thread = std::thread::spawn(move || {
            let ch = listener.accept().unwrap();
            let hb =
                ch.heartbeat_channel().receive(Duration::from_secs(5)).unwrap();
            let log = ch.log_channel().receive(Duration::from_secs(5)).unwrap();
            let ack = ch.ack_channel().receive(Duration::from_secs(5)).unwrap();
            let rst =
                ch.restore_channel().receive(Duration::from_secs(5)).unwrap();
            (hb, log, ack, rst)
        });

        let client =
            QuicMultiplexedChannel::connect(addr, "localhost").unwrap();
        client.heartbeat_channel().send(b"heartbeat").unwrap();
        client.log_channel().send(b"log").unwrap();
        client.ack_channel().send(b"ack").unwrap();
        client.restore_channel().send(b"restore").unwrap();

        let (hb, log, ack, rst) = server_thread.join().unwrap();
        assert_eq!(hb, Some(b"heartbeat".to_vec()));
        assert_eq!(log, Some(b"log".to_vec()));
        assert_eq!(ack, Some(b"ack".to_vec()));
        assert_eq!(rst, Some(b"restore".to_vec()));
    }

    #[test]
    fn test_mux_streams_dont_interfere() {
        // Send a large burst on the log stream; heartbeats on stream 0 must
        // still flow independently (QUIC per-stream flow control).
        let listener = loopback_mux_listener();
        let addr = listener.local_addr().unwrap();
        let large_payload: Vec<u8> = vec![0xABu8; 32 * 1024]; // 32 KiB

        let server_thread = std::thread::spawn(move || {
            let ch = listener.accept().unwrap();
            // Drain all log messages.
            for _ in 0..10 {
                ch.log_channel().receive(Duration::from_secs(5)).unwrap();
            }
            // Confirm heartbeat arrives promptly.
            let hb =
                ch.heartbeat_channel().receive(Duration::from_secs(5)).unwrap();
            assert_eq!(hb, Some(b"hb".to_vec()));
        });

        let client =
            QuicMultiplexedChannel::connect(addr, "localhost").unwrap();
        // Send 10 large log messages.
        for _ in 0..10 {
            client.log_channel().send(&large_payload).unwrap();
        }
        // Heartbeat sent after the log burst — must arrive independently.
        client.heartbeat_channel().send(b"hb").unwrap();

        server_thread.join().unwrap();
    }

    // -----------------------------------------------------------------------
    // CBVLSN datagrams
    // -----------------------------------------------------------------------

    #[test]
    fn test_mux_vlsn_datagram_roundtrip() {
        let listener = loopback_mux_listener();
        let addr = listener.local_addr().unwrap();

        let server_thread = std::thread::spawn(move || {
            let ch = listener.accept().unwrap();
            let vlsn = ch.recv_vlsn_datagram(Duration::from_secs(5)).unwrap();
            assert_eq!(vlsn, Some(42_i64));
        });

        let client =
            QuicMultiplexedChannel::connect(addr, "localhost").unwrap();
        client.send_vlsn_datagram(42).unwrap();

        server_thread.join().unwrap();
    }

    #[test]
    fn test_mux_vlsn_datagram_timeout() {
        let listener = loopback_mux_listener();
        let addr = listener.local_addr().unwrap();

        // Server accepts but never sends a datagram.
        let server_thread = std::thread::spawn(move || {
            listener.accept().unwrap()
            // channel held alive for the duration of the test
        });

        let client =
            QuicMultiplexedChannel::connect(addr, "localhost").unwrap();
        let result =
            client.recv_vlsn_datagram(Duration::from_millis(300)).unwrap();
        assert_eq!(result, None, "expected timeout → None");

        drop(server_thread.join().unwrap());
    }

    // -----------------------------------------------------------------------
    // Trait object usage
    // -----------------------------------------------------------------------

    #[test]
    fn test_mux_replication_channel_trait_object() {
        let listener = loopback_mux_listener();
        let addr = listener.local_addr().unwrap();

        let server_thread = std::thread::spawn(move || {
            let ch: Box<dyn ReplicationChannel> =
                Box::new(listener.accept().unwrap());
            let msg =
                ch.heartbeat_channel().receive(Duration::from_secs(5)).unwrap();
            assert_eq!(msg, Some(b"trait test".to_vec()));
        });

        let client: Box<dyn ReplicationChannel> = Box::new(
            QuicMultiplexedChannel::connect(addr, "localhost").unwrap(),
        );
        client.heartbeat_channel().send(b"trait test").unwrap();

        server_thread.join().unwrap();
    }

    // -----------------------------------------------------------------------
    // 0-RTT reconnect path (verifies endpoint is extractable and reusable)
    // -----------------------------------------------------------------------

    #[test]
    fn test_mux_reconnect_with_endpoint() {
        let listener = loopback_mux_listener();
        let addr = listener.local_addr().unwrap();

        // Server: accept two connections in sequence.
        let server_thread = std::thread::spawn(move || {
            let ch = listener.accept().unwrap();
            ch.heartbeat_channel().receive(Duration::from_secs(5)).unwrap();
            // Second connection on the same listener.
            let ch2 = listener.accept().unwrap();
            ch2.heartbeat_channel().receive(Duration::from_secs(5)).unwrap();
        });

        let first = QuicMultiplexedChannel::connect(addr, "localhost").unwrap();
        first.heartbeat_channel().send(b"first-conn").unwrap();

        // Consume `first` into a ReconnectToken — keeps the Runtime alive so
        // the endpoint's UDP background task is not cancelled on drop.
        let token = first.into_reconnect_token().unwrap();

        // Second connection reusing the endpoint + runtime — Quinn will use the
        // cached TLS session ticket for 0-RTT when the server supports it.
        let second = QuicMultiplexedChannel::connect_with_token(
            token,
            addr,
            "localhost",
        )
        .unwrap();
        second.heartbeat_channel().send(b"second-conn").unwrap();

        server_thread.join().unwrap();
    }

    // -----------------------------------------------------------------------
    // LOG-2 hardening
    // -----------------------------------------------------------------------

    /// Each multiplexed sub-channel must enforce the same payload bound as
    /// the single-stream `QuicChannel`.  We exercise the heartbeat
    /// sub-channel; the same `receive` implementation is shared by all
    /// four streams.
    #[test]
    fn test_quic_mux_rejects_oversize_frame() {
        let listener = loopback_mux_listener();
        let addr = listener.local_addr().unwrap();

        let server_thread = std::thread::spawn(move || {
            let ch = listener.accept().unwrap();
            let oversized =
                vec![0u8; crate::rep::net::channel::MAX_FRAME_PAYLOAD + 1];
            // Send through the heartbeat stream.  The send may fail when
            // the client tears down the connection after rejecting the
            // header — that's expected.
            let _ = ch.heartbeat_channel().send(&oversized);
        });

        let client =
            QuicMultiplexedChannel::connect(addr, "localhost").unwrap();
        let result =
            client.heartbeat_channel().receive(Duration::from_secs(10));
        let _ = client.heartbeat_channel().close();
        let err = result.expect_err("oversize QUIC mux frame must be rejected");
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
