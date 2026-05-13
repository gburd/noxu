//! Abstract network channel trait, in-memory implementation, and TCP
//! implementation.
//!
//! DataChannel extends
//! Java NIO's ByteChannel/GatheringByteChannel/ScatteringByteChannel
//! interfaces backed by a SocketChannel. This Rust port provides an abstract
//! `Channel` trait for bidirectional communication, a `LocalChannelPair`
//! for in-memory testing without real network sockets, and a `TcpChannel`
//! backed by a real `TcpStream`.
//!
//! ## Wire framing
//!
//! uses NIO ByteBuffers with explicit message boundaries managed at the
//! protocol layer. Our `TcpChannel` uses a simple length-prefix framing:
//! `[payload_len: u32 LE][payload bytes]`. This is consistent with the
//! `ProtocolMessage` encoding used everywhere else in noxu-rep.

use std::collections::VecDeque;
use std::io::{Read as IoRead, Write as IoWrite};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use noxu_sync::{Condvar, Mutex};

use crate::error::{RepError, Result};

/// Trait for bidirectional communication channels.
///
/// Corresponds to `DataChannel` interface which wraps a SocketChannel
/// providing ByteChannel read/write semantics. In our Rust port we use a
/// message-oriented API (send/receive of byte vectors) rather than stream
/// oriented I/O, which simplifies protocol message framing.
pub trait Channel: Send + Sync {
    /// Send a message (bytes) through the channel.
    fn send(&self, data: &[u8]) -> Result<()>;

    /// Receive a message, blocking until data is available or the timeout
    /// expires. Returns `Ok(None)` on timeout with no data.
    fn receive(&self, timeout: Duration) -> Result<Option<Vec<u8>>>;

    /// Close the channel. After closing, further sends will fail and
    /// receives will return `None`.
    fn close(&self) -> Result<()>;

    /// Check if the channel is still open.
    fn is_open(&self) -> bool;
}

/// Shared state for one direction of a `LocalChannelPair`.
///
/// Also tracks whether the writing end has been closed, so the reading end
/// can return `Err(ChannelClosed)` instead of blocking forever.
struct ChannelQueue {
    queue: Mutex<VecDeque<Vec<u8>>>,
    condvar: Condvar,
    /// Set when the *writer* of this queue (the sending `LocalChannel`) has
    /// been closed. The reader will observe this as `ChannelClosed`.
    writer_closed: AtomicBool,
}

impl ChannelQueue {
    fn new() -> Self {
        Self {
            queue: Mutex::new(VecDeque::new()),
            condvar: Condvar::new(),
            writer_closed: AtomicBool::new(false),
        }
    }

    fn push(&self, data: Vec<u8>) {
        let mut q = self.queue.lock();
        q.push_back(data);
        self.condvar.notify_one();
    }

    /// Mark this queue's writer end as closed and wake any blocked readers.
    fn close_writer(&self) {
        self.writer_closed.store(true, Ordering::SeqCst);
        self.condvar.notify_all();
    }

    /// Pop a message, blocking until data arrives, the timeout expires, or
    /// the writer closes the queue. Returns `None` on timeout; returns
    /// `Err(ChannelClosed)` if the writer was closed.
    fn pop(&self, timeout: Duration) -> std::result::Result<Option<Vec<u8>>, ()> {
        let mut q = self.queue.lock();
        if q.is_empty() {
            if self.writer_closed.load(Ordering::SeqCst) {
                return Err(());
            }
            self.condvar.wait_for(&mut q, timeout);
        }
        if let Some(data) = q.pop_front() {
            Ok(Some(data))
        } else if self.writer_closed.load(Ordering::SeqCst) {
            Err(())
        } else {
            Ok(None)
        }
    }
}

/// In-memory channel for testing. One end of a `LocalChannelPair`.
///
/// Messages sent on this channel appear in the receive queue of the paired
/// channel, and vice versa. This provides a simple loopback mechanism for
/// unit testing protocol and data channel code without real sockets.
pub struct LocalChannel {
    /// Queue we write to (the peer reads from this).
    send_queue: Arc<ChannelQueue>,
    /// Queue we read from (the peer writes to this).
    recv_queue: Arc<ChannelQueue>,
    /// Whether this end of the channel is open.
    open: AtomicBool,
}

impl LocalChannel {
    fn new(
        send_queue: Arc<ChannelQueue>,
        recv_queue: Arc<ChannelQueue>,
    ) -> Self {
        Self { send_queue, recv_queue, open: AtomicBool::new(true) }
    }
}

impl Channel for LocalChannel {
    fn send(&self, data: &[u8]) -> Result<()> {
        if !self.is_open() {
            return Err(RepError::ChannelClosed("channel is closed".into()));
        }
        self.send_queue.push(data.to_vec());
        Ok(())
    }

    fn receive(&self, timeout: Duration) -> Result<Option<Vec<u8>>> {
        if !self.is_open() {
            return Err(RepError::ChannelClosed("channel is closed".into()));
        }
        self.recv_queue
            .pop(timeout)
            .map_err(|()| RepError::ChannelClosed("peer closed the channel".into()))
    }

    fn close(&self) -> Result<()> {
        self.open.store(false, Ordering::SeqCst);
        // Mark this end's send queue (the peer's recv queue) as writer-closed
        // so the peer's receive() returns ChannelClosed instead of blocking.
        self.send_queue.close_writer();
        // Wake any blocked receiver on our own end.
        self.recv_queue.condvar.notify_all();
        Ok(())
    }

    fn is_open(&self) -> bool {
        self.open.load(Ordering::SeqCst)
    }
}

/// A pair of cross-connected in-memory channels for testing.
///
/// `channel_a` sends to `channel_b`'s receive queue and vice versa,
/// creating a bidirectional communication pipe without real network I/O.
pub struct LocalChannelPair {
    pub channel_a: LocalChannel,
    pub channel_b: LocalChannel,
}

impl LocalChannelPair {
    /// Create a new pair of cross-connected local channels.
    pub fn new() -> Self {
        let queue_a_to_b = Arc::new(ChannelQueue::new());
        let queue_b_to_a = Arc::new(ChannelQueue::new());

        let channel_a = LocalChannel::new(
            Arc::clone(&queue_a_to_b),
            Arc::clone(&queue_b_to_a),
        );
        let channel_b = LocalChannel::new(
            Arc::clone(&queue_b_to_a),
            Arc::clone(&queue_a_to_b),
        );

        Self { channel_a, channel_b }
    }
}

impl Default for LocalChannelPair {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// TcpChannel
// ---------------------------------------------------------------------------

/// A channel backed by a real TCP connection.
///
/// Wire framing: every message is prefixed with a 4-byte little-endian length
/// so the receiver knows exactly how many bytes to read. This mirrors the
/// explicit message-length negotiation in the equivalent `DataChannel` / protocol layer.
///
/// Corresponds to `SocketChannel`-backed `DataChannel`.
pub struct TcpChannel {
    /// The underlying TCP stream, shared between sender and receiver sides.
    /// `noxu_sync::Mutex` is used rather than `std::sync::Mutex` for
    /// consistency with the rest of the codebase.
    stream: Arc<Mutex<TcpStream>>,
    /// Whether the channel is still open (not yet explicitly closed).
    open: AtomicBool,
}

impl TcpChannel {
    /// Wrap an existing `TcpStream` in a `TcpChannel`.
    ///
    /// The stream must be in a connected state. The caller is responsible for
    /// configuring any socket options (e.g. `TCP_NODELAY`) before wrapping.
    pub fn new(stream: TcpStream) -> Self {
        Self { stream: Arc::new(Mutex::new(stream)), open: AtomicBool::new(true) }
    }

    /// Connect to a remote address and return a `TcpChannel`.
    ///
    /// Uses a 30-second timeout so that a dropped SYN under kernel netem
    /// packet-loss chaos does not block indefinitely (Linux default: ~127 s).
    pub fn connect(addr: SocketAddr) -> Result<Self> {
        let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(30))
            .map_err(|e| RepError::NetworkError(e.to_string()))?;
        Ok(Self::new(stream))
    }

    /// Connect by hostname (or IP string) and port, with DNS resolution and
    /// Happy Eyeballs address ordering (IPv6 candidates tried before IPv4).
    ///
    /// All addresses returned by DNS are tried in order; the first successful
    /// connection wins. Each attempt uses a 30-second TCP connect timeout.
    ///
    /// This enables peer addresses in `RepNode` to be specified as hostnames,
    /// IPv6 literals (`[::1]:6200`), or plain IPv4 (`127.0.0.1:6200`).
    pub fn connect_host(host: &str, port: u16) -> Result<Self> {
        let addrs: Vec<SocketAddr> = (host, port)
            .to_socket_addrs()
            .map_err(|e| RepError::NetworkError(format!("DNS resolution failed for {host}:{port}: {e}")))?
            .collect();

        if addrs.is_empty() {
            return Err(RepError::NetworkError(format!("no addresses resolved for {host}:{port}")));
        }

        // Happy Eyeballs: prefer IPv6 over IPv4 when both are available.
        let mut sorted = addrs;
        sorted.sort_by_key(|a| if a.is_ipv6() { 0u8 } else { 1u8 });

        let mut last_err = None;
        for addr in &sorted {
            match TcpStream::connect_timeout(addr, Duration::from_secs(30)) {
                Ok(stream) => return Ok(Self::new(stream)),
                Err(e) => last_err = Some(e),
            }
        }

        Err(RepError::NetworkError(format!(
            "could not connect to {host}:{port}: {}",
            last_err.unwrap()
        )))
    }

    /// Bind a dual-stack (IPv4 + IPv6) TCP listener on the given port.
    ///
    /// First attempts `[::]:port` (dual-stack on systems that support it, e.g.
    /// Linux with `IPV6_V6ONLY=0`). Falls back to `0.0.0.0:port` if IPv6 is
    /// unavailable (e.g., BSD with `IPV6_V6ONLY=1` requires a separate socket,
    /// or the kernel has IPv6 disabled).
    pub fn bind_dual_stack(port: u16) -> Result<TcpChannelListener> {
        // Try IPv6 wildcard first (accepts both IPv4-mapped and native IPv6 on Linux).
        if let Ok(listener) = TcpListener::bind(format!("[::]:{}",  port)) {
            return Ok(TcpChannelListener { listener });
        }
        // Fall back to IPv4 wildcard.
        let addr: SocketAddr = format!("0.0.0.0:{port}")
            .parse()
            .map_err(|e| RepError::NetworkError(format!("invalid bind addr: {e}")))?;
        TcpChannelListener::bind(addr)
    }
}

impl Channel for TcpChannel {
    /// Send a message.
    ///
    /// Writes a 4-byte LE length prefix followed by the payload bytes,
    /// atomically under the stream lock.
    fn send(&self, data: &[u8]) -> Result<()> {
        if !self.is_open() {
            return Err(RepError::ChannelClosed("TcpChannel is closed".into()));
        }
        let len = data.len() as u32;
        let mut stream = self.stream.lock();
        // Cap write time at 30 s to prevent indefinite stall under packet loss.
        stream.set_write_timeout(Some(Duration::from_secs(30))).ok();
        stream
            .write_all(&len.to_le_bytes())
            .map_err(|e| RepError::NetworkError(e.to_string()))?;
        stream
            .write_all(data)
            .map_err(|e| RepError::NetworkError(e.to_string()))?;
        stream
            .flush()
            .map_err(|e| RepError::NetworkError(e.to_string()))?;
        Ok(())
    }

    /// Receive a message, blocking until data arrives or the timeout expires.
    ///
    /// Sets `SO_RCVTIMEO` on the stream to implement the timeout. Returns
    /// `Ok(None)` if the timeout elapses with no data. Returns
    /// `Err(ChannelClosed)` if the peer closed the connection cleanly (EOF).
    fn receive(&self, timeout: Duration) -> Result<Option<Vec<u8>>> {
        if !self.is_open() {
            return Err(RepError::ChannelClosed("TcpChannel is closed".into()));
        }

        let mut stream = self.stream.lock();

        // Apply read timeout so we do not block indefinitely.
        stream
            .set_read_timeout(Some(timeout))
            .map_err(|e| RepError::NetworkError(e.to_string()))?;

        // Read the 4-byte length prefix.
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) => {
                // WouldBlock / TimedOut both mean the timeout expired.
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut
                {
                    return Ok(None);
                }
                // Unexpected EOF means the peer closed the connection.
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    return Err(RepError::ChannelClosed(
                        "connection closed by peer".into(),
                    ));
                }
                return Err(RepError::NetworkError(e.to_string()));
            }
        }

        let payload_len = u32::from_le_bytes(len_buf) as usize;

        // Use a generous timeout for the payload read: the caller's `timeout`
        // may be as short as 1 ms (FeederRunner ACK polling), which is far too
        // small once a message header has been received.  Cap at 30 s so we
        // never hang indefinitely under kernel netem packet loss while still
        // being patient enough for real retransmit scenarios.
        let payload_timeout = timeout.max(Duration::from_secs(30));
        stream.set_read_timeout(Some(payload_timeout)).ok();

        let mut payload = vec![0u8; payload_len];
        stream
            .read_exact(&mut payload)
            .map_err(|e| RepError::NetworkError(e.to_string()))?;

        Ok(Some(payload))
    }

    /// Shut down the TCP stream and mark the channel closed.
    fn close(&self) -> Result<()> {
        self.open.store(false, Ordering::SeqCst);
        let stream = self.stream.lock();
        stream
            .shutdown(std::net::Shutdown::Both)
            .map_err(|e| RepError::NetworkError(e.to_string()))
    }

    fn is_open(&self) -> bool {
        self.open.load(Ordering::SeqCst)
    }
}

// ---------------------------------------------------------------------------
// TcpChannelListener
// ---------------------------------------------------------------------------

/// Listens for incoming TCP connections and wraps each accepted socket in a
/// `TcpChannel`.
///
/// Corresponds to the server-socket accept loop inside the
/// `ServiceDispatcher`.
pub struct TcpChannelListener {
    listener: TcpListener,
}

impl TcpChannelListener {
    /// Bind to the given address and start listening.
    pub fn bind(addr: SocketAddr) -> Result<Self> {
        let listener = TcpListener::bind(addr)
            .map_err(|e| RepError::NetworkError(e.to_string()))?;
        Ok(Self { listener })
    }

    /// Return the local address the listener is bound to.
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.listener
            .local_addr()
            .map_err(|e| RepError::NetworkError(e.to_string()))
    }

    /// Accept the next incoming connection, blocking until one arrives.
    ///
    /// Returns a `TcpChannel` wrapping the accepted socket.
    pub fn accept(&self) -> Result<TcpChannel> {
        let (stream, _peer) = self
            .listener
            .accept()
            .map_err(|e| RepError::NetworkError(e.to_string()))?;
        Ok(TcpChannel::new(stream))
    }

    /// Set the accept timeout via SO_RCVTIMEO.
    ///
    /// After the timeout, `accept()` returns `Err(WouldBlock)`.
    /// Pass `None` to remove a previously set timeout (block forever).
    pub fn set_accept_timeout(&self, timeout: Option<Duration>) -> Result<()> {
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd;
            let fd = self.listener.as_raw_fd();
            let tv = match timeout {
                Some(d) => libc::timeval {
                    tv_sec:  d.as_secs() as libc::time_t,
                    tv_usec: d.subsec_micros() as libc::suseconds_t,
                },
                None => libc::timeval { tv_sec: 0, tv_usec: 0 },
            };
            let rc = unsafe {
                libc::setsockopt(
                    fd,
                    libc::SOL_SOCKET,
                    libc::SO_RCVTIMEO,
                    &tv as *const _ as *const libc::c_void,
                    std::mem::size_of::<libc::timeval>() as libc::socklen_t,
                )
            };
            if rc != 0 {
                return Err(RepError::NetworkError(
                    std::io::Error::last_os_error().to_string(),
                ));
            }
        }
        #[cfg(not(unix))]
        { let _ = timeout; }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_send_receive_basic() {
        let pair = LocalChannelPair::new();
        let msg = b"hello world";
        pair.channel_a.send(msg).unwrap();
        let received = pair.channel_b.receive(Duration::from_secs(1)).unwrap();
        assert_eq!(received, Some(msg.to_vec()));
    }

    #[test]
    fn test_bidirectional() {
        let pair = LocalChannelPair::new();

        pair.channel_a.send(b"from a").unwrap();
        pair.channel_b.send(b"from b").unwrap();

        let recv_b = pair.channel_b.receive(Duration::from_secs(1)).unwrap();
        assert_eq!(recv_b, Some(b"from a".to_vec()));

        let recv_a = pair.channel_a.receive(Duration::from_secs(1)).unwrap();
        assert_eq!(recv_a, Some(b"from b".to_vec()));
    }

    #[test]
    fn test_multiple_messages_fifo() {
        let pair = LocalChannelPair::new();
        pair.channel_a.send(b"first").unwrap();
        pair.channel_a.send(b"second").unwrap();
        pair.channel_a.send(b"third").unwrap();

        assert_eq!(
            pair.channel_b.receive(Duration::from_secs(1)).unwrap(),
            Some(b"first".to_vec())
        );
        assert_eq!(
            pair.channel_b.receive(Duration::from_secs(1)).unwrap(),
            Some(b"second".to_vec())
        );
        assert_eq!(
            pair.channel_b.receive(Duration::from_secs(1)).unwrap(),
            Some(b"third".to_vec())
        );
    }

    #[test]
    fn test_receive_timeout_empty_queue() {
        let pair = LocalChannelPair::new();
        let result = pair.channel_b.receive(Duration::from_millis(50)).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_send_after_close_fails() {
        let pair = LocalChannelPair::new();
        pair.channel_a.close().unwrap();
        let result = pair.channel_a.send(b"should fail");
        assert!(result.is_err());
    }

    #[test]
    fn test_receive_after_close_fails() {
        let pair = LocalChannelPair::new();
        pair.channel_b.close().unwrap();
        let result = pair.channel_b.receive(Duration::from_millis(10));
        assert!(result.is_err());
    }

    #[test]
    fn test_is_open() {
        let pair = LocalChannelPair::new();
        assert!(pair.channel_a.is_open());
        assert!(pair.channel_b.is_open());

        pair.channel_a.close().unwrap();
        assert!(!pair.channel_a.is_open());
        // Closing one end does not close the other.
        assert!(pair.channel_b.is_open());
    }

    #[test]
    fn test_empty_message() {
        let pair = LocalChannelPair::new();
        pair.channel_a.send(b"").unwrap();
        let received = pair.channel_b.receive(Duration::from_secs(1)).unwrap();
        assert_eq!(received, Some(vec![]));
    }

    #[test]
    fn test_large_message() {
        let pair = LocalChannelPair::new();
        let large = vec![0xABu8; 1024 * 1024]; // 1 MiB
        pair.channel_a.send(&large).unwrap();
        let received = pair.channel_b.receive(Duration::from_secs(1)).unwrap();
        assert_eq!(received, Some(large));
    }

    #[test]
    fn test_concurrent_send_receive() {
        let pair = LocalChannelPair::new();
        // Move channel_b into a thread that will receive.
        let queue_send = Arc::clone(&pair.channel_a.send_queue);
        let _queue_recv = Arc::clone(&pair.channel_b.recv_queue);

        let _channel_b_send = Arc::new(ChannelQueue::new());
        let _channel_b_recv = Arc::clone(&queue_send); // b reads from a's send queue

        // Simpler approach: use the pair directly with scoped threads.
        std::thread::scope(|s| {
            let a = &pair.channel_a;
            let b = &pair.channel_b;

            let handle = s.spawn(|| {
                let msg = b.receive(Duration::from_secs(5)).unwrap();
                assert_eq!(msg, Some(b"concurrent".to_vec()));
                b.send(b"ack").unwrap();
            });

            a.send(b"concurrent").unwrap();
            let ack = a.receive(Duration::from_secs(5)).unwrap();
            assert_eq!(ack, Some(b"ack".to_vec()));
            handle.join().unwrap();
        });
    }

    #[test]
    fn test_default_trait() {
        let pair = LocalChannelPair::default();
        assert!(pair.channel_a.is_open());
        assert!(pair.channel_b.is_open());
    }

    // -----------------------------------------------------------------------
    // TcpChannel tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_tcp_channel_send_receive() {
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let ch = TcpChannel::new(stream);
            let msg = ch.receive(Duration::from_secs(5)).unwrap();
            assert_eq!(msg, Some(b"hello tcp".to_vec()));
            ch.send(b"world").unwrap();
        });

        let client = TcpChannel::connect(addr).unwrap();
        client.send(b"hello tcp").unwrap();
        let reply = client.receive(Duration::from_secs(5)).unwrap();
        assert_eq!(reply, Some(b"world".to_vec()));

        handle.join().unwrap();
    }

    #[test]
    fn test_tcp_channel_multiple_messages() {
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let ch = TcpChannel::new(stream);
            for i in 0u8..5 {
                let msg = ch.receive(Duration::from_secs(5)).unwrap().unwrap();
                assert_eq!(msg, vec![i]);
            }
        });

        let client = TcpChannel::connect(addr).unwrap();
        for i in 0u8..5 {
            client.send(&[i]).unwrap();
        }
        handle.join().unwrap();
    }

    #[test]
    fn test_tcp_channel_receive_timeout() {
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        // Accept in background (never sends).
        let handle = std::thread::spawn(move || {
            let (_stream, _) = listener.accept().unwrap();
            std::thread::sleep(Duration::from_secs(2));
        });

        let client = TcpChannel::connect(addr).unwrap();
        let result = client.receive(Duration::from_millis(100)).unwrap();
        assert_eq!(result, None, "expected timeout → None");

        handle.join().unwrap();
    }

    #[test]
    fn test_tcp_channel_is_open_and_close() {
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = std::thread::spawn(move || {
            let (_stream, _) = listener.accept().unwrap();
            std::thread::sleep(Duration::from_millis(200));
        });

        let client = TcpChannel::connect(addr).unwrap();
        assert!(client.is_open());
        client.close().unwrap();
        assert!(!client.is_open());

        handle.join().unwrap();
    }

    #[test]
    fn test_tcp_channel_large_payload() {
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let payload: Vec<u8> = (0..65536).map(|i| (i % 256) as u8).collect();
        let expected = payload.clone();

        let handle = std::thread::spawn(move || {
            let (stream, _) = listener.accept().unwrap();
            let ch = TcpChannel::new(stream);
            let msg = ch.receive(Duration::from_secs(5)).unwrap().unwrap();
            assert_eq!(msg, expected);
        });

        let client = TcpChannel::connect(addr).unwrap();
        client.send(&payload).unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_tcp_channel_listener_bind_and_accept() {
        let listener =
            TcpChannelListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();

        let handle = std::thread::spawn(move || {
            let ch = listener.accept().unwrap();
            let msg = ch.receive(Duration::from_secs(5)).unwrap();
            assert_eq!(msg, Some(b"ping".to_vec()));
        });

        let client = TcpChannel::connect(addr).unwrap();
        client.send(b"ping").unwrap();
        handle.join().unwrap();
    }
}
