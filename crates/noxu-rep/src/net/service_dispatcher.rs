//! Service dispatcher for routing incoming connections.
//!
//! The
//! ServiceDispatcher listens on a server socket, accepts incoming TCP
//! connections, reads a service name from each new connection, and routes it
//! to the registered handler. This Rust port provides:
//!
//! - [`ServiceDispatcher`] — an in-memory registry used in tests via
//!   `LocalChannel`.
//! - [`TcpServiceDispatcher`] — a real TCP implementation with a spawned
//!   accept loop. Clients connect and immediately send a length-prefixed
//!   service-name string; the dispatcher routes the connection to the matching
//!   [`ServiceHandler`].
//!
//! ## Wire protocol for service negotiation
//!
//! ```text
//! [name_len: u32 LE][service_name: utf8 bytes]
//! ```
//! After sending the service name the client owns the connection and may
//! begin the actual service protocol. This `ServiceDispatcher`
//! which reads a service name from each new socket before routing.
//!
//! The `name_len` field is bounded by [`MAX_SERVICE_NAME_LEN`] to prevent
//! a malicious or accidental peer from triggering an unbounded allocation
//! by sending an arbitrary 4-byte length prefix. The dispatcher rejects
//! frames whose length exceeds the bound before any allocation occurs.

use hashbrown::HashMap;
use std::io::{Read as IoRead, Write as IoWrite};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use noxu_sync::Mutex;

use super::channel::{Channel, TcpChannel};
use crate::error::{RepError, Result};

/// Maximum permitted length, in bytes, of the service-name field on the
/// dispatcher wire protocol.
///
/// The longest defined service name today is `"PEER_FEEDER"` (11 bytes);
/// 256 bytes is comfortable headroom for future names while still being
/// small enough that a hostile peer cannot use the field to OOM the master.
///
/// Frames whose length prefix exceeds this bound are rejected before any
/// allocation occurs (see `handle_incoming`).
pub const MAX_SERVICE_NAME_LEN: usize = 256;

/// Callback for handling incoming connections on a named service.
///
/// Corresponds to `ServiceDispatcher.ServiceConnector` interface.
/// Implementations receive an open channel and process the connection.
pub trait ServiceHandler: Send + Sync {
    /// Handle an incoming connection on this service.
    fn handle(&self, channel: Box<dyn Channel>) -> Result<()>;

    /// The name of this service, used for routing.
    fn service_name(&self) -> &str;
}

/// Dispatches incoming connections to registered service handlers.
///
/// Provides the handler registry and
/// dispatch logic. The accept loop lives in [`TcpServiceDispatcher`], which
/// mirrors ownership of the server socket.
pub struct ServiceDispatcher {
    /// Map from service name to handler.
    services: Mutex<HashMap<String, Arc<dyn ServiceHandler>>>,
    /// Whether the dispatcher is running.
    running: AtomicBool,
}

impl ServiceDispatcher {
    /// Create a new service dispatcher.
    pub fn new() -> Self {
        Self {
            services: Mutex::new(HashMap::new()),
            running: AtomicBool::new(false),
        }
    }

    /// Register a service handler. If a handler with the same name already
    /// exists, it is replaced.
    pub fn register(&self, handler: Arc<dyn ServiceHandler>) {
        let name = handler.service_name().to_string();
        let mut services = self.services.lock();
        services.insert(name, handler);
    }

    /// Unregister a service handler by name, returning the handler if it
    /// was registered.
    pub fn unregister(
        &self,
        service_name: &str,
    ) -> Option<Arc<dyn ServiceHandler>> {
        let mut services = self.services.lock();
        services.remove(service_name)
    }

    /// Get a registered handler by service name.
    pub fn get_handler(&self, name: &str) -> Option<Arc<dyn ServiceHandler>> {
        let services = self.services.lock();
        services.get(name).cloned()
    }

    /// List all registered service names.
    pub fn list_services(&self) -> Vec<String> {
        let services = self.services.lock();
        let mut names: Vec<String> = services.keys().cloned().collect();
        names.sort();
        names
    }

    /// Start the dispatcher.
    ///
    /// Marks this base dispatcher as running. [`TcpServiceDispatcher::start()`]
    /// extends this by spawning the TCP accept loop, mirroring split
    /// between `ServiceDispatcher` (registry) and `TcpChannel` (transport).
    pub fn start(&self) {
        self.running.store(true, Ordering::SeqCst);
    }

    /// Stop the dispatcher.
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Whether the dispatcher is currently running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Dispatch an incoming channel to the appropriate handler based on
    /// the given service name. Returns an error if no handler is registered
    /// for that name.
    pub fn dispatch(
        &self,
        service_name: &str,
        channel: Box<dyn Channel>,
    ) -> Result<()> {
        let handler = self.get_handler(service_name).ok_or_else(|| {
            crate::error::RepError::ServiceNotFound(service_name.to_string())
        })?;
        handler.handle(channel)
    }
}

impl Default for ServiceDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// TcpServiceDispatcher
// ---------------------------------------------------------------------------

/// A TCP-backed service dispatcher with a real accept loop.
///
/// Corresponds to `ServiceDispatcher` which binds a server socket,
/// accepts connections, reads the service name, and routes to a handler.
///
/// ## Usage
///
/// ```ignore
/// let mut sd = TcpServiceDispatcher::new("127.0.0.1:5001".parse().unwrap())?;
/// sd.register("feeder", handler);
/// sd.start(); // spawns accept thread
/// ```
pub struct TcpServiceDispatcher {
    /// Map from service name to handler.
    services: Arc<Mutex<HashMap<String, Arc<dyn ServiceHandler>>>>,
    /// Bound address.
    addr: SocketAddr,
    /// Whether the accept loop is running.
    running: Arc<AtomicBool>,
}

impl TcpServiceDispatcher {
    /// Create a new dispatcher bound to the given address.
    pub fn new(addr: SocketAddr) -> Result<Self> {
        Ok(Self {
            services: Arc::new(Mutex::new(HashMap::new())),
            addr,
            running: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Register a service handler by name.
    pub fn register(
        &self,
        name: impl Into<String>,
        handler: Arc<dyn ServiceHandler>,
    ) {
        self.services.lock().insert(name.into(), handler);
    }

    /// Return the address this dispatcher is bound to.
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Whether the dispatcher accept loop is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Stop the accept loop.
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    /// Start the TCP accept loop in a background thread.
    ///
    /// Each accepted connection must first send the service name as
    /// `[len: u32 LE][utf8 bytes]`. The connection is then routed to the
    /// matching registered handler.
    ///
    /// The returned bound address may differ from `addr` if port 0 was used.
    pub fn start(&self) -> Result<SocketAddr> {
        use std::net::TcpListener;

        let listener = TcpListener::bind(self.addr)
            .map_err(|e| RepError::NetworkError(e.to_string()))?;
        let bound_addr = listener
            .local_addr()
            .map_err(|e| RepError::NetworkError(e.to_string()))?;

        let services = Arc::clone(&self.services);
        let running = Arc::clone(&self.running);
        running.store(true, Ordering::SeqCst);

        thread::spawn(move || {
            while running.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((stream, _peer_addr)) => {
                        let services_clone = Arc::clone(&services);
                        let running_check = Arc::clone(&running);
                        thread::spawn(move || {
                            handle_incoming(
                                stream,
                                services_clone,
                                running_check,
                            );
                        });
                    }
                    Err(_) => {
                        // Accept error — stop the loop.
                        break;
                    }
                }
            }
            running.store(false, Ordering::SeqCst);
        });

        Ok(bound_addr)
    }
}

/// Read the service name from a newly accepted TCP connection and dispatch.
///
/// Service name wire format: `[len: u32 LE][utf8 bytes]`.
/// After reading the service name the raw `TcpStream` is wrapped back into a
/// `TcpChannel` for the handler.
fn handle_incoming(
    stream: std::net::TcpStream,
    services: Arc<Mutex<HashMap<String, Arc<dyn ServiceHandler>>>>,
    _running: Arc<AtomicBool>,
) {
    // We need a clone for the TcpChannel wrapper after the read.
    // Use try_clone so the read and the channel share the same underlying fd.
    let mut read_stream = match stream.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    };

    // Read service name: [len: u32 LE][utf8]
    let mut len_buf = [0u8; 4];
    if read_stream.read_exact(&mut len_buf).is_err() {
        return;
    }
    let name_len = u32::from_le_bytes(len_buf) as usize;
    // Bound check BEFORE allocation. A hostile or buggy peer that sends a
    // 4-byte length of e.g. 0xFFFF_FFFF would otherwise trigger a 4 GiB
    // allocation here. See `MAX_SERVICE_NAME_LEN` and finding F3 in the
    // 2026-05 noxu-rep API audit.
    if name_len == 0 || name_len > MAX_SERVICE_NAME_LEN {
        log::warn!(
            "TcpServiceDispatcher: rejected service-name length {} (max {})",
            name_len,
            MAX_SERVICE_NAME_LEN
        );
        return;
    }
    let mut name_buf = vec![0u8; name_len];
    if read_stream.read_exact(&mut name_buf).is_err() {
        return;
    }
    let service_name = match String::from_utf8(name_buf) {
        Ok(s) => s,
        Err(_) => return,
    };
    drop(read_stream);

    let handler = {
        let guard = services.lock();
        guard.get(&service_name).cloned()
    };

    if let Some(h) = handler {
        let tcp_ch = TcpChannel::new(stream);
        let _ = h.handle(Box::new(tcp_ch));
    }
}

/// Connect to a `TcpServiceDispatcher` and request the named service.
///
/// This is a convenience function for client code. It connects, sends the
/// service name using the dispatcher's wire protocol, and returns the
/// connected `TcpChannel` ready for the service protocol.
///
/// Returns [`RepError::ConfigError`] if `service_name` is empty or longer
/// than [`MAX_SERVICE_NAME_LEN`].
pub fn connect_to_service(
    addr: SocketAddr,
    service_name: &str,
) -> Result<TcpChannel> {
    use std::net::TcpStream;

    let name_bytes = service_name.as_bytes();
    if name_bytes.is_empty() || name_bytes.len() > MAX_SERVICE_NAME_LEN {
        return Err(RepError::ConfigError(format!(
            "service name length {} out of range [1, {}]",
            name_bytes.len(),
            MAX_SERVICE_NAME_LEN,
        )));
    }

    let mut stream = TcpStream::connect(addr)
        .map_err(|e| RepError::NetworkError(e.to_string()))?;

    let len = name_bytes.len() as u32;
    stream
        .write_all(&len.to_le_bytes())
        .map_err(|e| RepError::NetworkError(e.to_string()))?;
    stream
        .write_all(name_bytes)
        .map_err(|e| RepError::NetworkError(e.to_string()))?;
    stream.flush().map_err(|e| RepError::NetworkError(e.to_string()))?;

    Ok(TcpChannel::new(stream))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    /// A simple test handler that counts how many times it has been called.
    struct CountingHandler {
        name: String,
        call_count: AtomicU32,
    }

    impl CountingHandler {
        fn new(name: &str) -> Self {
            Self { name: name.to_string(), call_count: AtomicU32::new(0) }
        }

        fn count(&self) -> u32 {
            self.call_count.load(Ordering::SeqCst)
        }
    }

    impl ServiceHandler for CountingHandler {
        fn handle(&self, _channel: Box<dyn Channel>) -> Result<()> {
            self.call_count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn service_name(&self) -> &str {
            &self.name
        }
    }

    #[test]
    fn test_register_and_get() {
        let dispatcher = ServiceDispatcher::new();
        let handler = Arc::new(CountingHandler::new("feeder"));
        dispatcher.register(handler);

        let retrieved = dispatcher.get_handler("feeder");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().service_name(), "feeder");
    }

    #[test]
    fn test_get_nonexistent() {
        let dispatcher = ServiceDispatcher::new();
        assert!(dispatcher.get_handler("nope").is_none());
    }

    #[test]
    fn test_unregister() {
        let dispatcher = ServiceDispatcher::new();
        let handler = Arc::new(CountingHandler::new("feeder"));
        dispatcher.register(handler);

        let removed = dispatcher.unregister("feeder");
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().service_name(), "feeder");
        assert!(dispatcher.get_handler("feeder").is_none());
    }

    #[test]
    fn test_unregister_nonexistent() {
        let dispatcher = ServiceDispatcher::new();
        assert!(dispatcher.unregister("nope").is_none());
    }

    #[test]
    fn test_list_services() {
        let dispatcher = ServiceDispatcher::new();
        dispatcher.register(Arc::new(CountingHandler::new("feeder")));
        dispatcher.register(Arc::new(CountingHandler::new("election")));
        dispatcher.register(Arc::new(CountingHandler::new("backup")));

        let names = dispatcher.list_services();
        assert_eq!(names, vec!["backup", "election", "feeder"]);
    }

    #[test]
    fn test_list_services_empty() {
        let dispatcher = ServiceDispatcher::new();
        assert!(dispatcher.list_services().is_empty());
    }

    #[test]
    fn test_start_stop() {
        let dispatcher = ServiceDispatcher::new();
        assert!(!dispatcher.is_running());

        dispatcher.start();
        assert!(dispatcher.is_running());

        dispatcher.stop();
        assert!(!dispatcher.is_running());
    }

    #[test]
    fn test_register_replaces_existing() {
        let dispatcher = ServiceDispatcher::new();
        let handler1 = Arc::new(CountingHandler::new("feeder"));
        let handler2 = Arc::new(CountingHandler::new("feeder"));

        dispatcher.register(handler1);
        dispatcher.register(handler2);

        // list_services should still have exactly one "feeder".
        assert_eq!(dispatcher.list_services(), vec!["feeder"]);
    }

    #[test]
    fn test_dispatch_to_handler() {
        use super::super::channel::LocalChannelPair;

        let dispatcher = ServiceDispatcher::new();
        let handler = Arc::new(CountingHandler::new("feeder"));
        dispatcher.register(handler.clone());

        let pair = LocalChannelPair::new();
        dispatcher.dispatch("feeder", Box::new(pair.channel_a)).unwrap();
        assert_eq!(handler.count(), 1);
    }

    #[test]
    fn test_dispatch_unknown_service() {
        use super::super::channel::LocalChannelPair;

        let dispatcher = ServiceDispatcher::new();
        let pair = LocalChannelPair::new();
        let result = dispatcher.dispatch("unknown", Box::new(pair.channel_a));
        assert!(result.is_err());
    }

    #[test]
    fn test_default_trait() {
        let dispatcher = ServiceDispatcher::default();
        assert!(!dispatcher.is_running());
        assert!(dispatcher.list_services().is_empty());
    }

    // -----------------------------------------------------------------------
    // TcpServiceDispatcher tests
    // -----------------------------------------------------------------------

    use super::{TcpServiceDispatcher, connect_to_service};
    use std::time::Duration;

    struct EchoHandler {
        name: String,
    }

    impl ServiceHandler for EchoHandler {
        fn handle(&self, channel: Box<dyn Channel>) -> Result<()> {
            // Echo one message back.
            let msg = channel.receive(Duration::from_secs(5))?.unwrap();
            channel.send(&msg)?;
            Ok(())
        }

        fn service_name(&self) -> &str {
            &self.name
        }
    }

    #[test]
    fn test_tcp_service_dispatcher_register_and_dispatch() {
        let sd =
            TcpServiceDispatcher::new("127.0.0.1:0".parse().unwrap()).unwrap();
        sd.register("echo", Arc::new(EchoHandler { name: "echo".into() }));
        let bound_addr = sd.start().unwrap();

        // Give the accept thread a moment to start.
        std::thread::sleep(Duration::from_millis(20));

        let client = connect_to_service(bound_addr, "echo").unwrap();
        client.send(b"hello dispatcher").unwrap();
        let reply = client.receive(Duration::from_secs(5)).unwrap();
        assert_eq!(reply, Some(b"hello dispatcher".to_vec()));

        sd.stop();
    }

    #[test]
    fn test_tcp_service_dispatcher_multiple_clients() {
        let sd =
            TcpServiceDispatcher::new("127.0.0.1:0".parse().unwrap()).unwrap();
        sd.register("echo", Arc::new(EchoHandler { name: "echo".into() }));
        let bound_addr = sd.start().unwrap();

        std::thread::sleep(Duration::from_millis(20));

        let mut handles = Vec::new();
        for i in 0u8..3 {
            let addr = bound_addr;
            handles.push(std::thread::spawn(move || {
                let client = connect_to_service(addr, "echo").unwrap();
                let msg = vec![i; 8];
                client.send(&msg).unwrap();
                let reply =
                    client.receive(Duration::from_secs(5)).unwrap().unwrap();
                assert_eq!(reply, msg);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        sd.stop();
    }
}
