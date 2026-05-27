//! F3: Dispatcher unbounded service-name allocation guard.
//!
//! Regression tests for the bound check added in Wave 3-3 against the
//! `TcpServiceDispatcher` service-name framing. Without the bound, a
//! 4-byte length prefix from any peer (or attacker on the replication
//! port) caused up to a 4 GiB allocation per connection.
//!
//! See `docs/src/internal/api-audit-2026-05-rep.md` finding F3.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use noxu_rep::error::Result as RepResult;
use noxu_rep::net::{
    Channel, MAX_SERVICE_NAME_LEN, ServiceHandler, TcpServiceDispatcher,
    connect_to_service,
};

struct PingHandler {
    name: String,
    invoked: Arc<AtomicBool>,
}

impl ServiceHandler for PingHandler {
    fn handle(&self, _channel: Box<dyn Channel>) -> RepResult<()> {
        self.invoked.store(true, Ordering::SeqCst);
        Ok(())
    }

    fn service_name(&self) -> &str {
        &self.name
    }
}

fn start_dispatcher()
-> (TcpServiceDispatcher, std::net::SocketAddr, Arc<AtomicBool>) {
    let invoked = Arc::new(AtomicBool::new(false));
    let sd = TcpServiceDispatcher::new("127.0.0.1:0".parse().unwrap()).unwrap();
    sd.register(
        "PING",
        Arc::new(PingHandler { name: "PING".into(), invoked: invoked.clone() }),
    );
    let addr = sd.start().unwrap();
    std::thread::sleep(Duration::from_millis(20));
    (sd, addr, invoked)
}

/// Send a 4-byte length prefix of 0xFFFFFFFF (~4 GiB). The dispatcher
/// must reject the frame BEFORE allocating the buffer and must close the
/// connection without invoking any handler. The dispatcher must remain
/// alive afterwards (subsequent legitimate clients still work).
#[test]
fn f3_dispatcher_rejects_oversized_service_name_length() {
    let (sd, addr, invoked) = start_dispatcher();

    // Hostile client: send 0xFFFFFFFF as little-endian u32 length.
    let mut sock = TcpStream::connect(addr).unwrap();
    sock.write_all(&u32::MAX.to_le_bytes()).unwrap();
    sock.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let mut sink = [0u8; 16];
    let _ = sock.read(&mut sink);
    drop(sock);

    std::thread::sleep(Duration::from_millis(40));

    assert!(
        !invoked.load(Ordering::SeqCst),
        "PING handler must not have been dispatched on an oversized name",
    );

    // The dispatcher must still be alive: a legitimate client succeeds.
    let _ch = connect_to_service(addr, "PING")
        .expect("dispatcher should still accept legitimate clients");
    std::thread::sleep(Duration::from_millis(40));
    assert!(
        invoked.load(Ordering::SeqCst),
        "PING handler should fire on a well-formed connection",
    );

    sd.stop();
}

/// Anything strictly larger than `MAX_SERVICE_NAME_LEN` must be rejected
/// even if it would fit comfortably in memory.
#[test]
fn f3_dispatcher_rejects_just_above_max_service_name_len() {
    let (sd, addr, invoked) = start_dispatcher();

    let oversize = (MAX_SERVICE_NAME_LEN as u32) + 1;
    let mut sock = TcpStream::connect(addr).unwrap();
    sock.write_all(&oversize.to_le_bytes()).unwrap();
    let payload = vec![b'A'; oversize as usize];
    let _ = sock.write_all(&payload);
    drop(sock);

    std::thread::sleep(Duration::from_millis(40));
    assert!(
        !invoked.load(Ordering::SeqCst),
        "oversize service-name must not reach a handler",
    );

    sd.stop();
}

/// A zero-length service-name is also rejected.
#[test]
fn f3_dispatcher_rejects_zero_length_service_name() {
    let (sd, addr, invoked) = start_dispatcher();

    let mut sock = TcpStream::connect(addr).unwrap();
    sock.write_all(&0u32.to_le_bytes()).unwrap();
    drop(sock);

    std::thread::sleep(Duration::from_millis(40));
    assert!(
        !invoked.load(Ordering::SeqCst),
        "zero-length service-name must not reach a handler",
    );

    sd.stop();
}

/// `connect_to_service` rejects empty / oversize names client-side too.
#[test]
fn f3_connect_to_service_rejects_oversize_name_client_side() {
    let bad_name = "x".repeat(MAX_SERVICE_NAME_LEN + 1);
    let res = connect_to_service("127.0.0.1:1".parse().unwrap(), &bad_name);
    assert!(res.is_err(), "oversize service name must be rejected");

    let res = connect_to_service("127.0.0.1:1".parse().unwrap(), "");
    assert!(res.is_err(), "empty service name must be rejected");
}
