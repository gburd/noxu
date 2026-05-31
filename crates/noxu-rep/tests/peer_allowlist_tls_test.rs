//! mTLS peer-allowlist enforcement integration tests.
//!
//! These tests verify the Phase 2 (v3.1.0) enforcement: a
//! [`TlsTcpChannelListener`] built with
//! [`TlsTcpChannelListener::bind_with_tls_and_allowlist`] rejects peers
//! whose certificate Subject CN / DNS SANs are not in the configured
//! allowlist, and admits those that are.
//!
//! The tests use [`rcgen`]-generated CA + node certificates so no external
//! PKI is needed.  All tests are gated on `#[cfg(feature = "tls-rustls")]`
//! and use `PemBytes` identity so they work in CI without filesystem access
//! to certificate files.
//!
//! ## Test structure
//!
//! All server/client pairs use real loopback TCP with TLS 1.3.  The
//! client calls `send` to pass a probe payload; the server calls `receive`.
//! A rejected client will fail at `connect_with_tls` or at the first
//! `receive` on the server side (the TLS handshake abort propagates as
//! an IO error on both ends).

#![cfg(feature = "tls-rustls")]

use std::net::SocketAddr;
use std::time::Duration;

use noxu_rep::auth::PeerAllowlist;
use noxu_rep::net::{Channel, TlsTcpChannel, TlsTcpChannelListener};
use noxu_rep::tls::{TlsConfig, TlsIdentity, TrustedCerts};

// ─── Test PKI helpers ────────────────────────────────────────────────────────

/// A minimal test PKI: one CA that signs node certificates.
struct TestPki {
    ca_cert_pem: Vec<u8>,
    ca_key_pair: rcgen::KeyPair,
    ca_cert: rcgen::Certificate,
}

impl TestPki {
    /// Generate a fresh self-signed CA.
    fn new() -> Self {
        let mut ca_params = rcgen::CertificateParams::new(vec![]).unwrap();
        ca_params.is_ca =
            rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        ca_params.distinguished_name.push(rcgen::DnType::CommonName, "test-ca");
        let ca_key = rcgen::KeyPair::generate().unwrap();
        let ca_cert = ca_params.self_signed(&ca_key).unwrap();
        let ca_cert_pem = ca_cert.pem().into_bytes();
        Self { ca_cert_pem, ca_key_pair: ca_key, ca_cert }
    }

    /// Sign a node certificate with the given DNS SANs.
    ///
    /// Returns `(cert_pem, key_pem)`.
    fn sign_node(&self, dns_names: &[&str]) -> (Vec<u8>, Vec<u8>) {
        let sans: Vec<String> =
            dns_names.iter().map(|s| s.to_string()).collect();
        let node_key = rcgen::KeyPair::generate().unwrap();
        let node_params = rcgen::CertificateParams::new(sans).unwrap();
        let node_cert = node_params
            .signed_by(&node_key, &self.ca_cert, &self.ca_key_pair)
            .unwrap();
        (node_cert.pem().into_bytes(), node_key.serialize_pem().into_bytes())
    }

    /// Build a `TlsConfig` for a server node (cert name == server name).
    fn node_tls_config(&self, node_name: &str) -> TlsConfig {
        let (cert_pem, key_pem) = self.sign_node(&[node_name]);
        TlsConfig {
            identity: TlsIdentity::PemBytes { cert: cert_pem, key: key_pem },
            trusted_certs: TrustedCerts::CaBytes(vec![
                self.ca_cert_pem.clone(),
            ]),
            server_name: node_name.to_string(),
        }
    }

    /// Build a `TlsConfig` for a client: its cert has SAN `cert_name` but it
    /// connects to (and validates) a server named `connect_to`.
    ///
    /// This separates the client identity (cert name) from the server it is
    /// connecting to.
    fn client_tls_config(
        &self,
        cert_name: &str,
        connect_to: &str,
    ) -> TlsConfig {
        let (cert_pem, key_pem) = self.sign_node(&[cert_name]);
        TlsConfig {
            identity: TlsIdentity::PemBytes { cert: cert_pem, key: key_pem },
            trusted_certs: TrustedCerts::CaBytes(vec![
                self.ca_cert_pem.clone(),
            ]),
            server_name: connect_to.to_string(),
        }
    }
}

/// Short timeout for receive — keeps failing tests snappy.
const RECV_TIMEOUT: Duration = Duration::from_secs(5);

// ─── Tests ───────────────────────────────────────────────────────────────────

/// A peer whose cert CN/SAN is in the allowlist connects and exchanges data.
#[test]
fn admitted_peer_connects_and_exchanges_data() {
    let pki = TestPki::new();

    // Server: allows "node-1" and "node-2".
    let server_tls = pki.node_tls_config("server.cluster");
    let allowlist = PeerAllowlist::new(["node-1.cluster", "node-2.cluster"]);
    let listener = TlsTcpChannelListener::bind_with_tls_and_allowlist(
        "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        &server_tls,
        allowlist,
    )
    .expect("server bind failed");
    let addr = listener.local_addr().unwrap();

    // Client: cert with SAN "node-1.cluster" — in the allowlist.
    // server_name = "server.cluster" so the client validates the server cert.
    let client_tls = pki.client_tls_config("node-1.cluster", "server.cluster");

    let server_thread = std::thread::spawn(move || {
        let ch = listener.accept().expect("server accept failed");
        let msg = ch
            .receive(RECV_TIMEOUT)
            .expect("server receive failed")
            .expect("server expected Some, got None");
        assert_eq!(msg, b"hello from node-1".to_vec());
    });

    let client = TlsTcpChannel::connect_with_tls(addr, &client_tls)
        .expect("client connect failed");
    client.send(b"hello from node-1").expect("client send failed");

    server_thread.join().expect("server thread panicked");
}

/// A peer whose cert CN/SAN is NOT in the allowlist is rejected at the TLS
/// handshake — the connection fails before any data is exchanged.
#[test]
fn rejected_peer_fails_at_handshake() {
    let pki = TestPki::new();

    // Server: allows only "node-1.cluster".
    let server_tls = pki.node_tls_config("server.cluster");
    let allowlist = PeerAllowlist::new(["node-1.cluster"]);
    let listener = TlsTcpChannelListener::bind_with_tls_and_allowlist(
        "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        &server_tls,
        allowlist,
    )
    .expect("server bind failed");
    let addr = listener.local_addr().unwrap();

    // Client: cert with SAN "evil-peer.cluster" — NOT in the allowlist.
    let client_tls =
        pki.client_tls_config("evil-peer.cluster", "server.cluster");

    // The server thread should see an error (handshake abort).
    let server_thread = std::thread::spawn(move || {
        // accept() returns Ok (TCP accepted), but the TLS handshake runs
        // inside accept() so it should return Err or the receive should fail.
        match listener.accept() {
            Err(_) => { /* TLS handshake rejected — expected */ }
            Ok(ch) => {
                // If accept() succeeded (e.g. lazy handshake), the
                // receive must fail.
                let result = ch.receive(RECV_TIMEOUT);
                assert!(
                    result.is_err(),
                    "server receive should fail for rejected peer, got Ok"
                );
            }
        }
    });

    // Client connect should fail because the server aborts the handshake.
    let result = TlsTcpChannel::connect_with_tls(addr, &client_tls);
    // Either the connect fails directly OR the first send/receive fails.
    // Either way the test must NOT panic.
    if let Ok(ch) = result {
        // If connect "succeeded" (TLS might be lazy), sending should error.
        let _send_result = ch.send(b"should be rejected");
        // Give the server a moment to process the rejection.
        std::thread::sleep(Duration::from_millis(50));
    }

    server_thread.join().expect("server thread panicked");
}

/// A peer from a DIFFERENT CA is rejected even if its SAN would match.
///
/// This tests that chain validation runs BEFORE the allowlist check.
/// A cert signed by a foreign CA must fail chain validation regardless
/// of whether its name is in the allowlist.
#[test]
fn foreign_ca_peer_is_rejected_despite_allowlisted_name() {
    let server_pki = TestPki::new();
    let foreign_pki = TestPki::new(); // completely independent CA

    // Server: allows "node-1.cluster".
    let server_tls = server_pki.node_tls_config("server.cluster");
    let allowlist = PeerAllowlist::new(["node-1.cluster"]);
    let listener = TlsTcpChannelListener::bind_with_tls_and_allowlist(
        "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        &server_tls,
        allowlist,
    )
    .expect("server bind failed");
    let addr = listener.local_addr().unwrap();

    // Client: cert signed by FOREIGN CA but with the allowlisted SAN.
    // The server will reject it because the chain doesn't trace to the
    // server's trusted CA.
    // Note: foreign_pki has a different CA so the client won't validate the
    // server cert (which is signed by server_pki). We use SkipVerification
    // on client side to get past server-cert validation and test server-side
    // client-cert rejection.
    let (foreign_cert_pem, foreign_key_pem) =
        foreign_pki.sign_node(&["node-1.cluster"]);
    let foreign_client_tls = TlsConfig {
        identity: TlsIdentity::PemBytes {
            cert: foreign_cert_pem,
            key: foreign_key_pem,
        },
        // Use the server_pki CA so client validates server cert, but the
        // foreign cert won't chain to server_pki — the server rejects it.
        trusted_certs: TrustedCerts::CaBytes(vec![server_pki.ca_cert_pem]),
        server_name: "server.cluster".to_string(),
    };

    let server_thread = std::thread::spawn(move || {
        match listener.accept() {
            Err(_) => { /* expected: handshake abort */ }
            Ok(ch) => {
                let result = ch.receive(RECV_TIMEOUT);
                assert!(
                    result.is_err(),
                    "server receive should fail for foreign-CA peer, got Ok"
                );
            }
        }
    });

    let result = TlsTcpChannel::connect_with_tls(addr, &foreign_client_tls);
    if let Ok(ch) = result {
        let _send = ch.send(b"foreign ca probe");
        std::thread::sleep(Duration::from_millis(50));
    }

    server_thread.join().expect("server thread panicked");
}

/// An empty allowlist is rejected at construction time (fail-closed).
///
/// Per the design doc: "an empty allowlist means no peer is authorised,
/// which is almost certainly a misconfiguration."
#[test]
fn empty_allowlist_errors_at_construction() {
    let pki = TestPki::new();
    let server_tls = pki.node_tls_config("server.cluster");
    let empty = PeerAllowlist::new::<[&str; 0], &str>([]);

    let result = TlsTcpChannelListener::bind_with_tls_and_allowlist(
        "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        &server_tls,
        empty,
    );
    assert!(
        result.is_err(),
        "empty allowlist must error at construction, got Ok"
    );
    let err = result.err().unwrap().to_string();
    assert!(
        err.contains("empty") || err.contains("allowlist"),
        "error message should mention empty/allowlist, got: {err}"
    );
}

/// A SkipVerification TlsConfig cannot be used with the allowlist verifier
/// (no CA means no chain validation).
#[test]
fn skip_verification_with_allowlist_errors() {
    let tls = TlsConfig::insecure("some-node");
    let allowlist = PeerAllowlist::new(["node-1"]);

    let result = TlsTcpChannelListener::bind_with_tls_and_allowlist(
        "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        &tls,
        allowlist,
    );
    assert!(result.is_err(), "SkipVerification + allowlist must error, got Ok");
    let err = result.err().unwrap().to_string();
    assert!(
        err.contains("SkipVerification") || err.contains("CA"),
        "error should mention SkipVerification or CA, got: {err}"
    );
}

/// Two admitted peers can connect sequentially to the same allowlisted server.
#[test]
fn two_admitted_peers_connect_sequentially() {
    use std::sync::{Arc, Barrier};
    let pki = TestPki::new();

    let server_tls = pki.node_tls_config("server.cluster");
    let allowlist = PeerAllowlist::new(["node-a.cluster", "node-b.cluster"]);
    let listener = TlsTcpChannelListener::bind_with_tls_and_allowlist(
        "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        &server_tls,
        allowlist,
    )
    .expect("server bind failed");
    let addr = listener.local_addr().unwrap();

    // Use a barrier so each client waits for the server to receive its
    // message before the next client connects.  This avoids a race where
    // a client drops its channel (sending TCP RST) before the server
    // has read the data.
    let barrier = Arc::new(Barrier::new(2));
    let barrier_srv = Arc::clone(&barrier);

    let server_thread = std::thread::spawn(move || {
        for expected in [b"from-node-a" as &[u8], b"from-node-b"] {
            let ch = listener.accept().expect("server accept failed");
            let msg = ch
                .receive(RECV_TIMEOUT)
                .expect("receive failed")
                .expect("got None");
            assert_eq!(&msg, expected);
            // Signal the client that the message was received — it can
            // now close its channel gracefully and the next client can
            // proceed.
            barrier_srv.wait();
        }
    });

    for (name, probe) in [
        ("node-a.cluster", b"from-node-a" as &[u8]),
        ("node-b.cluster", b"from-node-b"),
    ] {
        let client_tls = pki.client_tls_config(name, "server.cluster");
        let ch = TlsTcpChannel::connect_with_tls(addr, &client_tls)
            .unwrap_or_else(|e| panic!("connect as {name} failed: {e}"));
        ch.send(probe).unwrap_or_else(|e| panic!("send as {name} failed: {e}"));
        // Wait for the server to confirm it received the message before
        // dropping the channel (avoids RST-before-data race).
        barrier.wait();
        ch.close().ok();
    }

    server_thread.join().expect("server thread panicked");
}

// ─── DER cert-name extraction unit tests ─────────────────────────────────────

/// Verify that `extract_cert_names` returns the expected names from a
/// real rcgen-generated certificate.  This exercises the DER parser in
/// isolation without a full TLS handshake.
#[test]
fn extract_cert_names_from_rcgen_cert() {
    use noxu_rep::auth::extract_cert_names_for_test;

    let sans = vec!["node-1.cluster.example".to_string()];
    let ck = rcgen::generate_simple_self_signed(sans).unwrap();
    let cert_der = ck.cert.der();

    let names = extract_cert_names_for_test(cert_der.as_ref());
    assert!(
        !names.is_empty(),
        "extract_cert_names must return at least one name from a rcgen cert"
    );
    assert!(
        names.iter().any(|n| n == "node-1.cluster.example"),
        "expected 'node-1.cluster.example' in names, got: {:?}",
        names
    );
}

/// Multiple SANs are all extracted.
#[test]
fn extract_cert_names_multiple_sans() {
    use noxu_rep::auth::extract_cert_names_for_test;

    let sans = vec![
        "primary.cluster".to_string(),
        "secondary.cluster".to_string(),
        "tertiary.cluster".to_string(),
    ];
    let ck = rcgen::generate_simple_self_signed(sans).unwrap();
    let names = extract_cert_names_for_test(ck.cert.der().as_ref());
    for expected in ["primary.cluster", "secondary.cluster", "tertiary.cluster"]
    {
        assert!(
            names.iter().any(|n| n == expected),
            "expected '{expected}' in names, got: {names:?}"
        );
    }
}

/// Names are lowercased.
#[test]
fn extract_cert_names_are_lowercased() {
    use noxu_rep::auth::extract_cert_names_for_test;

    let ck =
        rcgen::generate_simple_self_signed(vec!["MyNode.Example".to_string()])
            .unwrap();
    let names = extract_cert_names_for_test(ck.cert.der().as_ref());
    assert!(
        names.iter().any(|n| n == "mynode.example"),
        "names must be lowercased, got: {names:?}"
    );
    assert!(
        !names.iter().any(|n| n == "MyNode.Example"),
        "names must NOT contain mixed-case: {names:?}"
    );
}

/// Garbage bytes produce an empty list (fail-closed).
#[test]
fn extract_cert_names_garbage_input_is_empty() {
    use noxu_rep::auth::extract_cert_names_for_test;
    let names = extract_cert_names_for_test(b"this is not a DER certificate");
    assert!(
        names.is_empty(),
        "garbage input must produce empty list, got: {names:?}"
    );
}
