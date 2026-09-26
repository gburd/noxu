//! S1 / F3b / F5 verified-peer-identity binding tests.
//!
//! These are the TDD tests for the S1 remediation
//! (`/tmp/audit/remediation/security-S1-design.md`, resolved policy):
//!
//!   * **(A)** `Channel::peer_identity()` surfaces the TLS-verified peer
//!     subject names on the server-accepted side of an mTLS channel, and
//!     `None` on plain TCP / in-process channels.
//!   * **(B)** F3b: under mTLS the Paxos acceptor rejects an
//!     `ElectionProposal` whose self-reported `node_name` does not match the
//!     verified peer identity, and accepts one that does.  The plain-TCP path
//!     (no verified identity) is unaffected.
//!   * **(C)** F5: under mTLS a privileged ADMIN command from an identity that
//!     is not in the admin allowlist is rejected; from an admin-allowlisted
//!     identity it succeeds.  Over an unauthenticated transport a privileged
//!     command is rejected unless `insecure_admin` is set (fail-closed).
//!
//! Every test uses `rcgen`-generated CA + node certs so no external PKI is
//! needed.  Gated on `tls-rustls`.

#![cfg(feature = "tls-rustls")]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use noxu_rep::auth::PeerAllowlist;
use noxu_rep::net::{
    Channel, ServiceHandler, TlsTcpChannel, TlsTcpChannelListener,
};
use noxu_rep::tls::{TlsConfig, TlsIdentity, TrustedCerts};

const RECV_TIMEOUT: Duration = Duration::from_secs(5);

// ─── Test PKI (same shape as peer_allowlist_tls_test.rs) ─────────────────────

struct TestPki {
    ca_cert_pem: Vec<u8>,
    ca_key_pair: rcgen::KeyPair,
    ca_cert: rcgen::Certificate,
}

impl TestPki {
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

/// Spin up an mTLS listener admitting `allow`, then connect a client whose
/// cert carries SAN `client_cert_name`.  Returns the server-accepted channel
/// and the connected client channel.  Both handshakes are forced by an
/// initial probe byte the client sends and the server reads.
fn mtls_pair(
    pki: &TestPki,
    allow: &[&str],
    client_cert_name: &str,
) -> (TlsTcpChannel, TlsTcpChannel) {
    let server_tls = pki.node_tls_config("server.cluster");
    let allowlist = PeerAllowlist::new(allow.iter().copied());
    let listener = TlsTcpChannelListener::bind_with_tls_and_allowlist(
        "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        &server_tls,
        allowlist,
    )
    .expect("server bind failed");
    let addr = listener.local_addr().unwrap();

    let client_tls = pki.client_tls_config(client_cert_name, "server.cluster");

    // The TLS handshake is bidirectional: the client's first `send` cannot
    // complete until the server side is also driving IO.  Run the server's
    // accept+probe-read in its own thread, concurrently with the client's
    // connect+probe-write, so neither blocks the other.  Both channels retain
    // their completed handshake state, so `peer_identity()` is populated after
    // this returns.
    let server_handle = std::thread::spawn(move || {
        let ch = listener.accept().expect("accept failed");
        let probe = ch
            .receive(RECV_TIMEOUT)
            .expect("probe receive failed")
            .expect("no probe");
        assert_eq!(probe, b"__probe__".to_vec());
        ch
    });

    let client = TlsTcpChannel::connect_with_tls(addr, &client_tls)
        .expect("client connect failed");
    client.send(b"__probe__").expect("probe send failed");
    let server = server_handle.join().expect("server probe thread panicked");
    (server, client)
}

// ═════════════════════════════════════════════════════════════════════════
// Part (A): Channel::peer_identity()
// ═════════════════════════════════════════════════════════════════════════

/// (A) The server-accepted side of an mTLS channel reports the *client's*
/// verified cert subject names.  The client-connected side reports `None`.
///
/// FAIL-ON-BASE: base 65ea5165 has no `Channel::peer_identity()` method (the
/// call does not compile); after the fix it returns the verified names.
#[test]
fn a_tls_channel_peer_identity_returns_client_cert_names() {
    let pki = TestPki::new();
    let (server, client) =
        mtls_pair(&pki, &["node-a.cluster"], "node-a.cluster");

    let id = server
        .peer_identity()
        .expect("server-accepted mTLS channel must expose a peer identity");
    assert!(
        id.subject_names.iter().any(|n| n == "node-a.cluster"),
        "expected verified client name 'node-a.cluster', got {:?}",
        id.subject_names
    );

    // The client side sees the server's cert, NOT a verified peer identity.
    assert!(
        client.peer_identity().is_none(),
        "client-connected side must not surface a peer identity"
    );
}

/// (A) Plain TCP and in-process channels have no verified identity.
///
/// FAIL-ON-BASE: `peer_identity()` does not exist on base (compile error).
#[test]
fn a_plain_and_local_channels_have_no_identity() {
    use noxu_rep::net::{LocalChannelPair, TcpChannel, TcpChannelListener};

    // Plain TCP.
    let listener =
        TcpChannelListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr().unwrap();
    let h = std::thread::spawn(move || listener.accept().unwrap());
    let client = TcpChannel::connect(addr).unwrap();
    let server = h.join().unwrap();
    assert!(server.peer_identity().is_none(), "plain TCP server: no identity");
    assert!(client.peer_identity().is_none(), "plain TCP client: no identity");

    // In-process.
    let pair = LocalChannelPair::new();
    assert!(pair.channel_a.peer_identity().is_none(), "LocalChannel: none");
    assert!(pair.channel_b.peer_identity().is_none(), "LocalChannel: none");
}

// ═════════════════════════════════════════════════════════════════════════
// Part (B): F3b election identity binding
// ═════════════════════════════════════════════════════════════════════════

use noxu_rep::elections::PersistentAcceptorState;
use noxu_rep::elections::paxos::run_acceptor_with_state;
use noxu_rep::protocol::ProtocolMessage;

/// Send one framed `ElectionProposal` claiming `claimed_name` down `client`,
/// run the acceptor on the server side, and return its result.
fn run_election_binding(
    server: TlsTcpChannel,
    client: TlsTcpChannel,
    claimed_name: &str,
) -> noxu_rep::Result<Option<String>> {
    let claimed = claimed_name.to_string();
    let proposal = ProtocolMessage::ElectionProposal {
        node_name: claimed,
        vlsn: 1_000_000, // inflated, as an impersonator would send
        priority: 1,
        term: 1,
        dtvlsn: 0,
    };
    // Proposer thread: send phase-1 proposal, then a phase-2 accept so a
    // successful acceptor round completes cleanly.
    let client_handle = std::thread::spawn(move || {
        let _ = client.send(&proposal.encode());
        // Read the acceptor's phase-1 reply (Promise or Reject).
        let _ = client.receive(RECV_TIMEOUT);
        let accept = ProtocolMessage::ElectionResult {
            master: "server-node".into(),
            term: 1,
        };
        let _ = client.send(&accept.encode());
        let _ = client.receive(RECV_TIMEOUT);
    });

    let state = PersistentAcceptorState::in_memory();
    let res = run_acceptor_with_state(
        &server,
        "server-node",
        50,
        1,
        1,
        0,
        &state,
        None,
    );
    let _ = client_handle.join();
    res
}

/// (B) Under mTLS, an `ElectionProposal` whose `node_name` does NOT match the
/// verified peer cert is REJECTED with a `ProtocolError`.
///
/// FAIL-ON-BASE: base believes the self-reported name (no binding), so the
/// acceptor returns `Ok(_)` instead of the `Err` this test requires.
#[test]
fn b_mismatched_election_name_is_rejected_under_mtls() {
    let pki = TestPki::new();
    // Client cert says "node-a.cluster"; it lies and claims to be "node-c".
    let (server, client) =
        mtls_pair(&pki, &["node-a.cluster", "node-c"], "node-a.cluster");

    let res = run_election_binding(server, client, "node-c");
    assert!(
        res.is_err(),
        "mismatched election node_name must be rejected under mTLS, got {res:?}"
    );
    let msg = res.err().unwrap().to_string();
    assert!(
        msg.contains("does not match verified peer identity"),
        "error should name the identity-binding failure, got: {msg}"
    );
}

/// (B) Under mTLS, an `ElectionProposal` whose `node_name` MATCHES the
/// verified peer cert is ACCEPTED (round proceeds normally).
#[test]
fn b_matching_election_name_is_accepted_under_mtls() {
    let pki = TestPki::new();
    let (server, client) =
        mtls_pair(&pki, &["node-a.cluster"], "node-a.cluster");

    // Claims exactly its verified cert name.
    let res = run_election_binding(server, client, "node-a.cluster");
    assert!(
        res.is_ok(),
        "matching election node_name must be accepted under mTLS, got {res:?}"
    );
}

/// (B) The plain-TCP path (no verified identity) is unaffected: a
/// self-reported `node_name` is accepted, exactly as before (documented
/// opt-out under `insecure_no_auth`).
#[test]
fn b_plain_tcp_election_binding_is_unaffected() {
    use noxu_rep::net::{TcpChannel, TcpChannelListener};

    let listener =
        TcpChannelListener::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let addr = listener.local_addr().unwrap();
    let accept_handle = std::thread::spawn(move || listener.accept().unwrap());
    let client = TcpChannel::connect(addr).unwrap();
    let server = accept_handle.join().unwrap();

    // No verified identity on plain TCP → any claimed name is honoured.
    let proposal = ProtocolMessage::ElectionProposal {
        node_name: "anything-goes".into(),
        vlsn: 1,
        priority: 1,
        term: 1,
        dtvlsn: 0,
    };
    let client_handle = std::thread::spawn(move || {
        let _ = client.send(&proposal.encode());
        let _ = client.receive(RECV_TIMEOUT);
        let accept = ProtocolMessage::ElectionResult {
            master: "server-node".into(),
            term: 1,
        };
        let _ = client.send(&accept.encode());
        let _ = client.receive(RECV_TIMEOUT);
    });
    let state = PersistentAcceptorState::in_memory();
    let res = run_acceptor_with_state(
        &server,
        "server-node",
        50,
        1,
        1,
        0,
        &state,
        None,
    );
    let _ = client_handle.join();
    assert!(
        res.is_ok(),
        "plain-TCP election path must be unaffected, got {res:?}"
    );
}

// ═════════════════════════════════════════════════════════════════════════
// Part (C): F5 admin authorization
// ═════════════════════════════════════════════════════════════════════════

use noxu_rep::group_admin::{
    ACK_OK, ACK_REJECTED, AdminService, CMD_SHUTDOWN_GROUP,
};
use noxu_rep::rep_config::RepConfig;
use noxu_rep::replicated_environment::ReplicatedEnvironment;

fn make_env(
    name: &str,
    dir: &tempfile::TempDir,
    peer_allow: &[&str],
    admin_allow: Option<Vec<String>>,
    insecure_admin: bool,
) -> Arc<ReplicatedEnvironment> {
    let cfg = RepConfig::builder("g1", name, "127.0.0.1")
        .node_port(0)
        .env_home(dir.path())
        .peer_allowlist(peer_allow.iter().map(|s| s.to_string()).collect())
        .admin_allowlist(admin_allow)
        .insecure_admin(insecure_admin)
        .build();
    Arc::new(ReplicatedEnvironment::new(cfg).unwrap())
}

/// Drive one `CMD_SHUTDOWN_GROUP` over the given `server`-accepted channel
/// into `AdminService::handle`, with a client thread that sends the command
/// and collects the single-byte ack.  Returns the ack byte.
fn drive_shutdown(
    env: &Arc<ReplicatedEnvironment>,
    server: TlsTcpChannel,
    client: TlsTcpChannel,
) -> u8 {
    let svc = AdminService::new(Arc::downgrade(env));
    let client_handle = std::thread::spawn(move || {
        client.send(&[CMD_SHUTDOWN_GROUP]).unwrap();
        client
            .receive(RECV_TIMEOUT)
            .unwrap()
            .and_then(|v| v.first().copied())
            .unwrap_or(0xFF)
    });
    svc.handle(Box::new(server)).unwrap();
    client_handle.join().unwrap()
}

/// (C) Under mTLS, a privileged command from an identity NOT in the admin
/// allowlist is REJECTED.
///
/// FAIL-ON-BASE: base performs no per-command authz, so any allowlisted peer
/// (and here the caller IS in `peer_allowlist`) shuts the group down → ACK_OK.
#[test]
fn c_privileged_from_non_admin_identity_rejected() {
    let dir = tempfile::TempDir::new().unwrap();
    // peer_allowlist admits node-a and node-b; admin tier is only node-b.
    let env = make_env(
        "server-node",
        &dir,
        &["node-a.cluster", "node-b.cluster"],
        Some(vec!["node-b.cluster".to_string()]),
        false,
    );
    let pki = TestPki::new();
    // Caller authenticates as node-a (allowlisted, but NOT an admin).
    let (server, client) = mtls_pair(
        &pki,
        &["node-a.cluster", "node-b.cluster"],
        "node-a.cluster",
    );
    let ack = drive_shutdown(&env, server, client);
    assert_eq!(
        ack, ACK_REJECTED,
        "non-admin identity must be rejected for SHUTDOWN_GROUP"
    );
    Arc::clone(&env).close().unwrap();
}

/// (C) Under mTLS, a privileged command from an admin-allowlisted identity
/// SUCCEEDS.
#[test]
fn c_privileged_from_admin_identity_succeeds() {
    let dir = tempfile::TempDir::new().unwrap();
    let env = make_env(
        "server-node",
        &dir,
        &["node-a.cluster", "node-b.cluster"],
        Some(vec!["node-b.cluster".to_string()]),
        false,
    );
    let pki = TestPki::new();
    // Caller authenticates as node-b (the admin).
    let (server, client) = mtls_pair(
        &pki,
        &["node-a.cluster", "node-b.cluster"],
        "node-b.cluster",
    );
    let ack = drive_shutdown(&env, server, client);
    assert_eq!(ack, ACK_OK, "admin identity must be authorized for SHUTDOWN");
    // env.close() already ran inside the handler; a second close is harmless.
    let _ = Arc::clone(&env).close();
}

/// (C) Over an unauthenticated transport (None identity) a privileged command
/// is REJECTED unless `insecure_admin` is set.
///
/// FAIL-ON-BASE: base has no authz at all — any TCP peer can shut the group
/// down, so the `insecure_admin = false` case returns ACK_OK on base.
#[test]
fn c_privileged_over_plain_tcp_requires_insecure_admin() {
    use noxu_rep::net::LocalChannelPair;

    // insecure_admin = false → reject.
    {
        let dir = tempfile::TempDir::new().unwrap();
        let env = make_env(
            "n",
            &dir,
            &["node-a.cluster"],
            None,
            /*insecure*/ false,
        );
        let svc = AdminService::new(Arc::downgrade(&env));
        let pair = LocalChannelPair::new();
        pair.channel_b.send(&[CMD_SHUTDOWN_GROUP]).unwrap();
        svc.handle(Box::new(pair.channel_a)).unwrap();
        let ack = pair.channel_b.receive(RECV_TIMEOUT).unwrap().unwrap();
        assert_eq!(
            ack,
            vec![ACK_REJECTED],
            "None identity must be rejected when insecure_admin = false"
        );
        Arc::clone(&env).close().unwrap();
    }

    // insecure_admin = true → allow (operator opted in).
    {
        let dir = tempfile::TempDir::new().unwrap();
        let env = make_env(
            "n",
            &dir,
            &["node-a.cluster"],
            None,
            /*insecure*/ true,
        );
        let svc = AdminService::new(Arc::downgrade(&env));
        let pair = LocalChannelPair::new();
        pair.channel_b.send(&[CMD_SHUTDOWN_GROUP]).unwrap();
        svc.handle(Box::new(pair.channel_a)).unwrap();
        let ack = pair.channel_b.receive(RECV_TIMEOUT).unwrap().unwrap();
        assert_eq!(
            ack,
            vec![ACK_OK],
            "None identity must be allowed when insecure_admin = true"
        );
        let _ = Arc::clone(&env).close();
    }
}
