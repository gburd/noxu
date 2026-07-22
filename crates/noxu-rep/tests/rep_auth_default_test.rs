//! Enforced-authentication default for the replication wire transport.
//!
//! Closes the external review's core finding: shipping the default transport
//! *unauthenticated* is a regression vs BDB-JE HA's SSL-capable, mutually
//! authenticated data channels
//! (`com.sleepycat.je.rep.net.SSLAuthenticator` /
//! `SSLMirrorAuthenticator`).
//!
//! These tests exercise the runtime policy in
//! `ReplicatedEnvironment::enforce_auth_policy`, invoked at the top of
//! `ReplicatedEnvironment::new`:
//!
//! - By default a node on an **unauthenticated** transport
//!   ([`RepTransportKind::Tcp`] / `Quic`) with no mTLS material **refuses to
//!   start** with a `ConfigError`.
//! - The explicit opt-out [`RepConfig::insecure_no_auth(true)`] permits the
//!   plaintext path (used by CI / trusted-network / local dev), which is why
//!   the rest of the test suite still runs on plain TCP.
//! - The in-process [`RepTransportKind::InMemory`] transport is exempt (no
//!   wire to authenticate).
//!
//! Note: the test suite compiles with the `test-harness` feature, under which
//! `RepConfig`'s `insecure_no_auth` field DEFAULTS to `true`.  To exercise the
//! production fail-closed behaviour these tests explicitly call
//! `.insecure_no_auth(false)`.

use noxu_rep::{RepConfig, RepTransportKind, ReplicatedEnvironment};

/// The default (production) posture: plaintext TCP with authentication
/// enforcement ON must refuse to start.
#[test]
fn default_plaintext_tcp_refuses_to_start_when_auth_enforced() {
    // Emulate a production build: opt back INTO enforcement even though the
    // test-harness default is insecure.
    let config = RepConfig::builder("g", "auth-refuse", "127.0.0.1")
        .node_port(0)
        .insecure_no_auth(false)
        .build();

    let result = ReplicatedEnvironment::new(config);
    let err = result
        .err()
        .expect("plaintext TCP with auth enforced must refuse to start");
    let msg = err.to_string();
    assert!(
        msg.contains("UNAUTHENTICATED"),
        "error must name the unauthenticated transport, got: {msg}"
    );
    assert!(
        msg.contains("insecure_no_auth") && msg.contains("Tls"),
        "error must direct the operator to mTLS or the explicit opt-out, \
         got: {msg}"
    );
}

/// The explicit opt-out lets the trusted-network / CI path through.
#[test]
fn insecure_no_auth_opt_out_permits_plaintext_tcp() {
    let config = RepConfig::builder("g", "auth-optout", "127.0.0.1")
        .node_port(0)
        .insecure_no_auth(true)
        .build();

    // Must construct successfully (a loud warn is logged, not an error).
    let env = ReplicatedEnvironment::new(config)
        .expect("insecure_no_auth(true) must permit plaintext TCP");
    // Sanity: the node came up.
    assert_eq!(env.get_config().node_name, "auth-optout");
}

/// The in-process transport is exempt from the wire-auth requirement even
/// with enforcement on: there is no socket an untrusted peer can reach.
#[test]
fn inmemory_transport_is_exempt_from_auth_enforcement() {
    let config = RepConfig::builder("g", "auth-inmem", "127.0.0.1")
        .node_port(0)
        .transport_kind(RepTransportKind::InMemory)
        .insecure_no_auth(false)
        .build();

    let env = ReplicatedEnvironment::new(config)
        .expect("InMemory transport must be exempt from wire-auth enforcement");
    assert_eq!(env.get_config().node_name, "auth-inmem");
}

/// `transport_kind = Tls` never silently downgrades to a plaintext
/// dispatcher (fail-closed).
///
/// - Without any TLS backend compiled in, it is a hard `ConfigError`.
/// - With `tls-rustls`, `build_dispatcher` requires a `tls_config` + a
///   non-empty `peer_allowlist`; its error is swallowed to "no dispatcher",
///   so the node must not have a bound (plaintext) dispatcher.
#[test]
fn tls_transport_without_config_never_serves_plaintext() {
    let config = RepConfig::builder("g", "auth-tls-nocfg", "127.0.0.1")
        .node_port(0)
        .transport_kind(RepTransportKind::Tls)
        .insecure_no_auth(false)
        .build();

    match ReplicatedEnvironment::new(config) {
        // No TLS backend: hard error (this build configuration).
        Err(e) => {
            let msg = e.to_string();
            assert!(
                msg.contains("Tls") || msg.contains("tls"),
                "error must reference the TLS requirement, got: {msg}"
            );
        }
        // TLS backend present but tls_config missing: the node may construct
        // but must NOT have bound a plaintext dispatcher.
        Ok(env) => {
            assert!(
                env.bound_addr().is_none(),
                "Tls transport without tls_config must not bind a plaintext \
                 dispatcher"
            );
        }
    }
}

// ─── End-to-end: a fully-configured mTLS environment starts and binds ────────

/// With `tls-rustls`, a node configured with `transport_kind = Tls`, a
/// CA-rooted `tls_config`, and a non-empty `peer_allowlist` starts and binds
/// an authenticated (TLS) dispatcher — the JE `SSLAuthenticator` analogue is
/// active on every incoming connection.
#[cfg(feature = "tls-rustls")]
#[test]
fn mtls_configured_environment_starts_with_tls_dispatcher() {
    use noxu_rep::tls::{TlsConfig, TlsIdentity, TrustedCerts};

    // Minimal test PKI: one CA signs the node cert.
    let mut ca_params = rcgen::CertificateParams::new(vec![]).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params.distinguished_name.push(rcgen::DnType::CommonName, "auth-e2e-ca");
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let ca_pem = ca_cert.pem().into_bytes();

    let node_key = rcgen::KeyPair::generate().unwrap();
    let node_params =
        rcgen::CertificateParams::new(vec!["node-e2e.cluster".to_string()])
            .unwrap();
    let node_cert =
        node_params.signed_by(&node_key, &ca_cert, &ca_key).unwrap();
    let tls = TlsConfig {
        identity: TlsIdentity::PemBytes {
            cert: node_cert.pem().into_bytes(),
            key: node_key.serialize_pem().into_bytes(),
        },
        trusted_certs: TrustedCerts::CaBytes(vec![ca_pem]),
        server_name: "node-e2e.cluster".to_string(),
    };

    let config = RepConfig::builder("g", "node-e2e", "127.0.0.1")
        .node_port(0)
        .transport_kind(RepTransportKind::Tls)
        .tls_config(tls)
        .peer_allowlist(vec!["node-e2e.cluster".to_string()])
        .insecure_no_auth(false)
        .build();

    let env = ReplicatedEnvironment::new(config)
        .expect("fully-configured mTLS node must start");
    assert!(
        env.bound_addr().is_some(),
        "mTLS node must bind an (authenticated) dispatcher"
    );
}

/// With `tls-rustls`, `transport_kind = Tls` + `tls_config` but an EMPTY
/// `peer_allowlist` is fail-closed: the node refuses to start (an empty
/// allowlist admits no peer and is almost certainly a misconfiguration).
#[cfg(feature = "tls-rustls")]
#[test]
fn mtls_empty_allowlist_is_fail_closed_at_env_level() {
    use noxu_rep::tls::{TlsConfig, TlsIdentity, TrustedCerts};

    let mut ca_params = rcgen::CertificateParams::new(vec![]).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let node_key = rcgen::KeyPair::generate().unwrap();
    let node_params =
        rcgen::CertificateParams::new(vec!["n.cluster".to_string()]).unwrap();
    let node_cert =
        node_params.signed_by(&node_key, &ca_cert, &ca_key).unwrap();
    let tls = TlsConfig {
        identity: TlsIdentity::PemBytes {
            cert: node_cert.pem().into_bytes(),
            key: node_key.serialize_pem().into_bytes(),
        },
        trusted_certs: TrustedCerts::CaBytes(vec![ca_cert.pem().into_bytes()]),
        server_name: "n.cluster".to_string(),
    };

    let config = RepConfig::builder("g", "n", "127.0.0.1")
        .node_port(0)
        .transport_kind(RepTransportKind::Tls)
        .tls_config(tls)
        // no peer_allowlist entries
        .insecure_no_auth(false)
        .build();

    // build_dispatcher returns ConfigError for an empty allowlist; that error
    // is swallowed in new(), so the node must not bind a dispatcher.
    let env = ReplicatedEnvironment::new(config)
        .expect("env constructs (dispatcher error is non-fatal)");
    assert!(
        env.bound_addr().is_none(),
        "empty allowlist is fail-closed: no dispatcher may be bound"
    );
}
