# Transport Layer

> **v2.0 status — GA.** The TCP service-name handshake is bounded
> at 256 bytes.  Network restore via the dispatcher's `RESTORE`
> service is wired end-to-end via
> `ReplicatedEnvironment::bootstrap_via_dispatcher`.
>
> **v2.4 update.** The in-memory transport, originally
> a `cfg(test)` test fixture, is now a first-class production
> transport.  See [In-Memory Transport](in-memory-transport.md).

Noxu DB supports four network transports for replication:

| Transport | Channel type | Use case |
|-----------|--------------|----------|
| **TCP**       | `TcpChannel`        | Plain LAN/WAN replication (default) |
| **TLS**       | `TlsTcpChannel`     | Encrypted WAN (`tls-rustls` or `tls-native`) |
| **QUIC**      | `QuicMultiplexedChannel` | Multiplexed UDP (`quic` feature) |
| **In-memory** | `InMemoryEndpoint`  | In-process clusters & tests (v2.4) |

All four implement the same `Channel` trait and are interchangeable
at the protocol layer; higher-level code (feeder, replica stream,
elections) consumes `dyn Channel` and works identically over any
transport.  See `RepTransportKind` on `RepConfig` for the
declarative selector.

## TCP Transport

`TcpChannel` provides simple, reliable ordered delivery over TCP. It is the
default transport.

### Features

- `connect(addr)` — connect to a peer by `SocketAddr`
- `connect_host(host, port)` — DNS resolution + Happy Eyeballs (IPv6 preferred)
- `connect_timeout` — 30s connection timeout (prevents OS SYN-retry hang)
- `read_timeout` — minimum 30s (prevents hang under packet loss)
- `bind_dual_stack(port)` — listen on `[::]:port` (dual-stack), fallback to `0.0.0.0:port`

### TcpChannelListener

The server-side listener accepts downstream connections:

```rust
let listener = TcpChannel::bind_dual_stack(5001)?;
while let Some(channel) = listener.accept()? {
    spawn_feeder_thread(channel);
}
```

## QUIC Transport

`QuicMultiplexedChannel` provides 4 independent QUIC streams per connection,
preventing head-of-line blocking between different message types:

| Stream | Purpose |
|--------|---------|
| 0 — `heartbeat` | Phi accrual heartbeat messages |
| 1 — `log` | Replicated log entry stream |
| 2 — `ack` | Replica acknowledgments |
| 3 — `restore` | Network restore file transfer |

VLSN sync uses **unreliable QUIC datagrams** (loss is acceptable because the
next VLSN update supersedes the lost one).

### PMTUD Disabled

Path MTU Discovery is disabled (`mtu_discovery_config(None)`) on all QUIC
configs. This is because PMTUD probes are sensitive to netem
duplicate/corrupt injection (they trigger a `quinn-proto` assertion at
`mtud.rs:88`). On loopback the MTU is fixed at 65535 so PMTUD adds no value.

### Reconnect with 0-RTT

`QuicMultiplexedChannel::into_reconnect_token()` returns a token containing
the `Endpoint` and `Runtime`. `connect_with_token(token, addr, name)` reuses
the endpoint for 0-RTT reconnect (no new TLS handshake required).

## TLS Transport and mTLS Peer Enforcement (v3.1.0)

`TlsTcpChannel` provides encrypted TCP replication.  Use the `tls-rustls`
feature for a pure-Rust implementation (no system library dependency).

### mTLS is the enforced default

`ReplicatedEnvironment::new` **refuses to start a replication dispatcher on an
unauthenticated wire transport by default.** A node configured with the
default plaintext `RepTransportKind::Tcp` (or skip-verify `Quic`) and no mTLS
material fails with a `ConfigError`:

```text
node 'node-1': replication would start on an UNAUTHENTICATED Tcp transport,
which is refused by default. Configure mutually-authenticated channels with
`RepConfig::transport_kind(RepTransportKind::Tls)` + `.tls_config(..)` + a
non-empty `.peer_allowlist(..)`, OR, for a trusted-network / development /
CI deployment, explicitly opt out with `RepConfig::insecure_no_auth(true)`.
```

This mirrors BDB-JE HA, which authenticates the data channel via mutual TLS
(`com.sleepycat.je.rep.net.SSLAuthenticator` / `SSLMirrorAuthenticator`) so a
peer's identity is *verified from its certificate*, not self-claimed on the
wire — you cannot accidentally run an unauthenticated replication cluster.

To configure the authenticated production path, set all three on `RepConfig`:

```rust
use noxu_rep::{RepConfig, RepTransportKind, TlsConfig, TlsIdentity, TrustedCerts};

let tls = TlsConfig::for_replication(
    TlsIdentity::PemFiles {
        cert: "/etc/noxu/cert.pem".into(),
        key:  "/etc/noxu/key.pem".into(),
    },
    TrustedCerts::CaFiles(vec!["/etc/noxu/ca.pem".into()]),
    "node-1.cluster.example",
)?;

let config = RepConfig::builder("group", "node-1", "10.0.0.1")
    .transport_kind(RepTransportKind::Tls)
    .tls_config(tls)
    .peer_allowlist(vec![
        "node-1.cluster.example".to_string(),
        "node-2.cluster.example".to_string(),
        "node-3.cluster.example".to_string(),
    ])
    .build();
```

With `transport_kind = Tls`, the RESTORE, PEER_FEEDER, ELECTION, and ADMIN
services all run on the mutually-authenticated dispatcher, and the election
RPC's proposer side connects with `connect_to_service_tls` — so every Paxos
promise/accept traverses an authenticated channel (the on-path "flip the
master" vector is closed at the transport layer). Note: mTLS authenticates
the *wire*, not each message; per-message election signing is not implemented
(see [known limitations](../operations/known-limitations.md)).

### Explicit opt-out for trusted networks / dev / CI

```rust
let config = RepConfig::builder("group", "node-1", "10.0.0.1")
    // Plaintext / in-memory transport, no peer authentication.
    .insecure_no_auth(true)
    .build();
```

`insecure_no_auth(true)` permits the plaintext / skip-verify path and emits a
loud `log::warn!` at startup. Only enable it where every peer IP is statically
known and the firewall / VPC blocks all other inbound traffic to the
replication port. (Under `cfg(test)` / the `test-harness` feature this
defaults to `true` so the test suite and the in-process harness run without
per-test PKI.)

### Quick setup (server)

```rust
use noxu_rep::{PeerAllowlist, TlsConfig, TlsIdentity, TrustedCerts};
use noxu_rep::net::TlsTcpChannelListener;

let server_tls = TlsConfig::for_replication(
    TlsIdentity::PemFiles {
        cert: "/etc/noxu/cert.pem".into(),
        key:  "/etc/noxu/key.pem".into(),
    },
    TrustedCerts::CaFiles(vec!["/etc/noxu/ca.pem".into()]),
    "node-1.cluster.example",
)?;

// mTLS enforcement: only listed peers admitted.
let allowlist = PeerAllowlist::new([
    "node-1.cluster.example",
    "node-2.cluster.example",
    "node-3.cluster.example",
]);
let listener = TlsTcpChannelListener::bind_with_tls_and_allowlist(
    "0.0.0.0:5001".parse()?,
    &server_tls,
    allowlist,
)?;
```

### Quick setup (client)

```rust
use noxu_rep::{TlsConfig, TlsIdentity, TrustedCerts};
use noxu_rep::net::TlsTcpChannel;

let client_tls = TlsConfig::for_replication(
    TlsIdentity::PemFiles {
        cert: "/etc/noxu/cert.pem".into(),
        key:  "/etc/noxu/key.pem".into(),
    },
    TrustedCerts::CaFiles(vec!["/etc/noxu/ca.pem".into()]),
    "node-2.cluster.example",  // server name to validate against
)?;

// Client automatically presents its certificate (mTLS client-auth).
let channel = TlsTcpChannel::connect_with_tls(addr, &client_tls)?;
```

### How `peer_allowlist` enforcement works

When `bind_with_tls_and_allowlist` is used, the server installs a
`PeerAllowlistVerifier` (`rustls::server::danger::ClientCertVerifier`):

1. **Chain validation** — client cert must chain to the configured CA.
2. **Name check** — the cert's Subject CN and all DNS SANs are extracted.
   At least one must match an entry in the allowlist
   (case-insensitive, exact match, no wildcards).
3. **Fail-closed** — an empty allowlist is rejected at construction time
   with a `ConfigError`; a peer with no matching name fails the handshake.

### Certificate requirements

| Party | Must have | Notes |
|-------|-----------|-------|
| Server cert | SAN / CN matching `server_name` | Validated by client |
| Client cert | SAN / CN in server's `peer_allowlist` | Validated by server |
| Both certs | Signed by a common CA | Configured via `TrustedCerts::CaFiles` |

### `tls-native` limitation

The `tls-native` backend (OpenSSL/LibreSSL) does not support server-side
client-cert verification (`native_tls::TlsAcceptorBuilder` has no such API).
`bind_with_tls_and_allowlist` is only available under `tls-rustls`.
If you attempt to use mTLS intent (non-empty `TrustedCerts`) with a
`tls-native` server, construction returns a `ConfigError`.

## ReplicationChannel Trait

Both transports implement:

```rust
pub trait ReplicationChannel: Send + Sync {
    fn send(&self, data: &[u8]) -> Result<()>;
    fn receive(&self, timeout: Duration) -> Result<Option<Vec<u8>>>;
    fn close(&self) -> Result<()>;

    // QUIC multiplexed streams:
    fn heartbeat_channel(&self) -> &dyn Channel;
    fn log_channel(&self) -> &dyn Channel;
    fn ack_channel(&self) -> &dyn Channel;
    fn restore_channel(&self) -> &dyn Channel;

    // VLSN unreliable datagrams:
    fn send_vlsn_datagram(&self, vlsn: i64) -> Result<()>;
    fn recv_vlsn_datagram(&self, timeout: Duration) -> Result<Option<i64>>;
}
```

## DNS and IPv6

`connect_host` resolves hostnames via `(host, port).to_socket_addrs()` and
applies **Happy Eyeballs** ordering: IPv6 candidates are sorted before IPv4.
Connection attempts use a 30s timeout each.

## In-Memory Transport

`InMemoryTransport` provides an in-process channel mesh
with the same `Channel` trait as TCP / TLS / QUIC.  Use it for
embedded multi-node deployments, integration tests, and Stateright
property-test driver harnesses.  Full chapter:
[In-Memory Transport](in-memory-transport.md).
