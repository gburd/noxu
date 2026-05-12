# Transport Layer

Noxu DB supports two network transports for replication: **TCP** and **QUIC**.
Both implement the `ReplicationChannel` trait and are interchangeable at
configuration time.

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
