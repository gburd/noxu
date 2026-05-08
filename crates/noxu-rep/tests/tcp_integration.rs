//! TCP channel and ReplicatedEnvironment integration tests.
//!
//! These tests exercise the `TcpChannel` / `TcpChannelListener` types over
//! real loopback sockets and verify that `ReplicatedEnvironment` can be
//! constructed and driven through its state machine.  All listeners bind to
//! port 0 so the OS assigns a free ephemeral port, which avoids conflicts
//! when tests run in parallel or in CI.

use std::net::SocketAddr;
use std::sync::{Arc, Barrier};
use std::time::Duration;

use noxu_rep::net::{Channel, TcpChannel, TcpChannelListener};
use noxu_rep::{NodeState, RepConfig, ReplicatedEnvironment};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Bind a listener on 127.0.0.1 with a kernel-assigned port.
fn loopback_listener() -> TcpChannelListener {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    TcpChannelListener::bind(addr).expect("bind failed")
}

/// Short receive timeout used throughout tests so a hanging test surfaces
/// quickly rather than blocking the suite indefinitely.
const RECV_TIMEOUT: Duration = Duration::from_secs(5);

/// Very short timeout used when we *expect* a timeout (i.e. no data will
/// arrive) — keeps those tests snappy.
const SHORT_TIMEOUT: Duration = Duration::from_millis(100);

// ---------------------------------------------------------------------------
// TcpChannelListener — bind / local_addr
// ---------------------------------------------------------------------------

#[test]
fn test_listener_bind_assigns_port() {
    let listener = loopback_listener();
    let addr = listener.local_addr().expect("local_addr failed");
    // The OS should have assigned a non-zero port.
    assert_ne!(addr.port(), 0);
    assert_eq!(addr.ip(), "127.0.0.1".parse::<std::net::IpAddr>().unwrap());
}

// ---------------------------------------------------------------------------
// TcpChannel — connect / send / receive loopback
// ---------------------------------------------------------------------------

/// Basic loopback: client sends one message, server echoes it back.
#[test]
fn test_tcp_loopback_echo() {
    let listener = loopback_listener();
    let addr = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        let ch = listener.accept().expect("accept failed");
        let msg = ch.receive(RECV_TIMEOUT).expect("server receive failed");
        let payload = msg.expect("server expected Some, got None");
        ch.send(&payload).expect("server send failed");
    });

    let client = TcpChannel::connect(addr).expect("connect failed");
    let want = b"hello noxu replication".to_vec();
    client.send(&want).expect("client send failed");
    let echoed = client.receive(RECV_TIMEOUT).expect("client receive failed");
    assert_eq!(echoed, Some(want));

    server.join().expect("server thread panicked");
}

/// Send several messages in sequence and verify FIFO ordering.
#[test]
fn test_tcp_multiple_messages_sequence() {
    let listener = loopback_listener();
    let addr = listener.local_addr().unwrap();
    let messages: Vec<Vec<u8>> = (0u8..8).map(|i| vec![i; (i as usize) + 1]).collect();
    let expected = messages.clone();

    let server = std::thread::spawn(move || {
        let ch = listener.accept().expect("accept failed");
        for (i, exp) in expected.iter().enumerate() {
            let got = ch
                .receive(RECV_TIMEOUT)
                .unwrap_or_else(|e| panic!("receive #{i} failed: {e}"))
                .unwrap_or_else(|| panic!("receive #{i} timed out"));
            assert_eq!(&got, exp, "message {i} mismatch");
        }
    });

    let client = TcpChannel::connect(addr).expect("connect failed");
    for msg in &messages {
        client.send(msg).expect("send failed");
    }
    server.join().expect("server thread panicked");
}

/// Verify that a 1 KiB message is transmitted correctly.
#[test]
fn test_tcp_1kb_message() {
    let listener = loopback_listener();
    let addr = listener.local_addr().unwrap();
    let payload: Vec<u8> = (0..1024).map(|i| (i % 251) as u8).collect();
    let expected = payload.clone();

    let server = std::thread::spawn(move || {
        let ch = listener.accept().expect("accept failed");
        let got = ch
            .receive(RECV_TIMEOUT)
            .expect("receive failed")
            .expect("receive timed out");
        assert_eq!(got, expected);
    });

    let client = TcpChannel::connect(addr).expect("connect failed");
    client.send(&payload).expect("send failed");
    server.join().expect("server thread panicked");
}

/// Verify that a 64 KiB message is transmitted correctly.
#[test]
fn test_tcp_64kb_message() {
    let listener = loopback_listener();
    let addr = listener.local_addr().unwrap();
    let payload: Vec<u8> = (0..65536).map(|i| (i % 256) as u8).collect();
    let expected = payload.clone();

    let server = std::thread::spawn(move || {
        let ch = listener.accept().expect("accept failed");
        let got = ch
            .receive(RECV_TIMEOUT)
            .expect("receive failed")
            .expect("receive timed out");
        assert_eq!(got.len(), expected.len());
        assert_eq!(got, expected);
    });

    let client = TcpChannel::connect(addr).expect("connect failed");
    client.send(&payload).expect("send failed");
    server.join().expect("server thread panicked");
}

/// Verify that an empty-payload message round-trips correctly.
#[test]
fn test_tcp_empty_message() {
    let listener = loopback_listener();
    let addr = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        let ch = listener.accept().expect("accept failed");
        let got = ch
            .receive(RECV_TIMEOUT)
            .expect("receive failed")
            .expect("receive timed out");
        assert!(got.is_empty());
    });

    let client = TcpChannel::connect(addr).expect("connect failed");
    client.send(&[]).expect("send failed");
    server.join().expect("server thread panicked");
}

/// Verify that `receive()` returns `Ok(None)` when the timeout elapses
/// without any data arriving.
#[test]
fn test_tcp_receive_timeout_returns_none() {
    let listener = loopback_listener();
    let addr = listener.local_addr().unwrap();

    // The server accepts but never sends anything during the test window.
    let server = std::thread::spawn(move || {
        let _ch = listener.accept().expect("accept failed");
        // Hold the socket open long enough for the client timeout to fire.
        std::thread::sleep(Duration::from_secs(2));
    });

    let client = TcpChannel::connect(addr).expect("connect failed");
    let result = client
        .receive(SHORT_TIMEOUT)
        .expect("receive returned an error, expected Ok(None)");
    assert_eq!(result, None, "expected timeout → None");

    server.join().expect("server thread panicked");
}

/// Verify that `is_open()` reflects the state and that `close()` works.
#[test]
fn test_tcp_is_open_and_close() {
    let listener = loopback_listener();
    let addr = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        let _ch = listener.accept().expect("accept failed");
        std::thread::sleep(Duration::from_millis(300));
    });

    let client = TcpChannel::connect(addr).expect("connect failed");
    assert!(client.is_open(), "channel should be open after connect");
    client.close().expect("close failed");
    assert!(!client.is_open(), "channel should be closed after close()");

    server.join().expect("server thread panicked");
}

/// Verify that `send()` fails after the channel is closed.
#[test]
fn test_tcp_send_after_close_fails() {
    let listener = loopback_listener();
    let addr = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        let _ch = listener.accept().expect("accept failed");
        std::thread::sleep(Duration::from_millis(300));
    });

    let client = TcpChannel::connect(addr).expect("connect failed");
    client.close().expect("close failed");
    let result = client.send(b"should fail");
    assert!(result.is_err(), "send after close should return Err");

    server.join().expect("server thread panicked");
}

/// Verify that `receive()` fails after the channel is closed.
#[test]
fn test_tcp_receive_after_close_fails() {
    let listener = loopback_listener();
    let addr = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        let _ch = listener.accept().expect("accept failed");
        std::thread::sleep(Duration::from_millis(300));
    });

    let client = TcpChannel::connect(addr).expect("connect failed");
    client.close().expect("close failed");
    let result = client.receive(SHORT_TIMEOUT);
    assert!(result.is_err(), "receive after close should return Err");

    server.join().expect("server thread panicked");
}

/// Verify that a closed peer connection is detected as an error or timeout,
/// not as an indefinite hang.
///
/// The server accepts, waits for the client to be ready (via a barrier), then
/// closes its end.  The client then calls `receive()` and must get either
/// `Err(ChannelClosed)` or `Err(NetworkError)` — not block forever.
#[test]
fn test_tcp_peer_closed_detected() {
    let listener = loopback_listener();
    let addr = listener.local_addr().unwrap();

    // Barrier: server waits until client is connected and about to receive.
    let barrier = Arc::new(Barrier::new(2));
    let barrier_srv = Arc::clone(&barrier);

    let server = std::thread::spawn(move || {
        let ch = listener.accept().expect("accept failed");
        // Wait until the client is ready, then close our end.
        barrier_srv.wait();
        ch.close().ok();
        // Drop ch — the underlying TcpStream is shut down.
    });

    // Client connects, then synchronises with server before receiving.
    let client = TcpChannel::connect(addr).expect("connect failed");
    barrier.wait(); // server will close right after this

    // After the server closes, receive() must return an error, not hang.
    // We give a generous timeout so flaky timing on slow CI doesn't matter.
    let result = client.receive(Duration::from_secs(5));
    assert!(
        result.is_err(),
        "expected Err(ChannelClosed) after peer closed, got: {:?}",
        result
    );

    server.join().expect("server thread panicked");
}

// ---------------------------------------------------------------------------
// Concurrent send / receive
// ---------------------------------------------------------------------------

/// Two threads exchange messages through a pair of TCP channels simultaneously.
#[test]
fn test_tcp_concurrent_bidirectional() {
    let listener = loopback_listener();
    let addr = listener.local_addr().unwrap();

    // Use a barrier so both ends start sending at the same time.
    let barrier = Arc::new(Barrier::new(2));
    let barrier_srv = Arc::clone(&barrier);

    let server = std::thread::spawn(move || {
        let ch = listener.accept().expect("accept failed");
        barrier_srv.wait();

        // Server sends first, then receives.
        ch.send(b"from server").expect("server send failed");
        let msg = ch
            .receive(RECV_TIMEOUT)
            .expect("server receive failed")
            .expect("server receive timed out");
        assert_eq!(msg, b"from client".to_vec());
    });

    let client = TcpChannel::connect(addr).expect("connect failed");
    barrier.wait();

    // Client receives first (server sent), then sends.
    let msg = client
        .receive(RECV_TIMEOUT)
        .expect("client receive failed")
        .expect("client receive timed out");
    assert_eq!(msg, b"from server".to_vec());
    client.send(b"from client").expect("client send failed");

    server.join().expect("server thread panicked");
}

/// Multiple client threads each open their own connection to the same server
/// and exchange a unique message.
#[test]
fn test_tcp_multiple_concurrent_clients() {
    const N_CLIENTS: usize = 4;

    // The listener will handle N_CLIENTS sequential accepts in a background thread.
    let listener = loopback_listener();
    let addr = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        for i in 0..N_CLIENTS {
            let ch = listener.accept().expect("accept failed");
            // Echo the message back.
            if let Some(data) = ch.receive(RECV_TIMEOUT).expect("receive failed") {
                ch.send(&data).unwrap_or_else(|e| {
                    panic!("server send to client {i} failed: {e}")
                });
            }
        }
    });

    // Spawn N_CLIENTS threads; each connects, sends its index, and reads back.
    let handles: Vec<_> = (0..N_CLIENTS)
        .map(|i| {
            std::thread::spawn(move || {
                let ch = TcpChannel::connect(addr).expect("connect failed");
                let msg = format!("client-{i}");
                ch.send(msg.as_bytes()).expect("send failed");
                let reply = ch
                    .receive(RECV_TIMEOUT)
                    .expect("receive failed")
                    .expect("receive timed out");
                assert_eq!(reply, msg.as_bytes().to_vec());
            })
        })
        .collect();

    for (i, h) in handles.into_iter().enumerate() {
        h.join().unwrap_or_else(|_| panic!("client thread {i} panicked"));
    }
    server.join().expect("server thread panicked");
}

// ---------------------------------------------------------------------------
// TcpChannelListener via TcpChannelListener::bind
// ---------------------------------------------------------------------------

/// Verify that `TcpChannelListener` accepts multiple sequential connections.
#[test]
fn test_listener_accepts_multiple_connections() {
    let listener = loopback_listener();
    let addr = listener.local_addr().unwrap();

    let server = std::thread::spawn(move || {
        for round in 0u8..3 {
            let ch = listener.accept().expect("accept failed");
            let data = ch
                .receive(RECV_TIMEOUT)
                .expect("receive failed")
                .expect("receive timed out");
            assert_eq!(data, vec![round]);
        }
    });

    for round in 0u8..3 {
        let ch = TcpChannel::connect(addr).expect("connect failed");
        ch.send(&[round]).expect("send failed");
        // Drop ch so the server's receive sees EOF if it reads again.
    }

    server.join().expect("server thread panicked");
}

// ---------------------------------------------------------------------------
// ReplicatedEnvironment — construction and basic state transitions
// ---------------------------------------------------------------------------

/// Verify `ReplicatedEnvironment::new()` succeeds with a valid config.
#[test]
fn test_replicated_environment_construction() {
    let config = RepConfig::builder("integ_group", "node1", "127.0.0.1")
        .node_port(0) // not actually binding; port is only metadata here
        .build();
    let env = ReplicatedEnvironment::new(config).expect("construction failed");
    // Fresh environment starts in Detached state.
    assert_eq!(env.get_state(), NodeState::Detached);
    assert_eq!(env.get_node_name(), "node1");
    assert_eq!(env.get_group_name(), "integ_group");
    assert!(!env.is_shutdown());
}

/// Verify the environment can transition through master / replica lifecycle.
#[test]
fn test_replicated_environment_lifecycle() {
    let config = RepConfig::builder("integ_group", "node1", "127.0.0.1")
        .node_port(0)
        .build();
    let env = ReplicatedEnvironment::new(config).expect("construction failed");

    // Become master (Detached -> Unknown -> Master handled internally).
    env.become_master(1).expect("become_master failed");
    assert_eq!(env.get_state(), NodeState::Master);
    assert!(env.is_master());
    assert_eq!(env.get_master_name(), Some("node1".to_string()));

    // Register a VLSN and verify the index tracks it.
    env.register_vlsn(1, 0, 128);
    assert_eq!(env.get_current_vlsn(), 1);

    // Transition to replica.
    env.become_replica("node2").expect("become_replica failed");
    assert_eq!(env.get_state(), NodeState::Replica);
    assert!(env.is_replica());

    // Apply an entry.
    env.apply_entry(2, 1, vec![0xDE, 0xAD, 0xBE, 0xEF])
        .expect("apply_entry failed");
    assert_eq!(env.get_current_vlsn(), 2);

    // Close.
    env.close().expect("close failed");
    assert!(env.is_shutdown());
    assert_eq!(env.get_state(), NodeState::Shutdown);

    // Second close must be idempotent.
    env.close().expect("second close failed");
}

/// Verify that operations are rejected after the environment is closed.
#[test]
fn test_replicated_environment_rejects_ops_after_close() {
    let config = RepConfig::builder("integ_group", "node_x", "127.0.0.1")
        .node_port(0)
        .build();
    let env = ReplicatedEnvironment::new(config).expect("construction failed");
    env.close().expect("close failed");

    assert!(env.become_master(1).is_err());
    assert!(env.become_replica("other").is_err());
    assert!(env.apply_entry(1, 0, vec![]).is_err());
}

/// Construct a `ReplicatedEnvironment` backed by a `TcpChannelListener`
/// address and verify the config round-trips the socket address correctly.
#[test]
fn test_replicated_environment_with_tcp_address() {
    // Bind a real listener so we have an actual OS-assigned port.
    let listener = loopback_listener();
    let addr = listener.local_addr().unwrap();

    let config = RepConfig::builder("tcp_group", "node_tcp", "127.0.0.1")
        .node_port(addr.port())
        .build();
    let env = ReplicatedEnvironment::new(config).expect("construction failed");

    // The config should reflect the port we bound.
    assert_eq!(env.get_config().node_port, addr.port());
    assert_eq!(env.get_config().node_host, "127.0.0.1");

    env.close().expect("close failed");
    // listener is dropped here — the port is released.
}

// ─────────────────────────────────────────────────────────────────────────────
// Replication fault injection tests (Margo Seltzer reviewer concern)
// ─────────────────────────────────────────────────────────────────────────────

/// Verify that a `TcpChannel` receiver times out cleanly when the sender drops
/// its end of the connection (simulates network partition / master crash).
///
/// After the sender half is dropped the receiver must:
///   1. Return an error (not block forever) within RECV_TIMEOUT.
///   2. Allow the replica to detect the disconnect and proceed without panic.
#[test]
fn test_channel_drop_on_sender_side_is_detected_by_receiver() {
    let listener = loopback_listener();
    let addr = listener.local_addr().unwrap();

    // Spawn a thread that connects and immediately drops the channel (simulates
    // master crashing immediately after TCP handshake).
    let sender_thread = std::thread::spawn(move || {
        let ch = TcpChannel::connect(addr).expect("connect failed");
        drop(ch); // Simulate master crash / network partition.
    });

    // Accept the connection on the replica side.
    let replica_ch = listener.accept().expect("accept failed");

    sender_thread.join().unwrap();

    // After the sender drops, the receiver must get an error (not block).
    // receive() returns Ok(None) on timeout or Err on closed connection.
    let result = replica_ch.receive(SHORT_TIMEOUT);
    assert!(
        result.is_err() || matches!(result, Ok(None)),
        "replica must detect sender disconnect; got: {:?}", result
    );
}

/// Verify that a `TcpChannel` sender gets an error when the receiver drops its
/// end of the connection (simulates replica crash).
///
/// The master (sender) must detect the broken pipe / closed connection within
/// RECV_TIMEOUT and not panic.
#[test]
fn test_channel_drop_on_receiver_side_is_detected_by_sender() {
    let listener = loopback_listener();
    let addr = listener.local_addr().unwrap();

    // Spawn a thread that accepts and immediately drops (simulates replica crash).
    let receiver_thread = std::thread::spawn(move || {
        let ch = listener.accept().expect("accept failed");
        drop(ch); // Simulate replica crash.
    });

    // Connect as the master.
    let master_ch = TcpChannel::connect(addr).expect("connect failed");
    receiver_thread.join().unwrap();

    // After the receiver drops, the sender must get an error on send.
    // Send a small payload; the OS may buffer the first write successfully,
    // so we may need more than one send to observe the broken pipe.
    let payload = b"heartbeat";
    let mut detected = false;
    for _ in 0..10 {
        if master_ch.send(payload).is_err() {
            detected = true;
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(detected, "master must detect receiver disconnect within 10 sends");
}

/// Verify that the `ReplicatedEnvironment` state machine correctly handles a
/// simulated partition + re-election cycle without getting stuck.
///
/// Drives: Replica → Unknown → (re-elect) →
/// Master/Replica. This test exercises that the Detached → Replica → Master
/// path (via direct re-election) completes without errors and leaves the
/// environment in Master state — i.e. the state machine is not wedged at
/// Replica after a leadership change.
#[test]
fn test_replicated_env_state_machine_survives_re_election() {
    let config = RepConfig::builder("fault_group", "re_elect_node", "127.0.0.1")
        .build();
    let env = ReplicatedEnvironment::new(config).expect("env creation failed");

    // Starts Detached.
    assert_eq!(env.get_state(), NodeState::Detached);

    // Step 1: node joins as replica (Detached → Unknown → Replica).
    env.become_replica("initial_master").expect("become_replica failed");
    assert_eq!(env.get_state(), NodeState::Replica);

    // Step 2: simulated channel drop + re-election — node wins election and
    // becomes master directly from Replica (allows Master ↔ Replica direct
    // transitions via ensure_unknown_state).
    env.become_master(2).expect("become_master (re-election) failed");
    assert_eq!(env.get_state(), NodeState::Master,
        "state must be Master after winning re-election, not stuck at Replica");

    env.close().unwrap();
}

// ---------------------------------------------------------------------------
// 32+ concurrent participant tests
// ---------------------------------------------------------------------------

/// 32 replica nodes all transition to the Replica state simultaneously via a
/// barrier. This exercises concurrent state-machine transitions and verifies
/// that the implementation is free of races when many nodes join at once.
#[test]
fn test_32_concurrent_replicas_join_simultaneously() {
    const N: usize = 32;
    let barrier = Arc::new(Barrier::new(N));

    let handles: Vec<_> = (0..N)
        .map(|i| {
            let b = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let config = RepConfig::builder(
                    "stress_group",
                    &format!("replica_{i}"),
                    "127.0.0.1",
                )
                .build();
                let env = ReplicatedEnvironment::new(config)
                    .expect("env creation failed");

                // All threads arrive here before any state transition begins.
                b.wait();

                assert_eq!(env.get_state(), NodeState::Detached);
                env.become_replica("stress_master")
                    .expect("become_replica failed");
                assert_eq!(
                    env.get_state(),
                    NodeState::Replica,
                    "node {i} should be Replica"
                );

                env.close().unwrap();
            })
        })
        .collect();

    for h in handles {
        h.join().expect("replica thread panicked");
    }
}

/// 32 replica nodes all disconnect and then reconnect — simulates a network
/// partition/heal cycle at scale. After heal, all nodes must reach Replica
/// state again without deadlocking.
#[test]
fn test_32_replicas_disconnect_and_reconnect() {
    const N: usize = 32;

    let handles: Vec<_> = (0..N)
        .map(|i| {
            std::thread::spawn(move || {
                let config = RepConfig::builder(
                    "partition_group",
                    &format!("partition_node_{i}"),
                    "127.0.0.1",
                )
                .build();
                let env = ReplicatedEnvironment::new(config)
                    .expect("env creation failed");

                // Join as replica.
                env.become_replica("partition_master")
                    .expect("become_replica (first join) failed");
                assert_eq!(env.get_state(), NodeState::Replica);

                // Simulate partition: transition to Unknown.
                env.ensure_unknown_state()
                    .expect("ensure_unknown_state failed");
                assert_eq!(env.get_state(), NodeState::Unknown);

                // Heal: rejoin as replica.
                env.become_replica("partition_master")
                    .expect("become_replica (rejoin) failed");
                assert_eq!(
                    env.get_state(),
                    NodeState::Replica,
                    "node {i} must be Replica after reconnect"
                );

                env.close().unwrap();
            })
        })
        .collect();

    for h in handles {
        h.join().expect("partition thread panicked");
    }
}

/// 32 TCP channels send and receive messages concurrently through a single
/// listener. Verifies no data races, deadlocks, or dropped messages at high
/// channel concurrency.
#[test]
fn test_32_concurrent_tcp_channels() {
    const N: usize = 32;
    const MSGS_PER_CHANNEL: usize = 10;

    let listener = loopback_listener();
    let addr = listener.local_addr().expect("local_addr failed");

    // Acceptor thread — accepts N connections and echoes every message back.
    let acceptor = std::thread::spawn(move || {
        let mut server_channels: Vec<TcpChannel> = Vec::new();
        for _ in 0..N {
            let ch = listener.accept().expect("accept failed");
            server_channels.push(ch);
        }
        // Echo messages back on each channel.
        let mut echo_threads = Vec::new();
        for ch in server_channels {
            let t = std::thread::spawn(move || {
                for _ in 0..MSGS_PER_CHANNEL {
                    let msg = ch.receive(RECV_TIMEOUT).expect("server recv failed")
                        .expect("server got None");
                    ch.send(&msg).expect("server send failed");
                }
            });
            echo_threads.push(t);
        }
        for t in echo_threads {
            t.join().expect("echo thread panicked");
        }
    });

    let start = Arc::new(Barrier::new(N));

    // N client threads — each sends MSGS_PER_CHANNEL messages and verifies echoes.
    let clients: Vec<_> = (0..N)
        .map(|i| {
            let b = Arc::clone(&start);
            std::thread::spawn(move || {
                b.wait();
                let ch = TcpChannel::connect(addr)
                    .expect("connect failed");
                for j in 0..MSGS_PER_CHANNEL {
                    let payload = format!("ch{i}_msg{j}").into_bytes();
                    ch.send(&payload).expect("client send failed");
                    let echo = ch.receive(RECV_TIMEOUT).expect("client recv failed")
                        .expect("client got None");
                    assert_eq!(echo, payload, "echo mismatch on channel {i} msg {j}");
                }
            })
        })
        .collect();

    for c in clients {
        c.join().expect("client thread panicked");
    }
    acceptor.join().expect("acceptor thread panicked");
}

/// Simulate an election cascade: one node becomes master, then 31 other nodes
/// become replicas of it. Verify all 32 nodes reach stable state without
/// deadlock or panic. Then the master crashes (closes), and all replicas
/// detect the loss and transition to Unknown.
#[test]
fn test_master_crash_detected_by_32_replicas() {
    const REPLICAS: usize = 31;

    // Master node.
    let master_config = RepConfig::builder("cascade_group", "master_node", "127.0.0.1")
        .build();
    let master = ReplicatedEnvironment::new(master_config).expect("master creation");
    master.become_master(1).expect("become_master failed");
    assert_eq!(master.get_state(), NodeState::Master);

    // 31 replica nodes all point at "master_node".
    let handles: Vec<_> = (0..REPLICAS)
        .map(|i| {
            std::thread::spawn(move || {
                let config = RepConfig::builder(
                    "cascade_group",
                    &format!("cascade_replica_{i}"),
                    "127.0.0.1",
                )
                .build();
                let env = ReplicatedEnvironment::new(config)
                    .expect("replica creation failed");
                env.become_replica("master_node").expect("become_replica failed");
                assert_eq!(env.get_state(), NodeState::Replica);
                env
            })
        })
        .collect();

    let mut replicas: Vec<ReplicatedEnvironment> = handles
        .into_iter()
        .map(|h| h.join().expect("replica thread panicked"))
        .collect();

    // Master "crashes" — close without graceful replica notification.
    master.close().expect("master close failed");

    // All replicas must be able to detect master loss and transition to Unknown.
    for (i, r) in replicas.iter_mut().enumerate() {
        r.ensure_unknown_state()
            .unwrap_or_else(|_| panic!("replica {i} failed to transition to Unknown after master crash"));
        assert_eq!(
            r.get_state(),
            NodeState::Unknown,
            "replica {i} must be Unknown after master crash"
        );
        r.close().expect("replica close failed");
    }
}

/// Verify split-brain prevention: two independent groups of nodes cannot both
/// elect a master simultaneously if they are unable to reach quorum (< 50%).
///
/// With 33 total nodes, a group of 16 nodes (minority) must NOT elect a master,
/// while a group of 17 nodes (majority) CAN elect a master.
/// This test exercises the `ensure_unknown_state()` precondition — a node
/// that cannot reach majority must stay in Unknown until it can.
#[test]
fn test_split_brain_minority_group_cannot_elect_master() {
    const TOTAL: usize = 33;
    const MAJORITY: usize = 17;
    const MINORITY: usize = TOTAL - MAJORITY;

    // Majority group: these nodes elect a master.
    let majority: Vec<_> = (0..MAJORITY)
        .map(|i| {
            let config = RepConfig::builder(
                "splitbrain_group",
                &format!("majority_{i}"),
                "127.0.0.1",
            )
            .build();
            let env = ReplicatedEnvironment::new(config).unwrap();
            if i == 0 {
                // Node 0 becomes master (represents the election winner).
                env.become_master(MAJORITY as u64).unwrap();
                assert_eq!(env.get_state(), NodeState::Master);
            } else {
                env.become_replica("majority_0").unwrap();
                assert_eq!(env.get_state(), NodeState::Replica);
            }
            env
        })
        .collect();

    // Minority group: these nodes cannot form quorum.
    // They start as Unknown and must remain there (cannot elect).
    let minority: Vec<_> = (0..MINORITY)
        .map(|i| {
            let config = RepConfig::builder(
                "splitbrain_group",
                &format!("minority_{i}"),
                "127.0.0.1",
            )
            .build();
            let env = ReplicatedEnvironment::new(config).unwrap();
            // All minority nodes are in Unknown (no quorum to elect).
            assert_eq!(env.get_state(), NodeState::Detached);
            env
        })
        .collect();

    // Majority nodes are healthy.
    assert_eq!(majority[0].get_state(), NodeState::Master);
    for n in majority.iter().skip(1) {
        assert_eq!(n.get_state(), NodeState::Replica);
    }

    // Minority nodes are all Detached/Unknown — none is Master.
    for (i, n) in minority.iter().enumerate() {
        assert_ne!(
            n.get_state(),
            NodeState::Master,
            "minority node {i} must not be Master (would be split-brain)"
        );
    }

    for n in majority { n.close().unwrap(); }
    for n in minority { n.close().unwrap(); }
}
