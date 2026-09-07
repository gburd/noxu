//! Group-administration service: master transfer and group shutdown.
//!
//! Closes findings F7 (`transfer_master`) and F8 (`shutdown_group`)
//! of the 2026 review.
//!
//! # Wire protocol (over `TcpServiceDispatcher` `ADMIN` channel)
//!
//! A single framed message from caller → recipient:
//!
//! ```text
//!   byte 0      : command code
//!     0x01 = TRANSFER_MASTER
//!     0x02 = SHUTDOWN_GROUP
//!     0x03 = STEP_DOWN
//!   bytes 1..9  : term       (u64 LE) — for TRANSFER and STEP_DOWN
//!   bytes 9..   : master_name (UTF-8) — for TRANSFER (the new master)
//! ```
//!
//! The recipient applies the command to its local `ReplicatedEnvironment`
//! and replies with a single-byte ack:
//!
//! ```text
//!   byte 0 : ack
//!     0x00 = OK
//!     0x01 = REJECTED (e.g., recipient is not in a state to honour
//!                       the request; details in the log)
//! ```

use std::sync::Arc;
use std::sync::Weak;
use std::time::Duration;

use crate::error::{RepError, Result};
use crate::net::Channel;
use crate::net::service_dispatcher::{ServiceHandler, connect_to_service};

pub const ADMIN_SERVICE_NAME: &str = "ADMIN";

pub const CMD_TRANSFER_MASTER: u8 = 0x01;
pub const CMD_SHUTDOWN_GROUP: u8 = 0x02;
pub const CMD_STEP_DOWN: u8 = 0x03;

pub const ACK_OK: u8 = 0x00;
pub const ACK_REJECTED: u8 = 0x01;

/// Service handler for the ADMIN channel.
///
/// Holds a `Weak<ReplicatedEnvironment>` so that handler-spawned per-
/// connection threads can apply commands to the live environment
/// without keeping the env alive past `close()`.
pub struct AdminService {
    env: Weak<crate::replicated_environment::ReplicatedEnvironment>,
}

impl AdminService {
    pub fn new(
        env: Weak<crate::replicated_environment::ReplicatedEnvironment>,
    ) -> Self {
        Self { env }
    }
}

impl ServiceHandler for AdminService {
    fn service_name(&self) -> &str {
        ADMIN_SERVICE_NAME
    }

    fn handle(&self, channel: Box<dyn Channel>) -> Result<()> {
        let msg =
            channel.receive(Duration::from_secs(10))?.ok_or_else(|| {
                RepError::ProtocolError("ADMIN: empty command frame".into())
            })?;
        if msg.is_empty() {
            let _ = channel.send(&[ACK_REJECTED]);
            return Ok(());
        }

        let env = match self.env.upgrade() {
            Some(e) => e,
            None => {
                // Env is gone; reject.
                let _ = channel.send(&[ACK_REJECTED]);
                return Ok(());
            }
        };

        match msg[0] {
            CMD_TRANSFER_MASTER => {
                if msg.len() < 1 + 8 {
                    let _ = channel.send(&[ACK_REJECTED]);
                    return Ok(());
                }
                let mut t = [0u8; 8];
                t.copy_from_slice(&msg[1..9]);
                let term = u64::from_le_bytes(t);
                let new_master =
                    String::from_utf8(msg[9..].to_vec()).map_err(|_| {
                        RepError::ProtocolError(
                            "ADMIN: TRANSFER non-UTF8 master".into(),
                        )
                    })?;
                let result = if new_master == env.get_node_name() {
                    // We are the target.  Become master at the new term.
                    env.become_master(term)
                } else {
                    // We are a peer — record the new master.
                    env.become_replica(&new_master)
                };
                let ack = if result.is_ok() { ACK_OK } else { ACK_REJECTED };
                let _ = channel.send(&[ack]);
            }
            CMD_SHUTDOWN_GROUP => {
                let result = env.close();
                let ack = if result.is_ok() { ACK_OK } else { ACK_REJECTED };
                let _ = channel.send(&[ack]);
            }
            CMD_STEP_DOWN => {
                if msg.len() < 1 + 8 {
                    let _ = channel.send(&[ACK_REJECTED]);
                    return Ok(());
                }
                // Old master self-demotes; target name unused on
                // step-down — the recipient just transitions out of
                // mastership.  Caller is expected to still hold
                // mastership at the time of the call.
                let res = env.ensure_unknown_state();
                let ack = if res.is_ok() { ACK_OK } else { ACK_REJECTED };
                let _ = channel.send(&[ack]);
            }
            other => {
                log::warn!("ADMIN: unknown command 0x{:02x}", other);
                let _ = channel.send(&[ACK_REJECTED]);
            }
        }
        Ok(())
    }
}

/// Send a `TRANSFER_MASTER` command to `peer_addr`.
pub fn send_transfer_master(
    peer_addr: std::net::SocketAddr,
    new_master: &str,
    term: u64,
) -> Result<bool> {
    let channel = connect_to_service(peer_addr, ADMIN_SERVICE_NAME)?;
    let mut buf = Vec::with_capacity(1 + 8 + new_master.len());
    buf.push(CMD_TRANSFER_MASTER);
    buf.extend_from_slice(&term.to_le_bytes());
    buf.extend_from_slice(new_master.as_bytes());
    channel.send(&buf)?;
    let reply = channel.receive(Duration::from_secs(10))?.unwrap_or_default();
    Ok(matches!(reply.first(), Some(&ACK_OK)))
}

/// Send a `SHUTDOWN_GROUP` command to `peer_addr`.
pub fn send_shutdown_group(peer_addr: std::net::SocketAddr) -> Result<bool> {
    let channel = connect_to_service(peer_addr, ADMIN_SERVICE_NAME)?;
    channel.send(&[CMD_SHUTDOWN_GROUP])?;
    let reply = channel.receive(Duration::from_secs(10))?.unwrap_or_default();
    Ok(matches!(reply.first(), Some(&ACK_OK)))
}

/// Send a `STEP_DOWN` command to `peer_addr`.
pub fn send_step_down(
    peer_addr: std::net::SocketAddr,
    term: u64,
) -> Result<bool> {
    let channel = connect_to_service(peer_addr, ADMIN_SERVICE_NAME)?;
    let mut buf = Vec::with_capacity(1 + 8);
    buf.push(CMD_STEP_DOWN);
    buf.extend_from_slice(&term.to_le_bytes());
    channel.send(&buf)?;
    let reply = channel.receive(Duration::from_secs(10))?.unwrap_or_default();
    Ok(matches!(reply.first(), Some(&ACK_OK)))
}

/// Shared helper: register the ADMIN service on `dispatcher`, holding a
/// `Weak<ReplicatedEnvironment>` so the handler outlives no longer
/// than the env itself.
pub(crate) fn register_admin_service(
    dispatcher: &crate::net::service_dispatcher::AnyServiceDispatcher,
    env: Weak<crate::replicated_environment::ReplicatedEnvironment>,
) {
    let svc = AdminService::new(env);
    dispatcher.register(ADMIN_SERVICE_NAME, Arc::new(svc));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::LocalChannelPair;
    use crate::rep_config::RepConfig;
    use crate::replicated_environment::ReplicatedEnvironment;

    fn env(name: &str, dir: &tempfile::TempDir) -> Arc<ReplicatedEnvironment> {
        let cfg = RepConfig::builder("g1", name, "127.0.0.1")
            .node_port(0)
            .env_home(dir.path())
            .build();
        Arc::new(ReplicatedEnvironment::new(cfg).unwrap())
    }

    /// `AdminService::handle` on an empty command frame must reject rather
    /// than panic on `msg[0]` — the wire protocol requires at least one
    /// command byte.
    #[test]
    fn handle_rejects_empty_frame() {
        let dir = tempfile::TempDir::new().unwrap();
        let e = env("n", &dir);
        let svc = AdminService::new(Arc::downgrade(&e));
        let pair = LocalChannelPair::new();
        pair.channel_b.send(&[]).unwrap();
        svc.handle(Box::new(pair.channel_a)).unwrap();
        let reply =
            pair.channel_b.receive(Duration::from_secs(1)).unwrap().unwrap();
        assert_eq!(reply, vec![ACK_REJECTED]);
        Arc::clone(&e).close().unwrap();
    }

    /// If the target `ReplicatedEnvironment` has already been dropped (the
    /// `Weak` no longer upgrades), the handler must reject instead of
    /// panicking on `.unwrap()`.
    #[test]
    fn handle_rejects_when_env_is_gone() {
        let weak = {
            let dir = tempfile::TempDir::new().unwrap();
            let e = env("n", &dir);
            Arc::downgrade(&e)
            // `e` (and its TempDir) drop here — env is gone.
        };
        assert!(weak.upgrade().is_none(), "env must actually be dropped");
        let svc = AdminService::new(weak);
        let pair = LocalChannelPair::new();
        pair.channel_b.send(&[CMD_TRANSFER_MASTER]).unwrap();
        svc.handle(Box::new(pair.channel_a)).unwrap();
        let reply =
            pair.channel_b.receive(Duration::from_secs(1)).unwrap().unwrap();
        assert_eq!(reply, vec![ACK_REJECTED]);
    }

    /// A TRANSFER_MASTER frame shorter than the mandatory 9-byte
    /// (command + term) prefix must be rejected, not panic on the
    /// `msg[1..9]` slice.
    #[test]
    fn handle_rejects_short_transfer_frame() {
        let dir = tempfile::TempDir::new().unwrap();
        let e = env("n", &dir);
        let svc = AdminService::new(Arc::downgrade(&e));
        let pair = LocalChannelPair::new();
        pair.channel_b.send(&[CMD_TRANSFER_MASTER, 1, 2, 3]).unwrap();
        svc.handle(Box::new(pair.channel_a)).unwrap();
        let reply =
            pair.channel_b.receive(Duration::from_secs(1)).unwrap().unwrap();
        assert_eq!(reply, vec![ACK_REJECTED]);
        Arc::clone(&e).close().unwrap();
    }

    /// A TRANSFER_MASTER frame whose master-name bytes are not valid UTF-8
    /// must surface a `ProtocolError`, not panic in `String::from_utf8`.
    #[test]
    fn handle_rejects_non_utf8_master_name() {
        let dir = tempfile::TempDir::new().unwrap();
        let e = env("n", &dir);
        let svc = AdminService::new(Arc::downgrade(&e));
        let pair = LocalChannelPair::new();
        let mut msg = vec![CMD_TRANSFER_MASTER];
        msg.extend_from_slice(&1u64.to_le_bytes());
        msg.extend_from_slice(&[0xff, 0xfe]); // invalid UTF-8
        pair.channel_b.send(&msg).unwrap();
        let err = svc.handle(Box::new(pair.channel_a)).unwrap_err();
        assert!(err.to_string().contains("non-UTF8"));
        Arc::clone(&e).close().unwrap();
    }

    /// TRANSFER_MASTER addressed to a peer name that is NOT this node must
    /// take the `become_replica` branch (not `become_master`), recording
    /// the named peer as the new master.
    #[test]
    fn handle_transfer_to_other_peer_calls_become_replica() {
        let dir = tempfile::TempDir::new().unwrap();
        let e = env("this_node", &dir);
        let svc = AdminService::new(Arc::downgrade(&e));
        let pair = LocalChannelPair::new();
        let mut msg = vec![CMD_TRANSFER_MASTER];
        msg.extend_from_slice(&7u64.to_le_bytes());
        msg.extend_from_slice(b"other_node");
        pair.channel_b.send(&msg).unwrap();
        svc.handle(Box::new(pair.channel_a)).unwrap();
        let reply =
            pair.channel_b.receive(Duration::from_secs(1)).unwrap().unwrap();
        assert_eq!(reply, vec![ACK_OK]);
        assert!(e.is_replica());
        assert_eq!(e.get_master_name(), Some("other_node".to_string()));
        Arc::clone(&e).close().unwrap();
    }

    /// STEP_DOWN on a node currently in `Master` state must succeed and
    /// leave the node in `Unknown` (no longer master) — the practical
    /// effect `ensure_unknown_state` exists for.
    #[test]
    fn handle_step_down_demotes_master() {
        let dir = tempfile::TempDir::new().unwrap();
        let e = env("n", &dir);
        e.become_master(1).unwrap();
        assert!(e.is_master());
        let svc = AdminService::new(Arc::downgrade(&e));
        let pair = LocalChannelPair::new();
        let mut msg = vec![CMD_STEP_DOWN];
        msg.extend_from_slice(&2u64.to_le_bytes());
        pair.channel_b.send(&msg).unwrap();
        svc.handle(Box::new(pair.channel_a)).unwrap();
        let reply =
            pair.channel_b.receive(Duration::from_secs(1)).unwrap().unwrap();
        assert_eq!(reply, vec![ACK_OK]);
        assert!(
            !e.is_master(),
            "node must no longer be master after step-down"
        );
        Arc::clone(&e).close().unwrap();
    }

    /// A STEP_DOWN frame shorter than the mandatory 9-byte prefix must be
    /// rejected rather than panic.
    #[test]
    fn handle_rejects_short_step_down_frame() {
        let dir = tempfile::TempDir::new().unwrap();
        let e = env("n", &dir);
        let svc = AdminService::new(Arc::downgrade(&e));
        let pair = LocalChannelPair::new();
        pair.channel_b.send(&[CMD_STEP_DOWN, 1]).unwrap();
        svc.handle(Box::new(pair.channel_a)).unwrap();
        let reply =
            pair.channel_b.receive(Duration::from_secs(1)).unwrap().unwrap();
        assert_eq!(reply, vec![ACK_REJECTED]);
        Arc::clone(&e).close().unwrap();
    }

    /// An unrecognised command byte must be rejected, not panic on an
    /// unmatched `match` arm.
    #[test]
    fn handle_rejects_unknown_command() {
        let dir = tempfile::TempDir::new().unwrap();
        let e = env("n", &dir);
        let svc = AdminService::new(Arc::downgrade(&e));
        let pair = LocalChannelPair::new();
        pair.channel_b.send(&[0xEE]).unwrap();
        svc.handle(Box::new(pair.channel_a)).unwrap();
        let reply =
            pair.channel_b.receive(Duration::from_secs(1)).unwrap().unwrap();
        assert_eq!(reply, vec![ACK_REJECTED]);
        Arc::clone(&e).close().unwrap();
    }

    /// `service_name()` must report the wire-protocol constant so the
    /// `TcpServiceDispatcher` / `ServiceDispatcher` route ADMIN connections
    /// correctly.
    #[test]
    fn service_name_matches_constant() {
        let dir = tempfile::TempDir::new().unwrap();
        let e = env("n", &dir);
        let svc = AdminService::new(Arc::downgrade(&e));
        assert_eq!(svc.service_name(), ADMIN_SERVICE_NAME);
        Arc::clone(&e).close().unwrap();
    }

    /// End-to-end `send_step_down` against a real `TcpServiceDispatcher`:
    /// exercises the client-side framing (`send_step_down`) together with
    /// the server-side STEP_DOWN branch that is otherwise only reachable
    /// over the network.
    #[test]
    fn send_step_down_round_trips_over_tcp() {
        let dir = tempfile::TempDir::new().unwrap();
        let mut cfg = RepConfig::builder("g1", "n", "127.0.0.1")
            .node_port(0)
            .env_home(dir.path())
            .build();
        cfg.insecure_no_auth = true;
        let e = Arc::new(ReplicatedEnvironment::new(cfg).unwrap());
        e.become_master(1).unwrap();
        e.register_admin_service();
        let addr = e.bound_addr().expect("admin dispatcher must bind");

        let ok = send_step_down(addr, 2).expect("step-down call must succeed");
        assert!(ok, "master must ack STEP_DOWN with ACK_OK");

        let mut demoted = !e.is_master();
        for _ in 0..50 {
            if demoted {
                break;
            }
            std::thread::sleep(Duration::from_millis(20));
            demoted = !e.is_master();
        }
        assert!(demoted, "node must no longer be master after STEP_DOWN");
        Arc::clone(&e).close().unwrap();
    }
}
