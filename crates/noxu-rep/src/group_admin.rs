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
