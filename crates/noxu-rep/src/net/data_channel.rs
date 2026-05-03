//! Typed protocol message channel wrapper.
//!
//! Wraps a raw `Channel` with protocol message serialization, providing
//! send/receive of typed `ProtocolMessage` values and message counting
//! statistics. This corresponds to the pattern in JE where protocol
//! messages are serialized/deserialized over the underlying DataChannel.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use super::channel::Channel;
use crate::error::Result;
use crate::protocol::ProtocolMessage;

/// A channel that sends and receives typed protocol messages.
///
/// Wraps a raw `Channel` and provides automatic serialization via
/// `ProtocolMessage::encode` and `ProtocolMessage::decode`. Tracks
/// message counts for monitoring.
pub struct DataChannel {
    /// The underlying raw channel.
    inner: Arc<dyn Channel>,
    /// Name identifying the peer node on this channel.
    node_name: String,
    /// Count of messages sent through this channel.
    messages_sent: AtomicU64,
    /// Count of messages received through this channel.
    messages_received: AtomicU64,
}

impl DataChannel {
    /// Create a new `DataChannel` wrapping the given raw channel.
    ///
    /// # Arguments
    /// * `channel` - The underlying channel for byte transport.
    /// * `node_name` - The name of the peer node on this channel.
    pub fn new(channel: Arc<dyn Channel>, node_name: String) -> Self {
        Self {
            inner: channel,
            node_name,
            messages_sent: AtomicU64::new(0),
            messages_received: AtomicU64::new(0),
        }
    }

    /// Send a protocol message through the channel.
    ///
    /// The message is encoded to bytes via `ProtocolMessage::encode` and
    /// sent through the underlying channel.
    pub fn send_message(&self, msg: &ProtocolMessage) -> Result<()> {
        let data = msg.encode();
        self.inner.send(&data)?;
        self.messages_sent.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Receive a protocol message with a timeout.
    ///
    /// Blocks until a message is available or the timeout expires.
    /// Returns `Ok(None)` on timeout.
    pub fn receive_message(
        &self,
        timeout: Duration,
    ) -> Result<Option<ProtocolMessage>> {
        match self.inner.receive(timeout)? {
            Some(data) => {
                let msg = ProtocolMessage::decode(&data)?;
                self.messages_received.fetch_add(1, Ordering::Relaxed);
                Ok(Some(msg))
            }
            None => Ok(None),
        }
    }

    /// Get the peer node name.
    pub fn get_node_name(&self) -> &str {
        &self.node_name
    }

    /// Get the total number of messages sent.
    pub fn messages_sent(&self) -> u64 {
        self.messages_sent.load(Ordering::Relaxed)
    }

    /// Get the total number of messages received.
    pub fn messages_received(&self) -> u64 {
        self.messages_received.load(Ordering::Relaxed)
    }

    /// Close the underlying channel.
    pub fn close(&self) -> Result<()> {
        self.inner.close()
    }

    /// Check if the underlying channel is open.
    pub fn is_open(&self) -> bool {
        self.inner.is_open()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::channel::LocalChannelPair;

    #[test]
    fn test_send_receive_message() {
        let pair = LocalChannelPair::new();
        let dc_a = DataChannel::new(Arc::new(pair.channel_a), "node_a".into());
        let dc_b = DataChannel::new(Arc::new(pair.channel_b), "node_b".into());

        let msg =
            ProtocolMessage::Heartbeat { master_vlsn: 0, timestamp_ms: 12345 };
        dc_a.send_message(&msg).unwrap();

        let received = dc_b.receive_message(Duration::from_secs(1)).unwrap();
        assert!(received.is_some());
        match received.unwrap() {
            ProtocolMessage::Heartbeat { timestamp_ms, .. } => {
                assert_eq!(timestamp_ms, 12345)
            }
            other => panic!("unexpected message: {:?}", other),
        }
    }

    #[test]
    fn test_bidirectional_messages() {
        let pair = LocalChannelPair::new();
        let dc_a = DataChannel::new(Arc::new(pair.channel_a), "node_a".into());
        let dc_b = DataChannel::new(Arc::new(pair.channel_b), "node_b".into());

        let msg_a =
            ProtocolMessage::Heartbeat { master_vlsn: 0, timestamp_ms: 100 };
        let msg_b =
            ProtocolMessage::Heartbeat { master_vlsn: 0, timestamp_ms: 200 };

        dc_a.send_message(&msg_a).unwrap();
        dc_b.send_message(&msg_b).unwrap();

        let recv_b =
            dc_b.receive_message(Duration::from_secs(1)).unwrap().unwrap();
        let recv_a =
            dc_a.receive_message(Duration::from_secs(1)).unwrap().unwrap();

        match recv_b {
            ProtocolMessage::Heartbeat { timestamp_ms, .. } => {
                assert_eq!(timestamp_ms, 100)
            }
            other => panic!("unexpected: {:?}", other),
        }
        match recv_a {
            ProtocolMessage::Heartbeat { timestamp_ms, .. } => {
                assert_eq!(timestamp_ms, 200)
            }
            other => panic!("unexpected: {:?}", other),
        }
    }

    #[test]
    fn test_message_counting() {
        let pair = LocalChannelPair::new();
        let dc_a = DataChannel::new(Arc::new(pair.channel_a), "node_a".into());
        let dc_b = DataChannel::new(Arc::new(pair.channel_b), "node_b".into());

        assert_eq!(dc_a.messages_sent(), 0);
        assert_eq!(dc_a.messages_received(), 0);

        for i in 0..5 {
            dc_a.send_message(&ProtocolMessage::Heartbeat {
                master_vlsn: 0,
                timestamp_ms: i,
            })
            .unwrap();
        }
        assert_eq!(dc_a.messages_sent(), 5);

        for _ in 0..5 {
            dc_b.receive_message(Duration::from_secs(1)).unwrap();
        }
        assert_eq!(dc_b.messages_received(), 5);
    }

    #[test]
    fn test_receive_timeout() {
        let pair = LocalChannelPair::new();
        let dc_b = DataChannel::new(Arc::new(pair.channel_b), "node_b".into());

        let result = dc_b.receive_message(Duration::from_millis(50)).unwrap();
        assert!(result.is_none());
        assert_eq!(dc_b.messages_received(), 0);
    }

    #[test]
    fn test_node_name() {
        let pair = LocalChannelPair::new();
        let dc = DataChannel::new(Arc::new(pair.channel_a), "my_node".into());
        assert_eq!(dc.get_node_name(), "my_node");
    }

    #[test]
    fn test_close_and_is_open() {
        let pair = LocalChannelPair::new();
        let dc = DataChannel::new(Arc::new(pair.channel_a), "node".into());

        assert!(dc.is_open());
        dc.close().unwrap();
        assert!(!dc.is_open());
    }

    #[test]
    fn test_send_after_close_fails() {
        let pair = LocalChannelPair::new();
        let dc = DataChannel::new(Arc::new(pair.channel_a), "node".into());
        dc.close().unwrap();

        let result = dc.send_message(&ProtocolMessage::Heartbeat {
            master_vlsn: 0,
            timestamp_ms: 0,
        });
        assert!(result.is_err());
    }
}
