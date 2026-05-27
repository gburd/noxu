//! Network transport layer for Noxu DB replication.
//!
//! provides abstract channel traits,
//! service dispatching, and typed message channels for replication
//! communication.

pub mod channel;
pub mod data_channel;
#[cfg(feature = "quic")]
pub mod quic_channel;
#[cfg(feature = "quic")]
pub mod quic_mux;
pub mod service_dispatcher;

pub use channel::{
    Channel, LocalChannel, LocalChannelPair, TcpChannel, TcpChannelListener,
};
#[cfg(any(feature = "tls-rustls", feature = "tls-native"))]
pub use channel::{TlsTcpChannel, TlsTcpChannelListener};
pub use data_channel::DataChannel;
#[cfg(feature = "quic")]
pub use quic_channel::{
    QuicChannel, QuicChannelListener, default_server_config,
    insecure_client_config,
};
#[cfg(feature = "quic")]
pub use quic_mux::{
    QuicMultiplexedChannel, QuicMultiplexedChannelListener, ReconnectToken,
    ReplicationChannel, mux_insecure_client_config, mux_server_config,
};
pub use service_dispatcher::{
    MAX_SERVICE_NAME_LEN, ServiceDispatcher, ServiceHandler,
    TcpServiceDispatcher, connect_to_service,
};
