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
pub use data_channel::DataChannel;
#[cfg(feature = "quic")]
pub use quic_channel::{
    QuicChannel, QuicChannelListener, default_server_config, insecure_client_config,
};
#[cfg(feature = "quic")]
pub use quic_mux::{
    QuicMultiplexedChannel, QuicMultiplexedChannelListener, ReconnectToken, ReplicationChannel,
    mux_server_config, mux_insecure_client_config,
};
pub use service_dispatcher::{
    ServiceDispatcher, ServiceHandler, TcpServiceDispatcher,
    connect_to_service,
};
