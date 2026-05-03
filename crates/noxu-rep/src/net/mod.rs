//! Network transport layer for Noxu DB replication.
//!
//! Port of `com.sleepycat.je.rep.net`  -  provides abstract channel traits,
//! service dispatching, and typed message channels for replication
//! communication.

pub mod channel;
pub mod data_channel;
pub mod service_dispatcher;

pub use channel::{
    Channel, LocalChannel, LocalChannelPair, TcpChannel, TcpChannelListener,
};
pub use data_channel::DataChannel;
pub use service_dispatcher::{
    ServiceDispatcher, ServiceHandler, TcpServiceDispatcher,
    connect_to_service,
};
