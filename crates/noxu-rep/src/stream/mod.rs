//! Replication stream subsystem.
//!
//! Rep.impl.node`. The feeder runs on the master side
//! and sends replication data to replicas. The replica stream runs on the
//! replica side and receives data from the master.

pub mod feeder;
pub mod output_thread;
pub mod peer_feeder;
pub mod reconnect;
pub mod replica_stream;

pub use feeder::{EnvironmentLogScanner, Feeder, FeederRunner, FeederState, LogScanner};
pub use output_thread::OutputQueue;
pub use peer_feeder::{
    PeerFeederRunner, PeerFeederSource, PeerLogScanner, PeerScannerAdapter, SyncupResult,
    negotiate_syncup,
};
pub use reconnect::{ReconnectConfig, ReconnectOutcome, catch_up_with_retry};
pub use replica_stream::{
    EnvironmentLogWriter, LogWriter, ReplicaReceiver, ReplicaStream,
    ReplicaStreamState,
};
