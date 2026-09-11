//! Replication stream subsystem.
//!
//! Rep.impl.node`. The feeder runs on the master side
//! and sends replication data to replicas. The replica stream runs on the
//! replica side and receives data from the master.

pub mod feeder;
pub mod live_txn_chain;
pub mod output_thread;
pub mod peer_feeder;
pub mod reconnect;
pub mod replica_stream;
pub mod syncup;
pub mod syncup_protocol;
pub mod syncup_reader;

pub use feeder::{
    EnvironmentLogScanner, Feeder, FeederRunner, FeederState, LogScanner,
};
pub use live_txn_chain::{
    build_live_chain, build_live_chain_unbounded, build_tail_chains,
};
pub use output_thread::OutputQueue;
pub use peer_feeder::{
    PeerFeederSource, PeerLogScanner, PeerScannerAdapter, SyncupResult,
    WalFeederSource, negotiate_syncup,
};
pub use reconnect::{ReconnectConfig, ReconnectOutcome, catch_up_with_retry};
pub use replica_stream::{
    EnvironmentLogWriter, LogWriter, ReplicaReceiver, ReplicaStream,
    ReplicaStreamState,
};
pub use syncup::{
    Matchpoint, RollbackDecision, SyncupView, TailSafety, VlsnEntry,
    classify_tail, find_matchpoint, verify_rollback,
};
pub use syncup_protocol::{
    RemoteFeederView, SYNCUP_SERVICE_NAME, SyncupMsg, SyncupOutcome,
    SyncupService, feeder_syncup_handshake, replica_syncup_handshake,
};
pub use syncup_reader::{SyncupLogView, VlsnIndexView};
