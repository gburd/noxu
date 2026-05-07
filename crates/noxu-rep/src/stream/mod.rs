//! Replication stream subsystem.
//!
//! Port of feeder and replica stream components from
//! `com.sleepycat.je.rep.impl.node`. The feeder runs on the master side
//! and sends replication data to replicas. The replica stream runs on the
//! replica side and receives data from the master.

pub mod feeder;
pub mod output_thread;
pub mod replica_stream;

pub use feeder::{EnvironmentLogScanner, Feeder, FeederRunner, FeederState, LogScanner};
pub use output_thread::OutputQueue;
pub use replica_stream::{
    EnvironmentLogWriter, LogWriter, ReplicaReceiver, ReplicaStream,
    ReplicaStreamState,
};
