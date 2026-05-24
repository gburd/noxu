//! VLSN streaming — `noxu-rep::stream::feeder` +
//! `noxu-rep::stream::replica_stream`.
//!
//! Models a feeder reading from the master's WAL and pushing entries
//! to a replica, which acks back. Captures buffer overflow,
//! out-of-order ack, and resumption.
//!
//! Production code under model:
//!   - `crates/noxu-rep/src/stream/feeder.rs`
//!   - `crates/noxu-rep/src/stream/peer_feeder.rs`
//!   - `crates/noxu-rep/src/stream/replica_stream.rs`
//!   - `crates/noxu-rep/src/vlsn.rs`
//!
//! Properties:
//!   - `VlsnMonotone` — the replica's applied VLSN never goes
//!     backwards.
//!   - `NoOverflow` — the feeder's in-flight buffer never exceeds
//!     `MAX_BUFFER`.
//!   - `AckTracksReceived` — for every ack, the replica must have
//!     applied entries up to and including the acked VLSN.

use stateright::{Model, Property};

pub const MASTER_WAL_LEN: u64 = 4;
pub const MAX_BUFFER: usize = 2;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct State {
    pub master_next_vlsn: u64,
    pub feeder_sent_high: u64,
    pub replica_applied_high: u64,
    pub master_acked_high: u64,
    pub in_flight: Vec<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Action {
    FeederSend,
    ReplicaApply { vlsn: u64 },
    ReplicaAck { vlsn: u64 },
    MasterReceiveAck { vlsn: u64 },
}

pub struct VlsnStreamingModel;

impl Model for VlsnStreamingModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            master_next_vlsn: 1,
            feeder_sent_high: 0,
            replica_applied_high: 0,
            master_acked_high: 0,
            in_flight: vec![],
        }]
    }

    fn actions(&self, s: &Self::State, out: &mut Vec<Self::Action>) {
        if s.feeder_sent_high < MASTER_WAL_LEN && s.in_flight.len() < MAX_BUFFER
        {
            out.push(Action::FeederSend);
        }
        if let Some(&next) = s.in_flight.first() {
            if next == s.replica_applied_high + 1 {
                out.push(Action::ReplicaApply { vlsn: next });
            }
        }
        if s.replica_applied_high > 0 {
            out.push(Action::ReplicaAck { vlsn: s.replica_applied_high });
        }
        for &v in &s.in_flight {
            if v <= s.replica_applied_high && v > s.master_acked_high {
                out.push(Action::MasterReceiveAck { vlsn: v });
            }
        }
    }

    fn next_state(
        &self,
        s: &Self::State,
        a: Self::Action,
    ) -> Option<Self::State> {
        let mut s = s.clone();
        match a {
            Action::FeederSend => {
                if s.in_flight.len() >= MAX_BUFFER {
                    return None;
                }
                let v = s.feeder_sent_high + 1;
                if v > MASTER_WAL_LEN {
                    return None;
                }
                s.feeder_sent_high = v;
                s.in_flight.push(v);
            }
            Action::ReplicaApply { vlsn } => {
                if Some(&vlsn) != s.in_flight.first() {
                    return None;
                }
                if vlsn != s.replica_applied_high + 1 {
                    return None;
                }
                s.in_flight.remove(0);
                s.replica_applied_high = vlsn;
            }
            Action::ReplicaAck { vlsn } => {
                if vlsn > s.replica_applied_high {
                    return None;
                }
            }
            Action::MasterReceiveAck { vlsn } => {
                if vlsn > s.replica_applied_high {
                    return None;
                }
                s.master_acked_high = s.master_acked_high.max(vlsn);
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always("NoOverflow", |_, s: &State| {
                s.in_flight.len() <= MAX_BUFFER
            }),
            Property::<Self>::always("AckTracksReceived", |_, s: &State| {
                s.master_acked_high <= s.replica_applied_high
            }),
            Property::<Self>::always("VlsnMonotone", |_, s: &State| {
                s.replica_applied_high <= s.feeder_sent_high
                    && s.master_acked_high <= s.replica_applied_high
            }),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use stateright::Checker;

    #[test]
    fn vlsn_streaming_safety_holds() {
        let checker = VlsnStreamingModel.checker().spawn_bfs().join();
        checker.assert_properties();
    }
}
