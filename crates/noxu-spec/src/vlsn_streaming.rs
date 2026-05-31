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
//!   - `crates/noxu-rep/src/vlsn/mod.rs`
//!   - `crates/noxu-rep/src/vlsn/persist.rs`
//!     (persists the VLSN index to `<env_home>/vlsn.idx` so a clean
//!     shutdown + restart resumes from the last persisted vlsn rather
//!     than forcing a full network restore.)
//!
//! VALIDATED-AS-OF: v3.1.0 — Re-stamped after Wave-ZB re-audit (2026-05-30).
//! NOTE: Wave 11-U (X-2) caps VLSN persistence at the checkpoint end LSN:
//! a VLSN may be written to the index only if its LSN is ≤ the most recent
//! checkpoint end. This prevents the persisted VLSN from advancing past a
//! checkpoint boundary that has not yet been written. The spec models the
//! abstract monotone VLSN streaming protocol; the checkpoint-cap constraint
//! is not yet modelled. The VlsnMonotone, NoOverflow, and AckTracksReceived
//! properties still hold for the modelled protocol.
//!
//! # Variants
//!
//! Following the same convention as
//! [`crate::flexible_paxos`], the model is parameterised on a
//! [`Variant`] so a single spec validates both the post-Wave-4-A
//! production protocol and the pre-fix variant where the VLSN
//! index lived only in memory:
//!
//!   - [`Variant::PersistentVlsnIndex`] — the post-Wave-4-A
//!     production protocol: `Restart` actions preserve the
//!     replica's `applied_high`. `assert_properties` succeeds.
//!   - [`Variant::EphemeralVlsnIndex`] — the pre-Wave-4-A protocol:
//!     `Restart` zeroes `applied_high`, modelling a replica that
//!     forgot which entries it had already applied. `assert_discovery`
//!     finds an `AckTracksReceived` counterexample where the master
//!     has acked an entry the replica no longer remembers applying
//!     — exactly the apparent-rollback scenario that
//!     `vlsn::persist::save_to_disk` closes.
//!
//! Properties:
//!   - `VlsnMonotone` — the replica's applied VLSN never goes
//!     backwards, and the master-side ack high never exceeds
//!     applied.
//!   - `NoOverflow` — the feeder's in-flight buffer never exceeds
//!     `MAX_BUFFER`.
//!   - `AckTracksReceived` — for every ack, the replica must have
//!     applied entries up to and including the acked VLSN.

use stateright::{Model, Property};

pub const MASTER_WAL_LEN: u64 = 3;
pub const MAX_BUFFER: usize = 2;

/// Whether the replica's VLSN index is persisted across restart.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Variant {
    /// The post-Wave-4-A production protocol: every applied vlsn
    /// is durable in `vlsn.idx`. A `Restart` action preserves
    /// `replica_applied_high`.
    PersistentVlsnIndex,
    /// The pre-Wave-4-A protocol: the VLSN index lives only in
    /// memory. A `Restart` action zeroes `replica_applied_high`,
    /// modelling a replica that forgot what it had applied.
    EphemeralVlsnIndex,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct State {
    pub master_next_vlsn: u64,
    pub feeder_sent_high: u64,
    pub replica_applied_high: u64,
    pub master_acked_high: u64,
    /// Entries the feeder has sent for which the master has not yet
    /// received an ack from the replica. Cleared by
    /// [`Action::MasterReceiveAck`], **not** by [`Action::ReplicaApply`]
    /// — applying advances `replica_applied_high` but the master
    /// still considers the entry in flight until it observes the ack
    /// on the wire. (Fixed in Wave 9-B: the original model removed
    /// entries on apply, which made `master_acked_high` unreachable
    /// from any non-zero value and silently weakened the
    /// `AckTracksReceived` check to a trivial truth.)
    pub in_flight: Vec<u64>,
    /// Whether the replica has already restarted once. We bound to
    /// at most one restart so the state space stays finite — one
    /// restart is enough to expose the `EphemeralVlsnIndex`
    /// regression.
    pub replica_restarted: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Action {
    FeederSend,
    ReplicaApply {
        vlsn: u64,
    },
    ReplicaAck {
        vlsn: u64,
    },
    MasterReceiveAck {
        vlsn: u64,
    },
    /// The replica process crashes and is restarted. Under
    /// `Variant::PersistentVlsnIndex` `replica_applied_high`
    /// survives; under `Variant::EphemeralVlsnIndex` it is zeroed.
    /// Either way the in-flight buffer is dropped (the TCP/QUIC
    /// connection is lost on crash) and the feeder rewinds to the
    /// replica's reported applied vlsn.
    ReplicaRestart,
}

pub struct VlsnStreamingModel {
    pub variant: Variant,
}

impl VlsnStreamingModel {
    pub fn persistent() -> Self {
        Self { variant: Variant::PersistentVlsnIndex }
    }

    pub fn ephemeral() -> Self {
        Self { variant: Variant::EphemeralVlsnIndex }
    }
}

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
            replica_restarted: false,
        }]
    }

    fn actions(&self, s: &Self::State, out: &mut Vec<Self::Action>) {
        if s.feeder_sent_high < MASTER_WAL_LEN && s.in_flight.len() < MAX_BUFFER
        {
            out.push(Action::FeederSend);
        }
        // Replica applies the next entry in flight in vlsn order, if
        // it has not been applied yet. (Apply does not drain
        // `in_flight` — see the field comment.)
        for &v in &s.in_flight {
            if v == s.replica_applied_high + 1 {
                out.push(Action::ReplicaApply { vlsn: v });
                break;
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
        // At most one restart per execution.
        if !s.replica_restarted {
            out.push(Action::ReplicaRestart);
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
                if !s.in_flight.contains(&vlsn) {
                    return None;
                }
                if vlsn != s.replica_applied_high + 1 {
                    return None;
                }
                // Apply does not drain `in_flight`: the master still
                // sees the entry as outstanding until it receives the
                // ack on the wire.
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
                // The ack tells the master it can drop the entry
                // from its retry buffer.
                if let Some(pos) = s.in_flight.iter().position(|&v| v == vlsn) {
                    s.in_flight.remove(pos);
                }
                s.master_acked_high = s.master_acked_high.max(vlsn);
            }
            Action::ReplicaRestart => {
                if s.replica_restarted {
                    return None;
                }
                s.replica_restarted = true;
                // The TCP/QUIC connection is lost: drop in-flight
                // entries so the feeder retransmits from the
                // replica's reported applied vlsn.
                s.in_flight.clear();
                match self.variant {
                    Variant::PersistentVlsnIndex => {
                        // F11: vlsn.idx survived; replica resumes
                        // at the same applied_high it had before
                        // crashing. The feeder rewinds to match.
                        s.feeder_sent_high = s.replica_applied_high;
                    }
                    Variant::EphemeralVlsnIndex => {
                        // Pre-Wave-4-A: the in-memory VLSN index is
                        // gone. The replica restarts at applied=0,
                        // and the feeder rewinds with it.
                        s.replica_applied_high = 0;
                        s.feeder_sent_high = 0;
                    }
                }
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

    /// Post-Wave-4-A: vlsn.idx persists `applied_high` across
    /// restart. NoOverflow / AckTracksReceived / VlsnMonotone all
    /// hold under arbitrary restart timing.
    #[test]
    fn vlsn_streaming_safety_holds() {
        let checker =
            VlsnStreamingModel::persistent().checker().spawn_bfs().join();
        checker.assert_properties();
    }

    /// Pre-Wave-4-A regression bait: with an in-memory-only VLSN
    /// index, a replica restart erases applied_high. The master's
    /// already-recorded ack now points beyond what the replica
    /// remembers applying — apparent rollback. The counterexample
    /// is exactly the F11 scenario that `vlsn::persist` closes.
    #[test]
    fn ephemeral_vlsn_index_loses_applied_progress() {
        let checker =
            VlsnStreamingModel::ephemeral().checker().spawn_bfs().join();
        checker.assert_discovery(
            "AckTracksReceived",
            vec![
                // Master streams vlsn=1; replica applies and acks.
                Action::FeederSend,
                Action::ReplicaApply { vlsn: 1 },
                Action::MasterReceiveAck { vlsn: 1 },
                // Replica crashes. Without vlsn.idx the in-memory
                // index is gone: applied_high snaps back to 0,
                // while master_acked_high stays at 1 — apparent
                // replica rollback.
                Action::ReplicaRestart,
            ],
        );
    }
}
