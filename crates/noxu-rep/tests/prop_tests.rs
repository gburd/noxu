//! Property-based tests for noxu-rep using proptest.

use noxu_rep::elections::Proposal;
use noxu_rep::vlsn::VlsnRange;
use proptest::prelude::*;

proptest! {
    // 1. Proposal ordering: proposals with higher VLSN always win
    //    (regardless of other fields).
    #[test]
    fn prop_higher_vlsn_wins(
        vlsn_a in 0u64..u64::MAX,
        vlsn_b in 0u64..u64::MAX,
        prio_a: u32,
        prio_b: u32,
        term_a: u64,
        term_b: u64,
        name_a in "[a-z]{1,8}",
        name_b in "[a-z]{1,8}",
    ) {
        prop_assume!(vlsn_a != vlsn_b);

        let pa = Proposal::with_timestamp(name_a, vlsn_a, prio_a, term_a, 0);
        let pb = Proposal::with_timestamp(name_b, vlsn_b, prio_b, term_b, 0);

        if vlsn_a > vlsn_b {
            prop_assert!(pa.is_better_than(&pb));
        } else {
            prop_assert!(pb.is_better_than(&pa));
        }
    }

    // Additional: Proposal ordering is total and antisymmetric.
    #[test]
    fn prop_proposal_ordering_antisymmetric(
        vlsn_a: u64,
        vlsn_b: u64,
        prio_a: u32,
        prio_b: u32,
        term_a: u64,
        term_b: u64,
        name_a in "[a-z]{1,8}",
        name_b in "[a-z]{1,8}",
    ) {
        let pa = Proposal::with_timestamp(name_a, vlsn_a, prio_a, term_a, 0);
        let pb = Proposal::with_timestamp(name_b, vlsn_b, prio_b, term_b, 0);

        // At most one can be "better" (antisymmetric), or they are equal.
        let a_better = pa.is_better_than(&pb);
        let b_better = pb.is_better_than(&pa);
        prop_assert!(!(a_better && b_better), "Both cannot be better than each other");
    }

    // 2. VlsnRange: first <= last always holds after extend operations.
    #[test]
    fn prop_vlsn_range_first_le_last(
        vlsns in prop::collection::vec(1u64..10000u64, 1..50),
    ) {
        let mut range = VlsnRange::new();
        for v in &vlsns {
            range.extend(*v);
        }

        // After extending, the range should not be empty.
        prop_assert!(!range.is_empty());
        // first <= last must always hold.
        prop_assert!(range.first() <= range.last());
        // first should be the min of all extended values.
        let expected_first = *vlsns.iter().min().unwrap();
        let expected_last = *vlsns.iter().max().unwrap();
        prop_assert_eq!(range.first(), expected_first);
        prop_assert_eq!(range.last(), expected_last);
    }

    // Additional: VlsnRange with_range always satisfies first <= last.
    #[test]
    fn prop_vlsn_range_with_range_valid(first in 1u64..10000u64, delta in 0u64..10000u64) {
        let last = first.saturating_add(delta);
        let range = VlsnRange::with_range(first, last);
        prop_assert!(range.first() <= range.last());
        prop_assert_eq!(range.len(), last - first + 1);
    }

    // Additional: VlsnRange contains is correct after extend.
    #[test]
    fn prop_vlsn_range_contains(
        vlsns in prop::collection::vec(1u64..10000u64, 1..20),
    ) {
        let mut range = VlsnRange::new();
        for v in &vlsns {
            range.extend(*v);
        }

        // Every extended value should be contained.
        for v in &vlsns {
            prop_assert!(range.contains(*v));
        }
    }

    // Additional: VlsnRange merge preserves first <= last.
    #[test]
    fn prop_vlsn_range_merge_valid(
        first_a in 1u64..5000u64,
        delta_a in 0u64..5000u64,
        first_b in 1u64..5000u64,
        delta_b in 0u64..5000u64,
    ) {
        let last_a = first_a.saturating_add(delta_a);
        let last_b = first_b.saturating_add(delta_b);
        let mut range_a = VlsnRange::with_range(first_a, last_a);
        let range_b = VlsnRange::with_range(first_b, last_b);

        range_a.merge(&range_b);

        prop_assert!(range_a.first() <= range_a.last());
        prop_assert!(range_a.first() <= first_a.min(first_b));
        prop_assert!(range_a.last() >= last_a.max(last_b));
    }
}

// ============================================================================
// Wave 11-E: Paxos acceptor invariants.
//
// Properties ported from the Stateright spec to the production code path.
// The PersistentAcceptorState is the F5/F31 closer: it persists the highest
// promised term so that an acceptor that restarts cannot "unmake" a
// previously-made promise.
// ============================================================================

use noxu_rep::elections::PersistentAcceptorState;

#[derive(Debug, Clone)]
enum AcceptorMsg {
    /// Proposer asks the acceptor to promise term `t`.
    Promise(u64),
    /// Proposer asks the acceptor to accept (term `t`, master `name`).
    Accept(u64, u8),
}

fn acceptor_msg_strategy(
    max_term: u64,
) -> impl proptest::strategy::Strategy<Value = AcceptorMsg> {
    prop_oneof![
        (1u64..=max_term).prop_map(AcceptorMsg::Promise),
        (1u64..=max_term, any::<u8>())
            .prop_map(|(t, n)| AcceptorMsg::Accept(t, n)),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Paxos safety: promised_term is monotonically non-decreasing.  No
    /// matter what arbitrary message arrival order is fed to the acceptor,
    /// `promised_term()` only ever grows.
    #[test]
    fn prop_acceptor_promised_term_monotone(
        msgs in prop::collection::vec(acceptor_msg_strategy(100), 0..32),
    ) {
        let acceptor = PersistentAcceptorState::in_memory();
        let mut prev = acceptor.promised_term();
        for msg in &msgs {
            match msg {
                AcceptorMsg::Promise(t) => { let _ = acceptor.try_promise(*t); }
                AcceptorMsg::Accept(t, n) => {
                    let name = format!("n{}", n);
                    let _ = acceptor.try_accept(*t, &name);
                }
            }
            let cur = acceptor.promised_term();
            prop_assert!(cur >= prev,
                "promised_term went backwards {} -> {} on msg {:?}",
                prev, cur, msg);
            prev = cur;
        }
    }

    /// Paxos safety: a `try_promise(t)` returning `true` ALWAYS leaves the
    /// promised_term equal to max(prev, t).  A return of `false` MUST mean
    /// `t < prev_promised_term`.  Catches accidental write-on-reject bugs.
    #[test]
    fn prop_acceptor_promise_contract(
        msgs in prop::collection::vec(acceptor_msg_strategy(50), 0..16),
        probe_term in 1u64..=200,
    ) {
        let acceptor = PersistentAcceptorState::in_memory();
        for msg in &msgs {
            match msg {
                AcceptorMsg::Promise(t) => { let _ = acceptor.try_promise(*t); }
                AcceptorMsg::Accept(t, n) => {
                    let name = format!("n{}", n);
                    let _ = acceptor.try_accept(*t, &name);
                }
            }
        }
        let before = acceptor.promised_term();
        let result = acceptor.try_promise(probe_term);
        let after = acceptor.promised_term();
        if result {
            prop_assert!(probe_term >= before,
                "try_promise({}) returned true but promised_term was {}",
                probe_term, before);
            prop_assert_eq!(after, probe_term.max(before));
        } else {
            prop_assert!(probe_term < before,
                "try_promise({}) returned false but promised_term was {}",
                probe_term, before);
            prop_assert_eq!(after, before, "rejected promise must not mutate state");
        }
    }

    /// Paxos safety: `try_accept(t, m)` returning `true` implies
    /// `t >= prev_promised_term`.  A successful accept also implicitly bumps
    /// promised_term to t.  Returning `false` MUST leave (accepted_term,
    /// accepted_master) unchanged.
    #[test]
    fn prop_acceptor_accept_contract(
        msgs in prop::collection::vec(acceptor_msg_strategy(50), 0..16),
        probe_term in 1u64..=200,
        probe_master_byte: u8,
    ) {
        let acceptor = PersistentAcceptorState::in_memory();
        for msg in &msgs {
            match msg {
                AcceptorMsg::Promise(t) => { let _ = acceptor.try_promise(*t); }
                AcceptorMsg::Accept(t, n) => {
                    let name = format!("n{}", n);
                    let _ = acceptor.try_accept(*t, &name);
                }
            }
        }
        let before_promised = acceptor.promised_term();
        let before_accepted = acceptor.accepted_term();
        let before_master = acceptor.accepted_master();
        let probe_master = format!("m{}", probe_master_byte);

        let result = acceptor.try_accept(probe_term, &probe_master);
        if result {
            prop_assert!(probe_term >= before_promised);
            prop_assert_eq!(acceptor.accepted_term(), probe_term);
            prop_assert_eq!(acceptor.accepted_master(), Some(probe_master));
            prop_assert_eq!(acceptor.promised_term(), probe_term);
        } else {
            prop_assert!(probe_term < before_promised);
            prop_assert_eq!(acceptor.accepted_term(), before_accepted);
            prop_assert_eq!(acceptor.accepted_master(), before_master);
        }
    }

    /// F5/F31 invariant: an acceptor that is "restarted" (loaded from disk)
    /// reconstructs the same (promised, accepted) state it had before.  This
    /// is the property that prevents split-brain on master restart.
    ///
    /// Uses a temp dir so the persistent path is exercised end-to-end.
    #[test]
    fn prop_acceptor_persistence_restart_preserves_promise(
        msgs in prop::collection::vec(acceptor_msg_strategy(50), 1..16),
    ) {
        let dir = tempfile::TempDir::new().unwrap();
        let (final_promised, final_accepted, final_master);
        {
            let acceptor = PersistentAcceptorState::load_or_default(dir.path());
            for msg in &msgs {
                match msg {
                    AcceptorMsg::Promise(t) => { let _ = acceptor.try_promise(*t); }
                    AcceptorMsg::Accept(t, n) => {
                        let name = format!("n{}", n);
                        let _ = acceptor.try_accept(*t, &name);
                    }
                }
            }
            final_promised = acceptor.promised_term();
            final_accepted = acceptor.accepted_term();
            final_master = acceptor.accepted_master();
        }
        // Simulate a restart by reloading.
        let acceptor2 = PersistentAcceptorState::load_or_default(dir.path());
        prop_assert_eq!(acceptor2.promised_term(), final_promised);
        prop_assert_eq!(acceptor2.accepted_term(), final_accepted);
        prop_assert_eq!(acceptor2.accepted_master(), final_master);
        // After restart, no smaller term can promise.
        if final_promised > 0 {
            prop_assert!(!acceptor2.try_promise(final_promised - 1),
                "restarted acceptor must reject promise below {}", final_promised);
        }
    }
}

// ============================================================================
// Wave 11-E: VLSN streaming invariants.
//
// Models a feeder writing VLSNs interleaved with replica reads.  The replica's
// observed VLSN must be monotonic AND must never exceed the master's range.
// ============================================================================

use noxu_rep::vlsn::VlsnIndex;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]

    /// VlsnIndex.get_latest_vlsn is monotonic across any sequence of put()
    /// calls, regardless of the order VLSNs arrive (in-order or out-of-order
    /// with respect to global VLSN order).
    #[test]
    fn prop_vlsn_index_latest_is_max(
        vlsns in prop::collection::vec(1u64..1_000u64, 1..50),
    ) {
        let idx = VlsnIndex::new(8);
        let mut max_seen = 0u64;
        for v in &vlsns {
            idx.put(*v, 1, *v as u32);
            max_seen = max_seen.max(*v);
            prop_assert_eq!(idx.get_latest_vlsn(), max_seen);
        }
    }

    /// VlsnIndex.get_lsn returns the exact (file, offset) the caller passed
    /// to put() for every registered VLSN — PROVIDED the index is constructed
    /// with stride=1 (every VLSN is a stride boundary).  This exercises the
    /// happy-path replica-read invariant.
    ///
    /// Note: VlsnBucket uses `(0, 0)` as the NO_OFFSET sentinel, so the
    /// generator avoids that value.  A real (0, 0) LSN would collide with
    /// an unpopulated stride slot; this is documented sentinel-collision
    /// behaviour.
    #[test]
    fn prop_vlsn_index_get_lsn_returns_what_was_put(
        entries in prop::collection::vec(
            (1u64..1_000_000u64, 1u32..16, 1u32..1_000_000u32),
            1..30,
        ),
    ) {
        let idx = VlsnIndex::new(1); // stride=1 → every VLSN is exact
        let mut last: std::collections::BTreeMap<u64, (u32, u32)> =
            Default::default();
        for (v, f, o) in &entries {
            idx.put(*v, *f, *o);
            last.insert(*v, (*f, *o));
        }
        for (v, (f, o)) in &last {
            prop_assert_eq!(idx.get_lsn(*v), Some((*f, *o)),
                "vlsn {} lookup mismatch", v);
        }
    }

    /// Replica must never observe a VLSN range whose `last` exceeds the
    /// master's `last`.  Models the feeder advancing while replica reads.
    /// After every interleaving step, replica.last <= master.last.
    #[test]
    fn prop_vlsn_replica_last_never_exceeds_master(
        steps in prop::collection::vec(
            (any::<bool>(), 1u64..1_000u64),
            1..50,
        ),
    ) {
        let master = VlsnIndex::new(8);
        let replica = VlsnIndex::new(8);
        for (is_feeder, v) in &steps {
            if *is_feeder {
                master.put(*v, 1, *v as u32);
            } else {
                // Replica only reads VLSNs the master has already written.
                if let Some((f, o)) = master.get_lsn(*v) {
                    replica.put(*v, f, o);
                }
            }
            prop_assert!(
                replica.get_latest_vlsn() <= master.get_latest_vlsn(),
                "replica latest {} > master latest {}",
                replica.get_latest_vlsn(), master.get_latest_vlsn(),
            );
        }
    }
}
