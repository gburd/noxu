//! Property-based tests for noxu-rep (Hegel / hegeltest).

use hegel::generators::{self, Generator};
use noxu_rep::elections::Proposal;
use noxu_rep::vlsn::VlsnRange;

// 1. Proposal ordering: proposals with higher VLSN always win
//    (regardless of other fields).
#[hegel::test]
fn prop_higher_vlsn_wins(tc: hegel::TestCase) {
    let vlsn_a = tc.draw(generators::integers::<u64>().max_value(u64::MAX - 1));
    let vlsn_b = tc.draw(generators::integers::<u64>().max_value(u64::MAX - 1));
    let prio_a = tc.draw(generators::integers::<u32>());
    let prio_b = tc.draw(generators::integers::<u32>());
    let term_a = tc.draw(generators::integers::<u64>());
    let term_b = tc.draw(generators::integers::<u64>());
    let name_a = tc.draw(generators::from_regex(r"[a-z]{1,8}").fullmatch(true));
    let name_b = tc.draw(generators::from_regex(r"[a-z]{1,8}").fullmatch(true));

    tc.assume(vlsn_a != vlsn_b);

    let pa = Proposal::with_timestamp(name_a, vlsn_a, prio_a, term_a, 0);
    let pb = Proposal::with_timestamp(name_b, vlsn_b, prio_b, term_b, 0);

    if vlsn_a > vlsn_b {
        assert!(pa.is_better_than(&pb));
    } else {
        assert!(pb.is_better_than(&pa));
    }
}

// Additional: Proposal ordering is total and antisymmetric.
#[hegel::test]
fn prop_proposal_ordering_antisymmetric(tc: hegel::TestCase) {
    let vlsn_a = tc.draw(generators::integers::<u64>());
    let vlsn_b = tc.draw(generators::integers::<u64>());
    let prio_a = tc.draw(generators::integers::<u32>());
    let prio_b = tc.draw(generators::integers::<u32>());
    let term_a = tc.draw(generators::integers::<u64>());
    let term_b = tc.draw(generators::integers::<u64>());
    let name_a = tc.draw(generators::from_regex(r"[a-z]{1,8}").fullmatch(true));
    let name_b = tc.draw(generators::from_regex(r"[a-z]{1,8}").fullmatch(true));

    let pa = Proposal::with_timestamp(name_a, vlsn_a, prio_a, term_a, 0);
    let pb = Proposal::with_timestamp(name_b, vlsn_b, prio_b, term_b, 0);

    // At most one can be "better" (antisymmetric), or they are equal.
    let a_better = pa.is_better_than(&pb);
    let b_better = pb.is_better_than(&pa);
    assert!(!(a_better && b_better), "Both cannot be better than each other");
}

// 2. VlsnRange: first <= last always holds after extend operations.
#[hegel::test]
fn prop_vlsn_range_first_le_last(tc: hegel::TestCase) {
    let vlsns: Vec<u64> = tc.draw(
        generators::vecs(
            generators::integers::<u64>().min_value(1).max_value(9999),
        )
        .min_size(1)
        .max_size(49),
    );
    let mut range = VlsnRange::new();
    for v in &vlsns {
        range.extend(*v);
    }

    // After extending, the range should not be empty.
    assert!(!range.is_empty());
    // first <= last must always hold.
    assert!(range.first() <= range.last());
    // first should be the min of all extended values.
    let expected_first = *vlsns.iter().min().unwrap();
    let expected_last = *vlsns.iter().max().unwrap();
    assert_eq!(range.first(), expected_first);
    assert_eq!(range.last(), expected_last);
}

// Additional: VlsnRange with_range always satisfies first <= last.
#[hegel::test]
fn prop_vlsn_range_with_range_valid(tc: hegel::TestCase) {
    let first =
        tc.draw(generators::integers::<u64>().min_value(1).max_value(9999));
    let delta = tc.draw(generators::integers::<u64>().max_value(9999));
    let last = first.saturating_add(delta);
    let range = VlsnRange::with_range(first, last);
    assert!(range.first() <= range.last());
    assert_eq!(range.len(), last - first + 1);
}

// Additional: VlsnRange contains is correct after extend.
#[hegel::test]
fn prop_vlsn_range_contains(tc: hegel::TestCase) {
    let vlsns: Vec<u64> = tc.draw(
        generators::vecs(
            generators::integers::<u64>().min_value(1).max_value(9999),
        )
        .min_size(1)
        .max_size(19),
    );
    let mut range = VlsnRange::new();
    for v in &vlsns {
        range.extend(*v);
    }

    // Every extended value should be contained.
    for v in &vlsns {
        assert!(range.contains(*v));
    }
}

// Additional: VlsnRange merge preserves first <= last.
#[hegel::test]
fn prop_vlsn_range_merge_valid(tc: hegel::TestCase) {
    let first_a =
        tc.draw(generators::integers::<u64>().min_value(1).max_value(4999));
    let delta_a = tc.draw(generators::integers::<u64>().max_value(4999));
    let first_b =
        tc.draw(generators::integers::<u64>().min_value(1).max_value(4999));
    let delta_b = tc.draw(generators::integers::<u64>().max_value(4999));
    let last_a = first_a.saturating_add(delta_a);
    let last_b = first_b.saturating_add(delta_b);
    let mut range_a = VlsnRange::with_range(first_a, last_a);
    let range_b = VlsnRange::with_range(first_b, last_b);

    range_a.merge(&range_b);

    assert!(range_a.first() <= range_a.last());
    assert!(range_a.first() <= first_a.min(first_b));
    assert!(range_a.last() >= last_a.max(last_b));
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
    /// Proposer `p` asks the acceptor to promise proposal `(t, p)`.
    Promise(u64, u8),
    /// Proposer `p` asks the acceptor to accept `(term t, proposer p,
    /// master `name`)`.
    Accept(u64, u8, u8),
}

/// Model-side proposal number `(term, proposer-name)`, mirroring the
/// acceptor's `(term, proposer)` key.  Proposer bytes become `"p{n}"`.
fn proposer_name(p: u8) -> String {
    format!("p{}", p)
}

#[hegel::composite]
fn acceptor_msg(tc: hegel::TestCase, max_term: u64) -> AcceptorMsg {
    tc.draw(hegel::one_of!(
        generators::tuples!(
            generators::integers::<u64>().min_value(1).max_value(max_term),
            generators::integers::<u8>(),
        )
        .map(|(t, p)| AcceptorMsg::Promise(t, p)),
        generators::tuples!(
            generators::integers::<u64>().min_value(1).max_value(max_term),
            generators::integers::<u8>(),
            generators::integers::<u8>(),
        )
        .map(|(t, p, n)| AcceptorMsg::Accept(t, p, n)),
    ))
}

/// Paxos safety: promised_term is monotonically non-decreasing.  No
/// matter what arbitrary message arrival order is fed to the acceptor,
/// `promised_term()` only ever grows.
#[hegel::test(test_cases = 64)]
fn prop_acceptor_promised_term_monotone(tc: hegel::TestCase) {
    let msgs: Vec<AcceptorMsg> =
        tc.draw(generators::vecs(acceptor_msg(100)).max_size(31));
    let acceptor = PersistentAcceptorState::in_memory();
    let mut prev = acceptor.promised_term();
    for msg in &msgs {
        match msg {
            AcceptorMsg::Promise(t, p) => {
                let _ = acceptor.try_promise_proposal(*t, &proposer_name(*p));
            }
            AcceptorMsg::Accept(t, p, n) => {
                let _ = acceptor.try_accept_proposal(
                    *t,
                    &proposer_name(*p),
                    &format!("m{}", n),
                );
            }
        }
        let cur = acceptor.promised_term();
        assert!(
            cur >= prev,
            "promised_term went backwards {} -> {} on msg {:?}",
            prev,
            cur,
            msg
        );
        prev = cur;
    }
}

/// Paxos safety (FID-ELECT-1): the acceptor keys on the whole proposal
/// number `(term, proposer)`.  A `try_promise_proposal((t, p))` returns `true` IFF the
/// current promise is NOT strictly greater than `(t, p)` (JE: reject only a
/// strictly-greater promise); a `false` return leaves state unchanged.
#[hegel::test(test_cases = 64)]
fn prop_acceptor_promise_contract(tc: hegel::TestCase) {
    let msgs: Vec<AcceptorMsg> =
        tc.draw(generators::vecs(acceptor_msg(50)).max_size(15));
    let probe_term =
        tc.draw(generators::integers::<u64>().min_value(1).max_value(200));
    let probe_proposer_byte = tc.draw(generators::integers::<u8>());
    let acceptor = PersistentAcceptorState::in_memory();
    for msg in &msgs {
        match msg {
            AcceptorMsg::Promise(t, p) => {
                let _ = acceptor.try_promise_proposal(*t, &proposer_name(*p));
            }
            AcceptorMsg::Accept(t, p, n) => {
                let _ = acceptor.try_accept_proposal(
                    *t,
                    &proposer_name(*p),
                    &format!("m{}", n),
                );
            }
        }
    }
    let before_term = acceptor.promised_term();
    let before_proposer = acceptor.promised_proposer();
    let probe_proposer = proposer_name(probe_proposer_byte);
    // The acceptor's `(term, proposer)` ordering: `None` proposer < any name.
    let promised = (before_term, before_proposer.clone());
    let incoming = (probe_term, Some(probe_proposer.clone()));

    let result = acceptor.try_promise_proposal(probe_term, &probe_proposer);
    let after_term = acceptor.promised_term();
    let after_proposer = acceptor.promised_proposer();
    if result {
        assert!(
            promised <= incoming,
            "try_promise({:?}) returned true but promised was {:?}",
            incoming,
            promised
        );
        assert_eq!(after_term, probe_term);
        assert_eq!(after_proposer.as_deref(), Some(probe_proposer.as_str()));
    } else {
        assert!(
            promised > incoming,
            "try_promise({:?}) returned false but promised was {:?}",
            incoming,
            promised
        );
        assert_eq!(after_term, before_term, "rejected promise must not mutate");
        assert_eq!(after_proposer, before_proposer);
    }
}

/// Paxos safety (JE `Acceptor.process(Accept)`, `Acceptor.java:207-222`),
/// FID-ELECT-1 — the EXACT accept contract.
///
/// `try_accept_proposal((t, p), m)` returns `true` IFF **both**:
///
///   1. the current promise EXACTLY equals the Accept's proposal number
///      `(t, p)` — the proposer identity is part of the key, so an Accept at
///      the right term but the WRONG proposer is rejected (JE
///      `promisedProposal.compareTo(accept.getProposal()) == 0`,
///      `Acceptor.java:210`); **and**
///   2. the Noxu value guard passes — there is no value yet accepted at that
///      EXACT term, OR the probe's value equals the value already accepted
///      there (`acceptor_state.rs`: reject iff
///      `accepted_term == t && accepted_master != Some(master)`).
///      This guard is a Noxu-specific backstop with NO JE counterpart (JE
///      overwrites `acceptedValue` unconditionally once the proposal matches,
///      `Acceptor.java:217`); it is load-bearing because Noxu's downstream
///      master bookkeeping keys on the bare `term` integer, not the full
///      `(term, proposer)` number.
///
/// A re-accept of the SAME value at an already-accepted `(t, p)` passes the
/// guard and succeeds (idempotent re-set), matching JE's unconditional
/// `acceptedValue = accept.getValue()`.
///
/// The failure branch holds exactly when that iff is false — i.e. the promise
/// is not equal, OR the value guard would fire.  On `false` the acceptor MUST
/// leave `(accepted_term, accepted_master)` AND the promise unchanged.
#[hegel::test(test_cases = 64)]
fn prop_acceptor_accept_contract(tc: hegel::TestCase) {
    let msgs: Vec<AcceptorMsg> =
        tc.draw(generators::vecs(acceptor_msg(50)).max_size(15));
    let probe_term =
        tc.draw(generators::integers::<u64>().min_value(1).max_value(200));
    let probe_proposer_byte = tc.draw(generators::integers::<u8>());
    let probe_master_byte = tc.draw(generators::integers::<u8>());
    let acceptor = PersistentAcceptorState::in_memory();
    for msg in &msgs {
        match msg {
            AcceptorMsg::Promise(t, p) => {
                let _ = acceptor.try_promise_proposal(*t, &proposer_name(*p));
            }
            AcceptorMsg::Accept(t, p, n) => {
                let _ = acceptor.try_accept_proposal(
                    *t,
                    &proposer_name(*p),
                    &format!("m{}", n),
                );
            }
        }
    }
    let before_promised = acceptor.promised_term();
    let before_proposer = acceptor.promised_proposer();
    let before_accepted = acceptor.accepted_term();
    let before_master = acceptor.accepted_master();
    let probe_proposer = proposer_name(probe_proposer_byte);
    let probe_master = format!("m{}", probe_master_byte);

    let promised = (before_promised, before_proposer.as_deref());
    let probe = (probe_term, Some(probe_proposer.as_str()));

    // The EXACT contract, computed from the pre-state (independently of the
    // call we are about to make):
    //   (1) the promise equals the probe's proposal number, AND
    //   (2) the value guard passes.
    let promise_matches = promised == probe;
    // Value guard fires (rejects) iff a value is ALREADY accepted at this
    // exact term AND the probe carries a DIFFERENT value.  Note the guard is
    // keyed on the bare `term`, mirroring `try_accept_proposal`.
    let value_guard_fires = before_accepted == probe_term
        && before_master.as_deref() != Some(probe_master.as_str());
    let expected = promise_matches && !value_guard_fires;

    let result = acceptor.try_accept_proposal(
        probe_term,
        &probe_proposer,
        &probe_master,
    );

    assert_eq!(
        result, expected,
        "accept({probe:?}, {probe_master:?}) returned {result} but the exact \
         contract (promise_matches={promise_matches} && \
         !value_guard_fires={}) requires {expected}; pre-state promised={promised:?}, \
         accepted=({before_accepted}, {before_master:?})",
        !value_guard_fires,
    );

    if result {
        // Success implies the promise equalled the probe (necessary
        // condition) — assert it directly so a regression that let an accept
        // through on a non-matching promise is still caught here.
        assert_eq!(
            promised, probe,
            "accept succeeds only when the promise equals the Accept's \
             proposal number (term, proposer)"
        );
        // ...and the value guard did not fire.
        assert!(
            !value_guard_fires,
            "accept succeeded but a different value was already accepted at \
             this term (value guard should have rejected it)"
        );
        assert_eq!(acceptor.accepted_term(), probe_term);
        assert_eq!(acceptor.accepted_master(), Some(probe_master));
        // The promise is unchanged (already equals the probe).
        assert_eq!(acceptor.promised_term(), before_promised);
        assert_eq!(acceptor.promised_proposer(), before_proposer);
    } else {
        // Failure holds exactly when the promise did not match OR the value
        // guard fired.  The old assertion `assert_ne!(promised, probe)` was
        // unsound: with the value guard, `promised == probe` is reachable on a
        // rejected accept (promise (t,p); accept (t,p,m1) ok; accept (t,p,m2)
        // with m2 != m1 rejected while promised still == probe).
        assert!(
            !promise_matches || value_guard_fires,
            "accept was rejected but the promise matched AND the value guard \
             did not fire — the acceptor should have accepted; \
             promised={promised:?}, probe={probe:?}, accepted=({before_accepted}, \
             {before_master:?})"
        );
        assert_eq!(acceptor.accepted_term(), before_accepted);
        assert_eq!(acceptor.accepted_master(), before_master);
        assert_eq!(
            acceptor.promised_term(),
            before_promised,
            "rejected accept must not mutate the promise"
        );
        assert_eq!(acceptor.promised_proposer(), before_proposer);
    }
}

/// F5/F31 invariant: an acceptor that is "restarted" (loaded from disk)
/// reconstructs the same (promised, accepted) state it had before.  This
/// is the property that prevents split-brain on master restart.
///
/// Uses a temp dir so the persistent path is exercised end-to-end.
#[hegel::test(test_cases = 64)]
fn prop_acceptor_persistence_restart_preserves_promise(tc: hegel::TestCase) {
    let msgs: Vec<AcceptorMsg> =
        tc.draw(generators::vecs(acceptor_msg(50)).min_size(1).max_size(15));
    let dir = tempfile::TempDir::new().unwrap();
    let (final_promised, final_accepted, final_master);
    {
        let acceptor = PersistentAcceptorState::load_or_default(dir.path());
        for msg in &msgs {
            match msg {
                AcceptorMsg::Promise(t, p) => {
                    let _ =
                        acceptor.try_promise_proposal(*t, &proposer_name(*p));
                }
                AcceptorMsg::Accept(t, p, n) => {
                    let _ = acceptor.try_accept_proposal(
                        *t,
                        &proposer_name(*p),
                        &format!("m{}", n),
                    );
                }
            }
        }
        final_promised = acceptor.promised_term();
        final_accepted = acceptor.accepted_term();
        final_master = acceptor.accepted_master();
    }
    // Simulate a restart by reloading.
    let acceptor2 = PersistentAcceptorState::load_or_default(dir.path());
    assert_eq!(acceptor2.promised_term(), final_promised);
    assert_eq!(acceptor2.accepted_term(), final_accepted);
    assert_eq!(acceptor2.accepted_master(), final_master);
    // After restart, no smaller term can promise (regardless of proposer).
    if final_promised > 0 {
        assert!(
            !acceptor2.try_promise_proposal(final_promised - 1, "probe"),
            "restarted acceptor must reject promise below {}",
            final_promised
        );
    }
}

// ============================================================================
// Wave 11-E: VLSN streaming invariants.
//
// Models a feeder writing VLSNs interleaved with replica reads.  The replica's
// observed VLSN must be monotonic AND must never exceed the master's range.
// ============================================================================

use noxu_rep::vlsn::VlsnIndex;

/// VlsnIndex.get_latest_vlsn is monotonic across any sequence of put()
/// calls, regardless of the order VLSNs arrive (in-order or out-of-order
/// with respect to global VLSN order).
#[hegel::test(test_cases = 48)]
fn prop_vlsn_index_latest_is_max(tc: hegel::TestCase) {
    let vlsns: Vec<u64> = tc.draw(
        generators::vecs(
            generators::integers::<u64>().min_value(1).max_value(999),
        )
        .min_size(1)
        .max_size(49),
    );
    let idx = VlsnIndex::new(8);
    let mut max_seen = 0u64;
    for v in &vlsns {
        idx.put(*v, 1, *v as u32);
        max_seen = max_seen.max(*v);
        assert_eq!(idx.get_latest_vlsn(), max_seen);
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
#[hegel::test(test_cases = 48)]
fn prop_vlsn_index_get_lsn_returns_what_was_put(tc: hegel::TestCase) {
    let entries: Vec<(u64, u32, u32)> = tc.draw(
        generators::vecs(generators::tuples!(
            generators::integers::<u64>().min_value(1).max_value(999_999),
            generators::integers::<u32>().min_value(1).max_value(15),
            generators::integers::<u32>().min_value(1).max_value(999_999),
        ))
        .min_size(1)
        .max_size(29),
    );
    let idx = VlsnIndex::new(1); // stride=1 → every VLSN is exact
    let mut last: std::collections::BTreeMap<u64, (u32, u32)> =
        Default::default();
    for (v, f, o) in &entries {
        idx.put(*v, *f, *o);
        last.insert(*v, (*f, *o));
    }
    for (v, (f, o)) in &last {
        assert_eq!(
            idx.get_lsn(*v),
            Some((*f, *o)),
            "vlsn {} lookup mismatch",
            v
        );
    }
}

/// Replica must never observe a VLSN range whose `last` exceeds the
/// master's `last`.  Models the feeder advancing while replica reads.
/// After every interleaving step, replica.last <= master.last.
#[hegel::test(test_cases = 48)]
fn prop_vlsn_replica_last_never_exceeds_master(tc: hegel::TestCase) {
    let steps: Vec<(bool, u64)> = tc.draw(
        generators::vecs(generators::tuples!(
            generators::booleans(),
            generators::integers::<u64>().min_value(1).max_value(999),
        ))
        .min_size(1)
        .max_size(49),
    );
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
        assert!(
            replica.get_latest_vlsn() <= master.get_latest_vlsn(),
            "replica latest {} > master latest {}",
            replica.get_latest_vlsn(),
            master.get_latest_vlsn(),
        );
    }
}
