//! FID-ELECT-1 follow-up: the election VALUE ranking must not use `term`.
//!
//! JE's election value ranking is
//! `Acceptor.SuggestionGenerator.Ranking.compareTo` = `(major=dtvlsn,
//! minor=vlsn)` (`Acceptor.java:305-311`, value from
//! `MasterSuggestionGenerator.getRanking` = `new Ranking(dtvlsn, vlsn)`,
//! `MasterSuggestionGenerator.java:81`), after which `RankingProposer`
//! breaks ties by **priority** then by **socket address**
//! (`RankingProposer.java:105-128`, comment: "Use socket address to choose in
//! case of a tie, so we always have a consistent ordering").  There is **NO
//! term / proposal-number component** in JE's value ranking: the Paxos round a
//! candidate runs in must never change WHICH node is the best master.
//!
//! Noxu's `Proposal::cmp` carried an extra `term` tiebreak between priority
//! and node name.  When two candidates are tied on `(dtvlsn, vlsn, priority)`
//! but their proposals carry different terms — which happens routinely, since
//! the acceptor *suggestion* a node returns in its `Promise` reports that
//! node's acceptor-view `own_term` (lagging a live proposer's round term) —
//! each proposer's OWN candidacy (its live, higher term) out-ranked the
//! peer's suggestion (the peer's stale term).  So each proposer picked ITSELF
//! as the phase-2 value: a value-selection DUEL.
//!
//! These witnesses exercise `choose_phase2_value`
//! (JE `RankingProposer.choosePhase2Value`) directly, isolating the value
//! ranking from the acceptor's `(term, proposer)` promise keying.  Each is
//! RED on the base (term tiebreak present) and GREEN after the fix.

use noxu_rep::elections::Proposal;
use noxu_rep::elections::paxos::{
    Phase2Value, PromiseRecord, choose_phase2_value,
};

/// Build the set of promise records a proposer named `me` (running at live
/// `my_term`) sees: its own candidacy plus the peer's suggestion as reported
/// by the peer's acceptor at the peer's (stale) `peer_term`.  Both are tied on
/// `(dtvlsn=0, vlsn=100, priority=1)`.
fn two_tied_candidates(
    me: &str,
    my_term: u64,
    peer: &str,
    peer_term: u64,
) -> Vec<PromiseRecord> {
    vec![
        PromiseRecord {
            proposal: Proposal::with_timestamp(me.into(), 100, 1, my_term, 0),
            is_arbiter: false,
        },
        PromiseRecord {
            proposal: Proposal::with_timestamp(
                peer.into(),
                100,
                1,
                peer_term,
                0,
            ),
            is_arbiter: false,
        },
    ]
}

fn chosen(records: &[PromiseRecord]) -> String {
    match choose_phase2_value(records) {
        Phase2Value::Value(p) => p.node_name,
        other => panic!("expected a value, got {other:?}"),
    }
}

/// Two proposers tied on `(dtvlsn, vlsn, priority)` but running at different
/// terms MUST choose the SAME phase-2 value — the term must not bias which
/// node is the best master (JE's ranking has no term component).
///
/// RED on the base (term tiebreak): proposer p-A at the higher live term
/// picks p-A, proposer p-B at the higher live term picks p-B — a duel.
/// GREEN after the fix: both pick the higher node name (p-B) regardless of
/// term.
#[test]
fn fid_elect_1_tied_candidates_choose_same_value_regardless_of_term() {
    // p-A runs at live term 5; it sees p-B's suggestion at p-B's stale
    // acceptor term 0.  p-B runs at live term 5; it sees p-A's suggestion at
    // p-A's stale acceptor term 0.  (Both proposers at the same round term is
    // the case where both pass phase 1; the stale peer suggestion term is what
    // the removed tiebreak keyed on.)
    let a_view = two_tied_candidates("p-A", 5, "p-B", 0);
    let b_view = two_tied_candidates("p-B", 5, "p-A", 0);

    let a_choice = chosen(&a_view);
    let b_choice = chosen(&b_view);

    assert_eq!(
        a_choice, b_choice,
        "value-layer DUEL: p-A chose {a_choice}, p-B chose {b_choice}; the \
         term tiebreak made each proposer prefer itself"
    );
    // The deterministic winner is JE's final tiebreak — the stable per-group
    // node identity (name, standing in for JE's socket address): p-B > p-A.
    assert_eq!(a_choice, "p-B", "name must decide the tie (p-B > p-A)");
}

/// The dual asymmetry: whichever proposer carries the higher term must NOT win
/// on that basis.  Here p-A has the higher term but the LOWER name; the fix
/// makes the lower-term/higher-name node (p-B) win in BOTH views.
#[test]
fn fid_elect_1_higher_term_does_not_out_rank_higher_name() {
    // p-A term 9 (higher), p-B term 1 (lower).  Name: p-B > p-A.
    let a_view = two_tied_candidates("p-A", 9, "p-B", 1);
    let b_view = two_tied_candidates("p-B", 1, "p-A", 9);
    assert_eq!(chosen(&a_view), "p-B", "term must not beat the name tiebreak");
    assert_eq!(chosen(&b_view), "p-B", "both views agree on p-B");
}

/// Priority still outranks the name tiebreak (JE: priority before socket
/// address), and term never enters: a higher-priority, lower-term, lower-name
/// candidate still wins.
#[test]
fn fid_elect_1_priority_outranks_name_term_ignored() {
    // low: name p-Z (highest name), priority 1, term 9 (highest term).
    // high: name p-A (lowest name), priority 5, term 1 (lowest term).
    let records = vec![
        PromiseRecord {
            proposal: Proposal::with_timestamp("p-Z".into(), 100, 1, 9, 0),
            is_arbiter: false,
        },
        PromiseRecord {
            proposal: Proposal::with_timestamp("p-A".into(), 100, 5, 1, 0),
            is_arbiter: false,
        },
    ];
    assert_eq!(
        chosen(&records),
        "p-A",
        "higher priority wins; neither term nor name overrides it"
    );
}
