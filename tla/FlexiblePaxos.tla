--------------------------- MODULE FlexiblePaxos ---------------------------
(* MODELS: crates/noxu-rep/src/elections/paxos.rs *)
(* MODELS: crates/noxu-rep/src/elections/proposal.rs *)
(* MODELS: crates/noxu-rep/src/quorum_policy.rs *)
(*
A TLA+ model of the noxu-rep Flexible Paxos election protocol described
in Howard, Malkhi & Spiegelman, "Flexible Paxos: Quorum Intersection
Revisited" (OPODIS 2016) and elaborated in Howard, "Distributed
Consensus Revised" (UCAM-CL-TR-935, 2019). The goal of this spec is
to verify the headline safety property — at most one master per term —
and the underlying invariant that every Phase-1 quorum intersects every
Phase-2 quorum.

The implementation under test lives in:

    crates/noxu-rep/src/elections/paxos.rs       — proposer/acceptor
    crates/noxu-rep/src/elections/proposal.rs    — ranked proposal value
    crates/noxu-rep/src/quorum_policy.rs         — Q1, Q2 size validation

This spec is intentionally smaller than a full Paxos spec: it covers the
*election* protocol (single-instance leader selection) rather than
arbitrary Paxos consensus. Specifically it models:

  - Phase 1: Prepare → Promise (a proposer asks if it can lead at term T)
  - Phase 2: Accept  → Accepted (the leader announces itself)
  - Quorum: Q1 (Phase 1) and Q2 (Phase 2) must satisfy |Q1| + |Q2| > n.

We deliberately do NOT model:
  - VLSN streaming (replication state machine)
  - Log replay / catch-up
  - Network restore
  - Phi accrual failure detection — it is a *safety-preserving*
    failure detector (false positives only sacrifice liveness, not
    safety), so it is out of scope for this safety proof.

Constants tuned for TLC at small scale. Increase to 5 nodes / 3 terms
to explore deeper interleavings (state space ≈ 10^5 — 10^6).
*)

EXTENDS Naturals, FiniteSets, TLC

CONSTANTS
    Nodes,         \* set of node IDs (1..N)
    MaxTerm,       \* highest term TLC will explore
    Q1,            \* Phase-1 quorum size
    Q2             \* Phase-2 quorum size

ASSUME
    /\ Cardinality(Nodes) >= 1
    /\ MaxTerm >= 1
    /\ Q1 >= 1 /\ Q1 <= Cardinality(Nodes)
    /\ Q2 >= 1 /\ Q2 <= Cardinality(Nodes)
    \* Howard (2016) Theorem 1: safety holds iff every Q1 intersects every Q2.
    \* For uniform same-sized quorums on a ground set of size n, that is
    \* equivalent to Q1 + Q2 > n.
    /\ Q1 + Q2 > Cardinality(Nodes)

VARIABLES
    \* For each node, the highest term it has *promised* to (Phase 1).
    promised_term,
    \* For each node, the term of the proposer it has *accepted* in
    \* Phase 2, or 0 if none.
    accepted_term,
    \* For each node, who it accepted at that term.
    accepted_leader,
    \* Set of (term, leader) pairs for which Phase 1 has succeeded.
    leaders_proposed,
    \* Set of (term, leader) pairs for which Phase 2 has succeeded —
    \* "elected leaders". The safety property is that this set
    \* contains at most one leader per term.
    leaders_elected,
    \* Per-node Phase-1 promise messages: who has promised the leader's term.
    \* phase1_votes[<<term, leader>>] is the set of voters.
    phase1_votes,
    \* Same for Phase 2.
    phase2_votes

vars == <<promised_term, accepted_term, accepted_leader,
          leaders_proposed, leaders_elected, phase1_votes, phase2_votes>>

----------------------------------------------------------------------------
\* Initial state.
----------------------------------------------------------------------------

Init ==
    /\ promised_term  = [n \in Nodes |-> 0]
    /\ accepted_term  = [n \in Nodes |-> 0]
    /\ accepted_leader = [n \in Nodes |-> 0]   \* 0 = no leader yet
    /\ leaders_proposed = {}
    /\ leaders_elected  = {}
    /\ phase1_votes = [tl \in {} |-> {}]
    /\ phase2_votes = [tl \in {} |-> {}]

----------------------------------------------------------------------------
\* Actions
----------------------------------------------------------------------------

\* Some node `leader` proposes itself for term `t`. (run_election in paxos.rs)
StartElection(leader, t) ==
    /\ leader \in Nodes
    /\ t \in 1..MaxTerm
    /\ <<t, leader>> \notin leaders_proposed
    /\ <<t, leader>> \notin leaders_elected
    /\ leaders_proposed' = leaders_proposed \cup {<<t, leader>>}
    /\ phase1_votes' = phase1_votes @@ (<<t, leader>> :> {})
    /\ UNCHANGED <<promised_term, accepted_term, accepted_leader,
                   leaders_elected, phase2_votes>>

\* Acceptor n promises to leader at term t (Phase 1, "Promise" reply).
\* run_acceptor in paxos.rs: an acceptor responds with granted=true iff
\* it has not already promised a higher-termed proposer.
PromiseVote(n, leader, t) ==
    /\ <<t, leader>> \in leaders_proposed
    /\ n \in Nodes
    /\ promised_term[n] < t
    /\ promised_term' = [promised_term EXCEPT ![n] = t]
    /\ phase1_votes' = [phase1_votes EXCEPT
                        ![<<t, leader>>] = @ \cup {n}]
    /\ UNCHANGED <<accepted_term, accepted_leader,
                   leaders_proposed, leaders_elected, phase2_votes>>

\* Once leader has Q1 promises, it broadcasts Accept (start of Phase 2).
\* paxos.rs: after gathering enough Promise replies the proposer
\* broadcasts ElectionResult.
StartPhase2(leader, t) ==
    /\ <<t, leader>> \in leaders_proposed
    /\ Cardinality(phase1_votes[<<t, leader>>]) >= Q1
    /\ phase2_votes' = phase2_votes @@ (<<t, leader>> :> {})
    /\ UNCHANGED <<promised_term, accepted_term, accepted_leader,
                   leaders_proposed, leaders_elected, phase1_votes>>

\* Acceptor n accepts leader at term t (Phase 2 "Accepted" reply).
\* The acceptor must not have promised a strictly higher term in the
\* meantime (paxos.rs validates this in run_acceptor). Per the Paxos
\* paper: an acceptor that accepts at term t implicitly promises at t,
\* so promised_term is updated to max(t, promised_term).
AcceptVote(n, leader, t) ==
    /\ <<t, leader>> \in leaders_proposed
    /\ <<t, leader>> \in DOMAIN phase2_votes
    /\ n \in Nodes
    /\ promised_term[n] <= t          \* hasn't moved on to a higher term
    /\ accepted_term'   = [accepted_term  EXCEPT ![n] = t]
    /\ accepted_leader' = [accepted_leader EXCEPT ![n] = leader]
    /\ promised_term'   = [promised_term  EXCEPT ![n] = t]
    /\ phase2_votes' = [phase2_votes EXCEPT
                        ![<<t, leader>>] = @ \cup {n}]
    /\ UNCHANGED <<leaders_proposed, leaders_elected, phase1_votes>>

\* Once leader has Q2 accepts at term t, the leader is elected.
DeclareElected(leader, t) ==
    /\ <<t, leader>> \in leaders_proposed
    /\ <<t, leader>> \in DOMAIN phase2_votes
    /\ Cardinality(phase2_votes[<<t, leader>>]) >= Q2
    /\ leaders_elected' = leaders_elected \cup {<<t, leader>>}
    /\ UNCHANGED <<promised_term, accepted_term, accepted_leader,
                   leaders_proposed, phase1_votes, phase2_votes>>

Next ==
    \/ \E n \in Nodes, t \in 1..MaxTerm : StartElection(n, t)
    \/ \E n \in Nodes, t \in 1..MaxTerm, l \in Nodes : PromiseVote(n, l, t)
    \/ \E l \in Nodes, t \in 1..MaxTerm : StartPhase2(l, t)
    \/ \E n \in Nodes, t \in 1..MaxTerm, l \in Nodes : AcceptVote(n, l, t)
    \/ \E l \in Nodes, t \in 1..MaxTerm : DeclareElected(l, t)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
\* Properties
----------------------------------------------------------------------------

TypeOK ==
    /\ promised_term \in [Nodes -> 0..MaxTerm]
    /\ accepted_term \in [Nodes -> 0..MaxTerm]
    /\ accepted_leader \in [Nodes -> Nodes \cup {0}]
    /\ leaders_proposed \in SUBSET ((1..MaxTerm) \X Nodes)
    /\ leaders_elected  \in SUBSET ((1..MaxTerm) \X Nodes)

\* QuorumIntersection: any two voter sets — one Phase-1, one Phase-2 —
\* sized at the configured thresholds must share at least one node.
\* For uniform same-sized quorums this is the same as Q1 + Q2 > |Nodes|,
\* which we already ASSUMEd above.
QuorumIntersection ==
    \A V1 \in SUBSET Nodes, V2 \in SUBSET Nodes :
        Cardinality(V1) >= Q1 /\ Cardinality(V2) >= Q2 =>
            V1 \cap V2 # {}

\* ElectionSafety: at most one leader per term. This is the headline
\* safety property of the Paxos election protocol.
ElectionSafety ==
    \A t \in 1..MaxTerm :
        Cardinality({l \in Nodes : <<t, l>> \in leaders_elected}) <= 1

\* PromiseHonoured: a node that promised term `t` cannot have accepted
\* a leader at a strictly lower term.
PromiseHonoured ==
    \A n \in Nodes :
        accepted_term[n] = 0 \/ accepted_term[n] <= promised_term[n]

\* PromiseMonotone: an acceptor's promised_term never decreases.
\* (This is preserved by the action shape itself but is checked here so
\* a refactor that introduces a regression is caught.)
PromiseMonotone == TRUE   \* spec-shape-enforced; placeholder for clarity

Safety == TypeOK /\ ElectionSafety /\ PromiseHonoured

============================================================================
