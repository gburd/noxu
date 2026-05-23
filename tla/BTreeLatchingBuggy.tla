--------------------------- MODULE BTreeLatchingBuggy ---------------------------
(* MODELS: crates/noxu-tree/src/tree.rs *)
(* MODELS: crates/noxu-tree/src/in_node.rs *)
(* MODELS: crates/noxu-tree/src/bin.rs *)
(*
A *deliberately buggy* variant of `BTreeLatching` that models the pre-fix
behaviour of `Tree::insert_recursive` — the descender drops the parent's
read lock BEFORE taking the BIN's write lock. This is the exact race
that Stream F (commit ee688aa) closed in `crates/noxu-tree/src/tree.rs`.

Running TLC on this spec produces a counterexample where one thread's
PUT lands in the wrong BIN because a concurrent split was observed
mid-completion. The trace TLC produces is the canonical counter-example
for the descender-vs-splitter bug.

This file is the regression artefact: the spec's invariants are the
same as the fixed model's, but the action shapes differ. If TLC ever
*stops* finding a counterexample here (i.e., a future change makes the
bug disappear from the model), that's a signal that someone has
silently changed the modelled action shape.

Re-running this should produce a violation of NoLostWrites within a
small number of states.
*)

EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS
    MaxThreads,
    MaxKeys,
    BinCapacity

ASSUME
    /\ MaxThreads \in 1 .. 8
    /\ MaxKeys \in 1 .. 16
    /\ BinCapacity \in 1 .. 8

NodeId  == {"root", "binL", "binR"}
ThreadId == 1 .. MaxThreads
Key      == 1 .. MaxKeys
NoVal    == 0
Value    == 1 .. MaxKeys

LockKind == {"free", "reading", "writing"}

ThreadPhase ==
    {"idle",
     "want_root_read",
     "have_root_read",
     "have_child_arc",       \* BUGGY: holding child Arc but no lock
     "want_bin_write",
     "have_bin_write",
     "want_root_write_split",
     "have_root_write_split",
     "done_ok",
     "done_lost"}

VARIABLES
    lock,
    bin,
    routing,
    phase,
    target_key,
    target_val,
    target_bin,
    committed,
    has_right

vars ==
    <<lock, bin, routing, phase, target_key, target_val, target_bin,
      committed, has_right>>

LockFree(n) == lock[n].kind = "free"
LockReadable(n) == lock[n].kind \in {"free", "reading"}
LockHeldExclusive(n, t) ==
    /\ lock[n].kind = "writing"
    /\ t \in lock[n].holders

AcquireRead(n, t) ==
    /\ LockReadable(n)
    /\ lock' = [lock EXCEPT
                ![n] = [kind |-> "reading", holders |-> @.holders \cup {t}]]

ReleaseRead(n, t) ==
    LET h == lock[n].holders \ {t} IN
    lock' = [lock EXCEPT
             ![n] = IF h = {}
                        THEN [kind |-> "free",   holders |-> {}]
                        ELSE [kind |-> "reading", holders |-> h]]

AcquireWrite(n, t) ==
    /\ LockFree(n)
    /\ lock' = [lock EXCEPT
                ![n] = [kind |-> "writing", holders |-> {t}]]

ReleaseWrite(n, t) ==
    /\ LockHeldExclusive(n, t)
    /\ lock' = [lock EXCEPT
                ![n] = [kind |-> "free", holders |-> {}]]

DefaultRouting == [k \in Key |-> "binL"]
PostSplitRouting ==
    [k \in Key |-> IF k <= MaxKeys \div 2 THEN "binL" ELSE "binR"]

EmptyBin == [k \in Key |-> NoVal]

Init ==
    /\ lock = [n \in NodeId |-> [kind |-> "free", holders |-> {}]]
    /\ bin = [n \in NodeId |-> EmptyBin]
    /\ routing = DefaultRouting
    /\ phase = [t \in ThreadId |-> "idle"]
    /\ target_key = [t \in ThreadId |-> 0]
    /\ target_val = [t \in ThreadId |-> 0]
    /\ target_bin = [t \in ThreadId |-> "binL"]
    /\ committed = <<>>
    /\ has_right = FALSE

StartPut(t, k, v) ==
    /\ phase[t] = "idle"
    /\ k \in Key
    /\ v \in Value
    /\ phase' = [phase EXCEPT ![t] = "want_root_read"]
    /\ target_key' = [target_key EXCEPT ![t] = k]
    /\ target_val' = [target_val EXCEPT ![t] = v]
    /\ UNCHANGED <<lock, bin, routing, target_bin, committed, has_right>>

TakeRootRead(t) ==
    /\ phase[t] = "want_root_read"
    /\ AcquireRead("root", t)
    /\ phase' = [phase EXCEPT ![t] = "have_root_read"]
    /\ target_bin' = [target_bin EXCEPT ![t] = routing[target_key[t]]]
    /\ UNCHANGED <<bin, routing, target_key, target_val, committed, has_right>>

\* === BUGGY ACTION ===
\* The pre-fix code captured the child Arc, dropped the parent read lock,
\* and only later took the BIN write lock. We split the descent into two
\* steps so the open window is reachable as a state.
DropRootReadEarly(t) ==
    /\ phase[t] = "have_root_read"
    /\ ReleaseRead("root", t)
    /\ phase' = [phase EXCEPT ![t] = "have_child_arc"]
    /\ UNCHANGED <<bin, routing, target_key, target_val, target_bin,
                    committed, has_right>>

TakeBinWriteRacy(t) ==
    /\ phase[t] = "have_child_arc"
    /\ AcquireWrite(target_bin[t], t)
    /\ phase' = [phase EXCEPT ![t] = "have_bin_write"]
    /\ UNCHANGED <<bin, routing, target_key, target_val, target_bin,
                    committed, has_right>>

\* The descender writes to the BIN it captured EARLIER under the root
\* read lock — even if a split has since relocated this key into a
\* different BIN. This is the silent-lost-write moment.
DoInsertRacy(t) ==
    /\ phase[t] = "have_bin_write"
    /\ LET n == target_bin[t]
           k == target_key[t]
           v == target_val[t]
       IN  /\ bin' = [bin EXCEPT ![n] = [@ EXCEPT ![k] = v]]
           /\ committed' = Append(committed, <<k, v>>)
    /\ ReleaseWrite(target_bin[t], t)
    /\ phase' = [phase EXCEPT ![t] = "done_ok"]
    /\ UNCHANGED <<routing, target_key, target_val, target_bin, has_right>>

\* split_child as in the fixed spec — atomic under root.write().
StartSplit(t) ==
    /\ phase[t] = "idle"
    /\ ~has_right
    /\ Cardinality({k \in Key : bin["binL"][k] # NoVal}) >= BinCapacity
    /\ AcquireWrite("root", t)
    /\ phase' = [phase EXCEPT ![t] = "have_root_write_split"]
    /\ UNCHANGED <<bin, routing, target_key, target_val, target_bin,
                   committed, has_right>>

CompleteSplit(t) ==
    /\ phase[t] = "have_root_write_split"
    /\ LET split_at == MaxKeys \div 2
           old      == bin["binL"]
           leftHalf == [k \in Key |-> IF k <= split_at THEN old[k] ELSE NoVal]
           rightHalf == [k \in Key |-> IF k > split_at  THEN old[k] ELSE NoVal]
       IN  /\ bin' = [bin EXCEPT
                    !["binL"] = leftHalf,
                    !["binR"] = rightHalf]
           /\ routing' = PostSplitRouting
    /\ has_right' = TRUE
    /\ ReleaseWrite("root", t)
    /\ phase' = [phase EXCEPT ![t] = "done_ok"]
    /\ UNCHANGED <<target_key, target_val, target_bin, committed>>

Next ==
    \E t \in ThreadId :
        \/ \E k \in Key, v \in Value : StartPut(t, k, v)
        \/ TakeRootRead(t)
        \/ DropRootReadEarly(t)
        \/ TakeBinWriteRacy(t)
        \/ DoInsertRacy(t)
        \/ StartSplit(t)
        \/ CompleteSplit(t)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------

TypeOK ==
    /\ lock \in [NodeId -> [kind: LockKind, holders: SUBSET ThreadId]]
    /\ bin  \in [NodeId -> [Key -> (Value \cup {NoVal})]]
    /\ routing \in [Key -> NodeId \ {"root"}]
    /\ phase \in [ThreadId -> ThreadPhase]
    /\ target_key \in [ThreadId -> 0 .. MaxKeys]
    /\ target_val \in [ThreadId -> 0 .. MaxKeys]
    /\ target_bin \in [ThreadId -> NodeId \ {"root"}]
    /\ committed \in Seq(Key \X Value)
    /\ has_right \in BOOLEAN

LockInvariant ==
    \A n \in NodeId :
        \/ /\ lock[n].kind = "free"
           /\ lock[n].holders = {}
        \/ /\ lock[n].kind = "reading"
           /\ Cardinality(lock[n].holders) >= 1
        \/ /\ lock[n].kind = "writing"
           /\ Cardinality(lock[n].holders) = 1

AtMostOneSplit ==
    Cardinality({t \in ThreadId : phase[t] = "have_root_write_split"}) <= 1

LastWriteWinsByKey(k) ==
    LET hits == { i \in 1..Len(committed) : committed[i][1] = k }
    IN  IF hits = {}
            THEN NoVal
            ELSE LET maxi == CHOOSE i \in hits : \A j \in hits : i >= j
                 IN committed[maxi][2]

\* This is the same NoLostWrites as the fixed spec — TLC will find a
\* state where it fails because of the racy actions above.
NoLostWrites ==
    \A k \in Key :
        LET expected == LastWriteWinsByKey(k)
            actual   == bin[routing[k]][k]
        IN  expected = NoVal \/ expected = actual

============================================================================
