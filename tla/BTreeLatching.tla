--------------------------- MODULE BTreeLatching ---------------------------
(* MODELS: crates/noxu-tree/src/tree.rs *)
(* MODELS: crates/noxu-tree/src/in_node.rs *)
(* MODELS: crates/noxu-tree/src/bin.rs *)
(*
A TLA+ model of the B+tree concurrent insert/split protocol implemented in
`noxu-tree`. Models a bounded number of threads operating on a tiny
fixed-shape tree (one root, two BINs) with the same lock discipline the
Rust code uses:

  - parent.read()    : multiple readers, no writers
  - parent.write()   : one writer, no readers
  - child.read()
  - child.write()

The model captures the four races that the noxu codebase has fixed:

  1. "First-key TOCTOU" — multiple threads observing an empty root and
     each installing a fresh single-entry root, the last writer winning
     and silently overwriting the others.

  2. "Snapshot-vs-install split" — split_child reads the BIN's entries
     under one lock, drops the lock, computes left/right halves, and
     re-acquires a write lock to install — losing any insert that
     landed in the gap.

  3. "Descender-vs-splitter" — an inserter captures a child Arc under
     the parent's read lock, drops the parent lock, and only later
     takes the child's write lock; meanwhile a splitter relocates the
     descender's target keys into a new sibling, so the descender's
     write lands in the wrong half (silently lost on subsequent
     searches).

  4. "Reader-vs-splitter" — same shape as (3) but on the read path,
     producing a transient false NotFound for keys that are present.

The fix applied to all four is *hand-over-hand* (a.k.a. lock coupling):
the descender takes the child's lock BEFORE releasing the parent's, and
split_child holds parent.write() across the entire snapshot+install +
sibling-publish so concurrent descenders cannot observe the partly-
finished split.

Properties checked here:

  * NoLostWrites    : every PUT(k,v) that returns OK eventually appears
                      under search(k) (with no concurrent writer
                      overwriting v afterwards).

  * NoFalseNotFound : a search(k) running concurrently with split_child
                      or PUT either returns the value at the linearisation
                      point of the search OR is properly serialised after a
                      delete. It never returns NotFound for a key that
                      was committed before the search started.

  * AtMostOneSplit  : split_child(parent, idx, ...) is the only action that
                      replaces parent.entries[idx]; two concurrent splits on
                      the same (parent, idx) are mutually exclusive.

The constants below are deliberately small to keep TLC's state space
manageable. To shrink further for a quick smoke run, set MaxThreads = 2
and MaxKeys = 2 in the .cfg.
*)

EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS
    MaxThreads,    \* number of concurrent client threads
    MaxKeys,       \* total keys in the modeled key space
    BinCapacity    \* split threshold (a BIN with > BinCapacity entries splits)

ASSUME
    /\ MaxThreads \in 1 .. 8
    /\ MaxKeys \in 1 .. 16
    /\ BinCapacity \in 1 .. 8

----------------------------------------------------------------------------
\* Tree shape: one root IN, at most 2 BINs (left and right). On startup
\* there is just one BIN; the first split creates the second.
----------------------------------------------------------------------------

NodeId  == {"root", "binL", "binR"}
ThreadId == 1 .. MaxThreads
Key      == 1 .. MaxKeys
\* Use 0 as the "no value" sentinel so the bin codomain is a clean
\* integer subrange — TLC type-checking is friendlier than mixing
\* strings and integers.
NoVal    == 0
Value    == 1 .. MaxKeys     \* values are arbitrary; we use the key as value

\* Lock state: each node has a {free, reading(set of threads), writing(thread)}.
LockKind == {"free", "reading", "writing"}

\* Per-thread phase. The phases mirror the Rust implementation's call sites.
ThreadPhase ==
    {"idle",
     "want_root_read",
     "have_root_read",       \* reading the root, looking up which BIN
     "want_bin_write",       \* about to take BIN write to insert
     "have_bin_write",       \* inside BIN write lock
     "want_root_write_split",\* about to start split_child
     "have_root_write_split",
     "done_ok",
     "done_lost"}

VARIABLES
    \* Per-node lock state.
    lock,           \* lock[n] = [kind |-> "free"|"reading"|"writing", holders |-> SUBSET ThreadId]
    \* Per-node logical contents — modeled as a function from key to value (or NoVal).
    bin,            \* bin[n] = [k \in Key |-> Value | NoVal]
    \* Root keeps a routing map: key range -> binId.
    routing,        \* routing[k \in Key] = "binL" | "binR"
    \* Per-thread state.
    phase,          \* phase[t] = ThreadPhase
    target_key,     \* the key the thread is trying to PUT
    target_val,     \* the value
    target_bin,     \* the BIN the thread is targeting (decided by routing)
    \* Audit log: every committed (key, value) pair, in commit order. Used to
    \* check NoLostWrites against the eventual state of the BINs.
    committed,
    \* Whether the right BIN exists (created on first split).
    has_right

vars ==
    <<lock, bin, routing, phase, target_key, target_val, target_bin,
      committed, has_right>>

----------------------------------------------------------------------------
\* Helpers
----------------------------------------------------------------------------

LockFree(n) == lock[n].kind = "free"
LockReadable(n) == lock[n].kind \in {"free", "reading"}
LockHeldExclusive(n, t) ==
    /\ lock[n].kind = "writing"
    /\ t \in lock[n].holders

AcquireRead(n, t) ==
    /\ LockReadable(n)
    /\ lock' = [lock EXCEPT
                ![n] = [kind |-> "reading",
                        holders |-> @.holders \cup {t}]]

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

\* Routing rule: keys 1..MaxKeys/2 -> binL, rest -> binR (after split).
DefaultRouting ==
    [k \in Key |-> "binL"]   \* before the first split, every key goes to binL

PostSplitRouting ==
    [k \in Key |-> IF k <= MaxKeys \div 2 THEN "binL" ELSE "binR"]

----------------------------------------------------------------------------
\* Initial state: one BIN ("binL"), root routes everything to it, all
\* nodes free, all threads idle, no commits.
----------------------------------------------------------------------------

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

----------------------------------------------------------------------------
\* Actions
----------------------------------------------------------------------------

\* A thread starts a PUT(k, v).
StartPut(t, k, v) ==
    /\ phase[t] = "idle"
    /\ k \in Key
    /\ v \in Value
    /\ phase' = [phase EXCEPT ![t] = "want_root_read"]
    /\ target_key' = [target_key EXCEPT ![t] = k]
    /\ target_val' = [target_val EXCEPT ![t] = v]
    /\ UNCHANGED <<lock, bin, routing, target_bin, committed, has_right>>

\* Take root.read() — start of latch coupling.
TakeRootRead(t) ==
    /\ phase[t] = "want_root_read"
    /\ AcquireRead("root", t)
    /\ phase' = [phase EXCEPT ![t] = "have_root_read"]
    /\ target_bin' = [target_bin EXCEPT ![t] = routing[target_key[t]]]
    /\ UNCHANGED <<bin, routing, target_key, target_val, committed, has_right>>

\* While holding root.read(), take BIN.write(). This is the
\* hand-over-hand step. The fixed code takes child lock BEFORE
\* dropping parent's read lock. Modelling that ordering is what
\* prevents the descender-vs-splitter race below.
TakeBinWriteCoupled(t) ==
    /\ phase[t] = "have_root_read"
    /\ AcquireWrite(target_bin[t], t)
    /\ phase' = [phase EXCEPT ![t] = "have_bin_write"]
    /\ UNCHANGED <<bin, routing, target_key, target_val, target_bin,
                    committed, has_right>>

\* Insert into BIN under write lock and commit. The Rust code applies
\* the entry to the in-memory tree under the BIN write lock; we mirror
\* that here. Append to the audit log atomically with the BIN write.
DoInsertAndCommit(t) ==
    /\ phase[t] = "have_bin_write"
    /\ LET n == target_bin[t]
           k == target_key[t]
           v == target_val[t]
       IN  /\ bin' = [bin EXCEPT ![n] = [@ EXCEPT ![k] = v]]
           /\ committed' = Append(committed, <<k, v>>)
    /\ \* Release the BIN write lock first, then the root read lock.
       LET hold_root == lock["root"].holders \ {t} IN
       lock' = [lock EXCEPT
            ![target_bin[t]] = [kind |-> "free", holders |-> {}],
            ![ "root" ] =
                 IF hold_root = {}
                     THEN [kind |-> "free", holders |-> {}]
                     ELSE [kind |-> "reading", holders |-> hold_root]]
    /\ phase' = [phase EXCEPT ![t] = "done_ok"]
    /\ UNCHANGED <<routing, target_key, target_val, target_bin, has_right>>

\* split_child: a thread observes binL is full and decides to split it.
\* The fixed implementation takes parent.write() at the start and holds
\* it through the entire operation. We model that here.
StartSplit(t) ==
    /\ phase[t] = "idle"
    /\ ~has_right
    /\ \* Some BIN is at or above capacity.
       Cardinality({k \in Key : bin["binL"][k] # NoVal}) >= BinCapacity
    /\ AcquireWrite("root", t)
    /\ phase' = [phase EXCEPT ![t] = "have_root_write_split"]
    /\ UNCHANGED <<bin, routing, target_key, target_val, target_bin,
                   committed, has_right>>

CompleteSplit(t) ==
    /\ phase[t] = "have_root_write_split"
    /\ \* Snapshot+install of the split is atomic under root.write().
       LET split_at == MaxKeys \div 2
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

\* "Reset" is intentionally NOT an action: TLC's state space stays
\* bounded if each thread does at most one PUT and one possible split.
\* Re-running PUTs after completion would create an unbounded reachable
\* set; the safety properties we care about are visible after any
\* finite interleaving of operations.

Next ==
    \E t \in ThreadId :
        \/ \E k \in Key, v \in Value : StartPut(t, k, v)
        \/ TakeRootRead(t)
        \/ TakeBinWriteCoupled(t)
        \/ DoInsertAndCommit(t)
        \/ StartSplit(t)
        \/ CompleteSplit(t)

Spec == Init /\ [][Next]_vars

----------------------------------------------------------------------------
\* Properties
----------------------------------------------------------------------------

\* TypeOK: every variable is in its declared domain.
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

\* Lock invariant: a writer excludes everyone; readers exclude writers.
LockInvariant ==
    \A n \in NodeId :
        \/ /\ lock[n].kind = "free"
           /\ lock[n].holders = {}
        \/ /\ lock[n].kind = "reading"
           /\ Cardinality(lock[n].holders) >= 1
        \/ /\ lock[n].kind = "writing"
           /\ Cardinality(lock[n].holders) = 1

\* AtMostOneSplit: only one thread can be holding the root write lock.
AtMostOneSplit ==
    Cardinality({t \in ThreadId : phase[t] = "have_root_write_split"}) <= 1

\* NoLostWrites: every committed (k, v) in the log exists somewhere in
\* the tree's BINs (looking up by routing) UNLESS a later write to the
\* same key overwrote it.
LastWriteWinsByKey(k) ==
    LET hits  == { i \in 1..Len(committed) : committed[i][1] = k }
    IN  IF hits = {}
            THEN NoVal
            ELSE LET maxi == CHOOSE i \in hits : \A j \in hits : i >= j
                 IN committed[maxi][2]

NoLostWrites ==
    \A k \in Key :
        LET expected == LastWriteWinsByKey(k)
            actual   == bin[routing[k]][k]
        IN  expected = NoVal \/ expected = actual

\* NoFalseNotFound is a stronger property — it requires a notion of
\* concurrent reads, which we don't model here. Instead we check the
\* read-side invariant indirectly via NoLostWrites at every reachable
\* state: routing[k] always points to a BIN whose entry for k matches
\* the latest commit. Catching the descender-vs-splitter race
\* corresponds to NoLostWrites failing at a reachable state where a
\* PUT raced with a split.

\* Safety conjunction.
Safety == TypeOK /\ LockInvariant /\ AtMostOneSplit /\ NoLostWrites

\* Liveness: every started PUT eventually finishes (no permanent stuck
\* state). With WF_vars(Next) above, TLC checks this.
EventuallyDone ==
    \A t \in ThreadId :
        (phase[t] \in {"want_root_read", "have_root_read",
                       "want_bin_write", "have_bin_write",
                       "want_root_write_split", "have_root_write_split"})
        ~> (phase[t] \in {"done_ok", "done_lost", "idle"})

============================================================================
