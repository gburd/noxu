//! B+tree latch coupling: the protocol that
//! `crates/noxu-tree/src/tree.rs::insert` and `split_child` follow.
//!
//! Models concurrent threads doing `PUT`s against a tiny tree (one
//! root IN, two BIN children) under the same lock discipline the
//! production code uses:
//!
//!   - `parent.read()` is held across the BIN write
//!   - `split_child` takes `parent.write()` and holds it through
//!     snapshot + install + sibling publish
//!
//! The model is *parameterised* on a [`Variant`] so we can validate
//! both the fixed protocol and the pre-fix racy variant from a
//! single spec — the answer to "why did we have BTreeLatching and
//! BTreeLatchingBuggy?". With Stateright we just generate two
//! `Model` instances and assert that:
//!
//!   - `Variant::HandOverHand` satisfies `NoLostWrites` for every
//!     reachable state.
//!   - `Variant::DropParentEarly` produces at least one trace where
//!     `NoLostWrites` is violated — and the trace is exactly the
//!     descender-vs-splitter race that Stream F closed.
//!
//! Properties:
//!   - `NoLostWrites`: every committed key is reachable through the
//!     parent's routing entries
//!   - `LockInvariant`: at most one writer per node; readers exclude
//!     writers
//!   - `AtMostOneSplit`: only one thread may hold `parent.write()`
//!     for the split phase
//!
//! Production code under model:
//!   - `crates/noxu-tree/src/tree.rs::insert`
//!   - `crates/noxu-tree/src/tree.rs::insert_recursive`
//!   - `crates/noxu-tree/src/tree.rs::split_child`
//!   - `crates/noxu-tree/src/tree.rs::split_root_if_needed`

use stateright::{Model, Property};

/// Which variant of the descent protocol the model checks.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Variant {
    /// The fixed protocol: take child write lock BEFORE releasing
    /// parent read lock. Models the post-Stream-F code.
    HandOverHand,
    /// The pre-fix protocol: drop parent read lock, then take child
    /// write lock — the descender-vs-splitter race window. Used as
    /// regression bait: a Stateright run on this variant must
    /// produce a counterexample for `NoLostWrites`.
    DropParentEarly,
}

/// Constants — kept tiny so BFS terminates in seconds. With three
/// threads and three keys we exercise the first-key path, the
/// descent path, and at least one split.
pub const N_THREADS: usize = 3;
pub const N_KEYS: usize = 3;
/// Number of entries that triggers a split.
pub const BIN_CAPACITY: usize = 1;

/// Logical node identifiers in the modelled tree.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Node {
    /// The root internal node (level 2).
    Root,
    /// The left BIN child.
    BinL,
    /// The right BIN child (created on first split).
    BinR,
}

/// Lock state for a single node.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Default)]
pub struct LockState {
    pub readers: Vec<usize>,
    pub writer: Option<usize>,
}

/// Per-thread phase. Mirrors the production code's call sites.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Phase {
    Idle,
    /// PUT(k, v) is in flight; values held in `target_*` for thread.
    HaveRootRead,
    /// Buggy variant only: holds the child Arc but no lock.
    HaveChildRefNoLock,
    HaveBinWrite,
    HaveRootWriteSplit,
    Done,
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct State {
    /// Per-node lock state.
    pub lock: [LockState; 3], // indexed as Node usize
    /// `bin[node][key] = Some(value)` if the BIN holds the entry,
    /// `None` otherwise.
    pub bin: [Vec<Option<usize>>; 3],
    /// Routing: which BIN currently holds entries for `key`.
    pub routing: Vec<Node>,
    /// Per-thread phase + target.
    pub phase: [Phase; N_THREADS],
    pub target_key: [usize; N_THREADS],
    pub target_val: [usize; N_THREADS],
    pub target_bin: [Node; N_THREADS],
    /// Audit log of all (key, value) commits in commit order. Used
    /// to define `NoLostWrites`.
    pub committed: Vec<(usize, usize)>,
    /// Whether the right BIN is live (created on first split).
    pub has_right: bool,
}

impl State {
    fn lock_idx(n: Node) -> usize {
        match n {
            Node::Root => 0,
            Node::BinL => 1,
            Node::BinR => 2,
        }
    }
    fn lock(&self, n: Node) -> &LockState {
        &self.lock[Self::lock_idx(n)]
    }
    fn bin_of(&self, n: Node) -> &[Option<usize>] {
        &self.bin[Self::lock_idx(n)]
    }
}

/// All actions a thread can take. The model alternates threads so
/// every interleaving is explored.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum Action {
    StartPut {
        tid: usize,
        key: usize,
        val: usize,
    },
    /// Buggy variant only.
    DropParentEarly {
        tid: usize,
    },
    /// Fixed variant: take child write while holding parent read.
    /// Buggy variant: take child write after dropping parent read.
    TakeBinWrite {
        tid: usize,
    },
    DoInsert {
        tid: usize,
    },
    StartSplit {
        tid: usize,
    },
    CompleteSplit {
        tid: usize,
    },
}

/// The B+tree latching model.
pub struct BTreeLatchingModel {
    pub variant: Variant,
}

impl BTreeLatchingModel {
    pub fn new(variant: Variant) -> Self {
        Self { variant }
    }
}

fn entries_in(s: &State, n: Node) -> usize {
    s.bin_of(n).iter().filter(|x| x.is_some()).count()
}

fn release_read(state: &mut State, n: Node, tid: usize) {
    state.lock[State::lock_idx(n)].readers.retain(|&t| t != tid);
}

fn release_write(state: &mut State, n: Node, tid: usize) {
    if state.lock[State::lock_idx(n)].writer == Some(tid) {
        state.lock[State::lock_idx(n)].writer = None;
    }
}

fn try_acquire_read(state: &mut State, n: Node, tid: usize) -> bool {
    let l = &mut state.lock[State::lock_idx(n)];
    if l.writer.is_some() {
        return false;
    }
    if !l.readers.contains(&tid) {
        l.readers.push(tid);
    }
    true
}

fn try_acquire_write(state: &mut State, n: Node, tid: usize) -> bool {
    let l = &mut state.lock[State::lock_idx(n)];
    if l.writer.is_some() || !l.readers.is_empty() {
        return false;
    }
    l.writer = Some(tid);
    true
}

fn route_post_split(_n_keys: usize) -> Vec<Node> {
    // keys 0..N_KEYS/2 → BinL; keys >= N_KEYS/2 → BinR
    let mid = N_KEYS / 2;
    (0..N_KEYS).map(|k| if k < mid { Node::BinL } else { Node::BinR }).collect()
}

impl Model for BTreeLatchingModel {
    type State = State;
    type Action = Action;

    fn init_states(&self) -> Vec<Self::State> {
        vec![State {
            lock: Default::default(),
            bin: [
                vec![None; N_KEYS], // root has no entries (it's an internal node)
                vec![None; N_KEYS], // BinL
                vec![None; N_KEYS], // BinR
            ],
            // Pre-split: every key routes to BinL.
            routing: vec![Node::BinL; N_KEYS],
            phase: [Phase::Idle; N_THREADS],
            target_key: [0; N_THREADS],
            target_val: [0; N_THREADS],
            target_bin: [Node::BinL; N_THREADS],
            committed: vec![],
            has_right: false,
        }]
    }

    fn actions(&self, s: &Self::State, out: &mut Vec<Self::Action>) {
        for tid in 0..N_THREADS {
            match s.phase[tid] {
                Phase::Idle => {
                    // Allow each thread to start a PUT for any key/value.
                    // Bound: each thread does at most one PUT per run.
                    if !s.committed.iter().any(|&(_, v)| v == tid + 100) {
                        for k in 0..N_KEYS {
                            out.push(Action::StartPut {
                                tid,
                                key: k,
                                val: tid + 100,
                            });
                        }
                    }
                    // Or start a split if a BIN is at/over capacity.
                    if !s.has_right && entries_in(s, Node::BinL) >= BIN_CAPACITY
                    {
                        out.push(Action::StartSplit { tid });
                    }
                }
                Phase::HaveRootRead => {
                    match self.variant {
                        Variant::HandOverHand => {
                            // Take BIN write while still holding parent
                            // read — the fixed protocol.
                            out.push(Action::TakeBinWrite { tid });
                        }
                        Variant::DropParentEarly => {
                            // The buggy descender drops parent first.
                            out.push(Action::DropParentEarly { tid });
                        }
                    }
                }
                Phase::HaveChildRefNoLock => {
                    // Only reachable in DropParentEarly variant.
                    out.push(Action::TakeBinWrite { tid });
                }
                Phase::HaveBinWrite => {
                    out.push(Action::DoInsert { tid });
                }
                Phase::HaveRootWriteSplit => {
                    out.push(Action::CompleteSplit { tid });
                }
                Phase::Done => {}
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
            Action::StartPut { tid, key, val } => {
                s.phase[tid] = Phase::HaveRootRead;
                s.target_key[tid] = key;
                s.target_val[tid] = val;
                if !try_acquire_read(&mut s, Node::Root, tid) {
                    return None;
                }
                // Capture target_bin from current routing.
                s.target_bin[tid] = s.routing[key];
            }
            Action::TakeBinWrite { tid } => {
                let bin = s.target_bin[tid];
                if !try_acquire_write(&mut s, bin, tid) {
                    return None;
                }
                s.phase[tid] = Phase::HaveBinWrite;
            }
            Action::DropParentEarly { tid } => {
                release_read(&mut s, Node::Root, tid);
                s.phase[tid] = Phase::HaveChildRefNoLock;
            }
            Action::DoInsert { tid } => {
                let n = s.target_bin[tid];
                let k = s.target_key[tid];
                let v = s.target_val[tid];
                let idx = State::lock_idx(n);
                s.bin[idx][k] = Some(v);
                s.committed.push((k, v));
                release_write(&mut s, n, tid);
                release_read(&mut s, Node::Root, tid);
                s.phase[tid] = Phase::Done;
            }
            Action::StartSplit { tid } => {
                if !try_acquire_write(&mut s, Node::Root, tid) {
                    return None;
                }
                s.phase[tid] = Phase::HaveRootWriteSplit;
            }
            Action::CompleteSplit { tid } => {
                let split_at = N_KEYS / 2;
                let mut left = vec![None; N_KEYS];
                let mut right = vec![None; N_KEYS];
                for (k, slot) in
                    s.bin[State::lock_idx(Node::BinL)].iter().enumerate()
                {
                    if k < split_at {
                        left[k] = *slot;
                    } else {
                        right[k] = *slot;
                    }
                }
                s.bin[State::lock_idx(Node::BinL)] = left;
                s.bin[State::lock_idx(Node::BinR)] = right;
                s.routing = route_post_split(N_KEYS);
                s.has_right = true;
                release_write(&mut s, Node::Root, tid);
                s.phase[tid] = Phase::Done;
            }
        }
        Some(s)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::<Self>::always("LockInvariant", |_, s: &State| {
                for n in [Node::Root, Node::BinL, Node::BinR] {
                    let l = s.lock(n);
                    // Writer excludes readers.
                    if l.writer.is_some() && !l.readers.is_empty() {
                        return false;
                    }
                    // At most one writer is enforced by the
                    // `Option<usize>` type itself.
                }
                true
            }),
            Property::<Self>::always("AtMostOneSplit", |_, s: &State| {
                s.phase
                    .iter()
                    .filter(|p| matches!(p, Phase::HaveRootWriteSplit))
                    .count()
                    <= 1
            }),
            Property::<Self>::always("NoLostWrites", |_, s: &State| {
                // For every committed (k, v), the routing path from
                // root to a BIN must lead to a slot whose value is v
                // (the most recent commit for k by commit order).
                use std::collections::HashMap;
                let mut last: HashMap<usize, usize> = HashMap::new();
                for &(k, v) in &s.committed {
                    last.insert(k, v);
                }
                for (&k, &expected) in &last {
                    let bin = s.routing[k];
                    let actual = s.bin_of(bin)[k];
                    if actual != Some(expected) {
                        return false;
                    }
                }
                true
            }),
        ]
    }
}

/// Default tests run by `cargo test -p noxu-spec`.
#[cfg(test)]
mod tests {
    use super::*;
    use stateright::Checker;

    /// Fixed protocol must satisfy every safety property.
    ///
    /// Cross-reference: this is the abstract counterpart to the
    /// production stress tests
    /// `crates/noxu-db/tests/concurrent_commits_stress.rs`
    /// (`concurrent_commits_no_lost_writes` and the `_smoke`
    /// variant) and
    /// `crates/noxu-db/tests/concurrent_reads_during_splits.rs`
    /// (`concurrent_reads_during_inserts_no_false_not_found`),
    /// which drive the real `Tree::insert` / `split_child` /
    /// `search` code under enough threads to reach the same race
    /// windows the model explores. If this spec test passes and
    /// either stress test fails, the implementation has diverged
    /// from the protocol the spec is verifying.
    #[test]
    fn hand_over_hand_is_safe() {
        let model = BTreeLatchingModel::new(Variant::HandOverHand);
        let checker = model.checker().spawn_bfs().join();
        checker.assert_properties();
    }

    /// Buggy protocol must produce at least one NoLostWrites
    /// counterexample. If this stops failing, the regression bait
    /// is no longer alive — flag immediately.
    ///
    /// Cross-reference: the production code closed the
    /// descender-vs-splitter race in Stream F (commit `ee688aa`,
    /// `fix(tree): close descender-vs-splitter races; serialize
    /// splits with descents`) and the post-v1.2.0 read_arc
    /// hand-over-hand conversions in `6cf14e0`. The action
    /// sequence this spec test discovers (`StartPut → take parent
    /// read → DropParentEarly → split runs → write to stale
    /// target_bin`) is exactly the bug those commits closed. A
    /// regression that re-introduces drop-then-take in any
    /// descent path would be caught by the production stress
    /// tests cited on `hand_over_hand_is_safe` above.
    #[test]
    fn buggy_protocol_loses_writes() {
        let model = BTreeLatchingModel::new(Variant::DropParentEarly);
        let checker = model.checker().spawn_bfs().join();
        let discoveries = checker.discoveries();
        let lost =
            discoveries.iter().find(|(name, _)| **name == "NoLostWrites");
        assert!(
            lost.is_some(),
            "regression bait must produce a NoLostWrites counterexample; \
             got discoveries: {:?}",
            discoveries.keys().copied().collect::<Vec<_>>()
        );
    }
}
