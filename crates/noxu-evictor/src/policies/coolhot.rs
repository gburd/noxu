//! COOL/HOT 2-bit cooling-clock eviction policy (LeanStore / 2Q-A1 model).
//!
//! Ported from the PostgreSQL buffer-manager proposal
//! `v3-0002-Replace-the-usage_count-clock-sweep-with-a-cooling-stage-evictor`
//! (see `.agent/archived-audits/bench/coolhot-clock-design-2026-07.md`).  It
//! replaces LRU's recency-ordered list and CLOCK's 0..N usage counter with a
//! two-state model plus a single reference bit:
//!
//!   * **COOL** — an eviction candidate (probationary).  A node is *admitted*
//!     COOL, never HOT.
//!   * **HOT**  — part of the working set.  Reached only by a *second* access.
//!   * **reference bit** — set on access; a set ref bit spares a HOT node one
//!     cooling pass (the second-chance bit).
//!
//! ## Why this is scan-resistant *by construction*
//! A demand-loaded node enters COOL.  A one-touch sequential scan therefore
//! fills and drains the COOL stage and is evicted from it **without ever
//! displacing the HOT working set** — the evictor prefers COOL victims and
//! only demotes HOT → COOL (`force_cool`) once a full sweep finds no COOL
//! victim.  A genuinely hot node is read repeatedly, so its *second* access
//! promotes it COOL → HOT and it leaves the eviction firing line.  This is the
//! decisive fix for the θ=0.99 Zipfian LN-cache hit-rate collapse (44% under
//! LRU): the hot set stays resident, cold/scan traffic churns COOL.
//!
//! ## Trickle / bgwriter coupling
//! Noxu's background evictor daemon is the analogue of PostgreSQL's bgwriter
//! LRU scan.  It calls [`CoolHotPolicy::trickle_cool`] to demote just enough
//! unpinned HOT nodes (bounded by the predicted next-cycle allocation) to COOL
//! **ahead of** the foreground sweep, so the foreground finds a COOL victim in
//! a single pass instead of paying `force_cool` on the hot path.  The
//! second-chance ref bit is consumed on the trickle's first HOT pass, so a
//! recently-accessed node earns one reprieve.
//!
//! ## Budget invariant (unchanged)
//! This is a *pluggable policy*: it only changes **which** node is chosen as
//! the eviction victim.  The explicit MemoryBudget accounting (charge on
//! insert / repopulate, credit on strip, reclaim to budget under pressure) is
//! entirely in the [`crate::evictor::Evictor`] / tree layer and is untouched.

use crate::policy::EvictionPolicy;
use crate::slab::{SENTINEL, SlabList};
use hashbrown::HashMap;
use noxu_sync::Mutex;

/// Per-node cooling state: two bits packed into a `u8`.
///
/// bit 0 = HOT/COOL state (1 = HOT, 0 = COOL); bit 1 = reference bit.
/// This mirrors the PG patch's in-place reinterpretation of `usage_count`
/// (`BUF_COOLSTATE_HOT = 1`, `BUF_REFBIT` = bit 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CoolState(u8);

impl CoolState {
    const HOT: u8 = 0b01;
    const REF: u8 = 0b10;

    /// Admission state: COOL, no ref bit (probationary).
    #[inline]
    fn cool() -> Self {
        CoolState(0)
    }
    #[inline]
    fn is_hot(self) -> bool {
        self.0 & Self::HOT != 0
    }
    #[inline]
    fn has_ref(self) -> bool {
        self.0 & Self::REF != 0
    }
    /// Promote to HOT and set the second-chance ref bit (an access).
    #[inline]
    fn promote(&mut self) {
        self.0 |= Self::HOT | Self::REF;
    }
    /// Demote HOT → COOL, clearing the ref bit (a cooling tick).
    #[inline]
    fn demote(&mut self) {
        self.0 = 0;
    }
    /// Consume the second-chance ref bit, staying HOT.
    #[inline]
    fn clear_ref(&mut self) {
        self.0 &= !Self::REF;
    }
}

#[derive(Debug)]
struct CoolHotState {
    /// Intrusive ring of tracked node ids (order = clock geometry, not LRU).
    list: SlabList,
    /// Per-node COOL/HOT + ref-bit state.
    state: HashMap<u64, CoolState>,
    /// node_id currently under the clock hand; `SENTINEL` sentinel = empty.
    hand: u64,
}

impl CoolHotState {
    fn new() -> Self {
        Self { list: SlabList::new(), state: HashMap::new(), hand: u64::MAX }
    }

    /// Admit a node COOL (probationary).  No-op if already tracked.
    fn admit(&mut self, id: u64) {
        if self.list.contains(id) {
            return;
        }
        self.list.add_back(id);
        self.state.insert(id, CoolState::cool());
        if self.hand == u64::MAX {
            self.hand = self.list.peek_front().unwrap_or(u64::MAX);
        }
    }

    /// Advance the hand to the successor of `current` (wrapping at the tail).
    fn advance_hand(&mut self, current: u64) {
        let slot = self.list.slot_of(current);
        if slot == SENTINEL {
            self.hand = self.list.peek_front().unwrap_or(u64::MAX);
            return;
        }
        let next_slot = self.list.slab[slot].as_ref().unwrap().next;
        if next_slot == SENTINEL {
            self.hand = self.list.peek_front().unwrap_or(u64::MAX);
        } else {
            self.hand = self.list.slab[next_slot].as_ref().unwrap().id;
        }
    }

    fn remove(&mut self, id: u64) -> bool {
        if self.hand == id {
            self.advance_hand(id);
        }
        if self.list.remove(id) {
            self.state.remove(&id);
            true
        } else {
            false
        }
    }

    /// Foreground clock sweep (PG `StrategyGetBuffer`).
    ///
    /// Prefer an already-COOL, node.  A full pass with no COOL victim
    /// escalates to `force_cool`: the first HOT node reached is then demoted
    /// HOT → COOL **and claimed as the victim** in the same step (the cooling
    /// tick that is guaranteed progress).  This bounds each call to cooling at
    /// most ONE hot node — it does not cascade-cool the whole hot set, which
    /// would defeat scan resistance.  A second unproductive full pass means
    /// the ring is empty; return `None`.
    fn evict(&mut self) -> Option<u64> {
        if self.list.len == 0 {
            return None;
        }
        let n = self.list.len;
        // One prefer-COOL pass, then (if no COOL victim) one force_cool pass
        // that cools+claims the first HOT node.  `n + n + 1` bounds both.
        let max_iters = 2 * n + 1;
        let mut force_cool = false;
        let mut ticks_this_pass = 0usize;

        for _ in 0..max_iters {
            let candidate = self.hand;
            if candidate == u64::MAX {
                break;
            }
            let st =
                self.state.get(&candidate).copied().unwrap_or(CoolState(0));

            if st.is_hot() {
                if force_cool {
                    // No COOL victim exists this cycle: demote this HOT node
                    // to COOL and claim it immediately.  Cooling exactly one
                    // node (not a cascade) preserves the rest of the hot set.
                    self.advance_hand(candidate);
                    self.list.remove(candidate);
                    self.state.remove(&candidate);
                    return Some(candidate);
                }
                // Prefer COOL: advance the hand, no state change.
                self.advance_hand(candidate);
                ticks_this_pass += 1;
                if ticks_this_pass >= n {
                    // A full pass found no COOL victim — escalate.
                    force_cool = true;
                    ticks_this_pass = 0;
                }
                continue;
            }

            // COOL: claim it as the victim.
            self.advance_hand(candidate);
            self.list.remove(candidate);
            self.state.remove(&candidate);
            return Some(candidate);
        }

        // Degenerate fall-through: evict whatever the hand points at.
        let candidate = self.hand;
        if candidate != u64::MAX {
            self.advance_hand(candidate);
            self.list.remove(candidate);
            self.state.remove(&candidate);
            Some(candidate)
        } else {
            None
        }
    }

    /// Trickle / bgwriter pre-cooling (PG `SyncOneBuffer(cool_if_hot=true)`).
    ///
    /// Walk forward from the hand demoting HOT nodes to COOL, staging up to
    /// `budget` eviction candidates ahead of the foreground sweep.  A HOT node
    /// whose ref bit is set is *spared* (its ref bit is consumed instead) — the
    /// second-chance that keeps the genuinely hot set out of COOL under scan
    /// pressure.  Returns the number of nodes newly demoted to COOL.
    fn trickle_cool(&mut self, budget: usize) -> usize {
        if budget == 0 || self.list.len == 0 {
            return 0;
        }
        let mut cooled = 0usize;
        // Bound the walk to one full pass so the trickle can't spin.
        let steps = self.list.len;
        for _ in 0..steps {
            if cooled >= budget {
                break;
            }
            let id = self.hand;
            if id == u64::MAX {
                break;
            }
            let st = self.state.get(&id).copied().unwrap_or(CoolState(0));
            if st.is_hot() {
                if st.has_ref() {
                    // Second chance: consume the ref bit, stay HOT.
                    if let Some(s) = self.state.get_mut(&id) {
                        s.clear_ref();
                    }
                } else {
                    // Not re-accessed since our last pass: demote to COOL.
                    if let Some(s) = self.state.get_mut(&id) {
                        s.demote();
                    }
                    cooled += 1;
                }
            }
            self.advance_hand(id);
        }
        cooled
    }

    /// Count of nodes currently in the COOL (reusable-candidate) state.
    fn cool_count(&self) -> usize {
        self.state.values().filter(|s| !s.is_hot()).count()
    }
}

/// COOL/HOT 2-bit cooling-clock eviction policy.
#[derive(Debug)]
pub struct CoolHotPolicy {
    state: Mutex<CoolHotState>,
}

impl CoolHotPolicy {
    pub fn new() -> Self {
        Self { state: Mutex::new(CoolHotState::new()) }
    }

    /// Pre-stage up to `budget` COOL victims by demoting unpinned HOT nodes
    /// ahead of the foreground sweep (the trickle/bgwriter path).  Returns the
    /// number newly cooled.  Called by the evictor daemon.
    pub fn trickle_cool(&self, budget: usize) -> usize {
        self.state.lock().trickle_cool(budget)
    }

    /// Number of nodes currently COOL (available as eviction candidates
    /// without a `force_cool` pass).  Used by the trickle to size its demand.
    pub fn cool_count(&self) -> usize {
        self.state.lock().cool_count()
    }
}

impl Default for CoolHotPolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl EvictionPolicy for CoolHotPolicy {
    /// ADMISSION: a newly resident node enters COOL (probationary), not HOT.
    /// The `Evictor::note_ins_added` DEFAULT path routes here; scan resistance
    /// depends on this staying COOL (a one-touch scan never reaches HOT).
    fn insert(&self, node_id: u64) {
        self.state.lock().admit(node_id);
    }

    /// Explicit cold admission (MAKE_COLD / scan): also COOL — already the
    /// coldest state this policy has.
    fn insert_cold(&self, node_id: u64) {
        self.state.lock().admit(node_id);
    }

    /// ACCESS: promote COOL → HOT and set the second-chance ref bit (the 2Q
    /// rescue).  Called from `Tree::search_with_data` via the CacheMode
    /// keep-hot wiring on every point read.  Returns `false` if the node is
    /// not tracked (the caller then tries the scan policy).
    fn touch(&self, node_id: u64) -> bool {
        let mut s = self.state.lock();
        if let Some(st) = s.state.get_mut(&node_id) {
            st.promote();
            true
        } else {
            false
        }
    }

    fn remove(&self, node_id: u64) -> bool {
        self.state.lock().remove(node_id)
    }

    fn evict_candidate(&self) -> Option<u64> {
        self.state.lock().evict()
    }

    /// A node selected by the sweep but not evictable (pinned / cursor) is put
    /// back.  It re-enters COOL, **not** HOT: the evictor selecting it is not
    /// an application access, so it must not be promoted into the working set
    /// (that was the bug that let stripped cold-tail nodes masquerade as HOT
    /// and starve the COOL victim supply, forcing `force_cool` to cool the
    /// genuinely hot set).  A genuinely hot node re-enters COOL here and is
    /// promoted back to HOT by its next real read via `touch`.
    fn put_back(&self, node_id: u64) {
        self.state.lock().admit(node_id);
    }

    fn contains(&self, node_id: u64) -> bool {
        self.state.lock().list.contains(node_id)
    }

    fn len(&self) -> usize {
        self.state.lock().list.len
    }

    fn name(&self) -> &'static str {
        "CoolHot"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::EvictionPolicy;

    #[test]
    fn admission_is_cool_and_evicted_before_hot() {
        let p = CoolHotPolicy::new();
        // Two nodes admitted COOL.  Node 2 is accessed (→ HOT).
        p.insert(1);
        p.insert(2);
        p.touch(2); // 2 → HOT
        // 1 is COOL → evicted before the HOT node 2.
        assert_eq!(p.evict_candidate(), Some(1));
    }

    #[test]
    fn one_touch_scan_does_not_displace_hot_set() {
        let p = CoolHotPolicy::new();
        // Hot working set: accessed twice (admitted COOL, then read → HOT).
        for id in 1..=3u64 {
            p.insert(id);
            p.touch(id);
        }
        // A scan streams in many one-touch pages (admitted COOL, never
        // re-accessed).  They stay COOL.
        for id in 100..=110u64 {
            p.insert(id);
        }
        // Every eviction victim must be a scan page (COOL), never a hot node.
        for _ in 0..11 {
            let v = p.evict_candidate().expect("victim");
            assert!(
                (100..=110).contains(&v),
                "scan page must be evicted before any hot node; got {v}"
            );
        }
        // All three hot nodes are still resident.
        assert_eq!(p.len(), 3);
        assert!(p.contains(1) && p.contains(2) && p.contains(3));
    }

    #[test]
    fn force_cool_when_all_hot() {
        let p = CoolHotPolicy::new();
        // Everything is HOT (accessed).  With no COOL victim the sweep must
        // still make progress by cooling then evicting.
        for id in 1..=4u64 {
            p.insert(id);
            p.touch(id);
        }
        let v = p.evict_candidate().expect("force_cool must yield a victim");
        assert!((1..=4).contains(&v));
        assert_eq!(p.len(), 3);
    }

    #[test]
    fn trickle_spares_recently_accessed_via_ref_bit() {
        let p = CoolHotPolicy::new();
        // node 1 HOT + ref set (just accessed); node 2 HOT + ref set.
        p.insert(1);
        p.touch(1);
        p.insert(2);
        p.touch(2);
        // First trickle pass: ref bits set → both spared (0 cooled), ref bits
        // consumed.
        assert_eq!(p.trickle_cool(10), 0);
        // Second pass: ref bits now clear → both demoted to COOL.
        assert_eq!(p.trickle_cool(10), 2);
        assert_eq!(p.cool_count(), 2);
    }

    #[test]
    fn trickle_bounded_by_budget() {
        let p = CoolHotPolicy::new();
        for id in 1..=10u64 {
            p.insert(id);
            p.touch(id);
        }
        // Consume ref bits first (one full pass).
        let _ = p.trickle_cool(0); // no-op
        p.trickle_cool(100); // consumes ref bits, cools some
        // After ref bits are gone, a budget of 3 cools at most 3.
        // (Re-touch to reset a HOT+ref state deterministically.)
        for id in 1..=10u64 {
            p.touch(id);
        }
        p.trickle_cool(100); // consume ref bits
        let cooled = p.trickle_cool(3);
        assert!(
            cooled <= 3,
            "trickle must respect the budget; cooled {cooled}"
        );
    }

    #[test]
    fn touch_promotes_cool_to_hot() {
        let p = CoolHotPolicy::new();
        p.insert(1);
        p.insert(2);
        p.insert(3);
        // Promote 2 to HOT before any cooling.
        assert!(p.touch(2));
        // 1 and 3 are COOL → evicted first; 2 survives.
        let v1 = p.evict_candidate().unwrap();
        let v2 = p.evict_candidate().unwrap();
        assert!(v1 != 2 && v2 != 2, "hot node 2 must not be evicted first");
        assert!(p.contains(2));
    }

    #[test]
    fn remove_and_len() {
        let p = CoolHotPolicy::new();
        p.insert(1);
        p.insert(2);
        assert_eq!(p.len(), 2);
        assert!(p.remove(1));
        assert!(!p.remove(1));
        assert_eq!(p.len(), 1);
        assert!(p.contains(2));
    }

    #[test]
    fn empty_evict_is_none() {
        let p = CoolHotPolicy::new();
        assert_eq!(p.evict_candidate(), None);
    }
}

#[cfg(test)]
mod tests_drain {
    use super::*;
    use crate::policy::EvictionPolicy;
    /// A pool of all-COOL nodes must drain completely in one sweep (no node
    /// left stuck): the evict_batch quota path relies on this.
    #[test]
    fn drain_three_cool_nodes() {
        let p = CoolHotPolicy::new();
        p.insert(1);
        p.insert(2);
        p.insert(3);
        assert_eq!(p.len(), 3);
        assert!(p.evict_candidate().is_some());
        assert!(p.evict_candidate().is_some());
        assert!(p.evict_candidate().is_some());
        assert_eq!(p.evict_candidate(), None);
        assert_eq!(p.len(), 0);
    }
}
