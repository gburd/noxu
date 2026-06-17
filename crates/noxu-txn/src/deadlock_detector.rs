//! Deadlock detection using waits-for graph analysis.
//!

use hashbrown::{HashMap, HashSet};

/// Deadlock detector using waits-for graph analysis.
///
/// Detects deadlocks by building a waits-for graph and checking for cycles.
/// When a locker requests a lock held by other lockers, the detector checks
/// if granting the lock would create a cycle in the waits-for graph.
///
/// # Algorithm
///
/// The detector performs a depth-first search (DFS) from each owner of the
/// requested lock, looking for a path back to the requester. If such a path
/// exists, a deadlock is detected.
///
/// # Example
///
/// Consider three transactions:
/// - T1 holds lock A, waits for lock B
/// - T2 holds lock B, waits for lock C
/// - T3 holds lock C, waits for lock A
///
/// When T3 requests lock A, the detector finds:
/// T3 -> T1 -> T2 -> T3 (cycle detected!)
///
pub struct DeadlockDetector;

impl DeadlockDetector {
    /// Checks if granting a lock to `requester_id` would create a deadlock.
    ///
    /// Builds a waits-for graph and checks for cycles using depth-first search.
    ///
    /// # Arguments
    ///
    /// * `requester_id` - The locker requesting the lock
    /// * `owner_ids` - The current owners of the lock
    /// * `waits_for` - Map of locker_id -> set of locker_ids it's waiting for
    ///
    /// # Returns
    ///
    /// Some(cycle) containing the deadlock cycle if detected, None otherwise.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let mut waits_for = HashMap::new();
    /// waits_for.insert(1, HashSet::from([2]));  // T1 waits for T2
    /// waits_for.insert(2, HashSet::from([3]));  // T2 waits for T3
    ///
    /// // T3 wants to acquire lock held by T1
    /// let cycle = DeadlockDetector::detect(3, &[1], &waits_for);
    /// assert!(cycle.is_some());  // Deadlock: T3 -> T1 -> T2 -> T3
    /// ```
    pub fn detect(
        requester_id: i64,
        owner_ids: &[i64],
        waits_for: &HashMap<i64, HashSet<i64>>,
    ) -> Option<Vec<i64>> {
        // For each owner of the lock, check if there's a path back to the requester.
        // If so, we have a deadlock cycle.
        for &owner_id in owner_ids {
            // Skip if the owner is the requester (no deadlock with self).
            if owner_id == requester_id {
                continue;
            }

            let mut visited = HashSet::new();
            let mut path = vec![requester_id, owner_id];

            if Self::dfs(
                owner_id,
                requester_id,
                waits_for,
                &mut visited,
                &mut path,
            ) {
                // Found a cycle!
                return Some(path);
            }
        }

        None
    }

    /// Selects the deadlock victim from a cycle.
    ///
    /// ## Algorithm
    ///
    /// 1. Select the locker with the **fewest locks held**.  A transaction
    ///    with fewer locks has done less work, so aborting it wastes less.
    /// 2. On tie, select the **youngest transaction** (highest locker ID).
    ///    Locker IDs are assigned sequentially, so highest ID = most recently
    ///    created = youngest.  Aborting the youngest preserves the most
    ///    accumulated work in the system.
    ///
    /// ## JE comparison (TXN-6, 2026-06-16)
    ///
    /// JE `DeadlockChecker.chooseTargetedLocker` picks the victim by
    /// identity-hash pseudo-random selection from lockers sorted by thread ID,
    /// deliberately varying the victim across repeated identical deadlocks to
    /// avoid always aborting the same transaction (anti-livelock).
    ///
    /// Noxu uses a deterministic "fewest locks then youngest" criterion instead.
    /// This is also correct (any victim breaks the cycle) and minimises rollback
    /// work. The trade-off: on a repeated *identical* deadlock involving
    /// transactions with the same lock counts, Noxu will always pick the same
    /// victim, which could theoretically livelock a workload that immediately
    /// re-forms the same cycle. In practice this is rare, and the deterministic
    /// choice aids test reproducibility. If livelock on repeated identical
    /// deadlocks becomes a problem in production, add a tie-break using
    /// `locker_id % some_prime` or a per-cycle random salt.
    ///
    /// # Arguments
    ///
    /// * `cycle` - The locker IDs involved in the deadlock cycle
    /// * `lock_counts` - Map of locker_id -> number of locks held
    ///
    /// # Returns
    ///
    /// The locker_id of the chosen victim.
    pub fn select_victim(
        cycle: &[i64],
        lock_counts: &HashMap<i64, usize>,
    ) -> i64 {
        use std::cmp::Reverse;

        // Deduplicate: cycle[0] == cycle[last] when it is a closed path.
        let unique: Vec<i64> = {
            let mut seen = HashSet::new();
            cycle.iter().copied().filter(|id| seen.insert(*id)).collect()
        };

        unique
            .into_iter()
            .min_by_key(|id| {
                let count = lock_counts.get(id).copied().unwrap_or(0);
                // Primary sort: fewest locks (ascending).
                // Tiebreaker: youngest = largest ID (Reverse so min_by_key
                // selects the largest ID on a tie).
                (count, Reverse(*id))
            })
            .unwrap_or_else(|| cycle[0])
    }

    /// Performs depth-first search to find a path from `current` to `target`.
    ///
    /// # Arguments
    ///
    /// * `current` - The current node in the search
    /// * `target` - The node we're searching for (the requester)
    /// * `waits_for` - The waits-for graph
    /// * `visited` - Set of already-visited nodes (to avoid infinite loops)
    /// * `path` - The current path being explored
    ///
    /// # Returns
    ///
    /// true if a path from current to target was found, false otherwise.
    fn dfs(
        current: i64,
        target: i64,
        waits_for: &HashMap<i64, HashSet<i64>>,
        visited: &mut HashSet<i64>,
        path: &mut Vec<i64>,
    ) -> bool {
        // If we've already visited this node, stop (avoid infinite loops).
        if !visited.insert(current) {
            return false;
        }

        // Check who this locker is waiting for.
        if let Some(waiting_for) = waits_for.get(&current) {
            for &next in waiting_for {
                // Add to path.
                path.push(next);

                // Check if we've found the target (cycle detected!).
                if next == target {
                    return true;
                }

                // Recursively search from the next node.
                if Self::dfs(next, target, waits_for, visited, path) {
                    return true;
                }

                // Backtrack.
                path.pop();
            }
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_deadlock_simple() {
        // T1 waits for T2, T2 doesn't wait for anyone.
        let mut waits_for = HashMap::new();
        waits_for.insert(1, HashSet::from([2]));

        // T3 wants lock held by T1 - no cycle.
        let cycle = DeadlockDetector::detect(3, &[1], &waits_for);
        assert!(cycle.is_none());
    }

    #[test]
    fn test_simple_deadlock_two_way() {
        // T1 waits for T2.
        let mut waits_for = HashMap::new();
        waits_for.insert(1, HashSet::from([2]));

        // T2 wants lock held by T1 - creates cycle T2 -> T1 -> T2.
        let cycle = DeadlockDetector::detect(2, &[1], &waits_for);
        assert!(cycle.is_some());

        let cycle = cycle.unwrap();
        assert_eq!(cycle.len(), 3); // T2, T1, T2
        assert_eq!(cycle[0], 2);
        assert_eq!(cycle[1], 1);
        assert_eq!(cycle[2], 2);
    }

    #[test]
    fn test_three_way_deadlock() {
        // T1 waits for T2, T2 waits for T3.
        let mut waits_for = HashMap::new();
        waits_for.insert(1, HashSet::from([2]));
        waits_for.insert(2, HashSet::from([3]));

        // T3 wants lock held by T1 - creates cycle T3 -> T1 -> T2 -> T3.
        let cycle = DeadlockDetector::detect(3, &[1], &waits_for);
        assert!(cycle.is_some());

        let cycle = cycle.unwrap();
        assert_eq!(cycle.len(), 4); // T3, T1, T2, T3
        assert_eq!(cycle[0], 3);
        assert_eq!(cycle[1], 1);
        assert_eq!(cycle[2], 2);
        assert_eq!(cycle[3], 3);
    }

    #[test]
    fn test_no_cycle_diamond() {
        // Diamond graph: T1 waits for T2 and T3, T2 and T3 both wait for T4.
        let mut waits_for = HashMap::new();
        waits_for.insert(1, HashSet::from([2, 3]));
        waits_for.insert(2, HashSet::from([4]));
        waits_for.insert(3, HashSet::from([4]));

        // T5 wants lock held by T1 - no cycle.
        let cycle = DeadlockDetector::detect(5, &[1], &waits_for);
        assert!(cycle.is_none());
    }

    #[test]
    fn test_self_deadlock_avoided() {
        // T1 waits for T2.
        let mut waits_for = HashMap::new();
        waits_for.insert(1, HashSet::from([2]));

        // T1 wants lock it already owns - no deadlock with self.
        let cycle = DeadlockDetector::detect(1, &[1], &waits_for);
        assert!(cycle.is_none());
    }

    #[test]
    fn test_multiple_owners_one_deadlock() {
        // T1 waits for T2.
        let mut waits_for = HashMap::new();
        waits_for.insert(1, HashSet::from([2]));

        // T2 wants lock held by T1 and T3.
        // T1 creates a deadlock, but T3 doesn't.
        let cycle = DeadlockDetector::detect(2, &[1, 3], &waits_for);
        assert!(cycle.is_some());

        let cycle = cycle.unwrap();
        // Should find the cycle through T1.
        assert!(cycle.contains(&1));
        assert!(cycle.contains(&2));
    }

    #[test]
    fn test_complex_graph_no_deadlock() {
        // More complex graph with no cycles.
        let mut waits_for = HashMap::new();
        waits_for.insert(1, HashSet::from([2]));
        waits_for.insert(2, HashSet::from([3]));
        waits_for.insert(3, HashSet::from([4]));
        waits_for.insert(5, HashSet::from([6]));

        // T7 wants lock held by T5 - no path from T5 to T7.
        let cycle = DeadlockDetector::detect(7, &[5], &waits_for);
        assert!(cycle.is_none());
    }

    #[test]
    fn test_long_chain_deadlock() {
        // Long chain: T1 -> T2 -> T3 -> T4 -> T5.
        let mut waits_for = HashMap::new();
        waits_for.insert(1, HashSet::from([2]));
        waits_for.insert(2, HashSet::from([3]));
        waits_for.insert(3, HashSet::from([4]));
        waits_for.insert(4, HashSet::from([5]));

        // T5 wants lock held by T1 - creates long cycle.
        let cycle = DeadlockDetector::detect(5, &[1], &waits_for);
        assert!(cycle.is_some());

        let cycle = cycle.unwrap();
        assert_eq!(cycle.len(), 6); // T5, T1, T2, T3, T4, T5
        assert_eq!(cycle[0], 5);
        assert_eq!(cycle[5], 5);
    }
}
