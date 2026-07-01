//! Process-global shared evictor for cross-environment cache balancing.
//!
//! Faithful port of JE `com.sleepycat.je.evictor.SharedEvictor` +
//! `EnvironmentImpl.getEvictor()`.  In JE, every `Environment` opened with
//! `EnvironmentConfig.setSharedCache(true)` registers with a *single*
//! process-global `SharedEvictor` that maintains ONE global LRU spanning all
//! sharing environments' INs and enforces ONE `MemoryBudget` (the shared
//! cache size, taken from the FIRST environment that joins the shared cache).
//! Eviction picks victims across all registered environments' trees, not
//! per-environment.
//!
//! ## Noxu mapping
//!
//! The existing [`Evictor`](crate::evictor::Evictor) already:
//! - walks *every* tree in its `db_trees_registry` (EVICTOR-RECLAIM-1), and
//! - enforces ONE budget via the [`Arbiter`](crate::arbiter::Arbiter) reading
//!   a single shared `cache_usage: Arc<AtomicI64>` counter.
//!
//! So a shared cache is simply: **all sharing environments point at the SAME
//! `Arc<Evictor>`, the SAME `cache_usage` atomic, and the SAME
//! `db_trees_registry`.**  Each env inserts its own database trees into the
//! shared registry (so eviction spans all envs) and each env's tree memory
//! counter is the shared `cache_usage` (so the one budget accounts for the
//! sum of all envs' resident nodes).
//!
//! ## Process-global singleton (keyed by nothing — one global)
//!
//! JE keys nothing: there is ONE `SharedEvictor` per JVM.  Noxu matches this
//! with a lazily-initialised `OnceLock<Mutex<Option<SharedEvictorState>>>`.
//! The `Option` (not just the value) is what makes it **resettable**: when the
//! last member environment deregisters, the daemon is shut down and the state
//! is torn down, so a subsequent env re-creates a fresh shared evictor.  This
//! also bounds test-isolation leakage (see `reset_for_test`).
//!
//! ## Concurrency
//!
//! `join`, `deregister`, and the daemon's `do_evict` scan all touch the same
//! process-global state.  The invariants:
//! - `join`/`deregister` mutate the singleton under the global `Mutex`, so
//!   two environments opening/closing concurrently serialise.
//! - The shared `db_trees_registry` is itself an `Arc<Mutex<HashMap>>`; the
//!   evictor's scan (`candidate_trees`) and an env's register/deregister both
//!   lock it, so a closing env's trees are removed atomically w.r.t. a scan.
//!   A scan that already snapshotted the tree `Arc`s before deregister still
//!   holds strong references, so no tree is freed mid-scan (no use-after-free);
//!   the *next* scan simply no longer sees the closed env's trees.
//! - Deregister removes the closed env's db_ids from the shared registry
//!   BEFORE the env's `EnvironmentImpl` (and thus its tree `Arc`s) drops, so
//!   the shared LRU never retains a dangling reference to a closed env's node.

use crate::arbiter::Arbiter;
use crate::evictor::Evictor;
use crate::policy::EvictionAlgorithm;
use noxu_tree::Tree;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

/// The per-env registry type the evictor, cleaner, and checkpointer share.
pub type TreeRegistry = Arc<Mutex<HashMap<i64, Arc<RwLock<Tree>>>>>;

/// Parameters the FIRST joining environment uses to size the one shared
/// budget (JE: the shared `MemoryBudget` is initialised from the first
/// shared-cache environment's `EnvironmentConfig`).  Later joiners' cache
/// sizes are ignored (JE behaviour) — matched here by only reading these on
/// initial creation.
#[derive(Debug, Clone, Copy)]
pub struct SharedCacheParams {
    /// The one shared budget in bytes (JE `MemoryBudget.maxMemory`).
    pub budget_bytes: i64,
    /// Eviction hysteresis (JE `EVICTOR_EVICT_BYTES`).
    pub evict_bytes: i64,
    /// Critical-eviction threshold in bytes (JE
    /// `maxMemory * EVICTOR_CRITICAL_PERCENTAGE / 100`).
    pub critical_threshold: i64,
    /// Batch size (JE `EVICTOR_NODES_PER_SCAN`).
    pub nodes_per_scan: usize,
    /// `EVICTOR_LRU_ONLY`.
    pub lru_only: bool,
    /// Eviction algorithm.
    pub algorithm: EvictionAlgorithm,
}

/// The mutable process-global state, guarded by the singleton `Mutex`.
struct SharedEvictorState {
    /// The one shared evictor (JE `SharedEvictor`).
    evictor: Arc<Evictor>,
    /// The one shared budget counter (JE shared `MemoryBudget.cacheUsage`).
    cache_usage: Arc<AtomicI64>,
    /// The one shared tree registry — union of every member env's trees
    /// (JE: the shared evictor's global INList).
    registry: TreeRegistry,
    /// Number of member environments currently joined.  The daemon runs while
    /// this is > 0; the state is torn down when it reaches 0.
    members: usize,
    /// Daemon shutdown flag (owned here, cloned into the daemon closure).
    daemon_shutdown: Arc<AtomicBool>,
    /// Daemon thread handle (joined on final deregister).
    daemon: Option<std::thread::JoinHandle<()>>,
}

fn singleton() -> &'static Mutex<Option<SharedEvictorState>> {
    static STATE: OnceLock<Mutex<Option<SharedEvictorState>>> = OnceLock::new();
    STATE.get_or_init(|| Mutex::new(None))
}

/// A per-environment handle to the shared evictor.
///
/// Holds clones of the shared `Arc`s so the environment wires them into its
/// tree memory counter and cleaner/checkpointer, plus the set of db_ids this
/// env contributed to the shared registry so [`SharedEvictorHandle::deregister`]
/// can remove exactly those on close.
pub struct SharedEvictorHandle {
    evictor: Arc<Evictor>,
    cache_usage: Arc<AtomicI64>,
    registry: TreeRegistry,
    /// db_ids this env inserted into the shared registry.  A shared-cache env
    /// keeps its OWN (env-local) registry for the cleaner/checkpointer, but
    /// ALSO mirrors its trees into the shared registry keyed by a
    /// process-unique id (see [`SharedEvictorHandle::register_tree`]).
    registered_keys: Mutex<Vec<i64>>,
    deregistered: AtomicBool,
}

/// Monotonic source of process-unique registry keys.
///
/// Multiple environments in one process each have their own db_id space
/// (every env starts at db_id=1 for its primary DB), so keying the SHARED
/// registry by raw db_id would collide across envs.  We allocate a unique
/// key per (env, tree) instead.
static NEXT_SHARED_KEY: AtomicI64 = AtomicI64::new(1);

impl SharedEvictorHandle {
    /// Join the process-global shared cache, creating it if this is the first
    /// member.  `params` sizes the ONE shared budget and is honoured only on
    /// creation (JE: the first shared-cache env's config wins).
    pub fn join(params: SharedCacheParams) -> SharedEvictorHandle {
        let mut guard = singleton().lock().expect("shared evictor poisoned");
        if guard.is_none() {
            // First member: build the one shared evictor + budget + registry.
            let cache_usage = Arc::new(AtomicI64::new(0));
            let arbiter = Arbiter::new(
                params.budget_bytes,
                Arc::clone(&cache_usage),
                params.evict_bytes,
                params.critical_threshold,
            );
            let evictor = Arc::new(
                Evictor::new(arbiter, params.nodes_per_scan, params.lru_only)
                    .with_algorithm(params.algorithm),
            );
            let registry: TreeRegistry = Arc::new(Mutex::new(HashMap::new()));
            evictor.set_db_trees_registry(Arc::clone(&registry));

            // One shared daemon thread (JE: the SharedEvictor runs its own
            // background eviction threads for the whole process).
            let daemon_shutdown = Arc::new(AtomicBool::new(false));
            let ev = Arc::clone(&evictor);
            let sd = Arc::clone(&daemon_shutdown);
            let daemon = std::thread::Builder::new()
                .name("noxu-shared-evictor".to_string())
                .spawn(move || {
                    while !sd.load(Ordering::Relaxed) {
                        ev.do_evict(crate::evictor::EvictionSource::Daemon);
                        std::thread::sleep(std::time::Duration::from_millis(5));
                    }
                })
                .expect("failed to spawn noxu-shared-evictor thread");

            *guard = Some(SharedEvictorState {
                evictor,
                cache_usage,
                registry,
                members: 0,
                daemon_shutdown,
                daemon: Some(daemon),
            });
        }
        let state = guard.as_mut().expect("shared evictor state present");
        state.members += 1;
        SharedEvictorHandle {
            evictor: Arc::clone(&state.evictor),
            cache_usage: Arc::clone(&state.cache_usage),
            registry: Arc::clone(&state.registry),
            registered_keys: Mutex::new(Vec::new()),
            deregistered: AtomicBool::new(false),
        }
    }

    /// The shared evictor `Arc` — every member env stores this instead of a
    /// private evictor, so `do_evict`/`do_critical_eviction` operate on the
    /// one global LRU.
    pub fn evictor(&self) -> Arc<Evictor> {
        Arc::clone(&self.evictor)
    }

    /// The one shared budget counter — the member env sets this as its tree
    /// memory counter (JE: every shared-cache env's tree memory is charged to
    /// the shared `MemoryBudget`).
    pub fn cache_usage(&self) -> Arc<AtomicI64> {
        Arc::clone(&self.cache_usage)
    }

    /// Insert one of this env's database trees into the SHARED registry under
    /// a process-unique key (so it does not collide with another env's
    /// db_id=1).  The member env still keeps this tree in its OWN env-local
    /// registry for the cleaner/checkpointer.
    ///
    /// Returns the process-unique key so the caller can `unregister_tree` an
    /// individual tree if the database is truncated/removed.
    pub fn register_tree(&self, tree: Arc<RwLock<Tree>>) -> i64 {
        let key = NEXT_SHARED_KEY.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut reg) = self.registry.lock() {
            reg.insert(key, tree);
        }
        if let Ok(mut keys) = self.registered_keys.lock() {
            keys.push(key);
        }
        key
    }

    /// Deregister ALL of this env's trees from the shared registry.
    ///
    /// CRITICAL close-safety: this removes the closing env's trees from the
    /// shared LRU BEFORE the env's tree `Arc`s drop, so the global LRU never
    /// retains a dangling reference to a closed env's node.  Idempotent.
    ///
    /// When the last member deregisters, the shared daemon is shut down and
    /// the process-global state is torn down (resettable — a later env
    /// re-creates it).
    pub fn deregister(&self) {
        if self.deregistered.swap(true, Ordering::AcqRel) {
            return; // already deregistered (idempotent)
        }
        // 1. Remove this env's trees from the shared registry so the next
        //    eviction scan cannot touch them.
        let keys: Vec<i64> =
            self.registered_keys.lock().map(|k| k.clone()).unwrap_or_default();
        if let Ok(mut reg) = self.registry.lock() {
            for k in &keys {
                reg.remove(k);
            }
        }
        // 2. Decrement the member count; tear down when it hits 0.
        let mut guard = singleton().lock().expect("shared evictor poisoned");
        let take_state = {
            if let Some(state) = guard.as_mut() {
                state.members = state.members.saturating_sub(1);
                state.members == 0
            } else {
                false
            }
        };
        if take_state {
            // Last member: shut the daemon down and clear the singleton so a
            // future env starts fresh.
            if let Some(mut state) = guard.take() {
                state.daemon_shutdown.store(true, Ordering::Relaxed);
                // Drop the mutex guard's borrow of `state.daemon` by taking it.
                if let Some(handle) = state.daemon.take() {
                    // Release the singleton lock before joining so the daemon
                    // (which does not touch the singleton) can never deadlock
                    // against us — it only touches the registry/evictor.
                    drop(guard);
                    let _ = handle.join();
                }
            }
        }
    }

    /// Number of member environments currently joined (test/introspection).
    pub fn member_count() -> usize {
        singleton()
            .lock()
            .ok()
            .and_then(|g| g.as_ref().map(|s| s.members))
            .unwrap_or(0)
    }

    /// Test-only: force-reset the process-global shared evictor.
    ///
    /// A process-global singleton otherwise leaks across tests in the same
    /// binary.  This shuts down the daemon and clears the state so an
    /// independent test starts from a clean slate.  NOT for production use —
    /// calling this while any shared-cache env is still open will orphan that
    /// env's evictor wiring.
    #[doc(hidden)]
    pub fn reset_for_test() {
        let mut guard = singleton().lock().expect("shared evictor poisoned");
        if let Some(mut state) = guard.take() {
            state.daemon_shutdown.store(true, Ordering::Relaxed);
            if let Some(handle) = state.daemon.take() {
                drop(guard);
                let _ = handle.join();
            }
        }
    }
}

impl Drop for SharedEvictorHandle {
    fn drop(&mut self) {
        // Safety net: if the env forgot to call deregister() explicitly
        // (e.g. dropped without close()), still remove its trees so the
        // shared LRU does not dangle.
        self.deregister();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(budget: i64) -> SharedCacheParams {
        SharedCacheParams {
            budget_bytes: budget,
            evict_bytes: 1024,
            critical_threshold: budget / 20,
            nodes_per_scan: 10,
            lru_only: false,
            algorithm: EvictionAlgorithm::Lru,
        }
    }

    #[test]
    fn join_and_deregister_lifecycle() {
        SharedEvictorHandle::reset_for_test();
        assert_eq!(SharedEvictorHandle::member_count(), 0);

        let h1 = SharedEvictorHandle::join(params(4 * 1024 * 1024));
        assert_eq!(SharedEvictorHandle::member_count(), 1);
        let h2 = SharedEvictorHandle::join(params(999)); // budget ignored
        assert_eq!(SharedEvictorHandle::member_count(), 2);

        // Both handles share the SAME evictor + budget counter (one cache).
        assert!(Arc::ptr_eq(&h1.evictor(), &h2.evictor()));
        assert!(Arc::ptr_eq(&h1.cache_usage(), &h2.cache_usage()));
        // The budget is the FIRST joiner's (JE behaviour), not the second's.
        assert_eq!(
            h1.evictor().get_arbiter().get_max_memory(),
            4 * 1024 * 1024
        );

        h1.deregister();
        assert_eq!(SharedEvictorHandle::member_count(), 1);
        h2.deregister();
        assert_eq!(SharedEvictorHandle::member_count(), 0);
        // Idempotent.
        h2.deregister();
        assert_eq!(SharedEvictorHandle::member_count(), 0);

        SharedEvictorHandle::reset_for_test();
    }

    #[test]
    fn deregister_removes_only_this_envs_trees() {
        SharedEvictorHandle::reset_for_test();
        let h1 = SharedEvictorHandle::join(params(4 * 1024 * 1024));
        let h2 = SharedEvictorHandle::join(params(4 * 1024 * 1024));

        let t1 = Arc::new(RwLock::new(Tree::new(1, 256)));
        let t2 = Arc::new(RwLock::new(Tree::new(1, 256)));
        h1.register_tree(Arc::clone(&t1));
        h2.register_tree(Arc::clone(&t2));

        // Shared registry now holds both envs' trees (distinct keys).
        assert_eq!(h1.registry.lock().unwrap().len(), 2);

        // Closing env1 removes ONLY t1; env2's t2 survives.
        h1.deregister();
        let reg = h2.registry.lock().unwrap();
        assert_eq!(reg.len(), 1);
        drop(reg);

        h2.deregister();
        SharedEvictorHandle::reset_for_test();
    }
}
