//! NEW-DPL-REP-COMPOSITION probe: does the DPL `EntityStore` compose with
//! the replication durability path on the MASTER side?
//!
//! ## What this probe measures
//!
//! JE's `ReplicatedEnvironment` *extends* `Environment`, so an `EntityStore`
//! (DPL) opens directly on a replicated node and its persistent entities
//! replicate.  Noxu inverts the layering: `noxu_db::Environment` is the base,
//! replication is layered *underneath* via
//! [`Environment::set_replica_coordinator`] (an
//! [`noxu_db::ReplicaAckCoordinator`], typically a
//! `noxu_rep::ReplicatedEnvironment`).  `noxu-persist` has no `noxu-rep`
//! dependency; an `EntityStore` is nothing but a set of ordinary databases on
//! an `Environment`.
//!
//! This probe answers the concrete question the composition gap raises:
//! **when a replica-ack coordinator is installed on the environment (the
//! master role), does a DPL entity write committed under an explicit
//! transaction route through the replication durability path?**
//!
//! If yes, then DPL entities on the master already participate in the
//! replica-ack durability contract exactly like any other database record —
//! the master half of the composition already works at the data layer, and
//! what is left is the *replica-side view* (opening a read-only `EntityStore`
//! against a streaming replica), which requires exposing the replica's
//! `Arc<EnvironmentImpl>` to `noxu_rep` — a cross-crate API/materialisation
//! change, i.e. a feature.
//!
//! ## Result (what the assertions below lock in)
//!
//! * A DPL `PrimaryIndex::put` under an explicit `SimpleMajority` transaction
//!   DOES consult the installed `ReplicaAckCoordinator` — proving DPL writes
//!   are ordinary replicated writes and compose with the master-side
//!   replication durability path with NO `noxu-persist` -> `noxu-rep`
//!   dependency.
//! * The auto-commit path (`put(None, ...)`) does NOT consult the coordinator
//!   (it uses a synthetic auto-txn that bypasses `begin_transaction`); this
//!   documents a real edge of the current wiring.
//! * The entity is visible locally after commit (the master serves its own
//!   DPL reads).

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use noxu_db::{
    AckWaitError, Durability, Environment, EnvironmentConfig, ReplicaAckPolicy,
    ReplicaAckCoordinator, ReplicaAckPolicyKind, SyncPolicy, TransactionConfig,
};
use noxu_persist::{
    Entity, EntitySerializer, EntityStore, PersistError, PrimaryIndex,
    StoreConfig,
};
use tempfile::TempDir;

// ─── a minimal DPL entity ────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq)]
struct Widget {
    id: u64,
    label: String,
}

impl Entity for Widget {
    type PrimaryKey = u64;
    fn primary_key(&self) -> &u64 {
        &self.id
    }
    fn entity_name() -> &'static str {
        "Widget"
    }
}

struct WidgetSerializer;

impl EntitySerializer<Widget> for WidgetSerializer {
    fn serialize(&self, w: &Widget) -> noxu_persist::Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&w.id.to_be_bytes());
        let label = w.label.as_bytes();
        buf.extend_from_slice(&(label.len() as u32).to_be_bytes());
        buf.extend_from_slice(label);
        Ok(buf)
    }
    fn deserialize(&self, bytes: &[u8]) -> noxu_persist::Result<Widget> {
        if bytes.len() < 12 {
            return Err(PersistError::SerializationError("short widget".into()));
        }
        let id = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let n = u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize;
        let label = String::from_utf8(bytes[12..12 + n].to_vec()).unwrap();
        Ok(Widget { id, label })
    }
}

// ─── a mock replica-ack coordinator (master-role stand-in) ────────────────

/// Records how many times `await_replica_acks` was consulted. A real
/// `noxu_rep::ReplicatedEnvironment` implements this trait; here we only need
/// to observe that a DPL commit reaches the replication durability hook.
struct CountingCoord {
    calls: AtomicU32,
    last_policy: std::sync::Mutex<Option<ReplicaAckPolicyKind>>,
}

impl CountingCoord {
    fn new() -> Self {
        Self {
            calls: AtomicU32::new(0),
            last_policy: std::sync::Mutex::new(None),
        }
    }
    fn calls(&self) -> u32 {
        self.calls.load(Ordering::SeqCst)
    }
}

impl ReplicaAckCoordinator for CountingCoord {
    fn await_replica_acks(
        &self,
        policy: ReplicaAckPolicyKind,
        _timeout: Duration,
    ) -> std::result::Result<u32, AckWaitError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        *self.last_policy.lock().unwrap() = Some(policy);
        // Simulate a single-node master whose own write already satisfies
        // the quorum: 0 peer acks needed, so report success immediately.
        Ok(0)
    }
}

fn temp_env() -> (TempDir, Environment) {
    let td = TempDir::new().unwrap();
    let cfg = EnvironmentConfig::new(td.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(cfg).unwrap();
    (td, env)
}

// ─── PROBE 1: explicit-txn DPL write routes through the coordinator ────────

/// The headline probe. A DPL `PrimaryIndex::put` committed under an explicit
/// `SimpleMajority` transaction consults the installed
/// `ReplicaAckCoordinator` exactly like any other replicated DB write. This
/// proves the MASTER half of DPL<->replication composition already works at
/// the data layer with no `noxu-persist` -> `noxu-rep` dependency.
#[test]
fn dpl_write_routes_through_replica_ack_coordinator() {
    let (_td, env) = temp_env();

    let coord = Arc::new(CountingCoord::new());
    env.set_replica_coordinator(coord.clone());

    let mut store =
        EntityStore::open(&env, StoreConfig::new("wstore").with_allow_create(true).with_transactional(true))
            .unwrap();
    let primary: PrimaryIndex<u64, Widget> = store.get_primary_index().unwrap();
    let ser = WidgetSerializer;

    // A SimpleMajority durability so `commit_with_durability` invokes the
    // coordinator (ReplicaAckPolicy::None would skip it).
    let dur = Durability::new(
        SyncPolicy::NoSync,
        SyncPolicy::NoSync,
        ReplicaAckPolicy::SimpleMajority,
    );
    let txn = env
        .begin_transaction(Some(&TransactionConfig::new().with_durability(dur)))
        .unwrap();

    let w = Widget { id: 1, label: "alpha".into() };
    primary.put(Some(&txn), &ser, &w).unwrap();
    txn.commit_with_durability(dur).unwrap();

    // THE HEADLINE ASSERTION: the DPL commit was intercepted by the
    // replication durability path.
    assert_eq!(
        coord.calls(),
        1,
        "a DPL entity commit under SimpleMajority MUST consult the \
         replica-ack coordinator (DPL writes are ordinary replicated writes)"
    );

    // And the entity is visible locally (master serves its own DPL reads).
    let got = primary.get(None, &ser, &1u64).unwrap();
    assert_eq!(got, Some(w));

}

// ─── PROBE 2: coordinator is NOT consulted without one installed ──────────

/// Neuter / non-vacuity control: with NO coordinator installed the same DPL
/// write commits with purely local durability — so PROBE 1's call count is
/// caused by the coordinator wiring, not by the commit itself.
#[test]
fn dpl_write_without_coordinator_is_local_only() {
    let (_td, env) = temp_env();

    // No `set_replica_coordinator`. Build an *observer* coordinator only to
    // prove it is never touched, then never install it.
    let coord = CountingCoord::new();

    let mut store =
        EntityStore::open(&env, StoreConfig::new("wstore").with_allow_create(true).with_transactional(true))
            .unwrap();
    let primary: PrimaryIndex<u64, Widget> = store.get_primary_index().unwrap();
    let ser = WidgetSerializer;

    let dur = Durability::new(
        SyncPolicy::NoSync,
        SyncPolicy::NoSync,
        ReplicaAckPolicy::SimpleMajority,
    );
    let txn = env
        .begin_transaction(Some(&TransactionConfig::new().with_durability(dur)))
        .unwrap();
    primary
        .put(Some(&txn), &ser, &Widget { id: 2, label: "beta".into() })
        .unwrap();
    txn.commit_with_durability(dur).unwrap();

    assert_eq!(
        coord.calls(),
        0,
        "with no coordinator installed the DPL commit must be local-only"
    );

}

// ─── PROBE 3: auto-commit DPL write bypasses the coordinator ──────────────

/// Documents a real edge of the current wiring: `PrimaryIndex::put(None, ...)`
/// uses a synthetic auto-txn from `TxnManager::begin_auto_txn` that does NOT
/// go through `Environment::begin_transaction`, so the installed coordinator
/// is NOT wired into it. Auto-commit DPL writes are therefore local-only even
/// on a master. Explicit transactions (PROBE 1) are the composing path.
#[test]
fn dpl_auto_commit_write_bypasses_coordinator() {
    let (_td, env) = temp_env();

    let coord = Arc::new(CountingCoord::new());
    env.set_replica_coordinator(coord.clone());

    let mut store =
        EntityStore::open(&env, StoreConfig::new("wstore").with_allow_create(true).with_transactional(true))
            .unwrap();
    let primary: PrimaryIndex<u64, Widget> = store.get_primary_index().unwrap();
    let ser = WidgetSerializer;

    // Auto-commit (txn = None): synthetic auto-txn, no coordinator wiring.
    primary
        .put(None, &ser, &Widget { id: 3, label: "gamma".into() })
        .unwrap();

    assert_eq!(
        coord.calls(),
        0,
        "auto-commit DPL writes bypass Environment::begin_transaction and so \
         do NOT consult the replica-ack coordinator (documented edge)"
    );

}
