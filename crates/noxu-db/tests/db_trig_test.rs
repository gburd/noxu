//! DB-TRIG — database / transaction triggers.
//!
//! Port of JE `com.sleepycat.je.trigger.Trigger` + `TransactionTrigger`,
//! fired by `TriggerManager.runPutTriggers` / `runDeleteTriggers` /
//! `runCommitTriggers` / `runAbortTriggers`.
//!
//! Headline tests:
//!  1. A `Trigger` registered on a DB sees `put(key, oldData, newData)` for an
//!     insert (oldData=None) and an update (oldData=Some(prev)), and
//!     `delete(key, oldData)` for a delete, all within the txn.
//!  2. The trigger fires BEFORE commit (calls observable after a put, before
//!     the txn commits).
//!  3. On abort, `TransactionTrigger.abort` fires and the data change is
//!     rolled back with the txn.
//!  4. Multiple triggers fire in registration order.
//!  5. No trigger registered => unchanged behaviour (zero firing).

use std::sync::{Arc, Mutex};

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    OperationStatus, Trigger,
};
use tempfile::TempDir;

/// A trigger that records every call it receives, in order, for assertions.
#[derive(Debug, Clone, PartialEq)]
enum Call {
    Put { txn: Option<u64>, key: Vec<u8>, old: Option<Vec<u8>>, new: Vec<u8> },
    Delete { txn: Option<u64>, key: Vec<u8>, old: Vec<u8> },
    Commit(u64),
    Abort(u64),
}

struct Recorder {
    name: String,
    calls: Arc<Mutex<Vec<Call>>>,
}

impl Recorder {
    fn new(name: &str) -> (Arc<Recorder>, Arc<Mutex<Vec<Call>>>) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let r =
            Arc::new(Recorder { name: name.to_string(), calls: calls.clone() });
        (r, calls)
    }
}

impl Trigger for Recorder {
    fn name(&self) -> &str {
        &self.name
    }
    fn put(
        &self,
        txn_id: Option<u64>,
        key: &[u8],
        old_data: Option<&[u8]>,
        new_data: &[u8],
    ) {
        self.calls.lock().unwrap().push(Call::Put {
            txn: txn_id,
            key: key.to_vec(),
            old: old_data.map(<[u8]>::to_vec),
            new: new_data.to_vec(),
        });
    }
    fn delete(&self, txn_id: Option<u64>, key: &[u8], old_data: &[u8]) {
        self.calls.lock().unwrap().push(Call::Delete {
            txn: txn_id,
            key: key.to_vec(),
            old: old_data.to_vec(),
        });
    }
    fn commit(&self, txn_id: u64) {
        self.calls.lock().unwrap().push(Call::Commit(txn_id));
    }
    fn abort(&self, txn_id: u64) {
        self.calls.lock().unwrap().push(Call::Abort(txn_id));
    }
}

fn env(dir: &TempDir) -> Environment {
    let cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    Environment::open(cfg).unwrap()
}

fn ent(b: &[u8]) -> DatabaseEntry {
    DatabaseEntry::from_bytes(b)
}

// ───────────────────────────────────────────────────────────────────────────
// HEADLINE 1 — put(insert: old=None), put(update: old=Some), delete(old).
// ───────────────────────────────────────────────────────────────────────────

#[test]
fn headline1_put_delete_old_new_within_txn() {
    let dir = TempDir::new().unwrap();
    let e = env(&dir);
    let (trig, calls) = Recorder::new("rec");
    let cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_trigger(trig);
    let db = e.open_database(None, "t1", &cfg).unwrap();

    let txn = e.begin_transaction(None).unwrap();
    let txn_id = txn.get_id();

    // Insert: oldData = None, newData = "v1".  JE Trigger.put insert path.
    db.put_in(&txn, &ent(b"k"), &ent(b"v1")).unwrap();
    // Update: oldData = Some("v1"), newData = "v2".  JE Trigger.put update path.
    db.put_in(&txn, &ent(b"k"), &ent(b"v2")).unwrap();
    // Delete: oldData = Some("v2").  JE Trigger.delete path.
    db.delete_in(&txn, &ent(b"k")).unwrap();

    txn.commit().unwrap();

    let c = calls.lock().unwrap().clone();
    assert_eq!(
        c,
        vec![
            Call::Put {
                txn: Some(txn_id),
                key: b"k".to_vec(),
                old: None,
                new: b"v1".to_vec(),
            },
            Call::Put {
                txn: Some(txn_id),
                key: b"k".to_vec(),
                old: Some(b"v1".to_vec()),
                new: b"v2".to_vec(),
            },
            Call::Delete {
                txn: Some(txn_id),
                key: b"k".to_vec(),
                old: b"v2".to_vec(),
            },
            // Commit fires last, once, on resolution.
            Call::Commit(txn_id),
        ]
    );
}

// ───────────────────────────────────────────────────────────────────────────
// HEADLINE 2 — put trigger fires BEFORE commit.
// ───────────────────────────────────────────────────────────────────────────

#[test]
fn headline2_put_trigger_fires_before_commit() {
    let dir = TempDir::new().unwrap();
    let e = env(&dir);
    let (trig, calls) = Recorder::new("rec");
    let cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_trigger(trig);
    let db = e.open_database(None, "t2", &cfg).unwrap();

    let txn = e.begin_transaction(None).unwrap();
    db.put_in(&txn, &ent(b"k"), &ent(b"v")).unwrap();

    // Asserted AFTER the put but BEFORE commit: the put trigger has already
    // fired, and no commit trigger has fired yet.  JE fires put within the
    // transaction (Cursor.putNotify), commit on resolution.
    {
        let c = calls.lock().unwrap();
        assert_eq!(c.len(), 1, "put trigger must have fired before commit");
        assert!(matches!(c[0], Call::Put { .. }));
    }

    txn.commit().unwrap();
    let c = calls.lock().unwrap();
    assert_eq!(c.len(), 2);
    assert!(matches!(c[1], Call::Commit(_)));
}

// ───────────────────────────────────────────────────────────────────────────
// HEADLINE 3 — abort fires TransactionTrigger.abort; data change rolled back.
// ───────────────────────────────────────────────────────────────────────────

#[test]
fn headline3_abort_fires_and_rolls_back() {
    let dir = TempDir::new().unwrap();
    let e = env(&dir);
    let (trig, calls) = Recorder::new("rec");
    let cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_trigger(trig);
    let db = e.open_database(None, "t3", &cfg).unwrap();

    let txn = e.begin_transaction(None).unwrap();
    let txn_id = txn.get_id();
    db.put_in(&txn, &ent(b"k"), &ent(b"v")).unwrap();
    // The put trigger fired within the txn (it saw the change)...
    assert!(matches!(calls.lock().unwrap().last(), Some(Call::Put { .. })));

    txn.abort().unwrap();

    // ...and on abort, TransactionTrigger.abort fires.
    let c = calls.lock().unwrap().clone();
    assert_eq!(c.last(), Some(&Call::Abort(txn_id)));
    // No commit trigger fired.
    assert!(!c.iter().any(|x| matches!(x, Call::Commit(_))));

    // The data change is rolled back with the txn: the record is gone.
    let mut data = DatabaseEntry::new();
    assert!(!(db.get_into(None, &ent(b"k"), &mut data).unwrap()),
        "aborted put must leave no record"
    );
}

// ───────────────────────────────────────────────────────────────────────────
// HEADLINE 4 — multiple triggers fire in registration order.
// ───────────────────────────────────────────────────────────────────────────

#[test]
fn headline4_multiple_triggers_fire_in_registration_order() {
    let dir = TempDir::new().unwrap();
    let e = env(&dir);
    // Shared call log: each trigger records its own name so we can read the
    // firing order off one timeline.
    let order = Arc::new(Mutex::new(Vec::<String>::new()));

    struct OrderTrig {
        name: String,
        order: Arc<Mutex<Vec<String>>>,
    }
    impl Trigger for OrderTrig {
        fn name(&self) -> &str {
            &self.name
        }
        fn put(
            &self,
            _t: Option<u64>,
            _k: &[u8],
            _o: Option<&[u8]>,
            _n: &[u8],
        ) {
            self.order.lock().unwrap().push(format!("put:{}", self.name));
        }
        fn delete(&self, _t: Option<u64>, _k: &[u8], _o: &[u8]) {}
        fn commit(&self, _t: u64) {
            self.order.lock().unwrap().push(format!("commit:{}", self.name));
        }
    }

    let a: Arc<dyn Trigger> =
        Arc::new(OrderTrig { name: "A".into(), order: order.clone() });
    let b: Arc<dyn Trigger> =
        Arc::new(OrderTrig { name: "B".into(), order: order.clone() });
    let cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_trigger(a) // registered first
        .with_trigger(b); // registered second
    let db = e.open_database(None, "t4", &cfg).unwrap();

    let txn = e.begin_transaction(None).unwrap();
    db.put_in(&txn, &ent(b"k"), &ent(b"v")).unwrap();
    txn.commit().unwrap();

    let o = order.lock().unwrap().clone();
    // Put fires A then B (registration order); commit then fires A then B.
    assert_eq!(o, vec!["put:A", "put:B", "commit:A", "commit:B"]);
}

// ───────────────────────────────────────────────────────────────────────────
// HEADLINE 5 — no trigger registered => unchanged behaviour, zero firing.
// ───────────────────────────────────────────────────────────────────────────

#[test]
fn headline5_no_trigger_unchanged_behaviour() {
    let dir = TempDir::new().unwrap();
    let e = env(&dir);
    // No .with_trigger() — the no-trigger fast path.
    let cfg =
        DatabaseConfig::new().with_allow_create(true).with_transactional(true);
    let db = e.open_database(None, "t5", &cfg).unwrap();

    let txn = e.begin_transaction(None).unwrap();
    db.put_in(&txn, &ent(b"k"), &ent(b"v")).unwrap();
    db.put_in(&txn, &ent(b"k"), &ent(b"v2")).unwrap();
    db.delete_in(&txn, &ent(b"k")).unwrap();
    txn.commit().unwrap();

    // Data path is unaffected: a fresh put round-trips.
    db.put( &ent(b"x"), &ent(b"y")).unwrap();
    let mut data = DatabaseEntry::new();
    assert!(db.get_into(None, &ent(b"x"), &mut data).unwrap());
    assert_eq!(data.data(), b"y");
}

// ───────────────────────────────────────────────────────────────────────────
// Extra — auto-commit (non-transactional) put fires put with txn=None and
// no commit trigger (no explicit txn handle to note).  JE: trigger txn arg is
// null when non-transactional; auto-commit commits immediately.
// ───────────────────────────────────────────────────────────────────────────

#[test]
fn auto_commit_put_fires_with_none_txn() {
    let dir = TempDir::new().unwrap();
    let e = env(&dir);
    let (trig, calls) = Recorder::new("rec");
    let cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_trigger(trig);
    let db = e.open_database(None, "auto", &cfg).unwrap();

    db.put( &ent(b"k"), &ent(b"v")).unwrap();

    let c = calls.lock().unwrap().clone();
    assert_eq!(
        c,
        vec![Call::Put {
            txn: None,
            key: b"k".to_vec(),
            old: None,
            new: b"v".to_vec(),
        }]
    );
}
