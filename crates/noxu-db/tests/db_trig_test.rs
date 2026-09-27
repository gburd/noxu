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
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, Trigger,
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

// JE: InvokeTest.testKVOpsTrans (put insert old=None, put update old=Some,
// delete old=Some, then commit) — record + txn trigger core.
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
    let txn_id = txn.id();

    // Insert: oldData = None, newData = "v1".  JE Trigger.put insert path.
    db.put_in(&txn, ent(b"k"), ent(b"v1")).unwrap();
    // Update: oldData = Some("v1"), newData = "v2".  JE Trigger.put update path.
    db.put_in(&txn, ent(b"k"), ent(b"v2")).unwrap();
    // Delete: oldData = Some("v2").  JE Trigger.delete path.
    db.delete_in(&txn, ent(b"k")).unwrap();

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

// JE: InvokeTest.verifyPut / verifyCommit ordering — put fires within the
// txn (Cursor.putNotify) before TransactionTrigger.commit on resolution.
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
    db.put_in(&txn, ent(b"k"), ent(b"v")).unwrap();

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

// JE: InvokeTest.testKVOpsAbort (verifyAbort(1)) — TransactionTrigger.abort
// fires on abort and the record change rolls back with the txn.
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
    let txn_id = txn.id();
    db.put_in(&txn, ent(b"k"), ent(b"v")).unwrap();
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
    assert!(
        !(db.get_into(None, ent(b"k"), &mut data).unwrap()),
        "aborted put must leave no record"
    );
}

// ───────────────────────────────────────────────────────────────────────────
// HEADLINE 4 — multiple triggers fire in registration order.
// ───────────────────────────────────────────────────────────────────────────

// JE: InvokeTest — triggers stored in a List<Trigger> and fired in list
// (registration) order by TriggerManager.runPutTriggers/runCommitTriggers.
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
    db.put_in(&txn, ent(b"k"), ent(b"v")).unwrap();
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
    db.put_in(&txn, ent(b"k"), ent(b"v")).unwrap();
    db.put_in(&txn, ent(b"k"), ent(b"v2")).unwrap();
    db.delete_in(&txn, ent(b"k")).unwrap();
    txn.commit().unwrap();

    // Data path is unaffected: a fresh put round-trips.
    db.put(ent(b"x"), ent(b"y")).unwrap();
    let mut data = DatabaseEntry::new();
    assert!(db.get_into(None, ent(b"x"), &mut data).unwrap());
    assert_eq!(data.data(), b"y");
}

// ───────────────────────────────────────────────────────────────────────────
// Extra — auto-commit (non-transactional) put fires put with txn=None and
// no commit trigger (no explicit txn handle to note).  JE: trigger txn arg is
// null when non-transactional; auto-commit commits immediately.
// ───────────────────────────────────────────────────────────────────────────

// JE: InvokeTest.testKVOpsAuto (partial) — put fires with a null/None txn
// arg under auto-commit (KVOps(null)).
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

    db.put(ent(b"k"), ent(b"v")).unwrap();

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

// ───────────────────────────────────────────────────────────────────────────
// JE: InvokeTest.testKVOpsTrans / testKVOpsAuto / testKVOpsAbort.
//
// Faithful port of InvokeTest.KVOps(transaction):
//   put(k, data1)         -> verifyPut(1, key, data1, null)   insert: old=None
//   put(k, data2)         -> verifyPut(1, key, data2, data1)  update: old=Some
//   delete(k) == SUCCESS  -> verifyDelete(1, key, data2)
//   delete(k) == NOTFOUND -> verifyDelete(0, null, null)      no trigger fires
// with a resetTriggers() between each step so counts are per-operation.
// The Trans / Auto variants differ only in the txn handle (Some vs None);
// the Abort variant runs the same KVOps under an explicit txn and then
// aborts, asserting TransactionTrigger.abort fires once (verifyAbort(1)).
// ───────────────────────────────────────────────────────────────────────────

/// Run the JE KVOps step sequence against `db` under the given (optional)
/// transaction, asserting the per-step put/delete trigger arguments exactly as
/// JE verifyPut / verifyDelete do.  `txn` is the Noxu handle (None = auto-commit,
/// i.e. JE KVOps(null)); `expect_txn` is the id the trigger should observe.
fn run_kvops(
    db: &noxu_db::Database,
    calls: &Arc<Mutex<Vec<Call>>>,
    txn: Option<&noxu_db::Transaction>,
    expect_txn: Option<u64>,
) {
    let put = |db: &noxu_db::Database, k: &[u8], v: &[u8]| match txn {
        Some(t) => db.put_in(t, ent(k), ent(v)).unwrap(),
        None => {
            db.put(ent(k), ent(v)).unwrap();
        }
    };
    let del = |db: &noxu_db::Database, k: &[u8]| -> bool {
        match txn {
            Some(t) => db.delete_in(t, ent(k)).unwrap(),
            None => db.delete(ent(k)).unwrap(),
        }
    };
    let reset = || calls.lock().unwrap().clear();

    // Nothing fired yet.  JE verifyPut(0, null, null, null).
    assert!(calls.lock().unwrap().is_empty());

    // Insert: oldData = None.  JE verifyPut(1, key, data1, null).
    put(db, b"k", b"\x02");
    assert_eq!(
        *calls.lock().unwrap(),
        vec![Call::Put {
            txn: expect_txn,
            key: b"k".to_vec(),
            old: None,
            new: b"\x02".to_vec(),
        }]
    );
    reset();

    // Update: oldData = Some(data1).  JE verifyPut(1, key, data2, data1).
    put(db, b"k", b"\x03");
    assert_eq!(
        *calls.lock().unwrap(),
        vec![Call::Put {
            txn: expect_txn,
            key: b"k".to_vec(),
            old: Some(b"\x02".to_vec()),
            new: b"\x03".to_vec(),
        }]
    );
    reset();

    // Delete SUCCESS: oldData = Some(data2).  JE verifyDelete(1, key, data2).
    assert!(del(db, b"k"), "first delete must succeed");
    assert_eq!(
        *calls.lock().unwrap(),
        vec![Call::Delete {
            txn: expect_txn,
            key: b"k".to_vec(),
            old: b"\x03".to_vec(),
        }]
    );
    reset();

    // Delete NOTFOUND: no record removed => NO delete trigger fires.
    // JE: status == NOTFOUND, verifyDelete(0, null, null).  This is the
    // vacuity guard — a hook that fired on a no-op delete would fail here.
    assert!(!del(db, b"k"), "second delete must find nothing");
    assert!(
        calls.lock().unwrap().is_empty(),
        "delete of an absent key must NOT fire a delete trigger"
    );
}

#[test]
fn kvops_trans() {
    let dir = TempDir::new().unwrap();
    let e = env(&dir);
    let (trig, calls) = Recorder::new("rec");
    let cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_trigger(trig);
    let db = e.open_database(None, "kv_t", &cfg).unwrap();

    let txn = e.begin_transaction(None).unwrap();
    let txn_id = txn.id();
    run_kvops(&db, &calls, Some(&txn), Some(txn_id));
    txn.commit().unwrap();
}

#[test]
fn kvops_auto() {
    let dir = TempDir::new().unwrap();
    let e = env(&dir);
    let (trig, calls) = Recorder::new("rec");
    let cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_trigger(trig);
    let db = e.open_database(None, "kv_a", &cfg).unwrap();

    // JE KVOps(null): auto-commit, trigger observes a null (None) txn arg.
    run_kvops(&db, &calls, None, None);
}

#[test]
fn kvops_abort() {
    let dir = TempDir::new().unwrap();
    let e = env(&dir);
    let (trig, calls) = Recorder::new("rec");
    let cfg = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_trigger(trig);
    let db = e.open_database(None, "kv_ab", &cfg).unwrap();

    let txn = e.begin_transaction(None).unwrap();
    let txn_id = txn.id();
    run_kvops(&db, &calls, Some(&txn), Some(txn_id));

    // The record ops fired within the txn; now abort.  JE verifyAbort(1):
    // TransactionTrigger.abort fires exactly once for the modified database.
    calls.lock().unwrap().clear();
    txn.abort().unwrap();
    assert_eq!(*calls.lock().unwrap(), vec![Call::Abort(txn_id)]);

    // And the record change rolled back with the txn: the key is gone.
    let mut data = DatabaseEntry::new();
    assert!(
        !db.get_into(None, ent(b"k"), &mut data).unwrap(),
        "aborted KVOps must leave no record"
    );
}
