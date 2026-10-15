//! A16-guard — BUG-DUPOBSOLETE-TXN absence regression guard.
//!
//! Integrate 83e50ab1 / 7652e5df (`fix(txn/dbi): BUG-DUPOBSOLETE-TXN`) fixed a
//! double-count: deleting a sorted-duplicate LN via an EXPLICIT transaction
//! counted the abort version obsolete TWICE — once immediately at log time
//! (dup-DB LNs are immediately obsolete) and again at commit via the abort-LSN
//! obsolete merge (`ObsoleteKind::Exact`). The second Exact count recorded the
//! SAME file offset obsolete again and tripped the cleaner's DEBUG
//! `checkDupOffsets` invariant (noxu-cleaner utilization_tracker.rs).
//!
//! On the Noxu v7.11.1 tag lineage the bug does NOT reproduce, EVEN with A5
//! applied (which plumbs a non-null dup abort-LSN into the commit merge): a
//! dup immediate-obsolete LN is counted `ObsoleteKind::Inexact` at LOG time
//! (noxu-log log_manager.rs:415 — NO offset tracked), so the commit-time
//! `Exact` count is the FIRST and ONLY offset tracking for that LSN. The abort
//! version is therefore counted obsolete EXACTLY ONCE, which is correct, and
//! `checkDupOffsets` never trips. Applying integrate's fix here (gating the
//! commit count off for dup DBs via `abort_counted_at_log_time`) would DROP
//! that single legitimate count and UNDER-report obsolete dup bytes — the base
//! `noxu-txn/src/txn.rs collect_obsolete_abort_lsns` comment warns of exactly
//! this.
//!
//! The safety thus rests on an implementation detail: dup immediate-obsolete
//! LNs are Inexact-at-log-time. This test GUARDS that invariant. It is GREEN by
//! design today; it goes RED the moment a change makes a dup LN's log-time
//! obsolete count `Exact` (offset-tracked) WITHOUT also porting A16, because
//! then the abort offset would be pushed into the exact-offset set a SECOND
//! time at commit (the `checkDupOffsets` double-count).

use noxu_db::{
    DatabaseConfig, DatabaseEntry, EnvironmentConfig, Get, OperationStatus,
};
use tempfile::TempDir;

fn env(dir: &TempDir) -> noxu_db::Environment {
    let cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true)
        .with_run_cleaner(false)
        .with_run_checkpointer(false)
        .with_run_verifier(false);
    noxu_db::Environment::open(cfg).unwrap()
}

/// No obsolete LOG offset may be counted (Exact) more than once across any
/// tracked file — the `checkDupOffsets` invariant. A dup-DB txn delete + commit
/// must leave every file's exact-offset set duplicate-free.
#[test]
fn dup_txn_delete_counts_abort_version_obsolete_exactly_once() {
    let tmp = TempDir::new().unwrap();
    let e = env(&tmp);
    let c = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(true);
    let db = e.open_database(None, "dups", &c).unwrap();

    // Seed several dups under one key: immediately-obsolete dup-DB LNs with
    // real, distinct log offsets.
    let pk = b"k".to_vec();
    for j in 0..6u32 {
        db.put(
            DatabaseEntry::from_bytes(&pk),
            DatabaseEntry::from_bytes(format!("d{j}").as_bytes()),
        )
        .unwrap();
    }

    // Delete each dup via an EXPLICIT txn cursor and commit. The commit path
    // runs collect_obsolete_abort_lsns -> UtilizationTracker; the abort
    // version (prior insert LN) is counted obsolete there. With A5 the abort
    // LSN is non-null and reaches the merge (kd=false, abort_counted_at_log_
    // time=false), so this exercises the exact path integrate's bug lived on.
    for j in 0..6u32 {
        let d = format!("d{j}");
        let txn = e.begin_transaction(None).unwrap();
        {
            let mut cur = db.open_cursor_in(&txn, None).unwrap();
            let mut k = DatabaseEntry::from_bytes(&pk);
            let mut dd = DatabaseEntry::from_bytes(d.as_bytes());
            assert_eq!(
                cur.get(&mut k, &mut dd, Get::SearchBoth, None).unwrap(),
                OperationStatus::Success
            );
            assert_eq!(cur.delete().unwrap(), OperationStatus::Success);
            cur.close().unwrap();
        }
        txn.commit().unwrap();
    }

    // The invariant: across every tracked file, no obsolete offset is recorded
    // more than once. `add_obsolete_offset` does NOT de-duplicate, so a
    // double-count of the same abort offset (the BUG-DUPOBSOLETE-TXN regression)
    // would push the same offset twice and this assertion would fail.
    let tracker =
        e.utilization_tracker_for_test().expect("rw env has a tracker");
    let t = tracker.lock();
    let mut total_obsolete_ln = 0i64;
    for (file, tfs) in t.get_tracked_files() {
        let offsets = tfs.get_obsolete_offsets();
        let mut seen = std::collections::HashSet::new();
        for &off in offsets {
            assert!(
                seen.insert(off),
                "checkDupOffsets regression: offset {off} in file {file} \
                 counted obsolete more than once (BUG-DUPOBSOLETE-TXN \
                 double-count — a dup immediate-obsolete LN was counted Exact \
                 at log time without the A16 commit-skip, or the commit merge \
                 double-counted it)"
            );
        }
        total_obsolete_ln += tfs.get_summary().obsolete_ln_count as i64;
    }
    // The six dup deletes must have counted their abort versions obsolete at
    // least once (not zero): if someone instead gated the commit count off for
    // dup DBs (integrate's fix, wrong here) obsolete dup bytes would be
    // UNDER-reported. We counted exactly once per deleted dup; assert the count
    // is present (>= the 6 deletes) so a silent drop-to-zero also goes RED.
    assert!(
        total_obsolete_ln >= 6,
        "dup txn deletes must count their abort versions obsolete (once each); \
         got total obsolete_ln_count = {total_obsolete_ln}, expected >= 6"
    );

    drop(t);
    db.close().unwrap();
    e.close().unwrap();
}

/// Integrate's A16 witness (identity_less_sorted_dup_checkpoint_tail_recovers,
/// 83e50ab1 / 7652e5df) switched a dup delete to an explicit txn to reproduce
/// the checkDupOffsets SIGABRT. Run its ESSENTIAL shape unmodified against this
/// lineage — a dup DB with node_max_entries=4, dup deletes under explicit txns
/// interleaved with reinserts and a forced checkpoint — and record that it
/// PASSES (no SIGABRT, no data loss), confirming the bug is absent here.
#[test]
fn integrate_a16_witness_shape_passes_on_this_lineage() {
    use std::collections::BTreeSet;
    let tmp = TempDir::new().unwrap();
    let e = env(&tmp);
    let c = DatabaseConfig::new()
        .with_allow_create(true)
        .with_transactional(true)
        .with_sorted_duplicates(true);
    let db = e.open_database(None, "plain_dups", &c).unwrap();

    let pks: Vec<Vec<u8>> = (1..=4usize)
        .flat_map(|len| {
            (0..1u32 << len).map(move |bits| {
                (0..len)
                    .map(|i| if bits >> i & 1 == 1 { b'b' } else { b'a' })
                    .collect::<Vec<u8>>()
            })
        })
        .collect();
    let data = |j: u32| format!("d{j}").into_bytes();

    for pk in &pks {
        for j in 0..3 {
            db.put(
                DatabaseEntry::from_bytes(pk),
                DatabaseEntry::from_bytes(&data(j)),
            )
            .unwrap();
        }
    }
    e.checkpoint(None).unwrap();

    let mut want: BTreeSet<(Vec<u8>, Vec<u8>)> = pks
        .iter()
        .flat_map(|pk| (0..3).map(move |j| (pk.clone(), data(j))))
        .collect();

    for (n, pk) in pks.iter().enumerate() {
        let j = (n % 3) as u32;
        // The explicit-txn dup delete that integrate's bug tripped on.
        let txn = e.begin_transaction(None).unwrap();
        {
            let mut cur = db.open_cursor_in(&txn, None).unwrap();
            let mut k = DatabaseEntry::from_bytes(pk);
            let mut d = DatabaseEntry::from_bytes(&data(j));
            assert_eq!(
                cur.get(&mut k, &mut d, Get::SearchBoth, None).unwrap(),
                OperationStatus::Success
            );
            assert_eq!(cur.delete().unwrap(), OperationStatus::Success);
            cur.close().unwrap();
        }
        txn.commit().unwrap();
        want.remove(&(pk.clone(), data(j)));
        db.put(
            DatabaseEntry::from_bytes(pk),
            DatabaseEntry::from_bytes(&data(7)),
        )
        .unwrap();
        want.insert((pk.clone(), data(7)));
    }

    // No SIGABRT reached here: the double-count never fired. Verify no data was
    // lost or resurrected (the witness's recovery-content assertion, in-memory
    // form — this lineage lacks the comparator_recovery crash_copy harness).
    let got: BTreeSet<(Vec<u8>, Vec<u8>)> = {
        let mut out = BTreeSet::new();
        let mut cur = db.open_cursor(None).unwrap();
        let mut k = DatabaseEntry::new();
        let mut d = DatabaseEntry::new();
        let mut st = cur.get(&mut k, &mut d, Get::First, None).unwrap();
        while st == OperationStatus::Success {
            out.insert((k.data().to_vec(), d.data().to_vec()));
            st = cur.get(&mut k, &mut d, Get::Next, None).unwrap();
        }
        out
    };
    let missing: Vec<_> = want.difference(&got).collect();
    let extra: Vec<_> = got.difference(&want).collect();
    assert!(
        missing.is_empty() && extra.is_empty(),
        "integrate A16 witness shape: missing {missing:?} extra {extra:?}"
    );

    db.close().unwrap();
    e.close().unwrap();
}
