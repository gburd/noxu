#![forbid(unsafe_code)]
// Copyright (C) 2024-2025 Greg Burd.  Licensed under either of the
// Apache License, Version 2.0 or the MIT license, at your option.
// See LICENSE-APACHE and LICENSE-MIT at the root of this repository.
// SPDX-License-Identifier: Apache-2.0 OR MIT
//! XDB-711 catalog-integrity detector (fail-closed recovery guard).
//!
//! On Noxu DB 7.11.0 / 7.11.1 the checkpoint catalog relog
//! (`relog_live_catalog_impl`) could append a **non-transactional live
//! NameLN `name -> old_id`** from a stale `name_map` snapshot at a *higher*
//! LSN than a later **committed transactional** rename / remove / recreate of
//! that name. Recovery's latest-NameLN-per-name-wins replay then binds the
//! name to the stale id: one database's committed records surface under
//! another database's name, and the correct database's records are lost —
//! silently (recovery returns success and `verify` passes). See `xdb-711.md`
//! §4 and `xdb-711-fix.md` §1/§9.
//!
//! A writer-side fix (7.11.2) stops new damage but cannot repair logs already
//! written. JE never silently serves a catalog it cannot trust: it throws
//! `EnvironmentFailureException(LOG_INTEGRITY, …)` ("Log information is
//! incorrect, problem is likely persistent",
//! `dbi/EnvironmentFailureReason.java:73-75`; e.g. `dbi/DbTree.java:803-808`).
//! This module is the recovery-side detector that produces the same
//! fail-closed outcome: it returns the set of database names whose recovered
//! (mounted) name->id binding cannot be trusted, so the open path can refuse
//! with `EnvironmentFailureReason::LogIntegrity` instead of mounting wrong
//! data.
//!
//! The rule is a **specific signature of the damage, not a heuristic** — see
//! [`detect_xdb711_damage`] for the precise predicate and the exclusion of
//! every legitimate history (relog of a current binding, auto-commit DDL, a
//! truncated/torn tail, an in-doubt XA DDL, rename chains, recreate-same-name,
//! remove+recreate).

use hashbrown::{HashMap, HashSet};
use noxu_util::Lsn;

/// One catalog (NameLN / NameLNTxn) event observed during the analysis scan.
///
/// Recorded per database name in ascending-LSN order. This is the only new
/// per-NameLN bookkeeping the detector adds; the list length is bounded by the
/// number of catalog operations, never by data volume (requirement: a bounded
/// per-NameLN check, not a new full scan).
#[derive(Debug, Clone, Copy)]
pub struct CatalogEvent {
    /// LSN of the NameLN/NameLNTxn entry.
    pub lsn: Lsn,
    /// Database id this record binds the name to (meaningless when
    /// `is_deleted`, but preserved as logged).
    pub id: u64,
    /// `true` for a Remove / rename-away tombstone NameLN.
    pub is_deleted: bool,
    /// Creating transaction id, if the record was transactional
    /// (`NameLNTxn`). `None` for a non-transactional NameLN (relog,
    /// auto-commit, or legacy) — the relog writer's fingerprint.
    pub txn_id: Option<u64>,
}

/// A detected XDB-711 catalog-integrity violation for one database name.
#[derive(Debug, Clone)]
pub struct CatalogDamage {
    /// The mounted (recovered) database name whose binding is untrustworthy.
    pub name: String,
    /// The id the stray non-transactional relog NameLN bound it to (what
    /// recovery would have mounted).
    pub mounted_id: u64,
    /// The id the latest committed transactional operation authoritatively
    /// bound it to, or `None` if the latest committed op *removed* the name.
    pub committed_id: Option<u64>,
    /// LSN of the stray non-transactional NameLN that won recovery.
    pub stray_lsn: Lsn,
    /// LSN of the committed transactional authority it superseded.
    pub authority_lsn: Lsn,
}

/// Run the XDB-711 damage detector.
///
/// `mounted` is the settled latest-NameLN-wins catalog recovery is about to
/// mount (`AnalysisResult::recovered_db_names`). `events` is the per-name,
/// ascending-LSN catalog-event history accumulated during the analysis scan.
/// `committed_txns` / `aborted_txns` are the resolved transaction sets.
///
/// Returns the (possibly empty) list of damaged names. An empty list means
/// the recovered catalog is consistent with the committed transactional
/// history and recovery may proceed.
///
/// # The predicate (fires ⇔ XDB-711 damage)
///
/// For a mounted name `N` with `mounted[N] = F`, the detector reports damage
/// iff **all** hold:
///
/// 1. **The record that won recovery for `N` is a non-transactional live
///    NameLN** — `N`'s highest-LSN event `e_win` has `txn_id == None`,
///    `!is_deleted`, `id == F`. (A relog NameLN is always non-transactional;
///    an auto-commit/legacy create is too, but is excluded by (3).)
/// 2. **`N` has a committed-transactional authority** — at least one event for
///    `N` is committed-transactional (`txn_id = Some(t)`, `t ∈ committed`).
///    Its latest such event `e_auth` is `N`'s durable authority: a live bind
///    `N→T` or a tombstone (committed remove/rename-away).
/// 3. **The authority predates the stray and disagrees** — `e_auth.lsn <
///    e_win.lsn`, and (`e_auth` is a tombstone, or `e_auth` binds `N→T` with
///    `T ≠ F`), and **no committed-transactional live `N→F` exists at any
///    LSN** (if one did, recovery's binding would be legitimate).
///
/// (1)+(2)+(3) is exactly: *a non-transactional NameLN `N→F` won recovery at a
/// higher LSN than `N`'s latest committed-transactional authority, which says
/// `N→T≠F` or `N-removed`, and no committed op ever bound `N→F`.* That is the
/// stray-relog fingerprint and nothing else.
///
/// # Why each legitimate history is excluded
///
/// - **Relog of a current binding** — the relog re-emits `N→F` where `F` is
///   the committed id, so either `N` has no committed-txnal authority (auto
///   create — fails (2)) or the authority is a committed `N→F` (fails (3)'s
///   "no committed live N→F" / "T≠F").
/// - **Auto-commit DDL (non-txn NameLNs)** — never enters the committed-txnal
///   authority set, so (2) is not satisfied by them; they only *win* (1), and
///   without a disagreeing committed authority below them (3) fails.
/// - **Truncated / torn tail** — the detector fires only when the *surviving*
///   winner is a non-txn NameLN above a *surviving committed* disagreeing
///   authority; a tail that drops the stray relog leaves `mounted == C`, and a
///   tail that drops a not-yet-committed DDL leaves no authority in `C`.
/// - **In-doubt XA DDL** — its NameLNTxn's `txn_id` is not in `committed`
///   (prepared/active), so it never becomes an authority and never fires.
/// - **Rename chains / recreate-same-name / remove+recreate** — `C` follows
///   the latest committed binding; `mounted` matches it unless a stray non-txn
///   relog re-asserted a superseded id above it (which *is* the damage).
pub fn detect_xdb711_damage(
    mounted: &HashMap<String, u64>,
    events: &HashMap<String, Vec<CatalogEvent>>,
    committed_txns: &HashMap<u64, Lsn>,
    _aborted_txns: &HashSet<u64>,
) -> Vec<CatalogDamage> {
    let mut damaged: Vec<CatalogDamage> = Vec::new();

    for (name, &mounted_id) in mounted.iter() {
        let Some(hist) = events.get(name) else {
            // No catalog events for a mounted name would be anomalous, but it
            // is not the XDB-711 signature; leave it to other guards.
            continue;
        };

        // The record that WON recovery for this name is its highest-LSN
        // event (the forward scan is ascending-LSN latest-wins).  The events
        // list is appended in scan order, so the last element is the winner.
        let Some(e_win) = hist.last().copied() else {
            continue;
        };

        // (1) The winner must be a NON-transactional LIVE NameLN binding N→F.
        //     A relog NameLN is always non-transactional.  If the winner is
        //     transactional, or a tombstone, this is not the stray-relog
        //     shape.
        if e_win.txn_id.is_some() || e_win.is_deleted || e_win.id != mounted_id
        {
            continue;
        }

        // (2) Find N's latest COMMITTED-TRANSACTIONAL authority (the latest
        //     event whose txn committed).  No authority ⇒ not the signature
        //     (auto-commit / legacy / in-doubt only).
        let mut authority: Option<CatalogEvent> = None;
        for &ev in hist.iter() {
            let is_committed_txnal =
                matches!(ev.txn_id, Some(t) if committed_txns.contains_key(&t));
            if !is_committed_txnal {
                continue;
            }
            // Latest committed-txnal event (ascending scan ⇒ overwrite).
            authority = Some(ev);
        }

        let Some(e_auth) = authority else {
            // (2) fails: no committed-transactional authority for this name.
            continue;
        };

        // (3) The authority must PREDATE the stray winner and DISAGREE.  The
        //     *latest* committed authority is what the committed history says
        //     N resolves to; if it binds N→F, the mounted binding is
        //     legitimate (a relog of the current binding, or F is genuinely
        //     the live id) and we must NOT fire — even if an EARLIER
        //     committed op had bound N→F before a later committed op moved it
        //     away (that earlier bind is superseded, not authoritative).
        if e_auth.lsn >= e_win.lsn {
            // The committed authority is at/after the winner — the winner is
            // not a stray that superseded a committed op.  (Cannot normally
            // happen since the winner is the highest-LSN event, but checked
            // for safety: a committed-txnal winner would have failed (1).)
            continue;
        }
        let disagrees = e_auth.is_deleted || e_auth.id != mounted_id;
        if !disagrees {
            // Latest committed authority is a live N→F — agreement.
            continue;
        }

        damaged.push(CatalogDamage {
            name: name.clone(),
            mounted_id,
            committed_id: if e_auth.is_deleted {
                None
            } else {
                Some(e_auth.id)
            },
            stray_lsn: e_win.lsn,
            authority_lsn: e_auth.lsn,
        });
    }

    // Deterministic order for the error message / tests.
    damaged.sort_by(|a, b| a.name.cmp(&b.name));
    damaged
}

/// Corroborating injectivity check (xdb-711-fix.md §9 option 2), logged as
/// supporting evidence only — the primary gate is [`detect_xdb711_damage`].
///
/// Under the committed transactional history, each live id has exactly one
/// name. If the mounted catalog maps two distinct names to the same id, that
/// is a durable injectivity violation of the committed history (the deeper
/// defect-#2 form). Returns the colliding `(id, names…)` groups.
pub fn mounted_injectivity_violations(
    mounted: &HashMap<String, u64>,
) -> Vec<(u64, Vec<String>)> {
    let mut by_id: HashMap<u64, Vec<String>> = HashMap::new();
    for (name, &id) in mounted.iter() {
        by_id.entry(id).or_default().push(name.clone());
    }
    let mut out: Vec<(u64, Vec<String>)> = by_id
        .into_iter()
        .filter(|(_, names)| names.len() > 1)
        .map(|(id, mut names)| {
            names.sort();
            (id, names)
        })
        .collect();
    out.sort_by_key(|(id, _)| *id);
    out
}

/// Build the human-readable `LOG_INTEGRITY` message for a set of damaged
/// databases (JE message style; names the affected database(s) and the
/// recovery options). The message deliberately does **not** name
/// `noxu-admin scavenge`: that offline-salvage subcommand does not exist in
/// the 7.11.x release line (it is a v8 feature).
pub fn log_integrity_message(damaged: &[CatalogDamage]) -> String {
    let mut names: Vec<&str> =
        damaged.iter().map(|d| d.name.as_str()).collect();
    names.sort_unstable();
    names.dedup();
    let name_list =
        names.iter().map(|n| format!("'{n}'")).collect::<Vec<_>>().join(", ");
    format!(
        "database catalog is inconsistent — the log was written by a Noxu DB \
         7.11.0/7.11.1 version affected by XDB-711 and the recovered \
         name->id binding for database(s) [{name_list}] cannot be trusted (a \
         non-transactional catalog record binds the name to an id that a \
         later committed operation had already superseded). This problem is \
         persistent; recovery will not mount a catalog it knows is wrong. To \
         recover: restore this environment from a backup, or preserve this \
         environment directory (copy it as-is; do not delete or reinitialise \
         it) for offline salvage. This version refuses to open the \
         environment, so nothing further is written to it."
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lsn(n: u32) -> Lsn {
        Lsn::new(0, n)
    }

    fn ev(l: u32, id: u64, del: bool, txn: Option<u64>) -> CatalogEvent {
        CatalogEvent { lsn: lsn(l), id, is_deleted: del, txn_id: txn }
    }

    fn committed(ids: &[u64]) -> HashMap<u64, Lsn> {
        ids.iter().map(|&t| (t, lsn(1))).collect()
    }

    // ───────────────────────── DAMAGE (must fire) ─────────────────────────

    /// seed 167229452944 shape: rename delta→charlie (id 7, txn 91 committed),
    /// charlie later recreated under its own id (txn 150 committed), then a
    /// STRAY non-txn relog re-binds charlie→7 at a higher LSN.
    #[test]
    fn fires_on_stray_relog_after_committed_rebind() {
        let mut events = HashMap::new();
        events.insert(
            "charlie".to_string(),
            vec![
                ev(0x1f78, 7, false, Some(91)), // committed rename-in
                ev(0x4100, 99, false, Some(150)), // committed recreate → id 99
                ev(0x5339, 7, false, None),     // STRAY relog → id 7
            ],
        );
        let mut mounted = HashMap::new();
        mounted.insert("charlie".to_string(), 7u64); // latest-wins picked stray
        let d = detect_xdb711_damage(
            &mounted,
            &events,
            &committed(&[91, 150]),
            &HashSet::new(),
        );
        assert_eq!(d.len(), 1, "must fire");
        assert_eq!(d[0].name, "charlie");
        assert_eq!(d[0].mounted_id, 7);
        assert_eq!(d[0].committed_id, Some(99));
    }

    /// Stray relog re-binds a name the committed history REMOVED
    /// (rename-away / remove). (seed 193773810554 / 347731084692 shape:
    /// charlie removed, but a stray non-txn relog resurrects it on a stale id.)
    #[test]
    fn fires_on_stray_relog_after_committed_remove() {
        let mut events = HashMap::new();
        events.insert(
            "charlie".to_string(),
            vec![
                ev(0x100, 7, false, Some(10)), // committed create
                ev(0x200, 7, true, Some(20)),  // committed remove (tombstone)
                ev(0x900, 7, false, None),     // STRAY relog resurrects charlie
            ],
        );
        let mut mounted = HashMap::new();
        mounted.insert("charlie".to_string(), 7u64);
        let d = detect_xdb711_damage(
            &mounted,
            &events,
            &committed(&[10, 20]),
            &HashSet::new(),
        );
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].committed_id, None, "committed op removed the name");
    }

    // ────────────────────── LEGITIMATE (must NOT fire) ─────────────────────

    /// A relog of the CURRENT binding: non-txn NameLN re-emits N→F where F is
    /// the committed id.
    #[test]
    fn excludes_relog_of_current_binding() {
        let mut events = HashMap::new();
        events.insert(
            "alpha".to_string(),
            vec![
                ev(0x100, 5, false, Some(10)), // committed create → 5
                ev(0x900, 5, false, None),     // relog of the CURRENT binding
            ],
        );
        let mut mounted = HashMap::new();
        mounted.insert("alpha".to_string(), 5u64);
        let d = detect_xdb711_damage(
            &mounted,
            &events,
            &committed(&[10]),
            &HashSet::new(),
        );
        assert!(d.is_empty(), "relog of current binding must not fire");
    }

    /// Auto-commit DDL: only non-transactional NameLNs, no committed-txnal
    /// authority. (Also covers legacy pre-C6 non-txn creates.)
    #[test]
    fn excludes_auto_commit_ddl() {
        let mut events = HashMap::new();
        events.insert(
            "auto".to_string(),
            vec![
                ev(0x100, 8, false, None), // auto-commit create (non-txn)
                ev(0x900, 8, false, None), // relog of it (non-txn)
            ],
        );
        let mut mounted = HashMap::new();
        mounted.insert("auto".to_string(), 8u64);
        let d = detect_xdb711_damage(
            &mounted,
            &events,
            &committed(&[]),
            &HashSet::new(),
        );
        assert!(d.is_empty(), "auto-commit DDL must not fire");
    }

    /// A torn tail that drops the stray relog: the surviving winner is the
    /// committed binding itself → mounted == C.
    #[test]
    fn excludes_truncated_tail_dropping_stray() {
        let mut events = HashMap::new();
        events.insert(
            "beta".to_string(),
            vec![ev(0x100, 3, false, Some(10))], // only the committed create survives
        );
        let mut mounted = HashMap::new();
        mounted.insert("beta".to_string(), 3u64);
        let d = detect_xdb711_damage(
            &mounted,
            &events,
            &committed(&[10]),
            &HashSet::new(),
        );
        assert!(d.is_empty());
    }

    /// In-doubt XA DDL: a prepared-but-unresolved NameLNTxn (txn not in the
    /// committed set) is never an authority.
    #[test]
    fn excludes_in_doubt_xa_ddl() {
        let mut events = HashMap::new();
        // Committed create → 4, then a stray non-txn relog of the SAME id 4
        // (current), plus an in-doubt txn 77 that renamed it away but is NOT
        // committed. mounted stays 4 (the live committed id).
        events.insert(
            "gamma".to_string(),
            vec![
                ev(0x100, 4, false, Some(10)), // committed create
                ev(0x200, 4, true, Some(77)), // in-doubt remove (txn 77 NOT committed)
                ev(0x900, 4, false, None),    // relog of current id 4
            ],
        );
        let mut mounted = HashMap::new();
        mounted.insert("gamma".to_string(), 4u64);
        let d = detect_xdb711_damage(
            &mounted,
            &events,
            &committed(&[10]), // 77 absent ⇒ in-doubt
            &HashSet::new(),
        );
        assert!(d.is_empty(), "in-doubt XA DDL must not fire");
    }

    /// Rename chain a→b→c, all committed, no stray relog: latest committed
    /// binding wins cleanly.
    #[test]
    fn excludes_clean_rename_chain() {
        // name "c" is the live end of a chain that carried id 7.
        let mut events = HashMap::new();
        events.insert(
            "c".to_string(),
            vec![ev(0x300, 7, false, Some(30))], // committed rename-in to c
        );
        events.insert(
            "b".to_string(),
            vec![
                ev(0x200, 7, false, Some(20)), // rename a→b (in)
                ev(0x300, 7, true, Some(30)), // rename b→c (b tombstoned), committed
            ],
        );
        let mut mounted = HashMap::new();
        mounted.insert("c".to_string(), 7u64); // b removed from catalog
        let d = detect_xdb711_damage(
            &mounted,
            &events,
            &committed(&[20, 30]),
            &HashSet::new(),
        );
        assert!(d.is_empty(), "clean rename chain must not fire");
    }

    /// Recreate same name under a NEW id: create N→1, remove, create N→2, all
    /// committed, no stray relog.
    #[test]
    fn excludes_recreate_same_name() {
        let mut events = HashMap::new();
        events.insert(
            "n".to_string(),
            vec![
                ev(0x100, 1, false, Some(10)), // create → 1
                ev(0x200, 1, true, Some(20)),  // remove
                ev(0x300, 2, false, Some(30)), // recreate → 2
            ],
        );
        let mut mounted = HashMap::new();
        mounted.insert("n".to_string(), 2u64);
        let d = detect_xdb711_damage(
            &mounted,
            &events,
            &committed(&[10, 20, 30]),
            &HashSet::new(),
        );
        assert!(d.is_empty(), "recreate-same-name must not fire");
    }

    /// But a stray non-txn relog re-asserting the OLD id above a recreate IS
    /// XDB-711 (guards against the exclusion above hiding real damage).
    #[test]
    fn fires_on_recreate_then_stray_relog_old_id() {
        let mut events = HashMap::new();
        events.insert(
            "n".to_string(),
            vec![
                ev(0x100, 1, false, Some(10)),
                ev(0x200, 1, true, Some(20)),
                ev(0x300, 2, false, Some(30)), // committed recreate → 2
                ev(0x900, 1, false, None),     // STRAY relog → old id 1
            ],
        );
        let mut mounted = HashMap::new();
        mounted.insert("n".to_string(), 1u64);
        let d = detect_xdb711_damage(
            &mounted,
            &events,
            &committed(&[10, 20, 30]),
            &HashSet::new(),
        );
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].mounted_id, 1);
        assert_eq!(d[0].committed_id, Some(2));
    }

    /// A clean single committed create, no relog at all.
    #[test]
    fn excludes_plain_committed_create() {
        let mut events = HashMap::new();
        events.insert("x".to_string(), vec![ev(0x100, 9, false, Some(10))]);
        let mut mounted = HashMap::new();
        mounted.insert("x".to_string(), 9u64);
        let d = detect_xdb711_damage(
            &mounted,
            &events,
            &committed(&[10]),
            &HashSet::new(),
        );
        assert!(d.is_empty());
    }

    #[test]
    fn injectivity_flags_two_names_one_id() {
        let mut mounted = HashMap::new();
        mounted.insert("charlie".to_string(), 7u64);
        mounted.insert("echo".to_string(), 7u64);
        let v = mounted_injectivity_violations(&mounted);
        assert_eq!(v.len(), 1);
        assert_eq!(v[0].0, 7);
        assert_eq!(v[0].1, vec!["charlie".to_string(), "echo".to_string()]);
    }

    #[test]
    fn message_names_affected_dbs_and_no_subcommand() {
        let d = vec![CatalogDamage {
            name: "charlie".to_string(),
            mounted_id: 7,
            committed_id: Some(99),
            stray_lsn: lsn(0x5339),
            authority_lsn: lsn(0x4100),
        }];
        let m = log_integrity_message(&d);
        assert!(m.contains("'charlie'"));
        assert!(m.contains("restore this environment from a backup"));
        assert!(m.contains("preserve this environment directory"));
        assert!(
            !m.contains("scavenge"),
            "must not name a 7.11.x-absent subcommand"
        );
    }
}
