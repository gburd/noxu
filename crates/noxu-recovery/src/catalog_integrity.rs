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
//! LSN than a later **committed** rename / remove / recreate of that name.
//! Recovery's latest-NameLN-per-name-wins replay then binds the name to the
//! stale id: one database's committed records surface under another
//! database's name, and the correct database's records are lost — silently
//! (recovery returns success and `verify` passes). See `xdb-711.md` §4 and
//! `xdb-711-fix.md` §1/§9.
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
//! # Identify the stray record, not "any non-transactional NameLN"
//!
//! The first cut of this detector (XDB-DETECT-FP-1) fired whenever the
//! recovery winner for a name was *any* non-transactional live NameLN sitting
//! above a disagreeing committed-transactional authority. That is wrong: a
//! **legitimate auto-commit** create / rename-in / recreate writes exactly
//! that shape (an auto-commit NameLN is non-transactional, and it legitimately
//! supersedes an earlier transactional binding). It refused healthy
//! environments whose history mixed transactional and auto-commit DDL on one
//! name.
//!
//! The corrected rule identifies the **stray relog record itself** — the
//! record the bug actually emits — rather than classifying every
//! non-transactional NameLN as a stray. The relog
//! ([`relog_live_catalog_impl`]) emits a **burst of live NameLNs, one per live
//! database, back to back, immediately before the checkpoint's `CkptStart`**
//! (the checkpointer calls the relog callback at the top of `do_checkpoint`,
//! then writes `CkptStart`). A user auto-commit DDL NameLN is **not** part of
//! such a burst — a create logs a single NameLN followed by its own data, a
//! rename logs a tombstone+live pair at operation time, a remove logs a lone
//! tombstone — none is a run of all-live NameLNs terminated by a `CkptStart`.
//! So the relog record is **positionally identifiable** and that position is
//! carried on each event as [`CatalogEvent::in_relog_run`], computed during
//! the forward analysis scan.
//!
//! The rule then fires only when a **relog-run** live NameLN `N->F` is the
//! recovery winner for `N` **and** `N`'s latest *legitimate* binding — the
//! latest event that is committed-transactional OR a non-relog
//! (auto-commit / legacy) non-transactional event — disagrees with `F` at a
//! lower LSN, with no legitimate op re-binding `N->F` afterwards. A later
//! auto-commit rename-in / recreate therefore counts as an authority: it
//! excludes the false positive (the auto-commit binding is the real current
//! one) without masking the genuine stray relog.
//!
//! The rule is a **specific signature of the damage, not a heuristic** — see
//! [`detect_xdb711_damage`] for the precise predicate and the exclusion of
//! every legitimate history (relog of a current binding, auto-commit DDL
//! mixed with transactional DDL, a truncated/torn tail, an in-doubt XA DDL,
//! rename chains, rename-back, recreate-same-name, remove+recreate).

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
    /// auto-commit, or legacy).
    pub txn_id: Option<u64>,
    /// `true` iff this NameLN is part of a **catalog relog burst** — a
    /// contiguous run of NameLN records terminated immediately by a
    /// `CkptStart`. That is the exact shape the checkpoint catalog relog
    /// (`relog_live_catalog_impl`) writes: one live NameLN per live database,
    /// back to back, then the checkpoint's `CkptStart`. This positional
    /// flag is what distinguishes a (possibly stray) relog NameLN from a
    /// legitimate auto-commit DDL NameLN (which is never in such a burst).
    /// Computed during the forward analysis scan; see
    /// `RecoveryManager::run_analysis`.
    pub in_relog_run: bool,
}

/// Which XDB-711 signature produced a [`CatalogDamage`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DamageKind {
    /// Signal (a): a stray catalog relog-run NameLN won recovery over a
    /// legitimate operation that had already superseded the binding.
    StrayRelog,
    /// Signal (b): two committed transactions bound one name to two different
    /// ids with no committed tombstone of the name between them (the
    /// defect-#2 collision — one database's committed records are orphaned).
    DoubleCommittedBinding,
}

/// A detected XDB-711 catalog-integrity violation for one database name.
#[derive(Debug, Clone)]
pub struct CatalogDamage {
    /// The mounted (recovered) database name whose binding is untrustworthy.
    pub name: String,
    /// Which signature fired.
    pub kind: DamageKind,
    /// For `StrayRelog`: the id the stray relog-run NameLN bound the name to
    /// (what recovery would mount). For `DoubleCommittedBinding`: the mounted
    /// (latest-committed) id.
    pub mounted_id: u64,
    /// The id of the superseded / orphaned binding, or `None` if the
    /// superseding op *removed* the name (only for `StrayRelog`).
    pub committed_id: Option<u64>,
    /// LSN of the record that won recovery (the stray relog NameLN for
    /// `StrayRelog`, or the later committed binding for
    /// `DoubleCommittedBinding`).
    pub stray_lsn: Lsn,
    /// LSN of the superseded authority (the legitimate op for `StrayRelog`,
    /// or the earlier committed binding for `DoubleCommittedBinding`).
    pub authority_lsn: Lsn,
}

/// Run the XDB-711 damage detector.
///
/// `mounted` is the settled latest-NameLN-wins catalog recovery is about to
/// mount (`AnalysisResult::recovered_db_names`). `events` is the per-name,
/// ascending-LSN catalog-event history accumulated during the analysis scan
/// (each event carries [`CatalogEvent::in_relog_run`]). `committed_txns` /
/// `aborted_txns` are the resolved transaction sets.
///
/// Returns the (possibly empty) list of damaged names. An empty list means
/// the recovered catalog is consistent with the legitimate catalog history
/// and recovery may proceed.
///
/// # The predicate (fires ⇔ XDB-711 damage)
///
/// For a mounted name `N` with `mounted[N] = F`, the detector reports damage
/// iff **all** hold:
///
/// 1. **The record that won recovery for `N` is a stray *relog-run* live
///    NameLN** — `N`'s highest-LSN event `e_win` has `txn_id == None`,
///    `!is_deleted`, `id == F`, **and `in_relog_run`**. The relog burst is the
///    only writer that emits a live NameLN inside a NameLN-run terminated by a
///    `CkptStart`; an auto-commit create/rename-in/recreate is a
///    non-transactional live NameLN too, but it is **not** in such a burst, so
///    this condition identifies the stray record itself rather than "any
///    non-transactional NameLN".
/// 2. **`N` has a legitimate authority that disagrees with `F`** — consider
///    `N`'s events that are *legitimate bindings*: committed-transactional
///    (`txn_id = Some(t)`, `t ∈ committed`) **or** non-transactional and **not**
///    a relog-run record (`txn_id == None && !in_relog_run` — a genuine
///    auto-commit / legacy create / rename / remove). The latest such event
///    `e_auth` is `N`'s durable authority. It must exist, satisfy
///    `e_auth.lsn < e_win.lsn`, and **disagree**: `e_auth` is a tombstone
///    (committed/auto remove or rename-away) **or** it binds `N→T` with
///    `T ≠ F`.
/// 3. **No legitimate op re-bound `N→F` at or after the authority** — there is
///    no legitimate (committed-transactional, or non-relog non-transactional)
///    **live** `N→F` event at `lsn ≥ e_auth.lsn`. If one existed, recovery's
///    `N→F` would be legitimate and the detector must not fire.
///
/// (1)+(2)+(3) is exactly: *a stray relog-run NameLN `N→F` won recovery above
/// `N`'s latest legitimate authority, which binds `N→T≠F` or removed `N`, and
/// no legitimate op ever re-bound `N→F`.* That is the stray-relog fingerprint
/// and nothing else.
///
/// # Why each legitimate history is excluded
///
/// - **Relog of a current binding** — the relog re-emits `N→F` where `F` is
///   the current id. `e_win` is a relog-run record (1 holds), but the latest
///   legitimate authority is a committed/auto `N→F`, so (2)'s "disagree" is
///   false (or (3)'s "no legitimate `N→F`" is false). **Does not fire.**
/// - **Auto-commit DDL mixed with transactional DDL (XDB-DETECT-FP-1)** — a
///   later auto-commit create/rename-in's live NameLN *wins* recovery, but it
///   is **not** a relog-run record (it is a standalone user op followed by its
///   own data or a tombstone+live pair), so (1) fails. Even if a close-time
///   relog then re-logs the current binding into a burst (so the winner *is*
///   in a relog run), that relog re-asserts the **current** auto-commit id, so
///   the latest legitimate authority (the auto-commit op) agrees and (2)/(3)
///   fail. **Does not fire.**
/// - **Rename-back `n→m→n`** — the final `m→n` rename-in re-binds `n` to its
///   original id; a close-time relog re-logs that current binding. The latest
///   legitimate authority is the `m→n` rename-in (a non-relog auto op) binding
///   `n→id`, which agrees with the mounted id. **Does not fire.**
/// - **Truncated / torn tail** — the detector fires only when the *surviving*
///   winner is a relog-run NameLN above a *surviving* legitimate disagreeing
///   authority; a tail that drops the stray relog leaves `mounted` equal to the
///   legitimate binding, and a tail that drops a not-yet-committed DDL leaves no
///   authority. **Does not fire.**
/// - **In-doubt XA DDL** — its NameLNTxn's `txn_id` is not in `committed`
///   (prepared/active), so it is neither a legitimate committed authority nor a
///   relog-run record. **Does not fire.**
/// - **Rename chains / recreate-same-name / remove+recreate** — the latest
///   legitimate binding follows the committed/auto history; `mounted` matches
///   it unless a stray relog-run NameLN re-asserted a superseded id above it
///   (which *is* the damage).
pub fn detect_xdb711_damage(
    mounted: &HashMap<String, u64>,
    events: &HashMap<String, Vec<CatalogEvent>>,
    committed_txns: &HashMap<u64, Lsn>,
    _aborted_txns: &HashSet<u64>,
) -> Vec<CatalogDamage> {
    let mut damaged: Vec<CatalogDamage> = Vec::new();

    // Is this event a LEGITIMATE binding authority — a committed-transactional
    // event, or a genuine (non-relog-run) non-transactional auto-commit /
    // legacy event?  A relog-run NameLN is explicitly NOT a legitimate
    // authority: it re-asserts a snapshot binding and is the record the bug
    // emits.
    let is_legit = |ev: &CatalogEvent| -> bool {
        match ev.txn_id {
            Some(t) => committed_txns.contains_key(&t),
            None => !ev.in_relog_run,
        }
    };
    let is_committed_txnal = |ev: &CatalogEvent| -> bool {
        matches!(ev.txn_id, Some(t) if committed_txns.contains_key(&t))
    };

    for (name, &mounted_id) in mounted.iter() {
        let Some(hist) = events.get(name) else {
            // No catalog events for a mounted name would be anomalous, but it
            // is not the XDB-711 signature; leave it to other guards.
            continue;
        };

        // Signal (a): the STRAY RELOG supersession signature.
        if let Some(d) = stray_relog_damage(name, mounted_id, hist, &is_legit) {
            damaged.push(d);
            continue;
        }

        // Signal (b): the DOUBLE-COMMITTED-LIVE-BINDING signature (defect #2,
        // xdb-711-fix.md §8).  A name cannot be live-bound by two committed
        // transactions to two different ids without a committed tombstone
        // (remove / rename-away) of the name in between — a legitimate
        // recreate or rename-back always tombstones the name first.  When two
        // committed-transactional live bindings `N→A` and `N→B` (A≠B) occur
        // with no committed tombstone of N between them, two committed DDLs
        // raced and both bound one name: one database's committed records are
        // under id A, the other's under id B, and recovery mounts only the
        // latest — the other database's committed records are orphaned under a
        // live id with no live name.  This is the durable fingerprint of the
        // XDB-711 defect-#2 race (seed 167229452944: charlie→11 by txn87 and
        // charlie→7 by txn91, no tombstone between; id 11 holds charlie's
        // records, id 7 the old delta records).
        if let Some(d) = double_committed_binding_damage(
            name,
            mounted_id,
            hist,
            &is_committed_txnal,
            &is_legit,
        ) {
            damaged.push(d);
            continue;
        }
    }

    // Deterministic order for the error message / tests.
    damaged.sort_by(|a, b| a.name.cmp(&b.name));
    damaged
}

/// Signal (a): the stray-relog supersession signature for one name. Returns a
/// [`CatalogDamage`] if the recovery winner is a stray relog-run NameLN that
/// won over a disagreeing legitimate authority (see [`detect_xdb711_damage`]
/// conditions 1–3). `None` otherwise.
fn stray_relog_damage(
    name: &str,
    mounted_id: u64,
    hist: &[CatalogEvent],
    is_legit: &impl Fn(&CatalogEvent) -> bool,
) -> Option<CatalogDamage> {
    // The record that WON recovery for this name is its highest-LSN event (the
    // forward scan is ascending-LSN latest-wins; the list is in scan order).
    let e_win = *hist.last()?;

    // (1) The winner must be a STRAY RELOG-RUN live NameLN binding N→F:
    //     non-transactional, live, id == F, and positionally inside a relog
    //     burst (a NameLN run terminated by CkptStart).  This is the stray
    //     record the bug emits; a legitimate auto-commit NameLN is
    //     non-transactional and live too but is NOT in a relog run.
    if e_win.txn_id.is_some()
        || e_win.is_deleted
        || e_win.id != mounted_id
        || !e_win.in_relog_run
    {
        return None;
    }

    // (2) Find N's latest LEGITIMATE authority (committed-transactional OR a
    //     non-relog non-transactional auto/legacy event).  A relog-run record
    //     is never an authority.
    let mut authority: Option<CatalogEvent> = None;
    for ev in hist.iter() {
        if is_legit(ev) {
            authority = Some(*ev);
        }
    }
    let e_auth = authority?;

    // The authority must PREDATE the stray winner.
    if e_auth.lsn >= e_win.lsn {
        return None;
    }

    // (2) …and DISAGREE with the mounted id: a committed/auto remove or
    //     rename-away (tombstone), or a bind to a different id.
    let disagrees = e_auth.is_deleted || e_auth.id != mounted_id;
    if !disagrees {
        return None;
    }

    // (3) No legitimate op re-bound N→F at or after the authority.
    let legit_rebind_to_f = hist.iter().any(|ev| {
        ev.lsn >= e_auth.lsn
            && !ev.is_deleted
            && ev.id == mounted_id
            && is_legit(ev)
    });
    if legit_rebind_to_f {
        return None;
    }

    Some(CatalogDamage {
        name: name.to_string(),
        kind: DamageKind::StrayRelog,
        mounted_id,
        committed_id: if e_auth.is_deleted { None } else { Some(e_auth.id) },
        stray_lsn: e_win.lsn,
        authority_lsn: e_auth.lsn,
    })
}

/// Signal (b): the double-committed-live-binding (defect #2) signature for one
/// name. Returns a [`CatalogDamage`] if two committed-transactional LIVE
/// bindings `N→A`, `N→B` (A≠B) exist with no committed tombstone of N between
/// them. `None` otherwise.
///
/// Exactness: a legitimate rebind of one name to a new id ALWAYS writes a
/// committed tombstone first (remove+recreate, or rename-away+rename-in), so
/// two adjacent committed live bindings with no tombstone between them cannot
/// arise from any single-writer-correct history — it is the durable trace of
/// two DDL transactions that both committed a binding for one name (the
/// defect-#2 race). The reported `authority_lsn`/`stray_lsn` are the earlier
/// and later of the two colliding committed bindings; `committed_id` is the
/// earlier id (whose committed records are orphaned).
fn double_committed_binding_damage(
    name: &str,
    mounted_id: u64,
    hist: &[CatalogEvent],
    is_committed_txnal: &impl Fn(&CatalogEvent) -> bool,
    is_legit: &impl Fn(&CatalogEvent) -> bool,
) -> Option<CatalogDamage> {
    // Walk events in ascending LSN; track the last OPEN committed-txnal LIVE
    // binding.  A collision is two committed-txnal LIVE bindings to different
    // ids with no LEGITIMATE closure between them.  A legitimate closure is
    // ANY non-relog event that ends or re-binds the name:
    //   * a tombstone of any kind (committed-txnal remove/rename-away, OR a
    //     non-transactional auto/legacy remove/rename-away);
    //   * a non-transactional auto/legacy LIVE rebind (the name was
    //     legitimately rebound outside a transaction).
    // A relog-run record is NEITHER a binding nor a closure — it is the stray
    // writer's artifact and is ignored here (signal (a) handles it).
    let mut last_live: Option<CatalogEvent> = None;
    for ev in hist.iter() {
        // Ignore relog-run records entirely and any in-doubt/aborted txn
        // record (not legitimate, not committed) — they neither open nor
        // close a binding for the purpose of this signal.
        let committed_live = is_committed_txnal(ev) && !ev.is_deleted;
        if !committed_live && !is_legit(ev) {
            // in-doubt / aborted txn record, or a relog-run NameLN: skip.
            continue;
        }
        if ev.is_deleted {
            // A legitimate tombstone (committed or auto) closes the binding.
            last_live = None;
            continue;
        }
        if !committed_live {
            // A legitimate NON-transactional (auto/legacy) LIVE rebind: the
            // name was legitimately rebound outside a transaction, which
            // closes any prior committed binding (no collision across it).
            last_live = None;
            continue;
        }
        // A committed-transactional LIVE binding.
        if let Some(prev) = last_live
            && prev.id != ev.id
        {
            // Two committed-txnal live bindings to different ids with no
            // legitimate closure between them — the defect-#2 collision.
            return Some(CatalogDamage {
                name: name.to_string(),
                kind: DamageKind::DoubleCommittedBinding,
                mounted_id,
                committed_id: Some(prev.id),
                stray_lsn: ev.lsn,
                authority_lsn: prev.lsn,
            });
        }
        last_live = Some(*ev);
    }
    None
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
/// evidence, and the recovery options). The message deliberately does **not**
/// assert which version wrote the log (the writer-side fix emits logs that
/// look the same minus the stray record, so the detector cannot tell which
/// buggy version produced a given image), and it does **not** name
/// `noxu-admin scavenge`: that offline-salvage subcommand does not exist in
/// the 7.11.x release line (it is a v8 feature).
pub fn log_integrity_message(damaged: &[CatalogDamage]) -> String {
    let mut names: Vec<&str> =
        damaged.iter().map(|d| d.name.as_str()).collect();
    names.sort_unstable();
    names.dedup();
    let name_list =
        names.iter().map(|n| format!("'{n}'")).collect::<Vec<_>>().join(", ");

    // Name the concrete evidence per damaged database: the stray catalog
    // record (LSN + the binding it asserts) and the committed/auto operation
    // that superseded it (LSN + what it bound the name to).
    let mut evidence: Vec<CatalogDamage> = damaged.to_vec();
    evidence.sort_by(|a, b| a.name.cmp(&b.name));
    let evidence_lines = evidence
        .iter()
        .map(|d| match d.kind {
            DamageKind::StrayRelog => {
                let superseded = match d.committed_id {
                    Some(t) => format!("re-bound it to id {t}"),
                    None => "removed it".to_string(),
                };
                format!(
                    "a stray catalog record at LSN {} binds database '{}' to \
                     id {}, but a later legitimate operation at LSN {} {}",
                    d.stray_lsn,
                    d.name,
                    d.mounted_id,
                    d.authority_lsn,
                    superseded
                )
            }
            DamageKind::DoubleCommittedBinding => format!(
                "two committed operations bound database '{}' to two ids with \
                 no removal between them — id {} (committed at LSN {}) and id \
                 {} (committed at LSN {}); one database's committed records \
                 are orphaned under id {}",
                d.name,
                d.committed_id.unwrap_or(d.mounted_id),
                d.authority_lsn,
                d.mounted_id,
                d.stray_lsn,
                d.committed_id.unwrap_or(d.mounted_id),
            ),
        })
        .collect::<Vec<_>>()
        .join("; ");

    format!(
        "database catalog is inconsistent — the recovered name->id binding \
         for database(s) [{name_list}] cannot be trusted: {evidence_lines}. \
         The damage is from an earlier version's concurrent catalog DDL + \
         crash (XDB-711): either a non-transactional checkpoint-relog record \
         won recovery over an operation that had superseded the binding, or \
         two committed operations bound one name to two ids, so recovery \
         would mount one database's records under another database's name. \
         This problem is persistent; recovery will not mount a catalog it \
         knows is wrong. To recover: restore this environment from a backup, \
         or preserve this environment directory (copy it as-is; do not delete \
         or reinitialise it) for offline salvage. This version refuses to \
         open the environment, so nothing further is written to it."
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lsn(n: u32) -> Lsn {
        Lsn::new(0, n)
    }

    /// Full event constructor (ascending LSN, id, tombstone, txn, relog-run).
    fn evr(
        l: u32,
        id: u64,
        del: bool,
        txn: Option<u64>,
        relog: bool,
    ) -> CatalogEvent {
        CatalogEvent {
            lsn: lsn(l),
            id,
            is_deleted: del,
            txn_id: txn,
            in_relog_run: relog,
        }
    }

    /// A NON-relog event (committed-txnal or genuine auto-commit).
    fn ev(l: u32, id: u64, del: bool, txn: Option<u64>) -> CatalogEvent {
        evr(l, id, del, txn, false)
    }

    /// A relog-burst event (non-transactional, in a NameLN run before a
    /// CkptStart).
    fn relog(l: u32, id: u64) -> CatalogEvent {
        evr(l, id, false, None, true)
    }

    fn committed(ids: &[u64]) -> HashMap<u64, Lsn> {
        ids.iter().map(|&t| (t, lsn(1))).collect()
    }

    fn one(name: &str, id: u64, events: Vec<CatalogEvent>) -> ProbeInput {
        let mut m = HashMap::new();
        m.insert(name.to_string(), id);
        let mut e = HashMap::new();
        e.insert(name.to_string(), events);
        ProbeInput { mounted: m, events: e }
    }

    struct ProbeInput {
        mounted: HashMap<String, u64>,
        events: HashMap<String, Vec<CatalogEvent>>,
    }

    fn fire(input: &ProbeInput, commit: &[u64]) -> Vec<CatalogDamage> {
        detect_xdb711_damage(
            &input.mounted,
            &input.events,
            &committed(commit),
            &HashSet::new(),
        )
    }

    // ───────────────────────── DAMAGE (must fire) ─────────────────────────

    /// seed 167229452944 shape: rename delta→charlie (id 7, txn 91 committed),
    /// charlie later recreated under its own id (txn 150 committed), then a
    /// STRAY relog-run NameLN re-binds charlie→7 at a higher LSN (the
    /// decoded burst at 0x5305..0x53da before CkptStart 0x540e).
    #[test]
    fn fires_on_stray_relog_after_committed_rebind() {
        let p = one(
            "charlie",
            7,
            vec![
                ev(0x1f78, 7, false, Some(91)), // committed rename-in
                ev(0x4100, 99, false, Some(150)), // committed recreate → 99
                relog(0x5339, 7),               // STRAY relog burst → id 7
            ],
        );
        let d = fire(&p, &[91, 150]);
        assert_eq!(d.len(), 1, "must fire");
        assert_eq!(d[0].name, "charlie");
        assert_eq!(d[0].mounted_id, 7);
        assert_eq!(d[0].committed_id, Some(99));
        assert_eq!(d[0].stray_lsn, lsn(0x5339));
    }

    /// Stray relog-run NameLN resurrects a name the committed history REMOVED.
    #[test]
    fn fires_on_stray_relog_after_committed_remove() {
        let p = one(
            "charlie",
            7,
            vec![
                ev(0x100, 7, false, Some(10)), // committed create
                ev(0x200, 7, true, Some(20)),  // committed remove (tombstone)
                relog(0x900, 7),               // STRAY relog resurrects charlie
            ],
        );
        let d = fire(&p, &[10, 20]);
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].committed_id, None, "committed op removed the name");
    }

    /// A stray relog-run NameLN re-asserting the OLD id above a committed
    /// recreate IS XDB-711.
    #[test]
    fn fires_on_recreate_then_stray_relog_old_id() {
        let p = one(
            "n",
            1,
            vec![
                ev(0x100, 1, false, Some(10)),
                ev(0x200, 1, true, Some(20)),
                ev(0x300, 2, false, Some(30)), // committed recreate → 2
                relog(0x900, 1),               // STRAY relog burst → old id 1
            ],
        );
        let d = fire(&p, &[10, 20, 30]);
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].mounted_id, 1);
        assert_eq!(d[0].committed_id, Some(2));
    }

    /// Damage where the superseding authority is a genuine AUTO-commit
    /// (non-transactional) recreate, not transactional: a stray relog-run
    /// NameLN re-binds N→old after an auto-commit recreate N→new.
    #[test]
    fn fires_on_stray_relog_after_auto_recreate() {
        let p = one(
            "n",
            3,
            vec![
                ev(0x100, 3, false, Some(10)), // committed create → 3
                ev(0x200, 3, true, None),      // auto remove (tombstone)
                ev(0x300, 8, false, None),     // auto recreate → 8 (authority)
                relog(0x900, 3),               // STRAY relog burst → old id 3
            ],
        );
        let d = fire(&p, &[10]);
        assert_eq!(d.len(), 1);
        assert_eq!(d[0].mounted_id, 3);
        assert_eq!(d[0].committed_id, Some(8), "auto recreate is authority");
    }

    // ────────────────────── LEGITIMATE (must NOT fire) ─────────────────────

    /// A relog of the CURRENT binding: relog-run NameLN re-emits N→F where F
    /// is the committed id.
    #[test]
    fn excludes_relog_of_current_binding() {
        let p = one(
            "alpha",
            5,
            vec![
                ev(0x100, 5, false, Some(10)), // committed create → 5
                relog(0x900, 5),               // relog of the CURRENT binding
            ],
        );
        assert!(fire(&p, &[10]).is_empty(), "relog of current must not fire");
    }

    /// Auto-commit DDL only: non-transactional, and the relog re-logs the
    /// current id. (Also covers legacy pre-C6 non-txn creates.)
    #[test]
    fn excludes_auto_commit_ddl_only() {
        let p = one(
            "auto",
            8,
            vec![
                ev(0x100, 8, false, None), // auto-commit create (non-txn)
                relog(0x900, 8),           // relog of it
            ],
        );
        assert!(fire(&p, &[]).is_empty(), "auto-commit DDL must not fire");
    }

    // ── XDB-DETECT-FP-1 regression: mixed txnal + auto DDL on one name ──

    /// V1: txn create n→1, auto remove n, auto create n→2. The winner is the
    /// auto create (NOT a relog record). A close-time relog then re-logs the
    /// current n→2 — a relog-run winner, but it AGREES with the auto-commit
    /// authority. Must NOT fire.
    #[test]
    fn fp1_v1_txn_create_auto_remove_auto_create() {
        let p = one(
            "n",
            2,
            vec![
                ev(0x100, 1, false, Some(10)), // txn create → 1
                ev(0x200, 1, true, None),      // auto remove
                ev(0x300, 2, false, None),     // auto create → 2 (authority)
                relog(0x900, 2),               // close relog of CURRENT n→2
            ],
        );
        assert!(fire(&p, &[10]).is_empty(), "V1 healthy must not fire");
    }

    /// V1 without a close relog (the auto create itself wins): still not a
    /// relog record, so must NOT fire.
    #[test]
    fn fp1_v1_no_close_relog() {
        let p = one(
            "n",
            2,
            vec![
                ev(0x100, 1, false, Some(10)),
                ev(0x200, 1, true, None),
                ev(0x300, 2, false, None), // auto create wins (not a relog)
            ],
        );
        assert!(fire(&p, &[10]).is_empty(), "V1 no-relog must not fire");
    }

    /// V2: txn create n→1, txn remove n, auto create n→2 (+ close relog).
    #[test]
    fn fp1_v2_txn_create_txn_remove_auto_create() {
        let p = one(
            "n",
            2,
            vec![
                ev(0x100, 1, false, Some(10)), // txn create → 1
                ev(0x200, 1, true, Some(20)),  // txn remove (committed)
                ev(0x300, 2, false, None),     // auto create → 2 (authority)
                relog(0x900, 2),               // close relog of CURRENT
            ],
        );
        assert!(fire(&p, &[10, 20]).is_empty(), "V2 healthy must not fire");
    }

    /// V3: txn create n→1, txn rename n→m (n tombstoned), auto create n→3.
    #[test]
    fn fp1_v3_txn_create_txn_rename_away_auto_create() {
        let mut m = HashMap::new();
        m.insert("n".to_string(), 3u64);
        m.insert("m".to_string(), 1u64);
        let mut e = HashMap::new();
        e.insert(
            "n".to_string(),
            vec![
                ev(0x100, 1, false, Some(10)), // txn create n→1
                ev(0x200, 1, true, Some(20)),  // txn rename n→m: n tombstoned
                ev(0x400, 3, false, None),     // auto create n→3 (authority)
                relog(0x900, 3),               // close relog current n→3
            ],
        );
        e.insert(
            "m".to_string(),
            vec![
                ev(0x200, 1, false, Some(20)), // txn rename-in m→1
                relog(0x900, 1),               // close relog current m→1
            ],
        );
        let p = ProbeInput { mounted: m, events: e };
        assert!(fire(&p, &[10, 20]).is_empty(), "V3 healthy must not fire");
    }

    /// V4: auto create m→10, txn create n→11, txn remove n, auto rename m→n
    /// (m tombstoned, n→10). The winner for n is the auto rename-in (not a
    /// relog); the close relog re-logs current n→10 (agrees).
    #[test]
    fn fp1_v4_auto_m_txn_n_remove_auto_rename() {
        let mut m = HashMap::new();
        m.insert("n".to_string(), 10u64);
        let mut e = HashMap::new();
        e.insert(
            "n".to_string(),
            vec![
                ev(0x200, 11, false, Some(20)), // txn create n→11
                ev(0x300, 11, true, Some(30)),  // txn remove n
                ev(0x500, 10, false, None),     // auto rename-in m→n (n→10)
                relog(0x900, 10),               // close relog current n→10
            ],
        );
        e.insert(
            "m".to_string(),
            vec![
                ev(0x100, 10, false, None), // auto create m→10
                ev(0x500, 10, true, None),  // auto rename-away: m tombstoned
            ],
        );
        let p = ProbeInput { mounted: m, events: e };
        assert!(fire(&p, &[20, 30]).is_empty(), "V4 healthy must not fire");
    }

    /// V5: txn create n→1, auto rename n→m, auto rename m→n (rename-back). The
    /// final m→n re-binds n→1 (its original id); the close relog re-logs it.
    #[test]
    fn fp1_v5_rename_back() {
        let p = one(
            "n",
            1,
            vec![
                ev(0x100, 1, false, Some(10)), // txn create n→1
                ev(0x200, 1, true, None),      // auto rename n→m: n tombstoned
                ev(0x400, 1, false, None),     // auto rename-in m→n (n→1 back)
                relog(0x900, 1),               // close relog current n→1
            ],
        );
        assert!(fire(&p, &[10]).is_empty(), "V5 rename-back must not fire");
    }

    /// A torn tail that drops the stray relog: the surviving winner is the
    /// committed binding itself.
    #[test]
    fn excludes_truncated_tail_dropping_stray() {
        let p = one("beta", 3, vec![ev(0x100, 3, false, Some(10))]);
        assert!(fire(&p, &[10]).is_empty());
    }

    /// In-doubt XA DDL: a prepared-but-unresolved NameLNTxn (txn not in the
    /// committed set) is never a legitimate authority, and the relog re-logs
    /// the current id.
    #[test]
    fn excludes_in_doubt_xa_ddl() {
        let p = one(
            "gamma",
            4,
            vec![
                ev(0x100, 4, false, Some(10)), // committed create
                ev(0x200, 4, true, Some(77)), // in-doubt remove (77 NOT committed)
                relog(0x900, 4),              // relog of current id 4
            ],
        );
        assert!(fire(&p, &[10]).is_empty(), "in-doubt XA DDL must not fire");
    }

    /// Rename chain a→b→c, all committed, no stray relog.
    #[test]
    fn excludes_clean_rename_chain() {
        let mut m = HashMap::new();
        m.insert("c".to_string(), 7u64);
        let mut e = HashMap::new();
        e.insert("c".to_string(), vec![ev(0x300, 7, false, Some(30))]);
        e.insert(
            "b".to_string(),
            vec![ev(0x200, 7, false, Some(20)), ev(0x300, 7, true, Some(30))],
        );
        let p = ProbeInput { mounted: m, events: e };
        assert!(fire(&p, &[20, 30]).is_empty(), "clean chain must not fire");
    }

    /// Recreate same name under a NEW id, all committed, no stray relog.
    #[test]
    fn excludes_recreate_same_name() {
        let p = one(
            "n",
            2,
            vec![
                ev(0x100, 1, false, Some(10)),
                ev(0x200, 1, true, Some(20)),
                ev(0x300, 2, false, Some(30)),
            ],
        );
        assert!(fire(&p, &[10, 20, 30]).is_empty());
    }

    // ── Signal (b): double-committed-live-binding (defect #2) ──

    /// seed 167229452944 shape: charlie bound by TWO committed txns to two
    /// ids (charlie→11 txn87, charlie→7 txn91) with NO committed tombstone of
    /// charlie between them → the defect-#2 collision. Must FIRE.
    #[test]
    fn fires_on_double_committed_live_binding() {
        let p = one(
            "charlie",
            7,
            vec![
                ev(0x1c28, 0, true, Some(76)), // committed remove (legit)
                ev(0x1ea5, 11, false, Some(87)), // committed charlie→11
                ev(0x1f78, 7, false, Some(91)), // committed charlie→7 (no tombstone!)
                relog(0x5339, 7),               // stray relog of current id 7
            ],
        );
        let d = fire(&p, &[76, 87, 91]);
        assert_eq!(d.len(), 1, "double committed binding must fire");
        assert_eq!(d[0].name, "charlie");
        assert_eq!(d[0].committed_id, Some(11), "orphaned earlier id");
        assert_eq!(d[0].authority_lsn, lsn(0x1ea5));
        assert_eq!(d[0].stray_lsn, lsn(0x1f78));
    }

    /// A committed TXNAL remove tombstone between the two bindings is a
    /// legitimate recreate → must NOT fire.
    #[test]
    fn excludes_double_binding_with_txnal_remove_between() {
        let p = one(
            "n",
            2,
            vec![
                ev(0x100, 1, false, Some(10)), // committed create n→1
                ev(0x200, 0, true, Some(20)),  // committed TXNAL remove
                ev(0x300, 2, false, Some(30)), // committed recreate n→2
            ],
        );
        assert!(fire(&p, &[10, 20, 30]).is_empty());
    }

    /// A committed TXNAL rename-AWAY tombstone (n as the old name) between the
    /// two bindings is a legitimate rebind → must NOT fire.
    #[test]
    fn excludes_double_binding_with_rename_away_between() {
        let p = one(
            "n",
            2,
            vec![
                ev(0x100, 1, false, Some(10)), // committed create n→1
                ev(0x200, 0, true, Some(20)), // committed rename n→m (n tombstoned)
                ev(0x300, 2, false, Some(30)), // committed create n→2 again
            ],
        );
        assert!(fire(&p, &[10, 20, 30]).is_empty());
    }

    /// A NON-txn AUTO remove tombstone between two committed-txnal bindings
    /// does NOT count as an intervening committed tombstone for signal (b)
    /// (an auto tombstone is not a committed-transactional event), so the two
    /// committed live bindings with an auto tombstone between them are still
    /// the defect-#2 shape — EXCEPT the auto remove IS a legitimate
    /// supersession, which signal (b) must not confuse. Here the auto remove
    /// legitimately closed n→1 before the committed recreate n→2, so (b) must
    /// NOT fire. (The auto tombstone is a legitimate binding event; signal (a)
    /// treats it as an authority, and signal (b) must respect it as closing
    /// the prior live binding.)
    #[test]
    fn excludes_double_binding_with_auto_remove_between() {
        let p = one(
            "n",
            2,
            vec![
                ev(0x100, 1, false, Some(10)), // committed create n→1
                ev(0x200, 0, true, None),      // AUTO remove (non-txn)
                ev(0x300, 2, false, Some(30)), // committed recreate n→2
            ],
        );
        assert!(
            fire(&p, &[10, 30]).is_empty(),
            "auto remove between committed bindings must not fire (b)"
        );
    }

    /// Two committed live bindings to the SAME id (a relog or re-log of the
    /// same committed binding) is not a collision → must NOT fire.
    #[test]
    fn excludes_double_binding_same_id() {
        let p = one(
            "n",
            5,
            vec![
                ev(0x100, 5, false, Some(10)),
                ev(0x200, 5, false, Some(20)), // same id, no collision
            ],
        );
        assert!(fire(&p, &[10, 20]).is_empty());
    }

    /// One committed live binding + an ABORTED second binding to a different
    /// id: the aborted one is not committed → not a collision.
    #[test]
    fn excludes_double_binding_second_aborted() {
        let p = one(
            "n",
            1,
            vec![
                ev(0x100, 1, false, Some(10)), // committed n→1
                ev(0x200, 2, false, Some(20)), // n→2 but txn20 ABORTED
            ],
        );
        assert!(
            fire(&p, &[10]).is_empty(),
            "aborted second binding is not a collision"
        );
    }

    /// A plain committed create, no relog at all.
    #[test]
    fn excludes_plain_committed_create() {
        let p = one("x", 9, vec![ev(0x100, 9, false, Some(10))]);
        assert!(fire(&p, &[10]).is_empty());
    }

    /// A non-txn NameLN above a disagreeing committed authority that is NOT a
    /// relog-run record (an auto-commit recreate that WON): the FP-1 shape in
    /// isolation — must NOT fire because the winner is not a relog record.
    #[test]
    fn excludes_auto_recreate_winner_not_relog() {
        let p = one(
            "n",
            2,
            vec![
                ev(0x100, 1, false, Some(10)), // committed create → 1
                ev(0x300, 2, false, None),     // auto recreate → 2 (NOT relog)
            ],
        );
        assert!(
            fire(&p, &[10]).is_empty(),
            "auto recreate winner (not relog) must not fire"
        );
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
    fn message_names_evidence_no_version_no_subcommand() {
        let d = vec![CatalogDamage {
            name: "charlie".to_string(),
            kind: DamageKind::StrayRelog,
            mounted_id: 7,
            committed_id: Some(99),
            stray_lsn: lsn(0x5339),
            authority_lsn: lsn(0x4100),
        }];
        let m = log_integrity_message(&d);
        assert!(m.contains("'charlie'"));
        // Names the concrete evidence: the stray record's LSN + binding and
        // the superseding op's LSN.
        assert!(m.contains("binds database 'charlie' to id 7"));
        assert!(m.contains("re-bound it to id 99"));
        assert!(m.contains("restore this environment from a backup"));
        assert!(m.contains("preserve this environment directory"));
        // Does NOT assert which version wrote the log.
        assert!(
            !m.contains("7.11.0/7.11.1") && !m.contains("was written by a"),
            "must not assert which version wrote the log"
        );
        assert!(
            !m.contains("scavenge"),
            "must not name a 7.11.x-absent subcommand"
        );
    }

    #[test]
    fn message_names_removed_evidence() {
        let d = vec![CatalogDamage {
            name: "charlie".to_string(),
            kind: DamageKind::StrayRelog,
            mounted_id: 7,
            committed_id: None,
            stray_lsn: lsn(0x900),
            authority_lsn: lsn(0x200),
        }];
        let m = log_integrity_message(&d);
        assert!(m.contains("removed it"), "names the removing op");
    }

    #[test]
    fn message_names_double_binding_evidence() {
        let d = vec![CatalogDamage {
            name: "charlie".to_string(),
            kind: DamageKind::DoubleCommittedBinding,
            mounted_id: 7,
            committed_id: Some(11),
            stray_lsn: lsn(0x1f78),
            authority_lsn: lsn(0x1ea5),
        }];
        let m = log_integrity_message(&d);
        assert!(
            m.contains("two committed operations bound database 'charlie'")
        );
        assert!(m.contains("orphaned under id 11"));
        assert!(!m.contains("scavenge"));
    }

    /// Cost bound (overhead requirement): `detect_xdb711_damage` is O(total
    /// catalog events), independent of data volume.
    #[test]
    fn detector_cost_is_linear_in_catalog_ops_only() {
        let k = 10_000usize;
        let mut mounted = HashMap::new();
        let mut events = HashMap::new();
        let mut commit = HashMap::new();
        for i in 0..k {
            let name = format!("db{i}");
            let id = (i as u64) + 100;
            let tid = (i as u64) + 1;
            commit.insert(tid, lsn(1));
            // Clean history: committed create + a relog of the CURRENT id.
            events.insert(
                name.clone(),
                vec![
                    ev(100 + i as u32, id, false, Some(tid)),
                    relog(900 + i as u32, id),
                ],
            );
            mounted.insert(name, id);
        }
        let start = std::time::Instant::now();
        let d =
            detect_xdb711_damage(&mounted, &events, &commit, &HashSet::new());
        let elapsed = start.elapsed();
        assert!(d.is_empty(), "clean catalog must not fire");
        assert!(
            elapsed.as_millis() < 200,
            "detector over {k} names took {elapsed:?} (must be ~O(ops))"
        );
    }
}
