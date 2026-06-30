//! HEADLINE: chained / replica-to-replica log feeding (master → R1 → R2).
//!
//! Proves a 3-node replication CHAIN over real TCP service dispatchers:
//!
//! ```text
//!   master ──PEER_FEEDER──► R1 ──PEER_FEEDER──► R2
//! ```
//!
//! - The master writes VLSN-tagged committed records to its WAL.
//! - R1 connects to the master, RECEIVES + PERSISTS (VLSN-tagged WAL) +
//!   APPLIES the records to its live tree, AND — with `cascade_feeding`
//!   enabled — serves a downstream feeder from its OWN WAL.
//! - R2 connects to **R1** (not the master), receives the stream FROM R1,
//!   and applies it to its live tree.
//! - A READ on R2's live tree returns the master's committed data — proving
//!   R2's data matches the master's, sourced via R1.
//!
//! ## Faithful to JE's cascading-feeder model — ONE mechanism
//!
//! `FeederSource.java` documents the source as "a real Master OR a Replica
//! in a Replica chain that is replaying log records it received from some
//! other source".  `Feeder.initMasterFeederSource(startVLSN)` builds
//! `new MasterFeederSource(repImpl, repNode.getVLSNIndex(), …)` regardless
//! of node role, and the output loop pulls
//! `feederSource.getWireRecord(feederVLSN, heartbeatMs)`
//! (`Feeder.java:1282`).  `FeederManager` runs feeders on any node that
//! holds the data.  Here the *same* `PeerFeederService` (= `FeederManager`)
//! → `FeederRunner` (= `Feeder`) → `EnvironmentLogScanner`
//! (= `MasterFeederSource`/`FeederReader`) machinery that serves the
//! master's WAL also serves R1's WAL to R2.  The test ASSERTS this via
//! `wal_feeds_served()` (see the proof block below): the cascade is NOT a
//! separate feeder path.
//!
//! ## Fail-pre / pass-post
//!
//! **Before this branch** (`origin/main`):
//! - `EnvironmentLogWriter::write_entry` wrote a 14-byte (no-VLSN) header,
//!   so R1's WAL carried no VLSN-tagged entries → R1's `PEER_FEEDER` had
//!   nothing for an `EnvironmentLogScanner` to find.
//! - There was no `cascade_feeding` config and no WAL-backed `PEER_FEEDER`
//!   on a replica.  R2 connecting to R1 received nothing; the read FAILS.
//!
//! **After this branch**:
//! - R1 re-logs received entries with the master's VLSN (`log_with_vlsn`),
//!   so its WAL is a valid feeder source.
//! - `cascade_feeding=true` makes R1's `PEER_FEEDER` serve from its WAL.
//! - R2 receives the master's records via R1 and the read PASSES.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::BytesMut;
use noxu_dbi::{DatabaseConfig, EnvironmentImpl};
use noxu_log::entry::LnLogEntry;
use noxu_log::{LogEntryType, LogManager};
use noxu_rep::{NodeType, RepConfig, RepNode, ReplicatedEnvironment};
use noxu_util::{NULL_LSN, NULL_VLSN};

// ─── helpers ────────────────────────────────────────────────────────────────

/// Build the on-log payload for a non-transactional InsertLN record.
fn ln_payload(db_id: u64, key: &[u8], data: &[u8]) -> Vec<u8> {
    let entry = LnLogEntry::new(
        db_id,
        None,
        NULL_LSN,
        false,
        None,
        None,
        NULL_VLSN,
        0,
        false,
        key.to_vec(),
        Some(data.to_vec()),
        0,
        NULL_VLSN,
    );
    let mut buf = BytesMut::new();
    entry.write_to_log(&mut buf);
    buf.to_vec()
}

/// Open a live `EnvironmentImpl` in `dir`, open the replicated database,
/// and return the env + db id + live tree + log manager.
fn open_node(
    dir: &std::path::Path,
) -> (
    Arc<EnvironmentImpl>,
    u64,
    Arc<std::sync::RwLock<noxu_tree::Tree>>,
    Arc<LogManager>,
) {
    let env = Arc::new(EnvironmentImpl::new(dir, false, true).unwrap());
    let mut cfg = DatabaseConfig::new();
    cfg.set_allow_create(true).set_transactional(true);
    let db = env.open_database("chain_db", &cfg).unwrap();
    let db_id = db.read().get_id().id() as u64;
    let tree = env.replica_tree_for_db(db_id).unwrap();
    let log_mgr = env.get_log_manager().expect("log manager");
    (env, db_id, tree, log_mgr)
}

/// Poll `tree` until `key` resolves to `Some(found)` data, or the deadline
/// passes.  Returns the found value (or `None` on timeout).
fn poll_read(
    tree: &Arc<std::sync::RwLock<noxu_tree::Tree>>,
    key: &[u8],
    timeout: Duration,
) -> Option<Vec<u8>> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(fetch) = tree.read().unwrap().search_with_data(key)
            && fetch.found
        {
            return fetch.data;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    None
}

// ─── HEADLINE: master → R1 → R2 ──────────────────────────────────────────────

#[test]
fn test_chain_master_r1_r2_read_on_r2_matches_master() {
    // Records the master "commits".
    let records: [(&[u8], &[u8]); 4] = [
        (b"alpha", b"one"),
        (b"bravo", b"two"),
        (b"charlie", b"three"),
        (b"delta", b"four"),
    ];

    // ── master ────────────────────────────────────────────────────────────
    let master_dir = tempfile::TempDir::new().unwrap();
    let master_home = master_dir.path().to_path_buf();
    let (master_env_impl, master_db_id, _m_tree, master_log) =
        open_node(&master_home);

    let master_cfg = RepConfig::builder("chain_grp", "master", "127.0.0.1")
        .node_port(0)
        .env_home(&master_home)
        .build();
    let master = Arc::new(ReplicatedEnvironment::new(master_cfg).unwrap());
    master.init_self_weak();
    master.with_environment(Arc::clone(&master_env_impl));

    // ── R1 (mid-tier, cascade ENABLED) ──────────────────────────────────────
    let r1_dir = tempfile::TempDir::new().unwrap();
    let r1_home = r1_dir.path().to_path_buf();
    let (r1_env_impl, r1_db_id, r1_tree, _r1_log) = open_node(&r1_home);
    assert_eq!(
        r1_db_id, master_db_id,
        "the replicated db must share a db id across the chain"
    );

    let r1_cfg = RepConfig::builder("chain_grp", "R1", "127.0.0.1")
        .node_port(0)
        .env_home(&r1_home)
        .cascade_feeding(true) // serve R2 from R1's own WAL
        .build();
    let r1 = Arc::new(ReplicatedEnvironment::new(r1_cfg).unwrap());
    r1.init_self_weak();
    r1.with_environment(Arc::clone(&r1_env_impl));

    // ── R2 (leaf) ───────────────────────────────────────────────────────────
    let r2_dir = tempfile::TempDir::new().unwrap();
    let r2_home = r2_dir.path().to_path_buf();
    let (r2_env_impl, r2_db_id, r2_tree, _r2_log) = open_node(&r2_home);
    assert_eq!(r2_db_id, master_db_id, "shared db id across the chain");

    let r2_cfg = RepConfig::builder("chain_grp", "R2", "127.0.0.1")
        .node_port(0)
        .env_home(&r2_home)
        .build();
    let r2 = Arc::new(ReplicatedEnvironment::new(r2_cfg).unwrap());
    r2.init_self_weak();
    r2.with_environment(Arc::clone(&r2_env_impl));

    // ── resolve bound addresses; wire the chain topology ────────────────────
    let master_addr = master.bound_addr().expect("master binds");
    let r1_addr = r1.bound_addr().expect("R1 binds");

    // R1 knows the master (its upstream).
    r1.add_peer(RepNode::new(
        "master".to_string(),
        NodeType::Electable,
        "127.0.0.1".to_string(),
        master_addr.port(),
        1,
    ))
    .unwrap();
    // R2 knows R1 (its upstream is the MID-TIER, not the master).
    r2.add_peer(RepNode::new(
        "R1".to_string(),
        NodeType::Electable,
        "127.0.0.1".to_string(),
        r1_addr.port(),
        2,
    ))
    .unwrap();
    // The master tracks R1 as an electable replica (durability bookkeeping).
    master
        .add_peer(RepNode::new(
            "R1".to_string(),
            NodeType::Electable,
            "127.0.0.1".to_string(),
            r1_addr.port(),
            1,
        ))
        .unwrap();

    // ── master becomes master; writes VLSN-tagged committed records ─────────
    master.become_master(1).unwrap();

    // The master "commits" each record: write a VLSN-tagged InsertLN to the
    // WAL (flushed) and register the VLSN→LSN mapping so the WAL feeder's
    // range negotiation sees the entries.  This is the WAL-level equivalent
    // of a committed put on a replicated master (the same `log_with_vlsn`
    // call `EnvironmentLogWriter` uses on a replica).
    for (i, (k, v)) in records.iter().enumerate() {
        let vlsn = (i + 1) as u64;
        let payload = ln_payload(master_db_id, k, v);
        let lsn = master_log
            .log_with_vlsn(
                LogEntryType::InsertLN,
                &payload,
                vlsn,
                /*flush=*/ true,
                /*fsync=*/ false,
            )
            .expect("master log_with_vlsn");
        master.register_vlsn_typed(
            vlsn,
            lsn.file_number(),
            lsn.file_offset(),
            LogEntryType::InsertLN,
        );
    }

    // ── R1 becomes a replica of the master ──────────────────────────────────
    //    The replica I/O thread connects to the master's PEER_FEEDER (WAL
    //    source), receives the records, persists them VLSN-tagged, and
    //    applies them to R1's live tree.  Because cascade_feeding=true, R1
    //    ALSO re-registers its PEER_FEEDER with a WAL source for R2.
    r1.become_replica("master").unwrap();

    // Wait for R1 to materialise all records in its live tree.
    for (k, v) in &records {
        let got = poll_read(&r1_tree, k, Duration::from_secs(15));
        assert_eq!(
            got.as_deref(),
            Some(&v[..]),
            "R1 must receive + apply '{}' from the master",
            std::str::from_utf8(k).unwrap()
        );
    }

    // ── R2 becomes a replica of R1 (the mid-tier!) ──────────────────────────
    let r1_range = r1.vlsn_index_arc().get_range();
    eprintln!(
        "PROBE: R1 vlsn range first={} last={}",
        r1_range.first(),
        r1_range.last()
    );
    r2.become_replica("R1").unwrap();

    // ── HEADLINE ASSERTION: R2's data matches the master's, sourced via R1 ──
    for (k, v) in &records {
        let got = poll_read(&r2_tree, k, Duration::from_secs(20));
        assert_eq!(
            got.as_deref(),
            Some(&v[..]),
            "FAIL-PRE: R2 read of '{}' returned nothing — without the \
             WAL-backed cascade feeder + VLSN-tagged replica WAL, R2 cannot \
             source the master's data via R1",
            std::str::from_utf8(k).unwrap()
        );
    }

    // R2's VLSN coverage should reach the master's last VLSN (4), proving the
    // full chain delivered every committed entry.
    let r2_range = r2.vlsn_index_arc().get_range();
    assert!(
        r2_range.last() >= records.len() as u64,
        "R2 VLSN range must cover all {} entries; got last={}",
        records.len(),
        r2_range.last()
    );

    // ── PROOF: R1 fed R2 via the SAME mechanism the master fed R1 ──────────
    //
    // `wal_feeds_served()` counts connections served by the JE
    // `Feeder`/`MasterFeederSource` path (`FeederRunner` +
    // `EnvironmentLogScanner` reading the node's OWN WAL), incremented inside
    // `PeerFeederService::handle`'s WAL branch.  Both the master and the
    // cascading replica register the IDENTICAL `PeerFeederService` with a WAL
    // source, so a non-zero count on R1 proves the cascade used the
    // `FeederRunner` mechanism reading R1's WAL — NOT the in-memory
    // `PeerScannerAdapter` pull fallback, and NOT a separate feeder path.
    //
    // JE: `FeederSource.java` ("a real Master OR a Replica in a Replica
    // chain"), `Feeder.initMasterFeederSource` → `new MasterFeederSource(
    // repImpl, repNode.getVLSNIndex(), …)`, `Feeder.java:1282`
    // `feederSource.getWireRecord(feederVLSN, heartbeatMs)`.
    assert!(
        r1.wal_feeds_served() >= 1,
        "R1 must have fed R2 via the WAL FeederRunner + EnvironmentLogScanner \
         mechanism (JE Feeder + MasterFeederSource), not the in-memory pull \
         fallback; wal_feeds_served() == {}",
        r1.wal_feeds_served(),
    );
    // And the master fed R1 by the very same mechanism.
    assert!(
        master.wal_feeds_served() >= 1,
        "the master must have fed R1 via the same WAL FeederRunner mechanism; \
         wal_feeds_served() == {}",
        master.wal_feeds_served(),
    );

    // ── tidy shutdown (leaf → mid-tier → master) ────────────────────────────
    r2.close().unwrap();
    r1.close().unwrap();
    master.close().unwrap();
}
