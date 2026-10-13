// Inverse-teeth integration witness for the BUG-VERIFY-TRACKER-LATCH-ABBA
// remedy: plant a genuinely-obsolete LSN into the tree's live set (seed a real
// live LN LSN into the tracker's obsolete set) and confirm Environment::verify
// STILL reports it.  The remedy (snapshot obsolete first, then walk) must not
// blind the check.
#![cfg(not(noxu_shuttle))]

use noxu_db::{
    CheckpointConfig, DatabaseConfig, Environment, EnvironmentConfig,
    VerifyConfig,
};
use tempfile::TempDir;

#[test]
fn verify_still_reports_a_genuinely_obsolete_live_lsn() {
    let dir = TempDir::new().unwrap();
    let env = Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true)
            .with_run_cleaner(false)
            .with_run_checkpointer(false)
            .with_run_verifier(false),
    )
    .unwrap();
    let db = env
        .open_database(
            None,
            "teeth",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .unwrap();
    for i in 0..32u32 {
        let k = format!("k{i:04}");
        // 64-byte value: > max_embedded_ln (16) so the LN is NON-embedded and
        // its LSN appears in the live set (embedded LNs are exempt by design).
        db.put(k.as_bytes(), [b'v'; 64]).unwrap();
    }
    env.checkpoint(Some(&CheckpointConfig::new().with_force(true))).unwrap();

    // Control: a clean verify reports no checkLsns inconsistency.
    let cfg = VerifyConfig::new();
    let clean = env.verify(&cfg).unwrap();
    let clean_fp = clean
        .errors
        .iter()
        .filter(|e| format!("{e:?}").contains("Obsolete LSN set contains"))
        .count();
    assert_eq!(
        clean_fp, 0,
        "clean env must have no checkLsns error: {clean:?}"
    );

    // Grab a real live LSN and plant it into the tracker's obsolete set.
    let live = env.live_lsns_for_test("teeth");
    assert!(!live.is_empty(), "populated db must have live LSNs");
    let victim = live[0];
    {
        let tracker =
            env.utilization_tracker_for_test().expect("rw env has a tracker");
        let mut t = tracker.lock();
        // Seed the corruption: a live LSN recorded obsolete (exact, with
        // offset tracking so it lands in the obsolete-offset set).
        t.count_obsolete_node(
            victim.file_number(),
            victim.file_offset(),
            64,
            true,
            None,
        );
    }

    // Inverse teeth: verify MUST still flag the planted obsolete-but-live LSN.
    let r = env.verify(&cfg).unwrap();
    let reported = r.errors.iter().any(|e| {
        let s = format!("{e:?}");
        s.contains("Obsolete LSN set contains")
            && s.contains(&format!("{victim}"))
    });
    assert!(
        reported,
        "remedy must NOT blind the check: planted obsolete live LSN {victim} \
         not reported; errors={:?}",
        r.errors
    );
}
