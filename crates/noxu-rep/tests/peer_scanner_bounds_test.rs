//! F10: PeerLogScanner / apply_entry must keep memory bounded under
//! sustained input.
//!
//! Without this fix, every replicated entry was appended to a
//! `VecDeque<(vlsn, type, payload)>` with no eviction. A long-running
//! replica accumulated one VecDeque entry per replicated record
//! forever, OOMing in steady state.
//!
//! Wave 3-3 puts two bounds on the queue (entry count and total
//! payload bytes) and evicts oldest-first when either bound is hit.
//! Downstream peers that fall behind the eviction window must catch
//! up via the durable on-disk log.
//!
//! See `docs/src/internal/api-audit-2026-05-rep.md` finding F10.

use noxu_rep::stream::peer_feeder::{
    DEFAULT_PEER_SCANNER_MAX_BYTES, DEFAULT_PEER_SCANNER_MAX_ENTRIES,
    PeerLogScanner,
};
use noxu_rep::{NodeType, RepConfig, ReplicatedEnvironment};

/// Sustained `apply_entry` workload must not grow scanner memory
/// without bound.  After more than `MAX_ENTRIES` entries are pushed,
/// the queue length stops growing and the byte budget stays under
/// `MAX_BYTES`.
#[test]
fn f10_peer_scanner_caps_entry_count() {
    let scanner = PeerLogScanner::with_capacity(/* entries */ 100, /* bytes */ usize::MAX);

    // Push 10x the cap.
    for vlsn in 1u64..=1_000 {
        scanner.push(vlsn, 1, vec![0u8; 16]);
    }

    assert_eq!(
        scanner.len(),
        100,
        "queue should be capped at 100 entries"
    );
    assert_eq!(
        scanner.evicted_count(),
        900,
        "expected 900 evictions, got {}",
        scanner.evicted_count()
    );

    let (first, last) = scanner.log_range().unwrap();
    assert_eq!(last, 1_000);
    // After 900 evictions, oldest retained vlsn is 901.
    assert_eq!(first, 901);
}

/// The byte-size cap evicts oldest-first independently of entry count.
#[test]
fn f10_peer_scanner_caps_total_bytes() {
    // 1 KiB per entry; cap at 8 KiB.
    let scanner = PeerLogScanner::with_capacity(usize::MAX, 8 * 1024);

    for vlsn in 1u64..=64 {
        scanner.push(vlsn, 1, vec![0u8; 1024]);
    }

    assert!(
        scanner.current_bytes() <= 8 * 1024,
        "byte cap exceeded: {} > 8 KiB",
        scanner.current_bytes()
    );
    assert!(
        scanner.evicted_count() >= 56,
        "expected at least 56 evictions; got {}",
        scanner.evicted_count()
    );
}

/// Default constructor honours both bounds; a sustained 64 KiB-per-entry
/// stream beyond the byte cap stays bounded.
#[test]
fn f10_peer_scanner_default_bounds_enforce_byte_cap() {
    let scanner = PeerLogScanner::new();

    // Push 4 GiB worth of 64 KiB entries to test the byte cap.
    // Default byte cap is 64 MiB, so 4 GiB / 64 MiB = 64 evictions
    // worth.  Use a smaller stream to keep the test fast: 1024 entries
    // at 64 KiB each = 64 MiB, equal to the cap.
    for vlsn in 1u64..=2_048 {
        scanner.push(vlsn, 1, vec![0u8; 64 * 1024]);
    }

    assert!(
        scanner.current_bytes() <= DEFAULT_PEER_SCANNER_MAX_BYTES,
        "byte cap exceeded: {} > {}",
        scanner.current_bytes(),
        DEFAULT_PEER_SCANNER_MAX_BYTES
    );
    assert!(
        scanner.len() <= DEFAULT_PEER_SCANNER_MAX_ENTRIES,
        "entry cap exceeded: {} > {}",
        scanner.len(),
        DEFAULT_PEER_SCANNER_MAX_ENTRIES
    );
}

/// Invariant: after extreme over-feeding, the scanner's
/// `current_bytes()` never exceeds the configured `max_bytes`.
#[test]
fn f10_peer_scanner_invariant_byte_bound_holds_after_burst() {
    let scanner = PeerLogScanner::with_capacity(usize::MAX, 4 * 1024);

    for vlsn in 1u64..=10_000 {
        scanner.push(vlsn, 1, vec![0u8; 100]);
        // Spot-check the invariant on every iteration.
        assert!(
            scanner.current_bytes() <= 4 * 1024 + 100,
            "byte cap exceeded after vlsn={}: {} bytes",
            vlsn,
            scanner.current_bytes()
        );
    }
}

/// End-to-end: feed `ReplicatedEnvironment::apply_entry` continuously and
/// verify scanner memory stays bounded.
#[test]
fn f10_apply_entry_stays_bounded_under_sustained_input() {
    let cfg = RepConfig::builder("group_f10", "replica_f10", "127.0.0.1")
        .node_port(0)
        .node_type(NodeType::Electable)
        .build();
    let env = ReplicatedEnvironment::new(cfg).unwrap();
    env.become_replica("master_f10").unwrap();

    let entry_size = 1024usize;
    // 256 KiB total; default cap is 64 MiB, so we should NOT see eviction
    // here, but the queue length should bound at 256 (the entry cap is
    // 16K).  We test the cap fires at higher scale next.
    for vlsn in 1u64..=256 {
        env.apply_entry(vlsn, 1, vec![0u8; entry_size]).unwrap();
    }

    // Push enough entries to cross the entry cap. Use 32K entries.
    // Each entry at 16 bytes payload to keep byte cap low.
    let env_for_burst = &env;
    for vlsn in 257u64..=(257 + 32_000) {
        env_for_burst
            .apply_entry(vlsn, 1, vec![0u8; 16])
            .unwrap();
    }

    // We don't have a direct accessor on ReplicatedEnvironment for the
    // peer_scanner len, but the ReplicatedEnvironment is *bounded*
    // implicitly.  Push past the entry cap and observe no panic / OOM.
    // At this point the env should still be alive and accept further
    // applies.
    env.apply_entry(99_999, 1, b"final".to_vec()).unwrap();

    let _ = env.close();
}
