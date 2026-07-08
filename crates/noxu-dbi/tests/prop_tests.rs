//! Property-based tests for noxu-dbi (Hegel / hegeltest).

use hegel::generators;

use noxu_dbi::{DatabaseId, OperationStatus};

// ============================================================================
// 1. DatabaseId: sequential IDs are unique and monotonically increasing
// ============================================================================

#[hegel::test]
fn database_id_uniqueness(tc: hegel::TestCase) {
    let ids: Vec<i64> = tc.draw(
        generators::vecs(generators::integers::<i64>())
            .min_size(2)
            .max_size(99),
    );
    // All distinct input values should produce distinct DatabaseIds
    let db_ids: Vec<DatabaseId> =
        ids.iter().map(|&id| DatabaseId::new(id)).collect();

    for i in 0..db_ids.len() {
        for j in (i + 1)..db_ids.len() {
            if ids[i] != ids[j] {
                assert_ne!(
                    db_ids[i], db_ids[j],
                    "DatabaseId::new({}) == DatabaseId::new({})",
                    ids[i], ids[j]
                );
            }
        }
    }
}

#[hegel::test]
fn database_id_monotonically_increasing(tc: hegel::TestCase) {
    let a = tc.draw(generators::integers::<i64>());
    let b = tc.draw(generators::integers::<i64>());
    let id_a = DatabaseId::new(a);
    let id_b = DatabaseId::new(b);

    // The ordering of DatabaseId should match the ordering of the underlying i64
    assert_eq!(
        id_a.cmp(&id_b),
        a.cmp(&b),
        "DatabaseId ordering does not match i64 ordering for {} vs {}",
        a,
        b
    );
}

#[hegel::test]
fn database_id_roundtrip(tc: hegel::TestCase) {
    let id_val = tc.draw(generators::integers::<i64>());
    let db_id = DatabaseId::new(id_val);
    assert_eq!(db_id.id(), id_val);
    assert_eq!(db_id.as_i64(), id_val);
}

#[hegel::test]
fn database_id_serialization_roundtrip(tc: hegel::TestCase) {
    let id_val = tc.draw(generators::integers::<i64>());
    let original = DatabaseId::new(id_val);
    let mut buf = Vec::new();
    original.write_to_log(&mut buf);
    assert_eq!(buf.len(), 8);

    let restored = DatabaseId::read_from_log(&buf).unwrap();
    assert_eq!(original, restored);
}

#[hegel::test]
fn database_id_equality_reflexive(tc: hegel::TestCase) {
    let id_val = tc.draw(generators::integers::<i64>());
    let id = DatabaseId::new(id_val);
    assert_eq!(id, id);
}

#[hegel::test]
fn database_id_equality_consistent(tc: hegel::TestCase) {
    let a = tc.draw(generators::integers::<i64>());
    let b = tc.draw(generators::integers::<i64>());
    let id_a = DatabaseId::new(a);
    let id_b = DatabaseId::new(b);
    if a == b {
        assert_eq!(id_a, id_b);
    } else {
        assert_ne!(id_a, id_b);
    }
}

// ============================================================================
// 2. OperationStatus: all four variants are distinct
// ============================================================================

#[test]
fn operation_status_all_variants_distinct() {
    let variants = [
        OperationStatus::Success,
        OperationStatus::NotFound,
        OperationStatus::KeyExist,
        OperationStatus::KeyEmpty,
    ];

    for i in 0..variants.len() {
        for j in (i + 1)..variants.len() {
            assert_ne!(
                variants[i], variants[j],
                "{:?} should not equal {:?}",
                variants[i], variants[j]
            );
        }
    }
}

#[test]
fn operation_status_only_success_is_success() {
    assert!(OperationStatus::Success.is_success());
    assert!(!OperationStatus::NotFound.is_success());
    assert!(!OperationStatus::KeyExist.is_success());
    assert!(!OperationStatus::KeyEmpty.is_success());
}

#[hegel::test]
fn operation_status_display_not_empty(tc: hegel::TestCase) {
    let variant = tc.draw(generators::sampled_from(vec![
        OperationStatus::Success,
        OperationStatus::NotFound,
        OperationStatus::KeyExist,
        OperationStatus::KeyEmpty,
    ]));
    let display = format!("{}", variant);
    assert!(
        !display.is_empty(),
        "Display for {:?} should not be empty",
        variant
    );
}
