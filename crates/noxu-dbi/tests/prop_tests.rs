//! Property-based tests for noxu-dbi using proptest.

use proptest::prelude::*;

use noxu_dbi::{DatabaseId, OperationStatus};

// ============================================================================
// 1. DatabaseId: sequential IDs are unique and monotonically increasing
// ============================================================================

proptest! {
    #[test]
    fn database_id_uniqueness(ids in prop::collection::vec(any::<i64>(), 2..100)) {
        // All distinct input values should produce distinct DatabaseIds
        let db_ids: Vec<DatabaseId> = ids.iter().map(|&id| DatabaseId::new(id)).collect();

        for i in 0..db_ids.len() {
            for j in (i + 1)..db_ids.len() {
                if ids[i] != ids[j] {
                    prop_assert_ne!(
                        db_ids[i], db_ids[j],
                        "DatabaseId::new({}) == DatabaseId::new({})",
                        ids[i], ids[j]
                    );
                }
            }
        }
    }

    #[test]
    fn database_id_monotonically_increasing(
        a in any::<i64>(),
        b in any::<i64>()
    ) {
        let id_a = DatabaseId::new(a);
        let id_b = DatabaseId::new(b);

        // The ordering of DatabaseId should match the ordering of the underlying i64
        prop_assert_eq!(
            id_a.cmp(&id_b),
            a.cmp(&b),
            "DatabaseId ordering does not match i64 ordering for {} vs {}",
            a, b
        );
    }

    #[test]
    fn database_id_roundtrip(id_val in any::<i64>()) {
        let db_id = DatabaseId::new(id_val);
        prop_assert_eq!(db_id.id(), id_val);
        prop_assert_eq!(db_id.as_i64(), id_val);
    }

    #[test]
    fn database_id_serialization_roundtrip(id_val in any::<i64>()) {
        let original = DatabaseId::new(id_val);
        let mut buf = Vec::new();
        original.write_to_log(&mut buf);
        prop_assert_eq!(buf.len(), 8);

        let restored = DatabaseId::read_from_log(&buf).unwrap();
        prop_assert_eq!(original, restored);
    }

    #[test]
    fn database_id_equality_reflexive(id_val in any::<i64>()) {
        let id = DatabaseId::new(id_val);
        prop_assert_eq!(id, id);
    }

    #[test]
    fn database_id_equality_consistent(a in any::<i64>(), b in any::<i64>()) {
        let id_a = DatabaseId::new(a);
        let id_b = DatabaseId::new(b);
        if a == b {
            prop_assert_eq!(id_a, id_b);
        } else {
            prop_assert_ne!(id_a, id_b);
        }
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

proptest! {
    #[test]
    fn operation_status_display_not_empty(
        variant in prop_oneof![
            Just(OperationStatus::Success),
            Just(OperationStatus::NotFound),
            Just(OperationStatus::KeyExist),
            Just(OperationStatus::KeyEmpty),
        ]
    ) {
        let display = format!("{}", variant);
        prop_assert!(!display.is_empty(), "Display for {:?} should not be empty", variant);
    }
}
