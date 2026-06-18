//! Integration tests for the `#[derive(Entity)]`, `#[derive(PrimaryKey)]`,
//! and `#[derive(SecondaryKey)]` proc-macros.
//!
//! These tests exercise the *public* surface of `noxu-persist`: they
//! verify that the macro-derived `Entity` / `PrimaryKey` impls and
//! generated secondary-index helpers produce identical behaviour to the
//! manual trait impls already covered in `entity_store.rs` /
//! `secondary_index.rs`.  If a future change to the derive output drifts
//! from the manual path, these tests catch it.

use noxu_db::{Environment, EnvironmentConfig};
use noxu_persist::{
    DeleteAction, Entity, EntitySerializer, EntityStore, PersistError,
    PrimaryIndex, PrimaryKey, Relate, SecondaryKey, StoreConfig,
};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Derived entity with primary key and two secondary keys.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Entity, SecondaryKey)]
#[entity(name = "DerivedUser")]
struct User {
    #[primary_key]
    id: u64,
    #[secondary_key(name = "by_email", relate = OneToOne)]
    email: String,
    #[secondary_key(
        name = "by_dept",
        relate = ManyToOne,
        related_entity = "Department",
        on_related_entity_delete = NULLIFY
    )]
    dept: Option<u64>,
    name: String,
}

struct UserSerializer;

impl EntitySerializer<User> for UserSerializer {
    fn serialize(&self, u: &User) -> noxu_persist::Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&u.id.to_be_bytes());
        let email = u.email.as_bytes();
        buf.extend_from_slice(&(email.len() as u32).to_be_bytes());
        buf.extend_from_slice(email);
        match u.dept {
            None => buf.push(0),
            Some(d) => {
                buf.push(1);
                buf.extend_from_slice(&d.to_be_bytes());
            }
        }
        let name = u.name.as_bytes();
        buf.extend_from_slice(&(name.len() as u32).to_be_bytes());
        buf.extend_from_slice(name);
        Ok(buf)
    }

    fn deserialize(&self, bytes: &[u8]) -> noxu_persist::Result<User> {
        if bytes.len() < 12 {
            return Err(PersistError::SerializationError("short user".into()));
        }
        let id = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
        let email_len =
            u32::from_be_bytes(bytes[8..12].try_into().unwrap()) as usize;
        let mut pos = 12;
        let email =
            String::from_utf8(bytes[pos..pos + email_len].to_vec()).unwrap();
        pos += email_len;
        let has_dept = bytes[pos];
        pos += 1;
        let dept = if has_dept == 0 {
            None
        } else {
            let d = u64::from_be_bytes(bytes[pos..pos + 8].try_into().unwrap());
            pos += 8;
            Some(d)
        };
        let name_len =
            u32::from_be_bytes(bytes[pos..pos + 4].try_into().unwrap())
                as usize;
        pos += 4;
        let name =
            String::from_utf8(bytes[pos..pos + name_len].to_vec()).unwrap();
        Ok(User { id, email, dept, name })
    }
}

// ---------------------------------------------------------------------------
// Composite key derived via `#[derive(PrimaryKey)]`.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, PrimaryKey)]
struct CompositeKey {
    region: String,
    customer_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, PrimaryKey)]
struct NewtypeKey(u64);

// PERSIST-COMP-1 round-trip matrix: every field-type combination must
// round-trip AND preserve order under the new no-length-prefix encoding.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, PrimaryKey)]
struct FixedFixedKey(u64, i32); // fixed + fixed
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, PrimaryKey)]
struct FixedVarKey(u32, String); // fixed + var
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, PrimaryKey)]
struct VarVarKey(String, Vec<u8>); // var + var
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, PrimaryKey)]
struct ThreeFieldKey {
    a: String,
    b: u32,
    c: Vec<u8>,
}

fn assert_rt_and_order<K: PrimaryKey + Ord + std::fmt::Debug>(
    mut keys: Vec<K>,
) {
    // Round-trip every key.
    for k in &keys {
        let enc = k.to_bytes();
        let dec = K::from_bytes(&enc).unwrap();
        assert_eq!(*k, dec, "round-trip mismatch");
    }
    // Encoded byte order must equal logical (derived Ord) order.
    keys.sort();
    let mut enc: Vec<Vec<u8>> = keys.iter().map(K::to_bytes).collect();
    let logical = enc.clone();
    enc.sort();
    assert_eq!(enc, logical, "encoded order must match logical order");
}

#[test]
fn composite_key_matrix_round_trip_and_order() {
    assert_rt_and_order(vec![
        FixedFixedKey(1, -5),
        FixedFixedKey(1, 5),
        FixedFixedKey(2, i32::MIN),
        FixedFixedKey(0, i32::MAX),
    ]);
    assert_rt_and_order(vec![
        FixedVarKey(1, "b".into()),
        FixedVarKey(1, "aa".into()),
        FixedVarKey(1, "a".into()),
        FixedVarKey(2, "a".into()),
        FixedVarKey(0, "zzz".into()),
    ]);
    assert_rt_and_order(vec![
        VarVarKey("a".into(), vec![2]),
        VarVarKey("a".into(), vec![1]),
        VarVarKey("aa".into(), vec![0]),
        VarVarKey("b".into(), vec![]),
        VarVarKey("".into(), vec![255]),
    ]);
    assert_rt_and_order(vec![
        ThreeFieldKey { a: "x".into(), b: 1, c: vec![1] },
        ThreeFieldKey { a: "x".into(), b: 1, c: vec![2] },
        ThreeFieldKey { a: "x".into(), b: 2, c: vec![] },
        ThreeFieldKey { a: "xx".into(), b: 0, c: vec![] },
        ThreeFieldKey { a: "y".into(), b: 0, c: vec![] },
    ]);
}

#[derive(Clone, Debug, PartialEq, Entity)]
struct Order {
    #[primary_key]
    key: CompositeKey,
    total_cents: u64,
}

struct OrderSerializer;

impl EntitySerializer<Order> for OrderSerializer {
    fn serialize(&self, o: &Order) -> noxu_persist::Result<Vec<u8>> {
        let mut buf = Vec::new();
        let kb = o.key.to_bytes();
        buf.extend_from_slice(&(kb.len() as u32).to_be_bytes());
        buf.extend_from_slice(&kb);
        buf.extend_from_slice(&o.total_cents.to_be_bytes());
        Ok(buf)
    }
    fn deserialize(&self, bytes: &[u8]) -> noxu_persist::Result<Order> {
        let kl = u32::from_be_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let key = CompositeKey::from_bytes(&bytes[4..4 + kl])?;
        let total_cents =
            u64::from_be_bytes(bytes[4 + kl..12 + kl].try_into().unwrap());
        Ok(Order { key, total_cents })
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn temp_env() -> (TempDir, Environment) {
    let td = TempDir::new().unwrap();
    let env = Environment::open(
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true),
    )
    .unwrap();
    (td, env)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn derived_entity_round_trip_via_store() {
    let (_td, env) = temp_env();
    let mut store =
        EntityStore::open(&env, StoreConfig::new("s").with_allow_create(true))
            .unwrap();
    let mut primary: PrimaryIndex<u64, User> =
        store.get_primary_index().unwrap();
    let ser = UserSerializer;

    let by_email = User::open_by_email_index(&mut primary);
    let by_dept = User::open_by_dept_index(&mut primary);

    let alice = User {
        id: 1,
        email: "alice@example.com".into(),
        dept: Some(10),
        name: "Alice".into(),
    };
    let bob = User {
        id: 2,
        email: "bob@example.com".into(),
        dept: Some(10),
        name: "Bob".into(),
    };
    let carol = User {
        id: 3,
        email: "carol@example.com".into(),
        dept: None,
        name: "Carol".into(),
    };

    primary.put(None, &ser, &alice).unwrap();
    primary.put(None, &ser, &bob).unwrap();
    primary.put(None, &ser, &carol).unwrap();

    // Primary read.
    assert_eq!(primary.get(None, &ser, &1u64).unwrap(), Some(alice.clone()));
    assert_eq!(primary.count().unwrap(), 3);

    // Secondary by email — OneToOne.
    let found = by_email
        .get(None, &ser, &primary, &"alice@example.com".to_string())
        .unwrap();
    assert_eq!(found, Some(alice));

    // Secondary by dept — ManyToOne with Option<u64>.
    let dept10 = by_dept.sub_index(&10u64);
    assert_eq!(dept10.len(), 2);

    // Carol has dept = None and must be absent from by_dept.
    assert!(!by_dept.contains(&999u64));
    assert_eq!(by_dept.keys_index().len(), 2);

    // Delete via secondary cascades to primary and clears email index.
    let removed = by_email
        .delete(None, &ser, &primary, &"alice@example.com".to_string())
        .unwrap();
    assert!(removed);
    assert_eq!(primary.get(None, &ser, &1u64).unwrap(), None);
    assert!(!by_dept.contains(&10u64) || by_dept.sub_index(&10u64).len() == 1);
}

#[test]
fn derived_entity_name_override() {
    assert_eq!(<User as Entity>::entity_name(), "DerivedUser");
}

#[test]
fn derived_entity_default_entity_name() {
    #[derive(Clone, Entity)]
    struct DefaultName {
        #[primary_key]
        id: u64,
    }
    assert_eq!(<DefaultName as Entity>::entity_name(), "DefaultName");
}

#[test]
fn derived_secondary_index_metadata() {
    // SECONDARY_INDEXES const is generated by the SecondaryKey derive.
    let specs = User::SECONDARY_INDEXES;
    assert_eq!(specs.len(), 2);

    let by_email = specs.iter().find(|s| s.name == "by_email").unwrap();
    assert_eq!(by_email.relate, Relate::OneToOne);
    assert_eq!(by_email.related_entity, None);
    assert_eq!(by_email.on_related_entity_delete, DeleteAction::Abort);

    let by_dept = specs.iter().find(|s| s.name == "by_dept").unwrap();
    assert_eq!(by_dept.relate, Relate::ManyToOne);
    assert_eq!(by_dept.related_entity, Some("Department"));
    assert_eq!(by_dept.on_related_entity_delete, DeleteAction::Nullify);
}

#[test]
fn derived_composite_primary_key_round_trip() {
    let key = CompositeKey { region: "us-east-1".into(), customer_id: 42 };
    let bytes = key.to_bytes();
    let decoded = CompositeKey::from_bytes(&bytes).unwrap();
    assert_eq!(key, decoded);
}

/// PERSIST-COMP-1 regression: the on-disk byte order of a composite key must
/// match the logical tuple order `(region, customer_id)`. With the old
/// length-prefix encoding, `("aa", 1)` sorted AFTER `("b", 1)` because the
/// 4-byte length prefix `len("aa")=2 > len("b")=1` dominated the comparison —
/// silently corrupting ordered iteration / range scans.
#[test]
fn composite_primary_key_encoded_order_matches_logical_order() {
    // Keys chosen so the variable-length first field has DIFFERENT lengths
    // that would invert order under a length-prefix scheme.
    let mut keys = [
        CompositeKey { region: "b".into(), customer_id: 1 },
        CompositeKey { region: "aa".into(), customer_id: 1 },
        CompositeKey { region: "a".into(), customer_id: 9 },
        CompositeKey { region: "a".into(), customer_id: 1 },
        CompositeKey { region: "aaa".into(), customer_id: 0 },
    ];
    // Logical order via derived Ord on (region, customer_id).
    keys.sort();

    // Encoded order must equal logical order.
    let mut encoded: Vec<Vec<u8>> =
        keys.iter().map(CompositeKey::to_bytes).collect();
    let logical_then_encoded = encoded.clone();
    encoded.sort();
    assert_eq!(
        encoded, logical_then_encoded,
        "encoded composite-key byte order must match logical tuple order"
    );

    // Explicit ladder check: ("aa",1) must encode BEFORE ("b",1).
    let aa = CompositeKey { region: "aa".into(), customer_id: 1 }.to_bytes();
    let b = CompositeKey { region: "b".into(), customer_id: 1 }.to_bytes();
    assert!(aa < b, "(\"aa\",1) must sort before (\"b\",1)");
}

#[test]
fn derived_newtype_primary_key_round_trip() {
    let key = NewtypeKey(0xCAFEBABE);
    let bytes = key.to_bytes();
    // Newtype delegates to inner u64 → 8 bytes.
    assert_eq!(bytes.len(), 8);
    let decoded = NewtypeKey::from_bytes(&bytes).unwrap();
    assert_eq!(key, decoded);
}

#[test]
fn derived_composite_primary_key_short_input_errors() {
    // Truncated bytes — should fail cleanly, not panic. A single 0x00 byte
    // is an incomplete string terminator (terminator is 0x00 0x00).
    let result = CompositeKey::from_bytes(&[0u8]);
    assert!(matches!(result, Err(PersistError::SerializationError(_))));
}

#[test]
fn derived_entity_with_composite_key_full_crud() {
    let (_td, env) = temp_env();
    let mut store =
        EntityStore::open(&env, StoreConfig::new("o").with_allow_create(true))
            .unwrap();
    let primary: PrimaryIndex<CompositeKey, Order> =
        store.get_primary_index().unwrap();
    let ser = OrderSerializer;

    let key1 = CompositeKey { region: "us-west-2".into(), customer_id: 1 };
    let key2 = CompositeKey { region: "us-east-1".into(), customer_id: 2 };

    primary
        .put(None, &ser, &Order { key: key1.clone(), total_cents: 1000 })
        .unwrap();
    primary
        .put(None, &ser, &Order { key: key2.clone(), total_cents: 2000 })
        .unwrap();

    let got1 = primary.get(None, &ser, &key1).unwrap().unwrap();
    assert_eq!(got1.total_cents, 1000);

    let got2 = primary.get(None, &ser, &key2).unwrap().unwrap();
    assert_eq!(got2.total_cents, 2000);

    assert!(primary.delete(None, &key1).unwrap());
    assert_eq!(primary.get(None, &ser, &key1).unwrap(), None);
}

#[test]
fn derived_secondary_helper_extractor_handles_option_none() {
    let (_td, env) = temp_env();
    let mut store =
        EntityStore::open(&env, StoreConfig::new("s2").with_allow_create(true))
            .unwrap();
    let mut primary: PrimaryIndex<u64, User> =
        store.get_primary_index().unwrap();
    let by_dept = User::open_by_dept_index(&mut primary);
    let ser = UserSerializer;

    let nodept =
        User { id: 7, email: "x@y.z".into(), dept: None, name: "X".into() };
    primary.put(None, &ser, &nodept).unwrap();

    // Option<u64> = None → entity excluded from the index.
    assert!(by_dept.keys_index().is_empty());
    assert!(!by_dept.contains(&0u64));
}

// ---------------------------------------------------------------------------
// Standalone crate-override tests: #[entity(crate = "noxu_persist")]
//
// These tests use `noxu_persist` directly (no umbrella) with the
// crate-path override attribute.  They document and verify the Wave FA
// escape hatch for direct noxu-persist users.
// ---------------------------------------------------------------------------

/// Newtype PrimaryKey with `#[entity(crate = "noxu_persist")]` round-trips
/// correctly when the generated code resolves to `::noxu_persist::…`
/// instead of `::noxu::persist::…`.
#[test]
fn standalone_crate_override_newtype_primary_key_round_trip() {
    #[derive(Clone, Debug, PartialEq, Eq, Hash, PrimaryKey)]
    #[entity(crate = "noxu_persist")]
    struct StandaloneId(u64);

    let key = StandaloneId(0xDEAD_BEEF);
    let bytes = key.to_bytes();
    assert_eq!(bytes.len(), 8); // delegates to u64 → 8 bytes
    let decoded = StandaloneId::from_bytes(&bytes).unwrap();
    assert_eq!(key, decoded);
}

/// Composite PrimaryKey with `#[entity(crate = "noxu_persist")]` encodes
/// and decodes correctly.
#[test]
fn standalone_crate_override_composite_primary_key_round_trip() {
    #[derive(Clone, Debug, PartialEq, Eq, Hash, PrimaryKey)]
    #[entity(crate = "noxu_persist")]
    struct RegionKey {
        region: String,
        shard: u64,
    }

    let key = RegionKey { region: "eu-west-1".into(), shard: 7 };
    let bytes = key.to_bytes();
    let decoded = RegionKey::from_bytes(&bytes).unwrap();
    assert_eq!(key, decoded);

    // Truncated input must fail cleanly, not panic.
    let result = RegionKey::from_bytes(&bytes[..3]);
    assert!(matches!(result, Err(PersistError::SerializationError(_))));
}

/// `derive(Entity)` with `#[entity(crate = "noxu_persist")]` produces the
/// correct entity name and primary-key accessor.
#[test]
fn standalone_crate_override_entity_derive_compiles_and_name_resolves() {
    #[derive(Clone, Debug, PartialEq, Entity)]
    #[entity(crate = "noxu_persist", name = "StandaloneWidget")]
    struct Widget {
        #[primary_key]
        id: u64,
        label: String,
    }

    assert_eq!(<Widget as Entity>::entity_name(), "StandaloneWidget");

    let w = Widget { id: 5, label: "bolt".into() };
    assert_eq!(*w.primary_key(), 5u64);
}

/// `derive(SecondaryKey)` with `#[entity(crate = "noxu_persist")]` emits
/// the `SECONDARY_INDEXES` const and `open_*_index` helpers correctly.
#[test]
fn standalone_crate_override_secondary_key_metadata() {
    #[derive(Clone, Debug, PartialEq, Entity, SecondaryKey)]
    #[entity(crate = "noxu_persist")]
    struct Product {
        #[primary_key]
        sku: u64,
        #[secondary_key(name = "by_category", relate = ManyToOne)]
        category: String,
        #[secondary_key(
            name = "by_supplier",
            relate = ManyToOne,
            related_entity = "Supplier",
            on_related_entity_delete = Cascade
        )]
        supplier_id: Option<u64>,
    }

    let specs = Product::SECONDARY_INDEXES;
    assert_eq!(specs.len(), 2);

    let cat = specs.iter().find(|s| s.name == "by_category").unwrap();
    assert_eq!(cat.relate, Relate::ManyToOne);
    assert_eq!(cat.related_entity, None);
    assert_eq!(cat.on_related_entity_delete, DeleteAction::Abort);

    let sup = specs.iter().find(|s| s.name == "by_supplier").unwrap();
    assert_eq!(sup.relate, Relate::ManyToOne);
    assert_eq!(sup.related_entity, Some("Supplier"));
    assert_eq!(sup.on_related_entity_delete, DeleteAction::Cascade);
}
