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
    // Truncated bytes — should fail cleanly, not panic.
    let result = CompositeKey::from_bytes(&[0u8, 0, 0, 5, b'h']);
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
