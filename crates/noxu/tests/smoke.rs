//! Smoke test for the `noxu` umbrella crate.
//!
//! Verifies that:
//!   1. Core types (`Environment`, `Database`, `DatabaseEntry`) are accessible
//!      via `noxu::`.
//!   2. `#[derive(Entity)]` works when the user only depends on `noxu = "3"`.
//!   3. A basic put/get round-trip works end-to-end.

use noxu::{DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig};
use tempfile::TempDir;

fn temp_env() -> (TempDir, Environment) {
    let td = TempDir::new().unwrap();
    let env = Environment::open(
        EnvironmentConfig::new(td.path().to_path_buf()).with_allow_create(true),
    )
    .unwrap();
    (td, env)
}

/// A derived entity that uses `::noxu::persist::…` paths in its generated
/// impl — this test proves that `noxu = "3"` alone is sufficient.
#[cfg(feature = "persist")]
mod derived {
    use noxu::Environment;
    use noxu::persist::{
        DeleteAction, Entity, EntitySerializer, EntityStore, PersistError,
        PrimaryIndex, Relate, SecondaryKey, StoreConfig,
    };

    #[derive(Clone, Debug, PartialEq, Entity, SecondaryKey)]
    pub struct Widget {
        #[primary_key]
        pub id: u64,
        #[secondary_key(name = "by_sku", relate = OneToOne)]
        pub sku: String,
        pub label: String,
    }

    pub struct WidgetSer;

    impl EntitySerializer<Widget> for WidgetSer {
        fn serialize(&self, w: &Widget) -> noxu::persist::Result<Vec<u8>> {
            let mut buf = Vec::new();
            buf.extend_from_slice(&w.id.to_be_bytes());
            let sku = w.sku.as_bytes();
            buf.extend_from_slice(&(sku.len() as u32).to_be_bytes());
            buf.extend_from_slice(sku);
            let label = w.label.as_bytes();
            buf.extend_from_slice(&(label.len() as u32).to_be_bytes());
            buf.extend_from_slice(label);
            Ok(buf)
        }

        fn deserialize(&self, b: &[u8]) -> noxu::persist::Result<Widget> {
            if b.len() < 12 {
                return Err(PersistError::SerializationError("short".into()));
            }
            let id = u64::from_be_bytes(b[0..8].try_into().unwrap());
            let sku_len =
                u32::from_be_bytes(b[8..12].try_into().unwrap()) as usize;
            let mut pos = 12;
            let sku =
                String::from_utf8(b[pos..pos + sku_len].to_vec()).unwrap();
            pos += sku_len;
            let label_len =
                u32::from_be_bytes(b[pos..pos + 4].try_into().unwrap())
                    as usize;
            pos += 4;
            let label =
                String::from_utf8(b[pos..pos + label_len].to_vec()).unwrap();
            Ok(Widget { id, sku, label })
        }
    }

    pub fn run(env: &Environment) {
        let mut store = EntityStore::open(
            env,
            StoreConfig::new("widgets").with_allow_create(true),
        )
        .unwrap();
        let primary: PrimaryIndex<u64, Widget> =
            store.get_primary_index().unwrap();
        let ser = WidgetSer;

        let w = Widget { id: 1, sku: "W-001".into(), label: "Gadget".into() };
        primary.put(None, &ser, &w).unwrap();

        let got = primary.get(None, &ser, &1u64).unwrap().unwrap();
        assert_eq!(got, w);
        assert_eq!(<Widget as Entity>::entity_name(), "Widget");

        // Secondary index metadata.
        let specs = Widget::SECONDARY_INDEXES;
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].name, "by_sku");
        assert_eq!(specs[0].relate, Relate::OneToOne);
        assert_eq!(specs[0].on_related_entity_delete, DeleteAction::Abort);
    }
}

#[test]
fn smoke_core_open_put_get() {
    let (_td, env) = temp_env();
    let db_config = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, "kv", &db_config).unwrap();

    let key = DatabaseEntry::from_bytes(b"hello");
    let val = DatabaseEntry::from_bytes(b"world");
    db.put(&key, &val).unwrap();

    let key2 = DatabaseEntry::from_bytes(b"hello");
    let mut out = DatabaseEntry::new();
    let status2 = db.get_into(None, &key2, &mut out).unwrap();
    assert!(status2);
    assert_eq!(out.get_data(), Some(b"world".as_ref()));
}

#[cfg(feature = "persist")]
#[test]
fn smoke_derive_entity_round_trip() {
    let (_td, env) = temp_env();
    derived::run(&env);
}

#[cfg(feature = "persist")]
#[test]
fn smoke_derive_entity_name() {
    use noxu::persist::Entity;
    assert_eq!(<derived::Widget as Entity>::entity_name(), "Widget");
}

#[cfg(feature = "persist")]
#[test]
fn smoke_primary_key_derive_round_trip() {
    use noxu::persist::PrimaryKey;

    #[derive(Clone, Debug, PartialEq, Eq, Hash, PrimaryKey)]
    struct MyKey {
        zone: String,
        seq: u64,
    }

    let k = MyKey { zone: "eu-west-1".into(), seq: 99 };
    let bytes = k.to_bytes();
    let decoded = MyKey::from_bytes(&bytes).unwrap();
    assert_eq!(k, decoded);
}
