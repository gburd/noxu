//! Integration tests for the noxu-persist crate.
//!
//! Demonstrates full entity-store workflows using the helper types:
//! DatabaseNamer / KeySelector — removed in v1.5.1 Wave 1C audit cleanup
//! because they were exported but unwired.  Tests for those modules
//! were dropped along with the modules.  See `crates/noxu-persist/src/lib.rs`
//! and the persist-xa Low audit recommendations.
//!
//! SimpleSerializer, FieldEncoder/FieldDecoder, Sequence, and the
//! entity store are still covered here.

use noxu_db::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    OperationStatus,
};
use noxu_persist::entity::{Entity, PrimaryKey};
use noxu_persist::entity_serializer::EntitySerializer;
use noxu_persist::sequence::{MemorySequence, Sequence};
use noxu_persist::simple_serializer::{
    FieldDecoder, FieldEncoder, SimpleSerializer, decode_string, decode_u64,
    encode_string, encode_u64,
};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Test entity definitions
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
struct User {
    id: u64,
    name: String,
    email: String,
    age: u32,
}

impl Entity for User {
    type PrimaryKey = u64;
    fn primary_key(&self) -> &u64 {
        &self.id
    }
    fn entity_name() -> &'static str {
        "User"
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Product {
    sku: String,
    name: String,
    price_cents: u64,
    in_stock: bool,
}

impl Entity for Product {
    type PrimaryKey = String;
    fn primary_key(&self) -> &String {
        &self.sku
    }
    fn entity_name() -> &'static str {
        "Product"
    }
}

#[derive(Debug, Clone, PartialEq)]
struct LogEntry {
    id: u64,
    timestamp: i64,
    level: u8,
    message: String,
    context: Option<String>,
}

impl Entity for LogEntry {
    type PrimaryKey = u64;
    fn primary_key(&self) -> &u64 {
        &self.id
    }
    fn entity_name() -> &'static str {
        "LogEntry"
    }
}

// ---------------------------------------------------------------------------
// Serializer factories
// ---------------------------------------------------------------------------

fn user_serializer() -> SimpleSerializer<User> {
    SimpleSerializer::new(
        |user: &User| {
            let mut enc = FieldEncoder::new();
            enc.write_u64(user.id);
            enc.write_string(&user.name);
            enc.write_string(&user.email);
            enc.write_u32(user.age);
            Ok(enc.finish())
        },
        |bytes| {
            let mut dec = FieldDecoder::new(bytes);
            Ok(User {
                id: dec.read_u64()?,
                name: dec.read_string()?,
                email: dec.read_string()?,
                age: dec.read_u32()?,
            })
        },
    )
}

fn product_serializer() -> SimpleSerializer<Product> {
    SimpleSerializer::new(
        |p: &Product| {
            let mut enc = FieldEncoder::new();
            enc.write_string(&p.sku);
            enc.write_string(&p.name);
            enc.write_u64(p.price_cents);
            enc.write_bool(p.in_stock);
            Ok(enc.finish())
        },
        |bytes| {
            let mut dec = FieldDecoder::new(bytes);
            Ok(Product {
                sku: dec.read_string()?,
                name: dec.read_string()?,
                price_cents: dec.read_u64()?,
                in_stock: dec.read_bool()?,
            })
        },
    )
}

fn log_entry_serializer() -> SimpleSerializer<LogEntry> {
    SimpleSerializer::new(
        |entry: &LogEntry| {
            let mut enc = FieldEncoder::new();
            enc.write_u64(entry.id);
            enc.write_i64(entry.timestamp);
            enc.write_u8(entry.level);
            enc.write_string(&entry.message);
            enc.write_option_string(&entry.context);
            Ok(enc.finish())
        },
        |bytes| {
            let mut dec = FieldDecoder::new(bytes);
            Ok(LogEntry {
                id: dec.read_u64()?,
                timestamp: dec.read_i64()?,
                level: dec.read_u8()?,
                message: dec.read_string()?,
                context: dec.read_option_string()?,
            })
        },
    )
}

// ---------------------------------------------------------------------------
// Helper: open an env + database
// ---------------------------------------------------------------------------

fn open_env_db(name: &str) -> (TempDir, Environment, Database) {
    let temp_dir = TempDir::new().unwrap();
    let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(false);
    let env = Environment::open(env_config).unwrap();
    let db_config = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, name, &db_config).unwrap();
    (temp_dir, env, db)
}

// ---------------------------------------------------------------------------
// Integration tests: full entity workflow via raw Database
// ---------------------------------------------------------------------------

#[test]
fn test_user_put_get_round_trip() {
    let (_td, _env, db) = open_env_db("users");
    let ser = user_serializer();

    let user = User {
        id: 1,
        name: "Alice".into(),
        email: "alice@example.com".into(),
        age: 30,
    };

    // Serialize and store
    let key = DatabaseEntry::from_bytes(&user.primary_key().to_bytes());
    let data = DatabaseEntry::from_bytes(&ser.serialize(&user).unwrap());
    assert_eq!(db.put(None, &key, &data).unwrap(), OperationStatus::Success);

    // Retrieve and deserialize
    let mut retrieved = DatabaseEntry::new();
    assert_eq!(
        db.get(None, &key, &mut retrieved).unwrap(),
        OperationStatus::Success
    );
    let decoded: User = ser.deserialize(retrieved.data()).unwrap();
    assert_eq!(decoded, user);
}

#[test]
fn test_user_update() {
    let (_td, _env, db) = open_env_db("users");
    let ser = user_serializer();

    let mut user = User {
        id: 1,
        name: "Alice".into(),
        email: "alice@example.com".into(),
        age: 30,
    };

    let key = DatabaseEntry::from_bytes(&user.primary_key().to_bytes());
    let data = DatabaseEntry::from_bytes(&ser.serialize(&user).unwrap());
    db.put(None, &key, &data).unwrap();

    // Update
    user.age = 31;
    user.email = "alice@newdomain.com".into();
    let data = DatabaseEntry::from_bytes(&ser.serialize(&user).unwrap());
    db.put(None, &key, &data).unwrap();

    // Verify update
    let mut retrieved = DatabaseEntry::new();
    db.get(None, &key, &mut retrieved).unwrap();
    let decoded: User = ser.deserialize(retrieved.data()).unwrap();
    assert_eq!(decoded.age, 31);
    assert_eq!(decoded.email, "alice@newdomain.com");
}

#[test]
fn test_user_delete() {
    let (_td, _env, db) = open_env_db("users");
    let ser = user_serializer();

    let user = User {
        id: 1,
        name: "Alice".into(),
        email: "alice@example.com".into(),
        age: 30,
    };

    let key = DatabaseEntry::from_bytes(&user.primary_key().to_bytes());
    let data = DatabaseEntry::from_bytes(&ser.serialize(&user).unwrap());
    db.put(None, &key, &data).unwrap();

    // Delete
    assert_eq!(db.delete(None, &key).unwrap(), OperationStatus::Success);

    // Verify gone
    let mut retrieved = DatabaseEntry::new();
    assert_eq!(
        db.get(None, &key, &mut retrieved).unwrap(),
        OperationStatus::NotFound
    );
}

#[test]
fn test_product_with_string_key() {
    let (_td, _env, db) = open_env_db("products");
    let ser = product_serializer();

    let product = Product {
        sku: "WIDGET-001".into(),
        name: "Blue Widget".into(),
        price_cents: 1999,
        in_stock: true,
    };

    let key = DatabaseEntry::from_bytes(&product.primary_key().to_bytes());
    let data = DatabaseEntry::from_bytes(&ser.serialize(&product).unwrap());
    db.put(None, &key, &data).unwrap();

    let mut retrieved = DatabaseEntry::new();
    db.get(None, &key, &mut retrieved).unwrap();
    let decoded: Product = ser.deserialize(retrieved.data()).unwrap();
    assert_eq!(decoded, product);
}

#[test]
fn test_log_entry_with_optional_fields() {
    let (_td, _env, db) = open_env_db("logs");
    let ser = log_entry_serializer();

    let entry_with_context = LogEntry {
        id: 1,
        timestamp: 1700000000,
        level: 3,
        message: "Something happened".into(),
        context: Some("request_id=abc123".into()),
    };

    let entry_without_context = LogEntry {
        id: 2,
        timestamp: 1700000001,
        level: 1,
        message: "Debug info".into(),
        context: None,
    };

    for entry in &[&entry_with_context, &entry_without_context] {
        let key = DatabaseEntry::from_bytes(&entry.primary_key().to_bytes());
        let data = DatabaseEntry::from_bytes(&ser.serialize(entry).unwrap());
        db.put(None, &key, &data).unwrap();
    }

    // Retrieve both
    for original in &[&entry_with_context, &entry_without_context] {
        let key = DatabaseEntry::from_bytes(&original.primary_key().to_bytes());
        let mut retrieved = DatabaseEntry::new();
        db.get(None, &key, &mut retrieved).unwrap();
        let decoded: LogEntry = ser.deserialize(retrieved.data()).unwrap();
        assert_eq!(&decoded, *original);
    }
}

#[test]
fn test_multiple_entity_types_same_environment() {
    let temp_dir = TempDir::new().unwrap();
    let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(false);
    let env = Environment::open(env_config).unwrap();

    let user_db_name = format!("persist_{}_{}", "store", User::entity_name());
    let product_db_name =
        format!("persist_{}_{}", "store", Product::entity_name());

    let db_config = DatabaseConfig::new().with_allow_create(true);
    let user_db = env.open_database(None, &user_db_name, &db_config).unwrap();
    let product_db =
        env.open_database(None, &product_db_name, &db_config).unwrap();

    let user_ser = user_serializer();
    let product_ser = product_serializer();

    // Store a user
    let user = User {
        id: 1,
        name: "Bob".into(),
        email: "bob@test.com".into(),
        age: 25,
    };
    let key = DatabaseEntry::from_bytes(&user.primary_key().to_bytes());
    let data = DatabaseEntry::from_bytes(&user_ser.serialize(&user).unwrap());
    user_db.put(None, &key, &data).unwrap();

    // Store a product
    let product = Product {
        sku: "ABC".into(),
        name: "Widget".into(),
        price_cents: 500,
        in_stock: false,
    };
    let key = DatabaseEntry::from_bytes(&product.primary_key().to_bytes());
    let data =
        DatabaseEntry::from_bytes(&product_ser.serialize(&product).unwrap());
    product_db.put(None, &key, &data).unwrap();

    // Verify independent storage
    assert_eq!(user_db.count().unwrap(), 1);
    assert_eq!(product_db.count().unwrap(), 1);
}

// ---------------------------------------------------------------------------
// Integration tests: Sequence
// ---------------------------------------------------------------------------

#[test]
fn test_sequence_generates_unique_ids_for_entities() {
    let (_td, _env, db) = open_env_db("seq_test");
    let seq = Sequence::new(&db, "user_id").unwrap();
    let ser = user_serializer();

    // Use sequence to assign IDs
    let mut users = Vec::new();
    for i in 0..5 {
        let id = seq.next().unwrap();
        let user = User {
            id,
            name: format!("User{}", i),
            email: format!("user{}@test.com", i),
            age: 20 + i as u32,
        };

        let key = DatabaseEntry::from_bytes(&user.primary_key().to_bytes());
        let data = DatabaseEntry::from_bytes(&ser.serialize(&user).unwrap());
        db.put(None, &key, &data).unwrap();
        users.push(user);
    }

    // Verify all are retrievable with distinct IDs
    for user in &users {
        let key = DatabaseEntry::from_bytes(&user.primary_key().to_bytes());
        let mut retrieved = DatabaseEntry::new();
        assert_eq!(
            db.get(None, &key, &mut retrieved).unwrap(),
            OperationStatus::Success
        );
        let decoded = ser.deserialize(retrieved.data()).unwrap();
        assert_eq!(&decoded, user);
    }

    // IDs should be 1..=5
    let ids: Vec<u64> = users.iter().map(|u| u.id).collect();
    assert_eq!(ids, vec![1, 2, 3, 4, 5]);
}

#[test]
fn test_memory_sequence_for_testing() {
    let seq = MemorySequence::starting_at(1000);
    let id1 = seq.next();
    let id2 = seq.next();
    let id3 = seq.next();
    assert_eq!(id1, 1000);
    assert_eq!(id2, 1001);
    assert_eq!(id3, 1002);
}

// ---------------------------------------------------------------------------
// Integration tests: FieldEncoder/FieldDecoder comprehensive
// ---------------------------------------------------------------------------

#[test]
fn test_field_encoder_all_types_round_trip() {
    let mut enc = FieldEncoder::new();
    enc.write_u8(255);
    enc.write_u16(60000);
    enc.write_u32(3_000_000_000);
    enc.write_u64(u64::MAX);
    enc.write_i8(-128);
    enc.write_i16(-30000);
    enc.write_i32(-2_000_000_000);
    enc.write_i64(i64::MIN);
    enc.write_f32(std::f32::consts::PI);
    enc.write_f64(std::f64::consts::E);
    enc.write_bool(true);
    enc.write_bool(false);
    enc.write_string("hello, world!");
    enc.write_bytes(&[0xCA, 0xFE, 0xBA, 0xBE]);
    enc.write_option_string(&Some("present".into()));
    enc.write_option_string(&None);
    enc.write_option_u64(&Some(42));
    enc.write_option_u64(&None);

    let bytes = enc.finish();
    let mut dec = FieldDecoder::new(&bytes);

    assert_eq!(dec.read_u8().unwrap(), 255);
    assert_eq!(dec.read_u16().unwrap(), 60000);
    assert_eq!(dec.read_u32().unwrap(), 3_000_000_000);
    assert_eq!(dec.read_u64().unwrap(), u64::MAX);
    assert_eq!(dec.read_i8().unwrap(), -128);
    assert_eq!(dec.read_i16().unwrap(), -30000);
    assert_eq!(dec.read_i32().unwrap(), -2_000_000_000);
    assert_eq!(dec.read_i64().unwrap(), i64::MIN);
    assert!((dec.read_f32().unwrap() - std::f32::consts::PI).abs() < 0.001);
    assert!((dec.read_f64().unwrap() - std::f64::consts::E).abs() < 1e-9);
    assert!(dec.read_bool().unwrap());
    assert!(!dec.read_bool().unwrap());
    assert_eq!(dec.read_string().unwrap(), "hello, world!");
    assert_eq!(dec.read_bytes().unwrap(), vec![0xCA, 0xFE, 0xBA, 0xBE]);
    assert_eq!(dec.read_option_string().unwrap(), Some("present".into()));
    assert_eq!(dec.read_option_string().unwrap(), None);
    assert_eq!(dec.read_option_u64().unwrap(), Some(42));
    assert_eq!(dec.read_option_u64().unwrap(), None);
    assert!(dec.is_exhausted());
}

#[test]
fn test_field_encoder_empty_strings_and_bytes() {
    let mut enc = FieldEncoder::new();
    enc.write_string("");
    enc.write_bytes(&[]);
    enc.write_option_string(&Some(String::new()));

    let bytes = enc.finish();
    let mut dec = FieldDecoder::new(&bytes);

    assert_eq!(dec.read_string().unwrap(), "");
    assert_eq!(dec.read_bytes().unwrap(), Vec::<u8>::new());
    assert_eq!(dec.read_option_string().unwrap(), Some(String::new()));
    assert!(dec.is_exhausted());
}

#[test]
fn test_field_encoder_unicode_strings() {
    let test_strings = vec![
        "caf\u{00E9}",
        "\u{1F600}\u{1F601}\u{1F602}",
        "\u{4F60}\u{597D}\u{4E16}\u{754C}",
        "",
        "plain ascii",
    ];

    let mut enc = FieldEncoder::new();
    for s in &test_strings {
        enc.write_string(s);
    }

    let bytes = enc.finish();
    let mut dec = FieldDecoder::new(&bytes);

    for expected in &test_strings {
        assert_eq!(dec.read_string().unwrap(), *expected);
    }
    assert!(dec.is_exhausted());
}

#[test]
fn test_field_encoder_large_payload() {
    let large_string = "x".repeat(100_000);
    let large_bytes = vec![0xABu8; 50_000];

    let mut enc = FieldEncoder::new();
    enc.write_string(&large_string);
    enc.write_bytes(&large_bytes);

    let bytes = enc.finish();
    let mut dec = FieldDecoder::new(&bytes);

    assert_eq!(dec.read_string().unwrap(), large_string);
    assert_eq!(dec.read_bytes().unwrap(), large_bytes);
}

// ---------------------------------------------------------------------------
// Integration tests: free-standing encode/decode helpers
// ---------------------------------------------------------------------------

#[test]
fn test_encode_decode_u64_helpers() {
    for val in [0u64, 1, 42, 256, 65536, u64::MAX / 2, u64::MAX] {
        let encoded = encode_u64(val);
        let decoded = decode_u64(&encoded).unwrap();
        assert_eq!(val, decoded);
    }
}

#[test]
fn test_encode_decode_string_helpers() {
    for s in ["", "hello", "caf\u{00E9}", "\u{1F600}"] {
        let encoded = encode_string(s);
        let mut offset = 0;
        let decoded = decode_string(&encoded, &mut offset).unwrap();
        assert_eq!(s, decoded);
        assert_eq!(offset, encoded.len());
    }
}

#[test]
fn test_decode_string_multiple_in_buffer() {
    let mut buf = Vec::new();
    buf.extend_from_slice(&encode_string("first"));
    buf.extend_from_slice(&encode_string("second"));
    buf.extend_from_slice(&encode_string("third"));

    let mut offset = 0;
    assert_eq!(decode_string(&buf, &mut offset).unwrap(), "first");
    assert_eq!(decode_string(&buf, &mut offset).unwrap(), "second");
    assert_eq!(decode_string(&buf, &mut offset).unwrap(), "third");
    assert_eq!(offset, buf.len());
}

// ---------------------------------------------------------------------------
// Integration tests: PrimaryKey trait
// ---------------------------------------------------------------------------

#[test]
fn test_primary_key_u64_ordering_preserved() {
    // Big-endian encoding preserves sort order for unsigned types
    let values: Vec<u64> = vec![0, 1, 255, 256, 65535, 65536, u64::MAX];
    let encoded: Vec<Vec<u8>> = values.iter().map(|v| v.to_bytes()).collect();
    for i in 0..encoded.len() - 1 {
        assert!(encoded[i] < encoded[i + 1]);
    }
}

#[test]
fn test_primary_key_string_round_trip() {
    let key = "my-entity-key".to_string();
    let bytes = key.to_bytes();
    let decoded = String::from_bytes(&bytes).unwrap();
    assert_eq!(key, decoded);
}

// ---------------------------------------------------------------------------
// Integration tests: EntityStore workflow (via PrimaryIndex from other agent)
// ---------------------------------------------------------------------------

#[test]
fn test_entity_store_full_crud() {
    // This test uses raw Database operations to simulate what EntityStore does
    let temp_dir = TempDir::new().unwrap();
    let env_config = EnvironmentConfig::new(temp_dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(false);
    let env = Environment::open(env_config).unwrap();

    let db_name = format!("persist_{}_{}", "test_store", User::entity_name());
    let db_config = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, &db_name, &db_config).unwrap();

    let ser = user_serializer();

    // CREATE
    let alice = User {
        id: 1,
        name: "Alice".into(),
        email: "alice@test.com".into(),
        age: 30,
    };
    let bob = User {
        id: 2,
        name: "Bob".into(),
        email: "bob@test.com".into(),
        age: 25,
    };

    for user in [&alice, &bob] {
        let key = DatabaseEntry::from_bytes(&user.primary_key().to_bytes());
        let data = DatabaseEntry::from_bytes(&ser.serialize(user).unwrap());
        db.put(None, &key, &data).unwrap();
    }

    assert_eq!(db.count().unwrap(), 2);

    // READ
    let key = DatabaseEntry::from_bytes(&1u64.to_bytes());
    let mut retrieved = DatabaseEntry::new();
    db.get(None, &key, &mut retrieved).unwrap();
    let read_alice: User = ser.deserialize(retrieved.data()).unwrap();
    assert_eq!(read_alice, alice);

    // UPDATE
    let updated_alice = User {
        id: 1,
        name: "Alice Smith".into(),
        email: "alice.smith@test.com".into(),
        age: 31,
    };
    let key =
        DatabaseEntry::from_bytes(&updated_alice.primary_key().to_bytes());
    let data =
        DatabaseEntry::from_bytes(&ser.serialize(&updated_alice).unwrap());
    db.put(None, &key, &data).unwrap();

    let mut retrieved = DatabaseEntry::new();
    db.get(None, &key, &mut retrieved).unwrap();
    let reread: User = ser.deserialize(retrieved.data()).unwrap();
    assert_eq!(reread.name, "Alice Smith");
    assert_eq!(reread.age, 31);

    // DELETE
    let key = DatabaseEntry::from_bytes(&2u64.to_bytes());
    db.delete(None, &key).unwrap();
    assert_eq!(db.count().unwrap(), 1);

    // Verify Bob is gone
    let mut retrieved = DatabaseEntry::new();
    assert_eq!(
        db.get(None, &key, &mut retrieved).unwrap(),
        OperationStatus::NotFound
    );
}

#[test]
fn test_many_entities() {
    let (_td, _env, db) = open_env_db("many_users");
    let ser = user_serializer();

    // Insert 100 entities
    for i in 0..100u64 {
        let user = User {
            id: i,
            name: format!("User {}", i),
            email: format!("user{}@test.com", i),
            age: (20 + i % 50) as u32,
        };
        let key = DatabaseEntry::from_bytes(&user.primary_key().to_bytes());
        let data = DatabaseEntry::from_bytes(&ser.serialize(&user).unwrap());
        db.put(None, &key, &data).unwrap();
    }

    assert_eq!(db.count().unwrap(), 100);

    // Read them all back
    for i in 0..100u64 {
        let key = DatabaseEntry::from_bytes(&i.to_bytes());
        let mut retrieved = DatabaseEntry::new();
        assert_eq!(
            db.get(None, &key, &mut retrieved).unwrap(),
            OperationStatus::Success
        );
        let decoded: User = ser.deserialize(retrieved.data()).unwrap();
        assert_eq!(decoded.id, i);
        assert_eq!(decoded.name, format!("User {}", i));
    }
}
