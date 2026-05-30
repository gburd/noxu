//! Persist example for Noxu DB.
//!
//! Example showing Noxu DB entity persistence (Direct Persistence Layer).
//! Persistence Layer (DPL) demo.
//!
//! Demonstrates the noxu-persist API:
//!   - Define entity types (Person) with primary and secondary keys
//!   - Open an Environment and an EntityStore
//!   - Obtain a PrimaryIndex
//!   - Store, retrieve, update, and delete entities
//!   - Iterate over all entities in key order

use noxu::persist::{
    Entity, EntitySerializer, EntityStore, PersistError, PrimaryIndex,
    StoreConfig,
};
use noxu::{Environment, EnvironmentConfig};

// ---------------------------------------------------------------------------
// Entity definition
// ---------------------------------------------------------------------------

/// Represents a person stored in the entity store.
///
/// The primary key is the numeric `person_id`.
#[derive(Clone, Debug, PartialEq)]
struct Person {
    person_id: u32,
    first_name: String,
    last_name: String,
    age: u32,
}

impl Entity for Person {
    type PrimaryKey = u32;

    fn primary_key(&self) -> &u32 {
        &self.person_id
    }

    fn entity_name() -> &'static str {
        "Person"
    }
}

// ---------------------------------------------------------------------------
// Serializer
// ---------------------------------------------------------------------------

/// Manual serializer for Person entities.
///
/// Wire format (big-endian):
///   person_id   : u32 (4 bytes)
///   age         : u32 (4 bytes)
///   first_len   : u32 (4 bytes)
///   first_name  : first_len bytes (UTF-8)
///   last_len    : u32 (4 bytes)
///   last_name   : last_len bytes (UTF-8)
struct PersonSerializer;

impl EntitySerializer<Person> for PersonSerializer {
    fn serialize(&self, entity: &Person) -> noxu::persist::Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&entity.person_id.to_be_bytes());
        buf.extend_from_slice(&entity.age.to_be_bytes());

        let first_bytes = entity.first_name.as_bytes();
        buf.extend_from_slice(&(first_bytes.len() as u32).to_be_bytes());
        buf.extend_from_slice(first_bytes);

        let last_bytes = entity.last_name.as_bytes();
        buf.extend_from_slice(&(last_bytes.len() as u32).to_be_bytes());
        buf.extend_from_slice(last_bytes);

        Ok(buf)
    }

    fn deserialize(&self, bytes: &[u8]) -> noxu::persist::Result<Person> {
        if bytes.len() < 8 {
            return Err(PersistError::SerializationError(
                "Person record too short".to_string(),
            ));
        }

        let person_id =
            u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let age = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);

        let mut pos = 8usize;

        let first_len = read_u32(bytes, pos)? as usize;
        pos += 4;
        let first_name = read_string(bytes, pos, first_len)?;
        pos += first_len;

        let last_len = read_u32(bytes, pos)? as usize;
        pos += 4;
        let last_name = read_string(bytes, pos, last_len)?;

        Ok(Person { person_id, first_name, last_name, age })
    }
}

fn read_u32(bytes: &[u8], pos: usize) -> noxu::persist::Result<u32> {
    if bytes.len() < pos + 4 {
        return Err(PersistError::SerializationError(
            "unexpected end of buffer reading u32".to_string(),
        ));
    }
    Ok(u32::from_be_bytes([
        bytes[pos],
        bytes[pos + 1],
        bytes[pos + 2],
        bytes[pos + 3],
    ]))
}

fn read_string(
    bytes: &[u8],
    pos: usize,
    len: usize,
) -> noxu::persist::Result<String> {
    if bytes.len() < pos + len {
        return Err(PersistError::SerializationError(
            "unexpected end of buffer reading string".to_string(),
        ));
    }
    String::from_utf8(bytes[pos..pos + len].to_vec()).map_err(|e| {
        PersistError::SerializationError(format!("invalid UTF-8: {}", e))
    })
}

// ---------------------------------------------------------------------------
// Example logic
// ---------------------------------------------------------------------------

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env_dir = std::env::temp_dir().join("noxu_persist_example");
    let _ = std::fs::remove_dir_all(&env_dir);
    std::fs::create_dir_all(&env_dir)?;

    println!("Opening environment at {:?}", env_dir);

    // 1. Open the environment.
    let env_config = EnvironmentConfig::new(env_dir.clone())
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(env_config)?;

    // 2. Open an entity store.
    let store_config = StoreConfig::new("PersonStore").with_allow_create(true);
    let mut store = EntityStore::open(&env, store_config)?;

    // 3. Obtain the primary index for Person entities.
    let index: PrimaryIndex<u32, Person> = store.get_primary_index()?;
    let ser = PersonSerializer;

    // 4. Store entities.
    let people = vec![
        Person {
            person_id: 1,
            first_name: "Alice".to_string(),
            last_name: "Smith".to_string(),
            age: 30,
        },
        Person {
            person_id: 2,
            first_name: "Bob".to_string(),
            last_name: "Jones".to_string(),
            age: 25,
        },
        Person {
            person_id: 3,
            first_name: "Carol".to_string(),
            last_name: "Williams".to_string(),
            age: 42,
        },
        Person {
            person_id: 4,
            first_name: "Dave".to_string(),
            last_name: "Brown".to_string(),
            age: 35,
        },
        Person {
            person_id: 5,
            first_name: "Eve".to_string(),
            last_name: "Davis".to_string(),
            age: 28,
        },
    ];

    println!("\nStoring {} persons...", people.len());
    for person in &people {
        index.put(None, &ser, person)?;
        println!(
            "  Stored: id={} {} {}",
            person.person_id, person.first_name, person.last_name
        );
    }

    // 5. Retrieve by primary key.
    println!("\nRetrieving by primary key:");
    for id in [1u32, 3, 5] {
        match index.get(None, &ser, &id)? {
            Some(p) => println!(
                "  id={}: {} {}, age={}",
                id, p.first_name, p.last_name, p.age
            ),
            None => println!("  id={}: NOT FOUND", id),
        }
    }

    // 6. Update an entity.
    println!("\nUpdating id=2 (Bob Jones -> Bob Johnson, age 26)...");
    let updated_bob = Person {
        person_id: 2,
        first_name: "Bob".to_string(),
        last_name: "Johnson".to_string(),
        age: 26,
    };
    index.put(None, &ser, &updated_bob)?;
    let bob = index.get(None, &ser, &2u32)?.expect("Bob should exist");
    println!(
        "  Updated: {} {}, age={}",
        bob.first_name, bob.last_name, bob.age
    );

    // 7. Check entity count.
    println!("\nEntity count: {}", index.count()?);

    // 8. Iterate over all entities in key order.
    println!("\nAll persons in key order:");
    let all: Vec<Person> = index
        .entities(None, &ser)?
        .collect::<noxu::persist::Result<Vec<_>>>()?;
    for p in &all {
        println!(
            "  id={}: {} {}, age={}",
            p.person_id, p.first_name, p.last_name, p.age
        );
    }

    // 9. Delete an entity.
    println!("\nDeleting id=4 (Dave Brown)...");
    let deleted = index.delete(None, &4u32)?;
    println!("  Deleted: {}", deleted);

    match index.get(None, &ser, &4u32)? {
        Some(_) => println!("  ERROR: id=4 still present after delete"),
        None => println!("  Confirmed: id=4 no longer present"),
    }

    println!("\nFinal entity count: {}", index.count()?);

    // 10. Verify all remaining entities are accessible.
    println!("\nFinal scan:");
    let remaining: Vec<Person> = index
        .entities(None, &ser)?
        .collect::<noxu::persist::Result<Vec<_>>>()?;
    for p in &remaining {
        println!(
            "  id={}: {} {}, age={}",
            p.person_id, p.first_name, p.last_name, p.age
        );
    }

    // 11. Close the store (must happen before environment is dropped because
    //     EntityStore holds a borrow of env).
    store.close()?;
    drop(store);

    let _ = std::fs::remove_dir_all(&env_dir);
    println!("\nDone!");
    Ok(())
}
