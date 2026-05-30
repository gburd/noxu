//! Persist example using the `#[derive(Entity)]` / `#[derive(SecondaryKey)]`
//! proc-macros (Wave 2C-1, v1.6).
//!
//! Demonstrates the derive-macro shape of the noxu-persist API:
//!
//!   - Define a `Person` entity with `#[derive(Entity, SecondaryKey)]`,
//!     marking the primary-key field with `#[primary_key]` and the
//!     secondary-keyed fields with `#[secondary_key(name = "...",
//!     relate = ..., …)]`.
//!   - Open an `EntityStore` and a `PrimaryIndex` exactly as before.
//!   - Open the auto-generated `Person::open_by_<name>_index(...)`
//!     helpers to obtain `SecondaryIndex` handles without writing the
//!     extractor closures by hand.
//!   - Exercise put / get / update / delete plus a secondary-key query.
//!
//! For the manual `impl Entity for User { … }` shape see `persist.rs`
//! in the same directory.

use noxu::persist::{
    Entity, EntitySerializer, EntityStore, PersistError, PrimaryIndex,
    SecondaryKey, StoreConfig,
};
use noxu::{Environment, EnvironmentConfig};

// ---------------------------------------------------------------------------
// Entity declared with derive macros
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Entity, SecondaryKey)]
struct Person {
    /// Primary key — auto-detected by `#[derive(Entity)]`.
    #[primary_key]
    person_id: u32,

    /// Unique secondary key on email.  `OneToOne` because each person
    /// must have a distinct email address.
    #[secondary_key(name = "by_email", relate = OneToOne)]
    email: String,

    /// Many-to-one secondary key on department: many people share one
    /// `department_id`.  Demonstrates the `Option<T>` form: the
    /// derive unwraps `Option<u32>` so the secondary key type `SK` is
    /// `u32` (entities with `dept_id == None` are excluded).
    #[secondary_key(
        name = "by_dept",
        relate = ManyToOne,
        related_entity = "Department",
        on_related_entity_delete = NULLIFY
    )]
    dept_id: Option<u32>,

    first_name: String,
    last_name: String,
}

// ---------------------------------------------------------------------------
// Serializer (still hand-written; serialization is orthogonal to the
// derive macros).
// ---------------------------------------------------------------------------

struct PersonSerializer;

impl EntitySerializer<Person> for PersonSerializer {
    fn serialize(&self, p: &Person) -> noxu::persist::Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&p.person_id.to_be_bytes());
        write_str(&mut buf, &p.email);
        match p.dept_id {
            None => buf.push(0),
            Some(d) => {
                buf.push(1);
                buf.extend_from_slice(&d.to_be_bytes());
            }
        }
        write_str(&mut buf, &p.first_name);
        write_str(&mut buf, &p.last_name);
        Ok(buf)
    }

    fn deserialize(&self, bytes: &[u8]) -> noxu::persist::Result<Person> {
        if bytes.len() < 4 {
            return Err(PersistError::SerializationError("short".into()));
        }
        let person_id =
            u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        let mut pos = 4usize;
        let email = read_str(bytes, &mut pos)?;
        let dept_id = if bytes[pos] == 0 {
            pos += 1;
            None
        } else {
            pos += 1;
            let d = u32::from_be_bytes([
                bytes[pos],
                bytes[pos + 1],
                bytes[pos + 2],
                bytes[pos + 3],
            ]);
            pos += 4;
            Some(d)
        };
        let first_name = read_str(bytes, &mut pos)?;
        let last_name = read_str(bytes, &mut pos)?;
        Ok(Person { person_id, email, dept_id, first_name, last_name })
    }
}

fn write_str(buf: &mut Vec<u8>, s: &str) {
    let b = s.as_bytes();
    buf.extend_from_slice(&(b.len() as u32).to_be_bytes());
    buf.extend_from_slice(b);
}

fn read_str(bytes: &[u8], pos: &mut usize) -> noxu::persist::Result<String> {
    if bytes.len() < *pos + 4 {
        return Err(PersistError::SerializationError("short str".into()));
    }
    let n = u32::from_be_bytes([
        bytes[*pos],
        bytes[*pos + 1],
        bytes[*pos + 2],
        bytes[*pos + 3],
    ]) as usize;
    *pos += 4;
    if bytes.len() < *pos + n {
        return Err(PersistError::SerializationError("short str body".into()));
    }
    let s = String::from_utf8(bytes[*pos..*pos + n].to_vec())
        .map_err(|e| PersistError::SerializationError(e.to_string()))?;
    *pos += n;
    Ok(s)
}

// ---------------------------------------------------------------------------
// Demo
// ---------------------------------------------------------------------------

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env_dir = std::env::temp_dir().join("noxu_persist_derive_example");
    let _ = std::fs::remove_dir_all(&env_dir);
    std::fs::create_dir_all(&env_dir)?;

    println!("Opening environment at {:?}", env_dir);
    let env = Environment::open(
        EnvironmentConfig::new(env_dir.clone()).with_allow_create(true),
    )?;

    let store_config = StoreConfig::new("PeopleStore").with_allow_create(true);
    let mut store = EntityStore::open(&env, store_config)?;

    // 1. Open the typed primary index.  `Person::entity_name()` defaults
    //    to the struct name "Person" since no `#[entity(name = "...")]`
    //    override was supplied.
    let mut primary: PrimaryIndex<u32, Person> = store.get_primary_index()?;
    let ser = PersonSerializer;

    // 2. Open the secondary indexes via the auto-generated helpers.
    //    Without the derive macro, the user would have written
    //    `primary.open_secondary_index(|p: &Person| Some(p.email.clone()))`
    //    by hand for each index.
    let by_email = Person::open_by_email_index(&mut primary);
    let by_dept = Person::open_by_dept_index(&mut primary);

    println!("\nDeclared secondary indexes: {:?}", Person::SECONDARY_INDEXES);

    // 3. Insert a few people.
    let people = [
        Person {
            person_id: 1,
            email: "alice@example.com".into(),
            dept_id: Some(10),
            first_name: "Alice".into(),
            last_name: "Smith".into(),
        },
        Person {
            person_id: 2,
            email: "bob@example.com".into(),
            dept_id: Some(10),
            first_name: "Bob".into(),
            last_name: "Jones".into(),
        },
        Person {
            person_id: 3,
            email: "carol@example.com".into(),
            dept_id: Some(20),
            first_name: "Carol".into(),
            last_name: "Williams".into(),
        },
        Person {
            person_id: 4,
            email: "dave@example.com".into(),
            dept_id: None,
            first_name: "Dave".into(),
            last_name: "Brown".into(),
        },
    ];

    println!("\nStoring {} people...", people.len());
    for p in &people {
        primary.put(None, &ser, p)?;
    }

    // 4. Look up by primary key.
    let found = primary.get(None, &ser, &2u32)?.expect("Bob exists");
    println!(
        "\nLookup by id=2 → {} {} <{}>",
        found.first_name, found.last_name, found.email
    );

    // 5. Look up by secondary key (email).
    let alice = by_email
        .get(None, &ser, &primary, &"alice@example.com".to_string())?
        .expect("Alice exists by email");
    println!(
        "Lookup by email=alice@... → id={} {} {}",
        alice.person_id, alice.first_name, alice.last_name
    );

    // 6. Range scan by dept (ManyToOne).
    println!("\nPeople in dept 10:");
    let dept10 = by_dept.sub_index(&10u32);
    for pk in &dept10 {
        let p = primary.get(None, &ser, pk)?.unwrap();
        println!("  id={} {}", p.person_id, p.first_name);
    }

    // 7. Update Bob's email — by_email index is auto-maintained.
    let mut bob = primary.get(None, &ser, &2u32)?.unwrap();
    bob.email = "bob@new.example.com".into();
    primary.put(None, &ser, &bob)?;
    assert!(
        by_email
            .get(None, &ser, &primary, &"bob@example.com".into())?
            .is_none()
    );
    assert!(
        by_email
            .get(None, &ser, &primary, &"bob@new.example.com".into())?
            .is_some()
    );
    println!("\nUpdated Bob's email; old key no longer in by_email index.");

    // 8. Delete via secondary index — cascades to the primary record.
    let removed =
        by_email.delete(None, &ser, &primary, &"alice@example.com".into())?;
    println!(
        "\nDelete by email=alice@example.com → removed = {removed}; \
         primary count = {}",
        primary.count()?
    );

    // 9. Final scan in primary-key order.
    println!("\nFinal primary scan:");
    let all: Vec<Person> =
        primary.entities(None, &ser)?.collect::<noxu::persist::Result<_>>()?;
    for p in &all {
        println!(
            "  id={} {} {} <{}> dept={:?}",
            p.person_id, p.first_name, p.last_name, p.email, p.dept_id
        );
    }

    store.close()?;
    drop(store);
    let _ = std::fs::remove_dir_all(&env_dir);
    println!("\nDone!");
    Ok(())
}
