// Trybuild pass fixture: demonstrate #[entity(crate = "noxu_persist")] for
// users who depend on `noxu-persist` directly (no `noxu` umbrella).
//
// The generated code emits `::noxu_persist::…` paths because of the
// `#[entity(crate = "noxu_persist")]` override on each struct.
use noxu_persist::{Entity, PrimaryKey, SecondaryKey};

// ---- Newtype primary key with crate override ----

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, PrimaryKey)]
#[entity(crate = "noxu_persist")]
struct UserId(u64);

// ---- Entity + SecondaryKey with crate override ----

#[derive(Clone, Debug, Entity, SecondaryKey)]
#[entity(crate = "noxu_persist", name = "StandaloneUser")]
struct User {
    #[primary_key]
    id: UserId,
    #[secondary_key(name = "by_email", relate = OneToOne)]
    email: String,
    #[secondary_key(name = "by_dept", relate = ManyToOne)]
    dept: Option<u64>,
}

fn main() {
    // Entity name override works.
    assert_eq!(<User as Entity>::entity_name(), "StandaloneUser");

    // PrimaryKey round-trip.
    let k = UserId(42);
    let bytes = k.to_bytes();
    let decoded = UserId::from_bytes(&bytes).unwrap();
    assert_eq!(k, decoded);

    // SECONDARY_INDEXES metadata is generated correctly.
    let specs = User::SECONDARY_INDEXES;
    assert_eq!(specs.len(), 2);
    assert!(specs.iter().any(|s| s.name == "by_email"));
    assert!(specs.iter().any(|s| s.name == "by_dept"));
}
