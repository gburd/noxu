use noxu::persist::{Entity, SecondaryKey};

#[derive(Clone, Entity, SecondaryKey)]
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
}

fn main() {
    let specs = User::SECONDARY_INDEXES;
    assert_eq!(specs.len(), 2);
}
