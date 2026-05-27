use noxu_persist::{Entity, SecondaryKey};

#[derive(Entity, SecondaryKey)]
struct BadDelete {
    #[primary_key]
    id: u64,
    #[secondary_key(name = "x", relate = OneToOne, on_related_entity_delete = Burninate)]
    field: String,
}

fn main() {}
