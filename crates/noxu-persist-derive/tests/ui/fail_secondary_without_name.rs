use noxu::persist::{Entity, SecondaryKey};

#[derive(Entity, SecondaryKey)]
struct NoName {
    #[primary_key]
    id: u64,
    #[secondary_key(relate = OneToOne)]
    field: String,
}

fn main() {}
