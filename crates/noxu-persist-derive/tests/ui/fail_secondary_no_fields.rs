use noxu_persist::{Entity, SecondaryKey};

#[derive(Entity, SecondaryKey)]
struct NoSecondary {
    #[primary_key]
    id: u64,
    name: String,
}

fn main() {}
