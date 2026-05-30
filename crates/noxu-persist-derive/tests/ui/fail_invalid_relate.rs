use noxu::persist::{Entity, SecondaryKey};

#[derive(Entity, SecondaryKey)]
struct BadRelate {
    #[primary_key]
    id: u64,
    #[secondary_key(name = "x", relate = NotARealVariant)]
    field: String,
}

fn main() {}
