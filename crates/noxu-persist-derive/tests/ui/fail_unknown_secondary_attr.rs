use noxu::persist::{Entity, SecondaryKey};

#[derive(Entity, SecondaryKey)]
struct UnknownAttr {
    #[primary_key]
    id: u64,
    #[secondary_key(name = "x", relate = OneToOne, bogus_key = "wat")]
    field: String,
}

fn main() {}
