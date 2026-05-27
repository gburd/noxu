use noxu_persist::Entity;

#[derive(Entity)]
struct TwoKeys {
    #[primary_key]
    id: u64,
    #[primary_key]
    other: String,
}

fn main() {}
