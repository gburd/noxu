use noxu_persist::Entity;

#[derive(Entity)]
struct NoPrimaryKey {
    id: u64,
    name: String,
}

fn main() {}
