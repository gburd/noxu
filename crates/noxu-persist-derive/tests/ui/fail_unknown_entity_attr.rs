use noxu_persist::Entity;

#[derive(Entity)]
#[entity(version = "wat")]
struct BadEntityAttr {
    #[primary_key]
    id: u64,
}

fn main() {}
