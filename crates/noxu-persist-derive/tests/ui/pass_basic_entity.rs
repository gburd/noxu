use noxu_persist::Entity;

#[derive(Clone, Entity)]
struct User {
    #[primary_key]
    id: u64,
    name: String,
}

fn main() {
    assert_eq!(<User as Entity>::entity_name(), "User");
    let _u = User { id: 1, name: "x".into() };
}
