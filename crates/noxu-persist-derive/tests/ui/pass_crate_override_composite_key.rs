// Trybuild pass fixture: composite PrimaryKey with #[entity(crate = "noxu_persist")].
use noxu_persist::PrimaryKey;

#[derive(Clone, Debug, PartialEq, Eq, Hash, PrimaryKey)]
#[entity(crate = "noxu_persist")]
struct OrderKey {
    region: String,
    customer_id: u64,
}

fn main() {
    let key = OrderKey { region: "us-east-1".into(), customer_id: 99 };
    let bytes = key.to_bytes();
    let decoded = OrderKey::from_bytes(&bytes).unwrap();
    assert_eq!(key, decoded);
}
