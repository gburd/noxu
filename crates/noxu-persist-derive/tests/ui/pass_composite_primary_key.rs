use noxu_persist::PrimaryKey;

#[derive(Clone, Debug, PartialEq, Eq, Hash, PrimaryKey)]
struct CompositeKey {
    region: String,
    customer_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PrimaryKey)]
struct NewtypeKey(u64);

fn main() {
    let key = CompositeKey { region: "x".into(), customer_id: 7 };
    let bytes = key.to_bytes();
    let decoded = CompositeKey::from_bytes(&bytes).unwrap();
    assert_eq!(key, decoded);

    let nk = NewtypeKey(42);
    assert_eq!(NewtypeKey::from_bytes(&nk.to_bytes()).unwrap(), nk);
}
