//! Collections example for Noxu DB (Wave 2B / v1.6 typed API).
//!
//! Demonstrates using noxu-collections `StoredMap<K, V, KB, VB>` to
//! provide a familiar map-like interface over a Noxu DB database
//! with typed keys and values.

use noxu::bind::StringBinding;
use noxu::collections::StoredMap;
use noxu::{DatabaseConfig, Environment, EnvironmentConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env_dir = std::env::temp_dir().join("noxu_collections_example");
    let _ = std::fs::remove_dir_all(&env_dir);

    // --- Setup ---
    let env_config =
        EnvironmentConfig::new(env_dir.clone()).with_allow_create(true);
    let env = Environment::open(env_config)?;
    let db_config = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, "mapDb", &db_config)?;

    // --- Create a typed StoredMap<String, String> view ---
    // String keys, String values, encoded via the StringBinding tuple
    // binding (length-prefixed UTF-8).
    let map: StoredMap<'_, String, String, _, _> =
        StoredMap::new(&db, StringBinding, StringBinding);

    // --- Put key-value pairs ---
    println!("=== Inserting records via StoredMap ===");
    let items = [
        ("alice", "engineer"),
        ("bob", "designer"),
        ("charlie", "manager"),
        ("dave", "analyst"),
        ("eve", "researcher"),
    ];

    for (name, role) in &items {
        let old = map.put(None, &name.to_string(), &role.to_string())?;
        println!("  put({}, {}) -> previous: {:?}", name, role, old);
    }

    // --- Get individual values ---
    println!("\n=== Looking up records ===");
    for name in ["alice", "charlie", "frank"] {
        let result = map.get(None, &name.to_string())?;
        match result {
            Some(role) => println!("  {} -> {}", name, role),
            None => println!("  {} -> NOT FOUND", name),
        }
    }

    // --- Check containment ---
    println!("\n=== Contains key checks ===");
    println!(
        "  contains 'bob': {}",
        map.contains_key(None, &"bob".to_string())?,
    );
    println!(
        "  contains 'frank': {}",
        map.contains_key(None, &"frank".to_string())?,
    );

    // --- Size ---
    println!("\n=== Map size ===");
    println!("  len: {}", map.len(None)?);
    println!("  is_empty: {}", map.is_empty(None)?);

    // --- Iterate over all entries ---
    println!("\n=== Iterating all entries (sorted by key) ===");
    for entry in map.iter(None)? {
        let (key, value) = entry?;
        println!("  {} -> {}", key, value);
    }

    // --- Iterate over keys only ---
    println!("\n=== Keys only ===");
    for key in map.keys(None)? {
        println!("  {}", key?);
    }

    // --- Iterate over values only ---
    println!("\n=== Values only ===");
    for value in map.values(None)? {
        println!("  {}", value?);
    }

    // --- Update a value ---
    println!("\n=== Updating 'bob' from designer to architect ===");
    let old = map.put(None, &"bob".to_string(), &"architect".to_string())?;
    println!("  Previous value: {:?}", old);
    let new_val = map.get(None, &"bob".to_string())?;
    println!("  New value: {:?}", new_val);

    // --- Remove a record ---
    println!("\n=== Removing 'dave' ===");
    let removed = map.remove(None, &"dave".to_string())?;
    println!("  Removed value: {:?}", removed);
    println!("  len after remove: {}", map.len(None)?);

    // --- Use a transaction across several writes ---
    println!("\n=== Transactional batch update ===");
    let txn = env.begin_transaction(None)?;
    map.put(Some(&txn), &"frank".to_string(), &"intern".to_string())?;
    map.put(Some(&txn), &"grace".to_string(), &"director".to_string())?;
    txn.commit()?;
    println!(
        "  After txn commit: frank={:?}, grace={:?}",
        map.get(None, &"frank".to_string())?,
        map.get(None, &"grace".to_string())?,
    );

    // --- Final iteration ---
    println!("\n=== Final state ===");
    for entry in map.iter(None)? {
        let (key, value) = entry?;
        println!("  {} -> {}", key, value);
    }

    // --- Clear all ---
    println!("\n=== Clearing all records ===");
    map.clear(None)?;
    println!("  len after clear: {}", map.len(None)?);
    println!("  is_empty: {}", map.is_empty(None)?);

    // --- Cleanup ---
    let _ = std::fs::remove_dir_all(&env_dir);

    println!("\nDone!");
    Ok(())
}
