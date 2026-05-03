//! Collections example for Noxu DB.
//!
//! Demonstrates using noxu-collections StoredMap to provide a familiar
//! map-like interface over a Noxu DB database.

use noxu_collections::StoredMap;
use noxu_db::{DatabaseConfig, Environment, EnvironmentConfig};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env_dir = std::env::temp_dir().join("noxu_collections_example");
    let _ = std::fs::remove_dir_all(&env_dir);

    // --- Setup ---
    let env_config =
        EnvironmentConfig::new(env_dir.clone()).with_allow_create(true);
    let env = Environment::open(env_config)?;
    let db_config = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, "mapDb", &db_config)?;

    // --- Create a StoredMap view ---
    let map = StoredMap::new(&db, false);

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
        let old = map.put(name.as_bytes(), role.as_bytes())?;
        println!(
            "  put({}, {}) -> previous: {:?}",
            name,
            role,
            old.map(|v| String::from_utf8_lossy(&v).into_owned())
        );
    }

    // --- Get individual values ---
    println!("\n=== Looking up records ===");
    for name in ["alice", "charlie", "frank"] {
        let result = map.get(name.as_bytes())?;
        match result {
            Some(v) => {
                let role = String::from_utf8_lossy(&v);
                println!("  {} -> {}", name, role);
            }
            None => {
                println!("  {} -> NOT FOUND", name);
            }
        }
    }

    // --- Check containment ---
    println!("\n=== Contains key checks ===");
    println!(
        "  contains 'bob': {}",
        map.contains_key(b"bob")?
    );
    println!(
        "  contains 'frank': {}",
        map.contains_key(b"frank")?
    );

    // --- Size ---
    println!("\n=== Map size ===");
    println!("  len: {}", map.len()?);
    println!("  is_empty: {}", map.is_empty()?);

    // --- Iterate over all entries ---
    println!("\n=== Iterating all entries (sorted by key) ===");
    for entry in map.iter()? {
        let (key, value) = entry?;
        let k = String::from_utf8_lossy(&key);
        let v = String::from_utf8_lossy(&value);
        println!("  {} -> {}", k, v);
    }

    // --- Iterate over keys only ---
    println!("\n=== Keys only ===");
    for key in map.keys()? {
        let key_bytes = key?;
        let k = String::from_utf8_lossy(&key_bytes);
        println!("  {}", k);
    }

    // --- Iterate over values only ---
    println!("\n=== Values only ===");
    for value in map.values()? {
        let value_bytes = value?;
        let v = String::from_utf8_lossy(&value_bytes);
        println!("  {}", v);
    }

    // --- Update a value ---
    println!("\n=== Updating 'bob' from designer to architect ===");
    let old = map.put(b"bob", b"architect")?;
    println!(
        "  Previous value: {:?}",
        old.map(|v| String::from_utf8_lossy(&v).into_owned())
    );
    let new_val = map.get(b"bob")?;
    println!(
        "  New value: {:?}",
        new_val.map(|v| String::from_utf8_lossy(&v).into_owned())
    );

    // --- Remove a record ---
    println!("\n=== Removing 'dave' ===");
    let removed = map.remove(b"dave")?;
    println!(
        "  Removed value: {:?}",
        removed.map(|v| String::from_utf8_lossy(&v).into_owned())
    );
    println!("  len after remove: {}", map.len()?);

    // --- Final iteration ---
    println!("\n=== Final state ===");
    for entry in map.iter()? {
        let (key, value) = entry?;
        let k = String::from_utf8_lossy(&key);
        let v = String::from_utf8_lossy(&value);
        println!("  {} -> {}", k, v);
    }

    // --- Clear all ---
    println!("\n=== Clearing all records ===");
    map.clear()?;
    println!("  len after clear: {}", map.len()?);
    println!("  is_empty: {}", map.is_empty()?);

    // --- Cleanup ---
    drop(map);
    drop(db);
    drop(env);
    let _ = std::fs::remove_dir_all(&env_dir);

    println!("\nDone!");
    Ok(())
}
