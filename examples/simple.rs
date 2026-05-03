//! Simple example for Noxu DB.
//!
//! Port of Berkeley DB Java Edition's `SimpleExample.java`.
//!
//! Demonstrates basic database operations: opening an environment and database,
//! inserting key-value pairs, retrieving them, deleting one, and iterating
//! with a cursor.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, Get,
    OperationStatus,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary directory for the environment.
    let env_dir = std::env::temp_dir().join("noxu_simple_example");
    let _ = std::fs::remove_dir_all(&env_dir);

    // --- Open environment ---
    println!("Opening environment at {:?}", env_dir);
    let env_config = EnvironmentConfig::new(env_dir.clone())
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(env_config)?;

    // --- Open database ---
    let db_config = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, "simpleDb", &db_config)?;

    // --- Insert records ---
    let num_records = 10;
    println!("Inserting {} records...", num_records);
    for i in 0..num_records {
        let key_str = format!("key{:03}", i);
        let data_str = format!("data{}", i * i);

        let key = DatabaseEntry::from_bytes(key_str.as_bytes());
        let data = DatabaseEntry::from_bytes(data_str.as_bytes());

        let status = db.put(None, &key, &data)?;
        assert_eq!(status, OperationStatus::Success);
    }

    // --- Retrieve individual records ---
    println!("\nRetrieving individual records:");
    for i in [0, 3, 7, 9] {
        let key_str = format!("key{:03}", i);
        let key = DatabaseEntry::from_bytes(key_str.as_bytes());
        let mut data = DatabaseEntry::new();

        let status = db.get(None, &key, &mut data)?;
        match status {
            OperationStatus::Success => {
                let value = std::str::from_utf8(data.data()).unwrap_or("<binary>");
                println!("  {} -> {}", key_str, value);
            }
            OperationStatus::NotFound => {
                println!("  {} -> NOT FOUND", key_str);
            }
            _ => {}
        }
    }

    // --- Delete a record ---
    println!("\nDeleting key003...");
    let del_key = DatabaseEntry::from_bytes(b"key003");
    let status = db.delete(None, &del_key)?;
    println!("  Delete status: {:?}", status);

    // Verify deletion
    let mut data = DatabaseEntry::new();
    let status = db.get(None, &del_key, &mut data)?;
    println!("  Get after delete: {:?}", status);

    // --- Cursor scan ---
    println!("\nForward cursor scan (all records):");
    let mut cursor = db.open_cursor(None, None)?;
    let mut key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let mut count = 0;

    let mut status = cursor.get(&mut key, &mut data, Get::First, None)?;
    while status == OperationStatus::Success {
        let k = std::str::from_utf8(data.data()).unwrap_or("<binary>");
        println!("  record {}: value={}", count, k);
        count += 1;
        status = cursor.get(&mut key, &mut data, Get::Next, None)?;
    }
    println!("  Total records after deletion: {}", count);
    cursor.close()?;

    // --- Record count ---
    println!("\nDatabase record count: {}", db.count()?);

    // --- Cleanup ---
    // Drop the database and environment handles. The Drop implementations
    // perform best-effort cleanup.
    drop(db);
    drop(env);
    let _ = std::fs::remove_dir_all(&env_dir);

    println!("\nDone!");
    Ok(())
}
