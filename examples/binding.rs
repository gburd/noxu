//! Binding example for Noxu DB.
//!
//! Example showing Noxu DB usage.java`.
//!
//! Demonstrates using noxu-bind tuple bindings to store typed data
//! (integers, strings, floats) as sortable keys and read them back
//! with proper deserialization.

use noxu::bind::{
    EntryBinding, IntBinding, LongBinding, SortedDoubleBinding, StringBinding,
};
use noxu::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, Get,
    OperationStatus,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env_dir = std::env::temp_dir().join("noxu_binding_example");
    let _ = std::fs::remove_dir_all(&env_dir);

    // --- Setup ---
    let env_config =
        EnvironmentConfig::new(env_dir.clone()).with_allow_create(true);
    let env = Environment::open(env_config)?;
    let db_config = DatabaseConfig::new().with_allow_create(true);

    // =========================================================================
    // Integer keys
    // =========================================================================
    println!("=== Integer Binding ===");
    {
        let db = env.open_database(None, "intDb", &db_config)?;
        let binding = IntBinding::new();

        // Store integers as sorted keys
        let values: Vec<i32> =
            vec![42, -7, 0, 1000, -999, 1, i32::MAX, i32::MIN];
        for &val in &values {
            let mut key_entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut key_entry)?;

            let data_entry =
                DatabaseEntry::from_bytes(format!("int:{}", val).as_bytes());
            db.put(&key_entry, &data_entry)?;
        }

        // Iterate  -  keys should come out in sorted order because IntBinding
        // produces sortable byte encodings.
        println!("  Records in sorted key order:");
        let mut cursor = db.open_cursor(None)?;
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let mut status = cursor.get(&mut key, &mut data, Get::First, None)?;
        while status == OperationStatus::Success {
            let v = std::str::from_utf8(data.data()).unwrap_or("?");
            println!("    {}", v);
            status = cursor.get(&mut key, &mut data, Get::Next, None)?;
        }
        cursor.close()?;

        // Look up a specific integer key
        let search_val: i32 = 42;
        let mut search_key = DatabaseEntry::new();
        binding.object_to_entry(&search_val, &mut search_key)?;

        let mut result_data = DatabaseEntry::new();
        if db.get_into(None, &search_key, &mut result_data)? {
            let v = std::str::from_utf8(result_data.data()).unwrap_or("?");
            println!("  Lookup key=42: {}", v);
        }

        db.close()?;
    }

    // =========================================================================
    // String keys
    // =========================================================================
    println!("\n=== String Binding ===");
    {
        let db = env.open_database(None, "strDb", &db_config)?;
        let binding = StringBinding::new();

        let names = ["Charlie", "Alice", "Eve", "Bob", "Dave"];
        for (i, name) in names.iter().enumerate() {
            let mut key_entry = DatabaseEntry::new();
            binding.object_to_entry(&name.to_string(), &mut key_entry)?;

            let data_entry =
                DatabaseEntry::from_bytes(format!("id:{}", i).as_bytes());
            db.put(&key_entry, &data_entry)?;
        }

        // Iterate in sorted order
        println!("  Records in sorted key order:");
        let mut cursor = db.open_cursor(None)?;
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let mut status = cursor.get(&mut key, &mut data, Get::First, None)?;
        while status == OperationStatus::Success {
            let v = std::str::from_utf8(data.data()).unwrap_or("?");
            println!("    {}", v);
            status = cursor.get(&mut key, &mut data, Get::Next, None)?;
        }
        cursor.close()?;

        // Look up a specific string key
        let search_name = "Bob".to_string();
        let mut search_key = DatabaseEntry::new();
        binding.object_to_entry(&search_name, &mut search_key)?;

        let mut result_data = DatabaseEntry::new();
        if db.get_into(None, &search_key, &mut result_data)? {
            let v = std::str::from_utf8(result_data.data()).unwrap_or("?");
            println!("  Lookup key='Bob': {}", v);
        }

        db.close()?;
    }

    // =========================================================================
    // Sorted double keys
    // =========================================================================
    println!("\n=== Sorted Double Binding ===");
    {
        let db = env.open_database(None, "dblDb", &db_config)?;
        let binding = SortedDoubleBinding::new();

        let temps = [98.6, -40.0, 0.0, 100.0, 37.0, -273.15, 212.0];
        for &temp in &temps {
            let mut key_entry = DatabaseEntry::new();
            binding.object_to_entry(&temp, &mut key_entry)?;

            let data_entry =
                DatabaseEntry::from_bytes(format!("{:.2}F", temp).as_bytes());
            db.put(&key_entry, &data_entry)?;
        }

        // Iterate in sorted order
        println!("  Temperatures in sorted order:");
        let mut cursor = db.open_cursor(None)?;
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let mut status = cursor.get(&mut key, &mut data, Get::First, None)?;
        while status == OperationStatus::Success {
            let v = std::str::from_utf8(data.data()).unwrap_or("?");
            println!("    {}", v);
            status = cursor.get(&mut key, &mut data, Get::Next, None)?;
        }
        cursor.close()?;
        db.close()?;
    }

    // =========================================================================
    // Long binding with round-trip verification
    // =========================================================================
    println!("\n=== Long Binding (round-trip) ===");
    {
        let db = env.open_database(None, "longDb", &db_config)?;
        let binding = LongBinding::new();

        let values: Vec<i64> = vec![i64::MIN, -1, 0, 1, i64::MAX];
        for &val in &values {
            let mut key_entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut key_entry)?;

            // Store the value in the data as well (as bytes) for verification
            let mut data_entry = DatabaseEntry::new();
            binding.object_to_entry(&val, &mut data_entry)?;

            db.put(&key_entry, &data_entry)?;
        }

        // Read back and deserialize
        println!("  Round-trip verification:");
        let mut cursor = db.open_cursor(None)?;
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let mut status = cursor.get(&mut key, &mut data, Get::First, None)?;
        while status == OperationStatus::Success {
            let deserialized = binding.entry_to_object(&data)?;
            println!("    value = {}", deserialized);
            status = cursor.get(&mut key, &mut data, Get::Next, None)?;
        }
        cursor.close()?;
        db.close()?;
    }

    // --- Cleanup ---
    drop(env);
    let _ = std::fs::remove_dir_all(&env_dir);

    println!("\nDone!");
    Ok(())
}
