//! Cursor scan example for Noxu DB.
//!
//! Demonstrates cursor operations: forward scan, reverse scan, search
//! positioning, and cursor-based deletion.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, Get,
    OperationStatus,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env_dir = std::env::temp_dir().join("noxu_cursor_example");
    let _ = std::fs::remove_dir_all(&env_dir);

    // --- Setup ---
    let env_config = EnvironmentConfig::new(env_dir.clone())
        .with_allow_create(true);
    let env = Environment::open(env_config)?;

    let db_config = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, "cursorDb", &db_config)?;

    // --- Insert sample data ---
    let fruits = [
        ("apple", "red"),
        ("banana", "yellow"),
        ("cherry", "red"),
        ("date", "brown"),
        ("elderberry", "purple"),
        ("fig", "green"),
        ("grape", "purple"),
    ];

    println!("Inserting {} records...", fruits.len());
    for (name, color) in &fruits {
        let key = DatabaseEntry::from_bytes(name.as_bytes());
        let data = DatabaseEntry::from_bytes(color.as_bytes());
        db.put(None, &key, &data)?;
    }

    // --- Forward scan ---
    println!("\n=== Forward Scan (First -> Next) ===");
    {
        let mut cursor = db.open_cursor(None, None)?;
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let mut status = cursor.get(&mut key, &mut data, Get::First, None)?;
        while status == OperationStatus::Success {
            let v = std::str::from_utf8(data.data()).unwrap_or("?");
            println!("  color: {}", v);
            status = cursor.get(&mut key, &mut data, Get::Next, None)?;
        }
        cursor.close()?;
    }

    // --- Reverse scan ---
    println!("\n=== Reverse Scan (Last -> Prev) ===");
    {
        let mut cursor = db.open_cursor(None, None)?;
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let mut status = cursor.get(&mut key, &mut data, Get::Last, None)?;
        while status == OperationStatus::Success {
            let v = std::str::from_utf8(data.data()).unwrap_or("?");
            println!("  color: {}", v);
            status = cursor.get(&mut key, &mut data, Get::Prev, None)?;
        }
        cursor.close()?;
    }

    // --- Search and iterate forward from a position ---
    println!("\n=== Search for 'cherry' then iterate forward ===");
    {
        let mut cursor = db.open_cursor(None, None)?;
        let mut search_key = DatabaseEntry::from_bytes(b"cherry");
        let mut data = DatabaseEntry::new();

        let status = cursor.get(&mut search_key, &mut data, Get::Search, None)?;
        if status == OperationStatus::Success {
            let v = std::str::from_utf8(data.data()).unwrap_or("?");
            println!("  Found cherry: {}", v);

            // Continue iterating from current position
            let mut key = DatabaseEntry::new();
            let mut status = cursor.get(&mut key, &mut data, Get::Next, None)?;
            while status == OperationStatus::Success {
                let v = std::str::from_utf8(data.data()).unwrap_or("?");
                println!("  Next: {}", v);
                status = cursor.get(&mut key, &mut data, Get::Next, None)?;
            }
        } else {
            println!("  'cherry' not found!");
        }
        cursor.close()?;
    }

    // --- Cursor-based deletion ---
    println!("\n=== Cursor delete: remove 'banana' ===");
    {
        let mut cursor = db.open_cursor(None, None)?;
        let mut search_key = DatabaseEntry::from_bytes(b"banana");
        let mut data = DatabaseEntry::new();

        let status = cursor.get(&mut search_key, &mut data, Get::Search, None)?;
        if status == OperationStatus::Success {
            println!("  Found banana, deleting...");
            let del_status = cursor.delete()?;
            println!("  Delete status: {:?}", del_status);
        }
        cursor.close()?;
    }

    // --- Verify deletion with another scan ---
    println!("\n=== After deletion (banana should be gone) ===");
    {
        let mut cursor = db.open_cursor(None, None)?;
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let mut status = cursor.get(&mut key, &mut data, Get::First, None)?;
        while status == OperationStatus::Success {
            let v = std::str::from_utf8(data.data()).unwrap_or("?");
            println!("  {}", v);
            status = cursor.get(&mut key, &mut data, Get::Next, None)?;
        }
        cursor.close()?;
    }

    println!("\nFinal record count: {}", db.count()?);

    // --- Cleanup ---
    drop(db);
    drop(env);
    let _ = std::fs::remove_dir_all(&env_dir);

    println!("Done!");
    Ok(())
}
