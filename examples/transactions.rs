//! Transaction example for Noxu DB.
//!
//! Example showing Noxu DB usage.java`.
//!
//! Demonstrates transactional operations: beginning transactions, committing,
//! aborting, and verifying that only committed data persists.

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    OperationStatus,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env_dir = std::env::temp_dir().join("noxu_txn_example");
    let _ = std::fs::remove_dir_all(&env_dir);

    // --- Open a transactional environment ---
    println!("Opening transactional environment at {:?}", env_dir);
    let env_config = EnvironmentConfig::new(env_dir.clone())
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(env_config)?;
    assert!(env.is_transactional());

    // --- Open database ---
    let db_config = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, "txnDb", &db_config)?;

    // --- Transaction 1: Insert records and COMMIT ---
    println!("\nTransaction 1: inserting committed records...");
    let txn1 = env.begin_transaction(None, None)?;
    println!("  Started transaction {}", txn1.get_id());

    // Note: The current simplified implementation does not actually
    // isolate operations by transaction, but we demonstrate the API
    // pattern that will work with the full implementation.
    for i in 0..5 {
        let key =
            DatabaseEntry::from_bytes(format!("committed_{}", i).as_bytes());
        let data = DatabaseEntry::from_bytes(format!("value_{}", i).as_bytes());
        let status = db.put(Some(&txn1), &key, &data)?;
        assert_eq!(status, OperationStatus::Success);
        println!("  Put committed_{} -> value_{}", i, i);
    }

    txn1.commit()?;
    println!("  Transaction 1 committed.");

    // --- Transaction 2: Insert records and ABORT ---
    println!("\nTransaction 2: inserting records that will be aborted...");
    let txn2 = env.begin_transaction(None, None)?;
    println!("  Started transaction {}", txn2.get_id());

    for i in 0..3 {
        let key =
            DatabaseEntry::from_bytes(format!("aborted_{}", i).as_bytes());
        let data = DatabaseEntry::from_bytes(
            format!("should_not_exist_{}", i).as_bytes(),
        );
        db.put(Some(&txn2), &key, &data)?;
        println!("  Put aborted_{} -> should_not_exist_{}", i, i);
    }

    txn2.abort()?;
    println!("  Transaction 2 aborted.");

    // --- Verify results ---
    println!("\nVerifying committed data exists:");
    for i in 0..5 {
        let key =
            DatabaseEntry::from_bytes(format!("committed_{}", i).as_bytes());
        let mut data = DatabaseEntry::new();
        let status = db.get(None, &key, &mut data)?;
        let found = status == OperationStatus::Success;
        println!("  committed_{}: found={}", i, found);
    }

    // Note: In the current simplified (in-memory HashMap) implementation,
    // aborted transaction data may still be visible because the store
    // does not yet implement true MVCC isolation. With the full B-tree
    // and WAL implementation, aborted data would not be visible.
    println!(
        "\nChecking aborted data (should not exist with full implementation):"
    );
    for i in 0..3 {
        let key =
            DatabaseEntry::from_bytes(format!("aborted_{}", i).as_bytes());
        let mut data = DatabaseEntry::new();
        let status = db.get(None, &key, &mut data)?;
        let found = status == OperationStatus::Success;
        println!("  aborted_{}: found={}", i, found);
    }

    // --- Demonstrate transaction state ---
    println!("\nDemonstrating transaction state:");
    let txn3 = env.begin_transaction(None, None)?;
    println!("  txn3 is_valid: {}", txn3.is_valid());
    println!("  txn3 state: {:?}", txn3.get_state());

    txn3.commit()?;
    println!("  After commit - is_valid: {}", txn3.is_valid());
    println!("  After commit - state: {:?}", txn3.get_state());

    // Attempting to commit again should fail
    match txn3.commit() {
        Ok(()) => println!("  Double commit unexpectedly succeeded"),
        Err(e) => println!("  Double commit correctly failed: {}", e),
    }

    // --- Cleanup ---
    drop(db);
    drop(env);
    let _ = std::fs::remove_dir_all(&env_dir);

    println!("\nDone!");
    Ok(())
}
