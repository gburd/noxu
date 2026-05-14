//! Transaction Configuration Example
//!
//! Demonstrates TransactionConfig options: lock timeouts, serializable
//! isolation, no-wait mode, and importunate lock stealing.
//!
//! Run with: cargo run --example transaction_config

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, TransactionConfig,
};
use tempfile::TempDir;

fn main() {
    println!("=== Transaction Configuration Example ===\n");

    let dir = TempDir::new().unwrap();
    let env = Environment::open(
        EnvironmentConfig::new(dir.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();

    let db = env
        .open_database(None, "config_demo", &DatabaseConfig::new().with_allow_create(true))
        .unwrap();

    // ── 1. Default transaction ────────────────────────────────────────────────
    println!("[1] Default transaction (COMMIT_SYNC durability)");
    {
        let txn = env.begin_transaction(None, None).unwrap();
        let key = DatabaseEntry::from_bytes(b"key_default");
        let val = DatabaseEntry::from_bytes(b"value_default");
        db.put(Some(&txn), &key, &val).unwrap();
        txn.commit().unwrap();
        println!("    Written and committed key_default\n");
    }

    // ── 2. Lock timeout ──────────────────────────────────────────────────────
    println!("[2] Lock timeout (100ms)");
    {
        let config = TransactionConfig::new().with_lock_timeout_ms(100);
        let txn = env.begin_transaction(None, Some(&config)).unwrap();
        let key = DatabaseEntry::from_bytes(b"key_timeout");
        let val = DatabaseEntry::from_bytes(b"value_timeout");
        db.put(Some(&txn), &key, &val).unwrap();
        txn.commit().unwrap();
        println!("    lock_timeout_ms=100 applied successfully\n");
    }

    // ── 3. Serializable isolation ────────────────────────────────────────────
    println!("[3] Serializable isolation");
    {
        let config = TransactionConfig::new().with_serializable_isolation(true);
        let txn = env.begin_transaction(None, Some(&config)).unwrap();
        let key = DatabaseEntry::from_bytes(b"key_serializable");
        let val = DatabaseEntry::from_bytes(b"value_serializable");
        db.put(Some(&txn), &key, &val).unwrap();
        txn.commit().unwrap();
        println!("    Serializable transaction committed\n");
    }

    // ── 4. No-wait mode ──────────────────────────────────────────────────────
    println!("[4] No-wait mode (fail immediately on lock conflict)");
    {
        let config = TransactionConfig::new().with_no_wait(true);
        let txn = env.begin_transaction(None, Some(&config)).unwrap();
        let key = DatabaseEntry::from_bytes(b"key_nowait");
        let val = DatabaseEntry::from_bytes(b"value_nowait");
        db.put(Some(&txn), &key, &val).unwrap();
        txn.commit().unwrap();
        println!("    no_wait=true transaction committed (no conflict)\n");
    }

    // ── 5. Importunate mode ──────────────────────────────────────────────────
    println!("[5] Importunate mode (steal locks)");
    {
        let config = TransactionConfig::new().with_importunate(true);
        let txn = env.begin_transaction(None, Some(&config)).unwrap();
        let key = DatabaseEntry::from_bytes(b"key_importunate");
        let val = DatabaseEntry::from_bytes(b"value_importunate");
        db.put(Some(&txn), &key, &val).unwrap();
        txn.commit().unwrap();
        println!("    importunate=true transaction committed\n");
    }

    // ── 6. Read-committed isolation ──────────────────────────────────────────
    println!("[6] Read-committed isolation");
    {
        let config = TransactionConfig::new().with_read_committed(true);
        let txn = env.begin_transaction(None, Some(&config)).unwrap();
        let key = DatabaseEntry::from_bytes(b"key_default");
        let mut val = DatabaseEntry::new();
        db.get(Some(&txn), &key, &mut val).unwrap();
        println!(
            "    Read: {:?}",
            std::str::from_utf8(val.get_data().unwrap())
        );
        txn.commit().unwrap();
        println!("    read_committed=true transaction committed\n");
    }

    // ── 7. Transaction timeout ───────────────────────────────────────────────
    println!("[7] Transaction timeout (5000ms)");
    {
        let config = TransactionConfig::new().with_txn_timeout_ms(5000);
        let txn = env.begin_transaction(None, Some(&config)).unwrap();
        let key = DatabaseEntry::from_bytes(b"key_txn_timeout");
        let val = DatabaseEntry::from_bytes(b"value_txn_timeout");
        db.put(Some(&txn), &key, &val).unwrap();
        txn.commit().unwrap();
        println!("    txn_timeout_ms=5000 transaction committed\n");
    }

    // ── Verify all data ──────────────────────────────────────────────────────
    println!("[Verify] Reading back all keys...");
    for key_str in &[
        "key_default",
        "key_timeout",
        "key_serializable",
        "key_nowait",
        "key_importunate",
        "key_txn_timeout",
    ] {
        let key = DatabaseEntry::from_bytes(key_str.as_bytes());
        let mut val = DatabaseEntry::new();
        db.get(None, &key, &mut val).unwrap();
        println!(
            "    {}: {:?}",
            key_str,
            std::str::from_utf8(val.get_data().unwrap())
        );
    }

    println!("\n=== Success: all TransactionConfig options demonstrated ===");
}
