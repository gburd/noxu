//! Sequence example for Noxu DB.
//!
//! Example showing Noxu DB usage.java`.
//!
//! Demonstrates the Sequence (auto-increment) API:
//!   - Open an Environment and Database
//!   - Open a Sequence with allow_create=true and a cache_size of 5
//!   - Call seq.get() multiple times to obtain sequential IDs
//!   - Show cache behaviour: the first call triggers a DB write to reserve a
//!     batch; subsequent calls within the batch are served purely from the
//!     in-process cache without additional DB writes
//!   - Use sequence IDs as database keys to store string records
//!   - Retrieve those records back by their auto-assigned IDs

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    OperationStatus, SequenceConfig,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env_dir = std::env::temp_dir().join("noxu_sequence_example");
    let _ = std::fs::remove_dir_all(&env_dir);

    // --- Open environment ---
    println!("Opening environment at {:?}", env_dir);
    let env_config = EnvironmentConfig::new(env_dir.clone())
        .with_allow_create(true)
        .with_transactional(false);
    let env = Environment::open(env_config)?;

    // --- Open database ---
    let db_config = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, "seqDb", &db_config)?;

    // =========================================================================
    // Open a Sequence on key "item_id_seq".
    //
    // cache_size=5 means the handle will reserve 5 IDs at a time from the
    // database record.  The first call to seq.get() writes the new batch
    // boundary to the DB; calls 2-5 are served from the in-process cache
    // without touching the database.
    // =========================================================================
    let seq_key = DatabaseEntry::from_bytes(b"item_id_seq");
    let seq_config = SequenceConfig::new()
        .with_allow_create(true)
        .with_initial_value(1)
        .with_cache_size(5);
    let seq = db.open_sequence(&seq_key, seq_config)?;

    // =========================================================================
    // Get sequential IDs, demonstrating the cache.
    // =========================================================================
    println!("\nRequesting 10 sequential IDs (cache_size=5):");
    println!("  IDs 1-5 are served after the first DB refill.");
    println!("  IDs 6-10 are served after the second DB refill.");

    let mut ids: Vec<i64> = Vec::new();
    for _ in 0..10 {
        let id = seq.get(None, 1)?;
        ids.push(id);
    }
    for (i, id) in ids.iter().enumerate() {
        println!("  call {}: id={}", i + 1, id);
    }

    // Verify IDs are strictly monotonically increasing.
    for window in ids.windows(2) {
        assert!(
            window[1] > window[0],
            "IDs must be strictly increasing: {} <= {}",
            window[1],
            window[0]
        );
    }
    println!("  All IDs are strictly increasing — verified.");

    // =========================================================================
    // Show sequence statistics.
    // =========================================================================
    let stats = seq.get_stats();
    println!("\nSequence statistics after 10 gets:");
    println!("  n_gets:        {}", stats.n_gets);
    println!("  n_cache_hits:  {}", stats.n_cache_hits);
    println!("  cache_value:   {}", stats.cache_value);
    println!("  cache_last:    {}", stats.cache_last);
    println!("  range_min:     {}", stats.range_min);
    println!("  range_max:     {}", stats.range_max);
    println!("  cache_size:    {}", stats.cache_size);

    // After 10 gets with cache_size=5 we expect exactly 2 DB refills,
    // meaning 10 - 2 = 8 cache hits.
    let expected_cache_hits = stats.n_gets - 2;
    println!(
        "  (expected ~{} cache hits for 2 refills; actual {})",
        expected_cache_hits, stats.n_cache_hits
    );

    // =========================================================================
    // Use sequence IDs as database keys to store records.
    // =========================================================================
    println!("\nOpening a second sequence (record_id_seq) to key records:");
    let rec_seq_key = DatabaseEntry::from_bytes(b"record_id_seq");
    let rec_seq_config = SequenceConfig::new()
        .with_allow_create(true)
        .with_initial_value(100)
        .with_cache_size(5);
    let rec_seq = db.open_sequence(&rec_seq_key, rec_seq_config)?;

    let items = ["apple", "banana", "cherry", "date", "elderberry"];
    println!("  Storing {} items with auto-assigned IDs:", items.len());
    let mut assigned_ids: Vec<i64> = Vec::new();
    for item in &items {
        let id = rec_seq.get(None, 1)?;
        assigned_ids.push(id);

        // Encode the i64 ID as a big-endian 8-byte key so that records are
        // stored in ID order (byte-sortable).
        let key = DatabaseEntry::from_bytes(&id.to_be_bytes());
        let value = DatabaseEntry::from_bytes(item.as_bytes());
        let status = db.put(None, &key, &value)?;
        assert_eq!(status, OperationStatus::Success);
        println!("    id={} -> {}", id, item);
    }

    // =========================================================================
    // Retrieve items back by their auto-assigned IDs.
    // =========================================================================
    println!("\nRetrieving items by their assigned IDs:");
    for (item, id) in items.iter().zip(assigned_ids.iter()) {
        let key = DatabaseEntry::from_bytes(&id.to_be_bytes());
        let mut data = DatabaseEntry::new();
        let status = db.get(None, &key, &mut data)?;
        match status {
            OperationStatus::Success => {
                let value = std::str::from_utf8(data.data()).unwrap_or("?");
                let matches = value == *item;
                println!("    id={} -> {} (match={})", id, value, matches);
            }
            OperationStatus::NotFound => {
                println!("    id={} -> NOT FOUND (unexpected)", id);
            }
            _ => {}
        }
    }

    // =========================================================================
    // Demonstrate delta > 1: reserve a block of 3 IDs at once.
    // =========================================================================
    println!("\nDemonstrating delta=3 (reserve a block of 3 IDs at once):");
    let block_start = seq.get(None, 3)?;
    println!(
        "  Reserved IDs: {}, {}, {}",
        block_start,
        block_start + 1,
        block_start + 2
    );
    println!("  Next single get:");
    let next_id = seq.get(None, 1)?;
    println!("  id={}", next_id);
    assert!(next_id > block_start + 2, "next ID must follow the block");

    // =========================================================================
    // Demonstrate a bounded sequence with wrap-around.
    // =========================================================================
    println!("\nDemonstrating a small bounded sequence [0, 4] with wrap:");
    let wrap_key = DatabaseEntry::from_bytes(b"wrap_seq");
    let wrap_config = SequenceConfig::new()
        .with_allow_create(true)
        .with_range(0, 4)
        .with_initial_value(0)
        .with_wrap(true)
        .with_cache_size(0);
    let wrap_seq = db.open_sequence(&wrap_key, wrap_config)?;

    for _ in 0..7 {
        let v = wrap_seq.get(None, 1)?;
        println!("  {}", v);
    }

    // --- Cleanup ---
    seq.close()?;
    rec_seq.close()?;
    wrap_seq.close()?;
    drop(db);
    drop(env);
    let _ = std::fs::remove_dir_all(&env_dir);

    println!("\nDone!");
    Ok(())
}
