//! Secondary database example for Noxu DB.
//!
//! Example showing Noxu DB usage.java`.
//!
//! Demonstrates secondary databases (secondary indexes):
//!   - Open a primary database keyed by employee name
//!   - Open a SecondaryDatabase that indexes employees by department
//!   - Put employee records into the primary (the secondary is updated via
//!     `update_secondary`)
//!   - Look up records via secondary key (department)
//!   - Iterate all entries via SecondaryCursor
//!   - Show that deleting from the primary cascades to the secondary

use noxu::Mutex;
use noxu::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
    OperationStatus, SecondaryConfig, SecondaryDatabase, SecondaryKeyCreator,
};
use std::sync::Arc;

// ---------------------------------------------------------------------------
// Key creator: extracts the department (everything before '|') from the
// primary value, which is stored as "department|title".
// ---------------------------------------------------------------------------

struct DepartmentKeyCreator;

impl SecondaryKeyCreator for DepartmentKeyCreator {
    fn create_secondary_key(
        &self,
        _secondary_db: &Database,
        _key: &DatabaseEntry,
        data: &DatabaseEntry,
        result: &mut DatabaseEntry,
    ) -> bool {
        if let Some(bytes) = data.data_opt()
            && let Ok(s) = std::str::from_utf8(bytes)
            && let Some(sep) = s.find('|')
        {
            result.set_data(&s.as_bytes()[..sep]);
            return true;
        }
        false
    }
}

// ---------------------------------------------------------------------------
// Helper: insert a primary record and manually update the secondary index.
//
// In a fully-integrated implementation the secondary would be updated
// automatically on every primary put; for now we mirror the pattern used in
// the test suite (manual update_secondary call).
// ---------------------------------------------------------------------------

fn put_employee(
    primary: &Arc<Mutex<Database>>,
    secondary: &SecondaryDatabase,
    name: &str,
    department: &str,
    title: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let key = DatabaseEntry::from_bytes(name.as_bytes());
    // Value encodes "department|title" so the key creator can extract the
    // department as the secondary key.
    let value_str = format!("{}|{}", department, title);
    let value = DatabaseEntry::from_bytes(value_str.as_bytes());

    // Insert into primary.
    primary.lock().put(&key, &value)?;

    // Update secondary index.  Pass `None` for the txn — this example
    // demonstrates auto-commit; see `docs/src/transactions/secondary-with-txn.md`
    // for the atomic-with-primary pattern that threads `Some(&txn)`
    // through both calls.
    secondary.update_secondary(None, &key, None, Some(&value))?;

    println!("  Inserted: {} -> {}", name, value_str);
    Ok(())
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env_dir = std::env::temp_dir().join("noxu_secondary_example");
    let _ = std::fs::remove_dir_all(&env_dir);

    // --- Open environment ---
    println!("Opening environment at {:?}", env_dir);
    let env_config = EnvironmentConfig::new(env_dir.clone())
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(env_config)?;

    // --- Open primary database (name -> "dept|title") ---
    let db_config = DatabaseConfig::new().with_allow_create(true);
    let primary_db = env.open_database(None, "employees", &db_config)?;
    let primary = Arc::new(Mutex::new(primary_db));

    // --- Open secondary database (department -> primary key) ---
    let sec_db_config = DatabaseConfig::new().with_allow_create(true);
    let sec_db = env.open_database(None, "by_department", &sec_db_config)?;
    let sec_config = SecondaryConfig::new()
        .with_allow_create(true)
        .with_allow_populate(true)
        .with_key_creator(Box::new(DepartmentKeyCreator));
    let secondary =
        SecondaryDatabase::open(Arc::clone(&primary), sec_db, sec_config)?;

    // --- Insert employee records ---
    println!("\nInserting employee records...");
    put_employee(
        &primary,
        &secondary,
        "Alice",
        "Engineering",
        "Senior Engineer",
    )?;
    put_employee(
        &primary,
        &secondary,
        "Bob",
        "Marketing",
        "Marketing Manager",
    )?;
    put_employee(
        &primary,
        &secondary,
        "Carol",
        "Engineering",
        "Staff Engineer",
    )?;
    put_employee(&primary, &secondary, "Dave", "HR", "HR Specialist")?;
    put_employee(
        &primary,
        &secondary,
        "Eve",
        "Engineering",
        "Junior Engineer",
    )?;
    put_employee(
        &primary,
        &secondary,
        "Frank",
        "Marketing",
        "Marketing Analyst",
    )?;

    // --- Look up by secondary key (department) ---
    println!("\nLooking up by department 'Engineering':");
    let eng_key = DatabaseEntry::from_bytes(b"Engineering");
    let mut p_key = DatabaseEntry::new();
    let mut data = DatabaseEntry::new();
    let status = secondary.get_into(None, &eng_key, &mut p_key, &mut data)?;
    if status {
        let employee =
            std::str::from_utf8(p_key.data_opt().unwrap_or(b"")).unwrap_or("?");
        let record =
            std::str::from_utf8(data.data_opt().unwrap_or(b"")).unwrap_or("?");
        println!("  Found: {} -> {}", employee, record);
    }

    println!("\nLooking up by department 'HR':");
    let hr_key = DatabaseEntry::from_bytes(b"HR");
    let status = secondary.get_into(None, &hr_key, &mut p_key, &mut data)?;
    if status {
        let employee =
            std::str::from_utf8(p_key.data_opt().unwrap_or(b"")).unwrap_or("?");
        let record =
            std::str::from_utf8(data.data_opt().unwrap_or(b"")).unwrap_or("?");
        println!("  Found: {} -> {}", employee, record);
    }

    println!("\nLooking up by department 'Finance' (should not exist):");
    let fin_key = DatabaseEntry::from_bytes(b"Finance");
    let status = secondary.get_into(None, &fin_key, &mut p_key, &mut data)?;
    println!("  Status: {:?}", status);

    // --- Iterate all entries via SecondaryCursor ---
    println!(
        "\nIterating all employees via SecondaryCursor (sorted by department):"
    );
    {
        let mut cursor = secondary.open_cursor(None)?;
        let mut sec_key = DatabaseEntry::new();
        let mut cursor_p_key = DatabaseEntry::new();
        let mut cursor_data = DatabaseEntry::new();

        let mut scan_status = cursor.get_first(
            &mut sec_key,
            &mut cursor_p_key,
            &mut cursor_data,
        )?;
        let mut count = 0;
        while scan_status == OperationStatus::Success {
            let dept = std::str::from_utf8(sec_key.data_opt().unwrap_or(b""))
                .unwrap_or("?");
            let name =
                std::str::from_utf8(cursor_p_key.data_opt().unwrap_or(b""))
                    .unwrap_or("?");
            let record =
                std::str::from_utf8(cursor_data.data_opt().unwrap_or(b""))
                    .unwrap_or("?");
            println!("  dept={} name={} record={}", dept, name, record);
            count += 1;
            scan_status = cursor.get_next(
                &mut sec_key,
                &mut cursor_p_key,
                &mut cursor_data,
            )?;
        }
        println!("  Total entries: {}", count);
        cursor.close()?;
    }

    // --- Search for first employee in Marketing via cursor ---
    println!("\nSearching secondary cursor for 'Marketing':");
    {
        let mut cursor2 = secondary.open_cursor(None)?;
        let mkt_key = DatabaseEntry::from_bytes(b"Marketing");
        let mut pk2 = DatabaseEntry::new();
        let mut d2 = DatabaseEntry::new();
        let mkt_status = cursor2.get_search_key(&mkt_key, &mut pk2, &mut d2)?;
        if mkt_status == OperationStatus::Success {
            let name = std::str::from_utf8(pk2.data_opt().unwrap_or(b""))
                .unwrap_or("?");
            let record = std::str::from_utf8(d2.data_opt().unwrap_or(b""))
                .unwrap_or("?");
            println!("  First Marketing employee: {} -> {}", name, record);
        }
        cursor2.close()?;
    }

    // --- Delete Carol from primary; verify secondary cascade ---
    println!("\nDeleting 'Carol' from primary...");
    let carol_key = DatabaseEntry::from_bytes(b"Carol");
    // First remove Carol's secondary index entry, then delete from primary
    // (mirrors the pattern in the test suite for the manual-update path).
    let carol_val_str = "Engineering|Staff Engineer".to_string();
    let carol_val = DatabaseEntry::from_bytes(carol_val_str.as_bytes());
    secondary.update_secondary(None, &carol_key, Some(&carol_val), None)?;
    let del_status = primary.lock().delete(&carol_key)?;
    println!("  Primary delete status: {:?}", del_status);

    // Verify Carol is gone from primary.
    let mut check_data = DatabaseEntry::new();
    let check_status =
        primary.lock().get_into(None, &carol_key, &mut check_data)?;
    println!("  Carol in primary after delete: {:?}", check_status);

    // The secondary Engineering key now maps to one fewer employee.
    println!("\nEngineering department entries after deleting Carol:");
    {
        let mut cursor3 = secondary.open_cursor(None)?;
        let eng_search = DatabaseEntry::from_bytes(b"Engineering");
        let mut pk3 = DatabaseEntry::new();
        let mut d3 = DatabaseEntry::new();
        let eng_status =
            cursor3.get_search_key(&eng_search, &mut pk3, &mut d3)?;
        if eng_status == OperationStatus::Success {
            let name = std::str::from_utf8(pk3.data_opt().unwrap_or(b""))
                .unwrap_or("?");
            let record = std::str::from_utf8(d3.data_opt().unwrap_or(b""))
                .unwrap_or("?");
            println!("  {} -> {}", name, record);
        } else {
            println!("  (none found)");
        }
        cursor3.close()?;
    }

    // --- Cleanup ---
    secondary.close()?;
    drop(secondary);
    drop(primary);
    drop(env);
    let _ = std::fs::remove_dir_all(&env_dir);

    println!("\nDone!");
    Ok(())
}
