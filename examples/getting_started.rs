//! Getting Started example for Noxu DB.
//!
//! Introductory Noxu DB example — getting started.
//! (Inventory/Vendor example from the).
//!
//! Demonstrates a complete "getting started" scenario:
//!   - Define two record types: Vendor and Item
//!   - Serialize / deserialize them using noxu-bind TupleOutput / TupleInput
//!   - Store vendors keyed by name and items keyed by SKU
//!   - Perform full put / get / scan workflows
//!   - Query inventory for items supplied by a specific vendor

use noxu_bind::{EntryBinding, TupleInput, TupleOutput};
use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, Get,
    OperationStatus,
};

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

/// A vendor in the catalog.
#[derive(Debug, Clone, PartialEq)]
struct Vendor {
    /// Company name — used as the primary key.
    name: String,
    street: String,
    city: String,
    phone: String,
}

/// An inventory item.
#[derive(Debug, Clone, PartialEq)]
struct Item {
    /// Stock-keeping unit — used as the primary key.
    sku: String,
    item_name: String,
    /// Name of the supplying vendor (foreign key into the vendors database).
    vendor_name: String,
    price: f64,
    /// Units currently in stock.
    inventory: u32,
}

// ---------------------------------------------------------------------------
// Bindings: serialize/deserialize using TupleOutput / TupleInput
// ---------------------------------------------------------------------------

/// Binding for `Vendor`.
///
/// Wire format (using TupleOutput helpers):
///   name (string), street (string), city (string), phone (string)
struct VendorBinding;

impl EntryBinding<Vendor> for VendorBinding {
    fn object_to_entry(
        &self,
        vendor: &Vendor,
        entry: &mut DatabaseEntry,
    ) -> noxu_bind::Result<()> {
        let mut out = TupleOutput::new();
        out.write_string(&vendor.name);
        out.write_string(&vendor.street);
        out.write_string(&vendor.city);
        out.write_string(&vendor.phone);
        entry.set_data(out.as_bytes());
        Ok(())
    }

    fn entry_to_object(&self, entry: &DatabaseEntry) -> noxu_bind::Result<Vendor> {
        let mut input = TupleInput::new(entry.data());
        let name = input.read_string()?;
        let street = input.read_string()?;
        let city = input.read_string()?;
        let phone = input.read_string()?;
        Ok(Vendor { name, street, city, phone })
    }
}

/// Binding for `Item`.
///
/// Wire format:
///   sku (string), item_name (string), vendor_name (string),
///   price (f64 big-endian), inventory (u32 big-endian)
struct ItemBinding;

impl EntryBinding<Item> for ItemBinding {
    fn object_to_entry(
        &self,
        item: &Item,
        entry: &mut DatabaseEntry,
    ) -> noxu_bind::Result<()> {
        let mut out = TupleOutput::new();
        out.write_string(&item.sku);
        out.write_string(&item.item_name);
        out.write_string(&item.vendor_name);
        out.write_double(item.price);
        out.write_u32(item.inventory);
        entry.set_data(out.as_bytes());
        Ok(())
    }

    fn entry_to_object(&self, entry: &DatabaseEntry) -> noxu_bind::Result<Item> {
        let mut input = TupleInput::new(entry.data());
        let sku = input.read_string()?;
        let item_name = input.read_string()?;
        let vendor_name = input.read_string()?;
        let price = input.read_double()?;
        let inventory = input.read_u32()?;
        Ok(Item { sku, item_name, vendor_name, price, inventory })
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let env_dir = std::env::temp_dir().join("noxu_getting_started_example");
    let _ = std::fs::remove_dir_all(&env_dir);

    println!("Opening environment at {:?}", env_dir);

    // --- Open environment ---
    let env_config = EnvironmentConfig::new(env_dir.clone())
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(env_config)?;

    // --- Open databases ---
    let db_config = DatabaseConfig::new().with_allow_create(true);
    let vendor_db = env.open_database(None, "vendors", &db_config)?;
    let item_db = env.open_database(None, "items", &db_config)?;

    let vendor_binding = VendorBinding;
    let item_binding = ItemBinding;

    // =========================================================================
    // Store vendors
    // =========================================================================
    let vendors = vec![
        Vendor {
            name: "Acme Foods".to_string(),
            street: "123 Main St".to_string(),
            city: "Springfield".to_string(),
            phone: "555-0100".to_string(),
        },
        Vendor {
            name: "Fresh Farms".to_string(),
            street: "456 Oak Ave".to_string(),
            city: "Shelbyville".to_string(),
            phone: "555-0200".to_string(),
        },
        Vendor {
            name: "Sunrise Organics".to_string(),
            street: "789 Elm Blvd".to_string(),
            city: "Capital City".to_string(),
            phone: "555-0300".to_string(),
        },
    ];

    println!("\nStoring {} vendors:", vendors.len());
    for vendor in &vendors {
        let key = DatabaseEntry::from_bytes(vendor.name.as_bytes());
        let mut data = DatabaseEntry::new();
        vendor_binding.object_to_entry(vendor, &mut data)?;
        let status = vendor_db.put(None, &key, &data)?;
        assert_eq!(status, OperationStatus::Success);
        println!("  {} ({}, {})", vendor.name, vendor.city, vendor.phone);
    }

    // =========================================================================
    // Store inventory items
    // =========================================================================
    let items = vec![
        Item {
            sku: "APPLE-001".to_string(),
            item_name: "Granny Smith Apples".to_string(),
            vendor_name: "Acme Foods".to_string(),
            price: 1.99,
            inventory: 500,
        },
        Item {
            sku: "BANANA-001".to_string(),
            item_name: "Cavendish Bananas".to_string(),
            vendor_name: "Fresh Farms".to_string(),
            price: 0.79,
            inventory: 300,
        },
        Item {
            sku: "CARROT-001".to_string(),
            item_name: "Organic Carrots".to_string(),
            vendor_name: "Sunrise Organics".to_string(),
            price: 2.49,
            inventory: 200,
        },
        Item {
            sku: "APPLE-002".to_string(),
            item_name: "Fuji Apples".to_string(),
            vendor_name: "Acme Foods".to_string(),
            price: 2.29,
            inventory: 400,
        },
        Item {
            sku: "TOMATO-001".to_string(),
            item_name: "Heirloom Tomatoes".to_string(),
            vendor_name: "Sunrise Organics".to_string(),
            price: 3.99,
            inventory: 150,
        },
        Item {
            sku: "LETTUCE-001".to_string(),
            item_name: "Romaine Lettuce".to_string(),
            vendor_name: "Fresh Farms".to_string(),
            price: 1.49,
            inventory: 250,
        },
    ];

    println!("\nStoring {} inventory items:", items.len());
    for item in &items {
        let key = DatabaseEntry::from_bytes(item.sku.as_bytes());
        let mut data = DatabaseEntry::new();
        item_binding.object_to_entry(item, &mut data)?;
        let status = item_db.put(None, &key, &data)?;
        assert_eq!(status, OperationStatus::Success);
        println!(
            "  SKU={} name='{}' vendor='{}' price={:.2} qty={}",
            item.sku, item.item_name, item.vendor_name, item.price, item.inventory
        );
    }

    // =========================================================================
    // Retrieve a vendor by name
    // =========================================================================
    println!("\nRetrieving vendor 'Fresh Farms':");
    {
        let key = DatabaseEntry::from_bytes(b"Fresh Farms");
        let mut data = DatabaseEntry::new();
        let status = vendor_db.get(None, &key, &mut data)?;
        if status == OperationStatus::Success {
            let vendor = vendor_binding.entry_to_object(&data)?;
            println!(
                "  name={} street={} city={} phone={}",
                vendor.name, vendor.street, vendor.city, vendor.phone
            );
        }
    }

    // =========================================================================
    // Retrieve an item by SKU
    // =========================================================================
    println!("\nRetrieving item SKU='CARROT-001':");
    {
        let key = DatabaseEntry::from_bytes(b"CARROT-001");
        let mut data = DatabaseEntry::new();
        let status = item_db.get(None, &key, &mut data)?;
        if status == OperationStatus::Success {
            let item = item_binding.entry_to_object(&data)?;
            println!(
                "  sku={} name='{}' vendor='{}' price={:.2} qty={}",
                item.sku, item.item_name, item.vendor_name, item.price, item.inventory
            );
        }
    }

    // =========================================================================
    // Scan all items and print them in SKU (key) order
    // =========================================================================
    println!("\nAll inventory items (sorted by SKU):");
    {
        let mut cursor = item_db.open_cursor(None, None)?;
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let mut status = cursor.get(&mut key, &mut data, Get::First, None)?;
        while status == OperationStatus::Success {
            let item = item_binding.entry_to_object(&data)?;
            println!(
                "  SKU={} '{}' ${:.2} qty={}",
                item.sku, item.item_name, item.price, item.inventory
            );
            status = cursor.get(&mut key, &mut data, Get::Next, None)?;
        }
        cursor.close()?;
    }

    // =========================================================================
    // Query: find all items supplied by "Acme Foods" and look up the vendor.
    // =========================================================================
    let target_vendor = "Acme Foods";
    println!("\nItems supplied by '{}':", target_vendor);
    {
        let mut cursor = item_db.open_cursor(None, None)?;
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let mut status = cursor.get(&mut key, &mut data, Get::First, None)?;
        while status == OperationStatus::Success {
            let item = item_binding.entry_to_object(&data)?;
            if item.vendor_name == target_vendor {
                println!(
                    "  SKU={} '{}' ${:.2} qty={}",
                    item.sku, item.item_name, item.price, item.inventory
                );
            }
            status = cursor.get(&mut key, &mut data, Get::Next, None)?;
        }
        cursor.close()?;

        // Look up the vendor details.
        let vendor_key = DatabaseEntry::from_bytes(target_vendor.as_bytes());
        let mut vendor_data = DatabaseEntry::new();
        let vstatus = vendor_db.get(None, &vendor_key, &mut vendor_data)?;
        if vstatus == OperationStatus::Success {
            let vendor = vendor_binding.entry_to_object(&vendor_data)?;
            println!(
                "  Vendor contact: {} — {} — {}",
                vendor.street, vendor.city, vendor.phone
            );
        }
    }

    // =========================================================================
    // Scan all vendors
    // =========================================================================
    println!("\nAll vendors (sorted by name):");
    {
        let mut cursor = vendor_db.open_cursor(None, None)?;
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let mut status = cursor.get(&mut key, &mut data, Get::First, None)?;
        while status == OperationStatus::Success {
            let vendor = vendor_binding.entry_to_object(&data)?;
            println!(
                "  '{}' — {} — {} — {}",
                vendor.name, vendor.street, vendor.city, vendor.phone
            );
            status = cursor.get(&mut key, &mut data, Get::Next, None)?;
        }
        cursor.close()?;
    }

    // =========================================================================
    // Round-trip verification: deserialize everything back and compare
    // =========================================================================
    println!("\nRound-trip verification for all items:");
    {
        let mut verified = 0usize;
        let mut cursor = item_db.open_cursor(None, None)?;
        let mut key = DatabaseEntry::new();
        let mut data = DatabaseEntry::new();

        let mut status = cursor.get(&mut key, &mut data, Get::First, None)?;
        while status == OperationStatus::Success {
            let item = item_binding.entry_to_object(&data)?;
            // Re-serialize and deserialize to confirm round-trip correctness.
            let mut roundtrip_entry = DatabaseEntry::new();
            item_binding.object_to_entry(&item, &mut roundtrip_entry)?;
            let item2 = item_binding.entry_to_object(&roundtrip_entry)?;
            assert_eq!(item, item2, "round-trip mismatch for SKU={}", item.sku);
            verified += 1;
            status = cursor.get(&mut key, &mut data, Get::Next, None)?;
        }
        cursor.close()?;
        println!("  {} items verified (serialize -> deserialize -> compare)", verified);
    }

    // --- Cleanup ---
    drop(item_db);
    drop(vendor_db);
    drop(env);
    let _ = std::fs::remove_dir_all(&env_dir);

    println!("\nDone!");
    Ok(())
}
