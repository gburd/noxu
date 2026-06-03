//! XA Distributed Transaction Example
//!
//! Demonstrates coordinating a two-phase commit across two independent
//! Noxu DB environments using the `noxu-xa` crate.
//!
//! Run with: cargo run --example xa_distributed

use noxu::xa::{PrepareResult, XaEnvironment, XaFlags, XaResource, Xid};
use noxu::{DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig};
use tempfile::TempDir;

fn main() {
    println!("=== XA Distributed Transaction Example ===\n");

    // Create two independent environments (simulating two databases)
    let dir1 = TempDir::new().unwrap();
    let dir2 = TempDir::new().unwrap();

    let env1 = Environment::open(
        EnvironmentConfig::new(dir1.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();
    let env2 = Environment::open(
        EnvironmentConfig::new(dir2.path().to_path_buf())
            .with_allow_create(true)
            .with_transactional(true),
    )
    .unwrap();

    // Wrap each in XaEnvironment
    let xa1 = XaEnvironment::new(env1);
    let xa2 = XaEnvironment::new(env2);

    let db1 = xa1
        .inner()
        .open_database(
            None,
            "accounts",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .unwrap();
    let db2 = xa2
        .inner()
        .open_database(
            None,
            "ledger",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .unwrap();

    // Create a global transaction ID
    let xid = Xid::new(1, b"transfer_001", b"branch_main").unwrap();
    println!("Transaction ID: {xid}");

    // ─── Phase 0: Start branches ────────────────────────────────────────────
    println!("\n[Phase 0] Starting XA branches...");
    xa1.xa_start(&xid, XaFlags::NOFLAGS).unwrap();
    xa2.xa_start(&xid, XaFlags::NOFLAGS).unwrap();

    // ─── Do work ────────────────────────────────────────────────────────────
    println!("[Work]    Writing to both databases...");

    // Debit account in db1
    {
        let txn = xa1.get_transaction(&xid).unwrap();
        let key = DatabaseEntry::from_bytes(b"account_alice");
        let val = DatabaseEntry::from_bytes(b"balance:-100");
        db1.put(Some(&*txn), &key, &val).unwrap();
        xa1.mark_write(&xid).unwrap();
    }

    // Credit ledger in db2
    {
        let txn = xa2.get_transaction(&xid).unwrap();
        let key = DatabaseEntry::from_bytes(b"ledger_entry_001");
        let val = DatabaseEntry::from_bytes(b"alice->bob:100");
        db2.put(Some(&*txn), &key, &val).unwrap();
        xa2.mark_write(&xid).unwrap();
    }

    // ─── End branches ───────────────────────────────────────────────────────
    xa1.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();
    xa2.xa_end(&xid, XaFlags::TMSUCCESS).unwrap();

    // ─── Phase 1: Prepare ───────────────────────────────────────────────────
    println!("[Phase 1] Preparing...");
    let p1 = xa1.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
    let p2 = xa2.xa_prepare(&xid, XaFlags::NOFLAGS).unwrap();
    println!("          DB1: {p1:?}, DB2: {p2:?}");

    // ─── Phase 2: Commit ────────────────────────────────────────────────────
    println!("[Phase 2] Committing...");
    if p1 == PrepareResult::Ok {
        xa1.xa_commit(&xid, XaFlags::NOFLAGS).unwrap();
    }
    if p2 == PrepareResult::Ok {
        xa2.xa_commit(&xid, XaFlags::NOFLAGS).unwrap();
    }

    // ─── Verify ─────────────────────────────────────────────────────────────
    println!("\n[Verify]  Reading committed data...");
    let mut val = DatabaseEntry::new();
    db1.get(None, &DatabaseEntry::from_bytes(b"account_alice"), &mut val)
        .unwrap();
    println!(
        "          DB1 account_alice: {:?}",
        std::str::from_utf8(val.get_data().unwrap())
    );

    db2.get(None, &DatabaseEntry::from_bytes(b"ledger_entry_001"), &mut val)
        .unwrap();
    println!(
        "          DB2 ledger_entry:  {:?}",
        std::str::from_utf8(val.get_data().unwrap())
    );

    println!("\n=== Success: distributed transaction committed atomically ===");
}
