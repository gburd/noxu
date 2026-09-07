//! Noxu crash-durability control test. Same protocol as the C tdb_crash /
//! wt_crash drivers.
//!
//!   write <dir> <ackfile>:
//!     open env at COMMIT_SYNC, single thread, insert id=0,1,2,...  After each
//!     txn.commit() returns Ok, record the highest acked id into <ackfile>
//!     (write + sync_data of the ackfile itself). Loop forever; never close
//!     the env cleanly (parent kill -9's us).
//!   verify <dir> <ackfile>:
//!     reopen the SAME dir, read last acked id A, get every id in [0,A], count
//!     survivors. Noxu is per-commit durable so expect survivors == A+1.

use noxu_db::{DatabaseConfig, Durability, Environment, EnvironmentConfig};
use std::fs::OpenOptions;
use std::io::{Seek, Write};
use std::sync::Arc;

fn key_bytes(id: u64) -> [u8; 16] {
    let mut k = [0u8; 16];
    k[..8].copy_from_slice(&id.to_be_bytes());
    k[8..].copy_from_slice(&id.wrapping_mul(2654435761).to_be_bytes());
    k
}

const VSZ: usize = 1024;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!("usage: {} write|verify <dir> <ackfile>", args[0]);
        std::process::exit(2);
    }
    let mode = &args[1];
    let dir = &args[2];
    let ackf = &args[3];

    let mut ecfg = EnvironmentConfig::new(std::path::PathBuf::from(dir));
    ecfg.set_allow_create(true);
    ecfg.set_transactional(true);
    ecfg.set_cache_size(256 * 1024 * 1024);
    ecfg.set_durability(Durability::COMMIT_SYNC);
    let env = Arc::new(Environment::open(ecfg).expect("open env"));
    let db = Arc::new(
        env.open_database(
            None,
            "crash",
            &DatabaseConfig::new()
                .with_allow_create(true)
                .with_transactional(true),
        )
        .expect("open db"),
    );

    let value = vec![0x5Au8; VSZ];

    if mode == "write" {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(ackf)
            .expect("open ackfile");
        let mut id: u64 = 0;
        loop {
            if let Ok(txn) = env.begin_transaction(None)
                && db.put_in(&txn, key_bytes(id), &value).is_ok()
                && txn.commit().is_ok()
            {
                // ACKED. Durably record the acked id (sync_data == fdatasync).
                let _ = f.seek(std::io::SeekFrom::Start(0));
                let _ = writeln!(f, "{id}");
                let _ = f.flush();
                let _ = f.sync_data();
                id += 1;
            }
        }
    } else {
        let acked: u64 = std::fs::read_to_string(ackf)
            .expect("read ackfile")
            .trim()
            .parse()
            .expect("parse ackfile");
        let mut survived: u64 = 0;
        let mut missing: u64 = 0;
        let mut first_missing: i64 = -1;
        for id in 0..=acked {
            if let Ok(txn) = env.begin_transaction(None) {
                match db.get_in(&txn, key_bytes(id)) {
                    Ok(Some(_)) => survived += 1,
                    _ => {
                        missing += 1;
                        if first_missing < 0 {
                            first_missing = id as i64;
                        }
                    }
                }
                let _ = txn.commit();
            }
        }
        println!(
            "CRASHTEST engine=noxu acked={acked} expected={} survived={survived} missing={missing} first_missing={first_missing}",
            acked + 1
        );
    }
}
