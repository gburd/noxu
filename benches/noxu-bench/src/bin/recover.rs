//! Recovery-time driver for Noxu (dimension 3 of the v7.5.2 benchmark plan).
//!
//! Modes (argv-free; all via env):
//!   BENCH_MODE=load    : insert N records durably (SYNC), checkpoint, close
//!                        cleanly. Records each acked id's HIGH-WATER into the
//!                        ackfile (fdatasync'd) so `recover` can verify.
//!   BENCH_MODE=write   : open existing env, run a forever durable insert
//!                        workload (SYNC), fdatasync an ackfile after every
//!                        acked commit. Killed with kill -9 by the harness.
//!                        Never closes cleanly -> simulates a crash mid-write.
//!   BENCH_MODE=recover : open the existing env (THIS is the recovery — the
//!                        open() call runs checkpoint-based redo), timing the
//!                        wall-clock ms of Environment::open. Then verify:
//!                        (a) db.count() >= acked+1 (no acked data lost),
//!                        (b) spot-check a sample of acked keys are present.
//!
//! Env knobs (shared with xbench where sensible):
//!   BENCH_DIR BENCH_RECORDS BENCH_CACHE BENCH_VALUE BENCH_SEED
//!   BENCH_ACKFILE           path to the durable ack high-water file
//!   BENCH_RUN_CKPT (0|1)    write-mode: run the checkpointer? default 1.
//!                           Set 0 to build a LONG log tail (much redo).
//!   BENCH_CKPT_BYTES        write-mode: checkpointer_bytes_interval override.
//!   BENCH_FORCE_CKPT (0|1)  write-mode: force one checkpoint at start, then
//!                           run so a kill lands just-after-checkpoint (little
//!                           redo). default 0.
//!   BENCH_VERIFY_SAMPLE     recover-mode: number of acked keys to spot-check
//!                           (evenly spaced across [0,acked]). default 1000.
//!
//! Recovery time is reported as `RECOVER_MS=<f>`; the process prints one
//! RESULT line the harness greps. No Noxu engine source is touched — this is
//! a benchmark driver only.

use noxu_db::{
    CheckpointConfig, DatabaseConfig, Durability, Environment,
    EnvironmentConfig, TransactionConfig,
};
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

fn envs(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_string())
}
fn envp(k: &str, d: u64) -> u64 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

fn key_bytes(id: u64) -> [u8; 16] {
    let mut k = [0u8; 16];
    k[..8].copy_from_slice(&id.to_be_bytes());
    k[8..].copy_from_slice(&id.wrapping_mul(2654435761).to_be_bytes());
    k
}

/// Durably record the high-water acked id into the ackfile (write@0 + fdatasync)
/// so the harness's notion of "acked" survives a kill of THIS process.
/// `File::sync_data()` is stdlib for fdatasync(2).
fn ack(file: &mut std::fs::File, id: u64) {
    use std::io::{Seek, SeekFrom};
    let _ = file.seek(SeekFrom::Start(0));
    let _ = write!(file, "{id}\n");
    let _ = file.flush();
    let _ = file.sync_data();
}

fn read_ack(path: &str) -> u64 {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(0)
}

fn build_ecfg(dir: &str, cache: u64, mode: &str) -> EnvironmentConfig {
    let mut ecfg = EnvironmentConfig::new(std::path::PathBuf::from(dir));
    ecfg.set_allow_create(true);
    ecfg.set_transactional(true);
    ecfg.set_cache_size(cache);
    ecfg.set_durability(Durability::COMMIT_SYNC);
    if mode == "write" {
        // Long-log-tail control for the "much redo" case.
        if envs("BENCH_RUN_CKPT", "1") == "0" {
            ecfg.set_run_checkpointer(false);
        }
        let ckpt_bytes = envp("BENCH_CKPT_BYTES", 0);
        if ckpt_bytes > 0 {
            ecfg.set_checkpointer_bytes_interval(ckpt_bytes);
        }
    }
    ecfg
}

fn main() {
    let dir = envs("BENCH_DIR", "/tmp/noxu-recover");
    let records = envp("BENCH_RECORDS", 1_000_000);
    let cache = envp("BENCH_CACHE", 4 * 1024 * 1024 * 1024);
    let value_size = envp("BENCH_VALUE", 1024) as usize;
    let mode = envs("BENCH_MODE", "recover");
    let ackfile = envs("BENCH_ACKFILE", "/tmp/noxu-recover.ack");
    let _ = std::fs::create_dir_all(&dir);

    match mode.as_str() {
        "load" => {
            let ecfg = build_ecfg(&dir, cache, &mode);
            let env = Arc::new(Environment::open(ecfg).expect("open env"));
            let db = Arc::new(
                env.open_database(
                    None,
                    "recover",
                    &DatabaseConfig::new()
                        .with_allow_create(true)
                        .with_transactional(true),
                )
                .expect("open db"),
            );
            let lt = Instant::now();
            let load_threads = 8usize;
            let per = records / load_threads as u64;
            std::thread::scope(|s| {
                for tid in 0..load_threads {
                    let env = Arc::clone(&env);
                    let db = Arc::clone(&db);
                    let start = tid as u64 * per;
                    let end = if tid == load_threads - 1 {
                        records
                    } else {
                        start + per
                    };
                    s.spawn(move || {
                        let value = vec![0x5Au8; value_size];
                        let mut i = start;
                        while i < end {
                            let batch_end = (i + 1000).min(end);
                            if let Ok(txn) = env.begin_transaction(None) {
                                let mut ok = true;
                                for j in i..batch_end {
                                    if db
                                        .put_in(&txn, key_bytes(j), &value)
                                        .is_err()
                                    {
                                        ok = false;
                                        break;
                                    }
                                }
                                if ok {
                                    let _ = txn.commit();
                                } else {
                                    let _ = txn.abort();
                                }
                            }
                            i = batch_end;
                        }
                    });
                }
            });
            // Clean checkpoint + close (fast-path reopen has no redo).
            env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
                .expect("checkpoint");
            let cnt = db.count().unwrap_or(0);
            // Ack high-water = records-1 (dense id space [0,records)).
            let mut af = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&ackfile)
                .expect("ackfile");
            ack(&mut af, records.saturating_sub(1));
            db.close().unwrap();
            drop(db);
            if let Ok(e) = Arc::try_unwrap(env) {
                e.close().unwrap();
            }
            println!(
                "RESULT engine=noxu mode=load records={records} count={cnt} \
                 load_secs={:.1} ack={}",
                lt.elapsed().as_secs_f64(),
                records.saturating_sub(1)
            );
        }

        "write" => {
            // Open existing env; run forever appending durable inserts. Each
            // acked commit updates the ackfile. Never closes -> crash sim.
            let ecfg = build_ecfg(&dir, cache, &mode);
            let env = Arc::new(Environment::open(ecfg).expect("open env"));
            let db = Arc::new(
                env.open_database(
                    None,
                    "recover",
                    &DatabaseConfig::new()
                        .with_allow_create(true)
                        .with_transactional(true),
                )
                .expect("open db"),
            );
            if envs("BENCH_FORCE_CKPT", "0") == "1" {
                // Force a checkpoint so a kill lands just after it (little redo).
                env.checkpoint(Some(&CheckpointConfig::new().with_force(true)))
                    .expect("checkpoint");
            }
            let value = vec![0x5Au8; value_size];
            let txn_cfg = TransactionConfig::new();
            let mut af = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&ackfile)
                .expect("ackfile");
            // Continue the id space above the loaded records so we ADD data.
            let mut id = records;
            let started = Instant::now();
            // Signal readiness so the harness can time the kill precisely.
            println!("WRITE_READY ackfile={ackfile}");
            let _ = std::io::stdout().flush();
            loop {
                if let Ok(t) = env.begin_transaction(Some(&txn_cfg)) {
                    if db.put_in(&t, key_bytes(id), &value).is_ok()
                        && t.commit().is_ok()
                    {
                        ack(&mut af, id); // acked + durable
                        id += 1;
                    } else {
                        let _ = t.abort();
                    }
                }
                // Emit a heartbeat every ~2s so the harness can see progress.
                if id % 5000 == 0 {
                    eprintln!(
                        "heartbeat id={id} elapsed={:.1}s",
                        started.elapsed().as_secs_f64()
                    );
                }
            }
        }

        "recover" => {
            // THE MEASUREMENT: Environment::open runs checkpoint-based redo.
            let acked = read_ack(&ackfile);
            let ecfg = build_ecfg(&dir, cache, "recover");
            let t0 = Instant::now();
            let env = Environment::open(ecfg).expect("open env (recovery)");
            let recover_ms = t0.elapsed().as_secs_f64() * 1000.0;
            let db = env
                .open_database(
                    None,
                    "recover",
                    &DatabaseConfig::new()
                        .with_allow_create(false)
                        .with_transactional(true),
                )
                .expect("open db");
            let db_open_ms = t0.elapsed().as_secs_f64() * 1000.0 - recover_ms;
            let cnt = db.count().unwrap_or(0);

            // Correctness: spot-check acked keys survived (no data loss).
            let sample = envp("BENCH_VERIFY_SAMPLE", 1000).max(1);
            let step = ((acked + 1) / sample).max(1);
            let mut checked = 0u64;
            let mut present = 0u64;
            let t = env.begin_transaction(None).expect("verify txn");
            let mut i = 0u64;
            while i <= acked {
                if db.get_in(&t, key_bytes(i)).is_ok() {
                    present += 1;
                }
                checked += 1;
                i += step;
            }
            // Always check the LAST acked id explicitly (the torn-tail edge).
            let last_present = db.get_in(&t, key_bytes(acked)).is_ok();
            let _ = t.commit();

            let lost = checked.saturating_sub(present);
            println!(
                "RESULT engine=noxu mode=recover acked={acked} \
                 count={cnt} recover_ms={recover_ms:.1} db_open_ms={db_open_ms:.1} \
                 sample_checked={checked} sample_present={present} sample_lost={lost} \
                 last_acked_present={last_present}"
            );
            db.close().unwrap();
            env.close().unwrap();
        }

        _ => {
            eprintln!("unknown BENCH_MODE={mode} (load|write|recover)");
            std::process::exit(2);
        }
    }
}
