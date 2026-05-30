/// W10 focused micro-benchmark for wave-11-J baseline/after measurements.
///
/// Usage:
///   cargo run --release -p noxu-w10-bench -- [--nvme] [--scale 1000,10000,100000]
///
/// By default uses tmpfs TempDir.  With --nvme writes to /scratch/noxu_w10bench.
use noxu::{
    Database, DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig,
};
use std::path::Path;
use std::sync::{Arc, Barrier};
use std::time::Instant;

fn open_env(dir: &Path) -> (Environment, Database) {
    let cfg = EnvironmentConfig::new(dir.to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(cfg).unwrap();
    let db = env
        .open_database(
            None,
            "bench",
            &DatabaseConfig::new().with_allow_create(true),
        )
        .unwrap();
    (env, db)
}

fn populate(db: &Database, n: usize) {
    let value = vec![b'x'; 64];
    for i in 0..n {
        let key = DatabaseEntry::from_vec(format!("{:010}", i).into_bytes());
        let val = DatabaseEntry::from_bytes(&value);
        db.put(None, &key, &val).unwrap();
    }
}

fn run_w10(
    db: Arc<Database>,
    readers: usize,
    writers: usize,
    ops_per_thread: usize,
) -> f64 {
    let total = readers + writers;
    let barrier = Arc::new(Barrier::new(total));
    let mut handles = Vec::with_capacity(total);
    let t_start = Arc::new(std::sync::Mutex::new(None::<Instant>));
    let t_end = Arc::new(std::sync::Mutex::new(None::<Instant>));

    for _ in 0..readers {
        let db2 = Arc::clone(&db);
        let b = Arc::clone(&barrier);
        let ts = Arc::clone(&t_start);
        handles.push(std::thread::spawn(move || {
            b.wait();
            *ts.lock().unwrap() = Some(Instant::now());
            let mut data = DatabaseEntry::new();
            let n = ops_per_thread;
            for i in 0..n {
                let k = DatabaseEntry::from_vec(
                    format!("{:010}", i % n).into_bytes(),
                );
                let _ = db2.get(None, &k, &mut data);
            }
            n as u64
        }));
    }

    for wid in 0..writers {
        let db2 = Arc::clone(&db);
        let b = Arc::clone(&barrier);
        let te = Arc::clone(&t_end);
        let ops = ops_per_thread;
        handles.push(std::thread::spawn(move || {
            b.wait();
            let value = vec![b'v'; 64];
            for i in 0..ops {
                let k = DatabaseEntry::from_vec(
                    format!("{:010}", wid * ops + i).into_bytes(),
                );
                let v = DatabaseEntry::from_bytes(&value);
                db2.put(None, &k, &v).unwrap();
            }
            *te.lock().unwrap() = Some(Instant::now());
            ops as u64
        }));
    }

    let t0 = Instant::now();
    let total_ops: u64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
    let elapsed = t0.elapsed().as_secs_f64();
    let ops_per_sec = total_ops as f64 / elapsed;
    println!(
        "  {readers}r/{writers}w  n={ops_per_thread}  total_ops={total_ops}  {:.0} ops/s  {:.1}ms",
        ops_per_sec,
        elapsed * 1000.0
    );
    ops_per_sec
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let nvme = args.iter().any(|a| a == "--nvme");
    let scales: Vec<usize> =
        if let Some(p) = args.iter().position(|a| a == "--scale") {
            args.get(p + 1)
                .map(|s| s.split(',').filter_map(|x| x.parse().ok()).collect())
                .unwrap_or_else(|| vec![1_000, 10_000, 100_000])
        } else {
            vec![1_000, 10_000, 100_000]
        };

    let configs: &[(&str, usize, usize)] = &[("4r/4w", 4, 4), ("8r/8w", 8, 8)];

    let storage = if nvme { "NVMe (/scratch)" } else { "tmpfs" };
    println!("W10 baseline  storage={storage}");
    println!("{:-<60}", "");

    for &scale in &scales {
        println!("Scale: {scale}");
        for &(label, r, w) in configs {
            let total = r + w;
            let ops = scale / total.max(1);

            let dir;
            let tmpdir;
            let path: &Path = if nvme {
                let d = std::path::PathBuf::from(format!(
                    "/scratch/noxu_w10bench_{}_{}",
                    scale,
                    label.replace('/', "_")
                ));
                let _ = std::fs::remove_dir_all(&d);
                std::fs::create_dir_all(&d).unwrap();
                dir = d;
                &dir
            } else {
                tmpdir = tempfile::TempDir::new().unwrap();
                tmpdir.path()
            };

            let (env, db) = open_env(path);
            populate(&db, scale);
            let db_arc = Arc::new(db);
            print!("  {label}  ");
            let _ = run_w10(Arc::clone(&db_arc), r, w, ops);
            drop(db_arc);
            drop(env);
        }
    }
}
