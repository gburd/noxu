//! Benchmarks for the noxu-db public API: Environment, Database, Cursor, Transaction.

use criterion::{Criterion, black_box, criterion_group, criterion_main};
use tempfile::TempDir;

use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, Get,
    OperationStatus, Transaction, TransactionConfig,
};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Creates a transactional environment in a fresh temp directory.
fn open_env() -> (TempDir, Environment) {
    let dir = TempDir::new().expect("temp dir");
    let config = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true)
        .with_transactional(true);
    let env = Environment::open(config).expect("open env");
    (dir, env)
}

// ---------------------------------------------------------------------------
// Environment benchmarks
// ---------------------------------------------------------------------------

fn bench_env_open_close(c: &mut Criterion) {
    c.bench_function("env_open_close", |b| {
        b.iter(|| {
            let dir = TempDir::new().unwrap();
            let config = EnvironmentConfig::new(dir.path().to_path_buf())
                .with_allow_create(true)
                .with_transactional(true);
            let env = Environment::open(config).unwrap();
            env.close().unwrap();
            black_box(());
        })
    });
}

// ---------------------------------------------------------------------------
// Database put / get / delete
// ---------------------------------------------------------------------------

fn bench_db_put(c: &mut Criterion) {
    let (_dir, env) = open_env();
    let db_config = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, "bench_put", &db_config).unwrap();

    let key = DatabaseEntry::from_bytes(b"bench_key");
    let value = DatabaseEntry::from_bytes(b"bench_value_0123456789abcdef");

    c.bench_function("db_put", |b| {
        b.iter(|| {
            black_box(db.put(None, &key, &value).unwrap());
        })
    });
}

fn bench_db_get_hit(c: &mut Criterion) {
    let (_dir, env) = open_env();
    let db_config = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, "bench_get_hit", &db_config).unwrap();

    let key = DatabaseEntry::from_bytes(b"bench_key");
    let value = DatabaseEntry::from_bytes(b"bench_value_0123456789abcdef");
    db.put(None, &key, &value).unwrap();

    c.bench_function("db_get_hit", |b| {
        let mut data = DatabaseEntry::new();
        b.iter(|| {
            let status = db.get(None, &key, &mut data).unwrap();
            black_box(status);
        })
    });
}

fn bench_db_get_miss(c: &mut Criterion) {
    let (_dir, env) = open_env();
    let db_config = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, "bench_get_miss", &db_config).unwrap();

    let key = DatabaseEntry::from_bytes(b"nonexistent_key");

    c.bench_function("db_get_miss", |b| {
        let mut data = DatabaseEntry::new();
        b.iter(|| {
            let status = db.get(None, &key, &mut data).unwrap();
            black_box(status);
        })
    });
}

fn bench_db_delete(c: &mut Criterion) {
    let (_dir, env) = open_env();
    let db_config = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, "bench_delete", &db_config).unwrap();

    let key = DatabaseEntry::from_bytes(b"bench_key");
    let value = DatabaseEntry::from_bytes(b"bench_value");

    c.bench_function("db_delete", |b| {
        b.iter(|| {
            db.put(None, &key, &value).unwrap();
            let status = db.delete(None, &key).unwrap();
            black_box(status);
        })
    });
}

// ---------------------------------------------------------------------------
// Cursor forward scan
// ---------------------------------------------------------------------------

fn bench_cursor_forward_scan_1000(c: &mut Criterion) {
    let (_dir, env) = open_env();
    let db_config = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, "bench_cursor_scan", &db_config).unwrap();

    // Insert 1000 records with sorted keys.
    for i in 0..1000u32 {
        let key = DatabaseEntry::from_vec(format!("key_{:06}", i).into_bytes());
        let val =
            DatabaseEntry::from_vec(format!("value_{:06}", i).into_bytes());
        db.put(None, &key, &val).unwrap();
    }

    c.bench_function("cursor_forward_scan_1000", |b| {
        b.iter(|| {
            let mut cursor = db.open_cursor(None, None).unwrap();
            let mut key = DatabaseEntry::new();
            let mut data = DatabaseEntry::new();
            let mut count = 0u32;

            let mut status =
                cursor.get(&mut key, &mut data, Get::First, None).unwrap();
            while status == OperationStatus::Success {
                count += 1;
                status =
                    cursor.get(&mut key, &mut data, Get::Next, None).unwrap();
            }
            cursor.close().unwrap();
            black_box(count);
        })
    });
}

// ---------------------------------------------------------------------------
// Transaction begin / commit / abort
// ---------------------------------------------------------------------------

fn bench_txn_commit(c: &mut Criterion) {
    c.bench_function("txn_commit", |b| {
        b.iter(|| {
            let txn =
                Transaction::new(black_box(1), TransactionConfig::default());
            txn.commit().unwrap();
            black_box(());
        })
    });
}

fn bench_txn_abort(c: &mut Criterion) {
    c.bench_function("txn_abort", |b| {
        b.iter(|| {
            let txn =
                Transaction::new(black_box(1), TransactionConfig::default());
            txn.abort().unwrap();
            black_box(());
        })
    });
}

fn bench_txn_begin_via_env(c: &mut Criterion) {
    let (_dir, env) = open_env();

    // Note: transactions accumulate in env's internal map since
    // commit/abort don't call back to the environment. This is fine
    // for a benchmark -- we measure the begin_transaction overhead.
    c.bench_function("txn_begin_via_env", |b| {
        b.iter(|| {
            let txn = env.begin_transaction(None).unwrap();
            txn.commit().unwrap();
            black_box(txn.get_id());
        })
    });
}

// ---------------------------------------------------------------------------
// DatabaseEntry construction
// ---------------------------------------------------------------------------

fn bench_database_entry_from_bytes(c: &mut Criterion) {
    let data = b"0123456789abcdef0123456789abcdef";
    c.bench_function("database_entry_from_bytes_32B", |b| {
        b.iter(|| black_box(DatabaseEntry::from_bytes(black_box(data))))
    });
}

fn bench_database_entry_get_data(c: &mut Criterion) {
    let entry = DatabaseEntry::from_bytes(b"0123456789abcdef");
    c.bench_function("database_entry_get_data", |b| {
        b.iter(|| black_box(entry.get_data()))
    });
}

// ---------------------------------------------------------------------------
// Groups
// ---------------------------------------------------------------------------

criterion_group!(env_benches, bench_env_open_close,);

criterion_group!(
    db_benches,
    bench_db_put,
    bench_db_get_hit,
    bench_db_get_miss,
    bench_db_delete,
);

criterion_group!(cursor_benches, bench_cursor_forward_scan_1000);

criterion_group!(
    txn_benches,
    bench_txn_commit,
    bench_txn_abort,
    bench_txn_begin_via_env,
);

criterion_group!(
    entry_benches,
    bench_database_entry_from_bytes,
    bench_database_entry_get_data,
);

criterion_main!(
    env_benches,
    db_benches,
    cursor_benches,
    txn_benches,
    entry_benches
);
