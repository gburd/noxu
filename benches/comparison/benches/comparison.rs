//! Comparative benchmarks: Noxu DB vs LMDB (heed 0.22), sled 0.34, redb 4.

#![allow(clippy::suspicious_map)]
//!
//! # Workloads
//!
//! | Group              | What is measured                                        |
//! |--------------------|----------------------------------------------------------|
//! | `single_put`       | One overwrite on a warm (pre-populated) DB              |
//! | `single_get_hit`   | One lookup of an existing key (warm B-tree / cache)     |
//! | `single_get_miss`  | One lookup of a key that does not exist                 |
//! | `seq_scan_1000`    | Full forward iteration over 1 000 pre-loaded records    |
//! | `bulk_load_1000`   | Insert 1 000 records into a *fresh* DB (iter_batched)   |
//!
//! # Durability notes
//!
//! * **LMDB / redb**: each write transaction commits with an fsync by default.
//!   `single_put` therefore measures fsync latency as much as B-tree cost.
//!   `bulk_load_1000` uses a *single* transaction for all 1 000 inserts and
//!   commits once, which is far cheaper per-record.
//! * **sled**: writes are buffered; `insert()` does not block on fsync.
//! * **Noxu**: txn commit is currently a no-op (WAL writes not yet
//!   implemented); results are artificially fast for write-heavy workloads.

use criterion::{
    BatchSize, Criterion, black_box, criterion_group, criterion_main,
};
use heed::types::Bytes as HeedBytes;
use heed::{Database as HeedDatabase, EnvOpenOptions};
use noxu_db::{
    DatabaseConfig, DatabaseEntry, Environment, EnvironmentConfig, Get,
    OperationStatus,
};
use redb::{
    Database as ReDb, ReadableDatabase, ReadableTable, TableDefinition,
};
use tempfile::TempDir;

// ---------------------------------------------------------------------------
// Shared constants
// ---------------------------------------------------------------------------

const N: usize = 1_000;
/// 64-byte value representative of a small record.
const VALUE: &[u8] =
    b"noxu-comparison-bench-value-0123456789abcdef0123456789abcdefXXXX";
const LMDB_MAP_SIZE: usize = 32 * 1024 * 1024; // 32 MiB
const REDB_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("bench");

fn key(i: usize) -> Vec<u8> {
    format!("{:08}", i).into_bytes()
}

// ---------------------------------------------------------------------------
// Noxu helpers
// ---------------------------------------------------------------------------

fn noxu_open() -> (TempDir, Environment, noxu_db::Database) {
    let dir = TempDir::new().unwrap();
    let cfg = EnvironmentConfig::new(dir.path().to_path_buf())
        .with_allow_create(true);
    let env = Environment::open(cfg).unwrap();
    let db_cfg = DatabaseConfig::new().with_allow_create(true);
    let db = env.open_database(None, "bench", &db_cfg).unwrap();
    (dir, env, db)
}

fn noxu_populate(db: &noxu_db::Database) {
    for i in 0..N {
        let k = DatabaseEntry::from_vec(key(i));
        let v = DatabaseEntry::from_bytes(VALUE);
        db.put(&k, &v).unwrap();
    }
}

// ---------------------------------------------------------------------------
// sled helpers
// ---------------------------------------------------------------------------

fn sled_open() -> (TempDir, sled::Db) {
    let dir = TempDir::new().unwrap();
    let db = sled::open(dir.path()).unwrap();
    (dir, db)
}

fn sled_populate(db: &sled::Db) {
    for i in 0..N {
        db.insert(key(i), VALUE).unwrap();
    }
}

// ---------------------------------------------------------------------------
// LMDB / heed helpers
// ---------------------------------------------------------------------------

type HeedDb = HeedDatabase<HeedBytes, HeedBytes>;

fn lmdb_open() -> (TempDir, heed::Env, HeedDb) {
    let dir = TempDir::new().unwrap();
    let env = unsafe {
        EnvOpenOptions::new().map_size(LMDB_MAP_SIZE).open(dir.path()).unwrap()
    };
    let mut wtxn = env.write_txn().unwrap();
    let db: HeedDb = env.create_database(&mut wtxn, None).unwrap();
    wtxn.commit().unwrap();
    (dir, env, db)
}

fn lmdb_populate(env: &heed::Env, db: &HeedDb) {
    let mut wtxn = env.write_txn().unwrap();
    for i in 0..N {
        db.put(&mut wtxn, key(i).as_slice(), VALUE).unwrap();
    }
    wtxn.commit().unwrap();
}

// ---------------------------------------------------------------------------
// redb helpers
// ---------------------------------------------------------------------------

fn redb_open() -> (TempDir, ReDb) {
    let dir = TempDir::new().unwrap();
    let db = ReDb::create(dir.path().join("bench.redb")).unwrap();
    (dir, db)
}

fn redb_populate(db: &ReDb) {
    let wtxn = db.begin_write().unwrap();
    {
        let mut table = wtxn.open_table(REDB_TABLE).unwrap();
        for i in 0..N {
            table.insert(key(i).as_slice(), VALUE).unwrap();
        }
    }
    wtxn.commit().unwrap();
}

// ---------------------------------------------------------------------------
// single_put — one overwrite on a warm database
//
// LMDB and redb open+commit a write txn per put, which includes an fsync.
// sled buffers writes.  Noxu's txn commit is a no-op today.
// ---------------------------------------------------------------------------

fn bench_single_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_put");

    {
        let (_dir, _env, db) = noxu_open();
        noxu_populate(&db);
        let mut counter = 0usize;
        group.bench_function("noxu", |b| {
            b.iter(|| {
                let k = DatabaseEntry::from_vec(key(counter % N));
                let v = DatabaseEntry::from_bytes(VALUE);
                db.put(&k, &v).unwrap();
                counter += 1;
            });
        });
    }

    {
        let (_dir, db) = sled_open();
        sled_populate(&db);
        let mut counter = 0usize;
        group.bench_function("sled", |b| {
            b.iter(|| {
                black_box(db.insert(key(counter % N), VALUE).unwrap());
                counter += 1;
            });
        });
    }

    {
        let (_dir, env, db) = lmdb_open();
        lmdb_populate(&env, &db);
        let mut counter = 0usize;
        group.bench_function("lmdb", |b| {
            b.iter(|| {
                // One write txn per put — includes fsync.
                let mut wtxn = env.write_txn().unwrap();
                db.put(&mut wtxn, key(counter % N).as_slice(), VALUE).unwrap();
                wtxn.commit().unwrap();
                black_box(());
                counter += 1;
            });
        });
    }

    {
        let (_dir, db) = redb_open();
        redb_populate(&db);
        let mut counter = 0usize;
        group.bench_function("redb", |b| {
            b.iter(|| {
                // One write txn per put — includes fsync.
                let wtxn = db.begin_write().unwrap();
                {
                    let mut table = wtxn.open_table(REDB_TABLE).unwrap();
                    table.insert(key(counter % N).as_slice(), VALUE).unwrap();
                }
                wtxn.commit().unwrap();
                black_box(());
                counter += 1;
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// single_get_hit — lookup an existing key on a warm database
// ---------------------------------------------------------------------------

fn bench_single_get_hit(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_get_hit");

    {
        let (_dir, _env, db) = noxu_open();
        noxu_populate(&db);
        let k = DatabaseEntry::from_vec(key(500));
        group.bench_function("noxu", |b| {
            b.iter(|| {
                let mut data = DatabaseEntry::new();
                black_box(db.get_into(None, &k, &mut data).unwrap());
            });
        });
    }

    {
        let (_dir, db) = sled_open();
        sled_populate(&db);
        let k = key(500);
        group.bench_function("sled", |b| {
            b.iter(|| {
                black_box(db.get(&k).unwrap());
            });
        });
    }

    {
        let (_dir, env, db) = lmdb_open();
        lmdb_populate(&env, &db);
        let k = key(500);
        group.bench_function("lmdb", |b| {
            b.iter(|| {
                let rtxn = env.read_txn().unwrap();
                black_box(db.get(&rtxn, k.as_slice()).unwrap());
            });
        });
    }

    {
        let (_dir, db) = redb_open();
        redb_populate(&db);
        let k = key(500);
        group.bench_function("redb", |b| {
            b.iter(|| {
                let rtxn = db.begin_read().unwrap();
                let table = rtxn.open_table(REDB_TABLE).unwrap();
                black_box(table.get(k.as_slice()).unwrap());
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// single_get_miss — lookup a key that does not exist
// ---------------------------------------------------------------------------

fn bench_single_get_miss(c: &mut Criterion) {
    let mut group = c.benchmark_group("single_get_miss");

    {
        let (_dir, _env, db) = noxu_open();
        noxu_populate(&db);
        let k = DatabaseEntry::from_bytes(b"zzzzz_missing");
        group.bench_function("noxu", |b| {
            b.iter(|| {
                let mut data = DatabaseEntry::new();
                black_box(db.get_into(None, &k, &mut data).unwrap());
            });
        });
    }

    {
        let (_dir, db) = sled_open();
        sled_populate(&db);
        let k = b"zzzzz_missing".to_vec();
        group.bench_function("sled", |b| {
            b.iter(|| {
                black_box(db.get(&k).unwrap());
            });
        });
    }

    {
        let (_dir, env, db) = lmdb_open();
        lmdb_populate(&env, &db);
        let k = b"zzzzz_missing".to_vec();
        group.bench_function("lmdb", |b| {
            b.iter(|| {
                let rtxn = env.read_txn().unwrap();
                black_box(db.get(&rtxn, k.as_slice()).unwrap());
            });
        });
    }

    {
        let (_dir, db) = redb_open();
        redb_populate(&db);
        let k = b"zzzzz_missing".to_vec();
        group.bench_function("redb", |b| {
            b.iter(|| {
                let rtxn = db.begin_read().unwrap();
                let table = rtxn.open_table(REDB_TABLE).unwrap();
                black_box(table.get(k.as_slice()).unwrap());
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// seq_scan_1000 — full forward iteration over 1 000 records
// ---------------------------------------------------------------------------

fn bench_seq_scan_1000(c: &mut Criterion) {
    let mut group = c.benchmark_group("seq_scan_1000");

    {
        let (_dir, _env, db) = noxu_open();
        noxu_populate(&db);
        group.bench_function("noxu", |b| {
            b.iter(|| {
                let mut cursor = db.open_cursor(None).unwrap();
                let mut key_entry = DatabaseEntry::new();
                let mut data = DatabaseEntry::new();
                let mut count = 0u32;
                let mut status = cursor
                    .get(&mut key_entry, &mut data, Get::First, None)
                    .unwrap();
                while status == OperationStatus::Success {
                    count += 1;
                    status = cursor
                        .get(&mut key_entry, &mut data, Get::Next, None)
                        .unwrap();
                }
                cursor.close().unwrap();
                black_box(count)
            });
        });
    }

    {
        let (_dir, db) = sled_open();
        sled_populate(&db);
        group.bench_function("sled", |b| {
            b.iter(|| {
                let count = db.iter().map(|r| r.unwrap()).count();
                black_box(count)
            });
        });
    }

    {
        let (_dir, env, db) = lmdb_open();
        lmdb_populate(&env, &db);
        group.bench_function("lmdb", |b| {
            b.iter(|| {
                let rtxn = env.read_txn().unwrap();
                let count = db.iter(&rtxn).unwrap().map(|r| r.unwrap()).count();
                black_box(count)
            });
        });
    }

    {
        let (_dir, db) = redb_open();
        redb_populate(&db);
        group.bench_function("redb", |b| {
            b.iter(|| {
                let rtxn = db.begin_read().unwrap();
                let table = rtxn.open_table(REDB_TABLE).unwrap();
                let count = table.iter().unwrap().count();
                black_box(count)
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// bulk_load_1000 — insert 1 000 records into a fresh database
//
// Uses iter_batched so the DB open/creation is NOT timed.  LMDB and redb
// use a single write transaction for all 1 000 inserts (one fsync total).
// ---------------------------------------------------------------------------

fn bench_bulk_load_1000(c: &mut Criterion) {
    let mut group = c.benchmark_group("bulk_load_1000");

    let keys: Vec<Vec<u8>> = (0..N).map(key).collect();

    group.bench_function("noxu", |b| {
        b.iter_batched(
            || {
                let dir = TempDir::new().unwrap();
                let cfg = EnvironmentConfig::new(dir.path().to_path_buf())
                    .with_allow_create(true);
                let env = Environment::open(cfg).unwrap();
                let db_cfg = DatabaseConfig::new().with_allow_create(true);
                let db = env.open_database(None, "bench", &db_cfg).unwrap();
                (dir, env, db)
            },
            |(_dir, _env, db)| {
                for k in &keys {
                    let ke = DatabaseEntry::from_vec(k.clone());
                    let ve = DatabaseEntry::from_bytes(VALUE);
                    db.put(&ke, &ve).unwrap();
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("sled", |b| {
        b.iter_batched(
            || {
                let dir = TempDir::new().unwrap();
                let db = sled::open(dir.path()).unwrap();
                (dir, db)
            },
            |(_dir, db)| {
                for k in &keys {
                    black_box(db.insert(k.as_slice(), VALUE).unwrap());
                }
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("lmdb", |b| {
        b.iter_batched(
            || {
                let dir = TempDir::new().unwrap();
                let env = unsafe {
                    EnvOpenOptions::new()
                        .map_size(LMDB_MAP_SIZE)
                        .open(dir.path())
                        .unwrap()
                };
                let mut wtxn = env.write_txn().unwrap();
                let db: HeedDb = env.create_database(&mut wtxn, None).unwrap();
                wtxn.commit().unwrap();
                (dir, env, db)
            },
            |(_dir, env, db)| {
                // All 1 000 inserts in one transaction → one fsync.
                let mut wtxn = env.write_txn().unwrap();
                for k in &keys {
                    db.put(&mut wtxn, k.as_slice(), VALUE).unwrap();
                }
                wtxn.commit().unwrap();
                black_box(());
            },
            BatchSize::SmallInput,
        );
    });

    group.bench_function("redb", |b| {
        b.iter_batched(
            || {
                let dir = TempDir::new().unwrap();
                let db = ReDb::create(dir.path().join("bench.redb")).unwrap();
                (dir, db)
            },
            |(_dir, db)| {
                // All 1 000 inserts in one transaction → one fsync.
                let wtxn = db.begin_write().unwrap();
                {
                    let mut table = wtxn.open_table(REDB_TABLE).unwrap();
                    for k in &keys {
                        table.insert(k.as_slice(), VALUE).unwrap();
                    }
                }
                wtxn.commit().unwrap();
                black_box(());
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

// ---------------------------------------------------------------------------

criterion_group!(
    comparison_benches,
    bench_single_put,
    bench_single_get_hit,
    bench_single_get_miss,
    bench_seq_scan_1000,
    bench_bulk_load_1000,
);
criterion_main!(comparison_benches);
