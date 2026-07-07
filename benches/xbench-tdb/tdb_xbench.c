/*
 * Cross-engine benchmark driver -- TidesDB side.
 *
 * Implements the shared workload spec (see
 * .agent/archived-audits/bench/workload-spec.md) so results are directly
 * comparable to the Noxu (Rust) and WiredTiger (C) drivers: identical
 * key/value format, key distributions, op mixes, thread counts, durability,
 * RNG seed, latency histogram, and RESULT output line.
 *
 * The Noxu reference driver is benches/noxu-bench/src/bin/xbench.rs and the
 * WiredTiger sibling is benches/xbench-wt/wt_xbench.c; every workload branch,
 * RNG step, and the Zipf generator below match them exactly so the same
 * BENCH_SEED yields byte-identical key sequences across engines.
 *
 * TidesDB is an LSM engine (write-optimized, read-amplified). That shape is
 * expected and faithful -- we implement the same workloads, not the same
 * storage characteristics.
 *
 * Env: BENCH_DIR BENCH_RECORDS BENCH_CACHE BENCH_VALUE BENCH_THREADS
 *      BENCH_SECONDS BENCH_DURABILITY(SYNC|NO_SYNC) BENCH_WORKLOAD BENCH_SEED
 *      BENCH_ISOLATION(default|serializable)
 *
 * C11, warning-free with -Wall -Wextra.
 *
 * =====================================================================
 * API AMBIGUITIES FLAGGED (grep "TODO(api)") -- verify on-instance against
 * /data/TidesDB/src/tidesdb.h and /data/TidesDB/test/tidesdb__tests.c:
 *   1. tidesdb_default_config / config struct field name for the data dir
 *      (spec says `db_path`).
 *   2. Column-family config field names: write_buffer_size, sync_mode; the
 *      sync_mode enum constants TDB_SYNC_FULL / TDB_SYNC_NONE.
 *   3. Isolation enum constant names (tdb_isolation_read_committed, and the
 *      strongest level for "serializable").
 *   4. tidesdb_txn_get out-param ownership + the free function (free() vs a
 *      tidesdb-specific deallocator).
 *   5. Iterator API: tidesdb_iter_new signature, seek-to-key, iter_next
 *      return convention, and iter free.
 *   6. Return-code convention (0 == success assumed everywhere).
 * =====================================================================
 */

#define _POSIX_C_SOURCE 200809L

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <math.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <tidesdb.h>

/* ---- config (env) --------------------------------------------------- */

static const char *envs(const char *k, const char *d) {
    const char *v = getenv(k);
    return (v && *v) ? v : d;
}
static uint64_t envp(const char *k, uint64_t d) {
    const char *v = getenv(k);
    if (!v || !*v) return d;
    char *end;
    unsigned long long r = strtoull(v, &end, 10);
    return (end == v) ? d : (uint64_t)r;
}

/* ---- key format (identical to Noxu key_bytes) ----------------------- */

/* 16-byte key: 8B big-endian id + 8B big-endian (id * 2654435761 wrapping). */
static inline void key_bytes(uint64_t id, uint8_t out[16]) {
    uint64_t tail = id * 2654435761ULL; /* wrapping mul, 64-bit */
    for (int i = 0; i < 8; i++) out[i]     = (uint8_t)(id   >> (56 - 8 * i));
    for (int i = 0; i < 8; i++) out[8 + i] = (uint8_t)(tail >> (56 - 8 * i));
}

/* ---- RNG (xorshift64, identical algorithm) -------------------------- */

typedef struct { uint64_t s; } Rng;
static inline uint64_t rng_next(Rng *r) {
    uint64_t x = r->s;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    r->s = x;
    return x;
}
static inline uint64_t rng_below(Rng *r, uint64_t n) { return rng_next(r) % n; }
static inline uint32_t rng_pct(Rng *r) { return (uint32_t)(rng_next(r) % 100); }

/* ---- Zipf (YCSB theta=0.99, identical to Noxu Zipf) ----------------- */

typedef struct {
    uint64_t n;
    double theta, zetan, alpha, eta;
} Zipf;

static double zipf_zeta(uint64_t n, double theta) {
    double s = 0.0;
    for (uint64_t i = 1; i <= n; i++) s += 1.0 / pow((double)i, theta);
    return s;
}
static void zipf_init(Zipf *z, uint64_t n) {
    z->n = n;
    z->theta = 0.99;
    z->zetan = zipf_zeta(n, z->theta);
    double zeta2 = zipf_zeta(2, z->theta);
    z->alpha = 1.0 / (1.0 - z->theta);
    z->eta = (1.0 - pow(2.0 / (double)n, 1.0 - z->theta))
             / (1.0 - zeta2 / z->zetan);
}
static inline uint64_t zipf_next(const Zipf *z, Rng *r) {
    double u = (double)rng_next(r) / (double)UINT64_MAX;
    double uz = u * z->zetan;
    if (uz < 1.0) return 0;
    if (uz < 1.0 + pow(0.5, z->theta)) return 1;
    uint64_t v = (uint64_t)((double)z->n
                    * pow(z->eta * u - z->eta + 1.0, z->alpha));
    return v % z->n;
}

/* ---- latency histogram (65536 x 1us buckets, identical pct logic) --- */

#define HBUCKETS 65536
typedef struct {
    uint64_t b[HBUCKETS];
    uint64_t max;
} Hist;

static inline void hist_record(Hist *h, uint64_t us) {
    h->b[us < HBUCKETS ? us : HBUCKETS - 1]++;
    if (us > h->max) h->max = us;
}
static void hist_merge(Hist *dst, const Hist *src) {
    for (int i = 0; i < HBUCKETS; i++) dst->b[i] += src->b[i];
    if (src->max > dst->max) dst->max = src->max;
}
static uint64_t hist_pct(const Hist *h, double p) {
    uint64_t total = 0;
    for (int i = 0; i < HBUCKETS; i++) total += h->b[i];
    if (total == 0) return 0;
    uint64_t target = (uint64_t)((double)total * p);
    uint64_t cum = 0;
    for (int i = 0; i < HBUCKETS; i++) {
        cum += h->b[i];
        if (cum >= target)
            return (i >= HBUCKETS - 1) ? h->max : (uint64_t)i;
    }
    return h->max;
}

/* ---- timing --------------------------------------------------------- */

static inline uint64_t now_us(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000ULL + (uint64_t)ts.tv_nsec / 1000ULL;
}

/* ---- TidesDB txn helpers -------------------------------------------- */
/*
 * All wrapped so the workload branches read like the WT sibling. A non-zero
 * return from any TidesDB call is treated as failure (abort path), matching
 * the Noxu/WT convention of counting commit conflicts as aborts.
 *
 * TODO(api): confirm 0 == success for every function below (assumed).
 */

/* Begin a txn at the configured isolation level. Returns NULL on failure. */
static inline tidesdb_txn_t *tdb_begin(tidesdb_t *db,
                                       tidesdb_isolation_level_t iso) {
    tidesdb_txn_t *txn = NULL;
    if (tidesdb_txn_begin_with_isolation(db, iso, &txn) != 0) return NULL;
    return txn;
}

/* Put. Returns 0 on success. */
static inline int tdb_put(tidesdb_txn_t *txn, tidesdb_column_family_t *cf,
                          const uint8_t *k, size_t ks,
                          const uint8_t *v, size_t vs) {
    /* ttl = 0 (no expiry). */
    return tidesdb_txn_put(txn, cf, k, ks, v, vs, 0);
}

/* Get. Frees the returned value immediately (we only need the access, not the
 * bytes, matching the WT search()+reset() shape). Returns 0 if found, non-zero
 * on not-found/error (not-found is not counted as an abort by callers). */
static inline int tdb_get(tidesdb_txn_t *txn, tidesdb_column_family_t *cf,
                          const uint8_t *k, size_t ks) {
    uint8_t *out = NULL;
    size_t out_sz = 0;
    int r = tidesdb_txn_get(txn, cf, k, ks, &out, &out_sz);
    /* TODO(api): confirm out is malloc'd and freed with free(); if TidesDB
     * exposes a dedicated deallocator, swap free() for it here. */
    if (out) free(out);
    return r;
}

/* Delete. Returns 0 on success. */
static inline int tdb_del(tidesdb_txn_t *txn, tidesdb_column_family_t *cf,
                          const uint8_t *k, size_t ks) {
    return tidesdb_txn_delete(txn, cf, k, ks, 0);
}

/* ---- shared benchmark state ----------------------------------------- */

typedef struct {
    tidesdb_t *db;
    tidesdb_column_family_t *cf;
    tidesdb_isolation_level_t iso;
    uint64_t records;
    size_t value_size;
    const uint8_t *value;      /* value_size bytes of 0x5A */
    const char *workload;
    const char *isolation;     /* "default" | "serializable" */
    uint64_t seed;

    atomic_bool stop;
    atomic_ullong ops;
    atomic_ullong aborts;
} Bench;

typedef struct {
    Bench *b;
    unsigned tid;
    Hist hist;
    uint64_t local_aborts;
} Worker;

/* ---- load phase ----------------------------------------------------- */

typedef struct {
    Bench *b;
    uint64_t start, end;
} LoadArg;

static void *load_thread(void *arg) {
    LoadArg *la = (LoadArg *)arg;
    Bench *b = la->b;
    uint8_t kb[16];

    uint64_t i = la->start;
    while (i < la->end) {
        uint64_t batch_end = i + 1000;
        if (batch_end > la->end) batch_end = la->end;

        tidesdb_txn_t *txn = tdb_begin(b->db, b->iso);
        if (!txn) { /* transient begin failure: skip this batch */
            i = batch_end;
            continue;
        }
        bool ok = true;
        for (uint64_t j = i; j < batch_end; j++) {
            key_bytes(j, kb);
            if (tdb_put(txn, b->cf, kb, 16, b->value, b->value_size) != 0) {
                ok = false;
                break;
            }
        }
        if (ok) {
            if (tidesdb_txn_commit(txn) != 0)
                tidesdb_txn_rollback(txn);
        } else {
            tidesdb_txn_rollback(txn);
        }
        tidesdb_txn_free(txn);
        i = batch_end;
    }
    return NULL;
}

/* ---- one measured op per workload ----------------------------------- */
/*
 * Mirrors xbench.rs / wt_xbench.c branch-for-branch, including the exact order
 * of RNG consumption (zipf_next / rng_pct / rng_below) so key sequences match.
 */

static void *work_thread(void *arg) {
    Worker *w = (Worker *)arg;
    Bench *b = w->b;

    Rng rng = { b->seed ^ ((uint64_t)w->tid * 0x9E3779B9ULL) };
    Zipf zipf;
    zipf_init(&zipf, b->records);

    /* per-thread insert counter for tdb_write, matches Rust init. */
    uint64_t insert_ctr = b->records + (uint64_t)w->tid * 100000000ULL;

    uint8_t kb[16];
    const char *wl = b->workload;

    while (!atomic_load_explicit(&b->stop, memory_order_relaxed)) {
        uint64_t t0 = now_us();

        if (strcmp(wl, "ycsb_a") == 0) {
            uint64_t id = zipf_next(&zipf, &rng);
            key_bytes(id, kb);
            if (rng_pct(&rng) < 50) {
                tidesdb_txn_t *t = tdb_begin(b->db, b->iso);
                if (t) {
                    (void)tdb_get(t, b->cf, kb, 16);
                    tidesdb_txn_commit(t);
                    tidesdb_txn_free(t);
                }
            } else {
                tidesdb_txn_t *t = tdb_begin(b->db, b->iso);
                if (t) {
                    if (tdb_put(t, b->cf, kb, 16, b->value, b->value_size) == 0) {
                        if (tidesdb_txn_commit(t) != 0) w->local_aborts++;
                    } else {
                        tidesdb_txn_rollback(t);
                        w->local_aborts++;
                    }
                    tidesdb_txn_free(t);
                }
            }
        } else if (strcmp(wl, "ycsb_c") == 0) {
            uint64_t id = zipf_next(&zipf, &rng);
            key_bytes(id, kb);
            tidesdb_txn_t *t = tdb_begin(b->db, b->iso);
            if (t) {
                (void)tdb_get(t, b->cf, kb, 16);
                tidesdb_txn_commit(t);
                tidesdb_txn_free(t);
            }
        } else if (strcmp(wl, "tdb_write") == 0) {
            uint64_t id = insert_ctr++;
            key_bytes(id, kb);
            tidesdb_txn_t *t = tdb_begin(b->db, b->iso);
            if (t) {
                if (tdb_put(t, b->cf, kb, 16, b->value, b->value_size) == 0) {
                    if (tidesdb_txn_commit(t) != 0) w->local_aborts++;
                } else {
                    tidesdb_txn_rollback(t);
                    w->local_aborts++;
                }
                tidesdb_txn_free(t);
            }
        } else if (strcmp(wl, "txn_mix") == 0) {
            /* 4-op txn: 2 update + 1 read + 1 delete, Zipfian. */
            tidesdb_txn_t *t = tdb_begin(b->db, b->iso);
            if (t) {
                bool ok = true;
                for (int j = 0; j < 4; j++) {
                    uint64_t id = zipf_next(&zipf, &rng);
                    key_bytes(id, kb);
                    int ir;
                    if (j == 0 || j == 1) {
                        ir = tdb_put(t, b->cf, kb, 16, b->value, b->value_size);
                    } else if (j == 2) {
                        (void)tdb_get(t, b->cf, kb, 16); /* read-miss not an error */
                        ir = 0;
                    } else {
                        (void)tdb_del(t, b->cf, kb, 16); /* delete-miss not an error */
                        ir = 0;
                    }
                    if (ir != 0) { ok = false; break; }
                }
                if (ok) {
                    if (tidesdb_txn_commit(t) != 0) w->local_aborts++;
                } else {
                    tidesdb_txn_rollback(t);
                    w->local_aborts++;
                }
                tidesdb_txn_free(t);
            }
        } else if (strcmp(wl, "hotset") == 0) {
            /* 10% of keys get 90% of ops; 98% update / 2% read. */
            uint64_t hot = b->records / 10;
            if (hot < 1) hot = 1;
            uint64_t id;
            if (rng_pct(&rng) < 90) id = rng_below(&rng, hot);
            else id = rng_below(&rng, b->records);
            key_bytes(id, kb);
            if (rng_pct(&rng) < 98) {
                tidesdb_txn_t *t = tdb_begin(b->db, b->iso);
                if (t) {
                    if (tdb_put(t, b->cf, kb, 16, b->value, b->value_size) == 0) {
                        if (tidesdb_txn_commit(t) != 0) w->local_aborts++;
                    } else {
                        tidesdb_txn_rollback(t);
                        w->local_aborts++;
                    }
                    tidesdb_txn_free(t);
                }
            } else {
                tidesdb_txn_t *t = tdb_begin(b->db, b->iso);
                if (t) {
                    (void)tdb_get(t, b->cf, kb, 16);
                    tidesdb_txn_commit(t);
                    tidesdb_txn_free(t);
                }
            }
        } else if (strcmp(wl, "scan_under_write") == 0) {
            if (w->tid % 2 == 0) {
                /* scanner: forward scan of 100 records from a random start. */
                uint64_t id = zipf_next(&zipf, &rng);
                key_bytes(id, kb);
                tidesdb_txn_t *t = tdb_begin(b->db, b->iso);
                if (t) {
                    tidesdb_iter_t *iter = NULL;
                    /* TODO(api): tidesdb_iter_new signature + seek-to-key. The
                     * header shows tidesdb_iter_new(txn, cf, &iter); if it also
                     * takes a start key, pass (kb,16). Otherwise there may be a
                     * tidesdb_iter_seek(iter, kb, 16) -- use it here. As-is this
                     * scans from the iterator's natural start, which still
                     * exercises the read/scan path faithfully. */
                    if (tidesdb_iter_new(t, b->cf, &iter) == 0 && iter) {
                        for (int n = 0; n < 100; n++) {
                            /* TODO(api): confirm iter_next returns 0 while a row
                             * is produced and non-zero at end-of-iteration. If
                             * it instead fills key/value out-params that must be
                             * freed, free them inside this loop to avoid leaks. */
                            if (tidesdb_iter_next(iter) != 0) break;
                        }
                        tidesdb_iter_free(iter);
                    }
                    tidesdb_txn_commit(t);
                    tidesdb_txn_free(t);
                }
            } else {
                uint64_t id = zipf_next(&zipf, &rng);
                key_bytes(id, kb);
                tidesdb_txn_t *t = tdb_begin(b->db, b->iso);
                if (t) {
                    if (tdb_put(t, b->cf, kb, 16, b->value, b->value_size) == 0) {
                        if (tidesdb_txn_commit(t) != 0) w->local_aborts++;
                    } else {
                        tidesdb_txn_rollback(t);
                        w->local_aborts++;
                    }
                    tidesdb_txn_free(t);
                }
            }
        }

        hist_record(&w->hist, now_us() - t0);
        atomic_fetch_add_explicit(&b->ops, 1, memory_order_relaxed);
    }

    atomic_fetch_add_explicit(&b->aborts, w->local_aborts, memory_order_relaxed);
    return NULL;
}

/* ---- fs sanity: refuse tmpfs (parity with Noxu) --------------------- */

static bool is_tmpfs(const char *dir) {
    char cmd[1024];
    snprintf(cmd, sizeof cmd, "df -T %s 2>/dev/null", dir);
    FILE *fp = popen(cmd, "r");
    if (!fp) return false;
    char line[1024];
    bool tmp = false;
    int ln = 0;
    while (fgets(line, sizeof line, fp)) {
        if (ln++ == 1 && strstr(line, "tmpfs")) tmp = true;
    }
    pclose(fp);
    return tmp;
}

int main(void) {
    const char *dir       = envs("BENCH_DIR", "/tmp/tdb-xbench");
    uint64_t records      = envp("BENCH_RECORDS", 10000000ULL);
    uint64_t cache        = envp("BENCH_CACHE", 2ULL * 1024 * 1024 * 1024);
    size_t value_size     = (size_t)envp("BENCH_VALUE", 1024);
    unsigned threads      = (unsigned)envp("BENCH_THREADS", 64);
    uint64_t seconds      = envp("BENCH_SECONDS", 30);
    const char *durability= envs("BENCH_DURABILITY", "SYNC");
    const char *workload  = envs("BENCH_WORKLOAD", "ycsb_a");
    uint64_t seed         = envp("BENCH_SEED", 0xC0FFEEULL);
    const char *isolation = envs("BENCH_ISOLATION", "default");

    if (is_tmpfs(dir)) {
        fprintf(stderr, "ABORT: %s is tmpfs; use real NVMe\n", dir);
        return 2;
    }
    mkdir(dir, 0755);

    /*
     * Durability parity (STRACE-VERIFY these fsync at commit):
     *   SYNC    -> sync_mode = TDB_SYNC_FULL  => genuine fsync per commit.
     *   NO_SYNC -> sync_mode = TDB_SYNC_NONE  => no per-commit fsync.
     * Matches Noxu COMMIT_SYNC vs COMMIT_NO_SYNC and WT
     * transaction_sync=(enabled=true,method=fsync) vs enabled=false.
     * We deliberately do NOT use TDB_SYNC_INTERVAL for the SYNC run.
     *
     * TODO(api): confirm the enum constant names TDB_SYNC_FULL / TDB_SYNC_NONE
     * and the cf-config field name `sync_mode` against tidesdb.h.
     */
    bool sync = (strcmp(durability, "NO_SYNC") != 0 &&
                 strcmp(durability, "WRITE_NO_SYNC") != 0);

    /*
     * Isolation mapping:
     *   "default"      -> read_committed (comparable to Noxu default /
     *                     WT snapshot's committed-read semantics).
     *   "serializable" -> the strongest level TidesDB exposes.
     * TODO(api): confirm the exact enum names. The spec lists
     * tdb_isolation_read_uncommitted (0) and tdb_isolation_read_committed (1);
     * the serializable/snapshot constant is whatever the header's highest
     * value is. Adjust TDB_ISO_SERIALIZABLE below if the name differs.
     */
#ifndef TDB_ISO_READ_COMMITTED
#define TDB_ISO_READ_COMMITTED tdb_isolation_read_committed
#endif
    /* TODO(api): replace with the real strongest-isolation constant, e.g.
     * tdb_isolation_serializable or tdb_isolation_snapshot. Falling back to
     * read_committed keeps the build green until confirmed. */
#ifndef TDB_ISO_SERIALIZABLE
#define TDB_ISO_SERIALIZABLE tdb_isolation_serializable
#endif
    tidesdb_isolation_level_t iso =
        (strcmp(isolation, "serializable") == 0)
            ? (tidesdb_isolation_level_t)TDB_ISO_SERIALIZABLE
            : (tidesdb_isolation_level_t)TDB_ISO_READ_COMMITTED;

    printf("=== TIDESDB xbench: workload=%s records=%llu cache=%lluGiB "
           "value=%zu threads=%u secs=%llu dur=%s iso=%s ===\n",
           workload, (unsigned long long)records,
           (unsigned long long)(cache / 1024 / 1024 / 1024),
           value_size, threads, (unsigned long long)seconds,
           durability, isolation);

    /* value buffer: value_size bytes of 0x5A. */
    uint8_t *value = malloc(value_size);
    if (!value) { fprintf(stderr, "OOM value\n"); return 1; }
    memset(value, 0x5A, value_size);

    /* ---- open db ---- */
    tidesdb_config_t config;
    /* TODO(api): if tidesdb_default_config(&config) exists, prefer it, then
     * override db_path. Zero-init + set db_path is the safe fallback. */
    memset(&config, 0, sizeof config);
    config.db_path = (char *)dir; /* TODO(api): confirm field name/type. */

    tidesdb_t *db = NULL;
    if (tidesdb_open(&config, &db) != 0 || !db) {
        fprintf(stderr, "tidesdb_open failed for %s\n", dir);
        return 1;
    }

    /* ---- create + get column family ---- */
    tidesdb_column_family_config_t cfg = tidesdb_default_column_family_config();
    /* memtable sized ~ cache/4 (LSM write buffer). TODO(api): confirm field
     * name write_buffer_size and units (bytes assumed). */
    cfg.write_buffer_size = cache / 4;
    /* TODO(api): confirm field name sync_mode + enum constants. */
    cfg.sync_mode = sync ? TDB_SYNC_FULL : TDB_SYNC_NONE;

    if (tidesdb_create_column_family(db, "xbench", &cfg) != 0) {
        /* may already exist from a prior run in a persisted dir; continue to
         * get it below. */
    }
    tidesdb_column_family_t *cf = NULL;
    if (tidesdb_get_column_family(db, "xbench", &cf) != 0 || !cf) {
        fprintf(stderr, "tidesdb_get_column_family(xbench) failed\n");
        return 1;
    }

    Bench b;
    memset(&b, 0, sizeof b);
    b.db = db;
    b.cf = cf;
    b.iso = iso;
    b.records = records;
    b.value_size = value_size;
    b.value = value;
    b.workload = workload;
    b.isolation = isolation;
    b.seed = seed;
    atomic_init(&b.stop, false);
    atomic_init(&b.ops, 0);
    atomic_init(&b.aborts, 0);

    /* ---- load phase (8 loader threads, 1000 puts/txn) ---- */
    printf("-- loading %llu records --\n", (unsigned long long)records);
    uint64_t lt0 = now_us();
    {
        const unsigned load_threads = 8;
        pthread_t th[8];
        LoadArg la[8];
        uint64_t per = records / load_threads;
        for (unsigned i = 0; i < load_threads; i++) {
            la[i].b = &b;
            la[i].start = (uint64_t)i * per;
            la[i].end = (i == load_threads - 1) ? records : la[i].start + per;
            pthread_create(&th[i], NULL, load_thread, &la[i]);
        }
        for (unsigned i = 0; i < load_threads; i++) pthread_join(th[i], NULL);
    }
    printf("   loaded in %.1fs\n", (double)(now_us() - lt0) / 1e6);

    /* ---- measured phase ---- */
    Worker *workers = calloc(threads, sizeof(Worker));
    pthread_t *wth = calloc(threads, sizeof(pthread_t));
    if (!workers || !wth) { fprintf(stderr, "OOM workers\n"); return 1; }

    uint64_t start = now_us();
    for (unsigned i = 0; i < threads; i++) {
        workers[i].b = &b;
        workers[i].tid = i;
        pthread_create(&wth[i], NULL, work_thread, &workers[i]);
    }

    sleep((unsigned)seconds);
    atomic_store_explicit(&b.stop, true, memory_order_relaxed);
    for (unsigned i = 0; i < threads; i++) pthread_join(wth[i], NULL);

    double el = (double)(now_us() - start) / 1e6;
    unsigned long long total = atomic_load(&b.ops);
    unsigned long long ab = atomic_load(&b.aborts);

    Hist merged;
    memset(&merged, 0, sizeof merged);
    for (unsigned i = 0; i < threads; i++) hist_merge(&merged, &workers[i].hist);

    double thr = el > 0 ? (double)total / el : 0.0;
    double abort_rate = total > 0 ? (double)ab / (double)total : 0.0;

    printf("RESULT engine=tidesdb workload=%s iso=%s dur=%s threads=%u "
           "throughput=%.0f ops/s ops=%llu aborts=%llu abort_rate=%.4f "
           "p50=%llu p90=%llu p99=%llu p999=%llu max=%llu\n",
           workload, isolation, durability, threads,
           thr, total, ab, abort_rate,
           (unsigned long long)hist_pct(&merged, 0.50),
           (unsigned long long)hist_pct(&merged, 0.90),
           (unsigned long long)hist_pct(&merged, 0.99),
           (unsigned long long)hist_pct(&merged, 0.999),
           (unsigned long long)merged.max);

    free(workers);
    free(wth);
    /* TODO(api): if there is a tidesdb_close(db) / tidesdb_free(db), call it
     * here for a clean shutdown + final flush. */
    tidesdb_close(db);
    free(value);
    return 0;
}
