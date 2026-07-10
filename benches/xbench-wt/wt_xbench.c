/*
 * Cross-engine benchmark driver -- WiredTiger side.
 *
 * Implements the shared workload spec (see
 * .agent/archived-audits/bench/workload-spec.md) so results are directly
 * comparable to the Noxu (Rust) and TidesDB drivers: identical key/value
 * format, key distributions, op mixes, thread counts, durability, RNG seed,
 * latency histogram, and RESULT output line.
 *
 * The Noxu reference driver is benches/noxu-bench/src/bin/xbench.rs; every
 * workload branch, RNG step, and the Zipf generator below match it exactly so
 * the same BENCH_SEED yields byte-identical key sequences across engines.
 *
 * Env: BENCH_DIR BENCH_RECORDS BENCH_CACHE BENCH_VALUE BENCH_THREADS
 *      BENCH_SECONDS BENCH_DURABILITY(SYNC|NO_SYNC) BENCH_WORKLOAD BENCH_SEED
 *      BENCH_ISOLATION(default|serializable)
 *
 * C11, warning-free with -Wall -Wextra.
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

#include <wiredtiger.h>

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

/* ---- shared benchmark state ----------------------------------------- */

typedef struct {
    WT_CONNECTION *conn;
    uint64_t records;
    size_t value_size;
    const uint8_t *value;      /* value_size bytes of 0x5A */
    const char *workload;
    const char *isolation;     /* "default" | "serializable" */
    const char *begin_cfg;     /* WT begin_transaction config (isolation) */
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

/* WT_ROLLBACK -> abort; anything else -> fatal for setup, treated per-op. */
static inline bool is_rollback(int r) { return r == WT_ROLLBACK; }

/* ---- load phase ----------------------------------------------------- */

typedef struct {
    Bench *b;
    uint64_t start, end;
} LoadArg;

static void *load_thread(void *arg) {
    LoadArg *la = (LoadArg *)arg;
    Bench *b = la->b;
    WT_SESSION *sess;
    int r = b->conn->open_session(b->conn, NULL, NULL, &sess);
    if (r != 0) { fprintf(stderr, "load open_session: %s\n", wiredtiger_strerror(r)); exit(1); }
    WT_CURSOR *cur;
    r = sess->open_cursor(sess, "table:xbench", NULL, NULL, &cur);
    if (r != 0) { fprintf(stderr, "load open_cursor: %s\n", wiredtiger_strerror(r)); exit(1); }

    uint8_t kb[16];
    WT_ITEM ki, vi;
    memset(&ki, 0, sizeof ki);
    memset(&vi, 0, sizeof vi);
    ki.data = kb; ki.size = 16;
    vi.data = b->value; vi.size = b->value_size;

    uint64_t i = la->start;
    while (i < la->end) {
        uint64_t batch_end = i + 1000;
        if (batch_end > la->end) batch_end = la->end;
        r = sess->begin_transaction(sess, NULL);
        if (r != 0) { fprintf(stderr, "load begin: %s\n", wiredtiger_strerror(r)); exit(1); }
        bool ok = true;
        for (uint64_t j = i; j < batch_end; j++) {
            key_bytes(j, kb);
            cur->set_key(cur, &ki);
            cur->set_value(cur, &vi);
            int ir = cur->insert(cur);
            if (ir != 0) { ok = false; break; }
        }
        if (ok) {
            r = sess->commit_transaction(sess, NULL);
            if (r != 0) { sess->rollback_transaction(sess, NULL); }
        } else {
            sess->rollback_transaction(sess, NULL);
        }
        i = batch_end;
    }
    sess->close(sess, NULL);
    return NULL;
}

/* ---- one measured op per workload ----------------------------------- */
/*
 * Mirrors xbench.rs branch-for-branch, including the exact order of RNG
 * consumption (zipf.next / rng.pct / rng.below) so key sequences match.
 */

static void *work_thread(void *arg) {
    Worker *w = (Worker *)arg;
    Bench *b = w->b;

    WT_SESSION *sess;
    int r = b->conn->open_session(b->conn, NULL, NULL, &sess);
    if (r != 0) { fprintf(stderr, "open_session: %s\n", wiredtiger_strerror(r)); exit(1); }
    WT_CURSOR *cur;
    r = sess->open_cursor(sess, "table:xbench", NULL, NULL, &cur);
    if (r != 0) { fprintf(stderr, "open_cursor: %s\n", wiredtiger_strerror(r)); exit(1); }

    Rng rng = { b->seed ^ ((uint64_t)w->tid * 0x9E3779B9ULL) };
    Zipf zipf;
    zipf_init(&zipf, b->records);

    /* per-thread insert counter for tdb_write, matches Rust init. */
    uint64_t insert_ctr = b->records + (uint64_t)w->tid * 100000000ULL;

    uint8_t kb[16];
    WT_ITEM ki, vi, got;
    memset(&ki, 0, sizeof ki);
    memset(&vi, 0, sizeof vi);
    memset(&got, 0, sizeof got);
    vi.data = b->value; vi.size = b->value_size;

    const char *bcfg = b->begin_cfg; /* NULL = WT default (snapshot) */
    const char *wl = b->workload;

    while (!atomic_load_explicit(&b->stop, memory_order_relaxed)) {
        uint64_t t0 = now_us();

        if (strcmp(wl, "ycsb_a") == 0) {
            uint64_t id = zipf_next(&zipf, &rng);
            key_bytes(id, kb);
            if (rng_pct(&rng) < 50) {
                if (sess->begin_transaction(sess, bcfg) == 0) {
                    ki.data = kb; ki.size = 16;
                    cur->set_key(cur, &ki);
                    (void)cur->search(cur);
                    cur->reset(cur);
                    sess->commit_transaction(sess, NULL);
                }
            } else {
                if (sess->begin_transaction(sess, bcfg) == 0) {
                    ki.data = kb; ki.size = 16;
                    cur->set_key(cur, &ki);
                    cur->set_value(cur, &vi);
                    int ir = cur->insert(cur);
                    cur->reset(cur);
                    if (ir == 0) {
                        if (sess->commit_transaction(sess, NULL) != 0) w->local_aborts++;
                    } else {
                        sess->rollback_transaction(sess, NULL);
                        w->local_aborts++;
                    }
                }
            }
        } else if (strcmp(wl, "ycsb_c") == 0) {
            uint64_t id = zipf_next(&zipf, &rng);
            key_bytes(id, kb);
            if (sess->begin_transaction(sess, bcfg) == 0) {
                ki.data = kb; ki.size = 16;
                cur->set_key(cur, &ki);
                (void)cur->search(cur);
                cur->reset(cur);
                sess->commit_transaction(sess, NULL);
            }
        } else if (strcmp(wl, "tdb_write") == 0) {
            uint64_t id = insert_ctr++;
            key_bytes(id, kb);
            if (sess->begin_transaction(sess, bcfg) == 0) {
                ki.data = kb; ki.size = 16;
                cur->set_key(cur, &ki);
                cur->set_value(cur, &vi);
                int ir = cur->insert(cur);
                cur->reset(cur);
                if (ir == 0) {
                    if (sess->commit_transaction(sess, NULL) != 0) w->local_aborts++;
                } else {
                    sess->rollback_transaction(sess, NULL);
                    w->local_aborts++;
                }
            }
        } else if (strcmp(wl, "txn_mix") == 0) {
            /* 4-op txn: 2 update + 1 read + 1 delete, Zipfian. */
            if (sess->begin_transaction(sess, bcfg) == 0) {
                bool ok = true;
                for (int j = 0; j < 4; j++) {
                    uint64_t id = zipf_next(&zipf, &rng);
                    key_bytes(id, kb);
                    ki.data = kb; ki.size = 16;
                    int ir;
                    if (j == 0 || j == 1) {
                        cur->set_key(cur, &ki);
                        cur->set_value(cur, &vi);
                        ir = cur->insert(cur);
                    } else if (j == 2) {
                        cur->set_key(cur, &ki);
                        ir = cur->search(cur);
                        if (ir == WT_NOTFOUND) ir = 0; /* read-miss is not an error */
                    } else {
                        cur->set_key(cur, &ki);
                        ir = cur->remove(cur);
                        if (ir == WT_NOTFOUND) ir = 0; /* delete-miss is not an error */
                    }
                    cur->reset(cur);
                    if (ir != 0) { ok = false; break; }
                }
                if (ok) {
                    if (sess->commit_transaction(sess, NULL) != 0) w->local_aborts++;
                } else {
                    sess->rollback_transaction(sess, NULL);
                    w->local_aborts++;
                }
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
                if (sess->begin_transaction(sess, bcfg) == 0) {
                    ki.data = kb; ki.size = 16;
                    cur->set_key(cur, &ki);
                    cur->set_value(cur, &vi);
                    int ir = cur->insert(cur);
                    cur->reset(cur);
                    if (ir == 0) {
                        if (sess->commit_transaction(sess, NULL) != 0) w->local_aborts++;
                    } else {
                        sess->rollback_transaction(sess, NULL);
                        w->local_aborts++;
                    }
                }
            } else {
                if (sess->begin_transaction(sess, bcfg) == 0) {
                    ki.data = kb; ki.size = 16;
                    cur->set_key(cur, &ki);
                    (void)cur->search(cur);
                    cur->reset(cur);
                    sess->commit_transaction(sess, NULL);
                }
            }
        } else if (strcmp(wl, "scan_under_write") == 0) {
            if (w->tid % 2 == 0) {
                /* scanner: forward scan of 100 records from a random start. */
                uint64_t id = zipf_next(&zipf, &rng);
                key_bytes(id, kb);
                if (sess->begin_transaction(sess, bcfg) == 0) {
                    ki.data = kb; ki.size = 16;
                    cur->set_key(cur, &ki);
                    int exact = 0;
                    int ir = cur->search_near(cur, &exact);
                    if (ir == 0) {
                        for (int n = 0; n < 100; n++) {
                            if (cur->next(cur) != 0) break;
                        }
                    }
                    cur->reset(cur);
                    sess->commit_transaction(sess, NULL);
                }
            } else {
                uint64_t id = zipf_next(&zipf, &rng);
                key_bytes(id, kb);
                if (sess->begin_transaction(sess, bcfg) == 0) {
                    ki.data = kb; ki.size = 16;
                    cur->set_key(cur, &ki);
                    cur->set_value(cur, &vi);
                    int ir = cur->insert(cur);
                    cur->reset(cur);
                    if (ir == 0) {
                        if (sess->commit_transaction(sess, NULL) != 0) w->local_aborts++;
                    } else {
                        sess->rollback_transaction(sess, NULL);
                        w->local_aborts++;
                    }
                }
            }
        }

        hist_record(&w->hist, now_us() - t0);
        atomic_fetch_add_explicit(&b->ops, 1, memory_order_relaxed);
    }

    (void)got;
    atomic_fetch_add_explicit(&b->aborts, w->local_aborts, memory_order_relaxed);
    sess->close(sess, NULL);
    return NULL;
}

/* ---- fs sanity: refuse tmpfs (parity with Noxu) --------------------- */

static bool is_tmpfs(const char *dir) {
    /* cheap check via `df -T`; best-effort, mirrors Noxu's guard. */
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
    const char *dir       = envs("BENCH_DIR", "/tmp/wt-xbench");
    uint64_t records      = envp("BENCH_RECORDS", 10000000ULL);
    uint64_t cache        = envp("BENCH_CACHE", 2ULL * 1024 * 1024 * 1024);
    size_t value_size     = (size_t)envp("BENCH_VALUE", 1024);
    unsigned threads      = (unsigned)envp("BENCH_THREADS", 64);
    uint64_t seconds      = envp("BENCH_SECONDS", 30);
    const char *durability= envs("BENCH_DURABILITY", "SYNC");
    const char *workload  = envs("BENCH_WORKLOAD", "ycsb_a");
    uint64_t seed         = envp("BENCH_SEED", 0xC0FFEEULL);
    const char *isolation = envs("BENCH_ISOLATION", "default");
    /* WT table type: "btree" (default row-store) or "lsm". LSM is the fairer
     * comparison to JE/Noxu's append-only-WAL-logged B-tree (LSM-like). */
    const char *wt_type   = envs("BENCH_WT_TYPE", "btree");

    if (is_tmpfs(dir)) {
        fprintf(stderr, "ABORT: %s is tmpfs; use real NVMe\n", dir);
        return 2;
    }
    mkdir(dir, 0755);

    /*
     * Durability config strings (STRACE-VERIFY these fsync at commit):
     *   SYNC    -> log enabled + transaction_sync=(enabled=true,method=fsync)
     *              => every commit_transaction flushes+fsyncs the log.
     *   NO_SYNC -> log enabled + transaction_sync=(enabled=false)
     *              => commits return without forcing the log to disk.
     * Log is kept enabled in both so the on-disk shape matches; only the
     * per-commit sync behaviour differs, matching Noxu COMMIT_SYNC vs
     * COMMIT_NO_SYNC.
     */
    bool sync = (strcmp(durability, "NO_SYNC") != 0 &&
                 strcmp(durability, "WRITE_NO_SYNC") != 0);
    char conn_cfg[512];
    snprintf(conn_cfg, sizeof conn_cfg,
        "create,cache_size=%llu,statistics=(none),"
        "log=(enabled=true),"
        "transaction_sync=(enabled=%s,method=%s)",
        (unsigned long long)cache,
        sync ? "true" : "false",
        sync ? "fsync" : "none");

    /*
     * Isolation: WT's strongest is snapshot (the default). "serializable" maps
     * to snapshot here -- WT has no separate serializable level, so serializable
     * and default runs are the same WT isolation (noted in the RESULT/iso tag
     * and here). "default" leaves begin_transaction config NULL (snapshot).
     */
    const char *begin_cfg = NULL; /* snapshot (default) */
    /* If a future run wants read-committed explicitly, set:
     *   begin_cfg = "isolation=read-committed";
     * Both default and serializable use snapshot to match Noxu's comparable run. */

    printf("=== WIREDTIGER xbench: workload=%s records=%llu cache=%lluGiB "
           "value=%zu threads=%u secs=%llu dur=%s iso=%s type=%s ===\n",
           workload, (unsigned long long)records,
           (unsigned long long)(cache / 1024 / 1024 / 1024),
           value_size, threads, (unsigned long long)seconds,
           durability, isolation, wt_type);
    if (strcmp(isolation, "serializable") == 0)
        printf("   note: WiredTiger max isolation is snapshot; "
               "serializable run uses snapshot.\n");

    /* value buffer: value_size bytes of 0x5A. */
    uint8_t *value = malloc(value_size);
    if (!value) { fprintf(stderr, "OOM value\n"); return 1; }
    memset(value, 0x5A, value_size);

    WT_CONNECTION *conn;
    int r = wiredtiger_open(dir, NULL, conn_cfg, &conn);
    if (r != 0) { fprintf(stderr, "wiredtiger_open: %s\n", wiredtiger_strerror(r)); return 1; }

    WT_SESSION *setup;
    r = conn->open_session(conn, NULL, NULL, &setup);
    if (r != 0) { fprintf(stderr, "open_session: %s\n", wiredtiger_strerror(r)); return 1; }
    /* Row-store (btree) vs LSM. LSM keeps u/u key/value format but adds
     * type=lsm so writes land in an in-memory chunk + background merges. */
    char create_cfg[256];
    bool use_lsm = (strcmp(wt_type, "lsm") == 0);
    snprintf(create_cfg, sizeof create_cfg,
             "key_format=u,value_format=u%s",
             use_lsm ? ",type=lsm" : "");
    r = setup->create(setup, "table:xbench", create_cfg);
    if (r != 0) { fprintf(stderr, "create: %s\n", wiredtiger_strerror(r)); return 1; }
    setup->close(setup, NULL);

    Bench b;
    memset(&b, 0, sizeof b);
    b.conn = conn;
    b.records = records;
    b.value_size = value_size;
    b.value = value;
    b.workload = workload;
    b.isolation = isolation;
    b.begin_cfg = begin_cfg;
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
    /* checkpoint so the load is durable and cache is primed. */
    {
        WT_SESSION *cp;
        if (conn->open_session(conn, NULL, NULL, &cp) == 0) {
            cp->checkpoint(cp, NULL);
            cp->close(cp, NULL);
        }
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

    printf("RESULT engine=wiredtiger workload=%s iso=%s dur=%s type=%s threads=%u "
           "throughput=%.0f ops/s ops=%llu aborts=%llu abort_rate=%.4f "
           "p50=%llu p90=%llu p99=%llu p999=%llu max=%llu\n",
           workload, isolation, durability, wt_type, threads,
           thr, total, ab, abort_rate,
           (unsigned long long)hist_pct(&merged, 0.50),
           (unsigned long long)hist_pct(&merged, 0.90),
           (unsigned long long)hist_pct(&merged, 0.99),
           (unsigned long long)hist_pct(&merged, 0.999),
           (unsigned long long)merged.max);

    free(workers);
    free(wth);
    conn->close(conn, NULL);
    free(value);
    return 0;
}
