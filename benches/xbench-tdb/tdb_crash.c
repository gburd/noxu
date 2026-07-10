/*
 * TidesDB crash-durability test.
 *
 * Question: does TidesDB ack a commit (tidesdb_txn_commit returns 0) BEFORE
 * that commit is durable on disk? If so, a kill -9 immediately after an acked
 * commit would LOSE acked data -> unfair "ack-before-durable" at SYNC.
 *
 * Method (two modes, selected by argv[1]):
 *   write <dir> <ack_file>:
 *     open at TDB_SYNC_FULL, single thread, insert keys id=0,1,2,...
 *     After EACH tidesdb_txn_commit() returns 0, record the highest acked id
 *     into <ack_file> (pwrite + fdatasync of the ack file itself, so OUR record
 *     of "acked" is itself durable and survives the kill). Runs forever until
 *     the parent kill -9's us. We never close the db cleanly.
 *   verify <dir> <ack_file>:
 *     reopen the SAME dir, read the last acked id A from <ack_file>, then
 *     tidesdb_txn_get every id in [0, A]. Report how many survived. If
 *     survivors == A+1 -> every acked commit was durable (FAIR). If
 *     survivors < A+1 -> acked-but-lost commits (ack-before-durable => UNFAIR).
 *
 * Value size 1024 bytes of 0x5A, 16-byte keys (same key_bytes as xbench).
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <tidesdb.h>

static void key_bytes(uint64_t id, uint8_t out[16]) {
    uint64_t tail = id * 2654435761ULL;
    for (int i = 0; i < 8; i++) out[i]     = (uint8_t)(id   >> (56 - 8 * i));
    for (int i = 0; i < 8; i++) out[8 + i] = (uint8_t)(tail >> (56 - 8 * i));
}

#define VSZ 1024

static tidesdb_t *open_db(const char *dir) {
    tidesdb_config_t cfg;
    memset(&cfg, 0, sizeof cfg);
    cfg.db_path = (char *)dir;
    tidesdb_t *db = NULL;
    if (tidesdb_open(&cfg, &db) != 0 || !db) {
        fprintf(stderr, "tidesdb_open(%s) failed\n", dir);
        exit(3);
    }
    return db;
}

static tidesdb_column_family_t *get_or_make_cf(tidesdb_t *db) {
    tidesdb_column_family_config_t cf = tidesdb_default_column_family_config();
    cf.write_buffer_size = 64ULL * 1024 * 1024;
    cf.sync_mode = TDB_SYNC_FULL;   /* per-commit durability requested */
    tidesdb_create_column_family(db, "crash", &cf); /* ok if exists */
    tidesdb_column_family_t *h = tidesdb_get_column_family(db, "crash");
    if (!h) { fprintf(stderr, "get_column_family(crash) failed\n"); exit(3); }
    return h;
}

int main(int argc, char **argv) {
    if (argc < 4) { fprintf(stderr, "usage: %s write|verify <dir> <ackfile>\n", argv[0]); return 2; }
    const char *mode = argv[1];
    const char *dir  = argv[2];
    const char *ackf = argv[3];

    uint8_t val[VSZ];
    memset(val, 0x5A, VSZ);
    uint8_t kb[16];

    tidesdb_t *db = open_db(dir);
    tidesdb_column_family_t *cf = get_or_make_cf(db);

    if (strcmp(mode, "write") == 0) {
        int afd = open(ackf, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (afd < 0) { perror("open ackfile"); return 3; }
        uint64_t id = 0;
        for (;;) {
            key_bytes(id, kb);
            tidesdb_txn_t *t = NULL;
            if (tidesdb_txn_begin_with_isolation(db, TDB_ISOLATION_READ_COMMITTED, &t) != 0) {
                fprintf(stderr, "begin failed at id=%llu\n", (unsigned long long)id);
                continue;
            }
            if (tidesdb_txn_put(t, cf, kb, 16, val, VSZ, 0) != 0) {
                tidesdb_txn_rollback(t); tidesdb_txn_free(t); continue;
            }
            if (tidesdb_txn_commit(t) != 0) {  /* commit ACKED here on 0 */
                tidesdb_txn_rollback(t); tidesdb_txn_free(t); continue;
            }
            tidesdb_txn_free(t);
            /* commit is now ACKED. Durably record that we acked id. */
            char buf[32];
            int n = snprintf(buf, sizeof buf, "%llu\n", (unsigned long long)id);
            (void)pwrite(afd, buf, (size_t)n, 0);
            fdatasync(afd);   /* our ack-record is durable regardless of TidesDB */
            id++;
        }
    } else { /* verify */
        FILE *fp = fopen(ackf, "r");
        if (!fp) { fprintf(stderr, "no ackfile %s\n", ackf); return 3; }
        unsigned long long acked = 0;
        if (fscanf(fp, "%llu", &acked) != 1) { fprintf(stderr, "empty ackfile\n"); return 3; }
        fclose(fp);
        /* acked = highest id whose commit returned 0. Expect ids [0..acked]. */
        uint64_t survived = 0, missing = 0;
        uint64_t first_missing = UINT64_MAX;
        for (uint64_t id = 0; id <= acked; id++) {
            key_bytes(id, kb);
            tidesdb_txn_t *t = NULL;
            if (tidesdb_txn_begin_with_isolation(db, TDB_ISOLATION_READ_COMMITTED, &t) != 0) { continue; }
            uint8_t *out = NULL; size_t osz = 0;
            int r = tidesdb_txn_get(t, cf, kb, 16, &out, &osz);
            if (r == 0 && out && osz == VSZ) survived++;
            else { missing++; if (first_missing == UINT64_MAX) first_missing = id; }
            if (out) tidesdb_free(out);
            tidesdb_txn_commit(t);
            tidesdb_txn_free(t);
        }
        printf("CRASHTEST engine=tidesdb acked=%llu expected=%llu survived=%llu missing=%llu first_missing=%lld\n",
               acked, acked + 1, (unsigned long long)survived, (unsigned long long)missing,
               first_missing == UINT64_MAX ? -1LL : (long long)first_missing);
        tidesdb_close(db);
    }
    return 0;
}
