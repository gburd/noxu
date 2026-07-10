/*
 * WiredTiger crash-durability control test. Same protocol as tdb_crash.c.
 *   write:  log=(enabled=true), transaction_sync=(enabled=true,method=fsync).
 *           Single thread; after each commit_transaction returns 0, record the
 *           acked id into <ackfile> (pwrite + fdatasync of the ackfile).
 *   verify: reopen, read last acked id A, search every id in [0,A], count
 *           survivors. WT is the durable control: expect survivors == A+1.
 */
#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <wiredtiger.h>

static void key_bytes(uint64_t id, uint8_t out[16]) {
    uint64_t tail = id * 2654435761ULL;
    for (int i = 0; i < 8; i++) out[i]     = (uint8_t)(id   >> (56 - 8 * i));
    for (int i = 0; i < 8; i++) out[8 + i] = (uint8_t)(tail >> (56 - 8 * i));
}
#define VSZ 1024

int main(int argc, char **argv) {
    if (argc < 4) { fprintf(stderr, "usage: %s write|verify <dir> <ackfile>\n", argv[0]); return 2; }
    const char *mode = argv[1], *dir = argv[2], *ackf = argv[3];
    mkdir(dir, 0755);

    WT_CONNECTION *conn;
    const char *cfg = "create,cache_size=256M,log=(enabled=true),"
                      "transaction_sync=(enabled=true,method=fsync)";
    int r = wiredtiger_open(dir, NULL, cfg, &conn);
    if (r != 0) { fprintf(stderr, "wiredtiger_open: %s\n", wiredtiger_strerror(r)); return 3; }
    WT_SESSION *s;
    conn->open_session(conn, NULL, NULL, &s);
    s->create(s, "table:crash", "key_format=u,value_format=u");
    WT_CURSOR *c;
    s->open_cursor(s, "table:crash", NULL, NULL, &c);

    uint8_t val[VSZ]; memset(val, 0x5A, VSZ);
    uint8_t kb[16];
    WT_ITEM ki, vi, got;
    memset(&ki,0,sizeof ki); memset(&vi,0,sizeof vi); memset(&got,0,sizeof got);
    vi.data = val; vi.size = VSZ;

    if (strcmp(mode, "write") == 0) {
        int afd = open(ackf, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (afd < 0) { perror("open ackfile"); return 3; }
        uint64_t id = 0;
        for (;;) {
            key_bytes(id, kb);
            if (s->begin_transaction(s, NULL) != 0) continue;
            ki.data = kb; ki.size = 16;
            c->set_key(c, &ki); c->set_value(c, &vi);
            if (c->insert(c) != 0) { c->reset(c); s->rollback_transaction(s, NULL); continue; }
            c->reset(c);
            if (s->commit_transaction(s, NULL) != 0) continue;  /* ACKED on 0 */
            char buf[32]; int n = snprintf(buf, sizeof buf, "%llu\n", (unsigned long long)id);
            (void)pwrite(afd, buf, (size_t)n, 0);
            fdatasync(afd);
            id++;
        }
    } else {
        FILE *fp = fopen(ackf, "r");
        if (!fp) { fprintf(stderr, "no ackfile\n"); return 3; }
        unsigned long long acked = 0;
        if (fscanf(fp, "%llu", &acked) != 1) { fprintf(stderr, "empty ackfile\n"); return 3; }
        fclose(fp);
        uint64_t survived = 0, missing = 0, first_missing = UINT64_MAX;
        for (uint64_t id = 0; id <= acked; id++) {
            key_bytes(id, kb);
            s->begin_transaction(s, NULL);
            ki.data = kb; ki.size = 16;
            c->set_key(c, &ki);
            int sr = c->search(c);
            c->reset(c);
            s->commit_transaction(s, NULL);
            if (sr == 0) survived++;
            else { missing++; if (first_missing == UINT64_MAX) first_missing = id; }
        }
        printf("CRASHTEST engine=wiredtiger acked=%llu expected=%llu survived=%llu missing=%llu first_missing=%lld\n",
               acked, acked + 1, (unsigned long long)survived, (unsigned long long)missing,
               first_missing == UINT64_MAX ? -1LL : (long long)first_missing);
    }
    conn->close(conn, NULL);
    return 0;
}
