/*
 * tc_netem_helper.c — setuid-root wrapper for 'tc qdisc' on loopback.
 *
 * Allows the torture test to inject kernel-level network faults without
 * requiring the test process to have CAP_NET_ADMIN.
 *
 * SECURITY: only permits 'qdisc' sub-command on device 'lo'.  Any other
 * invocation is rejected before exec.
 *
 * Build:
 *   gcc -O2 -Wall -o scripts/tc_netem_helper scripts/tc_netem_helper.c
 *
 * Install (run once as root):
 *   sudo chown root:root scripts/tc_netem_helper
 *   sudo chmod u+s       scripts/tc_netem_helper
 *
 * After that, the torture test binary will find and use this helper
 * automatically when it cannot run 'tc' directly.
 */

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>

/* Maximum number of arguments we'll forward to tc (including "tc" itself). */
#define MAX_ARGS 64

static void die(const char *msg) {
    fprintf(stderr, "tc_netem_helper: %s\n", msg);
    exit(1);
}

int main(int argc, char *argv[]) {
    if (argc < 2)
        die("usage: tc_netem_helper qdisc [add|change|del] dev lo ...");

    /* argv[0] is the helper binary; argv[1..] are the tc sub-args. */

    /* Rule 1: first user arg must be 'qdisc'. */
    if (strcmp(argv[1], "qdisc") != 0)
        die("only 'qdisc' sub-command is allowed");

    /* Rule 2: must contain 'dev lo' somewhere in the argument list. */
    int found_dev_lo = 0;
    for (int i = 2; i < argc - 1; i++) {
        if (strcmp(argv[i], "dev") == 0 && strcmp(argv[i+1], "lo") == 0) {
            found_dev_lo = 1;
            break;
        }
    }
    if (!found_dev_lo)
        die("'dev lo' required — only loopback manipulation is permitted");

    /* Rule 3: no shell metacharacters in any argument. */
    const char *forbidden = "|&;`$<>(){}\\\"'*?[]\n\r\t";
    for (int i = 1; i < argc; i++) {
        if (strpbrk(argv[i], forbidden))
            die("forbidden characters in argument");
    }

    /* Build the exec argv: ["tc", argv[1], argv[2], ..., NULL]. */
    if (argc >= MAX_ARGS)
        die("too many arguments");

    char *tc_argv[MAX_ARGS + 1];
    tc_argv[0] = "tc";
    for (int i = 1; i < argc; i++)
        tc_argv[i] = argv[i];
    tc_argv[argc] = NULL;

    /* Drop any supplementary groups, then set real+effective uid to root. */
    if (setuid(0) != 0) {
        perror("tc_netem_helper: setuid(0)");
        exit(1);
    }

    execvp("tc", tc_argv);
    perror("tc_netem_helper: execvp(tc)");
    return 1;
}
