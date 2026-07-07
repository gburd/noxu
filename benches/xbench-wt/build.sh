#!/usr/bin/env bash
# Build the WiredTiger cross-engine benchmark driver.
# WiredTiger is expected pre-built at /data/wiredtiger/build (header + lib).
set -euo pipefail

WT_BUILD="${WT_BUILD:-/data/wiredtiger/build}"
SRC="$(cd "$(dirname "$0")" && pwd)/wt_xbench.c"
OUT="$(cd "$(dirname "$0")" && pwd)/wt_xbench"

INC="$WT_BUILD/include"
[ -d "$INC" ] || INC="$WT_BUILD"   # header sometimes at build root
LIBDIR="$WT_BUILD"

# Prefer shared lib; fall back to static. -ldl -lm needed for static WT.
gcc -std=c11 -O2 -Wall -Wextra \
    -o "$OUT" "$SRC" \
    -I"$INC" \
    -L"$LIBDIR" \
    -Wl,-rpath,"$LIBDIR" \
    -lwiredtiger -lpthread -ldl -lm

echo "built: $OUT"
