#!/usr/bin/env bash
# Build the TidesDB cross-engine benchmark driver.
# TidesDB v9.3.10 is expected pre-built at /data/TidesDB/build with the header
# at /data/TidesDB/src/tidesdb.h and libtidesdb.so in the build dir.
set -euo pipefail

TDB_SRC_DIR="${TDB_SRC_DIR:-/data/TidesDB/src}"
TDB_BUILD="${TDB_BUILD:-/data/TidesDB/build}"
SRC="$(cd "$(dirname "$0")" && pwd)/tdb_xbench.c"
OUT="$(cd "$(dirname "$0")" && pwd)/tdb_xbench"

# libtidesdb.so normally pulls its own compression deps transitively. If the
# link fails with undefined refs to LZ4_*/ZSTD_*/snappy_*, uncomment EXTRA.
# EXTRA="-llz4 -lzstd -lsnappy"
EXTRA="${EXTRA:-}"

gcc -std=c11 -O2 -Wall -Wextra \
    -o "$OUT" "$SRC" \
    -I"$TDB_SRC_DIR" \
    -L"$TDB_BUILD" \
    -Wl,-rpath,"$TDB_BUILD" \
    -ltidesdb -lpthread -lm ${EXTRA}

echo "built: $OUT"
