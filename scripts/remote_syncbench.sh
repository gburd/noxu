#!/usr/bin/env bash
# Remote driver for the noxu-sync vs parking_lot A/B microbenchmark
# (crates/noxu-sync/benches/sync_vs_parking_lot.rs).
#
# Usage (on the measurement box):  scripts/remote_syncbench.sh <run-label>
#
# Builds --release, runs the full bench once, and appends host load before and
# after so the report can state what else the box was doing.  Writes
# /data/bench-out/DONE (or FAILED) as a poll target so the caller never has to
# hold an SSH session open for the whole run.
set -u
cd /data/noxu || exit 1
export PATH="$HOME/.cargo/bin:$PATH"
export CARGO_TARGET_DIR=/data/target

LABEL="${1:?usage: remote_syncbench.sh <run-label>}"
OUT=/data/bench-out
mkdir -p "$OUT"
find "$OUT" -maxdepth 1 -type f -name 'DONE' -delete
find "$OUT" -maxdepth 1 -type f -name 'FAILED' -delete

log() { echo "$*" >>"$OUT/log.txt"; }

{
  echo "=== run $LABEL @ $(date -u) ==="
  uptime
  nproc
  df -T /data | tail -1
} >>"$OUT/log.txt" 2>&1

if ! cargo build --release -p noxu-sync --benches >>"$OUT/log.txt" 2>&1; then
  echo build-failed >"$OUT/FAILED"
  log "BUILD FAILED"
  exit 1
fi

log "--- load before bench ---"
uptime >>"$OUT/log.txt"

if ! cargo bench -p noxu-sync --bench sync_vs_parking_lot \
      -- --noplot >"$OUT/bench-$LABEL.txt" 2>&1; then
  echo bench-failed >"$OUT/FAILED"
  log "BENCH FAILED (see bench-$LABEL.txt)"
  exit 1
fi

log "--- load after bench ---"
uptime >>"$OUT/log.txt"
log "=== run $LABEL done @ $(date -u) ==="
echo ok >"$OUT/DONE"
